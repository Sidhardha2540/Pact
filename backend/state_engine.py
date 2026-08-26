"""The four consistency models.

Each domain type (Decision, Discovery, Intent, Question) gets its own function
implementing the right consistency story. Generic write paths are deliberately
absent — they would lose the per-type semantics.

Every successful mutation:
  1. updates SQLite under db_lock,
  2. appends an immutable JSONL log entry,
  3. broadcasts a WebSocket event,
  4. (questions only) regenerates inbox.md.

Conflict codes returned to callers:
  409 — Decision conflict (FWW violated)
  410 — Discovery hash mismatch (caller passed a stale hash)
  423 — Intent scope overlap (locked)
  202 — Question accepted but non-blocking (queued)
  403 — Scope not owned by calling agent (exclusive mode)
"""
from __future__ import annotations

import json
import logging
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

from . import config, storage, ws_broadcast
from .config import (
    DEFAULT_INTENT_TTL_MINUTES,
    MAX_ACTIVE_INTENTS_PER_AGENT,
)
from .models import now_iso

logger = logging.getLogger(__name__)


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------

def _gen_id(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex[:8]}"


def _scopes_overlap(a: str, b: str) -> bool:
    """Prefix-aware overlap check.

    `auth/` overlaps `auth/middleware.ts` and vice versa.
    `auth.ts` does NOT overlap `auth_helpers.ts` (must be a path boundary).
    """
    a = a.rstrip("/")
    b = b.rstrip("/")
    if a == b:
        return True
    # Either is a strict path-prefix of the other.
    return (
        a.startswith(b + "/")
        or b.startswith(a + "/")
    )


def _uri_scope_matches(registered: str, requested: str) -> bool:
    """Does `registered` scope authorize access to `requested` scope?

    Rules (apply to both file paths and virt:// URIs):
    - Exact match always allowed.
    - registered is a strict path-prefix of requested (path boundary = '/').
    - src/auth/ owns src/auth/middleware.ts  ✓
    - src/auth  owns src/auth/sub.ts         ✓  (trailing slash normalised)
    - src/auth  does NOT own src/auth_helpers ✗
    - virt://db owns virt://db/schema        ✓
    """
    reg = registered.rstrip("/")
    req = requested.rstrip("/")
    if reg == req:
        return True
    return req.startswith(reg + "/")


def _normalize_sha256(value: str | None) -> str | None:
    if not value:
        return None
    return value.removeprefix("sha256:").strip().lower()


def _current_file_hash(scope: str) -> str | None:
    """Return the current sha256 for file-backed scopes, or None if not checkable."""
    if "://" in scope:
        return None

    root = config.WORKSPACE_ROOT
    path = (root / scope).resolve()
    try:
        path.relative_to(root)
    except ValueError:
        return None
    if not path.is_file():
        return None

    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


# ----------------------------------------------------------------------
# Scope permission check (NEW)
# ----------------------------------------------------------------------

async def check_scope_permission(agent_id: str, scope: str) -> tuple[bool, str]:
    """Return (allowed, mode) for agent_id writing to scope.

    Two-phase ownership model:
      Phase 1 — static: agent declared scope at register time.
      Phase 2 — dynamic: agent has an active intent covering the scope
                (established via claim_intent, the real coordination gate).

    Logic:
    - Unregistered agent                           → (False, "exclusive")
    - Static scope match                           → (True,  mode)
    - No static scope but active intent matches    → (True,  mode)
    - Registered, no match anywhere, exclusive     → (False, "exclusive")
    - Registered, no match anywhere, collaborative → (False, "collaborative")
      (caller logs and allows — collaborative agents may write anywhere)
    """
    conn = storage.db()
    cur = await conn.execute(
        "SELECT scope, mode FROM participants WHERE agent_id = ?",
        (agent_id,),
    )
    row = await cur.fetchone()
    await cur.close()

    if row is None:
        return False, "exclusive"

    mode: str = row["mode"]

    # Phase 1: static scope declared at registration.
    registered_scopes: list[str] = json.loads(row["scope"])
    for reg_scope in registered_scopes:
        if _uri_scope_matches(reg_scope, scope):
            return True, mode

    # Phase 2: dynamic ownership via an active intent.
    # If the agent has claimed an intent that covers this scope, allow it.
    cur = await conn.execute(
        "SELECT scope FROM intents WHERE agent = ? AND status = 'active'",
        (agent_id,),
    )
    active_intents = await cur.fetchall()
    await cur.close()

    for intent_row in active_intents:
        if _uri_scope_matches(intent_row["scope"], scope):
            return True, mode

    return False, mode


async def get_participant_mode(agent_id: str) -> str | None:
    """Return the participant's coordination mode, or None if unregistered."""
    conn = storage.db()
    cur = await conn.execute(
        "SELECT mode FROM participants WHERE agent_id = ?",
        (agent_id,),
    )
    row = await cur.fetchone()
    await cur.close()
    return row["mode"] if row is not None else None


# ----------------------------------------------------------------------
# DECISION — First-Write-Wins
# ----------------------------------------------------------------------

async def commit_decision(
    scope: str,
    key: str,
    value: str,
    agent: str,
    anchor: str | None = None,
    rationale: str | None = None,
) -> dict[str, Any]:
    """Commit a contract. Existing value with a different value → 409 conflict.

    Returns:
      {"status": "committed", "sequence": int} on first write
      {"status": "noop"}                       on idempotent re-write (same value)
      {"status": "conflict", "code": 403, ...} scope not owned
      {"status": "conflict", "code": 409, ...} on different existing value
    """
    allowed, mode = await check_scope_permission(agent, scope)
    if not allowed:
        if mode == "collaborative":
            logger.info(
                "commit_decision: agent %s writing outside owned scope %s "
                "(collaborative mode — allowed)", agent, scope,
            )
        else:
            return {
                "status": "conflict",
                "code": 403,
                "detail": f"Agent '{agent}' does not own scope '{scope}'.",
                "next_step": f"Call claim_intent(scope='{scope}', action='...') first to acquire a lease, "
                             f"then retry commit_decision. Or declare '{scope}' in register(scope=[...]).",
            }

    async with storage.transaction() as conn:
        cur = await conn.execute(
            "SELECT value, agent FROM decisions WHERE scope = ? AND key = ?",
            (scope, key),
        )
        existing = await cur.fetchone()
        await cur.close()

        if existing is not None:
            if existing["value"] == value:
                return {"status": "noop"}
            return {
                "status": "conflict",
                "code": 409,
                "existing": {
                    "value": existing["value"],
                    "agent": existing["agent"],
                },
                "attempted": {"value": value, "agent": agent},
                "next_step": f"Do NOT retry — First-Write-Wins means the existing value stands. "
                             f"Call raise_question(scope='{scope}', asks='...') to escalate "
                             f"the conflict to a human or the owning agent for resolution.",
            }

        seq = await storage.next_sequence()
        ts = now_iso()
        await conn.execute(
            "INSERT INTO decisions "
            "(scope, key, value, agent, rationale, anchor, mode, created_at, sequence) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (scope, key, value, agent, rationale, anchor, mode, ts, seq),
        )
        await conn.commit()

    log_entry = {
        "type": "decision_committed",
        "scope": scope, "key": key, "value": value,
        "agent": agent, "rationale": rationale, "anchor": anchor,
        "sequence": seq, "ts": ts,
    }
    await storage.append_log(log_entry)
    await ws_broadcast.publish("decision_committed", {
        "scope": scope, "key": key, "value": value,
        "agent": agent, "rationale": rationale, "anchor": anchor,
        "sequence": seq, "created_at": ts,
    })
    return {"status": "committed", "sequence": seq}


# ----------------------------------------------------------------------
# DISCOVERY — Last-Write-Wins + supersede flag
# ----------------------------------------------------------------------

async def share_discovery(
    scope: str,
    summary: str,
    agent: str,
    file_hash: str | None = None,
    confidence: str = "unverified",
) -> dict[str, Any]:
    """Share an observation. Older non-superseded entries on the same scope
    are flipped to superseded=1 (kept for audit, hidden from default reads).

    Returns:
      {"status": "shared", "id": str}         on success
      {"status": "conflict", "code": 403, ...} scope not owned (exclusive)
    """
    allowed, mode = await check_scope_permission(agent, scope)
    if not allowed:
        if mode == "collaborative":
            logger.info(
                "share_discovery: agent %s writing outside owned scope %s "
                "(collaborative mode — allowed)", agent, scope,
            )
        else:
            return {
                "status": "conflict",
                "code": 403,
                "detail": f"Agent '{agent}' does not own scope '{scope}'.",
                "next_step": f"Call claim_intent(scope='{scope}', action='...') first, "
                             f"then retry share_discovery.",
            }

    discovery_id = _gen_id("disc")
    ts = now_iso()

    async with storage.transaction() as conn:
        # Mark prior active discoveries on this scope as superseded.
        cur = await conn.execute(
            "SELECT id FROM discoveries WHERE scope = ? AND superseded = 0",
            (scope,),
        )
        prior_ids = [row["id"] for row in await cur.fetchall()]
        await cur.close()

        if prior_ids:
            await conn.execute(
                "UPDATE discoveries SET superseded = 1 WHERE scope = ? AND superseded = 0",
                (scope,),
            )

        seq = await storage.next_sequence()
        await conn.execute(
            "INSERT INTO discoveries "
            "(id, scope, summary, file_hash, agent, confidence, created_at, superseded, sequence) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)",
            (discovery_id, scope, summary, file_hash, agent, confidence, ts, seq),
        )
        await conn.commit()

    log_entry = {
        "type": "discovery_shared",
        "id": discovery_id, "scope": scope, "summary": summary,
        "file_hash": file_hash, "agent": agent, "confidence": confidence,
        "superseded_ids": prior_ids, "sequence": seq, "ts": ts,
    }
    await storage.append_log(log_entry)

    for pid in prior_ids:
        await ws_broadcast.publish("discovery_superseded", {"id": pid, "scope": scope})

    await ws_broadcast.publish("discovery_shared", {
        "id": discovery_id, "scope": scope, "summary": summary,
        "file_hash": file_hash, "agent": agent, "confidence": confidence,
        "created_at": ts, "superseded": False,
    })
    return {"status": "shared", "id": discovery_id}


# ----------------------------------------------------------------------
# INTENT — TTL lease with auto-GC and prefix-aware overlap
# ----------------------------------------------------------------------

async def claim_intent(
    scope: str,
    action: str,
    agent: str,
    ttl_minutes: int = DEFAULT_INTENT_TTL_MINUTES,
) -> dict[str, Any]:
    """Claim a working lease on a scope.

    Returns:
      {"status": "claimed", "id": str, "expires_at": str}      on success
      {"status": "conflict", "code": 403, ...}                  agent not registered
      {"status": "conflict", "code": 423, "existing_lease":..}  on overlap
      {"status": "conflict", "code": 429, "active_count":..}    if agent at cap
    """
    mode = await get_participant_mode(agent)
    if mode is None:
        return {
            "status": "conflict",
            "code": 403,
            "detail": f"Agent '{agent}' is not registered.",
            "next_step": "Call register(task='...') once at session start, then retry claim_intent.",
        }

    # GC first so callers see an honest picture.
    await gc_expired_intents()

    async with storage.transaction() as conn:
        # Per-agent cap.
        cur = await conn.execute(
            "SELECT COUNT(*) AS n FROM intents WHERE agent = ? AND status = 'active'",
            (agent,),
        )
        row = await cur.fetchone()
        await cur.close()
        if row["n"] >= MAX_ACTIVE_INTENTS_PER_AGENT:
            return {
                "status": "conflict",
                "code": 429,
                "active_count": row["n"],
                "max": MAX_ACTIVE_INTENTS_PER_AGENT,
                "next_step": f"You have {row['n']} active intents (max {MAX_ACTIVE_INTENTS_PER_AGENT}). "
                             f"Call release_intent(intent_id='...') on a completed scope first, "
                             f"then retry claim_intent.",
            }

        # Overlap check.
        cur = await conn.execute(
            "SELECT id, scope, agent, action, expires_at FROM intents WHERE status = 'active'",
            (),
        )
        active = await cur.fetchall()
        await cur.close()

        for lease in active:
            if lease["agent"] == agent and lease["scope"] == scope:
                # Same agent re-claiming exact scope — refresh TTL.
                new_expiry = (
                    datetime.now(timezone.utc) + timedelta(minutes=ttl_minutes)
                ).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                await conn.execute(
                    "UPDATE intents SET expires_at = ? WHERE id = ?",
                    (new_expiry, lease["id"]),
                )
                await conn.commit()
                await ws_broadcast.publish("intent_refreshed", {
                    "id": lease["id"], "scope": scope, "agent": agent,
                    "expires_at": new_expiry,
                })
                return {
                    "status": "claimed",
                    "id": lease["id"],
                    "expires_at": new_expiry,
                    "refreshed": True,
                }
            if _scopes_overlap(lease["scope"], scope):
                return {
                    "status": "conflict",
                    "code": 423,
                    "existing_lease": {
                        "id": lease["id"],
                        "scope": lease["scope"],
                        "agent": lease["agent"],
                        "action": lease["action"],
                        "expires_at": lease["expires_at"],
                    },
                    "next_step": f"Scope '{scope}' is locked by agent '{lease['agent']}' "
                                 f"until {lease['expires_at']}. "
                                 f"Wait for the lease to expire or ask '{lease['agent']}' "
                                 f"to call release_intent('{lease['id']}') when done.",
                }

        # Clear to claim.
        intent_id = _gen_id("int")
        ts = now_iso()
        expires_at = (
            datetime.now(timezone.utc) + timedelta(minutes=ttl_minutes)
        ).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        seq = await storage.next_sequence()
        await conn.execute(
            "INSERT INTO intents (id, scope, action, agent, mode, created_at, expires_at, status, sequence) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, 'active', ?)",
            (intent_id, scope, action, agent, mode, ts, expires_at, seq),
        )
        await conn.commit()

    await storage.append_log({
        "type": "intent_claimed",
        "id": intent_id, "scope": scope, "action": action,
        "agent": agent, "mode": mode, "expires_at": expires_at,
        "sequence": seq, "ts": ts,
    })
    await ws_broadcast.publish("intent_claimed", {
        "id": intent_id, "scope": scope, "action": action,
        "agent": agent, "created_at": ts, "expires_at": expires_at,
        "status": "active",
    })
    return {"status": "claimed", "id": intent_id, "expires_at": expires_at}


async def release_intent(intent_id: str, agent: str) -> dict[str, Any]:
    async with storage.transaction() as conn:
        cur = await conn.execute(
            "SELECT id, agent, scope, status FROM intents WHERE id = ?",
            (intent_id,),
        )
        row = await cur.fetchone()
        await cur.close()
        if row is None:
            return {
                "status": "conflict", "code": 404,
                "next_step": f"Intent '{intent_id}' does not exist. "
                             f"Call read_state() to see your active intents.",
            }
        if row["agent"] != agent:
            return {
                "status": "conflict", "code": 403,
                "next_step": f"Only the agent that claimed this intent can release it. "
                             f"This intent belongs to '{row['agent']}'.",
            }
        if row["status"] != "active":
            return {"status": "noop"}
        await conn.execute(
            "UPDATE intents SET status = 'released' WHERE id = ?",
            (intent_id,),
        )
        await conn.commit()
        scope = row["scope"]

    ts = now_iso()
    await storage.append_log({
        "type": "intent_released", "id": intent_id, "agent": agent, "ts": ts,
    })
    await ws_broadcast.publish("intent_released", {"id": intent_id, "scope": scope})
    return {"status": "released"}


async def gc_expired_intents() -> int:
    """Mark expired intents as expired. Returns count expired.

    Called on every read_state. Cheap because of the partial index.
    """
    now = now_iso()
    async with storage.transaction() as conn:
        cur = await conn.execute(
            "SELECT id, scope, agent FROM intents "
            "WHERE status = 'active' AND expires_at < ?",
            (now,),
        )
        expired = await cur.fetchall()
        await cur.close()
        if not expired:
            return 0
        await conn.execute(
            "UPDATE intents SET status = 'expired' "
            "WHERE status = 'active' AND expires_at < ?",
            (now,),
        )
        await conn.commit()

    for row in expired:
        await storage.append_log({
            "type": "intent_expired",
            "id": row["id"], "scope": row["scope"], "agent": row["agent"],
            "ts": now,
        })
        await ws_broadcast.publish("intent_expired", {
            "id": row["id"], "scope": row["scope"], "agent": row["agent"],
        })
    return len(expired)


# ----------------------------------------------------------------------
# QUESTION — append + status transitions
# ----------------------------------------------------------------------

async def raise_question(
    scope: str,
    asks: str,
    asker_agent: str,
    target: str = "human",
    blocking: bool = True,
) -> dict[str, Any]:
    # raise_question is intentionally exempt from scope permission checks —
    # it is a communication primitive, not a state mutation.
    qid = _gen_id("q")
    ts = now_iso()
    async with storage.transaction() as conn:
        seq = await storage.next_sequence()
        await conn.execute(
            "INSERT INTO questions "
            "(id, scope, asks, asker_agent, target, blocking, status, created_at, sequence) "
            "VALUES (?, ?, ?, ?, ?, ?, 'open', ?, ?)",
            (qid, scope, asks, asker_agent, target, 1 if blocking else 0, ts, seq),
        )
        await conn.commit()

    await storage.append_log({
        "type": "question_raised",
        "id": qid, "scope": scope, "asks": asks,
        "asker_agent": asker_agent, "target": target, "blocking": blocking,
        "sequence": seq, "ts": ts,
    })
    await ws_broadcast.publish("question_raised", {
        "id": qid, "scope": scope, "asks": asks,
        "asker_agent": asker_agent, "target": target,
        "blocking": blocking, "status": "open",
        "created_at": ts,
    })
    await _refresh_inbox()
    return {"status": "raised", "id": qid, "code": 202 if not blocking else None}


async def answer_question(
    question_id: str, answer: str, resolved_by: str
) -> dict[str, Any]:
    ts = now_iso()
    async with storage.transaction() as conn:
        cur = await conn.execute(
            "SELECT id, status FROM questions WHERE id = ?", (question_id,)
        )
        row = await cur.fetchone()
        await cur.close()
        if row is None:
            return {
                "status": "conflict", "code": 404,
                "next_step": f"Question '{question_id}' does not exist. "
                             f"Call read_state() to see open questions.",
            }
        if row["status"] in ("resolved", "deferred"):
            return {"status": "noop", "current_status": row["status"]}

        await conn.execute(
            "UPDATE questions SET answer = ?, resolved_by = ?, "
            "status = 'answered', resolved_at = ? WHERE id = ?",
            (answer, resolved_by, ts, question_id),
        )
        await conn.commit()

    await storage.append_log({
        "type": "question_answered",
        "id": question_id, "answer": answer, "resolved_by": resolved_by, "ts": ts,
    })
    await ws_broadcast.publish("question_answered", {
        "id": question_id, "answer": answer,
        "resolved_by": resolved_by, "resolved_at": ts,
    })
    await _refresh_inbox()
    return {"status": "answered"}


async def resolve_question(
    question_id: str, resolution: str, resolved_by: str
) -> dict[str, Any]:
    """Final close. Distinct from `answer` — answer is "here's the info";
    resolve is "this question is now closed and the system should reflect that."
    """
    ts = now_iso()
    async with storage.transaction() as conn:
        cur = await conn.execute(
            "SELECT id FROM questions WHERE id = ?", (question_id,)
        )
        row = await cur.fetchone()
        await cur.close()
        if row is None:
            return {
                "status": "conflict", "code": 404,
                "next_step": f"Question '{question_id}' does not exist. "
                             f"Call read_state() to see open questions.",
            }
        await conn.execute(
            "UPDATE questions SET status = 'resolved', "
            "answer = COALESCE(answer, ?), resolved_by = ?, resolved_at = ? "
            "WHERE id = ?",
            (resolution, resolved_by, ts, question_id),
        )
        await conn.commit()

    await storage.append_log({
        "type": "question_resolved",
        "id": question_id, "resolution": resolution,
        "resolved_by": resolved_by, "ts": ts,
    })
    await ws_broadcast.publish("question_resolved", {
        "id": question_id, "resolution": resolution,
        "resolved_by": resolved_by, "resolved_at": ts,
    })
    await _refresh_inbox()
    return {"status": "resolved"}


# ----------------------------------------------------------------------
# Read path
# ----------------------------------------------------------------------

async def get_state(
    scope_filter: str | None = None,
    since_id: int | None = None,
) -> dict[str, Any]:
    """Snapshot or delta, depending on whether since_id is provided.

    Full snapshot (since_id is None or 0):
      Returns all active records.  current_id is included so callers can
      store it and pass it as since_id on the next call.

    Delta (since_id > 0):
      Returns only records whose sequence counter exceeds since_id.
      Decisions use their own sequence column (monotonic, FWW-assigned).
      Discoveries / Intents / Questions use the sequence column added by
      _apply_migrations (DEFAULT 0; new rows get a global counter value).
      Rows with sequence = 0 (pre-migration legacy rows) are returned in
      full snapshots only — they are intentionally excluded from deltas.

    Side-effect: GC expired intents.
    """
    await gc_expired_intents()

    conn = storage.db()

    # Current global sequence value — included in every response.
    cur = await conn.execute("SELECT value FROM sequence_counter WHERE id = 1")
    row = await cur.fetchone()
    await cur.close()
    current_id: int = row["value"] if row else 0

    # Determine whether we are doing a delta read.
    delta = since_id is not None and since_id > 0

    # Build scope WHERE clause.  We wrap scope conditions in parentheses so
    # that AND for status/superseded binds correctly (see comment in original).
    scope_clause = ""
    scope_params: tuple = ()
    if scope_filter:
        scope_clause = "(scope = ? OR scope LIKE ?)"
        scope_params = (scope_filter, f"{scope_filter}/%")

    # ------------------------------------------------------------------
    # Helper: assemble params list for queries that may have both scope
    # and delta filters.
    # ------------------------------------------------------------------
    def _build(base_where: str, extra_clauses: list[str], extra_params: tuple) -> tuple[str, tuple]:
        """Return (where_string, params_tuple).

        base_where   — already-combined scope clause (may be empty)
        extra_clauses — additional AND conditions (do not include AND keyword)
        extra_params  — params matching the extra_clauses placeholders
        """
        all_clauses: list[str] = []
        if base_where:
            all_clauses.append(base_where)
        all_clauses.extend(extra_clauses)

        where = ("WHERE " + " AND ".join(all_clauses)) if all_clauses else ""
        params = scope_params + extra_params
        return where, params

    # ------------------------------------------------------------------
    # Decisions
    # ------------------------------------------------------------------
    if delta:
        d_where, d_params = _build(scope_clause, ["sequence > ?"], (since_id,))
    else:
        d_where, d_params = _build(scope_clause, [], ())

    cur = await conn.execute(
        f"SELECT * FROM decisions {d_where} ORDER BY sequence ASC", d_params
    )
    decisions = [dict(r) for r in await cur.fetchall()]
    await cur.close()

    # ------------------------------------------------------------------
    # Discoveries
    # ------------------------------------------------------------------
    if delta:
        disc_where, disc_params = _build(
            scope_clause, ["superseded = 0", "sequence > ?"], (since_id,)
        )
    else:
        disc_where, disc_params = _build(
            scope_clause,
            ["superseded = 0"] if not scope_clause else ["superseded = 0"],
            (),
        )
        if not scope_clause:
            disc_where = "WHERE superseded = 0"
            disc_params = ()

    cur = await conn.execute(
        f"SELECT * FROM discoveries {disc_where} ORDER BY created_at DESC", disc_params
    )
    discoveries = [dict(r) for r in await cur.fetchall()]
    for d in discoveries:
        d["superseded"] = bool(d["superseded"])
        current_hash = _current_file_hash(d["scope"])
        recorded_hash = _normalize_sha256(d.get("file_hash"))
        d["stale"] = bool(current_hash and recorded_hash and current_hash != recorded_hash)
    await cur.close()

    # ------------------------------------------------------------------
    # Intents
    # ------------------------------------------------------------------
    if delta:
        i_where, i_params = _build(
            scope_clause, ["status = 'active'", "sequence > ?"], (since_id,)
        )
    else:
        i_where, i_params = _build(scope_clause, ["status = 'active'"], ())
        if not scope_clause:
            i_where = "WHERE status = 'active'"
            i_params = ()

    cur = await conn.execute(
        f"SELECT * FROM intents {i_where} ORDER BY created_at DESC", i_params
    )
    intents = [dict(r) for r in await cur.fetchall()]
    await cur.close()

    # ------------------------------------------------------------------
    # Questions
    # ------------------------------------------------------------------
    if delta:
        q_where, q_params = _build(
            scope_clause,
            ["status IN ('open', 'answered')", "sequence > ?"],
            (since_id,),
        )
    else:
        q_where, q_params = _build(
            scope_clause, ["status IN ('open', 'answered')"], ()
        )
        if not scope_clause:
            q_where = "WHERE status IN ('open', 'answered')"
            q_params = ()

    cur = await conn.execute(
        f"SELECT * FROM questions {q_where} ORDER BY created_at DESC", q_params
    )
    questions = [dict(r) for r in await cur.fetchall()]
    # Coerce SQLite 0/1 to bool for clients.
    for q in questions:
        q["blocking"] = bool(q["blocking"])
    await cur.close()

    # Include participants in full snapshots so the dashboard shows agents
    # that registered before this WebSocket connection was opened.
    participants: list[dict] = []
    if not delta:
        import json as _json
        cur = await conn.execute(
            "SELECT agent_id, type, task, scope, role_tag, mode, "
            "registered_at, last_seen, status FROM participants ORDER BY registered_at ASC"
        )
        rows = await cur.fetchall()
        await cur.close()
        for r in rows:
            p = dict(r)
            try:
                p["scope"] = _json.loads(p["scope"]) if isinstance(p["scope"], str) else p["scope"]
            except Exception:
                p["scope"] = []
            participants.append(p)

    result: dict[str, Any] = {
        "decisions": decisions,
        "discoveries": discoveries,
        "intents": intents,
        "questions": questions,
        "current_id": current_id,
    }
    if not delta:
        result["participants"] = participants
    if delta:
        result["since_id"] = since_id

    return result


# ----------------------------------------------------------------------
# Internal: keep inbox.md in sync with the question table
# ----------------------------------------------------------------------

async def _refresh_inbox() -> None:
    state = await get_state()
    await storage.regenerate_inbox(state["questions"])
