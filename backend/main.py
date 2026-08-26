"""Coord daemon — FastAPI entry point.

Run with:
    cd backend && python -m uvicorn main:app --host 127.0.0.1 --port 49152

Or simply:
    cd backend && python main.py

Endpoints:
    GET  /healthz                         → liveness
    POST /api/register                    → register agent/human (NEW)
    GET  /api/state                       → full snapshot or delta (since_id)
    POST /api/intents                     → claim_intent
    DELETE /api/intents/{id}              → release_intent
    POST /api/decisions                   → commit_decision
    POST /api/discoveries                 → share_discovery
    POST /api/questions                   → raise_question
    POST /api/questions/{id}/answer       → answer_question
    POST /api/questions/{id}/resolve      → resolve_question
    WS   /ws                              → live event stream

Agent identity:
    All mutating endpoints require the X-Coord-Agent-Id header.
    The MCP shim sets it from $COORD_AGENT_ID.
    Agents must also call POST /api/register before any mutation — the scope
    checks in state_engine return 403 for unregistered agents.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Header, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from . import config, state_engine, storage, ws_broadcast
from .models import (
    AnswerQuestionRequest,
    ClaimIntentRequest,
    CommitDecisionRequest,
    Limits,
    ParticipantRegistration,
    RaiseQuestionRequest,
    ResolveQuestionRequest,
    ShareDiscoveryRequest,
    now_iso,
)

logger = logging.getLogger(__name__)


# ----------------------------------------------------------------------
# In-memory rate limiting state
# agent_id → list of call timestamps (epoch float, within last 60 s)
# ----------------------------------------------------------------------
rate_tracker: dict[str, list[float]] = {}

# agent_id → Limits (populated on register, reloaded from DB on miss)
_limits_cache: dict[str, Limits] = {}

DEFAULT_LIMITS = Limits()   # max_calls_per_min=20, max_state_size_kb=100, alert=0.8

# Synthetic test IDs like bot-0, bot-9 — not real tool identities (cursor, claude-code, ...).
_SYNTHETIC_BOT_ID = re.compile(r"^bot[-_]?\d+$", re.IGNORECASE)


def _is_synthetic_bot_agent_id(agent_id: str) -> bool:
    return bool(_SYNTHETIC_BOT_ID.match(agent_id.strip()))


async def _get_limits(agent_id: str) -> Limits:
    """Return cached Limits for agent, falling back to DB then defaults."""
    if agent_id in _limits_cache:
        return _limits_cache[agent_id]
    conn = storage.db()
    cur = await conn.execute(
        "SELECT limits FROM participants WHERE agent_id = ?", (agent_id,)
    )
    row = await cur.fetchone()
    await cur.close()
    if row:
        try:
            raw = json.loads(row["limits"])
            lim = Limits(**raw)
            _limits_cache[agent_id] = lim
            return lim
        except Exception:
            pass
    return DEFAULT_LIMITS


def _check_rate(agent_id: str, max_calls: int) -> bool:
    """Return True if agent is within rate limit, False if exceeded.

    Prunes stale timestamps (>60 s old) on every call.
    """
    now = time.monotonic()
    calls = rate_tracker.get(agent_id, [])
    calls = [t for t in calls if now - t < 60.0]   # prune old
    rate_tracker[agent_id] = calls

    if len(calls) >= max_calls:
        return False
    calls.append(now)
    return True


# ----------------------------------------------------------------------
# Lifespan: bootstrap and teardown
# ----------------------------------------------------------------------

async def _agent_lifecycle_task() -> None:
    """Mark agents idle/offline from heartbeat age; broadcast status changes."""
    while True:
        await asyncio.sleep(60)
        try:
            now = datetime.now(timezone.utc)
            idle_cutoff = (now - timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            offline_cutoff = (now - timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

            conn = storage.db()
            cur = await conn.execute(
                "SELECT agent_id FROM participants WHERE status = 'online' "
                "AND (last_seen IS NULL OR last_seen < ?)",
                (idle_cutoff,),
            )
            going_idle = [r["agent_id"] for r in await cur.fetchall()]
            await cur.close()

            cur = await conn.execute(
                "SELECT agent_id FROM participants WHERE status = 'idle' "
                "AND (last_seen IS NULL OR last_seen < ?)",
                (offline_cutoff,),
            )
            going_offline = [r["agent_id"] for r in await cur.fetchall()]
            await cur.close()

            async with storage.db_lock:
                conn = storage.db()
                if going_idle:
                    ph = ",".join("?" * len(going_idle))
                    await conn.execute(
                        f"UPDATE participants SET status = 'idle' WHERE agent_id IN ({ph})",
                        going_idle,
                    )
                if going_offline:
                    ph = ",".join("?" * len(going_offline))
                    await conn.execute(
                        f"UPDATE participants SET status = 'offline' WHERE agent_id IN ({ph})",
                        going_offline,
                    )
                if going_idle or going_offline:
                    await conn.commit()

            for aid in going_idle:
                await ws_broadcast.publish(
                    "agent_status_changed", {"agent_id": aid, "status": "idle"}
                )
            for aid in going_offline:
                await ws_broadcast.publish(
                    "agent_status_changed", {"agent_id": aid, "status": "offline"}
                )
        except Exception as e:
            logger.warning("lifecycle task error: %s", e)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await storage.init_storage()
    # Warm up the inbox file so editors auto-detect it.
    state = await state_engine.get_state()
    await storage.regenerate_inbox(state["questions"])
    asyncio.create_task(_agent_lifecycle_task())
    yield
    await storage.close_storage()


app = FastAPI(
    title="Coord — Agent Coordination Daemon",
    version="0.2.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[config.FRONTEND_ORIGIN],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------

def require_agent(x_coord_agent_id: str | None) -> str:
    if not x_coord_agent_id:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "Missing agent identity header.",
                "next_step": "Add 'X-Coord-Agent-Id: <your-agent-id>' to every request. "
                             "The MCP shim sets this automatically from $COORD_AGENT_ID.",
            },
        )
    return x_coord_agent_id


async def require_registered_agent(x_coord_agent_id: str | None) -> str:
    """Require that the agent header is present AND registered.

    Returns agent_id on success.  Raises 400 if missing or unregistered.
    """
    agent = require_agent(x_coord_agent_id)
    conn = storage.db()
    cur = await conn.execute(
        "SELECT agent_id FROM participants WHERE agent_id = ?", (agent,)
    )
    row = await cur.fetchone()
    await cur.close()
    if row is None:
        raise HTTPException(
            status_code=400,
            detail={
                "error": f"Agent '{agent}' is not registered.",
                "next_step": "Call register(task='describe your task here') before any other tool. "
                             "This is required once per session.",
            },
        )
    return agent


async def require_registered_with_rate_check(x_coord_agent_id: str | None) -> str:
    """require_registered_agent + rate limiting + heartbeat (last_seen)."""
    agent = await require_registered_agent(x_coord_agent_id)
    limits = await _get_limits(agent)
    if not _check_rate(agent, limits.max_calls_per_min):
        raise HTTPException(
            status_code=429,
            detail={
                "error": f"Rate limit exceeded for agent '{agent}'.",
                "limit": limits.max_calls_per_min,
                "next_step": f"Wait up to 60 seconds and retry. "
                             f"To raise the limit, pass limits={{max_calls_per_min: N}} in register().",
            },
        )
    ts = now_iso()
    async with storage.transaction() as conn:
        await conn.execute(
            "UPDATE participants SET last_seen = ?, status = 'online' WHERE agent_id = ?",
            (ts, agent),
        )
        await conn.commit()
    return agent


def _result_to_response(result: dict) -> JSONResponse:
    """Translate state_engine results into HTTP status codes.

    Conflicts map to their numeric code so HTTP clients (including curl)
    see the right status without parsing JSON.
    """
    if result.get("status") == "conflict":
        code = int(result.get("code", 409))
        return JSONResponse(status_code=code, content=result)
    return JSONResponse(status_code=200, content=result)


# ----------------------------------------------------------------------
# Health
# ----------------------------------------------------------------------

@app.get("/healthz")
async def healthz():
    return {
        "status": "ok",
        "server_time": now_iso(),
        "ws_clients": ws_broadcast.manager.count,
    }


# ----------------------------------------------------------------------
# Registration (NEW)
# ----------------------------------------------------------------------

@app.post("/api/register")
async def register_participant(body: ParticipantRegistration):
    """Announce agent existence to the daemon.

    This is a lightweight "I exist" call — no scope conflict checking happens
    here.  Scope ownership is established dynamically when the agent calls
    claim_intent.  That is the real coordination gate.

    Success  → 200  {"status": "registered", ...}
    Bad data → 400  (Pydantic validation)

    Call this once at session start.  Re-registering the same agent_id
    overwrites the prior registration (safe for session restarts).
    Scope list is optional — omit it to declare no static scopes and rely
    entirely on claim_intent for coordination.
    """
    if _is_synthetic_bot_agent_id(body.agent_id):
        raise HTTPException(
            status_code=400,
            detail={
                "error": f"Agent ID '{body.agent_id}' is reserved for synthetic tests.",
                "next_step": "Use your real tool identity as the agent ID. "
                             "Examples: 'cursor', 'claude-code', 'aider', 'human', 'antigravity'.",
            },
        )

    reg_ts = now_iso()
    # Upsert participant (overwrite on re-registration).
    async with storage.transaction() as db_conn:
        await db_conn.execute(
            """
            INSERT INTO participants
              (agent_id, type, task, scope, role_tag, mode, limits, registered_at, last_seen, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'online')
            ON CONFLICT(agent_id) DO UPDATE SET
              type         = excluded.type,
              task         = excluded.task,
              scope        = excluded.scope,
              role_tag     = excluded.role_tag,
              mode         = excluded.mode,
              limits       = excluded.limits,
              registered_at = excluded.registered_at,
              last_seen    = excluded.last_seen,
              status       = 'online'
            """,
            (
                body.agent_id,
                body.type,
                body.task,
                json.dumps(body.scope),
                body.role_tag,
                body.mode,
                json.dumps(body.limits.model_dump()),
                reg_ts,
                reg_ts,
            ),
        )
        await db_conn.commit()

    # Cache limits so rate checking is zero-latency after registration.
    _limits_cache[body.agent_id] = body.limits

    await storage.append_log({
        "type": "participant_registered",
        "agent_id": body.agent_id, "type": body.type,
        "task": body.task, "scope": body.scope,
        "role_tag": body.role_tag, "mode": body.mode,
        "ts": now_iso(),
    })

    await ws_broadcast.publish(
        "agent_registered",
        {
            "agent_id": body.agent_id,
            "type": body.type,
            "task": body.task,
            "scope": body.scope,
            "role_tag": body.role_tag,
            "mode": body.mode,
            "registered_at": reg_ts,
            "last_seen": reg_ts,
            "status": "online",
        },
    )

    return JSONResponse(
        status_code=200,
        content={
            "status": "registered",
            "agent_id": body.agent_id,
            "scope": body.scope,
            "mode": body.mode,
        },
    )


@app.delete("/api/participants/{agent_id}")
async def delete_participant(
    agent_id: str,
    x_coord_agent_id: str | None = Header(default=None),
):
    """Remove a participant. Only that agent or ``human`` may call."""
    caller = require_agent(x_coord_agent_id)
    if caller != agent_id and caller != "human":
        raise HTTPException(
            status_code=403,
            detail={
                "error": f"Agent '{caller}' cannot remove participant '{agent_id}'.",
                "next_step": "Only the agent itself or the 'human' identity can remove a participant. "
                             "Re-send with X-Coord-Agent-Id matching the agent being removed.",
            },
        )
    async with storage.transaction() as conn:
        await conn.execute("DELETE FROM participants WHERE agent_id = ?", (agent_id,))
        await conn.commit()
    _limits_cache.pop(agent_id, None)
    rate_tracker.pop(agent_id, None)
    await storage.append_log(
        {
            "type": "participant_removed",
            "agent_id": agent_id,
            "by": caller,
            "ts": now_iso(),
        }
    )
    await ws_broadcast.publish("agent_unregistered", {"agent_id": agent_id})
    return {"status": "removed", "agent_id": agent_id}


@app.post("/api/participants/purge")
async def purge_idle_participants(
    x_coord_agent_id: str | None = Header(default=None),
):
    """Drop participants with no heartbeat in the last 2 hours. Human only."""
    caller = require_agent(x_coord_agent_id)
    if caller != "human":
        raise HTTPException(
            status_code=403,
            detail={
                "error": "Only the 'human' identity can purge participants.",
                "next_step": "Re-send with X-Coord-Agent-Id: human header.",
            },
        )
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=2)).strftime(
        "%Y-%m-%dT%H:%M:%S.%fZ"
    )
    stale: list[str] = []
    async with storage.transaction() as conn:
        cur = await conn.execute(
            "SELECT agent_id FROM participants WHERE last_seen IS NULL OR last_seen < ?",
            (cutoff,),
        )
        stale = [row["agent_id"] for row in await cur.fetchall()]
        await cur.close()
        if stale:
            ph = ",".join("?" * len(stale))
            await conn.execute(
                f"DELETE FROM participants WHERE agent_id IN ({ph})",
                stale,
            )
        await conn.commit()
    for aid in stale:
        _limits_cache.pop(aid, None)
        rate_tracker.pop(aid, None)
        await ws_broadcast.publish("agent_unregistered", {"agent_id": aid})
    return {"status": "purged", "removed": stale}


@app.post("/api/participants/remove-synthetic-bots")
async def remove_synthetic_bot_participants(
    x_coord_agent_id: str | None = Header(default=None),
):
    """Remove participants whose ids match synthetic test agents (``bot-9``, ``bot_0``, ...).

    Human only. Does not remove real identities (cursor, claude-code, antigravity, …).
    """
    caller = require_agent(x_coord_agent_id)
    if caller != "human":
        raise HTTPException(
            status_code=403,
            detail={
                "error": "Only the 'human' identity can remove synthetic bot participants.",
                "next_step": "Re-send with X-Coord-Agent-Id: human header.",
            },
        )

    async with storage.transaction() as conn:
        cur = await conn.execute("SELECT agent_id FROM participants", ())
        rows = await cur.fetchall()
        await cur.close()
        to_remove = [r["agent_id"] for r in rows if _is_synthetic_bot_agent_id(r["agent_id"])]

        if to_remove:
            ph = ",".join("?" * len(to_remove))
            await conn.execute(
                f"DELETE FROM participants WHERE agent_id IN ({ph})",
                to_remove,
            )
        await conn.commit()

    for aid in to_remove:
        _limits_cache.pop(aid, None)
        rate_tracker.pop(aid, None)
        await storage.append_log(
            {
                "type": "participant_removed",
                "agent_id": aid,
                "by": caller,
                "reason": "synthetic_bot_cleanup",
                "ts": now_iso(),
            }
        )
        await ws_broadcast.publish("agent_unregistered", {"agent_id": aid})

    return {"status": "removed_synthetic_bots", "removed": to_remove}


# ----------------------------------------------------------------------
# State snapshot (updated: since_id delta + payload capping)
# ----------------------------------------------------------------------

@app.get("/api/state")
async def get_state(
    scope: Optional[str] = None,
    since_id: Optional[int] = None,
    x_coord_agent_id: Optional[str] = Header(default=None),
):
    """Return a full snapshot or a delta since a given sequence id.

    Query params:
      scope     — narrow to a single scope prefix (optional)
      since_id  — return only records with sequence > since_id (optional)
                  Omit or pass 0 for a full snapshot.

    Payload capping (per registered agent's limits):
      At alert_threshold  → response includes {"_payload_warning": true}
      Above max_state_size_kb → response truncated per type + non-blocking
                                Question raised to human.
    """
    state = await state_engine.get_state(scope_filter=scope, since_id=since_id)
    payload = {**state, "server_time": now_iso()}

    # Payload capping — only enforced when agent is identified and registered.
    if x_coord_agent_id:
        limits = await _get_limits(x_coord_agent_id)
        raw_size = len(json.dumps(payload).encode())
        cap_bytes = limits.max_state_size_kb * 1024
        warn_bytes = int(cap_bytes * limits.alert_threshold)

        if raw_size >= cap_bytes:
            # Truncate each list to the 20 most-recent items and add a warning.
            TRUNC = 20
            payload["decisions"]   = payload["decisions"][-TRUNC:]
            payload["discoveries"] = payload["discoveries"][:TRUNC]
            payload["intents"]     = payload["intents"][:TRUNC]
            payload["questions"]   = payload["questions"][:TRUNC]
            payload["_payload_truncated"] = True
            payload["_payload_size_kb"]   = round(raw_size / 1024, 1)

            # Non-blocking question to human so they know pruning happened.
            await state_engine.raise_question(
                scope="virt://coord/state",
                asks=(
                    f"State payload for agent '{x_coord_agent_id}' exceeded "
                    f"{limits.max_state_size_kb} KB ({round(raw_size/1024, 1)} KB). "
                    "Response was truncated. Human intervention may be needed to "
                    "prune stale decisions/discoveries."
                ),
                asker_agent="coord_daemon",
                target="human",
                blocking=False,
            )
        elif raw_size >= warn_bytes:
            payload["_payload_warning"] = True
            payload["_payload_size_kb"] = round(raw_size / 1024, 1)
            payload["_payload_limit_kb"] = limits.max_state_size_kb

    return payload


# ----------------------------------------------------------------------
# Intents
# ----------------------------------------------------------------------

@app.post("/api/intents")
async def post_intent(
    body: ClaimIntentRequest,
    x_coord_agent_id: str | None = Header(default=None),
):
    agent = await require_registered_with_rate_check(x_coord_agent_id)
    result = await state_engine.claim_intent(
        scope=body.scope,
        action=body.action,
        agent=agent,
        ttl_minutes=body.ttl_minutes,
    )
    return _result_to_response(result)


@app.delete("/api/intents/{intent_id}")
async def delete_intent(
    intent_id: str,
    x_coord_agent_id: str | None = Header(default=None),
):
    agent = await require_registered_with_rate_check(x_coord_agent_id)
    result = await state_engine.release_intent(intent_id, agent)
    return _result_to_response(result)


# ----------------------------------------------------------------------
# Decisions
# ----------------------------------------------------------------------

@app.post("/api/decisions")
async def post_decision(
    body: CommitDecisionRequest,
    x_coord_agent_id: str | None = Header(default=None),
):
    agent = await require_registered_with_rate_check(x_coord_agent_id)
    result = await state_engine.commit_decision(
        scope=body.scope,
        key=body.key,
        value=body.value,
        agent=agent,
        anchor=body.anchor,
        rationale=body.rationale,
    )
    return _result_to_response(result)


# ----------------------------------------------------------------------
# Discoveries
# ----------------------------------------------------------------------

@app.post("/api/discoveries")
async def post_discovery(
    body: ShareDiscoveryRequest,
    x_coord_agent_id: str | None = Header(default=None),
):
    agent = await require_registered_with_rate_check(x_coord_agent_id)
    result = await state_engine.share_discovery(
        scope=body.scope,
        summary=body.summary,
        agent=agent,
        file_hash=body.file_hash,
        confidence=body.confidence,
    )
    return _result_to_response(result)


# ----------------------------------------------------------------------
# Questions
# ----------------------------------------------------------------------

@app.post("/api/questions")
async def post_question(
    body: RaiseQuestionRequest,
    x_coord_agent_id: str | None = Header(default=None),
):
    # Questions exempt from registration requirement — humans and agents
    # without full scope may still raise questions.
    agent = require_agent(x_coord_agent_id)
    result = await state_engine.raise_question(
        scope=body.scope,
        asks=body.asks,
        asker_agent=agent,
        target=body.target,
        blocking=body.blocking,
    )
    return _result_to_response(result)


@app.post("/api/questions/{question_id}/answer")
async def post_answer(
    question_id: str,
    body: AnswerQuestionRequest,
    x_coord_agent_id: str | None = Header(default=None),
):
    # Humans answer too — agent header required but registration not enforced.
    agent = require_agent(x_coord_agent_id)
    result = await state_engine.answer_question(
        question_id=question_id, answer=body.answer, resolved_by=agent
    )
    return _result_to_response(result)


@app.post("/api/questions/{question_id}/resolve")
async def post_resolve(
    question_id: str,
    body: ResolveQuestionRequest,
    x_coord_agent_id: str | None = Header(default=None),
):
    agent = require_agent(x_coord_agent_id)
    result = await state_engine.resolve_question(
        question_id=question_id, resolution=body.resolution, resolved_by=agent
    )
    return _result_to_response(result)


# ----------------------------------------------------------------------
# WebSocket
# ----------------------------------------------------------------------

@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await ws_broadcast.manager.connect(websocket)
    try:
        # Initial snapshot — clients render immediately, no flicker.
        snapshot = await state_engine.get_state()
        await ws_broadcast.manager.send_to(
            websocket, "state_snapshot",
            {**snapshot, "server_time": now_iso()},
        )
        # Keep the socket alive. We don't expect inbound messages, but FastAPI
        # closes the socket if we don't await something.
        while True:
            # The dashboard may send pings; we ignore content.
            try:
                await websocket.receive_text()
            except WebSocketDisconnect:
                raise
            except Exception:
                # Stay alive on parse errors.
                await asyncio.sleep(0.1)
    except WebSocketDisconnect:
        pass
    finally:
        await ws_broadcast.manager.disconnect(websocket)


# ----------------------------------------------------------------------
# CLI entry — `python main.py`
# ----------------------------------------------------------------------

if __name__ == "__main__":
    # Pass the app object directly (not a string path) so we don't trigger
    # a double import when launched via `python -m backend.main`.
    import uvicorn
    uvicorn.run(
        app,
        host=config.HOST,
        port=config.PORT,
        log_level="info",
    )
