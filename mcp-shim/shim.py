"""Coord MCP shim — one process per agent.

Each agent's IDE config spawns this file as a stdio MCP server. The shim:
  1. Reads its identity from $COORD_AGENT_ID (e.g., "claude-code", "cursor").
  2. Exposes the 9 Coord tools as MCP tools (register is tool #1).
  3. Proxies every call to the daemon's HTTP API at $COORD_BASE_URL.
  4. Returns the daemon's response verbatim — including conflict codes —
     so the agent sees real 400/403/409/410/423/429 semantics.

IMPORTANT — call order:
  Call `register` first, before any other tool.  The daemon enforces scope
  ownership: unregistered agents receive 400 from mutation endpoints and 403
  from scope-protected endpoints.

Why per-agent processes:
  MCP is stdio-based. A single MCP server can serve only one client at a time.
  Spawning one shim per agent gives each agent its own MCP transport, while
  the daemon remains the single source of truth. Identity is set in the
  shim's environment — the agent cannot spoof another agent's ID.

Configure in your IDE:
    {
      "mcpServers": {
        "coord": {
          "command": "python",
          "args": ["/abs/path/to/coord/mcp-shim/shim.py"],
          "env": {
            "COORD_AGENT_ID": "claude-code",
            "COORD_BASE_URL": "http://127.0.0.1:49152"
          }
        }
      }
    }
"""
from __future__ import annotations

import json
import os
from typing import Any

import httpx
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import TextContent, Tool


AGENT_ID = os.environ.get("COORD_AGENT_ID", "unknown-agent")
BASE_URL = os.environ.get("COORD_BASE_URL", "http://127.0.0.1:49152").rstrip("/")
DEFAULT_TIMEOUT = 10.0


def _headers() -> dict[str, str]:
    return {"X-Coord-Agent-Id": AGENT_ID, "Content-Type": "application/json"}


async def _request(method: str, path: str, body: dict | None = None) -> dict[str, Any]:
    """One HTTP call to the daemon. Returns parsed JSON; HTTP error codes are
    surfaced inside the JSON body (the daemon always returns JSON)."""
    url = f"{BASE_URL}{path}"
    async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as client:
        resp = await client.request(
            method=method,
            url=url,
            headers=_headers(),
            content=json.dumps(body) if body is not None else None,
        )
        try:
            return resp.json()
        except json.JSONDecodeError:
            return {
                "status": "error",
                "code": resp.status_code,
                "detail": resp.text[:500],
            }


def _ok(payload: dict) -> list[TextContent]:
    return [TextContent(type="text", text=json.dumps(payload, indent=2))]


# ----------------------------------------------------------------------
# Tool catalog  (register is FIRST — agents must call it before others)
# ----------------------------------------------------------------------

TOOLS: list[Tool] = [
    # ------------------------------------------------------------------ #1
    Tool(
        name="register",
        description=(
            "Announce this agent to the Coord daemon. Call once at session start "
            "before any other tool.\n\n"
            "This is a lightweight existence announcement — no scope conflicts are "
            "checked here. Scope ownership is established dynamically when you call "
            "claim_intent (the real coordination gate).\n\n"
            "Two-phase flow:\n"
            "  1. register(task='...')                      — announce you exist\n"
            "  2. claim_intent(scope='...', action='...')   — lock scope before working\n\n"
            "Scope is optional. Omit it to rely entirely on claim_intent for "
            "coordination (recommended for dynamic work). Provide it only if you want "
            "to pre-declare static ownership for the whole session.\n\n"
            "All mutation endpoints return 400 until register is called."
        ),
        inputSchema={
            "type": "object",
            "required": ["task"],
            "properties": {
                "task": {
                    "type": "string",
                    "description": "Human-readable description of what this agent is doing.",
                },
                "scope": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": (
                        "Optional list of URI scopes this agent statically owns. "
                        "File paths: 'src/auth/', 'src/api/middleware.ts'. "
                        "Virtual: 'virt://db/schema', 'virt://cloud/aws/s3'. "
                        "Omit to use claim_intent for dynamic scope ownership."
                    ),
                },
                "type": {
                    "type": "string",
                    "enum": ["agent", "human"],
                    "default": "agent",
                    "description": "Whether this participant is an agent or a human.",
                },
                "role_tag": {
                    "type": "string",
                    "description": "Optional label e.g. 'coder', 'reviewer', 'planner'.",
                },
                "mode": {
                    "type": "string",
                    "enum": ["exclusive", "collaborative"],
                    "default": "exclusive",
                    "description": (
                        "exclusive — scope locks via claim_intent block other agents. "
                        "collaborative — writes outside claimed scopes are logged but allowed."
                    ),
                },
                "limits": {
                    "type": "object",
                    "description": "Optional rate and payload limits.",
                    "properties": {
                        "max_calls_per_min": {
                            "type": "integer",
                            "default": 20,
                            "description": "Max daemon calls per 60 s before 429.",
                        },
                        "max_state_size_kb": {
                            "type": "integer",
                            "default": 100,
                            "description": "Max read_state response size in KB before truncation.",
                        },
                        "alert_threshold": {
                            "type": "number",
                            "default": 0.8,
                            "description": "Warn when payload reaches this fraction of the limit.",
                        },
                    },
                },
            },
        },
    ),
    # ------------------------------------------------------------------ #2
    Tool(
        name="read_state",
        description=(
            "Snapshot of all four shared stores: decisions, discoveries, "
            "intents, questions. Pass `scope_filter` to narrow results. "
            "Pass `since_id` (the current_id from a prior call) to get only "
            "records newer than that point — reduces context window growth. "
            "Call this at the start of any non-trivial task."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "scope_filter": {
                    "type": "string",
                    "description": "Narrow results to this scope prefix.",
                },
                "since_id": {
                    "type": "integer",
                    "description": (
                        "Return only records with sequence > since_id. "
                        "Use the current_id value from a previous read_state response. "
                        "Omit for a full snapshot."
                    ),
                },
            },
        },
    ),
    # ------------------------------------------------------------------ #3
    Tool(
        name="claim_intent",
        description=(
            "Claim a TTL lease on a scope before working on it. Returns 423 "
            "if another agent already holds an overlapping scope. Returns "
            "429 if you have too many active intents (release one first). "
            "Returns 403 if scope is not owned by this agent (exclusive mode)."
        ),
        inputSchema={
            "type": "object",
            "required": ["scope", "action"],
            "properties": {
                "scope": {"type": "string"},
                "action": {"type": "string"},
                "ttl_minutes": {"type": "integer", "minimum": 1, "maximum": 120},
            },
        },
    ),
    # ------------------------------------------------------------------ #4
    Tool(
        name="release_intent",
        description="Release an active intent. Only the owning agent can release.",
        inputSchema={
            "type": "object",
            "required": ["intent_id"],
            "properties": {"intent_id": {"type": "string"}},
        },
    ),
    # ------------------------------------------------------------------ #5
    Tool(
        name="commit_decision",
        description=(
            "Record a contract (First-Write-Wins). Returns 409 if a different "
            "value already exists for the same scope::key — on 409, raise a "
            "question instead of retrying. Returns 403 if scope not owned."
        ),
        inputSchema={
            "type": "object",
            "required": ["scope", "key", "value"],
            "properties": {
                "scope": {"type": "string"},
                "key": {"type": "string"},
                "value": {"type": "string"},
                "anchor": {
                    "type": "string",
                    "description": "Optional link to a prior decision or rationale document.",
                },
                "rationale": {"type": "string"},
            },
        },
    ),
    # ------------------------------------------------------------------ #6
    Tool(
        name="share_discovery",
        description=(
            "Share an observation about the codebase (Last-Write-Wins). "
            "Include file_hash so peers can detect when this discovery becomes "
            "stale. Returns 403 if scope not owned (exclusive mode)."
        ),
        inputSchema={
            "type": "object",
            "required": ["scope", "summary"],
            "properties": {
                "scope": {"type": "string"},
                "summary": {"type": "string"},
                "file_hash": {"type": "string"},
                "confidence": {
                    "type": "string",
                    "enum": ["unverified", "verified", "contradicted"],
                },
            },
        },
    ),
    # ------------------------------------------------------------------ #7
    Tool(
        name="raise_question",
        description=(
            "Escalate a blocker to the human (target='human') or another "
            "agent. Use blocking=true if you cannot proceed without an answer. "
            "Questions are exempt from scope enforcement — any agent may raise one."
        ),
        inputSchema={
            "type": "object",
            "required": ["scope", "asks"],
            "properties": {
                "scope": {"type": "string"},
                "asks": {"type": "string"},
                "target": {"type": "string", "default": "human"},
                "blocking": {"type": "boolean", "default": True},
            },
        },
    ),
    # ------------------------------------------------------------------ #8
    Tool(
        name="answer_question",
        description="Answer an open question. The question moves to 'answered' status.",
        inputSchema={
            "type": "object",
            "required": ["question_id", "answer"],
            "properties": {
                "question_id": {"type": "string"},
                "answer": {"type": "string"},
            },
        },
    ),
    # ------------------------------------------------------------------ #9
    Tool(
        name="resolve_question",
        description="Mark a question fully resolved. Final close.",
        inputSchema={
            "type": "object",
            "required": ["question_id", "resolution"],
            "properties": {
                "question_id": {"type": "string"},
                "resolution": {"type": "string"},
            },
        },
    ),
]


# ----------------------------------------------------------------------
# Tool dispatch — every call becomes one HTTP request
# ----------------------------------------------------------------------

async def call_tool(name: str, args: dict[str, Any]) -> list[TextContent]:

    # ---- register (NEW — must be called first) ------------------------
    if name == "register":
        body: dict[str, Any] = {
            "agent_id": AGENT_ID,
            "type": args.get("type", "agent"),
            "task": args["task"],
            "scope": args.get("scope", []),
        }
        if "role_tag" in args:
            body["role_tag"] = args["role_tag"]
        if "mode" in args:
            body["mode"] = args["mode"]
        if "limits" in args:
            body["limits"] = args["limits"]
        return _ok(await _request("POST", "/api/register", body))

    # ---- read_state ---------------------------------------------------
    if name == "read_state":
        scope = args.get("scope_filter")
        since_id = args.get("since_id")
        parts = []
        if scope:
            parts.append(f"scope={scope}")
        if since_id is not None:
            parts.append(f"since_id={since_id}")
        qs = ("?" + "&".join(parts)) if parts else ""
        return _ok(await _request("GET", f"/api/state{qs}"))

    if name == "claim_intent":
        return _ok(await _request("POST", "/api/intents", {
            "scope": args["scope"],
            "action": args["action"],
            "ttl_minutes": args.get("ttl_minutes", 10),
        }))

    if name == "release_intent":
        return _ok(await _request(
            "DELETE", f"/api/intents/{args['intent_id']}"
        ))

    if name == "commit_decision":
        return _ok(await _request("POST", "/api/decisions", {
            "scope": args["scope"],
            "key": args["key"],
            "value": args["value"],
            "anchor": args.get("anchor"),
            "rationale": args.get("rationale"),
        }))

    if name == "share_discovery":
        return _ok(await _request("POST", "/api/discoveries", {
            "scope": args["scope"],
            "summary": args["summary"],
            "file_hash": args.get("file_hash"),
            "confidence": args.get("confidence", "unverified"),
        }))

    if name == "raise_question":
        return _ok(await _request("POST", "/api/questions", {
            "scope": args["scope"],
            "asks": args["asks"],
            "target": args.get("target", "human"),
            "blocking": args.get("blocking", True),
        }))

    if name == "answer_question":
        return _ok(await _request(
            "POST", f"/api/questions/{args['question_id']}/answer",
            {"answer": args["answer"]},
        ))

    if name == "resolve_question":
        return _ok(await _request(
            "POST", f"/api/questions/{args['question_id']}/resolve",
            {"resolution": args["resolution"]},
        ))

    return _ok({"status": "error", "detail": f"unknown tool: {name}"})


# ----------------------------------------------------------------------
# MCP server boot
# ----------------------------------------------------------------------

server: Server = Server("coord-shim")


@server.list_tools()
async def list_tools() -> list[Tool]:
    return TOOLS


@server.call_tool()
async def handle_call(name: str, args: dict[str, Any]) -> list[TextContent]:
    return await call_tool(name, args or {})


async def main() -> None:
    async with stdio_server() as (read, write):
        await server.run(read, write, server.create_initialization_options())


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
