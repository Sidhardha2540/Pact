# Coord — Agent Coordination Layer

A lightweight, local coordination layer for parallel coding agents (Claude Code, Cursor, Aider, etc.) working toward a shared goal.

Agents share **only what matters** — decisions, discoveries, intents, and blockers — through a single localhost daemon. The developer becomes a triager, not a manual integration layer.

---

## The Problem

When you run two coding agents in parallel toward a shared goal, they operate in isolation:

- They re-derive the same facts about the codebase (token waste).
- They make incompatible commitments — different naming, contracts, assumptions (integration failures).
- They cannot pick up where another left off (context loss).
- The developer ends up copy-pasting context between tabs and reconciling conflicts manually.

There is no lightweight, local mechanism for two agents working on the same goal to share what matters — and only what matters — between each other.

## The Solution

A single-process **coordination daemon** running on `127.0.0.1:49152` that holds four shared structures:

| Type          | Consistency           | Purpose                                                  |
| ------------- | --------------------- | -------------------------------------------------------- |
| **Decision**  | First-Write-Wins      | Contracts: naming, signatures, schemas. Conflict → 409.  |
| **Discovery** | Last-Write-Wins+Hash  | Facts read from the codebase. Invalidated on file change.|
| **Intent**    | TTL Lease             | "I'm working on X" — auto-expires, prevents collisions.  |
| **Question**  | Append + Status       | Blockers escalated to the human or another agent.        |

Agents talk to the daemon through **9 named MCP tools** (no generic `sync()`). Conflicts return structured codes (`403`, `409`, `423`, `429`) so agents can react mechanically.

A **web dashboard** streams every event live over WebSocket — the developer sees what's happening, triages conflicts, answers questions.

## Architecture

```
┌──────────────┐         ┌──────────────┐         ┌──────────────┐
│ Claude Code  │         │    Cursor    │         │    Aider     │
└──────┬───────┘         └──────┬───────┘         └──────┬───────┘
       │ stdio (MCP)            │ stdio (MCP)            │ stdio (MCP)
       ▼                        ▼                        ▼
┌──────────────┐         ┌──────────────┐         ┌──────────────┐
│   MCP Shim   │         │   MCP Shim   │         │   MCP Shim   │
│  (per agent) │         │  (per agent) │         │  (per agent) │
└──────┬───────┘         └──────┬───────┘         └──────┬───────┘
       │ HTTP                   │ HTTP                   │ HTTP
       └────────────┬───────────┴────────────────────────┘
                    ▼
        ┌────────────────────────┐         ┌──────────────────┐
        │  Coordination Daemon   │ ◀──WS───│  Web Dashboard   │
        │  (FastAPI on :49152)   │         │  (Next.js)       │
        └────────────┬───────────┘         └──────────────────┘
                     │
         ┌───────────┴───────────┐
         ▼                       ▼
   ┌──────────┐           ┌─────────────┐
   │state.db  │           │ log.jsonl   │
   │(SQLite)  │           │(append-only)│
   └──────────┘           └─────────────┘
```

**Why this shape:**

1. **Single source of truth.** Only the daemon writes to disk. No two agents race for `state.json`.
2. **MCP shims per agent.** Each agent gets its own stdio MCP process so identity is implicit. No spoofing.
3. **HTTP between shim and daemon.** Standard, debuggable, allows non-MCP agents and REST clients to participate.
4. **WebSocket for the UI.** The dashboard observes live state and only writes when a human answers or resolves a question.
5. **SQLite + JSONL.** State for queries, log for audit. SQLite WAL mode + `asyncio.Lock` serialization prevents corruption. JSONL append uses `fsync` for durability.

## The 9 Tools

```
register(task, scope=None, mode="exclusive")       → 200 (session registration)
read_state(scope_filter=None)                      → snapshot of all four stores
claim_intent(scope, action, ttl_minutes=10)        → 200 | 423 (overlap)
release_intent(intent_id)                          → 200
commit_decision(scope, key, value, rationale=None) → 200 | 409 (FWW conflict)
share_discovery(scope, summary, file_hash, conf)   → 200 (auto-supersedes prior)
raise_question(scope, asks, target, blocking=True) → 200 (queues to inbox)
answer_question(question_id, answer)               → 200
resolve_question(question_id, resolution)          → 200
```

Typical agent flow:

1. `register(task="...")`
2. `read_state(scope_filter="...")`
3. `claim_intent(scope="...", action="...")`
4. Share decisions, discoveries, and questions while working.
5. `release_intent(intent_id="...")` when the scope is done.

## Repo Layout

```
coord/
├── backend/              # FastAPI daemon — the single source of truth
│   ├── main.py           # App entry, routes, WebSocket
│   ├── state_engine.py   # Four consistency models (the heart)
│   ├── storage.py        # SQLite + JSONL atomic writes
│   ├── ws_broadcast.py   # WebSocket connection manager
│   ├── models.py         # Pydantic schemas
│   ├── schema.sql        # SQLite DDL
│   ├── config.py         # Paths, ports, defaults
│   └── requirements.txt
│
├── mcp-shim/             # Per-agent stdio MCP server, proxies to daemon
│   ├── shim.py
│   └── requirements.txt
│
├── frontend/             # Next.js 14 dashboard
│   ├── app/
│   │   ├── layout.tsx
│   │   ├── page.tsx
│   │   └── globals.css
│   ├── components/
│   │   ├── StatePanel.tsx
│   │   ├── ActivityFeed.tsx
│   │   ├── InboxPanel.tsx
│   │   ├── AgentBadge.tsx
│   │   └── cards/
│   ├── lib/
│   │   ├── store.ts      # Zustand
│   │   ├── ws-client.ts
│   │   └── types.ts
│   └── package.json
│
├── examples/
│   ├── claude_mcp_config.example.json
│   └── cursor_mcp_config.example.json
│
├── tests/                # API-level coordination behavior tests
│   └── test_coord_api.py
│
├── .shared/              # Runtime state (gitignored)
│   ├── state.db
│   ├── log.jsonl
│   └── inbox.md          # Human-readable mirror
│
├── ARCHITECTURE.md       # Deeper rationale
└── README.md
```

## Quick Start

```bash
# 1. Start the daemon (run from the repo root — relative imports require the package layout)
pip install -r backend/requirements.txt
python -m uvicorn backend.main:app --host 127.0.0.1 --port 49152
# → daemon listening on http://127.0.0.1:49152
#
# Note: `cd backend && python main.py` fails with ImportError (relative imports).
# Note: install deps first — otherwise ModuleNotFoundError: fastapi.

# 2. Start the dashboard
cd frontend
npm install
npm run dev
# → dashboard on http://localhost:3000

# 3. Wire up an agent (Claude Code example)
# Add to ~/.claude/mcp_config.json:
{
  "mcpServers": {
    "coord": {
      "command": "python",
      "args": ["/path/to/coord/mcp-shim/shim.py"],
      "env": { "COORD_AGENT_ID": "claude-code" }
    }
  }
}
```

## Quality Checks

```bash
# Backend API behavior
python -m unittest discover -s tests

# Python syntax
python -m compileall backend mcp-shim tests

# Frontend type safety and linting
cd frontend
npm run typecheck
npm run lint
```

## Status

This repo contains the daemon core, state engine, MCP shim, dashboard, setup examples, and API-level tests for the main coordination semantics. Runtime state lives under `.shared/` and is intentionally ignored by Git.
