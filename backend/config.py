"""Daemon configuration. Override via environment variables."""
import os
from pathlib import Path

# Network
HOST = os.environ.get("COORD_HOST", "127.0.0.1")
PORT = int(os.environ.get("COORD_PORT", "49152"))

# Storage
SHARED_DIR = Path(os.environ.get("COORD_SHARED_DIR", ".shared")).resolve()
DB_PATH = SHARED_DIR / "state.db"
LOG_PATH = SHARED_DIR / "log.jsonl"
INBOX_PATH = SHARED_DIR / "inbox.md"

# Workspace root used for file-backed discovery hash checks.
WORKSPACE_ROOT = Path(os.environ.get("COORD_WORKSPACE_ROOT", ".")).resolve()

# Defaults
DEFAULT_INTENT_TTL_MINUTES = 10
MAX_ACTIVE_INTENTS_PER_AGENT = 3
SCOPE_COOLDOWN_SECONDS = 5

# Schema location (sibling of this file)
SCHEMA_PATH = Path(__file__).parent / "schema.sql"

# Frontend origin for CORS — Next.js dev server by default
FRONTEND_ORIGIN = os.environ.get("COORD_FRONTEND_ORIGIN", "http://localhost:3000")


def ensure_shared_dir() -> None:
    SHARED_DIR.mkdir(parents=True, exist_ok=True)
