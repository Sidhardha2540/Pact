from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import hashlib
import unittest

from fastapi.testclient import TestClient

from backend import config, main


class CoordApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        shared_dir = Path(self.tmp.name)

        config.SHARED_DIR = shared_dir
        config.DB_PATH = shared_dir / "state.db"
        config.LOG_PATH = shared_dir / "log.jsonl"
        config.INBOX_PATH = shared_dir / "inbox.md"
        config.WORKSPACE_ROOT = shared_dir

        main.rate_tracker.clear()
        main._limits_cache.clear()

        self.client_ctx = TestClient(main.app)
        self.client = self.client_ctx.__enter__()

    def tearDown(self) -> None:
        self.client_ctx.__exit__(None, None, None)
        self.tmp.cleanup()

    @staticmethod
    def headers(agent_id: str) -> dict[str, str]:
        return {"X-Coord-Agent-Id": agent_id}

    def register(self, agent_id: str, scope: list[str] | None = None) -> None:
        response = self.client.post(
            "/api/register",
            json={
                "agent_id": agent_id,
                "type": "agent",
                "task": f"{agent_id} test task",
                "scope": scope or [],
            },
        )
        self.assertEqual(response.status_code, 200, response.text)

    def test_registered_agent_can_claim_dynamic_scope(self) -> None:
        self.register("agent-a")

        response = self.client.post(
            "/api/intents",
            headers=self.headers("agent-a"),
            json={"scope": "src/todos.py", "action": "edit todo API"},
        )

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["status"], "claimed")

    def test_overlapping_intent_returns_locked_conflict(self) -> None:
        self.register("agent-a")
        self.register("agent-b")

        first = self.client.post(
            "/api/intents",
            headers=self.headers("agent-a"),
            json={"scope": "src/", "action": "refactor package"},
        )
        self.assertEqual(first.status_code, 200, first.text)

        second = self.client.post(
            "/api/intents",
            headers=self.headers("agent-b"),
            json={"scope": "src/todos.py", "action": "edit endpoint"},
        )

        self.assertEqual(second.status_code, 423, second.text)
        self.assertEqual(second.json()["code"], 423)

    def test_decision_requires_owned_scope(self) -> None:
        self.register("agent-a")

        response = self.client.post(
            "/api/decisions",
            headers=self.headers("agent-a"),
            json={
                "scope": "src/todos.py",
                "key": "route_shape",
                "value": "paged",
            },
        )

        self.assertEqual(response.status_code, 403, response.text)

    def test_decision_conflict_is_first_write_wins(self) -> None:
        self.register("agent-a")
        claim = self.client.post(
            "/api/intents",
            headers=self.headers("agent-a"),
            json={"scope": "src/todos.py", "action": "define route shape"},
        )
        self.assertEqual(claim.status_code, 200, claim.text)

        first = self.client.post(
            "/api/decisions",
            headers=self.headers("agent-a"),
            json={
                "scope": "src/todos.py",
                "key": "route_shape",
                "value": "paged",
            },
        )
        self.assertEqual(first.status_code, 200, first.text)

        second = self.client.post(
            "/api/decisions",
            headers=self.headers("agent-a"),
            json={
                "scope": "src/todos.py",
                "key": "route_shape",
                "value": "plain_list",
            },
        )

        self.assertEqual(second.status_code, 409, second.text)
        self.assertEqual(second.json()["existing"]["value"], "paged")

    def test_discovery_reports_stale_when_file_hash_changes(self) -> None:
        self.register("agent-a")
        workspace_file = config.WORKSPACE_ROOT / "src" / "todos.py"
        workspace_file.parent.mkdir(parents=True, exist_ok=True)
        workspace_file.write_text("version = 1\n", encoding="utf-8")
        file_hash = hashlib.sha256(workspace_file.read_bytes()).hexdigest()

        claim = self.client.post(
            "/api/intents",
            headers=self.headers("agent-a"),
            json={"scope": "src/todos.py", "action": "inspect todo module"},
        )
        self.assertEqual(claim.status_code, 200, claim.text)

        discovery = self.client.post(
            "/api/discoveries",
            headers=self.headers("agent-a"),
            json={
                "scope": "src/todos.py",
                "summary": "todos.py defines version metadata",
                "file_hash": f"sha256:{file_hash}",
                "confidence": "verified",
            },
        )
        self.assertEqual(discovery.status_code, 200, discovery.text)

        current = self.client.get("/api/state").json()["discoveries"][0]
        self.assertFalse(current["stale"])

        workspace_file.write_text("version = 2\n", encoding="utf-8")

        stale = self.client.get("/api/state").json()["discoveries"][0]
        self.assertTrue(stale["stale"])


if __name__ == "__main__":
    unittest.main()
