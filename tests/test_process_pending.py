import sqlite3
import tempfile
import unittest
from pathlib import Path

from wechat_ai_bot.storage import Storage
from wechat_ai_bot.migrations import migrate_v2_to_v3, migrate_v3_to_v4
from wechat_ai_bot.decision_engine import decide
from wechat_ai_bot.process_pending import process_one

CFG = {
    "schema_version": 2,
    "app": {"auto_send": False},
    "sender": {"enabled": False},
    "runtime": {"global_pause": True},
    "agent_wechat": {
        "account_id": "a",
        "target_group_id": "g",
    },
    "safety": {"group_allowlist": ["g"]},
    "processing": {"max_context_messages": 12},
    "ollama": {"model": "qwen3.5:9b"},
}

DDL = """
CREATE TABLE messages (
 account_id TEXT NOT NULL, message_id TEXT NOT NULL, thread_id TEXT NOT NULL,
 sender_id TEXT, direction TEXT, body TEXT, created_at TEXT,
 source_cursor TEXT, status TEXT NOT NULL DEFAULT 'pending',
 PRIMARY KEY(account_id, message_id)
);
CREATE TABLE decisions (
 decision_id TEXT PRIMARY KEY,
 account_id TEXT NOT NULL,
 source_message_id TEXT,
 action TEXT NOT NULL CHECK(action IN ('reply','topic','skip')),
 reason TEXT,
 model TEXT,
 status TEXT NOT NULL DEFAULT 'proposed'
   CHECK(status IN ('proposed','accepted','rejected')),
 created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
 UNIQUE(account_id,source_message_id),
 FOREIGN KEY(account_id,source_message_id)
   REFERENCES messages(account_id,message_id)
);
CREATE TABLE outbox (
 outbox_id TEXT PRIMARY KEY,
 decision_id TEXT NOT NULL UNIQUE,
 account_id TEXT NOT NULL,
 thread_id TEXT NOT NULL,
 content TEXT NOT NULL,
 idempotency_key TEXT NOT NULL UNIQUE,
 status TEXT NOT NULL DEFAULT 'queued'
   CHECK(status IN ('queued','sending','sent','failed','needs_review','cancelled')),
 remote_send_id TEXT,
 created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
 sent_at TEXT,
 FOREIGN KEY(decision_id) REFERENCES decisions(decision_id)
);
CREATE TABLE schema_migrations (
 version INTEGER PRIMARY KEY,
 applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO schema_migrations(version) VALUES(2);
PRAGMA user_version=2;
"""


class PendingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        path = Path(self.temp.name) / "test.sqlite3"

        conn = sqlite3.connect(path)
        try:
            conn.execute("PRAGMA foreign_keys=ON")
            conn.executescript(DDL)
            migrate_v2_to_v3(conn)
            migrate_v3_to_v4(conn)
        finally:
            conn.close()

        self.store = Storage(path)

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def add(self, message_id, body, cursor, status="pending"):
        self.store.conn.execute(
            "INSERT INTO messages "
            "(account_id,message_id,thread_id,sender_id,direction,body,source_cursor,status) "
            "VALUES (?,?,?,?,?,?,?,?)",
            ("a", message_id, "g", "member",
             "incoming", body, cursor, status),
        )

    def test_skip_is_persisted(self):
        self.add("m1", "hello", "10")

        result = process_one(
            self.store, CFG,
            lambda c, x: {
                "action": "skip",
                "reason": "low_signal",
                "reply": "",
            },
        )

        self.assertEqual(result["status"], "saved")
        self.assertEqual(
            self.store.conn.execute(
                "SELECT status FROM messages"
            ).fetchone()[0],
            "processed",
        )
        self.assertEqual(
            self.store.conn.execute(
                "SELECT status FROM decisions"
            ).fetchone()[0],
            "rejected",
        )

    def test_model_error_keeps_pending(self):
        self.add("m1", "hello world", "10")

        # Exercise the real process_pending -> decision_engine boundary.
        def failing_requester(payload, config):
            raise TimeoutError("timeout")

        result = process_one(
            self.store, CFG,
            lambda context, config: decide(
                context, config, requester=failing_requester
            ),
        )

        self.assertEqual(result["status"], "deferred")
        self.assertEqual(
            result["error_code"], "model_request_failed"
        )
        self.assertEqual(result["error_type"], "TimeoutError")
        self.assertEqual(
            self.store.conn.execute(
                "SELECT status FROM messages"
            ).fetchone()[0],
            "pending",
        )
        self.assertEqual(
            self.store.conn.execute(
                "SELECT COUNT(*) FROM decisions"
            ).fetchone()[0],
            0,
        )

    def test_model_reason_prefix_cannot_trigger_system_defer(self):
        self.add("m1", "hello world", "10")

        result = process_one(
            self.store, CFG,
            lambda c, x: {
                "action": "skip",
                "reason": "model_error:TimeoutError",
                "reply": "",
            },
        )

        self.assertEqual(result["status"], "saved")
        self.assertEqual(result["action"], "skip")
        self.assertEqual(
            self.store.conn.execute(
                "SELECT status FROM messages"
            ).fetchone()[0],
            "processed",
        )
        self.assertEqual(
            self.store.conn.execute(
                "SELECT status FROM decisions"
            ).fetchone()[0],
            "rejected",
        )

    def test_reply_remains_proposed(self):
        self.add("m1", "hello world", "10")

        result = process_one(
            self.store, CFG,
            lambda c, x: {
                "action": "reply",
                "reason": "明确提问",
                "reply": "可以聊聊！",
            },
        )

        self.assertEqual(result["action"], "reply")
        self.assertEqual(
            self.store.conn.execute(
                "SELECT status FROM decisions"
            ).fetchone()[0],
            "proposed",
        )
        self.assertEqual(
            self.store.conn.execute(
                "SELECT reply FROM decisions"
            ).fetchone()[0],
            "可以聊聊！",
        )
        self.assertEqual(
            self.store.conn.execute(
                "SELECT COUNT(*) FROM outbox"
            ).fetchone()[0],
            0,
        )

    def test_context_does_not_include_future(self):
        self.add("m1", "earlier question", "10")
        self.add("m2", "later content", "11", "processed")

        def check(context, config):
            self.assertEqual(
                [item["text"] for item in context],
                ["earlier question"],
            )
            return {
                "action": "skip",
                "reason": "测试完成",
                "reply": "",
            }

        self.assertEqual(
            process_one(self.store, CFG, check)["status"],
            "saved",
        )

    def test_no_duplicate_decision(self):
        self.add("m1", "hello world", "10")

        def fake(context, config):
            return {
                "action": "skip",
                "reason": "不需要回复",
                "reply": "",
            }

        process_one(self.store, CFG, fake)

        self.assertEqual(
            process_one(self.store, CFG, fake)["status"],
            "empty",
        )
        self.assertEqual(
            self.store.conn.execute(
                "SELECT COUNT(*) FROM decisions"
            ).fetchone()[0],
            1,
        )


if __name__ == "__main__":
    unittest.main()
