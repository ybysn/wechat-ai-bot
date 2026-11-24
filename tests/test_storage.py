"""Storage tests use a self-contained v2 schema fixture."""

import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from wechat_ai_bot.migrations import migrate_v2_to_v3, migrate_v3_to_v4
from wechat_ai_bot.storage import Storage


ACCOUNT = "test-account"
GROUP = "test-group@chatroom"

V2_DDL = """
CREATE TABLE events (
    account_id TEXT NOT NULL,
    cursor TEXT NOT NULL,
    kind TEXT NOT NULL,
    thread_id TEXT,
    message_id TEXT,
    PRIMARY KEY(account_id, cursor)
);
CREATE TABLE consumer_state (
    consumer_name TEXT NOT NULL,
    account_id TEXT NOT NULL,
    last_durable_cursor TEXT,
    last_acked_cursor TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(consumer_name, account_id)
);
CREATE TABLE messages (
    account_id TEXT NOT NULL,
    message_id TEXT NOT NULL,
    thread_id TEXT NOT NULL,
    sender_id TEXT,
    direction TEXT,
    message_type INTEGER,
    body TEXT,
    mentions_self INTEGER CHECK(mentions_self IN (0,1) OR mentions_self IS NULL),
    created_at TEXT,
    source_cursor TEXT,
    status TEXT NOT NULL DEFAULT 'pending'
      CHECK(status IN ('pending','processing','processed','ignored','failed')),
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
    UNIQUE(account_id, source_message_id),
    FOREIGN KEY(account_id, source_message_id)
      REFERENCES messages(account_id, message_id)
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
CREATE TABLE send_attempts (
    attempt_id TEXT PRIMARY KEY,
    outbox_id TEXT
);
CREATE TABLE schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE topic_history (
    topic_id TEXT PRIMARY KEY,
    topic_text TEXT
);
INSERT INTO schema_migrations(version) VALUES(2);
PRAGMA user_version=2;
"""


class StorageTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "isolated.sqlite3"

        con = sqlite3.connect(self.db, isolation_level=None)
        try:
            con.execute("PRAGMA foreign_keys=ON")
            con.executescript(V2_DDL)
            migrate_v2_to_v3(con)
            migrate_v3_to_v4(con)
        finally:
            con.close()

        self.store = Storage(self.db)

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def test_event_dedup(self):
        s = self.store
        self.assertTrue(s.persist_event(
            ACCOUNT, "1", "message.committed", GROUP, "m1"
        ))
        self.assertFalse(s.persist_event(
            ACCOUNT, "1", "message.committed", GROUP, "m1"
        ))
        self.assertTrue(s.persist_event(
            ACCOUNT, "2", "group.updated", GROUP
        ))
        self.assertFalse(s.persist_event(
            ACCOUNT, "1", "message.committed", GROUP, "m1"
        ))
        self.assertEqual(
            s.checkpoint(ACCOUNT)["last_durable_cursor"],
            "2"
        )

    def test_cursor_conflict(self):
        s = self.store
        s.persist_event(
            ACCOUNT, "1", "message.committed", GROUP, "m1"
        )
        with self.assertRaises(ValueError):
            s.persist_event(
                ACCOUNT, "1",
                "message.committed", GROUP, "different"
            )
        self.assertEqual(
            s.checkpoint(ACCOUNT)["last_durable_cursor"],
            "1"
        )

    def test_transaction_rollback(self):
        s = self.store
        with self.assertRaises(RuntimeError):
            with s.transaction():
                s.conn.execute(
                    "INSERT INTO events "
                    "(account_id,cursor,kind) VALUES(?,?,?)",
                    (ACCOUNT, "rollback", "message.committed"),
                )
                raise RuntimeError("Simulated crash")

        count = s.conn.execute(
            "SELECT COUNT(*) FROM events"
        ).fetchone()[0]
        self.assertEqual(count, 0)

    def test_ack_requires_durable_event(self):
        s = self.store

        # 合法格式，但尚无对应的 durable event
        with self.assertRaises(ValueError):
            s.mark_acked(ACCOUNT, "999")

        s.persist_event(
            ACCOUNT, "1", "message.committed", GROUP, "m1"
        )
        self.assertTrue(s.mark_acked(ACCOUNT, "1"))
        self.assertFalse(s.mark_acked(ACCOUNT, "1"))

        s.persist_event(
            ACCOUNT, "2", "group.updated", GROUP
        )
        with self.assertRaises(ValueError):
            s.mark_acked(ACCOUNT, "1")

        self.assertTrue(s.mark_acked(ACCOUNT, "2"))
        self.assertEqual(
            s.checkpoint(ACCOUNT)["last_acked_cursor"],
            "2"
        )

    def test_message_dedup(self):
        s = self.store

        self.assertTrue(s.persist_message(
            ACCOUNT, "m1", GROUP,
            direction="incoming", body="你好"
        ))

        # 不允许冲突正文被静默忽略或覆盖
        with self.assertRaisesRegex(
            ValueError, "Conflicting message field: body"
        ):
            s.persist_message(
                ACCOUNT, "m1", GROUP,
                direction="incoming", body="重复内容"
            )

        # 原始正文必须保持不变
        self.assertEqual(
            s.message(ACCOUNT, "m1")["body"],
            "你好"
        )

        # 完全相同的消息仍然保持幂等
        self.assertFalse(s.persist_message(
            ACCOUNT, "m1", GROUP,
            direction="incoming", body="你好"
        ))

    def test_decision_and_outbox_dedup(self):
        s = self.store
        s.persist_message(
            ACCOUNT, "m1", GROUP,
            direction="incoming", body="晚上吃啥",
            created_at=datetime.now(timezone.utc).isoformat(),
        )

        decision, created = s.decide(
            ACCOUNT, "m1", "reply",
            model="qwen3.5:9b",
            reply="测试回复",
        )
        self.assertTrue(created)
        self.assertEqual(
            s.conn.execute(
                "SELECT reply FROM decisions WHERE decision_id=?",
                (decision,),
            ).fetchone()["reply"],
            "测试回复",
        )

        repeated, created = s.decide(
            ACCOUNT, "m1", "skip"
        )
        self.assertEqual(decision, repeated)
        self.assertFalse(created)

        with self.assertRaises(ValueError):
            s.enqueue(decision, ACCOUNT, GROUP, "测试回复")

        self.assertTrue(s.accept_decision(decision))

        outbox, created = s.enqueue(
            decision, ACCOUNT, GROUP, "测试回复"
        )
        self.assertTrue(created)

        repeated, created = s.enqueue(
            decision, ACCOUNT, GROUP, "测试回复"
        )
        self.assertEqual(outbox, repeated)
        self.assertFalse(created)

        with self.assertRaises(ValueError):
            s.enqueue(
                decision, ACCOUNT, GROUP, "不同的回复"
            )

        count = s.conn.execute(
            "SELECT COUNT(*) FROM outbox"
        ).fetchone()[0]
        self.assertEqual(count, 1)

        attempts = s.conn.execute(
            "SELECT COUNT(*) FROM send_attempts"
        ).fetchone()[0]
        self.assertEqual(attempts, 0)

    def test_accept_legacy_reply_without_body_is_rejected(self):
        legacy_db = Path(self.temp.name) / "legacy.sqlite3"
        con = sqlite3.connect(legacy_db, isolation_level=None)
        try:
            con.execute("PRAGMA foreign_keys=ON")
            con.executescript(V2_DDL)
            con.execute(
                "INSERT INTO messages "
                "(account_id,message_id,thread_id,direction,body) "
                "VALUES(?,?,?,?,?)",
                (ACCOUNT, "legacy", GROUP, "incoming", "旧消息"),
            )
            con.execute(
                "INSERT INTO decisions "
                "(decision_id,account_id,source_message_id,action,reason,model,status) "
                "VALUES(?,?,?,?,?,?,?)",
                (
                    "legacy-decision", ACCOUNT, "legacy", "reply",
                    "历史决策没有正文", "qwen3.5:9b", "proposed",
                ),
            )
            migrate_v2_to_v3(con)
            migrate_v3_to_v4(con)
        finally:
            con.close()

        legacy_store = Storage(legacy_db)
        try:
            self.assertFalse(
                legacy_store.accept_decision("legacy-decision")
            )
        finally:
            legacy_store.close()

    def test_reply_decision_requires_candidate_text(self):
        s = self.store
        s.persist_message(
            ACCOUNT, "m-no-reply", GROUP,
            direction="incoming", body="问题"
        )
        with self.assertRaises(ValueError):
            s.decide(
                ACCOUNT, "m-no-reply", "reply",
                model="qwen3.5:9b",
            )
        self.assertEqual(
            s.conn.execute(
                "SELECT COUNT(*) FROM decisions "
                "WHERE source_message_id='m-no-reply'"
            ).fetchone()[0],
            0,
        )

    def test_decide_and_process_rolls_back_as_one_transaction(self):
        s = self.store
        s.persist_message(
            ACCOUNT, "m-race", GROUP,
            direction="incoming", body="问题"
        )
        s.conn.execute(
            "UPDATE messages SET status='processed' "
            "WHERE account_id=? AND message_id=?",
            (ACCOUNT, "m-race"),
        )

        with self.assertRaises(RuntimeError):
            s.decide_and_process(
                ACCOUNT, "m-race",
                action="reply",
                reason="明确提问",
                model="qwen3.5:9b",
                reply="候选回复",
                decision_status="proposed",
            )

        self.assertEqual(
            s.conn.execute(
                "SELECT COUNT(*) FROM decisions "
                "WHERE source_message_id='m-race'"
            ).fetchone()[0],
            0,
        )


if __name__ == "__main__":
    unittest.main()
