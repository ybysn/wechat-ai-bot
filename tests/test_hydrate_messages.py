import sqlite3
import tempfile
import unittest
from pathlib import Path

from wechat_ai_bot.hydrate_messages import persist_hydrated_message
from wechat_ai_bot.storage import Storage


ACCOUNT = "test-account"
GROUP = "test-group@chatroom"


class HydrationIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "hydration.sqlite3"

        with sqlite3.connect(self.db) as conn:
            conn.executescript("""
                CREATE TABLE decisions (
                    decision_id TEXT PRIMARY KEY,
                    reply TEXT
                );

                CREATE TABLE scheduled_tasks (
                    task_id TEXT PRIMARY KEY
                );

                CREATE TABLE messages (
                    account_id TEXT NOT NULL,
                    message_id TEXT NOT NULL,
                    thread_id TEXT NOT NULL,
                    sender_id TEXT,
                    direction TEXT,
                    message_type INTEGER,
                    body TEXT,
                    mentions_self INTEGER,
                    created_at TEXT,
                    source_cursor TEXT,
                    status TEXT NOT NULL DEFAULT 'pending',
                    PRIMARY KEY(account_id, message_id)
                );

                CREATE TABLE events (
                    account_id TEXT NOT NULL,
                    cursor TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    thread_id TEXT,
                    message_id TEXT,
                    status TEXT NOT NULL DEFAULT 'received',
                    PRIMARY KEY(account_id, cursor)
                );

                PRAGMA user_version=4;
            """)

        self.store = Storage(self.db)

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def add_event(self, cursor="1", message_id="m1", status="received"):
        self.store.conn.execute(
            "INSERT INTO events "
            "(account_id,cursor,kind,thread_id,message_id,status) "
            "VALUES(?,?,?,?,?,?)",
            (
                ACCOUNT, cursor, "message.committed",
                GROUP, message_id, status,
            ),
        )

    def hydrate(self, **overrides):
        values = {
            "account": ACCOUNT,
            "message_id": "m1",
            "group": GROUP,
            "cursor": "1",
            "direction": "incoming",
            "body": "1",
            "sender_id": "sender-1",
            "message_type": 1,
            "mentions_self": True,
            "created_at": "2026-11-21T01:00:00+00:00",
            "max_chars": 1000,
        }
        values.update(overrides)
        return persist_hydrated_message(self.store, **values)

    def test_incoming_mentioned_message_becomes_pending(self):
        self.add_event()
        message, exceeded = self.hydrate()

        self.assertFalse(exceeded)
        self.assertEqual(message["body"], "1")
        self.assertEqual(message["mentions_self"], 1)
        self.assertEqual(message["status"], "pending")
        self.assertEqual(
            self.store.conn.execute(
                "SELECT status FROM events WHERE account_id=? AND cursor=?",
                (ACCOUNT, "1"),
            ).fetchone()[0],
            "processed",
        )

    def test_empty_body_is_ignored(self):
        self.add_event()
        message, _ = self.hydrate(body="   ", mentions_self=None)

        self.assertIsNone(message["body"])
        self.assertEqual(message["status"], "ignored")

    def test_incomplete_message_is_enriched_and_requeued(self):
        self.store.persist_message(
            ACCOUNT, "m1", GROUP,
            direction="incoming",
            body=None,
            mentions_self=None,
            source_cursor="1",
        )
        self.store.conn.execute(
            "UPDATE messages SET status='ignored' "
            "WHERE account_id=? AND message_id=?",
            (ACCOUNT, "m1"),
        )
        self.add_event()

        message, _ = self.hydrate(
            body="1",
            mentions_self=True,
        )

        self.assertEqual(message["body"], "1")
        self.assertEqual(message["mentions_self"], 1)
        self.assertEqual(message["status"], "pending")

    def test_legacy_blank_body_is_enriched_and_requeued(self):
        self.store.conn.execute(
            "INSERT INTO messages "
            "(account_id,message_id,thread_id,direction,body,status) "
            "VALUES(?,?,?,?,?,?)",
            (ACCOUNT, "m1", GROUP, "incoming", "", "ignored"),
        )
        self.add_event()

        message, _ = self.hydrate(body="1", mentions_self=True)

        self.assertEqual(message["body"], "1")
        self.assertEqual(message["mentions_self"], 1)
        self.assertEqual(message["status"], "pending")

    def test_conflicting_body_keeps_event_received(self):
        self.store.persist_message(
            ACCOUNT, "m1", GROUP,
            direction="incoming",
            body="original",
            mentions_self=None,
            source_cursor="1",
        )
        self.add_event()

        with self.assertRaisesRegex(
            ValueError, "Conflicting message field: body"
        ):
            self.hydrate(body="different")

        status = self.store.conn.execute(
            "SELECT status FROM events WHERE account_id=? AND cursor=?",
            (ACCOUNT, "1"),
        ).fetchone()[0]
        self.assertEqual(status, "received")

        self.assertEqual(
            self.store.message(ACCOUNT, "m1")["body"],
            "original",
        )

    def test_missing_received_event_does_not_lose_durable_message(self):
        self.add_event(status="processed")

        with self.assertRaisesRegex(
            RuntimeError, "事件状态发生并发变化"
        ):
            self.hydrate()

        # Message commit is intentionally durable before event completion.
        self.assertEqual(
            self.store.message(ACCOUNT, "m1")["body"],
            "1",
        )


if __name__ == "__main__":
    unittest.main()
