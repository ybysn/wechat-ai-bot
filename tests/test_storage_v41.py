import sqlite3
import tempfile
import unittest
from pathlib import Path

from wechat_ai_bot.storage import Storage


ACCOUNT = "test-account"
GROUP = "test-group@chatroom"


DDL = """
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
    reply TEXT,
    UNIQUE(account_id, source_message_id)
);

CREATE TABLE scheduled_tasks (
    task_id TEXT PRIMARY KEY
);

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
    last_durable_cursor TEXT NOT NULL,
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

PRAGMA user_version=4;
"""


class StorageV41Tests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "storage-v41.sqlite3"
        with sqlite3.connect(self.db) as conn:
            conn.executescript(DDL)
        self.store = Storage(self.db, consumer_name="test-consumer")

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def test_cursor_must_be_numeric_ascii(self):
        for invalid in ("", "c1", "１２３", "1 2", "12345678901234567890"):
            with self.subTest(cursor=invalid):
                with self.assertRaises(ValueError):
                    self.store.persist_event(
                        ACCOUNT, invalid, "message.committed"
                    )

    def test_cursor_must_fit_sqlite_integer(self):
        with self.assertRaises(ValueError):
            self.store.persist_event(
                ACCOUNT, "9223372036854775808", "message.committed"
            )

    def test_new_events_must_advance_numerically(self):
        self.assertTrue(self.store.persist_event(
            ACCOUNT, "6678", "message.committed", GROUP, "m1"
        ))
        self.assertTrue(self.store.persist_event(
            ACCOUNT, "6680", "message.committed", GROUP, "m2"
        ))
        with self.assertRaisesRegex(ValueError, "advance"):
            self.store.persist_event(
                ACCOUNT, "6679", "message.committed", GROUP, "m3"
            )
        self.assertEqual(
            self.store.checkpoint(ACCOUNT)["last_durable_cursor"],
            "6680",
        )
        self.assertEqual(
            self.store.conn.execute(
                "SELECT COUNT(*) FROM events"
            ).fetchone()[0],
            2,
        )

    def test_exact_old_event_replay_is_idempotent_without_regression(self):
        self.store.persist_event(
            ACCOUNT, "6678", "message.committed", GROUP, "m1"
        )
        self.store.persist_event(
            ACCOUNT, "6680", "group.updated", GROUP
        )
        self.assertFalse(self.store.persist_event(
            ACCOUNT, "6678", "message.committed", GROUP, "m1"
        ))
        self.assertEqual(
            self.store.checkpoint(ACCOUNT)["last_durable_cursor"],
            "6680",
        )

    def test_same_cursor_with_different_payload_is_rejected(self):
        self.store.persist_event(
            ACCOUNT, "6678", "message.committed", GROUP, "m1"
        )
        with self.assertRaisesRegex(ValueError, "Conflicting event"):
            self.store.persist_event(
                ACCOUNT, "6678", "message.committed", GROUP, "different"
            )
        self.assertEqual(
            self.store.checkpoint(ACCOUNT)["last_durable_cursor"],
            "6678",
        )

    def test_ack_must_match_latest_durable_cursor(self):
        self.store.persist_event(
            ACCOUNT, "6678", "message.committed", GROUP, "m1"
        )
        self.assertTrue(self.store.mark_acked(ACCOUNT, "6678"))
        self.assertFalse(self.store.mark_acked(ACCOUNT, "6678"))

        self.store.persist_event(
            ACCOUNT, "6680", "message.committed", GROUP, "m2"
        )
        with self.assertRaises(ValueError):
            self.store.mark_acked(ACCOUNT, "6678")
        self.assertTrue(self.store.mark_acked(ACCOUNT, "6680"))

    def test_invalid_ack_cursor_is_rejected(self):
        with self.assertRaises(ValueError):
            self.store.mark_acked(ACCOUNT, "cursor-x")

    def test_incomplete_message_can_be_enriched(self):
        self.assertTrue(self.store.persist_message(
            ACCOUNT, "m1", GROUP,
            direction=None,
            body=None,
            sender_id=None,
            mentions_self=None,
            source_cursor=None,
        ))

        self.assertTrue(self.store.persist_message(
            ACCOUNT, "m1", GROUP,
            direction="incoming",
            body="1",
            sender_id="member-1",
            message_type=1,
            mentions_self=True,
            created_at="2026-11-21T01:00:00+08:00",
            source_cursor="6681",
        ))

        row = self.store.message(ACCOUNT, "m1")
        self.assertEqual(row["direction"], "incoming")
        self.assertEqual(row["body"], "1")
        self.assertEqual(row["sender_id"], "member-1")
        self.assertEqual(row["message_type"], 1)
        self.assertEqual(row["mentions_self"], 1)
        self.assertEqual(row["source_cursor"], "6681")

    def test_legacy_blank_body_can_be_enriched(self):
        self.store.conn.execute(
            "INSERT INTO messages "
            "(account_id,message_id,thread_id,direction,body,status) "
            "VALUES(?,?,?,?,?,?)",
            (ACCOUNT, "m1", GROUP, "incoming", "", "ignored"),
        )

        self.assertTrue(self.store.persist_message(
            ACCOUNT, "m1", GROUP,
            direction="incoming",
            body="later hydrated text",
            mentions_self=True,
        ))

        row = self.store.message(ACCOUNT, "m1")
        self.assertEqual(row["body"], "later hydrated text")
        self.assertEqual(row["mentions_self"], 1)
        # Storage enrichment must not alter the processing state.
        self.assertEqual(row["status"], "ignored")

    def test_enrichment_preserves_existing_message_status(self):
        self.store.persist_message(
            ACCOUNT, "m1", GROUP,
            direction=None, body=None, mentions_self=None,
        )
        self.store.conn.execute(
            "UPDATE messages SET status='processed' "
            "WHERE account_id=? AND message_id=?",
            (ACCOUNT, "m1"),
        )
        self.store.persist_message(
            ACCOUNT, "m1", GROUP,
            direction="incoming", body="hello", mentions_self=True,
        )
        self.assertEqual(
            self.store.message(ACCOUNT, "m1")["status"],
            "processed",
        )

    def test_exact_message_replay_is_noop(self):
        args = dict(
            direction="incoming",
            body="hello",
            sender_id="member-1",
            mentions_self=False,
            created_at="2026-11-21T01:00:00+08:00",
            source_cursor="6681",
        )
        self.assertTrue(self.store.persist_message(
            ACCOUNT, "m1", GROUP, **args
        ))
        self.assertFalse(self.store.persist_message(
            ACCOUNT, "m1", GROUP, **args
        ))

    def test_unknown_values_do_not_overwrite_known_values(self):
        self.store.persist_message(
            ACCOUNT, "m1", GROUP,
            direction="incoming", body="hello", mentions_self=True,
        )
        self.assertFalse(self.store.persist_message(
            ACCOUNT, "m1", GROUP,
            direction=None, body=None, mentions_self=None,
        ))
        row = self.store.message(ACCOUNT, "m1")
        self.assertEqual(row["body"], "hello")
        self.assertEqual(row["mentions_self"], 1)

    def test_conflicting_body_is_rejected_and_original_preserved(self):
        self.store.persist_message(
            ACCOUNT, "m1", GROUP,
            direction="incoming", body="hello",
        )
        with self.assertRaisesRegex(ValueError, "body"):
            self.store.persist_message(
                ACCOUNT, "m1", GROUP,
                direction="incoming", body="different",
            )
        self.assertEqual(
            self.store.message(ACCOUNT, "m1")["body"],
            "hello",
        )

    def test_conflicting_mention_state_is_rejected(self):
        self.store.persist_message(
            ACCOUNT, "m1", GROUP,
            direction="incoming", body="1", mentions_self=True,
        )
        with self.assertRaisesRegex(ValueError, "mentions_self"):
            self.store.persist_message(
                ACCOUNT, "m1", GROUP,
                direction="incoming", body="1", mentions_self=False,
            )

    def test_conflicting_thread_is_rejected(self):
        self.store.persist_message(
            ACCOUNT, "m1", GROUP,
            direction="incoming", body="hello",
        )
        with self.assertRaisesRegex(ValueError, "thread_id"):
            self.store.persist_message(
                ACCOUNT, "m1", "other-group",
                direction="incoming", body="hello",
            )

    def test_mention_flag_requires_actual_bool(self):
        with self.assertRaises(ValueError):
            self.store.persist_message(
                ACCOUNT, "m1", GROUP,
                direction="incoming", body="1", mentions_self=1,
            )

    def test_message_source_cursor_uses_sse_cursor_rules(self):
        with self.assertRaises(ValueError):
            self.store.persist_message(
                ACCOUNT, "m1", GROUP,
                direction="incoming", body="hello",
                source_cursor="cursor-x",
            )


if __name__ == "__main__":
    unittest.main()
