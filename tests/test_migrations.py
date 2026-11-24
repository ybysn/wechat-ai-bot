import sqlite3
import unittest

from wechat_ai_bot.migrations import (
    backup_and_migrate_v3_to_v4, migrate_v2_to_v3, migrate_v3_to_v4,
)


V2_DDL = """
CREATE TABLE messages (
    account_id TEXT NOT NULL,
    message_id TEXT NOT NULL,
    thread_id TEXT NOT NULL,
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
CREATE INDEX idx_outbox_status ON outbox(status, created_at);
CREATE TABLE schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO schema_migrations(version) VALUES(2);
PRAGMA user_version=2;
"""


class MigrationTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:", isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.executescript(V2_DDL)

    def tearDown(self):
        self.conn.close()

    def test_migrates_v2_to_v3(self):
        self.assertTrue(migrate_v2_to_v3(self.conn))
        self.assertEqual(
            self.conn.execute("PRAGMA user_version").fetchone()[0], 3
        )
        columns = {
            row[1]
            for row in self.conn.execute(
                "PRAGMA table_info(decisions)"
            )
        }
        self.assertIn("reply", columns)
        self.assertEqual(
            self.conn.execute(
                "SELECT COUNT(*) FROM schema_migrations WHERE version=3"
            ).fetchone()[0],
            1,
        )

    def test_backfills_reply_from_existing_outbox(self):
        self.conn.execute(
            "INSERT INTO messages(account_id,message_id,thread_id) "
            "VALUES('a','m1','g')"
        )
        self.conn.execute(
            "INSERT INTO decisions "
            "(decision_id,account_id,source_message_id,action,status) "
            "VALUES('d1','a','m1','reply','accepted')"
        )
        self.conn.execute(
            "INSERT INTO outbox "
            "(outbox_id,decision_id,account_id,thread_id,content,idempotency_key) "
            "VALUES('o1','d1','a','g','历史回复','k1')"
        )

        migrate_v2_to_v3(self.conn)

        self.assertEqual(
            self.conn.execute(
                "SELECT reply FROM decisions WHERE decision_id='d1'"
            ).fetchone()[0],
            "历史回复",
        )

    def test_migration_is_idempotent(self):
        self.assertTrue(migrate_v2_to_v3(self.conn))
        self.assertFalse(migrate_v2_to_v3(self.conn))
        self.assertEqual(
            self.conn.execute(
                "SELECT COUNT(*) FROM schema_migrations WHERE version=3"
            ).fetchone()[0],
            1,
        )

    def test_reply_decision_requires_body(self):
        migrate_v2_to_v3(self.conn)
        self.conn.execute(
            "INSERT INTO messages(account_id,message_id,thread_id) "
            "VALUES('a','m1','g')"
        )
        with self.assertRaises(sqlite3.IntegrityError):
            self.conn.execute(
                "INSERT INTO decisions "
                "(decision_id,account_id,source_message_id,action) "
                "VALUES('d1','a','m1','reply')"
            )

    def test_outbox_must_match_accepted_reply(self):
        migrate_v2_to_v3(self.conn)
        self.conn.execute(
            "INSERT INTO messages(account_id,message_id,thread_id) "
            "VALUES('a','m1','g')"
        )
        self.conn.execute(
            "INSERT INTO decisions "
            "(decision_id,account_id,source_message_id,action,status,reply) "
            "VALUES('d1','a','m1','reply','accepted','正确回复')"
        )

        with self.assertRaises(sqlite3.IntegrityError):
            self.conn.execute(
                "INSERT INTO outbox "
                "(outbox_id,decision_id,account_id,thread_id,content,idempotency_key) "
                "VALUES('o1','d1','a','g','错误回复','k1')"
            )

        self.conn.execute(
            "INSERT INTO outbox "
            "(outbox_id,decision_id,account_id,thread_id,content,idempotency_key) "
            "VALUES('o2','d1','a','g','正确回复','k2')"
        )

    def test_skip_cannot_store_reply(self):
        migrate_v2_to_v3(self.conn)
        self.conn.execute(
            "INSERT INTO messages(account_id,message_id,thread_id) "
            "VALUES('a','m1','g')"
        )
        with self.assertRaises(sqlite3.IntegrityError):
            self.conn.execute(
                "INSERT INTO decisions "
                "(decision_id,account_id,source_message_id,action,reply) "
                "VALUES('d1','a','m1','skip','不应该有正文')"
            )


    def _migrate_to_v3(self):
        migrate_v2_to_v3(self.conn)

    def test_migrates_v3_to_v4_and_rebuilds_outbox(self):
        self._migrate_to_v3()
        self.assertTrue(migrate_v3_to_v4(self.conn))
        self.assertEqual(
            self.conn.execute("PRAGMA user_version").fetchone()[0], 4
        )
        outbox_columns = {
            row[1] for row in self.conn.execute("PRAGMA table_info(outbox)")
        }
        self.assertIn("scheduled_task_id", outbox_columns)
        self.assertIn("decision_id", outbox_columns)
        self.assertIn("scheduled_tasks", {
            row[0] for row in self.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        })
        self.assertEqual(
            self.conn.execute("PRAGMA integrity_check").fetchone()[0], "ok"
        )
        self.assertEqual(
            self.conn.execute("PRAGMA foreign_key_check").fetchall(), []
        )

    def test_v4_preserves_existing_outbox_rows(self):
        self._migrate_to_v3()
        self.conn.execute(
            "INSERT INTO messages(account_id,message_id,thread_id) VALUES('a','m1','g')"
        )
        self.conn.execute(
            "INSERT INTO decisions(decision_id,account_id,source_message_id,action,status,reply) "
            "VALUES('d1','a','m1','reply','accepted','历史回复')"
        )
        self.conn.execute(
            "INSERT INTO outbox(outbox_id,decision_id,account_id,thread_id,content,idempotency_key) "
            "VALUES('o1','d1','a','g','历史回复','key1')"
        )
        self.assertTrue(migrate_v3_to_v4(self.conn))
        row = self.conn.execute(
            "SELECT decision_id,scheduled_task_id,content,status FROM outbox WHERE outbox_id='o1'"
        ).fetchone()
        self.assertEqual(tuple(row), ('d1', None, '历史回复', 'queued'))

    def test_v4_migration_is_idempotent(self):
        self._migrate_to_v3()
        self.assertTrue(migrate_v3_to_v4(self.conn))
        self.assertFalse(migrate_v3_to_v4(self.conn))
        self.assertEqual(
            self.conn.execute(
                "SELECT COUNT(*) FROM schema_migrations WHERE version=4"
            ).fetchone()[0], 1
        )

    def test_v4_outbox_requires_exactly_one_source(self):
        self._migrate_to_v3()
        migrate_v3_to_v4(self.conn)
        with self.assertRaises(sqlite3.IntegrityError):
            self.conn.execute(
                "INSERT INTO outbox(outbox_id,account_id,thread_id,content,idempotency_key) "
                "VALUES('bad','a','g','x','bad-key')"
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.conn.execute(
                "INSERT INTO outbox(outbox_id,decision_id,scheduled_task_id,account_id,thread_id,content,idempotency_key) "
                "VALUES('bad2','d','t','a','g','x','bad-key2')"
            )

    def test_v4_scheduled_task_outbox_must_match(self):
        self._migrate_to_v3()
        migrate_v3_to_v4(self.conn)
        self.conn.execute(
            "INSERT INTO scheduled_tasks(task_id,task_type,account_id,thread_id,local_date,timezone,scheduled_at,content,idempotency_key) "
            "VALUES('t1','daily_checkin','a','g','2026-11-21','Asia/Shanghai','2026-11-21T09:00:00+08:00','签到','checkin-key')"
        )
        with self.assertRaises(sqlite3.IntegrityError):
            self.conn.execute(
                "INSERT INTO outbox(outbox_id,decision_id,scheduled_task_id,account_id,thread_id,content,idempotency_key) "
                "VALUES('o-bad',NULL,'t1','a','g','错误内容','send-bad')"
            )
        self.conn.execute(
            "INSERT INTO outbox(outbox_id,decision_id,scheduled_task_id,account_id,thread_id,content,idempotency_key) "
            "VALUES('o-good',NULL,'t1','a','g','签到','send-good')"
        )
        self.conn.execute(
            "UPDATE scheduled_tasks SET status='queued' WHERE task_id='t1'"
        )
        with self.assertRaises(sqlite3.IntegrityError):
            self.conn.execute(
                "UPDATE scheduled_tasks SET content='别的内容' WHERE task_id='t1'"
            )

    def test_backup_and_migrate_v3_to_v4(self):
        import os
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / "bot.sqlite3"
            source = sqlite3.connect(db_path, isolation_level=None)
            try:
                source.execute("PRAGMA foreign_keys=ON")
                source.executescript(V2_DDL)
                migrate_v2_to_v3(source)
            finally:
                source.close()

            backup = backup_and_migrate_v3_to_v4(db_path)
            self.assertIsNotNone(backup)
            self.assertTrue(backup.exists())
            self.assertEqual(os.stat(backup).st_mode & 0o777, 0o600)

            with sqlite3.connect(db_path) as migrated:
                self.assertEqual(
                    migrated.execute("PRAGMA user_version").fetchone()[0], 4
                )
            with sqlite3.connect(backup) as saved:
                self.assertEqual(
                    saved.execute("PRAGMA user_version").fetchone()[0], 3
                )

    def test_v4_installs_eight_integrity_triggers(self):
        self._migrate_to_v3()
        migrate_v3_to_v4(self.conn)
        names = {
            row[0] for row in self.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='trigger'"
            )
        }
        expected = {
            "trg_decisions_reply_insert",
            "trg_decisions_reply_update",
            "trg_outbox_reply_insert",
            "trg_outbox_reply_update",
            "trg_decision_reply_immutable_after_outbox",
            "trg_scheduled_task_insert_guard",
            "trg_scheduled_task_immutable_after_outbox",
            "trg_scheduled_task_status_transition",
        }
        self.assertTrue(expected.issubset(names))

    def test_v4_scheduled_task_status_transition_is_guarded(self):
        self._migrate_to_v3()
        migrate_v3_to_v4(self.conn)
        self.conn.execute(
            "INSERT INTO scheduled_tasks(task_id,task_type,account_id,thread_id,local_date,timezone,scheduled_at,content,idempotency_key) "
            "VALUES('t1','daily_checkin','a','g','2026-11-21','Asia/Shanghai','2026-11-21T09:00:00+08:00','签到','k1')"
        )
        with self.assertRaises(sqlite3.IntegrityError):
            self.conn.execute(
                "UPDATE scheduled_tasks SET status='sent' WHERE task_id='t1'"
            )

    def test_v4_scheduled_task_rejects_wrong_time_or_content(self):
        self._migrate_to_v3()
        migrate_v3_to_v4(self.conn)
        with self.assertRaises(sqlite3.IntegrityError):
            self.conn.execute(
                "INSERT INTO scheduled_tasks(task_id,task_type,account_id,thread_id,local_date,timezone,scheduled_at,content,idempotency_key) "
                "VALUES('t1','daily_checkin','a','g','2026-11-21','Asia/Shanghai','2026-11-21T10:00:00+08:00','签到','k1')"
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.conn.execute(
                "INSERT INTO scheduled_tasks(task_id,task_type,account_id,thread_id,local_date,timezone,scheduled_at,content,idempotency_key) "
                "VALUES('t2','daily_checkin','a','g','2026-11-21','Asia/Shanghai','2026-11-21T09:00:00+08:00','早上好','k2')"
            )


if __name__ == "__main__":
    unittest.main()
