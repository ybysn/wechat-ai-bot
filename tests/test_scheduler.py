import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from wechat_ai_bot.migrations import migrate_v2_to_v3, migrate_v3_to_v4
from wechat_ai_bot.scheduler import daily_checkin_tick
from wechat_ai_bot.storage import Storage


ACCOUNT = "a"
GROUP = "target@chatroom"

V2_DDL = """
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
CREATE INDEX idx_outbox_status ON outbox(status, created_at);
CREATE TABLE schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO schema_migrations(version) VALUES(2);
PRAGMA user_version=2;
"""


def base_config():
    return {
        "schema_version": 2,
        "app": {"auto_send": False},
        "sender": {"enabled": False},
        "runtime": {"global_pause": True},
        "agent_wechat": {
            "account_id": ACCOUNT,
            "target_group_id": GROUP,
        },
        "safety": {"group_allowlist": [GROUP]},
    }


class SchedulerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "scheduler.sqlite3"
        conn = sqlite3.connect(self.db, isolation_level=None)
        try:
            conn.execute("PRAGMA foreign_keys=ON")
            conn.executescript(V2_DDL)
            migrate_v2_to_v3(conn)
            migrate_v3_to_v4(conn)
        finally:
            conn.close()
        self.store = Storage(self.db)

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def test_paused_tick_materializes_task_without_outbox(self):
        cfg = base_config()
        now = datetime(2026, 11, 21, 1, 0, tzinfo=timezone.utc)  # 09:00 CST
        result = daily_checkin_tick(self.store, cfg, now)
        self.assertEqual(result["status"], "paused")
        self.assertEqual(
            tuple(self.store.conn.execute(
                "SELECT content,timezone,scheduled_at,status FROM scheduled_tasks"
            ).fetchone()),
            ("签到", "Asia/Shanghai", "2026-11-21T09:00:00+08:00", "scheduled"),
        )
        self.assertEqual(
            self.store.conn.execute("SELECT COUNT(*) FROM outbox").fetchone()[0], 0
        )

    def test_before_nine_is_not_due(self):
        cfg = base_config()
        cfg["app"]["auto_send"] = True
        cfg["sender"]["enabled"] = True
        cfg["runtime"]["global_pause"] = False
        now = datetime(2026, 11, 21, 0, 59, tzinfo=timezone.utc)  # 08:59 CST
        result = daily_checkin_tick(self.store, cfg, now, queue_enabled=True)
        self.assertEqual(result["status"], "not_due")
        self.assertEqual(
            self.store.conn.execute("SELECT COUNT(*) FROM outbox").fetchone()[0], 0
        )

    def test_due_tick_queues_exact_fixed_content(self):
        cfg = base_config()
        cfg["app"]["auto_send"] = True
        cfg["sender"]["enabled"] = True
        cfg["runtime"]["global_pause"] = False
        now = datetime(2026, 11, 21, 1, 0, tzinfo=timezone.utc)
        result = daily_checkin_tick(self.store, cfg, now, queue_enabled=True)
        self.assertEqual(result["status"], "queued")
        self.assertTrue(result["queued"])
        row = self.store.conn.execute(
            "SELECT decision_id,scheduled_task_id,content,idempotency_key,status FROM outbox"
        ).fetchone()
        self.assertIsNone(row["decision_id"])
        self.assertIsNotNone(row["scheduled_task_id"])
        self.assertEqual(row["content"], "签到")
        self.assertEqual(row["status"], "queued")
        task = self.store.conn.execute(
            "SELECT status FROM scheduled_tasks"
        ).fetchone()
        self.assertEqual(task["status"], "queued")

    def test_repeated_tick_is_idempotent(self):
        cfg = base_config()
        cfg["app"]["auto_send"] = True
        cfg["sender"]["enabled"] = True
        cfg["runtime"]["global_pause"] = False
        now = datetime(2026, 11, 21, 2, 0, tzinfo=timezone.utc)
        first = daily_checkin_tick(self.store, cfg, now, queue_enabled=True)
        second = daily_checkin_tick(self.store, cfg, now, queue_enabled=True)
        self.assertEqual(first["outbox_id"], second["outbox_id"])
        self.assertTrue(first["queued"])
        self.assertFalse(second["queued"])
        self.assertEqual(
            self.store.conn.execute("SELECT COUNT(*) FROM scheduled_tasks").fetchone()[0], 1
        )
        self.assertEqual(
            self.store.conn.execute("SELECT COUNT(*) FROM outbox").fetchone()[0], 1
        )

    def test_previous_day_scheduled_task_expires(self):
        self.store.ensure_daily_checkin_task(
            ACCOUNT, GROUP, "2026-11-20"
        )
        cfg = base_config()
        now = datetime(2026, 11, 21, 0, 0, tzinfo=timezone.utc)  # 08:00 CST
        result = daily_checkin_tick(self.store, cfg, now)
        self.assertEqual(result["expired_count"], 1)
        rows = self.store.conn.execute(
            "SELECT local_date,status FROM scheduled_tasks ORDER BY local_date"
        ).fetchall()
        self.assertEqual(
            [tuple(row) for row in rows],
            [("2026-11-20", "expired"), ("2026-11-21", "scheduled")],
        )

    def test_previous_day_queued_checkin_is_cancelled_and_expired(self):
        task_id, _ = self.store.ensure_daily_checkin_task(
            ACCOUNT, GROUP, "2026-11-20"
        )
        outbox_id, created, status = self.store.enqueue_scheduled_task(
            task_id,
            datetime(2026, 11, 20, 2, 0, tzinfo=timezone.utc),
        )
        self.assertTrue(created)
        self.assertEqual(status, "queued")
        expired = self.store.expire_stale_scheduled_tasks(
            ACCOUNT, GROUP, "2026-11-21"
        )
        self.assertEqual(expired, 1)
        self.assertEqual(
            self.store.conn.execute(
                "SELECT status FROM scheduled_tasks WHERE task_id=?", (task_id,)
            ).fetchone()[0],
            "expired",
        )
        self.assertEqual(
            self.store.conn.execute(
                "SELECT status FROM outbox WHERE outbox_id=?", (outbox_id,)
            ).fetchone()[0],
            "cancelled",
        )

    def test_in_flight_send_is_not_expired_at_day_boundary(self):
        task_id, _ = self.store.ensure_daily_checkin_task(
            ACCOUNT, GROUP, "2026-11-20"
        )
        outbox_id, created, status = self.store.enqueue_scheduled_task(
            task_id,
            datetime(2026, 11, 20, 2, 0, tzinfo=timezone.utc),
        )
        self.assertTrue(created)
        self.store.conn.execute(
            "UPDATE outbox SET status='sending' WHERE outbox_id=?",
            (outbox_id,),
        )
        expired = self.store.expire_stale_scheduled_tasks(
            ACCOUNT, GROUP, "2026-11-21"
        )
        self.assertEqual(expired, 0)
        self.assertEqual(
            self.store.conn.execute(
                "SELECT status FROM scheduled_tasks WHERE task_id=?", (task_id,)
            ).fetchone()[0],
            "queued",
        )

    def test_non_allowlisted_group_rejected(self):
        cfg = base_config()
        cfg["safety"]["group_allowlist"] = []
        now = datetime(2026, 11, 21, 1, 0, tzinfo=timezone.utc)
        with self.assertRaises(RuntimeError):
            daily_checkin_tick(self.store, cfg, now)
        self.assertEqual(
            self.store.conn.execute("SELECT COUNT(*) FROM scheduled_tasks").fetchone()[0], 0
        )

    def test_naive_datetime_rejected(self):
        task_id, _ = self.store.ensure_daily_checkin_task(
            ACCOUNT, GROUP, "2026-11-21"
        )
        with self.assertRaises(ValueError):
            self.store.enqueue_scheduled_task(
                task_id, datetime(2026, 11, 21, 9, 0)
            )


if __name__ == "__main__":
    unittest.main()
