"""P6.5D2D-1 regression: real SQLite read-only snapshot, no network or send."""

from contextlib import closing
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from test_storage import V2_DDL
from wechat_ai_bot.migrations import migrate_v2_to_v3, migrate_v3_to_v4
from wechat_ai_bot.sender_dry_run import (
    DryRunError, inspect_snapshot, main, observe_worker_service,
)
from wechat_ai_bot.storage import Storage


class SenderDryRunTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="wechat-dryrun-test-")
        self.dir = Path(self.tmp.name)
        self.db = self.dir / "outbox.sqlite3"
        with closing(sqlite3.connect(self.db)) as conn, conn:
            conn.executescript(V2_DDL)
            migrate_v2_to_v3(conn)
            migrate_v3_to_v4(conn)
        self.store = Storage(self.db)
        self.config = {
            "schema_version": 2,
            "storage": {"database": "outbox.sqlite3"},
            "app": {"auto_send": False},
            "sender": {"enabled": False},
            "runtime": {"global_pause": True},
            "agent_wechat": {"account_id": "test-acct", "target_group_id": "test-group"},
            "safety": {"group_allowlist": ["test-group"]},
            # The diagnostic must never open or read this nonexistent token.
            "token_file": "/should/never/be/read/sender-api-token",
        }
        self.config_path = self.dir / "config.json"
        self.now = datetime.now(timezone.utc) + timedelta(seconds=3)
        self.serial = 0
        self.write_config()

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def write_config(self):
        self.config_path.write_text(json.dumps(self.config), encoding="utf-8")

    def queue_ai(self, age=10):
        self.serial += 1
        mid = "source-" + str(self.serial)
        reply = "synthetic reply " + mid
        self.store.persist_message(
            "test-acct", mid, "test-group", direction="incoming",
            body="synthetic", source_cursor=str(self.serial),
            created_at=(self.now-timedelta(seconds=age)).isoformat(),
        )
        decision, _ = self.store.decide(
            "test-acct", mid, "reply", model="synthetic", reply=reply,
        )
        self.assertTrue(self.store.accept_decision(decision, now=self.now))
        outbox_id, created = self.store.enqueue(
            decision, "test-acct", "test-group", reply, now=self.now,
        )
        self.assertTrue(created)
        return outbox_id

    def snapshot(self, **kwargs):
        return inspect_snapshot(self.config_path, now=self.now, **kwargs)

    def test_no_candidates_fails_closed_and_reports_gate_reasons(self):
        report = self.snapshot()
        self.assertEqual(report["verdict"], "NO_CANDIDATES")
        self.assertEqual(report["queued_target_count"], 0)
        self.assertEqual(report["config_blockers"], [
            "auto_send_not_enabled", "sender_not_enabled", "global_pause_not_disabled",
        ])
        self.assertFalse(report["live_send_authorized"])
        self.assertEqual((report["network_requests"], report["sql_writes"]), (0, 0))

    def test_queued_candidate_denied_by_real_config(self):
        self.queue_ai()
        report = self.snapshot()
        self.assertEqual(report["verdict"], "BLOCKED")
        self.assertEqual(report["queued_target_count"], 1)
        self.assertEqual(report["queued_sample"][0]["reason"],
                         "sending_paused_or_target_mismatch")
        self.assertNotIn("synthetic reply", json.dumps(report))
        self.assertNotIn("test-acct", json.dumps(report))
        self.assertNotIn("test-group", json.dumps(report))
        self.assertNotIn("idempotency", json.dumps(report))

    def test_policy_pass_only_cannot_authorize_any_send(self):
        self.queue_ai()
        self.config["app"]["auto_send"] = True
        self.config["sender"]["enabled"] = True
        self.config["runtime"]["global_pause"] = False
        self.write_config()
        report = self.snapshot()
        self.assertEqual(report["verdict"], "POLICY_PASS_ONLY")
        self.assertFalse(report["live_send_authorized"])
        self.assertEqual(report["queued_sample"][0]["reason"], "none")

    def test_unresolved_sending_has_priority_over_queue(self):
        first = self.queue_ai()
        self.store.conn.execute(
            "UPDATE outbox SET status='sending' WHERE outbox_id=?", (first,)
        )
        self.queue_ai()
        report = self.snapshot()
        self.assertEqual(report["verdict"], "UNRESOLVED_BLOCK")
        self.assertEqual(report["unresolved_target_count"], 1)
        self.assertEqual(report["target_outbox_status_counts"]["sending"], 1)

    def test_needs_review_without_candidates_is_not_no_candidates(self):
        first = self.queue_ai()
        self.store.conn.execute(
            "UPDATE outbox SET status='needs_review' WHERE outbox_id=?", (first,)
        )
        self.assertEqual(self.snapshot()["verdict"], "UNRESOLVED_BLOCK")

    def test_sample_truncated_is_explicit(self):
        self.queue_ai()
        self.queue_ai()
        report = self.snapshot(sample_limit=1)
        self.assertTrue(report["sample_truncated"])
        self.assertEqual(len(report["queued_sample"]), 1)
        self.assertEqual(report["queued_target_count"], 2)
        self.assertFalse(report["live_send_authorized"])

    def test_expired_candidate_policy_block_with_hypothetical_config(self):
        self.queue_ai(age=10)
        self.config["app"]["auto_send"] = True
        self.config["sender"]["enabled"] = True
        self.config["runtime"]["global_pause"] = False
        self.write_config()
        report = inspect_snapshot(
            self.config_path, now=self.now + timedelta(seconds=121),
        )
        self.assertEqual(report["verdict"], "BLOCKED")
        self.assertEqual(report["queued_sample"][0]["reason"],
                         "reply_expired_or_invalid")

    def test_no_modify_outbox_or_config(self):
        self.queue_ai()
        prior_db = self.store.conn.execute(
            "SELECT outbox_id,status,remote_send_id,sent_at FROM outbox"
        ).fetchall()
        config_before = self.config_path.read_bytes()
        self.snapshot()
        after_db = self.store.conn.execute(
            "SELECT outbox_id,status,remote_send_id,sent_at FROM outbox"
        ).fetchall()
        self.assertEqual([tuple(x) for x in prior_db], [tuple(x) for x in after_db])
        self.assertEqual(config_before, self.config_path.read_bytes())

    def test_database_absent_does_not_create_a_new_file(self):
        self.config["storage"]["database"] = "absent.sqlite3"
        self.write_config()
        with self.assertRaisesRegex(DryRunError, "DATABASE_UNAVAILABLE"):
            self.snapshot()
        self.assertFalse((self.dir / "absent.sqlite3").exists())

    def test_non_v4_database_refused(self):
        self.store.close()
        with closing(sqlite3.connect(self.db)) as conn, conn:
            conn.execute("PRAGMA user_version=3")
        with self.assertRaisesRegex(DryRunError, "DATABASE_SCHEMA_NOT_V4"):
            self.snapshot()

    def test_foreign_group_not_misreported_as_target_candidate(self):
        self.queue_ai()
        self.config["agent_wechat"]["target_group_id"] = "another-group"
        self.config["safety"]["group_allowlist"] = ["another-group"]
        self.write_config()
        report = self.snapshot()
        self.assertEqual(report["verdict"], "NO_CANDIDATES")
        self.assertEqual(report["other_account_or_group_outbox_count"], 1)
        self.assertEqual(report["queued_target_count"], 0)

    def test_wal_snapshot_sees_recent_committed_row(self):
        # immutable=1 would risk ignoring committed WAL frames. mode=ro must
        # inspect the current committed snapshot without touching Outbox.
        journal = self.store.conn.execute("PRAGMA journal_mode=WAL").fetchone()[0]
        self.assertEqual(journal.lower(), "wal")
        outbox = self.queue_ai()
        report = self.snapshot()
        self.assertEqual(report["queued_target_count"], 1)
        self.assertEqual(self.store.conn.execute(
            "SELECT status FROM outbox WHERE outbox_id=?", (outbox,)
        ).fetchone()[0], "queued")

    def test_reject_invalid_sample_limit(self):
        with self.assertRaisesRegex(DryRunError, "INVALID_SAMPLE_LIMIT"):
            self.snapshot(sample_limit=0)

    def test_cli_cannot_use_http_or_token(self):
        with patch("socket.socket.connect", side_effect=AssertionError("network")):
            with patch("wechat_ai_bot.sender_dry_run.observe_worker_service",
                       return_value={"active": "inactive", "unit_file_state": "disabled",
                                     "expected_safe_state": True}):
                with patch("builtins.print") as printing:
                    rc = main(["--config", str(self.config_path)])
        self.assertEqual(rc, 0)
        self.assertTrue(printing.called)

    def test_worker_service_unknown_is_fail_closed(self):
        with patch("wechat_ai_bot.sender_dry_run.subprocess.run",
                   side_effect=OSError("systemctl unavailable")):
            result = observe_worker_service()
        self.assertFalse(result["expected_safe_state"])
        self.assertEqual(result["active"], "unknown")

    def test_worker_status_uses_the_selected_systemd_manager(self):
        for isolated, manager, name, unit in (
            (False, "--user", "wechat-bot-worker.service", "disabled"),
            (True, "--system", "wechat-bot-observer-worker.service", "static"),
        ):
            status = f"LoadState=loaded\nActiveState=inactive\nUnitFileState={unit}\nMainPID=0\n"
            with self.subTest(unit=name), patch(
                "wechat_ai_bot.sender_dry_run.subprocess.run",
                return_value=subprocess.CompletedProcess([], 0, status),
            ) as query:
                result = observe_worker_service(isolated=isolated)
                self.assertEqual(query.call_args.args[0][:4], ["systemctl", manager, "show", name])
                self.assertTrue(result["expected_safe_state"])

    def test_isolated_worker_must_be_loaded_and_stopped(self):
        for status in (
            "LoadState=not-found\nActiveState=inactive\nUnitFileState=static\nMainPID=0\n",
            "LoadState=loaded\nActiveState=inactive\nUnitFileState=static\nMainPID=12\n",
            "LoadState=loaded\nActiveState=active\nUnitFileState=static\nMainPID=12\n",
            "LoadState=loaded\nActiveState=inactive\nUnitFileState=enabled\nMainPID=0\n",
        ):
            with self.subTest(status=status), patch(
                "wechat_ai_bot.sender_dry_run.subprocess.run",
                return_value=subprocess.CompletedProcess([], 0, status),
            ):
                self.assertFalse(observe_worker_service(isolated=True)["expected_safe_state"])

    def test_cli_selects_the_isolated_worker(self):
        with patch("wechat_ai_bot.sender_dry_run.observe_worker_service") as worker, patch("builtins.print"):
            worker.return_value = {"expected_safe_state": True}
            self.assertEqual(main(["--config", str(self.config_path), "--isolated-worker"]), 0)
        worker.assert_called_once_with(isolated=True)


if __name__ == "__main__":
    unittest.main()
