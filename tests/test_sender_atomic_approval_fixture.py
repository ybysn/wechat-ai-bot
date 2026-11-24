"""D2D-2B2 reconstruction: offline transaction, crash and concurrency tests."""

from contextlib import closing
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

from wechat_ai_bot.sender_approval_fixture import ApprovalFailure, SyntheticIntent
from wechat_ai_bot.sender_atomic_approval_fixture import SyntheticAtomicApprovalFixture
from wechat_ai_bot.sender_http_fixture import (
    FIXTURE_ACCOUNT, FIXTURE_THREAD, FIXTURE_TEXT_PREFIX,
)


_CRASH_SCRIPT = """
from datetime import datetime
import os
from pathlib import Path
import sys
from wechat_ai_bot.sender_approval_fixture import SyntheticIntent
from wechat_ai_bot.sender_atomic_approval_fixture import SyntheticAtomicApprovalFixture
from wechat_ai_bot.sender_http_fixture import (
    FIXTURE_ACCOUNT, FIXTURE_THREAD, FIXTURE_TEXT_PREFIX,
)

root, grant, clock, phase = sys.argv[1:]
fixture = SyntheticAtomicApprovalFixture(Path(root))
intent = SyntheticIntent(FIXTURE_ACCOUNT, FIXTURE_THREAD, 'fixture-out-atomic',
                         'fixture-key-atomic', FIXTURE_TEXT_PREFIX + 'atomic hello')
if phase == 'before_commit':
    def crash():
        os._exit(73)
    fixture._ledger.conn.create_function('fixture_crash', 0, crash)
    fixture._ledger.conn.execute('''
        CREATE TEMP TRIGGER crash_between_updates
        BEFORE UPDATE OF status ON fixture_outbox
        BEGIN SELECT fixture_crash(); END
    ''')
result = fixture.claim_once(grant, intent, now=datetime.fromisoformat(clock))
if result['verdict'] != 'FIXTURE_CLAIMED':
    raise RuntimeError('FIXTURE_CLAIM_FAILED')
os._exit(74)
"""


class AtomicApprovalFixtureTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="p65d2d2b-fixture-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.database = self.root / "synthetic-approvals.sqlite3"
        self.fixture = SyntheticAtomicApprovalFixture(self.root)
        self.addCleanup(self.fixture.close)
        self.now = datetime(2026, 11, 22, 2, 0, tzinfo=timezone.utc)
        self.intent = SyntheticIntent(
            FIXTURE_ACCOUNT, FIXTURE_THREAD, "fixture-out-atomic",
            "fixture-key-atomic", FIXTURE_TEXT_PREFIX + "atomic hello",
        )

    def seed(self, *, ttl_seconds=60):
        self.fixture.seed_synthetic_outbox(self.intent)
        return self.fixture.seed_synthetic_approval(
            self.intent, now=self.now, ttl_seconds=ttl_seconds,
        )

    def states(self, grant):
        # Observe committed state through a different, read-only connection.
        with closing(sqlite3.connect(
            self.database.as_uri() + "?mode=ro", uri=True, isolation_level=None,
        )) as conn:
            conn.execute("BEGIN")
            approval = conn.execute(
                "SELECT state FROM fixture_approval WHERE grant_id=?", (grant,),
            ).fetchone()
            outbox = conn.execute(
                "SELECT status FROM fixture_outbox WHERE outbox_id=?",
                (self.intent.outbox_id,),
            ).fetchone()
            return (approval[0] if approval else None,
                    outbox[0] if outbox else None)

    def claim(self, grant, *, intent=None, now=None, fixture=None):
        return (fixture or self.fixture).claim_once(
            grant, intent or self.intent, now=now or self.now,
        )["verdict"]

    def crash_child(self, grant, phase):
        project = Path(__file__).resolve().parents[1]
        return subprocess.run(
            [sys.executable, "-c", _CRASH_SCRIPT, str(self.root), grant,
             self.now.isoformat(), phase],
            cwd=project,
            env={**os.environ, "PYTHONPATH": str(project / "src"),
                 "PYTHONDONTWRITEBYTECODE": "1"},
            capture_output=True, text=True, timeout=10,
        )

    def test_success_commits_both_rows_without_live_authority(self):
        grant = self.seed()
        self.assertEqual(self.states(grant), ("pending", "queued"))
        report = self.fixture.claim_once(grant, self.intent, now=self.now)
        self.assertEqual(report["verdict"], "FIXTURE_CLAIMED")
        self.assertTrue(report["fixture_claimed"])
        self.assertFalse(report["live_send_authorized"])
        self.assertFalse(report["token_read"])
        self.assertEqual(report["network_requests"], 0)
        self.assertEqual(self.states(grant), ("spent", "sending"))

    def test_second_claim_cannot_reuse_grant(self):
        grant = self.seed()
        self.assertEqual(self.claim(grant), "FIXTURE_CLAIMED")
        self.assertEqual(self.claim(grant), "GRANT_SPENT")
        self.assertFalse(self.fixture.revoke(grant))
        self.assertEqual(self.states(grant), ("spent", "sending"))

    def test_reopen_preserves_committed_reservation(self):
        grant = self.seed()
        self.claim(grant)
        with SyntheticAtomicApprovalFixture(self.root) as reopened:
            self.assertEqual(self.claim(grant, fixture=reopened), "GRANT_SPENT")
        self.assertEqual(self.states(grant), ("spent", "sending"))

    def test_missing_grant_leaves_outbox_queued(self):
        self.fixture.seed_synthetic_outbox(self.intent)
        grant = "fixture-grant-missing"
        self.assertEqual(self.claim(grant), "GRANT_NOT_FOUND")
        self.assertEqual(self.states(grant), (None, "queued"))

    def test_missing_outbox_leaves_grant_pending(self):
        grant = self.fixture.seed_synthetic_approval(self.intent, now=self.now)
        self.assertEqual(self.claim(grant), "OUTBOX_MISSING")
        self.assertEqual(self.states(grant), ("pending", None))

    def test_scope_mismatch_never_consumes_grant(self):
        grant = self.seed()
        for changed in (
            replace(self.intent, outbox_id="fixture-other-outbox"),
            replace(self.intent, idempotency_key="fixture-other-key"),
            replace(self.intent, text=self.intent.text + " changed"),
        ):
            with self.subTest(field=changed):
                self.assertEqual(self.claim(grant, intent=changed), "SCOPE_MISMATCH")
                self.assertEqual(self.states(grant), ("pending", "queued"))

    def test_revocation_survives_reopen_and_prevents_claim(self):
        grant = self.seed()
        self.assertTrue(self.fixture.revoke(grant))
        with SyntheticAtomicApprovalFixture(self.root) as reopened:
            self.assertEqual(self.claim(grant, fixture=reopened), "GRANT_REVOKED")
        self.assertEqual(self.states(grant), ("revoked", "queued"))

    def test_expiration_boundary_never_consumes_grant(self):
        grant = self.seed(ttl_seconds=1)
        self.assertEqual(
            self.claim(grant, now=self.now + timedelta(seconds=1)), "GRANT_EXPIRED",
        )
        self.assertEqual(self.states(grant), ("pending", "queued"))

    def test_time_before_issuance_never_consumes_grant(self):
        grant = self.seed()
        self.assertEqual(
            self.claim(grant, now=self.now - timedelta(seconds=1)), "BEFORE_ISSUE_TIME",
        )
        self.assertEqual(self.states(grant), ("pending", "queued"))

    def test_emergency_stop_is_persistent(self):
        grant = self.seed()
        self.fixture.emergency_stop()
        with SyntheticAtomicApprovalFixture(self.root) as reopened:
            self.assertEqual(self.claim(grant, fixture=reopened), "EMERGENCY_STOP_ACTIVE")
        self.assertEqual(self.states(grant), ("pending", "queued"))

    def test_separate_connections_share_one_use(self):
        grant = self.seed()
        with SyntheticAtomicApprovalFixture(self.root) as second:
            self.assertEqual(self.claim(grant, fixture=second), "FIXTURE_CLAIMED")
            self.assertEqual(self.claim(grant), "GRANT_SPENT")
        self.assertEqual(self.states(grant), ("spent", "sending"))

    def test_concurrent_claim_has_exactly_one_winner(self):
        grant = self.seed()
        ready = threading.Barrier(2)
        outcomes, errors = [], []

        def run():
            try:
                with SyntheticAtomicApprovalFixture(self.root) as fixture:
                    ready.wait(timeout=5)
                    outcomes.append(self.claim(grant, fixture=fixture))
            except BaseException as exc:
                errors.append(exc)

        threads = [threading.Thread(target=run) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)
        self.assertFalse(any(thread.is_alive() for thread in threads))
        self.assertEqual(errors, [])
        self.assertCountEqual(outcomes, ["FIXTURE_CLAIMED", "GRANT_SPENT"])
        self.assertEqual(self.states(grant), ("spent", "sending"))

    def test_error_between_updates_rolls_back_both(self):
        grant = self.seed()
        conn = self.fixture._ledger.conn
        conn.execute("""
            CREATE TEMP TRIGGER abort_claim BEFORE UPDATE ON fixture_outbox
            BEGIN SELECT RAISE(ABORT, 'FIXTURE_INJECTED_ABORT'); END
        """)
        with self.assertRaises(sqlite3.IntegrityError):
            self.claim(grant)
        self.assertEqual(self.states(grant), ("pending", "queued"))
        conn.execute("DROP TRIGGER abort_claim")
        self.assertEqual(self.claim(grant), "FIXTURE_CLAIMED")

    def test_commit_error_rolls_back_both(self):
        grant = self.seed()
        conn = self.fixture._ledger.conn
        conn.execute("PRAGMA foreign_keys=ON")
        conn.executescript("""
            CREATE TABLE fixture_parent (id INTEGER PRIMARY KEY);
            CREATE TABLE fixture_child (
                parent_id INTEGER REFERENCES fixture_parent(id)
                    DEFERRABLE INITIALLY DEFERRED
            );
            CREATE TEMP TRIGGER fail_commit AFTER UPDATE ON fixture_outbox
            BEGIN INSERT INTO fixture_child VALUES (1); END;
        """)
        with self.assertRaises(sqlite3.IntegrityError):
            self.claim(grant)
        self.assertEqual(self.states(grant), ("pending", "queued"))
        self.assertFalse(conn.in_transaction)

    def test_lost_transition_rolls_back_other_update(self):
        grant = self.seed()
        conn = self.fixture._ledger.conn
        for table in ("fixture_approval", "fixture_outbox"):
            with self.subTest(table=table):
                conn.execute(
                    f"CREATE TEMP TRIGGER ignore_update BEFORE UPDATE ON {table} "
                    "BEGIN SELECT RAISE(IGNORE); END"
                )
                with self.assertRaisesRegex(ApprovalFailure, "ATOMIC_TRANSITION_LOST"):
                    self.claim(grant)
                self.assertEqual(self.states(grant), ("pending", "queued"))
                conn.execute("DROP TRIGGER ignore_update")

    def test_process_exit_between_updates_recovers_both_rows(self):
        grant = self.seed()
        result = self.crash_child(grant, "before_commit")
        self.assertEqual(result.returncode, 73, result.stderr)
        with SyntheticAtomicApprovalFixture(self.root) as reopened:
            self.assertEqual(self.states(grant), ("pending", "queued"))
            self.assertEqual(self.claim(grant, fixture=reopened), "FIXTURE_CLAIMED")
        self.assertEqual(self.states(grant), ("spent", "sending"))

    def test_process_exit_after_commit_does_not_allow_reuse(self):
        grant = self.seed()
        result = self.crash_child(grant, "after_commit")
        self.assertEqual(result.returncode, 74, result.stderr)
        with SyntheticAtomicApprovalFixture(self.root) as reopened:
            self.assertEqual(self.states(grant), ("spent", "sending"))
            self.assertEqual(self.claim(grant, fixture=reopened), "GRANT_SPENT")

    def test_unresolved_outbox_cannot_be_reclaimed(self):
        grant = self.seed()
        for status in ("sending", "needs_review"):
            with self.subTest(status=status):
                self.fixture._ledger.conn.execute(
                    "UPDATE fixture_outbox SET status=?", (status,),
                )
                self.assertEqual(self.claim(grant), "OUTBOX_NOT_QUEUED")
                self.assertEqual(self.states(grant), ("pending", status))

    def test_persisted_candidate_tampering_blocks_claim(self):
        grant = self.seed()
        conn = self.fixture._ledger.conn
        conn.execute("UPDATE fixture_outbox SET idempotency_key='fixture-tampered'")
        self.assertEqual(self.claim(grant), "OUTBOX_SCOPE_MISMATCH")
        self.assertEqual(self.states(grant), ("pending", "queued"))
        conn.execute(
            "UPDATE fixture_outbox SET idempotency_key=?,fingerprint='tampered'",
            (self.intent.idempotency_key,),
        )
        self.assertEqual(self.claim(grant), "OUTBOX_SCOPE_MISMATCH")
        self.assertEqual(self.states(grant), ("pending", "queued"))

    def test_real_targets_and_plaintext_are_rejected(self):
        grant = self.seed()
        for intent, code in (
            (replace(self.intent, account_id="non-fixture-account"), "REAL_TARGET_FORBIDDEN"),
            (replace(self.intent, thread_id="non-fixture-thread"), "REAL_TARGET_FORBIDDEN"),
            (replace(self.intent, text="non-synthetic message"), "SYNTHETIC_CONTENT_REQUIRED"),
        ):
            with self.subTest(code=code):
                with self.assertRaisesRegex(ApprovalFailure, code):
                    self.fixture.seed_synthetic_outbox(intent)
                with self.assertRaisesRegex(ApprovalFailure, code):
                    self.claim(grant, intent=intent)
                self.assertEqual(self.states(grant), ("pending", "queued"))

    def test_duplicate_outbox_and_key_are_rejected(self):
        self.fixture.seed_synthetic_outbox(self.intent)
        for intent in (
            self.intent,
            replace(self.intent, idempotency_key="fixture-other-key"),
            replace(self.intent, outbox_id="fixture-other-outbox"),
        ):
            with self.subTest(intent=intent):
                with self.assertRaisesRegex(ApprovalFailure, "DUPLICATE_OUTBOX"):
                    self.fixture.seed_synthetic_outbox(intent)

    def test_invalid_clock_leaves_transaction_usable(self):
        grant = self.seed()
        with self.assertRaisesRegex(ApprovalFailure, "INVALID_UTC_TIME"):
            self.claim(grant, now=self.now.replace(tzinfo=None))
        self.assertEqual(self.states(grant), ("pending", "queued"))
        self.assertEqual(self.claim(grant), "FIXTURE_CLAIMED")

    def test_message_not_stored_and_report_is_sanitized(self):
        grant = self.seed()
        report = self.fixture.claim_once(grant, self.intent, now=self.now)
        self.assertNotIn(self.intent.text.encode(), self.database.read_bytes())
        serialized = json.dumps(report)
        for secret in (self.intent.text, self.intent.account_id, self.intent.thread_id,
                       self.intent.idempotency_key, self.intent.outbox_id, grant):
            self.assertNotIn(secret, serialized)
        self.assertEqual(self.database.stat().st_mode & 0o777, 0o600)

    def test_no_network_credentials_or_production_storage(self):
        with patch("socket.socket", side_effect=AssertionError("NO_NETWORK")), \
             patch("urllib.request.urlopen", side_effect=AssertionError("NO_NETWORK")), \
             patch.object(Path, "read_text", side_effect=AssertionError("NO_CREDENTIALS")), \
             patch.object(Path, "read_bytes", side_effect=AssertionError("NO_CREDENTIALS")), \
             patch("wechat_ai_bot.storage.Storage.__init__",
                   side_effect=AssertionError("NO_PRODUCTION_STORAGE")):
            grant = self.seed()
            self.assertEqual(self.claim(grant), "FIXTURE_CLAIMED")

    def test_nonprivate_directory_and_directory_symlink_rejected(self):
        self.root.chmod(0o755)
        try:
            with self.assertRaisesRegex(ApprovalFailure, "TEMP_FIXTURE_REQUIRED"):
                SyntheticAtomicApprovalFixture(self.root)
        finally:
            self.root.chmod(0o700)
        with tempfile.TemporaryDirectory() as folder:
            alias = Path(folder) / "alias"
            alias.symlink_to(self.root, target_is_directory=True)
            with self.assertRaisesRegex(ApprovalFailure, "TEMP_FIXTURE_REQUIRED"):
                SyntheticAtomicApprovalFixture(alias)

    def test_arbitrary_path_and_linked_database_rejected(self):
        with self.assertRaisesRegex(ApprovalFailure, "TEMP_FIXTURE_REQUIRED"):
            SyntheticAtomicApprovalFixture(Path.cwd())
        with tempfile.TemporaryDirectory(prefix="p65d2d2b-fixture-") as folder:
            root = Path(folder)
            target = root / "synthetic-approvals.sqlite3"
            target.symlink_to(self.database)
            with self.assertRaisesRegex(ApprovalFailure, "TEMP_FIXTURE_REQUIRED"):
                SyntheticAtomicApprovalFixture(root)
        with tempfile.TemporaryDirectory(prefix="p65d2d2b-fixture-") as folder:
            target = Path(folder) / "synthetic-approvals.sqlite3"
            os.link(self.database, target)
            with self.assertRaisesRegex(ApprovalFailure, "TEMP_FIXTURE_REQUIRED"):
                SyntheticAtomicApprovalFixture(Path(folder))


if __name__ == "__main__":
    unittest.main()
