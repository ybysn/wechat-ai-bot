"""P6.5D2D-2B1 offline synthetic approval semantics."""

from datetime import datetime, timedelta, timezone
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import patch

from wechat_ai_bot.sender_approval_fixture import (
    ApprovalFailure, SyntheticApprovalLedger, SyntheticIntent,
)
from wechat_ai_bot.sender_http_fixture import (
    FIXTURE_ACCOUNT, FIXTURE_THREAD, FIXTURE_TEXT_PREFIX,
)


class ApprovalFixtureTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="p65d2d2b-fixture-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.ledger = SyntheticApprovalLedger(self.root)
        self.addCleanup(self.ledger.close)
        self.now = datetime(2026, 11, 22, 1, 0, 0, tzinfo=timezone.utc)
        self.intent = SyntheticIntent(
            FIXTURE_ACCOUNT, FIXTURE_THREAD, "fixture-out-01",
            "fixture-idempotency-01", FIXTURE_TEXT_PREFIX + "Hello",
        )

    def _seed(self, **kwargs):
        return self.ledger.seed_synthetic_approval(self.intent, now=self.now, **kwargs)

    def _inspect(self, grant, intent=None, at=None):
        return self.ledger.inspect(grant, intent or self.intent, now=at or self.now)

    def test_approval_only_fixture_eligible_never_live(self):
        grant = self._seed()
        report = self._inspect(grant)
        self.assertEqual(report["verdict"], "FIXTURE_ELIGIBLE")
        self.assertTrue(report["fixture_eligible"])
        self.assertFalse(report["live_send_authorized"])
        self.assertEqual(report["network_requests"], 0)

    def test_scope_account_mismatch_fails_closed(self):
        grant = self._seed()
        i = self.intent
        wrong = SyntheticIntent("real-account", i.thread_id, i.outbox_id,
                                i.idempotency_key, i.text)
        with self.assertRaisesRegex(ApprovalFailure, "REAL_TARGET_FORBIDDEN"):
            self._inspect(grant, wrong)

    def test_scope_group_mismatch_fails_closed(self):
        grant = self._seed()
        i = self.intent
        wrong = SyntheticIntent(i.account_id, "real-group", i.outbox_id,
                                i.idempotency_key, i.text)
        with self.assertRaisesRegex(ApprovalFailure, "REAL_TARGET_FORBIDDEN"):
            self._inspect(grant, wrong)

    def test_scope_outbox_mismatch(self):
        grant = self._seed()
        i = self.intent
        wrong = SyntheticIntent(i.account_id, i.thread_id, "fixture-out-02",
                                i.idempotency_key, i.text)
        self.assertEqual(self._inspect(grant, wrong)["verdict"], "SCOPE_MISMATCH")

    def test_scope_idempotency_mismatch(self):
        grant = self._seed()
        i = self.intent
        wrong = SyntheticIntent(i.account_id, i.thread_id, i.outbox_id,
                                "fixture-idempotency-02", i.text)
        self.assertEqual(self._inspect(grant, wrong)["verdict"], "SCOPE_MISMATCH")

    def test_scope_message_mismatch(self):
        grant = self._seed()
        i = self.intent
        wrong = SyntheticIntent(i.account_id, i.thread_id, i.outbox_id,
                                i.idempotency_key, i.text + " changed")
        self.assertEqual(self._inspect(grant, wrong)["verdict"], "SCOPE_MISMATCH")

    def test_rejects_real_message(self):
        i = self.intent
        wrong = SyntheticIntent(i.account_id, i.thread_id, i.outbox_id,
                                i.idempotency_key, "A real WeChat reply")
        with self.assertRaisesRegex(ApprovalFailure, "SYNTHETIC_CONTENT_REQUIRED"):
            self.ledger.seed_synthetic_approval(wrong, now=self.now)

    def test_expired_including_boundary(self):
        grant = self._seed(ttl_seconds=1)
        at = self.now + timedelta(seconds=1)
        self.assertEqual(self._inspect(grant, at=at)["verdict"], "GRANT_EXPIRED")
        self.assertEqual(self.ledger.spend_once(grant, self.intent, now=at), "GRANT_EXPIRED")

    def test_future_issue_time_rejected(self):
        grant = self._seed()
        self.assertEqual(self._inspect(grant, at=self.now - timedelta(seconds=1))["verdict"], "BEFORE_ISSUE_TIME")

    def test_ttl_zero_bool_and_long_rejected(self):
        for ttl in (0, -1, 121, True, 1.5):
            with self.subTest(ttl=ttl), self.assertRaisesRegex(ApprovalFailure, "INVALID_TTL"):
                self._seed(ttl_seconds=ttl)

    def test_naive_clock_rejected(self):
        with self.assertRaisesRegex(ApprovalFailure, "INVALID_UTC_TIME"):
            self.ledger.seed_synthetic_approval(self.intent, now=datetime(2026, 11, 22))

    def test_revoke_prevents_spend(self):
        grant = self._seed()
        self.assertTrue(self.ledger.revoke(grant))
        self.assertFalse(self.ledger.revoke(grant))
        self.assertEqual(self.ledger.spend_once(grant, self.intent, now=self.now), "GRANT_REVOKED")

    def test_duplicate_permits_same_intent_denied(self):
        self._seed()
        with self.assertRaisesRegex(ApprovalFailure, "DUPLICATE_INTENT"):
            self._seed()

    def test_ledger_file_is_private(self):
        self._seed()
        self.assertEqual((self.root / "synthetic-approvals.sqlite3").stat().st_mode & 0o777, 0o600)

    def test_single_use(self):
        grant = self._seed()
        self.assertEqual(self.ledger.spend_once(grant, self.intent, now=self.now), "FIXTURE_SPENT")
        self.assertEqual(self.ledger.spend_once(grant, self.intent, now=self.now), "GRANT_SPENT")
        self.assertFalse(self.ledger.revoke(grant))

    def test_crash_reopen_keeps_spent(self):
        grant = self._seed()
        self.assertEqual(self.ledger.spend_once(grant, self.intent, now=self.now), "FIXTURE_SPENT")
        with SyntheticApprovalLedger(self.root) as reopened:
            self.assertEqual(reopened.spend_once(grant, self.intent, now=self.now), "GRANT_SPENT")

    def test_emergency_stop_persists_after_reopen(self):
        grant = self._seed()
        self.ledger.emergency_stop()
        self.assertEqual(self.ledger.spend_once(grant, self.intent, now=self.now), "EMERGENCY_STOP_ACTIVE")
        with SyntheticApprovalLedger(self.root) as reopened:
            self.assertEqual(reopened.inspect(grant, self.intent, now=self.now)["verdict"], "EMERGENCY_STOP_ACTIVE")
            with self.assertRaisesRegex(ApprovalFailure, "EMERGENCY_STOP_ACTIVE"):
                reopened.seed_synthetic_approval(self.intent, now=self.now)

    def test_wrong_grant_no_use(self):
        self.assertEqual(self._inspect("fixture-grant-does-not-exist")["verdict"], "GRANT_NOT_FOUND")

    def test_nonprivate_temp_directory_rejected(self):
        self.root.chmod(0o755)
        try:
            with self.assertRaisesRegex(ApprovalFailure, "TEMP_FIXTURE_REQUIRED"):
                SyntheticApprovalLedger(self.root)
        finally:
            self.root.chmod(0o700)

    def test_non_temp_path_rejected(self):
        with self.assertRaisesRegex(ApprovalFailure, "TEMP_FIXTURE_REQUIRED"):
            SyntheticApprovalLedger(Path("/mnt/data"))

    def test_synthetic_plaintext_not_saved_in_db(self):
        grant = self._seed()
        self.assertFalse(self.intent.text.encode() in (self.root / "synthetic-approvals.sqlite3").read_bytes())
        self.assertFalse(grant.encode() == self.intent.text.encode())

    def test_no_network_or_token_dependencies(self):
        with patch("socket.socket", side_effect=AssertionError("no network")), \
             patch("urllib.request.urlopen", side_effect=AssertionError("no network")):
            grant = self._seed()
            self.assertEqual(self._inspect(grant)["network_requests"], 0)

    def test_concurrent_spend_only_one_winner(self):
        grant = self._seed()
        ready = threading.Barrier(2)
        outcomes = []
        errors = []

        def run():
            try:
                with SyntheticApprovalLedger(self.root) as ledger:
                    ready.wait(timeout=3)
                    outcomes.append(ledger.spend_once(grant, self.intent, now=self.now))
            except BaseException as exc:
                errors.append(exc)

        t1 = threading.Thread(target=run)
        t2 = threading.Thread(target=run)
        t1.start()
        t2.start()
        t1.join(timeout=5)
        t2.join(timeout=5)
        self.assertFalse(t1.is_alive() or t2.is_alive())
        self.assertEqual(errors, [])
        self.assertCountEqual(outcomes, ["FIXTURE_SPENT", "GRANT_SPENT"])

    def test_separate_connections_can_only_spend_once(self):
        grant = self._seed()
        with SyntheticApprovalLedger(self.root) as second:
            outcomes = [self.ledger.spend_once(grant, self.intent, now=self.now),
                        second.spend_once(grant, self.intent, now=self.now)]
        self.assertEqual(outcomes, ["FIXTURE_SPENT", "GRANT_SPENT"])


if __name__ == "__main__":
    unittest.main()
