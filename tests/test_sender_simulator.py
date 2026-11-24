"""No-network state-machine checks for P6.5D2A."""
from datetime import timedelta
import unittest

import test_outbound_safety as old_tests
from wechat_ai_bot.sender_simulator import FakeTransport, simulate_send_once


class SenderSimulatorTests(unittest.TestCase):
    setUp = old_tests.OutboundSafetyTests.setUp
    tearDown = old_tests.OutboundSafetyTests.tearDown
    queue_ai = old_tests.OutboundSafetyTests.queue_ai
    checkin_now = old_tests.OutboundSafetyTests.checkin_now

    def test_synthetic_acceptance_sets_sent_and_remote_id(self):
        outbox = self.queue_ai()
        transport = FakeTransport()
        self.assertEqual(
            simulate_send_once(self.store, outbox, self.config, transport, now=self.now),
            (True, "sent"),
        )
        row = self.store.conn.execute(
            "SELECT status,remote_send_id,sent_at FROM outbox WHERE outbox_id=?",
            (outbox,),
        ).fetchone()
        self.assertEqual(row["status"], "sent")
        self.assertTrue(row["remote_send_id"].startswith("sim-"))
        self.assertIsNotNone(row["sent_at"])
        self.assertEqual(transport.submit_count, 1)

    def test_second_attempt_does_not_submit_again(self):
        outbox = self.queue_ai()
        transport = FakeTransport()
        simulate_send_once(self.store, outbox, self.config, transport, now=self.now)
        self.assertFalse(
            simulate_send_once(self.store, outbox, self.config, transport, now=self.now)[0]
        )
        self.assertEqual(transport.submit_count, 1)

    def test_timeout_receipt_lookup_recovers_without_resubmit(self):
        outbox = self.queue_ai()
        transport = FakeTransport(lose_ack=True)
        self.assertEqual(
            simulate_send_once(self.store, outbox, self.config, transport, now=self.now),
            (True, "sent"),
        )
        self.assertEqual((transport.submit_count, transport.lookup_count), (1, 1))

    def test_ambiguous_without_receipt_requires_review(self):
        outbox = self.queue_ai()
        transport = FakeTransport(lose_ack=True, lose_receipt=True)
        self.assertEqual(
            simulate_send_once(self.store, outbox, self.config, transport, now=self.now),
            (True, "needs_review"),
        )
        self.assertEqual((transport.submit_count, transport.lookup_count), (1, 1))
        self.assertEqual(self.store.conn.execute(
            "SELECT status FROM outbox WHERE outbox_id=?", (outbox,)
        ).fetchone()[0], "needs_review")
        newer = self.queue_ai()
        self.assertEqual(self.store.claim_outbox_for_send(
            newer, self.config, now=self.now
        ), (False, "unresolved_send_exists"))

    def test_explicit_failure_requires_review(self):
        outbox = self.queue_ai()
        self.assertEqual(simulate_send_once(
            self.store, outbox, self.config, FakeTransport("failed"), now=self.now
        ), (True, "needs_review"))

    def test_paused_config_never_reaches_transport(self):
        outbox = self.queue_ai()
        transport = FakeTransport()
        self.config["runtime"]["global_pause"] = True
        self.assertFalse(simulate_send_once(
            self.store, outbox, self.config, transport, now=self.now
        )[0])
        self.assertEqual(transport.submit_count, 0)

    def test_scheduled_task_acceptance_tracks_sent(self):
        outbox, due = self.checkin_now()
        moment = due + timedelta(seconds=10)
        self.assertEqual(simulate_send_once(
            self.store, outbox, self.config, FakeTransport(), now=moment,
        ), (True, "sent"))
        self.assertEqual(self.store.conn.execute(
            "SELECT status FROM scheduled_tasks"
        ).fetchone()[0], "sent")

    def test_scheduled_task_unknown_tracks_needs_review(self):
        outbox, due = self.checkin_now()
        moment = due + timedelta(seconds=10)
        self.assertEqual(simulate_send_once(
            self.store, outbox, self.config,
            FakeTransport(lose_ack=True, lose_receipt=True), now=moment,
        ), (True, "needs_review"))
        self.assertEqual(self.store.conn.execute(
            "SELECT status FROM scheduled_tasks"
        ).fetchone()[0], "needs_review")

    def test_receipt_idempotency_key_mismatch_blocked(self):
        outbox = self.queue_ai()
        self.assertTrue(self.store.claim_outbox_for_send(
            outbox, self.config, now=self.now
        )[0])
        self.assertEqual(self.store.record_send_result(
            outbox, "other-key", "accepted", remote_send_id="remote", now=self.now,
        ), (False, "idempotency_key_mismatch"))
        self.assertEqual(self.store.conn.execute(
            "SELECT status FROM outbox WHERE outbox_id=?", (outbox,)
        ).fetchone()[0], "sending")
        self.assertEqual(self.store.quarantine_unresolved_send(outbox),
                         (True, "needs_review"))

    def test_record_result_refuses_no_remote_receipt(self):
        outbox = self.queue_ai()
        self.assertTrue(self.store.claim_outbox_for_send(
            outbox, self.config, now=self.now
        )[0])
        key = self.store.conn.execute(
            "SELECT idempotency_key FROM outbox WHERE outbox_id=?", (outbox,)
        ).fetchone()[0]
        with self.assertRaisesRegex(ValueError, "requires verified"):
            self.store.record_send_result(outbox, key, "accepted", now=self.now)
        self.assertEqual(self.store.quarantine_unresolved_send(outbox),
                         (True, "needs_review"))

    def test_recovery_unknown_outbox_state_is_not_reusable(self):
        outbox = self.queue_ai()
        self.assertTrue(self.store.claim_outbox_for_send(
            outbox, self.config, now=self.now
        )[0])
        self.assertEqual(self.store.quarantine_unresolved_send(outbox),
                         (True, "needs_review"))
        self.assertEqual(self.store.quarantine_unresolved_send(outbox),
                         (False, "outbox_not_sending"))

    def test_disallows_non_temporary_database(self):
        outbox = self.queue_ai()
        transport = FakeTransport()
        # A fake connection/path wrapper simulates an accidentally passed
        # production DB and must be rejected before any claim or transport use.
        original = self.store.conn
        class RealPathConnection:
            def execute(self, sql):
                if sql == "PRAGMA database_list":
                    return self
                return original.execute(sql)
            def fetchone(self):
                return (0, "main", "/home/example/apps/wechat-ai-bot/data.sqlite3")
        self.store.conn = RealPathConnection()
        try:
            with self.assertRaises(PermissionError):
                simulate_send_once(self.store, outbox, self.config, transport, now=self.now)
        finally:
            self.store.conn = original
        self.assertEqual(transport.submit_count, 0)

    def test_disallows_any_non_fake_transport(self):
        outbox = self.queue_ai()
        with self.assertRaises(TypeError):
            simulate_send_once(self.store, outbox, self.config, object(), now=self.now)
        self.assertEqual(self.store.conn.execute(
            "SELECT status FROM outbox WHERE outbox_id=?", (outbox,)
        ).fetchone()[0], "queued")
