"""P6.5D2B2 HTTP exchange tests: 100% in-memory, no Agent WeChat calls."""

from datetime import timedelta
import unittest
from unittest.mock import patch

import test_outbound_safety as original
from wechat_ai_bot.sender_http_offline import (
    ScriptedHttpExecutor, ScriptedHttpResponse, simulate_http_send_once,
)


class OfflineHttpSenderTests(unittest.TestCase):
    setUp = original.OutboundSafetyTests.setUp
    tearDown = original.OutboundSafetyTests.tearDown
    queue_ai = original.OutboundSafetyTests.queue_ai
    checkin_now = original.OutboundSafetyTests.checkin_now

    def receipt(self, outbox_id, *, state="accepted", overrides=None):
        row = self.store.conn.execute(
            "SELECT account_id,thread_id,content,idempotency_key "
            "FROM outbox WHERE outbox_id=?", (outbox_id,),
        ).fetchone()
        result = {
            "id": "remote-synthetic-001", "state": state,
            "accountId": row["account_id"],
            "threadId": row["thread_id"],
            "text": row["content"],
            "idempotencyKey": row["idempotency_key"],
        }
        result.update(overrides or {})
        return result

    def run_case(self, outbox_id, steps, *, at=None):
        transport = ScriptedHttpExecutor(steps)
        outcome = simulate_http_send_once(
            self.store, outbox_id, self.config, transport,
            token="synthetic-token", now=at or self.now,
        )
        status = self.store.conn.execute(
            "SELECT status FROM outbox WHERE outbox_id=?", (outbox_id,),
        ).fetchone()[0]
        return outcome, status, transport

    def test_verified_accepted_post_marks_sent(self):
        item = self.queue_ai()
        outcome, status, exe = self.run_case(item, [
            ("POST", ScriptedHttpResponse(202, self.receipt(item))),
        ])
        self.assertEqual((outcome, status), ((True, "sent"), "sent"))
        self.assertEqual([c[0] for c in exe.calls], ["POST"])

    def test_timeout_followed_by_same_key_lookup_accepted(self):
        item = self.queue_ai()
        outcome, status, exe = self.run_case(item, [
            ("POST", TimeoutError("lost ACK")),
            ("GET", ScriptedHttpResponse(200, self.receipt(item))),
        ])
        self.assertEqual((outcome, status), ((True, "sent"), "sent"))
        self.assertEqual([c[0] for c in exe.calls], ["POST", "GET"])
        self.assertEqual(exe.remaining_steps, 0)

    def test_timeout_lookup_not_found_quarantines(self):
        item = self.queue_ai()
        outcome, status, exe = self.run_case(item, [
            ("POST", TimeoutError("dropped")),
            ("GET", ScriptedHttpResponse(404, {"error": "not_found"})),
        ])
        self.assertEqual((outcome, status), ((True, "needs_review"), "needs_review"))
        self.assertEqual([c[0] for c in exe.calls], ["POST", "GET"])

    def test_invalid_post_receipt_recovered_by_lookup(self):
        item = self.queue_ai()
        outcome, status, exe = self.run_case(item, [
            ("POST", ScriptedHttpResponse(200, {"state": "accepted"})),
            ("GET", ScriptedHttpResponse(200, self.receipt(item))),
        ])
        self.assertEqual((outcome, status), ((True, "sent"), "sent"))
        self.assertEqual(len(exe.calls), 2)

    def test_invalid_post_receipt_and_bad_lookup_quarantines(self):
        item = self.queue_ai()
        outcome, status, exe = self.run_case(item, [
            ("POST", ScriptedHttpResponse(200, {"state": "accepted"})),
            ("GET", ScriptedHttpResponse(200, {"state": "accepted"})),
        ])
        self.assertEqual((outcome, status), ((True, "needs_review"), "needs_review"))
        self.assertEqual(len(exe.calls), 2)

    def test_identity_mismatch_cannot_mark_sent(self):
        item = self.queue_ai()
        bad = self.receipt(item, overrides={"threadId": "wrong-group"})
        outcome, status, _ = self.run_case(item, [
            ("POST", ScriptedHttpResponse(200, bad)),
            ("GET", ScriptedHttpResponse(200, bad)),
        ])
        self.assertEqual((outcome, status), ((True, "needs_review"), "needs_review"))

    def test_needs_review_cannot_be_reinterpreted_as_acceptance(self):
        item = self.queue_ai()
        outcome, status, _ = self.run_case(item, [
            ("POST", ScriptedHttpResponse(202, self.receipt(item, state="needs_review"))),
            ("GET", ScriptedHttpResponse(200, self.receipt(item, state="needs_review"))),
        ])
        self.assertEqual((outcome, status), ((True, "needs_review"), "needs_review"))

    def test_daemon_error_does_not_resubmit(self):
        item = self.queue_ai()
        outcome, status, exe = self.run_case(item, [
            ("POST", ScriptedHttpResponse(503, {"error": "unavailable"})),
            ("GET", ScriptedHttpResponse(503, {"error": "unavailable"})),
        ])
        self.assertEqual((outcome, status), ((True, "needs_review"), "needs_review"))
        self.assertEqual([c[0] for c in exe.calls], ["POST", "GET"])

    def test_expired_key_conflict_cannot_resubmit(self):
        item = self.queue_ai()
        outcome, status, exe = self.run_case(item, [
            ("POST", ScriptedHttpResponse(409, {"code": "idempotency_key_expired"})),
            ("GET", ScriptedHttpResponse(410, {"code": "operation_expired"})),
        ])
        self.assertEqual((outcome, status), ((True, "needs_review"), "needs_review"))
        self.assertEqual([c[0] for c in exe.calls], ["POST", "GET"])

    def test_queued_receipt_then_accepted_lookup(self):
        item = self.queue_ai()
        outcome, status, _ = self.run_case(item, [
            ("POST", ScriptedHttpResponse(202, self.receipt(item, state="queued"))),
            ("GET", ScriptedHttpResponse(200, self.receipt(item))),
        ])
        self.assertEqual((outcome, status), ((True, "sent"), "sent"))

    def test_paused_config_never_executes(self):
        item = self.queue_ai()
        self.config["runtime"]["global_pause"] = True
        outcome, status, exe = self.run_case(item, [])
        self.assertEqual((outcome, status), ((False, "sending_paused_or_target_mismatch"), "queued"))
        self.assertEqual(exe.calls, [])

    def test_no_second_post_for_already_sent(self):
        item = self.queue_ai()
        result, status, exe = self.run_case(item, [
            ("POST", ScriptedHttpResponse(200, self.receipt(item))),
        ])
        self.assertEqual(result, (True, "sent"))
        result2, _, exe2 = self.run_case(item, [])
        self.assertFalse(result2[0])
        self.assertEqual(exe2.calls, [])

    def test_unresolved_send_blocks_followup(self):
        item = self.queue_ai()
        self.run_case(item, [
            ("POST", TimeoutError("lost")),
            ("GET", ScriptedHttpResponse(404, {})),
        ])
        newer = self.queue_ai()
        outcome, status, exe = self.run_case(newer, [])
        self.assertEqual(outcome, (False, "unresolved_send_exists"))
        self.assertEqual((status, exe.calls), ("queued", []))

    def test_scheduled_task_uses_same_http_path(self):
        item, due = self.checkin_now()
        outcome, status, exe = self.run_case(item, [
            ("POST", ScriptedHttpResponse(200, self.receipt(item))),
        ], at=due + timedelta(seconds=10))
        self.assertEqual((outcome, status), ((True, "sent"), "sent"))
        self.assertEqual(self.store.conn.execute(
            "SELECT status FROM scheduled_tasks"
        ).fetchone()[0], "sent")
        self.assertEqual(len(exe.calls), 1)

    def test_denies_non_scripted_executor(self):
        item = self.queue_ai()
        with self.assertRaises(TypeError):
            simulate_http_send_once(self.store, item, self.config, object())
        self.assertEqual(self.store.conn.execute(
            "SELECT status FROM outbox WHERE outbox_id=?", (item,)
        ).fetchone()[0], "queued")

    def test_never_uses_urlopen_or_socket(self):
        item = self.queue_ai()
        with patch("urllib.request.urlopen", side_effect=AssertionError("network used")) as urlopen:
            with patch("socket.create_connection", side_effect=AssertionError("socket used")) as sock:
                self.run_case(item, [
                    ("POST", ScriptedHttpResponse(202, self.receipt(item))),
                ])
        urlopen.assert_not_called()
        sock.assert_not_called()
