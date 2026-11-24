"""Offline compatibility with the audited v2.9.0 source, never live receipts.

Source commit: d0f3677dd997e22318550b911586ad0d5f067ae5.
Shapes follow wechat-store::SendIntent and wechat-api's text handlers.
Every ID and message below is synthetic; no recorded production payloads.
"""

import json
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

from wechat_ai_bot.sender_http_contract import (
    SendContractError, build_send_lookup_request, build_text_send_request,
    inspect_send_receipt,
)
from wechat_ai_bot.sender_http_transport import RestrictedHttpExecutor


class NativeProtocolCompatibilityTests(unittest.TestCase):
    def setUp(self):
        self.account = "fixture-native-account"
        self.thread = "fixture-native-group@chatroom"
        self.key = "fixture-native-key"
        self.text = "P65-NATIVE-SYNTHETIC:hello"

    def send(self, **overrides):
        return build_text_send_request(**{
            "account_id": self.account, "thread_id": self.thread,
            "text": self.text, "idempotency_key": self.key,
            "token": "synthetic", **overrides,
        })

    def lookup(self, key):
        return build_send_lookup_request(
            account_id=self.account, idempotency_key=key, token="synthetic",
        )

    def native_record(self, **overrides):
        # Matches the actual Store serialization, not just the generated SDK:
        # clientId is a string and localMessageId is omitted until projected.
        return {
            "id": "fixture-native-send", "accountId": self.account,
            "destination": self.thread, "state": "accepted",
            "clientId": "4294967295", "serverId": "18446744073709551615",
            "errorCode": None, "createdAt": "2026-11-22T02:00:00Z",
            **overrides,
        }

    def inspect(self, payload):
        return inspect_send_receipt(
            payload, expected_account_id=self.account,
            expected_thread_id=self.thread, expected_text=self.text,
            expected_key=self.key,
        )

    def test_plain_text_body_matches_native_request_fields(self):
        self.assertEqual(json.loads(self.send().data), {
            "threadId": self.thread, "text": self.text,
        })

    def test_maximum_ascii_key_works_for_send_and_lookup(self):
        key = "k" * 200
        self.assertEqual(self.send(idempotency_key=key).get_header("Idempotency-key"), key)
        self.assertEqual(parse_qs(urlsplit(self.lookup(key).full_url).query), {"key": [key]})

    def test_overlong_key_rejected_by_both_builders(self):
        for builder in (lambda key: self.send(idempotency_key=key), self.lookup):
            with self.assertRaisesRegex(SendContractError, "Invalid idempotency key"):
                builder("k" * 201)

    def test_non_ascii_key_rejected_before_header_encoding(self):
        for key in ("é" * 100, "fixture-中文", "fixture-😀"):
            for builder in (lambda key: self.send(idempotency_key=key), self.lookup):
                with self.subTest(key=key), self.assertRaises(SendContractError):
                    builder(key)

    def test_control_and_empty_keys_rejected(self):
        for key in ("", "key\r\nheader", "key\tvalue", "key\x00", "key\x7f"):
            for builder in (lambda key: self.send(idempotency_key=key), self.lookup):
                with self.subTest(key=repr(key)), self.assertRaises(SendContractError):
                    builder(key)

    def test_graphic_ascii_and_spaces_survive_lookup_encoding(self):
        key = "fixture key/+?&=%#"
        self.assertEqual(self.send(idempotency_key=key).get_header("Idempotency-key"), key)
        self.assertEqual(parse_qs(urlsplit(self.lookup(key).full_url).query), {"key": [key]})

    def test_destination_limit_counts_utf8_bytes(self):
        for target in ("g" * 256, "群" * 85):
            with self.subTest(size=len(target.encode())):
                self.assertEqual(json.loads(self.send(thread_id=target).data)["threadId"], target)
        for target in ("g" * 257, "群" * 86):
            with self.subTest(size=len(target.encode())), self.assertRaises(SendContractError):
                self.send(thread_id=target)

    def test_destination_rejects_whitespace_and_controls(self):
        for target in ("group name", "group\tname", "group\u0080", "group\u0085",
                       "group\u2028name", "group\x7f"):
            with self.subTest(target=repr(target)), self.assertRaises(SendContractError):
                self.send(thread_id=target)

    def test_invalid_unicode_destination_has_sanitized_error(self):
        with self.assertRaisesRegex(SendContractError, "^Invalid thread ID$"):
            self.send(thread_id="fixture-\ud800")

    def test_native_accepted_record_does_not_prove_request_binding(self):
        result = self.inspect(self.native_record())
        self.assertEqual(result.remote_state, "accepted")
        self.assertEqual(result.remote_send_id, "fixture-native-send")
        self.assertFalse(result.identity_verified)
        self.assertFalse(result.may_mark_sent)

    def test_native_submitting_is_recognized_without_marking_sent(self):
        result = self.inspect(self.native_record(state="submitting", serverId=None))
        self.assertEqual(result.remote_state, "submitting")
        self.assertFalse(result.may_mark_sent)

    def test_every_native_state_fails_closed_without_request_binding(self):
        for state in ("queued", "submitting", "accepted", "failed", "needs_review", "cancelled"):
            with self.subTest(state=state):
                result = self.inspect(self.native_record(state=state))
                self.assertEqual(result.remote_state, state)
                self.assertFalse(result.identity_verified)
                self.assertFalse(result.may_mark_sent)

    def test_projection_and_server_ids_do_not_replace_request_binding(self):
        record = self.native_record(localMessageId="fixture-profile-message")
        result = self.inspect(record)
        self.assertFalse(result.may_mark_sent)
        self.assertEqual(record["serverId"], "18446744073709551615")

    def test_destination_is_not_silently_aliased_to_thread_id(self):
        record = self.native_record(text=self.text, idempotencyKey=self.key)
        self.assertFalse(self.inspect(record).identity_verified)

    def test_http_error_objects_are_not_receipts(self):
        for code in ("unauthorized", "forbidden", "not_found", "idempotency_conflict",
                     "idempotency_key_expired", "operation_expired", "operation_failed"):
            with self.subTest(code=code):
                result = self.inspect({"error": {"code": code}})
                self.assertEqual(result.remote_state, "unknown")
                self.assertFalse(result.may_mark_sent)

    def test_production_post_stays_blocked_before_any_socket(self):
        with patch("socket.socket", side_effect=AssertionError("NO_NETWORK")):
            with self.assertRaisesRegex(PermissionError, "Production POST"):
                RestrictedHttpExecutor().perform(self.send())


if __name__ == "__main__":
    unittest.main()
