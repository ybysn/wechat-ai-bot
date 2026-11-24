"""Pure offline HTTP envelope/receipt tests; no fake can touch localhost."""

import json
from urllib.parse import urlparse, parse_qs
import unittest

from wechat_ai_bot.sender_http_contract import (
    BASE_URL, SendContractError, build_text_send_request,
    build_send_lookup_request, inspect_send_receipt,
)


class HttpSendContractTests(unittest.TestCase):
    def _send(self, **overrides):
        opts = dict(account_id="personal", thread_id="group@chatroom",
                    text="你好，世界", idempotency_key="outbox-001", token="test-token")
        opts.update(overrides)
        return build_text_send_request(**opts)

    def test_post_body_and_stable_idempotency_key(self):
        req = self._send()
        self.assertEqual(req.get_method(), "POST")
        self.assertEqual(req.full_url, BASE_URL + "/accounts/personal/sends")
        self.assertEqual(json.loads(req.data), {"threadId":"group@chatroom", "text":"你好，世界"})
        self.assertEqual(req.headers["Idempotency-key"], "outbox-001")
        self.assertEqual(req.headers["Authorization"], "Bearer test-token")

    def test_url_encodes_opaque_ids(self):
        req = self._send(account_id="alias with / slash")
        self.assertIn("accounts/alias%20with%20%2F%20slash/sends", req.full_url)

    def test_receipt_lookup_is_get_and_key_is_encoded(self):
        req = build_send_lookup_request(account_id="personal", idempotency_key="a/b +", token="test-token")
        self.assertEqual(req.get_method(), "GET")
        self.assertIsNone(req.data)
        self.assertEqual(parse_qs(urlparse(req.full_url).query)["key"], ["a/b +"])
        self.assertNotIn("Idempotency-key", req.headers)

    def test_rejects_non_loopback_or_alternate_port(self):
        for url in ("http://localhost:6174/v1", "https://127.0.0.1:6174/v1", "http://127.0.0.1:9999/v1", "http://remote/v1"):
            with self.subTest(url=url), self.assertRaises(SendContractError):
                self._send(base_url=url)

    def test_rejects_header_injection_and_empty_content(self):
        for overrides in (dict(token="key\r\nX-Evil: x"), dict(idempotency_key="a\nb"), dict(text="  "), dict(account_id="bad\x00id")):
            with self.subTest(overrides=repr(overrides)), self.assertRaises(SendContractError):
                self._send(**overrides)

    def test_acceptance_requires_exact_identity_and_remote_id(self):
        record = dict(id="send-1", state="accepted", accountId="personal",
                      threadId="group@chatroom", text="你好", idempotencyKey="outbox-001")
        expected = dict(expected_account_id="personal", expected_thread_id="group@chatroom",
                        expected_text="你好", expected_key="outbox-001")
        result = inspect_send_receipt(record, **expected)
        self.assertTrue(result.may_mark_sent)
        self.assertTrue(result.identity_verified)
        self.assertEqual(result.remote_send_id, "send-1")

    def test_unverified_acceptance_must_not_be_sent(self):
        expected = dict(expected_account_id="personal", expected_thread_id="group@chatroom",
                        expected_text="你好", expected_key="outbox-001")
        for bad in (dict(state="accepted", id="send-1"), dict(state="accepted", id="send-1", idempotencyKey="wrong"), {}, [], "bad"):
            with self.subTest(bad=repr(bad)):
                self.assertFalse(inspect_send_receipt(bad, **expected).may_mark_sent)

    def test_needs_review_is_not_success_even_if_identity_verified(self):
        record = dict(id="send-1", state="needs_review", accountId="personal",
                      threadId="group@chatroom", text="你好", idempotencyKey="outbox-001")
        result = inspect_send_receipt(record, expected_account_id="personal",
                                       expected_thread_id="group@chatroom", expected_text="你好",
                                       expected_key="outbox-001")
        self.assertTrue(result.identity_verified)
        self.assertFalse(result.may_mark_sent)

    def test_failed_and_queued_never_mark_sent(self):
        expected = dict(expected_account_id="a", expected_thread_id="b", expected_text="c", expected_key="d")
        for state in ("failed", "cancelled", "queued", "sending", "unrecognized"):
            record = dict(id="one", state=state, accountId="a", threadId="b", text="c", idempotencyKey="d")
            self.assertFalse(inspect_send_receipt(record, **expected).may_mark_sent)

    def test_module_does_not_provide_network_executor(self):
        import wechat_ai_bot.sender_http_contract as m
        self.assertFalse(hasattr(m, "send"))
        self.assertFalse(hasattr(m, "urlopen"))
