import json
import unittest

from wechat_ai_bot.decision_engine import (
    DecisionDeferred, decide, parse_decision,
)

CFG = {"ollama": {"model": "qwen3.5:9b"}}

CTX = [{
    "role": "group_member",
    "speaker": "群友1",
    "text": "有推荐的周末电影吗？",
}]


class DecisionTests(unittest.TestCase):

    def test_no_context(self):
        self.assertEqual(
            decide([], CFG)["action"], "skip"
        )

    def test_last_outgoing(self):
        result = decide([{
            "role": "account_owner",
            "text": "昨天看了电影",
        }], CFG)
        self.assertEqual(result["reason"], "not_incoming")

    def test_short_message(self):
        result = decide([{
            "role": "group_member",
            "text": "哈哈",
        }], CFG)
        self.assertEqual(result["reason"], "low_signal")

    def test_accepted_reply(self):
        def fake(payload, config):
            self.assertEqual(
                payload["format"]["properties"]
                ["action"]["enum"],
                ["reply", "skip"],
            )
            self.assertIs(payload["think"], False)
            return json.dumps({
                "action": "reply",
                "reason": "topic",
                "reply": "选喜剧吧！",
            })

        self.assertEqual(
            decide(CTX, CFG, fake)["reply"],
            "选喜剧吧！",
        )

    def test_valid_skip(self):
        response = (
            '{"action":"skip",'
            '"reason":"no_need","reply":""}'
        )
        result = decide(
            CTX, CFG, lambda p, c: response
        )
        self.assertEqual(result["action"], "skip")

    def test_invalid_action_defers_as_invalid_model_response(self):
        response = (
            '{"action":"send",'
            '"reason":"x","reply":"oops"}'
        )
        with self.assertRaises(DecisionDeferred) as caught:
            decide(CTX, CFG, lambda p, c: response)
        self.assertEqual(
            caught.exception.code, "invalid_model_response"
        )
        self.assertEqual(
            caught.exception.cause_type, "ValueError"
        )

    def test_extra_fields_rejected(self):
        with self.assertRaises(ValueError):
            parse_decision(
                '{"action":"reply","reason":"x",'
                '"reply":"hi","send":true}'
            )

    def test_invalid_skip_reply_rejected(self):
        with self.assertRaises(ValueError):
            parse_decision(
                '{"action":"skip","reason":"x",'
                '"reply":"hi"}'
            )

    def test_empty_reason_rejected(self):
        with self.assertRaises(ValueError):
            parse_decision(
                '{"action":"reply","reason":"",'
                '"reply":"你好"}'
            )

    def test_whitespace_reason_defers_as_invalid_model_response(self):
        raw = (
            '{"action":"skip","reason":"   ",'
            '"reply":""}'
        )
        with self.assertRaises(DecisionDeferred) as caught:
            decide(CTX, CFG, lambda p, c: raw)
        self.assertEqual(
            caught.exception.code, "invalid_model_response"
        )

    def test_model_request_error_is_typed_and_deferred(self):
        def failing(payload, config):
            raise TimeoutError("timeout")

        with self.assertRaises(DecisionDeferred) as caught:
            decide(CTX, CFG, failing)
        self.assertEqual(
            caught.exception.code, "model_request_failed"
        )
        self.assertEqual(
            caught.exception.cause_type, "TimeoutError"
        )


if __name__ == "__main__":
    unittest.main()
