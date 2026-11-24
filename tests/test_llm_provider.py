"""Offline DeepSeek/Ollama routing tests; no paid HTTP calls."""
import copy
import json
import tempfile
from datetime import datetime, timezone, timedelta
import unittest
from pathlib import Path
from unittest.mock import patch

from wechat_ai_bot.decision_engine import (
    DecisionDeferred, ModelRateLimited, ModelBudgetExhausted,
    _reserve_request, build_payload, decide, request_deepseek,
    selected_model, selected_provider,
)

CTX = [{"role":"group_member","speaker":"群友1", "text":"周末有推荐的电影吗？"}]


class Response:
    def __init__(self, data, url="https://api.deepseek.com/chat/completions"):
        self.payload = json.dumps(data, ensure_ascii=False).encode()
        self.url = url
    def geturl(self):
        return self.url
    def read(self, n):
        return self.payload[:n]
    def __enter__(self):
        return self
    def __exit__(self, *exc):
        return False


class FakeOpener:
    def __init__(self, response):
        self.response = response
        self.requests = []
    def open(self, req, timeout=None):
        self.requests.append((req, timeout))
        return self.response


class DeepSeekTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.key = root / "api-key"
        self.key.write_text("secret-for-offline-tests")
        self.key.chmod(0o600)
        self.log = root / "usage.jsonl"
        self.cfg = {
            "llm": {
                "provider": "deepseek",
                "deepseek": {
                    "base_url": "https://api.deepseek.com",
                    "model": "deepseek-flash",
                    "api_key_file": str(self.key),
                    "usage_log_file": str(self.log),
                    "daily_request_limit": 2,
                    # Existing tests exercise daily budget without rate control.
                    "min_request_interval_seconds": 0,
                    "max_requests_per_60_seconds": 60,
                    "timeout_seconds": 10,
                },
            },
            "ollama": {"base_url": "http://192.168.193.1:11434", "model": "qwen3.5:9b"},
        }

    @staticmethod
    def make_result(content='{"action":"skip","reason":"no need","reply":""}', finish="stop"):
        return {
            "choices": [{"finish_reason": finish, "message": {"content": content}}],
            "usage": {
                "prompt_tokens": 1000,
                "prompt_cache_hit_tokens": 0,
                "prompt_cache_miss_tokens": 1000,
                "completion_tokens": 100,
                "total_tokens": 1100,
            },
        }

    def ledger(self):
        return [json.loads(x) for x in self.log.read_text().splitlines()]

    def test_legacy_ollama_unchanged(self):
        cfg = {"ollama": self.cfg["ollama"]}
        self.assertEqual(selected_provider(cfg), "ollama")
        self.assertEqual(selected_model(cfg), "qwen3.5:9b")
        payload = build_payload(CTX, cfg)
        self.assertEqual(payload["model"], "qwen3.5:9b")
        self.assertIs(payload["think"], False)
        self.assertEqual(payload["format"]["properties"]["action"]["enum"], ["reply", "skip"])

    def test_deepseek_prompt_specifies_exact_json_contract(self):
        payload = build_payload(CTX, self.cfg)
        prompt = payload["messages"][0]["content"]
        self.assertIn("action", prompt)
        self.assertIn("reason", prompt)
        self.assertIn("reply", prompt)
        self.assertIn('"action":"skip"', prompt)
        self.assertIn('"action":"reply"', prompt)
        self.assertIn("不得省略字段", prompt)
        self.assertNotIn("不得省略字段", build_payload(
            CTX, {"ollama": self.cfg["ollama"]}
        )["messages"][0]["content"])

    def test_rejection_reason_is_a_safe_fixed_code(self):
        invalid = '{"action":"reply","reply":"ok"}'
        with self.assertRaises(DecisionDeferred) as caught:
            decide(CTX, self.cfg, lambda p, c: invalid)
        self.assertEqual(caught.exception.code, "invalid_model_response")
        self.assertEqual(caught.exception.validation_code, "fields")
        self.assertNotIn("ok", str(caught.exception))

    def test_invalid_json_is_classified_without_its_body(self):
        invalid = '{"secret":"private content"'
        with self.assertRaises(DecisionDeferred) as caught:
            decide(CTX, self.cfg, lambda p, c: invalid)
        self.assertEqual(caught.exception.validation_code, "json_syntax")
        self.assertNotIn("private content", str(caught.exception))

    def test_deepseek_payload_and_model(self):
        payload = build_payload(CTX, self.cfg)
        self.assertEqual(payload["model"], "deepseek-flash")
        self.assertEqual(payload["response_format"], {"type": "json_object"})
        self.assertEqual(payload["thinking"], {"type": "disabled"})
        self.assertEqual(payload["max_tokens"], 256)
        self.assertNotIn("format", payload)
        self.assertNotIn("num_ctx", payload)
        self.assertEqual(selected_model(self.cfg), "deepseek-flash")

    def test_offline_api_call_and_usage(self):
        payload = build_payload(CTX, self.cfg)
        fake = FakeOpener(Response(self.make_result()))
        raw = request_deepseek(payload, self.cfg, opener=fake)
        self.assertEqual(json.loads(raw)["action"], "skip")
        self.assertEqual(len(fake.requests), 1)
        req, timeout = fake.requests[0]
        self.assertEqual(req.full_url, "https://api.deepseek.com/chat/completions")
        self.assertEqual(timeout, 10)
        self.assertEqual(json.loads(req.data)["thinking"], {"type": "disabled"})
        self.assertTrue(req.get_header("Authorization").startswith("Bearer "))
        self.assertEqual([x["event"] for x in self.ledger()], ["request", "usage"])
        self.assertEqual(self.ledger()[1]["completion_tokens"], 100)
        self.assertNotIn("secret-for-offline-tests", self.log.read_text())
        self.assertNotIn("周末", self.log.read_text())
        self.assertEqual(self.log.stat().st_mode & 0o777, 0o600)

    def test_daily_limit_2000_accepted_and_used_in_request(self):
        cfg = copy.deepcopy(self.cfg)
        cfg["llm"]["deepseek"]["daily_request_limit"] = 2000
        payload = build_payload(CTX, cfg)
        fake = FakeOpener(Response(self.make_result()))
        raw = request_deepseek(payload, cfg, opener=fake)
        self.assertEqual(json.loads(raw)["action"], "skip")
        self.assertEqual(len(fake.requests), 1)
        self.assertEqual([record["event"] for record in self.ledger()],
                         ["request", "usage"])

    def test_daily_limit_2001_rejected_before_http_or_ledger(self):
        cfg = copy.deepcopy(self.cfg)
        cfg["llm"]["deepseek"]["daily_request_limit"] = 2001
        with self.assertRaisesRegex(ValueError, "daily request limit"):
            build_payload(CTX, cfg)
        self.assertFalse(self.log.exists())

    def test_daily_limit_boolean_rejected(self):
        cfg = copy.deepcopy(self.cfg)
        cfg["llm"]["deepseek"]["daily_request_limit"] = True
        with self.assertRaisesRegex(ValueError, "daily request limit"):
            build_payload(CTX, cfg)
        self.assertFalse(self.log.exists())

    def test_daily_cap_checked_before_http(self):
        payload = build_payload(CTX, self.cfg)
        fake = FakeOpener(Response(self.make_result()))
        request_deepseek(payload, self.cfg, opener=fake)
        request_deepseek(payload, self.cfg, opener=fake)
        with self.assertRaisesRegex(RuntimeError, "cap reached"):
            request_deepseek(payload, self.cfg, opener=fake)
        self.assertEqual(len(fake.requests), 2)
        self.assertEqual(sum(1 for r in self.ledger() if r["event"] == "request"), 2)

    def add_ledger_request(self, age_seconds):
        timestamp = datetime.now(timezone.utc) - timedelta(seconds=age_seconds)
        record = {
            "event": "request", "day": timestamp.date().isoformat(),
            "at_utc": timestamp.isoformat(), "model": "deepseek-flash",
        }
        with self.log.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")

    def test_default_live_limits_are_bounded(self):
        config = copy.deepcopy(self.cfg)
        del config["llm"]["deepseek"]["min_request_interval_seconds"]
        del config["llm"]["deepseek"]["max_requests_per_60_seconds"]
        self.add_ledger_request(2)
        fake = FakeOpener(Response(self.make_result()))
        with self.assertRaises(ModelRateLimited) as caught:
            request_deepseek(build_payload(CTX, config), config, opener=fake)
        self.assertGreater(caught.exception.retry_after_seconds, 15)
        self.assertEqual(len(fake.requests), 0)
        self.assertEqual(len(self.ledger()), 1)

    def test_rolling_window_limit_survives_restart_and_blocks_http(self):
        config = copy.deepcopy(self.cfg)
        config["llm"]["deepseek"]["daily_request_limit"] = 2000
        config["llm"]["deepseek"]["max_requests_per_60_seconds"] = 3
        for age in (55, 40, 25):
            self.add_ledger_request(age)
        fake = FakeOpener(Response(self.make_result()))
        with self.assertRaises(ModelRateLimited) as caught:
            request_deepseek(build_payload(CTX, config), config, opener=fake)
        self.assertGreater(caught.exception.retry_after_seconds, 4)
        self.assertLess(caught.exception.retry_after_seconds, 7)
        self.assertEqual(len(fake.requests), 0)
        self.assertEqual(len(self.ledger()), 3)

    def test_rolling_window_crosses_utc_midnight(self):
        fixed = datetime(2026, 11, 22, 0, 0, 5, tzinfo=timezone.utc)
        before_midnight = fixed - timedelta(seconds=10)
        self.log.write_text(json.dumps({
            "event": "request", "day": before_midnight.date().isoformat(),
            "at_utc": before_midnight.isoformat(), "model": "deepseek-flash",
        }) + "\n")
        opts = copy.deepcopy(self.cfg["llm"]["deepseek"])
        opts["min_request_interval_seconds"] = 20
        with patch("wechat_ai_bot.decision_engine.datetime") as clock:
            clock.now.return_value = fixed
            clock.fromisoformat.side_effect = datetime.fromisoformat
            clock.fromordinal.side_effect = datetime.fromordinal
            with self.assertRaises(ModelRateLimited):
                _reserve_request(opts)
        self.assertEqual(len(self.ledger()), 1)

    def test_rolling_window_releases_after_expiration(self):
        config = copy.deepcopy(self.cfg)
        config["llm"]["deepseek"]["daily_request_limit"] = 2000
        config["llm"]["deepseek"]["max_requests_per_60_seconds"] = 3
        for age in (120, 90, 70):
            self.add_ledger_request(age)
        fake = FakeOpener(Response(self.make_result()))
        request_deepseek(build_payload(CTX, config), config, opener=fake)
        self.assertEqual(len(fake.requests), 1)
        self.assertEqual(sum(r["event"] == "request" for r in self.ledger()), 4)

    def test_daily_cap_is_explicit_and_never_touches_http(self):
        self.add_ledger_request(30)
        self.add_ledger_request(20)
        fake = FakeOpener(Response(self.make_result()))
        with self.assertRaises(ModelBudgetExhausted) as caught:
            request_deepseek(build_payload(CTX, self.cfg), self.cfg, opener=fake)
        self.assertGreater(caught.exception.retry_after_seconds, 1)
        self.assertEqual(len(fake.requests), 0)
        self.assertEqual(len(self.ledger()), 2)

    def test_rate_limit_propagates_safe_wait_metadata(self):
        def blocked(_payload, _cfg):
            raise ModelRateLimited(19)
        with self.assertRaises(DecisionDeferred) as caught:
            decide(CTX, self.cfg, blocked)
        self.assertEqual(caught.exception.code, "model_rate_limited")
        self.assertEqual(caught.exception.retry_after_seconds, 19)
        self.assertNotIn("secret-for-offline-tests", str(caught.exception))

    def test_invalid_rate_settings_rejected_before_http(self):
        cfg = copy.deepcopy(self.cfg)
        cfg["llm"]["deepseek"]["max_requests_per_60_seconds"] = True
        with self.assertRaisesRegex(ValueError, "rolling request limit"):
            build_payload(CTX, cfg)
        self.assertFalse(self.log.exists())

    def test_token_budget_does_not_repeat_api_after_empty_response(self):
        payload = build_payload(CTX, self.cfg)
        fake = FakeOpener(Response(self.make_result("  ")))
        with self.assertRaisesRegex(ValueError, "Incomplete DeepSeek"):
            request_deepseek(payload, self.cfg, opener=fake)
        self.assertEqual(len(fake.requests), 1)
        self.assertEqual([r["event"] for r in self.ledger()], ["request"])

    def test_bad_origin_fails_before_charge(self):
        cfg = copy.deepcopy(self.cfg)
        cfg["llm"]["deepseek"]["base_url"] = "http://some-other-host"
        with self.assertRaisesRegex(ValueError, "origin"):
            build_payload(CTX, cfg)
        self.assertFalse(self.log.exists())

    def test_bad_key_permissions_refuse_without_http(self):
        self.key.chmod(0o644)
        fake = FakeOpener(Response(self.make_result()))
        with self.assertRaisesRegex(ValueError, "permissions"):
            request_deepseek(build_payload(CTX, self.cfg), self.cfg, opener=fake)
        self.assertEqual(len(fake.requests), 0)
        self.assertFalse(self.log.exists())

    def test_redirected_response_rejected(self):
        fake = FakeOpener(Response(self.make_result(), url="https://example.com/"))
        with self.assertRaisesRegex(ValueError, "origin mismatch"):
            request_deepseek(build_payload(CTX, self.cfg), self.cfg, opener=fake)
        self.assertEqual(len(fake.requests), 1)
        self.assertEqual(len(self.ledger()), 1)

    def test_model_routing_can_be_switched_back(self):
        config = copy.deepcopy(self.cfg)
        self.assertEqual(selected_provider(config), "deepseek")
        config["llm"]["provider"] = "ollama"
        self.assertEqual(selected_model(config), "qwen3.5:9b")
        self.assertIn("format", build_payload(CTX, config))

    def test_decide_preserves_reply_or_skip_contract(self):
        reply = '{"action":"reply","reason":"topic","reply":"可以看喜剧"}'
        parsed = decide(CTX, self.cfg, lambda payload, cfg: reply)
        self.assertEqual(parsed["action"], "reply")
        self.assertEqual(parsed["reply"], "可以看喜剧")
        with self.assertRaises(DecisionDeferred):
            decide(CTX, self.cfg, lambda payload, cfg: '{"action":"send"}')

    def test_missing_ledger_is_created_without_credentials(self):
        request_deepseek(build_payload(CTX, self.cfg), self.cfg,
                         opener=FakeOpener(Response(self.make_result())))
        self.assertTrue(self.log.exists())
        self.assertEqual(self.log.stat().st_mode & 0o777, 0o600)

    def test_deepseek_model_saved_in_decision_without_outbox(self):
        import sqlite3
        from wechat_ai_bot.storage import Storage
        from wechat_ai_bot.migrations import migrate_v2_to_v3, migrate_v3_to_v4
        from wechat_ai_bot.process_pending import process_one
        from test_process_pending import DDL, CFG as BASE_CFG
        db = Path(self.tmp.name) / "integration.sqlite3"
        with sqlite3.connect(db) as conn:
            conn.executescript(DDL)
            migrate_v2_to_v3(conn)
            migrate_v3_to_v4(conn)
        cfg = copy.deepcopy(BASE_CFG)
        cfg["llm"] = copy.deepcopy(self.cfg["llm"])
        store = Storage(db)
        try:
            store.conn.execute(
                "INSERT INTO messages "
                "(account_id,message_id,thread_id,sender_id,direction,body,source_cursor) "
                "VALUES(?,?,?,?,?,?,?)",
                ("a", "m-deepseek", "g", "member", "incoming", "周末有推荐的电影吗？", "42"),
            )
            result = process_one(store, cfg, lambda c, x: {
                "action":"reply", "reason":"topic", "reply":"试试轻松的喜剧"
            })
            self.assertEqual(result["status"], "saved")
            row = store.conn.execute(
                "SELECT action,status,model FROM decisions"
            ).fetchone()
            self.assertEqual(tuple(row), ("reply", "proposed", "deepseek-flash"))
            self.assertEqual(store.conn.execute("SELECT COUNT(*) FROM outbox").fetchone()[0], 0)
        finally:
            store.close()


if __name__ == "__main__":
    unittest.main()
