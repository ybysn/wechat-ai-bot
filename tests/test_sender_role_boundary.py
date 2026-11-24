"""P6.5D2D-2A: offline configuration-role contract tests."""

import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from wechat_ai_bot.sender_role_boundary import (
    BoundaryFailure, inspect_boundary, main,
)


class RoleBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.observe = self.root / "config.json"
        self.sender = self.root / "sender-runtime.json"
        self.config = {
            "schema_version": 2,
            "app": {"auto_send": False},
            "sender": {"enabled": False},
            "runtime": {"global_pause": True},
            "agent_wechat": {"account_id": "fixture-account", "target_group_id": "fixture-group"},
            "safety": {"group_allowlist": ["fixture-group"]},
        }
        self.policy = {
            "schema_version": 1, "role": "sender", "enabled": False,
            "account_id": "fixture-account", "thread_id": "fixture-group",
        }
        self.write_observe()

    def write_observe(self):
        self.observe.write_text(json.dumps(self.config), encoding="utf-8")

    def write_sender(self):
        self.sender.write_text(json.dumps(self.policy), encoding="utf-8")
        self.sender.chmod(0o600)

    def blocked(self, code):
        with self.assertRaises(BoundaryFailure) as exc:
            inspect_boundary(self.observe, self.sender)
        self.assertEqual(str(exc.exception), code)

    def test_absent_sidecar_reports_safe_but_denied(self):
        report = inspect_boundary(self.observe, self.sender)
        self.assertEqual(report["verdict"], "SENDER_POLICY_ABSENT")
        self.assertFalse(report["live_send_authorized"])
        self.assertEqual((report["network_requests"], report["sql_writes"]), (0, 0))

    def test_disabled_sender_sidecar_still_denies_live_send(self):
        self.write_sender()
        result = inspect_boundary(self.observe, self.sender)
        self.assertEqual(result["verdict"], "SENDER_POLICY_DISABLED")
        self.assertFalse(result["live_send_authorized"])

    def test_observe_flags_must_remain_off(self):
        for key, value in (("auto_send", True),):
            self.config["app"][key] = value
            self.write_observe()
            self.blocked("OBSERVE_MODE_NOT_SAFE")
        self.config["app"]["auto_send"] = False
        self.config["sender"]["enabled"] = True
        self.write_observe()
        self.blocked("OBSERVE_MODE_NOT_SAFE")
        self.config["sender"]["enabled"] = False
        self.config["runtime"]["global_pause"] = False
        self.write_observe()
        self.blocked("OBSERVE_MODE_NOT_SAFE")

    def test_sender_enabled_cannot_authorize(self):
        self.policy["enabled"] = True
        self.write_sender()
        self.blocked("SENDER_ENABLEMENT_UNSUPPORTED")

    def test_sender_must_match_account(self):
        self.policy["account_id"] = "other"
        self.write_sender()
        self.blocked("SENDER_POLICY_TARGET_MISMATCH")

    def test_sender_must_match_group(self):
        self.policy["thread_id"] = "other"
        self.write_sender()
        self.blocked("SENDER_POLICY_TARGET_MISMATCH")

    def test_group_must_be_allowlisted(self):
        self.config["safety"]["group_allowlist"] = []
        self.write_observe()
        self.blocked("OBSERVE_TARGET_INVALID")

    def test_sidecar_must_be_private(self):
        self.write_sender()
        self.sender.chmod(0o644)
        self.blocked("SENDER_POLICY_PERMISSIONS_INVALID")

    def test_sidecar_symlink_forbidden(self):
        self.write_sender()
        link = self.root / "linked-sender.json"
        link.symlink_to(self.sender)
        with self.assertRaisesRegex(BoundaryFailure, "SYMLINK_FORBIDDEN"):
            inspect_boundary(self.observe, link)

    def test_same_source_and_sender_path_forbidden(self):
        with self.assertRaisesRegex(BoundaryFailure, "CONFIG_PATHS_MUST_DIFFER"):
            inspect_boundary(self.observe, self.observe)

    def test_sender_unknown_keys_forbidden(self):
        self.policy["token_file"] = "/fake/token"
        self.write_sender()
        self.blocked("SENDER_POLICY_SCHEMA_INVALID")

    def test_sender_duplicate_json_keys_rejected(self):
        self.sender.write_text('{"role":"sender","role":"sender"}')
        self.sender.chmod(0o600)
        self.blocked("CONFIG_UNREADABLE_OR_INVALID")

    def test_invalid_sender_boolean_rejected(self):
        self.policy["enabled"] = 0
        self.write_sender()
        self.blocked("SENDER_POLICY_SCHEMA_INVALID")

    def test_sender_oversized_policy_rejected(self):
        self.sender.write_text(" " * 8193)
        self.sender.chmod(0o600)
        self.blocked("CONFIG_TOO_LARGE")

    def test_no_mutation_of_configs(self):
        self.write_sender()
        before = self.observe.read_bytes(), self.sender.read_bytes()
        inspect_boundary(self.observe, self.sender)
        self.assertEqual(before, (self.observe.read_bytes(), self.sender.read_bytes()))

    def test_cli_returns_no_secret_or_identity(self):
        from io import StringIO
        output = StringIO()
        with patch("sys.stdout", output):
            self.assertEqual(main([
                "--config", str(self.observe),
                "--sender-policy", str(self.sender),
            ]), 0)
        self.assertNotIn("fixture-account", output.getvalue())
        self.assertNotIn("fixture-group", output.getvalue())
        self.assertFalse(json.loads(output.getvalue())["live_send_authorized"])

    def test_no_network_or_token_dependency(self):
        with patch("socket.socket", side_effect=AssertionError("network forbidden")), \
             patch("urllib.request.urlopen", side_effect=AssertionError("network forbidden")):
            result = inspect_boundary(self.observe, self.sender)
        self.assertFalse(result["token_read"])

    def test_missing_observe_fails_closed(self):
        self.observe.unlink()
        self.blocked("CONFIG_UNREADABLE_OR_INVALID")

    def test_invalid_observe_schema_rejected(self):
        self.config["schema_version"] = True
        self.write_observe()
        self.blocked("OBSERVE_MODE_NOT_SAFE")

    def test_observe_duplicate_json_keys_rejected(self):
        self.observe.write_text('{"schema_version":2,"schema_version":2}')
        self.blocked("CONFIG_UNREADABLE_OR_INVALID")


if __name__ == "__main__":
    unittest.main()
