"""ADR feasibility probes, NOT a production approval issuer or verifier.

All keys are newly generated test keys in private temporary directories.
These tests do not authenticate a human or establish cross-UID isolation.
Existing atomic-fixture tests cover the separate durable one-use property.
"""

from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

from wechat_ai_bot.sender_http_contract import build_text_send_request


SSH_KEYGEN = Path("/usr/bin/ssh-keygen")
APPROVAL_NAMESPACE = "wx-chat-bot-approval-v1"
CONTROL_NAMESPACE = "wx-chat-bot-control-v1"


def canonical_json(value):
    return json.dumps(
        value, ensure_ascii=True, sort_keys=True, separators=(",", ":"),
        allow_nan=False,
    ).encode("ascii")


@unittest.skipUnless(SSH_KEYGEN.is_file(), "OpenSSH ssh-keygen is required for ADR probes")
class ApprovalDesignSignatureTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix="wx-approval-signature-probe-")
        cls.addClassCleanup(cls.temp.cleanup)
        cls.root = Path(cls.temp.name)
        cls.trusted_key = cls.root / "fixture-trusted-key"
        cls.rogue_key = cls.root / "fixture-untrusted-key"
        for key in (cls.trusted_key, cls.rogue_key):
            cls.run_keygen([
                "-q", "-t", "ed25519", "-N", "", "-C", "fixture-only", "-f", str(key),
            ], check=True)

    @staticmethod
    def run_keygen(args, *, data=None, check=False):
        return subprocess.run(
            [str(SSH_KEYGEN), *args], input=data, capture_output=True,
            timeout=5, check=check,
            env={**os.environ, "SSH_AUTH_SOCK": ""},
        )

    def setUp(self):
        self.allowed = self.root / "fixture-allowed-signers"
        self.write_trust(self.trusted_key)
        self.signature = self.root / "fixture-signature"
        self.plaintext = "P65-APPROVAL-DESIGN-SYNTHETIC:hello"
        request = build_text_send_request(
            account_id="fixture-approval-account", thread_id="fixture-approval-group@chatroom",
            text=self.plaintext, idempotency_key="fixture-approval-key", token="synthetic",
        )
        issued = int(datetime(2026, 11, 22, tzinfo=timezone.utc).timestamp())
        self.intent = {
            "schema_version": 1, "action": "approve", "approval_id": "fixture-approval-01",
            "environment_id": "fixture-installation", "control_epoch": 1,
            "actor_id": "fixture-operator", "key_id": "fixture-trusted-key",
            "account_id": "fixture-approval-account", "thread_id": "fixture-approval-group@chatroom",
            "outbox_id": "fixture-outbox-01", "idempotency_key": "fixture-approval-key",
            "source_kind": "ai_reply", "source_id": "fixture-decision-01",
            "source_cursor": "101", "activation_cursor": "100",
            "content_sha256": sha256(self.plaintext.encode()).hexdigest(),
            "request_sha256": sha256(request.data).hexdigest(),
            "issued_at": issued, "expires_at": issued + 60, "max_sends": 1,
        }

    def write_trust(self, key):
        public = key.with_suffix(".pub").read_text().strip()
        self.allowed.write_text(
            f'fixture-operator namespaces="{APPROVAL_NAMESPACE}" {public}\n',
        )
        self.allowed.chmod(0o600)

    def sign(self, data, *, key=None, namespace=APPROVAL_NAMESPACE):
        signed = self.run_keygen(
            ["-Y", "sign", "-f", str(key or self.trusted_key), "-n", namespace],
            data=data, check=True,
        )
        self.signature.write_bytes(signed.stdout)

    def verify(self, data, *, principal="fixture-operator", namespace=APPROVAL_NAMESPACE):
        result = self.run_keygen(
            ["-Y", "verify", "-f", str(self.allowed), "-I", principal,
             "-n", namespace, "-s", str(self.signature)], data=data,
        )
        return result.returncode == 0

    def test_signed_canonical_intent_verifies_with_pinned_signer(self):
        data = canonical_json(self.intent)
        self.sign(data)
        self.assertTrue(self.verify(data))

    def test_every_signed_field_is_bound(self):
        self.sign(canonical_json(self.intent))
        for key, value in self.intent.items():
            changed = {**self.intent, key: value + 1 if type(value) is int else value + "-changed"}
            with self.subTest(field=key):
                self.assertFalse(self.verify(canonical_json(changed)))

    def test_untrusted_signing_key_is_rejected(self):
        data = canonical_json(self.intent)
        self.sign(data, key=self.rogue_key)
        self.assertFalse(self.verify(data))

    def test_wrong_principal_is_rejected(self):
        data = canonical_json(self.intent)
        self.sign(data)
        self.assertFalse(self.verify(data, principal="fixture-other-operator"))

    def test_control_signature_cannot_be_used_as_approval(self):
        data = canonical_json(self.intent)
        self.sign(data, namespace=CONTROL_NAMESPACE)
        self.assertFalse(self.verify(data))

    def test_approval_signature_cannot_be_used_as_control(self):
        data = canonical_json(self.intent)
        self.sign(data)
        self.assertFalse(self.verify(data, namespace=CONTROL_NAMESPACE))

    def test_corrupted_signature_is_rejected(self):
        data = canonical_json(self.intent)
        self.sign(data)
        lines = self.signature.read_bytes().splitlines(keepends=True)
        lines[1] = (b"A" if lines[1][:1] != b"A" else b"B") + lines[1][1:]
        self.signature.write_bytes(b"".join(lines))
        self.assertFalse(self.verify(data))

    def test_canonical_order_is_stable_but_other_bytes_do_not_verify(self):
        data = canonical_json(self.intent)
        self.sign(data)
        reordered = dict(reversed(list(self.intent.items())))
        self.assertEqual(canonical_json(reordered), data)
        self.assertTrue(self.verify(canonical_json(reordered)))
        self.assertFalse(self.verify(json.dumps(self.intent, indent=2).encode()))

    def test_signature_alone_does_not_prevent_replay(self):
        # The second verification also succeeds: one-use must be enforced by
        # the same-database grant/Outbox transaction, not by this signature.
        data = canonical_json(self.intent)
        self.sign(data)
        self.assertTrue(self.verify(data))
        self.assertTrue(self.verify(data))

    def test_writable_trust_store_can_be_replaced_by_same_uid(self):
        # This deliberately demonstrates why 0600 is not a boundary against
        # automation sharing the owner UID. No production trust store is read.
        data = canonical_json(self.intent)
        self.sign(data, key=self.rogue_key)
        self.assertFalse(self.verify(data))
        self.write_trust(self.rogue_key)
        self.assertTrue(self.verify(data))

    def test_signature_alone_does_not_enforce_expiry_or_quota(self):
        # A cryptographically valid envelope can be semantically forbidden.
        # Production parsing/clock/one-shot checks remain future work.
        invalid = {**self.intent, "expires_at": self.intent["issued_at"] - 1, "max_sends": 2}
        data = canonical_json(invalid)
        self.sign(data)
        self.assertTrue(self.verify(data))

    def test_payload_contains_hashes_instead_of_message_body(self):
        data = canonical_json(self.intent)
        self.assertNotIn(self.plaintext.encode(), data)
        self.assertEqual(len(self.intent["content_sha256"]), 64)
        self.assertEqual(len(self.intent["request_sha256"]), 64)
        self.assertEqual(self.root.stat().st_mode & 0o777, 0o700)
        self.assertEqual(self.trusted_key.stat().st_mode & 0o777, 0o600)


if __name__ == "__main__":
    unittest.main()
