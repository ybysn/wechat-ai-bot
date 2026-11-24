"""Exercise the real offline verifier with temporary OpenSSH keys and SQLite."""

from contextlib import closing
from dataclasses import replace
from hashlib import sha256
import json
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from test_storage import V2_DDL
from wechat_ai_bot.migrations import migrate_v2_to_v3, migrate_v3_to_v4
from wechat_ai_bot.sender_approval_envelope import (
    APPROVAL_NAMESPACE, ApprovalCandidate, ApprovalEnvelopeError, verify_approval_envelope,
)
from wechat_ai_bot.sender_http_contract import build_text_send_request


_KEYGEN = "/usr/bin/ssh-keygen"


def _invoke_keygen(*args, data=None):
    return subprocess.run(
        [_KEYGEN, *args], input=data, capture_output=True, check=True,
        timeout=5, env={"LC_ALL": "C"},
    ).stdout


def _canonical(fields):
    return json.dumps(fields, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("ascii")


@unittest.skipUnless(Path(_KEYGEN).is_file(), "OpenSSH is required for real signature tests")
class ApprovalEnvelopeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        temp = tempfile.TemporaryDirectory(prefix="wx-approval-envelope-tests-")
        cls.addClassCleanup(temp.cleanup)
        cls.trusted_key = str(Path(temp.name) / "trusted")
        cls.other_key = str(Path(temp.name) / "other")
        for key in (cls.trusted_key, cls.other_key):
            _invoke_keygen("-q", "-t", "ed25519", "-N", "", "-C", "fixture", "-f", key)
        public_path = cls.trusted_key + ".pub"
        cls.public_key = " ".join(Path(public_path).read_text().split()[:2])
        cls.key_id = _invoke_keygen("-l", "-E", "sha256", "-f", public_path).decode().split()[1]

    def setUp(self):
        self.now = 1_795_315_000
        self.actor_id = "fixture-operator"
        self.candidate = ApprovalCandidate(
            environment_id="fixture-environment", control_epoch=2,
            account_id="fixture-account", thread_id="fixture-group@chatroom",
            outbox_id="fixture-outbox", idempotency_key="send-" + "af" * 16,
            source_id="fixture-decision", source_cursor="101", activation_cursor="100",
            text=" 合成正文\nexact\tbytes ",
        )
        candidate = self.candidate
        request = build_text_send_request(
            account_id=candidate.account_id, thread_id=candidate.thread_id,
            text=candidate.text, idempotency_key=candidate.idempotency_key, token="synthetic",
        )
        self.fields = {
            "schema_version": 1, "action": "approve",
            "approval_id": "1234567890ab4cde89abcdef12345678", "actor_id": self.actor_id,
            "key_id": self.key_id, "source_kind": "ai_reply", "max_sends": 1,
            "issued_at": self.now, "expires_at": self.now + 60,
            "content_sha256": sha256(candidate.text.encode()).hexdigest(),
            "request_sha256": sha256(request.data).hexdigest(),
            **{k: v for k, v in vars(candidate).items() if k != "text"},
        }

    def sign(self, payload, *, key=None, namespace=APPROVAL_NAMESPACE):
        return _invoke_keygen("-Y", "sign", "-f", key or self.trusted_key, "-n", namespace, data=payload)

    def verify(self, payload, signature, **overrides):
        return verify_approval_envelope(payload, signature, **{
            "candidate": self.candidate, "actor_id": self.actor_id,
            "public_key": self.public_key, "now": self.now, **overrides,
        })

    def rejected(self, code, payload, signature=b"unused", **overrides):
        with self.assertRaises(ApprovalEnvelopeError) as raised:
            self.verify(payload, signature, **overrides)
        self.assertEqual(str(raised.exception), code)

    def test_valid_proof_does_not_consume_approval(self):
        payload = _canonical(self.fields)
        signature = self.sign(payload)
        first = self.verify(payload, signature)
        self.assertEqual(first, self.verify(payload, signature))
        self.assertEqual(first.approval_id, self.fields["approval_id"])
        self.assertEqual(first.key_id, self.key_id)
        self.assertEqual(first.payload_sha256, sha256(payload).hexdigest())
        self.assertFalse(first.live_send_authorized)

    def test_actual_signature_rejects_wrong_key_namespace_and_tampering(self):
        payload = _canonical(self.fields)
        for signature in (
            self.sign(payload, key=self.other_key),
            self.sign(payload, namespace="wx-chat-bot-control-v1"),
            self.sign(_canonical({**self.fields, "expires_at": self.now + 61})),
            b"not an OpenSSH signature",
        ):
            self.rejected("INVALID_SIGNATURE", payload, signature)

    def test_principal_fingerprint_and_public_key_syntax_are_pinned(self):
        payload = _canonical(self.fields)
        self.rejected("SIGNER_MISMATCH", payload, actor_id="different-principal")
        self.rejected("SIGNER_MISMATCH", _canonical({**self.fields, "key_id": "untrusted"}))
        self.rejected("INVALID_VERIFICATION_CONTEXT", payload, actor_id="fixture-*")
        self.rejected("INVALID_TRUST_KEY", payload, public_key=self.public_key + "\nother-key")

    def test_signed_but_invalid_format_is_rejected(self):
        payload = _canonical(self.fields)
        for data, code in (
            (b'{"actor_id":"fixture-operator",' + payload[1:], "DUPLICATE_FIELD"),
            (payload + b"\n", "NONCANONICAL_JSON"),
            (_canonical({**self.fields, "text": "extra"}), "INVALID_FIELDS"),
            (_canonical({k: v for k, v in self.fields.items() if k != "actor_id"}), "INVALID_FIELDS"),
            (_canonical({**self.fields, "control_epoch": True}), "INVALID_FIELD_TYPE"),
            (_canonical({**self.fields, "max_sends": 1.0}), "INVALID_FIELD_TYPE"),
            (_canonical({**self.fields, "account_id": "fixture\u0000account"}), "INVALID_FIELD_VALUE"),
            (_canonical({**self.fields, "outbox_id": "\ud800"}), "INVALID_FIELD_VALUE"),
            (b"[" * 1500, "INVALID_JSON"),
            (b" " * (16 * 1024 + 1), "INVALID_PAYLOAD_SIZE"),
        ):
            with self.subTest(code=code):
                self.rejected(code, data, self.sign(data))

    def test_signed_out_of_scope_approval_is_rejected(self):
        for changes, code in (
            ({"max_sends": 2}, "INVALID_APPROVAL"),
            ({"action": "stop"}, "INVALID_APPROVAL"),
            ({"source_kind": "scheduled"}, "INVALID_APPROVAL"),
            ({"idempotency_key": "old-uuid5-key"}, "INVALID_APPROVAL"),
            ({"approval_id": "malformed-id"}, "INVALID_APPROVAL"),
            ({"expires_at": self.now + 121}, "INVALID_TTL"),
            ({"expires_at": self.now}, "INVALID_TTL"),
            ({"source_cursor": "100"}, "INVALID_CURSOR"),
            ({"source_cursor": "0101"}, "INVALID_CURSOR"),
            ({"source_cursor": "9223372036854775808"}, "INVALID_CURSOR"),
        ):
            with self.subTest(changes=changes):
                data = _canonical({**self.fields, **changes})
                self.rejected(code, data, self.sign(data))

    def test_time_window_includes_issuance_and_excludes_expiry(self):
        payload = _canonical(self.fields)
        signature = self.sign(payload)
        self.verify(payload, signature, now=self.now + 59)
        for now in (self.now - 1, self.now + 60):
            self.rejected("APPROVAL_OUTSIDE_WINDOW", payload, signature, now=now)

    def test_candidate_and_request_digest_use_independent_values(self):
        payload = _canonical(self.fields)
        signature = self.sign(payload)
        for changes in (
            {"account_id": "other-account"}, {"thread_id": "other-group"},
            {"outbox_id": "other-outbox"}, {"idempotency_key": "send-" + "bf" * 16},
            {"source_id": "other-decision"}, {"source_cursor": "102"},
            {"activation_cursor": "99"}, {"environment_id": "other-environment"},
            {"control_epoch": 3},
        ):
            with self.subTest(changes=changes):
                self.rejected("CANDIDATE_MISMATCH", payload, signature, candidate=replace(self.candidate, **changes))
        self.rejected("CONTENT_DIGEST_MISMATCH", payload, signature,
                      candidate=replace(self.candidate, text=self.candidate.text.strip()))
        data = _canonical({**self.fields, "request_sha256": "0" * 64})
        self.rejected("REQUEST_DIGEST_MISMATCH", data, self.sign(data))

    def test_verifier_timeout_or_missing_binary_fails_closed(self):
        payload = _canonical(self.fields)
        for failure in (OSError("redacted"), subprocess.TimeoutExpired("redacted", 5)):
            with patch("wechat_ai_bot.sender_approval_envelope.subprocess.run", side_effect=failure):
                self.rejected("VERIFIER_UNAVAILABLE", payload, b"placeholder")
        self.rejected("INVALID_SIGNATURE_SIZE", payload, b"s" * 4097)

    def test_signed_candidate_read_from_sqlite_rejects_later_edit(self):
        candidate = self.candidate
        payload = _canonical(self.fields)
        signature = self.sign(payload)
        with tempfile.TemporaryDirectory() as root, closing(sqlite3.connect(Path(root) / "test.sqlite3")) as db:
            db.executescript(V2_DDL)
            migrate_v2_to_v3(db)
            migrate_v3_to_v4(db)
            db.execute(
                "INSERT INTO messages(account_id,message_id,thread_id,source_cursor) VALUES (?,?,?,?)",
                (candidate.account_id, "fixture-message", candidate.thread_id, candidate.source_cursor),
            )
            db.execute(
                "INSERT INTO decisions(decision_id,account_id,source_message_id,action,reply) VALUES (?,?,?,'reply',?)",
                (candidate.source_id, candidate.account_id, "fixture-message", candidate.text),
            )
            db.commit()

            def read_candidate():
                account, thread, decision, cursor, text = db.execute(
                    "SELECT d.account_id,m.thread_id,d.decision_id,m.source_cursor,d.reply "
                    "FROM decisions d JOIN messages m ON m.account_id=d.account_id "
                    "AND m.message_id=d.source_message_id WHERE d.decision_id=?",
                    (candidate.source_id,),
                ).fetchone()
                return replace(candidate, account_id=account, thread_id=thread,
                               source_id=decision, source_cursor=cursor, text=text)

            changes = db.total_changes
            self.verify(payload, signature, candidate=read_candidate())
            self.assertEqual(db.total_changes, changes)
            db.execute("UPDATE decisions SET reply=? WHERE decision_id=?", ("合成正文更改后", candidate.source_id))
            self.rejected("CONTENT_DIGEST_MISMATCH", payload, signature, candidate=read_candidate())
            self.assertEqual(db.execute("SELECT COUNT(*) FROM outbox").fetchone()[0], 0)
