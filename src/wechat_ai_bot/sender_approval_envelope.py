"""Offline verification of one v1 approval against independently supplied context.

The caller supplies the pinned public key, candidate, control generation and
clock. Verification neither consumes an approval nor grants dispatch access.
"""

from __future__ import annotations

import base64
from dataclasses import dataclass, field
from hashlib import sha256
import json
from pathlib import Path
import re
import subprocess
from tempfile import TemporaryDirectory
from unicodedata import category

from wechat_ai_bot.sender_http_contract import SendContractError, encode_text_send_body


APPROVAL_NAMESPACE = "wx-chat-bot-approval-v1"
_SSH_KEYGEN = "/usr/bin/ssh-keygen"
_MAX_INT = 2**63 - 1
_INT_FIELDS = frozenset({"schema_version", "control_epoch", "issued_at", "expires_at", "max_sends"})
_STR_FIELDS = frozenset({
    "action", "approval_id", "environment_id", "actor_id", "key_id", "account_id",
    "thread_id", "outbox_id", "idempotency_key", "source_kind", "source_id",
    "source_cursor", "activation_cursor", "content_sha256", "request_sha256",
})
# SSH binary strings carry a four-byte big-endian length before their data.
_ED25519_PREFIX = b"\0\0\0\x0bssh-ed25519\0\0\0\x20"
_ED25519_LINE = re.compile(r"ssh-ed25519 ([A-Za-z0-9+/]{68})")
_PRINCIPAL = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.@-]{0,127}")
_CURSOR = re.compile(r"(?:0|[1-9][0-9]{0,18})")
_APPROVAL_ID = re.compile(r"[0-9a-f]{12}4[0-9a-f]{3}[89ab][0-9a-f]{15}")
_SEND_KEY = re.compile(r"send-[0-9a-f]{32}")


class ApprovalEnvelopeError(ValueError):
    """Fixed rejection code safe to log without envelope or verifier output."""


@dataclass(frozen=True, repr=False)
class ApprovalCandidate:
    environment_id: str
    control_epoch: int
    account_id: str
    thread_id: str
    outbox_id: str
    idempotency_key: str
    source_id: str
    source_cursor: str
    activation_cursor: str
    text: str


@dataclass(frozen=True, repr=False)
class ApprovedEnvelopeEvidence:
    """Verification evidence only. The same signed bytes can verify again."""

    approval_id: str
    actor_id: str
    key_id: str
    expires_at: int
    payload_sha256: str
    live_send_authorized: bool = field(default=False, init=False)


def _unique_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ApprovalEnvelopeError("DUPLICATE_FIELD")
        result[key] = value
    return result


def _parse(payload: bytes) -> dict[str, str | int]:
    if type(payload) is not bytes or not 0 < len(payload) <= 16 * 1024:
        raise ApprovalEnvelopeError("INVALID_PAYLOAD_SIZE")
    try:
        fields = json.loads(payload.decode("ascii"), object_pairs_hook=_unique_keys)
    except ApprovalEnvelopeError:
        raise
    except (ValueError, RecursionError):
        raise ApprovalEnvelopeError("INVALID_JSON") from None
    if not isinstance(fields, dict) or fields.keys() != _INT_FIELDS | _STR_FIELDS:
        raise ApprovalEnvelopeError("INVALID_FIELDS")
    if any(type(fields[k]) is not int or not 0 <= fields[k] <= _MAX_INT for k in _INT_FIELDS):
        raise ApprovalEnvelopeError("INVALID_FIELD_TYPE")
    if any(type(fields[k]) is not str or not fields[k] for k in _STR_FIELDS):
        raise ApprovalEnvelopeError("INVALID_FIELD_TYPE")
    if any(len(fields[k]) > 512 or any(category(c) in ("Cc", "Cs") for c in fields[k])
           for k in _STR_FIELDS):
        raise ApprovalEnvelopeError("INVALID_FIELD_VALUE")
    canonical = json.dumps(
        fields, ensure_ascii=True, sort_keys=True, separators=(",", ":"), allow_nan=False,
    ).encode("ascii")
    if payload != canonical:
        raise ApprovalEnvelopeError("NONCANONICAL_JSON")
    return fields


def _pinned_key(public_key: str) -> tuple[str, str]:
    # Construct the allowed-signers entry from one exact key, without accepting
    # comments, wildcard identities, options or a file from the envelope.
    match = _ED25519_LINE.fullmatch(public_key) if type(public_key) is str else None
    if match is None:
        raise ApprovalEnvelopeError("INVALID_TRUST_KEY")
    encoded = match[1]
    blob = base64.b64decode(encoded, validate=True)
    if not blob.startswith(_ED25519_PREFIX) or len(blob) != len(_ED25519_PREFIX) + 32:
        raise ApprovalEnvelopeError("INVALID_TRUST_KEY")
    fingerprint = base64.b64encode(sha256(blob).digest()).decode("ascii").rstrip("=")
    return public_key, "SHA256:" + fingerprint


def _check_candidate(fields, candidate: ApprovalCandidate, now: int) -> None:
    if (fields["schema_version"] != 1 or fields["action"] != "approve"
            or fields["source_kind"] != "ai_reply" or fields["max_sends"] != 1
            or not _APPROVAL_ID.fullmatch(fields["approval_id"])
            or not _SEND_KEY.fullmatch(fields["idempotency_key"])):
        raise ApprovalEnvelopeError("INVALID_APPROVAL")
    issued_at, expires_at = fields["issued_at"], fields["expires_at"]
    if not 1 <= expires_at - issued_at <= 120:
        raise ApprovalEnvelopeError("INVALID_TTL")
    if not issued_at <= now < expires_at:
        raise ApprovalEnvelopeError("APPROVAL_OUTSIDE_WINDOW")

    source, activation = fields["source_cursor"], fields["activation_cursor"]
    if not _CURSOR.fullmatch(source) or not _CURSOR.fullmatch(activation):
        raise ApprovalEnvelopeError("INVALID_CURSOR")
    if not int(activation) < int(source) <= _MAX_INT:
        raise ApprovalEnvelopeError("INVALID_CURSOR")

    names = (
        "environment_id", "control_epoch", "account_id", "thread_id", "outbox_id",
        "idempotency_key", "source_id", "source_cursor", "activation_cursor",
    )
    if any(fields[name] != getattr(candidate, name) for name in names):
        raise ApprovalEnvelopeError("CANDIDATE_MISMATCH")
    try:
        body = encode_text_send_body(thread_id=candidate.thread_id, text=candidate.text)
        content = candidate.text.encode("utf-8")
    except (SendContractError, UnicodeError):
        raise ApprovalEnvelopeError("INVALID_CANDIDATE_TEXT") from None
    if fields["content_sha256"] != sha256(content).hexdigest():
        raise ApprovalEnvelopeError("CONTENT_DIGEST_MISMATCH")
    if fields["request_sha256"] != sha256(body).hexdigest():
        raise ApprovalEnvelopeError("REQUEST_DIGEST_MISMATCH")


def _verify_signature(payload: bytes, signature: bytes, *, actor_id: str, public_key: str) -> None:
    if type(signature) is not bytes or not 0 < len(signature) <= 4096:
        raise ApprovalEnvelopeError("INVALID_SIGNATURE_SIZE")
    try:
        with TemporaryDirectory(prefix="wx-verify-approval-") as directory:
            allowed = Path(directory) / "allowed_signers"
            allowed.write_text(f'{actor_id} namespaces="{APPROVAL_NAMESPACE}" {public_key}\n', encoding="ascii")
            signature_path = Path(directory) / "signature"
            signature_path.write_bytes(signature)
            result = subprocess.run(
                [_SSH_KEYGEN, "-Y", "verify", "-f", str(allowed), "-I", actor_id,
                 "-n", APPROVAL_NAMESPACE, "-s", str(signature_path)],
                input=payload, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                timeout=5, check=False, env={"LC_ALL": "C"},
            )
    except (OSError, subprocess.TimeoutExpired):
        raise ApprovalEnvelopeError("VERIFIER_UNAVAILABLE") from None
    if result.returncode != 0:
        raise ApprovalEnvelopeError("INVALID_SIGNATURE")


def verify_approval_envelope(
    payload: bytes, signature: bytes, *, candidate: ApprovalCandidate,
    actor_id: str, public_key: str, now: int,
) -> ApprovedEnvelopeEvidence:
    """Verify immutable evidence using caller-supplied trust and UTC epoch seconds.

    Keep `now`, `candidate`, `actor_id` and `public_key` independent of `payload`.
    The future storage transaction owns replay protection and send admission.
    """
    if (type(candidate) is not ApprovalCandidate or type(candidate.control_epoch) is not int
            or type(now) is not int or not 0 <= now <= _MAX_INT
            or type(actor_id) is not str or not _PRINCIPAL.fullmatch(actor_id)):
        raise ApprovalEnvelopeError("INVALID_VERIFICATION_CONTEXT")
    fields = _parse(payload)
    key, fingerprint = _pinned_key(public_key)
    if fields["actor_id"] != actor_id or fields["key_id"] != fingerprint:
        raise ApprovalEnvelopeError("SIGNER_MISMATCH")
    _check_candidate(fields, candidate, now)
    _verify_signature(payload, signature, actor_id=actor_id, public_key=key)
    return ApprovedEnvelopeEvidence(
        fields["approval_id"], actor_id, fingerprint, fields["expires_at"], sha256(payload).hexdigest(),
    )
