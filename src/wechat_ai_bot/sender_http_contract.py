"""P6.5D2B1 Agent WeChat HTTP send *contract*, intentionally network-free.

The Worker never imports this module and no code here invokes urlopen() or a
transport. Constructing a request MUST NOT be confused with authorizing send.
The v2.9.0 source audit is recorded in docs/agent-wechat-v2.9.0-protocol-audit.md.
Its native receipt lacks the echo fields required below; this inspector still
serves the synthetic contract and must NOT mark a native receipt successful.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from unicodedata import category
from urllib.parse import quote, urlencode
from urllib.request import Request


BASE_URL = "http://127.0.0.1:6174/v1"


class SendContractError(ValueError):
    """Invalid or unverified send contract, without echoing sensitive data."""


def _id(value, name):
    if (
        not isinstance(value, str) or not value or len(value) > 512
        or any(ord(c) < 32 or ord(c) == 127 for c in value)
    ):
        raise SendContractError(f"Invalid {name}")
    return value


def _idempotency_key(value):
    # v2.9.0 wechat-api::idempotency_key: at most 200 ASCII graphic/space bytes.
    value = _id(value, "idempotency key")
    if len(value) > 200 or any(not 32 <= ord(c) <= 126 for c in value):
        raise SendContractError("Invalid idempotency key")
    return value


def _destination(value):
    value = _id(value, "thread ID")
    try:
        size = len(value.encode("utf-8"))
    except UnicodeError as exc:
        raise SendContractError("Invalid thread ID") from exc
    if size > 256 or any(c.isspace() or category(c) == "Cc" for c in value):
        raise SendContractError("Invalid thread ID")
    return value


def _auth_headers(token):
    if not isinstance(token, str) or not token or token.strip() != token:
        raise SendContractError("Invalid bearer token")
    if any(ord(c) < 33 or ord(c) > 126 for c in token):
        raise SendContractError("Invalid bearer token")
    return {
        "Authorization": "Bearer " + token,
        "Accept": "application/json",
    }


def _root(base_url):
    # No user-supplied remote host, proxy, HTTP->HTTPS redirect, or path variant.
    if base_url != BASE_URL:
        raise SendContractError("Unexpected Agent WeChat API base URL")
    return base_url


def encode_text_send_body(*, thread_id: str, text: str) -> bytes:
    """Encode the exact text body shared by approval and request construction."""
    thread = _destination(thread_id)
    if not isinstance(text, str) or not text.strip() or len(text) > 4000:
        raise SendContractError("Invalid text length")
    try:
        return json.dumps(
            {"threadId": thread, "text": text},
            ensure_ascii=False, separators=(",", ":"),
        ).encode("utf-8")
    except UnicodeError:
        raise SendContractError("Invalid text encoding") from None


def build_text_send_request(
    *, account_id: str, thread_id: str, text: str,
    idempotency_key: str, token: str, base_url: str = BASE_URL,
) -> Request:
    """Return an HTTP Request, WITHOUT opening a connection or sending.

    Must be used only after independent persisted Outbox claim + security checks.
    No quote, @mentions, media, retry, or send execution is implemented.
    """
    root = _root(base_url)
    account = quote(_id(account_id, "account ID"), safe="")
    key = _idempotency_key(idempotency_key)
    body = encode_text_send_body(thread_id=thread_id, text=text)
    headers = _auth_headers(token)
    headers["Content-Type"] = "application/json; charset=utf-8"
    headers["Idempotency-Key"] = key
    return Request(
        f"{root}/accounts/{account}/sends",
        data=body, headers=headers, method="POST",
    )


def build_send_lookup_request(
    *, account_id: str, idempotency_key: str, token: str,
    base_url: str = BASE_URL,
) -> Request:
    """Build a read-only same-key receipt lookup; never invoke it here."""
    root = _root(base_url)
    account = quote(_id(account_id, "account ID"), safe="")
    key = _idempotency_key(idempotency_key)
    query = urlencode({"key": key})
    return Request(
        f"{root}/accounts/{account}/sends/by-idempotency-key?{query}",
        headers=_auth_headers(token), method="GET",
    )


@dataclass(frozen=True)
class InspectedReceipt:
    remote_state: str
    remote_send_id: str | None
    identity_verified: bool
    may_mark_sent: bool
    reason: str


def inspect_send_receipt(
    payload: object, *, expected_account_id: str,
    expected_thread_id: str, expected_text: str, expected_key: str,
) -> InspectedReceipt:
    """Fail closed until remote record proves key + exact content/target.

    The audited v2.9.0 native record has destination, not threadId, and has no
    text/idempotencyKey echoes. A same-key lookup returns that same shape, so it
    cannot supply the missing proof for this synthetic inspector. Production
    reconciliation needs a separate design binding the authenticated lookup to
    the persisted request and approval. Never fabricate echoes from local data.
    Even a verified `accepted` is server acceptance, not user delivery.
    """
    for name, value in (
        ("account ID", expected_account_id),
        ("thread ID", expected_thread_id),
        ("idempotency key", expected_key),
    ):
        _id(value, name)
    if not isinstance(expected_text, str) or not expected_text:
        raise SendContractError("Missing expected content")
    if not isinstance(payload, dict):
        return InspectedReceipt("unknown", None, False, False, "invalid_receipt")
    state = payload.get("state")
    if state not in (
        "accepted", "failed", "needs_review", "cancelled", "queued",
        "submitting", "sending",  # Native state; legacy synthetic state.
    ):
        state = "unknown"
    remote_id = payload.get("id")
    if not isinstance(remote_id, str) or not remote_id:
        remote_id = None
    # Never infer identity from an HTTP 200 or from a remote ID alone.
    verified = (
        payload.get("accountId") == expected_account_id
        and payload.get("threadId") == expected_thread_id
        and payload.get("text") == expected_text
        and payload.get("idempotencyKey") == expected_key
    )
    accepted = bool(verified and state == "accepted" and remote_id)
    return InspectedReceipt(
        state, remote_id, verified, accepted,
        "verified_server_acceptance" if accepted else "requires_receipt_reconciliation",
    )
