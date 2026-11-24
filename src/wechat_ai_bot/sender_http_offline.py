"""P6.5D2B2: in-memory HTTP exchange for Sender lifecycle verification.

This is deliberately NOT a network transport. ScriptedHttpExecutor has no socket,
HTTP opener, subprocess, or credential source. A real Sender must be a separate,
reviewed release, and must enforce safety again immediately before POST.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from tempfile import gettempdir
from urllib.parse import urlparse

from wechat_ai_bot.sender_http_contract import (
    BASE_URL,
    build_send_lookup_request,
    build_text_send_request,
    inspect_send_receipt,
)


@dataclass(frozen=True)
class ScriptedHttpResponse:
    status: int
    body: object


class ScriptedHttpExecutor:
    """Only consumes scripted responses: never opens a network connection.

    Each script item is a (HTTP verb, ScriptedHttpResponse | Exception) pair.
    The exact next verb must match the Request built by the contract layer.
    No tokens or body text are stored in the call history.
    """

    def __init__(self, steps):
        if not isinstance(steps, (tuple, list)):
            raise TypeError("Script must be a list/tuple")
        self._steps = list(steps)
        self.calls = []

    def perform(self, request):
        method = request.get_method()
        parsed = urlparse(request.full_url)
        if parsed.scheme != "http" or parsed.netloc != "127.0.0.1:6174":
            raise ValueError("Unexpected HTTP target")
        if not request.full_url.startswith(BASE_URL + "/accounts/"):
            raise ValueError("Unexpected API path")
        if method not in ("GET", "POST"):
            raise ValueError("Unsupported method")
        if not self._steps:
            raise RuntimeError("No synthetic response available")
        expected_method, result = self._steps.pop(0)
        if method != expected_method:
            raise AssertionError("Unexpected synthetic HTTP method")
        # Keep only method/path (not URL query, auth header, request body).
        self.calls.append((method, parsed.path))
        if isinstance(result, Exception):
            raise result
        if type(result) is not ScriptedHttpResponse:
            raise TypeError("Invalid synthetic response")
        return result

    @property
    def remaining_steps(self):
        return len(self._steps)


def _require_temporary_storage(store):
    """No production DB can be claimed or mutated by the offline adapter."""
    conn = getattr(store, "conn", None)
    if conn is None:
        raise PermissionError("Temporary Storage fixture required")
    dbs = conn.execute("PRAGMA database_list").fetchall()
    if len(dbs) != 1 or dbs[0][1] != "main" or not dbs[0][2]:
        raise PermissionError("Isolated file-backed SQLite fixture required")
    root = Path(gettempdir()).resolve()
    db = Path(dbs[0][2]).resolve()
    if (
        not db.is_relative_to(root) or db.parent == root
        or not db.parent.name.startswith(("tmp", "wechat-p65d2-"))
    ):
        raise PermissionError("Offline adapter requires a temporary SQLite fixture")


def _verified_receipt(response, *, account_id, thread_id, text, key):
    if type(response) is not ScriptedHttpResponse:
        return None
    if type(response.status) is not int or not 200 <= response.status < 300:
        return None
    inspected = inspect_send_receipt(
        response.body,
        expected_account_id=account_id,
        expected_thread_id=thread_id,
        expected_text=text,
        expected_key=key,
    )
    return inspected if inspected.may_mark_sent else None


def simulate_http_send_once(
    store, outbox_id, config, executor, *, token="synthetic-token", now=None,
):
    """Claim on temp SQLite; one synthetic POST, at most one synthetic GET.

    Only an *exact* ScriptedHttpExecutor is accepted. It cannot make network
    calls. An incomplete, malformed, failed or ambiguous result is quarantined;
    there is no re-submit, retry loop, nor optimistic `sent` transition.
    Returns the existing Storage (changed: bool, reason: str) tuple.
    """
    if type(executor) is not ScriptedHttpExecutor:
        raise TypeError("Only ScriptedHttpExecutor is allowed")
    _require_temporary_storage(store)
    # Fail *before claim* if this contract cannot even build a request. The
    # Outbox row is still guarded by the claim transaction below.
    row = store.conn.execute(
        "SELECT account_id,thread_id,content,idempotency_key "
        "FROM outbox WHERE outbox_id=?", (outbox_id,),
    ).fetchone()
    if row is None:
        return False, "outbox_missing"
    account_id, thread_id, content, key = (
        row["account_id"], row["thread_id"],
        row["content"], row["idempotency_key"],
    )
    post = build_text_send_request(
        account_id=account_id, thread_id=thread_id,
        text=content, idempotency_key=key, token=token,
    )
    lookup = build_send_lookup_request(
        account_id=account_id, idempotency_key=key, token=token,
    )
    claimed, reason = store.claim_outbox_for_send(outbox_id, config, now=now)
    if not claimed:
        return False, reason

    verified = None
    try:
        result = executor.perform(post)
        verified = _verified_receipt(
            result, account_id=account_id, thread_id=thread_id,
            text=content, key=key,
        )
    except Exception:
        # A POST might have reached the daemon. Never blindly POST twice.
        pass

    if verified is None:
        try:
            result = executor.perform(lookup)
            verified = _verified_receipt(
                result, account_id=account_id, thread_id=thread_id,
                text=content, key=key,
            )
        except Exception:
            pass

    if verified is None:
        return store.quarantine_unresolved_send(outbox_id)
    return store.record_send_result(
        outbox_id, key, "accepted", remote_send_id=verified.remote_send_id,
        now=now,
    )
