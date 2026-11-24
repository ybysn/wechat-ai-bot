"""P6.5D2C: exercise Sender/Storage lifecycle over *synthetic loopback HTTP*.

The adapter is deliberately test-only. It refuses real account/thread IDs,
real message text, production SQLite files, and the real daemon port 6174.
It never reads a credential file and is never imported by Worker or Consumer.
A future production Sender needs an independent safety and code review.
"""

from __future__ import annotations

from wechat_ai_bot.sender_http_contract import (
    build_send_lookup_request, build_text_send_request, inspect_send_receipt,
)
from wechat_ai_bot.sender_http_offline import _require_temporary_storage
from wechat_ai_bot.sender_http_transport import (
    HttpExchangeResult, RestrictedHttpExecutor, mock_request,
)


FIXTURE_ACCOUNT = "fixture-p65d2c-account"
FIXTURE_THREAD = "fixture-p65d2c-thread"
FIXTURE_TEXT_PREFIX = "P65D2C-SYNTHETIC:"


def _inspect(response, row):
    if type(response) is not HttpExchangeResult:
        return None
    if type(response.status) is not int or not 200 <= response.status < 300:
        return None
    receipt = inspect_send_receipt(
        response.body,
        expected_account_id=row["account_id"],
        expected_thread_id=row["thread_id"],
        expected_text=row["content"],
        expected_key=row["idempotency_key"],
    )
    return receipt if receipt.may_mark_sent else None


def simulate_fixture_http_send_once(
    store, outbox_id, config, executor, *, now=None,
):
    """One synthetic POST and at most one GET on a non-daemon loopback port.

    Return the existing Storage (changed:bool, reason:str) contract. Every
    ambiguous remote result is quarantined. No automatic POST retry is allowed.
    """
    if type(executor) is not RestrictedHttpExecutor or executor._mock_port is None:
        raise PermissionError("Fixture-only RestrictedHttpExecutor required")
    _require_temporary_storage(store)
    if (
        not isinstance(config, dict)
        or config.get("agent_wechat", {}).get("account_id") != FIXTURE_ACCOUNT
        or config.get("agent_wechat", {}).get("target_group_id") != FIXTURE_THREAD
    ):
        raise PermissionError("Synthetic fixture account and thread required")

    row = store.conn.execute(
        "SELECT account_id, thread_id, content, idempotency_key "
        "FROM outbox WHERE outbox_id=?", (outbox_id,),
    ).fetchone()
    if row is None:
        return False, "outbox_missing"
    if (
        row["account_id"] != FIXTURE_ACCOUNT
        or row["thread_id"] != FIXTURE_THREAD
        or not isinstance(row["content"], str)
        or not row["content"].startswith(FIXTURE_TEXT_PREFIX)
    ):
        raise PermissionError("Only synthetic fixture content is permitted")

    post = build_text_send_request(
        account_id=row["account_id"], thread_id=row["thread_id"],
        text=row["content"], idempotency_key=row["idempotency_key"],
        token="synthetic",
    )
    lookup = build_send_lookup_request(
        account_id=row["account_id"], idempotency_key=row["idempotency_key"],
        token="synthetic",
    )
    post_mock = mock_request(post, executor._mock_port)
    lookup_mock = mock_request(lookup, executor._mock_port)

    claimed, reason = store.claim_outbox_for_send(outbox_id, config, now=now)
    if not claimed:
        return False, reason

    verified = None
    try:
        verified = _inspect(executor.perform(post_mock), row)
    except Exception:
        # The HTTP response may have been lost after acceptance.
        pass
    if verified is None:
        try:
            verified = _inspect(executor.perform(lookup_mock), row)
        except Exception:
            pass

    if verified is None:
        return store.quarantine_unresolved_send(outbox_id)
    return store.record_send_result(
        outbox_id, row["idempotency_key"], "accepted",
        remote_send_id=verified.remote_send_id, now=now,
    )
