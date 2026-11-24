"""Offline driver for automatic replies on temporary SQLite and scripted HTTP.

An exact ScriptedHttpExecutor and synthetic account, group and text are required.
The only transport in this stage consumes in-memory responses without sockets.
"""

from contextlib import contextmanager
import fcntl
from hashlib import sha256
import os
from pathlib import Path
from urllib.parse import quote
from urllib.request import Request

from wechat_ai_bot.sender_automatic_policy import automatic_dispatch_lock, read_automatic_reply_policy
from wechat_ai_bot.sender_http_contract import (
    BASE_URL, build_send_lookup_request, build_text_send_request, encode_text_send_body,
)
from wechat_ai_bot.sender_http_fixture import FIXTURE_ACCOUNT, FIXTURE_TEXT_PREFIX, FIXTURE_THREAD
from wechat_ai_bot.sender_http_offline import ScriptedHttpExecutor, _require_temporary_storage
from wechat_ai_bot.sender_role_boundary import BoundaryFailure, _observe_target


@contextmanager
def _dispatch_lock(store):
    _require_temporary_storage(store)
    db = Path(store.conn.execute("PRAGMA database_list").fetchone()[2]).resolve()
    parent = db.parent.stat()
    if parent.st_uid != os.getuid() or parent.st_mode & 0o077 or db.stat().st_nlink != 1:
        raise PermissionError("Private temporary SQLite fixture required")
    fd = os.open(db.with_name(db.name + ".automatic.lock"), os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
        else:
            yield True
    finally:
        os.close(fd)


def _identity(value):
    return (type(value) is str and 0 < len(value) <= 512
            and value.isascii() and all(33 <= ord(c) <= 126 for c in value))


def _native_record(response, claim, status):
    if response is None or response.status != status or not isinstance(response.body, dict):
        return None
    row = response.body
    if (not _identity(row.get("id")) or row.get("accountId") != claim.account_id
            or row.get("destination") != claim.thread_id
            or row.get("state") not in ("queued", "submitting", "accepted", "needs_review", "failed", "cancelled")
            or (claim.remote_send_id is not None and row["id"] != claim.remote_send_id)):
        return None
    return row


def _exchange(executor, request):
    try:
        return executor.perform(request)
    except OSError:
        return None  # Transport lost; retrying POST would risk a duplicate.


def _projection_matches(executor, record, claim):
    message_id, server_id = record.get("localMessageId"), record.get("serverId")
    if not _identity(message_id) or not _identity(server_id) or record.get("errorCode") is not None:
        return False
    url = (f"{BASE_URL}/accounts/{quote(claim.account_id, safe='')}"
           f"/messages/{quote(message_id, safe='')}")
    response = _exchange(executor, Request(url, headers={"Authorization": "Bearer synthetic"}, method="GET"))
    if response is None or response.status != 200 or not isinstance(response.body, dict):
        return False
    message = response.body
    return (
        message.get("id") == message_id and message.get("serverId") == server_id
        and message.get("accountId") == claim.account_id and message.get("threadId") == claim.thread_id
        and message.get("direction") == "outgoing" and type(message.get("type")) is int
        and message["type"] == 1 and message.get("recall") is None and message.get("body") == claim.text
    )


def _reconcile(store, executor, claim, now, *, post_record=None):
    request = build_send_lookup_request(
        account_id=claim.account_id, idempotency_key=claim.idempotency_key, token="synthetic",
    )
    record = _native_record(_exchange(executor, request), claim, 200)
    known_id = claim.remote_send_id or (post_record["id"] if post_record else None)
    if record is not None and (known_id is None or record["id"] == known_id):
        known_id = record["id"]
        if record["state"] == "accepted" and _projection_matches(executor, record, claim):
            if store.finish_automatic_reply(claim, "sent", remote_send_id=known_id, now=now()):
                return "sent"
            return "automatic_claim_changed"
    if store.finish_automatic_reply(claim, "needs_review", remote_send_id=known_id):
        return "needs_review"
    return "automatic_claim_changed"


def _verify_synthetic_binding(claim):
    if (claim.account_id, claim.thread_id) != (FIXTURE_ACCOUNT, FIXTURE_THREAD):
        raise PermissionError("Synthetic target required")
    body = encode_text_send_body(thread_id=claim.thread_id, text=claim.text)
    if not claim.text.startswith(FIXTURE_TEXT_PREFIX) or sha256(body).hexdigest() != claim.request_sha256:
        raise PermissionError("Synthetic request binding required")


def run_automatic_fixture_tick(store, read_policy, executor, *, now):
    """Run one offline tick. Recovery of any prior claim is GET-only."""
    if type(executor) is not ScriptedHttpExecutor:
        raise TypeError("Only ScriptedHttpExecutor is allowed")
    with _dispatch_lock(store) as owned:
        if not owned:
            return "automatic_dispatch_busy"
        pending = store.unresolved_automatic_dispatch(FIXTURE_ACCOUNT, FIXTURE_THREAD)
        if pending is not None:
            _verify_synthetic_binding(pending)
            return _reconcile(store, executor, pending, now)

        config, policy = read_policy()
        if _observe_target(config) != (FIXTURE_ACCOUNT, FIXTURE_THREAD):
            raise PermissionError("Synthetic target required")
        claim, reason = store.claim_automatic_reply(config, policy, now=now())
        if claim is None:
            return reason
        _verify_synthetic_binding(claim)
        try:
            config, refreshed = read_policy()
        except (BoundaryFailure, OSError):
            store.finish_automatic_reply(claim, "cancelled")
            return "automatic_policy_unavailable"
        reason = store.recheck_automatic_reply(claim, config, refreshed, now=now())
        if reason is not None:
            store.finish_automatic_reply(claim, "cancelled")
            return reason
        request = build_text_send_request(
            account_id=claim.account_id, thread_id=claim.thread_id, text=claim.text,
            idempotency_key=claim.idempotency_key, token="synthetic",
        )
        response = _exchange(executor, request)
        post_record = _native_record(response, claim, 202)
        return _reconcile(store, executor, claim, now, post_record=post_record)


def run_controlled_automatic_fixture_tick(store, config, policy_path: Path, executor, *, now):
    """Rehearse the operator/dispatcher interlock in the temporary fixture directory."""
    _require_temporary_storage(store)
    database = Path(store.conn.execute("PRAGMA database_list").fetchone()[2]).resolve()
    if policy_path.parent != database.parent:
        raise PermissionError("Control files must share the temporary fixture directory")
    owner = os.getuid()

    def read_policy():
        return config, read_automatic_reply_policy(policy_path, owner_uid=owner)

    try:
        with automatic_dispatch_lock(policy_path, owner_uid=owner) as owned:
            if not owned:
                return "automatic_dispatch_busy"
            return run_automatic_fixture_tick(store, read_policy, executor, now=now)
    except BoundaryFailure as exc:
        return str(exc).lower()
