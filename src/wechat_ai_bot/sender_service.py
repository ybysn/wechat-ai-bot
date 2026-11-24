"""Isolated, policy-gated Sender for automatic replies and scheduled text."""

from contextlib import closing
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from http.client import HTTPException
import json
import os
from pathlib import Path
import pwd
import signal
import sqlite3
import stat
import sys
from threading import Event
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import ProxyHandler, Request, build_opener

from wechat_ai_bot.outbound_safety import GROUP_SEND_COOLDOWN_SECONDS, _timestamp
from wechat_ai_bot.scheduled_plans import TIME, validate_limits, validate_messages
from wechat_ai_bot.sender_automatic_policy import (
    automatic_dispatch_lock, pause_automatic_reply, read_automatic_reply_policy,
)
from wechat_ai_bot.sender_http_contract import BASE_URL, build_send_lookup_request, build_text_send_request
from wechat_ai_bot.sender_http_transport import _NeverRedirect, _read_json_response
from wechat_ai_bot.sender_role_boundary import BoundaryFailure, _no_duplicate_keys, _observe_target
from wechat_ai_bot.sender_schedule import ScheduledHistory, ZONE, scheduled_occurrences
from wechat_ai_bot.storage import Storage


CONFIG_DIRECTORY = Path("/etc/wechat-ai-bot")
OBSERVER_PATH = CONFIG_DIRECTORY / "observer.json"
POLICY_PATH = CONFIG_DIRECTORY / "automatic-reply.json"
SETTINGS_PATH = CONFIG_DIRECTORY / "sender-settings.json"
STATE_DIRECTORY = Path("/var/lib/wechat-ai-bot/sender")
DB_DIRECTORY = Path("/var/lib/wechat-ai-bot/store")


def read_protected_config(path):
    directory = os.open(path.parent, os.O_RDONLY | os.O_NOFOLLOW | os.O_DIRECTORY)
    try:
        parent = os.fstat(directory)
        if parent.st_uid != 0 or parent.st_mode & 0o022:
            raise BoundaryFailure("SENDER_CONFIG_DIRECTORY_UNSAFE")
        fd = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
        with os.fdopen(fd, "rb") as stream:
            info = os.fstat(stream.fileno())
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or info.st_nlink != 1
                    or stat.S_IMODE(info.st_mode) != 0o640):
                raise BoundaryFailure("SENDER_CONFIG_FILE_UNSAFE")
            data = stream.read(512 * 1024 + 1)
        if len(data) > 512 * 1024:
            raise BoundaryFailure("SENDER_CONFIG_TOO_LARGE")
        return json.loads(data, object_pairs_hook=_no_duplicate_keys)
    finally:
        os.close(directory)


def validate_settings(settings, group):
    if not isinstance(settings, dict) or settings.keys() != {
        "targetGroupId", "replyWindowEnabled", "replyStart", "replyEnd", "checkinEnabled", "checkinTime",
        "limits", "scheduledMessages",
    } or settings["targetGroupId"] != group:
        raise BoundaryFailure("SENDER_SETTINGS_TARGET_MISMATCH")
    if any(type(settings[key]) is not bool for key in ("replyWindowEnabled", "checkinEnabled")):
        raise BoundaryFailure("SENDER_SETTINGS_INVALID")
    if any(not isinstance(settings[key], str) or not TIME.fullmatch(settings[key])
           for key in ("replyStart", "replyEnd", "checkinTime")):
        raise BoundaryFailure("SENDER_SETTINGS_INVALID")
    if settings["replyWindowEnabled"] and settings["replyStart"] == settings["replyEnd"]:
        raise BoundaryFailure("SENDER_REPLY_WINDOW_INVALID")
    validate_messages(settings["scheduledMessages"])
    validate_limits(settings["limits"])
    return settings


def reply_window_open(settings, now):
    if not settings["replyWindowEnabled"]:
        return True
    start, end = settings["replyStart"], settings["replyEnd"]
    local = now.astimezone(ZONE).strftime("%H:%M")
    return start <= local < end if start < end else local >= start or local < end


def _native_id(value):
    return (isinstance(value, str) and value.isascii() and 0 < len(value) <= 512
            and all(33 <= ord(char) <= 126 for char in value))


class NativeSender:
    """Construct only account-scoped Native requests; confirm delivery by two reads."""

    def __init__(self, account, group, token):
        self.account, self.group, self.token = account, group, token
        self.account_url = BASE_URL + "/accounts/" + quote(account, safe="")
        self.opener = build_opener(ProxyHandler({}), _NeverRedirect())

    def _get(self, suffix=""):
        request = Request(self.account_url + suffix, headers={"Authorization": "Bearer " + self.token})
        return self._exchange(request)

    def _exchange(self, request):
        try:
            try:
                response = self.opener.open(request, timeout=4)
            except HTTPError as exc:
                response = exc
            with response:
                if response.geturl() != request.full_url:
                    return None, None
                result = _read_json_response(response)
                return result.status, result.body if isinstance(result.body, dict) else None
        except (OSError, HTTPException, ValueError, RecursionError):
            return None, None

    def ready(self):
        status, info = self._get()
        return (status == 200 and info is not None and info.get("id") == self.account
                and info.get("connection") == "online" and info.get("authenticated") is True
                and info.get("storageMode") == "managed")

    def _bound(self, claim):
        if (claim.account_id, claim.thread_id) != (self.account, self.group):
            raise BoundaryFailure("SENDER_CLAIM_TARGET_MISMATCH")
        request = build_text_send_request(account_id=self.account, thread_id=self.group, text=claim.text,
                                          idempotency_key=claim.idempotency_key, token=self.token)
        if sha256(request.data).hexdigest() != claim.request_sha256:
            raise BoundaryFailure("SENDER_CLAIM_DIGEST_MISMATCH")
        return request

    def _lookup(self, claim):
        request = build_send_lookup_request(account_id=self.account, idempotency_key=claim.idempotency_key,
                                            token=self.token)
        return self._exchange(request)

    def can_lookup(self, key):
        request = build_send_lookup_request(account_id=self.account, idempotency_key=key, token=self.token)
        return self._exchange(request)[0] == 404

    def _receipt_id(self, value):
        if (value is not None and value.get("accountId") == self.account
                and value.get("destination") == self.group and _native_id(value.get("id"))):
            return value["id"]
        return None

    def submit_once(self, claim):
        request = self._bound(claim)
        status, value = self._lookup(claim)
        if status != 404:
            return self._receipt_id(value) if status == 200 else None
        status, value = self._exchange(request)
        return self._receipt_id(value) if status == 202 else None

    def confirm(self, claim, known_id):
        self._bound(claim)
        status, receipt = self._lookup(claim)
        remote_id = self._receipt_id(receipt) if status == 200 else None
        if remote_id is None or (known_id is not None and known_id != remote_id):
            return known_id, False
        message_id, server_id = receipt.get("localMessageId"), receipt.get("serverId")
        if (receipt.get("state") != "accepted" or receipt.get("errorCode") is not None
                or not _native_id(message_id) or not _native_id(server_id)):
            return remote_id, False
        status, message = self._get("/messages/" + quote(message_id, safe=""))
        return remote_id, (
            status == 200 and message is not None and message.get("id") == message_id
            and message.get("serverId") == server_id and message.get("accountId") == self.account
            and message.get("threadId") == self.group and message.get("direction") == "outgoing"
            and type(message.get("type")) is int and message["type"] == 1
            and message.get("recall") is None and message.get("body") == claim.text
        )


class Dispatcher:
    def __init__(self, store, history, native, config, controls, now):
        self.store, self.history, self.native = store, history, native
        self.config, self.controls, self.now = config, controls, now
        self.account, self.group = _observe_target(config)

    def _scheduled_result(self, claim, remote_id):
        remote_id, confirmed = self.native.confirm(claim, remote_id)
        self.history.finish(claim, remote_id, self.now().isoformat() if confirmed else None)
        return "scheduled_sent" if confirmed else "scheduled_needs_review"

    def _reply_result(self, claim, remote_id):
        remote_id, confirmed = self.native.confirm(claim, remote_id)
        status = "sent" if confirmed else "needs_review"
        if not self.store.finish_automatic_reply(claim, status, remote_send_id=remote_id, now=self.now()):
            raise RuntimeError("AUTOMATIC_DISPATCH_STATE_CHANGED")
        return "reply_" + status

    def _main_conflict(self, now):
        rows = self.store.conn.execute(
            "SELECT status,sent_at FROM outbox WHERE account_id=? AND thread_id=? "
            "AND status IN ('sending','needs_review','sent')", (self.account, self.group),
        )
        for row in rows:
            stamped = _timestamp(row["sent_at"])
            if row["status"] != "sent" or stamped is None:
                return "outbox_unresolved"
            if (now - stamped).total_seconds() < GROUP_SEND_COOLDOWN_SECONDS:
                return "group_send_cooldown"
        return None

    def _reply_limit(self, settings, now):
        limits = settings["limits"]
        hour = now - timedelta(hours=1)
        day = now.astimezone(ZONE).replace(hour=0, minute=0, second=0, microsecond=0)
        hourly, daily, latest = self.store.conn.execute(
            "SELECT COUNT(CASE WHEN julianday(sent_at)>=julianday(?) THEN 1 END), "
            "COUNT(CASE WHEN julianday(sent_at)>=julianday(?) THEN 1 END), MAX(unixepoch(sent_at)) "
            "FROM outbox WHERE account_id=? AND thread_id=? AND status='sent' AND decision_id IS NOT NULL",
            (hour.isoformat(), day.isoformat(), self.account, self.group),
        ).fetchone()
        return (hourly >= limits["replyHourly"] or daily >= limits["replyDaily"]
                or latest is not None and now.timestamp() - latest < limits["replyInterval"])

    def tick(self):
        # If a prior process reserved a send, recovery may read its receipt only.
        pending = self.history.pending()
        if pending is not None:
            return self._scheduled_result(pending, pending.remote_send_id)
        pending = self.store.unresolved_automatic_dispatch(self.account, self.group)
        if pending is not None:
            return self._reply_result(pending, pending.remote_send_id)

        policy, settings = self.controls()
        now = self.now()
        if not policy.permits(self.config, now):
            return "sender_paused"
        conflict = self._main_conflict(now)
        if conflict:
            return conflict
        last_scheduled = _timestamp(self.history.last_sent_at(self.account, self.group))
        if last_scheduled is not None and (now - last_scheduled).total_seconds() < GROUP_SEND_COOLDOWN_SECONDS:
            return "group_send_cooldown"
        due = next((claim for claim in scheduled_occurrences(settings, policy, now)
                    if not self.history.exists(claim.idempotency_key)), None)
        reply_allowed = reply_window_open(settings, now) and not self._reply_limit(settings, now)
        if due is None and not reply_allowed:
            return "waiting_for_schedule"
        if not self.native.ready():
            return "native_offline"
        if self.controls() != (policy, settings):
            return "sender_settings_changed"
        if due is not None:
            now = self.now()
            if not policy.permits(self.config, now) or due not in scheduled_occurrences(settings, policy, now):
                return "schedule_expired"
            if not self.history.reserve(due):
                return "schedule_already_reserved"
            return self._scheduled_result(due, self.native.submit_once(due))

        claim, reason = self.store.claim_automatic_reply(self.config, policy, now=self.now())
        if claim is None:
            return reason
        try:
            latest_policy, latest_settings = self.controls()
            now = self.now()
            permitted = (latest_settings == settings and reply_window_open(settings, now)
                         and self.store.recheck_automatic_reply(claim, self.config, latest_policy, now=now) is None)
        except (OSError, ValueError, RecursionError):
            permitted = False
        if not permitted:
            if not self.store.finish_automatic_reply(claim, "cancelled"):
                raise RuntimeError("AUTOMATIC_DISPATCH_STATE_CHANGED")
            return "sender_settings_changed"
        return self._reply_result(claim, self.native.submit_once(claim))


def _run(*, check=False):
    if os.geteuid() != pwd.getpwnam("wechat-sender").pw_uid:
        raise BoundaryFailure("SENDER_SERVICE_IDENTITY_REQUIRED")
    config = read_protected_config(OBSERVER_PATH)
    account, group = _observe_target(config)
    if config["agent_wechat"]["base_url"].rstrip("/") != BASE_URL:
        raise BoundaryFailure("SENDER_NATIVE_ORIGIN_MISMATCH")
    db = Path(config["storage"]["database"])
    if db.parent != DB_DIRECTORY or db.resolve(strict=True) != db:
        raise BoundaryFailure("SENDER_DATABASE_PATH_UNSAFE")
    token_path = Path(os.environ["CREDENTIALS_DIRECTORY"]) / "native-token"
    with token_path.open("rb") as stream:
        value = stream.read(513)
    token = value.decode("ascii").strip()
    if len(value) > 512 or not _native_id(token):
        raise BoundaryFailure("SENDER_CREDENTIAL_INVALID")
    native = NativeSender(account, group, token)
    stop = Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())

    def controls():
        if read_protected_config(OBSERVER_PATH) != config:
            raise BoundaryFailure("SENDER_OBSERVER_CONFIG_CHANGED")
        return read_automatic_reply_policy(POLICY_PATH), validate_settings(read_protected_config(SETTINGS_PATH), group)

    if check:
        from secrets import token_hex

        policy, _ = controls()
        if (policy.account_id, policy.thread_id) != (account, group):
            raise BoundaryFailure("SENDER_POLICY_TARGET_MISMATCH")
        with automatic_dispatch_lock(POLICY_PATH) as locked, closing(Storage(db)) as store:
            if not locked or not store.automatic_schema:
                raise BoundaryFailure("SENDER_PREFLIGHT_NOT_READY")
            if store.conn.execute(
                "SELECT 1 FROM outbox WHERE account_id=? AND thread_id=? "
                "AND status IN ('queued','sending','needs_review') LIMIT 1", (account, group),
            ).fetchone():
                raise BoundaryFailure("SENDER_PREFLIGHT_OUTBOX_NOT_EMPTY")
            if not native.ready() or not native.can_lookup("readiness-" + token_hex(16)):
                raise BoundaryFailure("SENDER_NATIVE_NOT_READY")
        print("SENDER_NATIVE_AND_STORAGE_READY")
        return

    with closing(Storage(db)) as store, closing(ScheduledHistory(STATE_DIRECTORY / "schedule.sqlite3")) as history:
        if not store.automatic_schema:
            raise BoundaryFailure("SENDER_REQUIRES_V5")
        dispatcher = Dispatcher(store, history, native, config, controls, lambda: datetime.now(timezone.utc))
        last = None
        while not stop.is_set():
            try:
                with automatic_dispatch_lock(POLICY_PATH) as locked:
                    status = dispatcher.tick() if locked else "sender_busy"
            except (BoundaryFailure, OSError, ValueError, RecursionError):
                status = "sender_configuration_unavailable"
            except sqlite3.OperationalError as exc:
                if (getattr(exc, "sqlite_errorcode", 0) & 0xff) not in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED):
                    raise
                status = "sender_database_busy"
            if status != last:
                print(status, flush=True)
                last = status
            stop.wait(5)


if __name__ == "__main__":
    try:
        if sys.argv[1:] == ["--pause"]:
            pause_automatic_reply(POLICY_PATH)
            print("SENDER_PAUSED")
        elif sys.argv[1:] in ([], ["--check"]):
            _run(check=bool(sys.argv[1:]))
        else:
            raise BoundaryFailure("SENDER_USAGE_INVALID")
    except (BoundaryFailure, OSError, ValueError, KeyError, RuntimeError, sqlite3.Error) as exc:
        print(f"Sender 启动或运行失败（{type(exc).__name__}）", file=sys.stderr)
        raise SystemExit(1) from None
