"""Long-running read-only hydrator and AI decision worker.

The worker never sends WeChat messages and never writes to the outbox.
A one-time history snapshot seeds context only; those messages are ignored.
Only events/messages newer than the durable activation cursor are eligible.
"""

from __future__ import annotations

import argparse
import fcntl
from contextlib import contextmanager
import json
import os
import signal
import sqlite3
import sys
import tempfile
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from wechat_ai_bot.consumer import (
    AgentWeChatAPI,
    FatalConsumerError,
    RetryableConsumerError,
    _observer_inputs,
    observer_state_directory,
    valid_cursor,
)
from wechat_ai_bot.hydrate_messages import persist_hydrated_message
from wechat_ai_bot.process_pending import process_one
from wechat_ai_bot.reply_safety import REPLY_FRESHNESS_SECONDS
from wechat_ai_bot.storage import Storage


HISTORY_CONTEXT_LIMIT = 20
HYDRATE_BATCH_LIMIT = 20
PENDING_SCAN_LIMIT = 50
EXPIRED_BATCH_LIMIT = 100
MAX_REPLY_AGE_SECONDS = REPLY_FRESHNESS_SECONDS
POLL_INTERVAL_SECONDS = 1.0
BACKOFF_INITIAL_SECONDS = 1.0
BACKOFF_MAX_SECONDS = 60.0
STATE_VERSION = 1
CONVERSATION_BATCH_LIMIT = 200
CONVERSATION_DEFAULT_QUIET_SECONDS = 8
CONVERSATION_DEFAULT_MAX_WAIT_SECONDS = 30


class WorkerStateError(RuntimeError):
    """The persisted worker activation state is invalid or inconsistent."""


def _atomic_write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd, temporary = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(value, stream, ensure_ascii=False, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def _read_state(path: Path, account_id: str, thread_id: str) -> dict | None:
    if not path.exists():
        return None
    if path.is_symlink() or not path.is_file():
        raise WorkerStateError("Worker state must be a regular non-symlink file")
    if path.stat().st_mode & 0o077:
        raise WorkerStateError("Worker state permissions must be 600 or stricter")
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise WorkerStateError("Worker state is unreadable or invalid JSON") from exc
    if not isinstance(state, dict) or type(state.get("state_version")) is not int or state.get("state_version") != STATE_VERSION:
        raise WorkerStateError("Unsupported worker state version")
    if state.get("account_id") != account_id or state.get("thread_id") != thread_id:
        raise WorkerStateError("Worker state belongs to another account or thread")
    phase = state.get("phase")
    if phase not in ("bootstrapping", "active"):
        raise WorkerStateError("Invalid worker state phase")
    cursor = state.get("activation_cursor")
    if not valid_cursor(cursor):
        raise WorkerStateError("Invalid worker activation cursor")
    Storage._cursor_number(cursor)
    count = state.get("history_message_count")
    if type(count) is not int or not 0 <= count <= HISTORY_CONTEXT_LIMIT:
        raise WorkerStateError("Invalid worker history message count")
    snapshot = state.get("snapshot_cursor")
    if not valid_cursor(snapshot):
        raise WorkerStateError("Invalid worker snapshot cursor")
    Storage._cursor_number(snapshot)
    if int(cursor) < int(snapshot):
        raise WorkerStateError("Worker boundary precedes snapshot")
    if not isinstance(state.get("source_generation"), str) or not state["source_generation"]:
        raise WorkerStateError("Worker history generation is missing")
    if phase == "bootstrapping":
        ids = state.get("history_message_ids")
        if isinstance(ids, list) and len(ids) != count:
            raise WorkerStateError("Worker history count mismatch")
        if (
            not isinstance(ids, list)
            or len(ids) > HISTORY_CONTEXT_LIMIT
            or any(not isinstance(item, str) or not item for item in ids)
            or len(set(ids)) != len(ids)
        ):
            raise WorkerStateError("Invalid pending history bootstrap IDs")
    return state


def _validate_history_detail(
    detail: object, *, account_id: str, thread_id: str, message_id: str,
    max_chars: int,
) -> dict:
    if not isinstance(detail, dict) or (
        detail.get("id") != message_id
        or detail.get("accountId") != account_id
        or detail.get("threadId") != thread_id
    ):
        raise ValueError("History detail identity mismatch")

    direction = detail.get("direction")
    body = detail.get("body")
    message_type = detail.get("type")
    sender_id = detail.get("senderId")
    created_at = detail.get("createdAt")
    if direction not in ("incoming", "outgoing", None):
        raise ValueError("Invalid history message direction")
    if body is not None and not isinstance(body, str):
        raise ValueError("Invalid history message body")
    if message_type is not None and type(message_type) is not int:
        raise ValueError("Invalid history message type")
    if sender_id is not None and not isinstance(sender_id, str):
        raise ValueError("Invalid history sender ID")
    if created_at is not None and not isinstance(created_at, str):
        raise ValueError("Invalid history created_at")

    mentions = detail.get("mentions")
    mentions_self = None
    if isinstance(mentions, dict) and mentions.get("state") == "known":
        mentions_self = mentions.get("self")
        if mentions_self is not None and type(mentions_self) is not bool:
            raise ValueError("Invalid history mention flag")

    if body is not None and (not body.strip() or len(body) > max_chars):
        body = None

    return {
        "direction": direction,
        "body": body,
        "message_type": message_type,
        "sender_id": sender_id,
        "created_at": created_at,
        "mentions_self": mentions_self,
    }


def _persist_history_context_message(
    store: Storage, *, account_id: str, thread_id: str,
    message_id: str, snapshot_cursor: str, detail: dict,
) -> None:
    """Persist history for context, but never make it AI-eligible."""
    store.persist_message(
        account_id,
        message_id,
        thread_id,
        direction=detail["direction"],
        body=detail["body"],
        sender_id=detail["sender_id"],
        message_type=detail["message_type"],
        mentions_self=detail["mentions_self"],
        created_at=detail["created_at"],
        source_cursor=None,
    )

    with store.transaction():
        row = store.conn.execute(
            "SELECT source_cursor,status FROM messages "
            "WHERE account_id=? AND message_id=?",
            (account_id, message_id),
        ).fetchone()
        if row is None:
            raise RuntimeError("History message disappeared after persistence")
        source_cursor = row["source_cursor"]
        if source_cursor is not None:
            if not valid_cursor(source_cursor) or int(source_cursor) > int(snapshot_cursor):
                raise ValueError("History message cursor exceeds snapshot boundary")
        store.conn.execute(
            "UPDATE messages SET "
            "source_cursor=COALESCE(source_cursor, ?), "
            "status=CASE WHEN status='pending' THEN 'ignored' ELSE status END "
            "WHERE account_id=? AND message_id=?",
            (snapshot_cursor, account_id, message_id),
        )


def _current_durable_cursor(store: Storage, account_id: str) -> str:
    checkpoint = store.checkpoint(account_id)
    if checkpoint is None or not valid_cursor(checkpoint.get("last_durable_cursor")):
        raise WorkerStateError("Consumer durable checkpoint is unavailable")
    durable = checkpoint["last_durable_cursor"]
    acked = checkpoint.get("last_acked_cursor")
    if acked is not None:
        if not valid_cursor(acked) or int(acked) > int(durable):
            raise WorkerStateError("Consumer ACK checkpoint is invalid")
    return durable


def ensure_activation_state(
    store: Storage, api: AgentWeChatAPI, config: dict, state_path: Path,
    *, logger=print, stop_event=None,
) -> dict:
    """Explicit one-time bootstrap for scheme B; never invoked by --run.

    The history snapshot establishes context. The *final* activation cursor
    is fixed after all 20 messages are seeded, so a slow/retried bootstrap
    does not accidentally convert contemporaneous messages into late replies.
    """
    wx = config["agent_wechat"]
    account_id = wx["account_id"]
    thread_id = wx["target_group_id"]
    state = _read_state(state_path, account_id, thread_id)

    if state is None:
        # Require an existing Consumer checkpoint: no standalone first-run
        # initialization that could silently expand the reply window.
        local_cursor_before = _current_durable_cursor(store, account_id)
        page = api.thread_messages_page(
            thread_id, limit=HISTORY_CONTEXT_LIMIT
        )
        if not isinstance(page, dict):
            raise WorkerStateError("History page is not an object")
        items = page.get("items")
        coverage = page.get("coverage")
        generation = page.get("sourceGeneration")
        snapshot_cursor = page.get("asOfEventCursor")
        if not isinstance(items, list):
            raise WorkerStateError("History page is missing items")
        if not isinstance(coverage, dict) or coverage.get("partial") is not False:
            raise WorkerStateError("History coverage is partial or unverified")
        if not isinstance(generation, str) or not generation:
            raise WorkerStateError("History sourceGeneration is missing")
        if not valid_cursor(snapshot_cursor):
            raise WorkerStateError("History snapshot cursor is invalid")
        Storage._cursor_number(snapshot_cursor)
        if len(items) > HISTORY_CONTEXT_LIMIT:
            raise WorkerStateError("History page exceeded requested limit")

        message_ids = []
        for item in items:
            if not isinstance(item, dict) or item.get("deleted") is True:
                raise WorkerStateError("History page contains invalid/deleted item")
            message_id = item.get("id")
            item_thread = item.get("threadId")
            if not isinstance(message_id, str) or not message_id:
                raise WorkerStateError("History item is missing message ID")
            if item_thread is not None and item_thread != thread_id:
                raise WorkerStateError("History page contains another thread")
            if message_id in message_ids:
                raise WorkerStateError("History page contains duplicate message ID")
            message_ids.append(message_id)

        # Persist the exact list before making further HTTP calls. Retries
        # must not silently resnapshot a later set of messages.
        state = {
            "state_version": STATE_VERSION,
            "phase": "bootstrapping",
            "account_id": account_id,
            "thread_id": thread_id,
            "snapshot_cursor": snapshot_cursor,
            "activation_cursor": str(max(int(snapshot_cursor), int(local_cursor_before))),
            "history_message_ids": message_ids,
            "source_generation": generation,
            "history_message_count": len(message_ids),
            "initialized_at": datetime.now(timezone.utc).isoformat(),
        }
        _atomic_write_json(state_path, state)
        logger(f"Worker 固定历史快照：消息数={len(message_ids)}；不会生成历史回复")

    if state["phase"] == "bootstrapping":
        max_chars = config.get("processing", {}).get("max_message_chars")
        if type(max_chars) is not int or max_chars < 1:
            raise WorkerStateError("max_message_chars configuration is invalid")

        # Fetch all details before changing any messages. If one fetch fails,
        # history remains only partially materialized on retried attempts;
        # phase stays bootstrapping, never active.
        details = []
        for message_id in state["history_message_ids"]:
            if stop_event is not None and stop_event.is_set():
                raise WorkerStateError("Bootstrap interrupted; state remains resumable")
            detail = api.message_detail(message_id)
            details.append((
                message_id,
                _validate_history_detail(
                    detail, account_id=account_id, thread_id=thread_id,
                    message_id=message_id, max_chars=max_chars,
                ),
            ))

        for message_id, detail in reversed(details):
            if stop_event is not None and stop_event.is_set():
                raise WorkerStateError("Bootstrap interrupted; state remains resumable")
            _persist_history_context_message(
                store,
                account_id=account_id,
                thread_id=thread_id,
                message_id=message_id,
                snapshot_cursor=state["activation_cursor"],
                detail=detail,
            )

        # Linearization/cutover point: everything at/before the latest durable
        # checkpoint is historical, regardless of how long bootstrap took.
        final_durable = _current_durable_cursor(store, account_id)
        activation_cursor = str(max(
            int(state["activation_cursor"]), int(final_durable)
        ))
        active_state = {
            key: value for key, value in state.items()
            if key != "history_message_ids"
        }
        active_state["phase"] = "active"
        active_state["activation_cursor"] = activation_cursor
        active_state["activated_at"] = datetime.now(timezone.utc).isoformat()
        _atomic_write_json(state_path, active_state)
        state = active_state
        logger(
            "Worker 上下文初始化完成；"
            f"历史消息数={state['history_message_count']}，"
            f"激活 Cursor={state['activation_cursor']}；历史回复禁止"
        )

    return state


def _validate_event_detail(
    detail: object, *, account_id: str, thread_id: str, message_id: str,
    max_chars: int,
) -> dict:
    return _validate_history_detail(
        detail,
        account_id=account_id,
        thread_id=thread_id,
        message_id=message_id,
        max_chars=max_chars,
    )


def hydrate_event(
    store: Storage, api: AgentWeChatAPI, config: dict, event,
) -> dict:
    """Fetch and persist one post-activation message event idempotently."""
    wx = config["agent_wechat"]
    account_id = wx["account_id"]
    thread_id = wx["target_group_id"]
    message_id = event["message_id"]
    cursor = event["cursor"]
    if not valid_cursor(cursor) or not isinstance(message_id, str) or not message_id:
        raise ValueError("Invalid event identity")

    detail = _validate_event_detail(
        api.message_detail(message_id),
        account_id=account_id,
        thread_id=thread_id,
        message_id=message_id,
        max_chars=config["processing"]["max_message_chars"],
    )
    message, exceeded = persist_hydrated_message(
        store,
        account=account_id,
        message_id=message_id,
        group=thread_id,
        cursor=cursor,
        direction=detail["direction"],
        body=detail["body"],
        sender_id=detail["sender_id"],
        message_type=detail["message_type"],
        mentions_self=detail["mentions_self"],
        created_at=detail["created_at"],
        max_chars=config["processing"]["max_message_chars"],
    )
    return {
        "message_status": message["status"],
        "body_available": bool(message["body"]),
        "body_exceeded": exceeded,
    }


class AgentWorker:
    """Independent local hydrator and AI processor, with per-item retries."""

    def __init__(
        self, *, config: dict, store: Storage, api: AgentWeChatAPI,
        activation_cursor: str, stop_event=None, logger=print,
        monotonic=time.monotonic, utcnow=lambda: datetime.now(timezone.utc),
    ):
        if not valid_cursor(activation_cursor):
            raise ValueError("Invalid activation cursor")
        if (
            config.get("schema_version") != 2
            or config["app"]["auto_send"] is not False
            or config["sender"]["enabled"] is not False
            or config["runtime"]["global_pause"] is not True
            or config["agent_wechat"]["target_group_id"]
            not in config["safety"]["group_allowlist"]
        ):
            raise WorkerStateError("Worker observe-only guard failed")
        self.config = config
        self.store = store
        self.api = api
        self.activation_cursor = activation_cursor
        self.stop_event = stop_event or threading.Event()
        self.logger = logger
        self.monotonic = monotonic
        self.utcnow = utcnow
        self.hydration_retries = {}
        self.processing_retries = {}
        self.hydration_scan_cursor = activation_cursor
        self.processing_scan_cursor = activation_cursor
        self.model_blocked_until = 0.0
        # Explicit opt-in; existing deployments keep their P6.5A behavior.
        options = config.get("processing", {}).get("conversation_aggregation", {})
        if not isinstance(options, dict):
            raise WorkerStateError("Invalid conversation aggregation config")
        enabled = options.get("enabled", False)
        if type(enabled) is not bool:
            raise WorkerStateError("Invalid conversation aggregation enabled flag")
        quiet = options.get("quiet_seconds", CONVERSATION_DEFAULT_QUIET_SECONDS)
        maximum = options.get("max_wait_seconds", CONVERSATION_DEFAULT_MAX_WAIT_SECONDS)
        if (
            type(quiet) is not int or type(maximum) is not int
            or not 1 <= quiet <= 30 or not quiet <= maximum <= 120
        ):
            raise WorkerStateError("Invalid conversation aggregation timing")
        self.conversation_enabled = enabled
        self.conversation_quiet_seconds = quiet
        self.conversation_max_wait_seconds = maximum

    @staticmethod
    def _next_delay(attempts: int) -> float:
        return min(
            BACKOFF_INITIAL_SECONDS * (2 ** min(max(0, attempts - 1), 16)),
            BACKOFF_MAX_SECONDS,
        )

    def _received_events(self, after_cursor: str):
        wx = self.config["agent_wechat"]
        return self.store.conn.execute(
            "SELECT cursor,message_id FROM events "
            "WHERE account_id=? AND thread_id=? "
            "AND kind='message.committed' AND status='received' "
            "AND cursor!='' AND cursor NOT GLOB '*[^0-9]*' "
            "AND length(cursor)<=19 "
            "AND CAST(cursor AS INTEGER)>? "
            "ORDER BY CAST(cursor AS INTEGER) LIMIT ?",
            (
                wx["account_id"], wx["target_group_id"],
                max(int(self.activation_cursor), int(after_cursor)),
                HYDRATE_BATCH_LIMIT,
            ),
        ).fetchall()

    def hydrate_batch(self) -> int:
        # Rotating keyset scan prevents a poisoned earliest page from starving
        # later events. Failed items remain received and are revisited on wrap.
        events = self._received_events(self.hydration_scan_cursor)
        if not events and self.hydration_scan_cursor != self.activation_cursor:
            self.hydration_scan_cursor = self.activation_cursor
            events = self._received_events(self.hydration_scan_cursor)
        completed = 0
        for event in events:
            if self.stop_event.is_set():
                break
            cursor = event["cursor"]
            self.hydration_scan_cursor = cursor
            retry = self.hydration_retries.get(cursor)
            if retry and self.monotonic() < retry["next_at"]:
                continue
            try:
                result = hydrate_event(self.store, self.api, self.config, event)
            except FatalConsumerError:
                # Protocol/authorization failures fail closed. These should
                # never be translated to an apparently successful message.
                raise
            except Exception as exc:
                self._record_hydration_failure(cursor, exc)
                continue
            self.hydration_retries.pop(cursor, None)
            completed += 1
            self.logger(
                f"Hydrator 完成 Cursor={cursor}，"
                f"状态={result['message_status']}，正文可用={result['body_available']}"
            )
        return completed

    def _record_hydration_failure(self, cursor: str, exc: Exception) -> None:
        previous = self.hydration_retries.get(cursor, {"attempts": 0})
        attempts = previous["attempts"] + 1
        delay = self._next_delay(attempts)
        self.hydration_retries[cursor] = {
            "attempts": attempts,
            "next_at": self.monotonic() + delay,
        }
        self.logger(
            f"Hydrator 暂缓 Cursor={cursor}，"
            f"错误={type(exc).__name__}，{delay:g}s 后重试"
        )

    def _pending_candidates(self, after_cursor: str, limit=PENDING_SCAN_LIMIT):
        wx = self.config["agent_wechat"]
        return self.store.conn.execute(
            "SELECT message_id,source_cursor,created_at FROM messages "
            "WHERE account_id=? AND thread_id=? "
            "AND direction='incoming' AND status='pending' "
            "AND source_cursor IS NOT NULL AND source_cursor!='' "
            "AND source_cursor NOT GLOB '*[^0-9]*' "
            "AND length(source_cursor)<=19 "
            "AND body IS NOT NULL AND TRIM(body)!='' "
            "AND CAST(source_cursor AS INTEGER)>? "
            "ORDER BY CAST(source_cursor AS INTEGER),rowid LIMIT ?",
            (
                wx["account_id"], wx["target_group_id"],
                max(int(self.activation_cursor), int(after_cursor)),
                limit,
            ),
        ).fetchall()

    def _select_due_pending(self):
        candidates = self._pending_candidates(self.processing_scan_cursor)
        if not candidates and self.processing_scan_cursor != self.activation_cursor:
            self.processing_scan_cursor = self.activation_cursor
            candidates = self._pending_candidates(self.processing_scan_cursor)
        for row in candidates:
            self.processing_scan_cursor = row["source_cursor"]
            retry = self.processing_retries.get(row["message_id"])
            if not retry or self.monotonic() >= retry["next_at"]:
                return row
        return None

    def expire_stale_batch(self) -> int:
        """Boundedly retire expired incoming messages without calling an LLM.

        Each message uses Storage's tested decision/message transaction; a
        crash may leave a partial batch, but already-committed items are
        immutable and no message can be retired without a rejected decision.
        The activation boundary and group/account scope apply to every row.
        """
        rows = self._pending_candidates(
            self.activation_cursor, limit=EXPIRED_BATCH_LIMIT
        )
        expired = 0
        account = self.config["agent_wechat"]["account_id"]
        for row in rows:
            if self.stop_event.is_set():
                break
            # Only well-formed, definitely expired timestamps qualify here.
            # Missing, invalid, or future timestamps remain under the existing
            # fail-closed single-message path in process_pending_once().
            if self._staleness_reason(row["created_at"]) != "stale_message_not_replied":
                continue
            # A separately created decision may exist while the message is
            # still pending. Leave it to process_pending_once()'s idempotent
            # reconciliation rather than treating it as a batch failure.
            already_decided = self.store.conn.execute(
                "SELECT 1 FROM decisions "
                "WHERE account_id=? AND source_message_id=?",
                (account, row["message_id"]),
            ).fetchone()
            if already_decided:
                continue
            self.store.decide_and_process(
                account, row["message_id"],
                action="skip", reason="stale_message_not_replied",
                model=None, reply=None, decision_status="rejected",
            )
            self.processing_retries.pop(row["message_id"], None)
            expired += 1
        if expired:
            self.logger(
                f"Processor 批量过期：count={expired}；"
                "仅写入 skip/rejected，未调用模型、未写入 Outbox"
            )
        return expired

    def _conversation_plan(self):
        """Select the newest bounded conversation and retire its backlog.

        The latest message is the only model anchor. Newer turns win over
        older still-fresh pending messages; the latter remain auditable as
        superseded, not falsely labelled as included in model context.
        """
        wx = self.config["agent_wechat"]
        rows = self.store.conn.execute(
            "SELECT m.message_id,m.source_cursor,m.created_at FROM messages m "
            "WHERE m.account_id=? AND m.thread_id=? "
            "AND m.direction='incoming' AND m.status='pending' "
            "AND m.source_cursor IS NOT NULL AND m.source_cursor!='' "
            "AND m.source_cursor NOT GLOB '*[^0-9]*' "
            "AND length(m.source_cursor)<=19 "
            "AND CAST(m.source_cursor AS INTEGER)>? "
            "AND m.body IS NOT NULL AND TRIM(m.body)!='' "
            "AND NOT EXISTS (SELECT 1 FROM decisions d "
            "  WHERE d.account_id=m.account_id "
            "  AND d.source_message_id=m.message_id) "
            "ORDER BY CAST(m.source_cursor AS INTEGER) DESC,m.rowid DESC LIMIT ?",
            (wx["account_id"], wx["target_group_id"],
             int(self.activation_cursor), CONVERSATION_BATCH_LIMIT),
        ).fetchall()
        if not rows:
            return None
        now = self.utcnow()
        eligible = []
        for row in rows:
            if self._staleness_reason(row["created_at"]) is not None:
                continue
            moment = datetime.fromisoformat(
                row["created_at"].replace("Z", "+00:00")
            ).astimezone(timezone.utc)
            eligible.append((row, moment))
        if not eligible:
            return None

        anchor, anchor_time = eligible[0]
        # A model transport/parser failure must not be retried immediately
        # via a different (older) due pending message's scan position.
        retry = self.processing_retries.get(anchor["message_id"])
        if retry and self.monotonic() < retry["next_at"]:
            return {"status": "empty_or_backoff"}
        # First pending in this sampled conversation bounds waiting, even
        # if the group never goes silent during a fast-moving discussion.
        oldest_time = min(moment for _, moment in eligible)
        if (
            (now - anchor_time).total_seconds() < self.conversation_quiet_seconds
            and (now - oldest_time).total_seconds() < self.conversation_max_wait_seconds
        ):
            return {"status": "waiting_for_conversation"}

        context_window_start = anchor_time - timedelta(
            seconds=self.conversation_max_wait_seconds
        )
        covered = []
        for row, moment in eligible[1:]:
            reason = (
                "conversation_window_coalesced"
                if moment >= context_window_start
                else "superseded_by_newer_context"
            )
            covered.append((row["message_id"], reason))
        return {
            "status": "ready", "anchor_id": anchor["message_id"],
            "coalesced": covered,
        }

    def retire_superseded_batch(self) -> int:
        """Boundedly retire older pending turns after a newer model decision.

        Also handles >200-message bursts: after the newest bounded batch
        commits, its unselected predecessors must not trigger late AI replies.
        """
        if not self.conversation_enabled:
            return 0
        wx = self.config["agent_wechat"]
        account, group = wx["account_id"], wx["target_group_id"]
        cursor_row = self.store.conn.execute(
            "SELECT MAX(CAST(m.source_cursor AS INTEGER)) FROM decisions d "
            "JOIN messages m ON d.account_id=m.account_id "
            "AND d.source_message_id=m.message_id "
            "WHERE m.account_id=? AND m.thread_id=? "
            "AND m.direction='incoming' AND m.status='processed' "
            "AND m.source_cursor IS NOT NULL "
            "AND m.source_cursor NOT GLOB '*[^0-9]*' "
            "AND length(m.source_cursor)<=19 "
            "AND CAST(m.source_cursor AS INTEGER)>? "
            "AND d.model IS NOT NULL",
            (account, group, int(self.activation_cursor)),
        ).fetchone()
        if cursor_row is None or cursor_row[0] is None:
            return 0
        last_anchor = int(cursor_row[0])
        rows = self.store.conn.execute(
            "SELECT m.message_id,m.created_at FROM messages m "
            "WHERE m.account_id=? AND m.thread_id=? "
            "AND m.direction='incoming' AND m.status='pending' "
            "AND m.source_cursor IS NOT NULL "
            "AND m.source_cursor NOT GLOB '*[^0-9]*' "
            "AND length(m.source_cursor)<=19 "
            "AND CAST(m.source_cursor AS INTEGER)>? "
            "AND CAST(m.source_cursor AS INTEGER)<? "
            "AND m.body IS NOT NULL AND TRIM(m.body)!='' "
            "AND NOT EXISTS (SELECT 1 FROM decisions d "
            "WHERE d.account_id=m.account_id "
            "AND d.source_message_id=m.message_id) "
            "ORDER BY CAST(m.source_cursor AS INTEGER) LIMIT ?",
            (account, group, int(self.activation_cursor),
             last_anchor, EXPIRED_BATCH_LIMIT),
        ).fetchall()
        retired = 0
        for row in rows:
            if self.stop_event.is_set():
                break
            # Malformed or already expired messages retain their distinct
            # failure reason in the existing single-message/expiry path.
            if self._staleness_reason(row["created_at"]) is not None:
                continue
            self.store.decide_and_process(
                account, row["message_id"], action="skip",
                reason="superseded_by_newer_context", model=None,
                reply=None, decision_status="rejected",
            )
            self.processing_retries.pop(row["message_id"], None)
            retired += 1
        if retired:
            self.logger(
                f"Processor 旧轮次覆盖：count={retired}；"
                "不调用模型、不写入 Outbox"
            )
        return retired

    def _staleness_reason(self, created_at: str | None) -> str | None:
        if not isinstance(created_at, str) or not created_at:
            return "missing_message_timestamp"
        try:
            moment = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        except ValueError:
            return "invalid_message_timestamp"
        if moment.tzinfo is None:
            return "missing_timestamp_timezone"
        age = (self.utcnow() - moment.astimezone(timezone.utc)).total_seconds()
        if age > MAX_REPLY_AGE_SECONDS:
            return "stale_message_not_replied"
        if age < -120:
            return "future_message_timestamp"
        return None

    def process_pending_once(self) -> dict:
        # Provider has supplied a durable local throttle/budget wait. Continue
        # hydration and expiry, but don't cycle through other billable messages.
        if self.monotonic() < self.model_blocked_until:
            return {"status": "model_throttled"}
        row = self._select_due_pending()
        if row is None:
            return {"status": "empty_or_backoff"}
        message_id = row["message_id"]
        wx = self.config["agent_wechat"]
        try:
            # Recover an already-persisted decision before applying expiry.
            # This covers interrupted/manual workflows without duplicating a
            # decision or ever invoking the model for an existing decision.
            existing = self.store.conn.execute(
                "SELECT 1 FROM decisions "
                "WHERE account_id=? AND source_message_id=?",
                (wx["account_id"], message_id),
            ).fetchone()
            if existing:
                result = process_one(
                    self.store, self.config,
                    activation_cursor=self.activation_cursor,
                    message_id=message_id,
                )
            else:
                reason = self._staleness_reason(row["created_at"])
                if reason is not None:
                    # Expired/malformed messages never invoke the model.
                    self.store.decide_and_process(
                        wx["account_id"], message_id,
                        action="skip", reason=reason,
                        model=None, reply=None, decision_status="rejected",
                    )
                    result = {"status": "saved", "action": "skip", "reason": reason}
                else:
                    if self.conversation_enabled:
                        plan = self._conversation_plan()
                        if plan is None:
                            return {"status": "empty_or_backoff"}
                        if plan["status"] != "ready":
                            return {"status": plan["status"]}
                        message_id = plan["anchor_id"]
                        result = process_one(
                            self.store, self.config,
                            activation_cursor=self.activation_cursor,
                            message_id=message_id,
                            coalesced=plan["coalesced"],
                        )
                    else:
                        result = process_one(
                            self.store, self.config,
                            activation_cursor=self.activation_cursor,
                            message_id=message_id,
                        )
        except sqlite3.Error:
            raise
        except Exception as exc:
            self._record_processing_failure(message_id, exc)
            return {"status": "error_deferred"}

        if result["status"] == "deferred":
            code = result.get("error_code", "model_deferred")
            if code in ("model_rate_limited", "model_daily_cap"):
                wait = result.get("retry_after_seconds")
                if type(wait) not in (int, float) or not 1 <= wait <= 86401:
                    # Invalid adapter metadata must not defeat backoff.
                    wait = BACKOFF_MAX_SECONDS
                self.model_blocked_until = self.monotonic() + wait
                self.processing_retries.pop(message_id, None)
                self.logger(
                    f"Processor 模型调度暂停：原因={code}；"
                    f"{wait:.1f}s 后恢复；消息接收与过期清理继续"
                )
            else:
                self._record_processing_failure(message_id, RuntimeError(code))
        else:
            self.processing_retries.pop(message_id, None)
            if result["status"] == "saved":
                count = result.get("decision_count", 1)
                self.logger(
                    f"Processor 保存决策：action={result['action']}；"
                    f"决策消息数={count}；"
                    "未写入 Outbox，未发送微信消息"
                )
        return result

    def _record_processing_failure(self, message_id: str, exc: Exception) -> None:
        previous = self.processing_retries.get(message_id, {"attempts": 0})
        attempts = previous["attempts"] + 1
        delay = self._next_delay(attempts)
        self.processing_retries[message_id] = {
            "attempts": attempts,
            "next_at": self.monotonic() + delay,
        }
        self.logger(
            f"Processor 暂缓决策，错误={type(exc).__name__}；"
            f"{delay:g}s 后重试；其他消息可继续处理"
        )

    def run_once(self) -> dict:
        hydrated = self.hydrate_batch()
        expired = self.expire_stale_batch()
        superseded = self.retire_superseded_batch()
        # Keep one processor slot per loop for fresh messages. The expired
        # cleanup is bounded and cannot monopolize an entire Worker run.
        processed = self.process_pending_once()
        return {
            "hydrated": hydrated,
            "expired": expired,
            "superseded": superseded,
            "processor": processed["status"],
        }

    def run_forever(self) -> None:
        self.logger(
            "Worker 启动；自动发送关闭；历史消息隔离；"
            f"对话聚合={'启用' if self.conversation_enabled else '关闭'}"
        )
        while not self.stop_event.is_set():
            try:
                result = self.run_once()
            except (sqlite3.OperationalError, RetryableConsumerError) as exc:
                detail = str(exc).lower()
                if isinstance(exc, sqlite3.OperationalError) and not (
                    "locked" in detail or "busy" in detail
                ):
                    raise
                self.logger(f"Worker 暂时不可用：{type(exc).__name__}")
                self.stop_event.wait(POLL_INTERVAL_SECONDS)
                continue
            # Always yield; prevents CPU spin on deferred processing and hot
            # queues while allowing another systemd process to make progress.
            idle = (
                result["hydrated"] == 0
                and result["expired"] == 0
                and result["superseded"] == 0
                and result["processor"] != "saved"
            )
            self.stop_event.wait(POLL_INTERVAL_SECONDS if idle else 0.2)
        self.logger("Worker 已优雅退出")


@contextmanager
def worker_single_instance_lock(lock_path: Path):
    lock_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "a+") as lock_file:
            try:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise WorkerStateError("另一个 Worker 已在运行") from exc
            yield
    except BaseException:
        try:
            os.close(fd)
        except OSError:
            pass
        raise


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Observe-only WeChat hydration and AI decision worker"
    )
    parser.add_argument(
        "--config", type=Path,
        default=Path.home() / "apps/wechat-ai-bot/config.json",
    )
    parser.add_argument(
        "--bootstrap-only", action="store_true",
        help="Seed the last 20 official history messages as ignored context, then exit",
    )
    parser.add_argument(
        "--run", action="store_true", help="Run only after an explicit bootstrap"
    )
    args = parser.parse_args(argv)
    if args.run == args.bootstrap_only:
        parser.error("Exactly one of --bootstrap-only or --run is required")

    try:
        config, database_path, token = _observer_inputs(args.config)
    except FatalConsumerError as exc:
        print(f"FATAL: {exc}", file=sys.stderr, flush=True)
        return 2

    wx = config["agent_wechat"]
    api = AgentWeChatAPI(
        base_url=wx["base_url"],
        account_id=wx["account_id"],
        consumer_name=wx["consumer_name"],
        token=token,
    )

    state_directory = observer_state_directory(config, database_path)
    state_path = state_directory / "worker-state.json"
    lock_path = state_directory / "worker.lock"
    stop_event = threading.Event()
    previous_handlers = {}

    def request_stop(signum, _frame):
        stop_event.set()

    for signum in (signal.SIGTERM, signal.SIGINT):
        previous_handlers[signum] = signal.signal(signum, request_stop)

    try:
        with worker_single_instance_lock(lock_path):
            store = Storage(database_path, consumer_name=wx["consumer_name"])
            try:
                if args.bootstrap_only:
                    state = ensure_activation_state(
                        store, api, config, state_path, stop_event=stop_event
                    )
                    print("===== Worker 历史上下文初始化 =====")
                    print("阶段:", state["phase"])
                    print("激活 Cursor:", state["activation_cursor"])
                    print("历史上下文消息数:", state["history_message_count"])
                    print("历史消息参与 AI 决策: 否")
                    print("微信发送: 禁止")
                    return 0

                state = _read_state(
                    state_path, wx["account_id"], wx["target_group_id"]
                )
                if state is None or state["phase"] != "active":
                    raise WorkerStateError(
                        "Worker 未显式完成初始化；请先执行 --bootstrap-only"
                    )
                worker = AgentWorker(
                    config=config,
                    store=store,
                    api=api,
                    activation_cursor=state["activation_cursor"],
                    stop_event=stop_event,
                )
                worker.run_forever()
            finally:
                store.close()
    except (FatalConsumerError, WorkerStateError) as exc:
        print(f"FATAL: {exc}", file=sys.stderr, flush=True)
        return 2
    except Exception as exc:
        print(
            f"FATAL: {type(exc).__name__}: {exc}",
            file=sys.stderr,
            flush=True,
        )
        return 1
    finally:
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
