"""Long-running, durable Agent WeChat SSE consumer.

Responsibilities are intentionally limited to:
- maintaining one SSE subscription for one configured account;
- persisting event metadata and the durable cursor before remote ACK;
- reconnecting without resetting the cursor;
- recording only allowlisted target-group message identifiers.

This module never hydrates message bodies, calls an AI model, or sends messages.
"""

from __future__ import annotations

import argparse
import fcntl
import http.client
import json
import os
import signal
import socket
import sqlite3
import stat
import sys
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlsplit
from urllib.request import Request, urlopen

from wechat_ai_bot.storage import Storage


EXPECTED_AGENT_WECHAT_BASE_URL = "http://127.0.0.1:6174/v1"
DEFAULT_CONNECT_TIMEOUT_SECONDS = 15
DEFAULT_STREAM_TIMEOUT_SECONDS = 35
MAX_SSE_FRAME_BYTES = 1024 * 1024
BACKOFF_INITIAL_SECONDS = 1.0
BACKOFF_MAX_SECONDS = 60.0
STABLE_CONNECTION_SECONDS = 30.0
HISTORY_PAGE_SIZE = 50
MAX_RECOVERY_MESSAGES = 10_000
MAX_RECOVERY_PAGES = 250
MAX_RECOVERY_SNAPSHOT_RETRIES = 3


class ConsumerError(RuntimeError):
    """Base error for consumer failures."""


class FatalConsumerError(ConsumerError):
    """Configuration, authentication, or protocol error; do not retry."""


class RetryableConsumerError(ConsumerError):
    """Temporary transport or database contention error; reconnect safely."""


class CursorExpiredError(FatalConsumerError):
    """The daemon no longer retains the requested SSE cursor."""


class CursorRecoveryError(FatalConsumerError):
    """History could not prove complete coverage of an expired cursor gap."""


@dataclass(frozen=True)
class SSEFrame:
    data: str
    event: str | None = None
    event_id: str | None = None


def valid_cursor(value: object) -> bool:
    """Return whether value is a valid SQLite-safe Agent WeChat cursor."""
    try:
        Storage._cursor_number(value)
    except (TypeError, ValueError):
        return False
    return True


def iter_sse_frames(
    lines: Iterable[bytes | str],
    *,
    max_frame_bytes: int = MAX_SSE_FRAME_BYTES,
    stop_event: threading.Event | None = None,
) -> Iterator[SSEFrame]:
    """Parse SSE lines, dispatching only frames terminated by a blank line.

    Comments/heartbeats are ignored. Multiple data fields are joined with a
    newline as required by SSE. An incomplete final frame is deliberately
    discarded at EOF and will be replayed because it was never ACKed.
    """
    data_lines: list[str] = []
    event_name: str | None = None
    event_id: str | None = None
    frame_bytes = 0

    for raw in lines:
        # Check after every received line, including heartbeat comments. This
        # lets SIGTERM stop a quiet-but-heartbeating SSE stream promptly. Any
        # unfinished frame is discarded and replayed after the durable cursor.
        if stop_event is not None and stop_event.is_set():
            return

        if isinstance(raw, bytes):
            try:
                line = raw.decode("utf-8").rstrip("\r\n")
            except UnicodeDecodeError as exc:
                raise FatalConsumerError("SSE stream is not valid UTF-8") from exc
        elif isinstance(raw, str):
            line = raw.rstrip("\r\n")
        else:
            raise FatalConsumerError("SSE line has an unsupported type")

        if line == "":
            if data_lines:
                yield SSEFrame(
                    data="\n".join(data_lines),
                    event=event_name,
                    event_id=event_id,
                )
            data_lines = []
            event_name = None
            event_id = None
            frame_bytes = 0
            continue

        if line.startswith(":"):
            continue

        field, separator, value = line.partition(":")
        if separator and value.startswith(" "):
            value = value[1:]

        if field == "data":
            data_lines.append(value)
            frame_bytes += len(value.encode("utf-8"))
            if frame_bytes > max_frame_bytes:
                raise FatalConsumerError("SSE frame exceeds size limit")
        elif field == "event":
            event_name = value
        elif field == "id":
            if "\x00" not in value:
                event_id = value
        # Unknown fields and retry: are ignored.


class AgentWeChatAPI:
    """Small HTTP client for the already-configured local Agent WeChat API."""

    def __init__(
        self,
        *,
        base_url: str,
        account_id: str,
        consumer_name: str,
        token: str,
        opener=urlopen,
        connect_timeout: float = DEFAULT_CONNECT_TIMEOUT_SECONDS,
        stream_timeout: float = DEFAULT_STREAM_TIMEOUT_SECONDS,
    ):
        if base_url.rstrip("/") != EXPECTED_AGENT_WECHAT_BASE_URL:
            raise FatalConsumerError("拒绝非本机 Agent WeChat 地址")
        if not all(
            isinstance(value, str) and value
            for value in (account_id, consumer_name, token)
        ):
            raise FatalConsumerError("Agent WeChat API 配置不完整")

        self.base_url = base_url.rstrip("/")
        self.account_id = account_id
        self.consumer_name = consumer_name
        self.token = token
        self.opener = opener
        self.connect_timeout = connect_timeout
        self.stream_timeout = stream_timeout
        self.account_endpoint = (
            f"{self.base_url}/accounts/{quote(account_id, safe='')}"
        )

    def _headers(self, accept: str) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token}",
            "Accept": accept,
        }

    @staticmethod
    def _classify_http_error(exc: HTTPError) -> None:
        code = exc.code
        try:
            detail = exc.read(1024).decode("utf-8", errors="replace")
        except Exception:
            detail = ""
        finally:
            exc.close()

        message = f"Agent WeChat HTTP {code}"
        if detail:
            message += f": {detail[:300]}"

        if code == 410:
            try:
                payload = json.loads(detail) if detail else {}
                error = payload.get("error", {}) if isinstance(payload, dict) else {}
                error_code = error.get("code") if isinstance(error, dict) else None
            except json.JSONDecodeError:
                error_code = None
            if error_code == "event_cursor_expired":
                raise CursorExpiredError(message) from exc
        if code in (408, 425, 429) or 500 <= code <= 599:
            raise RetryableConsumerError(message) from exc
        raise FatalConsumerError(message) from exc

    def _open(self, request: Request, timeout: float):
        try:
            return self.opener(request, timeout=timeout)
        except HTTPError as exc:
            self._classify_http_error(exc)
        except (URLError, TimeoutError, socket.timeout, ConnectionError, OSError,
                http.client.HTTPException) as exc:
            raise RetryableConsumerError(
                f"Agent WeChat transport error: {type(exc).__name__}"
            ) from exc

    @staticmethod
    def _assert_local_response(response) -> None:
        final_url = response.geturl()
        parsed = urlsplit(final_url)
        if (
            parsed.scheme != "http"
            or parsed.hostname != "127.0.0.1"
            or parsed.port != 6174
            or parsed.username is not None
            or parsed.password is not None
        ):
            response.close()
            raise FatalConsumerError(
                "Agent WeChat redirected outside the trusted local origin"
            )

    def request_json(self, url: str, payload: dict | None = None) -> dict:
        headers = self._headers("application/json")
        data = None
        method = "GET"
        if payload is not None:
            data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json"
            method = "POST"

        request = Request(url, headers=headers, data=data, method=method)
        with self._open(request, self.connect_timeout) as response:
            self._assert_local_response(response)
            try:
                raw = response.read(1_048_577)
                if len(raw) > 1_048_576:
                    raise FatalConsumerError(
                        "Agent WeChat JSON response exceeds size limit"
                    )
                value = json.loads(raw)
            except FatalConsumerError:
                raise
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                raise FatalConsumerError(
                    "Agent WeChat returned invalid JSON"
                ) from exc
            except (OSError, TimeoutError, socket.timeout,
                    http.client.HTTPException) as exc:
                raise RetryableConsumerError(
                    f"Agent WeChat response read failed: {type(exc).__name__}"
                ) from exc

        if not isinstance(value, dict):
            raise FatalConsumerError("Agent WeChat JSON response is not an object")
        return value

    def newest_cursor(self) -> str:
        page = self.request_json(
            self.account_endpoint + "/events?newest=true&limit=1"
        )
        cursor = page.get("nextCursor")
        if not valid_cursor(cursor):
            raise FatalConsumerError("初始 Cursor 无效")
        return cursor

    def thread_messages_page(
        self, thread_id: str, *, cursor: str | None = None,
        limit: int = HISTORY_PAGE_SIZE
    ) -> dict:
        if not isinstance(thread_id, str) or not thread_id:
            raise FatalConsumerError("历史恢复群 ID 无效")
        if type(limit) is not int or not 1 <= limit <= 200:
            raise FatalConsumerError("历史恢复分页大小无效")
        params = {"limit": str(limit)}
        if cursor is not None:
            if not isinstance(cursor, str) or not cursor:
                raise FatalConsumerError("历史分页 Cursor 无效")
            params["cursor"] = cursor
        url = (
            self.account_endpoint
            + "/threads/"
            + quote(thread_id, safe="")
            + "/messages?"
            + urlencode(params)
        )
        return self.request_json(url)

    def message_detail(self, message_id: str) -> dict:
        if not isinstance(message_id, str) or not message_id:
            raise FatalConsumerError("历史恢复消息 ID 无效")
        return self.request_json(
            self.account_endpoint + "/messages/" + quote(message_id, safe="")
        )

    def acknowledge(self, cursor: str) -> None:
        result = self.request_json(
            self.account_endpoint
            + "/consumers/"
            + quote(self.consumer_name, safe="")
            + "/ack",
            {"cursor": cursor},
        )
        if result.get("cursor") != cursor:
            raise FatalConsumerError("远程 ACK Cursor 不匹配")

    def open_event_stream(self, cursor: str):
        request = Request(
            self.account_endpoint + "/events/stream?after=" + quote(cursor, safe=""),
            headers=self._headers("text/event-stream"),
        )
        response = self._open(request, self.stream_timeout)
        self._assert_local_response(response)
        content_type = response.headers.get("Content-Type", "")
        if not content_type.lower().startswith("text/event-stream"):
            response.close()
            raise FatalConsumerError("服务器未返回 SSE 数据流")
        return response


class EventConsumer:
    """Durable, reconnecting SSE consumer. No business/AI/sending work."""

    def __init__(
        self,
        *,
        config: dict,
        store: Storage,
        api: AgentWeChatAPI,
        stop_event: threading.Event | None = None,
        logger=print,
        monotonic=time.monotonic,
        backoff_initial: float = BACKOFF_INITIAL_SECONDS,
        backoff_max: float = BACKOFF_MAX_SECONDS,
        stable_connection_seconds: float = STABLE_CONNECTION_SECONDS,
    ):
        self.config = config
        self.store = store
        self.api = api
        self.stop_event = stop_event or threading.Event()
        self.logger = logger
        self.monotonic = monotonic
        self.backoff_initial = backoff_initial
        self.backoff_max = backoff_max
        self.stable_connection_seconds = stable_connection_seconds

        wx = config["agent_wechat"]
        self.account_id = wx["account_id"]
        self.group_id = wx["target_group_id"]
        self.consumer_name = wx["consumer_name"]

    def prepare_cursor(self) -> str:
        """Load the durable cursor or initialize it once; compensate ACK gaps."""
        state = self.store.checkpoint(self.account_id)

        if state is None:
            cursor = self.api.newest_cursor()
            with self.store.transaction():
                # The process lock prevents a second local initializer. Keep
                # this defensive check in case another writer initialized it.
                state = self.store.checkpoint(self.account_id)
                if state is None:
                    self.store.conn.execute(
                        "INSERT INTO consumer_state "
                        "(consumer_name,account_id,last_durable_cursor) "
                        "VALUES(?,?,?)",
                        (self.consumer_name, self.account_id, cursor),
                    )
                else:
                    cursor = state["last_durable_cursor"]
            self.logger("首次订阅：从最新事件边界开始")
            return cursor

        cursor = state["last_durable_cursor"]
        if not valid_cursor(cursor):
            raise FatalConsumerError("本地 Cursor 无效，禁止自动重置")

        acked = state["last_acked_cursor"]
        if acked is not None:
            if not valid_cursor(acked):
                raise FatalConsumerError("本地 ACK Cursor 无效")
            if int(acked) > int(cursor):
                raise FatalConsumerError("ACK Cursor 超过 durable Cursor")

        if acked != cursor:
            self.api.acknowledge(cursor)
            self.store.mark_acked(self.account_id, cursor)
            self.logger("已补偿上次中断的远程 ACK")

        return cursor

    def _last_target_message_anchor(self, durable_cursor: str) -> str:
        row = self.store.conn.execute(
            "SELECT message_id FROM events "
            "WHERE account_id=? AND thread_id=? "
            "AND kind='message.committed' AND message_id IS NOT NULL "
            "AND CAST(cursor AS INTEGER)<=? "
            "ORDER BY CAST(cursor AS INTEGER) DESC LIMIT 1",
            (self.account_id, self.group_id, int(durable_cursor)),
        ).fetchone()
        if row is None or not isinstance(row["message_id"], str) or not row["message_id"]:
            raise CursorRecoveryError(
                "本地没有可验证的目标群历史锚点；拒绝自动跳过过期事件"
            )
        return row["message_id"]

    def _scan_history_to_anchor(
        self, anchor_message_id: str
    ) -> tuple[list[str], str]:
        """Read the target thread newest-first until the last durable anchor.

        The API page's asOfEventCursor is the exact event boundary associated
        with the first history snapshot. Pagination must keep that boundary,
        and coverage must explicitly be complete. sourceGeneration may change
        while the opaque page cursor preserves the original IDs and order.
        """
        page_cursor = None
        seen_page_cursors: set[str] = set()
        seen_message_ids: set[str] = set()
        missing_newest_first: list[str] = []
        snapshot_cursor = None

        for _page_number in range(MAX_RECOVERY_PAGES):
            page = self.api.thread_messages_page(
                self.group_id,
                cursor=page_cursor,
                limit=HISTORY_PAGE_SIZE,
            )
            if not isinstance(page, dict):
                raise CursorRecoveryError("群历史分页响应不是 JSON 对象")

            items = page.get("items")
            coverage = page.get("coverage")
            generation = page.get("sourceGeneration")
            as_of = page.get("asOfEventCursor")
            if not isinstance(items, list):
                raise CursorRecoveryError("群历史分页缺少 items 数组")
            if not isinstance(coverage, dict) or coverage.get("partial") is not False:
                raise CursorRecoveryError(
                    "群历史数据覆盖不完整或无法确认；拒绝推进 Cursor"
                )
            if not isinstance(generation, str) or not generation:
                raise CursorRecoveryError("群历史缺少 sourceGeneration")
            if not valid_cursor(as_of):
                raise CursorRecoveryError("群历史缺少有效 asOfEventCursor")

            if snapshot_cursor is None:
                snapshot_cursor = as_of
            elif as_of != snapshot_cursor:
                # asOfEventCursor is fixed when the first page is read. The
                # sourceGeneration may legitimately change because page fields
                # reflect current saved data while the cursor preserves IDs/order.
                raise CursorRecoveryError("群历史分页快照发生变化")

            for item in items:
                if not isinstance(item, dict) or item.get("deleted") is True:
                    raise CursorRecoveryError("群历史包含无法识别的消息项")
                message_id = item.get("id")
                if not isinstance(message_id, str) or not message_id:
                    raise CursorRecoveryError("群历史消息缺少 ID")
                item_thread = item.get("threadId")
                if item_thread is not None and item_thread != self.group_id:
                    raise CursorRecoveryError("群历史返回了其他会话的消息")
                if message_id in seen_message_ids:
                    raise CursorRecoveryError("群历史分页出现重复消息 ID")
                seen_message_ids.add(message_id)

                if message_id == anchor_message_id:
                    return missing_newest_first, snapshot_cursor

                missing_newest_first.append(message_id)
                if len(missing_newest_first) > MAX_RECOVERY_MESSAGES:
                    raise CursorRecoveryError(
                        "过期期间目标群消息超过恢复上限；拒绝不完整恢复"
                    )

            next_cursor = page.get("nextCursor")
            if next_cursor is None:
                break
            if not isinstance(next_cursor, str) or not next_cursor:
                raise CursorRecoveryError("群历史 nextCursor 格式异常")
            if next_cursor in seen_page_cursors:
                raise CursorRecoveryError("群历史分页 Cursor 循环")
            seen_page_cursors.add(next_cursor)
            page_cursor = next_cursor

        raise CursorRecoveryError(
            "在群历史中找不到最后一个已持久化消息锚点；拒绝跳过消息"
        )

    def _persist_recovered_message(self, message_id: str) -> None:
        msg = self.api.message_detail(message_id)
        if not isinstance(msg, dict) or (
            msg.get("id") != message_id
            or msg.get("accountId") != self.account_id
            or msg.get("threadId") != self.group_id
        ):
            raise CursorRecoveryError("历史消息详情身份不匹配")

        direction = msg.get("direction")
        body = msg.get("body")
        message_type = msg.get("type")
        sender_id = msg.get("senderId")
        created_at = msg.get("createdAt")
        mentions = msg.get("mentions")
        mentions_self = None

        if direction not in ("incoming", "outgoing", None):
            raise CursorRecoveryError("历史消息方向格式异常")
        if body is not None and not isinstance(body, str):
            raise CursorRecoveryError("历史消息正文格式异常")
        if message_type is not None and type(message_type) is not int:
            raise CursorRecoveryError("历史消息类型格式异常")
        if sender_id is not None and not isinstance(sender_id, str):
            raise CursorRecoveryError("历史消息发送者格式异常")
        if created_at is not None and not isinstance(created_at, str):
            raise CursorRecoveryError("历史消息时间格式异常")

        if isinstance(mentions, dict) and mentions.get("state") == "known":
            mentions_self = mentions.get("self")
            if mentions_self is not None and type(mentions_self) is not bool:
                raise CursorRecoveryError("历史消息 @ 元数据格式异常")

        max_chars = self.config.get("processing", {}).get("max_message_chars")
        if type(max_chars) is not int or max_chars < 1:
            raise FatalConsumerError("max_message_chars 配置异常")
        if body is not None and (
            not body.strip() or len(body) > max_chars
        ):
            body = None

        # No original per-message event cursor exists after the event log has
        # expired. source_cursor is assigned atomically to the history snapshot
        # boundary after every recovered detail has been persisted.
        try:
            self.store.persist_message(
                self.account_id,
                message_id,
                self.group_id,
                direction=direction,
                body=body,
                sender_id=sender_id,
                message_type=message_type,
                mentions_self=mentions_self,
                created_at=created_at,
                source_cursor=None,
            )
        except ValueError as exc:
            raise CursorRecoveryError(
                f"历史消息与本地记录冲突：{message_id}"
            ) from exc
        except sqlite3.OperationalError as exc:
            detail = str(exc).lower()
            if "locked" in detail or "busy" in detail:
                raise RetryableConsumerError(
                    "SQLite 暂时繁忙，历史消息补偿稍后重试"
                ) from exc
            raise CursorRecoveryError(
                "SQLite 无法持久化历史补偿消息"
            ) from exc

    def recover_expired_cursor(self) -> str:
        """Reconcile target-group history, then advance to its snapshot cursor."""
        state = self.store.checkpoint(self.account_id)
        if state is None:
            raise CursorRecoveryError("Cursor 已过期，但本地没有 checkpoint")
        durable = state["last_durable_cursor"]
        if not valid_cursor(durable):
            raise CursorRecoveryError("本地 durable Cursor 无效")

        anchor = self._last_target_message_anchor(durable)
        last_snapshot_error = None
        for _attempt in range(MAX_RECOVERY_SNAPSHOT_RETRIES):
            try:
                missing_ids, snapshot_cursor = self._scan_history_to_anchor(anchor)
                break
            except CursorRecoveryError as exc:
                if "分页快照发生变化" not in str(exc):
                    raise
                last_snapshot_error = exc
        else:
            raise CursorRecoveryError(
                "群消息持续变化，连续多次无法取得一致历史快照"
            ) from last_snapshot_error

        if int(snapshot_cursor) <= int(durable):
            raise CursorRecoveryError(
                "历史快照边界没有超过本地 Cursor；拒绝推进"
            )

        # The history endpoint returns newest-first. Insert oldest-first so
        # rowid ordering preserves chronology for messages sharing the snapshot
        # boundary cursor in process_pending.context_until().
        for message_id in reversed(missing_ids):
            self._persist_recovered_message(message_id)

        try:
            updated = self.store.persist_recovery_checkpoint(
                self.account_id,
                snapshot_cursor,
                self.group_id,
                list(reversed(missing_ids)),
            )
        except sqlite3.OperationalError as exc:
            detail = str(exc).lower()
            if "locked" in detail or "busy" in detail:
                raise RetryableConsumerError(
                    "SQLite 暂时繁忙，恢复 checkpoint 稍后重试"
                ) from exc
            raise CursorRecoveryError(
                "SQLite 无法提交恢复 checkpoint"
            ) from exc
        except (ValueError, RuntimeError) as exc:
            raise CursorRecoveryError(
                f"恢复 checkpoint 校验失败：{exc}"
            ) from exc

        # Critical order: history + checkpoint are durable before remote ACK.
        # If ACK fails, prepare_cursor() will retry it on the next start.
        self.api.acknowledge(snapshot_cursor)
        self.store.mark_acked(self.account_id, snapshot_cursor)

        self.logger(
            "Cursor 过期恢复完成："
            f"补偿群消息={len(missing_ids)}，"
            f"纳入上下文={updated}，新 Cursor={snapshot_cursor}"
        )
        return snapshot_cursor

    def _handle_frame(self, frame: SSEFrame, current_cursor: str) -> tuple[str, bool]:
        if frame.event == "error":
            raise FatalConsumerError("SSE 服务端返回 error 事件")

        try:
            event = json.loads(frame.data)
        except json.JSONDecodeError as exc:
            raise FatalConsumerError("SSE 事件 JSON 格式异常") from exc

        if not isinstance(event, dict):
            raise FatalConsumerError("SSE 事件格式异常")

        next_cursor = event.get("cursor")
        kind = event.get("kind")
        data = event.get("data")
        if (
            not valid_cursor(next_cursor)
            or not isinstance(kind, str)
            or not kind
            or not isinstance(data, dict)
        ):
            raise FatalConsumerError("无效的 SSE 事件")

        if frame.event_id and frame.event_id != next_cursor:
            raise FatalConsumerError("SSE ID 与 Cursor 不一致")

        if int(next_cursor) <= int(current_cursor):
            raise FatalConsumerError("Cursor 未向前推进")

        if event.get("accountId") not in (None, self.account_id):
            raise FatalConsumerError("收到其他账号的事件")

        thread_id = data.get("threadId")
        message_id = data.get("messageId")
        is_target = (
            kind == "message.committed"
            and thread_id == self.group_id
            and isinstance(message_id, str)
            and bool(message_id)
        )

        # Privacy boundary: for all non-target events/groups, persist only
        # account, cursor and event kind; never persist other group IDs.
        try:
            self.store.persist_event(
                self.account_id,
                next_cursor,
                kind,
                thread_id=self.group_id if is_target else None,
                message_id=message_id if is_target else None,
            )
        except sqlite3.OperationalError as exc:
            detail = str(exc).lower()
            if "locked" in detail or "busy" in detail:
                raise RetryableConsumerError(
                    "SQLite is temporarily busy while persisting event"
                ) from exc
            raise FatalConsumerError(
                "SQLite rejected event persistence"
            ) from exc

        # Critical ordering: durable local commit -> remote ACK -> local ACK.
        # If remote ACK fails, the next reconnect compensates it from SQLite.
        self.api.acknowledge(next_cursor)
        try:
            self.store.mark_acked(self.account_id, next_cursor)
        except sqlite3.OperationalError as exc:
            detail = str(exc).lower()
            if "locked" in detail or "busy" in detail:
                raise RetryableConsumerError(
                    "SQLite is temporarily busy while recording ACK"
                ) from exc
            raise FatalConsumerError(
                "SQLite rejected local ACK checkpoint"
            ) from exc

        return next_cursor, is_target

    def consume_once(self, cursor: str) -> dict[str, int]:
        """Consume one SSE connection until EOF, stop, or a fatal error."""
        counts = {"events": 0, "target_messages": 0}
        with self.api.open_event_stream(cursor) as response:
            self.logger("SSE 连接成功；Consumer 持续监听")
            for frame in iter_sse_frames(
                response, stop_event=self.stop_event
            ):
                if self.stop_event.is_set():
                    break

                next_cursor, is_target = self._handle_frame(frame, cursor)
                cursor = next_cursor
                counts["events"] += 1
                if is_target:
                    counts["target_messages"] += 1
                    self.logger(
                        f"目标群消息事件已持久化并 ACK | Cursor={cursor}"
                    )

        return counts

    def run_forever(self) -> None:
        """Reconnect indefinitely with bounded exponential backoff."""
        backoff = self.backoff_initial
        self.logger("Consumer 启动；自动发送保持关闭")

        while not self.stop_event.is_set():
            started = self.monotonic()
            try:
                try:
                    cursor = self.prepare_cursor()
                    self.logger(f"恢复订阅 Cursor={cursor}")
                    self.consume_once(cursor)
                except CursorExpiredError as exc:
                    self.logger(
                        f"检测到 event_cursor_expired：{exc}；开始安全历史补偿"
                    )
                    self.recover_expired_cursor()
                    backoff = self.backoff_initial
                    continue

                if self.stop_event.is_set():
                    break

                raise RetryableConsumerError("SSE stream ended; reconnecting")

            except FatalConsumerError:
                raise
            except sqlite3.OperationalError as exc:
                detail = str(exc).lower()
                if "locked" not in detail and "busy" not in detail:
                    raise FatalConsumerError(
                        "SQLite returned a non-retryable operational error"
                    ) from exc
                elapsed = self.monotonic() - started
                if elapsed >= self.stable_connection_seconds:
                    backoff = self.backoff_initial
                delay = min(backoff, self.backoff_max)
                self.logger(
                    f"SQLite 暂时繁忙；{delay:g}s 后重试 Consumer"
                )
                if self.stop_event.wait(delay):
                    break
                backoff = min(backoff * 2, self.backoff_max)
            except (RetryableConsumerError, URLError, TimeoutError,
                    socket.timeout, ConnectionError, OSError,
                    http.client.HTTPException) as exc:
                elapsed = self.monotonic() - started
                if elapsed >= self.stable_connection_seconds:
                    backoff = self.backoff_initial

                delay = min(backoff, self.backoff_max)
                self.logger(
                    f"Consumer 临时故障：{type(exc).__name__}；"
                    f"{delay:g}s 后重连"
                )
                if self.stop_event.wait(delay):
                    break
                backoff = min(backoff * 2, self.backoff_max)

        self.logger("Consumer 已优雅退出")


def _resolve_config_path(value: str | Path, base_dir: Path) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = base_dir / path
    return path.resolve()


def _canonical_shared_path(value: object) -> Path:
    if not isinstance(value, str):
        raise FatalConsumerError("隔离存储需要绝对路径")
    path = Path(value)
    try:
        if not path.is_absolute() or path.resolve(strict=True) != path:
            raise FatalConsumerError("隔离存储需要无链接的绝对路径")
    except (OSError, ValueError, RuntimeError) as exc:
        raise FatalConsumerError("隔离存储路径不可用") from exc
    return path


def observer_state_directory(config: dict, database_path: Path) -> Path:
    """Return the validated private directory for locks and Worker activation state."""
    storage = config["storage"]
    if "shared_group_id" not in storage:
        return database_path.parent
    return _canonical_shared_path(storage.get("service_state_directory"))


def _validate_database_layout(config: dict, database_path: Path) -> None:
    storage = config["storage"]
    if "shared_group_id" not in storage:
        if "service_state_directory" in storage:
            raise FatalConsumerError("单身份配置不能指定独立状态目录")
        if not database_path.is_file():
            raise FatalConsumerError("SQLite 数据库文件不存在；拒绝创建新数据库")
        if database_path.stat().st_mode & 0o077:
            raise FatalConsumerError("SQLite 数据库权限过宽；要求 600 或更严格")
        if database_path.parent.stat().st_mode & 0o077:
            raise FatalConsumerError("SQLite 数据目录权限过宽；要求 700 或更严格")
        return

    group = storage["shared_group_id"]
    if type(group) is not int or group <= 0 or group not in {*os.getgroups(), os.getegid()}:
        raise FatalConsumerError("进程不属于配置的 SQLite 共享组")
    if _canonical_shared_path(storage["database"]) != database_path:
        raise FatalConsumerError("SQLite 路径与隔离配置不一致")
    private_state = observer_state_directory(config, database_path)
    shared = database_path.parent
    if private_state.is_relative_to(shared) or shared.is_relative_to(private_state):
        raise FatalConsumerError("观察状态目录必须独立于 SQLite 共享目录")
    try:
        db, directory, state = database_path.stat(), shared.stat(), private_state.stat()
    except OSError as exc:
        raise FatalConsumerError("隔离存储不可用") from exc
    owner = os.geteuid()
    if (not stat.S_ISREG(db.st_mode) or db.st_nlink != 1 or db.st_uid != owner
            or db.st_gid != group or stat.S_IMODE(db.st_mode) != 0o660):
        raise FatalConsumerError("共享 SQLite 需要观察进程拥有且权限为 660")
    if (not stat.S_ISDIR(directory.st_mode) or directory.st_uid != owner
            or directory.st_gid != group or stat.S_IMODE(directory.st_mode) != 0o2770):
        raise FatalConsumerError("共享 SQLite 目录需要观察进程拥有且权限为 2770")
    if (not stat.S_ISDIR(state.st_mode) or state.st_uid != owner
            or stat.S_IMODE(state.st_mode) != 0o700):
        raise FatalConsumerError("独立观察状态目录需要观察进程拥有且权限为 700")


def validate_config(
    config: dict, *, base_dir: Path | None = None
) -> None:
    """Fail closed unless the current observe-only safety gates are intact."""
    if not isinstance(config, dict) or config.get("schema_version") != 2:
        raise FatalConsumerError("不支持的配置 Schema")

    base_dir = (base_dir or Path.cwd()).resolve()

    try:
        safe = (
            config["app"]["auto_send"] is False
            and config["sender"]["enabled"] is False
            and config["runtime"]["global_pause"] is True
        )
        wx = config["agent_wechat"]
        group = wx["target_group_id"]
        allowlisted = group in config["safety"]["group_allowlist"]
        base_url = wx["base_url"].rstrip("/")
        account = wx["account_id"]
        consumer = wx["consumer_name"]
        token_path = _resolve_config_path(wx["token_file"], base_dir)
        database_path = _resolve_config_path(
            config["storage"]["database"], base_dir
        )
    except (KeyError, TypeError, AttributeError) as exc:
        raise FatalConsumerError("配置字段缺失或格式错误") from exc

    if not safe:
        raise FatalConsumerError("安全开关异常；Consumer 拒绝启动")
    if not allowlisted:
        raise FatalConsumerError("目标群不在白名单中")
    if base_url != EXPECTED_AGENT_WECHAT_BASE_URL:
        raise FatalConsumerError("拒绝非本机 Agent WeChat 地址")
    if not all(isinstance(value, str) and value for value in (group, account, consumer)):
        raise FatalConsumerError("账号、群或 Consumer 名称为空")
    if not token_path.is_file():
        raise FatalConsumerError("Agent WeChat Token 文件不存在")
    if token_path.stat().st_mode & 0o077:
        raise FatalConsumerError("Token 文件权限过宽；要求 600 或更严格")
    if not token_path.read_text(encoding="utf-8").strip():
        raise FatalConsumerError("Agent WeChat Token 为空")
    _validate_database_layout(config, database_path)


@contextmanager
def single_instance_lock(lock_path: Path):
    """Hold an exclusive advisory lock for the full process lifetime."""
    lock_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    flags = os.O_CREAT | os.O_RDWR
    fd = os.open(lock_path, flags, 0o600)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "a+") as lock_file:
            try:
                fcntl.flock(
                    lock_file.fileno(),
                    fcntl.LOCK_EX | fcntl.LOCK_NB,
                )
            except BlockingIOError as exc:
                raise FatalConsumerError("另一个 Consumer 已在运行") from exc
            yield
    except BaseException:
        # fdopen owns fd after successful construction; if it failed, close it.
        try:
            os.close(fd)
        except OSError:
            pass
        raise


def _load_config(config_path: Path) -> dict:
    try:
        value = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, ValueError, RecursionError) as exc:
        raise FatalConsumerError(
            f"无法读取配置文件：{type(exc).__name__}"
        ) from exc
    if not isinstance(value, dict):
        raise FatalConsumerError("配置文件根节点必须是 JSON 对象")
    return value


def _observer_inputs(config_path: Path) -> tuple[dict, Path, str]:
    """Load the validated startup inputs shared by both observation services."""
    try:
        config_path = config_path.expanduser().resolve()
        config = _load_config(config_path)
        validate_config(config, base_dir=config_path.parent)
        token_path = _resolve_config_path(config["agent_wechat"]["token_file"], config_path.parent)
        database_path = _resolve_config_path(config["storage"]["database"], config_path.parent)
        token = token_path.read_text(encoding="utf-8").strip()
    except (OSError, ValueError, RecursionError) as exc:
        raise FatalConsumerError(f"无法读取观察服务启动配置：{type(exc).__name__}") from exc
    if not token:
        raise FatalConsumerError("Agent WeChat Token 为空")
    return config, database_path, token


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Long-running Agent WeChat SSE consumer"
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path.home() / "apps/wechat-ai-bot/config.json",
    )
    args = parser.parse_args(argv)

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

    stop_event = threading.Event()
    previous_handlers = {}

    def request_stop(signum, _frame):
        stop_event.set()

    for signum in (signal.SIGTERM, signal.SIGINT):
        previous_handlers[signum] = signal.signal(signum, request_stop)

    try:
        lock_path = observer_state_directory(config, database_path) / "consumer.lock"
        with single_instance_lock(lock_path):
            store = Storage(
                database_path,
                consumer_name=wx["consumer_name"],
            )
            try:
                consumer = EventConsumer(
                    config=config,
                    store=store,
                    api=api,
                    stop_event=stop_event,
                )
                consumer.run_forever()
            finally:
                store.close()
    except FatalConsumerError as exc:
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
