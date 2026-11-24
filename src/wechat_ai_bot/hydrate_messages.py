"""Read-only Agent WeChat message hydration with durable local persistence.

The remote API is read-only. Local SQLite writes are performed through Storage.
"""

import json
import sys
from pathlib import Path
from urllib.parse import quote
from urllib.request import Request, urlopen

from wechat_ai_bot.storage import Storage


def persist_hydrated_message(
    store,
    *,
    account,
    message_id,
    group,
    cursor,
    direction,
    body,
    sender_id,
    message_type,
    mentions_self,
    created_at,
    max_chars,
):
    """Persist one hydrated message, then mark its SSE event processed.

    Storage.persist_message() owns its transaction, so it must not be called
    inside another Storage.transaction(). The message is committed first.
    The event status and any message-status transition are committed second.

    If the process stops between these commits, the event remains 'received'.
    A retry replays persist_message() idempotently and can finish the event.
    """
    if type(max_chars) is not int or max_chars < 1:
        raise ValueError("Invalid processing.max_message_chars")

    existing = store.message(account, message_id)

    exceeded = body is not None and len(body) > max_chars
    blank = body is not None and not body.strip()

    # Oversized or blank text is not sent to the AI. Store it as unknown so
    # a later successful hydration can safely enrich NULL -> non-empty text.
    saved_body = None if exceeded or blank else body

    # Do not silently hide a conflict by converting an oversized new body to
    # NULL when a previously-known body exists.
    if (
        exceeded
        and existing is not None
        and existing["body"] is not None
        and existing["body"] != body
    ):
        raise ValueError("Conflicting message field: body")

    # This call commits its own transaction. Do not wrap it in
    # store.transaction(); nested BEGIN IMMEDIATE is not supported.
    store.persist_message(
        account,
        message_id,
        group,
        direction=direction,
        body=saved_body,
        sender_id=sender_id,
        message_type=message_type,
        mentions_self=mentions_self,
        created_at=created_at,
        source_cursor=cursor,
    )

    persisted = store.message(account, message_id)
    if persisted is None:
        raise RuntimeError("消息持久化后无法读取")

    usable = (
        persisted["direction"] == "incoming"
        and isinstance(persisted["body"], str)
        and bool(persisted["body"].strip())
        and len(persisted["body"]) <= max_chars
    )

    # The message row is already durable. Keep the status transition and
    # event completion in one transaction. A failed event update rolls back
    # the status transition, while the durable message remains safe to replay.
    with store.transaction():
        if usable:
            # An earlier hydration may have stored an incomplete message as
            # ignored. Once usable content is available, make it processable.
            store.conn.execute(
                "UPDATE messages SET status='pending' "
                "WHERE account_id=? AND message_id=? "
                "AND status='ignored'",
                (account, message_id),
            )
        else:
            # New rows default to pending. Empty, oversized, outgoing, or
            # direction-unknown messages must not enter the AI queue.
            store.conn.execute(
                "UPDATE messages SET status='ignored' "
                "WHERE account_id=? AND message_id=? "
                "AND status='pending'",
                (account, message_id),
            )

        updated = store.conn.execute(
            "UPDATE events SET status='processed' "
            "WHERE account_id=? AND cursor=? "
            "AND status='received'",
            (account, cursor),
        )

        if updated.rowcount != 1:
            raise RuntimeError("事件状态发生并发变化")

    final_message = store.message(account, message_id)
    return final_message, exceeded


def main():
    home = Path.home()
    cfg = json.loads(
        (home / "apps/wechat-ai-bot/config.json")
        .read_text(encoding="utf-8")
    )

    if (
        cfg.get("schema_version") != 2
        or cfg["app"]["auto_send"] is not False
        or cfg["sender"]["enabled"] is not False
        or cfg["runtime"]["global_pause"] is not True
    ):
        raise RuntimeError("安全开关异常，拒绝执行")

    wx = cfg["agent_wechat"]
    base = wx["base_url"].rstrip("/")
    account = wx["account_id"]
    group = wx["target_group_id"]

    if base != "http://127.0.0.1:6174/v1":
        raise RuntimeError("Agent WeChat 地址异常")

    if group not in cfg["safety"]["group_allowlist"]:
        raise RuntimeError("目标群不在白名单中")

    max_chars = cfg["processing"]["max_message_chars"]
    if type(max_chars) is not int or max_chars < 1:
        raise RuntimeError("max_message_chars 配置异常")

    token = Path(wx["token_file"]).read_text(
        encoding="utf-8"
    ).strip()

    store = Storage(
        cfg["storage"]["database"],
        wx["consumer_name"],
    )

    try:
        events = store.conn.execute(
            "SELECT cursor, message_id FROM events "
            "WHERE account_id=? AND thread_id=? "
            "AND kind='message.committed' "
            "AND status='received' "
            "AND message_id IS NOT NULL "
            "ORDER BY CAST(cursor AS INTEGER) LIMIT 20",
            (account, group),
        ).fetchall()

        print("===== 消息正文与 @ 状态补全 =====")
        print("待补全目标群事件:", len(events))

        if not events:
            print("没有待补全的事件，无需重复处理")
            return

        for event in events:
            cursor = event["cursor"]
            message_id = event["message_id"]

            url = (
                f"{base}/accounts/"
                f"{quote(account, safe='')}/messages/"
                f"{quote(message_id, safe='')}"
            )

            req = Request(
                url,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Accept": "application/json",
                },
            )

            with urlopen(req, timeout=15) as response:
                msg = json.load(response)

            if not isinstance(msg, dict) or (
                msg.get("id") != message_id
                or msg.get("accountId") != account
                or msg.get("threadId") != group
            ):
                raise RuntimeError("消息详情与事件身份不匹配")

            direction = msg.get("direction")
            body = msg.get("body")

            if direction not in ("incoming", "outgoing", None):
                raise RuntimeError("未知消息方向")

            if body is not None and not isinstance(body, str):
                raise RuntimeError("消息正文格式异常")

            msg_type = msg.get("type")
            if msg_type is not None and type(msg_type) is not int:
                raise RuntimeError("消息类型格式异常")

            mentions = msg.get("mentions")
            mentions_self = None

            if (
                isinstance(mentions, dict)
                and mentions.get("state") == "known"
            ):
                mentions_self = mentions.get("self")
                if (
                    mentions_self is not None
                    and type(mentions_self) is not bool
                ):
                    raise RuntimeError("@信息格式异常")

            sender_id = msg.get("senderId")
            created_at = msg.get("createdAt")

            if (
                sender_id is not None
                and not isinstance(sender_id, str)
            ):
                raise RuntimeError("发送者字段格式异常")

            if (
                created_at is not None
                and not isinstance(created_at, str)
            ):
                raise RuntimeError("消息时间格式异常")

            persisted, exceeded = persist_hydrated_message(
                store,
                account=account,
                message_id=message_id,
                group=group,
                cursor=cursor,
                direction=direction,
                body=body,
                sender_id=sender_id,
                message_type=msg_type,
                mentions_self=mentions_self,
                created_at=created_at,
                max_chars=max_chars,
            )

            print("消息方向:", persisted["direction"])
            print(
                "正文长度:",
                len(body) if isinstance(body, str) else "空",
            )
            print("超长正文:", exceeded)
            print("处理状态:", persisted["status"])
            print(
                "@本人:",
                persisted["mentions_self"]
                if persisted["mentions_self"] is not None
                else "未知",
            )
            print("消息持久化与事件完成: PASS")

        print("===== 入库结果：PASS =====")

    finally:
        store.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(
            f"FAIL: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        sys.exit(1)
