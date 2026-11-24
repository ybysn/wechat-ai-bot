"""Process one saved incoming message. Never send to WeChat."""

import json
import sys
from pathlib import Path

from wechat_ai_bot.decision_engine import (
    DecisionDeferred, decide, selected_model,
)
from wechat_ai_bot.storage import Storage
from wechat_ai_bot.reply_safety import candidate_is_fresh


def context_until(conn, cfg, anchor_cursor):
    wx = cfg["agent_wechat"]
    count = cfg["processing"]["max_context_messages"]

    if type(count) is not int or not 1 <= count <= 20:
        raise ValueError("Invalid context limit")

    if (
        not isinstance(anchor_cursor, str)
        or not anchor_cursor.isascii()
        or not anchor_cursor.isdecimal()
        or len(anchor_cursor) > 19
    ):
        raise ValueError("Invalid source cursor")

    rows = conn.execute("""
        SELECT sender_id, direction, body
        FROM messages
        WHERE account_id=? AND thread_id=?
          AND source_cursor IS NOT NULL
          AND source_cursor != ''
          AND source_cursor NOT GLOB '*[^0-9]*'
          AND CAST(source_cursor AS INTEGER) <= ?
          AND direction IN ('incoming','outgoing')
          AND body IS NOT NULL AND TRIM(body) != ''
          AND status IN ('pending','processed','ignored')
        ORDER BY CAST(source_cursor AS INTEGER) DESC, rowid DESC
        LIMIT ?
    """, (
        wx["account_id"],
        wx["target_group_id"],
        int(anchor_cursor),
        count,
    )).fetchall()

    result = []
    speakers = {}
    remaining = 1600

    for row in rows:
        if remaining <= 0:
            break

        body = row["body"].strip()[:min(320, remaining)]
        if not body:
            continue

        remaining -= len(body)

        if row["direction"] == "outgoing":
            role = "account_owner"
            speaker = "当前微信账号"
        else:
            role = "group_member"
            identity = row["sender_id"] or "未知成员"

            if identity not in speakers:
                speakers[identity] = len(speakers) + 1

            speaker = f"群友{speakers[identity]}"

        result.append({
            "role": role,
            "speaker": speaker,
            "text": body,
        })

    return list(reversed(result))


def process_one(
    store, cfg, decider=decide, *, activation_cursor=None, message_id=None,
    coalesced=None,
):
    if (
        cfg.get("schema_version") != 2
        or cfg["app"]["auto_send"] is not False
        or cfg["sender"]["enabled"] is not False
        or cfg["runtime"]["global_pause"] is not True
    ):
        raise RuntimeError("Observe-only safety guard failed")

    wx = cfg["agent_wechat"]
    account = wx["account_id"]
    group = wx["target_group_id"]

    if group not in cfg["safety"]["group_allowlist"]:
        raise RuntimeError("Group is not allowlisted")

    # Optional activation boundary prevents pre-activation pending messages
    # from becoming late replies. Optional message_id lets the worker retry
    # one selected row without changing the existing one-shot CLI behavior.
    conditions = [
        "account_id=?", "thread_id=?", "direction='incoming'",
        "status='pending'", "source_cursor IS NOT NULL",
        "source_cursor != ''",
        "source_cursor NOT GLOB '*[^0-9]*'",
        "length(source_cursor) <= 19",
        "body IS NOT NULL", "TRIM(body) != ''",
    ]
    parameters = [account, group]

    if activation_cursor is not None:
        Storage._cursor_number(activation_cursor)
        conditions.append("CAST(source_cursor AS INTEGER)>?")
        parameters.append(int(activation_cursor))

    if message_id is not None:
        if not isinstance(message_id, str) or not message_id:
            raise ValueError("Invalid message_id")
        conditions.append("message_id=?")
        parameters.append(message_id)

    row = store.conn.execute(
        "SELECT message_id, body, source_cursor, created_at FROM messages WHERE "
        + " AND ".join(conditions)
        + " ORDER BY CAST(source_cursor AS INTEGER), rowid LIMIT 1",
        parameters,
    ).fetchone()

    if row is None:
        return {"status": "empty"}

    message_id = row["message_id"]

    # 崩溃恢复：已有决策时不重新调用模型
    existing = store.conn.execute("""
        SELECT action FROM decisions
        WHERE account_id=? AND source_message_id=?
    """, (account, message_id)).fetchone()

    if existing:
        with store.transaction():
            store.conn.execute("""
                UPDATE messages SET status='processed'
                WHERE account_id=? AND message_id=?
                  AND status='pending'
            """, (account, message_id))

        return {
            "status": "already_decided",
            "action": existing["action"],
        }

    context = context_until(
        store.conn, cfg, row["source_cursor"]
    )

    if (
        not context
        or context[-1]["role"] != "group_member"
        or context[-1]["text"] != row["body"].strip()[:320]
    ):
        raise RuntimeError("Target message/context mismatch")

    # 模型调用发生在数据库事务之外。系统故障通过异常类型表达，
    # 不使用模型可控的 reason 文本判断是否需要重试。
    try:
        result = decider(context, cfg)
    except DecisionDeferred as exc:
        # 临时故障不能消费掉消息，也不能写入 decisions/outbox。
        return {
            "status": "deferred",
            "error_code": exc.code,
            "error_type": exc.cause_type,
            "validation_code": exc.validation_code,
            "retry_after_seconds": exc.retry_after_seconds,
        }

    action = result.get("action")
    reason = result.get("reason")

    if not isinstance(reason, str) or not reason.strip():
        raise RuntimeError("Invalid decision reason")

    if action not in ("reply", "skip"):
        raise RuntimeError("Invalid decision action")

    if action == "reply":
        if not (
            isinstance(result.get("reply"), str)
            and result["reply"].strip()
        ):
            raise RuntimeError("Empty reply candidate")
    elif result.get("reply") != "":
        raise RuntimeError("Skip must not contain reply")

    # The provider may return after the source message becomes obsolete.
    # A successful model response is not permission to publish a late reply.
    # Apply to the activated Worker path; legacy one-shot CLI behavior stays
    # observe-only and is separately fenced at accept/enqueue time.
    if (
        action == "reply" and activation_cursor is not None
        and not candidate_is_fresh(row["created_at"])
    ):
        action = "skip"
        reason = "reply_candidate_expired"
        result = {"reply": ""}

    status = (
        "proposed" if action == "reply"
        else "rejected"
    )
    reply = result["reply"] if action == "reply" else None

    # Storage owns the single transaction for decision + message status.
    if coalesced is not None:
        if activation_cursor is None:
            raise ValueError("Conversation requires activation boundary")
        decision_id, decision_count = store.decide_conversation_and_process(
            account, group, activation_cursor, message_id,
            action=action, reason=reason, model=selected_model(cfg),
            reply=reply, decision_status=status, coalesced=coalesced,
        )
    else:
        decision_id, created = store.decide_and_process(
            account, message_id,
            action=action,
            reason=reason,
            model=selected_model(cfg),
            reply=reply,
            decision_status=status,
        )
        if not created:
            raise RuntimeError("Decision was not newly persisted")
        decision_count = 1

    return {
        "status": "saved",
        "decision_id": decision_id,
        "decision_count": decision_count,
        "action": action,
        "reason": reason,
        "candidate": result.get("reply", ""),
    }


def main():
    config = json.loads(
        (Path.home() / "apps/wechat-ai-bot/config.json")
        .read_text(encoding="utf-8")
    )

    store = Storage(
        config["storage"]["database"],
        config["agent_wechat"]["consumer_name"],
    )

    try:
        result = process_one(store, config)
    finally:
        store.close()

    print("===== 真实群聊 AI 决策 =====")
    print("状态:", result["status"])
    if result["status"] == "deferred":
        print("错误代码:", result.get("error_code", "unknown"))
        print("错误类型:", result.get("error_type", "unknown"))
        if result.get("validation_code"):
            print("校验类别:", result["validation_code"])
    print("决策:", result.get("action", "(未执行)"))
    print("理由:", result.get("reason", "(无)"))
    print(
        "候选回复:",
        result.get("candidate") or "(无)",
    )
    print("微信发送: 未执行")
    print("Outbox: 未写入")

    return 1 if result["status"] == "deferred" else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(
            f"FAIL: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        sys.exit(1)
