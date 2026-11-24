"""P6.5D pre-dispatch policy. Never performs any network or WeChat send.

This module is intentionally dormant until a future sender explicitly uses
Storage.claim_outbox_for_send(). Admission uses actual sent_at, not queued_at.
"""

from datetime import datetime, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from wechat_ai_bot.reply_safety import candidate_is_fresh, utc_now

GROUP_SEND_COOLDOWN_SECONDS = 120
CHECKIN_SEND_GRACE_SECONDS = 120


def _timestamp(value):
    """Parse only SQLite/ISO UTC timestamps; missing or invalid fail closed."""
    if not isinstance(value, str) or not value:
        return None
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if result.tzinfo is None:
        result = result.replace(tzinfo=timezone.utc)  # SQLite CURRENT_TIMESTAMP
    return result.astimezone(timezone.utc)


def _checkin_due(row, now):
    if (
        row["task_type"] != "daily_checkin"
        or row["task_status"] != "queued"
        or row["content"] != "签到"
    ):
        return False
    try:
        zone = ZoneInfo(row["timezone"])
        scheduled_at = datetime.fromisoformat(row["scheduled_at"])
        if scheduled_at.tzinfo is None:
            return False
        scheduled_at = scheduled_at.astimezone(timezone.utc)
        return (
            row["timezone"] == "Asia/Shanghai"
            and now.astimezone(zone).date().isoformat() == row["local_date"]
            and 0 <= (now - scheduled_at).total_seconds() <= CHECKIN_SEND_GRACE_SECONDS
        )
    except (ValueError, TypeError, ZoneInfoNotFoundError):
        return False


def _config_permits(config, account_id, thread_id):
    """No implicit enablement: every sender gate and target must match."""
    try:
        return (
            config["schema_version"] == 2
            and config["app"]["auto_send"] is True
            and config["sender"]["enabled"] is True
            and config["runtime"]["global_pause"] is False
            and config["agent_wechat"]["account_id"] == account_id
            and config["agent_wechat"]["target_group_id"] == thread_id
            and isinstance(config["safety"]["group_allowlist"], (list, tuple))
            and thread_id in config["safety"]["group_allowlist"]
        )
    except (KeyError, TypeError):
        return False


def dispatch_block_reason(conn, row, config, now, *, automatic_policy=None, recheck=False):
    """Read-only policy; caller must run inside a BEGIN IMMEDIATE transaction.

    All queued sources share one cooldown and in-flight/uncertain block.
    Only the newest fresh queued AI proposal can be claimed. A queued,
    currently due scheduled check-in wins over AI candidates.
    """
    current = utc_now(now)
    account, group = row["account_id"], row["thread_id"]
    permits = _config_permits(config, account, group) if automatic_policy is None else (
        automatic_policy.permits(config, current)
        and (account, group) == (automatic_policy.account_id, automatic_policy.thread_id)
        and row["scheduled_task_id"] is None
    )
    if not permits:
        return "sending_paused_or_target_mismatch"
    expected_status = "sending" if automatic_policy is not None and recheck else "queued"
    if row["status"] != expected_status:
        return "outbox_not_queued"

    conflicts = conn.execute(
        "SELECT status, sent_at FROM outbox "
        "WHERE account_id=? AND thread_id=? AND outbox_id<>? "
        "AND status IN ('sending','needs_review','sent')",
        (account, group, row["outbox_id"]),
    ).fetchall()
    for other in conflicts:
        if other["status"] in ("sending", "needs_review"):
            return "unresolved_send_exists"
        sent_at = _timestamp(other["sent_at"])
        if sent_at is None:
            return "sent_record_missing_timestamp"
        elapsed = (current - sent_at).total_seconds()
        if elapsed < GROUP_SEND_COOLDOWN_SECONDS:
            return "group_send_cooldown"

    if row["decision_id"] is not None:
        source = conn.execute(
            "SELECT d.action,d.status,d.reply,d.created_at AS decided_at, "
            "m.created_at AS message_at,m.source_cursor,m.thread_id,m.direction "
            "FROM decisions d JOIN messages m "
            "ON m.account_id=d.account_id AND m.message_id=d.source_message_id "
            "WHERE d.decision_id=? AND d.account_id=?",
            (row["decision_id"], account),
        ).fetchone()
        if (
            source is None or source["action"] != "reply"
            or source["status"] != "accepted"
            or source["reply"] != row["content"]
            or source["thread_id"] != group
            or source["direction"] != "incoming"
            or not candidate_is_fresh(
                source["message_at"], source["decided_at"], now=current
            )
        ):
            return "reply_expired_or_invalid"

        # A due check-in takes priority over an AI reply, but only within its
        # explicit two-minute dispatch grace window.
        checkins = conn.execute(
            "SELECT t.task_type,t.status AS task_status,t.content,"
            "t.timezone,t.scheduled_at,t.local_date "
            "FROM outbox o JOIN scheduled_tasks t "
            "ON t.task_id=o.scheduled_task_id "
            "WHERE o.account_id=? AND o.thread_id=? AND o.status='queued' "
            "AND o.scheduled_task_id IS NOT NULL",
            (account, group),
        ).fetchall()
        if any(_checkin_due(task, current) for task in checkins):
            return "due_checkin_priority"

        # Only the newest eligible AI reply for this group can claim.
        contenders = conn.execute(
            "SELECT o.outbox_id,d.created_at AS decided_at,"
            "m.created_at AS message_at,m.source_cursor "
            "FROM outbox o JOIN decisions d ON d.decision_id=o.decision_id "
            "JOIN messages m ON m.account_id=d.account_id "
            "AND m.message_id=d.source_message_id "
            "WHERE o.account_id=? AND o.thread_id=? AND (o.status='queued' OR o.outbox_id=?) "
            "AND o.decision_id IS NOT NULL AND d.action='reply' "
            "AND d.status='accepted'",
            (account, group, row["outbox_id"]),
        ).fetchall()
        valid = []
        for candidate in contenders:
            cursor = candidate["source_cursor"]
            if not isinstance(cursor, str) or not cursor.isascii() or not cursor.isdigit():
                continue
            if candidate_is_fresh(
                candidate["message_at"], candidate["decided_at"], now=current
            ):
                valid.append((int(cursor), candidate["outbox_id"]))
        if not valid or max(valid)[1] != row["outbox_id"]:
            return "newer_reply_preferred"
    elif row["scheduled_task_id"] is not None:
        task = conn.execute(
            "SELECT task_type,status AS task_status,content,timezone,"
            "scheduled_at,local_date FROM scheduled_tasks WHERE task_id=? "
            "AND account_id=? AND thread_id=?",
            (row["scheduled_task_id"], account, group),
        ).fetchone()
        if task is None or not _checkin_due(task, current):
            return "checkin_not_due_or_expired"
    else:
        return "unknown_outbox_source"
    return None
