"""Deterministic scheduling primitives. This module never sends messages."""

from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo


CHECKIN_TIMEZONE = "Asia/Shanghai"


def daily_checkin_tick(store, config, now=None, *, queue_enabled=False):
    """Ensure today's 09:00 check-in task and queue it only when unpaused.

    Queueing is not sending. The current safety configuration deliberately
    leaves the task in `scheduled` state until automatic sending is enabled.
    """
    if config.get("schema_version") != 2:
        raise RuntimeError("Unsupported application config schema")

    app = config["app"]
    sender = config["sender"]
    runtime = config["runtime"]
    wx = config["agent_wechat"]
    account_id = wx["account_id"]
    thread_id = wx["target_group_id"]

    if thread_id not in config["safety"]["group_allowlist"]:
        raise RuntimeError("Check-in target is not allowlisted")

    current = now or datetime.now(timezone.utc)
    if not isinstance(current, datetime) or current.tzinfo is None:
        raise ValueError("now must be a timezone-aware datetime")

    zone = ZoneInfo(CHECKIN_TIMEZONE)
    local_now = current.astimezone(zone)
    local_date = local_now.date().isoformat()

    expired_count = store.expire_stale_scheduled_tasks(
        account_id, thread_id, local_date
    )
    task_id, created = store.ensure_daily_checkin_task(
        account_id,
        thread_id,
        local_date,
        timezone_name=CHECKIN_TIMEZONE,
    )

    # The scheduler may materialize the task while observe-only mode is on,
    # but it must not create an Outbox item until all sending gates are open.
    if (
        queue_enabled is not True
        or app.get("auto_send") is not True
        or sender.get("enabled") is not True
        or runtime.get("global_pause") is not False
    ):
        return {
            "status": "paused",
            "task_id": task_id,
            "created": created,
            "expired_count": expired_count,
            "local_time": local_now.isoformat(),
        }

    outbox_id, queued, status = store.enqueue_scheduled_task(
        task_id, now=current
    )
    return {
        "status": status,
        "task_id": task_id,
        "outbox_id": outbox_id,
        "created": created,
        "queued": queued,
        "expired_count": expired_count,
        "local_time": local_now.isoformat(),
    }
