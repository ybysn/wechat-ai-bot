"""Validation and next-occurrence previews for WeChat scheduling plans."""

from datetime import date, datetime, time, timedelta, timezone
import re
from zoneinfo import ZoneInfo

from wechat_ai_bot.scheduler import CHECKIN_TIMEZONE
from wechat_ai_bot.sender_http_contract import SendContractError, encode_text_send_body


SHANGHAI = ZoneInfo(CHECKIN_TIMEZONE)
MAX_MESSAGE_PLANS = 20
TIME = re.compile(r"(?:[01][0-9]|2[0-3]):[0-5][0-9]\Z")
LIMITS = {
    "modelDaily": (1, 2000), "modelPerMinute": (1, 60), "modelInterval": (0, 120),
    "replyHourly": (1, 2000), "replyDaily": (1, 2000), "replyInterval": (0, 86400),
}


def validate_limits(limits):
    if not isinstance(limits, dict) or limits.keys() != LIMITS.keys():
        raise ValueError("请填写全部限额。")
    for key, (minimum, maximum) in LIMITS.items():
        value = limits[key]
        if type(value) is not int or not minimum <= value <= maximum:
            raise ValueError("限额或间隔超出允许范围。")
    if limits["replyHourly"] > limits["replyDaily"]:
        raise ValueError("每小时回复上限不能超过每日回复上限。")


def validate_message(plan):
    if plan is None:
        return
    if (not isinstance(plan, dict) or plan.keys() != {"enabled", "cadence", "date", "time", "text"}
            or type(plan["enabled"]) is not bool or plan["cadence"] not in ("once", "daily")):
        raise ValueError("定时消息的配置无效。")
    if not isinstance(plan["time"], str) or not TIME.fullmatch(plan["time"]):
        raise ValueError("请选择定时消息的发送时间。")
    if plan["cadence"] == "once":
        try:
            if date.fromisoformat(plan["date"]).isoformat() != plan["date"]:
                raise ValueError
        except (ValueError, TypeError):
            raise ValueError("单次发送需要有效的日期。") from None
    elif plan["date"] != "":
        raise ValueError("每日发送只需填写时间。")
    try:
        encode_text_send_body(thread_id="draft@chatroom", text=plan["text"])
    except SendContractError:
        raise ValueError("请输入 1 至 4,000 字符的定时消息正文。") from None


def validate_messages(plans):
    if not isinstance(plans, list) or len(plans) > MAX_MESSAGE_PLANS:
        raise ValueError(f"最多可保存 {MAX_MESSAGE_PLANS} 条定时消息。")
    for index, plan in enumerate(plans, start=1):
        if plan is None:
            raise ValueError(f"第 {index} 条定时消息缺少配置。")
        try:
            validate_message(plan)
        except ValueError as exc:
            raise ValueError(f"第 {index} 条定时消息：{exc}") from None


def schedule_preview(plan, now=None):
    if not plan["enabled"]:
        return {"state": "paused", "nextAt": None}
    now = (now or datetime.now(timezone.utc)).astimezone(SHANGHAI)
    at = time.fromisoformat(plan["time"])
    day = date.fromisoformat(plan["date"]) if plan["cadence"] == "once" else now.date()
    planned = datetime.combine(day, at, tzinfo=SHANGHAI)
    if planned <= now:
        if plan["cadence"] == "once":
            return {"state": "expired", "nextAt": None}
        planned += timedelta(days=1)
    return {"state": "planned", "nextAt": planned.isoformat(timespec="minutes")}
