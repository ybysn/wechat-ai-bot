"""Scheduled occurrences and their durable send records for the isolated Sender."""

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from hashlib import sha256
import json
import sqlite3
from zoneinfo import ZoneInfo

from wechat_ai_bot.scheduler import CHECKIN_TIMEZONE
from wechat_ai_bot.sender_http_contract import encode_text_send_body


ZONE = ZoneInfo(CHECKIN_TIMEZONE)
OCCURRENCE_GRACE = timedelta(minutes=5)


@dataclass(frozen=True, repr=False)
class ScheduledDispatch:
    account_id: str
    thread_id: str
    text: str
    idempotency_key: str
    request_sha256: str
    remote_send_id: str | None = None


def scheduled_occurrences(settings, policy, now):
    """Yield current occurrences with identities stable across restarts."""
    local = now.astimezone(ZONE)
    day = local.date()
    plans = settings["scheduledMessages"]
    if settings["checkinEnabled"]:
        plans = [*plans, {"enabled": True, "cadence": "daily", "date": "",
                         "time": settings["checkinTime"], "text": "签到"}]
    due = {}
    for plan in plans:
        if not plan["enabled"]:
            continue
        days = (date.fromisoformat(plan["date"]),) if plan["cadence"] == "once" else (day, day - timedelta(days=1))
        when = time.fromisoformat(plan["time"])
        for planned_day in days:
            scheduled_for = datetime.combine(planned_day, when, tzinfo=ZONE)
            if (scheduled_for.timestamp() < policy.starts_at
                    or not timedelta(0) <= local - scheduled_for < OCCURRENCE_GRACE):
                continue
            body_hash = sha256(encode_text_send_body(thread_id=policy.thread_id, text=plan["text"])).hexdigest()
            identity = json.dumps([policy.account_id, policy.thread_id, scheduled_for.isoformat(), body_hash])
            key = "scheduled-" + sha256(identity.encode("ascii")).hexdigest()
            due[key] = (scheduled_for, ScheduledDispatch(policy.account_id, policy.thread_id, plan["text"], key, body_hash))
    return [claim for _, claim in sorted(due.values(), key=lambda item: (item[0], item[1].idempotency_key))]


class ScheduledHistory:
    """Private SQLite ledger. The production dispatcher serializes both sources."""

    def __init__(self, path):
        connection = sqlite3.connect(path, timeout=5, isolation_level=None)
        self.connection = connection
        try:
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA synchronous=FULL")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS schedule_dispatch (
                    idempotency_key TEXT PRIMARY KEY,
                    account_id TEXT NOT NULL,
                    thread_id TEXT NOT NULL,
                    text TEXT NOT NULL,
                    request_sha256 TEXT NOT NULL,
                    status TEXT NOT NULL CHECK(status IN ('sending','sent','needs_review')),
                    remote_send_id TEXT,
                    sent_at TEXT,
                    CHECK((status='sent') = (sent_at IS NOT NULL))
                )
                """
            )
        except BaseException:
            connection.close()
            raise

    def close(self):
        self.connection.close()

    def pending(self):
        row = self.connection.execute(
            "SELECT account_id,thread_id,text,idempotency_key,request_sha256,remote_send_id "
            "FROM schedule_dispatch WHERE status IN ('sending','needs_review') ORDER BY rowid LIMIT 1"
        ).fetchone()
        return ScheduledDispatch(**row) if row else None

    def reserve(self, claim):
        result = self.connection.execute(
            "INSERT INTO schedule_dispatch(idempotency_key,account_id,thread_id,text,request_sha256,status) "
            "VALUES(?,?,?,?,?,'sending') ON CONFLICT(idempotency_key) DO NOTHING",
            (claim.idempotency_key, claim.account_id, claim.thread_id, claim.text, claim.request_sha256),
        )
        return result.rowcount == 1

    def exists(self, key):
        return self.connection.execute(
            "SELECT 1 FROM schedule_dispatch WHERE idempotency_key=?", (key,),
        ).fetchone() is not None

    def last_sent_at(self, account, group):
        row = self.connection.execute(
            "SELECT MAX(sent_at) FROM schedule_dispatch WHERE account_id=? AND thread_id=? AND status='sent'",
            (account, group),
        ).fetchone()
        return row[0]

    def finish(self, claim, remote_id, confirmed_at=None):
        row = self.connection.execute(
            "UPDATE schedule_dispatch SET status=?,remote_send_id=COALESCE(?,remote_send_id),sent_at=? "
            "WHERE idempotency_key=? AND status IN ('sending','needs_review') "
            "AND (remote_send_id IS NULL OR ? IS NULL OR remote_send_id=?)",
            ("sent" if confirmed_at else "needs_review", remote_id, confirmed_at,
             claim.idempotency_key, remote_id, remote_id),
        )
        if row.rowcount != 1:
            raise RuntimeError("SCHEDULED_DISPATCH_STATE_CHANGED")
