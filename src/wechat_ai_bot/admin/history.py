"""Export confirmed bot sends with a read-only SQLite connection."""

import argparse
from contextlib import closing
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sqlite3
import stat


HISTORY_LIMIT = 100
HISTORY_SIZE_LIMIT = 2_500_000
DEPLOYED_CONFIG = Path("/etc/wechat-ai-bot/observer.json")
TRIAL_RECORD = Path("/var/lib/wechat-ai-bot/sender/approved-ok.json")
SCHEDULE_HISTORY = TRIAL_RECORD.with_name("schedule.sqlite3")


def _utc_timestamp(value):
    try:
        timestamp = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return timestamp.astimezone(timezone.utc).isoformat(timespec="seconds")


def _confirmed_trial(path, account, group):
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, "rb") as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_mode & 0o077:
                return None
            data = json.loads(stream.read(16_385))
        if (not isinstance(data, dict) or data.get("state") != "sent" or data.get("text") != "OK"
                or data.get("account") != account or data.get("group") != group
                or not isinstance(data.get("remoteSendId"), str) or not data["remoteSendId"]):
            return None
        stamp = _utc_timestamp(data.get("sentAt"))
        return {"text": "OK", "sentAt": stamp, "source": "test"} if stamp is not None else None
    except (OSError, ValueError, RecursionError):
        return None


def read_sent_history(config_path: Path) -> dict:
    config = json.loads(config_path.read_text(encoding="utf-8"))
    wx = config["agent_wechat"]
    account, group = wx["account_id"], wx["target_group_id"]
    if not account or not group or group not in config["safety"]["group_allowlist"]:
        raise ValueError("Invalid history target")
    path = (config_path.parent / Path(config["storage"]["database"]).expanduser()).resolve(strict=True)
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=3)) as conn:
        conn.execute("PRAGMA query_only=ON")
        rows = conn.execute(
            "SELECT content,sent_at,CASE WHEN scheduled_task_id IS NOT NULL THEN 'scheduled' "
            "WHEN decision_id IS NOT NULL THEN 'reply' ELSE 'other' END FROM outbox "
            "WHERE account_id=? AND thread_id=? AND status='sent' "
            "ORDER BY julianday(sent_at) DESC,outbox_id DESC LIMIT ?",
            (account, group, HISTORY_LIMIT + 1),
        ).fetchall()
    if config_path == DEPLOYED_CONFIG and SCHEDULE_HISTORY.is_file():
        with closing(sqlite3.connect(SCHEDULE_HISTORY.as_uri() + "?mode=ro", uri=True, timeout=3)) as conn:
            conn.execute("PRAGMA query_only=ON")
            rows += conn.execute(
                "SELECT text,sent_at,'scheduled' FROM schedule_dispatch "
                "WHERE account_id=? AND thread_id=? AND status='sent' ORDER BY sent_at DESC LIMIT ?",
                (account, group, HISTORY_LIMIT + 1),
            ).fetchall()
    entries = [{"text": message, "sentAt": _utc_timestamp(sent_at), "source": source}
               for message, sent_at, source in rows]
    trial = _confirmed_trial(TRIAL_RECORD, account, group) if config_path == DEPLOYED_CONFIG else None
    if trial:
        entries.append(trial)
    entries.sort(key=lambda row: row["sentAt"] or "", reverse=True)
    return {
        "checkedAt": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "entries": entries[:HISTORY_LIMIT],
        "hasMore": len(entries) > HISTORY_LIMIT,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    try:
        report = read_sent_history(args.config.absolute())
        encoded = json.dumps(report, ensure_ascii=False)
        if len(encoded.encode("utf-8")) > HISTORY_SIZE_LIMIT:
            raise ValueError("History is too large")
    except (OSError, sqlite3.Error, ValueError, KeyError, TypeError) as exc:
        parser.exit(1, f"History export failed ({type(exc).__name__}).\n")
    print(encoded)


if __name__ == "__main__":
    main()
