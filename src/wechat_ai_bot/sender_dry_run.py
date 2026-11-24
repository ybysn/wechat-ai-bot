"""P6.5D2D-1: read-only production Outbox dispatch eligibility diagnostic.

No Token access, Sender/Storage construction, DB mutation, network request,
LLM invocation, or process/service state change. The diagnostic is advisory:
its output must NEVER be used as authorization for a real send.
"""

from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path
import sqlite3
import subprocess

from wechat_ai_bot.migrations import automatic_schema_ready
from wechat_ai_bot.outbound_safety import dispatch_block_reason
from wechat_ai_bot.reply_safety import utc_now
from wechat_ai_bot.sender_automatic_policy import read_automatic_reply_policy
from wechat_ai_bot.sender_role_boundary import BoundaryFailure


class DryRunError(Exception):
    """Sanitized failure code; never include configuration or SQL content."""


REQUIRED_COLUMNS = frozenset({
    "outbox_id", "account_id", "thread_id", "status", "decision_id",
    "scheduled_task_id", "content", "idempotency_key", "sent_at",
})


def _config_flags(config: dict) -> tuple[list[str], str, str]:
    """Provide separate gate reasons without relaxing dispatch_block_reason."""
    blockers = []
    if config.get("schema_version") != 2:
        blockers.append("config_schema_not_v2")
    app = config.get("app")
    sender = config.get("sender")
    runtime = config.get("runtime")
    wx = config.get("agent_wechat")
    safety = config.get("safety")
    app = app if isinstance(app, dict) else {}
    sender = sender if isinstance(sender, dict) else {}
    runtime = runtime if isinstance(runtime, dict) else {}
    wx = wx if isinstance(wx, dict) else {}
    safety = safety if isinstance(safety, dict) else {}

    if app.get("auto_send") is not True:
        blockers.append("auto_send_not_enabled")
    if sender.get("enabled") is not True:
        blockers.append("sender_not_enabled")
    if runtime.get("global_pause") is not False:
        blockers.append("global_pause_not_disabled")

    account = wx.get("account_id")
    group = wx.get("target_group_id")
    if not isinstance(account, str) or not account:
        blockers.append("account_not_configured")
        account = ""
    if not isinstance(group, str) or not group:
        blockers.append("group_not_configured")
        group = ""
    allowlist = safety.get("group_allowlist")
    if not isinstance(allowlist, (list, tuple)) or group not in allowlist:
        blockers.append("group_not_allowlisted")
    return blockers, account, group


def _read_config(path: Path) -> tuple[dict, Path]:
    try:
        config = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError) as exc:
        raise DryRunError("CONFIG_UNAVAILABLE") from exc
    if not isinstance(config, dict):
        raise DryRunError("CONFIG_NOT_OBJECT")
    storage = config.get("storage")
    if not isinstance(storage, dict):
        raise DryRunError("DATABASE_NOT_CONFIGURED")
    raw = storage.get("database")
    if not isinstance(raw, str) or not raw.strip():
        raise DryRunError("DATABASE_NOT_CONFIGURED")
    db_path = Path(raw).expanduser()
    if not db_path.is_absolute():
        db_path = path.parent / db_path
    try:
        db_path = db_path.resolve(strict=True)
        if not db_path.is_file():
            raise DryRunError("DATABASE_UNAVAILABLE")
    except (OSError, RuntimeError) as exc:
        raise DryRunError("DATABASE_UNAVAILABLE") from exc
    return config, db_path


def inspect_snapshot(
    config_path: Path | str, *, now: datetime | None = None,
    sample_limit: int = 25, automatic_policy_path: Path | None = None,
) -> dict:
    """Inspect a consistent read transaction using SQLite mode=ro + query_only.

    The routine never treats a successful policy check as send authorization.
    Uses SQLite's normal WAL-aware read snapshot, rather than immutable=1
    which can silently skip recently committed WAL entries.
    """
    if type(sample_limit) is not int or not 1 <= sample_limit <= 100:
        raise DryRunError("INVALID_SAMPLE_LIMIT")
    current = utc_now(now)
    path = Path(config_path).expanduser().resolve()
    config, db_path = _read_config(path)
    blockers, account, group = _config_flags(config)
    if not account or not group:
        raise DryRunError("TARGET_NOT_CONFIGURED")

    try:
        # URI mode=ro prevents SQL writes; never use Storage() (RW connection).
        conn = sqlite3.connect(
            db_path.as_uri() + "?mode=ro", uri=True,
            timeout=2, isolation_level=None,
        )
    except sqlite3.Error as exc:
        raise DryRunError("DATABASE_READONLY_OPEN_FAILED") from exc
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only=ON")
        conn.execute("BEGIN")  # read snapshot only (deferred transaction)
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        if version != 4 and not automatic_schema_ready(conn):
            raise DryRunError("DATABASE_SCHEMA_NOT_V4")
        columns = {
            row[1] for row in conn.execute("PRAGMA table_info(outbox)")
        }
        if not REQUIRED_COLUMNS.issubset(columns):
            raise DryRunError("OUTBOX_SCHEMA_INCOMPATIBLE")
        # Status count is strictly scoped to configured account and group.
        counts = {
            row["status"]: row["total"] for row in conn.execute(
                "SELECT status,COUNT(*) AS total FROM outbox "
                "WHERE account_id=? AND thread_id=? GROUP BY status",
                (account, group),
            )
        }
        counts = {name: counts.get(name, 0) for name in (
            "queued", "sending", "needs_review", "sent", "failed", "cancelled",
        )}
        foreign = conn.execute(
            "SELECT COUNT(*) FROM outbox "
            "WHERE account_id<>? OR thread_id<>?",
            (account, group),
        ).fetchone()[0]
        queued_count = counts["queued"]
        unresolved_count = counts["sending"] + counts["needs_review"]
        sample = []
        for idx, row in enumerate(conn.execute(
            "SELECT * FROM outbox "
            "WHERE account_id=? AND thread_id=? AND status='queued' "
            "ORDER BY created_at DESC, outbox_id DESC LIMIT ?",
            (account, group, sample_limit),
        ), start=1):
            reason = dispatch_block_reason(conn, row, config, current)
            sample.append({
                "ordinal": idx,
                "source": (
                    "ai_reply" if row["decision_id"] is not None
                    else "scheduled" if row["scheduled_task_id"] is not None
                    else "unknown"
                ),
                "policy_result": "POLICY_PASS_ONLY" if reason is None else "BLOCKED",
                "reason": reason or "none",
            })
        conn.execute("ROLLBACK")  # end read transaction, no writes
    except DryRunError:
        raise
    except sqlite3.Error as exc:
        raise DryRunError("DATABASE_QUERY_FAILED") from exc
    finally:
        conn.close()

    if unresolved_count:
        verdict = "UNRESOLVED_BLOCK"
    elif not queued_count:
        verdict = "NO_CANDIDATES"
    elif blockers or any(item["reason"] != "none" for item in sample):
        verdict = "BLOCKED"
    elif queued_count > len(sample):
        verdict = "INCOMPLETE_SAMPLE"
    else:
        verdict = "POLICY_PASS_ONLY"
    report = {
        "stage": "P6.5D2D-1",
        "mode": "READ_ONLY_DIAGNOSTIC",
        "verdict": verdict,
        "live_send_authorized": False,
        "network_requests": 0,
        "sql_writes": 0,
        "token_read": False,
        "service_changes": 0,
        "evaluated_at_utc": current.isoformat(),
        "config_blockers": blockers,
        "target_outbox_status_counts": counts,
        "other_account_or_group_outbox_count": foreign,
        "unresolved_target_count": unresolved_count,
        "queued_target_count": queued_count,
        "sample_limit": sample_limit,
        "sample_truncated": queued_count > len(sample),
        "queued_sample": sample,
    }
    if automatic_policy_path is not None:
        try:
            policy = read_automatic_reply_policy(automatic_policy_path)
            policy_status = "ACTIVE_WINDOW" if policy.permits(config, current) else "INACTIVE_OR_TARGET_MISMATCH"
        except BoundaryFailure as exc:
            policy_status = str(exc)
        report["automatic_reply"] = {
            "database_schema": version,
            "automatic_schema_available": version != 4,
            "policy_status": policy_status,
            "unresolved_send_block": bool(unresolved_count),
        }
    return report


def observe_worker_service(*, isolated: bool = False) -> dict:
    """Read systemd properties, never call start/stop/reload/enable."""
    manager = "--system" if isolated else "--user"
    service = "wechat-bot-observer-worker.service" if isolated else "wechat-bot-worker.service"
    try:
        result = subprocess.run(
            ["systemctl", manager, "show", service, "--property=LoadState", "--property=MainPID",
             "--property=ActiveState", "--property=UnitFileState"],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            timeout=3, text=True, check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return {"active": "unknown", "unit_file_state": "unknown",
                "expected_safe_state": False}
    props = {}
    if result.returncode == 0:
        for line in result.stdout.splitlines():
            key, sep, value = line.partition("=")
            if sep:
                props[key] = value
    active = props.get("ActiveState", "unknown")
    unit = props.get("UnitFileState", "unknown")
    if active not in {"inactive", "active", "failed", "unknown"}:
        active = "unknown"
    if unit not in {"disabled", "enabled", "static", "masked", "unknown"}:
        unit = "unknown"
    expected_units = {"static", "disabled"} if isolated else {"disabled"}
    return {"active": active, "unit_file_state": unit, "expected_safe_state": (
        active == "inactive" and unit in expected_units
        and props.get("LoadState") == "loaded" and props.get("MainPID") == "0"
    )}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Read-only Sender policy diagnostic")
    parser.add_argument(
        "--config", type=Path,
        default=Path.home() / "apps/wechat-ai-bot/config.json",
    )
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument("--automatic-policy", type=Path, help="Check a protected automatic reply policy")
    parser.add_argument("--isolated-worker", action="store_true", help="Inspect the isolated system Worker unit")
    args = parser.parse_args(argv)
    try:
        report = inspect_snapshot(args.config, sample_limit=args.limit, automatic_policy_path=args.automatic_policy)
    except (DryRunError, ValueError) as exc:
        code = exc.args[0] if isinstance(exc, DryRunError) else "INVALID_TIMESTAMP"
        print(json.dumps({
            "stage": "P6.5D2D-1", "mode": "READ_ONLY_DIAGNOSTIC",
            "verdict": "ERROR", "error_code": code,
            "live_send_authorized": False,
        }, ensure_ascii=False, indent=2))
        return 2
    worker = observe_worker_service(isolated=args.isolated_worker)
    report["worker"] = worker
    if not worker["expected_safe_state"] and report["verdict"] == "POLICY_PASS_ONLY":
        report["verdict"] = "WORKER_STATE_UNEXPECTED"
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
