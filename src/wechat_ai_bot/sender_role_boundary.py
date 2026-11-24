"""P6.5D2D-2A: offline Sender/observer configuration boundary validation.

This is a *contract*, not a live-sending authorization mechanism. It never
opens a database, reads tokens, connects to a socket, or changes services.
Both the existing observe-only mode and all Sender activation attempts fail
closed. The existing outbound_safety/Storage gates remain unchanged.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import stat


class BoundaryFailure(ValueError):
    """An error with a fixed, non-sensitive error code."""


class _DuplicateKey(ValueError):
    pass


def _no_duplicate_keys(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise _DuplicateKey()
        value[key] = item
    return value


def _read_json_object(path: Path, *, max_bytes: int, private: bool) -> dict:
    """Bounded fail-closed read; never log filenames or JSON contents."""
    try:
        if path.is_symlink():
            raise BoundaryFailure("SYMLINK_FORBIDDEN")
        entry = path.stat()
        if not stat.S_ISREG(entry.st_mode):
            raise BoundaryFailure("NOT_REGULAR_FILE")
        if entry.st_size > max_bytes:
            raise BoundaryFailure("CONFIG_TOO_LARGE")
        if private and (entry.st_uid != os.getuid() or stat.S_IMODE(entry.st_mode) & 0o077):
            raise BoundaryFailure("SENDER_POLICY_PERMISSIONS_INVALID")
        contents = path.read_bytes()
        if len(contents) > max_bytes:
            raise BoundaryFailure("CONFIG_TOO_LARGE")
        obj = json.loads(contents.decode("utf-8"), object_pairs_hook=_no_duplicate_keys)
    except BoundaryFailure:
        raise
    except (OSError, UnicodeDecodeError, ValueError, _DuplicateKey) as exc:
        raise BoundaryFailure("CONFIG_UNREADABLE_OR_INVALID") from exc
    if not isinstance(obj, dict):
        raise BoundaryFailure("CONFIG_NOT_OBJECT")
    return obj


def _observe_target(config: dict) -> tuple[str, str]:
    """Consumer/Worker must keep their original observe-only safety mode."""
    try:
        wx = config["agent_wechat"]
        safety = config["safety"]
        safe = (
            type(config["schema_version"]) is int and config["schema_version"] == 2
            and config["app"]["auto_send"] is False
            and config["sender"]["enabled"] is False
            and config["runtime"]["global_pause"] is True
        )
        account, group = wx["account_id"], wx["target_group_id"]
        allowlist = safety["group_allowlist"]
    except (KeyError, TypeError) as exc:
        raise BoundaryFailure("OBSERVE_CONFIG_INVALID") from exc
    if not safe:
        raise BoundaryFailure("OBSERVE_MODE_NOT_SAFE")
    if not (
        isinstance(account, str) and bool(account.strip())
        and isinstance(group, str) and bool(group.strip())
        and isinstance(allowlist, list)
        and all(isinstance(x, str) for x in allowlist)
        and group in allowlist
    ):
        raise BoundaryFailure("OBSERVE_TARGET_INVALID")
    return account, group


def inspect_boundary(
    observe_config: Path | str,
    sender_policy: Path | str,
) -> dict:
    """Inspect configuration separation, never return permission to dispatch.

    A Sender policy file is *not* mandatory in this stage. If present, it
    must be private, match the existing allowlisted target, and remain disabled.
    There is intentionally no possible successful live-send outcome.
    """
    observe = Path(observe_config).expanduser().absolute()
    sender = Path(sender_policy).expanduser().absolute()
    if observe == sender or observe.resolve() == sender.resolve():
        raise BoundaryFailure("CONFIG_PATHS_MUST_DIFFER")

    account, group = _observe_target(
        _read_json_object(observe, max_bytes=262144, private=False)
    )

    if not sender.exists() and not sender.is_symlink():
        return {
            "stage": "P6.5D2D-2A",
            "verdict": "SENDER_POLICY_ABSENT",
            "observe_mode_safe": True,
            "sender_policy_present": False,
            "live_send_authorized": False,
            "network_requests": 0,
            "sql_writes": 0,
            "token_read": False,
            "service_changes": 0,
        }

    policy = _read_json_object(sender, max_bytes=8192, private=True)
    if set(policy) != {
        "schema_version", "role", "enabled", "account_id", "thread_id"
    } or type(policy.get("schema_version")) is not int or policy["schema_version"] != 1 \
            or policy["role"] != "sender" or type(policy["enabled"]) is not bool:
        raise BoundaryFailure("SENDER_POLICY_SCHEMA_INVALID")
    if policy["account_id"] != account or policy["thread_id"] != group:
        raise BoundaryFailure("SENDER_POLICY_TARGET_MISMATCH")
    if policy["enabled"] is not False:
        raise BoundaryFailure("SENDER_ENABLEMENT_UNSUPPORTED")
    return {
        "stage": "P6.5D2D-2A",
        "verdict": "SENDER_POLICY_DISABLED",
        "observe_mode_safe": True,
        "sender_policy_present": True,
        "live_send_authorized": False,
        "network_requests": 0,
        "sql_writes": 0,
        "token_read": False,
        "service_changes": 0,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Offline observer/Sender boundary check; never authorizes sends"
    )
    parser.add_argument(
        "--config", type=Path,
        default=Path.home() / "apps/wechat-ai-bot/config.json",
    )
    parser.add_argument(
        "--sender-policy", type=Path,
        default=Path.home() / ".config/wechat-ai-bot/sender-runtime.json",
    )
    args = parser.parse_args(argv)
    try:
        report = inspect_boundary(args.config, args.sender_policy)
    except BoundaryFailure as exc:
        report = {
            "stage": "P6.5D2D-2A", "verdict": "BLOCKED",
            "error_code": str(exc), "live_send_authorized": False,
            "network_requests": 0, "sql_writes": 0,
            "token_read": False, "service_changes": 0,
        }
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 2
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
