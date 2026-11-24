#!/usr/bin/python3
"""Prepare new observation files from a stopped legacy installation."""

import argparse
from contextlib import ExitStack, closing
import fcntl
import grp
import json
import os
from pathlib import Path
import pwd
import shutil
import sqlite3
import stat
import subprocess
import sys
import tempfile


INSTALL_ROOT = Path("/opt/wechat-ai-bot")
CONFIG = Path("/etc/wechat-ai-bot/observer.json")
DATA = Path("/var/lib/wechat-ai-bot")
SYSTEMCTL = "/usr/bin/systemctl"


class PreparationError(RuntimeError):
    pass


def require(condition, message):
    if not condition:
        raise PreparationError(message)


def private_file(path, owner):
    info = path.lstat()
    require(path.is_absolute() and path.resolve(strict=True) == path
            and stat.S_ISREG(info.st_mode) and info.st_uid == owner
            and info.st_nlink == 1 and not info.st_mode & 0o077,
            f"Unsafe legacy file: {path.name}")


def source_layout(path):
    owner = path.lstat().st_uid
    require(owner != 0 and os.geteuid() in (0, owner), "Unexpected legacy owner")
    private_file(path, owner)
    config = json.loads(path.read_text(encoding="utf-8"))
    require(config["schema_version"] == 2 and config["app"]["auto_send"] is False
            and config["sender"]["enabled"] is False and config["runtime"]["global_pause"] is True
            and config["llm"]["provider"] == "deepseek"
            and "shared_group_id" not in config["storage"]
            and "service_state_directory" not in config["storage"],
            "Expected the legacy observe-only DeepSeek configuration")

    def source(value):
        if value == "~" or value.startswith("~/"):
            home = Path(pwd.getpwuid(owner).pw_dir)
            return home if value == "~" else home / value[2:]
        return path.parent / Path(value).expanduser()

    wx = config["agent_wechat"]
    deepseek = config["llm"]["deepseek"]
    database = source(config["storage"]["database"])
    artifacts = {
        "worker-state.json": database.with_name("worker-state.json"),
        "native-read-token": source(wx["token_file"]),
        "deepseek-api-key": source(deepseek["api_key_file"]),
        "deepseek-usage.jsonl": source(deepseek["usage_log_file"]),
    }
    sender_token = source(config["sender"]["token_file"]).resolve()
    require(len({sender_token, database, *artifacts.values()}) == len(artifacts) + 2,
            "Legacy credentials and state must use distinct paths")
    for source_path in (database, *artifacts.values(), database.with_name("consumer.lock"),
                        database.with_name("worker.lock")):
        private_file(source_path, owner)
    return config, owner, database, artifacts


def check_units_stopped(owner):
    user = pwd.getpwuid(owner).pw_name
    runtime_dir = f"/run/user/{owner}"
    legacy = ["/usr/sbin/runuser", "-u", user, "--", "/usr/bin/env",
              f"XDG_RUNTIME_DIR={runtime_dir}", f"DBUS_SESSION_BUS_ADDRESS=unix:path={runtime_dir}/bus",
              SYSTEMCTL, "--user"]
    units = (
        (legacy, "wechat-bot-consumer.service", "disabled"),
        (legacy, "wechat-bot-worker.service", "disabled"),
        ([SYSTEMCTL], "wechat-bot-observer-consumer.service", "static"),
        ([SYSTEMCTL], "wechat-bot-observer-worker.service", "static"),
        ([SYSTEMCTL], "wechat-bot-sender-preflight.service", "static"),
    )
    for command, unit, expected in units:
        result = subprocess.run(
            [*command, "show", unit, "-p", "LoadState", "-p", "ActiveState",
             "-p", "UnitFileState", "-p", "MainPID"],
            check=True, capture_output=True, text=True, timeout=10,
        )
        properties = dict(line.split("=", 1) for line in result.stdout.splitlines())
        require(properties == {"LoadState": "loaded", "ActiveState": "inactive",
                               "UnitFileState": expected, "MainPID": "0"},
                f"Expected stopped {expected} unit: {unit}")


def check_directory(path, uid, gid, mode):
    info = path.lstat()
    require(path.resolve(strict=True) == path and stat.S_ISDIR(info.st_mode)
            and (info.st_uid, info.st_gid, stat.S_IMODE(info.st_mode)) == (uid, gid, mode),
            f"Unexpected staging directory: {path}")


def database_checkpoint(conn, config):
    from wechat_ai_bot.migrations import automatic_schema_ready
    from wechat_ai_bot.storage import Storage

    require(automatic_schema_ready(conn), "Expected a complete v5 database")
    wx = config["agent_wechat"]
    cursors = conn.execute(
        "SELECT last_durable_cursor, last_acked_cursor FROM consumer_state "
        "WHERE consumer_name=? AND account_id=?", (wx["consumer_name"], wx["account_id"]),
    ).fetchone()
    require(cursors is not None, "Existing Consumer checkpoint missing")
    durable = Storage._cursor_number(cursors[0])
    if cursors[1] is not None:
        require(Storage._cursor_number(cursors[1]) <= durable, "ACK is ahead of durable Cursor")
    return cursors


def copy_snapshot(source, target, config, state):
    with closing(sqlite3.connect(source.as_uri() + "?mode=ro", uri=True, timeout=5)) as original, \
            closing(sqlite3.connect(target, timeout=5)) as snapshot:
        original.execute("BEGIN")
        cursors = database_checkpoint(original, config)
        require(int(state["activation_cursor"]) <= int(cursors[0]),
                "Worker activation is ahead of durable Cursor")
        original.backup(snapshot)
        require(snapshot.execute("PRAGMA journal_mode=DELETE").fetchone() == ("delete",)
                and snapshot.execute("PRAGMA integrity_check").fetchall() == [("ok",)]
                and snapshot.execute("PRAGMA foreign_key_check").fetchone() is None
                and database_checkpoint(snapshot, config) == cursors,
                "SQLite snapshot verification failed")


def publish(target, uid, gid, mode, write, installed):
    # Keep files private until complete and publish without replacing any existing target.
    fd, name = tempfile.mkstemp(dir=target.parent, prefix=f".{target.name}.")
    temporary = Path(name)
    try:
        with os.fdopen(fd, "wb") as stream:
            write(temporary, stream)
            stream.flush()
            os.fchown(stream.fileno(), uid, gid)
            os.fchmod(stream.fileno(), mode)
            os.fsync(stream.fileno())
        os.link(temporary, target)
        installed.append(target)
    finally:
        temporary.unlink()
    fd = os.open(target.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def copy_file(source, stream):
    with source.open("rb") as original:
        shutil.copyfileobj(original, stream)


def prepare(config, source_db, artifacts, uid, gid, shared_gid):
    from wechat_ai_bot.worker import _read_state

    private = DATA / "observer"
    store = DATA / "store"
    wx = config["agent_wechat"]
    state = _read_state(artifacts["worker-state.json"], wx["account_id"], wx["target_group_id"])
    require(state is not None and state["phase"] == "active", "Expected active Worker activation state")
    require(not any(private.iterdir()) and not any(store.iterdir()) and not os.path.lexists(CONFIG),
            "Staging is not empty; inspect the existing files before proceeding")

    target_db = store / "bot.sqlite3"
    installed = []
    try:
        publish(target_db, uid, shared_gid, 0o660,
                lambda path, _: copy_snapshot(source_db, path, config, state), installed)
        for name, source in artifacts.items():
            publish(private / name, uid, gid, 0o600,
                    lambda _, stream: copy_file(source, stream), installed)

        config["storage"] = {"database": str(target_db), "shared_group_id": shared_gid,
                             "service_state_directory": str(private)}
        wx["token_file"] = str(private / "native-read-token")
        config["llm"]["deepseek"].update(api_key_file=str(private / "deepseek-api-key"),
                                         usage_log_file=str(private / "deepseek-usage.jsonl"))
        config["sender"].pop("token_file")
        payload = (json.dumps(config, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
        publish(CONFIG, 0, shared_gid, 0o640, lambda _, stream: stream.write(payload), installed)
    except BaseException:
        for path in reversed(installed):
            path.unlink()
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true", help="Check legacy configuration and file metadata")
    mode.add_argument("--prepare", action="store_true", help="Copy files after independently stopping legacy writers")
    parser.add_argument("--source-config", type=Path, required=True)
    args = parser.parse_args()
    try:
        config, owner, database, artifacts = source_layout(args.source_config.absolute())
        if args.check:
            print("Legacy observation configuration and source file metadata verified")
            return 0

        require(os.geteuid() == 0 and sys.flags.isolated, "Preparation requires root and python3 -I")
        for path in (INSTALL_ROOT.parent, INSTALL_ROOT, INSTALL_ROOT / "src",
                     INSTALL_ROOT / "src/wechat_ai_bot", CONFIG.parent, DATA):
            check_directory(path, 0, 0, 0o755)
        sys.path.insert(0, str(INSTALL_ROOT / "src"))
        observer = pwd.getpwnam("wechat-observer")
        shared = grp.getgrnam("wechat-store")
        check_directory(DATA / "observer", observer.pw_uid, observer.pw_gid, 0o700)
        check_directory(DATA / "store", observer.pw_uid, shared.gr_gid, 0o2770)
        check_units_stopped(owner)
        with ExitStack() as locks:
            for name in ("consumer.lock", "worker.lock"):
                fd = os.open(database.with_name(name), os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
                stream = locks.enter_context(os.fdopen(fd, "rb"))
                try:
                    fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError as exc:
                    raise PreparationError(f"A writer still holds {name}") from exc
            check_units_stopped(owner)
            prepare(config, database, artifacts, observer.pw_uid, observer.pw_gid, shared.gr_gid)
        print("Prepared v5 snapshot, Consumer checkpoint, Worker state and private credentials")
        return 0
    except PreparationError as exc:
        print(str(exc), file=sys.stderr)
    except (OSError, sqlite3.Error, KeyError, ValueError, TypeError, RuntimeError, subprocess.SubprocessError) as exc:
        print(f"Observer preparation failed ({type(exc).__name__}); inspect staging before retrying", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
