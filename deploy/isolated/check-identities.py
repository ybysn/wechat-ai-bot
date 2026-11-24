#!/usr/bin/python3
"""Verify staged service UIDs and SQLite locking with disposable synthetic data."""

from contextlib import contextmanager
import grp
import os
from pathlib import Path
import pwd
import selectors
import subprocess
import sys
import tempfile
import tomllib


DATA = Path("/var/lib/wechat-ai-bot")
CODE = Path("/opt/wechat-ai-bot/src/wechat_ai_bot/consumer.py")
OBS, SND, SHARED = "wechat-observer", "wechat-sender", "wechat-store"


def require(condition, code):
    if not condition:
        raise RuntimeError(code)


def as_user(user, *args):
    return ["/usr/sbin/runuser", "-u", user, "--", *args]


def python_as(user, code, *args):
    return as_user(user, "/usr/bin/python3", "-I", "-c", code, *map(str, args))


def command(args, error):
    result = subprocess.run(args, capture_output=True, text=True, timeout=15, check=False)
    require(result.returncode == 0, error)
    return result.stdout.strip()


def native_owner_token(operator):
    args = as_user(
        operator.pw_name, "/usr/bin/env", f"XDG_RUNTIME_DIR=/run/user/{operator.pw_uid}",
        f"DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/{operator.pw_uid}/bus",
        "/usr/bin/systemctl", "--user", "show", "agent-wechat-native.service", "--property=MainPID", "--value",
    )
    raw = command(args, "NATIVE_PROCESS_UNAVAILABLE")
    require(raw.isdigit() and int(raw) > 0, "NATIVE_PROCESS_UNAVAILABLE")
    process = Path("/proc") / raw
    require(process.stat().st_uid == operator.pw_uid, "NATIVE_PROCESS_OWNER_MISMATCH")
    argv = (process / "cmdline").read_bytes().rstrip(b"\0").split(b"\0")
    data_dir = None
    for index, arg in enumerate(argv):
        if arg == b"--data-dir" and index + 1 < len(argv):
            data_dir = Path(os.fsdecode(argv[index + 1]))
            break
        if arg.startswith(b"--data-dir="):
            data_dir = Path(os.fsdecode(arg.split(b"=", 1)[1]))
            break
    if data_dir is None:
        config = None
        for index, arg in enumerate(argv):
            if arg == b"--server-config" and index + 1 < len(argv):
                config = Path(os.fsdecode(argv[index + 1]))
                break
            if arg.startswith(b"--server-config="):
                config = Path(os.fsdecode(arg.split(b"=", 1)[1]))
                break
        require(config is not None and config.is_absolute(), "NATIVE_CONFIG_NOT_PINNED")
        with config.open("rb") as stream:
            value = tomllib.load(stream)

        def find_dirs(obj):
            for key, item in obj.items():
                if isinstance(item, dict):
                    yield from find_dirs(item)
                elif key in {"data_dir", "dataDir", "data-dir"} and isinstance(item, str):
                    yield item

        found = list(find_dirs(value))
        require(len(found) == 1, "NATIVE_DATA_DIRECTORY_AMBIGUOUS")
        data_dir = Path(found[0])
    require(data_dir.is_absolute(), "NATIVE_DATA_DIRECTORY_NOT_ABSOLUTE")
    path = data_dir / "api-token"
    require(path.is_file(), "NATIVE_OWNER_TOKEN_MISSING")
    return path


_ACCESS = """
import os, sys
code, store, own, peer, token, old_sender = sys.argv[1:]
assert os.geteuid() != 0
assert os.access(code, os.R_OK) and not os.access(code, os.W_OK)
assert not os.access(os.path.dirname(code), os.W_OK)
assert os.access(store, os.R_OK | os.W_OK | os.X_OK)
assert os.access(own, os.R_OK | os.W_OK | os.X_OK)
assert not any(os.access(peer, bit) for bit in (os.R_OK, os.W_OK, os.X_OK))
assert not os.access(token, os.R_OK)
assert not os.access(old_sender, os.R_OK)
"""

_HOLD_WRITE = """
from contextlib import closing
import os, sqlite3, stat, sys
from pathlib import Path
db, mode, gid = sys.argv[1:]
os.umask(0o007)
# The staged production copy is explicitly installed as 0660. SQLite's
# compiled default creation mode may omit group write even with this umask.
os.close(os.open(db, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o660))
with closing(sqlite3.connect(db, timeout=2, isolation_level=None)) as conn:
    assert conn.execute(f'PRAGMA journal_mode={mode}').fetchone()[0] == mode
    conn.execute('CREATE TABLE fixture (role TEXT PRIMARY KEY)')
    conn.execute('BEGIN IMMEDIATE')
    conn.execute("INSERT INTO fixture VALUES ('observer')")
    for suffix in (('-wal', '-shm') if mode == 'wal' else ('-journal',)):
        sidecar = Path(db + suffix).stat()
        assert sidecar.st_gid == int(gid) and stat.S_IMODE(sidecar.st_mode) == 0o660
    print('READY', flush=True)
    assert sys.stdin.readline().strip() == 'commit'
    conn.commit()
"""

_EXPECT_WRITE_LOCK = """
from contextlib import closing
import sqlite3, sys
with closing(sqlite3.connect(sys.argv[1], isolation_level=None, timeout=0.15)) as conn:
    try:
        conn.execute('BEGIN IMMEDIATE')
    except sqlite3.OperationalError as exc:
        assert exc.sqlite_errorcode == sqlite3.SQLITE_BUSY
    else:
        raise AssertionError('second writer was admitted')
"""

_WRITE_SECOND = """
from contextlib import closing
import os, sqlite3, sys
os.umask(0o007)
with closing(sqlite3.connect(sys.argv[1], isolation_level=None, timeout=2)) as conn:
    conn.execute('BEGIN IMMEDIATE')
    conn.execute("INSERT INTO fixture VALUES ('sender')")
    conn.commit()
"""

_READ_BOTH = """
from contextlib import closing
import sqlite3, sys
from pathlib import Path
with closing(sqlite3.connect(Path(sys.argv[1]).as_uri() + '?mode=ro', uri=True)) as conn:
    assert conn.execute('SELECT role FROM fixture ORDER BY role').fetchall() == [('observer',), ('sender',)]
    assert conn.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
"""


@contextmanager
def held_write(db, mode, group_id):
    args = python_as(OBS, _HOLD_WRITE, db, mode, group_id)
    with subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, text=True) as process:
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout, selectors.EVENT_READ)
                require(selector.select(10) and process.stdout.readline().strip() == "READY", "OBSERVER_WRITE_SETUP_FAILED")
            yield
            process.communicate("commit\n", timeout=10)
            require(process.returncode == 0, "OBSERVER_WRITE_COMMIT_FAILED")
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate(timeout=5)


def check_sqlite():
    group_id = grp.getgrnam(SHARED).gr_gid
    owner = pwd.getpwnam(OBS)
    with tempfile.TemporaryDirectory(prefix="identity-probe-", dir=DATA) as temp:
        base = Path(temp)
        base.chmod(0o755)
        store = base / "store"
        store.mkdir(mode=0o700)
        os.chown(store, owner.pw_uid, group_id, follow_symlinks=False)
        store.chmod(0o2770)
        for mode in ("delete", "wal"):
            db = store / f"{mode}.sqlite3"
            with held_write(db, mode, group_id):
                command(python_as(SND, _EXPECT_WRITE_LOCK, db), "SQLITE_LOCK_CHECK_FAILED")
            command(python_as(SND, _WRITE_SECOND, db), "SENDER_WRITE_FAILED")
            command(python_as(OBS, _READ_BOTH, db), "SQLITE_INTEGRITY_CHECK_FAILED")
            print(f"SQLite {mode}: shared sidecar permissions, writer lock and both commits verified")


def main():
    try:
        require(os.geteuid() == 0, "INTERACTIVE_ADMIN_AUTHENTICATION_REQUIRED")
        sudo_uid = os.environ.get("SUDO_UID", "")
        require(sudo_uid.isdigit() and int(sudo_uid) > 0, "ORIGINAL_OPERATOR_UID_REQUIRED")
        operator = pwd.getpwuid(int(sudo_uid))
        owner_token = native_owner_token(operator)
        old_sender = Path(operator.pw_dir) / ".config/wechat-ai-bot/sender-api-token"
        require(old_sender.is_file(), "LEGACY_SENDER_TOKEN_MISSING")
        for user, own, peer in ((OBS, "observer", "sender"), (SND, "sender", "observer")):
            args = python_as(user, _ACCESS, CODE, DATA / "store", DATA / own, DATA / peer, owner_token, old_sender)
            command(args, "SERVICE_UID_ACCESS_CHECK_FAILED")
        print("Both service UIDs: protected code, private directories and legacy credential isolation verified")
        check_sqlite()
    except (OSError, RuntimeError, ValueError, KeyError, subprocess.SubprocessError) as exc:
        # Log fixed failure codes only. OS errors can contain private paths.
        print(exc if type(exc) is RuntimeError else type(exc).__name__, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
