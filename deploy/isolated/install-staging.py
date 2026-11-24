#!/usr/bin/python3
"""Install the approved empty WX Chat Bot staging layout without starting services."""

import argparse
import grp
from hashlib import sha256
import os
from pathlib import Path
import pwd
import stat
import subprocess
import sys


HERE = Path(__file__).resolve().parent
PACKAGE = HERE.parents[1] / "src" / "wechat_ai_bot"
CODE = Path("/opt/wechat-ai-bot")
CONFIG = Path("/etc/wechat-ai-bot")
DATA = Path("/var/lib/wechat-ai-bot")
SYSTEMD = Path("/etc/systemd/system")
OBS = "wechat-observer"
SND = "wechat-sender"
DB_GROUP = "wechat-store"
NOLOGIN = "/usr/sbin/nologin"
UNITS = (
    "wechat-bot-observer-consumer.service",
    "wechat-bot-observer-worker.service",
    "wechat-bot-sender-preflight.service",
)


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def exists_in(lookup, name):
    try:
        lookup(name)
    except KeyError:
        return False
    return True


def read_input(path):
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK), "rb") as stream:
        info = os.fstat(stream.fileno())
        require(stat.S_ISREG(info.st_mode) and info.st_size <= 1024 * 1024,
                f"Unexpected staging input: {path.name}")
        data = stream.read(1024 * 1024 + 1)
    require(len(data) <= 1024 * 1024, f"Invalid staging input size: {path.name}")
    return data


def package_files():
    modules = sorted(PACKAGE.glob("*.py"))
    require(any(f.name == "__init__.py" for f in modules), "Python package missing")
    files = {}
    for path in modules:
        payload = read_input(path)
        compile(payload, path.name, "exec")
        files[CODE / "src" / "wechat_ai_bot" / path.name] = payload
    for unit in UNITS:
        payload = read_input(HERE / unit)
        require(b"[Service]" in payload and b"[Install]" not in payload, f"Invalid static unit: {unit}")
        files[SYSTEMD / unit] = payload
    return files


def check_installation_parents():
    for path in (CODE.parent, CONFIG.parent, DATA.parent, SYSTEMD):
        info = path.lstat()
        require(stat.S_ISDIR(info.st_mode) and info.st_uid == 0 and not info.st_mode & 0o022,
                f"Untrusted installation parent: {path}")
    for name in ("groupadd", "useradd", "nologin"):
        require(os.access(Path("/usr/sbin") / name, os.X_OK), f"Missing command: {name}")


def require_absent(path):
    require(not path.exists() and not path.is_symlink(), f"Path already exists: {path}")


def precheck(files):
    check_installation_parents()
    for name in (OBS, SND):
        require(not exists_in(pwd.getpwnam, name), f"User already exists: {name}")
    for name in (OBS, SND, DB_GROUP):
        require(not exists_in(grp.getgrnam, name), f"Group already exists: {name}")
    for path in (CODE, CONFIG, DATA, *(SYSTEMD / name for name in UNITS)):
        require_absent(path)
    print(f"Precheck passed: {len(files) - len(UNITS)} Python modules, {len(UNITS)} static units")


def directory_layout():
    obs = pwd.getpwnam(OBS)
    snd = pwd.getpwnam(SND)
    db_gid = grp.getgrnam(DB_GROUP).gr_gid
    return {
        CODE: (0, 0, 0o755), CODE / "src": (0, 0, 0o755),
        CODE / "src" / "wechat_ai_bot": (0, 0, 0o755),
        CONFIG: (0, 0, 0o755), DATA: (0, 0, 0o755),
        DATA / "store": (obs.pw_uid, db_gid, 0o2770),
        DATA / "observer": (obs.pw_uid, obs.pw_gid, 0o700),
        DATA / "sender": (snd.pw_uid, snd.pw_gid, 0o700),
    }


def verify_identities():
    for name in (OBS, SND):
        user = pwd.getpwnam(name)
        require(user.pw_uid != 0 and user.pw_dir == "/nonexistent" and user.pw_shell == NOLOGIN
                and user.pw_gid == grp.getgrnam(name).gr_gid, f"Unexpected user layout: {name}")
    require(pwd.getpwnam(OBS).pw_uid != pwd.getpwnam(SND).pw_uid, "Service UIDs must differ")
    shared = grp.getgrnam(DB_GROUP)
    members = set(shared.gr_mem) | {user.pw_name for user in pwd.getpwall() if user.pw_gid == shared.gr_gid}
    require(members == {OBS, SND}, "Unexpected shared group members")


def verify(files):
    verify_identities()
    for path, expected in directory_layout().items():
        info = path.lstat()
        require(stat.S_ISDIR(info.st_mode) and (info.st_uid, info.st_gid, stat.S_IMODE(info.st_mode)) == expected,
                f"Unexpected directory permissions: {path}")
    for path, payload in files.items():
        info = path.lstat()
        require(stat.S_ISREG(info.st_mode) and (info.st_uid, info.st_gid, stat.S_IMODE(info.st_mode)) == (0, 0, 0o644)
                and sha256(read_input(path)).digest() == sha256(payload).digest(), f"Unexpected installed file: {path}")
    print("Installed identities, directory permissions and file hashes verified")


def check_resume_checkpoint():
    """Recognize the interrupted install before any staged files were copied."""
    check_installation_parents()
    verify_identities()
    info = CODE.lstat()
    require(stat.S_ISDIR(info.st_mode) and (info.st_uid, info.st_gid, stat.S_IMODE(info.st_mode)) == (0, 0, 0o700)
            and next(CODE.iterdir(), None) is None, f"Unexpected interrupted directory: {CODE}")
    for path in (CONFIG, DATA, *(SYSTEMD / name for name in UNITS)):
        require_absent(path)
    print("Resume precheck passed: identities verified and first directory is empty")


def configure_directory(path, uid, gid, mode, *, created=False):
    if not created:
        path.mkdir(mode=0o700)
    os.chown(path, uid, gid, follow_symlinks=False)
    path.chmod(mode)


def install(files, *, resume=False):
    require(os.geteuid() == 0, "Install requires interactive administrator authentication")
    if resume:
        check_resume_checkpoint()
    else:
        precheck(files)
    try:
        if not resume:
            for name in (OBS, SND, DB_GROUP):
                subprocess.run(["/usr/sbin/groupadd", "--system", name], check=True)
            for name in (OBS, SND):
                subprocess.run([
                    "/usr/sbin/useradd", "--system", "--no-create-home", "--home-dir", "/nonexistent",
                    "--shell", NOLOGIN, "--gid", name, "--groups", DB_GROUP, "--password", "!", name,
                ], check=True)
        for path, (uid, gid, mode) in directory_layout().items():
            configure_directory(path, uid, gid, mode, created=resume and path == CODE)
        for path, payload in files.items():
            with path.open("xb") as stream:
                stream.write(payload)
                os.fchown(stream.fileno(), 0, 0)
                os.fchmod(stream.fileno(), 0o644)
        verify(files)
    except (OSError, KeyError, RuntimeError, subprocess.SubprocessError):
        print("Installation stopped. Inspect any partial setup before retrying.", file=sys.stderr)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true", help="Check inputs and confirm paths are unused")
    mode.add_argument("--install", action="store_true", help="Install staging identities, directories and files")
    mode.add_argument("--resume", action="store_true", help="Resume after creating identities and the first empty directory")
    mode.add_argument("--verify", action="store_true", help="Verify an existing staging installation")
    args = parser.parse_args()
    try:
        files = package_files()
        if args.check:
            precheck(files)
        elif args.verify:
            verify(files)
        else:
            install(files, resume=args.resume)
    except (KeyError, OSError, RuntimeError, SyntaxError, subprocess.SubprocessError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
