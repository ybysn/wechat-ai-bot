"""Install and enable the isolated Sender with a specifically reviewed draft."""

from contextlib import closing
from hashlib import sha256
import grp
import json
import os
from pathlib import Path
import pwd
import re
import sqlite3
import stat
import subprocess
import sys
from tempfile import NamedTemporaryFile
from time import monotonic, sleep, time
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener


CONFIG = Path("/etc/wechat-ai-bot")
PACKAGE = Path("/opt/wechat-ai-bot/src/wechat_ai_bot")
STATE = Path("/var/lib/wechat-ai-bot/sender")
UNIT = "wechat-bot-sender.service"
SYSTEMD = Path("/etc/systemd/system")
ORIGIN = "http://127.0.0.1:6174/v1"
TOKEN_FILE = CONFIG / "sender-api-token"
POLICY_FILE = CONFIG / "automatic-reply.json"
SETTINGS_FILE = CONFIG / "sender-settings.json"
LOCK_FILE = CONFIG / "automatic-reply.json.dispatch.lock"
MODULES = ("scheduled_plans.py", "sender_schedule.py", "sender_service.py")
OBSERVER = "wechat-bot-observer"


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args):
        return None


def require(condition, reason):
    if not condition:
        raise RuntimeError(reason)


def read_file(path, uid, mode, limit):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as stream:
        info = os.fstat(stream.fileno())
        require(stat.S_ISREG(info.st_mode) and info.st_uid == uid and info.st_nlink == 1
                and stat.S_IMODE(info.st_mode) == mode, "UNEXPECTED_FILE_PERMISSIONS")
        data = stream.read(limit + 1)
    require(len(data) <= limit, "UNEXPECTED_FILE_SIZE")
    return data


def write_file(path, data, gid=0, mode=0o644):
    with NamedTemporaryFile(dir=path.parent, delete=False) as stream:
        temporary = Path(stream.name)
        try:
            os.fchown(stream.fileno(), 0, gid)
            os.fchmod(stream.fileno(), mode)
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
            os.replace(temporary, path)
            fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        finally:
            temporary.unlink(missing_ok=True)


def encode(value):
    return json.dumps(value, ensure_ascii=False).encode("utf-8")


def native(opener, token, path, method="GET", payload=None):
    req = Request(ORIGIN + path, method=method, data=encode(payload) if payload is not None else None,
                  headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"})
    with opener.open(req, timeout=5) as response:
        require(response.geturl() == req.full_url, "NATIVE_RESPONSE_ORIGIN_MISMATCH")
        if response.code == 204:
            return {}
        body = response.read(16385)
    require(len(body) <= 16384, "NATIVE_RESPONSE_TOO_LARGE")
    return json.loads(body)


def systemctl(*arguments):
    subprocess.run(["/usr/bin/systemctl", "--system", *arguments], check=True, timeout=25)


def checkpoint(config, account):
    db = Path(config["storage"]["database"])
    require(db.parent == Path("/var/lib/wechat-ai-bot/store") and db.resolve(strict=True) == db,
            "OBSERVER_DATABASE_PATH_MISMATCH")
    with closing(sqlite3.connect(db.as_uri() + "?mode=ro", uri=True, timeout=4)) as conn:
        row = conn.execute(
            "SELECT last_durable_cursor,last_acked_cursor FROM consumer_state WHERE consumer_name=? AND account_id=?",
            (config["agent_wechat"]["consumer_name"], account),
        ).fetchone()
    require(row is not None and row[0] == row[1] and re.fullmatch(r"[0-9]{1,19}", row[0] or "")
            and int(row[0]) <= 2**63 - 1, "OBSERVER_CHECKPOINT_NOT_READY")
    return int(row[0])


def verify_paused_process():
    deadline = monotonic() + 12
    while monotonic() < deadline:
        invocation = subprocess.check_output(
            ["/usr/bin/systemctl", "show", "-p", "InvocationID", "--value", UNIT],
            text=True, timeout=3,
        ).strip()
        if re.fullmatch(r"[0-9a-f]{32}", invocation) and invocation.strip("0"):
            result = subprocess.run(
                ["/usr/bin/journalctl", f"_SYSTEMD_INVOCATION_ID={invocation}",
                 "--grep=^sender_paused$", "-n", "1", "-o", "cat", "--no-pager"],
                text=True, timeout=3, capture_output=True, check=False,
            )
            if result.returncode == 0 and result.stdout.strip() == "sender_paused":
                return
        sleep(.25)
    raise RuntimeError("SENDER_PAUSED_STARTUP_NOT_CONFIRMED")


def install(bundle, expected_digest):
    require(os.geteuid() == 0 and sys.flags.isolated, "ADMINISTRATOR_REQUIRED")
    operator = pwd.getpwuid(int(os.environ["SUDO_UID"]))
    require(operator.pw_uid != 0, "OPERATOR_IDENTITY_REQUIRED")
    for directory in (PACKAGE, SYSTEMD, CONFIG):
        info = directory.lstat()
        require(stat.S_ISDIR(info.st_mode) and info.st_uid == 0 and not info.st_mode & 0o022,
                "DEPLOYMENT_DIRECTORY_UNSAFE")
    sources = {name: read_file(bundle / name, operator.pw_uid, 0o600, 512 * 1024)
               for name in (*MODULES, UNIT)}
    for name in MODULES:
        compile(sources[name], name, "exec")
    draft_data = read_file(bundle / "draft.json", operator.pw_uid, 0o600, 512 * 1024)
    target_data = read_file(bundle / "target.json", operator.pw_uid, 0o600, 1024)
    live_draft = Path(operator.pw_dir) / ".local/state/wechat-ai-bot/admin/draft.json"
    require(sha256(draft_data + b"\0" + target_data).hexdigest() == expected_digest
            and read_file(live_draft, operator.pw_uid, 0o600, 512 * 1024) == draft_data,
            "REVIEWED_DRAFT_CHANGED")
    config = json.loads(read_file(CONFIG / "observer.json", 0, 0o640, 65536))
    wx = config["agent_wechat"]
    account, target = wx["account_id"], wx["target_group_id"]
    require(json.loads(target_data) == {"account": account, "group": target}, "REVIEWED_TARGET_CHANGED")
    require(wx["base_url"].rstrip("/") == ORIGIN and target in config["safety"]["group_allowlist"]
            and (config["app"]["auto_send"], config["sender"]["enabled"], config["runtime"]["global_pause"])
            == (False, False, True), "PRODUCTION_TARGET_NOT_AUTHORIZED")
    draft = json.loads(draft_data)["settings"]
    require(draft.get("targetGroupId", target) == target, "DRAFT_TARGET_DIFFERS_FROM_PRODUCTION")
    plans = draft.get("scheduledMessages")
    if plans is None:
        plans = [draft["specialMessage"]] if draft.get("specialMessage") else []
    settings = {key: draft[key] for key in (
        "replyWindowEnabled", "replyStart", "replyEnd", "checkinEnabled", "checkinTime", "limits",
    )}
    settings.update(targetGroupId=target, scheduledMessages=plans)
    require(any(plan["enabled"] for plan in plans), "NO_ENABLED_SCHEDULE_IN_REVIEWED_DRAFT")
    sender = pwd.getpwnam("wechat-sender")
    require(stat.S_IMODE(STATE.lstat().st_mode) == 0o700 and STATE.lstat().st_uid == sender.pw_uid,
            "SENDER_STATE_DIRECTORY_UNSAFE")
    for path in (TOKEN_FILE, POLICY_FILE, SETTINGS_FILE, LOCK_FILE, SYSTEMD / UNIT,
                 *(PACKAGE / name for name in MODULES)):
        require(not os.path.lexists(path), "SENDER_INSTALLATION_PATH_ALREADY_USED")
    for role in ("consumer", "worker"):
        systemctl("is-active", "--quiet", f"{OBSERVER}-{role}.service")
    cursor = checkpoint(config, account)
    owner_path = Path(operator.pw_dir) / ".local/share/agent-wechat-isolated/api-token"
    owner_token = read_file(owner_path, operator.pw_uid, 0o600, 512).decode("ascii").strip()
    opener = build_opener(ProxyHandler({}), NoRedirect())
    native_account = native(opener, owner_token, "/accounts/" + quote(account, safe=""))
    require(native_account["id"] == account and native_account["authenticated"] is True
            and native_account["connection"] == "online" and native_account["storageMode"] == "managed",
            "WECHAT_ACCOUNT_NOT_READY")

    created = []
    grant_id = None
    stage_started = False
    activated = False
    try:
        def create(path, data, gid=0, mode=0o644):
            created.append(path)
            write_file(path, data, gid, mode)

        for name in MODULES:
            create(PACKAGE / name, sources[name])
        create(SYSTEMD / UNIT, sources[UNIT])
        create(SETTINGS_FILE, encode(settings), sender.pw_gid, 0o640)
        create(LOCK_FILE, b"", sender.pw_gid, 0o640)
        started = int(time())
        policy = {"schema_version": 1, "role": "automatic_reply", "account_id": account, "thread_id": target,
                  "generation": started, "activation_cursor": cursor, "starts_at": started,
                  "expires_at": 253402300799, "enabled": True, "paused": True}
        create(POLICY_FILE, encode(policy), sender.pw_gid, 0o640)
        issued = native(opener, owner_token, "/tokens", "POST",
                        {"name": "wechat-ai-bot-isolated-sender", "accounts": [account], "scopes": ["read", "send"]})
        grant_id = issued["grant"]["id"]
        require(re.fullmatch(r"[0-9a-f]{64}", issued["token"]), "NATIVE_TOKEN_FORMAT_UNEXPECTED")
        create(TOKEN_FILE, issued["token"].encode("ascii"), mode=0o600)
        systemctl("daemon-reload")
        subprocess.run([
            "/usr/bin/systemd-run", "--quiet", "--collect", "--wait", "--pipe",
            "--property=User=wechat-sender", "--property=Group=wechat-sender",
            "--property=SupplementaryGroups=wechat-store", "--property=NoNewPrivileges=yes",
            "--property=ProtectHome=yes", "--property=ProtectSystem=strict",
            "--property=IPAddressDeny=any", "--property=IPAddressAllow=localhost",
            "--property=LoadCredential=native-token:" + str(TOKEN_FILE),
            "--setenv=PYTHONPATH=/opt/wechat-ai-bot/src", "--setenv=PYTHONDONTWRITEBYTECODE=1",
            "/usr/bin/python3", "-P", "-u", "-m", "wechat_ai_bot.sender_service", "--check",
        ], check=True, timeout=30, stdin=subprocess.DEVNULL)
        stage_started = True
        systemctl("start", UNIT)
        verify_paused_process()
        systemctl("enable", UNIT)
        systemctl("is-enabled", "--quiet", UNIT)
        require(read_file(live_draft, operator.pw_uid, 0o600, 512 * 1024) == draft_data, "REVIEWED_DRAFT_CHANGED")
        policy.update(paused=False, starts_at=int(time()), activation_cursor=checkpoint(config, account))
        activated = True
        write_file(POLICY_FILE, encode(policy), sender.pw_gid, 0o640)
    except BaseException:
        if activated:
            print("Policy publication began. Sender files and records require review.", file=sys.stderr)
        if stage_started:
            systemctl("disable", "--now", UNIT)
        if not activated:
            for path in reversed(created):
                path.unlink(missing_ok=True)
            systemctl("daemon-reload")
            if grant_id is not None:
                native(opener, owner_token, "/tokens/" + quote(grant_id, safe=""), "DELETE")
        raise
    print("SENDER_ENABLED_AND_ACTIVE")


if __name__ == "__main__":
    try:
        require(len(sys.argv) == 3, "EXPECTED_BUNDLE_AND_DRAFT_DIGEST")
        install(Path(sys.argv[1]), sys.argv[2])
    except RuntimeError as exc:
        sys.exit(str(exc))
    except (OSError, ValueError, KeyError, sqlite3.Error, subprocess.SubprocessError, HTTPError) as exc:
        sys.exit(f"SENDER_INSTALL_FAILED_{type(exc).__name__}")
