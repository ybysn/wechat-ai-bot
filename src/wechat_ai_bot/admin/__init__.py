"""Local operator dashboard for the observation deployment."""

from datetime import datetime, timezone
from hashlib import sha256
import hmac
from http import HTTPStatus
from http.client import HTTPException
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import re
import secrets
import sqlite3
import stat
import subprocess
from tempfile import NamedTemporaryFile
from threading import Lock
from time import monotonic
from urllib.error import HTTPError
from urllib.parse import quote, urlencode
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

from wechat_ai_bot.consumer import EXPECTED_AGENT_WECHAT_BASE_URL, FatalConsumerError, _load_config, _resolve_config_path
from wechat_ai_bot.admin.desktop import DesktopActions
from wechat_ai_bot.admin.history import HISTORY_LIMIT, HISTORY_SIZE_LIMIT, read_sent_history
from wechat_ai_bot.scheduled_plans import schedule_preview, validate_limits, validate_message, validate_messages
from wechat_ai_bot.decision_engine import _deepseek_settings, selected_model, selected_provider
from wechat_ai_bot.outbound_safety import GROUP_SEND_COOLDOWN_SECONDS
from wechat_ai_bot.scheduler import CHECKIN_TIMEZONE
from wechat_ai_bot.sender_automatic_policy import read_automatic_reply_policy
from wechat_ai_bot.sender_dry_run import DryRunError, inspect_snapshot
from wechat_ai_bot.sender_role_boundary import BoundaryFailure, _no_duplicate_keys


LOOPBACK = "127.0.0.1"
RUNTIME_CONFIG = Path("/etc/wechat-ai-bot/observer.json")
AUTOMATIC_POLICY = Path("/etc/wechat-ai-bot/automatic-reply.json")
WEB = Path(__file__).with_name("web")
ASSETS = {
    "/": ("index.html", "text/html; charset=utf-8"),
    "/admin.css": ("admin.css", "text/css; charset=utf-8"),
    "/admin.js": ("admin.js", "text/javascript; charset=utf-8"),
}
SERVICES = {
    "native": ("agent-wechat-native.service", True),
    "consumer": ("wechat-bot-observer-consumer.service", False),
    "worker": ("wechat-bot-observer-worker.service", False),
    "sender": ("wechat-bot-sender.service", False),
    "preflight": ("wechat-bot-sender-preflight.service", False),
}
TIME = re.compile(r"(?:[01][0-9]|2[0-3]):[0-5][0-9]\Z")
MODEL = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,79}\Z")
GROUP_ID = re.compile(r"[A-Za-z0-9_-]{1,128}@chatroom\Z")
BODY_LIMIT = 512 * 1024
SENDER_STATUS_CODES = frozenset({
    "sender_paused", "waiting_for_schedule", "native_offline", "scheduled_sent", "reply_sent",
    "scheduled_needs_review", "reply_needs_review", "outbox_unresolved", "no_automatic_candidate",
    "group_send_cooldown", "sender_configuration_unavailable", "sender_database_busy",
})


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def private_read(path: Path, limit: int) -> bytes:
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as stream:
        info = os.fstat(stream.fileno())
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                or info.st_nlink != 1 or info.st_mode & 0o077 or info.st_size > limit):
            raise PermissionError("Unsafe local file permissions")
        value = stream.read(limit + 1)
        if len(value) > limit:
            raise ValueError("Local file is too large")
        return value


def valid_group_id(value):
    return isinstance(value, str) and GROUP_ID.fullmatch(value) is not None


def validate_draft(value):
    base = {
        "provider", "model", "replyWindowEnabled", "replyStart", "replyEnd", "checkinEnabled", "checkinTime",
    }
    optional = {"limits", "specialMessage", "scheduledMessages", "targetGroupId"}
    if not isinstance(value, dict) or not base <= value.keys() <= base | optional:
        raise ValueError("草稿字段不完整。")
    if "targetGroupId" in value and not valid_group_id(value["targetGroupId"]):
        raise ValueError("请选择有效的目标微信群。")
    if value["provider"] not in ("deepseek", "ollama"):
        raise ValueError("请选择支持的模型提供方。")
    model = value["model"]
    if not isinstance(model, str) or not MODEL.fullmatch(model):
        raise ValueError("请输入有效的模型名称。")
    if value["provider"] == "deepseek" and model != "deepseek-flash":
        raise ValueError("当前 DeepSeek 适配器使用 deepseek-flash。")
    if any(type(value[field]) is not bool for field in ("replyWindowEnabled", "checkinEnabled")):
        raise ValueError("开关值无效。")
    if any(not isinstance(value[field], str) or not TIME.fullmatch(value[field])
           for field in ("replyStart", "replyEnd", "checkinTime")):
        raise ValueError("请输入有效的小时和分钟。")
    if value["replyWindowEnabled"] and value["replyStart"] == value["replyEnd"]:
        raise ValueError("回复时段的开始和结束时间必须不同。")
    if "limits" in value:
        validate_limits(value["limits"])
    if "scheduledMessages" in value:
        if "specialMessage" in value:
            raise ValueError("定时消息只能使用一种配置格式。")
        validate_messages(value["scheduledMessages"])
    else:
        validate_message(value.get("specialMessage"))
    return value


def describe_plan(draft):
    settings = draft["settings"]
    if "scheduledMessages" in settings:
        now = datetime.now(timezone.utc)
        return {**draft, "schedulePreviews": [schedule_preview(plan, now) for plan in settings["scheduledMessages"]]}
    message = settings.get("specialMessage")
    return {**draft, "schedulePreview": schedule_preview(message)} if message is not None else draft


class DraftConflict(RuntimeError):
    """The draft changed after the editor loaded it."""


class AdminState:
    def __init__(self, path: Path):
        self.path = path.expanduser().absolute()
        self.path.mkdir(parents=True, mode=0o700, exist_ok=True)
        info = self.path.lstat()
        if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid()
                or stat.S_IMODE(info.st_mode) != 0o700):
            raise PermissionError("Admin state directory must be private (0700)")
        self.key_path = self.path / "access.key"
        self.draft_path = self.path / "draft.json"
        self.draft_lock = Lock()
        try:
            self.key = private_read(self.key_path, 128).decode("ascii").strip()
        except FileNotFoundError:
            key = secrets.token_urlsafe(24)
            fd = os.open(self.key_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            with os.fdopen(fd, "w", encoding="ascii") as stream:
                stream.write(key + "\n")
                stream.flush()
                os.fsync(stream.fileno())
            self.key = key
        if not re.fullmatch(r"[A-Za-z0-9_-]{32}", self.key):
            raise ValueError("Invalid local dashboard access key")

    def _read_saved_draft(self):
        try:
            data = private_read(self.draft_path, BODY_LIMIT)
        except FileNotFoundError:
            return None
        try:
            draft = json.loads(data, object_pairs_hook=_no_duplicate_keys)
        except RecursionError as exc:
            raise ValueError("Invalid local saved configuration") from exc
        if not isinstance(draft, dict) or not isinstance(draft.get("updatedAt"), str):
            raise ValueError("Invalid local saved configuration")
        validate_draft(draft.get("settings"))
        return {**draft, "revision": sha256(data).hexdigest()}

    def read_draft(self):
        draft = self._read_saved_draft()
        return describe_plan(draft) if draft is not None else None

    def sent_history(self, config_path):
        try:
            return {"state": "live", **read_sent_history(config_path)}
        except (OSError, sqlite3.Error, KeyError, TypeError, ValueError, RecursionError):
            pass
        try:
            report = json.loads(private_read(self.path / "sent-history.json", HISTORY_SIZE_LIMIT))
            if (isinstance(report, dict) and isinstance(report.get("checkedAt"), str)
                    and isinstance(report.get("entries"), list) and len(report["entries"]) <= HISTORY_LIMIT
                    and all(isinstance(row, dict) and row.keys() == {"text", "sentAt", "source"}
                            and isinstance(row["text"], str)
                            and (row["sentAt"] is None or isinstance(row["sentAt"], str))
                            and row["source"] in ("reply", "scheduled", "other", "test") for row in report["entries"])):
                return {"checkedAt": report["checkedAt"], "entries": report["entries"],
                        "hasMore": report.get("hasMore") is True, "state": "snapshot"}
        except (OSError, ValueError, RecursionError):
            pass
        return {"state": "unavailable", "entries": []}

    def save_draft(self, value, *, if_match=None):
        draft = {"settings": validate_draft(value), "updatedAt": now_iso()}
        data = json.dumps(draft, ensure_ascii=False).encode("utf-8")
        if len(data) > BODY_LIMIT:
            raise ValueError("草稿大小无效。")
        with self.draft_lock:
            try:
                previous = self._read_saved_draft()
            except (OSError, ValueError) as exc:
                raise OSError("Existing draft could not be read") from exc
            revision = previous["revision"] if previous is not None else ""
            if if_match is not None and if_match != f'"{revision}"':
                raise DraftConflict()
            temporary = None
            try:
                with NamedTemporaryFile(dir=self.path, delete=False) as stream:
                    temporary = Path(stream.name)
                    stream.write(data)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temporary, self.draft_path)
                directory = os.open(self.path, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(directory)
                finally:
                    os.close(directory)
            finally:
                if temporary is not None:
                    temporary.unlink(missing_ok=True)
        return describe_plan({**draft, "revision": sha256(data).hexdigest()})


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def native_account_access(settings, path):
    wx = settings.get("agent_wechat") if settings else None
    if not isinstance(wx, dict):
        return None
    account, base_url = wx.get("account_id"), wx.get("base_url")
    if (not isinstance(base_url, str) or base_url.rstrip("/") != EXPECTED_AGENT_WECHAT_BASE_URL
            or not isinstance(account, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", account)):
        return None
    token_path = _resolve_config_path(wx["token_file"], path.parent)
    token = private_read(token_path, 512).decode("utf-8").strip()
    return account, token


def native_get(opener, account, token, suffix="", *, limit, timeout):
    url = EXPECTED_AGENT_WECHAT_BASE_URL + "/accounts/" + quote(account, safe="") + suffix
    request = Request(url, headers={"Authorization": "Bearer " + token, "Accept": "application/json"})
    with opener.open(request, timeout=timeout) as response:
        body = response.read(limit + 1)
    if len(body) > limit:
        raise ValueError("Native response is too large")
    return json.loads(body)


def native_status(settings, path):
    try:
        access = native_account_access(settings, path)
        if access is None:
            return {"state": "unconfigured"}
        opener = build_opener(ProxyHandler({}), _NoRedirect())
        payload = native_get(opener, *access, limit=16384, timeout=3)
        if not isinstance(payload, dict) or payload.get("id") != access[0]:
            return {"state": "unavailable"}
        return {
            "state": "reachable",
            "connection": payload.get("connection") if payload.get("connection") in (
                "online", "reconnecting", "disconnected",
            ) else "unknown",
            "authenticated": payload.get("authenticated") is True,
            "transport": payload.get("incomingTransport") if payload.get("incomingTransport") in (
                "native_push", "heartbeat_polling", "disconnected",
            ) else "unknown",
        }
    except HTTPError as exc:
        return {"state": "access_denied" if exc.code in (401, 403) else "unavailable"}
    except (OSError, HTTPException, ValueError, TypeError, KeyError, AttributeError, RecursionError):
        return {"state": "unavailable"}


def native_groups(settings, path):
    groups = {}
    state = "unavailable"
    try:
        access = native_account_access(settings, path)
        if access is None:
            return {"state": "unconfigured", "groups": []}
        opener = build_opener(ProxyHandler({}), _NoRedirect())
        cursor = None
        deadline = monotonic() + 7
        for _ in range(5):
            remaining = deadline - monotonic()
            if remaining <= 0:
                break
            query = {"limit": 100}
            if cursor is not None:
                query["cursor"] = cursor
            page = native_get(opener, *access, "/groups?" + urlencode(query),
                              limit=BODY_LIMIT, timeout=min(remaining, 3))
            if not isinstance(page, dict) or not isinstance(page.get("items"), list) or len(page["items"]) > 100:
                break
            state = "partial"
            for item in page["items"]:
                if (isinstance(item, dict) and valid_group_id(item.get("id"))
                        and item.get("roomId") == item["id"] and not item.get("deleted")):
                    name = item.get("displayName")
                    groups[item["id"]] = name.strip()[:200] if isinstance(name, str) and name.strip() else item["id"]
            next_cursor = page.get("nextCursor")
            if next_cursor is None:
                state = "ready"
                break
            if not isinstance(next_cursor, str) or not 1 <= len(next_cursor) <= 4096 or next_cursor == cursor:
                break
            cursor = next_cursor
    except HTTPError as exc:
        if state == "unavailable" and exc.code in (401, 403):
            state = "access_denied"
    except (OSError, HTTPException, ValueError, TypeError, KeyError, AttributeError, RecursionError):
        pass
    return {"state": state, "groups": [{"id": group, "name": name} for group, name in groups.items()]}


def service_status(unit, user_manager):
    command = ["/usr/bin/systemctl", "--user" if user_manager else "--system", "show", unit,
               "-p", "LoadState", "-p", "ActiveState", "-p", "UnitFileState", "-p", "InvocationID", "-p", "NRestarts",
               "-p", "ExecMainStartTimestampMonotonic"]
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=2, check=True)
    except (OSError, subprocess.SubprocessError):
        return {"active": "unknown", "startup": "unknown", "restarts": None}, ""
    props = dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)
    active = props.get("ActiveState")
    load = props.get("LoadState")
    startup = props.get("UnitFileState")
    restarts = props.get("NRestarts", "")
    started = props.get("ExecMainStartTimestampMonotonic", "")
    uptime = None
    if active == "active" and started.isascii() and started.isdigit() and int(started):
        uptime = max(0, int(monotonic() - int(started) / 1_000_000))
    return {
        "load": load if load in ("loaded", "not-found", "masked", "error", "bad-setting") else "unknown",
        "active": active if active in ("active", "inactive", "failed", "activating", "deactivating") else "unknown",
        "startup": startup if startup in ("enabled", "disabled", "static", "masked") else "unknown",
        "restarts": int(restarts) if restarts.isascii() and restarts.isdigit() else None,
        "uptimeSeconds": uptime,
    }, props.get("InvocationID", "")


def recent_event(invocation, marker, *, allowed_messages=None):
    if not re.fullmatch(r"[0-9a-f]{32}", invocation) or not invocation.strip("0"):
        return None
    try:
        result = subprocess.run(
            ["/usr/bin/journalctl", "-b", f"_SYSTEMD_INVOCATION_ID={invocation}", f"--grep={marker}",
             "-n", "1", "-o", "json", "--no-pager"], capture_output=True, text=True, timeout=2, check=True,
        )
        record = json.loads(result.stdout)
        moment = datetime.fromtimestamp(int(record["__REALTIME_TIMESTAMP"]) / 1_000_000,
                                        tz=timezone.utc).isoformat(timespec="seconds")
        message = str(record.get("MESSAGE", ""))
        if allowed_messages is not None:
            return {"at": moment, "code": message} if message in allowed_messages else None
        action = re.search(r"保存决策：action=(reply|skip)\b", message)
        return {"at": moment, "action": action[1] if action else None}
    except (OSError, subprocess.SubprocessError, ValueError, OverflowError, KeyError):
        return None


def read_settings(path):
    try:
        return _load_config(path)
    except (FatalConsumerError, UnicodeError, RecursionError):
        return None


def model_status(config, source):
    try:
        provider = selected_provider(config)
        model = selected_model(config)
        if not isinstance(model, str) or not MODEL.fullmatch(model):
            return {"source": "unavailable"}
        result = {"source": source, "provider": provider, "model": model}
        if provider == "deepseek":
            opts = _deepseek_settings(config)
            result.update(dailyLimit=opts.get("daily_request_limit", 50),
                          intervalSeconds=opts.get("min_request_interval_seconds", 20),
                          rollingLimit=opts.get("max_requests_per_60_seconds", 3))
        return result
    except (KeyError, TypeError, ValueError, AttributeError):
        return {"source": "unavailable"}


def configured_account(config):
    value = config.get("agent_wechat") if config else None
    return value.get("account_id") if isinstance(value, dict) else None


def send_mode(config):
    try:
        if (config["app"]["auto_send"] is False and config["sender"]["enabled"] is False
                and config["runtime"]["global_pause"] is True):
            return "observation"
    except (TypeError, KeyError):
        pass
    return "unknown"


def pipeline_state(native, services):
    if any(services[key].get("active") == "failed" for key in ("native", "consumer", "worker")):
        return "failed"
    if native["state"] != "reachable" or native.get("connection") == "unknown":
        return "unknown"
    if not native.get("authenticated") or native.get("connection") != "online":
        return "offline"
    if any(services[key].get("active") not in ("active", "inactive") for key in ("consumer", "worker")):
        return "unknown"
    if services["consumer"].get("active") != "active":
        return "receiving_paused"
    if services["worker"].get("active") != "active":
        return "receiving_only"
    return "observing"


class Dashboard:
    def __init__(self, config_path, runtime_path=RUNTIME_CONFIG, policy_path=AUTOMATIC_POLICY):
        self.config_path = config_path.expanduser().absolute()
        self.runtime_path = runtime_path.expanduser().absolute()
        self.policy_path = policy_path

    def open_wechat_login(self, desktop):
        runtime = read_settings(self.runtime_path)
        local = read_settings(self.config_path)
        if runtime is not None and configured_account(runtime) != configured_account(local):
            raise RuntimeError("本机账号与部署配置不一致，请先核对账号。")
        status = native_status(local, self.config_path)
        if status.get("authenticated") and status.get("connection") == "online":
            return {"status": "online"}
        if status["state"] != "reachable":
            raise RuntimeError("无法读取本机微信账号，登录窗口尚未打开。")
        desktop.open_wechat_login(local)
        return {"status": "opened"}

    def groups(self):
        runtime = read_settings(self.runtime_path)
        reference = read_settings(self.config_path)
        if runtime is None:
            result = native_groups(reference, self.config_path)
        else:
            result = native_groups(runtime, self.runtime_path)
            account = configured_account(runtime)
            if result["state"] == "unavailable" and account and account == configured_account(reference):
                result = native_groups(reference, self.config_path)

        config = runtime if runtime is not None else reference
        wx = config.get("agent_wechat") if config else None
        target = wx.get("target_group_id") if isinstance(wx, dict) else None
        source = "runtime" if runtime is not None else "reference"
        configured = {"id": target, "source": source} if valid_group_id(target) else None
        return {**result, "configured": configured}

    def status(self):
        runtime = read_settings(self.runtime_path)
        reference = read_settings(self.config_path)
        source = "runtime" if runtime is not None else "reference" if reference is not None else "unavailable"
        selected = runtime if runtime is not None else reference
        model = model_status(selected, source)
        services, invocations = {}, {}
        for key, (unit, manager) in SERVICES.items():
            services[key], invocations[key] = service_status(unit, manager)

        native = native_status(runtime, self.runtime_path) if runtime is not None else native_status(reference, self.config_path)
        if (runtime is not None and native["state"] == "unavailable"
                and configured_account(runtime) and configured_account(runtime) == configured_account(reference)):
            native = native_status(reference, self.config_path)

        try:
            policy = read_automatic_reply_policy(self.policy_path)
            policy_status = "active_window" if runtime and policy.permits(runtime, datetime.now(timezone.utc)) else "inactive"
        except BoundaryFailure as exc:
            policy_status = "unavailable" if str(exc) == "AUTOMATIC_POLICY_UNAVAILABLE" else "invalid"

        outbox = None
        if runtime is not None:
            try:
                snapshot = inspect_snapshot(self.runtime_path)
                outbox = snapshot["target_outbox_status_counts"]
            except DryRunError:
                pass

        ack = recent_event(invocations["consumer"], "已持久化并 ACK") or {}
        decision = recent_event(invocations["worker"], "Processor 保存决策") or {}
        sender = (recent_event(invocations["sender"], "^[a-z_]+$", allowed_messages=SENDER_STATUS_CODES)
                  if services["sender"].get("active") == "active" else None)
        return {
            "checkedAt": now_iso(),
            "timezone": CHECKIN_TIMEZONE,
            "pipeline": pipeline_state(native, services),
            "native": native,
            "services": services,
            "activity": {
                "lastAck": ack.get("at"),
                "lastDecision": decision.get("at"),
                "lastDecisionAction": decision.get("action"),
            },
            "model": model,
            "sending": {"mode": send_mode(selected), "source": source, "policy": policy_status,
                        "senderStatus": sender and sender["code"],
                        "checkinReferenceTime": "09:00", "minimumCooldownSeconds": GROUP_SEND_COOLDOWN_SECONDS},
            "outbox": outbox,
        }


class AdminServer(ThreadingHTTPServer):
    def __init__(self, port, state, dashboard, desktop=None):
        self.state = state
        self.dashboard = dashboard
        self.desktop = desktop if desktop is not None else DesktopActions()
        super().__init__((LOOPBACK, port), AdminHandler)


class AdminHandler(BaseHTTPRequestHandler):
    def setup(self):
        self.request.settimeout(8)
        super().setup()

    def log_message(self, format, *args):
        return

    def _respond(self, status, body, content_type):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("Content-Security-Policy", "default-src 'self'; base-uri 'none'; frame-ancestors 'none'; form-action 'none'")
        self.end_headers()
        self.wfile.write(body)

    def _json(self, status, payload):
        self._respond(status, json.dumps(payload, ensure_ascii=False).encode("utf-8"), "application/json; charset=utf-8")

    def _local_host(self):
        port = self.server.server_address[1]
        return self.headers.get("Host") in (f"{LOOPBACK}:{port}", f"localhost:{port}")

    def _same_origin(self):
        return self._local_host() and self.headers.get("Origin") == "http://" + self.headers.get("Host", "")

    def _authenticated(self):
        expected = ("Bearer " + self.server.state.key).encode("ascii")
        auth = self.headers.get("Authorization", "").encode("latin-1")
        if hmac.compare_digest(auth, expected):
            return True
        self._json(HTTPStatus.UNAUTHORIZED, {"error": "请输入本机访问密钥。"})
        return False

    def do_GET(self):
        if not self._local_host():
            self._json(HTTPStatus.BAD_REQUEST, {"error": "请使用本机地址访问。"})
            return
        page = self.path.partition("?")[0]
        if page in ASSETS:
            name, content_type = ASSETS[page]
            self._respond(HTTPStatus.OK, (WEB / name).read_bytes(), content_type)
        elif page in ("/api/session", "/api/status", "/api/draft", "/api/history", "/api/groups"):
            if self._authenticated():
                try:
                    if page == "/api/session":
                        payload = {"authenticated": True}
                    elif page == "/api/status":
                        payload = self.server.dashboard.status()
                    elif page == "/api/groups":
                        payload = self.server.dashboard.groups()
                    elif page == "/api/history":
                        payload = self.server.state.sent_history(self.server.dashboard.runtime_path)
                    else:
                        payload = self.server.state.read_draft()
                except (OSError, ValueError):
                    self._json(HTTPStatus.SERVICE_UNAVAILABLE, {"error": "暂时无法读取后台数据。"})
                else:
                    self._json(HTTPStatus.OK, payload)
        else:
            self._json(HTTPStatus.NOT_FOUND, {"error": "页面不存在。"})

    def do_POST(self):
        if not self._same_origin():
            self._json(HTTPStatus.FORBIDDEN, {"error": "请从本机页面执行此操作。"})
            return
        if self.path == "/api/unlock":
            try:
                confirmed = self.server.desktop.confirm_access()
            except (OSError, subprocess.SubprocessError, RuntimeError):
                self._json(HTTPStatus.SERVICE_UNAVAILABLE, {"error": "桌面确认不可用。请使用访问密钥。"})
                return
            if confirmed:
                self._json(HTTPStatus.OK, {"key": self.server.state.key})
            else:
                self._json(HTTPStatus.FORBIDDEN, {"error": "桌面确认已取消或超时。"})
        elif self.path == "/api/wechat/login":
            if not self._authenticated():
                return
            try:
                payload = self.server.dashboard.open_wechat_login(self.server.desktop)
            except RuntimeError as exc:
                self._json(HTTPStatus.SERVICE_UNAVAILABLE, {"error": str(exc)})
            except (OSError, subprocess.SubprocessError):
                self._json(HTTPStatus.SERVICE_UNAVAILABLE, {"error": "无法打开微信登录终端。"})
            else:
                self._json(HTTPStatus.OK, payload)
        elif self.path == "/api/history/export":
            if not self._authenticated():
                return
            try:
                self.server.desktop.open_history_export(self.server.state.path, self.server.dashboard.runtime_path)
            except (OSError, subprocess.SubprocessError):
                self._json(HTTPStatus.SERVICE_UNAVAILABLE, {"error": "无法打开读取记录的 Ubuntu 终端。"})
            else:
                self._json(HTTPStatus.OK, {"status": "opened"})
        else:
            self._json(HTTPStatus.NOT_FOUND, {"error": "页面不存在。"})

    def do_PUT(self):
        if not self._same_origin():
            self._json(HTTPStatus.FORBIDDEN, {"error": "草稿只能从本机后台保存。"})
            return
        if self.path != "/api/draft":
            self._json(HTTPStatus.NOT_FOUND, {"error": "页面不存在。"})
            return
        if not self._authenticated():
            return
        if self.headers.get_content_type() != "application/json":
            self._json(HTTPStatus.UNSUPPORTED_MEDIA_TYPE, {"error": "请提交 JSON 草稿。"})
            return
        lengths = self.headers.get_all("Content-Length", [])
        length = int(lengths[0]) if len(lengths) == 1 and re.fullmatch(r"[0-9]{1,6}", lengths[0]) else 0
        if self.headers.get("Transfer-Encoding"):
            length = 0
        if not 1 <= length <= BODY_LIMIT:
            self._json(HTTPStatus.BAD_REQUEST, {"error": "草稿大小无效。"})
            return
        try:
            body = self.rfile.read(length)
            value = json.loads(body, object_pairs_hook=_no_duplicate_keys) if len(body) == length else None
        except (ValueError, OSError, RecursionError):
            self._json(HTTPStatus.BAD_REQUEST, {"error": "草稿格式无效。"})
            return
        try:
            draft = self.server.state.save_draft(value, if_match=self.headers.get("If-Match"))
        except DraftConflict:
            self._json(HTTPStatus.PRECONDITION_FAILED, {"error": "另一页面已保存较新配置。请刷新页面后重新编辑。"})
        except ValueError as exc:
            self._json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
        except OSError:
            self._json(HTTPStatus.SERVICE_UNAVAILABLE, {"error": "本机草稿暂无法读取或保存。"})
        else:
            self._json(HTTPStatus.OK, draft)
