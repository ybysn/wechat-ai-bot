"""Operator actions that run in the local Ubuntu desktop session."""

import os
from pathlib import Path
import re
import stat
import subprocess
from threading import Lock

from wechat_ai_bot.consumer import EXPECTED_AGENT_WECHAT_BASE_URL


class DesktopActions:
    def __init__(self):
        self.confirmation = Lock()

    def confirm_access(self):
        if not self.confirmation.acquire(blocking=False):
            raise RuntimeError("已有一个桌面确认窗口。")
        try:
            result = subprocess.run(
                ["/usr/bin/zenity", "--question", "--title=微信机器人管理后台",
                 "--text=允许当前 Ubuntu 浏览器解锁本机微信机器人管理后台？",
                 "--ok-label=允许", "--cancel-label=取消", "--timeout=45"],
                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                timeout=48, check=False,
            )
            if result.returncode in (1, 5):
                return False
            if result.returncode != 0:
                raise RuntimeError("无法打开桌面确认。可以使用本机访问密钥解锁。")
            return True
        finally:
            self.confirmation.release()

    def open_wechat_login(self, config):
        wx = config.get("agent_wechat") if isinstance(config, dict) else None
        if (not isinstance(wx, dict) or not isinstance(wx.get("base_url"), str)
                or wx["base_url"].rstrip("/") != EXPECTED_AGENT_WECHAT_BASE_URL
                or not isinstance(wx.get("account_id"), str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", wx["account_id"])):
            raise RuntimeError("缺少本机微信账号配置。")
        home = Path.home()
        client = home / "apps/agent-wechat/wx"
        token = home / ".local/share/agent-wechat-isolated/api-token"
        if not client.is_file() or not os.access(client, os.X_OK):
            raise RuntimeError("本机 Agent WeChat 登录命令不可用。")
        metadata = token.lstat()
        if (not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.geteuid()
                or metadata.st_nlink != 1 or stat.S_IMODE(metadata.st_mode) != 0o600):
            raise RuntimeError("本机登录凭据文件的权限不符合要求。")
        # The Native CLI reads its private token and renders any QR in the Ubuntu terminal.
        env = {key: value for key, value in os.environ.items()
               if not key.startswith("AGENT_WECHAT_") and not key.lower().endswith("_proxy")}
        terminal = '"$@"; result=$?; read -r -t 90 -p "按回车关闭登录终端… " || true; exit "$result"'
        subprocess.run(
            ["/usr/bin/ptyxis", "--new-window", "--title", "微信账号登录", "--",
             "/usr/bin/bash", "-c", terminal, "wechat-login", str(client),
             "--server", "http://127.0.0.1:6174", "--account", wx["account_id"], "--token-file", str(token),
             "auth", "login", "--strategy", "auto", "--qr-refresh", "manual", "--timeout", "300"],
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            env=env, timeout=6, check=True,
        )

    def open_history_export(self, destination: Path, config: Path):
        runner = '''
umask 077
temporary=$(/usr/bin/mktemp -- "$1/history.XXXXXXXX") || exit 1
if /usr/bin/sudo -u wechat-sender /usr/bin/python3 -I - --config "$3" < "$2" > "$temporary" &&
   /usr/bin/mv -fT -- "$temporary" "$1/sent-history.json"; then
    printf '%s\\n' '发送记录已更新，可以回到后台查看。'
else
    /usr/bin/unlink -- "$temporary"
    printf '%s\\n' '读取失败，原有记录快照已保留。'
fi
read -r -t 90 -p '按回车关闭终端… ' || true
'''
        subprocess.run(
            ["/usr/bin/ptyxis", "--new-window", "--title", "读取生产发送记录", "--",
             "/usr/bin/bash", "-c", runner, "sent-history", str(destination),
             str(Path(__file__).with_name("history.py")), str(config)],
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=6, check=True,
        )
