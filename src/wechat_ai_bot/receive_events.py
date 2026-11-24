"""One-shot, read-only Agent WeChat SSE integration test."""

import fcntl
import json
import sys
import time
from pathlib import Path
from urllib.parse import quote
from urllib.request import Request, urlopen

from wechat_ai_bot.storage import Storage


def valid_cursor(value):
    return (
        isinstance(value, str)
        and value.isascii()
        and value.isdigit()
        and len(value) <= 19
    )


def main():
    home = Path.home()
    project = home / "apps/wechat-ai-bot"
    cfg = json.loads(
        (project / "config.json").read_text(encoding="utf-8")
    )

    if (
        cfg.get("schema_version") != 2
        or cfg["app"]["auto_send"] is not False
        or cfg["sender"]["enabled"] is not False
        or cfg["runtime"]["global_pause"] is not True
    ):
        raise RuntimeError("安全配置异常，拒绝启动")

    wx = cfg["agent_wechat"]
    account = wx["account_id"]
    group = wx["target_group_id"]
    consumer = wx["consumer_name"]
    base = wx["base_url"].rstrip("/")

    if base != "http://127.0.0.1:6174/v1":
        raise RuntimeError("拒绝非本机 Agent WeChat 地址")

    token = Path(wx["token_file"]).read_text(
        encoding="utf-8"
    ).strip()

    endpoint = (
        f"{base}/accounts/{quote(account, safe='')}"
    )

    def api(url, payload=None):
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }
        data = None
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"

        req = Request(url, headers=headers, data=data)

        with urlopen(req, timeout=15) as res:
            return json.load(res)

    def remote_ack(cursor):
        result = api(
            endpoint
            + "/consumers/"
            + quote(consumer, safe="")
            + "/ack",
            {"cursor": cursor},
        )
        if result.get("cursor") != cursor:
            raise RuntimeError("远程 ACK Cursor 不匹配")

    # 互斥锁：避免两个消费者同时修改游标
    lock_path = (
        home / ".local/share/wechat-ai-bot/state/consumer.lock"
    )
    with open(lock_path, "a+") as lock:
        try:
            fcntl.flock(
                lock.fileno(),
                fcntl.LOCK_EX | fcntl.LOCK_NB
            )
        except BlockingIOError:
            raise RuntimeError("另一个消费者正在运行")

        store = Storage(
            cfg["storage"]["database"],
            consumer_name=consumer,
        )

        try:
            state = store.checkpoint(account)

            if state is None:
                # 首次启动：获取最新事件边界。
                # 随后用 after=cursor 订阅，避免查询与连接
                # 之间的事件丢失。
                page = api(
                    endpoint + "/events?newest=true&limit=1"
                )
                cursor = page.get("nextCursor")

                if not valid_cursor(cursor):
                    raise RuntimeError("初始 Cursor 无效")

                with store.transaction():
                    store.conn.execute(
                        "INSERT INTO consumer_state "
                        "(consumer_name,account_id,"
                        "last_durable_cursor) "
                        "VALUES(?,?,?)",
                        (consumer, account, cursor),
                    )

                print("首次订阅：从最新事件边界开始")

            else:
                cursor = state["last_durable_cursor"]

                if not valid_cursor(cursor):
                    raise RuntimeError(
                        "本地 Cursor 无效，禁止自动重置"
                    )

                print("恢复订阅：使用已有持久化 Cursor")

                # 补偿可能发生在“SQLite 提交成功”
                # 与“远程 ACK 完成”之间的中断。
                if state["last_acked_cursor"] != cursor:
                    remote_ack(cursor)
                    store.mark_acked(account, cursor)
                    print("历史待确认 Cursor 已补偿 ACK")

            print("目标群:", group)
            print("当前 Cursor:", cursor)

            url = endpoint + "/events/stream?after=" + cursor
            req = Request(
                url,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Accept": "text/event-stream",
                },
            )

            count = 0
            target_count = 0
            deadline = time.monotonic() + 120

            with urlopen(req, timeout=35) as res:
                content_type = res.headers.get(
                    "Content-Type", ""
                )
                if not content_type.startswith(
                    "text/event-stream"
                ):
                    raise RuntimeError(
                        "服务器未返回 SSE 数据流"
                    )

                print("SSE 连接成功")
                print("请在目标微信群发送一条消息：SSE-41")
                print("等待目标群消息事件（最多120秒）")

                frame_data = []
                frame_id = ""
                frame_type = ""

                for raw in res:
                    line = raw.decode("utf-8").rstrip("\r\n")

                    if line.startswith(":"):
                        continue

                    if line.startswith("data:"):
                        frame_data.append(
                            line[5:].removeprefix(" ")
                        )
                        if sum(map(len, frame_data)) > 1048576:
                            raise RuntimeError("SSE 帧过大")

                    elif line.startswith("id:"):
                        frame_id = line[3:].strip()

                    elif line.startswith("event:"):
                        frame_type = line[6:].strip()

                    elif line == "":
                        if frame_data:
                            event = json.loads(
                                "\n".join(frame_data)
                            )

                            if frame_type == "error":
                                raise RuntimeError(
                                    "SSE 服务端错误: "
                                    + str(event)
                                )

                            if not isinstance(event, dict):
                                raise RuntimeError(
                                    "SSE 事件格式异常"
                                )

                            next_cursor = event.get("cursor")
                            kind = event.get("kind")
                            data = event.get("data")

                            if (
                                not valid_cursor(next_cursor)
                                or not isinstance(kind, str)
                                or not isinstance(data, dict)
                            ):
                                raise RuntimeError(
                                    "无效的 SSE 事件"
                                )

                            if (
                                frame_id
                                and frame_id != next_cursor
                            ):
                                raise RuntimeError(
                                    "SSE ID 与 Cursor 不一致"
                                )

                            if int(next_cursor) <= int(cursor):
                                raise RuntimeError(
                                    "Cursor 未向前推进"
                                )

                            if event.get("accountId") not in (
                                None, account
                            ):
                                raise RuntimeError(
                                    "收到其他账号的事件"
                                )

                            thread_id = data.get("threadId")
                            message_id = data.get("messageId")

                            is_target = (
                                kind == "message.committed"
                                and thread_id == group
                                and isinstance(message_id, str)
                                and bool(message_id)
                            )

                            # 其他群只保存事件类型和游标。
                            # 不保存其他群的消息 ID 或正文。
                            store.persist_event(
                                account,
                                next_cursor,
                                kind,
                                thread_id=(
                                    group if is_target else None
                                ),
                                message_id=(
                                    message_id if is_target else None
                                ),
                            )

                            # 严格遵循：先 SQLite，再远程 ACK。
                            remote_ack(next_cursor)
                            store.mark_acked(
                                account, next_cursor
                            )

                            cursor = next_cursor
                            count += 1

                            if is_target:
                                target_count += 1
                                print(
                                    "目标群事件捕获成功"
                                    f" | Cursor={cursor}"
                                )
                                break

                        frame_data = []
                        frame_id = ""
                        frame_type = ""

                    if time.monotonic() >= deadline:
                        break

            state = store.checkpoint(account)

            print("\n===== SSE 验收结果 =====")
            print("持久化事件总数:", count)
            print("目标群消息事件:", target_count)
            print(
                "Cursor 一致:",
                state["last_durable_cursor"]
                == state["last_acked_cursor"]
            )

            if target_count < 1:
                print("结果：未捕获目标群测试消息")
                return 1

            if (
                state["last_durable_cursor"]
                != state["last_acked_cursor"]
            ):
                print("结果：Cursor 不一致")
                return 1

            print("结果：PASS")
            return 0

        finally:
            store.close()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(
            f"FAIL: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        sys.exit(1)
