# 使用 Docker 运行微信机器人

Compose 运行两个观察服务。Consumer 接收 SSE、保存游标并向 Agent WeChat 确认事件。Worker 读取目标群消息、调用已配置的模型并保存决策。当前 Docker 交付范围是这两个服务。容器沿用项目的观察模式，自动发送关闭。

## 运行环境

使用 Linux Docker Engine 和 Docker Compose v2。Windows 可在 WSL2 的 Ubuntu 发行版里运行这一套，Docker Engine 和 Agent WeChat Daemon 都放在同一个发行版中，项目和 SQLite 文件放在 WSL 的 Linux 文件系统里。Windows 原生 `.exe` 需要把 Linux 文件锁、权限检查以及 Daemon 依赖移植到 Windows。

Compose 使用宿主机网络，让容器沿用 Daemon 的本机地址 `http://127.0.0.1:6174/v1`。先在宿主机确认 Daemon 已运行、微信账号已登录，观察凭据可接收事件、确认游标及读取目标群。容器沿用已经配置且可达的模型服务。[Docker 宿主机网络文档](https://docs.docker.com/engine/network/drivers/host/)

## 准备配置和数据

以持有这些文件的普通 Linux 用户，从仓库根目录执行：

```sh
install -d -m 700 docker-data/config docker-data/state
install -m 600 config.json docker-data/config/config.json
export WECHAT_BOT_UID="$(id -u)" WECHAT_BOT_GID="$(id -g)"
```

如果手头没有 `config.json`，将上面的复制来源改为 `config.example.json`。编辑 `docker-data/config/config.json`，填入自己的账号和目标群，并让目标群与 `safety.group_allowlist` 一致。迁移已有实例时，沿用原来的账号、目标群、白名单和模型设置。检查以下字段。配置目录以只读方式挂载，状态目录可写。

| 字段 | 值 |
| --- | --- |
| `agent_wechat.base_url` | `http://127.0.0.1:6174/v1` |
| `agent_wechat.token_file` | `/config/agent-wechat-token` |
| `storage.database` | `/state/bot.sqlite3` |
| `llm.deepseek.api_key_file` | 使用 DeepSeek 时设为 `/config/deepseek-api-key` |
| `llm.deepseek.usage_log_file` | 使用 DeepSeek 时设为 `/state/deepseek-usage.jsonl` |
| `app.auto_send`、`sender.enabled` | `false` |
| `runtime.global_pause` | `true` |

配置副本使用单用户布局。若复制的是隔离部署配置，删除副本里的 `storage.shared_group_id` 和 `storage.service_state_directory`。把现有观察凭据另存为 `docker-data/config/agent-wechat-token`。使用 DeepSeek 时，把现有 API 密钥另存为 `docker-data/config/deepseek-api-key`。两份凭据都使用 `600` 权限，并归当前 Linux 用户所有。配置目录只放 Compose 所需的配置和凭据。

启动前需要已有的 SQLite v4 或完整 v5 数据库。切换现有实例时，先停止同一账号上使用同一 `consumer_name` 的旧 Consumer，停止旧 Worker 和写入源数据库的其他服务。用 SQLite backup 得到一致性快照，保存为 `docker-data/state/bot.sqlite3`。沿用原来的 `consumer_name`，并从同一实例复制现有的 `worker-state.json` 和 DeepSeek 用量记录（如有），以保留激活游标及请求限额。保持状态目录为 `700`，数据库和状态文件为 `600`，并归运行 Compose 的 Linux 用户所有。

## 构建和运行

```sh
docker compose config --quiet
docker compose build
docker compose up -d consumer
docker compose logs --tail=50 consumer
```

迁移已有激活状态时，可以直接启动 Worker。首次激活时，确认 Consumer 已持久化游标，再运行一次初始化。初始化读取历史上下文，退出成功后即可启动 Worker。

```sh
docker compose run --rm worker python -m wechat_ai_bot.worker --config /config/config.json --bootstrap-only
```

```sh
docker compose up -d worker
docker compose ps
docker compose logs --tail=50 worker
```

需要停止时运行 `docker compose down`。文件保存在 `docker-data`，容器重建后会继续使用。新的终端需重新导出 UID 和 GID。若服务以状态码 2 退出，先查看日志并修复配置或游标问题，再重新启动相应服务。
