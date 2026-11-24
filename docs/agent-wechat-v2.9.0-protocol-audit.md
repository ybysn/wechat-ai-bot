# Agent WeChat 2.9.0：发送协议只读审计

- 日期：2026-11-22。
- 阶段：交接任务 C；**PASS（固定版本源码、运行文件与离线兼容性验证）**。
- 未验收：生产成功发送/回执、真实账号上的所有 HTTP 错误、生产 Sender 集成。
  本阶段不授予真实发送权限。

## 版本证据与完整性

以官方 [v2.9.0 发布](https://github.com/thisnick/agent-wechat/releases/tag/v2.9.0)
的固定提交 `d0f3677dd997e22318550b911586ad0d5f067ae5` 为准，不把 main 或滚动文档当成该版本事实。

1. 本机 `~/apps/agent-wechat/wx` 的 SHA256 为
   `477acc93d6a13c6c59fb54bf253f5e35825f9ef46991aec3efa10971c3373d49`，
   与 GitHub Release API 的 `wx-v2.9.0-linux-x64-gnu` 资产摘要一致。
2. 本机 `checksums.json` 的 SHA256 与官方 `wx-v2.9.0-checksums.json` 资产摘要一致；
   清单给出的源码提交与上述固定提交一致。
3. 只读取得 Native Daemon 的 MainPID，读取 `/proc/<pid>/exe`，其哈希与本机 `wx` 一致。
   因此版本证据覆盖当前运行的 Daemon，而非仅覆盖 PATH 中的 CLI。
4. 从固定提交读取 19 个相关源码/文档文件，逐个验证 Git blob 哈希；记录 Git blob、
   SHA256 和字节数，见 [证据清单](evidence/agent-wechat-v2.9.0-source-manifest.json)。
   未执行任何下载源码。
5. 本机唯一 HTTP 探测为无需凭据的 `GET /health`：HTTP 200、`status=ok`。
   该接口没有版本或回执信息；未请求 `/v1/status` 或任何账号发送查询，未读取真实 Token。

## 请求与真实响应的差异

直接证据：

- [文本 HTTP handler](https://github.com/thisnick/agent-wechat/blob/d0f3677dd997e22318550b911586ad0d5f067ae5/packages/native-client-rust/crates/wechat-api/src/media.rs#L621)。
- [运行时实际序列化的 SendIntent](https://github.com/thisnick/agent-wechat/blob/d0f3677dd997e22318550b911586ad0d5f067ae5/packages/native-client-rust/crates/wechat-store/src/operations.rs#L53)。
- [NativeSendIntent 公共类型](https://github.com/thisnick/agent-wechat/blob/d0f3677dd997e22318550b911586ad0d5f067ae5/packages/native-client-rust/crates/wechat-api/src/contracts.rs#L542)。

| 内容 | v2.9.0 源码事实 | 本项目处理 |
|---|---|---|
| 纯文本请求 | POST `/v1/accounts/{account}/sends`，JSON 为 `threadId`、`text`；需要 Idempotency-Key | 现有正文形状兼容；不启用执行 |
| HTTP 返回 | POST 返回 202 和 SendIntent；后台任务继续执行 | 202 不能当作微信已接受 |
| 回执字段 | `id`、`accountId`、`destination`、`state`、`clientId`、`serverId`、`errorCode`、`createdAt` | 不把 `destination` 自动别名为合成 `threadId` |
| 正文及键回显 | 没有 `text`、`idempotencyKey` 字段 | 不能依靠合成检查器证明请求绑定 |
| 处理中状态 | `submitting` | 新增识别；保留已有合成 `sending`，不自动转换数据库状态 |
| serverId | 可空字符串字段；大数字仍是字符串 | 不转成浮点数，不把它视为正文证明 |
| 本地消息投影 | 实际 Store 序列化在有记录时才包含 `localMessageId` | 投影 ID 不能替代审批/请求绑定 |
| clientId | 实际 Store 类型为 String，公共类型允许 null | 不假定公共类型就是全部运行约束 |

源码验证的远端状态为 `queued -> submitting -> accepted / failed / needs_review`；
`queued` 可被取消。`accepted` 是经关联验证的微信侧接受，不证明接收者已读或设备收妥。
本地 Outbox 的 `sending` 是本项目认领状态，与远端 `submitting` 分开保留。

现有 `inspect_send_receipt()` 仍要求账号、群、正文、幂等键全部回显。
真实同键 GET 也返回相同 SendIntent，不能补齐缺失字段。
**不得拿本地正文或幂等键填回响应来伪造“已验证”回执。**
未来必须用已持久化的不可变请求/审批绑定、经过认证的同键查询和远端记录身份另行设计对账。

## 输入限制与兼容修改

依据 [Idempotency-Key 验证](https://github.com/thisnick/agent-wechat/blob/d0f3677dd997e22318550b911586ad0d5f067ae5/packages/native-client-rust/crates/wechat-api/src/lib.rs#L1078)
及 [文本队列输入检查](https://github.com/thisnick/agent-wechat/blob/d0f3677dd997e22318550b911586ad0d5f067ae5/packages/native-client-rust/crates/wechat-core/src/runtime.rs#L1518)：

- POST 幂等键为 1–200 个 ASCII 可打印字节（包含空格）。项目构造器原先允许到 512 字符且可能包含非 ASCII，现已收紧。
- GET 查询 handler 对键检查非空且最多 200 字节；项目使用与 POST 相同的 ASCII 子集，便于稳定复用。
- 文本目标非空、最多 256 个 UTF-8 字节，不得含控制字符或空白。项目新增相同约束。
- 远端普通文本上限为 64 KiB；项目仍保持原有非空白、最多 4000 字符的保守子集。
- 引用、@、媒体、取消和重试均未新增到本项目发送能力。

## 幂等、权限与恢复

[队列实现](https://github.com/thisnick/agent-wechat/blob/d0f3677dd997e22318550b911586ad0d5f067ae5/packages/native-client-rust/crates/wechat-core/src/runtime.rs#L1589)
对目标和正文等请求参数计算摘要，先保存再启动发送任务；
[Store.create_send](https://github.com/thisnick/agent-wechat/blob/d0f3677dd997e22318550b911586ad0d5f067ae5/packages/native-client-rust/crates/wechat-store/src/operations.rs#L513)
对同键同摘要返回原记录，摘要冲突拒绝，不创建第二次提交。

- POST、按 ID GET 和[同键 GET](https://github.com/thisnick/agent-wechat/blob/d0f3677dd997e22318550b911586ad0d5f067ae5/packages/native-client-rust/crates/wechat-api/src/actions.rs#L199)
  均要求目标账号的 `send` scope；只读 Consumer Token 的 `read` 不足以查询发送记录。
  引用文本额外需要 `read`；这些是源码证据，本阶段未重新读取 Token 或现场验证权限。
- [重启恢复](https://github.com/thisnick/agent-wechat/blob/d0f3677dd997e22318550b911586ad0d5f067ae5/packages/native-client-rust/crates/wechat-store/src/operations.rs#L621)
  把未完成的 `submitting` 转为 `needs_review/restart_after_submission`，而非重新提交。
- [保留策略](https://github.com/thisnick/agent-wechat/blob/d0f3677dd997e22318550b911586ad0d5f067ae5/packages/native-client-rust/crates/wechat-store/src/retention.rs)
  给文本回执至少 90 天的重放窗口；退役键保存摘要并拒绝重新使用。
  未决记录按 7 天或较新的 100 条限制压缩为 `needs_review/review_expired`。
- 本项目继续采取更严格规则：`sending` / `needs_review` 禁止自动重新 POST，不能换键重试；
  即使远端文档建议某些失败后使用新键，也不构成本项目的自动授权。

## HTTP 错误证据及限制

[ApiError 与映射](https://github.com/thisnick/agent-wechat/blob/d0f3677dd997e22318550b911586ad0d5f067ae5/packages/native-client-rust/crates/wechat-api/src/lib.rs#L128)
使用 `{"error":{"code":"..."}}`：

| 源码映射 | 含义与本项目约束 |
|---|---|
| 400 `idempotency_key_required` / `invalid_idempotency_key` / `invalid_request` | 输入不合法，禁止构造后盲目尝试 |
| 401 `unauthorized` | 凭据缺失、无效或过期；不扩大权限 |
| 403 `forbidden` | scope 或账号边界不允许 |
| 404 `not_found` | 不足以证明消息从未发送；未决发送仍隔离 |
| 409 `idempotency_conflict` | 同键参数不同，不换键绕过 |
| 409 `idempotency_key_expired` | 旧键已退役，不能据此自动新建发送 |
| 410 `operation_expired` | 已删除记录的底层映射；HTTP 查询层存在下述回退疑点 |
| 500 `operation_failed` | 通用错误不能推断远端未发送 |

**静态推断、尚未现场复现的差异：** 按键和按 ID HTTP handler 在文本 Store 查询的
任何异常后都会回退媒体查询。文本退役仅记录 `SEND_KEY/SEND_ID`，媒体回退查询
检查不同命名空间。因此某些文本过期错误可能最终表现为媒体查询的 404，不能承诺
所有 GET 都返回上述底层 409/410。未来对账必须将 404 视为不确定证据。

审查的文本 API 文件未发现统一的 429 / Retry-After 合同；这不证明运行时永远不会限流。
本阶段不建立错误码自动重发策略。

## 变更与实际验证

修改：`src/wechat_ai_bot/sender_http_contract.py`（输入限制、状态识别和审计说明）。
新增：`tests/test_sender_native_protocol.py`、本文档、公开源码哈希证据清单及可逆源码补丁。
未改 Consumer、Worker、Storage、迁移、HTTP executor 或既有测试。

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src /usr/bin/python3 -m unittest discover -s tests -p 'test_sender_native_protocol.py' -v
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src /usr/bin/python3 -m unittest discover -s tests -q
PYTHONPYCACHEPREFIX=/tmp/wx-protocol-2.9.0-hhsps0q1/pycache /usr/bin/python3 -m compileall -q src/wechat_ai_bot tests
```

- Ubuntu Python 3.14.4：专项 **16/16**，0.002 秒；全量 **374/374**，1.979 秒；编译通过。
- 新增测试使用按固定源码形状人工构造的合成对象，**不是生产成功回执快照**。
- 测试覆盖 ASCII/UTF-8 字节边界、注入字符、原生状态、无回显拒绝、错误对象、
  大 serverId 字符串及生产 POST 在建立 socket 前被拒绝。
- 真正生产回执与本地批准请求的绑定方式仍未实现；本阶段没有把回执检查放宽为自动成功。

## 安全、回滚与下一交付

公开 GitHub 源码读取及一次本机无凭据 GET 之外，没有生产 API 请求。
没有真实 Token 读取、微信 POST、线上模型调用、服务启停、配置或 Cursor 变更、生产库写入或迁移。
全量离线测试中的 HTTP POST 仅发往既有专用合成回环服务。

2026-11-22T02:24:28Z 收尾复核：Consumer、Native Daemon 为 `active/enabled`，
Worker 为 `inactive/disabled`；观察开关仍为 `false/false/true`。
只读诊断为 `NO_CANDIDATES`，目标 Outbox 六种状态及其他目标计数均为 0，
Sender 策略仍不存在，激活 Cursor 为 `41030`。56 个基线文件中只有合同模块变化，
其余 55 个文件 SHA256 一致；未重复应用 D2D-2B2 补丁。

回滚只涉及恢复本阶段前的合同模块并撤回新增测试/文档，先核对后续修改；
生产配置、数据库及服务无需回滚。当前环境无法提供 Git diff，使用修改前文件哈希核对范围。
已保存[可逆源码差异](evidence/sender_http_contract-v2.9.0.patch)，其前版本与修改前 SHA256
完全匹配。以下反向应用预检已通过；没有实际回滚：

```bash
patch --dry-run -R -p1 -i docs/evidence/sender_http_contract-v2.9.0.patch
```

**下一最小交付：交接任务 B 的可信人工审批与对账 ADR。**
以本机 CLI、单条目标/正文/幂等键摘要绑定、短 TTL、一次性额度、撤销和紧急停止为起点，
明确同库认领、配置角色隔离，以及无正文回显时的可信请求绑定和崩溃恢复证据。
给出临时数据库验收方案与迁移/回滚设计，不执行生产迁移或开放 POST。

## 发送回执中的消息投影

固定版本的 `GET /v1/accounts/{a}/messages/{id}` 需要该账号的 `read` 权限。处理函数使用 `Core::read()` 读取并规范化本地 Profile 消息。群纯文本的 `body` 可能移除发送者前缀，撤回消息会携带 `recall`。离线 Sender 要求正文完全匹配，并拒绝将带撤回信息的投影用作成功证据。 [路由处理函数](https://github.com/thisnick/agent-wechat/blob/d0f3677dd997e22318550b911586ad0d5f067ae5/packages/native-client-rust/crates/wechat-api/src/lib.rs#L950) · [正文规范化](https://github.com/thisnick/agent-wechat/blob/d0f3677dd997e22318550b911586ad0d5f067ae5/packages/native-client-rust/crates/wechat-core/src/profile.rs#L252-L315)

`Core::read()` 对自有 Profile 建立读取快照，非自有 Profile 走串行分支并在原对象上运行查询。撤回处理遇到尚未记录的官方撤回时，在快照中构造临时标记，在非快照路径上则可能调用 `record_recall()`。根据这些源码路径，当前受限生产 executor 继续只允许按幂等键查询回执。生产 Sender 获准接入消息投影前，需要核验目标账号实际走快照路径并验收真实回执格式。 [读取视图](https://github.com/thisnick/agent-wechat/blob/d0f3677dd997e22318550b911586ad0d5f067ae5/packages/native-client-rust/crates/wechat-core/src/read_view.rs#L124-L165) · [撤回处理](https://github.com/thisnick/agent-wechat/blob/d0f3677dd997e22318550b911586ad0d5f067ae5/packages/native-client-rust/crates/wechat-core/src/recalls.rs#L612-L635)
