# WX-Chat-Bot P5.1 — event cursor expiry recovery

## 1. Problem addressed

The P5 Consumer previously treated HTTP 410 `event_cursor_expired` as a generic fatal error. systemd then retried the same expired cursor until `StartLimitBurst` was reached.

This patch adds a guarded recovery path. It does **not** reset the cursor to `newest=true` blindly.

## 2. Recovery design

The recovery path is entered only when the HTTP response is exactly:

```json
{"error":{"code":"event_cursor_expired"}}
```

It then:

1. Reads the latest already-persisted target-group `message.committed` event as a history anchor.
2. Calls `GET /accounts/{account}/threads/{threadId}/messages?limit=50` and follows its opaque `nextCursor` pagination.
3. Requires complete page coverage (`coverage.partial == false`), a valid `asOfEventCursor`, and a stable `asOfEventCursor` across pages.
4. Uses the documented newest-first ordering and stops only after it finds the last locally persisted target-group message anchor.
5. Fetches each missing message by `GET /accounts/{account}/messages/{messageId}` and verifies account, thread, and message identity.
6. Persists recovered messages through `Storage.persist_message()`.
7. Marks recovered history as context-only (`ignored`), assigns the history snapshot boundary cursor for chronological context, and commits a local `recovery.checkpoint` event plus the durable cursor in one SQLite transaction.
8. Sends the remote ACK only after local history and checkpoint persistence; then records the local ACK.
9. Resumes SSE after the snapshot's `asOfEventCursor`.

The history list's `sourceGeneration` may change while the API still preserves the page cursor's IDs/order; therefore it is validated as present but is not required to be identical across pages. `asOfEventCursor` must remain identical.

## 3. Fail-closed conditions

The Consumer refuses to advance the cursor if any of these occur:

- No previously persisted target-group message anchor exists.
- The anchor cannot be found in the available group history.
- The page coverage is partial or cannot be established as complete.
- `asOfEventCursor` is missing, invalid, or not newer than the local durable cursor.
- Pagination repeats a cursor or duplicates message IDs.
- A page contains an invalid item or another thread's message.
- Message detail identity does not match the configured account/group/message.
- More than 10,000 missed target-group messages would need recovery.
- A consistent history snapshot cannot be obtained after bounded retries.
- Existing message data conflicts with the recovered detail.

On a fail-closed error, the process exits with status 2. The systemd unit now has `RestartPreventExitStatus=2`, preventing another fatal restart loop. The cursor remains unchanged unless the recovery checkpoint transaction has committed.

## 4. Recovered-message policy

Recovered messages are **not** queued for late AI replies. They are marked `ignored` and assigned the history snapshot boundary as `source_cursor` so future messages can use them as chronological context. New messages arriving after the snapshot are delivered normally by SSE and can enter the ordinary pending/decision flow.

Only the configured allowlisted target group is read. Other chats are not backfilled. This matches the bot's current single-group scope.

## 5. Files in this patch

- `src/wechat_ai_bot/consumer.py` — exact 410 classification and automatic safe recovery.
- `src/wechat_ai_bot/storage.py` — atomic recovery checkpoint persistence.
- `tests/test_cursor_recovery.py` — 14 new recovery tests.
- `deploy/wechat-bot-consumer.service` — prevent systemd restart loops for fatal exit status 2.

No SQLite schema migration is required. No message-send API is called. No AI model is called. No Native Daemon configuration is changed.

## 6. Apply on Ubuntu

The Consumer service should already be stopped (`inactive`). Do not start it during patch application.

Download this ZIP to `~/Downloads/`, then run:

```bash
cd ~/apps/wechat-ai-bot
set -euo pipefail

# Refuse to patch while the Consumer is running.
state=$(systemctl --user is-active wechat-bot-consumer.service 2>/dev/null || true)
if [ "$state" = "active" ]; then
  echo "ERROR: stop wechat-bot-consumer.service before applying this patch"
  exit 1
fi

# Back up every file this patch replaces.
backup="$HOME/wechat-ai-bot-p51-backup-$(date +%Y%m%dT%H%M%S)"
mkdir -m 700 "$backup"
cp -a src/wechat_ai_bot/consumer.py "$backup/consumer.py"
cp -a src/wechat_ai_bot/storage.py "$backup/storage.py"
if [ -f ~/.config/systemd/user/wechat-bot-consumer.service ]; then
  cp -a ~/.config/systemd/user/wechat-bot-consumer.service "$backup/wechat-bot-consumer.service"
fi
printf 'Backup: %s\n' "$backup"

# Apply the source and test patch.
unzip -o ~/Downloads/wechat-ai-bot-p51-cursor-recovery.zip -d .

# Compile.
python3 -m compileall -q src/wechat_ai_bot tests

# Focused tests.
PYTHONPATH=src python3 -m unittest discover \
  -s tests -p 'test_consumer.py' -v
PYTHONPATH=src python3 -m unittest discover \
  -s tests -p 'test_cursor_recovery.py' -v

# Full project regression. Previous P5 baseline was 104 tests;
# with 14 new tests the expected total is 118 if all existing tests remain.
PYTHONPATH=src python3 -m unittest discover -s tests -v

# Install the updated unit definition, but DO NOT start the service yet.
mkdir -p ~/.config/systemd/user
install -m 644 deploy/wechat-bot-consumer.service \
  ~/.config/systemd/user/wechat-bot-consumer.service
systemctl --user daemon-reload
systemctl --user show wechat-bot-consumer.service \
  -p Restart -p RestartPreventExitStatus -p ActiveState
```

Only after the full Ubuntu test suite passes should the Consumer be started again:

```bash
systemctl --user reset-failed wechat-bot-consumer.service
systemctl --user start wechat-bot-consumer.service
sleep 5
systemctl --user status wechat-bot-consumer.service --no-pager
journalctl --user -u wechat-bot-consumer.service -n 100 --no-pager
```

Do not run `enable --now` yet. First verify the one-time history recovery and a stable SSE connection. Automatic sending remains disabled.

## 7. Local validation performed for this patch

- Python compilation: passed.
- Existing P5 Consumer tests: 26 passed.
- New Cursor Recovery tests: 14 passed.
- Existing v4 migration tests: 15 passed.
- Existing scheduler tests: 9 passed.
- Combined selected regression: 64 passed.

The uploaded runtime source bundle did not include the complete 104-test Ubuntu suite, so the full 118-test project regression must be run on the Ubuntu project after applying this patch.

## 8. API references used

- Agent WeChat HTTP API — event retention, `410 event_cursor_expired`, thread message pagination, `asOfEventCursor`, and message detail routes: https://thisnick.github.io/agent-wechat/use/api/
- Agent WeChat CLI recipes — `wx messages list` ordering (newest first): https://thisnick.github.io/agent-wechat/use/cli/
- Generated API contract (`NativeMessage`, `NativeThreadMessage`, `NativePage<T>`): https://github.com/thisnick/agent-wechat/blob/main/packages/shared/src/native.generated.ts
