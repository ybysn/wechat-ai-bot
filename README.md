# WeChat AI Bot

A Python bot for one allowlisted WeChat group. It consumes Agent WeChat events, saves its checkpoint in SQLite, and records AI reply decisions. The repository includes a local admin dashboard, a separate Sender service for Linux deployments, and offline regression tests.

The [Docker setup](docs/docker.md) runs the Consumer and Worker in observe-only mode. It requires a running Agent WeChat Daemon and an existing SQLite v4 or v5 database. A [sample configuration](config.example.json) shows the required settings.

Run the offline tests from the repository root on Linux:

```sh
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

## Historical SQLite v4 upgrade

This patch upgrades the existing SQLite v3 database to v4 and introduces the
persistent daily check-in task model. It does not enable sending and does not
start a background scheduler or worker.

## Scope

- Adds `scheduled_tasks` for one daily check-in task per account/group/local day.
- Fixes the check-in schedule at `09:00 Asia/Shanghai` with the immutable text `签到`.
- Rebuilds `outbox` so exactly one source is present:
  - `decision_id` for an accepted AI reply, or
  - `scheduled_task_id` for a scheduled task.
- Preserves all existing Outbox rows and their statuses/IDs/idempotency keys.
- Adds database triggers for source/content consistency, task immutability and
  legal task-state transitions.
- Adds `Storage` methods to create, queue and expire daily check-in tasks.
- Adds `daily_checkin_tick`, a deterministic scheduler tick. It only writes to
  SQLite; it never calls WeChat or Ollama.

## Explicitly not included

- No @-mention handler change. The existing `messages.mentions_self` field is
  retained; priority handling and short-message bypass are a subsequent worker
  integration step.
- No periodic scheduler loop or systemd unit.
- No sender implementation, rate limiter or automatic send enablement.
- No changes to `config.json` or the existing `decision_engine.py`.

## Safety

The migration CLI requires the existing observe-only configuration:

- `app.auto_send = false`
- `sender.enabled = false`
- `runtime.global_pause = true`

The scheduler tick may materialize today's scheduled task while paused, but it
will not create an Outbox row unless all sending gates are explicitly opened
and the caller also passes the explicit `queue_enabled=True` gate. The default
is `False`. The rate limiter and sender are not part of this patch, so normal
service code must leave this gate disabled until those later stages pass.
The Outbox is still not a sender; no network operation is performed.

Do not run the migration while another bot process is writing to the database.
The separate `agent-wechat-native.service` API daemon does not need to be
stopped; do not stop it for this migration.

## Files in this patch

- `src/wechat_ai_bot/migrations.py`
- `src/wechat_ai_bot/storage.py`
- `src/wechat_ai_bot/scheduler.py` (new)
- `tests/test_storage.py`
- `tests/test_process_pending.py`
- `tests/test_migrations.py`
- `tests/test_scheduler.py` (new)

The project's existing `decision_engine.py`, `context.py`, and
`process_pending.py` remain in place. The test suite expects those existing
modules to remain available.

## Apply code patch first

From the project root, back up the files that will be replaced, then extract the
ZIP so its `src/` and `tests/` directories merge into the project:

```bash
cd ~/apps/wechat-ai-bot

mkdir -p "$HOME/wechat-ai-bot-v4-code-backup"
cp --parents \
  src/wechat_ai_bot/migrations.py \
  src/wechat_ai_bot/storage.py \
  tests/test_storage.py \
  tests/test_process_pending.py \
  tests/test_migrations.py \
  "$HOME/wechat-ai-bot-v4-code-backup/"

# Place the downloaded ZIP at ~/Downloads/wechat-ai-bot-sqlite-v4-patch.zip
unzip -o ~/Downloads/wechat-ai-bot-sqlite-v4-patch.zip -d .

python3 -m compileall -q src/wechat_ai_bot tests
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

The patch's isolated test suite contains 38 tests and was run successfully in
the build environment. Run the full project test suite as shown above; the
existing context and decision-engine tests should remain unchanged.

Do not migrate the production database until the full project test suite passes.

## Migrate v3 to v4 after tests pass

```bash
cd ~/apps/wechat-ai-bot
PYTHONPATH=src python3 -m wechat_ai_bot.migrations
```

The migration:

1. Requires schema v3 and the v3 migration record.
2. Checks database integrity and foreign keys.
3. Creates a SQLite-consistent backup using the SQLite backup API.
4. Sets backup permissions to `0600` and verifies the backup is v3.
5. Creates `scheduled_tasks`.
6. Rebuilds `outbox` with nullable source columns and an exactly-one-source
   CHECK constraint.
7. Copies every existing Outbox row without changing its decision source,
   content, status, IDs, timestamps or idempotency key.
8. Recreates `idx_outbox_status` and installs v4 triggers.
9. Records schema version 4 and verifies integrity/foreign keys.

Expected backup filename:

`bot.sqlite3.pre-v4.<UTC timestamp>.bak`

## Verify migration

```bash
cd ~/apps/wechat-ai-bot
python3 - <<'PY'
import json
import sqlite3
from pathlib import Path

cfg = json.loads(Path("config.json").read_text(encoding="utf-8"))
db = Path(cfg["storage"]["database"])

with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as conn:
    print("schema:", conn.execute("PRAGMA user_version").fetchone()[0])
    print("integrity:", conn.execute("PRAGMA integrity_check").fetchone()[0])
    print("foreign keys:", len(conn.execute("PRAGMA foreign_key_check").fetchall()))
    print("scheduled_tasks:", conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='scheduled_tasks'"
    ).fetchone()[0])
    print("outbox scheduled_task_id:", "scheduled_task_id" in {
        row[1] for row in conn.execute("PRAGMA table_info(outbox)")
    })
    print("v4 migration:", conn.execute(
        "SELECT COUNT(*) FROM schema_migrations WHERE version=4"
    ).fetchone()[0])
PY
```

Expected:

- schema: `4`
- integrity: `ok`
- foreign keys: `0`
- scheduled_tasks: `1`
- outbox scheduled_task_id: `True`
- v4 migration: `1`

## Safe scheduler dry-run after migration

With the current paused configuration, this creates today's `scheduled_tasks`
row but does not create an Outbox row:

```bash
cd ~/apps/wechat-ai-bot
PYTHONPATH=src python3 - <<'PY'
import json
from datetime import datetime, timezone
from pathlib import Path
from wechat_ai_bot.scheduler import daily_checkin_tick
from wechat_ai_bot.storage import Storage

cfg = json.loads(Path("config.json").read_text(encoding="utf-8"))
store = Storage(cfg["storage"]["database"], cfg["agent_wechat"]["consumer_name"])
try:
    result = daily_checkin_tick(store, cfg, datetime.now(timezone.utc))
    print("status:", result["status"])
    print("task_id:", result["task_id"])
    print("local_time:", result["local_time"])
    print("Outbox created:", result.get("outbox_id") is not None)
finally:
    store.close()
PY
```

Expected while paused: `status: paused`, a task ID, and `Outbox created: False`.

No message is sent by any command in this patch.
