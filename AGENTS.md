# Repository Guidelines

## Project Structure & Module Organization

- `src/wechat_ai_bot/` contains the Python package. `consumer.py` persists SSE events; `hydrate_messages.py` retrieves message details; `worker.py`, `context.py`, and `decision_engine.py` handle context and AI decisions.
- `storage.py`, `migrations.py`, and `scheduler.py` manage SQLite persistence, schema upgrades, and scheduled tasks. `sender_*.py` modules cover simulation, diagnostics, contracts, and restricted transport.
- `tests/` contains offline regression tests. `deploy/` holds user-level systemd units and an example LLM configuration. `docs/` contains the Docker and protocol documentation. The local admin dashboard assets live under `src/wechat_ai_bot/admin/web/`.
- `README.md` and `PATCH_README.md` describe particular upgrade stages; check current source before relying on historical scope statements.

## Build, Test, and Development Commands

Run from the repository root with `PYTHONPATH=src`. For the containerized observer, see `docs/docker.md`.

```bash
python3 -m compileall -q src/wechat_ai_bot tests
PYTHONPATH=src python3 -m unittest discover -s tests -v
PYTHONPATH=src python3 -m unittest discover -s tests -p 'test_worker.py' -v
```

These check Python compilation, run the full suite, and run focused worker tests, respectively.

With a deliberately prepared local configuration:

```bash
PYTHONPATH=src python3 -m wechat_ai_bot.consumer --config ./config.json
PYTHONPATH=src python3 -m wechat_ai_bot.worker --config ./config.json --run
```

The consumer contacts the daemon and persists events. The worker requires an explicit prior `--bootstrap-only` run, writes state, and may call the configured model provider.

## Coding Style & Naming Conventions

Use four-space indentation, `snake_case` functions and modules, `PascalCase` classes, and `UPPER_SNAKE_CASE` constants. Follow surrounding import, typing, and quotation conventions. No formatter or linter configuration is present. Keep persistence, decision logic, and transport boundaries explicit.

## Testing Guidelines

Use standard-library `unittest`, `unittest.mock`, temporary SQLite databases, and dedicated loopback HTTP fixtures. Name files `test_*.py` and methods `test_*`. Cover failure paths, cursor recovery, idempotency, migrations, and sending gates. Avoid real WeChat or model requests. No numeric coverage threshold is configured.

## Commit & Pull Request Guidelines

Use concise imperative subjects, such as `Fix cursor recovery checkpoint`. PRs should describe behavior changes, relevant issues, exact validation commands/results, and configuration or migration implications.

## Security & Configuration

Keep credentials, `config.json`, database files, and backups out of commits; review untracked files for secrets. Preserve observe-only behavior and production POST restrictions. Treat service activation and database migration as separate operational changes; back up databases and stop concurrent bot writers before migration.
