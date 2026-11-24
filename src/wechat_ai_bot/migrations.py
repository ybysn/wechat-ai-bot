"""Explicit, backed-up SQLite migrations for wechat-ai-bot."""

from __future__ import annotations

import os
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path


SCHEMA_VERSION = 4

AUTOMATIC_SCHEMA_VERSION = 5
_AUTOMATIC_COLUMNS = {"outbox_id", "policy_digest", "policy_generation", "source_cursor", "request_sha256", "claimed_at"}
_AUTOMATIC_TRIGGERS = {"automatic_dispatch_no_update", "automatic_dispatch_no_delete", "automatic_outbox_guard"}


def automatic_schema_ready(conn: sqlite3.Connection) -> bool:
    """Recognize the complete additive schema without creating any objects."""
    if conn.execute("PRAGMA user_version").fetchone()[0] != AUTOMATIC_SCHEMA_VERSION:
        return False
    if _columns(conn, "automatic_dispatch") != _AUTOMATIC_COLUMNS or "schema_migrations" not in _tables(conn):
        return False
    triggers = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='trigger'")}
    return _AUTOMATIC_TRIGGERS <= triggers and conn.execute(
        "SELECT 1 FROM schema_migrations WHERE version=?", (AUTOMATIC_SCHEMA_VERSION,),
    ).fetchone() is not None


def migrate_v4_to_v5(conn: sqlite3.Connection) -> bool:
    """Add automatic reply claim evidence. Call explicitly after backup and approval."""
    if conn.in_transaction:
        raise RuntimeError("Migration requires a connection outside a transaction")
    conn.execute("BEGIN IMMEDIATE")
    try:
        if automatic_schema_ready(conn):
            conn.commit()
            return False
        if (conn.execute("PRAGMA user_version").fetchone()[0] != 4
                or not {"outbox", "messages", "decisions", "scheduled_tasks", "schema_migrations"} <= _tables(conn)
                or not {"outbox_id", "decision_id", "scheduled_task_id", "account_id", "thread_id", "content",
                        "idempotency_key", "status", "remote_send_id", "sent_at"} <= _columns(conn, "outbox")
                or "reply" not in _columns(conn, "decisions")
                or conn.execute("SELECT 1 FROM schema_migrations WHERE version=4").fetchone() is None):
            raise RuntimeError("Expected complete v4 schema")
        conn.execute("""
            CREATE TABLE automatic_dispatch (
                outbox_id TEXT PRIMARY KEY NOT NULL REFERENCES outbox(outbox_id),
                policy_digest TEXT NOT NULL CHECK (length(policy_digest)=64),
                policy_generation INTEGER NOT NULL CHECK (policy_generation>0),
                source_cursor TEXT NOT NULL,
                request_sha256 TEXT NOT NULL CHECK (length(request_sha256)=64),
                claimed_at TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TRIGGER automatic_dispatch_no_update BEFORE UPDATE ON automatic_dispatch
            BEGIN SELECT RAISE(ABORT, 'immutable automatic dispatch'); END
        """)
        conn.execute("""
            CREATE TRIGGER automatic_dispatch_no_delete BEFORE DELETE ON automatic_dispatch
            BEGIN SELECT RAISE(ABORT, 'immutable automatic dispatch'); END
        """)
        conn.execute("""
            CREATE TRIGGER automatic_outbox_guard BEFORE UPDATE ON outbox
            WHEN EXISTS (SELECT 1 FROM automatic_dispatch WHERE outbox_id=OLD.outbox_id)
                AND (NEW.outbox_id IS NOT OLD.outbox_id OR NEW.decision_id IS NOT OLD.decision_id
                     OR NEW.scheduled_task_id IS NOT OLD.scheduled_task_id
                     OR NEW.account_id IS NOT OLD.account_id OR NEW.thread_id IS NOT OLD.thread_id
                     OR NEW.content IS NOT OLD.content OR NEW.idempotency_key IS NOT OLD.idempotency_key
                     OR (OLD.remote_send_id IS NOT NULL AND NEW.remote_send_id IS NOT OLD.remote_send_id)
                     OR NOT (NEW.status=OLD.status OR
                             (OLD.status='sending' AND NEW.status IN ('needs_review','sent','cancelled')) OR
                             (OLD.status='needs_review' AND NEW.status='sent')))
            BEGIN SELECT RAISE(ABORT, 'invalid automatic dispatch change'); END
        """)
        conn.execute("INSERT INTO schema_migrations(version) VALUES (?)", (AUTOMATIC_SCHEMA_VERSION,))
        conn.execute(f"PRAGMA user_version={AUTOMATIC_SCHEMA_VERSION}")
        if (not automatic_schema_ready(conn)
                or conn.execute("PRAGMA integrity_check").fetchone()[0] != "ok"
                or conn.execute("PRAGMA foreign_key_check").fetchone() is not None):
            raise RuntimeError("v5 verification failed")
        conn.commit()
        return True
    except BaseException:
        conn.rollback()
        raise


def backup_and_migrate_v4_to_v5(database_path: str | Path) -> Path | None:
    """Back up and upgrade an explicit database. Stop its writers before calling."""
    db = Path(database_path).absolute()
    if db.is_symlink() or not db.is_file() or db.stat().st_nlink != 1:
        raise ValueError("Expected an existing regular database without links")
    with closing(sqlite3.connect(db.as_uri() + "?mode=rw", uri=True, timeout=15, isolation_level=None)) as conn:
        if automatic_schema_ready(conn):
            return None
        if (conn.execute("PRAGMA user_version").fetchone()[0] != 4
                or conn.execute("PRAGMA integrity_check").fetchone()[0] != "ok"
                or conn.execute("PRAGMA foreign_key_check").fetchone() is not None):
            raise RuntimeError("Pre-migration v4 verification failed")
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        backup = db.with_name(f"{db.name}.pre-v5.{stamp}.bak")
        os.close(os.open(backup, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600))
        with closing(sqlite3.connect(backup, isolation_level=None)) as saved:
            saved.execute("PRAGMA synchronous=FULL")
            conn.backup(saved)
            if (saved.execute("PRAGMA user_version").fetchone()[0] != 4
                    or saved.execute("PRAGMA integrity_check").fetchone()[0] != "ok"
                    or saved.execute("PRAGMA foreign_key_check").fetchone() is not None):
                raise RuntimeError("Pre-v5 backup verification failed")
        directory = os.open(db.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        if not migrate_v4_to_v5(conn):
            raise RuntimeError("Database changed after backup")
        return backup


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {
        row[1]
        for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
    }


def _tables(conn: sqlite3.Connection) -> set[str]:
    return {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }


def _install_v3_triggers(conn: sqlite3.Connection) -> None:
    """Install the v3 decision/reply integrity triggers."""
    statements = (
        """
        CREATE TRIGGER IF NOT EXISTS trg_decisions_reply_insert
        BEFORE INSERT ON decisions
        WHEN
            (NEW.action = 'reply' AND (
                NEW.reply IS NULL
                OR length(trim(NEW.reply)) = 0
                OR length(CAST(NEW.reply AS BLOB)) > 16384
            ))
            OR (NEW.action = 'skip' AND NEW.reply IS NOT NULL)
        BEGIN
            SELECT RAISE(ABORT, 'invalid decisions.reply');
        END
        """,
        """
        CREATE TRIGGER IF NOT EXISTS trg_decisions_reply_update
        BEFORE UPDATE OF action, reply ON decisions
        WHEN
            (NEW.action = 'reply' AND (
                NEW.reply IS NULL
                OR length(trim(NEW.reply)) = 0
                OR length(CAST(NEW.reply AS BLOB)) > 16384
            ))
            OR (NEW.action = 'skip' AND NEW.reply IS NOT NULL)
        BEGIN
            SELECT RAISE(ABORT, 'invalid decisions.reply');
        END
        """,
        """
        CREATE TRIGGER IF NOT EXISTS trg_outbox_reply_insert
        BEFORE INSERT ON outbox
        WHEN NOT EXISTS (
            SELECT 1
            FROM decisions AS d
            JOIN messages AS m
              ON m.account_id = d.account_id
             AND m.message_id = d.source_message_id
            WHERE d.decision_id = NEW.decision_id
              AND d.account_id = NEW.account_id
              AND d.action = 'reply'
              AND d.status = 'accepted'
              AND d.reply IS NOT NULL
              AND d.reply = NEW.content
              AND m.thread_id = NEW.thread_id
        )
        BEGIN
            SELECT RAISE(ABORT, 'outbox must match accepted decision reply');
        END
        """,
        """
        CREATE TRIGGER IF NOT EXISTS trg_outbox_reply_update
        BEFORE UPDATE OF decision_id, account_id, thread_id, content ON outbox
        WHEN NOT EXISTS (
            SELECT 1
            FROM decisions AS d
            JOIN messages AS m
              ON m.account_id = d.account_id
             AND m.message_id = d.source_message_id
            WHERE d.decision_id = NEW.decision_id
              AND d.account_id = NEW.account_id
              AND d.action = 'reply'
              AND d.status = 'accepted'
              AND d.reply IS NOT NULL
              AND d.reply = NEW.content
              AND m.thread_id = NEW.thread_id
        )
        BEGIN
            SELECT RAISE(ABORT, 'outbox must match accepted decision reply');
        END
        """,
        """
        CREATE TRIGGER IF NOT EXISTS trg_decision_reply_immutable_after_outbox
        BEFORE UPDATE OF action, account_id, source_message_id, reply ON decisions
        WHEN EXISTS (
            SELECT 1 FROM outbox WHERE decision_id = OLD.decision_id
        )
        BEGIN
            SELECT RAISE(ABORT, 'decision reply is immutable after enqueue');
        END
        """,
    )
    for statement in statements:
        conn.execute(statement)


def migrate_v2_to_v3(conn: sqlite3.Connection) -> bool:
    """Retained for isolated tests and controlled historical migrations."""
    if conn.in_transaction:
        raise RuntimeError("Migration requires a connection outside a transaction")

    version = conn.execute("PRAGMA user_version").fetchone()[0]
    required = {"decisions", "messages", "outbox", "schema_migrations"}
    missing = required - _tables(conn)
    if missing:
        raise RuntimeError(f"Missing required tables: {sorted(missing)}")

    decision_columns = _columns(conn, "decisions")
    if version == 3:
        if "reply" not in decision_columns:
            raise RuntimeError("Schema reports v3 but decisions.reply is missing")
        conn.execute("BEGIN IMMEDIATE")
        try:
            _install_v3_triggers(conn)
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        return False

    if version != 2:
        raise RuntimeError(f"Unsupported schema version for v2->v3: {version}")
    if "reply" in decision_columns:
        raise RuntimeError("Schema v2 unexpectedly already has decisions.reply")

    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute("ALTER TABLE decisions ADD COLUMN reply TEXT")
        conn.execute(
            """
            UPDATE decisions
               SET reply = (
                   SELECT o.content FROM outbox AS o
                    WHERE o.decision_id = decisions.decision_id
               )
             WHERE action = 'reply'
               AND EXISTS (
                   SELECT 1 FROM outbox AS o
                    WHERE o.decision_id = decisions.decision_id
               )
            """
        )
        _install_v3_triggers(conn)
        conn.execute("INSERT INTO schema_migrations(version) VALUES(3)")
        conn.execute("PRAGMA user_version=3")
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        fk_errors = conn.execute("PRAGMA foreign_key_check").fetchall()
        if integrity != "ok" or fk_errors:
            raise RuntimeError(
                f"v2->v3 verification failed: integrity={integrity}, fk={len(fk_errors)}"
            )
        conn.commit()
        return True
    except BaseException:
        conn.rollback()
        raise


def _create_scheduled_tasks(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE scheduled_tasks (
            task_id TEXT PRIMARY KEY,
            task_type TEXT NOT NULL CHECK(task_type = 'daily_checkin'),
            account_id TEXT NOT NULL,
            thread_id TEXT NOT NULL,
            local_date TEXT NOT NULL
                CHECK(length(local_date) = 10),
            timezone TEXT NOT NULL,
            scheduled_at TEXT NOT NULL,
            content TEXT NOT NULL CHECK(content = '签到'),
            idempotency_key TEXT NOT NULL UNIQUE,
            status TEXT NOT NULL DEFAULT 'scheduled'
                CHECK(status IN (
                    'scheduled','queued','sent','expired',
                    'cancelled','needs_review'
                )),
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(task_type, account_id, thread_id, local_date)
        )
        """
    )
    conn.execute(
        "CREATE INDEX idx_scheduled_tasks_status_due "
        "ON scheduled_tasks(status, scheduled_at)"
    )


def _create_outbox_v4(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE outbox_v4 (
            outbox_id TEXT PRIMARY KEY,
            decision_id TEXT UNIQUE,
            scheduled_task_id TEXT UNIQUE,
            account_id TEXT NOT NULL,
            thread_id TEXT NOT NULL,
            content TEXT NOT NULL,
            idempotency_key TEXT NOT NULL UNIQUE,
            status TEXT NOT NULL DEFAULT 'queued'
                CHECK(status IN (
                    'queued','sending','sent','failed',
                    'needs_review','cancelled'
                )),
            remote_send_id TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            sent_at TEXT,
            CHECK (
                (decision_id IS NOT NULL AND scheduled_task_id IS NULL)
                OR
                (decision_id IS NULL AND scheduled_task_id IS NOT NULL)
            ),
            FOREIGN KEY(decision_id) REFERENCES decisions(decision_id),
            FOREIGN KEY(scheduled_task_id) REFERENCES scheduled_tasks(task_id)
        )
        """
    )


def _install_v4_triggers(conn: sqlite3.Connection) -> None:
    """Install v4 guards for AI and scheduled-task Outbox sources."""
    # These two names existed in v3 with decision-only logic. Replace them.
    conn.execute("DROP TRIGGER IF EXISTS trg_outbox_reply_insert")
    conn.execute("DROP TRIGGER IF EXISTS trg_outbox_reply_update")

    statements = (
        """
        CREATE TRIGGER IF NOT EXISTS trg_outbox_reply_insert
        BEFORE INSERT ON outbox
        WHEN NOT (
            (
                NEW.decision_id IS NOT NULL
                AND NEW.scheduled_task_id IS NULL
                AND EXISTS (
                    SELECT 1
                    FROM decisions AS d
                    JOIN messages AS m
                      ON m.account_id = d.account_id
                     AND m.message_id = d.source_message_id
                    WHERE d.decision_id = NEW.decision_id
                      AND d.account_id = NEW.account_id
                      AND d.action = 'reply'
                      AND d.status = 'accepted'
                      AND d.reply IS NOT NULL
                      AND d.reply = NEW.content
                      AND m.thread_id = NEW.thread_id
                )
            )
            OR
            (
                NEW.decision_id IS NULL
                AND NEW.scheduled_task_id IS NOT NULL
                AND EXISTS (
                    SELECT 1 FROM scheduled_tasks AS t
                    WHERE t.task_id = NEW.scheduled_task_id
                      AND t.task_type = 'daily_checkin'
                      AND t.status = 'scheduled'
                      AND t.account_id = NEW.account_id
                      AND t.thread_id = NEW.thread_id
                      AND t.content = NEW.content
                )
            )
        )
        BEGIN
            SELECT RAISE(ABORT, 'outbox source/content mismatch');
        END
        """,
        """
        CREATE TRIGGER IF NOT EXISTS trg_outbox_reply_update
        BEFORE UPDATE OF decision_id, scheduled_task_id, account_id, thread_id, content ON outbox
        WHEN NOT (
            (
                NEW.decision_id IS NOT NULL
                AND NEW.scheduled_task_id IS NULL
                AND EXISTS (
                    SELECT 1
                    FROM decisions AS d
                    JOIN messages AS m
                      ON m.account_id = d.account_id
                     AND m.message_id = d.source_message_id
                    WHERE d.decision_id = NEW.decision_id
                      AND d.account_id = NEW.account_id
                      AND d.action = 'reply'
                      AND d.status = 'accepted'
                      AND d.reply IS NOT NULL
                      AND d.reply = NEW.content
                      AND m.thread_id = NEW.thread_id
                )
            )
            OR
            (
                NEW.decision_id IS NULL
                AND NEW.scheduled_task_id IS NOT NULL
                AND EXISTS (
                    SELECT 1 FROM scheduled_tasks AS t
                    WHERE t.task_id = NEW.scheduled_task_id
                      AND t.task_type = 'daily_checkin'
                      AND t.status = 'queued'
                      AND t.account_id = NEW.account_id
                      AND t.thread_id = NEW.thread_id
                      AND t.content = NEW.content
                )
            )
        )
        BEGIN
            SELECT RAISE(ABORT, 'outbox source/content mismatch');
        END
        """,
        """
        CREATE TRIGGER IF NOT EXISTS trg_scheduled_task_insert_guard
        BEFORE INSERT ON scheduled_tasks
        WHEN NEW.task_type != 'daily_checkin'
          OR NEW.content != '签到'
          OR NEW.timezone != 'Asia/Shanghai'
          OR substr(NEW.scheduled_at, 1, 10) != NEW.local_date
          OR substr(NEW.scheduled_at, 12, 5) != '09:00'
          OR length(NEW.scheduled_at) != 25
          OR substr(NEW.scheduled_at, 20, 6) != '+08:00'
        BEGIN
            SELECT RAISE(ABORT, 'invalid daily check-in schedule');
        END
        """,
        """
        CREATE TRIGGER IF NOT EXISTS trg_scheduled_task_immutable_after_outbox
        BEFORE UPDATE OF task_type, account_id, thread_id, local_date,
                         timezone, scheduled_at, content, idempotency_key
        ON scheduled_tasks
        WHEN EXISTS (
            SELECT 1 FROM outbox WHERE scheduled_task_id = OLD.task_id
        )
        BEGIN
            SELECT RAISE(ABORT, 'scheduled task is immutable after enqueue');
        END
        """,
        """
        CREATE TRIGGER IF NOT EXISTS trg_scheduled_task_status_transition
        BEFORE UPDATE OF status ON scheduled_tasks
        WHEN NOT (
            OLD.status = NEW.status
            OR (OLD.status = 'scheduled' AND NEW.status IN ('queued','expired','cancelled'))
            OR (OLD.status = 'queued' AND NEW.status IN ('sent','expired','cancelled','needs_review'))
        )
        BEGIN
            SELECT RAISE(ABORT, 'invalid scheduled task status transition');
        END
        """,
    )
    for statement in statements:
        conn.execute(statement)


def migrate_v3_to_v4(conn: sqlite3.Connection) -> bool:
    """Rebuild Outbox to support either an AI decision or a scheduled task."""
    if conn.in_transaction:
        raise RuntimeError("Migration requires a connection outside a transaction")

    version = conn.execute("PRAGMA user_version").fetchone()[0]
    required = {"decisions", "messages", "outbox", "schema_migrations"}
    missing = required - _tables(conn)
    if missing:
        raise RuntimeError(f"Missing required tables: {sorted(missing)}")

    if version == 4:
        if "scheduled_tasks" not in _tables(conn):
            raise RuntimeError("Schema reports v4 but scheduled_tasks is missing")
        required_outbox = {"decision_id", "scheduled_task_id"}
        if not required_outbox.issubset(_columns(conn, "outbox")):
            raise RuntimeError("Schema reports v4 but Outbox source columns are missing")
        conn.execute("BEGIN IMMEDIATE")
        try:
            _install_v3_triggers(conn)
            _install_v4_triggers(conn)
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        return False

    if version != 3:
        raise RuntimeError(f"Expected schema v3, found v{version}")

    migration_v3 = conn.execute(
        "SELECT 1 FROM schema_migrations WHERE version=3"
    ).fetchone()
    if migration_v3 is None:
        raise RuntimeError("schema_migrations has no v3 record")
    if "scheduled_tasks" in _tables(conn) or "outbox_v4" in _tables(conn):
        raise RuntimeError("Unexpected partial v4 objects; refusing migration")

    decision_columns = _columns(conn, "decisions")
    outbox_columns = _columns(conn, "outbox")
    if "reply" not in decision_columns:
        raise RuntimeError("v3 decisions.reply is missing")
    expected_old_outbox = {
        "outbox_id", "decision_id", "account_id", "thread_id",
        "content", "idempotency_key", "status", "remote_send_id",
        "created_at", "sent_at",
    }
    if not expected_old_outbox.issubset(outbox_columns):
        raise RuntimeError("Unexpected v3 Outbox schema; refusing rebuild")

    # PRAGMA foreign_keys cannot be toggled inside a transaction. The table
    # rebuild is performed with enforcement temporarily disabled, then all
    # foreign keys are explicitly checked before commit.
    fk_was_on = bool(conn.execute("PRAGMA foreign_keys").fetchone()[0])
    conn.execute("PRAGMA foreign_keys=OFF")
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            _create_scheduled_tasks(conn)

            # Drop old Outbox-bound triggers before rebuilding the table.
            for name in (
                "trg_outbox_reply_insert",
                "trg_outbox_reply_update",
                "trg_decision_reply_immutable_after_outbox",
            ):
                conn.execute(f"DROP TRIGGER IF EXISTS {name}")

            _create_outbox_v4(conn)
            conn.execute(
                """
                INSERT INTO outbox_v4 (
                    outbox_id, decision_id, scheduled_task_id,
                    account_id, thread_id, content, idempotency_key,
                    status, remote_send_id, created_at, sent_at
                )
                SELECT
                    outbox_id, decision_id, NULL,
                    account_id, thread_id, content, idempotency_key,
                    status, remote_send_id, created_at, sent_at
                FROM outbox
                """
            )
            old_count = conn.execute(
                "SELECT COUNT(*) FROM outbox"
            ).fetchone()[0]
            new_count = conn.execute(
                "SELECT COUNT(*) FROM outbox_v4"
            ).fetchone()[0]
            if old_count != new_count:
                raise RuntimeError("Outbox row count changed during migration")

            conn.execute("DROP TABLE outbox")
            conn.execute("ALTER TABLE outbox_v4 RENAME TO outbox")
            conn.execute(
                "CREATE INDEX idx_outbox_status ON outbox(status, created_at)"
            )

            _install_v3_triggers(conn)
            _install_v4_triggers(conn)
            conn.execute(
                "INSERT INTO schema_migrations(version) VALUES(4)"
            )
            conn.execute("PRAGMA user_version=4")

            integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
            fk_errors = conn.execute("PRAGMA foreign_key_check").fetchall()
            if integrity != "ok" or fk_errors:
                raise RuntimeError(
                    f"v3->v4 verification failed: integrity={integrity}, "
                    f"foreign_keys={len(fk_errors)}"
                )
            conn.commit()
            return True
        except BaseException:
            conn.rollback()
            raise
    finally:
        conn.execute(
            "PRAGMA foreign_keys=" + ("ON" if fk_was_on else "OFF")
        )


def backup_and_migrate_v3_to_v4(database_path: str | Path) -> Path | None:
    """Create and verify a SQLite backup, then migrate v3 to v4."""
    db_path = Path(database_path)
    if db_path.is_symlink() or not db_path.is_file():
        raise ValueError("Database path must be an existing regular file")

    with sqlite3.connect(str(db_path), timeout=15, isolation_level=None) as conn:
        conn.execute("PRAGMA busy_timeout=15000")
        conn.execute("PRAGMA foreign_keys=ON")
        version = conn.execute("PRAGMA user_version").fetchone()[0]

        if version == 4:
            migrate_v3_to_v4(conn)
            return None
        if version != 3:
            raise RuntimeError(f"Expected schema v3, found v{version}")

        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        fk_errors = conn.execute("PRAGMA foreign_key_check").fetchall()
        if integrity != "ok" or fk_errors:
            raise RuntimeError(
                f"Pre-migration check failed: integrity={integrity}, "
                f"foreign_keys={len(fk_errors)}"
            )

        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        backup_path = db_path.with_name(
            f"{db_path.name}.pre-v4.{stamp}.bak"
        )
        if backup_path.exists():
            raise FileExistsError(backup_path)

        with sqlite3.connect(str(backup_path)) as backup:
            conn.backup(backup)
        os.chmod(backup_path, 0o600)

        with sqlite3.connect(
            f"file:{backup_path}?mode=ro", uri=True
        ) as backup:
            backup_version = backup.execute(
                "PRAGMA user_version"
            ).fetchone()[0]
            backup_integrity = backup.execute(
                "PRAGMA integrity_check"
            ).fetchone()[0]
        if backup_version != 3 or backup_integrity != "ok":
            raise RuntimeError("Pre-v4 backup verification failed")

        if not migrate_v3_to_v4(conn):
            raise RuntimeError("Unexpected no-op during v3->v4 migration")

        final_version = conn.execute("PRAGMA user_version").fetchone()[0]
        final_integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        final_fk = conn.execute("PRAGMA foreign_key_check").fetchall()
        if final_version != 4 or final_integrity != "ok" or final_fk:
            raise RuntimeError(
                f"Post-migration verification failed: version={final_version}, "
                f"integrity={final_integrity}, foreign_keys={len(final_fk)}"
            )
        return backup_path


def main() -> int:
    import json

    config_path = Path.home() / "apps/wechat-ai-bot/config.json"
    config = json.loads(config_path.read_text(encoding="utf-8"))
    if (
        config.get("schema_version") != 2
        or config["app"]["auto_send"] is not False
        or config["sender"]["enabled"] is not False
        or config["runtime"]["global_pause"] is not True
    ):
        raise RuntimeError("Safety configuration is not in observe-only mode")

    backup = backup_and_migrate_v3_to_v4(config["storage"]["database"])
    print("===== SQLite v3 -> v4 migration =====")
    print("Schema version: 4")
    print("Backup:", str(backup) if backup else "Already v4; no backup needed")
    print("Scheduled tasks: daily_checkin")
    print("Check-in: 09:00 Asia/Shanghai / 固定文本：签到")
    print("Outbox sources: decision OR scheduled_task")
    print("Integrity: ok")
    print("Foreign key errors: 0")
    print("Automatic sending: disabled")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
