"""SQLite persistence only. No network or WeChat sending."""

import sqlite3
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from hashlib import sha256
from secrets import token_hex
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from wechat_ai_bot.migrations import automatic_schema_ready
from wechat_ai_bot.reply_safety import candidate_is_fresh, utc_now
from wechat_ai_bot.outbound_safety import _timestamp, dispatch_block_reason
from wechat_ai_bot.sender_automatic_policy import AutomaticReplyPolicy
from wechat_ai_bot.sender_http_contract import SendContractError, encode_text_send_body


@dataclass(frozen=True, repr=False)
class AutomaticDispatch:
    outbox_id: str
    decision_id: str
    account_id: str
    thread_id: str
    text: str
    idempotency_key: str
    request_sha256: str
    policy_digest: str
    remote_send_id: str | None


class _AutomaticClaimDenied(Exception):
    """Roll back a provisional decision/Outbox reservation on policy denial."""


class Storage:
    def __init__(self, path, consumer_name="wechat-ai-bot-v4"):
        self.consumer_name = consumer_name
        self.conn = sqlite3.connect(
            str(path), timeout=5, isolation_level=None
        )
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA busy_timeout=5000")
        self.conn.execute("PRAGMA synchronous=FULL")

        version = self.conn.execute(
            "PRAGMA user_version"
        ).fetchone()[0]

        self.automatic_schema = version == 5 and automatic_schema_ready(self.conn)
        if version != 4 and not self.automatic_schema:
            self.close()
            raise ValueError("Requires SQLite schema v4 or a complete v5 schema")

        columns = {
            row[1] for row in self.conn.execute(
                "PRAGMA table_info(decisions)"
            ).fetchall()
        }
        if "reply" not in columns:
            self.close()
            raise ValueError("SQLite schema v4 requires decisions.reply")

        task_tables = {
            row[0] for row in self.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        if "scheduled_tasks" not in task_tables:
            self.close()
            raise ValueError("SQLite schema v4 requires scheduled_tasks")

    def close(self):
        self.conn.close()

    @contextmanager
    def transaction(self):
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.conn.commit()
        except BaseException:
            self.conn.rollback()
            raise

    def checkpoint(self, account_id):
        row = self.conn.execute(
            "SELECT last_durable_cursor, last_acked_cursor "
            "FROM consumer_state "
            "WHERE consumer_name=? AND account_id=?",
            (self.consumer_name, account_id),
        ).fetchone()
        return dict(row) if row else None

    @staticmethod
    def _cursor_number(cursor):
        """Validate the Agent WeChat SSE cursor and return its numeric value."""
        if (
            not isinstance(cursor, str)
            or not cursor
            or not cursor.isascii()
            or not cursor.isdigit()
            or len(cursor) > 19
        ):
            raise ValueError("Invalid numeric SSE cursor")

        number = int(cursor)
        if number > 9_223_372_036_854_775_807:
            raise ValueError("SSE cursor exceeds SQLite INTEGER range")
        return number

    @staticmethod
    def _optional_text(name, value):
        if value is not None and not isinstance(value, str):
            raise ValueError(f"Invalid {name}")
        return value

    def persist_event(
        self, account_id, cursor, kind,
        thread_id=None, message_id=None
    ):
        if not all(
            isinstance(value, str) and value
            for value in (account_id, kind)
        ):
            raise ValueError("Invalid event identity")

        cursor_number = self._cursor_number(cursor)
        self._optional_text("thread_id", thread_id)
        self._optional_text("message_id", message_id)

        with self.transaction():
            existing = self.conn.execute(
                "SELECT kind,thread_id,message_id "
                "FROM events WHERE account_id=? AND cursor=?",
                (account_id, cursor),
            ).fetchone()

            if existing is not None:
                if tuple(existing) != (kind, thread_id, message_id):
                    raise ValueError(
                        "Conflicting event for existing cursor"
                    )

                state = self.checkpoint(account_id)
                if state is None:
                    raise RuntimeError(
                        "Existing event has no consumer checkpoint"
                    )

                durable_number = self._cursor_number(
                    state["last_durable_cursor"]
                )
                if cursor_number > durable_number:
                    raise RuntimeError(
                        "Existing event is ahead of durable checkpoint"
                    )

                # Exact replay is idempotent and must not move the cursor.
                return False

            state = self.checkpoint(account_id)
            if state is not None:
                durable_number = self._cursor_number(
                    state["last_durable_cursor"]
                )
                if cursor_number <= durable_number:
                    raise ValueError(
                        "New event cursor must advance beyond durable cursor"
                    )

                acked_cursor = state["last_acked_cursor"]
                if acked_cursor is not None:
                    acked_number = self._cursor_number(acked_cursor)
                    if acked_number > durable_number:
                        raise RuntimeError(
                            "ACK checkpoint is ahead of durable checkpoint"
                        )

            self.conn.execute(
                "INSERT INTO events "
                "(account_id,cursor,kind,thread_id,message_id) "
                "VALUES(?,?,?,?,?)",
                (account_id, cursor, kind, thread_id, message_id),
            )

            self.conn.execute(
                "INSERT INTO consumer_state "
                "(consumer_name,account_id,last_durable_cursor) "
                "VALUES(?,?,?) "
                "ON CONFLICT(consumer_name,account_id) "
                "DO UPDATE SET "
                "last_durable_cursor=excluded.last_durable_cursor, "
                "updated_at=CURRENT_TIMESTAMP",
                (self.consumer_name, account_id, cursor),
            )

        return True

    def mark_acked(self, account_id, cursor):
        """
        Record local ACK only AFTER the remote ACK succeeds.
        This method does not make a network request.
        """
        cursor_number = self._cursor_number(cursor)

        with self.transaction():
            state = self.checkpoint(account_id)

            if state is None:
                raise ValueError("ACK has no consumer checkpoint")

            durable_number = self._cursor_number(
                state["last_durable_cursor"]
            )
            if state["last_durable_cursor"] != cursor:
                raise ValueError(
                    "ACK must match newest durable cursor"
                )

            last_acked = state["last_acked_cursor"]
            if last_acked is not None:
                acked_number = self._cursor_number(last_acked)
                if acked_number > durable_number:
                    raise RuntimeError(
                        "ACK checkpoint is ahead of durable checkpoint"
                    )
                if cursor_number < acked_number:
                    raise ValueError("ACK cursor cannot move backwards")

            if last_acked == cursor:
                return False

            self.conn.execute(
                "UPDATE consumer_state "
                "SET last_acked_cursor=?, "
                "updated_at=CURRENT_TIMESTAMP "
                "WHERE consumer_name=? AND account_id=?",
                (cursor, self.consumer_name, account_id),
            )

        return True

    def persist_recovery_checkpoint(
        self, account_id, cursor, thread_id, message_ids
    ):
        """Commit a verified history-recovery boundary without inventing events.

        Recovered messages are assigned the snapshot's asOfEventCursor as a
        context boundary, not as their original per-message event cursor. They
        are marked ignored so the bot never replies late to historical items.
        """
        if not isinstance(account_id, str) or not account_id:
            raise ValueError("Invalid recovery account")
        if not isinstance(thread_id, str) or not thread_id:
            raise ValueError("Invalid recovery thread")

        cursor_number = self._cursor_number(cursor)
        if not isinstance(message_ids, (list, tuple)):
            raise ValueError("Invalid recovered message IDs")
        if any(not isinstance(mid, str) or not mid for mid in message_ids):
            raise ValueError("Invalid recovered message ID")
        if len(set(message_ids)) != len(message_ids):
            raise ValueError("Duplicate recovered message ID")

        updated_messages = 0
        with self.transaction():
            state = self.checkpoint(account_id)
            if state is None:
                raise RuntimeError("Recovery requires an existing checkpoint")

            durable_number = self._cursor_number(state["last_durable_cursor"])
            if cursor_number <= durable_number:
                raise ValueError("Recovery cursor must advance durable cursor")

            acked = state["last_acked_cursor"]
            if acked is not None:
                acked_number = self._cursor_number(acked)
                if acked_number > durable_number:
                    raise RuntimeError("ACK checkpoint is ahead of durable cursor")

            collision = self.conn.execute(
                "SELECT kind,thread_id,message_id FROM events "
                "WHERE account_id=? AND cursor=?",
                (account_id, cursor),
            ).fetchone()
            if collision is not None:
                raise ValueError("Recovery checkpoint cursor already exists")

            for message_id in message_ids:
                row = self.conn.execute(
                    "SELECT thread_id,source_cursor,status FROM messages "
                    "WHERE account_id=? AND message_id=?",
                    (account_id, message_id),
                ).fetchone()
                if row is None:
                    raise RuntimeError(
                        f"Recovered message was not persisted: {message_id}"
                    )
                if row["thread_id"] != thread_id:
                    raise ValueError("Recovered message belongs to another thread")

                source_cursor = row["source_cursor"]
                if source_cursor is None:
                    self.conn.execute(
                        "UPDATE messages SET source_cursor=?, "
                        "status=CASE WHEN status='pending' THEN 'ignored' "
                        "ELSE status END "
                        "WHERE account_id=? AND message_id=? "
                        "AND source_cursor IS NULL",
                        (cursor, account_id, message_id),
                    )
                    updated_messages += 1
                else:
                    source_number = self._cursor_number(source_cursor)
                    if source_number > cursor_number:
                        raise ValueError(
                            "Recovered message cursor exceeds snapshot boundary"
                        )

            # This is an explicit local boundary marker, not a fabricated
            # message.committed event. It is durable before the remote ACK.
            self.conn.execute(
                "INSERT INTO events "
                "(account_id,cursor,kind,thread_id,message_id,status) "
                "VALUES(?,?,'recovery.checkpoint',NULL,NULL,'processed')",
                (account_id, cursor),
            )
            self.conn.execute(
                "UPDATE consumer_state SET last_durable_cursor=?, "
                "updated_at=CURRENT_TIMESTAMP "
                "WHERE consumer_name=? AND account_id=?",
                (cursor, self.consumer_name, account_id),
            )

        return updated_messages

    def persist_message(
        self, account_id, message_id, thread_id, *,
        direction, body, sender_id=None,
        message_type=None, mentions_self=None,
        created_at=None, source_cursor=None
    ):
        """
        Insert or safely enrich one message.

        Returns True when a row is inserted or enriched, False for an exact
        replay. Existing non-NULL values are immutable: conflicting values
        raise ValueError and are never silently overwritten.
        """
        if not all(
            isinstance(value, str) and value
            for value in (account_id, message_id, thread_id)
        ):
            raise ValueError("Missing message identity")

        if direction not in ("incoming", "outgoing", None):
            raise ValueError("Invalid direction")
        if body is not None and not isinstance(body, str):
            raise ValueError("Invalid body")
        # Empty/whitespace-only text is not useful message content. Treat it
        # as unknown so a later hydration can enrich legacy blank rows.
        if isinstance(body, str) and not body.strip():
            body = None
        if (
            mentions_self is not None
            and type(mentions_self) is not bool
        ):
            raise ValueError("Invalid mention flag")
        if message_type is not None and type(message_type) is not int:
            raise ValueError("Invalid message_type")
        self._optional_text("sender_id", sender_id)
        self._optional_text("created_at", created_at)
        if source_cursor is not None:
            self._cursor_number(source_cursor)

        incoming = {
            "sender_id": sender_id,
            "direction": direction,
            "message_type": message_type,
            "body": body,
            "mentions_self": (
                None if mentions_self is None else int(mentions_self)
            ),
            "created_at": created_at,
            "source_cursor": source_cursor,
        }

        with self.transaction():
            existing = self.conn.execute(
                "SELECT * FROM messages "
                "WHERE account_id=? AND message_id=?",
                (account_id, message_id),
            ).fetchone()

            if existing is None:
                self.conn.execute(
                    "INSERT INTO messages "
                    "(account_id,message_id,thread_id,sender_id,"
                    "direction,message_type,body,mentions_self,"
                    "created_at,source_cursor) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (
                        account_id, message_id, thread_id,
                        sender_id, direction, message_type,
                        body,
                        None if mentions_self is None else int(mentions_self),
                        created_at, source_cursor,
                    ),
                )
                return True

            if existing["thread_id"] != thread_id:
                raise ValueError("Conflicting message field: thread_id")

            updates = {}
            for field, new_value in incoming.items():
                old_value = existing[field]

                # Older hydrators may have persisted "" or whitespace for
                # messages whose text was not yet available. Treat those
                # legacy body values as unknown, but keep all nonblank
                # existing values immutable.
                if (
                    field == "body"
                    and isinstance(old_value, str)
                    and not old_value.strip()
                ):
                    old_value = None

                if old_value is None:
                    if new_value is not None:
                        updates[field] = new_value
                elif new_value is not None and old_value != new_value:
                    raise ValueError(
                        f"Conflicting message field: {field}"
                    )

            if not updates:
                return False

            assignments = ", ".join(
                f"{field}=?" for field in updates
            )
            values = list(updates.values())
            values.extend((account_id, message_id))

            self.conn.execute(
                f"UPDATE messages SET {assignments} "
                "WHERE account_id=? AND message_id=?",
                values,
            )

        return True

    def message(self, account_id, message_id):
        row = self.conn.execute(
            "SELECT * FROM messages "
            "WHERE account_id=? AND message_id=?",
            (account_id, message_id),
        ).fetchone()
        return dict(row) if row else None

    @staticmethod
    def _validate_decision(action, reply):
        if action not in ("reply", "skip"):
            raise ValueError("Unsupported message decision")

        if action == "reply":
            if not isinstance(reply, str) or not reply.strip():
                raise ValueError("Reply decision requires non-empty reply")
            if len(reply.encode("utf-8")) > 16 * 1024:
                raise ValueError("Reply exceeds 16 KiB")
        elif reply is not None:
            raise ValueError("Skip decision cannot contain reply")

    @staticmethod
    def _decision_id(account_id, message_id):
        return "dec-" + uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"wechat-ai-bot/{account_id}/{message_id}"
        ).hex

    def _insert_decision(
        self, account_id, message_id, action,
        reason, model, reply, status="proposed"
    ):
        decision_id = self._decision_id(account_id, message_id)
        cur = self.conn.execute(
            "INSERT INTO decisions "
            "(decision_id,account_id,source_message_id,"
            "action,reason,model,status,reply) "
            "VALUES(?,?,?,?,?,?,?,?) "
            "ON CONFLICT(account_id,source_message_id) DO NOTHING",
            (
                decision_id, account_id, message_id,
                action, reason, model, status, reply,
            ),
        )
        return decision_id, cur.rowcount == 1

    def decide(
        self, account_id, message_id,
        action, reason="", model=None, reply=None
    ):
        """Persist one decision. Does not update message state or send."""
        self._validate_decision(action, reply)
        status = "proposed" if action == "reply" else "rejected"
        with self.transaction():
            return self._insert_decision(
                account_id, message_id, action,
                reason, model, reply, status,
            )

    def decide_and_process(
        self, account_id, message_id, *,
        action, reason, model=None, reply=None,
        decision_status="proposed"
    ):
        """Atomically persist a decision and mark its source message processed."""
        self._validate_decision(action, reply)

        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("Decision reason must be non-empty")

        if decision_status not in ("proposed", "rejected"):
            raise ValueError("Invalid decision status")

        with self.transaction():
            decision_id, created = self._insert_decision(
                account_id, message_id, action,
                reason, model, reply, decision_status,
            )
            if not created:
                raise RuntimeError("Decision already exists")

            updated = self.conn.execute(
                "UPDATE messages SET status='processed' "
                "WHERE account_id=? AND message_id=? AND status='pending'",
                (account_id, message_id),
            )
            if updated.rowcount != 1:
                raise RuntimeError("Source message is not pending")

        return decision_id, True

    def decide_conversation_and_process(
        self, account_id, thread_id, activation_cursor, anchor_id, *,
        action, reason, model=None, reply=None,
        decision_status="proposed", coalesced=(),
    ):
        """Atomically decide one conversation and retire covered pending rows.

        Each source gets a real decision. Nothing is written to outbox.
        All participant identities, cursors, and pending states are checked
        inside the same transaction; conflicts roll the *whole* batch back.
        """
        self._validate_decision(action, reply)
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("Decision reason must be non-empty")
        if decision_status != ("proposed" if action == "reply" else "rejected"):
            raise ValueError("Invalid decision status")
        boundary = self._cursor_number(activation_cursor)
        if not all(isinstance(x, str) and x for x in (
            account_id, thread_id, anchor_id,
        )):
            raise ValueError("Invalid conversation identity")
        if not isinstance(coalesced, (list, tuple)) or len(coalesced) > 199:
            raise ValueError("Invalid conversation batch size")
        entries = [(anchor_id, action, reason, model, reply, decision_status)]
        seen = {anchor_id}
        for entry in coalesced:
            if (
                not isinstance(entry, (tuple, list)) or len(entry) != 2
                or not isinstance(entry[0], str) or not entry[0]
                or entry[0] in seen
                or entry[1] not in (
                    "conversation_window_coalesced", "superseded_by_newer_context"
                )
            ):
                raise ValueError("Invalid coalesced conversation member")
            seen.add(entry[0])
            entries.append((entry[0], "skip", entry[1], None, None, "rejected"))

        with self.transaction():
            rows = {}
            for message_id, *_ in entries:
                row = self.conn.execute(
                    "SELECT thread_id,direction,status,source_cursor FROM messages "
                    "WHERE account_id=? AND message_id=?",
                    (account_id, message_id),
                ).fetchone()
                if (
                    row is None or row["thread_id"] != thread_id
                    or row["direction"] != "incoming"
                    or row["status"] != "pending"
                    or row["source_cursor"] is None
                ):
                    raise RuntimeError("Conversation member is not eligible")
                cursor = self._cursor_number(row["source_cursor"])
                if cursor <= boundary:
                    raise RuntimeError("Conversation member precedes activation")
                rows[message_id] = cursor
            anchor_cursor = rows[anchor_id]
            if any(cursor > anchor_cursor for cursor in rows.values()):
                raise RuntimeError("Conversation member follows anchor")

            decision_id = None
            for message_id, item_action, item_reason, item_model, item_reply, status in entries:
                item_id, created = self._insert_decision(
                    account_id, message_id, item_action,
                    item_reason, item_model, item_reply, status,
                )
                if not created:
                    raise RuntimeError("Conversation member was already decided")
                updated = self.conn.execute(
                    "UPDATE messages SET status='processed' "
                    "WHERE account_id=? AND message_id=? AND status='pending'",
                    (account_id, message_id),
                )
                if updated.rowcount != 1:
                    raise RuntimeError("Conversation member state changed")
                if message_id == anchor_id:
                    decision_id = item_id
        return decision_id, len(entries)

    def accept_decision(self, decision_id, *, now=None):
        """Accept only a fresh candidate; old proposed rows remain auditable."""
        current = utc_now(now)
        with self.transaction():
            row = self.conn.execute(
                "SELECT d.created_at AS decided_at, "
                "m.created_at AS message_at "
                "FROM decisions d JOIN messages m "
                "ON m.account_id=d.account_id "
                "AND m.message_id=d.source_message_id "
                "WHERE d.decision_id=? AND d.action='reply' "
                "AND d.status='proposed' AND d.reply IS NOT NULL "
                "AND TRIM(d.reply)!=''",
                (decision_id,),
            ).fetchone()
            if row is None or not candidate_is_fresh(
                row["message_at"], row["decided_at"], now=current
            ):
                return False
            cur = self.conn.execute(
                "UPDATE decisions SET status='accepted' "
                "WHERE decision_id=? AND action='reply' "
                "AND status='proposed' AND reply IS NOT NULL "
                "AND TRIM(reply) != ''",
                (decision_id,),
            )
        return cur.rowcount == 1


    def ensure_daily_checkin_task(
        self, account_id, thread_id, local_date,
        timezone_name="Asia/Shanghai"
    ):
        """Create the unique 09:00 Asia/Shanghai check-in task for a local day."""
        if not all(
            isinstance(value, str) and value.strip()
            for value in (account_id, thread_id, local_date)
        ):
            raise ValueError("Missing scheduled task identity")
        if timezone_name != "Asia/Shanghai":
            raise ValueError("Daily check-in timezone is fixed to Asia/Shanghai")

        try:
            day = date.fromisoformat(local_date)
            if day.isoformat() != local_date:
                raise ValueError("local_date must use YYYY-MM-DD")
            zone = ZoneInfo(timezone_name)
        except (ValueError, ZoneInfoNotFoundError) as exc:
            raise ValueError("Invalid daily check-in date/timezone") from exc

        scheduled_at = datetime.combine(
            day, time(hour=9, minute=0), tzinfo=zone
        ).isoformat(timespec="seconds")
        task_id = "task-" + uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"wechat-ai-bot/daily-checkin/{account_id}/{thread_id}/{local_date}/{timezone_name}",
        ).hex
        idempotency_key = str(uuid.uuid5(
            uuid.NAMESPACE_URL, f"checkin-send/{task_id}"
        ))

        with self.transaction():
            cur = self.conn.execute(
                "INSERT INTO scheduled_tasks "
                "(task_id,task_type,account_id,thread_id,local_date,"
                "timezone,scheduled_at,content,idempotency_key) "
                "VALUES(?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(task_type,account_id,thread_id,local_date) DO NOTHING",
                (
                    task_id, "daily_checkin", account_id, thread_id,
                    local_date, timezone_name, scheduled_at, "签到",
                    idempotency_key,
                ),
            )
            row = self.conn.execute(
                "SELECT task_id,timezone,scheduled_at,content,idempotency_key "
                "FROM scheduled_tasks "
                "WHERE task_type='daily_checkin' AND account_id=? "
                "AND thread_id=? AND local_date=?",
                (account_id, thread_id, local_date),
            ).fetchone()
            if row is None or tuple(row) != (
                task_id, timezone_name, scheduled_at, "签到", idempotency_key
            ):
                raise ValueError("Conflicting existing daily check-in task")

        return task_id, cur.rowcount == 1

    def expire_stale_scheduled_tasks(
        self, account_id, thread_id, current_local_date
    ):
        """Expire scheduled/queued check-ins from earlier local dates."""
        day = date.fromisoformat(current_local_date)
        if day.isoformat() != current_local_date:
            raise ValueError("current_local_date must use YYYY-MM-DD")

        expired = 0
        with self.transaction():
            rows = self.conn.execute(
                "SELECT task_id,status FROM scheduled_tasks "
                "WHERE account_id=? AND thread_id=? "
                "AND local_date<? AND status IN ('scheduled','queued')",
                (account_id, thread_id, current_local_date),
            ).fetchall()
            for row in rows:
                if row["status"] == "queued":
                    outbox = self.conn.execute(
                        "SELECT status FROM outbox WHERE scheduled_task_id=?",
                        (row["task_id"],),
                    ).fetchone()
                    if outbox and outbox["status"] == "sending":
                        # Do not race an in-flight send at the day boundary.
                        continue
                    if outbox and outbox["status"] == "sent":
                        self.conn.execute(
                            "UPDATE scheduled_tasks SET status='sent' "
                            "WHERE task_id=? AND status='queued'",
                            (row["task_id"],),
                        )
                        continue
                    if outbox and outbox["status"] in ('failed','needs_review'):
                        self.conn.execute(
                            "UPDATE scheduled_tasks SET status='needs_review' "
                            "WHERE task_id=? AND status='queued'",
                            (row["task_id"],),
                        )
                        continue
                    self.conn.execute(
                        "UPDATE outbox SET status='cancelled' "
                        "WHERE scheduled_task_id=? AND status='queued'",
                        (row["task_id"],),
                    )
                self.conn.execute(
                    "UPDATE scheduled_tasks SET status='expired' "
                    "WHERE task_id=? AND status IN ('scheduled','queued')",
                    (row["task_id"],),
                )
                expired += 1
        return expired

    def enqueue_scheduled_task(self, task_id, now=None):
        """Queue a due scheduled task. This method never sends to WeChat."""
        current = now or datetime.now(timezone.utc)
        if not isinstance(current, datetime) or current.tzinfo is None:
            raise ValueError("now must be a timezone-aware datetime")

        with self.transaction():
            task = self.conn.execute(
                "SELECT * FROM scheduled_tasks WHERE task_id=?",
                (task_id,),
            ).fetchone()
            if task is None:
                raise ValueError("Scheduled task not found")

            if task["status"] == "queued":
                prior = self.conn.execute(
                    "SELECT outbox_id FROM outbox WHERE scheduled_task_id=?",
                    (task_id,),
                ).fetchone()
                if prior is None:
                    raise RuntimeError("Queued task has no Outbox row")
                return prior["outbox_id"], False, "queued"

            if task["status"] != "scheduled":
                return None, False, task["status"]

            try:
                zone = ZoneInfo(task["timezone"])
                scheduled_at = datetime.fromisoformat(task["scheduled_at"])
                local_day = date.fromisoformat(task["local_date"])
            except (ValueError, ZoneInfoNotFoundError) as exc:
                raise RuntimeError("Stored scheduled task time is invalid") from exc

            if scheduled_at.tzinfo is None:
                raise RuntimeError("Stored scheduled_at must include timezone")

            local_now = current.astimezone(zone)
            if local_now.date() > local_day:
                self.conn.execute(
                    "UPDATE scheduled_tasks SET status='expired' "
                    "WHERE task_id=? AND status='scheduled'",
                    (task_id,),
                )
                return None, False, "expired"

            if local_now.date() < local_day or current < scheduled_at:
                return None, False, "not_due"

            outbox_id = "out-" + uuid.uuid5(
                uuid.NAMESPACE_URL, f"outbox/scheduled/{task_id}"
            ).hex
            self.conn.execute(
                "INSERT INTO outbox "
                "(outbox_id,decision_id,scheduled_task_id,account_id,"
                "thread_id,content,idempotency_key) "
                "VALUES(?,NULL,?,?,?,?,?)",
                (
                    outbox_id, task_id, task["account_id"],
                    task["thread_id"], task["content"],
                    task["idempotency_key"],
                ),
            )
            updated = self.conn.execute(
                "UPDATE scheduled_tasks SET status='queued' "
                "WHERE task_id=? AND status='scheduled'",
                (task_id,),
            )
            if updated.rowcount != 1:
                raise RuntimeError("Scheduled task changed concurrently")

        return outbox_id, True, "queued"

    def enqueue(
        self, decision_id, account_id,
        thread_id, content, *, now=None
    ):
        """Queue only a fresh accepted reply; this never sends a message."""
        current = utc_now(now)
        if not isinstance(content, str) or not content.strip():
            raise ValueError("Empty content")

        if len(content.encode("utf-8")) > 16 * 1024:
            raise ValueError("Content exceeds 16 KiB")

        outbox_id = "out-" + uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"outbox/{decision_id}"
        ).hex

        key = str(uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"send/{decision_id}"
        ))

        with self.transaction():
            decision = self.conn.execute(
                "SELECT d.account_id,d.action,d.status,d.reply,"
                "d.created_at AS decided_at,m.created_at AS message_at,"
                "m.thread_id "
                "FROM decisions d "
                "LEFT JOIN messages m "
                "ON m.account_id=d.account_id "
                "AND m.message_id=d.source_message_id "
                "WHERE d.decision_id=?",
                (decision_id,),
            ).fetchone()

            if (
                not decision
                or decision["status"] != "accepted"
                or decision["action"] != "reply"
            ):
                raise ValueError("Decision is not an accepted reply")

            if (
                decision["account_id"] != account_id
                or decision["thread_id"] != thread_id
            ):
                raise ValueError("Reply target mismatch")

            if decision["reply"] != content:
                raise ValueError(
                    "Outbox content differs from saved decision reply"
                )

            # Preserve retry/idempotency for an ALREADY queued item.
            # Do not allow an old accepted decision to create new Outbox work.
            prior = self.conn.execute(
                "SELECT outbox_id,account_id,thread_id,content "
                "FROM outbox WHERE decision_id=?",
                (decision_id,),
            ).fetchone()
            if prior is not None:
                if tuple(prior[k] for k in (
                    "account_id", "thread_id", "content"
                )) != (account_id, thread_id, content):
                    raise ValueError("Conflicting existing outbox item")
                return prior["outbox_id"], False

            if not candidate_is_fresh(
                decision["message_at"], decision["decided_at"], now=current
            ):
                raise ValueError("Reply candidate expired or has invalid timestamp")

            cur = self.conn.execute(
                "INSERT INTO outbox "
                "(outbox_id,decision_id,account_id,"
                "thread_id,content,idempotency_key) "
                "VALUES(?,?,?,?,?,?) "
                "ON CONFLICT(decision_id) DO NOTHING",
                (
                    outbox_id, decision_id, account_id,
                    thread_id, content, key,
                ),
            )

            if cur.rowcount == 0:
                prior = self.conn.execute(
                    "SELECT account_id,thread_id,content "
                    "FROM outbox WHERE decision_id=?",
                    (decision_id,),
                ).fetchone()

                if tuple(prior) != (
                    account_id, thread_id, content
                ):
                    raise ValueError(
                        "Conflicting existing outbox item"
                    )

        return outbox_id, cur.rowcount == 1

    def claim_outbox_for_send(self, outbox_id, config, *, now=None):
        """Atomically reserve a safe dispatch; NEVER calls the WeChat API.

        A future Sender must exclusively use this method, then perform
        additional authorization immediately before the remote call. On
        uncertain delivery it must stop and require manual review; never
        retry a sending record automatically.
        """
        current = utc_now(now)
        with self.transaction():
            row = self.conn.execute(
                "SELECT * FROM outbox WHERE outbox_id=?", (outbox_id,)
            ).fetchone()
            if row is None:
                return False, "outbox_missing"
            reason = self._reserve_outbox(row, config, current)
            if reason is not None:
                return False, reason
        return True, "claimed_without_network_send"

    def _reserve_outbox(self, row, config, current, *, automatic_policy=None):
        reason = dispatch_block_reason(
            self.conn, row, config, current, automatic_policy=automatic_policy,
        )
        if reason is not None:
            return reason
        changed = self.conn.execute(
            "UPDATE outbox SET status='sending' WHERE outbox_id=? AND status='queued'",
            (row["outbox_id"],),
        )
        if changed.rowcount != 1:
            raise RuntimeError("Outbox claim lost atomic transition")
        return None

    def _newest_automatic_decision(self, policy):
        return self.conn.execute(
            "SELECT d.decision_id,d.action,d.status,d.reply,d.created_at AS decided_at,"
            "m.created_at AS message_at,m.source_cursor FROM decisions d JOIN messages m "
            "ON m.account_id=d.account_id AND m.message_id=d.source_message_id "
            "JOIN events e ON e.account_id=m.account_id AND e.cursor=m.source_cursor "
            "AND e.message_id=m.message_id AND e.thread_id=m.thread_id AND e.kind='message.committed' "
            "WHERE m.account_id=? AND m.thread_id=? AND m.direction='incoming' AND m.status='processed' "
            "AND CAST(CAST(m.source_cursor AS INTEGER) AS TEXT)=m.source_cursor "
            "AND CAST(m.source_cursor AS INTEGER)>? "
            "ORDER BY CAST(m.source_cursor AS INTEGER) DESC,d.decision_id DESC LIMIT 1",
            (policy.account_id, policy.thread_id, policy.activation_cursor),
        ).fetchone()

    def automatic_dispatch(self, outbox_id):
        """Read the persisted binding without exposing it via repr or logs."""
        if not self.automatic_schema:
            return None
        row = self.conn.execute(
            "SELECT o.outbox_id,o.decision_id,o.account_id,o.thread_id,o.content AS text,"
            "o.idempotency_key,a.request_sha256,a.policy_digest,o.remote_send_id "
            "FROM outbox o JOIN automatic_dispatch a USING(outbox_id) WHERE o.outbox_id=?",
            (outbox_id,),
        ).fetchone()
        return AutomaticDispatch(**row) if row is not None else None

    def unresolved_automatic_dispatch(self, account_id, thread_id):
        if not self.automatic_schema:
            return None
        row = self.conn.execute(
            "SELECT o.outbox_id FROM outbox o JOIN automatic_dispatch a USING(outbox_id) "
            "WHERE o.account_id=? AND o.thread_id=? AND o.status IN ('sending','needs_review') "
            "ORDER BY a.claimed_at,o.outbox_id LIMIT 1", (account_id, thread_id),
        ).fetchone()
        return self.automatic_dispatch(row["outbox_id"]) if row else None

    def claim_automatic_reply(self, config, policy: AutomaticReplyPolicy, *, now=None):
        """Materialize and reserve only the newest eligible AI reply under policy."""
        current = utc_now(now)
        if not self.automatic_schema:
            return None, "automatic_schema_required"
        if type(policy) is not AutomaticReplyPolicy or not policy.permits(config, current):
            return None, "automatic_policy_inactive"
        try:
            with self.transaction():
                decision = self._newest_automatic_decision(policy)
                if decision is None or decision["action"] != "reply" or decision["status"] != "proposed":
                    return None, "no_automatic_candidate"
                if (not candidate_is_fresh(decision["message_at"], decision["decided_at"], now=current)
                        or _timestamp(decision["message_at"]).timestamp() < policy.starts_at):
                    return None, "reply_expired_or_predates_policy"
                try:
                    body = encode_text_send_body(thread_id=policy.thread_id, text=decision["reply"])
                except SendContractError:
                    return None, "reply_content_invalid"
                decision_id = decision["decision_id"]
                outbox_id = "out-" + uuid.uuid5(uuid.NAMESPACE_URL, f"outbox/{decision_id}").hex
                self.conn.execute("UPDATE decisions SET status='accepted' WHERE decision_id=?", (decision_id,))
                self.conn.execute(
                    "INSERT INTO outbox(outbox_id,decision_id,account_id,thread_id,content,idempotency_key) "
                    "VALUES(?,?,?,?,?,?)",
                    (outbox_id, decision_id, policy.account_id, policy.thread_id, decision["reply"],
                     "send-" + token_hex(16)),
                )
                row = self.conn.execute("SELECT * FROM outbox WHERE outbox_id=?", (outbox_id,)).fetchone()
                reason = self._reserve_outbox(row, config, current, automatic_policy=policy)
                if reason is not None:
                    raise _AutomaticClaimDenied(reason)
                self.conn.execute(
                    "INSERT INTO automatic_dispatch(outbox_id,policy_digest,policy_generation,source_cursor,"
                    "request_sha256,claimed_at) VALUES(?,?,?,?,?,?)",
                    (outbox_id, policy.digest, policy.generation, decision["source_cursor"],
                     sha256(body).hexdigest(), current.isoformat()),
                )
                claim = self.automatic_dispatch(outbox_id)
            return claim, "claimed_without_network_send"
        except _AutomaticClaimDenied as exc:
            return None, str(exc)

    def recheck_automatic_reply(self, claim: AutomaticDispatch, config, policy, *, now=None):
        """Refresh policy and admission checks just before one dispatch attempt."""
        current = utc_now(now)
        if type(policy) is not AutomaticReplyPolicy or policy.digest != claim.policy_digest:
            return "automatic_policy_changed"
        with self.transaction():
            if self.automatic_dispatch(claim.outbox_id) != claim:
                return "automatic_claim_changed"
            decision = self._newest_automatic_decision(policy)
            if decision is None or decision["decision_id"] != claim.decision_id:
                return "newer_reply_preferred"
            row = self.conn.execute("SELECT * FROM outbox WHERE outbox_id=?", (claim.outbox_id,)).fetchone()
            return dispatch_block_reason(self.conn, row, config, current, automatic_policy=policy, recheck=True)

    def finish_automatic_reply(self, claim, status, *, remote_send_id=None, now=None):
        """Persist a correlated result or quarantine without allowing another attempt."""
        if status not in {"sent", "needs_review", "cancelled"}:
            raise ValueError("Invalid automatic result")
        if status == "sent" and not remote_send_id:
            raise ValueError("Missing verified remote send ID")
        if claim.remote_send_id is not None and remote_send_id not in (None, claim.remote_send_id):
            return False
        timestamp = utc_now(now).isoformat() if status == "sent" else None
        with self.transaction():
            if self.automatic_dispatch(claim.outbox_id) != claim:
                return False
            cursor = self.conn.execute(
                "UPDATE outbox SET status=?,remote_send_id=COALESCE(?,remote_send_id),sent_at=? "
                "WHERE outbox_id=? AND (status='sending' OR (status='needs_review' AND ?!='cancelled'))",
                (status, remote_send_id, timestamp, claim.outbox_id, status),
            )
            return cursor.rowcount == 1

    def record_send_result(self, outbox_id, idempotency_key, outcome,
                           *, remote_send_id=None, now=None):
        """Persist a *verified* result for a previously claimed outbox item.

        This is a DB-only primitive. It performs no network request and is
        intentionally not wired to a running service. An actual Sender must
        verify the remote receipt is for the exact idempotency key, account,
        target and body before passing it here. All ambiguous outcomes are
        quarantined rather than retried.
        """
        current = utc_now(now)
        if not isinstance(outbox_id, str) or not outbox_id:
            raise ValueError("Invalid outbox identity")
        if not isinstance(idempotency_key, str) or not idempotency_key:
            raise ValueError("Invalid idempotency key")
        if outcome not in ("accepted", "failed", "needs_review"):
            raise ValueError("Unsupported remote send outcome")
        if remote_send_id is not None and (
            not isinstance(remote_send_id, str) or not remote_send_id.strip()
        ):
            raise ValueError("Invalid remote send identifier")
        if outcome == "accepted" and not remote_send_id:
            raise ValueError("Accepted result requires verified remote send id")
        with self.transaction():
            if self.automatic_dispatch(outbox_id) is not None:
                return False, "automatic_result_verification_required"
            row = self.conn.execute(
                "SELECT status,idempotency_key,remote_send_id,scheduled_task_id "
                "FROM outbox WHERE outbox_id=?", (outbox_id,)
            ).fetchone()
            if row is None:
                return False, "outbox_missing"
            if row["idempotency_key"] != idempotency_key:
                return False, "idempotency_key_mismatch"
            if row["status"] != "sending":
                return False, "outbox_not_sending"
            if row["remote_send_id"] is not None:
                return False, "remote_id_already_recorded"

            terminal = "sent" if outcome == "accepted" else "needs_review"
            timestamp = current.isoformat() if terminal == "sent" else None
            result = self.conn.execute(
                "UPDATE outbox SET status=?,remote_send_id=?,sent_at=? "
                "WHERE outbox_id=? AND status='sending'",
                (terminal, remote_send_id, timestamp, outbox_id),
            )
            if result.rowcount != 1:
                raise RuntimeError("Outbox result transition lost atomicity")
            if row["scheduled_task_id"] is not None:
                changed = self.conn.execute(
                    "UPDATE scheduled_tasks SET status=? "
                    "WHERE task_id=? AND status='queued'",
                    ("sent" if terminal == "sent" else "needs_review",
                     row["scheduled_task_id"]),
                )
                if changed.rowcount != 1:
                    raise RuntimeError("Scheduled task transition failed")
        return True, terminal

    def quarantine_unresolved_send(self, outbox_id):
        """Fail closed after crash/timeout; NEVER resend or query the network.

        Actual recovery should first look up the remote send with the persisted
        idempotency key, and call record_send_result if it can verify the
        outcome. Use this for unresolved sends only.
        """
        if not isinstance(outbox_id, str) or not outbox_id:
            raise ValueError("Invalid outbox identity")
        with self.transaction():
            row = self.conn.execute(
                "SELECT status,scheduled_task_id FROM outbox WHERE outbox_id=?",
                (outbox_id,),
            ).fetchone()
            if row is None:
                return False, "outbox_missing"
            if row["status"] != "sending":
                return False, "outbox_not_sending"
            self.conn.execute(
                "UPDATE outbox SET status='needs_review' WHERE outbox_id=?",
                (outbox_id,),
            )
            if row["scheduled_task_id"] is not None:
                changed = self.conn.execute(
                    "UPDATE scheduled_tasks SET status='needs_review' "
                    "WHERE task_id=? AND status='queued'",
                    (row["scheduled_task_id"],),
                )
                if changed.rowcount != 1:
                    raise RuntimeError("Scheduled task transition failed")
        return True, "needs_review"
