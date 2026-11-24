import io
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from urllib.error import HTTPError
from unittest.mock import Mock, patch

from wechat_ai_bot.consumer import (
    AgentWeChatAPI,
    CursorExpiredError,
    CursorRecoveryError,
    EventConsumer,
    FatalConsumerError,
    RetryableConsumerError,
    main as consumer_main,
)
from wechat_ai_bot.storage import Storage
from wechat_ai_bot.worker import main as worker_main


ACCOUNT = "account-1"
GROUP = "target-group@chatroom"
CONSUMER = "test-consumer"
BASE_URL = "http://127.0.0.1:6174/v1"


class FakeResponse:
    def __init__(self, lines=()):
        self.lines = list(lines)
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()

    def __iter__(self):
        yield from self.lines

    def close(self):
        self.closed = True


class FakeAPI:
    def __init__(self, *, pages=None, details=None):
        self.pages = pages or []
        self.details = details or {}
        self.page_calls = []
        self.detail_calls = []
        self.acks = []
        self.stream_opens = []
        self.fail_ack_once = False
        self.expire_first_stream = False
        self.stop_event = None

    def thread_messages_page(self, thread_id, *, cursor=None, limit=50):
        self.page_calls.append((thread_id, cursor, limit))
        index = 0 if cursor is None else int(cursor.removeprefix("page-"))
        return self.pages[index]

    def message_detail(self, message_id):
        self.detail_calls.append(message_id)
        return self.details[message_id]

    def acknowledge(self, cursor):
        if self.fail_ack_once:
            self.fail_ack_once = False
            raise RetryableConsumerError("simulated ACK failure")
        self.acks.append(cursor)

    def open_event_stream(self, cursor):
        self.stream_opens.append(cursor)
        if self.expire_first_stream and len(self.stream_opens) == 1:
            raise CursorExpiredError("event_cursor_expired")
        if self.stop_event is not None:
            self.stop_event.set()
        return FakeResponse()


class CursorRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db = self.root / "bot.sqlite3"
        with sqlite3.connect(self.db) as conn:
            conn.executescript("""
                CREATE TABLE decisions (
                    decision_id TEXT PRIMARY KEY,
                    reply TEXT
                );
                CREATE TABLE scheduled_tasks (
                    task_id TEXT PRIMARY KEY
                );
                CREATE TABLE consumer_state (
                    consumer_name TEXT NOT NULL,
                    account_id TEXT NOT NULL,
                    last_durable_cursor TEXT NOT NULL,
                    last_acked_cursor TEXT,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY(consumer_name, account_id)
                );
                CREATE TABLE events (
                    account_id TEXT NOT NULL,
                    cursor TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    thread_id TEXT,
                    message_id TEXT,
                    status TEXT NOT NULL DEFAULT 'received',
                    PRIMARY KEY(account_id, cursor)
                );
                CREATE TABLE messages (
                    account_id TEXT NOT NULL,
                    message_id TEXT NOT NULL,
                    thread_id TEXT NOT NULL,
                    sender_id TEXT,
                    direction TEXT,
                    message_type INTEGER,
                    body TEXT,
                    mentions_self INTEGER,
                    created_at TEXT,
                    source_cursor TEXT,
                    status TEXT NOT NULL DEFAULT 'pending',
                    PRIMARY KEY(account_id, message_id)
                );
                PRAGMA user_version=4;
            """)
        self.store = Storage(self.db, consumer_name=CONSUMER)
        self.store.conn.execute(
            "INSERT INTO consumer_state "
            "(consumer_name,account_id,last_durable_cursor,last_acked_cursor) "
            "VALUES(?,?,?,?)",
            (CONSUMER, ACCOUNT, "10", "10"),
        )
        self.store.conn.execute(
            "INSERT INTO events "
            "(account_id,cursor,kind,thread_id,message_id,status) "
            "VALUES(?,?,?,?,?,?)",
            (ACCOUNT, "10", "message.committed", GROUP, "anchor", "processed"),
        )
        self.store.persist_message(
            ACCOUNT, "anchor", GROUP,
            direction="incoming", body="old anchor",
            sender_id="sender-0", message_type=1,
            created_at="2026-11-21T01:00:00+00:00",
            source_cursor="10",
        )
        self.store.conn.execute(
            "UPDATE messages SET status='processed' "
            "WHERE account_id=? AND message_id='anchor'",
            (ACCOUNT,),
        )
        self.config = {
            "agent_wechat": {
                "account_id": ACCOUNT,
                "target_group_id": GROUP,
                "consumer_name": CONSUMER,
            },
            "processing": {"max_message_chars": 4000},
        }

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def detail(self, message_id, body=None, *, thread=GROUP):
        return {
            "id": message_id,
            "accountId": ACCOUNT,
            "threadId": thread,
            "direction": "incoming",
            "body": body or f"body-{message_id}",
            "type": 1,
            "senderId": f"sender-{message_id}",
            "createdAt": "2026-11-21T02:00:00+00:00",
            "mentions": {"state": "known", "self": False},
        }

    def page(self, ids, *, next_cursor=None, as_of="50", generation="gen-1", partial=False):
        return {
            "items": [
                {"id": mid, "threadId": GROUP}
                for mid in ids
            ],
            "nextCursor": next_cursor,
            "coverage": {"partial": partial, "databases": []},
            "sourceGeneration": generation,
            "asOfEventCursor": as_of,
        }

    def make_consumer(self, api, stop=None):
        return EventConsumer(
            config=self.config,
            store=self.store,
            api=api,
            stop_event=stop or threading.Event(),
            logger=lambda _message: None,
        )

    def test_recovers_newest_first_pages_and_acks_snapshot_boundary(self):
        api = FakeAPI(
            pages=[
                self.page(["new-2", "new-1"], next_cursor="page-1"),
                self.page(["anchor", "older"], next_cursor=None),
            ],
            details={"new-1": self.detail("new-1"), "new-2": self.detail("new-2")},
        )
        consumer = self.make_consumer(api)

        cursor = consumer.recover_expired_cursor()

        self.assertEqual(cursor, "50")
        self.assertEqual(api.acks, ["50"])
        self.assertEqual(api.detail_calls, ["new-1", "new-2"])
        state = self.store.checkpoint(ACCOUNT)
        self.assertEqual(state["last_durable_cursor"], "50")
        self.assertEqual(state["last_acked_cursor"], "50")

        for mid in ("new-1", "new-2"):
            row = self.store.message(ACCOUNT, mid)
            self.assertEqual(row["source_cursor"], "50")
            self.assertEqual(row["status"], "ignored")
            self.assertEqual(row["body"], f"body-{mid}")

        marker = self.store.conn.execute(
            "SELECT kind,status FROM events WHERE account_id=? AND cursor='50'",
            (ACCOUNT,),
        ).fetchone()
        self.assertEqual(tuple(marker), ("recovery.checkpoint", "processed"))

        # The oldest recovered message is inserted first; rowid order preserves
        # newest-first selection for the same snapshot cursor.
        rows = self.store.conn.execute(
            "SELECT message_id FROM messages WHERE source_cursor='50' ORDER BY rowid"
        ).fetchall()
        self.assertEqual([row[0] for row in rows], ["new-1", "new-2"])

    def test_missing_anchor_never_advances_or_acks(self):
        api = FakeAPI(pages=[self.page(["new-2", "new-1"])])
        with self.assertRaises(CursorRecoveryError):
            self.make_consumer(api).recover_expired_cursor()
        self.assertEqual(self.store.checkpoint(ACCOUNT)["last_durable_cursor"], "10")
        self.assertEqual(api.acks, [])
        self.assertEqual(api.detail_calls, [])

    def test_partial_history_coverage_is_rejected(self):
        api = FakeAPI(pages=[self.page(["new-1", "anchor"], partial=True)])
        with self.assertRaisesRegex(CursorRecoveryError, "覆盖不完整"):
            self.make_consumer(api).recover_expired_cursor()
        self.assertEqual(self.store.checkpoint(ACCOUNT)["last_durable_cursor"], "10")
        self.assertEqual(api.acks, [])

    def test_changed_snapshot_cursor_is_rejected(self):
        api = FakeAPI(pages=[
            self.page(["new-2"], next_cursor="page-1", as_of="50"),
            self.page(["anchor"], as_of="51"),
        ])
        with self.assertRaisesRegex(CursorRecoveryError, "无法取得一致历史快照"):
            self.make_consumer(api).recover_expired_cursor()
        self.assertEqual(self.store.checkpoint(ACCOUNT)["last_durable_cursor"], "10")

    def test_source_generation_may_change_while_page_cursor_snapshot_is_stable(self):
        api = FakeAPI(
            pages=[
                self.page(["new-1"], next_cursor="page-1", generation="gen-1"),
                self.page(["anchor"], generation="gen-2"),
            ],
            details={"new-1": self.detail("new-1")},
        )
        self.assertEqual(self.make_consumer(api).recover_expired_cursor(), "50")

    def test_snapshot_cursor_must_be_valid_and_newer(self):
        api = FakeAPI(pages=[self.page(["new-1", "anchor"], as_of="10")])
        with self.assertRaisesRegex(CursorRecoveryError, "没有超过"):
            self.make_consumer(api).recover_expired_cursor()
        self.assertEqual(self.store.checkpoint(ACCOUNT)["last_durable_cursor"], "10")

    def test_message_identity_mismatch_never_advances_checkpoint(self):
        api = FakeAPI(
            pages=[self.page(["new-1", "anchor"])],
            details={"new-1": self.detail("new-1", thread="other@chatroom")},
        )
        with self.assertRaisesRegex(CursorRecoveryError, "身份不匹配"):
            self.make_consumer(api).recover_expired_cursor()
        self.assertEqual(self.store.checkpoint(ACCOUNT)["last_durable_cursor"], "10")
        self.assertEqual(api.acks, [])

    def test_ack_failure_keeps_durable_boundary_for_retry(self):
        api = FakeAPI(
            pages=[self.page(["new-1", "anchor"])],
            details={"new-1": self.detail("new-1")},
        )
        api.fail_ack_once = True
        consumer = self.make_consumer(api)
        with self.assertRaises(RetryableConsumerError):
            consumer.recover_expired_cursor()

        state = self.store.checkpoint(ACCOUNT)
        self.assertEqual(state["last_durable_cursor"], "50")
        self.assertEqual(state["last_acked_cursor"], "10")

        self.assertEqual(consumer.prepare_cursor(), "50")
        state = self.store.checkpoint(ACCOUNT)
        self.assertEqual(state["last_acked_cursor"], "50")
        self.assertEqual(api.acks, ["50"])

    def test_recovered_messages_are_context_only_not_pending_replies(self):
        api = FakeAPI(
            pages=[self.page(["new-1", "anchor"])],
            details={"new-1": self.detail("new-1")},
        )
        self.make_consumer(api).recover_expired_cursor()
        row = self.store.message(ACCOUNT, "new-1")
        self.assertEqual(row["status"], "ignored")
        self.assertIsNotNone(row["source_cursor"])
        pending = self.store.conn.execute(
            "SELECT COUNT(*) FROM messages WHERE account_id=? "
            "AND status='pending' AND source_cursor IS NOT NULL",
            (ACCOUNT,),
        ).fetchone()[0]
        self.assertEqual(pending, 0)

    def test_repeated_page_cursor_is_rejected(self):
        api = FakeAPI(pages=[
            self.page(["new-2"], next_cursor="page-1"),
            self.page(["new-1"], next_cursor="page-1"),
        ])
        with self.assertRaisesRegex(CursorRecoveryError, "Cursor 循环"):
            self.make_consumer(api).recover_expired_cursor()
        self.assertEqual(self.store.checkpoint(ACCOUNT)["last_durable_cursor"], "10")

    def test_duplicate_message_id_across_pages_is_rejected(self):
        api = FakeAPI(pages=[
            self.page(["new-1"], next_cursor="page-1"),
            self.page(["new-1", "anchor"]),
        ])
        with self.assertRaisesRegex(CursorRecoveryError, "重复消息 ID"):
            self.make_consumer(api).recover_expired_cursor()
        self.assertEqual(self.store.checkpoint(ACCOUNT)["last_durable_cursor"], "10")

    def test_http_410_event_cursor_expired_is_typed(self):
        error = HTTPError(
            "http://127.0.0.1:6174/v1/events/stream",
            410,
            "Gone",
            {},
            io.BytesIO(b'{"error":{"code":"event_cursor_expired"}}'),
        )
        api = AgentWeChatAPI(
            base_url=BASE_URL,
            account_id=ACCOUNT,
            consumer_name=CONSUMER,
            token="token",
            opener=Mock(side_effect=error),
        )
        with self.assertRaises(CursorExpiredError):
            api.newest_cursor()

    def test_other_http_410_is_not_misclassified_as_cursor_expired(self):
        error = HTTPError(
            "http://127.0.0.1:6174/v1/events/stream",
            410,
            "Gone",
            {},
            io.BytesIO(b'{"error":{"code":"other_gone"}}'),
        )
        api = AgentWeChatAPI(
            base_url=BASE_URL,
            account_id=ACCOUNT,
            consumer_name=CONSUMER,
            token="token",
            opener=Mock(side_effect=error),
        )
        with self.assertRaises(FatalConsumerError) as caught:
            api.newest_cursor()
        self.assertNotIsInstance(caught.exception, CursorExpiredError)

    def test_run_forever_recovers_then_reconnects_at_snapshot_cursor(self):
        stop = threading.Event()
        api = FakeAPI(
            pages=[self.page(["new-1", "anchor"])],
            details={"new-1": self.detail("new-1")},
        )
        api.expire_first_stream = True
        api.stop_event = stop
        consumer = self.make_consumer(api, stop)
        consumer.run_forever()
        self.assertEqual(api.stream_opens, ["10", "50"])
        self.assertEqual(self.store.checkpoint(ACCOUNT)["last_acked_cursor"], "50")

    def test_recovery_retries_a_busy_local_ack_without_rescanning_history(self):
        stop = threading.Event()
        api = FakeAPI(pages=[self.page(["new-1", "anchor"])],
                      details={"new-1": self.detail("new-1")})
        api.expire_first_stream = True
        api.stop_event = stop
        blocker = sqlite3.connect(self.db, isolation_level=None)
        self.addCleanup(blocker.close)
        self.store.conn.execute("PRAGMA busy_timeout=0")
        real_acknowledge = api.acknowledge

        def acknowledge(cursor):
            real_acknowledge(cursor)
            if api.acks == ["50"]:
                blocker.execute("BEGIN IMMEDIATE")

        def release_lock(_delay):
            self.assertEqual(self.store.checkpoint(ACCOUNT),
                             {"last_durable_cursor": "50", "last_acked_cursor": "10"})
            blocker.rollback()
            return False

        with patch.object(api, "acknowledge", side_effect=acknowledge), \
                patch.object(stop, "wait", side_effect=release_lock) as wait:
            self.make_consumer(api, stop).run_forever()

        wait.assert_called_once_with(1.0)
        self.assertEqual(api.acks, ["50", "50"])
        self.assertEqual(api.stream_opens, ["10", "50"])
        self.assertEqual(api.detail_calls, ["new-1"])
        self.assertEqual(self.store.checkpoint(ACCOUNT)["last_acked_cursor"], "50")

    def test_invalid_startup_config_uses_systemd_nonretryable_exit_code(self):
        config = self.root / "bad-config.json"
        for bad_config in (b'{"missing":', b"\xff", b'{"schema_version": 0}'):
            config.write_bytes(bad_config)
            for main, flags in ((consumer_main, []), (worker_main, ["--run"])):
                with self.subTest(main=main.__module__, config=bad_config):
                    with patch("sys.stderr", new_callable=io.StringIO) as stderr:
                        self.assertEqual(main(["--config", str(config), *flags]), 2)
                    self.assertTrue(stderr.getvalue().startswith("FATAL: "))
                    self.assertNotIn("Traceback", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
