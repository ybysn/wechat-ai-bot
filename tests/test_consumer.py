from contextlib import closing
import json
import io
import os
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from urllib.error import HTTPError
from unittest.mock import Mock

from wechat_ai_bot.consumer import (
    AgentWeChatAPI,
    EventConsumer,
    FatalConsumerError,
    RetryableConsumerError,
    SSEFrame,
    iter_sse_frames,
    observer_state_directory,
    single_instance_lock,
    valid_cursor,
    validate_config,
)
from wechat_ai_bot.storage import Storage


ACCOUNT = "account-1"
GROUP = "target-group@chatroom"
CONSUMER_NAME = "test-consumer"
BASE_URL = "http://127.0.0.1:6174/v1"


class FakeResponse:
    def __init__(self, lines=(), content_type="text/event-stream; charset=utf-8"):
        self.lines = list(lines)
        self.headers = {"Content-Type": content_type}
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()

    def __iter__(self):
        yield from self.lines

    def close(self):
        self.closed = True


class FakeHTTPResponse(io.BytesIO):
    def __init__(self, payload, *, url="http://127.0.0.1:6174/v1/test"):
        super().__init__(payload)
        self.headers = {"Content-Type": "application/json"}
        self._url = url

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()

    def geturl(self):
        return self._url


class FakeAPI:
    def __init__(self, store=None, *, newest="10", stream_lines=()):
        self.store = store
        self.newest = newest
        self.stream_lines = list(stream_lines)
        self.acks = []
        self.fail_ack_once = False
        self.ack_observer = None
        self.stream_opens = []

    def newest_cursor(self):
        return self.newest

    def acknowledge(self, cursor):
        if self.ack_observer:
            self.ack_observer(cursor)
        if self.fail_ack_once:
            self.fail_ack_once = False
            raise RetryableConsumerError("simulated ACK transport failure")
        self.acks.append(cursor)

    def open_event_stream(self, cursor):
        self.stream_opens.append(cursor)
        return FakeResponse(self.stream_lines)


def event_frame(cursor, *, kind="message.committed", thread_id=GROUP,
                message_id="message-1", account_id=ACCOUNT):
    payload = {
        "cursor": cursor,
        "kind": kind,
        "accountId": account_id,
        "data": {
            "threadId": thread_id,
            "messageId": message_id,
        },
    }
    return (
        f"id: {cursor}\n"
        f"data: {json.dumps(payload, separators=(',', ':'))}\n"
        "\n"
    ).encode("utf-8").splitlines(keepends=True)


def make_config(tmpdir: Path):
    token = tmpdir / "token"
    token.write_text("test-token\n", encoding="utf-8")
    token.chmod(0o600)
    database = tmpdir / "bot.sqlite3"
    database.touch(mode=0o600)
    database.chmod(0o600)
    return {
        "schema_version": 2,
        "app": {"auto_send": False},
        "sender": {"enabled": False},
        "runtime": {"global_pause": True},
        "agent_wechat": {
            "base_url": BASE_URL,
            "account_id": ACCOUNT,
            "consumer_name": CONSUMER_NAME,
            "target_group_id": GROUP,
            "token_file": str(token),
        },
        "safety": {"group_allowlist": [GROUP]},
        "storage": {"database": str(database)},
    }


class ConsumerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db = self.root / "bot.sqlite3"
        with closing(sqlite3.connect(self.db)) as conn:
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
                PRAGMA user_version=4;
            """)
        self.store = Storage(self.db, consumer_name=CONSUMER_NAME)
        self.config = make_config(self.root)
        self.stop = threading.Event()

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def seed_checkpoint(self, durable="10", acked="10"):
        self.store.conn.execute(
            "INSERT INTO consumer_state "
            "(consumer_name,account_id,last_durable_cursor,last_acked_cursor) "
            "VALUES(?,?,?,?)",
            (CONSUMER_NAME, ACCOUNT, durable, acked),
        )

    def consumer(self, api):
        return EventConsumer(
            config=self.config,
            store=self.store,
            api=api,
            stop_event=self.stop,
            logger=lambda _message: None,
        )

    def test_valid_cursor_uses_sqlite_integer_range(self):
        self.assertTrue(valid_cursor("0"))
        self.assertTrue(valid_cursor("9223372036854775807"))
        self.assertFalse(valid_cursor(""))
        self.assertFalse(valid_cursor("١٢٣"))
        self.assertFalse(valid_cursor("1.0"))
        self.assertFalse(valid_cursor("9223372036854775808"))

    def test_sse_parser_joins_data_fields_and_ignores_heartbeat(self):
        frames = list(iter_sse_frames([
            b": keepalive\n",
            b"id: 12\n",
            b"event: message\n",
            b"data: {\"a\":\n",
            b"data: 1}\n",
            b"\n",
        ]))
        self.assertEqual(frames, [
            SSEFrame(data='{"a\":\n1}', event="message", event_id="12")
        ])

    def test_sse_parser_checks_stop_signal_on_heartbeat(self):
        stop = threading.Event()

        def lines():
            yield b": heartbeat\n"
            stop.set()
            yield b"id: 11\n"
            yield b"data: {}\n"
            yield b"\n"

        self.assertEqual(
            list(iter_sse_frames(lines(), stop_event=stop)),
            [],
        )

    def test_sse_parser_discards_incomplete_final_frame(self):
        frames = list(iter_sse_frames([
            b"id: 12\n",
            b"data: {\"cursor\":\"12\"}\n",
        ]))
        self.assertEqual(frames, [])

    def test_sse_parser_rejects_oversized_frame(self):
        with self.assertRaises(FatalConsumerError):
            list(iter_sse_frames(
                [b"data: 123456\n", b"\n"],
                max_frame_bytes=5,
            ))

    def test_sse_parser_rejects_invalid_utf8(self):
        with self.assertRaises(FatalConsumerError):
            list(iter_sse_frames([b"data: \xff\n", b"\n"]))

    def test_first_start_initializes_from_remote_newest_cursor(self):
        api = FakeAPI(newest="77")
        consumer = self.consumer(api)
        self.assertEqual(consumer.prepare_cursor(), "77")
        self.assertEqual(
            self.store.checkpoint(ACCOUNT)["last_durable_cursor"],
            "77",
        )
        self.assertIsNone(
            self.store.checkpoint(ACCOUNT)["last_acked_cursor"]
        )

    def test_resume_compensates_unfinished_remote_ack(self):
        self.seed_checkpoint(durable="77", acked="76")
        api = FakeAPI()
        consumer = self.consumer(api)
        self.assertEqual(consumer.prepare_cursor(), "77")
        self.assertEqual(api.acks, ["77"])
        self.assertEqual(
            self.store.checkpoint(ACCOUNT)["last_acked_cursor"],
            "77",
        )

    def test_resume_rejects_ack_ahead_of_durable_cursor(self):
        self.seed_checkpoint(durable="76", acked="77")
        with self.assertRaises(FatalConsumerError):
            self.consumer(FakeAPI()).prepare_cursor()

    def test_target_and_non_target_events_are_persisted_privately(self):
        self.seed_checkpoint()
        lines = (
            event_frame("11", message_id="target-message")
            + event_frame(
                "12", thread_id="other-group@chatroom",
                message_id="must-not-be-stored",
            )
        )
        api = FakeAPI(self.store, stream_lines=lines)

        def assert_durable_before_ack(cursor):
            state = self.store.checkpoint(ACCOUNT)
            self.assertEqual(state["last_durable_cursor"], cursor)
            row = self.store.conn.execute(
                "SELECT cursor FROM events WHERE account_id=? AND cursor=?",
                (ACCOUNT, cursor),
            ).fetchone()
            self.assertIsNotNone(row)

        api.ack_observer = assert_durable_before_ack
        result = self.consumer(api).consume_once("10")

        self.assertEqual(result, {"events": 2, "target_messages": 1})
        rows = self.store.conn.execute(
            "SELECT cursor,thread_id,message_id FROM events "
            "WHERE account_id=? ORDER BY CAST(cursor AS INTEGER)",
            (ACCOUNT,),
        ).fetchall()
        self.assertEqual(tuple(rows[0]), ("11", GROUP, "target-message"))
        self.assertEqual(tuple(rows[1]), ("12", None, None))
        self.assertEqual(api.acks, ["11", "12"])
        state = self.store.checkpoint(ACCOUNT)
        self.assertEqual(state["last_durable_cursor"], "12")
        self.assertEqual(state["last_acked_cursor"], "12")

    def test_remote_ack_failure_leaves_durable_cursor_for_recovery(self):
        self.seed_checkpoint()
        api = FakeAPI(stream_lines=event_frame("11"))
        api.fail_ack_once = True
        consumer = self.consumer(api)

        with self.assertRaises(RetryableConsumerError):
            consumer.consume_once("10")

        state = self.store.checkpoint(ACCOUNT)
        self.assertEqual(state["last_durable_cursor"], "11")
        self.assertEqual(state["last_acked_cursor"], "10")

        # Next connection startup ACKs the durable cursor before subscribing.
        self.assertEqual(consumer.prepare_cursor(), "11")
        state = self.store.checkpoint(ACCOUNT)
        self.assertEqual(state["last_acked_cursor"], "11")
        self.assertEqual(api.acks, ["11"])

    def test_invalid_event_is_never_acked(self):
        self.seed_checkpoint()
        payload = {
            "cursor": "not-a-cursor",
            "kind": "message.committed",
            "accountId": ACCOUNT,
            "data": {"threadId": GROUP, "messageId": "m1"},
        }
        lines = [f"data: {json.dumps(payload)}\n".encode(), b"\n"]
        api = FakeAPI(stream_lines=lines)
        with self.assertRaises(FatalConsumerError):
            self.consumer(api).consume_once("10")
        self.assertEqual(api.acks, [])
        self.assertIsNone(
            self.store.conn.execute(
                "SELECT 1 FROM events WHERE account_id=? AND cursor='not-a-cursor'",
                (ACCOUNT,),
            ).fetchone()
        )

    def test_wrong_account_event_is_never_acked(self):
        self.seed_checkpoint()
        lines = event_frame("11", account_id="another-account")
        api = FakeAPI(stream_lines=lines)
        with self.assertRaises(FatalConsumerError):
            self.consumer(api).consume_once("10")
        self.assertEqual(api.acks, [])

    def test_non_advancing_cursor_is_rejected(self):
        self.seed_checkpoint()
        api = FakeAPI(stream_lines=event_frame("10"))
        with self.assertRaises(FatalConsumerError):
            self.consumer(api).consume_once("10")
        self.assertEqual(api.acks, [])

    def test_config_requires_observe_only_safety_and_private_token(self):
        validate_config(self.config)
        self.config["app"]["auto_send"] = True
        with self.assertRaises(FatalConsumerError):
            validate_config(self.config)

    def test_config_rejects_token_permissions_that_are_too_open(self):
        token = Path(self.config["agent_wechat"]["token_file"])
        token.chmod(0o640)
        with self.assertRaises(FatalConsumerError):
            validate_config(self.config)

    def shared_storage(self):
        if os.getegid() == 0:
            self.skipTest("Shared storage requires a non-root fixture group")
        db_directory = self.root / "shared"
        db_directory.mkdir()
        db_directory.chmod(0o2770)
        database = db_directory / "test.sqlite3"
        database.touch()
        database.chmod(0o660)
        state = self.root / "observer"
        state.mkdir(mode=0o700)
        self.config["storage"] = {
            "database": str(database), "shared_group_id": os.getegid(),
            "service_state_directory": str(state),
        }
        return database, state

    def test_observer_can_use_explicit_shared_storage_with_private_state(self):
        database, state = self.shared_storage()
        validate_config(self.config)
        self.assertEqual(observer_state_directory(self.config, database), state)
        with single_instance_lock(state / "consumer.lock"):
            self.assertEqual((state / "consumer.lock").stat().st_mode & 0o777, 0o600)
            self.assertFalse((database.parent / "consumer.lock").exists())
        del self.config["storage"]["shared_group_id"]
        del self.config["storage"]["service_state_directory"]
        with self.assertRaises(FatalConsumerError):
            validate_config(self.config)

    def test_shared_config_rejects_wrong_permissions_or_group(self):
        database, state = self.shared_storage()
        for path, wrong, right in ((database, 0o664, 0o660), (database.parent, 0o770, 0o2770),
                                   (state, 0o750, 0o700)):
            with self.subTest(mode=oct(wrong)):
                path.chmod(wrong)
                with self.assertRaises(FatalConsumerError):
                    validate_config(self.config)
                path.chmod(right)
        self.config["storage"]["shared_group_id"] = True
        with self.assertRaisesRegex(FatalConsumerError, "共享组"):
            validate_config(self.config)

    def test_shared_config_rejects_db_links_and_state_inside_shared_directory(self):
        database, state = self.shared_storage()
        symlink = database.with_name("symlink.sqlite3")
        symlink.symlink_to(database)
        self.config["storage"]["database"] = str(symlink)
        with self.assertRaises(FatalConsumerError):
            validate_config(self.config)
        self.config["storage"]["database"] = str(database)
        hardlink = database.with_name("hardlink.sqlite3")
        os.link(database, hardlink)
        with self.assertRaisesRegex(FatalConsumerError, "共享 SQLite"):
            validate_config(self.config)
        hardlink.unlink()
        nested = database.parent / state.name
        nested.mkdir(mode=0o700)
        self.config["storage"]["service_state_directory"] = str(nested)
        with self.assertRaisesRegex(FatalConsumerError, "独立于"):
            validate_config(self.config)

    def test_single_instance_lock_rejects_second_holder(self):
        lock_path = self.root / "consumer.lock"
        with single_instance_lock(lock_path):
            with self.assertRaises(FatalConsumerError):
                with single_instance_lock(lock_path):
                    pass
        self.assertEqual(lock_path.stat().st_mode & 0o777, 0o600)


    def test_run_forever_reconnects_with_exponential_backoff(self):
        class StopAfterWait:
            def __init__(self, stop_after):
                self.stop_after = stop_after
                self.waits = []
                self.stopped = False

            def is_set(self):
                return self.stopped

            def wait(self, delay):
                self.waits.append(delay)
                if len(self.waits) >= self.stop_after:
                    self.stopped = True
                    return True
                return False

        self.seed_checkpoint()
        stop = StopAfterWait(stop_after=2)
        api = FakeAPI(stream_lines=[])
        consumer = EventConsumer(
            config=self.config,
            store=self.store,
            api=api,
            stop_event=stop,
            logger=lambda _message: None,
            backoff_initial=1,
            backoff_max=4,
        )
        consumer.run_forever()

        self.assertEqual(api.stream_opens, ["10", "10"])
        self.assertEqual(stop.waits, [1, 2])

    def test_run_forever_retries_sqlite_busy(self):
        class StopAfterWait:
            def __init__(self):
                self.waits = []
                self.stopped = False

            def is_set(self):
                return self.stopped

            def wait(self, delay):
                self.waits.append(delay)
                self.stopped = True
                return True

        stop = StopAfterWait()

        class BusyConsumer(EventConsumer):
            def prepare_cursor(inner_self):
                raise sqlite3.OperationalError("database is locked")

        consumer = BusyConsumer(
            config=self.config,
            store=self.store,
            api=FakeAPI(),
            stop_event=stop,
            logger=lambda _message: None,
        )
        consumer.run_forever()
        self.assertEqual(stop.waits, [1.0])

    def test_run_forever_does_not_retry_sqlite_disk_errors(self):
        class BrokenConsumer(EventConsumer):
            def prepare_cursor(inner_self):
                raise sqlite3.OperationalError("database or disk is full")

        consumer = BrokenConsumer(
            config=self.config,
            store=self.store,
            api=FakeAPI(),
            stop_event=self.stop,
            logger=lambda _message: None,
        )
        with self.assertRaises(FatalConsumerError):
            consumer.run_forever()

    def test_stop_during_complete_but_unhandled_frame_does_not_ack(self):
        self.seed_checkpoint()

        class StopOnFrameResponse(FakeResponse):
            def __iter__(inner_self):
                for line in event_frame("11"):
                    if line == b"\n":
                        self.stop.set()
                    yield line

        class StopOnFrameAPI(FakeAPI):
            def open_event_stream(inner_self, cursor):
                inner_self.stream_opens.append(cursor)
                return StopOnFrameResponse()

        api = StopOnFrameAPI()
        result = self.consumer(api).consume_once("10")
        self.assertEqual(result, {"events": 0, "target_messages": 0})
        self.assertEqual(api.acks, [])
        self.assertEqual(
            self.store.checkpoint(ACCOUNT)["last_durable_cursor"], "10"
        )

    def test_api_json_response_is_bounded_and_parsed(self):
        response = FakeHTTPResponse(b'{"nextCursor":"10"}')
        api = AgentWeChatAPI(
            base_url=BASE_URL,
            account_id=ACCOUNT,
            consumer_name=CONSUMER_NAME,
            token="token",
            opener=Mock(return_value=response),
        )
        self.assertEqual(api.newest_cursor(), "10")

    def test_api_rejects_redirect_outside_local_origin(self):
        response = FakeHTTPResponse(
            b'{"nextCursor":"10"}',
            url="https://example.invalid/steal-token",
        )
        api = AgentWeChatAPI(
            base_url=BASE_URL,
            account_id=ACCOUNT,
            consumer_name=CONSUMER_NAME,
            token="token",
            opener=Mock(return_value=response),
        )
        with self.assertRaises(FatalConsumerError):
            api.newest_cursor()
        self.assertTrue(response.closed)

    def test_api_rejects_oversized_json_response(self):
        response = FakeHTTPResponse(b" " * 1_048_577)
        api = AgentWeChatAPI(
            base_url=BASE_URL,
            account_id=ACCOUNT,
            consumer_name=CONSUMER_NAME,
            token="token",
            opener=Mock(return_value=response),
        )
        with self.assertRaises(FatalConsumerError):
            api.newest_cursor()

    def test_http_rate_limit_is_retryable(self):
        api = AgentWeChatAPI(
            base_url=BASE_URL,
            account_id=ACCOUNT,
            consumer_name=CONSUMER_NAME,
            token="token",
            opener=Mock(side_effect=HTTPError(
                "http://127.0.0.1", 429, "Too Many Requests", {}, None
            )),
        )
        with self.assertRaises(RetryableConsumerError):
            api.newest_cursor()

    def test_http_unauthorized_is_fatal(self):
        api = AgentWeChatAPI(
            base_url=BASE_URL,
            account_id=ACCOUNT,
            consumer_name=CONSUMER_NAME,
            token="token",
            opener=Mock(side_effect=HTTPError(
                "http://127.0.0.1", 401, "Unauthorized", {}, None
            )),
        )
        with self.assertRaises(FatalConsumerError):
            api.newest_cursor()


if __name__ == "__main__":
    unittest.main()
