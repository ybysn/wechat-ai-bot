"""P6.5D2C end-to-end loopback integration; no production requests."""

from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from test_storage import V2_DDL
from wechat_ai_bot.migrations import migrate_v2_to_v3, migrate_v3_to_v4
from wechat_ai_bot.sender_http_fixture import (
    FIXTURE_ACCOUNT, FIXTURE_THREAD, FIXTURE_TEXT_PREFIX,
    simulate_fixture_http_send_once,
)
from wechat_ai_bot.sender_http_transport import RestrictedHttpExecutor
from wechat_ai_bot.storage import Storage


class FixtureHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        self.server.calls.append("POST")
        length = int(self.headers.get("Content-Length", "0"))
        body = json.loads(self.rfile.read(length))
        key = self.headers.get("Idempotency-Key")
        self.server.receipts[key] = {
            "id": "fixture-remote-1", "state": "accepted",
            "accountId": FIXTURE_ACCOUNT,
            "threadId": body["threadId"],
            "text": body["text"],
            "idempotencyKey": key,
        }
        if self.server.drop_response:
            self.close_connection = True
            return
        payload = dict(self.server.receipts[key])
        if self.server.mismatch_post:
            payload["threadId"] = "wrong-thread"
        if self.server.post_state != "accepted":
            payload["state"] = self.server.post_state
        self.write_json(202, payload)

    def do_GET(self):
        self.server.calls.append("GET")
        key = parse_qs(urlsplit(self.path).query).get("key", [""])[0]
        record = self.server.receipts.get(key)
        if record is None or self.server.hide_lookup:
            self.write_json(404, {"error": {"code": "not_found"}})
        else:
            payload = dict(record)
            if self.server.mismatch_lookup:
                payload["text"] = "tampered"
            self.write_json(200, payload)

    def write_json(self, status, payload):
        body = json.dumps(payload).encode("utf-8")
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def log_message(self, *args):
        pass


class FixtureIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), FixtureHandler)
        cls.server.daemon_threads = True
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=5)

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="wechat-p65d2-fixture-")
        self.path = Path(self.tmp.name) / "fixture.sqlite3"
        with sqlite3.connect(self.path) as conn:
            conn.executescript(V2_DDL)
            migrate_v2_to_v3(conn)
            migrate_v3_to_v4(conn)
        self.store = Storage(self.path)
        self.config = {
            "schema_version": 2,
            "app": {"auto_send": True},
            "sender": {"enabled": True},
            "runtime": {"global_pause": False},
            "agent_wechat": {
                "account_id": FIXTURE_ACCOUNT,
                "target_group_id": FIXTURE_THREAD,
            },
            "safety": {"group_allowlist": [FIXTURE_THREAD]},
        }
        self.now = datetime.now(timezone.utc) + timedelta(seconds=5)
        self.executor = RestrictedHttpExecutor(
            mock_port=self.server.server_port, timeout_seconds=1,
        )
        self.server.receipts = {}
        self.server.calls = []
        self.server.drop_response = False
        self.server.mismatch_post = False
        self.server.mismatch_lookup = False
        self.server.hide_lookup = False
        self.server.post_state = "accepted"
        self.serial = 0

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def queue(self, text=None):
        self.serial += 1
        mid = "fixture-message-" + str(self.serial)
        body = text if text is not None else FIXTURE_TEXT_PREFIX + " hello " + mid
        self.store.persist_message(
            FIXTURE_ACCOUNT, mid, FIXTURE_THREAD, direction="incoming",
            body="synthetic source", source_cursor=str(self.serial),
            created_at=(self.now-timedelta(seconds=20)).isoformat(),
        )
        decision, _ = self.store.decide(
            FIXTURE_ACCOUNT, mid, "reply", model="synthetic", reply=body,
        )
        self.assertTrue(self.store.accept_decision(decision, now=self.now))
        outbox, created = self.store.enqueue(
            decision, FIXTURE_ACCOUNT, FIXTURE_THREAD, body, now=self.now,
        )
        self.assertTrue(created)
        return outbox

    def status(self, outbox):
        return self.store.conn.execute(
            "SELECT status FROM outbox WHERE outbox_id=?", (outbox,),
        ).fetchone()[0]

    def run_item(self, item):
        return simulate_fixture_http_send_once(
            self.store, item, self.config, self.executor, now=self.now,
        )

    def test_full_http_success_commits_sent(self):
        item = self.queue()
        self.assertEqual(self.run_item(item), (True, "sent"))
        self.assertEqual(self.status(item), "sent")
        self.assertEqual(self.server.calls, ["POST"])

    def test_lost_post_response_uses_lookup_once(self):
        item = self.queue()
        self.server.drop_response = True
        self.assertEqual(self.run_item(item), (True, "sent"))
        self.assertEqual(self.server.calls, ["POST", "GET"])

    def test_unconfirmed_remote_quarantines(self):
        item = self.queue()
        self.server.post_state = "needs_review"
        self.server.hide_lookup = True
        self.assertEqual(self.run_item(item), (True, "needs_review"))
        self.assertEqual(self.status(item), "needs_review")
        self.assertEqual(self.server.calls, ["POST", "GET"])

    def test_mismatched_post_receipt_requires_lookup(self):
        item = self.queue()
        self.server.mismatch_post = True
        self.assertEqual(self.run_item(item), (True, "sent"))
        self.assertEqual(self.server.calls, ["POST", "GET"])

    def test_mismatched_post_and_lookup_quarantine(self):
        item = self.queue()
        self.server.mismatch_post = True
        self.server.mismatch_lookup = True
        self.assertEqual(self.run_item(item), (True, "needs_review"))
        self.assertEqual(self.status(item), "needs_review")

    def test_prevents_repeat_post_on_sent(self):
        item = self.queue()
        self.assertEqual(self.run_item(item), (True, "sent"))
        self.assertFalse(self.run_item(item)[0])
        self.assertEqual(self.server.calls, ["POST"])

    def test_paused_config_prevents_any_network(self):
        item = self.queue()
        self.config["runtime"]["global_pause"] = True
        self.assertFalse(self.run_item(item)[0])
        self.assertEqual(self.server.calls, [])
        self.assertEqual(self.status(item), "queued")

    def test_forbid_production_executor_before_claim(self):
        item = self.queue()
        with self.assertRaises(PermissionError):
            simulate_fixture_http_send_once(
                self.store, item, self.config, RestrictedHttpExecutor(), now=self.now,
            )
        self.assertEqual(self.status(item), "queued")
        self.assertEqual(self.server.calls, [])

    def test_forbid_real_account_config_before_claim(self):
        item = self.queue()
        self.config["agent_wechat"]["account_id"] = "personal"
        with self.assertRaises(PermissionError):
            self.run_item(item)
        self.assertEqual(self.status(item), "queued")

    def test_forbid_nonsynthetic_reply_before_claim(self):
        item = self.queue(text="a real-looking message")
        with self.assertRaises(PermissionError):
            self.run_item(item)
        self.assertEqual(self.status(item), "queued")
        self.assertEqual(self.server.calls, [])

    def test_forbid_non_temp_db_before_claim(self):
        item = self.queue()
        conn = self.store.conn
        class FakeConnection:
            def execute(self, sql, *args):
                if sql == "PRAGMA database_list":
                    class Cursor:
                        def fetchall(self):
                            return [(0, "main", "/home/user/production.sqlite3")]
                    return Cursor()
                return conn.execute(sql, *args)
        self.store.conn = FakeConnection()
        try:
            with self.assertRaises(PermissionError):
                self.run_item(item)
        finally:
            self.store.conn = conn
        self.assertEqual(self.status(item), "queued")

    def test_no_send_after_prior_needs_review(self):
        one = self.queue()
        self.server.post_state = "needs_review"
        self.server.hide_lookup = True
        self.assertEqual(self.run_item(one), (True, "needs_review"))
        two = self.queue()
        self.server.calls.clear()
        self.assertEqual(self.run_item(two), (False, "unresolved_send_exists"))
        self.assertEqual(self.server.calls, [])


if __name__ == "__main__":
    unittest.main()
