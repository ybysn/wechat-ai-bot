"""P6.5D2B3 restricted real HTTP exchange against isolated loopback fixture."""

import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.request import Request

from wechat_ai_bot.sender_http_contract import (
    build_text_send_request, build_send_lookup_request,
)
from wechat_ai_bot.sender_http_transport import (
    RestrictedHttpExecutor, InvalidHttpExchange, mock_request,
)


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        data = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        self.server.received.append(("POST", self.path, json.loads(data),
                                     self.headers.get("Idempotency-Key")))
        self.reply(202, {"state": "queued", "id": "synthetic"})

    def do_GET(self):
        self.server.received.append(("GET", self.path, None, None))
        if self.path.endswith("key=redirect"):
            self.send_response(302)
            self.send_header("Location", "http://127.0.0.1:6174/v1/accounts/personal/sends")
            self.send_header("Content-Length", "0")
            self.end_headers()
        elif self.path.endswith("key=too-big"):
            data = json.dumps({"payload":"x"*66000}).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        else:
            self.reply(404, {"error": {"code": "not_found"}})

    def reply(self, status, payload):
        data = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args):
        pass


class RestrictedHttpTransportTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.server.received = []
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=5)

    def setUp(self):
        self.server.received.clear()
        self.port = self.server.server_port
        self.executor = RestrictedHttpExecutor(mock_port=self.port)

    def send(self):
        return build_text_send_request(account_id="personal", thread_id="group@chatroom",
                                       text="测试消息", idempotency_key="same-key", token="synthetic")

    def lookup(self, key="same-key"):
        return build_send_lookup_request(account_id="personal",
                                         idempotency_key=key, token="synthetic")

    def test_post_to_loopback_fixture_only(self):
        response = self.executor.perform(mock_request(self.send(), self.port))
        self.assertEqual((response.status, response.body["state"]), (202, "queued"))
        self.assertEqual(len(self.server.received), 1)
        method, path, body, key = self.server.received[0]
        self.assertEqual((method, key, body["text"]), ("POST", "same-key", "测试消息"))

    def test_get_404_preserves_structured_error(self):
        response = self.executor.perform(mock_request(self.lookup(), self.port))
        self.assertEqual(response.status, 404)
        self.assertEqual(response.body, {"error": {"code":"not_found"}})

    def test_production_post_fails_before_network(self):
        with self.assertRaisesRegex(PermissionError, "Production POST"):
            RestrictedHttpExecutor().perform(self.send())
        self.assertEqual(self.server.received, [])

    def test_fixture_executor_cannot_call_real_daemon(self):
        with self.assertRaises(PermissionError):
            self.executor.perform(self.send())
        self.assertEqual(self.server.received, [])

    def test_prod_cannot_be_disguised_as_mock_port(self):
        with self.assertRaises(InvalidHttpExchange):
            RestrictedHttpExecutor(mock_port=6174)
        with self.assertRaises(InvalidHttpExchange):
            mock_request(self.send(), 6174)

    def test_disallows_remote_targets(self):
        for url in (
            "http://localhost:6174/v1/accounts/personal/sends/by-idempotency-key?key=one",
            "http://10.0.0.1:6174/v1/accounts/personal/sends/by-idempotency-key?key=one",
            "https://127.0.0.1:6174/v1/accounts/personal/sends/by-idempotency-key?key=one",
            "http://x@127.0.0.1:6174/v1/accounts/personal/sends/by-idempotency-key?key=one",
        ):
            with self.subTest(url=url), self.assertRaises(PermissionError):
                RestrictedHttpExecutor().perform(Request(url, headers={"Authorization":"Bearer test"}))

    def test_disallows_arbitrary_production_route(self):
        base = "http://127.0.0.1:6174/v1/accounts/personal"
        for route in ("sends/abc", "messages/abc"):
            request = Request(f"{base}/{route}", headers={"Authorization":"Bearer test"})
            with self.subTest(route=route), self.assertRaises(PermissionError):
                RestrictedHttpExecutor().perform(request)

    def test_rejects_redirect_to_production(self):
        with self.assertRaises(InvalidHttpExchange):
            self.executor.perform(mock_request(self.lookup("redirect"), self.port))
        self.assertEqual(len(self.server.received), 1)

    def test_rejects_oversize_response(self):
        with self.assertRaisesRegex(InvalidHttpExchange, "size limit"):
            self.executor.perform(mock_request(self.lookup("too-big"), self.port))

    def test_mock_rejects_real_token(self):
        secret_request = build_text_send_request(
            account_id="personal", thread_id="group@chatroom",
            text="hello", idempotency_key="same-key", token="real-secret",
        )
        with self.assertRaises(PermissionError):
            mock_request(secret_request, self.port)

    def test_invalid_mock_port_and_timeout(self):
        for port in (0, 1, 6174, 70000, True, "5000"):
            with self.subTest(port=port), self.assertRaises(InvalidHttpExchange):
                RestrictedHttpExecutor(mock_port=port)
        for timeout in (0, 11, True, "5"):
            with self.subTest(timeout=timeout), self.assertRaises(InvalidHttpExchange):
                RestrictedHttpExecutor(timeout_seconds=timeout)

    def test_get_only_same_key_route(self):
        request = Request("http://127.0.0.1:6174/v1/accounts/personal/sends/by-idempotency-key?key=one&x=2",
                          headers={"Authorization":"Bearer test"})
        with self.assertRaises(PermissionError):
            RestrictedHttpExecutor().perform(request)

    def test_production_post_fails_even_with_healthy_config(self):
        # A caller cannot switch production POST on by adjusting config; this
        # executor takes no config and never permits POST to daemon port.
        self.assertFalse(hasattr(RestrictedHttpExecutor(), "enable_send"))
        with self.assertRaises(PermissionError):
            RestrictedHttpExecutor().perform(self.send())


if __name__ == "__main__":
    unittest.main()
