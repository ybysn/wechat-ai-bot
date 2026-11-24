"""P6.5D2B3: restricted HTTP executor for Agent WeChat send receipts.

A production GET to the fixed local daemon is permitted. A production POST is
*always* refused in this release, regardless of config, token, or caller.

POST/GET round trips may be tested only against a dedicated loopback test HTTP
server on a non-daemon port. The module deliberately does not import Storage,
Worker, or the Sender state machine: it cannot dispatch an Outbox entry.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import re
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlsplit
from urllib.request import (
    HTTPRedirectHandler, ProxyHandler, Request, build_opener,
)

from wechat_ai_bot.sender_http_contract import BASE_URL


MAX_RESPONSE_BYTES = 65536
_ENDPOINT = re.compile(
    r"/v1/accounts/(?:[a-zA-Z0-9_.~!$&'()+,;=:@%-]+)/sends"
    r"(?P<lookup>/by-idempotency-key)?\Z"
)


@dataclass(frozen=True)
class HttpExchangeResult:
    status: int
    body: object


class InvalidHttpExchange(ValueError):
    """Untrusted HTTP request/response or an invalid test endpoint."""


class _NeverRedirect(HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, msg, headers, newurl):
        return None


def _verify_request(request: Request, mock_port: int | None):
    if not isinstance(request, Request):
        raise TypeError("Expected urllib Request")
    parsed = urlsplit(request.full_url)
    if (
        parsed.scheme != "http" or parsed.hostname != "127.0.0.1"
        or parsed.username is not None or parsed.password is not None
        or parsed.fragment or parsed.path.startswith("//")
    ):
        raise PermissionError("HTTP target must be explicit loopback")
    try:
        port = parsed.port
    except ValueError as error:
        raise InvalidHttpExchange("Invalid HTTP port") from error
    expected_port = 6174 if mock_port is None else mock_port
    if port != expected_port:
        raise PermissionError("Unexpected HTTP port")
    match = _ENDPOINT.fullmatch(parsed.path)
    if not match:
        raise PermissionError("Unexpected send API route")

    method = request.get_method()
    if method == "GET":
        if not match.group("lookup"):
            raise PermissionError("GET is limited to same-key receipt lookup")
        args = parse_qs(parsed.query, strict_parsing=True)
        if set(args) != {"key"} or len(args["key"]) != 1 or not args["key"][0]:
            raise PermissionError("Invalid lookup parameters")
        if request.data is not None:
            raise PermissionError("GET cannot carry a request body")
    elif method == "POST":
        if mock_port is None:
            raise PermissionError("Production POST is disabled in P6.5D2B3")
        if match.group("lookup") or parsed.query:
            raise PermissionError("POST requires plain sends route")
        if not isinstance(request.data, bytes) or not request.data:
            raise InvalidHttpExchange("POST body is required")
        if not request.has_header("Idempotency-key"):
            raise InvalidHttpExchange("POST Idempotency-Key is required")
        try:
            body = json.loads(request.data)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise InvalidHttpExchange("POST JSON is invalid") from error
        if not isinstance(body, dict) or not (
            isinstance(body.get("threadId"), str) and body["threadId"]
            and isinstance(body.get("text"), str) and body["text"].strip()
        ):
            raise InvalidHttpExchange("POST payload is incomplete")
    else:
        raise PermissionError("Unsupported HTTP method")

    if not request.has_header("Authorization"):
        raise PermissionError("Bearer authorization is required")
    if mock_port is not None and request.get_header("Authorization") != "Bearer synthetic":
        raise PermissionError("Mock HTTP must use synthetic credentials")
    return method


def _read_json_response(response):
    # HTTPError has the same read()/status API as an HTTPResponse.
    data = response.read(MAX_RESPONSE_BYTES + 1)
    if len(data) > MAX_RESPONSE_BYTES:
        raise InvalidHttpExchange("HTTP response exceeds size limit")
    try:
        payload = json.loads(data)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise InvalidHttpExchange("HTTP response must be JSON") from error
    if not isinstance(payload, (dict, list)):
        raise InvalidHttpExchange("HTTP JSON response has unexpected type")
    return HttpExchangeResult(int(response.code), payload)


class RestrictedHttpExecutor:
    """Actual HTTP I/O, but production POST is mechanically impossible.

    Default instance is read-only against 127.0.0.1:6174/v1. Set mock_port
    only for an independently started loopback fixture on another port; that
    allows POST *to the fixture*, never to the Agent WeChat daemon.
    """

    def __init__(self, *, mock_port: int | None = None, timeout_seconds=5.0):
        if mock_port is not None and (
            type(mock_port) is not int or not 1024 <= mock_port <= 65535
            or mock_port == 6174
        ):
            raise InvalidHttpExchange("Invalid mock HTTP port")
        if (type(timeout_seconds) not in (int, float)
                or not 0 < timeout_seconds <= 10):
            raise InvalidHttpExchange("Invalid HTTP timeout")
        self._mock_port = mock_port
        self._timeout_seconds = float(timeout_seconds)

    def perform(self, request: Request) -> HttpExchangeResult:
        _verify_request(request, self._mock_port)
        # Disallow environment proxies, credentials, and cross-host redirects.
        opener = build_opener(ProxyHandler({}), _NeverRedirect())
        try:
            with opener.open(request, timeout=self._timeout_seconds) as response:
                return _read_json_response(response)
        except HTTPError as error:
            with error:
                return _read_json_response(error)


def mock_request(request: Request, port: int) -> Request:
    """Readdress only a *synthetic* test request to a loopback fixture.

    Keeps body, method, and headers unchanged; cannot target daemon port 6174.
    The returned Request is always revalidated by RestrictedHttpExecutor.
    """
    if type(port) is not int or not 1024 <= port <= 65535 or port == 6174:
        raise InvalidHttpExchange("Invalid mock HTTP port")
    url = request.full_url
    if request.get_header("Authorization") != "Bearer synthetic":
        raise PermissionError("Mock HTTP must use synthetic credentials")
    if not url.startswith(BASE_URL + "/accounts/"):
        raise PermissionError("Only canonical Agent WeChat contracts are accepted")
    rewritten = "http://127.0.0.1:" + str(port) + url[len("http://127.0.0.1:6174"):]
    return Request(
        rewritten, data=request.data, method=request.get_method(),
        headers=dict(request.header_items()),
    )
