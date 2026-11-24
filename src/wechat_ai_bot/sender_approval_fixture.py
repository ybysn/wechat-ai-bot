"""P6.5D2D-2B1: isolated, *synthetic-only* single-use approval experiment.

Not a production authorization gate, token issuer, sender, or Outbox writer.
Its sole purpose is to test the security semantics of message-scoped grants,
revocation, expiry, emergency-stop and crash-persistent at-most-once use.

A future production design needs a trusted human issuer, authenticated grants,
transactional integration with Storage, and independent safety review. Merely
creating a grant here never conveys authority to send a real WeChat message.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import secrets
import sqlite3
import stat
import tempfile

from wechat_ai_bot.sender_http_fixture import (
    FIXTURE_ACCOUNT, FIXTURE_THREAD, FIXTURE_TEXT_PREFIX,
)

_MAX_LIFETIME_SECONDS = 120
_MAX_MESSAGE_BYTES = 2048
_TEMP_PREFIX = "p65d2d2b-fixture-"


class ApprovalFailure(ValueError):
    """Fixed-code local validation failure. Do not include user content."""


@dataclass(frozen=True)
class SyntheticIntent:
    account_id: str
    thread_id: str
    outbox_id: str
    idempotency_key: str
    text: str


def _utc(now: datetime) -> datetime:
    if not isinstance(now, datetime) or now.tzinfo is None:
        raise ApprovalFailure("INVALID_UTC_TIME")
    current = now.astimezone(timezone.utc)
    if current.utcoffset() != timedelta(0):
        raise ApprovalFailure("INVALID_UTC_TIME")
    return current


def _digest(intent: SyntheticIntent) -> str:
    if type(intent) is not SyntheticIntent:
        raise ApprovalFailure("INVALID_INTENT")
    if intent.account_id != FIXTURE_ACCOUNT or intent.thread_id != FIXTURE_THREAD:
        raise ApprovalFailure("REAL_TARGET_FORBIDDEN")
    if (not isinstance(intent.outbox_id, str) or not intent.outbox_id.startswith("fixture-")
        or not 9 <= len(intent.outbox_id) <= 128
        or not isinstance(intent.idempotency_key, str)
        or not intent.idempotency_key.startswith("fixture-")
        or not 9 <= len(intent.idempotency_key) <= 128):
        raise ApprovalFailure("INVALID_SYNTHETIC_IDENTITY")
    if (not isinstance(intent.text, str)
            or not intent.text.startswith(FIXTURE_TEXT_PREFIX)):
        raise ApprovalFailure("SYNTHETIC_CONTENT_REQUIRED")
    payload = intent.text.encode("utf-8")
    if not 1 <= len(payload) <= _MAX_MESSAGE_BYTES:
        raise ApprovalFailure("INVALID_MESSAGE_LENGTH")
    record = json.dumps({
        "account_id": intent.account_id,
        "thread_id": intent.thread_id,
        "outbox_id": intent.outbox_id,
        "idempotency_key": intent.idempotency_key,
        "content_sha256": sha256(payload).hexdigest(),
    }, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return sha256(record).hexdigest()


def _private_fixture_dir(root: Path) -> Path:
    if not isinstance(root, Path) or root.is_symlink():
        raise ApprovalFailure("TEMP_FIXTURE_REQUIRED")
    try:
        path = root.resolve(strict=True)
        info = root.stat()
        parent = Path(tempfile.gettempdir()).resolve(strict=True)
    except OSError as exc:
        raise ApprovalFailure("TEMP_FIXTURE_REQUIRED") from exc
    if (path.parent != parent or not path.name.startswith(_TEMP_PREFIX)
            or not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) & 0o077):
        raise ApprovalFailure("TEMP_FIXTURE_REQUIRED")
    return path


class SyntheticApprovalLedger:
    """Durable single-use trial *only* under an owned private temp directory.

    Never connects to production Storage. Does not accept a database path,
    read tokens, implement a WeChat HTTP client, or return live send authority.
    """

    def __init__(self, private_temp_dir: Path):
        folder = _private_fixture_dir(private_temp_dir)
        path = folder / "synthetic-approvals.sqlite3"
        if path.is_symlink():
            raise ApprovalFailure("TEMP_FIXTURE_REQUIRED")
        if path.exists():
            info = path.stat()
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                or info.st_nlink != 1 or stat.S_IMODE(info.st_mode) & 0o077):
                raise ApprovalFailure("TEMP_FIXTURE_REQUIRED")
        else:
            # SQLite's default newly created mode can honor an unsafe umask.
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            os.close(fd)
        self.conn = sqlite3.connect(str(path), timeout=2, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA busy_timeout=2000")
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS fixture_approval (
                grant_id TEXT PRIMARY KEY,
                fingerprint TEXT NOT NULL UNIQUE,
                issued_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                state TEXT NOT NULL CHECK(state IN ('pending','spent','revoked'))
            );
            CREATE TABLE IF NOT EXISTS fixture_control (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            INSERT OR IGNORE INTO fixture_control(key, value) VALUES ('stopped', '0');
        """)

    def close(self):
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def seed_synthetic_approval(self, intent: SyntheticIntent, *, now: datetime,
                                ttl_seconds: int = 60) -> str:
        """TEST-ONLY seed. Simulates a reviewer; is NOT human authentication."""
        now = _utc(now)
        fingerprint = _digest(intent)
        if type(ttl_seconds) is not int or not 1 <= ttl_seconds <= _MAX_LIFETIME_SECONDS:
            raise ApprovalFailure("INVALID_TTL")
        grant_id = "fixture-grant-" + secrets.token_hex(16)
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            if self._stopped():
                raise ApprovalFailure("EMERGENCY_STOP_ACTIVE")
            self.conn.execute(
                "INSERT INTO fixture_approval VALUES (?,?,?,?,?)",
                (grant_id, fingerprint, now.isoformat(),
                 (now + timedelta(seconds=ttl_seconds)).isoformat(), "pending"),
            )
            self.conn.commit()
        except sqlite3.IntegrityError as exc:
            self.conn.rollback()
            raise ApprovalFailure("DUPLICATE_INTENT") from exc
        except BaseException:
            self.conn.rollback()
            raise
        return grant_id

    def _stopped(self) -> bool:
        value = self.conn.execute(
            "SELECT value FROM fixture_control WHERE key='stopped'"
        ).fetchone()
        return value is None or value[0] != "0"

    def _check(self, grant_id: str, intent: SyntheticIntent, now: datetime) -> str:
        now = _utc(now)
        fingerprint = _digest(intent)
        if not isinstance(grant_id, str) or not grant_id.startswith("fixture-grant-"):
            return "GRANT_NOT_FOUND"
        if self._stopped():
            return "EMERGENCY_STOP_ACTIVE"
        row = self.conn.execute(
            "SELECT fingerprint, issued_at, expires_at, state "
            "FROM fixture_approval WHERE grant_id=?", (grant_id,),
        ).fetchone()
        if row is None:
            return "GRANT_NOT_FOUND"
        if row["state"] != "pending":
            return "GRANT_" + row["state"].upper()
        if row["fingerprint"] != fingerprint:
            return "SCOPE_MISMATCH"
        issued = datetime.fromisoformat(row["issued_at"])
        expires = datetime.fromisoformat(row["expires_at"])
        if now < issued:
            return "BEFORE_ISSUE_TIME"
        if now >= expires:
            return "GRANT_EXPIRED"
        return "FIXTURE_ELIGIBLE"

    def inspect(self, grant_id: str, intent: SyntheticIntent, *, now: datetime) -> dict:
        verdict = self._check(grant_id, intent, now)
        return {
            "stage": "P6.5D2D-2B1",
            "mode": "SYNTHETIC_APPROVAL_FIXTURE",
            "verdict": verdict,
            "fixture_eligible": verdict == "FIXTURE_ELIGIBLE",
            "live_send_authorized": False,
            "network_requests": 0,
            "token_read": False,
        }

    def spend_once(self, grant_id: str, intent: SyntheticIntent, *, now: datetime) -> str:
        """At-most-once TEST consumption. Spent BEFORE any hypothetical POST.

        A crash leaves the grant spent; never automatically retries with a
        fresh idempotency key. The caller must still use its separate Outbox
        and HTTP test fixture, never a real WeChat endpoint.
        """
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            verdict = self._check(grant_id, intent, now)
            if verdict == "FIXTURE_ELIGIBLE":
                changed = self.conn.execute(
                    "UPDATE fixture_approval SET state='spent' "
                    "WHERE grant_id=? AND state='pending'", (grant_id,),
                ).rowcount
                if changed != 1:
                    verdict = "GRANT_UNAVAILABLE"
            self.conn.commit()
            return "FIXTURE_SPENT" if verdict == "FIXTURE_ELIGIBLE" else verdict
        except BaseException:
            self.conn.rollback()
            raise

    def revoke(self, grant_id: str) -> bool:
        """Only pending fixture approvals can be revoked. No un-revoke API."""
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            changed = self.conn.execute(
                "UPDATE fixture_approval SET state='revoked' "
                "WHERE grant_id=? AND state='pending'", (grant_id,),
            ).rowcount
            self.conn.commit()
            return changed == 1
        except BaseException:
            self.conn.rollback()
            raise

    def emergency_stop(self):
        """Persistent, irreversible for this fixture ledger."""
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            self.conn.execute(
                "UPDATE fixture_control SET value='1' WHERE key='stopped'"
            )
            self.conn.commit()
        except BaseException:
            self.conn.rollback()
            raise
