"""P6.5D2D-2B2 reconstruction: synthetic same-database atomic claim.

Reuses the restricted 2B1 temporary ledger. The Outbox here is a synthetic
table, NOT production Storage. No human authentication, production policy,
HTTP, credentials, migrations, or real send authority is implemented.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
import sqlite3

from wechat_ai_bot.sender_approval_fixture import (
    ApprovalFailure, SyntheticApprovalLedger, SyntheticIntent, _digest,
)


class SyntheticAtomicApprovalFixture:
    """Prove grant consumption and Outbox reservation commit together.

    Accepts only the existing owned/private fixture temp directory, never an
    arbitrary database path. Deliberately exposes no standalone spend method.
    A claimed fixture row cannot be requeued or sent through this interface.
    """

    def __init__(self, private_temp_dir: Path):
        self._ledger = SyntheticApprovalLedger(private_temp_dir)
        try:
            self._ledger.conn.execute("""
                CREATE TABLE IF NOT EXISTS fixture_outbox (
                    outbox_id TEXT PRIMARY KEY,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    fingerprint TEXT NOT NULL UNIQUE,
                    status TEXT NOT NULL
                        CHECK(status IN ('queued','sending','needs_review'))
                )
            """)
        except BaseException:
            self._ledger.close()
            raise

    def close(self):
        self._ledger.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def seed_synthetic_outbox(self, intent: SyntheticIntent) -> None:
        """TEST-ONLY queue; stores a bound digest, never message plaintext."""
        fingerprint = _digest(intent)
        try:
            self._ledger.conn.execute(
                "INSERT INTO fixture_outbox VALUES (?,?,?,'queued')",
                (intent.outbox_id, intent.idempotency_key, fingerprint),
            )
        except sqlite3.IntegrityError as exc:
            raise ApprovalFailure("DUPLICATE_OUTBOX") from exc

    def seed_synthetic_approval(
        self, intent: SyntheticIntent, *, now: datetime, ttl_seconds: int = 60,
    ) -> str:
        """TEST-ONLY reviewer simulation, never trusted human approval."""
        return self._ledger.seed_synthetic_approval(
            intent, now=now, ttl_seconds=ttl_seconds,
        )

    def revoke(self, grant_id: str) -> bool:
        return self._ledger.revoke(grant_id)

    def emergency_stop(self) -> None:
        self._ledger.emergency_stop()

    def _claim_verdict(
        self, grant_id: str, intent: SyntheticIntent, now: datetime,
    ) -> str:
        # Both authorization and the persisted candidate are read AFTER taking
        # the write reservation. No second connection participates in a claim.
        verdict = self._ledger._check(grant_id, intent, now)
        if verdict != "FIXTURE_ELIGIBLE":
            return verdict
        row = self._ledger.conn.execute(
            "SELECT fingerprint,idempotency_key,status FROM fixture_outbox "
            "WHERE outbox_id=?", (intent.outbox_id,),
        ).fetchone()
        if row is None:
            return "OUTBOX_MISSING"
        if (row["fingerprint"] != _digest(intent)
                or row["idempotency_key"] != intent.idempotency_key):
            return "OUTBOX_SCOPE_MISMATCH"
        if row["status"] != "queued":
            return "OUTBOX_NOT_QUEUED"
        return "FIXTURE_ELIGIBLE"

    def claim_once(
        self, grant_id: str, intent: SyntheticIntent, *, now: datetime,
    ) -> dict:
        """Consume and reserve atomically, without executing any send.

        Uncommitted failures roll back both rows. A committed claim remains
        spent/sending across restart and MUST NOT imply permission to retry.
        Production dispatch_block_reason and authenticated approval remain
        separate, unimplemented integration requirements.
        """
        conn = self._ledger.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            verdict = self._claim_verdict(grant_id, intent, now)
            if verdict == "FIXTURE_ELIGIBLE":
                spent = conn.execute(
                    "UPDATE fixture_approval SET state='spent' "
                    "WHERE grant_id=? AND state='pending'", (grant_id,),
                ).rowcount
                claimed = conn.execute(
                    "UPDATE fixture_outbox SET status='sending' "
                    "WHERE outbox_id=? AND status='queued'", (intent.outbox_id,),
                ).rowcount
                if spent != 1 or claimed != 1:
                    raise ApprovalFailure("ATOMIC_TRANSITION_LOST")
                verdict = "FIXTURE_CLAIMED"
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        return {
            "stage": "P6.5D2D-2B2",
            "mode": "SYNTHETIC_ATOMIC_CLAIM_FIXTURE",
            "verdict": verdict,
            "fixture_claimed": verdict == "FIXTURE_CLAIMED",
            "live_send_authorized": False,
            "network_requests": 0,
            "token_read": False,
        }
