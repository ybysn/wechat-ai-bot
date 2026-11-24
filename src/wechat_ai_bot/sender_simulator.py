"""P6.5D2A offline-only Sender contract simulator.

NO HTTP client, token access, subprocess, WeChat API, or background service is
implemented here. This module is not connected to the deployed Worker.
The caller MUST provide an isolated SQLite fixture/snapshot and a FakeTransport.
"""

from dataclasses import dataclass
from pathlib import Path
from tempfile import gettempdir
from uuid import NAMESPACE_URL, uuid5


@dataclass(frozen=True)
class FakeReceipt:
    account_id: str
    thread_id: str
    content: str
    idempotency_key: str
    outcome: str
    remote_send_id: str | None


class FakeTransport:
    """In-memory remote receipt store; never opens a socket."""

    def __init__(self, outcome="accepted", *, lose_ack=False, lose_receipt=False):
        if outcome not in ("accepted", "failed", "needs_review"):
            raise ValueError("Invalid fake outcome")
        self.outcome = outcome
        self.lose_ack = lose_ack
        self.lose_receipt = lose_receipt
        self.records = {}
        self.submit_count = 0
        self.lookup_count = 0

    def submit(self, account_id, thread_id, content, idempotency_key):
        identity = (account_id, thread_id, content)
        if idempotency_key in self.records:
            original = self.records[idempotency_key]
            if (original.account_id, original.thread_id, original.content) != identity:
                raise ValueError("Conflicting reuse of idempotency key")
            return original
        self.submit_count += 1
        remote_id = "sim-" + uuid5(NAMESPACE_URL, idempotency_key).hex
        receipt = FakeReceipt(
            account_id, thread_id, content, idempotency_key,
            self.outcome, remote_id,
        )
        if not self.lose_receipt:
            self.records[idempotency_key] = receipt
        if self.lose_ack:
            raise TimeoutError("Synthetic dropped response")
        return receipt

    def lookup_by_key(self, idempotency_key):
        self.lookup_count += 1
        return self.records.get(idempotency_key)


def simulate_send_once(store, outbox_id, config, transport, *, now=None):
    """Exercise DB transitions with FakeTransport and a caller-owned fixture.

    Caller must NOT pass a production Storage instance.  This function has no
    network capability and returns (updated, reason).  On an unverifiable
    outcome, it persists needs_review and never retries submission.
    """
    if type(transport) is not FakeTransport:
        raise TypeError("Only FakeTransport may be used by sender simulator")
    # Block accidental simulation against the real persistent DB.  Synthetic
    # fixtures live in a named temporary directory (not merely /tmp itself).
    db_name = store.conn.execute("PRAGMA database_list").fetchone()[2]
    db_path = Path(db_name).resolve() if db_name else None
    temp_root = Path(gettempdir()).resolve()
    if (
        db_path is None
        or not db_path.is_relative_to(temp_root)
        or db_path.parent == temp_root
        or not db_path.parent.name.startswith(("tmp", "wechat-p65d2-"))
    ):
        raise PermissionError("Simulator requires a temporary SQLite fixture")
    claimed, reason = store.claim_outbox_for_send(outbox_id, config, now=now)
    if not claimed:
        return False, reason
    row = store.conn.execute(
        "SELECT account_id,thread_id,content,idempotency_key "
        "FROM outbox WHERE outbox_id=?", (outbox_id,),
    ).fetchone()
    try:
        receipt = transport.submit(
            row["account_id"], row["thread_id"],
            row["content"], row["idempotency_key"],
        )
    except TimeoutError:
        # Lookup same key; never submit again on unknown outcome.
        receipt = transport.lookup_by_key(row["idempotency_key"])
    except Exception:
        # Unknown local/synthetic error, fail closed.
        receipt = None

    identity_ok = (
        isinstance(receipt, FakeReceipt)
        and receipt.account_id == row["account_id"]
        and receipt.thread_id == row["thread_id"]
        and receipt.content == row["content"]
        and receipt.idempotency_key == row["idempotency_key"]
    )
    if not identity_ok:
        return store.quarantine_unresolved_send(outbox_id)
    return store.record_send_result(
        outbox_id,
        receipt.idempotency_key,
        receipt.outcome,
        remote_send_id=receipt.remote_send_id,
        now=now,
    )
