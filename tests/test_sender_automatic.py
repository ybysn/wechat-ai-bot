from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import sqlite3
import tempfile
from threading import Barrier, Event
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

from test_storage import V2_DDL
from wechat_ai_bot.migrations import (
    backup_and_migrate_v4_to_v5, migrate_v2_to_v3, migrate_v3_to_v4, migrate_v4_to_v5,
)
from wechat_ai_bot.process_pending import process_one
from wechat_ai_bot import sender_automatic_policy
from wechat_ai_bot.sender_automatic_fixture import run_automatic_fixture_tick, run_controlled_automatic_fixture_tick
from wechat_ai_bot.sender_automatic_policy import (
    parse_automatic_reply_policy, pause_automatic_reply, read_automatic_reply_policy,
)
from wechat_ai_bot.sender_dry_run import inspect_snapshot
from wechat_ai_bot.sender_http_contract import encode_text_send_body
from wechat_ai_bot.sender_http_fixture import FIXTURE_ACCOUNT, FIXTURE_TEXT_PREFIX, FIXTURE_THREAD
from wechat_ai_bot.sender_http_offline import ScriptedHttpExecutor, ScriptedHttpResponse
from wechat_ai_bot.sender_role_boundary import BoundaryFailure
from wechat_ai_bot.storage import Storage


class AutomaticSenderTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="wechat-p65d2-auto-")
        self.addCleanup(temp.cleanup)
        self.path = Path(temp.name) / "fixture.sqlite3"
        with closing(sqlite3.connect(self.path)) as conn:
            conn.executescript(V2_DDL)
            migrate_v2_to_v3(conn)
            migrate_v3_to_v4(conn)
            self.assertTrue(migrate_v4_to_v5(conn))
            self.assertFalse(migrate_v4_to_v5(conn))
        self.store = Storage(self.path)
        self.addCleanup(self.store.close)
        # Decisions use SQLite CURRENT_TIMESTAMP. Keep the fixture clock slightly ahead.
        self.now = datetime.now(timezone.utc) + timedelta(seconds=2)
        self.text = FIXTURE_TEXT_PREFIX + "自动回复"
        self.observe = {
            "schema_version": 2, "app": {"auto_send": False}, "sender": {"enabled": False},
            "runtime": {"global_pause": True},
            "agent_wechat": {"account_id": FIXTURE_ACCOUNT, "target_group_id": FIXTURE_THREAD},
            "safety": {"group_allowlist": [FIXTURE_THREAD]},
        }
        self.policy_input = {
            "schema_version": 1, "role": "automatic_reply", "account_id": FIXTURE_ACCOUNT,
            "thread_id": FIXTURE_THREAD, "generation": 1, "activation_cursor": 100,
            "starts_at": int(self.now.timestamp()) - 60, "expires_at": int(self.now.timestamp()) + 120,
            "enabled": True, "paused": False,
        }
        self.policy = parse_automatic_reply_policy(self.policy_input)

    def candidate(self, cursor=101, *, age=10, kind="message.committed", action="reply", text=None):
        message_id = f"fixture-message-{cursor}"
        self.store.persist_event(FIXTURE_ACCOUNT, str(cursor), kind, FIXTURE_THREAD, message_id)
        self.store.persist_message(
            FIXTURE_ACCOUNT, message_id, FIXTURE_THREAD, direction="incoming", body="fixture input",
            created_at=(self.now - timedelta(seconds=age)).isoformat(), source_cursor=str(cursor),
        )
        decision_id, _ = self.store.decide_and_process(
            FIXTURE_ACCOUNT, message_id, action=action, reason="fixture",
            reply=(text or self.text) if action == "reply" else None,
            decision_status="proposed" if action == "reply" else "rejected",
        )
        return decision_id

    def status(self):
        return [row[0] for row in self.store.conn.execute("SELECT status FROM outbox ORDER BY rowid")]

    def claim(self, policy=None):
        return self.store.claim_automatic_reply(self.observe, policy or self.policy, now=self.now)

    def tick(self, executor, read_policy=None):
        return run_automatic_fixture_tick(
            self.store, read_policy or (lambda: (self.observe, self.policy)), executor, now=lambda: self.now,
        )

    def receipt(self, state="accepted", *, status=200, **overrides):
        return ScriptedHttpResponse(status, {
            "id": "fixture-send", "accountId": FIXTURE_ACCOUNT, "destination": FIXTURE_THREAD,
            "state": state, "serverId": "12345678901234567890", "localMessageId": "fixture-local",
            **overrides,
        })

    def projection(self, **overrides):
        return ScriptedHttpResponse(200, {
            "id": "fixture-local", "serverId": "12345678901234567890", "accountId": FIXTURE_ACCOUNT,
            "threadId": FIXTURE_THREAD, "direction": "outgoing", "type": 1, "body": self.text,
            **overrides,
        })

    def test_full_path_without_an_approval_envelope(self):
        self.store.persist_event(FIXTURE_ACCOUNT, "101", "message.committed", FIXTURE_THREAD, "fixture-incoming")
        self.store.persist_message(
            FIXTURE_ACCOUNT, "fixture-incoming", FIXTURE_THREAD, direction="incoming", body="fixture input",
            created_at=(self.now - timedelta(seconds=10)).isoformat(), source_cursor="101",
        )
        config = {**self.observe, "processing": {"max_context_messages": 12}, "ollama": {"model": "qwen3.5:9b"}}
        decision = process_one(
            self.store, config, lambda ctx, cfg: {"action": "reply", "reason": "fixture", "reply": self.text},
            activation_cursor="100",
        )["decision_id"]
        self.assertEqual(self.status(), [])
        executor = ScriptedHttpExecutor([
            ("POST", self.receipt("queued", status=202)), ("GET", self.receipt()), ("GET", self.projection()),
        ])
        path = self.path.with_name("automatic.json")
        path.write_text(json.dumps(self.policy_input), encoding="utf-8")
        path.chmod(0o600)
        with patch.object(executor, "perform", wraps=executor.perform) as requests:
            self.assertEqual(self.tick(executor, lambda: (
                self.observe, read_automatic_reply_policy(path, owner_uid=os.getuid()),
            )), "sent")
        post, lookup, projection = [call.args[0] for call in requests.call_args_list]
        key = post.get_header("Idempotency-key")
        self.assertRegex(key, r"^send-[0-9a-f]{32}$")
        self.assertEqual(parse_qs(urlsplit(lookup.full_url).query), {"key": [key]})
        self.assertTrue(urlsplit(projection.full_url).path.endswith("/messages/fixture-local"))
        row = self.store.conn.execute("SELECT * FROM automatic_dispatch").fetchone()
        self.assertEqual(row["policy_digest"], self.policy.digest)
        self.assertEqual(row["request_sha256"], sha256(post.data).hexdigest())
        self.assertEqual(row["source_cursor"], "101")
        self.assertFalse(self.store.conn.execute("SELECT 1 FROM sqlite_master WHERE name='sender_approvals'").fetchone())
        self.assertEqual(self.status(), ["sent"])
        self.assertEqual(self.store.automatic_dispatch(row["outbox_id"]).decision_id, decision)
        self.candidate(102)
        self.assertEqual(self.claim()[1], "group_send_cooldown")
        self.assertEqual(self.status(), ["sent"])

    def test_policy_time_scope_and_observer_config_are_enforced(self):
        self.candidate()
        for policy in (
            replace(self.policy, enabled=False), replace(self.policy, paused=True),
            replace(self.policy, starts_at=int(self.now.timestamp()) + 1),
            replace(self.policy, expires_at=int(self.now.timestamp())),
            replace(self.policy, thread_id="other-group@chatroom"),
        ):
            self.assertEqual(self.claim(policy), (None, "automatic_policy_inactive"))
        self.observe["app"]["auto_send"] = True
        self.assertEqual(self.claim(), (None, "automatic_policy_inactive"))
        self.assertEqual(self.status(), [])
        for obj in (None, {**self.policy_input, "generation": True}, {**self.policy_input, "token_file": "bad"}):
            with self.assertRaises(BoundaryFailure):
                parse_automatic_reply_policy(obj)

    def test_policy_file_rejects_missing_links_and_untrusted_writes(self):
        path = self.path.with_name("automatic.json")
        with self.assertRaises(BoundaryFailure):
            read_automatic_reply_policy(path, owner_uid=os.getuid())
        path.write_text(json.dumps(self.policy_input), encoding="utf-8")
        path.chmod(0o660)
        with self.assertRaisesRegex(BoundaryFailure, "AUTOMATIC_POLICY_PERMISSIONS"):
            read_automatic_reply_policy(path, owner_uid=os.getuid())
        path.chmod(0o640)
        link = path.with_name("linked.json")
        link.symlink_to(path)
        with self.assertRaises(BoundaryFailure):
            read_automatic_reply_policy(link, owner_uid=os.getuid())
        path.write_text('{"enabled":true,"enabled":false}', encoding="utf-8")
        with self.assertRaises(BoundaryFailure):
            read_automatic_reply_policy(path, owner_uid=os.getuid())

    def test_pause_is_persisted_before_confirmation_waits_for_dispatch(self):
        self.candidate()
        path = self.path.with_name("automatic.json")
        path.write_text(json.dumps(self.policy_input), encoding="utf-8")
        path.chmod(0o600)
        lock = path.with_name(path.name + ".dispatch.lock")
        lock.touch(mode=0o600)
        executor = ScriptedHttpExecutor([
            ("POST", self.receipt("queued", status=202)), ("GET", self.receipt("queued")),
        ])
        post_entered, pause_persisted, release_post = Event(), Event(), Event()
        perform = executor.perform
        publish = sender_automatic_policy._publish_paused_policy

        def delayed_post(request):
            if request.get_method() == "POST":
                post_entered.set()
                self.assertTrue(release_post.wait(5))
            return perform(request)

        def notify_pause(path, policy):
            publish(path, policy)
            pause_persisted.set()

        def dispatch():
            with closing(Storage(self.path)) as store:
                return run_controlled_automatic_fixture_tick(store, self.observe, path, executor, now=lambda: self.now)

        with patch.object(executor, "perform", side_effect=delayed_post), \
                patch.object(sender_automatic_policy, "_publish_paused_policy", side_effect=notify_pause), \
                ThreadPoolExecutor(max_workers=2) as pool:
            tick = pool.submit(dispatch)
            try:
                self.assertTrue(post_entered.wait(3))
                stop = pool.submit(pause_automatic_reply, path, owner_uid=os.getuid())
                self.assertTrue(pause_persisted.wait(3))
                self.assertTrue(read_automatic_reply_policy(path, owner_uid=os.getuid()).paused)
                self.assertFalse(stop.done())
                contender = ScriptedHttpExecutor([])
                self.assertEqual(run_controlled_automatic_fixture_tick(
                    self.store, self.observe, path, contender, now=lambda: self.now,
                ), "automatic_dispatch_busy")
                self.assertEqual(contender.calls, [])
            finally:
                release_post.set()
            self.assertEqual(tick.result(timeout=3), "needs_review")
            paused = stop.result(timeout=3)

        self.assertTrue(paused.paused)
        self.assertEqual(paused.generation, self.policy.generation + 1)
        self.assertEqual(pause_automatic_reply(path, owner_uid=os.getuid()), paused)
        lookup_only = ScriptedHttpExecutor([("GET", self.receipt()), ("GET", self.projection())])
        self.assertEqual(run_controlled_automatic_fixture_tick(
            self.store, self.observe, path, lookup_only, now=lambda: self.now,
        ), "sent")
        self.assertEqual([method for method, _ in lookup_only.calls], ["GET", "GET"])
        self.candidate(102)
        after_stop = ScriptedHttpExecutor([])
        self.assertEqual(run_controlled_automatic_fixture_tick(
            self.store, self.observe, path, after_stop, now=lambda: self.now,
        ), "automatic_policy_inactive")
        self.assertEqual(after_stop.calls, [])
        self.assertEqual(self.status(), ["sent"])

    def test_dispatch_requires_a_preinstalled_protected_lock(self):
        self.candidate()
        path = self.path.with_name("automatic.json")
        path.write_text(json.dumps(self.policy_input), encoding="utf-8")
        path.chmod(0o600)
        lock = path.with_name(path.name + ".dispatch.lock")
        executor = ScriptedHttpExecutor([])
        for mode, reason in ((None, "automatic_lock_unavailable"), (0o660, "automatic_lock_permissions")):
            if mode is not None:
                lock.touch(mode=0o600)
                lock.chmod(mode)
            self.assertEqual(run_controlled_automatic_fixture_tick(
                self.store, self.observe, path, executor, now=lambda: self.now,
            ), reason)
            with self.assertRaisesRegex(BoundaryFailure, reason.upper()):
                pause_automatic_reply(path, owner_uid=os.getuid())
        self.assertEqual(executor.calls, [])
        self.assertEqual(self.status(), [])
        self.assertFalse(read_automatic_reply_policy(path, owner_uid=os.getuid()).paused)

    def test_history_recovery_and_pre_policy_messages_cannot_send(self):
        self.candidate(100)
        self.assertEqual(self.claim()[1], "no_automatic_candidate")
        self.candidate(101, kind="recovery.checkpoint")
        self.assertEqual(self.claim()[1], "no_automatic_candidate")
        self.candidate(102, age=70)
        self.assertEqual(self.claim()[1], "reply_expired_or_predates_policy")
        self.assertEqual(self.status(), [])

    def test_latest_skip_does_not_resurrect_an_earlier_reply(self):
        self.candidate()
        self.candidate(102, action="skip")
        self.assertEqual(self.claim()[1], "no_automatic_candidate")
        self.assertEqual(self.status(), [])

    def test_pause_or_newer_candidate_before_post_cancels_the_claim(self):
        self.candidate()
        snapshots = iter([(self.observe, self.policy), (self.observe, replace(self.policy, paused=True))])
        executor = ScriptedHttpExecutor([])
        self.assertEqual(self.tick(executor, lambda: next(snapshots)), "automatic_policy_changed")
        self.assertEqual(self.status(), ["cancelled"])
        self.assertEqual(executor.calls, [])
        self.candidate(102)
        reads = 0

        def new_candidate_between_reads():
            nonlocal reads
            reads += 1
            if reads == 2:
                self.candidate(103)
            return self.observe, self.policy

        self.assertEqual(self.tick(executor, new_candidate_between_reads), "newer_reply_preferred")
        self.assertEqual(self.status(), ["cancelled", "cancelled"])
        self.assertEqual(executor.calls, [])

    def test_timeout_recovery_never_resubmits_and_checks_exact_body(self):
        self.candidate()
        lost = ScriptedHttpExecutor([("POST", TimeoutError()), ("GET", ScriptedHttpResponse(404, {}))])
        self.assertEqual(self.tick(lost), "needs_review")
        first = self.store.unresolved_automatic_dispatch(FIXTURE_ACCOUNT, FIXTURE_THREAD)
        self.candidate(102)
        with closing(Storage(self.path)) as restarted:
            for projection in (self.projection(body="wrong"), self.projection(recall={"observedAt": "fixture"})):
                wrong = ScriptedHttpExecutor([("GET", self.receipt()), ("GET", projection)])
                self.assertEqual(run_automatic_fixture_tick(
                    restarted, lambda: self.fail("Recovery must precede policy/claim"), wrong, now=lambda: self.now,
                ), "needs_review")
        self.assertEqual(self.status(), ["needs_review"])
        self.assertEqual(self.claim()[1], "unresolved_send_exists")
        accepted = ScriptedHttpExecutor([("GET", self.receipt()), ("GET", self.projection())])
        with patch.object(accepted, "perform", wraps=accepted.perform) as requests:
            self.assertEqual(self.tick(accepted), "sent")
        lookup = requests.call_args_list[0].args[0]
        self.assertEqual(parse_qs(urlsplit(lookup.full_url).query), {"key": [first.idempotency_key]})
        self.assertEqual(self.status(), ["sent"])

    def test_unverified_remote_id_cannot_replace_post_identity(self):
        self.candidate()
        script = ScriptedHttpExecutor([
            ("POST", self.receipt("queued", status=202)), ("GET", self.receipt(id="different-send")),
        ])
        self.assertEqual(self.tick(script), "needs_review")
        self.assertEqual(self.store.unresolved_automatic_dispatch(FIXTURE_ACCOUNT, FIXTURE_THREAD).remote_send_id,
                         "fixture-send")

    def test_durable_claim_is_single_use_and_binds_the_exact_request(self):
        self.candidate()
        claim, reason = self.claim()
        self.assertEqual(reason, "claimed_without_network_send")
        self.assertEqual(claim.request_sha256, sha256(encode_text_send_body(thread_id=FIXTURE_THREAD, text=self.text)).hexdigest())
        with closing(Storage(self.path)) as second:
            self.assertEqual(second.claim_automatic_reply(self.observe, self.policy, now=self.now)[1],
                             "no_automatic_candidate")
            self.assertIsNotNone(second.unresolved_automatic_dispatch(FIXTURE_ACCOUNT, FIXTURE_THREAD))
        for change in ("status='queued'", "idempotency_key='new-key'", "content='different'"):
            with self.assertRaises(sqlite3.IntegrityError):
                self.store.conn.execute(f"UPDATE outbox SET {change}")
        self.assertEqual(self.store.record_send_result(claim.outbox_id, claim.idempotency_key,
                         "accepted", remote_send_id="unverified")[1], "automatic_result_verification_required")

    def test_concurrent_claim_has_one_winner(self):
        self.candidate()
        barrier = Barrier(2)

        def compete():
            with closing(Storage(self.path)) as contender:
                barrier.wait(timeout=4)
                return contender.claim_automatic_reply(self.observe, self.policy, now=self.now)[1]

        with ThreadPoolExecutor(max_workers=2) as pool:
            jobs = [pool.submit(compete) for _ in range(2)]
            outcomes = [job.result(timeout=8) for job in jobs]
        self.assertCountEqual(outcomes, ["claimed_without_network_send", "no_automatic_candidate"])
        self.assertEqual(self.status(), ["sending"])
        self.assertEqual(self.store.conn.execute("SELECT COUNT(*) FROM automatic_dispatch").fetchone()[0], 1)

    def test_backup_and_upgrade_preserve_v4_rows_and_read_only_diagnostics(self):
        path = self.path.with_name("legacy.sqlite3")
        with closing(sqlite3.connect(path)) as conn:
            conn.executescript(V2_DDL)
            migrate_v2_to_v3(conn)
            migrate_v3_to_v4(conn)
        with closing(Storage(path)) as legacy:
            self.assertEqual(legacy.claim_automatic_reply(self.observe, self.policy, now=self.now)[1],
                             "automatic_schema_required")
            legacy.persist_message(FIXTURE_ACCOUNT, "legacy", FIXTURE_THREAD, direction="incoming",
                                   body="legacy body", created_at=self.now.isoformat(), source_cursor="50")
            decision, _ = legacy.decide(FIXTURE_ACCOUNT, "legacy", "reply", reply=self.text)
            self.assertTrue(legacy.accept_decision(decision, now=self.now))
            legacy.enqueue(decision, FIXTURE_ACCOUNT, FIXTURE_THREAD, self.text, now=self.now)
            original = tuple(legacy.conn.execute("SELECT * FROM outbox").fetchone())
        backup = backup_and_migrate_v4_to_v5(path)
        self.assertEqual(backup.stat().st_mode & 0o777, 0o600)
        with closing(Storage(backup)) as saved, closing(Storage(path)) as upgraded:
            self.assertFalse(saved.automatic_schema)
            self.assertTrue(upgraded.automatic_schema)
            self.assertEqual(tuple(saved.conn.execute("SELECT * FROM outbox").fetchone()), original)
            self.assertEqual(tuple(upgraded.conn.execute("SELECT * FROM outbox").fetchone()), original)
        self.assertIsNone(backup_and_migrate_v4_to_v5(path))
        config_path = self.path.with_name("observe.json")
        config_path.write_text(json.dumps({**self.observe, "storage": {"database": str(path)}}), encoding="utf-8")
        report = inspect_snapshot(
            config_path, now=self.now, automatic_policy_path=path.parent / "missing-directory" / "policy.json",
        )
        self.assertEqual(report["target_outbox_status_counts"]["queued"], 1)
        self.assertEqual(report["sql_writes"], 0)
        self.assertFalse(report["live_send_authorized"])
        self.assertEqual(report["automatic_reply"], {
            "database_schema": 5, "automatic_schema_available": True,
            "policy_status": "AUTOMATIC_POLICY_UNAVAILABLE", "unresolved_send_block": False,
        })

    def test_evidence_write_failure_rolls_back_decision_and_outbox(self):
        decision = self.candidate()
        self.store.conn.execute("CREATE TEMP TRIGGER reject_claim BEFORE INSERT ON automatic_dispatch "
                                "BEGIN SELECT RAISE(ABORT,'fixture failure'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            self.claim()
        self.assertEqual(self.status(), [])
        self.assertEqual(self.store.conn.execute("SELECT status FROM decisions WHERE decision_id=?",
                                                (decision,)).fetchone()[0], "proposed")
