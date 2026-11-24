from contextlib import closing
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import Mock, patch

from test_storage import V2_DDL
from wechat_ai_bot.migrations import migrate_v2_to_v3, migrate_v3_to_v4, migrate_v4_to_v5
from wechat_ai_bot.sender_automatic_policy import AutomaticReplyPolicy
from wechat_ai_bot.sender_schedule import ScheduledHistory, ZONE, scheduled_occurrences
from wechat_ai_bot.sender_service import Dispatcher, NativeSender, validate_settings
from wechat_ai_bot.storage import Storage


ACCOUNT = "account-fixture"
GROUP = "group-fixture@chatroom"
SCHEDULE = "scheduled fixture"
REPLY = "reply fixture"


class SenderServiceTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        db = self.root / "bot.sqlite3"
        with closing(sqlite3.connect(db)) as conn:
            conn.executescript(V2_DDL)
            migrate_v2_to_v3(conn)
            migrate_v3_to_v4(conn)
            migrate_v4_to_v5(conn)
        self.store = Storage(db)
        self.addCleanup(self.store.close)
        self.ledger = self.root / "schedule.sqlite3"
        self.history = ScheduledHistory(self.ledger)
        self.addCleanup(self.history.close)
        self.now = datetime.now(timezone.utc).replace(second=0, microsecond=0) + timedelta(minutes=1)
        self.settings = validate_settings({
            "targetGroupId": GROUP, "replyWindowEnabled": False, "replyStart": "09:00", "replyEnd": "22:00",
            "checkinEnabled": False, "checkinTime": "09:00",
            "limits": {"modelDaily": 50, "modelPerMinute": 3, "modelInterval": 20,
                       "replyHourly": 1, "replyDaily": 1, "replyInterval": 120},
            "scheduledMessages": [{"enabled": True, "cadence": "daily", "date": "",
                                   "time": self.now.astimezone(ZONE).strftime("%H:%M"), "text": SCHEDULE}],
        }, GROUP)
        self.config = {
            "schema_version": 2, "app": {"auto_send": False}, "sender": {"enabled": False},
            "runtime": {"global_pause": True}, "agent_wechat": {"account_id": ACCOUNT, "target_group_id": GROUP},
            "safety": {"group_allowlist": [GROUP]},
        }
        self.policy = AutomaticReplyPolicy(ACCOUNT, GROUP, 1, 100, int(self.now.timestamp()) - 300,
                                           int(self.now.timestamp()) + 7200, True, False)
        self.native = Mock(spec=NativeSender)
        self.native.confirm.return_value = ("verified-id", True)

    def tick(self, history=None):
        return Dispatcher(self.store, history or self.history, self.native, self.config,
                          lambda: (self.policy, self.settings), lambda: self.now).tick()

    def reply(self, cursor):
        message = f"message-{cursor}"
        self.store.persist_event(ACCOUNT, str(cursor), "message.committed", GROUP, message)
        self.store.persist_message(ACCOUNT, message, GROUP, direction="incoming", body="incoming",
                                   created_at=(self.now - timedelta(seconds=1)).isoformat(), source_cursor=str(cursor))
        self.store.decide_and_process(ACCOUNT, message, action="reply", reason="fixture",
                                      reply=REPLY, decision_status="proposed")

    def test_schedule_priority_then_reply_with_a_shared_cooldown_and_limits(self):
        self.assertEqual(self.tick(), "scheduled_sent")
        self.assertEqual(self.tick(), "group_send_cooldown")
        self.now += timedelta(seconds=125)
        # Write a fresh SQLite timestamp for the simulated later reply.
        self.reply(101)
        self.store.conn.execute("UPDATE decisions SET created_at=?", (self.now.isoformat(),))
        self.assertEqual(self.tick(), "reply_sent")
        self.now += timedelta(seconds=125)
        self.reply(102)
        self.assertEqual(self.tick(), "waiting_for_schedule")
        self.assertEqual(self.native.submit_once.call_count, 2)

    def test_schedule_recovers_by_lookup_after_restart_and_can_reconcile_while_paused(self):
        self.native.confirm.return_value = ("verified-id", False)
        self.assertEqual(self.tick(), "scheduled_needs_review")
        self.policy = replace(self.policy, paused=True)
        self.native.confirm.return_value = ("verified-id", True)
        with closing(ScheduledHistory(self.ledger)) as reopened:
            self.assertEqual(self.tick(reopened), "scheduled_sent")
            self.assertEqual(self.tick(reopened), "sender_paused")
        self.native.submit_once.assert_called_once()

    def test_native_has_one_post_and_confirms_the_exact_projection(self):
        claim, = scheduled_occurrences(self.settings, self.policy, self.now)
        native = NativeSender(ACCOUNT, GROUP, "synthetic")
        send = {"id": "verified-id", "accountId": ACCOUNT, "destination": GROUP,
                "localMessageId": "local-id", "serverId": "123", "state": "accepted"}
        msg = {"id": "local-id", "accountId": ACCOUNT, "threadId": GROUP, "serverId": "123",
               "direction": "outgoing", "type": 1, "body": SCHEDULE}
        with patch.object(native, "_exchange", side_effect=[(404, {}), (202, send),
                                                           (200, send), (200, {**msg, "body": "wrong"}),
                                                           (200, send), (200, msg)]) as exchange:
            remote = native.submit_once(claim)
            self.assertEqual(native.confirm(claim, remote), ("verified-id", False))
            self.assertEqual(native.confirm(claim, remote), ("verified-id", True))
            self.assertEqual([call.args[0].get_method() for call in exchange.call_args_list],
                             ["GET", "POST", "GET", "GET", "GET", "GET"])

    def test_missed_once_and_pre_activation_occurrences_do_not_send(self):
        day = self.now.astimezone(ZONE).date()
        plan = self.settings["scheduledMessages"][0]
        plan.update(cadence="once", date=(day - timedelta(days=1)).isoformat())
        self.assertFalse(scheduled_occurrences(self.settings, self.policy, self.now))
        plan.update(date=day.isoformat())
        self.assertFalse(scheduled_occurrences(self.settings, replace(self.policy, starts_at=int(self.now.timestamp()) + 1),
                                               self.now))
