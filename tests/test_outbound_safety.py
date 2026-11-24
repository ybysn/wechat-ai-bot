"""Offline P6.5D dispatch-claim policy tests; no WeChat sender exists."""

import sqlite3
import tempfile
import unittest
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from test_storage import V2_DDL
from wechat_ai_bot.migrations import migrate_v2_to_v3, migrate_v3_to_v4
from wechat_ai_bot.storage import Storage


class OutboundSafetyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / 'outbound.sqlite3'
        with sqlite3.connect(self.path) as conn:
            conn.executescript(V2_DDL)
            migrate_v2_to_v3(conn)
            migrate_v3_to_v4(conn)
        self.store = Storage(self.path)
        self.now = datetime.now(timezone.utc) + timedelta(seconds=5)
        self.config = {
            'schema_version':2,
            'app':{'auto_send':True},
            'sender':{'enabled':True},
            'runtime':{'global_pause':False},
            'agent_wechat':{'account_id':'a','target_group_id':'g'},
            'safety':{'group_allowlist':['g']},
        }
        self.serial = 100

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def queue_ai(self, *, age=20):
        self.serial += 1
        mid = 'm' + str(self.serial)
        self.store.persist_message(
            'a',mid,'g',direction='incoming',body='聊天内容',
            source_cursor=str(self.serial),
            created_at=(self.now-timedelta(seconds=age)).isoformat(),
        )
        decision, _ = self.store.decide(
            'a',mid,'reply',model='deepseek-flash',reply='安全的候选回复'+mid,
        )
        self.assertTrue(self.store.accept_decision(decision,now=self.now))
        outbox, created = self.store.enqueue(
            decision,'a','g','安全的候选回复'+mid,now=self.now,
        )
        self.assertTrue(created)
        return outbox

    def checkin_now(self):
        local = datetime.now(ZoneInfo('Asia/Shanghai'))
        due = local.replace(hour=9,minute=0,second=0,microsecond=0).astimezone(timezone.utc)
        task_id, _ = self.store.ensure_daily_checkin_task(
            'a','g',local.date().isoformat(),
        )
        outbox, created, status = self.store.enqueue_scheduled_task(task_id, now=due)
        self.assertEqual((created,status),(True,'queued'))
        return outbox,due

    def test_paused_sender_denies_claim(self):
        outbox = self.queue_ai()
        cfg = deepcopy(self.config)
        cfg['runtime']['global_pause'] = True
        self.assertEqual(self.store.claim_outbox_for_send(outbox,cfg,now=self.now),
                         (False,'sending_paused_or_target_mismatch'))
        self.assertEqual(self.store.conn.execute(
            'SELECT status FROM outbox WHERE outbox_id=?',(outbox,)).fetchone()[0],'queued')

    def test_auto_send_disabled_denies_claim(self):
        outbox = self.queue_ai()
        cfg = deepcopy(self.config)
        cfg['app']['auto_send']=False
        self.assertFalse(self.store.claim_outbox_for_send(outbox,cfg,now=self.now)[0])

    def test_disabled_sender_denies_claim(self):
        outbox = self.queue_ai()
        cfg = deepcopy(self.config)
        cfg['sender']['enabled']=False
        self.assertFalse(self.store.claim_outbox_for_send(outbox,cfg,now=self.now)[0])

    def test_group_not_allowlisted_denies_claim(self):
        outbox = self.queue_ai()
        cfg = deepcopy(self.config)
        cfg['safety']['group_allowlist']=[]
        self.assertFalse(self.store.claim_outbox_for_send(outbox,cfg,now=self.now)[0])

    def test_wrong_target_denies_claim(self):
        outbox = self.queue_ai()
        cfg = deepcopy(self.config)
        cfg['agent_wechat']['target_group_id']='wrong'
        self.assertFalse(self.store.claim_outbox_for_send(outbox,cfg,now=self.now)[0])

    def test_fresh_ai_claim_is_atomic_and_not_a_send(self):
        outbox = self.queue_ai()
        self.assertEqual(self.store.claim_outbox_for_send(outbox,self.config,now=self.now),
                         (True,'claimed_without_network_send'))
        self.assertEqual(self.store.claim_outbox_for_send(outbox,self.config,now=self.now),
                         (False,'outbox_not_queued'))
        row = self.store.conn.execute(
            'SELECT status,sent_at,remote_send_id FROM outbox WHERE outbox_id=?',(outbox,)
        ).fetchone()
        self.assertEqual(tuple(row),('sending',None,None))

    def test_old_reply_denied_after_queue(self):
        outbox = self.queue_ai(age=10)
        accepted, reason = self.store.claim_outbox_for_send(
            outbox,self.config,now=self.now+timedelta(seconds=121))
        self.assertEqual((accepted,reason),(False,'reply_expired_or_invalid'))

    def test_newest_fresh_reply_wins(self):
        old = self.queue_ai()
        newer = self.queue_ai()
        self.assertEqual(self.store.claim_outbox_for_send(old,self.config,now=self.now),
                         (False,'newer_reply_preferred'))
        self.assertEqual(self.store.claim_outbox_for_send(newer,self.config,now=self.now),
                         (True,'claimed_without_network_send'))
        self.assertEqual(self.store.conn.execute(
            'SELECT status FROM outbox WHERE outbox_id=?',(old,)).fetchone()[0],'queued')

    def test_in_flight_blocks_another_reply(self):
        first = self.queue_ai()
        self.assertTrue(self.store.claim_outbox_for_send(first,self.config,now=self.now)[0])
        other = self.queue_ai()
        self.assertEqual(self.store.claim_outbox_for_send(other,self.config,now=self.now),
                         (False,'unresolved_send_exists'))

    def test_uncertain_send_blocks_another_reply(self):
        first = self.queue_ai()
        self.store.conn.execute("UPDATE outbox SET status='needs_review' WHERE outbox_id=?",(first,))
        other = self.queue_ai()
        self.assertEqual(self.store.claim_outbox_for_send(other,self.config,now=self.now),
                         (False,'unresolved_send_exists'))

    def test_recent_sent_blocks_new_reply(self):
        first = self.queue_ai()
        self.store.conn.execute(
            "UPDATE outbox SET status='sent',sent_at=? WHERE outbox_id=?",
            ((self.now-timedelta(seconds=45)).isoformat(),first),
        )
        other = self.queue_ai()
        self.assertEqual(self.store.claim_outbox_for_send(other,self.config,now=self.now),
                         (False,'group_send_cooldown'))

    def test_sent_missing_timestamp_fails_closed(self):
        first = self.queue_ai()
        self.store.conn.execute("UPDATE outbox SET status='sent' WHERE outbox_id=?",(first,))
        other = self.queue_ai()
        self.assertEqual(self.store.claim_outbox_for_send(other,self.config,now=self.now),
                         (False,'sent_record_missing_timestamp'))

    def test_due_checkin_can_claim_without_sending(self):
        outbox,due = self.checkin_now()
        self.assertEqual(
            self.store.claim_outbox_for_send(outbox,self.config,now=due+timedelta(seconds=10)),
            (True,'claimed_without_network_send'))

    def test_delayed_checkin_is_rejected(self):
        outbox,due = self.checkin_now()
        self.assertEqual(
            self.store.claim_outbox_for_send(outbox,self.config,now=due+timedelta(seconds=121)),
            (False,'checkin_not_due_or_expired'))

    def test_queued_due_checkin_has_priority_over_ai(self):
        ai = self.queue_ai()
        checkin, due = self.checkin_now()
        # Simulate a fresh AI candidate at the check-in dispatch window.
        # Tests only modify their isolated SQLite fixture.
        self.store.conn.execute(
            "UPDATE messages SET created_at=? WHERE message_id=?",
            ((due+timedelta(seconds=1)).isoformat(), 'm101'),
        )
        self.store.conn.execute(
            "UPDATE decisions SET created_at=? WHERE source_message_id=?",
            ((due+timedelta(seconds=1)).strftime('%Y-%m-%d %H:%M:%S'), 'm101'),
        )
        moment = due+timedelta(seconds=10)
        self.assertEqual(self.store.claim_outbox_for_send(ai,self.config,now=moment),
                         (False,'due_checkin_priority'))
        self.assertTrue(self.store.claim_outbox_for_send(checkin,self.config,now=moment)[0])

    def test_sent_checkin_cools_down_ai_reply(self):
        checkin, due = self.checkin_now()
        # Historical sent record is scoped to the same account and group.
        self.store.conn.execute(
            "UPDATE outbox SET status='sent',sent_at=? WHERE outbox_id=?",
            ((self.now-timedelta(seconds=30)).isoformat(),checkin),
        )
        ai = self.queue_ai()
        self.assertEqual(self.store.claim_outbox_for_send(ai,self.config,now=self.now),
                         (False,'group_send_cooldown'))

    def test_cooldown_expired_allows_dispatch(self):
        first = self.queue_ai()
        self.store.conn.execute(
            "UPDATE outbox SET status='sent',sent_at=? WHERE outbox_id=?",
            ((self.now-timedelta(seconds=121)).isoformat(),first),
        )
        second = self.queue_ai()
        self.assertTrue(self.store.claim_outbox_for_send(second,self.config,now=self.now)[0])

    def test_missing_outbox_denied(self):
        self.assertEqual(self.store.claim_outbox_for_send('missing',self.config,now=self.now),
                         (False,'outbox_missing'))

    def test_naive_clock_rejected(self):
        outbox = self.queue_ai()
        with self.assertRaises(ValueError):
            self.store.claim_outbox_for_send(outbox,self.config,now=datetime.now())


if __name__ == '__main__':
    unittest.main()
