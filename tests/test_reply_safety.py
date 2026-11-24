"""P6.5C: never approve or enqueue stale AI replies (offline only)."""

import copy
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from test_process_pending import CFG
from test_storage import V2_DDL
from wechat_ai_bot.migrations import migrate_v2_to_v3, migrate_v3_to_v4
from wechat_ai_bot.process_pending import process_one
from wechat_ai_bot.reply_safety import candidate_is_fresh
from wechat_ai_bot.storage import Storage
from wechat_ai_bot.worker import AgentWorker


class ReplySafetyTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Path(self.tmp.name) / 'app.sqlite3'
        conn = sqlite3.connect(self.db)
        try:
            conn.executescript(V2_DDL)
            migrate_v2_to_v3(conn)
            migrate_v3_to_v4(conn)
        finally:
            conn.close()
        self.store = Storage(self.db)
        self.now = datetime.now(timezone.utc) + timedelta(seconds=5)

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def add(self, ident='m', age=30, timestamp=None):
        self.store.persist_message(
            'a', ident, 'g', direction='incoming',
            body='给我讲讲这个话题吧', source_cursor='201',
            created_at=(timestamp if timestamp is not None else (
                self.now - timedelta(seconds=age)).isoformat()),
        )

    def reply(self, ident='m'):
        decision, _ = self.store.decide(
            'a', ident, 'reply', reason='可以参与',
            model='deepseek-flash', reply='这话题值得聊聊。',
        )
        return decision

    def test_age_boundary_and_sqlite_utc_format(self):
        now = self.now
        self.assertTrue(candidate_is_fresh((now-timedelta(seconds=120)).isoformat(), now=now))
        self.assertFalse(candidate_is_fresh((now-timedelta(seconds=120, milliseconds=1)).isoformat(), now=now))
        self.assertFalse(candidate_is_fresh((now+timedelta(seconds=1)).isoformat(), now=now))
        sqlite_utc = now.strftime('%Y-%m-%d %H:%M:%S')
        self.assertTrue(candidate_is_fresh(now.isoformat(), sqlite_utc, now=now))

    def test_invalid_source_timestamps_fail_closed(self):
        for value in (None, '', 'not-a-date', '2026-11-21 12:00:00'):
            with self.subTest(value=value):
                self.assertFalse(candidate_is_fresh(value, now=self.now))
        self.assertFalse(candidate_is_fresh(self.now.isoformat(), 'broken', now=self.now))

    def test_historical_366_second_candidate_cannot_be_accepted(self):
        self.add(age=366)
        decision = self.reply()
        self.assertFalse(self.store.accept_decision(decision, now=self.now))
        self.assertEqual(self.store.conn.execute(
            'SELECT status FROM decisions WHERE decision_id=?', (decision,),
        ).fetchone()[0], 'proposed')
        self.assertEqual(self.store.conn.execute('SELECT COUNT(*) FROM outbox').fetchone()[0], 0)

    def test_historical_595_second_candidate_cannot_be_enqueued_even_if_accepted(self):
        self.add(age=595)
        decision = self.reply()
        self.store.conn.execute(
            "UPDATE decisions SET status='accepted' WHERE decision_id=?", (decision,)
        )
        with self.assertRaisesRegex(ValueError, 'expired'):
            self.store.enqueue(decision, 'a', 'g', '这话题值得聊聊。', now=self.now)
        self.assertEqual(self.store.conn.execute('SELECT COUNT(*) FROM outbox').fetchone()[0], 0)

    def test_recent_candidate_can_be_accepted_and_queued_once(self):
        self.add(age=20)
        decision = self.reply()
        self.assertTrue(self.store.accept_decision(decision, now=self.now))
        outbox, created = self.store.enqueue(
            decision, 'a', 'g', '这话题值得聊聊。', now=self.now,
        )
        self.assertTrue(created)
        self.assertIsInstance(outbox, str)
        repeated, created = self.store.enqueue(
            decision, 'a', 'g', '这话题值得聊聊。', now=self.now+timedelta(hours=1),
        )
        self.assertEqual(outbox, repeated)
        self.assertFalse(created)  # Idempotent retry; no new queue item.
        self.assertEqual(self.store.conn.execute('SELECT COUNT(*) FROM outbox').fetchone()[0], 1)

    def test_candidate_expires_between_accept_and_enqueue(self):
        self.add(age=50)
        decision = self.reply()
        self.assertTrue(self.store.accept_decision(decision, now=self.now))
        with self.assertRaisesRegex(ValueError, 'expired'):
            self.store.enqueue(
                decision, 'a', 'g', '这话题值得聊聊。',
                now=self.now+timedelta(seconds=71),
            )
        self.assertEqual(self.store.conn.execute('SELECT COUNT(*) FROM outbox').fetchone()[0], 0)

    def test_reply_returned_after_expiry_is_saved_as_skip(self):
        self.add(age=180)
        result = process_one(
            self.store, CFG,
            decider=lambda *_: {'action':'reply','reason':'能接话','reply':'稍微晚了'},
            activation_cursor='100', message_id='m',
        )
        self.assertEqual((result['action'], result['reason'], result['candidate']),
                         ('skip','reply_candidate_expired',''))
        self.assertEqual(self.store.conn.execute(
            'SELECT action,status FROM decisions'
        ).fetchone()[:], ('skip','rejected'))
        self.assertEqual(self.store.conn.execute('SELECT COUNT(*) FROM outbox').fetchone()[0], 0)

    def test_worker_retires_180_second_message_without_model(self):
        self.add(age=180)
        cfg = copy.deepcopy(CFG)
        cfg['processing']['max_message_chars']=2000
        worker = AgentWorker(
            config=cfg,store=self.store,api=object(),
            activation_cursor='100',utcnow=lambda: self.now,
            logger=lambda _:None,
        )
        with patch('wechat_ai_bot.worker.process_one', side_effect=AssertionError('LLM called')):
            result = worker.process_pending_once()
        self.assertEqual((result['action'],result['reason']),
                         ('skip','stale_message_not_replied'))
        self.assertEqual(self.store.conn.execute('SELECT COUNT(*) FROM outbox').fetchone()[0],0)


if __name__ == '__main__':
    unittest.main()
