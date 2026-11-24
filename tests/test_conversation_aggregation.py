"""P6.5B offline conversation coalescing; no external calls."""

from __future__ import annotations

import copy
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from test_process_pending import CFG as BASE_CFG
from test_storage import V2_DDL
from wechat_ai_bot.migrations import migrate_v2_to_v3, migrate_v3_to_v4
from wechat_ai_bot.process_pending import process_one
from wechat_ai_bot.storage import Storage
from wechat_ai_bot.worker import AgentWorker, WorkerStateError


class ConversationAggregationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Path(self.tmp.name) / 'app.sqlite3'
        conn = sqlite3.connect(self.db)
        try:
            conn.execute('PRAGMA foreign_keys=ON')
            conn.executescript(V2_DDL)
            migrate_v2_to_v3(conn)
            migrate_v3_to_v4(conn)
        finally:
            conn.close()
        self.store = Storage(self.db)
        self.cfg = copy.deepcopy(BASE_CFG)
        self.cfg['processing']['conversation_aggregation'] = {
            'enabled': True, 'quiet_seconds': 8, 'max_wait_seconds': 30,
        }
        self.clock = datetime.now(timezone.utc)
        self.calls = []
        self.logs = []

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def add(self, message_id, cursor, age=10, *, body=None,
            group='g', direction='incoming', when=None):
        self.store.persist_message(
            'a', message_id, group, direction=direction,
            body=body or f'聊一下 {message_id} 的主题',
            sender_id='member',
            created_at=(when or (self.clock-timedelta(seconds=age))).isoformat(),
            source_cursor=str(cursor),
        )

    def worker(self, config=None):
        return AgentWorker(
            config=config or self.cfg, store=self.store, api=object(),
            activation_cursor='100', logger=self.logs.append,
            utcnow=lambda: self.clock,
        )

    def decide_as(self, action='reply', *, deferred=False):
        def caller(store, cfg, *, activation_cursor, message_id, coalesced):
            self.calls.append((message_id, tuple(coalesced)))
            if deferred:
                return {'status': 'deferred', 'error_code': 'model_request_failed'}
            return process_one(
                store, cfg,
                decider=lambda context, _cfg: {
                    'action': action,
                    'reason': '参与群聊' if action == 'reply' else '无需接话',
                    'reply': '可以一起聊这个话题。' if action == 'reply' else '',
                },
                activation_cursor=activation_cursor, message_id=message_id,
                coalesced=coalesced,
            )
        return caller

    def outbox(self):
        return self.store.conn.execute('SELECT COUNT(*) FROM outbox').fetchone()[0]

    def decision_rows(self):
        return self.store.conn.execute(
            'SELECT source_message_id,action,reason,status FROM decisions '
            'ORDER BY source_message_id'
        ).fetchall()

    def test_quiet_window_waits_without_llm_or_writes(self):
        self.add('first','110',age=4)
        self.add('newest','111',age=1)
        worker=self.worker()
        with patch('wechat_ai_bot.worker.process_one', side_effect=AssertionError('model called')):
            self.assertEqual(worker.process_pending_once()['status'], 'waiting_for_conversation')
        self.assertEqual(len(self.decision_rows()),0)
        self.assertEqual(self.store.message('a','first')['status'],'pending')
        self.assertEqual(self.outbox(),0)

    def test_silent_conversation_one_model_decision_for_multiple_messages(self):
        self.add('one','110',age=25)
        self.add('two','111',age=20)
        self.add('three','112',age=10)
        with patch('wechat_ai_bot.worker.process_one', side_effect=self.decide_as('reply')):
            result=self.worker().process_pending_once()
        self.assertEqual(result['status'],'saved')
        self.assertEqual(result['decision_count'],3)
        self.assertEqual(len(self.calls),1)
        self.assertEqual(self.calls[0][0],'three')
        self.assertEqual(len(self.calls[0][1]),2)
        self.assertEqual(len(self.decision_rows()),3)
        self.assertEqual({r['source_message_id']:r['action'] for r in self.decision_rows()},
                         {'one':'skip','two':'skip','three':'reply'})
        self.assertEqual({r['source_message_id']:r['status'] for r in self.decision_rows()},
                         {'one':'rejected','two':'rejected','three':'proposed'})
        self.assertEqual(self.store.conn.execute("SELECT COUNT(*) FROM messages WHERE status='pending'").fetchone()[0],0)
        self.assertEqual(self.outbox(),0)

    def test_max_wait_prevents_continuous_chat_starvation(self):
        self.add('older','110',age=35)
        self.add('newer','111',age=1)
        with patch('wechat_ai_bot.worker.process_one',side_effect=self.decide_as('skip')):
            result=self.worker().process_pending_once()
        self.assertEqual(result['decision_count'],2)
        self.assertEqual(self.calls[0][0],'newer')
        self.assertEqual(self.store.message('a','older')['status'],'processed')
        self.assertEqual(self.outbox(),0)

    def test_backlog_superseded_is_auditable_not_falsely_context_covered(self):
        self.add('old','110',age=90)
        self.add('recent','111',age=12)
        with patch('wechat_ai_bot.worker.process_one',side_effect=self.decide_as('reply')):
            self.worker().process_pending_once()
        reasons={r['source_message_id']:r['reason'] for r in self.decision_rows()}
        self.assertEqual(reasons['old'],'superseded_by_newer_context')
        self.assertEqual(reasons['recent'],'参与群聊')

    def test_activation_and_other_group_are_not_retired(self):
        self.add('old','90',age=12)
        self.add('other','110',age=12,group='elsewhere')
        self.add('outgoing','111',age=12,direction='outgoing')
        self.add('eligible','112',age=12)
        with patch('wechat_ai_bot.worker.process_one',side_effect=self.decide_as('skip')):
            self.worker().process_pending_once()
        for mid in ('old','other','outgoing'):
            self.assertEqual(self.store.message('a',mid)['status'],'pending')
        self.assertEqual(len(self.decision_rows()),1)
        self.assertEqual(self.outbox(),0)

    def test_deferred_model_keeps_entire_group_pending(self):
        self.add('one','110',age=12)
        self.add('two','111',age=12)
        with patch('wechat_ai_bot.worker.process_one',side_effect=self.decide_as(deferred=True)):
            self.assertEqual(self.worker().process_pending_once()['status'],'deferred')
        self.assertEqual(len(self.decision_rows()),0)
        self.assertEqual(self.store.message('a','one')['status'],'pending')
        self.assertEqual(self.store.message('a','two')['status'],'pending')
        self.assertEqual(self.outbox(),0)

    def test_existing_decision_causes_atomic_rollback_for_stale_plan(self):
        self.add('one','110',age=12)
        self.add('two','111',age=12)
        self.store.decide('a','one',action='skip',reason='manual')
        before=len(self.decision_rows())
        with self.assertRaises(RuntimeError):
            self.store.decide_conversation_and_process(
                'a','g','100','two', action='reply',reason='join',
                model='test',reply='reply', decision_status='proposed',
                coalesced=[('one','conversation_window_coalesced')],
            )
        self.assertEqual(len(self.decision_rows()),before)
        self.assertEqual(self.store.message('a','two')['status'],'pending')
        self.assertEqual(self.outbox(),0)

    def test_group_store_rejects_cross_thread_and_pre_activation(self):
        self.add('anchor','120',age=12)
        self.add('foreign','119',age=12,group='other')
        self.add('before','90',age=12)
        for mid in ('foreign','before'):
            with self.subTest(mid=mid),self.assertRaises(RuntimeError):
                self.store.decide_conversation_and_process(
                    'a','g','100','anchor',action='skip',reason='none',
                    decision_status='rejected',
                    coalesced=[(mid,'conversation_window_coalesced')],
                )
        self.assertEqual(len(self.decision_rows()),0)
        self.assertEqual(self.store.message('a','anchor')['status'],'pending')
        self.assertEqual(self.outbox(),0)

    def test_group_is_bounded_to_200_members(self):
        for i in range(205):
            self.add(f'm-{i}',str(110+i),age=12)
        with patch('wechat_ai_bot.worker.process_one',side_effect=self.decide_as('skip')):
            result=self.worker().process_pending_once()
        self.assertEqual(result['decision_count'],200)
        self.assertEqual(len(self.decision_rows()),200)
        self.assertEqual(self.store.conn.execute("SELECT COUNT(*) FROM messages WHERE status='pending'").fetchone()[0],5)
        self.assertEqual(self.outbox(),0)

    def test_late_older_burst_does_not_generate_a_second_reply(self):
        for i in range(205):
            self.add(f'm-{i}', str(110+i), age=12)
        worker = self.worker()
        with patch('wechat_ai_bot.worker.process_one',side_effect=self.decide_as('reply')):
            self.assertEqual(worker.process_pending_once()['decision_count'],200)
            self.assertEqual(worker.retire_superseded_batch(),5)
            self.assertEqual(worker.process_pending_once()['status'],'empty_or_backoff')
        self.assertEqual(len(self.calls),1)
        self.assertEqual(len(self.decision_rows()),205)
        self.assertEqual(self.store.conn.execute(
            "SELECT COUNT(*) FROM decisions WHERE action='reply'"
        ).fetchone()[0],1)
        self.assertEqual(self.outbox(),0)

    def test_deferred_anchor_backoff_is_not_bypassed_by_old_pending(self):
        self.add('old','110',age=20)
        self.add('new','111',age=10)
        worker=self.worker()
        with patch('wechat_ai_bot.worker.process_one',side_effect=self.decide_as(deferred=True)):
            self.assertEqual(worker.process_pending_once()['status'],'deferred')
            self.assertEqual(worker.process_pending_once()['status'],'empty_or_backoff')
        self.assertEqual(len(self.calls),1)
        self.assertEqual(len(self.decision_rows()),0)
        self.assertEqual(self.outbox(),0)

    def test_invalid_config_fails_closed_and_default_remains_legacy(self):
        bad=copy.deepcopy(self.cfg)
        bad['processing']['conversation_aggregation']['quiet_seconds']=0
        with self.assertRaises(WorkerStateError):
            self.worker(bad)
        legacy=copy.deepcopy(self.cfg)
        del legacy['processing']['conversation_aggregation']
        self.assertFalse(self.worker(legacy).conversation_enabled)

    def test_skip_anchor_marks_others_rejected(self):
        self.add('first','110',age=11)
        self.add('second','111',age=10)
        with patch('wechat_ai_bot.worker.process_one',side_effect=self.decide_as('skip')):
            result=self.worker().process_pending_once()
        self.assertEqual(result['decision_count'],2)
        self.assertTrue(all(r['action']=='skip' and r['status']=='rejected'
                            for r in self.decision_rows()))
        self.assertEqual(self.outbox(),0)


if __name__ == '__main__':
    unittest.main()
