"""P6.2 scheme B: offline-only tests, no real Agent WeChat / Ollama."""

from __future__ import annotations

import copy
import json
import sqlite3
import tempfile
import threading
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from test_process_pending import CFG as BASE_CFG
from test_storage import V2_DDL
from wechat_ai_bot.migrations import migrate_v2_to_v3, migrate_v3_to_v4
from wechat_ai_bot.process_pending import process_one
from wechat_ai_bot.storage import Storage
from wechat_ai_bot.worker import (
    AgentWorker,
    WorkerStateError,
    _atomic_write_json,
    _read_state,
    ensure_activation_state,
    worker_single_instance_lock,
)


class FakeAPI:
    def __init__(self, page=None, details=None):
        self.page = page
        self.details = details or {}
        self.page_calls = 0
        self.detail_calls = []
        self.after_detail = None

    def thread_messages_page(self, thread_id, *, limit):
        self.page_calls += 1
        if isinstance(self.page, Exception):
            raise self.page
        return self.page

    def message_detail(self, mid):
        self.detail_calls.append(mid)
        result = self.details[mid]
        if self.after_detail is not None:
            self.after_detail(mid)
        if isinstance(result, BaseException):
            raise result
        return result


def sample_detail(mid, *, body='hello', created=None):
    return {
        'id': mid, 'accountId': 'a', 'threadId': 'g',
        'direction': 'incoming', 'body': body, 'type': 1,
        'senderId': 'member',
        'createdAt': created or datetime.now(timezone.utc).isoformat(),
        'mentions': {'state': 'known', 'self': False},
    }


def page_for(ids, cursor='100', partial=False):
    return {
        'items': [{'id': x, 'threadId': 'g'} for x in ids],
        'coverage': {'partial': partial, 'databases': []},
        'sourceGeneration': 'fixture-generation',
        'asOfEventCursor': cursor,
    }


class WorkerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.db = self.root / 'app.sqlite3'
        conn = sqlite3.connect(self.db)
        try:
            conn.execute('PRAGMA foreign_keys=ON')
            conn.executescript(V2_DDL)
            migrate_v2_to_v3(conn)
            migrate_v3_to_v4(conn)
            conn.execute(
                "ALTER TABLE events ADD COLUMN status TEXT NOT NULL DEFAULT 'received'"
            )
        finally:
            conn.close()
        self.store = Storage(self.db, consumer_name='test-consumer')
        self.cfg = copy.deepcopy(BASE_CFG)
        self.cfg['agent_wechat']['consumer_name'] = 'test-consumer'
        self.cfg['processing']['max_message_chars'] = 1000
        self.state_path = self.root / 'worker-state.json'
        self.logs = []
        self.set_checkpoint('100')

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def set_checkpoint(self, cursor):
        self.store.persist_event('a', cursor, 'heartbeat', 'g')
        self.store.mark_acked('a', cursor)

    def insert_event(self, cursor, mid):
        self.store.persist_event('a', str(cursor), 'message.committed', 'g', mid)

    def insert_message(self, mid, cursor, created=None, *, body=None):
        self.store.persist_message(
            'a', mid, 'g', direction='incoming',
            body=body or f'message {mid}', sender_id='member',
            created_at=(created or datetime.now(timezone.utc)).isoformat(),
            source_cursor=str(cursor),
        )

    def worker(self, api=None, cursor='100'):
        return AgentWorker(
            config=self.cfg, store=self.store,
            api=api or FakeAPI(), activation_cursor=cursor,
            logger=self.logs.append,
        )

    def outbox_count(self):
        return self.store.conn.execute('SELECT COUNT(*) FROM outbox').fetchone()[0]

    def test_scheme_b_seeds_20_ignored_context_rows(self):
        ids = [f'h{x}' for x in range(20, 0, -1)]
        api = FakeAPI(page_for(ids, '100'), {x: sample_detail(x) for x in ids})
        state = ensure_activation_state(
            self.store, api, self.cfg, self.state_path, logger=self.logs.append
        )
        self.assertEqual(state['phase'], 'active')
        self.assertEqual(state['history_message_count'], 20)
        self.assertEqual(state['activation_cursor'], '100')
        self.assertEqual(self.store.conn.execute(
            "SELECT COUNT(*) FROM messages WHERE status='ignored'"
        ).fetchone()[0], 20)
        self.assertEqual(self.store.conn.execute(
            "SELECT COUNT(*) FROM messages WHERE status='pending'"
        ).fetchone()[0], 0)
        self.assertEqual(self.outbox_count(), 0)
        self.assertEqual(self.state_path.stat().st_mode & 0o777, 0o600)
        second = ensure_activation_state(
            self.store, api, self.cfg, self.state_path, logger=self.logs.append
        )
        self.assertEqual(second, state)
        self.assertEqual(api.page_calls, 1)
        self.assertEqual(len(api.detail_calls), 20)

    def test_partial_history_fails_closed_before_state_is_created(self):
        api = FakeAPI(page_for(['h1'], partial=True), {'h1': sample_detail('h1')})
        with self.assertRaises(WorkerStateError):
            ensure_activation_state(self.store, api, self.cfg, self.state_path)
        self.assertFalse(self.state_path.exists())
        self.assertEqual(self.outbox_count(), 0)

    def test_duplicate_history_ids_fails_closed(self):
        api = FakeAPI(page_for(['h1', 'h1']))
        with self.assertRaises(WorkerStateError):
            ensure_activation_state(self.store, api, self.cfg, self.state_path)
        self.assertFalse(self.state_path.exists())

    def test_history_wrong_thread_stays_bootstrapping(self):
        bad = sample_detail('h1')
        bad['threadId'] = 'different'
        api = FakeAPI(page_for(['h1']), {'h1': bad})
        with self.assertRaises(ValueError):
            ensure_activation_state(self.store, api, self.cfg, self.state_path)
        self.assertEqual(_read_state(self.state_path, 'a', 'g')['phase'], 'bootstrapping')
        self.assertEqual(self.store.conn.execute('SELECT COUNT(*) FROM messages').fetchone()[0], 0)

    def test_activation_cursor_uses_later_consumer_checkpoint(self):
        api = FakeAPI(page_for(['h1'], '90'), {'h1': sample_detail('h1')})
        api.after_detail = lambda _mid: self.set_checkpoint('111')
        state = ensure_activation_state(self.store, api, self.cfg, self.state_path)
        self.assertEqual(state['activation_cursor'], '111')

    def test_failed_bootstrap_resumes_same_snapshot(self):
        api = FakeAPI(page_for(['h1'], '100'), {'h1': TimeoutError('offline')})
        with self.assertRaises(TimeoutError):
            ensure_activation_state(self.store, api, self.cfg, self.state_path)
        self.set_checkpoint('112')
        api2 = FakeAPI(page_for(['different'], '200'), {'h1': sample_detail('h1')})
        state = ensure_activation_state(self.store, api2, self.cfg, self.state_path)
        self.assertEqual(api2.page_calls, 0)
        self.assertEqual(state['activation_cursor'], '112')
        self.assertEqual(state['history_message_count'], 1)

    def test_history_does_not_touch_old_unrelated_pending(self):
        self.insert_message('old', '80')
        api = FakeAPI(page_for(['h1'], '100'), {'h1': sample_detail('h1')})
        ensure_activation_state(self.store, api, self.cfg, self.state_path)
        self.assertEqual(self.store.message('a','old')['status'], 'pending')
        self.assertEqual(self.store.message('a','h1')['status'], 'ignored')

    def test_invalid_state_file_permissions_rejected(self):
        _atomic_write_json(self.state_path, {
            'state_version':1, 'phase':'active', 'account_id':'a',
            'thread_id':'g', 'activation_cursor':'100',
            'snapshot_cursor':'100', 'source_generation':'g1',
            'history_message_count':0
        })
        self.state_path.chmod(0o644)
        with self.assertRaises(WorkerStateError):
            _read_state(self.state_path, 'a', 'g')

    def test_activation_rejects_other_group(self):
        _atomic_write_json(self.state_path, {
            'state_version':1, 'phase':'active', 'account_id':'a',
            'thread_id':'wrong', 'activation_cursor':'100',
            'snapshot_cursor':'100', 'source_generation':'g1',
            'history_message_count':0
        })
        with self.assertRaises(WorkerStateError):
            _read_state(self.state_path, 'a', 'g')

    def test_hydrate_postactivation_only(self):
        # An old received event remains untouched. A new event is hydrated.
        # We insert the old event directly to preserve Consumer cursor ordering.
        self.store.conn.execute(
            "INSERT INTO events (account_id,cursor,kind,thread_id,message_id) "
            "VALUES ('a','90','message.committed','g','old')"
        )
        self.insert_event('110', 'new')
        api = FakeAPI(details={'new': sample_detail('new')})
        worker = self.worker(api)
        self.assertEqual(worker.hydrate_batch(), 1)
        self.assertEqual(api.detail_calls, ['new'])
        self.assertIsNone(self.store.message('a','old'))
        self.assertEqual(self.store.message('a','new')['status'], 'pending')
        self.assertEqual(self.store.conn.execute(
            "SELECT status FROM events WHERE cursor='90'"
        ).fetchone()[0], 'received')
        self.assertEqual(self.outbox_count(), 0)

    def test_one_hydration_failure_does_not_block_next(self):
        self.insert_event('110','bad')
        self.insert_event('111','good')
        api = FakeAPI(details={
            'bad': TimeoutError('timeout'), 'good': sample_detail('good')
        })
        worker = self.worker(api)
        self.assertEqual(worker.hydrate_batch(), 1)
        self.assertEqual(api.detail_calls,['bad','good'])
        self.assertEqual(self.store.message('a','good')['status'], 'pending')
        self.assertEqual(self.store.conn.execute(
            "SELECT status FROM events WHERE cursor='110'"
        ).fetchone()[0], 'received')

    def test_hydration_rotating_scan_avoids_head_page_starvation(self):
        # A full failing page should not starve the next event.
        for n in range(101, 122):
            self.insert_event(str(n), f'm{n}')
        api = FakeAPI(details={
            **{f'm{n}': TimeoutError('timeout') for n in range(101,121)},
            'm121': sample_detail('m121'),
        })
        worker = self.worker(api)
        self.assertEqual(worker.hydrate_batch(),0)
        self.assertEqual(worker.hydrate_batch(),1)
        self.assertIsNotNone(self.store.message('a','m121'))

    def test_skip_stale_message_without_model_and_without_outbox(self):
        self.insert_message('stale','110', datetime.now(timezone.utc) - timedelta(hours=1))
        worker=self.worker()
        with patch('wechat_ai_bot.worker.process_one', side_effect=AssertionError('model called')):
            result = worker.process_pending_once()
        self.assertEqual(result['action'], 'skip')
        self.assertEqual(self.store.message('a','stale')['status'],'processed')
        self.assertEqual(self.store.conn.execute(
            "SELECT status FROM decisions WHERE source_message_id='stale'"
        ).fetchone()[0], 'rejected')
        self.assertEqual(self.outbox_count(),0)

    def test_batch_expiry_clears_backlog_bounded_and_without_model(self):
        old = datetime.now(timezone.utc) - timedelta(hours=2)
        for number in range(205):
            self.insert_message(f'old-{number}', str(101 + number), old)
        worker = self.worker()
        with patch('wechat_ai_bot.worker.process_one', side_effect=AssertionError('model called')):
            self.assertEqual(worker.expire_stale_batch(), 100)
            self.assertEqual(worker.expire_stale_batch(), 100)
            self.assertEqual(worker.expire_stale_batch(), 5)
            self.assertEqual(worker.expire_stale_batch(), 0)
        self.assertEqual(self.store.conn.execute(
            "SELECT COUNT(*) FROM messages WHERE status='pending'"
        ).fetchone()[0], 0)
        self.assertEqual(self.store.conn.execute(
            "SELECT COUNT(*) FROM decisions WHERE action='skip' "
            "AND status='rejected' AND reason='stale_message_not_replied'"
        ).fetchone()[0], 205)
        self.assertEqual(self.outbox_count(), 0)

    def test_batch_expiry_preserves_boundary_and_fresh_messages(self):
        old = datetime.now(timezone.utc) - timedelta(hours=2)
        self.insert_message('pre-activation', '90', old)
        self.insert_message('expired', '110', old)
        self.insert_message('fresh', '120')
        self.store.persist_message(
            'a', 'outgoing-old', 'g', direction='outgoing', body='hello',
            created_at=old.isoformat(), source_cursor='130',
        )
        worker = self.worker()
        self.assertEqual(worker.expire_stale_batch(), 1)
        self.assertEqual(self.store.message('a', 'pre-activation')['status'], 'pending')
        self.assertEqual(self.store.message('a', 'fresh')['status'], 'pending')
        self.assertEqual(self.store.message('a', 'outgoing-old')['status'], 'pending')
        self.assertEqual(self.store.message('a', 'expired')['status'], 'processed')
        self.assertEqual(worker.expire_stale_batch(), 0)
        self.assertEqual(self.outbox_count(), 0)

    def test_batch_expiry_leaves_existing_decisions_for_reconciliation(self):
        old = datetime.now(timezone.utc) - timedelta(hours=2)
        self.insert_message('already', '110', old)
        self.store.decide('a', 'already', action='skip', reason='already decided')
        worker = self.worker()
        self.assertEqual(worker.expire_stale_batch(), 0)
        self.assertEqual(self.store.message('a', 'already')['status'], 'pending')
        self.assertEqual(worker.process_pending_once()['status'], 'already_decided')
        self.assertEqual(self.store.message('a', 'already')['status'], 'processed')
        self.assertEqual(self.outbox_count(), 0)

    def test_run_once_retains_fresh_processor_slot_after_batch_expiry(self):
        old = datetime.now(timezone.utc) - timedelta(hours=2)
        for number in range(100):
            self.insert_message(f'old-{number}', str(101 + number), old)
        self.insert_message('fresh', '210')
        worker = self.worker()
        with patch('wechat_ai_bot.worker.process_one', return_value={
            'status': 'saved', 'action': 'skip',
        }) as model_path:
            result = worker.run_once()
        self.assertEqual(result['expired'], 100)
        self.assertEqual(result['processor'], 'saved')
        model_path.assert_called_once()
        self.assertEqual(model_path.call_args.kwargs['message_id'], 'fresh')
        self.assertEqual(self.outbox_count(), 0)

    def test_missing_timestamp_never_invokes_model(self):
        self.store.persist_message('a','unknown','g',direction='incoming',
                                   body='hello',source_cursor='110')
        worker=self.worker()
        with patch('wechat_ai_bot.worker.process_one', side_effect=AssertionError('model called')):
            result=worker.process_pending_once()
        self.assertEqual(result['reason'], 'missing_message_timestamp')

    def test_recent_message_can_be_proposed_but_not_sent(self):
        self.insert_message('recent','110')
        worker=self.worker()
        def local_decider(store,cfg,*,activation_cursor,message_id):
            return process_one(store,cfg,decider=lambda *_:{
                'action':'reply','reason':'question','reply':'reply candidate',
            },activation_cursor=activation_cursor,message_id=message_id)
        with patch('wechat_ai_bot.worker.process_one', side_effect=local_decider):
            result=worker.process_pending_once()
        self.assertEqual(result['action'],'reply')
        self.assertEqual(self.store.conn.execute(
            "SELECT status FROM decisions WHERE source_message_id='recent'"
        ).fetchone()[0], 'proposed')
        self.assertEqual(self.outbox_count(),0)

    def test_one_model_failure_does_not_block_next_message(self):
        self.insert_message('first','110')
        self.insert_message('second','111')
        worker=self.worker()
        def decision(store,cfg,*,activation_cursor,message_id):
            if message_id == 'first':
                return {'status':'deferred','error_code':'model_request_failed'}
            return process_one(store,cfg,decider=lambda *_:{
                'action':'skip','reason':'low_signal','reply':''
            },activation_cursor=activation_cursor,message_id=message_id)
        with patch('wechat_ai_bot.worker.process_one', side_effect=decision):
            first=worker.process_pending_once()
            second=worker.process_pending_once()
        self.assertEqual(first['status'],'deferred')
        self.assertEqual(second['action'],'skip')
        self.assertEqual(self.store.message('a','first')['status'],'pending')
        self.assertEqual(self.store.message('a','second')['status'],'processed')

    def test_provider_throttle_pauses_model_not_hydration_or_expiry(self):
        self.insert_message('first', '110')
        self.insert_message('second', '111')
        self.insert_event('112', 'late')
        worker = self.worker(FakeAPI(details={'late': sample_detail('late')}))
        with patch('wechat_ai_bot.worker.process_one', return_value={
            'status': 'deferred', 'error_code': 'model_rate_limited',
            'retry_after_seconds': 20,
        }) as model_path:
            first = worker.process_pending_once()
            self.assertEqual(first['status'], 'deferred')
            self.assertEqual(worker.process_pending_once()['status'], 'model_throttled')
            self.assertEqual(model_path.call_count, 1)
            result = worker.run_once()
            self.assertEqual(result['hydrated'], 1)
            self.assertEqual(result['processor'], 'model_throttled')
        self.assertEqual(self.store.message('a', 'first')['status'], 'pending')
        self.assertEqual(self.store.message('a', 'late')['status'], 'pending')
        self.assertEqual(self.outbox_count(), 0)
        self.assertTrue(any('模型调度暂停' in x for x in self.logs))

    def test_worker_safety_flags_reject_misconfig(self):
        bad=copy.deepcopy(self.cfg)
        bad['app']['auto_send']=True
        with self.assertRaises(WorkerStateError):
            AgentWorker(config=bad,store=self.store,api=FakeAPI(),activation_cursor='100')

    def test_single_instance_lock_is_exclusive(self):
        lock=self.root/'worker.lock'
        with worker_single_instance_lock(lock):
            with self.assertRaises(WorkerStateError):
                with worker_single_instance_lock(lock):
                    pass
        with worker_single_instance_lock(lock):
            pass

    def test_worker_stops_on_signal_event(self):
        event=threading.Event()
        event.set()
        worker=AgentWorker(config=self.cfg,store=self.store,
                           api=FakeAPI(),activation_cursor='100',
                           stop_event=event,logger=self.logs.append)
        worker.run_forever()
        self.assertTrue(any('优雅退出' in x for x in self.logs))

    def test_explicit_activation_boundary_excludes_older_pending(self):
        self.insert_message('old','90')
        self.insert_message('new','110')
        worker=self.worker()
        row=worker._select_due_pending()
        self.assertEqual(row['message_id'],'new')
        # One-shot P6.1 processor without explicit boundary would select old.
        self.assertEqual(self.store.message('a','old')['status'],'pending')


if __name__ == '__main__':
    unittest.main()
