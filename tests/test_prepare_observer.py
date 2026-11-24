from contextlib import closing
import importlib.util
import json
import os
from pathlib import Path
import sqlite3
import stat
import tempfile
import unittest
from unittest.mock import patch

from test_storage import V2_DDL
from wechat_ai_bot.consumer import EXPECTED_AGENT_WECHAT_BASE_URL, validate_config
from wechat_ai_bot.migrations import migrate_v2_to_v3, migrate_v3_to_v4, migrate_v4_to_v5


_SCRIPT = Path(__file__).resolve().parents[1] / "deploy/isolated/prepare-observer.py"
_SPEC = importlib.util.spec_from_file_location("observer_preparation", _SCRIPT)
preparation = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(preparation)


class ObserverPreparationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.database = self.root / "old.sqlite3"
        self.data = self.root / "data"
        self.private = self.data / "observer"
        self.store = self.data / "store"
        self.private.mkdir(parents=True, mode=0o700)
        self.store.mkdir()
        self.store.chmod(0o2770)
        self.config_path = self.root / "observer.json"
        self.config = {
            "schema_version": 2,
            "app": {"auto_send": False},
            "sender": {"enabled": False, "token_file": "/legacy/sender-token"},
            "runtime": {"global_pause": True},
            "agent_wechat": {"account_id": "fixture", "consumer_name": "consumer", "target_group_id": "group",
                             "base_url": EXPECTED_AGENT_WECHAT_BASE_URL},
            "safety": {"group_allowlist": ["group"]},
            "llm": {"provider": "deepseek", "deepseek": {}},
        }
        self.state = {"state_version": 1, "phase": "active", "account_id": "fixture", "thread_id": "group",
                      "activation_cursor": "10", "snapshot_cursor": "9", "history_message_count": 0,
                      "source_generation": "fixture-generation"}
        self.artifacts = {name: self.root / name for name in ("worker-state.json", "native-read-token",
                                                              "deepseek-api-key", "deepseek-usage.jsonl")}
        for name, path in self.artifacts.items():
            path.write_text(json.dumps(self.state) if name == "worker-state.json" else "fixture\n")
            path.chmod(0o600)
        with closing(sqlite3.connect(self.database)) as original:
            original.executescript(V2_DDL)
            migrate_v2_to_v3(original)
            migrate_v3_to_v4(original)
            migrate_v4_to_v5(original)
            original.execute("INSERT INTO consumer_state (consumer_name, account_id, last_durable_cursor, "
                             "last_acked_cursor) VALUES ('consumer', 'fixture', '10', '10')")
            original.commit()

    def prepare(self):
        uid, gid = os.geteuid(), os.getegid()
        self.shared_gid = gid or 12345
        real_chown = os.fchown

        def chown(fd, owner, group):
            if owner != 0 or uid == 0:
                real_chown(fd, owner, group)

        if gid == 0:
            os.chown(self.store, uid, self.shared_gid)
            self.store.chmod(0o2770)
        with patch.multiple(preparation, DATA=self.data, CONFIG=self.config_path), patch.object(os, "fchown", chown):
            preparation.prepare(self.config, self.database, self.artifacts, uid, gid, self.shared_gid)

    def test_wal_snapshot_preserves_checkpoint_activation_and_usage(self):
        with closing(sqlite3.connect(self.database)) as original:
            original.execute("PRAGMA journal_mode=WAL")
            original.execute("PRAGMA wal_autocheckpoint=0")
            original.execute("UPDATE consumer_state SET last_durable_cursor='15', last_acked_cursor='12'")
            original.commit()
            self.assertGreater(self.database.with_suffix(".sqlite3-wal").stat().st_size, 0)
            self.prepare()
            self.assertEqual(original.execute("PRAGMA journal_mode").fetchone(), ("wal",))

        copied = self.store / "bot.sqlite3"
        self.assertEqual(stat.S_IMODE(copied.stat().st_mode), 0o660)
        self.assertEqual(stat.S_IMODE(self.config_path.stat().st_mode), 0o640)
        with closing(sqlite3.connect(copied)) as snapshot:
            self.assertEqual(preparation.database_checkpoint(snapshot, self.config), ("15", "12"))
        for name, source in self.artifacts.items():
            target = self.private / name
            self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o600)
            self.assertEqual(source.read_bytes(), target.read_bytes())
        config = json.loads(self.config_path.read_text())
        self.assertNotIn("token_file", config["sender"])
        with patch.object(os, "getgroups", return_value=[self.shared_gid]):
            validate_config(config, base_dir=self.config_path.parent)

    def test_invalid_activation_leaves_no_targets(self):
        self.state["activation_cursor"] = "11"
        self.artifacts["worker-state.json"].write_text(json.dumps(self.state))
        with self.assertRaisesRegex(preparation.PreparationError, "ahead of durable Cursor"):
            self.prepare()
        self.assertEqual(list(self.store.iterdir()), [])
        self.assertEqual(list(self.private.iterdir()), [])
        self.assertFalse(self.config_path.exists())

    def test_copy_failure_removes_only_new_files(self):
        with patch.object(preparation, "copy_file", side_effect=OSError("fixture disk failure")):
            with self.assertRaisesRegex(OSError, "fixture disk failure"):
                self.prepare()
        self.assertEqual(list(self.store.iterdir()), [])
        self.assertEqual(list(self.private.iterdir()), [])
        self.assertFalse(self.config_path.exists())
        self.assertTrue(self.database.is_file())

    def test_existing_staging_is_preserved(self):
        existing = self.private / "deepseek-usage.jsonl"
        existing.write_text("existing ledger\n")
        with self.assertRaisesRegex(preparation.PreparationError, "Staging is not empty"):
            self.prepare()
        self.assertEqual(existing.read_text(), "existing ledger\n")
        self.assertEqual(list(self.store.iterdir()), [])
        self.assertFalse(self.config_path.exists())
