from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from http.client import HTTPConnection
from io import BytesIO
import json
from pathlib import Path
import sqlite3
import tempfile
from threading import Thread
import unittest
from unittest.mock import Mock, patch

from wechat_ai_bot.admin import AdminServer, AdminState, Dashboard, native_groups, native_status, pipeline_state
from wechat_ai_bot.admin.desktop import DesktopActions
from wechat_ai_bot.admin.history import _confirmed_trial
from wechat_ai_bot.scheduled_plans import schedule_preview


class DashboardTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.state = AdminState(self.root / "admin")
        self.dashboard = Mock()
        self.dashboard.status.return_value = {"native": {"state": "reachable"}}
        self.desktop = Mock()
        self.server = AdminServer(0, self.state, self.dashboard, self.desktop)
        self.addCleanup(self.server.server_close)
        thread = Thread(target=self.server.serve_forever, kwargs={"poll_interval": .01})
        thread.start()
        self.addCleanup(thread.join, 5)
        self.addCleanup(self.server.shutdown)
        self.origin = f"http://127.0.0.1:{self.server.server_port}"

    def request(self, method, path, *, authenticated=True, headers=None, body=None):
        request_headers = {"Content-Type": "application/json", "Origin": self.origin}
        if authenticated:
            request_headers["Authorization"] = "Bearer " + self.state.key
        request_headers.update(headers or {})
        connection = HTTPConnection("127.0.0.1", self.server.server_port, timeout=5)
        try:
            connection.request(method, path, body=body, headers=request_headers)
            response = connection.getresponse()
            payload = response.read()
            if response.getheader("Content-Type").startswith("application/json"):
                payload = json.loads(payload)
            return response.status, payload
        finally:
            connection.close()

    def draft(self):
        return {"provider": "ollama", "model": "fixture:latest", "replyWindowEnabled": True,
                "replyStart": "20:00", "replyEnd": "02:00", "checkinEnabled": True, "checkinTime": "09:30"}

    def test_page_is_public_but_status_requires_the_local_key(self):
        status, page = self.request("GET", "/", authenticated=False)
        self.assertEqual(status, 200)
        self.assertIn(b'id="unlock"', page)
        self.assertEqual(self.request("GET", "/api/status", authenticated=False)[0], 401)
        self.dashboard.status.assert_not_called()
        self.assertEqual(self.request("GET", "/api/status"), (200, self.dashboard.status.return_value))
        self.assertEqual(self.request("GET", "/api/status", headers={"Host": "foreign.example"})[0], 400)

    def test_draft_persists_and_only_touches_admin_state(self):
        config = self.root / "production.json"
        config.write_bytes(b'original configuration')
        desired = self.draft()
        self.assertEqual(self.request("GET", "/api/draft"), (200, None))
        status, saved = self.request("PUT", "/api/draft", body=json.dumps(desired))
        self.assertEqual(status, 200)
        self.assertEqual(saved["settings"], desired)
        self.assertEqual(self.request("GET", "/api/draft"), (200, saved))
        self.assertEqual(AdminState(self.state.path).read_draft(), saved)
        self.assertEqual(self.state.draft_path.stat().st_mode & 0o777, 0o600)
        self.assertEqual(config.read_bytes(), b'original configuration')
        self.dashboard.status.assert_not_called()

    def test_concurrent_saves_with_same_revision_preserve_first_write(self):
        initial = self.state.save_draft(self.draft(), if_match='""')
        headers = {"If-Match": f'"{initial["revision"]}"'}

        def save(model):
            return self.request("PUT", "/api/draft", headers=headers,
                                body=json.dumps({**self.draft(), "model": model}))

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(save, ("first:latest", "second:latest")))

        self.assertEqual(sorted(status for status, _ in results), [200, 412])
        winner = next(payload for status, payload in results if status == 200)
        self.assertEqual(self.request("GET", "/api/draft"), (200, winner))

    def test_unreadable_draft_is_preserved(self):
        self.state.draft_path.touch(mode=0o600)
        original = b'{"settings":'
        self.state.draft_path.write_bytes(original)
        self.assertEqual(self.request("GET", "/api/draft")[0], 503)
        self.assertEqual(self.request("PUT", "/api/draft", headers={"If-Match": '""'},
                                      body=json.dumps(self.draft()))[0], 503)
        self.assertEqual(self.state.draft_path.read_bytes(), original)

    def test_group_list_requires_authentication(self):
        result = {"state": "ready", "configured": None,
                  "groups": [{"id": "fixture@chatroom", "name": "Fixture group"}]}
        self.dashboard.groups.return_value = result
        self.assertEqual(self.request("GET", "/api/groups", authenticated=False)[0], 401)
        self.dashboard.groups.assert_not_called()
        self.assertEqual(self.request("GET", "/api/groups"), (200, result))

    def test_one_global_target_is_saved_and_can_be_cleared(self):
        desired = {**self.draft(), "scheduledMessages": [
            {"enabled": True, "cadence": "daily", "date": "", "time": "10:00", "text": "First"},
            {"enabled": True, "cadence": "daily", "date": "", "time": "11:00", "text": "Second"},
        ]}
        selected = {**desired, "targetGroupId": "fixture@chatroom"}
        status, saved = self.request("PUT", "/api/draft", body=json.dumps(selected))
        self.assertEqual(status, 200)
        self.assertEqual(saved["settings"], selected)
        self.assertEqual(self.request("GET", "/api/draft"), (200, saved))
        for invalid in ("", "filehelper", "invalid/path@chatroom", None):
            self.assertEqual(self.request("PUT", "/api/draft",
                                          body=json.dumps({**desired, "targetGroupId": invalid}))[0], 400)
        self.assertEqual(self.request("GET", "/api/draft"), (200, saved))
        status, cleared = self.request("PUT", "/api/draft", body=json.dumps(desired))
        self.assertEqual(status, 200)
        self.assertEqual(cleared["settings"], desired)

    def test_history_reads_confirmed_sends_only_in_the_configured_group(self):
        database = self.root / "history.sqlite3"
        config = self.root / "runtime.json"
        self.dashboard.runtime_path = config
        config.write_text(json.dumps({"agent_wechat": {"account_id": "bot", "target_group_id": "group"},
                                      "safety": {"group_allowlist": ["group"]},
                                      "storage": {"database": database.name}}))
        connection = sqlite3.connect(database)
        try:
            connection.execute("CREATE TABLE outbox(outbox_id TEXT, account_id TEXT, thread_id TEXT, status TEXT, "
                               "content TEXT, sent_at TEXT, scheduled_task_id TEXT, decision_id TEXT)")
            connection.executemany("INSERT INTO outbox VALUES (?,?,?,?,?,?,?,?)", [
                ("0", "bot", "group", "sent", "older", "2026-11-23 03:20:00", None, "decision"),
                ("1", "bot", "group", "sent", "latest", "2026-11-23T11:21:00+08:00", "task", None),
                ("2", "bot", "group", "queued", "pending", None, None, "decision"),
                ("3", "bot", "group", "needs_review", "unconfirmed", None, None, "decision"),
                ("4", "bot", "elsewhere", "sent", "other group", None, None, None),
            ])
            connection.commit()
        finally:
            connection.close()
        before = database.read_bytes()
        self.assertEqual(self.request("GET", "/api/history", authenticated=False)[0], 401)
        code, history = self.request("GET", "/api/history")
        self.assertEqual(code, 200)
        self.assertEqual(history["state"], "live")
        self.assertEqual(history["entries"], [
            {"text": "latest", "sentAt": "2026-11-23T03:21:00+00:00", "source": "scheduled"},
            {"text": "older", "sentAt": "2026-11-23T03:20:00+00:00", "source": "reply"},
        ])
        self.assertEqual(database.read_bytes(), before)

    def test_history_distinguishes_an_empty_snapshot_from_an_unavailable_database(self):
        self.dashboard.runtime_path = self.root / "missing.json"
        self.assertEqual(self.request("GET", "/api/history")[1]["state"], "unavailable")
        snapshot = self.state.path / "sent-history.json"
        snapshot.touch(mode=0o600)
        snapshot.write_text(json.dumps({"checkedAt": "2026-11-23T03:20:00+00:00", "entries": [], "hasMore": False}))
        self.assertEqual(self.request("GET", "/api/history")[1]["state"], "snapshot")
        self.assertEqual(self.request("POST", "/api/history/export", authenticated=False)[0], 401)
        self.assertEqual(self.request("POST", "/api/history/export", headers={"Origin": "https://example.org"})[0], 403)
        self.desktop.open_history_export.assert_not_called()
        self.assertEqual(self.request("POST", "/api/history/export"), (200, {"status": "opened"}))
        self.desktop.open_history_export.assert_called_once_with(self.state.path, self.dashboard.runtime_path)

    def test_one_shot_history_requires_confirmed_matching_receipt(self):
        record = self.root / "approved-ok.json"
        record.touch(mode=0o600)
        expected = {"account": "fixture", "group": "fixture@chatroom", "text": "OK", "state": "sent",
                    "sentAt": "2026-11-23T03:20:00+00:00", "remoteSendId": "private-id", "key": "private-key"}
        record.write_text(json.dumps(expected))
        self.assertEqual(_confirmed_trial(record, "fixture", "fixture@chatroom"),
                         {"text": "OK", "sentAt": "2026-11-23T03:20:00+00:00", "source": "test"})
        self.assertIsNone(_confirmed_trial(record, "fixture", "wrong@chatroom"))
        record.write_text(json.dumps({**expected, "state": "needs_review"}))
        self.assertIsNone(_confirmed_trial(record, "fixture", "fixture@chatroom"))

    def test_limits_and_scheduled_message_are_saved_with_a_preview(self):
        limits = {"modelDaily": 60, "modelPerMinute": 2, "modelInterval": 30,
                  "replyHourly": 10, "replyDaily": 30, "replyInterval": 120}
        plan = {"enabled": True, "cadence": "daily", "date": "", "time": "09:30", "text": "测试定时消息"}
        desired = {**self.draft(), "limits": limits, "specialMessage": plan}
        status, saved = self.request("PUT", "/api/draft", body=json.dumps(desired))
        self.assertEqual(status, 200)
        self.assertEqual(saved["settings"], desired)
        self.assertEqual(saved["schedulePreview"]["state"], "planned")
        self.assertTrue(saved["schedulePreview"]["nextAt"].endswith("T09:30+08:00"))
        self.assertEqual(self.request("GET", "/api/draft")[1]["settings"], desired)
        for bad in ({**desired, "limits": {**limits, "modelDaily": True}},
                    {**desired, "limits": {**limits, "replyHourly": 31}},
                    {**desired, "specialMessage": {**plan, "cadence": "once", "date": "2026-02-29"}},
                    {**desired, "specialMessage": {**plan, "text": " "}}):
            self.assertEqual(self.request("PUT", "/api/draft", body=json.dumps(bad))[0], 400)
        self.assertEqual(self.state.read_draft()["settings"], desired)

    def test_schedule_rollover_and_past_once_use_shanghai_time(self):
        now = datetime(2026, 11, 23, 1, 45, tzinfo=timezone.utc)
        plan = {"enabled": True, "cadence": "daily", "date": "", "time": "09:30", "text": "fixture"}
        self.assertEqual(schedule_preview(plan, now)["nextAt"], "2026-11-24T09:30+08:00")
        self.assertEqual(schedule_preview({**plan, "cadence": "once", "date": "2026-11-23"}, now)["state"], "expired")
        self.assertEqual(schedule_preview({**plan, "enabled": False}, now)["state"], "paused")

    def test_multiple_plans_preserve_order_and_preview_and_replace_the_single_plan(self):
        first = {"enabled": True, "cadence": "daily", "date": "", "time": "09:30", "text": "第一条"}
        second = {"enabled": False, "cadence": "once", "date": "2027-03-01", "time": "18:15", "text": "第二条"}
        old = {**self.draft(), "specialMessage": first}
        self.assertEqual(self.request("PUT", "/api/draft", body=json.dumps(old))[0], 200)
        self.assertEqual(self.request("GET", "/api/draft")[1]["settings"], old)
        desired = {**self.draft(), "scheduledMessages": [first, second]}
        status, saved = self.request("PUT", "/api/draft", body=json.dumps(desired))
        self.assertEqual(status, 200)
        self.assertEqual(AdminState(self.state.path).read_draft()["settings"], desired)
        self.assertEqual([p["state"] for p in saved["schedulePreviews"]], ["planned", "paused"])
        for invalid in ({**old, "scheduledMessages": []}, {**desired, "scheduledMessages": [first] * 21},
                        {**desired, "scheduledMessages": [first, None]},
                        {**desired, "scheduledMessages": [first, {**second, "time": "25:00"}]}):
            self.assertEqual(self.request("PUT", "/api/draft", body=json.dumps(invalid))[0], 400)
        self.assertEqual(self.request("GET", "/api/draft")[1]["settings"], desired)
        self.assertEqual(self.request("PUT", "/api/draft", body=json.dumps({**desired, "scheduledMessages": []}))[0], 200)
        self.assertEqual(self.request("GET", "/api/draft")[1]["schedulePreviews"], [])

    def test_pipeline_distinguishes_online_services_and_wechat_disconnect(self):
        native = {"state": "reachable", "connection": "online", "authenticated": True}
        services = {role: {"active": "active"} for role in ("native", "consumer", "worker")}
        self.assertEqual(pipeline_state(native, services), "observing")
        self.assertEqual(pipeline_state({**native, "connection": "disconnected"}, services), "offline")
        self.assertEqual(pipeline_state(native, {**services, "worker": {"active": "inactive"}}), "receiving_only")
        self.assertEqual(pipeline_state(native, {**services, "consumer": {"active": "failed"}}), "failed")

    def test_unlock_is_independent_of_a_failed_status_or_saved_configuration(self):
        self.state.draft_path.touch(mode=0o600)
        self.state.draft_path.write_text("malformed")
        self.dashboard.status.side_effect = OSError("fixture failure")
        self.assertEqual(self.request("GET", "/api/status")[0], 503)
        self.assertEqual(self.request("GET", "/api/draft")[0], 503)
        self.assertEqual(self.request("GET", "/api/session"), (200, {"authenticated": True}))

    def test_desktop_confirmation_and_wechat_login_have_separate_gates(self):
        self.desktop.confirm_access.return_value = False
        self.assertEqual(self.request("POST", "/api/unlock", authenticated=False)[0], 403)
        self.desktop.confirm_access.return_value = True
        self.assertEqual(self.request("POST", "/api/unlock", authenticated=False), (200, {"key": self.state.key}))
        self.assertEqual(self.request("POST", "/api/wechat/login", authenticated=False)[0], 401)
        self.assertEqual(self.request("POST", "/api/wechat/login", headers={"Origin": "https://example.org"})[0], 403)
        self.dashboard.open_wechat_login.assert_not_called()
        self.dashboard.open_wechat_login.return_value = {"status": "opened"}
        self.assertEqual(self.request("POST", "/api/wechat/login"), (200, {"status": "opened"}))
        self.dashboard.open_wechat_login.assert_called_once_with(self.desktop)

    def test_login_opens_a_local_terminal_with_the_configured_account(self):
        binary = self.root / "apps/agent-wechat/wx"
        binary.parent.mkdir(parents=True)
        binary.touch(mode=0o700)
        token = self.root / ".local/share/agent-wechat-isolated/api-token"
        token.parent.mkdir(parents=True)
        token.touch(mode=0o600)
        config = {"agent_wechat": {"account_id": "fixture-account", "base_url": "http://127.0.0.1:6174/v1"}}
        with patch("wechat_ai_bot.admin.desktop.Path.home", return_value=self.root), \
                patch("wechat_ai_bot.admin.desktop.subprocess.run") as call:
            DesktopActions().open_wechat_login(config)
        args = call.call_args.args[0]
        self.assertEqual(args[0], "/usr/bin/ptyxis")
        self.assertIn("http://127.0.0.1:6174", args)
        self.assertIn("fixture-account", args)
        self.assertIn(str(token), args)
        self.assertIn("login", args)

    def test_invalid_or_cross_origin_writes_preserve_the_draft(self):
        desired = self.draft()
        self.state.save_draft(desired)
        original = self.state.draft_path.read_bytes()
        self.assertEqual(self.request("PUT", "/api/draft", authenticated=False, body=json.dumps(desired))[0], 401)
        self.assertEqual(self.request("PUT", "/api/draft", headers={"Origin": "https://foreign.example"},
                                      body=json.dumps(desired))[0], 403)
        for invalid in ({**desired, "replyEnd": desired["replyStart"]},
                        {**desired, "provider": "deepseek"}, {**desired, "checkinTime": "25:00"},
                        {**desired, "enabledLive": True}):
            self.assertEqual(self.request("PUT", "/api/draft", body=json.dumps(invalid))[0], 400)
        duplicate = json.dumps(desired)[:-1] + ',"model":"second-model"}'
        self.assertEqual(self.request("PUT", "/api/draft", body=duplicate)[0], 400)
        self.assertEqual(self.state.draft_path.read_bytes(), original)

    def test_live_status_distinguishes_reference_settings_from_deployed_settings(self):
        local, runtime = self.root / "local.json", self.root / "runtime.json"
        local.write_text(json.dumps({"ollama": {"model": "reference:8b"}}))
        dashboard = Dashboard(local, runtime, self.root / "no-policy.json")
        with patch("wechat_ai_bot.admin.native_status", return_value={"state": "unavailable"}), \
                patch("wechat_ai_bot.admin.service_status", return_value=({}, "")), \
                patch("wechat_ai_bot.admin.inspect_snapshot", return_value={"target_outbox_status_counts": {"queued": 2}}) as snapshot:
            result = dashboard.status()
            self.assertEqual(result["model"], {"source": "reference", "provider": "ollama", "model": "reference:8b"})
            self.assertIsNone(result["outbox"])
            snapshot.assert_not_called()
            runtime.write_text(json.dumps({"ollama": {"model": "deployed:7b"}}))
            result = dashboard.status()
            self.assertEqual(result["model"], {"source": "runtime", "provider": "ollama", "model": "deployed:7b"})
            self.assertEqual(result["outbox"], {"queued": 2})
            snapshot.assert_called_once_with(runtime)

    def test_native_status_reads_only_fixed_origin_and_redacts_payload(self):
        credential = self.root / "read-token"
        credential.touch(mode=0o600)
        credential.write_text("fixture-credential")
        wx = {"base_url": "http://127.0.0.1:6174/v1", "account_id": "fixture", "token_file": str(credential)}
        reply = {"id": "fixture", "connection": "disconnected", "incomingTransport": "disconnected",
                 "authenticated": False, "ownUsername": "private-name"}
        with patch("wechat_ai_bot.admin.build_opener") as factory:
            opener = factory.return_value
            opener.open.return_value = BytesIO(json.dumps(reply).encode())
            result = native_status({"agent_wechat": wx}, self.root / "config.json")
            self.assertEqual(result, {"state": "reachable", "connection": "disconnected",
                                     "authenticated": False, "transport": "disconnected"})
            request, = opener.open.call_args.args
            self.assertEqual(request.full_url, "http://127.0.0.1:6174/v1/accounts/fixture")
            self.assertEqual(request.get_method(), "GET")
            self.assertEqual(native_status({"agent_wechat": {**wx, "base_url": "https://foreign.example"}}, self.root),
                             {"state": "unconfigured"})
            opener.open.assert_called_once()

    def test_group_list_paginates_and_returns_only_group_names_and_ids(self):
        credential = self.root / "read-token"
        credential.touch(mode=0o600)
        credential.write_text("fixture-credential")
        wx = {"base_url": "http://127.0.0.1:6174/v1", "account_id": "fixture", "token_file": str(credential)}
        pages = [
            {"items": [{"id": "first@chatroom", "roomId": "first@chatroom", "displayName": "First",
                        "announcement": "private", "ownerId": "private"},
                       {"id": "direct", "roomId": "direct", "displayName": "Direct"}],
             "nextCursor": "a/+?="},
            {"items": [{"id": "second@chatroom", "roomId": "second@chatroom", "displayName": None},
                       {"id": "removed@chatroom", "roomId": "removed@chatroom", "deleted": True}],
             "nextCursor": None},
        ]
        with patch("wechat_ai_bot.admin.build_opener") as factory:
            opener = factory.return_value
            opener.open.side_effect = [BytesIO(json.dumps(page).encode()) for page in pages]
            result = native_groups({"agent_wechat": wx}, self.root / "config.json")
        self.assertEqual(result, {"state": "ready", "groups": [
            {"id": "first@chatroom", "name": "First"}, {"id": "second@chatroom", "name": "second@chatroom"},
        ]})
        requests = [call.args[0] for call in opener.open.call_args_list]
        self.assertEqual([request.get_method() for request in requests], ["GET", "GET"])
        base = "http://127.0.0.1:6174/v1/accounts/fixture/groups?limit=100"
        self.assertEqual([request.full_url for request in requests], [base, base + "&cursor=a%2F%2B%3F%3D"])

    def test_group_listing_uses_only_same_account_fallback(self):
        local, runtime = self.root / "local.json", self.root / "runtime.json"
        local.write_text(json.dumps({"agent_wechat": {"account_id": "reference", "target_group_id": "local@chatroom"}}))
        runtime.write_text(json.dumps({"agent_wechat": {"account_id": "deployed", "target_group_id": "live@chatroom"}}))
        dashboard = Dashboard(local, runtime)
        with patch("wechat_ai_bot.admin.native_groups", return_value={"state": "unavailable", "groups": []}) as groups:
            self.assertEqual(dashboard.groups()["configured"], {"id": "live@chatroom", "source": "runtime"})
            self.assertEqual([call.args[1] for call in groups.call_args_list], [runtime])
            groups.reset_mock()
            runtime.unlink()
            self.assertEqual(dashboard.groups()["configured"], {"id": "local@chatroom", "source": "reference"})
            self.assertEqual([call.args[1] for call in groups.call_args_list], [local])
