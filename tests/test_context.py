import sqlite3
import unittest

from wechat_ai_bot.context import build_context


ACCOUNT = "test-account"
GROUP = "target@chatroom"


def config(limit=12):
    return {
        "schema_version": 2,
        "agent_wechat": {
            "account_id": ACCOUNT,
            "target_group_id": GROUP,
        },
        "safety": {
            "group_allowlist": [GROUP],
        },
        "processing": {
            "max_context_messages": limit,
        },
    }


class ContextTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.db.execute("""
            CREATE TABLE messages (
                message_id TEXT,
                account_id TEXT,
                thread_id TEXT,
                sender_id TEXT,
                direction TEXT,
                body TEXT,
                status TEXT,
                created_at TEXT
            )
        """)

    def tearDown(self):
        self.db.close()

    def add(
        self, msg_id, text, *,
        account=ACCOUNT,
        group=GROUP,
        direction="incoming",
        sender="member-a",
        status="pending",
    ):
        self.db.execute(
            "INSERT INTO messages VALUES (?,?,?,?,?,?,?,?)",
            (
                msg_id,
                account,
                group,
                sender,
                direction,
                text,
                status,
                f"2026-11-21T00:00:{int(msg_id):02d}Z",
            ),
        )

    def test_account_and_group_isolation(self):
        self.add("01", "允许")
        self.add("02", "其他群", group="other@chatroom")
        self.add("03", "其他账号", account="other-account")

        result = build_context(self.db, config())
        self.assertEqual(
            [item["text"] for item in result],
            ["允许"],
        )

    def test_chronological_order(self):
        self.add("01", "第一条")
        self.add("02", "第二条")
        self.add("03", "第三条")

        result = build_context(self.db, config())
        self.assertEqual(
            [item["text"] for item in result],
            ["第一条", "第二条", "第三条"],
        )

    def test_outgoing_not_treated_as_ai(self):
        self.add("01", "群友消息")
        self.add(
            "02", "当前账号发言",
            direction="outgoing",
        )

        result = build_context(self.db, config())
        self.assertEqual(
            result[-1]["role"],
            "account_owner",
        )

    def test_limits(self):
        for i in range(1, 21):
            self.add(f"{i:02d}", "内容" * 500)

        result = build_context(self.db, config(limit=12))
        self.assertLessEqual(len(result), 12)
        self.assertLessEqual(
            sum(len(item["text"]) for item in result),
            1600,
        )
        self.assertTrue(
            all(len(item["text"]) <= 320 for item in result)
        )

    def test_empty_messages_excluded(self):
        self.add("01", "  ")
        self.add("02", "有效消息")

        result = build_context(self.db, config())
        self.assertEqual(len(result), 1)

    def test_non_allowlisted_group_rejected(self):
        cfg = config()
        cfg["safety"]["group_allowlist"] = []

        with self.assertRaises(ValueError):
            build_context(self.db, cfg)


if __name__ == "__main__":
    unittest.main()
