"""Bounded, group-scoped conversation context."""

def build_context(conn, config):
    if config.get("schema_version") != 2:
        raise ValueError("Requires config schema v2")

    wx = config["agent_wechat"]
    account = wx["account_id"]
    group = wx["target_group_id"]

    if group not in config["safety"]["group_allowlist"]:
        raise ValueError("Group is not allowlisted")

    limit = config["processing"]["max_context_messages"]
    if type(limit) is not int or not 1 <= limit <= 20:
        raise ValueError("Invalid context message limit")

    # Additional bounds for a 4096-token local model.
    per_message_chars = 320
    total_chars = 1600

    rows = conn.execute(
        """
        SELECT message_id, sender_id, direction, body
        FROM messages
        WHERE account_id = ?
          AND thread_id = ?
          AND direction IN ('incoming', 'outgoing')
          AND body IS NOT NULL
          AND TRIM(body) != ''
          AND status IN ('pending', 'processed', 'ignored')
        ORDER BY
          (created_at IS NULL) ASC,
          created_at DESC,
          rowid DESC
        LIMIT ?
        """,
        (account, group, limit),
    ).fetchall()

    remaining = total_chars
    speakers = {}
    selected = []

    # Newest messages get priority when the budget is tight.
    for message_id, sender_id, direction, body in rows:
        if remaining <= 0:
            break

        text = body.strip()
        text = text[:min(per_message_chars, remaining)]

        if not text:
            continue

        remaining -= len(text)

        if direction == "outgoing":
            role = "account_owner"
            label = "当前微信账号"
        else:
            role = "group_member"
            identity = sender_id or f"unknown:{message_id}"
            if identity not in speakers:
                speakers[identity] = len(speakers) + 1
            label = f"群友{speakers[identity]}"

        selected.append({
            "role": role,
            "speaker": label,
            "text": text,
        })

    # Restore chronological order for the language model.
    return list(reversed(selected))
