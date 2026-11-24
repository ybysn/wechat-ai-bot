"""Fail-closed timing guards for AI reply candidates (no sending)."""

from datetime import datetime, timezone


REPLY_FRESHNESS_SECONDS = 120


def utc_now(now=None):
    """Validate an injected clock or obtain the current aware UTC time."""
    if now is None:
        return datetime.now(timezone.utc)
    if not isinstance(now, datetime) or now.tzinfo is None:
        raise ValueError("now must be a timezone-aware datetime")
    return now.astimezone(timezone.utc)


def _parse_utc(value, *, sqlite_timestamp=False):
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        if not sqlite_timestamp:
            return None  # Source messages require an explicit timezone.
        # SQLite DEFAULT CURRENT_TIMESTAMP is UTC without a zone suffix.
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def candidate_is_fresh(message_created_at, decision_created_at=None, *, now=None):
    """True iff original message AND optional decision are both <=120s old.

    Future, missing, or malformed timestamps fail closed. For historical
    SQLite decision timestamps, timezone-naive values mean UTC, not local time.
    """
    current = utc_now(now)
    message_time = _parse_utc(message_created_at)
    if message_time is None:
        return False
    age = (current - message_time).total_seconds()
    if age < 0 or age > REPLY_FRESHNESS_SECONDS:
        return False
    if decision_created_at is not None:
        decision_time = _parse_utc(decision_created_at, sqlite_timestamp=True)
        if decision_time is None:
            return False
        decision_age = (current - decision_time).total_seconds()
        if decision_age < 0 or decision_age > REPLY_FRESHNESS_SECONDS:
            return False
    return True
