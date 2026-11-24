"""Independent single-group automatic reply policy and time window."""

from contextlib import contextmanager
from dataclasses import asdict, dataclass, replace
import fcntl
from hashlib import sha256
import json
import os
from pathlib import Path
import stat
import tempfile

from wechat_ai_bot.sender_role_boundary import BoundaryFailure, _no_duplicate_keys, _observe_target


_MAX_POLICY_NUMBER = 2**63 - 1


@dataclass(frozen=True, repr=False)
class AutomaticReplyPolicy:
    account_id: str
    thread_id: str
    generation: int
    activation_cursor: int
    starts_at: int
    expires_at: int
    enabled: bool
    paused: bool

    def permits(self, observe_config, now):
        """Use the observer config as a target restriction, never as send permission."""
        try:
            target = _observe_target(observe_config)
        except BoundaryFailure:
            return False
        return (
            target == (self.account_id, self.thread_id)
            and self.enabled is True and self.paused is False
            and self.starts_at <= now.timestamp() < self.expires_at
        )

    @property
    def digest(self):
        payload = json.dumps(asdict(self), sort_keys=True, separators=(",", ":"))
        return sha256(payload.encode("ascii")).hexdigest()


def parse_automatic_reply_policy(value):
    """Parse an explicitly supplied trusted policy snapshot. Missing input stays disabled."""
    if not isinstance(value, dict) or value.keys() != {
        "schema_version", "role", "account_id", "thread_id", "generation",
        "activation_cursor", "starts_at", "expires_at", "enabled", "paused",
    }:
        raise BoundaryFailure("AUTOMATIC_POLICY_INVALID")
    if (type(value["schema_version"]) is not int or value["schema_version"] != 1
            or value["role"] != "automatic_reply"
            or type(value["enabled"]) is not bool or type(value["paused"]) is not bool
            or any(type(value[name]) is not int or not 0 <= value[name] <= _MAX_POLICY_NUMBER
                   for name in ("generation", "activation_cursor", "starts_at", "expires_at"))
            or not value["generation"] or value["starts_at"] >= value["expires_at"]
            or any(not isinstance(value[name], str) or not value[name].strip()
                   for name in ("account_id", "thread_id"))):
        raise BoundaryFailure("AUTOMATIC_POLICY_INVALID")
    return AutomaticReplyPolicy(**{k: v for k, v in value.items() if k not in ("schema_version", "role")})


def read_automatic_reply_policy(path: Path, *, owner_uid: int = 0) -> AutomaticReplyPolicy:
    """Read a policy pinned by deployment under a trusted directory hierarchy."""
    flags = os.O_RDONLY | os.O_NOFOLLOW
    try:
        directory = os.open(path.parent, flags | os.O_DIRECTORY)
        try:
            parent = os.fstat(directory)
            if parent.st_uid != owner_uid or parent.st_mode & 0o022:
                raise BoundaryFailure("AUTOMATIC_POLICY_PERMISSIONS")
            fd = os.open(path.name, flags | os.O_NONBLOCK, dir_fd=directory)
            with os.fdopen(fd, "rb") as stream:
                info = os.fstat(stream.fileno())
                if (not stat.S_ISREG(info.st_mode) or info.st_uid != owner_uid
                        or info.st_nlink != 1 or info.st_mode & 0o137):
                    raise BoundaryFailure("AUTOMATIC_POLICY_PERMISSIONS")
                payload = stream.read(8193)
        finally:
            os.close(directory)
        if len(payload) > 8192:
            raise BoundaryFailure("AUTOMATIC_POLICY_INVALID")
        value = json.loads(payload.decode("utf-8"), object_pairs_hook=_no_duplicate_keys)
    except BoundaryFailure:
        raise
    except (ValueError, OSError, RecursionError):
        raise BoundaryFailure("AUTOMATIC_POLICY_UNAVAILABLE") from None
    return parse_automatic_reply_policy(value)


@contextmanager
def _dispatch_lock_file(path: Path, owner_uid: int):
    """Open the preinstalled lock inode through the policy directory."""
    flags = os.O_RDONLY | os.O_NOFOLLOW
    try:
        directory = os.open(path.parent, flags | os.O_DIRECTORY)
    except OSError:
        raise BoundaryFailure("AUTOMATIC_LOCK_UNAVAILABLE") from None
    try:
        parent = os.fstat(directory)
        if parent.st_uid != owner_uid or parent.st_mode & 0o022:
            raise BoundaryFailure("AUTOMATIC_LOCK_PERMISSIONS")
        try:
            fd = os.open(path.name + ".dispatch.lock", flags | os.O_NONBLOCK, dir_fd=directory)
        except OSError:
            raise BoundaryFailure("AUTOMATIC_LOCK_UNAVAILABLE") from None
        try:
            info = os.fstat(fd)
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != owner_uid
                    or info.st_nlink != 1 or stat.S_IMODE(info.st_mode) not in (0o600, 0o640)):
                raise BoundaryFailure("AUTOMATIC_LOCK_PERMISSIONS")
            yield fd, directory
        finally:
            os.close(fd)
    finally:
        os.close(directory)


@contextmanager
def automatic_dispatch_lock(path: Path, *, owner_uid: int = 0):
    """Lock before reading policy and release only after the dispatch attempt."""
    with _dispatch_lock_file(path, owner_uid) as (fd, _):
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
        else:
            yield True


def _publish_paused_policy(path: Path, policy: AutomaticReplyPolicy):
    previous = path.lstat()
    payload = {"schema_version": 1, "role": "automatic_reply", **asdict(policy)}
    fd, name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
    temporary = Path(name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, ensure_ascii=True, sort_keys=True)
            stream.write("\n")
            stream.flush()
            if os.fstat(stream.fileno()).st_gid != previous.st_gid:
                os.fchown(stream.fileno(), -1, previous.st_gid)
            os.fchmod(stream.fileno(), stat.S_IMODE(previous.st_mode))
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def pause_automatic_reply(path: Path, *, owner_uid: int = 0) -> AutomaticReplyPolicy:
    """Persist a stop before draining the dispatch lock. Only the policy owner can stop it."""
    if os.geteuid() != owner_uid:
        raise BoundaryFailure("AUTOMATIC_POLICY_PERMISSIONS")
    with _dispatch_lock_file(path, owner_uid) as (fd, directory):
        policy = read_automatic_reply_policy(path, owner_uid=owner_uid)
        if not policy.paused:
            policy = replace(policy, paused=True, generation=min(policy.generation + 1, _MAX_POLICY_NUMBER))
            _publish_paused_policy(path, policy)
        os.fsync(directory)
        fcntl.flock(fd, fcntl.LOCK_EX)
        parked = read_automatic_reply_policy(path, owner_uid=owner_uid)
        if parked != policy:
            raise BoundaryFailure("AUTOMATIC_POLICY_CHANGED")
        return parked
