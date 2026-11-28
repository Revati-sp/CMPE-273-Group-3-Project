"""Versioned customer session snapshot in Redis, with ownership fencing.

Key: ``chat:session:{conversation_id}:{customer_id}``, a Redis hash. Each script
touches only that one key, so it is Redis Cluster safe without hash tags.

Version and fencing rules:

- **Claim.** A pod taking a conversation always succeeds: the customer has
  reconnected to it, so it is the authoritative owner. Claiming increments
  ``session_version`` and records the new pod and connection. The new version
  is the fencing token.
- **Update.** Every later write must carry the version its writer holds. If
  the stored version differs, the write is rejected as stale and the caller
  fences its socket. An old pod that survives a partition is therefore
  rejected the first time it reaches Redis again.
- **Restore.** A snapshot from Kafka replaces the stored one only if its
  version is newer (``put_if_newer``).
- **Lease renewal** checks the version without incrementing it, so heartbeats
  do not churn versions.
"""

from __future__ import annotations

import json
import time

from engagehub_common.events import utcnow
from pydantic import BaseModel, Field
from redis.exceptions import RedisError

UNAVAILABLE = (RedisError, OSError, TimeoutError)

WORKFLOW_FROM_PLATFORM = {
    "virtual": "ACTIVE",
    "waiting_human": "PENDING_HANDOFF",
    "human_active": "HUMAN_ACTIVE",
    "resolved": "RESOLVED",
}

_CLAIM = """
local existed = 0
local prev_pod, prev_conn, prev_connected, prev_lease, prev_reason = '', '', '0', '0', ''
if redis.call('TYPE', KEYS[1])['ok'] == 'string' then
  -- Legacy marker written by older pods: {"pod": ..., "connected_at": ...}
  local legacy = redis.call('GET', KEYS[1])
  prev_pod = string.match(legacy, '"pod":%s*"([^"]*)"') or ''
  prev_connected = '1'
  existed = 1
  redis.call('DEL', KEYS[1])
elseif redis.call('EXISTS', KEYS[1]) == 1 then
  existed = 1
  prev_pod = redis.call('HGET', KEYS[1], 'current_chat_pod') or ''
  prev_conn = redis.call('HGET', KEYS[1], 'connection_id') or ''
  prev_connected = redis.call('HGET', KEYS[1], 'connected') or '0'
  prev_lease = redis.call('HGET', KEYS[1], 'lease_expires_at') or '0'
  prev_reason = redis.call('HGET', KEYS[1], 'last_disconnect_reason') or ''
end
local version = tonumber(redis.call('HGET', KEYS[1], 'session_version') or '0') + 1
redis.call('HSET', KEYS[1], 'session_version', version, 'current_chat_pod', ARGV[3],
  'connection_id', ARGV[4], 'connected', '1', 'lease_expires_at', ARGV[5], 'last_updated_at', ARGV[6],
  'last_disconnect_reason', '')
if existed == 1 then redis.call('HINCRBY', KEYS[1], 'reconnect_count', 1)
else redis.call('HSET', KEYS[1], 'created_at', ARGV[6]) end
if prev_pod ~= '' and prev_pod ~= ARGV[3] then redis.call('HINCRBY', KEYS[1], 'handover_count', 1) end
for i = 7, #ARGV, 2 do redis.call('HSET', KEYS[1], ARGV[i], ARGV[i + 1]) end
redis.call('EXPIRE', KEYS[1], ARGV[1])
return {tostring(version), tostring(existed), prev_pod, prev_conn, prev_connected, prev_lease, prev_reason}
"""

_UPDATE = """
if redis.call('TYPE', KEYS[1])['ok'] ~= 'hash' then return {'MISSING', '0', ''} end
local current = tonumber(redis.call('HGET', KEYS[1], 'session_version') or '0')
if current ~= tonumber(ARGV[1]) then
  return {'STALE', tostring(current), redis.call('HGET', KEYS[1], 'current_chat_pod') or ''}
end
if ARGV[3] == '1' then
  current = current + 1
  redis.call('HSET', KEYS[1], 'session_version', current)
end
for i = 5, #ARGV, 2 do redis.call('HSET', KEYS[1], ARGV[i], ARGV[i + 1]) end
if ARGV[3] == '1' then redis.call('HSET', KEYS[1], 'last_updated_at', ARGV[4]) end
redis.call('EXPIRE', KEYS[1], ARGV[2])
return {'OK', tostring(current), ''}
"""

_PUT_IF_NEWER = """
local current = -1
local kind = redis.call('TYPE', KEYS[1])['ok']
if kind == 'hash' then current = tonumber(redis.call('HGET', KEYS[1], 'session_version') or '0') end
if tonumber(ARGV[1]) <= current then return {'STALE', tostring(current)} end
redis.call('DEL', KEYS[1])
for i = 3, #ARGV, 2 do redis.call('HSET', KEYS[1], ARGV[i], ARGV[i + 1]) end
redis.call('EXPIRE', KEYS[1], ARGV[2])
return {'OK', ARGV[1]}
"""


class SessionUnavailable(Exception):
    """Redis could not be reached."""


class StaleSession(Exception):
    """Another connection owns a newer version of this session: the writer is fenced."""

    def __init__(self, stored_version: int, owner_pod: str) -> None:
        super().__init__(f"session is at version {stored_version}, owned by {owner_pod or 'unknown'}")
        self.stored_version = stored_version
        self.owner_pod = owner_pod


class SessionSnapshot(BaseModel):
    conversation_id: str
    tenant_id: str
    customer_id: str
    assigned_agent_type: str = ""
    assigned_agent_id: str = ""
    assigned_agent_name: str = ""
    agent_kind: str = ""  # virtual_agent, human_agent or empty
    workflow_state: str = "ACTIVE"
    last_acknowledged_sequence: int = 0
    last_acknowledged_message_id: str = ""
    last_sent_sequence: int = 0
    pending_message_ids: list[str] = Field(default_factory=list)  # sent but not yet acknowledged
    typing_status: str = ""
    current_chat_pod: str = ""
    connection_id: str = ""
    connected: bool = False
    lease_expires_at: float = 0.0
    session_version: int = 0
    last_disconnect_reason: str = ""  # client_closed, server_draining; empty while connected
    reconnect_count: int = 0
    handover_count: int = 0
    created_at: str = ""
    last_updated_at: str = ""

    @classmethod
    def from_redis(cls, raw: dict[str, str]) -> SessionSnapshot:
        data: dict = {k: v for k, v in raw.items() if k in cls.model_fields}
        data["pending_message_ids"] = json.loads(raw.get("pending_message_ids") or "[]")
        data["connected"] = raw.get("connected") == "1"
        return cls.model_validate(data)

    def to_redis(self) -> dict[str, str]:
        return encode_fields(self.model_dump())


def encode_fields(fields: dict) -> dict[str, str]:
    out = {}
    for key, value in fields.items():
        if isinstance(value, bool):
            out[key] = "1" if value else "0"
        elif isinstance(value, list):
            out[key] = json.dumps(value)
        else:
            out[key] = str(value)
    return out


def _flatten(fields: dict) -> list[str]:
    return [item for kv in encode_fields(fields).items() for item in kv]


def now_iso() -> str:
    return utcnow().isoformat().replace("+00:00", "Z")


class ClaimResult(BaseModel):
    snapshot: SessionSnapshot
    existed: bool
    previous_pod: str
    previous_connection_id: str
    previous_connected: bool
    previous_lease_valid: bool
    previous_disconnect_reason: str = ""

    @property
    def handover(self) -> bool:
        return bool(self.previous_pod) and self.previous_pod != self.snapshot.current_chat_pod

    @property
    def handover_reason(self) -> str:
        if not self.previous_connected:
            return self.previous_disconnect_reason or "previous_disconnected"
        # A live lease means the old owner may still be running: a partition, a half-open
        # socket or a second tab. Either way the newest connection wins and the old one is fenced.
        return "takeover_while_owner_alive" if self.previous_lease_valid else "owner_unreachable"


class SessionSnapshotStore:
    def __init__(self, redis, ttl_seconds: int, lease_seconds: int, clock=time.time) -> None:
        self.redis = redis
        self.ttl = ttl_seconds
        self.lease = lease_seconds
        self.clock = clock

    @staticmethod
    def key(conversation_id: str, customer_id: str) -> str:
        return f"chat:session:{conversation_id}:{customer_id}"

    async def _eval(self, script: str, key: str, *args):
        try:
            return await self.redis.eval(script, 1, key, *args)
        except UNAVAILABLE as exc:
            raise SessionUnavailable(str(exc)) from exc

    async def get(self, conversation_id: str, customer_id: str) -> SessionSnapshot | None:
        key = self.key(conversation_id, customer_id)
        try:
            if await self.redis.type(key) != "hash":
                return None
            raw = await self.redis.hgetall(key)
        except UNAVAILABLE as exc:
            raise SessionUnavailable(str(exc)) from exc
        return SessionSnapshot.from_redis(raw) if raw else None

    async def claim(
        self, *, conversation_id: str, tenant_id: str, customer_id: str, pod: str, connection_id: str, fields: dict
    ) -> ClaimResult:
        """Take ownership for this connection. ``fields`` are merged into the snapshot."""
        now = self.clock()
        base = {"conversation_id": conversation_id, "tenant_id": tenant_id, "customer_id": customer_id, **fields}
        result = await self._eval(
            _CLAIM, self.key(conversation_id, customer_id),
            self.ttl, now, pod, connection_id, now + self.lease, now_iso(), *_flatten(base),
        )  # fmt: skip
        snapshot = await self.get(conversation_id, customer_id)
        return ClaimResult(
            snapshot=snapshot,
            existed=result[1] == "1",
            previous_pod=result[2],
            previous_connection_id=result[3],
            previous_connected=result[4] == "1",
            previous_lease_valid=float(result[5] or 0) > now,
            previous_disconnect_reason=result[6],
        )

    async def update(
        self, conversation_id: str, customer_id: str, expected_version: int, fields: dict, *, bump: bool = True
    ) -> int:
        """Optimistic write. Returns the new version, or raises StaleSession."""
        result = await self._eval(
            _UPDATE, self.key(conversation_id, customer_id),
            expected_version, self.ttl, "1" if bump else "0", now_iso(), *_flatten(fields),
        )  # fmt: skip
        if result[0] == "STALE":
            raise StaleSession(int(result[1]), result[2])
        if result[0] == "MISSING":
            raise StaleSession(0, "")
        return int(result[1])

    async def renew_lease(self, conversation_id: str, customer_id: str, version: int) -> None:
        await self.update(
            conversation_id, customer_id, version, {"lease_expires_at": self.clock() + self.lease}, bump=False
        )

    async def put_if_newer(self, snapshot: SessionSnapshot) -> bool:
        """Restore a snapshot, for example from Kafka. Older or equal versions are rejected."""
        result = await self._eval(
            _PUT_IF_NEWER, self.key(snapshot.conversation_id, snapshot.customer_id),
            snapshot.session_version, self.ttl, *_flatten(snapshot.model_dump()),
        )  # fmt: skip
        return result[0] == "OK"
