"""Hot session state in Redis.

Every key for one conversation shares the ``{tenant:conversation}`` hash tag, so
all of them live in one Redis Cluster slot and the Lua scripts below run
atomically. The tenant is part of the key: a lookup using a principal's tenant
cannot reach another tenant's session.

Message lifecycle:

    reserve -> sequence number assigned, message stored in ``pending``
    publish -> Kafka acknowledges with acks=all (handled by ConversationLog)
    commit  -> moved from ``pending`` into the ``log`` buffer of unacknowledged messages
    ack     -> client confirmed receipt, buffer trimmed up to that sequence number

``pending`` is what lets a replacement pod finish a dead pod's work: anything
reserved but never confirmed can be republished with its original identity.
"""

from __future__ import annotations

from dataclasses import dataclass

from pydantic import BaseModel

from engagehub_common.events import utcnow

WORKFLOW_VIRTUAL = "virtual"
WORKFLOW_WAITING_HUMAN = "waiting_human"
WORKFLOW_HUMAN = "human_active"
WORKFLOW_RESOLVED = "resolved"

_RESERVE = """
if redis.call('EXISTS', KEYS[1]) == 0 then return {'NO_SESSION'} end
if ARGV[5] ~= '' then
  local current = redis.call('HGET', KEYS[1], ARGV[5])
  if current and current ~= ARGV[6] then return {'STALE'} end
end
local existing = redis.call('HGET', KEYS[2], ARGV[1])
if existing then
  local seq = string.match(existing, '^(%d+)|')
  local is_pending = redis.call('HEXISTS', KEYS[3], seq)
  return {'DUP', existing, tostring(is_pending)}
end
local seq = redis.call('HINCRBY', KEYS[1], 'seq', 1)
local record = seq .. '|' .. ARGV[2]
redis.call('HSET', KEYS[2], ARGV[1], record)
redis.call('HSET', KEYS[3], seq, ARGV[4] .. '|' .. ARGV[2])
redis.call('EXPIRE', KEYS[1], ARGV[3])
redis.call('EXPIRE', KEYS[2], ARGV[3])
redis.call('EXPIRE', KEYS[3], ARGV[3])
return {'NEW', record}
"""

_COMMIT = """
redis.call('HDEL', KEYS[2], ARGV[1])
local ack = tonumber(redis.call('HGET', KEYS[1], 'last_ack_seq') or '0')
if tonumber(ARGV[1]) > ack then
  redis.call('ZADD', KEYS[3], ARGV[1], ARGV[2])
  redis.call('ZREMRANGEBYRANK', KEYS[3], 0, -(tonumber(ARGV[5]) + 1))
  redis.call('EXPIRE', KEYS[3], ARGV[4])
end
local committed = tonumber(redis.call('HGET', KEYS[1], 'last_committed_seq') or '0')
if tonumber(ARGV[1]) > committed then
  redis.call('HSET', KEYS[1], 'last_committed_seq', ARGV[1], 'kafka_offset', ARGV[3])
end
return 1
"""

_ACK = """
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
local field = ARGV[2]
local up_to = tonumber(ARGV[1])
local head = tonumber(redis.call('HGET', KEYS[1], 'seq') or '0')
if up_to > head then return -1 end
local current = tonumber(redis.call('HGET', KEYS[1], field) or '0')
if up_to <= current then return current end
redis.call('HSET', KEYS[1], field, up_to)
if ARGV[3] == '1' then
  redis.call('HSET', KEYS[1], 'last_ack_seq', up_to, 'last_ack_message_id', ARGV[4])
  redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', up_to)
end
return up_to
"""

_CONNECT = """
if redis.call('EXISTS', KEYS[1]) == 0 then return {'NO_SESSION'} end
local epoch = redis.call('HINCRBY', KEYS[1], 'epoch:' .. ARGV[1], 1)
local previous = redis.call('HGET', KEYS[1], 'conn:' .. ARGV[1]) or ''
redis.call('HSET', KEYS[1], 'conn:' .. ARGV[1], ARGV[2])
if ARGV[3] == '1' then redis.call('HSET', KEYS[1], 'current_pod', ARGV[2]) end
redis.call('EXPIRE', KEYS[1], ARGV[4])
return {'OK', tostring(epoch), previous}
"""

_DISCONNECT = """
local current = redis.call('HGET', KEYS[1], 'epoch:' .. ARGV[1])
if current == ARGV[2] then redis.call('HDEL', KEYS[1], 'conn:' .. ARGV[1]) end
return 1
"""

_UPDATE_IF_EXISTS = """
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
for i = 1, #ARGV, 2 do redis.call('HSET', KEYS[1], ARGV[i], ARGV[i + 1]) end
return 1
"""

_RESTORE = """
if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
for i = 2, #ARGV, 2 do redis.call('HSET', KEYS[1], ARGV[i], ARGV[i + 1]) end
redis.call('EXPIRE', KEYS[1], ARGV[1])
return 1
"""


class SessionState(BaseModel):
    conversation_id: str
    session_id: str
    tenant_id: str
    customer_id: str
    assigned_agent_type: str = "virtual"
    assigned_agent_id: str = ""
    assigned_agent_name: str = ""
    current_pod: str = ""
    seq: int = 0
    last_committed_seq: int = 0
    last_ack_seq: int = 0
    last_ack_message_id: str = ""
    workflow_state: str = WORKFLOW_VIRTUAL
    language: str = "en"
    customer_tier: str = ""
    typing: str = ""
    uncertain_count: int = 0
    negative_count: int = 0
    last_intent: str = ""
    created_at: str = ""
    expires_at: str = ""


@dataclass(frozen=True)
class Reservation:
    status: str  # NEW, DUP, NO_SESSION or STALE
    sequence_number: int = 0
    envelope_json: str = ""
    pending: bool = False


class SessionStore:
    def __init__(self, redis, ttl_seconds: int = 24 * 3600, hot_log_cap: int = 500) -> None:
        self.redis = redis
        self.ttl = ttl_seconds
        self.hot_log_cap = hot_log_cap

    @staticmethod
    def _key(tenant_id: str, conversation_id: str, suffix: str) -> str:
        return f"eh:{{{tenant_id}:{conversation_id}}}:{suffix}"

    def _keys(self, tenant_id: str, conversation_id: str, *suffixes: str) -> list[str]:
        return [self._key(tenant_id, conversation_id, s) for s in suffixes]

    def _expiry_iso(self) -> str:
        return str(int(utcnow().timestamp()) + self.ttl)

    async def create(self, state: SessionState) -> None:
        key = self._key(state.tenant_id, state.conversation_id, "session")
        fields = state.model_dump()
        fields["created_at"] = fields["created_at"] or utcnow().isoformat()
        fields["expires_at"] = self._expiry_iso()
        args = [item for kv in fields.items() for item in (kv[0], str(kv[1]))]
        await self.redis.eval(_RESTORE, 1, key, self.ttl, *args)

    async def restore(self, state: SessionState) -> bool:
        """Rebuild a lost session. Returns False if another pod already restored it."""
        key = self._key(state.tenant_id, state.conversation_id, "session")
        fields = state.model_dump()
        fields["expires_at"] = self._expiry_iso()
        args = [item for kv in fields.items() for item in (kv[0], str(kv[1]))]
        return bool(await self.redis.eval(_RESTORE, 1, key, self.ttl, *args))

    async def get(self, tenant_id: str, conversation_id: str) -> SessionState | None:
        raw = await self.redis.hgetall(self._key(tenant_id, conversation_id, "session"))
        if not raw:
            return None
        known = {k: v for k, v in raw.items() if k in SessionState.model_fields}
        return SessionState.model_validate(known)

    async def update(self, tenant_id: str, conversation_id: str, **fields) -> bool:
        args = [item for kv in fields.items() for item in (kv[0], str(kv[1]))]
        key = self._key(tenant_id, conversation_id, "session")
        return bool(await self.redis.eval(_UPDATE_IF_EXISTS, 1, key, *args))

    async def increment(self, tenant_id: str, conversation_id: str, field: str) -> int:
        return int(await self.redis.hincrby(self._key(tenant_id, conversation_id, "session"), field, 1))

    async def connect(
        self, tenant_id: str, conversation_id: str, participant: str, pod: str, is_customer: bool
    ) -> tuple[int, str] | None:
        """Register a connection. Returns (fencing epoch, previous pod), or None if no session."""
        key = self._key(tenant_id, conversation_id, "session")
        result = await self.redis.eval(_CONNECT, 1, key, participant, pod, "1" if is_customer else "0", self.ttl)
        if result[0] == "NO_SESSION":
            return None
        return int(result[1]), result[2]

    async def disconnect(self, tenant_id: str, conversation_id: str, participant: str, epoch: int) -> None:
        """Clean close: the next connection is a fresh one, not a migration."""
        key = self._key(tenant_id, conversation_id, "session")
        await self.redis.eval(_DISCONNECT, 1, key, participant, str(epoch))

    async def reserve(
        self,
        tenant_id: str,
        conversation_id: str,
        idempotency_key: str,
        envelope_json: str,
        *,
        participant: str = "",
        epoch: int | None = None,
    ) -> Reservation:
        keys = self._keys(tenant_id, conversation_id, "session", "idem", "pending")
        now_ms = int(utcnow().timestamp() * 1000)
        epoch_field = f"epoch:{participant}" if epoch is not None else ""
        result = await self.redis.eval(
            _RESERVE, 3, *keys, idempotency_key, envelope_json, self.ttl, now_ms,
            epoch_field, str(epoch or ""),
        )  # fmt: skip
        status = result[0]
        if status in ("NO_SESSION", "STALE"):
            return Reservation(status)
        seq, _, stored = result[1].partition("|")
        return Reservation(status, int(seq), stored, pending=status == "DUP" and result[2] == "1")

    async def commit(
        self, tenant_id: str, conversation_id: str, seq: int, envelope_json: str, kafka_offset: int
    ) -> None:
        keys = self._keys(tenant_id, conversation_id, "session", "pending", "log")
        await self.redis.eval(_COMMIT, 3, *keys, seq, envelope_json, kafka_offset, self.ttl, self.hot_log_cap)

    async def record_ack(
        self,
        tenant_id: str,
        conversation_id: str,
        participant: str,
        up_to_seq: int,
        *,
        is_customer: bool,
        message_id: str = "",
    ) -> int:
        """Advance a participant's ack. Returns the new value, or -1 if beyond the head."""
        keys = self._keys(tenant_id, conversation_id, "session", "log")
        result = await self.redis.eval(
            _ACK, 2, *keys, up_to_seq, f"ack:{participant}", "1" if is_customer else "0", message_id
        )
        return int(result)

    async def participant_ack(self, tenant_id: str, conversation_id: str, participant: str) -> int:
        key = self._key(tenant_id, conversation_id, "session")
        return int(await self.redis.hget(key, f"ack:{participant}") or 0)

    async def hot_log_after(self, tenant_id: str, conversation_id: str, after_seq: int) -> list[str]:
        key = self._key(tenant_id, conversation_id, "log")
        return await self.redis.zrangebyscore(key, f"({after_seq}", "+inf")

    async def unacked_count(self, tenant_id: str, conversation_id: str) -> int:
        return int(await self.redis.zcard(self._key(tenant_id, conversation_id, "log")))

    async def pending(self, tenant_id: str, conversation_id: str) -> dict[int, tuple[int, str]]:
        """Reserved but unconfirmed messages: ``{seq: (reserved_at_ms, envelope_json)}``."""
        raw = await self.redis.hgetall(self._key(tenant_id, conversation_id, "pending"))
        out: dict[int, tuple[int, str]] = {}
        for seq, value in raw.items():
            reserved_at, _, envelope = value.partition("|")
            out[int(seq)] = (int(reserved_at), envelope)
        return out

    async def delete_all(self, tenant_id: str, conversation_id: str) -> None:
        """Remove all hot state for a conversation (used by the Redis-loss chaos test)."""
        for suffix in ("session", "idem", "pending", "log"):
            await self.redis.delete(self._key(tenant_id, conversation_id, suffix))

    # Virtual-agent worker availability.
    @staticmethod
    def _workers_key(tenant_scope: str = "global") -> str:
        return f"eh:{{workers:{tenant_scope}}}:heartbeats"

    async def worker_heartbeat(self, agent_type: str, instance: str) -> None:
        await self.redis.zadd(self._workers_key(), {f"{agent_type}|{instance}": utcnow().timestamp()})

    async def worker_stopped(self, agent_type: str, instance: str) -> None:
        await self.redis.zrem(self._workers_key(), f"{agent_type}|{instance}")

    async def live_workers(self, ttl_seconds: int) -> dict[str, int]:
        cutoff = utcnow().timestamp() - ttl_seconds
        members = await self.redis.zrangebyscore(self._workers_key(), cutoff, "+inf")
        counts: dict[str, int] = {}
        for member in members:
            agent_type = member.split("|", 1)[0]
            counts[agent_type] = counts.get(agent_type, 0) + 1
        return counts
