"""Redis cursor: the customer's last acknowledged message.

Keys (no hash tags: each operation touches one key, so it is Redis Cluster safe):

    chat:cursor:{conversation_id}:{customer_id}     JSON cursor, advanced only on ack
    chat:delivered:{conversation_id}:{customer_id}  highest sequence sent to the customer

The session snapshot (``chat:session:...``) is managed by ``app.replies.session``.

All three expire after ``ttl`` seconds without activity. The cursor is a fast
position marker, not message storage: the PostgreSQL log holds the messages.
"""

from __future__ import annotations

import json

from engagehub_common.events import utcnow
from redis.exceptions import RedisError

from app.replies.schemas import Cursor

# Only ever moves forward, so a late or reordered ack cannot rewind the cursor.
_ADVANCE = """
local current = redis.call('GET', KEYS[1])
local last = 0
if current then last = tonumber(string.match(current, '"last_acknowledged_sequence":%s*(%d+)')) or 0 end
if tonumber(ARGV[1]) > last then
  redis.call('SET', KEYS[1], ARGV[2], 'EX', ARGV[3])
  return ARGV[2]
end
if current then redis.call('EXPIRE', KEYS[1], ARGV[3]) end
return current
"""

_MAX = """
local current = tonumber(redis.call('GET', KEYS[1]) or '0')
if tonumber(ARGV[1]) > current then redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
else redis.call('EXPIRE', KEYS[1], ARGV[2]) end
return math.max(current, tonumber(ARGV[1]))
"""

UNAVAILABLE_ERRORS = (RedisError, OSError, TimeoutError)


class CursorUnavailable(Exception):
    """Redis could not be reached. Callers fall back or return a retryable error."""


class CursorStore:
    def __init__(self, redis, ttl_seconds: int) -> None:
        self.redis = redis
        self.ttl = ttl_seconds

    @staticmethod
    def cursor_key(conversation_id: str, customer_id: str) -> str:
        return f"chat:cursor:{conversation_id}:{customer_id}"

    @staticmethod
    def _delivered_key(conversation_id: str, customer_id: str) -> str:
        return f"chat:delivered:{conversation_id}:{customer_id}"

    async def _call(self, coro):
        try:
            return await coro
        except UNAVAILABLE_ERRORS as exc:
            raise CursorUnavailable(str(exc)) from exc

    async def get(self, conversation_id: str, customer_id: str) -> Cursor | None:
        raw = await self._call(self.redis.get(self.cursor_key(conversation_id, customer_id)))
        return Cursor.model_validate_json(raw) if raw else None

    async def advance(
        self, conversation_id: str, customer_id: str, sequence: int, message_id: str, *, session_version: int = 0
    ) -> Cursor:
        value = json.dumps(
            {
                "last_acknowledged_sequence": sequence,
                "last_message_id": message_id,
                "session_version": session_version,
                "updated_at": utcnow().isoformat().replace("+00:00", "Z"),
            }
        )
        stored = await self._call(
            self.redis.eval(_ADVANCE, 1, self.cursor_key(conversation_id, customer_id), sequence, value, self.ttl)
        )
        return Cursor.model_validate_json(stored)

    async def record_delivered(self, conversation_id: str, customer_id: str, sequence: int) -> int:
        key = self._delivered_key(conversation_id, customer_id)
        return int(await self._call(self.redis.eval(_MAX, 1, key, sequence, self.ttl)))

    async def delivered(self, conversation_id: str, customer_id: str) -> int:
        return int(await self._call(self.redis.get(self._delivered_key(conversation_id, customer_id))) or 0)
