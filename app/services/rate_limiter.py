"""Per-user cap on documents in {queued, processing, enriching}.

Design
------
One Redis sorted set per user: ``{ns}:rl:active:{user_id}``.

* **member** = ``"{document_id}:{content_version}"`` -- a *slot*, not a counter.
* **score**  = slot expiry (unix seconds).

Why a set of slots instead of INCR/DECR:

* **Idempotent.** Acquiring an already-held slot or releasing an already-freed
  one is a no-op. A counter drifts permanently on any double-decrement, crash
  between steps, or retried message.
* **Self-healing.** Every acquire first purges members whose score is in the
  past, so a slot leaked by a crashed process blocks the user for at most
  ``rate_limit_slot_ttl_s``, never forever. Workers refresh the score while
  they hold the job.
* **Version-scoped.** A PATCH on an active document *transfers* slot v -> v+1
  atomically (no capacity check: the document was already counted). A late
  release of slot v by a fenced-out worker then cannot free the v+1 slot.

All check-and-modify logic runs inside one Lua script, so concurrent submits
for the same user cannot both pass the capacity check.

Degradation: if Redis is unreachable we fall back to counting active
documents in MongoDB (served by the ``user_state_created`` index). That path
is check-then-act and can admit one extra job under a tight race; we accept
that during an outage rather than rejecting all writes. MongoDB, not Redis,
is the source of truth for which documents are active.
"""

from __future__ import annotations

import logging
import time

from bson import ObjectId
from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.core.config import Settings
from app.core.errors import RateLimited
from app.repositories.documents import DocumentRepository

logger = logging.getLogger(__name__)

_ACQUIRE_LUA = """
-- KEYS[1] slot set | ARGV: new_member, transfer_from ('' = none), limit, now, expires_at, key_ttl
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[4])
local function grant()
  redis.call('ZADD', KEYS[1], ARGV[5], ARGV[1])
  redis.call('EXPIRE', KEYS[1], ARGV[6])
  return 1
end
if ARGV[2] ~= '' and redis.call('ZSCORE', KEYS[1], ARGV[2]) then
  redis.call('ZREM', KEYS[1], ARGV[2])
  return grant()
end
if redis.call('ZSCORE', KEYS[1], ARGV[1]) then
  return grant()
end
if redis.call('ZCARD', KEYS[1]) >= tonumber(ARGV[3]) then
  return 0
end
return grant()
"""

_RECONCILE_LUA = """
-- KEYS[1] slot set | ARGV: doc_prefix, keep_member ('' = none), expires_at, key_ttl
local members = redis.call('ZRANGE', KEYS[1], 0, -1)
local plen = string.len(ARGV[1])
for _, m in ipairs(members) do
  if string.sub(m, 1, plen) == ARGV[1] and m ~= ARGV[2] then
    redis.call('ZREM', KEYS[1], m)
  end
end
if ARGV[2] ~= '' then
  redis.call('ZADD', KEYS[1], ARGV[3], ARGV[2])
  redis.call('EXPIRE', KEYS[1], ARGV[4])
end
return 1
"""

_REDIS_ERRORS = (RedisError, OSError)


class ActiveJobLimiter:
    def __init__(self, redis: Redis, repo: DocumentRepository, settings: Settings) -> None:
        self._redis = redis
        self._repo = repo
        self._limit = settings.max_active_docs_per_user
        self._ttl = settings.rate_limit_slot_ttl_s
        self._ns = settings.redis_namespace
        self._acquire = redis.register_script(_ACQUIRE_LUA)
        self._reconcile = redis.register_script(_RECONCILE_LUA)

    def key(self, user_id: str) -> str:
        return f"{self._ns}:rl:active:{user_id}"

    async def acquire(
        self,
        user_id: str,
        doc_id: ObjectId,
        member: str,
        transfer_from: str | None = None,
    ) -> None:
        now = time.time()
        try:
            granted = await self._acquire(
                keys=[self.key(user_id)],
                args=[member, transfer_from or "", self._limit, now, now + self._ttl, self._ttl],
            )
        except _REDIS_ERRORS as exc:
            logger.warning(
                "rate limiter: redis unavailable, falling back to MongoDB count",
                extra={"user_id": user_id, "error": repr(exc)},
            )
            active = await self._repo.count_active(user_id, exclude_id=doc_id)
            granted = 1 if active < self._limit else 0
        if not granted:
            raise RateLimited(
                f"at most {self._limit} documents may be in the pipeline concurrently",
                details={"limit": self._limit},
            )

    async def release(self, user_id: str, member: str) -> None:
        try:
            await self._redis.zrem(self.key(user_id), member)
        except _REDIS_ERRORS as exc:
            # Not fatal: the slot expires on its own (score = expiry).
            logger.warning(
                "rate limiter: release failed; slot will expire",
                extra={"user_id": user_id, "member": member, "error": repr(exc)},
            )

    async def touch(self, user_id: str, member: str) -> None:
        """Refresh a held slot's expiry (called by workers when they start a stage)."""
        try:
            await self._redis.zadd(self.key(user_id), {member: time.time() + self._ttl}, xx=True)
        except _REDIS_ERRORS as exc:
            logger.warning(
                "rate limiter: touch failed",
                extra={"user_id": user_id, "member": member, "error": repr(exc)},
            )

    async def reconcile(self, user_id: str, slot_base: str, active_member: str | None) -> None:
        """Force one document's slots to match MongoDB's view (used after aborted writes)."""
        now = time.time()
        try:
            await self._reconcile(
                keys=[self.key(user_id)],
                args=[f"{slot_base}:", active_member or "", now + self._ttl, self._ttl],
            )
        except _REDIS_ERRORS as exc:
            logger.warning(
                "rate limiter: reconcile failed; slots will expire",
                extra={"user_id": user_id, "slot_base": slot_base, "error": repr(exc)},
            )
