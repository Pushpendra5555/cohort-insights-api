"""Content-addressed result cache.

Key: ``{ns}:cache:result:{pipeline_version}:{sha256(content)}``.

The key is a function of *content* (and the algorithm version), never of a
document id. A PATCH changes the hash, so it changes the key: the old
content's entry cannot be returned for the new content, nor vice versa.
Including ``pipeline_version`` means an algorithm change invalidates every
entry without a flush.

Two tiers:

* **Redis (hot, TTL'd).** Written by the worker after a successful final commit.
* **MongoDB (durable).** On a Redis miss *or outage*, look for any completed
  document with the same hash via the ``content_hash_state`` index. This keeps
  the cache correct when Redis is down and effective beyond the TTL.

Every hit is re-verified against the requested hash before use.

Scope assumption: the cache is global across users. Results are pure
functions of content, so sharing them reveals nothing the submitter does not
already hold. The residual timing side channel ("someone else submitted this
exact text before") is documented in the README; scoping per user is a
one-line key change.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass

from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.core.config import Settings
from app.domain.document import current_result
from app.repositories.documents import DocumentRepository

logger = logging.getLogger(__name__)

_REDIS_ERRORS = (RedisError, OSError)


@dataclass(frozen=True, slots=True)
class CachedResult:
    summary: str
    tags: list[str]

    def as_tuple(self) -> tuple[str, list[str]]:
        return self.summary, list(self.tags)


class ResultCache:
    def __init__(self, redis: Redis, repo: DocumentRepository, settings: Settings) -> None:
        self._redis = redis
        self._repo = repo
        self._ttl = settings.cache_ttl_s
        self._pv = settings.pipeline_version
        self._ns = settings.redis_namespace

    def key(self, chash: str) -> str:
        return f"{self._ns}:cache:result:{self._pv}:{chash}"

    async def get(self, chash: str) -> CachedResult | None:
        hit = await self._get_redis(chash)
        if hit is not None:
            return hit
        doc = await self._repo.find_completed_by_hash(chash, self._pv)
        if doc is None:
            return None
        result = current_result(doc)
        if result is None or result["content_hash"] != chash:
            return None
        cached = CachedResult(summary=result["summary"], tags=result["tags"])
        await self.set(chash, cached)  # warm the hot tier; best effort
        return cached

    async def set(self, chash: str, value: CachedResult) -> None:
        payload = json.dumps(
            {"content_hash": chash, "pipeline_version": self._pv, "summary": value.summary, "tags": value.tags}
        )
        try:
            await self._redis.set(self.key(chash), payload, ex=self._ttl)
        except _REDIS_ERRORS as exc:
            logger.warning("cache: write skipped, redis unavailable", extra={"error": repr(exc)})

    async def _get_redis(self, chash: str) -> CachedResult | None:
        try:
            raw = await self._redis.get(self.key(chash))
        except _REDIS_ERRORS as exc:
            logger.warning("cache: redis unavailable, using MongoDB tier", extra={"error": repr(exc)})
            return None
        if raw is None:
            return None
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            logger.error("cache: corrupt entry ignored", extra={"key": self.key(chash)})
            return None
        if data.get("content_hash") != chash or data.get("pipeline_version") != self._pv:
            logger.error("cache: entry/key mismatch ignored", extra={"key": self.key(chash)})
            return None
        return CachedResult(summary=data["summary"], tags=list(data["tags"]))
