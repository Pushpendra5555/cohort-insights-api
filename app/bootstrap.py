"""Composition root: builds every collaborator once, for both the API and the worker."""

from __future__ import annotations

from dataclasses import dataclass

from pymongo import AsyncMongoClient
from redis.asyncio import Redis

from app.core.config import Settings
from app.db.clients import create_mongo_client, create_redis_client
from app.repositories.documents import DocumentRepository
from app.services.cache import ResultCache
from app.services.documents import DocumentService
from app.services.rate_limiter import ActiveJobLimiter


@dataclass(slots=True)
class Container:
    settings: Settings
    mongo: AsyncMongoClient
    redis: Redis
    repo: DocumentRepository
    cache: ResultCache
    limiter: ActiveJobLimiter
    documents: DocumentService

    async def close(self) -> None:
        await self.redis.aclose()
        await self.mongo.close()


async def build_container(settings: Settings) -> Container:
    mongo = create_mongo_client(settings)
    redis = create_redis_client(settings)  # lazy: no connection until first use
    repo = DocumentRepository(mongo[settings.mongo_db])
    await repo.ensure_indexes()  # idempotent; also fails fast if MongoDB is unreachable
    cache = ResultCache(redis, repo, settings)
    limiter = ActiveJobLimiter(redis, repo, settings)
    documents = DocumentService(repo, cache, limiter, settings)
    return Container(
        settings=settings,
        mongo=mongo,
        redis=redis,
        repo=repo,
        cache=cache,
        limiter=limiter,
        documents=documents,
    )
