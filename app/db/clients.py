from __future__ import annotations

from pymongo import AsyncMongoClient
from redis.asyncio import Redis

from app.core.config import Settings


def create_mongo_client(settings: Settings) -> AsyncMongoClient:
    # tz_aware=True: datetimes round-trip as timezone-aware UTC, so comparisons
    # between "now" and stored timestamps are never naive-vs-aware bugs.
    return AsyncMongoClient(
        settings.mongo_url,
        tz_aware=True,
        serverSelectionTimeoutMS=settings.mongo_server_selection_timeout_ms,
        appname=settings.app_name,
    )


def create_redis_client(settings: Settings) -> Redis:
    # Short timeouts: Redis is an accelerator here, never a hard dependency.
    # A slow/unavailable Redis must degrade to the MongoDB fallback quickly.
    return Redis.from_url(
        settings.redis_url,
        decode_responses=True,
        socket_timeout=settings.redis_socket_timeout_s,
        socket_connect_timeout=settings.redis_socket_timeout_s,
        health_check_interval=30,
    )
