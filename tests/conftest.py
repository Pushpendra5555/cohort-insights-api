"""Integration fixtures.

Tests run against REAL MongoDB and Redis (``docker compose --profile test run
--rm tests``, or locally with MONGO_URL / REDIS_URL set). Each test gets its
own database and Redis key namespace, so tests are isolated and parallel-safe.

The worker is driven explicitly (``await worker.run_once()``) with zero
latency and scripted failures, which makes pipeline interleavings
deterministic instead of sleep-and-hope.
"""

from __future__ import annotations

import asyncio
import os
import random
import uuid
from collections.abc import AsyncIterator, Callable
from typing import Any

import httpx
import pytest
from fastapi import FastAPI
from redis.exceptions import RedisError

from app.bootstrap import Container
from app.core.config import Settings
from app.domain.enums import Stage
from app.domain.processing import MockProcessor
from app.main import create_app
from app.worker.pipeline import PipelineWorker


class ScriptedFailures:
    """Fail exactly the listed (stage, attempt) pairs; succeed otherwise."""

    def __init__(self, failures: set[tuple[Stage, int]] | None = None) -> None:
        self.failures = failures or set()

    def __call__(self, stage: Stage, attempt: int) -> bool:
        return (stage, attempt) in self.failures


async def _no_sleep(_: float) -> None:
    await asyncio.sleep(0)


def make_settings(**overrides: Any) -> Settings:
    base: dict[str, Any] = {
        "mongo_url": os.getenv("MONGO_URL", "mongodb://localhost:27017"),
        "redis_url": os.getenv("REDIS_URL", "redis://localhost:6379/0"),
        "mongo_db": f"cia_test_{uuid.uuid4().hex[:12]}",
        "redis_namespace": f"cia_test_{uuid.uuid4().hex[:12]}",
        "stage1_min_s": 0,
        "stage1_max_s": 0,
        "stage2_min_s": 0,
        "stage2_max_s": 0,
        "stage_failure_rate": 0,
        "retry_base_delay_s": 0,
        "retry_max_delay_s": 0,
        "lease_duration_s": 30,
        "log_level": "WARNING",
    }
    base.update(overrides)
    return Settings(**base)


@pytest.fixture
def settings() -> Settings:
    return make_settings()


@pytest.fixture
async def app(settings: Settings) -> AsyncIterator[FastAPI]:
    application = create_app(settings)
    async with application.router.lifespan_context(application):
        container: Container = application.state.container
        yield application
        await container.mongo.drop_database(settings.mongo_db)
        try:
            keys = [k async for k in container.redis.scan_iter(match=f"{settings.redis_namespace}:*")]
            if keys:
                await container.redis.delete(*keys)
        except (RedisError, OSError):
            pass  # redis-down tests: nothing was written


@pytest.fixture
def container(app: FastAPI) -> Container:
    return app.state.container


@pytest.fixture
async def client(app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


WorkerFactory = Callable[..., PipelineWorker]


@pytest.fixture
def make_worker(container: Container, settings: Settings) -> WorkerFactory:
    def factory(
        failures: set[tuple[Stage, int]] | None = None,
        sleeper: Callable[[float], Any] = _no_sleep,
        worker_id: str = "test-worker",
    ) -> PipelineWorker:
        processor = MockProcessor(settings, ScriptedFailures(failures), sleeper=sleeper, rng=random.Random(7))
        return PipelineWorker(
            container.repo,
            container.cache,
            container.limiter,
            processor,
            settings,
            worker_id=worker_id,
            rng=random.Random(7),
        )

    return factory


@pytest.fixture
def worker(make_worker: WorkerFactory) -> PipelineWorker:
    return make_worker()


# ----------------------------------------------------------------------------- helpers


def hdr(user: str) -> dict[str, str]:
    return {"X-User-Id": user}


async def submit(client: httpx.AsyncClient, user: str, content: str, **extra: Any) -> httpx.Response:
    body = {"user_id": user, "title": extra.pop("title", "A title"), "content": content, **extra}
    return await client.post("/documents", json=body, headers=hdr(user))


async def get_doc(client: httpx.AsyncClient, user: str, doc_id: str) -> dict[str, Any]:
    resp = await client.get(f"/documents/{doc_id}", headers=hdr(user))
    assert resp.status_code == 200, resp.text
    return resp.json()


async def drain(worker: PipelineWorker, max_steps: int = 200) -> int:
    """Run the worker until nothing is due. Returns the number of stage executions."""
    steps = 0
    while steps < max_steps and await worker.run_once():
        steps += 1
    return steps


async def wait_for(predicate: Callable[[], Any], timeout: float = 5.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        if await predicate():
            return
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.01)
