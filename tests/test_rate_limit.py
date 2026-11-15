"""Per-user cap of 3 documents across {queued, processing, enriching}."""

from __future__ import annotations

import asyncio
import time

import httpx
import pytest
from bson import ObjectId
from fastapi import FastAPI

from app.bootstrap import Container
from app.core.errors import RateLimited
from app.domain.document import slot_member
from app.main import create_app
from tests.conftest import drain, hdr, make_settings, submit


async def test_fourth_active_document_is_429(client: httpx.AsyncClient) -> None:
    for i in range(3):
        assert (await submit(client, "alice", f"Active doc {i}.")).status_code == 201
    resp = await submit(client, "alice", "One too many.")
    assert resp.status_code == 429
    assert resp.json()["error"]["code"] == "rate_limited"
    assert "Retry-After" in resp.headers


async def test_limit_is_per_user(client: httpx.AsyncClient) -> None:
    for i in range(3):
        await submit(client, "alice", f"Alice {i}.")
    assert (await submit(client, "bob", "Bob is unaffected.")).status_code == 201


async def test_enriching_counts_as_active(client: httpx.AsyncClient, worker) -> None:
    for i in range(3):
        await submit(client, "alice", f"Doc {i}.")
    for _ in range(3):
        await worker.run_once()  # all three now in `enriching`
    states = {
        d["status"]
        for d in (await client.get("/users/alice/documents", headers=hdr("alice"))).json()["items"]
    }
    assert states == {"enriching"}
    assert (await submit(client, "alice", "Still blocked.")).status_code == 429


async def test_completion_and_failure_free_slots(client: httpx.AsyncClient, worker) -> None:
    for i in range(3):
        await submit(client, "alice", f"Doc {i}.")
    await drain(worker)
    for i in range(3):
        assert (await submit(client, "alice", f"Next batch {i}.")).status_code == 201


async def test_concurrent_submissions_never_exceed_limit(client: httpx.AsyncClient) -> None:
    responses = await asyncio.gather(*(submit(client, "alice", f"Burst {i}.") for i in range(10)))
    codes = [r.status_code for r in responses]
    assert codes.count(201) == 3 and codes.count(429) == 7


async def test_cache_hits_bypass_the_limiter(client: httpx.AsyncClient, worker) -> None:
    await submit(client, "alice", "Cached content.")
    await drain(worker)
    for i in range(3):
        await submit(client, "alice", f"Fill slot {i}.")
    resp = await submit(client, "alice", "Cached content.")  # needs no processing
    assert resp.status_code == 201 and resp.json()["cache_hit"] is True


async def test_patch_of_active_document_transfers_its_slot(client: httpx.AsyncClient) -> None:
    ids = [(await submit(client, "alice", f"Doc {i}.")).json()["document_id"] for i in range(3)]
    resp = await client.patch(f"/documents/{ids[0]}", json={"content": "Revised."}, headers=hdr("alice"))
    assert resp.status_code == 200  # already counted: no extra capacity needed


async def test_patch_of_completed_document_needs_capacity(client: httpx.AsyncClient, worker) -> None:
    done = (await submit(client, "alice", "Finished doc.")).json()["document_id"]
    await drain(worker)
    for i in range(3):
        await submit(client, "alice", f"Busy {i}.")
    resp = await client.patch(f"/documents/{done}", json={"content": "Reopen me."}, headers=hdr("alice"))
    assert resp.status_code == 429


async def test_late_release_of_old_version_cannot_free_new_slot(container: Container) -> None:
    """Version-scoped slots: a fenced-out worker releasing v must not free v+1."""
    limiter, doc = container.limiter, ObjectId()
    await limiter.acquire("alice", doc, slot_member(str(doc), 1))
    await limiter.acquire("alice", doc, slot_member(str(doc), 2), transfer_from=slot_member(str(doc), 1))
    await limiter.release("alice", slot_member(str(doc), 1))  # stale release
    members = await container.redis.zrange(limiter.key("alice"), 0, -1)
    assert members == [slot_member(str(doc), 2)]


async def test_leaked_slots_expire(container: Container, settings) -> None:
    key = container.limiter.key("alice")
    await container.redis.zadd(key, {f"{ObjectId()}:1": time.time() - 1 for _ in range(3)})
    await container.limiter.acquire("alice", ObjectId(), "fresh:1")  # expired members purged first


@pytest.fixture
async def redis_down_app() -> FastAPI:
    settings = make_settings(redis_url="redis://127.0.0.1:1/0", redis_socket_timeout_s=0.2)
    application = create_app(settings)
    async with application.router.lifespan_context(application):
        yield application
        await application.state.container.mongo.drop_database(settings.mongo_db)


async def test_redis_outage_falls_back_to_mongodb_count(redis_down_app: FastAPI) -> None:
    transport = httpx.ASGITransport(app=redis_down_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        for i in range(3):
            assert (await submit(client, "alice", f"Degraded {i}.")).status_code == 201
        assert (await submit(client, "alice", "Over the limit.")).status_code == 429
        health = (await client.get("/health")).json()
        assert health == {"status": "degraded", "checks": {"mongodb": "ok", "redis": "unavailable"}}


async def test_direct_acquire_raises_domain_error(container: Container) -> None:
    for i in range(3):
        doc = ObjectId()
        await container.limiter.acquire("zed", doc, slot_member(str(doc), 1))
    with pytest.raises(RateLimited):
        doc = ObjectId()
        await container.limiter.acquire("zed", doc, slot_member(str(doc), 1))
