"""Content-hash cache: immediate results for identical content, safe across PATCH."""

from __future__ import annotations

import asyncio

import httpx
import pytest
from fastapi import FastAPI

from app.bootstrap import Container
from app.domain.hashing import content_hash, normalize_content
from app.domain.processing import MockProcessor
from app.main import create_app
from app.worker.pipeline import PipelineWorker
from tests.conftest import ScriptedFailures, drain, get_doc, hdr, make_settings, submit
from tests.invariants import assert_consistent

X = "Microgrids keep hospitals running. Storage smooths demand peaks."
Y = "Vertical farms use less water. Lighting remains the main cost."


async def test_identical_content_returns_result_immediately(client: httpx.AsyncClient, worker) -> None:
    first = (await submit(client, "alice", X)).json()["document_id"]
    await drain(worker)

    resp = await submit(client, "alice", X, title="Different title, same content")
    body = resp.json()
    assert resp.status_code == 201 and body["cache_hit"] is True and body["status"] == "completed"
    assert body["document_id"] != first  # a new document, served from cache

    doc = await get_doc(client, "alice", body["document_id"])
    original = await get_doc(client, "alice", first)
    assert doc["result"]["summary"] == original["result"]["summary"]
    assert doc["result"]["tags"] == original["result"]["tags"]
    assert doc["result"]["source"] == "cache"
    assert doc["stages"]["processing"]["status"] == "cached"


async def test_cache_survives_transport_normalisation(client: httpx.AsyncClient, worker) -> None:
    await submit(client, "alice", "Line one.\nLine two.")
    await drain(worker)
    resp = await submit(client, "alice", "Line one.\r\nLine two.")
    assert resp.json()["cache_hit"] is True


async def test_cache_is_keyed_by_content_not_document(client: httpx.AsyncClient, worker, container: Container) -> None:
    doc_id = (await submit(client, "alice", X)).json()["document_id"]
    await drain(worker)
    assert container.cache.key(content_hash(normalize_content(X))).endswith(content_hash(X))
    assert doc_id not in container.cache.key(content_hash(X))

    # PATCH to new content must NOT reuse X's cached result.
    resp = await client.patch(f"/documents/{doc_id}", json={"content": Y}, headers=hdr("alice"))
    assert resp.json()["cache_hit"] is False and resp.json()["status"] == "queued"
    await drain(worker)
    doc = await get_doc(client, "alice", doc_id)
    assert doc["result"]["summary"].startswith("Vertical farms")


async def test_patch_to_previously_seen_content_completes_instantly(
    client: httpx.AsyncClient, worker, container: Container
) -> None:
    other = (await submit(client, "alice", Y)).json()["document_id"]
    doc_id = (await submit(client, "alice", X)).json()["document_id"]
    await drain(worker)

    resp = await client.patch(f"/documents/{doc_id}", json={"content": Y}, headers=hdr("alice"))
    assert resp.json() == {**resp.json(), "cache_hit": True, "status": "completed", "content_version": 2}
    doc = await get_doc(client, "alice", doc_id)
    assert doc["result"]["content_version"] == 2
    assert doc["result"]["summary"] == (await get_doc(client, "alice", other))["result"]["summary"]
    assert_consistent(await container.repo.coll.find_one({"user_id": "alice", "content_version": 2}))


async def test_mongo_tier_serves_after_redis_entry_expires(
    client: httpx.AsyncClient, worker, container: Container
) -> None:
    await submit(client, "alice", X)
    await drain(worker)
    await container.redis.delete(container.cache.key(content_hash(X)))  # simulate TTL expiry
    resp = await submit(client, "alice", X)
    assert resp.json()["cache_hit"] is True


@pytest.fixture
async def redis_down_app() -> FastAPI:
    settings = make_settings(redis_url="redis://127.0.0.1:1/0", redis_socket_timeout_s=0.2)
    application = create_app(settings)
    async with application.router.lifespan_context(application):
        yield application
        await application.state.container.mongo.drop_database(settings.mongo_db)


async def test_cache_degrades_to_mongodb_when_redis_is_down(redis_down_app: FastAPI) -> None:
    c = redis_down_app.state.container
    processor = MockProcessor(c.settings, ScriptedFailures(), sleeper=lambda _: asyncio.sleep(0))
    worker = PipelineWorker(c.repo, c.cache, c.limiter, processor, c.settings)
    transport = httpx.ASGITransport(app=redis_down_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        await submit(client, "alice", X)
        await drain(worker)  # completes despite Redis being unreachable
        resp = await submit(client, "alice", X)
        assert resp.json()["cache_hit"] is True
