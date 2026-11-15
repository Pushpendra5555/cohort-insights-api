"""Two-stage pipeline: stage visibility, failure attribution, partial progress,
retry with backoff, and worker races (double claim, lease expiry, fencing)."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import httpx
from bson import ObjectId

from app.bootstrap import Container
from app.domain.enums import Stage
from app.domain.processing import MockProcessor
from app.worker.pipeline import PipelineWorker
from tests.conftest import ScriptedFailures, drain, get_doc, hdr, make_settings, submit, wait_for
from tests.invariants import assert_consistent

CONTENT = "Wind capacity doubled in a decade. Offshore projects led the growth."


async def test_stage_progression_is_visible(client: httpx.AsyncClient, worker) -> None:
    doc_id = (await submit(client, "alice", CONTENT)).json()["document_id"]
    assert (await get_doc(client, "alice", doc_id))["status"] == "queued"

    await worker.run_once()  # stage 1
    doc = await get_doc(client, "alice", doc_id)
    assert doc["status"] == "enriching"
    assert doc["stages"]["processing"]["status"] == "succeeded"
    assert doc["stages"]["enriching"]["status"] == "pending"
    assert doc["result"] is None  # summary alone is never exposed as a result

    await worker.run_once()  # stage 2
    assert (await get_doc(client, "alice", doc_id))["status"] == "completed"


async def test_enriching_failure_is_attributed_and_preserves_summary(
    client: httpx.AsyncClient, make_worker, container: Container, settings
) -> None:
    always_fail_enriching = {(Stage.ENRICHING, n) for n in range(1, settings.stage_max_attempts + 1)}
    worker = make_worker(failures=always_fail_enriching)
    doc_id = (await submit(client, "alice", CONTENT)).json()["document_id"]

    await drain(worker)
    doc = await get_doc(client, "alice", doc_id)
    assert doc["status"] == "failed"
    assert doc["failed_stage"] == "enriching"
    assert doc["stages"]["processing"] == {**doc["stages"]["processing"], "status": "succeeded", "attempts": 1}
    assert doc["stages"]["enriching"]["status"] == "failed"
    assert doc["stages"]["enriching"]["attempts"] == settings.stage_max_attempts
    assert "enriching" in doc["stages"]["enriching"]["last_error"]

    raw = await container.repo.coll.find_one({"_id": ObjectId(doc_id)})
    assert raw["result"]["summary"]["content_version"] == 1  # partial progress kept
    assert_consistent(raw)


async def test_manual_retry_resumes_at_enriching_without_redoing_processing(
    client: httpx.AsyncClient, make_worker, settings
) -> None:
    failing = make_worker(failures={(Stage.ENRICHING, n) for n in range(1, settings.stage_max_attempts + 1)})
    doc_id = (await submit(client, "alice", CONTENT)).json()["document_id"]
    await drain(failing)

    resp = await client.post(f"/documents/{doc_id}/retry", headers=hdr("alice"))
    assert resp.status_code == 202
    assert resp.json()["status"] == "enriching"

    healthy = make_worker()
    assert await drain(healthy) == 1  # one execution: enriching only
    doc = await get_doc(client, "alice", doc_id)
    assert doc["status"] == "completed"
    assert doc["stages"]["processing"]["attempts"] == 1  # never re-run


async def test_retry_of_non_failed_document_is_409(client: httpx.AsyncClient) -> None:
    doc_id = (await submit(client, "alice", CONTENT)).json()["document_id"]
    resp = await client.post(f"/documents/{doc_id}/retry", headers=hdr("alice"))
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "invalid_state"


async def test_processing_failure_is_attributed(client: httpx.AsyncClient, make_worker, settings) -> None:
    worker = make_worker(failures={(Stage.PROCESSING, n) for n in range(1, settings.stage_max_attempts + 1)})
    doc_id = (await submit(client, "alice", CONTENT)).json()["document_id"]
    await drain(worker)
    doc = await get_doc(client, "alice", doc_id)
    assert doc["status"] == "failed" and doc["failed_stage"] == "processing"
    assert doc["stages"]["enriching"]["status"] == "pending"


async def test_transient_failure_retries_with_backoff(client: httpx.AsyncClient, make_worker) -> None:
    worker = make_worker(failures={(Stage.ENRICHING, 1)})
    doc_id = (await submit(client, "alice", CONTENT)).json()["document_id"]
    await drain(worker)
    doc = await get_doc(client, "alice", doc_id)
    assert doc["status"] == "completed"
    assert doc["stages"]["enriching"]["attempts"] == 2
    assert doc["stages"]["processing"]["attempts"] == 1


async def test_backoff_schedules_future_due_time(client: httpx.AsyncClient, container: Container, settings) -> None:
    slow_retry = make_settings(
        retry_base_delay_s=10,
        retry_max_delay_s=10,
        mongo_db=settings.mongo_db,
        redis_namespace=settings.redis_namespace,
    )
    processor = MockProcessor(
        slow_retry, ScriptedFailures({(Stage.PROCESSING, 1)}), sleeper=lambda _: asyncio.sleep(0)
    )
    worker = PipelineWorker(container.repo, container.cache, container.limiter, processor, slow_retry)
    doc_id = (await submit(client, "alice", CONTENT)).json()["document_id"]
    await worker.run_once()

    doc = await get_doc(client, "alice", doc_id)
    stage = doc["stages"]["processing"]
    assert stage["status"] == "retry_scheduled"
    next_at = datetime.fromisoformat(stage["next_attempt_at"])
    assert next_at > datetime.now(timezone.utc) + timedelta(seconds=4)  # equal jitter: >= half of 10s
    assert await worker.run_once() is False  # not due yet


async def test_two_workers_never_claim_the_same_job(
    client: httpx.AsyncClient, container: Container, settings
) -> None:
    await submit(client, "alice", CONTENT)
    now = datetime.now(timezone.utc)
    claims = await asyncio.gather(
        *(container.repo.claim_due(now, f"w{i}", f"token-{i}", settings.lease_duration_s) for i in range(10))
    )
    assert sum(c is not None for c in claims) == 1


async def test_expired_lease_is_reclaimed_and_stale_worker_is_fenced(
    client: httpx.AsyncClient, container: Container, make_worker, settings
) -> None:
    """Worker A claims and stalls past its lease; B reclaims and commits; A's commit is discarded."""
    doc_id = (await submit(client, "alice", CONTENT)).json()["document_id"]
    oid = ObjectId(doc_id)

    release_a = asyncio.Event()

    async def stalled_sleep(_: float) -> None:
        await release_a.wait()

    worker_a = make_worker(sleeper=stalled_sleep, worker_id="A")
    task_a = asyncio.create_task(worker_a.run_once())

    async def a_running() -> bool:
        raw = await container.repo.coll.find_one({"_id": oid})
        return raw["pipeline"]["stages"]["processing"]["status"] == "running"

    await wait_for(a_running)
    # Simulate lease expiry (A is "hung").
    await container.repo.coll.update_one({"_id": oid}, {"$set": {"pipeline.due_at": datetime.now(timezone.utc)}})

    worker_b = make_worker(worker_id="B")
    await drain(worker_b)  # B redoes stage 1 (attempt 2) and stage 2
    release_a.set()
    await task_a  # A's commit must match nothing

    raw = await container.repo.coll.find_one({"_id": oid})
    assert raw["pipeline"]["state"] == "completed"
    assert raw["pipeline"]["stages"]["processing"]["attempts"] == 2
    assert_consistent(raw)


async def test_crash_looping_job_eventually_fails(
    client: httpx.AsyncClient, container: Container, worker, settings
) -> None:
    doc_id = (await submit(client, "alice", CONTENT)).json()["document_id"]
    oid = ObjectId(doc_id)
    await container.repo.coll.update_one(
        {"_id": oid}, {"$set": {"pipeline.stages.processing.attempts": settings.stage_max_attempts}}
    )
    await worker.run_once()
    doc = await get_doc(client, "alice", doc_id)
    assert doc["status"] == "failed" and doc["failed_stage"] == "processing"
    assert "lease" in doc["stages"]["processing"]["last_error"]
