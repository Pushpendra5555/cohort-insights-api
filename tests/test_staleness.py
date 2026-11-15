"""Staleness on update: the core guarantee.

1. A PATCH mid-run never lets the old summary be presented as current.
2. Old-version stage commits are discarded (fencing), so mixed pairs cannot be written.
3. The previous result stays available, explicitly labelled with its version.
4. Optimistic concurrency for racing PATCHes has a defined, tested outcome.
5. A concurrent stress test checks the storage invariants continuously.
"""

from __future__ import annotations

import asyncio
import random

import httpx
from bson import ObjectId

from app.bootstrap import Container
from app.domain.enums import Stage
from tests.conftest import drain, get_doc, hdr, submit, wait_for
from tests.invariants import assert_consistent

V1 = "Coral reefs are bleaching. Ocean temperatures keep rising."
V2 = "Urban gardens improve air quality. Cities are funding rooftop farms."
V3 = "Rail freight is cheaper per tonne. Electrification cuts emissions further."


async def _raw(container: Container, doc_id: str) -> dict:
    return await container.repo.coll.find_one({"_id": ObjectId(doc_id)})


async def test_patch_between_stages_discards_old_summary(
    client: httpx.AsyncClient, worker, container: Container
) -> None:
    doc_id = (await submit(client, "alice", V1)).json()["document_id"]
    await worker.run_once()  # v1 summary committed, now enriching

    resp = await client.patch(f"/documents/{doc_id}", json={"content": V2}, headers=hdr("alice"))
    assert resp.status_code == 200
    assert resp.json() == {**resp.json(), "outcome": "new_version", "content_version": 2, "status": "queued"}

    doc = await get_doc(client, "alice", doc_id)
    assert doc["result"] is None and doc["result_freshness"] == "none"
    raw = await _raw(container, doc_id)
    assert raw["result"] == {"summary": None, "tags": None}  # v1 summary cannot pair with v2 tags
    assert_consistent(raw)

    await drain(worker)
    doc = await get_doc(client, "alice", doc_id)
    assert doc["status"] == "completed"
    assert doc["result"]["content_version"] == 2
    assert doc["result"]["summary"].startswith("Urban gardens")
    assert_consistent(await _raw(container, doc_id))


async def test_patch_racing_in_flight_stage_fences_out_old_commit(
    client: httpx.AsyncClient, make_worker, container: Container
) -> None:
    """Worker is mid-`processing` on v1 when PATCH lands. Its v1 summary must never be stored."""
    doc_id = (await submit(client, "alice", V1)).json()["document_id"]
    gate = asyncio.Event()

    async def held(_: float) -> None:
        await gate.wait()

    in_flight = asyncio.create_task(make_worker(sleeper=held, worker_id="slow").run_once())

    async def running() -> bool:
        return (await _raw(container, doc_id))["pipeline"]["stages"]["processing"]["status"] == "running"

    await wait_for(running)
    assert (await get_doc(client, "alice", doc_id))["status"] == "processing"

    await client.patch(f"/documents/{doc_id}", json={"content": V2}, headers=hdr("alice"))
    gate.set()
    await in_flight  # finishes summarising V1 and attempts its commit

    raw = await _raw(container, doc_id)
    assert raw["content_version"] == 2
    assert raw["result"]["summary"] is None, "v1 summary leaked into v2 document"
    assert raw["pipeline"]["state"] == "queued"
    assert_consistent(raw)


async def test_old_version_tags_commit_is_rejected(
    client: httpx.AsyncClient, make_worker, container: Container, worker
) -> None:
    """Worker is mid-`enriching` on v1 when PATCH lands: v1 tags must not attach to anything."""
    doc_id = (await submit(client, "alice", V1)).json()["document_id"]
    await worker.run_once()  # stage 1 for v1

    gate = asyncio.Event()

    async def held(_: float) -> None:
        await gate.wait()

    in_flight = asyncio.create_task(make_worker(sleeper=held, worker_id="slow").run_once())

    async def enriching_running() -> bool:
        return (await _raw(container, doc_id))["pipeline"]["stages"]["enriching"]["status"] == "running"

    await wait_for(enriching_running)
    await client.patch(f"/documents/{doc_id}", json={"content": V2}, headers=hdr("alice"))
    await worker.run_once()  # v2 stage 1 commits a v2 summary BEFORE the stale v1 tags arrive
    gate.set()
    await in_flight

    raw = await _raw(container, doc_id)
    assert raw["result"]["summary"]["content_version"] == 2
    assert raw["result"]["tags"] is None, "v1 tags attached to v2 summary"
    assert_consistent(raw)


async def test_completed_then_patched_exposes_labelled_stale_result(
    client: httpx.AsyncClient, worker
) -> None:
    doc_id = (await submit(client, "alice", V1)).json()["document_id"]
    await drain(worker)
    await client.patch(f"/documents/{doc_id}", json={"content": V2}, headers=hdr("alice"))

    doc = await get_doc(client, "alice", doc_id)
    assert doc["content_version"] == 2
    assert doc["result"] is None
    assert doc["result_freshness"] == "stale"
    assert doc["stale_result"]["content_version"] == 1  # explicitly older -- comparable, not a bare flag
    assert doc["stale_result"]["content_hash"] != doc["content_hash"]
    assert doc["stale_result"]["summary"].startswith("Coral reefs")

    await drain(worker)
    doc = await get_doc(client, "alice", doc_id)
    assert doc["result_freshness"] == "current" and doc["stale_result"] is None


async def test_patch_with_identical_content_is_a_noop(client: httpx.AsyncClient, worker) -> None:
    doc_id = (await submit(client, "alice", V1)).json()["document_id"]
    await drain(worker)
    resp = await client.patch(f"/documents/{doc_id}", json={"content": V1}, headers=hdr("alice"))
    assert resp.json()["outcome"] == "unchanged" and resp.json()["content_version"] == 1
    assert (await get_doc(client, "alice", doc_id))["status"] == "completed"


async def test_racing_patches_with_same_expected_version_one_wins(
    client: httpx.AsyncClient, container: Container
) -> None:
    doc_id = (await submit(client, "alice", V1)).json()["document_id"]
    responses = await asyncio.gather(
        *(
            client.patch(
                f"/documents/{doc_id}",
                json={"content": content, "expected_content_version": 1},
                headers=hdr("alice"),
            )
            for content in (V2, V3)
        )
    )
    codes = sorted(r.status_code for r in responses)
    assert codes == [200, 409]
    loser = next(r for r in responses if r.status_code == 409)
    assert loser.json()["error"]["code"] == "version_conflict"
    assert loser.json()["error"]["details"]["current_content_version"] == 2

    raw = await _raw(container, doc_id)
    winner = next(r for r in responses if r.status_code == 200)
    assert raw["content_version"] == 2 == winner.json()["content_version"]
    assert_consistent(raw)


async def test_racing_patches_without_precondition_both_apply_in_order(
    client: httpx.AsyncClient, container: Container
) -> None:
    doc_id = (await submit(client, "alice", V1)).json()["document_id"]
    contents = (V2, V3)
    responses = await asyncio.gather(
        *(client.patch(f"/documents/{doc_id}", json={"content": c}, headers=hdr("alice")) for c in contents)
    )
    assert all(r.status_code == 200 for r in responses)
    assert sorted(r.json()["content_version"] for r in responses) == [2, 3]

    # The CAS loop serialised the writes. The request that committed second got
    # content_version 3, and its content is what is stored: last committed writer wins.
    committed_last = contents[[r.json()["content_version"] for r in responses].index(3)]
    raw = await _raw(container, doc_id)
    assert raw["content_version"] == 3
    assert raw["content"] == committed_last
    assert_consistent(raw)


async def test_concurrent_stress_invariants_hold_at_every_observation(
    client: httpx.AsyncClient, make_worker, container: Container
) -> None:
    """3 workers (random latency, ~30% stage failure) x random PATCHes x a reader
    sampling raw documents continuously. Every sample must satisfy the invariants;
    the final result must match the final content exactly."""
    rng = random.Random(42)
    contents = [f"Topic {i} matters a great deal. Detail number {i} follows here." for i in range(12)]
    doc_ids = [(await submit(client, "stress", contents[i])).json()["document_id"] for i in range(3)]

    async def jitter(_: float) -> None:
        await asyncio.sleep(rng.uniform(0, 0.004))

    flaky = {(s, a) for s in Stage for a in (1,) if rng.random() < 0.6}
    workers = [make_worker(failures=flaky, sleeper=jitter, worker_id=f"w{i}") for i in range(3)]
    stop = asyncio.Event()
    observations = 0

    async def worker_loop(w) -> None:
        while not stop.is_set():
            if not await w.run_once():
                await asyncio.sleep(0.002)

    async def reader() -> None:
        nonlocal observations
        while not stop.is_set():
            async for raw in container.repo.coll.find({"user_id": "stress"}):
                assert_consistent(raw)
                observations += 1
            for doc_id in doc_ids:  # the API view must agree with the stored version
                view = await get_doc(client, "stress", doc_id)
                if view["result"] is not None:
                    assert view["result"]["content_version"] == view["content_version"]
            await asyncio.sleep(0)

    async def patcher() -> None:
        for _ in range(30):
            doc_id = rng.choice(doc_ids)
            resp = await client.patch(
                f"/documents/{doc_id}", json={"content": rng.choice(contents)}, headers=hdr("stress")
            )
            assert resp.status_code in (200, 409, 429), resp.text
            await asyncio.sleep(rng.uniform(0, 0.01))

    tasks = [asyncio.create_task(worker_loop(w)) for w in workers]
    reader_task = asyncio.create_task(reader())
    await patcher()

    async def settled() -> bool:
        states = {d["pipeline"]["state"] async for d in container.repo.coll.find({"user_id": "stress"})}
        return states <= {"completed", "failed"}

    await wait_for(settled, timeout=20)
    stop.set()
    await asyncio.gather(*tasks, reader_task)

    assert observations > 50
    async for raw in container.repo.coll.find({"user_id": "stress"}):
        assert_consistent(raw)
        if raw["pipeline"]["state"] == "completed":
            assert raw["result"]["summary"]["content_hash"] == raw["content_hash"]
