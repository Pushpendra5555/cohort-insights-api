"""Crosswalk (client_doc_ref): lookup, repeat-submission policy, ordering, isolation, index use."""

from __future__ import annotations

import asyncio
import json

import httpx

from app.bootstrap import Container
from app.domain.enums import PipelineState
from tests.conftest import drain, get_doc, hdr, submit

A = "Glaciers are retreating. Meltwater feeds alpine rivers."
B = "Desalination costs are dropping. Coastal cities are investing."
C = "Heat pumps outsell gas boilers. Subsidies accelerated adoption."


async def test_lookup_by_ref_and_404_when_unknown(client: httpx.AsyncClient) -> None:
    doc_id = (await submit(client, "alice", A, client_doc_ref="cms-1")).json()["document_id"]
    found = await client.get("/documents/by-ref/cms-1", headers=hdr("alice"))
    assert found.status_code == 200 and found.json()["document_id"] == doc_id
    assert (await client.get("/documents/by-ref/cms-nope", headers=hdr("alice"))).status_code == 404


async def test_repeat_same_content_is_idempotent_replay(client: httpx.AsyncClient, container: Container) -> None:
    first = await submit(client, "alice", A, client_doc_ref="cms-1")
    again = await submit(client, "alice", A, client_doc_ref="cms-1")
    assert first.status_code == 201 and again.status_code == 200
    assert again.json()["document_id"] == first.json()["document_id"]
    assert again.json()["outcome"] == "unchanged" and again.json()["content_version"] == 1
    assert await container.repo.coll.count_documents({"client_doc_ref": "cms-1"}) == 1


async def test_repeat_different_content_becomes_new_version_of_same_document(
    client: httpx.AsyncClient, worker
) -> None:
    """Bonus: the 'repeat ref, different content' case, tested end to end."""
    doc_id = (await submit(client, "alice", A, client_doc_ref="cms-1")).json()["document_id"]
    await drain(worker)

    resp = await submit(client, "alice", B, client_doc_ref="cms-1")
    assert resp.status_code == 200
    assert resp.json()["document_id"] == doc_id
    assert resp.json()["outcome"] == "new_version" and resp.json()["content_version"] == 2

    by_ref = (await client.get("/documents/by-ref/cms-1", headers=hdr("alice"))).json()
    assert by_ref["result_freshness"] == "stale" and by_ref["stale_result"]["content_version"] == 1

    await drain(worker)
    doc = await get_doc(client, "alice", doc_id)
    assert doc["result"]["content_version"] == 2 and doc["result"]["summary"].startswith("Desalination")


async def test_out_of_order_versioned_resubmission_is_rejected(client: httpx.AsyncClient) -> None:
    await submit(client, "alice", A, client_doc_ref="cms-1", client_version=1)
    newer = await submit(client, "alice", C, client_doc_ref="cms-1", client_version=3)
    late = await submit(client, "alice", B, client_doc_ref="cms-1", client_version=2)  # arrives after v3

    assert newer.status_code == 200 and newer.json()["outcome"] == "new_version"
    assert late.status_code == 409 and late.json()["error"]["code"] == "stale_client_version"
    doc = (await client.get("/documents/by-ref/cms-1", headers=hdr("alice"))).json()
    assert doc["client_version"] == 3 and doc["content_version"] == 2  # v3 content stands


async def test_versioned_replay_is_idempotent_but_conflicting_payload_is_not(client: httpx.AsyncClient) -> None:
    await submit(client, "alice", A, client_doc_ref="cms-1", client_version=5)
    replay = await submit(client, "alice", A, client_doc_ref="cms-1", client_version=5)
    conflicting = await submit(client, "alice", B, client_doc_ref="cms-1", client_version=5)
    assert replay.status_code == 200 and replay.json()["outcome"] == "unchanged"
    assert conflicting.status_code == 409 and conflicting.json()["error"]["code"] == "stale_client_version"


async def test_versioned_ref_requires_version_on_resubmission(client: httpx.AsyncClient) -> None:
    await submit(client, "alice", A, client_doc_ref="cms-1", client_version=1)
    unversioned = await submit(client, "alice", B, client_doc_ref="cms-1")
    assert unversioned.status_code == 409
    assert unversioned.json()["error"]["code"] == "client_version_required"


async def test_unversioned_refs_follow_arrival_order(client: httpx.AsyncClient) -> None:
    await submit(client, "alice", A, client_doc_ref="cms-1")
    await submit(client, "alice", B, client_doc_ref="cms-1")
    await submit(client, "alice", C, client_doc_ref="cms-1")
    doc = (await client.get("/documents/by-ref/cms-1", headers=hdr("alice"))).json()
    assert doc["content_version"] == 3


async def test_refs_are_isolated_per_owner(client: httpx.AsyncClient) -> None:
    alice = (await submit(client, "alice", A, client_doc_ref="shared-ref")).json()["document_id"]
    bob = (await submit(client, "bob", B, client_doc_ref="shared-ref")).json()["document_id"]
    assert alice != bob
    assert (await client.get("/documents/by-ref/shared-ref", headers=hdr("alice"))).json()["document_id"] == alice
    assert (await client.get("/documents/by-ref/shared-ref", headers=hdr("carol"))).status_code == 404


async def test_concurrent_first_submissions_create_exactly_one_document(
    client: httpx.AsyncClient, container: Container
) -> None:
    responses = await asyncio.gather(*(submit(client, "alice", A, client_doc_ref="race-1") for _ in range(5)))
    assert all(r.status_code in (200, 201) for r in responses)
    assert sorted(r.status_code for r in responses).count(201) == 1
    assert len({r.json()["document_id"] for r in responses}) == 1
    assert await container.repo.coll.count_documents({"client_doc_ref": "race-1"}) == 1


async def test_many_documents_without_ref_do_not_collide(client: httpx.AsyncClient, worker) -> None:
    for i in range(3):
        assert (await submit(client, "alice", f"No ref {i}.")).status_code == 201
    await drain(worker)
    assert (await submit(client, "alice", "No ref 4.")).status_code == 201


def _winning_plan(plan: dict) -> str:
    """Serialised winning plan only (rejected candidate plans may legitimately scan)."""
    planner = plan.get("queryPlanner", plan)
    return json.dumps(planner["winningPlan"], default=str)


async def test_by_ref_lookup_uses_index_not_collection_scan(client: httpx.AsyncClient, container: Container) -> None:
    for i in range(3):
        await submit(client, "alice", f"Doc {i}.", client_doc_ref=f"ref-{i}")
    plan = await container.repo.coll.find(container.repo.by_ref_filter("alice", "ref-1")).explain()
    text = _winning_plan(plan)
    assert "IXSCAN" in text and "user_client_ref_unique" in text
    assert "COLLSCAN" not in text


async def test_listing_uses_compound_index_without_in_memory_sort(
    client: httpx.AsyncClient, container: Container, worker
) -> None:
    for i in range(3):
        await submit(client, "alice", f"Listing {i}.")
    await drain(worker)
    expected = {None: "user_created", PipelineState.COMPLETED: "user_state_created"}
    for status, index_name in expected.items():
        cursor = container.repo.coll.find(container.repo.list_filter("alice", status)).sort(
            [("created_at", -1), ("_id", -1)]
        )
        text = _winning_plan(await cursor.explain())
        assert "IXSCAN" in text and index_name in text
        assert "COLLSCAN" not in text
        assert '"stage": "SORT"' not in text  # order comes from the index, not memory
