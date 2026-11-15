"""HTTP contract: status codes, validation, ownership, pagination."""

from __future__ import annotations

import httpx
import pytest
from bson import ObjectId

from tests.conftest import drain, get_doc, hdr, submit


async def test_submit_returns_201_with_id_and_queued_status(client: httpx.AsyncClient) -> None:
    resp = await submit(client, "alice", "Solar adoption grew quickly. Costs fell sharply.")
    assert resp.status_code == 201
    body = resp.json()
    assert ObjectId.is_valid(body["document_id"])
    assert body["status"] == "queued"
    assert body["content_version"] == 1
    assert body["outcome"] == "created"
    assert resp.headers["Location"] == f"/documents/{body['document_id']}"


@pytest.mark.parametrize(
    "payload",
    [
        {"title": "t", "content": "x"},  # missing user_id
        {"user_id": "alice", "title": "t", "content": "   \n\t"},  # blank content
        {"user_id": "alice", "title": "   ", "content": "x"},  # blank title
        {"user_id": "alice", "title": "t", "content": "x" * 100_001},  # too long
        {"user_id": "alice", "title": "t", "content": "x", "client_doc_ref": "has space"},
        {"user_id": "alice", "title": "t", "content": "x", "client_version": -1},
        {"user_id": "alice", "title": "t", "content": "x", "unexpected": 1},  # extra field
    ],
)
async def test_submit_validation_errors_are_422(client: httpx.AsyncClient, payload: dict) -> None:
    resp = await client.post("/documents", json=payload, headers=hdr("alice"))
    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "validation_error"


async def test_missing_identity_is_401(client: httpx.AsyncClient) -> None:
    resp = await client.post("/documents", json={"user_id": "alice", "title": "t", "content": "x"})
    assert resp.status_code == 401


async def test_submit_on_behalf_of_another_user_is_403(client: httpx.AsyncClient) -> None:
    resp = await client.post(
        "/documents", json={"user_id": "bob", "title": "t", "content": "x"}, headers=hdr("alice")
    )
    assert resp.status_code == 403


async def test_non_owner_gets_identical_404_to_nonexistent(client: httpx.AsyncClient) -> None:
    doc_id = (await submit(client, "alice", "Private notes.")).json()["document_id"]

    as_bob = await client.get(f"/documents/{doc_id}", headers=hdr("bob"))
    missing = await client.get(f"/documents/{ObjectId()}", headers=hdr("bob"))
    malformed = await client.get("/documents/not-an-id", headers=hdr("bob"))

    assert as_bob.status_code == missing.status_code == malformed.status_code == 404
    assert as_bob.json()["error"] == missing.json()["error"] == malformed.json()["error"]


async def test_non_owner_cannot_patch_or_retry(client: httpx.AsyncClient) -> None:
    doc_id = (await submit(client, "alice", "Private notes.")).json()["document_id"]
    patch = await client.patch(f"/documents/{doc_id}", json={"content": "hijack"}, headers=hdr("bob"))
    retry = await client.post(f"/documents/{doc_id}/retry", headers=hdr("bob"))
    assert patch.status_code == 404
    assert retry.status_code == 404


async def test_list_is_scoped_newest_first_and_paginated(client: httpx.AsyncClient, worker) -> None:
    ids = []
    for i in range(5):
        ids.append((await submit(client, "alice", f"Document number {i}.")).json()["document_id"])
        if i == 1:
            await drain(worker)  # complete the first two to exercise the status filter
    await submit(client, "bob", "Bob's document.")

    page1 = (await client.get("/users/alice/documents?page=1&page_size=2", headers=hdr("alice"))).json()
    page3 = (await client.get("/users/alice/documents?page=3&page_size=2", headers=hdr("alice"))).json()

    assert page1["total"] == 5 and page1["has_next"] is True
    assert [d["document_id"] for d in page1["items"]] == [ids[4], ids[3]]
    assert [d["document_id"] for d in page3["items"]] == [ids[0]] and page3["has_next"] is False
    assert all(d["user_id"] == "alice" for d in page1["items"])

    completed = (await client.get("/users/alice/documents?status=completed", headers=hdr("alice"))).json()
    assert {d["document_id"] for d in completed["items"]} == {ids[0], ids[1]}


async def test_list_other_users_documents_is_404(client: httpx.AsyncClient) -> None:
    await submit(client, "alice", "Alice's document.")
    resp = await client.get("/users/alice/documents", headers=hdr("bob"))
    assert resp.status_code == 404


async def test_list_rejects_invalid_status_and_page(client: httpx.AsyncClient) -> None:
    assert (await client.get("/users/alice/documents?status=bogus", headers=hdr("alice"))).status_code == 422
    assert (await client.get("/users/alice/documents?page=0", headers=hdr("alice"))).status_code == 422


async def test_page_size_is_clamped_to_max(client: httpx.AsyncClient, settings) -> None:
    resp = await client.get("/users/alice/documents?page_size=100000", headers=hdr("alice"))
    assert resp.json()["page_size"] == settings.max_page_size


async def test_happy_path_end_to_end(client: httpx.AsyncClient, worker) -> None:
    doc_id = (await submit(client, "alice", "Battery storage is scaling. Grid operators are adapting.")).json()[
        "document_id"
    ]
    assert await drain(worker) == 2  # exactly one execution per stage

    doc = await get_doc(client, "alice", doc_id)
    assert doc["status"] == "completed"
    assert doc["result_freshness"] == "current"
    assert doc["result"]["content_version"] == doc["content_version"] == 1
    assert doc["result"]["summary"].startswith("Battery storage is scaling.")
    assert "battery" in doc["result"]["tags"]
    assert doc["stages"]["processing"]["status"] == doc["stages"]["enriching"]["status"] == "succeeded"
