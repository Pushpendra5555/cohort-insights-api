"""All MongoDB access for the ``documents`` collection.

Three write disciplines, each enforced by the update *filter* (so MongoDB's
single-document atomicity does the concurrency control, not application locks):

* **CAS writes** (API side): filter on ``rev``. Used for content replacement,
  metadata changes, and manual retry. A stale read can never overwrite.
* **Fenced writes** (worker side): filter on ``content_version`` AND
  ``pipeline.lease_token``. A worker whose lease was superseded -- by a PATCH or
  by another worker reclaiming an expired lease -- matches zero documents.
* **Claim**: ``find_one_and_update`` on ``pipeline.due_at <= now``. Exactly one
  worker can win a given due document.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from bson import ObjectId
from pymongo import ASCENDING, DESCENDING, IndexModel, ReturnDocument
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.asynchronous.database import AsyncDatabase

from app.domain.document import RawDoc
from app.domain.enums import ACTIVE_STATES, PipelineState

INDEXES: list[IndexModel] = [
    # GET /users/{id}/documents (no status filter), newest first, stable tiebreak on _id.
    IndexModel(
        [("user_id", ASCENDING), ("created_at", DESCENDING), ("_id", DESCENDING)],
        name="user_created",
    ),
    # GET /users/{id}/documents?status=..., and the Mongo fallback for the rate limiter.
    IndexModel(
        [
            ("user_id", ASCENDING),
            ("pipeline.state", ASCENDING),
            ("created_at", DESCENDING),
            ("_id", DESCENDING),
        ],
        name="user_state_created",
    ),
    # Crosswalk: GET /documents/by-ref/{ref}. Unique per owner; partial so documents
    # without a ref (field absent) are not indexed and never collide.
    IndexModel(
        [("user_id", ASCENDING), ("client_doc_ref", ASCENDING)],
        name="user_client_ref_unique",
        unique=True,
        partialFilterExpression={"client_doc_ref": {"$type": "string"}},
    ),
    # Durable tier of the content cache: completed result for a given content hash.
    IndexModel(
        [("content_hash", ASCENDING), ("pipeline.state", ASCENDING)],
        name="content_hash_state",
    ),
    # Work queue: only documents with pending work carry a due_at date, so this
    # partial index stays as small as the backlog, not the collection.
    IndexModel(
        [("pipeline.due_at", ASCENDING)],
        name="pipeline_due",
        partialFilterExpression={"pipeline.due_at": {"$type": "date"}},
    ),
]


class DocumentRepository:
    def __init__(self, db: AsyncDatabase) -> None:
        self.coll: AsyncCollection = db["documents"]

    async def ensure_indexes(self) -> None:
        await self.coll.create_indexes(INDEXES)

    # ------------------------------------------------------------------ reads

    @staticmethod
    def by_ref_filter(user_id: str, client_doc_ref: str) -> dict[str, Any]:
        # `$type: string` restates the partial-index predicate so the planner can
        # prove the index applies (verified by an explain() test: IXSCAN, never COLLSCAN).
        return {"user_id": user_id, "client_doc_ref": {"$eq": client_doc_ref, "$type": "string"}}

    async def get_owned(self, doc_id: ObjectId, user_id: str) -> RawDoc | None:
        # Ownership is part of the query itself: a non-owner gets exactly the same
        # "None" as a non-existent id -- no code path can leak existence.
        return await self.coll.find_one({"_id": doc_id, "user_id": user_id})

    async def get_by_ref(self, user_id: str, client_doc_ref: str) -> RawDoc | None:
        return await self.coll.find_one(self.by_ref_filter(user_id, client_doc_ref))

    @staticmethod
    def list_filter(user_id: str, status: PipelineState | None) -> dict[str, Any]:
        query: dict[str, Any] = {"user_id": user_id}
        if status is not None:
            query["pipeline.state"] = status.value
        return query

    async def list_for_user(
        self, user_id: str, status: PipelineState | None, skip: int, limit: int
    ) -> tuple[list[RawDoc], int]:
        query = self.list_filter(user_id, status)
        cursor = (
            self.coll.find(query)
            .sort([("created_at", DESCENDING), ("_id", DESCENDING)])
            .skip(skip)
            .limit(limit)
        )
        items = await cursor.to_list(length=limit)
        total = await self.coll.count_documents(query)
        return items, total

    async def find_completed_by_hash(self, chash: str, pipeline_version: str) -> RawDoc | None:
        return await self.coll.find_one(
            {
                "content_hash": chash,
                "pipeline.state": PipelineState.COMPLETED.value,
                "result.tags.pipeline_version": pipeline_version,
            }
        )

    async def count_active(self, user_id: str, exclude_id: ObjectId | None = None) -> int:
        query: dict[str, Any] = {
            "user_id": user_id,
            "pipeline.state": {"$in": [s.value for s in ACTIVE_STATES]},
        }
        if exclude_id is not None:
            query["_id"] = {"$ne": exclude_id}
        return await self.coll.count_documents(query)

    # ------------------------------------------------------------------ API-side writes

    async def insert(self, doc: RawDoc) -> None:
        await self.coll.insert_one(doc)

    async def cas_update(
        self, doc_id: ObjectId, user_id: str, expected_rev: int, update: dict[str, Any]
    ) -> RawDoc | None:
        """Apply `update` iff nobody has written since we read `expected_rev`."""
        update = dict(update)
        update.setdefault("$inc", {})
        update["$inc"] = {**update["$inc"], "rev": 1}
        return await self.coll.find_one_and_update(
            {"_id": doc_id, "user_id": user_id, "rev": expected_rev},
            update,
            return_document=ReturnDocument.AFTER,
        )

    # ------------------------------------------------------------------ worker-side writes

    async def claim_due(
        self, now: datetime, worker_id: str, token: str, lease_s: float
    ) -> RawDoc | None:
        """Atomically take the oldest due job. Pushing due_at forward IS the lease:
        if this worker dies, the job becomes due again when the lease expires."""
        return await self.coll.find_one_and_update(
            {"pipeline.due_at": {"$lte": now}},
            {
                "$set": {
                    "pipeline.due_at": now + timedelta(seconds=lease_s),
                    "pipeline.lease_token": token,
                    "pipeline.lease_owner": worker_id,
                },
                "$inc": {"rev": 1},
            },
            sort=[("pipeline.due_at", ASCENDING)],
            return_document=ReturnDocument.AFTER,
        )

    async def fenced_update(
        self,
        doc_id: ObjectId,
        content_version: int,
        lease_token: str,
        update: dict[str, Any],
        extra_filter: dict[str, Any] | None = None,
    ) -> bool:
        """Write iff this worker still holds the lease for this exact content version."""
        query: dict[str, Any] = {
            "_id": doc_id,
            "content_version": content_version,
            "pipeline.lease_token": lease_token,
        }
        if extra_filter:
            query.update(extra_filter)
        update = dict(update)
        update["$inc"] = {**update.get("$inc", {}), "rev": 1}
        result = await self.coll.update_one(query, update)
        return result.matched_count == 1
