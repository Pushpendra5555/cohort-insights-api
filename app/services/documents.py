"""Business logic for documents. Framework-agnostic: raises domain errors only."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timezone

from bson import ObjectId
from pymongo.errors import DuplicateKeyError

from app.core.config import Settings
from app.core.errors import (
    ClientVersionRequired,
    ConcurrentModification,
    InvalidState,
    NotFound,
    RateLimited,
    StaleClientVersion,
    VersionConflict,
)
from app.domain.document import (
    RawDoc,
    is_active,
    new_document,
    replacement_update,
    slot_base_for,
    slot_member,
    stage_doc,
)
from app.domain.enums import PipelineState, Stage, StageStatus, SubmitOutcome
from app.domain.hashing import content_hash, normalize_content
from app.repositories.documents import DocumentRepository
from app.services.cache import ResultCache
from app.services.rate_limiter import ActiveJobLimiter

logger = logging.getLogger(__name__)

Clock = Callable[[], datetime]


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True)
class SubmitResult:
    doc: RawDoc
    outcome: SubmitOutcome
    cache_hit: bool


def parse_object_id(raw: str) -> ObjectId:
    """Malformed ids are 404, not 422: such a document cannot exist, and the
    response must not differ from any other unknown id."""
    if not ObjectId.is_valid(raw):
        raise NotFound("document not found")
    return ObjectId(raw)


class DocumentService:
    MAX_CAS_ATTEMPTS = 5

    def __init__(
        self,
        repo: DocumentRepository,
        cache: ResultCache,
        limiter: ActiveJobLimiter,
        settings: Settings,
        clock: Clock = utcnow,
    ) -> None:
        self._repo = repo
        self._cache = cache
        self._limiter = limiter
        self._s = settings
        self._now = clock

    # ================================================================== reads

    async def get(self, raw_id: str, caller: str) -> RawDoc:
        doc = await self._repo.get_owned(parse_object_id(raw_id), caller)
        if doc is None:
            raise NotFound("document not found")
        return doc

    async def get_by_ref(self, client_doc_ref: str, caller: str) -> RawDoc:
        doc = await self._repo.get_by_ref(caller, client_doc_ref)
        if doc is None:
            raise NotFound("no document for this client_doc_ref")
        return doc

    async def list_for_user(
        self, user_id: str, status: PipelineState | None, page: int, page_size: int
    ) -> tuple[list[RawDoc], int]:
        return await self._repo.list_for_user(user_id, status, (page - 1) * page_size, page_size)

    # ================================================================== submit

    async def submit(
        self,
        *,
        user_id: str,
        title: str,
        content: str,
        client_doc_ref: str | None,
        client_version: int | None,
    ) -> SubmitResult:
        normalized = normalize_content(content)
        chash = content_hash(normalized)

        if client_doc_ref is None:
            return await self._create(user_id, title, normalized, chash, None, client_version)

        # Crosswalk path. Two attempts cover the race where two first-submissions of
        # the same ref arrive together: the unique index lets exactly one insert win;
        # the loser re-reads and is handled as a repeat submission.
        for _ in range(2):
            if await self._repo.get_by_ref(user_id, client_doc_ref) is not None:
                return await self._resubmit_ref(
                    user_id, client_doc_ref, title, normalized, chash, client_version
                )
            try:
                return await self._create(
                    user_id, title, normalized, chash, client_doc_ref, client_version
                )
            except DuplicateKeyError:
                logger.info(
                    "crosswalk: lost first-submission race; treating as repeat",
                    extra={"user_id": user_id, "client_doc_ref": client_doc_ref},
                )
            except RateLimited:
                # Concurrent duplicates of a brand-new ref each try to take a slot. If a
                # sibling already created the document, this is a repeat, not new work.
                if await self._repo.get_by_ref(user_id, client_doc_ref) is None:
                    raise
        raise ConcurrentModification("could not resolve concurrent submissions of this ref")

    async def _create(
        self,
        user_id: str,
        title: str,
        content: str,
        chash: str,
        client_doc_ref: str | None,
        client_version: int | None,
    ) -> SubmitResult:
        doc_id = ObjectId()
        cached = await self._cache.get(chash)
        member = slot_member(slot_base_for(doc_id, user_id, client_doc_ref), 1)
        if cached is None:
            # Cache hits never enter the pipeline, so they consume no capacity
            # and are deliberately not rate limited.
            await self._limiter.acquire(user_id, doc_id, member)
        doc = new_document(
            doc_id=doc_id,
            user_id=user_id,
            title=title,
            content=content,
            chash=chash,
            client_doc_ref=client_doc_ref,
            client_version=client_version,
            now=self._now(),
            cached=cached.as_tuple() if cached else None,
            pipeline_version=self._s.pipeline_version,
        )
        try:
            await self._repo.insert(doc)
        except DuplicateKeyError:
            if cached is None and client_doc_ref is not None:
                await self._release_if_unused(user_id, client_doc_ref, member)
            raise
        except Exception:
            if cached is None:
                await self._limiter.release(user_id, member)
            raise
        logger.info(
            "document created",
            extra={"document_id": str(doc_id), "user_id": user_id, "cache_hit": cached is not None},
        )
        return SubmitResult(doc=doc, outcome=SubmitOutcome.CREATED, cache_hit=cached is not None)

    async def _release_if_unused(self, user_id: str, client_doc_ref: str, member: str) -> None:
        """A duplicate first-submission lost the insert race. Its slot member is shared
        with the winner's document (same slot_base), so release it only if the winner
        is no longer active at version 1 -- otherwise we would free a live slot."""
        winner = await self._repo.get_by_ref(user_id, client_doc_ref)
        if winner is not None and is_active(winner) and winner["content_version"] == 1:
            return
        await self._limiter.release(user_id, member)

    async def _resubmit_ref(
        self,
        user_id: str,
        ref: str,
        title: str,
        content: str,
        chash: str,
        client_version: int | None,
    ) -> SubmitResult:
        """Repeat submission of a known ref => a new version of the SAME document.

        Ordering rules (see README "Crosswalk"):
        * same content as current               -> idempotent replay, nothing reprocessed
        * client_version <  stored              -> 409 stale_client_version (out-of-order)
        * client_version == stored, new content -> 409 (same version, conflicting payload)
        * stored has a version, request has none -> 409 client_version_required
        * otherwise                              -> new content_version, reprocess
        """

        def guard(doc: RawDoc) -> None:
            stored = doc.get("client_version")
            if stored is None:
                return  # unversioned ref: arrival order (last writer wins)
            if client_version is None:
                raise ClientVersionRequired(
                    "this client_doc_ref is version-ordered; client_version is required",
                    details={"current_client_version": stored},
                )
            same_content = doc["content_hash"] == chash
            if client_version < stored or (client_version == stored and not same_content):
                raise StaleClientVersion(
                    "client_version is not newer than the applied version",
                    details={"current_client_version": stored, "received": client_version},
                )

        return await self._apply_content(
            load=lambda: self._repo.get_by_ref(user_id, ref),
            user_id=user_id,
            content=content,
            chash=chash,
            title=title,
            client_version=client_version,
            guard=guard,
        )

    # ================================================================== PATCH

    async def update_content(
        self,
        raw_id: str,
        caller: str,
        content: str,
        expected_content_version: int | None,
    ) -> SubmitResult:
        doc_id = parse_object_id(raw_id)
        normalized = normalize_content(content)

        def guard(doc: RawDoc) -> None:
            if expected_content_version is not None and doc["content_version"] != expected_content_version:
                raise VersionConflict(
                    "document was modified concurrently",
                    details={
                        "expected_content_version": expected_content_version,
                        "current_content_version": doc["content_version"],
                    },
                )

        return await self._apply_content(
            load=lambda: self._repo.get_owned(doc_id, caller),
            user_id=caller,
            content=normalized,
            chash=content_hash(normalized),
            guard=guard,
        )

    async def _apply_content(
        self,
        *,
        load: Callable[[], Awaitable[RawDoc | None]],
        user_id: str,
        content: str,
        chash: str,
        guard: Callable[[RawDoc], None],
        title: str | None = None,
        client_version: int | None = None,
    ) -> SubmitResult:
        """Read -> validate -> compare-and-swap on `rev`, retried on contention.

        Every precondition is evaluated against the exact revision the CAS
        then requires, so validation and write are effectively atomic.
        """
        doc: RawDoc | None = None
        try:
            for _ in range(self.MAX_CAS_ATTEMPTS):
                doc = await load()
                if doc is None:
                    raise NotFound("document not found")
                guard(doc)
                doc_id, version, base = doc["_id"], doc["content_version"], doc["slot_base"]

                if doc["content_hash"] == chash:
                    meta: dict[str, object] = {}
                    if title is not None and title != doc["title"]:
                        meta["title"] = title
                    if client_version is not None and client_version != doc.get("client_version"):
                        meta["client_version"] = client_version
                    if not meta:
                        return SubmitResult(doc=doc, outcome=SubmitOutcome.UNCHANGED, cache_hit=False)
                    meta["updated_at"] = self._now()
                    updated = await self._repo.cas_update(doc_id, user_id, doc["rev"], {"$set": meta})
                    if updated is not None:
                        return SubmitResult(doc=updated, outcome=SubmitOutcome.UNCHANGED, cache_hit=False)
                    continue

                cached = await self._cache.get(chash)
                was_active = is_active(doc)
                old_member, new_member = slot_member(base, version), slot_member(base, version + 1)
                if cached is None:
                    # Transfers v -> v+1 if v still holds a slot; otherwise needs capacity.
                    await self._limiter.acquire(user_id, doc_id, new_member, transfer_from=old_member)

                update = replacement_update(
                    doc,
                    content=content,
                    chash=chash,
                    now=self._now(),
                    cached=cached.as_tuple() if cached else None,
                    pipeline_version=self._s.pipeline_version,
                    title=title,
                    client_version=client_version,
                )
                updated = await self._repo.cas_update(doc_id, user_id, doc["rev"], update)
                if updated is None:
                    logger.info("cas: lost race, retrying", extra={"document_id": str(doc_id)})
                    continue
                if cached is not None and was_active:
                    await self._limiter.release(user_id, old_member)
                logger.info(
                    "document content replaced",
                    extra={
                        "document_id": str(doc_id),
                        "from_version": version,
                        "to_version": version + 1,
                        "cache_hit": cached is not None,
                    },
                )
                return SubmitResult(doc=updated, outcome=SubmitOutcome.NEW_VERSION, cache_hit=cached is not None)
            raise ConcurrentModification("too much contention on this document; retry the request")
        except (VersionConflict, StaleClientVersion, ClientVersionRequired, ConcurrentModification):
            # A slot may have been transferred/acquired before the write was abandoned.
            if doc is not None:
                await self._resync_slots(doc["_id"], user_id)
            raise

    async def _resync_slots(self, doc_id: ObjectId, user_id: str) -> None:
        current = await self._repo.get_owned(doc_id, user_id)
        if current is None:
            return
        keep = slot_member(current["slot_base"], current["content_version"]) if is_active(current) else None
        await self._limiter.reconcile(user_id, current["slot_base"], keep)

    # ================================================================== manual retry

    async def retry(self, raw_id: str, caller: str) -> RawDoc:
        """Resume a failed document from the stage that failed.

        A failure at `enriching` resumes at `enriching`: the stored, version-
        stamped summary is reused, `processing` is not redone.
        """
        doc_id = parse_object_id(raw_id)
        for _ in range(self.MAX_CAS_ATTEMPTS):
            doc = await self._repo.get_owned(doc_id, caller)
            if doc is None:
                raise NotFound("document not found")
            pipeline = doc["pipeline"]
            if pipeline["state"] != PipelineState.FAILED.value:
                raise InvalidState(
                    "only failed documents can be retried",
                    details={"status": pipeline["state"]},
                )
            failed_stage = Stage(pipeline["failed_stage"])
            resume_state = (
                PipelineState.QUEUED if failed_stage is Stage.PROCESSING else PipelineState.ENRICHING
            )
            await self._limiter.acquire(caller, doc_id, slot_member(doc["slot_base"], doc["content_version"]))
            updated = await self._repo.cas_update(
                doc_id,
                caller,
                doc["rev"],
                {
                    "$set": {
                        "pipeline.state": resume_state.value,
                        "pipeline.failed_stage": None,
                        "pipeline.due_at": self._now(),
                        "pipeline.lease_token": None,
                        "pipeline.lease_owner": None,
                        f"pipeline.stages.{failed_stage.value}": stage_doc(StageStatus.PENDING),
                        "updated_at": self._now(),
                    }
                },
            )
            if updated is not None:
                logger.info(
                    "document retry scheduled",
                    extra={"document_id": str(doc_id), "resume_stage": failed_stage.value},
                )
                return updated
        await self._resync_slots(doc_id, caller)
        raise ConcurrentModification("too much contention on this document; retry the request")
