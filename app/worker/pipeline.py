"""Two-stage pipeline executor.

One unit of work = one stage of one document version:

    claim (atomic, leased) -> mark running (fenced) -> do work -> commit (fenced)

Race handling, all by MongoDB filters rather than locks:

* Two workers, same job: ``claim_due`` is a single find_one_and_update; only one
  can move ``due_at`` past ``now``.
* Same stage twice: every later write is fenced on ``lease_token``. If a lease
  expires and another worker reclaims, the first worker's commit matches nothing.
* PATCH racing an in-flight run: the PATCH bumps ``content_version`` and installs
  a fresh pipeline (lease_token = null) in one write. The in-flight worker's
  fenced commit no longer matches and is discarded. The tags commit additionally
  requires ``result.summary.content_version == v``.
* Worker crash: the claim pushed ``due_at`` to the lease expiry, so the job
  simply becomes due again. The interrupted attempt still counts toward
  ``stage_max_attempts``, so a poison job cannot crash-loop forever.
"""

from __future__ import annotations

import asyncio
import logging
import random
import uuid
from datetime import timedelta

from app.core.config import Settings
from app.domain.document import RawDoc, fresh_pipeline, slot_member, summary_field, tags_field
from app.domain.enums import PipelineState, ResultSource, Stage, StageStatus
from app.domain.processing import MockProcessor, StageFailure, backoff_delay
from app.repositories.documents import DocumentRepository
from app.services.cache import CachedResult, ResultCache
from app.services.documents import Clock, utcnow
from app.services.rate_limiter import ActiveJobLimiter

logger = logging.getLogger(__name__)


class PipelineWorker:
    def __init__(
        self,
        repo: DocumentRepository,
        cache: ResultCache,
        limiter: ActiveJobLimiter,
        processor: MockProcessor,
        settings: Settings,
        worker_id: str | None = None,
        clock: Clock = utcnow,
        rng: random.Random | None = None,
    ) -> None:
        self._repo = repo
        self._cache = cache
        self._limiter = limiter
        self._proc = processor
        self._s = settings
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self._now = clock
        self._rng = rng or random.Random()

    # ------------------------------------------------------------------ loop

    async def run_forever(self, stop: asyncio.Event) -> None:
        logger.info(
            "worker started",
            extra={"worker_id": self.worker_id, "concurrency": self._s.worker_concurrency},
        )
        await asyncio.gather(*(self._slot_loop(stop, i) for i in range(self._s.worker_concurrency)))
        logger.info("worker stopped", extra={"worker_id": self.worker_id})

    async def _slot_loop(self, stop: asyncio.Event, slot: int) -> None:
        while not stop.is_set():
            try:
                did_work = await self.run_once()
            except Exception:
                # Infrastructure fault (e.g. MongoDB blip). Log with traceback and back
                # off; the leased job, if any, is re-claimed after its lease expires.
                logger.exception("worker iteration failed", extra={"worker_id": self.worker_id, "slot": slot})
                did_work = False
            if not did_work:
                jitter = self._rng.uniform(0, self._s.worker_poll_interval_s)
                try:
                    await asyncio.wait_for(stop.wait(), timeout=self._s.worker_poll_interval_s + jitter)
                except TimeoutError:
                    pass  # normal: poll interval elapsed without a stop signal

    # ------------------------------------------------------------------ one unit

    async def run_once(self) -> bool:
        """Claim and execute one stage. Returns False when no work was due."""
        token = uuid.uuid4().hex
        doc = await self._repo.claim_due(self._now(), self.worker_id, token, self._s.lease_duration_s)
        if doc is None:
            return False

        version = doc["content_version"]
        state = PipelineState(doc["pipeline"]["state"])
        log = {"document_id": str(doc["_id"]), "content_version": version, "worker_id": self.worker_id}

        if state in (PipelineState.QUEUED, PipelineState.PROCESSING):
            stage = Stage.PROCESSING
        elif state is PipelineState.ENRICHING:
            stage = Stage.ENRICHING
        else:
            # Terminal documents never carry due_at; clear defensively if one does.
            logger.error("claimed a terminal document; clearing due_at", extra=log)
            await self._repo.fenced_update(
                doc["_id"], version, token, {"$set": {"pipeline.due_at": None, "pipeline.lease_token": None}}
            )
            return True

        attempt = doc["pipeline"]["stages"][stage.value]["attempts"] + 1
        log |= {"stage": stage.value, "attempt": attempt}

        if attempt > self._s.stage_max_attempts:
            await self._fail_terminal(doc, stage, token, "attempts exhausted after worker lease expiry", log)
            return True

        started = self._now()
        marked = await self._repo.fenced_update(
            doc["_id"],
            version,
            token,
            {
                "$set": {
                    "pipeline.state": (
                        PipelineState.PROCESSING if stage is Stage.PROCESSING else PipelineState.ENRICHING
                    ).value,
                    f"pipeline.stages.{stage.value}.status": StageStatus.RUNNING.value,
                    f"pipeline.stages.{stage.value}.attempts": attempt,
                    f"pipeline.stages.{stage.value}.started_at": started,
                    f"pipeline.stages.{stage.value}.next_attempt_at": None,
                }
            },
        )
        if not marked:
            logger.info("superseded before start; dropping", extra=log)
            return True
        await self._limiter.touch(doc["user_id"], slot_member(doc["slot_base"], version))
        logger.info("stage started", extra=log)

        try:
            if stage is Stage.PROCESSING:
                summary = await self._proc.summarize(doc["content"], attempt)
                await self._commit_summary(doc, token, summary, log)
            else:
                stored = doc["result"]["summary"]
                if stored is None or stored["content_version"] != version:
                    # Unreachable while the write invariants hold; recover rather than guess.
                    logger.error("enriching without a current summary; restarting at stage 1", extra=log)
                    await self._repo.fenced_update(
                        doc["_id"],
                        version,
                        token,
                        {"$set": {"pipeline": fresh_pipeline(self._now()), "result.tags": None}},
                    )
                    return True
                tags = await self._proc.tag(stored["value"], attempt)
                await self._commit_tags(doc, token, stored, tags, log)
        except StageFailure as exc:
            await self._record_failure(doc, stage, token, attempt, str(exc), log)
        return True

    # ------------------------------------------------------------------ commits

    async def _commit_summary(self, doc: RawDoc, token: str, summary: str, log: dict) -> None:
        now, version = self._now(), doc["content_version"]
        ok = await self._repo.fenced_update(
            doc["_id"],
            version,
            token,
            {
                "$set": {
                    "result.summary": summary_field(
                        value=summary,
                        version=version,
                        chash=doc["content_hash"],
                        pipeline_version=self._s.pipeline_version,
                        source=ResultSource.PIPELINE,
                        now=now,
                    ),
                    "pipeline.state": PipelineState.ENRICHING.value,
                    "pipeline.stages.processing.status": StageStatus.SUCCEEDED.value,
                    "pipeline.stages.processing.finished_at": now,
                    "pipeline.stages.processing.last_error": None,
                    "pipeline.due_at": now,  # stage 2 is immediately claimable
                    "pipeline.lease_token": None,
                    "pipeline.lease_owner": None,
                }
            },
        )
        logger.info("stage committed" if ok else "commit discarded: superseded", extra=log)

    async def _commit_tags(self, doc: RawDoc, token: str, stored_summary: dict, tags: list[str], log: dict) -> None:
        now, version = self._now(), doc["content_version"]
        ok = await self._repo.fenced_update(
            doc["_id"],
            version,
            token,
            {
                "$set": {
                    "result.tags": tags_field(
                        value=tags,
                        version=version,
                        chash=doc["content_hash"],
                        summary_version=stored_summary["content_version"],
                        pipeline_version=self._s.pipeline_version,
                        source=ResultSource.PIPELINE,
                        now=now,
                    ),
                    "pipeline.state": PipelineState.COMPLETED.value,
                    "pipeline.completed_at": now,
                    "pipeline.stages.enriching.status": StageStatus.SUCCEEDED.value,
                    "pipeline.stages.enriching.finished_at": now,
                    "pipeline.stages.enriching.last_error": None,
                    "pipeline.due_at": None,
                    "pipeline.lease_token": None,
                    "pipeline.lease_owner": None,
                }
            },
            # Tags may only land next to the summary of the SAME version.
            extra_filter={"result.summary.content_version": version},
        )
        if not ok:
            logger.info("commit discarded: superseded", extra=log)
            return
        logger.info("document completed", extra=log)
        await self._cache.set(doc["content_hash"], CachedResult(summary=stored_summary["value"], tags=tags))
        await self._limiter.release(doc["user_id"], slot_member(doc["slot_base"], version))

    async def _record_failure(
        self, doc: RawDoc, stage: Stage, token: str, attempt: int, error: str, log: dict
    ) -> None:
        if attempt >= self._s.stage_max_attempts:
            await self._fail_terminal(doc, stage, token, error, log)
            return
        now = self._now()
        delay = backoff_delay(attempt, self._s.retry_base_delay_s, self._s.retry_max_delay_s, self._rng)
        next_at = now + timedelta(seconds=delay)
        ok = await self._repo.fenced_update(
            doc["_id"],
            doc["content_version"],
            token,
            {
                "$set": {
                    f"pipeline.stages.{stage.value}.status": StageStatus.RETRY_SCHEDULED.value,
                    f"pipeline.stages.{stage.value}.last_error": error,
                    f"pipeline.stages.{stage.value}.finished_at": now,
                    f"pipeline.stages.{stage.value}.next_attempt_at": next_at,
                    "pipeline.due_at": next_at,
                    "pipeline.lease_token": None,
                    "pipeline.lease_owner": None,
                }
            },
        )
        logger.warning(
            "stage failed; retry scheduled" if ok else "failure discarded: superseded",
            extra=log | {"error": error, "retry_in_s": round(delay, 3)},
        )

    async def _fail_terminal(self, doc: RawDoc, stage: Stage, token: str, error: str, log: dict) -> None:
        now, version = self._now(), doc["content_version"]
        ok = await self._repo.fenced_update(
            doc["_id"],
            version,
            token,
            {
                "$set": {
                    "pipeline.state": PipelineState.FAILED.value,
                    "pipeline.failed_stage": stage.value,
                    f"pipeline.stages.{stage.value}.status": StageStatus.FAILED.value,
                    f"pipeline.stages.{stage.value}.last_error": error,
                    f"pipeline.stages.{stage.value}.finished_at": now,
                    "pipeline.due_at": None,
                    "pipeline.lease_token": None,
                    "pipeline.lease_owner": None,
                }
            },
        )
        if not ok:
            logger.info("failure discarded: superseded", extra=log)
            return
        logger.warning("document failed", extra=log | {"error": error})
        await self._limiter.release(doc["user_id"], slot_member(doc["slot_base"], version))
