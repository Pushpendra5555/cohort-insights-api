from __future__ import annotations

from enum import StrEnum


class PipelineState(StrEnum):
    """Coarse position in the pipeline: queued -> processing -> enriching -> completed | failed."""

    QUEUED = "queued"
    PROCESSING = "processing"
    ENRICHING = "enriching"
    COMPLETED = "completed"
    FAILED = "failed"


ACTIVE_STATES: frozenset[PipelineState] = frozenset(
    {PipelineState.QUEUED, PipelineState.PROCESSING, PipelineState.ENRICHING}
)
TERMINAL_STATES: frozenset[PipelineState] = frozenset(
    {PipelineState.COMPLETED, PipelineState.FAILED}
)


class Stage(StrEnum):
    PROCESSING = "processing"  # stage 1: content -> summary
    ENRICHING = "enriching"  # stage 2: summary -> tags


class StageStatus(StrEnum):
    """Per-stage detail. Lets a caller see exactly which stage failed or is retrying."""

    PENDING = "pending"
    RUNNING = "running"
    RETRY_SCHEDULED = "retry_scheduled"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CACHED = "cached"  # satisfied from the content-hash cache; never ran


class ResultFreshness(StrEnum):
    CURRENT = "current"  # `result` was produced from exactly the current content_version
    STALE = "stale"  # only an older version's result exists (`stale_result`)
    NONE = "none"  # nothing has ever completed for this document


class SubmitOutcome(StrEnum):
    CREATED = "created"
    UNCHANGED = "unchanged"  # idempotent replay: content hash identical to current
    NEW_VERSION = "new_version"  # same document, new content_version, reprocessing


class ResultSource(StrEnum):
    PIPELINE = "pipeline"
    CACHE = "cache"
