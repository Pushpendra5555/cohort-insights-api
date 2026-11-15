"""HTTP contract. Request models validate beyond Pydantic defaults (patterns,
bounds, blank-string rejection, unknown-field rejection); response models are
built exclusively through :func:`to_view`, which routes `result` through the
staleness read gate."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, field_validator

from app.domain.document import RawDoc, current_result
from app.domain.enums import (
    PipelineState,
    ResultFreshness,
    ResultSource,
    Stage,
    StageStatus,
    SubmitOutcome,
)

MAX_CONTENT_CHARS = 100_000
USER_ID_PATTERN = r"^[A-Za-z0-9_.\-]{1,64}$"
CLIENT_REF_PATTERN = r"^[A-Za-z0-9_.:\-]{1,128}$"

UserId = Annotated[str, StringConstraints(pattern=USER_ID_PATTERN)]
ClientDocRef = Annotated[str, StringConstraints(pattern=CLIENT_REF_PATTERN)]
Title = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=300)]
Content = Annotated[str, StringConstraints(min_length=1, max_length=MAX_CONTENT_CHARS)]


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


def _reject_blank(value: str) -> str:
    if not value.strip():
        raise ValueError("must contain non-whitespace characters")
    return value


# ----------------------------------------------------------------------------- requests


class SubmitDocumentRequest(_Strict):
    user_id: UserId
    title: Title
    content: Content
    client_doc_ref: ClientDocRef | None = None
    client_version: int | None = Field(
        default=None,
        ge=0,
        description="Optional monotonic version from the partner system; enables out-of-order rejection.",
    )

    @field_validator("content")
    @classmethod
    def _content_not_blank(cls, value: str) -> str:
        return _reject_blank(value)


class UpdateDocumentRequest(_Strict):
    content: Content
    expected_content_version: int | None = Field(
        default=None,
        ge=1,
        description="Optimistic concurrency: reject with 409 unless this is the current content_version.",
    )

    @field_validator("content")
    @classmethod
    def _content_not_blank(cls, value: str) -> str:
        return _reject_blank(value)


# ----------------------------------------------------------------------------- responses


class StageView(BaseModel):
    status: StageStatus
    attempts: int
    started_at: datetime | None
    finished_at: datetime | None
    last_error: str | None
    next_attempt_at: datetime | None


class ResultView(BaseModel):
    summary: str
    tags: list[str]
    content_version: int = Field(description="The content version this result was derived from.")
    content_hash: str
    source: ResultSource
    produced_at: datetime


class DocumentView(BaseModel):
    document_id: str
    user_id: str
    title: str
    client_doc_ref: str | None
    client_version: int | None
    content_version: int
    content_hash: str
    status: PipelineState
    failed_stage: Stage | None
    stages: dict[Stage, StageView]
    result_freshness: ResultFreshness = Field(
        description="current: `result` matches content_version. stale: only `stale_result` exists. none: no result."
    )
    result: ResultView | None = Field(description="Present only when derived from the CURRENT content_version.")
    stale_result: ResultView | None = Field(
        description="Last complete result of an OLDER content version; compare its content_version."
    )
    created_at: datetime
    updated_at: datetime


class SubmitDocumentResponse(BaseModel):
    document_id: str
    status: PipelineState
    content_version: int
    outcome: SubmitOutcome
    cache_hit: bool


class DocumentPage(BaseModel):
    items: list[DocumentView]
    page: int
    page_size: int
    total: int
    has_next: bool


class HealthView(BaseModel):
    status: str
    checks: dict[str, str]


class ErrorBody(BaseModel):
    code: str
    message: str
    details: dict | None = None


class ErrorResponse(BaseModel):
    error: ErrorBody
    request_id: str | None = None


# ----------------------------------------------------------------------------- mapping


def to_view(doc: RawDoc) -> DocumentView:
    current = current_result(doc)
    previous = doc.get("previous_result")
    if current is not None:
        freshness = ResultFreshness.CURRENT
    elif previous is not None:
        freshness = ResultFreshness.STALE
    else:
        freshness = ResultFreshness.NONE
    pipeline = doc["pipeline"]
    return DocumentView(
        document_id=str(doc["_id"]),
        user_id=doc["user_id"],
        title=doc["title"],
        client_doc_ref=doc.get("client_doc_ref"),
        client_version=doc.get("client_version"),
        content_version=doc["content_version"],
        content_hash=doc["content_hash"],
        status=PipelineState(pipeline["state"]),
        failed_stage=Stage(pipeline["failed_stage"]) if pipeline.get("failed_stage") else None,
        stages={Stage(name): StageView(**stage) for name, stage in pipeline["stages"].items()},
        result_freshness=freshness,
        result=ResultView(**current) if current is not None else None,
        stale_result=ResultView(**previous) if current is None and previous is not None else None,
        created_at=doc["created_at"],
        updated_at=doc["updated_at"],
    )


def to_submit_response(doc: RawDoc, outcome: SubmitOutcome, cache_hit: bool) -> SubmitDocumentResponse:
    return SubmitDocumentResponse(
        document_id=str(doc["_id"]),
        status=PipelineState(doc["pipeline"]["state"]),
        content_version=doc["content_version"],
        outcome=outcome,
        cache_hit=cache_hit,
    )
