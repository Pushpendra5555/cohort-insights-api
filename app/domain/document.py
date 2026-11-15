"""MongoDB document shape and the pure functions that build / interpret it.

Stored shape (one document per logical document, ``documents`` collection)::

    {
      _id:              ObjectId,
      user_id:          str,                      # owner; every query filters on it
      title:            str,
      client_doc_ref:   str            (absent when not supplied -> partial unique index)
      client_version:   int            (absent unless the partner versions its refs)
      content:          str,                      # normalised
      content_hash:     sha256(content),
      content_version:  int,                      # +1 on every content change; never reused
      rev:              int,                      # +1 on EVERY write; compare-and-swap token
      slot_base:        str,                      # rate-limit slot identity (see slot_base_for)
      created_at, updated_at,
      pipeline: {
        state:        queued|processing|enriching|completed|failed,
        failed_stage: null|processing|enriching,
        due_at:       datetime|null,              # claimable at/after; doubles as lease expiry
        lease_token:  str|null,                   # fencing token of the claiming worker
        lease_owner:  str|null,
        completed_at: datetime|null,
        stages: { processing: StageDoc, enriching: StageDoc }
      },
      result: {
        summary: null | {value, content_version, content_hash, pipeline_version, source, produced_at},
        tags:    null | {value, content_version, content_hash, derived_from_summary_version,
                         pipeline_version, source, produced_at}
      },
      previous_result: null | {summary, tags, content_version, content_hash, source, produced_at}
    }

The single source of truth for "is there a current result?" is
:func:`current_result`. The API never exposes ``result`` any other way.
"""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime
from typing import Any

from bson import ObjectId

from app.domain.enums import (
    ACTIVE_STATES,
    PipelineState,
    ResultSource,
    Stage,
    StageStatus,
)

logger = logging.getLogger(__name__)

RawDoc = dict[str, Any]


# --------------------------------------------------------------------------- builders


def stage_doc(status: StageStatus) -> dict[str, Any]:
    return {
        "status": status.value,
        "attempts": 0,
        "started_at": None,
        "finished_at": None,
        "last_error": None,
        "next_attempt_at": None,
    }


def fresh_pipeline(now: datetime) -> dict[str, Any]:
    """A pipeline run that starts from stage 1, claimable immediately."""
    return {
        "state": PipelineState.QUEUED.value,
        "failed_stage": None,
        "due_at": now,
        "lease_token": None,
        "lease_owner": None,
        "completed_at": None,
        "stages": {
            Stage.PROCESSING.value: stage_doc(StageStatus.PENDING),
            Stage.ENRICHING.value: stage_doc(StageStatus.PENDING),
        },
    }


def cached_pipeline(now: datetime) -> dict[str, Any]:
    """A pipeline that was satisfied entirely from the content cache."""
    return {
        "state": PipelineState.COMPLETED.value,
        "failed_stage": None,
        "due_at": None,
        "lease_token": None,
        "lease_owner": None,
        "completed_at": now,
        "stages": {
            Stage.PROCESSING.value: stage_doc(StageStatus.CACHED),
            Stage.ENRICHING.value: stage_doc(StageStatus.CACHED),
        },
    }


def summary_field(
    *, value: str, version: int, chash: str, pipeline_version: str, source: ResultSource, now: datetime
) -> dict[str, Any]:
    return {
        "value": value,
        "content_version": version,
        "content_hash": chash,
        "pipeline_version": pipeline_version,
        "source": source.value,
        "produced_at": now,
    }


def tags_field(
    *,
    value: list[str],
    version: int,
    chash: str,
    summary_version: int,
    pipeline_version: str,
    source: ResultSource,
    now: datetime,
) -> dict[str, Any]:
    return {
        "value": value,
        "content_version": version,
        "content_hash": chash,
        "derived_from_summary_version": summary_version,
        "pipeline_version": pipeline_version,
        "source": source.value,
        "produced_at": now,
    }


def cached_result(
    *, summary: str, tags: list[str], version: int, chash: str, pipeline_version: str, now: datetime
) -> dict[str, Any]:
    return {
        "summary": summary_field(
            value=summary,
            version=version,
            chash=chash,
            pipeline_version=pipeline_version,
            source=ResultSource.CACHE,
            now=now,
        ),
        "tags": tags_field(
            value=tags,
            version=version,
            chash=chash,
            summary_version=version,
            pipeline_version=pipeline_version,
            source=ResultSource.CACHE,
            now=now,
        ),
    }


def empty_result() -> dict[str, Any]:
    return {"summary": None, "tags": None}


def new_document(
    *,
    doc_id: ObjectId,
    user_id: str,
    title: str,
    content: str,
    chash: str,
    client_doc_ref: str | None,
    client_version: int | None,
    now: datetime,
    cached: tuple[str, list[str]] | None,
    pipeline_version: str,
) -> RawDoc:
    doc: RawDoc = {
        "_id": doc_id,
        "user_id": user_id,
        "title": title,
        "content": content,
        "content_hash": chash,
        "content_version": 1,
        "rev": 1,
        "slot_base": slot_base_for(doc_id, user_id, client_doc_ref),
        "created_at": now,
        "updated_at": now,
        "previous_result": None,
    }
    # Absent (not null) when unset: the partial unique index only covers strings,
    # so any number of documents may omit a ref.
    if client_doc_ref is not None:
        doc["client_doc_ref"] = client_doc_ref
    if client_version is not None:
        doc["client_version"] = client_version
    if cached is not None:
        summary, tags = cached
        doc["pipeline"] = cached_pipeline(now)
        doc["result"] = cached_result(
            summary=summary, tags=tags, version=1, chash=chash, pipeline_version=pipeline_version, now=now
        )
    else:
        doc["pipeline"] = fresh_pipeline(now)
        doc["result"] = empty_result()
    return doc


# --------------------------------------------------------------------------- read gate


def current_result(doc: RawDoc) -> dict[str, Any] | None:
    """Return the (summary, tags) pair iff BOTH were derived from the current content.

    This is the read-side half of the staleness guarantee: even if a bug ever
    wrote a mismatched pair, this gate fails closed -- the reader sees no
    result rather than a mixed one -- and the anomaly is logged loudly.
    """
    result = doc.get("result") or {}
    summary, tags = result.get("summary"), result.get("tags")
    if summary is None or tags is None:
        return None
    version, chash = doc["content_version"], doc["content_hash"]
    consistent = (
        summary["content_version"] == version
        and tags["content_version"] == version
        and tags["derived_from_summary_version"] == summary["content_version"]
        and summary["content_hash"] == chash
        and tags["content_hash"] == chash
    )
    if not consistent:
        logger.error(
            "staleness invariant violated; withholding result",
            extra={
                "document_id": str(doc["_id"]),
                "content_version": version,
                "summary_version": summary.get("content_version"),
                "tags_version": tags.get("content_version"),
            },
        )
        return None
    return {
        "summary": summary["value"],
        "tags": list(tags["value"]),
        "content_version": version,
        "content_hash": chash,
        "source": tags["source"],
        "produced_at": tags["produced_at"],
    }


def is_active(doc: RawDoc) -> bool:
    return PipelineState(doc["pipeline"]["state"]) in ACTIVE_STATES


def slot_base_for(doc_id: ObjectId, user_id: str, client_doc_ref: str | None) -> str:
    """Stable identity of a document's rate-limit slot, fixed at creation.

    Documents with a ref derive it from (user_id, ref), so concurrent duplicate
    first-submissions of one ref share ONE slot instead of each reserving one
    (which could wrongly 429 a pure replay). Others use the document id.
    """
    if client_doc_ref is None:
        return str(doc_id)
    digest = hashlib.sha256(f"{user_id}\x00{client_doc_ref}".encode()).hexdigest()[:24]
    return f"ref-{digest}"


def slot_member(slot_base: str, content_version: int) -> str:
    """Rate-limit slot for one content version. Version-scoped so a late release
    for v (by a fenced-out worker) can never free the slot held by v+1."""
    return f"{slot_base}:{content_version}"


def replacement_update(
    doc: RawDoc,
    *,
    content: str,
    chash: str,
    now: datetime,
    cached: tuple[str, list[str]] | None,
    pipeline_version: str,
    title: str | None = None,
    client_version: int | None = None,
) -> dict[str, Any]:
    """The ONE atomic write that changes content.

    In a single document update it (1) bumps content_version, (2) replaces the
    content + hash, (3) discards the in-flight pipeline (new lease_token => all
    in-flight workers are fenced out), and (4) clears `result`, preserving the
    last complete result as an explicitly-versioned `previous_result`.
    There is no instant at which new content coexists with an old summary/tags
    inside `result`.
    """
    new_version = doc["content_version"] + 1
    completed = current_result(doc)
    previous = completed if completed is not None else doc.get("previous_result")

    set_fields: dict[str, Any] = {
        "content": content,
        "content_hash": chash,
        "content_version": new_version,
        "updated_at": now,
        "previous_result": previous,
    }
    if title is not None:
        set_fields["title"] = title
    if client_version is not None:
        set_fields["client_version"] = client_version
    if cached is not None:
        summary, tags = cached
        set_fields["pipeline"] = cached_pipeline(now)
        set_fields["result"] = cached_result(
            summary=summary,
            tags=tags,
            version=new_version,
            chash=chash,
            pipeline_version=pipeline_version,
            now=now,
        )
    else:
        set_fields["pipeline"] = fresh_pipeline(now)
        set_fields["result"] = empty_result()
    return {"$set": set_fields, "$inc": {"rev": 1}}
