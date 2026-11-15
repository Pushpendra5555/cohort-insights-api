"""Pure-function unit tests and the health endpoint."""

from __future__ import annotations

import random
from datetime import datetime, timezone

import httpx
import pytest
from bson import ObjectId
from pydantic import ValidationError

from app.core.config import Settings
from app.domain.document import current_result, new_document, replacement_update
from app.domain.hashing import content_hash, normalize_content
from app.domain.processing import backoff_delay, make_summary, make_tags

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _completed_doc(content: str = "Alpha beta gamma. Delta epsilon.") -> dict:
    norm = normalize_content(content)
    summary = make_summary(norm)
    return new_document(
        doc_id=ObjectId(), user_id="u", title="t", content=norm, chash=content_hash(norm),
        client_doc_ref=None, client_version=None, now=NOW,
        cached=(summary, make_tags(summary)), pipeline_version="v1",
    )


def test_normalisation_unifies_line_endings_and_unicode_forms() -> None:
    assert normalize_content("a\r\nb\rc") == "a\nb\nc"
    composed, decomposed = "caf\u00e9", "cafe\u0301"
    assert content_hash(normalize_content(composed)) == content_hash(normalize_content(decomposed))


def test_processing_is_deterministic() -> None:
    text = "Solar panels are cheap. Installers are busy. Third sentence is ignored."
    assert make_summary(text) == "Solar panels are cheap. Installers are busy."
    assert make_tags(make_summary(text)) == make_tags(make_summary(text))
    assert "solar" in make_tags(make_summary(text))


def test_summary_is_word_capped() -> None:
    assert make_summary(" ".join(["word"] * 100)).endswith("…")


def test_backoff_grows_exponentially_with_bounded_jitter() -> None:
    rng = random.Random(0)
    for attempt, ceiling in [(1, 2.0), (2, 4.0), (3, 8.0), (10, 30.0)]:
        d = backoff_delay(attempt, base=2.0, cap=30.0, rng=rng)
        assert ceiling / 2 <= d <= ceiling


def test_read_gate_withholds_a_mismatched_pair() -> None:
    doc = _completed_doc()
    assert current_result(doc) is not None
    doc["result"]["tags"]["content_version"] = 99  # simulate corruption / a hypothetical bug
    assert current_result(doc) is None  # fails closed: no result rather than a mixed one


def test_read_gate_rejects_result_from_other_content() -> None:
    doc = _completed_doc()
    doc["content_hash"] = content_hash("something else")
    assert current_result(doc) is None


def test_replacement_is_one_atomic_update_that_clears_result_and_snapshots_previous() -> None:
    doc = _completed_doc()
    update = replacement_update(
        doc, content="New text.", chash=content_hash("New text."), now=NOW, cached=None, pipeline_version="v1"
    )
    s = update["$set"]
    assert s["content_version"] == 2
    assert s["result"] == {"summary": None, "tags": None}
    assert s["pipeline"]["lease_token"] is None and s["pipeline"]["state"] == "queued"
    assert s["previous_result"]["content_version"] == 1
    assert update["$inc"] == {"rev": 1}


def test_settings_reject_lease_shorter_than_stage() -> None:
    with pytest.raises(ValidationError):
        Settings(lease_duration_s=10, stage1_max_s=20)


async def test_health_reports_dependencies(client: httpx.AsyncClient) -> None:
    resp = await client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok", "checks": {"mongodb": "ok", "redis": "ok"}}
    assert (await client.get("/health/live")).status_code == 200


async def test_request_id_is_propagated(client: httpx.AsyncClient) -> None:
    resp = await client.get("/health/live", headers={"X-Request-Id": "abc123"})
    assert resp.headers["X-Request-Id"] == "abc123"
