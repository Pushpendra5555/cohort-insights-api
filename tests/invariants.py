"""The staleness invariants, asserted directly against the stored document.

If these hold for every reachable stored state, no reader -- not even one
bypassing the API's read gate -- can observe a mixed-version result.
"""

from __future__ import annotations

from typing import Any

from app.domain.hashing import content_hash
from app.domain.processing import make_summary, make_tags


def assert_consistent(doc: dict[str, Any]) -> None:
    version, chash = doc["content_version"], doc["content_hash"]
    state = doc["pipeline"]["state"]
    summary, tags = doc["result"]["summary"], doc["result"]["tags"]
    ctx = f"doc={doc['_id']} v={version} state={state}"

    # I0: the stored hash identifies the stored content.
    assert content_hash(doc["content"]) == chash, ctx

    # I1: a summary in `result` was produced from exactly the current content.
    if summary is not None:
        assert summary["content_version"] == version, ctx
        assert summary["content_hash"] == chash, ctx
        assert summary["value"] == make_summary(doc["content"]), f"{ctx}: summary not derived from content"

    # I2: tags exist only on top of a same-version summary, derived from it.
    if tags is not None:
        assert summary is not None, ctx
        assert tags["content_version"] == version, ctx
        assert tags["derived_from_summary_version"] == summary["content_version"], ctx
        assert tags["value"] == make_tags(summary["value"]), f"{ctx}: tags not derived from summary"

    # I3: status and result agree.
    assert (state == "completed") == (summary is not None and tags is not None), ctx
    if state == "enriching":
        assert summary is not None, ctx
    if state == "failed" and doc["pipeline"]["failed_stage"] == "enriching":
        assert summary is not None, f"{ctx}: partial progress lost"

    # I4: the stale snapshot is strictly older and internally consistent.
    prev = doc.get("previous_result")
    if prev is not None:
        assert prev["content_version"] < version, ctx
        assert prev["tags"] == make_tags(prev["summary"]), ctx
