# Requirements Traceability

Every requirement in the assessment PDF, mapped to where it is implemented and the test that proves it. Section names in the first column are the PDF's own headings.

## API Specification

| PDF requirement | Implementation | Verified by |
|---|---|---|
| **§1 Submit:** accepts `user_id`, `title`, `content`, optional `client_doc_ref`; validates; stores as `queued`; enqueues; returns `document_id` + `status` | `api/routes/documents.py::submit_document`, `services/documents.py::submit`, `domain/document.py::new_document` (a pending `due_at` *is* the enqueue) | `test_submit_returns_201_with_id_and_queued_status`, `test_submit_validation_errors_are_422` |
| **§2 Update:** same id, new content, reprocessed from scratch | `services/documents.py::update_content` → `domain/document.py::replacement_update` | `test_patch_between_stages_discards_old_summary` |
| §2: old summary never presented as current; no mix of new tags with old summary or vice versa | Version stamps + fenced writes + read gate (README "Schema & Staleness Design") | `test_patch_racing_in_flight_stage_fences_out_old_commit`, `test_old_version_tags_commit_is_rejected`, `test_concurrent_stress_invariants_hold_at_every_observation` |
| **§3 Poll:** position across both stages; summary and tags once complete | `api/schemas.py::to_view` (`status`, `failed_stage`, `stages.*`, `result`) | `test_stage_progression_is_visible`, `test_happy_path_end_to_end` |
| §3: caller can tell whether the stored result is current or stale | `result.content_version` vs `content_version`; `result_freshness`; `stale_result` labelled with its version | `test_completed_then_patched_exposes_labelled_stale_result` |
| **§4 List:** paginated, newest first, `page`, `page_size`, optional `status`; own documents only | `api/routes/users.py`, `repositories/documents.py::list_for_user` | `test_list_is_scoped_newest_first_and_paginated`, `test_list_other_users_documents_is_404`, `test_list_rejects_invalid_status_and_page` |
| **§5 By-ref lookup:** returns the associated document or 404 | `routes/documents.py::get_by_ref`, `repositories/documents.py::get_by_ref` | `test_lookup_by_ref_and_404_when_unknown` |
| §5: defined behaviour for repeats, retries, out-of-order delivery, different content | `services/documents.py::_resubmit_ref` (policy table in README "Crosswalk") | `test_repeat_same_content_is_idempotent_replay`, `test_repeat_different_content_becomes_new_version_of_same_document`, `test_out_of_order_versioned_resubmission_is_rejected`, `test_versioned_replay_is_idempotent_but_conflicting_payload_is_not`, `test_versioned_ref_requires_version_on_resubmission`, `test_unversioned_refs_follow_arrival_order`, `test_concurrent_first_submissions_create_exactly_one_document` |

## Core Requirements

| PDF requirement | Implementation | Verified by |
|---|---|---|
| **Two-stage pipeline:** `queued → processing → enriching → completed \| failed`; 10–20 s and 5–15 s; about 10% failure per stage | `worker/pipeline.py`, `domain/processing.py::MockProcessor`, timings and rate in `core/config.py` | `test_stage_progression_is_visible`, end-to-end process smoke run |
| Enriching failure must not force redoing processing | Stage-1 summary persisted, version-stamped, before stage 2; retries and `/retry` resume at `enriching` | `test_enriching_failure_is_attributed_and_preserves_summary`, `test_manual_retry_resumes_at_enriching_without_redoing_processing`, `test_transient_failure_retries_with_backoff` |
| Caller can tell exactly which stage failed, without ad-hoc string fields | Structured `failed_stage` + per-stage `{status, attempts, last_error, …}` | `test_enriching_failure_is_attributed_and_preserves_summary`, `test_processing_failure_is_attributed` |
| **Staleness:** PATCH reprocesses both stages | `replacement_update` installs `fresh_pipeline` | `test_patch_between_stages_discards_old_summary` |
| Unambiguous mechanism (not a bare boolean); rules out the mixed case entirely | `content_version` + `content_hash` stamped on each derived field; `domain/document.py::current_result` | `test_read_gate_withholds_a_mismatched_pair`, `test_read_gate_rejects_result_from_other_content`, `tests/invariants.py` |
| **Crosswalk:** optional and unique when present | Unique partial index `user_client_ref_unique` | `test_many_documents_without_ref_do_not_collide`, `test_refs_are_isolated_per_owner` |
| Lookup via an index, not a collection scan; index stated in the README | README "Indexes"; `repositories/documents.py::by_ref_filter` | `test_by_ref_lookup_uses_index_not_collection_scan` (asserts `IXSCAN` on the named index and no `COLLSCAN`) |
| **Rate limiting:** at most 3 in `{queued, processing, enriching}`; 429; Redis-tracked | `services/rate_limiter.py` (Lua slot set) | `test_fourth_active_document_is_429`, `test_enriching_counts_as_active`, `test_concurrent_submissions_never_exceed_limit`, `test_completion_and_failure_free_slots`, `test_limit_is_per_user` |
| **Content cache:** identical content returns the earlier result immediately | `services/cache.py` (Redis + MongoDB tiers) | `test_identical_content_returns_result_immediately`, `test_mongo_tier_serves_after_redis_entry_expires` |
| Cache keyed by content, not `document_id`; survives PATCH without leaking | Key = `pipeline_version` + sha256(content) | `test_cache_is_keyed_by_content_not_document`, `test_patch_to_previously_seen_content_completes_instantly` |
| **Ownership:** non-owner gets 404 (or argued 403); no open lookup by id | Owner filter inside every query (`get_owned`, `get_by_ref`, list scope) | `test_non_owner_gets_identical_404_to_nonexistent`, `test_non_owner_cannot_patch_or_retry`, `test_list_other_users_documents_is_404` |

## Deliverables

| PDF deliverable | Location |
|---|---|
| Source code | `app/` |
| Single-command `docker-compose.yml` (API, MongoDB, Redis) | `docker-compose.yml`, plus a worker service and a `test` profile |
| README: design decisions, assumptions, more-time list | `README.md` |
| README section **"Schema & Staleness Design"** | `README.md` |
| README section **"At 100×"** (300–500 words; four questions) | `README.md` (480 words) |
| Tests, including staleness and crosswalk | `tests/` (75 tests) |

## Evaluation criteria

| Dimension | Where it is addressed |
|---|---|
| API design (201/404/429, Pydantic, routers, DI) | `api/`; `test_api.py` |
| Data modelling (indexes, ObjectId handling, version field) | `domain/document.py`, `repositories/documents.py`; malformed id → 404 via `parse_object_id` |
| Redis usage (two concerns, TTLs, degradation) | `rate_limiter.py`, `cache.py`; `test_redis_outage_falls_back_to_mongodb_count`, `test_cache_degrades_to_mongodb_when_redis_is_down` |
| Async / concurrency (double claim, double stage, PATCH vs in-flight run) | `test_two_workers_never_claim_the_same_job`, `test_expired_lease_is_reclaimed_and_stale_worker_is_fenced`, `test_patch_racing_in_flight_stage_fences_out_old_commit` |
| Scale reasoning | README "At 100×" |
| Production readiness (logging, errors, config, health, separation) | `core/logging.py`, `core/errors.py`, `core/config.py`, `routes/health.py`, layered packages |

## Bonus points

| Bonus | Status | Evidence |
|---|---|---|
| Tested "repeat ref, different content" | Done | `test_repeat_different_content_becomes_new_version_of_same_document` and the other crosswalk tests |
| Optimistic concurrency for racing PATCHes | Done | `test_racing_patches_with_same_expected_version_one_wins`, `test_racing_patches_without_precondition_both_apply_in_order` |
| `GET /health` checking MongoDB and Redis | Done | `test_health_reports_dependencies`; degraded case in `test_redis_outage_falls_back_to_mongodb_count` |
| Retry with backoff for a failed stage | Done | `test_transient_failure_retries_with_backoff`, `test_backoff_schedules_future_due_time`, `test_backoff_grows_exponentially_with_bounded_jitter` |
| Integration tests with pytest + `httpx.AsyncClient` | Done | All API tests use `httpx.AsyncClient` over `ASGITransport` |
| `.env.example` with documented variables | Done | `.env.example` |

## Common pitfalls avoided

| Pitfall | How it is avoided |
|---|---|
| "Stale" as a boolean with nothing to compare | Freshness is computed by comparing version and hash stamps |
| Cache keyed by `document_id` | Keyed by content hash and pipeline version |
| By-ref lookup scanning the collection | Unique partial index, proven with `explain()` |
| Another user's document distinguishable from a missing one | Owner filter inside the query; identical 404 |
| No validation beyond Pydantic defaults | Patterns, length bounds, blank rejection, unknown-field rejection, page bounds |
| `except Exception: pass` | None in the codebase. The two `except Exception` blocks either release a reserved slot and re-raise (`services/documents.py`) or log the full traceback and back off (worker loop). The only `pass` handles `TimeoutError` from the idle poll's `wait_for`, which is the normal "interval elapsed" signal. |
