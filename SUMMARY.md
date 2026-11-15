# Assessment Summary: Cohort Insights API

Analysis of the assessment, the decisions it forced, and the solution's flow. "PDF §2" refers to the assessment document. Full rationale is in `README.md`; a requirement-by-requirement map is in `docs/requirements-traceability.md`.

## 1. What the assessment is really testing

**Observations (from the PDF)**

- Overview: no AI integration; the focus is "schema & staleness design, cross-system identifier mapping, multi-stage async pipelines, and reasoning about scale."
- Evaluation Criteria: "The functional requirements are intentionally straightforward. What we are really evaluating is how you build it."
- The staleness criterion asks whether a mismatch is "actually impossible — not just unlikely"; hand-waved answers "score low."
- Guidelines: crosswalk and staleness have no single correct answer; documented assumptions are "part of the evaluation."

**Conclusions**

- Staleness carries the most weight, so it was designed first and argued formally, not just tested by example.
- Every open question (crosswalk, ownership code, auth) gets an explicit, justified decision.

## 2. Key design decisions

**Staleness (PDF §2, §3, "Staleness on Update")**

- *Observation:* the PDF rejects "a boolean 'is_stale' flag with nothing to compare against" and suggests "a content hash or version counter stored alongside each derived field."
- *Decision:* `content_version` and `content_hash` are stamped on the summary and on the tags. Freshness is always a comparison.
- *Guarantee:*
  - A PATCH bumps the version and clears the result in **one atomic document update**.
  - Worker commits only apply if the document is still at their version and they still hold the lease.
  - Tags can only be written beside a summary of the same version.
  - All six write paths preserve the invariant, so a mixed state cannot be stored; a read-side gate fails closed as a second layer.
- *Debrief point:* correctness never depends on timing; leases affect only when work retries.

**Crosswalk (PDF §5, "Crosswalk")**

- *Observation:* the partner "may retry a submission or send the same ref twice, possibly out of order and possibly with different content." "Undefined behavior" is explicitly not acceptable.
- *Decision:* a repeat ref is a **new version of the same document**.
  - Identical content is an idempotent replay (200).
  - An optional partner `client_version` enables strict ordering: older versions are rejected with 409.
- *Justification:* without partner ordering data, the server cannot know which message is newer; opt-in ordering is honest about that.
- *Assumption:* refs are unique **per owner**; global uniqueness would leak one tenant's ref to another via a 409.

**Two-stage pipeline (PDF "Two-Stage Async Pipeline")**

- *Observation:* "A failure at enriching must not force redoing processing," and callers must tell "exactly which stage failed, rather than bolting on ad-hoc string fields."
- *Decision:* a top-level `status` plus structured per-stage records (status, attempts, error, next attempt). The stage-1 summary is persisted before stage 2 starts.
- *Concurrency:* atomic leased claims and fencing tokens on every worker write cover both named races: "two workers claiming the same job" and "a PATCH racing an in-flight run."

**Redis (PDF "Per-User Rate Limiting", "Content-Based Caching")**

- *Observation:* the criteria ask for "two distinct concerns", TTLs, and "graceful degradation if Redis is unavailable."
- *Rate limiter:* atomic Lua over per-user, version-scoped slots: idempotent, self-expiring, with a MongoDB fallback.
- *Cache:* keyed by content hash (the PDF's pitfall is keying by `document_id`), with a MongoDB tier for outages.

**Ownership (PDF "Ownership")**

- *Decision:* 404 for non-owners, enforced inside every query, so no code path can distinguish "not yours" from "doesn't exist."
- *Assumption:* identity comes from an `X-User-Id` header (the PDF specifies no auth), isolated in one dependency so JWT verification can replace it.

## 3. End-to-end flow

1. **Submit.** Validate, normalise and hash the content.
   - Cache hit: insert as completed, no slot needed.
   - Otherwise acquire a slot (or 429), insert as `queued`, return 201.
2. **Claim.** A worker atomically takes the oldest due document and sets a lease.
3. **Processing.** Summary committed, version-stamped, only if still current; status becomes `enriching`.
4. **Enriching.** Tags committed only beside a same-version summary; status becomes `completed`, cache written, slot released.
5. **Failure.** Exponential-backoff retries, then `failed` with `failed_stage`; `/retry` resumes there.
6. **PATCH, any time.** One atomic write bumps the version, clears the result (kept as a labelled `stale_result`), and restarts the pipeline; in-flight workers are fenced out.
7. **Read.** `result` appears only for the current version; otherwise `result_freshness` is `stale` or `none`.

## 4. Scale (PDF "At 100×")

- **Hot user:** counts and deep `skip` grow linearly; a `user_id` shard key creates unsplittable jumbo chunks.
- **Shard key:** `{user_id: "hashed", _id: 1}`: even tenant spread, splittable heavy users, targeted queries (every query includes `user_id`).
- **Rate limiter:** survives; needs separate limiter and cache Redis (different eviction policies) and a local pre-filter for abusive users.
- **Pagination:** move to keyset cursors on `(created_at, _id)`.

## 5. Evidence of quality

- **75 tests** against real MongoDB and Redis, covering every PDF requirement and all six bonus items.
- A **concurrent stress test** asserts the invariant on every stored-document sample while workers and PATCHes race.
- `explain()` tests prove the by-ref lookup uses its index.
- Concurrency suites passed 15 consecutive runs; a real-process smoke test covered startup, a live retry, and graceful shutdown.
- Testing caught one real defect, now fixed: concurrent duplicate submissions of a new ref could get a spurious 429.

## 6. Points to prepare for the debrief

- The README's six-writer table: why each write filter makes a mixed pair unwritable.
- Why hashed `user_id` plus `_id`, not plain `user_id` or hashed `_id`.
- Why the crosswalk is last-writer-wins without `client_version` and strictly ordered with it.
- The Redis-outage trade-off: one extra job may slip through a tight race, preferred over rejecting all writes.
