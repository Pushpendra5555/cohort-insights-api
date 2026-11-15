# Cohort Insights API

A backend service that ingests documents, runs them through a two-stage asynchronous pipeline (`processing` → `enriching`), and serves results to their owner and to a partner system that addresses documents by its own `client_doc_ref`.

**Stack:** Python 3.12 · FastAPI · MongoDB 7 · Redis 7 · Docker Compose

The central guarantee: **a reader can never observe a summary, tags, and content from different versions.** This holds by construction, not by timing, and is proven by the write-path argument in [Schema & Staleness Design](#schema--staleness-design). It is also checked by a concurrent stress test that inspects stored documents continuously.

---

## Contents

1. [Quick start](#quick-start)
2. [API reference](#api-reference)
3. [Architecture](#architecture)
4. [Schema & Staleness Design](#schema--staleness-design) *(required)*
5. [Crosswalk: repeat `client_doc_ref` policy](#crosswalk-repeat-client_doc_ref-policy)
6. [Pipeline, concurrency and failure handling](#pipeline-concurrency-and-failure-handling)
7. [Redis: rate limiting and content cache](#redis-rate-limiting-and-content-cache)
8. [Ownership](#ownership)
9. [Indexes](#indexes)
10. [At 100×](#at-100) *(required)*
11. [Assumptions](#assumptions)
12. [Testing](#testing)
13. [What I would do with more time](#what-i-would-do-with-more-time)
14. [Project layout](#project-layout)

---

## Quick start

**Prerequisites:** Docker with Compose v2.24 or newer (needed for the optional `.env` file syntax).

```bash
cp .env.example .env          # optional; every value has a default
docker compose up --build -d  # API, worker, MongoDB, Redis
curl localhost:8000/health    # {"status":"ok","checks":{"mongodb":"ok","redis":"ok"}}
```

Interactive OpenAPI docs are at <http://localhost:8000/docs>.

| Command | Purpose |
|---|---|
| `make up` / `make down` | Start / stop the stack |
| `make logs` | Follow API and worker JSON logs |
| `make scale` | Run three worker replicas (claims are race-safe) |
| `make test` | Run the test suite in a container against real MongoDB and Redis |
| `make clean` | Stop and delete the MongoDB volume |

### Walkthrough

Every request identifies its caller with `X-User-Id` (see [Assumptions](#assumptions)).

```bash
H='-H X-User-Id:alice -H Content-Type:application/json'

# 1. Submit (201). Optional client_doc_ref links the partner's identifier.
curl -s $H -X POST localhost:8000/documents -d '{
  "user_id": "alice", "title": "Energy note",
  "content": "Solar farms are expanding. Storage keeps grids stable.",
  "client_doc_ref": "cms-42"
}'
# {"document_id":"…","status":"queued","content_version":1,"outcome":"created","cache_hit":false}

# 2. Poll. Watch status move queued → processing → enriching → completed (~15–35 s).
curl -s $H localhost:8000/documents/<document_id>

# 3. Replace content in place; optional optimistic-concurrency precondition.
curl -s $H -X PATCH localhost:8000/documents/<document_id> \
  -d '{"content": "Wind turbines are getting taller.", "expected_content_version": 1}'

# 4. The partner resolves by its own identifier.
curl -s $H localhost:8000/documents/by-ref/cms-42

# 5. List, newest first, optionally filtered.
curl -s $H "localhost:8000/users/alice/documents?page=1&page_size=20&status=completed"
```

### Running tests without Docker

The tests need real MongoDB and Redis; no mocks are used for persistence.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
MONGO_URL=mongodb://localhost:27017 REDIS_URL=redis://localhost:6379/0 pytest -q
```

Each test uses its own database and Redis key namespace, so the suite is safe to run against a shared development instance.

---

## API reference

All errors share one envelope: `{"error": {"code", "message", "details"?}, "request_id"}`. Every response carries an `X-Request-Id` header, which also appears in the logs.

| Method & path | Success | Errors |
|---|---|---|
| `POST /documents` | **201** created · **200** repeat of a known `client_doc_ref` | 401, 403 (body `user_id` ≠ caller), 409 (crosswalk ordering), 422, 429 |
| `PATCH /documents/{id}` | **200** (`outcome`: `new_version` or `unchanged`) | 401, 404, 409 `version_conflict`, 422, 429 |
| `GET /documents/{id}` | **200** | 401, 404 |
| `GET /documents/by-ref/{client_doc_ref}` | **200** | 401, 404 |
| `GET /users/{user_id}/documents` | **200** (`page`, `page_size`, `status`) | 401, 404 (not the caller), 422 |
| `POST /documents/{id}/retry` | **202**, resumes from the failed stage | 401, 404, 409 `invalid_state`, 429 |
| `GET /health` | **200** `ok`/`degraded` · **503** if MongoDB is down | |
| `GET /health/live` | **200** liveness, no dependencies | |

### Document representation (`GET`)

```jsonc
{
  "document_id": "…", "user_id": "alice", "title": "…",
  "client_doc_ref": "cms-42", "client_version": null,
  "content_version": 2,                 // monotonic; +1 per content change
  "content_hash": "9f2c…",              // sha256 of the normalised content
  "status": "enriching",                // queued | processing | enriching | completed | failed
  "failed_stage": null,                 // processing | enriching when status = failed
  "stages": {
    "processing": {"status": "succeeded", "attempts": 1, "started_at": "…", "finished_at": "…",
                   "last_error": null, "next_attempt_at": null},
    "enriching":  {"status": "running",   "attempts": 1, "…": "…"}
  },
  "result_freshness": "stale",          // current | stale | none
  "result": null,                       // present ONLY if derived from content_version 2
  "stale_result": {                     // last complete result of an OLDER version
    "summary": "…", "tags": ["…"], "content_version": 1, "content_hash": "41ab…",
    "source": "pipeline", "produced_at": "…"
  },
  "created_at": "…", "updated_at": "…"
}
```

A caller tells whether a result is current by comparing `result.content_version` with `content_version`. The API only populates `result` when they are equal. Older output appears only as `stale_result`, which carries its own version number.

---

## Architecture

```
            ┌──────────────┐  POST/PATCH/GET   ┌──────────────────────────┐
 clients ──▶│ FastAPI (api)│──────────────────▶│ MongoDB                  │
 partner    │  routers     │  CAS on `rev`     │  documents (source of    │
            │  → services  │◀──────────────────│  truth + work queue)     │
            └──────┬───────┘                   └────────────▲─────────────┘
                   │ slots / cache                          │ claim (lease) +
                   ▼                                        │ fenced commits
            ┌──────────────┐   cache writes,   ┌────────────┴─────────────┐
            │ Redis        │◀──────────────────│ worker(s)                │
            │  rate-limit  │   slot release    │  N concurrent stage slots│
            │  content $   │                   │  per process, scalable   │
            └──────────────┘                   └──────────────────────────┘
```

**Layers.** Routers handle HTTP only. Services hold the business rules and raise domain errors, never HTTP exceptions. The repository is the only code that touches MongoDB. Pure domain functions build documents and decide freshness. One composition root (`app/bootstrap.py`) wires everything for both the API and the worker. Dependencies are injected through FastAPI `Depends`, and services receive their collaborators through their constructors.

**Why MongoDB is the job queue.** The API writes a document and its pending work in a single insert. A separate broker would create a dual-write problem: if the API wrote to MongoDB and then failed to enqueue (or the reverse), jobs would be lost or orphaned unless an outbox were added. Using MongoDB also means job state and document state can never disagree, because they are the same record. At the scale of this exercise, polling a small partial index is cheap. See [At 100×](#at-100) for when this stops being true.

---

## Schema & Staleness Design

### The mechanism

Two counters live on every document:

- **`content_version`** increases by one on every content change and is never reused.
- **`rev`** increases by one on *every* write. It is the compare-and-swap token for API-side writes.

Every derived field is stamped with the exact input it came from:

```
result.summary = { value, content_version, content_hash, … }
result.tags    = { value, content_version, content_hash, derived_from_summary_version, … }
```

A boolean `is_stale` with nothing to compare against is not used. Freshness is always a *comparison*: a stamped `content_version` and `content_hash` against the document's current ones.

### Invariant

For every stored document:

> **I1.** If `result.summary` exists, then `summary.content_version == content_version` and `summary.content_hash == content_hash`.
> **I2.** If `result.tags` exists, then `result.summary` exists and `tags.content_version == tags.derived_from_summary_version == content_version`.

If I1 and I2 hold for every stored state, a mismatched triple cannot be *stored*, and therefore cannot be read. MongoDB updates to a single document are atomic, and a read returns a whole document, so there are no torn reads between fields.

### Proof by enumeration of writers

Exactly six code paths write `content` or `result`. Each one preserves I1 and I2:

| # | Writer | Write filter (what MongoDB checks atomically) | Effect on `result` | Why I1/I2 hold |
|---|---|---|---|---|
| 1 | Insert | n/a | empty, or a cache pair stamped v1 | Both fields stamped with the document's own version |
| 2 | Content replacement (PATCH, ref resubmission) | `_id`, `user_id`, `rev == read rev` | **Cleared in the same update** that bumps v→v+1 (or a cache pair stamped v+1). The pipeline is reset with `lease_token = null`. | New content never coexists with an old field. The old complete pair moves to `previous_result` intact. |
| 3 | Stage-1 commit | `content_version == v` **and** `lease_token == t` | sets `summary` stamped v | Applies only if the document is still at v |
| 4 | Stage-2 commit | the above **and** `result.summary.content_version == v` | sets `tags` stamped v | Tags can land only beside a summary of the same version |
| 5 | Manual retry | `rev` CAS | does not touch `result` | Unchanged |
| 6 | Worker self-recovery | fenced as in row 3 | clears `tags`, restarts the pipeline | Clearing preserves the invariant |

Consider the race the assignment highlights: a PATCH lands while a worker is mid-stage on v1. The PATCH (row 2) bumps the version to 2 and nulls the lease token in one atomic write. The in-flight worker's commit (row 3 or 4) filters on `content_version: 1, lease_token: t`, matches zero documents, and is discarded. The v1 summary is computed but never stored.

### Defence in depth: the read gate

The API serialises `result` through exactly one function, `current_result()` in `app/domain/document.py`. It re-checks every stamp against the current version and hash. If a future bug ever violated the invariant, the gate *fails closed*: the reader gets `result: null` rather than a mixed pair, and an error is logged. The write path makes a mismatch impossible; the read gate makes it unobservable even if the write path were wrong.

### Safety never depends on time

Leases and `due_at` timestamps affect only *liveness*, meaning when work gets retried. They never affect *correctness*. A worker that stalls past its lease, clock skew between hosts, or a GC pause can at worst produce a commit that is fenced out. Correctness rests entirely on equality checks inside atomic single-document updates.

### Evidence

- `tests/invariants.py` asserts I1 and I2 on stored documents. It also asserts that the stored summary equals `make_summary(content)` and the tags equal `make_tags(summary)`, so it proves the output was *derived from* the current content, not just labelled with its version.
- `test_concurrent_stress_invariants_hold_at_every_observation` runs three workers with random latency and injected failures, about 30 random PATCHes, and a reader sampling every stored document continuously. Every observation must satisfy the invariant, and the final results must match the final contents.
- Targeted tests freeze a worker mid-`processing` and mid-`enriching`, PATCH underneath it, then release it and assert that its commit was discarded.

### What a reader sees during reprocessing

`result` is `null`. `result_freshness` is `"stale"` (with `stale_result` carrying `content_version: N-1`) or `"none"`. `status` and `stages` show exactly where the new run is.

---

## Crosswalk: repeat `client_doc_ref` policy

**Decision: a repeat submission of a known ref is a new version of the same document.** It is processed exactly like a PATCH and returns **200** with the existing `document_id`.

The reason: the partner's ref identifies *its* document. A second submission means "this document now looks like this". Creating a second document would break the one-ref-one-document lookup. Rejecting every repeat would break partner retries. Merging has no meaningful semantics for free text.

| Situation | Outcome |
|---|---|
| Content identical to current (by hash) | **200 `unchanged`**: an idempotent replay. Nothing is reprocessed. A safe retry. |
| Different content, no `client_version` on either side | **200 `new_version`**: last writer wins, in arrival order |
| `client_version` greater than stored | **200 `new_version`**, stored `client_version` updated |
| `client_version` lower than stored | **409 `stale_client_version`**: an out-of-order delivery is rejected |
| Same `client_version`, different content | **409 `stale_client_version`**: a conflicting payload for one version |
| Ref already versioned, request has no `client_version` | **409 `client_version_required`**: an unversioned write could reorder history |
| Concurrent first submissions of a new ref | Exactly one **201**; the rest become repeats (**200**) |

**Out-of-order delivery** is the case the PDF warns about. Without information from the partner, the server cannot know which of two messages is newer; arrival order is the only signal. So ordering is *opt-in*: a partner that sends a monotonic `client_version` gets strict ordering, enforced atomically because the check runs against the exact revision that the compare-and-swap then requires. A partner that does not gets documented last-writer-wins.

**Uniqueness scope.** `client_doc_ref` is unique **per owner**: a unique partial index on `(user_id, client_doc_ref)`. Global uniqueness would let user B's submission fail because user A holds the same ref. That 409 would confirm A's ref exists, the very existence leak the Ownership requirement forbids, and it would couple tenants that share a partner CMS.

All cases above are tested in `tests/test_crosswalk.py`.

---

## Pipeline, concurrency and failure handling

### Status model

```
queued ─▶ processing ─▶ enriching ─▶ completed
              │              │
              └──────────────┴──▶ failed  (failed_stage = processing | enriching)
```

The top-level `status` is the coarse position the PDF specifies. Per-stage detail lives in `stages.{processing,enriching}`: a status (`pending`, `running`, `retry_scheduled`, `succeeded`, `failed`, `cached`), plus attempts, timestamps, the last error, and the next attempt time. The caller always knows exactly which stage failed, without ad-hoc string fields being added.

### Execution

One unit of work is one stage of one document version: **claim → mark running → work → commit.**

- **Claim.** A single `find_one_and_update` on `pipeline.due_at <= now` pushes `due_at` forward by the lease duration and sets a fresh `lease_token`. Only one worker can win a given document; this is tested with ten concurrent claimers. Pushing `due_at` forward *is* the lease: if the worker dies, the job becomes due again automatically.
- **Fenced writes.** Every later write filters on `content_version` and `lease_token`. A superseded worker, whether replaced by a PATCH or by a peer that reclaimed an expired lease, writes nothing.
- **Partial progress.** The stage-1 summary is stored, version-stamped, before stage 2 starts. An enriching failure, retry, or crash resumes at `enriching` and never redoes `processing`. This is tested by asserting `processing.attempts == 1` after an enriching retry.
- **Retry with backoff.** A failed stage is rescheduled after exponential backoff with equal jitter (`base · 2^(n-1)`, capped, half fixed and half random). After `STAGE_MAX_ATTEMPTS` the document fails with `failed_stage` set. `POST /documents/{id}/retry` resumes from that stage.
- **Crash loops.** An attempt interrupted by a crash still counts toward the limit, so a job that repeatedly kills its worker eventually fails instead of looping forever.
- **Graceful shutdown.** On SIGTERM the worker stops claiming new work. Anything cut off is reclaimed when its lease expires.

### Concurrent PATCHes (optimistic concurrency)

- **With `expected_content_version`:** of two racing PATCHes, exactly one gets **200** and the other gets **409 `version_conflict`** with the current version in `details`.
- **Without it:** the compare-and-swap loop applies both in sequence (versions N+1 and N+2). The write that committed last is the stored content. Nothing is lost silently: each caller's response states the version its write became.

Both outcomes are tested.

### Identical content

A PATCH whose content hashes to the current content returns `unchanged` and does not reprocess. The output is a deterministic function of the content, so rerunning it would only consume capacity. Resuming a failed document is the job of the explicit `/retry` endpoint.

---

## Redis: rate limiting and content cache

The two concerns are kept separate: different key prefixes, different failure semantics, and different eviction needs (see [At 100×](#at-100)).

### Rate limiting: at most 3 documents in `{queued, processing, enriching}` per user

**Structure:** one sorted set per user, `{ns}:rl:active:{user_id}`. Each member is a **slot** `"{slot_base}:{content_version}"`, and its score is the slot's expiry time. All check-and-modify logic runs in **one Lua script**, so a burst of ten concurrent submissions yields exactly three 201s and seven 429s (tested).

Why slots rather than an `INCR`/`DECR` counter:

- **Idempotent.** Acquiring a held slot or releasing a freed one is a no-op. A counter drifts permanently after any double decrement, retried message, or crash between steps.
- **Self-healing.** Each acquire first purges expired members, and workers refresh the expiry while they hold a job. A slot leaked by a crashed process blocks a user for at most `RATE_LIMIT_SLOT_TTL_S`, never forever.
- **Version-scoped.** A PATCH of an active document *transfers* slot v → v+1 atomically, with no capacity check, because the document was already counted. A late release of slot v by a fenced-out worker therefore cannot free slot v+1 (tested).
- **Shared by duplicates.** For documents with a ref, `slot_base` is derived from `(user_id, ref)`, so concurrent duplicate first submissions share one slot. The test suite exposed this case: with random per-document slots, five simultaneous duplicates could wrongly receive 429 for what was really a replay.

**Policy choices:**

- **Cache hits bypass the limiter.** They never enter the pipeline, so they consume no processing capacity.
- **A PATCH of a completed document needs capacity**, because it re-enters the pipeline. A PATCH of an active document does not.

**Degradation:** if Redis is unreachable, the limiter counts active documents in MongoDB using the `user_state_created` index. That path is check-then-act and could admit one extra job under a tight race during an outage. I accepted that rather than rejecting every write. MongoDB, not Redis, is the source of truth for which documents are active.

### Content cache

**Key:** `{ns}:cache:result:{pipeline_version}:{sha256(content)}`. It is a function of content, never of a document id. A PATCH changes the hash and therefore the key, so the old content's entry cannot leak into the new content's result, or the reverse (tested). Including `pipeline_version` means changing the algorithm invalidates every entry without a flush. Every hit is re-verified against the requested hash.

**Two tiers:**

- **Redis** is the hot tier, written by the worker after the final commit, with a TTL of `CACHE_TTL_S`.
- **MongoDB** is the durable tier. On a Redis miss or outage, the service looks for any completed document with the same hash, using the `content_hash_state` index, and warms Redis from it. So the cache stays correct during a Redis outage and keeps working beyond the TTL.

Content is normalised before hashing (CRLF→LF, Unicode NFC), and the *normalised* text is stored, so the stored content always hashes to the stored hash. A cache hit on submit inserts the document directly as `completed`, with stages marked `cached` and `result.source = "cache"`. A cache hit on PATCH does the same within the single replacement write.

**Every Redis call** uses a 0.5 s timeout and catches only `RedisError` and `OSError`, logging a warning with context before falling back. Errors are never swallowed silently; there is no `except Exception: pass` anywhere in the codebase.

---

## Ownership

**Decision: 404 for another user's document**, which is indistinguishable from a document that does not exist.

Ownership is enforced *inside the query*: `find_one({"_id": id, "user_id": caller})`. No code path loads a document and then checks its owner, so no code path can leak existence. A test asserts that the non-owner, unknown-id, and malformed-id responses have the same status and the same error body (only the per-request `request_id` differs). The same rule applies to PATCH, retry, by-ref lookup, and listing another user's documents.

**One deliberate 403:** `POST /documents` with a body `user_id` different from the caller. It reveals nothing about any existing resource, and it tells an integrator precisely what is wrong.

---

## Indexes

Created idempotently at startup (`app/repositories/documents.py`):

| Name | Keys | Serves |
|---|---|---|
| `user_created` | `{user_id: 1, created_at: -1, _id: -1}` | Listing without a filter, newest first, with a stable tie-break |
| `user_state_created` | `{user_id: 1, "pipeline.state": 1, created_at: -1, _id: -1}` | Listing with a status filter; the limiter's MongoDB fallback count |
| `user_client_ref_unique` | `{user_id: 1, client_doc_ref: 1}`, **unique**, partial on `client_doc_ref: {$type: "string"}` | **`GET /documents/by-ref/{ref}`** and ref uniqueness. Documents without a ref are excluded from the index, so they never collide. |
| `content_hash_state` | `{content_hash: 1, "pipeline.state": 1}` | The cache's durable tier |
| `pipeline_due` | `{"pipeline.due_at": 1}`, partial on `due_at` being a date | Worker claims. The index holds only the backlog, not the whole collection. |

The by-ref query restates the partial-index predicate (`$type: "string"`) so the query planner can prove the index applies. Tests run `explain()` and assert that the winning plan is an `IXSCAN` on the named index, never a `COLLSCAN`, and that listing involves no in-memory `SORT`.

---

## At 100×

**500K documents for one user.** The `(user_id, created_at, _id)` index still serves page 1 well: it is one seek plus `page_size` keys. Three things break. First, `count_documents` for `total` walks all 500K index keys on *every* listing request. Second, deep `skip` values walk every skipped key. Third, if the collection were sharded on `user_id`, that user's 500K documents share one shard-key value. The chunk cannot split (a "jumbo" chunk), so one shard takes the user's entire read and write load.

**Shard key: `{user_id: "hashed", _id: 1}`** (a compound hashed key).

- Hashing `user_id` spreads tenants evenly and stops a burst of sign-ups from hitting one shard.
- The `_id` suffix lets a heavy user's range split across chunks, which fixes the jumbo-chunk problem.
- Every query already includes `user_id`, because ownership filters are part of the query. Point reads, by-ref lookups and listings therefore stay *targeted* to one shard rather than broadcast to all of them.

Why not plain `user_id`: the jumbo chunk and hot shard just described. Why not hashed `_id`: every listing becomes a scatter-gather across all shards. Two consequences follow:

- **Crosswalk uniqueness moves to its own collection.** MongoDB requires a unique index to be prefixed by the full shard key, and `(user_id, client_doc_ref)` is not. So uniqueness would move to a small `client_refs` collection whose `_id` is `"{user_id}:{ref}"`, inserted first so that `_id` uniqueness enforces it.
- **The polling queue must leave MongoDB.** The `due_at` claim query would fan out to every shard. Work would move to Redis Streams or Kafka partitioned by document, fed by a transactional outbox so the dual-write problem stays solved. The lease token remains the fencing mechanism.

**Rate limiter at 100× submit QPS.** The design survives. It is one Lua call per submit on a tiny sorted set, and each script touches a single key, so it is Redis Cluster-compatible and shards by user. I would change three things:

- **Separate Redis deployments for limiter and cache.** The limiter needs `noeviction` (an evicted slot set means an under-count). The cache wants `allkeys-lru`. One instance cannot have both policies.
- **A per-instance in-memory token bucket in front of the Lua call.** One abusive user hammering a single key would otherwise saturate one cluster slot.
- **Rethink the MongoDB fallback.** At 100× it would turn a Redis outage into a MongoDB count storm. I would make it fail-closed for new work, or apply a local per-process cap.

**Pagination.** `skip`/`limit` costs O(offset) and shifts under concurrent inserts: new documents push items onto the next page and cause duplicates. I would switch to **keyset (cursor) pagination**: an opaque cursor encoding the last item's `(created_at, _id)`, and a query for `(created_at, _id) < cursor`, sorted descending, with `limit n+1`. That is O(page size) on the *same* index and stable under inserts. I would replace the exact `total` with `has_next`, or with an asynchronously maintained per-user counter.

---

## Assumptions

Each assumption fills a gap the PDF leaves open, as its Guidelines invite.

| # | Assumption | Justification |
|---|---|---|
| A1 | The caller is identified by an `X-User-Id` header. Missing or malformed gives 401. | The PDF requires ownership but specifies no auth. The header stands in for a gateway-verified JWT subject. Only `app/api/deps.py` would change. |
| A2 | The partner system calls on behalf of a user, sending that user's `X-User-Id`. | The ownership rule says only the submitter may read a document, so by-ref lookups must be scoped too. |
| A3 | `client_doc_ref` is unique per owner. | Global uniqueness leaks existence across tenants (see Crosswalk). |
| A4 | The content cache is global across users. | Results are pure functions of content, so sharing reveals nothing the submitter lacks. A residual timing side channel exists ("someone submitted this exact text before"). Scoping per user is a one-line key change if that matters. |
| A5 | Identical-content PATCHes and resubmissions do not reprocess. | Output is deterministic; reprocessing would only burn capacity. `/retry` exists for failed documents. |
| A6 | Automatic retries: 3 attempts per stage. | The PDF lists "retry with backoff" as a bonus. With a 10% failure rate per attempt, terminal failure becomes about 0.1% per stage. Set `STAGE_MAX_ATTEMPTS=1` to see raw PDF failure rates. |
| A7 | A summary alone is never exposed as `result`. | The PDF says summary and tags are returned "once complete". Exposing half a result invites exactly the partial reads the design forbids. Partial progress is visible through `stages`. |
| A8 | `page_size` above the maximum is clamped, not rejected. | Friendlier to clients; the effective size is echoed in the response. |
| A9 | Content is capped at 100,000 characters and titles at 300. | Bounds memory per request and document size. The PDF warns against "no input validation beyond Pydantic defaults". |
| A10 | There is no delete endpoint. | Not in the specification. This keeps the mapping from `(user, ref)` to a document permanent. |

---

## Testing

**75 tests**, all against real MongoDB and Redis, using `pytest` and `httpx.AsyncClient` over the ASGI app. The worker is driven step by step (`await worker.run_once()`) with zero latency and scripted failures, so race interleavings are deterministic rather than dependent on sleeps.

| File | Covers |
|---|---|
| `test_staleness.py` | PATCH between stages and mid-stage (worker frozen); stale tags rejected; labelled `stale_result`; racing PATCHes with and without a precondition; concurrent stress test |
| `test_crosswalk.py` | Every row of the crosswalk policy table; per-owner isolation; concurrent first submissions; `explain()` index assertions |
| `test_pipeline.py` | Stage visibility; failure attribution; partial progress; manual retry; backoff scheduling; double-claim race; lease expiry and fencing; crash loops |
| `test_rate_limit.py` | Limit across all three active states; concurrency burst; cache bypass; slot transfer; stale-release safety; leak expiry; Redis outage fallback |
| `test_cache.py` | Immediate result; normalisation; keyed by content across PATCH; PATCH to known content; MongoDB tier after TTL; Redis outage |
| `test_api.py` | Status codes; validation; 401/403/404 behaviour; identical 404s; pagination; status filter; end-to-end flow |
| `test_domain_and_health.py` | Pure functions; the read gate failing closed; replacement atomicity; config validation; health; request ids |

The concurrency suites were run 15 times consecutively without a failure. The full stack was also smoke-tested as real processes (uvicorn plus the worker entrypoint), including a live random failure and retry and a clean SIGTERM shutdown.

---

## What I would do with more time

- **Real authentication.** JWT verification at the edge, with the user derived from the token's subject claim.
- **Push instead of poll.** Replace worker polling with change streams or a Redis Streams notification. Add webhooks or server-sent events so clients stop polling `GET`.
- **Keyset pagination now**, rather than only describing it for scale.
- **Metrics and tracing.** OpenTelemetry traces across API → MongoDB → worker; metrics for queue depth, stage latency histograms, retry and failure rates, cache hit ratio, and 429 rate.
- **A slot reconciler.** A periodic job that rebuilds each user's limiter set from MongoDB, turning "self-healing within the TTL" into "self-healing within a minute".
- **Content history.** A capped list of `{version, hash, updated_at}`, so partners can audit ref changes and the out-of-order policy can detect replays of superseded content even without `client_version`.
- **Property-based testing** (Hypothesis) over operation sequences, generalising the stress test into model checking.
- **Tooling.** CI with ruff, mypy `--strict`, and the containerised test suite; an image vulnerability scan.

---

## Project layout

```
app/
  main.py                 FastAPI factory: lifespan, middleware, error handlers
  bootstrap.py            composition root shared by the API and the worker
  core/                   config (env-based), JSON logging, domain errors
  domain/
    document.py           schema builders, replacement update, read gate, slot identity
    enums.py              pipeline/stage states, freshness, outcomes
    hashing.py            normalisation + sha256
    processing.py         deterministic mock stages, failure injection, backoff
  repositories/documents.py   indexes, CAS writes, fenced writes, atomic claim
  services/
    documents.py          submit, crosswalk policy, PATCH, retry
    rate_limiter.py       Lua slot limiter + MongoDB fallback
    cache.py              two-tier content-hash cache
  worker/
    pipeline.py           claim → run → fenced commit, retries, recovery
    main.py               process entrypoint with graceful shutdown
  api/
    deps.py               dependency injection, caller identity
    schemas.py            Pydantic request/response models, read mapping
    routes/               documents, users, health
tests/                    integration + unit tests; invariants.py
docs/requirements-traceability.md   each PDF requirement → code → test
SUMMARY.md                assessment analysis and flow
```
