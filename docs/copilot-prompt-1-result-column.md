# Copilot Agent Prompt 1 — Add a `result` column to `jobs`

> Paste everything below the line into the Copilot agent. Adjust repo/module names to
> match your codebase where they differ from the names used here.

---

## Context

We run a Postgres-backed job engine with four services sharing one `jobs` table:

- **job-api** — enqueues jobs (`POST /jobs`, `GET /jobs/{id}`), owns the schema/migrations.
- **job-dispatcher** — transactional outbox → Kafka; publishes pointer envelopes `{"job_id", "job_type"}`.
- **job-worker** — consumes pointers, claims the job (`queued|dispatched -> running`), resolves the
  effective payload (`job_type_config.payload` overlaid by `jobs.input_payload`), runs the handler,
  then `running -> completed|failed`.
- **job-reaper** — requeues runs stuck in `running` past `run_timeout` (`running -> queued`,
  `attempts+1`, re-arms the outbox row); dead-letters at the churn cap (`running -> failed`).

Today a job carries only its status. Handlers cannot hand anything back to the caller.

## Goal

Add a nullable `result JSONB` column to `jobs` so a handler's return value is persisted when the run
completes, and expose it read-only through the API. This is a prerequisite for a follow-up task
(an LLM handler that returns structured JSON).

## Semantics (must hold everywhere)

1. `result` is set **in the same `UPDATE` as the `running -> completed` transition**. A row must
   never be `completed` with a stale result, or carry a fresh result while still `running`.
   ```sql
   UPDATE jobs SET status='completed', result=%s, updated_at=now()
   WHERE id=%s AND status='running'
   ```
2. `NULL` means "the handler produced nothing"; `'{}'::jsonb` means "an explicitly empty result".
   Keep the two distinguishable — do not coalesce NULL to `{}` in the DB, the store, or the API.
3. `failed` runs do **not** write a result. Error detail stays in logs (or a separate column later);
   `result` is only ever a success artefact.
4. When the **reaper requeues** a run (`running -> queued`), the previous partial result is
   irrelevant — the guarded update should also `SET result = NULL` so a later completion cannot be
   confused with a stale one. (If the claim guard already rewrites `payload`, this is the natural
   place to reason about it: everything a run produced is reset when the run is restarted.)
5. Handlers remain **idempotent and at-least-once**: a redelivered pointer for an already-completed
   job is skipped by the claim guard, so the result is never overwritten by a duplicate run.
6. The column is not part of the dedup index, `run_timeout` logic, or the outbox — nothing in the
   dispatcher's SQL should change beyond re-created local schema files.

## Work items, per repo

### job-api (schema owner)
- New migration, idempotent:
  ```sql
  ALTER TABLE jobs ADD COLUMN IF NOT EXISTS result JSONB;
  ```
  Also add the column to the canonical `CREATE TABLE IF NOT EXISTS jobs (...)` in the seed/dev
  schema with the comment `-- handler's result, set at completion; NULL = none produced`.
- Domain model: `Job.result: dict[str, Any] | None = None`.
- Store protocol: `complete(job_id: int, result: dict | None = None) -> bool` (guarded
  `running -> completed`). Postgres impl binds JSONB via the driver's JSON adapter
  (psycopg: `Jsonb(result)`), passing SQL `NULL` when `result is None`. In-memory impl deep-copies.
- `GET /jobs/{id}` (and list endpoints, if they return full rows): add `result` to the response
  schema as `dict | null`. `POST /jobs` must **not** accept `result` from callers (reject or ignore
  per the API's existing style for unknown fields).
- Tests: in-memory store `complete(id, {"a": 1})` stores the dict; `complete(id)` stores `None`;
  `complete` on a non-running row returns `False` and leaves `result` untouched; the GET response
  renders `result` (`null` when absent). Keep the coverage gate green.

### job-worker
- `Handler` type becomes `Callable[[Envelope], Any]`; a non-`None` return is the run's result.
  The registry's dispatch function returns the handler's return value.
- `process()`: `result = handler(envelope)` then `store.complete(job_id, result)`. Exceptions still
  → `fail(job_id)`; `CircuitOpenError` still propagates unmarked (reaper recovers the run).
- `JobStore.complete(job_id, result=None)` on the protocol, the guarded wrapper, the in-memory
  store (add a `result_of(job_id)` test helper), and the Postgres store (single-statement update
  above; generalise the internal `_guarded_update(sql, params: tuple)` if it currently takes only a
  `job_id`).
- Local `deploy/schema.sql`: add the column **and** the idempotent `ALTER TABLE` (the compose
  Postgres may already be initialised).
- Update test doubles that subclass the store (e.g. a `FlakyJobStore`) to the new signature.
- Tests: through `process()` with the in-memory store — a handler returning a dict yields
  `status == 'completed'` and `result_of(id) == dict`; a `None`-returning handler yields
  `result_of(id) is None`; a raising handler yields `failed` with no result.

### job-reaper
- Requeue SQL: add `result = NULL` to the guarded `running -> queued` update. Dead-letter update
  unchanged. Add the column to the local `deploy/schema.sql` used by compose.
- Test: requeuing a run that has a stale result clears it (in-memory store + Postgres SQL string
  assertion, matching how the repo already tests its SQL).

### job-dispatcher
- No logic change. Only re-sync `deploy/schema.sql` so the local stack matches the canonical schema.

## Ground rules
- One PR per repo, titled `Add jobs.result column (<repo role>)`; land job-api first, then worker,
  then reaper, then dispatcher.
- Do not change status names, the dedup partial unique index, outbox columns, or `attempts` logic.
- Run each repo's lint and tests (`ruff check .`, `pytest`; coverage gates are enforced) before
  opening the PR. For Postgres stores excluded from unit coverage, exercise them once against the
  compose Postgres (`docker compose up -d postgres`).
- No comments that describe the diff; short docstrings only where the semantics above are not
  obvious from the code (the NULL-vs-`{}` distinction is the one worth a comment).

## Definition of done
- `POST /jobs` → dispatcher publishes → worker runs a handler returning `{"answer": 42}` →
  `GET /jobs/{id}` shows `"status": "completed", "result": {"answer": 42}`.
- A run killed mid-flight is requeued by the reaper with `result` reset to `NULL` and completes
  normally on the second attempt.
- All four repos: lint + tests green.
