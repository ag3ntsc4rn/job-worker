# job-worker

A standalone **job worker**: it consumes job pointers from Kafka, claims each run
with a compare-and-set, hands it to a handler, and records `completed` or
`failed`. The handler shipped here is generic — it does no processing and always
succeeds — so this is a working skeleton to graft real work onto.

```
Kafka topic ──worker──> claim (queued|dispatched -> running) ──> handler ──> completed | failed
```

## What one message costs

A message is a pointer, not the work: `{"job_id": 7, "job_type": "hello"}`. The
job row stays the single source of truth, so a redelivery from three hours ago
still runs against current data rather than a stale copy baked into Kafka.

```python
envelope = Envelope.parse(message)      # not a pointer -> "malformed", commit past it
payload = store.claim(envelope.job_id)  # queued|dispatched -> running, + resolve config
if payload is None:
    return "skipped"                    # redelivery, or another worker owns it
try:
    handler(envelope.with_payload(payload))
except Exception:
    store.fail(envelope.job_id); return "failed"
store.complete(envelope.job_id); return "completed"
```

Consequences worth knowing:

* **The payload is resolved at claim time, not at enqueue.** The claim returns
  the run's effective config (see below); a payload baked into the message would
  mean a redelivery three hours later runs against a stale copy.
* **The claim is the only thing preventing a double-run.** Delivery is
  at-least-once, so `claim()` — `UPDATE ... WHERE status IN ('queued',
  'dispatched')` judged by `rowcount` — is what makes the second delivery a
  no-op. It runs *before* the handler, never after.
* **The offset is committed after the database write, never before.** A crash in
  between redelivers the message and the claim guard skips it; committing first
  would drop the job outright. That ordering is the reason auto-commit is off.
* **`queued` is claimable as well as `dispatched`.** A fast worker can beat the
  dispatcher's own mark-dispatched update to the row; refusing to claim `queued`
  would strand exactly those runs.
* **Safe to scale out.** Replicas share one consumer group, so Kafka splits the
  partitions between them, and the claim guard covers the redeliveries a
  rebalance produces: `docker compose up --scale worker=3`.
* **A dependency outage is not a business failure.** If the handler raises
  because a circuit is open, the run is left `running` rather than marked
  `failed`, and the message stays uncommitted — the reaper's `run_timeout` is
  what eventually reclaims it.
* **A poison message is committed past.** A message that can never succeed is
  logged and skipped rather than re-read forever. That covers both shapes:
  valid JSON that is not a job pointer, and bytes that are not JSON at all —
  the latter fails in the consumer's decode, so it is raised as
  `MalformedEnvelope`, which the `kafka-source` guard neither retries nor counts
  as a broker failure.

## Job payloads

A handler gets its configuration from `envelope.payload`, resolved by the claim
as a shallow merge — the type's base config overlaid with the run's overrides,
run wins:

```sql
payload = COALESCE(job_type_config.payload, '{}') || jobs.input_payload
```

| Source | Set by | Meaning |
| --- | --- | --- |
| `job_type_config.payload` | operator | the type's defaults — the master set of keys |
| `jobs.input_payload` | producer, at enqueue | this run's overrides (audit of what was asked for) |
| `jobs.payload` | worker, at claim | the effective result, snapshotted for debugging |

Both sides are optional. A type with **no** `job_type_config` row resolves to
`{}` and runs normally — plenty of jobs need no payload at all, and a missing
config row is not an error. So a handler that *does* need a key should read it
directly (`envelope.payload["recipient"]`); the resulting `KeyError` fails that
run with a clear traceback rather than silently doing the wrong thing.

Merging happens inside the claim's `UPDATE ... RETURNING payload`, so the
snapshot and the status change can't disagree, and it costs no extra round trip.

```bash
docker compose run --rm seed report '{"rows": 5000}'   # job_type, then input_payload
```

## Adding a handler

A handler is a function of one `Envelope`. **Return** marks the run `completed`;
**raise** marks it `failed`. That is the whole contract — the worker does the
claiming, recording, retrying and committing around it.

### 1. Write it

In [`src/worker/handlers.py`](src/worker/handlers.py):

```python
def send_report(envelope: Envelope) -> None:
    recipient = envelope.payload["recipient"]     # missing -> KeyError -> failed
    rows = envelope.payload.get("rows", 100)      # optional, with a default
    email.send(recipient, build_report(rows), timeout=30)   # always bounded
```

`envelope.payload` is the type's config merged with this run's overrides — see
[Job payloads](#job-payloads). Per-*deployment* settings (URLs, credentials)
belong in `config.py` instead, so they fail at startup rather than per message.

### 2. Wire it up

One type — pass it where `always_succeeds` is today in
[`src/worker/__main__.py`](src/worker/__main__.py):

```diff
-run(store, consumer, always_succeeds, ...)
+run(store, consumer, send_report, ...)
```

Several types — route on `job_type`:

```python
run(store, consumer, by_job_type({
    "send_report": send_report,
    "reindex": reindex,
}), ...)
```

An unmapped type falls through to `unhandled`, which logs at ERROR and raises
`UnknownJobType` — so the run is marked `failed`. Every type on this table is one
this worker is expected to own, so an unknown one is a bug (a typo'd `job_type`,
or a handler nobody registered) and completing the run would report success for
work that never happened.

The exception is a `jobs` table shared by two worker deployments that handle
disjoint types: there, whichever claims a row first would fail the other's runs,
so pass a `default=` that simply returns. Note this is about the **table**, not
the topic — a second deployment reading the same topic against its own database
finds no such `job_id`, loses the claim, and skips the message before any handler
runs.

### 3. Test it

Through `process()`, against the in-memory store — the status transition *is*
the contract, so that's the assertion:

```python
def test_a_missing_recipient_fails_the_run():
    store = InMemoryJobStore()
    job_id = store.add("queued")

    outcome = process(store, send_report, {"job_id": job_id, "job_type": "send_report"})

    assert (outcome, store.status_of(job_id)) == ("failed", "failed")
```

Then end-to-end: `docker compose run --rm seed send_report` and watch the row.

### What a handler must be

* **Idempotent.** Delivery is at-least-once and the reaper re-queues stranded
  runs, so the same job can arrive twice. Key side effects on `envelope.job_id`
  (an upsert, a conditional insert, an idempotency key on the outbound call)
  rather than assuming one delivery.
* **Bounded.** A handler that hangs holds the run in `running` until the reaper's
  `run_timeout` reclaims it, which stalls that job type meanwhile. Pass explicit
  timeouts to every network call.

### When to raise, and when not to

Raising is **terminal**: `failed` is a final status and nothing retries it — the
next schedule enqueues a fresh run. So raise for a genuine business failure (bad
input, rejected downstream), not for a blip you expect to clear.

For a transient failure you have two better options: retry inside the handler
(wrap your dependency in a `Guard` from
[`resilience.py`](src/worker/resilience.py), same as the store and consumer), or
let the run stay `running` — `process()` deliberately re-raises `CircuitOpenError`
without marking the run failed, so an open circuit leaves the message
uncommitted and the reaper reclaims the run.

## Resilience

Kafka and Postgres fail independently, so each sits behind its own
[`Guard`](src/worker/resilience.py) — retry with exponential backoff for
transient blips, wrapped in a `pybreaker` circuit breaker for sustained outages:

| | retries | breaker |
| --- | --- | --- |
| Kafka source | `RETRY_MAX_ATTEMPTS` per poll/commit | `kafka-source` |
| Postgres jobs | `RETRY_MAX_ATTEMPTS` per claim/complete/fail | `postgres-jobs` |

Retries run **inside** the breaker: a call counts as one breaker failure only
once its retries are exhausted, and while the breaker is open calls are rejected
immediately instead of sleeping through a backoff schedule against a dependency
that is known to be down. The loop then pauses for `BREAKER_OPEN_SLEEP` rather
than hot-looping, and a trial call after `BREAKER_RESET_TIMEOUT` closes it again.
Separate breakers mean a broker outage never quarantines the database.

Nothing is lost to a failed iteration: the message was not committed, so it
comes back.

Shutdown is graceful: `SIGTERM`/`SIGINT` set a flag the loop checks between
messages, so a deploy does not tear down a run in flight.

Logging goes through `logging.getLogger(__name__)` and the library installs no
handlers: `python -m worker` calls `basicConfig(level=LOG_LEVEL)`, and an
embedding application configures its own formatting.

## Configuration

All via environment variables:

| Variable | Default | Meaning |
| --- | --- | --- |
| `DATABASE_URL` | `postgresql://app:app@localhost:5432/app` | Postgres holding the jobs |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Broker list |
| `KAFKA_TOPIC` | `jobs` | Topic to consume |
| `CONSUMER_GROUP` | `workers` | Shared by all replicas, so partitions are split |
| `WORKER_POLL_TIMEOUT` | `1.0` | Seconds a poll waits before the loop re-checks shutdown |
| `RETRY_MAX_ATTEMPTS` | `3` | Attempts per dependency call |
| `RETRY_BACKOFF_BASE` | `0.2` | First backoff, doubling each attempt |
| `RETRY_BACKOFF_MAX` | `5.0` | Backoff cap |
| `BREAKER_FAILURE_THRESHOLD` | `5` | Failures before a breaker opens |
| `BREAKER_RESET_TIMEOUT` | `30.0` | Cooldown before the trial call |
| `BREAKER_OPEN_SLEEP` | `5.0` | Loop pause while a breaker is open |
| `LOG_LEVEL` | `INFO` | Root log level, applied only by `python -m worker` |

## Schema contract

The worker does **not** own the schema; the producer does. It requires only:

```sql
jobs(id, job_type, status, input_payload, payload, updated_at)
    -- 'queued'|'dispatched' -> 'running' -> 'completed'|'failed'
job_type_config(job_type, payload)   -- optional; a missing row resolves to {}
```

[`deploy/schema.sql`](deploy/schema.sql) provides that for local dev only.

## Run it locally

```bash
docker compose up --build -d          # postgres + kafka + worker
docker compose run --rm seed hello    # producer+dispatcher stand-in: one job, one message
docker compose logs -f worker         # INFO worker.service: job 1 (hello) completed
```

`seed` sits behind the `cli` profile, so `up --build` skips it; run `docker compose build seed`
after changing the image if you already have a stale one.

Check the outcome landed on the row:

```bash
docker compose exec postgres psql -U app -d app -c "SELECT id, job_type, status FROM jobs"
```

## Run it against your own infrastructure

Nothing about the service is Docker-specific — point it at a reachable Postgres
and broker:

```bash
pip install .        # or: pip install -r requirements.txt, then PYTHONPATH=src
DATABASE_URL=postgresql://user:pass@db.host:5432/app \
KAFKA_BOOTSTRAP_SERVERS=broker.host:9092 \
python -m worker     # equivalently, the installed `job-worker` command
```

## Develop

```bash
pip install -r requirements-dev.txt
ruff check .
pytest               # unit tests against in-memory doubles; 90% coverage gate
```

The package lives under `src/`, so the repo root is not importable and the tests
can only reach `worker` the way a deployment does. `pytest` handles this via
`pythonpath = ["src"]`; anything else wants `pip install -e .` or `PYTHONPATH=src`.
`src/main.py` exists for the `cd src && python main.py` form, which works because
a script's own directory goes on `sys.path`.

The unit suite needs no Postgres or Kafka: `InMemoryJobStore` mirrors the guarded
transitions, `InMemoryConsumer` replays messages and redelivers uncommitted ones,
and the failure-injecting doubles in `tests/doubles.py` drive the retry and
breaker paths.

## Layout

| Path | Role |
| --- | --- |
| `src/worker/service.py` | Claim/run/record logic and the consume loop (pure) |
| `src/worker/handlers.py` | The handler contract, the generic no-op handler, per-type routing |
| `src/worker/store.py` | `JobStore` protocol, guarded wrapper, in-memory double |
| `src/worker/consumer.py` | `Consumer` protocol, guarded wrapper, in-memory double |
| `src/worker/db.py` | `PostgresJobStore` — the guarded status updates |
| `src/worker/messaging.py` | `KafkaConsumer` — manual-commit consumer |
| `src/worker/resilience.py` | Retry policy and circuit breaker |
| `src/worker/wiring.py` | Builds the two guards from config |
| `src/worker/__main__.py` | Entrypoint: `python -m worker` |
| `tests/` | Unit suite, at the root and importing the package by name |
