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
if not store.claim(envelope.job_id):    # queued|dispatched -> running, guarded
    return "skipped"                    # redelivery, or another worker owns it
try:
    handler(envelope)
except Exception:
    store.fail(envelope.job_id); return "failed"
store.complete(envelope.job_id); return "completed"
```

Consequences worth knowing:

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
* **A poison message is committed past.** An unparseable message can never
  succeed, so it is logged and skipped rather than wedging the partition
  forever.

## Writing a real handler

Replace `always_succeeds` in [`src/worker/handlers.py`](src/worker/handlers.py):

```python
def my_handler(envelope: Envelope) -> None:
    ...          # return -> completed;  raise -> failed
```

Two properties any handler needs: **idempotent**, since at-least-once delivery
plus reaper re-queues mean the same job can arrive twice; and **bounded**, since
a handler that hangs holds the run in `running` until the reaper reclaims it.

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
jobs(id, status, updated_at)  -- 'queued'|'dispatched' -> 'running' -> 'completed'|'failed'
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
| `src/worker/handlers.py` | The handler contract and the generic no-op handler |
| `src/worker/store.py` | `JobStore` protocol, guarded wrapper, in-memory double |
| `src/worker/consumer.py` | `Consumer` protocol, guarded wrapper, in-memory double |
| `src/worker/db.py` | `PostgresJobStore` — the guarded status updates |
| `src/worker/messaging.py` | `KafkaConsumer` — manual-commit consumer |
| `src/worker/resilience.py` | Retry policy and circuit breaker |
| `src/worker/wiring.py` | Builds the two guards from config |
| `src/worker/__main__.py` | Entrypoint: `python -m worker` |
| `tests/` | Unit suite, at the root and importing the package by name |
