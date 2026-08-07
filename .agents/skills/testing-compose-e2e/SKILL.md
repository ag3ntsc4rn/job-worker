---
name: testing-compose-e2e
description: How to test the job-worker service end-to-end on its docker compose stack (Kafka consumer, Postgres claim guard, payload resolution, outage/breaker scenarios). Use when verifying runtime behaviour rather than unit tests.
---

# End-to-end testing of job-worker on docker compose

This service is a headless Kafka consumer — there is no UI, so "testing" means the compose stack,
the `seed` CLI, `psql`, the Kafka CLI tools, and container logs. Do not record a screen video.

## Bring the stack up

```bash
# Ports 5432/9092 are published, so tear down any sibling stack first:
cd ../job-dispatcher && docker compose down -v   # likewise job-reaper, job-pusher
cd ../job-worker
docker compose up --build -d      # postgres + kafka + worker
docker compose build seed         # `seed` is behind the `cli` profile, so `up --build` SKIPS it
docker compose run --rm seed <job_type> ['{"json":"overrides"}']
```

`docker compose build seed` matters on a warm box: a profile-gated service keeps its stale image
through `up --build`, so a code change you are trying to test may silently not be in the seed image.
Verify with `docker image inspect job-worker-seed -f '{{.Config.Env}}'` or similar.

A `kafka-source attempt 1 failed ... UNKNOWN_TOPIC_OR_PART` warning right after startup is benign
(the topic is auto-created on first produce). Do not treat "kafka-source retry warnings exist" as
proof of an outage — assert on `circuit is open` lines instead, which name the breaker.

## Routing / handler changes are unreachable out of the box

`src/worker/__main__.py` wires `always_succeeds` **directly**, so `by_job_type` is never called by a
plain `docker compose up`. To test anything about routing (per-type handlers, the `unhandled`
fall-through, a custom `default=`) you must edit the `run(...)` call, e.g.

```python
by_job_type({"hello": always_succeeds})                                 # unmapped type -> failed
by_job_type({"hello": always_succeeds}, default=lambda envelope: None)  # unmapped type -> completed
```

then `docker compose up --build -d worker`. Treat this as scaffolding: `git checkout
src/worker/__main__.py` and rebuild before finishing, and report the edit.

## Useful assertions

```bash
# row state, incl. the payload snapshot the claim resolved
docker compose exec -T postgres psql -U app -d app -c \
  "select id,job_type,status,input_payload,payload from jobs order by id"

# was the message committed? LAG 0 == committed past. This is the assertion that distinguishes
# "failed and moved on" from "failed and stuck in a retry loop".
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group workers

# hand-craft any message (duplicates, nonexistent job_id, garbage) — the seed CLI always
# inserts a NEW row, so it cannot produce a duplicate envelope
echo '{"job_id": 8, "job_type": "hello"}' | docker compose exec -T kafka \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic jobs

# double-run check across replicas (any count > 1 is a bug)
for c in 1 2 3; do docker logs job-worker-worker-$c 2>&1 |
  grep -oE 'job [0-9]+ \([a-z0-9]+\) completed'; done | sort | uniq -c | awk '$1>1'
```

## Making outage scenarios actually trip

Use a temporary `docker-compose.override.yml` (delete it afterwards) with README-documented knobs:
`RETRY_MAX_ATTEMPTS=2 BREAKER_FAILURE_THRESHOLD=2 BREAKER_RESET_TIMEOUT=20 BREAKER_OPEN_SLEEP=5`.

Gotchas that will otherwise make you think the code is broken:

* **A single job cannot open the Postgres breaker.** After a failed iteration the message is left
  uncommitted, but librdkafka's read position has already advanced, so it is *not* re-polled until a
  restart or rebalance — the loop just goes idle. Deliver **several** pointer messages while Postgres
  is down (one breaker failure per failing claim) to reach the threshold.
* **Each failing claim takes ~60s** (psycopg pool timeout 30s × retries), so budget several minutes.
* **Seeding needs Postgres.** To get a message waiting while Postgres is down, insert the row with
  `psql` first, stop Postgres, then hand-produce the pointer with `kafka-console-producer.sh`.
* **To prove "no job lost / offset not committed"**, restart the worker after the dependency returns:
  redelivery is what makes the job run. The row going `queued -> completed` after the restart is the
  assertion.
* **A Kafka outage may be completely silent** at service level: librdkafka's `poll()` returns `None`
  rather than raising, so no `kafka-source` retry/breaker lines appear. Assert breaker *isolation* by
  counting `postgres-jobs circuit is open` lines before and after the outage instead.
* **Postgres down at worker startup**: the pool retry loop may not survive its first failure
  (`pool has already been opened/closed and cannot be reused`). The decisive test is to start the
  worker with Postgres down and bring Postgres **up mid-retry** — if the worker still exits non-zero,
  the retry loop is ineffective regardless of how long it appeared to wait.

## Poison-message shapes are not equivalent

Test both, they take different code paths:

* `{"nonsense": true}` — valid JSON, not a pointer → `MalformedEnvelope` in `Envelope.parse`,
  outcome `malformed`, offset **committed** past it.
* `not-json-at-all` — `json.loads` fails inside `KafkaConsumer.poll`, *under the kafka-source guard*,
  so it never becomes `malformed` and is **never committed** (LAG stays ≥1; re-read on every restart).
  If a "poison messages are committed past" claim is being verified, this shape may break it.

## Devin Secrets Needed

None. Everything runs locally against the compose stack; credentials (`app:app`) are in
`docker-compose.yml`.
