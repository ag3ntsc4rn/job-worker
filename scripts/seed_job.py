"""Producer + dispatcher stand-in for the compose stack.

Inserts a `queued` job row and publishes its pointer envelope to the topic —
i.e. exactly what job-dispatcher would have done — so the worker has something
to consume.

    python scripts/seed_job.py [job_type] ['{"per": "run overrides"}']

The optional second argument is the run's `input_payload`, which the worker
overlays on the type's `job_type_config.payload` when it claims the job.
"""

from __future__ import annotations

import json
import os
import sys

import psycopg
from confluent_kafka import Producer


def main() -> int:
    job_type = sys.argv[1] if len(sys.argv) > 1 else "hello"
    input_payload = json.loads(sys.argv[2]) if len(sys.argv) > 2 else {}
    database_url = os.environ.get("DATABASE_URL", "postgresql://app:app@localhost:5432/app")
    brokers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.environ.get("KAFKA_TOPIC", "jobs")

    with psycopg.connect(database_url, autocommit=True) as conn:
        job_id = conn.execute(
            "INSERT INTO jobs (job_type, status, input_payload) "
            "VALUES (%s, 'queued', %s) RETURNING id",
            (job_type, json.dumps(input_payload)),
        ).fetchone()[0]

    envelope = {"job_id": job_id, "job_type": job_type}
    producer = Producer({"bootstrap.servers": brokers})
    producer.produce(topic, key=str(job_id), value=json.dumps(envelope).encode())
    producer.flush(30)

    print(f"queued job {job_id} ({job_type}) and published {envelope} to {topic}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
