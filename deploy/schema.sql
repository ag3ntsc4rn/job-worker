-- Local-dev schema for the compose stack.
--
-- The worker does NOT own this schema: in a real deployment the producer
-- creates it. This file exists so `docker compose up` has something to run, and
-- documents the exact contract the worker relies on:
--
--   jobs(id, job_type, status, payload, input_payload, updated_at)
--       'queued'|'dispatched' -> 'running' -> 'completed'|'failed'
--   job_type_config(job_type, payload)   -- optional per-type base config
--
-- Postgres runs everything in /docker-entrypoint-initdb.d once, on first boot.

CREATE TABLE IF NOT EXISTS jobs (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_type      TEXT        NOT NULL,
    status        TEXT        NOT NULL DEFAULT 'queued',
    input_payload JSONB       NOT NULL DEFAULT '{}'::jsonb,  -- per-run overrides, set at enqueue
    payload       JSONB       NOT NULL DEFAULT '{}'::jsonb,  -- effective config, snapshot at claim
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Base config per job type. A type with no row here is fine — it simply
-- contributes no keys, which is the right answer for a job needing no payload.
CREATE TABLE IF NOT EXISTS job_type_config (
    job_type TEXT PRIMARY KEY,
    payload  JSONB NOT NULL DEFAULT '{}'::jsonb
);

INSERT INTO job_type_config (job_type, payload) VALUES
    ('hello', '{"greeting": "hello", "name": "Ada"}'::jsonb)
ON CONFLICT (job_type) DO NOTHING;
