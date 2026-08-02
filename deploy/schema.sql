-- Local-dev schema for the compose stack.
--
-- The worker does NOT own this schema: in a real deployment the producer
-- creates `jobs`. This file exists so `docker compose up` has something to run,
-- and documents the exact contract the worker relies on:
--
--   jobs(id, status, updated_at)  -- 'queued'|'dispatched' -> 'running' -> 'completed'|'failed'
--
-- Postgres runs everything in /docker-entrypoint-initdb.d once, on first boot.

CREATE TABLE IF NOT EXISTS jobs (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_type    TEXT        NOT NULL,
    status      TEXT        NOT NULL DEFAULT 'queued',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
