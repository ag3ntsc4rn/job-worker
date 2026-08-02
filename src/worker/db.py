"""Postgres-backed ``JobStore``.

Every transition is a single guarded ``UPDATE ... WHERE status = <expected>``
and is judged by ``rowcount``: that is what makes a redelivered message a no-op
rather than a second run, without any application-side locking. ``claim``
additionally snapshots the effective payload into ``jobs.payload`` and returns
it, so winning the claim and resolving the config are one atomic step.

This module needs a real Postgres, so it is exercised by docker-compose rather
than the unit suite and is excluded from coverage.
"""

from __future__ import annotations

import time
from typing import Any

from psycopg_pool import ConnectionPool


class PostgresJobStore:
    def __init__(self, database_url: str, *, max_retries: int = 30, retry_delay: float = 2.0):
        self._pool = ConnectionPool(database_url, min_size=1, max_size=5, open=False)
        self._connect_with_retry(max_retries, retry_delay)

    def _connect_with_retry(self, max_retries: int, retry_delay: float) -> None:
        last_err: Exception | None = None
        for _ in range(max_retries):
            try:
                self._pool.open(wait=True, timeout=5)
                return
            except Exception as err:  # noqa: BLE001 - retry until Postgres is ready
                last_err = err
                time.sleep(retry_delay)
        raise RuntimeError(f"could not connect to Postgres: {last_err}")

    def claim(self, job_id: int) -> dict[str, Any] | None:
        # Claim and resolve the effective payload in one statement, so the
        # snapshot and the status change cannot disagree. 'queued' is accepted
        # as well as 'dispatched': a fast worker can beat the dispatcher's own
        # mark-dispatched update to the row. COALESCE, not a join, so a type
        # with no job_type_config row still runs -- with an empty base config.
        with self._pool.connection() as conn:
            row = conn.execute(
                """
                UPDATE jobs
                SET status = 'running',
                    payload = COALESCE(
                        (SELECT c.payload FROM job_type_config c
                          WHERE c.job_type = jobs.job_type),
                        '{}'::jsonb
                    ) || input_payload,
                    updated_at = now()
                WHERE id = %s AND status IN ('queued', 'dispatched')
                RETURNING payload
                """,
                (job_id,),
            ).fetchone()
        return None if row is None else row[0]

    def complete(self, job_id: int) -> bool:
        return self._guarded_update(
            "UPDATE jobs SET status='completed', updated_at=now() "
            "WHERE id=%s AND status='running'",
            job_id,
        )

    def fail(self, job_id: int) -> bool:
        return self._guarded_update(
            "UPDATE jobs SET status='failed', updated_at=now() WHERE id=%s AND status='running'",
            job_id,
        )

    def _guarded_update(self, sql: str, job_id: int) -> bool:
        with self._pool.connection() as conn:
            return conn.execute(sql, (job_id,)).rowcount == 1

    def close(self) -> None:
        self._pool.close()
