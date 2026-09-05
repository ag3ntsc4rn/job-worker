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

import logging
import time
from typing import Any

from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

logger = logging.getLogger(__name__)


class PostgresJobStore:
    def __init__(self, database_url: str, *, max_retries: int = 30, retry_delay: float = 2.0):
        self._pool = self._open_with_retry(database_url, max_retries, retry_delay)

    @staticmethod
    def _open_with_retry(database_url: str, max_retries: int, retry_delay: float) -> ConnectionPool:
        """Wait for Postgres to accept connections, then return the open pool.

        A pool is single-use: once ``open`` has failed it cannot be opened again,
        so each attempt builds a fresh one. Sharing one across attempts turns
        every retry after the first into an instant "cannot be reused" error, and
        the wait the caller asked for silently disappears.
        """
        last_err: Exception | None = None
        for attempt in range(1, max_retries + 1):
            pool = ConnectionPool(database_url, min_size=1, max_size=5, open=False)
            try:
                pool.open(wait=True, timeout=5)
                return pool
            except Exception as err:  # noqa: BLE001 - retry until Postgres is ready
                last_err = err
                pool.close()
                logger.warning("Postgres not ready (attempt %d/%d): %s", attempt, max_retries, err)
                if attempt < max_retries:
                    time.sleep(retry_delay)
        raise RuntimeError(
            f"could not connect to Postgres after {max_retries} attempts: {last_err}"
        )

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

    def complete(self, job_id: int, result: Any = None) -> bool:
        # Result and status land in the same statement, so a row can never read
        # 'completed' with a stale result or carry a result while still 'running'.
        return self._guarded_update(
            "UPDATE jobs SET status='completed', result=%s, updated_at=now() "
            "WHERE id=%s AND status='running'",
            (None if result is None else Jsonb(result), job_id),
        )

    def fail(self, job_id: int) -> bool:
        return self._guarded_update(
            "UPDATE jobs SET status='failed', updated_at=now() WHERE id=%s AND status='running'",
            (job_id,),
        )

    def _guarded_update(self, sql: str, params: tuple[Any, ...]) -> bool:
        with self._pool.connection() as conn:
            return conn.execute(sql, params).rowcount == 1

    def close(self) -> None:
        self._pool.close()
