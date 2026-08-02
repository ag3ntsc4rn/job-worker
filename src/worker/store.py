"""The three job-row operations the worker needs from storage.

All three are compare-and-set updates guarded on the status the worker expects
to find. That is what makes a redelivered message safe: the second worker's
claim matches no row, so it skips instead of running the job twice.

Keeping storage behind this protocol is what lets the processing logic be pure
and unit-tested against :class:`InMemoryJobStore`, with ``PostgresJobStore``
(``worker.db``) as the production implementation.
"""

from __future__ import annotations

import itertools
from typing import Protocol

from worker.resilience import Guard

# Statuses a run may be claimed from: the dispatcher marks a row 'dispatched'
# after publishing, but a fast worker can outrace that update and still see
# 'queued'.
CLAIMABLE = ("queued", "dispatched")


class JobStore(Protocol):
    def claim(self, job_id: int) -> bool:
        """Compare-and-set ``queued|dispatched -> running``. True iff we won."""
        ...

    def complete(self, job_id: int) -> bool: ...
    def fail(self, job_id: int) -> bool: ...
    def close(self) -> None: ...


class GuardedJobStore:
    """Routes every store call through a :class:`Guard` (retry + breaker)."""

    def __init__(self, inner: JobStore, guard: Guard) -> None:
        self._inner = inner
        self._guard = guard

    @property
    def state(self) -> str:
        return self._guard.state

    def claim(self, job_id: int) -> bool:
        return self._guard.call(self._inner.claim, job_id)

    def complete(self, job_id: int) -> bool:
        return self._guard.call(self._inner.complete, job_id)

    def fail(self, job_id: int) -> bool:
        return self._guard.call(self._inner.fail, job_id)

    def close(self) -> None:
        self._inner.close()


class InMemoryJobStore:
    """Process-local ``JobStore`` mirroring the guarded transitions."""

    def __init__(self) -> None:
        self._status: dict[int, str] = {}
        self._ids = itertools.count(1)
        self.closed = False

    # -- pipeline stand-in -------------------------------------------------
    def add(self, status: str = "queued") -> int:
        job_id = next(self._ids)
        self._status[job_id] = status
        return job_id

    def status_of(self, job_id: int) -> str:
        return self._status[job_id]

    # -- JobStore ----------------------------------------------------------
    def claim(self, job_id: int) -> bool:
        return self._transition(job_id, CLAIMABLE, "running")

    def complete(self, job_id: int) -> bool:
        return self._transition(job_id, ("running",), "completed")

    def fail(self, job_id: int) -> bool:
        return self._transition(job_id, ("running",), "failed")

    def _transition(self, job_id: int, expected: tuple[str, ...], new: str) -> bool:
        if self._status.get(job_id) not in expected:
            return False
        self._status[job_id] = new
        return True

    def close(self) -> None:
        self.closed = True
