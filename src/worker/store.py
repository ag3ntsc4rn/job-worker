"""The three job-row operations the worker needs from storage.

All three are compare-and-set updates guarded on the status the worker expects
to find. That is what makes a redelivered message safe: the second worker's
claim matches no row, so it skips instead of running the job twice.

``claim`` also resolves the run's **effective payload** in the same statement:
the type's base config from ``job_type_config.payload`` overlaid with the run's
own ``jobs.input_payload`` (input wins, shallow). Resolving it here rather than
reading it off the message means a redelivery runs against current config, and
doing it *in* the claim means the snapshot and the status change can't disagree.

Keeping storage behind this protocol is what lets the processing logic be pure
and unit-tested against :class:`InMemoryJobStore`, with ``PostgresJobStore``
(``worker.db``) as the production implementation.
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass, field
from typing import Any, Protocol

from worker.resilience import Guard

# Statuses a run may be claimed from: the dispatcher marks a row 'dispatched'
# after publishing, but a fast worker can outrace that update and still see
# 'queued'.
CLAIMABLE = ("queued", "dispatched")


class JobStore(Protocol):
    def claim(self, job_id: int) -> dict[str, Any] | None:
        """Compare-and-set ``queued|dispatched -> running``.

        Returns the effective payload if we won the claim (``{}`` for a job type
        that needs none), or ``None`` if we lost it.
        """
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

    def claim(self, job_id: int) -> dict[str, Any] | None:
        return self._guard.call(self._inner.claim, job_id)

    def complete(self, job_id: int) -> bool:
        return self._guard.call(self._inner.complete, job_id)

    def fail(self, job_id: int) -> bool:
        return self._guard.call(self._inner.fail, job_id)

    def close(self) -> None:
        self._inner.close()


@dataclass
class _Job:
    job_type: str
    status: str
    input_payload: dict[str, Any] = field(default_factory=dict)


class InMemoryJobStore:
    """Process-local ``JobStore`` mirroring the guarded transitions."""

    def __init__(self) -> None:
        self._jobs: dict[int, _Job] = {}
        self._type_payloads: dict[str, dict[str, Any]] = {}
        self._ids = itertools.count(1)
        self.closed = False

    # -- pipeline stand-in -------------------------------------------------
    def add(
        self,
        status: str = "queued",
        *,
        job_type: str = "hello",
        input_payload: dict[str, Any] | None = None,
    ) -> int:
        job_id = next(self._ids)
        self._jobs[job_id] = _Job(job_type, status, dict(input_payload or {}))
        return job_id

    def set_type_payload(self, job_type: str, payload: dict[str, Any]) -> None:
        """The ``job_type_config`` row a type may or may not have."""
        self._type_payloads[job_type] = payload

    def status_of(self, job_id: int) -> str:
        return self._jobs[job_id].status

    # -- JobStore ----------------------------------------------------------
    def claim(self, job_id: int) -> dict[str, Any] | None:
        job = self._jobs.get(job_id)
        if job is None or job.status not in CLAIMABLE:
            return None
        job.status = "running"
        # A type with no config row contributes no base keys, rather than
        # making the run unrunnable: plenty of jobs need no payload at all.
        return {**self._type_payloads.get(job.job_type, {}), **job.input_payload}

    def complete(self, job_id: int) -> bool:
        return self._transition(job_id, "completed")

    def fail(self, job_id: int) -> bool:
        return self._transition(job_id, "failed")

    def _transition(self, job_id: int, new: str) -> bool:
        job = self._jobs.get(job_id)
        if job is None or job.status != "running":
            return False
        job.status = new
        return True

    def close(self) -> None:
        self.closed = True
