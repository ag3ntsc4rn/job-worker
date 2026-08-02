"""Failure-injecting doubles shared by the resilience tests."""

from __future__ import annotations

from typing import Any

from worker.consumer import InMemoryConsumer
from worker.store import InMemoryJobStore


class StorageDown(RuntimeError):
    pass


class BrokerDown(RuntimeError):
    pass


class FlakyJobStore(InMemoryJobStore):
    """Fails the first ``failures`` calls of each operation, then behaves normally."""

    def __init__(self, failures: int) -> None:
        super().__init__()
        self.remaining_failures = failures
        self.attempts = 0

    def _maybe_fail(self) -> None:
        self.attempts += 1
        if self.remaining_failures > 0:
            self.remaining_failures -= 1
            raise StorageDown("database unavailable")

    def claim(self, job_id: int) -> bool:
        self._maybe_fail()
        return super().claim(job_id)

    def complete(self, job_id: int) -> bool:
        self._maybe_fail()
        return super().complete(job_id)


class FlakyConsumer(InMemoryConsumer):
    """Fails the first ``failures`` polls, then behaves normally."""

    def __init__(self, failures: int, messages: list[dict[str, Any]] | None = None) -> None:
        super().__init__(messages)
        self.remaining_failures = failures
        self.attempts = 0

    def poll(self, timeout: float) -> dict[str, Any] | None:
        self.attempts += 1
        if self.remaining_failures > 0:
            self.remaining_failures -= 1
            raise BrokerDown("broker unavailable")
        return super().poll(timeout)


class RecordingSleep:
    """Stand-in for ``time.sleep`` that records the schedule instead of waiting."""

    def __init__(self) -> None:
        self.delays: list[float] = []

    def __call__(self, seconds: float) -> None:
        self.delays.append(seconds)
