"""The message source: poll one message, commit its offset when the run is recorded.

Offsets are committed manually and *after* the database write, never before: a
crash in between redelivers the message, and the claim guard turns that into a
skip. Auto-commit would instead lose the job outright.
"""

from __future__ import annotations

from typing import Any, Protocol

from worker.resilience import Guard


class Consumer(Protocol):
    def poll(self, timeout: float) -> dict[str, Any] | None:
        """Next message, or ``None`` if ``timeout`` elapsed with nothing to read."""
        ...

    def commit(self) -> None:
        """Commit the offset of the message last returned by ``poll``."""
        ...

    def close(self) -> None: ...


class GuardedConsumer:
    """Routes consumer calls through a :class:`Guard` (retry + breaker)."""

    def __init__(self, inner: Consumer, guard: Guard) -> None:
        self._inner = inner
        self._guard = guard

    @property
    def state(self) -> str:
        return self._guard.state

    def poll(self, timeout: float) -> dict[str, Any] | None:
        return self._guard.call(self._inner.poll, timeout)

    def commit(self) -> None:
        return self._guard.call(self._inner.commit)

    def close(self) -> None:
        self._inner.close()


class InMemoryConsumer:
    """Process-local ``Consumer`` that replays a scripted list of messages.

    Uncommitted messages are redelivered on ``redeliver_uncommitted()``, which is
    how the tests exercise the at-least-once path without a broker.
    """

    def __init__(self, messages: list[dict[str, Any]] | None = None) -> None:
        self._pending = list(messages or [])
        self._in_flight: dict[str, Any] | None = None
        self.committed: list[dict[str, Any]] = []
        self.closed = False

    def add(self, job_id: int, job_type: str = "hello") -> dict[str, Any]:
        message = {"job_id": job_id, "job_type": job_type}
        self._pending.append(message)
        return message

    def redeliver_uncommitted(self) -> None:
        if self._in_flight is not None:
            self._pending.insert(0, self._in_flight)
            self._in_flight = None

    # -- Consumer ----------------------------------------------------------
    def poll(self, timeout: float) -> dict[str, Any] | None:
        if not self._pending:
            return None
        self._in_flight = self._pending.pop(0)
        return self._in_flight

    def commit(self) -> None:
        if self._in_flight is not None:
            self.committed.append(self._in_flight)
            self._in_flight = None

    def close(self) -> None:
        self.closed = True
