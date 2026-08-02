"""Retry-with-backoff and circuit breaking, shared by both dependencies.

The worker talks to two things that fail independently — Kafka (the source) and
Postgres (the job rows) — so each gets its own :class:`Guard`: a retry policy for
*transient* blips wrapped in a `pybreaker` circuit breaker for *sustained*
outages. One dependency going down therefore never trips the other.

Ordering is deliberate: **retries run inside the breaker**. A logical operation
counts as one breaker failure only after its retries are exhausted, and once the
breaker is open the call is rejected immediately — no sleeping through a backoff
schedule against a dependency that is known to be down. The caller sees
:class:`CircuitOpenError` and can back off wholesale instead.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, TypeVar

import pybreaker

# Single import point for "the breaker is open", so callers never import
# pybreaker themselves.
CircuitOpenError = pybreaker.CircuitBreakerError


class NamedCircuitOpenError(CircuitOpenError):
    """``CircuitOpenError`` that says *which* breaker opened.

    pybreaker's own message is the same string for every breaker, so on its own
    it tells an operator nothing about what is actually down.
    """

    def __init__(self, breaker_name: str, cause: BaseException) -> None:
        super().__init__(f"{breaker_name} circuit is open: {cause}")
        self.breaker_name = breaker_name


T = TypeVar("T")


@dataclass(frozen=True)
class RetryPolicy:
    """Exponential backoff: ``base``, ``2*base``, ``4*base`` ... capped at ``max_delay``."""

    max_attempts: int = 3
    base_delay: float = 0.2
    max_delay: float = 5.0

    def delay_for(self, attempt: int) -> float:
        """Delay to wait *after* a failed ``attempt`` (1-based)."""
        return min(self.base_delay * (2 ** (attempt - 1)), self.max_delay)


def build_breaker(
    name: str,
    *,
    failure_threshold: int = 5,
    reset_timeout: float = 30.0,
    exclude: Sequence[type[BaseException]] = (),
) -> pybreaker.CircuitBreaker:
    """``exclude`` lists errors that pass through without counting as failures.

    Used so another dependency's failure, surfacing on this dependency's call
    stack, does not trip this breaker.
    """
    return pybreaker.CircuitBreaker(
        fail_max=failure_threshold,
        reset_timeout=reset_timeout,
        name=name,
        exclude=list(exclude),
    )


class Guard:
    """Runs a call under a retry policy, the whole thing behind a breaker.

    ``retryable`` filters which errors are worth another attempt. ``sleep`` and
    ``on_retry`` are injected so the unit tests can exercise the real backoff
    schedule without spending wall-clock time.
    """

    def __init__(
        self,
        breaker: pybreaker.CircuitBreaker,
        policy: RetryPolicy,
        *,
        sleep: Callable[[float], None] = time.sleep,
        on_retry: Callable[[str, int, float, BaseException], None] | None = None,
        retryable: Callable[[BaseException], bool] | None = None,
    ) -> None:
        self._breaker = breaker
        self._policy = policy
        self._sleep = sleep
        self._on_retry = on_retry
        self._retryable = retryable or (lambda err: True)

    @property
    def state(self) -> str:
        return self._breaker.current_state

    def call(self, fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Invoke ``fn``; raises the last error, or ``CircuitOpenError`` if open."""
        try:
            return self._breaker.call(self._with_retries, fn, args, kwargs)
        except NamedCircuitOpenError:
            raise  # already attributed to the breaker that actually opened
        except CircuitOpenError as err:
            raise NamedCircuitOpenError(self._breaker.name, err) from err

    def _with_retries(
        self, fn: Callable[..., T], args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> T:
        for attempt in range(1, self._policy.max_attempts + 1):
            try:
                return fn(*args, **kwargs)
            except Exception as err:
                if attempt == self._policy.max_attempts or not self._retryable(err):
                    raise
                delay = self._policy.delay_for(attempt)
                if self._on_retry is not None:
                    self._on_retry(self._breaker.name, attempt, delay, err)
                self._sleep(delay)
        raise AssertionError("unreachable: max_attempts must be >= 1")  # pragma: no cover
