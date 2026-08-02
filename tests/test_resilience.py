"""Retry + circuit breaker behaviour of :class:`Guard`."""

from __future__ import annotations

import pytest

from tests.doubles import RecordingSleep, StorageDown
from worker.resilience import (
    CircuitOpenError,
    Guard,
    NamedCircuitOpenError,
    RetryPolicy,
    build_breaker,
)


def guard(
    *,
    name: str = "test",
    max_attempts: int = 3,
    failure_threshold: int = 2,
    reset_timeout: float = 30.0,
    exclude: tuple[type[BaseException], ...] = (),
    sleep: RecordingSleep | None = None,
    retryable=None,
) -> Guard:
    return Guard(
        build_breaker(
            name,
            failure_threshold=failure_threshold,
            reset_timeout=reset_timeout,
            exclude=exclude,
        ),
        RetryPolicy(max_attempts=max_attempts, base_delay=0.1, max_delay=0.25),
        sleep=sleep or RecordingSleep(),
        retryable=retryable,
    )


def test_backoff_is_exponential_and_capped():
    policy = RetryPolicy(max_attempts=5, base_delay=0.1, max_delay=0.25)
    assert [policy.delay_for(n) for n in (1, 2, 3, 4)] == [0.1, 0.2, 0.25, 0.25]


def test_transient_failure_is_retried_and_the_call_succeeds():
    attempts = {"n": 0}

    def flaky() -> str:
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise StorageDown("blip")
        return "ok"

    sleep = RecordingSleep()
    g = guard(sleep=sleep)

    assert g.call(flaky) == "ok"
    assert attempts["n"] == 3
    assert sleep.delays == [0.1, 0.2]  # slept between attempts, not after success
    assert g.state == "closed"


def test_exhausted_retries_raise_the_underlying_error():
    def always_fails() -> None:
        raise StorageDown("down")

    with pytest.raises(StorageDown):
        guard(max_attempts=2).call(always_fails)


def test_breaker_opens_after_repeated_exhausted_calls_and_then_fails_fast():
    calls = {"n": 0}

    def always_fails() -> None:
        calls["n"] += 1
        raise StorageDown("down")

    g = guard(max_attempts=2, failure_threshold=2)

    with pytest.raises(StorageDown):
        g.call(always_fails)
    # The failure that crosses the threshold surfaces as the breaker opening.
    with pytest.raises(CircuitOpenError):
        g.call(always_fails)
    assert calls["n"] == 4  # 2 logical calls x 2 attempts

    with pytest.raises(CircuitOpenError):
        g.call(always_fails)
    assert calls["n"] == 4  # rejected without touching the dependency
    assert g.state == "open"


def test_breaker_closes_again_after_the_cooldown():
    g = guard(max_attempts=1, failure_threshold=1, reset_timeout=0.0)

    with pytest.raises(CircuitOpenError):
        g.call(_boom)
    assert g.state == "open"

    # reset_timeout=0 -> the next call is the trial call, and it succeeds
    assert g.call(lambda: "ok") == "ok"
    assert g.state == "closed"


def test_excluded_errors_neither_retry_nor_trip_the_breaker():
    calls = {"n": 0}

    def raises_excluded() -> None:
        calls["n"] += 1
        raise StorageDown("not this breaker's fault")

    g = guard(
        max_attempts=3,
        failure_threshold=1,
        exclude=(StorageDown,),
        retryable=lambda err: not isinstance(err, StorageDown),
    )

    for _ in range(3):
        with pytest.raises(StorageDown):
            g.call(raises_excluded)

    assert calls["n"] == 3  # one attempt each, no retries
    assert g.state == "closed"


def test_open_circuit_names_the_breaker_that_opened():
    g = guard(name="kafka-source", max_attempts=1, failure_threshold=1)

    with pytest.raises(NamedCircuitOpenError) as opened:
        g.call(_boom)

    assert opened.value.breaker_name == "kafka-source"
    assert "kafka-source circuit is open" in str(opened.value)


def test_an_already_named_open_circuit_is_not_re_attributed():
    """A nested guard's open breaker travels outward unchanged."""
    inner = guard(name="inner-dependency", max_attempts=1, failure_threshold=1)
    outer = guard(name="kafka-source", max_attempts=1, failure_threshold=5)

    with pytest.raises(NamedCircuitOpenError):
        inner.call(_boom)

    with pytest.raises(NamedCircuitOpenError) as opened:
        outer.call(inner.call, _boom)

    assert opened.value.breaker_name == "inner-dependency"


def _boom() -> None:
    raise StorageDown("down")
