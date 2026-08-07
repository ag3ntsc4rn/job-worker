"""The guarded wrappers: transient blips retried, outages isolated per dependency."""

from __future__ import annotations

import pytest

from tests.doubles import BrokerDown, FlakyConsumer, FlakyJobStore, RecordingSleep, StorageDown
from worker.config import Config
from worker.consumer import GuardedConsumer, InMemoryConsumer
from worker.handlers import always_succeeds
from worker.models import MalformedEnvelope
from worker.resilience import CircuitOpenError, NamedCircuitOpenError
from worker.service import process
from worker.store import GuardedJobStore, InMemoryJobStore
from worker.wiring import build_consumer_guard, build_store_guard


def config() -> Config:
    return Config.from_env(
        {
            "RETRY_MAX_ATTEMPTS": "3",
            "RETRY_BACKOFF_BASE": "0.01",
            "RETRY_BACKOFF_MAX": "0.02",
            "BREAKER_FAILURE_THRESHOLD": "2",
        }
    )


def guarded_store(inner, sleep: RecordingSleep | None = None) -> GuardedJobStore:
    # `sleep` runs the real backoff schedule without spending the wall clock.
    return GuardedJobStore(inner, build_store_guard(config(), sleep=sleep or RecordingSleep()))


def guarded_consumer(inner, sleep: RecordingSleep | None = None) -> GuardedConsumer:
    return GuardedConsumer(inner, build_consumer_guard(config(), sleep=sleep or RecordingSleep()))


def test_a_transient_database_blip_is_retried_and_the_job_still_completes():
    inner = FlakyJobStore(failures=2)
    job_id = inner.add("queued")
    sleep = RecordingSleep()

    message = {"job_id": job_id, "job_type": "hello"}

    assert process(guarded_store(inner, sleep), always_succeeds, message) == "completed"
    assert inner.status_of(job_id) == "completed"
    assert sleep.delays == [0.01, 0.02]  # backoff doubled between attempts


def test_a_transient_broker_blip_is_retried_and_the_message_arrives():
    inner = FlakyConsumer(failures=2)
    inner.add(1)

    assert guarded_consumer(inner).poll(1.0) == {"job_id": 1, "job_type": "hello"}


def test_a_sustained_database_outage_opens_the_store_breaker_and_fails_fast():
    inner = FlakyJobStore(failures=1000)
    inner.add("queued")
    store = guarded_store(inner)

    with pytest.raises(StorageDown):
        store.claim(1)
    with pytest.raises(CircuitOpenError):  # second failure crosses the threshold
        store.claim(1)

    attempts_before = inner.attempts
    with pytest.raises(NamedCircuitOpenError) as opened:
        store.claim(1)
    assert inner.attempts == attempts_before  # rejected without touching the database
    assert opened.value.breaker_name == "postgres-jobs"
    assert store.state == "open"


def test_a_broker_outage_does_not_trip_the_database_breaker():
    """Separate guards: one dependency being down never quarantines the other."""
    consumer_inner = FlakyConsumer(failures=1000)
    consumer = guarded_consumer(consumer_inner)
    store_inner = InMemoryJobStore()
    job_id = store_inner.add("queued")
    store = guarded_store(store_inner)

    for _ in range(3):
        with pytest.raises((BrokerDown, CircuitOpenError)):
            consumer.poll(1.0)

    assert consumer.state == "open"
    assert store.state == "closed"
    assert store.claim(job_id) == {}


def test_an_undecodable_message_is_neither_retried_nor_blamed_on_the_broker():
    """Garbage on the topic is a producer bug: the broker's breaker stays closed."""

    class UndecodableConsumer(InMemoryConsumer):
        polls = 0

        def poll(self, timeout: float) -> dict | None:
            self.polls += 1
            raise MalformedEnvelope("undecodable message: b'not-json-at-all'")

    inner = UndecodableConsumer()
    sleep = RecordingSleep()
    consumer = guarded_consumer(inner, sleep)

    for _ in range(5):
        with pytest.raises(MalformedEnvelope):
            consumer.poll(1.0)

    assert inner.polls == 5  # one attempt each: no backoff spent on bad bytes
    assert sleep.delays == []
    assert consumer.state == "closed"  # threshold is 2, so a counted failure would show


def test_the_guards_are_transparent_when_both_dependencies_are_healthy():
    store_inner, consumer_inner = InMemoryJobStore(), InMemoryConsumer()
    job_id = store_inner.add("queued")
    consumer_inner.add(job_id)
    store, consumer = guarded_store(store_inner), guarded_consumer(consumer_inner)

    assert consumer.poll(1.0) == {"job_id": job_id, "job_type": "hello"}
    consumer.commit()
    assert store.claim(job_id) is not None and store.complete(job_id)
    assert store_inner.status_of(job_id) == "completed"
    assert (store.state, consumer.state) == ("closed", "closed")

    store.close()
    consumer.close()
    assert store_inner.closed and consumer_inner.closed


def test_a_lost_run_can_still_be_marked_failed_through_the_guard():
    inner = InMemoryJobStore()
    job_id = inner.add("queued")
    store = guarded_store(inner)

    store.claim(job_id)
    assert store.fail(job_id) is True
    assert inner.status_of(job_id) == "failed"
