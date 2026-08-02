"""The consume loop: commit ordering, failure tolerance, and shutdown."""

from __future__ import annotations

from tests.doubles import RecordingSleep, StorageDown
from worker.consumer import InMemoryConsumer
from worker.handlers import always_succeeds
from worker.models import Envelope
from worker.resilience import CircuitOpenError
from worker.service import run
from worker.shutdown import ShutdownSignal
from worker.store import InMemoryJobStore


class StopAfter:
    """``should_stop`` that lets the loop run a fixed number of iterations."""

    def __init__(self, iterations: int) -> None:
        self._left = iterations

    def __call__(self) -> bool:
        if self._left <= 0:
            return True
        self._left -= 1
        return False


def run_loop(store, consumer, should_stop, sleep, handler=always_succeeds, **overrides):
    kwargs = {
        "poll_timeout": 1.0,
        "breaker_open_sleep": 5.0,
        "should_stop": should_stop,
        "sleep": sleep,
    }
    kwargs.update(overrides)
    return run(store, consumer, handler, **kwargs)


def stocked(count: int) -> tuple[InMemoryJobStore, InMemoryConsumer, list[int]]:
    store, consumer = InMemoryJobStore(), InMemoryConsumer()
    job_ids = [store.add("queued") for _ in range(count)]
    for job_id in job_ids:
        consumer.add(job_id)
    return store, consumer, job_ids


def test_each_message_is_run_and_its_offset_committed():
    store, consumer, job_ids = stocked(3)

    totals = run_loop(store, consumer, StopAfter(3), RecordingSleep())

    assert totals.completed == 3
    assert len(consumer.committed) == 3
    assert [store.status_of(job_id) for job_id in job_ids] == ["completed"] * 3


def test_an_empty_poll_is_not_counted_and_does_not_commit():
    store, consumer, _ = stocked(1)

    totals = run_loop(store, consumer, StopAfter(3), RecordingSleep())

    assert (totals.completed, totals.processed) == (1, 1)
    assert len(consumer.committed) == 1  # the two empty polls committed nothing


def test_a_crash_before_the_commit_redelivers_and_the_claim_guard_skips_it():
    store, consumer, job_ids = stocked(1)

    class DiesAfterTheDatabaseWrite(InMemoryConsumer):
        def commit(self) -> None:
            raise RuntimeError("killed before the offset landed")

    crashing = DiesAfterTheDatabaseWrite()
    crashing.add(job_ids[0])
    run_loop(store, crashing, StopAfter(1), RecordingSleep())
    assert store.status_of(job_ids[0]) == "completed"  # the work did land

    # Restart: the uncommitted message is redelivered to a healthy consumer.
    totals = run_loop(store, consumer, StopAfter(1), RecordingSleep())

    assert (totals.skipped, totals.completed) == (1, 0)
    assert store.status_of(job_ids[0]) == "completed"


def test_a_redelivery_after_an_uncommitted_poll_is_skipped():
    """The at-least-once path, without a broker: same message, second delivery."""
    store, consumer, job_ids = stocked(1)

    assert run_loop(store, consumer, StopAfter(1), RecordingSleep()).completed == 1
    consumer.add(job_ids[0])  # partition rewound to before the committed offset

    assert run_loop(store, consumer, StopAfter(1), RecordingSleep()).skipped == 1


def test_an_interrupted_poll_is_redelivered_intact():
    store, consumer, job_ids = stocked(1)

    assert consumer.poll(1.0) == {"job_id": job_ids[0], "job_type": "hello"}
    consumer.redeliver_uncommitted()  # killed before the offset was committed
    consumer.redeliver_uncommitted()  # nothing in flight now: a no-op

    assert run_loop(store, consumer, StopAfter(1), RecordingSleep()).completed == 1
    assert store.status_of(job_ids[0]) == "completed"


def test_a_failing_handler_does_not_stop_the_loop_and_still_commits():
    store, consumer, job_ids = stocked(2)
    calls = {"n": 0}

    def fails_the_first_job(envelope: Envelope) -> None:
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("simulated business failure")

    totals = run_loop(store, consumer, StopAfter(2), RecordingSleep(), handler=fails_the_first_job)

    assert (totals.failed, totals.completed) == (1, 1)
    assert [store.status_of(job_id) for job_id in job_ids] == ["failed", "completed"]
    assert len(consumer.committed) == 2  # a failed run is a processed run


def test_a_poison_message_is_committed_past_instead_of_wedging_the_partition():
    store, consumer = InMemoryJobStore(), InMemoryConsumer([{"garbage": True}])

    totals = run_loop(store, consumer, StopAfter(1), RecordingSleep())

    assert totals.malformed == 1
    assert len(consumer.committed) == 1


def test_an_open_circuit_backs_off_and_leaves_the_message_uncommitted():
    _, consumer, _ = stocked(2)

    class BreakerOpenStore(InMemoryJobStore):
        def claim(self, job_id: int) -> dict | None:
            raise CircuitOpenError("postgres-jobs is open")

    sleep = RecordingSleep()
    totals = run_loop(BreakerOpenStore(), consumer, StopAfter(2), sleep)

    assert totals.processed == 0
    assert sleep.delays == [5.0, 5.0]
    assert consumer.committed == []


def test_an_unexpected_error_does_not_kill_the_loop():
    class BrokenStore(InMemoryJobStore):
        def claim(self, job_id: int) -> dict | None:
            raise StorageDown("database unavailable")

    _, consumer, _ = stocked(2)
    sleep = RecordingSleep()

    totals = run_loop(BrokenStore(), consumer, StopAfter(2), sleep)

    assert totals.processed == 0
    assert sleep.delays == [5.0, 5.0]


def test_stops_promptly_when_shutdown_is_requested():
    store, consumer, job_ids = stocked(1)
    shutdown = ShutdownSignal()
    shutdown.request()

    assert run_loop(store, consumer, shutdown, RecordingSleep()).processed == 0
    assert store.status_of(job_ids[0]) == "queued"  # untouched, still on the topic
