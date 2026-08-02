"""Processing one message: the claim guard, the outcomes, and the handler contract."""

from __future__ import annotations

import pytest

from tests.doubles import StorageDown
from worker.handlers import always_succeeds
from worker.models import Envelope
from worker.resilience import NamedCircuitOpenError
from worker.service import process
from worker.store import InMemoryJobStore


def message(job_id: int, job_type: str = "hello") -> dict:
    return {"job_id": job_id, "job_type": job_type}


def test_the_generic_handler_runs_a_queued_job_to_completed():
    store = InMemoryJobStore()
    job_id = store.add("queued")

    assert process(store, always_succeeds, message(job_id)) == "completed"
    assert store.status_of(job_id) == "completed"


def test_a_job_already_marked_dispatched_is_claimable_too():
    """The worker can outrace the dispatcher's own mark-dispatched update."""
    store = InMemoryJobStore()
    job_id = store.add("dispatched")

    assert process(store, always_succeeds, message(job_id)) == "completed"


def test_a_redelivered_message_is_skipped_rather_than_run_twice():
    store = InMemoryJobStore()
    job_id = store.add("queued")
    runs: list[int] = []

    def counting_handler(envelope: Envelope) -> None:
        runs.append(envelope.job_id)

    assert process(store, counting_handler, message(job_id)) == "completed"
    # Same message again: the claim finds 'completed', not 'queued'.
    assert process(store, counting_handler, message(job_id)) == "skipped"
    assert runs == [job_id]
    assert store.status_of(job_id) == "completed"


def test_two_workers_racing_the_same_message_run_it_once():
    store = InMemoryJobStore()
    job_id = store.add("queued")

    outcomes = [process(store, always_succeeds, message(job_id)) for _ in range(3)]

    assert outcomes == ["completed", "skipped", "skipped"]


def test_a_raising_handler_fails_the_run():
    store = InMemoryJobStore()
    job_id = store.add("queued")

    def explodes(envelope: Envelope) -> None:
        raise RuntimeError("simulated business failure")

    assert process(store, explodes, message(job_id)) == "failed"
    assert store.status_of(job_id) == "failed"


def test_a_dependency_outage_is_not_recorded_as_a_business_failure():
    """A run whose handler hit an open circuit stays 'running' for the reaper."""
    store = InMemoryJobStore()
    job_id = store.add("queued")

    def dependency_is_down(envelope: Envelope) -> None:
        raise NamedCircuitOpenError("some-dependency", StorageDown("down"))

    with pytest.raises(NamedCircuitOpenError):
        process(store, dependency_is_down, message(job_id))

    assert store.status_of(job_id) == "running"


def test_an_unparseable_message_is_dropped_without_touching_any_job():
    store = InMemoryJobStore()
    job_id = store.add("queued")

    assert process(store, always_succeeds, {"nonsense": True}) == "malformed"
    assert process(store, always_succeeds, {"job_id": "abc", "job_type": "hello"}) == "malformed"
    assert store.status_of(job_id) == "queued"


def test_the_handler_sees_the_parsed_envelope():
    store = InMemoryJobStore()
    job_id = store.add("queued", job_type="report")
    seen: list[Envelope] = []

    process(store, seen.append, {"job_id": str(job_id), "job_type": "report"})

    assert seen == [Envelope(job_id=job_id, job_type="report", payload={})]


def test_the_handler_sees_the_type_config_as_its_payload():
    store = InMemoryJobStore()
    store.set_type_payload("report", {"format": "pdf", "rows": 50})
    job_id = store.add("queued", job_type="report")
    seen: list[Envelope] = []

    assert process(store, seen.append, message(job_id, "report")) == "completed"
    assert seen[0].payload == {"format": "pdf", "rows": 50}


def test_a_runs_input_payload_overrides_the_type_config():
    store = InMemoryJobStore()
    store.set_type_payload("report", {"format": "pdf", "rows": 50})
    job_id = store.add("queued", job_type="report", input_payload={"rows": 5000})
    seen: list[Envelope] = []

    process(store, seen.append, message(job_id, "report"))

    assert seen[0].payload == {"format": "pdf", "rows": 5000}


def test_a_type_with_no_config_runs_with_an_empty_payload():
    """Plenty of jobs need no payload; a missing config row must not fail the run."""
    store = InMemoryJobStore()
    job_id = store.add("queued", job_type="needs_nothing")
    seen: list[Envelope] = []

    assert process(store, seen.append, message(job_id, "needs_nothing")) == "completed"
    assert seen[0].payload == {}


def test_the_payload_is_resolved_at_claim_not_at_enqueue():
    """A redelivery hours later must run against current config, not a stale copy."""
    store = InMemoryJobStore()
    store.set_type_payload("report", {"rows": 50})
    job_id = store.add("queued", job_type="report")
    store.set_type_payload("report", {"rows": 999})  # config edited after enqueue
    seen: list[Envelope] = []

    process(store, seen.append, message(job_id, "report"))

    assert seen[0].payload == {"rows": 999}


def test_a_message_for_an_unknown_job_is_skipped():
    """Nothing to claim, so nothing runs — no row is invented."""
    assert process(InMemoryJobStore(), always_succeeds, message(999)) == "skipped"


def test_recording_an_outcome_for_a_run_we_do_not_hold_is_refused():
    """The completion guard mirrors the claim guard: only the holder records."""
    store = InMemoryJobStore()
    job_id = store.add("queued")

    assert store.complete(job_id) is False  # never claimed, so not 'running'
    assert store.fail(999) is False  # no such run
    assert store.status_of(job_id) == "queued"
