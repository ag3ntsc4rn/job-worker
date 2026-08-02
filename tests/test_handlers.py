from __future__ import annotations

import logging

import pytest

from worker.handlers import always_succeeds, by_job_type
from worker.models import Envelope, MalformedEnvelope
from worker.service import process
from worker.store import InMemoryJobStore


def test_the_default_handler_does_nothing_and_reports_success(caplog: pytest.LogCaptureFixture):
    caplog.set_level(logging.INFO, logger="worker.handlers")

    assert always_succeeds(Envelope(job_id=7, job_type="hello")) is None
    assert "job 7 (hello)" in caplog.text


def test_an_envelope_is_a_pointer_with_an_optional_payload():
    assert Envelope.parse({"job_id": "7", "job_type": "hello"}) == Envelope(7, "hello", {})
    assert Envelope.parse({"job_id": 7, "job_type": "hello", "payload": None}).payload == {}
    assert Envelope.parse({"job_id": 7, "job_type": "hello", "payload": {"a": 1}}).payload == {
        "a": 1
    }


@pytest.mark.parametrize(
    "message",
    [{}, {"job_id": 1}, {"job_type": "hello"}, {"job_id": "abc", "job_type": "hello"},
     {"job_id": None, "job_type": "hello"}],
)
def test_anything_that_is_not_a_job_pointer_is_rejected(message: dict):
    with pytest.raises(MalformedEnvelope):
        Envelope.parse(message)


def test_routing_sends_each_type_to_its_own_handler():
    seen: list[str] = []
    handler = by_job_type(
        {
            "send_report": lambda envelope: seen.append(f"report:{envelope.job_id}"),
            "reindex": lambda envelope: seen.append(f"reindex:{envelope.job_id}"),
        }
    )

    handler(Envelope(job_id=1, job_type="send_report"))
    handler(Envelope(job_id=2, job_type="reindex"))

    assert seen == ["report:1", "reindex:2"]


def test_an_unmapped_type_completes_but_warns(caplog: pytest.LogCaptureFixture):
    """A shared topic: types this deployment does not own pass — audibly, since the
    other way to get here is a typo'd job type."""
    caplog.set_level(logging.WARNING, logger="worker.handlers")
    store = InMemoryJobStore()
    job_id = store.add("queued")
    handler = by_job_type({"send_report": lambda envelope: None})

    outcome = process(store, handler, {"job_id": job_id, "job_type": "someone_elses_type"})

    assert (outcome, store.status_of(job_id)) == ("completed", "completed")
    assert "no handler for job type 'someone_elses_type'" in caplog.text


def test_a_strict_default_fails_the_run_instead():
    store = InMemoryJobStore()
    job_id = store.add("queued")

    def unknown_type(envelope: Envelope) -> None:
        raise LookupError(f"no handler for {envelope.job_type!r}")

    handler = by_job_type({"send_report": lambda envelope: None}, default=unknown_type)

    assert process(store, handler, {"job_id": job_id, "job_type": "mystery"}) == "failed"
    assert store.status_of(job_id) == "failed"


def test_a_raising_handler_is_what_marks_a_run_failed():
    """The worked example from the docs: bad input is a terminal business failure."""
    store = InMemoryJobStore()
    job_id = store.add("queued")

    def send_report(envelope: Envelope) -> None:
        recipient = envelope.payload["recipient"]  # missing -> KeyError -> failed
        assert recipient

    assert process(store, send_report, {"job_id": job_id, "job_type": "send_report"}) == "failed"
