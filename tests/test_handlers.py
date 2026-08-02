from __future__ import annotations

import logging

import pytest

from worker.handlers import always_succeeds
from worker.models import Envelope, MalformedEnvelope


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
