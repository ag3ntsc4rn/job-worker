"""The ``llm_structured`` handler, through ``process()`` against in-memory doubles."""

from __future__ import annotations

from typing import Any

import pytest

from worker.config import Config
from worker.llm import (
    BadPayload,
    Completion,
    CompletionRequest,
    InvalidOutput,
    parse_request,
    structured_completion,
)
from worker.models import Envelope
from worker.resilience import CircuitOpenError
from worker.service import process
from worker.store import InMemoryJobStore
from worker.wiring import build_llm_handler

SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": ["summary", "urgency"],
    "properties": {
        "summary": {"type": "string"},
        "urgency": {"type": "string", "enum": ["low", "medium", "high"]},
    },
}
GOOD = {"summary": "VPN drops since the 9/3 update", "urgency": "medium"}
BAD = {"summary": "x", "urgency": "urgent"}
TYPE_CONFIG = {"system": "Summarise the ticket.", "output_schema": SCHEMA}
INPUT = {"input": {"title": "VPN drops every 10 minutes", "comments": ["reinstalled"]}}
POINTER = {"job_id": 1, "job_type": "llm_structured"}


class FakeLLM:
    """Replays scripted replies (or raises scripted errors) and records requests."""

    def __init__(self, *replies: Any) -> None:
        self._replies = list(replies)
        self.requests: list[CompletionRequest] = []

    def complete(self, request: CompletionRequest) -> Completion:
        self.requests.append(request)
        reply = self._replies.pop(0)
        if isinstance(reply, BaseException):
            raise reply
        return Completion(output=reply, input_tokens=10, output_tokens=5)


def make(*replies: Any, clients: dict[str, Any] | None = None):
    llm = FakeLLM(*replies)
    handler = structured_completion(
        clients or {"fake": llm}, default_provider="fake", default_model="m-1"
    )
    return llm, handler


def store_with_job(**input_payload: Any) -> InMemoryJobStore:
    store = InMemoryJobStore()
    store.set_type_payload("llm_structured", TYPE_CONFIG)
    assert store.add("queued", job_type="llm_structured", input_payload=input_payload) == 1
    return store


def test_input_is_sent_as_json_and_the_validated_reply_becomes_the_result():
    store = store_with_job(**INPUT)
    llm, handler = make(GOOD)

    assert process(store, handler, POINTER) == "completed"

    [request] = llm.requests
    assert request.system == "Summarise the ticket."
    assert '"title": "VPN drops every 10 minutes"' in request.user_message
    assert request.model == "m-1"
    result = store.result_of(1)
    assert result["output"] == GOOD
    assert (result["provider"], result["model"], result["attempts"]) == ("fake", "m-1", 1)
    assert result["usage"] == {"input_tokens": 10, "output_tokens": 5}
    assert isinstance(result["latency_ms"], int)


def test_per_run_input_overrides_the_type_defaults():
    store = store_with_job(model="m-2", max_tokens=99, temperature=0.5, **INPUT)
    llm, handler = make(GOOD)

    process(store, handler, POINTER)

    [request] = llm.requests
    assert (request.model, request.max_tokens, request.temperature) == ("m-2", 99, 0.5)


def test_a_schema_violation_is_retried_once_then_fails_the_run():
    store = store_with_job(**INPUT)
    llm, handler = make(BAD, BAD)

    assert process(store, handler, POINTER) == "failed"
    assert store.status_of(1) == "failed"
    assert store.result_of(1) is None
    assert len(llm.requests) == 2


def test_a_retry_that_conforms_completes_and_counts_both_attempts():
    store = store_with_job(**INPUT)
    _, handler = make({"summary": "x"}, GOOD)

    assert process(store, handler, POINTER) == "completed"
    assert store.result_of(1)["attempts"] == 2


def test_the_invalid_output_error_names_the_offending_field():
    _, handler = make(BAD, BAD)
    with pytest.raises(InvalidOutput, match="urgency: 'urgent' is not one of") as info:
        handler(Envelope(1, "llm_structured", {**TYPE_CONFIG, **INPUT}))
    assert info.value.raw == BAD


@pytest.mark.parametrize(
    ("payload", "match"),
    [
        ({"input": {}, "output_schema": SCHEMA}, "missing required key.*system"),
        ({"system": "s", "output_schema": SCHEMA}, "missing required key.*input"),
        ({"system": "s", "input": {}}, "missing required key.*output_schema"),
        ({"system": "s", "input": {}, "output_schema": "nope"}, "must be a JSON Schema object"),
        ({"system": "s", "input": {}, "output_schema": {"type": "nope"}}, "not a valid JSON"),
        ({**TYPE_CONFIG, "input": {}, "max_tokens": "lots"}, "invalid payload value"),
    ],
)
def test_a_payload_that_cannot_become_a_request_is_a_bad_payload(payload: dict, match: str):
    with pytest.raises(BadPayload, match=match):
        parse_request(payload, default_model="m")


def test_a_bad_payload_fails_the_run_without_calling_the_model():
    store = InMemoryJobStore()
    store.set_type_payload("llm_structured", {"system": "s"})  # no output_schema
    store.add("queued", job_type="llm_structured", input_payload=INPUT)
    llm, handler = make(GOOD)

    assert process(store, handler, POINTER) == "failed"
    assert llm.requests == []


def test_provider_is_selected_from_the_payload():
    a, b = FakeLLM(GOOD), FakeLLM(GOOD)
    _, handler = make(clients={"fake": a, "other": b})

    result = handler(Envelope(1, "t", {**TYPE_CONFIG, **INPUT, "provider": "other"}))

    assert (len(a.requests), len(b.requests)) == (0, 1)
    assert result["provider"] == "other"


def test_an_unknown_provider_is_a_bad_payload():
    _, handler = make(GOOD)
    with pytest.raises(BadPayload, match="unknown provider 'gemini'"):
        handler(Envelope(1, "t", {**TYPE_CONFIG, **INPUT, "provider": "gemini"}))


def test_the_default_provider_must_have_a_client():
    with pytest.raises(ValueError, match="default provider 'openai'"):
        structured_completion({}, default_provider="openai", default_model="m")


# -- wiring --------------------------------------------------------------------


def cfg(**env: str) -> Config:
    return Config.from_env({"RETRY_MAX_ATTEMPTS": "2", "BREAKER_FAILURE_THRESHOLD": "1", **env})


def wired(*replies: Any):
    llm = FakeLLM(*replies)
    handler = build_llm_handler(
        cfg(LLM_DEFAULT_PROVIDER="fake"), {"fake": llm}, sleep=lambda s: None
    )
    assert handler is not None
    return llm, handler


class ProviderDown(RuntimeError):
    pass


def test_wired_handler_leaves_the_run_running_once_the_provider_breaker_opens():
    """Outage -> CircuitOpenError, which ``process`` lets through unmarked."""
    llm, handler = wired(ProviderDown("503"), ProviderDown("503"))
    store = store_with_job(**INPUT)

    # Threshold is 1: the retries' final failure trips the breaker in place.
    with pytest.raises(CircuitOpenError, match="llm-fake circuit is open"):
        process(store, handler, POINTER)
    assert store.status_of(1) == "running"
    assert store.result_of(1) is None
    assert len(llm.requests) == 2

    second = store.add("queued", job_type="llm_structured", input_payload=INPUT)
    with pytest.raises(CircuitOpenError):
        process(store, handler, {"job_id": second, "job_type": "llm_structured"})
    assert store.status_of(second) == "running"
    assert len(llm.requests) == 2  # rejected before reaching the provider


def test_wired_handler_does_not_retry_schema_violations_through_the_guard():
    llm, handler = wired({"summary": "x"}, {"summary": "x"})

    with pytest.raises(InvalidOutput):
        handler(Envelope(1, "t", {**TYPE_CONFIG, **INPUT}))
    assert len(llm.requests) == 2  # the handler's one retry, not the guard's


def test_no_configured_provider_means_no_handler():
    assert build_llm_handler(cfg(), {}) is None


def test_default_provider_without_a_key_is_a_startup_error():
    with pytest.raises(ValueError, match="LLM_DEFAULT_PROVIDER='anthropic' has no API key"):
        build_llm_handler(cfg(LLM_DEFAULT_PROVIDER="anthropic"), {"fake": FakeLLM()})


def test_config_reads_llm_settings():
    c = Config.from_env({"LLM_JOB_TYPES": "xsoar_summary, invoice_extract", "OPENAI_API_KEY": ""})
    assert c.llm_job_types == ("xsoar_summary", "invoice_extract")
    assert c.openai_api_key is None
    assert Config.from_env({}).llm_job_types == ("llm_structured",)
