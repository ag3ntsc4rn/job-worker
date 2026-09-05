"""``llm_structured``: the generic structured-completion handler.

One LLM call per run. The run's effective payload supplies *what to do*
(``system`` prompt, ``output_schema``) and *what to do it to* (``input``, any
JSON); the handler serialises ``input`` as the user message, asks the model to
answer in the schema, validates the reply, and returns it as the run's
``result``. The handler knows nothing about the domain -- a SOAR incident, an
invoice, a log excerpt are all just ``input``. What makes a job type "the
incident summariser" is its ``job_type_config`` row, so a new use case is a
config row, not code.

Payload keys (type config overlaid with the run's input, as usual)::

    system         str   required   instructions for the model
    input          any   required   the data to work on; sent as JSON
    output_schema  dict  required   JSON Schema the reply must satisfy
    provider       str   optional   "openai" | "anthropic" (default from Config)
    model          str   optional   provider model id (default from Config)
    max_tokens     int   optional   default 1024
    temperature    num   optional   default 0

Stored on ``jobs.result``::

    {"output": <schema-conforming reply>, "provider": ..., "model": ...,
     "usage": {"input_tokens": n, "output_tokens": n}, "latency_ms": n, "attempts": n}

Failure mapping follows the handler contract in :mod:`worker.handlers`: a bad
payload or a reply that still violates the schema after one retry raises and
fails the run (terminal); a provider outage surfaces as ``CircuitOpenError``
from the guard, which leaves the run ``running`` for the reaper.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol

import jsonschema

from worker.handlers import Handler
from worker.models import Envelope
from worker.resilience import Guard

logger = logging.getLogger(__name__)


class BadPayload(ValueError):
    """The run's payload cannot be turned into a completion request."""


class InvalidOutput(ValueError):
    """The model's reply did not satisfy ``output_schema``."""

    def __init__(self, detail: str, raw: Any) -> None:
        super().__init__(detail)
        self.raw = raw


@dataclass(frozen=True)
class CompletionRequest:
    system: str
    input: Any
    output_schema: dict[str, Any]
    model: str
    max_tokens: int = 1024
    temperature: float = 0.0

    @property
    def user_message(self) -> str:
        return json.dumps(self.input, indent=2, ensure_ascii=False, default=str)


@dataclass(frozen=True)
class Completion:
    output: Any
    input_tokens: int
    output_tokens: int


class LLMClient(Protocol):
    def complete(self, request: CompletionRequest) -> Completion: ...


class GuardedLLMClient:
    """Routes ``complete`` through a :class:`Guard` (retry + breaker)."""

    def __init__(self, inner: LLMClient, guard: Guard) -> None:
        self._inner = inner
        self._guard = guard

    @property
    def state(self) -> str:
        return self._guard.state

    def complete(self, request: CompletionRequest) -> Completion:
        return self._guard.call(self._inner.complete, request)


def parse_request(payload: Mapping[str, Any], *, default_model: str) -> CompletionRequest:
    missing = [key for key in ("system", "input", "output_schema") if key not in payload]
    if missing:
        raise BadPayload(f"payload missing required key(s): {', '.join(missing)}")
    schema = payload["output_schema"]
    if not isinstance(schema, dict):
        raise BadPayload("output_schema must be a JSON Schema object")
    try:
        jsonschema.Draft202012Validator.check_schema(schema)
    except jsonschema.SchemaError as err:
        raise BadPayload(f"output_schema is not a valid JSON Schema: {err.message}") from err
    try:
        return CompletionRequest(
            system=str(payload["system"]),
            input=payload["input"],
            output_schema=schema,
            model=str(payload.get("model") or default_model),
            max_tokens=int(payload.get("max_tokens", 1024)),
            temperature=float(payload.get("temperature", 0.0)),
        )
    except (TypeError, ValueError) as err:
        raise BadPayload(f"invalid payload value: {err}") from err


def validate_output(output: Any, schema: dict[str, Any]) -> None:
    try:
        jsonschema.validate(output, schema, cls=jsonschema.Draft202012Validator)
    except jsonschema.ValidationError as err:
        where = "/".join(map(str, err.absolute_path)) or "$"
        raise InvalidOutput(f"{where}: {err.message}", output) from err


def structured_completion(
    clients: Mapping[str, LLMClient],
    *,
    default_provider: str,
    default_model: str,
    output_retries: int = 1,
) -> Handler:
    """Build the handler. ``clients`` is keyed by provider name."""
    if default_provider not in clients:
        raise ValueError(f"default provider {default_provider!r} has no configured client")

    def handle(envelope: Envelope) -> dict[str, Any]:
        request = parse_request(envelope.payload, default_model=default_model)
        provider = str(envelope.payload.get("provider") or default_provider)
        client = clients.get(provider)
        if client is None:
            raise BadPayload(f"unknown provider {provider!r}; configured: {sorted(clients)}")

        started = time.monotonic()
        attempts = 0
        while True:
            attempts += 1
            completion = client.complete(request)
            try:
                validate_output(completion.output, request.output_schema)
                break
            except InvalidOutput as err:
                if attempts > output_retries:
                    raise
                logger.warning(
                    "job %s: model output violated schema (%s); retrying", envelope.job_id, err
                )

        logger.info(
            "job %s (%s): %s/%s answered in %d attempt(s), %d+%d tokens",
            envelope.job_id,
            envelope.job_type,
            provider,
            request.model,
            attempts,
            completion.input_tokens,
            completion.output_tokens,
        )
        return {
            "output": completion.output,
            "provider": provider,
            "model": request.model,
            "usage": {
                "input_tokens": completion.input_tokens,
                "output_tokens": completion.output_tokens,
            },
            "latency_ms": int((time.monotonic() - started) * 1000),
            "attempts": attempts,
        }

    return handle
