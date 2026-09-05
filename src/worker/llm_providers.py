"""Provider SDK adapters implementing :class:`worker.llm.LLMClient`.

Both return the model's reply already parsed as JSON; schema validation happens
in the handler so it is identical across providers. Each SDK's own retries are
disabled -- the worker's :class:`Guard` is the single retry/breaker policy.

Needs network, so exercised through the compose stack rather than the unit
suite and excluded from coverage.
"""

from __future__ import annotations

import json
from typing import Any

import anthropic
import openai

from worker.llm import Completion, CompletionRequest, InvalidOutput

_TOOL_NAME = "emit"


def _parse_json(text: str) -> Any:
    try:
        return json.loads(text)
    except json.JSONDecodeError as err:
        raise InvalidOutput(f"reply is not JSON: {err}", text) from err


class OpenAIClient:
    """Chat Completions with ``response_format=json_schema``.

    ``base_url`` makes this work against any OpenAI-compatible gateway.
    """

    def __init__(self, api_key: str, *, base_url: str | None = None, timeout: float = 60.0):
        self._client = openai.OpenAI(
            api_key=api_key, base_url=base_url, timeout=timeout, max_retries=0
        )

    def complete(self, request: CompletionRequest) -> Completion:
        response = self._client.chat.completions.create(
            model=request.model,
            temperature=request.temperature,
            max_completion_tokens=request.max_tokens,
            messages=[
                {"role": "system", "content": request.system},
                {"role": "user", "content": request.user_message},
            ],
            response_format={
                "type": "json_schema",
                "json_schema": {"name": _TOOL_NAME, "schema": request.output_schema},
            },
        )
        choice = response.choices[0]
        if choice.finish_reason == "length":
            raise InvalidOutput("reply truncated at max_tokens", choice.message.content)
        usage = response.usage
        return Completion(
            output=_parse_json(choice.message.content or ""),
            input_tokens=usage.prompt_tokens if usage else 0,
            output_tokens=usage.completion_tokens if usage else 0,
        )


class AnthropicClient:
    """Messages API; structured output via a forced tool call.

    The tool's ``input_schema`` *is* ``output_schema``, so the tool-use block's
    ``input`` is the reply.
    """

    def __init__(self, api_key: str, *, base_url: str | None = None, timeout: float = 60.0):
        self._client = anthropic.Anthropic(
            api_key=api_key, base_url=base_url, timeout=timeout, max_retries=0
        )

    def complete(self, request: CompletionRequest) -> Completion:
        response = self._client.messages.create(
            model=request.model,
            system=request.system,
            temperature=request.temperature,
            max_tokens=request.max_tokens,
            messages=[{"role": "user", "content": request.user_message}],
            tools=[
                {
                    "name": _TOOL_NAME,
                    "description": "Return the answer in the required structure.",
                    "input_schema": request.output_schema,
                }
            ],
            tool_choice={"type": "tool", "name": _TOOL_NAME},
        )
        if response.stop_reason == "max_tokens":
            raise InvalidOutput("reply truncated at max_tokens", None)
        for block in response.content:
            if block.type == "tool_use" and block.name == _TOOL_NAME:
                return Completion(
                    output=block.input,
                    input_tokens=response.usage.input_tokens,
                    output_tokens=response.usage.output_tokens,
                )
        raise InvalidOutput("reply contained no tool call", [b.type for b in response.content])
