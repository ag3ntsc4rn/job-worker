# Copilot Agent Prompt 2 — Implement the `llm_structured` handler in the worker

> Paste everything below the line into the Copilot agent. Prerequisite: Prompt 1 (the `jobs.result`
> column) has landed — the handler's return value must be persisted on completion.

---

## Context

The worker consumes Kafka pointer envelopes `{"job_id", "job_type"}`, claims the job in Postgres
(`queued|dispatched -> running`), and receives an `Envelope(job_id, job_type, payload)` where
`payload` is the **effective payload**: `job_type_config.payload` shallow-merged with the run's
`jobs.input_payload` (per-run keys win). Handlers are registered in a `HANDLERS: dict[str, Handler]`
registry; a handler's return value is stored in `jobs.result` when the run completes; an exception
marks the run `failed`; a `CircuitOpenError` from a guarded dependency propagates and leaves the
run `running` for the reaper. Dependencies (Postgres, Kafka) are wrapped by a `Guard` = retry with
exponential backoff + a `pybreaker` circuit breaker, built in `wiring.py` from `Config`.

## Goal

One **generic** handler that turns *any* JSON input into *schema-validated* JSON output via a single
LLM completion. No domain logic in code: what a job type does is defined entirely by its
`job_type_config` row (system prompt + output JSON Schema + model). The same handler function is
registered under many job types (`xsoar_summary`, `splunk_summary`, `invoice_extract`, …).

Explicitly out of scope: tool calling / agent loops, MCP, templating of the prompt, fetching data
from external systems. The caller sends the data; the model reads it as raw JSON.

## Payload contract

Effective payload the handler receives (config keys + per-run keys after the merge):

```jsonc
{
  // from job_type_config.payload — the same for every run of this type
  "system": "You are a SOC analyst. Summarise this incident using only the facts given.",
  "output_schema": {                       // JSON Schema (draft 2020-12)
    "type": "object",
    "required": ["summary", "urgency"],
    "properties": {
      "summary": {"type": "string"},
      "urgency": {"type": "string", "enum": ["low", "medium", "high"]}
    },
    "additionalProperties": false
  },
  "provider": "openai",                    // optional; "openai" | "anthropic"; default from env
  "model": "gpt-4.1-mini",                 // optional; default from env
  "max_tokens": 1024,                      // optional, default 1024
  "temperature": 0,                        // optional, default 0

  // from jobs.input_payload — differs per run
  "input": { "...any JSON: object, array, string..." }
}
```

- `system`, `input`, `output_schema` are required. Anything else is optional.
- The user message sent to the model is exactly `json.dumps(input, indent=2, ensure_ascii=False, default=str)`.
  Field names in `input` never matter to the handler; different source systems just get different
  config rows.
- Per-run overrides of `model`/`max_tokens`/`temperature`/`provider` are allowed (the merge does it).

Value stored in `jobs.result` on success:

```json
{
  "output": { "summary": "...", "urgency": "medium" },
  "provider": "openai",
  "model": "gpt-4.1-mini",
  "usage": {"input_tokens": 412, "output_tokens": 96},
  "latency_ms": 2310,
  "attempts": 1
}
```

## Module layout (worker package)

### `llm.py` — provider-agnostic core (unit-tested, no SDK imports)

```python
class BadPayload(ValueError): ...      # payload cannot become a request (missing keys, bad schema, unknown provider)
class InvalidOutput(ValueError):       # model reply violates output_schema (carries .raw = the reply)
    def __init__(self, message: str, raw: Any) -> None: ...

@dataclass(frozen=True)
class CompletionRequest:
    system: str; input: Any; output_schema: dict[str, Any]; model: str
    max_tokens: int = 1024; temperature: float = 0.0
    @property
    def user_message(self) -> str: ...   # json.dumps(input, ...)

@dataclass(frozen=True)
class Completion:
    output: Any; input_tokens: int; output_tokens: int

class LLMClient(Protocol):
    def complete(self, request: CompletionRequest) -> Completion: ...

class GuardedLLMClient:                  # same shape as the existing GuardedJobStore/GuardedConsumer
    def __init__(self, inner: LLMClient, guard: Guard) -> None: ...
    def complete(self, request) -> Completion: return self._guard.call(self._inner.complete, request)

def parse_request(payload: Mapping[str, Any], *, default_model: str) -> CompletionRequest:
    # missing system/input/output_schema -> BadPayload("payload missing required key(s): ...")
    # output_schema not a dict, or fails Draft202012Validator.check_schema -> BadPayload
    # max_tokens/temperature not coercible -> BadPayload("invalid payload value: ...")

def validate_output(output: Any, schema: dict[str, Any]) -> None:
    # jsonschema.validate(..., cls=Draft202012Validator); on error raise
    # InvalidOutput(f"{'/'.join(err.absolute_path) or '$'}: {err.message}", output)

def structured_completion(clients: Mapping[str, LLMClient], *, default_provider: str,
                          default_model: str, output_retries: int = 1) -> Handler:
    # factory returning the handler; raises ValueError at build time if default_provider not in clients
    def handle(envelope: Envelope) -> dict[str, Any]:
        request  = parse_request(envelope.payload, default_model=default_model)
        provider = str(envelope.payload.get("provider") or default_provider)
        client   = clients.get(provider) or raise BadPayload(f"unknown provider {provider!r}; configured: ...")
        # loop: completion = client.complete(request); validate_output(...)
        #   on InvalidOutput: retry while attempts <= output_retries, else re-raise
        # return the result dict shown above (latency measured with time.monotonic())
    return handle
```

### `llm_providers.py` — SDK adapters (excluded from unit coverage; no network in tests)

Both constructors: `(api_key: str, *, base_url: str | None = None, timeout: float = 60.0)` and build
the SDK client with **`max_retries=0`** and the explicit timeout — the worker's `Guard` owns retry
policy, not the SDK.

- `OpenAIClient.complete`: `client.chat.completions.create(model, temperature,
  max_completion_tokens, messages=[{"role":"system", ...}, {"role":"user", content=request.user_message}],
  response_format={"type": "json_schema", "json_schema": {"name": "emit", "schema": request.output_schema}})`.
  `finish_reason == "length"` → `InvalidOutput("reply truncated at max_tokens", content)`.
  Parse `choices[0].message.content` as JSON (`InvalidOutput` if not JSON). Usage from
  `response.usage.prompt_tokens/completion_tokens`. `base_url` makes any OpenAI-compatible gateway work.
- `AnthropicClient.complete`: `client.messages.create(model, system=request.system, temperature,
  max_tokens, messages=[{"role":"user", ...}], tools=[{"name": "emit", "description": "...",
  "input_schema": request.output_schema}], tool_choice={"type": "tool", "name": "emit"})`.
  The first `tool_use` content block's `.input` **is** the output; no block → `InvalidOutput`.
  `stop_reason == "max_tokens"` → `InvalidOutput`. Usage from `response.usage.input_tokens/output_tokens`.

Pin deps in `requirements.txt` (`jsonschema`, `openai`, `anthropic`) to versions published ≥ 7 days ago.
Add `llm_providers.py` to the coverage `omit` list in `pyproject.toml`, next to `db.py`/`messaging.py`.

### `config.py`

```
LLM_JOB_TYPES          comma-separated job types to register the handler under; default "llm_structured"
LLM_DEFAULT_PROVIDER   default "openai"
LLM_DEFAULT_MODEL      default "gpt-4.1-mini"
LLM_TIMEOUT            seconds, default 60.0
OPENAI_API_KEY, OPENAI_BASE_URL, ANTHROPIC_API_KEY, ANTHROPIC_BASE_URL   (empty -> None)
```

### `wiring.py`

```python
def build_llm_guard(provider: str, cfg: Config, *, sleep=time.sleep) -> Guard:
    # build_guard(f"llm-{provider}", cfg, sleep=sleep, non_transient=(InvalidOutput, BadPayload))
    # -> a schema violation or unusable payload says nothing about provider health:
    #    it must neither be retried by the guard nor count towards opening the breaker.

def build_llm_handler(cfg: Config, clients: Mapping[str, LLMClient], *, sleep=time.sleep) -> Handler | None:
    # {} clients -> None (deployment without LLM keys: don't register, don't crash)
    # cfg.llm_default_provider not in clients -> ValueError at startup (misconfiguration)
    # wrap every client in GuardedLLMClient(build_llm_guard(name, cfg)) and call structured_completion(...)
```

### `__main__.py`

```python
def llm_clients(cfg) -> dict[str, LLMClient]:   # only providers with an API key
handlers = dict(HANDLERS)
llm_handler = build_llm_handler(cfg, llm_clients(cfg))
if llm_handler is None: logger.warning("no LLM provider configured; job types %s will fail", cfg.llm_job_types)
else: handlers.update(dict.fromkeys(cfg.llm_job_types, llm_handler))
```

The `Handler` type must allow a return value (`Callable[[Envelope], Any]`) and the registry's
dispatch must return it (from Prompt 1).

### `deploy/schema.sql` + `docker-compose.yml`

Seed a demo config row for `llm_structured` (support-ticket summary: `system` + the
`summary`/`urgency` schema above) via `INSERT ... ON CONFLICT (job_type) DO NOTHING`. Pass the env
vars above through to the worker service with `${VAR:-}` defaults so the stack boots without keys.

## Failure mapping (test each)

| Situation | Handler behaviour | Run status |
|---|---|---|
| Output violates `output_schema` | retry once (same request), then raise `InvalidOutput` | `failed`, `result` NULL |
| Missing `system`/`input`/`output_schema`, invalid schema, unknown provider | raise `BadPayload` **before** calling the model | `failed` |
| Provider error (5xx, timeout, connection) | guard retries with backoff; breaker counts it | `failed` if retries exhausted while breaker closed |
| Breaker open | `CircuitOpenError` propagates out of `process()` | stays `running`; reaper requeues |
| Redelivered pointer for a completed job | claim guard skips; model never called | unchanged |

Note on pybreaker: when the failure that exhausts the retries is also the one that reaches
`fail_max`, pybreaker raises `CircuitBreakerError` on that same call — so with
`BREAKER_FAILURE_THRESHOLD=1` the *first* outage already surfaces as `CircuitOpenError` and the run
stays `running`. Write the test against that behaviour rather than expecting a plain provider error.

## Tests (`tests/test_llm.py`, in-memory doubles, through `process()`)

- `FakeLLM(*replies)` records `CompletionRequest`s and replays scripted outputs or raises scripted exceptions.
- Happy path: `system` reaches the request, `user_message` contains the serialised `input`, model
  defaults from config, `result_of(id)` has `output`/`provider`/`model`/`usage`/`attempts == 1`/`latency_ms`.
- Per-run overrides (`model`, `max_tokens`, `temperature`) beat config defaults.
- Schema violation twice → `failed`, two requests, no result. Violation then valid → `completed`, `attempts == 2`.
- `InvalidOutput` message names the field path (`urgency: 'urgent' is not one of [...]`) and carries `.raw`.
- Parametrised `parse_request` failures for each `BadPayload` case; bad payload never calls the model.
- Provider selection from payload; unknown provider → `BadPayload`; default provider without a
  client → `ValueError` at build time.
- Wiring: outage → `CircuitOpenError`, run stays `running`, second job rejected without reaching the
  fake; schema violations are *not* retried by the guard (exactly the handler's one retry).
- `build_llm_handler(cfg, {})` is `None`; default provider without key → `ValueError`.
- `Config` parses `LLM_JOB_TYPES` list and treats empty keys as `None`.
- Add a `test_service.py` case: a handler's return value lands in `result_of(id)`.

Keep the repo's 90% coverage gate green. `ruff check .` and `ruff format --check` must pass.

## Definition of done

1. Compose stack up with `OPENAI_API_KEY` (or `ANTHROPIC_API_KEY` + `LLM_DEFAULT_PROVIDER=anthropic`)
   → enqueue `{"job_type": "llm_structured", "input_payload": {"input": {"title": "VPN drops every 10 min", "body": "..."}}}`
   → `GET /jobs/{id}` returns `status: completed` and `result.output` matching the schema.
2. Adding a second use case (e.g. `xsoar_summary` with a different prompt/schema) requires only an
   `INSERT INTO job_type_config` and adding the type to `LLM_JOB_TYPES` — no code change.
3. With no API keys the worker starts, logs the warning, and `llm_structured` jobs fail as unhandled.
4. Lint, format, tests green; PR description explains the payload contract and the failure table.
