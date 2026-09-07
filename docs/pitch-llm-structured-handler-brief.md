# Brief: A Governed LLM Capability for APP

*One-page version of [pitch-llm-structured-handler.md](pitch-llm-structured-handler.md).*

**Ask:** one Tachyon API key (non-production first) for APP's job worker.

**Outcome:** every team gets a safe, audited, asynchronous way to apply a model to their data —
no new services, no new infrastructure, no per-use-case code.

## Problem

Teams want LLMs for routine text-heavy work: incident summaries, ticket triage, document
extraction, record classification. Today each attempt is its own project with its own credentials,
retries, logging, and audit gaps. Either nothing ships, or many small ungoverned integrations do.

## Proposal

Add one generic handler, `llm_structured`, to APP — our existing background job platform:

> JSON input + system prompt + JSON Schema → **one** model call via Tachyon → validate the reply
> against the schema → store it on the job record.

Each use case is a **configuration row**, not code: `xsoar_incident_summary`, `sir_triage`,
`invoice_extract`, `change_risk`, ... Callers use APP's API exactly as today. The model only reads
and answers in a fixed shape; a human or downstream system decides what to do with it.

## Why this shape

- **Governed by construction.** One key, held only by the worker. Every call is a job: requester,
  input, output, tokens, and latency are recorded in Postgres. Output that fails the schema is
  rejected. No tools, no actions, no cross-system access.
- **Reuses what we run.** No new service, queue, or pipeline — one module plus a nullable column.
  Retries, circuit breakers, timeouts, dedup, and stuck-run recovery are inherited.
- **Cheap to try and extend.** ~300 lines including tests, already prototyped; a Tachyon adapter is
  ~40 lines. A new use case is one `INSERT`; no release required.

## Cost control

Per-type `max_tokens` and model choice; schema-bounded output; input size limits; at most one
retry; identical inputs answered from the stored result; a daily token budget per type that fails
fast when exhausted; usage recorded on every run so cost per type/team/day is a query.

## Risks

| Risk | Control |
|---|---|
| Sensitive data to a model | Tachyon is the sanctioned interface; existing API authz decides who may enqueue each type |
| Wrong output | Strict schema, enums, `confidence` field; advisory only — humans act |
| Runaway spend | Hard caps, budgets, dedup, non-prod key first |
| Provider outage | Existing breaker + reaper; no cascade into other job types |
| Scope creep into agents | Out of scope; tool use would be a separately approved handler |

## Plan

1. **Week 0:** key issued (non-prod); wire adapter; run test suite and compose end-to-end.
2. **Pilot (2–3 weeks):** `xsoar_incident_summary`, advisory notes on cases tagged as
   AI-generated. Measure analyst rating, time saved, cost per case, schema-rejection rate.
3. **Review** with security leadership and model governance; decide on production key.
4. **Scale by configuration** with a lightweight review per new type (prompt, schema, owner, data
   classification).

## The ask

- A non-production Tachyon key for the job worker, with the usual model allowlist and quota.
- A named model-governance contact to review the pilot prompt/schema and sample outputs.
- Agreement in principle that successful pilot metrics unlock a production key.

One governed door, many use cases behind it.
