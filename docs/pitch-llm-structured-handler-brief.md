# Brief: A Governed LLM Capability for APP

*One-page version of [pitch-llm-structured-handler.md](pitch-llm-structured-handler.md).*

**Ask:** one Tachyon API key (non-production first) for APP's job worker.

**Outcome:** every team gets a safe, audited, asynchronous way to apply a model to their data —
no new services, no new infrastructure, no per-use-case code.

## Problem

Teams want LLMs for routine text-heavy work: incident summaries, ticket triage, document
extraction, record classification. Today each attempt is its own project with its own credentials,
retries, logging, and audit gaps. Either nothing ships, or many small ungoverned integrations do.

Worse, every such solution is **tightly coupled to the tool it lives in**: an XSOAR automation, a
ServiceNow script include, a Jira plugin. The prompt, the model call, the key handling, and the
output parsing are written in that tool's language, against that tool's API, and die with it. When
we move to a new SOAR or ticketing platform, all of it is rewritten from scratch — and the
rewrites drift from each other.

This proposal breaks that coupling. The LLM capability lives once, in APP, behind one API. The
tools become thin callers: enqueue a job, read the result. Prompts and schemas are portable
config rows, not tool-specific code. Swapping XSOAR for a new SOAR, or adding Jira, changes a few
lines of caller integration; nothing about the capability, its governance, or its audit trail is
touched.

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

## Security use cases

Each is one config row (prompt + output schema) behind the same handler; all are advisory and
written back to the system of record as an AI-generated note for an analyst to act on.

| Product | Job type | What the model produces |
|---|---|---|
| XSOAR / future SOAR | `incident_summary` | Handover summary, timeline, impact, recommended actions, suggested severity |
| XSOAR / future SOAR | `alert_dedup_hint` | Likely duplicate/related incidents with rationale, for playbook merge decisions |
| ServiceNow SIR | `sir_triage` | Category, severity, assignment group, extracted IOCs, confidence |
| ServiceNow SIR | `closure_notes` | Draft resolution/closure notes from the case worknotes |
| Vega | `finding_summary` | Plain-language summary and business impact of a finding for the asset owner |
| VECTR | `test_case_narrative` | Narrative of detection gaps and remediation suggestions from a purple-team run |
| Jira | `vuln_ticket_enrich` | Remediation steps, affected component, suggested priority and owner |
| Home-grown apps | `log_anomaly_explain` | Explanation and hypothesis for a flagged event, with fields to check next |
| Any | `ioc_extract` | Structured IOCs (IPs, domains, hashes, CVEs) from free-text reports or emails |
| Any | `phishing_triage` | Verdict, indicators, and user-facing response for reported emails |

Because APP fronts all of these through one API, a playbook in XSOAR, a flow in ServiceNow, or a
cron in a home-grown app onboards the same way: enqueue a job, poll the result. Swapping SOAR
vendors changes the caller, not the capability.

## The ask

- A non-production Tachyon key for the job worker, with the usual model allowlist and quota.
- A named model-governance contact to review prompts, schemas, and sample outputs.
- Agreement in principle that good non-production results unlock a production key.

One governed door, many use cases behind it.
