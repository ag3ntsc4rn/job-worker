# Proposal: A Governed LLM Capability for APP

**Ask:** one Tachyon API key (non-production first, then production) for APP's job worker, so we
can ship a generic, schema-validated LLM capability on a platform we already run.

**One-liner:** Add *one* handler to APP, and every team gets a safe, audited, async way to apply a
model to their data — no new services, no new infrastructure, no per-use-case code.

---

## 0. What is APP?

APP is our internal **background job platform**: a reliable way to say "run this task, exactly once
at a time, and tell me how it went." Teams use it for scheduled and on-demand back-office work
(data syncs, sweeps, integrations) so they don't each build their own queues, retries, and
monitoring.

How a job flows through APP, in plain terms:

```
1. Request   A team (or a system such as a SOAR playbook) calls APP's API with the
             kind of job it wants and the data for this run. The API authenticates the
             caller and records the job in a database with status "queued".
2. Dispatch  APP publishes a small "go do job #123" notice onto an internal message bus.
3. Run       A worker picks up the notice, claims the job so no other worker duplicates
             it, loads the job's configuration and data, and runs the matching handler.
4. Record    The worker marks the job "completed" (or "failed") and stores the result.
             The caller reads it back from the API.
5. Recover   If a worker dies mid-run, a watchdog notices and re-queues the job.
```

What matters for this proposal: APP already provides authentication, an audit record of every
run, one-at-a-time execution per job kind, timeouts, retries, and outage handling. A **handler**
is the pluggable piece of code that does the actual work in step 3 — and this proposal is about
adding one new, general-purpose handler.

## 1. The problem

Teams across security operations, IT, and business functions want to apply LLMs to routine,
high-volume, text-heavy work: summarising incidents, triaging tickets, extracting fields from
documents, classifying records. Today each attempt becomes its own project: a new script or service,
its own credentials, its own retry/timeout handling, its own logging — and usually its own way of
leaking a key or skipping an audit trail. The result is either nothing ships, or many small,
inconsistent, ungoverned integrations ship.

Meanwhile APP already solves the hard operational parts: authenticated requests, one active run
per job kind, retries and circuit breakers, stuck-run recovery, and a durable record of every run.

## 2. The proposal

Add a single generic handler to APP — `llm_structured` — that does exactly one thing:

> Take a JSON input + a system prompt + a JSON Schema for the answer → make **one** model call
> through Tachyon → validate the reply against the schema → store it on the job record.

Every use case is then a **configuration row**, not code:

| Job type (config row) | System prompt says… | Output schema |
|---|---|---|
| `xsoar_incident_summary` | "You are a SOC analyst writing a handover summary…" | `executive_summary, timeline[], impact, recommended_actions[], suggested_severity, confidence` |
| `sir_triage` | "Suggest category, severity and assignment group for this ServiceNow SIR…" | `summary, category, severity, assignment_group, iocs[], confidence` |
| `invoice_extract` | "Extract invoice fields; use null when absent; never guess amounts." | `vendor, invoice_number, total, currency, line_items[]` |
| `change_risk` | "Assess the risk of this change request…" | `risk_level, rationale, blast_radius[], questions[]` |

Callers use APP's API exactly as they do today:

```http
POST /jobs
{ "job_type": "xsoar_incident_summary",
  "input_payload": { "input": { ...the raw incident JSON... } } }

GET /jobs/6042
{ "status": "completed",
  "result": { "output": { "executive_summary": "...", "suggested_severity": "high", ... },
              "model": "...", "usage": {"input_tokens": 611, "output_tokens": 288}, "latency_ms": 3120 } }
```

The model never acts on anything. It reads, it answers in a fixed shape, and a human or a
downstream system decides what to do with the answer.

## 3. Why this is the right shape

**Governed by construction.**
- *One key, one place.* Tachyon credentials live only in the worker's environment. No team ever
  handles a model key; they call our API with the JWT they already have.
- *Every call is a job.* Who requested it, when, with what input, what came back, how many tokens —
  all in Postgres. Audit and cost attribution are free.
- *Schema-enforced output.* Replies that don't match the declared JSON Schema are rejected (one
  retry, then the job fails). Downstream consumers never see free text or hallucinated fields.
- *Read-only by default.* The handler has no tools, no network access to other systems, no ability
  to change state anywhere. Prompt-injection risk is bounded: the worst case is a wrong summary,
  which is validated, logged, and reviewed by a human.
- *Bounded spend.* `max_tokens`, per-type model choice, per-type deduplication (one active run per
  type), and run timeouts are all configuration. Token usage is recorded per run.

**Reuses what we already run.**
- No new service, queue, database, or deployment pipeline. The change is one module and a
  nullable `result` column.
- Resilience is inherited: Tachyon outages trip the existing circuit breaker, runs stay recoverable,
  and the reaper re-queues them — no lost work, no retry storms against the provider.
- Idempotent and at-least-once, like every other handler.

**Cheap to try, cheap to extend.**
- Roughly 300 lines of code with tests, already prototyped and unit-tested against fake providers
  (OpenAI- and Anthropic-style adapters exist; a Tachyon adapter is a ~40-line class).
- A new use case = one `INSERT` into `job_type_config`. No release required.
- Prompt and schema changes are data changes, versioned in the config table.

## 4. What we get (concrete value)

- **Security operations:** analyst-ready incident summaries and triage suggestions on every
  XSOAR / ServiceNow SIR case within minutes of creation — advisory, written to the case, reviewed
  by the analyst. Target: cut time-to-first-assessment and handover effort materially on the
  highest-volume alert classes.
- **IT service management:** urgency/category/routing suggestions and draft resolution notes on
  inbound tickets.
- **Back office:** structured extraction from invoices, contracts, and emails feeding existing
  AP/ERP validation — humans review the exceptions, not every document.
- **Platform teams:** root-cause hypotheses for failed jobs and alert-storm summaries for on-call.

Each of these is the same handler with a different config row. The platform value compounds: the
second use case costs a config row; the tenth costs a config row.

## 5. Risks and how they're contained

| Risk | Control |
|---|---|
| Sensitive data sent to a model | Tachyon is our sanctioned model interface; inputs are whatever the caller already has rights to; per-type prompts can instruct redaction; type-level allowlist of who may enqueue (existing API authz). |
| Wrong or fabricated output | Strict JSON Schema; enum-constrained fields; `confidence` field in schemas; advisory-only — a human acts, the model does not. |
| Runaway cost | `max_tokens` + cheap default model per type; one active run per type; token usage logged per run for attribution; key scoped to non-prod first. |
| Provider outage | Existing breaker + reaper: runs wait and retry; no cascading failure into the API or other job types. |
| Prompt injection | No tools, no actions, no cross-system access; output must fit the schema; worst case is a bad summary that is logged and reviewed. |
| Scope creep into "agents" | Explicitly out of scope; a tool-using mode would be a separate, separately approved handler. |

## 6. Plan

1. **Week 0 — key issued (non-prod).** Wire the Tachyon adapter; run the existing test-suite plus a
   compose end-to-end run with a `hello`-style demo type.
2. **Pilot (2–3 weeks).** One use case — `xsoar_incident_summary` — advisory only, results written
   back to the case as a note tagged as AI-generated. Measure: analyst rating of summary usefulness,
   time saved per case, tokens/cost per case, schema-rejection rate.
3. **Review.** Share metrics and a sample of outputs with security leadership and the model
   governance owners. Decide on production key and on the next two config rows.
4. **Scale by configuration.** Onboard use cases via config rows with a lightweight review
   (prompt + schema + owner + data classification) — no engineering cycle per use case.

## 7. The ask

- A Tachyon API key for the job worker (non-production), with the usual model allowlist and quota.
- A named contact on the model-governance side to review the pilot's prompt/schema and sample
  outputs.
- Agreement in principle that successful pilot metrics unlock a production key.

Everything else — code, tests, deployment, monitoring — reuses the platform we already own and
operate. This is the lowest-risk, highest-leverage way for us to put models to work: one governed
door, many use cases behind it.
