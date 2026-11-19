# WIT 0.1 pre-freeze review (D14)

**Status: adopted default, USER REVIEW.** The planner adopted this on 2026-11-19 under YOLO/auto-advance; see [05-DECISIONS.md](../.planning/phases/05-workflow-semantics-core-hardening/05-DECISIONS.md), D14. A user override that lands before the 05-20 freeze flip can still be folded in. After 05-20, `wit/ostro-runtime-0.1.0/runtime.wit` is byte-pinned (`wit/FROZEN.toml`), and every change goes into a new `ostro:runtime@0.1.N` package that `ostro-witcheck` proves additive.

**Outcome: no record, variant or enum shape changes in Phase 5.** Every candidate need from Phases 6–9 and 14 either fits the current shapes, or takes the additive path later: a new function or interface in a `0.1.N` package. The Phase 5 edits to `runtime.wit` (plan 05-05) are doc comments only. Verification: with comment lines removed, the file is byte-identical to the plan's base commit (`target/05-05-base.sha`).

Why shapes cannot change after the freeze: the component model checks record fields, variant cases and enum cases structurally, so an old guest that imports `run-info` with 9 fields would fail to link against a host whose `run-info` has 10. Several records are also mirrored, field by field, in postcard journal structs (`crates/ostro-types/src/journal.rs`) whose layout is frozen with `EVENT_CODEC_VER` 1.

## 1. Records, variants, enums

Column guide:
- **Journaled?** names the journal mirror struct and the event that carries it. "Hash" means the value feeds the command hash (`CommandArgs`).
- **Additive path** is the post-freeze route if the need materialises.

| Name | Kind | Fields / cases | Phase 6–9/14 need that could want more | Journaled? | Outcome (D14) | Additive path |
|---|---|---|---|---|---|---|
| `types.payload` | record | content-type, data | — (PF-W5 JSON convention is documentation) | `Payload` in RunStarted, StepCompleted, RunCompleted, signals, child/token completions | unchanged | — |
| `types.op-id` | type alias (u64) | — | JS precision (PF-W3) | as `OpId` everywhere | unchanged; doc comment | — |
| `types.datetime` | record | seconds, nanoseconds | — | `SqlValue::Timestamptz`, sleep-until hash | unchanged | — |
| `types.error-info` | record | kind, message, details | Reserved bridge and child kinds (W8, PF-F5) | `ErrorInfo` inside `Failure` | unchanged; kinds documented | new kinds are strings: no shape change |
| `types.failure` | variant | retryable, non-retryable, timeout, cancelled | Terminated child outcome (PF-W7) | `Failure` in every outcome | unchanged | `non-retryable{kind: "child_terminated"}` instead of a new case |
| `types.retry-policy` | record | max-attempts, initial/max interval, coefficient, non-retryable-kinds | jitter, per-kind policies | not journaled (host-applied; the `HttpRequest` mirror omits `retry`) | unchanged; PF-W6 semantics documented | new `begin-step-with` / `http-fetch-with(req, policy-v2)` in 0.1.N |
| `types.step-options` | record | retry, timeout-ms, fuel | heartbeat, schedule-to-close | not journaled (step hash = name) | unchanged; fuel portability documented | new `begin-step-ext(name, opts-v2)` in 0.1.N |
| `types.run-info` | record | run-id, workflow, workflow-version, attempt, module-hash, tenant, parent-run-id, deadline, continued-from | queue and tags (WF-06 / Phase 7), retry-of, schedule id (Phase 8) | derived from `RunStarted` (not a mirror) | unchanged; attempt/tenant/deadline/module-hash docs | new `context.run-labels() -> run-labels` (queue, tags, …) in 0.1.N |
| `journal.completion-ack` | variant | recorded, retry-now | — | not journaled | unchanged | — |
| `journal.step` | resource | seq(), complete() | heartbeat | — | unchanged; open-step rule documented (PF-W1) | new method on the resource in 0.1.N |
| `journal.step-start` | variant | replayed, execute | — | not journaled | unchanged | — |
| `effects.header` | record | name, value | secret templating (W1) | `Header` in `HttpRequest`/`HttpResponse` | unchanged; W1 documented | — |
| `effects.http-request` | record | method, url, headers, body, timeout-ms, idempotency-key, retry, redact | FX-01 redirect policy, response size cap, per-request TLS options | `HttpRequest` in EffectScheduled (no `retry`); hash = method, url, body, header names | unchanged; W1–W4 documented | new `http-fetch-ext(req, http-options)` in 0.1.N |
| `effects.http-response` | record | status, headers, body | streaming / truncated bodies | `HttpResponse` in EffectCompleted outcome | unchanged; W5 documented | `response_too_large` kind; streaming = new interface |
| `effects.sql-value` | variant | null, boolean, int8, float8, numeric, text, bytea, json, timestamptz | uuid, arrays, date/time without zone | `SqlValue` in SqlRequest/SqlResult, hash | unchanged | encode as `text`/`json`; typed values = new `sql-query-v2` with a new variant type |
| `effects.sql-request` | record | connection, statement, params, exactly-once | batches, isolation level | `SqlRequest` in EffectScheduled, hash | unchanged; W6 documented | new `sql-batch(req-v2)` in 0.1.N |
| `effects.sql-result` | record | columns, rows, rows-affected | column types | `SqlResult` in EffectCompleted | unchanged | new function with a richer result |
| `effects.object-request` | record | connection, op, key, body | FX-05 presign expiry, content-type, metadata | `ObjectRequest` in EffectScheduled, hash | unchanged; W7 op values documented | new `object-presign(req: object-presign-request) -> op-id` (expiry, content-type) in 0.1.N |
| `effects.child-options` | record | queue, version, detached, run-timeout-ms | WF-06 tags, schedule-to-start, id reuse policy | `ChildOptions` in ChildStarted | unchanged; PF-W7 detached contract documented | new `start-child-ext(workflow, input, child-options-v2)` in 0.1.N |
| `effects.await-token` | record | op, token | token expiry metadata | token string in AwaitTokenCreated | unchanged; one-shot documented | — |
| `effects.op-output` | variant | http, sql, object, timer, signal, child, token, cancel-requested | signal metadata (sender, request id), effect metadata | `OpOutput` in completion outcomes | unchanged | `wait-signal` metadata: new `wait-signal-ext` whose payload is a JSON envelope, or a new `signal-info(op)` function in 0.1.N |
| `effects.completion` | record | op, outcome | — | outcome bytes in WaitResolved/RecordedResult | unchanged | — |
| `effects.wait-mode` | enum | all, any | "n of m" | `WaitMode` in Wait hash | unchanged; PF-W2 documented | new `wait-n(ops, n)` in 0.1.N |
| `log.level` | enum | trace, debug, info, warn, error | — | not journaled | unchanged | — |
| `workflows.workflow-meta` | record | name, version, default-queue, run-timeout-ms | retention, schedule-to-start defaults | not journaled (read at upload) | unchanged; name grammar documented. Retention and s2s defaults stay **API/meta policy** (`WorkflowPolicy`, D9/D2) | a new world in 0.1.N (e.g. `workflow-ext`) that also exports a `describe-ext` interface; the host probes for it, and components targeting `workflow` keep working (the `workflow` world itself never gains exports) |
| `workflows.continuation` | record | input, workflow, version | carry tags/memo | `Continuation` in RunContinuedAsNew | unchanged; in-tenant resolution documented | — (tags/tenant/traceparent are copied by the host, F11) |
| `workflows.run-result` | variant | completed, continue-as-new | — | `RunCompleted` / `RunContinuedAsNew` | unchanged | — |

## 2. Doc-comment items (all: outcome "doc comment only")

Every row is a `///` (or header `//`) comment in `wit/ostro-runtime-0.1.0/runtime.wit`; no other byte changed.

### Phase 5 semantics (05-RESEARCH / 05-DECISIONS)

| Item | Location in runtime.wit | Source |
|---|---|---|
| FIFO signals; buffered-before-wait; oldest-op first; timeout → `failure::timeout`; always evicts, never hot-parked (`hot_waitable` false) | `effects.wait-signal` | D13/D21, 05-03; `crates/ostro-engine/src/host/effects.rs` `hot_waitable_op` |
| cancel-op → `failure::cancelled` unless already completed earlier in journal order | `effects.cancel-op` | D5 |
| `patched` memoised per change-id; unqueried markers ignored on replay | `context.patched` | D6 |
| Step-local RNG keyed on the step seq, fresh stream per attempt | `context.random-bytes` (+ `context` interface header) | D7 (USER REVIEW) |
| Await token one-shot; later completions rejected; timeout → `failure::timeout`; token waits always evict | `effects.create-await-token`, `effects.await-token` | D4, 05-03 |
| `run-info.deadline` = creation + run timeout; hard stop, no cleanup | `types.run-info.deadline` | D2 |
| `run-info.module-hash` = hash of the executing module | `types.run-info.module-hash` | 05-05 |
| SDK cancellation surfacing is SDK policy | `effects.cancellation` | D11 (SDK-only; no WIT change) |
| D12 (`ostro deploy`) | — (no WIT surface) | D12 |

### Phase 6 effect semantics (06-RESEARCH § "Pre-Freeze Findings")

| Item | Location in runtime.wit |
|---|---|
| W1 secret templating: header values only (+ body if granted), never URL/method/names, no redirects with placeholders, origin-scoped, `[REDACTED]` in responses | `effects.header` (record doc); `effects.http-request` points to it |
| W2 derived `ostro-<32 hex>` `Idempotency-Key`, stable across attempts/resumes, new for retry-as-new / continue-as-new | `effects.http-request.idempotency-key` |
| W3 redact applies to the journal, logs **and** response headers returned to the guest | `effects.http-request.redact` (replaces the old journal/log-only wording) |
| W4 per-attempt timeout; retryable conditions; same key; `effect_outcome_unknown` past the idempotency window | `effects.http-request.timeout-ms`, `effects.http-request.retry` |
| W5 any final status = success; `response_too_large` | `effects.http-response` |
| W6 exactly-once SQL: transaction + `ostro_effects` token row, stored result on re-drive, `sql_not_transactional`, `false` = at-least-once | `effects.sql-request.exactly-once` (replaces "host-side dedupe table") |
| W7 object op values, output encodings, presign expiry from config, `unsupported_op` | `effects.object-request.op` |
| W8 reserved bridge error kinds (+ Phase 5 kinds `timeout`, `cancelled`, `unsupported`) | `types.error-info` |

### Phase 7 (07-RESEARCH F4)

| Item | Location in runtime.wit |
|---|---|
| F4 `attempt` increases on every re-dispatch, not replay-stable, logging/metrics only, never for control flow (replaces "stable across replays") | `types.run-info.attempt`; `types.run-info` record doc |

### Phase 9 (09-RESEARCH § "Pre-Freeze Findings")

| Item | Location in runtime.wit |
|---|---|
| PF-W1 open-step rule | `journal` interface doc; referenced from `journal.step` |
| PF-W2 `wait` semantics (`any` returns every completed op by completion seq, re-wait, duplicates, empty `any`, no append when unsatisfied, one `WaitResolved` per satisfied wait) | `effects.wait` |
| PF-W3 opaque u64 op ids, cancellation = 2^64-1, `u64::MAX-15..` reserved, `bigint` | `types.op-id`; `effects.cancellation` |
| PF-W4 UTF-8 scalar values, lone surrogates, `invalid_string` | `types` interface doc |
| PF-W5 `application/json` = UTF-8 JSON without BOM, empty = `null` | `types.payload` |
| PF-W6 `retry: none`, `max-attempts` counts the first, coefficient clamp, saturation, fuel portability, `random-bytes` ≤ 65536 | `types.retry-policy`, `types.step-options` (+ `fuel`), `context.random-bytes` |
| PF-W7 child contract: outcome mapping, CAN chains followed, cancel-op cascades, detached contract, deterministic child ids | `effects.start-child`, `effects.child-options.detached`, `effects.cancel-op` |
| PF-F3 attached children get a graceful cancel at any parent terminal status | `effects.start-child` |
| PF-W8 fresh instance per activation, trap-ends-activation (no finalizers), no imports during pre-init, `describe` pure + unique | `workflows` interface doc; `workflows.describe` |
| PF-W9 `uuid-v7` lowercase 8-4-4-4-12; `random-u64`/`random-bytes` share a stream, `uuid-v7` has its own | `context.uuid-v7`, `context.random-u64`, `context` interface doc |

### Phase 14 (14-RESEARCH P14-F10, D23)

| Item | Location in runtime.wit |
|---|---|
| `run-info.tenant`: `none` = default tenant, immutable, inherited | `types.run-info.tenant` |
| Names seen in WIT are unqualified (grammar, reserved `/`, `:`, leading `__`) | `types.run-info.workflow`, `workflows.workflow-meta.name`, `world workflow` doc |
| Child, continuation, connection and secret names resolve inside the run's tenant | `world workflow` doc; `effects.start-child`, `workflows.continuation`, `effects.sql-request`, `effects.object-request`, `effects.header` |
| Tenant quotas delay work and never fail guest calls | `world workflow` doc |

## 3. What 05-20 does with this

1. Folds in any user override of this review (doc comments only, or, if the user rejects D14, a shape change before the freeze).
2. Creates `wit/FROZEN.toml` with the final sha256 of `runtime.wit`, so `frozen_0_1_0_is_byte_identical` starts enforcing.
3. Tags `wit/ostro-runtime-v0.1.0` (local tag).

After that, the "Additive path" column is the only route for every row above.
