---
phase: 14-multi-tenancy-security-1-0-release
plan: 20
subsystem: tenant ingress quotas (ostro-server p14/ingress.rs; cron fire / trigger-now gate in control/schedules.rs)
tags: [disp-06, quotas, gcra, event-rate, suspension, ingress, kafka, cron, webhooks]
requires:
  - phase: 14-01b
    provides: "p14::admit_ingress call sites (start after the dedupe read, signal, token), IngressKind::Start{dedupe_hit, trigger}, TriggerKind::of_key (__cron/, __hook/, __kafka/)"
  - phase: 14-02
    provides: "ErrorCode::PermissionDenied 403, reason::{TENANT_SUSPENDED, TENANT_EVENT_RATE}"
  - phase: 14-06
    provides: "TenantQuotaView::{share, led, partitions}; dispatch holds a suspended tenant's ready runs"
  - phase: 14-16
    provides: "tenant-scoped start tenant, run-record tenant, tenant schedules (<t>/<name>) and webhooks (/hooks/<t>/<name>)"
  - phase: 14-32
    provides: "Kafka driver: 429 / 403 pause the Kafka partition and retry the record, never skip"
provides:
  - "p14::ingress::admit (admit_ingress): suspended -> 403 tenant_suspended (first, every kind); dedupe hit free; event_rate GCRA over share(limit)/period with burst -> 429 rate_limited + reason tenant_event_rate + retry_after_ms"
  - "p14::ingress::EventRates (per-tenant TAT table; lazily rebuilt on a led change), rates_of(ctx) (per-server table keyed by incarnation, dropped at services stop)"
  - "p14::ingress::cron_fire_gate (schedule fires and trigger-now of a suspended tenant)"
  - "metric ostro_tenant_ingress_rejected_total{reason=suspended|event_rate, kind=start|webhook|kafka|cron|signal|token}"
  - "tests: p14::ingress unit tests (10), tests/tenant_ingress.rs (3)"
affects: [14-25, 14-26, 14-27]
tech-stack:
  added: []
  patterns:
    - "Phase 7 flow::limits::Gcra reused verbatim (ns from the injected clock); conformance check + advance under one mutex, a refusal charges nothing"
    - "hook state the frozen P14Ctx cannot hold lives in a process-global map keyed by P14Ctx.incarnation, removed on P14Ctx.shutdown"
key-files:
  created:
    - crates/ostro-server/tests/tenant_ingress.rs
  modified:
    - crates/ostro-server/src/p14/ingress.rs
    - crates/ostro-server/src/control/schedules.rs
key-decisions:
  - "Schedule fires bypass the API, so a suspended tenant's fire is refused in build_schedule_start (StartError::Skip: the driver drops that fire, counts it and fires again on the next due time); fires missed while suspended are not replayed by catch-up"
  - "Fires are not charged against event_rate (D-P14-14 lists start, signal, token, webhook, Kafka); trigger-now is not charged either (it does not pass admit_ingress), only its suspension is checked"
  - "burst is used as configured (not scaled by the leadership share); the rate is share(limit) per period, as the plan specifies"
  - "Tenant record read straight from the meta view on each admission (suspended + event_rate); TenantQuotaView only supplies share/led"
requirements-completed: []
duration: ~2h
completed: 2026-11-27
---

# Phase 14 Plan 20: Ingress quotas Summary

**Per-tenant `event_rate` (the Phase 7 GCRA over the D-P14-15 leadership share, injected clock) and suspended-tenant refusal on every ingress path: API/gRPC start, signal, await-token completion, webhook delivery, Kafka-triggered start, schedule trigger-now and cron fires. 429 `rate_limited` + `tenant_event_rate` + `Retry-After`, 403 `tenant_suspended`; dedupe hits are free; nothing is proposed on refusal.**

## Performance

- Duration: about 2 h
- Completed: 2026-11-27
- Tasks: 2 of 2 (+ 1 fix commit)
- Files: 3 (1 created, 2 modified)

## Accomplishments

- `p14/ingress.rs::admit` (`p14::admit_ingress`). Rules, in order: no tenant record means admitted; `suspended` means 403 `permission_denied` with `details.reason = "tenant_suspended"` for every kind, dedupe hits included; `Start{dedupe_hit: true}` is admitted free (D-W21); no `event_rate` means admitted; otherwise the GCRA runs (`T = period / share(limit)`, `τ = T × (burst − 1)`). Excess gets 429 `rate_limited` with `details.reason = "tenant_event_rate"` and `details.retry_after_ms` (the earliest conforming instant, ceil ms, at least 1). `Retry-After` and gRPC `RESOURCE_EXHAUSTED` come from the unchanged Phase 7 error encoding.
- State: one TAT per tenant per server. The table is rebuilt lazily (TAT restarts at now) when `TenantQuotaView::led()` changes. A spec change keeps the TAT. Idle cells are pruned above 1,024 entries. The table is dropped when `P14Ctx::shutdown` fires.
- Every refusal increments `ostro_tenant_ingress_rejected_total{reason, kind}`. There is no tenant label.
- `cron_fire_gate`: refuses a suspended tenant's schedule fire inside `build_schedule_start`, before anything is written, and also before trigger-now.

## Measured (tests/tenant_ingress.rs, local, debug build)

- Tenant a with `event_rate = 10/1s burst 10`: 30 concurrent REST starts gave **10 accepted / 20 x 429** in each of 4 recorded runs (42-58 ms per burst). Every 429 carried `Retry-After` = ceil(retry_after_ms / 1000) and retry_after_ms <= 1000. gRPC returned `RESOURCE_EXHAUSTED` with `{reason: tenant_event_rate, retry_after_ms}` within at most 5 further starts. Re-submitting the accepted key `keep-1` 5x over budget gave 200 `created: false` each time, never 429. Tenant b (same limit, own budget): 10/10 accepted right after. Waiting retry_after_ms, then starting, succeeded.
- One budget shared across kinds (`2/60s burst 2`): signal 1 OK, token 1 OK; then token 2, signal 2, `POST /hooks/a/x` and a `__kafka/k1/t/0/1` start were all 429 `tenant_event_rate`. A re-submitted `__kafka/k1/t/0/0` (dedupe hit) returned the existing run. The refused token completion was not applied (run 2 not completed).
- Suspension: start (REST + gRPC `PERMISSION_DENIED`), signal, token, webhook, Kafka-keyed start and trigger-now were all 403 `tenant_suspended`. Tenant b was unaffected. **Cron-skip behaviour:** a `* * * * * *` schedule created **0 runs** while suspended (runs before = after = 1). There were **4 counted `kind="cron"` rejections in 4.0 s**, i.e. one per due fire through the driver's existing bounded `Skip` path, with no retry spin (repeated identically in every run). After the resume the schedule fired again. A run delayed 3 s and accepted before the suspension was `scheduled`/`ready` (held, not failed) during it and `completed` after the resume.
- Repeats: tenant_ingress 3/3 passed 5x in a row after the last change. One timing assertion was dropped before commit: the "start after gRPC + dedupe is 429" check failed 2/3 because the budget refilled (> 100 ms elapsed).

## Task Commits

1. **Task 1: admit_ingress (event_rate GCRA on the leadership share, suspended) + unit tests**: `4168e15` (feat; includes the one-line cron fire gate in schedules.rs)
2. **Fix: trigger-now of a suspended tenant answers 403** (found by Task 2): `4a809c9` (fix)
3. **Task 2: server-level ingress tests**: `ad1049d` (test)

## Files Created/Modified

- `crates/ostro-server/src/p14/ingress.rs`: admission, EventRates, rates_of, cron_fire_gate, metric, 10 unit tests (burst then steady, retry_after exactness, tenant independence, share scaling + rebuild on leadership change, pruning, labels/errors, admit on the injected clock incl. dedupe-free and Kafka/webhook charging, admit share via `set_led(P/2)` gives half the rate, suspended precedence + cron gate + resume, table dropped at shutdown).
- `crates/ostro-server/src/control/schedules.rs`: `cron_fire_gate` in `build_schedule_start` (cron fires) and in the trigger-now handler (+8 lines).
- `crates/ostro-server/tests/tenant_ingress.rs`: 3 server tests (REST, gRPC, signals, tokens, webhooks, Kafka keys, schedules, held run).

## Decisions Made

See frontmatter key-decisions.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing critical functionality] Cron fires never reach admit_ingress**
- **Found during:** Task 1
- **Issue:** The plan's interface says "webhook and cron starts reach it through the server start path". Webhooks do, but cron **fires** are built by the Phase 8 driver through `ScheduleSource::build_start` (`control/schedules.rs::build_schedule_start`) and never call `start_run`. 14-01b records this ("cron fires bypass the API"). Without a gate, the truth "a schedule of a creates no runs while suspended" could not hold.
- **Fix:** `p14::ingress::cron_fire_gate(meta, tenant)`, called once in `build_schedule_start` before anything is written. A refusal is `StartError::Skip`, so the driver drops that fire and counts it as `start_error`.
- **Files modified:** crates/ostro-server/src/control/schedules.rs (outside files_modified; not a 14-01b call-site file)
- **Commit:** 4168e15

**2. [Rule 1 - Bug] Trigger-now of a suspended tenant answered 412**
- **Found during:** Task 2
- **Issue:** Trigger-now also goes through `build_schedule_start`, not `start_run`. Its error mapping turns `Skip` into 412 `precondition_failed` and drops the reason.
- **Fix:** trigger-now calls `cron_fire_gate` first and returns its 403 `tenant_suspended` directly.
- **Files modified:** crates/ostro-server/src/control/schedules.rs
- **Commit:** 4a809c9

**Plan wording vs behaviour (no code change):**
- "missed fires per the schedule's own catch-up policy": with the Skip path, fires due during the suspension are dropped (the floor advances). After the resume the schedule continues from its next due time and catch-up does **not** replay suspended fires. Replaying them would need `StartError::Retry`, which blocks the whole partition's cron driver (other tenants' schedules) until the resume. That trade-off was rejected; flagged for the orchestrator.
- The plan's verify reads `$(git rev-parse --git-dir)/p14-base`, which does not exist in this worktree. The clock gate was run with base 616917b (the worktree base): no `SystemTime::now(` / `SystemClock` / `Instant::now(` additions in `crates/ostro-server/src`. `scripts/p12-grep-gates.sh` passed.
- The real Kafka driver against a broker was not exercised here. The API-level `__kafka/` classification and charging is tested. The driver's pause on 429/403 is covered by 14-32's `rate_limited_start_pauses_then_resumes` (`kafka.rs::classify` pauses on RateLimited and PermissionDenied).

## Issues Encountered

- `P14Ctx` and `p14/mod.rs` are frozen, so the GCRA table cannot be a context field. It lives in a process-global map keyed by `P14Ctx.incarnation` and is removed when `P14Ctx.shutdown` is cancelled.

## Verification

- Task 1 verify: `cargo test -p ostro-server --locked --lib -- p14::ingress`: 10 passed.
- Task 2 verify: `cargo test -p ostro-server --locked --test tenant_ingress -- --test-threads=1`: 3 passed (5x in a row). Clock gate: no hits.
- `cargo clippy -p ostro-server --locked --all-targets -- -D warnings`: clean. `cargo fmt -p ostro-server -- --check`: clean.
- Regressions: p14_skeleton 7, schedules 4, tenant_quotas 6, tenant_triggers 3, lib `p14::` + `control::schedules` 52, all passed.
- **Plan-level gate: gate incomplete — cluster withdrawn; consolidated gate pending on dell-4.** Unverified: every ci-local group at ad1049d (lint incl. the full workspace clippy/doc steps, test1/test2 cargo-test shards beyond the suites above, failpoint suites, musl build + size step, polyglot/obuild, Phase 12/13 tier C); `kafka_trigger` against the broker (not run locally; this plan does not change the driver); `tenant_ingress` under CI load (the burst asserts `10 <= accepted <= 11 + elapsed/100 ms`, and the cron window asserts at most elapsed s + 2 rejections).

## For Later Plans

- 14-25 (docs): document `ostro_tenant_ingress_rejected_total{reason,kind}`, the rule order, and that fires missed while suspended are dropped, not caught up. Also that `burst` is not scaled by the leadership share.
- 14-27: no new ci-local step needed (`tests/tenant_ingress.rs` runs in the ostro-server cargo test shard).
- Fixup: the `p14/mod.rs` doc for `TriggerKind::Cron` says "`__cron/` key: the manual trigger". Trigger-now actually bypasses `start_run` too (it uses `build_schedule_start`), so trigger-now is never event-rate charged. Only its suspension is checked (by this plan).

## Self-Check: PASSED
