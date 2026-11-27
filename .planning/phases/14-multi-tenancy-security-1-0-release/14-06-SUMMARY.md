---
phase: 14-multi-tenancy-security-1-0-release
plan: 06
subsystem: dispatch (fair scheduler tenant level, node flow tenant quotas, executor fuel charge)
tags: [tenancy, quotas, fairness, drr, fuel, dispatch, DISP-06]
requires:
  - phase: 14-01b
    provides: "p14 seam (P14Ctx.quotas, quota_view::start in p14::start), flow/tenant.rs + quota_view.rs stubs, [tenants] config"
  - phase: 07
    provides: "leader/fair.rs (D-W10), node-wide flow counters + FlowPermit (D-W13), dispatcher admission/placement loop"
provides:
  - "leader/fair.rs: weighted DRR across tenants above the unchanged Phase 7 queue -> band -> key selector; TenantPolicy, tenant_key, block_tenant/release_tenant; weights clamped to 1..=10_000"
  - "Admit::TenantBlocked{until_ms} + FlowControl::tenant_weight (default 1)"
  - "p14/quota_view.rs: TenantQuotaView (meta-watched Tenant quotas, leadership share, change watch)"
  - "flow/tenant.rs: TenantFlow (node-wide per-tenant concurrency, instance-memory reservations, fuel bucket with debt, suspension), TenantPermit, ostro_tenant_* metrics"
  - "executor fuel charge: embedded = consumed fuel reported by the engine; remote = activation fuel limit (fallback)"
affects: [14-16, 14-17, 14-20, 14-23, 14-25, 14-26]
tech-stack:
  added: []
  patterns:
    - "tenant level as a wrapper of per-tenant Phase 7 selectors: queue/key/gate calls broadcast (node-wide limits), node-wide skip/park state seeds a tenant that appears later"
    - "tenant charge rides on the FlowPermit (rollback on drop, release at lease end), so placement-granted commit semantics are unchanged"
    - "engine -> server fuel report through an additive ActivationSink::fuel_consumed hook (default no-op)"
key-files:
  created:
    - crates/ostro-partition/tests/fair_tenant.rs
    - crates/ostro-server/tests/tenant_quotas.rs
  modified:
    - crates/ostro-partition/src/leader/fair.rs
    - crates/ostro-server/src/flow/tenant.rs
    - crates/ostro-server/src/p14/quota_view.rs
    - crates/ostro-server/src/executor.rs
    - "deviations (outside files_modified, minimal hunks): crates/ostro-partition/src/leader/{flow,dispatcher}.rs, crates/ostro-server/src/flow/mod.rs, crates/ostro-server/src/workers/{join,remote_executor}.rs, crates/ostro-engine/src/{sink,activation}.rs"
key-decisions:
  - "Tenant admission lives in NodeFlow::admit (after the queue/workflow pauses, before the queue limits) and returns Admit::TenantBlocked; the dispatcher blocks the tenant in the fair ring (until refill or the next changed() bump)"
  - "Fuel: embedded activations charge the fuel the engine reports (new additive ActivationSink::fuel_consumed); remote Done carries no fuel, so the activation fuel limit (engine.activation_fuel) is charged (D-P14 fallback)"
  - "Concurrency and memory release at lease end through the FlowPermit release hook (the D-W13 unit), not at the executor's activation end; the executor hunk is the fuel charge only"
  - "fuel_per_s = 0 is treated as unset and memory_bytes below one instance still admits one activation when the tenant holds nothing (quotas delay, never block forever)"
  - "Failover rebuild needs no extra read: rebuilt running runs are re-granted through admission (Entry::from_record already reads the RunRecord and its tenant), so per-tenant counts rebuild from the permits"
requirements-completed: [DISP-06]
metrics:
  duration: ~4h
  completed: 2026-11-26
---

# Phase 14 Plan 06: Dispatch-side tenant quotas Summary

Weighted DRR tenant level in the fair scheduler (Phase 7 order preserved for one tenant), node-wide per-tenant concurrency / instance memory / fuel-debt quotas on the leadership share with suspended-tenant holds, and the fairness evidence on a real node (1:2:4 shares exact, Jain 1.0).

## What was built

**Task 1, `leader/fair.rs` (pure).** `FairQueues` is now a weighted DRR ring over tenants (`""` = default tenant first) whose quantum is `Tenant.weight` clamped to `1..=10_000` (W16), supplied through `TenantPolicy` (`refresh_tenant_weights`, any `Fn(&str) -> u32` is one). Inside a tenant the Phase 7 selector runs unchanged (renamed `QueueSet`, code untouched). `block_tenant(t, until)` takes a tenant out of the ring (mirrors `KeyBlocked`) until `release_tenant`, `until` (`unpark_due`, `next_unpark_at`) or the next `changed()` bump (`release_held` / `unpark_all` release every blocked tenant). Queue, key and gate calls apply to every tenant's selector (queue/key limits, pauses and placement are node-wide); node-wide skip/park state seeds a tenant selector created later. `tenant_key(tenant, workflow)` = `ReadyValue.tenant`, else the D23 prefix of a qualified workflow (P14-F9), else `""`. The dispatcher files entries under their tenant, refreshes tenant weights each round and handles `Admit::TenantBlocked`.

**Task 2.** `TenantQuotaView` caches every `Tenant` record's dispatch fields in an `ArcSwap` snapshot (reloaded on meta applies by `quota_view::start`, which also follows `PartitionRoles::led()`); `share(x) = max(1, ceil(x*led/P))`, `0` when `led = 0`; until the roles are read the node assumes it leads every partition. Every swap bumps a watch that `NodeFlow` forwards to `changed()`. `TenantFlow::admit(tenant, mem_limit, now_ms)`: suspended -> `Blocked{None}`; `running >= share(max_concurrency)` -> `Blocked{None}`; `reserved + mem_limit > share(memory_bytes)` (and reserved > 0) -> `Blocked{None}`; fuel bucket (milli-fuel, refilled from the injected clock, burst default = 1 s of refill) `<= 0` -> `Blocked{until: refill instant}`; else a tentative `TenantPermit`. `charge_fuel` refills then subtracts (debt allowed). `NodeFlow` owns the `TenantFlow`, takes the view from the Phase 14 context in `flow::install` (via the join service's attached `P14Ctx`), and wraps the tenant permit into the `FlowPermit` (rollback on uncommitted drop, release at lease end; a capacity-limited tenant's release bumps `changed()`).

**Task 3.** `tests/tenant_quotas.rs` on a real single node (see evidence).

## Executor hunks (executor.rs, the only Phase 14 plan touching it)

- `ExhaustPolicy::charge_fuel(&self, record, consumed)` default no-op (NodeFlow, already the executor's exhaust policy, implements it: no p7.rs change).
- `EngineExecutor::charge_fuel` / `charge_remote_fuel` (remote = `runtime.engine().config().activation_fuel`).
- `EngineSink` gets an `on_fuel` callback (exhaust policy + record) that charges the tenant when the engine reports the fuel. The engine reports it right after the guest call and **before its terminal flush**, so the charge always lands before RunCompleted can be applied and the lease released (fix cbae331: charging after `activate()` returned let the next run be admitted against a stale balance under load).
- `workers/remote_executor.rs`: one line after `Done` is received: `engine.charge_remote_fuel(&record)`.

**Remote fuel source:** fallback. `WireOutcome` / `Done` carry no fuel (14-01 inventory), so every remote activation that returns `Done` charges the leader engine's `activation_fuel` (default 10^10, an upper bound). A tenant with a fuel quota whose runs go to remote workers is therefore throttled much harder than its real consumption; a `Done.fuel` field (worker protocol, additive) would fix it post-1.0.

## Evidence

Local (slot-4), `cargo test -p ostro-server --test tenant_quotas -- --test-threads=1`, three consecutive runs, all 6 tests green each time:

| Test | Result (run 1 / 2 / 3) |
|---|---|
| weights 1:2:4, saturated node (P=1, 1 slot, first 70 picks) | shares 0.143 / 0.286 / 0.571 vs 1/7, 2/7, 4/7 (exact, every run); Jain over weight-normalised throughput 1.0000 |
| noisy neighbour (P=1, 4 slots, a capped at 2, 300 runs vs b's 30) | b queueing p99 solo 0 ms, with a at 10x 0 ms (a backlog left at the end of b's load: 210 / 208 / 208) |
| fuel (rate 2 runs/s, burst 1 run, 6 runs, one at a time) | 2.03 s / 2.04 s / 2.04 s vs debt-aware bound 2.0 s (cpu-burn ms:20 = 105,007,552 fuel per run) |
| max_concurrency = 2 | sampled max 2, all complete |
| memory_bytes = 1.5 instances | sampled max 1 running, 1 instance reserved, all complete |
| suspended | none of 5 dispatched for 2 s, all complete after resume |
| /metrics | `ostro_tenant_running`, `ostro_tenant_blocked_total` present; no `tenant="` |

Notes on the evidence:
- Queueing = the run's first journaled clock (RunStarted at grant) minus `created_at_ms`, ms resolution. With `a` capped below the node's slots `b` never waits, so both p99s are 0 ms; the assertion is `noisy <= 2 * max(solo, 25 ms)` (25 ms floor = timer/scheduling noise, so the ratio cannot flake on a 0-1 ms baseline). The noisy-neighbour protection shown is the quota cap; with weights alone and no cap a saturating neighbour halves b's throughput by design (DRR 1:1), which is not a 2x-bounded queueing delay for a bursty b.
- Fuel bound: the plan's "total fuel / rate - burst" assumes charge-before-run; D-P14-14 charges after the activation (debt), so the last run's fuel can still be owed when it completes: bound = ((n-1)*fuel - burst)/rate. Measured times sit right on it (2.03 s vs 2.0 s).
- `fair_tenant.rs`: 10 tests; the single-tenant proptest compares every observable (candidate, take, len, per-queue counts/oldest/unblocked, queue names, next_unpark_at, held/parked flags) of the new scheduler against a verbatim copy of the Phase 7 `fair.rs` (kept in the test) for 256 random sequences (stressed once with 3,000); weighted shares within one quantum (proptest), weight 0 / u32::MAX clamped, blocked/timed-blocked tenants, weight-1 tenant served exactly once per round over 10,000 rounds.

## Gates

- Task 1 verify: `fair_tenant` 10 passed; `--lib -- fair` 17 passed (existing tests unchanged); `rebuild_digest` (PROPTEST_CASES=128) 8 passed; also `dispatcher_flow` 15, `p7_contract` 6.
- Task 2 verify: `--lib -- flow::tenant p14::quota_view` 11 passed; CLOCK_GATE_SCOPED clean (no new clock reads; fuel refill and `until` use the injected clock's ms).
- Task 3 verify: 3x green (above).
- `cargo clippy -p ostro-server -p ostro-engine -p ostro-partition --all-targets -D warnings` clean; `cargo fmt --check` clean; `scripts/p12-grep-gates.sh` rc 0.
- Plan gate: see "Plan gate" below.

## Deviations from Plan

### Auto-fixed / blocking (Rule 3)

**1. Files outside `files_modified` (minimal, additive hunks)**
- `ostro-partition/src/leader/flow.rs`: `Admit::TenantBlocked{until_ms}` and a defaulted `FlowControl::tenant_weight`. The plan's TenantBlocked result needs a variant of the existing admission contract.
- `ostro-partition/src/leader/dispatcher.rs`: entries filed with `insert_in(tenant_key)`, tenant weights refreshed per round, the `TenantBlocked` arm (+ 3 doc lines). Without it fair.rs never sees a tenant.
- `ostro-server/src/flow/mod.rs` (14-01b-owned, 14-01b done): `NodeFlow` owns the `TenantFlow`; the tenant check in `admit`; the tenant permit in `permit()`; `tenant_weight`; `ExhaustPolicy::charge_fuel`; `install_quotas` + the `flow::install` hook. This is where "the dispatcher admission path passes TenantFlow::admit" has to live (NodeFlow is the dispatcher's FlowControl).
- `ostro-server/src/workers/join.rs`: `pub(crate) fn p14()` getter, the only path from `flow::install` (P7) to `P14Ctx.quotas` without touching `server.rs` / `p14/mod.rs` (frozen seam).
- `ostro-server/src/workers/remote_executor.rs`: the remote fuel charge (one line); remote `Done` is handled there, not in executor.rs.
- `ostro-engine/src/{sink,activation}.rs`: additive `ActivationSink::fuel_consumed` (default no-op; `MeteredSink` forwards; called once with the consumed fuel already computed for `ostro_fuel_consumed_total`). Without it the executor only knows the fuel limit, which would make embedded fuel quotas charge 10^10 per activation.

**2. Proptest contract for the Phase 7 equality:** the first draft broadcast `skip_queue`/`park_*` from random states; the minimal counterexample (skip before any entry) showed that a tenant selector created later missed node-wide state. Fixed by seeding new selectors with the node-wide skip/park state, and the comparison drives flow-control answers on the proposed candidate (the dispatcher's contract).

### Not done / notes

- Metrics without labels are node totals; `ostro_tenant_fuel_balance` (no label) is the sum of the tenants' balances.
- Memory is reserved for remote activations too (the quota is the tenant's footprint, not this node's RAM).
- No `.github/workflows` change is needed (the new tests run in the existing `cargo test` steps).

## Plan gate

Not green as one run, because of cluster infrastructure. No product failure was left open:

1. `--parallel` attempt 1 (3422767): the runner exited at once with "PVC ostro-ci-par-0 missing" (transient; the retry started normally). Report only.
2. `--parallel` p204547-6737 (3422767 + docs), wall ~3.5 h under 5 concurrent parallel runs. PASS: test1, poly (all polyglot steps), fp1/fp2 (server campaign smoke 20 kills, replication mutant matrix), musl release + size limit, and in lint the Phase 13 tier C campaign (`cluster_campaign_p13` PASS: 2580 acked runs, 0 lost, 0 duplicates). FAIL, each passing alone on slot 1 at cbae331: fp3 `ostro-cluster` controller `a_new_controller_seeds_suspicion...` + `dead_leader_fails_over...` (10/10 alone); test2 `backup_cli::bucket_online_and_offline_round_trips` (/readyz > 60 s, known load flake; passes alone). cargo stopped test2 at that first binary, so test2's other binaries did not run in this attempt. obuild `template_tests::rust_template_test_passes` (passes alone, 59 s); musl2 `cli_p11::dlq_ls_dismiss_requeue_and_limits` (/readyz > 60 s; passes alone). lint: SIGKILLed during "phase 7 failpoint legs" while the cluster API was unreachable (infra).
3. Targeted slot 1 at 3422767: workspace `cargo fmt --check` + `cargo clippy --workspace --all-targets -D warnings` PASS; `cargo test -p ostro-partition -p ostro-engine` PASS; `cargo test -p ostro-server` (full) PASS except `tenant_quotas::fuel_quota_spreads...` (1.59 s vs the 2 s bound). Root cause: a real ordering race (fuel charged after the terminal append), fixed in **cbae331**.
4. Targeted slot 1 at cbae331: clippy (engine, server) PASS; `cargo test -p ostro-engine` PASS; `tenant_quotas` + `flow_control` + `executor` + `worker_remote` 3x PASS under pod load (fuel 2.07 / 2.10 / 2.12 s vs 2.0 s bound; shares exact; Jain 1.0).
5. `--parallel` p002134-27136 (cbae331): fp1/fp2/fp3 failpoint suites, replication mutant matrix, server campaign smoke and musl release/size PASS. musl1 zigbuild test FAIL only on `ostro-raft-driver fatal_apply_stops_before_applied` (known timing-sensitive test, listed in the brief). lint, test2 and musl2 never started ("ci container not started 10 min after the upload": node dell-1 not ready / API unreachable). poly's pod was never created in 240 min. test1's job completed, but its log was lost to the API outage (no summary).

**Orchestrator: a green `--parallel` at cbae331, or a later main, is still needed once dell-1 is healthy.** In particular, test2's binaries ran only in the slot-1 full `ostro-server` and partition/engine runs, not as the whole ci-local group.

## Self-Check: PASSED

- FOUND: crates/ostro-partition/tests/fair_tenant.rs, crates/ostro-server/tests/tenant_quotas.rs, crates/ostro-server/src/flow/tenant.rs (`pub struct TenantFlow`), crates/ostro-server/src/p14/quota_view.rs (`pub fn share`), `TenantBlocked` in fair.rs/dispatcher.rs, `charge_fuel` in executor.rs
- FOUND commits: bf39852, ec32a0c, 3422767, cbae331
