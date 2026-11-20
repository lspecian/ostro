---
phase: 08-triggers-bottomless-observability
plan: 16
subsystem: observability
tags: [metrics, prometheus, obs-02, dispatcher, engine, raft, dlq]

requires:
  - phase: 08-triggers-bottomless-observability
    provides: 08-04 services seam (ServiceCtx, metrics_poll stub, /metrics with _seconds buckets); 08-07 schedule apply; 08-11 run_spans RunCompleted consumer
provides:
  - "ostro_queue_ready{queue}, ostro_queue_lag_seconds{queue}, ostro_runs_started_total{workflow} (dispatcher); ostro_queue_worker_demand described only"
  - "ostro_steps_total{workflow,outcome}, ostro_step_duration_seconds{workflow}, ostro_replay_events_total, ostro_replay_duration_seconds, ostro_fuel_consumed_total{workflow}, ostro_instance_memory_bytes, ostro_wasm_memory_budget_bytes, ostro_parked_instances (engine)"
  - "ostro_runs_completed_total{workflow,status}, ostro_dlq_total{reason} (server run_spans consumer, always on)"
  - "services::metrics_poll: 5 s poller for ostro_partition_apply_lag, ostro_partition_hw_lag, ostro_isr_size, ostro_wal_log_start/end, ostro_raft_{term,commit_index,applied_index,is_leader}"
  - "MetaHandle::raft_status() -> MetaRaftStatus {term, commit_index, applied_index, is_leader}"
  - "docs/metrics.md: every ostro_* series with type, labels, unit, source, plan"
affects: [07-05, 07-16, 07-22, 08-19, 08-21, 08-22]

tech-stack:
  added: []
  patterns:
    - "MeteredSink: the activation driver wraps the ActivationSink and records step metrics only for committed appends (every trait method forwarded)"
    - "Shared per-label gauges across partition dispatchers move by per-owner deltas (like the lease gauge)"
    - "Partition/log/Raft gauges sampled by one service from watch/atomic accessors; no apply lock"

key-files:
  created:
    - docs/metrics.md
  modified:
    - crates/ostro-engine/src/activation.rs
    - crates/ostro-engine/src/runtime.rs
    - crates/ostro-engine/src/replay/mod.rs
    - crates/ostro-partition/src/leader/dispatcher.rs
    - crates/ostro-meta/src/handle.rs
    - crates/ostro-server/src/services/metrics_poll.rs
    - crates/ostro-server/src/services/run_spans.rs
    - crates/ostro-server/tests/metrics_endpoint.rs

key-decisions:
  - "DLQ counted in the post-apply RunCompleted consumer (services/run_spans.rs): no dlq rows are written yet, the executor's fail_run lives in executor.rs (another wave-4 plan's file), and apply/terminal.rs re-runs during catch-up; reason = stored failure reason_code from a bounded list, else other; workflow_failed and a non-resumable timeout are not DLQ"
  - "ostro_runs_started_total is recorded at the first successful grant in the dispatcher (Entry.first_activation), not in the engine: an engine-side first-activation test double counts a first activation that is fenced or evicted before journaling"
  - "run_spans now always runs (with partitions ready) for run metrics; workflow.run spans stay gated on OTLP traces"
  - "ostro_queue_worker_demand is describe_gauge! only (Phase 7 owns the value; the 07-16 awk setter check passes)"
  - "Step duration = journal time from the previous event to the step event (step begin is not journaled; upper bound, documented)"

requirements-completed: [OBS-02]

duration: ~2h
completed: 2026-11-20
---

# Phase 8 Plan 16: Metrics coverage (OBS-02) Summary

**Every OBS-02 family is now on `/metrics`. Queue ready/lag and run starts come from the dispatcher. Committed live steps, replay, fuel and instance memory come from the engine. Run completions and dead letters come from the post-apply `RunCompleted` consumer. Apply lag, HW lag, ISR, WAL log start/end and metadata Raft gauges come from a 5 s poller. All labels are bounded, and `docs/metrics.md` documents every `ostro_*` series.**

## Series added, per crate

| Crate / file | Series |
|---|---|
| ostro-partition `leader/dispatcher.rs` | `ostro_queue_ready{queue}` (G, per-dispatcher deltas; the share is removed on stop), `ostro_queue_lag_seconds{queue}` (H, ready_at → grant; skipped for ready_at 0 regrants), `ostro_runs_started_total{workflow}` (C, first grant); `ostro_queue_worker_demand` described only |
| ostro-engine `activation.rs` (`MeteredSink`) | `ostro_steps_total{workflow,outcome=ok\|error\|timeout\|cancelled\|retry}`, `ostro_step_duration_seconds{workflow}`, `ostro_fuel_consumed_total{workflow}` |
| ostro-engine `replay/mod.rs` + `activation.rs` | `ostro_replay_events_total`, `ostro_replay_duration_seconds` (start → first live command or call end; only when more than RunStarted was replayed) |
| ostro-engine `runtime.rs` | `ostro_instance_memory_bytes`, `ostro_wasm_memory_budget_bytes`, `ostro_parked_instances` (set at activation start and end; `Runtime::publish_gauges`) |
| ostro-server `services/run_spans.rs` | `ostro_runs_completed_total{workflow,status}`, `ostro_dlq_total{reason}` |
| ostro-server `services/metrics_poll.rs` | `ostro_partition_apply_lag{partition}`, `ostro_partition_hw_lag{partition,replica}` (RF=1: 0), `ostro_isr_size{partition}` (RF=1: 1), `ostro_wal_log_start{partition}`, `ostro_wal_log_end{partition}`, `ostro_raft_term`, `ostro_raft_commit_index`, `ostro_raft_applied_index`, `ostro_raft_is_leader` |
| ostro-meta `handle.rs` | `MetaHandle::raft_status()` reads the driver's published status from a watch value, never from the driver thread |

## DLQ counting site

This plan does **not** count in `apply/terminal.rs`. No DLQ rows are written anywhere: the `dlq` keyspace exists, but Phase 11 adds the rows. A run is dead-lettered when the executor's `fail_run` stores `CompleteRun{Failed}`, and that code is in `executor.rs`, which another wave-4 plan owns. Apply also re-runs during restart catch-up, so a counter in apply would count twice. The counter therefore sits in the post-apply `RunCompleted` consumer, which publishes only after catch-up. It reads the run's `PAYLOAD_RESULT` failure JSON and takes its `reason_code`. A run is counted when it is `Failed` with an empty `terminal_reason` and its code is in the 12 attempt-level and module-level codes, or `other`. `workflow_failed` is excluded, and so is a non-resumable `timeout` (a workflow's own `Failure::Timeout`). Unit tested in `run_spans::tests`.

## Task commits

1. **Task 1: source instrumentation + gauge poller**: `1e4732e`
2. **Task 2: /metrics coverage test + docs/metrics.md**: `0ebd883`

## Verification

- `cargo build --workspace --all-targets --locked`: 0 warnings. It was re-run on main at 1e4732e after main moved: 0 warnings.
- `cargo clippy --workspace --all-targets --locked -- -D warnings`: ok. ostro-server clippy was re-run after the rebase: ok.
- `cargo test -p ostro-engine -p ostro-partition -p ostro-meta --locked --no-fail-fast`: **642 passed, 0 failed, 74 targets**. This includes `handle::tests::raft_status_reports_the_lone_leader`.
  - The first, fail-fast run failed one test: `trim_safety::sc4_random_snapshot_trim_kill_sequences` panicked with "node open timed out: Elapsed(())" after 495 s. The machine was running ci-local and three other executors at the same time.
  - The rerun of `cargo test -p ostro-partition --test trim_safety` alone passed, **ok. 6 passed**, in 825.79 s.
  - The proptest regression file written by the failed run was deleted and not committed.
- `cargo test -p ostro-server --locked --no-fail-fast`: **266 passed, 0 failed, 3 ignored, 29 targets**.
- `cargo test -p ostro-server --test metrics_endpoint --locked`: **ok. 4 passed**, re-run on the moved main. The 4 tests are the 08-04 pair, `metrics_endpoint_covers_obs02_families_with_bounded_labels` and `sample_parser_handles_labels_and_escapes`.
- The 07-16 demand-setter awk check over `grep -rn --include=*.rs ostro_queue_worker_demand crates` finds no setter.
- `grep -q ostro_raft_term docs/metrics.md && grep -q ostro_queue_lag_seconds docs/metrics.md` passes. Every metric name in `crates` is in docs/metrics.md. The matches that are not series (crate paths, test prefixes, `_bucket`s, the SQL table) are listed at the end of the page.
- `bash scripts/ci-local.sh --no-musl` was **not run** by this executor, because a full ci-local run was already going in parallel. It is left to the phase gate (08-22).

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] `ostro_queue_ready` never appeared in the scrape**
- **Found during:** Task 2, first test run.
- **Issue:** A dispatcher with free slots grants each entry before the loop reports queue lengths, so the per-queue gauge was never registered.
- **Fix:** A queue's series is registered at 0 on its first grant (`track_queue`), then moved by deltas.
- **Files modified:** `crates/ostro-partition/src/leader/dispatcher.rs`.
- **Commit:** `1e4732e`.

**2. [Deviation] Run starts are counted in the dispatcher**
- The plan lists `ostro_runs_started_total` under "Engine". It is counted in the dispatcher, at the first successful grant, because that is exact. The engine cannot tell a repeated first activation apart. See key-decisions.

**3. [Deviation] `run_spans` always spawns**
- `run_spans` used to start only with OTLP traces on. It now starts whenever a partition is ready, so `ostro_runs_completed_total` and `ostro_dlq_total` exist without OTLP. Spans are still gated.

**4. [Deviation] DLQ is counted outside `apply/terminal.rs`**
- `terminal.rs` is unmodified. The phase rules allowed DLQ to be counted elsewhere, and the reasons are documented above and in docs/metrics.md.

**5. [Deviation] No DLQ row delta**
- The phase rules' fallback was to count DLQ in the poller from the `dlq` row delta. That would always read 0, because no rows exist yet. The post-apply consumer is used instead.

## Notes for later plans

- **07-05:** `dispatcher.rs` now holds this plan's metrics (`report_queues`, `track_queue`, `record_grant`, `move_gauge`, `ready_gauge`, `describe_queue_metrics`). The demand gauge is described only, so there is nothing to reduce.
- **11:** when DLQ rows land, `ostro_dlq_total` can move to the row write site. Keep the `reason` label set.
- **Trait forwarding:** a new `ActivationSink` trait method must be forwarded in `MeteredSink` (`activation.rs`). Otherwise the wrapper silently applies the trait default.

## Self-Check: PASSED

- FOUND: docs/metrics.md, crates/ostro-server/src/services/metrics_poll.rs, crates/ostro-server/tests/metrics_endpoint.rs
- FOUND: 1e4732e, 0ebd883 on main
