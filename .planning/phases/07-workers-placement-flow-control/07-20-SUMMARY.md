---
phase: 07-workers-placement-flow-control
plan: 20
subsystem: e2e (KEDA simulator, spawned server + workers)
tags: [keda, demand, scaling, workers, e2e, work-05, sc5]
requires:
  - phase: 07-workers-placement-flow-control
    provides: "07-16 GET /v1/queues/{q}/demand (D-W12), deploy/keda/scaledobject-metrics-api.yaml, P7Cluster spawned harness; 07-12 placement workers + selector routing; 07-07 worker drain on SIGTERM"
provides:
  - "crates/ostro/tests/keda_sim.rs: Scaler (KEDA metrics-api math) + zero_to_n_to_zero (SC5 local proof)"
affects: [07-22]
tech-stack:
  added: []
  patterns:
    - "Pure scaler decision (demand, current, now) -> desired, unit-tested apart from the process control loop"
    - "Scale target modelled as live + draining worker processes; drained processes must exit 0 and are reaped by the P7Proc drop guard"
key-files:
  created:
    - crates/ostro/tests/keda_sim.rs
  modified: []
key-decisions:
  - "N -> M (M > 0) follows desired at once (HPA stabilisation out of scope): the harsher case for exactly-once, because each scale-down drains a worker holding leased runs"
  - "Inactive with current > min: hold until cooldown since the last active poll, then min; never active since start: min at once (KEDA behaviour)"
  - "Long tail = 3 timeouts sleep_ms 6 s runs parked hot (hot_wait 15 s) so N -> M drains evict leased runs deterministically in time; a long cpu-burn is not usable (wall time unpredictable, ms 4000 exhausts fuel)"
requirements-completed: [WORK-05]
metrics:
  duration: "~15 min"
  completed: 2026-11-21
---

# Phase 7 Plan 20: KEDA simulator drives real workers 0 → N → 0 Summary

**A simulator applies KEDA's metrics-api math to `GET /v1/queues/heavy/demand` every 500 ms and drives real `ostro worker` processes: a 33-run burst takes the pool 0 → 4 in under a second. Scale-downs to 3 and then 2 drain workers that still hold leased runs, and the pool returns to 0 exactly one cool-down (5 s) after the last run. Every run completed exactly once.**

## Commits

1. `bb50464` test(07-20): KEDA metrics-api scaler simulator
2. `2f6ef07` test(07-20): SC5 KEDA simulator drives workers 0→N→0
3. `874a765` test(07-20): simulator polls the metrics-api URL path directly

## Scaler (KEDA metrics-api semantics)

`Scaler { target, activation_target, min, max, cooldown }`, `desired(demand, current, now)`:

- **Active** when `demand > activation_target`, strictly. The result is `clamp(ceil(demand / target), max(min, 1), max)`.
- **Inactive with `current > min`**: stays at `current` until `cooldown` has passed since the last active poll, then goes to `min`. If the scaler was never active, it goes to `min` at once.
- **N → M (M > 0)** follows `desired` at once. HPA stabilisation is out of scope (see the module docs).

Unit tests (6, filter `scaler`):
- strict activation (0 with activation 0 stays 0; equal to activation is inactive);
- ceil (9 / 8 → 2);
- clamp to max (including `f64::MAX`) and to min while active;
- N → 0 only after the cool-down, with a re-activation restarting the window;
- immediate scale-up from 0;
- immediate N → M scale-down.

## zero_to_n_to_zero

- **Setup:** P7Cluster with no workers and `hot_wait` 15 s. Queue `heavy` uses `placement = workers` with selector `pool=heavy`.
- **Scaler:** `{target 2, activation 0, min 0, max 4, cooldown 5 s}`. Each replica is `ostro worker --slots 2 --labels pool=heavy`.
- **Control loop:** every 500 ms it reads demand, decides, spawns workers up to `desired` or SIGTERMs the newest down to `desired`, and reaps drained workers. A drained worker must exit 0.
- **Burst:** 30 `cpu-burn {ms: 500}` runs plus 3 `timeouts {mode: sleep_ms, n: 6000}` runs, submitted concurrently.
- **Assertions:**
  - ≥ 2 workers within 5 s of the burst;
  - `running_on_workers > 0` ⇒ `demand > 0` on every poll;
  - the pool never reaches 0 while runs are open;
  - after the last run, the pool reaches 0 within cool-down + 15 s, not before cool-down − one poll, and never scales back up;
  - ≥ 6 polls over ≥ 3 s with current, demand and draining all 0;
  - `/v1/workers` is empty afterwards;
  - per run: status completed, exactly one `run_started`, exactly one terminal event, `burn` completed once (cpu-burn) or result `"slept"` (timeouts), and at least one `remote activate` on a worker;
  - ≥ 2 SIGTERM drains.
- **Run time:** 22–27 s in total, debug build, load average about 20.

### Timeline (representative run)

| t (s) | demand | on workers | backlog | desired | current | draining | open runs |
|---|---|---|---|---|---|---|---|
| 0.9 | 33 | 0 | 33 | 4 | 4 | 0 | 33 |
| 1.1 | 33 | 8 | 25 | 4 | 4 | 0 | 32 |
| 1.5 | 26 | 8 | 18 | 4 | 4 | 0 | 25 |
| 2.0 | 17 | 8 | 9 | 4 | 4 | 0 | 17 |
| 2.5 | 5 | 5 | 0 | 3 | 3 | 1 | 5 |
| 2.9 | 3 | 3 | 0 | 2 | 2 | 1 | 3 |
| 3.4 – 5.9 | 3 | 3 | 0 | 2 | 2 | 0 | 3 |
| 6.4 – 10.9 | 0 | 0 | 0 | 2 | 2 | 0 | 0 |
| 11.4 | 0 | 0 | 0 | 0 | 0 | 2 | 0 |
| 11.9 – 15.4 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |

### Measured (4 green runs)

| Run | Peak N | ≥ 2 workers after burst | Last run done | Pool at 0 | Zero after last run | SIGTERMs | Runs moved by a drain |
|---|---|---|---|---|---|---|---|
| 1 | 4 | 0.90 s | 6.47 s | 11.90 s | 5.44 s | 4 | 2 (2 extra activations) |
| 2 | 4 | 0.90 s | 6.41 s | 11.91 s | 5.49 s | 4 | 2 |
| 3 | 4 | 0.69 s | 6.20 s | 11.70 s | 5.49 s | 4 | 1 |
| 4 | 4 | 0.61 s | 6.12 s | 11.12 s | 5.00 s | 4 | 2 |

- The pool reached 0 one cool-down (plus at most one poll) after the last run every time.
- Demand stayed at 3 while the parked sleeps were leased on workers (Pitfall 4 / D-W12): the pool held at 2 and never went to 0 under load.
- The 4 → 3 → 2 drains evicted leased runs. They were re-granted on a surviving worker and completed exactly once.

## Verification

- Task 1: `cargo test -p ostro --test keda_sim --locked -- scaler`: `test result: ok. 6 passed`.
- Task 2: `cargo test -p ostro --test keda_sim --locked -- --test-threads=1 zero_to_n_to_zero`: `test result: ok. 1 passed` (plan verify command, exit 0).
- Whole file `-- --test-threads=1`: `7 passed`, three times (the last one after the direct-URL change).
- No `ostro` process from `target/wt-07-20` was left running after the runs.
- `cargo clippy -p ostro --test keda_sim --locked -- -D warnings` is clean, `cargo fmt -p ostro --check` is clean, and the build is warning-free.
- Not run: `bash scripts/ci-local.sh` (shared-load policy; 07-17/18/19/21 and the Phase 8 campaign are running). 07-22 runs the gate.

## Deviations from Plan

1. **[Plan adaptation] Long-tail runs added to the burst.** With only the 30 `cpu-burn {ms: 500}` runs, the whole burst drained in about 1.5 s. Demand went from 7 straight to 0 between two polls, so the only drains happened at the final N → 0, when no run was in flight. That left "exactly once despite scale-down drains" unproven. Three `timeouts sleep_ms 6 s` runs (parked hot, `hot_wait` 15 s) now keep demand at 3 after the short runs finish. The scaler then goes 4 → 3 → 2 while those runs are leased on workers, and the drains evict and re-grant them (1–2 runs moved per test run). A long `cpu-burn` was tried first and rejected: `ms: 1500` finished in under a second, and `ms: 4000` failed with `fuel_exhausted` after 3 attempts.
2. **Burst size** is 33 runs (30 + 3), so the first poll reads demand 33. The plan's 30 short runs are unchanged.

## human_needed: real KEDA run

The local simulator stands in for KEDA. A real ScaledObject run needs a cluster the user approves:

1. **Never use the microk8s cluster reachable via kubectl on this machine** without explicit user approval (D-W22). Use a throwaway kind cluster, the optional `workflow_dispatch` kind + KEDA CI job (07-22), or a cluster the user names.
2. Install KEDA (e.g. `helm install keda kedacore/keda -n keda --create-namespace`).
3. Apply `deploy/k8s/server-statefulset.yaml`.
4. Create a join token (`POST /v1/workers/tokens`). Put it in the worker token Secret and the `--ca-hash` in the ConfigMap, then apply `deploy/k8s/worker-deployment.yaml` (replicas 0).
5. Fill `deploy/keda/trigger-auth.yaml` with the cluster CA, then apply it and `deploy/keda/scaledobject-metrics-api.yaml`.
6. Set the queue flow: `placement=workers`, `selector=pool=heavy`.
7. Submit a burst (e.g. 100 `cpu-burn` runs on `heavy`).
8. Watch `kubectl -n ostro get deploy ostro-worker-heavy -w`. Expect 0 → `ceil(demand / 8)` replicas (max 50) within one `pollingInterval` (15 s), then back to 0 about `cooldownPeriod` (120 s) after demand reaches 0.
9. Check that every run is completed with one `run_started` and one terminal event.

## Self-Check: PASSED

- FOUND: crates/ostro/tests/keda_sim.rs (559 lines ≥ 150; contains `/demand`)
- FOUND: commits bb50464, 2f6ef07, 874a765 on main
