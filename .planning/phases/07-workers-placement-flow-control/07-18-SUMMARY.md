---
phase: 07-workers-placement-flow-control
plan: 18
subsystem: spawned-binary e2e (auto spill, selectors, worker failover, zombie fencing)
tags: [e2e, spill, hysteresis, selectors, failover, fencing, sigstop, failpoints, work-01, work-03, work-04]
requires:
  - phase: 07-workers-placement-flow-control
    provides: "07-16 P7Cluster harness (crates/ostro/tests/common/p7.rs); 07-13 spill controller + ostro_placement_* metrics; 07-12 NodePlacement/RemoteRunner log lines; 07-07 worker failpoints + pay-flow/cpu-burn guests"
provides:
  - "crates/ostro/tests/worker_failover.rs: auto_spill, oscillation, sustained_overload, selectors, kill_redispatch, sigstop_zombie (failpoints)"
affects: [07-22]
tech-stack:
  added: []
  patterns:
    - "Placement evidence from the leader's `remote activate run=<id> attempt=<n> worker=<id>` lines (no line = embedded)"
    - "Transition counts from GET /metrics, cross-checked against `placement spill transition` log lines"
    - "Re-dispatch latency measured by 10 ms polling of the captured server log after the signal"
key-files:
  created:
    - crates/ostro/tests/worker_failover.rs
  modified: []
key-decisions:
  - "Metric source: GET /metrics (Phase 8's exporter is in the binary): ostro_placement_transitions_total{queue,to} and ostro_placement_spilling{queue}; the transition log lines agree in every run"
  - "Embedded occupancy is sampled as running runs (GET /v1/runs?status=running) minus running_on_workers (demand); cpu-burn's started_ms is logical journaled time, so it cannot time execution"
  - "oscillation uses the CPU signal (the only one with sustain); its threshold is calibrated from the server's measured CPU (/proc/<pid>/stat), and a continuous-load positive control proves the threshold is reachable"
  - "Zombie late messages are forced by failpoint waits (`return(wait:4000)`) that expire while the worker is stopped (the worker failpoints have no `pause` action); the worker stays stopped until fp_wait + 1 s"
requirements-completed: [WORK-03, WORK-04, WORK-01]
metrics:
  duration: "~95 min"
  completed: 2026-11-21
---

# Phase 7 Plan 18: SC2 e2e (spill, selectors, failover, zombie) Summary

**ROADMAP Phase 7 success criterion 2 is now shown end to end with real `ostro server` and `ostro worker` processes. An auto queue spills under a spike and comes back after the cool-down with exactly 2 transitions. An oscillating CPU load causes 0 transitions. A sustained overload causes 1 transition while the embedded slots stay full. Selectors route work only to matching workers. Killing a worker with kill -9 re-dispatches its runs in 75 ms at most (lease_ttl is 5 s). A SIGSTOPped zombie is revoked after lease_ttl; once continued, its forced late StartEffects are refused and change nothing.**

## Commits

1. `0726645` test(07-18): SC2 auto spill, oscillation and selector e2e
2. `4573b52` test(07-18): SC2 kill -9 and SIGSTOP zombie e2e

## Results (debug build, host load average 17-29 on 12 cores)

| Test | Setup | Result |
|---|---|---|
| `auto_spill` | embedded slots 2, worker 4 slots, queue `burst` auto, spill policy slots 50 % (cpu/mem/lag disabled), cooldown 5 s; 40 × `cpu-burn {ms:300}` | entered at +5.5 s; 18/40 runs on the worker; exit 5.3 s after the drain; **transitions spilling=1 embedded=1 (2)**, 2 log lines, gauge 0 |
| `oscillation` | embedded slots 4, CPU trigger only, cpu_pct calibrated to half the measured on-phase level (31.6 % → 15 %), sustain 5 s, on-phase 16 × `cpu-burn {ms:800}` (about 1 s), off-phase 2.5 s, 60 s | 17 on-phases (max 1.59 s), all 17 measured above the line (19.7-32.7 %); **0 transitions**; control: continuous load entered after 8.3 s (exactly 1) |
| `sustained_overload` | embedded 2 + worker 2 slots, always ≥ 8 runs in flight (`cpu-burn {ms:500}`), cooldown 5 s | Spilling held 15.3 s (3 × cooldown) with **1 transition**; 186 runs, 84 on the worker, 102 embedded; embedded running == 2 in 30/34 samples that saw worker work (max 2); after the drain exit 5.8 s later → **exactly 2** |
| `selectors` | workers `gpu-t` (gpu=true, 2 slots) and `gpu-f` (gpu=false, 4 slots), queue `gpu` workers + selector `gpu=true`; 10 `sdk-basic` runs | all 24 activations of the 10 runs on gpu-t; after gpu-t drains, 3 new runs stay unstarted (history = acceptance `run_started` only), demand 3 / backlog 3 / workers 0, no activation anywhere; a new gpu=true worker (`gpu-t2`) completes them (6 activations) |
| `kill_redispatch` | lease_ttl 5 s, hot_wait 30 s; kill-a (10 slots) joins alone and takes 10 of 20 `timeouts sleep_ms 8000` runs; kill-b (30 slots) joins for the rest | kill-a held 10 leases; **10 affected runs re-dispatched to kill-b as attempt 2: max 75 ms, median 24 ms** (runs 11.6-14.1 s: max 75.1 ms / median 23.8 ms); 20/20 completed, one `run_started`, one terminal event each |
| `sigstop_zombie` (`--features failpoints`) | lease_ttl 3 s, `[workers] keepalive` 120 s (stream stays up), z-a (4 slots) with `FAILPOINTS=worker::before_append=1*return(wait:4000);worker::before_start_effect=return(wait:4000)`, 4 `pay-flow` runs, z-b (8 slots) clean | details below |

### Zombie evidence (sigstop_zombie)

- Before SIGSTOP: 4 activations on z-a. 3 runs had their EffectScheduled append applied and were paused before StartEffect. 1 run was paused before its first append. The mock had seen no request beyond the baseline (precondition asserted).
- Re-grant: all 4 runs were re-granted as attempt 2 on z-b 2.86-2.95 s after SIGSTOP (lease expiry counted from the last heartbeat before the stop) and completed at about 3.0 s. Each idempotency key has exactly 1 charge.
- The worker stays stopped until fp_wait + 1 s (5.0 s), so the failpoint waits expire while it is stopped. After SIGCONT:
  - **Late StartEffect: 3/3 forced and refused at the session/auth layer.** The worker logs `StartEffect failed ... code=PermissionDenied msg=the session holds no lease for this run and token`. The test asserts at least 1. The server logged no `StartEffect rejected` line, because the request never reached journal verification.
  - **Late append:** the server logged no `applied events run=<id> attempt=1` line after the revoke, for any run. For the run paused before its append, no attempt-1 append was ever applied (asserted). This run produced no log line on either side, so it is not known which layer stopped it. Possible layers: the worker's post-failpoint cancel check, a session PermissionDenied mapped to Fenced, or apply Fenced. W4 accepts any of them.
  - `MockProvider::requests(key)` was unchanged for every key, and charges stayed at 1.
  - The zombie never logged `effect started on the leader`.
- Journal equality: every disturbed run's history event kinds, excluding per-attempt bookkeeping (`run_started` / `attempt_failed` / `attempt_released`), equal the undisturbed baseline `pay-flow` run.

## Verification

- Task 1 verify: `cargo test -p ostro --test worker_failover --locked -- --test-threads=1 auto_spill oscillation sustained_overload selectors` gave `test result: ok. 4 passed` (124.6 s). Before that, each test passed individually.
- Task 2 verify: `kill_redispatch` gave `ok. 1 passed`. `cargo test -p ostro --features failpoints ... sigstop_zombie` gave `ok. 1 passed`. It passed 5 times in total: 2 with the final assertions, and in 3 of the runs that kept the worker stopped past fp_wait, the 3 late StartEffects were forced and refused.
- `cargo clippy -p ostro --tests --locked -- -D warnings` is clean, with and without `--features failpoints`. `rustfmt --check` is clean and the build is warning-free.
- No stopped child process of this plan remains (`ps` state T shows only a 07-21 worker).
- Not run: `bash scripts/ci-local.sh`. Several Phase 7 executors and the Phase 8 campaign were running; 07-22 runs the gate.

## Deviations from Plan

1. **[Plan adaptation] Failpoint `pause` mode does not exist.** The 07-07 worker failpoints support `return(error|hang|wait:<ms>)`, and FAILPOINTS cannot be changed in a running process. The test uses `wait:4000` and keeps the worker stopped until the waits have expired, so the late messages are due the moment it is continued. They race the queued Cancel. In every run that kept the worker stopped past the waits (3 of 3), the 3 late StartEffects won the race and reached the leader.
2. **[Plan adaptation] Embedded-first evidence.** No embedded-running gauge exists, and `cpu-burn`'s `started_ms` is logical (journaled) time that includes queue wait. The first attempt measured 7 overlapping "embedded" intervals for 2 slots. The test therefore samples `running` runs minus `running_on_workers` while Spilling. The requirement is ≤ 2 always and == 2 in ≥ 80 % of the samples that saw worker work. The two reads are not atomic, which explains the misses (30/34 observed).
3. **[Plan adaptation] Oscillation via the CPU signal.** Only CPU has a sustain timer; the slots, memory and lag signals enter immediately. The threshold is calibrated per host from `/proc/<pid>/stat`, so the test does not depend on a fixed CPU level. A positive control proves the threshold is reachable.
4. **[Plan adaptation] `run_started` is the acceptance event (seq 0)**, journaled once per run, not per attempt. "One RunStarted" is asserted that way, and "nothing ran" for held selector runs means a history of just that event.
5. **Real time, not ManualClock.** The spawned server reads its own clock (F5's ManualClock seam is covered by `ostro-server/tests/placement_spill.rs`). The test only measures time; it never feeds time into server logic.
6. `kill_redispatch`: worker A joins alone first, because `pick` prefers the worker with the most free slots, so with both connected every run went to B. B joins once A holds 10 runs.

## Product observations (no code changed)

- A late StartEffect from a revoked zombie is refused before journal verification (`PermissionDenied: the session holds no lease for this run and token`). The server logs nothing for it at debug, so operators see no trace of zombie traffic on the leader. This is a possible observability follow-up.
- The worker's post-failpoint `cancel.is_cancelled()` check races the server's Cancel after SIGCONT. This is correct (any outcome is safe) but means a late append cannot be forced deterministically from outside the worker.

## Self-Check: PASSED

- FOUND: crates/ostro/tests/worker_failover.rs (1,202 lines) on main
- FOUND: commits 0726645, 4573b52 on main
