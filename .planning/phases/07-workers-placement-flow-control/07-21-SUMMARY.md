---
phase: 07-workers-placement-flow-control
plan: 21
subsystem: QA-03 fencing chaos (spawned server + workers, MockProvider)
tags: [chaos, fencing, qa-03, d-w11, failpoints, workers, effects, idempotency]
requires:
  - phase: 07-workers-placement-flow-control
    provides: "07-16 P7Cluster spawned harness; 07-07 worker:: failpoints + pay-flow guest (secret pay_token); 07-12 leader-side StartEffect verification"
  - phase: 06-effects-bridge-connectors-secrets
    provides: "ostro-testkit MockProvider /pay (charges/requests, 409 in progress); bridge re-drive after a leader kill"
  - phase: 05-workflow-semantics-core-hardening
    provides: "executor::/dispatcher:: failpoints, FAILPOINTS env in the failpoint ostro binary"
provides:
  - "crates/ostro/tests/fencing_chaos.rs: sharded, resumable QA-03 campaign (campaign / report, #[ignore]) + chaos_smoke (100 runs, every fault kind once) + log_parsers"
  - "scripts/fencing-chaos.sh: release+failpoints build, resumable campaign, report"
  - "10,000-run campaign report under target/fencing-chaos/"
affects: [07-22]
tech-stack:
  added: []
  patterns:
    - "Recording HTTP/1.1 proxy in front of the MockProvider: per-request key, arrival time, status, replay flag; injects provider latency via /pay?delay_ms"
    - "Leader incarnations from the fault log (kill time, ready time) drive fenced-send and duplicate attribution"
key-files:
  created:
    - crates/ostro/tests/fencing_chaos.rs
    - scripts/fencing-chaos.sh
  modified: []
key-decisions:
  - "Server failpoints can only be set at process start (FAILPOINTS env), so a server_failpoint fault is a kill -9 + restart with a counted burst `<site>=<skip>*off-><n>*return`; it counts as a leader kill for attribution"
  - "fenced_sends is defined per W5 with three authorisations (accepted StartEffect in the same incarnation, re-drive of a request from an earlier incarnation, bridge retry after 409/503); without them the literal definition would flag legitimate re-grants"
  - "A run whose result is not {status: 200} (e.g. 409) counts as lost: D-W11 requires every run to complete with its charge"
requirements-completed: []
metrics:
  duration: "~1h50m (campaign 710 s wall clock, 1,416 shard-seconds)"
  completed: 2026-11-21
---

# Phase 7 Plan 21: QA-03 fencing chaos campaign Summary

**The 10,000-run fencing chaos campaign ran to completion. It found 0 double charges, 0 fenced sends, 0 unattributed duplicates and 0 raw duplicates without a leader kill, and every key was charged exactly once. The verdict is still FAIL on one D-W11 invariant: 7 runs completed with result status 409 instead of their charge. Each of them is a leader kill while the original `/pay` was still in progress at the provider. After the restart the leader re-drove the request and got 409 in_progress. The bridge does not retry that status without a guest retry policy, so the run completed with the 409 response. This is a product gap for gap closure, not a harness bug; the assertion was not weakened.**

## Commits

1. `ae1e4be` test(07-21): QA-03 fencing chaos campaign

No harness fixes were needed after the campaign, so Task 2 has no commit.

## Campaign report (target/fencing-chaos/report.txt)

```text
QA-03 fencing chaos report (D-W11)
shards 20/20 seed 0x721c4a05eed  verdict FAIL
  runs                                   10000
  completed                              10000
  charges_eq_1                           10000
  double_charges                         0
  lost                                   7
  fenced_sends                           0
  start_effect_rejections                2
  raw_duplicates                         131
  raw_duplicates_without_leader_kill     0
  unattributed_duplicates                0
  provider_requests                      10131
  provider_409                           7
  faults by kind:
    leader_kill                          36
    server_failpoint                     45
    worker_failpoint                     37
    worker_kill9                         44
    worker_sigstop                       64
  duplicates by cause:
    leader_kill                          131
    retry_409                            0
    unattributed                         0
  run latency ms: median (of shard medians) 284, max 10276
  FAILED shard 2 seed "0x927f7b2e77c62e86"
  FAILED shard 3 seed "0x12c1298e8c1c4e92"
  FAILED shard 5 seed "0x794a9d6b01ff5965"
  FAILED shard 9 seed "0xc0592ec06ea18e1b"
  FAILED shard 11 seed "0x8d1ffc1cdfc2dba3"
  FAILED shard 12 seed "0xcbd68d88abc1b11a"
  FAILED shard 17 seed "0xe0ec51cf8b28a082"
```

- Command: `OSTRO_CHAOS_RUNS=10000 OSTRO_CHAOS_SHARDS=20 OSTRO_CHAOS_PARALLEL=2 bash scripts/fencing-chaos.sh`, with `CARGO_TARGET_DIR=target/wt-07-21` and `OSTRO_CHAOS_STATE_DIR=<repo>/target/fencing-chaos`. PARALLEL was lowered to 2 because the machine was loaded (load average 21-28 from other executors); the run count was not lowered.
- Duration: 710 s wall clock for the campaign. Each shard took 67-76 s, and the shards sum to 1,416 s. The first release build took 6 min 38 s.
- Totals: 226 faults, of which 81 were leader kills (36 `leader_kill` + 45 `server_failpoint`). Campaign seed `0x721c4a05eed`; shard seed = splitmix64(seed + shard).
- Every one of the 131 extra raw requests was answered by the provider's dedupe: 124 replays plus 7 conflicts (409). Every one fell inside a leader-kill window. No raw duplicate came from worker kills, SIGSTOP zombies, worker failpoints or executor/dispatcher failpoints alone.
- StartEffect rejections: 2 real lease rejections were observed (`PERMISSION_DENIED: the session holds no lease for this run and token`). There were 0 sends after either. Over the whole campaign, workers logged an accepted StartEffect about 380 times per shard.
- The plan's Task 2 verify fails on `"lost": 0` only. All other greps pass: `runs` 10000, `double_charges` 0, `unattributed_duplicates` 0, `fenced_sends` 0, `raw_duplicates_without_leader_kill` 0.

## GAP: re-driven POST answered 409 in_progress becomes the run's result (7 of 10,000 runs)

All 7 cases follow the same timeline. Times are from `shard-NN/requests.jsonl` and `faults.jsonl`.

| Shard | Seed | Run | First `/pay` (arrive -> 200) | Leader SIGKILL -> ready | Re-drive (409) |
|---|---|---|---|---|---|
| 02 | 0x927f7b2e77c62e86 | 000g38fy14cq0wy7pdzghmkxhhzbt | ..371653 -> ..372010 | ..371752 -> ..371959 | ..371961 |
| 03 | 0x12c1298e8c1c4e92 | 000g38fy034t4wfgjz08j741vwfcj | ..826934 -> ..827335 | ..826985 -> ..827274 | ..827256 |
| 05 | 0x794a9d6b01ff5965 | 001038fy06bkjwy1psyb0802xqn4r | ..879565 -> ..879851 | ..879596 -> ..879890 | ..879808 |
| 09 | 0xc0592ec06ea18e1b | 001g38fy0f3htwnjpf37ted5429z8 | ..022901 -> ..023166 | ..022913 -> ..023217 | ..023131 |
| 11 | 0x8d1ffc1cdfc2dba3 | 001g38fy0jmdgw8dpsgg82c5prbd2 | ..080686 -> ..081040 | ..080757 -> ..081084 | ..080988 |
| 12 | 0xcbd68d88abc1b11a | 001g38fy0mpk8xe9kav9vqee0dv56 | ..114571 -> ..114935 | ..114623 -> ..114959 | ..114868 |
| 17 | 0xe0ec51cf8b28a082 | 000038fy10pn6wz7jmmy03rs6vfqa | ..311210 -> ..311569 | ..311270 -> ..311531 | ..311549 |

- The old leader sent `POST /pay`, and the provider (a 50-400 ms latency injected by the proxy) was still processing it when the leader was SIGKILLed. The restarted leader re-drove the effect with the same `Idempotency-Key` about 0.3 s later. The provider answered `409 {"error":"in_progress"}`. The original request then finished and charged once, so `charges == 1`.
- `ostro_bridge::http::retry::classify_status` classifies that 409 as `Class::RetryStatus`, because `is_in_progress_conflict` matches. But `Schedule::should_retry` retries `RetryStatus` only when `from_policy` is set, and `pay-flow` sets no `http_retry` policy. The effect therefore completed with the 409 response. The run completed with `result.status = 409`: it never learns its charge.
- Fix options (outside 07-21's files):
  1. The bridge retries a 409 in_progress without a guest policy, at least for a re-driven effect.
  2. `pay-flow` sets an `http_retry` policy (this hides the default behaviour rather than fixing it).
  3. Accept it under D-W11 as "the workflow sees the provider's answer", but that contradicts "every run Completed with its charge".
- **For user review together with D-W11.**
- Evidence: `target/fencing-chaos/shard-{02,03,05,09,11,12,17}/` holds `server.log`, worker logs, `requests.jsonl`, `faults.jsonl` and `runs.jsonl`. The first, interrupted attempt hit the same pattern once (shard 00); it is kept in the scratchpad copy `07-21-run1/`.

## Design as built

- **Shard:** each shard runs one `P7Cluster`:
  - server settings: lease_ttl 3 s, heartbeat 1 s, hot_wait 5 s, 2 embedded slots, spill `slots_pct 50 / lag 500ms / sustain 1s / cooldown 2s`, `[log] format = "json"`, `RUST_LOG=info,ostro_bridge::http=debug`, `auto_sealing_key`;
  - three workers (`pool=chaos`, 4 slots, `RUST_LOG=info,ostro_worker::effect_proxy=debug`);
  - secret `pay_token`, a grant and the egress allow for the proxy origin;
  - queues: `chaos-w` (workers, 3 of 4 runs) and `chaos-a` (auto, 1 of 4 runs).
- **Runs:** submitted evenly over `OSTRO_CHAOS_SUBMIT_SECS` (60 s), each with a client idempotency key (`chaos-sNN-rNNNNN`), and retried across leader restarts. A run lost at start would show up as an empty id, and none did.
- **Faults:** one every 2-5 s while runs are pending; see the module docs for the five kinds. Worker failpoint actions are `3*return(error)` or `2*return(hang)`; for `hang` the worker is replaced after a 2-4 s window.
- **Recording proxy:** the MockProvider has counters but no timeline, so the proxy records it. Its per-key counts are cross-checked against `MockProvider::requests(key)` (a mismatch fails the shard), and `MockProvider::charges(key)` is the charge oracle.
- **Attribution:** fenced sends, duplicate attribution, `KILL_SLACK` 250 ms and the `retry_409` correlation (by op and time, because the bridge's `http effect attempt` line carries no run id) are documented in the file header.
- **Resumability:** `shard-NN.json` is written atomically, and existing shard files are skipped. `OSTRO_CHAOS_SHARD_IDS` selects shards; `report` aggregates `report.json` (pretty, `"key": value`) and `report.txt`.

## Verification

- `cargo test -p ostro --features failpoints --test fencing_chaos --locked -- --test-threads=1 chaos_smoke`: `test result: ok. 1 passed`, run three times.
  - Last run: 100 runs, every fault kind once, 76 StartEffects accepted, 1 raw duplicate attributed to leader_kill, pass, 41 s.
  - The unfiltered binary run: 2 passed (chaos_smoke, log_parsers), 2 ignored.
- `cargo clippy -p ostro --features failpoints --tests --locked -- -D warnings` and the same without the feature: clean. `cargo fmt --all --check`: clean. The build is warning-free.
- Task 2 verify: fails on `"lost": 0` (see GAP). The other five greps pass.

## Deviations from Plan

1. **[Plan adaptation] Server failpoint windows.** The ostro binary reads `FAILPOINTS` only at start, so a server failpoint is armed by a kill -9 + restart with a counted burst. These faults are leader kills too, and are attributed as such.
2. **[Plan adaptation] Mock request log.** `MockProvider` has no per-request timestamps, and ostro-testkit is outside files_modified. The recording proxy lives in the test file. It also injects provider latency (`/pay?delay_ms=50..400`) so that sends are genuinely in flight across leader kills.
3. **[Plan adaptation] fenced_sends authorisations.** These are the three exceptions listed under key-decisions. Without them the literal W5 definition would flag legitimate re-grant sends and re-drives.
4. **[Plan adaptation] Run outcomes.** "every run Completed with the charge recorded in its result" is checked as `result.status == 200`, `result.idempotency_key` present and charged, and exactly one `run_completed`. pay-flow's result carries the status and key, not the charge id.
5. **PARALLEL 2 instead of 4** (machine load); all 10,000 runs were executed.
6. **First campaign attempt interrupted.** It was stopped by its own test PID after 2 shards so the design could be re-checked; the code was unchanged. That attempt already showed the same 409 pattern. The state dir was moved to the scratchpad, and the reported campaign ran from an empty state dir.

## Self-Check: PASSED

- FOUND: crates/ostro/tests/fencing_chaos.rs, scripts/fencing-chaos.sh (in `ae1e4be`)
- FOUND: target/fencing-chaos/report.json, report.txt, shard-00..19.json, 7 evidence dirs
