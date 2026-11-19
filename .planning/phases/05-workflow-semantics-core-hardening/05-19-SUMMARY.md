---
phase: 05-workflow-semantics-core-hardening
plan: 19
subsystem: testing
tags: [qa-01, crash-campaign, kill-9, failpoints, enospc, eio, checker, negative-controls, ci, sharding]

requires:
  - phase: 05-18
    provides: "failpoint ostro binary (target/failpoints, FAILPOINTS env, build-info failpoints=on) + meta::/aot::/executor::/dispatcher:: sites"
  - phase: 05-25
    provides: "D24: exactly one RunStarted per run at seq 0; API runs journal pre-start signals/cancels directly"
  - phase: 05-10
    provides: "crash-saga guest + documented output function"
  - phase: 02-15
    provides: "ostro-crashtest helpers (rebuild_and_compare, Rng)"
provides:
  - "ostro_crashtest::server_target: build/locate failpoint binary, spawn `ostro dev` with FAILPOINTS, ready-line address, kill9 confirmed by waitpid SIGKILL, stale-orphan kill, crash-saga deploy"
  - "ostro_crashtest::workload: seeded crash-saga load (idempotency duplicates, request_id-embedded signals, B1 pre-start signals, short-timeout no-signal runs, cancel 5%, terminate 2%, 300 KiB blob inputs) + fsync-per-record intent/ack log, resumable"
  - "ostro_crashtest::check: (a) existence/key conflicts, (b) final status/result vs crash_saga_expected, (c) journal invariants (seq, D24, terminal, steps, attempts, op completions, request_id signals)"
  - "ostro_crashtest::campaign: sharded resumable runner (OSTRO_CAMPAIGN_*), identity guard, report (kills= lost_acked_runs= duplicate_journal_entries= non_terminal_runs=)"
  - "tests/check_controls.rs (default suite, 15 tests) + tests/server_campaign.rs (#[ignore]: campaign, report, expected_matches_guest)"
  - "ci-local step `server campaign smoke (20 kills)` + identical ci.yml step, both wiping target/campaign-smoke"
affects: [05-20]

tech-stack:
  added: []
  patterns:
    - "Every counted fault = one iteration ending in kill -9; kills counted only on waitpid SIGKILL"
    - "Clean (unarmed) restart after every kill for checks; armed servers only for the fault window"
    - "Workload plan carried inside the run input (input.plan) so the ack log alone resumes the load"
    - "Signals the result depends on are read from the journal; every acked signal must be journaled"

key-files:
  created:
    - crates/ostro-crashtest/src/server_target.rs
    - crates/ostro-crashtest/src/workload.rs
    - crates/ostro-crashtest/src/check.rs
    - crates/ostro-crashtest/src/campaign.rs
    - crates/ostro-crashtest/tests/check_controls.rs
    - crates/ostro-crashtest/tests/server_campaign.rs
    - .planning/phases/05-workflow-semantics-core-hardening/deferred-items.md
  modified:
    - crates/ostro-crashtest/Cargo.toml
    - crates/ostro-crashtest/src/lib.rs
    - scripts/ci-local.sh
    - .github/workflows/ci.yml
    - Cargo.lock

key-decisions:
  - "Checks run against a clean restart after every kill (not the armed server): an armed server may legitimately have failed partitions, so its reads are not evidence. A plain next iteration reuses the clean server; a faulted one kills it (uncounted, extra_kills) and restarts armed"
  - "The expected crash-saga result uses the journaled signals: a signal whose response was lost to a kill can still be accepted (found by the dev run). Acked signals must be journaled exactly once (LostAckedSignal), and journaled payloads must match what was sent"
  - "Failed is a valid end state only for crash-saga's documented retry exhaustion: 9 journaled attempt failures plus a final transient step_completed error. Each activation restarts the guest's in-memory failure counter, so kills and executor evictions in the middle of a step can use up the 10 attempts"
  - "ENOSPC menu = wal::write, meta::log_append, meta::fsync. The blob sites ignore the arg (05-18), so they stay EIO in the failpoint menu at 5%. The Phase 2 sites were not extended"
  - "failpoint_hits is informational and counted from server log lines; executor engine-append faults log no WARN, so that count is low (deferred-items.md)"
  - "Relative OSTRO_CAMPAIGN_STATE_DIR resolves against the repo root (cargo test runs in the crate dir), so `rm -rf target/campaign-smoke` from the root wipes the dir the test uses"

requirements-completed: []

duration: ~2h10m
completed: 2026-11-19
---

# Phase 5 Plan 19: Whole-server kill campaign Summary

**QA-01 harness: a sharded, resumable campaign drives the real failpoint `ostro` binary with a crash-saga HTTP workload. Every counted fault ends in a kill -9 confirmed by waitpid. A checker, proven by negative controls, then asserts no lost acked run, no duplicate journal entry and the exact expected result. A 20-kill smoke runs in ci-local and CI. Measured: 3,200 kills across the two timing runs, with 0 violations. The 10,000-kill gate is projected at about 1 h 55 min.**

## Performance

- **Duration:** about 2 h 10 min (16:55 to about 19:05 CET), including the two timing runs (2 min and 28 min) and a full ci-local run.
- **Tasks:** 2 of 2.
- **Files:** 7 created (6 code/test, plus deferred-items.md); 5 modified.

## Accomplishments

### Server target (`server_target.rs`)
- `failpoint_bin()` builds `cargo build --release -p ostro --features failpoints --target-dir target/failpoints --locked` once per process, or uses `OSTRO_FAILPOINT_BIN`. It asserts that `build-info` contains `failpoints=on`.
- `ServerTarget::spawn` runs `ostro dev --data-dir <shard>/data --listen 127.0.0.1:0`:
  - with `FAILPOINTS` set or removed, and with inherited `OSTRO_*` and `RUST_LOG` stripped;
  - it parses the bound address from the ready line;
  - clean servers must also reach `/readyz` 200.
- stdout and stderr go into a ring buffer, and EIO/ENOSPC/injected log lines are counted.
- `kill9()` returns `Killed` only when waitpid reports signal 9.
- `kill_stale(data_dir)` kills orphans left by a killed harness.

### Workload (`workload.rs`)
- Each run is planned from `seed ^ blake3(key)`, and the plan rides in `input.plan`. The guest ignores unknown fields.
- Inputs:
  - 3..8 steps, with fail_pattern characters `0`..`3`; 8% of runs have one `x` (compensation);
  - sleep of 10..400 ms, or 2.1..3 s for 20% of runs (beyond the hot-wait threshold);
  - a 300 KiB pad on 2% of runs, which sends RunStarted through the blob store.
- Requests:
  - starts carry `idempotency_key = id`; 10% get a deliberate duplicate start;
  - `go` and `extra` signals carry `request_id = <key>-<name>`, embedded in the payload (W9);
  - 10% of runs get `go` immediately after the start ack (the D24 pre-start shape);
  - 10% get no signals and a 100..600 ms wait timeout (deterministic `null`);
  - 5% are cancelled and 2% terminated at a random time.
- The intent/ack log (`acks.jsonl`) writes an intent before the first send, then an ack after a 2xx or a rejection after a 409. Every record is `sync_data`'d.
- Retries keep the same key or request_id across kills. On resume, every pending follow-up is re-derived from the log.

### Checker (`check.rs`)
- (a) Every acked run exists:
  - the run list is checked, and a run missing from the list is confirmed with GET before it counts as lost;
  - one key maps to one run id.
- (c) Journal invariants:
  - seqs are contiguous from 0;
  - exactly one `run_started`, at seq 0 (D24);
  - at most one terminal event, and it is last apart from `status_published`/`patch_marker`;
  - at most one `step_completed` per step;
  - attempts strictly increase per step;
  - each op is completed at most once (`timer_fired`, `wait_timed_out`, `signal_matched`, `signal_received`, effect/child/token completions; `op_cancelled` once per op);
  - each embedded `request_id` is journaled at most once, and every acked one exactly once.
- (b) Final status:
  - terminate acked: `terminated` is required;
  - cancel sent: `cancelled`, or `completed` with the exact result;
  - otherwise: `completed` with `result == crash_saga_expected(input, journaled go, journaled extra)`, a direct port of the documented guest function;
  - `failed` is accepted only for journaled retry exhaustion.
- `crash_saga_expected` is cross-checked against the real guest by `server_campaign::expected_matches_guest`, which passed. It covers four inputs: completion with signals, timeouts, compensation, and a long sleep with retries.

### Negative controls (`tests/check_controls.rs`, default suite, 15 tests)
Each failure class has a control that expects exactly its own violation kind:

| Control | Expected violation |
|---|---|
| lost ack | `LostAckedRun` (and the tally's `lost_acked_runs` is 1) |
| duplicate StepCompleted | `DuplicateStepCompleted` |
| two completions of one op (a wait both matched and timed out; a timer fired twice) | `DuplicateOpCompletion` |
| duplicate request_id signal | `DuplicateSignal` |
| wrong result (a wrong step result, or a missing go) | `WrongResult` |
| acked signal missing from the journal | `LostAckedSignal` |
| one key mapped to two runs | `KeyConflict` |
| second RunStarted | `DuplicateRunStarted` |
| seq gap | `SeqGap` |
| two terminal events | `DuplicateTerminal` |
| an input after the terminal event | `TerminalNotLast` |
| attempts not increasing | `AttemptsNotIncreasing` |
| journaled payload that was never sent | `WrongResult` |
| status rules | `WrongStatus` / `NonTerminal` |
| `failed` without retry exhaustion | `WrongStatus` |

Two cases must pass, not fail: a clean history, and the lost-response signal regression.

### Campaign (`campaign.rs`, `tests/server_campaign.rs`)
- **Environment:**
  - `OSTRO_CAMPAIGN_FAULTS`: default 20.
  - `OSTRO_CAMPAIGN_SHARDS`: default 1.
  - `OSTRO_CAMPAIGN_SHARD_IDS`: a subset such as `0-4` or `0,3,7-9`.
  - `OSTRO_CAMPAIGN_PARALLEL`: default min(6, cores/2).
  - `OSTRO_CAMPAIGN_STATE_DIR`: default `target/campaign`; a relative path is resolved against the repo root.
  - `OSTRO_CAMPAIGN_SEED`: default 0x0519.
  - `OSTRO_CAMPAIGN_RATE`: default 8 runs/s per shard.
  - Quotas are `FAULTS / SHARDS`, with the remainder spread over the first shards.
- **Iteration:** each iteration picks a variant from its own seed:
  - plain (60%): kill -9 at 20..3000 ms after ready, under load;
  - failpoint (30%): one site from a 17-entry menu (05-18 sites, Phase 2 storage sites, three `sleep(150)` kill windows). The kill comes 0..300 ms after the first injected-fault log line, or at 300..3000 ms;
  - enospc (10%): `return(enospc)` at `wal::write`, `meta::log_append` or `meta::fsync`.
- **After every kill:** a clean restart runs check (a), plus check (c) on every run whose journal has not been verified terminal, all under load. Every 25 kills, `rebuild_and_compare` runs on the dead data dir.
- **Shard end:** the generator stops, every intent is driven to an ack or a 409, and the runner waits until every run is terminal. Then it runs (b) and (c) over every acked run and a final rebuild.
- **Persistence:** `shard-NN.json` is rewritten atomically every iteration and records `{shards, faults, seed}`. The runner and the report refuse a mismatching file with "wipe the state dir"; a unit test covers this, and so did a live run with FAULTS=40 on the smoke dir.
- **Resume:** a SIGKILLed harness was resumed. It killed the orphaned servers, resumed both shards from 22 and 23 kills, and finished.
- **Report:** prints `kills= lost_acked_runs= duplicate_journal_entries= non_terminal_runs= lost_acked_signals= wrong_results= other_violations=`, a per-shard table and per-site counts. It exits non-zero on any violation or failure, on an incomplete or missing shard, or when kills < FAULTS.

## Results

### Smoke (plan verify command, wiped target/campaign-smoke)
`kills=20 lost_acked_runs=0 duplicate_journal_entries=0 non_terminal_runs=0 lost_acked_signals=0 wrong_results=0 other_violations=0`
- Breakdown: plain 12, failpoint 6, enospc 2; 230 acked runs (222 completed, 5 cancelled, 3 terminated).
- The campaign test took 47 s, at 1.8 s per kill. RESULT: PASS. The plan's grep checks passed.

### Timing run (plan shape: 200 kills = 4 shards x 50, parallel 4, wiped target/campaign-timing)
`kills=200 lost_acked_runs=0 duplicate_journal_entries=0 non_terminal_runs=0 ...` PASS.
- Breakdown: plain 123, failpoint 56, enospc 21; failpoint_hits 79, extra_kills 81, rebuilds 12; 2,334 acked runs.
- **Wall time 2 min 15 s.** Per shard: 2.01 to 2.52 s per kill, about 1,430 to 1,790 kills/hour per shard; about 5,300 kills/hour in aggregate.

### Gate-shaped batch (extra: 3,000 kills = 6 shards x 500, parallel 6, target/campaign-timing500)
`kills=3000 lost_acked_runs=0 duplicate_journal_entries=0 non_terminal_runs=0 lost_acked_signals=0 wrong_results=0 other_violations=0` PASS.
- Breakdown: plain 1,815, failpoint 898, enospc 287; failpoint_hits 1,430, extra_kills 1,191, rebuilds 126; 36,408 acked runs.
  - Run outcomes: completed 34,696, cancelled 1,226, terminated 476, failed 10. All 10 failed runs are journaled retry exhaustions under the strict rule; one was inspected by hand.
  - Each of the 20 menu entries (17 failpoint + 3 ENOSPC) was hit 38 to 110 times. No self-exits and no variant fallbacks; spawn retries were 1 to 5 per shard.
- **Wall time 28 min 16 s**, while the machine's load average was about 25 from other work.
  - Per shard: 3.07 to 3.34 s per kill averaged over the shard, so **about 1,080 to 1,170 kills/hour per shard**; about 6,370 kills/hour in aggregate.
  - Per-kill time grows from about 2 s to about 3.5 s as a shard reaches about 6,000 runs, because every post-kill check lists all runs.
  - The final settle and check took 22 to 64 s per shard.
  - Peak RSS for the harness plus 6 servers was about 4 GB, with 375 MB of data per shard.
  - Clean restarts took up to 2.8 s.

### Projection for the 05-20 gate (10,000 kills = 20 x 500, parallel 6)
- Plan 4 batches with `OSTRO_CAMPAIGN_SHARD_IDS` = `0-5`, `6-11`, `12-17`, `18-19`, at about 28 min each. That is **about 1 h 55 min wall** in total (about 1 h 35 min of pure kill time).
- Each batch fits a 30 min window. A batch that is cut off resumes from its shard files.

## Invariant violations
**None in the product.** Across 3,200 measured kills, plus about 150 development kills, there were no lost acked runs, no lost acked signals, no duplicate journal entries, no D24 violations, no wrong results and no rebuild mismatches. No read-side anomaly was seen either: the list never omitted an acked run, which the checker confirms with GET, after d2b7e57.

## Task Commits

All are on `main`, committed through a private index (`commit-tree` + CAS `update-ref`).

1. **Task 1: server target + workload + checker + negative controls:** `8527bdc` (feat).
2. **Task 2: sharded campaign + report + ci-local/ci.yml smoke + timing:** `16f8e66` (feat).
3. **Plan metadata (this SUMMARY, deferred-items.md, ROADMAP):** the final docs commit.

## Deviations from Plan

### Auto-fixed issues

**1. [Rule 1 - Bug, harness] Armed servers were required to reach `/readyz`**
- With `state::commit=1%return`, a partition can fail during recovery, which keeps `/readyz` at 503, so a dev shard stalled in spawn retries.
- Armed servers now need only their ready line. Clean servers must still reach `/readyz`; a clean restart that cannot is a failure.

**2. [Rule 1 - Bug, checker] A lost-response signal was flagged as WrongResult (dev run, shard 0)**
- What happened: the `go` request was accepted, its response was lost to a kill, the run then completed with `go`, and the retry got 409 `run_terminal`.
- The expected result now uses the journaled signals.
- Acked signals must still be journaled exactly once, and journaled payloads must equal what was sent. There is a control for each of these, plus a regression test.

**3. [Rule 2 - Correctness, harness] Orphaned servers on resume**
- A stopped harness left a server running on a shard dir, and the resumed harness started a second server next to it. The server takes no data-dir lock (deferred-items.md).
- Resume now kills every process whose cmdline holds `--data-dir <shard dir>`. Dev data from that run was discarded.

**4. [Rule 2 - Correctness, checker] Tighter rule for `failed`**
- The first rule accepted any "transient" failure. It now requires journaled retry exhaustion: 9 attempt failures plus the final transient error. There is a control for it.
- The 200-kill timing run used the looser rule; its only failed run satisfies the strict one, checked by hand. The 3,000-kill run used the strict rule.

**5. ENOSPC on blob sites not added**
- The plan's ENOSPC variant lists WAL, blob or meta write sites. The blob sites ignore `return(enospc)` (05-18 hand-off), so ENOSPC covers WAL and meta write sites, and the blob sites run with EIO at 5% in the failpoint menu. The Phase 2 failpoint files were not touched.

**6. Extra measurement**
- A gate-shaped 3,000-kill batch, beyond the planned 200-kill timing run, so the 05-20 projection rests on full-length shards rather than an extrapolation from 50-kill shards.
- It ran in its own state dir; 05-20 must start `target/campaign-gate` clean.

### Notes
- Shell `pkill -f <pattern>` matched its own shell once (exit 144). Processes were then killed by pid.
- The 05-18 tip was rebased onto d2b7e57 (fix for torn reads in StoreReader) before any campaign run, as the coordinator asked.

## Verification

- `cargo test -p ostro-crashtest --test check_controls --locked`: 15/15. Lib unit tests: 7/7, including quota, shard-id parsing, variants, the identity guard and the report.
- `cargo clippy -p ostro-crashtest --all-targets --features failpoints --locked -- -D warnings`: clean, also without the feature. `cargo doc -p ostro-crashtest` with `-D warnings`: clean.
- The plan's Task 2 verify (smoke + report + greps) passed.
- `server_campaign::expected_matches_guest` passed.
- **`bash scripts/ci-local.sh --no-musl`** from the worktree at `16f8e66` (`CARGO_TARGET_DIR=target/wt-05-19`): **all steps passed** (CI_EXIT=0).
  - Steps: fmt, clippy, warning-free build, the sdk workspace, guests, order-saga, `cargo test` (612 s, 151 `test result: ok`, no failures), serial sleepers, failpoint list sync, failpoint suites (851 s), the new **server campaign smoke (20 kills) in 47 s** (`kills=20 lost_acked_runs=0 duplicate_journal_entries=0 ... RESULT: PASS`), deny, vendored, deny fixtures, forbidden crates and workflow validation. musl was skipped.
  - The smoke reused the failpoint binary already in `target/failpoints`. On a fresh CI runner the test builds it first (about 5 min).

## Hand-off notes for 05-20

- **Gate command** (start from a clean dir; never reuse the smoke, timing or timing500 dirs):
  ```
  rm -rf target/campaign-gate
  export OSTRO_CAMPAIGN_STATE_DIR=target/campaign-gate OSTRO_CAMPAIGN_FAULTS=10000 \
         OSTRO_CAMPAIGN_SHARDS=20 OSTRO_CAMPAIGN_PARALLEL=6
  for ids in 0-5 6-11 12-17 18-19; do
    OSTRO_CAMPAIGN_SHARD_IDS=$ids cargo test --release -p ostro-crashtest --features failpoints \
      --test server_campaign --locked -- --ignored --nocapture campaign
  done
  cargo test --release -p ostro-crashtest --features failpoints --test server_campaign --locked \
    -- --ignored --nocapture report | tee target/campaign-gate/report.txt
  ```
  Run each batch with `run_in_background` (about 28 min each). If a batch is cut off, rerun the same command: the runner resumes and kills any orphaned servers.
- The report's first line holds the QA-01 numbers. Expect `kills=10000 ... RESULT: PASS`.
- Variant mix: about 60% plain, 30% failpoint, 10% ENOSPC (timing500: 1,815 / 898 / 287). The per-site table gives the coverage.
- **Disk:** about 375 MB per shard at 500 kills, so about 7.5 GB for the gate. **Memory:** about 4 GB peak per batch of 6.
- **QA-01:** REQUIREMENTS left Pending. It flips after the 10k gate.
- **Deferred:** see deferred-items.md (no data-dir lock; executor-site faults have no WARN line).

## STATE notes (for the orchestrator; STATE.md not edited, no gsd-tools mutators run)
- Decision: campaign checks run on a clean restart after every kill; armed servers are used only for the fault window.
- Decision: the expected crash-saga result is computed from journaled signals, and acked signals must be journaled exactly once.
- Position: 05-19 complete. Phase 5 progress is 24/25 summaries. Next is 05-20 (wave 10, gate + freeze).
