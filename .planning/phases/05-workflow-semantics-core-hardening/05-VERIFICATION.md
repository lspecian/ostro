---
phase: 05-workflow-semantics-core-hardening
verified: 2026-11-20T08:30:00Z
status: human_needed
score: 5/5 success criteria verified by automated evidence (environment-bound legs of SC-1 and SC-4 human_needed)
re_verification: false
human_verification:
  - test: "Native Linux NVMe perf gate (D15/D16): profile A at 64 clients and at 1 client (300 runs), three runs each, median kept; then profiles b and expiry"
    expected: "steps/s >= 5,000 with fsync on; idle start->first step p50 < 5 ms; all steps verified on disk after restart"
    why_human: "D15 makes native Linux NVMe with 8 vCPU the gate environment. This host is WSL2, so the 30k steps/s and 3.5 ms p50 figures are advisory only"
  - test: "After the first push, check that the linux-aarch64-musl (ubuntu-24.04-arm) and macos-aarch64 (macos-15) CI legs run the workspace tests, including ostro-engine replay_corpus, and are green"
    expected: "replay_corpus_entries passes for all 30 entries on both aarch64 legs"
    why_human: "There is no git remote yet (FOUND-05). The aarch64 legs exist in ci.yml but have never run"
  - test: "Run bash examples/order-saga/quickstart.sh on a clean machine where docker info succeeds"
    expected: "QUICKSTART_RESULT: within_budget wall=<N>s with N <= 600"
    why_human: "Needs a clean machine or a working docker. The local cold-clone fallback took 254 s (05-17), but that is not a clean machine"
  - test: "Optional: real ENOSPC leg on a 64 MiB tmpfs (sudo mount), with OSTRO_CAMPAIGN_STATE_DIR on that mount"
    expected: "Campaign report RESULT: PASS"
    why_human: "Needs root. The gate already covers ENOSPC through failpoint return(enospc) variants (1,087 of the 10,000 kills)"
  - test: "User review of adopted decisions D7 (step-local RNG frozen), D8 (relaxed startup criterion), D14 (no record/variant shape changes before the freeze) and of the freeze flip (docs/wit-0.1-freeze-review.md)"
    expected: "User accepts them, or opens follow-ups. The user has already kept fc47569 and its tag and approved D26"
    why_human: "These decisions were adopted under auto_advance while the user was away"
---

# Phase 5: Workflow Semantics & Core Hardening Verification Report

**Phase Goal:** The single-node core is feature-complete for durable workflows and proven crash-safe. The docs/05 Phase 1 exit gate passes, and WIT 0.1, the command set and the on-disk format are frozen.
**Verified:** 2026-11-20 (HEAD `4a3c149`)
**Status:** human_needed
**Re-verification:** No, initial verification.

## How this was verified

- Everything was read from HEAD `4a3c149`, using a detached worktree. The main checkout was left alone. Its working tree is stale: it shows about 13k deleted lines under `crates/`, including all of `crates/ostro-cli/src/commands/` and the crashtest campaign, because HEAD was moved by private-index commits. Do not run `git add -A` or `git commit -a` from it.
- Targeted tests were re-run in that worktree with `CARGO_TARGET_DIR=target/wt-verify5`, which was deleted afterwards. Results are below.
- The gate artefacts were checked independently:
  - `target/campaign-gate/report.txt` and `report-provenance.txt` exist and say RESULT: PASS.
  - The checker blob at HEAD is `bebc425b…`, the same as in the provenance file.
  - The failpoint binary's sha256 is still `ddbd2bbb…`.
  - `git diff c6b1c09 HEAD -- crates/` touches no engine, server or partition source, only crashtest, examples, test fixtures and goldens. So the binary that ran the campaign matches HEAD's product code.

## Goal Achievement

### Observable Truths (ROADMAP success criteria)

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | The order-saga example (5 steps, a timer, a signal) runs from `ostro dev` on a clean machine in under 10 min. Concurrent gather/race completion order replays identically. A sleeping run holds no memory. | ✓ VERIFIED (clean-machine timing is human_needed) | **Example:** `examples/order-saga/src/lib.rs` has reserve, charge (retry policy), `ctx.sleep` timer, a `select!` of the `approve` signal against cancellation, then ship, notify and complete, with shielded compensations. **CLI run, re-run here:** `crates/ostro/tests/order_saga.rs` passed 4/4 (happy, compensated, cancelled, and kill -9 during the settle sleep) against a spawned `ostro dev`. **gather/race, re-run here:** `semantics_e2e::gather_race` passed. It calls `assert_replays_identically` on a fresh Runtime, and there is also the engine `gather_race_prop` proptest. **Sleepers, re-run serially here:** `semantics_sleepers::sleepers_hold_no_memory` passed. It asserts `live_activations()==0`, `parked_count()==0` and `budget().used()==0` with sleepers present. **Quickstart:** `quickstart.sh` has a 600 s budget and took 254 s locally (05-17). The docker clean-machine run is human_needed. |
| 2 | Users can signal, gracefully cancel (guest cleanup runs), terminate, retry, long-poll results and complete await tokens via the API and via `ostro run/ls/inspect/history/signal/cancel/retry` (`--json`, `--dry-run`). Run and schedule-to-start timeouts fire. `patched(id)` keeps in-flight runs on old paths. | ✓ VERIFIED | **REST** (`crates/ostro-api/src/rest.rs`): `/v1/runs/{id}/signals/{name}`, `/cancel`, `/terminate`, `/retry`, `/result` (long-poll), `/v1/tokens/complete`, plus workflow and queue policy routes. gRPC parity is in the same suite. **Server:** `control/lifecycle.rs` implements `dry_run`, which validates and submits nothing. **CLI:** `ClientCommand` has Run, Ls, Inspect, History, Signal, Cancel, Terminate, Retry, Result, Token and Deploy, with a global `--json` and per-verb `--dry-run`. **Re-run here:** `lifecycle_api` 13/13 (REST and gRPC; long-poll: no lost wakeup, timeout, continuations). `cli_verbs` 7/7. `semantics_e2e` 18/18, which includes `cancel_cleanup`, `terminate_runs_no_guest_code`, `run_timeouts`, `schedule_to_start_without_capacity`, `schedule_to_start_with_capacity_never_fires`, `patched_in_flight`, `tokens` and `continue_as_new`. SDK: `Ctx::patched` is at `sdks/rust/ostro-sdk/src/ctx.rs:187`. |
| 3 | The crash-injection harness runs ≥ 10,000 faults (`kill -9`, failpoints, EIO/ENOSPC) under load, with 0 lost accepted runs, 0 duplicate journal entries, and every run terminal or correctly resumed. | ✓ VERIFIED | **Report:** `target/campaign-gate/report.txt` shows `kills=10000 lost_acked_runs=0 duplicate_journal_entries=0 non_terminal_runs=0 lost_acked_signals=0 wrong_results=0 other_violations=0`, 20/20 shards, `acked_runs=120888`, RESULT: PASS. By variant: plain 5,865, failpoint 3,048, ENOSPC 1,087. 21 sites were hit, including `wal::write/fsync/dir_fsync`, `state::commit/persist`, `meta::*`, `blob::*`, `executor::*`, `dispatcher::*` and `aot::write`. **EIO:** the failpoint `return` variants produce `from_raw_os_error(5)` (`ostro-server/src/executor.rs`, `ostro-engine/src/cache.rs`). **Checker, re-run here:** `check_controls` 16/16, including the D26 controls (user-approved narrowing, with negative cases). **CI:** a 20-kill campaign smoke runs in ci.yml and ci-local. |
| 4 | The replay corpus is green in CI on x86_64 and aarch64. Terminal runs and journals are deleted after their TTL. One node sustains ≥ 5,000 trivial Rust steps/s with fsync on, and start → first step is < 5 ms p50. | ✓ VERIFIED locally on x86_64 (aarch64 CI and the native perf gate are human_needed) | **Corpus, re-run here:** `replay_corpus` 5/5, covering 30 entries and the negative controls (truncated journal, flipped byte, wrong sha). The x86_64 musl leg passed in ci-local. **CI:** ci.yml has `linux-aarch64-musl` on `ubuntu-24.04-arm` and `macos-aarch64` on `macos-15`, both running `cargo test --workspace`. There is no remote, so these legs have never run. **TTL, re-run here:** `semantics_e2e::retention` passed. The partition `apply_retention` suite includes the 50k-event expiry. Retention policy resolution is in `ostro-server/src/retention.rs`. **Perf:** WSL2 advisory only, about 30.1–30.5k steps/s at 64 clients and p50 3.50–3.59 ms at 1 client. Under D15 the native NVMe gate is human_needed. |
| 5 | WIT `ostro:runtime@0.1.0` is tagged frozen. A CI check fails any non-additive change to it. | ✓ VERIFIED | **Tag:** `wit/ostro-runtime-v0.1.0` is an annotated tag on `fc47569`. **Pins:** `wit/FROZEN.toml` sha256 `611d2982…` equals `sha256sum wit/ostro-runtime-0.1.0/runtime.wit` at HEAD. Nothing under `wit/` or in the goldens changed between `fc47569` and HEAD. **Re-run here:** `ostro-witcheck`. `frozen_0_1_0_is_byte_identical` fails on any byte edit. `runtime_0_1_chain_is_additive` checks new 0.1.N packages. `chain_check_on_fixture_trees` shows a breaking 0.1.1 is rejected. The semver fixture table also passed. **Other frozen formats:** `golden_frozen` 3/3 (command set and event codec), `meta_golden` 5/5 (META format), `format_corpus` 2/2 (on-disk v0.1). **CI:** all of these run under `cargo test --workspace` (members = `crates/*`) on every leg. |

**Score:** 5/5 truths have passing automated evidence. The environment-bound parts of SC-1 (clean-machine timing) and SC-4 (aarch64 CI run, native perf) are human_needed.

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `examples/order-saga/` (+ `quickstart.sh`) | SC-1 example | ✓ VERIFIED | Substantive. Driven by `crates/ostro/tests/order_saga.rs`. |
| `crates/ostro-api/src/rest.rs` + proto | lifecycle REST/gRPC | ✓ VERIFIED | Wired to the server ControlPlane. Tested by `lifecycle_api`. |
| `crates/ostro-server/src/control/lifecycle.rs` | ops + dry_run | ✓ VERIFIED | Used by both transports. |
| `crates/ostro-cli/src/commands/{runs,lifecycle,deploy}.rs` | CLI-06 verbs | ✓ VERIFIED | Present at HEAD (deleted only in the stale main working tree). Tested by `cli_verbs`. |
| `crates/ostro-partition/src/apply/{lifecycle,timer,retention,run}.rs`, `leader/retention.rs` | waits, timeouts, retention | ✓ VERIFIED | Covered by the apply_* suites and semantics_e2e. |
| `sdks/rust/ostro-sdk/src/ctx.rs` | patched, sleep, shielded, continue_as_new | ✓ VERIFIED | Used by the guests and order-saga. |
| `crates/ostro-crashtest/src/{campaign,check,server_target,workload}.rs` | QA-01 harness | ✓ VERIFIED | Produced the gate report. Checker blob matches the provenance. |
| `tests/replay-corpus/` (30 entries), `tests/format-corpus/v0.1/` | QA-02, on-disk freeze | ✓ VERIFIED | Both replay green. |
| `wit/FROZEN.toml`, `crates/ostro-witcheck/` | WIT-07 | ✓ VERIFIED | Tag present. Tests green. |
| `.github/workflows/ci.yml` | x86_64/aarch64 legs, failpoint suites, campaign smoke | ✓ VERIFIED (not yet executed remotely) | |

### Key Link Verification

| From | To | Via | Status |
|------|----|-----|--------|
| CLI verbs | REST `/v1/runs/{id}/…` | `ostro-cli/src/client.rs` | ✓ WIRED (cli_verbs and order_saga pass against a spawned binary) |
| REST/gRPC | partition apply | server ControlPlane → Cmd 8..12 | ✓ WIRED (lifecycle_api asserts on the store snapshot) |
| Run and s2s timeout policy | timer rows | `retention.rs` policy resolution → apply/timer | ✓ WIRED (run_timeouts, schedule_to_start_*) |
| Retention TTL | run/journal deletion | leader/retention → apply/retention | ✓ WIRED (semantics_e2e::retention) |
| WIT file | CI failure on change | FROZEN.toml sha + witcheck chain in `cargo test --workspace` | ✓ WIRED |
| Corpus | CI legs | `replay_corpus` in `cargo test --workspace` / `cargo-zigbuild test` | ✓ WIRED (remote run pending) |

### Requirements Coverage

| Requirement | Description | Status | Evidence |
|-------------|-------------|--------|----------|
| WF-03 | gather/race journaled and replayed identically | ✓ SATISFIED | gather_race (re-run), gather_race_prop |
| WF-04 | sleep, and sleeping runs hold no memory | ✓ SATISFIED | sleepers_hold_no_memory (re-run) |
| WF-05 | signal with timeout, sent via API | ✓ SATISFIED | signal_timeout, signal_rest_and_grpc, `ostro signal` |
| WF-08 | `patched(id)`, runs pinned to their sha | ✓ SATISFIED | patched_in_flight (re-run) |
| WF-09 | run timeout and schedule-to-start timeout | ✓ SATISFIED | run_timeouts, schedule_to_start_* (re-run) |
| API-05 | signal/cancel/terminate/retry | ✓ SATISFIED | lifecycle_api (re-run) |
| API-06 | long-poll result | ✓ SATISFIED | long_poll_* (re-run) |
| API-07 | complete await token | ✓ SATISFIED | token_completion_rest_and_grpc, tokens (re-run) |
| CLI-06 | CLI verbs with --json/--dry-run | ✓ SATISFIED | cli_verbs, order_saga (re-run) |
| OPS-06 | TTL deletion of runs/journals; dedupe retention | ✓ SATISFIED | retention e2e (re-run), apply_retention |
| WIT-07 | WIT 0.1 frozen, additive only | ✓ SATISFIED | tag + FROZEN.toml + witcheck (re-run). REQUIREMENTS.md still says "Pending" (bookkeeping) |
| QA-01 | ≥ 10,000 faults, 0 lost, 0 duplicates | ✓ SATISFIED | gate report PASS. REQUIREMENTS.md still says "Pending" (bookkeeping) |
| QA-02 | corpus replayed across versions and x86_64/aarch64 | ? NEEDS HUMAN (aarch64 leg) | x86_64 gnu+musl green. aarch64 legs wired in ci.yml, not yet run. REQUIREMENTS.md says "Partial" |

No orphaned requirements. Every Phase 5 ID in REQUIREMENTS.md's traceability table is claimed by at least one plan.

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| `crates/ostro-engine/tests/replay_corpus.rs` | 379 | Returns success when the corpus directory is empty ("bootstrap") | ℹ️ Info | Harmless while 30 entries are committed. It would hide an accidentally emptied corpus. Assert a minimum entry count (e.g. ≥ 30) as a cheap follow-up. |
| ostro-server executor (deferred-items) | — | Executor-site failpoint hits are not logged at WARN | ℹ️ Info | Only the campaign's informational `failpoint_hits` count is affected. Correctness is unaffected. |
| main checkout working tree | — | Stale tree/index (about 13k lines "deleted" vs HEAD) | ⚠️ Warning (process) | Not a code defect. A careless `git commit -a` from the main checkout would revert Phase 5. Sync it deliberately (e.g. `git stash` then `git checkout -- .`, or `git reset --hard HEAD` once nothing local is needed). |

No TODO/FIXME stubs or placeholder implementations were found in the Phase 5 surfaces checked.

### Test re-runs performed for this verification (HEAD 4a3c149, x86_64 gnu, WSL2)

| Suite | Result |
|-------|--------|
| `scripts/check-vendored.sh` | 5/5 PASS (confirms the da6c7a9 fix) |
| `ostro-witcheck` (semver_fixtures, wit_freeze) | 4 + 4 passed |
| `ostro-types --test golden_frozen` | 3 passed |
| `ostro-meta --test meta_golden` | 5 passed |
| `ostro-crashtest --test check_controls` | 16 passed |
| `ostro-engine --test replay_corpus` | 5 passed (30 entries) |
| `ostro-partition --test format_corpus` | 2 passed |
| `ostro-server --test semantics_e2e` | 18 passed |
| `ostro-server --test lifecycle_api` | 13 passed |
| `ostro-server --test semantics_sleepers -- --ignored --test-threads=1` | 2 passed |
| `ostro --test cli_verbs --test order_saga` | 7 + 4 passed |

Not re-run (cost): the 10,000-kill campaign, which was verified through its report, provenance, checker blob and binary hash; the failpoint suites, musl and full ci-local, which passed 20/21 on `3a6c592` with the one failure fixed and re-checked here; and perf.

### Human Verification Required

1. **Native Linux NVMe perf gate (SC-4, D15/D16)**
   - **Test:** On native Linux with NVMe and 8 vCPU, idle, run three times each: `OSTRO_BENCH_PROFILE=a cargo test --release -p ostro-server --test perf --locked -- --ignored --nocapture`, and the same with `OSTRO_BENCH_CONCURRENCY=1 OSTRO_BENCH_RUNS=300`. Then run profiles b and expiry.
   - **Expected:** ≥ 5,000 steps/s and idle p50 < 5 ms. Record the results in docs/benchmarks.md.
   - **Why human:** D15 makes native NVMe the gate. WSL2 numbers are advisory.
2. **aarch64 CI legs (SC-4, QA-02)**
   - **Test:** After the first push, confirm `linux-aarch64-musl` and `macos-aarch64` are green, including `replay_corpus`.
   - **Expected:** All 30 entries replay.
   - **Why human:** There is no git remote yet.
3. **Clean-machine quickstart (SC-1)**
   - **Test:** `bash examples/order-saga/quickstart.sh` with docker usable.
   - **Expected:** `QUICKSTART_RESULT: within_budget`, wall ≤ 600 s.
   - **Why human:** Needs a clean machine or docker.
4. **Optional real-ENOSPC tmpfs leg (QA-01)**
   - **Test:** Mount a 64 MiB tmpfs and run the campaign with its state dir on that mount.
   - **Expected:** PASS.
   - **Why human:** Needs root. ENOSPC is already covered by failpoints.
5. **Decision review (WIT-07)**
   - **Test:** Review D7, D8 and D14 and the freeze flip.
   - **Expected:** The user accepts them.
   - **Why human:** They were adopted under auto_advance.

### Gaps Summary

There are no code or behaviour gaps. Every success criterion and requirement has substantive, wired code and passing tests, re-run here where cheap. The crash gate passed on a binary whose product code matches HEAD.

Remaining items are environment-bound (native perf, aarch64 CI, clean-machine docker, root tmpfs) or a user review. All are by design, as listed in 05-VALIDATION.md § Human-needed.

Bookkeeping left for the orchestrator, not done here per instructions:
- Tick the Phase 5 checkbox in ROADMAP.md.
- In REQUIREMENTS.md, set WIT-07 and QA-01 to Complete and QA-02 to Complete on x86_64, with aarch64 human_needed.
- Update STATE.

Context for downstream gates:
- The freeze commit `fc47569` and its tag predate the final passing campaign. The user explicitly chose to keep them (05-20 "Decision 2 do A").
- The checker was narrowed once, under user-approved D26, with negative controls. All 20 shards were then re-run.

---

_Verified: 2026-11-20_
_Verifier: Claude (gsd-verifier)_
