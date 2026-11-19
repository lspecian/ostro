---
phase: 05-workflow-semantics-core-hardening
plan: 18
subsystem: testing
tags: [qa-01, failpoints, fault-injection, enospc, eio, crash-campaign, ci, vendored, raft, aot, executor, dispatcher]

requires:
  - phase: 05-14
    provides: "crates/ostro binary with the CLI-06 client verbs (main.rs shape)"
  - phase: 05-16
    provides: "pipelined AttemptGranted + spawn_ack window, DispatcherStats.leases gauge, final executor start path"
provides:
  - "`ostro --features failpoints` chain (-> ostro-server -> ostro-partition / ostro-log / ostro-wal / ostro-blob / ostro-meta / ostro-raft-driver / ostro-engine); main keeps fail::FailScenario::setup() alive (FAILPOINTS env); build-info ends in `failpoints=on`"
  - "meta::log_append / meta::fsync in the vendored raft log store (Ostro P2), EIO or ENOSPC (`return(enospc)`; log_append ENOSPC is a torn half-write)"
  - "MetaHandle::failure() and propose errors that name the driver's storage failure"
  - "aot::write in DiskCache::put (EIO / ENOSPC half-write), non-fatal"
  - "executor::before_append / executor::after_append_before_done around every executor append (RunStarted + engine appends), return -> AppendError::Unavailable (EIO / ENOSPC text)"
  - "dispatcher::before_grant (before the AttemptGranted append) and dispatcher::before_ack (pipelined-grant ack window), return -> log I/O error (EIO / ENOSPC)"
  - "Test targets meta_failpoints (5), aot_failpoints (2), failpoints_phase5 (5), dispatcher_failpoints (4), all required-features = [\"failpoints\"]"
  - "FAILPOINT_CRATES `crate[:target]` entries + ci-local `failpoint list sync (ci.yml)` step; ci.yml Failpoint suites runs the identical commands"
affects: [05-19, 05-20]

tech-stack:
  added: []
  patterns:
    - "Failpoint site = `#[cfg(feature = \"failpoints\")] fn failpoint_eval(name)` + `failpoint!(\"name\")` macro that expands to `Ok(())` without the feature (no call, no name string in the binary)"
    - "`return` = injected EIO, `return(enospc)` = ENOSPC (write sites write half the buffer first), following wal::write"
    - "FAILPOINT_CRATES `crate:target` restricts the test run to one failpoint target; the crate is still linted with the feature"

key-files:
  created:
    - crates/ostro-meta/tests/meta_failpoints.rs
    - crates/ostro-engine/tests/aot_failpoints.rs
    - crates/ostro-server/tests/failpoints_phase5.rs
    - crates/ostro-partition/tests/dispatcher_failpoints.rs
  modified:
    - crates/ostro/Cargo.toml
    - crates/ostro/src/main.rs
    - crates/ostro-server/Cargo.toml
    - crates/ostro-server/src/executor.rs
    - crates/ostro-partition/Cargo.toml
    - crates/ostro-partition/src/leader/dispatcher.rs
    - crates/ostro-meta/Cargo.toml
    - crates/ostro-meta/src/handle.rs
    - crates/ostro-meta/src/lib.rs
    - crates/ostro-engine/Cargo.toml
    - crates/ostro-engine/src/cache.rs
    - vendor/ostro-raft-driver/Cargo.toml
    - vendor/ostro-raft-driver/src/log_store.rs
    - VENDORED.md
    - scripts/ci-local.sh
    - .github/workflows/ci.yml
    - Cargo.lock

key-decisions:
  - "meta:: sites live in the vendored raft log store (Ostro P2): ostro-meta never calls the log store itself (RaftDriver::start owns it), so there is no ostro-meta seam that reaches real disk I/O"
  - "A meta storage error stops the meta driver (existing Fatal path); MetaHandle surfaces it but does not restart in place; recovery = process restart, which is what the campaign does"
  - "Executor sites cover the RunStarted append too, so `return` at the first evaluation hits seq 0; `after_append_before_done` reports an accepted append as Unavailable (replay, never re-append)"
  - "Extra dispatcher site dispatcher::before_ack (the 05-16 grant-append -> ack window); `return` there = ack outcome unknown -> lease dropped, job revoked, re-grant above the token"
  - "FAILPOINT_CRATES: ostro-meta full suite (~9 s single-threaded); ostro-engine and ostro-server restricted to aot_failpoints / failpoints_phase5 (full suites are far over 5 min single-threaded)"

patterns-established:
  - "Per-site failpoint test: FailScenario::setup() per test, counted actions (`1*off->1*return`) to target the Nth evaluation, assert the clean end state (journal once, contiguous seqs; meta state == acknowledged state after restart)"

requirements-completed: []

duration: ~1h45m
completed: 2026-11-19
---

# Phase 5 Plan 18: Failpoint feature chain + new sites + CI sync Summary

**The real `ostro` binary now builds with every failpoint compiled in (`--features failpoints --target-dir target/failpoints`, configured through `FAILPOINTS`). It has seven new sites in meta (vendored raft store), AOT, executor and dispatcher, each with EIO/ENOSPC variants and a single-threaded test. FAILPOINT_CRATES and ci.yml run the same failpoint commands, and ci-local now checks that they match.**

## Performance

- **Duration:** about 1h45m (15:53 to about 17:40 CET, 2026-11-19), including two ~25 min ci-local runs.
- **Tasks:** 2/2.
- **Files:** 4 test files created; 17 files modified, plus ROADMAP.

## Accomplishments

### Feature chain
- `cargo build --release -p ostro --features failpoints --target-dir target/failpoints --locked` builds; the integrated tree took 2m29s incrementally.
- `ostro`'s `failpoints` feature forwards to `ostro-server`, which forwards to the rest of the chain:
  - `ostro-partition`, `ostro-log` (and through it the vendored `ostro-wal`), `ostro-blob`;
  - `ostro-meta`, and through it the vendored `ostro-raft-driver`;
  - `ostro-engine`.
- `main` holds `fail::FailScenario::setup()` for the whole process, so the `FAILPOINTS` env var configures the sites.
  - An invalid spec aborts at start with `invalid failpoint: "..."`.
  - `build-info` ends in ` failpoints=on`.
- Without the feature no failpoint code is compiled:
  - `cargo tree -p ostro` without the feature has no `fail` package.
  - A plain debug `ostro` build contains none of the new site names (checked with `strings`). The dispatcher and executor call sites use a `failpoint!` macro that expands to `Ok(())` without the feature.
- The failpoint binary goes to `target/failpoints/release/ostro`; `target/release` is never touched.

### New sites

| Site | Where | `return` | `return(enospc)` | Effect |
|---|---|---|---|---|
| `meta::log_append` | vendored raft `log_store.rs` `write_entries`, before the segment write | EIO | half the batch written, then ENOSPC | existing `set_len` cut-back; driver stops (`Fatal`) |
| `meta::fsync` | before the append `sync_data`, and before the state/snapshot temp-file `sync_all` in `write_atomically` | EIO | ENOSPC | driver stops; `MetaHandle` propose error names `raft storage: ... (os error N)`; `MetaHandle::failure()` |
| `aot::write` | `DiskCache::put`, before the artefact write | EIO | half written to the temp file, then ENOSPC | non-fatal: warning logged, temp file removed, in-memory compile used, next load recompiles |
| `executor::before_append` | before every executor append (RunStarted + `EngineSink`) | `Unavailable` (EIO text) | `Unavailable` (ENOSPC text) | nothing appended; evicted `External`, re-activated with the same token |
| `executor::after_append_before_done` | after an accepted append, before the engine sees the result | as above | as above | accepted append reported as failed; re-activation replays it from the journal |
| `dispatcher::before_grant` | `pipelined_grant`, right before the `AttemptGranted` append | log I/O EIO | log I/O ENOSPC | nothing logged; re-queued after `GRANT_RETRY_MS` |
| `dispatcher::before_ack` | pipelined-grant ack task, before `wait_applied` (the job is already running) | log I/O EIO | log I/O ENOSPC | ack outcome unknown: lease dropped, job revoked, re-grant above the token |

`sleep(ms)` and `panic` are the fail crate's own actions:
- `sleep` blocks one runtime worker thread.
- A `panic` in a dispatcher or executor task is handled by the dispatcher's `on_join_error`, which drops the lease and retries.

### Tests (all `required-features = ["failpoints"]`, run with `--test-threads=1`)
- `ostro-meta/tests/meta_failpoints.rs` (5 tests):
  - `log_append` and `fsync` × EIO and ENOSPC;
  - each checks the propose error and `failure()`, then a restart recovers exactly the acknowledged state, and a new write survives another restart;
  - a fifth test checks that `off` changes nothing.
- `ostro-engine/tests/aot_failpoints.rs` (2 tests), EIO and ENOSPC:
  - the `det` activation completes with the reference output and nothing is left in `aot/`;
  - a restart under the fault recompiles;
  - once the fault is cleared the artefact is persisted, and the next restart gets an authenticated hit.
- `ostro-server/tests/failpoints_phase5.rs` (5 tests, real `EngineExecutor` running `sdk-basic`). Each checks the right result and a journal with each event once, in contiguous seqs:
  - `before_append` `return` on the first engine append (EIO/ENOSPC): evicted `External`, then re-activated with the same token;
  - `before_append` `return` on RunStarted;
  - `after_append_before_done` `return` on RunStarted and on the first engine append: replays instead of re-appending;
  - `sleep` only delays;
  - `panic` is retried with a higher token.
- `ostro-partition/tests/dispatcher_failpoints.rs` (4 tests):
  - a delayed grant still gives exactly one attempt (one `AttemptGranted`, one job, one append after it);
  - a failed grant (EIO/ENOSPC) logs nothing and gives one attempt;
  - a delayed ack gives one attempt;
  - a failed ack revokes job 1, re-grants n=2, and the run completes once with token 2;
  - every test ends with the `DispatcherStats.leases` gauge at 0.

### CI
- `FAILPOINT_CRATES` = `ostro-wal ostro-blob ostro-log ostro-partition ostro-crashtest ostro-meta ostro-engine:aot_failpoints ostro-server:failpoints_phase5`.
  - A `crate:target` entry adds `--test <target>`.
  - Restriction choices: ostro-meta runs its full suite (~9 s single-threaded); the engine and server full suites are far over 5 min single-threaded, so they are restricted.
  - Clippy with `--features failpoints` covers every listed crate plus `ostro` (`FAILPOINT_LINT_ONLY`).
- The ci.yml "Failpoint suites" step runs the identical 8 commands in the same order.
- New ci-local step **"failpoint list sync (ci.yml)"** rebuilds the commands from `FAILPOINT_CRATES` and diffs them against the ci.yml step. A negative test (one line deleted from a copy of ci.yml) fails with a diff.
- The ci-local crash-loop comment now covers the 05-19 server campaign and the `target/failpoints` build.

## Task Commits

All are on `main`. Each was built on the current main tip through a private index (`commit-tree` + CAS `update-ref`).

1. **Task 1: feature chain + sites + per-site tests:** `8722585` (feat).
2. **Task 2: FAILPOINT_CRATES / ci.yml sync:** `aaf3908` (chore).
3. **Compile-out tightening (`failpoint!` macro):** `6d2ee84` (refactor).
4. **Executor module doc after D24 (coordinator request):** `db8e3dd` (docs).
5. **Plan metadata (this SUMMARY, ROADMAP, executor private doc link):** see the final docs commit.

## Deviations from Plan

### Auto-fixed issues

**1. [Rule 3 - Blocking] The ostro-meta seam does not exist, so the meta:: sites are in the vendored raft log store**
- `RaftDriver::start` opens and drives `RaftLogStore` internally, so ostro-meta never calls the store.
- The sites went into `vendor/ostro-raft-driver/src/log_store.rs` as **Ostro P2**, as the plan's phase rules allow. Both of its VENDORED.md rows were updated (`Cargo.toml` and `log_store.rs`, which went from unchanged to modified), and `check-vendored.sh` is green.
- `sm.rs` (in files_modified) needed no change. `handle.rs` gained the failure surfacing instead.

**2. [Rule 3 - Blocking] `crates/ostro-partition/Cargo.toml` (not in files_modified)**
- It needed a `[[test]] dispatcher_failpoints` entry with `required-features`.
- No other plan in this wave touches it.

**3. [Rule 2 - Correctness] Meta failures were opaque**
- A storage error made `propose` return just `stopped`.
- `MetaHandle` now waits up to 1 s for the driver's published `failed` reason and includes it.
- It also adds `MetaHandle::failure()`.

**4. [Rule 1] Plain builds kept failpoint name strings**
- No-op stub functions still received the site names as string literals.
- Replaced by the `failpoint!` macro (`6d2ee84`).

**5. Extra site `dispatcher::before_ack`**
- This is the 05-16 hand-off candidate: the window between the grant append and its ack.
- Its name uses the reserved `dispatcher::` prefix.

**6. Coordinator request: executor module doc**
- The text said the server's start paths leave `run_started` empty (D3). That has been wrong since 05-25 (D24).
- It now says the RunStarted append covers starts without the field: the continue-as-new successor (`derive::successor_start`) today, and child starts once they exist (Phase 9).
- The suggested wording "inbound, child and continue-as-new" was adjusted: inbound and continue-as-new are the same path, and child starts do not exist yet.
- Also fixed my own private intra-doc link (`[EngineSink]`).

### Notes
- The first full ci-local log was unusable. It was written to the shared scratchpad name `ci-local.log`, and a parallel agent's run (target dir `wt-05-17`, no "failpoint list sync" step) overwrote it with its own summary, which showed a `cargo test` FAIL.
- The rerun with a unique log name passed on every step.

## Verification

- Per-site suites:
  - `meta_failpoints` 5/5;
  - `aot_failpoints` 2/2;
  - `failpoints_phase5` 5/5;
  - `dispatcher_failpoints` 4/4 (re-run after the macro change: 4/4 and 5/5).
- Vendored driver unit tests: 26/26, with and without `--features failpoints`.
- Clippy `-D warnings`:
  - the workspace, without the feature;
  - with the feature: `ostro-meta`, `ostro-engine`, `ostro-partition`, `ostro-server`, `ostro`.
- The warning-free workspace build is clean, and `bash scripts/check-vendored.sh` passes all checks.
- The plan's Task 2 grep passes.
- Release failpoint binary:
  - built into `target/failpoints`;
  - `build-info`, with and without `FAILPOINTS` set, prints `... allocator=mimalloc failpoints=on`;
  - an invalid spec aborts.
- **`bash scripts/ci-local.sh --no-musl`** on `6d2ee84` (rebased on 05-25 D24): **all steps passed**.
  - Steps: fmt, clippy, build, sdk, guests, order-saga, `cargo test` (617 s), sleepers, failpoint list sync, failpoint suites (905 s), deny, vendored, deny fixtures, forbidden crates, workflow validation. musl was skipped.
  - Later changes (the `db8e3dd` doc text, the doc-link fix, and main's newer 05-17 test/docs commits) were checked with fmt, ostro-server clippy with and without the feature, and rustdoc. There was no full rerun.

## Hand-off notes

### 05-19 (kill campaign)
- **Binary:** `cargo build --release -p ostro --features failpoints --target-dir target/failpoints --locked`.
  - Assert that `target/failpoints/release/ostro build-info` contains `failpoints=on`.
  - Configure through `FAILPOINTS='site=action;site2=action'`. An invalid spec makes the process panic at start.
- **Site menu:**
  - Storage: `wal::write|fsync|dir_fsync`, `state::commit|persist`, `blob::fsync|before_rename`, `meta::log_append|fsync`, `aot::write`.
  - Server logic: `executor::before_append`, `executor::after_append_before_done`, `dispatcher::before_grant`, `dispatcher::before_ack`.
  - `return(enospc)` gives ENOSPC at `wal::write` and at every 05-18 site. The Phase 2 sites `wal::fsync`, `wal::dir_fsync`, `blob::*` and `state::*` ignore the arg: `return` gives EIO, an injected `StoreError`, or `io::Error::other`. If the campaign needs ENOSPC at those sites, it has to extend them.
- **What each storage error does to the process:**
  - meta errors stop the meta driver for good (`MetaHandle::failure()` is set) but do **not** exit the process. Later metadata proposals fail with `stopped: raft storage: ...`. The campaign must end such iterations with `kill -9` and a restart.
  - WAL/state errors fail-stop the partition, as in Phase 2.
  - `aot::write` is harmless by design. It is useful as a "noise" fault that must never change outcomes.
- **Executor windows for kill timing:**
  - `executor::after_append_before_done=sleep(N)` holds an accepted, durable append before the engine learns of it. Kill inside it: after restart there must be no duplicate events.
  - `executor::before_append=sleep(N)` holds an append that has not been submitted. Kill inside it: the work must be redone exactly once.
  - `sleep` blocks a tokio worker thread. Keep N modest, or the lease heartbeat can stall.
- **Grant windows:**
  - `dispatcher::before_ack=sleep(N)` keeps a pipelined grant unacknowledged while its job runs. Kill there to land "job started, grant ack not seen".
  - The grant's *apply* is not delayed by either dispatcher site. The grant-append → apply window is inherent and short; it is hit by random kill timing.
- **Probes:**
  - `DispatcherStats.leases` back at 0 (no lease leaks).
  - The 05-16 log scan: per run, tokens strictly increase, and the first `EventsAppended` of each token comes after its grant. `dispatcher_failpoints.rs::grants_and_appends` is a compact copy.
- **CI smoke:** if 05-19 adds a failpoint test target, add it to `FAILPOINT_CRATES` as `crate:target`. Add the identical line to ci.yml in the same position, or the sync step fails.

### 05-20 (gate)
- QA-01 stays Pending until the campaign has run at least 10,000 kills.
- New vendored patch: `ostro-raft-driver` **P2** (failpoints only, compiled out without the feature).

## STATE notes (for the orchestrator; STATE.md not edited, no gsd-tools mutators run)
- Decision: the meta:: failpoints live in the vendored raft log store (Ostro P2). There is no ostro-meta seam.
- Decision: FAILPOINT_CRATES uses `crate:target` restriction syntax. ci-local checks that it matches ci.yml.
- Position: 05-18 complete; Phase 5 progress 23/25 summaries; next is 05-19 (wave 9).

## Self-Check: PASSED

- FOUND: crates/ostro-meta/tests/meta_failpoints.rs, crates/ostro-engine/tests/aot_failpoints.rs, crates/ostro-server/tests/failpoints_phase5.rs, crates/ostro-partition/tests/dispatcher_failpoints.rs
- FOUND: crates/ostro/src/main.rs contains `FailScenario`
- FOUND commits on main: 8722585, aaf3908, 6d2ee84, db8e3dd

