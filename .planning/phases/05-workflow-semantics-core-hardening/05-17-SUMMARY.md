---
phase: 05-workflow-semantics-core-hardening
plan: 17
subsystem: e2e-docs
tags: [sc-1, cli-06, order-saga, quickstart, docs, workflow-semantics, d21, d23, d24, wf-03, wf-04, wf-05, ops-06]
requires:
  - phase: 05-14
    provides: "ostro CLI verbs (deploy/run/signal/cancel/result/history/inspect/ls), exit codes 0/1/2/3, tests/common run_cli/cli_json"
  - phase: 05-15
    provides: "semantics_e2e / semantics_sleepers proofs cited by the docs"
  - phase: 05-16
    provides: "final start path (pipelined grant), apply support for StartRun.run_started"
  - phase: 05-25
    provides: "D24: the API sets StartRun.run_started (RunStarted at seq 0 for accepted runs); landed while this plan ran"
  - phase: 05-10
    provides: "examples/order-saga"
provides:
  - "crates/ostro/tests/order_saga.rs: success criterion 1 via the CLI only (happy, compensated x3, cancelled, kill -9 + restart)"
  - "tests/common order_saga_wasm(): once-per-process example build into target/order-saga"
  - "examples/order-saga/quickstart.sh: clean-clone quickstart timing (docker rust:1.100 gate, local cold-clone fallback with HUMAN_NEEDED)"
  - "examples/order-saga/README.md: quickstart matching the script exactly + Troubleshooting"
  - "docs/workflow-semantics.md: user-facing semantics reference with 89 verified file::test citations"
  - "docs/03-architecture.md: Phase 5 reality in 2.2/2.3/2.4/3 (commands 1-12, EXPIRE_OP, RequestCancel/TerminateRun, reserved ops, inputs journaled by apply, data-dir layout)"
affects: [05-20, phase-6, phase-8, phase-10, phase-14]
tech-stack:
  added: []
  patterns:
    - "CLI-only e2e: every interaction with the server goes through the spawned `ostro` binary (`cli_json`), history assertions on the `--json` history body"
    - "Quickstart script prints exactly one result line (QUICKSTART_RESULT: ... | HUMAN_NEEDED: ...), no hidden exit codes"
    - "Doc citations checked mechanically: every `file::test` in docs/workflow-semantics.md resolves to an existing fn"
key-files:
  created:
    - crates/ostro/tests/order_saga.rs
    - examples/order-saga/quickstart.sh
    - docs/workflow-semantics.md
  modified:
    - crates/ostro/tests/common/mod.rs
    - examples/order-saga/README.md
    - docs/03-architecture.md
key-decisions:
  - "History assertions use the Phase 5 signal shape (signal_payload + signal_matched, D21) instead of the plan's SignalReceived, and assert signal_received is absent"
  - "quickstart.sh treats docker as usable only if `docker info` succeeds: on this WSL2 host `command -v docker` finds a Docker Desktop shim that cannot run anything"
  - "The local fallback is a fresh `git clone --no-hardlinks file://...` with CARGO_TARGET_DIR/RUSTC_WRAPPER/RUSTFLAGS unset, so the build is cold; it still uses this machine's cargo registry cache and toolchain (labelled as not the gate)"
  - "Docs describe the D24 start path as landed (05-25 committed 4630500/f340835/8863a1e while this plan ran); outbox-started runs (children, continue-as-new successors) keep the executor RunStarted + held signals"
requirements-completed: [WF-03, WF-04, WF-05, CLI-06, OPS-06]
duration: ~1h
completed: 2026-11-19
---

# Phase 5 Plan 17: order-saga via the CLI, quickstart timing, workflow-semantics docs Summary

**Success criterion 1 is proven through the `ostro` CLI alone: the order-saga example (5 steps, a durable timer, an approval signal raced with cancellation, shielded compensations) completes its happy, compensated and cancelled paths against a spawned `ostro dev`, and survives a `kill -9` during its timer with every step executed exactly once. The README quickstart runs from a fresh clone in 254 s locally (cold build); the clean-machine docker figure is human_needed. `docs/workflow-semantics.md` documents every Phase 5 semantic with its proving tests, and `docs/03-architecture.md` now matches Phase 5.**

## Performance

- **Duration:** about 1 h (excluding the full gate).
- **Tasks:** 3 of 3.
- **Files:** 3 created, 3 modified (plus ROADMAP/REQUIREMENTS tracking hunks).
- **Suite time:** `order_saga` 4 tests in 5.7-7.2 s (debug, excluding the one-time example build).

## Task Commits

1. **Task 1: order-saga e2e through the CLI:** `8d2c8c7` (test)
2. **Task 2: quickstart.sh + README:** `157831a` (feat)
3. **Task 3: workflow-semantics.md + docs/03 corrections:** `de0bc8e` (docs)
4. **Task 3 follow-up after 05-25 landed (D24 start path, renamed test citation):** `5993b70` (docs)
5. **Task 1 follow-up (human result line through `run_cli`, the plan's key link):** `01bf244` (test)

Every commit went through a private index (`GIT_INDEX_FILE` seeded with `read-tree` of the current `main`, blobs hashed from the scratch worktree, `commit-tree` + `update-ref main <new> <old>` as a compare-and-swap), followed by `git reset -q -- <paths>` on the shared index and a `git show --stat` check. `main` moved several times under this plan (Phase 14 planning, 05-18 `6d2ee84`, 05-25 `4630500`/`f340835`/`8863a1e`/`1190a31`); each commit was built on the then-current `main`, and no other plan's path was touched.

## What landed

### Task 1: `crates/ostro/tests/order_saga.rs` (CLI only)

Each test spawns its own `ostro dev` (P=4) under `SERIAL` and talks to it only through `ostro deploy | run | signal | cancel | result | history | inspect | ls --json`.

| Test | Proves |
|---|---|
| `happy_path` | (also checks the human `result --wait` line `run <id> completed: {...}` through `run_cli`) deploy `--activate`, `run` (settle 300 ms), `signal approve --payload true`, `result --wait` exit 0 with `{"status":"completed","order_id":"o1","shipment_id":"ship-o1"}`. History: exactly one `step_completed` per forward step in order, one `timer_started` + one `timer_fired`, one `signal_payload` (payload `true`) + one `signal_matched`, no `signal_received`, one `run_completed`, `run_started` first. `inspect` and `ls` agree. |
| `compensated_paths` | `fail_at: create_shipment` → compensated, reason `forced_failure: create_shipment forced to fail`, `["refund_payment","release_inventory"]`, and in the history: failed `create_shipment` < `refund_payment` < `release_inventory`. `fail_at: charge_payment` → 2 `step_attempt_failed` + final failure (3 attempts), no timer, only `release_inventory`. Rejection (`--payload false`) → compensated, reason `rejected`, compensations after the `signal_matched`. |
| `cancelled_path` | `cancel --reason ...` once the run waits for approval (`signal_wait_started` seen): `result --wait` exits **3** with status `cancelled`; history `cancel_requested` < `refund_payment` < `release_inventory`, no forward step after the approval; a late `signal` exits 1 with 409 `run_terminal`. |
| `kill9_during_settle_sleep_then_approve` | non-ephemeral `--data-dir`, run with settle 3 s, SIGKILL while `timer_started` is journaled and `timer_fired` is not, restart on the same dir, wait for `signal_wait_started` (the timer survived and fired), approve → completed; the full happy-history assertions hold (one `step_completed` per step, no re-execution). |

`tests/common/mod.rs` (additive): `order_saga_wasm()` builds `examples/order-saga` once per process (`cargo build --release --target wasm32-wasip2 --target-dir target/order-saga`, inherited `CARGO_TARGET_DIR`/`RUSTFLAGS`/`RUSTC_WRAPPER` removed, the same target dir as the ci-local "order-saga example" step).

### Task 2: quickstart

- `examples/order-saga/quickstart.sh [<local repo path>]`: `git clone --no-hardlinks file://<path>` (URLs refused, exit 2) into `mktemp -d`, then the README steps as one script: `rustup target add wasm32-wasip2`, `cargo build --release --locked -p ostro`, PATH, `cargo build --release --target wasm32-wasip2` in the example, `ostro dev &` (readiness by polling `ostro ls`), `deploy --activate`, `run` (run id parsed from `started run <id>`), `signal approve --payload true`, `result --wait` (checked for `completed` and `ship-o-1`). Phase timestamps are printed.
  - docker usable (`docker info` succeeds): steps run in `docker run --rm -v <clone>:/src -w /src rust:1.100`; exactly one line `QUICKSTART_RESULT: within_budget wall=<N>s` (exit 0), `over_budget` (exit 1, N > 600) or `failed` (exit 1).
  - otherwise: the same steps locally on the fresh clone, cold target dir, labelled `local cold-clone equivalent (NOT the clean-machine gate)`, then `HUMAN_NEEDED: run with docker on a clean machine ...` (exit 0), or `QUICKSTART_RESULT: failed` (exit 1).
  - Env: `OSTRO_QUICKSTART_MODE`, `OSTRO_QUICKSTART_IMAGE`, `OSTRO_QUICKSTART_PORT`, `OSTRO_QUICKSTART_KEEP`.
- README: the quickstart now lists exactly the script's commands (build `ostro` from the repository root and put it on PATH, `ostro dev &`, human run output), explains `quickstart.sh`, and adds a Troubleshooting section (server unreachable / `OSTRO_SERVER`, port taken, missing wasm target, 404 workflow, run stays `scheduled`, 409 `run_terminal`, exit 3, pre-Phase-5 `.ostro` data, slow first build). The "commands are added in 05-14" note is gone.

### Task 3: docs

- `docs/workflow-semantics.md` (417 lines, 15 sections): start path and journal (D24), signals (FIFO per name, buffering incl. before the first activation, one signal for one wait, `request_id` dedupe, 409 on terminal runs, D21 history shape, blobs), wait timeouts (`wait_timed_out`, `e.is_timeout()`), gather/race (journal completion order, identical replay, cancellable losers, D5), cancel (cooperative, D11, `shielded`, Cancelled vs Completed, before start), terminate (hard stop, no guest code, children get graceful cancel), retry table (resumed / restarted / 409 `run_completed` / `run_not_terminal`, re-arm, no new run timeout), run timeout (per workflow metadata, created + timeout, fresh per continue-as-new, none on resume) and schedule-to-start (from `ready_at`, initial start only), await tokens (`ot1_` format, one-shot, 409s, never in URLs or logs), patched (pinning, memo, removal path, no re-pin before Phase 13 migrate-runs), continue-as-new (lineage, `next_run_id`, long-poll follow, 60 s cap), retention (request > workflow > queue > config, `168h`/`24h`, hours not days, `max_retention`, future runs only, key reuse, WAL bytes until Phase 8, blobs not GC'd, 05-09 gaps: guest timer rows and cron dedupe rows outlive expiry), eviction (W12: signal and token waits always evict), tenancy and names (D23 grammar, tenant-scoped keys, parse from the right, run-scoped request ids and tokens, `WorkflowVersion.module`, unqualified WIT names), and a worked example. Every section ends with **Proven by** citations; all 89 `file::test` references were checked to resolve to an existing `fn`.
- `docs/03-architecture.md`, structure kept:
  - §2.2: the real command table, tags 1-12 (incl. 8 SignalRun, 9 RequestCancel, 10 TerminateRun, 11 RetryRun, 12 CompleteToken, `StartRun.run_started`). No batch `RunsExpired` (per-run `EXPIRE_OP` timer); no `CancelRequested` command with a mode (RequestCancel + TerminateRun); completion is the `CompleteRun` intent; cron is the Phase 8 `CronStart` outbox message. Reserved timer ops paragraph. Fencing paragraph updated (attempt token, re-grant per epoch, Suspend CAS).
  - §2.3: Phase 5 kinds appended (`wait_timed_out`, `run_terminated`, `signal_payload`/`signal_matched`, `token_completed`/`cancel_requested`/`run_continued_as_new`).
  - §2.4: 16 keyspaces; `inbox` holds only unmatched signals (`SignalSlot`); `waits`, `signal_seen`, `running` rows; `dedupe` keyed by the qualified workflow and freed at expiry; "inputs are journaled by apply at `next_seq`" paragraph.
  - §3: the actual data-dir layout (checked against a live `ostro dev --data-dir`): `wal/p/<id>/wal_<id>_<seg>.log`, `state/pNNNN` (16 keyspaces), `blobs/<hh>/<sha>`, `cache/aot/`, `tmp/`, plus the retention/WAL/blob caveat and the unknown-entries rule.

## Results

### CLI paths (success criterion 1)

`cargo test -p ostro --test order_saga --locked`: **4 passed** in every run: 4 before 05-25 landed, 1 on the D24 start path at `5993b70`, 1 inside the gate, the gate's `cargo test` re-run, and 1 after `01bf244`; 5.7-7.3 s each. A manual CLI probe gave the history shape used in the assertions (`run_started`, 2 × `step_completed`, `timer_started`, `timer_fired`, `wait_resolved`, `signal_wait_started`, `signal_payload`, `signal_matched`, `wait_resolved`, 3 × `step_completed`, `run_completed`).

### Quickstart timing

Verify command: `bash -n examples/order-saga/quickstart.sh && bash -c 'set -o pipefail; timeout 1800 bash examples/order-saga/quickstart.sh 2>&1 | tee target/quickstart.log' && grep -Eq '^(QUICKSTART_RESULT: within_budget|HUMAN_NEEDED:)' target/quickstart.log` → **rc 0**.

- Mode: **local** (docker unusable: `/mnt/c/docker/resources/bin/docker` is a Docker Desktop shim, "could not be found in this WSL 2 distro").
- Fresh clone of `8d2c8c7`, cold target dir, release fat LTO:
  - `cargo build --release --locked -p ostro`: 3 m 54 s
  - order-saga example: 13.6 s
  - `ostro dev` ready → deploy → run → signal → result: about 3 s
  - **wall = 254 s** (budget 600 s), load average 3-10 (other executors building in parallel).
- Result line: `HUMAN_NEEDED: run with docker on a clean machine (examples/order-saga/quickstart.sh with a working docker; gate: QUICKSTART_RESULT: within_budget)`.
- Caveat: the local run reuses this machine's cargo registry cache and installed toolchain, so a clean machine also pays the crate downloads and the toolchain install (rust-toolchain.toml adds the musl and wasm32-wasip2 targets).

## Gate

`CARGO_TARGET_DIR=target/wt-05-17 bash scripts/ci-local.sh --no-musl` on `5993b70` (D24 / 05-25 included), in the detached scratch worktree, `set -o pipefail`:

```
toolchain 1.100.0                    PASS  0s
cargo fmt --check                    PASS  1s
cargo clippy -D warnings             PASS  28s
cargo build (warning-free)           PASS  62s
sdk workspace fmt + clippy           PASS  21s
sdk workspace test + wasm build      PASS  23s
guests build (wasm32-wasip2)         PASS  18s
order-saga example                   PASS  20s
cargo test                           FAIL  563s   (1 flake, see below; order_saga 4/4 inside it)
semantics sleepers (serial)          PASS  19s
failpoint suites                     PASS  1081s
cargo deny check                     PASS  3s
check-vendored.sh                    PASS  1s
check-deny-fixtures.sh               PASS  0s
no chronik/tokio-uring/prometheus    PASS  0s
workflow validation                  PASS  1s
musl legs                            SKIP  (--no-musl)
```

- **The one failure is not in this plan's files:** `crates/ostro-server/tests/lifecycle_api.rs::long_poll_no_lost_wakeup_when_completion_races_registration`, lane 5 iteration 8: the long poll answered `completed: true` with **`result: null`** (expected `{"lane":5,"i":8}`). This was under load average 11-13 from parallel executors. It did not reproduce afterwards: 6/6 runs of the single test, 15/15 runs of the whole `lifecycle_api` suite at load average 12, and a full re-run of the gate's `cargo test --workspace --locked` step, **rc 0 with 144 result blocks, all ok** (order_saga included). Owners: 05-13 (`await_result` / `RunWatchers`) and 05-25 (it changed `control/runs.rs`, `lifecycle.rs` and this test file at `4630500`/`f340835`). Reported, not fixed (scope boundary).
- After the gate, `01bf244` added one `run_cli` human-output assertion to `order_saga::happy_path`. Re-verified: `cargo test -p ostro --test order_saga --locked` 4 passed, clippy `-p ostro --all-targets -D warnings` clean, `cargo fmt --all --check` clean.

## Deviations from Plan

### Auto-fixed / resolved differently

**1. [Rule 1 - Bug] `command -v docker` is not a usable docker check.** On this host it finds a Docker Desktop shim that only prints an error. `quickstart.sh` requires `docker info` to succeed (30 s timeout) before choosing the container mode. Commit `157831a`.

**2. Signal history kinds.** The plan's behaviour list says "a SignalReceived". Since D21, Phase 5 journals `signal_payload` + `signal_matched` and never `signal_received`. The tests assert that shape and that `signal_received` is absent. Commit `8d2c8c7`.

**3. Extra compensated case.** The must_haves mention `fail_at charge`, the behaviour list `fail_at create_shipment`. Both are tested: `create_shipment` gives refund then release; `charge_payment` (3 attempts) gives only release, because the payment was never charged. Commit `8d2c8c7`.

**4. D24 landed mid-plan.** The first docs commit described D24 as the start path with a provisional "before 05-25" note. 05-25 committed while this plan ran, so `5993b70` replaces the note with the landed behaviour (incl. the outbox-start exception from the D24 text) and updates the renamed `lifecycle_api` test citation. `order_saga` was re-run on the D24 start path: green.

**5. [Rule 2 - key link] `run_cli` named in the suite.** The plan's key link greps `order_saga.rs` for `run_cli`. The suite used `cli_json`, which wraps it, so `01bf244` adds a real human-output assertion through `run_cli` (the README's `run <id> completed: ...` line) and names the helper in the module docs.

**6. Clone advice.** After the timed run, `git clone` got `-c advice.detachedHead=false` (cloning a detached worktree printed git's advice block). Output-only change; the timed steps are unchanged.

## Known limitations

- The clean-machine (< 10 min) figure needs docker: **human_needed**. Command: `examples/order-saga/quickstart.sh` on a machine with a working docker; pass = `QUICKSTART_RESULT: within_budget`.
- The main checkout's working tree is stale for paths committed through private indexes (this plan's six files and earlier plans'); this plan did not write it.

## Hand-off notes

### 05-20 (gate + freeze)

- **Flake to watch (not 05-17's code):** `lifecycle_api::long_poll_no_lost_wakeup_when_completion_races_registration` failed once under load with a long poll answering `completed: true, result: null`. That looks like a window in which the published terminal status is visible before the result payload is read. It did not reproduce in 22 later runs. If it shows up in the 05-20 gate, look at `await_result` (05-13) after the D24 start-path change (05-25).

- SC-1 evidence: `crates/ostro/tests/order_saga.rs` (4 tests, CLI only, incl. kill -9) runs inside `cargo test --workspace`; it builds the example into `target/order-saga` itself.
- Quickstart: local cold-clone equivalent 254 s; record the docker figure as human_needed in 05-VALIDATION (`QUICKSTART_RESULT:` line).
- `docs/workflow-semantics.md` cites tests by `file::test`. If 05-18/05-19/05-20 rename any cited test, re-run the citation check (a short script that resolves each `` `path::fn` `` to an existing `fn`; see this SUMMARY) or fix the doc.
- `docs/03-architecture.md` §2.2 lists commands 1-12 and §2.4 16 keyspaces. If the freeze adds anything, append there as well as in storage-format.md.
- The docs state D24 as landed. The format corpus must contain a `StartRun` with tag 23 (D24 / 05-16 hand-off).

### Phase 10 (CLI-05 build)

- `ostro deploy` derives the module name from the file stem, so the quickstart registers module `order_saga` (workflow `order-saga`). If `ostro build` changes the artefact name, update the README and `quickstart.sh`.

### Phase 14

- `docs/workflow-semantics.md` §14 is the user-facing D23 text (grammar, tenant-scoped keys, parse-from-the-right, run-scoped request ids/tokens). Token scopes (P14-F5/F6) are not documented yet.

## Self-Check

## Self-Check: PASSED

- FOUND on `main`: crates/ostro/tests/order_saga.rs (contains `run_cli`), crates/ostro/tests/common/mod.rs (`order_saga_wasm`), examples/order-saga/quickstart.sh (mode 100755), examples/order-saga/README.md, docs/workflow-semantics.md (420 lines, >= 120), docs/03-architecture.md (`EXPIRE_OP`, `TerminateRun`, no `RunsExpired{`).
- FOUND commits: 8d2c8c7, 157831a, de0bc8e, 5993b70, 01bf244.
- Task verifies: Task 1 green; Task 2 rc 0 (`HUMAN_NEEDED:` line); Task 3 grep checks `VERIFY3_OK`; 89 doc citations resolve.
