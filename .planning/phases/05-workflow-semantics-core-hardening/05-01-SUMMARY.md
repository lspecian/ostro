---
phase: 05-workflow-semantics-core-hardening
plan: 01
subsystem: contract
tags: [gate, ostro-types, prost, journal, golden, reservations, run-status, openapi]

requires:
  - phase: 04-single-node-server-rust-mvp
    provides: "Phase 4 complete (04-VERIFICATION passed 5/5), StartRun 14..18, Intent::Suspend, Running keyspace, InvalidCode 11/12"
  - phase: 02-durable-partition-log-state-machine
    provides: "02-16 committed; frozen Cmd tags 1..7, golden command/journal files"
provides:
  - "Phase 5 gate report (Phase 4 complete, code ordinals == RESERVATIONS.md) and the Phase 5 baseline commit"
  - "STATE.md: D1-D22 adopted decisions, final contract tag table, USER REVIEW / HUMAN / TODO items"
  - "ostro-types Phase 5 format contract: Cmd 8..12, StartRun 19..23, Intent RegisterWait(7)/CancelWait(8), CompleteRun.resumable(4)/parent_event(5), ChildCompleted.event(6), AttemptGranted.after_failure(3), TerminalStatus 3..5, outbox Kind::StartRun(6), journal kinds 70/71/96/97, reserved timer ops"
  - "Run status parity (partition, domain, REST, proto) for Terminated / TimedOut / ContinuedAsNew"
  - "Temporary BadIntent reject arms in apply for every not-yet-implemented item (owners listed below)"
affects: [05-02, 05-03, 05-04, 05-05, 05-06, 05-07, 05-08, 05-09, 05-11, 05-13, 05-20, phase-6, phase-7, phase-8, phase-9]

tech-stack:
  added: []
  patterns:
    - "Contract-first gate: every ordinal lands in ostro-types in one plan, goldens appended, dependents get temporary reject arms tagged `// Phase 5: implemented in 05-NN`"
    - "RESERVATIONS.md block comments at each oneof/enum (`// RESERVATIONS.md: reserved P6 = ...`)"
    - "Per-task commits for shared files built from a private index with intermediate blobs (git hash-object + update-index --cacheinfo)"

key-files:
  created:
    - crates/ostro-partition/tests/apply_contract_p5.rs
    - .planning/phases/05-workflow-semantics-core-hardening/05-01-SUMMARY.md
  modified:
    - .planning/STATE.md
    - crates/ostro-types/src/command.rs
    - crates/ostro-types/src/journal.rs
    - crates/ostro-types/tests/command.rs
    - crates/ostro-types/tests/journal_golden.rs
    - crates/ostro-types/tests/golden/commands.txt
    - crates/ostro-types/tests/golden/journal_v1.txt
    - crates/ostro-partition/src/apply/{mod,run,inbound,outbox}.rs
    - crates/ostro-partition/src/store/values.rs
    - crates/ostro-engine/src/host/effects.rs
    - crates/ostro-server/src/control/{runs,history}.rs
    - crates/ostro-proto/proto/ostro/api/v1/common.proto
    - crates/ostro-api/src/{domain,rest,grpc}.rs
    - crates/ostro-api/tests/parity.rs
    - crates/ostro-api/tests/snapshots/openapi__openapi_document.snap

key-decisions:
  - "Phase 5 decisions D1-D22 adopted as defaults (YOLO/auto_advance) and recorded in STATE.md with USER REVIEW todos for D7, D8, D14 and the WIT freeze flip"
  - "Gate passed: 04-VERIFICATION status passed (no human items), 02-16 committed, code tree clean; every code ordinal matches RESERVATIONS.md, so no docs-only correction was needed"
  - "Temporary arms reject with the existing InvalidCode::BadIntent; InvalidCode::NotImplemented already exists at index 6 (RESERVATIONS §8), so no new code was appended"
  - "START_DELAY_OP moved into ostro_types::command next to RUN_TIMEOUT_OP/SCHEDULE_TO_START_OP/EXPIRE_OP; ostro_partition::apply re-exports it (same value, same path)"
  - "Guest ScheduleTimer/CancelTimer in RESERVED_OP_MIN..=u64::MAX is now rejected (real rule, was START_DELAY_OP only)"
  - "Executor SendMessage(Kind::StartRun) rejected BadIntent until 05-09 (continue-as-new successor start is apply/derive-owned)"
  - "Inbound Kind::StartRun is consumed with no effect (deterministic drop, batch stays Accepted) until 05-09"
  - "CompleteRun.parent_event is accepted but NOT yet forwarded into ChildCompleted.event; 05-02 owns forwarding plus its oversize check"

patterns-established:
  - "Phase-5 tripwire tests (apply_contract_p5.rs): each `*_until_implemented` assertion is replaced by the owning plan"

requirements-completed: []

duration: ~55min
completed: 2026-11-19
---

# Phase 5 Plan 01: Phase 5 Gate and Contract Summary

**The Phase 5 gate passed against Phase 4 (verified 5/5) with every code ordinal matching RESERVATIONS.md. The full Phase 5 wire contract now lives in ostro-types: Cmd 8..12, StartRun 19..23, wait intents, statuses 3..5, outbox StartRun and journal kinds 70/71/96/97. Golden files are append-only and EVENT_CODEC_VER is unchanged. The three new run statuses have parity across partition, domain, REST and gRPC.**

## Performance

- **Duration:** about 55 min, including two full `ci-local --no-musl` runs of about 14 min each (the first overlapped the Task 2 drafting).
- **Started:** about 2026-11-19T05:18Z.
- **Completed:** 2026-11-19.
- **Tasks:** 3/3.
- **Files modified:** 1 docs file plus 37 code files over 2 code commits. One code file is new: `tests/apply_contract_p5.rs`.

## Gate report (Task 1)

| # | Check | Result |
|---|-------|--------|
| 1 | `04-{01..20}-SUMMARY.md` present | **20/20** |
| 2 | `04-VERIFICATION.md` status | `status: passed`, score 5/5. Classification script output: `status=passed; human items: 0 (by-design: 0, other: 0)`. The "Human Verification" section reads "None required for sign-off"; it mentions only the project-wide FOUND-05 remote CI legs, which are by-design category (a). |
| 3 | `02-16-SUMMARY.md` present | yes |
| 4 | `git status --porcelain crates vendor guests sdks scripts .github docs Cargo.toml Cargo.lock` | empty. Uncommitted `.planning/phases/09-*` edits from the planning agent were ignored by design and never staged. |
| 5 | Baseline `bash scripts/ci-local.sh --no-musl` (`CARGO_TARGET_DIR=target/wt-05-01`) | **all steps passed** at **`d79ae42`**, the Phase 5 baseline commit (no code change since the 04-20 full gate at `b0b4e58`) |
| 6 | Code vs `.planning/RESERVATIONS.md` (script output below) | **all code ordinals match**, so RESERVATIONS.md was not changed |

Baseline step timings (d79ae42):

```
toolchain 1.100.0                    PASS  0s
cargo fmt --check                    PASS  0s
cargo clippy -D warnings             PASS  32s
cargo build (warning-free)           PASS  53s
sdk workspace fmt + clippy           PASS  18s
sdk workspace test + wasm build      PASS  21s
guests build (wasm32-wasip2)         PASS  0s
cargo test                           PASS  412s
failpoint suites                     PASS  362s
cargo deny check                     PASS  1s
check-vendored.sh                    PASS  0s
check-deny-fixtures.sh               PASS  1s
no chronik/tokio-uring/prometheus    PASS  0s
workflow validation                  PASS  0s
zig/musl legs                        SKIP  (--no-musl)
```

The reservations check script lives in the session scratchpad as `rescheck.py`. It extracts `Cmd`, `StartRun`, `Intent` and outbox `Kind` tags, `TerminalStatus`, `EventKind`, `EventBody` order, `TABLES` names, `RejectReason::index()`, `InvalidCode` order, `MetaKind`/`MetaRecord` order and the reserved-op constants, then compares each against the RESERVATIONS rows. Before the code changes, condensed:

```
Cmd 1..7 OK | StartRun 1..18 OK | Intent 1..6 OK (Suspend=6) | outbox Kind 1..5 OK | TerminalStatus 0..2 OK
CompleteRun 1..3 OK | ChildCompleted 1..5 OK | AttemptGranted 1..2 OK | RegisterWait: not in code yet
EventKind 1,16..29,64..69 OK | TABLES 0..13 OK (running=13) | RejectReason 0..10 OK
InvalidCode 0..12 OK (BadLength=11, NotRunning=12) | MetaKind/MetaRecord 0..13 OK
const START_DELAY_OP: crates/ostro-partition/src/apply/mod.rs; RUN_TIMEOUT_OP/SCHEDULE_TO_START_OP/EXPIRE_OP/RESERVED_OP_MIN: absent
RESULT: all code ordinals match RESERVATIONS.md
```

After this plan, the same script reports `Cmd 8..12`, `StartRun 19..23`, `Intent 7..8`, `Kind 6`, `TerminalStatus 3..5`, `CompleteRun 4/5`, `ChildCompleted 6`, `AttemptGranted 3`, `RegisterWait 1..5` and `EventKind 70/71/96/97`, with all reserved-op constants in `crates/ostro-types/src/command.rs`. RESULT: all code ordinals match RESERVATIONS.md. The "per 04-12 plan" rows (Running = 13, Suspend = 6, BadLength = 11, NotRunning = 12) are confirmed `exists`.

STATE.md changes (commit `671b070`):
- D1..D22 are recorded under `[Phase 05 decisions (adopted defaults; user may override)]`, with a final contract tag table bullet.
- Six Pending Todos were added: four USER REVIEW items (D7, D8, D14 and the WIT freeze flip), one HUMAN item (NVMe perf and remote CI legs) and one TODO (Wasmtime 51 after 05-20).
- The Phase 4 "unsupported" decision is marked SUPERSEDED for await tokens and continue-as-new. The original text is kept.
- The P=16 partition-count blocker is marked resolved pending review (D8).

## Final contract tag table

| Space | Allocation (all from RESERVATIONS.md) |
|-------|---------------------------------------|
| `Cmd` | SignalRun = 8, RequestCancel = 9, TerminateRun = 10, RetryRun = 11, CompleteToken = 12 |
| `StartRun` | 19 schedule_to_start_ms, 20 retention_ms, 21 retry_of, 22 continued_from, 23 run_started (Option<JournalEvent>) |
| `derived::Intent` | RegisterWait = 7 `{op 1, kind WaitKind 2 (Signal = 0, Token = 1), name 3, timeout_due_ms 4, at_ms 5}`, CancelWait = 8 `{op 1}` |
| Other fields | CompleteRun.resumable = 4, CompleteRun.parent_event = 5, outbox ChildCompleted.event = 6, AttemptGranted.after_failure = 3 |
| `TerminalStatus` | Terminated = 3, TimedOut = 4, ContinuedAsNew = 5 |
| outbox `Kind` | StartRun = 6 (carries `StartRun` directly) |
| Journal `EventKind` | WaitTimedOut = 70 `{op}`, RunTerminated = 71 `{reason}`, SignalPayload = 96 `{name, payload}`, SignalMatched = 97 `{op, payload_seq}`. These are the last four `EventBody` variants, in that order. |
| Reserved timer ops | `RESERVED_OP_MIN = u64::MAX-15`, `START_DELAY_OP = u64::MAX`, `RUN_TIMEOUT_OP = MAX-1`, `SCHEDULE_TO_START_OP = MAX-2`, `EXPIRE_OP = MAX-3`, `is_reserved_op(op)` |
| Length limits | `MAX_SIGNAL_NAME_LEN = 256`, `MAX_REASON_LEN = 1024`, `MAX_REQUEST_ID_LEN = 64`. `check_limits()` on SignalRun, RequestCancel, TerminateRun and RegisterWait returns `FieldTooLong{field, max}`. |
| Partition `RunStatus` | Terminated, TimedOut and ContinuedAsNew appended (postcard indices 6, 7, 8), all terminal |
| Proto `RunStatus` (common.proto) | TERMINATED = 7, TIMED_OUT = 8, CONTINUED_AS_NEW = 9 |
| REST / domain | `terminated`, `timed_out`, `continued_as_new` |
| History API kind names | `wait_timed_out`, `run_terminated`, `signal_payload`, `signal_matched` |

Goldens:
- `commands.txt` gains 12 appended vectors: signal_run, request_cancel, terminate_run, retry_run, complete_token, start_run_v5, events_appended_waits, complete_run_terminated_resumable, complete_run_timed_out, complete_run_continued_as_new, attempt_granted_after_failure and inbound_batch_v5 (outbox StartRun + ChildCompleted.event).
- `journal_v1.txt` gains 4 appended event lines for kinds 70, 71, 96 and 97.
- `git diff d79ae42 HEAD -- crates/ostro-types/tests/golden/ | grep -c '^-[^-]'` returns **0**.
- `EVENT_CODEC_VER` is still 1.

## Temporary reject arms and their owners

| Location | Arm | Owner |
|----------|-----|-------|
| `apply/mod.rs` dispatch | `Cmd::SignalRun`, `RequestCancel`, `CompleteToken` → `Rejected(Invalid(BadIntent))` | 05-08 |
| `apply/mod.rs` dispatch | `Cmd::TerminateRun`, `RetryRun` → BadIntent | 05-09 |
| `apply/run.rs` check_intents | `Intent::RegisterWait`, `CancelWait` → BadIntent (the write loop has a no-op arm) | 05-08 |
| `apply/run.rs` check_intents | `CompleteRun{Terminated \| TimedOut}` → BadIntent. **This is permanent for executors**: these statuses are set by apply-side paths only. | 05-09 (apply paths) |
| `apply/run.rs` check_intents | `CompleteRun{ContinuedAsNew}` → BadIntent | 05-09 |
| `apply/run.rs` check_intents | `SendMessage(Kind::StartRun)` → BadIntent | 05-09 |
| `apply/run.rs` CompleteRun write | `ChildCompleted.event: None` (parent_event not forwarded yet) | 05-02 |
| `apply/inbound.rs` apply_msg | inbound `Kind::StartRun` consumed with no effect | 05-09 (`lifecycle::inbound_start_run`; 05-02 stubs the seam) |
| `ostro-engine host/effects.rs` | `SignalMatched` completion → bad-journal violation | 05-03 |
| Real rules (keep) | Guest ScheduleTimer/CancelTimer with `is_reserved_op(op)` → BadIntent. Engine `WaitTimedOut` → `Err(Failure::Timeout)` (05-03 owns its tests). `outbox.rs` oversize check for `Kind::StartRun` inspects `input`. `target_partition` routes `Kind::StartRun` by the new run id. | — |

`crates/ostro-partition/tests/apply_contract_p5.rs` pins each arm. The tests are `phase5_commands_are_rejected_until_implemented`, `phase5_intents_are_rejected_until_implemented` and `inbound_start_run_is_consumed_without_effect_until_implemented`. The owning plan replaces the matching assertion.

## Task Commits

1. **Task 1: gate + decisions in STATE.md**: `671b070` (docs)
2. **Task 2: ostro-types Phase 5 contract + appended goldens + temporary arms**: `b131114` (feat). Built from a private index, and its tree was `cargo check --all-targets` clean with `-D warnings` on its own.
3. **Task 3: run status parity**: `204247f` (feat)

TDD: the new command vectors failed `golden_encodings` (RED) until blessed with `OSTRO_BLESS=1`. Re-encoding the old StartRun, start_run_v4, AttemptGranted, EventsAppended and InboundBatch lines is byte-identical.

## Files Created/Modified

- `crates/ostro-types/src/command.rs`: new Cmd variants and messages, StartRun 19..23, intents, WaitKind, field additions, TerminalStatus 3..5, outbox Kind::StartRun, reserved ops, limits, run_id/check_limits helpers, constructors, `kind_name`.
- `crates/ostro-types/src/journal.rs`: kinds 70/71/96/97, payload structs, appended EventBody variants, `completes_op` (Some for 70 and 97), `summary`, codec arms, docs (SignalReceived is never produced from Phase 5 on).
- `crates/ostro-types/tests/{command,journal_golden}.rs` and goldens: appended fixtures, first-principles byte tests, old-bytes decode, limits, routing, proptest arms.
- `crates/ostro-partition/src/apply/*`, `store/values.rs`: temporary arms, the reserved-op rule, RunStatus parity.
- `crates/ostro-partition/tests/apply_contract_p5.rs` (new): reserved-op rule, tripwires, RunStatus postcard indices.
- `crates/ostro-engine/src/host/effects.rs`: WaitTimedOut and SignalMatched arms.
- `crates/ostro-server/src/control/{runs,history}.rs`: status mapping, StartRun Phase 5 fields (0/empty), history kind names and JSON.
- `crates/ostro-proto/.../common.proto`, `crates/ostro-api/src/{domain,rest,grpc}.rs`, `tests/parity.rs`, OpenAPI snapshot: status parity.

## Decisions Made

See `key-decisions` in the frontmatter. The most important one for later plans: **`InvalidCode::NotImplemented` already exists** at index 6. 05-DECISIONS.md "Apply split" says 05-02 "appends ... InvalidCode::NotImplemented"; 05-02 must not add a duplicate. Only `RejectReason::RunCompleted` (index 11) is new.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Files outside the plan list needed edits to compile**
- **Found during:** Task 2 build.
- **Issue:** The new prost fields (`CompleteRun.resumable/parent_event`, `AttemptGranted.after_failure`, `ChildCompleted.event`, StartRun 19..23) break struct literals that don't use `..Default::default()`. New EventKind/EventBody variants break the exhaustive matches in `control/history.rs`.
- **Fix:** Field-only additions (`false`/`None`/empty) in `ostro-partition/src/handle.rs`, `ostro-server/src/{executor,sink}.rs`, `ostro-crashtest/{src/bin/crash-target.rs,tests/apply_throughput.rs}` and the partition tests `ack_after_apply`, `apply_core`, `apply_cross`, `apply_phase4`, `apply_timer_input`, `rebuild_basic`, `rebuild_digest` and `shuffler`. History arms (`wait_timed_out`, `run_terminated`, `signal_payload`, `signal_matched`) were added to `ostro-server/src/control/history.rs`.
- **Commit:** `b131114`.

**2. [Rule 2 - Missing critical] Executor `SendMessage(Kind::StartRun)` rejected**
- **Issue:** Without a check, an executor could enqueue a continue-as-new successor start before 05-09 implements the paired CompleteRun{ContinuedAsNew} semantics.
- **Fix:** check_intents rejects it with BadIntent (tagged for 05-09) and a tripwire test covers it.
- **Commit:** `b131114` (arm) and `204247f` (test).

**3. [Scope note] Plan-listed files left unchanged**
- `crates/ostro-engine/src/replay/cursor.rs`: no compile impact, since `summary()` lives in ostro-types. `deliver_inputs` still rejects a delivered RunTerminated (71) or SignalPayload (96) as "not an input". 05-03 owns that change (see hand-off).
- `crates/ostro-server/src/derive.rs`: only struct-literal fields. Its catch-all `_ =>` arms already ignore the new apply-only kinds.
- `crates/ostro-server/src/control/list.rs`: reuses `runs::status()`, so it needs no change.
- `crates/ostro-api/src/fake.rs`: `set_run_outcome` already accepts any status.
- Proto: the status enum is in `common.proto`, not `runs.proto`.
- RESERVATIONS.md: no discrepancy, so it was not changed. The Phase 5 rows still read `planned P5 (05-01)`, which rule 4 allows.

**4. [Process] Baseline isolation**
- Task 2 edits were drafted in a temporary git worktree while the baseline ci-local ran, so the baseline measured `d79ae42` exactly. The worktree was removed after the diff was applied to the main tree.

---

**Total deviations:** 2 auto-fixed (1 blocking, 1 missing-critical), plus scope notes.
**Impact on plan:** None on the contract. The extra edits are field-only or arm-only.

## Final gate

`CARGO_TARGET_DIR=target/wt-05-01 bash scripts/ci-local.sh --no-musl` at code HEAD **`204247f`**: **all steps passed** (EXIT=0).

```
toolchain 1.100.0                    PASS  0s
cargo fmt --check                    PASS  0s
cargo clippy -D warnings             PASS  6s
cargo build (warning-free)           PASS  12s
sdk workspace fmt + clippy           PASS  0s
sdk workspace test + wasm build      PASS  1s
guests build (wasm32-wasip2)         PASS  0s
cargo test                           PASS  397s
failpoint suites                     PASS  438s
cargo deny check                     PASS  1s
check-vendored.sh                    PASS  0s
check-deny-fixtures.sh               PASS  1s
no chronik/tokio-uring/prometheus    PASS  0s
workflow validation                  PASS  0s
zig/musl legs                        SKIP  (--no-musl)
```

- The known flake did not occur: `dev_startup::warm_restart_under_1s` passed inside the full run.
- `FAILPOINT_CRATES` is unchanged and still matches ci.yml.
- No new dependencies: no chrono, bincode or wasmtime `cache`. Cargo.lock is unchanged.
- The per-plan verifies passed: `cargo test -p ostro-types` (7 + 30 + 11 + 17 + 10 tests), `cargo test -p ostro-api -p ostro-partition` (incl. the new `apply_contract_p5`, 5 tests), and `cargo clippy --workspace --all-targets --locked -- -D warnings`.
- Golden removed-line check: 0.

## Hand-off to wave 2 (05-02 .. 05-07, parallel)

- **05-02 (partition store/apply seams):**
  - Forward `CompleteRun.parent_event` into `ChildCompleted.event` in `apply/run.rs`. The literal currently sets `event: None` with a comment. Add the oversize check in `any_oversize`.
  - **Do not append `InvalidCode::NotImplemented`**: it exists at index 6.
  - `RunStatus` already has the three Phase 5 variants (values.rs). Append RunRecord fields after them, but do not touch the enum.
  - Inbound `Kind::StartRun` is a no-op arm in `apply/inbound.rs::apply_msg`. Route it to your `inbound_start_run` stub.
  - `apply_contract_p5.rs` is a new test file you may extend.
- **05-03 (engine):**
  - `host/effects.rs` already maps `WaitTimedOut` → `Err(Failure::Timeout)`. Add the tests.
  - Replace the `SignalMatched` violation arm with the payload_seq lookup.
  - `replay/cursor.rs::deliver_inputs` rejects 71/96 (completes_op = None): decide how to skip them.
  - `EventBody::completes_op` returns Some for 70 and 97.
- **05-04 (API/proto):**
  - Status parity is done. `common.proto` RunStatus 7..9, domain `RunStatus::ALL` (9 entries), the REST DTO, gRPC both ways, `parity.rs` helpers and the OpenAPI snapshot already include the three statuses.
  - Re-bless the snapshot only for your own additions.
- **05-05 (WIT/witcheck, `golden_frozen.rs`):**
  - Golden prefix baselines are now `commands.txt` with 21 vectors and `journal_v1.txt` with 25 event lines. The last of those is `event 97 signal_matched`.
- **05-06 (SDK):** no contract dependency beyond the journal kinds.
- **05-07 (meta):**
  - Untouched by 05-01. MetaKind/MetaRecord 0..13 are verified.
  - D23 and the Phase 14 pre-freeze findings (F1–F12, ext TLV §10b) were added to 05-DECISIONS.md and RESERVATIONS.md while 05-01 was executing. Those edits are uncommitted and belong to another agent. STATE.md carries D1–D22 only; per 05-DECISIONS, 05-07 Task 2 copies D23/F* into STATE.md.
  - No 05-01 ostro-types ordinal is affected.
- **Shared-file risk:**
  - 05-02 and 05-04 both list nothing in common with each other, and 05-03 is disjoint too.
  - 05-01 already edited `apply/run.rs`, `apply/inbound.rs`, `apply/mod.rs` and `store/values.rs` (05-02's files) and `host/effects.rs` (05-03's). These edits are committed, so start from HEAD.
  - `crates/ostro-partition/tests/rebuild_digest.rs` is listed by 05-02; 05-01 only added struct fields there.
  - `crates/ostro-api/tests/snapshots/openapi__openapi_document.snap` belongs only to 05-04.
  - Risk is low as long as each plan starts from `204247f` or later.

## Self-Check: PASSED

- All six files were FOUND: `apply_contract_p5.rs`, ostro-types `command.rs`, `journal.rs`, both golden files, and this SUMMARY.
- All three commits were FOUND: `671b070`, `b131114`, `204247f`.
- Each artifact contains its check string: `RUN_TIMEOUT_OP` in command.rs, `WaitTimedOut` in journal.rs, the "Phase 05 decisions (adopted defaults; user may override)" block in STATE.md, and `Terminated` in `control/runs.rs`.
