---
phase: 11-run-operations-embedded-ui
plan: 01
subsystem: planning / gate
tags: [gate, reservations, decisions, coordination]
requires:
  - Phase 7 complete (07-VERIFICATION human_needed, by-design items only)
  - Phase 8 complete (08-VERIFICATION passed)
  - Phase 5 freeze (wit/FROZEN.toml, tag wit/ostro-runtime-v0.1.0, FROZEN v0.1 storage format)
provides:
  - PHASE-11 GATE passed (2026-11-22)
  - RESERVATIONS section 11 D-P11-25 name rows
  - STATE D-P11-1..34, review/human todos, Phase 11 <-> 9/10/14 coordination, Phase 13 obligation
affects: [11-02, 11-03, 11-04, all later Phase 11 plans, Phase 13 (emission gate)]
tech-stack:
  added: []
  patterns: []
key-files:
  created:
    - .planning/phases/11-run-operations-embedded-ui/11-01-SUMMARY.md
  modified:
    - .planning/RESERVATIONS.md
    - .planning/STATE.md
decisions:
  - "D-P11-1..D-P11-34 recorded as adopted defaults; D-P11-3/8/13/14/17 USER REVIEW pending"
  - "Baseline cargo deny failure (RUSTSEC-2026-0592, url 2.5.8) is advisory-DB drift, not a gate precondition; already fixed on main by 7f40156 (url 2.5.9) before the 11-01 commits"
metrics:
  duration: "~2h25m (2h22m of it the baseline ci-local run)"
  completed: 2026-11-22
---

# Phase 11 Plan 01: PHASE-11 GATE Summary

The gate passed: Phases 7 and 8 are complete, the Phase 5 freeze (including PF-11-1/2) is in force, every Phase 7/8/5 seam Phase 11 builds on exists, and the code matches RESERVATIONS.md with the Phase 11 block empty. The D-P11-25 name rows are now in RESERVATIONS section 11, and STATE.md carries D-P11-1..34, the review and human todos, and the coordination rules. No code changed.

## Gate report

### Check 1: Phase 7 complete: PASS
- `07-01..07-22-SUMMARY.md` all present (23 SUMMARYs including 07-23).
- `07-VERIFICATION.md`: `status: human_needed`, `gaps: []`.
- Classification script (`11-01-classify.py`: every `human_verification` item is matched against a 07-VALIDATION "Manual-Only / Human-Needed" row):
```
status: human_needed; human items: 5; manual-only rows: 5
BY-DESIGN | Real KEDA ScaledObject drives a worker Deployment 0 -> N -> 0 (H1, F10) | Real KEDA ScaledObject drives a worker Deployment 0 → N → 0
BY-DESIGN | ACME against Let's Encrypt staging on a public host (H3) | ACME against a public CA (Let's Encrypt staging)
BY-DESIGN | ACME against Pebble in CI (H2) | ACME against Pebble (if Pebble did not build locally)
BY-DESIGN | Remote CI legs (H4): linux-aarch64-musl, macos-aarch64, linux-x86_64-gnu with PG/MySQL/SeaweedFS | Remote CI legs (aarch64-musl, macOS)
BY-DESIGN | User review: D-W27, D-W28 (+D-W11), W6 template rule (+D-W3), F4 attempt_failures, D-W8 server-profile TLS refusal | Review of D-W3 (effects on the leader), D-W8 (TLS auto default), D-W11 (QA-03 interpretation)
RESULT: all by-design
```
  Note: the user-review item names D-W27, D-W28 and F4 as well as the D-W3/D-W8/D-W11 that the VALIDATION row lists. These are decisions of the same class (YOLO decisions awaiting user review), and STATE Pending Todos already carries each one, so they count as by-design.

### Check 2: Phase 8 complete: PASS
- `08-01..08-22-SUMMARY.md` all present (23 SUMMARYs including 08-23).
- `08-VERIFICATION.md`: `status: passed` (the 10k campaign PASS and the quiet-machine restore_rpo are recorded in its Closure section). The remaining items it lists (SeaweedFS S3 legs, R2/AWS conditional PUT, fresh-machine restore) are environment-bound and do not block the gate.

### Check 3: Phase 5 freeze with PF-11-1/2: PASS
```
wit/FROZEN.toml                                  present
refs/tags/wit/ostro-runtime-v0.1.0               ddddddca5c4cf8823416aea9c9a4f99627dfdc89
grep -c "planned P5" RESERVATIONS.md             0
docs/storage-format.md:3   **Status: FROZEN v0.1 (2026-11-19, plan 05-20).** ...
runtime.wit:51             /// - Run operations (Phase 11): `limit_exceeded`, `payload_too_large`.
docs/storage-format.md:429 - **Derived keyspaces (Phase 11 PF-11-2).** A keyspace whose on-disk name starts with `x_` ... `x_run_index` is reserved (Phase 11).
PF-11-4 absent: grep rerun_from_seq crates/ostro-partition/src/store/values.rs -> no match (rc=1)
ostro-witcheck (in the baseline cargo test): semver_fixtures 4 passed, wit_freeze 4 passed
```

### Check 4: seams: PASS (one line each)
Phase 7:
```
command.rs:216-221   #[prost(message, tag = "20")] PauseRun(PauseRun) / #[prost(message, tag = "21")] ResumeRun(ResumeRun)
domain.rs:234 fn pause_run | :243 fn resume_run | :213 fn pause_queue | :218 fn resume_queue | :223 fn pause_workflow
domain.rs:228 fn resume_workflow | :253 fn list_workers | :257 fn drain_worker | :248 fn queue_demand
flow/limits.rs:1     //! Node-wide concurrency counters and the hand-rolled GCRA (plan 07-08, D-W13, D-W24).
ostro-worker-proto   proto/ostro/worker/v1/execute.proto:170 // postcard-encoded ostro_worker_proto::WireOutcome (D-W19).
store/values.rs:147  pub pause: Option<PauseMark>,
```
Phase 8:
```
ostro-api/src/lib.rs:20            pub use phase8::ApiExtensions;
services/mod.rs:48                 pub struct ServiceCtx {
control/mod.rs:504                 pub struct ControlCtx {   (:523 the one injectable clock)
store/snapshot.rs:1109             pub fn install_snapshot<R: Read>(
services/run_spans.rs:196          metrics::counter!("ostro_dlq_total", "reason" => reason).increment(1);
ostro-api/src/phase8.rs:750/756    pub schedules: Option<Arc<dyn ScheduleApi>>, / pub run_logs: Option<Arc<dyn RunLogsApi>>,
```
The `schedules` and `run_logs` slots are on `ApiExtensions`, not in `services/mod.rs`.

Phase 5:
```
derive.rs:467        pub fn rearm_unresolved(journal: &[Event]) -> Vec<Derived> {
command.rs           StartRun 21 retry_of, 23 run_started (field table in the reservations output below)
crates/ostro-engine/tests/replay_corpus.rs   present
```
Recorded only (none of these fails the gate):
- `ostro_engine::replay_check` does **not** exist yet. Whichever of 11-10 and 10-11 lands first creates it.
- Phase 10 10-09 has **not** landed: `crates/ostro-cli/src/client.rs` is still the full typed client, not the `pub use ostro_client::*;` shim. No Phase 10 SUMMARY exists.
- `ostro-server` is in `FAILPOINT_CRATES` as `ostro-server:failpoints_phase5`, so 11-04 does not add it (D-P11-26).
- Phase 9 SUMMARYs present: 09-01..09-16 at the time of the commit. 09-17 is landing (its feat commits are on main), and 09-18..09-20 remain. Phase 12's gate 12-01 landed in parallel; its RESERVATIONS rows (P12 MetaCommand 4-6, ext tag 80, P12 names) do not collide with any Phase 11 name.

### Check 5: working tree: PASS
The worktree, detached at main, has 0 dirty paths under `crates guests scripts .github docs wit Cargo.toml Cargo.lock deny.toml VENDORED.md`. No contention for 11-02/11-03/11-04.

### Check 6: baseline `bash scripts/ci-local.sh --no-musl`
Run at HEAD **237dbe1** from 08:39:39Z to 11:01:20Z (2h22m), `CARGO_TARGET_DIR=target/slot-2`, `CARGO_BUILD_JOBS=4`, `nice -n 10`, with other agents' builds running on the machine.

| Step | Result | Time |
|------|--------|------|
| toolchain 1.100.0 | PASS | 0 s |
| cargo fmt --check | PASS | 2 s |
| cargo clippy -D warnings | PASS | 41 s |
| cargo build (warning-free) | PASS | 53 s |
| sdk workspace fmt + clippy | PASS | 26 s |
| sdk workspace test + wasm build | PASS | 29 s |
| guests build (wasm32-wasip2) | PASS | 21 s |
| order-saga example | PASS | 24 s |
| external: PG / MySQL / S3 | SKIP x3 | - (no `.ostro-test-env` in the worktree) |
| cargo test | PASS | 3834 s (418 `test result: ok`, 0 FAILED) |
| semantics sleepers (serial) | PASS | 30 s |
| failpoint list sync (ci.yml) | PASS | 0 s |
| failpoint suites | PASS | 3778 s |
| server campaign smoke (20 kills) | PASS | 596 s |
| phase 7 failpoint legs + manifests | PASS | 55 s |
| cargo deny check | **FAIL** | 2 s |
| check-vendored.sh | PASS | 2 s |
| check-deny-fixtures.sh | PASS | 1 s |
| no chronik/tokio-uring/prometheus | PASS | 0 s |
| workflow validation | PASS | 7 s |
| musl legs | SKIP (--no-musl) | - |

`cargo deny check` failed in advisories only: `RUSTSEC-2026-0592` (stack overflow in `Url::origin()` on nested `blob:` URLs), `url 2.5.8`, reached through hickory-net/hickory-proto/ostro-bridge. Bans, licenses and sources were ok. The advisory database gained a new entry; no code regressed. I checked locally that `cargo update -p url` (url 2.5.9, form_urlencoded 1.2.3) gives `advisories ok`, then put the lockfile back, because 11-01 may not edit Cargo.lock. While the baseline was running, main had already gained **7f40156 `build(deps): url 2.5.9 for RUSTSEC-2026-0592 (ci-fix3)`**. At 7f40156 the full `cargo deny check` reports `advisories ok, bans ok, licenses ok, sources ok`. The baseline is therefore green apart from this external advisory, which main fixed before the 11-01 commits. Between 237dbe1 and 7f40156, main received only test fixes (ci-fix3), Phase 9 conformance work, the 12-01 docs and the url bump; the full baseline was not re-run.

### Check 7: reservations check: PASS
`11-01-rescheck.py` is modelled on 09-01. It extracts Cmd tags, StartRun and JournalEvent fields, Derived intents, outbox kinds, TerminalStatus, EventKind, TABLES order and names, RejectReason, InvalidCode, MetaRecord/MetaKind/MetaCommand, META_FORMAT and registered ext tags, and asserts that the Phase 11 block is empty. Output at 237dbe1, re-run with the same result at 7f40156:
```
OK   Cmd 1..13, 20 PauseRun, 21 ResumeRun, 27 ScheduleFire (16 tags, each == registry)
     Cmd tags in code: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 27, 13, 20, 21]
OK   StartRun fields 1..=23 exist, max 23   (19 schedule_to_start_ms, 20 retention_ms, 21 retry_of, 22 continued_from, 23 run_started)
OK   JournalEvent fields 1..=3 only (4 reserved P11)   [(1,'seq'),(2,'kind'),(3,'payload')]
OK   Intent 1..10, 19 ChildJournal
OK   Outbox kind 1..6
OK   TerminalStatus [Completed, Failed, Cancelled, Terminated, TimedOut, ContinuedAsNew] = 0..5
OK   EventKind codes [1, 16..29, 64..71, 96, 97] (outside exists: [])
OK   TABLES order/names [meta, runs, events, payloads, ready, timers, inbox, dedupe, outbox, inbound_seen, children, dlq, effects, running, waits, signal_seen, schedule_state]
OK   no x_* keyspace in TABLES
OK   RejectReason 0..11, 12-14 ReservedP6_12..14, 15 RunPaused
OK   InvalidCode 0..12, 13-14 ReservedP5, 15 BadEffectCompletion, 16-17 ReservedP6, 18-21 ReservedP7, 22 BadScheduleFire, 23 ScheduleOwnerMismatch
OK   MetaRecord/MetaKind 0..=16 (len 17/17)
OK   MetaCommand ['InitCluster', 'Put', 'Delete', 'SetPartitionLeader']
OK   META_FORMAT = 2
OK   ext tag 15 EXT_TAG_TEST (crates/ostro-meta/src/ext.rs) (registry: 15 = golden/test only)
OK   Phase 11 block empty in code (found: [])   [Cmd 45-49, StartRun 40-43, JournalEvent.4, InvalidCode 29-31, ext 144-159, x_run_index/DlqUpdate/BadRerunPrefix/BadDlqUpdate/rerun_prefix/rerun_from_seq identifiers]
RESERVATIONS CHECK: PASS
```
The new D-P11-25 names (`bulk::`/`rerun::`/`dlq::`, `[limits]`/`[ui]`/`[bulk]`/`[dlq]`, `blk_`, `ostro_csrf`, `OSTRO_UI_ALLOWED_HOSTS`, `on_failure_of`, `run_limits`, `ostro_{bulk,rerun,ui}_`) appear nowhere in `crates/`.

### Check 8: RESERVATIONS.md: done (a44959d)
I appended 11 rows to section 11, owner `P11 (11-01)`: config sections, env var, metric prefixes (with the shared `ostro_dlq_` series), failpoint prefixes, `blk_`, `__ostro/on_failure_of=<run>` (placed after the `__ostro/rerun_from=<seq>` row), guest names, `ostro_csrf`/`X-Ostro-Csrf`, the `details.reason` vocabulary, the `run_limits` engine module, and the failure-policy/rerun-cuts REST paths (D-P11-29). The P11 ordinal rows are unchanged and stay `reserved P11` until 11-18.

## Task commits
| Task | Commit | Files |
|------|--------|-------|
| 1 Gate + name rows | a44959d | .planning/RESERVATIONS.md |
| 2 Decisions, todos, coordination | 39de18e | .planning/STATE.md |

## Deviations from Plan
- **Worktree and target paths:** the work ran in the coordinator-assigned scratchpad worktree and build slot `target/slot-2`, not in `/tmp/ostro-wt-11-01` and `/tmp/ostro-target-11-01` (executor brief override). Commits went through the worktree's own index plus a CAS `update-ref` on main.
- **Baseline cargo deny failure** (external advisory RUSTSEC-2026-0592). It is recorded rather than fixed here, because Cargo.lock belongs to 11-03, and main had already fixed it in 7f40156. It is not a gate precondition (checks 1-4 are the STOP conditions).
- **Rebase:** main moved from 237dbe1 to 7f40156 during the baseline run (Phase 9, 12-01, ci-fix3). The RESERVATIONS edit was re-applied cleanly on 7f40156, and the reservations check and seam check were re-run there. The STATE edit was built from the main blob at a44959d.

## Open issues
- None blocking. Wave 2 (11-02, 11-03, 11-04) may start under the mutual-exclusion rules: 11-03 and 11-04 never run concurrently with 10-02.
- External PG/MySQL/S3 legs were SKIPped in the baseline (no `.ostro-test-env` in the scratch worktree).

## Self-Check: PASSED
- FOUND: .planning/RESERVATIONS.md rows (`__ostro/on_failure_of=`, `dlq::`, `ostro_csrf`)
- FOUND: .planning/STATE.md (Task 2 verify grep set green)
- FOUND: a44959d, 39de18e on main
