---
phase: 07-workers-placement-flow-control
plan: 01
subsystem: planning-gate
tags: [gate, reservations, decisions, coordination]
requires:
  - phase: 06-effects-bridge-connectors-secrets
    provides: "20/20 SUMMARYs, 06-VERIFICATION human_needed (by-design items only), ServerEffectDriver/EffectHooks, MockProvider charges/requests, canary harness, Cmd 13"
  - phase: 05-workflow-semantics-core-hardening
    provides: "pre-freeze findings F1-F4, F7, F8"
provides:
  - "Phase 7 gate report (Phase 6 complete, F1-F4/F7/F8 in code, code == RESERVATIONS.md)"
  - "RESERVATIONS.md Phase 7 rows: Cmd 20 PauseRun / 21 ResumeRun, RejectReason 15 RunPaused planned P7 (07-03); keyspaces 17/18 and meta 19/20 unused; section 11 Phase 7 names"
  - "STATE.md: Phase 07 decisions D-W1..D-W26, [USER REVIEW] D-W3/D-W8/D-W11, [HUMAN] KEDA + ACME, Phase 7 <-> Phase 8 coordination"
affects: [07-02, 07-03, 07-04, 07-05, 07-08, 07-11, 07-12, 07-15, 07-16, 07-22, 08-*]
tech-stack:
  added: []
  patterns: ["gate scripts in the scratchpad: 07-01-gate.sh, 07-01-classify.py, 07-01-rescheck.py"]
key-files:
  created:
    - .planning/phases/07-workers-placement-flow-control/07-01-SUMMARY.md
  modified:
    - .planning/RESERVATIONS.md
    - .planning/STATE.md
key-decisions:
  - "Gate passed at 000dced; Phase 7 needs 0 keyspaces and 0 meta variants (F1/F2 landed)"
  - "F7 is set from current_attempt.is_none() (not next_seq == 0, which never holds after 05-16); semantics match the plan's intent"
  - "Coordination: 08-16 landed first, so ostro_queue_worker_demand is describe-only and 07-05 keeps 08-16's dispatcher metrics; 07-11 extends ApiExtensions/router_with, never ApiConfig; 07-12 sink_for reuses EngineExecutor's bridge + guest-log construction; ostro worker follows 08-18's top-level Command split"
requirements-completed: []
duration: ~3h35m (dominated by the baseline ci-local under campaign load)
completed: 2026-11-21
---

# Phase 7 Plan 01: PHASE-7 GATE Summary

**Gate passed at 000dced. Phase 6 is complete (6 by-design human items, no gaps), Phase 5 F1–F4/F7/F8 are in code and the code ordinals match RESERVATIONS.md. Phase 7's Cmd 20/21, RejectReason 15 and §11 names are reserved, and STATE.md now carries D-W1..D-W26, the review/human todos and the Phase 7 ↔ Phase 8 shared-file rules.**

## Performance

- Started 2026-11-20 ~23:10 UTC; finished 2026-11-21 ~02:50 UTC.
- Tasks: 2/2. Files modified: 2 planning docs (+ this SUMMARY, ROADMAP tick, STATE Current Position line).

## Task commits

1. Task 1, RESERVATIONS.md Phase 7 rows: `de545d7` docs(07-01): reserve Phase 7 ordinals and names
2. Task 2, STATE.md decisions/todos/coordination: `5baab9f` docs(07-01): record Phase 7 decisions, review items and coordination

## Gate report

### Check 1: Phase 6 summaries
`ls 06-{01..20}-SUMMARY.md | wc -l` → `20`.

### Check 2: 06-VERIFICATION (07-01-classify.py output)
```
status=human_needed gaps=[] items=6
  [a MySQL 8.4 legs (D-FX5)] SC2 on MySQL 8.4: OSTRO_TEST_MYSQL_URL='mysql://root:<pw>@127.0.0.1:3306/ostro?ssl-mode=RE
  [b S3-compatible object store incl. presign (D-FX5)] SeaweedFS leg of SC5: docker run -d -p 8333:8333 -e AWS_ACCESS_KEY_ID=ostro -e AWS_SECRET_
  [e remote CI legs (FOUND-05)] Remote CI legs after the first push (linux-x86_64-gnu with postgres:17/mysql:8.4/SeaweedFS
  [c GCS via S3 interop] GCS through S3 interoperability: point OSTRO_TEST_S3_* at https://storage.googleapis.com (
  [f user review FX-05/D-FX2/D-FX6] User review of the FX-05 wording and Azure deferral (D-FX1), D-FX2 (native TLS roots + ca_
  [g optional real Vault dev server] Optional: real Vault dev server: OSTRO_TEST_VAULT_ADDR=http://127.0.0.1:8200 OSTRO_TEST_VA
GATE CHECK 2: PASS
```
(d) The OTLP canary leg is not a human item. It was closed at the Phase 8 gate (08-21 `otlp_canary` via `register_sink("otlp")`).

### Check 3: Phase 5 F1–F4, F7, F8 (grep evidence, 07-01-gate.sh)
- F1: `crates/ostro-partition/src/store/values.rs:147: pub pause: Option<PauseMark>,` and `:152: pub struct PauseMark {` (`reason u8, at_ms, detail`).
- F3: `pub struct ReadyValue { concurrency_key, workflow: String, tenant: Option<String>, first: bool }`; `apply/outcome.rs:188 EnqueueReady {` carries `workflow`, `tenant` and `first_activation: bool` (199).
- F2: `crates/ostro-meta/src/records.rs` counts: `struct SpillPolicy` 1, `struct RateSpec` 1, `key_rate_limit` 1, `key_concurrency` 1, `on_retries_exhausted` 5, `placement` 4, `selector` 4; `WorkflowPolicy` has `pub paused: bool` (records.rs:271).
- F4: `wit/ostro-runtime-0.1.0/runtime.wit:108` / `:112` "… never for control flow."
- F7: `values.rs:174 pub first: bool`; `query.rs:48 pub first_activation: bool` (`:111 first_activation: rv.first`); `outcome.rs:199 first_activation: bool`; `apply/run.rs:183 first_activation: first`. **Note:** `enqueue_ready` sets `first = rec.current_attempt.is_none()`, not `rec.next_seq == 0`. The code comment explains why: 05-16 writes RunStarted at StartRun, so `next_seq == 0` never holds at enqueue. The semantics ("no attempt ever granted" means the next activation is the first) are what D-W13's throttle needs. Accepted; this is not a missing finding.
- F8: `records.rs:196 pub expires_at_ms: Option<u64>,` on ApiToken.

### Check 4: Phase 6 artefacts
`effects.rs:276 pub struct ServerEffectDriver`, `:137 pub trait EffectHooks`; `mock_provider.rs:112 fn requests`, `:117 fn charges`; `canary.rs` present; `command.rs` Cmd 13 `CompleteEffect`; tag `wit/ostro-runtime-v0.1.0` exists. Result: `GATE CHECKS 1-4: PASS`.

### Check 5: Working tree
- The 07-01 worktree (detached at main) is clean for `crates vendor guests sdks scripts .github docs Cargo.toml Cargo.lock deny.toml`: 0 paths.
- The **main checkout** shows hundreds of `M`/`D`/`MM` paths across crates/, docs/, guests/ and Cargo files. These are not in-flight Phase 8 edits: every Phase 8 plan is committed. The main checkout's index and working tree are stale, predating Phases 6/8 on main, as the orchestrator noted.
- The stale paths include files in 07-02's `files_modified` (Cargo.toml, Cargo.lock, crates/ostro-server/src/{server,config,lib}.rs, scripts/ci-local.sh, .github/workflows/ci.yml). This is **not a contention**. It is recorded in STATE.md Blockers/Concerns: Phase 7 executors must check cleanliness in their own worktree at main, never in the main checkout.

### Check 6: Baseline `bash scripts/ci-local.sh --no-musl`
HEAD `000dced` (worktree), `CARGO_TARGET_DIR=target/wt-07-01`, `CARGO_BUILD_JOBS=6`. It ran concurrently with the Phase 8 10k campaign and the Phase 8 verifier, at load average 12–19 on 12 cores.

| Step | Result | Time |
|------|--------|------|
| toolchain 1.100.0 | PASS | 0s |
| cargo fmt --check | PASS | 1s |
| cargo clippy -D warnings | PASS | 77s |
| cargo build (warning-free) | PASS | 118s |
| sdk workspace fmt + clippy | PASS | 25s |
| sdk workspace test + wasm build | PASS | 24s |
| guests build (wasm32-wasip2) | PASS | 15s |
| order-saga example | PASS | 22s |
| external: OSTRO_TEST_PG_URL / MYSQL_URL / S3_URL | SKIP (human_needed) | - |
| cargo test | **FAIL** (1 load flake, see below) | 5718s |
| semantics sleepers (serial) | PASS | 136s |
| failpoint list sync (ci.yml) | PASS | 0s |
| failpoint suites | PASS | 5536s |
| server campaign smoke (20 kills) | PASS | 809s |
| cargo deny check | PASS | 5s |
| check-vendored.sh | PASS | 1s |
| check-deny-fixtures.sh | PASS | 1s |
| no chronik/tokio-uring/prometheus | PASS | 1s |
| workflow validation | PASS | 7s |
| musl legs | SKIP (--no-musl) | - |

The single `cargo test` failure, verbatim:
```
thread 'continue_as_new' (929773) panicked at crates/ostro-server/tests/semantics_e2e.rs:1134:5:
assertion `left == right` failed: {"completed":false,"run_id":"001038fvk39b6wvtptxdsh6bm4r3p","status":"running"}
  left: Null
 right: 30
test result: FAILED. 17 passed; 1 failed; ... finished in 32.56s
```
`GET /v1/runs/{second}/result?wait=30s` returned while the 30-run continue-as-new chain was still running, at load ~18. The reruns were sequential, at the same load and on the same build:
- `cargo test -p ostro-server --test semantics_e2e -- continue_as_new --exact`, three times: `test result: ok. 1 passed` each, 1.09 s / 1.09 s / 1.16 s.
- The whole binary: `test result: ok. 18 passed; 0 failed`, 10.52 s.

This is classified as a timing flake under external load, not a regression. No Phase 7 code exists yet, and every commit after 000dced up to the 07-01 commits is docs or 08-23. **Recommendation:** watch `continue_as_new` in later Phase 7 ci-local runs. If it recurs on a quiet machine, open a debug item. Every other step was green, so the baseline is recorded as green with this one flake.

Note: while the baseline ran, main advanced from 000dced to 6e69248 (08-23 RPO gap closure: ostro-backup, ostro-server config.rs/uploader, restore_rpo). The baseline covers 000dced; 07-02 runs on the newer HEAD anyway.

### Check 7: Reservations check (07-01-rescheck.py; condensed output, full output in the scratchpad `07-01-rescheck.out`)
```
Cmd 1..13 -> exists (P2/P5/P6) OK; Cmd 27 ScheduleFire -> exists (P8, 08-01) OK
Intent 1..10 -> exists OK (9 RegisterEffect, 10 CancelEffect = P6)
Outbox 1..6 -> exists OK
TerminalStatus 0..5 -> exists OK
StartRun fields in code: 1 .. 23 count 23 contiguous (all exists)
EventKind 1, 16..29, 64..71, 96, 97 -> exists OK
Keyspace 0..16 (meta … signal_seen, 16 schedule_state) -> names and status match OK
RejectReason 0..11 -> exists OK
InvalidCode 0..12 exists; 13/14 ReservedP5_*; 15 BadEffectCompletion; 16/17 ReservedP6_*; 18..21 ReservedP7_*; 22 BadScheduleFire; 23 ScheduleOwnerMismatch OK
MetaRecord 0..16 (… 14 WorkflowPolicy, 15 QueuePolicy, 16 Webhook) -> exists OK
MetaCommand order: ['InitCluster', 'Put', 'Delete', 'SetPartitionLeader']; META_FORMAT: 2
P7 block Cmd/Intent/StartRun/EventKind/TerminalStatus/Keyspace/RejectReason/InvalidCode/Outbox/MetaRecord: empty
RejectReason 12-14: absent (07-03 declares ReservedP6_12..14) | 15-17: absent
Phase 8 landed: Cmd 27 = ScheduleFire | keyspace 16 = ScheduleState | InvalidCode 22/23 = ['BadScheduleFire', 'ScheduleOwnerMismatch']
RESERVATIONS CHECK: PASS
```
- RejectReason 12–14: **absent**, so 07-03 declares the `#[doc(hidden)] ReservedP6_12..14` placeholders. 15–17: nothing.
- Phase 8 has landed Cmd 27, keyspace 16 `schedule_state` and InvalidCode 22/23.
- Nothing occupies Phase 7's blocks. InvalidCode 18–21 hold the canonical never-emitted `ReservedP7_*` placeholders that 08-01 declared, as RESERVATIONS §8 requires.

### Check 8: RESERVATIONS.md (de545d7)
- Phase 7 gate status note.
- §1: tag 20 PauseRun and 21 ResumeRun `planned P7 (07-03)`; 22–26 spare.
- §6: 17/18 reserved P7 and unused.
- §7: 15 RunPaused (code 31) `planned P7 (07-03)`, 16–17 spare, plus the ReservedP6_12..14 note.
- §10: 19–20 unused, no META_FORMAT bump.
- §11: 12 Phase 7 rows (crates, config sections, gRPC services, REST paths, failpoint/metric prefixes with the demand-gauge value, `wkr_` + `worker:join`, CLI verbs, `<data_dir>/tls/`, cert SAN URIs, in-memory actions, the proto_version note).

## Phase 7 ↔ Phase 8 coordination adjustments (recorded in STATE.md Blockers/Concerns)

From 06-14, 08-10, 08-16, 08-17 and 08-18 SUMMARYs plus code greps:
- **08-16 / dispatcher.rs:** 08-16 has landed. `ostro_queue_worker_demand` is described only (`describe_queue_metrics`); `grep -rn ostro_queue_worker_demand crates --include='*.rs'` finds only doc lines and the describe call. 07-05 keeps 08-16's metric helpers and has nothing to reduce. 07-16 owns the value.
- **08-16 / MeteredSink (ostro-engine activation.rs):** any new `ActivationSink` method must be forwarded there and in `EngineSink`. Phase 7 edits no engine file (D-W19).
- **06-14 / executor.rs:** `EngineExecutor::with_bridge` (executor.rs:344) and `EngineSink::effect_driver` forwarding exist. 08-17 added `handles_guest_logs`/`guest_log` forwarding. 07-12's `sink_for(job)` must reuse this construction. The 07-DECISIONS open question on 08-17's `guest_log` signature is settled: `guest_log(&self, rec: GuestLogRecord)`.
- **08-10 / ostro-api:** `ApiConfig` stays `Copy + Eq`, because tests build it with exhaustive literals. 07-11 adds its surface through `ApiExtensions` + `router_with` slots, never through `ApiConfig` fields.
- **08-18 / main.rs:** backup and restore are top-level `Command::Backup/Restore`, because `ClientCommand` dispatch always builds a REST client. The long-running `ostro worker` should follow that split. Token, queue, run and workflow verbs are client verbs.
- These went into STATE.md, not 07-DECISIONS.md, because 07-DECISIONS.md is outside this plan's `files_modified`.

## Deviations from Plan

1. **[Rule 1 - plan wording] F7 check:** the plan expects `enqueue_ready` to set `first_activation` from `rec.next_seq == 0`. The code uses `current_attempt.is_none()`, with a comment giving the 05-16 reason. The behaviour is equivalent to what was intended, so the gate passes and the difference is recorded above and in STATE.md.
2. **Baseline flake:** one `cargo test` failure under load, fixed by sequential reruns (details above). It is not counted as a gate failure.
3. **Decisions count:** 07-DECISIONS.md has D-W26 (clock seam) beyond the plan's D-W1..D-W25, so STATE.md records D-W1..D-W26.
4. **Commit script:** the first two commits landed correctly via CAS. The worktree checkout step afterwards refused because of the modified file. This was fixed in the script; no repository effect.

## Issues / follow-ups

- Watch `semantics_e2e::continue_as_new`; it timed out once at load ~18.
- Phase 7 executors must not treat the stale main checkout as contention (rule recorded in STATE.md).

## Self-Check: PASSED
- FOUND: .planning/RESERVATIONS.md rows (PauseRun, RunPaused, ostro-worker-proto)
- FOUND: .planning/STATE.md block "Phase 07 decisions (adopted defaults; user may override)", D-W25, describe_gauge
- FOUND: commits de545d7, 5baab9f on main
