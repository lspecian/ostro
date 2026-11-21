---
phase: 09-polyglot-sdks-conformance
plan: 01
subsystem: planning-gate
tags: [gate, reservations, decisions, coordination, polyglot]
requires:
  - phase: 06-effects-bridge-connectors-secrets
    provides: "20/20 SUMMARYs, 06-VERIFICATION human_needed (by-design only)"
  - phase: 05-workflow-semantics-core-hardening
    provides: "WIT freeze (wit/FROZEN.toml + tag wit/ostro-runtime-v0.1.0), PF-F1/PF-F2/PF-F3/PF-W docs"
provides:
  - "PHASE-9 GATE passed at baseline 38c3b66 (ci-local --no-musl all steps passed)"
  - "RESERVATIONS.md: P9 block reserved-unused, D-P9-20 §11 name rows, Phase 9 gate note"
  - "STATE.md: D-P9-1..D-P9-30, [USER REVIEW] D-P9-3, two [HUMAN] todos, Phase 9 <-> Phase 7/8 coordination"
affects: [09-02, 09-03, 09-04, 09-05, 09-06, 09-07, 09-08, 09-09, 09-10, 09-11, 09-12, 09-13, 09-14, 09-15, 09-16, 09-17, 09-18, 09-19, 09-20]
tech-stack:
  added: []
  patterns: ["docs-only gate commits through a private index (index-09-01) with a CAS update-ref"]
key-files:
  created:
    - .planning/phases/09-polyglot-sdks-conformance/09-01-SUMMARY.md
  modified:
    - .planning/RESERVATIONS.md
    - .planning/STATE.md
    - .planning/ROADMAP.md
key-decisions:
  - "Phase 9 allocates no ordinal (D-P9-6): PF-F1/F2 are in code, so the whole P9 block stays reserved, unused (fallback only)"
  - "Check 5 (working tree) is judged in the 09-01 worktree at main (clean); the main checkout is stale, not in-flight work"
  - "ostro-server is already in FAILPOINT_CRATES (ostro-server:failpoints_phase5), so 09-02 adds nothing there"
requirements-completed: []
duration: ~110 min (of which ~102 min the baseline ci-local run)
completed: 2026-11-21
---

# Phase 9 Plan 01: PHASE-9 GATE Summary

**The gate passed. Phase 6 is complete (6 human items, all by design). The Phase 5 freeze is in force. PF-F1/PF-F2 and the PF-F3/PF-W doc comments are in the code. Children are still unbridged. The code matches RESERVATIONS.md, and nothing occupies the P9 block. The baseline `ci-local --no-musl` run at 38c3b66 passed every step. Node, uv, componentize-py and jco are at their pinned versions.**

## Performance

- Started 2026-11-21 ~17:15Z, finished ~19:10Z.
- 2 tasks, 2 task commits, plus the docs commit for this SUMMARY.
- No code changes.

## Gate report

### Check 1: Phase 6 complete: PASS
- All of `06-01..06-20-SUMMARY.md` exist (20 files).
- `06-VERIFICATION.md` has `status: human_needed` and `gaps: []`.
- Output of the classifier (`09-01-classify.py`; it maps each `human_verification` test to the by-design set (a)–(g) from 06-VALIDATION and fails on anything outside it):

```
status: human_needed
  [a MySQL 8.4 legs] SC2 on MySQL 8.4: OSTRO_TEST_MYSQL_URL='mysql://root:<pw>@127.0.0.1:3306/ostro?ssl-mode=RE
  [b MinIO/S3-compatible incl. presign] SeaweedFS leg of SC5: docker run -d -p 8333:8333 -e AWS_ACCESS_KEY_ID=ostro -e AWS_SECRET_
  [e remote CI legs (FOUND-05)] Remote CI legs after the first push (linux-x86_64-gnu with postgres:17/mysql:8.4/SeaweedFS
  [c GCS via S3 interop] GCS through S3 interoperability: point OSTRO_TEST_S3_* at https://storage.googleapis.com (
  [f user review FX-05 / D-FX2 / D-FX6] User review of the FX-05 wording and Azure deferral (D-FX1), D-FX2 (native TLS roots + ca_
  [g optional real Vault dev server] Optional: real Vault dev server: OSTRO_TEST_VAULT_ADDR=http://127.0.0.1:8200 OSTRO_TEST_VAULT_TOKEN=<root> ...
6 human items, 0 outside the by-design set
```

Item (d), the OTLP canary, is no longer listed: it closed at the Phase 8 gate.

### Check 2: Phase 5 freeze: PASS
- `wit/FROZEN.toml` is present. It pins `ostro-runtime-0_1_0` with sha256 `611d2982…bd98e`, `frozen_at = "2026-11-19"`.
- `refs/tags/wit/ostro-runtime-v0.1.0` = `ddddddca5c4cf8823416aea9c9a4f99627dfdc89`.
- `ostro-witcheck` is green in the baseline `cargo test`: `frozen_0_1_0_is_byte_identical ... ok`, `runtime_0_1_chain_is_additive ... ok`.
- No `planned P5` row is left in RESERVATIONS.md.
- `docs/storage-format.md:3` reads `**Status: FROZEN v0.1 (2026-11-19, plan 05-20).**`, and §14 is `FROZEN v0.1`.

### Check 3: PF-F1 / PF-F2 / PF-F3 / PF-W in code: PASS
PF-F1 (`crates/ostro-types/src/command.rs`):
```
671-    /// Phase 9 PF-F1). Apply forwards them verbatim in [`ChildCompleted::event`]. Phase 5.
672:    #[prost(message, optional, tag = "5")]
673-    pub parent_event: Option<PayloadRef>,          // CompleteRun
758-    /// Submitter-encoded `ChildCompleted` (kind 67) journal event bytes for the parent, copied
759-    /// verbatim from [`CompleteRun::parent_event`] (D21, Phase 9 PF-F1). Phase 5.
760:    #[prost(message, optional, tag = "6")]
761-    pub event: Option<PayloadRef>,                 // outbox ChildCompleted
```
`crates/ostro-partition/src/apply/run.rs`:
```
30://! - `CompleteRun.parent_event` is forwarded verbatim into the outbox `ChildCompleted.event`
534:            Some(Intent::CompleteRun(c)) => oversize(&c.result) || oversize(&c.parent_event),
699:                            event: c.parent_event.clone(),
```
PF-F2:
```
crates/ostro-partition/src/store/values.rs:145:    pub continued_to: Option<RunId>,                        (RunRecord field)
crates/ostro-partition/src/apply/lifecycle.rs:606:    rec.continued_to = Some(succ_id);                     (05-09 setter)
crates/ostro-partition/src/apply/waits.rs:667/690: terminal ContinuedAsNew -> CancelRun forwarded to continued_to (PF-F2)
  (one shared `cancel()` path serves both `request_cancel` (RequestCancel) and `inbound_cancel` (CancelRun), 05-08)
crates/ostro-server/src/derive.rs:541:        parent: pred.parent.map(|(id, op)| ParentRef {   (successor copies the parent, 05-11)
crates/ostro-server/src/derive.rs:554:        continued_from: parts.pred_id.as_bytes().to_vec(),
```
PF-F3 / PF-W docs in `wit/ostro-runtime-0.1.0/runtime.wit`:
```
50:  /// - Children and SDKs (Phase 9): `child_terminated`, `sdk_protocol`, `invalid_string`.
388: /// ... Terminated -> `err(non-retryable{kind: "child_terminated"})`
391: /// `cancel-op` on a child op completes it with `err(cancelled)` and requests graceful
15:  /// Guest toolchains differ on lone surrogates (StarlingMonkey silently replaces them with
```

### Check 4: artefacts Phase 9 reads: PASS
- `crates/ostro-server/src/sink.rs:99` `pub const CHILDREN_UNSUPPORTED: &str = "child workflows arrive in Phase 6/9";` and `:162` `EventBody::ChildStarted(_) => { out = Err(unsupported_failure(CHILDREN_UNSUPPORTED));`. Children are still unbridged, so the WF-06 server wiring belongs to Phase 9.
- `sdks/rust/ostro-sdk/src/effects.rs:67` `pub fn idempotency_key(run_id: &str, name: &str) -> String`.
- In the Rust SDK, `ctx.rs:227` has `pub fn shielded` and `ctx.rs:210` has `pub fn continue_as_new` (05-06).
- `crates/ostro-testkit/src/mock_provider.rs` is present.
- `crates/ostro-server/src/derive.rs:413` `pub trait ModuleResolver: Send + Sync`.
- `crates/ostro-server/Cargo.toml` has `failpoints = [` (05-18).
- **`ostro-server` is in FAILPOINT_CRATES** as `ostro-server:failpoints_phase5`, so 09-02 adds nothing there.

### Check 5: working tree: PASS (no contention)
- The 09-01 worktree, detached at main, has 0 dirty paths under `crates vendor guests sdks scripts .github docs wit Cargo.toml Cargo.lock deny.toml`.
- The **main checkout** shows hundreds of `M`/`D`/`MM` paths, including Cargo.toml, Cargo.lock, ci.yml, ci-local.sh and `wit/FROZEN.toml` (D). This is the stale main-checkout index and tree recorded by 07-01, not in-flight plan work: no Phase 7/8 plan is executing. So these paths are **not a contention for 09-02**, even though some of them are in 09-02's `files_modified`.
- STATE.md now repeats the rule that executors check cleanliness in their own worktree at main, never in the main checkout.

### Check 6: baseline `bash scripts/ci-local.sh --no-musl`: PASS
- HEAD `38c3b667f0b35159ab0129d0463bf57e30161acf`, `CARGO_TARGET_DIR=<repo>/target/wt-09-01`, `nice`, machine otherwise idle.
- Ran 2026-11-21 17:24:21Z → 19:06:07Z (1 h 42 min).
- A private PG 14.24 cluster (port 55491, scram auth, data dir in the scratchpad, stopped by its PID afterwards) set `OSTRO_TEST_PG_URL`. Its log has 108 `ostro_effects` lines, so the PG legs really ran.
- MySQL and S3 SKIP (human_needed by design, D-FX5). The object-store `external_s3` tests that passed are the in-process s3s-fs conformance legs.

| Step | Result | Time |
|------|--------|------|
| toolchain 1.100.0 | PASS | 0s |
| cargo fmt --check | PASS | 2s |
| cargo clippy -D warnings | PASS | 57s |
| cargo build (warning-free) | PASS | 86s |
| sdk workspace fmt + clippy | PASS | 18s |
| sdk workspace test + wasm build | PASS | 22s |
| guests build (wasm32-wasip2) | PASS | 12s |
| order-saga example | PASS | 20s |
| external: OSTRO_TEST_MYSQL_URL | SKIP | - |
| external: OSTRO_TEST_S3_URL | SKIP | - |
| cargo test | PASS | 2753s |
| semantics sleepers (serial) | PASS | 53s |
| failpoint list sync (ci.yml) | PASS | 0s |
| failpoint suites | PASS | 2424s |
| server campaign smoke (20 kills) | PASS | 600s |
| phase 7 failpoint legs + manifests | PASS | 53s |
| cargo deny check | PASS | 1s |
| check-vendored.sh | PASS | 2s |
| check-deny-fixtures.sh | PASS | 0s |
| no chronik/tokio-uring/prometheus | PASS | 1s |
| workflow validation | PASS | 2s |
| musl legs (3) | SKIP (--no-musl) | - |

Result: `all steps passed`, EXIT=0.

**Main moved during the run, by commits from outside Phase 9:**
- `5c33b85`: planning-doc local paths replaced with placeholders.
- `6ec19c2`: README.
- `5b81da7`, `2a8ace4`, `2aa044a`, `4e36f38`, `8e55ee3`: test-only fixes in `cli_bridge`, `sql_mysql`, `worker_e2e` and `ostro-engine/tests/determinism.rs`.
- `de02182`: a docs/debug note on the first GitHub CI run.

None of them touches non-test code under `crates/`, or `sdks/`, `guests/`, `wit/` or the scripts. The baseline was not re-run.

### Check 7: toolchains

| Tool | Found | Expected |
|------|-------|----------|
| node | v24.13.0 | v24.13.0 |
| npm | 11.6.2 | — |
| uv | 0.7.15 | 0.7.15 (D-P9-28) |
| `uv python find 3.12` | `/home/linuxbrew/.linuxbrew/opt/python@3.12/bin/python3.12` | 3.12 |
| `uv tool run --from componentize-py==0.25.1 componentize-py --version` | `componentize-py 0.25.1` (exit 0) | 0.25.1 |
| jco (`spikes/s2-guests/ts/node_modules/@bytecodealliance/jco/package.json`, main checkout; untracked, so absent in a fresh worktree) | 1.42.0 | 1.42.0 |
| componentize-js (same node_modules) | 0.23.0 | 0.23.0 |

No toolchain risk for 09-02/09-04. `spikes/s2-guests/ts/node_modules` is untracked and exists only in the main checkout, not in fresh worktrees. 09-02's own pinned npm toolchain package (D-P9-5) avoids depending on it.

### Check 8: reservations check: PASS
`09-01-rescheck.py` is the 07-01/07-22 script. Its Phase 7 block assertions are replaced by Phase 9 assertions: every P9 range must be empty in code. The script also prints the Phase 7/8 ordinals that have landed. Summary of the output (147 OK lines, 0 FAIL):

```
# Cmd tags 1..13, 20, 21, 27; Intents 1..10; Outbox 1..6; TerminalStatus 0..5; StartRun 1..23 contiguous;
# EventKind 1, 16..29, 64..71, 96, 97; Keyspaces 0..16; RejectReason 0..11, ReservedP6_12..14, 15;
# InvalidCode 0..12, ReservedP5_13/14, 15, ReservedP6_16/17, ReservedP7_18..21, 22, 23; MetaRecord 0..16  -> all exists/placeholder OK
MetaCommand order: ['InitCluster', 'Put', 'Delete', 'SetPartitionLeader']
META_FORMAT: 2
# Phase 9 block must be EMPTY in code (D-P9-6: PF-F1/F2 landed in Phase 5)
P9 block Cmd 34-40             : empty OK
P9 block StartRun 36-39        : empty OK
P9 block Intent 19-22          : empty OK
P9 block EventKind 98-105      : empty OK
P9 block TerminalStatus 10-11  : empty OK
P9 block Keyspace 19           : empty OK
P9 block RejectReason 20-22    : empty OK
P9 block InvalidCode 25-28     : empty OK
P9 block Outbox 11-12          : empty OK
P9 block MetaRecord 22-23      : empty OK
# Phase 7 / Phase 8 ordinals landed in code
P7: Cmd {20: 'PauseRun', 21: 'ResumeRun'} | RejectReason 15 = ['RunPaused'] | keyspace 17/18 = absent | MetaRecord 19/20 = absent
P8: Cmd {27: 'ScheduleFire'} | keyspace 16 = ScheduleState | InvalidCode 22-24 = ['BadScheduleFire', 'ScheduleOwnerMismatch']
Code tops: Cmd max 27 | StartRun max 23 | Intent max 10 | EventKind max 97 | TABLES 17 | RejectReason 16 | InvalidCode 24 | Outbox max 6 | MetaRecord 17
RESERVATIONS CHECK: PASS
```

Meta ext tags: only `EXT_TAG_TEST = 15` exists in code, so the P9 ext range 64–79 is empty too.

**Phase 7/8 ordinals that have landed:**
- Cmd 20 `PauseRun`, 21 `ResumeRun` and RejectReason 15 `RunPaused`, with the `ReservedP6_12..14` and `ReservedP7_18..21` placeholders (P7).
- Cmd 27 `ScheduleFire`, keyspace 16 `schedule_state` and InvalidCode 22/23 (P8).

### Check 9: RESERVATIONS.md (commit 13df6f7)
- Every P9 row reads "reserved P9, **unused** (PF-F1/F2 landed in Phase 5; fallback only)" (10 rows): §1 34–40, §2 36–39, §3 19–22, §4 98–105, §5 10–11, §6 19 `p9_a` (never declared by Phase 9), §7 20–22, §8 25–28, §9 11–12, §10 22–23.
- `CompleteRun.5` and outbox `ChildCompleted.6` were already `exists (P5, 05-01)`. "verified in code by 09-01" was appended; the status itself was already right.
- A Phase 9 gate note sits in rule 5, next to the Phase 6/7/8 gate notes.
- §11 gained the D-P9-20 rows:
  - the `polyglot` feature and the `ostro-conf-inspect` bin;
  - dirs `conformance/`, `sdks/scripts/` (incl. `toolchain/`), `target/polyglot-guests/`, `target/toolchains/`;
  - config keys `[engine] class` and `pool_total_core_instances`;
  - failpoints `child::detached_after_flush`, `child::parent_event_before_flush`, `child::start_child_after_flush`;
  - the seven guest names;
  - error kinds `python_exception` and `js_exception`, plus `unknown_workflow` reused for child targets;
  - CancelRun reasons `parent_closed`, `parent_cancel_op`, `chain_done`;
  - env vars `OSTRO_POLYGLOT_CACHE`, `OSTRO_CONF_LANGS`, `OSTRO_CRASH_CHAIN_ITERS`.

## Task 2: STATE.md (commit 4fd6952)
- **Decisions:** a `[Phase 09 decisions (adopted defaults; user may override)]` block with one bullet each for D-P9-1..D-P9-30. D-P9-3 reads "adopted; USER REVIEW pending".
- **Pending Todos:** `[USER REVIEW] D-P9-3`, `[HUMAN] Phase 9 publishing`, `[HUMAN] Phase 9 upstream`.
- **Blockers/Concerns:**
  - "Phase 9 ↔ Phase 7/8 coordination (parallel)", the condensed shared-file table plus the worktree-cleanliness rule;
  - the polyglot toolchain rule (`--no-polyglot`).
- The edits are additions only: 46 lines, no other section touched.

## Coordination notes from the SDK-adjacent Phase 6–8 SUMMARYs
These are recorded in STATE Blockers/Concerns.
- **06-09:** the Rust SDK `idempotency_key(run, name)` is `"ostro-" + hex(sha256("ostro/idem/v1/name" ‖ run_id ‖ 0x00 ‖ name))[..32]`, 38 chars, with sha2 `=0.10.9` in both `sdks/rust` and `guests/`. The Python/TS SDKs and C-VAL-01 (D-P9-21) must reproduce it byte for byte. The fx-* guests are the Rust references for the HTTP/SQL/object builders.
- **07-12 W6:** a worker `StartEffect` accepts a redacted header value containing `{{` only if it consists of well-formed `{{secret:NAME}}` placeholders. FX-06 grammar requires lowercase names; 07-16 fixed `pay-flow` to `pay_token`. The Python/TS effect builders (09-12) must emit the same template grammar, or their effects fail when a run executes on a worker. W6 itself is under USER REVIEW together with D-W28.
- **08-17 guest logs:**
  - `log.emit` goes through the node `GuestLogSink`: per-run 50/s with burst 500, a 10,000-line / 10 MiB lifetime cap held in memory only, nothing emitted on replay, never journaled.
  - D-P9-15 routes stdout/stderr/console to `log.emit`, so conformance must not treat logs as durable or journaled. Chatty Python/TS guests may hit the caps.
  - The engine falls back to its own logging when a sink does not consume guest logs. This matters for the testkit engine leg.
- **Guests workspace:**
  - `members = ["*"]` with exact pins; `guests/Cargo.lock` is gitignored.
  - No Phase 7/8 guest name collides with the D-P9-20 names: Phase 7/8 added `cpu-burn`, `long-journal`, `pay-flow`, `log-spammer`, `hook-order`; Phase 6 added `fx-*`.
  - Nothing from Phase 7/8 touched `sdks/`.

## Deviations from Plan
1. **[Brief] ROADMAP tick and the STATE Current Position line.** The plan says to touch only Decisions/Pending Todos/Blockers in STATE and that executors never edit ROADMAP. The orchestrator's executor brief asks for `- [x] 09-01` in ROADMAP and a Phase 9 plan line in STATE Current Position. Both were done in the final docs commit, built from the current main blobs.
2. **[Rule 3] Check 5 judged in the worktree,** as 07-01 did. The literal `git status --porcelain` in the main checkout shows the stale-checkout artefacts described above.
3. **Main moved during the baseline run** (non-Phase-9, test/docs-only commits, listed under Check 6). The baseline stands at 38c3b66.

## Issues Encountered
None. The gate passed on the first attempt, and no fix was needed.

## Next Phase Readiness
- Wave 2 can start: 09-02 (Cargo/harness/build script) and 09-03 (SDK skeletons).
- 09-02 adds nothing to FAILPOINT_CRATES.
- The toolchains are present at their pins.

## Self-Check: PASSED
- FOUND: .planning/phases/09-polyglot-sdks-conformance/09-01-SUMMARY.md
- FOUND: 13df6f7 docs(09-01): record Phase 9 reservations status and names
- FOUND: 4fd6952 docs(09-01): record Phase 9 decisions, review items and coordination
- Task 1 and Task 2 automated verify commands: VERIFY1 OK, VERIFY2 OK
