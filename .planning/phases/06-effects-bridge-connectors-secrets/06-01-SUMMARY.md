---
phase: 06-effects-bridge-connectors-secrets
plan: 01
subsystem: planning
tags: [gate, reservations, decisions, docs-only]
requires:
  - "Phase 5 complete (05-VERIFICATION human_needed, by-design items only; WIT 0.1 frozen at fc47569)"
provides:
  - "Phase 6 gate report and baseline commit 6984651"
  - "Phase 6 decisions D-FX1..D-FX9, P-1..P-21 and ordinal allocations in STATE.md"
  - "FX-05 wording (shared ostro-objstore, Azure deferred) in REQUIREMENTS.md"
  - "RESERVATIONS §11 P-14 names + MinIO spec pointer; §8 canonical InvalidCode placeholder names Reserved<Phase>_<index>"
affects: [06-02, 06-03, 06-08, 06-12, 06-16, 08-01, 08-03]
tech-stack:
  added: []
  patterns: ["InvalidCode placeholder rule (Reserved<Phase>_<index>, doc(hidden), never emitted)"]
key-files:
  created:
    - .planning/phases/06-effects-bridge-connectors-secrets/06-01-SUMMARY.md
  modified:
    - .planning/STATE.md
    - .planning/REQUIREMENTS.md
    - .planning/RESERVATIONS.md
    - .planning/ROADMAP.md
key-decisions:
  - "Gate passed at 6984651: Phase 5 complete, every code ordinal matches RESERVATIONS.md, P6 block untouched"
  - "ostro-objstore not yet created at 06-01 (neither Phase 6 nor Phase 8 code has landed); first of 06-02 / 08-01 creates it"
  - "W1-W8 doc comments are present in frozen runtime.wit; no STATE todo needed"
metrics:
  duration: "~45 min (ci-local --no-musl ~51 min of it ran in the background)"
  completed: 2026-11-20
---

# Phase 6 Plan 01: Phase-6 Gate, Decisions and Reservations Summary

**The Phase 6 gate passed at baseline `6984651`. Phase 5 is complete: 20/20 required SUMMARYs exist, 05-VERIFICATION is human_needed with only the five by-design items, the WIT 0.1 freeze is in force, the code tree is clean and ci-local `--no-musl` is green. Every code ordinal matches RESERVATIONS.md and the P6 block is untouched. The Phase 6 decisions, review todos, FX-05 wording, P-14 names and the canonical InvalidCode placeholder names are recorded in a docs-only commit.**

## Gate report (Task 1)

| # | Check | Result |
|---|-------|--------|
| 1 | `05-{01..20}-SUMMARY.md` exist | 20/20 present (05-21..05-27 SUMMARYs also present) |
| 2 | 05-VERIFICATION status | `status: human_needed`; all 5 human items classified as by-design (output below) |
| 3 | Freeze in force | `wit/FROZEN.toml` present; tag `wit/ostro-runtime-v0.1.0` -> `ddddddca5c4c…`; `cargo test -p ostro-witcheck --locked`: test result: ok. 4 passed; no `planned P5` in RESERVATIONS.md; `FROZEN v0.1` in docs/storage-format.md |
| 4 | `git status --porcelain crates vendor guests sdks scripts .github docs wit Cargo.toml Cargo.lock` | empty |
| 5 | W1-W8 doc comments in runtime.wit | all present: `effect_outcome_unknown` 2, `presign-get` 1, `sql_not_transactional` 2, `Idempotency-Key` 1. No STATE todo needed |
| 6 | Baseline `bash scripts/ci-local.sh --no-musl` | all steps passed at HEAD `6984651` (timings below) |
| 7 | Code vs RESERVATIONS.md | all match; P6 block untouched; no Phase 7/8 code landed (output below) |

### Check 2: by-design classification (script output)

```
status: human_needed
(a) native NVMe perf :: "Native Linux NVMe perf gate (D15/D16): profile A at 64 clients and at 1 client (300 runs)
(b) aarch64/macOS CI legs :: "After the first push, check that the linux-aarch64-musl (ubuntu-24.04-arm) and macos-aarc
(c) docker quickstart :: "Run bash examples/order-saga/quickstart.sh on a clean machine where docker info succeeds"
(d) real ENOSPC tmpfs :: "Optional: real ENOSPC leg on a 64 MiB tmpfs (sudo mount), with OSTRO_CAMPAIGN_STATE_DIR o
(e) decision/freeze review :: "User review of adopted decisions D7 (step-local RNG frozen), D8 (relaxed startup criterio
CLASSIFY: all by-design
```

### Check 6: baseline ci-local `--no-musl` at 6984651

```
toolchain 1.100.0                    PASS  1s
cargo fmt --check                    PASS  1s
cargo clippy -D warnings             PASS  79s
cargo build (warning-free)           PASS  134s
sdk workspace fmt + clippy           PASS  31s
sdk workspace test + wasm build      PASS  35s
guests build (wasm32-wasip2)         PASS  19s
order-saga example                   PASS  42s
cargo test                           PASS  863s
semantics sleepers (serial)          PASS  104s
failpoint list sync (ci.yml)         PASS  0s
failpoint suites                     PASS  1248s
server campaign smoke (20 kills)     PASS  498s
cargo deny check                     PASS  1s
check-vendored.sh                    PASS  1s
check-deny-fixtures.sh               PASS  1s
no chronik/tokio-uring/prometheus    PASS  0s
workflow validation                  PASS  0s
zig + cargo-zigbuild present         SKIP  -
musl release (static, mimalloc)      SKIP  -
musl zigbuild test                   SKIP  -
all steps passed
```

The only `warning` lines in the log are cargo-deny `warning[duplicate]` notices (pre-existing, allowed by the stock config). There were no compiler warnings. The run ran in a detached worktree with `CARGO_TARGET_DIR=target/wt-06-01`, alongside 08-01's worktree.

### Check 7: reservations script output

The 05-01 script (`rescheck.py`) lived in an earlier session scratchpad and is gone, so a new one was written. It parses `ostro-types` command.rs/journal.rs, `ostro-partition` store/mod.rs and apply/outcome.rs, and `ostro-meta` command.rs. It then compares each ordinal space against the RESERVATIONS `exists` rows and the P6 block:

```
Cmd oneof (§1)               code=1..12  exists=1..12  P6-block=13..19  OK
StartRun fields (§2)         code=1..23  exists=1..23  P6-block=24..27  OK
Intent oneof (§3)            code=1..8  exists=1..8  P6-block=9..12  OK
EventKind (§4)               code=1,16..29,64..71,96..97  exists=1,16..29,64..71,96..97  P6-block=72..79  OK
TerminalStatus (§5)          code=0..5  exists=0..5  P6-block=-  OK
Tables (§6)                  code=0..15  exists=0..15  P6-block=-  OK
   table names: Meta Runs Events Payloads Ready Timers Inbox Dedupe Outbox InboundSeen Children Dlq Effects Running Waits SignalSeen
RejectReason (§7)            code=0..11  exists=0..11  P6-block=12..14  OK
InvalidCode (§8)             code=0..12  exists=0..12  P6-block=15..17  OK
   invalid codes: Decode Empty BadRunId MissingToken EventsNotContiguous MissingField NotImplemented BadIntent FieldTooLong BadSource TruncateBeyondEnd BadLength NotRunning
Outbox Kind (§9)             code=1..6  exists=1..6  P6-block=7  OK
MetaKind (§10)               code=0..16  exists=0..16  P6-block=17..18  OK
MetaRecord (§10)             code=0..16  exists=0..16  P6-block=17..18  OK
   meta kinds: Cluster Member Partition Module Workflow Active Queue Schedule Token Tenant Secret Grant Connection Egress WorkflowPolicy QueuePolicy Webhook
   meta records match kinds: True
MetaCommand (§10a)           code=0..3  exists=0..3  P6-block=-  OK
   meta commands: InitCluster Put Delete SetPartitionLeader
META_FORMAT = 2 (frozen baseline 2): OK
EVENT_CODEC_VER = 1: OK
crates/ostro-objstore: absent
crates/ostro-backup: absent
crates/ostro-triggers: absent
RESULT: all code ordinals match RESERVATIONS.md; P6 block untouched
```

The same result came back on `8abba5e`, after the docs commit.

**Phase 7/8 status and objstore owner:** at 06-01, no Phase 7 or Phase 8 ordinal is in code, and none of `crates/ostro-objstore`, `crates/ostro-backup` or `crates/ostro-triggers` exists. `crates/ostro-bridge` exists only as the 01-01 skeleton. **`ostro-objstore` has no owner yet.** Whichever of 06-02 or 08-01 runs first creates it, which is also recorded in STATE Decisions. InvalidCode indices 13–14 are still absent, so 06-02 must declare `ReservedP5_13` and `ReservedP5_14` before `BadEffectCompletion` at 15.

## Task 2: tracking edits (commit 8abba5e, docs only)

- **STATE.md § Decisions:** added a `[Phase 06 decisions (adopted defaults; user may override)]` block with one line for each of D-FX1..D-FX9 and P-1..P-21, plus the P6 ordinal allocations and the objstore owner bullet. The Phase 4 decision (4) on unsupported effects gained `— SUPERSEDED for http/sql/object effects by Phase 6 (06-14); child workflows still unsupported until Phase 9`; nothing was deleted.
- **STATE.md § Pending Todos:** the three `[USER REVIEW]` items (FX-05/Azure, D-FX6, D-FX2) and the two `[HUMAN]` items (GCS interop, MySQL/MinIO legs).
- **REQUIREMENTS.md:** the FX-05 bullet was replaced with the new wording. It stays unchecked and the traceability table is untouched.
- **RESERVATIONS.md:**
  - §8: the P-15 placeholder rule, with canonical names `ReservedP5_13`, `ReservedP5_14`, `ReservedP6_15..17`, `ReservedP7_18..21` and `ReservedP8_22..24`.
  - §11: the P-14 rows (gRPC services `ostro.api.v1.Secrets`/`ostro.api.v1.Connectors`, `ostro/api/v1/bridge.proto`, CLI verbs, `ostro_effect_`, env vars, testkit features) and a pointer to the shared MinIO/S3 test spec in 06-DECISIONS.md.
- All hunks were built from the current main blob by a script, staged in a private `GIT_INDEX_FILE` and committed with a CAS `update-ref`, which succeeded on the first try.

## Commits

| Task | Commit | Description |
|------|--------|-------------|
| 1 | (none) | Gate checks only; no files changed |
| 2 | 8abba5e | docs(06-01): record Phase 6 decisions, FX-05 wording, P6 names and InvalidCode placeholder rule |

## Deviations from Plan

- **Reservations script:** the 05-01/05-20 script was not recoverable (session scratchpad), so a new one was written. It is equivalent in scope and the plan allows this ("or write a short one").
- STATE Current Position gains one Phase 6 progress line in the metadata commit, and ROADMAP ticks 06-01. Both are tracking edits the orchestrator asked for. No gsd-tools mutators were run.

Otherwise none: the plan was executed as written. No code changed.

## Self-Check: PASSED

- FOUND: .planning/phases/06-effects-bridge-connectors-secrets/06-01-SUMMARY.md
- FOUND: commit 8abba5e
- Task 1 verify and Task 2 verify both pass on main `8abba5e`.

Note 2026-11-20: MinIO replaced by SeaweedFS per user direction (see DECISIONS).
