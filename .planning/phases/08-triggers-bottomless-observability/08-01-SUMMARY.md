---
phase: 08-triggers-bottomless-observability
plan: 01
subsystem: infra
tags: [gate, reservations, prost, fjall, opentelemetry, prometheus, rusty-s3, croner, cargo-deny]

requires:
  - phase: 05-workflow-semantics-core-hardening
    provides: frozen format (D18), WIT 0.1 freeze (fc47569), 05-07 Schedule fields + Webhook meta, empty-log anchor (05-21)
provides:
  - Phase 5 completion gate passed and recorded (baseline ci-local green at 6984651)
  - Cmd::ScheduleFire at prost tag 27 + three appended golden vectors
  - keyspace 16 schedule_state (Table::ScheduleState), key shapes and row types
  - InvalidCode 22 BadScheduleFire, 23 ScheduleOwnerMismatch (+ canonical placeholders 13-21)
  - routing::schedule_owner, cron idempotency keys, OverlapPolicy mapping
  - config/trait contracts (SnapshotConfig, ScheduleSpec/ScheduleSource/CronConfig, BackupConfig, telemetry span-id override)
  - every Phase 8 dependency declared once; crates ostro-triggers, ostro-backup, ostro-objstore
  - scripts/forbidden-crates.sh (anchored, self-tested) shared by ci-local and ci.yml
affects: [08-02 .. 08-22, phase 6 (ostro-objstore, InvalidCode placeholders, Cargo.lock), phase 7]

tech-stack:
  added: [opentelemetry 0.33.1, opentelemetry_sdk 0.33.1, opentelemetry-otlp 0.33.1, opentelemetry-proto 0.33.1, opentelemetry-appender-tracing 0.33.1, tracing-opentelemetry 0.34.0, metrics-exporter-prometheus 0.18.3, rusty-s3 0.12.0, croner 4.0.3, jiff 0.2.46 (as jiff02), tar 0.4.46, lz4_flex 0.13.2, url 2.5.8, futures-core/futures-util 0.3.34]
  patterns:
    - "Declared-but-unused dependencies are anchored with `use x as _;` (cargo 1.100's unused-dependency lint), as ostro-engine already did for chacha20"
    - "InvalidCode placeholders Reserved<Phase>_<index>, renamed in place by the owning phase"

key-files:
  created:
    - crates/ostro-partition/src/apply/schedule.rs
    - crates/ostro-partition/src/leader/cron.rs
    - crates/ostro-partition/src/leader/snapshot.rs
    - crates/ostro-partition/src/store/snapshot.rs
    - crates/ostro-partition/src/store/blobrefs.rs
    - crates/ostro-engine/src/telemetry.rs
    - crates/ostro-backup/ (config.rs contract + stubs)
    - crates/ostro-triggers/ (stubs)
    - crates/ostro-objstore/ (skeleton)
    - scripts/forbidden-crates.sh
    - ci/deny-fixtures/ban-prometheus/
  modified:
    - crates/ostro-types/src/command.rs
    - crates/ostro-types/src/routing.rs
    - crates/ostro-types/tests/command.rs
    - crates/ostro-types/tests/golden/commands.txt
    - crates/ostro-partition/src/store/{mod,keys,values}.rs
    - crates/ostro-partition/src/apply/{mod,outcome}.rs
    - crates/ostro-partition/src/{leader/mod,node}.rs
    - Cargo.toml, Cargo.lock, member Cargo.toml files
    - scripts/ci-local.sh, scripts/check-deny-fixtures.sh, .github/workflows/ci.yml
    - docs/storage-format.md, .planning/RESERVATIONS.md, .planning/STATE.md

key-decisions:
  - "routing::schedule_owner takes PartitionCount (like partition_for_key), not a raw u16 as the plan sketched, so P = 0 cannot divide by zero"
  - "storage-format gets a new section 15 (sections 13/14 were already Phase 5's), not 13 as the plan wrote"
  - "opentelemetry_sdk without rt-tokio: the 0.33.1 batch processors run on SDK-owned std threads; `testing` exists under that name and is a server dev-dependency"
  - "ostro-objstore was absent, so Phase 8 created the skeleton (D19)"

requirements-completed: []

duration: ~1h45m (most of it in two full ci-local runs)
completed: 2026-11-20
---

# Phase 8 Plan 01: Phase-8 gate, format contract, dependencies Summary

**The gate passed. Phase 8's additive contract landed: `Cmd::ScheduleFire` (tag 27), keyspace 16 `schedule_state` and InvalidCode 22/23. All of Phase 8's dependencies are declared, the three skeleton crates exist, and the forbidden-crate check is now anchored. ci-local is green with no new deny exception.**

## Performance

- **Duration:** about 1 h 45 min. Two full `ci-local.sh --no-musl` runs took about 50 min each.
- **Completed:** 2026-11-20
- **Tasks:** 3/3
- **Files:** about 60 (most are one-line registration edits)

## Why this plan is wide (W5)

Most of the ~60 files are mechanical registrations:
- a dependency line per member `Cargo.toml`;
- a `pub mod` line plus a doc stub per new module;
- one `match` arm.

The plan is also the deliberate serialisation point (P8-D21) for files that later waves or Phases 6/7 would otherwise edit concurrently: `Cargo.lock`, `command.rs`, `TABLES`, and the shared `lib.rs`/`mod.rs` files. The real logic is small: the gate, one command, one keyspace, two codes and the CI script.

## Gate report (Task 1)

| Check | Result |
|---|---|
| 1. `05-{01..20}-SUMMARY.md` | 20/20 present (05-21..27 also exist) |
| 2. 05-VERIFICATION | `status: human_needed`; all 5 human items are in the by-design set (script output below) |
| 3. Freeze | `wit/FROZEN.toml` present; tag `wit/ostro-runtime-v0.1.0` present; every P5 row in RESERVATIONS reads `exists (P5, …)` |
| 4. 05-07 meta | records.rs has `timezone`, `catchup_window_ms` and `Webhook`; state.rs has `webhooks` |
| 4b. Empty-log anchor | storage-format §10 states it (`An empty log is anchored`). `cargo test -p ostro-log --test recovery_empty_anchor`: **ok, 10 passed**. `ostro-partition --test startup_order -- empty_wal_anchors`: **ok, 1 passed** |
| 5. Clean tree | `git status --porcelain crates vendor guests sdks scripts .github docs Cargo.toml Cargo.lock` was empty (detached worktree at main) |
| 6. Baseline | `ci-local.sh --no-musl` **all steps passed** at **6984651**. Timings: clippy 79 s, build 147 s, cargo test 858 s, sleepers 107 s, failpoint suites 1253 s, campaign smoke 503 s, other steps ≤ 35 s |
| 7. Reservations | PASS (output below) |

Classification of the 05-VERIFICATION human items, from `classify-p5-human.py`:
```
status: human_needed
OK   [a native NVMe perf gate (D15)] Native Linux NVMe perf gate (D15/D16): profile A at 64 clients and at 1 client (300 runs),
OK   [b aarch64/macOS CI legs] After the first push, check that the linux-aarch64-musl (ubuntu-24.04-arm) and macos-aarch
OK   [c docker/clean-machine quickstart] Run bash examples/order-saga/quickstart.sh on a clean machine where docker info succeeds
OK   [d real-ENOSPC tmpfs leg] Optional: real ENOSPC leg on a 64 MiB tmpfs (sudo mount), with OSTRO_CAMPAIGN_STATE_DIR on
OK   [e user review D7/D8/D14 + freeze flip] User review of adopted decisions D7 (step-local RNG frozen), D8 (relaxed startup criterion
GATE-2: PASS (5 items, 0 unclassified)
```

Reservations check:
- **Before Task 2**, at 6984651: PASS. Every code ordinal was `exists`, and every `exists` row was present in the code.
- **After Task 2 and the docs flip**, at d4d150d: PASS, output below.
- The script extracts these spaces from the code: Cmd tags, StartRun fields, Derived intents, outbox kinds, TerminalStatus, journal EventKind, TABLES order and names, RejectReason, InvalidCode and MetaRecord.

```
## Cmd tags (13 in code)                max code ordinal 27; RESERVATIONS exists up to 27
## StartRun fields (23 in code)         max code ordinal 23; RESERVATIONS exists up to 23
## Derived intents (8 in code)          max code ordinal 8;  RESERVATIONS exists up to 8
## Outbox kinds (6 in code)             max code ordinal 6;  RESERVATIONS exists up to 6
## TerminalStatus (6 in code)           max code ordinal 5;  RESERVATIONS exists up to 5
## Journal EventKind (25 in code)       max code ordinal 97; RESERVATIONS exists up to 97
## Keyspaces (TABLES order) (17 in code) max code ordinal 16; RESERVATIONS exists up to 16
## RejectReason (12 in code)            max code ordinal 11; RESERVATIONS exists up to 11
## InvalidCode (24 in code)
  PLACEHOLDER 13 ReservedP5_13 | RES: spare [reserved P5]
  PLACEHOLDER 14 ReservedP5_14 | RES: spare [reserved P5]
  PLACEHOLDER 15 ReservedP6_15 | RES: BadEffectCompletion [reserved P6]   (06-02 renamed it in place afterwards)
  PLACEHOLDER 16..17 ReservedP6_16/17 | [reserved P6]
  PLACEHOLDER 18..21 ReservedP7_18..21 | [reserved P7]
  max code ordinal 23; RESERVATIONS exists up to 23
## MetaRecord (17 in code)              max code ordinal 16; RESERVATIONS exists up to 16
RESERVATIONS CHECK: PASS
```

Phase 6 and Phase 7 had landed no code ordinals at gate time. 06-01 had only committed docs (8abba5e), and Phase 7 had not started. Neither had declared a slot-16 placeholder.

## Final Phase 8 ordinal table

| Space | Ordinal | Item | Status |
|---|---|---|---|
| Cmd tag | 27 | `ScheduleFire` | exists (P8, 08-01) |
| Keyspace | 16 | `schedule_state` (`Table::ScheduleState`) | exists (P8, 08-01) |
| InvalidCode | 22 | `BadScheduleFire` | exists (P8, 08-01) |
| InvalidCode | 23 | `ScheduleOwnerMismatch` | exists (P8, 08-01) |
| InvalidCode | 13–21 | `ReservedP5_13` … `ReservedP7_21` placeholders (15 since renamed by 06-02) | placeholder rule |

Name rows added to RESERVATIONS §11:
- config sections: `[telemetry]`, `[snapshots]`, `[backup]`, `[blob_gc]`, `[triggers]`, `[guest_logs]`;
- failpoint prefixes: `snapshot::`, `trim::`, `upload::`, `restore::`, `blobgc::`, `cron::`;
- REST paths: `GET /metrics`, `/v1/schedules` (+ pause/resume/trigger), `GET /v1/runs/{id}/logs`, `GET /v1/admin/backup`;
- gRPC services: `ostro.api.v1.Schedules` and `ostro.api.v1.Webhooks`. `GetRunLogs` is to be appended to the existing Runs service, so no new service name is reserved;
- node.toml keys: `incarnation` and `restored_from`;
- object-store prefixes: `LOCK`, `manifest/`, `meta/`, `blobs/`, `p/<pppp>/{snap,seg}/`;
- data-dir entries: `snapshots/pNNNN/` and `state/.install-pNNNN`;
- the `__hook/<name>/<key>` namespace;
- the run tag `ostro.schedule`;
- the env prefix `OSTRO_BACKUP_*`.

## Dependencies (resolved versions)

| Crate | Version | Features | Declared in |
|---|---|---|---|
| opentelemetry | 0.33.1 | default | engine (API only), server |
| opentelemetry_sdk | 0.33.1 | `trace`, `logs` (no default; **no `rt-tokio`**: batch processors use SDK threads); dev: `testing` | server |
| opentelemetry-otlp | 0.33.1 | `grpc-tonic`, `trace`, `logs`, `tls-aws-lc` (no default, never zstd) | server |
| opentelemetry-proto | 0.33.1 | `gen-tonic`, `trace`, `logs` | testkit |
| tracing-opentelemetry | 0.34.0 | default | server |
| opentelemetry-appender-tracing | 0.33.1 | default | server |
| metrics-exporter-prometheus | 0.18.3 | no default features | server |
| rusty-s3 | =0.12.0 | default | objstore |
| reqwest | 0.13.5 (existing) | + `rustls`, `stream` locally | objstore |
| croner | 4.0.3 | `jiff` only (no chrono) | triggers |
| jiff02 (jiff) | 0.2.46 | `alloc`, `std` | triggers only |
| jiff | 1.0.1 (existing) | + `tzdb-bundle-always` | workspace |
| tracing-subscriber | 0.3.23 (existing) | + `json` | workspace |
| tar | 0.4.46 | default | backup, cli |
| lz4_flex | 0.13.2 | `std` | partition |
| crc32c | 0.6.9 (existing) | | partition |
| url / futures-core / futures-util | 2.5.8 / 0.3.34 / 0.3.34 | | objstore |
| hmac | 0.13 (existing) | | server, triggers |
| ostro-api (dev, `fake`) | path | | cli |

How the dependency set was verified:
- `cargo deny check`: advisories, bans, licenses and sources all ok. **No deny.toml change, and no new licence.**
- `cargo tree -d`, compared with the baseline: the only new duplicate is **jiff 0.2.46 + 1.0.1** (with jiff-core 0.1.8 + 1.0.1), which P8-D9 tolerates.
- tonic 0.14.9 and prost 0.14.6 remain single-version.
- No chrono, zstd-sys or openssl-sys in the normal tree.
- The croner + jiff02 combination resolved and passed deny, so the cronexpr fallback was not needed. **For 08-05: croner 4.0.3 as planned.**

**ostro-objstore:** it was absent at main (4a909e7/2718a6d) when Task 3 ran, so **Phase 8 08-01 created it** (40047c3) as an empty skeleton with dependencies only. Phase 6 06-02 (6c9db25) landed afterwards and did not rewrite it.

**deny.toml licence additions:** none.

## Stub-module map (file → owning plan)

| File | Owner | Contents now |
|---|---|---|
| ostro-partition `store/snapshot.rs` | 08-02 | doc only |
| ostro-partition `store/blobrefs.rs` | 08-02 | doc only |
| ostro-partition `apply/schedule.rs` | 08-07 | `apply_schedule_fire` → `Rejected(Invalid(NotImplemented))` |
| ostro-partition `leader/snapshot.rs` | 08-09 | `SnapshotConfig` (enabled, 1 h, 60 s, 256 MiB, keep 2) |
| ostro-partition `leader/cron.rs` | 08-14 | `ScheduleSpec`, `trait ScheduleSource`, `CronConfig { catchup_cap: 100 }` |
| ostro-partition `node.rs` | 08-09/08-14 | `NodeConfig.{snapshots, cron, schedule_source, backup_enabled}` |
| ostro-engine `telemetry.rs` | 08-11 (complete contract) | `set_next_ids` / `take_next_ids` + test |
| ostro-triggers `cron.rs`, `tz.rs` | 08-05 | doc only |
| ostro-triggers `webhook.rs`, `extract.rs` | 08-06 | doc only |
| ostro-backup `config.rs` | 08-01 (contract) | `BackupConfig`, `UrlStyle`, `TlsRoots`, redacting `Debug` + 2 tests |
| ostro-backup `chunk`, `manifest`, `fence`, `uploader` | 08-13 | doc only |
| ostro-backup `restore`, `archive` | 08-18 | doc only |
| ostro-backup `remote_gc` | 08-19 | doc only |
| ostro-objstore `lib.rs` | 08-03 | doc + dependency anchors |

## Task Commits

1. **Task 1: gate, RESERVATIONS names, STATE decisions** – `4a909e7`. Docs only. The first CAS lost to 06-01's 8abba5e, so the commit was rebuilt from the new main blob.
2. **Task 3: dependencies, skeletons, forbidden-crate check** – `40047c3`. Committed before Task 2 so that every commit builds. It includes `ostro-engine/src/telemetry.rs`, because the engine `lib.rs` was edited for both tasks.
3. **Task 2: format contract** – `bab9ed6`.
4. **RESERVATIONS flip to `exists`** – `d4d150d`. Docs only.
5. **Fix: ostro-backup `fail` anchor** – `fe6fb45`.

## Verification

Task 2 checks:
- `cargo test -p ostro-types --locked`: every target ok. The command suite has 35 passed, including 5 new Phase 8 tests. The routing unit test is pinned to `nightly`/16 → 2, `hourly-report`/64 → 1 and `""`/1 → 0.
- Golden diff against the base: `git diff 4a909e7 -- commands.txt | grep '^-[^-]'` is empty, with 3 lines appended. `golden_frozen.rs` still passes. Its 22-line pin was left as is, not extended, because Phase 6 appends right after it.
- `cargo test -p ostro-partition --test store --test store_digest --test rebuild_digest`: ok (23, 4 and 3 passed respectively). The `outcome` unit test passed, and `format_corpus` passed 2 (the empty new keyspace is tolerated by design).
- `cargo test -p ostro-engine --lib -- telemetry`: ok, 1 passed.

Task 3 checks:
- `forbidden-crates.sh --self-test` (4 allowed and 4 forbidden fixtures OK; also tested under LC_ALL=C) and the default mode both pass.
- `check-deny-fixtures.sh`: dup-prost, bad-licence and **ban-prometheus** are all rejected as expected.
- `cargo deny check` is ok, the chrono/zstd-sys/openssl-sys check is empty, and the grep checks pass.

Plan-level checks:
- **`ci-local.sh --no-musl` passed every step at d4d150d.** Timings: clippy 44 s, build 35 s, cargo test 671 s, sleepers 46 s, failpoint sync, failpoint suites 1335 s (now including ostro-backup), campaign smoke 314 s, deny, check-vendored, deny fixtures, forbidden crates and workflow validation.
- That run's failpoint step printed a cargo manifest warning, `unused dependency fail` (ostro-backup), which clippy `-D warnings` does not catch. fe6fb45 fixed it.
- After the fix, the builds of ostro-backup and ostro, with and without `failpoints`, are warning-free. Clippy with `-D warnings` is clean, the workspace `--all-targets` build is warning-free, and the ostro-backup failpoint suite is ok (2 passed).

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] cargo 1.100's `unused_dependencies` lint warned on every declared-but-unused Phase 8 dependency**
- **Found during:** Task 3 build.
- **Fix:** I added `use <crate> as _;` anchor modules, the existing ostro-engine pattern:
  - in ostro-server/testkit/cli/engine `lib.rs`, which are outside the plan's file list (anchor blocks only);
  - in the new crates.
- **Commits:** 40047c3, plus fe6fb45 for the optional `fail` dependency under `failpoints`.

**2. [Deviation] storage-format section number**
- §13 and §14 already existed (Phase 5 and the FROZEN text), so the Phase 8 additions are **§15**. The header line and the §1 tree are updated.

**3. [Deviation] `schedule_owner(name: &str, p: PartitionCount)`**
- It takes `PartitionCount` instead of `u16`, so P = 0 cannot divide by zero, and it matches `partition_for_key`.
- 08-07 and 08-14 should call it with `NodeConfig.partitions`.

**4. [Deviation] Task order**
- Task 3 was committed before Task 2, so every commit builds: the engine's telemetry module and its `opentelemetry` anchor share `lib.rs`.

**5. [Rule 1] Additions beyond the plan's sketch**
- `ScheduleFire::check_limits` and `overlap_policy()`.
- `OverlapPolicy::from_u32`.
- Key-shape constants `SCHEDULE_STATE`/`SCHEDULE_RUN` with decoders.
- `BackupConfig::enabled()`.
- A store test showing that the empty `schedule_state` keyspace opens empty and that its digest is stable across reopen.

## Cross-phase notes

- **06-01** landed `8abba5e` and `2718a6d` (docs) between my commits. **06-02** landed `3ca83b2` and `6c9db25` on top of `d4d150d`:
  - it renamed `ReservedP6_15` to `BadEffectCompletion` in place;
  - it appended its own golden lines after `schedule_fire_*`;
  - it added `ostro-bridge` to `FAILPOINT_CRATES` after `ostro-backup`.

  This is consistent with the coordination rule.
- The STATE.md coordination note records the ostro-objstore owner.

## Self-Check: PASSED

- The files exist: `crates/ostro-partition/src/leader/{cron,snapshot}.rs`, `crates/ostro-backup/src/config.rs`, `crates/ostro-engine/src/telemetry.rs` and `scripts/forbidden-crates.sh`.
- The commits are on main: 4a909e7, 40047c3, bab9ed6, d4d150d and fe6fb45.
