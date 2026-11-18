---
phase: 2
slug: durable-partition-log-state-machine
status: draft
nyquist_compliant: true
wave_0_complete: false
created: 2026-11-18
---

# Phase 2 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Adapted from 02-RESEARCH.md "Validation Architecture". Every [S3]/redb item is re-derived for the
> locked state store **fjall 3.1.13** (PROJECT.md Key Decisions, docs/spikes/S3-redb-durability.md).

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | `cargo test` (always available, used by CI) + **cargo-nextest** (installed in Wave 0, process-per-test isolation for failpoints) + proptest 1.12 + proptest-state-machine 0.9 + fail 0.5.1 |
| **Config file** | none yet — Wave 0 (02-01) adds `.config/nextest.toml` (profiles `default`, `crash`) |
| **Quick run command** | `cargo nextest run -p ostro-types -p ostro-log -p ostro-partition -p ostro-blob -p ostro-testkit` (fallback: `cargo test -p <same crates>`) |
| **Failpoint suite** | `bash scripts/ci-local.sh` step "failpoint suites" = for each crate in `FAILPOINT_CRATES`: `cargo test -p <crate> --features failpoints -- --test-threads=1` (ostro-wal, ostro-log, ostro-partition, ostro-blob) |
| **Crash loop (quick)** | `OSTRO_CRASH_ITERS=50 cargo test --release -p ostro-crashtest --test crash_loop -- --ignored --nocapture` |
| **Full suite command** | `cargo nextest run --workspace && cargo test --doc --workspace && PROPTEST_CASES=1000 cargo test --release -p ostro-partition --test rebuild_digest && bash scripts/ci-local.sh --no-musl` |
| **Estimated runtime** | quick ~30 s warm; full ~6–10 min (incl. ci-local without musl) |

---

## Sampling Rate

- **After every task commit:** the quick run command (or the task's own `<automated>` command)
- **After every plan wave:** quick + failpoint suite; from Wave 8 on also the crash loop (50 iterations)
- **Before `/gsd:verify-work`:** full suite + `OSTRO_CRASH_ITERS=500` crash loop + `PROPTEST_CASES=1000` rebuild digest, all green
- **Max feedback latency:** 60 seconds per task command

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 02-01-01 | 01 | 1 | STOR-05 | build + deny | `cargo build --workspace --locked && cargo deny check && ! grep -q '^redb' Cargo.toml` | ✅ | ⬜ pending |
| 02-01-02 | 01 | 1 | STOR-02, STOR-03 | unit | `cargo test -p ostro-testkit && cargo nextest --version` | ❌ W0 | ⬜ pending |
| 02-02-01 | 02 | 1 | STOR-07 | failpoint integration | `cargo test -p ostro-wal --features failpoints --test poison -- --test-threads=1` | ❌ W0 | ⬜ pending |
| 02-02-02 | 02 | 1 | STOR-07, STOR-01 | script | `bash scripts/check-vendored.sh && cargo test -p ostro-wal` | ✅ | ⬜ pending |
| 02-03-01 | 03 | 2 | STOR-09 | unit + golden | `cargo test -p ostro-types --test ids` | ❌ W0 | ⬜ pending |
| 02-03-02 | 03 | 2 | STOR-01, STOR-14 | unit + proptest | `cargo test -p ostro-types --test envelope` | ❌ W0 | ⬜ pending |
| 02-04-01 | 04 | 2 | STOR-14 | integration | `cargo test -p ostro-blob` | ❌ W0 | ⬜ pending |
| 02-04-02 | 04 | 2 | STOR-14 | failpoint integration | `cargo test -p ostro-blob --features failpoints -- --test-threads=1` | ❌ W0 | ⬜ pending |
| 02-05-01 | 05 | 3 | STOR-05, STOR-09 | integration | `cargo test -p ostro-partition --test store` | ❌ W0 | ⬜ pending |
| 02-05-02 | 05 | 3 | STOR-06 | integration | `cargo test -p ostro-partition --test store_digest` | ❌ W0 | ⬜ pending |
| 02-06-01 | 06 | 3 | STOR-01, STOR-10 | unit + golden | `cargo test -p ostro-types --test command` | ❌ W0 | ⬜ pending |
| 02-06-02 | 06 | 3 | STOR-12, STOR-13 | unit | `cargo test -p ostro-types --test command -- routing` | ❌ W0 | ⬜ pending |
| 02-07-01 | 07 | 3 | STOR-02 | integration (offline injection) | `cargo test -p ostro-log --test corruption` | ❌ W0 | ⬜ pending |
| 02-07-02 | 07 | 3 | STOR-03 | integration (exhaustive small) | `cargo test -p ostro-log --test torn_tail` | ❌ W0 | ⬜ pending |
| 02-08-01 | 08 | 4 | STOR-04, STOR-01 | integration (generic) | `cargo test -p ostro-log --test replicated_log_conformance --test append_group_commit` | ❌ W0 | ⬜ pending |
| 02-08-02 | 08 | 4 | STOR-07, STOR-08 | failpoint integration | `cargo test -p ostro-log --features failpoints --test fatal_io -- --test-threads=1 && cargo test -p ostro-log --test trim_prefix` | ❌ W0 | ⬜ pending |
| 02-09-01 | 09 | 4 | STOR-10, STOR-14 | integration (pure apply) | `cargo test -p ostro-partition --test apply_core` | ❌ W0 | ⬜ pending |
| 02-09-02 | 09 | 4 | STOR-05, STOR-11 | integration | `cargo test -p ostro-partition --test apply_core -- reads rolling_hash` | ❌ W0 | ⬜ pending |
| 02-10-01 | 10 | 5 | STOR-12 | integration (pure apply) | `cargo test -p ostro-partition --test apply_cross -- outbox inbound` | ❌ W0 | ⬜ pending |
| 02-10-02 | 10 | 5 | STOR-13 | integration (pure apply) | `cargo test -p ostro-partition --test apply_cross -- timer` | ❌ W0 | ⬜ pending |
| 02-11-01 | 11 | 5 | STOR-05, STOR-11 | integration | `cargo test -p ostro-partition --test apply_hw --test ack_after_apply` | ❌ W0 | ⬜ pending |
| 02-11-02 | 11 | 5 | STOR-07, STOR-14 | failpoint + integration | `cargo test -p ostro-partition --test blob_offload && cargo test -p ostro-partition --features failpoints --test fatal_io_state -- --test-threads=1` | ❌ W0 | ⬜ pending |
| 02-12-01 | 12 | 6 | STOR-08, STOR-09, STOR-03 | integration | `cargo test -p ostro-partition --test startup_order` | ❌ W0 | ⬜ pending |
| 02-12-02 | 12 | 6 | STOR-10, STOR-06 | integration | `cargo test -p ostro-partition --test fencing --test rebuild_basic` | ❌ W0 | ⬜ pending |
| 02-13-01 | 13 | 7 | STOR-12 | integration | `cargo test -p ostro-partition --test shuffler` | ❌ W0 | ⬜ pending |
| 02-13-02 | 13 | 7 | STOR-12 | proptest-state-machine | `cargo test --release -p ostro-partition --test shuffler_exactly_once` | ❌ W0 | ⬜ pending |
| 02-14-01 | 14 | 7 | STOR-13 | integration | `cargo test -p ostro-partition --test timers` | ❌ W0 | ⬜ pending |
| 02-14-02 | 14 | 7 | STOR-08 | integration | `cargo test -p ostro-partition --test trim_safety` | ❌ W0 | ⬜ pending |
| 02-15-01 | 15 | 8 | STOR-06 | proptest | `cargo test --release -p ostro-partition --test rebuild_digest` | ❌ W0 | ⬜ pending |
| 02-15-02 | 15 | 8 | STOR-06, STOR-02, STOR-03, STOR-07 | crash (kill -9) | `OSTRO_CRASH_ITERS=50 cargo test --release -p ostro-crashtest --test crash_loop -- --ignored --nocapture` | ❌ W0 | ⬜ pending |
| 02-16-01 | 16 | 9 | STOR-05 | bench (ignored) | `cargo test --release -p ostro-crashtest --test reopen_time -- --ignored --nocapture` | ❌ W0 | ⬜ pending |
| 02-16-02 | 16 | 9 | all STOR | doc check + gate | `test -f docs/storage-format.md && bash scripts/ci-local.sh --no-musl` | ❌ | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

### Requirement → test map (summary)

| Req | Behaviour | Test file(s) |
|-----|-----------|--------------|
| STOR-01 | envelope round trip; group commit shares fsyncs; contiguous offsets; records carry epoch/codec/crc | `ostro-types/tests/envelope.rs`, `ostro-log/tests/append_group_commit.rs` |
| STOR-02 | flip in sealed segment → open Err; non-zero flip in last segment → Err; bad envelope CRC with valid frame CRC → Err; corruption after open → read Err | `ostro-log/tests/corruption.rs`, crash loop corruption leg |
| STOR-03 | cut at every byte of the last record → open OK, LEO = previous end; zero-sector tail → cut; appends continue; double reopen idempotent | `ostro-log/tests/torn_tail.rs`, `ostro-partition/tests/startup_order.rs` |
| STOR-04 | generic conformance over `L: ReplicatedLog` (`log_start ≤ HW ≤ LEO`, stale epoch rejected, ISR={self}, `wait_committed` only after fsync) | `ostro-log/tests/replicated_log_conformance.rs` |
| STOR-05 | apply stops at HW (ManualHwLog); checkpoint cadence 100 ms / 256 batches via `persist(SyncAll)`; recovery replays from the state's own `applied_offset`; ≤ 1 point read per hot-path command | `ostro-partition/tests/{apply_hw,apply_core,store}.rs`, crash loop |
| STOR-06 | live digest == rebuilt digest + rolling hash equal, across random chunking/checkpoints/crash points and after kill -9 + deleting `state/pNNNN` | `ostro-partition/tests/{store_digest,rebuild_basic,rebuild_digest}.rs`, crash loop |
| STOR-07 | EIO on WAL fsync, ENOSPC partial write, fjall commit/persist error → waiters Err, health Failed, later appends Err, never Ok | `ostro-wal/tests/poison.rs`, `ostro-log/tests/fatal_io.rs`, `ostro-partition/tests/fatal_io_state.rs` |
| STOR-08 | trim before Ready → NotReady; floor = min(checkpoint, snapshot_floor); segments ≥ floor survive; startup order asserted | `ostro-log/tests/trim_prefix.rs`, `ostro-partition/tests/{trim_safety,startup_order}.rs` |
| STOR-09 | golden xxh3 vectors; RunId string round trip; `partition()`; P mismatch at open → Err | `ostro-types/tests/ids.rs`, `ostro-partition/tests/{store,startup_order}.rs` |
| STOR-10 | each restart's first entry is `LeaderAnnounced{e+1}`; stale epoch / lower attempt / wrong token rejected; zombie appending first still rejected | `ostro-partition/tests/{apply_core,fencing}.rs` |
| STOR-11 | `submit` returns after apply; read right after ack sees state; rejection returns `Rejected` | `ostro-partition/tests/ack_after_apply.rs` |
| STOR-12 | P=16 in-process router, random restarts + duplicate deliveries → every message applied exactly once; outbox drains | `ostro-partition/tests/{apply_cross,shuffler,shuffler_exactly_once}.rs` |
| STOR-13 | window ≤ WINDOW_MAX; refill; overdue fire after restart; duplicate `TimerFired` no-op; 100k timers bounded | `ostro-partition/tests/{apply_cross,timers}.rs` |
| STOR-14 | 300 KiB payload → Blob ref, file durable before append; orphan only on crash between; inline > 256 KiB rejected at apply; sha mismatch → Err | `ostro-blob/tests/*`, `ostro-partition/tests/{apply_core,blob_offload}.rs` |

---

## Wave 0 Requirements

- [ ] `cargo install --locked cargo-nextest` and `.config/nextest.toml` (profiles `default`, `crash`) — plan 02-01
- [ ] Workspace deps + crate manifests pre-wired (fjall replaces redb; cargo-deny green with fjall/lsm-tree/lz4_flex tree) — plan 02-01
- [ ] `crates/ostro-testkit`: ext4 tempdir helper, `corrupt::{flip_bit, zero_range, zero_sector, truncate_at}`, `proptest_cases()` — plan 02-01
- [ ] `crates/ostro-partition/clippy.toml` determinism lint — plan 02-01
- [ ] `vendor/ostro-wal`: `failpoints` feature + patches P1/P2/P3 + VENDORED.md rows; ci-local "failpoint suites" step — plan 02-02
- [ ] Golden vector file `crates/ostro-types/tests/golden/routing.txt` — plan 02-03
- [ ] `ManualHwLog` test double (`crates/ostro-partition/tests/common/`) — plan 02-11
- [ ] `crates/ostro-crashtest` (crash-target bin, kill -9 loop, reopen-time bench) — plans 02-15/02-16

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Power-loss simulation (drop un-fsynced data) with LazyFS | STOR-02, STOR-03, STOR-05 | Needs a FUSE mount and a C++ build of LazyFS; kept out of the Cargo graph | Optional/nightly: mount LazyFS over a data dir, run crash-target, `lazyfs::clear-cache`, reopen and run the crash-loop assertions. Not required for phase sign-off |
| Remote CI legs (aarch64 musl, macOS) running the Phase 2 suites | all | No git remote yet (FOUND-05 human_needed) | Push, `gh run watch`; macOS `F_FULLFSYNC` makes fsync-heavy tests slow — use small iteration counts |

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers all MISSING references
- [x] No watch-mode flags
- [x] Feedback latency < 60s per task command (crash/proptest gates run in release and are wave/phase-level)
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
