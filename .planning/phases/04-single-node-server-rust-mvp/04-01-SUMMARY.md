---
phase: 04-single-node-server-rust-mvp
plan: 01
subsystem: metadata
tags: [raft, metadata, cas, postcard, meta-01, meta-02, meta-03]
requires:
  - phase: 01-foundation-vendoring-spikes
    provides: vendored ostro-raft-driver (RaftDriver, durable RaftLogStore) + ostro-meta NullOutbox smoke
provides:
  - "ostro_meta::records: Versioned<T> + 14 record kinds (ClusterInfo, Member, PartitionAssignment, ModuleRecord, WorkflowVersion, ActiveVersion, QueueConfig, Schedule, ApiToken, Tenant, SealedSecret, Grant, Connection, EgressPolicy)"
  - "ostro_meta::command: MetaKind::ALL, MetaKey, MetaRecord (+ natural key(), #[doc(hidden)] sample()), MetaCommand {InitCluster, Put, Delete, SetPartitionLeader}, MetaResult, [META_FORMAT=1] || postcard encode/decode"
  - "ostro_meta::MetaState::apply (validate-then-mutate CAS) + accessors cluster/member/partition(s)/module(s)/workflow(s)/active/queue(s)/token/token_key/get"
  - "ostro_meta::{MetaSm, MetaView}: StateMachine with deterministic snapshot; read/applied/subscribe"
  - "ostro_meta::{MetaHandle, MetaError}: start (campaign immediately), propose (OutcomeUnknown retry), init_cluster, view, is_leader, raft, async stop"
affects: [04-17 server startup (MetaHandle::start + init_cluster + MetaLeadership), 04-20 meta_stale e2e, Phase 12 cluster membership/transport]
tech-stack:
  added: []
  patterns:
    - "Metadata commands are CAS-guarded at apply: create = expected None, update = exact version, partition version == epoch"
    - "Single-voter Raft: campaign() right after RaftDriver::start, then wait is_leader + applied >= commit before serving reads"
key-files:
  created:
    - crates/ostro-meta/src/records.rs
    - crates/ostro-meta/src/command.rs
    - crates/ostro-meta/src/state.rs
    - crates/ostro-meta/src/sm.rs
    - crates/ostro-meta/src/handle.rs
    - crates/ostro-meta/tests/meta_versions.rs
    - crates/ostro-meta/tests/meta_records.rs
    - crates/ostro-meta/tests/meta_persist.rs
  modified:
    - crates/ostro-meta/Cargo.toml
    - crates/ostro-meta/src/lib.rs
    - Cargo.lock
key-decisions:
  - "Put must use the record's natural key (MetaRecord::key()); mismatched kind or identity -> Invalid"
  - "Put with Some(v) on an absent key -> NotFound (not StaleVersion); Delete returns Ok{version: deleted version}"
  - "Partition records: version IS the epoch. Put on a partition sets version = record.epoch, which must exceed the current epoch (else StaleVersion). SetPartitionLeader: lower epoch -> StaleVersion, equal epoch with identical leader/isr -> idempotent Ok, equal epoch with different leader/isr -> Conflict, NotFound if the partition was never assigned"
  - "InitCluster repeat with the same partition count returns Ok without touching anything (cluster id and token key of the first boot win); the cluster record's partition count is immutable (also enforced on Put)"
  - "InitCluster's founding Member has addr \"\" (the plan's InitCluster fields carry no address; later phases Put the member)"
  - "Await-token key = SealedSecret \"__ostro/token-key\", ciphertext = raw 32 bytes, key_id \"plaintext-v0\" until Phase 14 sealing"
  - "MetaHandle::start waits for is_leader AND applied >= commit, so restart reads see every durable record; MetaHandle::stop is async (driver join on spawn_blocking)"
  - "META-01 left Pending in REQUIREMENTS.md: the single voter is done, but 'Ostro's own transport' is still NullOutbox (multi-node transport comes with clustering)"
patterns-established:
  - "MetaKind::ALL + MetaRecord::sample(kind, id, generation) for table-driven tests that must cover every metadata kind"
requirements-completed: [META-02, META-03]
duration: 25min
completed: 2026-11-18
---

# Phase 4 Plan 01: ostro-meta Records, Versioned CAS, MetaSm and MetaHandle Summary

**A single-voter metadata Raft group that campaigns as soon as it starts (leader in 29-63 ms instead of about 2.85 s). It holds all 14 META-02 record kinds under versioned CAS at apply. Partition epochs never regress, and state survives a restart byte for byte through deterministic postcard snapshots and log replay.**

## Performance

- **Duration:** ~25 min
- **Started:** 2026-11-18T20:35:00Z
- **Completed:** 2026-11-18T21:00:00Z
- **Tasks:** 3 (TDD: RED + GREEN commits each)
- **Files modified:** 11

## Accomplishments

- **META-02:** every record kind is defined, persisted and round-trips propose -> snapshot -> restore with equal state. Snapshots of equal states are byte-identical.
- **META-03:** for every one of the 14 kinds, the table-driven test confirms that a stale or future `expected_version`, a create of an existing key, and a lower partition epoch are all rejected and leave the state unchanged. A proptest of random CAS interleavings confirms that the final version equals the number of successful puts and that each key's chain runs 1, 2, 3, ...
- **META-01 (single-voter part):** `MetaHandle::start` measured 62.9 ms on first boot and 29.3 ms on restart. The test asserts < 2 s. Proposals give read-your-writes, and records and versions survive a restart in the same directory.

## Task Commits

1. **Task 1: records, keys/commands/results, versioned apply.** RED `259a76f`, GREEN `fbfc0a9`
2. **Task 2: MetaSm snapshot/restore + record round-trip suite.** RED `367d2f3`, GREEN `b01572c`
3. **Task 3: MetaHandle + persistence test.** RED `b7df5c5`, GREEN `8cd88d4`

## Files Created/Modified

- `crates/ostro-meta/src/records.rs`: `Versioned<T>`, the 14 record structs, and the `TOKEN_KEY_SECRET` / `CLUSTER_FORMAT` consts.
- `crates/ostro-meta/src/command.rs`: `MetaKind`, `MetaKey`, `MetaRecord`, `MetaCommand`, `MetaResult`, `DecodeError`, and encode/decode with a format byte.
- `crates/ostro-meta/src/state.rs`: `MetaState` (one BTreeMap per kind) and its deterministic `apply`.
- `crates/ostro-meta/src/sm.rs`: `MetaSm` (StateMachine) and `MetaView`.
- `crates/ostro-meta/src/handle.rs`: `MetaHandle` and `MetaError`.
- `crates/ostro-meta/tests/meta_versions.rs`, `meta_records.rs`, `meta_persist.rs`: the three suites (11 tests incl. 2 proptests).
- `crates/ostro-meta/Cargo.toml` + `Cargo.lock`: serde, postcard, thiserror and tracing (dev: proptest), all from existing workspace pins. Only the ostro-meta stanza of the lock changed.

## Decisions Made

See `key-decisions` in the frontmatter. The main choices:
- The partition version is the epoch.
- `Put` must use the record's natural key.
- A repeated `InitCluster` with the same partition count is an idempotent no-op.
- `start` waits for leadership and catch-up before it returns.

## Deviations from Plan

None that change behaviour. Small additions beyond the listed API:
- `MetaKind` enum plus `MetaRecord::key()` and `MetaRecord::sample()` (the "ALL_KINDS helper/test constructors").
- `MetaState::get(&MetaKey)` and more accessors (`member(s)`, `partitions`, `modules`, `queue(s)`, `token`).
- `MetaView::applied()`, `MetaHandle::raft()`.
- `MetaError::{Start, Rejected}` variants.

`stop` is `async`.

Verification ran in a scratch worktree at HEAD with only the ostro-meta files copied in, because the main checkout's workspace was briefly broken by 04-02's in-progress root `Cargo.toml` (`workspace.dependencies.config` missing). The Cargo.lock hunk was taken from that worktree and applied with `git apply --cached`, so only the ostro-meta dependency list changed.

## Issues Encountered

- `cargo clippy` flagged `g % 2 == 0` (`manual_is_multiple_of`) in the test constructor. Fixed before the Task 1 commit.

## Verification

- `cargo test -p ostro-meta --locked`: meta_persist 1, meta_records 3, meta_versions 6, single_voter 1. All green.
- `cargo clippy -p ostro-meta --all-targets --locked -- -D warnings`: clean.
- `cargo build -p ostro-meta --locked`: 0 warnings. `cargo doc -D warnings` and `cargo fmt --check`: clean.
- No `HashMap`, `SystemTime` or `Instant` in `crates/ostro-meta/src`.

## Next Phase Readiness

- 04-17 can call `MetaHandle::start(<data>/meta, node_id)` then `init_cluster(uuid, node_id, P, now_ms, key)` and read `view().read(|s| s.token_key())`. `MetaLeadership` can read `s.partition(p).map(|a| a.value.epoch)`.
- The old `single_voter.rs` smoke still runs without campaign (about 5 s). It stays as a regression check on the raw driver.

## Self-Check: PASSED
