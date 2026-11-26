---
phase: 13-cluster-membership-routing-upgrades
plan: 04
subsystem: compatibility gate (CLUS-06)
tags: [compat, feature-level, finalize, hold, boot-fence, running-fence, advertiser, fail-stop, level-emulation, mutants]
requires:
  - phase: 13-cluster-membership-routing-upgrades
    provides: "13-01 compat registry (ostro_types::compat), ext tags 96-102, StoreError::NewerFormat, compat/upgrade stubs, LevelSource / ClusterStatusView contracts"
provides:
  - "ostro_cluster::compat: require/check (FeatureNotEnabled -> 409 feature_not_enabled), boot_fence/fence, check_meta_format/meta_open_error, start_running_fence, advertise_once/start_advertiser(_with), MetaLevels (LevelSource), effective_level_max, cluster_version/finalized_level"
  - "ostro_cluster::upgrade: finalize_decision (pure), upgrade_view, start_finalizer(_with) + FinalizerConfig, finalize/hold/release"
  - "mutant upgrade::mutant::emit_ungated placed and biting"
  - "HELD (not on main): partition fail-stop + reject_and_continue mutant + level-emulation leg, on branch wip/13-04-task2"
affects: [13-02, 13-05, 13-08, 13-09, 13-10, 13-12, 13-15, 13-16, 13-17, 13-18, 13-19, 13-20]
tech-stack:
  added: []
  patterns:
    - "pure decision fn + leader-only task that re-reads and CASes (Put(Cluster) expected_version)"
    - "child-process test for process-global OnceLock env (level_max)"
key-files:
  created:
    - crates/ostro-cluster/tests/upgrade_finalize.rs
    - crates/ostro-cluster/tests/compat_mutants.rs
    - crates/ostro-cluster/tests/compat.rs
  modified:
    - crates/ostro-cluster/src/compat.rs
    - crates/ostro-cluster/src/upgrade.rs
key-decisions:
  - "Task 2 (partition fail-stop) HELD on branch wip/13-04-task2: rebuild_digest's Op::Raw generator feeds random bytes and expects reject-and-continue; bytes that decode with unknown fields now fail-stop by design. Changing that test needs orchestrator OK (hard rule); proposed change is commit d2c20ad on the same branch"
  - "Liveness for finalization = meta leader's RaftHandle::sample_live(3 s); a freshly elected leader waits one window before acting; manual finalize works only on the meta leader (NotReady elsewhere)"
  - "An unreported member counts as LEVEL_MIN; it yields Waiting only when the reported members could go higher, else Stay"
  - "Any hold blocks auto and manual finalize (Held / NotReady); hold = Some(finalized), release clears; both idempotent and forwardable from any node"
  - "Undecodable tag 98 reads as LEVEL_MIN for gates/fences (safe direction) but the finalizer refuses to overwrite it"
  - "start_advertiser / start_finalizer take the node's Arc<dyn Clock> (P12 clock gate: no SystemClock in ostro-cluster); *_with variants add config"
requirements-completed: [CLUS-06]
duration: "see Metrics"
completed: 2026-11-24
---

# Phase 13 Plan 04: Compatibility gate Summary

**Feature-level gate (`compat::require`), pure finalization rule plus a leader-only auto-finalizer with hold/release/manual finalize, NodeVersion advertiser, and boot and running fences have landed and are tested on 1- and 3-voter meta groups. The partition fail-stop (Task 2) is written and green, but it is HELD on `wip/13-04-task2`: it breaks the `rebuild_digest` proptest's raw-bytes generator, and changing that test needs the orchestrator's OK.**

## Gate API (as landed)

```rust
// ostro_cluster::compat
pub const MUTANT_EMIT_UNGATED: &str = "upgrade::mutant::emit_ungated";
pub const BOOT_FENCE_CATCH_UP: Duration = 30 s;
pub fn require(view: &MetaView, f: Feature, level_max: u32) -> Result<(), FeatureNotEnabled>; // 13-01 signature
pub fn check(f: Feature, finalized: u32, level_max: u32) -> Result<(), FeatureNotEnabled>;    // pure rule
impl FeatureNotEnabled { pub const REASON = "feature_not_enabled"; pub const HTTP_STATUS = 409; pub fn detail(&self) -> String }
pub fn read_cluster_version(&MetaState) -> Result<Option<ClusterVersion>, ExtError>;
pub fn cluster_version(&MetaState) -> ClusterVersion;   // absent/undecodable -> {LEVEL_MIN, None, 0}
pub fn finalized_level(&MetaState) -> u32;
pub fn fence(finalized, level_max) -> Result<(), CompatError>;
pub fn check_meta_format(found: u8) -> Result<(), CompatError>;
pub fn meta_error(err: &MetaError) -> Option<CompatError>;  // 13-05 MetaError::NewerFormat -> NewerMetaFormat, else meta_open_error
pub fn meta_open_error(err: &str) -> Option<CompatError>;   // "unknown metadata format byte N", N > META_FORMAT
pub async fn boot_fence(meta: &MetaHandle, level_max: u32) -> Result<(), CompatError>;
pub struct MetaLevels; MetaLevels::new(view: MetaView, level_max_override: Option<u32>); impl LevelSource
pub fn effective_level_max(level_max_override: Option<u32>) -> u32;
pub struct AdvertiserConfig { refresh: 600 s, retry: 1 s }
pub enum AdvertiseOutcome { Written, Unchanged, NoMember }
pub async fn advertise_once(meta, node_id, &NodeVersion, now_ms, refresh_ms) -> Result<AdvertiseOutcome, MetaError>;
pub fn start_advertiser(meta, node_id, version, clock: Arc<dyn Clock>, stop) -> JoinHandle<()>; // 13-01 signature + clock (20ddfb0)
pub fn start_advertiser_with(meta, node_id, version, clock: Arc<dyn Clock>, AdvertiserConfig, stop);
pub fn running_fence_message(finalized, level_max) -> String;
pub fn start_running_fence(meta, level_max, on_fatal, stop) -> JoinHandle<()>;                 // 13-01 signature

// ostro_cluster::upgrade
pub const LIVE_WINDOW: Duration = 3 s;
pub fn finalize_decision(status: &ClusterStatusView, auto_finalize: bool) -> FinalizeDecision; // 13-01 signature
pub fn upgrade_view(state: &MetaState, live: &BTreeSet<u64>) -> ClusterStatusView;          // members/finalized/hold/level_range only
pub struct FinalizerConfig { auto_finalize: true, interval: 1 s, live_window: LIVE_WINDOW }
pub fn start_finalizer(meta, auto_finalize, interval, clock: Arc<dyn Clock>, stop);            // 13-01 signature + clock (20ddfb0)
pub fn start_finalizer_with(meta, FinalizerConfig, clock: Arc<dyn Clock>, stop);
pub async fn finalize(meta, now_ms) -> Result<ClusterVersion, UpgradeError>;  // meta leader only
pub async fn hold(meta, now_ms) / release(meta, now_ms) -> Result<ClusterVersion, UpgradeError>;
```

The rules:
- **Gate:** `Ok` iff `finalized >= f.introduced_at()` **and** `level_max >= f.introduced_at()`. `finalized` comes from `ClusterInfo.ext` tag 98; when the tag is absent it is `LEVEL_MIN`.
- **Finalize decision:** only non-`Removed` members count. Target = the lowest reported `level_max`. If a member is unreported:
  - `Waiting("member N has not reported its version")` when the reported members could go higher;
  - otherwise `Stay`.

  Then, in order:
  - target ≤ finalized → `Stay` (the level never decreases);
  - a hold is set → `Held{at: hold}`;
  - a member is dead → `Waiting("member N is not live")`;
  - auto-finalize is off → `Waiting("... auto-finalize is off (run `ostro cluster upgrade finalize`)")`;
  - otherwise `Finalize{to}`.
- **Write:** `Put(Cluster)` with `expected_version` = record version. Only tag 98 changes; every other ext tag is kept byte for byte (tested with `EXT_TAG_TEST`). `StaleVersion` re-reads and re-evaluates (5 attempts).
- **Finalizer task:** runs on every node and acts only while it is meta leader. After an election it waits one `live_window` before acting, unless it is the only voter/learner. A single node finalizes as soon as its NodeVersion is advertised. On `FinalizeDecision::Finalize`, `finalized_at_ms` comes from the injected `Clock`.
- **Advertiser:**
  - Read-modify-write with a CAS on the member version.
  - Writes only when a field other than `reported_at_ms` changed, or when the stored report is older than `refresh`.
  - Skips a missing member record (the self-upsert creates it) and a `Removed` tombstone.
  - Re-checks after every applied meta change, so a rewrite that dropped tag 96 is repaired.
- **Boot fence** also refuses when `MetaView::newer_format()` is set (13-05), and maps `MetaError::NewerFormat` from `wait_ready` through `meta_error`.
- **Running fence:** checks at start, after every applied meta change and after every raft status change. It calls `on_fatal(msg)` once, and the task ends, when any of these holds:
  - `finalized > level_max`;
  - `MetaView::newer_format()` is set;
  - `RaftStatus.fatal` is set (meta apply halted on unknown input, 13-05).

## Message texts

- Boot fence, downgrade after finalization: `data finalized by a newer Ostro (level 2, this binary supports up to 1); upgrade this binary` (`CompatError::FinalizedAbove`, 13-01 text).
- Boot fence, skipped minor: `data at level F is older than this binary supports (minimum 1); upgrade through a release supporting level F first` (`FinalizedBelow`, `through = "a release supporting level F"`).
- Newer META_FORMAT: `metadata written by a newer Ostro (format N); upgrade this binary` (`NewerMetaFormat`, via `meta_open_error`).
- Running fence: `running fence: data finalized by a newer Ostro (level 2, this binary supports up to 1); upgrade this binary; stopping`.
- Gate: `FeatureNotEnabled` Display `feature RepinRun needs level 2; the cluster is finalized at 1`. `detail()`: `repin_run needs cluster feature level 2 (finalized 1); finish and finalize the upgrade`. Reason `feature_not_enabled`, HTTP 409.
- Finalize: `cannot finalize: member N is not live` / `... has not reported its version` / `an upgrade hold keeps the cluster at level L; run `ostro cluster upgrade release` first` / `liveness is known only on the meta leader`.
- Partition fail-stop (HELD branch): `written by a newer Ostro; upgrade this binary: unknown command tag` | `...: command carries fields unknown to this version` | `...: command repin_run needs feature level 2 (this binary supports up to 1)`.

## Partition fail-stop (Task 2): HELD on `wip/13-04-task2`

**Detection path:** re-encode identity. `decode(bytes).encoded_len() == bytes.len()` holds for every `commands.txt` golden line and every command in the v0.1 format-corpus logs (`reencode_identity_over_goldens`, `reencode_identity_over_the_format_corpus`, run first and green). The D-P13-6 wire-walk fallback is not needed.

**Rule in `apply/mod.rs::dispatch`** (after a successful decode):
- non-empty bytes with no known oneof variant → `NewerFormat("unknown command tag")`;
- otherwise a re-encode length mismatch → `NewerFormat("command carries fields unknown to this version")`;
- otherwise a `Cmd` whose gated feature (`feature_for_cmd_tag`) is above `ostro_types::compat::level_max()` → `NewerFormat("command repin_run needs feature level 2 ...")` (bites only with `level-emulation`);
- empty bytes → `Rejected(Invalid(Empty))` and undecodable bytes → `Rejected(Invalid(Decode))`, both unchanged.

The mutant `upgrade::mutant::reject_and_continue` (`MUTANT_REJECT_AND_CONTINUE`) maps every unknown case back to `Invalid(Empty)`.

**Halt mechanism (existing apply machine, unchanged):**
- `apply_command` returns `Err` before the cursor or hash moves.
- `Machine::apply_once` maps it to `Err("apply failed: written by a newer Ostro; upgrade this binary: ...")` before `b.commit`, so the state batch is dropped, `applied_offset` stays put and `applied_tx` is not advanced.
- `run_inner` returns, and `Machine::fail` publishes `PartitionHealth::Failed { reason }`. This is terminal until the partition is reopened, with no retry loop. Pending and later submits fail with `PartitionError::Failed`.
- `/healthz` and the replication status read that health, so the partition shows as failed with that reason.
- A reopen replays the same entry and fails again. In the proptest shrink, `Node::open` then timed out waiting for Ready.

**Tests (all green on the branch):**
- `ostro-partition/tests/apply_newer_format.rs`, 5 tests + 1 failpoints test:
  - re-encode identity ×2;
  - unknown tag / top-level field / nested field / bare unknown field all fail-stop with the cursor, hash and digest unchanged;
  - empty and undecodable input keep their rejections;
  - machine halt: health `Failed` with the message, `applied == receipt.base`, and later submits fail;
  - `reject_and_continue_mutant_bites`.
- `ostro-repl-conformance/tests/level_emulation_apply.rs`, 2 tests: the parent (level 2) applies `RepinRun` without a fail-stop. A child process of the same binary with `OSTRO_TEST_LEVEL_MAX=1` sees `level_max() == 1` and fail-stops with `repin_run` in the message.
- `format_corpus` green; `p13_format` and `apply_core` green.

**Why it is held:** with Task 2, `PROPTEST_CASES=128 cargo test -p ostro-partition --test rebuild_digest` **fails** in `live_state_equals_rebuilt_state` at `rebuild_digest.rs:487`, with `raw apply: Failed("apply failed: written by a newer Ostro; upgrade this binary: unknown command tag")`. The `Op::Raw` arm draws `vec(any::<u8>(), 0..24)` and asserts the partition applies it. Random bytes that decode as a protobuf with unknown fields now fail-stop, as D-P13-6 intends. Making that test pass means changing its input generator. The hard rule says to report instead.

**Proposed fix** (commit `d2c20ad` on the branch, not on main):
- Random draws that decode get a leading `0x07` (field 0, never decodable), so `Op::Raw` stays on the reject-and-continue path.
- The digest oracle (live == rebuilt) is untouched.
- With it, `rebuild_digest` at 128 cases passes: 8 passed, 1172 s.

To land, the orchestrator approves `d2c20ad` (or another generator change), then cherry-picks `3424bb6` + `d2c20ad` onto main and runs ci-local.

## Task Commits

1. `88f4c78` feat(13-04): feature-level gate + finalizer (compat.rs, upgrade.rs, upgrade_finalize.rs, compat_mutants.rs; compat.rs also carries the Task 3 implementation)
2. Task 2: `3424bb6` feat(13-04): fail-stop on newer partition commands, plus `d2c20ad` (proposed generator change), **on `refs/heads/wip/13-04-task2` only**
3. `08bd9be` feat(13-04): NodeVersion advertisement + boot/running fences (tests/compat.rs)
4. `20ddfb0` fix(13-04): injected clock only (P12 clock gate) + 13-05 meta fence signals

## Verification

- `cargo test -p ostro-cluster --test upgrade_finalize`: 6 passed (decision table, upgrade_view defaults, single-voter gate→finalizer with an unknown tag preserved, hold/release/manual finalize, auto-finalize off, 3 voters over mTLS: dead member blocks, follower refused, finalize after restart, all voters see 2).
- `cargo test -p ostro-cluster --features failpoints --test compat_mutants -- --test-threads=1`: 1 passed (emit_ungated bites, holds again after remove).
- `cargo test -p ostro-cluster --test compat`: 6 passed (boot fence + mtime/size snapshot, unfinalized/blank allow level 1, 3-voter fence, running fence once, advertiser RMW/refresh/restore, MetaLevels).
- `cargo test -p ostro-cluster --lib`: 27 passed (4 new compat unit tests).
- `cargo clippy -p ostro-cluster -p ostro-partition -p ostro-meta --all-targets --locked -- -D warnings`: clean. `cargo fmt --check`: clean.
- Plan-level `bash scripts/ci-local.sh --no-musl --no-polyglot` (nice 15, slot-3, separate worktree): **all steps passed** at `20ddfb0` (cargo test 6117 s, failpoint suites 6838 s, replication mutant matrix 243 s, server campaign smoke 987 s, one log path + P12 grep gates PASS; external PG/MySQL/S3, polyglot and musl SKIP). An earlier run at `08bd9be` was stopped after the orchestrator reported that the P12 clock gate flagged `SystemClock` (fixed in `20ddfb0`). Before it was stopped, that run had one red in cargo test: `ostro-raft-driver` `driver::membership_tests::fatal_apply_stops_before_applied` (a 13-05 test at `membership_tests.rs:281`). It passed in the green run, so it looks load-flaky; 13-05 should look at it.

## Deviations from Plan

1. **[Rule 4 / hard rule] Task 2 held** (see above). Its tests and `must_haves` truth 5 and the `reject_and_continue` half of truth 6 are therefore not on main.
2. **[Rule 3] Level-emulation leg moved** to `ostro-repl-conformance/tests/level_emulation_apply.rs`. `ostro-types/level-emulation` is reachable only through the failpoints chains of ostro-server / ostro / ostro-repl-conformance. ostro-partition's `failpoints` does not chain it, and Cargo files are 13-01-only (STATE "Phase 13 shared files"), so `cargo test -p ostro-partition --features failpoints` cannot enable it. Process-global `level_max()` is handled with a child-process test.
3. **[Adaptation] `finalize_decision` keeps the 13-01 signature** `(&ClusterStatusView, bool)` instead of the plan's `(members, live, cluster_version, target, auto)`. The target is derived from the members (lowest reported `level_max`). `upgrade_view` builds the needed part of the view from meta plus the leader's live set.
4. **[Rule 1, orchestrator report] Clock injection:** `start_advertiser` and `start_finalizer` now take `clock: Arc<dyn Clock>`, a change to the 13-01 signatures (they had no callers on main). Their first versions defaulted to `SystemClock`, which the P12 clock gate (`scripts/p12-grep-gates.sh`) rejects; fixed in `20ddfb0`. The leadership-tenure `tokio::time::Instant` is marked `clock-exempt`. The `*_with` variants also take a config.
4a. **[Rule 2] 13-05 handover wired:** `MetaError::NewerFormat`, `MetaView::newer_format()` and `RaftStatus.fatal` feed the boot and running fences.
5. **Task split:** the Task 3 implementation (advertiser and fences) landed in the Task 1 commit together with the gate, because all of it lives in `compat.rs`. The Task 3 commit carries its tests.
6. **Boot fence catch-up** uses `MetaHandle::wait_ready()` (leader known, `applied >= commit`) bounded by 30 s, not a `join_timeout` parameter (the fixed signature has none). On timeout it fences on the local view.
7. **Worktree/target:** scratchpad worktree and `target/slot-3` per the executor brief, instead of `/tmp/ostro-wt-13-04` / `/tmp/ostro-target-13-04`.
8. **ci-local with `--no-polyglot`** (orchestrator instruction).

No ci.yml change is needed. FAILPOINT_CRATES already runs `ostro-cluster`, `ostro-partition` and `ostro-repl-conformance` with `--features failpoints`, so `compat_mutants`, the partition mutant test and the level-emulation test run there once Task 2 lands.

## Handover notes

- **13-02 (server wiring):**
  - call `compat::boot_fence(&meta, level_max)` after meta is ready and before `Node::open`;
  - map a `MetaHandle::start_with` error through `compat::meta_error` (or a `probe_meta` error string through `compat::meta_open_error`) to keep the newer-format message;
  - `level_max = compat::effective_level_max(ServerOptions.level_max_override)`;
  - start `start_advertiser(meta, node_id, NodeVersion{ostro_version: CARGO_PKG_VERSION, level_min: LEVEL_MIN, level_max, wasmtime, event_codec_ver, wit_lines, ..})` after `members::upsert_self`;
  - start `start_running_fence(meta, level_max, on_fatal = log + process exit non-zero, stop)` and `start_finalizer(meta, cfg.cluster.auto_finalize, 1 s, clock, stop)`; `start_advertiser` also takes the node's clock;
  - `MetaLevels::new(meta.view().clone(), override)` is the `LevelSource`.
- **ApiError mapping:** `FeatureNotEnabled` → FAILED_PRECONDITION / 409, reason `FeatureNotEnabled::REASON`, message `detail()`. ostro-cluster has no ostro-api dep, so the mapping goes in ostro-server.
- **Every gated emitter** (13-10 AlterReplicas, 13-07/13-08 WitRequirement, 13-15 Retired, 13-17 RepinRun, 13-10 Reassignment ext): call `compat::require(meta.view(), Feature::X, level_max)` before proposing.
- **13-09 / 13-16:** `upgrade::finalize` must run on the meta leader (it returns NotReady elsewhere), so the 13-12 router should forward `upgrade_finalize` to the meta leader. `hold` / `release` work anywhere. `upgrade_view` gives members / finalized / hold / level_range for `UpgradeStatus`.
- **13-05:** the MetaSm fail-stop half (D-P13-6, plan check B6) is independent of this plan. With `level-emulation`, the meta half should mirror the partition rule using `feature_for_meta_variant`.
- **13-18:** both 13-04 mutant sites exist: `upgrade::mutant::emit_ungated` in `ostro-cluster/src/compat.rs` (on main) and `upgrade::mutant::reject_and_continue` in `ostro-partition/src/apply/mod.rs` (on the held branch).

## Self-Check

PASSED: commits 88f4c78, 08bd9be, 20ddfb0 are on main; 3424bb6 and d2c20ad are on `refs/heads/wip/13-04-task2`; every key file listed exists at 20ddfb0.
