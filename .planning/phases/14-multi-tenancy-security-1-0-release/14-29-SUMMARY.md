---
phase: 14-multi-tenancy-security-1-0-release
plan: 29
subsystem: bottomless backup (cluster meta head, cut search) + partition replay observer
tags: [backup, restore, cluster, meta-head, consistent-cut, replay, BACK-05]
requires:
  - phase: 14-01b
    provides: "Phase 14 seam (no 14-29 stub); ostro-backup/ostro-partition manifests unchanged (14-01 recorded none)"
  - phase: 12-15
    provides: "cluster-mode uploader (per-partition heads, epoch fencing, multi-member branch without manifest)"
  - phase: 08-13/08-18
    provides: "meta_key scheme, put_immutable, Pitfall 1 check semantics (restore.rs check_consistency)"
provides:
  - "ostro_backup::meta_head: meta/head JSON pointer + term-fenced conditional put + meta retention helper"
  - "Uploader::with_meta_leader(watch::Receiver<Option<u64>>) and the multi-member meta branch (meta snapshot + meta/head after the round's partition uploads)"
  - "ostro-server services/uploader.rs: meta leader/term watch from MetaHandle::raft_status()"
  - "ostro_partition::replay_observer: ReplayObserver, CursorSample, CursorRecorder(s), replay_with_observer; NodeConfig::replay_observer (Inspect only)"
  - "ostro_backup::cluster_cut: StepFn, CursorSteps, steps_from_replay, check, search (greatest fixpoint), CutError::NoConsistentCut"
affects: [14-30, 14-27]
tech-stack:
  added: []
  patterns:
    - "term-fenced pointer object: If-Match on the seen etag, re-read + fence check + one retry on a failed precondition"
    - "observer replay = the apply machine's loop (apply_command per envelope, one ApplyBatch per read chunk, meta_update commit) with a read-only callback, run before the machine starts"
    - "cut search as a greatest fixpoint of forced lowerings over monotone step functions"
key-files:
  created:
    - crates/ostro-backup/src/meta_head.rs
    - crates/ostro-backup/src/cluster_cut.rs
    - crates/ostro-backup/tests/meta_head.rs
    - crates/ostro-backup/tests/cluster_cut.rs
    - crates/ostro-partition/src/replay_observer.rs
    - crates/ostro-server/tests/cluster_meta_head.rs
  modified:
    - crates/ostro-backup/src/uploader.rs
    - crates/ostro-backup/src/lib.rs
    - crates/ostro-server/src/services/uploader.rs
    - crates/ostro-partition/src/lib.rs
    - crates/ostro-partition/src/node.rs
key-decisions:
  - "meta/head = {format:1, term, index, key, sha256 (hex), at_ms, members, partitions}; no ordinal, no META_FORMAT bump, snapshot bytes = ostro_meta::encode_snapshot under meta_key(index)"
  - "Leadership accessor: MetaHandle::raft_status() -> is_leader.then_some(term), re-read on every meta applied-index change and a 1 s tick (META_LEADER_TICK)"
  - "Meta failures in cluster mode never fail the round nor fence the uploader (logged, failures_total{kind=meta}, retried next round); different bytes under meta_key(index) is an Io error, not an uploader-wide fence"
  - "A meta leader that leads no partition still uploads meta (the led-empty early return got a meta-only path)"
  - "Meta retention keeps the newest remote_snapshots_keep snapshots at or below the head index and never deletes the meta snapshot a Phase 8 manifest/current still names"
  - "Observer API passes &Cursor (CursorMeta has no inbound_seen); it is hooked in PartitionStart::run_observed (Inspect only) — no fallback needed"
  - "NoConsistentCut names the partition whose floor the first impossible lowering would cross (Vec for 14-30)"
requirements-completed: [BACK-05]
metrics:
  duration: ~3h
  completed: 2026-11-27
---

# Phase 14 Plan 29: Whole-cluster restore primitives Summary

The metadata leader of a multi-member cluster now uploads meta snapshots and moves a term-fenced `meta/head`; an additive Inspect-mode replay observer reports every partition's cross-partition cursors per offset; and a proven greatest-fixpoint search turns per-partition heads into one Pitfall-1-consistent cut. 14-30 assembles these into `ostro restore` of a cluster bucket.

## meta/head as landed (Task 1)

- Object key `meta/head`, JSON: `{"format":1,"term":T,"index":I,"key":"meta/<I 20d>.msnap","sha256":"<64 hex>","at_ms":…,"members":M,"partitions":P}` (`meta_head::encode/decode`, `META_HEAD_FORMAT = 1`).
- `meta_head::get(store) -> Option<(MetaHead, etag)>`; `meta_head::put(store, new, seen_etag) -> MetaHeadPut::{Written(etag), Fenced(cur), Superseded(cur, etag), Conflict(cur)}`. `If-Match` on the seen etag (`If-None-Match: *` when none). On a failed precondition: re-read; `cur.term > new.term` -> `Fenced` (gauge `ostro_backup_meta_fenced = 1`); same term with `cur.index >= new.index` -> `Superseded`; otherwise one retry with the fresh etag (`Conflict` if that fails too). `may_replace(cur, new)` = higher term, or same term and higher index. Without conditional PUT: re-read + fence check right before an unconditional PUT (documented, weaker, as for partition heads). `seen_etag` must be an etag the caller wrote or checked (documented).
- `Uploader::with_meta_leader(rx)`: `Some(term)` while this node leads meta. Multi-member branch (`!cl.single`), after partition heads, floors and partition retention: if leading, not fenced at this term (`Uploader::meta_fenced()`), and `view.applied()` differs from the last pointed index, then `put_immutable(meta_key(index))`, then `meta/head` with `at_ms = clock.now_ms()` (>= the round's `cut_at_ms`), then meta retention (`meta_head::retention_victims`). `RoundReport` gained `meta_head: Option<MetaHead>` and `meta_fenced: bool`. A fenced writer stops at that term and resumes only at a higher term. Single-member rounds are untouched (the meta filter `is_none_or(|c| c.single)` and the manifest path are byte-identical).
- Server: `services/uploader.rs` builds `meta_leader_watch(Arc<MetaHandle>, shutdown)` (task: `select!{shutdown, view.subscribe().changed(), 1 s tick}` -> `send_if_modified(raft_status().is_leader.then_some(term))`) and passes it with `.with_meta_leader(..)`. `UploaderClusterDeps` unchanged (two fields).

## Replay observer (Task 2; files touched in ostro-partition)

- `crates/ostro-partition/src/replay_observer.rs` (new): `trait ReplayObserver: Send { fn start(&mut self, at: Offset, cursor: &Cursor) /* default = after_apply */; fn after_apply(&mut self, at: Offset, cursor: &Cursor); }` (`at = cursor.next`, a cut position); `type ReplayObserverFactory = Arc<dyn Fn(PartitionId) -> Option<Box<dyn ReplayObserver>> + Send + Sync>`; `CursorSample {at, next, truncated, seen}`; `CursorRecorder` (samples on start and on change only) and `CursorRecorders` (per-partition, `factory()`, `samples(p)`); `replay_with_observer(store, log, upto, max_chunk_bytes, observer) -> Result<Cursor, String>`: the machine's loop (`log.read`, `apply_command` per envelope into one `ApplyBatch` per read chunk, `meta_update` commit, actions discarded) with the observer called after each command, then `store.checkpoint`.
- `crates/ostro-partition/src/lib.rs`: `pub mod replay_observer;` (one line + comment).
- `crates/ostro-partition/src/node.rs`: `NodeConfig::replay_observer: Option<ReplayObserverFactory>` (default `None`); `Node::open` step 4 passes `f(p)` only in `TermMode::Inspect`; `PartitionStart::run` now delegates to `run_observed(observer)` (roles.rs's caller unchanged), which in the Inspect branch, after `become_leader` raised the HW, replays `[applied, HW)` through the observer and re-reads the store meta, so the apply machine starts caught up. No apply code path, digest, `TABLES`, OSNP or format change; `rebuild_digest` and `format_corpus` green. The 14-DECISIONS fallback (chunk-boundary checkpoints + bisect) was **not** needed.

## Cut search (Task 2)

- `cluster_cut::StepFn(Vec<(offset, value)>)` (value at `c` = last entry with offset <= c, else default: next 1, truncated/seen 0), `largest_where`, `push_change`; `CursorSteps {floor, end, next, truncated, seen}`; `steps_from_replay(samples, floor, end)` (entries only on change; absent key = default); `check(parts, cut)` = Pitfall 1 for every ordered pair (same messages as restore.rs `check_consistency`).
- `search(parts) -> Result<Cut {offsets, lowered: Vec<(p, from, to, Lowering)>}, CutError>`: start at every `end`; repeat over ordered pairs until stable: rule 1 (`seen_B[A](c_B) >= next[A->B](c_A)`) lowers `c_B` to the largest offset in `[floor_B, c_B]` with `seen_B[A] < next[A->B](c_A)` (`Lowering::Unsent{from}`); rule 2 (`truncated[A->B](c_A) > seen_B[A](c_B)`) lowers `c_A` to the largest offset in `[floor_A, c_A]` with `truncated <= seen` (`Lowering::Untruncated{to}`); no such offset -> `CutError::NoConsistentCut{partitions}`; `floor > end` refused up front. Monotone cursors make every lowering forced, so the result dominates every consistent cut below the heads (proof sketch in the module docs).
- Proptest statistics: `search_is_consistent_maximal_and_bounded` 256 cases (`PROPTEST_CASES=256`), generated causal histories of 2-6 partitions (up to 48 events: sends, in-order deliveries, truncations to what the receiver has seen, no-ops), per-partition lag 0-100 % and in half the cases random floors. Deterministic companion `search_statistics` (512 cases, `TestRunner::deterministic`): `cases 512, lowered_cases 94, lowerings 105, no_cut 5, brute_forced 506, partitions 2116`. Properties: consistent; within `[floor, end]`; maximal (raising any partition by 1 or to its next step breaks consistency); greatest (dominates every consistent cut of the box, brute force when the box has <= 40 000 points); equal to the heads at zero lag; `NoConsistentCut` only with floors and only when brute force finds no consistent cut. Unit cases: each rule, a two-step cascade, floor in the way, floor above end, `steps_from_replay`.
- Real replay (`observed_replay_matches_the_store_and_leaves_the_digest_unchanged`): a 3-partition node runs 6 runs x 4 rounds of cross-partition signals until every outbox is delivered and truncated; state dirs are deleted in two copies; Inspect replay with and without `CursorRecorders`: digest, state hash and applied equal (and equal the live state); step functions at `end` equal `read_meta()`'s `outbox_next_seq` / `outbox_truncated` / `inbound_seen`; `search` keeps the settled heads; cutting p0 early forces lowerings and stays consistent.

## Evidence

- Task 1 verify: `meta_head` 10 passed; `cluster_meta_head` 1 passed (3-node loopback cluster: head at the meta leader's term, snapshot decodes with the cluster id, members 3, no `manifest/current`; `transfer_leader` -> head moves to the new leader's higher term and index and stays there); existing suites `uploader` 9, `cluster_head` 3, `cluster_repair` 9, `restore` 5 (+1 ignored), `cut_consistency` 2, `cluster_mutants` 1 (with `--features failpoints`, see deviations).
- Task 2 verify: `PROPTEST_CASES=256 cluster_cut` 9 passed; `PROPTEST_CASES=128` ostro-partition `rebuild_digest` 8 passed, `format_corpus` 2 passed; `scripts/p12-grep-gates.sh` clean; CLOCK_GATE_SCOPED clean (only `clock.now_ms()` and a tokio interval added).
- `cargo clippy -p ostro-backup -p ostro-server -p ostro-partition --all-targets -- -D warnings` clean.
- Plan gate: incomplete (cluster withdrawn), see "Plan gate" below.
- `cargo fmt --all -- --check` clean after 155d891 (rustfmt only, no code change).

## Deviations from Plan

1. **[Rule 3] Observer signature takes `&Cursor`, not `&CursorMeta`.** `CursorMeta` holds `outbox_next_seq`/`outbox_truncated` but `inbound_seen` lives on `Cursor`; the observer needs all three. Also added a defaulted `start()` so the step functions carry the replay's starting state.
2. **[Rule 2] Meta errors are non-fatal in cluster mode.** `put_immutable` reports different bytes as `FenceError::Fenced`, which in the round would fence the whole uploader (partition uploads too); for meta it is mapped to an Io error and, like store errors, logged and retried next round without failing the round.
3. **[Rule 2] Meta-only path for a meta leader leading no partition.** The cluster-mode early return (`led.is_empty()`) would have kept such a leader from ever uploading meta; it now opens the session and runs only the meta step (still `barrier_closed = true`).
4. **Real-replay workload uses cross-partition signals only** (no child runs): signals already drive all three cursors (sends, deliveries, truncations) on every partition pair; child starts ride the same outbox.
5. **Task 1 verify literal:** `cluster_mutants` is `#![cfg(feature = "failpoints")]`, so `cargo test -p ostro-backup --test cluster_mutants` runs 0 tests and the plan's `[1-9][0-9]* passed` grep cannot match; it was run with `--features failpoints` (1 passed). The ci-local failpoint suites cover it.
6. s3s-fs leg: ostro-backup's tests have no s3s-fs dev-dependency (no Cargo edits allowed); the conditional and non-conditional LocalFs legs both run (`put_is_fenced_by_term_with/without_conditional_put`).

## Plan gate

**Incomplete — cluster withdrawn; consolidated gate pending on dell-4.**

`ostro-k8s-ci --parallel` was started at 5e9166f (logs `target/k8s-ci-logs/20261127-045552-par-5e9166fb-*.log`) before the user ended cluster use. State when it was abandoned (182 min):

- Passed: fp1, fp2, fp3, musl1.
- poly, obuild: infrastructure. "ci container not started 10 min after the upload" on dell-1; never ran.
- test1: one failure outside 14-29's code, ostro-bridge `providers_vault::kubernetes_login_and_relogin_on_lease_expiry` (providers_vault.rs:290). Not rerun alone.
- musl2: one failure outside 14-29's code, ostro `queue_limits` (tests/common/p7.rs:712) after 617 s, in a step that took 9907 s (load). Not rerun alone.
- lint, test2: still running when the cluster was withdrawn; no result.

Final runner result (it finished after the withdrawal, exit 1, wall 320 min): test2 failed one test outside 14-29's code, ostro `live_start_with_toml` (tests/common/mod.rs:205, server start; 10460 s step, load); the lint group was killed (exit 137, dell-1) with no ci-local summary.

Unverified items for the consolidated gate on dell-4: poly and obuild groups; alone reruns of the vault relogin test and `queue_limits`; the lint group (fmt, clippy, grep gates, mutant matrices, ...); an alone rerun of `live_start_with_toml` (test2). Locally verified: every task verify, clippy `-D warnings` on the three touched crates, `p12-grep-gates.sh`, CLOCK_GATE_SCOPED, `cargo fmt --check`. The fmt failure on main (ostro-backup cluster_cut.rs, uploader.rs, tests/cluster_cut.rs, tests/meta_head.rs, ostro-server tests/cluster_meta_head.rs) was fixed in 155d891.

## Notes for 14-30

- Use `CursorRecorders::factory()` in `NodeConfig::replay_observer` with `StartMode::Inspect` on the materialised per-partition data dir; `steps_from_replay(rec.samples(p), floor = snapshot/chain start, end = head.end)`; then `cluster_cut::search`.
- Meta: read `meta/head`, then try `meta/<index>.msnap` keys at or below it newest-first (retention keeps `remote_snapshots_keep` of them; the one a Phase 8 manifest names is protected).
- `meta/head` carries no node id (per D-P14-45's field list); the server test asserts leadership through the term instead.

## Self-Check

PASSED: all 6 created files present on main; commits 358c740, 5e9166f, 155d891 on main. Gate: incomplete (see above).
