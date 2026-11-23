---
phase: 12-replicated-partition-logs
plan: 13
subsystem: partition-roles
tags: [replication, roles, failover, b2, w1, w3, f2, replica-services, install, identity, d-p12-16, clus-03, clus-04]
requires:
  - phase: 12-replicated-partition-logs
    provides: "12-02 role contracts (AssignmentSource incl. request_term/resign, FollowerDriver/FollowerTask, PartitionRoles, PartitionControl/InstallGuard); 12-08 PartitionLog become_leader/become_follower/set_published_isr/replica_status + NotEnoughReplicas; 12-11 FetcherDriver; 12-12 install_from_leader (quiesce/reset/install/reopen order)"
provides:
  - "ostro_partition::roles::{PartitionRole, RoleDecision, decide, replica_set, RoleConfig, NodeTestHooks} + one role driver per open partition"
  - "NodeConfig.{assignments, follower_driver, roles, test_hooks}; Node::{role(p), control(), roles()}; impl PartitionControl + PartitionRoles for Node"
  - "replica_services::{spawn_replica_services, ReplicaTasks}: snapshot + retention in every role; leader::spawn_all = shuffler, timers, dispatcher, cron only"
  - "PendingAcks::{refuse_not_leader, accept}; InProcessRouter::{unregister, leader_hint, set_leader_hint}; StartupEvent::RoleChanged"
  - "ostro_partition::{bind_node_id, bind_cluster_id}; NodeError::IdentityMismatch; Node::open checks a bound node_id"
  - "Node::handle(p) -> Option<PartitionHandle> and Node::slots() -> Vec<PartitionSlot> (owned; a reopen swaps the parts)"
affects: [12-14, 12-15, 12-16, 12-17, 12-18]
tech-stack:
  added: []
  patterns:
    - "role changes only in the per-partition driver, from meta's assignment watch and the partition's own health; RF=1 = replicas [self], no RF branch"
    - "takeover is a state (Announcing) whose waits race assignment changes, shutdown and failure; ISR updates of the same epoch are adopted mid-takeover"
    - "per-partition cell (Running | Failed | Quiesced) behind Node so a snapshot install can close and reopen the store"
key-files:
  created:
    - crates/ostro-partition/tests/role_lifecycle.rs
    - crates/ostro-partition/tests/replica_services.rs
    - crates/ostro-partition/tests/node_identity.rs
    - crates/ostro-partition/tests/common/p12_roles.rs
  modified:
    - crates/ostro-partition/src/{roles,node,replica_services,leadership,acks,router,startup,lib}.rs
    - crates/ostro-partition/src/leader/mod.rs
    - crates/ostro-partition/tests/{dispatcher,dispatcher_clock,dispatcher_flow,timers,apply_rearm,snapshot_tail,trim_safety,startup_order,run_index}.rs, tests/common/mod.rs (one line)
    - crates/ostro-partition/examples/gen_format_corpus.rs, crates/ostro-server/examples/export_run.rs
key-decisions:
  - "quiesce_for_install keeps the fetcher running (12-12: the install runs inside the fetcher round); it stops the replica services and the apply machine and closes the store only once no other handle holds it (else it fails after RoleConfig.quiesce_timeout and restarts the partition)"
  - "Node::handle/slots return owned values: a reopen replaces the partition's handle and the old store must really close (fjall would otherwise keep writing into a swapped-in directory). Callers must not cache a handle across an install"
  - "A demotion refuses acks (PendingAcks::refuse_not_leader): current waiters and every later registration answer NotLeader until the next takeover, so a submit that appended before the demotion never waits on an offset a new leader may fill with another command"
  - "Retention on a follower ignores the upload floor: the role driver forwards the uploader's floor into an effective floor only while leading (None otherwise)"
  - "Announcing{epoch} is an explicit role (the partition is unavailable while LeaderAnnounced waits for min_insync)"
  - "StartMode::Inspect still leads locally at next_term_epoch with ISR {self} (HW reaches the durable end, the whole tail replays) and appends nothing; it ignores the assignment source"
requirements-completed: []
duration: ~4 h
completed: 2026-11-23
---

# Phase 12 Plan 13: Partition role lifecycle Summary

**Every partition now has a role that only meta drives. A partition is a Leader at a meta-assigned epoch, a Follower of a named leader, or Offline. Epoch 0 means Offline. A node takes the lead only at an epoch above its log's latest epoch, and otherwise asks for a fresh term. A failed leader steps down and resigns once. The takeover announces its term before any leader service starts. A demotion fails pending acks with a leader hint and starts the fetcher. Snapshots and retention run in every role. `Node` implements the install and roles contracts. `node.toml` binds `node_id` and `cluster_id`. With an `AssignmentSource` set, RF=1 runs the same code as RF=N (replicas `[self]`). Without one, the marked legacy self-leader path keeps the server unchanged until 12-14.**

## Commits

| Commit | Message |
| --- | --- |
| ed86179 | feat(12-13): per-partition role lifecycle driven by meta assignments |
| a2138b4 | feat(12-13): replica services in both roles, install and roles contracts |
| f5acc51 | feat(12-13): node.toml node_id/cluster_id binding |

## Final `NodeConfig` fields

```rust
pub struct NodeConfig {
    pub data_dir, pub partitions, pub node_id, pub store, pub machine, pub wal, pub leader, pub mode,
    pub leadership, pub clock, pub router_wrapper, pub snapshots, pub cron, pub schedule_source,
    pub backup_enabled,
    // 12-13
    pub assignments: Option<Arc<dyn AssignmentSource>>,      // Some = meta-driven roles; None = legacy path (12-14 removes it)
    pub follower_driver: Option<Arc<dyn FollowerDriver>>,    // 12-11 FetcherDriver; None = followers do not fetch
    pub roles: RoleConfig,         // announce_backoff_min 20 ms / max 1 s, term_retry 2 s, quiesce_timeout 10 s
    pub test_hooks: NodeTestHooks, // per node: fail_partition(p) (#[doc(hidden)])
}
```

New `Node` API: `role(p) -> Option<watch::Receiver<PartitionRole>>`, `control() -> Arc<dyn PartitionControl>` (does not own the node, so it can go to 12-12's `InstallCtx::new`), `roles() -> Arc<dyn PartitionRoles>` (for 12-15's uploader), `impl PartitionControl for Node`, `impl PartitionRoles for Node`, plus `router().leader_hint(p)`. `bind_node_id(data_dir, id)` and `bind_cluster_id(data_dir, id)` are free functions that touch only `node.toml`.

## Role transition table (`roles::decide`)

`latest` = the log's latest epoch, including the seeded history.

| Assignment for p | Current role | Decision |
| --- | --- | --- |
| none, `epoch == 0`, `leader == 0`, or self not in `replicas` | any | Offline (B2: no term request at epoch 0) |
| `leader == l != self`, `epoch e ≥ 1` | not already `Follower{e, l}` | Follow: step down if leading → `become_follower(e, l)` → `FollowerDriver::start` |
| `leader == self`, epoch `e` | `Leader{e}` / `Announcing{e}` | keep leading; `set_published_isr(e, set)` only (W1: AlterIsr never requests a term) |
| `leader == self`, `e > latest` | anything else | Lead(e). From an older term this is a full step-down followed by a step-up |
| `leader == self`, `e ≤ latest` | anything else | stay Follower/Offline (step down if leading); `request_term(p, latest + 1)` once per value, resent after `term_retry` |
| partition failed (machine `Failed` or `fail_partition`) | any | Offline for good; `resign(p, e)` once if it led at `e` (W3) |
| quiesced for an install | any | no change until the reopen |

A restarted node always opens `Offline` (`RoleChanged{Offline}` is the first role event) and leads only at a meta epoch above its log. `next_term_epoch` picks an epoch only for Inspect and the legacy path (F2).

## LA timing on takeover

1. Stop the fetcher (await `FollowerTask::stop`).
2. `log.become_leader(e, ReplicaSet{replicas, isr, isr_version, min_insync})`. Role becomes `Announcing{e}`: not routable, not in `led()`, no leader services.
3. Wait until applied ≥ the local HW. With ISR {self} the HW is the durable LEO at once. With RF=N it is the inherited HW.
4. `handle.set_epoch(e)`, `acks.accept()`, then submit `LeaderAnnounced{e, self, clock.now_ms()}`. `NotEnoughReplicas` is retried with backoff (20 ms doubling to 1 s). Every wait races assignment changes: an ISR or `min_insync` update at epoch `e` is adopted and the wait continues. Any other change abandons the takeover (step down, then decide again). Shutdown and failure also end the wait.
5. `submit_one` returns `Accepted` only after LA is committed and applied. Then: `TermStarted{e, offset}`, actions on, `router.register`, the effective upload floor follows the uploader, `leader::spawn_all` (the dispatcher's rebuild re-grants running rows under `e`), `LeaderServicesStarted`, `led` gets `(p, e)`, role `Leader{e}`.

Demotion runs in this order: `led` remove, router unregister plus leader hint, effective upload floor set to `None`, `LeaderTasks::shutdown`, actions off, handle epoch 0, `acks.refuse_not_leader()`. Then follower or offline.

## How Inspect mode still picks terms

`StartMode::Inspect` ignores `assignments`. It seeds anchored epochs, then calls `become_leader(next_term_epoch(floor, latest), ReplicaSet::solo(node))` before catch-up. 12-08's F4 opens the log with HW = applied, and leading with ISR {self} raises the HW to the durable end, so the catch-up replays `[applied, LEO)`. Nothing is appended, the barrier stays closed, and no service runs. The log reports role Leader. The 12-08 open item is decided this way: following at its own durable end would need a log API that 12-08 does not have.

## Tests

| Suite | Result |
| --- | --- |
| `--test role_lifecycle --test-threads=1` | 8 passed: LA{2} appended and applied before `LeaderServicesStarted` (Offline → Announcing → TermStarted → services); leader → follower (a pending submit under ISR {1,2} fails `NotLeader`, `leader_hint` = 2, not routable, `led` empty, fetcher started (2, 2), log role Follower); follower → leader (the fetcher stop saw no epoch-2 record); `NotEnoughReplicas` keeps `Announcing{1}` and the partition unroutable until meta lowers `min_insync`; unassigned / epoch-0 / epoch-0-naming-self partitions stay Offline without term requests (barrier open, handle present), and placement then leads; restart: follows when meta names node 2 (roles Offline → Follower only), and with meta at the old epoch it calls `request_term(p0, 2)` exactly once, then leads at 2; AlterIsr (two version bumps) keeps Leader{1} with no term request and one TermStarted; `fail_partition(p0)` gives Offline and exactly one `resign(p0, 1)`, p1 on the same node is untouched |
| `--test replica_services` | 4 passed: a follower with backup on snapshots on cadence and trims below its snapshot (the upload floor stays `Some(0)`); the same dir as leader keeps the upload floor until the uploader raises it, then trims ≤ it; leader snapshots and trims, and the services survive demotion; quiesce + external install (`reset_to` + Phase 8 `install_snapshot`) + reopen (state at S, fetcher kept, the tail replicated by hand applies to the leader's `(applied, state_hash)`, `x_run_index` digest equals the leader's, W13, reopen from disk agrees); a cached handle makes the quiesce fail ("still in use") and the partition restarts; a leader refuses an install; `led()` follows promotion and demotion |
| `--test node_identity` | 5 passed: fresh dir binds both, then the first open appends format_version/partitions/incarnation after them; same values are no-ops (file and incarnation unchanged); node_id or cluster_id mismatch is fatal with both values in the message and nothing written; `Node::open` as another node id fails before `state/` exists; node_id 0 and bad cluster ids are refused; Phase 8 keys are kept byte for byte; a pre-08-08 file with a comment, an unknown key and no trailing newline gains the keys |
| lib unit tests | 45 passed (new: `roles::transition_table`, `led_set_and_hooks`, `node::identity_keys_parse`) |
| `cargo test -p ostro-partition --no-fail-fast` (before the handle-lifetime fixes) | 474 passed, 2 failed (`run_index::inspect_mode_never_rebuilds_and_leader_mode_does`, `startup_order::empty_wal_anchors_at_applied_and_rebuild_matches`: both held an owned handle across shutdown + reopen, `Fjall(Locked)`); both fixed with `drop(h)` and green on rerun |
| clippy `-D warnings`, ostro-partition all targets, with and without failpoints; ostro-server `export_run` example; `cargo fmt`; `cargo check --workspace --all-targets --locked` after rebasing onto 8d744c9 | clean |

| `cargo test -p ostro-server --locked --no-fail-fast` (legacy path, owned handle API) | 588 passed, 0 failed, 4 ignored |
| `cargo test -p ostro-partition --features failpoints --locked --no-fail-fast -- --test-threads=1` (Task 3's gate, all binaries incl. role_lifecycle, replica_services, node_identity, rebuild_digest, trim_safety, snapshot_tail) | 496 passed, 0 failed, 3 ignored |
| `cargo test -p ostro-backup -p ostro-cluster --locked --no-fail-fast` (Node API users; 12-12 install suites) | 113 passed, 0 failed, 1 ignored |

Not run: the full `scripts/ci-local.sh --no-musl` and `PROPTEST_CASES=256 rebuild_digest` (12-08 measured about 48 min for the latter under load). Four other executors were building, so the phase gate (12-18) covers them, as with 12-08/12-11/12-12. `rebuild_digest` at the default case count passed in both the plain and failpoint runs. The apply format and semantics are untouched: no byte written by the log or the state store changed.

## Deviations from Plan

### Auto-fixed issues

**1. [Rule 3 - Blocking] `Node::handle()` / `Node::slots()` return owned values.**
- **Issue:** `PartitionControl` must close the partition's store, and a reopen must replace the handle. A `&PartitionHandle` handed out from an immutable `slots` vector pins the old store forever. fjall would then keep an open database whose background writes go into the directory the install swapped in.
- **Fix:** partitions live in per-partition cells (`Running | Failed | Quiesced`). `handle(p) -> Option<PartitionHandle>` and `slots() -> Vec<PartitionSlot>` (`PartitionSlot: Clone`) return the current parts. 95 test call sites needed a `&` and four helpers return an owned handle. Three tests held an owned handle across shutdown + reopen and now drop it first. One line in `tests/common/mod.rs` changed (`handle_of`), plus `gen_format_corpus.rs` and the server's `export_run.rs` example. No production server code needed a change.
- **Commit:** ed86179

**2. [Rule 1 - Correctness] Demotion refuses later ack registrations too.** The plan says `acks.fail_all(NotLeader)`, but `fail_all` poisons the partition for good. `fail_pending` alone leaves a window: a submit that appended before the demotion and registers afterwards could wait on an offset that the new leader fills with another command. `refuse_not_leader` and `accept` close that window.

**3. quiesce does not stop the fetcher.** The plan's Task 2 text says to stop it, but 12-12's obligation wins: the install runs inside the fetcher's round, so the quiesce must not wait for that fetcher. The fetcher survives the install, and the test asserts this.

**4. `PartitionRole::Announcing{epoch}`** was added next to Leader/Follower/Offline so the partition can be "reported unavailable meanwhile".

**5. `StartupEvent::RoleChanged`** made `startup_order.rs`'s exhaustive `kind()` match need one arm (outside files_modified, a one-line Rule 3 fix).

**6. Commit granularity.** The Task 2 source (replica services, `PartitionControl`/`PartitionRoles`) shares `node.rs`/`roles.rs` with Task 1, so it landed in the Task 1 commit. The Task 2 commit holds its test suite. Task 3 is separate, and the Task 1 tree compiles and passes clippy without it. Tests and code were written together, with no separate RED commits (same as 12-08/12-11/12-12).

## Handed to 12-14 (and later)

- **`AssignmentSource` implementation (12-10 obligation):** the meta-driven source lives in the server/cluster layer (12-14 Task 1). Map `request_term(p, min)` to `controller::request_fresh_term(&meta, self, p.0, min.0, &*clock)` and `resign(p, e)` to `controller::resign(..)`. All outcomes (Elected/AlreadyFresh/Unplaced/NotLeader/NotInIsr/UnknownPartition) can be ignored by the driver: it reacts only to the next assignment, and it resends a still-stale term request after `term_retry`.
- **dlq_hook / per-partition services (11-12/11-13 coordination):** `dlq_hook::spawn` (ostro-server, outside this plan) still runs on every ready slot. It must gate on `Node::role(p)` being `Leader` (or subscribe to `Node::roles().led()`) once followers exist. Bulk jobs remain node-local, and forwarding them is Phase 13's job.
- **Handle caching:** server code that caches `PartitionHandle` clones (P7Ctx, effects, executor, ...) pins the store. A follower install then fails safely with "state store still in use" after `quiesce_timeout`. 12-14 must fetch handles from `Node::handle(p)` per use on install-capable paths, or drop its caches on `RoleChanged`.
- **Legacy path:** marked `// Phase 12: legacy path, removed by 12-14` in `node.rs` (`TermMode::Legacy`). 12-14 deletes it and makes `assignments` mandatory for `StartMode::Leader`.
- **12-08 "append exactly at the leader epoch":** not tightened (that is ostro-log). The partition handle appends at the takeover epoch and is set to epoch 0 on step-down, so appends through any handle clone are fenced. A raw log append above the leader epoch is still accepted, as 12-08 documented.
- **Identity:** the server boot calls `bind_node_id` before meta and `bind_cluster_id` before `Node::open`. `Node::open` itself enforces only the node id.

## Self-Check: PASSED

- Files: crates/ostro-partition/src/{roles,replica_services,node}.rs, tests/{role_lifecycle,replica_services,node_identity}.rs and tests/common/p12_roles.rs exist at f5acc51.
- Commits ed86179, a2138b4 and f5acc51 are on main.
