---
phase: 12-replicated-partition-logs
plan: 14
subsystem: server-integration
tags: [replication, server, boot-order, identity, meta, controller, roles, not-leader, self-demotion, module-blobs, status, d-p12-13, d-p12-14, d-p12-15, d-p12-16, d-p12-21, d-p12-22, clus-02, clus-03, clus-04]
requires:
  - phase: 12-replicated-partition-logs
    provides: "12-05 MetaHandle::start_with/PeerOutbox/GrpcForwarder/MetaPeerService/TcpTransport; 12-10 Controller/LivenessHandle/request_fresh_term/resign/upsert_self; 12-11 FetchCtx/FetcherDriver/IsrPublisher/blob; 12-12 SnapshotCtx/InstallCtx; 12-13 role drivers, bind_node_id/bind_cluster_id, Node::{role,roles,control}; 12-15 UploaderClusterDeps/acquire_if_single_member; 12-06/12-07 scenarios + mutant matrix"
provides:
  - "ostro_server::cluster::ClusterServices (boot order steps 1-9, attach_node, status_router, leader_hints, module_push, stop_tasks/into_meta) + MetaAssignments (meta-driven AssignmentSource with self-demotion masking)"
  - "ostro_cluster::{identity (resolve_cluster_id, probe_meta, check_meta_voters, check_voters_after_ready, check_cluster_info, cluster_id_from_ca_pem, joiner_error), config::ReplicationTuning, service (ReplicationService late-bound, replication_server, serve over TCP/mTLS or in-memory incoming, PeerIo, report_suspect), status::document}"
  - "workers::service::{LateExecute, execute_service, listener_client_roots}; P7Ctx::cluster_listener"
  - "ostro_partition::{LocalAssignments, bound_cluster_id}; the legacy self-leader path deleted"
  - "control::{LeaderHints, not_leader_error, ModulePush}; 503 not_leader / not_enough_replicas / module_replication details"
  - "GET /v1/admin/replication"
affects: [12-16, 12-17, 12-18]
tech-stack:
  added: []
  patterns:
    - "one cluster listener in every profile: MetaPeer + Replication bound before meta, the worker protocol attached later through a late-bound tower service"
    - "late binding instead of construction cycles: Replication contexts and the FetcherDriver are bound after Node::open; the role drivers started earlier wait for them"
    - "self-demotion as an assignment-view mask: the role drivers step down through their normal path, no node API"
key-files:
  created:
    - crates/ostro-server/tests/cluster_boot.rs
    - crates/ostro-server/tests/cluster_api.rs
    - crates/ostro-server/tests/common/p12_cluster.rs
  modified:
    - crates/ostro-server/src/{cluster,server,p7}.rs
    - crates/ostro-server/src/control/{mod,modules,runs}.rs
    - crates/ostro-server/src/workers/service.rs
    - crates/ostro-server/src/services/uploader.rs
    - crates/ostro-cluster/src/{identity,config,service,status}.rs
    - crates/ostro-partition/src/{node,roles,lib}.rs
    - crates/ostro-partition/tests/{startup_order,rebuild_basic}.rs
    - crates/ostro-repl-conformance/src/oracle/la_rule.rs
    - crates/ostro-repl-conformance/src/harness/inproc.rs
    - crates/ostro-repl-conformance/src/pending/{core,rest}.rs
    - crates/ostro-repl-conformance/tests/{harness_smoke,oracle_selftest,runner_selftest}.rs
key-decisions:
  - "The status field is published as both `published_isr` and `isr` (the 12-03 oracle reads `isr`); `hw`, `hw_publishes`, `max_envelopes_per_fetch` keep the ReplicaStatus names"
  - "No assignment source in NodeConfig = LocalAssignments (single-node in-memory placement + fresh terms) on the same role drivers, and Node::open then waits until every partition leads; the server always passes MetaAssignments"
  - "Self-demotion masks the partitions this node leads as leaderless in its own assignment view when leader_contact_age > session_timeout; contact back = real view = a fresh-term request"
  - "IsrPublishers run while a partition is Leader OR Announcing: the takeover's LeaderAnnounced waits for min_insync, which only an ISR expansion can satisfy (led() excludes Announcing)"
  - "Replication snapshot contexts never export (Exporter::None); a fresh FetchSnapshot first takes Node::snapshot_now when no local snapshot covers log_start, so no server object pins a follower's state store"
  - "ServerControlPlane::partition(p) is the not-leader gate (writes and reads alike): a follower/offline/announcing partition answers 503 with {reason not_leader, partition, leader, leader_addr}; leader_addr is the leader's Member.addr (its API address)"
  - "/readyz needs a meta leader anywhere (not this node leading meta)"
  - "Module push: k = min(min_insync, live members) − 1; candidates = live peers when this node leads meta (liveness), else every other voter; k = 0 on a single node"
requirements-completed: []
duration: ~5 h
completed: 2026-11-23
---

# Phase 12 Plan 14: Server integration Summary

**Every server, a single node included, now boots through the Phase 12 cluster wiring: identity and voter checks before meta, an always-bound cluster listener (MetaPeer + Replication + the worker protocol), a static multi-voter meta over the live transport, the controller, meta-driven partition roles, follower fetchers, ISR publishers and snapshot installs. RF=1 runs the same path (placement on the first boot, one fresh `ElectLeader` term per partition per restart). The legacy `SetPartitionLeader` loop and the ostro-partition self-leader path are gone. Non-leaders answer 503 with leader hints, a node that loses meta contact self-demotes, module blobs reach enough peers before their `Module` record, and `GET /v1/admin/replication` reports the published ISR and measured follower progress. On the first integrated tier-B red run 22 of 30 scenarios pass; 8 remain for 12-16.**

## Commits

| Commit | Message |
| --- | --- |
| 7f00580 | feat(12-14): cluster services and meta-first boot order (RF=1 included) — Tasks 1 and 2 (one commit, see deviations) |
| 053ccf1 | test(12-14): first conformance run against the integrated server |
| (docs) | docs(12-14): complete server integration plan |

## Final boot sequence (`ClusterServices::start`, then the server)

1. Data dir, `LOCK`, API listener bind (unchanged).
2. `bind_node_id` (node.toml), before meta opens.
3. `identity::probe_meta(<data>/meta)`: reads the vendored store's `state` (HardState + ConfState), `snapshot` (ConfState fallback, `ClusterInfo` from the snapshot state) and, when needed, the first committed `InitCluster` in `log/*.seg`; nothing is opened or written.
4. Cluster id: `[cluster] cluster_id` → node.toml → `<data>/tls/ca.crt` CN (`ostro cluster <id>`) → `ClusterInfo.cluster_id` → fresh UUIDv7 only when meta is blank; two present sources that disagree are fatal ("cluster id conflict").
5. Joiner: blank meta over existing `state/` or `wal/` data fail-stops (peers: the wiped-voter remedy text naming `ostro cluster remove/join`; no peers: "meta dir missing"). Blank meta without partition data bootstraps with `voters = self ∪ peers`, even when node.toml already holds a cluster id.
6. Voter check: a persisted ConfState whose voters ≠ `self ∪ peers` is fatal ("grow via Phase 13 `ostro cluster` commands").
7. `bind_cluster_id`; TLS: `ClusterCa::import_pem` of `[cluster.tls] ca_cert_file/ca_key_file` (peers), else the node's own CA; node certificate.
8. Cluster listener, every profile: `Routes{Replication (limit max(fetch_decode_limit, SNAPSHOT_MESSAGE_LIMIT)), MetaPeer (64 MiB), LateExecute}` on `[cluster] listen` with mTLS (client roots = cluster CA + `[cluster.tls] client_ca`), or on the injected transport's in-memory incoming (`PeerIo` puts the caller's `PeerNode` in the request extensions). Optional `replication_listen` serves Replication only.
9. `MetaHandle::start_with(voters, PeerOutbox, GrpcForwarder, [replication] ticks)` → `RaftSlot::set` → `wait_ready` → live voters must equal the configured set.
10. `InitCluster` with the bound id (idempotent) → `ClusterInfo.cluster_id` must equal it → `members::upsert_self` (addr = API address, cluster_addr = bound listener or `mem://<id>`).
11. `Controller::run` (every node; acts as meta leader), `MetaAssignments` (view task + self-demotion watchdog every `session_timeout/4`).
12. Effects bridge, runtime, Phase 7 init (`p7.cluster_listener = execute slot`), `Node::open` with `assignments = MetaAssignments`, `follower_driver = LateFollower`.
13. `attach_node`: `FetchCtx` + `SnapshotCtx` per partition bound into Replication, `FetcherDriver` (blob store, `InstallCtx::new(node_id, data_dir, count, transport, node.control())`, suspects → local liveness or `MetaPeer.ReportSuspect` to the meta leader) set live, one `IsrPublisher` task per partition, module-blob sync task.
14. `first_terms`: a single node waits until it leads every partition (60 s bound); a cluster node waits up to 10 s for Leader/Announcing/Follower roles.
15. Services, Phase 7 services (Execute attaches to the listener), API with `/v1/admin/replication`.

Shutdown: API drain → Phase 7 → services → bridge → cluster tasks (controller, publishers, sync, views; Replication unbound) → `Node::shutdown` → meta → cluster listener → data-dir lock.

## Timings (single node, tmpfs, debug build, `cluster_boot`)

| | before (04-17 path) | after |
| --- | --- | --- |
| first boot (3 partitions) | not re-measured | 62 ms |
| warm restart, `total` | 04-19 budget 5 s (bootstrap.rs green) | 46-48 ms (meta 2 ms, node_open 8 ms, first_terms 3 ms) |

The existing warm-restart / startup suites (`ostro-server --test bootstrap`, the `ostro` binary suites) are unchanged and green. A before-number on the same machine was not taken; the 04-19 budget is the comparison.

## `/v1/admin/replication` example (node 1 of a 3-voter cluster with node 3 never started)

```json
{"node_id": 1,
 "meta": {"leader": 1, "term": 2, "voters": [1, 2, 3], "learners": []},
 "partitions": [
  {"partition": 0, "role": "leader", "epoch": 1, "leader": 1, "hw": 2, "durable_end": 3,
   "published_isr": [1, 2], "isr": [1, 2], "isr_version": 1, "min_insync": 2,
   "replicas": [1, 2, 3],
   "followers": [{"replica": 2, "durable_leo": 2, "fetch_offset": 2, "lag_ms": 1, "in_sync": true},
                 {"replica": 3, "durable_leo": 0, "fetch_offset": 0, "lag_ms": 749, "in_sync": true}],
   "hw_publishes": 2, "max_envelopes_per_fetch": 0},
  {"partition": 1, "role": "follower", "epoch": 1, "leader": 2, "hw": 1, "durable_end": 1,
   "published_isr": [], "isr": [], "isr_version": 0, "min_insync": 1, "replicas": [2, 3, 1],
   "followers": [], "hw_publishes": 1, "max_envelopes_per_fetch": 0},
  {"partition": 2, "role": "follower", "epoch": 1, "leader": 0, "hw": 0, "durable_end": 0,
   "published_isr": [], "isr": [], "isr_version": 0, "min_insync": 1, "replicas": [3, 1, 2],
   "followers": [], "hw_publishes": 0, "max_envelopes_per_fetch": 0}]}
```

Partition 2 was placed with node 3 as its preferred leader; node 3 never ran, so it is offline (leader 0, ISR keeps its last member, no unclean election). Note the tracker's row for node 3 on partition 0: it never fetched, so nothing is attributed to it (`durable_leo 0`), and it is not in the published ISR; its `in_sync: true` is the tracker's join grace (see open items).

## Tier-B red run after integration

`OSTRO_REPL_RED_RUN=1 cargo test -p ostro-repl-conformance --locked --test red_run -- --ignored --nocapture --test-threads=1`, at 7f00580 + the harness restart fix, 320.6 s:

| scenario | pending (before) | result | oracle: detail |
| --- | --- | --- | --- |
| placement_rf3_three_replicas | yes | pass | |
| placement_rf1_single_node | no | pass | |
| isr_empty_follower_not_in_isr | yes | pass | |
| isr_lagging_follower_rejoins | yes | pass | |
| isr_slow_follower_shrinks | yes | FAIL | election: partition 1: not shrunk without the slow follower within 4s (leader 2 epoch 1 isr [2, 3, 1]) |
| hw_is_min_over_isr | yes | pass | |
| hw_rf3_commits_within_rounds | yes | pass | |
| fetch_with_stale_epoch_fenced | yes | pass | |
| fetch_with_future_epoch_unknown | yes | pass | |
| leader_dies_with_unreplicated_writes | yes | pass | |
| isolated_leader_rejoin_truncates | yes | FAIL | election: meta index 76: ElectLeader partition 3 epoch 2 elects node 1 outside the published ISR [3, 2] (apply: StaleVersion { current: 2 }) — the controller proposed from a stale view |
| two_quick_leader_changes_truncate | yes | FAIL | replica: node 1 partition 0: unacknowledged divergent command kip2-conf-615-738 is below HW 33 |
| failover_elects_isr_member | yes | pass | |
| whole_isr_dead_goes_offline | yes | pass | |
| resigned_leader_moves | yes | pass | |
| writes_stop_below_min_insync | yes | pass | |
| writes_resume_when_follower_rejoins | yes | pass | |
| followers_are_hot_standbys | yes | pass | |
| empty_node_bootstraps_from_snapshot | yes | FAIL | bootstrap: partition 0: leader 1 log starts at 0 after snapshot + retention, not above 0: no snapshot install would be needed |
| node_behind_log_start_reinstalls | yes | FAIL | bootstrap: partition 0: leader 1 log starts at 0 after snapshot + retention, not above 13 |
| install_stream_cut_resumes | yes | FAIL | bootstrap: partition 0: leader 1 log starts at 0 after snapshot + retention, not above 0 |
| slow_read_longer_than_max_wait_completes | yes | pass | |
| max_size_envelope_replicates | yes | pass | |
| batching_on_user_partitions | yes | FAIL | status: partition 0: max_envelopes_per_fetch 0 < 2 |
| blob_before_commit_survives_failover | yes | pass | |
| one_path_rf1_hw_through_tracker | yes | pass | |
| isolated_leader_cannot_commit | yes | FAIL | zombie: isolated leader 1 of partition 0 did not answer 503 within 1.6s of losing meta contact: no answer (request hung) |
| remote_chain_equals_committed_prefix | yes | pass | |
| remote_chain_never_above_hw | yes | pass | |

**22 / 30 pass; no harness error rows.** 19 of the 20 passing pending entries were removed from `pending/{core,rest}.rs` (no scenario or oracle change made them pass in this task). `leader_dies_with_unreplicated_writes` passed the red run but failed the next full suite run (`replica: node 1 partition 0: unacknowledged divergent command div-conf-605-14 is below HW 26`, the same class as `two_quick_leader_changes_truncate`), so it stays pending. 12-16's list: the 8 failing scenarios plus that one (9 pending entries). With it pending, the mutant matrix SKIPs `hw_leader_leo`'s and `no_truncate`'s `leader_dies_with_unreplicated_writes` rows (the run below was taken before it went back).

## Mutant matrix

`cargo test -p ostro-repl-conformance --features failpoints --locked --test mutants -- --test-threads=1 --nocapture` (329 s, `test result: ok`):

```
MUTANT repl::mutant::isr_from_assignment: isr_empty_follower_not_in_isr FAILED as expected: status: partition 0: no stable status sample to check within the window
MUTANT repl::mutant::hw_leader_leo: leader_dies_with_unreplicated_writes FAILED as expected: election: partition 0: not rejoined by the old leader(s) within 22s (leader 2 epoch 2 isr [2, 3])
MUTANT repl::mutant::hw_leader_leo: hw_is_min_over_isr FAILED as expected: hw: partition 0: leader 1 HW 4 above ISR member 3's measured durable end 2
MUTANT repl::mutant::count_nondurable_leo: TIER-C (process_mutants) follower_power_loss
MUTANT repl::mutant::skip_epoch_check: fetch_with_stale_epoch_fenced FAILED as expected: fence: partition 0: Fetch from node 3 to node 2 at epoch 1: answered None (leader epoch 2), expected FencedLeaderEpoch
MUTANT repl::mutant::no_truncate: leader_dies_with_unreplicated_writes FAILED as expected: election: partition 1: not rejoined by the old leader(s) within 22s (leader 3 epoch 2 isr [1, 3])
MUTANT repl::mutant::unclean_election: failover_elects_isr_member FAILED as expected: election: meta index 26: ElectLeader partition 1 epoch 2 elects node 3 outside the published ISR [2, 1] (apply: Invalid(..))
MUTANT repl::mutant::unclean_election: whole_isr_dead_goes_offline FAILED as expected: election: meta index 28: ElectLeader partition 0 epoch 2 elects node 2 outside the published ISR [1] (apply: Invalid(..))
MUTANT repl::mutant::ignore_min_insync: writes_stop_below_min_insync FAILED as expected: min_insync: partition 1: run 000g38g76tdtexmtjrd2cdpvhay7a acknowledged with ISR [2] below min_insync
MUTANT repl::mutant::shrink_before_commit: isr_slow_follower_shrinks SKIP (pending)
MUTANT repl::mutant::upload_above_hw: remote_chain_never_above_hw FAILED as expected: objstore: partition 1: chunk p/0001/seg/0000000001-00000000000000000010-00000000000000000016.oseg (epoch 1) ends at 16 above HW 11
MUTANT repl::mutant::rf1_bypass: one_path_rf1_hw_through_tracker FAILED as expected: one_path: partition 0: 5 sequential acknowledged appends, but the tracker published the HW 0 time(s) (hw_publishes 0 -> 0): the RF=1 HW bypasses the replica tracker
```

`rf1_bypass` now runs and bites. Two bites have weak reasons that 12-16/12-18 should look at: `isr_from_assignment` ("no stable status sample") and the "not rejoined by the old leader(s) within 22s" rows of `hw_leader_leo` / `no_truncate`. Each is a `ScenarioFailure`, not a harness error, but the oracle the scenario aims at is not the one that fired.

## Tests

| Suite | Result |
| --- | --- |
| `ostro-server --test cluster_boot -- --test-threads=1` | 7 passed: single node meta-first (node.toml ids, Put(Partition) placement epoch 1 reason Bootstrap, leads at 1; two restarts → ElectLeader epoch 2, 3 reason Startup, LA at that epoch in every partition log, warm < 5 s); other node_id on an existing dir fatal with meta `state` mtime unchanged; configured id ≠ node.toml fatal; wiped meta without peers → "meta dir missing", no meta written; pre-Phase-12 node.toml (identity keys removed) adopts the stored id and takes a fresh term; persisted voters ≠ peers → "grow via Phase 13"; 3-node loopback mTLS cluster with node.toml pre-bound (crash after bind_cluster_id) forms, places RF 3 epoch 1, followers join the ISR, roles match meta, every node's log holds epoch 1; wiped meta over intact partitions with peers → wiped-voter remedy, no meta written |
| `ostro-server --test cluster_api` | 1 passed: 2 of 3 voters running; module upload leaves the blob on both live nodes when it returns; runs on non-led partitions → 503 `{reason not_leader, partition, leader 2, leader_addr}` (offline partition: leader null), led partitions accept; status doc (voters [1,2,3], node 3 never in a published ISR, no progress attributed to it, `hw_publishes > 0`); node 2 stopped → node 1 self-demotes (no led partition) and answers 503 |
| `cargo test -p ostro-server --locked --no-fail-fast` | 601 passed, 0 failed, 4 ignored |
| `cargo test -p ostro -p ostro-api -p ostro-cluster --locked --no-fail-fast` | 364 passed, 0 failed |
| `cargo test -p ostro-partition --no-fail-fast` | green after updating 3 tests to the role-path event order (startup_order ×2, rebuild_basic ×1) |
| `ostro-repl-conformance --test one_path` | 1 passed, not PENDING |
| `cargo test -p ostro-repl-conformance --locked --no-fail-fast -- --test-threads=1` | 50 binaries green, 1 failed: `divergence` (`leader_dies_with_unreplicated_writes`, intermittent; re-pended, then green) |
| `ostro-repl-conformance` harness_smoke / oracle_selftest / runner_selftest / skeleton / fault_layer | green after updating the three "until 12-14" self-test assertions |
| clippy `-D warnings` ostro-partition, ostro-cluster, ostro-server, ostro-repl-conformance (all targets, with and without `failpoints`); `cargo fmt --all --check` | clean |

Not run: the full `scripts/ci-local.sh --no-musl` (about 2-3 h; four other executors were building). The plan's verification step and the 12-18 gate cover it; the per-crate suites above include every pre-existing server, binary and partition suite.

## Deviations from Plan

### Auto-fixed issues

1. **[Rule 1 - Bug] ISR publishers must run during the takeover.** `PartitionRoles::led()` lists Leader partitions only, but a new leader stays `Announcing` until `LeaderAnnounced` meets `min_insync`, which needs the ISR expansion only the publisher proposes: deadlock with `min_insync = 2`. The server runs one publisher per partition while its role is Leader or Announcing.
2. **[Rule 1 - Bug] `la_rule` oracle ignored the bootstrap placement.** It knew `ElectLeader`/`SetPartitionLeader` only, so an RF=1 node placed by `Put(Partition)` failed "the committed meta log has no leader for epoch 1". The bootstrap `Put(Partition)` (epoch > 0) now counts (`oracle/la_rule.rs`, 12-03's file, minimal arm).
3. **[Rule 3 - Blocking] Harness `restart` healed after the start.** A node's start now waits for its meta group, so a restart inside the isolation timed out (30 s) in every restart scenario. `restart` heals first (the documented semantics: "restarts and heals").
4. **[Rule 3] Harness self-tests encoded the unintegrated build** (`harness_smoke` expected one voter and no status route; `oracle_selftest` / `runner_selftest` used "two independent nodes" as the cluster_formed red case). They now assert the formed cluster and use an rf-3 spec on two nodes as the red case.
5. **[Rule 3] Role-path event order.** Removing the legacy path moved Leader-mode startup events to the role-driver order (`RoleChanged Offline`, barrier, `Announcing`, term, services, `Leader`), and the catch-up at open no longer replays past the persisted applied offset (HW at open = applied, F4; the takeover replays). `startup_order.rs` (2 tests) and `rebuild_basic.rs` (1 test) assert the new order.
6. **Execute on the shared listener.** The plan's listener module is `workers/service.rs` (12-01). It now exposes `execute_service` / `listener_client_roots` / `LateExecute`; `serve()` keeps its behaviour for the Phase 7 tests. `P7Ctx` gained `cluster_listener` (p7.rs, one field) so `workers::service::start` attaches instead of binding.
7. **One commit for Tasks 1 and 2.** `cluster.rs` and `server.rs` carry both tasks' wiring (leader hints, module push, status route are `ClusterServices` methods); splitting would have left a non-building intermediate commit.
8. **Files outside `files_modified`** (each a minimal hunk): `control/runs.rs` (NotEnoughReplicas / NotLeader details), `services/uploader.rs` (12-15's obligation), `p7.rs`, `ostro-partition/src/{roles,lib}.rs`, the conformance harness/oracle/self-tests above.

### Not done here (handed on)

- **Handle caching:** `ServerControlPlane.partitions`, the bridge partition table and `P7Shared.node` still cache `PartitionHandle` clones; a follower's snapshot install fails after `quiesce_timeout` ("state store still in use") and restarts the partition. The bootstrap scenarios that would exercise installs fail earlier (no trimming, below), so this is unmeasured; 12-16 should fetch handles per use (`Node::handle`) on those paths.
- **dlq_hook:** per-action hooks fire only on leaders (followers never emit actions) and the startup catch-up goes through `cp.partition(p)`, which now refuses non-led partitions; a partition that becomes led later does not get a catch-up pass.
- `docs/replication.md` (the restore refusal message points there): owned by 12-18 per 12-DECISIONS.
- Tier C: the node certificate SANs are `[cluster] advertise` + loopback; 12-09's proxied peers dial the target's IP, so tier-C configs must list that IP in `advertise`.

## Open items for 12-16 (from the red run)

- `isolated_leader_rejoin_truncates`: the controller proposed an `ElectLeader` from a stale published ISR (apply rejected it with StaleVersion, but the election oracle audits every proposal). Controller should re-read before proposing or the oracle should only audit proposals made from the then-current view; decide which.
- `two_quick_leader_changes_truncate` and (intermittently) `leader_dies_with_unreplicated_writes`: an unacknowledged divergent command below HW on the old leader after it rejoined — investigate whether it was truncated and re-replicated, or legitimately committed by a follower that had fetched it before the kill (the oracle should then not call it divergent).
- `isr_slow_follower_shrinks`: no shrink within 4 s with `replica_lag_max` 2 s — publisher decision period/timing.
- bootstrap ×3: the leader's log never trims (`log_start` 0 after snapshot + retention) — retention on a leader with an out-of-ISR replica.
- `batching_on_user_partitions`: `ReplicaStatus.max_envelopes_per_fetch` is always 0 (nobody records it).
- `isolated_leader_cannot_commit`: the isolated leader's request hung instead of answering 503 within 1.6 s. With `session_timeout` 600 ms the meta leader steps down only after its election timeout (check_quorum), then the watchdog needs another `session_timeout`; pending submits should also fail fast on demotion. Tune or demote on "no meta leader" too.
- Tracker reports `in_sync: true` for a replica that never fetched (join grace); the published ISR is honest, but the status row is misleading.

## Self-Check: PASSED

- Files: crates/ostro-server/src/cluster.rs (contains `pub struct ClusterServices`, `Controller::`), crates/ostro-cluster/src/service.rs (contains `max_decoding_message_size`), crates/ostro-server/tests/cluster_boot.rs (contains `meta_first`), tests/cluster_api.rs, tests/common/p12_cluster.rs exist.
- `grep SetPartitionLeader crates/ostro-server/src/server.rs`, `grep "MetaHandle::start(" crates/ostro-server/src/server.rs`, `grep "legacy path" crates/ostro-partition/src/node.rs`: no matches.
- Commits 7f00580 and 053ccf1 are on main.
