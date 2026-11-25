---
phase: 13-cluster-membership-routing-upgrades
plan: 12
subsystem: any-node request routing (ostro-server route/, node API on the cluster listener)
tags: [routing, api-08, forwarding, fan-out, hop-limit, unkeyed-placement, node-identity, tier-b]
requires:
  - phase: 13-cluster-membership-routing-upgrades
    provides: "13-01 route table + RequestMeta; 13-02 seam (wrap_control_plane / wrap_extensions / node_api_services / pinned_partition call sites, consistency hook signatures); 13-03 GrpcControlPlane + request_meta middleware + TrustedNode; 13-08 tier-B harness; 13-11 handoff fence (leader_changed)"
provides:
  - "route::RoutedControlPlane + RoutedExtensions over every ControlPlane / extension method, one hop, one re-route, 503 + Retry-After"
  - "route::table::{RouteClass, ROUTES, owner, pinned_partition, listed_partitions}; failpoint route::stale_view"
  - "route::peer::PeerControlPlane (GrpcControlPlane per node over ClusterTransport::channel)"
  - "route::fanout: partition fan-out with cursor-order merge, node fan-out (complete-or-error)"
  - "node-only ostro.api.v1 on the cluster listener (route::node_api_router / trusted_node)"
  - "route::stats(&Arc<P13Ctx>) routing counters (test probe); metrics ostro_route_requests_total, ostro_route_forward_seconds"
  - "tier-B routing group (7 scenarios incl. parity_through_every_node)"
affects: [13-13, 13-16, 13-17, 13-18, 13-19, 13-20]
tech-stack:
  added: []
  patterns:
    - "RouteEnv trait between the router and the node (production NodeEnv over P13Ctx; unit tests plug fakes in)"
    - "per-node routing state shared by the three seam entry points through a weak registry keyed by the P13Ctx (no cycle, no strong ref outlives the API / listener)"
    - "late node-API router slot (ArcSwapOption) filled by wrap_extensions after the listener services attached"
key-files:
  created:
    - crates/ostro-server/tests/p13_routing_unit.rs
    - crates/ostro-repl-conformance/tests/routing.rs
  modified:
    - crates/ostro-server/src/route/{mod,table,peer,fanout}.rs
    - crates/ostro-server/src/control/{list,p11_search,p11_dlq}.rs
    - crates/ostro-api/src/error.rs
    - crates/ostro-server/tests/cluster_api.rs
    - crates/ostro-repl-conformance/src/scenarios/routing.rs
key-decisions:
  - "Tasks 1 and 2 landed as one commit: forwarding without the node API would answer clients UNIMPLEMENTED from every peer"
  - "List partial-result convention: the listing APIs have none, so a fan-out with an offline partition is 503 partition_offline (details.partitions, retry_after_ms), never a silently incomplete page"
  - "Offline (assignment leader 0) is 503 immediately, as the plan says; that includes the short leader-0 window of a ResignLeader move (13-11's handoff moves by ElectLeader epoch+1 and has no such window)"
  - "HookIngress.deliver is classified local: its start / signal / completion already go through the routed control plane (13-02), which routes the start to its key's or an unkeyed partition"
  - "queue_demand sums every member (demand, backlog and worker slots are node-local), not one leg per partition leader"
  - "Retry-After for 503s: ostro-api emits it for `unavailable` with details.retry_after_ms (only the router's answers carry it; other 503s unchanged)"
  - "upgrade_finalize and run_module_gc run on the meta leader (13-04 handover)"
requirements-completed: []
duration: "about 2 h of work + the cluster ci-local"
completed: 2026-11-25
---

# Phase 13 Plan 12: Any-node routing Summary

**Every API call issued at any node now reaches the owning partition's leader (or fans out) with single-node results: `RoutedControlPlane` + `RoutedExtensions` over the 13-01 route table, forwarding once over the cluster listener (`ostro-hop: 1`, node identities only), one re-route on `not_leader` / `leader_changed` / transport failure, then 503 + `Retry-After: 1`; unkeyed starts land on a locally led partition; `list_runs` and the node-local registries fan out and merge in cursor order. The parity transcript through each node of a 3-node RF=3 cluster equals a single node's, REST and gRPC.**

## Final route table (`route::table::ROUTES`)

| Trait | Methods | Class |
|-------|---------|-------|
| ControlPlane | upload_module, presigned_object, join_worker, dev_time, dev_skip, replay_module, ready | local |
| ControlPlane | get_module, list_workflows, get_workflow_policy, get_queue_policy, list_secrets, list_grants, get_connection, list_connections, get_egress, list_egress, get_queue_flow, list_worker_tokens | meta_read |
| ControlPlane | activate_workflow, set_workflow_policy, set_queue_policy, put/delete_secret, put/delete_grant, put/delete_connection, put/delete_egress, put_queue_flow, pause/resume_queue, pause/resume_workflow, create/revoke_worker_token | meta_write |
| ControlPlane | start_run | key_partition (`partition_for_key(D23-qualified workflow, key)`); no key → unkeyed_start |
| ControlPlane | get_run, run_history, signal_run, cancel_run, terminate_run, retry_run, await_result, pause_run, resume_run | run_partition |
| ControlPlane | complete_token | token_partition (`ostro_engine::token::parse`) |
| ControlPlane | list_runs, queue_demand | fanout_partitions (queue_demand: every member, summed) |
| ControlPlane | list_workers, drain_worker | fanout_nodes (merge / first hit) |
| ScheduleApi | list/get_schedule | meta_read |
| ScheduleApi | create/update/delete/pause/resume_schedule | meta_write |
| ScheduleApi | trigger_schedule | key_partition (`schedule_owner(name)`) |
| WebhookApi | list/get_webhook / create/update/delete_webhook | meta_read / meta_write |
| HookIngress | deliver | local (the start inside is routed) |
| RunLogsApi | run_logs | run_partition |
| AdminApi | backup | local |
| OpsApi | rerun_run, analyze_rerun, requeue_dlq, dismiss_dlq | run_partition |
| OpsApi | list_dlq | fanout_partitions |
| OpsApi | create_bulk, limits | local |
| OpsApi | list_bulk / get_bulk, cancel_bulk | fanout_nodes (merge / first hit) |
| OpsApi | get/set_failure_policy | meta_read / meta_write |
| InventoryApi | list_modules, list_queues | meta_read |
| InventoryApi | list_partitions | fanout_nodes (each partition's row from its leader) |
| ClusterApi | status, upgrade_status | meta_read |
| ClusterApi | init, join, create_node_token, remove, drain, reassign, rebalance, upgrade_hold, upgrade_release | meta_write |
| ClusterApi | upgrade_finalize | meta_leader |
| ModuleLifecycleApi | retire_module / run_module_gc / migrate_runs | meta_write / meta_leader / fanout_partitions |

`every_trait_method_classified` parses `domain.rs`, `phase8.rs`, `phase11.rs` and `phase13.rs` (`include_str!`) and fails on a method missing from the table or a table row naming no method.

**Owner** (`table::owner`): led here (led set) → local; published leader = this node (taking over) → local (the inner call answers `not_leader` until the takeover is done, and the router waits and re-routes); leader 0 → offline; another leader → forward; no assignment row → local.

## Retry / timeout constants

| What | Value |
|------|-------|
| Hops | 1 (a call with `hop >= 1` always runs locally) |
| Re-routes | 1 |
| Retryable | `unavailable` with reason `not_leader`, `leader_changed`, `peer_unreachable`; a reason-less `unavailable` from a peer (transport failure, the 13-03 client's mapping) |
| Wait before the re-route | until `owner(p)` differs from the (target, epoch) the call was routed on, or this node gains / loses `p` in its led set (a takeover completing); at most min(remaining deadline, `RETRY_WAIT_SESSIONS` (2) × `[replication] session_timeout`) = 6 s by default, 1.2 s in tier B |
| Expired deadline | no retry (`unavailable`, reason `deadline`); a forwarded long-poll carries the remaining deadline (`grpc-timeout`) and returns at it |
| 503 after the re-route | the last refusal's details + `retry_after_ms: 1000` + `partition` → REST 503 + `Retry-After: 1`, gRPC `UNAVAILABLE` |
| Offline partition | 503 immediately, reason `partition_offline`, `leader` / `leader_addr` null (the D-P12-21 fields), `retry_after_ms: 1000` |
| Self-demoted node (meta contact lost) | no wait: its masked view cannot move, so the re-route happens at once |
| Peer client | one `GrpcControlPlane` per node (max message = `api.max_module_bytes` + 64 KiB, at least the 80 MiB default), dropped after a transport failure |

## Forwarding latency (tier B, in-process memory transport, dev machine under load)

300 `GET /v1/runs/{id}` on the leader vs through a follower (forwarded), and 100 keyed starts (random partitions) through each:

| Path | GET p50 | GET p99 | keyed start p50 | keyed start p99 |
|------|---------|---------|-----------------|-----------------|
| leader node | 2.17 ms | 3.25 ms | 3.78 ms | 6.07 ms |
| follower node (forwarded) | 0.78 ms | 1.12 ms | 3.96 ms | 5.31 ms |

The forwarding hop is below the measurement noise in process (the leader node also runs the executor; the order of the two series favours the second). Native TCP + mTLS numbers belong to the 13-19 tier-C runs.

## List partial-result convention

The listing APIs (`RunPage`, `DlqPage`, `MigrateRunsReport`) have no partial-result field, so a partition fan-out with a partition that has no leader answers **503 `partition_offline`** with `details.partitions` and `retry_after_ms` (the "503 if the API has none" branch of the plan). Node fan-out is complete-or-error too: a member that cannot be asked fails a merged list; a lookup that misses locally and cannot ask a member answers 503 instead of a false `not_found`.

Merge (`fanout::merge_run_pages`): every leg returns up to `limit` rows and its own token; the merged page keeps rows at or before the earliest leg position, sorted by the cursor order (`c1.` scan: `(uuid, partition)`; `i1.` index: `(created_at desc, uuid desc, partition)`), cut at `limit`. The token is that leg's token, or the last returned row's position when the limit cuts. Mixed scan / index tokens (a run index rebuilding on one leg) → 503 `index_rebuilding`. `list_runs` is exact. `list_dlq` entries carry no creation time, so its newest-first merge uses the run id's uuid v7 millisecond in place of `created_at_ms` (equal to the index key unless the two clock reads straddle a millisecond).

A fan-out leg runs locally in a `RequestMeta` scope with `partitions = <its set>` and `from_node`; `list.rs`, `p11_search.rs` and `p11_dlq.rs` scan `table::listed_partitions(count)` (the leg's set from node identities, else every partition). A node that leads every partition answers alone, exactly as before.

## Node API on the cluster listener

`node_api_services` returns one late-bound service (the cluster listener attaches its services before the extension slots exist); `wrap_extensions` builds the router: `ostro_api::router_with(routed cp, routed ext, the client listener's ApiConfig)` behind a node-identity layer (`trusted_node`: the in-memory transport's `PeerNode`, or the verified peer certificate through `PeerIdentity` + `authz::require_node`). Worker certificates and other clusters' nodes get `PERMISSION_DENIED`, no certificate `UNAUTHENTICATED`; a node gets `TrustedNode(id)`, so the 13-03 middleware honours `ostro-partition` / `ostro-partitions` / `ostro-principal` and sets `RequestMeta.from_node`. Unknown paths answer gRPC `UNIMPLEMENTED`; before the router exists, `UNAVAILABLE`.

## Task Commits

1. `0522ca1` feat(13-12): RoutedControlPlane, route table, fan-out, extension routing, node-only API (Tasks 1 + 2)
2. `883b132` test(13-12): tier-B routing scenarios (Task 3)

Both on main through one CAS (base `e3f3bc7`, after 13-04 Task 2, 13-11 and 13-14 landed).

## Verification

- `cargo test -p ostro-server --locked --test p13_routing_unit`: 26 passed (incl. `every_trait_method_classified`, `local_branch_calls_consistency_hooks` with direct and forwarded-in calls per class, hop limit, re-route on not_leader / leader_changed / transport, 503 + retry_after, offline, deadline, unkeyed placement and pin rule, fan-out legs and merge, node registries, node identity rule and the node API router).
- `cargo test -p ostro-repl-conformance --locked --test routing -- --test-threads=1`: 6 passed (parity_through_every_node, not_leader_retry_after_move, offline_partition_503, unkeyed_start_zero_hops_when_local, list_fanout_complete, internal_consumers_route); `--features failpoints ... hop_limit`: 1 passed. Re-run green on the 998fbd1 base (13-09/13-11/13-14/13-15 landed).
- `cargo test -p ostro-server --locked --test cluster_api` (adapted, see deviations): passed. `cargo test -p ostro-api --lib error`: 7 passed.
- Regression subset (local, slot-3): ostro-server `cluster_boot`, `p13_join`, `bootstrap`, `p13_module_gc`; ostro-repl-conformance tier-B groups `placement`, `isr_honesty`, `hw`, `stale_epoch`, `divergence`, `min_insync`, `standby`, `bootstrap`, `fetch_path`, `blobs`, `one_path`, `object_storage`, `failover`, `zombie` (3/3 after deviation 7), `module_gc`, `handoff`, `version_gate`, `p13_harness_smoke`, `harness_smoke`, `skeleton`, `p13_skeleton`: all green.
- `cargo clippy -p ostro-server -p ostro-api -p ostro-repl-conformance --all-targets --locked -- -D warnings` and the same with `--features failpoints` (ostro-server, ostro-repl-conformance): clean. `cargo fmt --all -- --check`: clean.
- Plan-level ci-local: see "Plan-level ci-local" below.

## Deviations from Plan

### Auto-fixed / adapted

1. **[Rule 3] Tasks 1 and 2 in one commit.** Task 1 alone routes calls to peers whose cluster listener does not serve `ostro.api.v1` yet (Task 2), so every forwarded call would answer `UNIMPLEMENTED`. Both commits moved main in one CAS.
2. **[Rule 3] Files outside `files_modified`:** `control/{list,p11_search,p11_dlq}.rs` (one line each: scan `listed_partitions`; without it a fan-out leg scans partitions it does not lead and answers `not_leader`), `ostro-api/src/error.rs` (`Retry-After` for `unavailable` with `retry_after_ms`, plus a unit assertion), `tests/cluster_api.rs` (below). `control/runs.rs` needed no change: 13-02 already calls `pinned_partition()`, and the node-identity rule lives in `table::pinned_partition`.
3. **[Rule 1] `tests/cluster_api.rs` (12-14):** its loop asserted that unkeyed starts on node 1 answer 503 `not_leader` for partitions led elsewhere, which D-P13-14 replaces with forwarding. The requests now carry `ostro-hop: 1` (a call that already took its hop is served locally), so the test still checks the D-P12-21 answer and its details unchanged. Not an oracle or crash checker.
4. **[Adaptation] Classifications:** `HookIngress.deliver` local (its start is routed inside); `queue_demand` summed over every member; `list_partitions` node fan-out with the leader's row per partition; `upgrade_finalize` / `run_module_gc` meta leader. All recorded in the table above.
5. **[Adaptation] `not_leader_retry_after_move`** uses `AssignmentSource::resign` (ResignLeader; the controller elects another ISR member). Resignation publishes leader 0 briefly, and offline is 503 immediately per the plan, so the scenario accepts a 503 + `Retry-After: 1` only inside the move window (from the resignation to the new leader's publication plus two session timeouts) and fails on any other error. Observed runs: 40/40 calls succeeded (one earlier run had two in-window 503s).
6. **[Adaptation] Parity transcript:** the 04-11 transcript lives in `ostro-api/tests/parity.rs` over fakes; the tier-B scenario issues an equivalent transcript against real nodes (workflows, keyed start + dedupe, unkeyed start, get / history / result, list by tag oldest + newest + two pages, signal and cancel of a completed run, absent run on every partition, malformed run id, garbage token, put / get policy), REST and gRPC, compared per transport to a single-node cluster. Normalised: run ids (first-seen order), the per-execution tag, `*_ms` numbers, RFC 3339 timestamps, history details JSON.
7. **[Rule 1] Offline answer keeps the D-P12-21 leader fields.** The Phase 12 `zombie` oracle requires an isolated, self-demoted leader to answer 503 with a leader hint; a self-demoted node masks its view (leader 0), so the router answers `partition_offline`. The answer now names the leader fields (`leader` / `leader_addr` null, message "has no leader"), as D-P12-21 did for offline partitions. Oracle unchanged; `zombie` green 3/3.
8. **[Rule 1] Re-route wait:** returns when the led set changes (a takeover keeps target and epoch) and immediately on a self-demoted node.
9. **[Rule 3] Scenario precondition:** the routing scenarios wait until every node's metadata shows the deployed workflow active before calling through it (a follower's meta read can trail the meta leader until 13-13's read barrier: a 412 "no active version" was seen once on the rebased tree).
10. **[Rule 2] Test binary serialisation:** `tests/routing.rs` runs its scenarios one at a time (a static async mutex) because `route::stale_view` is process-wide under `--features failpoints`.
11. **Worktree / target / CI:** session-scratchpad worktree, `CARGO_TARGET_DIR=target/slot-3`, heavy runs on `ostro-k8s-ci` slot 2.

### Deferred Issues

None.

## Plan-level ci-local

**Not completed (infrastructure).** `ostro-k8s-ci 883b132 2 --no-musl --no-polyglot` was started twice on the landed commit; both runs passed fmt, clippy, the warning-free build, sdk, guests and order-saga and then ended inside the `cargo test` step with `exit 1` and no failing test in the stream (run 1 stopped in `ostro` backup_cli after 7 min, run 2 in `ostro` flow_e2e after 10 min; logs `target/k8s-ci-logs/20261125-031149-s2-883b1328.log` and `20261125-032223-s2-883b1328.log`). Every test that reported before the cut passed. Two earlier submissions of the pre-rebase tree were cancelled by me (one waited over an hour for quota). The orchestrator should rerun the plan-level ci-local (or the parallel mode) on `883b132`; the targeted and regression runs listed under Verification are green.

## Next Phase Readiness / handover

- **13-13:** the hooks are called at the declared points: partition reads (`before_partition_read(p)` for every partition of a local fan-out leg too), meta reads, tokens after successful partition / meta writes (published into the `RequestMeta` out-slot; a forwarded call's token is copied back into the caller's slot). Watcher re-routes: answer `unavailable` with reason `leader_changed` (as `PartitionError::LeaderChanged` already maps in `control/runs.rs`) and the router re-routes once.
- **13-17:** `migrate_runs` fans out one leg per leader with `ostro-partitions`; the local implementation must restrict itself to `route::table::listed_partitions(count)`.
- **13-16 / 13-19:** `upgrade_finalize` and `run_module_gc` reach the meta leader from any node. Forward latency over native TCP + mTLS is still to be measured (tier C).
- **13-18:** routing counters per node: `ostro_server::route::stats(handle.p13().unwrap())` (`forwarded`, `rerouted`, `exhausted`, `offline`, `local`, `forwarded_in`). The failpoint `route::stale_view` is in `route/table.rs`.
- **13-20 (ci.yml):** no hunk: ostro-repl-conformance is already a whole crate in `FAILPOINT_CRATES` and the ci.yml Failpoint suites step, so `hop_limit_no_loop` runs there; the other routing scenarios run in `cargo test --workspace` and in `red_run_all`.
- Known limitation: `rerun_run` / DLQ restart route by the source run's partition; the new run they create is built on partition handles of that node (Phase 11 code), so a rerun whose new run hashes to a partition led elsewhere answers `not_leader` from the source leader. Routing those starts needs the Phase 11 rerun path to start through the routed control plane (13-18 or a follow-up).

## Self-Check

PASSED: `crates/ostro-server/tests/p13_routing_unit.rs`, `crates/ostro-repl-conformance/tests/routing.rs` and every modified file exist at `883b132`; commits `0522ca1` and `883b132` are on main. Plan grep checks: `every_trait_method_classified`, `local_branch_calls_consistency_hooks` in the unit tests; `pinned_partition` in `control/runs.rs`; `fn node_api_services`, `pub struct RoutedControlPlane` in `route/mod.rs`; `pub enum RouteClass` in `route/table.rs`; `GrpcControlPlane::new` in `route/peer.rs`; `before_partition_read` in `route/mod.rs`; all seven scenario names in `scenarios/routing.rs`.
