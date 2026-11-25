---
phase: 13-cluster-membership-routing-upgrades
plan: 16
subsystem: online promotion (`ostro cluster init`) + Phase 13 client and CLI surface
tags: [promotion, cluster-init, ostro-client, cli, insta, tier-b, placement-wiring]
requires:
  - phase: 13-02
    provides: "p13::promote::init stub + ClusterApi delegation, CLI clap tree (cluster/migrate-runs/modules), client stubs, main.rs `cluster join` interception, p13/mod.rs seam"
  - phase: 13-03
    provides: "REST routes + adapters for the cluster / module_lifecycle slots, FakeCluster / FakeModuleLifecycle"
  - phase: 13-09
    provides: "join::ensure_cluster_ca / stored_ca, joiner (run_join), status"
  - phase: 13-10
    provides: "placement controller (RF raise + balancing under a ReplicationPolicy), the p13/mod.rs clock/liveness hunk"
  - phase: 13-15
    provides: "DELETE /v1/modules/{sha} (idempotent), POST /v1/modules/gc"
  - phase: 13-17
    provides: "POST /v1/runs/migrate"
provides:
  - "p13::promote::{init, policy_of, already_cluster, advertise_of, node_cert_covers, REASON_NOT_SUPPORTED_IN_PROFILE, FP_INIT_AFTER_CA, MAX_VOTERS}"
  - "ostro_client::cluster: raw Client methods for every D-P13-23 path + TypedClient methods and request/answer types (InitRequest, NodeTokenRequest, ReassignRequest, MigrateRunsRequest, VersionOrSha, ClusterStatus, ClusterMember, NodeVersion, ReplicationPolicy, Reassignment, NodeToken, UpgradeStatus, MemberLevel, MigrateRunsReport, MigrateFailure, RetiredModule, WorkflowVersionRef, GcReport)"
  - "ostro-cli: cluster::{render, run, status_text, upgrade_text}, migrate_runs::{request, render, run}, modules_lc::{render, run}"
  - "tier-B group promotion (3 scenarios) in scenarios::all()"
  - "server wiring: the placement controller runs on ctx.clock and the 12-10 liveness live set"
affects: [13-18, 13-19, 13-20]
tech-stack:
  added: []
  patterns:
    - "init = idempotent steps with the ReplicationPolicy written last, so its presence means initialised and a cut-short init is completed by running it again"
    - "CLI verbs return their stdout text from `render` (transcript snapshot tests); human output decodes the body into ostro_client types"
key-files:
  created:
    - crates/ostro-server/tests/p13_promote.rs
    - crates/ostro-cli/tests/p13_cli.rs
    - crates/ostro-cli/tests/cluster_contract.rs
    - crates/ostro-cli/tests/snapshots/p13_cli__*.snap (9)
    - crates/ostro-repl-conformance/tests/promotion.rs
  modified:
    - crates/ostro-server/src/p13/promote.rs
    - crates/ostro-server/src/p13/mod.rs (authorised, 13-10 hunk)
    - crates/ostro-cluster/src/placement/controller.rs
    - crates/ostro-cluster/tests/support_placement/mod.rs
    - crates/ostro-client/src/{cluster,client,typed}.rs
    - crates/ostro-cli/src/commands/{cluster,migrate_runs,modules_lc}.rs
    - crates/ostro-api/src/fake.rs
    - crates/ostro-api/tests/phase13_routes.rs
    - crates/ostro-repl-conformance/src/scenarios/{promotion,reassign}.rs
key-decisions:
  - "init writes the ReplicationPolicy last; `already a cluster` = policy present or > 1 non-Removed member, answered 200 with the status (idempotent, nothing written)"
  - "ClusterVersion is written as finalized = the node's level_max (not the LEVEL constant), so an emulated-level node is never fenced by its own init"
  - "no node-certificate hot swap: hostname SANs are not part of node-to-node mTLS (identity URI), the listener identity is fixed at boot from [cluster] advertise; init warns when the advertised host is not covered"
  - "`ostro cluster init` accepts only --advertise-cluster (InitRequest carries one address; --advertise-api / --advertise-repl are a usage error)"
  - "`ostro migrate-runs` exits 4 (as `ostro replay`) when a run diverged or errored; --filter values are tags (`tag=x` or `x`), joined with commas"
  - "PlacementController::new takes the clock; the SystemClock default is gone"
duration: "about 3 h"
completed: 2026-11-25
---

# Phase 13 Plan 16: Online promotion + client/CLI Summary

**`POST /v1/cluster/init` (`ostro cluster init`) turns a running single server-profile node into a joinable cluster in one call (~3–14 ms) without a restart, an export or a leadership change; `ostro-client` and the CLI now cover every Phase 13 endpoint; under load, a promoted node grows to three nodes with RF 3 / min_insync 2, survives the loss of the original node, and grows to five balanced nodes. The placement controller now runs on the server's clock and liveness.**

## Init as landed (`p13::promote::init`)

Preconditions, in order:

| # | check | answer |
|---|-------|--------|
| 1 | dev profile (`TlsProfile::of`: `secrets.auto_sealing_key`) or `--ephemeral` | 409 `conflict`, `details.reason = not_supported_in_profile` |
| 2 | `rf` ∈ {1, 3, 5} (default 3), `1 ≤ min_insync ≤ rf` (default `rf / 2 + 1`) | 400 `invalid_argument` |
| 3 | ReadIndex barrier, then already a cluster: `ReplicationPolicy` present or > 1 non-Removed member | 200 with the current status, nothing written |
| 4 | advertised cluster address: the request's `advertise`, else the first `[cluster] advertise` entry (a bare host / IP gets the bound listener port; `host:port`, `[v6]:port` and `scheme://…` are kept) | none → 400 `invalid_argument` |

Steps (each idempotent):

1. own `Member` (CAS read-modify-write, unknown ext tags kept, Removed refused 409): `cluster_addr` = advertise, `MemberState` Active (`changed_at_ms` from the injected clock), `NodeVersion` = `ctx.node_version()`;
2. `join::ensure_cluster_ca` (13-09; create-only `__ostro/cluster-ca` from `<data>/tls/ca.{crt,key}`); no stored CA afterwards → 503; failpoint `member::init_after_ca` (503 `failpoint …`);
3. `ClusterInfo` ext `ClusterVersion{finalized: level_max, hold: None, finalized_at_ms: now}` only when absent (an undecodable tag 98 is an error, never overwritten);
4. node-certificate check: warns when the advertised host is not among `[cluster] advertise` + loopback (see Deviations 3);
5. `ClusterInfo` ext `ReplicationPolicy{rf, min_insync, max_voters: 5}` (CAS), then `placement_trigger.notify()` and the status (behind the barrier).

The cluster id is untouched (the one resolved at boot). Partitions keep their leader and epoch (asserted in `p13_promote` and in the tier-B scenario); `min_insync` rises only through the controller's Shrink step (13-10).

## CLI verbs (exit codes: Phase 10 contract)

| verb | REST | human output | exit |
|------|------|--------------|------|
| `ostro cluster init [--advertise-cluster A] [--rf N] [--min-insync N]` | POST /v1/cluster/init | headline + status | 0; 1 on 400/409/503; 2 for `--advertise-api` / `--advertise-repl` |
| `ostro cluster join <SEED> --token T --ca-hash H [--data D] [--advertise-*] [--no-start]` | the ostro binary → 13-09 joiner (+ server start unless `--no-start`) | — | `run`/`render` refuse it with 2 (binary-only) |
| `ostro cluster remove <ID> [--force]` | POST /v1/cluster/members/{id}/remove | headline + status | 0; 1 (404, 409 quorum, 503) |
| `ostro cluster drain <ID>` | POST /v1/cluster/members/{id}/drain | headline + status | 0; 1 |
| `ostro cluster status` | GET /v1/cluster/status | cluster line, policy line, member table (NODE STATE ROLE LIVE LEVELS API CLUSTER), joint config, reassignments table | 0; 1 (503 not_leader) |
| `ostro cluster token create --node [--ttl D]` | POST /v1/cluster/tokens | token id, expiry, the full `ostro cluster join <server> --token … --ca-hash …` line (shown once) | 0; 1 (412 before init); 2 without `--node` |
| `ostro cluster reassign --partition P (--to a,b,c \| --cancel)` | POST /v1/cluster/reassign | headline + status | 0; 1 (409 in progress, 404 no move, 412 feature_not_enabled); 2 neither flag |
| `ostro cluster rebalance` | POST /v1/cluster/rebalance | headline + status | 0; 1 (412) |
| `ostro cluster upgrade {status,finalize,hold,release}` | GET /v1/cluster/upgrade, POST …/{finalize,hold,release} | level line (+ "level N can be finalized"), member table | 0; 1 (409 held, 503) |
| `ostro migrate-runs --workflow W --from V\|sha --to V [--filter tag=x …] [--limit N] [--dry-run]` | POST /v1/runs/migrate | counts, failed table, busy / seq-mismatch lists | 0; **4** when a run diverged/errored; 1 (404, 409 module_retired, 412); 2 non-tag filter |
| `ostro modules rm <SHA>` | DELETE /v1/modules/{sha} | retired at, versions, GC hint | 0 (idempotent); 1 (400, 404, 409 module_active, 412) |
| `ostro modules gc` | POST /v1/modules/gc | candidates / deleted / referenced, or "postponed: reason" | 0; 1 (412) |

`--json` prints the response body (or the error body) exactly. Snapshots: `crates/ostro-cli/tests/snapshots/p13_cli__{cluster_init,cluster_status,cluster_token,cluster_remove_drain,cluster_reassign_rebalance,cluster_upgrade,migrate_runs,modules_rm,modules_gc}.snap` (transcripts with every error path).

## Promotion timings

| measurement | value |
|-------------|-------|
| `POST /v1/cluster/init` (real TCP/mTLS, local, runs in flight) | 13.7–14.0 ms |
| `cluster init` (tier B, in-process, cluster pod) | 2.8 ms |
| two joins after init (tier B, cluster pod) | 61.4 s (≈ 30 s per `add_node`, as 13-10's rf_raised_on_join) |
| **time to RF 3** on all 4 partitions after the second join returned | **0.55 s** (61.9 s from the first join incl. 3 meta voters) |
| node 1 killed → every partition led by a survivor | 6.1 s from the kill (meta leader 2) |
| workload over the whole scenario | 627 runs acknowledged, all completed, longest ack gap 1.42 s (includes the kill) |
| grow to five: four joins / balanced RF 3 after | 141.7 s / +6 ms; replica counts {1: 2, 2: 2, 3: 3, 4: 3, 5: 2}; 1281 runs, longest gap 0.23 s |

Leaders stayed on node 1 for every partition until the kill (the default `leader_rebalance_interval` is 5 min; the scenario would accept a RESIGN handoff and fails on any other leader change).

## Task Commits

| # | commit | message |
|---|--------|---------|
| 0 | f0f1590 | fix(13-16): placement controller on the server's clock and liveness (authorised) |
| 1 | c884c47 | feat(13-16): online single-node promotion |
| 2 | 23e8974 | feat(13-16): ostro-client cluster module + CLI verbs |
| 3 | 3b978c0 | test(13-16): tier-B promotion scenarios |
| 3b | 652c46e | fix(13-16): cluster init help names what init accepts |
| 3c | e8ee29c | fix(13-16): typed route-coverage test calls the Phase 13 methods |

## Verification

Local (`target/slot-2`, nice 10, 6 jobs), each with a non-zero test count:

| run | result |
|-----|--------|
| `-p ostro-server --test p13_promote -- --test-threads=1` | 3 passed (61 s) |
| `-p ostro-server --features failpoints --test p13_promote -- --test-threads=1` | 4 passed (crash after CA + re-init) |
| `-p ostro-server --lib promote` | 3 passed |
| `-p ostro-cluster --test placement_controller --test reassign_mutants -- --test-threads=1` | 9 + 0 passed (after the `new(clock)` change) |
| `-p ostro-cli --test p13_cli --test cluster_contract` | 10 + 2 passed |
| `-p ostro-cli --lib` | 80 passed (clap parse tests unchanged) |
| `-p ostro-client --lib`, `--test contract` (ROUTES ⊂ OpenAPI incl. the 15 P13 routes), `--test typed` (after e8ee29c) | 12, 5, 4 passed |
| `-p ostro-api --features fake --test phase13_routes --test grpc_client_parity`, `--lib fake` | 3 + 9, 15 passed |
| `ostro cluster --help` lists `upgrade`; `ostro migrate-runs --help` lists `--dry-run` | ok |
| clippy `-D warnings` (ostro-server ± failpoints lib + p13_promote, ostro-cli / ostro-client / ostro-api all targets, ostro-api `fake`, ostro-repl-conformance all targets), `cargo fmt`, `scripts/p12-grep-gates.sh` | clean |

Cluster (slot 2): `-p ostro-repl-conformance --test promotion -- --test-threads=1` 3 passed (226 s); first attempt failed all but the dev scenario on my own bug (the scenario runner starts nodes with `ClusterHarness::start`, so `[cluster] advertise` was empty → init answered 400; the scenarios now pass `advertise` like the CLI does).

**Plan-level ci-local:** `ostro-k8s-ci --parallel` at **652c46e** (full run incl. musl + polyglot, 78 min). PASS: fmt, clippy, build, sdk fmt/clippy/test, guests, order-saga, polyglot toolchains / python / ts / conformance / clippy, clients python / ts / go, ostro-build polyglot, sleepers, failpoint list sync, **failpoint suites** (fp1+fp2+fp3, incl. `ostro-repl-conformance --features failpoints` with `promotion`), replication mutant matrix, server campaign smoke (20 kills), phase 7 legs, deny, vendored, deny fixtures, forbidden deps, P12 grep gates, workflow validation, musl release, musl zigbuild test in musl1. FAIL:
  - `cargo test` (test2): `worker_failover::oscillation` (known flaky); cargo's fail-fast then skipped the other 326 test2 targets. Re-run on slot 2 with `--no-fail-fast` (64 min): every target green except `ostro-client --test typed` (**my bug**: the route-coverage test did not call the new methods; fixed in e8ee29c), `ostro-server --test tls_ca` (`CA + node cert took 2.9 s`, a timing bound) and `--test worker_session` (`elapsed < 1 s`, timing). All three green alone on slot 2 at e8ee29c (4 + 8 + 18 passed). test1 was green.
  - `musl zigbuild test` (musl2): `worker_failover::sustained_overload` (known flaky); the targets fail-fast skipped there ran natively in the slot-2 re-run above (not re-run under musl).
  - **musl binary size 60.6 MB** (> 60): main was at 60.4 MB (another agent is fixing it); this plan adds about 0.2 MB (client types, CLI rendering, promote).

## Deviations from Plan

1. **[Authorised by the orchestrator] 13-10's `p13/mod.rs` hunk applied** (f0f1590): the controller gets `ctx.clock` and `ctx.cluster.liveness().lock().live_set(u)`. The `SystemClock` clock-exempt default it replaced is removed: `PlacementController::new(meta, handoff, cfg, clock)`; `with_clock` is gone (only the server and `support_placement` built controllers; both pass the clock). The second `SystemClock` (clock-exempt) in `request_rebalance` stays: the 13-02 API signature carries no clock (could take `ctx.clock.now_ms()` like `request_reassign`; 13-10/13-18 follow-up).
2. **[Rule 3] Contract test location.** The plan names `crates/ostro-client/tests/cluster_contract.rs` against `ostro_api::router`; ostro-client has no `ostro-api` dev-dependency and manifests are 13-01-only, so the test is `crates/ostro-cli/tests/cluster_contract.rs` (ostro-cli already has `ostro-api` with `fake` and depends on ostro-client). The ostro-client side: request/answer types + typed methods in `cluster.rs`, the 15 routes in `typed::ROUTES` (checked against the built binary's OpenAPI by the existing `tests/contract.rs`).
3. **Node certificate: no re-issue / hot swap.** No swap point exists for the cluster listener (its `ServerTlsConfig` identity is built once in `cluster.rs`, a 13-02-only file), node-to-node mTLS authenticates the `ostro://<cluster>/node/<id>` URI SAN (hostnames are not checked between nodes), and a restart re-derives the certificate from `[cluster] advertise` (`load_or_issue_node_cert`), so a re-issued certificate would flip back. Init therefore warns when the advertised host is not covered; workers dialling that host need it in `[cluster] advertise`.
4. **`ostro cluster init --advertise-api / --advertise-repl` are refused (exit 2).** `InitRequest` (13-01 API contract, OpenAPI + proto) carries one `advertise` (the cluster address); the API and replication addresses of a promoted node are its listeners. The flags stay in the clap tree (13-02's shape) with help text saying so (652c46e).
5. **Gate status 412.** As 13-10 noted, `feature_not_enabled` from the cluster API is 412 (13-02 mapping); the snapshots use 412 where the plan says 409. `module_active` is 409.
6. **[Rule 1] 13-03 fakes follow the server** (orchestrator note on 13-15's finding): `FakeCluster::init` answers the status on a second init (the real init is idempotent; was 409 `already_cluster`), `FakeModuleLifecycle::retire_module` answers the first retirement again (was 409 `module_retired`) and an active module is 409 `module_active` (was a reason-less 409). `ostro-api/tests/phase13_routes.rs` asserts the new behaviour; `grpc_client_parity` is unchanged and green.
7. **`migrate-runs --filter`.** 13-02's help said `key=value` (`status=running`, `tag=x`); 13-17's server takes a comma-separated tag list. Values are tags (`tag=x` or `x`); other keys are a usage error.
8. **`migrate-runs` exit 4** when the report has failed runs (Phase 10's divergence code, as `ostro replay`); the plan only named the error codes.
9. **Init `ClusterVersion` = `level_max`** (the node's emulated level), not the `LEVEL` constant, so `start_single_with(Some(1))` nodes are not fenced by their own init.
10. **`reassign.rs` touched** (13-10's file): `Load` and the oracle/wait helpers are `pub(super)` so `promotion` reuses them; no behaviour change.
11. **ostro-client `client.rs` / `typed.rs`:** `request`, `send`, `get`, `post` and `decode` are `pub(crate)` (the cluster module is a sibling).
12. **[Rule 1] `ostro-client/tests/typed.rs`** (e8ee29c): its route-coverage test requires every `typed::ROUTES` entry to be called; it now calls the 15 Phase 13 methods (found by the plan check).
13. **Worktree / target:** scratchpad worktree, `target/slot-2`; the plan's `/tmp/ostro-wt-13-16` / `/tmp/ostro-target-13-16` not used (executor brief).

## ci.yml / ci-local hunk (not applied; 13-20 / 13-01 own both files)

The crash-and-rerun test only runs with failpoints. Add to `FAILPOINT_CRATES` in `scripts/ci-local.sh`: `ostro-server:p13_promote`, and to the ci.yml "Failpoint suites" step:

```yaml
          cargo test -p ostro-server --features failpoints --test p13_promote --locked -- --test-threads=1
```

The non-failpoint `p13_promote`, `p13_cli`, `cluster_contract` and `promotion` run in the existing cargo test steps (`ostro-repl-conformance --features failpoints` also runs `promotion`).

## Handover

- **13-02 owner / follow-up:** on restart `cluster.rs` calls `members::upsert_self` with `cluster_addr = <bound listener>` and overwrites the advertised `cluster_addr` init (and a joiner) wrote, e.g. back to `0.0.0.0:7701`; joins through a restarted promoted node would get that as a seed. Fix: keep the stored `cluster_addr` unless `[cluster] advertise` names one, or derive it like `promote::advertise_of`.
- **13-18:** `promote_running_single_node_to_three` is a candidate for the `reassign::mutant::raise_min_insync_early` row (RF never rises under the mutant). The harness's `P13Harness::cluster_init` is not used by the scenarios (the runner starts nodes with `ClusterHarness::start`, so `[cluster] advertise` is empty there); `start_single` sets it.
- **13-19 (tier C):** `ostro cluster init --advertise-cluster <host:port>` against the real binary; `ostro cluster token create --node` prints the full join line.
- **13-20:** the ci hunk above; `not_supported_in_profile` is a new `details.reason` (RESERVATIONS reasons list); docs: 412 vs 409 for `feature_not_enabled`.
- **13-10 follow-up:** `request_rebalance` could take the injected clock (Deviation 1).

## Self-Check

## Self-Check: PASSED

- FOUND on main: f0f1590, c884c47, 23e8974, 3b978c0, 652c46e, e8ee29c
- FOUND: tests/p13_promote.rs, ostro-cli tests/p13_cli.rs + 9 snapshots, tests/cluster_contract.rs, ostro-repl-conformance tests/promotion.rs
- promote.rs contains `pub async fn init` and `ensure_cluster_ca`; cli cluster.rs contains `Upgrade` and `ostro_client::cluster`; scenarios/promotion.rs names all three scenarios
