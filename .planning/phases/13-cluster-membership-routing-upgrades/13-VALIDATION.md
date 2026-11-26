---
phase: 13
slug: cluster-membership-routing-upgrades
status: gate-run (phase verification pending)
nyquist_compliant: true
wave_0_complete: true
created: 2026-11-19
---

# Phase 13: Validation Strategy

> This is the per-phase validation contract for feedback sampling during execution.
> Source: 13-RESEARCH.md § Validation Architecture, mapped onto plans 13-01..13-20 (revised after the plan check). Adopted decisions: 13-DECISIONS.md (D-P13-1..D-P13-29).

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Rust built-in test harness (`cargo test`, Rust 1.100), proptest 1.12 + proptest-state-machine 0.9 (tier A: `ostro-cluster` `reconcile_model`, `placement_model`, `reassign_model`), `fail` 0.5.1 failpoints (`member::`, `reassign::`, `route::`, `upgrade::`, `gc::`, `handoff::`, `migrate::` + the eight permanent `*::mutant::*`), the Phase 12 `ostro-repl-conformance` harness extended with `P13Harness`: tier B in-process clusters over the in-memory tonic transport + FaultLayer (per-node `level_max_override`); tier C spawned failpoint `ostro` processes on `127.r.r.n` behind the Rust chaos proxy with real `kill -9` and `OSTRO_TEST_LEVEL_MAX` emulation. Oracles: Phase 12 (disk, replica identity, standby, LA rule, ISR-only election) + Phase 13 (RYW session checker, membership, upgrade, GC). |
| **Config** | No new config file. `FAILPOINT_CRATES` unchanged (ostro-cluster and ostro-repl-conformance already in it since 12-02, the revised Phase 12 Cargo owner), kept equal to the ci.yml "Failpoint suites" step. 13-20 adds `check_p13_mutants` (`OSTRO_P13_REQUIRE_ALL=1`), the `wit-next-fixture` engine step and `check_p13_gates` to both ci-local and ci.yml. |
| **External services** | None. No Docker. **The microk8s cluster reachable via kubectl on this machine is never used.** Real previous-binary upgrade (`OSTRO_PREV_BIN`) is human_needed until a release tag exists. |
| **Quick run command** | `cargo test -p ostro-cluster --locked --test <plan test>` · `cargo test -p ostro-raft-driver --locked -- membership_tests` · `cargo test -p ostro-engine --locked --test linker_set` · `cargo test -p ostro-repl-conformance --locked --test <scenario group> -- --test-threads=1` |
| **Full suite command** | `bash scripts/ci-local.sh --no-musl` (includes the failpoint suites; from 13-20 also the P13 mutant matrix and the fixture suite) |
| **Phase gate commands** | 1. `bash scripts/ci-local.sh` (with musl).<br>2. `OSTRO_P13_REQUIRE_ALL=1 cargo test -p ostro-repl-conformance --features failpoints --locked --test mutants_p13 -- --test-threads=1 --nocapture` (≥ 8 `FAILED as expected`, no `SKIP`).<br>3. `OSTRO_REPL_REQUIRE_ALL=1 … --test mutants` (Phase 12 matrix still ≥ 11).<br>4. `cargo test -p ostro-engine --features wit-next-fixture --locked --test linker_set`.<br>5. `cargo build -p ostro --features failpoints --target-dir target/failpoints --locked`, then `OSTRO_UPGRADE_ITERS=3 cargo test --release -p ostro-repl-conformance --features failpoints --locked --test process_rolling_upgrade -- --ignored --test-threads=1`.<br>6. `cargo test --release -p ostro-repl-conformance --features failpoints --locked --test process_promotion --test process_ryw --test process_downgrade --test process_shutdown -- --ignored --test-threads=1`.<br>7. `OSTRO_CAMPAIGN_FAULTS=10000 cargo test --release -p ostro-repl-conformance --features failpoints --locked --test cluster_campaign_p13 -- --ignored --nocapture --test-threads=1` (4 h box → HUMAN_NEEDED marker).<br>All tier-C gate runs export the pinned absolute `OSTRO_P13_REPORT_DIR=/tmp/ostro-p13-gate` (plan check W14).<br>8. `OSTRO_PREV_BIN=<prev> … --test upgrade_from_prev -- --ignored` (human_needed). |
| **Estimated runtime** | Quick: < 60 s per command. Tier-B scenario group: 20–120 s. Full ci-local: ~30–40 min debug. Rolling upgrade ×3 (tier C): 20–40 min. Campaign 10,000 faults: hours (time-boxed). |

---

## Sampling Rate

- **After every task commit:** the task's `<automated>` command, plus `cargo clippy -p <crate> --all-targets --locked -- -D warnings` for touched crates (with `--features failpoints` where failpoint tests exist).
  - Tasks touching `ostro-partition` also run `PROPTEST_CASES=128 cargo test -p ostro-partition --locked --test rebuild_digest` and `--test format_corpus`.
  - Tasks touching `ostro-meta` also run `cargo test -p ostro-meta --locked` (goldens append-only).
  - Tasks touching `ostro-engine` also run `cargo test -p ostro-engine --locked --test replay_corpus`.
- **After every plan / wave:** `bash scripts/ci-local.sh --no-musl` in a detached `/tmp` worktree at the plan's last commit (Worktree procedure); from wave 3 on also the plan's tier-B scenario group plus the Phase 12 tier-B suite.
- **Before `/gsd:verify-work`:** the 13-20 gate.
- **Max feedback latency:** 60 s for task-level unit checks; tier-B scenario groups ≤ 2 min; tier C and measurements are gate level.
- Piped verify commands use `set -euo pipefail`; cargo test filters go after `--`; no `|| true` masking; filtered runs must report ≥ 1 passed test (`test result: ok\. [1-9][0-9]* passed`).

---

## Wave Structure

| Wave | Plans | Exclusive shared files |
|------|-------|------------------------|
| 1 | 13-01 gate (Phase 12 + 9–11 complete) + RESERVATIONS/STATE + manifests (first) + library contracts | **Cargo files**, ci sync, RESERVATIONS, STATE, `ostro-types` compat/command, meta command/ext/state stubs, partition apply arms + `store/mod.rs` NewerFormat, ostro-cluster module list/contracts/constructor stubs, engine LinkerSet stub, ostro-api phase13 + request_meta + grpc_client stub |
| 2 | 13-02 server/CLI/harness seam; 13-04 compat gate + partition fail-stop; 13-05 driver FatalApply/transfer/ReadIndex/progress + MetaSm fail-stop + MetaHandle + lease anchor; 13-06 pure algorithms; 13-07 LinkerSet | 13-02: **server.rs / cluster.rs / config.rs / main.rs / p13/** , route + control stubs, client/CLI stubs, conformance module lists + harness trait. 13-04: ostro-cluster `compat/upgrade`, partition `apply/mod.rs`. 13-05: **vendor driver + VENDORED.md**, meta `sm.rs`/`command.rs`/`handle.rs`, `meta_peer.rs`. 13-06: `membership/reconcile.rs`, `placement/{plan,reassign,leaders}.rs`. 13-07: **ostro-engine** |
| 3 | 13-03 API wire surface; 13-08 harness + oracles | 13-03: **ostro-proto**, worker proto, ostro-api adapters/client/OpenAPI, `p13/node_ops.rs` type swap. 13-08: conformance `harness/{inproc,process}`, `oracle/p13_*` |
| 4 | 13-09 membership; 13-11 handoff + shutdown; 13-12 routing; 13-14 worker compat + upload gate + pre-warm; 13-15 module retire + GC | 13-09: `address_book`, `members.rs`, `membership/{service,remove}`, `p13/{membership,join,joiner,status}`. 13-11: partition `handoff.rs` + `leader/{dispatcher,mod}.rs`, server `executor.rs`, cluster `handoff/shutdown`. 13-12: `route/{mod,peer,table,fanout}`, `control/runs.rs`. 13-14: `workers/*`, worker `session.rs`, `control/modules.rs`, `p13/prewarm.rs`. 13-15: `control/module_retire.rs`, `p13/{module_gc,module_refs}`. Each owns its scenario + test files. |
| 5 | 13-10 reassignment; 13-13 RYW + watchers + controller wait; 13-17 RepinRun + migrate-runs + module_gone | 13-10: meta `state.rs`, `placement/controller.rs`, partition `roles.rs` (if needed). 13-13: `route/consistency.rs`, `control/watch.rs`, cluster `controller.rs`. 13-17: partition `apply/repin.rs`, `control/migrate.rs`, server `executor.rs` |
| 6 | 13-16 promotion + client + CLI | `p13/promote.rs`, ostro-client `cluster.rs`, CLI command files + snapshots |
| 7 | 13-18 tier-B suite green + P13 mutant matrix | sole plan: integration scenarios, `mutants_p13`, fixes across Phase 13 product files |
| 8 | 13-19 tier C | conformance process test files, `scenarios/process_p13.rs` |
| 9 | 13-20 gate | ci files, docs, RESERVATIONS, REQUIREMENTS, STATE, VALIDATION, DECISIONS |

------|-------|------------------------|
| 1 | 13-01 gate (Phase 12 + 9–11 complete) + RESERVATIONS/STATE + manifests (first) + library contracts | **Cargo files**, ci sync, RESERVATIONS, STATE, `ostro-types` compat/command, meta command/ext/state stubs, partition apply arms + `store/mod.rs` NewerFormat, ostro-cluster module list/contracts/constructor stubs, engine LinkerSet stub, ostro-api phase13 + request_meta + grpc_client stub |
| 2 | 13-02 server/CLI/harness seam; 13-04 compat gate + partition fail-stop; 13-05 driver FatalApply/transfer/ReadIndex/progress + MetaSm fail-stop + MetaHandle + lease anchor; 13-06 pure algorithms; 13-07 LinkerSet | 13-02: **server.rs / cluster.rs / config.rs / main.rs / p13/** , route + control stubs, client/CLI stubs, conformance module lists + harness trait. 13-04: ostro-cluster `compat/upgrade`, partition `apply/mod.rs`. 13-05: **vendor driver + VENDORED.md**, meta `sm.rs`/`command.rs`/`handle.rs`, `meta_peer.rs`. 13-06: `membership/reconcile.rs`, `placement/{plan,reassign,leaders}.rs`. 13-07: **ostro-engine** |
| 3 | 13-03 API wire surface; 13-08 harness + oracles | 13-03: **ostro-proto**, worker proto, ostro-api adapters/client/OpenAPI, `p13/node_ops.rs` type swap. 13-08: conformance `harness/{inproc,process}`, `oracle/p13_*` |
| 4 | 13-09 membership; 13-11 handoff + shutdown; 13-12 routing; 13-14 worker compat + upload gate + pre-warm; 13-15 module retire + GC | 13-09: `address_book`, `members.rs`, `membership/{service,remove}`, `p13/{membership,join,joiner,status}`. 13-11: partition `handoff.rs` + `leader/{dispatcher,mod}.rs`, server `executor.rs`, cluster `handoff/shutdown`. 13-12: `route/{mod,peer,table,fanout}`, `control/runs.rs`. 13-14: `workers/*`, worker `session.rs`, `control/modules.rs`, `p13/prewarm.rs`. 13-15: `control/module_retire.rs`, `p13/{module_gc,module_refs}`. Each owns its scenario + test files. |
| 5 | 13-10 reassignment; 13-13 RYW + watchers + controller wait; 13-17 RepinRun + migrate-runs + module_gone | 13-10: meta `state.rs`, `placement/controller.rs`, partition `roles.rs` (if needed). 13-13: `route/consistency.rs`, `control/watch.rs`, cluster `controller.rs`. 13-17: partition `apply/repin.rs`, `control/migrate.rs`, server `executor.rs` |
| 6 | 13-16 promotion + client + CLI | `p13/promote.rs`, ostro-client `cluster.rs`, CLI command files + snapshots |
| 7 | 13-18 tier-B suite green + P13 mutant matrix | sole plan: integration scenarios, `mutants_p13`, fixes across Phase 13 product files |
| 8 | 13-19 tier C | conformance process test files, `scenarios/process_p13.rs` |
| 9 | 13-20 gate | ci files, docs, RESERVATIONS, REQUIREMENTS, STATE, VALIDATION, DECISIONS |

------|-------|------------------------|
| 1 | 13-01 gate (Phase 12 + 9–11 complete) + RESERVATIONS/STATE + manifests (first) + library contracts | **Cargo files**, ci sync, RESERVATIONS, STATE, `ostro-types` compat/command, meta command/ext/state stubs, partition apply arms + `store/mod.rs` NewerFormat, ostro-cluster module list/contracts/constructor stubs, engine LinkerSet stub, ostro-api phase13 + request_meta + grpc_client stub |
| 2 | 13-02 server/CLI/harness seam; 13-04 compat gate + partition fail-stop; 13-05 driver FatalApply/transfer/ReadIndex/progress + MetaSm fail-stop + MetaHandle + lease anchor; 13-06 pure algorithms; 13-07 LinkerSet | 13-02: **server.rs / cluster.rs / config.rs / main.rs / p13/** , route + control stubs, client/CLI stubs, conformance module lists + harness trait. 13-04: ostro-cluster `compat/upgrade`, partition `apply/mod.rs`. 13-05: **vendor driver + VENDORED.md**, meta `sm.rs`/`command.rs`/`handle.rs`, `meta_peer.rs`. 13-06: `membership/reconcile.rs`, `placement/{plan,reassign,leaders}.rs`. 13-07: **ostro-engine** |
| 3 | 13-03 API wire surface; 13-08 harness + oracles | 13-03: **ostro-proto**, worker proto, ostro-api adapters/client/OpenAPI, `p13/node_ops.rs` type swap. 13-08: conformance `harness/{inproc,process}`, `oracle/p13_*` |
| 4 | 13-09 membership; 13-11 handoff + shutdown; 13-12 routing; 13-14 worker compat + upload gate + pre-warm; 13-15 module retire + GC | 13-09: `address_book`, `members.rs`, `membership/{service,remove}`, `p13/{membership,join,joiner,status}`. 13-11: partition `handoff.rs` + `leader/{dispatcher,mod}.rs`, server `executor.rs`, cluster `handoff/shutdown`. 13-12: `route/{mod,peer,table,fanout}`, `control/runs.rs`. 13-14: `workers/*`, worker `session.rs`, `control/modules.rs`, `p13/prewarm.rs`. 13-15: `control/module_retire.rs`, `p13/{module_gc,module_refs}`. Each owns its scenario + test files. |
| 5 | 13-10 reassignment; 13-13 RYW + watchers + controller wait; 13-17 RepinRun + migrate-runs + module_gone | 13-10: meta `state.rs`, `placement/controller.rs`, partition `roles.rs` (if needed). 13-13: `route/consistency.rs`, `control/watch.rs`, cluster `controller.rs`. 13-17: partition `apply/repin.rs`, `control/migrate.rs`, server `executor.rs` |
| 6 | 13-16 promotion + client + CLI | `p13/promote.rs`, ostro-client `cluster.rs`, CLI command files + snapshots |
| 7 | 13-18 tier-B suite green + P13 mutant matrix | sole plan: integration scenarios, `mutants_p13`, fixes across Phase 13 product files |
| 8 | 13-19 tier C | conformance process test files, `scenarios/process_p13.rs` |
| 9 | 13-20 gate | ci files, docs, RESERVATIONS, REQUIREMENTS, STATE, VALIDATION, DECISIONS |

------|-------|------------------------|
| 1 | 13-01 gate (Phase 12 + 9–11 complete) + contracts + seam + deps | **Cargo files**, RESERVATIONS, STATE, every `lib.rs`/`mod.rs` line, `ostro-types` compat/command, meta command/ext shapes, partition apply arms, **server.rs / cluster.rs / config.rs / main.rs / p13/mod.rs**, ostro-api phase13 + request_meta, CLI variants, harness trait |
| 2 | 13-03 API surface; 13-04 compat + fail-stop; 13-05 driver + MetaHandle; 13-06 pure algorithms; 13-07 LinkerSet; 13-08 harness + oracles | 13-03: **ostro-proto**, worker proto, ostro-api adapters/client/OpenAPI, `p13/node_ops.rs`. 13-04: ostro-cluster `compat/upgrade`, partition `apply/mod.rs`, meta `sm.rs`/`command.rs`. 13-05: **vendor driver + VENDORED.md**, meta `handle.rs`, ostro-cluster `meta_peer.rs`. 13-06: `membership/reconcile.rs`, `placement/{plan,reassign,leaders}.rs`. 13-07: **ostro-engine**. 13-08: conformance `harness/{inproc,process}`, `oracle/p13_*` |
| 3 | 13-09 membership; 13-10 reassignment; 13-11 handoff + shutdown; 13-12 routing; 13-13 RYW + watchers; 13-14 worker compat + upload gate + pre-warm; 13-15 module retire + GC | 13-09: `address_book`, `membership/{service,remove}`, `p13/{membership,join,joiner,status}`. 13-10: meta `state.rs`, `placement/controller.rs`. 13-11: partition `handoff.rs` + `leader/dispatcher.rs`, server `executor.rs`, cluster `handoff/shutdown`. 13-12: `route/{mod,peer,table,fanout}`, `control/runs.rs`. 13-13: `route/consistency.rs`, `control/watch.rs`. 13-14: `workers/*`, worker `session.rs`, `control/modules.rs`, `p13/prewarm.rs`. 13-15: `control/module_retire.rs`, `p13/{module_gc,module_refs}`. Each owns its scenario + test files. |
| 4 | 13-16 promotion + client + CLI; 13-17 RepinRun + migrate-runs + module_gone | 13-16: `p13/promote.rs`, ostro-client `cluster.rs`, CLI command files + snapshots. 13-17: partition `apply/repin.rs`, `control/migrate.rs`, server `executor.rs` |
| 5 | 13-18 tier-B suite green + P13 mutant matrix | sole plan: integration scenarios, `mutants_p13`, fixes across Phase 13 product files |
| 6 | 13-19 tier C (promotion, rolling upgrade, RYW, downgrade, shutdown, prev-binary hook, campaign) | conformance process test files, `scenarios/process_p13.rs` |
| 7 | 13-20 gate | ci files, docs, RESERVATIONS, REQUIREMENTS, STATE, VALIDATION, DECISIONS |

---

## Per-Task Verification Map

All commands run from the plan's worktree `$WT=/tmp/ostro-wt-13-NN` with `CARGO_TARGET_DIR=/tmp/ostro-target-13-NN` (13-DECISIONS procedure).

| Task ID | Plan | Wave | Requirements | Task | Automated command(s) | Status |
|---------|------|------|--------------|------|----------------------|--------|
| 13-01-01 | 01 | 1 | CLUS-05, CLUS-06, CLUS-07, ENG-10, API-08 | Phase 12 (+9–11) completion gate, seam signatures, route inventory, reservations check, RESERVATIONS rows, decisions in STATE.md | `grep / doc checks` | ✅ green |
| 13-01-02 | 01 | 1 | CLUS-05, CLUS-06, CLUS-07, ENG-10, API-08 | Manifests + features (first), then contracts: compat registry, RepinRun + AlterReplicas shapes, ext structs 96–102, RejectReasons, HandoffControl, ostro-cluster contracts + stubs, LinkerSet stub, ostro_api::phase13 + RequestMeta | `cargo deny check`; `--features failpoints`; `-p ostro-types -p ostro-meta -p ostro-cluster --lib -p ostro-api --lib`; `-p ostro-partition --test rebuild_digest` | ✅ green |
| 13-02-01 | 02 | 2 | CLUS-05, CLUS-06, CLUS-07, ENG-10, API-08 | p13 seam with fixed call sites (both voter checks, boot rule, fence, routing wrap, start, shutdown) + config keys | `-p ostro-server --test config -p ostro-server -p ostro` | ✅ green |
| 13-02-02 | 02 | 2 | CLUS-05, CLUS-06, CLUS-07, ENG-10, API-08 | Client/CLI stubs + P13Harness trait + stub oracle/scenario/mutant modules | `-p ostro-repl-conformance --test p13_skeleton -p ostro-cli -p ostro-client` | ✅ green |
| 13-03-01 | 03 | 3 | API-08, CLUS-05, CLUS-06, CLUS-07 | Protos: ostro.api.v1 Cluster + ModuleLifecycle, ostro.cluster.v1 Membership + NodeOps, worker Hello feature_level | `-p ostro-proto --test p13_wire`; `-p ostro-worker-proto --test wire` | ✅ green |
| 13-03-02 | 03 | 3 | API-08, CLUS-05, CLUS-06, CLUS-07 | REST + gRPC adapters for ClusterApi / ModuleLifecycleApi, RequestMeta parse/emit, fakes, OpenAPI | `-p ostro-api --test phase13_routes`; `-p ostro-api` | ✅ green |
| 13-03-03 | 03 | 3 | API-08, CLUS-05, CLUS-06, CLUS-07 | Production GrpcControlPlane (ControlPlane + every extension trait) with metadata propagation and lossless errors | `-p ostro-api --test grpc_client_parity` | ✅ green |
| 13-04-01 | 04 | 2 | CLUS-06 | Gate core (require, LevelSource, finalize_decision pure) + finalizer, hold/release/finalize | `-p ostro-cluster --test upgrade_finalize`; `-p ostro-cluster --test compat_mutants --features failpoints` | ✅ green |
| 13-04-02 | 04 | 2 | CLUS-06 | Fail-stop on unknown input in partition apply (unknown tag, unknown fields, emulated level); the MetaSm half is 13-05 (plan check B6) | `-p ostro-partition --test apply_newer_format`; `-p ostro-partition --test apply_newer_format --features failpoints`; `-p ostro-partition --test format_corpus`; `-p ostro-partition --test rebuild_digest` | ✅ green |
| 13-04-03 | 04 | 2 | CLUS-06 | NodeVersion advertiser, boot fence (before Node::open), running fence, level emulation wiring | `-p ostro-cluster --test compat` | ✅ green |
| 13-05-01 | 05 | 2 | CLUS-05, CLUS-06, API-08 | Driver FatalApply + MetaSm fail-stop, TransferLeader, ReadIndex, RaftStatus progress/conf state, remedy text + membership tests | `-p ostro-meta --test newer_command`; `-p ostro-raft-driver` | ✅ green |
| 13-05-02 | 05 | 2 | CLUS-05, CLUS-06, API-08 | MetaHandle surface (read_index_barrier, transfer_leader, conf changes, conf_state, raft_status) | `-p ostro-meta --test membership_handle`; `-p ostro-meta` | ✅ green |
| 13-05-03 | 05 | 2 | CLUS-05, CLUS-06, API-08 | MetaPeer decode limit + snapshot failure reporting + lease anchor, large-snapshot learner test | `-p ostro-cluster --test meta_peer_large`; `-p ostro-cluster --test meta_peer` | ✅ green |
| 13-06-01 | 06 | 2 | CLUS-05 | Reconciler (RED → GREEN) with proptest-state-machine model | `-p ostro-cluster --test reconcile_model` | ✅ green |
| 13-06-02 | 06 | 2 | CLUS-05 | Placement planner, reassignment state machine, leader balancer (RED → GREEN) with models | `placement_model reassign_model` | ✅ green |
| 13-07-01 | 07 | 2 | ENG-10 | Fixture builders + LinkerSet for the 0.1 line, WIT_0_1_LATEST, JOURNAL_IFACE fix, select() with introduced_in | `-p ostro-engine --test linker_set`; `-p ostro-engine --test replay_corpus` | ✅ green |
| 13-07-02 | 07 | 2 | ENG-10 | Test-only 0.2 line (fixture package + bindgen module), side-by-side selection, cache key by line | `-p ostro-engine --test linker_set --features wit-next-fixture`; `-p ostro-engine` | ✅ green |
| 13-08-01 | 08 | 3 | CLUS-05, CLUS-06, CLUS-07, API-08 | P13Harness for InProcCluster and ProcCluster (single start, init, add_node via join, remove, drain, upgrade at an emulated level, status) | `-p ostro-repl-conformance --test p13_harness_smoke`; `-p ostro-repl-conformance --test harness_smoke` | ✅ green |
| 13-08-02 | 08 | 3 | CLUS-05, CLUS-06, CLUS-07, API-08 | Phase 13 oracles (RYW checker, membership, upgrade, GC) with damaged-input self-tests | `-p ostro-repl-conformance --test p13_oracle_selftest` | ✅ green |
| 13-09-01 | 09 | 4 | CLUS-05 | Dynamic address book + server-side join handshake (tokens, id allocation/resume, CSR signing, CA secret) + Member self-upsert RMW | `-p ostro-cluster --test address_book_dynamic`; `-p ostro-server --test p13_join` | ✅ green |
| 13-09-02 | 09 | 4 | CLUS-05 | Joiner (marker-first), boot_mode, both voter checks, five crash tests | `-p ostro-server --test p13_boot_modes --features failpoints` | ✅ green |
| 13-09-03 | 09 | 4 | CLUS-05 | Reconciler driver, removal/drain with tombstones, orphan-joiner GC, Membership.Removed, status, mutants + tier-B membership scenarios | `-p ostro-cluster --test membership_driver`; `-p ostro-cluster --test membership_mutants --features failpoints`; `-p ostro-repl-conformance --test membership` | ✅ green |
| 13-10-01 | 10 | 5 | CLUS-05, CLUS-06 | AlterReplicas apply in meta (validation tables, isr_version CAS, ext 100 verbatim) | `-p ostro-meta --test alter_replicas`; `-p ostro-meta` | ✅ green |
| 13-10-02 | 10 | 5 | CLUS-05, CLUS-06 | Placement controller (RF raise, Draining evacuation, explicit reassign/cancel, balancer) + mutant | `-p ostro-cluster --test placement_controller`; `-p ostro-cluster --test reassign_mutants --features failpoints` | ✅ green |
| 13-10-03 | 10 | 5 | CLUS-05, CLUS-06 | Tier-B reassignment scenarios under load | `-p ostro-repl-conformance --test reassign` | ✅ green |
| 13-11-01 | 11 | 4 | CLUS-06, CLUS-05 | Partition quiescing: admission off, in-flight wait, hot-wait eviction, abort | `-p ostro-partition --test handoff`; `-p ostro-partition --test dispatcher`; `-p ostro-server` | ✅ green |
| 13-11-02 | 11 | 4 | CLUS-06, CLUS-05 | LocalHandoff executor, NodeOps handoff client, controlled shutdown | `-p ostro-cluster --test handoff_executor` | ✅ green |
| 13-11-03 | 11 | 4 | CLUS-06, CLUS-05 | Tier-B handoff and controlled-shutdown scenarios | `-p ostro-repl-conformance --test handoff` | ✅ green |
| 13-12-01 | 12 | 4 | API-08 | Route table + RoutedControlPlane core (owner resolution, local vs forward, hop limit, NotLeader retry, unkeyed placement + pin) | `-p ostro-server --test p13_routing_unit` | ✅ green |
| 13-12-02 | 12 | 4 | API-08 | Fan-out, extension-trait wrapping, node-only ostro.api.v1 on the cluster listener, deadline/principal propagation | `-p ostro-server --test p13_routing_unit`; `-p ostro-server` | ✅ green |
| 13-12-03 | 12 | 4 | API-08 | Tier-B routing scenarios (parity through every node, NotLeader retry, offline 503, unkeyed placement, fan-out, hop limit, internal consumers) | `-p ostro-repl-conformance --test routing`; `-p ostro-repl-conformance --test routing --features failpoints` | ✅ green |
| 13-13-01 | 13 | 5 | API-08 | Lease check, meta barrier, consistency tokens, strict reads + mutant | `-p ostro-server --test p13_consistency_unit`; `-p ostro-server --test p13_consistency_unit --features failpoints`; `-p ostro-cluster --test controller_lease_wait` | ✅ green |
| 13-13-02 | 13 | 5 | API-08 | Per-term RunWatchers re-subscribe + LeaderChanged wake | `-p ostro-server --test p13_watch`; `-p ostro-server` | ✅ green |
| 13-13-03 | 13 | 5 | API-08 | Tier-B RYW scenarios with the session checker (moves, failover, zombie incl. asymmetric, tokens, await across handoff) | `-p ostro-repl-conformance --test ryw`; `-p ostro-repl-conformance --test ryw --features failpoints` | ✅ green |
| 13-14-01 | 14 | 4 | CLUS-06, ENG-10 | Worker version advertisement, admission policy, compatibility predicate in placement, GoAway on node drain | `-p ostro-server --test worker_compat`; `-p ostro-server --test worker_session` | ✅ green |
| 13-14-02 | 14 | 4 | CLUS-06, ENG-10 | Upload gate (WitRequirement ext 101, feature_not_enabled, mixed lines) + AOT pre-warm | `p13_upload_gate p13_prewarm` | ✅ green |
| 13-14-03 | 14 | 4 | CLUS-06, ENG-10 | Tier-B version-gate scenarios (auto-finalize, hold, manual finalize, gated upload, downgrade before/after finalize) | `-p ostro-repl-conformance --test version_gate` | ✅ green |
| 13-15-01 | 15 | 4 | CLUS-07 | Retire + refusal rules + ModuleRefsSource over led partitions | `-p ostro-server --test p13_module_retire` | ✅ green |
| 13-15-02 | 15 | 4 | CLUS-07 | GC service with grace + two-pass scan through every partition leader + mutant | `-p ostro-server --test p13_module_gc`; `-p ostro-server --test p13_module_gc --features failpoints` | ✅ green |
| 13-15-03 | 15 | 4 | CLUS-07 | Tier-B module GC scenarios | `-p ostro-repl-conformance --test module_gc` | ✅ green |
| 13-16-01 | 16 | 6 | CLUS-05, CLUS-06, CLUS-07 | Online promotion (`cluster init`) | `-p ostro-server --test p13_promote`; `-p ostro-server --test p13_promote --features failpoints` | ✅ green |
| 13-16-02 | 16 | 6 | CLUS-05, CLUS-06, CLUS-07 | ostro-client cluster module + CLI verbs (cluster, migrate-runs, modules rm/gc) | `-p ostro-client --test cluster_contract`; `-p ostro-cli --test p13_cli`; `-p ostro` | ✅ green |
| 13-16-03 | 16 | 6 | CLUS-05, CLUS-06, CLUS-07 | Tier-B promotion scenario under load | `-p ostro-repl-conformance --test promotion` | ✅ green |
| 13-17-01 | 17 | 5 | CLUS-07 | RepinRun apply (ModuleMismatch, SeqMismatch, RunBusy) + digest coverage | `-p ostro-partition --test apply_repin`; `-p ostro-partition --test rebuild_digest` | ✅ green |
| 13-17-02 | 17 | 5 | CLUS-07 | migrate_runs (per-leader dry-run replay, gated submission, report) + executor module_gone + mutant | `migrate_runs p13_module_gone`; `-p ostro-server --test migrate_runs --features failpoints` | ✅ green |
| 13-17-03 | 17 | 5 | CLUS-07 | Tier-B migrate-runs scenarios | `-p ostro-repl-conformance --test migrate`; `-p ostro-repl-conformance --test migrate --features failpoints` | ✅ green |
| 13-18-01 | 18 | 7 | CLUS-05, CLUS-06, CLUS-07, API-08 | Cross-feature integration scenarios + fixes | `-p ostro-repl-conformance --test integration_p13`; `-p ostro-repl-conformance --test integration_p13 --features failpoints`; `-p ostro-repl-conformance --test regressions_p13` | ✅ green |
| 13-18-02 | 18 | 7 | CLUS-05, CLUS-06, CLUS-07, API-08 | Full tier-B suite green ×3 (Phase 12 + Phase 13) | `-p ostro-repl-conformance` | ✅ green |
| 13-18-03 | 18 | 7 | CLUS-05, CLUS-06, CLUS-07, API-08 | Phase 13 mutant matrix (eight mutants, REQUIRE_ALL) | `-p ostro-repl-conformance --test mutants_p13 --features failpoints` | ✅ green |
| 13-19-01 | 19 | 8 | CLUS-05, CLUS-06, CLUS-07, API-08 | Tier-C promotion, rolling upgrade, RYW (scenario library + three tests) | `process_promotion process_ryw`; `-p ostro-repl-conformance --test process_rolling_upgrade --features failpoints` | ✅ green |
| 13-19-02 | 19 | 8 | CLUS-05, CLUS-06, CLUS-07, API-08 | Tier-C downgrade fences, controlled-shutdown timing, env-gated real N−1 test | `process_downgrade process_shutdown upgrade_from_prev` | ✅ green |
| 13-19-03 | 19 | 8 | CLUS-05, CLUS-06, CLUS-07, API-08 | Phase 13 cluster campaign smoke (membership + upgrade faults) | `-p ostro-repl-conformance --test cluster_campaign_p13 --features failpoints` | ✅ green |
| 13-20-01 | 20 | 9 | CLUS-05, CLUS-06, CLUS-07, ENG-10, API-08 | Hard CI gates (P13 mutant matrix, fixture suite, grep gates) + full gate run + final tier-C measurements | `--features failpoints`; `-p ostro-repl-conformance --test mutants_p13 --features failpoints` | ✅ green |
| 13-20-02 | 20 | 9 | CLUS-05, CLUS-06, CLUS-07, ENG-10, API-08 | Documentation | `grep / doc checks` | ✅ green |
| 13-20-03 | 20 | 9 | CLUS-05, CLUS-06, CLUS-07, ENG-10, API-08 | RESERVATIONS flip, REQUIREMENTS ticks, 13-VALIDATION.md, STATE and DECISIONS | `grep / doc checks` | ✅ green |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Success Criteria → Evidence

| SC | Statement | Evidence | Plans |
|----|-----------|----------|-------|
| SC1 / CLUS-05 | `ostro cluster init` promotes a running single node without export; `join/remove` change the 3/5-node meta Raft safely | `promote_running_single_node_to_three`, `process_promotion` (kill -9 node 1 after RF 3); `join_grows_to_five_voters`, `shrink_to_three`, `force_remove_dead_node`, `joiner_never_bootstraps`, `ids_never_reused`; driver `leave_joint_after_leader_kill`; reconciler model; mutants `skip_quorum_check`, `no_leave_joint` | 13-05, 13-06, 13-09, 13-10, 13-16, 13-18, 13-19 |
| SC2 / API-08 | Any node forwards to the owner; a client always reads its own prior writes | `parity_through_every_node` (REST + gRPC), `not_leader_retry_after_move`, `hop_limit_no_loop`; RYW checker scenarios (`ryw_across_nodes_*`, `zombie_leader_never_serves_stale`, `await_across_handoff`) + `process_ryw`; mutant `serve_without_lease` | 13-03, 13-12, 13-13, 13-18, 13-19 |
| SC3 / CLUS-06 + ENG-10 | Rolling upgrade across one minor with runs in flight, none drained; WIT 0.1.x and 0.2.x side by side; workers get compatible activations only | `process_rolling_upgrade` (OSTRO_UPGRADE_ITERS=3), `emulated_rolling_upgrade_under_load`, version-gate scenarios, fail-stop tests, mutants `emit_ungated`/`reject_and_continue`; `linker_set` incl. `select_0_2_fixture`, `mixed_lines_rejected`, `journal_overrides_cover_0_1_0_guest`; `worker_compat` (`incompatible_worker_never_receives`); real N−1 human_needed | 13-04, 13-07, 13-11, 13-14, 13-18, 13-19 |
| SC4 / CLUS-07 | Module GC never deletes a module referenced by a non-terminal run; migrate-runs re-pins only after a passing dry-run replay | `never_deletes_module_of_live_run`, `protects_retryable_terminal_runs`, `start_during_gc_race`; `diverging_runs_never_repinned`, `concurrent_growth_seq_mismatch`; mutants `single_scan`, `skip_seq_check`; GC oracle | 13-15, 13-17, 13-18 |
| QA-01 (phase gate) | Cluster crash campaign with membership/upgrade faults: 0 lost acked runs, 0 duplicate journal entries | `cluster_campaign_p13` report (≥ 10,000 faults or reached count + HUMAN_NEEDED) | 13-19, 13-20 |

---

## Wave 0 Requirements

- [x] `ostro_types::compat` (levels, Feature registry, MUTANTS), ext structs 96–102, RepinRun/AlterReplicas shapes — 13-01
- [x] `P13Harness` trait, stub scenario/oracle modules, `tests/p13_skeleton.rs` — 13-01
- [x] Harness implementations (tiers B/C) + Phase 13 oracles with self-tests — 13-08
- [x] `crates/ostro-cluster/tests/{reconcile_model,placement_model,reassign_model}.rs` — 13-06
- [x] `vendor/ostro-raft-driver/src/membership_tests.rs` — 13-05
- [x] `crates/ostro-engine/tests/linker_set.rs` + `wit-next-fixture` + fixture builders — 13-07
- [x] `crates/ostro-partition/tests/apply_newer_format.rs`, `crates/ostro-meta/tests/newer_command.rs` — 13-04
- [x] Tier-C scenarios `process_promotion`, `process_rolling_upgrade`, `process_ryw` — 13-19

---

## Manual-Only / Human-Needed

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Rolling upgrade from a real previous release binary | CLUS-06 | No release tag exists yet (D-P13-7) | `OSTRO_PREV_BIN=<path to previous ostro> cargo test --release -p ostro-repl-conformance --features failpoints --locked --test upgrade_from_prev -- --ignored --test-threads=1` |
| Native-Linux timings (handoff, controlled shutdown, promotion time-to-RF-3, first-activation p99, lease vs strict read latency) | CLUS-05, CLUS-06, API-08 | WSL2 numbers are advisory (D15) | Re-run 13-19 tests on native Linux NVMe, 8 vCPU |
| Cluster campaign to 10,000 faults if the 4 h box is exceeded | QA-01 | Time box on WSL2 | `OSTRO_CAMPAIGN_FAULTS=10000 … --test cluster_campaign_p13 -- --ignored` on the native machine |
| Remote CI legs (aarch64, macOS) | all | No git remote yet (FOUND-05) | Push and confirm the P13 mutant matrix and fixture steps are green |
| ~~User review of D-P13-4 and D-P13-15~~ | CLUS-06, API-08 | **Decided 2026-11-26: both confirmed** | - |

---

## Gate Evidence (13-20, 2026-11-26)

All runs on the user-approved microk8s `ostro-ci` namespace (`ostro-k8s-ci`, native Linux, Dell nodes dell-1/dell-2 shared with other executors; dell-3 never used). Logs under `target/k8s-ci-logs/`.

| Gate command | Commit | Result | Evidence |
|---|---|---|---|
| 1. full ci-local with musl (`ostro-k8s-ci --parallel`) | 9055589 | every step PASS except `cargo test` / `musl zigbuild test`: both red only on ostro-partition `trim_safety` (6) and `snapshot_tail` "node open timed out" under load; cargo fail-fast then skipped the rest of each shard. Re-run at 9055589 with `--no-fail-fast` (slot 1): both tests alone PASS, the whole test2 shard PASS, the whole musl2 shard PASS | `20261126-065142-par-9055589a-*.log`, `20261126-085958-s1-9055589a-cmd.log` |
| 1. full ci-local with musl, final code commit (after d33fce7) | 7942813 | every step PASS except `cargo test`: red only on ostro-partition `cron_driver::catch_up_skip_advances_the_watermark_without_runs` ("timed out after 10s", test2 pod at 27 GiB peak); re-run: PASS (cron_driver alone PASS; whole test2 shard with --no-fail-fast PASS, 373 test binaries ok; 20261126-104011-s0-7942813b-cmd.log) | `20261126-091656-par-7942813b-*.log` |
| 2. P13 mutant matrix (`OSTRO_P13_REQUIRE_ALL=1`) | 9055589, 7942813 | 8 of 8 bite, 0 SKIP (521 s / 929 s step) | lint logs above |
| 3. Phase 12 mutant matrix | 9055589, 7942813 | PASS (>= 11 bites) | fp2 logs |
| 4. engine `wit-next-fixture` test + clippy | 9055589, 7942813 | PASS | lint logs |
| P13 grep gates (`scripts/p13-grep-gates.sh`, negative self-test in 13-20-SUMMARY) + P12 gates | 9055589, 7942813 | PASS | lint logs |
| failpoint suites incl. `ostro-server:p13_boot_modes` (9), `p13_module_gc` (8), `p13_promote` (4) | 9055589, 7942813 | PASS | fp3 logs |
| musl binary size | 7942813 | 57.7 MB (< 60 MB) | musl1 log |
| 5./6. tier C (ci-local step, `OSTRO_UPGRADE_ITERS=3`, campaign 50) | 9055589 (child runs off), 7942813 (child runs **on**) | all seven PASS both times; 7942813: rolling upgrade x3 7907 runs 0 lost, promotion time-to-RF-3 3.0 s, RYW 0 violations, downgrade fences, shutdown, campaign 50 (1597 acks, membership + upgrade oracles pass), `upgrade_from_prev` SKIP | lint logs |
| 7. Phase 13 campaign, `OSTRO_CAMPAIGN_FAULTS=10000`, 4 h box | b47721e (child runs off) | **HUMAN_NEEDED: reached 1,110 faults** (all phase A, finalized 1) in 3 h 38 min with every batch disk check green; then `/dev/shm` in the pod filled up (fjall "No space left on device"), node 1 could not restart and the harness aborted before the final oracles. Not an oracle failure. 13-19's 500-fault run (478e54d) and the orchestrator's 500-fault run with child runs on (151,150 acks, after d33fce7) passed every oracle | `20261126-065118-s0-b47721e0-cmd.log` |
| 8. real N-1 (`OSTRO_PREV_BIN`) | - | human_needed (no release tag) | - |

### Measurements (7942813, tier C, 4-CPU lint pod on dell-1, under CI load)

| measurement | p50 | p99 | n |
|---|---|---|---|
| rolling upgrade: SIGTERM -> exit | 666 ms | 781 ms | 9 |
| rolling upgrade: restart -> ready | 768 ms | 851 ms | 9 |
| rolling upgrade: handoff unavailability per partition per restart | 276 ms | 2.00 s | 36 |
| first activation through a restarted node (AOT pre-warm) | 464 ms | 522 ms | 36 |
| baseline, same 20 ms link delay | 455 ms | 1.69 s | 36 |
| finalization after the last restart | 552 ms | 569 ms | 3 |
| shutdown: SIGTERM -> exit | 216 ms | 1.51 s | 10 |
| shutdown: unavailability, partitions led / followed | 650 / 642 ms | 1.37 / 1.09 s | 25 / 15 |
| promotion: time-to-RF-3 from the first join | 2.99 s | - | 1 |
| promotion: failover after kill -9 of node 1 | 1.31 s | - | 1 |
| RYW (9000 ops): read / write | 3.2 / 6.5 ms | 20.6 / 21.2 ms | - |

### human_needed

- Phase 13 campaign beyond 1,110 faults (needs more `/dev/shm` / a disk-backed proc root, or retention that trims under the campaign; logs did not trim, `floor=0`), ideally at a commit with d33fce7 and child runs on.
- Real previous-binary rolling upgrade (`OSTRO_PREV_BIN`) after the first release tag.
- Quiet-node re-measurement (all numbers above were taken on shared, loaded nodes).
- Remote GitHub CI legs (aarch64, macOS) for the new steps after a push.
- Deferred: controlled-shutdown follower-ISR stall (p13-bugs2 bug 4); joined-node chaos-proxy routing in tier C.

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers all MISSING references
- [x] No watch-mode flags
- [x] Feedback latency < 60 s at task level
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** 13-20 gate run 2026-11-26 (evidence below); phase verification by the orchestrator
