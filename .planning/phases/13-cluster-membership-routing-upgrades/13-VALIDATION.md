---
phase: 13
slug: cluster-membership-routing-upgrades
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-11-19
---

# Phase 13: Validation Strategy

> This is the per-phase validation contract for feedback sampling during execution.
> Source: 13-RESEARCH.md § Validation Architecture, mapped onto plans 13-01..13-19. Adopted decisions: 13-DECISIONS.md (D-P13-1..D-P13-28).

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Rust built-in test harness (`cargo test`, Rust 1.100), proptest 1.12 + proptest-state-machine 0.9 (tier A: `ostro-cluster` `reconcile_model`, `placement_model`, `reassign_model`), `fail` 0.5.1 failpoints (`member::`, `reassign::`, `route::`, `upgrade::`, `gc::`, `handoff::`, `migrate::` + the eight permanent `*::mutant::*`), the Phase 12 `ostro-repl-conformance` harness extended with `P13Harness`: tier B in-process clusters over the in-memory tonic transport + FaultLayer (per-node `level_max_override`); tier C spawned failpoint `ostro` processes on `127.r.r.n` behind the Rust chaos proxy with real `kill -9` and `OSTRO_TEST_LEVEL_MAX` emulation. Oracles: Phase 12 (disk, replica identity, standby, LA rule, ISR-only election) + Phase 13 (RYW session checker, membership, upgrade, GC). |
| **Config** | No new config file. `FAILPOINT_CRATES` unchanged (ostro-cluster and ostro-repl-conformance already in it since 12-02, the revised Phase 12 Cargo owner), kept equal to the ci.yml "Failpoint suites" step. 13-19 adds `check_p13_mutants` (`OSTRO_P13_REQUIRE_ALL=1`), the `wit-next-fixture` engine step and `check_p13_gates` to both ci-local and ci.yml. |
| **External services** | None. No Docker. **The microk8s cluster reachable via kubectl on this machine is never used.** Real previous-binary upgrade (`OSTRO_PREV_BIN`) is human_needed until a release tag exists. |
| **Quick run command** | `cargo test -p ostro-cluster --locked --test <plan test>` · `cargo test -p ostro-raft-driver --locked -- membership_tests` · `cargo test -p ostro-engine --locked --test linker_set` · `cargo test -p ostro-repl-conformance --locked --test <scenario group> -- --test-threads=1` |
| **Full suite command** | `bash scripts/ci-local.sh --no-musl` (includes the failpoint suites; from 13-19 also the P13 mutant matrix and the fixture suite) |
| **Phase gate commands** | 1. `bash scripts/ci-local.sh` (with musl).<br>2. `OSTRO_P13_REQUIRE_ALL=1 cargo test -p ostro-repl-conformance --features failpoints --locked --test mutants_p13 -- --test-threads=1 --nocapture` (≥ 8 `FAILED as expected`, no `SKIP`).<br>3. `OSTRO_REPL_REQUIRE_ALL=1 … --test mutants` (Phase 12 matrix still ≥ 11).<br>4. `cargo test -p ostro-engine --features wit-next-fixture --locked --test linker_set`.<br>5. `cargo build -p ostro --features failpoints --target-dir target/failpoints --locked`, then `OSTRO_UPGRADE_ITERS=3 cargo test --release -p ostro-repl-conformance --features failpoints --locked --test process_rolling_upgrade -- --ignored --test-threads=1`.<br>6. `cargo test --release -p ostro-repl-conformance --features failpoints --locked --test process_promotion --test process_ryw --test process_downgrade --test process_shutdown -- --ignored --test-threads=1`.<br>7. `OSTRO_CAMPAIGN_FAULTS=10000 cargo test --release -p ostro-repl-conformance --features failpoints --locked --test cluster_campaign_p13 -- --ignored --nocapture --test-threads=1` (4 h box → HUMAN_NEEDED marker).<br>8. `OSTRO_PREV_BIN=<prev> … --test upgrade_from_prev -- --ignored` (human_needed). |
| **Estimated runtime** | Quick: < 60 s per command. Tier-B scenario group: 20–120 s. Full ci-local: ~30–40 min debug. Rolling upgrade ×3 (tier C): 20–40 min. Campaign 10,000 faults: hours (time-boxed). |

---

## Sampling Rate

- **After every task commit:** the task's `<automated>` command, plus `cargo clippy -p <crate> --all-targets --locked -- -D warnings` for touched crates (with `--features failpoints` where failpoint tests exist).
  - Tasks touching `ostro-partition` also run `PROPTEST_CASES=128 cargo test -p ostro-partition --locked --test rebuild_digest` and `--test format_corpus`.
  - Tasks touching `ostro-meta` also run `cargo test -p ostro-meta --locked` (goldens append-only).
  - Tasks touching `ostro-engine` also run `cargo test -p ostro-engine --locked --test replay_corpus`.
- **After every plan / wave:** `bash scripts/ci-local.sh --no-musl` in a detached `/tmp` worktree at the plan's last commit (Worktree procedure); from wave 3 on also the plan's tier-B scenario group plus the Phase 12 tier-B suite.
- **Before `/gsd:verify-work`:** the 13-19 gate.
- **Max feedback latency:** 60 s for task-level unit checks; tier-B scenario groups ≤ 2 min; tier C and measurements are gate level.
- Piped verify commands use `set -euo pipefail`; cargo test filters go after `--`; no `|| true` masking; filtered runs must report ≥ 1 passed test (`test result: ok\. [1-9][0-9]* passed`).

---

## Wave Structure

| Wave | Plans | Exclusive shared files |
|------|-------|------------------------|
| 1 | 13-01 gate (Phase 12 + 9–11 complete) + contracts + seam + deps | **Cargo files**, RESERVATIONS, STATE, every `lib.rs`/`mod.rs` line, `ostro-types` compat/command, meta command/ext shapes, partition apply arms, **server.rs / cluster.rs / config.rs / main.rs / p13/mod.rs**, ostro-api phase13 + request_meta, CLI variants, harness trait |
| 2 | 13-02 API surface; 13-03 compat + fail-stop; 13-04 driver + MetaHandle; 13-05 pure algorithms; 13-06 LinkerSet; 13-07 harness + oracles | 13-02: **ostro-proto**, worker proto, ostro-api adapters/client/OpenAPI, `p13/node_ops.rs`. 13-03: ostro-cluster `compat/upgrade`, partition `apply/mod.rs`, meta `sm.rs`/`command.rs`. 13-04: **vendor driver + VENDORED.md**, meta `handle.rs`, ostro-cluster `meta_peer.rs`. 13-05: `membership/reconcile.rs`, `placement/{plan,reassign,leaders}.rs`. 13-06: **ostro-engine**. 13-07: conformance `harness/{inproc,process}`, `oracle/p13_*` |
| 3 | 13-08 membership; 13-09 reassignment; 13-10 handoff + shutdown; 13-11 routing; 13-12 RYW + watchers; 13-13 worker compat + upload gate + pre-warm; 13-14 module retire + GC | 13-08: `address_book`, `membership/{service,remove}`, `p13/{membership,join,joiner,status}`. 13-09: meta `state.rs`, `placement/controller.rs`. 13-10: partition `handoff.rs` + `leader/dispatcher.rs`, server `executor.rs`, cluster `handoff/shutdown`. 13-11: `route/{mod,peer,table,fanout}`, `control/runs.rs`. 13-12: `route/consistency.rs`, `control/watch.rs`. 13-13: `workers/*`, worker `session.rs`, `control/modules.rs`, `p13/prewarm.rs`. 13-14: `control/module_retire.rs`, `p13/{module_gc,module_refs}`. Each owns its scenario + test files. |
| 4 | 13-15 promotion + client + CLI; 13-16 RepinRun + migrate-runs + module_gone | 13-15: `p13/promote.rs`, ostro-client `cluster.rs`, CLI command files + snapshots. 13-16: partition `apply/repin.rs`, `control/migrate.rs`, server `executor.rs` |
| 5 | 13-17 tier-B suite green + P13 mutant matrix | sole plan: integration scenarios, `mutants_p13`, fixes across Phase 13 product files |
| 6 | 13-18 tier C (promotion, rolling upgrade, RYW, downgrade, shutdown, prev-binary hook, campaign) | conformance process test files, `scenarios/process_p13.rs` |
| 7 | 13-19 gate | ci files, docs, RESERVATIONS, REQUIREMENTS, STATE, VALIDATION, DECISIONS |

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirements | Task | Automated command(s) | Status |
|---------|------|------|--------------|------|----------------------|--------|
| 13-01-01 | 01 | 1 | all | Phase 12 (+9–11) gate, seams, route inventory, RESERVATIONS rows, STATE decisions | `grep / doc checks` (SUMMARYs + VERIFICATION status, ClusterServices, STATE/RESERVATIONS rows) | ⬜ pending |
| 13-01-02 | 01 | 1 | all | Contracts: compat registry, RepinRun/AlterReplicas shapes, ext 96–102, RejectReasons, HandoffControl, cluster contracts, LinkerSet stub, phase13 API traits | `cargo build --workspace --all-targets`; `cargo test -p ostro-types / -p ostro-meta / -p ostro-cluster --lib / -p ostro-api --lib`; goldens append-only; `rebuild_digest` | ⬜ pending |
| 13-01-03 | 01 | 1 | all | Server seam + call sites, config, CLI/client stubs, P13Harness, Cargo features, CI sync | `cargo deny check`; failpoints build; list diff; `-p ostro-server --test config`; `--test p13_skeleton`; `-p ostro-server`; `-p ostro-cli` | ⬜ pending |
| 13-02-01 | 02 | 2 | API-08, CLUS-05..07 | Protos + worker Hello feature_level | `cargo test -p ostro-proto --test p13_wire`; `-p ostro-worker-proto --test wire` | ⬜ pending |
| 13-02-02 | 02 | 2 | API-08, CLUS-05..07 | REST/gRPC adapters, RequestMeta, fakes, OpenAPI | `cargo test -p ostro-api --test phase13_routes`; `cargo test -p ostro-api` | ⬜ pending |
| 13-02-03 | 02 | 2 | API-08 | Production GrpcControlPlane + parity through it | `cargo test -p ostro-api --test grpc_client_parity` | ⬜ pending |
| 13-03-01 | 03 | 2 | CLUS-06 | Gate core, finalize decision, finalizer, hold/release + emit_ungated mutant | `cargo test -p ostro-cluster --test upgrade_finalize`; `--features failpoints --test compat_mutants` | ⬜ pending |
| 13-03-02 | 03 | 2 | CLUS-06 | Fail-stop partition + meta, re-encode identity, reject_and_continue mutant | `cargo test -p ostro-partition --test apply_newer_format`; `-p ostro-meta --test newer_command`; `format_corpus`; `rebuild_digest` | ⬜ pending |
| 13-03-03 | 03 | 2 | CLUS-06 | NodeVersion advertiser, boot fence (before Node::open), running fence | `cargo test -p ostro-cluster --test compat` | ⬜ pending |
| 13-04-01 | 04 | 2 | CLUS-05, API-08 | Driver TransferLeader/ReadIndex/progress + remedy text | `cargo test -p ostro-raft-driver -- membership_tests`; `check-vendored.sh` | ⬜ pending |
| 13-04-02 | 04 | 2 | CLUS-05, API-08 | MetaHandle surface | `cargo test -p ostro-meta --test membership_handle` | ⬜ pending |
| 13-04-03 | 04 | 2 | CLUS-05 | MetaPeer decode limit + large snapshot | `cargo test -p ostro-cluster --test meta_peer_large`; `--test meta_peer` | ⬜ pending |
| 13-05-01 | 05 | 2 | CLUS-05 | Reconciler + model (TDD) | `cargo test -p ostro-cluster --test reconcile_model`; `PROPTEST_CASES=1000 --release` | ⬜ pending |
| 13-05-02 | 05 | 2 | CLUS-05 | Placement, reassign, leaders + models (TDD) | `cargo test -p ostro-cluster --test placement_model --test reassign_model`; release ×1000 | ⬜ pending |
| 13-06-01 | 06 | 2 | ENG-10 | 0.1 LinkerSet, select(), introduced_in, JOURNAL_IFACE fix | `cargo test -p ostro-engine --test linker_set`; `--test replay_corpus` | ⬜ pending |
| 13-06-02 | 06 | 2 | ENG-10 | 0.2 fixture line, cache key by line | `cargo test -p ostro-engine --features wit-next-fixture --test linker_set`; `cargo test -p ostro-engine` | ⬜ pending |
| 13-07-01 | 07 | 2 | all | P13Harness tiers B and C | `cargo test -p ostro-repl-conformance --test p13_harness_smoke` (+ `--ignored` tier C); `--test harness_smoke` | ⬜ pending |
| 13-07-02 | 07 | 2 | all | Phase 13 oracles + self-tests | `cargo test -p ostro-repl-conformance --test p13_oracle_selftest` | ⬜ pending |
| 13-08-01 | 08 | 3 | CLUS-05 | Dynamic address book, join handshake, joiner boot | `cargo test -p ostro-cluster --test address_book_dynamic`; `-p ostro-server --test p13_join` | ⬜ pending |
| 13-08-02 | 08 | 3 | CLUS-05 | Reconciler driver, removal/tombstones, status, mutants | `cargo test -p ostro-cluster --test membership_driver`; `--features failpoints --test membership_mutants` | ⬜ pending |
| 13-08-03 | 08 | 3 | CLUS-05 | Tier-B membership scenarios | `cargo test -p ostro-repl-conformance --test membership` | ⬜ pending |
| 13-09-01 | 09 | 3 | CLUS-05 | AlterReplicas apply | `cargo test -p ostro-meta --test alter_replicas`; `cargo test -p ostro-meta` | ⬜ pending |
| 13-09-02 | 09 | 3 | CLUS-05, CLUS-06 | Placement controller + balancer + mutant | `cargo test -p ostro-cluster --test placement_controller`; `--features failpoints --test reassign_mutants` | ⬜ pending |
| 13-09-03 | 09 | 3 | CLUS-05, CLUS-06 | Tier-B reassignment scenarios | `cargo test -p ostro-repl-conformance --test reassign` | ⬜ pending |
| 13-10-01 | 10 | 3 | CLUS-06 | Partition quiescing | `cargo test -p ostro-partition --test handoff --test dispatcher`; `cargo test -p ostro-server` | ⬜ pending |
| 13-10-02 | 10 | 3 | CLUS-06 | Handoff executor, NodeOps client, controlled shutdown | `cargo test -p ostro-cluster --test handoff_executor` | ⬜ pending |
| 13-10-03 | 10 | 3 | CLUS-06 | Tier-B handoff + shutdown scenarios | `cargo test -p ostro-repl-conformance --test handoff` | ⬜ pending |
| 13-11-01 | 11 | 3 | API-08 | Route table + RoutedControlPlane core | `cargo test -p ostro-server --test p13_routing_unit` | ⬜ pending |
| 13-11-02 | 11 | 3 | API-08 | Fan-out, extension routing, node-only API | `cargo test -p ostro-server --test p13_routing_unit`; `cargo test -p ostro-server` | ⬜ pending |
| 13-11-03 | 11 | 3 | API-08 | Tier-B routing scenarios | `cargo test -p ostro-repl-conformance --test routing` (+ failpoints `hop_limit`) | ⬜ pending |
| 13-12-01 | 12 | 3 | API-08 | Lease, barrier, tokens, strict reads + mutant | `cargo test -p ostro-server --test p13_consistency_unit` (+ failpoints) | ⬜ pending |
| 13-12-02 | 12 | 3 | API-08 | Per-term RunWatchers | `cargo test -p ostro-server --test p13_watch`; `cargo test -p ostro-server` | ⬜ pending |
| 13-12-03 | 12 | 3 | API-08 | Tier-B RYW scenarios | `cargo test -p ostro-repl-conformance --test ryw` (+ failpoints `zombie`) | ⬜ pending |
| 13-13-01 | 13 | 3 | CLUS-06 | Worker version compatibility | `cargo test -p ostro-server --test worker_compat --test worker_session` | ⬜ pending |
| 13-13-02 | 13 | 3 | CLUS-06, ENG-10 | Upload gate + pre-warm | `cargo test -p ostro-server --test p13_upload_gate --test p13_prewarm` | ⬜ pending |
| 13-13-03 | 13 | 3 | CLUS-06, ENG-10 | Tier-B version-gate scenarios | `cargo test -p ostro-repl-conformance --test version_gate` | ⬜ pending |
| 13-14-01 | 14 | 3 | CLUS-07 | Retire + refusal + ModuleRefsSource | `cargo test -p ostro-server --test p13_module_retire` | ⬜ pending |
| 13-14-02 | 14 | 3 | CLUS-07 | GC service two-pass + mutant | `cargo test -p ostro-server --test p13_module_gc` (+ failpoints) | ⬜ pending |
| 13-14-03 | 14 | 3 | CLUS-07 | Tier-B module GC scenarios | `cargo test -p ostro-repl-conformance --test module_gc` | ⬜ pending |
| 13-15-01 | 15 | 4 | CLUS-05 | Online promotion | `cargo test -p ostro-server --test p13_promote` (+ failpoints) | ⬜ pending |
| 13-15-02 | 15 | 4 | CLUS-05..07 | ostro-client cluster module + CLI verbs | `cargo test -p ostro-client --test cluster_contract`; `-p ostro-cli --test p13_cli`; `ostro cluster --help` | ⬜ pending |
| 13-15-03 | 15 | 4 | CLUS-05 | Tier-B promotion scenarios | `cargo test -p ostro-repl-conformance --test promotion` | ⬜ pending |
| 13-16-01 | 16 | 4 | CLUS-07 | RepinRun apply | `cargo test -p ostro-partition --test apply_repin`; `rebuild_digest` | ⬜ pending |
| 13-16-02 | 16 | 4 | CLUS-07 | migrate_runs + module_gone + mutant | `cargo test -p ostro-server --test migrate_runs --test p13_module_gone` (+ failpoints) | ⬜ pending |
| 13-16-03 | 16 | 4 | CLUS-07 | Tier-B migrate scenarios | `cargo test -p ostro-repl-conformance --test migrate` (+ failpoints `concurrent_growth`) | ⬜ pending |
| 13-17-01 | 17 | 5 | all | Cross-feature integration scenarios + fixes | `cargo test -p ostro-repl-conformance --test integration_p13` (+ failpoints race); `--test regressions_p13` | ⬜ pending |
| 13-17-02 | 17 | 5 | all | Full tier-B suite ×3 | `cargo test -p ostro-repl-conformance -- --test-threads=1` ×3 | ⬜ pending |
| 13-17-03 | 17 | 5 | all | P13 mutant matrix | `OSTRO_P13_REQUIRE_ALL=1 … --features failpoints --test mutants_p13` (≥ 8 bites) | ⬜ pending |
| 13-18-01 | 18 | 6 | CLUS-05, CLUS-06, API-08 | Tier-C promotion, rolling upgrade, RYW | `--release --test process_promotion --test process_ryw -- --ignored`; `OSTRO_UPGRADE_ITERS=1 … --test process_rolling_upgrade -- --ignored` | ⬜ pending |
| 13-18-02 | 18 | 6 | CLUS-06 | Tier-C downgrade, shutdown timing, prev-binary hook | `--release --test process_downgrade --test process_shutdown --test upgrade_from_prev -- --ignored` | ⬜ pending |
| 13-18-03 | 18 | 6 | all | Phase 13 campaign smoke | `OSTRO_CAMPAIGN_FAULTS=50 … --test cluster_campaign_p13 -- --ignored` (task); ≥ 500 recorded | ⬜ pending |
| 13-19-01 | 19 | 7 | all | Hard CI gates + full gate + final measurements | REQUIRE_ALL P13 matrix; list diff; greps; reports present | ⬜ pending |
| 13-19-02 | 19 | 7 | all | Documentation | `grep / doc checks` | ⬜ pending |
| 13-19-03 | 19 | 7 | all | RESERVATIONS flip, REQUIREMENTS ticks, VALIDATION, STATE, DECISIONS | `grep / doc checks` | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Success Criteria → Evidence

| SC | Statement | Evidence | Plans |
|----|-----------|----------|-------|
| SC1 / CLUS-05 | `ostro cluster init` promotes a running single node without export; `join/remove` change the 3/5-node meta Raft safely | `promote_running_single_node_to_three`, `process_promotion` (kill -9 node 1 after RF 3); `join_grows_to_five_voters`, `shrink_to_three`, `force_remove_dead_node`, `joiner_never_bootstraps`, `ids_never_reused`; driver `leave_joint_after_leader_kill`; reconciler model; mutants `skip_quorum_check`, `no_leave_joint` | 13-04, 13-05, 13-08, 13-09, 13-15, 13-17, 13-18 |
| SC2 / API-08 | Any node forwards to the owner; a client always reads its own prior writes | `parity_through_every_node` (REST + gRPC), `not_leader_retry_after_move`, `hop_limit_no_loop`; RYW checker scenarios (`ryw_across_nodes_*`, `zombie_leader_never_serves_stale`, `await_across_handoff`) + `process_ryw`; mutant `serve_without_lease` | 13-02, 13-11, 13-12, 13-17, 13-18 |
| SC3 / CLUS-06 + ENG-10 | Rolling upgrade across one minor with runs in flight, none drained; WIT 0.1.x and 0.2.x side by side; workers get compatible activations only | `process_rolling_upgrade` (OSTRO_UPGRADE_ITERS=3), `emulated_rolling_upgrade_under_load`, version-gate scenarios, fail-stop tests, mutants `emit_ungated`/`reject_and_continue`; `linker_set` incl. `select_0_2_fixture`, `mixed_lines_rejected`, `journal_overrides_cover_0_1_0_guest`; `worker_compat` (`incompatible_worker_never_receives`); real N−1 human_needed | 13-03, 13-06, 13-10, 13-13, 13-17, 13-18 |
| SC4 / CLUS-07 | Module GC never deletes a module referenced by a non-terminal run; migrate-runs re-pins only after a passing dry-run replay | `never_deletes_module_of_live_run`, `protects_retryable_terminal_runs`, `start_during_gc_race`; `diverging_runs_never_repinned`, `concurrent_growth_seq_mismatch`; mutants `single_scan`, `skip_seq_check`; GC oracle | 13-14, 13-16, 13-17 |
| QA-01 (phase gate) | Cluster crash campaign with membership/upgrade faults: 0 lost acked runs, 0 duplicate journal entries | `cluster_campaign_p13` report (≥ 10,000 faults or reached count + HUMAN_NEEDED) | 13-18, 13-19 |

---

## Wave 0 Requirements

- [ ] `ostro_types::compat` (levels, Feature registry, MUTANTS), ext structs 96–102, RepinRun/AlterReplicas shapes — 13-01
- [ ] `P13Harness` trait, stub scenario/oracle modules, `tests/p13_skeleton.rs` — 13-01
- [ ] Harness implementations (tiers B/C) + Phase 13 oracles with self-tests — 13-07
- [ ] `crates/ostro-cluster/tests/{reconcile_model,placement_model,reassign_model}.rs` — 13-05
- [ ] `vendor/ostro-raft-driver/src/membership_tests.rs` — 13-04
- [ ] `crates/ostro-engine/tests/linker_set.rs` + `wit-next-fixture` + fixture builders — 13-06
- [ ] `crates/ostro-partition/tests/apply_newer_format.rs`, `crates/ostro-meta/tests/newer_command.rs` — 13-03
- [ ] Tier-C scenarios `process_promotion`, `process_rolling_upgrade`, `process_ryw` — 13-18

---

## Manual-Only / Human-Needed

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Rolling upgrade from a real previous release binary | CLUS-06 | No release tag exists yet (D-P13-7) | `OSTRO_PREV_BIN=<path to previous ostro> cargo test --release -p ostro-repl-conformance --features failpoints --locked --test upgrade_from_prev -- --ignored --test-threads=1` |
| Native-Linux timings (handoff, controlled shutdown, promotion time-to-RF-3, first-activation p99, lease vs strict read latency) | CLUS-05, CLUS-06, API-08 | WSL2 numbers are advisory (D15) | Re-run 13-18 tests on native Linux NVMe, 8 vCPU |
| Cluster campaign to 10,000 faults if the 4 h box is exceeded | QA-01 | Time box on WSL2 | `OSTRO_CAMPAIGN_FAULTS=10000 … --test cluster_campaign_p13 -- --ignored` on the native machine |
| Remote CI legs (aarch64, macOS) | all | No git remote yet (FOUND-05) | Push and confirm the P13 mutant matrix and fixture steps are green |
| User review of D-P13-4 (auto-finalize) and D-P13-15 (lease reads by default; strict_reads opt-in) | CLUS-06, API-08 | Adopted defaults, user may override | Read 13-DECISIONS.md; 13-12 records the strict_reads latency |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 60 s at task level
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
