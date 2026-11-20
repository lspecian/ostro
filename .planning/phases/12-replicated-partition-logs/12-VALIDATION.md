---
phase: 12
slug: replicated-partition-logs
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-11-19
---

# Phase 12: Validation Strategy

> This is the per-phase validation contract for feedback sampling during execution.
> Source: 12-RESEARCH.md § Validation Architecture, mapped onto plans 12-01..12-18 (revised after the plan check: 7 blockers, 15 warnings; renumbered 16 → 18). Adopted decisions: 12-DECISIONS.md (D-P12-1..D-P12-26).

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Rust built-in test harness (`cargo test`, Rust 1.100), proptest 1.12 + proptest-state-machine 0.9 (tier A: `ostro-log` `replica_model`), `fail` 0.5.1 failpoints (`repl::`, `fetch::`, `isr::`, `elect::`, `install::` + the ten permanent `repl::mutant::*`), the `ostro-repl-conformance` harness: tier B in-process cluster over an in-memory tonic transport with a seeded FaultLayer; tier C spawned failpoint `ostro server` processes on `127.r.r.{1..n}` behind a Rust TCP chaos proxy (RST passthrough) with real `kill -9`. One runner: hard per-scenario deadline, measured `cluster_formed` precondition, failures only as `ScenarioFailure{oracle}`. Oracles inspect bytes on disk, state digests, the full meta log (incl. rejected proposals) and the bucket. |
| **Config** | No new config file. `scripts/ci-local.sh` `FAILPOINT_CRATES` gains `ostro-cluster` and `ostro-repl-conformance` (12-01), mirrored in the ci.yml "Failpoint suites" step (strict-pattern sync check); 12-08 adds `check_one_log_path`; 12-18 adds `OSTRO_REPL_REQUIRE_ALL=1`, the "Replication mutant matrix" step and the clock/legacy/rf grep gates in both files. Tier-C tests skip unless the failpoint binary exists (`OSTRO_FAILPOINT_BIN` or `target/failpoints/debug/ostro`). |
| **External services** | None. Object-storage tests use a shared LocalFs bucket dir; SeaweedFS legs stay env-gated / human_needed (P8-D18). No Docker. **The microk8s cluster reachable via kubectl on this machine is never used.** |
| **Quick run command** | `cargo test -p ostro-log --locked --test replica_unit --test replica_model` · `cargo test -p ostro-cluster --locked --test <plan test>` · `cargo test -p ostro-repl-conformance --locked --test <scenario group> -- --test-threads=1` |
| **Full suite command** | `bash scripts/ci-local.sh --no-musl` (includes the failpoint suites, hence the mutant matrix, and the one-path grep gate) |
| **Phase gate commands** | 1. `bash scripts/ci-local.sh` (with musl).<br>2. `OSTRO_REPL_REQUIRE_ALL=1 cargo test -p ostro-repl-conformance --features failpoints --locked --test mutants -- --test-threads=1 --nocapture` (≥ 11 `FAILED as expected`, no `SKIP (pending)`).<br>3. `OSTRO_REPL_RED_RUN=1 cargo test -p ostro-repl-conformance --locked --test red_run -- --ignored --nocapture --test-threads=1` (all green, no HarnessError) and `cargo test -p ostro-repl-conformance --locked --test skeleton -- --ignored no_pending_left`.<br>4. `cargo build -p ostro --features failpoints --target-dir target/failpoints --locked`, then `cargo test --release -p ostro-repl-conformance --locked --test process_divergence --test process_power_loss --test process_mutants -- --ignored --test-threads=1`.<br>5. `OSTRO_FAILOVER_ITERS=200 OSTRO_FAILOVER_ASSERT=1 cargo test --release -p ostro-repl-conformance --locked --test failover_p99 -- --ignored --nocapture --test-threads=1` (≥ 1/3 controller faults, ≥ 1/3 no-RST blackholes, worst partition per iteration).<br>6. `OSTRO_CAMPAIGN_FAULTS=10000 cargo test --release -p ostro-repl-conformance --locked --test cluster_campaign -- --ignored --nocapture --test-threads=1` (4 h time box → HUMAN_NEEDED marker if exceeded).<br>7. `OSTRO_REPL_BENCH_SECS=60 cargo test --release -p ostro-repl-conformance --locked --test bench_rf3 -- --ignored --nocapture --test-threads=1` (reported, not gated). |
| **Estimated runtime** | Quick: < 60 s per command. Tier-B scenario group: 10–60 s. Full ci-local: ~20–30 min debug. Failover p99 (200 iterations): 30–60 min. Campaign 10,000 faults: hours (time-boxed). |

---

## Sampling Rate

- **After every task commit:** the task's `<automated>` command, plus `cargo clippy -p <crate> --all-targets --locked -- -D warnings` for touched crates (with `--features failpoints` where failpoint tests exist).
  - Tasks touching `ostro-log` also run `cargo test -p ostro-log --features failpoints --locked -- --test-threads=1`.
  - Tasks touching `ostro-partition` also run `PROPTEST_CASES=128 cargo test -p ostro-partition --locked --test rebuild_digest`.
- **After every plan / wave:** `bash scripts/ci-local.sh --no-musl` in a detached `/tmp` worktree at the plan's last commit (Worktree procedure); 12-06/12-07 take their red-run evidence there (uniform `cluster_formed` reason expected before integration; oracle-specific proof = mutant matrix); from wave 6 on also the tier-B suite via `red_run`.
- **Before `/gsd:verify-work`:** the 12-18 gate.
- **Max feedback latency:** 60 s for task-level checks; tier-C and measurements are gate level.
- Piped verify commands use `set -euo pipefail`; cargo test filters go after `--`; no `|| true` masking; filtered runs must report ≥ 1 passed test (`test result: ok\. [1-9][0-9]* passed`).

---

## Wave Structure

| Wave | Plans | Exclusive shared files |
|------|-------|------------------------|
| 1 | 12-01 gate (Phase 7 + 8 complete) + deps + CI sync | **Cargo files, ci files**, RESERVATIONS, STATE |
| 2 | 12-02 contracts | **proto**, meta command shapes, log trait + mutants + replica stubs, role contracts, every `lib.rs`/module stub, server config, conformance registry |
| 3 | 12-03 tier-B harness + oracles + runner; 12-04 pure algorithms; 12-05 meta commands + MetaPeer | 12-03: conformance `harness/{inproc,fault,certs}`, `oracle/*`, `scenarios/runner.rs`. 12-04: `ostro-log/src/replica/*`, **VENDORED.md**. 12-05: `ostro-meta/src/{command,state,handle}`, ostro-cluster `meta_peer/authz/address_book/transport/tcp` |
| 4 | 12-06 core scenarios; 12-07 remaining scenarios + mutant matrix; 12-08 one log impl; 12-09 tier-C harness; 12-10 controller | 12-06: core scenario modules, `pending/core.rs`. 12-07: remaining modules, `pending/rest.rs`, `mutants.rs`. 12-08: **ostro-log/src (all but replica/)**, rename ripple, ci-local grep gate. 12-09: conformance `harness/{process,chaos_proxy}`. 12-10: ostro-cluster `controller/liveness/bootstrap/members`, **vendor driver + VENDORED.md** |
| 5 | 12-11 fetch + fetcher + ISR publisher; 12-12 snapshot bootstrap; 12-13 role lifecycle | 12-11: ostro-cluster `fetch/fetcher/blob/isr_publisher`. 12-12: ostro-cluster `snapshot/install`. 12-13: **ostro-partition** node/roles/replica_services/leader/acks/router |
| 6 | 12-14 server wiring; 12-15 bottomless cluster mode | 12-14: **ostro-server** server/cluster/leadership/control + Phase 7 listener module, ostro-cluster `service/status/config/identity`, partition `node.rs` (legacy removal), `pending/{core,rest}.rs`. 12-15: **ostro-backup** cluster/uploader/fence/chunk/restore |
| 7 | 12-16 tier-B suite 100% green | sole plan: bug fixes across replication files + `pending/{core,rest}.rs` |
| 8 | 12-17 multi-process kill paths + p99 + tuning + benchmark + campaign | conformance process tests, `scenarios/process.rs`, `pending/process.rs`, server `config.rs` defaults |
| 9 | 12-18 gate | ci files, docs, RESERVATIONS, REQUIREMENTS, STATE, VALIDATION, DECISIONS |

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirements | Task | Automated command(s) | Status |
|---------|------|------|--------------|------|----------------------|--------|
| 12-01-01 | 01 | 1 | CLUS-01..04 | Phase 7 + Phase 8 completion gate, seam signatures, reservations check, RESERVATIONS rows, decisions and coordination in STATE.md | `grep / doc / build checks` | ⬜ pending |
| 12-01-02 | 01 | 1 | CLUS-01..04 | Every Phase 12 dependency, the two crates' manifests, failpoint feature chain, FAILPOINT_CRATES ↔ ci.yml sync | `cargo deny check`; tree bans; strict FAILPOINT list diff; `cargo build -p ostro --features failpoints` | ⬜ pending |
| 12-02-01 | 02 | 2 | CLUS-01..04 | Proto, meta command shapes + ext 80, ReplicatedLog additions, mutant names, ostro-partition role contracts | `cargo build --workspace --all-targets`; `cargo test -p ostro-proto`; `cargo test -p ostro-meta`; `cargo test -p ostro-log`; `cargo test -p ostro-partition` | ⬜ pending |
| 12-02-02 | 02 | 2 | CLUS-01..04 | ostro-cluster / backup / server / conformance skeletons, config sections with validation, transport hook, harness API, scenario registry | `cargo test -p ostro-repl-conformance --test skeleton`; `cargo test -p ostro-server --test config` | ⬜ pending |
| 12-03-01 | 03 | 3 | CLUS-01 | Test certificates, in-memory transport and FaultLayer | `cargo test -p ostro-repl-conformance --test fault_layer` | ⬜ pending |
| 12-03-02 | 03 | 3 | CLUS-01 | InProcCluster (tier B) implementing ClusterHarness + RF=1 smoke | `cargo test -p ostro-repl-conformance --test harness_smoke` | ⬜ pending |
| 12-03-03 | 03 | 3 | CLUS-01 | Measured oracles (incl. cluster_formed) with self-tests on damaged inputs + scenario runner | `cargo test -p ostro-repl-conformance --test oracle_selftest --test runner_selftest` | ⬜ pending |
| 12-04-01 | 04 | 3 | CLUS-02, CLUS-03 | Replica tracker, ISR, watermark, divergence and failover planner | `cargo test -p ostro-log --test replica_unit --test replica_model`; `cargo test -p ostro-log --features failpoints --test replica_mutants`; `PROPTEST_CASES=2000 cargo test -p ostro-log --test replica_model` | ⬜ pending |
| 12-05-01 | 05 | 3 | CLUS-03 | ElectLeader / AlterIsr / ResignLeader apply + command golden | `cargo test -p ostro-meta --test elect_leader --test meta_commands_golden`; `cargo test -p ostro-meta` | ⬜ pending |
| 12-05-02 | 05 | 3 | CLUS-03 | Multi-voter MetaHandle with live transport in every mode + Propose forwarding | `cargo test -p ostro-meta --test multi_voter --test single_voter` | ⬜ pending |
| 12-05-03 | 05 | 3 | CLUS-03 | MetaPeer service, per-peer outbox, production mTLS transport, node-identity authz | `cargo test -p ostro-cluster --test meta_peer` | ⬜ pending |
| 12-06-01 | 06 | 4 | CLUS-01 | Placement, ISR honesty, HW, stale-epoch scenarios | `cargo test -p ostro-repl-conformance --test placement --test isr_honesty --test hw --test stale_epoch` | ⬜ pending |
| 12-06-02 | 06 | 4 | CLUS-01 | Divergence, failover/offline, min_insync scenarios (red run in the worktree gate) | `cargo test -p ostro-repl-conformance --test divergence --test failover --test min_insync` | ⬜ pending |
| 12-07-01 | 07 | 4 | CLUS-01 | Standby, bootstrap, fetch path, blobs, one path, zombie, object storage scenarios | `cargo test -p ostro-repl-conformance --test standby --test bootstrap --test fetch_path --test blobs --test one_path --test zombie --test object_storage` | ⬜ pending |
| 12-07-02 | 07 | 4 | CLUS-01 | Mutant matrix (red run in the worktree gate) | `cargo test -p ostro-repl-conformance --lib -- mutants`; `cargo test -p ostro-repl-conformance --features failpoints --test mutants` | ⬜ pending |
| 12-08-01 | 08 | 4 | CLUS-02, CLUS-04 | Tracker-driven HW through the single publish_hw point (RF=1 included), F4 HW at open, min_insync gate, roles | `cargo test -p ostro-log`; `cargo test -p ostro-log --features failpoints`; `cargo test -p ostro-log --features failpoints --test partition_log_mutants` | ⬜ pending |
| 12-08-02 | 08 | 4 | CLUS-02, CLUS-04 | Follower and install primitives — read_replica, append_replica, set_follower_hw, reset_to, replica_status | `cargo test -p ostro-log --test partition_log_follower` | ⬜ pending |
| 12-08-03 | 08 | 4 | CLUS-02, CLUS-04 | Parsed-base torn-first-record rule, rename ripple across crates, Rf1Log grep gate | `cargo test -p ostro-log --test recovery_empty_anchor`; `cargo test -p ostro-partition`; `cargo test -p ostro-crashtest --features failpoints` | ⬜ pending |
| 12-09-01 | 09 | 4 | CLUS-04 | ChaosProxy with per-link faults | `cargo test -p ostro-repl-conformance --test chaos_proxy` | ⬜ pending |
| 12-09-02 | 09 | 4 | CLUS-04 | ProcCluster (tier C) implementing ClusterHarness + smoke | `cargo test -p ostro-repl-conformance --test process_smoke` | ⬜ pending |
| 12-10-01 | 10 | 4 | CLUS-03, CLUS-04 | sample_live over voters ∪ learners + Ostro wiped-voter remedy text + liveness detector with suspect confirmation and controller seeding | `cargo test -p ostro-raft-driver`; `cargo test -p ostro-cluster --test liveness` | ⬜ pending |
| 12-10-02 | 10 | 4 | CLUS-03, CLUS-04 | Controller loop — bootstrap placement (any voter count), fresh terms, planner → ElectLeader/AlterIsr, resign handling, Member self-registration | `grep / doc / build checks` | ⬜ pending |
| 12-10-03 | 10 | 4 | CLUS-03, CLUS-04 | Controller tests on a 3-voter meta group with scripted liveness | `cargo test -p ostro-cluster --test controller` | ⬜ pending |
| 12-11-01 | 11 | 5 | CLUS-02, CLUS-03 | Leader Fetch handler (validation order, two timeouts, durable-capped reads, size limits) | `cargo test -p ostro-cluster --test fetch_service` | ⬜ pending |
| 12-11-02 | 11 | 5 | CLUS-02, CLUS-03 | Follower fetch loop + FetchBlob/PushBlob (blobs before durability) | `cargo test -p ostro-cluster --test fetcher` | ⬜ pending |
| 12-11-03 | 11 | 5 | CLUS-02, CLUS-03 | ISR publisher (AlterIsr from measured progress) + mutants bite | `cargo test -p ostro-cluster --test isr_publisher`; `cargo test -p ostro-cluster --features failpoints --test fetch_mutants` | ⬜ pending |
| 12-12-01 | 12 | 5 | CLUS-04 | FetchSnapshot server — newest OSNP or on-demand export, frame streaming with resume | `cargo test -p ostro-cluster --test snapshot_stream` | ⬜ pending |
| 12-12-02 | 12 | 5 | CLUS-04 | install_from_leader with the fixed order + crash-at-each-step loop | `cargo test -p ostro-cluster --test install`; `cargo test -p ostro-cluster --features failpoints --test install_crash` | ⬜ pending |
| 12-13-01 | 13 | 5 | CLUS-03, CLUS-04 | Per-partition role state machine driven by assignments (become leader / follower / offline) | `cargo test -p ostro-partition --test role_lifecycle` | ⬜ pending |
| 12-13-02 | 13 | 5 | CLUS-03, CLUS-04 | Replica services in both roles; PartitionControl and PartitionRoles implementations | `cargo test -p ostro-partition --test replica_services` | ⬜ pending |
| 12-13-03 | 13 | 5 | CLUS-03, CLUS-04 | node.toml identity binding (node_id before meta, cluster_id before partitions) | `cargo test -p ostro-partition --test node_identity`; `cargo test -p ostro-partition`; `cargo test -p ostro-partition --features failpoints` | ⬜ pending |
| 12-14-01 | 14 | 6 | CLUS-02, CLUS-03, CLUS-04 | ClusterServices and the new boot order (single-node meta-first included); legacy leadership paths removed | `cargo test -p ostro-server --test cluster_boot`; `cargo test -p ostro-server`; `cargo test -p ostro` | ⬜ pending |
| 12-14-02 | 14 | 6 | CLUS-02, CLUS-03, CLUS-04 | Non-leader API behaviour, self-demotion, module blob replication, /v1/admin/replication | `cargo test -p ostro-server --test cluster_api`; `cargo test -p ostro-api` | ⬜ pending |
| 12-14-03 | 14 | 6 | CLUS-02, CLUS-03, CLUS-04 | First conformance contact — RF=1 and boot scenarios green, pending trimmed, red run re-recorded | `cargo test -p ostro-repl-conformance --test one_path`; `cargo test -p ostro-repl-conformance --features failpoints --test mutants` | ⬜ pending |
| 12-15-01 | 15 | 6 | CLUS-02 | Leader-only per-partition uploads + per-partition head with epoch fencing | `cargo test -p ostro-backup --test cluster_head`; `cargo test -p ostro-backup` | ⬜ pending |
| 12-15-02 | 15 | 6 | CLUS-02 | Takeover reconcile/repair of divergent remote tails + restore refusal for cluster buckets + mutant bites | `cargo test -p ostro-backup --test cluster_repair`; `cargo test -p ostro-backup --features failpoints --test cluster_mutants`; `cargo test -p ostro-backup --features failpoints` | ⬜ pending |
| 12-16-01 | 16 | 7 | CLUS-01..04 | Core scenarios green — placement, ISR honesty, HW, stale epoch, divergence, failover/offline, min_insync | `cargo test -p ostro-repl-conformance --test placement --test hw --test isr_honesty --test stale_epoch --test min_insync --test failover --test divergence` | ⬜ pending |
| 12-16-02 | 16 | 7 | CLUS-01..04 | Remaining scenarios green — standby, bootstrap, fetch path, blobs, zombie, object storage | `cargo test -p ostro-repl-conformance --test standby --test fetch_path --test blobs --test bootstrap --test zombie --test object_storage --test one_path --test regressions` | ⬜ pending |
| 12-16-03 | 16 | 7 | CLUS-01..04 | Mutant matrix complete for tier B (≥ 11 bites, REQUIRE_ALL, no SKIP) + red run shows zero pending | `OSTRO_REPL_REQUIRE_ALL=1 cargo test -p ostro-repl-conformance --features failpoints --test mutants`; `OSTRO_REPL_RED_RUN=1 cargo test -p ostro-repl-conformance --test red_run -- --ignored` | ⬜ pending |
| 12-17-01 | 17 | 8 | CLUS-02, CLUS-04 | SC2 kill path, power-loss-like follower crash, and the tier-C mutant | `OSTRO_PROC_ITERS=3 cargo test -p ostro-repl-conformance --test process_divergence --test process_power_loss --test process_mutants -- --ignored`; `cargo test -p ostro-repl-conformance --test skeleton -- --ignored` | ⬜ pending |
| 12-17-02 | 17 | 8 | CLUS-02, CLUS-04 | Failover p99 harness + meta election tick tuning sweep + tuned defaults | `OSTRO_FAILOVER_ITERS=20 OSTRO_FAILOVER_ASSERT=1 cargo test --release -p ostro-repl-conformance --test failover_p99 -- --ignored`; `cargo test -p ostro-server --test config` | ⬜ pending |
| 12-17-03 | 17 | 8 | CLUS-02, CLUS-04 | RF=3 bytes-on-disk benchmark + cluster crash-campaign smoke | `OSTRO_REPL_BENCH_SECS=15 cargo test --release -p ostro-repl-conformance --test bench_rf3 -- --ignored`; `OSTRO_CAMPAIGN_FAULTS=50 cargo test --release -p ostro-repl-conformance --test cluster_campaign -- --ignored` | ⬜ pending |
| 12-18-01 | 18 | 9 | CLUS-01..04 | Hard CI gates (mutant matrix REQUIRE_ALL, grep gates) + full gate run + final measurements | `cargo test -p ostro-repl-conformance --test skeleton -- --ignored`; `OSTRO_REPL_REQUIRE_ALL=1 cargo test -p ostro-repl-conformance --features failpoints --test mutants` | ⬜ pending |
| 12-18-02 | 18 | 9 | CLUS-01..04 | Documentation | `grep / doc / build checks` | ⬜ pending |
| 12-18-03 | 18 | 9 | CLUS-01..04 | RESERVATIONS flip, REQUIREMENTS ticks, 12-VALIDATION.md, STATE and DECISIONS | `grep / doc / build checks` | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Success Criteria → Evidence

| SC | Statement | Evidence | Plans |
|----|-----------|----------|-------|
| SC1 / CLUS-01 | Suite exists before integration, fails on a deliberately broken build; covers placement, ISR honesty, divergence/truncation, failover, min-insync | 12-06 + 12-07 red-run tables (RF>1 red with oracle reasons on the unintegrated build, RF=1 green); ten permanent `repl::mutant::*` each turning a scenario red with a ScenarioFailure (tier-C `count_nondurable_leo` via `process_mutants`); CI step with `OSTRO_REPL_REQUIRE_ALL=1` | 12-02, 12-03, 12-06, 12-07, 12-16, 12-17, 12-18 |
| SC2 / CLUS-02 | RF=3 HW = min(LEO over ISR); stale-epoch fetch rejected; divergent tail truncated on rejoin, incl. object storage; suite 100% green | `hw_is_min_over_isr`, `isr_slow_follower_shrinks` (held AlterIsr), `fetch_with_stale_epoch_fenced`, `leader_dies_with_unreplicated_writes`, `process_leader_dies_with_unreplicated_writes`, `remote_chain_equals_committed_prefix`, `remote_chain_never_above_hw`, takeover gap re-chain test; replica_model invariants; 12-16 green table | 12-04, 12-08, 12-11, 12-15, 12-16, 12-17 |
| SC3 / CLUS-03 | Elected only from the published ISR; writes stop below min_insync | `elect_leader.rs` apply tables (ISR ⊆ published, offline included); controller tests; `failover_elects_isr_member` (first live replica outside the ISR), `whole_isr_dead_goes_offline`, `writes_stop_below_min_insync`; `election::isr_only` audits every proposed ElectLeader and any extra `Put(Partition)` | 12-03, 12-04, 12-05, 12-10, 12-16 |
| SC4 / CLUS-04 | Hot standby, snapshot bootstrap, failover p99 < 10 s with runs resuming; RF=1 same code path | `followers_are_hot_standbys`, `empty_node_bootstraps_from_snapshot`, `install_crash` loop (derived keyspaces checked), `failover_p99` report, `one_path_rf1_hw_through_tracker` + `Rf1Log` grep gate + `rf1_bypass` mutant | 12-08, 12-12, 12-13, 12-14, 12-17, 12-18 |
| QA-01 (phase gate) | Cluster crash campaign: 0 lost acked runs, 0 duplicate journal entries | `cluster_campaign` report (≥ 10,000 faults or reached count + HUMAN_NEEDED) | 12-17, 12-18 |

---

## Wave 0 Requirements

- [ ] `crates/ostro-repl-conformance/` and `crates/ostro-cluster/` manifests, FAILPOINT_CRATES entries — 12-01
- [ ] Harness API, scenario registry, pending lists, `tests/{skeleton,red_run}.rs`, `ostro_log::mutants`, `ostro_log::replica` stubs, role contracts — 12-02
- [ ] Tier-B in-process harness, FaultLayer, test PKI, oracles (incl. `cluster_formed`), runner — 12-03
- [ ] `crates/ostro-log/tests/replica_model.rs` proptest-state-machine model — 12-04
- [ ] `crates/ostro-meta/tests/elect_leader.rs` — 12-05
- [ ] Scenarios (12-06, 12-07) and the mutant matrix (12-07)
- [ ] Tier-C process harness + chaos proxy — 12-09

---

## Manual-Only / Human-Needed

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Failover p99 and RF=3 benchmark on native Linux NVMe, 8 vCPU | CLUS-04 | WSL2 numbers are advisory (D15) | `OSTRO_FAILOVER_ITERS=200 OSTRO_FAILOVER_ASSERT=1 cargo test --release -p ostro-repl-conformance --locked --test failover_p99 -- --ignored --nocapture --test-threads=1`; `--test bench_rf3 -- --ignored` |
| Cluster campaign to 10,000 faults if the 4 h box is exceeded | QA-01 | Time box on WSL2 | `OSTRO_CAMPAIGN_FAULTS=10000 … --test cluster_campaign -- --ignored` on the native machine |
| SeaweedFS/S3 legs of the cluster bucket tests | CLUS-02 | No Docker locally (P8-D18) | Run the env-gated backup tests in CI service containers after the first push |
| Remote CI legs (aarch64, macOS) | all | No git remote yet (FOUND-05) | Push and confirm the new failpoint and mutant steps are green |
| User review of D-P12-2, D-P12-3, D-P12-4 | CLUS-03, CLUS-02, CLUS-04 | Adopted defaults, user may override | Read 12-DECISIONS.md |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 60 s at task level
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
