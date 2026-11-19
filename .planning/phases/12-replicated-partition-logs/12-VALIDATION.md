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
> Source: 12-RESEARCH.md § Validation Architecture, mapped onto plans 12-01..12-16. Adopted decisions: 12-DECISIONS.md (D-P12-1..D-P12-26).

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Rust built-in test harness (`cargo test`, Rust 1.100), proptest 1.12 + proptest-state-machine 0.9 (tier A: `ostro-log` `replica_model`), `fail` 0.5.1 failpoints (`repl::`, `fetch::`, `isr::`, `elect::`, `install::` + the ten permanent `repl::mutant::*`), the `ostro-repl-conformance` harness: tier B in-process cluster over an in-memory tonic transport with a seeded FaultLayer; tier C spawned failpoint `ostro server` processes on `127.r.r.{1..n}` behind a Rust TCP chaos proxy with real `kill -9`. Oracles inspect bytes on disk, state digests, the meta log and the bucket. |
| **Config** | No new config file. `scripts/ci-local.sh` `FAILPOINT_CRATES` gains `ostro-cluster` and `ostro-repl-conformance` (12-01), mirrored in the ci.yml "Failpoint suites" step; 12-06 adds `check_one_log_path`; 12-16 adds `OSTRO_REPL_REQUIRE_ALL=1`, the "Replication mutant matrix" step and the clock/legacy grep gates in both files. |
| **External services** | None. Object-storage tests use a shared LocalFs bucket dir; MinIO legs stay env-gated / human_needed (P8-D18). No Docker. **The microk8s cluster reachable via kubectl on this machine is never used.** |
| **Quick run command** | `cargo test -p ostro-log --locked --test replica_unit --test replica_model` · `cargo test -p ostro-cluster --locked --test <plan test>` · `cargo test -p ostro-repl-conformance --locked --test <scenario group> -- --test-threads=1` |
| **Full suite command** | `bash scripts/ci-local.sh --no-musl` (includes the failpoint suites, hence the mutant matrix, and the one-path grep gate) |
| **Phase gate commands** | 1. `bash scripts/ci-local.sh` (with musl).<br>2. `OSTRO_REPL_REQUIRE_ALL=1 cargo test -p ostro-repl-conformance --features failpoints --locked --test mutants -- --test-threads=1 --nocapture` (≥ 11 `FAILED as expected`, no `SKIP (pending)`).<br>3. `OSTRO_REPL_RED_RUN=1 cargo test -p ostro-repl-conformance --locked --test red_run -- --ignored --nocapture --test-threads=1` (zero pending, all green).<br>4. `cargo build -p ostro --features failpoints --target-dir target/failpoints --locked`, then `cargo test --release -p ostro-repl-conformance --locked --test process_divergence --test process_power_loss --test process_mutants -- --ignored --test-threads=1`.<br>5. `OSTRO_FAILOVER_ITERS=200 OSTRO_FAILOVER_ASSERT=1 cargo test --release -p ostro-repl-conformance --locked --test failover_p99 -- --ignored --nocapture --test-threads=1`.<br>6. `OSTRO_CAMPAIGN_FAULTS=10000 cargo test --release -p ostro-repl-conformance --locked --test cluster_campaign -- --ignored --nocapture --test-threads=1` (4 h time box → HUMAN_NEEDED marker if exceeded).<br>7. `OSTRO_REPL_BENCH_SECS=60 cargo test --release -p ostro-repl-conformance --locked --test bench_rf3 -- --ignored --nocapture --test-threads=1` (reported, not gated). |
| **Estimated runtime** | Quick: < 60 s per command. Tier-B scenario group: 10–60 s. Full ci-local: ~20–30 min debug. Failover p99 (200 iterations): 30–60 min. Campaign 10,000 faults: hours (time-boxed). |

---

## Sampling Rate

- **After every task commit:** the task's `<automated>` command, plus `cargo clippy -p <crate> --all-targets --locked -- -D warnings` for touched crates (with `--features failpoints` where failpoint tests exist).
  - Tasks touching `ostro-log` also run `cargo test -p ostro-log --features failpoints --locked -- --test-threads=1`.
  - Tasks touching `ostro-partition` also run `PROPTEST_CASES=128 cargo test -p ostro-partition --locked --test rebuild_digest`.
- **After every plan / wave:** `bash scripts/ci-local.sh --no-musl` in a detached `/tmp` worktree at the plan's last commit (Worktree procedure); from wave 5 on also the tier-B suite via `red_run`.
- **Before `/gsd:verify-work`:** the 12-16 gate.
- **Max feedback latency:** 60 s for task-level checks; tier-C and measurements are gate level.
- Piped verify commands use `set -euo pipefail`; cargo test filters go after `--`; no `|| true` masking; filtered runs must report ≥ 1 passed test (`test result: ok\. [1-9][0-9]* passed`).

---

## Wave Structure

| Wave | Plans | Exclusive shared files |
|------|-------|------------------------|
| 1 | 12-01 gate (Phase 7 + 8 complete) + contracts + deps | **Cargo files, proto, ci files**, RESERVATIONS, STATE, every `lib.rs`/module stub, meta command shapes, trait signatures |
| 2 | 12-02 tier-B harness + oracles; 12-03 pure algorithms; 12-04 meta commands + MetaPeer transport | 12-02: conformance `harness/{inproc,fault,certs}`, `oracle/*`. 12-03: `ostro-log/src/replica/*`, **VENDORED.md**. 12-04: `ostro-meta/src/{command,state,handle}`, ostro-cluster `meta_peer/authz/address_book/transport/tcp` |
| 3 | 12-05 scenarios + mutants + red run; 12-06 one log impl; 12-07 tier-C harness; 12-08 controller | 12-05: conformance `scenarios/*`, `pending.rs`, `mutants.rs`, scenario tests. 12-06: **ostro-log/src (all but replica/)**, rename ripple, ci-local grep gate. 12-07: conformance `harness/{process,chaos_proxy}`. 12-08: ostro-cluster `controller/liveness/bootstrap/members`, **vendor driver + VENDORED.md** |
| 4 | 12-09 fetch + fetcher + ISR publisher; 12-10 snapshot bootstrap; 12-11 role lifecycle | 12-09: ostro-cluster `fetch/fetcher/blob/isr_publisher`. 12-10: ostro-cluster `snapshot/install`. 12-11: **ostro-partition** node/roles/replica_services/leader/acks/router |
| 5 | 12-12 server wiring; 12-13 bottomless cluster mode | 12-12: **ostro-server** server/cluster/leadership/control, ostro-cluster `service/status/config`, partition `node.rs` (legacy removal), `pending.rs`. 12-13: **ostro-backup** cluster/uploader/fence/chunk/restore |
| 6 | 12-14 tier-B suite 100% green | sole plan: bug fixes across replication files + `pending.rs` |
| 7 | 12-15 multi-process kill paths + p99 + tuning + benchmark + campaign | conformance process tests, `scenarios/process.rs`, server `config.rs` defaults |
| 8 | 12-16 gate | ci files, docs, RESERVATIONS, REQUIREMENTS, STATE, VALIDATION, DECISIONS |

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirements | Task | Automated command(s) | Status |
|---------|------|------|--------------|------|----------------------|--------|
| 12-01-01 | 01 | 1 | CLUS-01..04 | Phase 7/8 gate, seam signatures, reservations, RESERVATIONS rows, STATE decisions | `grep / doc checks` (08/07 SUMMARYs + VERIFICATION status, F7 fields, seed_epochs, STATE/RESERVATIONS rows) | ⬜ pending |
| 12-01-02 | 01 | 1 | CLUS-01..04 | Contracts: proto, MetaCommand 4–6 shapes, ext 80, trait additions, mutant names, role contracts, stubs, config | `cargo build --workspace --all-targets`; `cargo test -p ostro-proto`; `-p ostro-meta`; `-p ostro-log`; `-p ostro-repl-conformance --test skeleton`; `-p ostro-server --test config` | ⬜ pending |
| 12-01-03 | 01 | 1 | CLUS-01..04 | Dependencies, crates, failpoint chain, FAILPOINT_CRATES ↔ ci.yml | `cargo deny check`; tree bans; list diff; `cargo build -p ostro --features failpoints` | ⬜ pending |
| 12-02-01 | 02 | 2 | CLUS-01 | Test PKI, MemTransport, FaultLayer | `cargo test -p ostro-repl-conformance --test fault_layer` | ⬜ pending |
| 12-02-02 | 02 | 2 | CLUS-01 | InProcCluster + RF=1 smoke | `cargo test -p ostro-repl-conformance --test harness_smoke` | ⬜ pending |
| 12-02-03 | 02 | 2 | CLUS-01 | Measured oracles + damaged-input self-tests | `cargo test -p ostro-repl-conformance --test oracle_selftest` | ⬜ pending |
| 12-03-01 | 03 | 2 | CLUS-02, CLUS-03 | Tracker, ISR, watermark, divergence, failover planner (TDD) + model + mutants | `cargo test -p ostro-log --test replica_unit --test replica_model`; `--features failpoints --test replica_mutants`; `PROPTEST_CASES=2000 … --release --test replica_model` | ⬜ pending |
| 12-04-01 | 04 | 2 | CLUS-03 | ElectLeader/AlterIsr/ResignLeader apply + command golden | `cargo test -p ostro-meta --test elect_leader --test meta_commands_golden`; `cargo test -p ostro-meta` | ⬜ pending |
| 12-04-02 | 04 | 2 | CLUS-03 | Multi-voter MetaHandle + forwarding | `cargo test -p ostro-meta --test multi_voter --test single_voter` | ⬜ pending |
| 12-04-03 | 04 | 2 | CLUS-03 | MetaPeer service, PeerOutbox, TcpTransport, node authz | `cargo test -p ostro-cluster --test meta_peer` | ⬜ pending |
| 12-05-01 | 05 | 3 | CLUS-01 | Registry, pending, core scenarios | `cargo test -p ostro-repl-conformance --test placement --test isr_honesty --test hw --test stale_epoch --test divergence --test failover --test min_insync` | ⬜ pending |
| 12-05-02 | 05 | 3 | CLUS-01 | Standby, bootstrap, fetch path, blobs, one path, zombie, object storage scenarios | `cargo test -p ostro-repl-conformance --test standby --test bootstrap --test fetch_path --test blobs --test one_path --test zombie --test object_storage` | ⬜ pending |
| 12-05-03 | 05 | 3 | CLUS-01 | Mutant matrix + red run evidence | `OSTRO_REPL_RED_RUN=1 … --test red_run -- --ignored`; `--features failpoints --test mutants` | ⬜ pending |
| 12-06-01 | 06 | 3 | CLUS-02, CLUS-04 | PartitionLog, tracker-driven HW via publish_hw, F4, min_insync gate | `cargo test -p ostro-log`; `--features failpoints`; `--test partition_log_mutants` | ⬜ pending |
| 12-06-02 | 06 | 3 | CLUS-02, CLUS-04 | read_replica, append_replica, set_follower_hw, reset_to | `cargo test -p ostro-log --test partition_log_follower` | ⬜ pending |
| 12-06-03 | 06 | 3 | CLUS-02, CLUS-04 | Parsed-base torn rule, rename ripple, grep gate | `cargo test -p ostro-log --test recovery_empty_anchor`; `! grep -rn Rf1Log crates/`; `cargo test -p ostro-partition`; `cargo test -p ostro-crashtest --features failpoints` | ⬜ pending |
| 12-07-01 | 07 | 3 | CLUS-04 | ChaosProxy | `cargo test -p ostro-repl-conformance --test chaos_proxy` | ⬜ pending |
| 12-07-02 | 07 | 3 | CLUS-04 | ProcCluster + smoke | `cargo test -p ostro-repl-conformance --test process_smoke` | ⬜ pending |
| 12-08-01 | 08 | 3 | CLUS-03, CLUS-04 | sample_live over learners + liveness detector | `cargo test -p ostro-raft-driver -- sample_live`; `cargo test -p ostro-cluster --test liveness`; `check-vendored.sh` | ⬜ pending |
| 12-08-02 | 08 | 3 | CLUS-03, CLUS-04 | Controller loop, bootstrap, fresh term, resign, Member upsert | `cargo build -p ostro-cluster --all-targets`; clippy; grep (no SetPartitionLeader) | ⬜ pending |
| 12-08-03 | 08 | 3 | CLUS-03, CLUS-04 | Controller tests on a 3-voter meta group | `cargo test -p ostro-cluster --test controller` | ⬜ pending |
| 12-09-01 | 09 | 4 | CLUS-02, CLUS-03 | Leader Fetch handler | `cargo test -p ostro-cluster --test fetch_service` | ⬜ pending |
| 12-09-02 | 09 | 4 | CLUS-02, CLUS-03 | Follower fetcher + blobs | `cargo test -p ostro-cluster --test fetcher` | ⬜ pending |
| 12-09-03 | 09 | 4 | CLUS-02, CLUS-03 | ISR publisher + mutants | `cargo test -p ostro-cluster --test isr_publisher`; `--features failpoints --test fetch_mutants` | ⬜ pending |
| 12-10-01 | 10 | 4 | CLUS-04 | FetchSnapshot streaming | `cargo test -p ostro-cluster --test snapshot_stream` | ⬜ pending |
| 12-10-02 | 10 | 4 | CLUS-04 | install_from_leader + crash loop | `cargo test -p ostro-cluster --test install`; `--features failpoints --test install_crash` | ⬜ pending |
| 12-11-01 | 11 | 4 | CLUS-03, CLUS-04 | Role state machine | `cargo test -p ostro-partition --test role_lifecycle` | ⬜ pending |
| 12-11-02 | 11 | 4 | CLUS-03, CLUS-04 | Replica services both roles, PartitionControl, PartitionRoles | `cargo test -p ostro-partition --test replica_services` | ⬜ pending |
| 12-11-03 | 11 | 4 | CLUS-03, CLUS-04 | node.toml identity binding | `cargo test -p ostro-partition --test node_identity`; `cargo test -p ostro-partition`; `--features failpoints` | ⬜ pending |
| 12-12-01 | 12 | 5 | CLUS-02..04 | ClusterServices + boot order, legacy paths removed | `cargo test -p ostro-server --test cluster_boot`; greps; `cargo test -p ostro-server`; `cargo test -p ostro` | ⬜ pending |
| 12-12-02 | 12 | 5 | CLUS-02..04 | Non-leader API, self-demotion, module blob replication, status | `cargo test -p ostro-server --test cluster_api`; `cargo test -p ostro-api` | ⬜ pending |
| 12-12-03 | 12 | 5 | CLUS-02..04 | First conformance run against the integrated server | `cargo test -p ostro-repl-conformance --test one_path`; `--features failpoints --test mutants` (rf1_bypass bites) | ⬜ pending |
| 12-13-01 | 13 | 5 | CLUS-02 | Leader-only uploads + epoch-fenced heads | `cargo test -p ostro-backup --test cluster_head`; `cargo test -p ostro-backup` | ⬜ pending |
| 12-13-02 | 13 | 5 | CLUS-02 | Takeover repair, restore refusal, mutant | `cargo test -p ostro-backup --test cluster_repair`; `--features failpoints --test cluster_mutants` | ⬜ pending |
| 12-14-01 | 14 | 6 | CLUS-01..04 | Core scenarios green ×3 | tier-B core groups ×3, no PENDING | ⬜ pending |
| 12-14-02 | 14 | 6 | CLUS-01..04 | Remaining scenarios green ×3 | tier-B remaining groups + regressions ×3 | ⬜ pending |
| 12-14-03 | 14 | 6 | CLUS-01..04 | Tier-B mutant matrix complete + red run zero pending | `--features failpoints --test mutants` (≥ 10 bites); `red_run` | ⬜ pending |
| 12-15-01 | 15 | 7 | CLUS-02, CLUS-04 | SC2 kill path, follower power loss, tier-C mutant | `--test process_divergence --test process_power_loss --test process_mutants -- --ignored` | ⬜ pending |
| 12-15-02 | 15 | 7 | CLUS-04 | Failover p99 + tick tuning sweep | `OSTRO_FAILOVER_ITERS=20 OSTRO_FAILOVER_ASSERT=1 … --test failover_p99 -- --ignored` (task); 200 iterations recorded | ⬜ pending |
| 12-15-03 | 15 | 7 | CLUS-02, CLUS-04 | RF=3 bytes-on-disk benchmark + campaign smoke | `--test bench_rf3 -- --ignored`; `OSTRO_CAMPAIGN_FAULTS=50 … --test cluster_campaign -- --ignored` (task); 500 recorded | ⬜ pending |
| 12-16-01 | 16 | 8 | CLUS-01..04 | Hard CI gates + full gate + final measurements | REQUIRE_ALL mutant run; list diff; greps; reports present | ⬜ pending |
| 12-16-02 | 16 | 8 | CLUS-01..04 | Documentation | `grep / doc checks` | ⬜ pending |
| 12-16-03 | 16 | 8 | CLUS-01..04 | RESERVATIONS flip, REQUIREMENTS ticks, VALIDATION, STATE, DECISIONS | `grep / doc checks` | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Success Criteria → Evidence

| SC | Statement | Evidence | Plans |
|----|-----------|----------|-------|
| SC1 / CLUS-01 | Suite exists before integration, fails on a deliberately broken build; covers placement, ISR honesty, divergence/truncation, failover, min-insync | 12-05 red-run table (RF>1 red on the unintegrated build, RF=1 green); ten permanent `repl::mutant::*` each turning their scenario red; CI step with `OSTRO_REPL_REQUIRE_ALL=1` | 12-01, 12-05, 12-14, 12-15, 12-16 |
| SC2 / CLUS-02 | RF=3 HW = min(LEO over ISR); stale-epoch fetch rejected; divergent tail truncated on rejoin, incl. object storage; suite 100% green | `hw_is_min_over_isr`, `fetch_with_stale_epoch_fenced`, `leader_dies_with_unreplicated_writes`, `process_leader_dies_with_unreplicated_writes`, `remote_chain_equals_committed_prefix`; replica_model invariants; 12-14 green table | 12-03, 12-06, 12-09, 12-13, 12-14, 12-15 |
| SC3 / CLUS-03 | Elected only from the published ISR; writes stop below min_insync | `elect_leader.rs` apply tables; controller tests; `failover_elects_isr_member`, `whole_isr_dead_goes_offline`, `writes_stop_below_min_insync`; `election::isr_only` oracle on every scenario | 12-03, 12-04, 12-08, 12-14 |
| SC4 / CLUS-04 | Hot standby, snapshot bootstrap, failover p99 < 10 s with runs resuming; RF=1 same code path | `followers_are_hot_standbys`, `empty_node_bootstraps_from_snapshot`, `install_crash` loop, `failover_p99` report (≥ 200 iterations incl. controller kills), `one_path_rf1_hw_through_tracker` + `Rf1Log` grep gate + `rf1_bypass` mutant | 12-06, 12-10, 12-11, 12-12, 12-15, 12-16 |
| QA-01 (phase gate) | Cluster crash campaign: 0 lost acked runs, 0 duplicate journal entries | `cluster_campaign` report (≥ 10,000 faults or reached count + HUMAN_NEEDED) | 12-15, 12-16 |

---

## Wave 0 Requirements

- [ ] `crates/ostro-repl-conformance/` crate, harness API, stub modules, FAILPOINT_CRATES entry — 12-01
- [ ] `crates/ostro-cluster/` crate skeleton with contracts — 12-01
- [ ] `ostro_log::mutants` names and `ostro_log::replica` stubs — 12-01
- [ ] Tier-B in-process harness, FaultLayer, test PKI, oracles — 12-02
- [ ] `crates/ostro-log/tests/replica_model.rs` proptest-state-machine model — 12-03
- [ ] `crates/ostro-meta/tests/elect_leader.rs` — 12-04
- [ ] Scenario registry, pending list, mutant map, red runner — 12-05
- [ ] Tier-C process harness + chaos proxy — 12-07

---

## Manual-Only / Human-Needed

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Failover p99 and RF=3 benchmark on native Linux NVMe, 8 vCPU | CLUS-04 | WSL2 numbers are advisory (D15) | `OSTRO_FAILOVER_ITERS=200 OSTRO_FAILOVER_ASSERT=1 cargo test --release -p ostro-repl-conformance --locked --test failover_p99 -- --ignored --nocapture --test-threads=1`; `--test bench_rf3 -- --ignored` |
| Cluster campaign to 10,000 faults if the 4 h box is exceeded | QA-01 | Time box on WSL2 | `OSTRO_CAMPAIGN_FAULTS=10000 … --test cluster_campaign -- --ignored` on the native machine |
| MinIO/S3 legs of the cluster bucket tests | CLUS-02 | No Docker locally (P8-D18) | Run the env-gated backup tests in CI service containers after the first push |
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
