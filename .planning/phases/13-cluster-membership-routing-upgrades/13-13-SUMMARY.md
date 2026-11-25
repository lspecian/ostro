---
phase: 13-cluster-membership-routing-upgrades
plan: 13
subsystem: read-your-writes (ostro-server route/consistency.rs + control/watch.rs, ostro-cluster controller)
tags: [ryw, api-08, lease, read-index, consistency-token, strict-reads, run-watchers, zombie, tier-b]
requires:
  - phase: 13-cluster-membership-routing-upgrades
    provides: "13-02 hook signatures; 13-03 RequestMeta.read_after / set_consistency; 13-05 lease anchor + read_index_barrier; 13-08 RywChecker; 13-11 in_handoff; 13-12 RoutedControlPlane local-branch hook calls + one re-route"
provides:
  - "route::consistency: check_partition_read / check_meta_read / partition_token / meta_token over a ReadEnv (NodeReadEnv over P13Ctx); parse_token; lease_limit / lease_holds; stats(); MUTANT_SERVE_WITHOUT_LEASE"
  - "metrics ostro_route_lease_rejections_total, ostro_route_read_barrier_seconds"
  - "controller W2 lease wait (ControllerConfig.lease_wait = session_timeout in production; LiveSource::heard_within)"
  - "RunWatchers::follow (led-set aware), WaitOutcome {Completed, Retry, LeaderChanged}, ActionSource / Subscription; await_result answers leader_changed"
  - "tier-B ryw group (9 scenarios + 1 mutant scenario under failpoints)"
affects: [13-17, 13-18, 13-19, 13-20]
tech-stack:
  added: []
  patterns:
    - "consistency rules as pure async functions over a ReadEnv trait (fake time / roles / anchor in unit tests, a real MetaHandle in the NB2 tests)"
    - "probe-paused zombie in tier B: outbound links of the leader cut + a 120 ms blip of the meta leader's stream to it (raft probe state, no further commits reach it)"
key-files:
  created:
    - crates/ostro-server/tests/p13_consistency_unit.rs
    - crates/ostro-server/tests/p13_watch.rs
    - crates/ostro-cluster/tests/controller_lease_wait.rs
    - crates/ostro-repl-conformance/tests/ryw.rs
  modified:
    - crates/ostro-server/src/route/consistency.rs
    - crates/ostro-server/src/control/watch.rs
    - crates/ostro-cluster/src/controller.rs
    - crates/ostro-cluster/src/liveness.rs
    - crates/ostro-cluster/src/config.rs
    - crates/ostro-repl-conformance/src/scenarios/ryw.rs
    - crates/ostro-repl-conformance/src/harness/inproc.rs
key-decisions:
  - "Partition lease = 13-05 metadata lease anchor (ReadIndex round trip on a meta follower, quorum-acked heartbeat round on the meta leader, now on a one-voter leader), never wall time; age < session_timeout x (1 - lease_margin)"
  - "Meta reads gate on read_index_barrier (bounded by min(deadline, 2 x session_timeout)), not on is_leader or contact age: closes the 12-18 stale-meta-read follow-up"
  - "Token offset = the partition's applied cursor (cursor.next) after the write; covered = handle epoch >= token epoch and cursor >= offset; meta token = local applied index"
  - "A token the leader does not cover takes the barrier + epoch check and then waits for the apply (bounded), with or without strict_reads (correctness never depends on tokens)"
  - "The read hook waits (bounded by session_timeout) while the metadata names this node and its takeover is running; await_result on LeaderChanged waits (bounded) for the metadata to name a successor: both needed for the 13-12 router's single re-route"
  - "RYW terminate instead of graceful cancel for the RunTerminal effect: a cancel is a request with no read-observable effect at its acknowledgement"
  - "Signal visibility is probed by re-sending every acked request_id (duplicate: true = the run's dedupe state holds it): the history only shows a signal once the guest consumes it"
requirements-completed: [API-08]
duration: "about 4 h of work + one parallel cluster ci-local (77 min)"
completed: 2026-11-25
---

# Phase 13 Plan 13: Read-your-writes Summary

**Every read now reflects the session's acknowledged writes from any node. A partition read is served only by a leader that holds a monotonic lease measured from its 13-05 metadata lease anchor, and the controller fails over only after the old leader's lease must have ended (W2). Meta reads pass a raft ReadIndex barrier, which closes the 12-18 stale-meta-read follow-up. Writes return `ostro-consistency` tokens, and a covered `ostro-read-after` skips the barrier. `[api] strict_reads` adds a barrier and epoch check. Long-poll waiters now follow leadership per term. The 13-08 session checker finds 0 stale reads across about 8,800 operations in 9 tier-B scenarios. The `serve_without_lease` mutant produced 62.**

## Rules as landed (`route/consistency.rs`)

**Partition read** (`before_partition_read`, called on the 13-12 local branch for direct and forwarded-in calls). It is served only when all of these hold:
1. **Leading:** the role is `Leader{epoch}`, which is set only after `LeaderAnnounced{epoch}` applied, and the handle stamps the same epoch. If the metadata already names this node and its takeover is still running, the hook waits for it, bounded by `session_timeout` and the request deadline.
2. **Not frozen:** `HandoffControl::in_handoff(p)` is false. It stays true after `complete()` until the demotion (13-11).
3. **Lease:** `now − meta_lease_anchor < session_timeout × (1 − lease_margin)`, measured on `Instant` (monotonic). If the anchor is missing or stale, the hook re-asks every 5 ms for at most `min(lease/4, 500 ms, deadline)`. A meta follower's first ask starts its rounds. A per-node keeper task asks every 1 s so a follower's rounds never stop; 13-05 runs them only while the anchor was asked for within 10 s.
4. **Strict mode or an uncovered token:** `read_index_barrier_until(min(deadline, now + 2 × session_timeout))`, then the meta assignment must still be `(self, epoch)`. For an uncovered token, the hook then waits for the apply to reach the token's offset, bounded by the same deadline.

When any condition fails, the answer is `unavailable` with `{reason: "not_leader", partition, leader}`. The 13-12 router re-routes it once and then answers 503 with `Retry-After: 1`. Each refusal also increments `ostro_route_lease_rejections_total`. A single RF=1 node keeps the fast path: the anchor is `now`, and there is no barrier and no RPC.

**Meta read** (`before_meta_read`): the barrier, bounded by `min(deadline, 2 × session_timeout)`. A timeout answers `unavailable` with reason `meta_unavailable` and `retry_after_ms: 1000`. A covered `m:<index>` token skips the barrier. Barrier latency goes to `ostro_route_read_barrier_seconds`. **12-18 follow-up: closed.** A cut-off old meta leader cannot pass the barrier because it never gets the quorum acknowledgement. It answers 503 instead of serving stale plain reads for about 2.6 s. The gate is the barrier, not `is_leader` or contact age.

**Tokens:** a partition write returns `p<P>:<epoch>:<offset>`, where `offset` is the partition's applied cursor after the write. A meta write returns `m:<applied index>`. A malformed `ostro-read-after` is a 400 `invalid_argument` on both paths. A token for a different partition, or a meta token on a partition read, is ignored.

**Controller W2** (`ostro-cluster/src/controller.rs`). `ControllerConfig.lease_wait` defaults to zero; `ReplicationTuning::controller` sets it to `session_timeout`. A failover `ElectLeader` that replaces leader L is proposed only when both hold:
- this controller has led meta for at least `lease_wait`;
- L is absent from `LiveSource::heard_within(lease_wait)`. The default implementation is the meta driver's own `sample_live` contacts.

L is the current leader, or the previous leader of an offline partition that did not resign. Resignations, handoffs, placements and fresh terms do not wait. A controller node never waits on itself.

**RunWatchers** (`control/watch.rs`). `follow_partition` (unchanged call site in control/mod.rs) delegates to `follow(p, led, source)`:
- On a new term here, the hub re-subscribes: it takes the new receiver first, then drains and drops the old one.
- When the partition is lost, waiters wake with `LeaderChanged`, and `await_result` answers `unavailable` reason `leader_changed` once the metadata names a successor (bounded by 2 × `session_timeout` and the long-poll deadline). The router then re-routes once.
- A new epoch on the same node wakes waiters with `Retry`. `Lagged` and `Closed` behave as before.

## Measurements

| What | Value |
|------|-------|
| Single-node partition read hook (one-voter meta, micro-benchmark) | 217 ns per read (stub 13 ns); the assertion bound is 5% of the 13-12 in-process GET p50 (39 µs) |
| Cut-off 3-voter meta leader, lease 320 ms (st 400 ms) | lease lost 306-313 ms after the cut, no successor yet (3 runs) |
| Tier-B probe-paused zombie, lease 480 ms (st 600 ms) | last read served +466-469 ms after the cut, first refusal +489-492 ms |
| Tier-B asymmetric zombie | refused from the first probe after the cut (answered `partition_offline` 0.7 s later, after its self-demotion); the successor was elected after that |
| Barriers, 900 ops without vs with tokens | 91 vs 16 (75 skipped) |
| Lease mode (steady, 2,004 ops) | read p50 3.0-3.6 ms, p99 12.7-14.3 ms; write p50 5.7-7.2 ms, p99 28.6-35.4 ms |
| `strict_reads = true` (1,200 ops, 778 barriers) | read p50 3.2 ms, p99 9.4 ms; write p50 4.1 ms, p99 12.3 ms |
| Await across handoff / resignation | answered `completed` after 0.85 s / 0.98 s (deadline 30 s) |

In-process, the lease and strict modes are within noise of each other because a barrier is a loopback raft round. Real network numbers belong to tier C (13-19), as input to the D-P13-15 user review.

## Checker operation counts (tier B, `RywChecker`)

| Scenario | ops | writes acked | reads recorded | unavailable | stale reads |
|----------|-----|--------------|----------------|-------------|-------------|
| ryw_across_nodes_steady | 2,004 | 940 | 915-916 | 0-1 | 0 |
| ryw_across_nodes_through_moves (4 handoffs, 4 resignations) | 1,500 | 693 | 663 | 28 | 0 |
| ryw_through_failover | 1,500 | 703 | 673 | 0 | 0 |
| zombie_leader_never_serves_stale | 400 + 40 pairs | 108-135 | 98-111 | 109-129 | 0 |
| tokens_skip_barrier (without / with tokens) | 900 / 900 | 419 / 419 | 401 / 402 | 1 / 0 | 0 |
| zombie_asymmetric | 400 | 147-150 | 144-148 | 64-68 | 0 |
| strict_reads_mode | 1,200 | 563 | 532 | 1 | 0 |
| zombie_serve_without_lease_mutant_bites (failpoints) | 400 + 40 pairs | | | | **62** (mutant detected) |

## Task commits

1. `d5209a9` feat(13-13): leader leases, meta read barrier, consistency tokens
2. `a7264a7` feat(13-13): per-term RunWatchers
3. `d2ce09b` fix(13-13): takeover wait in the read hook, successor wait in await
4. `7aca00c` test(13-13): tier-B RYW scenarios
5. `70bf2a5` test(13-13): 3-voter lease tests in ostro-cluster, no ostro-server dev-dep
6. `2e48cce` test(13-13): ryw failover counts 500 "transport error" separately

All on main through one CAS (rebased onto `8460b4d`, after 13-10 and 13-15 landed). The plan-level CI ran at `7aca00c` (the same code before the rebase onto 13-10 and before commits 5 and 6).

## Verification

- `cargo test -p ostro-server --test p13_consistency_unit`: 14 passed. With `--features failpoints`, the mutant test also runs and passes. `--test p13_watch`: 6 passed, 3 runs clean. `--lib watch`: 3 passed.
- `cargo test -p ostro-cluster --test controller_lease_wait`: 5 passed, 3 runs clean. `--test controller` (Phase 12): 9 passed.
- `cargo test -p ostro-repl-conformance --test ryw -- --test-threads=1`: 9 passed. `--features failpoints ... zombie`: 3 passed, including the mutant scenario.
- Regression (local): ostro-server `lifecycle_api`, `p13_routing_unit`, `cluster_api`, `cluster_boot`, `runs_api` all green.
- clippy `-D warnings` on ostro-cluster, ostro-server and ostro-repl-conformance, all targets, with and without failpoints: clean. fmt clean. `scripts/p12-grep-gates.sh` (including the clock gate): pass.
- After the rebase onto 13-10 (`ef8f828`): ostro-cluster `controller_lease_wait` 5, `controller` 9 and `placement_controller` 9 passed; tier-B `membership` 6, `reassign` 8 and `ryw` 9 passed (`ryw_through_failover` after commit 6, 2 runs).
- **Plan-level `ostro-k8s-ci --parallel` at `7aca00c`** (77 min): every step PASS except:
  - `cargo test`: `ostro worker_failover::oscillation`, which is on the known-flaky list;
  - `musl zigbuild test`: the same `worker_failover::oscillation`;
  - `failpoint suites`: `ostro-repl-conformance version_gate::single_node_downgrade_after_finalize_refused` ("workload: no run was acknowledged"). See the Deferred Issues entry below.
  - Four groups (poly, obuild, fp1, musl1) never ran: "database is locked" when the pod was created, and poly hit its backoff limit. This is cluster infrastructure, not a test failure.
  - `70bf2a5` only moves two tests and reverts the dev-dependency. It ran locally, not on the cluster.

## Deviations from Plan

1. **[Rule 3, Phase 13 shared-files rule] 3-voter NB2 tests moved to `ostro-cluster/tests/controller_lease_wait.rs`.** An in-process 3-voter group needs the raft message type (`ostro-raft-driver`), which ostro-server does not depend on, and manifests are 13-01-only. The first commit added a dev-dependency anyway. `70bf2a5` reverts it, so the net Cargo change of this plan is zero. `p13_consistency_unit.rs` keeps the one-voter `single_node_reads_served`, which uses `NullOutbox`, plus a pointer to the moved tests. The moved tests check the same lease rule (mirrored) against the real anchor.
2. **[Rule 1] Takeover wait and successor wait.** Found by `await_across_*`. The router's single re-route landed on a node that the metadata already named but that had not finished its takeover, or inside a resignation's leader-0 window, and answered 503. The fixes are in 13-13's own files (`d2ce09b`); route/mod.rs is unchanged.
3. **Files outside `files_modified`:**
   - `liveness.rs`: a defaulted `LiveSource::heard_within`.
   - `ostro-cluster/src/config.rs`: `Tuning::controller` sets `lease_wait`.
   - `harness/inproc.rs`: `InProcCluster::config_mut`, used for the strict_reads restarts.
4. **RYW workload adaptations:**
   - terminate stands in for cancel as the terminal write;
   - signal visibility is checked by re-sending request ids (history shows a signal only once the guest consumes it);
   - signal acks and probes go to a twin checker session, because a GET cannot show signals. The checker is unchanged.
5. **Zombie construction.** "Isolated from meta and followers but not from clients" is built as a probe-paused zombie: everything the leader sends is cut, plus a 120 ms blip of the meta leader's stream to it. With full isolation, the Phase 12 self-demotion (contact age > `session_timeout`) always fires before a successor can exist under W2, so no read lease is ever needed and the mutant could never bite. The probe-paused zombie keeps hearing heartbeats, never learns its replacement and never self-demotes. That is exactly the window the lease protects, and the mutant bites there (62 stale reads).
6. **`zombie_asymmetric`** cuts every link the others open to the zombie, not only `Raft`. A `Raft`-kind `Cut` does not touch the already-open stream, which left the zombie fully connected on the first try.
7. **Worktree / target / CI:** session-scratchpad worktrees, `target/slot-3`, and `ostro-k8s-ci --parallel` instead of `/tmp/ostro-wt-13-13` with a local ci-local.

## Deferred Issues / handover

- **Zombie write paths answer from stale state (found by the zombie scenario, not fixed here).** Signal, terminate (also cancel and retry) read the run record before submitting. On a probe-paused zombie, a run started through the new leader gets `404 not_found` from those writes; 3 such answers were seen per run. They cannot commit, so no state is corrupted, but the client sees "not found" for its own acked run. Proposed one-line fix, which belongs to 13-12 (route/mod.rs, fixed call sites; owner's decision): in `RouteCore::local`, also run `before_partition_read(p)` for `Access::PartitionWrite(p)`. A zombie then refuses writes with `not_leader` before the pre-read.
- **version_gate::single_node_downgrade_after_finalize_refused** (13-14, changed in f5e4663) failed in the cluster failpoint suite and 1 of 3 local runs on this tree. The scenario lasts about 1 s and starts one run per 200 ms. Base comparison at f5e4663 (without 13-13): it failed 1 of 6 runs too, so it is pre-existing and not caused by 13-13. 65c6249 (13-14) has since changed that workload.
- **500 "transport error" instead of 503 (13-12):** in `ryw_through_failover`, a start forwarded to the leader being killed answered `500 internal "transport error"` twice in one run. It should be a retryable 503. The scenario counts these as their own class (`transport 500s`). They are not stale reads. The fix belongs in the 13-12 peer client mapping or the router.
- **Remaining deadline after a re-route:** a REST long-poll without `ostro-deadline-ms` is re-routed with its original `wait`. The re-routed poll still returns as soon as the run completes, but it may wait up to `wait` again. The router (13-12) captures the meta before the call, so 13-13 cannot shorten it.
- **D-P13-15 user review:** the lease and strict numbers above (in-process). The lease anchor design (ReadIndex round trip instead of a per-batch RaftAck, 13-05) is unchanged.
- **13-18:** `route::consistency::stats()` (barriers, lease_refusals, token_skips) are process-wide probes, and the RYW tests serialise on them. The mutant is `route::consistency::MUTANT_SERVE_WITHOUT_LEASE` and bites in `zombie_serve_without_lease_mutant_bites`.
- **ci.yml:** no hunk needed. `ostro-repl-conformance` is already a whole crate in `FAILPOINT_CRATES`, so the mutant scenario runs in the failpoint suites, and the other ryw scenarios run in `cargo test --workspace` (about 2 min serial).

## Self-Check

PASSED: every created/modified file listed exists; commits d5209a9, a7264a7, d2ce09b, 2a864c5, 70bf2a5 and 2e48cce exist. Plan grep checks: `fn before_partition_read`, `read_index_barrier` and `meta_lease_anchor` in consistency.rs; `LeaderChanged` and `led()` in watch.rs; `routed_hooks_effective` and `single_node_reads_served` in p13_consistency_unit.rs (the two 3-voter tests are in controller_lease_wait.rs, see deviation 1); all nine scenario names in scenarios/ryw.rs.
