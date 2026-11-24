---
phase: 12-replicated-partition-logs
plan: 18
subsystem: replication-gate
tags: [replication, gate, ci, mutants, grep-gates, liveness, failover, campaign, docs, clus-01, clus-02, clus-03, clus-04]
requires:
  - phase: 12-replicated-partition-logs
    provides: "12-01..12-17: replication, tier-B suite 29/29 + 11 mutant pairs (12-16), tier-C kill paths, failover p99 harness and campaign (12-17)"
provides:
  - "ci-local: OSTRO_REPL_REQUIRE_ALL=1 in the failpoint suites + mandatory 'replication mutant matrix' step (>= 11 bites, no SKIP (pending))"
  - "scripts/p12-grep-gates.sh: one log path (ostro-log + ostro-cluster), no SetPartitionLeader, no legacy path, clock gate, no bincode/object_store in Cargo.lock; shared by ci-local and (after the coordinator applies the hunk) ci.yml"
  - "liveness: presence in a sample proves contact only since the sample's window start (suspect reports survive the regular samples; confirmed-dead nodes are not revived by stale windows)"
  - "stale_epoch::raw_fetch_request: raw fetches as a real follower sends them (last record's epoch)"
  - "docs/replication.md; storage-format §1/§10.1, snapshot-format §9.1, bottomless cluster mode, docs/03 corrected"
  - "RESERVATIONS P12 block final; REQUIREMENTS CLUS-01..04 complete with evidence; 12-VALIDATION filled; 12-DECISIONS execution-time amendments"
affects: [13]
tech-stack:
  added: []
  patterns:
    - "A liveness sample proves contact only since its window start: it may settle a suspicion or revive a dead node only if the window starts after the report / the confirmation window"
    - "Raw protocol requests in scenarios are built from the replica's measured disk state, exactly as the real client builds them"
key-files:
  created:
    - docs/replication.md
    - scripts/p12-grep-gates.sh
  modified:
    - crates/ostro-cluster/src/liveness.rs
    - crates/ostro-cluster/tests/liveness.rs
    - crates/ostro-cluster/src/controller.rs
    - crates/ostro-cluster/src/meta_peer.rs
    - crates/ostro-repl-conformance/src/scenarios/stale_epoch.rs
    - crates/ostro-repl-conformance/tests/stale_epoch.rs
    - scripts/ci-local.sh
    - docs/storage-format.md
    - docs/snapshot-format.md
    - docs/bottomless.md
    - docs/03-architecture.md
    - .planning/RESERVATIONS.md
    - .planning/REQUIREMENTS.md
    - .planning/phases/12-replicated-partition-logs/12-VALIDATION.md
    - .planning/phases/12-replicated-partition-logs/12-DECISIONS.md
key-decisions:
  - "Kill detection (12-17 handover 3) was a liveness accumulator bug, not tuning: the controller's 100 ms regular sample (window 1 s) cleared a fresh suspect report because the killed node was still inside the window. Fixed in two steps (362f51e revival after confirmation, c10f7b7 the suspicion itself). kill -9 of a non-controller leader: p50 2.45 s -> 0.89 s, p99 2.50 s -> 0.95 s"
  - "macOS stale_epoch (handover 1) is a harness fidelity bug: the raw fetch claimed last_fetched_epoch = the new epoch, which a leader that has not appended its LeaderAnnounced yet rightly answers SNAPSHOT_REQUIRED (KIP-101). Fixed in the scenario's request builder; server and oracle unchanged"
  - "Blackholed controller (handover 2) is not a correctness bug: writes to the cut-off leader never commit and are on no replica after the heal; its reads are stale until it self-demotes 4.6-5.2 s after the cut, because a meta leader's contact age starts only after check-quorum steps it down. Recorded as a 13-03 follow-up (ReadIndex / leader lease)"
  - ".github/workflows/ci.yml was not edited (coordinator-owned during this run); the matching hunk is below and is a human/coordinator step"
requirements-completed: [CLUS-01, CLUS-02, CLUS-03, CLUS-04]
duration: ~6.5 h wall (06:00-10:45 UTC plus the earlier build/investigation; long runs in the background)
completed: 2026-11-24
---

# Phase 12 Plan 18: Phase 12 gate Summary

**Phase 12 gate, run in full. ci-local with musl is green at 754f271 except one environment-caused `cargo test` red (a missing `CACHEDIR.TAG` in the wiped build slot; the test passes on re-run, and the musl leg ran the whole workspace green). The mutant matrix is now mandatory in ci-local: 11 bites, no pending. The new Phase 12 grep gates pass. The red run is 29/29. The tier-C kill paths pass 10/10 each and their mutant bites. Failover p99 over 200 iterations is 5.44 s (< 10 s). Two of the 12-17 handovers were real bugs and are fixed with regression tests: kill detection (a liveness accumulator bug; kill -9 failover of a non-controller leader dropped from p99 2.50 s to 0.95 s) and the macOS `stale_epoch` failure (harness fidelity). The blackholed-controller delay is a stale-read window, not a write-safety problem. The 10,000-fault campaign is HUMAN_NEEDED: a 6,000-fault run reached 1,020 faults and 179,412 acks before its WAL-only checker (no D-P12-27 state probe) flagged runs that the post-mortem shows are on 3/3 nodes. Wiring the approved rule into the campaign is a user decision. Docs and tracking files are complete; ci.yml is handed to the coordinator.**

All WSL2 numbers are advisory (D15). Tier-C runs used the debug failpoint server and release test
harnesses built at c10f7b7, which is the final code; the commits after it touch docs, scripts and
`.planning` only.

## Commits

| Commit | Message |
| --- | --- |
| 362f51e | fix(12-18): a confirmed-dead node is not revived by the same poll's wider sample |
| aaa5eab | fix(12-18): stale_epoch's raw fetch carries the follower's real last epoch |
| 5782db7 | ci(12-18): replication mutant matrix required in ci-local + Phase 12 grep gates |
| a6372af | docs(12-18): replication, storage/snapshot/bottomless Phase 12 notes |
| 141954f | ci(12-18): Phase 12 grep gates in a shared script for ci-local and ci.yml |
| c10f7b7 | fix(12-18): a regular liveness sample older than a suspect report keeps the suspicion |
| 754f271 | docs(12-18): replication failover table from the 12-18 gate run |
| df38c20 | docs(12-18): RESERVATIONS, REQUIREMENTS, VALIDATION, DECISIONS |
| (docs) | docs(12-18): summary, ROADMAP tick, STATE line |

## Handovers from 12-17

### 1. macOS `stale_epoch`: "answered SnapshotRequired, expected None" (run 45309300428)

**Root cause: harness fidelity, not the server.** `fetch_expect` built its raw `Fetch` with
`last_fetched_epoch = leader_epoch`, i.e. it claimed the follower had already fetched records of
the new leader's epoch. `PartitionLog::become_leader(e)` sets the role `Leader{e}` before the role
driver appends `LeaderAnnounced{e}`, so for a few ms the leader's epoch history ends at `e-1`.
`EpochCache::end_offset_for_epoch(e)` for an epoch above the latest is UNDEFINED, which the fetch
check correctly answers `SNAPSHOT_REQUIRED` (Kafka does the same: "the requested epoch is larger
than any known epoch"). A real follower cannot be in that state: it sends the epoch of its last
record. The "current epoch is still served" leg of `fetch_with_stale_epoch_fenced` sends its fetch
right after the failover, so on a slow host (macOS CI) it lands in that window.

**Fix (aaa5eab):** `stale_epoch::raw_fetch_request(scan, leader_epoch)` builds the request from
the follower's scanned log: `fetch_offset = durable_leo =` its durable end,
`last_fetched_epoch =` its last record's epoch. Oracles and server unchanged.

**Regression test:** `tests/stale_epoch.rs::raw_fetch_is_served_before_the_new_leaders_announcement`
puts a real `PartitionLog` in exactly that window (led at epoch 2, history `[(1, 0)]`) and calls
`handle_fetch`: the new request is served (`None`), the old request's claim is answered
`SNAPSHOT_REQUIRED`. Deterministic.

**Reproduction attempts on Linux (pre-fix scenario binary built from cc56188's scenario source):**
480 scenario runs, 0 failures: 2 parallel loops × 60 repetitions × both `stale_epoch` scenarios at
ambient load 2.5-3 (the campaign running), then the same with 6 CPU burners (load 11-16). The window
is a few ms on this host; the deterministic test is the evidence.

### 2. Blackholed controller: pre-fault run completes ~2.6 s after new starts are acked

**Not a correctness bug.** Investigation (a throwaway tier-C test, not committed; log in
`target/p12-gate/cutoff-controller-investigation.log`): 4 iterations, each blackholing the
controller while it leads a partition with an acknowledged 300 ms durable-sleep run, sampling the
old leader and the survivors every 50 ms and sending a new start straight to the old leader 200 ms
into the cut.

| Iteration | New leader serves, run `completed` | Old leader stops answering (503) | Old leader log: Raft "became follower from Leader" / "self-demotion" | Write sent to old leader | On any replica after heal |
|---|---|---|---|---|---|
| 0 | +3.1-3.3 s | +4.82 s | +1.55 s / +4.80 s | hung, 503 at +4.80 s | no (3/3 nodes, not even uncommitted) |
| 1 | +2.94 s | +4.66 s | +1.56 s / +4.60 s | hung, 503 at +4.61 s | no |
| 2 | +3.14-3.20 s | +4.59 s | +1.47 s / +4.57 s | hung, 503 at +4.57 s | no |
| 3 | +3.45 s | +5.16 s | +1.55 s / +5.10 s | hung, 503 at +5.10 s | no |

Mechanism: `leader_contact_age()` is kept fresh while a node is meta leader. A cut-off controller
stays meta leader until Raft check-quorum steps it down (about one election timeout, 1.5 s here);
only then does its contact age grow, so it self-demotes `session_timeout` (3 s) later, at 4.6-5.2 s.
Meanwhile the survivors elect a new meta leader, whose seeding window elects a new partition leader
at about 3 s. Between ~3 s and ~5 s two nodes answer reads as leader: the old one from its last
committed state (the run shows `ready` there while it is `completed` on the new leader). The old
leader's writes cannot commit: its HW needs ISR followers that cannot fetch from it, and the ISR can
only change through meta, which it cannot reach. The failover harness's client follows the old
leader's self-hint for reads, so its "pre-fault run completes" time includes that window: the
"catch-up + re-grant" p99 of 2.4 s is client-side staleness, not a slow re-grant.

**Follow-up (recorded in STATE and 12-DECISIONS, for 13-03):** reads are not linearizable in
Phase 12 (known; ReadIndex and leader leases are 13-03). Closing the overlap needs the meta
leader's contact age to be its last quorum contact (raft-rs `check_quorum` bookkeeping in the
vendored driver), not a small change.

### 3. Kill detection ~2.3 s although `confirm_window` is 750 ms

**Root cause: a liveness accumulator bug.** The controller polls liveness every controller tick
(100 ms): a confirming sample when one is due, then a regular sample with
`within = sample_window` (1 s). A `kill -9`'d leader is still "heard within 1 s" for up to a second,
so:

1. the follower's suspect report arrives within a few ms of the kill;
2. the next regular sample (≤ 100 ms later) contains the dead node and cleared the suspicion;
3. later reports from the fetcher's retries were cleared the same way until the node had been
   silent for a whole sample window (~1 s);
4. only then did a report survive to its confirmation 750 ms later: ~1 s + backoff + 750 ms
   + tick ≈ 2.3 s.

A second instance of the same flaw: the regular sample taken right after a confirmation revived a
node just confirmed dead.

**Fix (small, two commits, each with a regression test that fails without it):**
`Liveness::observe` treats presence as proof of contact only since the sample's window start
(`now − sample_window`). A suspicion reported at R is settled by a regular sample only when the
window starts after R (otherwise the confirming sample decides, which is the documented design);
a node confirmed dead at D is revived only when the window starts after `D − confirm_window`.
362f51e (revival; `a_wider_sample_right_after_the_confirmation_does_not_revive`) and c10f7b7 (the
suspicion; `regular_samples_older_than_the_report_keep_the_suspicion`; the existing
`suspect_present_in_the_confirming_sample_is_cleared` now expects a same-instant regular sample to
keep the suspicion and a later one to clear it). Safety: a live node is still cleared by the
confirming sample (heartbeat responses every 200 ms); the change can only delay clearing a
suspicion by at most one sample window, never declare a node dead without the confirming sample.

**Effect (200-iteration gate run):** kill/non-controller p50 2446 → 894 ms, p99 2496 → 954 ms;
detection step p50 2323 → 1037 ms. Controller kills (seeding path) and blackholes (session-timeout
path) are unchanged, as expected.

### 4. The 10,000-fault campaign

**Result: not completed: HUMAN_NEEDED (user decision on a crash checker), no acked run lost.**
Evidence in `target/campaign-p12-gate/` (logs, `HUMAN_NEEDED`, `postmortem-fault-1030.log`, the
kept cluster `cluster-at-fault-1030/`).

| Run | Outcome |
|-----|---------|
| 10,000 faults at aaa5eab | stopped at ~400 faults to restart on the full liveness fix (c10f7b7) |
| 10,000 faults at c10f7b7 | stopped by me at fault 570 (82,658 acks, 57 batch disk checks green). At ~13 KB of `/dev/shm` per ack (the tier-C harness puts clusters on `/dev/shm`) 10k faults would need ~18 GB of the 17 GB free, and at 4-5 s per fault under the concurrent ci-local load it would also overrun the ~11 h box. Restarted at 6,000 |
| 6,000 faults at c10f7b7 | 1,020 faults, 179,412 acknowledged runs, **102 batch disk checks green**; the check after fault 1,030 failed: 133,443 acked runs "on disk on 1 node(s) [1] < min_insync 2" |

**Post-mortem of fault 1,030** (a throwaway probe over copies of the kept data dirs, not
committed): at 08:58 nodes 2 and 3 got `SNAPSHOT_REQUIRED`, installed the leader's snapshot
(p0 135188, p1 132778, p3 135228) and WAL retention then trimmed their logs below it. All 133,443
flagged runs lie below nodes 2/3's WAL start, and **every one is in both nodes' checkpointed state
stores**; node 1's untrimmed WAL holds all of them. Every flagged run is on 3/3 nodes.

| Node | WAL p0 / p1 / p3 | flagged runs in WAL | in state only | neither |
|------|------------------|---------------------|---------------|---------|
| 1 | [0, 138081) / [0, 135565) / [0, 138217) | 133,443 | 0 | 0 |
| 2 | [135188, …) / [132778, …) / [135228, …) trimmed | 0 | 133,443 | 0 |
| 3 | [134515, …) / [132423, …) / [134649, …) trimmed | 0 | 133,443 | 0 |

**Cause: the campaign's checker, not the cluster.** `tests/cluster_campaign.rs::disk_check` and
`scenarios::process::tools::final_checks` call the WAL-only `oracle::disk::acked_on_disk`, without
the user-approved D-P12-27 state probe (`acked_on_disk_with`) that tier B uses. 12-17's 500-fault smoke
(34 min) evidently never trimmed a copy (it passed with the same check). Any run long enough for a
follower snapshot install or a retention pass trims a copy, and from then on it always fails. Following the hard rule, I did **not** change the checker. **Decision needed:**
approve wiring D-P12-27 into the tier-C campaign (a state probe that opens a read-only copy of a
trimmed node's state store), then re-run `OSTRO_CAMPAIGN_FAULTS=10000`. That run needs ~18 GB in
`/dev/shm` (or a disk-backed test dir) and 7-12 h on WSL2, so the native machine is recommended.

**Follow-up p12-campaign (2026-11-24, after the user approved D-P12-28).** The D-P12-27 rule is now
wired into the campaign's batch `disk_check` and `tools::final_checks` (df9c153:
`oracle::disk::acked_on_disk_copies` = `acked_on_disk_with` + a probe that opens a copy of a trimmed
node's state store; `acked_on_disk` / `acked_on_scans` unchanged; harness knob `OSTRO_PROC_ROOT` for a
disk-backed cluster dir).

- Fault-1030 recheck on the kept cluster: 133,443 of 133,443 flagged runs are evidenced on 3/3 nodes
  (node 1 WAL; nodes 2 and 3 trimmed copy + state store), 0 violations at min_insync 2 and 3. The WAL-only
  oracle still reports all 133,443.
- The rule still catches loss: `count_nondurable_leo` (tier C) still bites on the disk oracle (baseline
  passes through the new final checks), and `oracle_selftest` passes 11/11, including a new tier-C-shaped
  test over real WAL dirs and state stores (trimmed + in state passes; trimmed + missing fails; untrimmed
  stays strict; a missing store is reported).
- 10,000-fault re-run (cluster on disk): **FAILED at fault ~641-650 on availability**, not on a disk
  check. 64 batch disk checks passed (640 faults, 72,209 acks, 2,808 s), then every partition sat at
  `leader 3 isr [3]` for 90 s. Cause (code read, not fixed): node 3 lost metadata contact while
  Announcing e259 (LeaderAnnounced pending on NotEnoughReplicas). The log stayed in `Mode::Leader{259}`
  with no record at 259. When contact came back, `decide()` said Lead(259) because the epoch cache said 258,
  `become_leader(259)` returned StaleEpoch{259}, and `go_leader` went Offline without requesting a term,
  so the partitions were wedged. This is a liveness bug and needs a server fix before the 10k run.
  Evidence: `target/campaign-p12-10k/` (POSTMORTEM.txt, campaign log, kept cluster `ostro-proc-Y9CzGN/`).

Side observation (follow-up, not investigated further): node 2 got `SNAPSHOT_REQUIRED` from a
leader whose own log was not trimmed at that moment. That is safe (an install replaces the
follower's state with committed state) but costly. A likely cause is the same new-leader window
as the `stale_epoch` bug: a real follower whose last epoch is above the epochs the new leader has
logged before its `LeaderAnnounced` gets UNDEFINED. Worth a look in Phase 13: seed the new epoch
into the leader's epoch history at `become_leader`, or wait for LA before serving fetches.

### 5. Full ci-local with musl

`bash scripts/ci-local.sh` (with musl) in a detached worktree at **754f271** (the last code/script
commit; everything after it is `.planning` only), slot-4, 06:46-10:33 UTC (3 h 47 min), with the
campaign running alongside. Log: `target/p12-gate/ci-local.log`.

```
toolchain 1.100.0                    PASS  0s
cargo fmt --check                    PASS  5s
cargo clippy -D warnings             PASS  107s
cargo build (warning-free)           PASS  103s
sdk workspace fmt + clippy           PASS  32s
sdk workspace test + wasm build      PASS  37s
guests build (wasm32-wasip2)         PASS  25s
order-saga example                   PASS  33s
external: OSTRO_TEST_PG/MYSQL/S3     SKIP
cargo test                           FAIL  3688s   <- environment, see below
polyglot toolchains / python sdk / ts sdk / polyglot conformance / clippy polyglot   PASS
clients python / ts / go             PASS
ostro-build polyglot                 PASS  547s
semantics sleepers (serial)          PASS  66s
failpoint list sync (ci.yml)         PASS  0s
failpoint suites                     PASS  3971s   (with OSTRO_REPL_REQUIRE_ALL=1)
replication mutant matrix            PASS  234s    (new step)
server campaign smoke (20 kills)     PASS  551s
phase 7 failpoint legs + manifests   PASS  50s
cargo deny check / check-vendored / deny fixtures / no chronik   PASS
one log path + P12 grep gates        PASS  0s      (new gates)
workflow validation                  PASS  2s
zig + cargo-zigbuild present         PASS  1s
musl release (static, mimalloc)      PASS  433s
musl binary size < 60 MB             PASS  0s
musl zigbuild test                   PASS  3175s   (388 test binaries, 3241 passed, 0 failed)
```

**The one red, `cargo test`, came from the environment, not the code:** `ostro-server
--test triggers_e2e::cron_cli_policies` locates the `ostro` binary by walking up to the nearest
`CACHEDIR.TAG`. The freshly wiped `target/slot-4` had none (the new cargo build-dir layout did not
recreate it), so the helper picked `target/` and ran `cargo build --target slot-4`. I copied
cargo's standard `CACHEDIR.TAG` into slot-4 and re-ran the binary at the same commit: 3/3 passed,
`cron_cli_policies` included (`target/p12-gate/12-18-triggers-rerun.log`). `cargo test` stops at
the first failing binary (354 of the workspace's test binaries had run). The musl leg ran the
whole workspace test suite (`cargo-zigbuild test --workspace`, 388 binaries) green, which covers
the binaries the host run skipped. Linux-only gate; macOS not run locally.

## Task 1: CI gates

- `scripts/ci-local.sh`: the failpoint suites run with `OSTRO_REPL_REQUIRE_ALL=1`; new step
  "replication mutant matrix" (`check_repl_mutants`): `OSTRO_REPL_REQUIRE_ALL=1 cargo test -p
  ostro-repl-conformance --features failpoints --locked --test mutants -- --test-threads=1
  --nocapture` must pass with ≥ 11 `FAILED as expected` and no `SKIP (pending)`.
- `scripts/p12-grep-gates.sh` (called by ci-local's "one log path + P12 grep gates" step): no
  `Rf1Log` in `crates/`; no `rf == 1` / `replicas.len() == 1` in ostro-log **and ostro-cluster**
  src; no non-comment `SetPartitionLeader` in ostro-server/ostro-cluster src; no "legacy path" in
  ostro-partition src; the clock gate (`SystemTime::now(` / `SystemClock` / `Instant::now(` in
  ostro-cluster src and the Phase 12 files, `clock-exempt:` opt-out); no `bincode` /
  `object_store` package in Cargo.lock (chrono and bincode for normal dependencies stay with
  `cargo deny`; chrono is in the lock only through the s3s test server). grep errors other than
  "no match" fail the step. Probed: a file with `rf == 1`, a non-comment `SetPartitionLeader` and
  an `Instant::now()` in ostro-cluster/src fails all three gates.
- The new gates found two hits, both fixed: the controller's info log for an under-replicated
  partition compared `replicas.len() == 1` (now `< rf`, same message; also covers rf−1 replicas),
  and the MetaPeer reconnect backoff's connection timer is now marked `clock-exempt:`.
- `FAILPOINT_CRATES` unchanged and still equal to the ci.yml "Failpoint suites" list.

### ci.yml hunk (not applied: coordinator-owned; validated with a YAML parse and ci-local's sync check)

```diff
@@ lint job, after "No chronik / tokio-uring / prometheus in the tree"
         run: bash scripts/forbidden-crates.sh --self-test && bash scripts/forbidden-crates.sh
+      - name: Phase 12 grep gates
+        # shared with ci-local.sh (12-08, 12-18): one log path, no SetPartitionLeader,
+        # no legacy path, clock gate, no bincode/object_store in Cargo.lock
+        run: bash scripts/p12-grep-gates.sh
@@ test job, "Failpoint suites"
         # order; ci-local's "failpoint list sync" step checks it)
+        env:
+          # 12-18: a pending (skipped) replication mutant fails the suites (CLUS-01)
+          OSTRO_REPL_REQUIRE_ALL: "1"
         run: |
@@ test job, before "Phase 7 failpoint legs + manifests"
+      - if: matrix.name == 'linux-x86_64-gnu'
+        name: Replication mutant matrix
+        # keep in sync with check_repl_mutants in scripts/ci-local.sh (12-18): every
+        # tier-B mutant must bite (>= 11) and none may be skipped as pending
+        run: |
+          out="$(OSTRO_REPL_REQUIRE_ALL=1 cargo test -p ostro-repl-conformance --features failpoints --locked --test mutants -- --test-threads=1 --nocapture 2>&1)" || { echo "$out"; exit 1; }
+          grep -E 'MUTANT |test result' <<<"$out" || true
+          grep -Eq 'test result: ok\. [1-9][0-9]* passed' <<<"$out"
+          test "$(grep -c 'FAILED as expected' <<<"$out")" -ge 11
+          ! grep -q 'SKIP (pending)' <<<"$out"
```

### Gate measurements

**Mutant matrix** (REQUIRE_ALL, at 5782db7's tree with the 362f51e/aaa5eab fixes; 244 s): 11
`FAILED as expected`, no SKIP, every baseline green; every bite on its 12-16 target oracle
(isr_from_assignment/status, hw_leader_leo ×2 disk/hw, skip_epoch_check/fence, no_truncate/replica,
unclean_election ×2/election, ignore_min_insync/min_insync, shrink_before_commit/hw,
upload_above_hw/objstore, rf1_bypass/one_path); count_nondurable_leo is tier C (below).

**Red run** (`OSTRO_REPL_RED_RUN=1 … --test red_run`, at 754f271): 29/29 tier-B scenarios pass, 0
pending, 0 harness errors (247 s); `skeleton::no_pending_left` ok.

**Tier C** (`OSTRO_PROC_ITERS=10`): `process_divergence` 10/10 (3.1-4.3 s each),
`process_power_loss` 10/10 (9.8-11.1 s each), `process_mutants`: baseline pass, mutant
`count_nondurable_leo` FAILED as expected on the disk oracle ("on disk on 1 node(s) [3] <
min_insync 2").

**Failover p99** (`target/failover-p99/report.txt`, 200 iterations, 3 processes, RF 3, min_insync 2,
shipped defaults; run concurrently with the campaign, load about 3):

```
failover p99 (12-17): 200 iterations, 3 processes, RF 3, min_insync 2
config: session_timeout 3s, meta_tick_interval 100ms, meta_election_tick 10, meta_heartbeat_tick 2, replica_lag_max 10s, fetch_max_wait 500ms
steady workload: 10314 starts acknowledged, 1414 not (during faults)
overall                            n=200  p50=  2431 p99=  5438 max=  5491 ms
kill -9                            n=134  p50=   897 p99=  3460 max=  3464 ms
blackhole (no RST)                 n=66   p50=  4148 p99=  5491 max=  5491 ms
controller target                  n=67   p50=  2440 p99=  5491 max=  5491 ms
non-controller target              n=133  p50=   897 p99=  4203 max=  4203 ms
kill / non-controller              n=89   p50=   894 p99=   954 max=   954 ms
kill / controller                  n=45   p50=  2435 p99=  3464 max=  3464 ms
blackhole / non-controller         n=44   p50=  4102 p99=  4203 max=  4203 ms
blackhole / controller             n=22   p50=  5129 p99=  5491 max=  5491 ms
step: detection                    n=200  p50=  1037 p99=  4050 max=  4077 ms
step: meta re-election (ctl)       n=67   p50=  1177 p99=  1750 max=  1750 ms
step: ElectLeader apply            n=200  p50=    41 p99=    55 max=    56 ms
step: LA commit                    n=200  p50=     0 p99=    53 max=    54 ms
step: catch-up + re-grant          n=200  p50=    68 p99=  2416 max=  2528 ms
detection paths: {"new-controller seeding": 67, "session-timeout sample": 44, "suspect report confirmed": 89}
controller iterations: 67, blackhole iterations: 66, targets that led no partition: 0, not resumed: 0
```

(The header still says "12-17": it is the test's fixed title.) An aborted first attempt at aaa5eab
(only the 362f51e half of the liveness fix) showed kill/non-controller still at ~2.4 s, which is how
the second half (c10f7b7) was found; both campaign and failover runs were restarted on c10f7b7.

## Task 2: Documentation

- `docs/replication.md` (new): model (one path, follower pull, durable LEO, maximal ISR,
  min_insync freeze, KIP-595 divergence and the validation order, ElectLeader/AlterIsr/ResignLeader
  apply rules, no unclean election, taking over, offline and self-demotion, hot standby, snapshot
  bootstrap), static cluster setup (full TOML with `cluster_id`, `[[cluster.peers]]`, shared CA,
  `[replication]` keys and defaults, identity checks, bootstrap placement, existing single nodes
  grow in Phase 13, the sweep), failover timing budget with the 12-18 table and the cut-off-leader
  behaviour, `/v1/admin/replication` field reference, 503 + leader hint and indeterminate outcomes
  (idempotency keys, signal `request_id`, `(epoch, attempt)` fencing), downgrade unsupported
  (D-P12-17), cluster restore deferred (D-P12-3), test tiers, commands, red run and mutant matrix.
- `docs/storage-format.md`: §1 `node.toml` `node_id`/`cluster_id` binding (D-P12-16, D-P12-14
  source order); §10.1 behavioural notes: F4 HW at open = `applied_offset`, the D-P12-4
  parsed-base rule (verbatim plus the as-built 47-byte envelope prefix), install resets the log
  first, follower trimming; "no byte format changed in Phase 12". The stale `Rf1Log::seed_epochs`
  name in §10 is now `PartitionLog::seed_epochs`.
- `docs/snapshot-format.md`: §9.1 Phase 12 follower install (framing, which snapshot, resume and
  pins, staging and verification, fixed order, crash analysis per step); §11 checklist ticked.
- `docs/bottomless.md`: "Cluster mode" (leader-only uploads, `p/<pppp>/head` and epoch fencing,
  takeover repair steps, single-member LOCK/manifest, restore refusal with the exact message).
- `docs/03-architecture.md`: the new leader appends `LeaderAnnounced` at the meta-assigned epoch
  and waits for it to commit and apply (not "applies up to its LEO").

## Task 3: Tracking files

- RESERVATIONS: MetaCommand 4-6 and ext tag 80 → `exists (P12)`; MetaCommand 7, ext 81-95,
  Cmd 41-42, MetaRecord 26 → `reserved P12, unused`; a 12-18 end-gate line. Re-verified against the
  final tree (the 12-01 script was not committed, so the P12 block was checked directly):
  `MetaCommand` variants 0-6 = InitCluster, Put, Delete, SetPartitionLeader, ElectLeader,
  AlterIsr, ResignLeader; `EXT_*` constants 15 and 80 only; `Cmd` oneof tags
  1-13, 20, 21, 27, 45; `MetaRecord`/`MetaKind` end at index 16. §11 names unchanged.
- REQUIREMENTS: CLUS-01..04 ticked; traceability rows Complete with evidence (test names,
  commits, report paths; native-Linux p99 human_needed noted on CLUS-04).
- 12-VALIDATION: frontmatter `nyquist_compliant: true`, `wave_0_complete: true`; every task row
  green; Wave 0 and sign-off ticked; new "Gate Evidence (12-18)" section; human_needed list
  extended (ci.yml hunk, stale-read follow-up).
- 12-DECISIONS: "Execution-time amendments" table (D-P12-4/5/7/9/10/13/18/19/20/27 and the test
  fidelity note, each with its plan).
- STATE: 12-18 line, Phase 12 18/18 with verification pending (not marked complete), decisions
  amendment, Phase 13 follow-ups, macOS backlog note.

## Deviations from Plan

### Auto-fixed issues

1. **[Rule 1] Liveness: suspect reports cleared by regular samples** (handover 3). c10f7b7 +
   362f51e, regression tests above. Each change is ≤ 10 lines of code in `liveness.rs`.
2. **[Rule 1] `stale_epoch` raw fetch claimed the new epoch** (handover 1). aaa5eab. The phase rule
   asks for ≤ 10-line gate fixes; this one is 13 lines of request-builder code plus a doc comment
   and a 70-line regression test, all in the conformance crate. No server or oracle change.
3. **[Rule 3] The new grep gates' two hits** (controller log condition, MetaPeer timer), 5782db7.
4. **Grep gates moved to `scripts/p12-grep-gates.sh`** (not in `files_modified`), so ci.yml can
   call the same script; ci-local calls it.

### Not done as planned

- **`.github/workflows/ci.yml` not edited** (coordinator instruction for this run). The hunk above
  makes CI match ci-local; the plan's verify lines that grep ci.yml for `OSTRO_REPL_REQUIRE_ALL` and
  "Replication mutant matrix" fail until it is applied.
- **Campaign time box:** the plan says 4 h; the coordinator set about 11 h. The 10k run was stopped at 570 faults (shm budget and time box) and a 6,000-fault run reached 1,020 faults before its checker hit the D-P12-27 gap (above): QA-01 at the gate is HUMAN_NEEDED (`target/campaign-p12-gate/HUMAN_NEEDED`).
- `bench_rf3` not re-run (reported, not gated; 12-17's numbers stand).

## Open for the verifier

- Apply the ci.yml hunk (coordinator) and confirm the CI steps are green.
- **Campaign (QA-01): user decision** on wiring the D-P12-27 trimmed-log rule into the tier-C campaign/final checks, then a 10,000-fault re-run (native machine). Until then the evidence is 1,020 faults / 179,412 acks / 102 green batch checks, plus the post-mortem showing the stop was the checker, not a loss.
- Stale reads from a cut-off controller until self-demotion (about 1.5-2 s overlap with the new
  leader): 13-03 (ReadIndex / leader lease; meta-leader contact age from quorum contact).
- 12-16's open item stays: `isr_from_assignment` sometimes bites through the status oracle's
  weaker form ("no stable status sample"); it bit in the gate run.
- human_needed: native-Linux failover p99 + benchmark; SeaweedFS legs; remote CI legs; user review
  of D-P12-2/3/4.
- macOS: the only macOS-only failure seen is the `stale_epoch` one (fixed in the harness). No other
  macOS-only failure was observed by this plan (macOS CI was not run locally).

## Self-Check: PASSED

- Commits 362f51e, aaa5eab, 5782db7, a6372af, 141954f, c10f7b7, 754f271 and df38c20 are ancestors of main.
- Files present: docs/replication.md, scripts/p12-grep-gates.sh; evidence (main checkout, not committed): target/failover-p99/report.txt, target/p12-gate/{ci-local.log, mutant-matrix.log, 12-18-redrun.log, 12-18-process_*.log, cutoff-controller-investigation.log, 12-18-repro*.log}, target/campaign-p12-gate/{HUMAN_NEEDED, campaign logs, postmortem-fault-1030.log, cluster-at-fault-1030/}.
- Plan verifies: Task 2 and Task 3 automated checks pass. Task 1's check fails only on the two ci.yml greps (`OSTRO_REPL_REQUIRE_ALL`, "Replication mutant matrix") because ci.yml was not edited, and on `target/campaign-p12-gate/report.txt`; `HUMAN_NEEDED` is present instead, which the check accepts.
