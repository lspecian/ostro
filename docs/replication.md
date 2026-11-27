# Replicated partition logs (Phase 12; membership and routing: Phase 13)

Operator and design reference for Ostro's replication: CLUS-01..04, decisions D-P12-1..27
(`.planning/phases/12-replicated-partition-logs/12-DECISIONS.md`). The research behind it is
`12-RESEARCH.md` in the same directory. Byte formats did not change in Phase 12
(`docs/storage-format.md` §14.5, §10).

## 1. Model

**One path.** There is one partition log implementation, `PartitionLog` (`crates/ostro-log`).
A single node is a cluster of one: replicas `[self]`, ISR `{self}`, `min_insync = 1`. It boots
meta-first exactly like a cluster (placement, then an `ElectLeader` term, then
`LeaderAnnounced`). CI keeps it that way with grep gates (no `Rf1Log`, no `rf == 1` /
`replicas.len() == 1` branch in `ostro-log` or `ostro-cluster`) and the `rf1_bypass` mutant
(D5, D-P12-15).

**Metadata.** A static multi-voter Raft group (the meta log, `crates/ostro-meta`) holds the
partition assignments: `{replicas, leader, epoch, isr, isr_version, min_insync}` plus the
election info (ext tag 80). In a static cluster the voters are this node plus `[[cluster.peers]]`
(3 or 5 nodes); with dynamic membership (Phase 13, §9) the meta configuration is authoritative. Raft
traffic runs over `ostro.cluster.v1.MetaPeer` on the cluster listener (mTLS). The metadata
leader also runs the **controller**: bootstrap placement, liveness, elections. Joining and
removing members is Phase 13's dynamic membership (CLUS-05; §9 and `docs/cluster-operations.md`).

**Follower pull.** Followers fetch from the leader with `Replication.Fetch` (long poll,
`fetch_max_wait`). A response carries the leader's envelopes byte for byte (followers never
re-encode, F1), the leader's HW, `log_start` and durable end. A follower fetches every blob a
batch references before it appends the batch (D-P12-22), appends the envelopes to its own
WAL, and reports its **durable LEO** (fsynced) separately from its fetch offset. Only the
durable LEO counts toward the HW (D-P12-5).

**High watermark.** `HW = min(durable LEO over the published ISR ∪ pending additions)`
(KIP-497 maximal ISR). A pending shrink keeps counting the removed replica until meta commits
the shrink. HW never decreases on one leader. It is published at one point
(`HwVector::publish_hw`), which also feeds bottomless (F12a). A command is committed (the
client is acknowledged, state machines apply it) once the HW passes it.

**ISR.** A follower is in sync while `now − last_caught_up ≤ replica_lag_max`. It catches up
when its fetch offset reaches the `leader_durable_end` the leader returned in its previous
response (never the moving LEO). The leader's `IsrPublisher` proposes `AlterIsr` from measured
progress. Expansion requires `durable_leo ≥ max(HW, start of the leader's current epoch)`. A
replica the controller dropped is re-added only after it fetched again. The leader follows the
**published** ISR (meta), never the assignment.

**min_insync.** While `|published ISR| < min_insync` the leader refuses appends
(`NotEnoughReplicas`, HTTP 503) and the HW is frozen. Availability is traded for durability
on purpose (SC3).

**Divergence (KIP-595 inside Fetch).** The follower sends `last_fetched_epoch` (the epoch of its
last record; the snapshot's `latest_epoch` for an anchored empty log). The leader validates the
fetch in this order: replica identity (`NOT_REPLICA`), epoch fencing (KIP-320:
`FENCED_LEADER_EPOCH` for an older epoch, `UNKNOWN_LEADER_EPOCH` for a newer one), leadership
of exactly that epoch (`NOT_LEADER`), then divergence. If the follower's epoch ended earlier on
the leader, the answer is `diverging{epoch, end}`: the follower truncates to
`min(end, its own end of that epoch)` and fetches again (KIP-279). An epoch older than the
leader's retained history, or a fetch offset below the leader's `log_start`, is answered
`SNAPSHOT_REQUIRED`. Ostro never truncates to the HW (KIP-101).

**Election.** Three meta commands, all validated at apply (D-P12-7):

| Command | Apply rule |
|---------|-----------|
| `ElectLeader{partition, epoch, leader, isr, expected_isr_version, info}` (MetaCommand 4) | `epoch > current`, `isr_version == expected`, the new `isr` is non-empty and ⊆ the current published ISR (also when `leader == 0`), `leader ∈ isr` unless 0; bumps `isr_version`, writes ext tag 80 |
| `AlterIsr{partition, epoch, expected_isr_version, isr}` (5) | `epoch == current`, version match, `isr ⊆ replicas`, `leader ∈ isr`; bumps `isr_version` |
| `ResignLeader{partition, epoch, node, info}` (6) | `epoch == current`, `node == leader`; result: `epoch + 1`, `leader = 0`, `isr` minus `node` when `|isr| > 1`, `isr_version + 1` |

An election can only shrink the ISR; only `AlterIsr` grows it. **There is no unclean election**,
anywhere. When no ISR member is alive the partition goes offline (`leader = 0`) and keeps its
last ISR member, so it comes back cleanly when that node returns. `SetPartitionLeader` is never
proposed by cluster code (grep gate).

**Taking over.** A node elected at epoch `e` leads only if `e` is above its log's latest epoch.
It applies everything up to its HW, appends `LeaderAnnounced{e}` and waits for it to commit and
apply, then starts the leader services (dispatcher, timers, uploader). In-flight runs carry the
old epoch's tokens and are re-granted at once (no `lease_ttl` wait).

**Offline semantics.** A partition with `leader = 0` refuses partition-scoped requests with 503
and no leader hint. A leader that has not heard from the meta leader for `session_timeout`
masks its led partitions as leaderless (self-demotion, `ostro_repl_self_demotions_total`), so
its clients fail fast; safety never depends on this, because a cut-off leader cannot advance
its HW without the ISR, and the ISR can change only through meta. A takeover cut short by
self-demotion (still announcing, `LeaderAnnounced{e}` not yet logged) never resumes at `e`: when
contact returns and meta still names it at `e`, the node asks meta for a fresh term above `e`
and takes over at that one.

**Hot standby.** Followers apply committed entries to their own state store, take snapshots and
trim their own log, so a new leader does not replay from scratch.

**Snapshot bootstrap.** An empty or trimmed-past follower gets `SNAPSHOT_REQUIRED`, streams the
leader's newest OSNP v1 snapshot (`Replication.FetchSnapshot`, resumable per frame), verifies
it, resets its log, installs the state and fetches from the snapshot offset. The fixed order and
its crash analysis are in `docs/snapshot-format.md` § "Phase 12 follower install".

## 2. Static cluster setup

Every node needs the same `cluster_id`, the same CA (certificate and key) and the list of the
other voters. Each node issues its own node certificate from the shared CA at startup (node SAN
URI with its `node_id`, plus `[cluster] advertise` names and loopback).

```toml
[node]
node_id = 1                      # non-zero, unique; bound into node.toml at first boot

[cluster]
listen = "0.0.0.0:7701"          # cluster listener: workers, MetaPeer, Replication (mTLS)
advertise = ["10.0.0.11"]        # names in the node certificate and in the Member record
cluster_id = "prod-eu-1"         # required with peers
# replication_listen = "0.0.0.0:7702"   # optional second listener serving only Replication

[cluster.tls]
ca_cert_file = "/etc/ostro/cluster-ca.pem"   # shared CA, required with peers
ca_key_file  = "/etc/ostro/cluster-ca.key"

[[cluster.peers]]
id = 2
addr = "10.0.0.12:7701"
# repl_addr = "10.0.0.12:7702"   # when that peer runs replication_listen

[[cluster.peers]]
id = 3
addr = "10.0.0.13:7701"

[replication]                    # all optional; defaults shown
session_timeout = "3s"           # liveness window and self-demotion
replica_lag_max = "10s"          # a follower not caught up this long leaves the ISR
meta_tick_interval = "100ms"
meta_election_tick = 10          # meta election timeout in ticks
meta_heartbeat_tick = 2
fetch_max_wait = "500ms"         # fetch long-poll idle wait
fetch_read_timeout = "5s"        # server-side bound of one fetch read
fetch_max_bytes = "8MiB"
# rf = 3                         # bootstrap seed: default min(3, voters)
# min_insync = 2                 # bootstrap seed: default 2 when rf >= 3, else 1
```

**Identity checks (D-P12-14, D-P12-16).** `node_id` and `cluster_id` are written to
`node.toml` at the first boot, before meta starts; a later mismatch is fatal. A non-blank meta
log whose voters differ from this node plus the configured peers is fatal ("grow via
Phase 13") **as long as no `Member` record exists**; once dynamic membership is in use, the
meta configuration is authoritative and drift from `[[cluster.peers]]` is logged once (§9). A wiped meta directory over existing partition data fail-stops with a remedy
message instead of bootstrapping a new cluster. After meta is ready the Raft voters must equal
the configured set and `ClusterInfo.cluster_id` must equal the bound id.

**Bootstrap placement.** On the first boot of a fresh cluster each partition (epoch 0 =
unplaced) gets `replicas` round-robin over the sorted voters, `leader = replicas[0]`,
`isr = [leader]`, `min_insync = min([replication] min_insync, rf)`, epoch 1. Followers join the
ISR through the normal expand rule once they have caught up. `rf` and `min_insync` are seeds:
changing them later does not re-place existing partitions.

**Existing single nodes.** A data directory that was already running as a single node (epoch
≥ 1, replicas `[n]`) is never re-placed when peers are added to its config. Grow it with
`ostro cluster init` and `ostro cluster join` instead (§9, `docs/cluster-operations.md`).

**Tuned defaults.** The 12-17 sweep (`target/failover-p99/sweep.txt`) ran
`meta_election_tick ∈ {10, 6, 4} × session_timeout ∈ {3 s, 2 s}`, 60 failover iterations plus a
10-minute loaded steady state each. Rule: the most conservative setting with p99 < 8 s and no
spurious meta term change. The shipped defaults (10 / 3 s) pass with p99 5.6 s and 0 term
changes, so they stay. Shorter timeouts fail over proportionally faster (4 / 2 s: p99 3.2 s);
`session_timeout = "2s"` is the next step on hosts with headroom.

## 3. Failover timing

Budget (SC4: < 10 s, runs resume). From the fault to the first acknowledged write on the new
leader:

1. **Detection.** A follower whose fetch fails with connection refused/reset reports the leader
   as suspect (`MetaPeer.ReportSuspect`). The controller confirms with a liveness sample
   `confirm_window` (`session_timeout / 4` = 750 ms) later: a node unheard since the report is
   dead. Without a report (a silent network cut) a node is dead after `session_timeout` of
   absence from the samples (taken every `session_timeout / 6` with a window of
   `session_timeout / 3`). A new controller gives every node a fresh grace period and suspects
   the ones absent from its first `session_timeout / 3` of samples.
2. **Metadata re-election** (only when the controller itself failed): about
   `meta_election_tick × meta_tick_interval` (1-2 s).
3. **`ElectLeader` apply** and the new leader's log leading (tens of ms).
4. **`LeaderAnnounced` commit** (one replication round).
5. **Catch-up and re-grant**: in-flight runs are re-dispatched at once.

Measured on WSL2 (advisory, D15; debug failpoint server build, 3 processes, RF 3,
min_insync 2, shipped defaults; every third fault hits the controller, a third of the faults
are no-RST blackholes). See `.planning/phases/12-replicated-partition-logs/12-VALIDATION.md`
for the gate run and `target/failover-p99/report.txt` for the per-iteration table.

12-18 gate run (200 iterations, after the 12-18 liveness fixes), with the 12-17 run for comparison:

| Fault | n | p50 | p99 | max | 12-17 p50 / p99 |
|-------|---|-----|-----|-----|-----------------|
| overall | 200 | 2.43 s | 5.44 s | 5.49 s | 2.47 s / 5.42 s |
| `kill -9`, non-controller leader | 89 | 0.89 s | 0.95 s | 0.95 s | 2.45 s / 2.50 s |
| `kill -9`, controller | 45 | 2.44 s | 3.46 s | 3.46 s | 2.46 s / 3.49 s |
| blackhole (no RST), non-controller | 44 | 4.10 s | 4.20 s | 4.20 s | 4.06 s / 4.23 s |
| blackhole (no RST), controller | 22 | 5.13 s | 5.49 s | 5.49 s | 4.80 s / 5.70 s |

Steps (12-18): detection p50 1.04 s / p99 4.05 s (suspect report confirmed in 89
iterations, new-controller seeding in 67, session-timeout sample in 44); metadata re-election
p50 1.18 s / p99 1.75 s; `ElectLeader` apply p99 55 ms; `LeaderAnnounced` commit p99 53 ms;
catch-up and re-grant p50 68 ms, p99 2.42 s (the blackholed-controller iterations, see "Cut-off
old leaders" below). Every pre-fault run resumed in every iteration. A controller kill is
dominated by the metadata re-election plus the new controller's seeding window
(`session_timeout / 3`) and confirmation; a blackhole is detected only by the session-timeout
sample.

The native-Linux run is human_needed (commands in 12-17-SUMMARY.md § "Reproduce on native
Linux").

**Cut-off old leaders.** A leader that is alive but cut off keeps believing it leads until it
self-demotes. A cut-off **meta leader** first has to lose its Raft leadership (check-quorum,
about one election timeout) before its meta contact age starts to grow, so it self-demotes
`session_timeout` after that: 4.6-5.2 s after the cut in the 12-18 investigation, while the new
partition leader served (and completed the in-flight run) from 2.9-3.4 s. In that window the old
leader answers reads from its last committed state, and a write sent to it hangs and is then
refused with 503 at the self-demotion; it never commits (the HW cannot advance without its
followers, and the ISR can only change through meta, which it cannot reach), and after the heal
it is on no replica's log. The failover harness's client follows the cut-off leader's self-hint
for reads, which is why the "catch-up and re-grant" step shows about 2.4 s for those iterations.
Phase 12 reads were not linearizable. Phase 13 serves partition reads under a leader lease and
meta reads behind a ReadIndex barrier (read-your-writes, `docs/cluster-operations.md` §6).

## 4. `GET /v1/admin/replication`

Server-local route (like `/metrics`; not part of the ControlPlane/gRPC parity). Everything in it
is measured by this node; nothing is assumed.

```json
{"node_id": 1,
 "meta": {"leader": 1, "term": 2, "voters": [1, 2, 3], "learners": []},
 "partitions": [{"partition": 0, "role": "leader", "epoch": 1, "leader": 1,
                 "hw": 12, "durable_end": 12, "published_isr": [1, 2], "isr": [1, 2],
                 "isr_version": 3, "min_insync": 2, "replicas": [1, 2, 3],
                 "followers": [{"replica": 2, "durable_leo": 12, "fetch_offset": 12,
                                "lag_ms": 40, "in_sync": true}],
                 "hw_publishes": 9, "max_envelopes_per_fetch": 0}]}
```

| Field | Meaning |
|-------|---------|
| `node_id` | this node |
| `meta.leader`, `meta.term` | the metadata Raft leader and term as this node sees them (0 = unknown) |
| `meta.voters`, `meta.learners` | Raft configuration (learners: Phase 13) |
| `partition` | partition id |
| `role` | `leader`, `follower`, `offline`, or `failed` (the partition is not running here) |
| `epoch` | the epoch of the local role |
| `leader` | meta's leader for the partition (0 = none) |
| `hw` | this replica's high watermark |
| `durable_end` | this replica's fsynced log end |
| `published_isr` / `isr` | the ISR meta published (`isr` repeats it) |
| `isr_version` | meta CAS token of the ISR |
| `min_insync` | the partition's minimum ISR size |
| `replicas` | the assigned replicas |
| `followers[]` (leader only) | per follower: `durable_leo` (reported, capped at `fetch_offset`), `fetch_offset`, `lag_ms` (since it was last caught up), `in_sync` (needs at least one fetch) |
| `hw_publishes` | HW publications through the tracker (> 0 on an RF=1 node: the one path) |
| `max_envelopes_per_fetch` | largest number of envelopes in one fetch response served (batching) |

## 5. Clients and indeterminate outcomes

A partition-scoped request on a node that does not lead the partition (follower, offline, taking
over, self-demoted) returns `UNAVAILABLE` (HTTP 503, gRPC `UNAVAILABLE`) with details
`{"reason": "not_leader", "partition": p, "leader": n | null, "leader_addr": "host:port" | null}`.
Since Phase 13 (API-08) a client never sees `not_leader` for a routable request: any node
forwards it to the leader (one hop) and answers 503 `Retry-After: 1` only when the re-route
fails (`docs/cluster-operations.md` §5). Acknowledgements pending at a demotion still fail with
this error or the retryable `leader_changed` (handoff write fence).

A request that was in flight when the leader failed has an **indeterminate outcome**: it may or
may not have committed (research OQ5). Retrying is safe:

- `StartRun` with an **idempotency key** returns the existing run if the first attempt committed;
- a signal with a **`request_id`** is applied at most once;
- step attempts are fenced by **`(epoch, attempt)`**: a result from an old leader's attempt is
  rejected after a re-grant, so a retried activity never double-commits its journal entry.

Clients should follow the leader hint, or rotate through the nodes when there is none (or when
a node hints at itself: a cut-off old leader may still see itself as leader).

## 6. Upgrades and downgrades

Phase 12 appends MetaCommand variants 4-6 and ext tag 80; it changes no existing apply
semantics (D-P12-17, P13-F1). **Downgrading a data directory from a Phase 12 binary to a
pre-Phase-12 binary is unsupported**: the older binary would reject-and-continue on
`ElectLeader`. From Phase 13 on, the feature-level gate, fail-stop on unknown commands and the
boot / running fences make N/N−1 rolling upgrades safe: `docs/compat.md`,
`docs/upgrade-runbook.md`.

## 7. Bottomless and restore in a cluster

Only a partition's leader uploads its chunks, still capped at the HW. Each partition has an
epoch-fenced head object `p/<pppp>/head`; a new leader repairs a divergent remote tail before its
first upload. `LOCK` and `manifest/current` are written only while the cluster has exactly one
member. In a multi-member cluster the metadata leader also uploads metadata snapshots and moves
a term-fenced `meta/head` pointer (14-29).

**Whole-cluster restore** (14-30, D-P14-45; replaces the D-P12-3 refusal): `ostro restore` on
a cluster bucket replays every partition's chain, finds the greatest cut that keeps
cross-partition causality (Pitfall 1 for every pair of partitions: no partition holds a
message its sender has not sent at its cut, and no sender dropped outbox rows the receiver
has not seen), restores every partition there, and rewrites the newest metadata snapshot that
covers the open runs to a single seed member (old members `Removed`, every assignment on the
seed at an epoch above every restored epoch). The cluster then grows back with `ostro
cluster join`. The RPO per partition is `head end - cut`. `--at` is refused for cluster
buckets. Runbook: [`docs/cluster-restore.md`](cluster-restore.md); bucket layout:
`docs/bottomless.md` § "Cluster mode".

## 8. Tests

| Tier | What | Where |
|------|------|-------|
| A | pure model: proptest state machine against the replica algorithms | `crates/ostro-log/tests/replica_model.rs` |
| B | in-process clusters of N servers over an in-memory tonic transport with a seeded fault layer (drop, delay, blackhole, cut per link and RPC) | `crates/ostro-repl-conformance` scenario groups |
| C | spawned failpoint `ostro server` processes on `127.r.r.{1..n}` behind a TCP chaos proxy, real `kill -9` | `process_*`, `failover_p99`, `bench_rf3`, `cluster_campaign` (all `#[ignore]`) |

Scenarios are library functions with one runner: a hard deadline, a measured `cluster_formed`
precondition, and failures only as `ScenarioFailure{oracle, detail}` (timeouts and panics are
harness errors, never accepted as evidence). Oracles read bytes on disk, state digests, the full
meta log (rejected proposals included) and the bucket.

```bash
# tier B, one group
cargo test -p ostro-repl-conformance --locked --test failover -- --test-threads=1
# all tier-B scenarios in one table
OSTRO_REPL_RED_RUN=1 cargo test -p ostro-repl-conformance --locked --test red_run -- --ignored --nocapture --test-threads=1
# the mutant matrix: every mutant must turn its scenarios red (CI requires all of them)
OSTRO_REPL_REQUIRE_ALL=1 cargo test -p ostro-repl-conformance --features failpoints --locked --test mutants -- --test-threads=1 --nocapture
# tier C (needs the failpoint server binary)
cargo build -p ostro --features failpoints --locked
export OSTRO_FAILPOINT_BIN=$PWD/target/debug/ostro
OSTRO_PROC_ITERS=10 cargo test --release -p ostro-repl-conformance --locked \
  --test process_divergence --test process_power_loss --test process_mutants -- --ignored --test-threads=1 --nocapture
OSTRO_FAILOVER_ITERS=200 OSTRO_FAILOVER_ASSERT=1 cargo test --release -p ostro-repl-conformance --locked \
  --test failover_p99 -- --ignored --nocapture --test-threads=1 --exact failover_p99
OSTRO_CAMPAIGN_FAULTS=10000 cargo test --release -p ostro-repl-conformance --locked \
  --test cluster_campaign -- --ignored --nocapture --test-threads=1
```

**Red run.** Before any replication code was wired in (12-06, 12-07), every RF > 1 scenario was
red with a measured failure and RF = 1 was green; `OSTRO_REPL_RED_RUN=1` makes a scenario still
listed as pending fail the run, so that evidence stays reproducible. The pending lists are empty
since 12-16.

**Mutant matrix.** Ten permanent failpoint mutants (`repl::mutant::*` in `ostro_log::mutants`)
each break one rule: `isr_from_assignment`, `hw_leader_leo`, `count_nondurable_leo`,
`skip_epoch_check`, `no_truncate`, `unclean_election`, `ignore_min_insync`,
`shrink_before_commit`, `upload_above_hw`, `rf1_bypass`. `tests/mutants.rs` runs each mutant's
scenarios without it (must pass) and with it (must fail on the target oracle): 11 tier-B pairs.
`count_nondurable_leo` bites on tier C (`process_mutants`). With `OSTRO_REPL_REQUIRE_ALL=1` (set
by `scripts/ci-local.sh` and CI) a skipped or non-biting mutant fails the build.

### 8.1 Phase 13 tests

Tier B adds `P13Harness` (single-node start, `add_node` through the real join flow, `remove`,
`drain`, `upgrade(node, level)`) and the per-plan scenario files (`membership`, `reassign`,
`handoff`, `routing`, `ryw`, `version_gate`, `module_gc`, `migrate`, `promotion`,
`integration_p13`, `regressions_p13`, `cross_node_children`). The Phase 13 mutant matrix
(`tests/mutants_p13.rs`, eight mutants, see `docs/compat.md` §9) runs only when
`OSTRO_P13_REQUIRE_ALL=1` (CI) or `OSTRO_P13_MUTANT=<name>` (one row) is set.

Tier C (13-19), all `#[ignore]`, release mode, `--test-threads=1`, failpoint binary
(`cargo build -p ostro --features failpoints --target-dir target/failpoints`):
`process_promotion`, `process_rolling_upgrade`, `process_ryw`, `process_downgrade`,
`process_shutdown`, `upgrade_from_prev`, `cluster_campaign_p13`. CI runs them in the step
"Phase 13 tier C (multi-process)" with three upgrade iterations and a 50-fault campaign; the
gate campaign (≥ 10,000 faults, 4 h time box) is run by hand.

| variable | default | meaning |
|---|---|---|
| `OSTRO_P13_REPORT_DIR` | `/tmp/ostro-p13-reports` (absolute) | each test writes `<dir>/<test>/report.txt` |
| `OSTRO_UPGRADE_ITERS` | 1 (gate and CI: 3) | rolling upgrade iterations |
| `OSTRO_CAMPAIGN_FAULTS` | 50 (smoke ≥ 500, gate ≥ 10,000) | Phase 13 campaign faults |
| `OSTRO_CAMPAIGN_TIMEBOX_S` | 14400 (4 h) | no new fault after this; the report carries `HUMAN_NEEDED` |
| `OSTRO_SHUTDOWN_REPEATS` | 10 | `process_shutdown` SIGTERMs |
| `OSTRO_PREV_BIN` | unset (SKIP) | previous release binary for a real N−1 upgrade (human_needed until the first release tag) |
| `OSTRO_P13_CHILD_RUNS` | on | `0` leaves cross-partition child runs out of the workload (a diagnostic switch; 13-19 finding 1 was fixed in d33fce7 and CI runs with child runs on); every report names the mode |
| `OSTRO_TEST_LEVEL_MAX` | unset | level emulation (`docs/compat.md` §7) |
| `OSTRO_FAILPOINT_BIN`, `OSTRO_PROC_ROOT`, `OSTRO_PROC_KEEP` | — | existing harness variables (binary path, process root, keep dirs) |

The static-cluster tier-C tests (rolling upgrade, RYW, downgrade, shutdown, campaign) start at
`OSTRO_TEST_LEVEL_MAX=1` so chaos-proxy faults reach every link (D-13-19-1); joined nodes dial
peers directly, so proxy faults do not reach their links yet (open).

## 9. Phase 13: dynamic membership, reassignment, handoff

- **Membership is meta-driven.** Each node has a `Member` record (ext 96 `NodeVersion`,
  ext 97 `MemberState`). Once any member record exists, `[[cluster.peers]]` is a seed list only:
  configured addresses stay pinned (drift logged once), ids that are not configured follow their
  member record. A learner-first reconciler on the meta leader grows and shrinks the voter set
  with joint `ConfChangeV2`s (odd voter counts, `max_voters` = 5). Joining uses a node join token
  and the CA-hash pin; removal leaves a tombstone forever. See `docs/cluster-operations.md`
  §1-2.
- **`AlterReplicas` (MetaCommand 8).** Replica-set changes are one meta command CAS'd on
  `isr_version` (the frozen field name now covers replica-set changes too). Apply requires
  `epoch == current`, `isr_version == expected`, `leader ∈ replicas` (unless offline),
  `isr ⊆ replicas`, `leader ∈ isr` (unless 0) and `1 ≤ min_insync ≤ |isr|`; it bumps
  `isr_version` and writes ext 100 (the in-flight reassignment) from the command. Epoch-0
  partitions are refused. The controller drives Expand → CatchUp → LeaderMove → Shrink
  (`docs/cluster-operations.md` §3). Gated at level 2.
- **Graceful handoff.** A leader moves a partition to an ISR member by quiescing it, fencing
  submissions with `leader_changed`, waiting until the target's durable LEO equals its own, then
  `ElectLeader{epoch + 1}` with reason RESIGN; handoff elections keep the ISR unchanged.
  SIGTERM in cluster mode hands off every led partition first (controlled shutdown). Known
  limitation: followed partitions stall for about a session timeout because the leaving node is
  not removed from their ISR first (deferred; `docs/cluster-operations.md` §4).
- **Routing and RYW.** Any node accepts any request (one hop); partition reads are served under
  a leader lease, meta reads behind ReadIndex (`docs/cluster-operations.md` §5-6). Outbox
  batches for another leader's partition (children, run-to-run messages) are delivered with
  `NodeOps.DeliverInbound`; keyed restarts start on the target leader through
  `NodeOps.StartRestart`.
- **Promotion.** `ostro cluster init` turns a running single node into a cluster online; RF
  rises to the policy as nodes join (13-19: time-to-RF-3 3.8 s after the first join).

