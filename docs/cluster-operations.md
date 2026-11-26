# Cluster operations (Phase 13)

This is the operator reference for running Ostro as a cluster: forming and growing it,
removing and draining nodes, moving replicas, controlled shutdown, request routing,
read-your-writes, and the status endpoint. Upgrades are covered in
[upgrade-runbook.md](upgrade-runbook.md) and the compatibility contract in
[compat.md](compat.md). The replication model itself (partition logs, ISR, failover) is in
[replication.md](replication.md).

Requirements covered: CLUS-05 (membership), API-08 (routing and read-your-writes). Design
records: D-P13-8..16 and D-P13-29 in `.planning/phases/13-cluster-membership-routing-upgrades/13-DECISIONS.md`.

## 1. Forming and growing a cluster

Membership lives in the metadata Raft group (meta). Each node has a `Member` record whose
state is `joining`, `active`, `draining` or `removed`. Once any member record exists,
`[[cluster.peers]]` is only a seed list (P13-F4); the meta configuration is authoritative.
A Phase 12 static cluster that never joins or drains a node stays exactly as configured.

### 1.1 `ostro cluster init`: promote a running single node

```
ostro cluster init --advertise-cluster 10.0.0.11:7701 [--rf 3] [--min-insync 2]
```

- This is an online promotion: no restart, no export. It works only in the server profile
  and answers 409 `not_supported_in_profile` under `ostro dev` / `--ephemeral`.
- `--advertise-cluster` is the address peers dial. The default is the first
  `[cluster] advertise` entry, with the listener port added when the entry names only a host.
  `--advertise-api` and `--advertise-repl` are usage errors for `init`. A promoted node's API
  address is its `[api] listen`, and its replication listener is `[cluster] replication_listen`.
- The steps, in order: the node's own `Member` record (Active, NodeVersion ext 96); the cluster
  CA stored as the sealed secret `__ostro/cluster-ca`; `ClusterInfo` ext 98 (finalized level =
  this binary's `level_max`); and last, the replication policy (ext 99,
  `{rf: 3, min_insync: 2, max_voters: 5}` by default).
- It is idempotent. On a node that is already a cluster (a policy exists, or more than one
  non-removed member), it answers 200 with the status and writes nothing.
- No restart and no certificate swap is needed. Node-to-node mTLS identifies nodes by a URI
  SAN, not by hostname. `init` warns when the advertised host is not covered by the node
  certificate's SANs.
- Node 1 keeps leading every partition throughout. The effective RF is min(rf, active
  members), so RF rises through reassignment as nodes join (section 3). `min_insync` rises
  only in the shrink step and is capped at |ISR|.

### 1.2 `ostro cluster token create --node` and `ostro cluster join`

On any member:

```
ostro cluster token create --node [--ttl 1h]
```

This prints the token id, its expiry and the complete join line (shown once; store the token
as a secret):

```
ostro cluster join https://10.0.0.11:7700 --token njt_... --ca-hash sha256:<hex>
```

Run that line on a blank node, optionally with `--data DIR`, `-c config.toml`,
`--advertise-api`, `--advertise-cluster`, `--advertise-repl`, and `--no-start` to only prepare
the data directory.

- The joiner pins the cluster CA by its hash (D-W20), then calls `POST /v1/cluster/join` on the
  seed's client listener. Any member can serve the call. It checks the `node:join` token,
  allocates the next node id (ids are never reused; tombstones count), signs the joiner's CSR
  with the cluster CA, and returns the cluster id, the seeds and the finalized level.
- The joiner refuses when the finalized level is outside the levels its binary supports.
- The joiner then writes, in this order: the join marker `<data>/cluster-seeds.toml`; the
  joined certificate set under `<data>/tls/joined/`; and `node.toml`. A crash between steps
  resumes cleanly. Re-running the same join line reuses the marker's node id. If that id was
  tombstoned and the data dir holds no data, join discards the marker and takes a new id.
- A joiner starts meta with no initial voters and never forms a cluster of its own. It fails
  loudly after `[cluster] join_timeout` (120 s) without a meta leader.
- A joining node becomes a meta **learner** first. It is promoted to voter only when it is
  caught up (within `promote_lag` = 64 entries of the commit index, heard from, no snapshot
  pending).

**Voter policy.** The desired voter count is the largest odd number ≤ min(live active members,
`max_voters` = 5). Growth 1 → 3 → 5 and shrink 5 → 3 → 1 are single joint configuration changes.
Every stable configuration therefore has an odd voter count, and growth waits until two
learners are caught up. Other members stay learners: they serve partitions and requests and
replicate meta, but do not vote. A dead voter keeps counting, so a transient failure never
shrinks the meta group. A dead voter is replaced only by a caught-up live learner.

Measured (13-19, tier C, native Linux, under load): join ready 443–532 ms; time-to-RF-3 from
the first join 3.8 s (two joins ≈ 1 s, then RF 3 / ISR 3 / min_insync 2 with 3 voters after
2.8 s); after `kill -9` of the original node, every partition was led by a survivor after 1.4 s.

## 2. Removing and draining

```
ostro cluster drain <id>           # move replicas and leaderships away, keep the member
ostro cluster remove <id> [--force]
```

- **drain** marks the member `draining`. The placement controller moves its replicas and
  leaderships to active members, and its workers get `GoAway{reason: "node_draining"}`. New
  worker sessions on that node are refused the same way.
- **remove** writes a `removed` tombstone, which is kept forever, and takes the node out of
  the meta configuration. It is refused while the node still holds replicas.
  `--force` skips only the wait for a **dead** node's replicas to move; the controller
  re-places them afterwards because removed members are excluded from placement.
- **Quorum refusal.** A voter is removed only if the remaining live voters form a quorum of the
  new configuration. This check is never skipped, `--force` included. When the meta leader
  itself is removed, it transfers leadership first.
- A removed node is told to stop (`Membership.Removed`). It also stops on its own after
  3 × `session_timeout` without leader contact while its last-known state is removed. A node
  whose own id is removed refuses to start.
- **Orphan joiners.** A `joining` member with no join call and no contact for
  `[cluster] join_orphan_timeout` (25 h, at least `[workers] join_token_ttl`) is tombstoned.
- **Wiped node remedy.** A voter whose meta directory was wiped over existing partition data
  fail-stops with: "Remove it with `ostro cluster remove <id> --force`, wipe its data dir, and
  add it back with `ostro cluster join` (it gets a new node id)."

## 3. Reassignment and rebalance

```
ostro cluster reassign --partition 3 --to 1,2,4
ostro cluster reassign --partition 3 --cancel
ostro cluster rebalance
```

Reassignment follows the KIP-455 shape (D-P13-12). Each step is one `AlterReplicas` meta
command (MetaCommand 8), CAS'd on the partition's `isr_version`. The in-flight state is ext
100 on the assignment, so a meta failover resumes it.

1. **Expand:** replicas = old ∪ target.
2. **CatchUp:** the new replicas join the ISR through the Phase 12 expand rule.
3. **LeaderMove:** a graceful handoff (section 4) when the leader is not in the target.
4. **Shrink:** replicas = target, ISR = ISR ∩ target. This happens only once the target ⊆ the
   published ISR. Then `min_insync = min(max(current, min(policy, |target|)), |target|)`, which
   is never below the policy.

- `--cancel` is allowed before LeaderMove.
- At most `[cluster] max_concurrent_moves` (2) partitions move at once.
- The preferred-leader balancer runs on the meta leader every
  `[cluster] leader_rebalance_interval` (5 min) and moves one leadership at a time.
  `ostro cluster rebalance` plans replica moves and leader moves now.
- Unplaced (epoch-0) partitions are never touched (`AlterReplicas` at epoch 0 is invalid).
- A removed replica keeps its data inert: offline, not fetched, never served. A re-added
  replica truncates through divergence or installs a snapshot.
- Without a replication policy (a Phase 12 static cluster that never ran `init`), the
  controller only evacuates draining and removed members. It never rebalances on its own.
  Explicit `reassign` and `rebalance` still work.
- Reassignment and rebalance need cluster level 2. Before finalization they answer 409
  `feature_not_enabled` (see [compat.md](compat.md)).

Measured (13-19, rolling upgrade): `reassign p0 [1,2,3] → [1,2] → [1,2,3]` took 110–435 ms
per step.

**Partition-count skew (P13-F7).** A promoted cluster keeps P = 16 partitions. After the joins,
node 1 still leads every partition until the leader balancer moves them, one per
`leader_rebalance_interval`, so a fresh three-node cluster is leader-skewed for up to about an
hour at the defaults. Run `ostro cluster rebalance` to start at once. With 16 partitions over
3 nodes, the balanced spread is 6 / 5 / 5 leaders. Partition counts are fixed at creation.

## 4. Graceful handoff and controlled shutdown

A handoff moves partition p from leader A to B, where B is in the published ISR (D-P13-13):

1. A turns admission off for p and gives in-flight activations up to `handoff_grace` (2 s) to
   reach a flush or suspend point. Hot-waiting runs are evicted.
2. A fences new submissions to p with the retryable `leader_changed`. Clients and the router
   retry it exactly like `not_leader`.
3. A waits until B's durable log end equals its own (`handoff_catchup_timeout`, 10 s; abort
   after 2 s without progress).
4. `ElectLeader{epoch + 1, leader: B}` goes through meta with election reason RESIGN, and A
   becomes a follower.

No run is drained, cancelled or failed. Unfinished activations replay on B. On abort (B left
the ISR, timeout, no meta quorum), A turns admission back on and keeps serving. Partition reads
are refused on A from the start of the handoff.

**Controlled shutdown.** SIGTERM in cluster mode, or `ostro cluster drain`, does three
things: it hands off every led partition (targets: ISR members in replica order), transfers
meta leadership, then runs the normal shutdown. The whole sequence is bounded by
`shutdown_handoff_timeout` (30 s). An isolated node skips the handoffs at once, because a 1 s
ReadIndex probe finds no meta quorum.

Measured (13-19, tier C, native Linux, under load; `process_shutdown`, 10 SIGTERMs of the
busiest leader, and `process_rolling_upgrade` ×3):

| measurement | p50 | p99 | n |
|---|---|---|---|
| SIGTERM → process exit (`process_shutdown`) | 180 ms | 207 ms | 10 |
| restart → ready line | 442 ms | 555 ms | 10 |
| unavailability of the partitions the stopped node **led** | 565 ms | 677 ms | 23 |
| unavailability of the partitions it only **followed** | 568 ms | 670 ms | 17 |
| SIGTERM → exit during a rolling upgrade (20 ms link delay) | 702 ms | 873 ms | 9 |
| handoff unavailability per partition per restart (rolling upgrade) | 277 ms | 1.72 s | 36 |

Every partition the stopped node led was handed off (reason RESIGN) before it exited.

**Known limitation: ISR stall on controlled shutdown (deferred).** The stopping node is not
removed from the ISR of the partitions it only follows before it exits. Those partitions
therefore stall for about one session timeout, until the Phase 12 ISR shrink drops the
departed follower. The table above shows it: the followed partitions are unavailable as long as
the handed-off ones. A fix needs a new "leaving node" signal and a rule in the 12-04 two-phase
ISR protocol. It is a candidate for Phase 14 or a follow-up plan.

## 5. Routing (API-08)

Any node accepts any request (D-P13-14). REST, gRPC, the UI and every in-process consumer
(triggers, cron, bulk jobs, on-failure handlers) go through the routed control plane:

- **Keyed calls** go to the leader of the key's partition. **Run-id calls** go to the leader
  of the run's partition. The call is forwarded over the mTLS cluster listener, which serves
  `ostro.api.v1.*` to node identities only.
- **Hop limit.** A forwarded call carries `ostro-hop: 1` and is never forwarded again. On
  `not_leader`, the router waits for local meta to reach the hinted epoch and re-routes once.
  If that fails, the client gets **503 with `Retry-After: 1`** (gRPC `UNAVAILABLE`), details
  `retry_after_ms`.
- **Offline partitions.** A partition with no leader is an immediate 503 `partition_offline`
  (details `partitions`, `retry_after_ms`). A list that fans out over such a partition answers
  the same 503 rather than an incomplete page.
- **Unkeyed starts** (no idempotency key, no run id) follow `[api] unkeyed_start_placement`.
  `local` (default) uses a partition this node leads (zero hops), or a uniform partition when
  it leads none. `uniform` always picks a uniform partition.
- `list_runs` fans out with `ostro-partitions`. The remaining deadline travels as
  `grpc-timeout`. The REST deadline header is `ostro-deadline-ms`.
- The `ostro-partition`, `ostro-partitions` and `ostro-principal` headers are honoured only
  from node identities (a client cannot pin a partition). `ostro-principal` is the Phase 14
  hook for scoped tokens on forwarded calls.
- Node-local registries (workers, bulk jobs) are found through a fan-out lookup. Meta writes
  are forwarded to the meta leader by `MetaHandle`.
- **Keyed restarts** (DLQ restart, bulk restart, keyed rerun, DLQ requeue in rerun mode) build
  the new run on the source run's leader. The dedupe key fixes the target partition. When
  another node leads that partition, the source node calls `NodeOps.StartRestart` on that leader
  with the frozen `StartRun` bytes and every payload inline. Metric:
  `ostro_route_keyed_restarts_total{path}`. Unkeyed restarts (retry `Restarted`, rerun without a
  key) land on a partition this node leads.
- **Cross-node children and run-to-run messages.** Outbox batches (`StartChild`,
  `ChildStarted` / `ChildCompleted`, signals between runs) whose target partition another node
  leads are delivered with `NodeOps.DeliverInbound`, bounded at 10 s. Exactly-once is unchanged:
  the target dedupes `(source, seq)` against its replicated `inbound_seen` whichever path
  delivered, and the shuffler truncates only after Accepted / Duplicate. Metric:
  `ostro_route_inbound_forwards_total{result}`. Every Phase 13 node-to-node service
  (`DeliverInbound`, `HandOff`, `StartRestart`, ...) requires the caller's node identity from the
  mTLS peer certificate (d33fce7 fixed the late-bound routes, which answered UNAUTHENTICATED and
  lost child runs under restarts, 13-19 finding 1).

## 6. Read-your-writes (D-P13-15)

A write acknowledged by any node is visible to a later read through any node.

- **Partition reads** are served only under a **leader lease**. The node must be the leader,
  its `LeaderAnnounced` must be applied, and the age of its meta lease anchor must be below
  `session_timeout × (1 − lease_margin)` (`[api] lease_margin` = 0.2, on monotonic clocks).
  On a meta follower, the anchor is a ReadIndex round trip through the meta leader. On the
  meta leader, it is the latest heartbeat round acked by a quorum (on a single voter: now).
  The controller elects a failover leader only after the old leader's lease must have expired.
- **Meta reads** pass a Raft ReadIndex barrier: `read_index`, then wait until local applied ≥
  index, bounded by min(deadline, 2 × `session_timeout`).
- **Consistency tokens (optional).** Write responses carry `ostro-consistency: p<P>:<epoch>:<offset>`
  (partition) or `m:<index>` (meta). A read with `ostro-read-after: <token>` skips the barrier
  when local state already covers the token. Otherwise it takes the barrier and waits for the
  apply. Correctness never depends on clients echoing tokens. A start passes the meta barrier
  only when it carries `ostro-read-after m:<index>` (D-13-18-2).
- **`[api] strict_reads = true`** (default false) makes partition leaders confirm with a meta
  `read_index` plus an epoch check before every read.
- **Long-poll watchers** (`await_result`, run watchers) follow leadership per term. On demotion,
  waiters get `LeaderChanged` and the router re-routes with the remaining deadline.

Measured cost (13-13 tier B, in-process, so a barrier is a loopback Raft round): lease mode
read p50 3.0–3.6 ms / p99 12.7–14.3 ms; `strict_reads` read p50 3.2 ms / p99 9.4 ms, within
noise of lease mode. Tier C (13-19, native Linux, three processes, 9,000 ops through a SIGTERM
leader move, a `kill -9` failover and a proxy-isolated zombie): 0 RYW violations; read p50
4.8 ms / p99 22–28 ms, write p50 10.3 ms / p99 16 ms in lease mode. On a real network, every
strict read pays one meta round trip to the meta leader on top of that.

User review (2026-11-26): lease by default, `strict_reads` opt-in, confirmed.

## 7. Status: `GET /v1/cluster/status` / `ostro cluster status`

| field | meaning |
|---|---|
| `cluster_id` | the cluster id |
| `members[]` | every member, tombstones included: `node_id`, `addr` (client API), `cluster_addr`, `state` (`joining` / `active` / `draining` / `removed`), `version` (reported binary and levels), `live` (meta leader's liveness view), `raft_role` (`voter` / `learner` / `outgoing` / `none`) |
| `voters`, `learners`, `voters_outgoing` | the meta Raft configuration; `voters_outgoing` is non-empty while joint |
| `meta_leader` | meta leader id (0 = unknown) |
| `finalized`, `hold` | finalized feature level and operator hold (see [compat.md](compat.md)) |
| `level_min`, `level_max` | lowest `level_min` / `level_max` over members |
| `policy` | replication policy `{rf, min_insync, max_voters}` (ext 99), absent on a static cluster |
| `reassignments[]` | in-flight reassignments: `partition`, `target`, `adding`, `removing`, `started_at_ms` |

`GET /v1/admin/replication` (Phase 12) stays server-local and shows per-partition replication
state on the node asked.

## 8. Configuration reference (D-P13-23, defaults D-P13-26)

| key | default | meaning |
|---|---|---|
| `[cluster] advertise` | `[]` | Phase 7 list of advertised names/addresses (node cert SANs, worker endpoints); `init` uses the first entry as the cluster address unless `--advertise-cluster` is given |
| `[cluster] auto_finalize` | `true` | finalize the feature level once every member supports it ([compat.md](compat.md)) |
| `[cluster] join_timeout` | `120s` | a joiner fails when meta has no leader after this long |
| `[cluster] join_orphan_timeout` | `25h` | a `joining` member without contact is tombstoned after this long (≥ `[workers] join_token_ttl`) |
| `[cluster] promote_lag` | `64` | a learner is promoted when within this many entries of the commit index |
| `[cluster] handoff_grace` | `2s` | handoff: wait for in-flight activations |
| `[cluster] handoff_catchup_timeout` | `10s` | handoff: wait for the target to catch up |
| `[cluster] shutdown_handoff_timeout` | `30s` | controlled shutdown bound |
| `[cluster] leader_rebalance_interval` | `5m` | preferred-leader balancing period |
| `[cluster] max_concurrent_moves` | `2` | concurrent reassignments (≥ 1) |
| `[cluster] meta_max_message_bytes` | `256 MiB` | MetaPeer gRPC message limit (≥ 4 MiB) |
| `[api] unkeyed_start_placement` | `local` | `local` or `uniform` (section 5) |
| `[api] strict_reads` | `false` | barrier + epoch check before every partition read |
| `[api] lease_margin` | `0.2` | lease safety margin as a fraction of `session_timeout` (0 < m < 0.5) |
| `[modules] gc_interval` | `10m` | module GC period ([upgrade-runbook.md](upgrade-runbook.md)) |
| `[modules] gc_grace` | `10m` | a retired module is a GC candidate after this long |
| `[modules] gc_protect_resumable` | `true` | runs that can still be retried keep their module |
| `[workers] version_policy` | `compatible` | worker admission ([compat.md](compat.md)) |

## 9. Metrics

| metric | what |
|---|---|
| `ostro_cluster_voters`, `ostro_cluster_learners` | meta configuration size |
| `ostro_cluster_conf_changes_total` | meta configuration changes proposed by the reconciler |
| `ostro_cluster_moves_total`, `ostro_cluster_reassignments_in_flight`, `ostro_cluster_reassignments_blocked` | placement controller |
| `ostro_cluster_meta_snapshot_bytes` | meta snapshot size |
| `ostro_cluster_prewarm_modules_total{result}` | AOT pre-warm after a restart ([upgrade-runbook.md](upgrade-runbook.md)) |
| `ostro_route_requests_total`, `ostro_route_forward_seconds` | routing and forwarding |
| `ostro_route_lease_rejections_total`, `ostro_route_read_barrier_seconds` | read-your-writes |
| `ostro_route_keyed_restarts_total{path}` | keyed restarts started on another leader |
| `ostro_route_inbound_forwards_total{result}` | cross-node child / message delivery |
| `ostro_module_gc_deleted_total`, `ostro_module_gc_kept_total` | module GC rounds |
| `ostro_migrate_runs_total{outcome}` | `migrate-runs` outcomes, incl. `already_on_target` |

See also [metrics.md](metrics.md).
