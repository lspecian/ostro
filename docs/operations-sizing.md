# Operations and sizing

This guide collects what you need to size and run Ostro nodes: partitions, memory per workflow
language, disk per step and for the audit journal, snapshots and backups, restore, upgrades,
file descriptors and observability. It summarises and links the detailed references; numbers
come from the measurements cited in each section. Day-to-day run operations (rerun, DLQ, bulk
jobs) are in [operations.md](operations.md), cluster membership in
[cluster-operations.md](cluster-operations.md).

> **Measurements are advisory.** The numbers below were measured on WSL2 or on a shared, loaded
> CI machine (dell-4), not on the native reference box. They show orders of magnitude and
> ratios between languages; measure your own workload before you commit to a size
> ([benchmarks.md](benchmarks.md)).

## Partitions (P)

Each node's data is split into `P` partitions (`[node] partitions`, default **16**), each with
its own write-ahead log, state store and single writer. `P` is fixed when the node (or cluster)
first starts and cannot change afterwards.

- A partition is the unit of leadership and of failover in a cluster: a node leads some
  partitions and replicates others ([replication.md](replication.md)). With 3 nodes and `P = 16`
  the nodes lead 5 or 6 partitions each.
- More partitions spread leadership and write load more evenly across nodes; each partition
  costs a WAL, a state store, file descriptors and a share of the snapshot and backup work.
- Tenant quotas are split across nodes by the partitions each one leads
  ([tenancy-and-auth.md](tenancy-and-auth.md#cluster-wide-quotas-the-per-node-leadership-share)):
  a larger `P` makes that split finer.

The default of 16 suits a single node and clusters of up to five nodes.

## Memory per workflow language

A workflow activation runs in a pooled Wasm instance. Its cost depends on the language
([sdk-resources.md](sdk-resources.md), Phase 9 measurements):

| | Rust | Python | TypeScript (QuickJS) | TypeScript (StarlingMonkey) |
|---|---|---|---|---|
| module size (`.wasm`) | 0.29 MB | 22.04 MB | 2.31 MB | 13.21 MB |
| compiled (`.cwasm`) / compile time | 1.01 MB / 36 ms | 43.66 MB / 1,088 ms | 6.34 MB / 397 ms | 43.33 MB / 1,578 ms |
| start to first step (p50) | 24 µs | 3,176 µs | 621 µs | 1,452 µs |
| memory charged per live instance (`node_memory_budget`) | 1.06 MiB | 15.94 MiB | 2.13 MiB | 9.06 MiB |
| private memory per parked instance | 55 KB | 1,691 KB | 427 KB | 689 KB |
| RSS per parked instance | 61 KB | 10,936 KB | 1,145 KB | 2,877 KB |

`ostro init --lang ts` builds for QuickJS. For physical memory, plan **private memory ×
instances + one copy of the module image**; RSS overstates shared pages, but container limits
count them once.

**Sleeping runs hold no memory.** A run that waits for something not imminent is evicted and
costs nothing until it wakes; only short timer waits (within `[engine] hot_wait`) and in-flight
effects may keep an instance parked hot, within the memory budget below. Signal and token waits
always evict ([workflow-semantics.md](workflow-semantics.md#13-sleeping-runs-hold-no-memory)).
Only activations that are running or parked hot count.

### The instance budget, especially for Python

Two limits decide how many instances are live at once:

- **The pool** (`[engine] pool_total_instances`, default 1000, and the core-instance budget set
  by `[engine] class`). A Python instance takes 38 core instances. With the default
  `class = "rust"` a node holds only **78 live Python instances**; the rest wait (they do not
  fail). Set `class = "python"` or `"mixed"` on nodes that run Python: up to 1,000 live Python
  instances. QuickJS instances take 3 memory slots each, so at most `pool_total_instances / 3`
  (333) are live.
- **The memory budget** (`[engine] node_memory_budget`, default 1 GiB), charged with each
  instance's declared linear memory. Above it, parked instances are evicted (and re-activated
  later, never failed). Per GiB that is about 963 Rust, **64 Python**, 481 QuickJS or 112
  StarlingMonkey parked instances. Raise it for Python-heavy nodes.

| `[engine] class` | Rust | Python | QuickJS | StarlingMonkey |
|---|---|---|---|---|
| `rust` (default), `typescript` | 1,000 | 78 | 230 | 1,000 |
| `python`, `mixed` | 1,000 | 1,000 | 333 | 1,000 |

(live instances per node at `pool_total_instances = 1000`; details in
[sdk-resources.md](sdk-resources.md#pool-sizing-eng-2)). Fuel also differs by language: a
Python step burns far more fuel than the same Rust step
([sdk-resources.md](sdk-resources.md#fuel-per-language-pf-w6-from-09-07--s9)).

Remote workers have their own engine budget (`OSTRO_WORKER_MEMORY_BYTES`, default 1 GiB) and
slots ([workers.md](workers.md)).

## Disk

### Per step

Measured by the Phase 14 bytes-on-disk benchmark (profile A: 10 trivial steps per run, token
auth and audit on; [benchmarks.md](benchmarks.md#phase-14-10-configuration)), on a loaded
machine:

| Setup | Bytes per step |
|---|---|
| single node (RF = 1, P = 16) | **≈ 246**: WAL 147.7 + state 98.1 |
| three nodes (RF = 3) | ≈ 443 of WAL in total, about 148 per replica, plus state on each node |

The audit journal does not grow with starts or steps. Step inputs and outputs above the inline
limit go to the blob store and add their own size. Disk is reclaimed as runs reach their
retention (`[runs] retention`, default `168h` on a server, [workflow-semantics.md](workflow-semantics.md#12-retention))
and the WAL below the latest snapshot is trimmed.

A rough budget: `runs per day × steps per run × bytes per step × retention days`, plus payloads,
plus the snapshots kept (below), plus headroom for the WAL tail between snapshots
(`[snapshots] bytes_threshold`, 256 MiB per partition at most before a snapshot is due).

### Audit disk

Each node writes its own audit journal (`<data_dir>/audit`, [audit.md](audit.md)). Records are
one JSON line each, a few hundred bytes. What drives its size:

- administrative actions (deploys, policy changes, cancels, token and tenant changes);
- `secret.access`: one coalesced record per run, secret, origin and outcome per 60 s window,
  so workflows that use secrets add records in proportion to their runs;
- `auth.failure` / `authz.denied`: one record per key and 60 s window.

The journal is capped by `[audit] max_bytes` (default **10 GiB** per node) and
`[audit] retention` (2160 h); the oldest 64 MiB segments are deleted first. Reserve
`max_bytes` on the data volume, or point `[audit] dir` at its own volume. A full disk under the
journal makes the node refuse audited actions with 503 until it is fixed
([audit.md](audit.md#5-fail-closed-and-what-it-costs)). Backups do not include the journal.

## Snapshots and bottomless backups

Each partition snapshots its state store so recovery replays only the WAL tail:

| Key | Default | Meaning |
|---|---|---|
| `[snapshots] enabled` | `true` | |
| `[snapshots] interval` | `1h` | longest time between snapshots |
| `[snapshots] min_interval` | `60s` | shortest time between snapshots |
| `[snapshots] bytes_threshold` | 256 MiB | WAL bytes since the last snapshot that make one due |
| `[snapshots] keep` | 2 | local snapshots kept per partition |

**Bottomless backups** (`[backup] url`, S3-compatible or a local directory) continuously upload
sealed WAL segments and snapshots, with a data-loss window under one second
([bottomless.md](bottomless.md)). In a cluster all nodes share one prefix and each partition is
uploaded by its current leader ([bottomless.md](bottomless.md#cluster-mode-phase-12-d-p12-20));
whole-cluster restore points are described in [cluster-restore.md](cluster-restore.md).
Cost and retention of the remote copies are in [bottomless.md](bottomless.md#cost) and
[bottomless.md](bottomless.md#remote-retention). `ostro backup` writes a single backup file,
online or from a stopped data directory.

## Restore

- **One node** (or a single-node server): `ostro restore` from the bucket or a backup file;
  see the [restore runbook](bottomless.md#restore-runbook).
- **A whole cluster**, to one consistent cut across all partitions and the metadata:
  [cluster-restore.md](cluster-restore.md). It needs `OSTRO_SEALING_KEY` when the cluster CA is
  sealed.
- The sealing key is not in any backup; keep it apart from the bucket
  ([secrets.md](secrets.md#5-the-sealed-store)). The audit journal is not in any backup either.

## Sealing key

Every node of a cluster needs the same operator sealing key (`OSTRO_SEALING_KEY` or
`[secrets] sealing_key_file`). It seals user secrets and, in metadata, the API-token key, the
cluster CA and the UI session key. Rotation without downtime (new key plus old key on every
node, `ostro secret reseal`, then remove the old key) is in
[secrets.md §5.2](secrets.md#52-rotation).

## Upgrades

Rolling upgrades, the downgrade window, `ostro migrate-runs` and module GC are in the
[upgrade runbook](upgrade-runbook.md); feature levels and finalization in
[compat.md](compat.md). New clusters start at the newest feature level; a cluster upgraded from
an older release keeps its level until `ostro cluster upgrade finalize`
([compat.md](compat.md#new-clusters-start-at-the-newest-level)).

## File descriptors

Raise the open-file limit of the server process. Each partition holds WAL, state and snapshot
files, and each client, worker and replication connection takes one descriptor; the default
soft limit of 1,024 on many Linux systems is too low under load. In the 14-23 soak smoke a node
at the 1,024 limit hit `Too many open files`, after which its metadata driver stopped and the
node answered 503 without exiting, so no supervisor restarted it (an open finding). Run
servers with **at least 65,536**:

```ini
# systemd unit for ostro server
[Service]
LimitNOFILE=1048576
```

For a shell or container, `ulimit -n 65536` before `ostro server`, or the runtime's `--ulimit
nofile=65536:65536`. Kubernetes pods inherit the container runtime's limit (commonly 1,048,576
with containerd); check it with `cat /proc/<pid>/limits` inside a server pod.

## Kafka triggers

A `[[triggers.kafka]]` entry consumes a topic from any Kafka-protocol broker and starts one run
per record ([kafka-triggers.md](kafka-triggers.md)). Consumer placement follows partition
leadership, and its lag is exported as metrics
([kafka-triggers.md](kafka-triggers.md#metrics-and-alerts)). In a NetworkPolicy setup, allow
the brokers in the server egress ([kubernetes.md](kubernetes.md#6-networkpolicies)).

## Observability

- **Metrics** on `/metrics` (Prometheus): every series is in [metrics.md](metrics.md). Tenant
  metrics carry no `tenant` label by default, because `/metrics` is unauthenticated;
  `[tenants] metrics_tenant_labels = true` adds it (capped by `[tenants] metrics_label_limit`)
  when only trusted scrapers can reach the endpoint
  ([tenancy-and-auth.md](tenancy-and-auth.md#quota-metrics)).
- **Traces and logs** over OTLP, JSON logs, per-run guest logs: [observability.md](observability.md).
- **Audit** records can be exported as OTLP logs, and the chain head should be shipped off the
  node ([audit.md](audit.md#chain-head-export-detecting-truncation)).

Signals worth alerting on: `ostro_audit_unavailable_total` (audited actions refused), a stuck
backup upload floor ([bottomless.md](bottomless.md#alerting-stuck-upload-floor)), queue demand
and DLQ growth ([metrics.md](metrics.md#dead-letters)), and `ostro_security_policy_drift`.

## Capacity planning from the benchmarks

| Measurement | Result | Where |
|---|---|---|
| single node, 1.0 configuration (auth, audit, quotas), profile A | 29,421 verified steps/s in a short run on a loaded machine; 41,472 and 42,002 in two 60 s runs at lower load | [benchmarks.md](benchmarks.md#phase-14-10-configuration) |
| three nodes RF = 3, loopback, loaded machine | 4,359 verified steps/s, commit p99 180.7 ms | same |
| soak smoke, one node at 2,903 runs/s for 300 s | 0 lost runs, 0 duplicates, start-ack p99 47.6 ms; failed the memory-growth and fd criteria (open findings) | [soak.md](soak.md) |

The 1.0 targets are 10,000 steps/s on one node and 5,000 steps/s with commit p99 under 20 ms at
RF = 3, measured on the native reference box (pending). Plan with your own measurement:
`cargo test --release -p ostro-jepsen --test bench_single -- --ignored` runs the same benchmark
against a release build.
