# Ostro

**Download one binary, deploy a Wasm workflow, and it runs durably.**

Ostro is a durable-execution engine that ships as a single Rust binary. You write workflows in
Rust, Python or TypeScript; `ostro deploy` compiles them to WebAssembly components against one
frozen WIT contract (`ostro:runtime@0.1.0`) and uploads them, and the server runs them. There is
no external database, no queue, no worker fleet and no app process to keep alive. Runs that
Ostro has accepted are never lost: they survive crashes, restarts and failovers, and replay
deterministically from their journal. The same binary runs as a laptop `dev` server, a
single-node `server`, a stateless `worker`, or a member of a replicated three-node cluster.

## Quickstart

```sh
curl -fsSL https://github.com/<owner>/ostro/releases/latest/download/install.sh | sh   # docs/install.md
ostro dev --ephemeral --time-skipping &         # API, gRPC and UI on http://127.0.0.1:7700
ostro init hello --lang ts && cd hello          # or --lang rust, --lang python
ostro deploy                                    # fetches the toolchain, builds, uploads, activates
ostro run hello --input '{"name":"ada"}'        # prints the run id
ostro signal <run-id> approve --payload '{"ok":true}'
ostro result <run-id> --wait
```

The only thing to install is `ostro`: language toolchains are downloaded on first use, pinned
and checksum-verified. Step-by-step guides: [Rust](docs/getting-started/rust.md),
[Python](docs/getting-started/python.md), [TypeScript](docs/getting-started/typescript.md).

## Features

- **Durable workflows in three languages.** Steps with retries, durable timers, signals,
  await tokens, cancellation, child workflows, continue-as-new, sagas and versioned patching,
  with one semantics checked by a cross-language conformance suite. See
  [docs/workflow-semantics.md](docs/workflow-semantics.md).
- **Deterministic replay, enforced.** The sandbox virtualises time and randomness, divergence
  is detected at the exact journal position, and `ostro replay` gates deploys against recorded
  runs. See [docs/determinism.md](docs/determinism.md).
- **Log-first embedded storage.** Partitioned write-ahead logs with group commit, per-partition
  state and snapshots, checked by crash campaigns of thousands of `kill -9`, failpoint and
  ENOSPC faults with zero lost or duplicated runs.
- **Replication and clustering.** Replicated partitions, a Raft metadata group, rolling
  upgrades, and `ostro cluster join`, `drain` and `remove`. See
  [docs/cluster-operations.md](docs/cluster-operations.md) and [docs/replication.md](docs/replication.md).
- **Effects with safe retries.** HTTP with stable idempotency keys, SQL (Postgres, MySQL) with
  an exactly-once mode, S3-compatible object storage. Secrets never enter guest code; egress
  allow-lists and an SSRF guard sit in front of every call. See [docs/connectors.md](docs/connectors.md).
- **Triggers.** REST and gRPC, signed webhooks, time-zone-aware cron, and Kafka topics. See
  [docs/webhooks.md](docs/webhooks.md), [docs/cron.md](docs/cron.md) and
  [docs/kafka-triggers.md](docs/kafka-triggers.md).
- **Multi-tenancy and security.** Tenants with quotas, scoped API tokens, a hash-chained audit
  log, signed modules, mutual TLS between nodes and workers. See
  [docs/tenancy-and-auth.md](docs/tenancy-and-auth.md), [docs/audit.md](docs/audit.md) and
  [docs/signed-modules.md](docs/signed-modules.md).
- **Workers and flow control.** Stateless workers, per-queue placement and spill-over, weights,
  priorities, keyed concurrency, throttles, rate limits, and KEDA autoscaling to zero. See
  [docs/workers.md](docs/workers.md) and [docs/flow-control.md](docs/flow-control.md).
- **Bottomless backups and restore.** Continuous upload of logs and snapshots to S3-compatible
  storage with a data-loss window under one second, plus single-node and whole-cluster restore.
  See [docs/bottomless.md](docs/bottomless.md) and [docs/cluster-restore.md](docs/cluster-restore.md).
- **Operations.** Embedded UI, Prometheus metrics, OpenTelemetry traces and logs, DLQ, rerun
  and bulk operations. See [docs/operations.md](docs/operations.md) and
  [docs/observability.md](docs/observability.md).

## Documentation

The full index is [docs/README.md](docs/README.md). Start with:

| Topic | Guides |
|---|---|
| Install and verify | [docs/install.md](docs/install.md), [docs/kubernetes.md](docs/kubernetes.md) |
| First workflow | [docs/getting-started.md](docs/getting-started.md) and the per-language guides in [docs/getting-started/](docs/getting-started/) |
| Run it in production | [docs/operations-sizing.md](docs/operations-sizing.md), [docs/tenancy-and-auth.md](docs/tenancy-and-auth.md), [docs/upgrade-runbook.md](docs/upgrade-runbook.md) |
| Coming from Temporal, Restate or cron + queue tables | [docs/migrations/index.md](docs/migrations/index.md) |

## Repository layout

| Path | Contents |
|---|---|
| `crates/ostro` | The `ostro` binary |
| `crates/ostro-server` | Server bootstrap, control plane, auth, tenancy, audit, workers, TLS, flow control |
| `crates/ostro-engine` | Wasmtime host, replay and the guest runtime |
| `crates/ostro-partition` | Partition state machine, apply, dispatcher, snapshots |
| `crates/ostro-log`, `ostro-meta`, `ostro-cluster` | Replicated log, metadata state machine, clustering |
| `crates/ostro-bridge` | Effects: HTTP, SQL, object store, secrets, egress |
| `crates/ostro-backup`, `ostro-objstore` | Bottomless backups and the object-store layer |
| `crates/ostro-triggers`, `ostro-kafka` | Cron and webhook verification, the Kafka client |
| `crates/ostro-worker` | The stateless worker runtime |
| `crates/ostro-cli`, `ostro-client`, `ostro-build`, `ostro-ui` | CLI, client and test kit, toolchains and builds, embedded UI |
| `sdks/` | Guest SDKs for Rust, Python and TypeScript |
| `wit/` | The frozen `ostro:runtime@0.1.0` contract |
| `examples/`, `guests/` | Example and test workflows |
| `deploy/` | Container image, Helm chart, Kubernetes and KEDA manifests |
| `docs/` | Guides, references and design documents |

## Building and testing

```sh
cargo build --workspace --locked
cargo test --workspace --locked
bash scripts/ci-local.sh          # the full local gate (mirrors CI)
bash scripts/docs-check.sh        # doc snippets match compiled code, links resolve
```

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE). Parts of the log and consensus layers are
adapted from [Chronik Stream](https://github.com/lspecian/chronik-stream), also Apache-2.0.
