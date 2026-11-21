# Ostro

**Download one binary, deploy a Wasm workflow, and it runs durably.**

Ostro is a durable-execution engine that ships as a single Rust binary. You write workflows in
Rust (Python and TypeScript are in progress), compile them to WebAssembly components against one
WIT contract (`ostro:runtime@0.1.0`), and deploy them to Ostro. Runs that Ostro has accepted are
never lost. They survive crashes and restarts, and they replay deterministically from their
journal. Ostro needs no external database, no queue and no separate worker fleet.

The same binary runs as a local `dev` server, a single-node `server` or a stateless `worker`.

> **Status: pre-release, under active development.** No release has been cut yet, and the WIT
> contract and on-disk format are frozen at v0.1. See [Status](#status) for what is done.

## Features

- **Durable workflows.** Steps, retries with backoff, durable timers, signals, cancellation,
  child workflows, continue-as-new, await tokens and versioned patching. Replay detects
  non-determinism. See [docs/workflow-semantics.md](docs/workflow-semantics.md).
- **Log-first embedded storage.** A partitioned write-ahead journal with group commit, per-partition
  state on fjall, and snapshot plus log-tail recovery. Checked by crash campaigns of 10,000
  `kill -9`, failpoint and ENOSPC faults with zero lost or duplicated runs.
- **Effects with safe retries.** HTTP calls carry stable idempotency keys and are re-driven after a
  crash. SQL (Postgres and MySQL) has an exactly-once mode. Object storage works with any
  S3-compatible store or the local disk. Secrets never enter guest code, and egress allow-lists
  plus an SSRF guard sit in front of every outbound call. See [docs/connectors.md](docs/connectors.md),
  [docs/secrets.md](docs/secrets.md) and [docs/security-egress.md](docs/security-egress.md).
- **Triggers.** HTTP and gRPC API, signed webhooks (GitHub, Stripe, Slack, Standard Webhooks,
  generic HMAC) and time-zone-aware cron schedules with catch-up policies. See
  [docs/webhooks.md](docs/webhooks.md) and [docs/cron.md](docs/cron.md).
- **Bottomless backups.** Continuous upload of the log and snapshots to S3-compatible storage with a
  data-loss window under one second, plus `ostro backup` and `ostro restore`. See
  [docs/bottomless.md](docs/bottomless.md).
- **Workers and flow control.** Stateless workers over mutual TLS. Per-queue placement
  (`embedded`, `workers` or `auto` spill-over), queue weights, priorities, keyed concurrency,
  throttles, rate limits and pause/resume. A KEDA demand metric drives autoscaling. See
  [docs/workers.md](docs/workers.md), [docs/flow-control.md](docs/flow-control.md) and
  [docs/tls.md](docs/tls.md).
- **Observability.** Prometheus metrics, OpenTelemetry traces and logs, and per-run guest logs. See
  [docs/metrics.md](docs/metrics.md) and [docs/observability.md](docs/observability.md).

## Quickstart

You need Rust 1.100 (pinned in `rust-toolchain.toml`) and the `wasm32-wasip2` target.

```sh
# Build the binary
cargo build --release --locked -p ostro
export PATH="$PWD/target/release:$PATH"

# Build the example workflow
(cd examples/order-saga && cargo build --release --target wasm32-wasip2)

# Start a local dev server (http://127.0.0.1:7700)
ostro dev &

# Deploy the workflow and start a run
ostro deploy examples/order-saga/target/wasm32-wasip2/release/order_saga.wasm --activate
ostro run order-saga --input '{"order_id":"o-1","amount":4999,"sku":"BOOK-42","qty":1}'

# Send it a signal and wait for the result
ostro signal <run-id> approve --payload true
ostro result <run-id> --wait
```

`examples/order-saga/quickstart.sh` runs the same steps end to end.

## Repository layout

| Path | Contents |
|---|---|
| `crates/ostro` | The `ostro` binary |
| `crates/ostro-server` | Server bootstrap, control plane, workers, TLS, flow control |
| `crates/ostro-engine` | Wasmtime host, replay and the guest runtime |
| `crates/ostro-partition` | Partition state machine, apply, dispatcher, snapshots |
| `crates/ostro-log`, `ostro-meta` | Replicated log and metadata state machine |
| `crates/ostro-bridge` | Effects: HTTP, SQL, object store, secrets, egress |
| `crates/ostro-backup`, `ostro-objstore` | Bottomless backups and the object-store layer |
| `crates/ostro-triggers` | Cron and webhook verification |
| `crates/ostro-worker` | The stateless worker runtime |
| `sdks/rust` | Rust guest SDK (`#[workflow]` / `#[step]`) |
| `wit/` | The frozen `ostro:runtime@0.1.0` contract |
| `examples/` | Example workflows |
| `deploy/` | Kubernetes and KEDA manifests |
| `docs/` | Design documents and reference docs |

## Status

Ostro is being built in phases (see [docs/05-roadmap.md](docs/05-roadmap.md)):

| Phase | Scope | State |
|---|---|---|
| 1–4 | Foundation, storage, Wasm engine, API and Rust SDK | Done |
| 5 | Workflow semantics, crash harness, WIT 0.1 freeze | Done |
| 6 | Effects bridge, connectors, secrets | Done |
| 7 | Workers, placement, flow control, TLS | Done |
| 8 | Triggers, bottomless backups, observability | Built and verified; final gate pending |
| 9 | Python and TypeScript SDKs, conformance suite | In progress |
| 10–14 | Hardening, multi-tenancy, replicated partitions, clustering, release | Planned |

## Building and testing

```sh
cargo build --workspace --locked
cargo test --workspace --locked
bash scripts/ci-local.sh          # the full local gate (mirrors CI)
```

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE). Parts of the log and consensus layers are
adapted from [Chronik Stream](https://github.com/lspecian/chronik-stream), also Apache-2.0.
