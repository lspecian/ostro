# Ostro documentation

> **One Rust binary. Upload durable workflows written in Rust, Python or TypeScript as Wasm: Ostro stores them, runs them, and scales them.**

This page indexes every guide and reference. The design documents written before the build
started are at the end ([Design documents](#design-documents)). `bash scripts/docs-check.sh`
checks that every code snippet in these pages matches the file it was copied from and that every
link resolves.

## Start here

| Guide | Contents |
|---|---|
| [install.md](install.md) | Release artefacts, signature verification, the install script, macOS, the container image, building from source, first server settings |
| [getting-started.md](getting-started.md) | The loop in all three languages: init, dev, deploy, run, test, replay; toolchain notes |
| [getting-started/rust.md](getting-started/rust.md) | Rust, step by step: deploy, trigger (CLI and HTTP), observe, time-skipping test |
| [getting-started/python.md](getting-started/python.md) | Python, step by step |
| [getting-started/typescript.md](getting-started/typescript.md) | TypeScript, step by step |
| [migrations/index.md](migrations/index.md) | Migrating from [Temporal](migrations/from-temporal.md), [Restate](migrations/from-restate.md) or [cron and queue tables](migrations/from-cron-and-queue-tables.md) |

## Writing workflows

| Guide | Contents |
|---|---|
| [workflow-semantics.md](workflow-semantics.md) | Starts, signals, timeouts, gather and race, cancel, terminate, retry, await tokens, versions, continue-as-new, retention, tenancy of names |
| [determinism.md](determinism.md) | What the sandbox virtualises, steps and effects, divergence detection, `ostro replay` in CI, `patched` and `migrate-runs`, pitfalls per language |
| [children.md](children.md) | Child workflows |
| [replay.md](replay.md) | Replaying recorded runs against a new module |
| [testing.md](testing.md) | The test kits (Rust, Python, TypeScript, Go), time skipping, HTTP mocks |
| [connectors.md](connectors.md) | HTTP, SQL and object-store effects; secrets, grants and egress quick reference; triggers |
| [secrets.md](secrets.md) | Secret templates, grants, scrubbing, the sealed store and key rotation, providers |
| [security-egress.md](security-egress.md) | SSRF defence and per-module egress allow-lists |
| [limits.md](limits.md) | Engine and API limits |
| [sdk-builds.md](sdk-builds.md), [toolchains.md](toolchains.md) | How modules are built; pinned toolchains, cache, mirrors, `--docker` |
| [conformance.md](conformance.md) | The cross-language conformance suite |
| SDK references | [Rust](../sdks/rust/README.md), [Python](../sdks/python/README.md), [TypeScript](../sdks/ts/README.md) |

## Triggers

| Guide | Contents |
|---|---|
| [webhooks.md](webhooks.md) | Signed webhooks (GitHub, Stripe, Slack, Standard Webhooks, HMAC) |
| [cron.md](cron.md) | Time-zone-aware cron schedules and catch-up |
| [kafka-triggers.md](kafka-triggers.md) | One run per Kafka record, from any Kafka-protocol broker |

## Running Ostro

| Guide | Contents |
|---|---|
| [operations-sizing.md](operations-sizing.md) | Partitions, memory per language, the Python instance budget, disk per step, audit disk, snapshots, backup and restore, upgrades, file descriptors, observability, capacity |
| [kubernetes.md](kubernetes.md) | The Helm chart, values, Secrets, KEDA scaling to zero, NetworkPolicies, backups |
| [operations.md](operations.md) | Rerun, dead-letter queue, on-failure hooks, bulk operations, search |
| [workers.md](workers.md) | Remote workers, placement, spill, demand and KEDA |
| [flow-control.md](flow-control.md) | Queue concurrency, weights, priorities, keys, throttles, rate limits, pause |
| [bottomless.md](bottomless.md) | Continuous backups to object storage and the single-node restore runbook |
| [cluster-restore.md](cluster-restore.md) | Restoring a whole cluster to a consistent cut |
| [cluster-operations.md](cluster-operations.md) | Forming, growing, draining and rebalancing a cluster |
| [replication.md](replication.md) | Replicated partitions |
| [upgrade-runbook.md](upgrade-runbook.md), [compat.md](compat.md) | Rolling upgrades, feature levels, `migrate-runs`, module GC |
| [tls.md](tls.md) | Listener TLS modes, the cluster CA, mTLS, ACME |
| [ui.md](ui.md) | The embedded UI |
| [observability.md](observability.md), [metrics.md](metrics.md) | Traces, logs, guest logs; every metric |
| [cli-json.md](cli-json.md) | The `--json` envelope of every command |
| [benchmarks.md](benchmarks.md), [soak.md](soak.md), [sdk-resources.md](sdk-resources.md), [binary-size.md](binary-size.md) | Measurements |

## Security

| Guide | Contents |
|---|---|
| [tenancy-and-auth.md](tenancy-and-auth.md) | Tenants, API tokens and their lifecycle, scopes, the 404 rule, quotas and the per-node leadership share, UI sign-in |
| [audit.md](audit.md) | The hash-chained audit log: what is recorded, verification, fail-closed behaviour, retention, export |
| [signed-modules.md](signed-modules.md) | Requiring minisign or cosign signatures for deploys |
| [secrets.md](secrets.md), [security-egress.md](security-egress.md), [tls.md](tls.md) | Secrets, egress, transport security |
| `security-model.md`, `security/` | The security model, threat model and route-by-route auth matrix (written by plan 14-24) |

## Formats and internals

| Doc | Contents |
|---|---|
| [storage-format.md](storage-format.md), [snapshot-format.md](snapshot-format.md) | The frozen v0.1 on-disk formats |
| [wit-0.1-freeze-review.md](wit-0.1-freeze-review.md) | The WIT 0.1 freeze |
| [spikes/](spikes/) | Early feasibility spikes |

## Design documents

These were written on 2026-11-18, before the build started. They record the reasoning behind
the design; where they differ from the guides above, the guides describe what was built.

> `ostro dev` on a laptop → `ostro server` on one VM → add `ostro worker`s → three-node HA cluster. Same file, different flags.

**Decided 2026-11-18:** the name is **Ostro**, and it is a **separate product** built on shared Chronik crates ([04 §5](04-chronik-reuse.md#5-relationship-options-separate-product-vs-chronik-capability), option 1).

| Doc | Contents |
|---|---|
| [01-research.md](01-research.md) | Why single-binary wins, embedded storage options, Wasm Component Model toolchains, determinism & replay, the I/O bridge, competitive landscape (Restate, Temporal, Inngest, Windmill, Hatchet, DBOS, Cloudflare Workflows) |
| [02-prd.md](02-prd.md) | Product requirements: personas, modes, functional & non-functional requirements |
| [03-architecture.md](03-architecture.md) | Node roles, partitioned journal + state machines, replication, storage layout, WIT contract, suspend/resume trace, workers, security |
| [04-chronik-reuse.md](04-chronik-reuse.md) | What to lift from Chronik Stream (Raft driver, WAL, follower-pull replication, object store) — and the lessons from its replication history |
| [05-roadmap.md](05-roadmap.md) | Four phases from single-node MVP to replicated HA, with risks and exit criteria |
| [archive/pgflow/](archive/pgflow/) | The previous Postgres-embedded design (superseded; most of the Wasm/WIT/replay work carried over) |

---

## The pitch

Every durable-execution product makes you run a pile of things before your first workflow executes:

| Product | What you run to get going |
|---|---|
| Temporal | Frontend + history + matching + worker services, a database, **plus your worker fleet** |
| Restate / Inngest / DBOS | One server or a database — **plus your app process that hosts the code** |
| Hatchet / Windmill | A server **plus Postgres**, plus workers |
| Cloudflare Workflows | Nothing — but you are on Cloudflare forever |

**Ostro: one binary, nothing else.** You don't run workers and you don't keep an app process alive: you `ostro deploy` a Wasm component and the server executes it. When one box isn't enough, the same binary joins as a stateless worker or as a replica.

```bash
curl -fsSL https://get.ostro.dev | sh
ostro dev                                  # server + executor + UI on :7700, data in ./.ostro
ostro init --lang ts hello && cd hello
ostro deploy                               # build → Wasm component → upload
curl -XPOST localhost:7700/v1/workflows/hello/runs -d '{"name":"world"}'
```

## Key decisions

| # | Decision | Rationale |
|---|---|---|
| D1 | **Single static binary** containing server, worker, CLI and UI; role chosen by subcommand/flags | The "NATS/Caddy/k3s" adoption curve: download → run → works |
| D2 | **The server hosts the code.** Workflows are Wasm components uploaded to the server, not processes the user runs | The real moat: no worker fleet, no app process. Only Cloudflare (closed, hosted) does this today |
| D3 | **Embedded storage, log-first.** Each partition is an append-only journal (WAL with group commit) + a materialized state machine in an embedded KV (`redb`) | No external DB; single writer per partition → no queue-table contention, no polling, no vacuum |
| D4 | **Reuse Chronik Stream's distributed core**: tikv `raft-rs` driver for metadata, Kafka-style follower-pull ISR with leader epochs for partition logs, object-store tiering | Proven in a production Rust system the author owns; turns "years" into "integration + conformance" |
| D5 | **Single-node runs the replicated code path with RF=1** (ISR = {self}) | Chronik's main lesson: a mechanism wired for one path and not the other fails silently for months |
| D6 | **Deterministic replay over an event journal**; the sandbox virtualises clocks/RNG so determinism is enforced, not requested | Portable across nodes, versions, and languages; small state |
| D7 | **One WIT contract, three idiomatic SDKs** (Rust, Python, TS) | Polyglot with identical semantics, tested by one conformance suite |
| D8 | **Secrets never enter guest memory**; effects (HTTP etc.) run in a host I/O bridge with outbox + idempotency keys | Safe to run other people's code; effectively-once side effects |
| D9 | **"Bottomless" single node**: sealed segments + state snapshots stream to S3-compatible storage | A real Day-30 story (disaster recovery) *before* anyone needs a cluster |

## Naming

Checked on 2026-11-18 against crates.io, PyPI and npm (all three free). **Not yet checked:** GitHub org, domains, trademarks.

| Name | Meaning | Why it fits | CLI feel |
|---|---|---|---|
| **Ostro** ⭐ | Mediterranean south wind | 5 letters, easy to say and type; brandable, no baggage | `ostro dev` · `ostro worker --join` |
| **Meltemi** | Aegean wind that blows steadily for weeks | "Reliable, persistent" = durable execution | `meltemi server` |
| **Etesian** | The *annual* winds of the Aegean (Greek *etos*, year) | Recurring + dependable = scheduled durable workflows | `etesian deploy` |
| **Chronflow** | Chronik + flow | Signals a product family with Chronik Stream; shared crates, shared ops model | `chronflow run` |
| **Windward** | Facing into the wind | Plain English, positive; but generic for SEO, and existing companies use it | `windward start` |
| Tramontana / Libeccio / Windline | Other free options | Longer or less distinctive | — |

Taken on at least one registry (avoid): cairn, sluice, weir, baton, tern, gale, bora, sirocco, kestrel, ferry, jetstream, tailwind, tradewind (PyPI), chinook (PyPI/npm), foehn, aeolus.

**Recommendation:** **Ostro** if the product stands alone; **Chronflow** if you want it positioned as the second Chronik product (see [04 §5](04-chronik-reuse.md#5-relationship-options-separate-product-vs-chronik-capability)).

## Assumptions to re-verify before kick-off

1. Wasmtime Component Model async / WASI 0.3 stability (drives WIT `0.2` timing).
2. componentize-py / componentize-js output sizes and WIT feature support.
3. Restate's current licence, clustering and feature set (it is the closest competitor).
4. How cleanly Chronik's replication code (currently inside `chronik-server`) can be extracted — see [04 §3](04-chronik-reuse.md#3-what-needs-extraction-work).
