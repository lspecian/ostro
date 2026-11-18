# Ostro — Design Documentation

> **One Rust binary. Upload durable workflows written in Rust, Python or TypeScript as Wasm — Ostro stores them, runs them, and scales them.**
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
