# SDK resource numbers (Phase 9, SC5)

Per-language cost of a workflow instance on the real engine, measured with the
SDK-built conformance guests. These are the guests the conformance suite runs:
- `guests/conf-rust` (Rust SDK);
- `conformance/guests/python` (Python SDK, componentize-py 0.25.1);
- `conformance/guests/ts`, built twice: on QuickJS (the default TS backend) and on
  StarlingMonkey.

The earlier spikes measured trivial guests: S2 (`docs/spikes/S2-guest-toolchains.md`) and
S9 (`docs/spikes/S9-polyglot.md`). These numbers include everything the SDKs pull in:
Python's asyncio, json, dataclasses and the OstroLoop; the TS SdkPromise loop and its
shims.

- **Harness:** `crates/ostro-conformance/tests/resources.rs`. Every test is `#[ignore]`.
- **Raw data:** `docs/data/sdk-resources.jsonl`, one JSON line per measurement. Each line
  carries `loadavg`, `profile` and `host`.

```text
cargo test --release -p ostro-conformance --features polyglot --test resources \
    -- --ignored --test-threads=1 --nocapture
```

## Environment (D15: these numbers are advisory)

| | |
|---|---|
| machine | WSL2 dev machine: AMD Ryzen 9 7950X3D, 12 vCPUs, kernel `6.6.87.2-microsoft-standard-WSL2` |
| build | `--release`, Wasmtime 50.0.2, pooling allocator, fuel and epoch instrumentation on (production flags) |
| load | Run 1 (the tables below) started at a 1-minute load of 3.99, which rose to 5.2–7.6 during `warm_instantiate`. Run 2 started at 3.98 and rose to 4.9–5.8. Other agents were building on the same machine, and the load never dropped below 3 in a 20-minute wait. Every JSON line records its own `loadavg`. |
| date | 2026-11-22 |

The perf gate machine is native Linux on NVMe with 8 vCPUs, run by a human (D15). WSL2
timings are advisory. Treat a WSL2 *miss* as `human_needed`, never as a pass or a fail.

## Per language and backend

The table shows run 1. Run 2 (below) agrees within noise. Each row of the table was
measured as follows.
- **`.wasm`, `.cwasm`, compile:** component bytes; `Engine::precompile_component`.
- **Core instances:** the static count, then the calibrated admission weight (09-07,
  ENG-2).
- **Memories / tables, memory floor:** defined by the component's core modules, and the
  largest initial linear memory.
- **Warm instantiate:** `Runtime::prepare_cached` once, then 500 instantiations of the
  cached `InstancePre`, each through `Runtime::instantiate_probe`. That is the same store
  setup an activation uses (host state, fuel, epoch deadline, core-instance permit).
- **First call:** `Runtime::describe` on a fresh instance, n = 200. It includes its own
  instantiation; "call only" subtracts the warm instantiate p50.
- **Start-to-first-step:** `Runtime::activate` of `<prefix>conf_steps` (three steps) from
  `RunStarted`, until the first `StepCompleted` reaches the sink, n = 100 (Pitfall 11,
  NFR-2.4). "Run" is the whole activation to `Completed`.
- **Idle memory:** 50 live `<prefix>conf_timer` activations parked hot on their 1 h
  durable sleep. Columns: the RSS delta from `/proc/self/statm`, the PSS delta from
  `/proc/self/smaps_rollup`, both divided by 50, and the bytes each instance charges to
  `engine.node_memory_budget`.

| | Rust | Python | TS QuickJS | TS StarlingMonkey |
|---|---|---|---|---|
| `.wasm` | 0.29 MB | 22.04 MB | **2.31 MB** | 13.21 MB |
| `.cwasm` / compile | 1.01 MB / 36 ms | 43.66 MB / 1,088 ms | 6.34 MB / 397 ms | 43.33 MB / 1,578 ms |
| core instances (static / admission weight) | 3 / 3 | 38 / 38 | 11 / **13** | 3 / 3 |
| memories / tables per instance | 1 / 2 | 1 / 2 | **3 / 3** | 1 / 2 |
| declared memory floor | 1.06 MiB | **15.94 MiB** | 2.13 MiB | 9.06 MiB |
| **warm instantiate p50 / p90** | 22 / 22 µs | **460 / 490 µs** | **64 / 73 µs** | **11 / 17 µs** |
| first call (`describe`, incl. instantiate) p50 / p90 | 35 / 38 µs | 2,253 / 2,575 µs | 476 / 605 µs | 1,448 / 1,687 µs |
| first call only p50 | 13 µs | 1,793 µs | 412 µs | 1,437 µs |
| start-to-first-step p50 / p90 | 24 / 25 µs | 3,176 / 3,523 µs | 621 / 717 µs | 1,452 / 1,644 µs |
| run to completion (3 steps) p50 | 37 µs | 3,787 µs | 704 µs | 1,637 µs |
| **idle RSS per parked instance** (statm) | 61 KB | **10,936 KB** | **1,145 KB** | 2,877 KB |
| idle PSS per parked instance | 55 KB | 1,876 KB | 442 KB | 734 KB |
| node budget charge per instance (run 2) | 1,114,112 B | 16,711,680 B | 2,228,224 B | 9,502,720 B |

Run 2 (same binary, 30 minutes later, load 4.9–5.8):

| | Rust | Python | TS QuickJS | TS StarlingMonkey |
|---|---|---|---|---|
| warm instantiate p50 / p90 | 22 / 22 µs | 448 / 479 µs | 66 / 77 µs | 11 / 18 µs |
| first call (incl. instantiate) p50 | 36 µs | 2,196 µs | 531 µs | 1,501 µs |
| start-to-first-step p50 / p90 | 26 / 31 µs | 3,020 / 3,338 µs | 697 / 749 µs | 1,517 / 1,652 µs |
| idle RSS / PSS per parked instance | 66 / 55 KB | 10,915 / 1,876 KB | 1,139 / 442 KB | 2,880 / 733 KB |

### Reading the memory columns

- **RSS overstates what a Python instance really costs.** A pool slot maps the module's
  copy-on-write memory image. A page the guest only reads is one physical page, but
  `statm` counts it once per slot that touched it.
  - Python reads about 9 MB of its image while it parks. RSS charges that 9 MB to every
    instance.
  - PSS shares it out: `Pss_Shmem` is 185 KB per instance, against an RSS–PSS gap of
    about 9 MB.
- **What a parked instance really costs:** its private pages (`Anonymous`) plus a 1/n
  share of the image.

| private memory per parked instance (`Anonymous`) | 55 KB | 1,691 KB | 427 KB | 689 KB |
|---|---|---|---|---|

- **Capacity planning per language:**
  - Use **PSS × instances + one copy of the image** (about the `.wasm` data size) for
    physical memory.
  - Use **RSS** to stay safe under tools that sum RSS: container limits count shared
    shmem pages once, but `ps` does not.

## Comparison with the trivial-guest spikes

| | S2 (trivial guests) | S9 (raw-binding prototypes) | 09-19 (real SDK guests) |
|---|---|---|---|
| Python warm instantiate p50 | 454 µs (pooling) | 451–477 µs | 460 µs |
| QuickJS warm instantiate p50 | — | 66 µs | 64 µs |
| StarlingMonkey warm instantiate p50 | 10.2 µs (pooling) | 12.3 µs | 11 µs |
| Rust warm instantiate p50 | 9.2 µs (pooling) | — | 22 µs |
| Python first call per fresh instance p50 | 881 µs (`run`) | 397 µs (`describe`) | 2,253 µs (`describe`, incl. instantiate) |
| QuickJS first `describe` p50 | — | 212 µs | 476 µs |
| StarlingMonkey first call p50 | 601 µs (`run`) | 750 µs (`describe`) | 1,448 µs (`describe`) |
| Python RSS per instance | 6.5 MB (on-demand, after `run`) | 3.78 MB (pooled, `describe` only) | 10.9 MB RSS / 1.9 MB PSS (pooled, parked mid-run) |
| QuickJS RSS per instance | — | 0.61 MB | 1.15 MB RSS / 0.44 MB PSS |
| StarlingMonkey RSS per instance | 2.4 MB | 2.67 MB | 2.88 MB RSS / 0.73 MB PSS |
| Python `.wasm` / memory floor | 18.5 MB / 12.56 MiB | 21.82 MB / 15.69 MiB | 22.04 MB / 15.94 MiB |

Growth caused by the SDKs:
- **Instantiation does not grow.** It is dominated by the component's core-instance
  count (38 for Python, 13 for QuickJS), not by what the snapshot contains.
- **The first export call does grow, 2–5×.** The SDKs register their workflows, build
  the asyncio loop or SdkPromise scheduler, and encode the `describe` list through their
  JSON/codec layers.
- **Memory grows from what a parked run keeps alive:** the asyncio task and its coroutine
  frames, the SDK op tables and the journal-backed context. A `describe`-only instance
  keeps none of these. Python's memory floor grew 3.38 MiB over S2's raw guest.
- The Rust guest is larger than S2's trivial one (0.29 MB against 82 KB) and
  instantiates in 22 µs instead of 9 µs. Its runtime cost is still negligible.

## SC5 verdict

| target (warm instantiation p50) | measured on WSL2 | margin |
|---|---|---|
| Python < 10 ms | 0.46 ms | 21× |
| TS (QuickJS) < 5 ms | 0.064 ms | 78× |
| TS (StarlingMonkey) < 5 ms | 0.011 ms | 450× |

- **SC5 is met on WSL2 (advisory, D15).** Even the strictest reading holds: instantiate,
  run the SDK and journal the first step, which is the start-to-first-step column.
  - Python: 3.2 ms against 10 ms.
  - QuickJS: 0.62 ms against 5 ms.
  - StarlingMonkey: 1.45 ms against 5 ms.
- Resident memory per idle instance is published for each language (table above).
- **`human_needed`:** confirm on the native perf machine, quiet, with enforcement on:

```text
OSTRO_SC5_ENFORCE=1 cargo test --release -p ostro-conformance --features polyglot --test resources -- --ignored --test-threads=1
```

With `OSTRO_SC5_ENFORCE=1`, `warm_instantiate` fails if any target is missed. Without it,
a miss is printed and recorded but not asserted.

## Pool sizing (ENG-2)

Admission is weighted over **every pooled resource** an instantiation takes (ENG-2, plan
09-07; memories and tables since plan eng2-mem). Each activation, `describe` and warm
probe acquires, from one FIFO budget per resource:

| resource | weight (Rust / Python / QuickJS / StarlingMonkey) | capacity |
|---|---|---|
| component instances (also the async stacks) | 1 / 1 / 1 / 1 | `total_instances` |
| core instances | 3 / 38 / 13 / 3 (calibrated once per module) | `pool_total_core_instances`, default `total_instances × class.core_instances_per_instance()` |
| linear memories | 1 / 1 / 3 / 1 | `total_memories = total_instances` |
| tables | 2 / 2 / 3 / 2 | `total_tables = 2 × total_instances` |

- Memory and table weights are exact and static: Wasmtime's
  `Component::resources_required` counts every core module the compiled component
  instantiates, adapter modules included. The core-instance weight is calibrated as before.
- The budgets are acquired in that fixed order, with one deadline, so waiters cannot
  deadlock. A waiter that times out releases what it already holds.
- When any resource is full, activations **wait**; they do not fail. At the activation
  deadline they evict with `BudgetPressure` and are re-activated by the executor; a
  cancelled waiter evicts with `External`; `describe` and the warm probe wait 5 s and then
  fail with an error naming the resource (e.g. `memory budget: no 3 free slots before the
  deadline`).
- A module needing more memories or tables than the whole pool holds is rejected at deploy
  validation, like one needing more core instances.
- The pool reservation and the default engine-config snapshot are unchanged. For Rust-only
  nodes the four budgets bind at the same point (1,000 instances at defaults), so
  admission behaves exactly as before.

Measured (`pool_mixed`, all runs `conf_timer`):

| case | result |
|---|---|
| `class = mixed` (38,000 core instances): 100 Python + 50 Rust + 50 QuickJS + 50 StarlingMonkey parked at once | all 250 parked in 97 ms; 4,750 / 38,000 slots in use; **0 failures** |
| default `class = rust` (3,000 core instances, 78 Python at once): 100 concurrent Python runs, timers fire after 300 ms | peak 3,000 / 3,000 slots; the 79th–100th waited for permits; **all 100 completed** in 630 ms, 0 `AttemptFailed` |
| `class = mixed`, `total_instances = 60`: 30 QuickJS runs | 09-19, before the fix (core budget only, admits 175): **19 parked / 11 failed (run 1), 20 parked / 10 failed (run 2)**, the failures with `AttemptFailed(Trap("instance pool exhausted (engine.pool.total_instances=60): maximum concurrent limit of 60 for memory stripe 0 reached"))`. Since eng2-mem (memories budget admits 20): **20 parked, 10 waiting, 0 failed**; the waiters park as soon as parked runs release memories (also the non-ignored `quickjs_memories_wait_for_admission` in `tests/polyglot_engine.rs`) |

How many live instances fit per node at the default `total_instances = 1000`. The limit
is the smallest of the four budgets; past it, runs wait:

| `[engine] class` | core budget | Rust | Python | QuickJS | StarlingMonkey |
|---|---|---|---|---|---|
| `rust` (default) | 3,000 | 1,000 | 78 | 230 | 1,000 |
| `typescript` | 3,000 | 1,000 | 78 | 230 | 1,000 |
| `python` | 38,000 | 1,000 | 1,000 | **333 by memories** (core alone: 2,923) | 1,000 |
| `mixed` | 38,000 | 1,000 | 1,000 | **333 by memories** (core alone: 2,923) | 1,000 |

Guidance:
- **Pure Rust nodes:** keep the default.
- **Nodes that run Python:** set `class = "python"` or `"mixed"`. With the default class a
  node holds only 78 live Python instances, and the rest queue.
- **`class = "typescript"` is sized for StarlingMonkey** (3 core instances per instance),
  which gives 230 QuickJS instances. More QuickJS density needs an override, up to the
  memories limit: `pool_total_core_instances = 13 × total_instances / 3` (4,333 at
  defaults, 333 QuickJS instances).
- **QuickJS-heavy nodes:** a QuickJS instance takes 3 of the `total_instances` memory
  slots, so at most `total_instances / 3` are live at once; the rest wait for admission
  (they no longer fail, plan eng2-mem). Raising `pool_total_core_instances` past
  `13 × total_instances / 3` adds no QuickJS density. Raise `total_instances` instead.
- **Node memory budget:** `engine.node_memory_budget` (default 1 GiB) is charged with each
  instance's linear memory. Parked instances above it are LRU-evicted (`BudgetPressure`,
  re-activated later, not failed). Per instance the charge is the declared linear memory:
  - Rust 1.06 MiB, so about 963 parked instances per GiB;
  - Python 15.94 MiB, so about **64 per GiB**;
  - QuickJS 2.13 MiB (3 memories), so about 481 per GiB;
  - StarlingMonkey 9.06 MiB, so about 112 per GiB.

  With the default budget, a node keeps that many parked instances hot. Raise
  `node_memory_budget` for Python-heavy nodes.
- **Memory floor:** `pool.max_memory_bytes` is raised to the class floor
  (`LanguageClass::min_memory_bytes`): Rust 1,114,112 B, TypeScript 9,502,720 B (the
  StarlingMonkey guest; QuickJS declares 2.13 MiB), Python and mixed 16,711,680 B (the
  SDK guest's 255 pages; S2's 13,172,736 B was stale until plan eng2-mem).
  `tests/polyglot_engine.rs` derives these from the real SDK guests.

## Fuel per language (PF-W6, from 09-07 / S9)

Fuel counts Wasm instructions of the guest **runtime**, so an interpreted language burns
far more per workflow line. Fresh instance, one-step `hello`, against the default
`engine.activation_fuel` of 10,000,000,000:

| | Python | QuickJS | StarlingMonkey |
|---|---|---|---|
| `describe` | ~120k | 302k | 2.48M |
| `run(hello)` | 185k–417k | 778k | 2.54M |

- All of these are far below 0.1% of the default activation fuel.
- Step bodies are bounded separately by `step-options.fuel`. That bound must be chosen per
  language: the same logic costs far more fuel in an interpreter than as compiled Rust.
