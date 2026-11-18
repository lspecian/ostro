# Spike S2: guest toolchains (Python, TypeScript, Rust)

Status: complete (FOUND-06, S2 half). Run on 2026-11-18. The quiet-machine confirmation of instantiate p50 and cold start (plan 01-08) is at the end; it confirms every number. Step handle **decided: resource** (PROJECT.md Key Decisions).
Apparatus (plan 01-03): `spikes/wit/spike.wit`, `spikes/s2-guests/build.sh` and `spikes/s2-harness` (wasmtime 50.0.2).
Runner: `spikes/s2-guests/measure.sh <env|pool|perf|probes|repro|all|confirm>`. Each measurement runs in its own harness process.
Raw data is in `docs/spikes/data/S2/`:

| File | Contents |
|---|---|
| `env.txt` | machine and toolchain versions |
| `perf.jsonl` | compile; instantiate n=1000 and memory n=50, each for on-demand and pooling; 20 cold-start processes per allocator. Every line is tagged `guest`, `allocator`, `kind`, `loadavg` |
| `pool-limits.jsonl` | smallest pooling limits per component instance |
| `probes.jsonl` | ambient-nondeterminism probes, reseed, step round trip and drop counts, for build `a` and an independent build `b` |
| `imports.jsonl` | component imports and exports |
| `confirm.jsonl` | 01-08 quiet-machine confirmation: instantiate n=1000 and 10 cold starts per guest and allocator |
| `repro.txt` | 3 builds per language (plus Python ×3 with `PYTHONHASHSEED=0`): sha256, size, first differing bytes, section attribution |

All numbers below cite their source as `file[selector]`. For example, `perf[py/pooling/instantiate]` means the `perf.jsonl` line with `guest=py`, `allocator=pooling`, `kind=instantiate`.

## Summary

- **NFR-2.3 is met by a wide margin in all three languages and with both allocators.** Steady instantiate p50 from a warm `InstancePre`:
  - Python: 440 µs (on-demand) and 454 µs (pooling), against a 10 ms target.
  - TS: 19.5 µs and 10.2 µs, against 1 ms.
  - Rust: 15.7 µs and 9.2 µs, against 1 ms.
- **Python costs about 6.5 MB RSS per live instance.** TS costs 2.4–2.5 MB and Rust about 40 KB. Python is at the low end of the feared 10–50 MB range, so it does not need to default to workers. A memory budget (NFR-1.3) is enough. This is a recommendation, not a decision.
- **Pooling barely helps Python.** The componentize-py component instantiates **38 core instances**, against 3 for TS and Rust (`pool-limits.jsonl`). Wasmtime's default pool (1000 total core instances) therefore fits only **26 live Python instances**. ENG-08 must size `total_core_instances` by guest mix.
- **No toolchain reseeds randomness after pre-init.**
  - Python: the random state, `hash()` and set order are baked per build, identical across instances and different across builds. `time.time()` and `uuid.uuid4()` trap.
  - TS: `Math.random()` and `crypto.getRandomValues()` are identical across instances *and* across builds. `Date.now()` is frozen at the build timestamp.
  - Rust: std `HashMap` and `SystemTime::now()` trap on the undefined WASI imports.
  - **Host-driven reseeding works in all three languages:** the same seed gives the same draws, and different seeds give different draws.
- **`resource step` round-trips in Python, TS and Rust.** A step dropped without `complete` reaches `HostStep::drop` (count 1) in all three. But Python needs the context manager and TS needs an explicit `Symbol.dispose`. In TS, a completed step in `run` is **never dropped** (`step_drops: 0`). **Recommendation: resource.**
- **The Rust wasm32-wasip2 guest imports 16 `wasi:*@0.2.12` interfaces**, even though its exports make no WASI calls. Python and TS import only `ostro:spike/*`.
- **Only Rust builds are reproducible** (3/3 identical shas from clean builds). Python (with or without `PYTHONHASHSEED=0`) and TS give a different sha every build. The difference is in the pre-init memory snapshot (data section). Python also shuffles the order of its small shim core modules.

## Environment and toolchains

From `env.txt`:
- **Machine:** AMD Ryzen 9 7950X3D, 12 vCPUs exposed to a WSL2 VM (kernel 6.6.87.2-microsoft-standard-WSL2), 48 GB RAM.
- **Toolchains:** rustc 1.100.0, componentize-py 0.25.1 (`--stub-wasi`), jco 1.42.0 / componentize-js 0.23.0 (`--disable all`), node v24.13.0, wit-bindgen 0.63.0 (wasm32-wasip2), wasmtime 50.0.2.
- **Engine config:** `cranelift_nan_canonicalization(true)` and `relaxed_simd_deterministic(true)`. Unknown imports are defined as traps; no `wasmtime-wasi`.

**WSL2 caveat:** absolute latencies on a Hyper-V guest are indicative only. Treat the relative comparisons (language vs language, on-demand vs pooling) as the result.

**Load:** plans 01-02 (S3) and 01-04 (raft vendoring builds) ran concurrently.
- The final `perf.jsonl` was captured at 1-min loadavg 1.06–1.07, well under `nproc` = 12, so no latency measurement needed a repeat.
- An earlier perf pass at loadavg 3.4–3.5 was discarded because its lines lacked `allocator_config`. Its numbers agreed within about 15% (for example, Python on-demand p50 was 510 µs then and 440 µs now).
- Plan 01-08 re-measures instantiate p50 and cold start on a quiet machine and appends the result here.

## Sizes and AOT compile

Source: `perf[<g>/n/a/compile]`. Compile runs in its own process. Peak RSS comes from `/usr/bin/time %M`, which equals the harness VmHWM.

| | Python | TS | Rust |
|---|---|---|---|
| `.wasm` | 18,499,437 B | 12,017,241 B | 81,981 B |
| gzip (`.wasm`) | 6,275,440 B | 3,890,668 B | 31,380 B |
| AOT `.cwasm` | 32,868,024 B | 33,613,560 B | 231,968 B |
| compile time (Cranelift) | 846 ms | 1,131 ms | 15 ms |
| compile peak RSS | 469,652 KB | 531,772 KB | 23,724 KB |

Deploy-time AOT for interpreted guests costs about 1 s and about 0.5 GB peak RSS per module. Phase 4 deploy must serialise these compiles, or bound how many run at once. Each deployed Python or TS version also costs about 33 MB of `.cwasm` on disk.

## Instantiation (NFR-2.3)

Source: `perf[<g>/<alloc>/instantiate]`. Each component is loaded with `Component::deserialize_file` (mmap). The first instantiate and first `run` call are reported separately. Then come n=1000 fresh `Store` instantiations, each followed by its first `run`, and 100 steady calls on one warm instance.

| | Python on-demand | Python pooling | TS on-demand | TS pooling | Rust on-demand | Rust pooling |
|---|---|---|---|---|---|---|
| `deserialize_file` | 2.67 ms | 2.50 ms | 0.96 ms | 0.89 ms | 0.27 ms | 0.25 ms |
| first instantiate | 838 µs | 819 µs | 65 µs | 50 µs | 52 µs | 52 µs |
| steady instantiate p50 | **440 µs** | **454 µs** | **19.5 µs** | **10.2 µs** | **15.7 µs** | **9.2 µs** |
| steady instantiate p99 | 709 µs | 648 µs | 36.8 µs | 20.0 µs | 34.0 µs | 22.4 µs |
| first `run` call (first instance in process) | 1,695 µs | 1,553 µs | 1,520 µs | 1,092 µs | 60 µs | 107 µs |
| first `run` call per fresh instance, p50 / p99 | 903 / 1,173 µs | 881 / 1,124 µs | 585 / 828 µs | 601 / 781 µs | 5.5 / 15.6 µs | 7.8 / 14.1 µs |
| steady `run` call p50 (same instance) | 12.5 µs | 12.4 µs | 34.4 µs | 35.1 µs | 0.89 µs | 0.95 µs |
| **NFR-2.3** (Python < 10 ms, TS/Rust < 1 ms) | **met** | **met** | **met** | **met** | **met** | **met** |

Notes:
- The Python pooling run used `total_core_instances = 1900` (= 50 × 38, recorded in `allocator_config.overrides`) so that the n=50 memory run fits. TS and Rust used `PoolingAllocationConfig::default()`.
- **The first call on each fresh instance is far more expensive than a steady call:** about 0.9 ms for Python and about 0.6 ms for TS, against 12–35 µs steady. Lazy guest-side initialisation runs on the first export call of every instance. For NFR-2.4 (start to first step, p50 < 5 ms), the per-activation cost is instantiate + first call: about 1.3 ms for Python, about 0.6 ms for TS and about 25 µs for Rust. That is within budget. Phase 3 should still measure it with a realistic workflow.
- Pooling halves TS and Rust instantiate time but leaves Python unchanged. Python's cost is dominated by its 38 core instances, not by memory setup.

## Memory per instance

Source: `perf[<g>/<alloc>/memory]`, n=50 live instances that have each made one `run` call. "Declared" is the largest `desired` size seen by the `ResourceLimiter`.

| | Python on-demand | Python pooling | TS on-demand | TS pooling | Rust on-demand | Rust pooling |
|---|---|---|---|---|---|---|
| declared linear memory | 13,172,736 B (201 pages) | same | 9,502,720 B (145 pages) | same | 1,114,112 B (17 pages) | same |
| RSS per live instance | **6,500 KB** | **6,497 KB** | 2,542 KB | 2,409 KB | 38 KB | 46 KB |
| RSS retained after dropping all 50 | 14,888 KB | 12,656 KB | 8,636 KB | 5,784 KB | 1,152 KB | 1,148 KB |
| run errors | 0 | 0 | 0 | 0 | 0 | 0 |

- Retention is the process total after drop compared with before. It is not per instance: it includes allocator and runtime caches and is bounded (about 12–15 MB for Python after 50 instances).
- Declared memory equals the module's initial memory. A pool `max_memory_size` one page smaller is rejected with "module memory does not fit" (`pool-limits[*/max-memory-size]`). The pool's memory slot must be at least 13.2 MB for Python, 9.5 MB for TS and 1.1 MB for Rust, plus headroom for growth during real workflows.

**Open question answered: Python per-instance memory vs NFR-2.** Python costs about 6.5 MB of resident memory per live instance for a trivial workflow, about 2.6× TS and about 170× Rust. At the default resident-memory budget scale this is roughly 150 live Python instances per GB. It is at the low end of the feared 10–50 MB range.
- **Recommendation (not a decision):** Python does **not** need to default to workers. Run Python embedded under the NFR-1.3 instance and memory budgets, which queue excess work. Keep workers as the scaling path.
- Revisit in Phase 9 with a realistic workflow (stdlib imports such as `json`, `dataclasses` and `datetime`, which grow the snapshot).

### Pooling limits (input for Phase 3 ENG-08)

Source: `pool-limits.jsonl`. Each value is the smallest per-component-instance limit that lets one instance run, found by shrinking the pool totals.

| | total core instances | total memories | total tables | min `max_memory_size` |
|---|---|---|---|---|
| Python | **38** | 1 | 2 | 13,172,736 B |
| TS | 3 | 1 | 2 | 9,502,720 B |
| Rust | 3 | 1 | 2 | 1,114,112 B |

With the defaults (1000 total core instances), at most **26** Python instances can be live at once. The 27th fails with `maximum concurrent limit of 1000 for core instances reached` (observed with `memory --n 27`). ENG-08 must derive `total_core_instances` from the instance budget × the per-component core-instance count of the deployed modules. Alternatively, it can fall back to on-demand for Python, since pooling gives Python no latency benefit.

## Cold start

Source: `perf[<g>/<alloc>/cold]`, 20 fresh processes each. In-process total runs from the first statement of `main` to the return of the first `run` (exec and ld.so excluded). `process_wall_ms` is measured by `measure.sh` around the whole process, so it includes exec, the dynamic loader, process teardown and roughly 3 ms of `date`/fork overhead.

| | Python on-demand | Python pooling | TS on-demand | TS pooling | Rust on-demand | Rust pooling |
|---|---|---|---|---|---|---|
| **in-process total, median** | **5.07 ms** | **5.35 ms** | **2.47 ms** | **2.69 ms** | **0.72 ms** | **0.80 ms** |
| in-process total, p90 | 5.39 ms | 5.70 ms | 2.98 ms | 2.86 ms | 0.84 ms | 0.83 ms |
| process wall, median / p90 | 8.89 / 9.33 ms | 9.50 / 10.38 ms | 5.78 / 6.57 ms | 6.05 / 6.33 ms | 3.86 / 4.15 ms | 3.56 / 3.87 ms |
| stage: `main` entry | 130 µs | 138 µs | 123 µs | 129 µs | 143 µs | 122 µs |
| stage: engine | 0.13 ms | 0.29 ms | 0.12 ms | 0.26 ms | 0.15 ms | 0.26 ms |
| stage: `deserialize_file` | 2.53 ms | 2.59 ms | 0.91 ms | 0.95 ms | 0.27 ms | 0.23 ms |
| stage: linker + `instantiate_pre` | 0.02 ms | 0.02 ms | 0.02 ms | 0.02 ms | 0.06 ms | 0.06 ms |
| stage: instantiate | 0.78 ms | 0.79 ms | 0.07 ms | 0.05 ms | 0.06 ms | 0.06 ms |
| stage: first `run` | 1.51 ms | 1.57 ms | 1.25 ms | 1.27 ms | 0.03 ms | 0.02 ms |

Stage values are medians per stage, so they need not sum exactly to the median total. For Python, cold start is dominated by `deserialize_file` (mmap + relocation of a 33 MB artefact) and the first `run`. Pooling engine setup adds about 0.15 ms and buys nothing in a one-shot process.

## Determinism probes

Source: `probes.jsonl` (`build:"a"` and `build:"b"`; `b` is an independent build: Python and TS rebuilt, Rust from a clean `cargo` target). Each probe runs on 2 fresh instances.

| | Python | TS | Rust |
|---|---|---|---|
| random across instances (one build) | **identical** (`random.random()` → 0.89687…, 0.28387…, 0.94977…) | **identical** (`Math.random()` → 0.48402…, 0.86622…, 0.18233…) | n/a: std randomness **traps** (`unknown import wasi:random/insecure-seed@0.2.12`) |
| random across builds | **differs** (build b: 0.51160…) | **identical** (build b: same 0.48402…) | n/a (traps) |
| time | `time.time()` **traps** (`unreachable` in `libc.so!wall_clock_now`) | `Date.now()` = **frozen build timestamp** (a: 1795022450603, b: 1795022948628, which are the pre-init times of each build) | `SystemTime::now()` **traps** (`unknown import wasi:clocks/wall-clock@0.2.12#now`) |
| hash / set order | `hash("ostro")` baked per build: a 1394312507, b 91299055; identical across instances. Set order identical within this pair (c,b,a,d) | `Object.keys` insertion order `["2","10","b","a","c"]` (spec-defined, identical) | `HashMap::new()` **traps** (`insecure-seed`) |
| uuid / crypto | `uuid.uuid4()` **traps** (`unreachable` in `libc.so!random_get_random_bytes`) | `crypto.getRandomValues` → identical bytes `[204,112,232,123,…]` across instances and builds | n/a (no uuid in the probe) |
| `run-reseeded` without a host seed, twice | identical draws | identical draws | identical draws |
| **host reseed: seed A twice gives equal draws** | **yes** (`same_host_seed_same_draws: true`) | **yes** | **yes** |
| **host reseed: seed A vs seed B gives different draws** | **yes** (`different_host_seed_different_draws: true`) | **yes** | **yes** |

**Open question answered: do componentize-py/js reseed randomness after pre-init?** **No.**
- **componentize-py 0.25.1 `--stub-wasi`:** the RNG state, `hash()` secret and set order are captured in the pre-init snapshot. They are the same in every instance of a build and different in every build. Time and OS randomness trap.
- **componentize-js 0.23.0 `--disable all`:** `Math.random()` and `crypto.getRandomValues()` use a fixed seed that is identical even across builds. `Date.now()` returns the snapshot time.
- **Neither is a usable source of per-run entropy, and neither is non-deterministic at run time.** Replay of a pinned module sha is therefore stable. But every run would see the same "random" values.
- **Host-driven reseeding at activation start works in all three languages.** The guest calls `random.seed(context.random_u64())`, overrides `Math.random`, or seeds its own PRNG.

**Ambient sources the SDKs (Phase 9/10) must patch or reseed from `ctx` at activation start:**
- **Python:**
  - `random.seed(ctx.random_u64())` (module-level `random` and any `random.Random()` instances created at import time).
  - Replace `time.time`, `time.time_ns`, `time.monotonic`, `datetime.datetime.now/utcnow/today`, `datetime.date.today`, `uuid.uuid1/uuid4`, `os.urandom`, and `secrets.*` (via `os.urandom`).
  - `hash()`/set iteration order is baked per build. It is deterministic for a pinned sha, but must never feed command arguments unsorted.
- **TS/JS:** replace `Math.random`, `crypto.getRandomValues`, `crypto.randomUUID`, `Date.now`, `new Date()` (no-arg), and `performance.now`.
- **Rust:**
  - The host must provide deterministic `wasi:random/insecure-seed` (seeded from `RunStarted.rng_seed`, so std `HashMap` works) and `wasi:clocks/wall-clock`/`monotonic-clock` (virtualised, per-step-journaled time).
  - The SDK exposes `ctx.now()`/`ctx.random()`.
  - Alternatively, the SDK can steer users to fixed-hasher maps.

## Step handle

Source: `probes[<g>/a].run` and `probes[<g>/a].run_dropped_step`. `run` does a resource round trip (`begin-step` → `execute(step)` → `seq` → `complete`) and then the u64 round trip (`begin-step-u64` → `complete-u64`).

| | Python | TS | Rust |
|---|---|---|---|
| `resource step` round trip (`run` output) | works: `resource_seq: 1` | works: `resource_seq: "1"` (u64 arrives as BigInt) | works: `resource_seq: 1` |
| u64 fallback round trip | works: `u64_seq: 2` | works: `u64_seq: "2"` (BigInt) | works: `u64_seq: 2` |
| `step_completes` in `run` | 2 | 2 | 2 |
| completed step dropped in `run` (`step_drops`) | 1 (`with start.value as step:` exits) | **0**: never dropped without explicit dispose (leaks until GC/teardown) | 1 (Rust `Drop`) |
| dropped without `complete` (`run_dropped_step`) | `step_drops: 1`, `step_completes: 0` (explicit `step.__exit__`) | `step_drops: 1` **only with explicit `Symbol.dispose`** (`explicit_dispose: true`) | `step_drops: 1` (automatic `drop(step)`) |
| live resources in table after the export returns | 0 | 0 | 0 |
| binding ergonomics | class with context-manager protocol; variant is `journal.StepStart_Execute`/`_Replayed` | class with `[Symbol.dispose]`; variant is `{tag:'execute', val}`; works with TS 5.2+ `using` | owned `Step` with RAII `Drop`; variant is `StepStart::Execute(step)` |

Comparison with the u64 fallback:
- Both shapes round-trip in all three languages, so both are viable.
- **u64:** the host keeps a per-activation `handle → step` map and must validate every handle. A guest can pass a stale, forged or already-completed handle, and nothing marks a step abandoned. Bindings are plain integers (BigInt in JS).
- **resource:** the canonical ABI enforces ownership. Handles are per-instance table indices, cannot be forged across instances, and `complete` after a drop is a trap rather than a silent mis-route. `HostStep::drop` gives an abandonment signal when the SDK uses RAII. Store teardown reclaims everything regardless (live resources = 0 in every run).
- **Caveat for both:** the drop signal is **not reliable** without SDK discipline. TS never drops a completed step on its own, and Python only does so inside `with`. **The engine must not depend on drop timing for correctness.** An activation that ends with an uncompleted step is handled by activation teardown, not by `drop`. The SDKs wrap steps in `with` (Python), `using`/`try…finally` + dispose (TS) and RAII (Rust).

**Recommendation: resource.** It works in all three languages and gives typed, unforgeable, ownership-checked handles at no measurable cost: Rust steady `run`, which includes both round trips, is 0.9 µs. The drop hook is a useful observability signal when the SDK wraps steps correctly. The u64 shape adds host-side validation work and no ergonomic gain. Keep `begin-step-u64` out of WIT 0.1.

## Rust guest imports under wasm32-wasip2

Source: `imports.jsonl`. Python and TS import only `ostro:spike/journal@0.1.0` and `ostro:spike/context@0.1.0`. Python additionally exports componentize-py's internal `exports` interface (`init(app-name, symbols, stub-wasi)`).

The Rust wasip2 guest (wit-bindgen 0.63, Rust 1.100.0, `cdylib`, std) imports 16 `wasi:*@0.2.12` interfaces, even though `run`, `run-reseeded` and `run-dropped-step` make no WASI calls:

```
wasi:io/poll            wasi:io/error             wasi:io/streams
wasi:clocks/monotonic-clock                       wasi:clocks/wall-clock
wasi:cli/stdout         wasi:cli/stderr           wasi:cli/stdin
wasi:cli/environment    wasi:cli/exit
wasi:cli/terminal-input wasi:cli/terminal-output
wasi:cli/terminal-stdin wasi:cli/terminal-stdout  wasi:cli/terminal-stderr
wasi:random/insecure-seed
```

The research outline expected 17. This build imports 16; no `wasi:filesystem/*` and no `wasi:random/random` appear.

Implications:
- **ENG-02 deploy-time import validation:** a std Rust guest cannot be validated against "only `ostro:*`". The validator needs an allow-list for exactly these interfaces at a pinned 0.2.x version range. All of them must be satisfied by Ostro's own deterministic WASI 0.2 shim (Research 3 decision):
  - stdio goes to the bounded guest log;
  - `environment` is empty;
  - `exit` becomes a trap / attempt failure;
  - clocks are virtualised;
  - `insecure-seed` is seeded from the run seed.
- The shim must be complete enough that a plain std `HashMap` works. Today, an undefined `insecure-seed` traps on `HashMap::new()`.
- **SDK-01 (Rust SDK):** staying on std with the shim is the pragmatic path. A `no_std` or WASI-free build would be needed only if Ostro wanted zero WASI imports, which would cost users std collections and formatting conveniences.

## Reproducibility

Source: `repro.txt`. Rust builds are clean (`cargo clean --release --target wasm32-wasip2` before each).

| Build set | shas | sizes | verdict |
|---|---|---|---|
| Python r1–r3 (`PYTHONHASHSEED` unset) | 3 different | 18,576,677 / 18,537,576 / 18,572,612 | **not reproducible** |
| Python h1–h3 (`PYTHONHASHSEED=0`) | 3 different | 18,552,343 / 18,551,328 / 18,551,971 | **not reproducible**. The seed narrowed the size spread in this run (about 1 KB vs about 39 KB), but an earlier, discarded repro pass with the seed spread 16 KB, so the seed does not fix it |
| TS r1–r3 | 3 different | 12,017,259 / 12,017,240 / 12,017,296 | **not reproducible** |
| Rust r1–r3 | 3 identical (`12e27071…`) | 81,981 | **reproducible** |

Section attribution (by `spikes/s2-guests/wasm_sections.py`, a dependency-free component walker; no wasm-tools needed):
- **Python:**
  - Most differing bytes are in the main core module's **data section** (`/4:core-module/9:data`, about 6 MB of 11.2 MB differ, length varies). This is the pre-init (wizer-style) heap snapshot, which embeds the per-build RNG state, the hash secret and allocation layout.
  - The small shim/adapter core modules (`/5`–`/13`) also differ in type/function/export/code sections, with lengths swapped between positions. The **order** in which componentize-py emits these modules appears to change per build, which suggests hash-map iteration order inside the componentize-py tool itself.
  - `PYTHONHASHSEED=0` does not remove either effect.
- **TS:** the main core module's **data section** (`/4:core-module/10:data`, about 2.1 MB of 2.76 MB) differs, plus 8–10 bytes in its code section. Both reflect the snapshot, which embeds at least the build timestamp `Date.now()` observed in the probes.

Implications:
- **CLI-04 / SDK-04 (hermetic `ostro build`):** Rust is reproducible today. Python and TS need either a deterministic pre-init (fixed time, fixed seed, no hash randomisation in the snapshot, deterministic module ordering), which means upstream patches or a post-processing canonicaliser, or for Ostro to stop promising bit-for-bit reproducibility for interpreted guests and pin replay to the deployed module sha instead.
- **Replay safety is unaffected:** replay always uses the exact deployed artefact.

## Comparison with the research preliminary numbers

The preliminary run used on-demand allocation, in-memory `Component::deserialize`, and Python/TS only. This run uses `deserialize_file`, pooling as well as on-demand, and adds Rust.

| Metric | Preliminary (research) | This run | What changed |
|---|---|---|---|
| Python deserialize | about 20 ms (in-memory) | 2.67 ms (`deserialize_file`) | mmap avoids copying the 33 MB artefact (about 7.5× faster) |
| TS deserialize | about 18 ms | 0.96 ms | same, about 19× faster |
| Python first instantiate | 6.3 ms | 0.84 ms | likely the mmap'd CoW image avoiding the in-memory copy on first instantiate (not isolated separately) |
| TS first instantiate | 3.9 ms | 65 µs (on-demand), 50 µs (pooling) | same |
| Python steady p50 / p99 | 435 / 634 µs | 440 / 709 µs (on-demand), 454 / 648 µs (pooling) | unchanged: Python cost is core-instance count, not memory init |
| TS steady p50 / p99 | 9 / 25 µs | 19.5 / 36.8 µs (on-demand), 10.2 / 20.0 µs (pooling) | pooling matches the preliminary figure; on-demand is about 2× slower here (n=1000 vs small n, concurrent load) |
| first call of `run` | Python 1.0–1.3 ms, TS 0.7–0.9 ms | Python 0.88–0.90 ms p50, TS 0.59–0.60 ms p50 (per fresh instance) | consistent |
| RSS per live instance | Python about 5.5 MB, TS about 2.4 MB | Python 6.5 MB, TS 2.4–2.5 MB | Python about 1 MB higher (n=50 here; the preliminary n and timing are unrecorded) |
| `.wasm` sizes | Python 18,508,365, TS 12,014,255 | Python 18,499,437, TS 12,017,241 | per-build variation (see Reproducibility) |
| compile peak RSS | about 500 / 545 MB | 470 / 532 MB | consistent |
| TS builds | 2-byte size difference | sizes differ by up to 56 B | consistent: not reproducible |

## Open items for later phases

- **Phase 3 ENG-08:** pool sizing per guest mix (38 core instances per Python instance). Decide whether to use on-demand for Python, since pooling gives it no latency benefit. Set the `max_memory_size` floor (13.2 MB for Python) plus growth headroom.
- **Phase 3:** measure instantiate + first call with a realistic workflow, and confirm NFR-2.4 (start to first step < 5 ms) including the about 0.9 ms Python first-call cost.
- **Phase 3/4 ENG-02:** the WASI 0.2.x allow-list (16 interfaces above) and a deterministic shim complete enough for std `HashMap`.
- **Phase 4 deploy:** about 1 s and about 0.5 GB peak RSS per Python/TS AOT compile. Bound concurrent compiles. About 33 MB `.cwasm` per interpreted module version.
- **Phase 9/10 SDKs:** reseed and patch every ambient source listed above. Wrap steps in `with` / `using` / RAII. Never rely on `HostStep::drop` for correctness.
- **Phase 10 CLI-04:** reproducible Python/TS builds need a deterministic pre-init or a canonicaliser. Python also has non-deterministic shim-module ordering.
- **Phase 9:** re-measure Python per-instance memory with a realistic stdlib footprint before finalising the "Python embedded by default" recommendation.
- ~~**Plan 01-08:** quiet-machine confirmation of instantiate p50 and cold start, appended below.~~ Done.

## Confirmation run (quiet machine, plan 01-08)

Run on 2026-11-18 with `N_INST=1000 N_COLD=10 spikes/s2-guests/measure.sh confirm`, which writes `confirm.jsonl`. No other executors were running, and the S3 confirmation had finished before it started.
- Same wasmtime 50.0.2 harness and the same AOT artefacts (`spikes/out/<g>.cwasm`) as the main run.
- Both allocators were measured: pooling first (the better default for TS and Rust per the main run), then on-demand. Python pooling again used `total_core_instances = 1900`.
- **Load average:** 0.18–0.24 (1-min) throughout, against 1.06–1.07 for the main run (nproc 12).

| | Python on-demand | Python pooling | TS on-demand | TS pooling | Rust on-demand | Rust pooling |
|---|---|---|---|---|---|---|
| steady instantiate p50, **confirm** | **440 µs** | **445 µs** | **19.4 µs** | **9.9 µs** | **16.5 µs** | **9.1 µs** |
| steady instantiate p50, main run | 440 µs | 454 µs | 19.5 µs | 10.2 µs | 15.7 µs | 9.2 µs |
| steady instantiate p99, confirm | 670 µs | 658 µs | 36.9 µs | 19.3 µs | 35.2 µs | 20.4 µs |
| first call per fresh instance p50, confirm | 893 µs | 879 µs | 590 µs | 590 µs | 5.6 µs | 7.5 µs |
| cold start in-process median (n=10), **confirm** | **5.07 ms** | **5.27 ms** | **2.87 ms** | **2.86 ms** | **0.58 ms** | **0.77 ms** |
| cold start in-process median (n=20), main run | 5.07 ms | 5.35 ms | 2.47 ms | 2.69 ms | 0.72 ms | 0.80 ms |
| cold start process wall median, confirm | 8.76 ms | 8.91 ms | 6.39 ms | 6.08 ms | 3.19 ms | 3.46 ms |

**Verdict:** the quiet machine confirms every main-run number within run-to-run noise:
- Instantiate p50 is within ±5% in all six cells.
- Cold start is within ±0.4 ms. TS is about 0.3 ms slower and Rust on-demand about 0.15 ms faster, both well inside the spread of 10–20 single-process samples.

**The NFR-2.3 conclusions are unchanged:** the target is met in all six cells by more than 20x for Python (< 10 ms) and by 50–100x for TS and Rust (< 1 ms). The memory, pool-sizing and step-handle conclusions above did not depend on load and were not re-measured.
