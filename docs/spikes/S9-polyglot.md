# Spike S9: polyglot toolchains (Python, TypeScript on QuickJS and StarlingMonkey)

Status: complete (plan 09-04, Wave 0 of Phase 9). Run on 2026-11-21 on the WSL2 dev machine (AMD Ryzen 9 7950X3D, 12 vCPUs), while other Phase 9 executors were building: **every timing here is advisory (D15)**. Each JSON line carries its own `loadavg` (the final measurement run saw a 1-minute load of 8.9–10.8).
Apparatus: `spikes/s9-polyglot/` (standalone harness with wasmtime =50.0.2 and wasmparser =0.261.0 that path-depends on `ostro-engine` with `testkit`; guests; `run.sh`).
Raw data:

| File | Contents |
|---|---|
| `docs/spikes/data/S9/measurements.jsonl` | one JSON line per measurement or probe: `cmd` (`inspect`, `pool`, `instantiate`, `aot`, `fuel`, `engine`, `probe`), `guest`, `backend`, optional `probe` tag, values, `loadavg`, `wasmtime` |
| `docs/spikes/data/S9/repro.txt` | 3 plain and 3 wrapper builds each of `py-base` and `ts-qjs`: sha256, lengths, `cmp -l` counts, a walk over every section and data segment, timestamp hits, and the snapshot hash seed of each Python build |

Citations below look like `m[cmd/guest]`, or `m[probe=name/guest/workflow]` for probe lines. Toolchains: componentize-py 0.25.1 (`--stub-wasi`, embedded CPython 3.14.0), jco 1.42.0 with componentize-qjs 0.5.0 (`--backend quickjs --backend-qjs-disable-async --backend-qjs-stub-wasi`) and componentize-js 0.23.0 (`--disable all`), Rolldown 1.3.3 with `transform.target = "es2016"`, Node 24.13.0, uv 0.7.15.
`sdks/scripts/build-guest.sh` (09-02) was not on main when this ran, so `run.sh` inlines the same pinned commands.

Guests:
- `py-asyncio`: the raw-binding Python guest `py/app_asyncio.py`. At module level it imports what a real SDK pulls in: asyncio, contextvars, dataclasses, datetime, json, random and uuid.
- `py-base`: the same file with that import block stripped.
- `py-loop`: `py-asyncio` plus `loop_proto.py`.
- `ts-qjs` and `ts-sm`: `ts/probe.ts` (the SdkPromise prototype plus the probe workflows) on QuickJS and on StarlingMonkey.

## Summary

- **D-P9-1 is confirmed: QuickJS stays the TS default.** It wins on every density metric. It costs 13 core instances, 3 memories and 3 tables per instance, which is far more than the 3 in the class table, and it has a narrower set of globals and result shapes. All three costs are cheap to handle in the SDK and the pool (see "Consequences for plans").
- **D-P9-2 is confirmed: durable asyncio timers work.** The OstroLoop prototype maps `call_later`/`call_at` to `sleep-until` and `TimerHandle.cancel()` to `cancel-op`. It passes 10 pytest cases on host CPython 3.12. Its `selfcheck()` gives identical results inside the embedded CPython 3.14.0.
- **The TS SdkPromise loop runs on the real `ostro-engine` on both backends.** The workflow is `Promise.all` over two locked steps, then `Promise.race(sleep 1h, wait-signal "go")`, then a third step. It completes in 2 activations with the same 10-event journal on QuickJS and StarlingMonkey. Neither build imports a `wasi:` interface, and replay appends nothing.
- **D-P9-3 step 1 (the wrapper) is not enough for either toolchain.**
  - Python changes in about 1,500 sections and data segments on every build. The main cause is that the host `PYTHONHASHSEED=0` never reaches the pre-init interpreter.
  - QuickJS keeps a residual of 85–189 bytes in 9–18 snapshot segments, with or without the wrapper.
  - 09-17 has to go to step 2 (the fork) for Python. QuickJS needs its own fix.
- **Python activation fuel is negligible.** The hello workflow uses 185k–417k fuel, which is 0.002–0.004% of `engine.activation_fuel` (10,000,000,000). No `[USER REVIEW]` todo is needed.
- **New pitfalls for the SDK plans:**
  - A Python module imported for the first time inside `run` traps, because `--stub-wasi` leaves no filesystem.
  - A JS `Error` escaping `run` traps on both backends. That is `AttemptFailed`, so the activation is re-dispatched; it is not a run failure.
  - QuickJS has no `TextEncoder`, `crypto`, `console`, `setTimeout` or `structuredClone`.
  - On StarlingMonkey, a run that returns with a begun but uncompleted step completes live and fails replay with NonDeterminism.

## Resource measurements (Task 1)

Final run: `m[inspect|pool|instantiate|aot|fuel/<guest>]`. The pool is sized from the module: `pool` bisects the smallest `max_core_instances_per_component` and `max_memory_size` that instantiate and call `describe`, then holds N=50 live instances, and the 51st must fail. Instantiation uses the pooling allocator and an AOT artefact mapped with `deserialize_file`. The engine flags match Ostro's (NaN canonicalisation, deterministic relaxed SIMD, fuel, epoch interruption).

| Metric | py-asyncio | py-base | ts-qjs | ts-sm |
|---|---|---|---|---|
| `.wasm` size | 21.82 MB | 18.36 MB | **1.70 MB** | 12.36 MB |
| core instances (static `Instantiate` entries / pool minimum) | 38 / 38 | 38 / 38 | **11 / 13** | 3 / 3 |
| core modules / nested components | 38 / 2 | 38 / 2 | 11 / 24 | 3 / 1 |
| memories / tables per instance | 1 / 2 | 1 / 2 | **3 / 3** | 1 / 2 |
| declared memory floor (= pool minimum `max_memory_size`) | 15.69 MiB | 12.31 MiB | **1.56 MiB** | 9.06 MiB |
| RSS per live instance (n=50, pooling) | 3.78 MB | 3.40 MB | **0.61 MB** | 2.67 MB |
| instantiate p50 / p90 (200 fresh stores) | 451.5 / 482.2 µs | 466.3 / 516.0 µs | 66.2 / 80.8 µs | 12.3 / 14.2 µs |
| first `describe` on a fresh instance p50 / p90 | 397.2 / 424.7 µs | 482.9 / 521.6 µs | 211.5 / 245.8 µs | 750.1 / 814.4 µs |
| AOT compile / `.cwasm` | 1.18 s / 43.4 MB | 1.28 s / 39.9 MB | **0.39 s / 5.7 MB** | 1.46 s / 43.3 MB |
| fuel: `describe` / `run(hello)` | 120k / 185k | 120k / 417k | 302k / 778k | 2.48M / 2.54M |

Notes:
- The two earlier runs in the same session (load 10–18, not kept in the data file) agree within noise. Python first-describe p50 ranged 323–483 µs, and py-base RSS ranged 2.88–3.40 MB. Instantiate p50 was stable at 452–477 µs for Python, 62–66 µs for QJS and about 12 µs for SM.
- RSS here is lower than S2's 6.5 MB for Python. S2 measured on-demand instances that had called `run`; here they are pooled instances that called `describe` only. The `.cwasm` sizes are larger than in S2 because fuel and epoch instrumentation is on, as in production.
- The SC5 targets (< 10 ms Python, < 5 ms TS) hold even when instantiate and first call are added together: about 0.9 ms for Python, 0.28 ms for QJS and 0.76 ms for SM.

### Question 5: QuickJS core instances and pool floor

- **The component has 11 static core `Instantiate` entries spread over 24 nested components, but wasmtime needs `max_core_instances_per_component = 13`.** That figure is the bisected pool minimum, `m[pool/ts-qjs].min_core_instances_per_component`.
- QJS also uses **3 memories** (one of 25 pages and two of 0 pages) and **3 tables**. Python and SM use 1 memory and 2 tables.
- The memory floor is 1,638,400 B (25 pages).
- **The class-table assumption "3 for TS" holds for SM only.**
- Today's default pool is `LanguageClass::Rust`: 1000 component instances × (3 core instances, 2 tables, 1 memory). That admits only ⌊3000/13⌋ = **230 live QJS instances**, or ⌊3000/38⌋ = 78 Python instances. Memories (1000 → 333 QJS) and tables (2000 → 666 QJS) are the next limits after core instances.

### Question 6: Python snapshot growth with asyncio and the SDK import set

From `py-base` to `py-asyncio`:

| Metric | Change |
|---|---|
| `.wasm` size | **+18.8%** (18.36 → 21.82 MB) |
| memory floor | +27.5% (12.31 → 15.69 MiB) |
| RSS per instance | +11% in this run, +21–30% in earlier runs |
| instantiate p50 | unchanged within noise (451–477 µs both) |
| `.cwasm` | +8.9% |
| core instances | unchanged (38) |
| hello fuel | lower (185k vs 417k) |

- Hello fuel goes down because initialisation work moves into the snapshot.
- Both fallback thresholds in 09-DECISIONS ("> +50% size or > 2x instantiate") are far off, so **the SDK keeps real asyncio**.
- `dataclasses` and `random` are already in `sys.modules` even in `py-base`, because the bindings and runtime import them (`m[probe=py_loop/py-base/classes]`).

## Behaviour probes (Task 2)

### Question 9 and Rolldown lowering coverage

`ts/lowering_fixture.ts` exercises:
- async functions, async arrows and async methods;
- an async generator consumed with **for await**;
- `try/catch` around an awaited throw;
- `Promise.allSettled` and `Promise.any`;
- a local dependency `dep.ts`, built once with a top-level await and once without.

Results (`m[probe=lowering/*]`):
- **Everything except top-level await lowers.** The dumped bundle has **zero leftover `async`/`await` tokens**. The `lowering` workflow returns `{"d":10,"t":15,"s":3,"w":12,"settled":["fulfilled","rejected"],"any":"any-ok","caught":"async throw"}` on the real engine on both backends: 6 steps, 1 activation, and replay appends nothing.
- **Top-level await is not lowered, but it is not fatal either.** Rolldown prints `[TOLERATED_TRANSFORM] Top-level await is not available in the configured target environment.` and leaves `await` in the bundle. The build exits 0, and both QJS and SM evaluate the TLA at pre-init (`dep` = `"dep-tla"`).
- Caveat 1: the TLA ran with the *native* Promise, because `dep.ts` is evaluated before the SDK installs `SdkPromise`.
- Caveat 2: a TLA on something that cannot settle during pre-init would hang or fail the build. This was not exercised.

### TS SdkPromise prototype on the real engine (`m[probe=ts_engine/*]`)

`probe.ts` follows the 09-RESEARCH Pattern 2 shape:
- `SdkPromise` is installed as `globalThis.Promise` at module evaluation.
- Ops are thenables, so `then` registers interest.
- A synchronous `run` drives one `effects.wait(sorted ops + cancellation, any)` per quiescent point.
- Steps go through a FIFO lock.

It ran on `ostro_engine::runtime::Runtime` with the production linker validation (`prepare_cached`), the testkit fake sink and a frozen FakeClock. Timers never fire, and the `go` signal is committed after the first eviction.

| | ts-qjs | ts-sm |
|---|---|---|
| validated / `wasi:` imports | yes / none | yes / none |
| activations | `Evicted{WaitUnsatisfied}`, then `Completed` | same |
| journal | RunStarted, StepCompleted×2, TimerStarted, SignalWaitStarted, SignalReceived, WaitResolved, OpCancelled, StepCompleted, RunCompleted | identical |
| output | `{"a":4,"b":6,"c":10,"winner":{"signal":{"go":true}},"promiseIsSdk":true,...}` | identical |
| replay | same outcome, 0 appended | same |
| fuel for the full run on the fake host | 2.39M | 6.53M |

### Question 7: the `result<run-result, failure>` export mapping (`m[probe=result_mapping/*]`)

| What `run` does | QuickJS | StarlingMonkey |
|---|---|---|
| returns the plain ok value `{tag:"completed", val}` | **Completed** | **Completed** |
| returns `{tag:"ok", val:{...}}` | trap (`unreachable`), `AttemptFailed` | Completed |
| returns `{tag:"err", val: failure}` | trap, `AttemptFailed` | Failed (err) |
| throws the `failure` variant value itself | **Failed (err)** | **Failed (err)** |
| throws `{tag:"err", val: failure}` | trap | trap |
| throws `new Error(..)` | trap | trap |

**Rule for 09-10:**
- On success, return the plain `run-result` value.
- On failure, throw exactly the WIT `failure` variant value.
- Catch everything else at the export boundary and convert it to a `failure` before throwing. An escaping JS error is an `AttemptFailed` trap: the run does not fail, and the host re-dispatches the activation.

### Question 8: lone surrogates and `isWellFormed` (`m[probe=surrogates/*]`)

- **`String.prototype.isWellFormed` and `toWellFormed` exist on both backends.** A lone surrogate gives `false` and a proper pair gives `true`, so `utf8.ts` needs no manual scan.
- QJS traps (`unreachable`, `AttemptFailed`) when a lone surrogate crosses the boundary. This happens both for a `begin-step` name and for a returned payload's content type.
- SM silently replaces the surrogate with U+FFFD (the content type came back as `text/x�`). This matches PF-W4 and the WIT comment.
- **Engine observation (SM `surrogate-step`):** the guest began a step (`bad�`), never completed it, and returned. Live, the run completed with journal `[RunStarted, RunCompleted]`. Replay of that journal failed with `NonDeterminism { seq: 1, expected: "run completed", actual: "step \"bad�\"" }`. An open step at return is therefore not rejected live. 09-07/09-13 should decide whether `run` returning with an open step must fail the activation. The SDKs must always close steps (Pitfall 1 "closed" flag).
- Backend globals (`m[probe=surrogates/*/env]`):
  - QJS has no `TextEncoder`, `TextDecoder`, `structuredClone`, `crypto`, `setTimeout` or `console`. It has `WeakRef`, `FinalizationRegistry`, `AggregateError` and `performance`.
  - SM has all of them.

### componentize-py export naming and library WIT (`m[probe=export_naming/*]`)

- The app module must expose **`Workflows`**, a subclass of `wit_world.exports.Workflows` (a Protocol). It is named after the exported interface `workflows`. Naming the class `WitWorld` fails the build with `AssertionError: AttributeError: module 'app' has no attribute 'Workflows'. Did you mean: 'workflows'?`.
- **A re-export is enough.** `app.py` containing only `from ostrolib import Workflows` builds and runs `hello`. 09-09's `export.py` can therefore ship the class in the SDK package.
- Module and type names:
  - The generated bindings module is `wit_world`; `--world-module` can change it.
  - Export types live in `wit_world.exports.workflows`: `WorkflowMeta`, `RunResult_Completed`, `RunResult_ContinueAsNew`, `Continuation`.
  - Imports live in `wit_world.imports.{types,context,journal,effects,log}`.
  - A `result` err is raised as `componentize_py_types.Err(failure)`. `Ok(..)` and `Err(..)` wrap `result` parameters such as `step.complete(Ok(payload))`.
- **Library WIT discovery:** a package on a `-p` path that contains `componentize-py.toml` with `wit_directory = "wit"` (next to a `wit/` copy) is found automatically.
  - The build succeeds without `-d` when `-w ostro:runtime/workflow@0.1.0` is given, and the app does not need to import the package.
  - It also succeeds without `-w` when the app imports the package.
  - The config struct also knows `bindings` (a pre-generated bindings path), `import_interface_names`, `export_interface_names` and `full_names`.
- `poll_loop.py` is generated by `componentize-py bindings <dir>` as `<dir>/poll_loop.py`, from a template bundled in the componentize-py wheel. It was read from there, not committed.

### Python OstroLoop prototype (`py/loop_proto.py`, `py/test_loop_proto.py`, `m[probe=py_loop/*]`)

The prototype is an `asyncio.AbstractEventLoop` shaped like `PollLoop`: drain the ready queue to quiescence, collect interested ops (awaited op futures plus live timers), sort them, and add `CANCELLATION`.

- Host CPython 3.12 with pytest 8.4.2: **10 passed**.
  - gather resolves in the returned order;
  - race returns the first returned completion and cancels the losing sleep through `cancel-op`;
  - `asyncio.sleep(3600)` produces exactly one `sleep_until` and one `wait`, with no cancel;
  - `wait_for(signal, 5)` produces `wait_signal, sleep_until, wait, cancel_op` on success and no cancel when it times out;
  - a bare Future raises `Deadlock`;
  - `CANCELLATION` rejects unshielded waits with `Cancelled`;
  - unsupported methods raise `NotImplementedError`;
  - the asyncio APIs it uses exist.
- Inside the embedded CPython **3.14.0** (`py-loop` guest, `loop-check` workflow): the API checks all pass, and the gather, sleep, wait_for and deadlock results are identical to 3.12.
- Findings that 09-09 must carry over:
  - `TimerHandle` has `__slots__`, so the op id needs a subclass with an extra slot.
  - `asyncio.sleep` calls `h.cancel()` in `finally` even after the timer fired. The loop must remember fired timers, or every sleep journals a spurious `cancel-op`.
  - `create_task` must accept `**kwargs`: 3.14 passes `eager_start` through.
  - **A module imported for the first time inside `run` traps** (`lazy-import` workflow: `wasm trap: unreachable`). With `--stub-wasi` there is no filesystem after pre-init, so the SDK must import everything at SDK import time.
- 3.14 differences that matter for the loop, from the CPython 3.14 "What's New" (not re-verified offline):
  - the policy system is deprecated (`get_event_loop_policy` and related, removal planned for 3.16);
  - `create_task` forwards extra keyword arguments to the Task (`eager_start`);
  - call-graph introspection (`capture_call_graph`) is new;
  - `asyncio.iscoroutinefunction` is deprecated.
  - None of these broke the prototype.
- ENG-1 is confirmed on the real engine: `prepare_cached(py-asyncio)` fails with `component validation failed: export exports is not allowed` (`m[probe=py_engine_eng1/py-asyncio]`). Python on the real engine waits for 09-07.

### Python activation fuel vs `engine.activation_fuel`

Fuel on a fresh instance, on the fake host (`m[fuel/*]`):

| Run | Fuel | Share of `activation_fuel` |
|---|---|---|
| Python hello (`py-asyncio` / `py-base`) | 185k / 417k | 0.0018% / 0.0042% |
| Python `loop-check` (four asyncio scenarios) | 1.32M | — |
| `describe` (Python) | about 120k | — |
| QJS hello | 0.78M | — |
| SM hello | 2.54M | — |

- `activation_fuel` is 10,000,000,000.
- Fuel is identical on a second fresh instance in every case (`fuel_identical_on_fresh_instance: true`).
- **A hello activation uses well under 10% of the default, so the decision table's `[USER REVIEW]` trigger does not fire.** 09-07 only documents this. Per-step `fuel` budgets must still be per language (PF-W6).

## Reproducibility under the D-P9-3 step-1 wrapper (Task 3)

Wrapper: sources are staged into a fresh temp dir with every mtime set to `SOURCE_DATE_EPOCH` (the HEAD commit time, 1795289796). `PYTHONHASHSEED=0 PYTHONDONTWRITEBYTECODE=1 TZ=UTC LC_ALL=C SOURCE_DATE_EPOCH` are exported. There are three builds per mode, at least 3 s apart (`repro.txt`).

| Build set | shas | Diff (section walk) | Timestamps in equal-length pieces |
|---|---|---|---|
| Python, plain | 3 different, lengths 18.36–18.42 MB | 1,446–1,512 differing pieces in data, type, function, export and code sections across many modules | 6 u32 (+0–4 f64) |
| Python, wrapper | 3 different, lengths 18.35–18.39 MB | 1,494–1,502 pieces, same picture | 6 u32 |
| QJS, plain | 3 different (1 pair same layout) | 9 segments with 85 B, or about 2,900 pieces with layout drift | 0 |
| QJS, wrapper | 3 different, **all the same length** | 9–18 snapshot data segments with **133–189 differing bytes** | 0 |

Causes:
1. **Python hash seed.** The snapshot's str hash seed is random on every build, wrapper or not. The `hash-probe` workflow shows `hash("ostro")` different in all 6 builds, `sys.flags.hash_randomization = 1`, `PYTHONHASHSEED` unset in the guest, and set order shuffled. componentize-py's pre-init `WasiCtxBuilder` sets only `PYTHONUNBUFFERED`, `PYTHONHOME` and `PYTHONPATH`, and the CLI has no env option. The wrapper's `PYTHONHASHSEED` therefore cannot reach the interpreter.
2. **Python module order.** Differing type, function and export sections in modules 1..N mean the core-module order still shuffles. `src/stubwasi.rs` collects the WASI stub adapters in a `HashMap` (RandomState) and adds them in iteration order.
3. **Python mtimes.** Six 4-byte LE Unix times (in data segment 25), plus some f64 `st_mtime`s, differ by exactly the build gap. `SOURCE_DATE_EPOCH` never appears. These are mtimes of files componentize-py generates or rewrites per build: the `world` bindings tempdirs, the bundled helpers rewritten by `replace(.., "wit_world", module)`, and the tempdir roots. They are cached by importlib's `FileFinder` and source loaders. The wrapper fixes only the user's own sources.
4. **QuickJS.** The 133–189 B residual sits in snapshot data segments and contains no timestamps. In the first repro run (since overwritten), one 8-byte segment (`seg82`) differed in every byte, which looks like a PRNG or hash seed taken at componentize-qjs pre-init. The wrapper cannot reach it: identical lengths, but still different.

**Verdict for 09-17:**
- **The released componentize-py 0.25.1 is not bit-reproducible under the wrapper alone, so D-P9-3 step 2 (the pinned fork) is required.** The result matches the S2/research expectation, but the dominant cause is the hash seed and adapter order, not mtimes.
- **QJS is not reproducible under the wrapper either.** The residual is small and entropy-driven: file the componentize-qjs issue, and treat TS SC2 like Python's step 3 unless a QJS patch appears.
- Replay safety is unaffected, because replay always uses the deployed artefact.

### Fork estimate (componentize-py v0.25.1; source read from a shallow clone in `target/toolchains/componentize-py-src`)

The minimal patch touches 3 files, about 40–80 lines:

| Location | Change |
|---|---|
| `src/lib.rs:674–691` and `:811–816` (pre-init `WasiCtxBuilder`) | Add `.env("PYTHONHASHSEED","0")` and `.env("PYTHONDONTWRITEBYTECODE","1")`; deterministic `secure_random`, `insecure_random` and `insecure_random_seed`; fixed `wall_clock` and `monotonic_clock`. |
| `src/lib.rs:729, 752` (world-dir tempdirs), `:761–777` (in-place `replace` of the bundled helpers) and `src/prelink.rs:31–57` (stdlib and bundled extraction tempdirs) | Set every file and directory mtime, tempdir roots included, to a constant (`SOURCE_DATE_EPOCH`, default 0) after writing or extracting. |
| `src/stubwasi.rs:15, 32` | Replace `HashMap` with `BTreeMap` so the adapter order is deterministic. |
| `build.rs:294–306` (`add`, tar of the stdlib and bundled) | Optional: normalise tar header mtimes. Without it, the extracted-stdlib mtimes stay constant per fork binary but change whenever the fork is rebuilt elsewhere, so the cache key has to be the artefact, not the source. |

Building from source needs:
- Rust ≥ 1.94 with the `wasm32-wasip2` target (the repo pins 1.100);
- wasi-sdk **33** (`build.rs` `DEFAULT_SDK_VERSION`, auto-downloaded or taken from `WASI_SDK_PATH`);
- the CPython source tarball `dicej/cpython` tag **`v3.14.0-wasi-sdk-30`** (auto-downloaded by `build.rs`), built natively first and then for wasm32-wasip2, with zlib and sqlite;
- a host C toolchain (make, clang/gcc) and network on the first build.

Estimate (not measured here):
- disk 4–6 GB: wasi-sdk about 1.5 GB, CPython source and both builddirs about 1.5 GB, cargo target 2–3 GB;
- 15–30 min cold on 12 vCPUs.

09-DECISIONS already plans to cache by patch sha in CI. That works with the build.rs mtime normalisation; without it, the cache key has to be the fork artefact.

## D-P9-1 and D-P9-2

**D-P9-1 (TS backend): confirmed, QuickJS is the default.**

| Metric | QJS | SM |
|---|---|---|
| `.wasm` | 1.70 MB | 12.36 MB |
| RSS per instance | 0.61 MB | 2.67 MB |
| memory floor | 1.56 MiB | 9.06 MiB |
| AOT | 0.39 s / 5.7 MB | 1.46 s / 43.3 MB |
| first `describe` | 212 µs | 750 µs |
| hello fuel | 0.78M | 2.54M |
| repro residual | 133–189 B | 1.6 MB in the research |

- Same SDK source on both backends, with identical journals and outputs.
- QJS costs, each handled by the SDK or the pool:
  - 13 core instances, 3 memories and 3 tables per instance (the pool must count them);
  - no web globals (the SDK ships UTF-8, sha256 and console shims);
  - a stricter result mapping (the SDK rule above);
  - traps on lone surrogates (the `isWellFormed` pre-check).
- SM stays the fallback, as decided.

**D-P9-2 (asyncio timers): confirmed, durable mapping.** The prototype shows the mapping is small and deterministic on 3.12 and 3.14.0. It needs three implementation details: fired-timer tracking, a `TimerHandle` subclass slot, and `create_task(**kwargs)`.

**Recommended amendments for 09-20 to fold into 09-DECISIONS (not edited here):**
1. D-P9-17 / ENG-2: the class table entry "3 for TS" holds for SM only. QJS needs **13 core instances**, and each module also needs its **memories (3)** and **tables (3)** counted. Per-module weights should cover all three pool dimensions, or `total_memories` and `total_tables` must be sized at the largest per-module count. `LanguageClass::TypeScript::min_memory_bytes` (9,502,720, the SM figure) is 5.8× the QJS floor.
2. D-P9-3: step 1 alone is not enough for Python (hash seed and adapter order). 09-17 should plan for step 2 from the start. The fork patch should also cover `stubwasi.rs` and the generated-file and tempdir mtimes.
3. Add to the SDK rules (09-09/09-10): import everything at SDK import (no lazy imports in Python); JS export-boundary catch-all (Error → `failure`); the QJS globals gap.

## Consequences for plans

- **09-07 (engine/pool):**
  - Weigh admission by measured core instances: Python 38, QJS 13, SM 3, Rust 3.
  - Size or weigh memories (QJS 3) and tables (QJS 3) as well.
  - The default Rust-class pool fits only 230 QJS or 78 Python live instances.
  - The Python hello activation is 0.004% of `activation_fuel`: document it, no review todo.
  - ENG-1's `exports` export is confirmed as the only blocker for Python on the real engine.
  - Decide whether returning from `run` with an open step should fail the activation (SM observation above).
- **09-09 (Python SDK):**
  - `export.py` provides `class Workflows(wit_world.exports.Workflows)`; apps re-export it, which is verified to work.
  - Ship `componentize-py.toml` with `wit_directory = "wit"`.
  - Use the OstroLoop shape from `loop_proto.py`, including the three details above.
  - Import every module the SDK needs at import time. A first import inside `run` traps.
- **09-10 (TS SDK):**
  - Return the plain `run-result`; throw only the `failure` variant; catch-all at the export.
  - Use `isWellFormed` for the string check; it exists on both backends.
  - Ship the UTF-8 codec, a sync sha256 and a `console` → `log.emit` shim for QJS.
  - Fail the build on leftover `async function`/`async (`/`await` outside TLA.
  - Document TLA as unsupported: it is tolerated by Rolldown and runs on the native Promise at pre-init, and it is not lowered.
- **09-13 (conformance and fuel docs):** per-language fuel numbers start from the `fuel` lines here. The SM open-step replay divergence belongs in the children/edge bug list if 09-07 does not take it.
- **09-17 (repro ladder):**
  - Step 1 measured, not sufficient. Go to step 2 with the patch locations above.
  - Estimate 4–6 GB and 15–30 min per cold fork build, cached.
  - File the componentize-qjs issue for the 8-byte seed residual.
  - SM stays out of SC2.
