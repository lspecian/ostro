# Phase 3: Deterministic Wasm Engine & WIT 0.1 - Research

**Researched:** 2026-11-18
**Domain:** Wasmtime 50 component host, deterministic replay, Wasm sandboxing, the `ostro:runtime@0.1.0` WIT contract
**Confidence:** HIGH for Wasmtime API facts and spike measurements (read from the 50.0.2 source in `~/.cargo/registry` and run locally). MEDIUM for the replay/journal design (built from first principles plus Temporal/Restate prior art, but not proven at scale).

## Summary

No CONTEXT.md exists for this phase, so nothing here is a locked user decision beyond what PROJECT.md, REQUIREMENTS.md, ROADMAP.md and `.planning/research/*` already settle. Phase 1 (decides step-handle shape and Wasmtime pin policy) is running at the same time, and its RESEARCH.md was not present when this was written.

The engine is buildable today with **Wasmtime 50.0.2 (`default-features = false`) using sync WIT functions implemented as async host functions on fibers.** A throwaway spike, run on 2026-11-18, verified these points end to end:
- A draft `ostro:runtime@0.1.0` world, including the research additions and a `resource step`, compiles on both sides: `wasmtime::component::bindgen!` on the host, and wit-bindgen 0.63 on a Rust guest built with `cargo build --target wasm32-wasip2`.
- `InstancePre` plus the pooling allocator instantiates a 97 KB Rust guest in **10 µs p50 / 62 µs p99**. The NFR target is under 1 ms.
- Fuel stops an infinite loop with `Trap::OutOfFuel`. An epoch deadline stops it with `Trap::Interrupt`. A `StoreLimits` memory cap stops a memory bomb. Deep recursion traps with `Trap::StackOverflow`. None of these took down the host process.
- **A host function that uses more than about 1.5 MiB of native stack while the guest is at maximum Wasm depth aborts the whole process** (SIGABRT, "execution on async fiber has overflowed its stack"). With the defaults (`async_stack_size` 2 MiB, `max_wasm_stack` 512 KiB), 1.4 MiB survived and 1.7 MiB aborted. This confirms Pitfall 7 and sets the shape of the ENG-06 test.
- A pooling engine with 64 slots at default 4 GiB reservations **fails `Engine::new` under `ulimit -v`** with "mmap failed to reserve 0x4082000000 bytes". Ostro must wrap this error with sizing guidance (ENG-08).

**The biggest surprise:** a plain Rust `wasm32-wasip2` guest on Rust 1.100 **always imports 17 WASI 0.2.12 interfaces**: io/poll, io/error, io/streams, clocks/wall + monotonic, random/random + insecure-seed, cli/stdin/stdout/stderr/environment/exit/terminal-*. This happens even when the guest never touches them, because std pulls them in, `HashMap`'s RandomState needs insecure-seed, and `rand` needs random. So ENG-02 is not optional for Rust. The host must link a deterministic WASI shim for exactly that set. Python (`--stub-wasi`) and TS (`--disable all`) guests import none of it.

**Primary recommendation:** Build `ostro-engine` from five parts:
1. **A Wasmtime 50 engine** with `default-features = false` (no gc/threads/stack-switching/component-model-async), NaN canonicalisation, deterministic relaxed SIMD, fuel and epochs. Raise `async_stack_size` to 4 MiB.
2. **An Ostro-owned deterministic WASI 0.2 shim** for the 17 interfaces, built on `bindgen!` over vendored WASI WIT. Do not use `wasmtime-wasi`.
3. **A pure-Rust replay core**: journal cursor, command hashing, wait ordering. It is independent of Wasmtime and covered by proptest.
4. **An AOT cache**: read the bytes, verify the HMAC, then `Component::deserialize`. Key it on sha256 + compat hash + Ostro version.
5. **An activation driver against a fake `ActivationSink`**. Eviction happens when a host import returns an `Evict` error, and the Store is then dropped.

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| ENG-01 | Wasmtime 50.x component host with NaN canonicalisation, deterministic relaxed SIMD, threads/GC disabled | §Standard Stack (feature list); §Code Examples "Engine config". Verified that with `default-features=false` and no `gc`/`threads` features, `Config::features()` strips GC_TYPES/EXCEPTIONS/THREADS and `validate()` refuses to enable them. `wasm_threads()` is not even callable without the `threads` feature. `relaxed_simd_deterministic` defaults to **false** and NaN canonicalisation defaults to **false**, so both must be set |
| ENG-02 | Deterministic WASI imports; deploy-time validation rejects anything else | §Architecture Pattern 3 (measured import list of a Rust 1.100 guest: 17 interfaces @0.2.12); semver-compatible linker lookup (verified in linker.rs docs); allow-list validation via `Component::component_type().imports()` + `Linker::instantiate_pre` |
| ENG-03 | Replay compares kind + canonical command hash; mismatch → DLQ reason `nondeterminism` with seq, expected vs actual | §Architecture Pattern 4 (replay cursor), Pattern 5 (command hash spec), §Pitfalls 2–3 |
| ENG-04 | Recorded results returned as exact journaled canonical bytes | §Pattern 5: results stored as versioned canonical bytes, decoded (never re-derived) on replay |
| ENG-05 | Memory caps, fuel per step/run, epoch slicing and deadlines; breach fails only that attempt | Spike verified `OutOfFuel`, `Interrupt` and memory-cap traps with no host impact. §Pattern 6 (per-step fuel accounting), §Code Examples (limiter, epoch callback) |
| ENG-06 | Max-stack guest calling a host function cannot abort the node | Spike measured the abort threshold (host budget ≈ `async_stack_size − max_wasm_stack`). §Pitfall 1, §Validation (binary-search depth test) |
| ENG-07 | AOT compile in bounded pool; cache keyed sha256 + compat hash + Ostro version; HMAC verified | `Engine::precompile_component`, `Engine::precompile_compatibility_hash` (returns `impl Hash`, hashes triple + flags + isa_flags + tunables + features + module_version). Incompatible engine → clean deserialize error (verified). §Pattern 7 |
| ENG-08 | InstancePre + pooling within NFR; configurable pool; clear failure under address-space limits | Measured 10 µs p50. Pooling defaults read from source (total 1000 slots, 4 GiB reservation + 32 MiB guard, 2 MiB stacks). `ulimit -v` failure reproduced. §Pattern 8 |
| ENG-09 | Hot-wait vs evict; node memory budget with LRU eviction | §Pattern 9. Dropping a `call_async` future disposes the fiber cleanly (verified in fiber.rs: resumes with "future dropped"). An import returning `Err(Evict)` unwinds the guest |
| WIT-01 | Sync world: context, journal, effects + `wait`, log, `workflows` export | §WIT 0.1 draft. Compiled on host and guest in the spike |
| WIT-02 | Guest-visible cancellation; terminate separate | §WIT draft: `effects.cancellation() -> op-id`, delivered through `wait`; terminate is host-only |
| WIT-03 | First-class continue-as-new | §WIT draft: `run` returns `run-result::continue-as-new(continuation)`; plus `context.continue-as-new-suggested()` |
| WIT-04 | Await tokens | §WIT draft: `effects.create-await-token(timeout) -> await-token{op, token}`; token = journaled opaque string embedding partition + run + op + MAC |
| WIT-05 | `set-status` | §WIT draft: `context.set-status(json)`; journaled, kind-only hash, not re-published on replay |
| WIT-06 | Non-retryable error kind + per-run timeouts | §WIT draft: `failure::non-retryable`, `failure::timeout`, `workflow-meta.run-timeout-ms`, `child-options.run-timeout-ms`, `run-info.deadline` |
</phase_requirements>

## Standard Stack

### Core

| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| `wasmtime` | **=50.0.2** (exact pin; 51.0.0-rc.1 is already on crates.io and releases ~2026-11-20) | Component host, Cranelift, pooling, fuel, epochs | Reference CM runtime. Pin exactly: monthly majors break APIs. For example, in 50 `imports()` yields `ComponentExtern { ty, .. }` rather than `ComponentItem`, and epoch callbacks take `StoreHookState` |
| `wit-bindgen` (guest) | **0.63.0** | Rust test guests' bindings | Verified with Rust 1.100 + `wasm32-wasip2`: produces a component directly, no `cargo-component` |
| `tokio` | 1.54 | Async runtime for host imports, timers, compile pool (`spawn_blocking` + `Semaphore`) | Already chosen |
| `sha2` + `hmac` | sha2 0.11.1 / hmac 0.13.0 (pick one sha2 major workspace-wide; spike used 0.10) | Module sha256, AOT HMAC-SHA256, stable hasher for compat hash, command hashes | RustCrypto, pure Rust |
| `rand_chacha` **or** `chacha20` | rand_chacha =0.10.0 / chacha20 0.10.2 | Per-run ChaCha20 streams | Pin exactly **and** add a golden-vector test, because the stream must never change |
| `postcard` | 1.1.3 | Canonical encoding of journal result bytes (Ostro-owned mirror types) | Deterministic for a fixed type. Version the codec explicitly (see Pattern 5) |
| `async-trait` | 0.1.92 | `dyn ActivationSink` (docs/03 seam) | Native async-fn-in-trait is not dyn-compatible |
| `thiserror` | 2.0 | Engine error taxonomy (`Evict`, `NonDeterminism`, `MemoryExceeded`, …) | Standard |

**Wasmtime features (verified against the 50.0.2 Cargo.toml):**
```toml
wasmtime = { version = "=50.0.2", default-features = false, features = [
  "runtime", "cranelift", "component-model", "async", "pooling-allocator",
  "std", "parallel-compilation", "addr2line", "demangle", "coredump",
] }
```
Defaults in 50 include `gc`, `gc-*`, `threads`, `stack-switching`, `component-model-async`, `debug`, `wat`, `wit-parser`, `compile-time-builtins` and `cache`. All of these are excluded on purpose:
- **`component-model-async` changes behaviour.** When it is on, `TypedFunc::call_async` routes through `call_async_concurrent` (`store.concurrency_support()`).
- **`cache` is Wasmtime's own global cache.** Ostro owns its cache.

### Supporting

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `proptest` | 1.12.0 | Property tests of the pure replay core (record → replay equality; single mutation → divergence at exact seq) | Replay core unit tests |
| `insta` | 1.49.0 | Golden journal snapshots (render journal as stable text/JSON) and `Config` debug snapshot | Golden tests |
| `tokio-util` | 0.7.20 | `CancellationToken` for external eviction of hot-waiting activations | ENG-09 |
| `lru` (or hand-rolled `IndexMap`) | latest | L2 `InstancePre` cache and resident-activation LRU | ENG-08/09 |
| `cargo-nextest` | 0.9.152 | Process-per-test isolation (abort tests must not kill the runner) | Required for the ENG-06 abort-safety test |

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Own WASI 0.2 shim | `wasmtime-wasi` 50.0.2 (`default-features=false, features=["p2"]`) with a custom `WasiCtxBuilder` (custom wall/monotonic clocks, `insecure_random_seed`, custom stdout) | Less code (~0 vs ~400 LOC). But its `subscribe-duration` pollables sleep in **real time** via tokio, so `std::thread::sleep(1h)` would park a fiber for an hour. It also pulls cap-std, rustix and fs crates, and makes determinism depend on upstream internals. Choose it only if the shim proves costly |
| `resource step` | `u64` step handle | See Pattern 2. **The decision belongs to Phase 1 S2.** Keep the host adapter thin so either shape is cheap |
| Fuel + epochs | Epochs only | Fuel costs up to 2–3x per the Wasmtime docs, but it is the only *deterministic* budget. ENG-05 requires per-step/run fuel, so keep both |
| Read-then-deserialize | `Component::deserialize_file` (mmap) | mmap opens a TOCTOU window between HMAC check and load. Read into memory, verify, then `deserialize(&bytes)` |

**Installation (inside the Phase 1 workspace):**
```bash
rustup target add wasm32-wasip2 --toolchain 1.100.0
cargo install --locked cargo-nextest wasm-tools@1.263.0   # wasm-tools only for CI import checks / debugging
```

## Architecture Patterns

### Recommended Project Structure
```
wit/
└── ostro-runtime-0.1.0/runtime.wit   # source of truth (repo root, shared with SDKs)
crates/ostro-engine/
├── Cargo.toml                 # depends on ostro-types only (no redb, no wal: worker path)
├── wit-wasi/                  # vendored WASI 0.2.x WIT (io, clocks, random, cli) ≥ 0.2.12
├── src/
│   ├── lib.rs
│   ├── config.rs              # EngineConfig (pool sizes, stacks, fuel, epoch tick) → wasmtime::Config
│   ├── engine.rs              # Engine construction, epoch ticker, pool-failure wrapping/fallback
│   ├── validate.rs            # deploy-time import/export allow-list
│   ├── cache.rs               # AOT L1 (disk, HMAC) + L2 (InstancePre LRU) + compile pool
│   ├── replay/                # PURE RUST, no wasmtime: cursor, command, hash, codec, wait ordering
│   │   ├── cursor.rs
│   │   ├── command.rs
│   │   ├── hash.rs
│   │   └── codec.rs
│   ├── host/                  # bindgen! glue: thin, forwards into replay core
│   │   ├── bindings.rs        # bindgen!(ostro world) + bindgen!(wasi shim)
│   │   ├── context.rs  journal.rs  effects.rs  log.rs
│   │   └── wasi/              # clocks, random, io(streams/poll/error), cli
│   ├── limits.rs              # ResourceLimiter + NodeMemoryBudget
│   ├── activation.rs          # Activate → instantiate → run → outcome; hot-wait/evict
│   └── sink.rs                # ActivationSink trait (+ FakeSink behind cfg(test)/feature "testkit")
└── tests/
    ├── determinism.rs  sandbox.rs  stack_depth.rs  cache.rs  pool.rs  validate.rs  wit_surface.rs
    └── golden/                # insta snapshots of journals
tests/guests/                  # SEPARATE cargo workspace (target wasm32-wasip2), excluded from main
├── det-basic/  det-divergent/ (feature-flag variant)  sandbox/ (loop, bomb, deep)
├── wasi-forbidden/ (uses std::fs → must be rejected)  wit-surface/ (calls every import)
```

### Pattern 1: Sync WIT, async host imports on fibers (Wasmtime ≥ 42)
**What:** `Config::async_support` is a deprecated no-op in 50.0.2 (`#[deprecated(note = "no longer has any effect")]`). Async is available whenever the `async` crate feature is on. A store becomes "async-required" as soon as the linker contains any async host function. From then on every `*_async` API must be used (`instantiate_async`, `call_async`), and sync calls return an error. Generate **all** Ostro imports as `async | trappable`. This keeps them uniform, lets them await sink appends and bridge completions, and lets them return `Err(Evict)` / `Err(NonDeterminism)` to unwind the guest.
**When:** Always. Guests see sync functions, so this works with WASI 0.2-style toolchains in all three languages.
**Key facts (verified in source):**
- Async host futures are **polled on the fiber stack** (`StoreFiber::block_on`). Everything a host import does inside `poll` counts against the `async_stack_size − max_wasm_stack` budget.
- Dropping a `call_async` future resumes the fiber with a "future dropped" error and unwinds it cleanly (`FiberFuture::drop → dispose`). Host code must not swallow that error.

### Pattern 2: Step handle: `resource step` vs `u64` (DEPENDS ON PHASE 1 S2)

| | `resource step` (docs/03) | `u64` handle |
|---|---|---|
| Rust guest (wit-bindgen 0.63) | **Verified working** in spike (`with: { "ostro:runtime/journal.step": StepRep }` on host, RAII handle on guest) | Trivial |
| Python/JS | componentize-py documents resource support; jco support assumed, S2 measures | Trivial everywhere |
| Safety | Guest can't complete a step it doesn't own. Drop without complete tells the host the step was abandoned. Handles die with the Store on eviction | Host must validate seq; unknown/duplicate → trap |
| Host cost | `ResourceTable` push/get/delete per step (negligible) | None |
| Change later | Breaking WIT change, so it **must** be settled before the Phase 5 freeze | Same |

**Recommendation:** Implement the replay core in terms of `StepId(u64)`. Make `host/journal.rs` the only file that knows the WIT shape, as about 50 LOC per variant. Default to `resource` unless S2 reports a Python/JS problem.

### Pattern 3: Deterministic WASI shim (ENG-02)
**Measured on 2026-11-18** (Rust 1.100, wit-bindgen 0.63, guest using `SystemTime::now`, `HashMap`, `rand::random`). The component imports:
```
wasi:io/poll  wasi:io/error  wasi:io/streams
wasi:clocks/monotonic-clock  wasi:clocks/wall-clock
wasi:random/random  wasi:random/insecure-seed
wasi:cli/stdin  wasi:cli/stdout  wasi:cli/stderr  wasi:cli/environment  wasi:cli/exit
wasi:cli/terminal-input  wasi:cli/terminal-output  wasi:cli/terminal-stdin  wasi:cli/terminal-stdout  wasi:cli/terminal-stderr
(all @0.2.12)
```
Without `rand`, `wasi:random/random` disappears, but the rest stay.

| Interface | Ostro implementation |
|---|---|
| `clocks/wall-clock.now` | Logical time (Pattern 4 "time rule"). `resolution` = 1 µs constant |
| `clocks/monotonic-clock.now` | Logical time in ns since `run.accepted_at`. `subscribe-*` returns an **already-ready** pollable (no real sleep). Emit one warn log per activation |
| `io/poll.poll`, `pollable.block` | All ready, immediately |
| `random/random.get-random-bytes/u64` | ChaCha20 stream `WASI_RANDOM` from the per-run seed |
| `random/insecure.*` | ChaCha20 stream `WASI_INSECURE` |
| `random/insecure-seed` | Constant per run, derived from the seed (keeps `HashMap` order stable across replays and different across runs) |
| `cli/environment` | Empty env, empty args, `initial-cwd` = none |
| `cli/stdout`, `stderr` | Output streams into the bounded per-run log buffer. Suppressed while replaying. A Rust panic message arrives here before the `unreachable` trap, so capture it into the failure reason |
| `cli/stdin` | Empty stream (EOF) |
| `cli/terminal-*` | `get-terminal-*` → `none` |
| `cli/exit` | Trap with a typed `GuestExit(code)` error; the attempt fails |
| `io/streams` (`check-write`, `write`, `blocking-flush`, `subscribe`, …) | Bounded buffer; `check-write` returns the remaining budget; never blocks |

**Versioning:** The Wasmtime component linker matches semver-compatible names in both directions: host `0.2.x` satisfies an import of `0.2.y` (verified in the `linker.rs` docs). Matching is per *function*, though. If the guest imports a function added in a later 0.2.z than the vendored WIT, `instantiate_pre` fails. **Vendor the WASI WIT at ≥ 0.2.12** (the version Rust 1.100 std emits), and re-run the import probe whenever the Rust toolchain is bumped.

**Validation (deploy time):**
1. `Component::from_binary`. A core module (not a component) gets a clear rejection.
2. For each import name in `component.component_type().imports(&engine)`, parse `ns:pkg/iface@ver`. The interface must be in the allow-list: `ostro:runtime/{types,context,journal,effects,log}@0.1.*` plus the 17 WASI interfaces above at `0.2.*`. Report every disallowed import, for example `wasi:filesystem/preopens@0.2.12 is not allowed: Ostro guests have no filesystem`.
3. Exports must be exactly `ostro:runtime/workflows@0.1.*`. Reject `wasi:cli/run` and `wasi:http/incoming-handler`.
4. `linker.instantiate_pre(&component)` must succeed. This catches missing functions inside allowed interfaces.

### Pattern 4: Replay cursor (pure Rust; the heart of ENG-03/04)
**What:** The engine keeps an in-memory `Journal` for the activation, with events `0..n` from `Activate`, and a cursor. Events come in two classes:
- **Guest-command events**, appended by the executor because the guest called an import: `step_started?`, `step_completed`, `step_failed`, `effect_scheduled`, `timer_started`, `signal_wait_started`, `child_started`, `await_token_created`, `wait_resolved`, `patch_marker`, `status_published`, `run_completed`/`run_failed`/`run_continued_as_new`.
- **Input events**, appended by apply on behalf of others: `timer_fired`, `effect_completed`, `signal_received`, `child_completed`, `token_completed`, `cancel_requested`, `activation_started`.

```
on guest command C {kind, hash, args}:
  next = journal.next_guest_command(cursor)          # skips input events
  if next exists:                                    # REPLAYING
      if next.kind != C.kind or next.hash != C.hash:
          return Err(NonDeterminism{ seq: next.seq,
                     expected: (next.kind, next.hash, next.summary()),
                     actual:   (C.kind,   C.hash,   C.summary()) })   # trap → attempt fails → DLQ "nondeterminism"
      cursor = next.seq + 1
      return decode(next.recorded_result_bytes)       # ENG-04: exact bytes, never re-derived
  else:                                              # LIVE
      ev = C.to_event(ts = clock.now_for_append())
      pending.push(ev); possibly flush via sink.append (must flush before any I/O-gated op)
      return live result
on guest returns from `run`:
  if journal.next_guest_command(cursor) exists and it is not the terminal event:
      NonDeterminism{seq, expected: next, actual: "run returned"}
```

**`wait(ops, mode)`:**
- *Replay:* the next guest command must be `wait_resolved{hash(ops, mode)}`. Return its recorded ordered completion list. Outcomes are looked up by op id from input events.
- *Live:* collect completions for `ops` that already exist as input events, **ordered by their journal seq**, which makes them deterministic. If `mode` is satisfied, append `wait_resolved` and return. Otherwise go to hot-wait or evict (Pattern 9).

**Time rule (deterministic `now`):** logical now is the `ts` of the most recent **guest-command** event at or before the cursor. Before the first one, it is `run.accepted_at`. Live command events get `ts` from the executor's clock when they are created, before control returns to the guest. Replay therefore observes the same values at the same program points. No per-read `clock_read` event is needed: a read is derived from journaled data, which satisfies "journaled clocks". Input events never move time, because their position relative to guest execution is not fixed.

**Randomness:** a 32-byte `rng_seed` is generated by the leader at `RunStarted` and **journaled in the run record**, then delivered in `Activate`. Use separate ChaCha20 streams per source (`CTX_RANDOM`, `UUID`, `WASI_RANDOM`, `WASI_INSECURE`). That way, a library that starts using `wasi:random` does not shift the `ctx.random` sequence. `uuid-v7` = logical-now ms + 74 bits from the `UUID` stream.

**Context calls that are NOT journaled:** `info`, `now`, `random-*`, `uuid-v7` and `is-replaying` are derived values. `is-replaying` = `next_guest_command(cursor).is_some()`.

**`patched(id)`** (Phase 3 basic semantics; full WF-08 rules come in Phase 5):
- *Live:* return true and append `patch_marker{id}`.
- *Replay:* return true and consume the marker only if the next guest command is `patch_marker{id}`. Otherwise return false and consume nothing.
- Phase 5 adds the "marker present but never queried" rule (patch removed).

**`set-status(json)`:** a guest command with a **kind-only hash**, so changing the status text does not break replay. It is skipped on replay, and the published value comes from the journal.

### Pattern 5: Command hash and canonical bytes (Pitfall 1 defence)
- `CommandHash = sha256(domain ‖ hash_ver:u8 ‖ kind:u8 ‖ canonical_args)[..16]`. The `hash_ver` is stored in every guest-command event, and replay recomputes with the **recorded** `hash_ver`. That way a future change to the hash definition never invalidates old journals.
- `canonical_args` is a **hand-specified** per-kind byte layout: length-prefixed UTF-8, little-endian ints, f64 as bits with NaN canonicalised to `0x7ff8000000000000`, lists in guest order. Do **not** postcard-encode whole WIT structs: adding a field would change the hash.

| Kind | Hashed fields | Not hashed (may change between versions) |
|---|---|---|
| step | name | retry policy, timeout, fuel |
| http effect | method, url, sha256(body), sorted header *names* | header values (may hold `{{secret:…}}` templates), timeout, retry |
| sql effect | connection, statement, sha256(canonical params) | exactly-once flag |
| sleep / sleep-until | duration-ms / target | — |
| wait-signal | name | timeout |
| start-child | workflow, sha256(input) | queue, version, detached |
| create-await-token | — (kind only) | timeout |
| wait | ordered op ids, mode | — |
| patch_marker | change-id | — |
| set-status | — (kind only) | json |
| terminal | kind only | output |

- **Recorded results** are stored as `{codec_ver:u8, bytes}` using Ostro-owned mirror types encoded with postcard. Step outputs are the guest's `payload` (content-type + data) **verbatim**. Effect outputs are serialized **once** at completion time (headers in received order), so replay decodes those bytes and never touches `http::HeaderMap` again.

### Pattern 6: Resource governance (ENG-05)
- **Memory:** a custom `ResourceLimiter` holds the per-instance cap and charges a shared `NodeMemoryBudget` (an `AtomicU64`).
  - On breach, set `self.breach = Some(Breach::Memory)` and return `Err(MemoryExceeded)`. This traps. Return `Err` from `memory_grow_failed` too.
  - **Never return `Ok(false)`.** That makes `memory.grow` return −1, which is guest-visible, and the guest's behaviour would then depend on node load. Trapping keeps resource limits invisible to guest logic.
  - Classify the failure from the store flag or a downcast of the typed error. In the spike, `StoreLimits` with `trap_on_grow_failure(true)` produced a non-`Trap` error ("forcing trap when growing memory to 67174400 bytes"), so `downcast_ref::<Trap>()` alone cannot classify it.
- **Fuel:**
  - Before each activation, `set_fuel(min(activation_budget, run_remaining))`.
  - When `begin-step` returns `execute`, record `get_fuel()` and lower the fuel to `step_fuel` if one is set. At `complete`, compute what was consumed and restore the outer remainder.
  - On `Trap::OutOfFuel`, the attempt fails with `fuel_exhausted`.
  - Journal `fuel_used` in `step_completed` for metrics and run budgets. Count live execution only: replayed steps don't re-run their bodies.
  - Fuel is deterministic only for one Wasmtime version. It must never feed a guest-visible value.
- **Epochs:** one ticker thread per `Engine` (10 ms tick, holding an `EngineWeak`) and `store.set_epoch_deadline(1)`. The `epoch_deadline_callback` returns `Interrupt` once the activation's wall deadline has passed, and `Yield(1)` otherwise. That gives time slicing plus a clean `Trap::Interrupt` (verified: ~94 ms for 10 ticks × 10 ms).
- **Outcome mapping:**

| Breach | Outcome |
|---|---|
| `OutOfFuel` | `fuel_exhausted` (retry once) |
| `Interrupt` | `timeout` |
| `MemoryExceeded` | `memory_exceeded` |
| `StackOverflow` | `stack_overflow` |
| `GuestExit` | `guest_exit(code)` |
| Other trap | `trap(msg)` |
| `NonDeterminism` | `nondeterminism` (DLQ, no retry) |

All of these end the attempt only. The Store is dropped and nothing else is touched.

### Pattern 7: AOT cache (ENG-07)
```
key = hex(sha256(module_bytes)) + "-" + hex(sha256(
        stable_hash(engine.precompile_compatibility_hash()) ‖ "wasmtime=50.0.2" ‖ OSTRO_VERSION ‖ target_triple))[..16]
file: cache/aot/<key>.cwasm   (bytes ‖ 32-byte HMAC-SHA256 trailer, or sidecar .mac)
load: read whole file → verify HMAC (constant-time) → unsafe { Component::deserialize(&engine, &bytes) }
      HMAC mismatch → delete, metric ostro_aot_cache_tamper_total, recompile
      deserialize Err (incompatible config) → treat as miss, recompile
compile: Semaphore(compile_permits) + spawn_blocking(engine.precompile_component) + single-flight per sha
         write tmp → fsync → rename
```
- `precompile_compatibility_hash()` returns `impl Hash` and covers triple, Cranelift flags, ISA flags, tunables, wasm features and module version. **Do not use `DefaultHasher`**: its algorithm is unspecified across Rust releases. Feed it into a `Hasher` adapter over SHA-256 (Code Examples).
- Set `wasm_backtrace_details(WasmBacktraceDetails::Disable)` explicitly. The default reads `WASMTIME_BACKTRACE_DETAILS` from the environment into tunables, which changes the compat hash between processes.
- Spike numbers for the 97 KB Rust guest: precompile 24 ms, `.cwasm` 377 KB. Python/JS take about 1 s and 460–530 MB RSS (STACK.md), so the compile pool defaults to 1–2 permits.

### Pattern 8: Pooling allocator sizing (ENG-08). Open gap now closed.
Wasmtime 50.0.2 defaults, read from `config.rs` and `wasmtime-environ` tunables on 64-bit:

| Setting | Default |
|---|---|
| `total_component_instances` / `total_core_instances` / `total_memories` / `total_tables` / `total_stacks` / `total_gc_heaps` | **1000** each |
| `max_memory_size` | **4 GiB** |
| `memory_reservation` | **4 GiB**, `memory_guard_size` **32 MiB**, `memory_reservation_for_growth` 2 GiB |
| Pool stack size (`async_stack_size`) | **2 MiB**; `max_wasm_stack` **512 KiB** |
| `component_instance_size` / `core_instance_size` | 1 MiB |
| `max_memories_per_module` | 1; `table_elements` 20 000 |
| `max_unused_warm_slots` | 100 |
| `linear_memory_keep_resident` / `async_stack_zeroing` / MPK | 0 / false / off |

**Virtual address space reserved at `Engine::new`** ≈ `total_memories × (memory_reservation + guard)` plus stacks. The default 1000 slots ≈ **4 TiB VA**. The ceiling is about 31k slots on 47-bit address spaces.

Recommended Ostro defaults for the Rust engine class:
- `total_* = 1000` (configurable).
- Keep the 4 GiB reservation (bounds-check elision).
- `max_memory_size` = per-instance cap (default 256 MiB).
- `async_stack_size` 4 MiB with `max_wasm_stack` 512 KiB, giving a 3.5 MiB host budget. Use `max_unused_warm_slots` to bound RSS.

On `Engine::new` failure:
- Wrap the error: "pooling allocator could not reserve N GiB (total_memories × (reservation+guard)); lower `engine.pool.total_instances`, check `ulimit -v`, `vm.overcommit_memory`, `vm.max_map_count`".
- Then **fall back to the on-demand allocator with a warning** (`engine.pool.fallback = "on-demand" | "fail"`, default on-demand).
- Expose `pooling_allocator_metrics()`, and call `release_idle_pool_memory()` (new in 50) under memory pressure.

### Pattern 9: Activation model (ENG-09)
```
activate(Activate{run, attempt, module_sha, journal, rng_seed, deadline}):
  pre   = cache.instance_pre(module_sha)                       # L2 → L1 → compile
  store = Store::new(engine, HostState{cursor, sink, limiter, cancel_token, ...})
  wf    = pre.instantiate_async(&mut store).await
  r     = wf.ostro_runtime_workflows().call_run(&mut store, name, &input).await
  match r: Ok(completed|continue_as_new|failure) → append terminal event → sink.done(Terminated)
           Err(e) if e.is::<Evict>()            → flush pending → sink.done(Evicted); drop store
           Err(e) if e.is::<NonDeterminism>()   → sink.done(Failed{nondeterminism, seq, expected, actual})
           Err(trap)                            → classify (Pattern 6) → sink.done(AttemptFailed{reason})
inside effects.wait (live, not satisfied):
  if every awaited op can resolve within hot_wait (timer due ≤ hot_wait, effect in flight) and node budget OK:
       select!{ completion arrives → record + return,  hot_wait elapses → Err(Evict),  cancel_token → Err(Evict) }
  else Err(Evict)
```
- **Node memory budget:** `NodeMemoryBudget` tracks resident bytes (the limiter charges and releases them, and the release happens on Store drop through a `Drop` guard). When it is over budget, fire the `CancellationToken` of the least-recently-active *parked* activation (one waiting inside `wait`). Running activations are never interrupted for budget reasons. Their growth past the per-instance cap traps.
- **Thin host imports:** imports only enqueue onto channels and await oneshots. Real work runs on tokio worker stacks, not the fiber: sink append, serde of large payloads, bridge I/O.

### Pattern 10: `ActivationSink` and the fake (parallel with Phase 2)
Keep the docs/03 trait. The engine's event type is `ostro_types::journal::Event`.

**Coordination point with Phase 2:** apply must understand `timer_started`, `effect_scheduled` and the other events in order to derive timers, effects and waits. Phase 3 should *propose* the `Event` enum and the `RunStarted.rng_seed` field in `ostro-types`, and Phase 2 consumes them. Treat the event list in Pattern 4 as the draft.

`FakeSink` (feature `testkit`):
- An in-memory journal per run. `append` validates `from_seq` and `attempt` and returns `Fenced` when scripted.
- It records every appended batch.
- A **script** injects input events (timer fired, signal, cancel, token completion, effect completion) in response to `effect_scheduled`/`timer_started`, or "after N activations".
- `drive_to_completion(module, input, script) -> (Outcome, Journal)` loops: activate → on Evicted deliver scripted inputs → re-activate with the full journal.

### Anti-Patterns to Avoid
- **Heavy work inside a host import's `poll`.** It runs on the fiber stack, and past the budget the process aborts (verified). Keep imports thin and move the work to tasks.
- **`ResourceLimiter` returning `Ok(false)`.** It makes resource pressure guest-visible and non-deterministic. Return `Err` (trap) instead.
- **Re-deriving recorded results** from structured host data on replay. Always decode journaled bytes.
- **postcard/serde of whole WIT structs as the command hash.** Additive field changes would break every in-flight run.
- **`DefaultHasher` for persistent keys.** Its algorithm is unspecified across Rust versions.
- **`Component::deserialize_file` after verifying the HMAC.** The file can change between check and mmap (TOCTOU).
- **Mixing `call` and `call_async`.** Once any async import exists, sync calls error.
- **Leaving `wasm_backtrace_details` at `Environment`.** The env var leaks into tunables and the compat hash.
- **Calling `Config::wasm_threads(false)`.** It doesn't compile without the `threads` crate feature. Removing the feature is the switch.
- **Adding fields or cases to WIT records/variants/enums after the freeze.** The Component Model treats that as a type change. "Additive" means new functions, interfaces or types only. Get `op-output`, `failure`, `run-info`, `step-options`, `workflow-meta` and `child-options` complete **now**.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Host bindings for WIT | Manual `func_wrap` + `Lift/Lower` | `wasmtime::component::bindgen!` (`imports: { default: async \| trappable }`, `with:` for resources) | Type-safe, regenerates on WIT change |
| Guest bindings | Hand-written canonical ABI | `wit_bindgen::generate!` 0.63 | Verified on Rust 1.100 |
| Instance pre-linking | Re-link per activation | `Linker::instantiate_pre` + generated `WorkflowPre::new` / `instantiate_async` | 10 µs instantiation measured |
| Memory caps | Polling memory size | `ResourceLimiter` / `Store::limiter` | Called on every grow |
| CPU limits | Watchdog threads killing tasks | Fuel (`consume_fuel`, `set_fuel`) + epochs (`increment_epoch`, `epoch_deadline_callback`) | Only safe interruption of JIT code |
| Compat detection | Parsing `.cwasm` headers | `Engine::precompile_compatibility_hash` + `Component::deserialize` error | Wasmtime knows which flags matter |
| ChaCha20 | Own cipher | `rand_chacha`/`chacha20`, pinned + golden vector | Correctness + stability |
| HMAC | `sha256(key‖msg)` | `hmac::Hmac<Sha256>` + `verify_slice` (constant time) | Length-extension, timing |
| Import introspection | Parsing component binary with wasmparser | `Component::component_type().imports(&engine)` | No second wasmparser copy |

**Key insight:** Wasmtime already provides every sandbox primitive. What Ostro must hand-build carefully is **only the replay core**: cursor, hash spec, time/random derivation and wait ordering. That core is pure Rust and should be over-tested with proptest.

## Common Pitfalls

### Pitfall 1: Fiber stack overflow aborts the node (VERIFIED)
**What goes wrong:** At maximum Wasm depth, a host import that uses more than `async_stack_size − max_wasm_stack` of native stack causes SIGABRT for the whole process. The spike measured: defaults 2 MiB / 512 KiB, a 1.4 MiB host stack survived, 1.7 MiB aborted (exit 134). At shallow depth the same import is fine, so normal tests never see it.
**How to avoid:**
- Set `async_stack_size` = 4 MiB.
- Keep imports thin (Pattern 9).
- Run a test that binary-searches the deepest successful recursion depth for a guest that calls *each* import at the bottom with maximum-size arguments. Run it under nextest so an abort fails one test, not the runner.
**Warning signs:** "execution on async fiber has overflowed its stack" in stderr; exit code 134.

### Pitfall 2: Host-side non-determinism after upgrades
**What goes wrong:**
- The hash or codec changes with a dependency bump.
- The ChaCha crate changes its stream.
- Wasmtime changes NaN bits.
**How to avoid:**
- Version `hash_ver` and `codec_ver`.
- Add golden-vector tests for ChaCha, the command hash and the codec (fixed inputs → fixed bytes, committed).
- Snapshot `format!("{config:?}")` with insta. `Config: Debug` prints every wasm feature flag, compiler settings incl. `enable_nan_canonicalization`, and tunables.
- Start the replay corpus (golden journals) in this phase.

### Pitfall 3: Divergence that isn't detected
**What goes wrong:** The guest returns early or skips trailing commands, and the engine treats it as success.
**How to avoid:** At `run` return, if the journal still has guest commands beyond the cursor (other than the terminal event), fail with `NonDeterminism`. Also test the reordered, extra, missing and changed-args variants with a divergent guest build.

### Pitfall 4: Resource limits becoming guest-visible
**What goes wrong:** `memory.grow` returns −1 under node pressure, and the guest takes another branch. Replay then diverges or behaves differently.
**How to avoid:** Trap (`Err`), never `Ok(false)`. Fuel, epochs and stack overflow already trap.

### Pitfall 5: WASI version skew
**What goes wrong:** A Rust toolchain bump emits `@0.2.13` imports with a new function. `instantiate_pre` fails for every new Rust module.
**How to avoid:** Vendor WASI WIT at ≥ the highest version the pinned toolchain emits (0.2.12 for 1.100). A CI probe builds the probe guest and lists its imports (as in the spike), so a change shows up as a red test.

### Pitfall 6: Pool exhausts address space or hoards RSS
**What goes wrong:** `Engine::new` fails in containers (reproduced), or freed slots keep RSS.
**How to avoid:** Wrapped error + on-demand fallback; configurable totals; budget from `pooling_allocator_metrics()`; `release_idle_pool_memory()`.

### Pitfall 7: AOT cache trust and compat
**What goes wrong:** Tampered `.cwasm` leads to arbitrary code execution (`deserialize` is `unsafe`). A stale cache after an upgrade causes deserialize errors.
**How to avoid:** HMAC verified on the in-memory bytes; the key includes the compat hash; deserialize errors count as misses. Never set `ModuleVersionStrategy::None`.

### Pitfall 8: WIT records/variants can't grow
**What goes wrong:** Adding `op-output::http` after the freeze breaks every 0.1 guest.
**How to avoid:** Define every effect output type in 0.1 now (http, sql, object, timer, signal, child, token, cancel), even though the bridge only lands in Phase 6 and children in Phase 9. Host stubs return `failure::non-retryable{kind:"unsupported"}` until then.

### Pitfall 9: Quadratic replay
**What goes wrong:** A long loop with sleeps replays O(n²) events.
**How to avoid:** `continue-as-new` as a return variant (WIT-03), `continue-as-new-suggested()` based on event count and bytes, and a metric `ostro_replay_events_total / activations`.

## Code Examples

All examples compiled and ran against wasmtime 50.0.2 in the 2026-11-18 spike unless marked *(design)*.

### Engine config
```rust
// Source: wasmtime-50.0.2/src/config.rs (verified), spike host2
fn build_config(c: &EngineConfig) -> wasmtime::Config {
    let mut cfg = wasmtime::Config::new();
    cfg.cranelift_nan_canonicalization(true)      // default false
       .relaxed_simd_deterministic(true)          // default false (!)
       .consume_fuel(true)
       .epoch_interruption(true)
       .max_wasm_stack(512 << 10)                 // default 512 KiB
       .async_stack_size(4 << 20)                 // default 2 MiB; host budget = 3.5 MiB
       .wasm_backtrace_details(wasmtime::WasmBacktraceDetails::Disable) // keep compat hash env-independent
       .parallel_compilation(c.parallel_compilation);
    // threads/gc/stack-switching/CM-async: removed via crate features, not setters
    let mut p = wasmtime::PoolingAllocationConfig::default();
    p.total_component_instances(c.pool_total).total_core_instances(c.pool_total)
     .total_memories(c.pool_total).total_tables(c.pool_total).total_stacks(c.pool_total)
     .max_memory_size(c.max_memory_bytes);
    cfg.allocation_strategy(wasmtime::InstanceAllocationStrategy::Pooling(p));
    cfg
}
```

### Host bindings
```rust
// Source: spike host2 (compiled), wasmtime-50.0.2 bindgen docs
wasmtime::component::bindgen!({
    world: "workflow",
    path: "../../wit/ostro-runtime-0.1.0",
    imports: { default: async | trappable },     // every import may await + may trap (Evict/NonDeterminism)
    exports: { default: async },
    with: { "ostro:runtime/journal.step": StepRep },
});
// linker
let mut linker: Linker<HostState> = Linker::new(&engine);
Workflow::add_to_linker::<_, wasmtime::component::HasSelf<_>>(&mut linker, |h| h)?;
wasi_shim::add_to_linker(&mut linker)?;          // own bindgen! over vendored WASI WIT
let pre = WorkflowPre::new(linker.instantiate_pre(&component)?)?;
// per activation
let wf = pre.instantiate_async(&mut store).await?;
let r = wf.ostro_runtime_workflows().call_run(&mut store, &name, &input).await;
```

### Store setup: limiter, fuel, epoch deadline
```rust
// Source: wasmtime-50.0.2 store.rs/limits.rs (verified); classification per spike results
let mut store = Store::new(&engine, host_state);
store.limiter(|h| &mut h.limits);                // custom ResourceLimiter: Err on breach, never Ok(false)
store.set_fuel(activation_fuel)?;
store.set_epoch_deadline(1);
store.epoch_deadline_callback(|st| {
    if st.data().wall_deadline_passed() { Ok(UpdateDeadline::Interrupt) }   // → Trap::Interrupt
    else { Ok(UpdateDeadline::Yield(1)) }                                    // time slicing
});
// ticker (once per Engine)
let weak = engine.weak();
std::thread::spawn(move || loop {
    std::thread::sleep(Duration::from_millis(10));
    match weak.upgrade() { Some(e) => e.increment_epoch(), None => break }
});
```

### Stable compat hash *(design; API verified)*
```rust
struct Sha256Hasher(sha2::Sha256);
impl std::hash::Hasher for Sha256Hasher {
    fn write(&mut self, bytes: &[u8]) { sha2::Digest::update(&mut self.0, bytes) }
    fn finish(&self) -> u64 { 0 } // unused; we take the digest
}
fn compat_digest(engine: &Engine) -> [u8; 32] {
    use std::hash::Hash;
    let mut h = Sha256Hasher(sha2::Sha256::default());
    engine.precompile_compatibility_hash().hash(&mut h);
    "wasmtime=50.0.2".hash(&mut h); env!("CARGO_PKG_VERSION").hash(&mut h);
    sha2::Digest::finalize(h.0).into()
}
```

### Error classification
```rust
// Source: spike output (Trap::OutOfFuel / Interrupt / StackOverflow verified; memory cap is NOT a Trap)
fn classify(e: &wasmtime::Error, st: &HostState) -> AttemptFailure {
    if let Some(nd) = e.downcast_ref::<NonDeterminism>() { return AttemptFailure::NonDeterminism(nd.clone()) }
    if e.is::<Evict>() { return AttemptFailure::Evicted }
    if let Some(b) = st.limits.breach { return b.into() }                 // memory_exceeded
    match e.downcast_ref::<wasmtime::Trap>() {
        Some(Trap::OutOfFuel) => AttemptFailure::FuelExhausted,
        Some(Trap::Interrupt) => AttemptFailure::Timeout,
        Some(Trap::StackOverflow) => AttemptFailure::StackOverflow,
        _ => AttemptFailure::Trap(format!("{:#}", e.root_cause())),
    }
}
```

### Import listing (validation core)
```rust
// Source: spike host (compiled); note 50.0.2 yields ComponentExtern { ty, implements, external_id }
for (name, ext) in component.component_type().imports(&engine) {
    if !allowed(name) { violations.push(name.to_string()) }
    let _ = &ext.ty; // ComponentItem::ComponentInstance(..) for interface imports
}
```

### Test guest build
```bash
# tests/guests is its own workspace; Rust 1.100 emits a component directly
cargo +1.100.0 build --release --target wasm32-wasip2 --manifest-path tests/guests/Cargo.toml
# det-basic.wasm ≈ 97 KB (spike, opt-level="s", lto, strip)
```

## WIT 0.1 Draft (complete surface; compiled host + guest in spike, minus http/sql/object/child records)

Additions over docs/03 §6 are marked `// NEW`. The `types`, `context`, `journal`, `log` and the `effects`/`workflows` cores were compiled in the spike. The http/sql/object/child records are copied unchanged from docs/03 and must be included now (Pitfall 8).

```wit
package ostro:runtime@0.1.0;

interface types {
    record payload { content-type: string, data: list<u8> }
    type op-id = u64;
    record datetime { seconds: u64, nanoseconds: u32 }
    record error-info { kind: string, message: string, details: option<payload> }
    variant failure {
        retryable(error-info),
        non-retryable(error-info),      // NEW (replaces docs/03 `fatal`) — WIT-06
        timeout,                        // step/run/effect timeout — WIT-06
        cancelled,                      // WIT-02
    }
    record retry-policy { max-attempts: u32, initial-interval-ms: u64, max-interval-ms: u64,
                          backoff-coefficient: f64, non-retryable-kinds: list<string> }
    record step-options { retry: option<retry-policy>, timeout-ms: option<u64>, fuel: option<u64> }
    record run-info {
        run-id: string, workflow: string, workflow-version: u32, attempt: u32,
        module-hash: string, tenant: option<string>, parent-run-id: option<string>,
        deadline: option<datetime>,     // NEW — run timeout visible to guest (WIT-06)
        continued-from: option<string>, // NEW — continue-as-new lineage (WIT-03)
    }
}

interface context {
    use types.{run-info, datetime};
    info: func() -> run-info;
    now: func() -> datetime;
    random-u64: func() -> u64;
    random-bytes: func(len: u32) -> list<u8>;
    uuid-v7: func() -> string;
    is-replaying: func() -> bool;
    patched: func(change-id: string) -> bool;
    set-status: func(json: string);                 // NEW — WIT-05
    continue-as-new-suggested: func() -> bool;      // NEW — WIT-03 / Pitfall 9
}

interface journal {   // resource vs u64: Phase 1 S2 decides (Pattern 2)
    use types.{payload, step-options, failure};
    variant completion-ack { recorded, retry-now }
    resource step {
        seq: func() -> u64;
        complete: func(outcome: result<payload, failure>) -> completion-ack;
    }
    variant step-start { replayed(result<payload, failure>), execute(step) }
    begin-step: func(name: string, opts: step-options) -> step-start;
}

interface effects {
    use types.{payload, op-id, datetime, failure, retry-policy};
    // header / http-request / http-response / sql-value / sql-request / sql-result /
    // object-request: exactly as docs/03 §6 (include now; bridge implements in Phase 6)
    record child-options { queue: option<string>, version: option<u32>, detached: bool,
                           run-timeout-ms: option<u64> }               // NEW field — WIT-06
    record await-token { op: op-id, token: string }                     // NEW — WIT-04
    variant op-output {
        http(http-response), sql(sql-result), object(list<u8>),
        timer, signal(payload), child(payload),
        token(payload),                                                 // NEW — WIT-04
        cancel-requested,                                               // NEW — WIT-02
    }
    record completion { op: op-id, outcome: result<op-output, failure> }
    enum wait-mode { all, any }

    http-fetch: func(req: http-request) -> op-id;
    sql-query: func(req: sql-request) -> op-id;
    object-store: func(req: object-request) -> op-id;
    sleep: func(duration-ms: u64) -> op-id;
    sleep-until: func(at: datetime) -> op-id;
    wait-signal: func(name: string, timeout-ms: option<u64>) -> op-id;
    start-child: func(workflow: string, input: payload, opts: child-options) -> op-id;
    create-await-token: func(timeout-ms: option<u64>) -> await-token;  // NEW — WIT-04
    /// Reserved op that completes with `cancel-requested` once a graceful cancel is journaled.
    /// SDKs include it implicitly in every `wait`; after it fires, the guest may run cleanup
    /// (new ops still execute). Hard terminate is host-only and never visible to the guest.
    cancellation: func() -> op-id;                                     // NEW — WIT-02
    cancel-op: func(op: op-id);
    wait: func(ops: list<op-id>, mode: wait-mode) -> list<completion>;
}

interface log {
    enum level { trace, debug, info, warn, error }
    emit: func(level: level, message: string, fields: list<tuple<string, string>>);
}

interface workflows {
    use types.{payload, failure};
    record workflow-meta { name: string, version: u32, default-queue: option<string>,
                           run-timeout-ms: option<u64> }                // NEW field — WIT-06
    record continuation { input: payload, workflow: option<string>, version: option<u32> } // NEW
    variant run-result { completed(payload), continue-as-new(continuation) }               // NEW — WIT-03
    describe: func() -> list<workflow-meta>;
    run: func(name: string, input: payload) -> result<run-result, failure>;
}

world workflow {
    import context; import journal; import effects; import log;
    export workflows;
}
```

**Await-token format** *(design)*: `"ot1_" + base32(partition:u16 ‖ run_uuid:16B ‖ op:u64 ‖ hmac_sha256(cluster_key, prefix)[..10])`.
- The token is created live and **recorded as the command result**, so replay never needs the key.
- The API (Phase 5) verifies the MAC and routes by the embedded partition.

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| `Config::async_support(true)` | No-op (deprecated); async implied by the `async` feature + async host fns | Wasmtime 42 | docs/01 is stale; use `*_async` everywhere |
| `epoch_deadline_callback(StoreContextMut)` | `StoreHookState<T>` (`data()`, `data_mut()`) | Wasmtime 50 | If Phase 1 picks 48 LTS, examples change |
| `imports()` yields `ComponentItem` | yields `ComponentExtern { ty, implements, external_id }` | ≤ 50 (verified 50.0.2) | Validation code |
| `bindgen!` `async: true` | `imports: { default: async \| trappable }` flags, `HasSelf<_>` in `add_to_linker` | ~Wasmtime 35+ | Use the flag syntax |
| `cargo-component` | `cargo build --target wasm32-wasip2` + wit-bindgen | 2025 | Verified |
| Wasmtime-only error = `anyhow::Error` | `wasmtime::Error`/`wasmtime::Result` own type; `anyhow` is an opt-in feature | ≤ 50 | Use `wasmtime::Result` in host traits (bindgen default) |

**Deprecated/outdated:** `Config::async_support`, `cargo-component`, relying on wasmtime's default features. Note that Wasmtime 50 has an `rr` (record/replay) feature: it is execution-trace replay for debugging, not durable-execution replay. Don't use it, but its `validate_rr_determinism_conflicts` confirms the two flags Ostro needs (NaN canonicalisation and deterministic relaxed SIMD).

## Open Questions

1. **Step handle shape (resource vs u64).**
   - Known: resource works on Rust and the Wasmtime host (spike).
   - Unclear: Python/JS behaviour (S2).
   - Recommendation: build behind a thin adapter and default to resource. Phase 1 S2 confirms.
2. **Wasmtime pin (50.0.2 tracking vs 48 LTS).**
   - Known: the code here is verified on 50.0.2, and 51 releases ~2026-11-20.
   - Recommendation: write Phase 3 against `=50.0.2`. If Phase 1 picks 48, re-verify the epoch callback signature, `imports()` item type, `release_idle_pool_memory` (absent in 48) and the bindgen flags.
3. **Event enum ownership with Phase 2.**
   - Known: apply must understand journal events.
   - Recommendation: Phase 3's first plan drafts `ostro_types::journal::Event` and `RunStarted.rng_seed`, and Phase 2 reviews them. Lock both in Phase 4 integration and freeze them in Phase 5.
4. **Python/JS stack and memory needs.**
   - Known: 4 MiB fiber stacks are fine for Rust.
   - Unclear: CPython recursion inside Wasm, and 30–50 MB heaps.
   - Recommendation: one `Engine` per language class (separate pool configs). Phase 3 implements only the Rust class but makes `EngineConfig` per class.
5. **HMAC key provenance.**
   - Recommendation: in Phase 3, a node-local key file (`0600`, generated on first start) passed in config. Workers (Phase 7) use their own local key, since they compile locally.
6. **Fuel unit budgets** (defaults per step/run). Recommendation: make them configurable and pick defaults from the spike: a trivial run is about 5k fuel, and 50M fuel took about 5 ms of tight loop. Default activation budget 10^10, step budget unset.

## Validation Architecture

### Test Framework

| Property | Value |
|----------|-------|
| Framework | Rust built-in test harness run via **cargo-nextest 0.9.152** (process-per-test isolation needed for abort tests), plus proptest 1.12 and insta 1.49 |
| Config file | none yet. Wave 0: `.config/nextest.toml` (profile `ci`, slow-timeout for the 1000-replay test), `tests/guests/Cargo.toml` (separate workspace) |
| Guest build | `cargo +1.100.0 build --release --target wasm32-wasip2 --manifest-path tests/guests/Cargo.toml`. Wrap it in `cargo xtask guests` or a test helper that builds once into `target/guests` and caches |
| Quick run command | `cargo nextest run -p ostro-engine --lib` (pure replay core + unit; < 10 s) |
| Full suite command | `cargo xtask guests && cargo nextest run -p ostro-engine --profile ci` |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| ENG-01 | Config has NaN canon, deterministic relaxed SIMD, threads/GC off (insta snapshot of `format!("{config:?}")`); a guest computing NaN via `f64` ops returns canonical bits `0x7ff8…`; a component using shared memory/threads is rejected | unit + integration | `cargo nextest run -p ostro-engine -E 'test(engine_config)'` | ❌ Wave 0 |
| ENG-02 | Guest using `SystemTime::now`, `HashMap`, `rand` runs with logical time and seeded randomness; `wasi-forbidden` guest (std::fs) is rejected with a message naming `wasi:filesystem/*`; core module rejected; `wasi:cli/run` export rejected; probe guest import list matches the allow-list snapshot | integration | `cargo nextest run -p ostro-engine --test validate --test wasi_shim` | ❌ Wave 0 |
| ENG-03 | (a) proptest: random command/completion sequences record → replay with zero appends; a single mutation (reorder/insert/delete/args change) yields `NonDeterminism` at the exact seq. (b) `det-divergent` guest vs `det-basic` journal → DLQ reason `nondeterminism`, correct seq, expected vs actual. (c) early return with trailing journal commands → divergence | unit (proptest) + integration | `cargo nextest run -p ostro-engine -E 'test(replay_)' --test determinism` | ❌ Wave 0 |
| ENG-04 | Golden-vector tests for codec and command hash (committed bytes); replayed step/effect results byte-identical to journaled bytes (assert on the guest echo of the received bytes) | unit + integration | `cargo nextest run -p ostro-engine -E 'test(golden_)'` | ❌ Wave 0 |
| ENG-05 | Infinite loop → `fuel_exhausted` (fuel) and `timeout` (epoch deadline); memory bomb → `memory_exceeded`; per-step fuel cap trips only that step; concurrent healthy run on the same engine completes unaffected | integration | `cargo nextest run -p ostro-engine --test sandbox` | ❌ Wave 0 |
| ENG-06 | Binary-search max depth; at that depth call every import with maximum-size args (1 MiB payloads, long strings) → returns StackOverflow or Ok, **process survives** (nextest reports a crash as failure) | integration | `cargo nextest run -p ostro-engine --test stack_depth` | ❌ Wave 0 |
| ENG-07 | Compile pool bounded (N concurrent compiles never exceed permits, single-flight dedups same sha); cache key includes compat hash + versions; flipping one byte in `.cwasm` → HMAC failure → recompile + metric; artefact from an engine with different config → miss → recompile | integration | `cargo nextest run -p ostro-engine --test cache` | ❌ Wave 0 |
| ENG-08 | Warm instantiate p50 < 1 ms (assert < 1 ms; spike 10 µs); pool exhaustion → clear error; `Engine::new` under `prlimit --as` → wrapped error naming config keys, plus on-demand fallback works | integration (+ subprocess with `prlimit`, linux-only) | `cargo nextest run -p ostro-engine --test pool` | ❌ Wave 0 |
| ENG-09 | Sleep > hot_wait → `Evicted`, Store dropped, budget released; re-activate with journal + `timer_fired` → completes, journal identical to a no-evict run; short timer ≤ hot_wait completes in one activation; node budget exceeded → LRU parked activation evicted | integration (FakeSink) | `cargo nextest run -p ostro-engine --test activation` | ❌ Wave 0 |
| ENG-03 + SC1 | `det-basic` (SystemTime, HashMap, rand) driven 1,000× with the same seed + script → byte-identical journals; 1,000 replays of the recorded journal → zero appends, identical outcome | integration (slow, ~seconds) | `cargo nextest run -p ostro-engine --test determinism -E 'test(thousand_)'` | ❌ Wave 0 |
| WIT-01 | `wit-surface` guest calls every import + both exports; host bindgen compiles; `describe` round-trips | integration | `cargo nextest run -p ostro-engine --test wit_surface` | ❌ Wave 0 |
| WIT-02 | Cancel injected while waiting → `wait` returns `cancel-requested`; guest runs a cleanup step afterwards (journaled); replay identical | integration | `... --test wit_surface -E 'test(cancel_)'` | ❌ Wave 0 |
| WIT-03 | `run` returns `continue-as-new` → outcome `ContinuedAsNew{input}`; `continue-as-new-suggested` flips at the configured threshold | integration | `... -E 'test(continue_as_new)'` | ❌ Wave 0 |
| WIT-04 | `create-await-token` returns token embedding partition + run + op; replay returns identical token without key; scripted `token_completed` resolves the op | unit + integration | `... -E 'test(await_token)'` | ❌ Wave 0 |
| WIT-05 | `set-status` journaled once; changed status text on replay does not diverge; latest status exposed via the outcome/sink | integration | `... -E 'test(set_status)'` | ❌ Wave 0 |
| WIT-06 | `failure::non-retryable` surfaced as non-retryable outcome; `run-timeout-ms` from `describe` exposed; `run-info.deadline` populated; epoch deadline → `timeout` | integration | `... -E 'test(non_retryable or test(run_timeout))'` | ❌ Wave 0 |

### Sampling Rate
- **Per task commit:** `cargo nextest run -p ostro-engine --lib` (replay core + proptest at a reduced case count, `PROPTEST_CASES=64`)
- **Per wave merge:** `cargo xtask guests && cargo nextest run -p ostro-engine --profile ci`
- **Phase gate:** the full suite green on linux x86_64 **and** aarch64 (determinism/golden tests must agree across architectures), plus macOS, before `/gsd:verify-work`

### Wave 0 Gaps
- [ ] `tests/guests/` workspace with guests: `det-basic`, `det-divergent` (feature flag), `sandbox` (loop/bomb/deep), `wasi-forbidden`, `wit-surface`, `nan-probe`
- [ ] `cargo xtask guests` (or a `build_guests()` test helper with a separate `CARGO_TARGET_DIR`)
- [ ] `crates/ostro-engine/src/sink.rs` `FakeSink` + scripted input injector (feature `testkit`)
- [ ] `.config/nextest.toml` (ci profile; slow tests; crash = fail)
- [ ] Golden-vector fixtures: ChaCha stream, command hash, codec (`tests/golden/`)
- [ ] Install: `cargo install --locked cargo-nextest`; `rustup target add wasm32-wasip2 --toolchain 1.100.0` (already present locally)
- [ ] `wit/ostro-runtime-0.1.0/runtime.wit` + vendored `crates/ostro-engine/wit-wasi/` (WASI ≥ 0.2.12)

## Sources

### Primary (HIGH confidence)
- Wasmtime 50.0.2 source in `~/.cargo/registry/src/index.crates.io-*/wasmtime-50.0.2` and `wasmtime-environ-50.0.2`, read 2026-11-18:
  - `src/config.rs`: `async_support` deprecated no-op; `max_wasm_stack` default 512 KiB; `async_stack_size` 2 MiB; `relaxed_simd_deterministic` default false; NaN canonicalisation default false; `features()` strips GC/threads when the crate features are off; `validate()`; `PoolingAllocationConfig`/`InstanceLimits` defaults; `Config: Debug`
  - `wasmtime-environ/src/tunables.rs`: 64-bit `memory_reservation` 4 GiB, guard 32 MiB, growth 2 GiB
  - `src/engine.rs`: `precompile_component`, `precompile_compatibility_hash`, `increment_epoch`, `weak`, `pooling_allocator_metrics`, `release_idle_pool_memory`
  - `src/compile/code_builder.rs`: `HashedEngineCompileEnv` contents
  - `src/runtime/component/mod.rs`: `bindgen!` options reference
  - `src/runtime/component/linker.rs`: semver lookup, `instantiate_pre`, `define_unknown_imports_as_traps`
  - `src/runtime/component/func/typed.rs`, `instance.rs`: `call`/`call_async`, CM-async routing
  - `src/runtime/fiber.rs`: `block_on` on the fiber stack; drop → dispose
  - `src/runtime/store.rs`: `UpdateDeadline`, `epoch_deadline_callback(StoreHookState)`, fuel APIs
  - `src/runtime/limits.rs`: `ResourceLimiter` semantics
  - `src/lib.rs`: Async section
  - `Cargo.toml` `[features]`
- Local spike (2026-11-18, x86_64 WSL2, Rust 1.100.0, wasmtime =50.0.2, wit-bindgen 0.63.0):
  - Guest import list
  - Instantiate 10 µs p50 / 62 µs p99
  - Precompile 24 ms / 377 KB
  - Trap classifications
  - Fiber-stack abort at 1.7 MiB host stack (exit 134)
  - `ulimit -v` pool failure message
  - Incompatible-engine deserialize error
- `.planning/research/STACK.md`, `PITFALLS.md`, `ARCHITECTURE.md`, `SUMMARY.md`; `docs/03-architecture.md` §2.3, §5, §6, §11

### Secondary (MEDIUM confidence)
- crates.io search (2026-11-18) for current versions: wit-bindgen 0.63.0, wasmtime 51.0.0-rc.1, proptest 1.12.0, insta 1.49.0, hmac 0.13.0, sha2 0.11.1, chacha20 0.10.2, rand_chacha 0.10.0, postcard 1.1.3, async-trait 0.1.92, tokio-util 0.7.20
- Temporal workflow-time semantics (time = workflow task time) and patch-marker semantics, from the project's PITFALLS.md citations; the time and patched rules here are adapted from them

### Tertiary (LOW confidence)
- Python/JS resource and stack behaviour under componentize-py/jco: not measured here (Phase 1 S2)
- Fuel default budgets: extrapolated from one tight-loop measurement

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH. Versions from crates.io; Wasmtime facts from pinned source; spike compiled and ran
- Architecture: MEDIUM-HIGH. Wasmtime mechanics verified; the replay/hash/time design is reasoned, not yet battle-tested
- Pitfalls: HIGH for stack abort, pool VA and memory-trap classification (reproduced); MEDIUM for WASI version skew and upgrade determinism (reasoned)

**Research date:** 2026-11-18
**Valid until:** ~2026-12-18 for Wasmtime specifics (monthly majors: 51 lands ~2026-11-20; re-verify the APIs listed in "State of the Art" on any bump). The replay design is stable.
