# componentize-py deterministic fork (D-P9-3 step 2)

This directory vendors the minimal patch that makes componentize-py v0.25.1 pre-initialize
deterministically, plus the texts for the upstream reports. Only the patch, the build
script (`sdks/scripts/build-componentize-py-fork.sh`) and this README are committed; the
source clone, the cargo target dir and the installed binary live under `target/toolchains/`
(gitignored). Outcome and measurements: `docs/sdk-builds.md`.

## Use

```bash
bash sdks/scripts/build-componentize-py-fork.sh          # cold: ~6-30 min, ~3.6 GB
export OSTRO_COMPONENTIZE_PY="$(bash sdks/scripts/build-componentize-py-fork.sh --print)"
bash sdks/scripts/repro-check.sh py                       # 3 builds, compares sha256
cargo test -p ostro-conformance --features polyglot --test rebuild -- --test-threads=1
```

`build-guest.sh py` uses the binary named by `OSTRO_COMPONENTIZE_PY`; without it, the
released 0.25.1 wheel via uv (the default, and what Phase 10 ships, D-P9-30).

Requirements: make, clang or gcc, rustup with toolchain `1.100.0` (override with
`OSTRO_CPY_RUST_TOOLCHAIN`, upstream needs >= 1.94) and its `wasm32-wasip2` target, network
on the first build (github.com/bytecodealliance/componentize-py tag, wasi-sdk 33 release,
dicej/cpython `v3.14.0-wasi-sdk-30` tarball, zlib 1.3.1 and SQLite 3.51.2 from their
upstream release URLs, all fetched by upstream's `build.rs`).

## The patch (`componentize-py-0.25.1-deterministic.patch`)

| File | Change |
|---|---|
| `src/deterministic.rs` (new) | fixed random sources (a 256-byte cycle) and `insecure_random_seed`; wall clock from `SOURCE_DATE_EPOCH` (default 0) and monotonic clock, both advancing 1 µs per read; `pin_mtimes` sets atime and mtime of a tree to `SOURCE_DATE_EPOCH` |
| `src/lib.rs` | pre-init `WasiCtxBuilder`: `PYTHONHASHSEED=0`, `PYTHONDONTWRITEBYTECODE=1`, `deterministic::configure`; pins the times of the generated binding dirs and the rewritten bundled helpers |
| `src/prelink.rs` | pins the times of the extracted stdlib and bundled helpers; `BTreeMap` for leftover configs |
| `src/stubwasi.rs` | `BTreeMap` instead of `HashMap` for the WASI stub adapters (module order) |

Measured effect (3 builds of `conformance/guests/python`): released wheel, 3 different
lengths (21.99–22.06 MB) with differences across most sections; fork, identical length
(21,128,563 B) and 73–76 differing bytes, all wall-clock file timestamps (status-change /
birth and access times reported by wasmtime-wasi) plus a few small heap values. The str hash
secret is pinned (set iteration order identical across builds).

## Upstream PR description (for a human to file; no remotes from this repo)

> **Title:** Deterministic pre-initialization: pin hash seed, entropy, clocks, mtimes and adapter order
>
> Building the same app twice with componentize-py 0.25.1 yields different component bytes
> (and different lengths). Causes we measured:
>
> 1. The pre-init `WasiCtxBuilder` passes only `PYTHONUNBUFFERED`/`PYTHONHOME`/`PYTHONPATH`,
>    so CPython draws a random str hash secret during pre-init and bakes it into the
>    snapshot. Besides breaking reproducibility, this changes `set`/`frozenset` iteration
>    order between builds of the same source, which matters for deterministic-replay
>    runtimes.
> 2. Pre-init sees real entropy (`secure_random`, `insecure_random`, `insecure_random_seed`)
>    and real clocks.
> 3. `stubwasi::link_stub_modules` collects adapters in a `HashMap`, so with `--stub-wasi`
>    the core-module order of the output changes per run.
> 4. Files componentize-py writes per build (extracted stdlib and bundled helpers,
>    generated bindings, helpers rewritten for the world module name) carry build-time
>    mtimes, which importlib caches in the snapshot.
>
> This PR (patch attached, ~130 lines, one new module) sets `PYTHONHASHSEED=0` and
> `PYTHONDONTWRITEBYTECODE=1` for pre-init, installs deterministic random sources and
> clocks (honouring `SOURCE_DATE_EPOCH`), pins access/modification times of everything it
> extracts or generates to `SOURCE_DATE_EPOCH` (default 0), and uses `BTreeMap` for the stub
> adapters. With it, three builds of the same app have identical length and differ in ~75
> bytes; the remainder comes from wasmtime-wasi reporting host birth/access times in
> `descriptor.stat` during pre-init, which needs either a timestamp-masking filesystem view
> for pre-init or an option in wasmtime-wasi. Would you accept this, perhaps behind a
> `--deterministic` flag, and how would you prefer to handle the filesystem timestamps?

## componentize-qjs issue text (TypeScript/QuickJS, for a human to file)

> **Title:** Non-reproducible output: entropy/clock captured in the QuickJS snapshot at pre-init
>
> With jco 1.42.0 (bundled componentize-qjs 0.5.0), `jco componentize --backend quickjs
> --backend-qjs-disable-async --backend-qjs-stub-wasi` on identical inputs (sources staged
> with fixed mtimes, `SOURCE_DATE_EPOCH`, `TZ=UTC`, `LC_ALL=C`) produces different
> components on every run. The first difference is one 8-byte value in the QuickJS core
> module's data segments (4 bytes differ), consistent with a PRNG or hash state seeded from
> `wasi:random` or the clock during pre-initialization (e.g. `JSContext` random state);
> after it the snapshot's segment layout drifts (output length changes by 10–45 bytes for a
> 2.3 MB component). No timestamps appear in the diff. Could pre-init run with fixed
> entropy and clocks (deterministic `WasiCtx`), or expose an option for it? This would make
> QuickJS components bit-reproducible for durable-execution runtimes that identify modules
> by sha256.
