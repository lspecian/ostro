# SDK builds: pinned recipe, reproducibility, packaging

Plan 09-17 (SC2 / SDK-04, D-P9-3, D-P9-28, D-P9-30). This page covers how Python and
TypeScript workflow modules are built, how reproducible each language's build is, and how
the two SDK packages are packaged. Replay safety never depends on reproducibility: replay
always runs the artefact that was deployed (its sha256 is the module identity), never a
rebuild.

## Pinned toolchains (D-P9-28)

| Tool | Version | Where it is pinned |
|---|---|---|
| uv | **0.7.15 exactly** | `build-guest.sh` (`UV_VERSION`), ci-local `check_polyglot_toolchains`, ci.yml `astral-sh/setup-uv` `version` |
| Python | 3.12 (SDK `requires-python >=3.12`; the guest runs componentize-py's CPython 3.14) | `sdks/python/pyproject.toml`, ci.yml `actions/setup-python` |
| componentize-py | 0.25.1 (released wheel, via `uv tool run`) | `build-guest.sh` |
| Node | 24.13.0 (ci.yml), any 24.x locally | ci.yml `actions/setup-node`, `build-guest.sh` (`NODE_MAJOR`) |
| jco | 1.42.0 (bundles componentize-qjs 0.5.0) | `sdks/scripts/toolchain/package-lock.json` |
| componentize-js | 0.23.0 (StarlingMonkey fallback, `ts-sm`) | `sdks/scripts/toolchain/package-lock.json` |
| Rust | 1.100.0 + `wasm32-wasip2` | `rust-toolchain.toml` |

## Recipe per language

Every polyglot build goes through one script (interface stable, later phases consume it):

```bash
sdks/scripts/build-guest.sh py    <guest_dir> <out.wasm>   # Python, componentize-py
sdks/scripts/build-guest.sh ts    <guest_dir> <out.wasm>   # TypeScript, jco + QuickJS (default)
sdks/scripts/build-guest.sh ts-sm <guest_dir> <out.wasm>   # TypeScript, StarlingMonkey fallback
```

- **Python:** `componentize-py -d wit/ostro-runtime-0.1.0 -w ostro:runtime/workflow@0.1.0
  componentize --stub-wasi -p <stage> app -o <out>`; `sdks/python/ostro` is staged next to
  the guest's `app.py`. The released 0.25.1 wheel runs through `uv tool run --from
  componentize-py==0.25.1`, unless `OSTRO_COMPONENTIZE_PY` names a componentize-py binary
  (the D-P9-3 fork below).
- **TypeScript (QuickJS):** `jco componentize index.ts --wit wit/ostro-runtime-0.1.0
  --world-name workflow --backend quickjs --backend-qjs-disable-async
  --backend-qjs-stub-wasi --bundle-config sdks/scripts/toolchain/rolldown.ostro.mjs`;
  `sdks/ts` is staged as `node_modules/ostro`. The bundle config lowers async to ES2016,
  injects the SDK's deterministic `console`/timer/`queueMicrotask` globals, and marks the
  guest-only `ostro:runtime/*` imports external (no Rolldown UNRESOLVED_IMPORT warnings).
- **Rust:** `cargo build --release --locked --target wasm32-wasip2` (e.g.
  `--manifest-path guests/Cargo.toml -p conf-rust`).

The polyglot harness (`crates/ostro-conformance/src/polyglot.rs`) caches components by
sha256 over the mode, `build-guest.sh`, the toolchain lock, the bundle config, the WIT, the
guest and SDK sources, and (Python) the componentize-py selection, so the released wheel
and the fork never share cache entries.

### Deterministic wrapper (D-P9-3 step 1)

`build-guest.sh` stages the sources into a fresh directory with every mtime set to
`SOURCE_DATE_EPOCH` (default: commit time of `HEAD`) and runs the build with
`PYTHONHASHSEED=0 PYTHONDONTWRITEBYTECODE=1 TZ=UTC LC_ALL=C SOURCE_DATE_EPOCH`. These
settings are deterministic and documented; they fix the inputs the wrapper controls (user
sources, environment). The staging path itself does not leak into the output (Python
mounts it as `/0`, the TS bundle uses relative paths), so no fixed staging path is used.

## Reproducibility check

`sdks/scripts/repro-check.sh [rust|py|ts|all]` builds each conformance guest
(`guests/conf-rust`, `conformance/guests/python`, `conformance/guests/ts`) three times from
the same checkout, at least 2 s apart, with a fresh `OSTRO_POLYGLOT_CACHE` (Python/TS) or
a fresh `--target-dir target/repro/rust-<i>` (Rust), and prints the sha256 and size of each
build plus `REPRO <lang> OK|DIFF <bytes>`. It fails when Rust differs, or when Python/TS
differ without a `RESIDUAL-ACCEPTED: <lang>` line below.

## D-P9-3 ladder outcome (2026-11-22, x86_64 Linux, 12 cores)

| Language | Step 1: wrapper | Step 2: pinned fork | Rung reached |
|---|---|---|---|
| Rust | **identical**: 3 × sha256 `debb9349…eb79f2770`, 287,002 B (`REPRO rust OK`) | n/a | bit-reproducible (hard requirement met) |
| Python | differs: 3 different shas and lengths per run (21,995,290–22,059,308 B over two runs) | differs: 3 shas, **same length** 21,128,563 B, **73–76 B** differ | step 3: documented residual |
| TypeScript (QuickJS) | differs: 3 different shas per run, lengths 2,308,337–2,308,382 B over two runs | n/a (no fork for componentize-qjs in D-P9-3) | step 3: documented residual |
| TypeScript (StarlingMonkey) | excluded from SC2 (D-P9-1; ~1.6 MB residual in the research) | n/a | out of scope |

### Python, step 1 (released componentize-py 0.25.1)

Matches S9 (`docs/spikes/S9-polyglot.md`): the str hash secret is random per build (the
pre-init `WasiCtxBuilder` never sees `PYTHONHASHSEED`), the stub adapters are added in
`HashMap` order (core-module order shuffles), and componentize-py-generated files carry
build-time mtimes. Lengths change by up to 64 KB; differences span data, type, function,
export and code sections of many core modules.

### Python, step 2 (fork with the vendored patch)

`sdks/python/toolchain/componentize-py-0.25.1-deterministic.patch` (4 files: new
`src/deterministic.rs`, `src/lib.rs`, `src/prelink.rs`, `src/stubwasi.rs`):

- pre-init `WasiCtxBuilder`: `PYTHONHASHSEED=0`, `PYTHONDONTWRITEBYTECODE=1`, fixed
  `secure_random`/`insecure_random` (a fixed 256-byte cycle) and `insecure_random_seed`,
  a wall clock starting at `SOURCE_DATE_EPOCH` and a monotonic clock, both advancing 1 µs
  per read (deterministic as long as the read count is);
- access and modification times of the extracted stdlib and bundled helpers, the generated
  binding dirs and the rewritten helpers (tempdir roots included) set to
  `SOURCE_DATE_EPOCH`;
- `BTreeMap` instead of `HashMap` for the stub adapters (`stubwasi.rs`) and the leftover
  `componentize-py.toml` configs (`prelink.rs`).

Built by `sdks/scripts/build-componentize-py-fork.sh` (clone of tag v0.25.1 =
`2a53e46a`, patch applied, Rust 1.100.0 with `wasm32-wasip2` via a `rust-toolchain.toml`
written into the clone, because componentize-py's nested runtime build strips `RUST*`
variables). Build log: attempt 1 cold, 6 min 31 s wall, 1.5 GB peak RSS, 3.6 GB on disk
(wasi-sdk 33 + CPython `v3.14.0-wasi-sdk-30` + zlib + SQLite downloaded by `build.rs`);
attempt 2 (atime pinning added) 42 s incremental. That is the D-P9-3 time box (two
attempts); the ladder then moved to step 3.

Result: the hash secret is pinned (the `py-setorder` fixture iterates a set of 20 str in
the same order on every build, see C-REBUILD-01) and the length is stable, but **73–76
bytes** still differ, all in data segments of the main core module's memory snapshot
(13 segments). 38 of them sit in one segment as `{seconds, nanoseconds}` pairs equal to
the build's wall time next to the pinned `SOURCE_DATE_EPOCH` mtime: file **status-change
/ birth and access times**. wasmtime-wasi 46 reports `status-change-timestamp` from the
host's birth time and the access time moves when pre-init reads a file (relatime), and
neither can be set from user space. The remaining ~35 bytes are 1–4-byte differences
scattered in small heap objects (not attributed; consistent with layout effects of those
values). Closing the gap needs a change below componentize-py (a WASI filesystem view that
reports fixed timestamps during pre-init, in wasmtime-wasi or component-init) or reusing
persistent extraction directories (machine-local only).

RESIDUAL-ACCEPTED: py

**SC2 (Python): human_needed — upstream fix required (D-P9-3).** The upstream PR text is in
`sdks/python/toolchain/README.md`.

### TypeScript (QuickJS), step 1

The difference starts in one 8-byte value of the QuickJS core module's snapshot data (4
bytes differ), followed by segment-layout drift in the rest of the snapshot (10–45 B
length changes). No timestamps; the value looks like a PRNG/hash state taken from WASI
entropy or the clock at componentize-qjs pre-init (S9 recommendation 4). The wrapper
cannot reach it (componentize-qjs and its pre-init host are inside jco). The SDK replaces
`Math.random`/`crypto` with host-seeded versions at run time, so the snapshot value has no
effect on workflow results.

RESIDUAL-ACCEPTED: ts

**SC2 (TypeScript): human_needed — upstream fix required (D-P9-3).** The componentize-qjs
issue text is in `sdks/python/toolchain/README.md`.

## Replay across rebuilds (C-REBUILD-01)

`cargo test -p ostro-conformance --features polyglot --test rebuild -- --test-threads=1`
(`crates/ostro-conformance/tests/rebuild.rs`, spec
`conformance/spec/engine/C-REBUILD-01.json`): every engine-leg scenario allowed for the
language is recorded on build A (the conformance cache) and its journals are replayed on a
fresh build B of the same source.

| Language | Build A vs B | Result |
|---|---|---|
| Rust | identical sha (asserted) | 27 scenarios, 29 journals replay with 0 appended events |
| Python (released toolchain) | different shas | 27 scenarios, 29 journals replay with 0 appended events, no NonDeterminism; C-NDET-01 stays NonDeterminism |
| Python (fork, `OSTRO_COMPONENTIZE_PY`) | different shas, same length | 27 scenarios, 29 journals replay with 0 appended events |
| TypeScript | different shas | 27 scenarios, 29 journals replay with 0 appended events |
| `py-setorder`, released toolchain | step order differs between builds | replaying A's journal on B ends in **NonDeterminism** (detected, never silently accepted) |
| `py-setorder`, fork (`OSTRO_COMPONENTIZE_PY`) | same step order (asserted) | A's journal replays on B |

So a rebuilt module of unchanged source replays recorded journals for the conformance
workflows even where the bytes differ. The one hazard is code whose command sequence
depends on the str hash secret, i.e. iteration over a `set`/`frozenset` of str (or of
objects hashing via str): with the released toolchain each build bakes a different secret.

**Guidance:** never feed an unsorted set of str into commands (step names, signal waits,
children, effects). Iterate `sorted(the_set)` or use a `dict`/`list` (insertion-ordered).
Deploying a rebuilt module for in-flight runs is a new module version anyway: the engine
detects any divergence as NonDeterminism instead of corrupting a run.

## Artefact for Phase 10

Phase 10's hermetic `ostro build` ships the **released componentize-py 0.25.1 wheel** from
PyPI (D-P9-30), with the reproducibility gap above documented: Python builds are not
bit-reproducible and the str hash secret differs per build. The D-P9-3 fork stays a
CI-built, opt-in toolchain (`OSTRO_COMPONENTIZE_PY`, built by
`sdks/scripts/build-componentize-py-fork.sh`), because no hosted fork artefact exists for
Phase 10 to download. Even with the fork the residual is 73–76 bytes (timestamps from the
WASI filesystem at pre-init); with the released wheel it is the whole snapshot layout.
CI does not build the fork (a cold build needs ~3.6 GB and ~6–30 min).

## Packaging (dry runs only)

Publishing to PyPI/npm is human_needed (D-P9-19): `uv publish`, `twine upload` and
`npm publish` are never run by plans or CI.

Python (`ostro` 0.1.0, pure-python wheel + sdist, hatchling 1.27.0):

```bash
cd sdks/python && uv build --out-dir ../../target/dist/python
python3 -m zipfile -l ../../target/dist/python/ostro-0.1.0-py3-none-any.whl
tar tzf ../../target/dist/python/ostro-0.1.0.tar.gz
```

- wheel: `ostro/{__init__,_bindings,_determinism,_host,ctx,effects,errors,export,json,loop,ops,saga,step}.py`,
  `ostro/py.typed`, `ostro/wit/runtime.wit`, `ostro-0.1.0.dist-info/{METADATA,WHEEL,RECORD}`;
  metadata `Name: ostro`, `Version: 0.1.0`, `License-Expression: Apache-2.0`,
  `Requires-Python: >=3.12`, no dependencies;
- sdist: the same package files plus `pyproject.toml`, `PKG-INFO`, `.gitignore`; no tests,
  examples or caches. No project URL (the repository has no public remote yet), no README
  until 09-20.

TypeScript (`ostro` 0.1.0, `"type": "module"`, Apache-2.0, no dependencies):

```bash
cd sdks/ts && npm pack --dry-run --json
```

- files: `package.json`, `rolldown.ostro.mjs`, `src/*.ts` (16 files), `wit/runtime.wit`;
  `README.md` is listed in `files` and appears once 09-20 writes it. No `test/`,
  `examples/` or `node_modules/`. The build toolchain (jco, Rolldown) lives in
  `sdks/scripts/toolchain`, not in the package.
