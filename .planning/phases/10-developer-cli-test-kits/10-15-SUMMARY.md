---
phase: 10-developer-cli-test-kits
plan: 15
subsystem: build-pipeline
tags: [ostro-build, python, typescript, docker, uv, componentize-py, jco, parity, toolchains]
requires:
  - phase: 10-developer-cli-test-kits
    provides: "10-12 pipeline (StepCtx, LangStep, step_for, run_streamed, block_on, write_atomic, scrubbed_env), 10-07 toolchain cache (ensure, Cache::{state_dir, lock}, musl_host, glibc_version), 10-08 SDK bundle (python/ostro, ts, toolchain lock)"
provides:
  - "ostro_build::lang::python::{PythonStep, COMMAND_LINE, componentize_args, check_pure, pyproject_dependencies, entry_module, UV_VERSION, COMPONENTIZE_PY_VERSION, HOST_PYTHON, GUEST_PYTHON, WORLD}"
  - "ostro_build::lang::ts::{TsStep, COMMAND_LINE, jco_args, npm_registry, JCO_VERSION, NPM_REGISTRY}"
  - "ostro_build::lang::docker::{DockerStep, DOCKERFILE, image_tag, docker_bin, linux_bin, elf_interp, ensure_image, run_args, IMAGE, SRC_MOUNT, CACHE_MOUNT, BIN_MOUNT}"
  - "deploy/build-image/Dockerfile (debian:12-slim@sha256 index digest + ca-certificates)"
  - "ostro_conformance::polyglot::{ostro_builder, ostro_bin} + the OSTRO_POLYGLOT_BUILDER=ostro branch of polyglot::build"
  - "docs/toolchains.md"
affects: [10-16, 10-17, 10-19, 10-20]
tech-stack:
  added: []
  patterns:
    - "argument templates as consts (COMMAND_LINE) with placeholder substitution: the parity test compares exactly what run() executes against build-guest.sh"
    - "builder state that embeds absolute paths (venvs) keyed by a hash of the canonical cache root, so the host and the --docker container (/cache) never share one"
key-files:
  created:
    - crates/ostro-build/tests/build_polyglot.rs
    - crates/ostro-build/tests/docker_shim.rs
    - crates/ostro-build/tests/templates_build.rs
    - crates/ostro/tests/parity.rs
    - deploy/build-image/Dockerfile
    - docs/toolchains.md
  modified:
    - crates/ostro-build/src/lang/{python,ts,docker}.rs
    - crates/ostro-build/src/pipeline.rs (step_for routing, OSTRO_COMPONENTIZE_PY in the input hash)
    - crates/ostro-build/tests/pipeline.rs (10-12 NotImplemented test replaced)
    - crates/ostro-conformance/src/polyglot.rs (append-only branch)
key-decisions:
  - "The componentize-py venv lives at state/cpy-venv-0.25.1-<sha8>/venv-<roothash8> (uv cache and CPython shared one level up): a venv is bound to its absolute path and --docker mounts the same cache at /cache"
  - "Dependencies install with `--python 3.12` (uv-managed) + `--only-binary :all: --python-version 3.14`; uv 0.7.15 `uv export` has no --require-hashes flag (hashes are emitted by default), only the install side uses --require-hashes"
  - "`-p <stage>/.deps` as a second componentize-py path works with 0.25.1: no copy-into-stage fallback needed"
  - "Mirror: UV_DEFAULT_INDEX=<mirror>/pypi/simple added next to UV_PYTHON_INSTALL_MIRROR (dependencies would otherwise bypass the mirror)"
  - "OSTRO_COMPONENTIZE_PY is folded into the set-id field of the input hash for Python, so toggling the fork rebuilds"
  - "--toolchain system for Python/TS prints a warning and uses the pinned toolchain (D-P10-4)"
  - "Docker: the docker CLI runs with the host process env; the container gets only HOME=/tmp, SOURCE_DATE_EPOCH, OSTRO_OFFLINE, OSTRO_TOOLCHAIN_MIRROR; the host copies the container's wasm and re-validates it; tool id `docker (ostro-build:<version>)`"
  - "OSTRO_DOCKER accepts a path or a bare name looked up on PATH (as ostro doctor)"
requirements-completed: [CLI-04]
duration: ~1h45m
completed: 2026-11-22
---

# Phase 10 Plan 15: Python/TS builders, --docker, docs/toolchains.md Summary

**`ostro build` now builds Python (uv 0.7.15 + uv-managed CPython 3.12 + the released componentize-py 0.25.1 wheel) and TypeScript (Node 24.13.0 + `npm ci --ignore-scripts` of the bundled jco lock + jco QuickJS) hermetically, with build-guest.sh's exact flags, and runs any build in a locally built `ostro-build` image with `--docker`. The Phase 9 conformance suites pass on ostro-built Python and TS artefacts.**

## Commits
- 3657e45 feat(10-15): hermetic Python builder (uv 0.7.15, managed CPython, componentize-py 0.25.1 wheel)
- af1b507 feat(10-15): hermetic TS builder, build-guest.sh parity, OSTRO_POLYGLOT_BUILDER=ostro
- 9c068ae feat(10-15): --docker runner, template builds, docs/toolchains.md

## Flag and env diff against build-guest.sh (HEAD)
| | build-guest.sh | ostro build | diff |
|---|---|---|---|
| py command | `"${cpy[@]}" -d "$WIT" -w ostro:runtime/workflow@0.1.0 componentize --stub-wasi -p "$STAGE/src" app -o "$OUT"` | `<venv>/bin/componentize-py -d <sdk>/python/ostro/wit -w ostro:runtime/workflow@0.1.0 componentize --stub-wasi -p <stage> [-p <stage>/.deps] <entry> -o <out>` | same tokens. The extra `-p .deps` appears only with dependencies. The WIT dir copy is byte-identical (`cmp`), and the entry comes from ostro.toml (default `app`) |
| py binary | `uv tool run --from componentize-py==0.25.1` (uv's default Python) or `OSTRO_COMPONENTIZE_PY` | venv with the sha-verified wheel on uv-managed CPython 3.12, or `OSTRO_COMPONENTIZE_PY` (warning) | the same released 0.25.1 wheel; ostro pins the host Python and verifies the wheel sha |
| ts command | `jco componentize "$STAGE/src/index.ts" --wit "$WIT" --world-name workflow --backend quickjs --backend-qjs-disable-async --backend-qjs-stub-wasi --bundle-config "$BUNDLE_CONFIG" -o "$OUT"` | `<toolchain>/node_modules/.bin/jco componentize <stage>/<entry> --wit <sdk>/ts/wit --world-name workflow --backend quickjs --backend-qjs-disable-async --backend-qjs-stub-wasi --bundle-config <toolchain>/rolldown.ostro.mjs -o <out>` | same tokens. The lock and rolldown config are byte-identical copies from the SDK bundle |
| npm install | `npm ci --silent --no-audit --no-fund` (scripts run) | `npm ci --ignore-scripts --no-audit --no-fund`, private npmrc + cache | `--ignore-scripts`: the lock has no install scripts, so the result is the same |
| env | `export PYTHONHASHSEED=0 PYTHONDONTWRITEBYTECODE=1 TZ=UTC LC_ALL=C SOURCE_DATE_EPOCH` over the inherited env | `env_clear` + exactly those plus `PATH=<tool dirs>:/usr/bin:/bin`, `HOME` (10-12 scrubbed env) | ostro is stricter: no inherited variables |
| cwd | `$STAGE/src` | `<project>/target/ostro/stage` | – |
| staging | sources + `sdks/python/ostro` as `ostro/` / `sdks/ts` as `node_modules/ostro`, mtimes = SDE | the same, from the embedded SDK bundle, mtimes = SDE | – |

`crates/ostro/tests/parity.rs::build_guest_flags_match` asserts the normalised token sequences and flag sets are equal (both modes). It also asserts that build-guest.sh's export line is a subset of `scrubbed_env`.

## Parity shas and residuals (`crates/ostro/tests/parity.rs`)
| Fixture | ostro build | build-guest.sh | Result |
|---|---|---|---|
| `ts-min` | b3fac419…a6c7, 1,494,729 B | 216c080d…12b7, 1,494,729 B | same imports/exports, **same length**, shas differ (residual: the 4-byte QuickJS snapshot value, `RESIDUAL-ACCEPTED: ts`; the test allows up to the recorded 45 B length spread) |
| `py-min` | 79d80966…384c, 18,449,687 B | 1bcb11db…6fa8, 18,513,677 B | same imports/exports; shas and lengths differ (Δ 63,990 B), as the released wheel does from run to run (`RESIDUAL-ACCEPTED: py`, up to 64 KB per docs/sdk-builds.md) |

## Conformance on ostro-built artefacts
`OSTRO_POLYGLOT_BUILDER=ostro OSTRO_BIN=<slot>/debug/ostro OSTRO_POLYGLOT_CACHE=<fresh> cargo test -p ostro-conformance --features polyglot --test polyglot_python --test polyglot_ts -- --test-threads=2`:
- **polyglot_python:** 8 passed (engine_leg_python, the v2 patch/ndet test and 6 server-leg tests).
- **polyglot_ts:** 7 passed (engine_leg_ts, engine_leg_ts_sm_smoke via build-guest.sh as designed, and 5 server-leg tests).
- **Cache:** the guests were built by ostro (`python-py-ostro-*`, `python-v2-py-ostro-*`, `ts-ts-ostro-*`, `ts-v2-ts-ostro-*`), and only `ts-ts-sm-*` came from build-guest.sh.

## Cold and warm timings (linux-x86_64, WSL2)
| Project | Cold (empty cache) | Up to date | Warm rebuild after an edit |
|---|---|---|---|
| Python template | 11.7 s (CLI, fresh `OSTRO_TOOLCHAIN_DIR`) | 0.25–0.28 s | 6.3–7.3 s |
| Python + attrs | – | – | 7.5 s |
| TS template | 22.1 s (Node download + npm ci) | 35–58 ms | 2.0–2.6 s |
| Python template `--docker` | 35 s (image build + container venv); 12.8 s once the image exists | – | – |
| TS template `--docker` (CLI) | 12 s (image build included, warm tools) | – | – |
| Rust template (system rustup, templates_build) | 19–23 s | – | – |

Cache size for Python + TS is about 1 GB: tools 262 MB, plus state 700 MB (jco node_modules 426 MB, npm cache 123 MB, CPython + venv 154 MB). The Python up-to-date check is slower than TS because the Python source set is every `*.py` file.

## npm postinstall findings (Pitfall 12)
No package in `sdks/scripts/toolchain/package-lock.json` (jco 1.42.0, componentize-js 0.23.0 and their dependencies) declares `preinstall`, `install` or `postinstall`. A grep over every installed `package.json` found none. jco componentize works after `npm ci --ignore-scripts`, so no allowlist is needed.

## `.deps` outcome
- componentize-py 0.25.1 accepts a second `-p <stage>/.deps`, so the 10-DECISIONS fallback (copying into the stage root) was not needed.
- `attrs==25.3.0` (py3-none-any) builds. `import attrs` runs at pre-init, and a workflow named `"attrs-" + attrs.__version__` shows up in describe as `attrs-25.3.0`.
- `pydantic-core==2.41.5` resolves to `cp314-cp314-manylinux_2_17_x86_64`. The build fails with `pydantic_core is not a pure-Python wheel (tag …); native extensions are not supported (Phase 10)`, and inputs.sha256 is not written.
- A version without cp314 wheels (2.33.2) fails inside uv. The error names the package and adds the pure-Python hint.

## Dockerfile digest source
`FROM debian:12-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc` is the multi-arch OCI index digest from `docker buildx imagetools inspect debian:12-slim` against docker.io on 2026-11-22. The linux/amd64 manifest is `sha256:c67e92c8…f018`. The image is built locally as `ostro-build:0.1.0` and never pushed.

## Tests run
- **Task 1 verify:** `build_polyglot -- python`: 4 passed. The plan's grep for the GitHub binary name (`componentize-py-v0.25.1-linux`) finds nothing. Clippy `-p ostro-build --features polyglot --all-targets -D warnings` is clean.
- **Task 2 verify:**
  - `build_polyglot`: 6 passed;
  - `ostro --features polyglot --test parity`: 3 passed;
  - `cargo build -p ostro`;
  - OSTRO_POLYGLOT_BUILDER=ostro polyglot_python 8 + polyglot_ts 7 passed;
  - clippy `-p ostro-build -p ostro-conformance --features polyglot --all-targets` clean.
- **Task 3 verify:** docker_shim 3 passed; templates_build 3 passed; Dockerfile contains `@sha256:`; docs/toolchains.md has 232 lines and every grep key; clippy `-p ostro-build --all-targets --all-features` clean.
- **Real Docker** (Docker Desktop WSL, 29.1.3): `OSTRO_TEST_DOCKER=1 OSTRO_LINUX_BIN=<slot>/debug/ostro … --test docker_shim real_docker_python_build` passed twice (35 s and 12.8 s). A CLI `ostro build tsc --docker --json` also succeeded (workflows `[hello@1]`, tool `docker (ostro-build:0.1.0)`). The binary was the dynamically linked debug build: it prints the PT_INTERP warning and runs because the image has glibc 2.36 and the host 2.35.
- **Also run:**
  - `cargo test -p ostro-build --locked` (all default suites green: lib 20, pipeline 9, docker_shim 3, build_rust 2, …);
  - the ci-local `check_ostro_build_polyglot` commands (see the self-check);
  - `cargo fmt --all --check`.

## Deviations from Plan
1. **[Rule 3 - Blocking] `pipeline.rs` edited (not in files_modified).** 10-12 left a private `NotYet` step instead of routing `step_for` to the lang modules, so `step_for` now routes Python, TS and Docker. Python's input hash also includes `OSTRO_COMPONENTIZE_PY`.
2. **[Rule 3 - Blocking] `tests/pipeline.rs::python_and_ts_steps_are_not_implemented_yet` replaced.** The new `python_and_ts_steps_need_their_toolchains_offline` asserts the routing: offline with an empty cache, Python reports `uv … is offline and not cached` and TS reports `node …`.
3. **The TS and Docker step sources landed in the Task 1 commit** (`step_for` references them), and their tests followed in Tasks 2 and 3.
4. **`uv export --require-hashes`** does not exist in uv 0.7.15 (hashes are emitted by default), so it is dropped from the export. `uv pip install --require-hashes -r` stays.
5. **Venv keyed by cache root** (`venv-<roothash8>` inside the plan's `cpy-venv-0.25.1-<sha8>`): a venv embeds absolute paths, and `--docker` mounts the cache at `/cache`.
6. **`UV_DEFAULT_INDEX=<mirror>/pypi/simple`** added to the mirror mapping (documented).
7. **Docker run env:** `-e HOME=/tmp` and `-e SOURCE_DATE_EPOCH` (the image has no git, so the container would fall back to 0) are passed as explicit values, not `-e NAME` pass-through.
8. **Python native-dep test** uses pydantic-core 2.41.5. It has cp314 wheels, so the tag scan (not uv's resolver) produces the error.
9. **The real Docker test exists** (`real_docker_python_build`, polyglot + `OSTRO_TEST_DOCKER=1` + `OSTRO_LINUX_BIN`, clear SKIP line otherwise), because Docker is available locally (coordinator note). The plan expected real Docker only in 10-19.
10. **Worktree, target dir and commits** follow the coordinator brief (scratchpad worktree, `target/slot-1`, worktree index + CAS) instead of `/tmp/ostro-wt-10-15`.

## Open Issues
- **Image tag:** `ostro-build:<version>` does not change when the Dockerfile changes within one version. A dev build with an edited Dockerfile needs `docker rmi` (documented). A content-hash label check could automate this.
- **Static musl binary:** `--docker` with the dynamically linked host ostro works only while the image's glibc (2.36) is at least the host's. The static musl binary and the < 60 MB gate are 10-19's.
- **Python and docker caches:** Python builds are not reproducible (inherited residual, documented). The `--docker` venv is separate from the host's (root hash), so the first container build creates its own venv (about 3 s).
- **CI dependencies:**
  - the polyglot step now also downloads uv, CPython 3.12, the wheel, Node and the jco lock into `target/ostro-toolchains-test` (about 1 GB with state) and reaches PyPI for the attrs/pydantic-core tests;
  - parity needs uv 0.7.15 + Node 24 on PATH (as the Phase 9 polyglot job);
  - templates_build's Rust template needs crates.io.
- **Not run:** the full `scripts/ci-local.sh --no-musl` (the 10-20 gate).

## ci-local polyglot step (`check_ostro_build_polyglot` commands, run by hand)
- **Clippy:** `-p ostro-build --features polyglot --all-targets` and `-p ostro --features polyglot --all-targets`, both clean.
- **`cargo test -p ostro-build --features polyglot -- --test-threads=2`:** all green.
  - New here: build_polyglot 6, docker_shim 4 (real Docker reports SKIP without OSTRO_TEST_DOCKER), templates_build 3.
  - Existing: lib 20, build_rust 3, build_rust_hermetic 1, pipeline 9, project 6, scaffold 6, sdk_bundle 6, toolchain_cache 13, toolchain_network 1.
- **`cargo test -p ostro --features polyglot --test parity --test replay_cli -- --test-threads=1`:** 3 + 6 passed.

## Self-Check: PASSED
- Files on main: crates/ostro-build/src/lang/{python,ts,docker}.rs, crates/ostro-build/tests/{build_polyglot,docker_shim,templates_build}.rs, crates/ostro/tests/parity.rs, crates/ostro-conformance/src/polyglot.rs (OSTRO_POLYGLOT_BUILDER branch), deploy/build-image/Dockerfile, docs/toolchains.md.
- Commits 3657e45, af1b507 and 9c068ae are on main.
