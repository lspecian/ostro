---
phase: 10-developer-cli-test-kits
plan: 12
subsystem: build-pipeline
tags: [ostro-build, ostro-build-pipeline, manifest, rust, rustup, zig, cli, validation]
requires:
  - phase: 10-developer-cli-test-kits
    provides: "10-07 toolchain cache (toolchain_set, ensure, Cache::{state_dir, lock}, FetchOptions); 10-08 sdk_bundle::materialize, project::load, templates::render; 10-02 BuildOptions/BuildOutput/BuildError/IGNORE contract; 10-03 BuildArgs + local_json"
provides:
  - "ostro_build::pipeline::{build, build_with, StepCtx, LangStep, step_for, resolve_mode, input_hash, scrubbed_env, run_streamed, block_on, write_atomic, is_ignored/ignored_dir/ignored_file, which_in, cc_on_path, INPUTS_FILE, MANIFEST_FILE, BASE_PATH}"
  - "BuildError::{Project, Toolchain, Io, Step{step, message, stderr_tail}, Validation} + is_usage()"
  - "ostro_build::stage::{source_files, stage, source_date_epoch}"
  - "ostro_build::validate::validate_and_describe(wasm) on Runtime::prepare + describe"
  - "ostro_build::manifest::{Manifest, Module, Workflow, Toolchains, KIND, VERSION, embedded_runtimes, read, write}"
  - "ostro_build::lang::rust::{RustStep, RUST_CHANNEL, WASM_TARGET, needs_zig, system_cargo, lib_name, host_triple, host_zig_triple}"
  - "ostro build verb (commands::build::{run, render, outcome_of, BuildArgs::{mode, explicit_mode, options}})"
  - "crates/ostro-build/tests/fixtures/rust-offline (hello workflow, committed Cargo.lock) for 10-16/10-18"
affects: [10-15, 10-16, 10-17, 10-18, 10-20]
tech-stack:
  added: []
  patterns:
    - "host env injected as a closure (build_with(opts, host, steps)) so tests never mutate the process env, as in 10-07"
    - "async work (ensure, cache lock, describe) on a fresh current-thread runtime inside a scoped thread: safe from inside or outside a tokio runtime"
key-files:
  created:
    - crates/ostro-build/tests/pipeline.rs
    - crates/ostro-build/tests/build_rust.rs
    - crates/ostro-build/tests/build_rust_hermetic.rs
    - crates/ostro-build/tests/fixtures/rust-offline/{Cargo.toml,Cargo.lock,ostro.toml,rust-toolchain.toml,src/lib.rs}
    - crates/ostro/tests/build_cli.rs
  modified:
    - crates/ostro-build/src/{pipeline,stage,manifest,validate}.rs
    - crates/ostro-build/src/lang/rust.rs
    - crates/ostro-cli/src/commands/build.rs
    - crates/ostro-cli/src/commands/mod.rs (stub test only)
    - crates/ostro/tests/p10_scaffold.rs (stub test only)
    - crates/ostro-build/tests/scaffold.rs (10-02 stub test only)
key-decisions:
  - "Local validation = Runtime::prepare (the same validate_loaded path as upload's prepare_cached, ENG-1 included) + Runtime::describe on a throwaway runtime: class Mixed, pool disabled (no reservation), no AOT dir, default process-local token key"
  - "The input hash is recomputed after the step and that value is recorded: cargo may create Cargo.lock, which is a source; otherwise a lockless project would never be up to date"
  - "SOURCE_DATE_EPOCH is not part of the input hash; the manifest records the value the build used (a commit without source changes does not force a rebuild)"
  - "The resolved mode (system/hermetic, never auto) is hashed, so switching toolchains rebuilds"
  - "Python/TS/Docker map to a private NotYet step in pipeline.rs (NotImplemented(\"10-15\")); lang/python.rs, ts.rs, docker.rs untouched"
  - "OSTRO_NO_CC=1 forces the zig fallback (the --no-cc test masks cc with failing stubs, which are still executables)"
  - "The scrubbed env is exactly TZ, LC_ALL, PYTHONHASHSEED, PYTHONDONTWRITEBYTECODE, SOURCE_DATE_EPOCH, PATH=/usr/bin:/bin, HOME; the Rust step adds its own tool dirs to PATH plus CARGO_TARGET_DIR, RUSTUP_HOME/CARGO_HOME (hermetic, or passed through in system mode), CARGO_NET_OFFLINE and the proxy/SSL_CERT variables"
  - "--toolchain became Option (no clap default) so [build] toolchain applies when the flag is absent; BuildArgs::mode() still returns auto then (10-03 test unchanged)"
requirements-completed: []
duration: ~2h
completed: 2026-11-22
---

# Phase 10 Plan 12: Build pipeline and Rust builder Summary

**`ostro build` runs one pipeline for every language: input-hash cache, fresh staging with `SOURCE_DATE_EPOCH` mtimes, the scrubbed D-P9-3 env, local validation and describe through `ostro_engine`, and an atomic `<name>.wasm` + `manifest.json` + `inputs.sha256`. The Rust step uses the system rustup or a hermetic rustup-init, with a zig 0.17.1 `cc`/`ar` shim when the host has no C compiler.**

## Commits
- 6983af8 feat(10-12): build pipeline with input-hash cache, staging, env scrub, local validation and manifest
- 224d88e feat(10-12): Rust builder with hermetic rustup and zig cc fallback; ostro build verb
- 97bea5c test(10-12): the 10-02 build stub test checks the real pipeline's usage error
- 7b4f90e fix(10-12): resolve a relative project dir before running the language step

## Input-hash file sets (`stage::source_files`, also what gets staged)
- **Rust:** `Cargo.toml`, `Cargo.lock`, `build.rs`, `rust-toolchain.toml`, `rust-toolchain`, `src/**`.
- **Python:** `**/*.py` except under the top-level `tests/`, plus `pyproject.toml`, `uv.lock` and the top-level `requirements*.txt`.
- **TS:** `src/**`, `package.json`, `package-lock.json`, `tsconfig.json`.
- **Always:** `ostro.toml`, and the `[project] entry` when it names a file (Python: also `<entry>.py`).
- `pipeline::IGNORE` drops paths at any depth: `target/`, `node_modules/`, `__pycache__/`, `.ostro/`, `.git/`, `.venv/`, `venv/`, `.tox/`, `.mypy_cache/`. It also drops the editor temp files `*~`, `.#*`, `*.swp` and `4913`. Symlinked dirs are not followed.
- **Hash:** sha256 over length-prefixed fields. The fields are `ostro.inputs/v1`, lang, name, the resolved mode, the toolchain-set id and the SDK bundle id, then each sorted relative path with its bytes.

## Manifest sample (the offline fixture, `ostro build` at 7b4f90e)
```json
{
  "kind": "ostro.manifest",
  "version": 1,
  "name": "hello-offline",
  "lang": "rust",
  "module": { "sha256": "de70e8baf8e9d5af8df91e29f38f8ee8fa78c81d347e6776ffc699a7bdd19c87", "bytes": 148424 },
  "workflows": [ { "name": "hello", "version": 1 } ],
  "toolchains": { "set_id": "e0df6679af5744b9", "tools": ["system-rustup"] },
  "sdk_bundle": "c03564ede0f0dea1",
  "source_date_epoch": 0,
  "input_hash": "3d8029da9d5349abc2d8dd1c103b2d66b6b6507b979fc06bddd3deddae63783a",
  "embedded_runtimes": []
}
```
(The file itself is pretty-printed with one key per line, in this order.) The human output is `built hello-offline sha256=de70e8baf8e9 bytes=148424 workflows=[hello@1] (toolchain: system-rustup)` followed by the wasm path, or `up to date: …`.

## Build times and sizes (linux-x86_64, WSL2, system rustup 1.100.0, CARGO_BUILD_JOBS=4 under nice)
| project | cold | up-to-date check | warm rebuild after an edit | size |
|---|---|---|---|---|
| offline fixture (`hello-offline`) | 18–20 s | 13 ms | 1.6–1.9 s | 148,424 B |
| `ostro init --lang rust` template (`template_build`, polyglot) | 19–20 s | 12.6 ms | – | 176,268 B |
| fixture, hermetic + zig, no cc (polyglot) | 67 s (cold cache: rustup-init, Rust 1.100.0 minimal + wasm32-wasip2, zig, crates) | – | – | 148,648 B |

## Reproducibility check
- **Same toolchain:** the fixture rebuilt after edit, then broken, then restored, gave the same sha (`de70e8ba…`, asserted in `build_rust.rs`). An `ostro build` of a fresh copy in another directory also gave `de70e8ba…`.
- **Hermetic toolchain:** the zig-linked build has a different sha (148,648 B vs 148,424 B). The toolchain install and CARGO_HOME differ, so the shas are not expected to match. Byte-identical output across system and hermetic toolchains is not claimed.

## zig shim
- **Files:** `state/zig-shim/cc` and `ar`, POSIX sh, 0755, written atomically and rewritten only when the content changes.
- **Credit:** the `cc` header credits cargo-zigbuild (MIT OR Apache-2.0, `src/zig.rs`) and lists every rule:
  - `-lgcc_s` becomes `-lunwind`;
  - `-lgcc_eh` is dropped;
  - `--target=` and `-target <triple>` are mapped to zig's spelling (`*-linux-gnu` → `<arch>-linux-gnu`, `*-linux-musl*` → `<arch>-linux-musl`, `*-apple-darwin` → `<arch>-macos`). One explicit `-target` is always passed, the host's by default;
  - `-fuse-ld=*` and `-B<dir>` are dropped (rustc's self-contained gcc-ld);
  - `-Wl,--no-undefined-version`, `-Wl,-znostart-stop-gc`, `-Wl,--disable-auto-image-base` and `-Wl,--dynamicbase` are dropped;
  - on macOS, `-Wl,-exported_symbols_list,F` is split in two and `-Wl,-dylib` is dropped.
- **ar:** `ar` execs `zig ar`.
- **Env:** the step sets `CC`, `AR` and `CARGO_TARGET_<HOST>_LINKER`, puts the shim dir first on PATH and prints one info line.
- **Logging:** with `OSTRO_ZIG_SHIM_LOG` set (passed through), each call appends its argv to that file.
- **Hermetic run:** 17 shim calls, all host links of build scripts and proc-macros. Rust 1.100's x86_64-linux-gnu link line carries `-fuse-ld=lld`, `-B…/gcc-ld` and `-lgcc_s`, and the filtering rules handle all three.

## Hermetic test
**Run, not deferred.** `cargo test -p ostro-build --features polyglot --test build_rust_hermetic` passed in 67.2 s on a cold cache. The setup:
- **PATH:** a constructed dir of basic utilities, with `cc`, `gcc` and `clang` as failing stubs;
- **OSTRO_NO_CC:** set to 1;
- **HOME:** a temp dir;
- **Cache:** `target/ostro-toolchains-test`.

The manifest tools were `rustup-init@1.29.3`, `rust@1.100.0` and `zig@0.17.1`.

## Tests
- `tests/pipeline.rs` (9 tests, fake steps over guests/smoke and guests/wasi-forbidden):
  - the input hash is stable; it changes with a source byte, a new source, a lockfile, ostro.toml, the mode, the set id or the bundle id; it ignores the 12 noise paths;
  - the Python and TS source sets;
  - the ignore rules;
  - a good build: manifest field order and values, inputs.sha256, the exact scrubbed env keys, SOURCE_DATE_EPOCH = 0 outside git, a fresh stage with mtime 0, up to date with the step not called and under 2 s, rebuilds after an edit or a tampered wasm;
  - a forbidden import gives `Validation` and writes no manifest, inputs file, wasm or temp file;
  - a failing step keeps the old wasm and manifest and is not up to date;
  - no project is a usage error;
  - SOURCE_DATE_EPOCH from the env reaches the stage mtimes and the manifest;
  - Python is `NotImplemented("10-15")`.
- `tests/build_rust.rs` (offline system toolchain, 2 tests; polyglot adds `template_build`): the fixture build is cached, rebuilt, fails on a compile error and reproduces its sha; a project without Cargo.lock builds with the warning, the generated lock is recorded and the next run is up to date.
- `crates/ostro/tests/build_cli.rs` (3 tests + 2 p10 helper tests):
  - `--json` gives the `ostro.build.result` envelope, then `up_to_date`, then the human `up to date:` line;
  - `/nonexistent` and a bad ostro.toml exit 2;
  - a compile error exits 1 with the diagnostics on stderr;
  - `std::fs` in a workflow (wasi:filesystem) exits 1 with the validator's message, through a relative DIR.
- Plan verify blocks:
  - Task 1 `--test pipeline`: 9 passed, clippy clean.
  - Task 2: build_rust offline 2 passed; build_cli 5 passed; polyglot build_rust_hermetic 1 passed and build_rust 3 passed, no FAILED; clippy `-p ostro-build -p ostro-cli -p ostro --all-targets --all-features -D warnings` clean.
- Also run:
  - `cargo test -p ostro-build --locked`: all suites green (lib 13, build_rust 2, pipeline 9, project 6, scaffold 6, sdk_bundle 6, toolchain_cache 13);
  - `cargo test -p ostro-cli --locked`: 72 + 10 + 6;
  - `cargo test -p ostro --test p10_scaffold`: 6;
  - `cargo fmt --all --check`;
  - `cargo check --workspace --all-targets --locked` after rebasing onto 10-06/10-11: warning-free.
- **Not run:**
  - the full `scripts/ci-local.sh --no-musl` (the 10-20 gate covers it);
  - the full `cargo test -p ostro` (only build_cli and p10_scaffold).

## Deviations from Plan
1. **[Rule 1 - Bug] Relative project dir.** `ostro build p` resolved `CARGO_TARGET_DIR` against cargo's cwd. `build_with` now makes the dir absolute, and build_cli covers it (7b4f90e).
2. **[Rule 1 - Bug] Lockless projects were never up to date.** cargo created Cargo.lock during the build, and Cargo.lock is a source. The input hash is now recomputed after the step.
3. **[Rule 3 - Blocking] Stub tests outside files_modified edited.**
   - `commands/mod.rs::local_stubs_exit_2` and `p10_scaffold.rs::stubs_exit_2_naming_their_plan` held only the `build` row once 10-11 removed `replay`, so both tests were removed.
   - The 10-02 `tests/scaffold.rs::build_stub_answers_not_implemented` became `build_without_a_project_is_a_usage_error`.
4. **Fixture `rust-toolchain.toml` added** (not listed). Without it, cargo in a temp dir used the default rustup toolchain (1.94.1 here). With it, the fixture has the template's shape and exercises "auto honours rust-toolchain.toml".
5. **Python/TS/Docker placeholder in pipeline.rs** (a private `NotYet` step) instead of stubs in `lang/{python,ts,docker}.rs`, which stay untouched for 10-15.
6. **API additions:**
   - `build_with` (host env + step factory injection) and the `StepCtx.host` field;
   - `resolve_mode`, `run_streamed`, `block_on` and `write_atomic`, public for 10-15;
   - `BuildArgs::{explicit_mode, options}`;
   - `OSTRO_NO_CC` and `OSTRO_ZIG_SHIM_LOG`.
7. **Task 1's commit also carried `lang/rust.rs`**, because `step_for` references `RustStep`. Task 2 carried its tests, the fixture and the verb.
8. **Worktree and target dir** follow the coordinator brief (scratchpad worktree, `target/slot-1`).

## Open Issues
- **CLI-04 stays Pending.** Python, TS and `--docker` are 10-15. `docs/toolchains.md` (mirror rustup variables, `OSTRO_NO_CC`) is also 10-15.
- **The fixture's committed Cargo.lock pins the bundled SDK's dependency set.** If `sdks/rust/ostro-sdk` changes its dependencies, `--locked` fails and the lock must be regenerated. Regenerate it offline in a temp copy: copy `sdks/rust/ostro-sdk{,-macros}` into `target/ostro/sdk/rust/`, then run `cargo generate-lockfile --offline`.
- **The Rust step does not pre-check the wasm32-wasip2 target in system mode.** A missing target shows cargo's own diagnostic.
- **CI dependencies:**
  - `build_cli.rs` and `build_rust.rs` need rustup plus the wasm32-wasip2 target in every job that runs `cargo test -p ostro` / `-p ostro-build` (CI already builds guests for that target);
  - the polyglot step now also downloads Rust 1.100.0 and zig into `target/ostro-toolchains-test` (about 1 GB) and fetches crates for `template_build`.

## Self-Check: PASSED
- Files present on main: src/{pipeline,stage,manifest,validate}.rs, lang/rust.rs, tests/{pipeline,build_rust,build_rust_hermetic}.rs, tests/fixtures/rust-offline/*, crates/ostro/tests/build_cli.rs, commands/build.rs.
- Commits 6983af8, 224d88e, 97bea5c, 7b4f90e are on main.
