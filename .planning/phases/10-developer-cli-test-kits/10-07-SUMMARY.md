---
phase: 10-developer-cli-test-kits
plan: 07
subsystem: build-toolchains
tags: [toolchain, cache, sha256, supply-chain, extraction, locking, cli, doctor]
requires:
  - phase: 10-developer-cli-test-kits
    provides: "10-02 ostro-build skeleton + fixture HTTP server; 10-03 toolchain/doctor verb stubs, local_json, commands::block_on"
provides:
  - "crates/ostro-build/toolchains.toml: uv 0.7.15, componentize-py 0.25.1 wheels, Node 24.13.0, rustup-init 1.29.3, zig 0.17.1 for linux/macos x86_64/aarch64, each sha from its official checksum source"
  - "ostro_build::toolchain::{Platform, Pin, ArchiveKind, ToolchainSet{id,pins}, ToolchainError, toolchain_set, ensure, tools_for, tools_for_with, cc_available, glibc_version, musl_host, which}"
  - "ostro_build::cache::{Cache{resolve,resolve_with,with_root,tools_dir,state_dir,entry_dir,entry_name,is_installed,lock}, CacheLock, EntryInfo, entries, prune}"
  - "ostro_build::fetch::{FetchOptions{new,from_env,from_env_with,client_for,effective_url}, download_verified, mirror_url}; ostro_build::extract::extract"
  - "ostro toolchain install|list|prune and ostro doctor (ostro.toolchain.result / ostro.doctor.result)"
affects: [10-12, 10-15, 10-18, 10-20]
tech-stack:
  added: []
  patterns:
    - "Env lookup injected as a closure (Cache::resolve_with, FetchOptions::from_env_with, commands::toolchain::Env) so tests never mutate the process env"
    - "process-wide tokio mutex keyed by canonical root + File::lock on spawn_blocking"
key-files:
  created:
    - crates/ostro-build/toolchains.toml
    - crates/ostro-build/tests/toolchain_cache.rs
    - crates/ostro-build/tests/toolchain_network.rs
  modified:
    - crates/ostro-build/src/{cache,fetch,extract,toolchain}.rs
    - crates/ostro-cli/src/commands/toolchain.rs
    - crates/ostro-cli/src/commands/mod.rs (test list only)
    - crates/ostro/tests/p10_scaffold.rs (stub rows only)
key-decisions:
  - "Lock = process-wide LazyLock<Mutex<HashMap<PathBuf, Arc<tokio::sync::Mutex<()>>>>> keyed by the canonical root, then std::fs::File::lock (exists on 1.100) on <root>/.lock via tokio::task::spawn_blocking; no fallback needed"
  - "cache::prune returns Result<Vec<PathBuf>, ToolchainError> (the interface sketch said Vec<PathBuf>): a lock/remove failure must not be silent"
  - "rustup-init pinned at 1.29.3 (release-stable.toml at implementation time)"
  - "install with neither --lang nor --all = the tools of all three languages (zig only without cc); --all = every pin of the platform"
  - "Tmp dirs are tools/.tmp-<pid>-<n>; under the lock a tmp dir of a dead pid (/proc) or of this pid is stale; without /proc, older than 1 h"
  - "Extraction does not use unpack_in (it cannot apply strip and resolves hardlinks itself): each entry is validated, written via the canonical parent, symlinks/hardlinks created by hand; setuid/setgid bits dropped; device/fifo entries rejected"
requirements-completed: [CLI-04]
duration: ~1h30m
completed: 2026-11-22
---

# Phase 10 Plan 07: Hermetic toolchain cache Summary

**sha256-pinned toolchains for four platforms, streamed-verify-then-extract downloads with timeouts and a size cap, extraction that rejects traversal/absolute/symlink/hardlink escapes, atomic installs serialised by a process mutex plus file lock, offline and mirror modes, and the `ostro toolchain` / `ostro doctor` verbs.**

## Commits
- 553ffe7 feat(10-07): verified toolchain cache with safe extraction, lock, offline and mirror
- ab32748 feat(10-07): pinned toolchain set, ostro toolchain/doctor verbs, network smoke

## Pin table
Toolchain-set id at landing: `e0df6679af5744b9`.

| tool | version | platform | sha8 | source |
|---|---|---|---|---|
| uv | 0.7.15 | linux-x86_64 | c97afc12 | github.com/astral-sh/uv/releases/download/0.7.15/uv-x86_64-unknown-linux-musl.tar.gz.sha256 |
| uv | 0.7.15 | linux-aarch64 | 4351c1e2 | …/uv-aarch64-unknown-linux-musl.tar.gz.sha256 |
| uv | 0.7.15 | macos-aarch64 | 7a20f3d3 | …/uv-aarch64-apple-darwin.tar.gz.sha256 |
| uv | 0.7.15 | macos-x86_64 | 4c7c1fe1 | …/uv-x86_64-apple-darwin.tar.gz.sha256 |
| componentize-py | 0.25.1 | linux-x86_64 | 8623cd8c | pypi.org/pypi/componentize-py/0.25.1/json (manylinux_2_28_x86_64 wheel) |
| componentize-py | 0.25.1 | linux-aarch64 | 7b6a87c5 | same (manylinux_2_28_aarch64) |
| componentize-py | 0.25.1 | macos-aarch64 | a5569915 | same (macosx_11_0_arm64) |
| componentize-py | 0.25.1 | macos-x86_64 | 65fbc4a9 | same (macosx_10_12_x86_64) |
| node | 24.13.0 | linux-x86_64 | e7985996 | nodejs.org/dist/v24.13.0/SHASUMS256.txt (linux-x64.tar.xz) |
| node | 24.13.0 | linux-aarch64 | aa881151 | same (linux-arm64.tar.xz) |
| node | 24.13.0 | macos-aarch64 | d595961e | same (darwin-arm64.tar.gz) |
| node | 24.13.0 | macos-x86_64 | 6f03c1b4 | same (darwin-x64.tar.gz) |
| rustup-init | 1.29.3 | linux-x86_64 | b1c8cb14 | static.rust-lang.org/rustup/archive/1.29.3/x86_64-unknown-linux-gnu/rustup-init.sha256 |
| rustup-init | 1.29.3 | linux-aarch64 | 9f608c14 | …/aarch64-unknown-linux-gnu/rustup-init.sha256 |
| rustup-init | 1.29.3 | macos-aarch64 | 63752a19 | …/aarch64-apple-darwin/rustup-init.sha256 |
| rustup-init | 1.29.3 | macos-x86_64 | bc107f1f | …/x86_64-apple-darwin/rustup-init.sha256 |
| zig | 0.17.1 | linux-x86_64 | 3581863e | ziglang.org/download/index.json ("0.17.1".x86_64-linux.shasum) |
| zig | 0.17.1 | linux-aarch64 | a1546d56 | same (aarch64-linux) |
| zig | 0.17.1 | macos-aarch64 | 75f1bc7a | same (aarch64-macos) |
| zig | 0.17.1 | macos-x86_64 | 9af07b20 | same (x86_64-macos) |

- **Node pin = the 09-17 CI pin:** yes, 24.13.0 (ci.yml `node-version: "24.13.0"`, as recorded in 10-01-SUMMARY).
- CPython is not pinned (uv installs and verifies it); the jco toolchain is not pinned here (10-15 runs `npm ci` against the embedded lockfile). Both are noted in toolchains.toml.
- The componentize-py pins are PyPI wheels only; no GitHub release binary appears (unit test asserts it).

## Lock mechanism used
`Cache::lock()` creates `<root>/tools`, canonicalises the root, takes the per-root `tokio::sync::Mutex` from a process-wide `LazyLock<Mutex<HashMap<PathBuf, Arc<tokio::sync::Mutex<()>>>>>` (`lock_owned`), then opens `<root>/.lock` and calls `std::fs::File::lock` inside `tokio::task::spawn_blocking`. `CacheLock` unlocks the file on drop, then releases the mutex guard. The runtime never blocks; the documented create-exclusive fallback was not needed (`File::lock` is stable on 1.100). Stale `.tmp-*` dirs are removed right after the lock is taken.

## Network smoke timing
`cargo test -p ostro-build --features polyglot --test toolchain_network -- --nocapture` on linux-x86_64 (WSL2), cold cache: uv 0.7.15 downloaded+verified+extracted in 1.10 s, the componentize-py wheel in 1.80 s, cache hit 27 µs; whole test 2.9 s. `uv --version` printed `uv 0.7.15`.

A one-off scratch run (not committed) also installed every linux-x86_64 pin from the real URLs: node 8.2 s (bin/npm, npx, corepack internal symlinks kept; `node --version` = v24.13.0), rustup-init 0.4 s (ELF, 0755), zig 17.2 s (`zig version` = 0.17.1). So every strip prefix for this platform is confirmed; the macOS/aarch64 prefixes follow the same naming and are checked by unit tests only.

## Tests
- `cargo test -p ostro-build --locked`: lib 13 (pins: every platform has every tool, versions match D-P10-2/3/4, 64-hex shas, https URLs, unique shas, manylinux_2_28 wheels, stable id, bad-pin parse rejection, glibc parsing, root precedence, mirror rewrite, env parsing, lexical normalisation), scaffold 6, **toolchain_cache 13** (good tar.gz + 0-request cache hit, sha mismatch leaves nothing, traversal/abs/symlink-escape/symlink-chain/hardlink-escape rejected with the right entry and reason and nothing written outside, internal symlink allowed, tar.xz + binary 0755 + wheel verify-only, offline cached/uncached with 0 requests, mirror redirect + tampered mirror ShaMismatch, concurrent `join!` and two `Cache` handles each download once under a 30 s timeout, size cap TooLarge with cleanup, read-idle 1 s timeout under 5 s plus total timeout, 404, prune with/without state, dead-pid tmp + incomplete entry cleaned). All pass, ~1.7 s.
- `--features polyglot`: + toolchain_network 1 passed.
- `cargo test -p ostro-cli --locked`: 70 + 6 passed (5 new toolchain tests: list with unknown entries + envelope, prune state handling, offline install cached/uncached, install selection, doctor with fake OSTRO_DOCKER and mirror).
- `cargo test -p ostro --test p10_scaffold`: 7 passed.
- `cargo clippy -p ostro-build -p ostro-cli --all-targets --all-features --locked -D warnings`: clean. `cargo fmt --all --check`: clean. `cargo check --workspace --all-targets --locked` after rebasing onto 10-05: warning-free. No `dirs::` in ostro-build.
- Full `scripts/ci-local.sh --no-musl` not run (the phase runs it at 10-20); the ci-local/ci.yml "ostro-build polyglot" step already runs `cargo test -p ostro-build --features polyglot`, so the network smoke is picked up there.

## Deviations from Plan
1. **[Rule 3 - Blocking] 10-03 stub tests edited.** `commands/mod.rs::local_stubs_exit_2` and `p10_scaffold.rs::stubs_exit_2_naming_their_plan` asserted that toolchain/doctor exit 2 (the p10 one would also have run `toolchain install --all` against the real network). Their toolchain/doctor rows were removed; nothing else in those files changed. Both files are outside `files_modified`.
2. **Task split.** cache.rs needs the `Pin`/`ToolchainError` types and `toolchain_set` reads toolchains.toml via `include_str!`, so toolchain.rs and toolchains.toml landed with Task 1; Task 2 carried the CLI verbs and the network smoke.
3. **`prune` returns `Result<Vec<PathBuf>, ToolchainError>`** instead of `Vec<PathBuf>`; `ensure`, `Cache`, `FetchOptions`, `Pin`, `ToolchainSet`, `entries` keep the sketched names and shapes.
4. **Stale tmp test uses `.tmp-4194399-0`** (above Linux PID_MAX_LIMIT) instead of `.tmp-123`, which may be a live pid on the host.
5. Worktree/target dir per the coordinator brief (scratchpad worktree, `target/slot-1`) instead of `/tmp/ostro-wt-10-07`.
6. `ToolchainError` is not re-exported at the crate root (lib.rs is outside this plan's files); use `ostro_build::toolchain::ToolchainError`.

## Notes for consumers (10-12, 10-15)
- Call `ensure(&Cache::resolve()?, set.pin(tool).unwrap(), &FetchOptions::from_env()?)`; the returned dir holds the stripped tree (uv: `uv`, `uvx`; node: `bin/node`; zig: `zig`; rustup-init: `rustup-init`; wheel: `<wheel file name>`).
- Builder state goes in `cache.state_dir(name)`; take `cache.lock()` around state mutations if they can race.
- `tools_for(Lang::Rust)` adds Zig only when no cc/gcc/clang is on PATH.

## Self-Check: PASSED
- Files present on main: crates/ostro-build/toolchains.toml, src/{cache,fetch,extract,toolchain}.rs, tests/toolchain_cache.rs, tests/toolchain_network.rs, crates/ostro-cli/src/commands/toolchain.rs.
- Commits 553ffe7 and ab32748 are on main.
