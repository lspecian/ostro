---
phase: 10-developer-cli-test-kits
plan: 02
subsystem: build-contract
tags: [cargo, deny, ci, ostro-build, ostro-client, fixture-server, contract]
requires:
  - phase: 10-developer-cli-test-kits
    provides: "10-01 gate (D-P10-1..21, D-P10-11 pins, stale-blob rule, Phase 10/11/12 coordination)"
provides:
  - "Every Phase 10 Rust dependency declared (workspace pins + member manifests); later Phase 10 plans never edit Cargo/deny files"
  - "deny.toml: one crate-scoped exception { allow = [\"CC0-1.0\"], crate = \"notify\" } (USER REVIEW D-P10-11)"
  - "crates/ostro-build: every module declared with its owner, contract stubs (Tool, ToolchainSet, Lang, ToolchainMode, BuildOptions, BuildOutput, BuildError, build() -> NotImplemented(\"10-12\"), pipeline::IGNORE, sdk_bundle::SDK_BUNDLE), build.rs stub"
  - "crates/ostro-client: bundleable manifest (explicit versions), `testing` feature stub"
  - "ostro-cli watch.rs stub (WatchOptions, run_watch -> Err(\"not yet implemented (10-16)\"))"
  - "crates/ostro-build/tests/common/fixture_server.rs: deterministic in-code archives + request counters + tampered mirror"
  - "ci-local: --no-go, clients python/ts/go steps (SKIP \"scaffold pending (10-04)\" until 10-04), ostro-build polyglot step; ci.yml polyglot job gains setup-go + the same steps"
affects: [10-03, 10-04, 10-07, 10-08, 10-09, 10-12, 10-15, 10-16, 10-17, 10-19, 10-20]
tech-stack:
  added: [notify 8.2.2, notify-debouncer-full 0.7.0, clap_complete 4.6.12, flate2 1.1.11 (rust_backend), lzma-rust2 0.25.0]
  patterns: ["contract crate with per-module owner doc comments", "fixture archives generated in code with raw tar headers (unsafe paths bypass tar's validation)"]
key-files:
  created:
    - crates/ostro-build/Cargo.toml
    - crates/ostro-build/build.rs
    - crates/ostro-build/src/{lib,cache,fetch,extract,toolchain,sdk_bundle,project,templates,pipeline,stage,manifest,validate}.rs
    - crates/ostro-build/src/lang/{mod,rust,python,ts,docker}.rs
    - crates/ostro-build/tests/common/fixture_server.rs
    - crates/ostro-build/tests/scaffold.rs
    - crates/ostro-client/Cargo.toml
    - crates/ostro-client/src/lib.rs
    - crates/ostro-cli/src/watch.rs
  modified:
    - Cargo.toml
    - Cargo.lock
    - deny.toml
    - crates/ostro-cli/Cargo.toml
    - crates/ostro-cli/src/lib.rs
    - crates/ostro/Cargo.toml
    - crates/ostro/src/main.rs
    - scripts/ci-local.sh
    - .github/workflows/ci.yml
key-decisions:
  - "reqwest TLS = the `rustls` feature (Phase 6/7 choice: aws-lc-rs + platform verifier) on ostro-build's and ostro-client's reqwest lines; webpki-roots absent"
  - "toml spec `toml = \"1.2\"` (copied from ostro-engine/ostro-partition; one toml 1.2.1 in the lock)"
  - "tar keeps the Phase 8 workspace pin `tar = \"0.4.46\"` (already 0.4.46 in the lock); not rewritten to `=0.4.46`"
  - "main.rs anchor WAS needed: cargo 1.100's `cargo::unused_dependencies` lint warns on clap_complete (ostro-build is not flagged because the polyglot feature references it); the plan's one line `use {clap_complete as _, ostro_build as _};` was added"
  - "Lang/ToolchainMode live in project.rs, Tool/ToolchainSet in toolchain.rs, BuildOptions/BuildOutput/BuildError/build in pipeline.rs, all re-exported at the crate root"
  - "Clients python/ts steps follow --no-polyglot (they need the polyglot toolchains, and run in the ci.yml polyglot job); clients go skips only with --no-go"
  - "The ostro-build polyglot step runs the ostro crate's polyglot targets (parity, replay_cli, template_tests) by name once each file exists, not the whole ostro test suite single-threaded"
  - "TS client tests run via the glob 'clients/ts/test/**/*.test.js': `node --test <dir>/` needs a test/index.js (as sdks/ts has)"
patterns-established:
  - "Test targets include the fixture server with `#[path = \"common/fixture_server.rs\"] mod fixture_server;`"
duration: ~1h15m
completed: 2026-11-22
---

# Phase 10 Plan 02: Contract A (Cargo/deny/ci) Summary

**Every Phase 10 dependency (notify/notify-debouncer-full/clap_complete/flate2/lzma-rust2 exact pins), a crate-scoped CC0 deny exception for notify, the ostro-build and ostro-client skeleton crates, a deterministic fixture HTTP server with 13 archive and network cases, and the ci-local/ci.yml client and ostro-build polyglot steps.**

## Performance
- 2026-11-22, about 1h15m. Three task commits, each rebased onto a moving main and landed with a CAS update-ref. Parallel agents were 11-18, 12-03, 12-04 and 12-05. None of them touched Cargo, deny or ci files during this plan.

## Accomplishments

### Dependency diff
- **Root `[workspace.dependencies]`:** `notify = "=8.2.2"`, `notify-debouncer-full = "=0.7.0"`, `clap_complete = "=4.6.12"`, `flate2 = { version = "=1.1.11", default-features = false, features = ["rust_backend"] }`, `lzma-rust2 = "=0.25.0"`. tar keeps its existing `0.4.46` pin.
- **ostro-build:**
  - deps: ostro-engine, serde, serde_json, toml "1.2", sha2, reqwest (+rustls), tokio, tar, flate2, lzma-rust2, tempfile, thiserror, tracing;
  - build-deps: toml, sha2, tar;
  - dev-deps: axum, futures-util, tokio (macros, rt-multi-thread), insta, tempfile;
  - features: `default = []`, `polyglot = []`.
- **ostro-client:**
  - explicit `[package]` fields: 0.1.0, edition 2024, Apache-2.0, description, `publish = false`;
  - deps: reqwest `0.13.5` (json, http2, rustls), serde `1` (derive), serde_json `1`, sha2 `0.11`, thiserror `2`, tokio `1.54` (time, process, io-util, macros, rt), tracing `0.1`;
  - dev-deps: axum `0.8.9`, tokio (macros, rt-multi-thread), tempfile `3`;
  - features: `testing = []`;
  - the only `workspace = true` is `[lints]`.
- **ostro-cli:** + ostro-build, ostro-client, ostro-engine, notify, notify-debouncer-full; dev-dep insta.
- **ostro:**
  - deps: + ostro-build, clap_complete;
  - feature `polyglot = ["ostro-build/polyglot"]`;
  - dev-deps: + ostro-conformance, ostro-client (`testing`).
- **Cargo.lock:** adds only clap_complete, file-id, flate2, inotify(-sys), kqueue(-sys), lzma-rust2, notify, notify-debouncer-full, notify-types, objc2-core-services, simd-adler32, windows-sys 0.60.2 + windows-targets 0.53 family, and the two new crates. No existing package changed version.

### cargo tree / deny evidence
- `cargo deny check`: `advisories ok, bans ok, licenses ok, sources ok`. check-deny-fixtures.sh still rejects all 3 fixtures.
- `cargo tree -i <c> --workspace --locked -e normal,build --target all`:
  - dirs-sys, option-ext, webpki-roots and bincode match no package;
  - chrono and ring print "nothing to print".
  - chrono is in the lock only through the dev-dependency s3s-fs of ostro-objstore, which predates this plan. A plain `cargo tree -i chrono` (dev edges included) therefore finds it.
- `cargo tree -d --depth 0 -e normal --workspace --locked`: no duplicate of wasmtime, wit-component, wasmparser, wasm-encoder, tonic, prost, hyper, http or axum. One new duplicate, which is a warning only and not banned: windows-sys 0.60.2 (file-id ← notify-debouncer-full, Windows target only).
- ostro-client: `cargo tree -d -p ostro-client -e normal` shows only syn 2/3, a proc-macro duplicate that already existed.

### Crates, stubs, fixture server
- **Module owners in ostro-build** (each file's doc comment names its owner):

  | Owner | Modules |
  |---|---|
  | 10-07 | cache, fetch, extract, toolchain |
  | 10-08 | sdk_bundle, project, templates |
  | 10-12 | pipeline, stage, manifest, validate, lang::rust |
  | 10-15 | lang::python, lang::ts, lang::docker |

- **`build.rs`:** writes a 1024-zero-byte tar (`tar::Builder::finish` on an empty archive), and `sdk_bundle::SDK_BUNDLE` embeds it. The stub also anchors sha2 and toml, so cargo's unused-build-dependency lint stays quiet.
- **Fixture server:** `FixtureServer::start() -> (base_url, shas, Arc<RequestCounts>)`. `spawn_default()` returns the full handle, and `spawn_tampered()` publishes the same shas while serving flipped bytes (for the mirror-mismatch test).
  - Files: good.tar.gz/xz (`tool-1.0/bin/tool` 0755), badsha.tar.gz (pinned to good's sha), traversal, abs, symlink-escape, symlink-chain, symlink-ok, hardlink-escape, tool.bin, tool.whl, big.bin (2 MiB), slow.bin (headers + 16 bytes, then it stalls).
  - Any directory prefix in the URL is ignored.
  - Generation is deterministic: tar mtime/uid/gid are 0 and the gzip mtime is 0.

### CI
- **ci-local:**
  - `--no-go`;
  - `client_step` records SKIP "scaffold pending (10-04)" while `clients/python/pyproject.toml`, `clients/ts/package.json` or `clients/go/go.mod` is absent;
  - `check_clients_{python,ts,go}` pass `OSTRO_BIN` (the debug binary) to the tests; a missing Go fails loudly;
  - `check_ostro_build_polyglot`.
- **ci.yml `polyglot` job:**
  - `actions/setup-go@d8cbb8045d7f43706b9177cceb36c1f91a6644d5 # v7.1.0` with go-version "1.23.x". `git ls-remote --tags https://github.com/actions/setup-go` resolves both v7.1.0 and v7 to d8cbb80, the same pin the acme-pebble job already uses;
  - steps: build the debug ostro (`OSTRO_BIN` via `GITHUB_ENV`), Clients python/ts/go with the same pending rule, ostro-build polyglot.
- FAILPOINT_CRATES is unchanged, and the "failpoint list sync" step passes.

## Task Commits
1. `d5a6fe1` build(10-02): Phase 10 dependencies, deny exception and anchors
2. `b266d9f` feat(10-02): ostro-build skeleton and fixture HTTP server
3. `9391468` ci(10-02): client and ostro-build polyglot steps

## Verification
- `cargo build --workspace --locked`: warning-free, including cargo's manifest warnings.
- `cargo clippy --workspace --all-targets` (the ci-local exclusions) `-D warnings`: clean. `cargo clippy -p ostro-build -p ostro-client -p ostro-cli -p ostro --all-targets --all-features -D warnings`: clean.
- `cargo fmt --all --check`: ok.
- `cargo test -p ostro-build --test scaffold`: **6 passed**. The tests cover:
  - every published sha (badsha must not match its pin);
  - xz and gz decode to the same tar;
  - the raw unsafe entries;
  - the tampered mirror;
  - the slow.bin stall;
  - the build stub.
- `cargo test -p ostro-cli -p ostro-build -p ostro-client`: all pass (ostro-cli 56 + 6).
- A partial ci-local harness (scratch copy of the script containing the new steps plus failpoint sync, workflow validation, deny, the forbidden-crates check and the deny fixtures) passes all steps. The client steps SKIP "scaffold pending (10-04)", and ostro-build polyglot PASSES (the ostro polyglot targets are not present yet).
- I also ran the client steps against throwaway scaffolds (deleted afterwards): pytest, node TAP and go vet/test all PASS with `OSTRO_BIN` set.
- actionlint (docker `rhysd/actionlint`) on ci.yml: clean.
- `check-vendored.sh`: passed.
- **Not run: the full `bash scripts/ci-local.sh --no-musl`** (~3 h). The steps it adds beyond the harness are workspace cargo test, failpoint suites, campaign, SDK/guests and sleepers. This plan changes no existing crate's code or dependency versions; Cargo.lock only gains packages. The next gate that runs ci-local (10-20 at the latest) covers it.

## Deviations from Plan

### Auto-fixed Issues
**1. [Rule 3 - Blocking] build.rs stub landed with Task 1.** Cargo's `unused_dependencies` lint warned on the ostro-build build-deps (toml, sha2, tar) without a build.rs. Task 1's warning-free build therefore needed the stub, which now anchors sha2 and toml and uses tar.

**2. [Rule 3 - Blocking] futures-util dev-dep on ostro-build.** slow.bin needs a body that stalls after the headers (`stream::once(..).chain(stream::pending())`). Without a Stream crate the server could not express that. futures-util is a workspace pin that was already in the lock (one lock line added: a new dependency edge).

**3. [Rule 1 - Bug] TS client test invocation.** `node --test clients/ts/test/` fails with "Cannot find module …/test" unless the directory has an index.js. Both gates use the glob `'clients/ts/test/**/*.test.js'` instead.

**4. ostro polyglot test scope.** The plan said `cargo test -p ostro --features polyglot -- --test-threads=1`. That would re-run every spawned-binary suite of the ostro crate single-threaded. The step runs only the named polyglot targets (parity, replay_cli, template_tests) once each file exists, and prints "not present yet" otherwise.

### Notes
- The plan's Task 3 automated verify greps each FAILPOINT_CRATES entry literally in ci.yml. It cannot pass for the `crate:target` entries (`ostro-engine:aot_failpoints` and others, a format in use since 05-18), because ci.yml spells them `-p crate ... --test target`. The authoritative `check_failpoint_sync` step passes. The Task 1 verify's `cargo tree -i chrono` (dev edges included) also finds chrono through the s3s-fs dev-dependency that already existed. On normal/build edges chrono is absent.
- Docker is available locally (Docker Desktop WSL 29.1.3), contrary to the plan's assumption. D-P10-15/D-P10-20 are unchanged. Workflow validation used actionlint through docker.

## Issues / Follow-ups
- **10-04:** its verify uses `node --test clients/ts/test/`. That fails without a `clients/ts/test/index.js`, so either add one (as sdks/ts has) or use the glob. The ci gates already use the glob.
- **10-20:** assert that no "scaffold pending (10-04)" SKIP remains, and that the ostro polyglot targets print no "not present yet".
- **USER REVIEW D-P10-11:** the notify CC0-1.0 crate-scoped exception is in place.

## Self-Check: PASSED
- Files: crates/ostro-build/{Cargo.toml,build.rs,src/lib.rs,tests/common/fixture_server.rs,tests/scaffold.rs}, crates/ostro-client/{Cargo.toml,src/lib.rs} and crates/ostro-cli/src/watch.rs are present on main.
- Commits: d5a6fe1, b266d9f and 9391468 are present on main.
