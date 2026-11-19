# Phase 10: Developer CLI & Test Kits - Research

**Researched:** 2026-11-19
**Domain:** Toolchain-free `ostro init/build/deploy/replay/dev --watch`, hermetic toolchain cache, server-side time skipping, per-language test kits, thin control-plane clients (TS/Python/Go/Rust), shell completions and `--json` stability
**Confidence:** MEDIUM overall.
- HIGH: dependency, licence and registry facts. These were checked against crates.io, PyPI, npm, nodejs.org, ziglang.org and the GitHub release APIs on 2026-11-19. The componentize-py release binary and wheel were downloaded and inspected but not executed.
- HIGH: the repo seams, read at HEAD `21d9e58`.
- MEDIUM: the time-skipping design (from code reading plus Temporal's documented model) and the Rust "no C linker" fallback.
- LOW: cold-cache timings. Nothing was measured end to end; Wave 0 measures them.

<user_constraints>
## User Constraints

There is no `10-CONTEXT.md` (`/gsd:discuss-phase` was not run). The binding constraints come from the roadmap entry, the requirements and the locked project decisions. They are copied below from ROADMAP.md, REQUIREMENTS.md, PROJECT.md, STATE.md, RESERVATIONS.md, 05-DECISIONS.md and 09-DECISIONS.md.

### Locked decisions (must be honoured)
- **Roadmap Phase 10.** Goal: "A developer goes from an empty directory to a deployed, running workflow in any of the three languages with one toolchain-free CLI, and can test workflows locally with time skipping." Depends on Phase 9. Success criteria:
  1. In Rust, Python and TS, `ostro init → ostro deploy → ostro run` works on a clean machine with no pre-installed language toolchain (hermetic downloads or `--docker`) in under 10 minutes.
  2. `ostro deploy` builds when needed, validates imports and uploads idempotently. `ostro dev --watch` rebuilds and redeploys on save.
  3. `ostro replay <run|--sample N> --module new.wasm` catches a deliberate non-deterministic change in each language and exits non-zero, so it can serve as a CI gate.
  4. Each language's test harness runs a workflow with a multi-day sleep against `ostro dev --ephemeral` in seconds, using time skipping. TS, Python, Go and Rust client libraries can start, query and signal runs.
- **Packaging (PROJECT.md).** Single static binary for linux x86_64/aarch64 (musl) and macOS. NFR-2.1: binary < 60 MB. The Phase 4 musl release binary measured **22.7 MB** (`target/wt-04-20/x86_64-unknown-linux-musl/release/ostro`).
- **cargo-deny policy (`deny.toml`).**
  - Licence allow-list: Apache-2.0, Apache-2.0 WITH LLVM-exception, MIT, BSD-2/3, ISC, Unicode-3.0, Zlib, 0BSD, BSL-1.0.
  - Single version only: wasmtime, wit-component, wit-parser, wasmparser, wasm-encoder, tonic, prost, hyper, http, axum, opentelemetry.
  - Banned: chrono, bincode, openssl-sys, native-tls, tokio-uring, prometheus.
  - Not allowed by Phase 6 decision: webpki-roots (CDLA-Permissive-2.0), object_store.
  - Only crates.io as a source; no git dependencies.
- **Wasmtime pin policy.** `=50.0.2`, bumped monthly in a single PR with the replay corpus green (D17). Nothing in Phase 10 may tie the Wasmtime version to a third-party crate's cadence.
- **D12 (Phase 5).** `ostro deploy <file.wasm> [--activate] [--name] [--tenant]` uploads only. Phase 10 adds build-if-needed and **must keep this `.wasm` form working**.
- **CLI-06 output contract (05-14, `crates/ostro-cli/src/output.rs`).** `--json` prints exactly the REST response body. Two-call verbs combine their bodies into one object. Exit codes: 0 ok, 1 API/server error, 2 usage, 3 `--wait` ended not completed. Client verbs take `--server` / `OSTRO_SERVER` (default `http://127.0.0.1:7700`).
- **Phase 9 (D-P9-1/3/12/14).**
  - TS builds use QuickJS through jco 1.42.0: `--backend quickjs --backend-qjs-disable-async --backend-qjs-stub-wasi --bundle-config rolldown.ostro.mjs`.
  - Python builds use componentize-py 0.25.1 with `--stub-wasi`.
  - `sdks/scripts/build-guest.sh <py|ts|ts-sm> <dir> <out>` is the reference recipe, with a pinned npm toolchain package in `sdks/scripts/toolchain/` (committed `package-lock.json`) and the D-P9-3 wrapper env (`SOURCE_DATE_EPOCH` staging, `PYTHONHASHSEED=0`, `TZ=UTC`, `LC_ALL=C`).
  - Polyglot tests run behind the cargo feature `polyglot` and `--no-polyglot`.
  - Publishing to PyPI/npm is human_needed (D-P9-19).
  - 09-DECISIONS: "Phase 10 consumes `sdks/scripts/build-guest.sh` as the reference recipe for hermetic `ostro build`, and the conformance suite as the `ostro replay` regression set."
- **RESERVATIONS.md is the only registry** for ordinals and reserved names. Phase 10 has **no block or name rows yet** (see Pre-Phase findings F1).
- **Standing rules.** Shared working tree, so commits go through a private index. No remotes, pushes or package publishing. Commits end with the Co-Authored-By line. `scripts/ci-local.sh` is the gate. clippy runs with `-D warnings`. No Chronik paths.

### Claude's Discretion (research recommends; planner decides)
- How toolchain-free builds work: embedded, downloaded, Wasm-hosted, or Docker.
- Project layout and templates, the `ostro.toml` schema, and the build output location.
- Time-skipping semantics and API shape.
- Test-kit packaging per language.
- Client-library packaging, and hand-written vs generated clients.
- Replay transport (server-side vs exported journals).
- The completion mechanism and the JSON schema policy for local verbs.

### Deferred / out of scope
- Declarative grants/egress/secrets in the project manifest. Phase 6 verbs (`ostro secret/grant/egress/connection`) already cover them.
- Native-extension Python packages (numpy etc.).
- `ostro migrate-runs` (Phase 13).
- Signed modules (Phase 14).
- UI work (Phase 11).
- Windows.
- Publishing packages, crates, Go module tags or a build image to public registries (human_needed).
- A user-facing in-process engine test kit (it would need `ostro-engine` published). Phase 9's engine leg covers the repo internally.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description (REQUIREMENTS.md) | Research Support |
|----|-------------|-----------------|
| CLI-03 | `ostro init --lang {rust,python,ts}` scaffolds a project with pinned toolchains [FR-2.1] | §Pattern 3. Templates and the SDK bundle are embedded in the binary. `ostro.toml` pins the toolchain set of this ostro version. The Rust SDK must first become self-contained (F4). |
| CLI-04 | `ostro build` produces a validated component + manifest for Rust, Python and TS with hermetic toolchains (cached downloads or `--docker`) [FR-2.2/2.3] | §Toolchain-free strategy (download + verify + cache; embedding rejected), §Pattern 1/2, local `validate` + `describe` through the embedded engine, `manifest.json` |
| CLI-05 | `ostro deploy [--activate]` builds if needed and uploads idempotently [FR-2.4] | §Pattern 4. An input-hash build cache plus server-side sha idempotency. The D12 `.wasm` form is kept. |
| CLI-07 | `ostro replay <run\|--sample N> --module new.wasm` detects non-determinism before deploy (CI gate) [FR-2.6] | §Pattern 6. Server-side replay endpoint, because the token key and histories never leave the server. `--corpus` offline mode reuses the 05-12 corpus format. Exit code 4. |
| CLI-08 | `ostro dev --watch` rebuilds and redeploys on save [FR-2.7] | §Pattern 5. notify 8.2 + debouncer, with coalesced serial rebuilds. Build errors never kill the server. |
| SDK-06 | Per-language test harness with time-skipping against `ostro dev --ephemeral` [FR-1.10] | §Pattern 7 (server-side DevClock, auto-skip while a result long-poll is pending, manual skip API) and §Pattern 8 (Rust/Python/TS kits spawn the binary) |
| SDK-07 | Thin client libraries (TS, Python, Go, Rust) for the control-plane API | §Pattern 9. Hand-written zero-dependency clients over REST. The Rust client is extracted from `ostro-cli::client`. A route-coverage contract test runs against `ostro openapi`. |
</phase_requirements>

## Summary

**"Toolchain-free" can only mean "downloads pinned, verified toolchains into a cache", with `--docker` as the fallback. Embedding the compilers in `ostro` is not possible under the project's own rules.** The evidence:
- **componentize-py 0.25.1 as a library** depends on `wasmtime ^46.0.1`, `wasmtime-wasi ^46`, `wit-component/wasmparser/wasm-encoder ^0.245` and **`bincode`** (crates.io dependency list, 2026-11-19). That violates three cargo-deny rules at once: the wasmtime and wasm-tools single-version bans, and the bincode ban.
- **componentize-qjs 0.6.0** depends on `wasmtime ^50` but `wit-component ^0.262`. Ostro pins 0.261 single-version, so it would also break the ban. It would also tie Ostro's monthly Wasmtime bump to a one-maintainer 0.x crate. It only componentizes JS anyway: TS stripping and the async lowering Phase 9 relies on happen in jco's Rolldown.
- **Running the componentizers as Wasm inside ostro** does not work. componentize-py itself runs Wasmtime (pre-init / component-init), and jco needs Node plus native Rolldown/QuickJS bindings.
- **Embedding binaries with `include_bytes!`** would add about 49 MB (componentize-py, uncompressed) plus Node (about 100 MB unpacked) to a 22.7 MB binary, which breaks NFR-2.1 (< 60 MB). cargo-deny would not see their licences either.

**The download path works with today's artefacts, but there is a portability trap.** componentize-py's GitHub release binary for linux-amd64 needs **glibc 2.39**. Measured with `readelf -V`: the highest required symbol version is `GLIBC_2.39`. So it does not run on Ubuntu 22.04, Debian 12 or Alpine, and **this development machine runs glibc 2.35**. The PyPI wheel (`manylinux_2_28`, a pyo3 abi3 extension with a `componentize-py` console script) runs on glibc ≥ 2.28 but needs a host CPython. The portable recipe is therefore:
1. uv's static musl binary, sha-pinned.
2. A uv-managed CPython.
3. `uv pip install --require-hashes componentize-py==0.25.1`.

That is exactly Phase 9's `uv tool run --from componentize-py==0.25.1` recipe made hermetic, so `ostro build` and `build-guest.sh` produce the same artefacts. TS uses a sha-pinned official Node 24 tarball and `npm ci` against the lockfile Phase 9 commits in `sdks/scripts/toolchain/`. Rust uses rustup-init (sha-pinned) with `RUSTUP_HOME`/`CARGO_HOME` in the Ostro cache and a pinned `rust-toolchain.toml`. One more trap here: proc-macros and build scripts need a **host C linker** (`cc`). A clean machine has none, so a pinned zig 0.17.1 (already the CI zig) serves as a fallback `cc`. Each step adds only a few MB to the binary: tar/flate2/lzma-rust2, notify, clap_complete and an SDK bundle under 1 MB.

**Time skipping belongs in the server, not in the test kits.**
- A dev-only `DevClock` (wall time plus a monotonic offset) implements both `ostro_partition::Clock` (ms) and `ostro_engine::env::Clock` (ns). It is injected into partitions, the engine runtime, meta/API timestamps and Phase 8's trigger driver.
- With `ostro dev --ephemeral --time-skipping`, the clock jumps to the next due timer or delayed `ready_at` **only when the node is idle**: no ready, granting or executing runs, no in-flight effects, no pending outbox, **and at least one client is long-polling a result**. This follows Temporal's model, where skipping is unlocked while the test waits on a result and timers are not skipped while activities run.
- `POST /v1/dev/time/skip` advances time manually.
- All test kits (Rust, Python pytest plugin, TS node:test helpers) then become thin wrappers: spawn the binary, `ostro build --json`, deploy, run, skip, assert on history. HTTP effects are mocked by a loopback server in the test language plus `OSTRO_EGRESS__ALLOW_PRIVATE=127.0.0.0/8`, which Phase 6 already supports.

**Replay must run on the server.** Faithful replay needs the cluster token key (05-12 corpus entries carry `token_key_hex`). Await-token strings flow into step/effect arguments, and a different key changes command hashes, which produces false NonDeterminism. Exporting the key to CI would let anyone forge token completions. The recommended flow:
1. `ostro replay` uploads the module, idempotently and content-addressed, without activating it.
2. It calls `POST /v1/modules/{sha}/replay {runs | sample, workflow?}`.
3. The server replays the selected journals through the 05-12 replay logic, moved into a library, and returns per-run verdicts.
4. The CLI exits with **4** on any divergence.

The offline `--corpus DIR` mode replays committed 05-12 corpus entries with no server.

**Primary recommendation:** add a new `ostro-build` crate (toolchain cache, language builders, SDK bundle, templates, manifest) and an `ostro-client` crate (extracted from `ostro-cli::client`, plus a `testing` feature). Put time skipping in the server behind `--time-skipping` (ephemeral only). Use server-side replay. Hand-write zero-dependency clients in `clients/{python,ts,go}`. Settle F1–F6 below before planning.

## Pre-Phase Findings (settle before or at 10-01)

| ID | Finding | Owner / when |
|----|---------|--------------|
| **F1** | **RESERVATIONS.md has no Phase 10 rows.** Phase 10 needs name-only rows (no ordinals, see F7):<br>- crates `ostro-build`, `ostro-client`;<br>- dirs `clients/{python,ts,go}`, `crates/ostro-build/templates/`, project output dir `target/ostro/`;<br>- project file `ostro.toml`;<br>- REST `GET /v1/dev/time`, `POST /v1/dev/time/skip`, `POST /v1/modules/{sha}/replay`;<br>- config section `[dev]` (`time_skipping`);<br>- env vars `OSTRO_BIN`, `OSTRO_TOOLCHAIN_DIR`, `OSTRO_OFFLINE`, `OSTRO_TOOLCHAIN_MIRROR`, `OSTRO_POLYGLOT_BUILDER`;<br>- packages `ostro-client` on PyPI, npm and crates.io (all three returned 404, i.e. free, on 2026-11-19), Go module `github.com/lspecian/ostro/clients/go`;<br>- CLI exit code 4 = replay divergence;<br>- metric prefix `ostro_dev_`.<br>Note: npm `@ostro` returned 200 (the scope may be taken), so avoid scoped names. | 10-01 (docs-only, rule 2) |
| **F2** | **D-P9-3 outcome decides the hermetic Python artefact.** If 09-17 lands on step 1 (wrapper) or step 3 (residual), Phase 10 downloads the released 0.25.1 wheel. If it adopts **step 2 (pinned fork)**, there is no upstream artefact for the fork. Phase 10 would need Ostro-hosted fork binaries (a human_needed release) or a local source build, which needs wasi-sdk and Rust and is not toolchain-free. **Recommendation:** whatever 09-17 decides, Phase 10's hermetic path ships the released wheel and inherits the documented residual. The fork stays a CI/repro-check tool until a hosted artefact exists. Record this as D-P10-2. | Orchestrator, after 09-17 and before 10-01 |
| **F3** | **Pin one exact uv version across Phases 9 and 10.** Phase 9 uses local uv 0.7.15 and CI `setup-uv ≥ 0.7.15`; the latest is 0.13.9. `ostro build` embeds one uv sha per platform. 09-17's CI pin should be the same exact version so the two paths resolve identically. | 09-17 (pin) / 10-03 (embed) |
| **F4** | **The Rust SDK is not self-contained.** `sdks/rust/ostro-sdk/src/bindings.rs` uses `path: "../../../wit/ostro-runtime-0.1.0"` (04-RESEARCH already flagged "vendor a copy before crates.io publish (Phase 10/14)"). `ostro init` cannot hand users this crate until the WIT is copied into `ostro-sdk/wit/`, with a byte-identity test like Phase 9's Python/TS copies. It is cheap if 09-06 does it while it owns `sdks/rust/**`; otherwise 10-04 does it. | Prefer 09-06; else 10-04 |
| **F5** | **A server clock seam is needed for time skipping.** Today `ostro_partition::NodeConfig.clock: Arc<dyn Clock>` exists, but `ostro-server/src/server.rs::now_ms()` reads `SystemClock` directly, and the engine `Runtime` has its own `Clock` (ns). Phase 8 (08-14/08-15) introduces a "TestServer with FakeClock" for triggers. **Phase 10 should reuse that seam, not add a second one.** Phase 7/8 executors should add no new direct `SystemTime`/`SystemClock` reads in server, trigger or webhook code. The dispatcher does not wake when a ManualClock advances (`dispatcher.rs` doc, about L42), so time skipping needs a clock-change wake in the dispatcher select. | Check at 10-01 against the shipped Phase 8 code; 10-06 implements |
| **F6** | **Phase 11 may run in parallel with Phase 10** (ROADMAP: "can run in parallel with Phases 9–10"). Both edit `crates/ostro-cli/src/{client.rs,commands/mod.rs}`, `crates/ostro/src/main.rs`, the `ControlPlane` trait plus FakeControlPlane, and `ostro-api` routes. Extracting `client.rs` into `ostro-client` (10-05) conflicts with any Phase 11 client edits. Adopt the Phase 9 shared-file protocol (clean-file check, append-only, re-apply on a moved HEAD). If Phase 11 starts first, 10-05 goes **last** among the CLI edits. | 10-01 shared-file table |
| F7 | **No ordinals.** Phase 10 adds no Cmd, StartRun field, intent, journal kind, keyspace, RejectReason, InvalidCode, outbox kind or meta record. DevClock state is process memory (ephemeral only). Replay reads journals. Build/manifest data stays local. Server-side source-hash dedupe would need a meta `ext` tag, and RESERVATIONS has no P10 block (144+ unallocated). **Not recommended** (see Pitfall 6). | 10-01 records "none" |
| F8 | **The `ostro dev` data dir defaults to `./.ostro`.** Putting project build artefacts there would mix them with node files (Phase 14 F8 says node open ignores unknown entries, but do not rely on it). Use `target/ostro/` for all languages. | D-P10-5 |

## Toolchain-Free Strategy (the key decision)

### Options evaluated

| Option | Verdict | Evidence |
|--------|---------|----------|
| A. Link the componentizers into `ostro` as crates | **Rejected** | componentize-py 0.25.1 needs wasmtime ^46 + wasmtime-wasi, wasm-tools 0.245 and bincode. componentize-qjs 0.6.0 needs wit-component/wasmparser ^0.262 (Ostro pins 0.261 single-version) and wasmtime-wasi/wizer ^50, with no TS stripping or bundling. Both break `deny.toml` bans and would couple the Wasmtime pin policy to third parties. (crates.io dependency API, HIGH) |
| B. Embed upstream binaries (`include_bytes!`) | **Rejected** | componentize-py is 49.4 MB unpacked (17.2 MB tgz); Node 24 is 58 MB tgz. That breaks the < 60 MB NFR-2.1 budget, hides licences from cargo-deny, and per-platform embedding multiplies release builds. (HIGH) |
| C. Run the componentizers as Wasm inside ostro's Wasmtime | **Rejected** | componentize-py runs Wasmtime itself for pre-init (`component-init-transform` + wasmtime 46), and Cranelift cannot JIT inside Wasm. jco needs Node APIs, Rolldown's native binding and QuickJS napi bindings. (MEDIUM-HIGH, from dependency graphs) |
| **D. Download pinned, sha256-verified toolchains into a content-addressed cache** | **Recommended** | All artefacts exist upstream with checksums: componentize-py wheel (PyPI hashes), uv musl static (`.sha256` files), Node (SHASUMS256.txt), rustup-init (`.sha256`), zig (index.json sha256). Binary growth is a few MB. Builds match Phase 9's recipe. |
| **E. `--docker`** | **Recommended fallback** | Covers musl/old-glibc hosts and machines without network to upstreams (mirror the image). Image publishing is human_needed. |

### Pinned toolchain set (embedded `toolchains.toml` in `ostro-build`; one entry per platform: linux-x86_64, linux-aarch64, macos-aarch64, macos-x86_64)

| Tool | Version | Artefact | Size | Notes |
|------|---------|----------|------|-------|
| uv | exact pin = F3 (latest 0.13.9) | `uv-<arch>-unknown-linux-musl.tar.gz` / `uv-<arch>-apple-darwin.tar.gz` + `.sha256` | ~17 MB / ~15 MB | The musl build is static and runs on any Linux. Set `UV_CACHE_DIR`, `UV_PYTHON_INSTALL_DIR`, `UV_TOOL_DIR` under the Ostro cache, plus `UV_NO_CONFIG=1` and `UV_PYTHON_PREFERENCE=only-managed`. |
| CPython (host, for running componentize-py) | 3.12.x (D-P9-13 host Python) | uv-managed python-build-standalone | ~30 MB | uv verifies managed-Python hashes itself (MEDIUM). The guest CPython 3.14 is inside componentize-py. |
| componentize-py | **0.25.1** | `componentize_py-0.25.1-cp39-abi3-manylinux_2_28_{x86_64,aarch64}.whl` (17.1 MB), `macosx_11_0_arm64` (15.4 MB), `macosx_10_12_x86_64` (16.1 MB) | — | Installed with `uv pip install --require-hashes` into a venv in the cache. Console script `componentize-py=componentize_py:script` (inspected). **Do not use** the GitHub release binary on Linux: it requires GLIBC_2.39. |
| Node.js | 24 LTS, exact (latest v24.22.3, 2026-11-10; Phase 9 CI pins 24.13.0, **align with 09-17**) | `node-v24.x-<os>-<arch>.tar.xz` (linux-x64 32.1 MB) or `.tar.gz` (58.3 MB) | — | Official builds are glibc-only (no musl), so Alpine falls back to `--docker`. Verify against SHASUMS256.txt contents pinned in the binary. |
| jco toolchain | jco 1.42.0 + componentize-js 0.23.0 (bundled componentize-qjs 0.5.0) | `npm ci --ignore-scripts --no-audit --no-fund` against Phase 9's `sdks/scripts/toolchain/package{,-lock}.json`, **embedded byte-identically** in the binary | hundreds of MB installed (LOW, measure) | jco declares caret ranges (`rolldown ^1.2.4`, `componentize-qjs ^0.5.0`), so pinning comes only from the lockfile (sha512 integrity). The QuickJS binding comes from per-platform optionalDependencies (`@andreiltd/componentize-qjs-binding-linux-x64-gnu` etc., glibc). |
| rustup-init | pinned release | `static.rust-lang.org/rustup/archive/<ver>/<triple>/rustup-init` + `.sha256` | ~10–20 MB | `RUSTUP_HOME`/`CARGO_HOME` under the Ostro cache. `--profile minimal`, the toolchain from the project's `rust-toolchain.toml` (`1.100.0`, `targets = ["wasm32-wasip2"]`). The wasip2 target ships `wasm-component-ld`, so no wasm-opt or wasm-tools are needed (MEDIUM). |
| zig (host-linker fallback) | 0.17.1 (= CI) | `zig-<arch>-<os>-0.17.1.tar.xz` (~57 MB) | — | Used only when no `cc` is on PATH. An ostro-provided `cc` shim runs `zig cc`, set via `CARGO_TARGET_<HOST>_LINKER`. Needs xz decoding (lzma-rust2). macOS behaviour is LOW confidence, so test it or document `--docker`. |

### Rust policy
`--toolchain auto|hermetic|system|docker` (also settable in `ostro.toml`):
- `auto` (the default) uses the system `rustup` when present, because it honours `rust-toolchain.toml`, and the hermetic rustup otherwise.
- Python and TS default to **hermetic always**: exact componentize/jco versions decide conformance and reproducibility. `system` is opt-in and prints a warning.

### Licences
- Downloaded toolchains are not linked into ostro. Ostro does not redistribute them, so they have no cargo-deny impact. **The `--docker` image does redistribute** Node, CPython and Rust, so it needs a NOTICE (human_needed at publish).
- Built components embed third-party runtimes:
  - Python: CPython (PSF-2.0), libc++/libc++abi (Apache-2.0 WITH LLVM-exception), wasi-libc, and the componentize-py runtime (Apache-2.0 WITH LLVM-exception).
  - TS: QuickJS-NG and rquickjs (MIT).
- `manifest.json` should list these under `embedded_runtimes`.
- New Rust dependencies and their licences:
  - notify 8.2.2: **CC0-1.0**, not on the allow-list, so it needs a `deny.toml` entry with a comment (USER REVIEW). Its transitive deps are already allowed: inotify (ISC), kqueue/fsevent-sys (MIT), walkdir (Unlicense/MIT), notify-types (MIT/Apache).
  - Already allowed: notify-debouncer-full 0.7.0 (MIT/Apache), clap_complete 4.6.12 (MIT/Apache), tar 0.4.46 (MIT/Apache), flate2 1.1.11 (MIT/Apache; miniz_oxide MIT/Zlib/Apache), lzma-rust2 0.25.0 (Apache-2.0), filetime/xattr (MIT/Apache).
  - **Avoid `dirs`/`directories`.** dirs-sys pulls `option-ext` (MPL-2.0), which is not allowed. XDG/macOS cache paths are about 15 lines of code.
- **Avoid `webpki-roots`.** Use reqwest 0.13.5's `rustls` feature, which is `__rustls-aws-lc-rs` + `rustls-platform-verifier` (feature list checked). Phase 6 makes the same choice.

## Standard Stack

### Core
| Library / tool | Version | Purpose | Why |
|---------|---------|---------|--------------|
| reqwest (workspace) | 0.13.5 + feature `rustls` | Toolchain downloads, remote `--server https://` | Already the workspace HTTP client. Uses the platform verifier; no webpki-roots. |
| tar | 0.4.46 | Extract tarballs (`Entry::unpack_in` guards path traversal) | Standard; already transitive in many trees |
| flate2 | 1.1.11 (rust backend) | gzip | Pure Rust (miniz_oxide) |
| lzma-rust2 | 0.25.0 | xz (zig, Node `.tar.xz`) | Pure Rust, Apache-2.0, maintained (16.6M recent downloads), static-musl friendly |
| sha2 (workspace) | 0.11 | Artefact verification, input hashes | Already pinned |
| notify + notify-debouncer-full | 8.2.2 + 0.7.0 | `ostro dev --watch` | The de-facto Rust watcher (inotify/FSEvents/kqueue) |
| clap_complete | 4.6.12 | `ostro completions <shell>` | Same clap family as the workspace clap 4.6.8 |
| std `File::lock` | Rust ≥ 1.89 (repo 1.100) | Cache install lock | No fs2/fd-lock dependency needed (MEDIUM: verify the API name at implementation) |
| ostro-engine (in-tree) | — | `ostro build` local validate + describe; replay check | The same code the server runs, so local validation equals server validation |
| uv / CPython 3.12 / componentize-py 0.25.1 / Node 24 / jco 1.42.0 / rustup / zig 0.17.1 | see the table above | Hermetic toolchains (downloaded) | Phase 9 pins |

### Supporting
| Library | Purpose | When |
|---------|---------|------|
| insta (workspace dev) | Snapshot `--json` outputs, init trees, completions | Tests |
| axum (workspace, dev) | Local HTTP fixture serving fake toolchain archives (no network in the default suite) | 10-03 tests |
| tempfile (workspace) | Staging dirs | build |
| Python stdlib only (`urllib.request`, `json`, `subprocess`) | `ostro-client` (PyPI) + pytest plugin | Zero runtime deps, so it works in any test venv |
| Node ≥ 18 globals (`fetch`, `node:child_process`, `node:test`) | `ostro-client` (npm) + testing helpers | Zero deps |
| Go stdlib (`net/http`, `encoding/json`, `os/exec`) | Go client + `ostrotest` | Zero deps; local Go 1.23.10 |

### Alternatives Considered
| Instead of | Could use | Tradeoff |
|------------|-----------|----------|
| uv + PyPI wheel | componentize-py GitHub release binary | No host Python needed, but it requires **glibc 2.39** (fails on Ubuntu 22.04/Debian 12/Alpine and on this dev box). Viable on macOS only. |
| Official Node + npm ci | Bun single binary | Not Phase 9's toolchain, and jco plus napi bindings are untested on Bun |
| Hand-written thin clients | OpenAPI generators (openapi-typescript, openapi-python-client, oapi-codegen) | They add generator toolchains and generated-code churn, and clients stop being "thin". A route-coverage contract test gives the safety without the weight. |
| Server-side replay | Export journals + token key to CI | Leaks the forging key and the run data (PII) to CI. A redacted key gives false divergences whenever a token string reaches command args. |
| Spawned-binary test kits | In-process server linked into user tests (Rust) | Rust users would compile all of wasmtime+fjall into tests (minutes), and Python/TS cannot do it at all. One mechanism for all languages. |
| zig as host-linker fallback | Require `cc` / Xcode CLT | Simpler, but fails SC1's clean-machine wording on Linux minimal images and fresh macOS |

**Installation (workspace pins, done once by the Cargo-owning plan):**
```toml
notify = "=8.2.2"
notify-debouncer-full = "=0.7.0"
clap_complete = "=4.6.12"
tar = "=0.4.46"
flate2 = { version = "=1.1.11", default-features = false, features = ["rust_backend"] }
lzma-rust2 = "=0.25.0"
reqwest = { version = "0.13.5", default-features = false, features = ["json", "http2", "rustls"] }  # if Phase 6 has not already added rustls
```

## Architecture Patterns

### Recommended layout
```
crates/
├── ostro-build/                 # NEW (publish = false)
│   ├── build.rs                 # deterministic tar of sdks/{rust,python,ts} + sdks/scripts/toolchain/package*.json
│   ├── toolchains.toml          # pinned URLs + sha256 per platform (include_str!)
│   ├── templates/{rust,python,ts}/...   # include_str!; {{name}} placeholders
│   └── src/{lib,cache,fetch,extract,toolchain,sdk_bundle,project,stage,manifest,validate}.rs
│       └── lang/{rust,python,ts,docker}.rs
├── ostro-client/                # NEW: typed REST client (moved from ostro-cli/src/client.rs) + feature "testing"
├── ostro-cli/                   # verbs: init, build, deploy(+build), replay, toolchain, doctor, completions
├── ostro/                       # main.rs: `dev --watch --time-skipping`, completions wiring
├── ostro-engine/src/replay_check.rs   # NEW library form of tests/replay_corpus.rs logic
└── ostro-server/src/{devclock.rs, dev_api.rs, replay.rs}   # NEW files; minimal hunks elsewhere
clients/
├── python/  (PyPI "ostro-client": ostro_client/{__init__,client,testing,pytest_plugin}.py)
├── ts/      (npm "ostro-client": src/{index,client,testing}.ts; exports "." and "./testing")
└── go/      (module github.com/lspecian/ostro/clients/go; packages ostro, ostrotest)
deploy/build-image/Dockerfile    # --docker image (P14 reserves deploy/image for the server image)
```

User project (from `ostro init`):
```
my-wf/
├── ostro.toml          # [project] name, lang, entry; [build] toolchain="auto"; [deploy] activate=true
├── .gitignore          # target/, node_modules/, __pycache__/, .ostro/
├── README.md
├── rust:   Cargo.toml (cdylib; ostro-sdk = { path = "target/ostro/sdk/rust/ostro-sdk" } until crates.io), rust-toolchain.toml, src/lib.rs, tests/workflow.rs
├── python: pyproject.toml (deps = []), app.py, tests/test_workflow.py
└── ts:     package.json, tsconfig.json (paths: ostro -> target/ostro/sdk/ts/src/index.ts), src/index.ts, test/workflow.test.ts
target/ostro/           # build output: sdk/ (materialised bundle), stage/, <name>.wasm, manifest.json, inputs.sha256
```

### Pattern 1: Toolchain cache (download → verify → extract → atomic rename)
**What:** cache root is `$OSTRO_TOOLCHAIN_DIR`, else `$XDG_CACHE_HOME/ostro/toolchains`, else `~/.cache/ostro/toolchains` (Linux), else `~/Library/Caches/ostro/toolchains` (macOS). Entries are `<tool>-<version>-<sha8>/`. Install sequence:
1. Take an exclusive `File::lock` on `<root>/.lock`.
2. Download to `<root>/.tmp-<pid>/archive` over rustls.
3. Stream sha256 and compare with the embedded pin. Any mismatch deletes the file and fails.
4. Extract with `unpack_in` (rejects `..`; validate symlink targets stay inside the dir, since Node tarballs contain `bin/npm → ../lib/...`).
5. Write `.ok` with the pin, then `rename` into place.

Other behaviour:
- `OSTRO_OFFLINE=1` never touches the network.
- `OSTRO_TOOLCHAIN_MIRROR=<base>` rewrites URLs; the sha stays authoritative.
- `ostro toolchain install [--lang ..]|list|prune` and `ostro doctor` show and repair state.

**Never** execute anything before verification, and never pipe to a shell.

### Pattern 2: Language builders (`ostro build [DIR] [--toolchain ..] [--docker] [--json]`)
Common pipeline:
1. Load `ostro.toml`, or detect from Cargo.toml / pyproject.toml / package.json.
2. Compute the **input hash**: sorted relative paths and bytes of the sources, lockfiles, `ostro.toml`, the toolchain-set id and the SDK bundle sha. If it matches `target/ostro/inputs.sha256` and the wasm exists, stop with "up to date".
3. Materialise the embedded SDK into `target/ostro/sdk/`.
4. Stage the sources into `target/ostro/stage/` with mtimes = `SOURCE_DATE_EPOCH` (default 0, or the commit time when in git), exactly as `build-guest.sh` does.
5. Run the language step with a scrubbed env (`TZ=UTC`, `LC_ALL=C`, `PYTHONHASHSEED=0`, `PYTHONDONTWRITEBYTECODE=1`, `SOURCE_DATE_EPOCH`).
6. **Validate locally** with `ostro_engine::validate` (import allow-list, the ENG-1 `exports` rule from 09-07) and **describe** (09-07 `Runtime::describe`) to list workflows.
7. Write `manifest.json` and print a human summary or `--json`.

Language steps:
- **Rust:** `cargo build --release --target wasm32-wasip2 --locked?`. Pass `--locked` only if Cargo.lock exists; otherwise generate the lock and warn. Output: `target/wasm32-wasip2/release/<crate>.wasm`.
- **Python:** `<cache>/cpy-venv/bin/componentize-py -d <sdk>/python/ostro/wit -w ostro:runtime/workflow@0.1.0 componentize --stub-wasi -p <stage> -p <stage>/.deps <entry> -o <out>`. Third-party deps: `uv pip install --target <stage>/.deps --only-binary :all: --python-version 3.14 -r <uv export>`, then reject any wheel that is not `py3-none-any` with a clear error.
- **TS:** stage the SDK as `node_modules/ostro`. If `package-lock.json` has dependencies, `npm ci --ignore-scripts` with the hermetic node. Then `<toolchain>/node_modules/.bin/jco componentize <stage>/src/index.ts --wit <sdk>/ts/wit --world-name workflow --backend quickjs --backend-qjs-disable-async --backend-qjs-stub-wasi --bundle-config <toolchain>/rolldown.ostro.mjs -o <out>`.
- **`--docker`:** run the same `ostro build --in-container` inside `ostro-build:<version>`:
  - On Linux, bind-mount the host's static musl `ostro`.
  - On macOS, use the image's linux ostro of the same version.
  - Mount the project at `/src` and the cache at `/cache`.

**Parity rule:** `ostro build` and `sdks/scripts/build-guest.sh` must pass identical flags and env. Enforce it with a test: build the Phase 9 conformance guests both ways. Rust and QuickJS give equal shas (within the documented residual); Python passes the conformance replay. Also add `OSTRO_POLYGLOT_BUILDER=ostro` so Phase 9's `polyglot::build` can use `ostro build`.

### Pattern 3: `ostro init [DIR] --lang L [--name N] [--force] [--json]`
- Refuses a non-empty dir unless `--force`.
- Writes the template, substituting the name (validated against D23: no `/`, no `:`, no leading `__`).
- Materialises `target/ostro/sdk/` for IDEs.
- Prints next steps (`ostro dev --watch`, `ostro deploy`, `ostro run hello`).
- Each template has: one workflow with a step, a durable `sleep` of 3 days and a signal; a test that uses the language test kit; README; `.gitignore`.
- Templates are snapshot-tested as file trees and **built** in CI (polyglot feature) so they never rot.

### Pattern 4: `ostro deploy [PATH] [--activate] [--no-build] [--name] [--tenant]`
- `PATH` ending in `.wasm` keeps the D12 behaviour unchanged.
- Otherwise `PATH` (default `.`) is a project: build if stale (Pattern 2 input hash), then upload.
- Upload is already idempotent server-side (04-20: re-uploading the same sha is a no-op).
- Activate only the versions the upload reports that are not already active.
- `--json` combines `{build, upload, activated}` under the existing two-call convention.
- Module name defaults to `ostro.toml [project] name`.

### Pattern 5: `ostro dev --watch [DIR]`
- Start the server as today: in-process, `./.ostro` or `--ephemeral`.
- Do one build + deploy.
- Watch DIR with `notify-debouncer-full` (about 250 ms). Ignore `target/`, `node_modules/`, `.ostro/`, `__pycache__/`, `.git/`, and editor temp files (`*~`, `.#*`, `*.swp`, `4913`).
- Serialise builds; changes during a build set a dirty flag, giving exactly one follow-up build.
- Deploy through the REST client against the local address (same code path as `ostro deploy`).
- A build or validation failure prints diagnostics and keeps the server and the last good version running.
- In-flight runs stay pinned to their module (by design). Print "new runs use vN".
- `--watch` without a project is a usage error (exit 2).

### Pattern 6: Replay (`ostro replay`)
**Remote (default):**
- `ostro replay --module new.wasm (--run ID… | --sample N [--workflow W] [--status S]) [--server URL] [--json]`.
- Calls `POST /v1/modules/{sha}/replay` after an idempotent upload; no activation.
- Server side:
  - Selection: the explicit runs, or the newest N runs per workflow described by the module (any status, running included). Bounded: N ≤ `max_sample` (default 100, hard cap 1000).
  - Per run: `read_journal` (as in 05-12's `export_run`), then `replay_check(runtime_with_cluster_token_key, prepared(new module), run_ref, journal)` on a dedicated low-concurrency pool. Prepare once per request (Pitfall 10 of Phase 9: about 0.5 GB per Python AOT).
- **Verdict rules:**
  - `NonDeterminism` during the journaled prefix → `diverged{seq, detail}`.
  - A terminal journal must reproduce its terminal and append 0 events → `ok`.
  - A non-terminal journal may append live events past the frontier → `ok_frontier`. The recording sink executes nothing: effects are only intents, and guests have no ambient I/O.
  - The workflow is not exported by the module → `skipped`, counted as failure unless `--allow-missing`.
- Exit codes: 0 all ok; **4** any diverged/skipped; 1 errors; 2 usage.

**Offline:** `ostro replay --corpus DIR [--module new.wasm]` replays 05-12 entries (`entry.toml`, `journal.bin`, `module.wasm`) using the entry's own token key. Nothing leaves the machine. Good for committed corpora in user repos.

**Library:** move the logic of `crates/ostro-engine/tests/replay_corpus.rs` (decode, re-encode check, outcome compare, appended count) into `ostro_engine::replay_check` (new file + one `lib.rs` line). The test then calls the library.

### Pattern 7: Time skipping (server, dev only)
- **`DevClock`:** `base_wall` (read once from SystemClock at start) + `offset_ms` (atomic, only grows) + a `watch` notifier. It implements `ostro_partition::Clock::now_ms` and `ostro_engine::env::Clock::now_unix_nanos`, both reading the same `SystemClock + offset`. It is injected wherever F5's seam reaches: partitions/leader services, engine Runtime, meta/API timestamps, the Phase 8 trigger driver, webhook staleness checks.
- **Enable:** `ostro dev --ephemeral --time-skipping[=auto|manual]` (config `[dev] time_skipping`). It is **rejected without `--ephemeral`** (Pitfall 3) and rejected in `server` mode.
- **Idle detector:** a `DevTime` service aggregates per-partition stats:
  - dispatcher ready / granting / executing counts and earliest delayed `ready_at`;
  - timer-window earliest due;
  - shuffler/outbox pending;
  - Phase 6 bridge in-flight effects;
  - pending API write requests.

  Skip target = min(earliest due) across partitions. Reserved `EXPIRE_OP` timers are excluded as targets.
- **Auto mode:** skip only when idle **and** at least one `result` long-poll (05-13 RunWatchers) is waiting. Re-check after a 10–20 ms quiescence window to avoid racing the apply → action fan-out. **Manual:** `POST /v1/dev/time/skip {"by":"72h"} | {"to_ms":…}` always works. `GET /v1/dev/time` returns `{now_ms, offset_ms, mode, idle}`.
- **Dispatcher:** add a select arm on the clock's `changed()` watch so delayed `ready_at` and backoffs re-evaluate. The timer service already polls every 50 ms (`MAX_SLEEP`).
- **Leases, heartbeats, effect HTTP timeouts and the API drain** stay on tokio real time. That is correct: skipping must not expire leases.

### Pattern 8: Test kits (one design, three languages)
- `start()` finds the binary (`OSTRO_BIN`, else PATH; checks `ostro build-info` version ≥ kit version). It spawns `ostro dev --ephemeral --time-skipping --listen 127.0.0.1:0`, with env `OSTRO_RUNS__RETENTION=87600h` (Pitfall 4) and optionally `OSTRO_EGRESS__ALLOW_PRIVATE='["127.0.0.0/8"]'`. It parses the ready line `ostro dev listening on http://ADDR (data: …, ephemeral) ready in N ms`. **Freeze that line as a contract** with a snapshot or regex test.
- `build_and_deploy(project_dir)` runs `ostro build --json`, then `ostro deploy --activate --json` (or uses the client).
- `start_run`, `signal`, `result(wait)` (auto-skips), `skip(duration)`, `now()`, `history(run)`.
- Assertion helpers over history JSON: `steps(run)`, `assert_completed`, `timers(run)`.
- HTTP mock: a loopback server in the test language plus `env.allow_egress(module, url)` through the Phase 6 egress API.
- Packaging:
  - **Rust:** `ostro-client` feature `testing` (`TestEnv`, `#[tokio::test]`-friendly, kills the child on Drop).
  - **Python:** `ostro_client.testing.TestEnv` plus a pytest plugin (entry point `pytest11: ostro = ostro_client.pytest_plugin`, fixtures `ostro_env` scope=session and `ostro_project`).
  - **TS:** `ostro-client/testing` (`await TestEnv.start()`, works with `node:test` `before/after`).
  - **Go:** `ostrotest` (client e2e only; Go has no SDK).

### Pattern 9: Thin clients
- Core surface, the same in all four:
  - `upload_module(bytes, name?)`, `activate(workflow, version)`;
  - `start_run(workflow, input, opts{run_id/idempotency_key, conflict, tags})`, `get_run`, `list_runs(filter)`, `history`;
  - `result(run, wait?)`, `signal(run, name, payload)`, `cancel`, `terminate`, `retry`, `complete_token`;
  - `dev_time()`, `dev_skip()`.
- Errors map to one typed error carrying the REST error body (`code`, `message`, `details`).
- **The Rust client is the extracted `ostro-cli::client`.** The CLI keeps its output/exit-code layer on top.
- A contract test (`clients/contract.rs` in `ostro-client` tests) parses `ostro openapi` and checks every (method, path) any client uses exists. A shared e2e scenario runs per language against a spawned ephemeral server: start → query → signal → result.

### Anti-Patterns to Avoid
- **Linking or embedding componentizers or compilers in `ostro`.** See the options table: it breaks deny bans or the size budget.
- **Time skipping in the client.** The server owns the timers; client-side fakes cannot move journal timestamps or timer firing.
- **Skipping on a persistent data dir.** Timestamps run ahead of wall time, and after a restart they go back to real time (Pitfall 3).
- **Server-side "source hash" dedupe through new meta fields.** That is a frozen-format change for a convenience (F7).
- **`npm install` (not `ci`) or `uv pip install` without hashes** in hermetic builds. It is not reproducible.
- **Parsing human CLI output in test kits.** Always use `--json` or the REST client.
- **Exporting the token key** for client-side replay.

## Don't Hand-Roll

| Problem | Don't build | Use instead | Why |
|---------|-------------|-------------|-----|
| Python → component | Own CPython linking / pre-init | componentize-py 0.25.1 wheel via uv | Wizer/component-init, dylib linking, stub-wasi |
| TS → component | Own bundler / async lowering | jco 1.42.0 + Phase 9 `rolldown.ostro.mjs` | Verified in Phase 9 |
| Python dependency resolution | A PyPI client | uv (`pip install --target --only-binary :all: --require-hashes`) | Resolution, hashes, markers |
| npm resolution | A lockfile reader | `npm ci` with the embedded lockfile | sha512 integrity |
| File watching | Polling loops | notify + notify-debouncer-full | Rename/atomic-save semantics per OS |
| Tar/xz/gz | Custom decoders | tar + flate2 + lzma-rust2 | Path-traversal guards |
| Completions | Hand-written scripts | clap_complete | Generated from the clap tree, stays in sync |
| Replay checking | A new checker | Move 05-12 `replay_corpus.rs` logic into `ostro_engine::replay_check` | Already proven against 24 corpus entries |
| Import validation in build | Re-implementing rules | `ostro_engine::validate` / witcheck | Same rules as the server |
| Rust host linker on clean machines | Bundling a libc/sysroot | zig 0.17.1 as `cc` (cargo-zigbuild pattern) | Already the repo's CI approach |

**Key insight:** "toolchain-free" is a **supply-chain** feature, not a compiler feature. The work is pinning, verification, caching, env scrubbing and parity with the Phase 9 recipe, not reimplementing componentizers.

## Common Pitfalls

### Pitfall 1: componentize-py release binary needs glibc 2.39
**What goes wrong:** the build works on CI (Ubuntu 24.04) but fails on Ubuntu 22.04/Debian 12/RHEL 9/Alpine, and on this dev box (glibc 2.35), with `GLIBC_2.39 not found`.
**How to avoid:** use uv + managed CPython + the manylinux_2_28 wheel (Pattern 2). Use `--docker` for musl hosts.
**Warning signs:** a clean-machine test on `debian:12` fails.

### Pitfall 2: Rust proc-macros need a host linker
**What goes wrong:** `cargo build --target wasm32-wasip2` still compiles proc-macros and build scripts for the host and invokes `cc`, which a clean machine lacks ("linker `cc` not found").
**How to avoid:** detect `cc`. If it is missing, use hermetic zig as a `cc` shim. On macOS without Xcode CLT, test zig, else recommend `--docker`.
**Warning signs:** a minimal-image e2e (`ubuntu:24.04` without build-essential) fails.

### Pitfall 3: Time skipping on persistent state
**What goes wrong:** journal timestamps and timer dues run days ahead of wall time. After a restart the clock is real time again, timers appear "3 days out", and engine logical time (max(clock, last ts)) freezes.
**How to avoid:** `--time-skipping` requires `--ephemeral` and is rejected in `server`. Test that rejection.

### Pitfall 4: Skipping past retention expires earlier runs
**What goes wrong:** a 3-day skip passes the 24h dev retention, so earlier tests' runs are deleted and `result` returns 404.
**How to avoid:** test kits set `OSTRO_RUNS__RETENTION=87600h`. Exclude `EXPIRE_OP` from skip targets.

### Pitfall 5: Cron schedules under skipping
**What goes wrong:** skipping 3 days fires a per-minute schedule's catch-up storm (Phase 8 catch-up policies).
**How to avoid:** document it. Test kits create no schedules by default. Skip honours catch-up policy, which is Phase 8 semantics.

### Pitfall 6: Non-reproducible Python/TS builds defeat deploy idempotency
**What goes wrong:** on a fresh clone (no `target/ostro` cache) a rebuild of unchanged sources gives a new sha, so a new module version, so a new active version, on every CI deploy.
**How to avoid:** the input-hash cache covers local reruns. Document the D-P9-3 residual. Recommend committing the built artefact or caching `target/ostro` in CI. **Do not** add server-side source-hash fields (F7).
**Warning signs:** `ostro deploy` twice in fresh clones gives two versions.

### Pitfall 7: Idle detection races
**What goes wrong:** the clock jumps between apply committing a `StartRun` and the dispatcher seeing `EnqueueReady`, or while a signal is in flight from the API to the partition. The run then observes a skipped clock.
**How to avoid:**
- Count pending API writes and un-fanned actions.
- Require a quiescence window.
- Skip only while a result waiter exists.
- Test with signal-then-await and start-then-immediately-await scenarios, 1000 iterations.

### Pitfall 8: Dispatcher does not wake on clock change
**What goes wrong:** delayed `ready_at` and step backoffs are computed from `LeaderCtx::clock`, but the loop sleeps on tokio `Instant` and is never woken by an advance (documented in `dispatcher.rs`).
**How to avoid:** add a `changed()` arm. This is a minimal hunk in a Phase 7-owned file, so use the shared-file protocol.

### Pitfall 9: Engine and partition clocks diverge
**What goes wrong:** the partition skips but the engine `Runtime` keeps `SystemClock`, so `sleep` dues are computed from real time and journal ts are inconsistent.
**How to avoid:** one `DevClock` implements both traits. Assert in a test that `RunStarted.accepted_at`, `TimerStarted` due and `TimerFired` ts are consistent after a skip.

### Pitfall 10: Archive symlinks and path traversal
**What goes wrong:** a malicious or broken archive writes outside the cache, or a symlink escapes it.
**How to avoid:** `unpack_in`, reject absolute and escaping symlinks, verify the sha before extracting, use a temp dir plus atomic rename.

### Pitfall 11: User config leaking into hermetic builds
**What goes wrong:** `~/.config/uv/uv.toml`, `~/.npmrc` registries or `RUSTFLAGS` change outputs.
**How to avoid:** set `UV_NO_CONFIG=1`, `npm_config_userconfig=<cache>/npmrc` (with `registry` pinned unless mirrored), clear `RUSTFLAGS`/`CARGO_ENCODED_RUSTFLAGS`, and set `CARGO_HOME` in the cache for hermetic mode.

### Pitfall 12: `npm ci --ignore-scripts` breaking jco
**What goes wrong:** some dependency needs a postinstall.
**How to avoid:** test in Wave 0. The QuickJS binding uses optionalDependencies (no script). If scripts are required, allowlist them explicitly.

### Pitfall 13: Ready-line format drift
**What goes wrong:** test kits in three languages parse the ready line, so a cosmetic change breaks them all.
**How to avoid:** freeze the format with a regex/snapshot test in `crates/ostro/tests`. Any change becomes a versioned contract change.

## Code Examples

### Hermetic Python build (shape)
```bash
# Executed by ostro-build with paths inside $OSTRO_TOOLCHAIN_DIR; source of the flags: sdks/scripts/build-guest.sh (09-02)
UV_NO_CONFIG=1 UV_CACHE_DIR=$C/uv-cache UV_PYTHON_INSTALL_DIR=$C/python \
  $C/uv-<pin>/uv venv --python 3.12 --python-preference only-managed $C/cpy-0.25.1
UV_NO_CONFIG=1 $C/uv-<pin>/uv pip install --python $C/cpy-0.25.1 --require-hashes -r <embedded componentize-py.txt>
#   componentize-py.txt:  componentize-py==0.25.1 --hash=sha256:<wheel sha per platform>
env -i PATH=/usr/bin:/bin TZ=UTC LC_ALL=C PYTHONHASHSEED=0 PYTHONDONTWRITEBYTECODE=1 SOURCE_DATE_EPOCH=$EPOCH \
  $C/cpy-0.25.1/bin/componentize-py -d $SDK/python/ostro/wit -w ostro:runtime/workflow@0.1.0 \
  componentize --stub-wasi -p $STAGE app -o target/ostro/$NAME.wasm
```

### Replay verdict (library signature)
```rust
// crates/ostro-engine/src/replay_check.rs (new); logic moved from tests/replay_corpus.rs (05-12)
pub enum ReplayVerdict {
    Ok { appended: usize },                 // terminal journal: must be 0
    OkFrontier { appended: usize },         // non-terminal journal: live progress past the end
    Diverged { seq: u64, detail: String },  // NonDeterminism inside the journaled prefix
    Error(String),                          // decode / prepare / trap unrelated to divergence
}
pub async fn replay_check(rt: &Runtime, module: &Prepared, run: RunRef, journal: &[Event]) -> ReplayVerdict;
```

### DevClock (shape)
```rust
// crates/ostro-server/src/devclock.rs (new)
pub struct DevClock { base: SystemClock, offset_ms: AtomicU64, tx: watch::Sender<u64> }
impl ostro_partition::Clock for DevClock { fn now_ms(&self) -> u64 { self.base.now_ms() + self.offset_ms.load(SeqCst) } }
impl ostro_engine::env::Clock for DevClock { fn now_unix_nanos(&self) -> u64 { /* same source, *1_000_000 */ } }
impl DevClock { pub fn skip_to(&self, target_ms: u64) { /* offset = max(offset, target - base.now); notify */ } }
```

### pytest usage (template test)
```python
# tests/test_workflow.py generated by `ostro init --lang python`
def test_hello_sleeps_three_days(ostro_env, ostro_project):
    run = ostro_env.start_run("hello", {"name": "ada"})
    ostro_env.signal(run, "approve", {"ok": True})
    res = ostro_env.result(run, wait="30s")     # auto time-skip covers the 3-day sleep
    assert res["status"] == "completed"
    assert ostro_env.now_ms() - run.accepted_at_ms >= 3 * 86_400_000
```

## State of the Art

| Old approach | Current approach | When | Impact |
|--------------|------------------|------|--------|
| esbuild → componentize-js (PRD FR-2.2 text) | jco 1.42 built-in Rolldown + QuickJS backend | jco 1.4x / Phase 9 | `ostro build` TS = jco only |
| `cargo → wasm32-wasip2 → wasm-opt` (PRD) | `wasm32-wasip2` emits components via `wasm-component-ld`; binaryen does not process components | Rust ≥ 1.82 | Drop wasm-opt; use profile `opt-level="s"`, `lto`, `strip` (MEDIUM) |
| componentize-py via pip/uv tool | Same; release binaries exist but need glibc 2.39 | 0.25.1 (2026-09-11) | Use the wheel on Linux |
| Temporal-style test server binaries | Ostro's own binary is the test server (`dev --ephemeral --time-skipping`) | — | No extra download for tests |

**Deprecated/outdated:** cargo-component (stale, per Phase 9); Javy; relying on `webpki-roots`.

## Open Questions

1. **Cold-cache wall time per language (SC1 < 10 min).** Unknown, and the npm ci footprint of jco is the largest unknown. Measure in Wave 0 in `ubuntu:24.04` and `debian:12` containers with network. Fallback: a pre-warm `ostro toolchain install --all` step in docs.
2. **zig as host linker on macOS.** LOW. Test on a macOS runner (human_needed if none). Fallback: require Xcode CLT, or `--docker`.
3. **Does `npm ci --ignore-scripts` keep jco 1.42 working?** Wave 0 check.
4. **REST-only or gRPC parity for `/v1/dev/time*` and `/v1/modules/{sha}/replay`?** Recommendation: REST-only, documented as dev/tooling endpoints, with an explicit exclusion in the 04-11 parity suite. Planner check: confirm the parity suite tolerates REST-only routes.
5. **Auto-skip trigger.** Recommendation: result long-poll pending (Temporal-like, MEDIUM). Alternative: always-auto when idle, which is simpler but races signal-before-await tests. Decide at D-P10-6.
6. **Do Phase 7 workers inherit DevClock?** Only the embedded executor does. Document that time skipping requires `placement=embedded` (the dev default).
7. **Python third-party deps.** Pure-python only. Confirm componentize-py 0.25.1 handles a `.deps` dir passed via `-p` (09 fixtures do not cover it). Wave 0.

## Proposed Plan Breakdown (waves / dependencies)

Precondition: Phase 9 complete (09-17 ladder outcome recorded, `build-guest.sh` stable, 09-07 `Runtime::describe` and ENG-1 shipped). Phase 8's clock seam shipped (F5).

| Plan | Wave | Depends | Content | Reqs |
|------|------|---------|---------|------|
| 10-01 | 1 | — | **Gate + contract (docs-only).**<br>- Verify Phase 9 outputs (build-guest.sh interface, toolchain lockfile, D-P9-3 outcome, `Runtime::describe`, SDK packaging) and Phase 8's clock seam (F5).<br>- RESERVATIONS P10 name rows (F1); STATE decisions D-P10-1..10.<br>- USER REVIEW: notify CC0-1.0 deny entry, uv pin (F3), Python artefact (F2), auto-skip trigger.<br>- Shared-file table (F6). STOP if F4/F5 are missing. | all |
| 10-02 | 2 | 10-01 | **All Cargo/deny edits + skeletons + Wave 0:**<br>- workspace pins and `deny.toml` CC0-1.0;<br>- crates `ostro-build`, `ostro-client` (empty APIs);<br>- feature `polyglot` on `ostro-build` tests;<br>- test scaffolds: fixture HTTP server, fake-toolchain archives, `clients/*` skeletons with `pytest`/`node --test`/`go test` stubs;<br>- ci-local step stubs. | all |
| 10-03 | 3 | 10-02 | **Toolchain cache:**<br>- `toolchains.toml` pins (uv, CPython via uv, componentize-py wheel hashes, Node, rustup-init, zig);<br>- fetch/verify/extract/atomic install/lock, offline/mirror;<br>- `ostro toolchain install/list/prune`, `ostro doctor`.<br>Tests run against the local fixture server; a network smoke runs `#[ignore]`. | CLI-04 |
| 10-04 | 3 | 10-02 | **SDK bundle + templates + `ostro init`:**<br>- deterministic build.rs tar of `sdks/*` and the toolchain lockfile;<br>- Rust SDK WIT self-contained (if 09-06 did not, F4);<br>- three templates, `ostro.toml` schema, init snapshot tests, name validation. | CLI-03 |
| 10-05 | 3 | 10-02 | **`ostro-client` (Rust):** extract `ostro-cli::client` (CLI switched to it, `cli_verbs` snapshots unchanged); typed core surface; OpenAPI route-coverage contract test. | SDK-07 |
| 10-06 | 3 | 10-02 | **Time skipping (server):**<br>- DevClock (both traits) through the F5 seam;<br>- dispatcher clock-change wake;<br>- DevTime idle detector (dispatcher, timers, outbox, effects, API writes), auto-on-result-wait plus manual;<br>- `GET /v1/dev/time`, `POST /v1/dev/time/skip`;<br>- `ostro dev --time-skipping` (ephemeral only), EXPIRE exclusion, `ostro_dev_*` metrics.<br>Tests: a 3-day sleep in < 5 s, signal-before-await, run timeout, step backoff, 1000-iteration race test, persistent-dir rejection. | SDK-06 |
| 10-07 | 3 | 10-02 | **Replay:**<br>- `ostro_engine::replay_check` (05-12 logic moved; corpus test switched to it);<br>- server `POST /v1/modules/{sha}/replay` (ControlPlane + FakeControlPlane, sampling, bounded pool, token key server-side);<br>- `ostro replay` remote + `--corpus`, exit 4.<br>SC3 test using Phase 9 `build-guest.sh` artefacts (conf v1/v2 and `patched` v3 per language, `polyglot` feature). | CLI-07 |
| 10-08 | 4 | 10-03, 10-04 | **Build pipeline + Rust builder:**<br>- input-hash cache, staging, env scrub;<br>- local validate + describe, `manifest.json`;<br>- Rust hermetic/system/auto, zig `cc` fallback;<br>- `ostro build --json`. | CLI-04 |
| 10-09 | 5 | 10-08 | **Python + TS builders + `--docker`:**<br>- uv/wheel recipe, pure-python deps;<br>- node + `npm ci` of the embedded lock + jco flags;<br>- `deploy/build-image/Dockerfile`, `--docker` and `--in-container`;<br>- parity test against `build-guest.sh`; `OSTRO_POLYGLOT_BUILDER=ostro` runs the Phase 9 conformance engine leg on `ostro build` artefacts. | CLI-04 |
| 10-10 | 5 | 10-08, 10-05 | **`ostro deploy` build-if-needed + `ostro dev --watch`:**<br>- D12 `.wasm` path kept;<br>- idempotent activation;<br>- notify debouncer, ignore set, coalescing, failure isolation.<br>Spawned-binary tests: edit a file, then a new version and new runs use it. | CLI-05, CLI-08 |
| 10-11 | 4 | 10-05, 10-06 | **Python/TS/Go thin clients** (zero deps) with the shared e2e scenario against spawned `ostro dev --ephemeral`, plus dev-time methods. | SDK-07 |
| 10-12 | 6 | 10-06, 10-09, 10-11 | **Test kits:**<br>- Rust (`ostro-client` `testing`), Python pytest plugin, TS `ostro-client/testing`, Go `ostrotest`;<br>- ready-line contract test;<br>- loopback HTTP mock + egress helper;<br>- each template's generated test passes (multi-day sleep in seconds). | SDK-06 |
| 10-13 | 6 | 10-10 | **Completions + `--json` stability:**<br>- `ostro completions <bash\|zsh\|fish\|powershell\|elvish>`;<br>- local-verb JSON objects carry `"kind":"ostro.<verb>.result","version":1`, are additive-only and snapshot-tested;<br>- `docs/cli-json.md`; exit-code table incl. 4. | CLI-03..08 |
| 10-14 | 7 | 10-09, 10-10, 10-12 | **Clean-machine e2e (SC1):**<br>- a script running `init → deploy → run` per language in fresh `ubuntu:24.04` and `debian:12` containers with no toolchains, timed (< 10 min), `#[ignore]`/network;<br>- the same via `--docker`;<br>- binary size gate (musl release < 60 MB, delta recorded);<br>- macOS human_needed. | CLI-03/04/05, SC1 |
| 10-15 | 8 | all | **Phase gate:**<br>- getting-started docs per language, `docs/toolchains.md` (pins, licences, mirror/offline), `docs/testing.md`, `docs/replay.md`;<br>- VALIDATION, REQUIREMENTS ticks with evidence;<br>- human_needed list: publishing ostro-client to PyPI/npm/crates.io, Go tag, build image, Rust SDK crates, macOS clean run, remote CI. | all |

**Parallelism:** wave 3 has five plans with disjoint files. The shared files are `crates/ostro/src/main.rs` (10-06 adds a `dev` flag, 10-07 `replay`, 10-10 `--watch`, 10-13 completions) and `crates/ostro-cli/src/commands/mod.rs`. Give each plan its own verb module and append-only enum variants. Never edit the same file in the same wave without the Phase 9 clean-file protocol: append-only, re-apply on a moved HEAD. Server files: 10-06 (`devclock.rs`, `dev_api.rs`, a minimal `server.rs` hunk, `dispatcher.rs` wake) and 10-07 (`replay.rs`, the ControlPlane trait method) must not share a wave without that protocol, or else put 10-07's ControlPlane hunk in wave 4.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | cargo test (Rust 1.100) + insta; pytest 8.4.2 via `uv run --no-project --python 3.12` (D-P9-13); `node --test` (Node 24); `go test` (Go ≥ 1.23) |
| Config file | none yet. Wave 0 (10-02) creates `clients/python/pyproject.toml [tool.pytest]`, `clients/ts/package.json` scripts, `clients/go/go.mod` |
| Quick run command | `cargo test -p ostro-build -p ostro-client -p ostro-cli --locked -- --test-threads=4` |
| Full suite command | `bash scripts/ci-local.sh`. It gains the steps "ostro-build polyglot builds", "clients python/ts/go", "test kits", each fail-loud and skippable only via `--no-polyglot` (and `--no-go`). Gate-only: `cargo test -p ostro --test clean_machine -- --ignored` (network + docker) |

### Phase Requirements → Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| CLI-03 | init writes the expected tree per language; refuses non-empty; name validation | unit/snapshot | `cargo test -p ostro-cli --test init` | ❌ Wave 0 |
| CLI-03 | generated projects build (all three) | integration (polyglot) | `cargo test -p ostro-build --features polyglot --test templates_build` | ❌ Wave 0 |
| CLI-04 | toolchain fetch: sha mismatch fails, traversal/symlink rejected, atomic + lock, offline/mirror | unit (fixture HTTP) | `cargo test -p ostro-build --test toolchain_cache` | ❌ Wave 0 |
| CLI-04 | build: input-hash cache, validate rejects a WASI-forbidden guest, manifest fields | integration | `cargo test -p ostro-build --test build_rust` | ❌ Wave 0 |
| CLI-04 | Python/TS parity with build-guest.sh; conformance engine leg on ostro-built artefacts | integration (polyglot) | `OSTRO_POLYGLOT_BUILDER=ostro cargo test -p ostro-conformance --features polyglot --test polyglot_python --test polyglot_ts` + `cargo test -p ostro-build --features polyglot --test parity` | ❌ Wave 0 |
| CLI-05 | deploy builds if stale, no-op on re-run, `.wasm` path unchanged, activation idempotent | spawned binary | `cargo test -p ostro --test deploy_build` | ❌ Wave 0 |
| CLI-07 | replay_check verdicts (ok / ok_frontier / diverged); corpus test via the library | unit | `cargo test -p ostro-engine --test replay_corpus --test replay_check` | ✅ / ❌ Wave 0 |
| CLI-07 | `ostro replay --sample` exits 4 on a v2 reorder and 0 on `patched` v3, in Rust/Python/TS | spawned (polyglot) | `cargo test -p ostro --features polyglot --test replay_cli` | ❌ Wave 0 |
| CLI-08 | `dev --watch`: edit → rebuild → new version; a broken edit keeps the old version; coalescing | spawned binary | `cargo test -p ostro --test dev_watch -- --test-threads=1` | ❌ Wave 0 |
| SDK-06 | DevClock: 3-day sleep < 5 s; signal-before-await; run timeout; backoff; 1000-iteration race; persistent rejected | integration | `cargo test -p ostro-server --test time_skipping` | ❌ Wave 0 |
| SDK-06 | per-language kits: template test passes in seconds | e2e | `cargo test -p ostro-client --features testing --test kit`; `uv run … pytest clients/python/tests`; `node --test clients/ts/test/`; `go test ./clients/go/...` | ❌ Wave 0 |
| SDK-07 | start/query/signal/result in TS/Python/Go/Rust; route coverage vs `ostro openapi` | e2e + contract | same as the line above + `cargo test -p ostro-client --test contract` | ❌ Wave 0 |
| SC1 | clean container `init → deploy → run` < 10 min per language; binary < 60 MB | e2e (`#[ignore]`, network, docker) | `cargo test -p ostro --test clean_machine -- --ignored --nocapture` | ❌ Wave 0 |
| — | ready-line contract; completions generate for 5 shells; `--json` schemas stable | snapshot | `cargo test -p ostro --test cli --test completions` | ✅ partial / ❌ |

### Sampling Rate
- **Per task commit:** `cargo test -p <touched crate> --locked`, plus the touched client's quick command.
- **Per wave merge:** `bash scripts/ci-local.sh --no-musl`.
- **Phase gate:** full `ci-local.sh` green, the clean-machine e2e run recorded (Linux), and the binary size recorded, all before `/gsd:verify-work`.

### Wave 0 Gaps
- [ ] `crates/ostro-build/tests/common/fixture_server.rs`: axum server serving fake tool archives with known shas (good, bad sha, traversal, symlink escape).
- [ ] `crates/ostro-build/tests/fixtures/`: tiny fake toolchains (shell-script "compilers") so builder logic is tested without network.
- [ ] `clients/python/{pyproject.toml,tests/conftest.py}`, `clients/ts/{package.json,test/}`, `clients/go/{go.mod,ostro_test.go}`.
- [ ] `crates/ostro/tests/common`: spawn helper for `--time-skipping` and `--watch` (new file; do not edit the shared `common/mod.rs` without the protocol).
- [ ] ci-local + ci.yml: a Go toolchain step (`actions/setup-go` pinned by SHA); polyglot block extensions; a `--no-go` flag.
- [ ] A Wave 0 measurement script: cold-cache timings per language in containers (Open Question 1).

## Sources

### Primary (HIGH confidence)
- crates.io API, 2026-11-19:
  - componentize-py 0.25.1 dependency list: wasmtime ^46.0.1, wasmtime-wasi ^46.0.1, wit-component/wasmparser/wasm-encoder ^0.245, bincode ^1.3.3, pyo3 optional; crate has a `[[bin]] componentize-py`.
  - componentize-qjs 0.6.0 dependency list: wasmtime/wasmtime-wasi/wasmtime-wizer ^50, wit-component/wasmparser ^0.262, oxc_resolver; no bin.
  - reqwest 0.13.5 feature map (`rustls` = aws-lc-rs + rustls-platform-verifier).
  - Licences: notify 8.2.2 CC0-1.0, option-ext MPL-2.0, webpki-roots CDLA-Permissive-2.0, lzma-rust2 Apache-2.0, clap_complete/tar/flate2/notify-debouncer-full MIT/Apache.
- GitHub releases API: componentize-py v0.25.1 assets and `checksums-v0.25.1.txt` (linux-amd64 tgz sha256 `4c799e8f…5b`, verified locally); uv 0.13.9 musl/darwin assets.
- Local inspection (not executed): componentize-py linux-amd64 binary needs `GLIBC_2.39` (readelf); wheel `manylinux_2_28` = `componentize_py.abi3.so` + entry point `componentize-py=componentize_py:script`.
- PyPI JSON (componentize-py 0.25.1 wheel sizes); npm registry (jco 1.42.0 dependencies, componentize-qjs 0.5.0/0.6.0, QuickJS binding packages glibc); nodejs.org `dist/index.json` (v24.22.3 LTS, tarball sizes); ziglang.org `download/index.json` (0.17.1 `.tar.xz` only).
- Repo at `21d9e58`:
  - `crates/ostro/src/main.rs` (ready line, dev flags);
  - `crates/ostro-cli/src/{client,output,commands/*}.rs` (05-14);
  - `crates/ostro-engine/src/{testkit,env}.rs`, `tests/replay_corpus.rs`, `examples/record_corpus.rs`;
  - `crates/ostro-partition/src/{clock,node}.rs`, `leader/{timers,dispatcher}.rs`;
  - `crates/ostro-server/src/server.rs` (`now_ms` via SystemClock);
  - `sdks/rust/ostro-sdk/src/bindings.rs` (WIT path); `deny.toml`; `Cargo.toml`.
- `.planning/`: ROADMAP, REQUIREMENTS, PROJECT, STATE, RESERVATIONS; 05-12-SUMMARY; 05-DECISIONS (D12); 09-RESEARCH; 09-DECISIONS (D-P9-1/3/12/13/14/19, shared files, Phase 10 seam); 09-02/09-17 plans; 06-DECISIONS/06-RESEARCH (egress `allow_private`, no webpki-roots); 08-14 plan (injected Clock, FakeClock TestServer); docs/02-prd.md FR-1.10/FR-2.x, NFR-2.1; docs/05-roadmap.md.

### Secondary (MEDIUM confidence)
- [Temporal Python testing suite](https://docs.temporal.io/develop/python/testing-suite): time skipping fast-forwards timers "except when Activities are running", plus `env.sleep()` manual skip and the global-time caveat. The "unlock while awaiting result" behaviour is from training knowledge of the TS/Java SDKs; not re-verified.
- Rust `wasm32-wasip2` ships `wasm-component-ld`; proc-macros need a host `cc`; `std::fs::File::lock` is stable since 1.89. These come from training knowledge and are consistent with the repo's CI (zigbuild); verify at implementation.

### Tertiary (LOW confidence)
- jco toolchain install size and `npm ci --ignore-scripts` compatibility.
- zig-as-`cc` on macOS for Rust proc-macros.
- Cold-cache timings per language.

## Metadata

**Confidence breakdown:**
- Toolchain strategy and stack: HIGH for the rejections (dependency graphs, deny rules, sizes, glibc measurement); MEDIUM for the uv/wheel recipe on all platforms (Phase 9 uses the same recipe, but it has not been run hermetically).
- Architecture (time skipping, replay): MEDIUM, from code reading of seams that Phases 7/8 will still change.
- Pitfalls: HIGH for 1, 3, 4, 8, 9 and 13 (code or measurement); MEDIUM for the others.

**Research date:** 2026-11-19
**Valid until:** 2026-12-19. Re-check uv, Node 24, componentize-py (a canary exists) and jco pins at 10-01. Wasmtime 51 (~2026-12-20) does not affect this phase.
