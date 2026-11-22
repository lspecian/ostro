# Build toolchains (`ostro build`)

`ostro build` turns a Rust, Python or TypeScript project into a validated workflow
component (`target/ostro/<name>.wasm` + `manifest.json`) without a preinstalled
toolchain. This page covers where the toolchains come from, where they are cached, how to
build offline or through a mirror, the `--docker` fallback and the licensing position.
Plans 10-07 (cache), 10-12 (pipeline, Rust) and 10-15 (Python, TypeScript, Docker).

## Why downloaded and not embedded (D-P10-1)

"Toolchain-free" means: pinned, sha256-verified downloads go into a content-addressed
cache on first use, with `--docker` as the fallback. Nothing is linked into or embedded in
the ostro binary. The rejected options:

- **componentize-py / componentize-qjs as crates:** they pull wasmtime, wit-component and
  bincode versions that break the workspace's `cargo deny` bans.
- **Embedded tool binaries:** they would break the ostro binary size budget (NFR-2.1,
  under 60 MB).
- **Wasm-hosted componentizers:** they cannot pre-initialise the guest (snapshotting
  needs a native host).

Only the SDK sources (Rust, Python, TS, the TS toolchain lock) are embedded and
materialised into `target/ostro/sdk/`.

## Pins

The pins are embedded from `crates/ostro-build/toolchains.toml`. Every sha256 comes from
the official checksum source named in a `# source:` comment there. A sha mismatch fails the
install, and nothing is executed before verification.

| Tool | Version | Platforms | Source of the sha |
|---|---|---|---|
| uv | 0.7.15 | linux x86_64/aarch64 (static musl), macOS arm64/x86_64 | `<file>.sha256` release files on github.com/astral-sh/uv |
| componentize-py (wheel) | 0.25.1 | `manylinux_2_28_{x86_64,aarch64}`, `macosx_11_0_arm64`, `macosx_10_12_x86_64` | PyPI JSON API (`urls[].digests.sha256`) |
| CPython (host interpreter for componentize-py) | 3.12 (uv-managed) | as uv | uv's own python-build-standalone checksums |
| Node.js | 24.13.0 (the 09-17 CI pin) | linux x64/arm64 (glibc), macOS arm64/x64 | `SHASUMS256.txt` on nodejs.org |
| jco + componentize-js | 1.42.0 + 0.23.0 | anywhere Node runs | `integrity` hashes in the embedded `package-lock.json` (byte-identical to `sdks/scripts/toolchain/`) |
| rustup-init | 1.29.3 | all four | `rustup-init.sha256` on static.rust-lang.org |
| Rust | 1.100.0 + `wasm32-wasip2` (the template's `rust-toolchain.toml`) | all four | rustup's channel manifest |
| zig (host `cc` fallback) | 0.17.1 | all four | ziglang.org `index.json` shasum |

The `manifest.json` `toolchains.tools` list records what a build used, for example
`uv@0.7.15`, `cpython@3.12 (uv-managed)`, `componentize-py@0.25.1 (wheel 8623cd8c)`,
`node@24.13.0`, `jco@1.42.0 (lock 6fa8b71e)`, `system-rustup`,
`rustup-init@1.29.3`, `rust@1.100.0`, `zig@0.17.1`, or `docker (ostro-build:<version>)`.

## Cache location and layout

The cache root is resolved in this order:

1. `$OSTRO_TOOLCHAIN_DIR`;
2. `$XDG_CACHE_HOME/ostro/toolchains`, when that path is absolute;
3. `~/.cache/ostro/toolchains` (Linux) or `~/Library/Caches/ostro/toolchains` (macOS).

```
<root>/.lock                               exclusive lock (installs, venv/npm setup, prune)
<root>/tools/<tool>-<version>-<sha8>/      one verified artefact, complete once .ok exists
<root>/state/cpy-venv-0.25.1-<sha8>/       uv cache, uv-managed CPython, venv-<roothash8>/
<root>/state/jco-<lockhash16>/             npm ci of the jco toolchain lock
<root>/state/npm/{npmrc,npm-cache}         private npm config and cache
<root>/state/rustup, state/cargo           hermetic RUSTUP_HOME / CARGO_HOME
<root>/state/zig-shim/{cc,ar}              the zig cc/ar shims
```

- `tools/` holds verified downloads. Archives are extracted with path checks (no absolute
  paths, no `..`, no symlinks escaping the entry). Wheels are stored as they are.
- `state/` holds builder state derived from `tools/`.
- **Commands:** `ostro toolchain install [--lang L | --all]` pre-fetches, `ostro toolchain
  list` shows entries, and `ostro toolchain prune` removes `tools/` entries no pin
  references. `prune --state` also clears `state/`. `ostro doctor` reports the cache, the
  platform, the host glibc/musl and docker.
- **Venv path binding:** the venv dir name carries a hash of the canonical cache root,
  because a venv is bound to its absolute path. The same cache is mounted at `/cache` by
  `--docker`, and the host and the container each get their own venv.
- **Downloads:** connect timeout 30 s, read-idle timeout 60 s, total 15 min per artefact,
  and a size cap per pin (`max_bytes`, default 512 MiB).

Disk use measured on linux-x86_64 for Python + TS: about 1 GB in total.
- `tools/`: node 202 MB, uv 43 MB, wheel 17 MB.
- `state/`: jco toolchain 426 MB, npm cache 123 MB, CPython + venv 154 MB.
- Rust hermetic adds about 1 GB (rustup, Rust 1.100.0 minimal + wasm32-wasip2, zig).

## Offline and mirrors

- **`OSTRO_OFFLINE=1`** (or `ostro build --offline`): the build never touches the network.
  - Cached tools work.
  - A missing tool fails with `<tool> is offline and not cached`.
  - uv gets `UV_OFFLINE=1`, npm gets `npm_config_offline=true` and cargo gets
    `CARGO_NET_OFFLINE=true`.
  - A warm cache rebuilds edited Python and TS projects offline (tested: `python_offline_after_warm` and `ts_offline_after_warm`).
- **`OSTRO_TOOLCHAIN_MIRROR=<base>`** replaces the scheme and host of every pinned URL with
  `<base>`, keeping the path. The sha stays authoritative, so a mirror cannot substitute
  bytes. Expected mirror layout:

| What | URL under the mirror | Used as |
|---|---|---|
| pinned artefacts | `<base>/<path of the pinned URL>`, e.g. `<base>/astral-sh/uv/releases/download/0.7.15/uv-x86_64-unknown-linux-musl.tar.gz`, `<base>/dist/v24.13.0/node-v24.13.0-linux-x64.tar.xz` | the ostro downloader |
| uv-managed CPython | `<base>/python-build-standalone/<release>/<file>` | `UV_PYTHON_INSTALL_MIRROR=<base>/python-build-standalone` |
| PyPI (project dependencies) | `<base>/pypi/simple/` (PEP 503) | `UV_DEFAULT_INDEX=<base>/pypi/simple` |
| npm registry | `<base>/npm/` (npm-registry compatible) | `registry=` in the private npmrc |
| rustup | `<base>/rustup/dist/…`, `<base>/rustup/rustup/…` | `RUSTUP_DIST_SERVER=<base>/rustup`, `RUSTUP_UPDATE_ROOT=<base>/rustup/rustup` |

Proxies (`HTTP(S)_PROXY`, `NO_PROXY`, `ALL_PROXY`) and `SSL_CERT_FILE`/`SSL_CERT_DIR` pass
through to the tools. Nothing else from the host environment does: every step runs with the
scrubbed environment `TZ=UTC LC_ALL=C PYTHONHASHSEED=0 PYTHONDONTWRITEBYTECODE=1
SOURCE_DATE_EPOCH PATH=<tool dirs>:/usr/bin:/bin HOME`, as `sdks/scripts/build-guest.sh`
uses.

## Rust (D-P10-4)

`--toolchain auto|hermetic|system|docker` selects the toolchain. The same values work as
`[build] toolchain` in `ostro.toml`.

- **`auto`** (the default) uses the system `rustup` when it is on `PATH`, and hermetic
  otherwise. rustup honours the project's `rust-toolchain.toml` (the template pins 1.100.0
  with `targets = ["wasm32-wasip2"]`).
- **`hermetic`** installs the pinned `rustup-init` into the cache (`--profile minimal`,
  `RUSTUP_HOME`/`CARGO_HOME` in `state/`), then installs the project's channel and targets
  under the cache lock. `RUSTFLAGS`, `CARGO_ENCODED_RUSTFLAGS`, `RUSTC_WRAPPER` and
  `CARGO_BUILD_*` are never inherited.
- **zig cc fallback:** when the host has no `cc`, `gcc` or `clang`, ostro uses zig 0.17.1
  through a generated `cc`/`ar` shim in `state/zig-shim`, set as
  `CARGO_TARGET_<HOST>_LINKER` and `CC`. The argument filtering is ported from
  cargo-zigbuild (MIT OR Apache-2.0). `OSTRO_NO_CC=1` forces the zig fallback even when a
  C compiler exists, and `OSTRO_ZIG_SHIM_LOG=<file>` logs every shim call.
- **`Cargo.lock`:** `--locked` is passed when `Cargo.lock` exists. Without one it is
  generated, with a warning.

## Python (D-P10-2)

- **Toolchain:** uv 0.7.15 creates a venv with a uv-managed CPython 3.12
  (`UV_PYTHON_PREFERENCE=only-managed`, `UV_NO_CONFIG=1`; `UV_CACHE_DIR`,
  `UV_PYTHON_INSTALL_DIR` and `UV_TOOL_DIR` live in the cache). It installs the
  sha-verified **componentize-py 0.25.1 wheel** offline (`--no-index --no-deps <wheel>`).
- **Why not the release binary:** ostro never uses the GitHub release binary of
  componentize-py on Linux, because it needs GLIBC_2.39. The manylinux_2_28 wheel runs on
  glibc 2.28 and newer.
- **Flags:** componentize-py runs with exactly build-guest.sh's flags: `-d <sdk>/python/ostro/wit
  -w ostro:runtime/workflow@0.1.0 componentize --stub-wasi -p <stage> <entry> -o <out>`. The
  SDK is staged next to the entry module. `crates/ostro/tests/parity.rs` checks the flags
  token by token.
- **Dependencies:** only pure-Python dependencies are supported.
  - They come from `uv.lock` (`uv export --frozen --no-dev --no-emit-project`, which emits
    hashes, then `uv pip install --require-hashes -r`) or else from pyproject
    `[project] dependencies`.
  - They install into `<stage>/.deps` with `--only-binary :all: --python-version 3.14`
    (the CPython inside componentize-py) and are passed as a second `-p`.
  - Any wheel not tagged `py3-none-any` fails the build with the package named, for
    example `pydantic_core is not a pure-Python wheel (tag cp314-…); native extensions are
    not supported`.
- **`OSTRO_COMPONENTIZE_PY=<path>`** uses another componentize-py binary, typically the
  D-P9-3 deterministic fork built by `sdks/scripts/build-componentize-py-fork.sh`. It is a
  CI opt-in and prints a warning. The path is part of the input hash.
- **Reproducibility (inherited residual):** Python builds are **not bit-reproducible**.
  With the released wheel, the str hash secret and the snapshot layout differ per build
  (see [sdk-builds.md, "Artefact for Phase 10"](sdk-builds.md#artefact-for-phase-10),
  `RESIDUAL-ACCEPTED: py`). Recorded journals still replay across rebuilds, with one
  exception: code that iterates an unsorted `set` of `str` into commands.
- **Unsupported hosts:** musl Linux and glibc < 2.28 get `Python builds need glibc >= 2.28;
  use --docker`.

## TypeScript (D-P10-3)

- **Toolchain:** Node 24.13.0 comes from the cache. The SDK bundle's pinned toolchain
  `package.json` + `package-lock.json` (jco 1.42.0, componentize-js 0.23.0) is installed
  once per lock hash with `npm ci --ignore-scripts --no-audit --no-fund`. The install uses
  a private npmrc and the npm cache inside the ostro cache. No package in the pinned lock
  has an install script, so `--ignore-scripts` loses nothing.
- **Staging:** the project's `src/**`, `package.json`, `package-lock.json` and
  `tsconfig.json`, then `npm ci --ignore-scripts` when the project lock has dependencies,
  then the SDK as `node_modules/ostro`.
- **Flags:** `jco componentize <entry> --wit <sdk>/ts/wit --world-name workflow --backend
  quickjs --backend-qjs-disable-async --backend-qjs-stub-wasi --bundle-config
  <toolchain>/rolldown.ostro.mjs -o <out>`, token for token build-guest.sh's.
- **Reproducibility:** QuickJS builds differ by a snapshot PRNG value (`RESIDUAL-ACCEPTED:
  ts` in sdk-builds.md). The ostro-built `ts-min` has the same length as the
  build-guest.sh build and the same imports and exports.
- **Unsupported hosts:** official Node builds are glibc-only, so musl/Alpine hosts get an
  error pointing at `--docker`.
- **`--toolchain system`** is not supported for Python or TypeScript. It prints a warning
  and uses the pinned toolchain.

## `--docker` (D-P10-15)

`ostro build --docker` (or `[build] toolchain = "docker"`) runs the build in the image
`ostro-build:<ostro version>`.

- **Image:** built locally on first use from the Dockerfile embedded in the binary
  (`deploy/build-image/Dockerfile`, fed to `docker build -t ostro-build:<version> -` on
  stdin). The base is `debian:12-slim` pinned by digest, plus `ca-certificates`. No
  toolchain is baked in: the container runs the hermetic path and fills the mounted cache.
  The tag only changes with the ostro version, so after changing the Dockerfile in a
  development build, run `docker rmi ostro-build:<version>`.
- **Run:** `docker run --rm --user <uid>:<gid> -v <ostro>:/usr/local/bin/ostro:ro -v
  <project>:/src -v <cache>:/cache -e HOME=/tmp -e SOURCE_DATE_EPOCH=… [-e OSTRO_OFFLINE=1]
  [-e OSTRO_TOOLCHAIN_MIRROR=…] ostro-build:<version> ostro build /src --in-container --json`.
  The container writes `target/ostro/` in place. The host copies the wasm, validates it
  again and writes the manifest (tool `docker (ostro-build:<version>)`).
- **The ostro binary:** the mounted binary must run in the container. On Linux it is the
  running ostro, which should be a static musl build: a dynamically linked binary (one
  with an ELF `PT_INTERP`) gets a warning, and works only when the image's glibc 2.36 is
  new enough for it. On macOS, set `OSTRO_LINUX_BIN` to a static Linux musl ostro for
  the container's architecture.
- **Docker CLI:** `OSTRO_DOCKER=<path>` overrides the docker CLI (the tests use a
  recording shim).
- **Publishing:** the image is never pushed in Phase 10. Publishing `ostro-build` images is
  human_needed.

## Licences

- Downloaded tools (uv, CPython, componentize-py, Node, jco, rustup, Rust, zig, npm
  packages) are fetched by the user's machine from their upstream locations and are **not
  redistributed** by ostro.
- Some runtime code is embedded in the built component, and `manifest.json`
  `embedded_runtimes` lists it:
  - **Python:** CPython (PSF-2.0), libc++/libc++abi (Apache-2.0 WITH LLVM-exception),
    wasi-libc and the componentize-py runtime (Apache-2.0 WITH LLVM-exception).
  - **TypeScript:** QuickJS-NG and rquickjs (MIT).
- The `ostro-build` image contains Debian packages under their own licences. A NOTICE for
  a published image is human_needed at publish time.

## Measured build times (linux-x86_64, WSL2, 12 cores, 2026-11-22)

| Project | Cold (empty cache) | Up-to-date check | Warm rebuild after an edit | Size |
|---|---|---|---|---|
| Python template (`ostro build`) | 11.7 s (uv, wheel, CPython download, venv) | 0.25–0.28 s | 6.3–7.3 s | ~19.8 MB |
| Python template + `attrs` | – | – | 7.5 s | ~22.7 MB |
| TS template (`ostro build`) | 22.1 s (Node download + `npm ci` of the jco lock) | 0.035–0.06 s | 2.0–2.6 s | ~2.1 MB |
| TS template, `--docker` (first image build included) | 12 s with a warm cache | – | – | ~2.1 MB |
| Python template, `--docker` (image build + container venv) | 35 s | – | – | ~19.9 MB |
| Rust template (system rustup) | 19–23 s | 13 ms | 1.6–1.9 s | 176 KB |
| Rust fixture, hermetic + zig, no cc | 67 s | – | – | 148 KB |
