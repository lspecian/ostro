# Getting started

This guide goes from an empty directory to a deployed, running durable workflow in Rust,
Python or TypeScript. The only thing to install is the `ostro` binary. Language
toolchains (Rust, uv + CPython + componentize-py, Node + jco) are downloaded on first use,
pinned and sha256-verified, into the Ostro cache ([toolchains.md](toolchains.md)).

Each section runs the same loop:

1. `ostro init`: generate a project with one workflow, `hello`. It greets, sleeps for
   three days (a durable timer), then waits for the signal `approve`.
2. `ostro dev --watch`: a local server that rebuilds and redeploys on every save.
3. `ostro deploy`: build if needed, upload and activate.
4. `ostro run` + `ostro signal`: start a run and approve it.
5. The generated test (`cargo test`, `pytest`, `npm test`), against a throwaway server.
6. `ostro replay`: check a change against recorded runs before you deploy it.

## Before you start

```sh
ostro --version
ostro doctor                    # what is cached, what will be downloaded, Docker, cc
ostro toolchain install --all   # optional: pre-fetch every toolchain now (about 2 GB)
```

First-run downloads, per language (sizes from [toolchains.md](toolchains.md)):

| Language | Downloaded on first build | Cache size |
|---|---|---|
| Rust | rustup-init, Rust 1.100.0 (minimal) + `wasm32-wasip2`, zig 0.17.1 if no `cc` | about 1 GB (none if your rustup already has `wasm32-wasip2`) |
| Python | uv 0.7.15, uv-managed CPython 3.12, the componentize-py 0.25.1 wheel | about 200 MB |
| TypeScript | Node 24.13.0, the pinned jco 1.42.0 lockfile (`npm ci`) | about 750 MB |

The cache lives in `$OSTRO_TOOLCHAIN_DIR` (else the platform cache dir). `ostro doctor`
prints the path.

`ostro dev` serves on `http://127.0.0.1:7700` by default. The other commands talk to it
through `--server` / `OSTRO_SERVER` (the same default), so the loop needs two terminals.

## Rust

```sh
ostro init hello --lang rust
cd hello
ostro dev --watch               # terminal 1: server + rebuild/redeploy on save
```

In terminal 2:

```sh
ostro deploy                    # builds target/ostro/hello.wasm if stale, uploads, activates
ostro run hello --input '{"name":"ada"}'            # prints the run id
ostro signal <run-id> approve --payload '{"ok":true}'
ostro result <run-id> --wait    # completes after the three-day sleep
```

With a plain `ostro dev` the run really sleeps for three days. For a quick look, run the
server with time skipping instead (in-memory only):

```sh
ostro dev --ephemeral --time-skipping --watch
ostro run hello --input '{"name":"ada"}' --wait     # in terminal 2; send approve meanwhile
```

Test and replay:

```sh
cargo test                      # tests/workflow.rs: throwaway `ostro dev --ephemeral --time-skipping`
ostro build && ostro replay --sample 20 --module target/ostro/hello.wasm
```

`ostro build` uses your rustup if it has `wasm32-wasip2`, and the hermetic Rust otherwise
(`[build] toolchain = "auto"`). Without a C compiler on the host it links build scripts
and proc macros with zig (`OSTRO_NO_CC=1` forces this).

## Python

```sh
ostro init hello --lang python
cd hello
ostro dev --watch               # terminal 1
```

In terminal 2:

```sh
ostro deploy
ostro run hello --input '{"name":"ada"}'
ostro signal <run-id> approve --payload '{"ok":true}'
ostro result <run-id> --wait
```

Test and replay:

```sh
pytest                          # tests/test_workflow.py with the ostro_env / ostro_project fixtures
ostro build && ostro replay --sample 20 --module target/ostro/hello.wasm
```

`pytest` itself is not downloaded by Ostro; any Python with `pytest` and the bundled
`ostro_client` runs the test (for example
`uv run --no-project --python 3.12 --with pytest python -m pytest tests -q`). Workflow
dependencies in `pyproject.toml` must be pure-Python wheels.

## TypeScript

```sh
ostro init hello --lang ts
cd hello
ostro dev --watch               # terminal 1
```

In terminal 2:

```sh
ostro deploy
ostro run hello --input '{"name":"ada"}'
ostro signal <run-id> approve --payload '{"ok":true}'
ostro result <run-id> --wait
```

Test and replay:

```sh
npm test                        # node --test; the bundled ostro-client/testing TestEnv
ostro build && ostro replay --sample 20 --module target/ostro/hello.wasm
```

`npm test` needs a Node on your PATH (the hermetic Node in the Ostro cache works too).

## Changing a workflow

Workflow versions are declared in the source (`#[workflow(name = "hello", version = 2)]`,
`@ostro.workflow(name="hello", version=2)`, `workflow({ name: "hello", version: 2 }, …)`).
A real server (and a persistent `ostro dev`) refuses a second module for the same name and
version: versions are write-once, so bump the version when you change a workflow's code.
`ostro dev --watch` prints the refusal with a hint and keeps the previous version active.

`ostro dev --ephemeral --watch` is the exception: there a changed module under an existing
name and version replaces the old one in place. New runs use the new module; runs already
started keep the module they started with. No version bump is needed while you iterate
on a throwaway dev server. Before deploying the change to a real server, replay
recorded runs against it ([replay.md](replay.md)):

```sh
ostro replay --server "$OSTRO_SERVER" --sample 20 --module target/ostro/hello.wasm
```

## Toolchain notes

- **Hermetic by default.** Nothing from your PATH is used except rustup (Rust,
  `toolchain = "auto"`) and a C compiler (else zig). Builds run with a scrubbed
  environment. `ostro build --toolchain hermetic` ignores the system rustup.
- **Offline / mirrors.** `OSTRO_OFFLINE=1` uses only the cache;
  `OSTRO_TOOLCHAIN_MIRROR=<url>` redirects every download ([toolchains.md](toolchains.md)).
- **`--docker`.** `ostro build --docker` / `ostro deploy --docker` builds inside a locally
  built `ostro-build:<version>` image (debian:12-slim, pinned by digest). Use it on musl
  hosts (Alpine), hosts with a glibc older than the toolchains need, and macOS. The
  container needs a Linux `ostro`: the static musl release binary works everywhere, and
  `OSTRO_LINUX_BIN=<path>` points at it when the host binary is not Linux.
- **macOS.** Native builds use the darwin toolchains of the same pins (uv, the wheel, Node,
  rustup and zig are pinned for macOS arm64 and x86_64). If a build fails natively,
  `ostro deploy --docker` with Docker Desktop and `OSTRO_LINUX_BIN`
  set to the linux-musl `ostro` is the fallback.
- **Inside a container.** `ostro dev --ephemeral` puts its data in `/dev/shm` only when
  that exists, is writable and has at least 512 MiB free. Otherwise it uses the temp dir
  (`TMPDIR`, usually `/tmp`), and one startup log line says which it picked and why.
  Docker's default 64 MB `/dev/shm` therefore works, but startup is slower because the
  data is on disk. `--shm-size 1g` keeps it on tmpfs (faster startup), and is optional.
- **`ostro doctor --json`** reports the platform, cache, toolchains, Docker and `cc` in
  the stable JSON envelope ([cli-json.md](cli-json.md)).

## Measured: clean machine (SC1)

`scripts/clean-machine.sh` runs `ostro init → ostro dev → ostro deploy → ostro run`
(including the approve signal and the time-skipped three-day sleep) with no language
toolchain on PATH, a fresh HOME and an empty toolchain cache. Timings from
`bash scripts/clean-machine.sh all --local --no-cc` (linux-x86_64, WSL2, cold cache, so
every toolchain is downloaded; the run uses `--no-cc`, so Rust links with zig):

| Language | Cold (empty cache) | Warm (`--cache`, toolchains already downloaded) | of which `ostro deploy` (cold) |
|---|---|---|---|
| Rust (hermetic rustup + Rust 1.100.0, zig as `cc`) | 57.8–64.5 s | 24.9 s | 57.2–63.6 s |
| Python (uv, CPython 3.12, componentize-py) | 16.5–19.9 s | 16.4 s | 15.7–18.9 s |
| TypeScript (Node 24, jco `npm ci`) | 23.0–24.6 s | 5.9 s | 22.3–23.9 s |

In fresh containers (static musl `ostro`, cold cache): Rust 46.7 s / 74.4 s, Python
13.8 s / 13.3 s, TypeScript 16.0 s / 15.6 s (ubuntu:24.04 / debian:12). Every language is
far below the 10-minute target; almost all of the time is the first toolchain download and
the first build. `init`, `dev` startup and the run itself (the three-day sleep is skipped)
take well under a second.

The same script runs in fresh `ubuntu:24.04` and `debian:12` containers with only
ca-certificates installed (`--image`), which is the CI `clean-machine` job.

## Further reading

- [toolchains.md](toolchains.md): pins, cache layout, mirrors, `--docker`.
- [testing.md](testing.md): the test kits in Rust, Python, TS and Go.
- [replay.md](replay.md): replaying recorded runs against a new module.
- [cli-json.md](cli-json.md): the `--json` envelope of every command.
