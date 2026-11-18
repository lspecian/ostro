---
phase: 04-single-node-server-rust-mvp
plan: 02
subsystem: server-config
tags: [workspace, config, cli-02, axum, tonic, jiff, tracing]
requires: []
provides:
  - Phase 4 root workspace pins (utoipa, utoipa-axum, serde_json, config, jiff, tracing-subscriber, tower-http, getrandom, reqwest)
  - crates/ostro-server (ServerConfig, load_layered, init_tracing)
  - one-port HTTP/1.1 + gRPC smoke test for ostro-api
affects: [04-08, 04-11, 04-17, 04-19]
tech-stack:
  added: [utoipa 6.0.0, utoipa-axum 0.3.0, serde_json 1, config 0.15.27, jiff =1.0.1, tracing-subscriber 0.3.23, tower-http 0.7.1, getrandom 0.4.3, reqwest 0.13.5 (dev)]
  patterns: [layered config base<TOML<env<overrides, FriendlyDuration via jiff friendly, use x as _ dependency anchors]
key-files:
  created:
    - crates/ostro-server/Cargo.toml
    - crates/ostro-server/src/lib.rs
    - crates/ostro-server/src/config.rs
    - crates/ostro-server/src/logging.rs
    - crates/ostro-server/tests/config.rs
    - crates/ostro-api/tests/http_smoke.rs
  modified:
    - Cargo.toml
    - Cargo.lock
    - crates/ostro-api/Cargo.toml
    - crates/ostro-api/src/lib.rs
decisions:
  - "Env layer reads only OSTRO_<SECTION>__* for known sections (node/api/engine/executor/log); other OSTRO_* vars (e.g. OSTRO_DET_ITERS) are ignored, unknown keys inside a known section are rejected"
  - "Server log format = compact (no tracing-subscriber json feature); dev = pretty"
  - "--print-config TOML rendered by a small serde_json-based renderer (config shape is tables of scalars), no toml serializer dep"
  - "EngineSection exposes hot_wait, activation_timeout, activation_fuel, compile_permits, node_memory_budget, continue_as_new_events, pool_total_instances; no aot_cache flag (EngineConfig has no such field)"
  - "deny.toml unchanged: no new licenses, no banned duplicates (utoipa-axum unifies with axum 0.8.9)"
metrics:
  duration: 25min
  completed: 2026-11-18
---

# Phase 4 Plan 02: Workspace pins, ostro-server layered config, one-port smoke test Summary

All Phase 4 third-party pins now sit in the root manifest and every one is used. The new `ostro-server` crate holds `ServerConfig`: defaults, then a TOML file, then `OSTRO_*` env vars, then CLI overrides. Durations are friendly strings parsed by jiff, and unknown keys are rejected. The one-port HTTP/1.1 + gRPC mux has a real smoke test (reqwest plus a tonic `HealthClient` on one ephemeral listener).

## What was built

**Task 1 (3817a9f): pins, ostro-api anchors, smoke test.**
- Root `[workspace.dependencies]` gets a `# Phase 4` block. Versions were checked with `cargo search`/`cargo info`. jiff is pinned exactly at `=1.0.1`. reqwest 0.13.5 has `default-features = false` and `json, http2`, so it brings no TLS and no native-tls or openssl. getrandom 0.4.3 is the same major uuid already uses.
- `sdks` was added to the workspace `exclude` list.
- ostro-api anchors utoipa, utoipa-axum, tower-http, serde_json, jiff, serde, async-trait and bytes with `use x as _;`. 04-08 consumes them. reqwest is a dev-dep.
- `tests/http_smoke.rs`: an http1-only reqwest call to `GET /healthz` returns 200 "ok" over HTTP/1.1, and `HealthClient::connect` + `check` returns SERVING on the same port.
- `cargo deny check`: advisories, bans, licenses and sources all ok. deny.toml needed no change.

**Task 2 (6989736): ostro-server.**
- `ServerConfig { node, api, engine, executor, log }` uses `#[serde(default, deny_unknown_fields)]` on every section.
- `dev_defaults()`: 127.0.0.1:7700, `./.ostro`, pretty logs. `server_defaults()` (also `Default`): 0.0.0.0:7700, `./ostro-data`, compact logs. Both use P=16 and node_id 1.
- Executor: lease_ttl 30s, heartbeat None (`effective_heartbeat()` = lease_ttl/3), max_attempt_failures 3, slots 64, retry_backoff 1s. API: max_module_bytes 64 MiB, max_request_bytes 4 MiB. Engine defaults come from `EngineConfig::default()` (hot_wait 2s).
- `load_layered(base, file, explicit_file, &EnvSource, &Overrides)`:
  - Layer order: `Config::try_from(&base)`, then the TOML `File` (`required(explicit_file)`; a missing explicit file is a `FileNotFound(path)` error), then `Environment::with_prefix("OSTRO").prefix_separator("_").separator("__").try_parsing(true).source(..)`, then `try_deserialize`, then overrides.
  - `EnvSource::Map` lets tests inject env vars without touching the process env.
- `FriendlyDuration`: serde goes through `jiff::fmt::friendly` `SpanParser::parse_unsigned_duration` / `SpanPrinter`. A bad value gives an error with the field and the input in it, e.g. `lease_ttl` / "5 parsecs".
- `to_toml_string()` prints durations as friendly strings (`lease_ttl = "30s"`), and the output loads back to an identical config.
- `EngineSection::to_engine_config()`.
- `logging::init_tracing(&LogSection) -> bool`: `RUST_LOG` takes priority over `log.level`; output is pretty or compact; it uses `try_init`, so calling it twice is harmless.
- Tests: 16 integration tests in tests/config.rs and 1 unit test (logging idempotence).

## Verification

- `cargo test -p ostro-api -p ostro-server --locked`: all green.
- `cargo clippy -p ostro-api -p ostro-server --all-targets --locked -- -D warnings`: clean.
- `cargo build --workspace --locked`: no warnings.
- `cargo fmt --all --check`: ok.
- `cargo deny check`: ok.
- All of the above were re-run on committed HEAD in a scratch worktree, so other plans' uncommitted work could not affect the results.

## Deviations from Plan

**1. [Rule 3 - Blocking] Pins moved from Task 1 to Task 2.** `config`, `tracing-subscriber` and `getrandom` have no consumer until ostro-server exists. The `unused_workspace_dependencies` lint would have warned at the Task 1 commit, so these three pins went into the Task 2 commit.

**2. [Rule 2 - Correctness] Env layer filtered to known sections.** `deny_unknown_fields` combined with a prefix-only env source would make any unrelated `OSTRO_*` variable fail startup; `OSTRO_DET_ITERS` is already used in tests. Only `OSTRO_<known section>__*` is read. Typos inside a known section are still rejected, and a test covers this.

**3. API shape: `load_layered` takes an `&EnvSource` argument.** The plan's 4-argument signature plus "injectable env source" became a 5th parameter.

**4. No `aot_cache` field in `EngineSection`.** `EngineConfig` has no such field, and the plan says to include only fields that map.

**5. TDD: no separate RED commit.** RED was confirmed locally: the test compiled against the empty module and failed. A RED commit would have left HEAD with unused-dependency warnings and a lockfile out of step with the manifest, so the tests and the implementation went into one commit.

**6. Cargo.lock staged selectively in Task 1.** The working-tree lock already held 04-01's in-progress ostro-meta deps. The committed lock keeps HEAD's ostro-meta block, so `--locked` stays consistent at each commit.

## Notes for later plans

- CLI-02 is proven at library level only. The `ostro server` binary wiring, zero-config startup and `--print-config` belong to 04-19, so CLI-02 is not marked complete.
- `toml` 1.2.1 is now in the lock (transitively via `config`) if a real serializer is ever wanted.

## Self-Check: PASSED
