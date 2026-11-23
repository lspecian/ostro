# size1: shrink the musl release binary (binary-size option 1)

**Date:** 2026-11-23 · **Type:** follow-up (coordinator-approved build-profile change)

## What changed

- Root `Cargo.toml`: added `[profile.release.package.<crate>] opt-level = "s"` for cold
  crates only: `ostro-api`, `ostro-cli`, `ostro-client`, `ostro-ui`, `ostro-build`, `clap`,
  `clap_builder`, `utoipa`, `utoipa-axum`, `kube`, `kube-client`, `kube-core`, `k8s-openapi`,
  `serde-saphyr`. Only profile sections changed: no dependency changes and no changes to
  `Cargo.lock`. The hot path keeps opt-level 3: wasmtime/cranelift, engine, partition, log,
  wal, server, fjall, tokio, hyper/h2, tonic, prost, rustls/aws-lc.
- `docs/binary-size.md`: new measurements and an "Applied: option 1" section.
- Option 2 (splitting the route builders) was not applied. It was not needed to reach the
  target.

## Result (`cargo zigbuild --release --locked -p ostro --target x86_64-unknown-linux-musl`, as in ci-local `check_musl_size`)

| | bytes | headroom to 60 MB |
|---|---:|---:|
| before (main `73e53a7`) | 59,093,232 | 0.91 MB |
| after | 56,023,344 | **3.98 MB** |

The saving is 3.07 MB, almost all of it in `.text` (-3.03 MB).

## Verification

- `cargo test --release -p ostro-api -p ostro-cli -p ostro-ui`: 306 passed and 0 failed,
  across 25 test binaries. This includes the OpenAPI document and route-list tests.
- `e2e_dev` with `OSTRO_BIN` set to the musl release binary: 3/3 passed.
- `dev_startup` with `--include-ignored` (including `cold_first_run_measurement`), old binary
  and new binary run back to back: the cold first run had a best time of 1559 ms before and
  1554 ms after (median 1583 vs 1585 ms). `--ephemeral` took 91 ms before and 94 ms after.
  One earlier pair of runs differed by about 1.2 s. The machine load was rising then (6 to 10
  with other agents building), and the rerun showed no difference.

## Open

- Option 2 (route builders, ~0.5–1 MB) and option 3 (feature-gate kube/sqlx) are still
  available if the binary grows again.
