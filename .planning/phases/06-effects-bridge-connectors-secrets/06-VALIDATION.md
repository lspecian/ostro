---
phase: 6
slug: effects-bridge-connectors-secrets
status: complete
nyquist_compliant: true
wave_0_complete: true
created: 2026-11-19
---

# Phase 6 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Source: 06-RESEARCH.md § Validation Architecture, mapped onto plans 06-01..06-20. Adopted decisions: 06-DECISIONS.md (D-FX1..D-FX9, P-1..P-21).

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | `cargo test` (libtest), plus proptest 1.12, insta 1 and `fail` 0.5.1 failpoints (via `bridge_fail!`, P-13). Spawned-binary tests use `CARGO_BIN_EXE_ostro` under `--features failpoints`. Mock HTTP, Vault and K8s servers use axum. The canary harness lives in `ostro-testkit` (feature `canary`). |
| **Config file** | None beyond Cargo.toml. `scripts/ci-local.sh` `FAILPOINT_CRATES` must equal the ci.yml "Failpoint suites" step (06-02 adds ostro-bridge). |
| **External services** | PG 14 runs locally (`OSTRO_TEST_PG_URL`, mandatory, from the untracked `.ostro-test-env`). MySQL 8.4 (`OSTRO_TEST_MYSQL_URL`) and SeaweedFS (`OSTRO_TEST_S3_*`) are env-gated: loud SKIP locally, CI service containers (06-18). Vault/K8s use mocks (a real Vault is optional). |
| **Guest build** | `cargo build --release --target wasm32-wasip2 --manifest-path guests/Cargo.toml --target-dir target/guests` (fx-http, fx-sql, fx-object, fx-canary, fx-ssrf from 06-09) |
| **Quick run command** | `cargo test -p ostro-bridge --locked` (unit, no DB) |
| **Full suite command** | `bash scripts/ci-local.sh --no-musl` |
| **Phase gate commands** | 1. `bash scripts/ci-local.sh` (with musl).<br>2. `cargo test -p ostro --features failpoints --test fx_redrive --test fx_sql_crash --locked -- --test-threads=1`.<br>3. `cargo test -p ostro-server --test fx_canary --test fx_ssrf_e2e --test fx_object_e2e --test fx_providers_e2e --locked -- --test-threads=1`.<br>4. `cargo test -p ostro-engine --test replay_corpus --locked`.<br>5. `set -o pipefail; cargo deny check && cargo tree --workspace -e normal --locked > target/tree.txt && ! grep -q 'chrono v' target/tree.txt` (no exit code masked by a pipe; W8). |
| **Estimated runtime** | Quick: < 30 s. Full ci-local: Phase 5 baseline plus the kube/k8s-openapi/sqlx compile time (measured in 06-02). The e2e crash tests take a few minutes each. |

---

## Sampling Rate

- **After every task commit:** `cargo test -p <crate touched> --locked` plus `cargo clippy -p <crate> --all-targets --all-features --locked -- -D warnings`. Bridge tasks with failpoint sites also run `cargo test -p ostro-bridge --features failpoints --locked -- --test-threads=1`.
- **After every plan wave:** `cargo test --workspace --locked` with `OSTRO_TEST_PG_URL` set, plus `cargo test -p ostro-engine --test replay_corpus --locked`.
- **Before `/gsd:verify-work`:** the 06-20 gate.
- **Max feedback latency:** 60 s for task-level checks (the DB and crash e2e tests are wave-level).
- **Piped verify commands** use `set -o pipefail`.

---

## Wave Structure

| Wave | Plans | Exclusive shared files |
|------|-------|------------------------|
| 1 | 06-01 gate + decisions + docs | Phase 5 complete (all 05-*-SUMMARY, 05-VERIFICATION passed or by-design human_needed, WIT freeze in place). Edits only STATE.md, REQUIREMENTS.md and RESERVATIONS.md (names + canonical InvalidCode placeholder names). |
| 2 | 06-02 deps + skeletons + format contract | **Only plan editing** Cargo.toml, Cargo.lock, member dependency tables, ostro-bridge `lib.rs`, the ostro-objstore skeleton, command.rs (CompleteEffect = encoded event payload, B2), InvalidCode, ci-local/ci.yml FAILPOINT_CRATES. |
| 3 | 06-03 partition apply, 06-04 egress, 06-05 secrets core, 06-06 engine seam, 06-07 driver core + rate limits, 06-08 API surface, 06-09 SDK + guests | Disjoint: ostro-partition (+ docs/storage-format.md) / bridge `egress/*` / bridge `secrets/*` (except `providers/{mod,cache,vault,k8s}`) / ostro-engine (host/state.rs flush site) / bridge `{driver,registry,ratelimit,outcome,config}.rs` / ostro-proto + ostro-api / sdks/rust + guests/fx-* |
| 4 | 06-10 HTTP, 06-11 SQL, 06-12 object + ostro-objstore, 06-13 Vault/K8s | Disjoint: bridge `http/*` + testkit `mock_provider.rs` / bridge `sql/*` + testkit `db.rs` / `crates/ostro-objstore/**` + bridge `object/*` / bridge `providers/{cache,vault,k8s}.rs` |
| 5 | 06-14 server seam | ostro-server src (config, bridge_services incl. PresignKeyStore, meta_view, effects, sink incl. the B1 unsupported_input change, derive, server, lib, control/mod) + tests/{config,sink}.rs + tests/common/mod.rs |
| 6 | 06-15 seam e2e, 06-16 admin impl + CLI | Disjoint: tests/effects_seam.rs / control/bridge.rs, control/mod.rs, tests/bridge_admin.rs, tests/common/{fx,mod}.rs, ostro-cli, crates/ostro/tests/cli_bridge.rs |
| 7 | 06-17 e2e HTTP/SSRF/rate/re-drive, 06-18 e2e SQL crash + CI services, 06-19 e2e canary/object/providers | Disjoint test files. **06-18 alone** edits ci-local.sh, ci.yml and .gitignore. **06-19 alone** edits testkit `canary.rs`. |
| 8 | 06-20 phase gate | docs/*, tests/replay-corpus/, RESERVATIONS.md flip, 06-VALIDATION.md |

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Task | Automated Command | Status |
|---------|------|------|-------------|------|-------------------|--------|
| 6-01-01 | 01 | 1 | all | Phase 5 completion gate, baseline and reservations check | `bash -c 'set -euo pipefail; ls .planning/phases/05-workflow-semantics-core-hardening/05-{01..20}-SUMMARY.md >/dev/null; grep -Eq "^status: (passed\|human_needed)$" .planning/phases/05-workflow-semantics-core-hardening/05-VERIFI…` | ✅ green |
| 6-01-02 | 01 | 1 | all | Decisions to STATE, FX-05 wording, RESERVATIONS names + placeholder ru | `bash -c 'set -euo pipefail; grep -q "Phase 06 decisions (adopted defaults; user may override)" .planning/STATE.md; grep -q "D-FX6" .planning/STATE.md; grep -q "ostro-objstore. crate: S3-compatible" .planning/REQUIREMENTS.md; gr…` | ✅ green |
| 6-02-01 | 02 | 2 | all | Dependencies, ostro-bridge contract skeleton, ostro-objstore skeleton, | `bash -c 'set -euo pipefail; cargo build --workspace --all-targets --locked; cargo build -p ostro-bridge --no-default-features --locked; cargo test -p ostro-bridge --lib --locked; cargo clippy -p ostro-bridge -p ostro-objstore -…` | ✅ green |
| 6-02-02 | 02 | 2 | all | ostro-types/partition contract (CompleteEffect, RegisterEffect/CancelE | `bash -c 'set -euo pipefail; cargo test -p ostro-types --locked; cargo test -p ostro-types --test golden_frozen --locked; git diff "$(cat target/06-02-base.sha)" -- crates/ostro-types/tests/golden/commands.txt > target/06-02-gol…` | ✅ green |
| 6-03-01 | 03 | 3 | FX-01, FX-03 | Effects rows, RegisterEffect/CancelEffect intents and CompleteEffect a | `cargo test -p ostro-partition --test apply_effects --locked` | ✅ green |
| 6-03-02 | 03 | 3 | FX-01, FX-03 | Terminal/retention cleanup (P-8) and LeaderAnnounced wake (P-9) | `bash -c 'set -euo pipefail; cargo test -p ostro-partition --test apply_effects --locked; cargo test -p ostro-partition --test format_corpus --locked'` | ✅ green |
| 6-03-03 | 03 | 3 | FX-01, FX-03 | rebuild_digest generator + storage-format documentation | `bash -c 'set -euo pipefail; cargo test -p ostro-partition --test rebuild_digest --locked; cargo test -p ostro-partition --features failpoints --locked -- --test-threads=1; cargo clippy -p ostro-partition --all-targets --locked …` | ✅ green |
| 6-04-01 | 04 | 3 | FX-08 | Classifier tables and unwrap (classify.rs) | `cargo test -p ostro-bridge --lib egress::classify --locked` | ✅ green |
| 6-04-02 | 04 | 3 | FX-08 | URL gate, allow-list rules, resolver (url_gate.rs, policy.rs, resolver | `bash -c 'set -euo pipefail; cargo test -p ostro-bridge --test ssrf_table --locked; cargo test -p ostro-bridge --lib egress --locked'` | ✅ green |
| 6-04-03 | 04 | 3 | FX-08 | Egress client builder, operator client, capped reader (client.rs) | `bash -c 'set -euo pipefail; cargo test -p ostro-bridge --test egress_client --locked -- --test-threads=1; cargo clippy -p ostro-bridge --all-targets --locked -- -D warnings'` | ✅ green |
| 6-05-01 | 05 | 3 | FX-06, FX-07, FX-10 | SecretValue, template parser/expander, grants model (value.rs, templat | `cargo test -p ostro-bridge --lib --locked -- secrets::template secrets::grants secrets::value` | ✅ green |
| 6-05-02 | 05 | 3 | FX-06, FX-07, FX-10 | Scrubber (scrub.rs) and sealed store (sealed.rs) | `cargo test -p ostro-bridge --lib --locked -- secrets::scrub secrets::sealed` | ✅ green |
| 6-05-03 | 05 | 3 | FX-06, FX-07, FX-10 | env/file/sealed providers, chain, SecretResolver | `bash -c 'set -euo pipefail; cargo test -p ostro-bridge --test secrets --locked -- --test-threads=1; cargo test -p ostro-bridge --lib secrets --locked; cargo clippy -p ostro-bridge --all-targets --locked -- -D warnings'` | ✅ green |
| 6-06-01 | 06 | 3 | FX-01, FX-03, FX-06 | EffectDriver trait, sink default method, LiveEffect capture with redac | `bash -c 'set -euo pipefail; cargo test -p ostro-engine --test effects_driver --locked; cargo build --workspace --all-targets --locked'` | ✅ green |
| 6-06-02 | 06 | 3 | FX-01, FX-03, FX-06 | Start after commit (live) and re-drive on replay | `bash -c 'set -euo pipefail; cargo test -p ostro-engine --test effects_driver --locked; cargo test -p ostro-engine --locked; cargo test -p ostro-engine --test replay_corpus --locked; cargo clippy -p ostro-engine --all-targets --…` | ✅ green |
| 6-07-01 | 07 | 3 | FX-09, FX-01, FX-03 | Outcome helpers, config, rate limiter (outcome.rs, config.rs, ratelimi | `bash -c 'set -euo pipefail; cargo test -p ostro-bridge --test ratelimit --locked; cargo test -p ostro-bridge --lib --locked -- outcome ratelimit config'` | ✅ green |
| 6-07-02 | 07 | 3 | FX-09, FX-01, FX-03 | Bridge facade and EffectRegistry (driver.rs, registry.rs) | `bash -c 'set -euo pipefail; cargo test -p ostro-bridge --test driver --locked; cargo test -p ostro-bridge --features failpoints --test driver --locked -- --test-threads=1; cargo clippy -p ostro-bridge --all-targets --all-featur…` | ✅ green |
| 6-08-01 | 08 | 3 | FX-05, FX-06, FX-07, FX-08, FX-10 | bridge.proto + domain types + ControlPlane defaults + FakeControlPlane | `bash -c 'set -euo pipefail; cargo build --workspace --all-targets --locked; cargo test -p ostro-proto -p ostro-api --lib --locked'` | ✅ green |
| 6-08-02 | 08 | 3 | FX-05, FX-06, FX-07, FX-08, FX-10 | REST routes, gRPC services, presign route outside auth, parity + OpenA | `bash -c 'set -euo pipefail; cargo test -p ostro-api --locked; cargo clippy -p ostro-api -p ostro-proto --all-targets --locked -- -D warnings; cargo build --workspace --all-targets --locked'` | ✅ green |
| 6-09-01 | 09 | 3 | FX-02, FX-04, FX-05 | SDK effect builders + idempotency_key | `bash -c 'set -euo pipefail; cargo test --manifest-path sdks/rust/Cargo.toml; cargo clippy --manifest-path sdks/rust/Cargo.toml --target wasm32-wasip2 -- -D warnings; cargo clippy --manifest-path sdks/rust/Cargo.toml --all-targe…` | ✅ green |
| 6-09-02 | 09 | 3 | FX-02, FX-04, FX-05 | Five Phase 6 test guests | `bash -c 'set -euo pipefail; cargo build --release --target wasm32-wasip2 --manifest-path guests/Cargo.toml --target-dir target/guests; for g in fx_http fx_sql fx_object fx_canary fx_ssrf; do test -f target/guests/wasm32-wasip2/…` | ✅ green |
| 6-10-01 | 10 | 4 | FX-01, FX-02, FX-03, FX-07 | Key derivation, window decision, retry classification, target config ( | `cargo test -p ostro-bridge --lib http:: --locked` | ✅ green |
| 6-10-02 | 10 | 4 | FX-01, FX-02, FX-03, FX-07 | Mock provider (ostro-testkit, feature mock-http) | `cargo build -p ostro-testkit --features mock-http --locked && cargo clippy -p ostro-testkit --features mock-http --all-targets --locked -- -D warnings` | ✅ green |
| 6-10-03 | 10 | 4 | FX-01, FX-02, FX-03, FX-07 | HttpConnector send path (send.rs, mod.rs) + integration tests | `bash -c 'set -euo pipefail; cargo test -p ostro-bridge --test http_connector --locked -- --test-threads=1; cargo test -p ostro-bridge --features failpoints --test http_connector --locked -- --test-threads=1; cargo clippy -p ost…` | ✅ green |
| 6-11-01 | 11 | 4 | FX-04 | Statement gate and value mapping (classify.rs, value.rs, config.rs) | `cargo test -p ostro-bridge --lib sql:: --locked` | ✅ green |
| 6-11-02 | 11 | 4 | FX-04 | Pools, exactly-once protocol, error mapping, DDL, GC (pool.rs, exactly | `bash -c 'set -euo pipefail; test -n "${OSTRO_TEST_PG_URL:-}" \|\| { echo "export OSTRO_TEST_PG_URL (P-17) first"; exit 1; }; cargo test -p ostro-bridge --test sql_pg --locked -- --test-threads=1; cargo test -p ostro-bridge --fe…` | ✅ green (PG legs ran; MySQL leg ⚠ human_needed) |
| 6-12-01 | 12 | 4 | FX-05 | ostro-objstore backends (LocalFs + S3Store) and presign | `bash -c 'set -euo pipefail; cargo test -p ostro-objstore --locked -- --test-threads=1; cargo clippy -p ostro-objstore --all-targets --locked -- -D warnings'` | ✅ green (S3 leg ⚠ human_needed) |
| 6-12-02 | 12 | 4 | FX-05 | Bridge object connector + local HMAC presign | `bash -c 'set -euo pipefail; cargo test -p ostro-bridge --test object_local --locked; cargo test -p ostro-bridge --test object_s3 --locked; cargo clippy -p ostro-bridge --all-targets --all-features --locked -- -D warnings'` | ✅ green (object_s3 ⚠ human_needed) |
| 6-13-01 | 13 | 4 | FX-10 | TtlCache + VaultProvider (cache.rs, vault.rs) with a mock Vault | `bash -c 'set -euo pipefail; cargo test -p ostro-bridge --test providers_vault --locked; cargo test -p ostro-bridge --lib secrets::providers::cache --locked'` | ✅ green |
| 6-13-02 | 13 | 4 | FX-10 | K8sProvider (k8s.rs) with a mock API server | `bash -c 'set -euo pipefail; cargo test -p ostro-bridge --test providers_k8s --locked; cargo build -p ostro-bridge --no-default-features --locked; cargo clippy -p ostro-bridge --all-targets --all-features --locked -- -D warnings'` | ✅ green |
| 6-14-01 | 14 | 5 | FX-01, FX-02, FX-03, FX-04, FX-05, FX-06, FX-08 | Config sections, MetaHandleView, BridgeServices construction | `bash -c 'set -euo pipefail; cargo test -p ostro-server --test config --locked; cargo test -p ostro-server --lib --locked -- bridge_services; cargo build --workspace --all-targets --locked'` | ✅ green |
| 6-14-02 | 14 | 5 | FX-01, FX-02, FX-03, FX-04, FX-05, FX-06, FX-08 | Effect seam: driver adapter, CompleteEffect sink, SendGate, derive map | `bash -c 'set -euo pipefail; cargo test -p ostro-server --lib --locked; cargo test -p ostro-server --test sink --locked; cargo build --workspace --all-targets --locked'` | ✅ green |
| 6-15-01 | 15 | 6 | FX-01, FX-03, FX-06 | Intent before I/O, journal delivery, 1 MiB blob outcome, children regr | `bash -c 'set -euo pipefail; cargo test -p ostro-server --test effects_seam --locked -- --test-threads=1; cargo test -p ostro-server --features failpoints --test effects_seam --locked -- --test-threads=1'` | ✅ green |
| 6-15-02 | 15 | 6 | FX-01, FX-03, FX-06 | B1 crash re-drive (no unsupported repair) and crash wake of an evicted | `bash -c 'set -euo pipefail; cargo test -p ostro-server --features failpoints --test effects_seam --locked -- --test-threads=1 --nocapture; cargo clippy -p ostro-server --all-targets --all-features --locked -- -D warnings'` | ✅ green |
| 6-16-01 | 16 | 6 | FX-05, FX-06, FX-07, FX-08, FX-10 | Server ControlPlane bridge methods + presign route impl | `bash -c 'set -euo pipefail; cargo test -p ostro-server --test bridge_admin --locked -- --test-threads=1; cargo clippy -p ostro-server --all-targets --locked -- -D warnings'` | ✅ green |
| 6-16-02 | 16 | 6 | FX-05, FX-06, FX-07, FX-08, FX-10 | CLI verbs + API-based e2e helpers | `bash -c 'set -euo pipefail; cargo test -p ostro --test cli_bridge --locked -- --test-threads=1; cargo test -p ostro-cli --locked; cargo build -p ostro-server --tests --locked; cargo clippy -p ostro-cli -p ostro --all-targets --…` | ✅ green |
| 6-17-01 | 17 | 7 | FX-01, FX-02, FX-03, FX-08, FX-09 | SC1 in-process (fx_http_e2e.rs) + kill -9 re-drive with the failpoint  | `bash -c 'set -euo pipefail; cargo test -p ostro-server --test fx_http_e2e --locked -- --test-threads=1; cargo test -p ostro-server --features failpoints --test fx_http_e2e --locked -- --test-threads=1; cargo test -p ostro --fea…` | ✅ green |
| 6-17-02 | 17 | 7 | FX-01, FX-02, FX-03, FX-08, FX-09 | SC4 through a guest: SSRF table, env proxy, allow-lists (fx_ssrf_e2e.r | `bash -c 'set -euo pipefail; cargo test -p ostro-server --test fx_ssrf_e2e --locked -- --test-threads=1; cargo test -p ostro-server --test fx_rate_e2e --locked -- --test-threads=1; cargo clippy -p ostro-server -p ostro --all-tar…` | ✅ green |
| 6-18-01 | 18 | 7 | FX-04 | SC2 crash test on PG (+ MySQL when available) | `bash -c 'set -euo pipefail; test -n "${OSTRO_TEST_PG_URL:-}" \|\| { echo "export OSTRO_TEST_PG_URL (06-11 SUMMARY form)"; exit 1; }; cargo test -p ostro --features failpoints --test fx_sql_crash --locked -- --test-threads=1 --n…` | ✅ green (PG ran; MySQL ⚠ human_needed) |
| 6-18-02 | 18 | 7 | FX-04 | CI service containers + ci-local env gating | `bash -c 'set -euo pipefail; grep -q "mysql:8.4" .github/workflows/ci.yml; grep -q "postgres:17" .github/workflows/ci.yml; grep -q "seaweedfs" .github/workflows/ci.yml; grep -q "OSTRO_TEST_MYSQL_URL" scripts/ci-local.sh; for c in $(…` | ✅ green |
| 6-19-01 | 19 | 7 | FX-05, FX-06, FX-07, FX-10 | Canary harness (ostro-testkit, feature canary) | `bash -c 'set -euo pipefail; cargo test -p ostro-testkit --features canary --locked; cargo clippy -p ostro-testkit --all-features --all-targets --locked -- -D warnings'` | ✅ green |
| 6-19-02 | 19 | 7 | FX-05, FX-06, FX-07, FX-10 | SC3 end to end (fx_canary.rs) | `bash -c 'set -euo pipefail; cargo test -p ostro-server --test fx_canary --locked -- --test-threads=1'` | ✅ green |
| 6-19-03 | 19 | 7 | FX-05, FX-06, FX-07, FX-10 | SC5 end to end: object store via guest (fx_object_e2e.rs) and provider | `bash -c 'set -euo pipefail; cargo test -p ostro-server --test fx_object_e2e --locked -- --test-threads=1; cargo test -p ostro-server --test fx_providers_e2e --locked -- --test-threads=1; cargo clippy -p ostro-server --all-targe…` | ✅ green (S3 leg ⚠ human_needed) |
| 6-20-01 | 20 | 8 | all | Documentation + docs/03 and docs/02 corrections | `bash -c 'set -euo pipefail; grep -q effect_outcome_unknown docs/connectors.md; grep -q "fd00:ec2::254" docs/security-egress.md; grep -q "__ostro/" docs/secrets.md; grep -q ostro-objstore docs/03-architecture.md; grep -qi "azure…` | ✅ green |
| 6-20-02 | 20 | 8 | all | Replay-corpus entries + reservations flip + freeze/deny re-check | `bash -c 'set -euo pipefail; ls tests/replay-corpus \| grep -q "fx-"; cargo test -p ostro-engine --test replay_corpus --locked; cargo test -p ostro-witcheck --locked; cargo test -p ostro-types --test golden_frozen --locked; carg…` | ✅ green |
| 6-20-03 | 20 | 8 | all | Full ci-local gate + 06-VALIDATION.md | `bash -c 'set -euo pipefail; bash scripts/ci-local.sh; grep -q "Gate results" .planning/phases/06-effects-bridge-connectors-secrets/06-VALIDATION.md; grep -q "Human-needed" .planning/phases/06-effects-bridge-connectors-secrets/0…` | ✅ green |

MySQL/S3 legs inside these commands SKIP loudly when the env var is unset (⚠ human_needed). The SC3 OTLP leg is deferred to Phase 8 (D-FX6).

*Status: ⬜ pending · ✅ green · ❌ red · ⚠ flaky / human_needed*

---

## Wave 0 Requirements

Test files are created by the task that first needs them (TDD tasks write tests first).

- [x] **ostro-bridge tests**: `ssrf_table`, `egress_client`, `secrets`, `driver`, `ratelimit`, `http_connector`, `sql_pg`, `sql_mysql`, `object_local`, `object_s3`, `providers_vault`, `providers_k8s`
- [x] **ostro-objstore tests**: `localfs`, `presign`, `s3` (unless Phase 8 already created them; 06-12 then adds only presign cases)
- [x] **ostro-partition**: `tests/apply_effects.rs`, `tests/common/gen_effects.rs`
- [x] **ostro-engine**: `tests/effects_driver.rs`
- [x] **ostro-api**: `tests/bridge_surface.rs`
- [x] **ostro-server**: `tests/{effects_seam, bridge_admin, fx_http_e2e, fx_ssrf_e2e, fx_rate_e2e, fx_canary, fx_object_e2e, fx_providers_e2e}.rs`, `tests/common/fx.rs`
- [x] **ostro binary**: `tests/{cli_bridge, fx_redrive, fx_sql_crash}.rs`
- [x] **ostro-testkit** (features): `mock_provider.rs` (06-10), `db.rs` (06-11), `canary.rs` (06-19)
- [x] **Guests**: fx-http, fx-sql, fx-object, fx-canary, fx-ssrf (06-09)
- [x] **Failpoint sites**: `bridge::http::before_send`, `bridge::http::after_response`, `bridge::sql::before_user_stmt`, `bridge::sql::after_commit`, `bridge::object::before_put`, `bridge::effect::before_complete_submit`
- [x] **CI**: ci.yml services postgres:17 and mysql:8.4, SeaweedFS via `docker run`, env exports (06-18)

---

## Manual-Only / Human-Needed Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| SC2 on MySQL 8.4 (exactly one row per run after kill -9) and the sql_mysql suite | FX-04 | No mysqld and no Docker in WSL (D-FX5) | After the first push the CI service container runs it. Locally: install mysql-server 8.4, then `OSTRO_TEST_MYSQL_URL='mysql://root:…@127.0.0.1:3306/ostro?ssl-mode=REQUIRED' cargo test -p ostro --features failpoints --test fx_sql_crash -- --test-threads=1` and `cargo test -p ostro-bridge --test sql_mysql`. |
| Object store on S3-compatible storage (SeaweedFS) incl. presign | FX-05 | No Docker in WSL | CI SeaweedFS step. Locally: `OSTRO_TEST_S3_URL=http://127.0.0.1:8333 OSTRO_TEST_S3_ACCESS_KEY=… OSTRO_TEST_S3_SECRET_KEY=… OSTRO_TEST_S3_BUCKET=… cargo test -p ostro-objstore -p ostro-bridge --test object_s3 && cargo test -p ostro-server --test fx_object_e2e`. |
| GCS through S3 interoperability | FX-05 | Needs a GCS bucket and HMAC keys (MEDIUM confidence) | Point the `OSTRO_TEST_S3_*` vars at `https://storage.googleapis.com`, region `auto`, and run the object suites. |
| Canary absent from OTLP export | FX-06 (SC3) | OTLP export lands in Phase 8 (D-FX6) | Phase 8 registers an OTLP mock collector with `ostro_testkit::canary::register_sink` and re-runs `fx_canary`. |
| Remote CI legs (aarch64-musl, macOS) | all | No git remote yet (FOUND-05) | Confirm green after the first push. |
| Review of the FX-05 wording/Azure deferral, D-FX2 (native roots) and D-FX6 | FX-04, FX-05, FX-06 | User unavailable (YOLO) | Read 06-DECISIONS.md. The STATE.md Pending Todos list each item. |
| Real Vault dev server (optional) | FX-10 | The mock covers the suite | `OSTRO_TEST_VAULT_ADDR=… OSTRO_TEST_VAULT_TOKEN=… cargo test -p ostro-bridge --test providers_vault`. |

---

## Gate results

Recorded by 06-20 on 2026-11-20. The full ci-local run was shared with the 08-22 gate and ran once on the final code. Everything committed after `5faa97c` is docs/planning only.

### ci-local (full, with musl)

| Step | Result | Time |
|------|--------|------|
| toolchain 1.100.0 | PASS | 0 s |
| cargo fmt --check | PASS | 2 s |
| cargo clippy -D warnings | PASS | 55 s |
| cargo build (warning-free) | PASS | 86 s |
| sdk workspace fmt + clippy | PASS | 21 s |
| sdk workspace test + wasm build | PASS | 23 s |
| guests build (wasm32-wasip2) | PASS | 12 s |
| order-saga example | PASS | 21 s |
| external: OSTRO_TEST_MYSQL_URL | SKIP (human_needed) | - |
| external: OSTRO_TEST_S3_URL | SKIP (human_needed) | - |
| cargo test | PASS (1,959 passed) | 2164 s |
| semantics sleepers (serial) | PASS | 39 s |
| failpoint list sync (ci.yml) | PASS | 0 s |
| failpoint suites | PASS (956 passed) | 2093 s |
| server campaign smoke (20 kills) | PASS | 413 s |
| cargo deny check | PASS | 1 s |
| check-vendored.sh | PASS | 1 s |
| check-deny-fixtures.sh | PASS | 1 s |
| no chronik/tokio-uring/prometheus | PASS | 0 s |
| workflow validation | PASS | 2 s |
| zig + cargo-zigbuild present | PASS | 0 s |
| musl release (static, mimalloc) | PASS | 311 s |
| musl zigbuild test | PASS (1,959 passed) | 2146 s |

`all steps passed`, exit 0. Run on main `5faa97c` (worktree, fresh `CARGO_TARGET_DIR`), 2026-11-20 21:50–23:53 CET (about 2 h 03 min). `OSTRO_TEST_PG_URL` was set (banner: `set: OSTRO_TEST_PG_URL (ostro-bridge sql_pg, ostro fx_sql_crash pg_after_commit/pg_before_user_stmt)`), pointing at a private throwaway PostgreSQL 14.24 cluster (127.0.0.1:55468, scram, data dir in the session scratchpad, loaded via the untracked `.ostro-test-env`; nothing committed).

PG legs that really ran:
- `ostro-bridge --test sql_pg`: 10 passed in `cargo test` and 10 passed in `musl zigbuild test`; 13 passed in `failpoint suites` (with the three `failpoints::pg_*` cases).
- `ostro --features failpoints --test fx_sql_crash`: `pg_after_commit` and `pg_before_user_stmt` passed in `failpoint suites`. Re-run alone with `--nocapture`: `N=50 held in 452 ms, restart (readyz) 164 ms, all terminal 471 ms after kill -9` and `N=50 held in 533 ms, restart 170 ms, all terminal 470 ms`.
- The PG server recorded 1,344 committed transactions and 2,506 inserted tuples. The tests drop their own schemas.

The MySQL legs (`sql_mysql`, `fx_sql_crash mysql_*`) and the S3 legs passed as loud SKIPs.

Campaign smoke: `kills=20 lost_acked_runs=0 duplicate_journal_entries=0 non_terminal_runs=0 lost_acked_signals=0 wrong_results=0 other_violations=0`, acked_runs=231, RESULT: PASS.

The musl release binary (`x86_64-unknown-linux-musl/release/ostro`) is 44,033,456 bytes (42.0 MiB), static, mimalloc. No 06-02 size baseline was recorded, so this number is the baseline from now on.

No step failed and no test needed a re-run.

### Corpus, reservations, freeze, deny

| Check | Result |
|-------|--------|
| `cargo test -p ostro-engine --test replay_corpus` | 5 passed, including `replay_corpus_entries`. The five new `fx-*` entries (fx-http-pay, fx-http-redrive, fx-sql-exactly-once, fx-object-local, fx-canary-scrubbed) replay with no EffectDriver. Canary and DB-password scan of the exported files: 0 hits (ca8d1f3). |
| Reservations (06-01 script) | every Phase 6 ordinal in code matches. Cmd 13, Intent 9/10 and InvalidCode 15 are `exists (P6, 06-02)`; the keyspace #12 effects rows are `exists (P6, 06-03)`; unused spares stay `reserved P6` |
| WIT sha256 | `611d2982b88e0884d55f0acb8f47691edbcc4c4b2a8918efaad4a7092d6bd98e`, equal to `wit/FROZEN.toml` |
| golden_frozen (ostro-types) + ostro-witcheck | green (`golden_frozen_prefixes_unchanged`, `appended_lines_are_allowed`, `tampered_copies_are_rejected`); `commands.txt` vs the 06-02 base `d4d150d`: +3 / −0 |
| format corpus v0.1 (ostro-partition `format_corpus`) | green (`format_corpus_opens_rebuilds_and_matches_format_toml`) |
| `cargo deny check` | PASS (stock deny.toml; `git diff d4d150d..HEAD -- deny.toml` is empty) |
| normal tree | `cargo tree --workspace -e normal --locked`: no `chrono v`; no zstd-sys, openssl-sys or object_store |
| FAILPOINT_CRATES vs ci.yml | in sync (`failpoint list sync` PASS). It includes `ostro:fx_redrive` and `ostro:fx_sql_crash` (06-18) |

### Phase 6 success criteria (local evidence)

- **SC1 (HTTP with idempotency key, re-drive after kill -9):** `fx_http_e2e`, `fx_redrive` (4 passed in the failpoint suites).
- **SC2 (SQL exactly-once after kill -9):** `fx_sql_crash` PG legs (above). MySQL is human_needed.
- **SC3 (secret canary):** `fx_canary`. The OTLP leg ran in 08-21 (`otlp_canary`, clean) and passed again in this gate.
- **SC4 (SSRF):** `fx_ssrf_e2e`, `ssrf_table`.
- **SC5 (object store + providers):** `fx_object_e2e` (local leg), `fx_providers_e2e`. The S3 leg is human_needed.

## Human-needed

Each item below is ⚠ human_needed. None of them blocks the local gate.

a. **MySQL 8.4 legs (FX-04, SC2).** These run in the ci.yml `mysql:8.4` service container after the first push, or locally once mysqld 8.4 is available:
   `OSTRO_TEST_MYSQL_URL='mysql://root:<pw>@127.0.0.1:3306/ostro?ssl-mode=REQUIRED' cargo test -p ostro --features failpoints --test fx_sql_crash --locked -- --test-threads=1`
   and `cargo test -p ostro-bridge --test sql_mysql --locked -- --test-threads=1` (plus `--features failpoints`).
b. **SeaweedFS legs (FX-05).** These run in the ci.yml SeaweedFS docker-run step, or locally:
   `docker run -d -p 8333:8333 -e AWS_ACCESS_KEY_ID=ostro -e AWS_SECRET_ACCESS_KEY=ostroseaweed -e S3_BUCKET=ostro-test chrislusf/seaweedfs:4.54 mini -dir=/data`, then
   `OSTRO_TEST_S3_URL=http://127.0.0.1:8333 OSTRO_TEST_S3_ACCESS_KEY=ostro OSTRO_TEST_S3_SECRET_KEY=ostroseaweed OSTRO_TEST_S3_BUCKET=ostro-test cargo test -p ostro-objstore --locked -- --test-threads=1 && cargo test -p ostro-bridge --test object_s3 --locked && cargo test -p ostro-server --test fx_object_e2e --locked -- --test-threads=1`.
c. **GCS S3-interop.** Set the same `OSTRO_TEST_S3_*` variables to `https://storage.googleapis.com` with HMAC keys and region `auto`, then run the commands in (b).
d. **OTLP leg of the SC3 canary (D-FX6):** **done in 08-21.** `cargo test -p ostro-server --test otlp_canary --locked` printed `PHASE6_CANARY_LEG: run (fx-canary through the harness sink "otlp"): clean` in 08-21. In this gate's `cargo test` and `musl zigbuild test` it passed again (`otlp_export_never_carries_the_canary`, 1 passed). Nothing is pending.
e. **Remote CI legs (aarch64-musl, macOS, linux-x86_64-gnu with the DB/S3 services).** Confirm they are green after the first push (FOUND-05).
f. **User review** of the FX-05 wording and the Azure deferral (D-FX1), D-FX2 (native TLS roots plus `ca_file`) and D-FX6. See 06-DECISIONS.md; STATE.md Pending Todos lists each item.
g. **Optional real Vault dev server:** `vault server -dev`, then `OSTRO_TEST_VAULT_ADDR=http://127.0.0.1:8200 OSTRO_TEST_VAULT_TOKEN=<root token> cargo test -p ostro-bridge --test providers_vault --locked`. The mock already covers the suite.

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers all MISSING references
- [x] No watch-mode flags
- [x] Feedback latency < 60 s for task-level checks
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** gate evidence recorded by 06-20 (2026-11-20); phase verification pending (`/gsd:verify-work`)
