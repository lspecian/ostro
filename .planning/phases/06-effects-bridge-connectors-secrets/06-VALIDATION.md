---
phase: 6
slug: effects-bridge-connectors-secrets
status: draft
nyquist_compliant: false
wave_0_complete: false
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
| **External services** | PG 14 runs locally (`OSTRO_TEST_PG_URL`, mandatory, from the untracked `.ostro-test-env`). MySQL 8.4 (`OSTRO_TEST_MYSQL_URL`) and MinIO (`OSTRO_TEST_S3_*`) are env-gated: loud SKIP locally, CI service containers (06-18). Vault/K8s use mocks (a real Vault is optional). |
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
| 6-01-01 | 01 | 1 | all | Phase 5 completion gate, baseline and reservations check | `bash -c 'set -euo pipefail; ls .planning/phases/05-workflow-semantics-core-hardening/05-{01..20}-SUMMARY.md >/dev/null; grep -Eq "^status: (passed\|human_needed)$" .planning/phases/05-workflow-semantics-core-hardening/05-VERIFI…` | ⬜ pending |
| 6-01-02 | 01 | 1 | all | Decisions to STATE, FX-05 wording, RESERVATIONS names + placeholder ru | `bash -c 'set -euo pipefail; grep -q "Phase 06 decisions (adopted defaults; user may override)" .planning/STATE.md; grep -q "D-FX6" .planning/STATE.md; grep -q "ostro-objstore. crate: S3-compatible" .planning/REQUIREMENTS.md; gr…` | ⬜ pending |
| 6-02-01 | 02 | 2 | all | Dependencies, ostro-bridge contract skeleton, ostro-objstore skeleton, | `bash -c 'set -euo pipefail; cargo build --workspace --all-targets --locked; cargo build -p ostro-bridge --no-default-features --locked; cargo test -p ostro-bridge --lib --locked; cargo clippy -p ostro-bridge -p ostro-objstore -…` | ⬜ pending |
| 6-02-02 | 02 | 2 | all | ostro-types/partition contract (CompleteEffect, RegisterEffect/CancelE | `bash -c 'set -euo pipefail; cargo test -p ostro-types --locked; cargo test -p ostro-types --test golden_frozen --locked; git diff "$(cat target/06-02-base.sha)" -- crates/ostro-types/tests/golden/commands.txt > target/06-02-gol…` | ⬜ pending |
| 6-03-01 | 03 | 3 | FX-01, FX-03 | Effects rows, RegisterEffect/CancelEffect intents and CompleteEffect a | `cargo test -p ostro-partition --test apply_effects --locked` | ⬜ pending |
| 6-03-02 | 03 | 3 | FX-01, FX-03 | Terminal/retention cleanup (P-8) and LeaderAnnounced wake (P-9) | `bash -c 'set -euo pipefail; cargo test -p ostro-partition --test apply_effects --locked; cargo test -p ostro-partition --test format_corpus --locked'` | ⬜ pending |
| 6-03-03 | 03 | 3 | FX-01, FX-03 | rebuild_digest generator + storage-format documentation | `bash -c 'set -euo pipefail; cargo test -p ostro-partition --test rebuild_digest --locked; cargo test -p ostro-partition --features failpoints --locked -- --test-threads=1; cargo clippy -p ostro-partition --all-targets --locked …` | ⬜ pending |
| 6-04-01 | 04 | 3 | FX-08 | Classifier tables and unwrap (classify.rs) | `cargo test -p ostro-bridge --lib egress::classify --locked` | ⬜ pending |
| 6-04-02 | 04 | 3 | FX-08 | URL gate, allow-list rules, resolver (url_gate.rs, policy.rs, resolver | `bash -c 'set -euo pipefail; cargo test -p ostro-bridge --test ssrf_table --locked; cargo test -p ostro-bridge --lib egress --locked'` | ⬜ pending |
| 6-04-03 | 04 | 3 | FX-08 | Egress client builder, operator client, capped reader (client.rs) | `bash -c 'set -euo pipefail; cargo test -p ostro-bridge --test egress_client --locked -- --test-threads=1; cargo clippy -p ostro-bridge --all-targets --locked -- -D warnings'` | ⬜ pending |
| 6-05-01 | 05 | 3 | FX-06, FX-07, FX-10 | SecretValue, template parser/expander, grants model (value.rs, templat | `cargo test -p ostro-bridge --lib --locked -- secrets::template secrets::grants secrets::value` | ⬜ pending |
| 6-05-02 | 05 | 3 | FX-06, FX-07, FX-10 | Scrubber (scrub.rs) and sealed store (sealed.rs) | `cargo test -p ostro-bridge --lib --locked -- secrets::scrub secrets::sealed` | ⬜ pending |
| 6-05-03 | 05 | 3 | FX-06, FX-07, FX-10 | env/file/sealed providers, chain, SecretResolver | `bash -c 'set -euo pipefail; cargo test -p ostro-bridge --test secrets --locked -- --test-threads=1; cargo test -p ostro-bridge --lib secrets --locked; cargo clippy -p ostro-bridge --all-targets --locked -- -D warnings'` | ⬜ pending |
| 6-06-01 | 06 | 3 | FX-01, FX-03, FX-06 | EffectDriver trait, sink default method, LiveEffect capture with redac | `bash -c 'set -euo pipefail; cargo test -p ostro-engine --test effects_driver --locked; cargo build --workspace --all-targets --locked'` | ⬜ pending |
| 6-06-02 | 06 | 3 | FX-01, FX-03, FX-06 | Start after commit (live) and re-drive on replay | `bash -c 'set -euo pipefail; cargo test -p ostro-engine --test effects_driver --locked; cargo test -p ostro-engine --locked; cargo test -p ostro-engine --test replay_corpus --locked; cargo clippy -p ostro-engine --all-targets --…` | ⬜ pending |
| 6-07-01 | 07 | 3 | FX-09, FX-01, FX-03 | Outcome helpers, config, rate limiter (outcome.rs, config.rs, ratelimi | `bash -c 'set -euo pipefail; cargo test -p ostro-bridge --test ratelimit --locked; cargo test -p ostro-bridge --lib --locked -- outcome ratelimit config'` | ⬜ pending |
| 6-07-02 | 07 | 3 | FX-09, FX-01, FX-03 | Bridge facade and EffectRegistry (driver.rs, registry.rs) | `bash -c 'set -euo pipefail; cargo test -p ostro-bridge --test driver --locked; cargo test -p ostro-bridge --features failpoints --test driver --locked -- --test-threads=1; cargo clippy -p ostro-bridge --all-targets --all-featur…` | ⬜ pending |
| 6-08-01 | 08 | 3 | FX-05, FX-06, FX-07, FX-08, FX-10 | bridge.proto + domain types + ControlPlane defaults + FakeControlPlane | `bash -c 'set -euo pipefail; cargo build --workspace --all-targets --locked; cargo test -p ostro-proto -p ostro-api --lib --locked'` | ⬜ pending |
| 6-08-02 | 08 | 3 | FX-05, FX-06, FX-07, FX-08, FX-10 | REST routes, gRPC services, presign route outside auth, parity + OpenA | `bash -c 'set -euo pipefail; cargo test -p ostro-api --locked; cargo clippy -p ostro-api -p ostro-proto --all-targets --locked -- -D warnings; cargo build --workspace --all-targets --locked'` | ⬜ pending |
| 6-09-01 | 09 | 3 | FX-02, FX-04, FX-05 | SDK effect builders + idempotency_key | `bash -c 'set -euo pipefail; cargo test --manifest-path sdks/rust/Cargo.toml; cargo clippy --manifest-path sdks/rust/Cargo.toml --target wasm32-wasip2 -- -D warnings; cargo clippy --manifest-path sdks/rust/Cargo.toml --all-targe…` | ⬜ pending |
| 6-09-02 | 09 | 3 | FX-02, FX-04, FX-05 | Five Phase 6 test guests | `bash -c 'set -euo pipefail; cargo build --release --target wasm32-wasip2 --manifest-path guests/Cargo.toml --target-dir target/guests; for g in fx_http fx_sql fx_object fx_canary fx_ssrf; do test -f target/guests/wasm32-wasip2/…` | ⬜ pending |
| 6-10-01 | 10 | 4 | FX-01, FX-02, FX-03, FX-07 | Key derivation, window decision, retry classification, target config ( | `cargo test -p ostro-bridge --lib http:: --locked` | ⬜ pending |
| 6-10-02 | 10 | 4 | FX-01, FX-02, FX-03, FX-07 | Mock provider (ostro-testkit, feature mock-http) | `cargo build -p ostro-testkit --features mock-http --locked && cargo clippy -p ostro-testkit --features mock-http --all-targets --locked -- -D warnings` | ⬜ pending |
| 6-10-03 | 10 | 4 | FX-01, FX-02, FX-03, FX-07 | HttpConnector send path (send.rs, mod.rs) + integration tests | `bash -c 'set -euo pipefail; cargo test -p ostro-bridge --test http_connector --locked -- --test-threads=1; cargo test -p ostro-bridge --features failpoints --test http_connector --locked -- --test-threads=1; cargo clippy -p ost…` | ⬜ pending |
| 6-11-01 | 11 | 4 | FX-04 | Statement gate and value mapping (classify.rs, value.rs, config.rs) | `cargo test -p ostro-bridge --lib sql:: --locked` | ⬜ pending |
| 6-11-02 | 11 | 4 | FX-04 | Pools, exactly-once protocol, error mapping, DDL, GC (pool.rs, exactly | `bash -c 'set -euo pipefail; test -n "${OSTRO_TEST_PG_URL:-}" \|\| { echo "export OSTRO_TEST_PG_URL (P-17) first"; exit 1; }; cargo test -p ostro-bridge --test sql_pg --locked -- --test-threads=1; cargo test -p ostro-bridge --fe…` | ⬜ pending |
| 6-12-01 | 12 | 4 | FX-05 | ostro-objstore backends (LocalFs + S3Store) and presign | `bash -c 'set -euo pipefail; cargo test -p ostro-objstore --locked -- --test-threads=1; cargo clippy -p ostro-objstore --all-targets --locked -- -D warnings'` | ⬜ pending |
| 6-12-02 | 12 | 4 | FX-05 | Bridge object connector + local HMAC presign | `bash -c 'set -euo pipefail; cargo test -p ostro-bridge --test object_local --locked; cargo test -p ostro-bridge --test object_s3 --locked; cargo clippy -p ostro-bridge --all-targets --all-features --locked -- -D warnings'` | ⬜ pending |
| 6-13-01 | 13 | 4 | FX-10 | TtlCache + VaultProvider (cache.rs, vault.rs) with a mock Vault | `bash -c 'set -euo pipefail; cargo test -p ostro-bridge --test providers_vault --locked; cargo test -p ostro-bridge --lib secrets::providers::cache --locked'` | ⬜ pending |
| 6-13-02 | 13 | 4 | FX-10 | K8sProvider (k8s.rs) with a mock API server | `bash -c 'set -euo pipefail; cargo test -p ostro-bridge --test providers_k8s --locked; cargo build -p ostro-bridge --no-default-features --locked; cargo clippy -p ostro-bridge --all-targets --all-features --locked -- -D warnings'` | ⬜ pending |
| 6-14-01 | 14 | 5 | FX-01, FX-02, FX-03, FX-04, FX-05, FX-06, FX-08 | Config sections, MetaHandleView, BridgeServices construction | `bash -c 'set -euo pipefail; cargo test -p ostro-server --test config --locked; cargo test -p ostro-server --lib --locked -- bridge_services; cargo build --workspace --all-targets --locked'` | ⬜ pending |
| 6-14-02 | 14 | 5 | FX-01, FX-02, FX-03, FX-04, FX-05, FX-06, FX-08 | Effect seam: driver adapter, CompleteEffect sink, SendGate, derive map | `bash -c 'set -euo pipefail; cargo test -p ostro-server --lib --locked; cargo test -p ostro-server --test sink --locked; cargo build --workspace --all-targets --locked'` | ⬜ pending |
| 6-15-01 | 15 | 6 | FX-01, FX-03, FX-06 | Intent before I/O, journal delivery, 1 MiB blob outcome, children regr | `bash -c 'set -euo pipefail; cargo test -p ostro-server --test effects_seam --locked -- --test-threads=1; cargo test -p ostro-server --features failpoints --test effects_seam --locked -- --test-threads=1'` | ⬜ pending |
| 6-15-02 | 15 | 6 | FX-01, FX-03, FX-06 | B1 crash re-drive (no unsupported repair) and crash wake of an evicted | `bash -c 'set -euo pipefail; cargo test -p ostro-server --features failpoints --test effects_seam --locked -- --test-threads=1 --nocapture; cargo clippy -p ostro-server --all-targets --all-features --locked -- -D warnings'` | ⬜ pending |
| 6-16-01 | 16 | 6 | FX-05, FX-06, FX-07, FX-08, FX-10 | Server ControlPlane bridge methods + presign route impl | `bash -c 'set -euo pipefail; cargo test -p ostro-server --test bridge_admin --locked -- --test-threads=1; cargo clippy -p ostro-server --all-targets --locked -- -D warnings'` | ⬜ pending |
| 6-16-02 | 16 | 6 | FX-05, FX-06, FX-07, FX-08, FX-10 | CLI verbs + API-based e2e helpers | `bash -c 'set -euo pipefail; cargo test -p ostro --test cli_bridge --locked -- --test-threads=1; cargo test -p ostro-cli --locked; cargo build -p ostro-server --tests --locked; cargo clippy -p ostro-cli -p ostro --all-targets --…` | ⬜ pending |
| 6-17-01 | 17 | 7 | FX-01, FX-02, FX-03, FX-08, FX-09 | SC1 in-process (fx_http_e2e.rs) + kill -9 re-drive with the failpoint  | `bash -c 'set -euo pipefail; cargo test -p ostro-server --test fx_http_e2e --locked -- --test-threads=1; cargo test -p ostro-server --features failpoints --test fx_http_e2e --locked -- --test-threads=1; cargo test -p ostro --fea…` | ⬜ pending |
| 6-17-02 | 17 | 7 | FX-01, FX-02, FX-03, FX-08, FX-09 | SC4 through a guest: SSRF table, env proxy, allow-lists (fx_ssrf_e2e.r | `bash -c 'set -euo pipefail; cargo test -p ostro-server --test fx_ssrf_e2e --locked -- --test-threads=1; cargo test -p ostro-server --test fx_rate_e2e --locked -- --test-threads=1; cargo clippy -p ostro-server -p ostro --all-tar…` | ⬜ pending |
| 6-18-01 | 18 | 7 | FX-04 | SC2 crash test on PG (+ MySQL when available) | `bash -c 'set -euo pipefail; test -n "${OSTRO_TEST_PG_URL:-}" \|\| { echo "export OSTRO_TEST_PG_URL (06-11 SUMMARY form)"; exit 1; }; cargo test -p ostro --features failpoints --test fx_sql_crash --locked -- --test-threads=1 --n…` | ⬜ pending |
| 6-18-02 | 18 | 7 | FX-04 | CI service containers + ci-local env gating | `bash -c 'set -euo pipefail; grep -q "mysql:8.4" .github/workflows/ci.yml; grep -q "postgres:17" .github/workflows/ci.yml; grep -q "minio" .github/workflows/ci.yml; grep -q "OSTRO_TEST_MYSQL_URL" scripts/ci-local.sh; for c in $(…` | ⬜ pending |
| 6-19-01 | 19 | 7 | FX-05, FX-06, FX-07, FX-10 | Canary harness (ostro-testkit, feature canary) | `bash -c 'set -euo pipefail; cargo test -p ostro-testkit --features canary --locked; cargo clippy -p ostro-testkit --all-features --all-targets --locked -- -D warnings'` | ⬜ pending |
| 6-19-02 | 19 | 7 | FX-05, FX-06, FX-07, FX-10 | SC3 end to end (fx_canary.rs) | `bash -c 'set -euo pipefail; cargo test -p ostro-server --test fx_canary --locked -- --test-threads=1'` | ⬜ pending |
| 6-19-03 | 19 | 7 | FX-05, FX-06, FX-07, FX-10 | SC5 end to end: object store via guest (fx_object_e2e.rs) and provider | `bash -c 'set -euo pipefail; cargo test -p ostro-server --test fx_object_e2e --locked -- --test-threads=1; cargo test -p ostro-server --test fx_providers_e2e --locked -- --test-threads=1; cargo clippy -p ostro-server --all-targe…` | ⬜ pending |
| 6-20-01 | 20 | 8 | all | Documentation + docs/03 and docs/02 corrections | `bash -c 'set -euo pipefail; grep -q effect_outcome_unknown docs/connectors.md; grep -q "fd00:ec2::254" docs/security-egress.md; grep -q "__ostro/" docs/secrets.md; grep -q ostro-objstore docs/03-architecture.md; grep -qi "azure…` | ⬜ pending |
| 6-20-02 | 20 | 8 | all | Replay-corpus entries + reservations flip + freeze/deny re-check | `bash -c 'set -euo pipefail; ls tests/replay-corpus \| grep -q "fx-"; cargo test -p ostro-engine --test replay_corpus --locked; cargo test -p ostro-witcheck --locked; cargo test -p ostro-types --test golden_frozen --locked; carg…` | ⬜ pending |
| 6-20-03 | 20 | 8 | all | Full ci-local gate + 06-VALIDATION.md | `bash -c 'set -euo pipefail; bash scripts/ci-local.sh; grep -q "Gate results" .planning/phases/06-effects-bridge-connectors-secrets/06-VALIDATION.md; grep -q "Human-needed" .planning/phases/06-effects-bridge-connectors-secrets/0…` | ⬜ pending |

MySQL/S3 legs inside these commands SKIP loudly when the env var is unset (⚠ human_needed). The SC3 OTLP leg is deferred to Phase 8 (D-FX6).

*Status: ⬜ pending · ✅ green · ❌ red · ⚠ flaky / human_needed*

---

## Wave 0 Requirements

Test files are created by the task that first needs them (TDD tasks write tests first).

- [ ] **ostro-bridge tests**: `ssrf_table`, `egress_client`, `secrets`, `driver`, `ratelimit`, `http_connector`, `sql_pg`, `sql_mysql`, `object_local`, `object_s3`, `providers_vault`, `providers_k8s`
- [ ] **ostro-objstore tests**: `localfs`, `presign`, `s3` (unless Phase 8 already created them; 06-12 then adds only presign cases)
- [ ] **ostro-partition**: `tests/apply_effects.rs`, `tests/common/gen_effects.rs`
- [ ] **ostro-engine**: `tests/effects_driver.rs`
- [ ] **ostro-api**: `tests/bridge_surface.rs`
- [ ] **ostro-server**: `tests/{effects_seam, bridge_admin, fx_http_e2e, fx_ssrf_e2e, fx_rate_e2e, fx_canary, fx_object_e2e, fx_providers_e2e}.rs`, `tests/common/fx.rs`
- [ ] **ostro binary**: `tests/{cli_bridge, fx_redrive, fx_sql_crash}.rs`
- [ ] **ostro-testkit** (features): `mock_provider.rs` (06-10), `db.rs` (06-11), `canary.rs` (06-19)
- [ ] **Guests**: fx-http, fx-sql, fx-object, fx-canary, fx-ssrf (06-09)
- [ ] **Failpoint sites**: `bridge::http::before_send`, `bridge::http::after_response`, `bridge::sql::before_user_stmt`, `bridge::sql::after_commit`, `bridge::object::before_put`, `bridge::effect::before_complete_submit`
- [ ] **CI**: ci.yml services postgres:17 and mysql:8.4, MinIO via `docker run`, env exports (06-18)

---

## Manual-Only / Human-Needed Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| SC2 on MySQL 8.4 (exactly one row per run after kill -9) and the sql_mysql suite | FX-04 | No mysqld and no Docker in WSL (D-FX5) | After the first push the CI service container runs it. Locally: install mysql-server 8.4, then `OSTRO_TEST_MYSQL_URL='mysql://root:…@127.0.0.1:3306/ostro?ssl-mode=REQUIRED' cargo test -p ostro --features failpoints --test fx_sql_crash -- --test-threads=1` and `cargo test -p ostro-bridge --test sql_mysql`. |
| Object store on S3-compatible storage (MinIO) incl. presign | FX-05 | No Docker in WSL | CI MinIO step. Locally: `OSTRO_TEST_S3_URL=http://127.0.0.1:9000 OSTRO_TEST_S3_ACCESS_KEY=… OSTRO_TEST_S3_SECRET_KEY=… OSTRO_TEST_S3_BUCKET=… cargo test -p ostro-objstore -p ostro-bridge --test object_s3 && cargo test -p ostro-server --test fx_object_e2e`. |
| GCS through S3 interoperability | FX-05 | Needs a GCS bucket and HMAC keys (MEDIUM confidence) | Point the `OSTRO_TEST_S3_*` vars at `https://storage.googleapis.com`, region `auto`, and run the object suites. |
| Canary absent from OTLP export | FX-06 (SC3) | OTLP export lands in Phase 8 (D-FX6) | Phase 8 registers an OTLP mock collector with `ostro_testkit::canary::register_sink` and re-runs `fx_canary`. |
| Remote CI legs (aarch64-musl, macOS) | all | No git remote yet (FOUND-05) | Confirm green after the first push. |
| Review of the FX-05 wording/Azure deferral, D-FX2 (native roots) and D-FX6 | FX-04, FX-05, FX-06 | User unavailable (YOLO) | Read 06-DECISIONS.md. The STATE.md Pending Todos list each item. |
| Real Vault dev server (optional) | FX-10 | The mock covers the suite | `OSTRO_TEST_VAULT_ADDR=… OSTRO_TEST_VAULT_TOKEN=… cargo test -p ostro-bridge --test providers_vault`. |

---

## Gate results

(Filled in by 06-20 Task 3.)

## Human-needed

(Filled in by 06-20 Task 3.)

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 60 s for task-level checks
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
