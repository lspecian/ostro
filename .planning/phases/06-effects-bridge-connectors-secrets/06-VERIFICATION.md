---
phase: 06-effects-bridge-connectors-secrets
verified: 2026-11-20T23:10:12Z
status: human_needed
score: 5/5 success criteria verified on the local legs (SC2 MySQL leg and SC5 S3 leg are human_needed by design, D-FX5/D-FX10); 10/10 FX requirements satisfied
gaps: []
human_verification:
  - test: "SC2 on MySQL 8.4: OSTRO_TEST_MYSQL_URL='mysql://root:<pw>@127.0.0.1:3306/ostro?ssl-mode=REQUIRED' cargo test -p ostro --features failpoints --test fx_sql_crash --locked -- --test-threads=1, plus cargo test -p ostro-bridge --test sql_mysql --locked -- --test-threads=1 (with and without --features failpoints). Or the ci.yml mysql:8.4 service after the first push."
    expected: "mysql_after_commit and mysql_before_user_stmt each end with exactly 50 user rows, one effect_completed per run and byte-identical stored token results; sql_mysql 8/8 (10/10 with failpoints) without SKIP."
    why_human: "No mysqld and no Docker in WSL (D-FX5). The MySQL code paths ('?' placeholders, HEX(result), performance_schema.data_locks hold detection, InnoDB deadlock mapping) have never been executed against a server."
  - test: "SeaweedFS leg of SC5: docker run -d -p 8333:8333 -e AWS_ACCESS_KEY_ID=ostro -e AWS_SECRET_ACCESS_KEY=ostroseaweed -e S3_BUCKET=ostro-test chrislusf/seaweedfs:4.54 mini -dir=/data, then OSTRO_TEST_S3_URL=http://127.0.0.1:8333 OSTRO_TEST_S3_ACCESS_KEY=ostro OSTRO_TEST_S3_SECRET_KEY=ostroseaweed OSTRO_TEST_S3_BUCKET=ostro-test cargo test -p ostro-objstore --locked -- --test-threads=1 && cargo test -p ostro-bridge --test object_s3 --locked && cargo test -p ostro-server --test fx_object_e2e --locked -- --test-threads=1"
    expected: "put/get/head/delete and presign-get/presign-put work against S3 (s3_backend_through_guest passes without the SKIP line)."
    why_human: "No Docker in WSL (D-FX5/D-FX10); CI runs it in the SeaweedFS docker-run step."
  - test: "Remote CI legs after the first push (linux-x86_64-gnu with postgres:17/mysql:8.4/SeaweedFS, aarch64-musl, macOS)"
    expected: "All green; the matrix-conditional empty service image really skips the DB services on non-gnu legs."
    why_human: "No git remote yet (FOUND-05); the empty-image skip is unverified GitHub runner behaviour (06-18)."
  - test: "GCS through S3 interoperability: point OSTRO_TEST_S3_* at https://storage.googleapis.com (HMAC keys, region auto) and run the S3 commands above"
    expected: "Object suites pass"
    why_human: "Needs a GCS bucket and HMAC keys (MEDIUM confidence; docs do not promise it)."
  - test: "User review of the FX-05 wording and Azure deferral (D-FX1), D-FX2 (native TLS roots + ca_file) and D-FX6"
    expected: "User accepts or overrides; an override after shipping becomes a follow-up plan"
    why_human: "Adopted in YOLO mode; listed in STATE.md Pending Todos."
  - test: "Optional: real Vault dev server: OSTRO_TEST_VAULT_ADDR=http://127.0.0.1:8200 OSTRO_TEST_VAULT_TOKEN=<root> cargo test -p ostro-bridge --test providers_vault --locked"
    expected: "real_vault_dev_server passes instead of SKIP"
    why_human: "Optional; the mock Vault covers the suite."
---

# Phase 6: Effects Bridge, Connectors & Secrets Verification Report

**Phase Goal:** Workflows safely call the outside world. HTTP effects are idempotent and never blindly resent. SQL effects are exactly-once. Secrets never enter guests, and egress cannot reach internal networks.
**Verified:** 2026-11-20T23:10:12Z (main at 8e6ccd7)
**Status:** human_needed
**Re-verification:** No (initial verification)

All six human_needed items are by design: environment-gated legs (MySQL, SeaweedFS, GCS, remote CI) and user reviews. Every one was planned as such in 06-DECISIONS (D-FX1, D-FX2, D-FX5, D-FX6, D-FX10). None is a code gap. This matches the Phase 7/9 gate condition "human_needed with by-design items only".

## Method

- The code and tests were read on main 8e6ccd7, in a detached worktree. The SUMMARY claims were not taken as evidence.
- The gate log `gates68-cilocal.log` was checked: full ci-local with musl on 5faa97c, `all steps passed`, EXIT=0. Every commit after 5faa97c touches only `.planning/` and `docs/` (`git diff --name-only 5faa97c 8e6ccd7`).
- The PG server log `gates68-pg.log` of the private PG 14.24 cluster (port 55468) shows real `ostro_effects` traffic during the run, including the expected duplicate-token PK errors from the reconcile/re-drive paths. So the PG legs really ran and did not SKIP.
- Spot runs (sequential, `nice`, `-j 2`, `CARGO_TARGET_DIR=target/wt-verify6`, deleted afterwards) on 8e6ccd7:

| Command | Result |
|---------|--------|
| `cargo test -p ostro-bridge --test ssrf_table --test ratelimit` | 5 + 4 passed |
| `cargo test -p ostro-bridge --lib -- http:: secrets:: sql::classify egress::classify` | 91 passed |
| `cargo test -p ostro-bridge --test http_connector --test secrets --test object_local --test providers_vault --test providers_k8s -- --test-threads=1` | 24 + 10 + 8 + 8 + 5 passed |
| `sha256sum wit/ostro-runtime-0.1.0/runtime.wit` | `611d2982…bd98e`, equal to `wit/FROZEN.toml` |

## Goal Achievement

### Observable Truths (ROADMAP Success Criteria)

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | An HTTP effect's intent is committed before any I/O and carries a stable idempotency key. In-flight effects are re-driven by replay with the same key. Past the provider's window, the effect fails with `effect_outcome_unknown` instead of resending. | ✓ VERIFIED | **Intent first:** `ostro-engine/src/host/state.rs::flush` calls `driver.start` only after `sink.append(..)` returned `Ok`; a failed append starts nothing, and an effect scheduled and cancelled in the same batch is never started. **Re-drive:** `host/effects.rs::schedule_effect` re-drives a replayed `EffectScheduled` that has no completion (`redrive = true`). **Key:** `bridge/src/http/key.rs::derive_key` = `ostro-` + hex(sha256("ostro/idem/v1" ‖ run ‖ op_le))[..16]; it can be overridden by the `idempotency-key` field or the guest header, and the SDK provides `ctx.idempotency_key(name)` for (run, name) keys. **Window:** `http/send.rs` applies `window::decide` only when `effect.redrive` is set; non-idempotent methods past window − margin become `NonRetryable{effect_outcome_unknown}`. **Tests:** `crates/ostro/tests/fx_redrive.rs` uses a real kill -9 of a spawned node: `same_key_after_crash` (2 requests with the same key, 1 charge), `window_unknown` (POST not re-sent, `effect_outcome_unknown`, retryable=false), `idempotent_resend` (GET re-sent with the same key) and `suspended_inflight_wakes`. All 4 passed in the gate's failpoint suites. `effects_seam.rs` (`intent_before_io`, `uncompleted_effect_redriven_not_repaired`, `evicted_inflight_wakes`) and `fx_http_e2e.rs` (`intent_durable_before_send`, key tests) are green in the gate. |
| 2 | With a crash between the user DB commit and `effect_completed`, SQL exactly-once mode produces exactly one row change per run on Postgres and MySQL. | ✓ VERIFIED (PG); ? MySQL human_needed | `crates/ostro/tests/fx_sql_crash.rs::scenario` holds 50 exactly-once INSERT runs at `bridge::sql::after_commit` (and at `before_user_stmt`), asserts that the user rows committed and that no `effect_completed` exists, then kills the node with kill -9 and restarts it without failpoints. It asserts: every run completed; exactly 1 `effect_scheduled` and 1 `effect_completed` per run; `count(*) = 50`; the exact (run, amount) set; one token row per run with byte-identical stored results. `pg_after_commit` and `pg_before_user_stmt` passed in the gate's failpoint suites against private PG 14. `sql/exactly_once.rs` implements the token-first transaction, the post-COMMIT reconcile that never completes Retryable (P-19), and the W12 retention horizon. `sql/classify.rs` implements the implicit-commit allow-list. The MySQL legs share the same macro-generated protocol and the same test code, but were a loud SKIP. |
| 3 | A canary `{{secret:name}}` never appears on disk, in the journal, logs, OTLP or object storage, and is never readable by the guest. Other-origin grants are refused, templated requests don't follow redirects, echoed values are scrubbed. | ✓ VERIFIED | `crates/ostro-server/tests/fx_canary.rs::scan_everything` scans, for every needle the canary produces: REST and gRPC responses, guest output, raw and Debug journal events, decoded effect outcomes and results, TRACE capture (with a non-vacuity check on run ids), registered sinks, and after a clean shutdown every file in the data dir and the object root. `leak_scan` asserts the provider got the real value while the guest saw `[REDACTED]`. `grant_origin_refused` asserts `secret_not_granted` with zero requests sent. `no_redirect` asserts the 302 is returned and `/echo` gets 0 hits. `echo_scrubbed_encodings` runs every encoding form with a body grant. `assert_guest_blind` checks guest env, args and random. The OTLP leg is `otlp_canary.rs::otlp_export_never_carries_the_canary`; it passed in the gate's `cargo test` and musl legs. In code, `SecretValue` sits in a zeroize-on-drop `SecretBox` with a redacted Debug, and the journal stores the redacted request copy (`convert::redact_http_request`). |
| 4 | The SSRF bypass table is blocked (private, link-local, metadata, IPv4-mapped IPv6, NAT64, decimal/octal, `fd00:ec2::254`, redirects, env proxies). Per-module allow-lists and per-target rate limits are enforced. | ✓ VERIFIED | `fx_ssrf_e2e.rs::bypass_table_denied_by_default` runs every row through the fx-ssrf guest with the allow-list admitting each host, so only the classifier can deny. The rows include 2130706433, 0177.0.0.1, 0x7f.1, 127.1, %31%32%37.0.0.1, 0, 169.254.169.254, 169.254.170.2, 100.100.100.200, [fd00:ec2::254], [fd20:ce::254], ::ffff:mapped, ::127.0.0.1, 64:ff9b:: NAT64, 2002:: 6to4, Teredo, ::1 and fe80::1, and each must deny with the expected `details.reason`. `redirects_to_internal_literals`, `env_proxies_are_ignored` and `per_module_allow_lists` cover the rest. `egress/client.rs` sets `.no_proxy()` and `Policy::none()` and pins addresses through `EgressResolver`. `ssrf_table` covers the DNS-name rows and passed in the spot run. Rate limits use the governor GCRA per target in `ratelimit.rs`, wired in `bridge_services.rs`; `fx_rate_e2e::per_target_rate_limit` and `ratelimit` (spot run 4/4) cover them. |
| 5 | Object-store get/put/presign works against S3-compatible and local backends. Secrets resolve from sealed, env/file, Vault and Kubernetes providers. | ✓ VERIFIED (local, providers); ? S3 human_needed | `fx_object_e2e.rs::local_backend_through_guest` runs put → get → head → presign-get → presign-put through the guest, uses the presigned URLs over HTTP, and checks expiry and tamper rejection. It is green in the gate, and the `tamper()` flake was fixed in 8533678. `object_local` passed 8/8 in the spot run. `s3_backend_through_guest` and `object_s3` are written in full against `OSTRO_TEST_S3_*`; they SKIP locally. `fx_providers_e2e.rs::all_five_providers_resolve_through_the_chain` resolves sealed, env, file, Vault (mock) and K8s (mock) values through a real guest request, and checks chain order. `providers_vault` 8/8 and `providers_k8s` 5/5 passed in the spot run. |

**Score:** 5/5 truths verified on the available legs. The MySQL part of SC2 and the S3 part of SC5 are human_needed.

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `crates/ostro-engine/src/effects_driver.rs`, `host/state.rs`, `host/effects.rs` | EffectDriver seam, start after commit, replay re-drive | ✓ VERIFIED | Wired through `ActivationSink::effect_driver`. The server overrides it in `sink.rs:632` and delegates in `executor.rs:613`. |
| `crates/ostro-bridge/src/{driver,registry,ratelimit,outcome}.rs` | Bridge facade, registry, rate limits, panic → internal_error | ✓ VERIFIED | `bridge.register(http/sql/object)` in `ostro-server/src/bridge_services.rs:369-371`. |
| `crates/ostro-bridge/src/http/*` | Key, window, retry, target, send (templating, no-redirect, identity encoding, scrub) | ✓ VERIFIED | `http_connector` passed 24/24 (spot run). |
| `crates/ostro-bridge/src/sql/*` | Pools, allow-list, exactly-once, GC | ✓ VERIFIED | `spawn_gc` is wired in `bridge_services.rs:379`. The PG legs ran. |
| `crates/ostro-bridge/src/egress/*` | Classifier, URL gate, policy, pinned resolver, guarded client | ✓ VERIFIED | Only `egress/client.rs` builds reqwest clients. |
| `crates/ostro-bridge/src/secrets/*` | Template, grants, scrubber, sealed store, providers (env/file/sealed/vault/k8s), resolver | ✓ VERIFIED | Spot runs green. |
| `crates/ostro-objstore`, `crates/ostro-bridge/src/object/*` | LocalFs + S3 (rusty-s3), presign, local HMAC presign | ✓ VERIFIED | No TODO/unimplemented stubs found. |
| `crates/ostro-partition` effects rows, Cmd 13 / Intent 9,10 / InvalidCode 15 | Apply plus RESERVATIONS | ✓ VERIFIED | RESERVATIONS rows are `exists (P6, …)`. Golden append-only; format corpus green in the gate. |
| `crates/ostro-server/src/{bridge_services,effects,control/bridge}.rs`, CLI verbs | Server seam, CompleteEffect submission, admin API | ✓ VERIFIED | `bridge_admin` and `cli_bridge` green in the gate. |
| `guests/fx-{http,sql,object,canary,ssrf}`, SDK effect builders | Test guests and SDK | ✓ VERIFIED | Guest build step PASS. |
| `docs/connectors.md`, `docs/security-egress.md`, `docs/secrets.md` | Normative docs | ✓ VERIFIED | Present, with §9 known limitations. |
| `tests/replay-corpus/fx-*` (5 entries) | Replay with no EffectDriver | ✓ VERIFIED | `replay_corpus` green in the gate. |
| `.github/workflows/ci.yml` services | postgres:17, mysql:8.4, SeaweedFS | ✓ VERIFIED (static) | Lines 93-234. Not yet executed remotely. |

### Key Link Verification

| From | To | Via | Status |
|------|----|-----|--------|
| `HostState::flush` | `EffectDriver::start` | after `sink.append` Ok | WIRED |
| replayed `EffectScheduled` without completion | `EffectDriver::start(redrive=true)` | `schedule_effect` | WIRED |
| `PartitionSink` | `BridgeServices` driver adapter | `effect_driver()` override | WIRED |
| Bridge outcome | partition | `Cmd::CompleteEffect` (`ostro-server/src/effects.rs:406`) | WIRED |
| `LeaderAnnounced` apply | ready rows for runs with open effects rows | P-9 wake | WIRED (`evicted_inflight_wakes`, `suspended_inflight_wakes`) |
| HTTP connector | egress gate → grants → key → window → guarded client | `http/send.rs` | WIRED |
| SQL connector | `connection:X` policy → pool → exactly-once | `sql/mod.rs` | WIRED |
| Secret providers chain | `[secrets] providers` config | `bridge_services.rs` | WIRED (e2e resolves all five) |

### Requirements Coverage

| Requirement | Description | Status | Evidence |
|-------------|-------------|--------|----------|
| FX-01 | HTTP intent before I/O; timeouts, size cap, redirect policy, retries | ✓ SATISFIED | flush ordering, `http_connector`, `effects_seam::intent_before_io*`, `fx_http_e2e` |
| FX-02 | Stable idempotency key (run, op, name), overridable | ✓ SATISFIED | `http/key.rs`, SDK `ctx.idempotency_key`, `fx_http_e2e::override_and_semantic_keys` |
| FX-03 | Re-drive with the same key; past the window, `effect_outcome_unknown` | ✓ SATISFIED | `fx_redrive` (kill -9) |
| FX-04 | SQL PG/MySQL named connections, exactly-once token | ✓ SATISFIED (PG); MySQL leg human_needed | `fx_sql_crash` PG, `sql_pg` 10/13 |
| FX-05 | Object store via ostro-objstore (S3-compatible + local) with get/put/delete/head/presign | ✓ SATISFIED (local); S3/GCS human_needed | `fx_object_e2e` local, `object_local` |
| FX-06 | `{{secret:name}}` resolved host-side only, zeroized, never journaled/logged, never in guests | ✓ SATISFIED | `fx_canary`, `otlp_canary`, `secrets/value.rs` |
| FX-07 | Origin-bound grants, no redirects for templated requests, scrubbed responses | ✓ SATISFIED | `fx_canary` cases 2-4 |
| FX-08 | Per-module allow-lists, SSRF table, resolve once and pin, no proxy | ✓ SATISFIED | `fx_ssrf_e2e`, `ssrf_table` |
| FX-09 | Per-target rate limits | ✓ SATISFIED (per node, D-FX7) | `fx_rate_e2e`, `ratelimit` |
| FX-10 | Pluggable providers: sealed, env/file, Vault, Kubernetes | ✓ SATISFIED | `fx_providers_e2e`, `providers_vault`, `providers_k8s` |

No orphaned requirements: REQUIREMENTS.md maps exactly FX-01..FX-10 to Phase 6, and every one is claimed by at least one plan. The REQUIREMENTS.md checkboxes and traceability rows still read "Pending". That is tracking only and the orchestrator updates it on phase completion; this verifier did not edit it.

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| `crates/ostro-bridge/src/sql/mod.rs` (via `fetch_all`) | - | Result set fully buffered before the row/byte caps | ℹ️ Info | Documented in docs/connectors.md §9. The cap still rolls back and fails `response_too_large`; only peak memory is unbounded. Follow-up: add `futures-core` and stream with an early stop. |
| `crates/ostro-bridge/src/secrets/providers/vault.rs` | - | vaultrs keeps the Vault session token in a plain `String` | ℹ️ Info | Operator credential, not a resolved template secret; never formatted or logged. Documented in docs/secrets.md §9. |
| `secrets` template grammar | - | Vault `#field` and upper-case K8s keys unreachable from `{{secret:..}}` | ℹ️ Info | Documented limitation (docs/secrets.md §9); FX-10 is still met. |

No TODO, FIXME, `unimplemented!` or `todo!` in `ostro-bridge/src`, `ostro-objstore/src` or the server bridge files.

### Human Verification Required

1. **MySQL 8.4 legs (FX-04, SC2).** Run `fx_sql_crash` (`mysql_*`) with `--features failpoints`, plus `sql_mysql` with and without failpoints, against mysql 8.4 with `OSTRO_TEST_MYSQL_URL=…?ssl-mode=REQUIRED`. Expected: exactly 50 rows per scenario and no SKIP lines. Why human: no mysqld or Docker locally. The MySQL-specific SQL has never been executed.
2. **SeaweedFS legs (FX-05, SC5).** Run `ostro-objstore`, `ostro-bridge --test object_s3` and `ostro-server --test fx_object_e2e` with `OSTRO_TEST_S3_*` set against `chrislusf/seaweedfs:4.54 mini`. Expected: `s3_backend_through_guest` passes with no SKIP. Why human: no Docker locally.
3. **Remote CI legs.** After the first push, confirm linux-x86_64-gnu (DB and S3 services), aarch64-musl and macOS are green, and that the empty-image service skip works on the non-gnu legs.
4. **GCS S3-interop.** Optional, MEDIUM confidence.
5. **User review** of FX-05/Azure (D-FX1), D-FX2 and D-FX6.
6. **Optional real Vault dev server** run of `providers_vault`.

### Gaps Summary

No gaps. Every success criterion and every FX requirement is backed by substantive code that is wired into the server and covered by green tests. The tests include real kill -9 crash scenarios for SC1 and SC2. The remaining open items are the environment-gated MySQL and S3 legs, remote CI, GCS, and user reviews, all planned as human_needed in 06-DECISIONS. The three documented limitations above are informational and do not violate any success criterion.

---

_Verified: 2026-11-20T23:10:12Z_
_Verifier: Claude (gsd-verifier)_
