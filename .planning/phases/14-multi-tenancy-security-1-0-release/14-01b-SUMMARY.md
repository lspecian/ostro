---
phase: 14-multi-tenancy-security-1-0-release
plan: 01b
subsystem: server seam + call sites + config + CLI/client stubs + fuzz bins + test tooling
tags: [gate, seam, p14, auth, audit, signing, quotas, kafka, config, fuzz, tools, seaweedfs, kafka-broker]
requires:
  - phase: 14-01
    provides: "contracts (ostro_api::auth, ostro_api::phase14, ostro_types::audit, ext 112-117, Audited<R>), seam/method/tenant-field inventories, every manifest, fuzz workspace + nightly"
provides:
  - "crates/ostro-server/src/p14/mod.rs: the single Phase 14 seam (final after 14-01b) + 21 passthrough hook files, audit/* (14-04) and flow/tenant.rs (14-06) stubs"
  - "every fixed call site: server.rs (ctx, clock, authenticator, guarded REST/gRPC/UI, services, triggers, shutdown), control/modules.rs (upload gate before the existing-sha lookup, register_existing, evidence, activation gate), version gates (start/migrate/rerun), ingress admission (start/signal/token), audited bridge resolver, worker join/renew/revoke events"
  - "config [auth] / [tenants] / [audit] / [security.modules] / raw [[triggers.kafka]] (both profiles)"
  - "main.rs offline intercepts (auth bootstrap, audit verify); CLI tenant/auth/audit/security and client stubs; shared OSTRO_TOKEN bearer"
  - "ostro-api contract additions for the call sites: ApiConfig.authenticator, UploadModule.signatures"
  - "15 fuzz target bins; scripts/fetch-tools.sh (pinned + checksummed); scripts/test-services.sh (loopback SeaweedFS + Kafka with PLAINTEXT/SASL/SSL); shared binary size limit (D-P14-54)"
affects: [14-02, 14-03, 14-04, 14-05, 14-06, 14-07, 14-08, 14-10, 14-15, 14-16, 14-17, 14-18, 14-19, 14-20, 14-21, 14-27, 14-30, 14-31, 14-32, 14-34]
tech-stack:
  added: []
  patterns:
    - "control-plane hooks reach the seam through a weak OnceLock slot (`p14::attach` / `p14::ctx_of`): unset = Phase 13 behaviour (unit-built control planes), released = 503 (fail closed during shutdown)"
    - "startup failures after the API exists (p14::start, start_triggers) shut the running server down through its own handle"
    - "client-facing routers receive `p11::ApiFront` built by server.rs; in-process consumers keep `ControlCtx::routed`"
key-files:
  created:
    - crates/ostro-server/src/p14/{mod,guard,authn,authz,tokens,bootstrap,signing,quota_view,test_hooks,principal_fwd,tenant_scope,upload_tenant,tenants_api,tokens_api,audit_hooks,audit_api,module_sign,security_api,reseal,ingress,ui_auth,kafka}.rs
    - crates/ostro-server/src/audit/{mod,journal,segment,chain,writer,verify,export}.rs
    - crates/ostro-server/src/flow/tenant.rs
    - crates/ostro-server/tests/p14_skeleton.rs
    - crates/ostro-client/src/{tenants,api_tokens,audit,security}.rs
    - crates/ostro-cli/src/commands/{tenant,auth,audit,security}.rs
    - crates/ostro-repl-conformance/src/scenarios/p14.rs
    - fuzz/fuzz_targets/*.rs (15), fuzz/regressions/README.md
    - scripts/fetch-tools.sh, scripts/test-services.sh, scripts/binary-size-limit, scripts/lib/binary-size.sh
  modified:
    - crates/ostro-server/src/{lib,config,server,p11,bridge_services}.rs
    - crates/ostro-server/src/control/{mod,modules,runs,migrate,lifecycle,p11_rerun}.rs
    - crates/ostro-server/src/{route/mod,workers/join,flow/mod}.rs
    - crates/ostro-server/tests/{config,sink}.rs
    - crates/ostro-api/src/{lib,domain,rest,grpc,fake}.rs, crates/ostro-api/tests/{parity,grpc_client_parity,phase7_surface}.rs, crates/ostro-ui/tests/common/mod.rs
    - crates/ostro/src/main.rs
    - crates/ostro-client/src/{lib,client}.rs, crates/ostro-cli/src/commands/mod.rs
    - crates/ostro-repl-conformance/src/scenarios/mod.rs
    - scripts/ci-local.sh
key-decisions:
  - "build_ctx, start and start_triggers return Result<_, P14Error> (the seam is final: audit-journal open, bootstrap and Kafka validation must be able to fail startup)"
  - "P14Ctx is built right after the cluster start (meta + clock), before the bridge and the worker join service; blobs and partition roles are bound later with P14Ctx::attach_node"
  - "the upload path also gets evidence_ext (new record) and merge_evidence (existing sha) so 14-19 can store ext 113 without editing modules.rs"
  - "TriggerKind::of_key classifies starts by idempotency-key prefix (__kafka/, __hook/, __cron/)"
  - "ApiConfig.authenticator (AllowDev default) and UploadModule.signatures were added to ostro-api: 14-02 expects both as existing contracts and owns neither file"
  - "OSTRO_TOKEN is read once per Client and sent as Authorization: Bearer on every request (Client::with_bearer overrides)"
requirements-completed: []
duration: ~4h
completed: 2026-11-26
---

# Phase 14 Plan 01b: PHASE-14 GATE part 2 (seam, stubs, call sites, config, tools) Summary

**The final Phase 14 seam with 21 passthrough hook files and every fixed call site (upload gate before the existing-sha lookup, version/activation gates, ingress admission, audited bridge resolver, worker events, guarded REST/gRPC/UI with the `/ui` dev scope), the `[auth]`/`[tenants]`/`[audit]`/`[security.modules]`/`[[triggers.kafka]]` config, offline intercepts, CLI/client stubs with the shared `OSTRO_TOKEN` bearer, 15 fuzz bins on the pinned nightly, checksum-pinned tools and Docker-free loopback SeaweedFS + Kafka (PLAINTEXT, SASL PLAIN/SCRAM, SSL).**

Requirement ids are not ticked here (14-27's, D-P14-35).

## Commits

| Commit | Task | Content |
|---|---|---|
| 62c003f | 1 | feat: p14 seam, stubs and config |
| 57c5da2 | 2 | feat: Phase 14 call sites + CLI/client stubs |
| b4c514a | 3 | build: fuzz bins, tool pins, CI sync |
| (this) | — | docs: SUMMARY, ROADMAP tick, STATE line |

(Concurrent commits landed in between: 37a0e8a p13-campaign harness retry; the CAS rebased onto it, no overlap.)

## Stub-module map (file → owning plan)

| File | Owner | Stub behaviour |
|---|---|---|
| `p14/guard.rs` (`wrap`) | 14-03 | passthrough clones |
| `p14/authn.rs` (`authenticator`) | 14-03 | `AllowDev` in both modes |
| `p14/tokens.rs` | 14-03 | empty (token codec) |
| `p14/bootstrap.rs` (`ensure_admin`, `offline`, `BootstrapArgs`) | 14-03 | Ok / "not implemented until 14-03" |
| `audit/{journal,segment,chain,export}.rs` | 14-04 | empty |
| `audit/writer.rs` (`open`, `start`, `flush`) | 14-04 | `NoopAuditSink`, no task, no-op |
| `audit/verify.rs` (`offline`, `VerifyArgs`) | 14-04 | "not implemented until 14-04" |
| `p14/signing.rs` | 14-05 | empty |
| `p14/quota_view.rs` (`TenantQuotaView::new`, `start`) | 14-06 | empty view, no task |
| `flow/tenant.rs` (`TenantFlow`) | 14-06 | empty |
| `p14/test_hooks.rs` (`wrap_clock`) | 14-10 | identity |
| `p14/authz.rs` (`require(&Principal, Method)`) | 14-15 | Ok |
| `p14/principal_fwd.rs` (`encode`, `decode`) | 14-15 | `Principal::{encode,decode}_forwarded` |
| `p14/tenant_scope.rs` (`before`, `after`) | 14-16 | passthrough |
| `p14/upload_tenant.rs` (`register_existing`) | 14-16 | `RegisterDecision::Proceed` |
| `p14/tenants_api.rs`, `p14/tokens_api.rs` (`install`) | 14-17 | slot stays `None` |
| `p14/audit_hooks.rs` (`reserve`, `after`, `worker_event`) | 14-18 | `Ok(None)`, Ok, no-op |
| `p14/audit_api.rs` (`install`) | 14-18 | slot stays `None` |
| `p14/module_sign.rs` (`upload_gate`, `evidence_ext`, `merge_evidence`, `version_gate`, `activation_gate`) | 14-19 | empty evidence / allow |
| `p14/security_api.rs` (`install`) | 14-19 | slot stays `None` |
| `p14/reseal.rs` (`start`) | 14-19 | no task |
| `p14/ingress.rs` (`admit`) | 14-20 | admit |
| `p14/ui_auth.rs` (`ui_auth`, `scope_layer`, `UiScopeLayer`, `token_principal`) | 14-21 | `AllowAll`; mode none → `Principal::dev()` (stays); token mode → `Principal::dev()` until 14-21 |
| `p14/kafka.rs` (`start`) | 14-32 | `Ok(None)` without `[[triggers.kafka]]`, else "Kafka triggers land in 14-32" |
| client `tenants.rs`/`api_tokens.rs` (14-17), `audit.rs` (14-18), `security.rs` (14-19) | as named | `ClientError{code: "unimplemented"}` |
| CLI `commands/{tenant,auth,audit,security}.rs` | 14-17 / 14-18 / 14-19 | usage error "not implemented until 14-NN" |
| `ostro-repl-conformance/src/scenarios/p14.rs` | later Phase 14 plans (14-26) | `Vec::new()` |

## Seam signatures as landed (`crates/ostro-server/src/p14/mod.rs`)

```rust
pub struct P14Error(pub String);                       // -> ServerError::Config
pub struct P14Ctx { pub cfg: Arc<ServerConfig>, pub meta: Arc<MetaHandle>, pub clock: Arc<dyn Clock>,
    pub node_id: u64, pub incarnation: u64, pub data_dir: PathBuf, pub audit: Arc<dyn AuditSink>,
    pub quotas: Arc<quota_view::TenantQuotaView>, pub level_max: u32, pub shutdown: CancellationToken, /* blobs, roles */ }
impl P14Ctx { pub fn attach_node(&self, blobs: Option<Arc<BlobStore>>, roles: Arc<dyn PartitionRoles>);
              pub fn blobs(&self) -> Option<&Arc<BlobStore>>; pub fn roles(&self) -> Option<&Arc<dyn PartitionRoles>>; }
pub struct P14Init { cfg, meta, clock, node_id, data_dir, level_max }
pub fn build_ctx(i: P14Init) -> Result<Arc<P14Ctx>, P14Error>
pub fn attach(cp: &ServerControlPlane, ctx: &Arc<P14Ctx>)
pub fn ctx_of(cp: &ServerControlPlane) -> Result<Option<Arc<P14Ctx>>, ApiError>   // None = never attached; Err(503) = released
pub struct Guarded { pub control: Arc<dyn ControlPlane>, pub ext: ApiExtensions }
pub fn authenticator(ctx: &Arc<P14Ctx>) -> Arc<dyn Authenticator>
pub fn wrap_ingress(ctx: &Arc<P14Ctx>, routed: Arc<dyn ControlPlane>, routed_ext: ApiExtensions) -> Guarded
pub fn fill_extensions(ctx: &Arc<P14Ctx>, ext: &mut ApiExtensions)
pub fn ui_auth(ctx: &Arc<P14Ctx>) -> Arc<dyn ostro_ui::auth::UiAuth>
pub fn ui_scope_layer(ctx: &Arc<P14Ctx>, auth: Arc<dyn UiAuth>) -> UiScopeLayer   // UiScopeLayer::apply(Router) -> Router
pub fn wrap_clock(clock: Arc<dyn Clock>) -> Arc<dyn Clock>
pub struct P14Services;  // push(JoinHandle<()>), async stop()  (cancels P14Ctx::shutdown, joins)
pub async fn start(ctx: Arc<P14Ctx>) -> Result<P14Services, P14Error>
pub async fn before_shutdown(ctx: &P14Ctx)
pub enum TriggerKind { Cron, Webhook, Kafka }          // TriggerKind::of_key(Option<&str>) -> Option<TriggerKind>
pub const CRON_KEY_PREFIX: &str = "__cron/"; pub const KAFKA_KEY_PREFIX: &str = "__kafka/";  // webhooks: control::webhooks::HOOK_KEY_PREFIX
pub enum IngressKind { Start { dedupe_hit: bool, trigger: Option<TriggerKind> }, Signal, TokenComplete }
pub async fn start_triggers(ctx: &Arc<P14Ctx>, routed: Arc<dyn ControlPlane>, roles: Arc<dyn PartitionRoles>)
    -> Result<Option<JoinHandle<()>>, P14Error>
pub async fn admit_ingress(ctx: &P14Ctx, kind: IngressKind, tenant: Option<&str>) -> Result<(), ApiError>
pub struct UploadEvidence { pub sigs: Vec<ostro_meta::ext::ModuleSig> }   // is_empty()
pub async fn upload_gate(ctx: &P14Ctx, tenant: Option<&str>, sha: [u8; 32], bytes: &[u8], sigs: &[Vec<u8>]) -> Result<UploadEvidence, ApiError>
pub fn evidence_ext(evidence: &UploadEvidence, ext: &mut Vec<u8>)
pub async fn merge_evidence(ctx: &P14Ctx, sha: [u8; 32], evidence: UploadEvidence) -> Result<(), ApiError>
pub async fn version_gate(ctx: &P14Ctx, tenant: Option<&str>, workflow: &str, version: u32, sha: [u8; 32]) -> Result<(), ApiError>
pub async fn activation_gate(ctx: &P14Ctx, tenant: Option<&str>, sha: [u8; 32]) -> Result<(), ApiError>
pub enum RegisterDecision { Proceed, Deny(ApiError) }
pub async fn register_existing(ctx: &P14Ctx, sha: [u8; 32], tenant: Option<&str>) -> RegisterDecision
pub enum WorkerAction { Join, Renew, RevokeToken }      // as_str(): worker.join / worker.renew / worker.token.revoke
pub struct WorkerAuditEvent { pub action: WorkerAction, pub token_id: String, pub worker_id: Option<String>, pub outcome: AuditOutcome }
pub fn worker_event(ctx: &P14Ctx, ev: WorkerAuditEvent)
pub fn audit_sink(ctx: &P14Ctx) -> Arc<dyn AuditSink>
pub fn lands_in(method: &str, plan: &str) -> ApiError   // 501 helper for stubs
```

**Raw `[triggers] kafka` type:** `TriggersSection.kafka: Vec<config::RawTable>`, `pub struct RawTable(pub config::Value)` (the config crate's value; equality and `Serialize` through `RawTable::to_json()`, origin ignored), `RawTable::try_into_typed::<T>() -> Result<T, ConfigError>` for 14-32's `kafka::validate`. Printed configs render it inline (`kafka = []` / `kafka = [{ … }]`) and reload identically.

**Config (`crates/ostro-server/src/config.rs`, SECTIONS += auth, tenants, audit, security):** `AuthSection { mode: AuthMode::{None, Token} (dev none, server token), allow_insecure_none = false, bootstrap_token_file: Option<PathBuf>, default_token_ttl = 2160h, ui_session_ttl = 8h }`; `TenantsSection { metrics_label_limit = 1000, metrics_tenant_labels = false, quota_share: QuotaShare::Leadership }`; `AuditSection { enabled = true, dir: Option<PathBuf>, segment_bytes = 67108864, queue = 4096, group_commit = 5ms, reserve_timeout = 2s, retention = 2160h, max_bytes = 10737418240, secret_coalesce = 60s, auth_failure_coalesce = 60s, head_export_interval = 1h, otlp = true }`; `SecuritySection { modules: SecurityModulesSection { require_signed = false, minisign_keys = [], cosign_keys = [] } }`; all `deny_unknown_fields`. `validate_phase14()` (non-zero durations, segment ≥ 1 MiB, max_bytes ≥ segment, queue ≥ 1, label limit ≥ 1) runs at load; `validate_auth(server_profile)` (server profile + `mode = "none"` + non-loopback listener needs `allow_insecure_none`) runs in `start_inner` next to `validate_time_skipping`.

**ostro-api additions (contracts 14-02 expects as existing):** `ApiConfig.authenticator: Arc<dyn auth::Authenticator>` (default `AllowDev`; manual `Debug`/`PartialEq` (pointer identity)/`Eq`); `UploadModule.signatures: Vec<Vec<u8>>` (REST/gRPC adapters pass `Vec::new()` until 14-02 decodes the `Ostro-Module-Signature` headers / gRPC field into it).

## Call sites (file : function)

| Site | Call |
|---|---|
| `server.rs : start_inner` (clock construction) | `p14::wrap_clock`; `cfg.validate_auth(server_profile)` before it |
| `server.rs : start_inner` (after `ClusterServices::start`, before the bridge) | `p14::build_ctx` |
| `server.rs : start_inner` → `BridgeEnv.audit` | `p14::audit_sink` → `BridgeServices.resolver: Audited<SecretResolver>` (`bridge_services.rs : BridgeServices::build`) |
| `server.rs : start_inner` (after `p7::init`) | `p7.shared.join.attach_p14` → `workers/join.rs : JoinService::{join, renew, revoke_token}` → `p14::worker_event` (Ok and Failed outcomes) |
| `server.rs : start_inner` (after `Node::open`) | `P14Ctx::attach_node(blobs, node.roles())` |
| `server.rs : start_inner` (after `ServerControlPlane` construction) | `p14::attach` |
| `server.rs : start_inner` (`ApiConfig` literal) | `p14::authenticator` |
| `server.rs` → `p11.rs : wire` (slot closure, before Phase 13 routing) | `p14::fill_extensions` then `p13::fill_extensions` |
| `server.rs` → `p11.rs : wire` (`front` closure → `p11::ApiFront`) | `p14::wrap_ingress`, `p14::ui_auth`, `p14::ui_scope_layer` (REST + gRPC via `router_with`, `/ui` via `UiScopeLayer::apply`) |
| `server.rs : start_inner` (after `p13::start`) | `p14::start` (result applied once the handle exists; Err → `handle.shutdown()` + `ServerError::Config`) |
| `server.rs : start_inner` (end, routed instance + `node.roles()`) | `p14::start_triggers` (Err → shutdown + `ServerError::Config("triggers: …")`; Some(task) joins `P14Services`) |
| `server.rs : ServerHandle::shutdown` (first) | `p14::before_shutdown`; then after the Phase 13 tasks: `P14Services::stop`, drop the context (before meta stops) |
| `control/modules.rs : upload_module` (right after the sha check + tenant/name qualification) | `p14::upload_gate` (before `s.module(&sha)`, compile, meta writes and the auto-activation `MetaRecord::Active` put) |
| `control/modules.rs : upload_module` (existing-sha branch) | `p14::register_existing` (Deny → error) |
| `control/modules.rs : upload_module` (new record / existing sha) | `p14::evidence_ext` / `p14::merge_evidence` |
| `control/modules.rs : activate_workflow` (after `check_not_retired`) | `p14::activation_gate` (tenant from the qualified name) |
| `control/runs.rs : start_run` (explicit `req.version`) | `p14::version_gate` |
| `control/runs.rs : start_run` (after a dedupe-row read, before the rate limit) | `p14::admit_ingress(Start { dedupe_hit, trigger: TriggerKind::of_key(idem) })` |
| `control/migrate.rs : migrate_runs` (target) | `p14::version_gate` |
| `control/p11_rerun.rs : rerun` (every target: source pin, explicit, active) | `p14::version_gate` |
| `control/lifecycle.rs : signal_run`, `complete_token` (after the dry-run return) | `p14::admit_ingress(Signal / TokenComplete, run tenant)` |
| `crates/ostro/src/main.rs : main` | `ostro auth bootstrap --data-dir` → `p14::bootstrap::offline`; `ostro audit verify --data-dir` → `audit::verify::offline` |

`retry_run` has no version override (14-01 inventory): no gate. `workers/service.rs` needed no edit (stream renewal calls `JoinService::renew`). `rerun.rs` needed no edit (the version override is in `control/p11_rerun.rs`, as 14-01 recorded).

**In-process consumers that keep the routed (unguarded) instance** (`ControlCtx::routed`): webhook deliveries (`control/webhooks.rs`, `HookIngress::deliver`), schedule manual trigger (`control/schedules.rs`), DLQ requeue (`control/p11_dlq.rs`), the `on_failure` hook service (`services/dlq_hook.rs`), bulk jobs (`services/bulk.rs`), the node API on the cluster listener (`route::wrap_extensions` → `node_api_router`), and the Kafka trigger (`start_triggers` receives `routed`). Only REST, gRPC and `/ui` get `Guarded`.

## Tools (scripts/fetch-tools.sh → target/tools/<tool>-<version>/)

| Tool | Version | Artifact | Checksum |
|---|---|---|---|
| kubeconform | 0.8.0 | kubeconform-linux-amd64.tar.gz | sha256 9bc2bffbf71f261128533edaf912153948b7ff238f9a531ae6d34466ec287883 |
| crane | 0.22.1 | go-containerregistry_Linux_x86_64.tar.gz | sha256 0ab7a1d6932a213aed964ce97666c3077fe691c8606413674a8b3e0b9ec4cda0 |
| shellcheck | 0.11.0 | shellcheck-v0.11.0.linux.x86_64.tar.gz | sha256 b7af85e41cc99489dcc21d66c6d5f3685138f06d34651e6d34b42ec6d54fe6f6 |
| rcodesign (optional) | 0.29.0 | apple-codesign-0.29.0-x86_64-unknown-linux-musl.tar.gz | sha256 dbe85cedd8ee4217b64e9a0e4c2aef92ab8bcaaa41f20bde99781ff02e600002 |
| weed (SeaweedFS) | 4.54 | linux_amd64.tar.gz | sha256 e7424a82eee327809bfa449fda4924c9a0f42f6669a01288eebb1b79674c8196 |
| kafka (Apache Kafka) | 4.1.2 | kafka_2.13-4.1.2.tgz (archive.apache.org, downloads.apache.org fallback) | sha512 78ac6e488b1071122f9608dfdb363f6fe50e1dbbc492347002c0398dfbf77e0d8caa5bf794c5937379721004dfe92025937d238b0faf4eb715529417fd43b491 |
| jre (Eclipse Temurin) | 21.0.13.1+2 | OpenJDK21U-jre_x64_linux_hotspot_21.0.13.1_2.tar.gz | sha256 18f44fab9065217f3ba61d0eef8a7b5f3c3713aa7293001b0d4ac10ede3850f0 |
| cargo-fuzz | 0.13.2 | `cargo install --locked --root target/tools/cargo` | crates.io lock |
| rsign2 | 0.6.7 | `cargo install --locked --root target/tools/cargo` (binary `rsign`) | crates.io lock |

Checksums from the upstream release pages on 2026-11-26 (GitHub asset digests, rcodesign's `.sha256`, Apache's `.sha512`, Adoptium's API). All fetched and verified locally (`cargo-fuzz 0.13.2`, `rsign2 0.6.7`, `apple-codesign 0.29.0` print their versions); idempotent (marker `.ostro-tool-ok`); Linux x86_64 only.

## Test services (scripts/test-services.sh)

- `start s3`: `weed mini` on 127.0.0.1, every port (S3, master, volume, filer, admin + their gRPC ports, vector) chosen free at start, webdav/admin UI/iceberg/lance off, `-bucket=ostro-test`, credentials `ostro`/`ostroseaweed`; readiness = a signed HEAD on the bucket (signed PUT fallback, the ci.yml logic). **Host curl 7.81 cannot SigV4-sign for SeaweedFS** (it omits `x-amz-content-sha256`; 403 `SignatureDoesNotMatch`), so the script falls back to an inline `python3 -I` signer when curl fails.
- `start kafka`: KRaft combined (`kafka-storage.sh format --standalone --add-scram SCRAM-SHA-256/512`), `-Xmx512m`, listeners PLAINTEXT / SASL_PLAINTEXT (PLAIN + SCRAM-SHA-256 + SCRAM-SHA-512, user `ostro`, random password) / SSL (fresh keytool RSA CA + server cert, SAN `IP:127.0.0.1,DNS:localhost`, PKCS12; CA PEM under the state dir) on free loopback ports; readiness `kafka-broker-api-versions.sh` on PLAINTEXT within 90 s (ready in 2 s locally). Client property files for each mechanism and SSL are written next to the broker (`client-{plain,scram256,scram512,ssl}.properties`).
- `env` prints `export OSTRO_TEST_S3_{URL,ACCESS_KEY,SECRET_KEY,BUCKET}` and `export OSTRO_TEST_KAFKA_{BOOTSTRAP,SASL_BOOTSTRAP,SASL_USER,SASL_PASSWORD,TLS_BOOTSTRAP,TLS_CA}`; `stop` TERM→KILL only its pidfile PIDs and removes the data; `status`. shellcheck 0.11.0 clean.

**Local check of all three Kafka listeners** (`kafka-broker-api-versions.sh` with the matching client properties): PLAINTEXT rc=0, SASL_PLAINTEXT PLAIN rc=0, SCRAM-SHA-256 rc=0, SCRAM-SHA-512 rc=0, SSL (truststore = the per-start CA) rc=0; a wrong SCRAM password rc=1; `openssl s_client -CAfile $OSTRO_TEST_KAFKA_TLS_CA` → `Verify return code: 0 (ok)`, `subject=CN=localhost`.

**Kafka inside an `ostro-k8s-ci` pod:** **yes, no fallback needed** (D-P14-44's Redpanda route not taken). Slot 1, job `ostro-ci-s1-9dde2787-151919-14909` (node dell-2, image curl 8.5.0, python3 3.12.3, openssl present): `fetch-tools.sh kafka jre weed shellcheck` → `test-services.sh start kafka` ready in 3 s (PLAINTEXT, SASL_PLAINTEXT, SSL on free ports; all six `OSTRO_TEST_KAFKA_*` printed) → stop → `start s3` ready in 1 s (all four `OSTRO_TEST_S3_*`) → stop; shellcheck clean; exit 0 in 95 s. Log: `target/k8s-ci-logs/20261126-151919-s1-9dde2787-cmd.log`.

## Verification

- Task 1 verify: PASS (`cargo build --workspace --all-targets --locked` warning-free; `--test config` 54 passed, `--test p14_skeleton` 7 passed; greps).
- Task 2 verify: PASS for the build, the greps and the order check (upload_gate @11895 < `s.module(&sha)` @12072 < register_existing @12314 < first `MetaRecord::Active` put @18065). **Adapted:** the plan's `act` regex also matches the `ActiveVersion` import in the `use` block at the top of modules.rs (before any function), so the check was run over the body from `upload_module` on (the plan allows updating the patterns). Tests run locally: `ostro-server` (`p14_skeleton`, `modules_api`, `runs_api`, `start_run_api`, `sink`), `ostro-cli`, `ostro-client`, `ostro-ui`, `ostro-api`: all green (40 result lines, 0 failures); the full `ostro-server` suite ran in the plan gate.
- Task 3 verify: PASS (`scratch verify3.sh`), with two adaptations: the signed HEAD falls back to a python3 SigV4 signer because the host curl 7.81 cannot sign for SeaweedFS (see above); the plan's naive FAILPOINT diff (`awk '{print $4}'` over ci.yml vs the `crate:target` entries) fails at the base commit cb38add as well, so the authoritative `check_failpoint_sync` from ci-local was run: **in sync, no ci.yml change**. 15 bins build on nightly-2026-11-25 (`fuzz/Cargo.lock` deleted again, 14-07 owns it); no manifest changed since the base; CLOCK_GATE_SCOPED empty.
- `cargo clippy --workspace --all-targets --locked -- -D warnings` clean; `cargo fmt --all --check` clean.
- Plan gate `ostro-k8s-ci --parallel`: run `p151733-313` at b4c514a (worktree snapshot b5deb616, same tree), wall 102m31s (heavy load: test2 27.6 GiB, musl2 23.6 GiB peak on dell-1). Every step PASS — toolchain, fmt, clippy, warning-free build, sdk workspace, guests, order-saga, `cargo test` (test1+test2, incl. the full ostro-server suite and `p14_skeleton`), polyglot/clients, ostro-build polyglot, semantics sleepers, failpoint list sync, failpoint suites, replication mutant matrix, Phase 13 mutant matrix, wit-next fixture, **Phase 13 tier C**, server campaign smoke (20 kills), phase 7 legs, deny, vendored, grep gates, workflow validation, zig, musl release — except two, neither a product failure:

| Step | Failure in the parallel run | Resolution |
|---|---|---|
| musl binary size limit (lint group) | `musl binary missing: target/x86_64-unknown-linux-musl/release/ostro` — the runner still maps `musl binary size < 60 MB` to musl1, so the renamed step fell into the rest group, which has no musl release build (expected, see Orchestrator actions 1) | slot 1 job `ostro-ci-s1-ca568c0d-153616-14675`: `CI_LOCAL_ONLY` = zig + musl release + **musl binary size limit** → PASS / PASS (706 s) / **PASS** (limit read from `scripts/binary-size-limit`) |
| musl zigbuild test (musl2) | `ostro-server sdk_e2e`: 8/9 `server start timed out` (20 s budget, 8 servers booting at once under the musl2 load); the same suite passed on gnu in test2 (9/9, 11 s) | rerun alone on slot 1 under musl (`cargo-zigbuild test --target x86_64-unknown-linux-musl -p ostro-server --test sdk_e2e`): **9/9 PASS twice** (10.5 s, 10.1 s) — load only |

Logs: `target/k8s-ci-logs/20261126-151733-par-b5deb616-*.log`, `…-153616-s1-ca568c0d-cmd.log`, `…-170036-s1-77d63745-cmd.log`.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] The REST/gRPC/UI construction lives in `p11::wire` (not in files_modified)**
- `p11.rs : wire` gained a `front: &dyn Fn(Arc<dyn ControlPlane>, ApiExtensions) -> ApiFront` parameter and the `ApiFront { guarded, ui_auth, ui_layer }` struct; `server.rs` builds it from `p14::wrap_ingress` / `ui_auth` / `ui_scope_layer`, so the grep-visible call sites stay in `server.rs` and in-process consumers keep `cc.routed`. Commit 57c5da2.

**2. [Rule 3 - Blocking] Control-plane hooks need the context: `control/mod.rs` gains `ServerControlPlane.p14` (weak `OnceLock`)** — one field + initializer. Commit 62c003f.

**3. [Rule 3 - Blocking] Contract gaps in ostro-api for the final call sites**
- `ApiConfig.authenticator` ("`p14::authenticator` into ApiConfig"; 14-02's plan lists it as an existing 14-01 contract) did not exist: added in `ostro-api/src/lib.rs` with an `AllowDev` default (manual `Debug`/`PartialEq`/`Eq`); `route/mod.rs : api_config` takes the default. Every test literal already used `..Default`.
- `UploadModule.signatures` (the `sigs` of `upload_gate`; 14-02 "passes to UploadModule.signatures" but owns neither `domain.rs` nor the literals): added; 9 literals updated (`rest.rs`, `grpc.rs`, `fake.rs`, ostro-api tests `parity.rs`, `grpc_client_parity.rs`, `phase7_surface.rs`, `ostro-ui/tests/common/mod.rs`) with `signatures: Vec::new()`.
- Commit 57c5da2.

**4. [Rule 3 - Blocking] Bridge construction: `BridgeEnv.audit`** — `bridge_services.rs` (named by D-P14-33 as 14-01b's site) plus the one `BridgeEnv` literal in `tests/sink.rs`. Commit 57c5da2.

**5. [Rule 2 - Missing critical functionality] The shared bearer is actually sent** — `ostro-client/src/client.rs` (not listed): `Client` reads `OSTRO_TOKEN` once (`ostro_client::bearer_from_env`) and sends `Authorization: Bearer` on every request (`Client::with_bearer` overrides; header marked sensitive). Without it every existing CLI verb would 401 once 14-03 lands, and no later plan owns `client.rs`. Commit 57c5da2.

**6. Seam shapes adjusted to make the final seam workable** (plan text → landed): `build_ctx`/`start`/`start_triggers` return `Result<_, P14Error>`; `ui_scope_layer` also takes the `UiAuth`; `wrap_clock` is over `Arc<dyn Clock>`; `evidence_ext`/`merge_evidence` added (the upload path writes ext 113 evidence, 14-19); `attach`/`ctx_of`, `P14Ctx::attach_node`, `P14Ctx::shutdown` added; `TriggerKind::of_key` + `KAFKA_KEY_PREFIX`/`CRON_KEY_PREFIX`. `start_triggers` runs once the server handle exists (the Phase 8 cron drivers start inside `Node::open`, before the routed instance exists) on the routed instance and `node.roles()`; `p14::start` runs right after `p13::start` and its error is applied once the handle exists (both failure paths shut the server down through the handle).

**7. `admit_ingress` on starts reads the dedupe row** for keyed starts (one point read, like the rate limiter's) so `dedupe_hit` is real for 14-20; dry runs return before admission.

### Plan text vs code (recorded, no change)
- `control/signals.rs` / `control/tokens.rs` do not exist (14-01): signal/token admission is in `control/lifecycle.rs`; `rerun.rs` has no version path (it is `control/p11_rerun.rs`).
- `ListRunsRequest.tenant` (14-02's tenant list filter) is also in `ostro-api/src/domain.rs`, which is not in 14-02's files — not needed by 14-01b, flagged for 14-02.
- No `.github/workflows` change was needed: FAILPOINT_CRATES and ci.yml's Failpoint suites step are in sync.

## Workflow hunks

None. No `wip/14-01b-workflows` branch was created (ci.yml needs no sync change).

## Orchestrator actions / decisions

1. **ostro-k8s-ci `G_STEPS[musl1]`** must be renamed from `musl binary size < 60 MB` to **`musl binary size limit`** in the same push as b4c514a (D-P14-54). Confirmed by the gate: with the old mapping the step is skipped in musl1 and runs (and fails: no binary) in the rest group (lint); once renamed it passes in musl1 (proven on slot 1).
2. **Secret-access audit gap (14-18):** only `BridgeServices`' default resolver is wrapped in `Audited`; the connectors (`TenantHttp` per-tenant `HttpConnector`, `SqlConnector`, `ObjectConnector`) take a concrete `SecretResolver` (ostro-bridge types), so the run-time `{{secret:…}}` resolutions 14-18 must coalesce never pass through `Audited`. Closing it needs an ostro-bridge change (connectors generic over/holding `Audited<SecretResolver>`, or an audit hook inside `SecretResolver`) that no Phase 14 plan owns (14-18 lists only `secrets/audited.rs`). Decision needed: extend 14-18's files or add it to another plan.
3. **Nightly-only warning for 14-07:** on nightly-2026-11-25 `ostro-server` builds with one future-incompat warning `recursion_depth_exceeding_limit` (auto-trait cycle `ServerControlPlane → DevClock → DevTime → Weak<ServerControlPlane>`, Phase 10 types); stable 1.100 is clean. Not caused by 14-01b (the cycle has no Phase 14 type); 14-07 should keep an eye on it if it builds with `-D warnings` on nightly.
4. **New load-sensitive test (not in the brief):** ostro-server `sdk_e2e` under musl (8 servers boot at once, 20 s start budget) timed out in musl2 at ~24 GiB load; passes alone 9/9 twice. Candidate for the brief's known list.
5. **Host curl 7.81** (WSL) cannot SigV4-sign for SeaweedFS (no `x-amz-content-sha256`); the cluster image has curl 8.5.0 and python3. Local scripts that sign with `curl --aws-sigv4` (e.g. the 14-01b verify line) need the python fallback that `test-services.sh` uses.

## Self-Check

PASSED — all 14 key created files present plus the 15 `fuzz/fuzz_targets/*.rs`; commits 62c003f, 57c5da2, b4c514a are ancestors of main (a concurrent p13-campaign commit 37a0e8a landed between 62c003f and 57c5da2; it touches only `ostro-repl-conformance/src/harness/{inproc,process}.rs`).
