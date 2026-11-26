---
phase: 14-multi-tenancy-security-1-0-release
plan: 01b
type: execute
wave: 2
depends_on: ["14-01"]
files_modified:
  - crates/ostro-server/src/lib.rs
  - crates/ostro-server/src/server.rs
  - crates/ostro-server/src/config.rs
  - crates/ostro-server/src/p14/mod.rs
  - crates/ostro-server/src/p14/guard.rs
  - crates/ostro-server/src/p14/tokens.rs
  - crates/ostro-server/src/p14/authn.rs
  - crates/ostro-server/src/p14/bootstrap.rs
  - crates/ostro-server/src/p14/signing.rs
  - crates/ostro-server/src/p14/quota_view.rs
  - crates/ostro-server/src/p14/test_hooks.rs
  - crates/ostro-server/src/p14/authz.rs
  - crates/ostro-server/src/p14/principal_fwd.rs
  - crates/ostro-server/src/p14/tenant_scope.rs
  - crates/ostro-server/src/p14/upload_tenant.rs
  - crates/ostro-server/src/p14/tenants_api.rs
  - crates/ostro-server/src/p14/tokens_api.rs
  - crates/ostro-server/src/p14/audit_hooks.rs
  - crates/ostro-server/src/p14/audit_api.rs
  - crates/ostro-server/src/p14/module_sign.rs
  - crates/ostro-server/src/p14/security_api.rs
  - crates/ostro-server/src/p14/reseal.rs
  - crates/ostro-server/src/p14/ingress.rs
  - crates/ostro-server/src/p14/ui_auth.rs
  - crates/ostro-server/src/p14/kafka.rs
  - crates/ostro-server/src/audit/mod.rs
  - crates/ostro-server/src/audit/journal.rs
  - crates/ostro-server/src/audit/segment.rs
  - crates/ostro-server/src/audit/chain.rs
  - crates/ostro-server/src/audit/writer.rs
  - crates/ostro-server/src/audit/verify.rs
  - crates/ostro-server/src/audit/export.rs
  - crates/ostro-server/src/flow/mod.rs
  - crates/ostro-server/src/flow/tenant.rs
  - crates/ostro-server/src/control/runs.rs
  - crates/ostro-server/src/control/modules.rs
  - crates/ostro-server/src/control/migrate.rs
  - crates/ostro-server/src/control/signals.rs
  - crates/ostro-server/src/control/tokens.rs
  - crates/ostro-server/src/control/lifecycle.rs
  - crates/ostro-server/src/rerun.rs
  - crates/ostro-server/src/workers/join.rs
  - crates/ostro-server/src/workers/service.rs
  - crates/ostro-server/tests/config.rs
  - crates/ostro-server/tests/p14_skeleton.rs
  - crates/ostro/src/main.rs
  - crates/ostro-client/src/lib.rs
  - crates/ostro-client/src/tenants.rs
  - crates/ostro-client/src/api_tokens.rs
  - crates/ostro-client/src/audit.rs
  - crates/ostro-client/src/security.rs
  - crates/ostro-cli/src/commands/mod.rs
  - crates/ostro-cli/src/commands/tenant.rs
  - crates/ostro-cli/src/commands/auth.rs
  - crates/ostro-cli/src/commands/audit.rs
  - crates/ostro-cli/src/commands/security.rs
  - crates/ostro-repl-conformance/src/scenarios/mod.rs
  - crates/ostro-repl-conformance/src/scenarios/p14.rs
  - fuzz/fuzz_targets/
  - fuzz/regressions/README.md
  - scripts/fetch-tools.sh
  - scripts/test-services.sh
  - scripts/binary-size-limit
  - scripts/lib/binary-size.sh
  - scripts/ci-local.sh
  - .github/workflows/ci.yml
autonomous: true
requirements: [SEC-02, SEC-03, SEC-04, SEC-05, DISP-06, QA-04, QA-05, DIST-01, DIST-02, DIST-03, TRIG-04, BACK-05]

must_haves:
  truths:
    - "The p14 seam (final after this plan) and every stub exist with fixed signatures; every stub preserves Phase 13 behaviour except that the default principal outside any scope is Anonymous (14-01 contract)"
    - "Hook call sites are in place: upload_gate right after the sha check and before the existing-sha lookup and auto-activation; version_gate on every path that selects a non-active version or sha (start with explicit version, migrate target, rerun/retry version overrides); activation_gate; admit_ingress after the dedupe lookup; register_existing at the existing-sha branch; audited resolver; worker events; wrap_clock"
    - "The /ui router runs handlers inside PRINCIPAL.scope(Principal::dev()) in auth mode none and delegates to UiAuth in token mode, so Phase 11 UI suites stay green behind the deny-by-default guard"
    - "main.rs intercepts `ostro auth bootstrap --data-dir` and `ostro audit verify --data-dir` (offline commands that need server internals)"
    - "Config sections [auth], [tenants] (metrics_tenant_labels default false), [audit] (head_export_interval), [security.modules] and the [triggers] kafka array (raw [[triggers.kafka]] tables, empty by default) parse in both profiles"
    - "The trigger seam exists: IngressKind::Start carries TriggerKind::{Cron, Webhook, Kafka}; p14::start_triggers (stub in p14/kafka.rs) is called where the Phase 8 cron/webhook drivers start; a non-empty [[triggers.kafka]] list fails startup with 'Kafka triggers land in 14-32' until then"
    - "scripts/test-services.sh starts/stops loopback SeaweedFS (weed mini) and Apache Kafka (KRaft, pinned JRE; PLAINTEXT + SASL_PLAINTEXT (PLAIN/SCRAM) + SSL listeners) from target/tools and prints OSTRO_TEST_S3_* / OSTRO_TEST_KAFKA_{BOOTSTRAP,SASL_*,TLS_*} (D-P14-44/46)"
    - "Fuzz target bins build on the pinned nightly; scripts/fetch-tools.sh pins every tool with sha256; FAILPOINT_CRATES equals the ci.yml list; no manifest is edited by this plan"
  artifacts:
    - path: "crates/ostro-server/src/p14/mod.rs"
      provides: "the single Phase 14 seam"
      contains: "pub async fn version_gate"
    - path: "scripts/fetch-tools.sh"
      provides: "pinned, sha256-verified tool downloads into target/tools"
      contains: "kubeconform"
  key_links:
    - from: "crates/ostro-server/src/server.rs"
      to: "crates/ostro-server/src/p14/mod.rs"
      via: "wrap_ingress at the single API/UI construction site; start; before_shutdown"
      pattern: "p14::wrap_ingress"
    - from: "crates/ostro-server/src/control/modules.rs"
      to: "crates/ostro-server/src/p14/mod.rs"
      via: "upload_gate before the existing-sha lookup"
      pattern: "p14::upload_gate"
---

<objective>
**PHASE-14 GATE, part 2 of 2** (wave 2, after 14-01; every later plan depends on it). Land the server seam (`p14/`, `audit/`, `flow/tenant.rs`) with every fixed call site, config sections, CLI/client stubs, the fuzz target bins, the conformance scenario module, the tool pins and the CI sync check. No manifest is edited here (14-01 owns them).

Purpose: later waves edit only their own hook files (D-P14-33).

Output: seam, stubs, call sites, config, CLI/client stubs, fuzz bins, `scripts/fetch-tools.sh`, `scripts/test-services.sh`, CI sync.
</objective>

<execution_context>
@~/.claude/get-shit-done/workflows/execute-plan.md
@~/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/phases/14-multi-tenancy-security-1-0-release/14-DECISIONS.md
@.planning/phases/14-multi-tenancy-security-1-0-release/14-01-SUMMARY.md
@crates/ostro-server/src/server.rs
@crates/ostro-server/src/p13/mod.rs
@crates/ostro-api/src/auth.rs
@scripts/ci-local.sh

<phase_rules>
- **Ordinals/names only from the P14 block** (rows added by 14-01). No allocation here.
- **Frozen:** additive only; stubs pass through.
- Injected clock only; CLOCK_GATE_SCOPED on this plan's diff.
- Build warning-free; clippy `-D warnings`; the plan-level gate is `ostro-k8s-ci --parallel <worktree>` (scripts/ci-local.sh on the microk8s `ostro-ci` namespace, Bash `run_in_background: true`, durable brief). **No Cargo edits** (a missing dependency → STOP; 14-01's matrix was incomplete). No chrono, bincode, openssl, sigstore. No Docker; the microk8s cluster is used only through `ostro-k8s-ci` (namespace `ostro-ci`, slots 0/1 or `--parallel`, never slot 2; no kubectl, no product deploys, no helm install). Never reference a Chronik clone path. Executors never push or add remotes (the orchestrator fast-forwards github.com/lspecian/ostro main).
- **Workflow files (D-P14-41):** per the durable brief executors do not edit `.github/workflows/*`. Write and validate any `ci.yml` sync hunk in the worktree; never commit it to main: commit it on `wip/14-01b-workflows` and copy the file/hunk into the SUMMARY (`## Workflow hunks`); the orchestrator applies it (user decision 2026-11-26, D-P14-53). The verify's workflow greps run against the worktree copy either way.
- Worktree-only per the durable executor brief (`/home/lspecian/.local/share/ostro-orchestrator/exec-brief.md`; 14-DECISIONS "Commit procedure"): worktree `/home/lspecian/.local/share/ostro-orchestrator/wt/14-01b` from current main, the assigned `CARGO_TARGET_DIR=target/slot-N` (`CARGO_INCREMENTAL=0`, `nice -n 10`, `CARGO_BUILD_JOBS=6`), private index `/home/lspecian/.local/share/ostro-orchestrator/idx-14-01b`, CAS `update-ref` onto main, stale-blob rule, final-HEAD re-check; only `files_modified` (control/rerun/lifecycle file names are those recorded by 14-01; exhaustive-match arms elsewhere are deviations); Co-Authored-By trailer; no gsd-tools mutators.
- Verify commands start with `cd /home/lspecian/.local/share/ostro-orchestrator/wt/14-01b;`, use `set -euo pipefail`, filters after `--`, no `|| true`, negations as `if …; then exit 1; fi`, nonzero test count.
</phase_rules>
</context>

<tasks>

<task type="auto">
  <name>Task 1: p14 seam + server stubs (p14/*, audit/*, flow/tenant.rs) + config sections</name>
  <files>crates/ostro-server/src/{lib,config}.rs, crates/ostro-server/src/p14/*.rs, crates/ostro-server/src/audit/*.rs, crates/ostro-server/src/flow/{mod,tenant}.rs, crates/ostro-server/tests/{config,p14_skeleton}.rs</files>
  <action>
**`p14/mod.rs` (final after this plan):**
```rust
pub struct P14Ctx { /* cfg, meta: MetaHandle, clock, node_id, incarnation, data_dir, audit: Arc<dyn AuditSink>, quotas: Arc<quota_view::TenantQuotaView>, blobs, level_max */ }
pub struct Guarded { pub control: Arc<dyn ControlPlane>, pub ext: ApiExtensions }
pub fn build_ctx(..) -> P14Ctx                                    // audit sink = audit::writer::open (stub: NoopAuditSink)
pub fn authenticator(ctx) -> Arc<dyn Authenticator>               // → authn (14-03); stub = AllowDev
pub fn wrap_ingress(ctx, routed, routed_ext) -> Guarded           // → guard::wrap (14-03); stub = passthrough clone
pub fn fill_extensions(ctx, ext: &mut ApiExtensions)              // 14-17/18/19 impls; stub leaves None
pub fn ui_auth(ctx) -> Arc<dyn UiAuth>                            // → ui_auth (14-21); stub = AllowAll
pub fn ui_scope_layer(ctx) -> UiScopeLayer                        // N2: mode none → PRINCIPAL.scope(Principal::dev()); token mode → UiAuth principal (14-21)
pub fn wrap_clock(clock: Clock) -> Clock                          // → test_hooks (14-10, failpoints only); stub = identity
pub async fn start(ctx: Arc<P14Ctx>) -> P14Services               // audit writer (14-04), bootstrap (14-03), reseal (14-19), quota view (14-06)
pub async fn before_shutdown(ctx)                                 // audit + coalescer flush
pub enum TriggerKind { Cron, Webhook, Kafka }                    // revision 2026-11-26: Kafka (14-32; 14-20 treats it like Webhook). StartRunRequest has no trigger field: the start-path call site derives Kafka from the `__kafka/` idempotency-key prefix (and Cron/Webhook from their recorded markers)
pub enum IngressKind { Start { dedupe_hit: bool, trigger: Option<TriggerKind> }, Signal, TokenComplete }
pub async fn start_triggers(ctx, routed: Arc<dyn ControlPlane>, roles: PartitionRoles /* or the leadership watch recorded by 14-01 */) -> Option<JoinHandle<()>>  // → kafka (14-32); stub: None for an empty [[triggers.kafka]] list, startup error otherwise
pub async fn admit_ingress(ctx, kind: IngressKind, tenant: Option<&str>) -> Result<(), ApiError>          // → ingress (14-20)
pub async fn upload_gate(ctx, tenant: Option<&str>, sha: [u8;32], bytes: &[u8], sigs: &[Vec<u8>]) -> Result<UploadEvidence, ApiError> // → module_sign (14-19)
pub async fn version_gate(ctx, tenant: Option<&str>, workflow: &str, version: u32, sha: [u8;32]) -> Result<(), ApiError>   // → module_sign (14-19)
pub async fn activation_gate(ctx, tenant: Option<&str>, sha: [u8;32]) -> Result<(), ApiError>             // → module_sign (14-19)
pub async fn register_existing(ctx, sha: [u8;32], tenant: Option<&str>) -> RegisterDecision               // → upload_tenant (14-16); stub = Phase 13 behaviour
pub fn worker_event(ctx, ev: WorkerAuditEvent)                                                            // → audit_hooks (14-18)
pub fn audit_sink(ctx) -> Arc<dyn AuditSink>
```
Hook stubs: `guard::wrap` passthrough; `authz::require(&Principal, Method) -> Result<(), ApiError>` → Ok; `tenant_scope::{before, after}` passthrough; `audit_hooks::{reserve → Ok(None), after → Ok}`; `bootstrap::offline(args)` and `audit::verify::offline(args)` → "not implemented until 14-03/14-04"; every other file a stub with `//! Phase 14: implemented in 14-NN` and its fixed signature.

**Config (append only, `deny_unknown_fields` stays):** `[auth] mode ("none"|"token"; dev none, server token), allow_insecure_none = false, bootstrap_token_file, default_token_ttl = "2160h", ui_session_ttl = "8h"`; `[tenants] metrics_label_limit = 1000, metrics_tenant_labels = false, quota_share = "leadership"`; `[audit] enabled = true, dir, segment_bytes = 67108864, queue = 4096, group_commit = "5ms", reserve_timeout = "2s", retention = "2160h", max_bytes = 10737418240, secret_coalesce = "60s", auth_failure_coalesce = "60s", head_export_interval = "1h", otlp = true`; `[security.modules] require_signed = false, minisign_keys = [], cosign_keys = []`; inside the existing Phase 8 `[triggers]` section a new key `kafka` = array of raw tables (`[[triggers.kafka]]`, the config crate's value type; default empty; field-level validation is 14-32's `kafka::validate`, called from `start_triggers` before anything starts). Server profile rejects `mode = "none"` on a non-loopback listener unless `allow_insecure_none`. Parse tests in both profiles (incl. a `[[triggers.kafka]]` table that parses into the raw array); `p14_skeleton.rs` asserts the seam links, `auth::current()` outside a scope is Anonymous, and `start_triggers` with an empty Kafka list returns None.
Commit `feat(14-01b): p14 seam, stubs and config`.
  </action>
  <verify>
    <automated>cd /home/lspecian/.local/share/ostro-orchestrator/wt/14-01b; bash -c 'set -euo pipefail; cargo build --workspace --all-targets --locked; for t in "--test config" "--test p14_skeleton"; do out=$(cargo test -p ostro-server --locked $t -- 2>&1); grep -Eq "test result: ok\. [1-9][0-9]* passed" <<<"$out"; done; grep -q "pub async fn version_gate" crates/ostro-server/src/p14/mod.rs; grep -q "metrics_tenant_labels" crates/ostro-server/src/config.rs; grep -q "head_export_interval" crates/ostro-server/src/config.rs'</automated>
  </verify>
  <done>The seam, stubs and config exist and compile.</done>
</task>

<task type="auto">
  <name>Task 2: Call sites (server.rs, control/*, rerun, migrate, workers, bridge, clock) + main.rs intercepts + CLI/client stubs + conformance scenario module</name>
  <files>crates/ostro-server/src/server.rs, crates/ostro-server/src/control/{runs,modules,migrate,signals,tokens,lifecycle}.rs, crates/ostro-server/src/rerun.rs, crates/ostro-server/src/workers/{join,service}.rs, crates/ostro/src/main.rs, crates/ostro-client/src/{lib,tenants,api_tokens,audit,security}.rs, crates/ostro-cli/src/commands/{mod,tenant,auth,audit,security}.rs, crates/ostro-repl-conformance/src/scenarios/{mod,p14}.rs</files>
  <action>
**Call sites (one edit each):**
- `server.rs`: `p14::build_ctx` after meta + clock; `p14::wrap_clock` at the clock construction; `p14::authenticator` into `ApiConfig`; `p14::fill_extensions` then `p14::wrap_ingress` right after `p13::wrap_control_plane` — REST, gRPC and the UI router receive `Guarded`, in-process consumers keep the routed instances; `p14::ui_auth` into the UI router; `p14::start` after `p13::start`; `p14::start_triggers(ctx, routed, roles)` right where the Phase 8 cron/webhook drivers are started (the routed, unguarded instance, like them); `p14::before_shutdown` before `p13::before_shutdown`.
- `control/modules.rs` (B1b): `p14::upload_gate` **immediately after the sha is computed/checked, before the existing-module lookup (`s.module(&sha)` or the name recorded by 14-01), before compilation, before any meta write and before the auto-activation `Active` put** (the verify asserts all three orderings; if the lookup/put use different identifiers at HEAD, update the check's patterns to the recorded names); `p14::register_existing` at the existing-sha branch; `p14::activation_gate` in activation.
- `p14::version_gate` (B1a) at every non-active-version path from the 14-01 inventory: start with an explicit `version` (control/runs.rs), the migrate target (`control/migrate.rs`), rerun with a version override (`rerun.rs`) and retry/restart with a version override (lifecycle file recorded by 14-01). Called for **every** explicit version or sha selection whenever enforcement may apply (not only non-active ones); `version_gate` accepts immediately when the target is the active version or already has valid evidence, so a concurrent Activate cannot open a window (W3).
- `control/runs.rs` start path after the dedupe lookup, and the signal/token completion paths: `p14::admit_ingress`; the start path builds `IngressKind::Start{trigger}` with `Some(TriggerKind::Kafka)` when the idempotency key starts with `__kafka/` (unit test in `p14_skeleton.rs`: a `__kafka/x/t/0/1` key yields Kafka, `__hook/` Webhook, `__cron/` Cron, anything else None).
- Bridge resolver construction: wrap with `Audited::new(inner, p14::audit_sink(ctx))`. Worker join/renew/revoke sites: `p14::worker_event`.
- **UI scope layer (N2):** the `/ui` router gets a tower layer built in `p14::ui_scope_layer(ctx)` (declared in `p14/mod.rs`, implemented in `p14/ui_auth.rs`): with `[auth] mode = "none"` it runs every UI handler inside `PRINCIPAL.scope(Principal::dev(), …)`; in token mode it delegates to `UiAuth` (stub: AllowAll → Dev until 14-21). This keeps the Phase 11 UI working behind the deny-by-default guard from 14-03 on.
- `main.rs` (B5): intercept `ostro auth bootstrap --data-dir` → `ostro_server::p14::bootstrap::offline(args)` and `ostro audit verify --data-dir` → `ostro_server::audit::verify::offline(args)`.

**CLI/client stubs:** client modules `tenants`, `api_tokens`, `audit`, `security` (declared in `lib.rs`) returning `ClientError::Unimplemented`; shared `Authorization: Bearer $OSTRO_TOKEN` helper in `lib.rs`. `commands/mod.rs` variants `Tenant`, `Auth{Token{Create,Ls,Revoke,Rotate}, Whoami, Bootstrap}`, `Audit{Ls, Verify}`, `Security{Policy{Get,Set}, Trust{Add,Rm}}` with stub files (Bootstrap and Audit Verify are dispatched by main.rs).
**Conformance:** `scenarios/p14.rs` declared in `scenarios/mod.rs` (`pub fn all() -> Vec<Scenario> { vec![] }`, appended to `all()`).
Commit `feat(14-01b): Phase 14 call sites + CLI/client stubs`.
  </action>
  <verify>
    <automated>cd /home/lspecian/.local/share/ostro-orchestrator/wt/14-01b; bash -c 'set -euo pipefail; cargo build --workspace --all-targets --locked; grep -q "p14::wrap_ingress" crates/ostro-server/src/server.rs; grep -q "p14::before_shutdown" crates/ostro-server/src/server.rs; grep -q "p14::upload_gate" crates/ostro-server/src/control/modules.rs; grep -q "p14::version_gate" crates/ostro-server/src/control/migrate.rs; grep -q "p14::version_gate" crates/ostro-server/src/control/runs.rs; grep -q "p14::admit_ingress" crates/ostro-server/src/control/runs.rs; grep -q "audit::verify::offline" crates/ostro/src/main.rs; grep -q "bootstrap::offline" crates/ostro/src/main.rs; python3 -I - crates/ostro-server/src/control/modules.rs <<"EOF"
import sys
s=open(sys.argv[1]).read()
g=s.index("p14::upload_gate"); r=s.index("p14::register_existing")
m=s.index("s.module(&sha)")
import re as _re
act=min(x.start() for x in _re.finditer(r"MetaRecord::Active|MetaKey::Active|ActiveVersion", s))
assert g < m, "upload_gate must precede the existing-module lookup s.module(&sha)"
assert g < act, "upload_gate must precede the auto-activation Active put"
assert g < r, "upload_gate must precede the existing-sha branch"
EOF
for t in "-p ostro-server" "-p ostro-cli" "-p ostro-client" "-p ostro-ui"; do out=$(cargo test $t --locked -- 2>&1); grep -Eq "test result: ok\. [1-9][0-9]* passed" <<<"$out"; done'</automated>
  </verify>
  <done>Every hook is wired at its fixed site with passthrough behaviour; offline commands are dispatched.</done>
</task>

<task type="auto">
  <name>Task 3: Fuzz target bins + nightly build, tool pins, test services, CI sync, plan gate</name>
  <files>fuzz/fuzz_targets/*.rs, fuzz/regressions/README.md, scripts/fetch-tools.sh, scripts/test-services.sh, scripts/binary-size-limit, scripts/lib/binary-size.sh, scripts/ci-local.sh, .github/workflows/ci.yml</files>
  <action>
- `fuzz/fuzz_targets/<target>.rs` for the 15 targets: `#![no_main] libfuzzer_sys::fuzz_target!(|data: &[u8]| ostro_fuzz_targets::run(ostro_fuzz_targets::Target::X, data));`; `fuzz/regressions/README.md` (policy: minimised crash inputs, one dir per target, replayed on stable).
- `scripts/fetch-tools.sh [tool…]`: into `target/tools/<tool>-<version>/`, each download into a fresh empty dir, sha256 verified before extraction, idempotent: kubeconform v0.8.0, crane v0.22.1, shellcheck v0.11.0 (sha256 from the release page, recorded), rcodesign 0.29.0 (optional), `cargo install --locked --version 0.13.2 cargo-fuzz` and `--version 0.6.7 rsign2` with `--root target/tools/cargo`; prints resolved paths; never touches system dirs. **Revision 2026-11-26 (D-P14-44/46):** also `weed` (SeaweedFS 4.54 `linux_amd64.tar.gz`, sha256 from the release page, recorded), `kafka` (Apache Kafka 4.1.x `kafka_2.13-4.1.<z>.tgz`, sha512 from downloads.apache.org, recorded) and `jre` (Eclipse Temurin 21 JRE linux x64 tarball, sha256 from its `.sha256.txt`, recorded).
- `scripts/test-services.sh {start [s3] [kafka] | env | stop | status}` (bash, shellcheck-clean): state under `${OSTRO_TEST_SERVICES_DIR:-target/test-services}`; `s3` = `weed mini -dir=<state>/s3` on 127.0.0.1 with `AWS_ACCESS_KEY_ID=ostro`, `AWS_SECRET_ACCESS_KEY=ostroseaweed`, bucket `ostro-test` (readiness = signed `curl --aws-sigv4` HEAD, else a signed PUT creates it, the ci.yml logic); `kafka` = KRaft combined mode (`kafka-storage.sh format --standalone --add-scram 'SCRAM-SHA-256=[name=ostro,password=<random>]' --add-scram 'SCRAM-SHA-512=[name=ostro,password=<same>]'`, `KAFKA_HEAP_OPTS=-Xmx512m`) with three client listeners on free loopback ports: `PLAINTEXT`, `SASL_PLAINTEXT` (mechanisms PLAIN + SCRAM-SHA-256 + SCRAM-SHA-512; PLAIN user `ostro` in the listener JAAS config with the same random password) and `SSL` (a fresh CA + server keypair per start via the JRE's `keytool` into a PKCS12 keystore, SAN `IP:127.0.0.1,DNS:localhost`; the CA certificate exported as PEM under the state dir); readiness = `kafka-broker-api-versions.sh` succeeds on the PLAINTEXT listener within 90 s; free ports chosen at start; pidfiles; `stop` kills only its own pidfile PIDs (no pattern kills); `env` prints `OSTRO_TEST_S3_URL/ACCESS_KEY/SECRET_KEY/BUCKET`, `OSTRO_TEST_KAFKA_BOOTSTRAP`, `OSTRO_TEST_KAFKA_SASL_BOOTSTRAP`, `OSTRO_TEST_KAFKA_SASL_USER`, `OSTRO_TEST_KAFKA_SASL_PASSWORD`, `OSTRO_TEST_KAFKA_TLS_BOOTSTRAP` and `OSTRO_TEST_KAFKA_TLS_CA` lines for `eval`/`.ostro-test-env`; it never touches Docker. ci-local wiring is 14-27's.
- **Shared binary size limit (D-P14-54):** create `scripts/binary-size-limit` (one line: `60000000`) and `scripts/lib/binary-size.sh` (`ostro_max_binary_bytes` = `${OSTRO_MAX_BINARY_BYTES:-<file value>}`, validated as an integer; `ostro_max_binary_mb` for messages); switch `check_musl_size` in `scripts/ci-local.sh` to source it (no literal 60000000 left), and rename the step to **"musl binary size limit"** in the header comment (line ~33) and in both the `step` and `skip` lines; the size line prints `limit=<MB>`. List in the SUMMARY that the orchestrator must rename `ostro-k8s-ci`'s `G_STEPS[musl1]` entry to the new step name in the same push.
- CI: `FAILPOINT_CRATES` unchanged unless it drifted from ci.yml (then fix both).
- Plan gate: `ostro-k8s-ci --parallel <worktree>` (scripts/ci-local.sh on the microk8s `ostro-ci` namespace, Bash `run_in_background: true`, durable brief) on the worktree.
Commit `build(14-01b): fuzz bins, tool pins, CI sync`.
  </action>
  <verify>
    <automated>cd /home/lspecian/.local/share/ostro-orchestrator/wt/14-01b; bash -c 'set -euo pipefail; test "$(ls fuzz/fuzz_targets/*.rs | wc -l)" -eq 15; tc=$(sed -n "s/^channel = \"\(.*\)\"/\1/p" fuzz/rust-toolchain.toml); cargo +"$tc" build --manifest-path fuzz/Cargo.toml --bins; bash scripts/fetch-tools.sh kubeconform crane weed; test -x target/tools/kubeconform-0.8.0/kubeconform; bash scripts/test-services.sh start s3; trap "bash scripts/test-services.sh stop" EXIT; eval "$(bash scripts/test-services.sh env)"; test -n "$OSTRO_TEST_S3_URL"; curl -sf --aws-sigv4 "aws:amz:us-east-1:s3" --user "$OSTRO_TEST_S3_ACCESS_KEY:$OSTRO_TEST_S3_SECRET_KEY" -o /dev/null -I "$OSTRO_TEST_S3_URL/$OSTRO_TEST_S3_BUCKET"; bash scripts/test-services.sh stop; trap - EXIT; bash scripts/fetch-tools.sh kafka jre; bash scripts/test-services.sh start kafka; trap "bash scripts/test-services.sh stop" EXIT; eval "$(bash scripts/test-services.sh env)"; for v in OSTRO_TEST_KAFKA_BOOTSTRAP OSTRO_TEST_KAFKA_SASL_BOOTSTRAP OSTRO_TEST_KAFKA_SASL_USER OSTRO_TEST_KAFKA_SASL_PASSWORD OSTRO_TEST_KAFKA_TLS_BOOTSTRAP OSTRO_TEST_KAFKA_TLS_CA; do test -n "${!v:-}"; done; test -s "$OSTRO_TEST_KAFKA_TLS_CA"; bash scripts/test-services.sh stop; trap - EXIT; grep -Eq "^[0-9]+$" scripts/binary-size-limit; grep -q "binary-size.sh" scripts/ci-local.sh; if grep -q "60000000" scripts/ci-local.sh; then exit 1; fi; grep -q "musl binary size limit" scripts/ci-local.sh; shellcheck_bin=$(ls target/tools/shellcheck-*/shellcheck 2>/dev/null | head -n1); if [ -n "$shellcheck_bin" ]; then "$shellcheck_bin" scripts/test-services.sh; fi; diff <(sed -n "s/^FAILPOINT_CRATES=(\(.*\))/\1/p" scripts/ci-local.sh | tr " " "\n") <(grep -oE "cargo test -p [a-z0-9-]+ --features failpoints" .github/workflows/ci.yml | awk "{print \$4}"); base=$(cat "$(git rev-parse --git-dir)/p14-base"); if git diff --name-only "$base" -- "*Cargo.toml" Cargo.lock | grep -q .; then exit 1; fi; hits=$(git diff -U0 "$base" -- crates/ostro-server/src crates/ostro-partition/src crates/ostro-bridge/src | grep -E "^[+]" | grep -E "SystemTime::now\(|SystemClock\b|Instant::now\(" | grep -v "clock-exempt:" || test $? -eq 1); test -z "$hits"'</automated>
  </verify>
  <done>Fuzz bins build, tools are pinned (incl. weed/Kafka/JRE), loopback SeaweedFS starts and answers a signed HEAD, CI is in sync, no manifest touched, clock gate clean; the plan gate is green.</done>
</task>

</tasks>

<verification>
- `ostro-k8s-ci --parallel <worktree>` (scripts/ci-local.sh on the microk8s `ostro-ci` namespace, Bash `run_in_background: true`, durable brief) green on the worktree at the last commit; CLOCK_GATE_SCOPED empty.
</verification>

<success_criteria>
All Phase 14 seams and stubs exist with fixed call sites; waves 2+ edit only their own files.
</success_criteria>

<output>
After completion, create `.planning/phases/14-multi-tenancy-security-1-0-release/14-01b-SUMMARY.md`: the stub-module map (file → owning plan), the exact seam signatures as landed (incl. `TriggerKind`, `start_triggers`, the raw `[triggers] kafka` type), the call-site list (file:function), in-process consumers that keep the routed instance, tool versions/sha256 (incl. weed, Kafka, JRE), and whether the Kafka service starts inside an `ostro-k8s-ci` pod (one `--cmd` run on slot 0/1: `bash scripts/fetch-tools.sh kafka jre && bash scripts/test-services.sh start kafka && bash scripts/test-services.sh stop`; on failure take the D-P14-44 fallback and record it), and a local check that all three Kafka listeners answer (`kafka-broker-api-versions.sh` with the matching client properties).
</output>
