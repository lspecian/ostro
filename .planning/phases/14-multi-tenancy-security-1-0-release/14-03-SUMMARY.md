---
phase: 14-multi-tenancy-security-1-0-release
plan: 03
subsystem: server principal pipeline (tokens, authentication, admin bootstrap, guard)
tags: [p14, auth, tokens, bootstrap, guard, deny-by-default, SEC-02]
requires:
  - phase: 14-01b
    provides: "p14 seam (P14Ctx, wrap_ingress, authenticator, start), passthrough hook stubs, [auth] config, main.rs intercept of `ostro auth bootstrap`"
  - phase: 14-02
    provides: "authn_layer (PRINCIPAL scope from ApiConfig.authenticator; System for hooks/join, Anonymous for /ui and probes)"
provides:
  - "p14/tokens.rs: ostro_otk_<16 b32>_<52 b32> codec (OS RNG via aws-lc-rs, canonical base32, sha256 of decoded secret, constant-time verify)"
  - "p14/authn.rs: MetaAuthenticator (local meta view, no meta write) + authenticator() factory + reason()"
  - "p14/bootstrap.rs: ensure_admin (online, token mode, meta leader) + offline `ostro auth bootstrap`"
  - "p14/guard.rs (final): GuardedControlPlane + 13 guarded extension wrappers, fixed hook order, completeness lists"
affects: [14-15, 14-16, 14-17, 14-18, 14-21, 14-26]
tech-stack:
  added: []
  patterns:
    - "guard wrappers are macro-generated from one `fn name(args) -> Out => Method, target;` line per trait method; only request-typed steps are generic (binary size)"
    - "hook order is observed through per-stage TRACE events (target ostro::p14::guard), no test-only hook seam"
    - "legacy loopback test harnesses run with [auth] mode = none; Phase 14 tests opt into token mode"
key-files:
  created:
    - crates/ostro-server/tests/p14_tokens.rs
    - crates/ostro-server/tests/p14_authn.rs
    - crates/ostro-server/tests/p14_guard.rs
  modified:
    - crates/ostro-server/src/p14/{tokens,authn,bootstrap,guard}.rs
    - crates/ostro-server/src/p14/{tenant_scope,audit_hooks}.rs (stub signatures only)
    - crates/ostro-server/src/lib.rs (aws_lc_rs anchor removed)
    - crates/ostro-server/tests/p14_skeleton.rs
    - test harnesses (see Deviations 1)
decisions:
  - "Bootstrap runs only in [auth] mode = token (no admin.token in dev / mode none)"
  - "Only unexpired system admins (tenant None, scopes ⊇ admin, otk_ id, API scopes only) count as existing"
  - "ext 112 is written only when compat::require(ApiTokenMetaExt) passes, i.e. after `upgrade finalize` to level 2; fresh clusters (finalized 1) store bootstrap tokens without ext"
  - "Expiry is checked after the hash, so only a secret holder learns that a token expired"
  - "ControlPlane::ready passes through the guard without a principal check (sync /readyz probe, no tenant data)"
  - "Hook order observed via TRACE capture; no GuardHooks trait object in P14Ctx"
metrics:
  completed: 2026-11-27
  tasks: 3
---

# Phase 14 Plan 03: Tokens, authenticator, admin bootstrap, guard Summary

**Bearer tokens `ostro_otk_<16>_<52>` with a hand-written canonical base32 codec, a meta-backed authenticator that never writes meta, a token-mode admin bootstrap (env / file / generated 0600 `admin.token`, idempotent, leader-only with a watcher) plus offline `ostro auth bootstrap`, and the final deny-by-default guard over the ControlPlane and all 13 extension traits.**

## Commits

| Commit | Task | Content |
|---|---|---|
| db5870d | 1 | feat: API token codec |
| 03fdc72 | 2 | feat: meta authenticator + admin bootstrap (+ legacy harnesses to mode none) |
| 5ea700b | 3 | feat: guard decorator |
| 1082df4 | 3 (fix) | fix: keep the guard's per-method instantiations small (musl size) |
| (this) | — | docs: SUMMARY, ROADMAP tick, STATE line |

## Token codec (Task 1)

- `generate(&impl SecureRandom)` / `generate_os()` (aws-lc-rs `SystemRandom`): id `otk_` + 16 chars (10 bytes), secret 52 chars (32 bytes), bearer `ostro_<id>_<secret>` (79 bytes).
- `parse`: `> 128` bytes → `TooLong`; missing `ostro_` → `Prefix`; `rsplit_once('_')`; `wkr_`/`njt_` ids → `NotApiToken`; bad id → `Id`; secret not 52 canonical base32 chars → `Secret`. Decoder rejects non-zero trailing bits, so a one-character change to the secret can never decode to the same bytes.
- `hash = sha256(decoded secret bytes)`, `verify` via `aws_lc_rs::constant_time::verify_slices_are_equal`. `Secret`/`Bearer` Debug output is redacted.
- Tests: shape, RFC 4648 vectors, 23-case malformed table, 10,000 unique ids, proptest (512 cases): parse never panics, any single-character mutation fails, generate→parse→verify, base32 round trip.

## Authenticator (Task 2)

`MetaAuthenticator::check`: mode none → `Principal::dev()`; no bearer → `Missing`; parse failure → `Malformed` (`NotApiToken` for wkr_/njt_); failpoint `auth::lookup` (failpoints builds) → `Unavailable`; record missing → `Invalid`; hash mismatch → `Invalid`; stored scopes contain `worker:join`/`node:join`/unknown → `NotApiToken`; `expires_at_ms <= clock.now_ms()` → `Expired`; else `Principal::token(id, token.tenant, scopes)`. Side effects: an LRU last-seen map (10,000) and `ostro_auth_requests_total{result=ok|missing|malformed|invalid|expired|not_api_token|unavailable}`. Never a meta write (the test checks the applied index).

**Exact error reasons** (`authn::reason`): `Expired` → `expired_token`; `Malformed`/`Invalid`/`NotApiToken` → `invalid_token`; `Missing` → `missing_token`; `Unavailable` → `auth_unavailable` (503). 14-02's layer currently answers `details.reason = "unauthenticated"` for every 401 (it does not call `reason()`); adopting `authn::reason` in the 401 body is left to 14-26 (ostro-api is 14-02's).

`p14::authenticator`: mode none → `AllowDev`; token → `MetaAuthenticator` over `ctx.meta.view()` and `ctx.clock`.

## Bootstrap (Task 2)

- **Online `ensure_admin`** (from `p14::start`): token mode only. Source order: `OSTRO_BOOTSTRAP_TOKEN` → `[auth] bootstrap_token_file` (must parse; message never echoes the value) → generated bearer written to `<data>/admin.token` (0600, tmp + fsync + rename + dir fsync, via `tls::ca::write_atomic`) **before** the create-only `Put`. Log: `bootstrap admin token written to <path>` / `created from <origin>` with the token id, never the secret. A process-wide lock + re-check under it → one token for concurrent calls; a supplied-id `Conflict` counts as success; leader-unavailable → remove the file, start a watcher (1 s, stops on `P14Ctx::shutdown`) that bootstraps if this node becomes leader before an admin exists.
- **Offline mode used**: `bootstrap::offline` takes the data dir `LOCK` (`flock`, refuses with "in use by a running server … use `ostro auth token create` against the running cluster"), probes the persisted meta configuration read-only with `ostro_cluster::identity::probe_meta` (refuses unless exactly one voter and no learners: same message), opens meta through the single-member path `MetaHandle::start(<data>/meta, <voter>)` (the offline-backup path, on the real directory because it writes), proposes the token (name `offline-bootstrap-admin`, optional `--expires`) and prints the bearer once on stdout. The partition node is not opened (Inspect mode not needed). Wall clock via `SystemClock` marked `clock-exempt: offline CLI`; `offline_with_clock` is the testable core.
- Tests: concurrent `ensure_admin` → one token + 0600 file whose bearer authenticates; mode none / existing admin no-op; tenant admin and expired admin do not count; source precedence; ext 112 written at finalized level 2; in-process server: bootstrap, restart → still one token and the same file; `bootstrap_token_file`; offline bootstrap on a stopped node then restart and authenticate (expiry = now + 1 h, ext name), refused while running, bad inputs.

## Guard (Task 3)

**Guarded traits**: `ControlPlane` (51 guarded methods + pass-through `ready`), `ScheduleApi` (8), `WebhookApi` (5), `HookIngress` (1), `RunLogsApi` (1), `AdminApi` (1), `OpsApi` (12), `InventoryApi` (3), `ClusterApi` (12), `ModuleLifecycleApi` (3), `TenantsApi` (7), `ApiTokensApi` (5), `AuditApi` (1), `SecurityApi` (5). That is 116 = `Method::all()`. Only populated slots are wrapped; `hook_body_limit` is copied.

Per method (`Core::call`): `auth::current()`; Anonymous → 401 `unauthenticated` before any hook; `authz::require(&p, m)`; `tenant_scope::before(&p, m, routed_control_plane, req)`; context upgrade (503 once released) + `audit_hooks::reserve(ctx, &p, m)`; target helper; inner call; `audit_hooks::after(ctx, permit, &p, m, target, Ok|Failed(code))`; `tenant_scope::after(&p, m, out)`. The request is the single argument, `()` for none, or a tuple in parameter order. Targets: run id / name / sha / `node:<id>` / `partition:<n>` / `workflow:`/`queue:` scope, empty otherwise.

**Hook signatures changed to the final call shape** (stubs only, still passthrough): `tenant_scope::before` is `async` and receives `&dyn ControlPlane` (14-16 needs `get_run` for the run-id class); `tenant_scope::after` takes and returns the inner `Result`; `audit_hooks::reserve` is `async` (14-18's `reserve_timeout`). 14-16/14-18 implement these bodies without touching guard.rs.

**Hook-observation approach**: per-stage `TRACE` events (target `ostro::p14::guard`, fields `method`, `stage`) captured by a test `tracing_subscriber::Layer`; no `GuardHooks` object in `P14Ctx`. (Tests must warm the callsites and call `tracing::callsite::rebuild_interest_cache()`, because parallel tests on threads without a subscriber can cache "never" for a callsite.)

**Completeness**: `guard::guarded_methods()` (macro-generated `METHODS` per wrapper + `Ready`) equals `Method::all()` with no duplicates; `guard::wrapper_of(Method)` is an exhaustive `match` (a new variant breaks the build).

Tests (`p14_guard.rs`, 8): completeness; deny-by-default (17 probes across all wrappers → 401, counting inner = 0; spawned work loses the scope → 401; System/Dev/token scopes pass); 503 after release; passthrough equivalence over the ostro-api fakes (upload, list, get_module, start, get_run, list_runs, errors, schedules, ops, inventory, cluster, module lifecycle, tenants, security); empty slots stay empty; hook order; **N2**: full server in mode none: REST deploy + start + `GET /v1/runs/{id}` and `GET /ui/runs` 200 containing the run id, no `admin.token`; token mode: no bearer 401, garbage 401, the bootstrap bearer 200, `/healthz` 200.

## Verification

- Task verifies (local, slot-3 target dir): `p14_tokens` 10/10, `p14_authn` 9/9 (10/10 with `--features failpoints --test-threads=1`), `p14_guard` 8/8, `p14_skeleton` 7/7, `ostro-ui` all green; CLOCK_GATE_SCOPED empty; `authz::require` / `audit_hooks::after` greps OK.
- Local: `cargo clippy -p ostro-server -p ostro-repl-conformance -p ostro --all-targets -D warnings` clean (also ostro-server `--features failpoints`); `cargo fmt --all --check` clean; `scripts/p12-grep-gates.sh` and `p13-grep-gates.sh` pass.
- Cluster, targeted (slot 1, worktree at 5ea700b content before rebase): `cargo test -p ostro-server -p ostro -p ostro-repl-conformance -p ostro-ui --no-fail-fast` passed. The only failure was the hook-order test's tracing interest race, which was fixed before commit.
- **Plan gate: incomplete — cluster withdrawn; consolidated gate pending on dell-4.** Three `--parallel` runs:
  - p174141 (pre-commit): found the `blob_gc` unit-test 401 (fixed). `cargo test` stopped at the first failing binary.
  - p221229 at 5ea700b: `cargo test` (test1) PASS, failpoint suites otherwise PASS, campaign smoke PASS, replication mutants PASS. **musl size FAIL 60.4 MB** (the guard added about 1.5 MB), fixed by 1082df4. Three failures passed alone on slot 3 at 1082df4: `ostro-partition timers::hundred_thousand_timers_bounded_window_large` (179 s), `ostro-repl-conformance stale_epoch` (harness start timeout under load; 105 s alone) and `ostro-bridge providers_vault::kubernetes_login_and_relogin_on_lease_expiry` (2.9 s).
  - p023530 at 1082df4: `cargo test` (test1) PASS, failpoint suites PASS, replication mutants PASS, campaign smoke PASS, zig + musl release + **musl binary size limit PASS** + musl zigbuild test (musl1) PASS, ts sdk / polyglot conformance / polyglot clippy / clients python / ts / go PASS. Failures: python sdk tests (`Failed to fetch https://pypi.org/simple/pytest/`, network) and ostro-build polyglot (`toolchain_cache::{read_idle_timeout_fires, unsafe_archives_are_rejected}`, crate untouched by this plan, not rerun). The lint, test2 and musl2 pods never ran: "ci container not started", then API unreachable / deadline on dell-1.
  - Size check alone at 1082df4's guard (slot 3 job ostro-ci-s3-395471b3-004556-8851): musl binary size limit PASS. The exact byte count was cut off by the output filter.
- **Unverified steps (need the dell-4 consolidated gate)**: toolchain, cargo fmt --check (passes locally), cargo clippy -D warnings for the whole workspace (affected crates pass locally), cargo build (warning-free), sdk workspace fmt+clippy, sdk workspace test + wasm build, guests build, order-saga example, the test2 half of `cargo test` (and the targeted workspace run t2 never reported back), the musl2 half of musl zigbuild test, python sdk tests, ostro-build polyglot (`toolchain_cache` alone rerun), semantics sleepers, failpoint list sync, Phase 13 mutant matrix, engine wit-next fixture suite, Phase 13 tier C, phase 7 failpoint legs, cargo deny, check-vendored, check-deny-fixtures, chronik/tokio-uring/prometheus grep, P12/P13 grep gates (pass locally), workflow validation.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Legacy test harnesses would 401 once token mode authenticates**
- 14-02's layer plus this plan's `MetaAuthenticator` make the server profile's default (`mode = "token"`) effective. Every pre-Phase-14 test server built from `server_defaults()` would answer 401.
- Fix: loopback harnesses set `[auth] mode = "none"`. Files: `ostro-server/tests/common/{mod,p12_cluster}.rs`, the `services/blob_gc.rs` unit test, `ostro-repl-conformance/src/harness/{inproc,process}.rs` and `scenarios/membership.rs`, and `ostro/tests/{common/mod,common/p7,fx_redrive,worker_e2e}.rs` (`OSTRO_AUTH__MODE=none` on spawned `ostro server`). `ostro dev`-based harnesses (crashtest, clients) needed nothing. Commit 03fdc72.

**2. [Rule 3 - Blocking] Hook stub signatures** (`tenant_scope.rs`, `audit_hooks.rs`, owned by 14-16/14-18): changed to the plan's final call shape (async, inner control plane, Result in/out) so guard.rs can be final. Stubs remain passthrough. Commit 5ea700b.

**3. [Rule 1 - Bug] p14_skeleton asserted 14-01b stub behaviour**: the authenticator assertion now expects `Missing` in token mode, and the guard assertion expects a wrapped instance (03fdc72, 5ea700b).

**4. [Rule 1 - Bug] musl size over the 60 MB limit (60.4 MB) after the guard**: the generic hook sequence and its tracing callsites were instantiated about 115 times. 1082df4 moves everything that is not request-typed into non-generic helpers with one TRACE callsite, and the size step passes again. The binary is still close to the limit; D-P14-53 / 14-34 may raise it to 70 MB.

### Plan text vs code (recorded)
- The plan wrote `tenant_scope::before(...).await` / `audit_hooks::reserve(...).await`, but 14-01b landed them sync. Fixed by deviation 2.
- `bootstrap::ensure_admin` takes `&Arc<P14Ctx>` (mod.rs's `start` passes `&ctx` with `ctx: Arc<P14Ctx>`, unchanged) so it can spawn the weak watcher.
- The concurrent-leader race for *generated* bearers (two leaders in quick succession, both seeing no admin) could create two bootstrap tokens. Supplied bearers collide on the id and stay one. This is recorded, not mitigated beyond leader-only + lock.

## Workflow hunks

None.

## Orchestrator actions / decisions

1. **Queued slot-3 runner still pending (could not stop it):** after the cluster pause, my own lint-group rerun (`ostro-k8s-ci --timeout 21600 --cmd CI_LOCAL_ONLY=…lint steps…`, local PID 17878, submitted before the pause) was still waiting locally for slot 3 behind my running t2 job (ostro-ci-s3-1082df4d-073213-11120). Stopping my own PID was blocked by the permission classifier. When t2 finishes, that runner will submit a job unless someone stops it. Its worktree has been removed, so the upload will most likely fail.
2. 14-26 / 14-02 follow-up: put `p14::authn::reason()` (`invalid_token` / `expired_token`) into the 401 `details.reason`.
3. Consolidated gate on dell-4 for the unverified steps listed above.
