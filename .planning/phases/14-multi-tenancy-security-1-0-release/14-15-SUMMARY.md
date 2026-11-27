---
phase: 14-multi-tenancy-security-1-0-release
plan: 15
subsystem: server authorization (scope enforcement, principal propagation)
tags: [p14, auth, scopes, authz, matrix, forwarding, principal, SEC-02]
requires:
  - phase: 14-02
    provides: "authn_layer (bearer layer, UNAUTHENTICATED list, TrustedNode bypass), phase14 adapters, FakeP14 fakes, ostro_proto::GRPC_METHODS"
  - phase: 14-03
    provides: "MetaAuthenticator, token codec, guard.rs (authz::require as step 2, per-stage TRACE events)"
  - phase: 13-12
    provides: "RouteCore::forward (one hop), PeerControlPlane, node API on the cluster listener (trusted_node)"
provides:
  - "p14/authz.rs: exhaustive Method -> Requirement table (required) + require() with ostro_auth_denied_total{reason}"
  - "p14/principal_fwd.rs: encode/decode/ingest of ostro-principal"
  - "route/peer.rs stamp_principal on every forwarded call; node API runs calls inside PRINCIPAL.scope(forwarded principal)"
  - "tests/auth_scopes.rs: route x scope matrix over every OpenAPI operation and descriptor-set gRPC method, OSTRO_EXPORT_AUTH_MATRIX export; join-token separation on a full server"
  - "tests/auth_forwarding.rs: 3-node mTLS token-mode cluster, forwarded principal observed at the leader, client header ignored"
affects: [14-16, 14-17, 14-18, 14-19, 14-24, 14-26, 14-27]
tech-stack:
  added: []
  patterns:
    - "the matrix checks each row's declared method against the guard's own authz TRACE event, so the route -> method map cannot drift"
    - "expected outcomes derive from the 14-01 inventory (scope + tenant class), independently of authz.rs"
key-files:
  created:
    - crates/ostro-server/tests/auth_scopes.rs
    - crates/ostro-server/tests/auth_forwarding.rs
  modified:
    - crates/ostro-server/src/p14/authz.rs
    - crates/ostro-server/src/p14/principal_fwd.rs
    - crates/ostro-server/src/route/peer.rs
    - crates/ostro-server/src/route/mod.rs (the node-API ingest file; minimal hunks)
    - crates/ostro-server/tests/p14_guard.rs (deviation 1)
    - crates/ostro-server/tests/tenant_isolation.rs (deviation 2)
decisions:
  - "Requirement has a fifth variant SystemScope(Scope) for the read + system_only methods (cluster/upgrade status, workers, partitions, tenants, module policy): tenant tokens are refused by the scope check, not only by the tenant hook"
  - "whoami and ready need only authentication (any token)"
  - "A tenant-less token holding scopes other than admin/read is 401 invalid_token everywhere (defence in depth)"
  - "Node principals pass only the read class; Dev and System pass everything"
  - "Denials: 403 missing_scope {method, scope} plus authority: system when tenant-less authority is required"
  - "Forwarded calls always carry the current principal (any inherited header value replaced); Anonymous internal callers send none and the receiver runs them as System"
  - "A malformed ostro-principal from a node identity is refused (INVALID_ARGUMENT), never defaulted to System"
metrics:
  completed: 2026-11-27
  tasks: 2
  duration: ~2h30m
---

# Phase 14 Plan 15: Scope enforcement, route x scope matrix, principal propagation Summary

**`authz::require` now enforces D-P14-3 for all 116 methods through an exhaustive `Method -> Requirement` table. An exact matrix test covers every REST operation and every gRPC method against 7 tokens plus no token, and its route -> method map is checked against the guard's own trace, so it cannot drift. Join tokens and API tokens are proven separate. Forwarded calls carry the caller's principal in `ostro-principal`, and the leader runs them as that principal. A client cannot inject the header.**

## Commits

| Task | Commit | What |
|------|--------|------|
| 1 | 000671a | `feat(14-15): scope enforcement + route × scope matrix` |
| 2 | 3a0c006 | `feat(14-15): principal propagation across forwarding` |
| — | (this) | docs: SUMMARY, ROADMAP tick, STATE line |

## Method -> requirement table (`p14/authz.rs::required`)

The table is one `match` arm per variant with no wildcard. A unit test checks it against the 14-01 inventory (`table_follows_the_inventory`).

| Requirement | Rule for a token | Methods |
|---|---|---|
| `Scope(read)` | `read` or `admin`, any tenant | get_module, list_workflows, get_run, run_history, list_runs, await_result, get_workflow_policy, get_queue_policy, presigned_object, get_queue_flow, queue_demand, dev_time, replay_module, list/get schedules, list/get webhooks, run_logs, analyze_rerun, list_dlq, list/get bulk, limits, get_failure_policy, list_modules, list_queues |
| `Scope(deploy)` | `deploy` or `admin` | upload_module, activate_workflow, set_workflow_policy, set_failure_policy, retire_module, migrate_runs, attach_signatures |
| `Scope(operate)` | `operate` or `admin` | cancel/terminate/retry/pause/resume run, set_queue_policy, put_queue_flow, pause/resume queue and workflow, pause/resume/trigger schedule, rerun, requeue/dismiss DLQ, create/cancel bulk |
| `Scope(trigger)` | `trigger` or `admin` | start_run, signal_run, complete_token, HookIngress.deliver |
| `SystemScope(read)` | tenant `None` with `read` or `admin` | list_workers, list_partitions, cluster status, upgrade_status, get/list tenants, get_module_policy |
| `SystemAdmin` | tenant `None` with `admin` | drain_worker, worker tokens (create/list/revoke), join_worker, dev_skip, backup, cluster init/join/create_node_token/remove/drain/reassign/rebalance/upgrade finalize/hold/release, run_module_gc, tenant create/update/suspend/resume/delete, set_module_policy |
| `TenantAdmin` | `admin` on any tenant (14-16 narrows it to the token's own tenant) | secrets, grants, connections, egress (put/get/list/delete), schedule and webhook create/update/delete, API tokens create/list/revoke/rotate, audit query, add/remove trusted key |
| `Authenticated` | any valid token | whoami, ready (the guard passes `ready` through) |

`Dev` and `System` pass everything. `Node` passes only the read class. `Anonymous` gets 401. A tenant-less token whose scopes go beyond admin/read gets 401 `invalid_token`. A denial increments `ostro_auth_denied_total{reason}` and returns 403 `PERMISSION_DENIED` with `details = {reason: "missing_scope", method, scope}`, plus `authority: "system"` when the method needs a tenant-less token.

## Route x scope matrix (`tests/auth_scopes.rs`)

- **Harness:** the production `ostro_api::router_with`, which includes the bearer layer and the REST/gRPC adapters. It sits over the real guard (`p14::wrap_ingress`), so the real `authz::require` and the 14-16 tenant hook run. Authentication uses the real `MetaAuthenticator` in token mode. Tokens are written straight into meta. Every slot, including the four Phase 14 slots, is backed by the 14-02 fakes.
- **Coverage is exact:** 123 REST operations (the OpenAPI document) and 122 gRPC methods (`ostro_proto::GRPC_METHODS`, including the 12 `ostro.cluster.v1` node-to-node methods, which show as `n/s` on the client listener). The exercised key set equals the table key set in both directions. Each row's unauthenticated class is asserted equal to `authn_layer::classify`. The unauthenticated set is the 14-02 list plus the routes the orchestrator notes require: `/v1/objects/*` presign (System), gRPC `Workers/JoinWorker` and `Cluster/Join` (System), `GET /`, `ostro.health.v1.Health/*`, `grpc.health.v1.Health/*`, `/healthz`, `/readyz`, `/metrics`, `/ui`, `/ui/*`.
- **Route -> method:** for each row, a call with the system-admin token (or no token for System routes) must produce exactly the declared method in the guard's `authz` TRACE event. Hand-encoded protobuf samples and JSON bodies make every dummy input reach the guard.
- **Outcomes:** expectations come from the 14-01 inventory, independently of `authz.rs`. Each cell is `allow` (neither 401 nor 403), `403` (`missing_scope`), `401` or `n/s`. A 403 from the tenant hook (`forbidden_tenant`, 14-16) is shown as `allow (403 tenant)`, because the scope check passed. Today that happens only for connection put/delete with `admin@_`, since 14-16 makes connections cluster-scoped. All mismatches are reported together.
- **Export:** `OSTRO_EXPORT_AUTH_MATRIX=<abs path>`. Produced locally at `target/auth-matrix.md` in the worktree: 363 table lines and 218 rows with at least one 403. 14-24 publishes it as `docs/security/auth-matrix.md`. Head of the file:

```
| Route | Method | Requirement | no token | admin@_ | deploy | operate | trigger | read | system admin | system read |
|---|---|---|---|---|---|---|---|---|---|---|
| `GET /` | - | unauthenticated | allow | allow | allow | allow | allow | allow | allow | allow |
| `GET /healthz` | - | unauthenticated | allow | allow | allow | allow | allow | allow | allow | allow |
| `GET /readyz` | - | unauthenticated | allow | allow | allow | allow | allow | allow | allow | allow |
| `GET /openapi.json` | - | authenticated | 401 | allow | allow | allow | allow | allow | allow | allow |
| `POST /hooks/{name}` | HookIngress.deliver | unauthenticated (System; own credential) | allow | allow | allow | allow | allow | allow | allow | allow |
| `POST /hooks/{tenant}/{name}` | HookIngress.deliver | unauthenticated (System; own credential) | allow | allow | allow | allow | allow | allow | allow | allow |
| `POST /v1/workers/join` | ControlPlane.join_worker | unauthenticated (System; own credential) | allow | allow | allow | allow | allow | allow | allow | allow |
| `POST /v1/cluster/join` | ClusterApi.join | unauthenticated (System; own credential) | allow | allow | allow | allow | allow | allow | allow | allow |
| `GET /v1/objects/{connection}/{key}` | - | unauthenticated (System; own credential) | allow | allow | allow | allow | allow | allow | allow | allow |
| `PUT /v1/objects/{connection}/{key}` | - | unauthenticated (System; own credential) | allow | allow | allow | allow | allow | allow | allow | allow |
| `GET /v1/admin/backup` | AdminApi.backup | system admin | 401 | 403 | 403 | 403 | 403 | 403 | allow | 403 |
| `GET /v1/audit` | AuditApi.query | tenant admin | 401 | allow | 403 | 403 | 403 | 403 | allow | 403 |
```

The export also contains the 116-row method -> requirement table with a route count per method. `ControlPlane.ready` is served by `/readyz` as a pass-through. `presigned_object` is reached by `/v1/objects/*` as System once the presign signature verifies; the dummy inputs carry no signature.

## Join-token separation and the demand endpoint (full token-mode server)

- Worker (`wkr_`, `worker:join`) and node (`njt_`, `node:join`) join-token records, in both the join spelling `<id>.<secret>` and the API spelling `ostro_<id>_<52>`, get **401** on `/v1/runs`, `/v1/auth/whoami` and `/v1/queues/q1/demand`.
- `otk_` API tokens (system admin and read) sent as the body token of `POST /v1/workers/join` and `POST /v1/cluster/join` are refused with `join_token_invalid`, with or without an `Authorization: Bearer` admin header. Crossed join tokens are also refused. As a positive control, the `wkr_` token passes the token check and fails later, on the dummy CSR.
- **Worker join credential confirmed** (orchestrator note): REST `/v1/workers/join` and gRPC `Workers/JoinWorker` both go to `ControlPlane::join_worker`, then `workers/join.rs::verify_secret` (`wkr_` id, sha256 secret compared in constant time, `worker:join` scope, expiry) before any CSR is signed. Node join uses `p13/join.rs::verify_token` (`njt_`, `node:join`). Presign routes 400 without `exp` and `sig`.
- Demand endpoint: 401 without a bearer, 200 with a read token, 403 `missing_scope` (scope `read`) with a trigger token.
- `/metrics`, `/healthz` and `/readyz` are not 401 on the real server. Read token start -> 403. Tenant admin `GET /v1/cluster/status` -> 403. Bootstrap system admin -> allowed.

## Principal propagation (Task 2)

- **Forwarding ingest file used:** `crates/ostro-server/src/route/mod.rs`. The plan's `route/node_api.rs` was a placeholder; `node_api_router` and `RouteCore::forward` live in `route/mod.rs`.
- **Forwarder:** `RouteCore::forward` calls `peer::stamp_principal(&mut out)` after the per-leg edit. This covers every forwarded call and every fan-out leg, since all of them go through `forward`. `stamp_principal` sets `RequestMeta.principal = principal_fwd::encode(&auth::current())`, which the gRPC client copies into `ostro-principal`. It replaces any inherited value, and an `Anonymous` internal caller sends none.
- **Receiver:** `node_api_router` checks the mTLS node identity, then `forwarded_principal` decodes the header. A missing header means System; a repeated or malformed header is `INVALID_ARGUMENT`. The call runs inside `PRINCIPAL.scope`, with the kind preserved. A debug event (target `ostro::p14::principal_fwd`: `from_node`, `principal`, `kind`, `tenant`, `forwarded`) reads the principal back from the task-local, which is exactly what node-local lookups see. The node API is unguarded, so the principal is never re-authorized or widened.
- **Client listener:** `ostro-principal` is dropped there by `request_meta::from_headers` (no `TrustedNode`), and the principal comes only from the bearer.
- **`auth_forwarding.rs`:** 3 static nodes over loopback mTLS in token mode, with tenant `acme` (a meta record) at finalized level 2.
  1. An `acme` trigger token's keyed start through a node that does not lead every partition is forwarded (the ingress `RouteStats.forwarded` goes up). The leader logs the forwarded principal: the token id, kind `token`, tenant `acme`, from the ingress node.
  2. A read token is denied 403 `missing_scope` at ingress 8 times. The forwarded count does not move and no peer sees the read token.
  3. A read token plus `ostro-principal: p1;otk_xxxxxxxxxxxxxxxx;*;admin` still gets 403 on `GET /v1/cluster/status` and `POST /v1/workers/tokens`. A forwarded start carrying the spoofed header shows the real trigger token at the leader, and `otk_xxxxxxxxxxxxxxxx` never reaches a peer.
- The header format is unchanged: the 14-01 `encode_forwarded` string, at most 256 bytes. `principal_fwd::{encode, decode}` keep the 14-01b `String` signatures. The plan text said `HeaderValue`, but `RequestMeta.principal` is a `String`.

## Verification (local, slot-5 target dir, CARGO_BUILD_JOBS=4, nice 10)

- Task 1 verify (plan command, with `target/` created in the worktree): PASS. `auth_scopes` 2 passed, the matrix file is written, and there is no `_ =>` in authz.rs.
- Task 2 verify (plan command): PASS. `auth_forwarding` 1 passed, the `principal_fwd::encode` grep matches, and CLOCK_GATE_SCOPED on the diff from p14-base is empty (no clock reads added).
- Both verifies were re-run at the final HEAD 3a0c006, after 14-32 landed in between: PASS.
- Unit tests: `p14::authz` (2), `p14::principal_fwd` (2), all `p14::` lib tests (15).
- Integration: p14_guard 8, p14_skeleton 7, p14_authn 9, p14_tokens 10, p14_signing 24, tenant_isolation 4, tenant_triggers 3, tenant_quotas 6, p13_routing_unit 29, p13_consistency_unit 15, p13_inbound_tcp 1, cluster_api 1, p13_join 3.
- ostro-repl-conformance tier B (mode none, node API now scoped as System): `routing` 6, `ryw` 10.
- `cargo clippy -p ostro-server --lib --tests -D warnings`: clean. rustfmt (edition 2024) is clean on every touched file. `scripts/p12-grep-gates.sh` and `scripts/p13-grep-gates.sh`: pass.
- **Plan gate: incomplete. The cluster was withdrawn; the consolidated gate is pending on dell-4.** `ostro-k8s-ci --parallel` was not run. **Unverified here:** every ci-local group at 3a0c006, namely toolchain, workspace `cargo fmt --check` (it already fails on 14-29 files on main), workspace clippy and `cargo build`, the full `cargo test` shards test1/test2 (only the suites listed above ran), failpoint suites fp1-3 (ostro-server `--features failpoints` was not built here), the musl release, the musl binary size limit (this plan adds about 150 lines of non-generic production code; size not re-measured), musl zigbuild tests, SDK/polyglot/clients, the Phase 13 mutant matrix, Phase 13 tier C (process clusters with forwarding under token mode), the remaining ostro-repl-conformance suites (membership, handoff, failover, campaign smoke), cargo deny / vendored checks, and workflow validation.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] `tests/p14_guard.rs` asserted the authz stub (14-03's test)**
- **Found during:** Task 1
- **Issue:** `calls_outside_any_principal_scope_are_401_and_never_reach_inner` expected an `acme` read token to reach the inner plane on every probe, which held only while `authz::require` allowed everything.
- **Fix:** that pass-through probe token is now a system admin token. The test's intent, that a scope lets calls through the 401 check, is unchanged.
- **Commit:** 000671a

**2. [Rule 1 - Bug] `tests/tenant_isolation.rs` (14-16, landed in parallel) required `forbidden_tenant` for every system-only method**
- **Found during:** Task 2 regression run
- **Issue:** with scopes enforced, a tenant principal on a system-only method gets `missing_scope` from authz before the tenant hook runs. 14-16's own module docs anticipate this ("14-15's scope check usually answers first").
- **Fix:** the assertion now accepts either 403 reason.
- **Commit:** 3a0c006

**3. Harness shape of the matrix (plan said a full in-process server with fakes in the four Phase 14 slots).** The server has no seam for injecting slot fakes; `fill_extensions` and the slot files belong to 14-01b/14-17/18/19. So the matrix runs the production API router, bearer layer, guard and meta authenticator over fakes for every slot. That is the same composition `p11::wire` builds, minus the UI merge. A full token-mode server covers what needs real implementations: join-token separation, the demand endpoint, the non-OpenAPI routes, and a sample of the matrix end to end.

**4. Tenant of the forwarding test is `acme`, not `_`.** See the orchestrator item below: `_`-tenant tokens cannot start runs on a real server since 14-16.

**5. `Requirement::SystemScope`.** This variant is added to the plan's four. Without it, read + system_only methods would be allowed for tenant read tokens by authz and refused only by the tenant hook.

### CA-mismatch boot check (14-30 note): not taken

It does not fit this plan cleanly. The check belongs to node boot, in `server.rs` / p13 boot / tls, where `server.rs` is 14-01b-only under the Phase 14 shared-files rule. It is a TLS/boot concern, not request authorization. I recommend 14-26 (or a small dedicated fix), comparing the configured `[cluster.tls]` CA with `__ostro/cluster-ca` once meta is readable and refusing with a clear error.

## Orchestrator decisions / follow-ups

1. **Tokens on the default tenant `_` are broken for writes (14-16/14-17).** `tenant_scope` treats `Principal.tenant = Some("_")` as a literal tenant. On a real server, `StartRun` gets `tenant = "_"` forced, and `control::names::validate_tenant` rejects that with 400 ("tenant \"_\" is reserved for the default tenant"); workflow names are qualified as `_/<name>`. Every keyed or unkeyed start with a `_`-tenant trigger token fails. The fake-backed matrix does not see this, because fakes do not validate. Someone needs to decide how a default-tenant token is represented (and what 14-17's `create_token` stores) and map it in `tenant_scope::tenant_of`.
2. **Connections policy:** the inventory (and so authz) gives tenant admins `put/delete_connection`, but 14-16's tenant hook refuses them as cluster-scoped (shown as `allow (403 tenant)` in the matrix). If that is the 1.0 policy, changing `required()` for `PutConnection`/`DeleteConnection` (and maybe `Get`/`List`) to `SystemAdmin` is a one-line change per method. Otherwise 14-16 should relax. This is for the D-P14-3 user review and 14-24.
3. 14-26: the 401 body still says `unauthenticated` rather than `authn::reason()` (unchanged from 14-03's note).
4. The consolidated dell-4 gate must cover the unverified list above.

## Workflow hunks

None. Both tests are new targets in the existing ostro-server crate, so `cargo test` picks them up. 14-24 may want an export step: `OSTRO_EXPORT_AUTH_MATRIX="$PWD/docs/security/auth-matrix.md" cargo test -p ostro-server --test auth_scopes`.

## Self-Check: PASSED

- FOUND: crates/ostro-server/src/p14/authz.rs, crates/ostro-server/src/p14/principal_fwd.rs, crates/ostro-server/tests/auth_scopes.rs, crates/ostro-server/tests/auth_forwarding.rs
- FOUND: 000671a, 3a0c006 on main
