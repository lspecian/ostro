---
phase: 14-multi-tenancy-security-1-0-release
plan: 17
subsystem: tenant + API-token admin services, ostro-client, CLI verbs
tags: [p14, tenants, tokens, cli, sec-02, disp-06, default-tenant, connections-authz]
requires:
  - phase: 14-02
    provides: "REST/gRPC adapters over the TenantsApi / ApiTokensApi slots, FakeTenants / FakeTokens"
  - phase: 14-03
    provides: "token codec (generate_os, hash, is_api_token_id), MetaAuthenticator, bootstrap admin, guard"
  - phase: 14-06
    provides: "TenantQuotaView (meta-watched quotas) used to check quota changes"
  - phase: 14-15
    provides: "authz::required table (amended here for connections)"
  - phase: 14-16
    provides: "tenant_scope (amended here for the default tenant `_`)"
provides:
  - "p14/tenants_api.rs: TenantsService (create / get / list / patch / suspend / resume / delete with the emptiness checks), tenants_api::bind_routed / routed_of"
  - "p14/tokens_api.rs: TokensService (create / list / revoke / rotate / whoami; otk_ only; creation rule matrix)"
  - "ostro-client tenants.rs / api_tokens.rs: raw methods for every REST path"
  - "ostro tenant create|ls|get|set|suspend|resume|rm, ostro auth token create|ls|revoke|rotate, ostro auth whoami"
  - "default-tenant (`_`) tokens work as tenant principals of the default tenant; connections need a system admin"
affects: [14-18, 14-19, 14-21, 14-22, 14-24, 14-26, 14-27]
tech-stack:
  added: []
  patterns:
    - "services hold the P14Ctx weakly (503 once released), like ctx_of"
    - "the routed control plane reaches a slot impl through a ctx-keyed weak binding set by guard::wrap (p14/mod.rs is 14-01b-only)"
key-files:
  created:
    - crates/ostro-server/tests/p14_admin_api.rs
    - crates/ostro-cli/tests/p14_cli.rs
    - crates/ostro-cli/tests/snapshots/p14_cli__{tenant_human,tenant_json,tenant_usage,token_usage,token_verbs}.snap
  modified:
    - crates/ostro-server/src/p14/{tenants_api,tokens_api}.rs
    - crates/ostro-client/src/{tenants,api_tokens}.rs
    - crates/ostro-cli/src/commands/{tenant,auth}.rs
    - "deviations: crates/ostro-api/src/phase14.rs (TenantPatch null), crates/ostro-server/src/p14/guard.rs (1 line), crates/ostro-server/tests/p14_skeleton.rs (1 assertion), crates/ostro-server/src/p14/{tenant_scope,authz}.rs + tests/{auth_scopes,tenant_isolation}.rs (orchestrator follow-ups)"
key-decisions:
  - "Token creation: every requested scope must be allowed by the caller's own scopes (admin allows all); tenant principals create for their own tenant only (unset = own); tenant None only with admin/read; the tenant must exist (`_` always does); no_expiry only for a system admin; default expiry = [auth] default_token_ttl"
  - "revoke / rotate / list check the record's tenant against auth::current(): a tenant principal gets the same 404 for another tenant's token as for an unknown id"
  - "rotate without ttl keeps the old token's lifetime (from now); a no-expiry token rotated by a non-system-admin gets the default ttl; created_at and expires_at come from one clock read"
  - "TenantPatch: JSON null clears a quota (custom deserializer in ostro-api), absent keeps it"
  - "A `_` token is a tenant principal confined to the default tenant (never system scope): bare names, None-means-default request fields forced to None"
  - "Connections (put/get/list/delete) require a system admin in authz (1.0 cluster-scoped policy)"
requirements-completed: []
duration: ~5h
completed: 2026-11-27
---

# Phase 14 Plan 17: Tenant and API-token admin services + CLI Summary

**Tenants and scoped API tokens are now managed through the API and the CLI. `TenantsApi` does create (`_` refused, quotas > 0), patch (null clears), idempotent suspend/resume, and delete only when the tenant is suspended and empty (409 `tenant_not_empty`). `ApiTokensApi` touches only `otk_` tokens: it shows the bearer once (`no-store`), stores hash + ext 112, revokes by CAS Delete (401 on the next request) and rotates with `rotated_from`. Both have `ostro-client` methods and `ostro tenant …` / `ostro auth token …` / `ostro auth whoami` verbs. Two orchestrator follow-ups from 14-15 are included: `_`-tenant tokens can start runs, and connections need a system admin.**

Requirement ids are not ticked here (14-27's, D-P14-35).

## Commits

| Commit | Task | Content |
|---|---|---|
| c004a0e | 1 | `feat(14-17): tenants + API tokens admin services` |
| 83d014e | follow-ups | `fix(14-17): default-tenant tokens + connections need a system admin` |
| b0fffa3 | 2 | `feat(14-17): tenant and token CLI verbs` |
| (this) | — | docs: SUMMARY, ROADMAP tick, STATE line |

## Token-creation rule matrix (as tested, `p14_admin_api::token_creation_rules`)

Calls go to `TokensService` on the server's own context inside `auth::scope(principal, …)`. Guard denials are not relied on.

| Caller | Request | Result |
|---|---|---|
| tenant admin `acme` | tenant `acme`, deploy+read | created, tenant acme, `created_by` = caller id |
| tenant admin `acme` | no tenant, trigger | created for `acme` (unset = own) |
| tenant admin `acme` | tenant `globex` | 403 `forbidden_tenant` |
| tenant admin `acme` | `no_expiry` | 403 `missing_scope` (system admins only) |
| `acme` {deploy, read} | operate / admin / deploy+trigger | 403 `missing_scope` |
| `acme` {deploy, read} | deploy | created (subset) |
| system admin | tenant `globex`, deploy | created; expiry = now + 2160h (default_token_ttl) |
| system admin | system token, read, ttl 1h | created, tenant None, expiry now + 1h |
| system admin | system token, admin, `no_expiry` | created, `expires_at_ms` null |
| system admin | tenant `_`, deploy | created (default tenant always exists) |
| system admin | system token, deploy | 400 (system tokens: admin/read only) |
| system admin | tenant `nope` | 404 tenant not found |
| system admin | empty name / no scopes / `worker:join` / ttl 0 / ttl + no_expiry | 400 |
| tenant admin `acme` | list (no filter) | only acme tokens |
| tenant admin `acme` | list `globex` | 403 `forbidden_tenant` |
| system admin | list / list `globex` | all (incl. the bootstrap admin) / globex only |
| tenant admin `acme` | revoke / rotate a globex token | 404, record survives |
| tenant admin `globex` | revoke the same token | deleted; second revoke 404 |
| any | whoami | `{token_id, tenant, scopes, kind}` of the caller |

REST evidence (`bearers_revocation_rotation_and_listing`): create is 201 with `Cache-Control: no-store`, the bearer is `ostro_<id>_…`, and ext 112 name/description/created_by are set (level 2). The new bearer authenticates as the tenant principal. Listings contain no `hash`/`bearer` key anywhere and never the secret. Rotate with `revoke_old: false` makes both bearers work, sets `rotated_from` and keeps the same name/scopes/tenant and lifetime. Rotate with `revoke_old: true` makes the old bearer 401. After revoke the next request is 401 and a second revoke is 404. `wkr_…`/`njt_…` records get 404 on revoke and rotate, survive, and never appear in a listing. `ext_112_needs_finalized_level_2`: with fresh metadata the record has no ext (empty name), and the injected clock drives created_at/expiry.

Tenants (`tenant_lifecycle_over_rest`, `delete_counts_active_versions_and_non_terminal_runs`):
- create 201 / duplicate 409 / `_`, `Acme`, `-x`, `""` 400 (`_` also straight into the impl, "default tenant" message).
- Every zero quota and a zero rate limit or period gives 400 and no record is written.
- list is sorted and get / 404 work.
- PATCH sets weight, clears `max_concurrency` with `null`, keeps the rest. 0 is 400 and an unknown tenant is 404.
- The **14-06 quota view sees the patch and the suspension within one meta-watch tick** (polled ≤ 5 s, observed at once).
- suspend/resume are idempotent.
- delete: not suspended → 409 `tenant_not_empty`; a token of the tenant → 409 ("token"); a secret `acme/…` → 409 ("secret"); empty → 200 `{id, deleted}` and then 404.
- On a bare context: no routed plane bound → 503 (fail closed); an active `acme/orders` version → 409; a non-terminal acme run in the fake routed plane → 409 naming the run (another tenant's run does not block); after completion → deleted; again → 404.

## CLI examples

```
$ ostro tenant create acme --weight 2 --max-concurrency 8 --memory 512MiB --event-rate 100/1s+10
$ ostro tenant set acme --fuel-per-s 1000 --clear max-concurrency
$ ostro tenant suspend acme && ostro tenant rm acme
$ ostro auth token create --name ci --scope deploy --scope read --tenant acme --ttl 720h --description "CI deployer"
token id:      otk_…
tenant:        acme
scopes:        deploy,read
expires at:    …
bearer (shown once):
  ostro_otk_…_…
(stderr) ostro: store it now; it cannot be shown again
$ ostro auth token create --name root --scope admin --system --no-expiry --json
$ ostro auth token ls [--tenant acme]
$ ostro auth token rotate otk_… [--keep-old] [--ttl 720h]
$ ostro auth token revoke otk_…
$ ostro auth whoami [--json]
```

`p14_cli.rs` has 5 insta transcripts over the 14-02 fakes (human + `--json`). `--json` prints the REST body as it arrives. Usage errors exit 2 with the clap message: `_` / bad grammar, `12XB`, zero quota, a bad rate, `1d`, an unknown `--clear` field, `--clear` together with a value for the same field, missing `--name`/`--scope`, an unknown scope, `--tenant` with `--system`, `--ttl` with `--no-expiry`. The transcripts check that the bearer (and the stderr hint) appear in create/rotate output only.

**Manual smoke against a real token-mode `ostro server`** (debug binary, loopback, `OSTRO_TOKEN` = `admin.token`): whoami (system admin), `tenant create acme --max-concurrency 4 --memory 64MiB`, `auth token create … --tenant acme`, that bearer's `whoami --json` (tenant acme), `token ls`, `token revoke`, the revoked bearer's whoami → `HTTP 401`, then `tenant suspend` + `tenant rm`. All worked.

## Orchestrator follow-ups (from 14-15), included

1. **Default tenant `_`.** Decision: a `_` token stays a **tenant principal confined to the default tenant**. It is never system scope, because mapping `_` to None would give a `_` deploy token every tenant's view. In `tenant_scope`:
   - `qualified()` keeps names bare for `_`, and `owns()` accepts unqualified names only.
   - Runs match `None`/`""`.
   - A new `force_default` sets the fields the server reads as "None = default tenant" (`UploadModule.tenant`, `StartRunRequest.tenant`, put/list/delete secret) to `None`. Before this they were set to `"_"`, which `validate_tenant` refused with 400.
   - Filter fields where None means "all" (`ListRunsRequest`, `list_tokens`, `AuditQuery`) and `CreateTokenRequest` keep `"_"`. Response filters use `tenant_is` (default-aware).
   - Webhook `secret_ref` is not prefixed for `_`.
   - Regression test `p14_admin_api::default_tenant_token_starts_runs_and_stays_in_its_tenant` runs a token-mode server with the `signals` guest. The `_` token deploys and starts with bare names (201), reads its run, and lists only its run. It gets 404 on acme's run and 403 on `?tenant=acme`. acme gets 404 on the `_` run. The system admin sees both. **The test fails without the fix** (verified: 400 "tenant \"_\" is reserved"). There is also a unit test for the helpers.
2. **Connections authz.** `authz::required` now maps Put/Get/List/DeleteConnection to `SystemAdmin`. `auth_scopes.rs` expectations (an override of the inventory class) and the authz unit test `table_follows_the_inventory` were updated. `tenant_isolation.rs` now expects 403 `missing_scope` (was `forbidden_tenant`, or 200 empty list / 404) for tenant connection calls. 14-16's tenant hook stays behind it as defence in depth.

## Verification

- Task 1 verify (plan command, clock gate on `git diff fd5c0d1 -- crates/ostro-server/src`): **PASS**. `p14_admin_api` 5/5 at Task 1, 6/6 with the follow-up test, 6 consecutive green runs after the fix below.
- Task 2 verify (plan command): **PASS**. `p14_cli` 5 passed; `ostro tenant --help` lists `create`; `ostro auth token --help` lists `rotate`.
- Regression (local, slot-1): ostro-server `--lib p14::` 23; `p14_skeleton` (6/7, see below), `p14_guard` 8, `p14_authn` 9, `tenant_isolation` 4, `tenant_triggers` 3, `auth_scopes` 2, `auth_forwarding` 1. `ostro-api` lib 85, `openapi` 3 (snapshot unchanged), `parity` 11, `p14_routes` 8. `ostro-cli` + `ostro-client` all targets green (the `contract` test needs the ostro binary; it ran green after the Task 2 verify built it).
- `cargo clippy -p ostro-server -p ostro-api -p ostro-cli -p ostro-client -p ostro --all-targets --locked -- -D warnings`: clean. `cargo fmt --all --check`: clean on the merged main.
- **Plan gate: incomplete. The cluster was withdrawn; the consolidated gate is pending on dell-4.** `ostro-k8s-ci --parallel` was not run. **Unverified:**
  - every ci-local group at b0fffa3: toolchain, workspace clippy and warning-free `cargo build`, and the full `cargo test` shards test1/test2 (only the suites listed above ran)
  - failpoint suites fp1–3 (`--features failpoints` was not built)
  - musl release + musl binary size limit (two new non-generic services plus CLI code; size not re-measured)
  - musl zigbuild tests, SDK/polyglot/clients, Phase 13 mutant matrix and tier C, repl-conformance, server campaign smoke
  - semantics sleepers, wit-next fixtures, phase 7 legs, cargo deny / vendored checks, P12/P13 grep gates, workflow validation

## Deviations from Plan

### Auto-fixed / required

**1. [Rule 3 - Blocking] guard.rs: one line binding the routed control plane.** `delete_tenant` must count non-terminal runs through the routed fan-out. `fill_extensions` only gets the context, and `p14/mod.rs` is 14-01b-only. So `guard::wrap` (which has `routed`) calls `tenants_api::bind_routed(ctx, &routed)`, a ctx-keyed weak registry inside tenants_api.rs. Without a binding, delete answers 503 (fail closed). Commit c004a0e.

**2. [Rule 2 - Missing critical] ostro-api `TenantPatch`: JSON `null` clears.** Without a custom deserializer `null` read as "absent" (14-01 left the spelling to 14-17). I added `deserialize_with = "double_option"` on the six fields. Absent still keeps the value, and the OpenAPI snapshot is unchanged (I reverted a doc-comment tweak that changed it). Commit c004a0e.

**3. [Rule 1 - Bug] Rotated lifetime off by 1 ms.** `created_at_ms` and `expires_at_ms` came from two clock reads (found as a 2-in-5 flake of the "same lifetime" assertion). `mint` now reads the clock once. Commit c004a0e.

**4. [Rule 1 - Bug] p14_skeleton asserted the stub** (`tenants`/`api_tokens` slots `None` after `fill_extensions`). It now expects them filled. Commit c004a0e.

**5. Orchestrator follow-ups (out of the plan's files):** `p14/tenant_scope.rs` (14-16's), `p14/authz.rs` + `tests/auth_scopes.rs` (14-15's), `tests/tenant_isolation.rs` (expectations). See above. Commit 83d014e.

**6. TDD order:** the tests were written together with the implementation (no separate failing-test commit). Each task commit contains its tests.

### Plan text vs code (recorded, no change)

- `whoami` returns `{token_id, tenant, scopes, kind}`. The 14-01 `WhoAmI` DTO has no `expires_at_ms`, and adding it means a DTO + proto + OpenAPI change.
- "list/get return quotas and counts": `TenantInfo` (14-01) has no counts field, so quotas only.
- `RotateTokenRequest` (14-01) has `revoke_old` (default false). The CLI sends `revoke_old = !--keep-old`, so the CLI revokes by default as the plan says. A bare REST rotate keeps the old token.
- The REST adapter (14-02) rejects `tenant: "_"` on token creation (tenant grammar). `_` tokens can be created through the trait (gRPC/REST adapters would need 14-02's check to accept `_`). See orchestrator item 2.
- ext 112 (name, description, created_by, rotated_from) is only written at finalized level 2, per the phase rule. On a fresh cluster (level 1) these fields are empty in listings. The manual smoke shows an empty NAME column.

## Workflow hunks

None. The new tests live in existing crates and existing `cargo test` steps.

## Orchestrator actions / decisions

1. **Fresh clusters lose token names.** At finalized level 1 (the fresh-cluster default per 14-03), ext 112 is not written, so `name`/`description`/`created_by`/`rotated_from` are empty until `ostro cluster upgrade finalize`. This follows the phase rule. Decide whether a fresh 1.0 cluster should start finalized at level 2, or accept this.
2. **`_` tokens over REST:** `phase14_adapters::create_token` validates `tenant` with the D23 grammar, which refuses `_`. If operators should create default-tenant tokens from the CLI (`--tenant _`), 14-02's adapter check needs to accept `_` (ostro-api, not touched here).
3. **Pre-existing failure, not 14-17:** `cargo test -p ostro-server --test p14_skeleton start_triggers_is_none_without_kafka_and_refuses_a_kafka_list` fails when ostro-server is built alone (without the `kafka` feature, which the `ostro` package enables). The message is now "built without the kafka feature" instead of "Kafka triggers land in 14-32" (since 14-32's c822137). This belongs to 14-32.

## Self-Check: PASSED

- Files: `p14/tenants_api.rs` (`impl TenantsApi for TenantsService`), `p14/tokens_api.rs` (`impl ApiTokensApi for TokensService`), `tests/p14_admin_api.rs`, `ostro-cli/src/commands/{tenant,auth}.rs`, `ostro-cli/tests/p14_cli.rs` + 5 snapshots, `ostro-client/src/{tenants,api_tokens}.rs` present on main.
- Commits c004a0e, 83d014e, b0fffa3 are ancestors of main.
