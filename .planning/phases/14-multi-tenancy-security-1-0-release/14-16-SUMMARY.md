---
phase: 14-multi-tenancy-security-1-0-release
plan: 16
subsystem: server tenant scoping (guard hook), run listing, bulk, triggers
tags: [p14, tenancy, isolation, d23, sec-02, run-index, triggers, ext-114]
requires:
  - phase: 14-02
    provides: "ListRunsRequest.tenant, POST /hooks/{tenant}/{name} -> <tenant>/<name>, authn layer (PRINCIPAL scope)"
  - phase: 14-03
    provides: "final guard (tenant_scope::before async with the routed control plane, tenant_scope::after over the Result), MetaAuthenticator, token codec"
provides:
  - "p14/tenant_scope.rs: per-Method tenant rules (one match, no wildcard) for before and after"
  - "p14/upload_tenant.rs: ext 114 ModuleUploaders recorded on the existing-sha upload branch (CAS, feature-gated)"
  - "run lists honour ListRunsRequest.tenant (scan predicate + x_run_index Tenant rows, token kind n)"
  - "bulk jobs carry the creator's tenant (selection, list, get, cancel)"
  - "tenant-correct starts: cron (schedule-name tenant), webhook deliveries (webhook-name tenant), on-failure hook (hook workflow tenant); qualified webhook names accepted"
affects: [14-15, 14-17, 14-18, 14-19, 14-20, 14-21, 14-22, 14-23, 14-24, 14-25, 14-26]
tech-stack:
  added: []
  patterns:
    - "tenant_scope inspects requests/responses as &mut dyn Any behind thin generic entry points (one downcast per Method; a mismatch is a 500 caught by the sweep test)"
    - "tenant principal = Principal.tenant Some(t); every system-scope principal passes through"
key-files:
  created:
    - crates/ostro-server/tests/tenant_isolation.rs
    - crates/ostro-server/tests/tenant_triggers.rs
  modified:
    - crates/ostro-server/src/p14/tenant_scope.rs
    - crates/ostro-server/src/p14/upload_tenant.rs
    - crates/ostro-server/src/p14/guard.rs ('static bounds only)
    - crates/ostro-server/src/control/list.rs
    - crates/ostro-server/src/control/p11_search.rs
    - crates/ostro-server/src/control/p11_bulk.rs
    - crates/ostro-server/src/services/bulk.rs
    - crates/ostro-server/src/control/schedules.rs
    - crates/ostro-server/src/control/webhooks.rs
    - crates/ostro-server/src/services/dlq_hook.rs
key-decisions:
  - "Tenant principal = Principal.tenant Some(t); Dev/System/Node and tenant-less tokens are system scope and see every tenant with qualified names"
  - "A tenant request may never carry a qualified name, not even with its own prefix (400 invalid_name); nested names (schedule/webhook workflow, on-failure hook, bulk/DLQ filters, replay sample) are qualified too"
  - "Run-id methods resolve RunInfo.tenant through the routed get_run; foreign or missing runs answer the same body (control::runs::not_found)"
  - "Await-token completion: the token is parsed (no MAC check) to find the run; an unparsable token is left to the inner call (token_invalid)"
  - "Modules by sha (get/replay/attach signatures/retire) are visible iff the module registers a workflow in the tenant; retire of a module shared with other tenants is 403 forbidden_tenant"
  - "Connections stay cluster-scoped in 1.0 (the bridge resolves them by bare name for every tenant): tenants list none, get 404, put/delete 403"
  - "System-only methods are 403 forbidden_tenant in tenant_scope too (14-15's authz answers missing_scope first once it lands)"
  - "x_run_index Tenant rows (0x04, already written for every run with a tenant since Phase 11) serve tenant listings; no new rows, so no marker bump and no change to apply/index.rs"
  - "Bulk job tenant comes from the creating principal (no request field): JobFile.tenant (serde default, skipped when None)"
  - "On-failure hook runs take the tenant of their qualified hook workflow (D23), which equals the failed run's tenant for every tenant-defined policy"
  - "ext 114 seeds from the record's registered tenants on first write (so the first uploader is listed) and is only written at finalized level 2"
requirements-completed: []
duration: ~3h
completed: 2026-11-27
---

# Phase 14 Plan 16: Tenant isolation Summary

**`p14::tenant_scope` now enforces D-P14-7/9/38 for every guarded method (qualify/strip, 404 on foreign run ids, forced or refused tenant fields, filtered lists), run lists honour `ListRunsRequest.tenant` through the existing `x_run_index` Tenant rows, bulk jobs, cron fires, webhook deliveries and on-failure hooks stay in their tenant, and re-uploads of a stored sha record their tenant in ext 114.**

Requirement ids are not ticked here (14-27's, D-P14-35).

## Commits

| Task | Commit | What |
|------|--------|------|
| 1 | 7058478 | `feat(14-16): tenant scoping + per-tenant module registration` |
| 2 | e31e9b5 | `feat(14-16): tenant run index + tenant-correct triggers` |
| — | (this) | docs: SUMMARY, ROADMAP tick, STATE line |

## Per-method tenant-class mapping as landed (tenant principals; system scope passes through)

`before` (request) / `after` (response). "own run" = `get_run` on the routed control plane, `RunInfo.tenant == t`, else 404 `run <id> not found`. "qualify" = plain resource name required (400 `invalid_name` otherwise), then `<t>/<name>`. "force" = tenant field filled with `t` when empty, 403 `forbidden_tenant` when it names another tenant (`_` included). "strip" = `<t>/` removed from names; "filter" = foreign items dropped.

| Method(s) | before | after |
|---|---|---|
| `upload_module` | force `tenant`; `name` must be plain | module workflows filtered + stripped |
| `get_module` | module visible (registers a workflow in `t`) else 404 | workflows filtered + stripped |
| `list_workflows` | — | filter + strip names |
| `activate_workflow` | qualify `name` | strip |
| `start_run` | force `tenant`; qualify `workflow`, `queue` | — |
| `get_run`, `run_history`, `signal_run`, `cancel_run`, `terminate_run`, `retry_run`, `await_result`, `pause_run`, `resume_run`, `run_logs`, `rerun_run`, `analyze_rerun`, `requeue_dlq`, `dismiss_dlq` | own run | `get_run`: strip workflow/queue; `rerun_run`: strip workflow; `dismiss_dlq`: strip entry |
| `complete_token` | parse token -> own run (unparsable: inner call answers) | — |
| `list_runs` | force `tenant`; qualify `workflow`, `queue` filters | filter by workflow tenant + strip workflow/queue |
| `set/get_workflow_policy`, `pause/resume_workflow` | qualify workflow | pause state: strip |
| `set/get_queue_policy`, `get/put_queue_flow`, `pause/resume_queue`, `queue_demand` | qualify queue | flow info / pause state / demand: strip queue |
| `put_secret`, `list_secrets(tenant)`, `delete_secret(_, tenant)` | force tenant | list: keep `tenant == t` |
| `put_grant` | qualify `module`, recompute id | strip module + id |
| `list_grants(module)` | qualify module filter | filter + strip |
| `delete_grant(id)` | qualify the module inside `secret:<module>:<secret>` | — |
| `put/get/list/delete_connection` | put/delete 403, get 404 | list emptied |
| `put/get/delete_egress`, `list_egress` | qualify module | filter + strip module |
| `presigned_object` | unscoped (query signature is the credential) | — |
| `replay_module` | module visible; `Runs` each own run; `Sample.workflow` qualified | verdicts filtered + stripped, summary recomputed |
| schedules `get/pause/resume/delete/trigger`, `create/update` | qualify name (+ workflow on create/update) | strip name + workflow; list filtered |
| webhooks `get/delete`, `create/update` | qualify name, action workflow; `secret_ref` -> `<t>/<ref>` | strip name, workflow, secret_ref; list filtered |
| `deliver` (HookIngress) | qualify name (REST `/hooks/*` runs as System: passthrough) | — |
| `list_dlq` | qualify workflow/queue filters | filter by workflow tenant + strip |
| `create_bulk` | qualify filter workflow/queue; job tenant stamped by `control::p11_bulk` | — |
| `list_bulk`, `get_bulk`, `cancel_bulk` | — (server side: `p11_bulk` lists only the caller's jobs, 404 on another tenant's id, job untouched) | — |
| `get/set_failure_policy` | qualify scope; set: qualify `on_failure.workflow/queue` | strip hook |
| `list_modules` | — | module workflow lists filtered + stripped, empty modules dropped |
| `list_queues` | — | filter + strip |
| `retire_module` | module visible; shared with another tenant -> 403 | versions filtered + stripped |
| `migrate_runs` | qualify workflow | — |
| `create_token` (`tenant`), `list_tokens(tenant)`, `query` audit (`tenant`), `add_trusted_key` (`tenant`) | force | tokens / audit records: keep `tenant == t` |
| `revoke_token`, `rotate_token`, `remove_trusted_key` | passthrough (record tenant checked by 14-17 / 14-19) | — |
| `attach_signatures` | module visible | — |
| `limits`, `whoami`, `dev_time` | — | — |
| `ready` | never reaches the hooks | — |
| system-only: workers (list/drain/tokens/join), `dev_skip`, `backup`, `list_partitions`, all cluster methods, `run_module_gc`, all tenant methods, `get/set_module_policy` | 403 `forbidden_tenant` | — |

## Index layout and marker

No change to `apply/index.rs`, `store/derived.rs` or the rebuild marker: 14-01 found that `x_run_index` prefix `0x04` is already the Tenant row (`0x04 ‖ tenant ‖ 0x00 ‖ !created_at_ms ‖ !uuid`), written for every run with a non-empty tenant at creation and rebuilt from `runs` with the existing marker. Default-tenant runs have no Tenant row; a system listing of `tenant=_` uses the All/Status/Workflow prefix with the tenant applied in memory. Digest, `TABLES`, apply semantics and the format corpus are untouched (no partition file changed).

Listing (`control/p11_search.rs::plan`): first tag -> Tag; else workflow -> Workflow (already tenant-scoped by the qualified name); else a tenant id -> **Tenant (`0x04`, token kind `n`, `i1.n.<created>.<run>`)**; else dlq / status / All. Newest first only (the store has no reverse scan). Oldest-first listings use the Phase 11 budgeted scan; `control/list.rs::matches` now applies `tenant_matches` (`None`/`""` all, `_` default tenant, else exact) on both paths. Cross-partition fan-out unchanged.

## Trigger / index files actually edited (plan placeholders)

| Plan placeholder | Actual file | Change |
|---|---|---|
| `ostro-partition/src/apply/index.rs` | — | none needed (rows exist) |
| `ostro-server/src/control/list.rs` | `control/list.rs` + `control/p11_search.rs` | tenant predicate; Tenant prefix in the index plan |
| `ostro-triggers/src/cron/driver.rs` | `ostro-server/src/control/schedules.rs::build_schedule_start` (shared by the cron driver source and trigger-now) | one branch: a qualified schedule name fixes the tenant and qualifies the workflow in it (another tenant's workflow -> skip) |
| `ostro-triggers/src/webhook/deliver.rs` | `ostro-server/src/control/webhooks.rs` | `StartRun.tenant` from the webhook name (start action + the idempotency-lookup dry-run start); `webhook_from_dto` accepts `<tenant>/<name>` |

ostro-triggers carries no tenant logic (cron parsing / webhook verification only), so it is unchanged.

## Verification

- Task 1 verify: `cargo test -p ostro-server --locked --test tenant_isolation -- --test-threads=1` -> 4 passed (sweep over all 115 guarded methods; names/keys/lists; foreign run ids; every start site).
- Task 2 verify: `tenant_triggers` -> 3 passed; `PROPTEST_CASES=128 cargo test -p ostro-partition --locked --test rebuild_digest` and `--test format_corpus` -> 8 passed (827 s, ostro-partition untouched) and 2 passed; CLOCK_GATE_SCOPED on `git diff 4f02b67 -- crates/ostro-server/src crates/ostro-partition/src`: no hits (the worktree has no `p14-base` file; the base is the main commit the worktree started from).
- Regression (local, slot-1, before the commits): `cargo test -p ostro-server --lib` (217) and the integration suites webhooks, schedules, bulk_ops, search_api, runs_api, dlq_api, tenancy_api, modules_api, p14_guard, p14_skeleton, triggers_e2e, start_run_api, inventory_api, lifecycle_api, rerun_api, phase8_smoke, bridge_admin, tenant_quotas: all 19 binaries green.
- `cargo clippy -p ostro-server --all-targets --locked -- -D warnings`: clean (also re-run on merged main e31e9b5). `cargo fmt --all --check`: clean.
- **Plan gate: incomplete — cluster withdrawn; consolidated gate pending on dell-4.** Unverified (needs the dell-4 consolidated `--parallel`): the whole `cargo test` workspace run (test1 + test2 shards), failpoint suites, replication mutants, server campaign smoke, musl release + musl binary size limit (tenant_scope adds one non-generic match per hook; the generic entry points only box `&mut dyn Any`), musl zigbuild test, SDK/polyglot/clients groups, Phase 13 tier C, Phase 13 mutant matrix, semantics sleepers, wit-next fixtures, phase 7 failpoint legs, cargo deny / vendored / deny-fixture checks, P12/P13 grep gates, workflow validation, workspace-wide clippy and `cargo build` warning check.

## Deviations from Plan

### Auto-fixed / required

**1. [Rule 3 - Blocking] guard.rs: `'static` on `Core::call`'s `Req`/`Out`.** The hook signature `before<Req: Send>` cannot inspect an unbounded generic; `tenant_scope` takes `Req: Send + 'static` / `Resp: 'static` and works on `&mut dyn Any`. Two bounds (plus a comment) in `p14/guard.rs`; the hook order and call shape are unchanged (p14_guard green).

**2. [Rule 1 - Bug] Run lists ignored `ListRunsRequest.tenant`.** 14-02 added the field but neither the scan nor the index path read it; `control/list.rs::matches` now applies it and `p11_search.rs` (the indexed path, not in the plan's file list) picks the Tenant prefix.

**3. [Rule 1 - Bug] Tenant webhooks could not be created or deliver.** `webhook_from_dto` rejected `<tenant>/<name>` (plain-name grammar) and `act_start` sent `tenant: None` with a qualified workflow (400). Both fixed in `control/webhooks.rs` (the plan's webhook line); landed with Task 1 because the isolation test creates tenant webhooks.

**4. [Rule 1 - Bug] Tenant on-failure hooks failed to start.** `services/dlq_hook.rs` started the hook with `tenant: None` and a qualified hook workflow (400). It now passes the hook workflow's tenant (not in the plan's file list; the plan requires every StartRun site to keep the tenant).

**5. [Rule 2 - Missing critical] Bulk jobs had no tenant.** `services/bulk.rs` (`JobFile.tenant`, selection filter, `tenant_of`, `list_of`) and `control/p11_bulk.rs` (caller tenant from `ostro_api::auth::current()`, 404 on another tenant's job) — the guard's `before` only sees the routed `ControlPlane`, not the bulk registry, so the job-id rule lives with the bulk controller. Not in the plan's file list.

**6. No index change / marker bump.** The plan's Task 2 index hunk and marker bump were unnecessary (rows exist since Phase 11, see "Index layout"). `apply/index.rs` is untouched; the "rebuild == incremental" property is the existing Phase 11 test.

**7. TDD order.** The tests were written together with the implementation (no separate failing-test commit); each task commit contains its test.

### Plan text vs code (recorded)

- `RegisterDecision::Register` in the plan is `RegisterDecision::Proceed` in 14-01b's seam; the existing-sha branch already registered per tenant since Phase 13 (F4), so `upload_tenant` only records ext 114.
- `TokenInfo`/`AuditRecordView` lists are filtered in `after` as defence in depth; `revoke_token`, `rotate_token`, `remove_trusted_key` need the record's tenant, which only 14-17 / 14-19 can read: they pass through here.
- DLQ pages for a tenant are filtered after the inner listing (no tenant field on `DlqListRequest`): pages can be short but tokens stay valid; with a workflow/queue filter the selection is tenant-scoped already.

## Workflow hunks

None (new tests live in an existing crate).

## Orchestrator actions / decisions

1. **Connections are cluster-scoped for tenants** (403 on write, hidden on read). Tenant-owned connections need the bridge to resolve `<tenant>/<name>` for tenant runs (ostro-bridge sql/object/http lookups use the bare name). Decide whether 1.0 keeps this or schedules a bridge change.
2. 14-15 heads-up: once `authz::require` lands, tenant tokens on system-only methods get `missing_scope` before `tenant_scope` runs (both 403). A tenant-scoped matrix probe of a run-id method on a fake run id answers 404 (run-id rule), not "allowed".
3. 14-17 / 14-19: `revoke_token`, `rotate_token` and `remove_trusted_key` must check the record's tenant against `auth::current()` (404 for another tenant), like `p11_bulk` does for jobs.
4. 14-21 (UI): the guarded control plane strips tenant prefixes for tenant principals; UI links built from names must not re-qualify.

## Self-Check: PASSED

- Files: `p14/tenant_scope.rs`, `p14/upload_tenant.rs`, `tests/tenant_isolation.rs`, `tests/tenant_triggers.rs` present on main.
- Commits: 7058478, e31e9b5 on main.
