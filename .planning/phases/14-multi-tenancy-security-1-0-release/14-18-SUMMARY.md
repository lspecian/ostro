---
phase: 14-multi-tenancy-security-1-0-release
plan: 18
subsystem: audit capture (guard hooks, bridge secret access, cluster query, CLI)
tags: [p14, audit, sec-04, fail-closed, coalescing, fan-out, bridge, cli]
requires:
  - phase: 14-02
    provides: "AuditApi trait + REST/gRPC adapters, FakeAudit / FakeP14, the bearer layer, reasons audit_unavailable / audit_commit_failed"
  - phase: 14-03
    provides: "guard call sites (async reserve, after), MetaAuthenticator"
  - phase: 14-04
    provides: "AuditWriter (reserve / commit / record_coalesced / flush), journal format, verify_dir / offline, failpoints audit::writer_stall and audit::before_fsync"
  - phase: 14-15
    provides: "authz::require; the caller's principal on every forwarded / fan-out leg"
  - phase: 14-16
    provides: "tenant_scope forces AuditQuery.tenant for tenant principals and filters AuditRecordView lists"
provides:
  - "p14/audit_hooks.rs: classify (exhaustive Method -> action), reserve (fail closed), after (durable commit, W8), denied (coalesced authz failures), worker_event, AuditingAuthenticator"
  - "p14/audit_api.rs: NodeAudit (journal scan) and fan_out (per-member merge by (ts_ms, node, seq), cursor, partial_nodes)"
  - "ostro-bridge: SecretResolver::with_audit and secrets::audited::record_access (coalesced secret.access); Audited<SecretResolver> records through it"
  - "route: RouteCore::each_node (partial node fan-out), RoutedExtensions routes the audit slot, peers expose it; ostro-api GrpcControlPlane implements AuditApi"
  - "ostro-client Client::audit_query + AuditFilter; `ostro audit ls`"
affects: [14-22, 14-24, 14-25, 14-26, 14-27]
tech-stack:
  added: []
  patterns:
    - "guard hooks reserve before acting and commit before answering; high-volume events go through record_coalesced and never block"
    - "a node-local registry queried cluster-wide through RouteCore::each_node, which reports failed members instead of failing the call"
key-files:
  created:
    - crates/ostro-server/tests/audit_log.rs
    - crates/ostro-server/tests/audit_cluster.rs
    - crates/ostro-cli/tests/p14_audit_cli.rs
    - crates/ostro-cli/tests/snapshots/p14_audit_cli__{audit_ls_human,audit_ls_json_and_paging,audit_ls_errors_and_verify_shape}.snap
  modified:
    - crates/ostro-server/src/p14/{audit_hooks,audit_api}.rs
    - crates/ostro-bridge/src/secrets/{audited,resolver}.rs
    - crates/ostro-client/src/audit.rs
    - crates/ostro-cli/src/commands/audit.rs
    - "deviations: crates/ostro-server/src/p14/{guard,authn}.rs, crates/ostro-server/src/audit/writer.rs (bind_clock), crates/ostro-server/src/bridge_services.rs, crates/ostro-server/src/route/{fanout,mod,peer}.rs, crates/ostro-api/src/grpc_client.rs, crates/ostro-server/tests/p14_skeleton.rs, crates/ostro-client/src/lib.rs + crates/ostro-cli/src/commands/mod.rs (unused stub helpers removed)"
key-decisions:
  - "Worker joins are recorded by worker_event (join token id + worker id), not by the guard (classify(JoinWorker) = None). Worker token revokes are recorded by the guard (real actor), and worker_event ignores RevokeToken. Each gets exactly one record."
  - "Authorization failures are a coalesced `authz.denied`, keyed CoalesceKey::AuthFailure{token_id, client_ip: \"method:<Method>\"}. The 14-01 contract has no per-method key."
  - "Authentication failures are a coalesced `auth.failure`, keyed by (token id | malformed | missing, client ip). The client ip is recorded as `unknown`, because the API layer exposes no peer address."
  - "Secret access is audited inside SecretResolver (with_audit), so the connectors' own resolvers are covered too. Denied and failed resolutions are coalesced separately from successes (the outcome class is part of the key's origin)."
  - "Audit query cursor: `a1.<node>-<seq>[.<node>-<seq>]…`. A member that does not answer within 10 s goes to partial_nodes and keeps its cursor position."
  - "request_id is always null, because the API has no request-id concept yet."
requirements-completed: []
duration: ~5h
completed: 2026-11-27
---

# Phase 14 Plan 18: Audit capture, secret-access audit, cluster audit query Summary

**Every audited guarded method now takes a journal permit before it acts (503 `audit_unavailable` when none is available) and commits one chained record before it answers (500 `audit_commit_failed` with `executed = true` when the commit fails after the action). Authentication and authorization failures and secret resolutions become coalesced summaries with a count, and never contain a secret value. Worker joins and renewals are recorded as well. `GET /v1/audit` / `ostro audit ls` merge every member's journal by `(ts_ms, node, seq)`, page with an opaque per-node cursor, and name members that did not answer.**

Requirement ids are not ticked here; that is 14-27's job (D-P14-35).

## Commits

| Commit | Task | Content |
|---|---|---|
| 5422c81 | 1 | `feat(14-18): audit capture through the guard` (audit_hooks, guard 1 line, authn wrapper, audit_log.rs parts 1-3) |
| 3630c49 | 2 | `feat(14-18): coalesced secret-access audit` (bridge audited.rs + resolver.rs, bridge_services.rs, audit_log.rs part 4) |
| ca62d76 | 3 | `feat(14-18): cluster audit query + CLI` (audit_api, route, grpc_client, client, CLI, audit_cluster.rs, p14_audit_cli.rs) |
| 490c0d4 | fix | `fix(14-18): bind the audit writer clock before reserving; skeleton audit slot` (writer bind_clock, p14_skeleton assertion, unused `lands_in` stub helpers removed from ostro-client / ostro-cli) |

The tests and the code were written in the same session and committed together per task; there are no separate RED commits.

## Action names (the `classify` table, guard-recorded)

| Area | Method → action |
|---|---|
| Modules / workflows | UploadModule `module.upload`, ActivateWorkflow `workflow.activate`, RetireModule `module.retire`, RunModuleGc `module.gc`, AttachSignatures `module.signatures.attach`, MigrateRuns `run.migrate` |
| Policies | SetWorkflowPolicy `workflow.policy.set`, SetQueuePolicy `queue.policy.set`, PutQueueFlow `queue.flow.set`, SetFailurePolicy `failure_policy.set` |
| Run lifecycle | CancelRun `run.cancel`, TerminateRun `run.terminate`, RetryRun `run.retry`, PauseRun `run.pause`, ResumeRun `run.resume`, RerunRun `run.rerun` |
| Queues / workflows pause | PauseQueue `queue.pause`, ResumeQueue `queue.resume`, PauseWorkflow `workflow.pause`, ResumeWorkflow `workflow.resume` |
| Secrets / grants / connections / egress | PutSecret `secret.set`, DeleteSecret `secret.delete`, PutGrant `grant.put`, DeleteGrant `grant.delete`, PutConnection `connection.put`, DeleteConnection `connection.delete`, PutEgress `egress.put`, DeleteEgress `egress.delete` |
| Schedules / webhooks | `schedule.{create,update,delete,pause,resume,trigger}`, `webhook.{create,update,delete}` |
| DLQ / bulk | RequeueDlq `dlq.requeue`, DismissDlq `dlq.dismiss`, CreateBulk `bulk.create`, CancelBulk `bulk.cancel` |
| Workers | DrainWorker `worker.drain`, CreateWorkerToken `worker.token.create`, RevokeWorkerToken `worker.token.revoke` |
| Cluster | ClusterInit `cluster.init`, ClusterJoin `cluster.join`, CreateNodeToken `cluster.token.create`, ClusterRemove `cluster.remove`, ClusterDrain `cluster.drain`, ClusterReassign `cluster.reassign`, ClusterRebalance `cluster.rebalance`, `cluster.upgrade.{finalize,hold,release}` |
| Backup | Backup `backup.download` |
| Tenants / tokens / security | `tenant.{create,update,suspend,resume,delete}`, CreateApiToken `token.create`, RevokeApiToken `token.revoke`, RotateApiToken `token.rotate`, SetModulePolicy `security.policy.set`, AddTrustedKey `security.key.add`, RemoveTrustedKey `security.key.remove` |

Recorded outside the guard: `worker.join` and `worker.renew` (worker_event: actor = worker id, kind `worker`, target = join token id), `auth.failure` (coalesced; actor = token id | `malformed` | `missing`, kind `anonymous`, `details.reason`, `details.client_ip = unknown`), `authz.denied` (coalesced; actor = the principal, target = `Trait.method`, `details.reason`, e.g. `missing_scope`), `secret.access` (coalesced; actor `run:<id>`, kind `system`, target `<tenant|_>/<secret>`, `details.{module, origin, outcome, run_id}`).

Not recorded: reads, starts, signals, token completions, and every other method whose inventory `audited` column is false. A unit test pins `classify(m).is_some() == m.spec().audited` (except JoinWorker), unique names, and no wildcard arm.

Outcomes: `ok`; `failed:<api error code>`; `denied:<reason>` for `permission_denied` results (e.g. a signature policy), authz and authn failures, and ungranted secrets. Targets come from the guard's per-method helper: run id, qualified name, sha, token or key id, `node:<id>`, `partition:<n>`, `workflow:`/`queue:` scope. They never include request bodies or secret values. For system-scope principals the tenant comes from a qualified target (`<t>/<name>`).

## Fail-closed evidence (`audit_log.rs`, failpoints build, `--test-threads=1`)

- `writer_stall_refuses_audited_calls_before_acting`: with `audit::writer_stall=return` and `reserve_timeout = 300ms`, `POST /v1/runs/{id}/cancel` returns **503**, `reason = audit_unavailable`, with `retry_after_ms`. The run stays live, `GET /v1/runs` still answers 200, and no record is written. Once the failpoint is removed, the same cancel succeeds and leaves exactly one `run.cancel`.
- `commit_failure_after_execution_is_500_and_fails_closed`: with `audit::before_fsync=return`, a cancel returns **500** `audit_commit_failed`, `details.executed = true`, and the run does end cancelled. The next audited cancel on another run returns 503 `audit_unavailable`, and that run stays live. The writer stays poisoned until restart, and the full record is logged at ERROR.

## Coalescing evidence

- `secret_access_is_coalesced_per_origin_and_never_carries_the_value` (bridge server, fx-http, sealed secret `canary-key`): 20 requests to mock A with `x-api-key: {{secret:canary-key}}` plus 1 to mock B in one run produce **one record for A with `count = 20`** and one for B with `count = 1`. Both have target `_/canary-key`, actor `run:<id>`, `details.module = fx-http` and `outcome ok`, with `first_ts_ms <= last_ts_ms`. Records are flushed at shutdown, and the canary value appears in no journal file.
- `server_actions_are_recorded_once_with_actor_target_and_outcome`: 3 malformed bearers produce one `auth.failure` with actor `malformed` and `count = 3`. One request without a bearer produces one record with actor `missing`. A read token writing a secret twice (403 `missing_scope`) produces one `authz.denied` with `count = 2`.
- Bridge unit tests: granted and refused resolutions get distinct keys; operator and reserved lookups record nothing; `Audited` over a resolver that has its own sink records once.

## Query cursor and paging

`a1.` followed by `<node>-<seq>` pairs joined by `.` (e.g. `a1.1-42.3-7`), one pair per node seen so far: the last seq returned from that node. Each member returns its oldest matching records after its position, in seq order (at most `limit`, default 100, max 1,000). The entry node k-way merges by `(ts_ms, node, seq)`, so each node's records in a page are a seq prefix. The cursor is null once no node has more. A member that does not answer within 10 s is listed in `partial_nodes`, and its position stays where it was. Journal control records (`segment_open`, `audit_recovered`, `retention_trim`) appear only when the action filter names them. Coalesced `count` / `first_ts_ms` / `last_ts_ms` are exposed in `details`. `tenant = _` selects records without a tenant.

`audit_cluster.rs` (3 static mTLS nodes, token mode) checks the following:
- Upload through node 1, pause through node 2, and a pause (acme admin) plus a failed cancel through node 3 each appear once, with `node` = the ingress node.
- All three nodes answer the identical merged list, sorted.
- `limit = 2` paging walks the same list.
- The acme admin sees only `tenant = acme` records through every node.
- With node 3 stopped, the query answers with `partial_nodes = [3]` and the records from nodes 1 and 2.

## CLI

`ostro audit ls [--since 1h|ms|RFC3339] [--until …] [--action a|prefix.*] [--tenant t] [--actor a] [--limit n] [--cursor c] [--json]` shows a table (TIME, NODE, ACTION with `(xN)` for coalesced records, ACTOR (kind), TENANT, TARGET, OUTCOME), then `partial: nodes … did not answer` and `more: --cursor …` when they apply. `ostro audit verify --data-dir` stays the binary's offline intercept (14-04). It is exercised on a good journal and a tampered one through `audit::verify::offline` in `audit_log.rs`; the tamper error names the seq. ostro-cli has no ostro-server dependency, and Phase 14 plans make no Cargo edits.

## Verification (local, slot-5, CARGO_BUILD_JOBS=4)

Plan verify commands, run on the final tree (rebased onto 855be9d):
- Task 1: `--test audit_log -- --test-threads=1` **3 passed**. `--features failpoints --test audit_log -- --test-threads=1` **5 passed**. `audit_hooks.rs` has no `_ =>`.
- Task 2: `--test audit_log -- secret_access` passed (part of the runs above). `record_coalesced` is present in `audited.rs`. CLOCK_GATE_SCOPED on the diff from p14-base is empty (no clock reads were added; the per-leg wait is a tokio timeout).
- Task 3: `--test audit_cluster -- --test-threads=1` **1 passed**. `ostro-cli --test p14_audit_cli` **4 passed**.

Regression suites. The first set ran at the pre-rebase tree (base ded9e5d, plus the fix). The `*` suites ran again after rebasing onto 68cbfb2 / 855be9d.
- ostro-server: lib `p14::` 30 (39 after the 14-19 rebase*), `audit_journal` 21, `p14_skeleton` 7*, `p14_guard` 8*, `p14_authn` 9, `p14_admin_api` 6*, `auth_scopes` 2, `auth_forwarding` 1, `tenant_isolation` 4*, `tenancy_api` 3, `fx_canary` 4, `fx_http_e2e` 4, `sink` 19, `p7_control` 10, `cluster_api` 1, `p13_routing_unit` 29, `signed_modules` 3*, `p14_reseal` 3*, `fresh_cluster_level` 3*.
- ostro-bridge (all), ostro-api (all), ostro-client (all*, including `contract` once the `ostro` binary was built in the slot), ostro-cli (all*).
- `cargo clippy --all-targets -- -D warnings` on ostro-server (also with `--features failpoints`), ostro-bridge, ostro-api, ostro-client and ostro-cli: clean. `cargo fmt --check` on those crates: clean. `cargo build -p ostro-server`: 0 warnings. `scripts/p12-grep-gates.sh`: exit 0.

## Plan gate

**Gate incomplete: cluster withdrawn; consolidated gate pending on dell-4.** `ostro-k8s-ci --parallel` was not run (the user withdrew the cluster on 2026-11-27). The following are unverified for this plan:
- the full `cargo test` workspace (shards test1/test2), the musl release and musl zigbuild test shards, the musl binary size step (this plan adds code to ostro-server, ostro-api and ostro-bridge; no measurement was taken);
- the failpoint suites group (only `ostro-server --features failpoints --test audit_log` and the clippy failpoints build ran locally), the replication and Phase 13 mutant matrices, the server campaign smoke, Phase 13 tier C;
- the polyglot / SDK / clients / ostro-build steps, cargo deny, the wit-next fixtures, workflow validation;
- the ostro-server test suites not listed under Verification above. Any server test whose context is built with `p14::build_ctx` and calls audited methods through the guard now depends on `audit::writer::bind_clock` (see deviation 3).

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Authorization-failure hook point: `p14/guard.rs` (1 line, 14-03's file)**
- **Found during:** Task 1. `authz::require` runs in the guard before `reserve`, and no hook sees a refusal.
- **Fix:** `admit(m).inspect_err(|e| audit_hooks::denied(&self.ctx, m, e))?`. The hook order is unchanged.

**2. [Rule 3 - Blocking] Authentication-failure hook point: `p14/authn.rs` (14-03's file)**
- **Found during:** Task 1. The plan said "give the layer an on_auth_failure callback, if a hook point is missing STOP". ostro-api's `authn_layer` has no callback and no peer address. The authenticator itself is the hook point: `authn::authenticator` now returns `audit_hooks::auditing(MetaAuthenticator)`, a decorator that records failures (not `Unavailable`). No API contract changed. Two limits remain: bearer headers that fail the layer's own parsing (`Basic …`, two `Authorization` headers) never reach the authenticator and are not recorded, and the client ip is `unknown`.

**3. [Rule 1 - Bug] Guarded calls hung on contexts whose writer never started: `audit/writer.rs` `bind_clock` (14-04's file, additive)**
- **Found during:** the regression run. `p14_guard` hung: 14-04's writer holds commits until `set_clock`, which only `writer::start` calls. In production `p14::start` runs before the client listener serves, but test contexts built by `build_ctx` never start the writer.
- **Fix:** `pub fn bind_clock(ctx)` hands the writer `P14Ctx.clock` (idempotent, the same clock `start` passes). `reserve`, `denied` and `worker_event` call it.

**4. [Scope extension per p14-notes] Connector secret resolution: `ostro-bridge/src/secrets/resolver.rs` + `ostro-server/src/bridge_services.rs`**
- The HTTP (per tenant), SQL and object connectors hold a concrete `SecretResolver`, so `Audited` alone missed them. `SecretResolver::with_audit(sink)` records every `resolve` through `audited::record_access` (`resolve_unaudited` is the raw path, which `Audited` uses to avoid a double record). `bridge_services.rs` `ChainSpec` carries the audit sink and puts it on every chain resolver (3 small hunks in a 14-01b call-site file). No connector signature changed. SQL and object only use `resolve_operator`, which stays unaudited per access by design.

**5. [Rule 3 - Blocking] Fan-out for the audit slot: `route/{fanout,mod,peer}.rs` + `ostro-api/src/grpc_client.rs`**
- The Phase 13 node fan-out (`all_nodes`) is complete-or-error, peers had no audit slot, `GrpcControlPlane` did not implement `AuditApi`, and `RoutedExtensions` did not route the slot.
- **Fix:** `RouteCore::each_node` (per-node results; a forwarded-in call answers locally), `RoutedExtensions` routes `audit` to `p14::audit_api::fan_out`, `PeerApi::grpc` fills `audit`, and `impl AuditApi for GrpcControlPlane` (`ostro.api.v1.Audit/Query`).

**6. [Rule 1] `tests/p14_skeleton.rs`: the slot assertion now expects the audit slot to be filled (the same kind of one-line change 14-17 made for its slots). It was merged with 14-19's security-slot line during the rebase.**

**7. [Rule 3 - Blocking] `ostro-client/src/lib.rs`, `ostro-cli/src/commands/mod.rs`: the Phase 14 stub helpers `lands_in` were removed.** Audit was their last user (14-17 and 14-19 had already replaced the other stubs), so `-D warnings` would fail on dead code.

### Notes

- A tenant principal cancelling an unknown or foreign run id gets the tenant hook's 404 before `reserve` (14-03 guard order), so that attempt is not audited. The same call by a system principal executes and is recorded `failed:not_found`.
- `worker_event` is synchronous (seam signature), so its commit is spawned. Worker joins and renewals are recorded but are not fail-closed. A reserve failure is logged at ERROR.
- No OTLP leg in the canary test (no collector configured). The 14-04 OTLP export copies the journal record, which by construction holds no value.

## Workflow hunks (branch `wip/14-18-workflows`, e157f90 on top of 490c0d4)

The failpoint cases of `audit_log` run only with `--features failpoints`:

```diff
--- a/scripts/ci-local.sh
-FAILPOINT_CRATES=(… ostro-server:p13_promote ostro-server:audit_journal)
+FAILPOINT_CRATES=(… ostro-server:p13_promote ostro-server:audit_journal ostro-server:audit_log)
--- a/.github/workflows/ci.yml   (step "Failpoint suites", after the audit_journal line)
           cargo test -p ostro-server --features failpoints --test audit_journal --locked -- --test-threads=1
+          cargo test -p ostro-server --features failpoints --test audit_log --locked -- --test-threads=1
```

The `ostro-k8s-ci` runner may need the new step name `ostro-server:audit_log` in its group map.

## Orchestrator decisions / follow-ups

1. **Client ip for `auth.failure`:** recording it needs the peer address in the bearer layer (ostro-api `authn_layer`, e.g. axum `ConnectInfo`) plus serving with connect info (`server.rs` / `p7::serve_client`, 14-01b-only). No plan owns this. Until then the key is (token id, `unknown`).
2. **Request ids:** `request_id` is always null. The API has no request-id header or task-local. Add one (ostro-api) if D-P14-11's request id is required for 1.0.
3. **Tenant-scope 404s are not audited** (see Notes). If a tenant's probing of foreign run ids must show up in the log, `tenant_scope::before` refusals would need a `denied`-style hook (guard.rs).
4. Apply `wip/14-18-workflows` and teach the runner the new failpoint step.
5. 14-26 completes the D-P14-13 table against the real 14-17 / 14-19 implementations (B6). 14-25's `docs/audit.md` should state "check before retrying" for non-idempotent creates after a 500 `audit_commit_failed` (token create, upload).

## Self-Check

- FOUND (at 490c0d4): crates/ostro-server/src/p14/{audit_hooks,audit_api}.rs, crates/ostro-bridge/src/secrets/{audited,resolver}.rs, crates/ostro-client/src/audit.rs, crates/ostro-cli/src/commands/audit.rs, crates/ostro-server/tests/{audit_log,audit_cluster}.rs, crates/ostro-cli/tests/p14_audit_cli.rs + its 3 snapshots
- FOUND commits: 5422c81, 3630c49, ca62d76, 490c0d4; branch wip/14-18-workflows at e157f90

## Self-Check: PASSED
