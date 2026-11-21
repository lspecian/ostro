---
phase: 07-workers-placement-flow-control
plan: 14
subsystem: control plane (ostro-server control/p7_*)
tags: [control-plane, flow-control, pause, resume, retry-alias, workers, join, tokens, demand, ops-01]
requires:
  - phase: 07-workers-placement-flow-control
    provides: "07-04 PauseRun/ResumeRun apply; 07-08 NodeFlow + policy watcher (changed() on meta apply); 07-09 JoinService / WorkerRegistry; 07-11 ControlPlane Phase 7 methods + ostro_api::phase7 types; 07-02 ServerControlPlane.p7 (B3) + workers::demand::snapshot"
provides:
  - "All 15 Phase 7 ControlPlane methods implemented on ServerControlPlane (no `lands in 07-14` default left in ostro-server)"
  - "control/p7_flow.rs: queue flow GET/PUT (meta QueuePolicy F2 CAS) + queue/workflow pause/resume"
  - "control/p7_pause.rs: run pause/resume (Cmd::PauseRun{operator}/ResumeRun) + the paused-run /retry alias"
  - "control/p7_workers.rs: list/drain workers, join tokens, join, demand"
  - "tests/p7_control.rs: REST + gRPC suite (10 tests)"
affects: [07-16, 07-17, 07-19, 07-20, 07-22]
tech-stack:
  added: []
  patterns:
    - "read-modify-CAS of meta policy records preserving the other phase's fields"
    - "ControlPlane override intercepting before the Phase 5 implementation (retry alias) instead of editing the Phase 5 file"
key-files:
  created:
    - crates/ostro-server/src/control/p7_flow.rs
    - crates/ostro-server/src/control/p7_pause.rs
    - crates/ostro-server/src/control/p7_workers.rs
    - crates/ostro-server/tests/p7_control.rs
  modified:
    - crates/ostro-server/src/control/mod.rs
key-decisions:
  - "The /retry alias sits in the mod.rs `retry_run` override (p7_pause::retry_alias before lifecycle::retry_run): retry lives in control/lifecycle.rs, not runs.rs, and lifecycle.rs is not in files_modified"
  - "Refused join = InvalidArgument + details.reason join_token_invalid (07-11 contract kept; the ErrorCode set has no Unauthenticated, and the answer does not reveal which tokens exist)"
  - "PUT flow writes every F2 field including `paused` (the flow body is the whole F2 view); Phase 5 fields and ext carried over"
  - "list_workers merges sessions per worker id (newest session's labels/slots/connected_at, free slots and leases summed, draining only if all sessions drain)"
  - "Run pause/resume time = cp.now_ms() (the runtime clock, the same injected server clock)"
requirements-completed: [OPS-01]
metrics:
  duration: "~50 min"
  completed: 2026-11-21
---

# Phase 7 Plan 14: Phase 7 control plane Summary

**Every Phase 7 `ControlPlane` method now has a server implementation. Queue flow is a read-modify-CAS of the meta `QueuePolicy` F2 fields, with the Phase 5 fields kept and a stale `expected_version` answered with 412. Queue and workflow pause/resume CAS the `paused` flag, and the 07-08 watcher wakes the dispatchers. Run pause/resume submit `PauseRun{operator}` / `ResumeRun`, and `/retry` on a paused live run resumes it. Worker list/drain, join tokens, join and demand delegate to `P7Shared`. A 10-test suite exercises everything over REST and gRPC. OPS-01's control-plane part closes here.**

## Commits

1. `9865bd6` feat(07-14): flow config and pause/resume control plane
2. `10c3905` feat(07-14): worker admin, join tokens, join and demand endpoints

## Method → implementation

| ControlPlane method | Implementation |
|---|---|
| `get_queue_flow` | `p7_flow::get_queue_flow`: `QueuePolicy` F2 fields → `QueueFlow`, version = meta version (absent → defaults, 0) |
| `put_queue_flow` | `p7_flow::put_queue_flow`: `QueueFlow::validate` → loop { read; `expected_version` ≠ current → 412 `details.current_version`; Put with `expected_version = current` } (8 attempts; a lost race with an explicit version → 412, without one → re-read) |
| `pause_queue` / `resume_queue` | `p7_flow::set_paused` on `QueuePolicy.paused` (idempotent, `changed`) |
| `pause_workflow` / `resume_workflow` | same on `WorkflowPolicy.paused`; 404 for an unknown workflow |
| `pause_run` | `p7_pause::pause_run`: validate detail; read record (404 / 409 `run_terminal`); submit `Cmd::PauseRun{reason: 0, detail, token: None, requested_at_ms: now}`; `changed = pause.is_none()` before the submit |
| `resume_run` | `p7_pause::resume_run`: `Cmd::ResumeRun{requested_at_ms: now}`; `changed = pause.is_some()` |
| `retry_run` (alias) | `p7_pause::retry_alias` first: non-terminal + paused → ResumeRun, 200 `{mode: resumed, run_id, dry_run}` (dry run submits nothing); otherwise `lifecycle::retry_run` unchanged |
| `queue_demand` | `workers::demand::snapshot(&cp.p7, q)` mapped to `phase7::DemandSnapshot` (placement via `Placement::parse`, default embedded) |
| `list_workers` | `registry.sessions()` merged per worker id, `node_id = [node] node_id` |
| `drain_worker` | `registry.drain(id)`: `Some(already)` → `DrainResult`, `None` → 404 |
| `create_worker_token` | `CreateWorkerTokenRequest::validate`; `join.create_token(ttl_ms → Duration, join_url)` |
| `list_worker_tokens` | `join.list_tokens()` |
| `revoke_worker_token` | `join.revoke_token(id)` (404 when absent) |
| `join_worker` | `join.join(req)` |

Meta key names follow D23 through `policy::policy_key` (bare = the default tenant, `acme/q` qualified), the same as the Phase 5 policy routes. Responses echo the name as given.

## Where the /retry alias sits

The alias is in `control/mod.rs` `ServerControlPlane::retry_run`, which calls `p7_pause::retry_alias(self, &r)` before `lifecycle::retry_run`. The plan expected it in `control/runs.rs`, but the retry implementation is in `control/lifecycle.rs`, which is outside files_modified. The override is in a file this plan owns, so no Phase 5 file changed. Lookup failures (bad id, unknown run, partition down) fall through to the D1 path, so its error answers are unchanged. The cost is one extra run-record read per retry.

## Join error codes

| Situation | Answer |
|---|---|
| Unknown, revoked or expired token, or wrong secret | `InvalidArgument` (400 / `INVALID_ARGUMENT`) with `details.reason = "join_token_invalid"`. The token is never echoed |
| Malformed CSR, worker id or labels | plain `InvalidArgument` |
| No cluster CA / no cluster endpoints (`[workers] enabled = false`) | `Unavailable` |

## Join URL in `join_command`

- The scheme follows the effective TLS mode.
- The host is chosen in this order:
  1. the first ACME domain (acme mode only);
  2. the authority of `[connectors] public_base_url`;
  3. `[api] listen`, with an unspecified IP shown as loopback.
- **Limitation:** the control plane does not know the bound port. An ephemeral `listen = …:0` shows `:0` in the join command, and tests only check the `http://127.0.0.1:` prefix. A fix needs server.rs (07-02-owned) to hand the bound `local_addr` to the control plane, for example through a `OnceLock` on `ServerControlPlane` set after bind (follow-up).

## Tests (`crates/ostro-server/tests/p7_control.rs`, 10 tests)

Timings are single runs at load average 20–60.

| Test | What | Time |
|---|---|---|
| `flow_round_trip_validation_and_cas_rest_and_grpc` | defaults v0; Phase 5 policy kept through PUT flow (all fields incl. selector/spill/4 rate specs); stale 412 + `current_version`; 4 invalid bodies 400/422 with nothing stored; gRPC get/put, stale FAILED_PRECONDITION, invalid INVALID_ARGUMENT | 0.13 s |
| `queue_pause_holds_backlog_and_resume_dispatches` | REST pause (changed, then idempotent); 3 runs held 1 s (not granted, `ready`), another queue unaffected; gRPC resume dispatches all | 1.5 s |
| `workflow_pause_holds_two_queues_and_resume_dispatches` | gRPC pause; 404 unknown on both transports; 4 runs over 2 queues held; REST resume completes them | 1.5–2.4 s |
| `run_pause_resume_idempotent_and_terminal_rest_and_grpc` | pause during a durable sleep (mark reason 0, detail), twice idempotent; timer fires while paused with no progress; gRPC resume completes; 409 `run_terminal` REST/ALREADY_EXISTS gRPC; 404 / 400 / detail too long 400 | 3.0 s |
| `run_pause_of_a_running_activation_stops_progress` | `timeouts busy_steps 20000` paused while Running: `next_seq` frozen over 1 s; resume → `busy:20000` | 2.6 s |
| `retry_on_paused_run_resumes_it` | live unpaused retry still 409 `run_not_terminal`; paused: dry run reports resumed and changes nothing; REST retry resumes (`resumed_count` stays 0); gRPC Retry resumes | 2.0 s |
| `cancel_while_paused_stays_paused_until_resume` | cancel journaled, run stays paused and non-terminal; resume → Cancelled | 1.5 s |
| `terminate_while_paused_is_immediate` | Terminated, pause mark cleared | 0.5 s |
| `worker_tokens_join_and_revoke_rest_and_grpc` | 201 + `Cache-Control: no-store`, join command; list (REST/gRPC) never contains the secret; REST join → cert parses as `WorkerIdentity{token_id, worker_id}`; wrong secret 400 `join_token_invalid`; gRPC join; gRPC revoke → REST 400 / gRPC INVALID_ARGUMENT; 404 on re-revoke; REST revoke 204; `ttl_ms = 0` 400 | 0.16 s |
| `workers_list_drain_and_demand_with_a_connected_worker` | empty list, drain 404; an in-process `ostro_worker` joins through this server's REST route and connects; listed with labels/slots/node_id (REST + gRPC); drain → `already_draining` false then true (gRPC), draining with 0 free slots; demand JSON shape + gRPC | 3.0 s |

Unit tests: `p7_flow` 2 (flow ↔ meta round trip, Phase 5 fields/ext preserved) and `p7_workers` 2 (session merge, demand placement mapping).

## Verification

- Task 1 verify (`-- flow pause retry cancel terminate`): `test result: ok. 8 passed`; clippy `-D warnings` clean.
- Task 2 verify:
  - `cargo test -p ostro-server --test p7_control --locked`: `10 passed`.
  - `cargo clippy -p ostro-server --all-targets --locked -- -D warnings`: clean.
  - `cargo test -p ostro-server --locked --no-fail-fast` (on 9865bd6 + Task 2): 47 `test result: ok` lines, 417 tests passed, 0 build warnings, one failure: `leases::heartbeat_keeps_long_activation_leased` (`activations() == 2`, a 400 ms lease / 100 ms heartbeat timing test, load average ~45). `--test leases` alone: 4 passed. This test does not touch the control plane.
- After rebasing onto main a4eee8f (07-12 and 07-13 landed): clippy clean, p7_control 10 passed, lib `control::` tests passed.
- `! grep -rn "lands in 07-14" crates/ostro-server/src crates/ostro-api/src/fake.rs` succeeds. The string remains only in `ostro-api` domain.rs/phase7.rs, the trait defaults, and in the phase7_surface test of those defaults.
- Not run: `bash scripts/ci-local.sh`, because of the shared-load policy (four other agents and the Phase 8 campaign were running). 07-22 runs the gate.

## Deviations from Plan

1. **[Plan defect, worked around in scope] The retry alias is not in `control/runs.rs`.** Retry lives in `control/lifecycle.rs`. Rather than STOP or edit an unlisted Phase 5 file, the alias is an interception in the `retry_run` override in `control/mod.rs`, which this plan owns. `runs.rs` is unchanged.
2. **"Return the resulting run status".** The 07-11 contract for pause/resume is `RunPauseState{run_id, paused, changed}` (no status field), and that is what is returned.
3. **Run pause tests use the `timeouts` guest** (`sleep_ms`, `sleep_forever`, `busy_steps`) instead of `long-journal` / sdk sleep. It gives a deterministic sleeping run and a long single activation.
4. **Drain** only stops assignment (07-09 has no server→worker Drain message). The test asserts registry state (draining, 0 free slots), not a message on the worker.
5. **Not done here (files not in this plan):**
   - `tls::acme::set_clock(ctx.clock.clone())` (07-10 follow-up) and the `tls::init` `auto_sealing_key` dev heuristic (07-06/07-15 follow-up) both need p7.rs or server.rs, neither of which is in files_modified. Both are still open for the p7.rs owner or 07-22.
   - The bound-port join URL (see above) needs server.rs.

## Follow-ups

- p7.rs owner: call `tls::acme::set_clock(ctx.clock.clone())` in `p7::init`, and replace `tls::init`'s `auto_sealing_key` profile heuristic with an explicit profile.
- server.rs owner: give `ServerControlPlane` the bound client address so `join_command` shows the real port.
- 07-16: `queue_demand` already maps whatever `workers::demand::snapshot` returns, so nothing changes on the control-plane side.

## Self-Check: PASSED

- FOUND: control/p7_flow.rs, control/p7_pause.rs (9865bd6); control/p7_workers.rs (10c3905); tests/p7_control.rs (963 lines, both commits)
- FOUND: commits 9865bd6, 10c3905 on main
