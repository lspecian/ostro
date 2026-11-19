# Workflow semantics (Phase 5)

This is the user-facing reference for how an Ostro workflow run behaves: signals, waits and
their timeouts, concurrency, cancellation, termination, retries, timeouts, await tokens,
code changes, continue-as-new, retention, memory, and tenancy. Each section states the
semantics, shows the API, CLI and Rust SDK usage, and lists the tests that prove it.

Conventions:

- **Proven by** cites `file::test`. Paths are relative to the repository root:
  `crates/ostro-server/tests/` (in-process server over REST and gRPC),
  `crates/ostro/tests/` (the spawned `ostro` binary and its CLI),
  `crates/ostro-partition/tests/` (deterministic apply), `crates/ostro-engine/tests/`
  (the Wasm engine with SDK guests).
- REST paths are under the one API port (default `http://127.0.0.1:7700`). The CLI
  (`ostro <verb>`) talks to `$OSTRO_SERVER` or `--server` and prints the REST body with
  `--json`. Exit codes: 0 ok, 1 API or connection error, 2 usage, 3 when `--wait` ends in
  a status other than `completed`.
- Durations in requests and config use hours as the largest unit: `90s`, `15m`, `24h`,
  `168h`. Days are rejected (`7d` gives "write it as 168h").
- Run statuses: `scheduled`, `ready`, `running` (live) and `completed`, `failed`,
  `cancelled`, `terminated`, `timed_out`, `continued_as_new` (terminal).

## 1. Starting a run and the journal

A start request (`POST /v1/workflows/{name}/runs`, `ostro run <name> --input <json>`) is
acknowledged once its `StartRun` command is durable in the partition log. The run is
pinned to the module sha of the workflow version active at that moment (or the version
named in the request).

**Start path (D24).** The API pre-encodes the run's `RunStarted` event, and apply journals
it at seq 0 as part of the `StartRun` command. An accepted run that has not started
executing therefore already has a one-event journal (`RunStarted` at seq 0, `next_seq` 1),
never an empty one. Everything that happens before the first activation is journaled after
it: buffered signals (§2), a cancel request (§5), and the `RunTerminated` marker of a
terminate or a schedule-to-start timeout (§6, §8). Exactly one `RunStarted` exists, always
at seq 0.

> Status at the time of writing: apply support for `StartRun.run_started` landed in 05-16
> (`crates/ostro-partition/tests/apply_run_started.rs`). The API switch and the updated
> end-to-end pins are plan 05-25. Builds before 05-25 still write `RunStarted` at the first
> activation, so a never-started run has an empty journal there.

**Inputs are journaled by apply.** Timer firings, wait timeouts, signal payloads and
matches, token completions and cancel requests are written into the run's journal by the
partition's deterministic apply, at the run's current `next_seq`. The executor never
invents them, so live execution and replay see exactly the same inputs in the same order.

Idempotent starts: with an `idempotency_key`, a second start of the same (qualified)
workflow with the same key returns the existing run (`created: false`), or 409 with
`conflict_policy: reject`. `--dry-run` / `dry_run: true` validates the request and reports
what would happen without submitting anything.

**Proven by:** `crates/ostro-server/tests/start_run_api.rs::rest_idempotency_keys_survive_restart`,
`start_run_api.rs::dry_run_start_validates_and_submits_nothing`,
`crates/ostro-partition/tests/apply_run_started.rs`,
`crates/ostro/tests/order_saga.rs::happy_path` (`RunStarted` first, one per run).

## 2. Signals

A signal is a named JSON payload sent to a run:
`POST /v1/runs/{id}/signals/{name}` `{"payload": ..., "request_id": "..."}`,
`ostro signal <run> <name> --payload <json> [--request-id <id>]`. In the SDK a workflow
waits with `ctx.wait_signal::<T>("name", timeout).wait()`.

- **FIFO per name, one signal for one wait.** Signals with the same name are delivered in
  the order the partition accepted them. A signal satisfies exactly one wait, the oldest
  open wait for that name. Two waits on `approve` need two `approve` signals.
- **Buffering.** A signal that arrives before the workflow waits for it (including before
  the run's first activation) is buffered, not dropped. Its payload is journaled at once as
  `signal_payload`; when a wait for that name starts, the oldest buffered signal is matched
  (`signal_matched{op, payload_seq}` pointing back at the payload event).
- **History shape (D21).** Every accepted signal journals `signal_payload{name, payload}`.
  A match journals `signal_matched{op, payload_seq}` immediately (a wait was open) or later
  (when the wait starts). Phase 5 never produces the older `signal_received` kind.
- **Dedupe.** An optional `request_id` (at most 64 bytes) makes a resend a no-op: the
  response says `duplicate: true` and nothing is journaled. Request ids are run-scoped and
  are deleted with the run.
- **Terminal runs.** A signal to a run that has ended is refused with 409
  (`details.reason = "run_terminal"`, plus the status) and journals nothing. Buffering
  applies only to live runs.
- **Large payloads.** Payloads above 256 KiB are stored as blobs and referenced from the
  journal; the history and the workflow see the same bytes.

**Proven by:** `crates/ostro-partition/tests/apply_waits.rs::signal_matches_the_oldest_wait_fifo`,
`apply_waits.rs::signals_before_waits_are_consumed_in_send_order`,
`apply_waits.rs::signal_run_validation_and_dedupe`,
`apply_waits.rs::signal_before_first_activation_is_pending_then_journaled_after_seq0_events`,
`crates/ostro-server/tests/semantics_e2e.rs::signal_timeout` (`before` mode buffering, 409
after the run ended), `semantics_e2e.rs::signal_immediately_after_start` (B1, arrival
order), `semantics_e2e.rs::large_payloads`,
`crates/ostro/tests/cli_verbs.rs::signals_fifo_dedupe_and_result`,
`crates/ostro/tests/order_saga.rs::cancelled_path` (late signal 409),
`crates/ostro-engine/tests/phase5_guests.rs::signals_fifo_returns_payloads_in_order`.

## 3. Wait timeouts

`wait_signal` and `create_await_token` take an optional timeout. When it expires before
the wait is satisfied, apply journals `wait_timed_out{op}` and the op completes with a
timeout failure: in the SDK, `Err(e)` with `e.is_timeout()` (WIT `failure::timeout`). A
signal that arrives after the timeout does not complete the old wait: it is buffered for a
later wait of that name, or refused with 409 if the run has ended meanwhile. A timeout
that races a signal already matched is a no-op. Sleep timers (`ctx.sleep`) have no
timeout; they simply fire (`timer_fired`).

```rust
match ctx.wait_signal::<bool>("approve", Some(Duration::from_secs(3600))).wait() {
    Ok(ok) => { /* approved or rejected */ }
    Err(e) if e.is_timeout() => { /* nobody answered within an hour */ }
    Err(e) => return Err(e.into()), // cancelled, see §5
}
```

**Proven by:** `crates/ostro-server/tests/semantics_e2e.rs::signal_timeout`,
`crates/ostro-partition/tests/apply_waits.rs::register_wait_with_past_due_times_out_once`,
`apply_waits.rs::signal_after_timeout_goes_to_the_inbox`,
`apply_waits.rs::timeout_after_signal_is_a_no_op`,
`crates/ostro-engine/tests/phase5_guests.rs::signals_timeout_mode`.

## 4. Concurrency: gather and race

`ostro_sdk::join!(a, b, ...)` waits for all ops (gather) and `ostro_sdk::select!(a, b, ...)`
returns the first to complete (race). Underneath, every `wait` that returns is journaled as
`wait_resolved{completed: [op, ...]}`, the **ordered** list of ops it returned.

- **Order = journal completion order.** The winner of a race is the op whose completion is
  earliest in the run's journal, not the one that "felt" first to the executor.
- **Replay is identical.** Replay returns exactly the journaled order, so a run replayed on
  another node (or after a crash) takes the same branch with the same values.
- **Losers are cancellable.** `ctx.cancel(&op)` completes a pending op with
  `Err(cancelled)`; if the op had already completed earlier in journal order, the earlier
  completion wins (D5). Cancel a race loser that must not be consumed later, as the
  order-saga example does with its approval wait after a cancellation.

**Proven by:** `crates/ostro-server/tests/semantics_e2e.rs::gather_race` (concurrent
signals, forced restart, fresh-runtime replay identical),
`crates/ostro-engine/tests/phase5_guests.rs::race_fuzz_replay_equals_live_for_20_seeds`,
`crates/ostro-engine/tests/sdk_cancellation.rs::select_loser_cancel_completes_with_cancelled`.

## 5. Cancel (graceful)

`POST /v1/runs/{id}/cancel` `{"reason"}`, `ostro cancel <run> [--reason ...]`.

- Cancel is **cooperative**. Apply journals `cancel_requested` once (a repeat answers
  `already_requested: true`) and wakes the run. The workflow observes it: by default every
  `Op::wait()` returns `Err(cancelled)` once a cancel is journaled (D11), and
  `ctx.cancellation()` is an op that completes on cancel, for `select!`.
- Code inside `ctx.shielded(|| ...)` is **not** interrupted, so compensations and cleanup
  run to completion after a cancel.
- **Status.** If the workflow returns the cancellation error, the run ends `cancelled`. If
  it handles the cancel and returns a value, the run ends `completed`; cancel is a request,
  not a verdict.
- A cancel before the first activation is recorded and delivered at the first activation
  (journaled after `RunStarted`), so the workflow still gets to run its cleanup.
- A cancel of a terminal run is 409 `run_terminal`. Cancel is forwarded to the successor
  of a continue-as-new chain (§11).

**Proven by:** `crates/ostro-server/tests/semantics_e2e.rs::cancel_cleanup` (undo steps in
reverse order, shielded), `crates/ostro/tests/order_saga.rs::cancelled_path` (via the CLI:
cleanup, `cancelled`, `result --wait` exits 3),
`crates/ostro-engine/tests/sdk_cancellation.rs::shielded_cleanup_runs_after_cancel`,
`sdk_cancellation.rs::timer_fired_before_cancel_is_slept` (a completion earlier in journal
order wins), `crates/ostro-server/tests/executor_phase5.rs::cancel_before_start_is_journaled_after_run_started`,
`executor_phase5.rs::cancel_before_start_runs_shielded_cleanup`,
`crates/ostro-server/tests/lifecycle_api.rs::cancel_before_start_is_flag_only_and_idempotent`,
`crates/ostro-partition/tests/apply_waits.rs::request_cancel_journals_once_wakes_and_cascades`,
`apply_waits.rs::cancel_is_forwarded_along_a_continue_as_new_chain`.

## 6. Terminate (hard stop)

`POST /v1/runs/{id}/terminate` `{"reason"}`, `ostro terminate <run>`.

- Terminate ends the run **immediately** as `terminated`. **No guest code runs**: no
  cleanup, no compensation. A live activation is stopped and its later appends are fenced.
- The journal gets a `run_terminated{reason}` marker as its last event (with D24 this holds
  for never-started runs too: `RunStarted` at seq 0, then `RunTerminated`).
- Attached children get a **graceful** cancel (§5), so their cleanup does run.
- A terminated run is resumable with retry (§7).

Use cancel when the workflow should clean up; use terminate when it must stop now.

**Proven by:** `crates/ostro-server/tests/semantics_e2e.rs::terminate_runs_no_guest_code`,
`crates/ostro-server/tests/executor_phase5.rs::terminate_stops_a_hot_activation`,
`crates/ostro-partition/tests/apply_lifecycle.rs::terminate_mid_run_journals_run_terminated_last`,
`apply_lifecycle.rs::terminate_notifies_the_parent_and_cancels_children_gracefully`,
`crates/ostro/tests/cli_verbs.rs::cancel_terminate_retry`.

## 7. Retry: resumed vs restarted

`POST /v1/runs/{id}/retry`, `ostro retry <run>`. The mode follows from how the run ended
(D1):

| ended as | retry | result |
|---|---|---|
| `failed` by the executor (trap, fuel, memory, nondeterminism, step attempts exhausted), `terminated`, `timed_out` | **resumed** | same run id; the journal is replayed and execution continues where it stopped |
| `failed` or `cancelled` by the workflow itself (its journal ends in a guest terminal event) | **restarted** | a new run with the same input and `retry_of` = the old id |
| `completed` | refused | 409 `run_completed` |
| live | refused | 409 `run_not_terminal` |

On resume, the first activation **re-arms every unresolved op** from the journal: pending
sleeps are rescheduled with their original due time (past-due ones fire at once), and
signal and token waits are registered again, so a signal sent after the resume completes
the original wait. A resumed run gets **no new run timeout** (§8). The response says which
mode was used (`resumed` / `restarted`); `--dry-run` reports it without acting.

**Proven by:** `crates/ostro-server/tests/semantics_e2e.rs::resume_after_signal_wait_timeout`,
`semantics_e2e.rs::resume_after_sleep_timeout`, `semantics_e2e.rs::retry_restart_links_a_new_run`,
`crates/ostro-server/tests/lifecycle_api.rs::retry_resume_restart_and_completed_rest_and_grpc`,
`crates/ostro-server/tests/resume_rearm.rs::a_signal_wait_is_rearmed_after_a_timeout`,
`resume_rearm.rs::a_past_due_sleep_fires_immediately_after_resume`,
`resume_rearm.rs::a_stale_wait_timeout_never_surfaces_as_timer_fired`,
`crates/ostro-partition/tests/apply_lifecycle.rs::retry_rejections`,
`crates/ostro/tests/cli_verbs.rs::cancel_terminate_retry`.

## 8. Run timeout and schedule-to-start timeout

**Run timeout** is declared by the workflow: `#[workflow(name = "order", version = 1,
run_timeout = "10m")]`. It is per workflow version metadata, not a per-request field.

- Deadline = run creation + timeout. When it passes, the run is **hard-stopped** like a
  terminate (§6): status `timed_out`, reason `run_timeout`, no guest code, journal ends in
  `run_terminated("run_timeout")`. It is resumable.
- Each continue-as-new successor gets a **fresh** deadline from its own creation.
- A **resumed** run (retry mode `resumed`) gets **no new run timeout**; its pending timers
  and waits are re-armed from the journal (§7).

**Schedule-to-start** (`schedule_to_start_timeout: "30s"` on start, `--schedule-to-start`,
or a workflow/queue policy) bounds how long a run may wait for its **first** activation. It
is measured from the run's `ready_at` (so a delayed start is not penalised) and applies to
the initial start only: once the first attempt is granted it is cleared, and it never
applies to a resumed run. On expiry the run ends `timed_out` with reason
`schedule_to_start`.

**Proven by:** `crates/ostro-server/tests/semantics_e2e.rs::run_timeouts`,
`crates/ostro-server/tests/executor_phase5.rs::run_timeout_while_sleeping_and_while_running`,
`crates/ostro-partition/tests/apply_lifecycle.rs::start_arms_the_run_timeout_and_completion_removes_it`,
`apply_lifecycle.rs::inbound_successor_start_stores_lineage_and_a_fresh_deadline`,
`crates/ostro-server/tests/semantics_e2e.rs::resume_after_sleep_timeout` (exactly one
`run_timeout`), `semantics_e2e.rs::schedule_to_start_without_capacity`,
`semantics_e2e.rs::schedule_to_start_with_capacity_never_fires` (measured from ready_at),
`crates/ostro-partition/tests/apply_lifecycle.rs::schedule_to_start_fires_before_the_first_grant`,
`apply_lifecycle.rs::first_grant_deletes_the_schedule_to_start_row_by_s2s_due_ms`.

## 9. Await tokens

A workflow creates a token, hands it to an external system, and waits for that system to
complete it:

```rust
let t = ctx.create_await_token::<serde_json::Value>(Some(Duration::from_secs(86_400)));
send_to_partner(t.token());          // e.g. inside a step
let reply = t.op().wait()?;          // timeout -> e.is_timeout()
```

The external system completes it with `POST /v1/tokens/complete`
`{"token": "ot1_...", "payload": ...}` or `ostro token complete <token> --payload ...`.

- **Format:** `ot1_` + lowercase unpadded base32 of the partition, run id, op and a
  truncated HMAC-SHA256. The token routes itself and is authenticated by the server key.
- **One-shot.** The first completion wins. A second completion, or a completion after the
  token's wait timed out or the run ended, is 409 (`token_already_completed` /
  `run_terminal`); a malformed or forged token is `token_invalid`.
- **A bearer credential.** The token is sent only in request bodies, **never in URLs**, and
  the server never writes it into logs or error messages. Human CLI output shows only a
  prefix.
- Tokens are run-scoped: a token completes exactly one op of one run.

**Proven by:** `crates/ostro-server/tests/semantics_e2e.rs::tokens`,
`crates/ostro-server/tests/lifecycle_api.rs::token_completion_rest_and_grpc`,
`lifecycle_api.rs::token_text_never_reaches_the_logs`,
`crates/ostro-partition/tests/apply_waits.rs::complete_token_once`,
`apply_waits.rs::complete_token_after_timeout_is_duplicate_and_terminal_is_rejected`,
`crates/ostro-engine/tests/phase5_guests.rs::tokens_complete_and_time_out`,
`crates/ostro/tests/cli_verbs.rs::token_complete`, and the unit tests `await_token_golden`,
`await_token_tamper_detected` in `crates/ostro-engine/src/replay/token.rs`.

## 10. Changing workflow code: versions and `patched`

- **Runs are pinned.** A run executes the module (sha) it was started with until it ends.
  Activating a new version affects new runs only.
- **`ctx.patched("change-id")`** lets one version of the code serve both old and new runs:
  it returns `false` while replaying a journal recorded before the change, and `true` for
  new execution (which records a `patch_marker`). The answer is memoised per change id
  within a run. Markers that the code no longer asks about are skipped on replay.
- **Removing a patch:** once no run started before the patch is still live, delete the
  `patched("id")` call together with the old branch. Journals recorded with the patch
  replay fine on the new code (their unqueried `patch_marker` is skipped). A journal from
  before the patch, replayed by code without the old branch, fails with nondeterminism
  (the run ends `failed`, resumable).
- There is **no API to re-pin a running run** to another module before Phase 13's
  migrate-runs; Phase 5 proves in-flight patching with a test-only override.

**Proven by:** `crates/ostro-engine/tests/patching.rs::v1_journal_replayed_by_v2_takes_old_path`,
`patching.rs::fresh_v2_takes_new_path_with_one_marker`,
`patching.rs::v2_journal_replayed_by_v3_skips_unqueried_marker`,
`patching.rs::v1_journal_on_v3_is_nondeterminism`,
`crates/ostro-server/tests/semantics_e2e.rs::patched_in_flight`,
`crates/ostro/tests/e2e_dev.rs::activate_v2_new_runs_use_v2`.

## 11. Continue-as-new

A long-lived workflow returns `Err(ctx.continue_as_new(&next_input))` to end the current
run as `continued_as_new` and start a successor with fresh history.
`ctx.continue_as_new_suggested()` hints when the history is getting large.

- **Lineage.** The successor records `continued_from`; the predecessor records
  `continued_to`, and its result is `{"next_run_id": "..."}`. The successor keeps the
  parent link, tenant, tags and traceparent, and gets its own run timeout deadline and
  freshly resolved policies. Exactly one successor is started, also across crashes.
- **Long-poll follow.** `GET /v1/runs/{id}/result?wait=30s&follow_continuations=true`
  (`ostro result <run> --wait --follow`) follows the chain and answers with the final
  run's result. Long polls are capped at 60 s per request; the CLI loops.

**Proven by:** `crates/ostro-server/tests/semantics_e2e.rs::continue_as_new` (chains,
restart mid-chain, no duplicate successor),
`crates/ostro-server/tests/lifecycle_api.rs::long_poll_follows_continuations_with_transport_parity`,
`crates/ostro-partition/tests/apply_lifecycle.rs::continue_as_new_hands_off_without_child_completed`,
`crates/ostro-server/tests/executor_phase5.rs::can_chain_copies_tenant_tags_traceparent_f11`,
`executor_phase5.rs::can_chain_keeps_the_parent_and_reports_once_pf_f2`,
`crates/ostro-engine/tests/sdk_cancellation.rs::continue_as_new_returns_the_continuation`.

## 12. Retention

Every terminal run is deleted after its retention period (D9). Terminal runs always
expire; there is no "keep forever".

- **Resolution order** (first that is set wins): the start request's `retention`
  (`--retention 48h`) > the workflow policy (`PUT /v1/workflows/{name}/policy
  {"retention": "72h"}`) > the queue policy > the `[runs] retention` config default.
  Defaults: **`168h` for `ostro server`, `24h` for `ostro dev`**. Write days as hours: the
  duration parser rejects `7d`. `[runs] max_retention` caps every level.
- The value is resolved **once, at start**, and stored with the run. **Policy changes
  affect future runs only.**
- **Expiry** fires the retention period after the run ended. The run is then gone from
  every API (GET and history 404, absent from lists), and its **idempotency key is free**:
  a new start with that key creates a new run. A run that was resumed (§7) before its
  expiry is not deleted by the old expiry timer; it gets a new one when it ends again.
- **Disk space caveat until Phase 8.** Expiry deletes the run from the partition state, but
  the **bytes stay in the WAL** (the log is not trimmed until Phase 8 snapshots). **Blobs
  referenced by expired runs are not garbage-collected in Phase 5.**
- **Known gaps** (05-09): expiry deletes the run-scoped rows only. A pending guest timer
  row of a terminal run and a cron dedupe row (`__cron/<schedule>` ‖ fire time) are not
  run-prefixed and outlive the expiry. A leftover timer row is harmless: a timer that fires
  for an absent run only loses its row. Cron dedupe rows accumulate until a later phase
  bounds them.

**Proven by:** `crates/ostro-server/tests/semantics_e2e.rs::retention` (expiry about 1 s
after completion, 404s, key reuse, resumed run kept, restart),
`crates/ostro-partition/tests/apply_retention.rs::expiry_deletes_every_run_scoped_row_and_frees_the_idempotency_key`,
`apply_retention.rs::expiry_keeps_a_dedupe_row_that_points_elsewhere`,
`apply_retention.rs::expiry_on_a_resumed_run_is_a_no_op`,
`apply_retention.rs::a_50k_event_run_expires_in_one_apply`, the unit tests
`retention_defaults_per_profile`, `request_beats_workflow_beats_queue_beats_config`,
`max_retention_caps_every_level` in `crates/ostro-server/src/retention.rs`,
`crates/ostro/tests/cli_verbs.rs::usage_and_connection_errors` (`7d` rejected).

## 13. Sleeping runs hold no memory

A run that waits for something that is not imminent is **evicted**: its Wasm instance is
dropped, nothing stays in memory, and the run is rebuilt from its journal when the input
arrives. Only short timer waits (within the engine's `hot_wait` window) and in-flight
effects may park an instance hot, within a node memory budget with LRU eviction.
**Signal and token waits always evict, whatever their timeout**, because their completion
depends on an external party. A thousand sleeping runs cost durable timer rows, not
memory.

**Proven by:** `crates/ostro-server/tests/semantics_sleepers.rs::sleepers_hold_no_memory`
(1,000 sleepers: 0 live activations, about 11-17 KiB RSS each, bound 64 KiB),
`semantics_sleepers.rs::wake_all_short_sleepers`,
`crates/ostro-engine/tests/hot_wait.rs::hot_wait_long_sleep_evicts_immediately`,
`hot_wait.rs::hot_wait_budget_lru`, the unit test
`signal_and_token_waits_are_never_hot_waitable` in `crates/ostro-engine/src/host/effects.rs`.

## 14. Tenancy and names (D23)

- **Qualified names.** Workflows, queues and deployed module names are stored as
  `<tenant>/<name>`. The default tenant is unqualified: plain `<name>`. In REST paths a
  qualified name is one path segment, `acme%2Forder-saga`. `ostro run --tenant acme` and
  `ostro deploy --tenant acme` set the tenant.
- **Name grammar:** `^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`: no `/`, no `:`, no leading `__`
  (`__ostro/` and `__cron/` are reserved). **Tenant ids:** `^[a-z0-9][a-z0-9-]{0,62}$`.
  `_` is the reserved spelling of the default tenant and cannot be created. Both are
  validated at the API (400 with `details.field`).
- **Tenant-scoped keys.** Because a run carries its qualified workflow, idempotency and
  dedupe keys, partition routing, cron keys and webhook start keys are tenant-scoped with
  no format change: two tenants can use the same workflow name and idempotency key without
  colliding. Composite keys built around a qualified name (for example the cron keys
  `__cron/<tenant>/<schedule>` with a fire time) are parsed **from the right**, because the
  qualified name itself contains `/`.
- **Run-scoped, not tenant-scoped:** signal `request_id`s (§2) and await tokens (§9).
- **Deployed module names** live on the workflow version (`WorkflowVersion.module`), not on
  the content-addressed module record, which stays keyed by sha and shared: a re-upload of
  the same bytes by another tenant cannot rename or take over a module.
- **Inside the workflow** (WIT and SDK), names are unqualified: `RunStarted.workflow` is
  the plain name and the tenant is a separate field.

**Proven by:** `crates/ostro-server/tests/tenancy_api.rs::two_tenants_same_workflow_and_key_never_collide`,
`tenancy_api.rs::upload_registers_qualified_versions_with_module_name`,
`tenancy_api.rs::start_rejects_bad_names_and_tenants`,
`crates/ostro-server/tests/executor_phase5.rs::qualified_workflow_runs_the_local_name_with_tenant_d23`,
the unit tests `tenant_grammar_boundaries`, `name_grammar_boundaries`,
`qualify_and_split_round_trip` in `crates/ostro-server/src/control/names.rs`,
`crates/ostro/tests/cli_verbs.rs::deploy_run_ls_inspect_history` (`%2F` segment).

## 15. Worked example

`examples/order-saga` combines most of the above: five steps (one with a retry policy), a
durable settle timer, an approval signal raced with `ctx.cancellation()`, and shielded
compensations in reverse order. Its README has the quickstart.
`crates/ostro/tests/order_saga.rs` drives it only through the CLI: approved, compensated
(failed step, exhausted retries, rejection), cancelled during the approval wait, and a
`kill -9` of `ostro dev` during the settle sleep followed by a restart and approval, with
every step executed exactly once.

See also: [03-architecture.md](03-architecture.md) (commands, keyspaces, storage),
[storage-format.md](storage-format.md) (frozen on-disk format),
[benchmarks.md](benchmarks.md) (D16 step definition and measured numbers).
