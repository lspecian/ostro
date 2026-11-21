---
phase: 07-workers-placement-flow-control
plan: 12
subsystem: remote execution (ostro-server workers/, executor seam)
tags: [workers, placement, selectors, remote-runner, start-effect, secrets, await-tokens, paging, work-01, work-02, work-04, work-06]
requires:
  - phase: 07-workers-placement-flow-control
    provides: "07-05 Placement/Reservation/Job.reservation/ExecOutcome::Abandoned; 07-07 worker runtime (RemoteSink framing, EffectProxy, ordered input bridge); 07-08 ExhaustPolicy; 07-09 WorkerRegistry (pick/attach_job/rpcs/changed), Execute service, harness; 07-11 ostro_api::phase7::Selector; 07-13 SpillHandle (spilling/note_overflow/changed)"
  - phase: 06-effects-bridge-connectors-secrets
    provides: "06-14 PartitionSink::with_bridge + ServerEffectDriver (unchanged)"
  - phase: 08-triggers-bottomless-observability
    provides: "08-17 GuestLogSink + ActivationSink::guest_log"
provides:
  - "workers::selector: QueuePlacement (placement + compiled selector), PlacementPolicies (per meta applied index), PolicySource"
  - "workers::placement: NodePlacement (embedded / workers / auto embedded-first overflow), EmbeddedSlot, WorkerSlot, SpillSource, install()"
  - "workers::remote_executor: PlacementExecutor, RemoteRunner, rebuild_request, RemoteHooks (test tap)"
  - "executor.rs: pub(crate) sink_for / module_sha / prelude / map_outcome / fail_run; ExecutorHooks.remote"
  - "D-W27 implemented: leader-only await-token minting with re-mint + forced eviction"
  - "tests: worker_remote (7), worker_paging (1); harness ServerWorkers / spawn_isolated / wire_listener / journal_shape"
affects: [07-13, 07-14, 07-16, 07-17, 07-18, 07-21, 07-22]
tech-stack:
  added: []
  patterns:
    - "one sink construction site (EngineExecutor::sink_for) for embedded and remote jobs"
    - "leader-authoritative remote job: every worker event decoded server-side and appended through PartitionSink"
    - "remote Reactivate mapped to Abandoned (never re-send Activate with the same token)"
key-files:
  created:
    - crates/ostro-server/tests/worker_remote.rs
    - crates/ostro-server/tests/worker_paging.rs
  modified:
    - crates/ostro-server/src/workers/selector.rs
    - crates/ostro-server/src/workers/placement.rs
    - crates/ostro-server/src/workers/remote_executor.rs
    - crates/ostro-server/src/executor.rs
    - crates/ostro-server/tests/common/p7_workers.rs
key-decisions:
  - "W6 adapted: a redacted value containing `{{` is accepted only when it consists of well-formed `{{secret:NAME}}` placeholders (bridge grammar); anything else is rejected `template in redacted value`. Rejecting every `{{` (the plan text) would make templated, redacted headers (the D-W3 canary case) impossible on workers"
  - "D-W27: a batch whose AwaitTokenCreated differs from the leader mint is cut after that event, re-minted with the meta token key, appended, and answered conflict_expected(next): the worker evicts and the run replays the leader token"
  - "Remote Reactivate -> ExecOutcome::Abandoned (re-grant n+1, no failure counted): the worker's duplicate-lease check could swallow a same-token Activate while the previous activation unregisters"
  - "A re-grant under the same lease (Retry) picks a fresh selector-matching credit (same worker first); no credit -> Abandoned"
  - "ExecutorHooks gains one field (`remote: RemoteHooks`) for the B3 sink assertion and the message tap"
requirements-completed: [WORK-01, WORK-02, WORK-04, WORK-06]
metrics:
  duration: "~2h30m"
  completed: 2026-11-21
---

# Phase 7 Plan 12: Remote execution (NodePlacement, PlacementExecutor, RemoteRunner) Summary

**Queues now execute where their policy says: `NodePlacement` reserves an embedded slot or a selector-matching worker credit (auto: embedded first, overflow to workers while spilling), and `PlacementExecutor` runs worker reservations through the leader-authoritative `RemoteRunner`. The runner builds the run's sink with the embedded construction (`sink_for`, bridge included), journals `RunStarted` on the server, decodes every worker event server-side and appends it through `PartitionSink`, verifies `StartEffect` against the journal before the Phase 6 driver runs it, re-mints await tokens on the leader, and maps `Done` through the embedded outcome mapper. Remote journals equal embedded ones, every bulk message stays within a 64 KiB frame, and a canary secret never crosses the worker channel.**

## Commits

1. `3f63d69` feat(07-12): label selectors and NodePlacement
2. `e91830a` feat(07-12): leader-authoritative RemoteRunner and PlacementExecutor

## executor.rs (shared file; refactor + visibility, no embedded behaviour change)

```rust
// EngineExecutor
pub(crate) fn sink_for(&self, job: &Job, inputs: Option<mpsc::UnboundedSender<Event>>,
                       on_renew: Option<RenewFn>) -> PartitionSink;      // resolvers + guest logs + bridge
pub(crate) fn module_sha(&self, run: &RunId, record: &RunRecord) -> Option<[u8; 32]>;
pub(crate) async fn prelude(&self, handle, run, token, record, sink, rref, cancel, sha)
    -> Result<Vec<Event>, ExecOutcome>;                                   // RunStarted, B2 re-arm, journal + repair
pub(crate) async fn map_outcome(&self, sink, outcome, engine_end, failures, record) -> ExecOutcome;
pub(crate) async fn fail_run(&self, sink, reason_code, kind, message, resumable) -> ExecOutcome;
// ExecutorHooks
pub remote: workers::remote_executor::RemoteHooks,   // sinks built (kind, bridge addr), tap, remints, activations
```

- `run_job` builds its sink through `sink_for` (the old inline construction moved there verbatim), then destructures the job as before.
- `drive`'s steps 2 (RunStarted), 2b (re-arm) and 3 (journal + `repair_unsupported`) moved verbatim into `prelude`; `(None, x)` returns became `Err(x)`.
- `map_outcome` / `fail_run`: visibility only. `sink_for(job)` takes the input channel and renew callback in addition to the plan's `job`, because both differ per path.
- No `ActivationSink` method was added (MeteredSink/EngineSink forwarding unchanged).

## Placement (workers/placement.rs, workers/selector.rs)

| placement | reservation |
|---|---|
| `embedded` | embedded slot (`cfg.slots`, the `[executor] slots` semaphore), else Blocked |
| `workers` | `registry.pick(selector matches labels)`, else Blocked |
| `auto`, not spilling | embedded slot, else Blocked |
| `auto`, spilling | embedded slot if free, else worker credit, else Blocked; every overflow attempt calls `SpillHandle::note_overflow(queue, clock.now_ms())` |

- Placement and selector come from `QueuePolicy` (default `[workers] default_placement`), compiled once per queue and meta applied index. An unparsable stored selector matches no worker.
- `changed()`: `EmbeddedSlot` drop (slot returned, then bump), a forwarder task merging `WorkerRegistry::changed()` and `SpillHandle::changed()` (stops on `P7Ctx::shutdown`), and `NodePlacement::notify()`.
- Metrics: `ostro_placement_reservations_total{queue,kind}` (`embedded` | `worker`), `ostro_placement_blocked_total{queue}`.
- `install` always sets `cfg.placement` (also with `[workers] enabled = false`: embedded queues behave as before).

## RemoteRunner (workers/remote_executor.rs)

Per job: credit (reservation's `WorkerSlot`, or a fresh pick on a re-grant) → `sink_for` → module present check (`no_module` / `module_not_found` like embedded) → `prelude` → `attach_job` → `Activate{run, token, module_sha, journal_len, workflow, workflow_version, traceparent, hot_wait_ms}` + info `remote activate run=<id> attempt=<n> worker=<id>` → event loop:

- **Append:** decode every `(kind, bytes)` with `decode_event(from_seq + i, ..)` (decode error → `rejected`), D-W27 re-mint (below), `PartitionSink::append` → `next` / `fenced` / `conflict_expected` / `rejected(<Unavailable reason>)`; debug `applied events run=<id> attempt=<n> from_seq=<s> count=<k>`. After a revoke: `fenced`.
- **StartEffect verification as built:** (1) request token == job token and lease not revoked, else `FAILED_PRECONDITION`; (2) `journal_io::read_event(op)` is `EffectScheduled` and nothing in `[op+1, end)` completes it, else `FAILED_PRECONDITION`; (3) the names of `redacted_values` equal, in order, the journal headers named in `redact` (ASCII case-insensitive), else `rejected`; for SQL/object effects `redacted_values` must be empty; (3b) a value with `{{` must parse with `ostro_bridge::secrets::template::parse`, contain ≥ 1 placeholder and no other `{{`, else `rejected: template in redacted value`; (4) `LiveEffect{op, request: journal copy with redacted values substituted positionally, http_retry, scheduled_ts_nanos: event.ts_nanos, redrive}` → `sink.effect_driver().start(rref, token.n, live)` (no bridge → `rejected`).
- **Inputs:** dispatcher `InputAppended(s)` and sink completions → `Input{up_to_seq: s + 1}` (monotonic).
- **GuestLogs:** converted to `GuestLogRecord` and passed to `sink.guest_log` → the **08-17 GuestLogSink** (same path as embedded lines; tracing fallback only if a sink had no guest-log sink).
- **CancelEffect:** forwarded to the run's effect driver `cancel`.
- **Done:** `WireOutcome::decode → into_engine` (undecodable → `AttemptFailed(ProtocolViolation)`), then `map_outcome` at the journal end the leader tracked from the worker's accepted appends; `Reactivate` → `Abandoned`.
- **Revoke:** `Cancel{run, token}`, then wait for `Done` at most `min(lease_ttl, 10 s)` → `Released`.
- **Abandoned / stream loss:** `ExecOutcome::Abandoned` (no failure counted).
- Every job is recorded in `ExecutorHooks::jobs` like embedded ones.

### D-W27 (await tokens)

On `Append`, the first `AwaitTokenCreated` is compared with `token::mint(partition, run_id, seq, meta token_key)`. If it differs, the batch is cut after it, the leader token replaces the worker's, the prefix is appended, and the reply is `conflict_expected(next)`: the worker's engine evicts (`AppendConflict`), the job maps to `Abandoned`, and the re-grant replays the journaled leader token. Events after the token in that batch (e.g. a `StatusPublished` carrying the worker token) are never applied. The key never leaves the leader. Test: `await_tokens_are_minted_on_the_leader` (the journaled token == the published token, completes through `/v1/tokens/complete`, remints ≥ 1).

## Journal equality method

`p7_workers::journal_shape`: per event `seq`, kind, the command hash of command events, and for `StepCompleted` (name + recorded outcome), `RunCompleted` and `RunFailed` the full body. Times, run ids, seeds, fuel and await-token strings are execution-specific and excluded. Result payloads are compared byte for byte too.

- Exact equality: `long-journal` (40 steps + 4 KiB step, and the 2,000-step paging run) and `retry-probe` (`exhaust`, `recover:2`, `trap`).
- `sdk-basic`: equal after dropping `TimerFired` / `WaitResolved` and seqs (`timing_free_shape` in worker_remote.rs). How many `WaitResolved` a `join!` over two sleeps journals depends on when the timers fire relative to the wait; it differed between the embedded and the remote run in one of three runs. That is input timing, which differs between any two executions, embedded ones included.

## Frame overhead measured

`worker_paging`: `max_frame_bytes = 64 KiB`; 2,000-step `long-journal` with a 3 MiB step result and a 3 MiB signal input; 4,307 recorded bulk messages (both directions, on a second recording listener over the server's registry/join), max append chunk 65,280 B, max journal page / blob chunk 65,280 B. **Overhead constant = 256 B (`FRAME_RESERVE`, both sides)**: payload ≤ 64 KiB − 256 B, so the encoded message ≤ 64 KiB. Journal identical to embedded.

## Verification

- Task 1: `cargo test -p ostro-server --lib -- workers::selector workers::placement`: 14 passed; clippy `-D warnings` clean.
- Task 2: `worker_remote` 7 passed (`remote_run_journal_equals_embedded`, `retries_behave_identically` [exhaust, recover:2, trap], `worker_loss_regrants_without_failure`, `start_effect_is_verified_on_the_leader`, `secret_never_on_stream`, `selector_routes_gpu_queue_to_the_labelled_worker`, `await_tokens_are_minted_on_the_leader`); `worker_paging` 1 passed; lib unit tests of `remote_executor` (rebuild/templating) pass.
- `worker_remote` + `worker_paging` green twice in a row on main `4945749` (07-13/07-14/apply-panic fix landed), plus `executor` 8, `leases` 4, `flow_control` 10, `p7_control` 10, `placement_spill` 20.
- Full `cargo test -p ostro-server --locked --no-fail-fast` on main `4945749` + this plan: exit 0, 50 `test result: ok`, 0 failed.
- `cargo clippy -p ostro-server --all-targets --locked -- -D warnings` clean, also with `--features failpoints`; `cargo fmt -p ostro-server --check` clean; build warning-free.
- Not run: `bash scripts/ci-local.sh --no-musl` (shared-load policy, as 07-05/07-08/07-09/07-11; several Phase 7 executors, the debugger and the Phase 8 campaign were running). 07-22 runs the full gate.

## Deviations from Plan

1. **[Plan defect] W6 vs the canary must-have.** The plan says any `{{` in a redacted value is rejected, and also that a canary in a *templated, redacted* header must reach the provider from a worker run. The worker sends redacted values as written (`{{secret:..}}` stays a template, 07-07), so literal W6 makes every templated, redacted header fail on workers. Built: only well-formed placeholders are accepted (bridge grammar, reserved names refused), everything else with `{{` is rejected. The bridge still enforces grants and egress; URL, method, body and the other headers come from the journal. **Security-relevant: flag for user review with D-W3.**
2. **[Bug, 07-07 guest] `pay-flow` cannot send its effect anywhere.** Its header is `Bearer {{secret:PAY_TOKEN}}`; the FX-06 grammar allows lowercase names only, so the bridge refuses the template (embedded too). The canary test uses `fx-canary` with `redact: true` (template `{{secret:canary-key}}` in a redacted `X-Api-Key`). Follow-up for 07-17/07-21: rename the secret in `guests/pay-flow` (e.g. `pay_token`).
3. **[Plan adaptation] Message recorder.** ostro-server cannot name prost, and the 07-09 `WireStats` records sizes only. Sizes: a second cluster listener with `WireStats` over the server's own registry/join (`p7_workers::wire_listener`). Bytes (canary scan): `RemoteHooks::tap` records the content (every string/bytes field) of every message the runner sends or receives; journal pages and blob chunks are journal rows, so the test scans the run's journal rows too.
4. **[Rule 3] `WorkerProc::kill` does not drop running activations** (they are tokio tasks on the test runtime holding the session link, so the server never sees the stream end). Added `ServerWorkers::spawn_isolated` → `IsolatedWorker` on its own runtime; `kill()` = `shutdown_background()`.
5. **`sink_for` signature** takes `inputs` and `on_renew` besides the job (see executor.rs).
6. **Remote `Reactivate` → `Abandoned`** (see key decisions).
7. **`ExecutorHooks.remote`**: one test-hook field added to executor.rs beyond visibility changes.
8. **execute.proto unchanged** (no field needed; no server→worker Drain added).

## Hand-offs

- **07-13 (spill):** `NodePlacement` reads `SpillHandle::spilling(queue)` per reservation, calls `note_overflow(queue, now_ms)` on every overflow attempt (spilling + embedded full + worker wanted, found or not) and merges `SpillHandle::changed()` into `Placement::changed()`. Nothing else is needed from spill.rs.
- **07-16 (demand):** `running_on_workers` = `QueueGauges` (07-05 counts leases by `ReservationKind::Worker`, which `NodePlacement` sets for every worker reservation). Worker credit: `registry.slots_free()`. Placement per queue: `workers::selector::QueuePlacement::from_policy`.
- **07-21 (chaos/failpoints):** no new failpoints were added in 07-12. Natural sites if wanted: `placement::reserve` (before deciding), `worker::remote_before_activate`, `worker::remote_after_append` (after the sink accepted, before the reply), `worker::remote_before_start_effect`. The runner logs `remote activate run=<id> attempt=<n> worker=<id>` (info) and `applied events run=<id> attempt=<n> from_seq=<s> count=<k>` (debug).
- **07-17:** `ServerWorkers` (spawn / spawn_isolated / raw), `wire_listener`, `journal_shape` and `RemoteHooks::tap` are reusable; fix `pay-flow`'s secret name first (deviation 2).
- **07-14:** the server REST join route can replace the test loopback join endpoint (`p7_workers::join_endpoint`).

## Issues

- `leases::heartbeat_keeps_long_activation_leased` (400 ms lease, TestNode, no Phase 7 code on its path; executor.rs change is the verbatim `prelude` move) failed once in a batch at load ~25 and passed 3/3 alone right after.

- `flow_control::queue_concurrency_caps_leased_activations` failed 2 of 12 full-binary runs at load 35-60 with `sampled dispatcher leases 3 > 2` (flow counters max = 2 every time), 0 of 6 with placement disabled and 0 of 8 in a later batch with it enabled. The assertion samples `DispatcherStats.leases`, which each of the 4 partition dispatchers updates at the end of its own loop turn, so one partition's grant can be counted before another's release. Not changed (07-08's test file); recorded for 07-22.

## Self-Check: PASSED

- FOUND: crates/ostro-server/src/workers/{selector,placement,remote_executor}.rs, crates/ostro-server/tests/{worker_remote,worker_paging}.rs, tests/common/p7_workers.rs additions
- FOUND: commits 3f63d69, e91830a on main
