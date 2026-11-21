# Flow control: concurrency, fairness, throttles, rate limits and pause

Phase 7 (DISP-03, DISP-04, DISP-05, OPS-01) gives every queue limits that the partition
dispatcher enforces when it grants activations, a rate limit on run starts, keyed fairness, and
pause/resume for runs, workflows and queues (including auto-pause when retries run out).

Implementation: `crates/ostro-partition/src/leader/{fair,flow,dispatcher}.rs` (selection and
the dispatcher contract), `crates/ostro-server/src/flow/` (`NodeFlow`: counters, GCRA,
policy view, rate limit), `crates/ostro-partition/src/apply/pause.rs` (`Cmd::PauseRun` 20 /
`Cmd::ResumeRun` 21, `RejectReason::RunPaused` 15) and `crates/ostro-server/src/control/p7_*.rs`
(API). End-to-end proof: `crates/ostro/tests/flow_e2e.rs` (spawned server) and
`crates/ostro-server/tests/{flow_control,auto_pause,p7_control}.rs`.

---

## 1. Configuring a queue

```bash
ostro queue flow heavy                                   # show (TOML subset)
ostro queue flow heavy --set concurrency=4 --set weight=3 \
  --set key_concurrency=1 \
  --set throttle.limit=5 --set throttle.period_ms=1000 --set throttle.burst=2 \
  --set rate_limit.limit=100 --set rate_limit.period_ms=1000 \
  --set on_retries_exhausted=pause
ostro queue flow heavy --set concurrency=null            # remove a field (server default)
```

REST: `GET/PUT /v1/queues/{q}/flow` (`expected_version` or `If-Match`; a stale version is 412).
The fields live in `QueuePolicy` (meta), next to the Phase 5 retention/schedule-to-start fields,
which a flow PUT preserves.

| Field | Meaning |
|-------|---------|
| `concurrency` | max activations of the queue running at once (leased) |
| `key_concurrency` | max activations per `concurrency_key` |
| `weight` | fair-share weight of the queue (default `[flow] default_queue_weight` = 1) |
| `throttle` / `key_throttle` | `{limit, period_ms, burst}`: first activations started per period |
| `rate_limit` / `key_rate_limit` | `{limit, period_ms, burst}`: run starts accepted per period |
| `on_retries_exhausted` | `fail` (default) or `pause` (auto-pause, §6) |
| `placement`, `selector`, `spill` | where activations run (docs/workers.md §3-4) |
| `paused` | set by `ostro queue pause|resume` |

## 2. The concurrency unit (D-W13)

- Concurrency counts **leased activations**: runs executing now, on an embedded slot or a
  worker. A suspended or sleeping run holds no lease and counts nothing.
- Counters are **node-wide**, shared by every partition dispatcher of the node. In a cluster
  each node enforces its own limit until Phase 13 adds cluster-wide quotas.
- A flow charge is tentative until placement grants capacity: `FlowControl::admit` returns a
  `FlowPermit` that the dispatcher commits only after `Placement::reserve` is `Granted`; an
  uncommitted permit rolls back (counters and throttle state) when dropped.
- Releases (activation end, lease revoke, pause) free the counters before the revoked job is
  signalled, so a woken zombie never holds capacity.

## 3. Fairness: weights, priority bands, keys (D-W10)

Each dispatcher selects the next ready entry with three levels (`leader/fair.rs`):

1. **Queues: weighted deficit round robin.** The queue whose turn it is serves up to `weight`
   entries before the turn passes on. With every queue backlogged, shares are proportional to
   the weights. A blocked or skipped queue loses only its turn; probing it costs nothing.
2. **Strict priority bands** within a queue: the run's `priority` (start request field), highest
   first.
3. **Round robin across concurrency keys** within a band (quantum 1), FIFO per key by
   `(ready_at, run)`. A hot key gets one pick per round, so it cannot starve the others. Unkeyed
   work is one key, so a queue without keys behaves exactly like the Phase 4 FIFO.

A key at its `key_concurrency` leaves the rotation until a release; the other keys keep
being served. A throttle (`throttle` or `key_throttle`) holds back only **first activations**
of its queue (or key) until its `until_ms` or the next flow-control change; re-activations of
the same queue and key keep being granted in their usual fair order, even when they sit behind
a throttled first activation (gap G1, fixed in 07-23). Weights and bands are per partition dispatcher,
so cross-partition ordering is not strict (the e2e weight and priority legs use one partition).

## 4. Throttle versus rate limit (D-W21, D-W24)

Both use a hand-rolled GCRA (`flow/limits.rs`) with time from the injected clock:
`T = period_ms / limit`, `tau = T x (burst - 1)` (`limit`, `burst` at least 1); a request
conforms iff `now >= TAT - tau`, and then `TAT = max(TAT, now) + T`.

| | Throttle (`throttle`, `key_throttle`) | Rate limit (`rate_limit`, `key_rate_limit`) |
|---|---|---|
| Where | dispatcher, at grant time | StartRun admission, before the proposal |
| Applies to | **first activations** only (a run's first grant, F7 `first_activation`); re-activations after sleeps, signals or effects are never throttled | run starts (REST, gRPC, CLI, and Phase 8 webhook and cron starts, which use the same path) |
| Excess | the first activation waits in the ready queue until `until_ms`; re-activations queued behind it are still granted | rejected: HTTP 429 / gRPC `RESOURCE_EXHAUSTED`, `code = "rate_limited"`, `Retry-After` (ceil seconds) and `details.retry_after_ms`; nothing is enqueued |
| Idempotent re-submits | - | a start whose idempotency key already has a dedupe row is not charged |

The CLI prints `ostro: rate limited: retry after <s> s (Retry-After), <ms> ms` for any verb
that gets a 429. Memory is bounded: key cells are capped by `[flow] max_tracked_keys` (100,000)
and idle cells are evicted after `[flow] idle_key_evict` (10 min); evicting an idle key is lossy
(it may start early), cells with running activations are never evicted.

## 5. Pause and resume

| Scope | Command | Effect |
|-------|---------|--------|
| Run | `ostro run pause <id> [--detail T]`, `ostro run resume <id>`; `POST /v1/runs/{id}/pause|resume` | `Cmd::PauseRun` writes `RunRecord.pause = {reason, at_ms, detail}`; the run gets no ready row and no grant; a running activation's lease is revoked (its uncommitted work is discarded and re-executed after resume) |
| Workflow | `ostro workflow pause|resume <name>` | `WorkflowPolicy.paused`: no new activations of that workflow on any queue; other workflows on the same queue keep running (held entries, fix-0705) |
| Queue | `ostro queue pause|resume <q>` | `QueuePolicy.paused`: no new activations of the queue |

- Pause and resume are idempotent (`changed: false` the second time). A terminal run answers
  409 `run_terminal`.
- `GET /v1/runs/{id}` and `ostro inspect <id>` show the pause mark while paused:
  `"pause": {"reason": "operator" | "retries_exhausted", "detail": "...", "at_ms": ...}`
  (`null` otherwise; gRPC `RunInfo.pause`).
- While paused, inputs are still journaled: timers fire, effects complete, signals and tokens
  match their waits. Nothing is activated until resume, which re-queues the run with
  `first_activation = false` (no throttle charge for a started run).
- **Cancel** while paused is journaled and takes effect after resume (the guest sees the
  cancellation on its next activation). **Terminate** and the **run timeout** are immediate:
  the run becomes terminal and its pause mark is cleared. A run that timed out while paused can
  be resumed with `/retry` (D1) and completes.
- `POST /v1/runs/{id}/retry` on a **paused** run is an alias for resume (the response says
  `resumed`); on a terminal run it is the Phase 5 retry.

## 6. Auto-pause on exhausted retries (OPS-01)

With `on_retries_exhausted = "pause"` on the queue (or the workflow), a run whose retryable
attempt failures exhaust its retry policy is paused instead of failed:
`PauseRun{reason: retries_exhausted, detail: <failure JSON>, token}`. The token fences it like
an append, so a stale executor cannot pause a newer attempt. An operator fixes the cause (deploys
a new version, repairs a dependency) and resumes the run (`resume` or `/retry`).

Review item (recorded in 07-VALIDATION): a resume keeps the run's `attempt_failures`, so a
single further failure exhausts the budget again and re-pauses the run. A D1 retry of a
terminal run resets the budget (F9). Resetting on resume would change apply semantics (a
partition format decision), so it is left for user review.

## 7. Observability

- `ostro_flow_rebuild_entries{partition}` and `ostro_flow_rebuild_seconds{partition}` (dispatcher
  rebuild), the Phase 8 queue gauges (`ostro_queue_*`), and the demand series of
  docs/workers.md §5.
- `GET /v1/queues/{q}/demand` shows `backlog` (ready and admissible) and the running split.
