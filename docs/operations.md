# Run operations

This guide covers the Phase 11 run operations: rerun, the dead-letter queue (DLQ),
`on_failure` hooks, bulk jobs, tag search and the run limits. Everything here is
available over REST (`/v1`), gRPC (`ostro.api.v1.RunOps`, `ostro.api.v1.Inventory`), the
`ostro` CLI and the embedded UI ([ui.md](ui.md)). The published limits are in
[limits.md](limits.md).

Every refusal is an API error with a stable `details.reason` (see
[Error reasons](#error-reasons)). The CLI prints it on stderr and exits 1; `--json` prints
the REST error body on stdout.

## Rerun

A rerun starts a **new run** from a chosen point of a terminal run's journal. The events
before the cut are copied into the new run as they are, and the rest executes again on the
target module. The typical use is fix-and-rerun: a run failed at step `s3`, you deploy a
fixed module, and you rerun from `s3`. Steps `s1` and `s2` are not executed again; their
results come from the copied journal.

```sh
ostro rerun <run> --from-step s3                 # the first execution of step s3
ostro rerun <run> --from-step s3#2               # its second execution
ostro rerun <run> --from-seq 4                   # an explicit journal seq
ostro rerun <run> --from-step s3 --version source  # on the source's exact module
ostro rerun <run> --from-step s3 --dry-run       # verdict only, no run
ostro rerun <run> --from-step s3 --idempotency-key fix-1234
```

REST: `POST /v1/runs/{id}/rerun` with
`{"from": {"step": {"name": "s3", "occurrence": 1}} | {"seq": 4}, "version": "active" | "source" | N, "idempotency_key": "...", "dry_run": false}`.
`GET /v1/runs/{id}/rerun-cuts` lists every op of the journal with whether a cut there is
safe (the UI's "rerun from here" uses it).

### Semantics

- **The source must be terminal.** A running run is 409 `run_not_terminal`: terminate it
  first.
- **Target version.** `active` (the default; 412 when the workflow has no active version),
  `source` (the source's version, pinned to its exact module) or a version number.
- **Mandatory replay check.** Before anything is created, the copied prefix is replayed
  against the target module as the new run. A module that makes different decisions inside
  the prefix is refused with 409 `rerun_nondeterministic` (`details.seq` names the first
  diverging event). Fix the module so that it reproduces the prefix (for example with a
  `patched()` marker) or cut earlier. `--dry-run` runs exactly this check and reports the
  verdict (`ok` or `ok_frontier`) without creating a run.
- **Effects after the cut re-execute.** Effects inside the prefix are never re-run: their
  completions are copied. Effects after the cut run again, for real, **under the new run
  id**.
- **Rerun-safe workflows.** Exactly-once effect keys are derived from the run and the
  effect arguments. If you put `run-info.run_id` into hashed effect arguments, a rerun
  produces a different key and the external system sees a new request. Use business keys
  (an order id from the input) in effect arguments instead, so a rerun of the same order
  is recognised by the downstream system.
- **Open timers and signal waits are re-armed.** A cut may leave a durable timer or a signal
  wait open; the new run re-arms them before its first activation, even across a crash
  (`rerun::before_rearm` kill test).
- **Lineage.** The new run carries the source's user tags plus the system tag
  `__ostro/rerun_from=<cut>`, links to the source (`retry_of`), and is **detached**: a
  rerun of a child run has no parent.
- **Idempotency.** With `idempotency_key` the rerun is deduplicated per source
  (`__rerun/<source>/<key>`): repeating the request returns the same run with
  `existing: true`.
- **Envelope.** The copied prefix travels in one start command. Large copied payloads are
  moved to the blob store automatically; a prefix that still exceeds 3.5 MiB
  (`MAX_RERUN_PREFIX_BYTES`) is 413 `rerun_prefix_too_large`. A prefix longer than
  `[limits] max_history_events` is the same 413 with `details.limit = history_events`.

### Cut rules

A cut `c` copies events `0..c`. Valid cuts are `1` up to the index of the terminal event.
A cut is **unsafe** (409 `rerun_cut_unsafe`, with `details.detail` and
`details.nearest_safe_seq` when one exists) when the prefix contains:

| `detail` | Why |
|---|---|
| `open_child` | a child run started but not finished: it is bound to the old parent |
| `open_effect` | an effect scheduled but not completed: the bridge is bound to the old run |
| `open_await_token` | an await token created but not completed: the token embeds the old run id |
| `unmatched_signal` | a signal payload journaled without its match |
| `terminal_or_out_of_range` | the cut is 0, past the terminal event, or past the journal end |

## Dead-letter queue

The DLQ is a **view**, not a run status. A run is a DLQ member when it is `failed` or
`timed_out`, its queue's DLQ policy is enabled (the default), and it has not been dismissed
or requeued **in its current failure episode**. A run that is resumed and fails again is a
new episode and is listed again.

```sh
ostro dlq ls [--workflow W] [--queue Q] [--tag T]... [--reason PREFIX] [--all]
ostro dlq requeue <run> [--mode auto|resume|restart|rerun] [--from-step s3] [--dry-run]
ostro dlq dismiss <run> --reason "known flake"
ostro ls --dlq
```

REST: `GET /v1/dlq` (newest first, bounded pages; `include_dismissed=true` lists dismissed
and requeued entries with their marks), `POST /v1/dlq/{run}/requeue`,
`POST /v1/dlq/{run}/dismiss`.

**Reason.** Each entry has a `reason`: the terminal reason when set (`run_timeout`,
`schedule_to_start`, an operator's terminate reason), else `"<reason_code>: <message>"`
from the failure, for example `workflow_failed: s3 fails`,
`nondeterminism: seq=3 mismatch` or `limit_exceeded: history_events 21 > 20`. The
`--reason` filter is a prefix match on it.

**Requeue modes.**

- `auto` (default): `resume` when the run is resumable (an executor-side failure: a limit,
  nondeterminism after a fix), else `restart`. A guest failure (`RunFailed` from the
  workflow) is not resumable: replaying it would fail the same way.
- `resume`: the run keeps its id and resumes from its journal (the Phase 5 retry).
- `restart`: a new linked run with the same workflow, version, module, input, queue and
  tags, deduplicated per episode (`__dlq/<run>/<n>`), so a repeated requeue returns the
  same new run.
- `rerun`: a rerun from `--from-step`/`--from-seq` (all the rules above apply).

**Dismiss** removes an entry from the default view with a reason; dismissing a run that is
not a member is 409 `dlq_not_member`.

**Auto-pause interplay.** Phase 7 flow control can pause a queue after repeated failures.
DLQ membership does not depend on it: failed runs are listed whether or not their queue is
paused. A requeued (resumed or restarted) run on a paused queue waits until the queue is
resumed, like any other run.

**Policy.** `GET|PUT /v1/queues/{name}/failure-policy` carries
`{"dlq": {"enabled": bool, "hook_catchup_ms": n}}`; with `enabled = false` the queue's
failed runs are never DLQ members and never fire hooks.

## on_failure hooks

A workflow or queue may name a hook workflow that runs once for each failure:

```sh
curl -X PUT http://127.0.0.1:7700/v1/workflows/pipeline/failure-policy \
  -H 'content-type: application/json' \
  -d '{"on_failure": {"workflow": "on_fail", "include_input": true}}'
```

- **Resolution.** The workflow's `on_failure` wins over the queue's
  (`/v1/queues/{name}/failure-policy`); the queue's DLQ policy must be enabled.
- **Hook input.** A JSON object: `run_id`, `workflow`, `version`, `status` (`failed` or
  `timed_out`), `reason` (as in the DLQ), `failed_at_ms`, `tags` (user tags) and, with
  `include_input`, the failed run's `input`.
- **Exactly once per failure episode.** The hook run is started with the dedupe key
  `__onfail/<run>/<resumed_count>` and the tag `__ostro/on_failure_of=<run>`; a crash
  between starting the hook and recording it re-drives to the same hook run
  (`dlq::hook_after_start` kill test). A run that is resumed and fails again gets a second
  hook.
- **Loop guard.** A hook run never fires a hook itself, so a failing hook (or a hook that
  names itself) stops there.
- **Catch-up.** Failures applied while the node was down or while hooks were disabled are
  picked up at start for failures inside the catch-up window (`[dlq] hook_catchup`, default
  24 h, overridable per queue with `hook_catchup_ms`). `[dlq] hooks_enabled = false`
  disables hooks entirely.
- Hook starts go through the normal start rate limits; a 429 is retried by the next
  catch-up.
- Today hooks run on every started partition slot, which is the partition leader (single
  node). With replication (Phase 12/13) they run on leader slots only.

## Bulk operations

A bulk job applies one action to every run matching a filter, paced and resumable:

```sh
ostro bulk cancel    --tag batch --dry-run         # estimate + up to 20 sample runs
ostro bulk retry     --tag batch --rate 20
ostro bulk rerun     --tag batch --from-step s3 [--version active|source|N]
ostro bulk requeue   --dlq [--mode auto|resume|restart|rerun]
ostro bulk terminate --status running --workflow w --reason cleanup
ostro bulk pause | resume | signal --name go --payload '{}'
ostro bulk ls
ostro bulk status <blk_...> [--watch]
ostro bulk cancel <blk_...>                        # cancel the job itself
```

REST: `POST /v1/bulk` (`{"action": ..., "filter": {"workflow", "queue", "status", "tags",
"dlq"}, "rate_per_sec": n, "max_in_flight": n, "dry_run": bool}`), `GET /v1/bulk`,
`GET /v1/bulk/{job}`, `POST /v1/bulk/{job}/cancel`.

- **Actions.** `cancel`, `terminate`, `retry` (resume when resumable, else restart),
  `rerun`, `requeue`, `pause`, `resume`, `signal`.
- **Snapshot bound.** A job selects only runs created before the job; runs the job itself
  creates (restarts, reruns, which inherit the tags) are never selected. A rerun job over
  a tag therefore creates exactly one rerun per matched run.
- **Rate and backpressure.** Ops are paced by a GCRA limiter at `rate_per_sec`
  (`[bulk] default_rate` 20, at most `[bulk] max_rate` 500) with a burst of
  `[bulk] burst` (10), and at most `max_in_flight` (4) at once. While a target
  partition's ready backlog is above `[bulk] backlog_high` the job is
  `paused_backpressure`. A 429 from a start waits `retry_after_ms` and retries.
- **Caps.** At most `[bulk] max_jobs` (4) unfinished jobs per node (429
  `bulk_too_many_jobs` with `retry_after_ms`), at most `[bulk] max_matches` (100,000) runs
  per job (400 `bulk_too_many_matches`).
- **Per-run idempotency.** Each run is processed at most once per job: cancel, terminate,
  pause and resume are natural (a run no longer eligible counts as `skipped`), signals use
  `request_id = <job>/<run>`, restarts and reruns use the dedupe key `__bulk/<job>/<run>`.
- **Checkpoints.** The job file `<data>/bulk/<job>.json` is written at creation, on state
  changes and every `[bulk] checkpoint_every` (1 s) or `checkpoint_ops` (500) ops.
  Unfinished jobs resume from their checkpoint after a restart; ops between the last
  checkpoint and a crash are re-delivered and recognised (`bulk::after_op` kill test: no
  duplicate cancel).
- **Node-local.** Jobs and their files belong to the node that accepted them. They are not
  part of snapshots or backups, and in a cluster a job only reaches the runs that node can
  list; cluster-wide forwarding of `/v1/bulk` is Phase 13.

## Search

Runs carry user tags (`--tag k=v` at start; at most 64 per run, 128 bytes each; the
`__ostro/` prefix is reserved for system tags).

```sh
ostro ls --tag team=a --tag env=prod --newest     # AND of the tags, newest first
ostro ls --dlq                                    # DLQ members
GET /v1/runs?tags=team%3Da&tags=env%3Dprod&order=newest&limit=50
GET /v1/runs?dlq=true
```

- Tags combine with AND; at most 8 tags per query (400 `details.limit = search_tags`).
- `order=newest` uses the run index (a derived, rebuildable keyspace); pages are bounded by
  `limit` (at most `MAX_PAGE_LIMIT`) and by a per-call scan budget, so a page can be shorter
  than `limit` (even empty) while still carrying a `next_page_token`. Always follow the
  token.
- The default order (no `order`) is unchanged: oldest first through the partition scan.
- **Index rebuild fallback.** After an upgrade or a lost index the node rebuilds the index
  in the background. Meanwhile a search without a page token falls back to the scan: results
  come **oldest first** and the page tokens start with `c1.`. A request that carries an
  index token (`i1.`) from before is answered 503 `index_rebuilding`; restart the listing
  without the token. There is no response header for the fallback yet (an
  `X-Ostro-Order-Fallback` header needs an API field; tracked as a follow-up): clients tell
  the fallback by the `c1.` tokens.

## Limits

The engine and API limits, their defaults, configuration keys and errors are documented in
[limits.md](limits.md) and served at `GET /v1/limits` (`ostro limits`). The per-run
limits are set in `[limits]`:

```toml
[limits]
max_history_events = 50000
max_children_per_run = 10000
max_payload_bytes = "4MiB"     # must not exceed [api] max_request_bytes
```

- A payload over `max_payload_bytes` at the API (start input, signal, token completion) is
  413 with `details = {"reason": "limit_exceeded", "limit": "payload_bytes", "max", "actual"}`;
  nothing is created.
- A run that would exceed `max_history_events` or `max_children_per_run`, or produce a step
  output or result over `max_payload_bytes`, fails **resumable** with the message
  `limit_exceeded: <limit> <actual> > <max>` (for example
  `limit_exceeded: history_events 21 > 20`). Raise the limit and resume it with
  `ostro retry <run>` or `ostro dlq requeue <run> --mode resume`; it continues from its
  journal. Limits are enforced only on new live commands, never during replay.

## Error reasons

| `details.reason` | Status | Where |
|---|---|---|
| `run_not_terminal` | 409 | rerun / retry of a run that is still live |
| `rerun_cut_unsafe` | 409 | rerun cut rules (`detail`, `seq`, `nearest_safe_seq`) |
| `rerun_nondeterministic` | 409 | the replay check diverged (`seq`, `detail`) |
| `rerun_prefix_too_large` | 413 | prefix over 3.5 MiB or over `max_history_events` (`limit`, `max`, `actual`) |
| `dlq_not_member` | 409 | dismiss / requeue of a run that is not in the DLQ |
| `bulk_too_many_jobs` | 429 | `[bulk] max_jobs` unfinished jobs (`retry_after_ms`) |
| `bulk_too_many_matches` | 400 | the filter matches more than `[bulk] max_matches` runs |
| `limit_exceeded` | 400 / 413 | payload, tag and search caps (`limit`, `max`, `actual`) |
| `index_rebuilding` | 503 | an `i1.` page token while the run index rebuilds |

## Examples

```sh
# fix-and-rerun
ostro dlq ls --workflow pipeline
ostro deploy pipeline-v2.wasm --activate
ostro rerun <run> --from-step s3 --dry-run
ostro rerun <run> --from-step s3 --idempotency-key fix-1234

# drain a batch of failures after a fix, 20 runs per second
ostro bulk rerun --dlq --workflow pipeline --from-step s3 --rate 20
ostro bulk status <blk_...> --watch

# raise a limit and resume the runs that hit it
ostro dlq ls --reason limit_exceeded
ostro bulk requeue --dlq --mode resume
```
