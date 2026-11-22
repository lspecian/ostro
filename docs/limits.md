# Limits

Every limit Ostro enforces, with its default and how a breach behaves (OPS-07, D-P11-13).
The run limits in the first three rows are new in Phase 11. The other rows republish limits
that already existed, with their values unchanged.

`GET /v1/limits` serves the same table (11-11), and a doc-sync test keeps this file, the
endpoint and the code constants equal. The format is part of that contract. There is one
row per limit. Column 1 is the name in backticks: a `[section] key` config name or a code
constant. Column 2 is an integer default, and its unit is in column 3.

| Name | Default | Unit | Configurable | Where enforced | Error | Replay behaviour |
|---|---|---|---|---|---|---|
| `max_history_events` | 50000 | events per run | `[limits] max_history_events` | engine, live appends only | run `Failed` (resumable), reason `limit_exceeded: history_events <actual> > <max>` | never enforced; terminal and failure records exempt |
| `max_children_per_run` | 10000 | `ChildStarted` per run | `[limits] max_children_per_run` | engine, live appends only | run `Failed` (resumable), reason `limit_exceeded: children_per_run <actual> > <max>` | never enforced |
| `max_payload_bytes` | 4194304 | bytes | `[limits] max_payload_bytes` (must be <= `[api] max_request_bytes`) | engine, live appends only (step output, run result, child input, continue-as-new input); API (11-11) | engine: run `Failed` (resumable), reason `limit_exceeded: payload_bytes <actual> > <max>`; API: 413 `payload_too_large` / gRPC `RESOURCE_EXHAUSTED`, `details{limit, max, actual}` | never enforced |
| `max_request_bytes` | 4194304 | bytes | `[api] max_request_bytes` | API (REST body, gRPC message) | 413 `payload_too_large` / gRPC `RESOURCE_EXHAUSTED` | n/a |
| `max_module_bytes` | 67108864 | bytes | `[api] max_module_bytes` | API (module upload) | 413 `payload_too_large` / gRPC `RESOURCE_EXHAUSTED` | n/a |
| `MAX_CONTEXT_BYTES` | 65536 | bytes | no | engine (`set-status` JSON and `random-bytes` length, live and replay) | attempt fails `protocol_violation` (`set-status json exceeds 65536 bytes`, `random-bytes len exceeds 65536`) | a recorded status replays unchanged |
| `MAX_RUN_TAGS` | 64 | tags per run | no | API and apply (`StartRun`) | 400 `invalid_argument` | n/a (start only) |
| `MAX_RUN_TAG_LEN` | 128 | bytes per tag | no | API and apply (`StartRun`) | 400 `invalid_argument` | n/a (start only) |
| `MAX_SIGNAL_NAME_LEN` | 256 | bytes | no | API and apply | 400 `invalid_argument` | n/a |
| `MAX_REASON_LEN` | 1024 | bytes | no | API and apply (cancel / terminate / pause reasons) | 400 `invalid_argument` | n/a |
| `MAX_REQUEST_ID_LEN` | 64 | bytes | no | API and apply | 400 `invalid_argument` | n/a |
| `MAX_PAGE_LIMIT` | 1000 | items per page | no | API (list endpoints) | none: a larger page size is capped | n/a |
| `MAX_RESULT_WAIT_MS` | 60000 | milliseconds | no | API (result long-poll) | none: a longer wait is capped | n/a |

## How a run past a limit behaves

The engine checks the run limits each time the guest appends a new (live) event. Replayed
events are never checked.

1. The guest issues a command whose event would exceed a limit. The event is not
   appended. The engine ends the attempt with `AttemptFailure::LimitExceeded{limit, max,
   actual}`, after flushing the live events appended before the breach.
2. This failure is not retryable: another attempt would hit the same limit. The executor
   completes the run `Failed` with `resumable: true`. The failure result is
   `{"reason_code": "limit_exceeded", "kind": "limit_exceeded", "message":
   "limit_exceeded: <limit> <actual> > <max>"}`, for example
   `limit_exceeded: history_events 21 > 20`.
3. To continue the run, raise the limit in `[limits]`, restart the node, then call
   `POST /v1/runs/{id}/retry`. The run resumes in place with the same run id: its journal
   replays and execution continues from where it stopped.

The history limit counts every event in the journal, including inputs delivered by the
partition such as timer fires and effect completions. `actual` is the length the journal
would have had with the rejected event. Terminal and failure records (`RunCompleted`,
`RunFailed`, `RunContinuedAsNew`, `StepAttemptFailed`) are exempt from the history limit,
so a run at the limit can always complete, fail or continue-as-new cleanly. A step's
closing `StepCompleted` is not exempt.

A run is only checked when it appends something new. If its journal is already at or past
a lowered limit, it replays normally and fails at its first new live append.

Runs executed on remote workers enforce the same limits. The server sends them in
`Welcome.limits` (`EngineLimits` fields 10 to 12), and the worker reports a breach on
the wire as `WireAttemptFailure::LimitExceeded`.

The payload size is the length of the payload data: the step's `Ok` output, the run
result, the child input or the continue-as-new input. A step that fails with an error
counts the encoded length of its outcome.

## Never enforced during replay

Run limits are checked only on the live append path (`ReplayState::append_live` in the
engine). They are never checked when an event is replayed, and partition apply never
checks them for existing commands. You can lower any run limit at any time: journals that
already exceed it still replay with zero appended events, so recovery, rebuild and replay
are unaffected.
