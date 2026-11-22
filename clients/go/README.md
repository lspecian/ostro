# Ostro Go client

`github.com/lspecian/ostro/clients/go/ostro`: a hand-written client for the Ostro REST API.
Standard library only (`net/http`, `encoding/json`): the module has no requirements.
Go 1.23+.

## Install

```sh
go get github.com/lspecian/ostro/clients/go
```

## Quick start

```go
c := ostro.NewClient("http://127.0.0.1:7700") // "" = $OSTRO_SERVER, else this address
ctx := context.Background()

wasm, _ := os.ReadFile("module.wasm")
if _, err := c.UploadModule(ctx, wasm, "orders", ""); err != nil { ... }
if _, err := c.Activate(ctx, "order", 1); err != nil { ... }

run, err := c.StartRun(ctx, ostro.StartRunRequest{Workflow: "order", Input: map[string]any{"id": 7}})
_, err = c.Signal(ctx, run.RunID, "approved", map[string]any{"by": "ana"}, "")
res, err := c.Result(ctx, run.RunID, 30*time.Second) // res.Completed, res.Status, res.Result (raw JSON)
```

## Methods

Every method takes a `context.Context` first; its deadline and cancellation are honoured.
`Client.Timeout` (default 30 s) bounds each request; a result long-poll gets its wait on top.

| Method | Route |
| --- | --- |
| `UploadModule(ctx, wasm, name, tenant)` | `PUT /v1/modules/{sha256}` (the SHA-256 is computed for you) |
| `Workflows(ctx)` | `GET /v1/workflows` |
| `Activate(ctx, workflow, version)` | `PUT /v1/workflows/{name}/active` |
| `StartRun(ctx, StartRunRequest)` | `POST /v1/workflows/{name}/runs` |
| `GetRun(ctx, runID)` | `GET /v1/runs/{run_id}` |
| `ListRuns(ctx, RunFilter)` | `GET /v1/runs` |
| `History(ctx, runID, fromSeq, limit)` | `GET /v1/runs/{run_id}/history` |
| `Result(ctx, runID, wait)` | `GET /v1/runs/{run_id}/result` (long-poll) |
| `Signal(ctx, runID, name, payload, requestID)` | `POST /v1/runs/{run_id}/signals/{name}` |
| `Cancel(ctx, runID, reason)` | `POST /v1/runs/{run_id}/cancel` |
| `Terminate(ctx, runID, reason)` | `POST /v1/runs/{run_id}/terminate` |
| `Retry(ctx, runID)` | `POST /v1/runs/{run_id}/retry` |
| `CompleteToken(ctx, token, payload)` | `POST /v1/tokens/complete` |
| `DevTime(ctx)` | `GET /v1/dev/time` |
| `DevSkipBy(ctx, d)` / `DevSkipTo(ctx, toMs)` | `POST /v1/dev/time/skip` |
| `Replay(ctx, sha, ReplayRequest)` | `POST /v1/modules/{sha}/replay` |

Path values go through `url.PathEscape`, so a tenant-qualified workflow name (`acme/order`)
stays one segment. `ostro.Routes` lists every route; a contract test checks it against the
server's OpenAPI document.

## Errors

Every failure is an `*ostro.Error` (match it with `errors.As`): `Status` (0 without a
response), `Code`, `Message` and `Details` from the REST error body; `Reason()` is
`Details["reason"]`. Transport failures use `Code` `"unreachable"`, `"timeout"` (client
timeout or context deadline; `errors.Is(err, context.DeadlineExceeded)` holds) or
`"cancelled"`; a non-JSON error response uses `"http_<status>"`.

```go
var e *ostro.Error
if errors.As(err, &e) && e.Status == 404 { ... }
```

## Time skipping (tests)

Against `ostro dev --ephemeral --time-skipping[=manual]` the dev clock can be moved forward
so long sleeps, timers and backoffs finish at once:

```go
_, err := c.DevSkipBy(ctx, 72*time.Hour)
t, err := c.DevTime(ctx) // t.NowMs, t.OffsetMs, t.Mode, t.Idle, t.NextDueMs
```

Without time skipping these fail with status 409 and `Reason() == "time_skipping_disabled"`.
