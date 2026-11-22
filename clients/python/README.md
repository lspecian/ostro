# ostro-client (Python)

A hand-written client for the Ostro REST API. Standard library only (`urllib`, `json`):
no runtime dependencies. Python 3.9+.

## Install

Once published: `pip install ostro-client`. Until then install from a checkout
(`pip install clients/python`) or from the path inside the SDK bundle.

## Quick start

```python
from ostro_client import Client, OstroError

c = Client("http://127.0.0.1:7700")          # default: $OSTRO_SERVER, else this address

with open("module.wasm", "rb") as f:
    c.upload_module(f.read(), name="orders")
c.activate("order", 1)

run = c.start_run("order", {"id": 7}, idempotency_key="order-7", tags=["team=shop"])
c.signal(run["run_id"], "approved", {"by": "ana"})
print(c.result(run["run_id"], wait="30s"))   # {"completed": True, "status": "completed", "result": ...}
```

Responses are the server's JSON objects as plain `dict`s (see `ostro openapi` for the
schemas).

## Methods

| Method | Route |
| --- | --- |
| `upload_module(wasm, name=None, tenant=None)` | `PUT /v1/modules/{sha256}` (the SHA-256 is computed for you) |
| `workflows()` | `GET /v1/workflows` |
| `activate(workflow, version)` | `PUT /v1/workflows/{name}/active` |
| `start_run(workflow, input=None, *, idempotency_key, tags, conflict_policy, version, queue, **extra)` | `POST /v1/workflows/{name}/runs` |
| `get_run(run_id)` | `GET /v1/runs/{run_id}` |
| `list_runs(*, workflow, status, queue, tags, limit, page_token)` | `GET /v1/runs` |
| `history(run_id, *, from_seq, limit)` | `GET /v1/runs/{run_id}/history` |
| `result(run_id, wait=None)` | `GET /v1/runs/{run_id}/result` (long-poll; `wait` as `"30s"` or seconds) |
| `signal(run_id, name, payload=None, *, request_id)` | `POST /v1/runs/{run_id}/signals/{name}` |
| `cancel(run_id, reason=None)` | `POST /v1/runs/{run_id}/cancel` |
| `terminate(run_id, reason=None)` | `POST /v1/runs/{run_id}/terminate` |
| `retry(run_id)` | `POST /v1/runs/{run_id}/retry` |
| `complete_token(token, payload=None)` | `POST /v1/tokens/complete` |
| `dev_time()` | `GET /v1/dev/time` |
| `dev_skip(by=None, *, to_ms=None)` | `POST /v1/dev/time/skip` |
| `replay(sha, *, runs=None, sample=None, workflow, status, allow_missing)` | `POST /v1/modules/{sha}/replay` |

Path values are percent-encoded as one segment, so a tenant-qualified workflow name
(`acme/order`) works as is. `ostro_client.ROUTES` lists every route; a contract test checks
it against the server's OpenAPI document.

## Errors

Every failure raises `OstroError` with `status` (HTTP status, `None` without a response),
`code`, `message` and `details` from the REST error body
`{"error": {"code", "message", "details"}}`; `e.reason` is `details.reason` when present.
A server that cannot be reached gives `code="unreachable"`, a timed-out request
`code="timeout"`, a non-JSON error response `code="http_<status>"`.

```python
try:
    c.get_run(run_id)
except OstroError as e:
    if e.status == 404:
        ...
```

## Time skipping (tests)

Against `ostro dev --ephemeral --time-skipping[=manual]` the dev clock can be moved forward
so long sleeps, timers and backoffs finish at once:

```python
c.dev_skip("24h")          # or dev_skip(3600) seconds, or dev_skip(to_ms=...)
print(c.dev_time())        # {"now_ms", "offset_ms", "mode", "idle", "next_due_ms"}
```

Without time skipping these answer 409 with `reason == "time_skipping_disabled"`.
