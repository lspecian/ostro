# Webhooks

Ostro accepts signed webhooks from third parties (Stripe, GitHub, Slack, any Standard
Webhooks sender, or any HMAC-signing service) and maps each verified delivery to a workflow
action: start a run, signal a run, or complete an await token. Requirement TRIG-01, design
P8-D12.

- [Route and limits](#route-and-limits)
- [Defining a webhook](#defining-a-webhook)
- [Provider setup](#provider-setup)
- [Idempotency](#idempotency)
- [Actions](#actions)
- [Responses: ack after apply](#responses-ack-after-apply)
- [Retention: at least 72 h for Stripe hooks](#retention-at-least-72-h-for-stripe-hooks)
- [Security notes](#security-notes)
- [Metrics](#metrics)

## Route and limits

```
POST /hooks/{name}
```

- `{name}` is the webhook's name (`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`).
- The body is read as **raw bytes**. Nothing parses it until the signature is verified.
- The body limit is `[triggers] webhook_body_limit` (default 1 MiB, `1048576`). A larger
  body is rejected with `413` before the webhook is even looked up.
- There is no gRPC form: a signature covers the raw HTTP body and headers.
- The replay window for Stripe, Slack and Standard Webhooks is the webhook's own
  `tolerance_s` (see below); when that is unset (0), the node-level key
  `[triggers] webhook_tolerance` (default `300s`) applies (08-23). Generic HMAC keeps
  unset = no replay window; GitHub and `none` carry no timestamp.

## Defining a webhook

Webhooks are managed through `/v1/webhooks` (REST) or `ostro.api.v1.Webhooks` (gRPC):

| Method | Path | |
|---|---|---|
| GET | `/v1/webhooks` | list |
| POST | `/v1/webhooks` | create (201; 409 if the name exists) |
| GET | `/v1/webhooks/{name}` | read |
| PUT | `/v1/webhooks/{name}` | replace; CAS with `version_tag` (body) or `If-Match`; stale = 409 `version_mismatch` |
| DELETE | `/v1/webhooks/{name}` | delete; optional CAS with `?version_tag=` or `If-Match` |

```json
{
  "name": "stripe-checkout",
  "action": { "kind": "start", "workflow": "fulfil-order", "input": "/data/object" },
  "verifier": { "kind": "stripe", "secret_ref": "stripe-endpoint-secret" },
  "idempotency": "json:/id"
}
```

On create and update the server checks:

- the verifier spec: options valid for the kind, `tolerance_s` unset for `github` and `none`,
  generic-HMAC options consistent (400);
- `secret_ref` names an existing sealed secret, unless the kind is `none` (400);
- the action's workflow exists, for `start` and `signal` (400);
- the idempotency extractor syntax (400).

The secret itself never appears in any response. A webhook only carries the **name** of a
sealed secret (`secret_ref`). The secret is resolved at delivery time: the sealed meta
`Secret` record with exactly that name, or `_/<name>` for a bare name (the Phase 6
default-tenant form). `plaintext-v0` records (Phase 4 format) are used as stored.
`aes256gcm:<kid>` records are opened with the node sealing key: `OSTRO_SEALING_KEY`, else
`<data_dir>/sealing.key` if it exists.

## Provider setup

| `verifier.kind` | Provider setting | Headers checked | Signed content | Default key |
|---|---|---|---|---|
| `stripe` | Endpoint signing secret `whsec_…`, stored as is (not base64-decoded) | `Stripe-Signature: t=…,v1=…` | `{t}.{body}`, HMAC-SHA256 hex | `json:/id` (`evt_…`) |
| `github` | Webhook secret | `X-Hub-Signature-256: sha256=…` | body, HMAC-SHA256 hex | `header:X-GitHub-Delivery` |
| `slack` | App signing secret | `X-Slack-Signature: v0=…`, `X-Slack-Request-Timestamp` | `v0:{ts}:{body}`, HMAC-SHA256 hex | `json:/event_id` |
| `standard` | Standard Webhooks secret `whsec_<base64>` | `webhook-id`, `webhook-timestamp`, `webhook-signature: v1,…` | `{id}.{ts}.{body}`, HMAC-SHA256 base64 | `header:webhook-id` |
| `hmac` | Any shared secret | configurable | configurable | `none` |
| `none` | none: **unauthenticated** | none | none | `none` |

Notes per provider:

- **Stripe:**
  - every `v1` entry is tried, so secret rotation works;
  - unknown entries (`v0`, `secret=`, `destination=`, `payload=`) are ignored and never
    trusted.
- **Slack:**
  - `{"type":"url_verification","challenge":…}` is answered `200 text/plain` with the
    challenge, after the signature check, so "Enable Events" works as soon as the webhook
    exists;
  - Slack retries (`X-Slack-Retry-Num`) carry the same body and `event_id`, so they
    deduplicate.
- **Generic HMAC (`hmac`):** the options are
  - `header` (required);
  - `prefix`, for example `sha256=`;
  - `algorithm`: `sha256` (default) or `legacy_sha1` (opt-in, for old senders);
  - `encoding`: `hex` (default) or `base64`;
  - `timestamp_header` with `template: "{ts}.{body}"`, or neither (`template: "{body}"`);
  - `tolerance_s` needs a `timestamp_header`.

  ```json
  "verifier": { "kind": "hmac", "secret_ref": "partner", "header": "x-signature",
                "prefix": "sha256=", "encoding": "hex",
                "timestamp_header": "x-timestamp", "template": "{ts}.{body}", "tolerance_s": 300 }
  ```

- **`tolerance_s`:** the replay window in seconds. Unset means `[triggers] webhook_tolerance`
  (default 300 s) for Stripe, Slack and Standard Webhooks, and no window for generic HMAC. It is measured against the server's
  clock (the one injectable clock).

## Idempotency

Every delivery gets a **delivery key**:

| `idempotency` | Key |
|---|---|
| unset | the provider default (table above) |
| `header:<name>` | that header's value |
| `json:<pointer>` | the string or number at that JSON pointer in the body |
| `none`, or the extractor finds nothing | `sha256:<hex sha256 of the body>`: a byte-identical redelivery still deduplicates |

Keys are at most 256 bytes; a longer key is rejected with `400`.

How the key is used:

| Action | Dedupe |
|---|---|
| `start` | `StartRun.idempotency_key = "__hook/<webhook name>/<delivery key>"` with `ConflictPolicy::ReturnExisting` |
| `signal` | `SignalRun.request_id` = the delivery key (the 64-hex sha256 of it when longer than 64 bytes); the run's SignalSeen set drops redeliveries (D13) |
| `complete_token` | the token completes once; a redelivery is reported as a duplicate |

## Actions

### `start`

```json
"action": { "kind": "start", "workflow": "fulfil-order", "version": 3, "input": "body" }
```

- Starts the workflow. `version` is optional: unset means the active version.
- `input` is `"body"` (the whole body is the run input) or a JSON pointer into the body.
- A W3C `traceparent` request header, if present and valid, becomes the run's trace parent.
  An invalid one is ignored.
- Answer: `200 {"run_id", "created", "duplicate"}`. A duplicate delivery returns the
  existing run with `created: false, duplicate: true`, and only one run exists.

### `signal`

```json
"action": { "kind": "signal", "workflow": "fulfil-order", "name": "paid",
            "run_by": { "idempotency_pointer": "/data/object/id" } }
```

- Signals a run of `workflow`. The signal payload is the whole body (it must be JSON).
- The target run is found in one of two ways:
  - `run_by.run_id_pointer`: the run id at that pointer. A run of another workflow counts as
    not found.
  - `run_by.idempotency_pointer`: a value `v` at that pointer. Ostro looks for the run of
    `workflow` whose idempotency key is `__hook/<h>/<v>`, trying every `start` webhook `h`
    of that workflow in name order, then `v` itself (a run started over the API with
    idempotency key `v`).
- This is the Stripe "start, then signal" round trip:
  1. Hook A, `start`, `idempotency: "json:/data/object/id"` (the checkout session id).
  2. The workflow waits for signal `paid`.
  3. Hook B, `signal`, `run_by.idempotency_pointer: "/data/object/id"`, completes the run.
- Answer: `200 {"run_id", "accepted", "duplicate"}`.
- An unknown run is `404`. Providers retry `404`s on their normal schedule, so a signal that
  arrives before its start is delivered on a later retry.
- A run that has already finished:
  - If this delivery's signal was applied before the run ended (a redelivery: the run's
    SignalSeen set holds the delivery's request id), the answer is
    `200 {"run_id", "accepted": true, "duplicate": true}`, so the provider stops retrying.
  - Otherwise the signal can never be applied, and the answer is `409` with
    `error.details.reason = "run_terminal"`.

### `complete_token`

```json
"action": { "kind": "complete_token", "token_pointer": "/token" }
```

- Completes the await token (`ot1_…`) found at that pointer. The completion payload is the
  whole body.
- Answer: `200 {"run_id", "accepted", "duplicate"}`.
- A token that is already completed, or whose run has finished, answers
  `200 {"duplicate": true}`, so the provider stops retrying.

## Responses: ack after apply

A `2xx` is sent **after the command has been applied** by the partition (the run exists,
the signal is journaled, the token is completed). It is never sent after the workflow
finishes. A crash before the answer means the provider retries, and the delivery key
makes the retry a duplicate.

| Case | Status |
|---|---|
| applied, or a duplicate delivery | 200 (JSON; `duplicate` says which) |
| Slack `url_verification` | 200 `text/plain` (the challenge) |
| missing, malformed or wrong signature; stale timestamp | 401, `error.details.reason` = `missing_header`, `malformed`, `bad_signature` or `stale` |
| body not JSON where the action needs it, pointer missing, key too long | 400 |
| signal to a finished run that never took this delivery's signal | 409, `error.details.reason` = `run_terminal` |
| unknown webhook; signal target run not found | 404 |
| body larger than `webhook_body_limit` | 413 |
| verifier misconfigured, secret missing or unopenable | 500 |
| partition unavailable (retry is safe) | 503 |

No failure case submits a command.

## Retention: at least 72 h for Stripe hooks

Stripe retries a failed delivery for up to **3 days**. Deduplication of `start` deliveries
lasts as long as the run's dedupe row, which is the run's **retention** (D9). Run retention
defaults to 24 h in `ostro dev`, and `[runs] retention` sets it on a server. **Set
retention ≥ 72 h for Stripe hooks**, through a `WorkflowPolicy` or `QueuePolicy` on the
target workflow or `[runs] retention`. Otherwise a retry that arrives after the run was
purged starts a second run. The same applies to any provider with a long retry schedule:
retention must cover the provider's retry window.

## Security notes

- **Verify before parse.** The order is fixed:
  1. look up the webhook;
  2. read the secret;
  3. verify the raw bytes;
  4. only then parse the body.

  The verifier returns a zero-size proof token, and every body reader requires it, so
  nothing can parse an unverified body.
- **Constant-time comparison.** Every MAC check uses `Mac::verify_slice`.
- **No auth layer on `/hooks`.** The signature is the authentication. Do not put
  `/hooks/*` behind the API's bearer auth; do restrict it at your proxy if the provider
  publishes source IP ranges.
- **`none` is unauthenticated.** Anyone who knows the URL can trigger the action. Use it
  only on private networks or for testing.
- **Secrets stay inside.** Secret values are never logged, returned, or put in error
  messages. A `401` names the failure class only (`bad_signature`, `stale` and so on),
  never a header value. The secret is held in a zeroize-on-drop buffer for one
  verification.
- **Replay window.** A captured request can be replayed only within `tolerance_s`, and
  within that window the delivery key turns a replay into a duplicate.

## Metrics

| Series | Labels |
|---|---|
| `ostro_webhook_requests_total` (counter) | `hook` (the webhook name; empty for unknown names, so scanners cannot grow the label set), `outcome` = `ok`, `duplicate`, `unauthorized`, `not_found`, `error` |
