---
phase: 08-triggers-bottomless-observability
plan: 15
subsystem: triggers
tags: [webhooks, stripe, github, slack, standard-webhooks, hmac, idempotency, signals, await-tokens, metrics]

requires:
  - phase: 08-triggers-bottomless-observability
    provides: 08-06 pure verifiers/extractors (VerifierSpec::from_meta, verify, slack_challenge, IdempotencyExtractor, InputSelector); 08-10 WebhookApi/HookIngress traits, ControlCtx (meta, secrets, clock, partition client), tests/common/phase8.rs signers
provides:
  - "crates/ostro-server/src/control/webhooks.rs: WebhookController (WebhookApi CRUD + HookIngress), webhook_to_dto/webhook_from_dto, hook_idempotency_key, signal_request_id, body_hash_key"
  - "ostro_webhook_requests_total{hook,outcome} counter"
  - "docs/webhooks.md operator guide"
affects: [08-20 SC1 Stripe round trip, 08-21 OTLP canary (webhook secret leg), 08-16 metrics docs, 06-14 secrets resolver wiring]

tech-stack:
  added: [metrics facade on ostro-server (workspace dep, already in the lock)]
  patterns:
    - "Fixed delivery order: lookup -> sealed secret -> verify raw bytes (injected clock) -> parse -> submit -> answer after apply"
    - "Signal run_by stored in meta run_ref: '/ptr' = run-id pointer, 'idempotency:/ptr' = idempotency pointer"
    - "Unknown hook names are recorded with hook=\"\" so the metric label set stays meta-bounded"

key-files:
  created:
    - crates/ostro-server/tests/webhooks.rs
    - docs/webhooks.md
  modified:
    - crates/ostro-server/src/control/webhooks.rs
    - crates/ostro-server/src/control/mod.rs
    - crates/ostro-server/tests/phase8_smoke.rs
    - crates/ostro-server/Cargo.toml
    - Cargo.lock

key-decisions:
  - "Secret resolution path: sealed meta Secret record read directly (the Phase 6 SecretResolver exists in ostro-bridge but is not wired into the server until 06-14). secret_ref matches exactly, or `_/<name>` for a bare name; plaintext-v0 opens raw, aes256gcm opens via ostro-bridge SealedStore over OSTRO_SEALING_KEY or an existing <data_dir>/sealing.key (never created here)"
  - "Delivery key: configured extractor, else the vendor default; with `none` or when nothing is found, `sha256:<hex(sha256(body))>`"
  - "Start idempotency key `__hook/<name>/<key>` + ReturnExisting; signal request_id = key (64-hex sha256 when the key exceeds MAX_REQUEST_ID_LEN = 64)"
  - "Signal by idempotency pointer tries `__hook/<h>/<v>` for every start hook h of the same workflow (name order), then `v`, via the dry-run start lookup (dedupe row); run-id pointer targets must belong to the hook's workflow"
  - "complete_token conflicts token_already_completed / run_terminal answer 200 duplicate (redelivery after the completion applied)"
  - "verifier kind none is allowed (unauthenticated, documented); its secret_ref is dropped"
  - "Stored idempotency \"\" = the verifier default (DTO None round-trips)"

requirements-completed: [TRIG-01]

duration: ~1h15m
completed: 2026-11-20
---

# Phase 8 Plan 15: Webhooks controller Summary

**`POST /hooks/{name}` and webhook CRUD now work on the real server. A delivery is verified on its raw bytes, with the sealed secret and the injected clock, before anything parses it. It is then mapped to `StartRun` (`__hook/<name>/<key>`, ReturnExisting), `SignalRun` (request_id = delivery key, SignalSeen) or `CompleteToken`, and the 200 is sent after apply. Duplicate deliveries start exactly one run, and no failure path submits a command. CRUD validates the verifier spec, the secret and the workflow, and never returns secret material. docs/webhooks.md is the operator guide.**

## Secret resolution path

**Sealed meta, not the Phase 6 resolver.** `ostro_bridge::secrets::SecretResolver` exists (06-05), but nothing builds one in ostro-server: 06-14 wires it, and `ControlCtx` only carries `SecretsAccess` (sealed records).

- The controller reads the sealed `Secret` record named by `secret_ref` (exact, else `_/<name>`).
- It opens `plaintext-v0` records directly (Phase 4 format) and `aes256gcm:<kid>` records with `ostro_bridge::secrets::SealedStore`, over the node key: `OSTRO_SEALING_KEY`, else `<data_dir>/sealing.key` if present. The ring is loaded lazily, once.
- The value is a zeroize-on-drop `SecretValue`, held for one `verify` call.
- When 06-14 wires a resolver into `ControlCtx`, `open_secret` is the one function to switch.

## Status codes

| Case | Status |
|---|---|
| applied (new run / signal / completion) | 200 JSON, `duplicate: false` |
| duplicate (existing run, SignalSeen, token already completed or run terminal) | 200 JSON, `duplicate: true` |
| Slack `url_verification` (after verification) | 200 `text/plain` challenge |
| unknown hook | 404 |
| missing header / malformed / bad signature / stale | 401, `error.details.reason` = `missing_header` / `malformed` / `bad_signature` / `stale` |
| body not JSON where needed, pointer missing, key > 256 bytes | 400 |
| signal target run not found | 404 |
| verifier misconfigured, secret missing or unopenable | 500 |
| body > `[triggers] webhook_body_limit` | 413 (router, 08-10) |
| partition unavailable | 503 |

CRUD: create 201 or 409 (exists); invalid verifier, missing secret, missing workflow, bad extractor or reserved name 400; stale `version_tag` on PUT/DELETE 409 `version_mismatch` (gRPC `ALREADY_EXISTS`, the existing Conflict mapping); unknown 404.

## Dedupe key formats

| Use | Format |
|---|---|
| delivery key | extracted (`header:<name>`, `json:<pointer>`, vendor default: Stripe `json:/id`, GitHub `header:x-github-delivery`, Slack `json:/event_id`, Standard `header:webhook-id`), else `sha256:<64 hex of sha256(body)>` |
| start idempotency key | `__hook/<hook name>/<delivery key>` |
| signal request id | delivery key if <= 64 bytes, else 64-hex sha256 of it |

## Task Commits

1. **Task 1 + Task 2 code: controller (ingress + CRUD), tests, metrics dep, smoke update:** `fd55a6c`
2. **Task 2 docs: docs/webhooks.md:** `cc38c60`

The CRUD half of Task 2 shares the controller file and the test file with Task 1, so the code landed in one commit.

## Tests (crates/ostro-server/tests/webhooks.rs, 5 tests)

- `start_action_per_verifier_dedupes_deliveries`:
  - covers Stripe (input pointer), GitHub (an **AES-256-GCM sealed** secret via `<data_dir>/sealing.key`), Slack, Standard Webhooks, generic HMAC (`header:x-delivery`) and `none`;
  - for each: valid = `created=true`; same delivery = same run id, `created=false`, `duplicate=true`; tampered body = 401 `bad_signature`; no headers = 401;
  - exactly 6 runs exist, and each run's result equals its selected input;
  - stale Stripe = 401 `stale`;
  - Slack challenge echoed as text/plain, and a wrong signature on the challenge = 401;
  - a Slack retry (`X-Slack-Retry-Num`, re-signed) = duplicate;
  - unknown hook = 404;
  - `/metrics` has `ostro_webhook_requests_total` with ok/duplicate/unauthorized/not_found and no `hook="nope"` label.
- `stripe_tolerance_uses_the_injected_clock`: the server runs on a `ManualClock` at 1_700_000_000 s.
  - Signed at system time = stale.
  - Signed at `t0-59` with `tolerance_s` 60 = accepted.
  - After `advance(120 s)` = stale.
  - One run total.
- `signal_action_targets_runs_and_dedupes_redelivery` (the SC1 shape):
  - Stripe start keyed by `json:/data/object/id` starts the `signals` fifo run;
  - the GitHub signal hook by `idempotency_pointer` delivers d1, then d1 again (`duplicate=true`), then d2;
  - a run-id-pointer hook delivers d3;
  - the result is exactly `[1, 2, 3]`;
  - an unknown session and a bad run id are 404, a bad signature is 401, and only one run exists.
- `complete_token_action_completes_an_await_token`:
  - the generic HMAC (base64) hook completes the `tokens` guest, and the run result carries the body;
  - a redelivery = 200 `duplicate`;
  - a body without a token = 400.
- `crud_rest_and_grpc_validate_and_never_leak_secrets`:
  - create, duplicate (409), 8 invalid cases (400), get, list, PUT stale (409 `version_mismatch`), PUT ok (tag increases), PUT unknown (404), DELETE stale (409) then ok, then 404;
  - gRPC create, get, duplicate (`ALREADY_EXISTS`), missing secret (`INVALID_ARGUMENT`), list, stale delete, delete;
  - every response (and a 401 delivery body) is scanned for the canary secret as raw, hex and base64.

Unit tests in `control::webhooks`: 5, covering verifier ordinals against meta, DTO/meta round trip for every action and kind, invalid verifiers, the `none` secret drop and the key formats.

## Verification

All runs used worktree wt-08-15 and `CARGO_TARGET_DIR=target/wt-08-15`, re-run after rebasing onto main 6532976.

- `cargo test -p ostro-server --locked --test webhooks`: `test result: ok. 5 passed`.
- `cargo test -p ostro-server --locked --test phase8_smoke`: `ok. 1 passed`.
- `cargo test -p ostro-server --locked --lib`: `ok. 55 passed`.
- `cargo test -p ostro-server --locked` (before the rebase, at 9f3cd0d + changes): every binary ok, 0 failed. Counts: 55, 29, 18, 16, 15, 13, 12, 11, 9, 8, 5, 5, 4, 4, 4, 3, 3, 2, 2, 1, 1, 1; perf/sleepers ignored by design.
- `cargo clippy -p ostro-server --all-targets --locked -- -D warnings`: clean.
- `cargo build -p ostro-server`: warning-free; `metrics` is used, so there is no unused-dependency warning.
- `grep -q '72 h' docs/webhooks.md`: ok.
- `cargo fmt --all --check`: my files are clean.
- **Not run:** `bash scripts/ci-local.sh --no-musl`, because of the parallel-build cap (other executors running). Covered by the ostro-server suite and clippy above.

## Deviations from Plan

**1. [Rule 3 - Blocking] Added `metrics.workspace = true` to ostro-server (Cargo.toml plus one Cargo.lock line).**
- The plan requires `ostro_webhook_requests_total` but forbids Cargo changes.
- ostro-server had no `metrics` facade: 08-01 declared `metrics-exporter-prometheus` only, which does not re-export the facade.
- The crate is already in the lock at 0.24.6. The only lock change is `"metrics"` added to ostro-server's dependency list.
- Rebuilt from the current main blob before the commit. 08-16 (whose `services/metrics_poll.rs` is in ostro-server) needs the same dependency and now has it.

**2. [Rule 3 - Blocking] Edited `tests/phase8_smoke.rs` (outside files_modified).**
- It asserted that the webhook routes answer the 08-15 stub (503 "not yet implemented (08-15)").
- The webhook rows were removed from the stub loop and replaced by real-answer checks: `GET /v1/webhooks` = 200 empty, `POST /hooks/h` = 404, gRPC `Webhooks.List` = ok. The 413 body-limit check stays.
- 08-14/17/18 will need the same edit for their rows.

**3. [Doc] `control/mod.rs` table:** the webhooks row "Now" column was changed from `stub` to `done` (one doc line).

**4. [Design] The signal `run_by` encoding in meta `run_ref`** (`idempotency:<ptr>`) and the start-hook search for idempotency pointers. The DTO has no "start hook" field, so the lookup tries every start hook of the same workflow, then the raw value.

**5. [Design] A missing key falls back to the body hash.** When a configured extractor finds no key, the body hash is used instead of rejecting with 400: identical redeliveries still deduplicate. This is documented.

## Out of scope (pre-existing)

- `cargo fmt --all --check` reports a diff in `crates/ostro-partition/src/apply/terminal.rs` (import order) at main 9f3cd0d. That file is not touched here (ba5e72d "rustfmt 06-03 partition files" landed meanwhile and may have fixed it).

## Self-Check: PASSED

- FOUND: crates/ostro-server/src/control/webhooks.rs (`impl HookIngress`, `webhook::verify` via `verify`, `ReturnExisting`), crates/ostro-server/tests/webhooks.rs, docs/webhooks.md
- FOUND: fd55a6c and cc38c60 on main
