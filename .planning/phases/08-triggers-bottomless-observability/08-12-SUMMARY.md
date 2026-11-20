---
phase: 08-triggers-bottomless-observability
plan: 12
subsystem: cli
tags: [cli, schedules, webhooks, rest-client, clap]

requires:
  - phase: 08-triggers-bottomless-observability
    provides: 08-10 Phase 8 REST surface (/v1/schedules, /v1/webhooks), FakePhase8, router_with; 05-14 CLI client/output conventions
provides:
  - "ostro schedules {ls,get,create,update,delete,pause,resume,trigger}"
  - "ostro webhooks {ls,get,create,update,delete}"
  - "ostro_cli::client::Client::call generic JSON request"
  - "schedules::render / webhooks::render return the verb's stdout text (testable); run prints it"
affects: [08-14, 08-15, 08-18, 08-20]

tech-stack:
  added: []
  patterns:
    - "update = GET + overlay of the given flags + PUT with --version-tag (CAS; stale = 409, exit 1)"
    - "unimplemented_hint: 503 'not yet implemented' stub and 404 'API is not enabled' read 'not implemented on this server' (status/code kept, exit 1)"
    - "webhook bodies scrubbed of any field containing 'secret' except secret_ref before printing"

key-files:
  created:
    - crates/ostro-cli/src/commands/schedules.rs
    - crates/ostro-cli/src/commands/webhooks.rs
    - crates/ostro-cli/tests/phase8_verbs.rs
    - crates/ostro/tests/cli_phase8.rs
  modified:
    - crates/ostro-cli/src/commands/mod.rs
    - crates/ostro-cli/src/client.rs
    - crates/ostro-cli/src/lib.rs
    - crates/ostro-cli/Cargo.toml
    - Cargo.lock

key-decisions:
  - "Verbs render to a String (schedules::render / webhooks::render) and run() prints it, so in-process tests scan stdout for secret bytes without capturing the process stdout"
  - "One generic Client::call(method, segments, query, body) instead of eight typed client methods; route paths live in the verb modules"
  - "--timezone optional (empty = UTC on the wire); --input-json (inline or @file) conflicts with --input-file"
  - "--start-at/--end-at take RFC 3339 or integer ms; --catchup-window and --tolerance take friendly durations (tolerance whole seconds >= 1s)"
  - "webhooks update keeps the current action/verifier and overlays given flags; --action/--verifier with a different kind starts a fresh object (old fields dropped)"
  - "secret_ref required locally unless --verifier none (usage error, exit 2); --hmac-* only with --verifier hmac"

requirements-completed: []

duration: ~45m
completed: 2026-11-20
---

# Phase 8 Plan 12: CLI verbs for schedules and webhooks Summary

**`ostro schedules` and `ostro webhooks` are thin REST clients over the 08-10 surface. They support `--json` or 05-14-style tables, CAS-aware updates via `--version-tag`, and a clear "not implemented on this server" message for stub controllers. They never print secret material: `--secret-ref` names a sealed secret, and response bodies are scrubbed.**

## Verb table

| Verb | Request | Human output |
|---|---|---|
| `schedules ls` | `GET /v1/schedules` | table NAME WORKFLOW CRON TIMEZONE PAUSED OVERLAP NEXT_FIRE TAG (or `no schedules`) |
| `schedules get <name>` | `GET /v1/schedules/{name}` | `key: value` lines (times RFC 3339) |
| `schedules create --name --workflow --cron [--version] [--timezone] [--input-json\|--input-file] [--overlap skip\|buffer-one\|allow-all\|cancel-other\|terminate-other] [--catchup skip\|one\|all] [--catchup-window] [--start-at] [--end-at] [--paused]` | `POST /v1/schedules` | `schedule N created` + detail |
| `schedules update <name> --version-tag T [--workflow] [--cron] [same options]` | `GET`, then `PUT /v1/schedules/{name}` (current + overlay, output-only fields dropped, `version_tag = T`) | `schedule N updated` + detail |
| `schedules delete <name> [--version-tag T]` | `DELETE /v1/schedules/{name}[?version_tag=T]` | `schedule N deleted` |
| `schedules pause\|resume <name>` | `POST /v1/schedules/{name}/pause\|resume` | `schedule N paused\|resumed` + detail |
| `schedules trigger <name> [--request-id]` | `POST /v1/schedules/{name}/trigger` | `schedule N triggered: run <id>` |
| `webhooks ls` | `GET /v1/webhooks` | table NAME ACTION TARGET VERIFIER SECRET_REF IDEMPOTENCY TAG |
| `webhooks get <name>` | `GET /v1/webhooks/{name}` | detail incl. `endpoint: POST /hooks/{name}` |
| `webhooks create --name --action start\|signal\|complete-token --verifier none\|stripe\|github\|slack\|standard\|hmac [--secret-ref] [--workflow --version --input body\|/ptr] [--run-id-pointer\|--idempotency-pointer --signal] [--token-pointer] [--idempotency header:X\|json:/p\|none] [--tolerance] [--hmac-header --hmac-prefix --hmac-algorithm sha256\|legacy-sha1 --hmac-encoding hex\|base64 --hmac-timestamp-header --hmac-template]` | `POST /v1/webhooks` | `webhook N created (deliveries: POST /hooks/N)` + detail |
| `webhooks update <name> --version-tag T [...]` | `GET`, then `PUT /v1/webhooks/{name}` | `webhook N updated` + detail |
| `webhooks delete <name> [--version-tag T]` | `DELETE /v1/webhooks/{name}[?version_tag=T]` | `webhook N deleted` |

Every verb takes `--server`, `--json` and `--timeout`, through the shared `Conn`. With `--json`, the verb prints the REST body verbatim. Webhook bodies are scrubbed first.

## Error-code mapping

| Case | Exit | Message |
|---|---|---|
| clap error (bad enum value, missing required flag, bad pointer/idempotency/duration/time, unknown flag such as `--secret`) | 2 | clap usage |
| Local usage check after parsing (`--verifier` other than none without `--secret-ref`, action fields that do not fit the action, `--hmac-*` without `hmac`) | 2 | `ostro: error: ...` |
| 400 validation | 1 | server message `(HTTP 400 invalid_argument)` |
| 404 not found | 1 | `(HTTP 404 not_found)` |
| 409 stale `version_tag` | 1 | `... (HTTP 409 conflict, reason version_mismatch)` |
| 503 from a controller stub ("not yet implemented (08-NN)") | 1 | `schedules\|webhooks are not implemented on this server (not yet implemented (08-14)) (HTTP 503 unavailable)` |
| 404 "the ... API is not enabled on this server" (slot absent) | 1 | same "not implemented on this server" wording |
| Any other 503, or a transport error | 1 | unchanged |

## Cargo.lock

**Changed by one line.** `axum` was added to ostro-cli's dependency list, as a dev-dependency (workspace) that serves the fake router on 127.0.0.1:0. The `ostro-api` dev-dependency already existed (08-01). No new crates entered the lock. The lock was regenerated from main 0b1d58c's lock before the commit.

## Task Commits

1. **Task 1: verbs + FakePhase8-backed tests**: `5d28348`
2. **Task 2: spawned-binary smoke**: `407c8f4`

## Verification

All runs used the worktree with `CARGO_TARGET_DIR=target/wt-08-12`, rebased onto main 0b1d58c.

Plan checks:
- `cargo test -p ostro-cli --test phase8_verbs --locked`: ok, 6 passed.
  - schedule CRUD with CAS: stale tag gives 409, exit 1, `version_mismatch` in the message; a fresh tag is applied; delete stale/fresh; 404 after delete; 400 on a bad name;
  - pause/resume flip `paused`; trigger prints a run id; `--request-id` dedupes;
  - webhooks CRUD; output never contains the dummy secret; no `--secret` flag;
  - a misbehaving server's secret fields are scrubbed from human and JSON output;
  - a 503 stub and a 404 disabled API both give "not implemented on this server";
  - exit-code mapping.
- `cargo test -p ostro-cli --lib`: 31 passed, including new parser/body/scrub unit tests.
- `cargo test -p ostro --test cli_phase8 --locked`: ok, 3 passed. Against the current `ostro dev`, the controllers are stubs, so the round trip took the mapped "not implemented" path.
- `cargo clippy -p ostro -p ostro-cli --all-targets --locked -- -D warnings`: clean.

Wider checks:
- `cargo test -p ostro -p ostro-cli --locked`: all ok. ostro has cli 5, cli_phase8 3, cli_verbs 7, data_dir_lock 1, dev_startup 4 (+1 ignored), e2e_dev 3, order_saga 4 and server_config 4; ostro-cli has lib 31 and phase8_verbs 6.
- `cargo fmt -p ostro -p ostro-cli --check`: clean.

**Not run:** the full `bash scripts/ci-local.sh --no-musl` (about 50 min), because four other executors were building in parallel.

Workspace `cargo fmt --all --check` fails on main 0b1d58c in files this plan does not touch:
- `crates/ostro-partition/src/apply/terminal.rs`;
- `crates/ostro-partition/tests/{apply_effects,apply_lifecycle,rebuild_digest}.rs`;
- `crates/ostro-partition/tests/common/gen_effects.rs`.

These come from 06-03 and are out of scope.

## Deviations from Plan

**1. [Rule 3] `crates/ostro-cli/src/client.rs`: added `Client::call`.** The file is not in files_modified. The existing client's `request`/`send` are private and it had no PUT/DELETE helper. One generic method (18 lines) avoids duplicating the transport/error mapping. The route paths stay in the verb modules.

**2. [Rule 3] `crates/ostro-cli/src/lib.rs`: `use axum as _;` dev-dep anchor.** The file is not in files_modified. It is the same pattern as the existing `ostro_api` anchor, and keeps the lib test target free of the unused-dependency warning.

**3. [Plan reading] `axum` dev-dependency.** Serving `ostro_api::router_with` on 127.0.0.1:0 needs `axum::serve`. The lock change is that one line, so Cargo.lock changed.

**4. [Plan reading] Secret scan.** The 08-10 DTO and fake carry only `secret_ref`, so there is no stored secret value to leak. The tests do two things instead:
- they use a dummy secret constant that every output is scanned for, and assert there is no flag that accepts secret material;
- they add a misbehaving-server test whose bodies contain `secret`/`signing_secret` fields. The CLI's `scrub_secrets` removes those fields.

**5. [Plan reading] `--timezone` is optional** (empty = UTC, per the DTO), and `--verifier none` is accepted too, because the DTO has it. The server may refuse it.

## Follow-ups

- 08-14 and 08-15: once the controllers land, `cli_phase8::round_trip_against_ostro_dev` takes its success branch automatically.
- 08-20: full CLI flow end to end.
- 08-18: `ostro backup` can reuse `Client::call` for JSON, but needs a streaming download for the tar.

## Self-Check: PASSED

- Created files exist on main: schedules.rs, webhooks.rs, tests/phase8_verbs.rs, crates/ostro/tests/cli_phase8.rs.
- Commits 5d28348 and 407c8f4 are on main.

## Post-plan fixes (2026-11-20, 4a5007a)

- `cli_phase8::round_trip_against_ostro_dev` still accepted only the stub answer, and failed with `workflow "greet" does not exist` once 08-14/08-15 landed the controllers. It now drives the real success path against `ostro dev`:
  - deploys `signals`;
  - schedules: create, get, ls, update, pause, resume, trigger (request-id dedupe) and delete, plus the 400, 404 and 409 cases;
  - webhooks: create, get, ls, update and delete, with a github verifier whose secret is sealed via `secret set --from-env`, plus an unknown secret_ref (400).
- The secret-material checks stay: no `--secret` flag, and the canary never appears in CLI output or in the server debug log.
