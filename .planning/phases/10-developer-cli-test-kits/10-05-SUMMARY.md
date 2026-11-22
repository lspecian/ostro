---
phase: 10-developer-cli-test-kits
plan: 05
subsystem: api
tags: [api, rest, openapi, time-skipping, replay, fake, parity, client]
requires:
  - phase: 10-developer-cli-test-kits
    provides: "10-01 reservations (REST paths, error codes time_skipping_disabled / sample_too_large)"
  - phase: 11-operations-ui
    provides: "11-09 ostro-api surface + OpenAPI snapshot (base of this plan; not running concurrently)"
provides:
  - "ControlPlane::{dev_time, dev_skip, replay_module} (appended at the trait's end; Unimplemented defaults)"
  - "domain types DevTimeMode, DevTimeInfo, DevSkip, DevSkipResult, ReplaySelect, ReplayRequest (+validate), ReplayVerdictKind, ReplayVerdictInfo, ReplaySummary (+of), ReplayResult; MAX_REPLAY_SAMPLE = 1000; REASON_TIME_SKIPPING_DISABLED, REASON_SAMPLE_TOO_LARGE; time_skipping_disabled(), sample_too_large(n)"
  - "REST GET /v1/dev/time, POST /v1/dev/time/skip, POST /v1/modules/{sha}/replay (OpenAPI tags dev / tooling)"
  - "FakeControlPlane: set_dev_mode, grow-only offset, replay ok/skipped over stored runs"
  - "ostro-server control/dev.rs (409 stub, 10-10) and control/replay.rs (501 stub, 10-11)"
  - "ostro-cli Client::{dev_time, dev_skip, replay_module}, REPLAY_EXTRA_TIMEOUT = 300 s"
affects: [10-09, 10-10, 10-11, 10-13]
tech-stack:
  added: []
  patterns:
    - "Phase 10 tooling routes are REST-only (D-P10-18): parity.rs REST_ONLY list pinned against the OpenAPI dev/tooling tags"
    - "Replay route documented as /v1/modules/{sha}/replay, mounted as /v1/modules/{sha256}/replay via rest::wildcard_routes (matchit needs one param name per segment)"
key-files:
  created:
    - crates/ostro-api/tests/dev_replay_surface.rs
    - crates/ostro-server/src/control/dev.rs
    - crates/ostro-server/src/control/replay.rs
    - crates/ostro-server/tests/dev_surface.rs
  modified:
    - crates/ostro-api/src/domain.rs
    - crates/ostro-api/src/fake.rs
    - crates/ostro-api/src/rest.rs
    - crates/ostro-api/tests/parity.rs
    - crates/ostro-api/tests/snapshots/openapi__openapi_document.snap
    - crates/ostro-server/src/control/mod.rs
    - crates/ostro-cli/src/client.rs
key-decisions:
  - "Error codes stay inside the fixed ErrorCode table: time_skipping_disabled = 409 conflict + details.reason, sample_too_large = 400 invalid_argument + details.reason (+ limit, requested). Unknown module / run = plain 404 not_found (module_not_found is not a reserved name, and get_module already answers not_found without a reason)"
  - "The reason constants live in domain.rs (error.rs is not in this plan's files), like REASON_SECRET_* and phase7::REASON_JOIN_TOKEN_INVALID"
  - "`by` uses rest::parse_friendly_ms; calendar units get a computed hint (`7d` -> `write it as 168h`) via jiff Span total with days_are_24_hours"
  - "ReplayBody.sample is u64 on the wire so any oversize number (even > u32) is 400 sample_too_large rather than a JSON decode error; sample 0 is plain invalid_argument"
  - "`workflow` / `status` with `runs` is 400 (they apply only with `sample`), matching 10-03's CLI (--workflow/--status require --sample)"
  - "Explicit run ids: 1..=1000, each 1..=128 printable ASCII bytes without spaces"
  - "openapi.rs needed no change: the routes are part of rest::api_routes, so the document picks them up"
requirements-completed: []
duration: ~1h
completed: 2026-11-22
---

# Phase 10 Plan 05: API contract for dev time and replay Summary

**The REST-only Phase 10 tooling contract (dev clock + server-side replay) is in place end to end: trait defaults, REST + OpenAPI, the fake, a pinned parity exclusion, server stubs answering 409 / 501, and the Rust client methods.**

## Route table

| Method | Path | Tag | Body | 200 | Errors |
|---|---|---|---|---|---|
| GET | `/v1/dev/time` | dev | — | `{now_ms, offset_ms, mode: off\|auto\|manual, idle, next_due_ms}` | 409 `time_skipping_disabled` |
| POST | `/v1/dev/time/skip` | dev | exactly one of `{"by":"72h"}` / `{"to_ms":N}` | `{now_ms, offset_ms, skipped_ms}` | 400 neither/both/bad duration (`7d` → "write it as 168h"); 409 `time_skipping_disabled` |
| POST | `/v1/modules/{sha}/replay` | tooling | `{runs? \| sample?, workflow?, status?, allow_missing?}` | `{module_sha256, verdicts: [{run_id, workflow, verdict, seq?, detail?, appended}], summary: {ok, ok_frontier, diverged, skipped, error}}` | 400 malformed sha / neither or both / bad run id / unknown status; 400 `sample_too_large` (> 1000); 404 unknown module or run |

Server today (ServerControlPlane): dev routes 409 `time_skipping_disabled` (10-10 fills `control/dev.rs`), replay 501 `unimplemented` with `details.method = replay_module` (10-11 fills `control/replay.rs`). Edge validation (e.g. `sample_too_large`) already applies on the real server.

## Error codes used

- `conflict` + `details.reason = "time_skipping_disabled"` (HTTP 409), message "time skipping is off; start `ostro dev --ephemeral --time-skipping`".
- `invalid_argument` + `details.reason = "sample_too_large"`, `details.limit = 1000`, `details.requested` (HTTP 400).
- `invalid_argument` without a reason for every other shape error; `not_found` for unknown module/run; `unimplemented` for the default trait bodies and the server replay stub.

## Parity exclusion mechanism

`crates/ostro-api/tests/parity.rs` gains a commented `REST_ONLY: &[(&str, &str)]` with exactly `(get, /v1/dev/time)`, `(post, /v1/dev/time/skip)`, `(post, /v1/modules/{sha}/replay)` citing D-P10-18. The parity scenarios never exercise them; the new test `rest_only_exclusions_are_exactly_the_phase10_tooling_routes` asserts that the set of OpenAPI operations tagged `dev`/`tooling` equals the list and that each is described as REST-only. No proto changes.

## OpenAPI snapshot diff stat

```
 .../tests/snapshots/openapi__openapi_document.snap | 430 +++++++++++++++++++++
 1 file changed, 430 insertions(+)
```

Additions only: the three paths, the `dev` / `tooling` tags, and the schemas DevTimeDto, DevTimeModeDto, DevSkipBody, DevSkipDto, ReplayBody, ReplayDto, ReplayRunDto, ReplaySummaryDto, ReplayVerdictDto.

## Base / Phase 11 contention

Base 4b65737. 11-04 and 11-09 both have SUMMARYs (not in progress). While working, main moved (10-03, 10-07, a 10-DECISIONS amendment) but none touched ostro-api, the snapshot, `control/mod.rs` or `client.rs`; both commits rebased cleanly and the snapshot test passed unchanged on the new base, so no re-application was needed.

## Commits

- cbf6f3c feat(10-05): dev time and replay REST contract, fake control plane, parity exclusion
- 9ff94a1 feat(10-05): server dev/replay delegation stubs and client methods

## Verification

- `cargo test -p ostro-api --locked`: all suites pass (dev_replay_surface 6, parity 9 incl. the exclusion test, openapi 2, domain unit tests incl. phase10_defaults_are_unimplemented / replay_request_validation).
- `cargo test -p ostro-server --test dev_surface`: 1 passed (in-process server: 409, 409, 501, 400 sample_too_large).
- `cargo test -p ostro-cli --locked`: 65 + 6 passed (phase10_urls).
- `cargo test -p ostro --test cli openapi`: passed (`ostro openapi` = library document).
- `cargo clippy --workspace --all-targets --locked -- -D warnings`: clean. `cargo fmt --all --check`: clean. `cargo check --workspace --all-targets` after rebase: clean.
- Clock grep on control/dev.rs and control/replay.rs: no hits.
- Full `scripts/ci-local.sh --no-musl` was NOT run (shared machine with parallel builds); the code-level steps above cover the touched crates.

## Deviations from Plan

- **[Rule 3] Replay route mounted under `{sha256}`.** axum/matchit rejects two parameter names in one segment (`/v1/modules/{sha256}` exists), so the route is documented as `/v1/modules/{sha}/replay` but mounted as `/v1/modules/{sha256}/replay` through the existing `rest::wildcard_routes` helper.
- **No `module_not_found` reason.** Not among the reserved names (D-P10-19); unknown module is plain 404 `not_found`, as `get_module` already answers.
- **openapi.rs untouched.** Listed in files_modified but unnecessary (routes flow in via `rest::api_routes`).
- **Single commit for Task 1** instead of a separate RED commit (the plan asked for one commit per task).

## Self-Check: PASSED

- Files exist: crates/ostro-api/tests/dev_replay_surface.rs, crates/ostro-server/src/control/{dev,replay}.rs, crates/ostro-server/tests/dev_surface.rs.
- Commits cbf6f3c and 9ff94a1 are on main.
