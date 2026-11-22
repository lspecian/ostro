---
phase: 11-run-operations-embedded-ui
plan: 15
subsystem: ui
tags: [ui, askama, htmx, csp, csrf, dns-rebinding, timeline, replay-markers]
requires:
  - 11-03 (ostro-ui skeleton, askama, vendored htmx, UiAuth seam)
  - 11-09 (fakes, ListRunsRequest.{tags,order,dlq}, HistoryEvent.attempt, OpsApi)
provides:
  - ostro_ui::router with the run pages, assets and the D-P11-15 guard on every /ui route
  - security::{guard, SameOriginMutation (is_htmx, field, fields), csrf_token, HostAllowlist, apply_headers}
  - pages::{UiState, UiError (Api|Forbidden|NotFound|BadRequest|Rejected|Internal)}
  - auth::{Reader (read), Operator (operate)} extractors over UiAuth
  - timeline::build_timeline (exact/approximate replay markers, copied rerun prefix, rerun controls)
  - ostro_ui::ROUTES (doc-hidden route table the hardening tests walk; 11-16/11-17 append)
affects: [11-16, 11-17, 11-18, Phase 14 (UiAuth seam in use)]
tech-stack:
  added: []
  patterns:
    - "one axum middleware (from_fn_with_state) does the Host/:authority check and stamps the security headers, so even the guard's own 403 is hardened"
    - "the mutation extractor buffers the body (256 KiB) and exposes the parsed form fields, so handlers keep one extractor"
    - "rendered-page lint runs over raw tags only (escaped user text has no raw '<'); template lint runs over the whole file"
key-files:
  created:
    - crates/ostro-ui/templates/ui/{runs,dlq,run_detail,_run_status,_timeline,_logs,error}.html
    - crates/ostro-ui/tests/{hardening,security,pages}.rs
    - crates/ostro-ui/tests/common/mod.rs
    - crates/ostro-ui/tests/snapshots/pages__*.snap (8)
  modified:
    - crates/ostro-ui/src/{lib,security,auth,assets,pages,timeline}.rs
    - crates/ostro-ui/templates/ui/layout.html
    - crates/ostro-ui/assets/{app.css,app.js}
decisions:
  - "app.js is used: a small progressive helper that shows htmx:responseError/sendError in #flash (htmx does not swap 4xx/5xx). No eval or Function constructor"
  - "Assets are served at /ui/assets/{*path} (wildcard) because 11-03 vendored htmx under assets/vendor/; the layout references /ui/assets/vendor/htmx-2.0.11.min.js"
  - "Host check: every host the request names (Host header and URI authority) must be allowlisted; no host at all is 403. allowed_hosts entries with a port match exactly, without a port match any port"
  - "Origin fallback (no Sec-Fetch-Site) accepts http:// or https:// with the exact request host: HTTP/1.1 carries no scheme and the host is already allowlisted. A present Origin must match even with Sec-Fetch-Site: same-origin; same-site/cross-site/none are 403"
  - "Tabs are plain links (?tab=timeline|logs), not htmx swaps, so aria-current stays correct and the page works without JS; the fragments exist for refresh and polling"
  - "DLQ tab is its own template (dlq.html) and route /ui/dlq; 'not available' when ext.ops is None"
metrics:
  duration: "~1h30m"
  completed: 2026-11-22
---

# Phase 11 Plan 15: UI foundation Summary

The embedded UI now renders the run list, DLQ tab, run detail, timeline and logs from the same traits REST uses. All of it sits behind the D-P11-15 security model: CSP and security headers, the Host/`:authority` allowlist on every route, and a CSRF/same-origin guard for mutations. Tests are plain Rust against the 11-09 fakes. `GET /` → `/ui/` no longer lands on a blank page.

## Route table

| Method | Path | Handler | Notes |
|---|---|---|---|
| GET | `/ui` | `pages::ui_root` | 308 → `/ui/` |
| GET | `/ui/`, `/ui/runs` | `pages::runs` | filters `workflow`, `queue`, `status`, `tags` (repeatable, at most 8), `order=newest\|oldest`, `page_token` |
| GET | `/ui/runs/{run_id}` | `pages::run_detail` | `?tab=timeline` (default) or `?tab=logs` |
| GET | `/ui/runs/{run_id}/status` | `pages::run_status` | fragment; `hx-get` + `hx-trigger="every 2s"` + `hx-swap="outerHTML"` only while non-terminal |
| GET | `/ui/runs/{run_id}/timeline` | `pages::run_timeline` | fragment |
| GET | `/ui/runs/{run_id}/logs` | `pages::run_logs` | fragment; "Logs not available" when `ext.run_logs` is None |
| GET | `/ui/dlq` | `pages::dlq` | `workflow`, `queue`, `reason`, `tags`, `include_dismissed`, `page_token` |
| GET | `/ui/assets/{*path}` | `assets::serve` | ETag / 304 |
| POST | `/ui/_probe` | `security::probe` | only when `UiConfig::test_probe` |

- Every route needs the `read` scope through `auth::Reader`, except assets and the probe.
- Unknown run: 404 `error.html`. `ApiError`: its HTTP status (a 503 `unavailable` renders as 503).
- A malformed status or more than 8 tags: 400.

## Asset table

| Path under `/ui/assets/` | Bytes | Content-Type | ETag |
|---|---|---|---|
| `app.css` | 5743 | `text/css; charset=utf-8` | `"sha256-b3cfc4fa1a6a79e96943efae80c4ebe2f906d5f0a171e91b88d4c65fafcd32c4"` |
| `app.js` | 952 | `text/javascript; charset=utf-8` | `"sha256-e8bcbabffdfb80d50fc96b079df312453c29c86988ad179b560346a073756727"` |
| `vendor/htmx-2.0.11.min.js` | 52182 | `text/javascript; charset=utf-8` | `"sha256-d6fdc75f204e6bdefa99b69bf1e6d4ac69b8a364f77929f45c13476b4000f717"` |

- ETags are computed once (`OnceLock`) from the `include_bytes!` data.
- `Cache-Control: no-cache`. `If-None-Match` (a list, `W/` or `*`) gets a 304 that carries the ETag.
- The tests recompute the sha256 from the files on disk, so a changed asset cannot ship with a stale ETag.

**app.js is used.** It shows htmx's `responseError`/`sendError` in the `#flash` region and is optional: every page works without it.

## Security model as built (D-P11-15)

- **Headers on every `/ui` response**, error pages and the guard's 403 included:
  - the exact CSP string
  - `nosniff`, `Referrer-Policy: no-referrer`, `X-Frame-Options: DENY`
  - `Cache-Control: no-store`, unless the handler set its own (assets: `no-cache`)
- **Host / `:authority` allowlist, checked on every route.** Allowed:
  - the bound address `host:port`
  - `localhost`, `127.0.0.1` and `[::1]` with any port
  - `[ui] allowed_hosts`

  The Host header and the URI authority (HTTP/2 `:authority`) are both checked. Malformed values, `user@host`, bare IPv6, `127.0.0.1.evil.example` and requests with no host are 403.
- **`SameOriginMutation`** (for 11-16's handlers): Host check again, then:
  - (a) Same origin: `Sec-Fetch-Site: same-origin` (a present Origin must also match), or, when Sec-Fetch-Site is absent, `Origin` equal to `http(s)://<host>`.
  - (b) Token: `HX-Request: true` plus an `X-Ostro-Csrf` header equal to the `ostro_csrf` cookie, or a `csrf` form field equal to the cookie. Comparison is constant-time.

  Otherwise 403. It buffers at most 256 KiB, parses `application/x-www-form-urlencoded` bodies, and exposes `field(name)` / `fields()` / `is_htmx()`.
- **CSRF cookie:** `ostro_csrf=<64 hex>; Path=/ui; HttpOnly; SameSite=Strict` (`; Secure` when the URI scheme is https). Pages mint it when it is absent (32 bytes from `getrandom`; an RNG failure is a 500, with no fallback token) and embed the token in `<body hx-headers='{"X-Ostro-Csrf":"…"}'>`.
- **Layout:** the htmx-config meta is exactly `{"allowEval":false,"allowScriptTags":false,"includeIndicatorStyles":false,"historyCacheSize":0,"selfRequestsOnly":true}`, plus `hx-history="false"` on body. Only external scripts and styles, and indicator styles live in app.css.

## Timeline (D-P11-18)

- **Exact mode:** used when any event has a non-zero `attempt`. Events are grouped by attempt. Inputs with no attempt stay in the open group, so a hot wait shows no marker. Every later group opens with `attempt e.n replayed seq 0..k`, where k is the seq before the group.
- **Approximate mode:** used for pre-Phase 11 journals. Markers:
  - `likely re-activation` before an input that follows a command
  - `likely resume (requeued retry)` after a StepAttemptFailed with `backoff=requeue`
  - `retry: step attempt failed`
  - `terminated by an operator`

  Each re-activation opens an `activation N (approximate)` group.
- **Rerun prefix:** events with seq < the `__ostro/rerun_from=<cut>` tag form one leading group, and each row is labelled `copied from <source>`.
- **Durations and bars:** durations come from consecutive `ts` values. Bars are SVG `<rect x=… width=…>`, never `style=`.
- **"Rerun from here":** shown per cut that `OpsApi::analyze_rerun` reports.
  - Safe cut: a link to `/ui/runs/{id}/rerun?from_seq=N`. That is 11-16's form target; until 11-16 lands it 404s.
  - Unsafe cut: disabled, with the analyzer's reason and the nearest safe seq.
  - No controls (with a note) when the ops slot is absent, the run is non-terminal, or the analyzer errors.

## Snapshots (`crates/ostro-ui/tests/snapshots/`)

`pages__run_list`, `pages__run_list_filtered_empty`, `pages__dlq`, `pages__run_detail_completed`, `pages__status_running`, `pages__timeline_completed`, `pages__timeline_rerun`, `pages__logs_running`. They capture the `<main>` element or the fragment. The fakes are deterministic and requests carry a fixed `ostro_csrf` cookie, so nothing is redacted.

## Verification

- `cargo test -p ostro-ui --locked` passes:
  - lib: 14 (host allowlist rules, CSRF mint/reuse/Secure, timeline unit tests: exact grouping, hot wait, approximate markers, rerun prefix, durations/SVG geometry, rerun controls; formatting; escaping)
  - hardening: 6
  - security: 10
  - pages: 13
  - vendored: 2
- The plan's verify loop passes (each suite matches `test result: ok. [1-9]… passed`).
- `cargo clippy -p ostro-ui --all-targets --locked -- -D warnings`: clean. `cargo fmt -p ostro-ui --check`: clean.
- `cargo test -p ostro-server --test p11_contract --locked`: 10 passed. `GET /` → `/ui/` now renders the real run list (200) on a live server.
- `cargo test -p ostro --test dev_startup --locked`: 4 passed, 1 ignored.
- `cargo check --workspace --all-targets --locked`: clean after rebasing onto 875b042 (11-10).
- Not run: the full `scripts/ci-local.sh`. The machine is shared with parallel executors, so the affected crates were run individually.

## Deviations from Plan

### Auto-fixed / adjusted

**1. [Rule 3 - Blocking] One commit instead of two.**
- Task 1's security layer, lib routes and the error page depend on `pages::{UiState, UiError}`, so Task 1 cannot compile without the page module.
- Both tasks therefore landed in one green commit (c4afd47).

**2. Extra template `dlq.html` (not in files_modified).**
- The DLQ tab is its own page (`/ui/dlq`) rather than a branch inside `runs.html`.

**3. Asset route is `/ui/assets/{*path}`, not `{name}`.**
- 11-03 vendored htmx under `assets/vendor/`, and its layout already referenced `/ui/assets/vendor/htmx-2.0.11.min.js`.

**4. `lib.rs` unit test replaced.**
- 11-03's `render_layout_probe` was removed together with the probe template struct, as its own doc comment said it would be.
- The escaping check now goes through a real 404 page.
- The `dep_anchors` for sha2/getrandom were removed because they are now used.

**5. Workflow per the executor brief.**
- Worktree `wt-11-15`, slot-5 target, CAS on main, instead of the plan's `/tmp/ostro-wt-11-15` and private index.

## Open issues

- **`retry_of` is not in the API.** `RunInfo` has no `retry_of`, and the server's history details for `run_started` do not include it.
  - The lineage card therefore shows "rerun from seq N (source run not reported by the API)", and copied rows read "copied from the source run".
  - The code already reads a `retry_of` key from the RunStarted details if one appears.
  - Suggested fix (11-10 or 11-18): add an optional `retry_of` to `RunInfo` or to the history `run_started` details. Both are additive.
- **`status_seq` is not shown.** `RunInfo` has no `status_seq`, so the published-status card shows the pretty JSON without a sequence number.
- **Browser checks are human_needed (11-18 checklist).** Render, keyboard navigation, the polling swap, and no CSP console errors.
- **Real local address.** `UiConfig.bound_addr` is still the configured listen address (11-04). Loopback names are always allowed, so a `:0` test listener works. A non-loopback `0.0.0.0` listener needs `[ui] allowed_hosts` for the hostname operators use.

## Commits

- c4afd47 feat(11-15): UI security layer, embedded assets, run pages, timeline and logs

## Self-Check: PASSED
- All files in key-files exist in c4afd47 (`git show --stat`).
- c4afd47 is on main.
