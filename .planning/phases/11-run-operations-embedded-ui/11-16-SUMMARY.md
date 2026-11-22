---
phase: 11-run-operations-embedded-ui
plan: 16
subsystem: ui
tags: [ui, htmx, askama, csrf, actions, rerun, dlq, bulk, progressive-enhancement]
requires:
  - 11-15 (SameOriginMutation, auth::{Reader,Operator}, UiState/UiError, ROUTES, status fragment, "Rerun from here" links)
  - 11-09 (FakeControlPlane/FakeOps, OpsApi DTOs)
  - 11-10 (rerun_run on the real server; landed during this plan)
provides:
  - actions::routes() with the run lifecycle, rerun, DLQ and bulk routes (merged in ostro_ui::router)
  - pages::parse_run_filter (shared run-list filter parser, used by the bulk form)
  - ostro_ui::ROUTES now lists the 11-16 routes (hardening + foreign-host walks cover them)
affects: [11-17 (appends its routes after these), 11-18 (ui_e2e, browser checklist), Phase 14 (operate scope enforced through Operator)]
tech-stack:
  added: []
  patterns:
    - "every action is a plain form (csrf field, 303) that htmx enhances (hx-post + header token); htmx gets a fragment or HX-Redirect"
    - "htmx errors answer 200 + HX-Retarget #flash (htmx does not swap 4xx/5xx); plain forms get the error page with the API status"
    - "lifecycle htmx answers carry the status card plus the action buttons as an hx-swap-oob section, so buttons follow the new state"
    - "tests record trait calls through FakeControlPlane::clock_ticks (one mutating call each) and a hand-boxed recording OpsApi (no async-trait dev-dep)"
key-files:
  created:
    - crates/ostro-ui/templates/ui/{_run_actions,_dlq_actions,_flash,rerun,_rerun_preview,bulk,_bulk_progress}.html
    - crates/ostro-ui/tests/actions.rs
    - crates/ostro-ui/tests/snapshots/actions__rerun_preview_{ok,unsafe}.snap
  modified:
    - crates/ostro-ui/src/{actions,lib,pages}.rs
    - crates/ostro-ui/templates/ui/{run_detail,runs,dlq}.html
    - crates/ostro-ui/assets/app.css
    - crates/ostro-ui/tests/{hardening,security}.rs
    - crates/ostro-ui/tests/snapshots/pages__{dlq,run_detail_completed,run_list,run_list_filtered_empty}.snap
decisions:
  - "htmx action errors are 200 + HX-Retarget: #flash / HX-Reswap: outerHTML with a visible #flash div: the layout's htmx-config (pinned by the hardening test) keeps default responseHandling, so a 409 would not be swapped. Plain form posts keep the API status (409/400/404/501/429)"
  - "Retry is offered for Failed and TimedOut only. The plan said 'Failed/TimedOut/paused', but retry_run on a live (paused) run is 409 run_not_terminal; a paused run gets Resume"
  - "DLQ dismiss via htmx answers an empty 200 and the row form has hx-target='closest tr' hx-swap='delete'; a plain form goes back to /ui/dlq. Requeue lands on the resulting run (same id for resume, the new id for restart/rerun)"
  - "The rerun form is a page (GET, read scope); preview and submit need operate. Rerun refusals render in #rerun-preview (htmx: 200 + HX-Retarget #rerun-preview; form: the rerun page with the API status)"
  - "Bulk: the runs page 'Bulk…' link carries the current filter to GET /ui/bulk?…, which pre-fills the form; preview is a plain form post (whole page with estimate, sample and a confirm form re-posting the fields as hidden inputs); progress polls every 2 s while pending/running/paused_backpressure"
  - "dlq.html (an 11-15 template, not in files_modified) gained an Actions column including _dlq_actions.html: needed for 'from the DLQ tab an operator can requeue and dismiss'. hardening.rs/security.rs gained a {job_id} mapping so the route walks cover /ui/bulk/{job_id}"
metrics:
  duration: "~1h15m"
  completed: 2026-11-22
---

# Phase 11 Plan 16: UI actions Summary

The UI now covers the SC5 actions: cancel, terminate, retry, signal, pause/resume, rerun from a step with a dry-run preview, DLQ requeue and dismiss, and bulk jobs (dry run, create, progress, cancel). Every mutating route takes `Operator` (the `operate` scope) and `SameOriginMutation` (Host, same-origin and CSRF checks). Every action works as a plain form without JavaScript (csrf field, 303) and is enhanced by htmx.

## Routes (all behind `security::guard`)

| Method | Path | Answer | hx-confirm |
|---|---|---|---|
| POST | `/ui/runs/{run_id}/cancel` (optional `reason`) | status card + OOB action buttons (htmx) / 303 run page | yes |
| POST | `/ui/runs/{run_id}/terminate` (`reason`) | same | yes |
| POST | `/ui/runs/{run_id}/retry` | same; a restart → `HX-Redirect` / 303 to the new run | no |
| POST | `/ui/runs/{run_id}/signal` (`name`, `payload` JSON, empty = null) | same; invalid JSON → 400 with no trait call | no |
| POST | `/ui/runs/{run_id}/pause` (optional `detail`), `/resume` | same | no |
| GET | `/ui/runs/{run_id}/rerun?seq=\|from_seq=\|step=&occurrence=&version=` | rerun form (cut, version radio active/source/number, analyzer note on the chosen seq) | — |
| POST | `/ui/runs/{run_id}/rerun/preview` | `rerun_run{dry_run:true}`: preview fragment (htmx) or the form page | no |
| POST | `/ui/runs/{run_id}/rerun` | `rerun_run{dry_run:false}` → `HX-Redirect` / 303 new run | no |
| POST | `/ui/dlq/{run_id}/requeue` (`mode` auto/resume/restart/rerun, `step`/`occurrence`/`seq`) | `HX-Redirect` / 303 resulting run | no |
| POST | `/ui/dlq/{run_id}/dismiss` (`reason`) | empty 200 (htmx deletes the row) / 303 `/ui/dlq` | no |
| GET | `/ui/bulk?<run-list filter>` | jobs list + new-job form pre-filled from the filter | — |
| POST | `/ui/bulk/preview` | `create_bulk{dry_run:true}`: estimate, sample, confirm form | — (confirm form: yes) |
| POST | `/ui/bulk` | `create_bulk` → `HX-Redirect` / 303 `/ui/bulk/{job_id}` | yes |
| GET | `/ui/bulk/{job_id}`, `/ui/bulk/{job_id}/progress` | job page / progress fragment (polls every 2 s while active) | — |
| POST | `/ui/bulk/{job_id}/cancel` | progress fragment (htmx) / 303 job page | yes |

Without JavaScript, `hx-confirm` does not apply: plain forms submit directly. The terminate reason field and the bulk preview page are the deliberate steps on that path.

Rerun refusals render their details: `rerun_cut_unsafe` shows the reason and a "Use the nearest safe step (seq N)" link (it keeps the version choice). `rerun_nondeterministic` shows the seq and detail. `run_not_terminal` says "terminate first" and links to the run page. Any other API error, such as a 501 Unimplemented, is shown with its status.

## Tests

`crates/ostro-ui/tests/actions.rs` has 17 tests:

- **lifecycle (5):**
  - Each htmx action makes exactly one mutating call (fake clock) with the right arguments, and its fragment carries the OOB buttons.
  - Form posts get a 303.
  - Cross-site and tokenless posts get a 403 and call nothing.
  - API errors: a 409 page for form posts, `#flash` retargeting for htmx. A bad JSON payload or a missing signal name gets a 400 with no call. An unknown run gets a 404.
  - Buttons follow run state, and cancel/terminate carry `hx-confirm`.
- **dlq (2):**
  - Requeue covers auto→restart, resume (same id), and rerun from step `s3` (checked through a recording OpsApi). Rerun with no cut gets a 400, and a non-member gets the API's 409.
  - Dismiss records the reason and the row disappears from the tab. htmx gets an empty 200.
- **scope (1):** with a read-only `UiAuth`, every mutation (lifecycle, DLQ, rerun, bulk) gets a 403 and calls nothing, while the GET pages still render.
- **rerun (4):**
  - The form renders the cut, the analyzer note and the version choice.
  - Preview sends the right dry-run request and renders verdict, cut and copied events (snapshot), with and without JS. Bad cuts or versions get a 400.
  - Submit redirects to the new run.
  - Refusals render their details (unsafe, with a snapshot; nondeterministic; not terminal; 501).
- **bulk (5):**
  - The runs-page link carries the filter into the form.
  - Preview sends the right dry-run request and shows the estimate, the sample, hidden fields, `hx-confirm` and the edit link.
  - All eight actions parse into the right requests, and bad forms get a 400 with no call.
  - Create → job page; progress polls until done; the jobs list shows the job; cancel works via htmx and via form; an unknown job gets a 404.
  - The 501, 429 and missing-slot cases are surfaced.

Whole `cargo test -p ostro-ui --locked` run: unit 14, actions 17, hardening 6, pages 13, security 10, vendored 2, all green. `cargo clippy -p ostro-ui --all-targets -- -D warnings` is clean and `cargo fmt --all --check` passes. `cargo check --workspace --all-targets` is green after rebasing onto 11-10.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] DLQ actions needed `dlq.html`**
- The DLQ tab template belongs to 11-15 and was not in files_modified. It gained one Actions column that includes `_dlq_actions.html`. Its snapshot was re-blessed.

**2. [Rule 3 - Blocking] Route walks needed a `{job_id}` mapping**
- `hardening.rs` creates a fake bulk job and maps `{job_id}`. It also lints the new rendered pages: run detail with actions, the rerun form, the bulk form and a bulk job.
- `security.rs` maps `{job_id}` for the foreign-host walk.

**3. [Rule 1 - Bug in plan text] Retry is not offered on paused runs**
- `retry_run` on a non-terminal run is 409 `run_not_terminal`, so paused runs get Resume instead (see decisions).

**4. Snapshots changed**
- `pages__run_detail_completed` (Actions card), `pages__run_list` and `pages__run_list_filtered_empty` ("Bulk…" link), and `pages__dlq` (Actions column) were re-blessed.

## Open items (for 11-18)

- Real-server UI e2e: rerun through `/ui/runs/{id}/rerun{,/preview}` against 11-10's `p11_rerun` is not exercised here. The UI is tested against FakeOps only, so this goes in ui_e2e. Bulk on the real server is still the Unimplemented stub (11-13 has not landed). The UI shows the API's 501 cleanly: the bulk page reports "Bulk jobs unavailable (501): …", and create/preview show the 501 error page or `#flash`.
- Real-browser checks are human_needed. They cover htmx swaps (OOB buttons, `hx-swap="delete"` on DLQ rows, `HX-Redirect`, retargeted `#flash`), `hx-confirm` dialogs, the 2 s polling stop on bulk progress, and the form-only path with JS disabled.
- The `#[allow(dead_code)]` markers on `SameOriginMutation::{field,fields}` and `auth::Operator` (11-15 files) are now stale. They are harmless; 11-18 can drop them.

## Self-Check: PASSED

- Files exist: crates/ostro-ui/src/actions.rs, crates/ostro-ui/tests/actions.rs, templates `_run_actions`, `_dlq_actions`, `_flash`, `rerun`, `_rerun_preview`, `bulk` and `_bulk_progress`.
- Commits: f7d5972 (Task 1), b03719a (Task 2), both on main.
