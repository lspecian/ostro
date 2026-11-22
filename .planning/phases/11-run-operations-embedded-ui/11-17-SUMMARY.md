---
phase: 11-run-operations-embedded-ui
plan: 17
subsystem: ui
tags: [ui, htmx, askama, inventory, queues, schedules, workers, partitions, csrf]
requires:
  - 11-15 (UiState/UiError, SameOriginMutation, auth::{Reader,Operator}, ROUTES, hardening walks)
  - 11-16 (action pattern: htmx fragment / 303 / #flash retarget)
  - 11-09/11-11 (InventoryApi + FakeInventory), Phase 7 (pause/resume, list_workers, drain_worker), Phase 8 (ScheduleApi)
provides:
  - inventory::routes() merged in ostro_ui::router (modules, workflows, queues, schedules, workers, partitions + actions)
  - ostro_ui::ROUTES lists the 11-17 routes; hardening/security walks map {w},{q},{name},{id}
  - layout nav links runs, DLQ, bulk, modules, workflows, queues, schedules, workers, partitions
  - tests/common: inventory fake in every fixture, ui_bare(), seed_inventory()
affects: [11-18 (ui_e2e, browser checklist, docs/ui.md)]
tech-stack:
  added: []
  patterns:
    - "inventory actions swap their table row (hx-target=closest tr, outerHTML); plain forms 303 back to the page (schedule trigger: to the started run)"
    - "'not available' = slot None, ErrorCode::Unimplemented, or the Phase 7 P7_PENDING Unavailable placeholder; any other error renders with its status"
key-files:
  created:
    - crates/ostro-ui/templates/ui/{modules,workflows,queues,schedules,workers,partitions}.html
    - crates/ostro-ui/templates/ui/{_workflow_row,_queue_row,_schedule_row,_worker_row,_partitions}.html
    - crates/ostro-ui/tests/inventory.rs
    - crates/ostro-ui/tests/snapshots/inventory__{layout_nav,modules,workflows,queues,schedules,workers,partitions}.snap
  modified:
    - crates/ostro-ui/src/{inventory,lib,auth,security}.rs
    - crates/ostro-ui/templates/ui/layout.html
    - crates/ostro-ui/assets/app.css
    - crates/ostro-ui/tests/{common/mod,hardening,security}.rs
decisions:
  - "Workflow pause state is not in WorkflowInfo, so the list offers both Pause and Resume; after an action the row shows the returned state (paused/resumed badge) and only the opposite button"
  - "Queue rows after pause/resume keep the inventory counts but take `paused` from the PauseState answer; a queue the inventory does not list yet renders with dashes"
  - "Schedule trigger: htmx gets the row with a 'started <run>' link; a plain form 303s to the run page. No request_id is passed (hx-confirm guards double fires)"
  - "Phase 7's default `Unavailable(P7_PENDING)` counts as 'not available'; a genuine 503 (e.g. no leader) is shown as a 503 error page"
  - "Partition health bar: SVG rect width = applied/log_end x 120 (full when log_end is 0), omitted when either offset is unknown; lag = log_end - applied"
metrics:
  duration: "~1h"
  completed: 2026-11-22
---

# Phase 11 Plan 17: UI inventory Summary

The UI now shows modules, workflows with their versions and active version, queues, schedules, workers and partition health (UI-03). Operators can pause and resume queues and workflows, pause, resume and trigger schedules, and drain workers (the remaining SC5 pause/resume actions). Every action takes `Operator` (the `operate` scope) and `SameOriginMutation`, and works as a plain form (303) that htmx enhances with a row swap.

## Routes (all behind `security::guard`)

| Method | Path | Source | Answer | hx-confirm |
|---|---|---|---|---|
| GET | `/ui/modules?page_token=` | `InventoryApi::list_modules` (50/page) | short sha (full in `title`), size, workflow chips → runs, uploaded | — |
| GET | `/ui/workflows` | `ControlPlane::list_workflows` | versions newest first, active highlighted, runs link, pause/resume | — |
| POST | `/ui/workflows/{w}/pause`, `/resume` | `pause_workflow`/`resume_workflow` | row (htmx) / 303 `/ui/workflows` | no |
| GET | `/ui/queues` | `InventoryApi::list_queues` | paused badge, ready, running, demand, flow `k=v` summary | — |
| POST | `/ui/queues/{q}/pause`, `/resume` | `pause_queue`/`resume_queue` | row (htmx) / 303 `/ui/queues` | no |
| GET | `/ui/schedules` | `ApiExtensions.schedules.list_schedules` | workflow (+version), cron, time zone (empty = UTC), state, next/last fire | — |
| POST | `/ui/schedules/{name}/pause`, `/resume` | `pause_schedule`/`resume_schedule` | row (htmx) / 303 `/ui/schedules` | no |
| POST | `/ui/schedules/{name}/trigger` | `trigger_schedule` | row + started-run link (htmx) / 303 `/ui/runs/{new}` | yes |
| GET | `/ui/workers` | `ControlPlane::list_workers` | id, labels, active/draining, leases, slots, node, connected | — |
| POST | `/ui/workers/{id}/drain` | `drain_worker` | row (htmx) / 303 `/ui/workers` | yes |
| GET | `/ui/partitions` | `InventoryApi::list_partitions` | role, leader, epoch, applied, log end, SVG bar + lag, ready, running, health + detail | — |
| GET | `/ui/partitions/fragment` | same | `#partitions` div polling `every 5s` (no polling when not available) | — |

htmx errors answer 200 with `HX-Retarget: #flash`. Plain forms get the error page with the API status (404 unknown workflow/worker/schedule, 503 for the Phase 7 placeholder).

## "Not available" with the current fakes and slots

- Every view renders "… not available on this server." (200, no table) when its slot is None:
  - `ext.inventory` None: modules, queues and partitions (the page and the fragment).
  - `ext.schedules` None: schedules. The schedule actions answer 404 / `#flash`.
- An `Unimplemented` answer (e.g. `InventoryApi` defaults "lands in 11-11") also renders "not available".
- So does Phase 7's default `Unavailable("phase 7: lands in 07-14")` from `list_workers`.
- Workflows render "not available" when `list_workflows` is Unimplemented.
- `FakeControlPlane` implements `list_workflows`, `list_workers` and pause/resume/drain, and `FakePhase8` implements `ScheduleApi`. With the full fixture (`ui()` + `seed_inventory`), every view is available.
- On the real server (11-11 inventory, Phase 7, Phase 8), every view should be available. That still needs confirming in 11-18's ui_e2e.

## Tests

`crates/ostro-ui/tests/inventory.rs` has 13 tests:

- **Nav (1):** a layout header snapshot; every view is linked and is `aria-current` on its own page.
- **Modules (1):** snapshot showing the short/full sha, KiB/MiB sizes, workflow chips and upload time. A hostile name is escaped, and `page_token` reaches `list_modules`.
- **Workflows (2):** a snapshot with the active version highlighted and both buttons. Pause and resume are each one call (fake clock), give a row fragment for htmx and a 303 for a form, and are idempotent. An unknown workflow gives `#flash` or a 404. Cross-site and tokenless posts are 403 with no call.
- **Partitions (1):** a snapshot with the SVG bar at width 90 (90/120). Unknown fields show as "—" with no bar, the unhealthy row is styled, and the detail is escaped. The fragment polls every 5 s and matches the page.
- **Workers (2):** a snapshot with label chips; a draining worker has no drain button and drain carries `hx-confirm`. Drain makes one call (htmx row; a form gets a 303; a repeat call does not tick the clock again). An unknown worker gives `#flash` or a 404. Guard posts are 403.
- **Queues (2):** a snapshot (flow summary, pause/resume per state). Pause and resume are each one call; the row keeps the inventory counts. A queue the inventory does not list gives a dashed row. Guard posts are 403.
- **Schedules (2):** a snapshot (time zone, UTC default, `hx-confirm` on trigger). Pause and resume change the state and bump `version_tag` once. Trigger via htmx gives a row with a started-run link to a real greet run; via a form it gives a 303 to the new run. An unknown schedule gives `#flash` or a 404. Guard posts change nothing.
- **Not available (1):** with every slot empty, a control plane with only the required methods (Phase 7 placeholder, Unimplemented workflow list) renders all six views as "not available" with status 200. The fragment does not poll. Actions report the error, and Unimplemented inventory methods also count as not available. A real 503 is shown as a 503.
- **Scope (1):** with a read-only principal, all 8 mutations are 403 "missing scope" with no call, no trigger and no schedule change, while the pages still render. With a principal that has no scopes, the pages are 403 as well.

Hardening: `every_route_carries_csp_and_security_headers` walks the 15 new ROUTES. The rendered-page lint covers the six new pages. A new test, `inventory_fragments_and_unavailable_views_pass_the_lint`, lints the fragment and the not-available pages. The security foreign-host walk covers the new routes.

Results:
- `cargo test -p ostro-ui --locked` passes: unit 17 (+3: byte/flow formatting, bar geometry, P7 placeholder), actions 17, hardening 7, inventory 13, pages 13, security 10, vendored 2.
- `cargo clippy -p ostro-ui --all-targets --locked -- -D warnings` is clean, and `cargo fmt --all --check` is clean.
- `cargo check --workspace --all-targets --locked` is green after rebasing onto a062e76 (11-14 / 12-02).
- `cargo test -p ostro-server --test p11_contract --locked`: 10 passed.
- The full `scripts/ci-local.sh` was not run, because the machine is shared with parallel executors.

## Deviations from Plan

### Auto-fixed / adjusted

**1. One commit instead of two (Task 1 + Task 2 in 4c24d14).**
- Both tasks share `inventory.rs`, the row helpers and the test fixture, and were built and verified together. Each task's verify command passes on the commit.

**2. [Rule 3] Row partial templates not listed in files_modified.**
- `_workflow_row`, `_queue_row`, `_schedule_row`, `_worker_row` and `_partitions` are shared by the page and the htmx fragment, following 11-16's `_run_actions` / `_dlq_actions` approach.

**3. [Rule 3] Test fixture and route walks.**
- `tests/common/mod.rs` installs a `FakeInventory` in every fixture, and adds `ui_bare()` and `seed_inventory()`. `Slots` is unchanged, so 11-15/11-16 tests are untouched.
- `hardening.rs` and `security.rs` map the new path parameters, and `hardening.rs` lints the new pages.

**4. Stale `#[allow(dead_code)]`.**
- Removed on `SameOriginMutation::{fields field, field(), fields()}`.
- `auth::Operator`'s allow is narrowed to the never-read field, like `Reader`. It is not stale: the principal is never read.

**5. Queue demand comes from `QueueView.demand`.**
- 11-11's `list_queues` already folds in the Phase 7 demand, so the page does not call `queue_demand` per queue.

## Open items (for 11-18)

- Real-browser checks (human_needed):
  - row swaps (`hx-target="closest tr"` with `<tr>` responses)
  - the `hx-confirm` dialogs on drain and trigger
  - the 5 s partition poll
  - the no-JS form paths
- Real-server ui_e2e should confirm all six views render with data on a live node: 11-11 inventory, Phase 7 workers, Phase 8 schedules.
- The "Bulk" nav link is not `aria-current` on bulk pages, because 11-16's handlers pass `nav: "runs"`. This is cosmetic: changing it means editing `actions.rs`.

## Self-Check: PASSED

- Files exist: crates/ostro-ui/src/inventory.rs, crates/ostro-ui/tests/inventory.rs, the six page templates, the five partials and the seven inventory snapshots.
- Commit 4c24d14 is on main.
