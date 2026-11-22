---
phase: 11
slug: run-operations-embedded-ui
status: complete
nyquist_compliant: true
wave_0_complete: true
created: 2026-11-19
---

# Phase 11: Validation Strategy

> This is the per-phase validation contract for feedback sampling during execution.
> Source: 11-RESEARCH.md § Validation Architecture, mapped onto plans 11-01..11-18. Adopted decisions: 11-DECISIONS.md (D-P11-1..D-P11-34). Ordinals: RESERVATIONS.md P11 block at de4e303 (Cmd 45 DlqUpdate).

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Rust built-in test harness (`cargo test`, tokio tests), insta 1 (HTML and OpenAPI snapshots), proptest (`run_index`, `rebuild_digest`), `fail` failpoints (`rerun::`, `dlq::`, `bulk::` + Phase 5 prefixes; `--features failpoints`, single-threaded), tower `ServiceExt::oneshot` for in-process UI/API tests against `FakeControlPlane`/`FakeOps`/`FakeInventory`, spawned `ostro` binary for e2e (and the failpoint binary from `--target-dir target/failpoints` for kill tests), Phase 6 `MockProvider` for HTTP effects. |
| **UI stack under test** | askama `=0.16.1` (or maud 0.27.0 per the 11-03 decision), htmx 2.0.11 vendored (sha256 in VENDORED.md, enforced by `ostro-ui/tests/vendored.rs`). No Node, no headless browser. |
| **Config** | `scripts/ci-local.sh` is the gate. `FAILPOINT_CRATES` must equal the ci.yml "Failpoint suites" list (ostro-server added by 11-04 if absent). New test guests `p11-limits`, `p11-rerun`, `p11-rerun-v2`, `p11-hook` build in the existing guests step. |
| **External services** | None. MockProvider is in-process. **The microk8s cluster reachable via kubectl on this machine is never used.** |
| **Quick run command** | `cargo test -p <crate> --test <suite> --locked` (each < 30 s except e2e) · `cargo test -p ostro-ui --locked` · `cargo clippy -p <crate> --all-targets --locked -- -D warnings` |
| **Full suite command** | `bash scripts/ci-local.sh --no-musl` (intermediate waves) |
| **Phase gate commands** | 1. `bash scripts/ci-local.sh` (with musl).<br>2. `cargo test -p ostro --test ops_e2e --test ui_e2e --locked`.<br>3. `cargo build -p ostro --features failpoints --target-dir target/failpoints --locked && cargo test -p ostro --features failpoints --test ops_e2e --locked -- --test-threads=1 kill`.<br>4. `PROPTEST_CASES=1000 cargo test -p ostro-partition --test run_index --locked`.<br>5. `cargo test -p ostro-engine --test replay_corpus --locked`.<br>6. `cargo deny check`; musl binary size recorded vs NFR-2.1. |
| **Estimated runtime** | Quick: < 60 s per command. Full ci-local --no-musl: ~15–20 min debug. Gate with musl: ~25–35 min. |

---

## Sampling Rate

- **After every task commit:** the task's `<automated>` command (each requires a nonzero passed count) plus `cargo clippy -p <crate> --all-targets --locked -- -D warnings`.
- **After every plan wave:** `cargo test --workspace --locked`, the failpoint suites of touched crates, `cargo deny check`.
- **Before `/gsd:verify-work`:** the phase gate commands above, green.
- **Max feedback latency:** 60 s for task-level commands (e2e and failpoint suites excepted).

---

## Wave Structure

| Wave | Plans | Notes |
|------|-------|-------|
| 1 | 11-01 | Gate (Phase 7 AND Phase 8 complete; Phase 5 freeze), names, decisions to STATE. Docs only. |
| 2 | 11-02, 11-03 | Contract a: ordinals + every struct-literal fix-up · Contract b: Cargo/ostro-ui/htmx (exclusive with 10-02). Disjoint files. |
| 3 | 11-04, 11-05, 11-08 | Contract c: API contract, config, final p11 seam, stubs, ci · derived index · DLQ apply + storage-format doc. |
| 4 | 11-06, 11-09 | Engine limits · API surface (exclusive with 10-05). |
| 5 | 11-07, 11-11, 11-12, 11-15 | Rerun apply/analyzer/re-arm (after 11-06: shared executor.rs) · search/inventory/limits server · DLQ server + on_failure · UI foundation. |
| 6 | 11-10, 11-16 | Rerun control plane (+ replay_check if absent) · UI actions. |
| 7 | 11-13, 11-14, 11-17 | Bulk ops · CLI verbs (exclusive with 10-09) · UI inventory views. |
| 8 | 11-18 | E2E + PHASE GATE. |


---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 11-01-01 | 01 | 1 | all | gate | `ls 07-*/08-* SUMMARY + VERIFICATION status; freeze greps; seam greps; ci-local --no-musl` | ✅ | ✅ green |
| 11-01-02 | 01 | 1 | all | doc | `grep D-P11-34 / Phase 13 obligation / USER REVIEW / HUMAN in STATE.md` | ✅ | ✅ green |
| 11-02-01 | 02 | 2 | OPS-02/04/07, UI-01 | unit/golden + build | `cargo build --workspace --all-targets --locked`; `cargo test -p ostro-types --test command --locked`; golden diff add-only (`git diff -U0 HEAD`) | ✅ (goldens) | ✅ green |
| 11-02-02 | 02 | 2 | OPS-02/04/07 | regression | `cargo test -p ostro-partition --test rebuild_digest --locked`; `cargo test -p ostro-types --test command --locked` | ✅ | ✅ green |
| 11-03-01 | 03 | 2 | UI-01..03 | deps | `cargo deny check`; `cargo metadata --locked`; banned-crate check on `cargo tree -p ostro-ui` | ✅ | ✅ green |
| 11-03-02 | 03 | 2 | UI-01 | unit | `cargo test -p ostro-ui --locked` (vendored + skeleton) | ✅ | ✅ green |
| 11-04-01 | 04 | 3 | all | build/unit | `cargo build --workspace --all-targets --locked`; `cargo test -p ostro-api --locked` | ✅ | ✅ green |
| 11-04-02 | 04 | 3 | all | integration | `cargo test -p ostro-server --test p11_contract --locked`; FAILPOINT_CRATES check; `ci-local --no-musl` | ✅ | ✅ green |
| 11-05-01 | 05 | 3 | OPS-05 | integration | `cargo test -p ostro-partition --test run_index --locked`; `--test rebuild_digest` | ✅ | ✅ green |
| 11-05-02 | 05 | 3 | OPS-05 | proptest | `cargo test -p ostro-partition --test run_index --test snapshot --locked` | ✅ | ✅ green |
| 11-06-01 | 06 | 4 | OPS-07 | integration | `cargo test -p ostro-engine --test run_limits --locked` | ✅ | ✅ green |
| 11-06-02 | 06 | 4 | OPS-07 | integration | `cargo test -p ostro-server --test limits_run --locked`; `cargo test -p ostro-worker-proto --test wire --locked` | ✅ | ✅ green |
| 11-07-01 | 07 | 5 | OPS-02 | integration | `cargo test -p ostro-partition --test apply_rerun --locked` | ✅ | ✅ green |
| 11-07-02 | 07 | 5 | OPS-02 | unit | `cargo test -p ostro-server --lib --locked -- rerun::`; guests build | ✅ | ✅ green |
| 11-07-03 | 07 | 5 | OPS-02, UI-01 | integration + failpoint | `cargo test -p ostro-server --test rerun_rearm --locked`; same with `--features failpoints -- --test-threads=1` | ✅ | ✅ green |
| 11-08-01 | 08 | 3 | OPS-04 | integration/proptest | `cargo test -p ostro-partition --test apply_dlq --test rebuild_digest --locked` | ✅ | ✅ green |
| 11-08-02 | 08 | 3 | OPS-04 | doc | `grep "Phase 11 additions" docs/storage-format.md`; add-only diff | ✅ | ✅ green |
| 11-09-01 | 09 | 4 | OPS-02..07, UI-03 | integration (fake) | `cargo test -p ostro-api --test phase11_surface --locked` | ✅ | ✅ green |
| 11-09-02 | 09 | 4 | OPS-02..07, UI-03 | parity/snapshot | `cargo test -p ostro-api --test parity --test openapi --locked` | ✅ | ✅ green |
| 11-10-01 | 10 | 6 | OPS-02 | integration | `cargo test -p ostro-engine --test replay_corpus --locked` (library form) | ✅ | ✅ green |
| 11-10-02 | 10 | 6 | OPS-02 | integration | `cargo test -p ostro-server --test rerun_api --test dlq_api --locked` | ✅ | ✅ green |
| 11-11-01 | 11 | 5 | OPS-05 | integration | `cargo test -p ostro-server --test search_api --locked` | ✅ | ✅ green |
| 11-11-02 | 11 | 5 | OPS-07, UI-03 | integration | `cargo test -p ostro-server --test inventory_api --test limits_api --test limits_doc --locked` | ✅ | ✅ green |
| 11-12-01 | 12 | 5 | OPS-04 | integration | `cargo test -p ostro-server --test dlq_api --locked -- view requeue dismiss policy` | ✅ | ✅ green |
| 11-12-02 | 12 | 5 | OPS-04 | integration + failpoint | `cargo test -p ostro-server --test dlq_api --locked`; `--features failpoints -- --test-threads=1 hook` | ✅ | ✅ green |
| 11-13-01 | 13 | 7 | OPS-03 | integration (paused time) | `cargo test -p ostro-server --test bulk_ops --locked` | ✅ | ✅ green |
| 11-13-02 | 13 | 7 | OPS-03 | failpoint | `cargo test -p ostro-server --features failpoints --test bulk_ops --locked -- --test-threads=1 crash` | ✅ | ✅ green |
| 11-14-01 | 14 | 7 | OPS-02/04/05/07 | e2e snapshot | `cargo test -p ostro --test cli_p11 --locked -- rerun dlq limits ls`; `--test cli_verbs` unchanged | ✅ | ✅ green |
| 11-14-02 | 14 | 7 | OPS-03 | e2e snapshot (fake server) | `cargo test -p ostro --test cli_p11 --locked` | ✅ | ✅ green |
| 11-15-01 | 15 | 5 | UI-01 | unit/integration | `cargo test -p ostro-ui --test hardening --test security --locked` | ✅ | ✅ green |
| 11-15-02 | 15 | 5 | UI-01 | snapshot | `cargo test -p ostro-ui --test pages --locked` | ✅ | ✅ green |
| 11-16-01 | 16 | 6 | UI-02 | integration (fake) | `cargo test -p ostro-ui --test actions --locked -- lifecycle dlq scope` | ✅ | ✅ green |
| 11-16-02 | 16 | 6 | UI-02 | integration/snapshot | `cargo test -p ostro-ui --test actions --locked` | ✅ | ✅ green |
| 11-17-01 | 17 | 7 | UI-03 | snapshot | `cargo test -p ostro-ui --test inventory --locked -- modules workflows partitions workers nav` | ✅ | ✅ green |
| 11-17-02 | 17 | 7 | UI-03, UI-02 | integration | `cargo test -p ostro-ui --test inventory --locked` | ✅ | ✅ green |
| 11-18-01 | 18 | 8 | all | e2e + failpoint | `cargo test -p ostro --test ops_e2e --test ui_e2e --locked`; kill tests; `replay_corpus` | ✅ | ✅ green |
| 11-18-02 | 18 | 8 | all | gate | `bash scripts/ci-local.sh` (musl); `cargo deny check`; size record | ✅ | ✅ green |
| 11-18-03 | 18 | 8 | all | doc | RESERVATIONS flip / VALIDATION / DECISIONS greps | ✅ | ✅ green |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Success Criteria → Evidence

| SC | Criterion (ROADMAP) | Evidence | Plans |
|----|---------------------|----------|-------|
| SC1 | Failed run → DLQ with reason; optional `on_failure` fires; requeue; rerun from a chosen step copies the prefix and skips completed steps | `dlq_api` (view, requeue, dismiss, hook exactly-once incl. failpoint, loop guard), `rerun_api` (fix-and-rerun, prefix bytes identical, no re-execution, nondeterminism refusal, unsafe cuts), `apply_rerun`, `rerun_rearm`, `ops_e2e` SC1 — **Spawned binary (11-18):** `ops_e2e::sc1_dlq_on_failure_requeue_and_fixed_rerun` (DLQ reason `workflow_failed: s3 fails`; one hook run tagged `__ostro/on_failure_of=`, input as documented, still one after 2 s; `dlq requeue --mode restart` new linked run, source leaves the view; `rerun --from-step s3` on v2 completes 5 with one `step_completed` per step and prefix kind/details identical) ok; kill tests `ops_kill::kill_hook_after_start`, `kill_rerun_before_rearm` ok. **Server:** `dlq_api` 11, `rerun_api` 5, `rerun_rearm` 3 (failpoints), `apply_rerun`, `apply_dlq` 7. | 11-07, 11-08, 11-10, 11-12, 11-18 |
| SC2 | Bulk cancel/retry/rerun over a tag/status filter, spread over time without overloading; bounded indexed tag search | `bulk_ops` (pacing under paused time, backpressure, 429, snapshot bound = exactly N reruns, crash resume), `run_index` proptest, `search_api`, `ops_e2e` SC2 — **Spawned binary (11-18):** `ops_e2e::sc2_bulk_retry_and_rerun_paced_bounded_and_searchable` (60 failed runs tagged `batch`: `ostro bulk retry --rate 20` matched 60, exactly 60 restarts; `ostro bulk rerun --from-step s3 --rate 20` matched 120 = the snapshot, exactly 120 reruns, all completed on v2; ops after the burst span ≥ 0.9 × (k-1)/rate (2450 ms for 50, 5451 ms for 110); `tags=batch&order=newest&limit=25`: 10 pages ≤ 25, newest first, no duplicates) ok; `ops_kill::kill_bulk_after_op` (stall at 26 ops, checkpoint 20, kill -9, resume: 60 cancelled, `cancel_requested` once each) ok. **Server:** `bulk_ops` 13 (failpoints), `search_api`, `run_index`. | 11-05, 11-11, 11-13, 11-18 |
| SC3 | Exceeding a published limit (payload, history, children) → clear, documented error | `run_limits` (engine, incl. replay of over-limit journals), `limits_run` (resumable limit_exceeded + raise/retry, worker path), `limits_api` (413/400 details), `limits_doc` (doc == endpoint == constants), `ops_e2e` SC3 — **Spawned binary (11-18):** `ops_e2e::sc3_limits_errors_resumable_failures_and_published_table` (`ostro server` with `[limits]` 20 / 3 / 64KiB: `limits-history(30)` failed `limit_exceeded: history_events 21 > 20`, `retry --dry-run` = resumed, journal 20 events; `limits-children(4)` failed `limit_exceeded: children…` (children bridged at HEAD); 100 KiB input 413 `{reason: limit_exceeded, limit: payload_bytes, max: 65536}`; rerun from seq 25 413 `rerun_prefix_too_large` `limit: history_events`; restart with defaults: `ostro retry` and `ostro dlq requeue --mode resume` complete with 32 events, children run completes; `GET /v1/limits` == docs/limits.md rows) ok. **Server/engine:** `run_limits` 6, `limits_run` 2, `limits_api`, `limits_doc` 2. | 11-06, 11-09, 11-11, 11-18 |
| SC4 | Embedded UI from the binary: run list with filters/tags, timeline with replay markers, published status; modules, versions, queues, schedules, workers, partition health | `ostro-ui` `pages` (snapshots, escaping, exact/approximate markers), `inventory`, `hardening` (CSP, lint, assets), `ui_e2e` pages over HTTP on real runs — **Spawned binary (11-18):** `ui_e2e::ui_pages_and_actions_change_real_state` (`/` 303 `/ui/`; `/ui/`, runs (+filter), run detail, rerun form, DLQ, bulk, modules (sha), workflows, queues, schedules, workers, partitions, timeline/logs/status/partition fragments, htmx asset: 200 with the exact D-P11-15 CSP, nosniff, DENY, no-referrer, real ids) and `ui_inventory_views_on_a_live_server_with_a_worker` (TLS `ostro server` + real `ostro worker` + cron schedule: modules sha, workflows, queues, schedule name + cron, worker id, ≥ 4 healthy partitions) ok. **UI crate:** `pages`, `inventory`, `hardening`, `security` green. Real browser: Manual-Only below. | 11-15, 11-17, 11-18 |
| SC5 | From the UI: cancel, terminate, retry, rerun-from-step, pause/resume, DLQ requeue, bulk | `ostro-ui` `actions` (trait calls, fragment vs 303, refusal rendering, operate scope), `security` (CSRF/Host negatives), `ui_e2e` actions over HTTP changing real state — **Spawned binary (11-18):** `ui_e2e::ui_pages_and_actions_change_real_state`: htmx (`HX-Request` + `X-Ostro-Csrf`) or no-JS form (`csrf` field) same-origin posts terminate (→ terminated), cancel (→ cancelled, 303), pause + resume a run (pause mark set/cleared) and the `default` queue (paused flag), retry (HX-Redirect to the restart), rerun preview (verdict ok, cut seq 3, pipeline@2, no run created) + submit (v2 completes), DLQ requeue restart (source leaves the view), bulk preview (no job) + create (4 runs cancelled) + cancel of a slow job (state cancelled, runs left alone), schedule pause/resume on the TLS server; cross-site post 403, `Host: evil.example` 403, tokenless post 403, state unchanged. **UI crate:** `actions`, `security` green. | 11-16, 11-17, 11-18 |

---

## Wave 0 Requirements

- [x] `crates/ostro-ui/` crate skeleton (`publish = false`), template engine pin, vendored htmx + `tests/vendored.rs` — 11-03
- [x] `crates/ostro-server/tests/common/p11.rs` (`P11Server`, REST helpers) + `tests/p11_contract.rs` — 11-04
- [x] Compiling stubs for every new module (apply/{index,dlq,rerun} + engine run_limits — 11-02; server p11/rerun/onfailure, control/p11_*, services/{dlq_hook,bulk} — 11-04)
- [x] Struct-literal fix-ups for the new fields — 11-02 (`JournalEvent`/`StartRun`/`EngineConfig`), 11-04 (`ApiConfig`/`ApiExtensions`)
- [x] `FAILPOINT_CRATES` includes ostro-server in ci-local.sh and ci.yml — 11-04 (if absent)
- [x] Test files created by their owning plans: `ostro-partition/tests/{run_index,apply_dlq,apply_rerun}.rs`; `ostro-engine/tests/run_limits.rs`; `ostro-server/tests/{limits_run,rerun_rearm,search_api,inventory_api,limits_api,limits_doc,dlq_api,rerun_api,bulk_ops}.rs`; `ostro-api/tests/phase11_surface.rs`; `ostro-ui/tests/{hardening,security,pages,actions,inventory}.rs`; `ostro/tests/{cli_p11,ops_e2e,ui_e2e}.rs`
- [x] Guests `p11-limits` (11-06), `p11-rerun` + `p11-rerun-v2` (11-07), `p11-hook` (11-12)

---

## Manual-Only / Human-Needed

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Real-browser render of every `/ui` page (Firefox and Chromium, current stable) | UI-01, UI-03 | No browser automation in Phase 11 (Playwright needs Node; out of scope) | `ostro dev`, open `http://127.0.0.1:<port>/ui/`; visit runs, run detail, timeline, logs, DLQ, bulk, modules, workflows, queues, schedules, workers, partitions; layout intact, no broken assets. |
| No CSP violations in the browser console | UI-01..03 | Console inspection | DevTools console open on each page and during each action: zero CSP errors (htmx `allowEval:false`, no inline script/style). |
| htmx swaps, polling and confirm dialogs | UI-02 | Interactive | Start a long run: status fragment updates every 2 s and stops at terminal; terminate shows a confirm dialog, then swaps the status fragment; bulk progress polls and stops at done. |
| No-JS fallback | UI-02 | Needs a browser with JS disabled | Disable JavaScript; perform terminate, retry, DLQ dismiss and a bulk dry run via plain forms; each ends on the expected page (303 flow). |
| Keyboard navigation and focus | UI-01..03 | Interactive | Tab through the run list filters, action buttons and forms; every control reachable and visibly focused; Enter submits forms. |
| CSRF / DNS-rebinding from a real foreign origin | UI-02 | Needs a second origin in a browser | Serve a page from another origin (e.g. `python3 -m http.server` on another port) that auto-submits a form POST to `/ui/runs/<id>/terminate`: the server answers 403 and the run is unaffected. |
| htmx specifics from 11-16 (actions) | UI-02 | Interactive swaps | On a run detail: action buttons swap via OOB (`hx-swap-oob`) after cancel/terminate/pause/resume; on `/ui/dlq` a dismiss removes the row (`hx-swap="delete"`); rerun submit, DLQ requeue and bulk create navigate via `HX-Redirect`; refusals (e.g. a 409 `rerun_cut_unsafe`) render in the retargeted `#flash`; every destructive button shows its `hx-confirm` dialog; the bulk job page polls progress every 2 s and stops at a terminal state. |
| htmx specifics from 11-17 (inventory) | UI-02, UI-03 | Interactive swaps | Queue/workflow/schedule pause-resume and worker drain replace only their row (`hx-target="closest tr"` with a `<tr>` response); drain and schedule trigger show `hx-confirm`; `/ui/partitions` refreshes every 5 s; with JS disabled each inventory form posts and lands back on its page (303). Cosmetic, known: the "Bulk" nav link is not `aria-current` on bulk pages. |
| UI through a non-loopback hostname | UI-01 | Needs a real hostname / proxy | `ostro server` on `0.0.0.0` with `[ui] enabled = true` and `[ui] allowed_hosts = ["<host>"]`: pages load via `http://<host>:<port>/ui/`, an unlisted Host is 403, the startup WARN names Phase 14. |
| User review of D-P11-3/8, D-P11-13, D-P11-14, D-P11-17 | — | Product decisions | See STATE.md Pending Todos `[USER REVIEW]` items. |
| Remote CI legs (aarch64/macOS) for the new suites | — | No git remote yet (FOUND-05) | After the first push, confirm the Phase 11 suites pass on every CI leg. |

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers all MISSING references
- [x] No watch-mode flags
- [x] Feedback latency < 60 s at task level
- [x] `nyquist_compliant: true` set in frontmatter

**Gate deviations (11-18):** the kill tests live in `crates/ostro/tests/ops_kill.rs` (feature `failpoints`, FAILPOINT_CRATES `ostro:ops_kill`) instead of a `kill` filter inside `ops_e2e`, and use the binary that `cargo test -p ostro --features failpoints` builds (no separate `target/failpoints` build); the gate ran `ci-local.sh --no-musl-test` (musl release built and checked, musl test leg skipped, per the coordinator's `--no-musl` instruction); `PROPTEST_CASES=1000 run_index` was not re-run at the gate (default-case proptests ran in `cargo test` and the failpoint suites).

**Approval:** automated gate green (11-18); browser checks human_needed (Manual-Only).

**Gate (11-18):** `bash scripts/ci-local.sh --no-musl-test` at b460ad7 (2026-11-22 16:04-19:03 UTC, 2 h 58 min on a shared machine): all steps passed (cargo test 4261 s, failpoint suites 4530 s incl. the four Phase 11 entries, server campaign smoke 669 s, polyglot conformance 346 s, musl release static/mimalloc 513 s); musl zigbuild test skipped (--no-musl-test), external PG/MySQL/S3 legs SKIP (no .ostro-test-env). musl release binary 53,113,136 B (50.7 MiB) < 60 MB (NFR-2.1). Strict CLOCK_GATE: 33 pre-existing hits, none from Phase 11 (Phase 10 sweep pending; follow-up in STATE).
