---
phase: 11-run-operations-embedded-ui
verified: 2026-11-22T20:30:00Z
status: human_needed
score: 5/5 success criteria verified (8/8 requirements satisfied)
re_verification: null
gaps: []
follow_ups:
  - item: "Strict CLOCK_GATE red: 33 unannotated time reads"
    owner: "Phase 10 (10-06 clock-exempt sweep)"
    blocks_phase_11: false
    evidence: "All 33 hits are in files from Phases 2/4/9. git blame on the 15 hits in files Phase 11 touched (executor.rs, server.rs, sink.rs, node.rs) gives 04-16, 04-17, 09-08 and 02-12 commits. Phase 11 added none."
  - item: "API fields with no ostro.api.v1 field numbers: RunInfo.retry_of, RunInfo.status_seq, RunPage order-fallback flag / X-Ostro-Order-Fallback header"
    owner: "a future API-proto reservations block"
    blocks_phase_11: false
    evidence: "None of these fields appear in the ostro-api REST/gRPC surface. Retry lineage is in the retry response (new run id) and the DLQ requeued_to. Clients detect the index-rebuild fallback from c1. page tokens (docs/operations.md:224-230). No SC asserts these fields."
  - item: "MAX_SEARCH_TAGS (8) and the [bulk] caps (max_jobs 4, max_matches 100000, max_rate 500) are missing from the docs/limits.md table and GET /v1/limits"
    owner: "OPS-07 doc-table follow-up"
    blocks_phase_11: false
    evidence: "Both are documented with their errors in docs/operations.md (line 218: 400 details.limit=search_tags; lines 187-198 and 264-265: 429 bulk_too_many_jobs with retry_after_ms, 400 bulk_too_many_matches). Enforcement returns a structured limit_error (ostro-api/src/phase11.rs:894-901). SC3 holds: exceeding them gives a clear, documented error. Only the single canonical table is incomplete."
  - item: "Leader-only coordination: dlq_hook::spawn runs on every slot and bulk jobs are node-local"
    owner: "Phase 12/13"
    blocks_phase_11: false
    evidence: "Correct for the single-node topology Phase 11 ships. Recorded in STATE Blockers and 11-DECISIONS. It becomes an obligation once followers exist."
  - item: "Cosmetic: `ostro bulk retry --help` says 'Retry (resume)', but a guest failure is restarted; the 'Bulk' nav link is not aria-current"
    owner: "polish"
    blocks_phase_11: false
human_verification:
  - test: "Real-browser render of every /ui page in Firefox and Chromium (runs, run detail, timeline, logs, DLQ, bulk, modules, workflows, queues, schedules, workers, partitions)"
    expected: "Layout intact, no broken assets"
    why_human: "By design there is no browser automation in Phase 11 (no Node/Playwright)"
  - test: "DevTools console open on every page and during every action"
    expected: "Zero CSP violations"
    why_human: "Needs a browser console"
  - test: "htmx behaviour: 2 s run-status poll stops at terminal; OOB action buttons; hx-swap=delete on DLQ rows; HX-Redirect; retargeted #flash on refusals; 2 s bulk progress poll stops at done; 5 s partition poll; inventory rows swap via closest tr"
    expected: "Each swap and poll behaves as described in 11-VALIDATION Manual-Only"
    why_human: "Client-side behaviour; the HTTP responses are covered by automated tests"
  - test: "hx-confirm dialogs on terminate, other destructive actions, worker drain and schedule trigger"
    expected: "A confirm dialog appears before the request"
    why_human: "Interactive"
  - test: "With JavaScript disabled, run the action and inventory forms"
    expected: "Each posts and lands back on its page via 303"
    why_human: "Needs a browser with JS disabled"
  - test: "Keyboard navigation"
    expected: "Every control can be reached with Tab and shows visible focus; Enter submits"
    why_human: "Interactive / accessibility"
  - test: "CSRF / DNS-rebinding from a real foreign origin that auto-submits a POST to /ui/runs/<id>/terminate"
    expected: "403 and the run is unaffected"
    why_human: "Needs a second browser origin. The cross-site, foreign-Host and tokenless cases are already automated in ui_e2e"
  - test: "UI through a non-loopback hostname with [ui] allowed_hosts"
    expected: "Pages load, an unlisted Host gets 403, and the startup WARN names Phase 14"
    why_human: "Needs a real hostname or proxy"
  - test: "Remote CI legs (aarch64/macOS) for the Phase 11 suites after the first push"
    expected: "Green"
    why_human: "No git remote yet (FOUND-05)"
  - test: "User review of D-P11-3/8 (rerun semantics), D-P11-13 (limit defaults), D-P11-14 (askama/htmx), D-P11-17 + amendment (UI exposure; dev UI loopback-only)"
    expected: "Accepted or overridden"
    why_human: "Product decisions"
---

# Phase 11: Run Operations & Embedded UI Verification Report

**Phase Goal:** Operators triage and repair runs from the CLI/API, or from an embedded UI that has real actions, within documented and enforced limits.
**Verified:** 2026-11-22T20:30:00Z (against main @ 11ced2a)
**Status:** human_needed. All automated checks pass. The only open items are real-browser checks (by design) and user-review decisions.
**Re-verification:** No. This is the initial verification.

## Goal Achievement

### Observable Truths (ROADMAP Success Criteria)

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | A failed run lands in the DLQ with a reason and triggers `on_failure`. It can be requeued or rerun from a step, with the journal prefix copied and completed steps skipped. | VERIFIED | `crates/ostro/tests/ops_e2e.rs::sc1_dlq_on_failure_requeue_and_fixed_rerun` uses the spawned binary (common/mod.rs runs `CARGO_BIN_EXE_ostro`; no in-process server). Its assertions: the exact reason `workflow_failed: s3 fails`; the hook run is tagged `__ostro/on_failure_of=<src>` with the documented input; there is exactly 1 hook after a 2 s grace; `dlq requeue --mode restart` creates a new run with tags copied, the source leaves the default view and `requeued_to` is set; after deploying v2, `rerun --from-step s3` completes with result 5; there is exactly one `step_completed` per step s1..s5; prefix events 1..cut match the source's seq, kind and details; the `__ostro/rerun_from=<cut>` tag is present. The kill tests `ops_kill::kill_rerun_before_rearm` and `kill_hook_after_start` (exactly one hook after kill -9) cover crash safety. |
| 2 | Bulk cancel/retry/rerun over a tag or status filter is rate-spread, and tag search is bounded and indexed. | VERIFIED | `ops_e2e::sc2_bulk_retry_and_rerun_paced_bounded_and_searchable`: `ostro bulk retry` and `bulk rerun --rate 20` run over a tag. `assert_paced` checks the span after the burst against (k-1)/rate. The snapshot bound is asserted (rerun matched == done == the snapshot, exactly one rerun per matched run, lineage tags). Paging `GET /v1/runs?tags=batch&order=newest&limit=25` gives pages ≤ 25, newest first, no duplicates and complete. The `ostro ls --tag --newest` CLI form is also covered. The index is `x_run_index` (ostro-partition apply/index.rs, store/derived.rs), searched in control/p11_search.rs with a scan fallback. `kill_bulk_after_op` resumes with no duplicate cancels. |
| 3 | Exceeding a published limit (payload, history, children) produces a clear, documented error. | VERIFIED | `ops_e2e::sc3_limits_errors_resumable_failures_and_published_table`, run on `ostro server` with lowered `[limits]`. History fails with `limit_exceeded: history_events 21 > 20`, is resumable and the journal stops at 20. Children fail with `limit_exceeded: children…`. A 100 KiB input returns 413 with `details{reason: limit_exceeded, limit: payload_bytes, max: 65536, actual}`. A rerun prefix over the limit returns 413 `rerun_prefix_too_large` (CLI and REST). After the defaults are restored, retry and requeue resume to completion. `GET /v1/limits` equals the `docs/limits.md` rows. MAX_SEARCH_TAGS and the bulk caps are outside the canonical table but are documented with their errors in docs/operations.md (see Follow-ups). |
| 4 | The UI is served from the binary and shows the run list with filters/tags, a timeline with replay markers, published status, and modules, versions, queues, schedules, workers and partition health. | VERIFIED (automated HTTP), browser rendering human_needed | `ostro-server/src/p11.rs:244` merges `ostro_ui::router`. Assets are `include_bytes!` in ostro-ui/src/assets.rs (vendored htmx 2.0.11). `ui_e2e::ui_pages_and_actions_change_real_state` returns 200 with the exact CSP and security headers on 14 pages, 4 fragments and the asset, with real ids. `ui_inventory_views_on_a_live_server_with_a_worker` uses TLS server + a real worker + a cron schedule, and shows modules (sha), workflows, queues, schedules, workers and ≥ 4 healthy partitions. Replay markers come from ostro-ui/src/timeline.rs (exact from `JournalEvent.attempt`, approximate for older journals) and are snapshot-tested in the ostro-ui `pages` suite. The spawned-binary ui_e2e checks timeline steps but not the marker text (info). |
| 5 | From the UI an operator can cancel, terminate, retry, rerun-from-step, pause/resume, requeue from the DLQ and run bulk operations. | VERIFIED (automated HTTP), interactive htmx behaviour human_needed | `ui_e2e` checks that each action changes real server state: terminate leads to `terminated`, the cancel form gives 303 and `cancelled`, pause/resume of a run is observed in `pause`, pause/resume of the `default` queue is observed in `/v1/queues`, retry redirects to a new run and the count goes +1, rerun preview shows Verdict ok / Cut seq 3 / pipeline@2 and creates no run, rerun submit completes on v2 with one s1 completion, DLQ requeue makes the source leave `/v1/dlq`, bulk preview creates no job, bulk create cancels 4 runs, a slow bulk job can be cancelled, and schedule pause/resume works on TLS. Cross-site, `Host: evil.example` and tokenless posts get 403 with state unchanged. |

**Score:** 5/5 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `crates/ostro/tests/ops_e2e.rs` | SC1–SC3 spawned-binary e2e | VERIFIED | 526 lines, 3 tests, meaningful exact assertions (spot-read in full) |
| `crates/ostro/tests/ui_e2e.rs` | SC4–SC5 spawned-binary e2e | VERIFIED | 458 lines, 2 tests; state change asserted after each action |
| `crates/ostro/tests/ops_kill.rs` | kill -9 crash tests (rerun re-arm, hook exactly once, bulk resume) | VERIFIED | 228 lines, 3 tests, in the FAILPOINT_CRATES/ci failpoint step |
| `crates/ostro-ui/src/*` | UI crate: pages, timeline, actions, inventory, security, assets, auth seam | VERIFIED | 4,575 lines; no TODO/unimplemented markers |
| `crates/ostro-server/src/services/{bulk,dlq_hook}.rs` | bulk engine, on_failure hook service | VERIFIED | 1,699 and 504 lines |
| `crates/ostro-server/src/control/p11_{rerun,dlq,bulk,search,inventory}.rs` | control-plane ops | VERIFIED | present and wired via p11.rs |
| `x_run_index` (ostro-partition apply/index.rs, store/derived.rs, query.rs) | derived run index | VERIFIED | used by control/p11_search.rs |
| `docs/limits.md`, `docs/operations.md`, `docs/ui.md` | published limits and operator guides | VERIFIED | limits.md is kept equal to `/v1/limits` by the SC3 test and limits_doc |

### Key Link Verification

| From | To | Via | Status |
|------|----|-----|--------|
| ostro binary | ostro-ui router | `ostro_server::p11` merges `ostro_ui::router(UiDeps{..})` | WIRED |
| ostro-ui actions | control plane | same OpsApi/ControlPlane as REST; ui_e2e observes state through `/v1/*` | WIRED |
| run failure | DLQ + hook | DlqUpdate (Cmd 45) apply, then `dlq_hook` service, then hook run tagged `__ostro/on_failure_of` | WIRED (SC1 and kill test) |
| `/v1/runs?tags=` | `x_run_index` | p11_search, with scan fallback | WIRED (SC2) |
| `[limits]` config | engine + API | EngineLimits / Welcome.limits; API 413 limit_error | WIRED (SC3 on server config) |
| `/v1/limits` | docs/limits.md | equality test in SC3 and limits_doc | WIRED |

### Requirements Coverage

| Requirement | Description | Status | Evidence |
|-------------|-------------|--------|----------|
| OPS-02 | Rerun from a chosen step | SATISFIED | SC1 rerun, kill_rerun_before_rearm, UI rerun |
| OPS-03 | Bulk cancel/retry/rerun, rate-spread | SATISFIED | SC2, kill_bulk_after_op, UI bulk |
| OPS-04 | DLQ with reason, on_failure hook, requeue | SATISFIED | SC1, SC3 requeue resume, kill_hook_after_start, UI requeue |
| OPS-05 | Tags with bounded indexed search | SATISFIED | SC2 paging; x_run_index |
| OPS-07 | Published limits documented and enforced with clear errors | SATISFIED (follow-up: table completeness) | SC3; MAX_SEARCH_TAGS/bulk caps documented in operations.md only |
| UI-01 | Embedded UI: run list, filter, tags, timeline with replay markers, published status | SATISFIED (browser human_needed) | ui_e2e pages; ostro-ui pages snapshots |
| UI-02 | UI actions | SATISFIED (browser human_needed) | ui_e2e actions |
| UI-03 | UI inventory views | SATISFIED (browser human_needed) | ui_inventory_views_on_a_live_server_with_a_worker |

There are no orphaned requirements. All eight IDs mapped to Phase 11 in REQUIREMENTS.md are claimed by plans and ticked with evidence.

### Classification of open items

1. **Strict CLOCK_GATE (33 hits): out-of-scope follow-up, not a Phase 11 gap.** I re-ran the exact strict command at 11ced2a and got 33 hits in 13 files. Of the four hit files Phase 11 touched, `git blame` on every hit line gives pre-Phase-11 commits (02-12, 04-16, 04-17, 09-08). The other nine files have no Phase 11 commits. 11-DECISIONS assigns the annotation sweep to Phase 10's 10-06, which has not run yet. Phase 11's own scoped clock gate (CLOCK_GATE_SCOPED, lines added per plan) passed. The strict gate should be re-run in the Phase 10 gate (10-20).
2. **API fields without proto numbers: do not block any SC.** RunInfo.retry_of/status_seq and the order-fallback flag are not exposed anywhere in ostro-api. The behaviour they would describe is already observable: the retry response carries the new run id, the DLQ carries `requeued_to`, rerun lineage is the `__ostro/rerun_from` tag, and the fallback shows as `c1.` tokens, with `i1.` tokens getting 503 `index_rebuilding`. Field numbers should be assigned when an API-proto reservations block exists.
3. **Unpublished limits: SC3 holds.** SC3 requires that exceeding a published limit gives a clear, documented error. MAX_SEARCH_TAGS and the bulk caps are published in docs/operations.md, with their status codes and `details`, and enforced with structured errors. They are missing only from the canonical `docs/limits.md` table and `/v1/limits`, which the limits.md preamble says should list "every limit". That is a documentation consistency follow-up for OPS-07, not a case of undefined behaviour.
4. **Leader-only dlq_hook and node-local bulk: a Phase 12/13 obligation.** It is correct on the single-node topology Phase 11 delivers. It is recorded in STATE Blockers and 11-DECISIONS.
5. **Real-browser checks: human_needed by design** (11-VALIDATION Manual-Only; no Node in Phase 11).

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| ostro-ui/src, services/{bulk,dlq_hook}.rs, p11.rs | — | TODO/FIXME/unimplemented scan | none found | — |
| crates/ostro CLI (`bulk retry --help`) | — | help says "Retry (resume)", but guest failures restart | Info | cosmetic |
| ostro-ui nav | — | "Bulk" link not aria-current on bulk pages | Info | cosmetic |
| docs/limits.md | table | MAX_SEARCH_TAGS and bulk caps not in the canonical table or /v1/limits | Warning | doc consistency (OPS-07 follow-up) |

### Test evidence

- 11-18 gate: `ci-local.sh --no-musl-test` at b460ad7, all steps green (2 h 58 min), including the four Phase 11 failpoint entries. The musl release binary is 50.7 MiB, under the 60 MB NFR-2.1 limit. After the gate, 487fce6 (bulk reload) was re-verified on its own with bulk_ops 3x, ops_e2e, ui_e2e and ops_kill.
- This verification: targeted run of `cargo test -p ostro --test ops_e2e` at 11ced2a (result below).

Result: `test result: ok. 3 passed; 0 failed` (sc1, sc2, sc3; 46.14 s run time, built in slot-2 on a shared machine).

### Human Verification Required

These are the items in the frontmatter `human_verification`, which is the 11-VALIDATION Manual-Only list: browser rendering, CSP console, htmx swaps/polls/confirm, no-JS paths, keyboard focus, real foreign-origin CSRF, non-loopback hostname, remote CI legs, and user review of D-P11-3/8, D-P11-13, D-P11-14 and D-P11-17 (+ amendment).

### Gaps Summary

There are no gaps. All five success criteria are delivered in code and covered by real-binary tests whose assertions check exact behaviour, not just status codes. The open items are by-design human checks, user-review decisions and follow-ups owned by other phases or by documentation (the CLOCK_GATE sweep belongs to Phase 10 10-06, the API-proto numbers, the limits-table completeness, and leader-only coordination for Phase 12/13). None of them blocks an SC.

---

_Verified: 2026-11-22T20:30:00Z_
_Verifier: Claude (gsd-verifier)_
