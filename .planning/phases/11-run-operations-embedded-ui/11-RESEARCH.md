# Phase 11: Run Operations & Embedded UI - Research

**Researched:** 2026-11-19
**Domain:** operator run repair (rerun-from-step, DLQ, bulk ops), indexed run search, published limits, and a server-rendered embedded web UI inside the single binary
**Confidence:** HIGH for the code facts (read from the tree at `a6197c1`) and the freeze analysis. MEDIUM for the UI stack: crate and asset versions were checked on crates.io and npm today, but no prototype was built. MEDIUM for the bulk-operation and index designs, which are new.

## Pre-freeze findings (read first)

Phase 5's freeze flip (05-20) pins `wit/FROZEN.toml` (byte identity of `runtime.wit`), marks `docs/storage-format.md` "FROZEN v0.1", re-pins the goldens and flips RESERVATIONS. **Phase 11 needs no mandatory pre-freeze code change.** Its design uses only additive post-freeze paths:
- a new `Cmd` tag;
- new `StartRun` fields read only by new apply code;
- a meta `ext` tag;
- a derived keyspace that sits outside the digest.

Some items are impossible after 05-20, or much cheaper before it:

| ID | Item | Kind | Why pre-freeze | Recommendation |
|----|------|------|----------------|----------------|
| **PF-11-1** | Add `limit_exceeded` and `payload_too_large` to the reserved `error-info.kind` list in the `runtime.wit` doc comment (line 44ff), as "Run operations (Phase 11)". | WIT doc comment only | `FROZEN.toml` pins the file's sha256, so even a doc edit is impossible after 05-20. | **Adopt (insurance).** Phase 11's design surfaces limit breaches as run-level attempt failures, so it does not depend on this item. Without it, no limit can ever reach a guest as a typed, documented failure kind. |
| **PF-11-2** | One "Derived keyspaces" paragraph in `docs/storage-format.md`. A keyspace whose on-disk name starts with `x_`: is not in `TABLES` and not in the digest; is not exported in OSNP snapshots; is not visited by blob GC; is rebuilt from digest state when its version marker is missing or stale (including after snapshot install); and is ignored by older binaries. Reserve the name `x_run_index`. | Doc rule | Without it, adding a non-digest keyspace later amends a FROZEN document's keyspace rules. Today the doc says every keyspace is in `TABLES` and therefore in digest order. | **Adopt.** One paragraph in 05-20's storage-format edit. |
| **PF-11-3** | A Phase 11 block in `.planning/RESERVATIONS.md` (§ "Proposed RESERVATIONS block" below): Cmd 43–47, StartRun 40–43, `JournalEvent.4`, InvalidCode 29–31, ext tags 144–159, plus name rows. | Docs only | Allowed at any time under rule 2. Phases 9, 10 and 11 run in parallel, though, so the block should exist before any of them allocates. Phase 10 F1 also asks for its own name rows. | **Adopt.** Fold into 05-20's RESERVATIONS flip, or do it first thing in 11-01. |
| **PF-11-4** | `RunRecord.rerun_from_seq: Option<u64>`, appended last and stored only, like Phase 7 F1 `pause`. | State value (positional postcard) | After the freeze, any RunRecord change needs a row migrator (D18). Before it, the change costs one append plus golden/digest re-blesses. | **Optional, recommended.** It gives an O(1) lineage display ("rerun of X from seq N") and a crash-robust re-arm trigger (`next_seq == rerun_from_seq` means no live event yet). Zero-format fallback: a system tag `__ostro/rerun_from=<seq>` on the rerun, which RunRecord already stores (`tags`). |
| PF-11-5 | Apply stamps the attempt token into each stored `JournalEvent` (new prost field 4), giving exact replay markers. | State value | Doing this later in apply would change the stored rows of old logs and break the format corpus. | **Do not adopt.** Use the post-freeze path instead: the executor sets `JournalEvent.attempt` in the `EventsAppended` it submits (submitter-encoded, as in D21), and apply stores it verbatim. Runs from before Phase 11 get approximate markers. |

Explicit non-items, confirmed by the analysis below:
- No new journal kinds. A rerun copies events verbatim. In-place rewind and journal editing are rejected.
- No new `TerminalStatus`. The DLQ is a view, not a status.
- No meta variants. `on_failure` config goes in ext tags.
- No WIT records or functions.
- **DLQ rows are never written by apply for existing commands.** Writing them would change apply semantics for old logs and break the format corpus. Rows come only from the new `DlqUpdate` command.

---

<user_constraints>
## User Constraints

There is no `11-CONTEXT.md` (`/gsd:discuss-phase` was not run). The binding constraints come from the orchestrator prompt, ROADMAP/REQUIREMENTS, PROJECT.md, STATE.md and RESERVATIONS.md. They are copied here as locked:

### Locked Decisions
- **Roadmap Phase 11.** Goal: "Operators triage and repair runs from the CLI/API or from an embedded UI that has real actions, within documented and enforced limits." Depends on Phase 8; can run in parallel with Phases 9–10. Success criteria:
  1. A failed run lands in the DLQ with a reason and triggers the optional `on_failure` workflow. It can be requeued, or rerun from a chosen step: the journal prefix is copied into a new run that skips completed steps.
  2. Bulk cancel/retry/rerun over a tag or status filter runs spread over time without overloading the node. Tag search returns bounded, indexed results.
  3. Exceeding a published limit (payload size, history length, children per run) produces a clear, documented error rather than undefined behaviour.
  4. The embedded UI, served from the binary, shows the run list with filters/tags, a history timeline with replay markers and published status. It also shows modules, versions, queues, schedules, workers and partition health.
  5. From the UI an operator can cancel, terminate, retry, rerun-from-step, pause/resume, requeue from the DLQ, and run bulk operations.
- **Single static binary** (D1). The UI is embedded. **No Node.js at build time.** NFR-2.1: binary < 60 MB including the UI.
- **Frozen formats after 05-20:**
  - WIT 0.1 is byte-frozen.
  - Cmd tags and journal kinds are append-only.
  - No `EVENT_CODEC_VER` bump.
  - RunRecord changes need a row migrator (D18).
  - Meta changes are new variants with a META_FORMAT bump, or ext tags (D20).
  - Every ordinal and reserved name comes only from `.planning/RESERVATIONS.md`.
- **Determinism:** replay divergence is detected and never tolerated. Apply never decodes payloads or fetches blobs (D21, BlobRef invariant).
- **Supply chain:**
  - cargo-deny with `exclude-dev = true`.
  - Banned: chrono, bincode, prometheus, openssl, native-tls, tokio-uring.
  - `deny-multiple-versions` on tonic, prost, hyper, http, axum, OTel, wasmtime and the wit crates.
  - No git deps.
- **Process:**
  - Commits go through a private index and end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
  - Never reference a Chronik clone path. Never read `~/.ssh`.
  - **Never use the reachable microk8s cluster.**
  - No pushes; tags stay local.
  - Executors never run gsd-tools state/roadmap/phase/requirements mutators.
- **Phase 14 scopes (planned, P14-F6):** `admin`, `deploy`, `operate` (cancel, terminate, retry, pause/resume, signal, queue policies, schedule pause/trigger, DLQ), `trigger`, `read`. Phase 11 must leave a seam for them, not implement them.

### Claude's Discretion
- Which operations OPS-02..07 add, and their API shapes.
- The rerun mechanism, within the freeze.
- Index, DLQ and bulk designs.
- The UI technology and security model.
- The plan breakdown.

### Deferred Ideas (OUT OF SCOPE)
- Journal editing/patching, skipping a step with an operator-provided result, and in-place rewind/reset of the same run. All are rejected; see § "Rerun safety".
- Auth enforcement, tenant isolation and audit log (Phase 14). Phase 11 provides seams only.
- Secrets, grants, connections and egress UI. UI-03 does not list them, and Phase 6 D-FX9 is API/CLI only.
- Multi-node job routing for bulk ops (Phase 13). A Playwright browser suite (needs Node).
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| OPS-02 | Rerun a run from a chosen step (journal prefix copied into a new run) | § Rerun safety; D-P11-1..3. Mechanism: StartRun 40/41 carry a verbatim prefix. Before submit, a cut-safety analyzer runs and a mandatory replay check runs against the target module. Re-arm through 05-11's `rearm_unresolved`. |
| OPS-03 | Bulk cancel/retry/rerun over a filter, rate-spread | § Bulk operations; D-P11-7. Node-local job with a snapshot bound, hand-rolled GCRA (Phase 7 `flow/limits.rs`), backpressure, per-run idempotency keys and a checkpoint file. |
| OPS-04 | DLQ with reason; optional `on_failure` hook; requeue | § DLQ; D-P11-5/6. DLQ membership is a derived view, with `Cmd 43 DlqUpdate` marks in keyspace 11. A leader-side hook service uses dedupe key `__onfail/<run>` and has a loop guard. |
| OPS-05 | Tags with bounded indexed search | § Derived run index; D-P11-4. Non-digest keyspace `x_run_index` (tag / workflow / terminal-status / tenant prefixes, newest-first keys), k-way merge across partitions, page tokens. |
| OPS-07 | Published limits documented and enforced with clear errors | § Limits; D-P11-8. `[limits]` config, live-only engine enforcement mapped to `limit_exceeded` attempt failures, API caps with uniform `details{limit,max,actual}`, `docs/limits.md`, `GET /v1/limits`, and a doc-sync test. |
| UI-01 | Embedded UI: run list/filter/tags, timeline with replay markers, published status | § UI stack (askama + vendored htmx in a new `ostro-ui` crate), § Replay markers |
| UI-02 | UI actions: cancel, terminate, retry, rerun-from-step, pause/resume, DLQ requeue, bulk | § UI actions. Forms (PRG) enhanced by htmx, calling `ControlPlane` and extension traits server-side, with CSRF guard. |
| UI-03 | UI views: modules, workflows/versions, queues, schedules, workers, partition health | § Inventory APIs: new `GET /v1/modules`, `GET /v1/queues`, `GET /v1/partitions`, plus Phase 7 workers/flow/demand and Phase 8 schedules |
</phase_requirements>

## Summary

Most of what Phase 11 needs already exists below the API:
- Phase 5 provides the run lifecycle (signal/cancel/terminate/retry with D1 resume-vs-restart), the re-arm of unresolved ops (`derive::rearm_unresolved`), the RunStarted-at-StartRun path (`StartRun.run_started`, field 23) and `retry_of` lineage.
- Phase 7 provides run, queue and workflow pause/resume (Cmd 20/21) plus GCRA admission.
- Phase 8 provides schedules, webhooks, run logs, metrics and the OSNP snapshot format.
- The `dlq` keyspace (11) exists with no rows, and retention already deletes it by run prefix.
- `RunRecord.tags` exists. List is a budgeted scan (`SCAN_BUDGET = 10,000`), oldest first, with no index.

The phase therefore adds five operator capabilities on top of existing primitives. Each uses an additive, post-freeze-safe mechanism:
1. **Rerun-from-step** = a new run whose `StartRun` carries the source journal prefix verbatim.
2. **DLQ** = a derived view of Failed/TimedOut runs, minus explicit marks.
3. **Bulk ops** = a rate-spread, node-local job over a snapshot of the index.
4. **Tag search** = a derived, rebuildable index keyspace outside the digest.
5. **Limits** = live-only engine checks plus API caps, published in one document with a sync test.

The key safety argument for rerun is that **every prefix of a valid journal is a state the run actually passed through**. Replaying that prefix with the same RunStarted bytes (same input, rng seed and `accepted_at`, hence the same logical clock), and then continuing live, is ordinary replay. Three things can break it:
- **Ops open at the cut whose completion is bound to the old run id** (children, effects, await tokens, unmatched signal payloads). The analyzer rejects these cuts.
- **Guest code that put `run-info.run_id` or `module_hash` into hashed command arguments** before the cut (HTTP url/body, SQL params, object key/body, child input are all hashed). The mandatory pre-submit replay check catches these as NonDeterminism and returns `409 rerun_nondeterministic{seq}`.
- **Rerunning on a changed module.** This is the intended "fix and rerun" flow; the same replay check validates it.

In-place rewind of the same run id is rejected:
- It would truncate an append-only journal (a new destructive command).
- It would reuse `(run, op)` idempotency keys for effects that already executed.
- It would destroy the audit trail.

The UI is **server-rendered HTML (askama 0.16.1) with htmx 2.0.11 vendored as one static file**, embedded with `include_bytes!` in a new `ostro-ui` crate:
- It needs no Node, no WASM toolchain and no npm supply chain. It adds about 60 KB of assets.
- Every UI page and action is testable in plain Rust: HTTP requests against `FakeControlPlane` plus insta snapshots. No headless browser is needed.
- It runs under a strict CSP (`script-src 'self'`, no inline script or style, htmx `allowEval:false`).
- UI handlers call the same `ControlPlane` and extension traits as REST, so Phase 14's scope checks and audit apply to both automatically.
- CSRF and DNS-rebinding defences are needed **in Phase 11 itself**, even with auth off. Form POSTs to `localhost` are simple cross-site requests.

**Primary recommendation:** adopt PF-11-1..3 (doc-only) in 05-20, and PF-11-4 if a RunRecord edit is still possible. Gate Phase 11 on **Phases 7 and 8 complete**, since UI-02 pause/resume and UI-03 workers need Phase 7. Build it in 16 plans and 7 waves, with one owner per shared file, following the Phase 7/8/9 shared-file protocol.

## Standard Stack

### Core (new in Phase 11)
| Library / asset | Version | Purpose | Why standard |
|---|---|---|---|
| `askama` | `=0.16.1` (2026-09-04, MIT OR Apache-2.0) | Compile-time-checked HTML templates with auto-escaping | Actively maintained (5 releases in 2026). Templates are files, and invalid templates fail the build. Deps: `itoa`, `askama_macros` → `askama_derive` (proc-macro2, quote, **syn 3**, memchr, rustc-hash, askama_parser/winnow). Disable `config` (basic-toml/serde) unless needed. |
| htmx | `2.0.11` (npm `latest`; `next` is 4.0.2; licence **0BSD**) | Partial page updates, polling, `hx-confirm` | One file, no build. CSP-compatible with `allowEval:false` and `includeIndicatorStyles:false`. 0BSD is already in the `deny.toml` allow list. Vendored once with its integrity recorded. |
| `include_bytes!` / `include_str!` | std | Embed htmx, `app.css` and a tiny `app.js` | 3–5 files need no `rust-embed`. That crate adds walkdir/mime_guess/proc-macro, and in debug builds it reads from disk, which makes debug and release behave differently. |

### Already in the workspace (reuse)
| Item | Version | Use in Phase 11 |
|---|---|---|
| axum | 0.8.9 | `/ui` router merged on the one port |
| utoipa / utoipa-axum | 6.0.0 / 0.3.0 | New REST routes in the OpenAPI document |
| tonic / prost / protox | 0.14.9 / 0.14.6 | New `ostro.api.v1.RunOps` and `ostro.api.v1.Inventory` services |
| insta | 1 | HTML snapshots and the OpenAPI snapshot |
| reqwest | 0.13.5 | Test client and CLI |
| sha2 | workspace | Asset ETags and the vendored-asset integrity test |
| Phase 7 `flow/limits.rs` (hand-rolled GCRA, D-W24) | in-tree | Bulk-job rate spreading. Do not add `governor`. |
| Phase 10 `ostro-server/src/replay.rs` `replay_check` (10-RESEARCH Pattern 6) | in-tree (whoever lands first) | Rerun pre-submit check and dry-run |
| 05-11 `derive::rearm_unresolved` | in-tree | Re-arm open timers and signal waits in a rerun |

### Alternatives Considered
| Instead of | Could use | Tradeoff / verdict |
|---|---|---|
| askama | **maud 0.27.0** (2025-02-02; syn 2; axum 0.8 via axum-core 0.5) | Viable fallback: smaller and uses syn 2, which is already in the tree. But there has been no release in 21 months, and templates live in Rust macros. Use it only if `syn 3` causes a deny or MSRV problem. |
| SSR + htmx | Hand-written vanilla-JS SPA over `/v1` with a bearer token in sessionStorage | No server-side UI routes, but the JS logic is only testable in a browser, and the token is readable by any XSS. Rejected. |
| SSR + htmx | Leptos / Dioxus WASM | Needs a wasm32-unknown-unknown build, a pinned wasm-bindgen CLI or trunk, and hundreds of deps under cargo-deny. Hydration still needs a browser to test. Rejected. |
| SSR + htmx | A committed prebuilt SPA bundle (React/Svelte) | Opaque minified artefacts. The npm supply chain is invisible to cargo-deny. Regenerating it needs Node. Rejected. |
| `include_bytes!` | `rust-embed` 8.x | More deps, and debug builds read from disk. Not needed for under 10 files. |
| htmx 2.0.11 | htmx 4.x (`next`) | Pre-release with a changed attribute-inheritance model. Do not adopt in 1.0. |

**Installation (11-02, the only plan that edits Cargo files):**
```toml
# Cargo.toml [workspace.dependencies]
askama = { version = "=0.16.1", default-features = false, features = ["derive", "std"] }  # verify feature names in 11-02 Task 1 (cargo add --dry-run / docs.rs)
```
The vendoring step uses network access once, at plan time. The build never needs it.
```bash
d=$(mktemp -d) && curl -fsSL -o "$d/htmx.tgz" https://registry.npmjs.org/htmx.org/-/htmx.org-2.0.11.tgz
# verify sha512 == npm dist.integrity "sha512-Thx/WtpeOQqSrqBCw/A1cwGJGg4UrVa3+sW0GmrM3p4gJgO89ecH4qtbnyzDDWFvBTqjnIMCgELTNt636dtamA=="
# extract package/dist/htmx.min.js -> crates/ostro-ui/assets/vendor/htmx-2.0.11.min.js; record sha256 in VENDORED.md
```

## Architecture Patterns

### Recommended structure
```
crates/
├── ostro-types/src/command.rs        # 11-02: StartRun 40 rerun_prefix, 41 rerun_from_seq; Cmd 43 DlqUpdate; JournalEvent.4 attempt (+goldens)
├── ostro-partition/src/
│   ├── store/derived.rs               # 11-03: x_run_index keyspace, version marker, rebuild
│   ├── apply/index.rs                 # 11-03: index hooks (called from run/terminal/lifecycle/retention)
│   ├── apply/dlq.rs                   # 11-06: DlqUpdate apply
│   └── apply/rerun.rs                 # 11-05: StartRun prefix journaling (called from run.rs)
├── ostro-engine/src/limits.rs         # 11-04: live-only limit checks → AttemptFailure::LimitExceeded
├── ostro-api/src/phase11.rs           # 11-07: OpsApi + InventoryApi traits, DTOs; ApiExtensions.ops slot
├── ostro-proto/proto/ostro/api/v1/ops.proto   # 11-07: RunOps + Inventory services
├── ostro-server/src/
│   ├── rerun.rs                       # 11-05 (analyzer) / 11-08 (control plane)
│   ├── control/p11_{search,dlq,rerun,bulk,inventory}.rs   # one file per plan (D-W17 pattern)
│   └── services/{dlq_hook,bulk}.rs    # Phase 8 services seam; one registration line each
├── ostro-ui/                          # NEW crate (11-13..15)
│   ├── src/{lib,security,auth,assets,pages,actions,inventory,timeline}.rs
│   ├── templates/ui/*.html            # askama
│   └── assets/{app.css,app.js,vendor/htmx-2.0.11.min.js}
└── ostro-cli/src/commands/{rerun,dlq,bulk,limits}.rs     # 11-12 (Phase 10 coordination)
docs/{limits.md, operations.md, ui.md}  # 11-04 / 11-16
```

### Pattern 1: Rerun = new run with a verbatim journal prefix (OPS-02)
**What.** `POST /v1/runs/{id}/rerun` with body `{from_seq | from_step:{name, occurrence?}, version: "active"|"source"|N, idempotency_key?, dry_run}`. The default version is `"active"`, which is the "fix and rerun" flow.

The control plane:
1. Load the source RunRecord. The source must be **terminal**: a non-terminal source returns `409 run_not_terminal` (terminate it first).
2. Read the journal `[0, cut)`. `cut` is the first event of the chosen step's op, so the step re-executes (including its StepAttemptFailed chain). Rules: `1 ≤ cut ≤ index of the terminal event` (RunCompleted 26, RunFailed 27, RunContinuedAsNew 28, RunTerminated 71 are never copied).
3. Run the **cut-safety analyzer**, a pure function over decoded events in `ostro-server/src/rerun.rs`. Reject cuts where the prefix holds:
   - an open `ChildStarted` (the child completes to the old parent);
   - an open `EffectScheduled` (the bridge is bound to the old run);
   - an open `AwaitTokenCreated` (the token string embeds the old run id; `replay/token.rs`);
   - a `SignalPayload` (96) with no `SignalMatched` (97) (inbox rows are not copied).

   Open `TimerStarted` and `SignalWaitStarted` are **allowed**. They are re-armed the way 05-11 does it, and past-due timers fire immediately, as in D1. The error is `409 rerun_cut_unsafe{seq, reason, nearest_safe_seq}`.
4. Run the **mandatory replay check** of `[0, cut)` against the target module (Phase 10's `replay_check`; verdict `ok_frontier`). `NonDeterminism` returns `409 rerun_nondeterministic{seq, expected, actual}`. `dry_run` stops here and returns the verdict plus the plan.
5. Submit `StartRun` with:
   - a new run id;
   - `workflow`, `queue`, `tenant` and `tags` (plus the lineage tag) copied;
   - `rng_seed` = source;
   - `input` = the source PayloadRef (blob refs may be shared; P8-D8 GC is reachability-based);
   - `run_started` = **the source seq-0 event bytes verbatim** (same `ts`, `accepted_at_nanos` and `deadline_nanos`);
   - `rerun_prefix` (field 40) = events `1..cut` as `JournalEvent{seq, kind, payload: PayloadRef}`, with payloads over the inline limit kept or offloaded as BlobRefs;
   - `rerun_from_seq` (41) = `cut`;
   - `retry_of` (21) = source;
   - `module_sha` / `workflow_version` = the target;
   - idempotency key `__rerun/<source>/<key>` when one is given.
6. **Apply** (11-05, `apply/rerun.rs`) runs only when field 40 is non-empty, so old logs are unaffected. It checks: seqs are contiguous from 1, kinds are in 16–63/64–127 and not terminal, each inline payload ≤ 256 KiB (W5), and `run_started` is present. It writes events `0..cut` verbatim and sets `next_seq = cut`, `status_seq` (last kind 25 in the prefix, kind only) and `rerun_from_seq`. It never decodes a payload.
7. **Executor** (11-05): on any activation where `rec.rerun_from_seq == Some(rec.next_seq)` (no live event yet), submit `rearm_unresolved(journal)` as a zero-event `EventsAppended` before loading the journal. This is idempotent: RegisterWait is idempotent (05-08) and timers are point keys. It survives crashes, unlike 05-11's in-memory `rearmed` map.

**Envelope bound.** The StartRun must fit `DEFAULT_MAX_ENVELOPE_BYTES` (4 MiB). The submitter keeps existing BlobRefs and offloads any event payload over 1 KiB until the encoded command is ≤ 3.5 MiB. If it still does not fit, the request fails with `413 rerun_prefix_too_large`. With `max_history_events = 50,000`, about 60 B per offloaded event is ≈ 3 MB. Chunked staging is a documented follow-up; see Open Questions.

### Pattern 2: Derived run index (OPS-05, UI-01 filters, DLQ view)
- **Keyspace** `x_run_index`. It lives in the same fjall `Database` as the partition, so it shares the apply `WriteBatch` and stays atomic with `applied_offset`. It is **not** a `Table` variant and **not** in `TABLES`, so the digest, OSNP export and the blob-GC visitor stay unchanged. Writes go through a new `ApplyBatch::insert_derived/remove_derived`.
- **Keys** use inverted uuid order, so the native forward scan returns newest first. Today's `list.rs` notes "there is no reverse scan".
  - `0x01 ‖ str tag ‖ !uuid` → `()`
  - `0x02 ‖ str workflow ‖ !uuid` → `()`
  - `0x03 ‖ status u8 ‖ !uuid` → `()`, terminal statuses only
  - `0x04 ‖ str tenant ‖ !uuid` → `()` (Phase 14 reuses it instead of the optional tenant keyspace ≥ 20 in 14-RESEARCH)
  - `0x00 'v'` → `{index_version u32}`
- **Hooks** (four call sites, plus retention):
  - `StartRun` insert: tags, workflow, tenant.
  - `terminal::on_terminal`: status row.
  - `RetryRun` resume (lifecycle): delete the status row.
  - Retention expiry: delete all of the run's rows, computed from the RunRecord being deleted. Phase 9's deferred-EXPIRE branch means this must run at the actual delete.
  - The status index is **not** maintained for Ready/Running churn: that would add 2 writes per activation on the hot path. "Running" and "ready" listings use the existing `running`/`ready` keyspaces.
- **Rebuild.** At store open, a missing or stale marker triggers a background scan of `runs` (decode RunRecord → rows) in batches. Queries fall back to the existing budgeted scan until the marker is written. Snapshot install (08-02's install path) must drop `x_run_index` so it rebuilds.
- **Query** (`query::index_page`). Prefix scan per partition, then a k-way merge across P partitions by `!uuid`. The page token is `i1.<prefix-kind>.<run id>`, budgeted like `list.rs`. Multi-tag AND scans the first tag and filters the rest in memory within the scan budget (documented).
- **Property test.** Index rows equal `f(scan(runs))` after random command sequences, after a crash/reopen, and after marker deletion.

### Pattern 3: DLQ as a derived view plus marks (OPS-04)
- **Membership** = status index rows for `Failed` and `TimedOut`, minus runs whose `dlq` row (keyspace 11) says `dismissed` or `requeued_to`.
- **`Cmd 43 DlqUpdate{run, op}`** with `op` = `HookStarted{hook_run}` | `Dismissed{reason, at_ms}` | `Requeued{mode, new_run?, at_ms}`. It upserts `run(18) → postcard DlqRow{hook_run, dismissed_at_ms, dismiss_reason, requeued_to, requeued_at_ms}`. Retention already deletes `dlq` by run prefix (`retention.rs:98`). Old logs never contain Cmd 43, so their state is unchanged.
- **Reason.** Read per row from RunRecord `terminal_reason` (TimedOut) or the result payload JSON (`failure_json`, executor failures and guest RunFailed). That is bounded by the page size. A `reason` filter is applied in memory.
- **Requeue** = `RetryRun` (D1):
  - resume, when `resumable`: the run leaves Failed and drops out of the DLQ automatically;
  - restart: a new run with idempotency `__dlq/<run>/<n>`, then `DlqUpdate Requeued{new_run}`;
  - or rerun with `from_seq`, then `Requeued`.

  The two-command sequences are not atomic but are idempotent. Re-issuing them after a crash returns the existing run.
- **Auto-pause interplay (Phase 7 OPS-01).** With `on_retries_exhausted = pause`, the run is paused, not Failed, so it is not in the DLQ. The UI lists it under "Paused (retries exhausted)" via a budgeted scan on `RunRecord.pause`.

### Pattern 4: `on_failure` hook (OPS-04)
- **Config.** Meta ext tag on `WorkflowPolicy` (and `QueuePolicy`, where workflow wins over queue): `OnFailure{workflow, queue?, include_input: bool}` (ext tag 144). This is post-freeze safe with no variant and no META_FORMAT bump (D20 amended).
- **Leader service** `services/dlq_hook.rs`, on the Phase 8 seam:
  1. On `Action::RunCompleted{Failed|TimedOut}`, resolve the policy and submit `StartRun(hook_workflow, input={run_id, workflow, version, status, reason, failed_at_ms, tags, input?}, idempotency_key="__onfail/<run>", conflict=ReturnExisting)`.
  2. Then submit `DlqUpdate HookStarted`.
  3. At leader start, re-drive DLQ entries newer than `[dlq] hook_catchup` (default 24h) that lack `HookStarted`.
- **Loop guard.** Hook runs carry the system tag `__ostro/on_failure_of=<run>`. A failed run carrying that tag never fires a hook.
- **Exactly once.** The dedupe row makes the start idempotent. The mark stops re-drives after the dedupe row expires.
- **Metric.** Phase 8's 08-16 owns `ostro_dlq_total{reason}`. Phase 11 adds only `ostro_dlq_hooks_total{result}` and `ostro_dlq_requeues_total{mode}`.

### Pattern 5: Bulk operations (OPS-03)
- **Model.** A node-local job: `{id: "blk_<uuidv7>", action: cancel|terminate|retry|rerun{from}|requeue{mode}|pause|resume|signal{name,payload}, filter, rate, max_in_flight, dry_run}`.
- **Snapshot bound.** The filter always gets `created_before_ms = job.created_at_ms`, and the job excludes runs it created itself. This prevents chasing its own reruns, which carry the same tags.
- **Selection.** Through the derived index (bounded pages), capped at `[bulk] max_matches` (default 100,000).
- **Rate.**
  - Phase 7 GCRA, default 20 ops/s with burst 10, cap 500 ops/s, `max_in_flight` 4.
  - Adaptive pause when the target partition's ready backlog exceeds `[bulk] backlog_high` or a start returns 429 `rate_limited` (honour `retry_after_ms`, D-W21).
  - **Resumed runs bypass Phase 7 throttles (D-W13: re-activations are never throttled), so the bulk rate is the only guard for bulk retry.**
- **Idempotency per run.**
  - cancel/terminate are naturally idempotent (409 on terminal → `skipped`).
  - signal uses `request_id = blk_<id>/<run>`.
  - restart/rerun use idempotency `__bulk/<id>/<run>`.
  - DLQ requeue writes a mark.
- **Durability.** Spec plus cursor plus counters are checkpointed to `<data>/bulk/<id>.json` every 1 s or 500 ops (tmp + fsync + rename). On restart, unfinished jobs resume from their cursor; re-issued ops are idempotent. Backup ignores the dir (Phase 14 F8 rule). In a cluster, jobs are node-local, and `GET /v1/bulk/{id}` on another node returns 404 until Phase 13 forwarding.
- **API.**
  - `POST /v1/bulk` → `{job_id}`; dry-run returns `{matched_estimate, sample[≤20]}`.
  - `GET /v1/bulk`, `GET /v1/bulk/{id}` → `{state, matched, done, skipped, failed[≤100 with reasons], rate, started_at, eta}`.
  - `POST /v1/bulk/{id}/cancel`.
  - At most `[bulk] max_jobs` (4) concurrent jobs, else 429.

### Pattern 6: Limits (OPS-07)
- **Live-only enforcement in the engine.**
  - Limits are checked only when the guest issues a *new* command, never during replay. Lowering a limit therefore cannot break replay of existing journals. A run past the limit fails at its next live command (documented).
  - A breach becomes `AttemptFailure::LimitExceeded{limit, max, actual}` → non-retryable → `CompleteRun{Failed, resumable: true}` (executor-level, D1). Operators raise the limit and retry (resume).
  - Result payload reason: `limit_exceeded: history_events 50001 > 50000`.
  - **Phase 7's `WireOutcome` mirror (D-W19, compile-time exhaustive) must gain the variant in the same plan.**
- **API caps.** Checked in the API layer before proposal. Errors: `400 invalid_argument` / `413 payload_too_large` / gRPC `INVALID_ARGUMENT` / `RESOURCE_EXHAUSTED`, with uniform `details: {"limit": "<name>", "max": N, "actual": M}`.
- **Publication.**
  - `docs/limits.md`: one table with name, default, configurable?, where enforced, error code and replay behaviour.
  - `GET /v1/limits`: effective values.
  - `limits_doc` test: the doc table == `/v1/limits` == constants.

Proposed defaults. The numbers marked "proposed" are discretionary; Temporal's published limits are the reference.

| Limit | Default | Enforced | Status |
|---|---|---|---|
| History events per run | 50,000 (suggest continue-as-new at 10,000, exists) | engine, live | proposed (Temporal: 51,200 hard / 10,240 warn) |
| Children started per run | 10,000 total (journal count) | engine, live | proposed (Temporal: 2,000 *pending* per type) |
| User payload (input, signal, step output, result, child input, token completion) | 4 MiB (= `[api] max_request_bytes`) | API + engine | proposed; > 256 KiB is offloaded to blobs (exists) |
| Published status | 64 KiB | engine (exists) | exists |
| Context bytes | 64 KiB | engine (exists) | exists |
| Tags | 64 × 128 B | API + apply (exists) | exists |
| Signal name / reason / request id | 256 / 1024 / 64 B | API + apply (exist) | exists |
| Module size | 64 MiB | API (exists) | exists |
| List page / long-poll | 1,000 / 60 s | API (exists) | exists |
| Rerun prefix | ≤ history limit and ≤ 3.5 MiB encoded | control plane | new |
| Bulk | 100,000 matches/job, 4 jobs, ≤ 500 ops/s | control plane | new |
| Guest logs | Phase 8 P8-D16 | Phase 8 | exists (P8) |

### Pattern 7: Embedded UI (UI-01..03)
- **Routes** live under `/ui/*` in `ostro-ui`, merged into the one-port router. `GET /` becomes a `303 → /ui/` (edit 04-08's `ui_shell` handler and re-bless the OpenAPI snapshot).
- **Pages render server-side by calling `Arc<dyn ControlPlane>` and the extension traits** (Phase 7/8 slots, Phase 11 `OpsApi`/`InventoryApi`). The UI is a sibling adapter of REST, not a client of it.
- **Progressive enhancement.**
  - Every action is a `<form method=post>` that works without JS (POST → 303 → GET).
  - htmx adds `hx-post` + `hx-target` fragment swaps, `hx-confirm` for destructive actions, and `hx-trigger="every 2s"` polling of status fragments. The server omits the trigger once the run is terminal.
- **htmx config** via `<meta name="htmx-config">`: `{"allowEval":false,"allowScriptTags":false,"includeIndicatorStyles":false,"historyCacheSize":0,"selfRequestsOnly":true}`, plus `hx-history="false"` on `<body>` so run data never goes to localStorage. `allowEval:false` disables `hx-on:*`, event filters and `js:` values; none are used.
- **Security headers** on every `/ui` response:
  - `Content-Security-Policy: default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'`
  - `X-Content-Type-Options: nosniff`, `Referrer-Policy: no-referrer`, `X-Frame-Options: DENY`, `Cache-Control: no-store` (pages).
  - Assets: `Cache-Control: no-cache` + strong `ETag` (sha256 at startup).
- **No inline `style=` attributes** (CSP blocks them). Bar widths in the timeline use SVG geometry attributes (`x`, `width`), which CSP does not treat as style.
- **CSRF and DNS-rebinding guard** (`security.rs`, required in Phase 11 even with auth off). Mutating `/ui` requests must satisfy all of:
  - (a) `Sec-Fetch-Site: same-origin`, or (when absent) `Origin` equal to the request's scheme://host;
  - (b) `HX-Request: true`, or a valid per-session `csrf` form field (double-submit cookie, `SameSite=Strict`);
  - (c) `Host` in the allowlist: the bound address, `localhost`, `127.0.0.1`, `[::1]`, plus `[ui] allowed_hosts`.
  - Otherwise 403. While auth is off, (c) also applies to GET `/ui/*`.
- **Auth seam for Phase 14.** `trait UiAuth { fn principal(&self, parts: &Parts) -> Result<UiPrincipal, Reject>; }`. The default `AllowAll` is used while auth is off. Phase 14 implements a login flow:
  - `POST /ui/login` exchanges a bearer for an `HttpOnly; SameSite=Strict; Secure`(TLS) session cookie mapped in memory to the token id;
  - every request re-validates expiry and revocation;
  - scopes drive button rendering (`read` views, `operate` actions) and are re-checked at the ControlPlane.
- **Config** `[ui] enabled` (default true in dev). In the server profile it is true only when the listener is loopback or `enabled = true` is explicit, because the REST API is equally open until Phase 14 (documented, logged). Also `[ui] allowed_hosts = []`.

### Pattern 8: Replay markers in the timeline (UI-01)
- **Exact** (runs from Phase 11 on):
  - The executor (`sink.rs`) sets `JournalEvent.attempt` (field 4, `fixed64 = epoch<<32 | n`) on events it submits.
  - Apply-originated inputs (SignalPayload/Matched, TokenCompleted, TimerFired, RunTerminated) have `attempt = 0` ("input").
  - Copied rerun events keep the source's values and render as "copied from <source>" up to `rerun_from_seq`.
  - The timeline groups events by attempt. **The first event of each attempt group carries a replay marker**: "attempt (e,n) replayed seq 0..k". A hot wait keeps the attempt, so it correctly shows no marker.
- **Approximate** (runs from before Phase 11, no field): a marker before each input event that follows a command event, labelled "likely re-activation (evict model)", plus StepAttemptFailed (retry), RunTerminated and resume points (`resumed_count`).
- **Other overlays:** published status (`status_seq`) as a pinned card, step durations from consecutive `ts`, and a "Rerun from here" button on each step's first event, disabled with the analyzer reason when the cut is unsafe.

### Anti-Patterns to Avoid
- **In-place rewind / journal truncation** of the same run id. It reuses `(run, op)` effect idempotency keys, rewrites audit history and needs a destructive command. Use rerun-as-new-run.
- **Apply writing new rows for existing command types into digest keyspaces** after the freeze, such as DLQ rows on `CompleteRun{Failed}` or an in-digest tag index. It breaks the format corpus and mixed-version replicas. Use derived keyspaces or new commands.
- **Status index on Ready/Running transitions.** It puts write amplification on the activation hot path (5k steps/s gate).
- **UI calling `/v1` from browser JS with a stored bearer token.** It exposes the token to XSS and needs JS tests in a browser.
- **`style="width:…"` or inline `<script>`/`onclick`** in templates. CSP blocks them. The lint test must forbid them.
- **Bulk jobs without a `created_before` bound.** A bulk rerun matches its own new runs and never terminates.
- **Enforcing limits during replay**, or in apply for existing commands. Lowering a limit would then break deterministic replay or old-log rebuild.
- **Rerunning without the replay check.** It creates runs that fail at once with `nondeterminism` and land in the DLQ.

## Don't Hand-Roll

| Problem | Don't build | Use instead | Why |
|---|---|---|---|
| HTML escaping / templating | `format!` HTML with an escape helper | askama auto-escaping (`.html` templates) | XSS via tags, reasons, signal names and failure JSON. |
| Partial updates, polling, confirm dialogs | Custom fetch/DOM JS | htmx 2.0.11 (vendored) | Upstream-tested, CSP-compatible with `allowEval:false`. |
| Rate spreading | A new token bucket or adding `governor` | Phase 7 `flow/limits.rs` GCRA | Already hand-rolled and tested (D-W24); one implementation. |
| Unresolved-op re-arm for reruns | A new scanner | `derive::rearm_unresolved` (05-11) | Same semantics as D1 resume. |
| Prefix replay validation | A new replay loop | Phase 10 `replay_check` (or the 05-12 corpus replay lib, whichever is in-tree) | One verdict model (`ok`, `ok_frontier`, `diverged{seq}`). |
| Idempotent restarts | Custom "already done" tables | Dedupe keys `__rerun/`, `__onfail/`, `__bulk/`, `__dlq/` + `ReturnExisting` | Exactly-once via the existing dedupe keyspace. |
| Partition merge pagination | A new cursor scheme | The `list.rs` k-way merge + token pattern | Proven stable pagination. |
| Asset embedding | `rust-embed` | `include_bytes!` + a static table + sha256 ETag | 3–5 files. |

**Key insight:** every new Phase 11 capability composes existing durable primitives: StartRun with a verbatim journal, RetryRun, dedupe, the outbox-free leader services and the derived index. Correctness rests on mechanisms already proven by the Phase 5 kill campaign.

## Rerun safety (determinism and frozen formats)

1. **Prefix validity.** The journal is append-only, so `[0, cut)` is exactly the state the source run had after appending event `cut-1`. Replaying it is ordinary replay.
2. **Same deterministic inputs.**
   - The RunStarted bytes are copied verbatim, so `input`, `rng_seed` (context random and uuid streams), `accepted_at_nanos` (logical clock base, `replay/cursor.rs:224`) and `deadline_nanos` are identical.
   - Event `ts` values are copied, so `now()` during the prefix matches.
   - D7 step-local RNG is keyed by `(seed, seq)`, so it matches.
3. **What differs and how it is handled:**

| Differs | Effect | Handling |
|---|---|---|
| `run-info.run_id` (new id) | Diverges if guest code put it into a hashed arg (HTTP url/body, SQL params, object key/body, child input) before the cut | Mandatory replay check → `409 rerun_nondeterministic` (documented: use business keys) |
| `run-info.module_hash` / version | Same as above | Same |
| Effect idempotency keys after the cut | New run id → new keys → **effects after the cut really re-execute** | Intended; documented prominently. Effects in the prefix never re-run. |
| Open child / effect / await token / unmatched signal at the cut | Completion is bound to the old run; it would hang | Analyzer rejects, with `nearest_safe_seq` |
| Open timers / signal waits | Need registry rows | Re-armed (idempotent); past-due timers fire immediately (D1 semantics) |
| `run-info.parent` / `continued_from` | RunStarted copied verbatim, so the guest sees the source's values; the rerun is detached (no `StartRun.parent`) | Documented. Reruns of child runs are detached; the parent is not notified. |

4. **Freeze compliance.**
   - New `StartRun` fields 40/41 are read only by new apply code (old logs unchanged).
   - `JournalEvent` field 4 is submitter-set.
   - No journal kinds, no EVENT_CODEC bump, no RunRecord change unless PF-11-4 is adopted pre-freeze.
   - Mixed-version clusters ignoring field 40 are Phase 13's proposal-gating concern, as for every post-freeze field (P6–P9 precedent).

## Common Pitfalls

### Pitfall 1: run_id or module hash in hashed command args breaks the rerun
**What goes wrong:** the rerun replays the prefix and fails with `nondeterminism` at the first effect whose url or body contains `ctx.info().run_id`.
**Why it happens:** `CommandArgs::Http{url, body}`, `Sql{params}`, `Object{key, body}` and `StartChild{input}` are hashed, and `run-info.run_id` is the new id.
**How to avoid:** run the replay check before submit (never only on dry-run). Return the seq and expected/actual. Document "rerun-safe workflows".
**Warning signs:** reruns that immediately land in the DLQ with a `nondeterminism` reason.

### Pitfall 2: copied RunStarted with a fresh timestamp shifts logical time
**What goes wrong:** sleeps or `now()`-derived args diverge.
**Why:** the cursor's logical clock starts at `accepted_at_nanos` and is monotonic over event `ts`.
**How to avoid:** copy seq 0 **verbatim**; never rebuild it with `build_run_started`. A test compares the bytes of the rerun and source prefixes.

### Pitfall 3: re-arm lost after a crash
**What goes wrong:** a rerun with an open timer never wakes.
**Why:** 05-11's re-arm marker is in memory and keyed on `resumed_count`. A rerun has `resumed_count = 0`.
**How to avoid:** use the durable condition `rerun_from_seq == next_seq`, or the system-tag fallback. Re-arm on every activation until the first live event (idempotent). Add a kill test between grant and re-arm.

### Pitfall 4: bulk job feeds on itself
**How to avoid:** `created_before_ms = job.created_at_ms`, plus excluding `__bulk/<id>/` children. Test: a bulk rerun of tag `t` over 50 runs creates exactly 50 runs.

### Pitfall 5: bulk retry floods the node
**Why:** resumed runs skip Phase 7 throttles (D-W13).
**How to avoid:** bulk GCRA plus a backlog high-water pause. Test with paused tokio time and assert op spacing ≥ 1/rate.

### Pitfall 6: CSRF and DNS rebinding against `localhost` with auth off
**What goes wrong:** a web page the operator visits auto-submits `<form action="http://127.0.0.1:7700/ui/runs/x/terminate">`. Or a rebinding domain gets same-origin access.
**Why:** form POSTs are simple requests, so there is no preflight. Auth is off until Phase 14.
**How to avoid:** the Pattern 7 guard (Sec-Fetch-Site/Origin + HX-Request-or-token + Host allowlist). Negative tests for each.

### Pitfall 7: CSP silently breaks htmx features
**What goes wrong:** indicators and `hx-on` stop working, and CSP violations appear in the console.
**Why:** htmx injects indicator `<style>` unless `includeIndicatorStyles:false`; `hx-on` needs eval.
**How to avoid:** the meta config above, indicator CSS in `app.css`, and a template lint test (no `hx-on`, `style=`, inline `<script>` or `on*=`).

### Pitfall 8: derived index stale after snapshot install or downgrade/upgrade
**How to avoid:** install drops `x_run_index`. The version marker triggers a rebuild. Queries fall back to a budgeted scan until the rebuild finishes. Test: delete the marker, reopen, and the index equals the scan.

### Pitfall 9: htmx history cache leaks run data into localStorage
**How to avoid:** `historyCacheSize:0` + `hx-history="false"`. Snapshot-test that the attribute is present on the layout.

### Pitfall 10: adding an `AttemptFailure` variant breaks the Phase 7 worker wire
**Why:** D-W19's mirror enum match is compile-time exhaustive. This is intended to break loudly.
**How to avoid:** 11-04 updates `ostro-worker-proto`'s `WireOutcome` (postcard variant appended last) in the same commit.

### Pitfall 11: `on_failure` loops, or never fires after a restart
**How to avoid:** the loop-guard system tag, the `HookStarted` mark, and startup re-drive within `hook_catchup`. Kill test between StartRun and DlqUpdate: exactly one hook run.

### Pitfall 12: OpenAPI snapshot churn across parallel phases
**How to avoid:** a single API-surface plan (11-07) owns `ostro-api`, the protos and the snapshot, as 08-10 did for Phase 8. The diff must only add lines, except the documented `/` → 303 change.

## Code Examples

### Rerun apply (shape; 11-05)
```rust
// crates/ostro-partition/src/apply/rerun.rs: called from apply_start_run only when !cmd.rerun_prefix.is_empty()
// Never decodes payloads (D21); kinds only. Old logs never reach this function.
pub(crate) fn journal_prefix(b: &mut ApplyBatch<'_>, run: &RunId, rec: &mut RunRecord,
                             seq0: &JournalEvent, prefix: &[JournalEvent], from_seq: u64)
    -> Result<(), ApplyOutcome> {
    // checks: seq0.seq==0 && kind==RunStarted; prefix[i].seq == i+1; from_seq == prefix.len()+1;
    // kind in 16..=63 | 64..=127, not in {26,27,28,71}; inline payload <= INLINE_PAYLOAD_LIMIT
    put_event(b, run, seq0);
    for ev in prefix { put_event(b, run, ev); if ev.kind == EventKind::StatusPublished.code() { rec.status_seq = Some(ev.seq); } }
    rec.next_seq = from_seq;
    rec.rerun_from_seq = Some(from_seq); // PF-11-4; else the system tag carries it
    Ok(())
}
```

### UI action with guard (11-14)
```rust
// crates/ostro-ui/src/actions.rs
async fn terminate(State(ui): State<Ui>, guard: SameOriginMutation, principal: UiPrincipal,
                   Path(id): Path<String>, Form(f): Form<ReasonForm>) -> Result<Response, UiError> {
    principal.require("operate")?;                     // no-op until Phase 14
    ui.cp.terminate_run(TerminateRequest { run_id: id.clone(), reason: f.reason, dry_run: false }).await?;
    Ok(if guard.is_htmx() { RunStatusFragment::load(&ui, &id).await?.into_response() }
       else { Redirect::to(&format!("/ui/runs/{id}")).into_response() })
}
```

### htmx config + layout head (11-13)
```html
<!-- crates/ostro-ui/templates/ui/layout.html -->
<meta name="htmx-config" content='{"allowEval":false,"allowScriptTags":false,"includeIndicatorStyles":false,"historyCacheSize":0,"selfRequestsOnly":true}'>
<link rel="stylesheet" href="/ui/assets/app.css">
<script src="/ui/assets/htmx-2.0.11.min.js" defer></script>
<body hx-history="false" hx-headers='{"X-Ostro-Csrf": "{{ csrf }}"}'>
```
(`hx-headers` with a static JSON object is not a `js:` value, so it stays allowed with `allowEval:false`.)

## State of the Art

| Old approach | Current approach | Impact |
|---|---|---|
| Temporal "reset" terminates the run and copies history up to an event id into a new execution | Same model: rerun = a new run with a copied prefix (non-terminal source refused in v1) | Familiar semantics. Ostro adds a mandatory pre-submit replay check and a cut-safety analysis. |
| SPA dashboards built with npm | SSR + htmx for embedded admin UIs | No JS build chain. Rust-only tests. |
| htmx 1.x | htmx 2.0.11 (stable); 4.x is `next` | 2.x has `selfRequestsOnly` on by default. Stay on 2.x. |
| `askama_axum` integration crate | askama 0.16 `Template::render()` → `axum::response::Html` | No extra integration crate. |

## Proposed RESERVATIONS block (PF-11-3)

| Space | Allocation | Item |
|---|---|---|
| Cmd tags | 43 | `DlqUpdate` (44–47 spare, P11) |
| StartRun fields | 40 | `rerun_prefix` (repeated JournalEvent) |
| | 41 | `rerun_from_seq` (uint64) |
| | 42–43 | spare P11 |
| Other message fields | `JournalEvent.4` | `attempt` (fixed64, submitter-set; 0 = apply-originated) |
| InvalidCode | 29 | `BadRerunPrefix` |
| | 30 | `BadDlqUpdate` |
| | 31 | spare |
| RejectReason / intents / journal kinds / TerminalStatus / outbox / meta variants / MetaCommand | — | **0** |
| Keyspaces (TABLES) | — | **0**. Derived (non-digest) keyspace name `x_run_index` (PF-11-2 rule) |
| RunRecord | — | `rerun_from_seq: Option<u64>` only if PF-11-4 lands pre-freeze |
| Meta ext tags | 144 | `OnFailure` (WorkflowPolicy, QueuePolicy) |
| | 145 | `DlqPolicy{enabled, hook_catchup_ms}` (spare use) |
| | 146–159 | spare P11 |

Name rows:
- crate `ostro-ui`; dirs `crates/ostro-ui/{templates,assets}`, `<data>/bulk/`.
- REST: `/ui/*`, `GET /v1/dlq`, `POST /v1/dlq/{run}/requeue|dismiss`, `POST /v1/runs/{id}/rerun`, `GET|POST /v1/bulk`, `GET /v1/bulk/{id}`, `POST /v1/bulk/{id}/cancel`, `GET /v1/modules` (list), `GET /v1/queues` (list), `GET /v1/partitions`, `GET /v1/limits`.
- gRPC: `ostro.api.v1.RunOps`, `ostro.api.v1.Inventory`.
- Dedupe namespaces: `__rerun/`, `__onfail/`, `__bulk/`, `__dlq/`. System tag prefix `__ostro/` (API rejects user tags with it).
- Config: `[limits]`, `[ui]`, `[bulk]`, `[dlq]`.
- Metric prefixes: `ostro_bulk_`, `ostro_rerun_`, `ostro_ui_` (`ostro_dlq_` is shared with P8).
- Failpoint prefixes: `bulk::`, `rerun::`.
- Id prefix: `blk_`.

## Proposed plan breakdown (16 plans, 7 waves)

| Plan | Wave | Depends on | Content | Reqs |
|---|---|---|---|---|
| **11-01** PHASE-11 GATE | 1 | — | Gate checks:<br>- Phases 7 **and** 8 complete (SUMMARYs + VERIFICATION passed or human_needed by design);<br>- Phase 5 freeze present, with PF-11-1/2/4 status recorded;<br>- code vs RESERVATIONS; allocate the P11 block if 05-20 did not.<br>Decisions D-P11-1..13 to STATE; the shared-file table with Phases 9/10/14; Phase 10 F6 protocol. Docs only. | all |
| **11-02** Contract | 2 | 01 | - ostro-types: StartRun 40/41, Cmd 43, `JournalEvent.4`, InvalidCode 29/30, goldens appended.<br>- Apply dispatch stubs for Cmd 43 (`NotImplemented`).<br>- **Cargo files (only plan):** askama, the `ostro-ui` crate skeleton.<br>- `[limits]/[ui]/[bulk]/[dlq]` config stubs.<br>- `phase11.rs` trait stubs with default `Unimplemented` + `ApiExtensions.ops` slot.<br>- `control/p11_*.rs` skeletons + `impl OpsApi` delegation.<br>- services seam registrations (stubs).<br>- Vendor htmx + VENDORED.md row.<br>- forbidden-crate / deny green. | all |
| **11-03** Derived run index | 3 | 02 | `store/derived.rs`, `apply/index.rs` hooks (run.rs, terminal.rs, lifecycle.rs, retention.rs; minimal hunks), rebuild, install drop, `query::index_page`, proptest | OPS-05 |
| **11-04** Limits enforcement | 3 | 02 | Engine `limits.rs` (live-only), `AttemptFailure::LimitExceeded` + Phase 7 `WireOutcome` mirror, executor mapping, `[limits]` config, `docs/limits.md`, limit test guests (`guests/p11-limits`) | OPS-07 |
| **11-06** DLQ apply | 3 | 02 | `apply/dlq.rs` (Cmd 43), `DlqRow` value, storage-format "Phase 11 additions", rebuild_digest generator `gen_dlq` | OPS-04 |
| **11-07** API surface (single owner of ostro-api/proto/OpenAPI) | 3 | 02 | Domain DTOs; REST + gRPC for rerun, DLQ, bulk, search extensions (`tags[]`, `order`, `dlq`), inventory (modules/queues/partitions), limits; API-side limit validation + uniform details; system-tag rejection; Fake impls; parity; OpenAPI re-bless (`/` → 303) | OPS-02..07, UI-03 |
| **11-05** Rerun partition + analyzer + re-arm | 4 | 03, 07 | `apply/rerun.rs` (+ one call in run.rs, after 11-03), cut-safety analyzer in `ostro-server/src/rerun.rs`, executor re-arm trigger (`executor.rs` minimal hunk), kill test | OPS-02 |
| **11-09** Search + inventory server | 4 | 03, 07 | `control/p11_search.rs` (index-backed, fallback scan), `control/p11_inventory.rs` (modules, queues merged with Phase 7 flow/demand, partitions health from Node handles) | OPS-05, UI-03 |
| **11-10** DLQ server + `on_failure` | 4 | 03, 06, 07 | `control/p11_dlq.rs` (list/requeue/dismiss), `services/dlq_hook.rs`, ext tag 144 codec, loop guard, startup re-drive, metrics | OPS-04 |
| **11-13** UI foundation | 4 | 07 | `ostro-ui`: layout, assets, security guard, auth seam, runs list (filters/tags/DLQ tab), run detail (published status, lineage), timeline + replay markers, logs tab (Phase 8); insta + lint tests vs Fake | UI-01 |
| **11-08** Rerun control plane | 5 | 05, 07 | `control/p11_rerun.rs`: prefix read, envelope fitting, version selection, mandatory `replay_check` (reuse Phase 10's or move 05-12's lib; owner = first lander), idempotency, dry-run | OPS-02 |
| **11-14** UI actions | 5 | 13 | cancel/terminate/retry/signal/pause-resume (run/workflow/queue), rerun-from-step with dry-run preview, DLQ requeue/dismiss, bulk create/progress/cancel (against traits) | UI-02 |
| **11-11** Bulk operations | 6 | 08, 09, 10 | `services/bulk.rs` + `control/p11_bulk.rs`: job model, snapshot bound, GCRA, backpressure, checkpoint file, idempotent per-run ops, `bulk::` failpoints | OPS-03 |
| **11-12** CLI verbs | 6 | 08, 09, 10 | `ostro rerun`, `ostro dlq ls/requeue/dismiss`, `ostro bulk …`, `ostro ls --tag/--newest`, `ostro limits`; CLI-06 output contract; Phase 10 coordination (new modules + one dispatch arm; build on the latest `ostro-client` shape) | OPS-02..05, OPS-07 |
| **11-15** UI inventory views | 6 | 14 (`ui/mod.rs` order) | modules, workflows/versions, queues (+pause/resume), schedules (+pause/resume/trigger), workers (+drain), partitions health | UI-03 |
| **11-16** E2E + PHASE GATE | 7 | all | Spawned-binary suites SC1–SC5 (`ops_e2e`, `ui_e2e`), kill tests (rerun re-arm, hook exactly-once, bulk resume), replay-corpus entries recorded from reruns, docs (`operations.md`, `ui.md`), binary size vs NFR-2.1, full `ci-local.sh` incl. musl, VALIDATION + human_needed browser checklist | all |

**Why 11-05 is in wave 4:** both 11-03 and 11-05 edit `apply/run.rs` (StartRun), so they are serialized. **Why 11-15 follows 11-14:** both register routes in `ostro-ui/src/lib.rs`.

## Cross-phase file coordination

Use the Phase 7/8/9 protocol for every row:
- `git status --porcelain -- <file>` must be empty before the first edit;
- record the base sha; append only, with minimal hunks;
- if the file is dirty, STOP and report;
- re-apply the hunk if HEAD moved;
- commit through a private index.

| File | Phase 11 owner | Other phases | Rule |
|---|---|---|---|
| `Cargo.toml`, `Cargo.lock`, member manifests, `deny.toml` | **11-02 only** | 09-02; 10 (new crates `ostro-build`, `ostro-client`) | Never concurrently with 09-02 or Phase 10's Cargo plan. |
| `scripts/ci-local.sh`, `.github/workflows/ci.yml` | 11-02, 11-16 | 09-02/17/20; Phase 10 | Append steps; keep `FAILPOINT_CRATES` in sync. |
| `crates/ostro-types/src/command.rs` + goldens | 11-02 | 09 (read-only); 10 (none, F7) | Append by tag. |
| `crates/ostro-partition/src/apply/{run,terminal,lifecycle,retention}.rs` | 11-03 (index hooks), 11-05 (run.rs call) | **09-08, 09-13** (run.rs guard, terminal parent-close, retention deferred-EXPIRE); P7/P8 already landed | Index removal sits at the real delete point *after* Phase 9's deferral branch. One call line per site. |
| `crates/ostro-partition/src/store/{mod,batch}.rs` | 11-03 | — (P8 08-02 install path) | Derived handle added, `TABLES` untouched. Install drops `x_run_index` (one hunk in 08-02's install code). |
| `crates/ostro-engine/src/{config,runtime}.rs`, `host/*` | 11-04 (new `limits.rs` + config fields + check calls) | **09-07** (config.rs, runtime.rs core budget); 08-11/16/17 (landed) | Never concurrently with 09-07. `activation.rs` minimal. |
| `crates/ostro-worker-proto` `WireOutcome` | 11-04 | P7 (owner, landed) | Append one variant last. |
| `crates/ostro-server/src/executor.rs` | 11-05 (re-arm trigger), 11-04 (LimitExceeded mapping) | P9: none; P10: none expected | Waves 3/4 are distinct, so no overlap. Never move the file. |
| `crates/ostro-server/src/sink.rs` | 11-05 (`JournalEvent.attempt`) | **09-05/08/13** | Minimal hunk, clean HEAD. |
| `crates/ostro-server/src/derive.rs` | none (read-only use of `rearm_unresolved`) | 09-05/08/13 | The analyzer lives in the new `rerun.rs`. |
| `crates/ostro-server/src/{server,config,lib}.rs` | 11-02 only (one seam block, config sections, `pub mod` lines) | 09-05/07 (lib.rs, config.rs lines); 10 (`devclock`, `dev_api`, `replay` modules; `[dev]`) | Append only. |
| `crates/ostro-server/src/replay.rs` (`replay_check`) | 11-08 if absent | **10 (CLI-07 owner)** | Whoever lands first creates it. The other reuses it unchanged or STOPs and reports. |
| `crates/ostro-server/src/control/mod.rs` | 11-02 (module lines + `impl OpsApi` delegation) | 10 (replay/dev routes) | Append only. |
| `crates/ostro-api/src/**`, `ostro-api/tests/{parity,openapi}.rs` + snapshot, `ostro-proto/{build.rs,src/lib.rs,proto/**}` | **11-07 only** (11-02 stubs) | **10** (`/v1/modules/{sha}/replay`, `/v1/dev/time*`, `ostro-client` extraction) | Never concurrently with Phase 10's API plan. Re-bless OpenAPI with added lines only (plus `/` → 303). |
| `crates/ostro/src/main.rs`, `crates/ostro-cli/src/**` | 11-12 | **10** (heavy restructure; 10-05 extracts `client.rs` → `ostro-client`) | New modules + one dispatch arm. If Phase 11 starts first, 10-05 goes last among CLI edits (10-RESEARCH F6). Otherwise 11-12 builds on `ostro-client`. |
| `crates/ostro-server/tests/common/mod.rs` | none | 07/08/09/10 | P11 helpers in `tests/common/p11_*.rs`. |
| `guests/*` | 11-04 (`p11-limits`), 11-05/16 (`p11-rerun`, `p11-rerun-v2`) | 09 (`child-*`, `conf-*`, …) | Distinct names; `members = ["*"]`. |
| `docs/storage-format.md` | 11-03, 11-06 (append "Phase 11 additions") | 09 (none) | Append section. |
| `.planning/RESERVATIONS.md`, `STATE.md` | 11-01, 11-16 | 09-01/20, 10-01 | Docs-only commits, named sections only. |
| Phase 8 services seam `services/mod.rs` | 11-02 (two registration lines) | P8 (landed) | Append only. |

**Phase 14 notes (record in STATE):**
- `UiAuth` and `UiPrincipal` are the seam.
- Prefer a request-extension `Principal`, so both REST and UI handlers pass it to `ControlPlane` uniformly.
- Reuse `x_run_index` prefix `0x04` for the tenant listing.
- Scopes: views need `read`; actions, bulk, rerun and DLQ need `operate`.
- Audit lives at the ControlPlane level, so UI actions are covered.

**Phase 12/13 notes:**
- Followers maintain `x_run_index` through the same apply path.
- `/v1/partitions` gains ISR fields additively.
- Bulk jobs stay node-local until Phase 13 forwarding.
- Rerun reads the source journal through the same forwarding.

## Open Questions

1. **Rerun prefix larger than one envelope.**
   - Known: 4 MiB envelope; about 60 B per offloaded event.
   - Unclear: whether users will rerun journals near 50k events with many small inline payloads.
   - Recommendation: v1 fails with `413 rerun_prefix_too_large`. A post-1.0 option is a staging state with chunked `RerunAppend` commands (Cmd 44, spare).
2. **PF-11-4 adoption.** If 05-20 has already run, use the system-tag fallback (`__ostro/rerun_from=<seq>`). It works with zero format change.
3. **Default payload cap (4 MiB vs 2 MiB).** 05-15 tests 1 MiB payloads, and `[api] max_request_bytes` is 4 MiB. Recommend 4 MiB, configurable.
4. **askama feature names / syn 3.** Verify in 11-02 Task 1 (`cargo tree -d`, `cargo deny check`). Fallback: maud 0.27.0.
5. **Partition health fields available today.** Confirm in 11-09 what `Node`/`PartitionHandle` expose: role, epoch, LEO, HW, applied, health, ready/running counts. Expose only what exists.
6. **Running-run rerun ("terminate and rerun").** v1 refuses it. A bulk action `terminate+rerun` could chain both later.

## Validation Architecture

### Test Framework
| Property | Value |
|---|---|
| Framework | Rust `cargo test` (built-in harness, tokio tests), insta 1 snapshots, proptest (in-tree), failpoints (`--features failpoints`, single-threaded) |
| Config file | none; `scripts/ci-local.sh` is the gate |
| Quick run command | `cargo test -p <crate> --test <suite> --locked` (each < 30 s, except e2e) |
| Full suite command | `bash scripts/ci-local.sh` (with `--no-musl` for intermediate waves) |

### Phase Requirements → Test Map
| Req ID | Behavior | Type | Automated command | File exists? |
|---|---|---|---|---|
| OPS-02 | Apply journals prefix verbatim; rejects bad seqs/kinds/terminal kinds | integration | `cargo test -p ostro-partition --test apply_rerun --locked` | ❌ Wave 0 |
| OPS-02 | Analyzer rejects open child/effect/token/unmatched signal; reports `nearest_safe_seq` | unit | `cargo test -p ostro-server --lib rerun:: --locked` | ❌ |
| OPS-02 | v1 fails at step 3 → deploy v2 → rerun from step 3 completes; prefix bytes identical; steps 1–2 not re-executed (mock effect count); run_id-in-body guest → 409 `rerun_nondeterministic` | integration | `cargo test -p ostro-server --test rerun_api --locked` | ❌ |
| OPS-02 | Kill between grant and re-arm → open timer still fires | failpoint | `cargo test -p ostro-server --features failpoints --test rerun_api -- --test-threads=1` | ❌ |
| OPS-03 | Spacing ≥ 1/rate (paused time); backlog pause; 429 honoured; snapshot bound (rerun of 50 → exactly 50 new); restart resumes from checkpoint with no duplicate | integration | `cargo test -p ostro-server --test bulk_ops --locked` | ❌ |
| OPS-04 | DlqUpdate apply + retention; digest/rebuild unaffected for old logs | integration/proptest | `cargo test -p ostro-partition --test apply_dlq --test rebuild_digest --locked` | ❌ / ✅ (gen appended) |
| OPS-04 | Failed → DLQ with reason; hook exactly once across kill; loop guard; requeue resume/restart/rerun; dismiss | integration | `cargo test -p ostro-server --test dlq_api --locked` | ❌ |
| OPS-05 | Index == scan (proptest), rebuild after marker loss, install drop, newest-first, AND tags, bounded pages | proptest/integration | `cargo test -p ostro-partition --test run_index --locked`; `cargo test -p ostro-server --test search_api --locked` | ❌ |
| OPS-07 | Each limit → documented error (API: 400/413 + details; engine: Failed `limit_exceeded`, resumable); replay of an over-limit journal still green | integration | `cargo test -p ostro-engine --test limits --locked`; `cargo test -p ostro-server --test limits_api --locked` | ❌ |
| OPS-07 | `docs/limits.md` table == `GET /v1/limits` == constants | unit | `cargo test -p ostro-server --test limits_doc --locked` | ❌ |
| UI-01 | Runs list filters/tags/DLQ tab; detail with published status; timeline replay markers (exact + approximate); escaping of hostile tags | integration (Fake) | `cargo test -p ostro-ui --test pages --locked` | ❌ |
| UI-02 | Each action calls the trait and returns fragment (htmx) or 303 (no-JS); CSRF negatives (cross-site, missing HX/token, bad Host) → 403 | integration (Fake) | `cargo test -p ostro-ui --test actions --test security --locked` | ❌ |
| UI-03 | Modules, versions, queues, schedules, workers and partitions pages render with Fake data; "not available" when an extension slot is None | integration | `cargo test -p ostro-ui --test inventory --locked` | ❌ |
| UI-* | CSP/security headers on every `/ui` route; template lint (no inline script/style/`on*`/`hx-on`); htmx sha256 == VENDORED.md; asset ETag/content-type | unit | `cargo test -p ostro-ui --test hardening --locked` | ❌ |
| SC1–5 | Spawned binary: DLQ + hook + requeue + rerun; bulk over tag; limits; UI pages and actions over HTTP on real runs | e2e | `cargo test -p ostro --test ops_e2e --test ui_e2e --locked` | ❌ |
| UI visual | Real browser render (Firefox/Chrome), keyboard nav, htmx swaps, no CSP console errors | manual | human_needed checklist in 11-VALIDATION.md (Playwright needs Node: out of scope) | — |

### Sampling Rate
- **Per task commit:** the plan's own `cargo test -p <crate> --test <suite> --locked`, plus `cargo clippy -p <crate> --all-targets -- -D warnings`.
- **Per wave merge:** `cargo test --workspace --locked` + failpoint suites for touched crates + `cargo deny check`.
- **Phase gate:** full `bash scripts/ci-local.sh` (incl. musl), green before `/gsd:verify-work`. Binary size recorded against NFR-2.1.

### Wave 0 Gaps
- [ ] `crates/ostro-partition/tests/{apply_rerun,apply_dlq,run_index}.rs` + `tests/common/gen_dlq.rs` (append to rebuild_digest)
- [ ] `crates/ostro-server/tests/{rerun_api,bulk_ops,dlq_api,search_api,limits_api,limits_doc}.rs` + `tests/common/p11_*.rs`
- [ ] `crates/ostro-engine/tests/limits.rs`
- [ ] `crates/ostro-ui/tests/{pages,actions,security,inventory,hardening}.rs` (new crate)
- [ ] `crates/ostro/tests/{ops_e2e,ui_e2e}.rs`
- [ ] Guests: `guests/p11-limits`, `guests/p11-rerun` (fails at step 3 by input flag; one variant with run_id in an HTTP body), `guests/p11-rerun-v2`
- [ ] Framework install: none (insta and proptest already present; askama added by 11-02)

## Sources

### Primary (HIGH confidence)
- Repository at `a6197c1`:
  - `crates/ostro-api/src/{lib,rest,domain}.rs` (UI shell at `/`, router, ListRunsRequest, limits)
  - `crates/ostro-server/src/{control/list.rs, derive.rs, executor.rs, run_started.rs}` (scan budget, `rearm_unresolved`, in-memory re-arm marker, RunStarted construction)
  - `crates/ostro-partition/src/{store/mod.rs, store/values.rs, apply/run.rs, apply/retention.rs}` (TABLES/digest, RunRecord fields, `store_run`, DLQ prefix delete)
  - `crates/ostro-types/src/{journal.rs, command.rs, ids.rs}` (kinds, `CommandArgs` hashing, StartRun fields, RunId = partition ‖ uuid v7)
  - `crates/ostro-engine/src/{replay/token.rs, host/effects.rs, replay/cursor.rs}` (journaled token on replay, logical clock base)
  - `wit/ostro-runtime-0.1.0/runtime.wit` (reserved error kinds)
- `.planning/{RESERVATIONS,STATE,ROADMAP,REQUIREMENTS,PROJECT}.md`; `05-DECISIONS.md`, `05-20-PLAN.md`; `07-DECISIONS.md`, `07-11/07-14/07-08-PLAN.md`; `08-DECISIONS.md`, `08-10/08-16-PLAN.md`; `09-DECISIONS.md`; `10-RESEARCH.md` (HEAD); `12-RESEARCH.md`; `14-RESEARCH.md`; `docs/02-prd.md`, `docs/03-architecture.md`, `docs/storage-format.md`
- crates.io API: askama 0.16.1 (2026-09-04) and deps of askama / askama_derive 0.16.1 (syn ^3); maud 0.27.0 (2025-02-02) and deps; maud CHANGELOG (axum 0.8 via axum-core 0.5)
- npm registry: htmx.org dist-tags (`latest` 2.0.11, `next` 4.0.2); 2.0.11 licence 0BSD, integrity sha512
- htmx docs: https://htmx.org/reference/ (config options) and https://htmx.org/docs/#security (allowEval, hx-history, CSP, CSRF)

### Secondary (MEDIUM confidence)
- Temporal Cloud limits (https://docs.temporal.io/cloud/limits): 51,200 events / 50 MB, warn at 10,240 / 10 MB, 2 MB payload, 2,000 pending ops per type
- Temporal reset semantics (search snippets of docs.temporal.io cancellation/reset pages): terminate the current run and copy history up to the reset point into a new execution

### Tertiary (LOW confidence; validate in plans)
- askama 0.16 feature names (`derive`, `std`, `config`): check in 11-02.
- htmx `historyCacheSize: 0` fully disables the snapshot cache in 2.0.11 (`hx-history="false"` is the documented control and is applied too).

## Metadata

**Confidence breakdown:**
- Freeze analysis / pre-freeze items: HIGH. Based on the RESERVATIONS rules, D18/D20/D21 and the code.
- Rerun determinism: HIGH for the mechanism (code-verified hashing and token replay). MEDIUM for envelope sizing.
- UI stack: MEDIUM. Versions and licences are verified; there is no prototype.
- Bulk / index / DLQ designs: MEDIUM. New designs composed from verified primitives.
- Pitfalls: HIGH for CSP/CSRF/htmx, which are documented. MEDIUM for perf effects.

**Research date:** 2026-11-19
**Valid until:** 2026-12-19, or earlier if 05-20 runs first. Then re-check PF-11-1/2/4 status and use the documented fallbacks.
