---
phase: 11-run-operations-embedded-ui
plan: 10
subsystem: rerun control plane / engine replay library
tags: [rerun, OPS-02, replay_check, envelope, idempotency, dlq]
requires:
  - 11-07 (rerun apply, analyzer, re-arm, guests p11-rerun{,-v2})
  - 11-09 (REST/gRPC rerun + rerun-cuts routes)
  - 11-12 (DLQ requeue mode rerun handoff)
provides:
  - ostro_engine::replay_check::{replay_check, ReplayVerdict, journaled_outcome} (created here; Phase 10 reuses it)
  - control::p11_rerun::{rerun_run, analyze_rerun, MAX_RERUN_PREFIX_BYTES, set_prefix_bound_for_source (test hook)}
  - RunStarted history details carry retry_of (UI source link)
affects: [10-11, 11-14, 11-16, 11-18]
tech-stack:
  added: []
  patterns: ["replay check fences the first live append (nothing runs past the frontier)", "conservative-estimate offloading with an exact final encode"]
key-files:
  created:
    - crates/ostro-engine/src/replay_check.rs
    - crates/ostro-server/tests/rerun_api.rs
  modified:
    - crates/ostro-engine/src/lib.rs
    - crates/ostro-engine/tests/replay_corpus.rs
    - crates/ostro-server/src/control/p11_rerun.rs
    - crates/ostro-server/src/control/history.rs
    - crates/ostro-server/src/control/p11_inventory.rs
    - crates/ostro-server/tests/limits_doc.rs
    - docs/limits.md
decisions:
  - "replay_check was created by this plan (875b042): Phase 10 had not landed it. 4 arguments exactly; module type = ostro_engine::runtime::Prepared (what 05-12 uses)"
  - "The check's private sink refuses (Fenced) the first append past the journal: OkFrontier{appended} is the size of that refused batch; no effect driver, no blobs, guest logs dropped"
  - "Non-terminal journal: every outcome other than NonDeterminism / ProtocolViolation is OkFrontier (live behaviour past the prefix, not a replay divergence)"
  - "Run id: keyed reruns route by partition_for_key(workflow, __rerun/<source>/<key>); an existing dedupe row's run id is reused (check runs as that id, answer existing: true without submitting); else a fresh uuid v7 (partition_for_new_run without a key), as starts do"
  - "DLQ requeue passes __dlq/<run>/<n> as idempotency key; it is wrapped uniformly to __rerun/<source>/__dlq/<run>/<n>"
  - "Retention / schedule-to-start of a rerun come from the current policy (workflow, queue, [runs]), not the source's per-request values"
  - "Seq 0 is never offloaded (verbatim); only copied events 1..cut are candidates"
metrics:
  duration: "~2h"
  completed: 2026-11-22
---

# Phase 11 Plan 10: Rerun control plane Summary

`POST /v1/runs/{id}/rerun` works end to end. It resolves the cut, checks that the cut is safe and picks the target version. It then pre-allocates the run id and replays `[0, cut)` against the target module as that new id (the replay check is mandatory). Finally it fits the envelope and submits the copied prefix idempotently. `GET /v1/runs/{id}/rerun-cuts` lists every op with its safety. The DLQ `mode: rerun` requeue now completes.

## Commits

| Task | Commit | Description |
|------|--------|-------------|
| 1 | 875b042 | `feat(11-10)`: ostro_engine::replay_check library (shared with Phase 10) |
| 2 | ad31959 | `feat(11-10)`: rerun control plane with mandatory replay check (plus limits row, history retry_of) |

## replay_check (who created it)

Phase 10's 10-11 had not landed, so this plan created `crates/ostro-engine/src/replay_check.rs` (875b042). The shape is exactly D-P10-7 / D-P11-5: `replay_check(rt: &Runtime, module: &Prepared, run: RunRef, journal: &[Event]) -> ReplayVerdict {Ok{appended}, OkFrontier{appended}, Diverged{seq, detail}, Error(String)}`. `lib.rs` got one line. `tests/replay_corpus.rs` keeps corpus loading and now runs every entry through the library: 35 entries, 417 events, all `Ok{appended: 0}`. A new verdict test covers three cases:
- every proper prefix of `det-basic` is `OkFrontier`;
- a flipped command hash in the first `StepCompleted` gives `Diverged` at that seq, on both the terminal journal and a truncated one;
- malformed journals give `Error`.

No `src/replay_corpus.rs` was created; that module belongs to 10-11.

## Run-id allocation rule

- With an idempotency key, the dedupe key is `__rerun/<source>/<key>` and the partition is `partition_for_key(workflow, dedupe)`.
  - If the dedupe row exists, its run id is used: the replay check runs as that id and the answer is `existing: true`, with nothing submitted.
  - Otherwise the id is `RunId::new(p, uuid v7)`.
  - A race lost at apply returns `Duplicate` and is also answered `existing: true`.
- Without a key, the partition is `partition_for_new_run(uuid v7)`, the same as a plain start.

## Envelope fitting measurements

`envelope_offloads_large_payloads_or_refuses` uses a `long-journal` source with 3,000 steps and a 200,000-byte inline step output (3,002 events). The rerun cuts at the terminal event.

| Bound | Prefix rows | Payloads offloaded | Result |
|---|---|---|---|
| Default 3.5 MiB | 430,043 B, all inline | 0 | 201, completes |
| 330,043 B (test hook) | 230,015 B | 1 (the 200 KB step) | 201, completes |
| 115,007 B | — | — | 413 `rerun_prefix_too_large`, `limit: rerun_prefix_bytes` |

With the 330,043 B bound, the copied rows match the source on `seq`, `kind`, resolved payload bytes and `attempt` (W15). Prefixes longer than `max_history_events` are also 413 (`limit: history_events`); that check is in the code but has no e2e test.

`MAX_RERUN_PREFIX_BYTES = 3670016` is published in `docs/limits.md` and `GET /v1/limits` as row 14. `limits_doc` was updated to 14 rows.

## MockProvider evidence (re-executed vs copied effects)

`nondeterministic_prefix_unsafe_cut_and_reexecuted_effect` runs v1 with `{fail_at: 4, effect_url: <mock>/echo, put_run_id_in_body: true}`:

1. The source sends 1 `/echo` request; its `EffectScheduled` body carries the source run id.
2. A rerun from `s3` (after the effect) is 409 `rerun_nondeterministic` with `seq` = the `EffectScheduled` seq. A dry run of the same rerun is also 409, and the run count does not change.
3. `from_seq = eff + 1` is 409 `rerun_cut_unsafe`, `detail: open_effect`, `nearest_safe_seq = eff`.
4. A rerun from `eff` (the nearest safe cut) is 201. The mock then counts **2** `/echo` requests: the effect after the cut re-executed. The new run's `EffectScheduled` body carries the **new** run id, and `s1` appears once (copied, not re-run).

## Other coverage (tests/rerun_api.rs, 5 tests)

- **Fix and rerun.** v1 fails at s3. v2 is deployed and activated, and a rerun from step s3 completes with 5.
  - Rows `0..cut` are byte-identical to the source; s1..s5 appear once each.
  - `retry_of` is the source, the tags are `[team:a, x, __ostro/rerun_from=<cut>]`, and the run has no parent.
- **Versions.** `version: source` runs v1 and fails at s3 again. `{version: 2}` completes. `{version: 99}` is 404.
- **Dry run** returns `new_run_id` absent and verdict `ok_frontier`, and submits nothing.
- **Idempotency key** (`__rerun/<src>/k1`): 201, then 200 with `existing: true` and the same id.
- **Refusals:**
  - RunFailed+1 cut → 409 `terminal_or_out_of_range`.
  - Unknown step → 400.
  - Running source → 409 `run_not_terminal`.
  - 64 user tags → 400 `limit: tags`, max 64, actual 65.
  - Unknown source → 404.
- **rerun-cuts** on the completed rerun lists s1..s5, and each `safe` flag equals `analyze_cut`.
- **Blobs (D-P11-27).** The source has a 300 KB input stored as a blob and `retention: 3s`.
  - After the source expired, a `BlobGc` pass with grace 0 deleted an orphan blob and kept the input blob the rerun shares.
- **DLQ requeue `{mode: {rerun: {from: {step: {name: s3}}}}}`** creates a new run that completes on v2.
  - The dedupe key is `__rerun/<src>/__dlq/<src>/1`, and `requeued_to` is set.
  - A repeat returns the same run, and the entry leaves the default view.

## Coordinator request: retry_of / status_seq for the UI (11-15)

- **retry_of: done, server-side only.** The `run_started` event of `GET /v1/runs/{id}/history` now carries `details.retry_of` (from `RunRecord.retry_of`) for restarts and reruns. The key is omitted for ordinary runs, so existing history snapshots are unchanged. 11-15's `pages.rs::retry_of` reads exactly this key. The rerun e2e test asserts it.
- **Not done: `retry_of` / `status_seq` on `RunInfo`.** That needs a domain + REST + gRPC field (a new `ostro.api.v1` proto field number) plus an OpenAPI re-bless. The phase rules say ordinals come only from RESERVATIONS, and Phase 10 also edits the API surface, so I left it for a reserved follow-up. The OpenAPI snapshot is untouched (no API types changed).

## Deviations from Plan

1. **Unsafe-cut scenario uses an open effect, not an await token.** `p11-rerun` creates no await tokens, and guests are outside this plan. The analyzer path is the same (`open_effect` vs `open_await_token`); 11-07's unit tests cover the await-token reason.
2. **Envelope test uses the test hook and a 200 KB inline payload, not 3,000 × 2 KiB events.** No existing guest produces thousands of 2 KiB events. A 3,000-step source still exercises scale. The hook, `set_prefix_bound_for_source`, is per source so it is safe in parallel tests; it drives both the offload path and the 413 path.
3. **"Deliberately divergent module" in Task 1 is a mutated journal** (a flipped command hash). That gives the same `NonDeterminism` path without a new guest.
4. **Extra files beyond `files_modified`, by coordinator instruction:**
   - `docs/limits.md`, `control/p11_inventory.rs` and `tests/limits_doc.rs`: the rerun-prefix cap row.
   - `control/history.rs`: the `retry_of` detail.
5. **Rate limiting.** A rerun goes through the StartRun rate-limit admission (`flow::rate_limit::admit`), as API starts and hook starts do.
6. **Workflow per the executor brief.** I used the brief's worktree, slot-2 target, and moved main by CAS, not `/tmp/ostro-wt-11-10` with a private index.

## Tests run

- `cargo test -p ostro-engine --test replay_corpus`: 6 passed. Lib `replay_check`: 4 passed.
- `cargo clippy -p ostro-engine -p ostro-server --all-targets -D warnings`: clean, also with `--features failpoints`. `cargo fmt --check`: clean. `cargo check --workspace --all-targets`: clean after rebasing onto 11-15.
- `cargo test -p ostro-server --no-fail-fast` (full, before the history edit): 64 binaries ok. The only failure was `rerun_api`, from a source edit mid-build: the history assertion was compiled against an older lib.
- After the rebase: `rerun_api` 5, `dlq_api` 10, `limits_doc` 2, `runs_api` 2, `search_api` 1, `semantics_e2e` 18, `sdk_e2e` 9, `lifecycle_api` 13, ostro-server lib 158; ostro-ui all green.
- Clock check: no new `SystemTime::now` / `Instant::now` / `SystemClock` in p11_rerun.rs.
- Not run: `scripts/ci-local.sh --no-musl` (whole-workspace gate; skipped to spare the shared machine).

## Open items

- `RunInfo.retry_of` / `status_seq` need reserved proto field numbers (see above).
- 10-11: reuse `ostro_engine::replay_check` unchanged; do not re-create it.
- The `history_events` 413 has no e2e test.

## Self-Check: PASSED
- FOUND: crates/ostro-engine/src/replay_check.rs, crates/ostro-server/tests/rerun_api.rs
- FOUND: 875b042, ad31959 on main
