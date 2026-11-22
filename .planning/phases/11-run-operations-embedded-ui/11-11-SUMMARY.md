---
phase: 11-run-operations-embedded-ui
plan: 11
subsystem: server controllers (search, inventory, limits, history)
tags: [search, run-index, inventory, limits, ops-05, ops-07, ui-03]
requires:
  - 11-04 (P11Ctx, ServerControlPlane::p11(), p11_search / p11_inventory stubs)
  - 11-05 (query::index_page / index_state, store::derived prefixes)
  - 11-06 (docs/limits.md, RunLimits)
  - 11-09 (ListRunsRequest.{tags, order, dlq}, HistoryEvent.attempt, payload cap, routes)
provides:
  - p11_search::list_runs_indexed (+ list_runs_indexed_with_budget test hook)
  - p11_inventory::{limits, list_modules, list_queues, list_partitions}
  - list::list_runs_scan (scan with an extra predicate) and tags-AND / dlq in list::matches
  - history::{attempt_of, event_seq_and_attempt}
affects: [11-12 (dlq membership plugs into the search predicate), 11-15/11-16/11-17 (UI pages read these endpoints), 11-18 (operations docs, order-fallback header)]
tech-stack:
  added: []
  patterns:
    - "k-way merge over (prefix, partition) index streams with one global position token"
    - "hand-rolled protobuf field read for JournalEvent.attempt (ostro-server has no prost dependency)"
key-files:
  created:
    - crates/ostro-server/tests/search_api.rs
    - crates/ostro-server/tests/inventory_api.rs
    - crates/ostro-server/tests/limits_api.rs
    - crates/ostro-server/tests/limits_doc.rs
  modified:
    - crates/ostro-server/src/control/p11_search.rs
    - crates/ostro-server/src/control/p11_inventory.rs
    - crates/ostro-server/src/control/list.rs
    - crates/ostro-server/src/control/history.rs
    - crates/ostro-server/src/control/runs.rs
decisions:
  - "The index serves only order=newest. Oldest-order requests (the default) stay on the scan, including multi-tag ones (tags AND is now in list::matches) and dlq=true (scan + is_dlq_member). D-P11-10 says the default order is unchanged, and the index cannot serve oldest first"
  - "Token i1.<kind>.<created_at_ms>.<run id>, kind t/w/d/s/a; one position resumes every (prefix, partition) stream, including the two DLQ status prefixes"
  - "Rebuilding index: a newest request without a token (or with a c1. scan token) gets the scan fallback in oldest order with scan tokens; an i1. token while rebuilding is 503 unavailable, details.reason index_rebuilding"
  - "GET /v1/limits serves the 13 docs/limits.md rows in doc order; docs/limits.md unchanged"
  - "StartRun tag caps now use phase11::limit_error (limit tags / tag_bytes) so the 65-tag case carries details.limit on both transports (runs.rs, outside files_modified)"
metrics:
  duration: "~1h30m"
  completed: 2026-11-22
---

# Phase 11 Plan 11: Indexed search, inventory and limits Summary

`GET /v1/runs?order=newest` is now served from `x_run_index`. Each request is a k-way merge over one stream per (prefix, partition), newest first, with `i1.` page tokens and the `list::SCAN_BUDGET` budget. While a partition's index is rebuilding, the same filters fall back to the budgeted scan. The four inventory and limits endpoints return real server data, and `docs/limits.md`, `GET /v1/limits` and the code constants are test-locked together. History events carry the stored `attempt` (0 becomes absent).

## Commits

| Task | Commit | Description |
|------|--------|-------------|
| 1 | 727ad08 | `feat(11-11)`: indexed run search with scan fallback and history attempts |
| 2 | b46f5a1 | `feat(11-11)`: inventory and limits endpoints with doc sync |

## Search routing

| Request | Path |
|---|---|
| `order=newest` (any filters) | index: `p11_search::indexed` |
| `order=newest`, index rebuilding, no token or a `c1.` token | budgeted scan, oldest order, `c1.` tokens |
| `order=newest` with an `i1.` token, index rebuilding | 503 `unavailable`, `details.reason = index_rebuilding` |
| `dlq=true`, default order | budgeted scan + `p11_dlq::is_dlq_member` |
| anything else (incl. `tag=x`, `tags=a&tags=b` oldest) | existing scan (`list::list_runs`), unchanged for requests without new params |

Prefix choice (index path): first tag -> `Tag`; else workflow -> `Workflow`; else `dlq` -> `Status(Failed)` + `Status(TimedOut)`; else a terminal status -> `Status`; else `All`. Tags and workflows that contain `0x00` have no index rows, so they fall through to the next choice and are filtered in memory. All remaining filters run in memory on the decoded RunRecord, plus `is_dlq_member` when `dlq`.

## Token format

`i1.<kind>.<created_at_ms>.<run id>`, where `kind` is `t` (tag), `w` (workflow), `d` (dlq), `s` (status) or `a` (all). The token is the last run examined in merge order `(created_at_ms desc, uuid desc, partition asc)`. Stream `(prefix, q)` resumes strictly after `prefix ‖ !created ‖ !uuid` when `q <= p`. When `q > p` it resumes from the predecessor key (the 24-byte suffix minus one), so it starts at the position itself. A token of another kind, a malformed token, or a partition outside the count is 400. The run id is enough even for the two merged DLQ prefixes, because every prefix sorts by the same suffix. No base64 key was needed.

## PartitionView fields (D-P11-19)

| Field | Source | Today |
|---|---|---|
| `id` | slot index | always |
| `role` | `leader` for a started slot (RF1), `failed` for a slot that failed to start | always |
| `leader` | `[node] node_id` | started slots |
| `epoch` | `PartitionHandle::epoch` | started slots |
| `applied_offset` | `MachineHandle::applied` | started slots |
| `log_end_offset` | `ReplicatedLog::log_end` | started slots |
| `ready` / `running` | store `ready` rows / `running` rows | started slots |
| `healthy` | `MachineHandle::health == Healthy` | always |
| `detail` | `starting`, the fail-stop reason, or `failed to start: <reason>` | when not healthy |

Failed slots report only `id`, `role`, `healthy = false` and `detail`. There are no follower or ISR fields yet; Phase 12 adds them as additive fields.

## Other views

- **Modules.** Meta `Module` records, newest `uploaded_at_ms` first (ties: sha ascending). The token is `m1.<uploaded_at_ms>.<sha hex>`. `workflows` holds the distinct names. `uploaded_at_ms` is omitted when it is 0.
- **Queues.** The names are the union of: meta queue configs, workflow default queues, `default`, queues with Phase 7 gauges, and queues with ready or running runs. A queue that has only a `QueuePolicy` and no runs, gauges or config is not listed, because MetaState has no queue-policy iterator. `paused` and `flow` come from the policy (`p7_flow::flow_of`), `demand` comes from `demand::snapshot_all`, and the counts come from the partitions' `ready` rows and running runs.
- **Limits.** The same 13 rows as `docs/limits.md`, in doc order. The `[limits]` and `[api]` values are the effective ones; the rest are constants.

## docs/limits.md

No corrections. The sync test proved all 13 rows correct against `RunLimits::default()`, `ServerConfig::server_defaults().api`, `ostro_engine::host::context::MAX_CONTEXT_BYTES`, `ostro_types::command::MAX_*` (cross-checked with `ostro_api::domain::MAX_*`) and `ostro_api::domain::{MAX_PAGE_LIMIT, MAX_RESULT_WAIT_MS}`. Status and context are a single row because they share `MAX_CONTEXT_BYTES`.

## Deviations from Plan

**1. [Rule 1 - Bug] The index serves only `order=newest`.** The plan's eligibility (`!tags.is_empty() || newest || dlq`) would list legacy `?tag=x` requests newest first. That changes the default order, which D-P11-10 says stays the same, and it would break `runs_api`. Multi-tag and DLQ requests in the default order therefore use the scan, with tags AND and `is_dlq_member` applied. The search test checks both orders.

**2. [Rule 3 - Blocking] No `X-Ostro-Order-Fallback` header.** `RunPage` (ostro-api) has no field to carry it, and ostro-api is outside this plan. During the fallback the pages carry `c1.` tokens, which tells a client that the order is oldest. Open item for 11-18 / an ostro-api plan: a `RunPage` flag plus the header.

**3. [Rule 3 - Blocking] History attempt without prost.** `query::journal_rows` drops `attempt`, and ostro-server has no prost dependency (only 11-03 edits Cargo files). `history.rs` reads the same event range a second time in the same blocking task and pulls fields 1 and 4 out of the protobuf encoding. A unit test pins the parser against hand-encoded messages, including unknown fields of every wire type and truncated input.

**4. [Rule 2 - Missing] Uniform tag-cap details.** `control/runs.rs` (outside `files_modified`) returned plain 400s for more than 64 tags and for tags over 128 bytes. They now use `phase11::limit_error` with `limit = tags` / `tag_bytes`. The message text for the count case is unchanged.

**5. The `list.rs` branch already existed.** 11-04 put the `p11_search` call in `ServerControlPlane::list_runs` (mod.rs). The `list.rs` hunk is therefore the in-memory filter (tags AND, dlq) plus `list_runs_scan(keep)`, which the fallback uses.

**6. Rebuild test.** "Store reopened, rebuild paused by a test hook" is done with the existing 11-05 helpers. `delete_run_index_marker` puts the index in Rebuilding, and the test then calls `start_run_index_rebuild` + `join_run_index_rebuild`. A restart would race the Leader-start rebuild.

**7. [Rule 3] `tests/p11_contract.rs` updated** (outside `files_modified`). Its stub assertions for `limits` and `list_partitions` now expect real data.

**8. Workflow.** Worktree `wt-11-11`, slot-5 and a CAS `update-ref` were used instead of the plan's `/tmp/ostro-wt-11-11` and private index (executor brief).

## Tests

- `search_api` (1 test, ~300 runs over P=4): 20 newest; `tags=blue` gives 120 runs over exactly 3 pages of 50, strictly newest first, with `i1.t.` tokens; legacy `tag=blue&order=newest` gives the same; `blue+big` gives exactly 40 (index and scan); DLQ newest and oldest (6 Failed + 2 TimedOut); status and workflow prefixes; 400 on a garbage token or another query's token; budget 100 yields empty pages with tokens (`ceil(308/100)` calls) and a partial `blue+big` page; default path identical to `list_runs_with_budget`; fallback with markers deleted (identical sets, `c1.` tokens, 503 for an `i1.` token); rebuild and identical results; history `attempt` never 0.
- `inventory_api`: modules newest first, paginated, workflows listed, garbage token 400; queues sorted, `qp` paused with 1 ready and flow/demand; 4 partitions with the full field list, and ready total 1 on the held run's partition.
- `limits_api`: `[limits]` overrides reflected in `/v1/limits`. REST and gRPC payload caps on start, signal and token: 413 / RESOURCE_EXHAUSTED with exact details, and exactly-at-cap accepted. `__ostro/` tag 400 / INVALID_ARGUMENT; 65 tags `limit = tags` on both transports; 129-byte tag `tag_bytes`. No run created and no journal growth after rejections.
- `limits_doc` (2 tests): doc == constants; doc == endpoint (names, order, values; `configurable` matches).
- Unit: `p11_search` (4), `history` attempt parser, `p11_inventory` token.
- `cargo test -p ostro-server --lib --test runs_api --test start_run_api --test p11_contract --test search_api`: green. Full `cargo test -p ostro-server --locked --no-fail-fast`: 506 passed, 1 failed, and the failure was expected: `p11_contract::seam_is_wired_after_start_and_released_at_shutdown` asserted that the 11-11 stubs were still unimplemented. That assertion now checks the real limits (777 override) and partitions, and still checks the 11-13 bulk stub. Re-run: 10 passed.
- `cargo clippy -p ostro-server --all-targets --locked -- -D warnings`: clean. `cargo fmt`: clean. `cargo check --workspace --all-targets --locked`: clean after rebasing onto 636f1c4.
- No new `SystemTime::now` / `Instant::now` / `SystemClock` in server source.

## Open items

- **Order-fallback header.** This needs a `RunPage` field in ostro-api (deviation 2). docs/operations.md (11-18) should describe the fallback as "oldest order, `c1.` tokens; an `i1.` token is 503 index_rebuilding".
- **History attempt.** It becomes non-zero once 11-07's sink stamps `JournalEvent.attempt`. 11-07 had not landed at execution time, so the e2e test only asserts "absent or non-zero".
- **Limits not published.** `MAX_SEARCH_TAGS` (8), the bulk caps (`[bulk] max_matches`/`max_jobs`/`max_rate`) and the rerun prefix cap (11-10) are not in docs/limits.md or `/v1/limits`. Adding a row means changing both, and the sync test enforces that.
- **Queue-only policies.** A queue that has only a `QueuePolicy` is not listed by `/v1/queues` (no MetaState iterator).

## Self-Check: PASSED

- FOUND: the four test files, p11_search.rs, p11_inventory.rs
- FOUND: 727ad08, b46f5a1 on main
