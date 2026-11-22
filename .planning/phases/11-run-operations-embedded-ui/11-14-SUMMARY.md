---
phase: 11-run-operations-embedded-ui
plan: 14
subsystem: CLI (ostro-cli verbs over the Phase 11 REST surface)
tags: [cli, CLI-06, OPS-02, OPS-03, OPS-04, OPS-05, OPS-07, rerun, dlq, bulk, limits]
requires:
  - 11-09 (REST routes + DTOs, ostro_api::fake FakeOps)
  - 11-10 (rerun API, rerun_cut_unsafe details)
  - 11-11 (GET /v1/runs tags/order/dlq, GET /v1/limits)
  - 11-12 (DLQ list/requeue/dismiss)
provides:
  - ostro rerun, ostro dlq ls|requeue|dismiss, ostro bulk <action>|ls|status|cancel <job>, ostro limits
  - ostro ls --tag (repeatable) / --newest / --dlq
  - ostro-cli Client Phase 11 block (rerun, rerun_cuts, list_dlq, requeue_dlq, dismiss_dlq, create_bulk, list_bulk, get_bulk, cancel_bulk, limits)
  - crates/ostro/tests/common/p11.rs (P11Dev spawned dev server, FakeApi in-process fake API, mask helpers)
affects: [10-09 (moves the client block verbatim), 10-18 (completions), 11-18 (RESERVATIONS §11 verb names, real-server bulk smoke)]
tech-stack:
  added: []
  patterns: ["in-process ostro_api::router_with over FakeOps on its own thread/runtime, with an axum middleware that advances bulk jobs on GET"]
key-files:
  created:
    - crates/ostro-cli/src/commands/rerun.rs
    - crates/ostro-cli/src/commands/dlq.rs
    - crates/ostro-cli/src/commands/bulk.rs
    - crates/ostro-cli/src/commands/limits.rs
    - crates/ostro/tests/cli_p11.rs
    - crates/ostro/tests/common/p11.rs
    - crates/ostro/tests/snapshots/cli_p11__*.snap (29)
  modified:
    - crates/ostro-cli/src/client.rs (one appended, delimited Phase 11 block)
    - crates/ostro-cli/src/commands/mod.rs (4 module lines, 4 appended variants, one conn arm + one dispatch arm each)
    - crates/ostro-cli/src/commands/runs.rs (ls flags)
decisions:
  - "Client location: 10-09 has not landed (no 10-09-SUMMARY.md, no ostro_client:: in client.rs, no crates/ostro-client), so the methods are one contiguous block `// --- Phase 11 (11-14) ---` ... `// --- end Phase 11 (11-14) ---` in crates/ostro-cli/src/client.rs, placed before the test module (clippy items_after_test_module); no ostro-client files"
  - "Dispatch: main.rs is untouched. It embeds ostro_cli::ClientCommand with #[command(flatten)] and calls ostro_cli::run, so the new variants dispatch through commands::dispatch (one conn arm + one dispatch arm per verb)"
  - "--json prints the REST body verbatim (CLI-06): the rerun JSON is the RerunResponse {new_run_id, source_run_id, cut_seq, copied_events, workflow, version, verdict, existing}, not a synthetic {kind, ...} object"
  - "Exit codes are the existing table: 0 ok, 1 API error (409 conflict such as rerun_cut_unsafe / dlq_not_member, 429 bulk_too_many_jobs, 404), 2 usage, 3 not completed (bulk status --watch ending cancelled/failed)"
  - "ls --tag: one tag keeps the pre-Phase-11 `tag` query parameter (cli_verbs unchanged); two or more go out as repeated `tags`"
  - "bulk cancel is both the action and the job verb: `bulk cancel <JOB>` (positional conflicts with every selection flag) cancels a job, otherwise it creates a cancel job"
metrics:
  duration: "~1h30"
  completed: 2026-11-22
---

# Phase 11 Plan 14: CLI run operations Summary

The `ostro` binary now covers every Phase 11 run operation from the command line: `rerun`, `dlq`, `bulk` and `limits`, plus search flags on `ls`. All of them follow the CLI-06 contract: human output by default, the REST body under `--json`, and the existing exit-code table. The rerun, DLQ, limits and search verbs are tested against a spawned `ostro dev` server, and the bulk verbs against the in-process API fakes.

## Commits

| Task | Commit | Description |
|------|--------|-------------|
| 1 | a062e76 | `feat(11-14)`: ostro rerun, dlq, limits verbs and ls search flags |
| 2 | b26e64a | `feat(11-14)`: ostro bulk verbs |

## Verbs (CLI verb names for 11-18 / RESERVATIONS §11)

| Verb | REST | Human output | Exit |
|---|---|---|---|
| `ostro rerun <run> (--from-step NAME[#N] \| --from-seq N) [--version active\|source\|N] [--idempotency-key K] [--dry-run]` | `POST /v1/runs/{id}/rerun` | `started rerun <new> of <src> (...)` / `existing rerun ...` / `dry run: rerun of <src> would be accepted (...)`; a refusal prints the error line, then the details (`cut at seq S is unsafe (detail); nearest safe cut: --from-seq N`, nondeterministic seq, limit max/actual) | 0 / 1 / 2 |
| `ostro dlq ls [--workflow --queue --reason --tag* --all --limit --page]` | `GET /v1/dlq` (`tags` repeated, `include_dismissed`) | table: RUN ID, WORKFLOW, VERSION, STATUS, QUEUE, REASON, RESUMABLE, FAILED, STATE (dlq / dismissed: r / requeued -> id) | 0 / 1 |
| `ostro dlq requeue <run> [--mode auto\|resume\|restart\|rerun] [--from-step\|--from-seq] [--dry-run]` | `POST /v1/dlq/{id}/requeue` | `requeued <run> (mode M): new run X` / `resumed in place` | 0 / 1 / 2 (`rerun` without a cut, or a cut without `rerun`) |
| `ostro dlq dismiss <run> --reason R` | `POST /v1/dlq/{id}/dismiss` | `dismissed <run> from the DLQ (R)` | 0 / 1 (`dlq_not_member`) |
| `ostro bulk <cancel\|terminate --reason\|retry\|rerun <cut> [--version]\|requeue [--mode]\|pause\|resume\|signal --name [--payload]> [--tag* --status --workflow --queue --dlq] [--rate N] [--max-in-flight N] [--dry-run]` | `POST /v1/bulk` | `started bulk job <id> (action, about N runs matched)`; dry run: `dry run: bulk A would match about N runs (matched_estimate)` + sample | 0 / 1 (429 prints the reason and `retry after N s`) / 2 |
| `ostro bulk ls` | `GET /v1/bulk` | table: JOB, STATE, ACTION, MATCHED, DONE, SKIPPED, FAILED, RATE, CREATED | 0 / 1 |
| `ostro bulk status <job> [--watch [--interval 1s]]` | `GET /v1/bulk/{id}` | key/value view (+ failures table); `--watch` prints a progress line when it changes | 0; `--watch`: 0 done, 3 cancelled/failed |
| `ostro bulk cancel <job>` | `POST /v1/bulk/{id}/cancel` | `bulk job <id>: cancelled` | 0 / 1 |
| `ostro limits` | `GET /v1/limits` | table: NAME, VALUE, UNIT, CONFIGURABLE, ENFORCED, ERROR | 0 / 1 |
| `ostro ls --tag T (repeatable) --newest --dlq` | `GET /v1/runs?tag=` or `tags=`*, `order=newest`, `dlq=true` | unchanged table | unchanged |

## Tests

- `cli_p11` (6 tests, all green):
  - `rerun_and_ls_search_flags` (spawned dev, p11-rerun v1/v2): `ls --tag blue --tag big [--newest]` returns the AND set in both orders; `ls --dlq` returns only the failed source. `rerun --dry-run` gives a verdict and creates no run (the run list is compared). `rerun --from-step s3 --idempotency-key k1` on the active v2 creates a run that completes with 5; the same key returns `existing rerun`. An unsafe cut exits 1, prints `rerun_cut_unsafe` and `nearest safe cut: --from-seq 4`, and puts the error body on stdout under `--json`. Snapshots: `rerun_dry_run`, `rerun_json`, `rerun_cut_unsafe`.
  - `dlq_ls_dismiss_requeue_and_limits`: `dlq ls` JSON + table snapshots (ids and times masked); tag/workflow filters. `dismiss --reason flake` hides the entry and `--all` shows it with `dismissed: flake`. Dismissing a non-member exits 1 with `dlq_not_member`. `requeue --dry-run`, then `requeue` (auto → restart) with snapshot; the entry shows `requeued -> <new>`. `limits --json` equals `GET /v1/limits` (14 rows); the table snapshot has one line per limit.
  - `bulk_dry_run_create_watch_cancel_ls` (FakeApi): dry run (human + JSON snapshots, no job created), create, `status --json`, `status --watch` (exit 0, snapshot), `bulk cancel <job>` → `cancelled`, `--watch` on the cancelled job → exit 3, `bulk ls` JSON + table, unknown job → exit 1.
  - `bulk_request_bodies_and_too_many_jobs`: the request bodies of rerun (`--from-step s3 --version active --rate 5`), signal (`--name go --payload '{}'`), requeue (`--dlq --mode restart`), terminate, retry and resume are asserted on what the fake received. An injected 429 `bulk_too_many_jobs` exits 1 with the reason and `retry after 1 s`. Usage errors exit 2 and never reach the server.
  - `help_rerun_dlq_bulk_limits` (11 `--help` snapshots, plus top-level presence) and `rerun_usage_errors_exit_2`.
- `cargo test -p ostro -p ostro-cli --locked --no-fail-fast`: every binary green (ostro-cli lib 56; cli_verbs 7 with snapshots byte-identical; cli 5, cli_p7 15, cli_phase8 3, phase8_verbs 6, flow_e2e 12, worker_* and the other e2e suites ok; dev_startup 1 ignored as before).
- `cargo clippy -p ostro -p ostro-cli --all-targets --locked -- -D warnings`: clean. `cargo fmt --check`: clean. `cargo check --workspace --all-targets --locked` after rebasing onto 11-17: clean.
- Not run: `scripts/ci-local.sh` (the whole-workspace gate; skipped to spare the shared machine, left for 11-18).

## Deviations from Plan

1. **Rerun `--json` shape.** The plan's behaviour line shows `{"kind": ..., "new_run_id": ..., "cut_seq": ...}`. CLI-06 says `--json` is exactly the REST body, so the verb prints the `RerunResponse` as is (it has `new_run_id` and `cut_seq`; there is no `kind`).
2. **Unsafe cut scenario.** The spawned-server test uses a cut one past the terminal event (409 `rerun_cut_unsafe`, `detail: terminal_or_out_of_range`, `nearest_safe_seq: 4`) instead of an open effect, which would need an egress allow-list and a mock HTTP target in `ostro dev`. The open-effect path is covered server-side by 11-10's `rerun_api`. The CLI renders both the same way.
3. **"Fake advances on each GET".** `FakeOps` only advances through `set_bulk_progress`, and the crate has no `async-trait` dependency to wrap the trait, so `FakeApi` adds an axum middleware that advances the job by one op before each `GET /v1/bulk/{job}`.
4. **No `main.rs` edit.** Dispatch goes through `ostro_cli::run` via the flattened `ClientCommand` (see decisions).
5. **No completions assertion.** `ostro completions` does not exist at HEAD (Phase 10's 10-18 has not landed).
6. **Workflow per the executor brief.** Worktree under the session scratchpad, target slot-6, and main moved by CAS (not `/tmp/ostro-wt-11-14` with a private index).

## Open items

- 11-13 (real bulk service) had not landed when this plan finished, so there is no real-server `ostro bulk` smoke; 11-18's ops e2e covers it.
- 11-18: add the verb names above to RESERVATIONS §11 if Phase 10 reserves CLI verbs there.
- 10-09: move the `// --- Phase 11 (11-14) ---` block of `ostro-cli/src/client.rs` verbatim into `ostro-client`.
- 10-18: the completions presence check (`rerun`, `dlq`, `bulk`, `limits`) belongs in `cli_p11.rs` once `ostro completions` exists.
