---
phase: 10-developer-cli-test-kits
plan: 11
subsystem: replay
tags: [replay, cli-07, determinism, corpus, exit-4, sc3, polyglot]
requires:
  - phase: 10-developer-cli-test-kits
    provides: "10-02 deps (ostro-cli -> ostro-engine, ostro dev-deps ostro-conformance/polyglot feature); 10-03 ReplayArgs + Outcome::Diverged (exit 4) + tests/common/p10.rs; 10-05 ReplayRequest/ReplayResult domain, REST route, Client::replay_module (+300 s), server stub control/replay.rs"
  - phase: 11-operations-ui
    provides: "11-10 ostro_engine::replay_check::{replay_check, ReplayVerdict} (4-arg, refusing sink), reused unchanged"
provides:
  - "crates/ostro-engine/src/replay_corpus.rs: CorpusEntry, load_corpus_entry, load_corpus, corpus_entry_dirs, decode_journal_bin / encode_journal_bin, unhex32, ENTRY_FILES"
  - "crates/ostro-server/src/control/replay.rs: POST /v1/modules/{sha}/replay implemented (REPLAY_CONCURRENCY = 2, SKIPPED_DETAIL)"
  - "crates/ostro-cli/src/commands/replay.rs: ostro replay remote + --corpus, table / ostro.replay.result JSON, outcome_of (exit 4/1/0), Counts"
  - "guests/p10-replay-v{1,2,3} (replay_demo v1/v2/v3); crates/ostro/tests/fixtures/replay/{py,ts}-v{1,2,3}"
  - "tests: ostro-engine tests/replay_check.rs, ostro-server tests/replay_endpoint.rs, ostro tests/replay_cli.rs"
  - "docs/replay.md"
affects: [10-20, 11-follow-ups]
tech-stack:
  added: []
  patterns:
    - "Frozen wire formats read by hand where a crate has no protobuf/TOML dependency (JournalEvent fields 1-4, flat entry.toml), cross-checked in tests against prost + toml"
    - "Server-side replay pool: tokio JoinSet + owned semaphore permits, verdicts re-ordered by selection index"
key-files:
  created:
    - crates/ostro-engine/src/replay_corpus.rs
    - crates/ostro-engine/tests/replay_check.rs
    - crates/ostro-server/tests/replay_endpoint.rs
    - crates/ostro/tests/replay_cli.rs
    - crates/ostro/tests/fixtures/replay/py-v1/app.py
    - crates/ostro/tests/fixtures/replay/py-v2/app.py
    - crates/ostro/tests/fixtures/replay/py-v3/app.py
    - crates/ostro/tests/fixtures/replay/ts-v1/index.ts
    - crates/ostro/tests/fixtures/replay/ts-v2/index.ts
    - crates/ostro/tests/fixtures/replay/ts-v3/index.ts
    - guests/p10-replay-v1/{Cargo.toml,src/lib.rs}
    - guests/p10-replay-v2/{Cargo.toml,src/lib.rs}
    - guests/p10-replay-v3/{Cargo.toml,src/lib.rs}
    - docs/replay.md
  modified:
    - crates/ostro-engine/src/lib.rs
    - crates/ostro-engine/tests/replay_corpus.rs
    - crates/ostro-server/src/control/replay.rs
    - crates/ostro-server/tests/dev_surface.rs
    - crates/ostro-cli/src/commands/replay.rs
    - crates/ostro-cli/src/commands/mod.rs
    - crates/ostro/tests/p10_scaffold.rs
key-decisions:
  - "11-10's replay_check reused unchanged (signature takes `&Prepared`; no PreparedModule alias added because the file is 11-10's). The corpus loader went into the new replay_corpus.rs per the plan's shared-checker rule"
  - "No Cargo edits: ostro-engine has protobuf/TOML only as dev-deps, so replay_corpus reads the frozen JournalEvent wire shape and the flat entry.toml by hand (canonical re-encode check kept); tests/replay_corpus.rs cross-checks it against prost and toml on all 37 entries"
  - "Unknown explicit run id = 404 not_found for the whole request (10-05 contract, fake parity, coordinator note), not a per-run error verdict as the plan text said"
  - "Workflow bindings are write-once, so the replay guests/fixtures declare versions 1/2/3 (v1 auto-activates; v2/v3 upload inactive)"
  - "allow_missing does not change server verdicts (a missing workflow is always skipped); it is the CLI exit policy"
  - "Exit precedence: diverged (or skipped without --allow-missing) -> 4 wins over error -> 1; unreadable --module -> 2 (as ostro deploy); missing corpus dir -> 1"
  - "Remote upload names the module after the file stem (as ostro deploy), so replay-then-deploy of the same file never hits a module-name conflict"
  - "Sample = newest N per described workflow (or only --workflow, even if not exported, which then shows as skipped) via ControlPlane::list_runs order=Newest, re-sorted newest-first (the scan fallback is oldest-first)"
requirements-completed: [CLI-07]
duration: ~2h
completed: 2026-11-22
---

# Phase 10 Plan 11: Server-side replay and `ostro replay` Summary

**`ostro replay` gates CI on a new module version: it replays stored journals on the server with the cluster token key (only verdicts come back), or a 05-12 corpus offline, and exits 4 on divergence. SC3 holds in Rust, Python and TypeScript.**

## What was built

- **Engine library** (`ostro_engine::replay_corpus`): the 05-12 entry loader (file set, name, sha256s, event count, canonical journal codec, token key, RunStarted workflow/input). 11-10's `replay_check` (Ok / OkFrontier / Diverged{seq, detail} / Error) is the verdict logic, unchanged. `tests/replay_corpus.rs` now loads through the library, with its assertions unchanged and the corpus files untouched.
- **Server endpoint** (`control/replay.rs`):
  - the module is resolved (404) and prepared once through the runtime cache, then described;
  - runs are selected (explicit ids in order, or the newest N per workflow) and their journals read with `journal_io::read_journal`;
  - each run is checked with `replay_check` on `cp.runtime`, on a 2-task pool;
  - skipped = the module does not export the workflow;
  - one info line per request.
- **CLI** (`ostro replay`):
  - remote mode uploads the module (file-stem name, never activated) and calls the endpoint;
  - `--corpus DIR [--module]` replays offline with each entry's own token key (one runtime per key, one prepare per (key, module));
  - output is a table or the `ostro.replay.result` envelope; exit 4 / 1 / 0 / 2.

## API surface used to read journals

`ostro_server::journal_io::read_journal(&PartitionHandle<Rf1Log>, &RunId)`: the paged `Events` rows with blobs resolved, decoded with the journal codec. This is the same reader the executor and the 11-10 rerun use. Run records come from `ostro_partition::query::run_record`, and samples from `ControlPlane::list_runs` (`order: Newest`, workflow and status filters).

## Verdict tables (SC3, `cargo test -p ostro --features polyglot --test replay_cli -- --nocapture`)

The runs were recorded on v1 (`a`, `b`, sleep). seq 1 is v1's first `StepCompleted`.

| Language | Module | Exit | Verdicts (3 runs) | Detail |
|---|---|---|---|---|
| Rust | v2 (swap) | 4 | 3 diverged, seq 1 | `expected step "step_a" #6783ae6c, actual step "step_b" #9724c8b7` |
| Rust | v3 (patched) | 0 | 3 ok | - |
| Python | v2 | 4 | 3 diverged, seq 1 | `expected step "a" #186b7763, actual step "b" #46a76db5` |
| Python | v3 | 0 | 3 ok | - |
| TS | v2 | 4 | 3 diverged, seq 1 | `expected step "a" #186b7763, actual step "b" #46a76db5` |
| TS | v3 | 0 | 3 ok | - |

The Python and TS command hashes are identical (the same step names through the shared WIT). Also covered:
- `--run <id> --module v2` exits 4;
- positional ids merge with `--run`, in order;
- a module without the workflow gives skipped: exit 4, or 0 with `--allow-missing`;
- a malformed run id exits 1.

The server suite (`replay_endpoint`) additionally covers:
- a sleeping (in-progress) v1 run: diverged on v2 (4/4 runs diverged) and `ok_frontier` on v3 (3 ok + 1 ok_frontier);
- the status filter, sample size and request order;
- an unknown run → 404, a malformed id → 400, `sample: 0` → 400, an unknown module → 404;
- the verdict objects carry only `run_id, workflow, verdict, seq, detail, appended`;
- replay leaves the active version and the runs untouched.

## Corpus replay

- `ostro replay --corpus tests/replay-corpus` (debug binary): 37 entries, **37 ok**, exit 0, about 2-3 s.
- The `patch-v2` entry (A, C, sleep, B under `patched("add-c")`) replayed with `--module patch-v1` diverges at seq 3 (`expected step "step_c" #767780e0, actual sleep #7e39326c`), exit 4.
- `cargo test -p ostro-engine --test replay_corpus`: 6/6 passed on the unchanged corpus; `git diff` on `tests/replay-corpus` is empty.

## Replay request timings (debug `ostro dev --ephemeral`, 3 runs)

From the server's `module replay` info line:

| Request | prepare_ms | total_ms |
|---|---|---|
| v2, sample 5 (3 diverged) | 1 | 6 |
| v3, sample 5 (3 ok) | 1 | 7 |
| v3 again | 1 | 7 |

The module is prepared once per request. The upload that precedes the call has already AOT-compiled it into the runtime cache, so the prepare is a cache hit.

## Tests

- `cargo test -p ostro-engine --locked --lib replay_corpus`: 4 passed.
- `cargo test -p ostro-engine --locked --test replay_corpus --test replay_check`: 6 + 4 passed.
- `cargo test -p ostro-server --locked --test replay_endpoint --test dev_surface`: 1 + 1 passed.
- `cargo test -p ostro-cli --locked -- replay`: 8 passed (exit mapping, counts, request bodies, unreadable inputs, empty corpus, plus the 10-03 parse tests).
- `cargo test -p ostro --locked --test replay_cli --test p10_scaffold -- --test-threads=1`: 4 + 7 passed.
- `cargo test -p ostro --features polyglot --locked --test replay_cli -- --test-threads=1 --nocapture`: 6 passed, including `python_replay_gate ... ok` and `ts_replay_gate ... ok`.
- clippy `-D warnings` on ostro-engine, ostro-server, ostro-cli and ostro (also with `--features polyglot`, all targets): clean. `cargo fmt --all --check`: clean. Strict CLOCK_GATE: empty.
- `bash scripts/ci-local.sh --no-musl` (at 5e137f7): every step PASS except one out-of-scope test failing in two steps (`cargo test`, `ostro-build polyglot`): `ostro-build/tests/scaffold.rs::build_stub_answers_not_implemented` still expects the 10-12 build stub, which 10-12's 6983af8 replaced (BuildError::NotImplemented no longer returned). Polyglot conformance, failpoint suites, the 20-kill campaign smoke, deny and fmt/clippy all pass, as do all replay suites (incl. `replay_cli`, `replay_corpus`, ostro-client `typed`).

## Deviations from Plan

### Auto-fixed issues

**1. [Rule 3 - Blocking] Loader without new dependencies.** The plan said the engine "already depends on toml". It does so only as a dev-dependency, prost too, and Cargo edits are 10-02's only. `replay_corpus.rs` therefore reads the frozen `JournalEvent` wire shape (seq/kind/payload{inline}/attempt) and the flat top-level `entry.toml` by hand, keeping the canonical re-encode check. The corpus test cross-checks both readers against prost and toml on every entry. Commit cba9864.

**2. [Contract] Unknown explicit run = 404.** The plan's Task 2 asked for a per-run `error` verdict. The coordinator and the 10-05 contract (OpenAPI 404 "Unknown module or run", fake parity) say the whole request is a plain 404, so the endpoint follows the contract. The endpoint test asserts the 404. Commit 728bdea.

**3. [Rule 3] Distinct workflow versions.** Workflow (name, version) bindings are write-once per module. Same-version v1/v2/v3 guests would conflict on upload, so v1/v2/v3 declare versions 1/2/3 (the engine dispatches by name, so replay is unaffected). The Python/TS fixtures do the same.

**4. Stub tests updated.** `ostro-server/tests/dev_surface.rs` now expects 404 for an unknown module (it asserted the 501 stub). The replay rows were dropped from the 10-03 stub tests (`commands/mod.rs::local_stubs_exit_2`, `crates/ostro/tests/p10_scaffold.rs::stubs_exit_2_naming_their_plan`); the remaining single build row is written without a loop (clippy `single_element_loop`). `ostro-client/tests/typed.rs` already accepts a report instead of 501, so it is unchanged.

**5. Corpus divergence fixture.** For "replay one corpus entry against a different module", the test uses the committed `patch-v2` entry with `--module` = the `patch-v1` guest, a real step-vs-sleep divergence, instead of re-recording a v1 journal into a temp corpus. The engine-level Diverged test uses det-basic's journal with the `reordered` input against `det-divergent`.

## Open items

- Out of scope (10-12): `crates/ostro-build/tests/scaffold.rs::build_stub_answers_not_implemented` fails on main since 6983af8; 10-12 must drop or update that stub test.

- Sampling selects by the stored workflow name in the default tenant; tenant-qualified runs need `--workflow tenant/name` (documented in docs/replay.md).
- With the 11-11 run index rebuilding, `list_runs` falls back to an oldest-first scan. The endpoint re-sorts what it received, but a large workflow could then be sampled from its oldest page.

## Self-Check: PASSED
