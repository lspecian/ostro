---
phase: 05-workflow-semantics-core-hardening
plan: 14
subsystem: cli
tags: [cli, cli-06, rest-client, reqwest, json, dry-run, deploy, d12, d23, insta, spawned-binary]

requires:
  - phase: 05-13
    provides: "Server ControlPlane: lifecycle ops with dry_run, long-poll, start retention / schedule_to_start / dry_run, GET run attempt_failures / retention_ms / schedule_to_start_ms"
  - phase: 05-04
    provides: "REST lifecycle routes and bodies, error reasons (run_terminal, run_completed, run_not_terminal, token_already_completed, token_invalid)"
  - phase: 05-24
    provides: "PUT /v1/modules/{sha}?name=&tenant= (deployed module name, tenant upload)"
  - phase: 04-19
    provides: "ostro clap tree (dev/server/openapi/build-info) and the spawned-binary harness in crates/ostro/tests/common"
provides:
  - "ostro-cli crate: client.rs (typed REST client over reqwest, json + http2, no TLS), output.rs (--json / human output, exit_code), commands/{mod,runs,lifecycle,deploy}.rs"
  - "ostro run / ls / inspect / history / signal / cancel / terminate / retry / result / token complete / deploy, each with --server (OSTRO_SERVER, default http://127.0.0.1:7700), --json, --timeout"
  - "--dry-run on run / signal / cancel / terminate / retry / token complete, mapped to server-side dry_run: true"
  - "Exit codes: 0 ok, 1 API or connection error, 2 usage, 3 when run --wait / result --wait ends not completed"
  - "ostro deploy <file.wasm> [--activate] [--name (default file stem)] [--tenant] (D12, upload only)"
  - "crates/ostro/tests/cli_verbs.rs (7 tests) + 19 insta --json snapshots; tests/common run_cli / cli_json / write_guest / redact helpers"
affects: [05-17, 05-18, 05-20, phase-10, phase-11, phase-14]

tech-stack:
  added: [reqwest (normal dep of ostro-cli; workspace pin, default-features = false, json + http2), insta (dev-dep of ostro)]
  patterns:
    - "Each client verb flattens the shared Conn args (--server/--json/--timeout), so they never appear on ostro dev/server"
    - "Every verb returns the REST body as serde_json::Value; --json prints it verbatim (pretty)"
    - "Path segments are built with Url::path_segments_mut, so a tenant-qualified name is one %2F segment"
    - "Snapshot redaction by key (ids, *_at / *_at_ms, shas, sizes, record_version) plus per-test run-id substitution inside strings, with no insta features added"

key-files:
  created:
    - crates/ostro-cli/src/client.rs
    - crates/ostro-cli/src/output.rs
    - crates/ostro-cli/src/commands/mod.rs
    - crates/ostro-cli/src/commands/runs.rs
    - crates/ostro-cli/src/commands/lifecycle.rs
    - crates/ostro-cli/src/commands/deploy.rs
    - crates/ostro/tests/cli_verbs.rs
    - crates/ostro/tests/snapshots/cli_verbs__*.snap (19)
  modified:
    - crates/ostro-cli/Cargo.toml
    - crates/ostro-cli/src/lib.rs
    - crates/ostro/Cargo.toml
    - crates/ostro/src/main.rs
    - crates/ostro/tests/common/mod.rs
    - Cargo.lock

key-decisions:
  - "Two-call verbs print one combined JSON object: run --wait prints {\"start\": <start body>, \"result\": <result body>}; deploy --activate prints {\"upload\": <upload body>, \"activated\": [<workflow bodies>]}. Every other verb prints exactly one REST body"
  - "Under --json, an API error prints the server's error body on stdout (exit 1), plus one `ostro: error: ...` line on stderr. Connection errors print only the stderr line"
  - "--wait without a value waits until the run is terminal (a loop of polls, each capped at 60 s); --wait=DUR stops at DUR. require_equals: `--wait 5s` is a usage error, not a wait value"
  - "run --dry-run --wait does not wait (nothing was started); it prints the dry-run start body"
  - "result without --wait always exits 0 (reading a finished failed run is not an error); exit 3 applies only to --wait"
  - "Local file errors (deploy file, @file input) and https:// server URLs are usage errors (exit 2); https is rejected with a hint until SEC-01"
  - "deploy --activate qualifies each described workflow with --tenant (`acme/name`, sent as %2F) and activates the versions the upload response lists, in (name, version) order"
  - "ls --page maps to the REST page_token; tags are free-form strings (`--tag env=prod` sends \"env=prod\")"
  - "Usage errors from durations / JSON come from clap value parsers, so they share clap's exit code 2 and message format; `7d` says `write it as 168h`"
  - "The `Client` variant in main.rs is boxed (clippy large_enum_variant)"

requirements-completed: [CLI-06]

duration: ~1h15m
completed: 2026-11-19
---

# Phase 5 Plan 14: CLI-06 client verbs Summary

**The `ostro` binary now drives a server over REST: `ostro deploy x.wasm --activate`, `ostro run w --input @in.json --wait`, then `ls`, `inspect`, `history`, `signal`, `cancel`, `terminate`, `retry`, `result` and `token complete`, each with `--json` (the REST body verbatim, snapshot-tested) and, on mutating verbs, `--dry-run` (server-side validation, nothing changes). Exit codes are 0 ok, 1 API or connection error, 2 usage, and 3 when a wait ends in a status other than completed. Everything is proven against a spawned `ostro dev --ephemeral`.**

## Performance

- **Duration:** about 1 h 15 min, including one full gate run.
- **Tasks:** 3 of 3.
- **Files:** 26 created (7 source/test files plus 19 snapshots), 6 modified.

## Task Commits

1. **Task 1: client, output layer, run/ls/inspect/history/deploy:** `5a3f506` (feat)
2. **Task 2: lifecycle verbs and exit codes:** `1a924b9` (feat)
3. **Task 3: wiring into `ostro` and the spawned-binary suite with snapshots:** `45ab54c` (feat)

Every commit was made from the main checkout through a private index (`GIT_INDEX_FILE` seeded with `read-tree HEAD`, blobs hashed from the scratch worktree). Before each commit, a per-path check confirmed that HEAD's blob still matched this plan's previous commit. After it, the shared index was reset for exactly those paths and `git show --stat HEAD` was checked. Task 3 was amended once, through a private index, to add the 19 snapshots. zsh did not word-split the path list on the first attempt. HEAD was still that commit, so nothing else was rewritten.

## What landed

### ostro-cli (Tasks 1 and 2)

- **`client.rs`**
  - `Client::new(server, timeout)` takes `http://host:port[/prefix]` or `host:port`. `https` is rejected until SEC-01.
  - Each method maps to one REST route and returns `Response { status, body: Value }`.
  - Non-2xx answers become `CallError::Api { status, body }`, keeping the server's error body (non-JSON bodies are wrapped as `{"error": {"code": "http_<status>", ...}}`).
  - Transport errors produce `cannot reach the Ostro server at URL: <cause chain>`.
  - A long poll gets `timeout + wait` as its request timeout. No `reqwest` `query` feature is used: query strings come from `Url::query_pairs_mut`.
- **`output.rs`**
  - `Outcome { Ok, Error, Usage, NotCompleted }` with `exit_code()` (0/1/2/3), the single mapping, unit-tested per code.
  - `Out::json` pretty-prints a body. `Out::error` prints the JSON error body under `--json`, and one stderr line in every case.
  - Also: a table helper and `token_prefix` (8 characters plus `...`).
- **`commands/mod.rs`**
  - `ClientCommand` (clap `Subcommand`) with the `Conn` args flattened per verb.
  - `run(cmd) -> Outcome` runs the verb on a current-thread tokio runtime.
  - Parsers: `parse_duration` (jiff friendly, the same parser as the server; days or weeks give `write it as 168h`), `parse_duration_text` (validated locally, sent as text) and `parse_json_arg` (inline JSON or `@file`).
- **`runs.rs`**
  - Verbs: `run` (all plan flags plus `--tenant`), `ls`, `inspect` (shows `attempt_failures` as `retries`), `history` (`--limit`, `--from-seq`).
  - Helpers shared with lifecycle: `wait_result` (polls in a loop) and `print_result`.
- **`lifecycle.rs`**: `signal`, `cancel`, `terminate`, `retry`, `result` (`--wait[=DUR]`, `--follow` sends `follow_continuations=true`) and `token complete`. Only the flags given are sent; cancel, terminate and retry may send an empty object.
- **`deploy.rs`**: computes the sha256, then sends `PUT /v1/modules/{sha}?name=<--name or file stem>&tenant=`, then, with `--activate`, `PUT /v1/workflows/{tenant/name}/active {version}` for each workflow the upload response describes.

### `ostro` wiring and suite (Task 3)

- `main.rs`: `#[command(flatten)] Client(Box<ostro_cli::ClientCommand>)`. `ostro --help` lists dev, server, openapi, build-info, run, ls, inspect, history, signal, cancel, terminate, retry, result, token and deploy. `ostro dev --json` is a usage error (exit 2).
- `tests/cli_verbs.rs`. The five server tests each spawn their own `ostro dev --ephemeral` (P=4) under `SERIAL`:
  - `help_lists_every_verb`
  - `usage_and_connection_errors`: no server gives exit 1 with one stderr line. Exit 2 for `7d` (the message names 168h), invalid JSON, https and a missing deploy file.
  - `deploy_run_ls_inspect_history`:
    - deploy `--activate` (snapshot), then a re-deploy reports "unchanged";
    - `run sdk-basic --wait` (snapshot);
    - an `@file` input with tag, retention `48h` and idempotency key;
    - `ls` / `inspect` / `history` snapshots, plus tag and status filters and history paging;
    - human output facts;
    - dry runs: a new-run dry run (snapshot) and the existing id for a reused key. The run list is unchanged afterwards;
    - `--conflict reject` gives 409 with exit 1 (snapshot);
    - an unknown run gives 404 (snapshot), and a malformed id gives 400;
    - `acme/sdk-echo --tenant acme` gives a clean 404, which proves the `%2F` segment.
  - `signals_fifo_dedupe_and_result`:
    - `signal --dry-run` leaves status and history length unchanged;
    - a repeated `--request-id` gives `duplicate: true` (snapshot);
    - payloads go inline and as `@file`;
    - `result --wait` gives `[1,2,3]` (snapshot);
    - signalling the finished run gives 409 `run_terminal`.
  - `cancel_terminate_retry`:
    - cancel and terminate dry runs change nothing;
    - cancel (snapshot), then `result --wait` exits 3 (cancelled). A second cancel gives 409 `run_terminal` (snapshot);
    - terminate (snapshot), then a second terminate gives exit 1;
    - a retry dry run on the terminated run gives `resumed`, and status and history are unchanged;
    - the retry resumes the same id (snapshot), and a signal then completes it with `"go"`;
    - retrying the completed run gives 409 `run_completed` (snapshot);
    - retrying the guest-cancelled run restarts it as a new id.
  - `failed_run_exits_3`: `run signals {"mode":"bogus"} --wait` exits 3 (snapshot of the failure JSON). `result --wait` exits 3, `result` without a wait exits 0, and `result --wait=300ms` on a live run exits 3 with `completed: false`.
  - `token_complete`:
    - the token is read from the published status;
    - the dry run (snapshot) leaves the run unchanged, and the human output never contains the token;
    - completing with an `@file` payload completes the run (snapshot);
    - a second completion gives exit 1 without echoing the token, and `ot1_garbage` gives `token_invalid`.

## Verification

- `cargo test -p ostro-cli --locked`: 23 unit tests. They cover clap `try_parse_from` for every verb, the body and query mapping, `@file` loading, durations (including `7d` giving exit 2 with `168h`), URL and segment encoding, the API error message, the exit-code mapping and the deploy name default.
- `cargo test -p ostro --test cli_verbs --locked`: 7 passed. The suite ran three times in a row with `CI=true` (strict insta, no snapshot writes), about 7.7 s each.
- `cargo test -p ostro --test cli --locked`: 5 passed (unchanged suite).
- `cargo clippy -p ostro -p ostro-cli --all-targets --locked -- -D warnings`, `cargo fmt --all --check`: clean.
- `cargo deny check`: advisories, bans, licenses and sources all ok. `Cargo.lock` gains only dependency edges (ostro-cli: clap, jiff, reqwest, serde_json, sha2, tokio, tempfile; ostro: ostro-cli, insta) and no new packages, so no new TLS stack or duplicate hyper/axum/tonic.

## Gate

`bash scripts/ci-local.sh --no-musl` on committed HEAD `45ab54c`, run in a detached scratch worktree outside the repo (`CARGO_TARGET_DIR=target/wt-05-14`): **every step passed on the first run, rc 0.**

- **Passed:** toolchain, fmt, clippy `-D warnings`, warning-free build, SDK workspace fmt/clippy/test/wasm, guests build, order-saga example, `cargo test` (cli_verbs included), failpoint suites, cargo deny, check-vendored, check-deny-fixtures, forbidden crates, workflow validation.
- **Skipped:** the musl steps (`--no-musl`).

No flake occurred, so no reruns were needed.

## Deviations from Plan

### Auto-fixed issues

**1. [Rule 1 - Bug] `large_enum_variant` in main.rs.** Clippy flagged the flattened `ClientCommand` variant (RunArgs is large). The variant is boxed (`Client(Box<ostro_cli::ClientCommand>)`) and unboxed at dispatch. Commit `45ab54c`.

**2. [Rule 3 - Blocking] `wait_result` / `print_result` live in `runs.rs`, not `lifecycle.rs`.** `run --wait` (Task 1) needs them, and Task 1 had to build and test without Task 2's file. `lifecycle.rs` imports them. Commit `5a3f506`.

### Plan items resolved differently

- **Global flags are per verb.** `--server`, `--json` and `--timeout` are accepted after any client verb (`ostro ls --json`) but not before it (`ostro --json ls`), so they cannot leak into `ostro dev` / `ostro server`.
- **Combined JSON for two-call verbs.** See key-decisions (`run --wait`, `deploy --activate`). Their snapshots pin the shape.
- **The 409 snapshot set** covers run_terminal (cancel), run_completed (retry) and idempotency reject. `retry` of a live run (`run_not_terminal`) is covered by the 05-13 server suite, not here.
- **Extra flag:** `run --tenant` (the start body's tenant), so tenant runs are reachable from the CLI.
- **Snapshot redaction** is done by the helper in `tests/common` (by key plus run-id substitution), not by insta's `redactions` feature. That keeps the workspace `insta` pin featureless, so the lockfile gains no packages.

## Known limitations

- `ostro dev` / `ostro server` cannot take `--server`; the client verbs cannot take TLS URLs until SEC-01.
- `ls` shows one page (`--page` for the next); no auto-pagination.
- No `ostro workflows` / `ostro policy` verbs. The policy endpoints from 05-13 are REST/gRPC only. Not in CLI-06; candidates for 05-17 docs or later.

## Hand-off notes

### 05-17 (quickstart / docs)
- Quickstart sequence, verified by `cli_verbs.rs`:
  - `ostro dev`
  - `ostro deploy target/wasm32-wasip2/release/<crate>.wasm --activate`
  - `ostro run <workflow> --input '{"name":"x","n":4}' --wait`
  - `ostro ls`, `ostro inspect <id>`, `ostro history <id>`.
- Document:
  - The default server is `http://127.0.0.1:7700`, overridable with `--server` / `OSTRO_SERVER`.
  - `--json` prints the REST body; `run --wait --json` gives `{start, result}`, and `deploy --activate --json` gives `{upload, activated}`.
  - Exit codes 0/1/2/3, as in `output.rs`.
  - Durations: hours are the largest unit (`--retention 168h`; `7d` is rejected with that hint).
  - `--input` / `--payload` take JSON (quote strings: `'"hello"'`) or `@file`.
  - `deploy --name` defaults to the file stem and is write-once per registered version (409 `existing_module_name` on a re-deploy under another name, 05-24).
  - Tenant workflows: `ostro run acme/w --tenant acme` or `ostro run w --tenant acme`; `deploy --tenant acme --activate` activates `acme/<name>`.
  - Dry-run semantics: `run --dry-run` gives `run_id ""` and `created: true` for a new run, or the existing id for a reused idempotency key; `retry --dry-run` reports the mode.
  - A token is shown only as an 8-character prefix.
- The usage text in `ostro <verb> --help` comes from the doc comments in `crates/ostro-cli/src/commands/*.rs`.

### 05-18
- CLI JSON shapes are pinned by `crates/ostro/tests/snapshots/cli_verbs__*.snap`. A REST DTO change that adds a field shows up as a snapshot diff in `ostro` tests, not only in the OpenAPI snapshot. Re-bless deliberately: run with `INSTA_UPDATE=always`, then review the diff.
- `tests/common/mod.rs` now has `run_cli(addr, args)`, `cli_json(addr, args)`, `write_guest(dir, name)` and `redact` / `redacted(value, subst)` for any further spawned-binary tests.
- No cargo-insta is needed: tests run under `CI=true` (as ci-local does), and a missing or changed snapshot fails.

### Phase 10 / 11 / 14
- Phase 10 (CLI-05): `deploy` is the upload half. Build-if-needed can call `commands::deploy::deploy` after building.
- SEC-01: `client::parse_server` is the one place that rejects https. Adding TLS means enabling a reqwest TLS feature (cargo-deny: one TLS stack) and removing that branch.
- Phase 14: `--tenant` exists on `run` and `deploy`. `ls`/`inspect` have no tenant filter yet; the REST list has none either.

## STATE notes (for the orchestrator; STATE.md not edited, no gsd-tools mutators run)

- 05-14 complete (3/3 tasks). Commits `5a3f506`, `1a924b9`, `45ab54c`, plus the docs commit.
- Decisions:
  - combined JSON for `run --wait` and `deploy --activate`;
  - connection flags per verb;
  - `--wait` without a value waits until terminal;
  - `result` without a wait always exits 0;
  - https rejected until SEC-01;
  - snapshot redaction by helper (no insta features).
- Requirement: CLI-06 complete.
- The main checkout's working tree does not have the 05-14 files (commits went through a private index; per instructions, nothing was copied over the main checkout). `git status` there shows them as deleted/modified relative to HEAD until it is refreshed (for example with `git checkout -- crates/ostro-cli crates/ostro Cargo.lock`, which the orchestrator should run only once the parallel plans are done).
