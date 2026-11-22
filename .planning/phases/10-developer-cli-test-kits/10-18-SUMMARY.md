---
phase: 10-developer-cli-test-kits
plan: 18
subsystem: cli
tags: [ostro-cli, completions, clap_complete, json-contract, exit-codes, cli-03, cli-04, cli-05, cli-07, cli-08]
requires:
  - phase: 10-developer-cli-test-kits
    provides: "10-02 clap_complete =4.6.12 pin + main.rs anchor line; 10-03 Outcome/exit_code/local_json + tests/common/p10.rs; 10-07 toolchain/doctor; 10-08 init; 10-11 replay + p10-replay guests; 10-12 build + rust-offline fixture; 10-16 deploy [DIR]"
provides:
  - "ostro completions <bash|zsh|fish|powershell|elvish> (main.rs Completions { shell } -> clap_complete::generate over Cli::command(); install lines in --help)"
  - "ostro_cli::output::EXIT_CODES: &[(i32, &str)] (0 ok, 1 API/server/build error, 2 usage, 3 --wait not completed, 4 replay divergence)"
  - "crates/ostro/tests/completions.rs (presence checks, tcsh exit 2, bash -n)"
  - "crates/ostro/tests/json_contract.rs + 6 json_contract__*.snap (local-verb envelopes, redacted)"
  - "docs/cli-json.md (JSON families, compatibility rules, per-verb fields, manifest.json, exit-code table, stderr)"
affects: [10-19, 10-20]
tech-stack:
  added: []
  patterns:
    - "completions are checked for presence of verbs/flags with word boundaries (build does not match build-info), fish long flags as `-l name`; no full-script snapshots (W3a)"
    - "envelope order (kind, version first) checked on the raw stdout text; snapshots are the redacted, key-sorted JSON"
    - "doc table rows parsed from the `## Exit codes` section and compared with EXIT_CODES"
key-files:
  created:
    - crates/ostro/tests/completions.rs
    - crates/ostro/tests/json_contract.rs
    - crates/ostro/tests/snapshots/json_contract__init.snap
    - crates/ostro/tests/snapshots/json_contract__build.snap
    - crates/ostro/tests/snapshots/json_contract__deploy_project.snap
    - crates/ostro/tests/snapshots/json_contract__replay.snap
    - crates/ostro/tests/snapshots/json_contract__toolchain_list.snap
    - crates/ostro/tests/snapshots/json_contract__doctor.snap
    - docs/cli-json.md
  modified:
    - crates/ostro/src/main.rs
    - crates/ostro-cli/src/output.rs
    - crates/ostro/tests/cli_p11.rs
    - crates/ostro-cli/src/commands/mod.rs
    - crates/ostro-cli/src/commands/bulk.rs
    - crates/ostro/tests/snapshots/cli_p11__help_bulk.snap
key-decisions:
  - "The 10-02 anchor line `use {clap_complete as _, ostro_build as _};` became `use ostro_build as _;` now that clap_complete is used"
  - "toolchain verbs share kind `ostro.toolchain.result` (the `action` field names the subcommand), as implemented by 10-07; docs say so"
  - "doctor's host probes (cc/docker present+path, musl, glibc, warnings) are redacted to placeholders so the snapshot holds on any machine; their keys stay frozen"
  - "Coordinator hand-offs done here: cli_p11 completions presence check for rerun/dlq/bulk/limits; root --help deploy line no longer says '(no build)'; bulk retry help says 'resume in place or restart as a new linked run' (cli_p11__help_bulk.snap updated for that one line)"
requirements-completed: [CLI-03, CLI-04, CLI-05, CLI-07, CLI-08]
duration: ~1h
completed: 2026-11-23
---

# Phase 10 Plan 18: completions + local-verb JSON contract Summary

**`ostro completions <shell>` generates clap_complete scripts from the live clap tree for five shells. The `--json` envelopes of init, build, deploy DIR, replay, toolchain list and doctor are frozen by redacted snapshots, with `kind`/`version` checked as the first two keys. docs/cli-json.md documents the JSON policy and the exit-code table, which a test compares with `ostro_cli::output::EXIT_CODES`.**

## Tasks

| # | Task | Commit |
| - | ---- | ------ |
| 1 | `ostro completions` via clap_complete + presence checks (+ cli_p11 Phase 11 check) | b9411ef |
| - | Help wording: root deploy line, bulk retry (coordinator hand-off) | 8ca8c8c |
| 2 | EXIT_CODES, json_contract snapshots, docs/cli-json.md | 964fd38 |

## Snapshots

New (crates/ostro/tests/snapshots/): `json_contract__init`, `json_contract__build`,
`json_contract__deploy_project`, `json_contract__replay`, `json_contract__toolchain_list`,
`json_contract__doctor`.

Changed on purpose: `cli_p11__help_bulk` (the retry line). The cli_verbs snapshots are
byte-identical, and so are the other cli_p11 snapshots.

## Redaction rules (json_contract.rs `redact`)

- Keys: `sha256`/`module_sha256`/`actual_sha256`/`inputs_sha256` -> `[SHA]`;
  `toolchain_set`/`sdk_bundle`/`entry`/`bundle_id` -> `[ID]`;
  `run_id`/`retry_of`/`existing_run_id` -> `[RUN_ID]`; `bytes`/`size_bytes` -> `[SIZE]`;
  `record_version` -> `[N]`; `platform` -> `[PLATFORM]`; `glibc` -> `[VER]`; a string
  `version` (tool versions; workflow versions are numbers and stay) -> `[VER]`; `ms` and
  `*_ms` -> `[MS]`; `*_at` -> `[TIMESTAMP]`. Null values are left alone.
- Values: any 64-hex string -> `[SHA]`; any absolute path (`/`, `C:\`, `C:/`, `\\`) ->
  `[PATH]`; the test's temp root inside a longer string -> `[PATH]`.
- doctor only: `cc`/`docker` `present` -> `[BOOL]` and `path` -> `[PATH]` (even when
  null), `musl` -> `[BOOL]`, `glibc` -> `[VER]`, `warnings` -> `[WARNINGS]`.

## main.rs base

The base was 7c394db. main moved to 365ba05 (10-13) before the CAS, but 10-13 did not
touch main.rs, so the append-only `Completions` variant and arm applied unchanged.
The generated scripts include the Phase 11 verbs (rerun, dlq, bulk, limits).
cli_p11 `completions_list_phase11_verbs` checks them in all five shells.

## Verification

- `cargo test -p ostro --test completions`: 6 passed (4 own and 2 p10 helper tests)
- `cargo test -p ostro --test json_contract`: 6 passed (4 own and 2 p10 helper tests; about 41 s, with offline cargo builds of the fixture and a spawned dev)
- `cargo test -p ostro-cli`: 78 passed (including `exit_codes_match_table`)
- `cargo test -p ostro --test cli_verbs`: 7 passed (snapshots unchanged); `cli_p11` help + completions: 2 passed; `p10_scaffold`: 6 passed
- `cargo clippy -p ostro -p ostro-cli --all-targets -D warnings` clean; `cargo fmt --check` clean; `cargo check --workspace --all-targets` clean after the rebase onto 365ba05
- Full `scripts/ci-local.sh --no-musl` was not run (four parallel agents shared the machine; 10-20 owns the final gate)

## Deviations from Plan

- Worktree/target dir: per the coordinator brief, I used a scratchpad worktree and `target/slot-6` instead of `/tmp/ostro-wt-10-18` and `/tmp/ostro-target-10-18`.
- [Coordinator hand-off] Files outside files_modified: `cli_p11.rs` (completions check), `commands/mod.rs` and `commands/bulk.rs` (one help line each), and `cli_p11__help_bulk.snap`.
- json_contract.rs uses `mod common;` (for `guest_wasm` / `repo_root`) as well as p10. The plan said to use p10 only, or the read-only common helpers; common is used read-only.

## Open Issues

- The OpenAPI description of the bulk `retry` action (`crates/ostro-api/src/phase11.rs:376`, in the frozen openapi snapshot) still says "Retry (resume) failed runs.". I left it alone because the OpenAPI snapshot is frozen.
- The json_contract build/deploy tests need the system Rust toolchain + wasm32-wasip2, as build_cli.rs does.

## Self-Check: PASSED
