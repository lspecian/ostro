---
phase: 10-developer-cli-test-kits
plan: 03
subsystem: cli
tags: [cli, clap, exit-codes, json-envelope, test-harness, ready-line]
requires:
  - phase: 10-developer-cli-test-kits
    provides: "10-01 reservations (verbs, exit code 4, ready-line contract); 10-02 crate skeletons (no Cargo edits here)"
provides:
  - "ClientCommand::{Init, Build, Toolchain, Doctor, Replay} with their final flag sets; each stub exits 2 with `ostro: error: <verb> is not yet implemented (10-NN)`"
  - "Outcome::Diverged -> exit 4; ostro_cli::output::local_json(kind, &Value) -> String (kind/version first)"
  - "crates/ostro/tests/common/p10.rs: parse_ready_line, spawn_dev -> DevProc, run_cli_in, cli_json_in, ostro_bin, http_get"
affects: [10-07, 10-08, 10-11, 10-12, 10-14, 10-17, 10-18]
tech-stack:
  added: []
  patterns:
    - "local verbs run outside the async runtime (`commands::run`) or on the blocking pool (`dispatch`), so their owners may use `commands::block_on`"
    - "local_json written by hand (serde_json preserve_order is off workspace-wide)"
key-files:
  created:
    - crates/ostro-cli/src/commands/init.rs
    - crates/ostro-cli/src/commands/build.rs
    - crates/ostro-cli/src/commands/toolchain.rs
    - crates/ostro-cli/src/commands/replay.rs
    - crates/ostro/tests/common/p10.rs
    - crates/ostro/tests/p10_scaffold.rs
  modified:
    - crates/ostro-cli/src/output.rs
    - crates/ostro-cli/src/commands/mod.rs
key-decisions:
  - "The D-P10-9 ready-line regex `^(\\S+) listening on ...` does not match the frozen line: the binary's names are `ostro dev` / `ostro server` (with a space). The p10 parser implements the regex with the name group widened to lazy `(.+?)`; 10-14/10-17 kits must use the same correction"
  - "replay needs exactly one selector kind: run ids (positional RUN... merged with --run) | --sample | --corpus; a selector is required; --workflow/--status require --sample and conflict with the other selectors (clap waives `requires` when a conflicting arg is present, so the conflicts are explicit)"
  - "local_json(kind, body) takes a &Value and returns the pretty string; body's own kind/version keys are dropped, a non-object body goes under \"result\""
  - "Init/Build/Toolchain/Doctor carry their own --json (no Conn); Replay flattens Conn, whose --json is reused"
requirements-completed: []
duration: ~1h
completed: 2026-11-22
---

# Phase 10 Plan 03: Contract B (CLI verb stubs, exit 4, local JSON, p10 helpers) Summary

**The Phase 10 local verbs parse with their final flags and exit 2 as stubs. Exit code 4 and the `ostro.<verb>.result` envelope helper exist. One shared spawn and ready-line helper serves the Phase 10 binary tests.**

## Commits
- 263690b feat(10-03): Phase 10 CLI verb stubs, exit code 4 and local JSON envelope
- d2bf2ad test(10-03): p10 spawned-binary helpers

## Clap tree (new verbs only; the rest of `ostro --help` is unchanged)
```
  init        Scaffold a workflow project (rust, python or ts)
  build       Build a workflow project into a Wasm component
  toolchain   Manage the hermetic build toolchains (install, list, prune)
  doctor      Check the build environment and toolchains
  replay      Replay recorded runs against a module and report divergence (exit 4)

ostro init [DIR] [--lang rust|python|ts] [--name N] [--force] [--json]                      -> 10-08
ostro build [DIR] [--toolchain auto|hermetic|system|docker] [--docker] [--offline] [--json]
            (hidden --in-container; --docker conflicts with --toolchain; BuildArgs::mode()) -> 10-12
ostro toolchain install [--lang rust|python|ts | --all] [--json]                            -> 10-07
ostro toolchain list [--json]                                                                -> 10-07
ostro toolchain prune [--state] [--json]                                                     -> 10-07
ostro doctor [--json]                                                                        -> 10-07
ostro replay [OPTIONS] <RUN|--run <ID>|--sample <N>|--corpus <DIR>>                          -> 10-11
  [RUN]... --run <ID> --sample <N> --workflow <W> --status <STATUS> --corpus <DIR>
  --module <PATH> --allow-missing  + Conn (--server --json --timeout --ca-file)
```
Each stub prints `ostro: error: <verb> is not yet implemented (10-NN)` on stderr and returns `Outcome::Usage` (exit 2).

## Replay arg group rules
- The group `selector` holds `runs`, `run`, `sample` and `corpus`. It is required, and `multiple(true)` lets positional ids and `--run` combine. `ReplayArgs::run_ids()` returns the positional ids first, then the `--run` ids.
- The run ids conflict with `--sample` and `--corpus`, and `--sample` conflicts with `--corpus`.
- `--workflow` and `--status` require `--sample` and conflict with `runs`, `run` and `corpus`.
- `--module` is `required_unless_present = "corpus"`.

## Ready-line parser
- It is equivalent to `^(.+?) listening on http://(\S+) \(data: (.*?)(, ephemeral)?\) ready in (\d+) ms$`. That is D-P10-9 with the name widened from `\S+` (see the deviation below). Digits are ASCII only, and a value that overflows u64 is rejected.
- Accepted lines:
  - `ostro dev ... ephemeral) ready in 205 ms`
  - `ostro server ... (data: /var/lib/ostro) ready in 1234 ms`
  - an IPv6 address
  - data that contains `) ready in 3 ms, ephemeral`
  - empty data
  - an earlier ` listening on http://` whose address part fails (it is skipped)
- Rejected lines:
  - empty input or an empty name
  - a name with a newline
  - https
  - the TLS fingerprint form
  - an empty address
  - `ready in ms`, `1ms`, a trailing space or a trailing newline
  - `-1`
  - a missing `)`
  - a newline in the tail or in the data
  - unrelated text

## Tests
- `cargo test -p ostro-cli --locked`: 64 lib tests (7 new: init, build, toolchain/doctor, replay merge, sample/corpus, selector rules, stubs through `run` and `dispatch`; exit codes 0..4; local_json), plus phase8_verbs 6. All ok.
- `cargo test -p ostro --test p10_scaffold`: 7 passed. These cover `--help` verbs, per-verb help and flags, stubs exit 2 with the plan id and leave the cwd untouched, replay conflicts exit 2 at clap, a spawned dev answers `GET /v1/workflows` 200 and `ls --json`, and the 2 parser tests.
- `cli_verbs` 7, `cli_p11` 6 and `cli` 5 passed. `git diff --exit-code crates/ostro/tests/snapshots/` is clean.
- `cargo clippy -p ostro-cli -p ostro --all-targets -D warnings` is clean, and `cargo fmt --check` is clean. After the rebase onto 4b65737, `cargo check --workspace --all-targets` is warning-free.
- Full `ci-local --no-musl` was not run. The plan puts it after wave 2.

## Deviations from Plan
1. **[Rule 1 - Bug] The D-P10-9 regex rejects the real ready line.** The real line is `ostro dev listening on http://... ready in N ms`. The regex's `(\S+)` name group cannot match `ostro dev`, so spawn_dev timed out on the first run. The parser widens the name group to lazy `(.+?)`. The frozen line itself is unchanged. The 10-14 and 10-17 plans quote the same regex and need the same fix; 10-DECISIONS.md was not edited because it is outside this plan's files.
2. Worktree and target dir: these followed the coordinator brief (a scratchpad worktree, `target/slot-1`) instead of the plan's `/tmp/ostro-wt-10-03` and `/tmp/ostro-target-10-03`.
3. `--workflow` and `--status` got explicit conflicts. The reason is a clap quirk: `requires` is waived when a conflicting arg is present.
4. `p10.rs` adds `ostro_cmd`, `http_get` and `DevProc::{cli, cli_json, url}` beyond the listed API. `DevProc` also holds `ready`, `stdout_rx` and the temp `cwd`.

## Self-Check: PASSED
