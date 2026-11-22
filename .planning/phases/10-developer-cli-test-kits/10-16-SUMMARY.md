---
phase: 10-developer-cli-test-kits
plan: 16
subsystem: cli
tags: [ostro-cli, deploy, dev-watch, notify, cli-05, cli-08]
requires:
  - phase: 10-developer-cli-test-kits
    provides: "10-12 ostro_build::build / build_with / pipeline::IGNORE / project::load / manifest::read and the rust-offline fixture; 10-09 ostro-client (raw Client: upload_module, list_workflows, activate); 10-02 watch.rs stub + notify/notify-debouncer-full pins; 10-03 local_json + tests/common/p10.rs"
provides:
  - "ostro deploy [DIR] (default .): build if stale, upload, activate the versions not active before the upload; --activate[=BOOL], --no-build, --toolchain, --docker; ostro.deploy.result envelope"
  - "ostro_cli::commands::deploy::{deploy_project, DeployOptions, DeployReport, DeployError, render_json, render_human, build_json}"
  - "ostro_cli::watch::{run_watch(opts, server, shutdown), WatchOptions::new, check_project, relevant, DEBOUNCE_MS}"
  - "ostro dev --watch [DIR] (main.rs: append-only flag, run_server gains an Option<WatchOptions>, spawn_watch helper)"
  - "crates/ostro/tests/deploy_build.rs (3 offline + 2 polyglot), crates/ostro/tests/dev_watch.rs (2)"
affects: [10-17, 10-18, 10-19, 10-20]
tech-stack:
  added: []
  patterns:
    - "--no-build = pipeline::build_with with a step factory whose step refuses to run: the pipeline's own up-to-date check decides, no duplicated input-hash logic"
    - "blocking build on a dedicated std::thread + oneshot, so neither the CLI runtime nor the server runtime's shutdown waits on cargo"
    - "watch only non-ignored top-level dirs recursively (root non-recursive): no inotify watches or file-id scans under target/ or .git/"
key-files:
  created:
    - crates/ostro/tests/deploy_build.rs
    - crates/ostro/tests/dev_watch.rs
  modified:
    - crates/ostro-cli/src/commands/deploy.rs
    - crates/ostro-cli/src/watch.rs
    - crates/ostro/src/main.rs
key-decisions:
  - "Workflow versions are declared in the source (#[workflow(name, version)]) and the server refuses a second module for the same (name, version) (409). A changed module therefore needs a version bump to deploy; deploy and the watch loop print the 409 with a 'bump the workflow's version' hint, and the watch loop keeps the previous version. The plan's 'edit the greeting -> v2' assumed server-assigned versions; the tests bump the version in the edit"
  - "'Activate only versions not already active' is judged against GET /v1/workflows taken before the upload: the server auto-activates the first version of a new name on upload, and that version is still reported in `activated` on the first deploy (the plan's activated [hello@1])"
  - "A path with the .wasm extension that is not a directory takes the 05-14 path (a missing x.wasm keeps 'cannot read', exit 2); anything else is a project dir"
  - "--activate became Option<bool> with require_equals (--activate, --activate=false); `deploy --activate x.wasm` still parses x.wasm as the path"
  - "Project --json: local_json('deploy', {build, upload, activated}); serde_json sorts keys, so after kind/version the order is activated, build, upload"
  - "Watch output on stderr, prefixed `ostro dev:`: watching, building <name>, deployed <wf> v<N>; new runs use v<N> (<ms> ms), up to date; new runs use v<N>, build|deploy failed; keeping v<N> + the error line"
requirements-completed: [CLI-05, CLI-08]
duration: ~1h15m
completed: 2026-11-22
---

# Phase 10 Plan 16: ostro deploy build-if-needed + ostro dev --watch Summary

**`ostro deploy [DIR]` builds a project only when its input hash changed, uploads `target/ostro/<name>.wasm` idempotently and activates only the versions that were not active before. `ostro dev --watch` runs the same deploy path after the ready line, then again on every debounced save, with one coalesced follow-up per build and broken edits that never take the server or the active version down. The 05-14 `.wasm` form and its snapshots are unchanged.**

## Commits
- bc12cf1 feat(10-16): ostro deploy builds if needed, uploads and activates idempotently
- 5188995 feat(10-16): ostro dev --watch rebuilds and redeploys on save

## Deploy JSON sample (first deploy of a fresh fixture copy)
stdout starts with `"kind": "ostro.deploy.result", "version": 1`, followed by these fields (shown in sorted order as the test re-prints them):
```json
{
  "activated": [
    { "active_version": 1, "name": "hello", "record_version": 1,
      "versions": [ { "default_queue": null, "module_sha256": "de70e8baf8e9d5af8df91e29f38f8ee8fa78c81d347e6776ffc699a7bdd19c87", "run_timeout_ms": null, "version": 1 } ] }
  ],
  "build": {
    "bytes": 148424, "lang": "rust",
    "manifest": "/tmp/.tmpDYgXx6/p/target/ostro/manifest.json",
    "name": "hello-offline",
    "sha256": "de70e8baf8e9d5af8df91e29f38f8ee8fa78c81d347e6776ffc699a7bdd19c87",
    "up_to_date": false,
    "wasm": "/tmp/.tmpDYgXx6/p/target/ostro/hello-offline.wasm",
    "workflows": [ { "name": "hello", "version": 1 } ]
  },
  "kind": "ostro.deploy.result",
  "upload": {
    "created": true,
    "module": { "language": null, "sha256": "de70e8ba…9c87", "size_bytes": 148424,
      "uploaded_at": "2026-11-22T21:47:41.685Z", "uploaded_at_ms": 1795384061685,
      "workflows": [ { "name": "hello", "version": 1 } ] }
  },
  "version": 1
}
```
Human form: the `ostro build` line (`built …` / `up to date: …`), `uploaded <sha12> (created|unchanged, N bytes) as module <name>`, `workflows: hello@1`, then `activated hello@1` lines, `activated: - (already active)` or `not activated (--activate=false)`.

## Idempotency evidence (deploy_build::project_deploy_is_idempotent)
- **1st deploy:** build.up_to_date false, upload.created true, module.workflows [hello@1], activated [hello active 1]. GET /v1/workflows: hello active 1, versions [1]. A run started afterwards is version 1.
- **2nd deploy (no change):** build.up_to_date true, upload.created false with the same sha `de70e8ba…`, activated `[]`, workflows unchanged (active 1, versions [1]). Human output: `up to date: hello-offline …`, `unchanged`, `already active`.
- **Source change + version bump to 2:** new sha, created true, activated [active_version 2], versions [1, 2], and a new run is version 2.
- **Source change without a bump:** exit 1, `… is already registered by module … (the module changed but hello@2 did not: bump the workflow's version in the source to deploy it)`, hello stays active 2.
- **`--activate=false`** (with v3): created true, activated [], active stays 2, versions [1, 2, 3].
- **`--no-build`:** before any build, after deleting the wasm, or after a source edit, it exits 2 with `--no-build: target/ostro/hello-offline.wasm is missing or stale; run \`ostro build\` first`. After `ostro build p` it deploys with build.up_to_date true (relative DIR).
- **Syntax error:** exit 1, cargo's diagnostics on stderr, empty stdout, GET /v1/workflows byte-identical before and after. No project: exit 2 `no Ostro project`.
- **`.wasm` form:** JSON keys still exactly `{activated, upload}`; without `--activate` it prints the bare upload body. `cli_verbs` (7 tests, incl. `deploy_activate.snap`) passes with snapshots byte-identical.

## Watch latencies (dev_watch, WSL2, system rustup, CARGO_BUILD_JOBS=4 under nice)
| step | time |
|---|---|
| spawn → `deployed hello v1` (cold cargo build of a fresh copy) | 18.7 s |
| save → `new runs use v2` (250 ms debounce + warm rebuild + upload + activate) | 1.79 s |
| warm build + deploy as reported by the loop | ~2.0 s |

## Coalescing counts
After v3, one edit (v4) started a build. Five more edits (v5, 40–80 ms apart) were made while it ran. Lines from that point:
```
ostro dev: building hello-offline
ostro dev: deployed hello v5; new runs use v5 (2009 ms)
ostro dev: building hello-offline
ostro dev: up to date; new runs use v5
```
That is 2 builds for 6 edits: the edit's build plus exactly one follow-up. The first build had staged the v5 sources already, so the follow-up was up to date. The test allows 1–2 builds. Noise (target/noise.txt, .git/index, src/.lib.rs.swp, src/4913 created and removed, src/lib.rs~) produced no `ostro dev:` line within 2 s.

## Failure-isolation transcript (broken edit after v2)
```
error: this file contains an unclosed delimiter
  --> src/lib.rs:45:14
   |
45 | fn broken( {
…
error: could not compile `hello-offline` (lib) due to 1 previous error
ostro dev: build failed; keeping v2
ostro dev: cargo build failed: exited with exit status: 101
```
Afterwards the server still answers, GET /v1/workflows shows hello active 2, and a new run is version 2. The fix (v3) gives `new runs use v3`.

## main.rs base check
- **Base:** b65f2ea. main had not moved when either task was committed (bc12cf1 → 5188995 by CAS, ancestry asserted), so no re-apply was needed.
- **10-10's `--time-skipping` has not landed**, so the "--watch together with --time-skipping" case was not testable. The hunk is append-only and does not touch the other `Dev` fields:
  - the `watch` field is added after `data_dir`;
  - one usage check runs before `load_layered`;
  - `run_server` gains a trailing `Option<WatchOptions>` (Server passes `None`);
  - `spawn_watch` runs after `print_ready`, and the loop is dropped before `handle.shutdown()`.
- The ready line is unchanged (dev_startup 4 passed, `dev_watch` asserts the parsed line).

## Tests
- **Task 1 verify:**
  - `CARGO_NET_OFFLINE=true cargo test -p ostro --test deploy_build`: 5 passed;
  - cli_verbs 7 passed;
  - `git diff --exit-code crates/ostro/tests/snapshots/` clean;
  - clippy `-p ostro-cli -p ostro --all-targets -D warnings` clean.
- **Task 2 verify:**
  - `dev_watch`: 4 passed (2 tests + 2 p10 helper tests);
  - dev_startup 4 passed (1 ignored) and cli 5 passed;
  - clippy clean.
- **Polyglot:** `cargo test -p ostro --features polyglot --test deploy_build python_project_deploy rust_template_deploy`: 2 passed (36 s, network). Clippy with `--features polyglot` is clean.
- **Also run:**
  - `cargo test -p ostro-cli`: 77 + 10 + 6, including the new deploy/watch unit tests;
  - p10_scaffold 6 and build_cli 5;
  - `cargo fmt --all --check`;
  - `cargo check --workspace --all-targets --locked`: warning-free.
- **Not run:** `scripts/ci-local.sh --no-musl`. The 10-20 gate covers it, and the disk budget rules out a full run per plan.

## Deviations from Plan
1. **[Rule 1 - Bug in the plan's model] Versions are source-declared.** "Change the greeting → version 2" cannot happen: the SDK macro fixes `version = 1` and the server answers 409 for a different module with the same (name, version). The tests bump the version with the edit. Deploy and watch surface the 409 with a bump hint, and watch keeps the active version. See Open Issues.
2. **First-deploy activation reporting.** "Not already active" is measured before the upload. Otherwise the server's auto-activation of a new name's first version would make the first deploy report `activated []`, against the plan's `[hello@1]`.
3. **`--no-build` side effects.** It goes through `build_with` with a refusing step, so a stale run also:
   - removes `inputs.sha256`;
   - re-materialises `target/ostro/sdk`;
   - re-stages before failing.
   All of these are already invalid at that point, and no wasm or manifest is touched.
4. **Worktree, target dir and index** follow the coordinator brief: scratchpad worktree, `target/slot-1`, private index `index-10-16`.

## Open Issues
- **USER DECISION (D-P10-14 / D-P10-13 follow-up):** `ostro dev --watch` redeploys a changed module only if the workflow version in the source was bumped. Otherwise every save of a changed module gives `deploy failed; keeping vN` plus the hint. Possible follow-ups (none is in this plan's scope, and each touches frozen surfaces):
  - (a) dev-only server support to replace or auto-number versions;
  - (b) the watch loop rewriting versions in a staged copy;
  - (c) keep it as is and document it.
- **Help text.** The root `--help` line for deploy still reads "Upload a Wasm component (no build) and optionally activate its workflows". The doc comment is in `commands/mod.rs`, which is outside this plan's files_modified. `ostro deploy --help` itself documents both forms.
- **`--watch --time-skipping`** is untested until 10-10 lands (expected to compose, since the flags are independent).
- **TLS dev listeners.** `spawn_watch` uses the endpoint's scheme. With a self-signed TLS dev listener, the watch client trusts only platform roots, so its deploys would fail. The default dev listener is plain http.

## Self-Check: PASSED
- Files on main: crates/ostro-cli/src/commands/deploy.rs, crates/ostro-cli/src/watch.rs, crates/ostro/src/main.rs, crates/ostro/tests/deploy_build.rs, crates/ostro/tests/dev_watch.rs.
- Commits bc12cf1 and 5188995 are on main.
