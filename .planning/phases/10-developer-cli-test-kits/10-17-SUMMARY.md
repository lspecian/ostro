---
phase: 10-developer-cli-test-kits
plan: 17
subsystem: test-kits
tags: [sdk-06, sdk-07, test-kit, time-skipping, ready-line, pytest, node-test, go, templates]
requires:
  - phase: 10-developer-cli-test-kits
    provides: "10-13 auto time skipping (result long-poll triggers skips on idle); 10-14 Python/TS/Go clients; 10-15 Python/TS builders; 10-16 ostro deploy (build if needed); 10-09 ostro-client + tests/common_p10.rs; 10-08 SDK bundle + ostro init templates; 10-03 tests/common/p10.rs"
provides:
  - "ostro_client::testing (feature testing): TestEnv, TestEnvBuilder, ReadyLine, Run, Timer, TestError, READY_LINE_PATTERN, parse_duration, ostro_bin/resolve_ostro_bin, check_version"
  - "ostro_client.testing.TestEnv + ostro_client.pytest_plugin (pytest11: ostro; fixtures ostro_env, ostro_project)"
  - "ostro-client/testing (src/testing.js + testing.d.ts; package.json exports ./testing)"
  - "github.com/lspecian/ostro/clients/go/ostrotest (Start/StartE + Env methods)"
  - "crates/ostro/tests/ready_line.rs (ready line frozen), crates/ostro/tests/template_tests.rs (SC4, polyglot)"
  - "OSTRO_EGRESS__ALLOW_PRIVATE as a comma-separated env list"
  - "docs/testing.md"
affects: [10-19, 10-20]
tech-stack:
  added: []
  patterns:
    - "kit = spawned `ostro dev --ephemeral --time-skipping --listen 127.0.0.1:0` in a temp cwd, OSTRO_<SECTION>__* scrubbed, OSTRO_RUNS__RETENTION=87600h, ready line scanned on stdout (server logs precede it), stdout drained to EOF"
    - "one pattern literal per kit, cross-checked by a Rust test that greps the four kit sources"
key-files:
  created:
    - crates/ostro-client/src/testing.rs
    - crates/ostro-client/tests/kit.rs
    - clients/python/ostro_client/testing.py
    - clients/python/ostro_client/pytest_plugin.py
    - clients/python/tests/test_kit.py
    - clients/ts/src/testing.js
    - clients/ts/src/testing.d.ts
    - clients/ts/test/kit.test.js
    - clients/go/ostrotest/ostrotest.go
    - clients/go/ostrotest/ostrotest_test.go
    - crates/ostro/tests/ready_line.rs
    - crates/ostro/tests/template_tests.rs
    - docs/testing.md
  modified:
    - crates/ostro-client/src/lib.rs
    - crates/ostro-server/src/config.rs
    - clients/python/pyproject.toml
    - clients/ts/package.json
    - clients/go/ostro/doc.go
    - crates/ostro-build/templates/rust/src/lib.rs
    - crates/ostro-build/templates/ts/package.json
    - crates/ostro-cli/tests/snapshots/init__tree_ts.snap
key-decisions:
  - "The ready line is frozen as the amended D-P10-9 regex `^(.+?) listening on http://(\\S+) \\(data: (.*?)(, ephemeral)?\\) ready in (\\d+) ms$`. It is NOT the first stdout line: `ostro dev` logs its INFO lines (raft etc.) on stdout before it, so every kit scans all lines and keeps draining stdout"
  - "OSTRO_EGRESS__ALLOW_PRIVATE could not be set from the environment at all (config-rs gave a string where a sequence is expected); it now takes a comma-separated list (list_separator + with_list_parse_key for egress.allow_private only). The kits set 127.0.0.0/8 for loopback mocks"
  - "HTTP mocking = [egress] allow_private (env) + PUT /v1/egress/{module} {allow:[origin]} merged with the existing rules; /v1/grants is for secrets and is not needed"
  - "Rust result(run, wait) returns the output and errors unless status == completed (matches the 10-08 template); Python/TS result() return the REST body plus `output` (= wire `result`), Go Result returns *ostro.RunResult"
  - "Run.accepted_at_ms = GET /v1/runs/{id} created_at_ms (equal to history run_started.details.accepted_at_ms, dev clock)"
  - "allow_egress resolves a workflow name to the module the kit deployed it under (deploy_wasm: file stem with _ -> -, build_and_deploy: project name), otherwise treats the argument as the module name"
  - "Rust kit child = std::process::Child (kill + wait on drop, so the pid is reaped synchronously); tokio is used only for sleeps and `ostro deploy` (no new tokio features: the bundled manifest is fixed by 10-02)"
  - "The Rust template's `ostro_sdk::export!` is cfg(target_arch = \"wasm32\"): host `cargo test` builds the cdylib for the host, and rust-lld rejects the WIT export symbol names (`cabi_post_ostro:runtime/workflows@0.1.0#describe`) in the version script"
requirements-completed: [SDK-06, SDK-07]
duration: ~2h30m
completed: 2026-11-23
---

# Phase 10 Plan 17: Test kits (Rust, Python/pytest, TS/node:test, Go) Summary

**Four test kits start a throwaway `ostro dev --ephemeral --time-skipping`, deploy into it and run workflows. A 3-day sleep finishes in 60–90 ms of wall time in every language through the server's auto time skipping. The ready line is frozen and cross-checked across the kits, and every `ostro init` template's generated test passes in its own language.**

## Commits
- fcb89a3 feat(10-17): Rust test kit (ostro-client testing) with auto time skipping
- 4a3cc57 feat(10-17): Python (pytest plugin) and TS test kits
- b75e65c feat(10-17): Go ostrotest, passing template tests in rust/python/ts, docs/testing.md

## Ready line (frozen)
```
^(.+?) listening on http://(\S+) \(data: (.*?)(, ephemeral)?\) ready in (\d+) ms$
```
How each kit holds the pattern:

| Kit | Form |
|---|---|
| Rust | `READY_LINE_PATTERN` (raw string), with a hand-written matcher of the same semantics (no regex crate) |
| Python | `READY_LINE_PATTERN` (`r"..."`) |
| TS | ``String.raw`...` `` passed to `new RegExp` (a `/.../` literal would need `\/`) |
| Go | `ReadyLinePattern` (backquoted) |

`ready_line.rs` checks four things:
- each kit source file contains the literal;
- the Rust constant equals it;
- `dev --ephemeral`, `dev --ephemeral --time-skipping` and `dev --ephemeral --watch <rust project>` each print exactly one matching line, and no earlier line contains `listening on http://`;
- the line rebuilt from the five groups equals the printed line, and the `common/p10.rs` parser agrees.

## Kit API
| Operation | Rust | Python | TS | Go |
|---|---|---|---|---|
| start | `TestEnv::start()`, `TestEnv::builder().binary().egress_loopback().time_skipping().env().timeout().start()` | `TestEnv.start(binary, egress_loopback, env, time_skipping, timeout)` (context manager) | `TestEnv.start({binary, egressLoopback, env, timeSkipping, timeoutMs})` | `Start(t, WithBinary, WithEgressLoopback, WithTimeSkipping, WithEnv)` / `StartE` |
| stop | drop / `close()` | `close()` / `with` | `close()` (SIGTERM, SIGKILL after 2 s) | `t.Cleanup` / `Close()` |
| deploy project | `build_and_deploy(dir)` → deploy JSON | `build_and_deploy(dir)` | `buildAndDeploy(dir)` | `BuildAndDeploy(dir)` |
| deploy wasm | `deploy_wasm(path)`, `deploy_wasm_bytes(b, name)` | `deploy_wasm(path, name)`, `deploy_wasm_bytes` | `deployWasm(path, {name})`, `deployWasmBytes` | `DeployWasm(path[, name])`, `DeployWasmBytes` |
| start run | `start_run(wf, input)` / `start_run_with(StartRun)` → `Run{run_id, workflow, accepted_at_ms}` | `start_run(wf, input, **opts)` → `Run` | `startRun(wf, input, opts)` → `{runId, workflow, acceptedAtMs}` | `StartRun` / `StartRunWith` → `Run{ID, Workflow, AcceptedAtMs}` |
| signal | `signal(&run, name, payload)` | `signal(run, name, payload)` | `signal(run, name, payload)` | `Signal(run, name, payload)` |
| result | `result(&run, "30s")` → output | `result(run, wait="30s")` → body + `output` | `result(run, {wait})` → body + `output` | `Result(run, d)` → `*ostro.RunResult` |
| assert | `assert_completed(&run, Option<Value>)` | `assert_completed(run, output)` | `assertCompleted(run, output)` | `AssertCompleted(run, want)` |
| skip / now | `skip("72h")`, `now_ms()` | `skip`, `now_ms` | `skip`, `nowMs` | `Skip(d)`, `NowMs()` |
| history / steps / timers | `history`, `steps`, `timers` → `Vec<Timer>` | same (timers as dicts) | same (camelCase) | `History`, `Steps`, `Timers` |
| egress | `allow_egress(module_or_wf, prefix)` | `allow_egress` | `allowEgress` | `AllowEgress` |
| client | `client()` (`TypedClient`) | `.client` | `.client` | `.Client` |

Python plugin fixtures:
- `ostro_env` (session; `OSTRO_TEST_EGRESS_LOOPBACK=1` turns on loopback egress);
- `ostro_project` (session): deploys `OSTRO_PROJECT_WASM` (a prebuilt wasm), else `OSTRO_PROJECT_DIR`, else the rootdir.

## Wall times
| Test | Wall |
|---|---|
| Rust kit `sleep_for` 3 d (start → result) | 62–72 ms (4 runs) |
| Rust kit `signal_then_sleep` | 74–85 ms |
| Rust `tests/kit.rs` (9 tests incl. 5 server spawns) | 2.7–3.8 s (warm), 43 s first run (guest build) |
| Python `test_kit.py` (8 tests incl. the subprocess pytest run) | 2.9 s; full `clients/python` suite 25 passed in 9.7 s |
| TS kit 3-day sleep | 67 ms; full `clients/ts` suite 26 pass in 2.2 s |
| Go `ostrotest` package | 1.6–1.9 s |
| Template test, rust (`cargo test`, crates + host + wasm build included) | 60.1 s via template_tests (27.8 s by hand with a warm crates cache; the workflow test itself is inside the 18 s test run, which includes `ostro deploy`'s wasm build) |
| Template test, python (`pytest`, build included) | 14.5 s |
| Template test, ts (`node --test`, build included) | 5.1 s (warm toolchains; 23 s with the first jco npm ci) |
| `template_tests` (3 tests) | 80.5 s |

## HTTP mock mechanics
1. The kit starts the server with `OSTRO_EGRESS__ALLOW_PRIVATE=127.0.0.0/8`. This needed the config fix below.
2. The test serves a local mock: axum, `http.server`, `node:http` or `httptest`.
3. `allow_egress("fx-http", "http://127.0.0.1:<port>")` does a `GET /v1/egress/fx-http` and merges the rule (404 counts as empty). It then sends `PUT /v1/egress/fx-http` with `{"allow":[...]}`.
4. `guests/fx-http` GETs `/hello`, and the test decodes `results[0].body_b64` == `hello from the mock`.

The rule must be canonical (`http://127.0.0.1:PORT`). The kits strip a trailing `/`.

## Tests run
| Command | Result |
|---|---|
| `cargo test -p ostro-client --features testing --test kit -- --test-threads=1` | 9 passed (×4) |
| `cargo test -p ostro-client --features testing --lib` | 13 passed (incl. ready-line matcher, durations, versions) |
| `cargo test -p ostro-server --lib -- config::` | 3 passed (new `egress_allow_private_env_is_a_comma_list`) |
| `cd clients/python && uv run --no-project --python 3.12 --with pytest==8.4.2 python -m pytest tests -q` | 25 passed |
| `node --test --test-reporter=tap 'clients/ts/test/**/*.test.js'` | # pass 26, # fail 0 |
| `cd clients/go && go vet ./... && go test ./...` | ok ostro, ok ostrotest |
| `cargo test -p ostro --test ready_line -- --test-threads=1` | 6 passed |
| `cargo test -p ostro --features polyglot --test template_tests -- --test-threads=1` | 5 passed (3 templates + 2 p10 helpers) |
| `cargo test -p ostro-cli --test init`; `-p ostro-build --test scaffold --test sdk_bundle` | 10; 6 + 6 passed |
| `OSTRO_REQUIRE_ALL_CLIENTS=1 cargo test -p ostro-client --test contract` | 5 passed |
| `cargo clippy -p ostro --features polyglot --all-targets -D warnings`; `-p ostro-server -p ostro-client --all-targets --all-features` | clean |
| `cargo fmt --all --check`, `gofmt -l` | clean |

**The full `scripts/ci-local.sh --no-musl` was not run** (the 10-20 gate covers it; four agents share the machine). It picks up the kit tests through these existing steps:
- `cargo test --workspace`: ostro dev-depends on ostro-client with `testing`, so feature unification enables kit.rs;
- the three client steps;
- the polyglot step: `template_tests` is already listed in `OSTRO_POLYGLOT_TESTS`.

## Deviations from Plan
1. **[Rule 1 - Bug] `OSTRO_EGRESS__ALLOW_PRIVATE` was unusable** (`crates/ostro-server/src/config.rs`, outside files_modified).
   - The plan's `'["127.0.0.0/8"]'` value fails with `invalid type: string ..., expected a sequence`. The plain-env override that docs/connectors.md documents could not express an array either.
   - The fix adds `.list_separator(",").with_list_parse_key("egress.allow_private")`; every other key stays a scalar. A unit test covers it.
   - Committed with Task 1 (fcb89a3).
2. **[Rule 1 - Bug] The Rust template's `cargo test` could not link** (`templates/rust/src/lib.rs`, outside files_modified).
   - The host cdylib failed in rust-lld's version script on the WIT export names.
   - The fix: `#[cfg(target_arch = "wasm32")]` on `ostro_sdk::export!` (wasm builds unchanged; the init tree snapshot lists only file names).
   - A follow-up could put the cfg inside the SDK macro, so user projects that drop the template line still work.
3. **[Rule 1 - Bug] The TS template's `npm test` (`node --test test/`) runs 0 tests on Node 24.** Node treats a directory argument as a module (sdks/ts works only because it has `test/index.js`). The script is now `node --test 'test/**/*.test.js'`, and `init__tree_ts.snap` was updated (Task 3 commit).
4. **The ready line is not stdout's first line.** About 1600 INFO log lines (raft, sealing-key warning) precede it on stdout. The test asserts "the first and only line matching the pattern, with no earlier `listening on http://` line". This is a property of the server, which this plan did not change.
5. **Template test files unchanged.**
   - 10-08's `tests/workflow.rs`, `conftest.py`, `test_workflow.py` and `workflow.test.js` were written against this exact surface: `Run.accepted_at_ms`, Rust `result` → output, Python/TS `result` with `status` + `output`, `buildAndDeploy`, `nowMs`.
   - The kits were shaped to them. With fixes 2 and 3, all three pass unmodified.
6. **`common_p10.rs` was not rewritten.** The coordinator's "lift 10-09's helpers into `ostro_client::testing`" is done as follows:
   - `ostro_client::testing` now holds the binary lookup (`ostro_bin`/`resolve_ostro_bin`), the ready-line parser (`ReadyLine::parse`) and the spawn/drain logic;
   - kit.rs uses the kit directly;
   - `typed.rs`/`contract.rs` keep `common_p10.rs`, because they run without the `testing` feature (default `cargo test -p ostro-client`).
7. **Extra kit surface.** Also added:
   - `deploy_wasm_bytes`, `start_run_with`, `pid`/`stderr`/`ready_line` accessors;
   - Go `StartE` (non-test use) and `AllowEgress` (raw `net/http`, so the Go client's route table is unchanged);
   - an AllowEgress map from workflow names to deployed modules.
8. **Worktree, slot and commits** follow the executor brief: scratchpad worktree, `target/slot-2`, worktree index and CAS onto main with ancestry asserted.

## Documented for users (docs/testing.md)
- A changed module needs a workflow version bump to redeploy into the same server (409). Dev-mode replacement is a pending user decision; the kits do not work around it.
- Cron schedules are not skip targets: they fire under their catch-up policy when a skip passes them (10-13).
- `RunStatusDto::Scheduled` also covers suspended runs (10-14).
- The rest of the time-skipping semantics (auto trigger D-P10-6 under USER REVIEW, manual skip with 503 busy, ephemeral only, retention, leases and executor backoff stay real time, embedded placement only) and the troubleshooting table.

## Open issues
- **Server stdout noise:** `ostro dev` prints its log lines on stdout before the ready line (about 1600 lines in 2 s at the default level). The kits cope, but it is noisy for `ostro dev` users. Moving logs to stderr would be a server change.
- **SDK-level fix for host builds:** `ostro_sdk::export!` could gate `__export_world!` on wasm32 itself (see deviation 2).
- **Guest builds in the kit tests:** the tests build `guests/` into `<repo>/target/guests`. The Rust kit test builds the whole guests workspace (common_p10), and the Python/TS/Go tests build `-p p10-time -p fx-http`.
- **Not run:** full ci-local (10-20 gate); Python 3.9 for the kit (it uses only 3.9-compatible syntax but was run on 3.12).

## Self-Check: PASSED
- Files exist: crates/ostro-client/src/testing.rs, crates/ostro-client/tests/kit.rs, clients/python/ostro_client/{testing,pytest_plugin}.py, clients/python/tests/test_kit.py, clients/ts/src/testing.{js,d.ts}, clients/ts/test/kit.test.js, clients/go/ostrotest/ostrotest{,_test}.go, crates/ostro/tests/{ready_line,template_tests}.rs, docs/testing.md.
- Commits fcb89a3, 4a3cc57 and b75e65c are on main.
