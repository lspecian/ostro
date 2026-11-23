# Testing workflows locally (test kits)

Ostro ships a test kit for Rust, Python (pytest), TypeScript (node:test) and Go. Each
kit starts a throwaway `ostro dev --ephemeral --time-skipping` server, deploys your
workflows into it and drives runs through the REST client. A workflow that sleeps for
three days finishes in well under a second of wall time: the server skips its clock
forward whenever it has nothing else to do (see [Time skipping](#time-skipping)).

## How the kits work

1. **Find the binary.** The kit uses `OSTRO_BIN` if it is set, otherwise `ostro` on
   `PATH`. It runs `ostro build-info --json` and refuses a binary whose `version` is older
   than the kit's (0.1.0).
2. **Spawn the server**:
   `ostro dev --ephemeral --time-skipping --listen 127.0.0.1:0`. It runs in a fresh temp
   directory with these environment changes:
   - inherited server configuration (`OSTRO_<SECTION>__*`, `OSTRO_LISTEN`, `RUST_LOG`) is
     removed;
   - `OSTRO_RUNS__RETENTION=87600h` is set, so finished runs never expire while days are
     skipped;
   - with loopback egress turned on, `OSTRO_EGRESS__ALLOW_PRIVATE=127.0.0.0/8` is set (see
     [HTTP mocking](#http-mocking)).
3. **Wait for the ready line.** The kit reads stdout (the server also logs there) until a
   line matches the frozen ready-line contract. Group 2 is the bound address and group 4 is
   present for an ephemeral server:

   ```
   ^(.+?) listening on http://(\S+) \(data: (.*?)(, ephemeral)?\) ready in (\d+) ms$
   ```

   For example: `ostro dev listening on http://127.0.0.1:41234 (data: /dev/shm/ostro-x, ephemeral) ready in 212 ms`.

   This format is frozen. `crates/ostro/tests/ready_line.rs` checks it against the binary,
   with and without `--time-skipping` and `--watch`, and checks that all four kits carry
   this exact pattern. The kit gives up after 30 s and reports the server's stderr.
4. **Clean up.** The server is killed and its temp directory removed when the kit is done:
   - Rust: when `TestEnv` is dropped;
   - Python: on `close()` or at the end of the `with` block;
   - TS: on `await close()` (SIGTERM, then SIGKILL after 2 s);
   - Go: through `t.Cleanup`.

## Setup per language

`ostro init` projects already contain a passing test (`cargo test`, `pytest`,
`npm test`). It uses the kit copy that `ostro init` / `ostro build` materialise under
`target/ostro/sdk/`. The kits are not published yet. Once they are, you will install
them like this:

| Language | Until published (bundled) | After publishing |
|----------|---------------------------|------------------|
| Rust | `ostro-client = { path = "target/ostro/sdk/rust/ostro-client", features = ["testing"] }` (dev-dependency) | `cargo add --dev ostro-client --features testing` |
| Python | `conftest.py` puts `target/ostro/sdk/clients/python` on `sys.path` and sets `pytest_plugins = ["ostro_client.pytest_plugin"]` | `pip install ostro-client`: the plugin registers itself (entry point `pytest11: ostro`) |
| TS | `import { TestEnv } from "../target/ostro/sdk/clients/ts/src/testing.js"` | `npm i -D ostro-client`, then `import { TestEnv } from "ostro-client/testing"` |
| Go | – (no workflow SDK; deploy components built in another language) | `go get github.com/lspecian/ostro/clients/go`, then `import ".../clients/go/ostrotest"` |

## API

Every kit has the same surface, named in each language's style.

| Operation | Rust (`ostro_client::testing`) | Python (`ostro_client.testing`) | TS (`ostro-client/testing`) | Go (`ostrotest`) |
|-----------|-------------------------------|----------------------------------|-----------------------------|------------------|
| start | `TestEnv::start().await?` / `TestEnv::builder().egress_loopback(true).start()` | `TestEnv.start(egress_loopback=False)` (context manager) | `await TestEnv.start({ egressLoopback })` | `ostrotest.Start(t, ostrotest.WithEgressLoopback())` |
| stop | drop / `close()` | `close()` | `await close()` | `t.Cleanup` / `Close()` |
| build + deploy a project | `build_and_deploy(dir)` | `build_and_deploy(dir)` | `buildAndDeploy(dir)` | `BuildAndDeploy(dir)` |
| deploy a prebuilt `.wasm` | `deploy_wasm(path)` / `deploy_wasm_bytes(b, name)` | `deploy_wasm(path, name=None)` | `deployWasm(path, { name })` | `DeployWasm(path[, name])` |
| start a run | `start_run(wf, input)` → `Run` | `start_run(wf, input, **opts)` → `Run` | `startRun(wf, input, opts)` → `{runId, workflow, acceptedAtMs}` | `StartRun(wf, input)` → `Run` |
| signal | `signal(&run, name, payload)` | `signal(run, name, payload)` | `signal(run, name, payload)` | `Signal(run, name, payload)` |
| result (waits, auto-skips) | `result(&run, "30s")` → output (`Err` unless completed) | `result(run, wait="30s")` → `{completed, status, result, output, failure}` | `result(run, { wait: "30s" })` → same as Python | `Result(run, 30*time.Second)` → `*ostro.RunResult` |
| assert completed | `assert_completed(&run, Some(json!(..))).await` | `assert_completed(run, output)` | `assertCompleted(run, output)` | `AssertCompleted(run, want)` |
| manual skip | `skip("72h")` | `skip("72h")` | `skip("72h")` | `Skip(72*time.Hour)` |
| server time (ms) | `now_ms()` | `now_ms()` | `nowMs()` | `NowMs()` |
| history | `history(&run)` (all pages) | `history(run)` | `history(run)` | `History(run)` |
| completed step names | `steps(&run)` | `steps(run)` | `steps(run)` | `Steps(run)` |
| timers (started/fired pairs) | `timers(&run)` → `Vec<Timer>` | `timers(run)` → list of dicts | `timers(run)` | `Timers(run)` |
| allow outbound HTTP | `allow_egress(module_or_wf, prefix)` | `allow_egress(module_or_wf, prefix)` | `allowEgress(moduleOrWf, prefix)` | `AllowEgress(moduleOrWf, prefix)` |
| underlying client | `client()` (`TypedClient`) | `client` (`Client`) | `client` (`Client`) | `Client` (`*ostro.Client`) |

Notes:

- **`accepted_at_ms`** on a `Run` is server time (the dev clock) when the run was
  accepted. `now_ms() - run.accepted_at_ms` is the logical time the run has lived.
- **`output`:** the REST result carries the workflow output in the field `result`. The
  Python and TS kits add `output` with the same value.
- **`timers`** pairs each `timer_started` event with the `timer_fired` whose `op` is the
  start's `seq`. It reports `fire_at_ms` and, once fired, `fired_ms`.
- **Run status:** a run parked on a signal or a timer reads `scheduled` in `get_run`, not
  `running`. The server uses `RunStatusDto::Scheduled` both for runs waiting on a start
  delay and for suspended runs.
- **Redeploying a changed module:** the server refuses a different module under an
  existing workflow name and version. Bump `version` in the source (Rust
  `#[workflow(name, version = N)]`, Python `@ostro.workflow(version=N)`, TS
  `workflow({ name, version: N }, ...)`) before redeploying into the same
  server. A fresh `TestEnv` per test session avoids the issue. Dev-mode replacement is an
  open decision; the kits do not work around it.

## Time skipping

- **Auto trigger.** The server skips its clock when both of these hold:
  - it is fully idle: nothing runnable, no lease or grant, no effect or API write in
    flight, and the log fully applied;
  - at least one result long-poll is waiting.

  It then skips to the next due timer or step backoff. Every kit's `result(..., wait)` is
  such a long-poll, so waiting for a result is what moves time forward. This is the
  behaviour of D-P10-6, which is still under user review.
- **Manual control.** `skip(by)` moves the clock forward explicitly, which is useful to
  observe a run in the middle of a sleep. It waits up to 5 s for in-flight activations
  and then answers `503 busy`. Start the server with `time_skipping("manual")`
  (`time_skipping="manual"`, `{ timeSkipping: "manual" }`,
  `WithTimeSkipping("manual")`) to turn the auto trigger off.
- **Ephemeral only.** `--time-skipping` needs `--ephemeral`, and `ostro server` refuses
  it. Skipped time is never applied to real data.
- **Retention.** Retention timers fire as days are skipped, so the kits set
  `OSTRO_RUNS__RETENTION=87600h` (ten years). Without it, a run could expire before the
  test reads its history.
- **Schedules.** Cron schedules (Phase 8) are not skip targets. The clock never jumps *to*
  a schedule. When a skip passes over schedule times, the schedules fire under their
  catch-up policy.
- **Leases are never skipped.** Lease TTLs and heartbeats stay in real time, and no skip
  happens while a lease is held. Executor retry backoff inside an activation is also real
  time.
- **Embedded placement only.** Time skipping drives the embedded executor of `ostro dev`.
  Remote workers are not part of the idle check.

## HTTP mocking

Workflows reach the network only through the effects bridge, which by default blocks
private and loopback addresses and denies any module without an egress policy. To point
a workflow at a mock server on `127.0.0.1`:

1. Start the kit with loopback egress: `TestEnv::builder().egress_loopback(true)`,
   `TestEnv.start(egress_loopback=True)`, `TestEnv.start({ egressLoopback: true })` or
   `ostrotest.WithEgressLoopback()`. This sets
   `OSTRO_EGRESS__ALLOW_PRIVATE=127.0.0.0/8`, a comma-separated list of CIDRs.
2. Start a local server in the test language: axum, `http.server`, `node:http` or
   `httptest`.
3. Call `allow_egress(module_or_workflow, "http://127.0.0.1:<port>")`. It merges the rule
   into `PUT /v1/egress/{module}` (`{"allow": [...]}`). A workflow name deployed through
   the kit resolves to its module, which is the project name for `build_and_deploy` and
   the file stem for `deploy_wasm`.
4. Run the workflow. Its HTTP effect reaches the mock.

Each kit's own test does exactly this with the `guests/fx-http` component.

## CI

- **Build or install `ostro`** and export `OSTRO_BIN`.
- **Rust:** `cargo test` runs the kit tests. They spawn servers, so `--test-threads=1` is
  only needed when a test relies on wall-clock timing.
- **Python:** `pytest`; the session fixture `ostro_env` starts one server for the whole
  session.
- **TS:** `node --test 'test/**/*.test.js'`. Pass a glob, not a directory: Node 24 runs a
  directory argument as a module.
- **Go:** `go test ./...`.
- **Repository CI:** `scripts/ci-local.sh` runs the four kit suites (Rust through
  `cargo test --workspace`, plus the Python, TS and Go client steps). The polyglot step
  runs `crates/ostro/tests/template_tests.rs`, which runs `ostro init` and then each
  generated test.

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `OSTRO_BIN=… does not exist` | Point `OSTRO_BIN` at a built binary (`cargo build -p ostro` → `target/debug/ostro`) or unset it to use `ostro` on PATH |
| `no ostro on PATH and OSTRO_BIN is not set` | Install ostro or set `OSTRO_BIN` |
| `… is version "0.0.x"; this test kit needs ostro >= 0.1.0` | The binary is older than the kit: update the binary or use a matching kit |
| `no ready line within 30 s` (with stderr) | The server failed to start: the stderr tail says why (e.g. an invalid `OSTRO_*` variable passed through `env`) |
| `result` returns `completed: false` / Rust `did not finish` | The run is waiting for something the test never sends (a signal), or it is stuck in a real-time retry loop |
| `ostro deploy … failed … bump the workflow's version` | A changed module under an existing name and version: bump the version (see the redeploy note under [API](#api)) |
| HTTP effect fails with `egress_denied` / a blocked address | Start with loopback egress and call `allow_egress` for the module |
