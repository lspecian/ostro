# Getting started: Rust

This guide takes a Rust workflow from an empty directory to a deployed, running and tested
workflow. Every command here is one that `scripts/clean-machine.sh` runs on a machine with no
language toolchain installed, or that the generated project's own test runs. For the overview
of all three languages, see [getting-started.md](../getting-started.md). Python and
TypeScript have their own guides: [python.md](python.md), [typescript.md](typescript.md).

## 1. Prerequisites

You need only the `ostro` binary ([install.md](../install.md)). `ostro build` downloads the
Rust toolchain it needs on first use (rustup-init, Rust 1.100.0 with `wasm32-wasip2`, and zig
when the host has no C compiler), pins it and checks its sha256. If your own rustup already
has the `wasm32-wasip2` target, it is used instead. See [toolchains.md](../toolchains.md).

```sh
ostro --version
ostro doctor                    # what is cached and what will be downloaded
```

## 2. Create a project

```sh
ostro init hello --lang rust
cd hello
```

The project has one workflow, `hello`. It runs a step, sleeps for three days on a durable
timer, then waits for the signal `approve`:

<!-- ostro:snippet path=crates/ostro-build/templates/rust/src/lib.rs lines=30-47 -->
```rust
/// A durable step: it runs at most once to a recorded outcome; replays return it.
#[step]
fn greet(name: &str) -> Result<String, StepError> {
    Ok(format!("hello {name}"))
}

/// Greets, sleeps for three days (a durable timer: it survives restarts and deploys),
/// then waits for the `approve` signal (no timeout).
#[workflow(name = "hello", version = 1)]
pub fn hello(ctx: &Ctx, input: HelloInput) -> Result<HelloOutput, WorkflowError> {
    let greeting = greet(&input.name)?;
    ctx.sleep(Duration::from_secs(3 * 24 * 60 * 60)).wait()?;
    let approval: Approval = ctx.wait_signal("approve", None).wait()?;
    Ok(HelloOutput {
        greeting,
        approved: approval.ok,
    })
}
```

`ostro.toml` is the project manifest. `ostro init` fills in the project name; the rest is:

<!-- ostro:snippet path=crates/ostro-build/templates/rust/ostro.toml lines=6-11 -->
```toml
[build]
# auto | hermetic | system | docker
toolchain = "auto"

[deploy]
activate = true
```

## 3. Start a dev server

In a first terminal:

```sh
ostro dev --ephemeral --time-skipping
```

This serves the API, gRPC and the UI on `http://127.0.0.1:7700`. `--ephemeral` keeps all state
in a throwaway directory, and `--time-skipping` jumps the clock to the next due timer whenever
the server is idle, so the three-day sleep takes no time. A plain `ostro dev` keeps its data in
`./.ostro` and really sleeps. `ostro dev` runs with `[auth] mode = "none"`: no token is needed
on your own machine. `ostro dev --watch` also rebuilds and redeploys the project on every save.

The other commands find the server through `--server` / `OSTRO_SERVER` (default
`http://127.0.0.1:7700`).

## 4. Build and deploy

In a second terminal, inside `hello/`:

```sh
ostro build                     # -> target/ostro/hello.wasm (a Wasm component)
ostro deploy                    # builds if stale, uploads, activates
```

`ostro deploy` uploads the component and activates the workflow version it declares
(`version = 1`). Versions are write-once on a persistent server: bump the version when you
change the code ([getting-started.md](../getting-started.md#changing-a-workflow)).

## 5. Trigger a run

From the CLI:

```sh
ostro run hello --input '{"name":"ada"}'            # prints the run id
ostro signal <run-id> approve --payload '{"ok":true}'
ostro result <run-id> --wait                        # prints the output once the run completes
```

`ostro run hello --input '{"name":"ada"}' --wait` starts the run and waits for its result in
one command (send the signal from another terminal meanwhile).

Over HTTP, the same run is:

```sh
curl -sS -X POST http://127.0.0.1:7700/v1/workflows/hello/runs \
  -H 'content-type: application/json' -d '{"input":{"name":"ada"}}'
# {"run_id":"…","created":true}
curl -sS -X POST "http://127.0.0.1:7700/v1/runs/<run-id>/signals/approve" \
  -H 'content-type: application/json' -d '{"payload":{"ok":true}}'
curl -sS "http://127.0.0.1:7700/v1/runs/<run-id>/result?wait=30s"
```

Against a server with authentication on (`ostro server` defaults to `[auth] mode = "token"`),
pass a bearer: the CLI reads it from `OSTRO_TOKEN`, and curl needs
`-H "Authorization: Bearer $OSTRO_TOKEN"`. A token for this needs the `trigger` scope (and
`read` to fetch the result); see [tenancy-and-auth.md](../tenancy-and-auth.md).

## 6. Observe

```sh
ostro ls                        # recent runs
ostro inspect <run-id>          # one run: status, workflow, version, output
ostro history <run-id>          # the run's journal, event by event
```

The embedded UI is at <http://127.0.0.1:7700/ui/>: runs, histories, the dead-letter queue
and workflows ([ui.md](../ui.md)). Metrics are on `/metrics` ([metrics.md](../metrics.md)).

## 7. Test with time skipping

The project comes with a test that starts its own throwaway
`ostro dev --ephemeral --time-skipping` server through the Rust test kit
(`ostro_client::testing::TestEnv`), deploys the project and checks that the run completes after
three days of server time:

<!-- ostro:snippet path=crates/ostro-build/templates/rust/tests/workflow.rs lines=11-36 -->
```rust
#[tokio::test]
async fn hello_completes_after_three_days_and_approval() {
    let env = TestEnv::start().await.expect("start ostro dev");
    env.build_and_deploy(env!("CARGO_MANIFEST_DIR"))
        .await
        .expect("build and deploy");

    let run = env
        .start_run("hello", json!({ "name": "ada" }))
        .await
        .expect("start hello");
    env.signal(&run, "approve", json!({ "ok": true }))
        .await
        .expect("send approve");

    // `result` waits for the run; idle time is skipped (the 3-day sleep).
    let out = env.result(&run, "30s").await.expect("hello completes");
    assert_eq!(out["greeting"], "hello ada");
    assert_eq!(out["approved"], true);

    let now = env.now_ms().await.expect("server time");
    assert!(
        now - run.accepted_at_ms >= THREE_DAYS_MS,
        "the durable sleep lasted three days of server time"
    );
}
```

```sh
cargo test                      # needs the ostro binary on PATH (or OSTRO_BIN)
```

The test kit API (HTTP mocks, manual time skipping) is described in
[testing.md](../testing.md).

## 8. Before you change the code: replay

Replay the new build against recorded runs before you deploy it. A divergence exits with
status 4:

```sh
ostro build && ostro replay --sample 20 --module target/ostro/hello.wasm
```

[determinism.md](../determinism.md) explains what must stay deterministic and how
`ctx.patched` makes an intentional change safe.

## Next steps

- [workflow-semantics.md](../workflow-semantics.md): signals, timeouts, cancel, retry, children,
  continue-as-new.
- [connectors.md](../connectors.md): HTTP, SQL and object-store effects with secrets.
- [webhooks.md](../webhooks.md), [cron.md](../cron.md), [kafka-triggers.md](../kafka-triggers.md):
  start runs from events.
- [sdks/rust/README.md](../../sdks/rust/README.md): the Rust SDK reference.
- [install.md](../install.md) and [operations-sizing.md](../operations-sizing.md): run a real
  server.
