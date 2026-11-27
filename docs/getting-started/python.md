# Getting started: Python

This guide takes a Python workflow from an empty directory to a deployed, running and tested
workflow. Every command here is one that `scripts/clean-machine.sh` runs on a machine with no
language toolchain installed, or that the generated project's own test runs. For the overview
of all three languages, see [getting-started.md](../getting-started.md). Rust and TypeScript
have their own guides: [rust.md](rust.md), [typescript.md](typescript.md).

## 1. Prerequisites

You need only the `ostro` binary ([install.md](../install.md)). `ostro build` downloads what
it needs on first use (uv 0.7.15, a uv-managed CPython 3.12 and the componentize-py 0.25.1
wheel, about 200 MB), pins it and checks its sha256. Nothing from your own Python
installation is used for the build. See [toolchains.md](../toolchains.md).

```sh
ostro --version
ostro doctor                    # what is cached and what will be downloaded
```

## 2. Create a project

```sh
ostro init hello --lang python
cd hello
```

The project has one workflow, `hello`, in `app.py`. It runs a step, sleeps for three days on a
durable timer, then waits for the signal `approve`:

<!-- ostro:snippet path=crates/ostro-build/templates/python/app.py lines=7-26 -->
```python
import ostro


@ostro.step
def greet(name: str) -> str:
    # A durable step: it runs at most once to a recorded outcome; replays return it.
    return f"hello {name}"


@ostro.workflow(name="hello")
async def hello(ctx: ostro.Ctx, inp: dict) -> dict:
    """Greets, sleeps for three days (a durable timer: it survives restarts and deploys),
    then waits for the ``approve`` signal (no timeout)."""
    greeting = await greet(inp["name"])
    await ctx.sleep(3 * 24 * 60 * 60)
    approval = await ctx.wait_signal("approve")
    return {"greeting": greeting, "approved": bool((approval or {}).get("ok"))}


ostro.export()  # last line: binds the module's `workflows` export
```

`ostro.export()` must stay the last line of the module. Workflow dependencies listed in
`pyproject.toml` must be pure-Python wheels. `ostro.toml` is the project manifest; besides the
project name and language it holds:

<!-- ostro:snippet path=crates/ostro-build/templates/python/ostro.toml lines=6-11 -->
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

The workflow declares no version, so it is version 1. Versions are write-once on a persistent
server: declare `@ostro.workflow(name="hello", version=2)` when you change the code
([getting-started.md](../getting-started.md#changing-a-workflow)).

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
and workflows ([ui.md](../ui.md)). `print` output inside the workflow goes to the run's log
(the run page in the UI and `GET /v1/runs/{id}/logs`, see [observability.md](../observability.md)).

## 7. Test with time skipping

The project comes with a pytest test. The `ostro_env` fixture of the Python test kit starts a
throwaway `ostro dev --ephemeral --time-skipping` server for the session, and `ostro_project`
builds and deploys the project once:

<!-- ostro:snippet path=crates/ostro-build/templates/python/tests/test_workflow.py lines=7-17 -->
```python
THREE_DAYS_MS = 3 * 24 * 60 * 60 * 1000


def test_hello_sleeps_three_days(ostro_env, ostro_project):
    # ostro_project has built and deployed this project once for the session.
    run = ostro_env.start_run("hello", {"name": "ada"})
    ostro_env.signal(run, "approve", {"ok": True})
    res = ostro_env.result(run, wait="30s")  # auto time-skip covers the 3-day sleep
    assert res["status"] == "completed"
    assert res["output"] == {"greeting": "hello ada", "approved": True}
    assert ostro_env.now_ms() - run.accepted_at_ms >= THREE_DAYS_MS
```

```sh
pytest                          # needs the ostro binary on PATH (or OSTRO_BIN)
```

Ostro does not download pytest. Any Python 3.12 with pytest works, for example
`uv run --no-project --python 3.12 --with pytest python -m pytest tests -q`. Until
`ostro-client` is on PyPI, `tests/conftest.py` imports the kit from the SDK bundle that
`ostro build` writes under `target/ostro/sdk/clients/python`. The kit API is described in
[testing.md](../testing.md).

## 8. Before you change the code: replay

Replay the new build against recorded runs before you deploy it. A divergence exits with
status 4:

```sh
ostro build && ostro replay --sample 20 --module target/ostro/hello.wasm
```

Importing `ostro` makes `time`, `random`, `uuid4` and `datetime.now()` deterministic;
[determinism.md](../determinism.md) lists the rules and shows how `ctx.patched` makes an
intentional change safe.

## Next steps

- [sdks/python/README.md](../../sdks/python/README.md): the Python SDK reference, the event-loop
  model and its rules.
- [sdk-resources.md](../sdk-resources.md): Python instances are larger than Rust ones; size the
  node for them ([operations-sizing.md](../operations-sizing.md)).
- [workflow-semantics.md](../workflow-semantics.md), [connectors.md](../connectors.md),
  [webhooks.md](../webhooks.md), [cron.md](../cron.md), [kafka-triggers.md](../kafka-triggers.md).
- [install.md](../install.md): run a real server.
