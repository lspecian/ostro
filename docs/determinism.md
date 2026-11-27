# Determinism and replay

Ostro runs workflows by **replaying a journal**. Every time a run resumes (after a sleep, a
signal, a crash, a failover or a move to a worker), the workflow code runs again from the
start, and every command it issues is answered from the run's journal until the code reaches
the point where it stopped. For that to work, the code must issue the same commands in the
same order on every replay. This guide explains what Ostro makes deterministic for you, where
the boundary to real I/O is, how divergence is detected, how to gate deploys on it, and how to
change code safely.

## 1. What the sandbox makes deterministic

Workflow modules are WebAssembly components. A module may import only `ostro:runtime/*` and a
fixed set of WASI interfaces that Ostro implements itself; anything else (files, sockets,
the real clock) is rejected when the module is deployed. Inside a workflow:

| Source | What the code gets |
|---|---|
| wall clock (`wasi:clocks/wall-clock`) | the activation's **logical time**, recorded in the journal; identical on replay |
| monotonic clock | logical time since the run was accepted |
| randomness (`wasi:random/*`) | a stream seeded per run from the journal; identical on replay |
| environment, stdin | empty |
| stdout, stderr | the run's log (never journaled, suppressed on replay) |
| filesystem, sockets, HTTP | not available: the module is refused at deploy time |

The SDKs route the language's own APIs through these, so ordinary code stays deterministic:

- **Rust:** `SystemTime::now()` is logical time, `Instant` is logical time since acceptance,
  `std::collections::HashMap` and `rand` are seeded from the run, through the host WASI shim.
  The SDK also offers `ctx.now()`, `ctx.random_u64()`, `ctx.random_bytes(n)` and `ctx.uuid_v7()`.
- **Python:** importing `ostro` makes `time.time()`, `time.monotonic()`, `datetime.now()`,
  `random.random()`, `os.urandom`, `secrets` and `uuid.uuid4` deterministic.
- **TypeScript:** the SDK makes `Date.now()`, `new Date()`, `performance.now()`,
  `Math.random()`, `crypto.getRandomValues` and `crypto.randomUUID` deterministic;
  `setTimeout` is a durable sleep.

The repository's determinism guest uses native Rust APIs exactly like that, and its journal is
replayed and compared in CI:

<!-- ostro:snippet path=guests/det-basic/src/workflow.rs lines=68-79 -->
```rust
    // (1) native wall clock: the journal's logical time.
    let now_secs = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    // (2) std HashMap (RandomState keys) filled with rand keys (WASI random stream).
    let mut map: HashMap<u64, u32> = HashMap::new();
    for i in 0..16u32 {
        map.insert(rand::random::<u64>(), i);
    }
    let order: Vec<u64> = map.keys().copied().collect();
```

Values drawn for the same seed and run id (`random_u64`, `random_f64`, `uuid_v7`,
`idempotency_key`) are byte-identical in Rust, Python and TypeScript
([conformance.md](conformance.md)).

## 2. Steps and effects: the boundary to the real world

Anything that talks to the outside world or is not deterministic belongs in a **step** or an
**effect**:

- A **step** runs its body once to a recorded outcome. On replay the step is not executed; its
  recorded result is returned. Randomness drawn inside a step comes from a step-local stream,
  so it does not shift the workflow's own stream.
- An **effect** (HTTP, SQL, object store) is a request the host executes outside the guest,
  with stable idempotency keys, and whose result is journaled ([connectors.md](connectors.md)).
  Secrets are filled in by the host and never enter guest memory.

A step body cannot wait on durable operations (sleep, signals, children); those belong in the
workflow body. Branching on values that differ between attempts is a replay bug, for example
on `ctx.info().attempt`, which grows on every re-dispatch: use it for logs only.

## 3. How divergence is detected

Each command the code issues (a step, a timer, a signal wait, a child, an effect, a patch
marker, ...) is journaled with a **command hash** that identifies it: its kind, its name and
the arguments that identify it. On replay the engine compares every command the code issues,
in order, with the journaled one. A mismatch is **nondeterminism**:

- the activation fails with `nondeterminism` and the sequence number of the first mismatch;
- the run is **not** corrupted: nothing is written past the mismatch, and the run goes to the
  dead-letter queue as resumable ([operations.md](operations.md#dead-letter-queue));
- you can fix the code and retry the run, or rerun it ([operations.md](operations.md#rerun)).

Typical causes: reordering steps, renaming a step, changing step arguments that are part of its
identity, adding or removing a step before code that existing runs already passed, or deciding
control flow from something that is not journaled (a log line, a global counter, the attempt
number).

## 4. `ostro replay` as a CI gate

`ostro replay` runs a new build against recorded runs of a server **before** you deploy it.
The check runs on the server (journals and keys never leave it), executes nothing (no step body,
effect or timer runs), and reports one verdict per run: `ok`, `ok_frontier` (an unfinished run
whose whole prefix replayed), `diverged` (with the sequence number and the expected and actual
command), `skipped` or `error`. Any divergence exits with status **4**.

```sh
set -euo pipefail
ostro build                                         # -> target/ostro/orders.wasm
ostro replay --server "$OSTRO_SERVER" --module target/ostro/orders.wasm --sample 50
ostro deploy target/ostro/orders.wasm --activate    # only if replay passed
```

Point it at an environment whose runs represent production traffic, raise `--sample` for
workflows with many code paths, and add explicit run ids for rare branches. `--corpus` replays
journals exported to files, offline. Details, flags and exit codes: [replay.md](replay.md). The
token needs `read` for the check and `deploy` for the module upload it does first.

## 5. Changing code safely: versions, `patched` and `migrate-runs`

**Runs are pinned.** A run executes the module it started with until it ends. A new workflow
version (`#[workflow(name = "orders", version = 2)]`, `@ostro.workflow(..., version=2)`,
`workflow({ name, version: 2 }, ...)`) affects new runs only. Versions are write-once on a
server: bump the version for every change you deploy.

**`ctx.patched(change_id)`** lets one version serve old and new runs. The first call journals a
marker in new runs and returns true; a journal recorded before the change has no marker, so it
returns false and takes the old path. The answer is memoised per change id. Once no open run
needs the old path, delete the branch together with the `patched` call: markers the code no
longer asks about are skipped on replay.

The repository's replay gate (`crates/ostro/tests/replay_cli.rs`) records runs on v1 (steps
`a`, `b`), shows that v2 (steps swapped) diverges with exit 4, and that v3 (the swap behind
`patched("reorder")`) replays cleanly, in all three languages:

<!-- ostro:snippet path=guests/p10-replay-v3/src/lib.rs lines=24-33 -->
```rust
#[workflow(name = "replay_demo", version = 3)]
pub fn replay_demo(ctx: &Ctx, input: serde_json::Value) -> Result<Vec<String>, WorkflowError> {
    let steps = if ctx.patched("reorder") {
        vec![step_b()?, step_a()?]
    } else {
        vec![step_a()?, step_b()?]
    };
    ctx.sleep(sleep_of(&input)).wait()?;
    Ok(steps)
}
```

<!-- ostro:snippet path=crates/ostro/tests/fixtures/replay/py-v3/app.py lines=26-33 -->
```python
@ostro.workflow(name="py_replay_demo", version=3)
async def replay_demo(ctx: ostro.Ctx, input: object) -> list:
    if ctx.patched("reorder"):
        steps = [await b(), await a()]
    else:
        steps = [await a(), await b()]
    await ctx.sleep(sleep_of(input))
    return steps
```

<!-- ostro:snippet path=crates/ostro/tests/fixtures/replay/ts-v3/index.ts lines=16-22 -->
```typescript
workflow({ name: "ts_replay_demo", version: 3 }, async (ctx: Ctx, input: unknown) => {
  const steps = ctx.patched("reorder")
    ? [await step("b", () => "b"), await step("a", () => "a")]
    : [await step("a", () => "a"), await step("b", () => "b")];
  await ctx.sleep(sleepMs(input));
  return steps;
});
```

**`ostro migrate-runs`** moves running runs from one version to another, but only the runs whose
own journal replays cleanly against the target. Dry-run first:

```sh
ostro migrate-runs --workflow orders --from 3 --to 4 --dry-run
ostro migrate-runs --workflow orders --from 3 --to 4
```

Runs that diverge stay on their version and are listed with the sequence number of the
mismatch; the command exits 4 when any run fails the check. See the
[upgrade runbook](upgrade-runbook.md#3-ostro-migrate-runs-move-live-runs-to-a-new-version).
After a migration a run's module hash (as the guest sees it in run info) is the new module:
never use it for workflow logic or idempotency keys.

## 6. Pitfalls per language

**All languages**

- Do not branch on `attempt`, log output, or anything read outside a step or effect.
- Put real I/O in steps or effects. A step body runs at most once to a recorded outcome, but
  may run more than once before that (retries, crashes): make it idempotent.
- Do not keep state in globals across waits: an instance can be evicted at any wait and the
  run rebuilt from its journal in a fresh instance, on any node. The journal records commands,
  not variables.

**Rust** ([sdks/rust/README.md](../sdks/rust/README.md#determinism))

- `random_u64` / `random_bytes` share one stream and `uuid_v7` has its own; adding a
  `uuid_v7()` call does not shift later `random_u64()` values.
- Durable operations (`ctx.sleep`, `ctx.wait_signal`, effects, children) return handles;
  `.wait()` blocks the workflow until the operation completes.

**Python** ([sdks/python/README.md](../sdks/python/README.md#determinism-rules-wf-07))

- `set` iteration over `str` depends on the build's hash seed, which differs per build: iterate
  such sets through `sorted()`. `dict` order is deterministic.
- Explicitly seeded `random.Random(seed)` instances are left alone (they are deterministic
  anyway).
- `ctx.sleep(...)` and `ctx.wait_signal(...)` allocate their operation at once, while a
  coroutine such as `asyncio.sleep(...)` passed to `gather` allocates when its task first runs:
  switching between the two forms changes the journal.
- Call nothing on `ctx` at module level, import everything at the top of the module, and keep
  `ostro.export()` as the last line. A host call from a finalizer raises `ProtocolError`.

**TypeScript** ([sdks/ts/README.md](../sdks/ts/README.md#determinism-rules-wf-07))

- `Map`, `Set` and object key order are insertion-ordered and deterministic.
- `setTimeout` is a durable sleep; inside a step it is an `sdk_protocol` error.
- `WeakRef` holds a strong reference and `FinalizationRegistry` does nothing, because garbage
  collection timing differs between a live run and a replay.
- The entry module must re-export `workflows` from `ostro`.

Deterministic builds matter too: the same source must produce a module that issues the same
commands. The Python and TypeScript toolchains are pinned for that reason
([sdk-builds.md](sdk-builds.md), [toolchains.md](toolchains.md)), and `ostro replay` catches a
build that does not.
