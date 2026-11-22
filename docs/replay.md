# Replay: catch non-determinism before you deploy

`ostro replay` replays recorded workflow journals against a new module version. It
reports, run by run, whether the new code would make the same decisions as the old one.
Run it in CI before `ostro deploy --activate`: a replay that diverges exits **4**, and the
pipeline stops before broken code reaches running workflows.

## What replay checks

A workflow run is a journal: `RunStarted`, then one event per command the code issued
(step completions, timers, signals, children, effects, patch markers, ...), then a
terminal event when the run ended. Each command event carries a **command hash** that
identifies what was issued (the command kind, the step name, the arguments that identify
it). On replay the engine runs the new module over the stored journal and compares every
command the code issues, in order, with the journaled one.

Each run gets one verdict:

| Verdict | Meaning |
|---|---|
| `ok` | The journal is terminal, and the module reproduced its terminal outcome (same result, failure or continuation) without issuing anything new. |
| `ok_frontier` | The journal is not terminal (the run is still in progress), its whole prefix replayed, and the module made progress past the end. This is what the live run will do next. |
| `diverged` | The module issued a different command than the journal at `seq` (`detail` shows the expected and the actual command), or a terminal journal's outcome changed. |
| `skipped` | The module does not export the run's workflow. |
| `error` | The check itself could not run (unreadable journal, engine error). |

Replay **executes nothing**. Steps that were journaled return their recorded results.
Effects (HTTP, SQL, objects, ...) are intents only, and the check's sink refuses the first
append past the journal end, so no step body, effect, timer or child runs live. A
terminal journal must replay with zero new events; a running run is `ok_frontier` as soon
as its prefix matches.

## Why it runs on the server

Journals can contain business data. Replaying await-token and run-info calls
byte-identically needs the cluster's await-token key. So the remote mode runs **on the
server** (`POST /v1/modules/{sha}/replay`):

- the CLI uploads the module (idempotently, never activating it);
- the server selects the runs, reads their journals and replays them with its own
  runtime and key, two runs at a time, preparing the module once per request;
- only the verdicts come back (run id, workflow, verdict, seq, detail, appended).

No journal, payload or key leaves the server. The module is uploaded under the file
stem as its name, as `ostro deploy` would name it. A later `ostro deploy` of the same
file is therefore a no-op upload followed by activation.

## Remote usage

```sh
# the newest 50 runs of every workflow the module describes, any status
ostro replay --module target/ostro/orders.wasm --sample 50

# only one workflow, only completed runs
ostro replay --module orders.wasm --sample 20 --workflow order --status completed

# explicit runs (positional ids and --run merge)
ostro replay 0030... 0050... --run 0070... --module orders.wasm

# machine-readable: the ostro.replay.result envelope
ostro replay --module orders.wasm --sample 50 --json
```

Selection rules:

- Give exactly one selector: run ids, `--sample N`, or `--corpus DIR`.
- `--workflow` and `--status` only go with `--sample`.
- `--sample` is capped at **1000** runs per workflow. A larger value is a 400 with
  `details.reason = "sample_too_large"`. An explicit list may hold up to 1000 ids.
- An unknown module or run id fails the whole request (404). Use the ids `ostro ls`
  shows.
- `--allow-missing` accepts `skipped` runs (workflows the new module no longer
  exports) instead of failing the gate.

The human output is a table (`RUN WORKFLOW VERDICT SEQ DETAIL`) plus a summary line. With
`--json` the output is:

```json
{
  "kind": "ostro.replay.result",
  "version": 1,
  "module_sha256": "…",
  "summary": {"ok": 3, "ok_frontier": 1, "diverged": 0, "skipped": 0, "error": 0},
  "verdicts": [{"run_id": "…", "workflow": "order", "verdict": "ok", "seq": null, "detail": null, "appended": 0}]
}
```

## Offline: `--corpus`

`ostro replay --corpus DIR [--module new.wasm]` replays a QA-02 replay corpus (05-12
format) locally, with no server. Each entry is replayed on a runtime built with **that
entry's own** await-token key, against the entry's `module.wasm`, or against `--module`
when it is given (an entry whose workflow the module does not export is `skipped`).

An entry is a directory `DIR/<entry>/` with exactly three files:

- `module.wasm`: the component the journal was recorded with;
- `journal.bin`: repeated `u32 LE length ‖ JournalEvent protobuf`, payloads inline;
- `entry.toml`: `name`, `workflow`, `expected`, `partition`, `run_id`, `token_key_hex`,
  `journal_events`, the sha256s and `[recorded_with]` (see `tests/replay-corpus/README.md`).

The loader verifies every sha256, the event count and the journal codec before
replaying. `tests/replay-corpus` is the repo's own corpus:
`ostro replay --corpus tests/replay-corpus` must report every entry `ok`. Server-side
journals can be exported into this format with ostro-server's `export_run` example, from
dev/test data dirs only.

## Exit codes

| Code | When |
|---|---|
| 0 | Every verdict is `ok` or `ok_frontier` (or `skipped` with `--allow-missing`). |
| 1 | Any `error` verdict, or the request failed (server unreachable, unknown module/run, 4xx/5xx). |
| 2 | Usage error (bad flags, no selector, unreadable `--module` file). |
| 4 | Any `diverged` verdict, or any `skipped` verdict without `--allow-missing`. A divergence wins over errors in other runs. |

## A CI recipe

```sh
set -euo pipefail
ostro build                                   # -> target/ostro/orders.wasm
ostro replay --server "$OSTRO_SERVER" \
  --module target/ostro/orders.wasm --sample 50   # exit 4 stops the pipeline
ostro deploy target/ostro/orders.wasm --activate
```

Point `--server` (or `OSTRO_SERVER`) at an environment whose runs represent production
traffic. Raise `--sample` for workflows with many code paths. Add explicit run ids for
runs that exercise rare branches.

## Making an intentional change replay-safe: `patched`

Changing the order or the identity of commands breaks every open run that already passed
that point. Put the new code behind a patch check:

```rust
let steps = if ctx.patched("reorder") {
    vec![step_b()?, step_a()?]   // new runs
} else {
    vec![step_a()?, step_b()?]   // runs recorded before the change
};
```

Python: `if ctx.patched("reorder"): ...`. TypeScript: `ctx.patched("reorder") ? ... : ...`.

The first `patched` call journals a marker in new runs and returns true. A journal recorded
before the change has no marker, so it returns false and takes the old path. Replay of old
runs then reports `ok`. Once no open run needs the old path, delete the branch and the
`patched` call: markers the code no longer queries are skipped on replay.

The repo's SC3 gate (`crates/ostro/tests/replay_cli.rs`) runs this flow in Rust, Python
and TypeScript:

- runs recorded on v1 (steps `a`, `b`) replay against v2 (steps swapped) with exit 4 at
  the seq of the first step;
- the same runs replay against v3 (the swap behind `patched("reorder")`) with exit 0.

## Limits

- Replay checks determinism, not behaviour. A step whose body changed but whose name and
  position did not still replays `ok`, because its recorded result is reused.
- Running runs are `ok_frontier`: only their prefix is checked, and what they do next is
  not.
- Sampling sees only the runs still stored (retention). It selects by the workflow name in
  the default tenant.
- One request replays at most 1000 runs per workflow. The CLI allows the server 300 s
  beyond `--timeout`.
