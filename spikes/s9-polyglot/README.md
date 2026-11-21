# Spike S9: polyglot toolchains (plan 09-04)

Throwaway scratch work behind `docs/spikes/S9-polyglot.md`. Nothing here is product code,
and nothing here is part of the root workspace (`spikes` is in the root `exclude`).

| Path | What |
|------|------|
| `harness/` | Standalone Rust binary (wasmtime =50.0.2, wasmparser =0.261.0, path deps on `ostro-engine` with `testkit` and on `ostro-types`). Subcommands `inspect`, `pool`, `instantiate`, `aot`, `fuel`, `engine`, `note`; each appends JSON lines to `docs/spikes/data/S9/measurements.jsonl`. Its `Cargo.lock` is never committed; `run.sh` seeds it from the root lock. |
| `py/app_asyncio.py` | Raw-binding Python guest for the real `ostro:runtime/workflow@0.1.0` world. The `SDK-IMPORTS` block (asyncio, contextvars, dataclasses, datetime, json, random, uuid) is stripped by `run.sh` for the `py-base` baseline. |
| `py/loop_proto.py`, `py/test_loop_proto.py` | OstroLoop prototype (asyncio loop over one `wait` per quiescent point, durable timers) and its pytest suite (host CPython 3.12). `run.sh` also builds `py-loop` (app + loop_proto) and runs `selfcheck()` inside the embedded CPython 3.14.0. |
| `ts/probe.ts` | SdkPromise + SDK-owned job loop prototype for the real world, plus the result-mapping, surrogate and environment probes. Built for QuickJS and StarlingMonkey. |
| `ts/lowering_fixture.ts` | Async-construct coverage fixture (question 9); `run.sh` stages it with `probe.ts` and a generated `dep.ts` (with and without top-level await). |
| `ts/package.json` | jco 1.42.0 + componentize-js 0.23.0 pins (componentize-qjs 0.5.0 comes with jco). |
| `run.sh` | `build`, `measure`, `probes`, `repro`, `summary`, `all`. |

Run (needs uv, Node 24, cargo; network for the first uv/npm fetch):

```bash
S9_FRESH=1 spikes/s9-polyglot/run.sh all     # truncates measurements.jsonl first
cd spikes/s9-polyglot/py && uv run --no-project --python 3.12 --with pytest==8.4.2 python -m pytest -q test_loop_proto.py
```

Outputs under `out/` are gitignored (`spikes/**/out/`, `*.wasm`, `*.cwasm`).
