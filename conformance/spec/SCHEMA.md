# Conformance scenario schema

Language-neutral scenarios for the Phase 9 conformance suite. The Rust model is
`crates/ostro-conformance/src/spec.rs` (`Spec`, `ScriptStep`, `Expect`, `Catalogue`).
`cargo test -p ostro-conformance --test spec_catalogue` checks the catalogue and every
spec file against it.

## Layout

```
conformance/spec/
  SCHEMA.md            this file
  catalogue.json       every scenario id (written once by 09-02, never edited)
  engine/<ID>.json     engine leg (ostro-engine testkit; 09-11)
  server/<ID>.json     server leg (in-process ostro-server; 09-13, 09-16)
  crash/<ID>.json      crash leg (failpoint `ostro` binary, kill -9; 09-18)
```

An entry is **pending** exactly while `conformance/spec/<leg>/<ID>.json` does not exist.
Owner plans add spec files; nobody edits `catalogue.json`. A spec file whose id is not in
the catalogue fails the test. Spec files change after they land only for a demonstrated
spec bug.

## catalogue.json

```json
{"entries": [{"id": "C-STEP-01", "leg": "engine", "kind": "scenario",
              "title": "...", "owner": "09-11", "languages": ["rust", "py", "ts", "ts-sm"]}]}
```

| Field | Type | Meaning |
|-------|------|---------|
| `id` | string | `^C-[A-Z-]+-[0-9]{2}$`, unique |
| `leg` | `engine` \| `server` \| `crash` | harness that runs it; also the spec directory |
| `kind` | `scenario` \| `rebuild` | leg runners iterate `scenario` only (`spec::scenarios(leg)`); the single `rebuild` entry (C-REBUILD-01) is run by 09-17's `tests/rebuild.rs` (W6) |
| `title` | string | one-line intent |
| `owner` | `09-NN` | the plan that writes the spec file (Python/TS passes of engine and server scenarios are 09-14 / 09-15) |
| `languages` | list of `rust` \| `py` \| `ts` \| `ts-sm` | languages the scenario runs in; `ts-sm` = TypeScript on StarlingMonkey (smoke subset, D-P9-1) |

## Scenario file

```json
{
  "id": "C-SIG-01",
  "title": "two signals, one buffered before start, are received in order",
  "leg": "engine",
  "workflow": "conf_signals",
  "input": {"n": 2},
  "seed": "42",
  "run_id": "01890000-0000-7000-8000-000000000001",
  "variant": null,
  "script": [
    {"input": "signal", "name": "go", "payload": {"v": 1}, "before_start": true},
    {"input": "signal", "name": "go", "payload": {"v": 2}}
  ],
  "expect": {
    "status": "completed",
    "result": {"got": [{"v": 1}, {"v": 2}]},
    "projection": ["signal:go#1", "signal:go#2", "terminal:completed"]
  },
  "languages": null
}
```

Unknown top-level fields are rejected.

| Field | Type | Required | Meaning |
|-------|------|----------|---------|
| `id` | string | yes | must equal the file name and a catalogue id |
| `title` | string | yes | intent of the scenario |
| `leg` | `engine` \| `server` \| `crash` | yes | must equal the catalogue leg and the directory |
| `workflow` | string | yes | **base** workflow name (`conf_steps`); the harness maps it per language to `rs_<base>`, `py_<base>`, `ts_<base>` (D-P9-23) |
| `input` | JSON | no (null) | run input payload |
| `seed` | string (decimal u64) | no | run RNG seed; harness default when absent |
| `run_id` | string (UUID) | no | fixed run id; harness default when absent (needed by value probes such as C-VAL-01) |
| `variant` | string | no | module variant to start on (`v2`); default is the base module |
| `script` | list of steps | no (empty) | scripted inputs, see below |
| `expect` | object | yes | expected outcome, see below |
| `languages` | list | no | narrows the catalogue's languages for this file |

### Script steps

Each step is an object tagged by `input`. The engine leg delivers the next step whose
preconditions hold whenever an activation ends `WaitUnsatisfied`; with no deliverable step
it fires the earliest pending timer at its due time (D-P9-22). `StepBackoff` advances the
fake clock to `retry_at` and re-activates without consuming a step.

| `input` | Fields | Leg | Meaning |
|---------|--------|-----|---------|
| `signal` | `name`, `payload` (JSON, default null), `before_start` (bool, default false) | engine, server | deliver a signal; `before_start` buffers it before the first activation |
| `token` | `payload` (JSON) | engine | complete the first open await token |
| `cancel` | — | engine, server | request graceful cancellation of the run (D11) |
| `fire_timers` | — | engine | fire every pending timer at its due time |
| `swap_module` | `variant` | engine | re-activate on another module variant (patching, non-determinism) |
| `api` | `op` + operation fields | server | one control-plane call, e.g. `{"input":"api","op":"terminate","target":"child"}`; ops are defined by the server leg (09-13) |
| `kill` | `at` (failpoint or phase name) | crash | kill -9 the node at that point (e.g. `child::start_child_after_flush`) |

### expect

| Field | Type | Meaning |
|-------|------|---------|
| `status` | `completed` \| `failed` \| `cancelled` \| `terminated` \| `continued_as_new` \| `non_determinism` | terminal status of the (last) run |
| `result` | JSON | expected result, compared as canonical JSON |
| `failure_kind` | string | failure kind of a failed run or caught failure (`boom`, `child_terminated`, ...) |
| `projection` | list of strings | normalised journal projection, vocabulary below |
| *any other key* | JSON | scenario-specific checks kept in `Expect::extra`, e.g. `"differs_across_seeds": true` (C-RAND-*, C-NATIVE-01: a second seed must change the result while replay reproduces it) |

## Projection vocabulary

A projection is the ordered list of normalised journal facts of a run. Ordinals `#N` count
per kind in scheduling order starting at 1; op ids never appear.

| Token | Fact |
|-------|------|
| `step:<name>` | step `<name>` completed (recorded outcome compared via `result`) |
| `retry:<name>#<attempt>` | attempt `<attempt>` of step `<name>` failed and was scheduled for retry |
| `timer#N` | the N-th timer (sleep / sleep-until) was started and resolved |
| `signal:<name>#N` | the N-th signal named `<name>` was matched |
| `token#N` | the N-th await token was created and completed |
| `child:<workflow>#N` | the N-th child of base workflow `<workflow>` was started and completed |
| `cancel-op#N` | the N-th `cancel-op` was recorded |
| `patch:<id>` | a patch marker for change id `<id>` was recorded |
| `status:<canonical json>` | `set-status` with that canonical JSON |
| `effect:<kind>#N` | the N-th effect of kind `http` \| `sql` \| `object` |
| `terminal:<status>` | the terminal event (`completed`, `failed`, `cancelled`, ...) |

## Comparison rules (D-P9-21)

**Per language (strict):**
1. Replaying the produced journal on the same module appends 0 events.
2. Two runs with the same seed and run id give byte-identical journals.

**Across languages:**
1. Outcomes are compared as canonical JSON: object keys sorted, integral numbers compared
   exactly, non-integral numbers compared as f64 (`1.0 == 1`).
2. Projections are normalised before comparison:
   - dropped: `WaitResolved`, `ts_nanos`, command hashes (HTTP body / child-input byte
     formatting differs per language), `fuel_used`;
   - op ids renamed to scheduling ordinals `#1, #2, ...`;
   - kept: step names, effect kinds and canonical args, recorded outcomes as canonical JSON,
     patch markers, status JSON, the terminal;
   - inputs compared as a multiset keyed by ordinal;
   - Rust's re-issued `wait([op, CANCEL])` is normalised against the Python/TS remembered
     cancellation.
3. Value probe C-VAL-01: `random_u64` (decimal string), `random_f64`, `uuid_v7` and
   `idempotency_key("x")` must be equal across the three SDKs for the same seed and run id.
   C-NATIVE-01 values are per language: only replay identity and seed sensitivity count.

Guest logs (`log.emit`, stdout/stderr/console) are not durable and never journaled; no
projection or expectation may depend on them.
