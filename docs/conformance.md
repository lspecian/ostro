# SDK conformance suite

Phase 9 (SDK-05). One language-neutral set of scenarios proves that the Rust, Python and
TypeScript SDKs behave identically on the Ostro engine and server. Each language passes
every scenario on its own terms, and its outcome and normalised journal must also equal
the Rust reference's. At the Phase 9 gate the suite passes 100% in all three languages
(135/135 cells of the scenario × language table, see
`.planning/phases/09-polyglot-sdks-conformance/09-16-SUMMARY.md`).

## Architecture

| Piece | Where | What it does |
|-------|-------|--------------|
| Catalogue | `conformance/spec/catalogue.json` | Every scenario id with its leg, kind, owner and languages. Written once, never edited. |
| Spec files | `conformance/spec/{engine,server,crash}/<ID>.json` | One scenario each: base workflow, input, seed, run id, script, expectations. Schema: `conformance/spec/SCHEMA.md`. |
| Spec model | `crates/ostro-conformance/src/spec.rs` | `Spec`, `ScriptStep`, `Expect`, `Catalogue`, `Lang`. Unknown fields are rejected. |
| Guests | `guests/conf-rust{,-v2}`, `conformance/guests/{python,python-v2,ts,ts-v2}` | One module per language exporting every base workflow as `<lang>_<base>`. `-v2` modules carry the patched / reordered bodies for C-PATCH and C-NDET. |
| Language registry | `src/langs.rs` | `OSTRO_CONF_LANGS` selection, workflow naming, module builds. |
| Polyglot builds | `src/polyglot.rs` | Runs `sdks/scripts/build-guest.sh` and caches components under `target/polyglot-guests/<sha256(sources ‖ script ‖ mode ‖ componentize-py selection)>.wasm` (override with `OSTRO_POLYGLOT_CACHE`). |
| Canonical JSON | `src/canon.rs` | Sorted keys, non-integral numbers compared as f64, `1.0 == 1`. |
| Projection | `src/projection.rs` | Turns a journal into language-neutral facts, values and inputs (below). |
| Comparator | `src/compare.rs` | Readable diffs, byte-identical journal check, cross-language comparison. |
| Engine leg | `src/engine_leg.rs` | The harness is the scheduler on the ostro-engine testkit. |
| Server leg | `src/server_leg.rs` | `ServerHarness`: an in-process `ostro-server` driven through its REST API, with a loopback `MockProvider`. |
| Crash leg | `tests/crash_leg.rs` | A spawned failpoint `ostro dev` binary, kill -9 and restart. |
| Inspector | `src/inspect.rs`, bin `ostro-conf-inspect` | wasmparser view of a component: imports, exports, core instances and modules. |

### Legs (D-P9-22)

- **engine:** each spec runs on the ostro-engine testkit with a `FakeClock`. After every
  activation the scheduler acts on the outcome. Completed/Failed end the run.
  ContinuedAsNew starts the successor. NonDeterminism ends with `non_determinism`.
  `StepBackoff{retry_at}` advances the clock and re-activates. `WaitUnsatisfied` triggers
  these steps in order: (1) match buffered signals, (2) deliver the next script step whose
  precondition holds, (3) fire the earliest pending timer or wait timeout, (4) otherwise
  fail as stuck.
- **server:** a real `ostro-server` (ephemeral tmpfs data dir, 4 partitions, API on
  `127.0.0.1:0`) gets the modules deployed through the control plane. Signals, tokens,
  cancel, terminate and retry go through the REST API. HTTP effects hit the `MockProvider`
  under a module egress policy that allows only the mock origin. Roles are positional
  (`langs[0]` is the root, `langs[1]` the child, ...), so one spec runs single-language or
  cross-language (`run_spec(spec, &[Ts, Py, Rust])`). After every family the harness
  checks there is exactly one child run per (parent, op), at the deterministic id.
- **crash:** C-CRASH-01 runs the TS → Python → Rust chain under kill -9 and `child::`
  failpoints. C-CRASH-02 checks that a root cancel reaches every descendant across
  crashes. Both are `#[ignore]` (they spawn processes).

### Strict checks per language (D-P9-21)

Every scenario run checks:
- two executions with the same seed and run id give **byte-identical** encoded journals;
- replaying every journal of the run (and of each continue-as-new successor) appends
  **0 events**;
- `expect.status`, `expect.result` (canonical JSON), `expect.failure_kind` and
  `expect.projection`;
- scenario extras such as `differs_across_seeds` (a second seed must change the result
  while replay still reproduces it: C-RAND-*, C-NATIVE-01);
- on the server leg, at most one `ChildCompleted` per op in every journal.

### Projection vocabulary

- **Facts** (ordered, emitted at each command's journal position): `step:<name>`,
  `retry:<name>#<attempt>`, `timer#N`, `signal:<name>#N`, `token#N`, `child:<base>#N`
  (language prefix stripped), `cancel-op#N`, `effect:<http|sql|object>#N`, `patch:<id>`,
  `status:<canonical json>`, `terminal:<completed|failed|cancelled|continued_as_new|terminated>`.
- **Values** (ordered): `step:<name>=<json>` / `step:<name>!<failure>`,
  `child:<base>#N<-<input>`, `cancel-op#N-><target>`, `effect:<kind>#N=<language-neutral
  args>`, `terminal=<output>` / `terminal!<failure>` / `terminal->continue:<input>`.
- **Inputs** (multiset): `fired:timer#N`, `timeout:<label>`, `<signal>=<payload>`,
  `token#N=<json>`, `<child>=<outcome>`, `<effect>=<outcome>`, `cancel`.
- **Dropped:** `RunStarted`, `WaitResolved`, `SignalPayload`, timestamps, command hashes,
  fuel. Op ids become scheduling ordinals (`#1`, `#2`, ...). Dropping `WaitResolved`
  normalises Rust's re-issued `wait([op, CANCEL])` against the remembered cancellation of
  Python and TS (proven by `harness_selftest`).
- Guest logs are never durable or journaled, so no projection or expectation may depend on
  them.

### Cross-language comparison

After the strict checks, Python and TS results are compared with the Rust run of the same
spec, in the same process:
- engine leg: status, canonical outcome and projection (facts and values). Per-language
  probes such as C-NATIVE-01 compare facts only;
- server leg: status, failure kind and root facts (results that embed run ids differ per run).

**Value probe C-VAL-01** pins, for a fixed seed and run id, `random_u64` (decimal string),
`random_f64`, `uuid_v7` and `idempotency_key("x")`. These are byte-identical in Rust,
Python, TS on QuickJS and TS on StarlingMonkey for three seeds (`polyglot_cross::
values_cross_language`). The cross-SDK vectors in `conformance/vectors/` (idempotency key,
`random_f64`, JSON encodings) are checked by each SDK's own unit tests.

## Naming (D-P9-23)

Every conformance workflow is exported as `<lang>_<base>`: `rs_conf_steps`,
`py_conf_steps`, `ts_conf_steps`. StarlingMonkey builds reuse `ts_`. Specs name only the
base (`conf_steps`); `langs::workflow_name` maps it. The Rust, Python and TS modules
therefore deploy side by side on one server (`conf-rust`, `conf-py`, `conf-ts`). Cross-language
parents receive the full child workflow name in their input (`wf:<base>` / `wfN:<base>`
refs in a server spec resolve to role N's prefix). Projections strip the prefix from child
names.

## Running the legs

```bash
# Rust only (no Node/uv needed; part of `cargo test --workspace`)
cargo test -p ostro-conformance --locked --test engine_leg -- --test-threads=2
cargo test -p ostro-conformance --locked --test server_leg -- --test-threads=2
cargo test -p ostro-conformance --locked --test harness_selftest

# Every catalogue id has a spec file (phase gate)
cargo test -p ostro-conformance --locked --test spec_catalogue -- --ignored all_specs_present

# Python / TypeScript (feature `polyglot`; needs Node 24 + npm and uv 0.7.15)
cargo test -p ostro-conformance --features polyglot --locked --test polyglot_python -- --test-threads=2
cargo test -p ostro-conformance --features polyglot --locked --test polyglot_ts -- --test-threads=2
cargo test -p ostro-conformance --features polyglot --locked --test polyglot_cross -- --test-threads=2
OSTRO_CONF_LANGS=rust,py,ts,ts-sm cargo test -p ostro-conformance --features polyglot --locked --test engine_leg

# Everything polyglot, as ci-local runs it (incl. rebuild, smoke, SDK build checks)
cargo test -p ostro-conformance --features polyglot --locked -- --test-threads=2

# Crash leg (SC3; builds the failpoint binary into target/failpoints unless
# OSTRO_FAILPOINT_BIN points at one)
OSTRO_CRASH_CHAIN_ITERS=100 cargo test -p ostro-conformance --features polyglot --locked \
  --test crash_leg -- --ignored --test-threads=1 --nocapture

# Rebuild leg (C-REBUILD-01) and build reproducibility
cargo test -p ostro-conformance --features polyglot --locked --test rebuild -- --test-threads=1
bash sdks/scripts/repro-check.sh all
```

`polyglot_cross::full_table` prints the scenario × language table. `bash
scripts/ci-local.sh` runs the whole polyglot package and the SDK host tests in its
`polyglot` block. The block fails when Node 24 or uv is missing; `--no-polyglot` skips it
and prints SKIP. The `polyglot` job in `.github/workflows/ci.yml` does the same on GitHub.
The crash leg is not part of ci-local or CI.

## Adding a scenario

1. Add the base workflow to every guest, with the same name, input and output in each:
   `guests/conf-rust/src/` (shared bodies in `shared.rs`), `conformance/guests/python/app.py`
   and `conformance/guests/ts/index.ts`. Use explicit, language-neutral step names. The
   `-v2` guests only change what a patching or non-determinism scenario needs
   (`polyglot_python` checks that `python-v2` differs from `python` only in the marked
   section).
2. Phase 9 froze the catalogue after 09-02 ("never edited"), so a new id belongs to a
   later phase's plan. That plan appends one entry and never edits or reorders existing
   ones. Pick an id matching `^C-[A-Z-]+-[0-9]{2}$`, the leg, the owner and the languages.
3. Write `conformance/spec/<leg>/<ID>.json`: base `workflow`, `input`, optional
   `seed`/`run_id`, `script`, and `expect` (status, result, projection, extras). Read the
   actual values from a failing run's report only for probe scenarios, then review and
   pin them.
4. Run the leg in Rust first, then with `OSTRO_CONF_LANGS=rust,py,ts`. A spec changes after
   landing only for a demonstrated spec bug.

## Adding a language

1. Add a `Lang` variant (`spec.rs`) with its spec name, and the name, parse and prefix
   arms in `langs.rs`. Add a build mode in `sdks/scripts/build-guest.sh` and
   `polyglot.rs`, and gate the new language behind the `polyglot` feature (or a new
   feature) so the default workspace stays toolchain-free.
2. Implement the SDK against the frozen WIT. Mirror the determinism surface (host-routed
   randomness, logical time, logs to `log.emit`), step exclusivity (PF-W1), op ids as u64,
   lone-surrogate rejection, JSON payload rules, and `random_f64 = (u64 >> 11) * 2^-53`.
   Pass `conformance/vectors/*.json` in the SDK's unit tests.
3. Write the conformance guest exporting every base workflow as `<prefix>_<base>`, plus a
   `-v2` variant for C-PATCH/C-NDET.
4. Add a `polyglot_<lang>.rs` test running the engine leg (`engine_leg::cross_compare`
   against Rust) and the server families, plus a coverage test that every server scenario
   ran. List the language in new catalogue entries. Old entries keep their language list,
   so record the language's coverage in its own test.
5. Add the language to `polyglot_cross::full_table` and `values_cross_language`, and to the
   ci-local polyglot block and the ci.yml polyglot job.
