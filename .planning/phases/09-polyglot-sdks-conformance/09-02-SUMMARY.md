---
phase: 09-polyglot-sdks-conformance
plan: 02
subsystem: conformance-harness
tags: [conformance, polyglot, wasmparser, componentize-py, jco, quickjs, starlingmonkey, catalogue]
requires:
  - phase: 09-polyglot-sdks-conformance
    provides: "09-01 gate (FAILPOINT_CRATES already holds ostro-server; toolchains at pins)"
  - phase: 09-polyglot-sdks-conformance
    provides: "09-03 sdks/ts/rolldown.ostro.mjs (copied byte for byte)"
provides:
  - "wasmparser =0.261.0 workspace pin (single copy in Cargo.lock), in ostro-engine (anchor) and ostro-conformance"
  - "crates/ostro-conformance: spec model + catalogue API, component inspector, polyglot build cache, ostro-conf-inspect bin"
  - "conformance/spec/catalogue.json (46 ids) + SCHEMA.md"
  - "byte-identical SDK WIT copies sdks/python/ostro/wit/runtime.wit, sdks/ts/wit/runtime.wit"
  - "sdks/scripts/build-guest.sh (py|ts|ts-sm) + pinned npm toolchain package"
  - "raw fixtures conformance/fixtures/{py-min,ts-min} + polyglot smoke test"
affects: [09-04, 09-07, 09-09, 09-10, 09-11, 09-12, 09-13, 09-14, 09-15, 09-16, 09-17, 09-18, 09-19, 09-20]
tech-stack:
  added: ["wasmparser 0.261.0 (direct dep; already in the lock via wasmtime)", "npm: @bytecodealliance/jco 1.42.0, @bytecodealliance/componentize-js 0.23.0 (build-time only)"]
  patterns: ["dep_anchors module for deps declared ahead of use", "source-hash cache under target/polyglot-guests with atomic rename", "catalogue entry pending iff its spec file is absent"]
key-files:
  created:
    - crates/ostro-conformance/Cargo.toml
    - crates/ostro-conformance/src/lib.rs
    - crates/ostro-conformance/src/spec.rs
    - crates/ostro-conformance/src/inspect.rs
    - crates/ostro-conformance/src/polyglot.rs
    - crates/ostro-conformance/src/bin/ostro-conf-inspect.rs
    - crates/ostro-conformance/tests/wit_copies.rs
    - crates/ostro-conformance/tests/spec_catalogue.rs
    - crates/ostro-conformance/tests/polyglot_smoke.rs
    - conformance/spec/SCHEMA.md
    - conformance/spec/catalogue.json
    - conformance/fixtures/py-min/app.py
    - conformance/fixtures/ts-min/index.ts
    - sdks/scripts/build-guest.sh
    - sdks/scripts/toolchain/package.json
    - sdks/scripts/toolchain/package-lock.json
    - sdks/scripts/toolchain/rolldown.ostro.mjs
    - sdks/python/ostro/wit/runtime.wit
    - sdks/ts/wit/runtime.wit
  modified:
    - Cargo.toml
    - Cargo.lock
    - crates/ostro-engine/Cargo.toml
    - crates/ostro-engine/src/validate.rs
key-decisions:
  - "ScriptStep is tagged by \"input\" (the 09-RESEARCH Pattern 8 example); Api steps carry `op` plus flattened fields; Spec rejects unknown top-level fields"
  - "spec::Lang (rust|py|ts|ts-sm) lives in spec.rs; 09-11's langs.rs should re-export it rather than define a second enum"
  - "ostro-conformance also declares tempfile (crash/server leg data dirs) and ostro-testkit with feature mock-http (MockProvider is feature-gated)"
  - "FAILPOINT_CRATES unchanged (ostro-server already listed, per 09-01); ci-local.sh and ci.yml untouched"
requirements-completed: []
duration: ~75 min
completed: 2026-11-21
---

# Phase 9 Plan 02: Contract + Wave 0 (Rust/build) Summary

**Phase 9's one Cargo edit (wasmparser =0.261.0, no new lock entry), the `ostro-conformance` crate (spec model and catalogue API, wasmparser component inspector, hash-cached polyglot build helper), a 46-entry scenario catalogue with its schema, byte-identical SDK WIT copies, and a pinned `build-guest.sh` whose raw Python, QuickJS and StarlingMonkey fixtures import only `ostro:runtime/*`.**

## Commits
- `cf3c610` feat(09-02): conformance crate skeleton, catalogue and Phase 9 deps (15 files)
- `4ef7074` feat(09-02): pinned polyglot guest build, fixtures and smoke test (9 files)

## Dependencies
- Root `[workspace.dependencies]`: `wasmparser = { version = "=0.261.0", default-features = false, features = ["std", "component-model"] }`.
- `Cargo.lock` gained only the `ostro-conformance` package and a `wasmparser` edge from `ostro-engine`. There is still exactly one `wasmparser` entry (0.261.0). `cargo deny check` reports advisories, bans, licenses and sources all ok.
- `ostro-engine` gets `wasmparser.workspace = true`, anchored by `use wasmparser as _;` in `validate.rs`. 09-07 replaces the anchor.
- `ostro-conformance` dependencies:
  - serde, serde_json, sha2, wasmparser, thiserror, tokio, tempfile, reqwest, ostro-types;
  - ostro-engine (`testkit`), ostro-server, ostro-api, ostro-meta;
  - ostro-testkit (`mock-http`).
  
  The deps that only later plans use are anchored in `lib.rs::dep_anchors`. No later Phase 9 plan needs to touch a Cargo file.

## Catalogue
46 entries:

| Owner | Leg / kind | Count | Notes |
|-------|------------|-------|-------|
| 09-11 | engine / scenario | 27 | includes C-STEP-04 and C-NATIVE-01; `ts-sm` is listed only for the D-P9-1 smoke subset C-STEP-01, C-RACE-01, C-SIG-01, C-RAND-01, C-CANCEL-01 |
| 09-17 | engine / rebuild | 1 | C-REBUILD-01 |
| 09-13 | server / scenario | 14 | C-CHILD-01..07, 09, 10, 11, C-SIG-API-01, C-TOKEN-API-01, C-CAN-02, C-FX-01 |
| 09-16 | server / scenario | 2 | C-CHILD-08, C-SAGA-03 |
| 09-18 | crash / scenario | 2 | C-CRASH-01, C-CRASH-02 |

All 46 are pending until the owner plans add their spec files. `spec::scenarios(leg)` yields `kind == scenario` entries only. `all_specs_present` is `#[ignore]` and is meant for the 09-20 gate.

## Build commands (exact)
- **py:** `uv tool run --from componentize-py==0.25.1 componentize-py -d wit/ostro-runtime-0.1.0 -w ostro:runtime/workflow@0.1.0 componentize --stub-wasi -p "$STAGE/src" app -o "$OUT"`. The command runs inside the stage. `sdks/python/ostro` is staged next to `app.py` when it exists.
- **ts (QuickJS):** `sdks/scripts/toolchain/node_modules/.bin/jco componentize "$STAGE/src/index.ts" --wit wit/ostro-runtime-0.1.0 --world-name workflow --backend quickjs --backend-qjs-disable-async --backend-qjs-stub-wasi --bundle-config sdks/scripts/toolchain/rolldown.ostro.mjs -o "$OUT"`.
- **ts-sm:** the same command with `--disable all` in place of the three quickjs flags.
- The jco 1.42.0 flag spellings match the plan; I checked them with `jco componentize --help`.
- Staging:
  - `sdks/ts` (without node_modules) is staged as `node_modules/ostro` when `sdks/ts/package.json` exists.
  - Every staged mtime is set to `SOURCE_DATE_EPOCH`.
  - The build exports `PYTHONHASHSEED=0 PYTHONDONTWRITEBYTECODE=1 TZ=UTC LC_ALL=C SOURCE_DATE_EPOCH`.
- The script exits 2 with a message if uv is missing or not exactly 0.7.15, or if node/npm is missing or Node is not v24.
- `npm ci` runs only when `node_modules/.package-lock.json` is missing or older than the lock.
- Rolldown prints an `UNRESOLVED_IMPORT` notice for `ostro:runtime/*` and then treats them as external. This is expected: jco resolves them as WIT imports.
- `sdks/scripts/toolchain/rolldown.ostro.mjs` is byte-identical to 09-03's `sdks/ts/rolldown.ostro.mjs` (checked with `cmp`).

## componentize-py export naming
For the export interface `workflows` of world `ostro:runtime/workflow@0.1.0`, componentize-py 0.25.1 generates:
- the protocol `wit_world.exports.Workflows`, with `describe(self) -> List[WorkflowMeta]` and `run(self, name, input: types.Payload) -> RunResult`;
- errors raised as `componentize_py_types.Err(types.Failure_*)`.

The entry module (`app`) must define **`class Workflows(wit_world.exports.Workflows)`**. No `WitWorld` class is needed. Other generated names:
- `journal.StepStart_Execute` / `StepStart_Replayed`;
- `workflows.RunResult_Completed`;
- `types.StepOptions(retry, timeout_ms, fuel)`;
- `Ok` / `Err` from `componentize_py_types`.

## Fixture measurements (smoke run, worktree at 4ef7074)

| Fixture [mode] | bytes | sha256 | core_instances | core_modules | imports | exports |
|----------------|-------|--------|----------------|--------------|---------|---------|
| py-min [py] | 18,455,289 | 4af68f895f3e285284e68335e2206c4754da1bb8dea62a1a7f7744f5fb496232 | 111 | 38 | ostro:runtime/{types,context,journal,effects,log}@0.1.0 | `exports`, `ostro:runtime/workflows@0.1.0` |
| ts-min [ts, QuickJS] | 1,494,761 | 1ba380e4cf376a4616d8a5889cd88c329fe5b012fe109aa1e4ef0969c7de940f | 53 | 11 | same 5, zero `wasi:` (SDK-03) | `ostro:runtime/workflows@0.1.0` |
| ts-min [ts-sm] | 12,272,436 | 43e06b49cc6bdb6d17432011f6e9416ea84b6e9ba70fbbdb6c6d1a95a02de46f | 8 | 3 | same 5, zero `wasi:` | `ostro:runtime/workflows@0.1.0` |
| guests/smoke [Rust, reference] | — | — | 18 | 3 | ostro:runtime/types + wasi shim | `ostro:runtime/workflows@0.1.0` |

The counts are `CoreInstanceSection` entries summed over the outer and all nested components. 09-04 and 09-07 consume them.

**Early reproducibility signal for 09-04/09-17:**
- Building the same sources again, from a fresh stage with the same SOURCE_DATE_EPOCH, changed the bytes in every mode:
  - py: 18,453,126 → 18,455,289;
  - ts: 1,494,731 → 1,494,729 → 1,494,761;
  - ts-sm: 12,272,452 → 12,272,436.
- Under the step-1 wrapper alone, the released toolchains are **not** bit-reproducible. This was expected (D-P9-3 ladder), and I did not investigate further in this plan.

## Test results
- `cargo build --workspace --locked`: warning-free.
- `cargo test -p ostro-conformance --locked` (toolchain-free): all pass:
  - inspector lib tests 3;
  - spec lib tests 3;
  - spec_catalogue 4 passed, plus 1 ignored (`all_specs_present`);
  - wit_copies 4. After the rebase the Rust SDK copy from 09-06 exists and is byte-identical, so the "absent" note no longer prints.
- `cargo test -p ostro-conformance --features polyglot --locked --test polyglot_smoke -- --test-threads=1`: 4 passed (Py, QuickJS, StarlingMonkey shapes; cache hit).
- `cargo clippy -p ostro-conformance [--features polyglot] -p ostro-engine --all-targets --locked -- -D warnings`: clean. `cargo fmt --check`: clean.
- `cargo deny check`: ok.
- The WIT copies are identical to the source (`cmp`), and the FROZEN.toml sha256 matches.

## FAILPOINT_CRATES
Unchanged. 09-01 confirmed `ostro-server:failpoints_phase5` is already listed. `scripts/ci-local.sh` and `.github/workflows/ci.yml` were not touched.

## Deviations from Plan
1. **[Rule 1 - plan defect] Duplicate check.** The verify line `! grep -q "^wasmparser " <<<"$(cargo tree -d --depth 0 -e normal --workspace --locked --prefix none)"` fails on unmodified main too. `cargo tree -d` lists `wasmparser v0.261.0` twice (the same goes for `wasm-encoder` and `wit-component` 0.261.0), because of the same-version feature/host split that ostro-witcheck and wasmtime already had. I checked this in a throwaway worktree at main. The real invariants hold instead: exactly one `name = "wasmparser"` entry in Cargo.lock, and the duplicate list is identical to main's (no new duplicate).
2. **[Rule 2] Extra dependencies.** `tempfile` was added to ostro-conformance (data dirs for later legs), and `ostro-testkit` is declared with `features = ["mock-http"]`, which `MockProvider` needs. Both are existing workspace crates, so the lock gained no new packages.
3. **ostro-engine tests not re-run in full.** The only engine change is the `use wasmparser as _;` anchor and its manifest line. Build and `clippy --all-targets -D warnings` on ostro-engine pass.
4. **[Brief]** The ROADMAP tick and the STATE Current Position line were updated in the docs commit, built from the current main blobs.

## Notes for later plans
- 09-11: use `spec::Lang` (re-export it from `langs.rs`). `ScriptStep` variants are `Signal{name,payload,before_start}`, `Token{payload}`, `Cancel{}`, `FireTimers{}`, `SwapModule{variant}`, `Api{op,args}`, `Kill{at}`. `Expect.extra` holds keys such as `differs_across_seeds`.
- 09-04/09-07: `ostro_conformance::inspect` and `ostro-conf-inspect <wasm>` print the counts. `polyglot::build(Mode, dir)` returns a cached path, and `polyglot::script_runs()` reports cache misses.
- Guest logs are not durable. SCHEMA.md forbids projections or expectations that depend on them.

## Self-Check: PASSED
- FOUND: every key-files path above, at main.
- FOUND: cf3c610, 4ef7074 in `git log main`.
