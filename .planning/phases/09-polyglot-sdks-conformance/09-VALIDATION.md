---
phase: 9
slug: polyglot-sdks-conformance
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-11-19
---

# Phase 9: Validation Strategy

> This is the per-phase validation contract for feedback sampling during execution.
> Source: 09-RESEARCH.md § Validation Architecture, mapped onto plans 09-01..09-20 (revised after the plan check: 3 blockers, 9 warnings; WF-07 remapped to Phase 9). Adopted decisions: 09-DECISIONS.md (D-P9-1..D-P9-30).

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Rust built-in test harness (`cargo test`), proptest (`rebuild_digest`), `fail` 0.5.1 failpoints (`child::` + Phase 5 prefixes), pytest 8.4.2 via `uv run --no-project --python 3.12 --with pytest==8.4.2` (Python SDK host tests, fake host), `node --test` on Node 24.13.0 (TS SDK host tests, native type stripping), the `ostro-conformance` harness (engine leg on ostro-engine testkit; server leg on an in-process server; crash leg on the spawned failpoint `ostro` binary), `ostro-conf-inspect` (wasmparser component inspection). |
| **Toolchains** | componentize-py 0.25.1 (`uv tool run --from componentize-py==0.25.1`, `--stub-wasi`, embedded CPython 3.14); jco 1.42.0 (QuickJS via bundled componentize-qjs 0.5.0, `--backend-qjs-disable-async --backend-qjs-stub-wasi`; StarlingMonkey via componentize-js 0.23.0, `--disable all`); Rolldown `transform.target: "es2016"` via `sdks/ts/rolldown.ostro.mjs`; Rust 1.100.0 + wit-bindgen 0.63 for Rust guests. All builds through `sdks/scripts/build-guest.sh` (D-P9-3 wrapper env), cached in `target/polyglot-guests/`. |
| **Config** | `scripts/ci-local.sh` gains the polyglot block in 09-17 (toolchain check fails loudly; skippable only with `--no-polyglot`, shown as SKIP). Cargo feature `polyglot` on `ostro-conformance` gates every Python/TS test (D-P9-12). `FAILPOINT_CRATES` must equal the ci.yml "Failpoint suites" step (ostro-server added by 09-02 if absent). ci.yml gains a SHA-pinned `polyglot` job (setup-node, setup-python, setup-uv). |
| **External services** | None required locally. Phase 6 `MockProvider` (in-process) for HTTP effects and saga compensations. Publishing to PyPI/npm is human_needed. **The microk8s cluster reachable via kubectl on this machine is never used.** |
| **Quick run command** | `cd sdks/python && uv run --no-project --python 3.12 --with pytest==8.4.2 python -m pytest tests -q` · `node --test sdks/ts/test/` · `cargo test -p ostro-conformance --locked --test engine_leg -- --test-threads=2` · `cargo test -p <touched crate> --locked` |
| **Full suite command** | `bash scripts/ci-local.sh --no-musl` (includes the polyglot block from 09-17 onwards) |
| **Phase gate commands** | 1. `bash scripts/ci-local.sh` (with musl).<br>2. `cargo test -p ostro-conformance --features polyglot --locked -- --test-threads=2` (polyglot_python, polyglot_ts, polyglot_cross, rebuild, smoke and engine tests).<br>3. `OSTRO_CRASH_CHAIN_ITERS=100 cargo test -p ostro-conformance --features polyglot --locked --test crash_leg -- --ignored --test-threads=1 --nocapture` (after `cargo build -p ostro --features failpoints --target-dir target/failpoints --locked`).<br>4. `bash sdks/scripts/repro-check.sh all`.<br>5. `cargo test --release -p ostro-conformance --features polyglot --locked --test resources -- --ignored --test-threads=1 --nocapture` (SC5; enforce with `OSTRO_SC5_ENFORCE=1` on the native perf machine, D15).<br>6. `cargo test -p ostro-engine --test replay_corpus --locked` and `cargo test -p ostro-witcheck --locked` (WIT unchanged).<br>7. `cargo test -p ostro-conformance --locked --test spec_catalogue -- --ignored all_specs_present`. |
| **Estimated runtime** | Quick: < 60 s per command (first polyglot build of a guest ≈ 1–3 min, then cached). Full ci-local: ~15–25 min debug incl. polyglot. Crash leg at 100 iterations: 30–90 min. Repro check: ~10 min. |

---

## Sampling Rate

- **After every task commit:** the task's `<automated>` command, plus `cargo clippy -p <crate> --all-targets --locked -- -D warnings` for touched Rust crates (with `--features polyglot` for ostro-conformance, `--features failpoints` for ostro-server failpoint tests).
  - SDK tasks also run the full host suite of the touched SDK (pytest or node --test).
  - Children tasks (09-05, 09-08, 09-13) also run `cargo test -p ostro-partition --locked --test apply_retention` (09-08 edits retention) and `cargo test -p ostro-partition --locked` and `PROPTEST_CASES=128 cargo test -p ostro-partition --locked --test rebuild_digest`.
- **After every plan wave / plan end:** `bash scripts/ci-local.sh --no-musl` (recorded as plan-end `<verification>`, not in task verifies) + `cargo test -p ostro-engine --test replay_corpus --locked`; from wave 4 on also `cargo test -p ostro-conformance --features polyglot --locked -- --test-threads=2`.
- **Before `/gsd:verify-work`:** the 09-20 gate.
- **Max feedback latency:** 60 s for task-level checks (cached polyglot builds); crash leg, repro check and measurements are gate level.
- Piped verify commands use `set -o pipefail`; cargo test filters go after `--`; no `|| true`; filtered runs must report ≥ 1 passed test (`test result: ok\. [1-9][0-9]* passed`); pytest must report `N passed` with no failed/error; node must report `# pass N≥1` and `# fail 0`.

---

## Wave Structure

| Wave | Plans | Exclusive shared files |
|------|-------|------------------------|
| 1 | 09-01 gate (Phase 6 complete, Phase 5 freeze, PF-F1/F2 in code) + reservations + decisions | RESERVATIONS.md, STATE.md |
| 2 | 09-02 contract/build; 09-03 SDK skeletons; 09-04 S9 spike; 09-05 children 1/2; 09-06 Rust SDK additions | 09-02: **Cargo files (vs 07-02, 08-01/03/12)**, engine validate.rs (anchor), ostro-conformance crate, SDK WIT copies, `sdks/scripts/` (build-guest.sh + toolchain), fixtures, ci files (FAILPOINT_CRATES only). 09-03: sdks/python, sdks/ts skeletons. 09-04: spikes/s9-polyglot, docs/spikes/S9*. 09-05: **apply/inbound.rs, server derive.rs, sink.rs (vs 07-04, 08-17), lib.rs (one line)**, children.rs. 09-06: **sdks/rust/** (sole owner)**, guests child-*/sdk-saga, conformance/vectors. |
| 3 | 09-07 engine ENG-1/ENG-2; 09-08 children 2/2; 09-09 Python core; 09-10 TS core; 09-11 harness + Rust engine leg | 09-07: engine validate/config/core_budget, **runtime.rs (vs 08-16; activation.rs never), engine lib.rs (vs 08-01), server config.rs (vs 07-02, 08-04)**. 09-08: **apply/terminal.rs (vs 07-04, 08-07, 08-16), apply/run.rs (vs 07-04), apply/retention.rs, apply/inbound.rs, ostro-types ids.rs, derive.rs, sink.rs, children.rs**. 09-09: sdks/python/ostro/*. 09-10: sdks/ts/src/*. 09-11: ostro-conformance src, conf-rust guests, spec/engine/. |
| 4 | 09-12 Python/TS effects + saga + first engine runs; 09-13 server leg + Rust pass (children e2e) | 09-12: SDK sources (both), smoke fixtures. 09-13: ostro-conformance server_leg, conf-rust (append), spec/server/, children code bug fixes only. |
| 5 | 09-14 Python conformance; 09-15 TS conformance (separate test files) | 09-14: conformance/guests/python*, polyglot_python.rs, Python SDK fixes. 09-15: conformance/guests/ts*, polyglot_ts.rs, TS SDK fixes. |
| 6 | 09-16 cross-language (C-CHILD-08, C-SAGA-03, C-VAL-01, full table) | polyglot_cross.rs, two server specs, SDK fixes (both) |
| 7 | 09-17 repro + packaging + CI (before crash/measurements, W8) | **build-guest.sh + toolchain lock, ci-local.sh, ci.yml (vs 07-22, 08-20/22)**, pyproject.toml, package.json, docs/sdk-builds.md, rebuild.rs |
| 8 | 09-18 crash leg | crash_leg.rs, spec/crash/ |
| 9 | 09-19 measurements | resources.rs, docs/sdk-resources.md |
| 10 | 09-20 phase gate | READMEs, docs/conformance.md, docs/children.md, RESERVATIONS flip, REQUIREMENTS ticks, VALIDATION, DECISIONS, STATE |

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirements | Task | Automated command(s) | Status |
|---------|------|------|--------------|------|----------------------|--------|
| 9-01-01 | 01 | 1 | SDK-02, SDK-03, SDK-04, SDK-05, WF-06, WF-07, WF-10 | Phase 6 + Phase 5 freeze gate, PF-F1/F2 check, baseline, toolchains, reservations check + RESERVATIONS.md Phase 9 rows | `grep / doc checks` | ⬜ pending |
| 9-01-02 | 01 | 1 | SDK-02, SDK-03, SDK-04, SDK-05, WF-06, WF-07, WF-10 | Decisions, review/human todos and Phase 9 ↔ Phase 7/8 coordination in STATE.md | `grep / doc checks` | ⬜ pending |
| 9-02-01 | 02 | 2 | SDK-02, SDK-03, SDK-04, SDK-05, WF-06, WF-10 | Dependencies, ostro-conformance crate (spec model, inspector, catalogue) and WIT-copy test | `cargo test -p ostro-conformance` | ⬜ pending |
| 9-02-02 | 02 | 2 | SDK-02, SDK-03, SDK-04, SDK-05, WF-06, WF-10 | Pinned polyglot build script, raw-binding fixtures, polyglot build helper and smoke test | `cargo test -p ostro-conformance --features polyglot --test polyglot_smoke` | ⬜ pending |
| 9-04-01 | 04 | 2 | SDK-02, SDK-03, SDK-04 | Build matrix and resource measurements (QJS vs SM vs Python-with-asyncio) | `grep / doc checks` | ⬜ pending |
| 9-04-02 | 04 | 2 | SDK-02, SDK-03, SDK-04 | Behaviour probes — lowering coverage, result mapping, surrogates, export naming, loop prototypes | `pytest` | ⬜ pending |
| 9-04-03 | 04 | 2 | SDK-02, SDK-03, SDK-04 | Reproducibility under the D-P9-3 wrapper + S9 report | `grep / doc checks` | ⬜ pending |
| 9-05-01 | 05 | 2 | WF-06 | Parent-side apply — journal ChildCompleted.event verbatim, hard-stop mapping | `cargo test -p ostro-partition --test children_apply`; `cargo test -p ostro-partition`; `cargo test -p ostro-partition --test rebuild_digest` | ⬜ pending |
| 9-05-02 | 05 | 2 | WF-06 | children.rs + derive StartChild + child parent_event + sink without the children unsupported path | `cargo test -p ostro-server --test children_derive`; `cargo test -p ostro-server` | ⬜ pending |
| 9-06-01 | 06 | 2 | WF-06, WF-10, SDK-04 | Cross-SDK vectors, random_f64 and a pure idempotency-key function | `cargo test -p ostro-sdk --test vectors` | ⬜ pending |
| 9-06-02 | 06 | 2 | WF-06, WF-10, SDK-04 | start_child + ChildOptions + child guests + engine tests | `cargo test -p ostro-engine --test sdk_children_saga` | ⬜ pending |
| 9-06-03 | 06 | 2 | WF-06, WF-10, SDK-04 | Saga helper + sdk-saga guest + engine tests | `cargo test -p ostro-engine --test sdk_children_saga`; `cargo test -p (sdk manifest)` | ⬜ pending |
| 9-07-01 | 07 | 3 | SDK-02, SDK-03 | ENG-1 — exact-shape allowance for componentize-py's `exports` export | `cargo test -p ostro-engine --test exports_shape`; `cargo test -p ostro-engine --test validate`; `cargo test -p ostro-conformance --features polyglot --test polyglot_engine` | ⬜ pending |
| 9-07-02 | 07 | 3 | SDK-02, SDK-03 | ENG-2 — core-instance count, weighted admission budget, class/pool config | `cargo test -p ostro-engine --test core_budget`; `cargo test -p ostro-engine`; `cargo test -p ostro-server --test engine_class`; `cargo test -p ostro-server --test config`; `cargo test -p ostro-conformance --features polyglot --test polyglot_engine` | ⬜ pending |
| 9-08-01 | 08 | 3 | WF-06 | Apply — parent close, detached rows, duplicate-start re-completion, row-guarded child completion | `cargo test -p ostro-partition --test children_close,children_chain`; `cargo test -p ostro-types`; `cargo test -p ostro-partition`; `cargo test -p ostro-partition --test rebuild_digest` | ⬜ pending |
| 9-08-02 | 08 | 3 | WF-06 | Chain bookkeeping — chain_done and deferred EXPIRE of continue-as-new predecessors | `cargo test -p ostro-partition --test children_chain`; `cargo test -p ostro-partition --test apply_retention`; `cargo test -p ostro-partition --test rebuild_digest'` | ⬜ pending |
| 9-08-03 | 08 | 3 | WF-06 | Server — cancel-op → CancelRun, detached completion, repair rules, resume re-send, failpoints | `cargo test -p ostro-server --test children_detached,children_resume`; `cargo test -p ostro-server --features failpoints --test children_failpoints`; `cargo test -p ostro-server` | ⬜ pending |
| 9-09-01 | 09 | 3 | SDK-02, SDK-04, WF-07 | OstroLoop, ops, errors and JSON | `pytest` | ⬜ pending |
| 9-09-02 | 09 | 3 | SDK-02, SDK-04, WF-07 | Ctx, steps with serialisation, cancellation/shielding, determinism surface, export | `pytest` | ⬜ pending |
| 9-09-03 | 09 | 3 | SDK-02, SDK-04, WF-07 | Example guest build + import/export check | `cargo test -p ostro-conformance --features polyglot --test py_sdk_build` | ⬜ pending |
| 9-10-01 | 10 | 3 | SDK-03, SDK-04, WF-07 | SdkPromise, job queue, drive loop, ops, errors, encodings | `node --test` | ⬜ pending |
| 9-10-02 | 10 | 3 | SDK-03, SDK-04, WF-07 | ctx, steps, shielding, determinism, export and bindings | `node --test` | ⬜ pending |
| 9-10-03 | 10 | 3 | SDK-03, SDK-04, WF-07 | Example guest build on QuickJS and StarlingMonkey + import/export and lowering checks | `cargo test -p ostro-conformance --features polyglot --test ts_sdk_build` | ⬜ pending |
| 9-11-01 | 11 | 3 | SDK-05, SDK-04, WF-10, WF-07 | Canonical JSON, projection, comparator, language registry + self-test | `cargo test -p ostro-conformance --test harness_selftest` | ⬜ pending |
| 9-11-02 | 11 | 3 | SDK-05, SDK-04, WF-10, WF-07 | Rust conformance guests (conf-rust, conf-rust-v2) | `grep / doc checks` | ⬜ pending |
| 9-11-03 | 11 | 3 | SDK-05, SDK-04, WF-10, WF-07 | Engine-leg runner + spec files + 100% Rust pass | `cargo test -p ostro-conformance --test engine_leg`; `cargo test -p ostro-conformance --test spec_catalogue` | ⬜ pending |
| 9-12-01 | 12 | 4 | WF-10, SDK-02, SDK-03, SDK-04 | Python effects builders, idempotency key, saga + vector tests | `pytest` | ⬜ pending |
| 9-12-02 | 12 | 4 | WF-10, SDK-02, SDK-03, SDK-04 | TS effects builders, idempotencyKey, saga + vector tests | `node --test` | ⬜ pending |
| 9-12-03 | 12 | 4 | WF-10, SDK-02, SDK-03, SDK-04 | First real-engine runs of SDK-built Python/TS guests + deviation fixes | `cargo test -p ostro-conformance --features polyglot --test sdk_engine_smoke`; `pytest`; `node --test` | ⬜ pending |
| 9-13-01 | 13 | 4 | WF-06, SDK-05 | Server harness + Rust server workflows | `grep / doc checks` | ⬜ pending |
| 9-13-02 | 13 | 4 | WF-06, SDK-05 | Server-leg specs + 100% Rust pass (children e2e, API signals/tokens, CAN chain, HTTP effect) | `cargo test -p ostro-conformance --test server_leg`; `cargo test -p ostro-conformance --test spec_catalogue'` | ⬜ pending |
| 9-14-01 | 14 | 5 | SDK-05, SDK-04, WF-06, WF-07, WF-10, SDK-02 | Python conformance guests + engine leg (100%, cross-compared with Rust) | `cargo test -p ostro-conformance --features polyglot --test polyglot_python`; `pytest` | ⬜ pending |
| 9-14-02 | 14 | 5 | SDK-05, SDK-04, WF-06, WF-07, WF-10, SDK-02 | Python server leg (children incl. C-CHILD-10/11, API signals/tokens, CAN, HTTP effect) | `cargo test -p ostro-conformance --features polyglot --test polyglot_python` | ⬜ pending |
| 9-15-01 | 15 | 5 | SDK-05, SDK-04, WF-06, WF-07, WF-10, SDK-03 | TypeScript conformance guests + engine leg (100%, cross-compared with Rust) + StarlingMonkey smoke | `cargo test -p ostro-conformance --features polyglot --test polyglot_ts`; `node --test` | ⬜ pending |
| 9-15-02 | 15 | 5 | SDK-05, SDK-04, WF-06, WF-07, WF-10, SDK-03 | TypeScript server leg (children incl. C-CHILD-10/11, API signals/tokens, CAN, HTTP effect) | `cargo test -p ostro-conformance --features polyglot --test polyglot_ts` | ⬜ pending |
| 9-16-01 | 16 | 6 | SDK-05, SDK-04, WF-06, WF-10, SDK-02, SDK-03 | Cross-language chain (C-CHILD-08) and saga with HTTP compensations (C-SAGA-03) | `cargo test -p ostro-conformance --features polyglot --test polyglot_cross` | ⬜ pending |
| 9-16-02 | 16 | 6 | SDK-05, SDK-04, WF-06, WF-10, SDK-02, SDK-03 | Cross-language value probes (C-VAL-01) + full three-language table | `cargo test -p ostro-conformance --features polyglot --test polyglot_cross` | ⬜ pending |
| 9-17-01 | 17 | 7 | SDK-04, SDK-02, SDK-03 | D-P9-3 ladder, repro-check.sh, C-REBUILD-01, docs/sdk-builds.md | `repro-check` | ⬜ pending |
| 9-17-02 | 17 | 7 | SDK-04, SDK-02, SDK-03 | Packaging dry runs for PyPI and npm | `grep / doc checks` | ⬜ pending |
| 9-17-03 | 17 | 7 | SDK-04, SDK-02, SDK-03 | CI wiring — ci-local polyglot block and SHA-pinned ci.yml polyglot job | `ci-local` | ⬜ pending |
| 9-18-01 | 18 | 8 | WF-06, SDK-05 | Spawned-server crash harness + C-CRASH-01 (chain completes under kill -9 and child failpoints) | `cargo test -p ostro-conformance --features polyglot --test crash_leg` | ⬜ pending |
| 9-18-02 | 18 | 8 | WF-06, SDK-05 | C-CRASH-02 (cancel propagates to every descendant under crashes) + gate run | `cargo test -p ostro-conformance --features polyglot --test crash_leg` | ⬜ pending |
| 9-19-01 | 19 | 9 | SDK-02, SDK-03 | Measurement harness (instantiate, first call, start-to-first-step, idle RSS, pool sizing) | `cargo test -p ostro-conformance --features polyglot --test resources` | ⬜ pending |
| 9-19-02 | 19 | 9 | SDK-02, SDK-03 | Publish docs/sdk-resources.md and the SC5 verdict | `grep / doc checks` | ⬜ pending |
| 9-20-01 | 20 | 10 | SDK-02, SDK-03, SDK-04, SDK-05, WF-06, WF-07, WF-10 | Full gate + success-criteria evidence + reservations check | `cargo test -p ostro-conformance --test spec_catalogue`; `cargo test -p ostro-engine --test replay_corpus`; `cargo test -p ostro-witcheck`; `repro-check`; `ci-local` | ⬜ pending |
| 9-20-02 | 20 | 10 | SDK-02, SDK-03, SDK-04, SDK-05, WF-06, WF-07, WF-10 | SDK READMEs + docs/conformance.md + docs/children.md | `grep / doc checks` | ⬜ pending |
| 9-20-03 | 20 | 10 | SDK-02, SDK-03, SDK-04, SDK-05, WF-06, WF-07, WF-10 | 09-VALIDATION results, 09-DECISIONS fold-in, STATE.md | `grep / doc checks` | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Success Criteria → Evidence

| SC | Criterion (ROADMAP) | Evidence | Plans |
|----|---------------------|----------|-------|
| SC1 | Conformance suite 100% for Rust, Python, TS (steps, retries, timers, signals, concurrency/race, cancellation, children, CAN, await tokens, patching, non-determinism) | Rust `engine_leg`/`server_leg` in `cargo test --workspace`; `polyglot_python`, `polyglot_ts`, `polyglot_cross` (scenario × language table) | 09-11, 09-13, 09-14, 09-15, 09-16 |
| SC2 | Two runs differ in `ctx.random`, replay reproduces; rebuild → same module sha | C-RAND-01/02, C-VAL-01 in all languages; `repro-check.sh` (Rust identical; Python/TS identical or accepted residual per D-P9-3 → human_needed); C-REBUILD-01 | 09-14, 09-15, 09-16, 09-17 |
| SC3 | TS → Python → Rust chain completes under crash injection; cancel propagates to every descendant | C-CRASH-01/02 at 100 iterations; C-CHILD-08/09/10/11 (non-crash) | 09-08, 09-13, 09-16, 09-18 |
| SC4 | Saga compensations run in reverse when a later step fails | C-SAGA-01/02 (engine), C-SAGA-03 (server, HTTP) in all languages | 09-06, 09-11, 09-12, 09-16 |
| SC5 | Warm instantiation p50 < 10 ms Python, < 5 ms TS; idle RSS published per language | `resources` measurements + docs/sdk-resources.md (WSL2 advisory, D15) | 09-19 |
| WF-07 | Native time/random APIs and map iteration behave deterministically under replay (Python, TS) | C-NATIVE-01 + C-RAND-02 per language; host tests in 09-09/09-10 | 09-09, 09-10, 09-11, 09-14, 09-15, 09-20 (ticks only with this evidence) |

---

## Wave 0 Requirements

- [ ] `crates/ostro-conformance/` skeleton (`publish = false`, feature `polyglot`, bin `ostro-conf-inspect`), spec model, catalogue + schema, WIT-copy test — 09-02
- [ ] `sdks/python/{pyproject.toml, ostro/_host.py, tests/conftest.py}` with FakeHost — 09-03
- [ ] `sdks/ts/{package.json, package-lock.json, rolldown.ostro.mjs, src/host.ts, test/helpers.ts}` with FakeHost — 09-03
- [ ] `sdks/scripts/build-guest.sh` (py / ts / ts-sm, D-P9-3 wrapper) + pinned `sdks/scripts/toolchain/` (jco 1.42.0, componentize-js 0.23.0) + `polyglot::build` cache — 09-02
- [ ] Raw-binding fixtures `conformance/fixtures/{py-min,ts-min}` + `polyglot_smoke` — 09-02
- [ ] Cross-SDK vectors `conformance/vectors/*.json` — 09-06
- [ ] `sdks/scripts/repro-check.sh` — 09-17
- [ ] ci-local polyglot block + ci.yml polyglot job — 09-17

---

## Manual-Only / Human-Needed

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Publish `ostro` to PyPI and npm | SDK-02, SDK-03 | No credentials, no remotes; publishing is irreversible (D-P9-19) | `cd sdks/python && uv build && uv publish`; `cd sdks/ts && npm publish` after reviewing the 09-17 dry-run file lists |
| Remote CI run of the polyglot job | all | No git remote yet (FOUND-05 pattern) | Push, confirm the `polyglot` job and the existing legs are green |
| Upstream componentize-py determinism PR; componentize-qjs residual issue | SDK-04 | Needs a GitHub account and remotes | File from `sdks/python/toolchain/README.md` |
| SC2 for Python and/or TS if a residual was accepted | SDK-04 | Released toolchains not bit-reproducible (D-P9-3 step 3) | Re-run `bash sdks/scripts/repro-check.sh py ts` after an upstream release |
| SC5 on the native perf machine | SDK-02, SDK-03 | WSL2 numbers are advisory (D15) | `OSTRO_SC5_ENFORCE=1 cargo test --release -p ostro-conformance --features polyglot --test resources -- --ignored --test-threads=1` on native Linux NVMe, 8 vCPU |
| User review of D-P9-3 (and any 09-07 fuel review candidate) | SDK-04 | Adopted default, user may override | Read 09-DECISIONS.md and docs/sdk-builds.md |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 60 s at task level (cached polyglot builds)
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
