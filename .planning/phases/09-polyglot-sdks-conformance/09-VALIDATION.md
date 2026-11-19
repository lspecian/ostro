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
> Source: 09-RESEARCH.md § Validation Architecture, mapped onto plans 09-01..09-17. Adopted decisions: 09-DECISIONS.md (D-P9-1..D-P9-23).

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Rust built-in test harness (`cargo test`), proptest (`rebuild_digest`), `fail` 0.5.1 failpoints (`child::` + Phase 5 prefixes), pytest 8.4.2 via `uv run --no-project --python 3.12 --with pytest==8.4.2` (Python SDK host tests, fake host), `node --test` on Node 24.13.0 (TS SDK host tests, native type stripping), the `ostro-conformance` harness (engine leg on ostro-engine testkit; server leg on an in-process server; crash leg on the spawned failpoint `ostro` binary), `ostro-conf-inspect` (wasmparser component inspection). |
| **Toolchains** | componentize-py 0.25.1 (`uv tool run --from componentize-py==0.25.1`, `--stub-wasi`, embedded CPython 3.14); jco 1.42.0 (QuickJS via bundled componentize-qjs 0.5.0, `--backend-qjs-disable-async --backend-qjs-stub-wasi`; StarlingMonkey via componentize-js 0.23.0, `--disable all`); Rolldown `transform.target: "es2016"` via `sdks/ts/rolldown.ostro.mjs`; Rust 1.100.0 + wit-bindgen 0.63 for Rust guests. All builds through `sdks/scripts/build-guest.sh` (D-P9-3 wrapper env), cached in `target/polyglot-guests/`. |
| **Config** | `scripts/ci-local.sh` gains the polyglot block in 09-15 (toolchain check fails loudly; skippable only with `--no-polyglot`, shown as SKIP). Cargo feature `polyglot` on `ostro-conformance` gates every Python/TS test (D-P9-12). `FAILPOINT_CRATES` must equal the ci.yml "Failpoint suites" step (ostro-server added by 09-02 if absent). ci.yml gains a SHA-pinned `polyglot` job (setup-node, setup-python, setup-uv). |
| **External services** | None required locally. Phase 6 `MockProvider` (in-process) for HTTP effects and saga compensations. Publishing to PyPI/npm is human_needed. **The microk8s cluster reachable via kubectl on this machine is never used.** |
| **Quick run command** | `cd sdks/python && uv run --no-project --python 3.12 --with pytest==8.4.2 python -m pytest tests -q` · `node --test sdks/ts/test/` · `cargo test -p ostro-conformance --locked --test engine_leg -- --test-threads=2` · `cargo test -p <touched crate> --locked` |
| **Full suite command** | `bash scripts/ci-local.sh --no-musl` (includes the polyglot block from 09-15 onwards) |
| **Phase gate commands** | 1. `bash scripts/ci-local.sh` (with musl).<br>2. `cargo test -p ostro-conformance --features polyglot --locked -- --test-threads=2` (engine + server legs, three languages).<br>3. `OSTRO_CRASH_CHAIN_ITERS=100 cargo test -p ostro-conformance --features polyglot --locked --test crash_leg -- --ignored --test-threads=1 --nocapture` (after `cargo build -p ostro --features failpoints --target-dir target/failpoints --locked`).<br>4. `bash sdks/scripts/repro-check.sh all`.<br>5. `cargo test --release -p ostro-conformance --features polyglot --locked --test resources -- --ignored --test-threads=1 --nocapture` (SC5; enforce with `OSTRO_SC5_ENFORCE=1` on the native perf machine, D15).<br>6. `cargo test -p ostro-engine --test replay_corpus --locked` and `cargo test -p ostro-witcheck --locked` (WIT unchanged).<br>7. `cargo test -p ostro-conformance --locked --test spec_catalogue -- --ignored all_specs_present`. |
| **Estimated runtime** | Quick: < 60 s per command (first polyglot build of a guest ≈ 1–3 min, then cached). Full ci-local: ~15–25 min debug incl. polyglot. Crash leg at 100 iterations: 30–90 min. Repro check: ~10 min. |

---

## Sampling Rate

- **After every task commit:** the task's `<automated>` command, plus `cargo clippy -p <crate> --all-targets --locked -- -D warnings` for touched Rust crates (with `--features polyglot` for ostro-conformance, `--features failpoints` for ostro-server failpoint tests).
  - SDK tasks also run the full host suite of the touched SDK (pytest or node --test).
  - Children tasks (09-04, 09-07, 09-12) also run `cargo test -p ostro-partition --locked` and `PROPTEST_CASES=128 cargo test -p ostro-partition --locked --test rebuild_digest`.
- **After every plan wave:** `bash scripts/ci-local.sh --no-musl` + `cargo test -p ostro-engine --test replay_corpus --locked`; from wave 4 on also `cargo test -p ostro-conformance --features polyglot --locked -- --test-threads=2`.
- **Before `/gsd:verify-work`:** the 09-17 gate.
- **Max feedback latency:** 60 s for task-level checks (cached polyglot builds); crash leg, repro check and measurements are gate level.
- Piped verify commands use `set -o pipefail`; cargo test filters go after `--`; no `|| true`; filtered runs must report ≥ 1 passed test (`test result: ok\. [1-9][0-9]* passed`); pytest must report `N passed` with no failed/error; node must report `# pass N≥1` and `# fail 0`.

---

## Wave Structure

| Wave | Plans | Exclusive shared files |
|------|-------|------------------------|
| 1 | 09-01 gate (Phase 6 complete, Phase 5 freeze, PF-F1/F2 in code) + reservations + decisions | RESERVATIONS.md, STATE.md |
| 2 | 09-02 contract/Wave 0; 09-03 S9 spike; 09-04 children 1/2; 09-05 Rust SDK additions + child guests | 09-02: **Cargo files (vs 07-02, 08-01/03/12)**, engine validate.rs (anchor), ostro-conformance crate, sdks/{python,ts} skeletons, build-guest.sh, ci-local/ci.yml (FAILPOINT_CRATES only if needed). 09-03: spikes/s9-polyglot, docs/spikes/S9*. 09-04: **apply/inbound.rs, server derive.rs, sink.rs (vs 07-04, 08-17), lib.rs (one line)**, children.rs. 09-05: **sdks/rust/** (sole owner)**, guests child-*/sdk-saga, conformance/vectors. |
| 3 | 09-06 engine ENG-1/ENG-2; 09-07 children 2/2; 09-08 Python core; 09-09 TS core; 09-10 harness + Rust engine leg | 09-06: engine validate/config/core_budget, **runtime.rs (vs 08-16), engine lib.rs (vs 08-01), server config.rs (vs 07-02, 08-04)**, exports-shape guests. 09-07: **apply/terminal.rs (vs 07-04, 08-07, 08-16), apply/run.rs (vs 07-04), derive.rs, sink.rs**. 09-08: sdks/python/ostro/*. 09-09: sdks/ts/src/*. 09-10: ostro-conformance src, conf-rust guests, spec/engine/. |
| 4 | 09-11 Python/TS effects + saga + first engine runs; 09-12 server leg + Rust pass (children e2e) | 09-11: SDK sources (both), smoke fixtures. 09-12: ostro-conformance server_leg, conf-rust (append), spec/server/, children code bug fixes only. |
| 5 | 09-13 Python/TS conformance (engine + server legs), cross-language chain, saga, SM smoke | conformance guests, SDK fixes, polyglot_legs.rs |
| 6 | 09-14 crash leg; 09-15 repro + packaging + CI; 09-16 measurements | 09-14: crash_leg.rs, spec/crash/. 09-15: **build-guest.sh, ci-local.sh, ci.yml (vs 07-22, 08-20/22)**, pyproject.toml, package.json, docs/sdk-builds.md. 09-16: resources.rs, docs/sdk-resources.md. |
| 7 | 09-17 phase gate | READMEs, docs/conformance.md, docs/children.md, RESERVATIONS flip, VALIDATION, DECISIONS, STATE |

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 9-01-01 | 01 | 1 | (gate) | check + reservations script | 06-{01..20}-SUMMARY; 06-VERIFICATION by-design only; `wit/FROZEN.toml` + tag; witcheck; PF-F1 (`parent_event`), PF-F2 (`continued_to`), PF-W greps; RESERVATIONS rows | n/a | ⬜ pending |
| 9-01-02 | 01 | 1 | (decisions) | grep | STATE.md: Phase 09 decisions D-P9-1..23, `[USER REVIEW] D-P9-3`, `[HUMAN]` publishing, coordination concern | n/a | ⬜ pending |
| 9-02-01 | 02 | 2 | all (contract) | build + deny + unit | `cargo build --workspace` warning-free, `cargo deny check`, no duplicate wasmparser, `cargo test -p ostro-conformance --locked` (catalogue, WIT copies, inspector) | ❌ W0 | ⬜ pending |
| 9-02-02 | 02 | 2 | SDK-02/03 (scaffold) | unit (pytest, node) | `uv run … pytest tests -q`; `npm ci && node --test test/` | ❌ W0 | ⬜ pending |
| 9-02-03 | 02 | 2 | SDK-02/03 (build) | polyglot build + inspect | `cargo test -p ostro-conformance --features polyglot --test polyglot_smoke` (py imports only ostro:*, exports workflows+exports; QJS zero wasi) | ❌ W0 | ⬜ pending |
| 9-03-01 | 03 | 2 | SDK-02/03 | measurement | `docs/spikes/data/S9/measurements.jsonl` has inspect/pool/instantiate for py-asyncio, py-base, ts-qjs, ts-sm | ❌ W0 | ⬜ pending |
| 9-03-02 | 03 | 2 | SDK-02/03 | probe + prototype tests | `pytest test_loop_proto.py`; probes lowering/result_mapping/surrogates/export_naming/ts_engine recorded | ❌ W0 | ⬜ pending |
| 9-03-03 | 03 | 2 | SDK-04 | repro measurement + report | `docs/spikes/data/S9/repro.txt`; S9 report ≥ 120 lines with required sections | ❌ W0 | ⬜ pending |
| 9-04-01 | 04 | 2 | WF-06 | unit (apply) TDD | `cargo test -p ostro-partition --test children_apply` + partition suite + rebuild_digest | ❌ W0 | ⬜ pending |
| 9-04-02 | 04 | 2 | WF-06 | integration (synthetic journals) TDD | `cargo test -p ostro-server --test children_derive` + server suite; no ChildStarted unsupported path | ❌ W0 | ⬜ pending |
| 9-05-01 | 05 | 2 | SDK-04 (vectors) | unit (host) | `cargo test --manifest-path sdks/rust/Cargo.toml -p ostro-sdk --test vectors` | ❌ W0 | ⬜ pending |
| 9-05-02 | 05 | 2 | WF-06 (Rust SDK) | engine integration | guests child-parent/child-leaf build + `cargo test -p ostro-engine --test sdk_children_saga -- children` | ❌ W0 | ⬜ pending |
| 9-05-03 | 05 | 2 | WF-10 (Rust) | engine integration | sdk-saga build + `cargo test -p ostro-engine --test sdk_children_saga -- saga` + ci-local --no-musl | ❌ W0 | ⬜ pending |
| 9-06-01 | 06 | 3 | SDK-02 (ENG-1) | unit + polyglot integration | `cargo test -p ostro-engine --test exports_shape` + `--features polyglot --test polyglot_engine` | ❌ W0 | ⬜ pending |
| 9-06-02 | 06 | 3 | SDK-02/03 (ENG-2) | unit + integration | `cargo test -p ostro-engine --test core_budget`, `-p ostro-server --test engine_class --test config`, polyglot_engine (100 py instances), ci-local --no-musl | ❌ W0 | ⬜ pending |
| 9-07-01 | 07 | 3 | WF-06 | unit (apply) TDD | `cargo test -p ostro-partition --test children_close` + rebuild_digest | ❌ W0 | ⬜ pending |
| 9-07-02 | 07 | 3 | WF-06 | integration + failpoints | `cargo test -p ostro-server --test children_detached` + `--features failpoints --test children_failpoints -- --test-threads=1` + ci-local --no-musl | ❌ W0 | ⬜ pending |
| 9-08-01 | 08 | 3 | SDK-02 | unit (pytest) TDD | `pytest tests/test_loop.py tests/test_ops_errors.py` | ❌ W0 | ⬜ pending |
| 9-08-02 | 08 | 3 | SDK-02/04 | unit (pytest) TDD | `pytest tests -q` (steps, ctx, determinism, export) | ❌ W0 | ⬜ pending |
| 9-08-03 | 08 | 3 | SDK-02 | polyglot build + inspect | `cargo test -p ostro-conformance --features polyglot --test py_sdk_build` | ❌ W0 | ⬜ pending |
| 9-09-01 | 09 | 3 | SDK-03 | unit (node) TDD | `node --test sdks/ts/test/` (promise, loop, encoding) | ❌ W0 | ⬜ pending |
| 9-09-02 | 09 | 3 | SDK-03/04 | unit (node) TDD | `node --test sdks/ts/test/` (step/ctx, export) | ❌ W0 | ⬜ pending |
| 9-09-03 | 09 | 3 | SDK-03 | polyglot build + inspect + lowering | `cargo test -p ostro-conformance --features polyglot --test ts_sdk_build` (zero wasi on QJS, SM builds, no async tokens) | ❌ W0 | ⬜ pending |
| 9-10-01 | 10 | 3 | SDK-05 | unit (harness self-test) TDD | `cargo test -p ostro-conformance --test harness_selftest` | ❌ W0 | ⬜ pending |
| 9-10-02 | 10 | 3 | SDK-05 | guest build | `cargo build … -p conf-rust -p conf-rust-v2` | ❌ W0 | ⬜ pending |
| 9-10-03 | 10 | 3 | SDK-05, SDK-04, WF-10 (Rust) | conformance (engine leg) | `cargo test -p ostro-conformance --test engine_leg -- --test-threads=2` + spec_catalogue + ≥ 25 engine specs + ci-local --no-musl | ❌ W0 | ⬜ pending |
| 9-11-01 | 11 | 4 | WF-10, SDK-02 | unit (pytest) TDD | `pytest tests -q` (effects, saga, vectors; no skips) | ❌ W0 | ⬜ pending |
| 9-11-02 | 11 | 4 | WF-10, SDK-03 | unit (node) TDD | `node --test sdks/ts/test/` (effects, saga, vectors; skipped 0) | ❌ W0 | ⬜ pending |
| 9-11-03 | 11 | 4 | SDK-02/03/04 | engine integration (polyglot) | `cargo test -p ostro-conformance --features polyglot --test sdk_engine_smoke -- --test-threads=1` | ❌ W0 | ⬜ pending |
| 9-12-01 | 12 | 4 | WF-06, SDK-05 | build | conf-rust build + ostro-conformance tests compile warning-free + clippy | ❌ W0 | ⬜ pending |
| 9-12-02 | 12 | 4 | WF-06, SDK-05 (Rust) | conformance (server leg) | `cargo test -p ostro-conformance --test server_leg -- --test-threads=2` + ≥ 12 server specs + ci-local --no-musl | ❌ W0 | ⬜ pending |
| 9-13-01 | 13 | 5 | SDK-05 (Python) | conformance (engine leg) | `cargo test -p ostro-conformance --features polyglot --test polyglot_legs -- engine_leg_python` | ❌ W0 | ⬜ pending |
| 9-13-02 | 13 | 5 | SDK-05 (TS) | conformance (engine leg + SM smoke) | `… --test polyglot_legs -- engine_leg_ts` | ❌ W0 | ⬜ pending |
| 9-13-03 | 13 | 5 | SDK-05, SDK-04, WF-06, WF-10 | conformance (server legs, cross-language, values) | `cargo test -p ostro-conformance --features polyglot --test polyglot_legs -- --test-threads=2` | ❌ W0 | ⬜ pending |
| 9-14-01 | 14 | 6 | WF-06 (SC3) | crash (`#[ignore]`) | `OSTRO_CRASH_CHAIN_ITERS=10 … --test crash_leg -- --ignored crash_chain_completes` | ❌ W0 | ⬜ pending |
| 9-14-02 | 14 | 6 | WF-06 (SC3 cancel) | crash (`#[ignore]`) | `OSTRO_CRASH_CHAIN_ITERS=10 … --test crash_leg -- --ignored` (2 passed); 100-iteration gate run recorded | ❌ W0 | ⬜ pending |
| 9-15-01 | 15 | 6 | SDK-04 (SC2) | build check + rebuild replay | `bash sdks/scripts/repro-check.sh all` (Rust OK) + `--test rebuild` + docs/sdk-builds.md | ❌ W0 | ⬜ pending |
| 9-15-02 | 15 | 6 | SDK-02/03 (packaging) | dry run | `uv build` wheel/sdist contents; `npm pack --dry-run --json` file list | ❌ W0 | ⬜ pending |
| 9-15-03 | 15 | 6 | all (CI) | gate | ci-local polyglot block PASS; ci.yml SHA-pinned setup-node/python/uv; FAILPOINT_CRATES sync | ✅ (extended) | ⬜ pending |
| 9-16-01 | 16 | 6 | SDK-02/03 (SC5) | measurement (`#[ignore]`) | `cargo test --release -p ostro-conformance --features polyglot --test resources -- --ignored --test-threads=1` | ❌ W0 | ⬜ pending |
| 9-16-02 | 16 | 6 | SDK-02/03 (SC5) | doc check | docs/sdk-resources.md ≥ 50 lines with per-language tables and verdict | ❌ W0 | ⬜ pending |
| 9-17-01 | 17 | 7 | all | gate | full ci-local (musl), all_specs_present, replay corpus, witcheck, repro-check, deny, RESERVATIONS flip | ✅ | ⬜ pending |
| 9-17-02 | 17 | 7 | all (docs) | doc check | READMEs ≥ 120 lines with frozen-semantics markers; docs/conformance.md, docs/children.md | ❌ W0 | ⬜ pending |
| 9-17-03 | 17 | 7 | all (records) | grep | VALIDATION Gate results + Human-needed, DECISIONS amendments, STATE results | ✅ | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Success Criteria → Evidence

| SC | Criterion (ROADMAP) | Evidence | Plans |
|----|---------------------|----------|-------|
| SC1 | Conformance suite 100% for Rust, Python, TS (steps, retries, timers, signals, concurrency/race, cancellation, children, CAN, await tokens, patching, non-determinism) | `polyglot_legs` (engine + server legs, three languages) + Rust `engine_leg`/`server_leg` in `cargo test --workspace`; scenario × language table | 09-10, 09-12, 09-13 |
| SC2 | Two runs differ in `ctx.random`, replay reproduces; rebuild → same module sha | C-RAND-01/02, C-VAL-01 in all languages; `repro-check.sh` (Rust identical; Python/TS identical or accepted residual per D-P9-3 → human_needed); C-REBUILD-01 | 09-13, 09-15 |
| SC3 | TS → Python → Rust chain completes under crash injection; cancel propagates to every descendant | C-CRASH-01/02 at 100 iterations; C-CHILD-08/09 (non-crash) | 09-12, 09-13, 09-14 |
| SC4 | Saga compensations run in reverse when a later step fails | C-SAGA-01/02 (engine), C-SAGA-03 (server, HTTP) in all languages | 09-05, 09-10, 09-11, 09-13 |
| SC5 | Warm instantiation p50 < 10 ms Python, < 5 ms TS; idle RSS published per language | `resources` measurements + docs/sdk-resources.md (WSL2 advisory, D15) | 09-16 |

---

## Wave 0 Requirements

- [ ] `crates/ostro-conformance/` skeleton (`publish = false`, feature `polyglot`, bin `ostro-conf-inspect`), spec model, catalogue + schema, WIT-copy test — 09-02
- [ ] `sdks/python/{pyproject.toml, ostro/_host.py, tests/conftest.py}` with FakeHost — 09-02
- [ ] `sdks/ts/{package.json, package-lock.json, rolldown.ostro.mjs, src/host.ts, test/helpers.ts}` with FakeHost — 09-02
- [ ] `sdks/scripts/build-guest.sh` (py / ts / ts-sm, D-P9-3 wrapper) + `polyglot::build` cache — 09-02
- [ ] Raw-binding fixtures `conformance/fixtures/{py-min,ts-min}` + `polyglot_smoke` — 09-02
- [ ] Cross-SDK vectors `conformance/vectors/*.json` — 09-05
- [ ] `sdks/scripts/repro-check.sh` — 09-15
- [ ] ci-local polyglot block + ci.yml polyglot job — 09-15

---

## Manual-Only / Human-Needed

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Publish `ostro` to PyPI and npm | SDK-02, SDK-03 | No credentials, no remotes; publishing is irreversible (D-P9-19) | `cd sdks/python && uv build && uv publish`; `cd sdks/ts && npm publish` after reviewing the 09-15 dry-run file lists |
| Remote CI run of the polyglot job | all | No git remote yet (FOUND-05 pattern) | Push, confirm the `polyglot` job and the existing legs are green |
| Upstream componentize-py determinism PR; componentize-qjs residual issue | SDK-04 | Needs a GitHub account and remotes | File from `sdks/python/toolchain/README.md` |
| SC2 for Python and/or TS if a residual was accepted | SDK-04 | Released toolchains not bit-reproducible (D-P9-3 step 3) | Re-run `bash sdks/scripts/repro-check.sh py ts` after an upstream release |
| SC5 on the native perf machine | SDK-02, SDK-03 | WSL2 numbers are advisory (D15) | `OSTRO_SC5_ENFORCE=1 cargo test --release -p ostro-conformance --features polyglot --test resources -- --ignored --test-threads=1` on native Linux NVMe, 8 vCPU |
| User review of D-P9-3 (and any 09-06 fuel review candidate) | SDK-04 | Adopted default, user may override | Read 09-DECISIONS.md and docs/sdk-builds.md |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 60 s at task level (cached polyglot builds)
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
