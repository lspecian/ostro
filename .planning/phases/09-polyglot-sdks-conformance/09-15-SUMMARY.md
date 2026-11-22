---
phase: 09-polyglot-sdks-conformance
plan: 15
subsystem: conformance
tags: [conformance, typescript, engine-leg, server-leg, starlingmonkey, wf-07, sdk-05, wave-5]
requires:
  - phase: 09-polyglot-sdks-conformance
    provides: "09-10/09-12 TS SDK (core, effects, saga); 09-11 engine leg + conf-rust contract; 09-13 server leg harness + server workflows"
provides:
  - "conformance/guests/ts: every conf_* engine workflow and every 09-13 server workflow as ts_<base>"
  - "conformance/guests/ts-v2: v2 bodies of conf_patch / conf_ndet"
  - "crates/ostro-conformance/tests/polyglot_ts.rs: engine_leg_ts, engine_leg_ts_sm_smoke, server_leg_ts_* (4 families) + coverage test"
affects: [09-16, 09-17, 09-18, 09-20]
tech-stack:
  added: []
  patterns:
    - "Per-language test file that runs the Rust reference and the language side by side in one process and cross-compares"
    - "Shielded child await in TS = child op created through the shielded ctx `s` (no AsyncContext)"
key-files:
  created:
    - conformance/guests/ts/index.ts
    - conformance/guests/ts-v2/index.ts
    - crates/ostro-conformance/tests/polyglot_ts.rs
  modified: []
decisions:
  - "ts-v2 exports only conf_patch and conf_ndet: they are the only workflows the engine leg runs on the v2 module, and a guest dir is built on its own (build-guest.sh skips symlinks), so sharing v1 bodies would mean a 500-line copy"
  - "conf_rand_native uses the native APIs (crypto.getRandomValues for the 8 bytes, Math.random for the f64). They are host random-bytes / one random-u64 draw, so the values still equal Rust's random_bytes / random_f64 and the cross-language value compare holds"
  - "conf_parent_shielded_child creates the child op through the shielded ctx, so the run cancellation never rejects the await; the CancelledError comes from the child's cancelled outcome (the cascade). This makes C-CHILD-11 test 'cascade beats shielding' for real in TS"
  - "conf_native (TS) observes Date.now(), new Date().toISOString(), Math.random(), a 16-key Map's iteration order and the order of a Map keyed by Math.random() draws, outside and inside step observe"
metrics:
  duration: "~45 min"
  completed: 2026-11-22
---

# Phase 9 Plan 15: TypeScript conformance (engine + server legs, C-NATIVE-01, StarlingMonkey smoke) Summary

**The TypeScript SDK passes the language-neutral suite 100%. That is 27/27 engine-leg scenarios on QuickJS and 14/14 server-leg scenarios with every role in TypeScript, each compared with the Rust reference run in the same process. The StarlingMonkey smoke subset passes 5/5. WF-07 has TypeScript evidence through C-NATIVE-01. No SDK, engine or server deviation was found, so no product code changed.**

## Commits

| Task | Commit | Content |
|------|--------|---------|
| 1 | `a25493d` | `conformance/guests/ts/index.ts`, `conformance/guests/ts-v2/index.ts`, `polyglot_ts.rs` engine leg + SM smoke |
| 2 | `d4129f9` | `polyglot_ts.rs` server leg (4 families) + coverage test |

## Scenario table (TypeScript, QuickJS unless noted; debug build, `--test-threads=2`)

Every row passed:
- the runner's strict checks: two executions byte-identical, every journal of the chain replays with 0 appended events, and `differs_across_seeds` where the spec asks for it;
- the spec expectations;
- `cross_compare` with Rust. For the engine leg that is status, canonical outcome and projection (facts and values; C-NATIVE-01 compares facts only). For the server leg it is status and root facts.

| Scenario | Status | Act. | Time | Scenario | Status | Time |
|----------|--------|------|------|----------|--------|------|
| C-STEP-01 | completed | 1 | 817 ms (first module prepare) | C-CHILD-01 | completed | 1.22 s (first TS deploy) |
| C-STEP-02 | completed | 1 | 9 ms | C-CHILD-02 | completed | 20 ms |
| C-STEP-03 | completed | 1 | 10 ms | C-CHILD-03 | completed | 2.05 s |
| C-STEP-04 | completed | 2 | 17 ms | C-CHILD-04 | completed | 1.04 s |
| C-NATIVE-01 | completed | 1 | 16 ms | C-CHILD-05 | completed | 1.05 s |
| C-RETRY-01 | completed | 3 | 21 ms | C-CHILD-06 (+ run_timeout variant) | completed | 2.93 s |
| C-RETRY-02 | failed | 2 | 15 ms | C-CHILD-07 | completed | 43 ms |
| C-TIMER-01 / 02 | completed | 2 / 2 | 14 / 15 ms | C-CHILD-09 | cancelled | 34 ms |
| C-SIG-01 / 02 | completed | 3 / 2 | 20 / 14 ms | C-CHILD-10 (+ child_terminated variant) | completed | 2.99 s |
| C-RACE-01 / 02 | completed | 4 / 2 | 27 / 15 ms | C-CHILD-11 | completed | 1.07 s |
| C-CANCEL-01 / 02 / 03 | cancelled / cancelled / completed | 2 / 2 / 1 | 14 / 14 / 9 ms | C-SIG-API-01 | completed | 1.20 s |
| C-PATCH-01 / 02 | completed | 3 / 2 | 752 (v2 prepare) / 16 ms | C-TOKEN-API-01 | completed | 35 ms |
| C-NDET-01 | non_determinism | 2 | 13 ms | C-CAN-02 | completed | 23 ms |
| C-RAND-01 / 02 | completed | 1 / 1 | 13 / 12 ms | C-FX-01 | completed | 13 ms |
| C-VAL-01 | completed | 1 | 10 ms | | | |
| C-CAN-01 | completed (3-run chain) | 3 | 25 ms | | | |
| C-TOKEN-01 | completed | 2 | 15 ms | | | |
| C-STATUS-01 | completed | 1 | 10 ms | | | |
| C-SAGA-01 / 02 | failed / cancelled | 1 / 2 | 11 / 18 ms | | | |

**StarlingMonkey smoke (`Lang::TsSm`, D-P9-1):**

| Scenario | Status | Time |
|----------|--------|------|
| C-STEP-01 | completed | 6.2 s (includes module prepare) |
| C-RACE-01 | completed | 25 ms |
| C-SIG-01 | completed | 20 ms |
| C-RAND-01 | completed | 13 ms |
| C-CANCEL-01 | cancelled | 13 ms |

All five are also cross-compared with Rust.

**Leg runtimes** (cached guest components):

| Leg | Wall time |
|-----|-----------|
| Engine leg, TS | 2.2 s (27 scenarios, Rust reference runs included) |
| SM smoke | 6.3 s |
| children_basic | 9.9 s |
| children_lifecycle | 5.4 s |
| children_resume_shielding | 7.5 s |
| api_signals_tokens_can_fx | 1.8 s |
| Whole `polyglot_ts` binary (`--test-threads=2`) | 18 s |

A fresh guest build adds about 7 s (QuickJS) and 4 s (StarlingMonkey).

## Key evidence

- **C-STEP-04 (step exclusivity, B1).** `conf_step_sibling` runs `Promise.all([step("s", async () => { await Promise.resolve(); return "s" }), sibling()])`. The sibling first yields, then draws `ctx.randomU64()` and sleeps 1 s. TS produces Rust's projection `step:s, timer#1, terminal:completed`, and `sibling_rand` = `16357943517170225811`, which is the first ctx-stream draw and not a step-local one.
  - Mutation check: I removed the sibling's yield, so it ran before the step opened. The run then fails with `C-STEP-04 [ts]: projection differs`. This shows the comparison really pins the order.
- **C-NATIVE-01 (WF-07).** The values are replay-identical (strict replay with 0 appended events, byte-identical double execution). They differ across seeds (`differs_across_seeds`). Inside step `observe`, `Math.random()` gives a different value from the body's draw (the step-local stream). `Date.now()` is logical time.
- **C-RAND-01/02, C-VAL-01** equal Rust exactly. C-VAL-01's pinned seed-42 values also match on QuickJS.
- **C-SAGA-01/02:** the same step values and order as Rust (`book_a, book_b, book_c, cancel_b, cancel_a`). The same failure (`book_c_failed`), and cancelled under run cancellation.
- **C-CHILD-10:** resume after close. Both the base case (`caught: cancelled`) and the variant (`caught: child_terminated`) pass with TS parent and TS child.
- **C-CHILD-11:** cascade beats shielding. The shielded await on the in-flight child still gets `CancelledError`, which comes from the child's cancelled outcome through the cascade with reason `conformance`. The child started during cleanup runs until the parent closes, then gets `parent_closed`.
- **Exactly one child run per (parent, op).** This is checked after every server family (10 + 10 + 8 child starts, Rust and TS together).

## SDK deviation log

**None.** Every scenario passed on the first TypeScript run, on both QuickJS and StarlingMonkey. No file under `sdks/ts/**` changed, so `sdks/ts/test/regressions.test.ts` was not needed and was not created.

## Spec changes

None. No file under `conformance/spec/**` changed.

## Deviations from Plan

1. **ts-v2 is minimal.** It holds only `conf_patch` and `conf_ndet`, the bodies the plan says it overrides. The other workflows are not repeated (see decisions). `conf-rust-v2` likewise exports only the engine set.
2. **The server families are tested in four groups.** The HTTP effect family is merged with the API family, so each group starts one harness. All 14 server specs are still covered, and `server_leg_ts_covers_every_09_13_scenario` asserts this.
3. **Not run:** `scripts/ci-local.sh` (load rule of this session; the 09-20 gate owns it). Also not run: the whole-workspace test suite.

## Verification

- `cargo test -p ostro-conformance --features polyglot --locked --test polyglot_ts -- --test-threads=2`: **test result: ok. 7 passed**, 18 s. The `engine_leg` filter gives 2 passed.
- `OSTRO_CONF_LANGS=rust,ts,ts-sm cargo test -p ostro-conformance --features polyglot --locked --test engine_leg -- --test-threads=2`: **ok. 16 passed**. 09-11's generic runner agrees.
- `node --test --test-reporter=tap sdks/ts/test/`: **# pass 99, # fail 0** (SDK unchanged).
- `cargo clippy -p ostro-conformance --features polyglot --all-targets --locked -- -D warnings`: clean. `cargo fmt -p ostro-conformance -- --check`: clean.

## Open issues

- None blocking.
- 09-16 (cross-language children) can call `run_spec(spec, &[Lang::Rust, Lang::Ts])` directly: the TS server workflows take the child's full prefixed name as input, like Rust's.

## Self-Check: PASSED

- FOUND: `conformance/guests/ts/index.ts`, `conformance/guests/ts-v2/index.ts`, `crates/ostro-conformance/tests/polyglot_ts.rs`.
- FOUND: `a25493d`, `d4129f9`.
