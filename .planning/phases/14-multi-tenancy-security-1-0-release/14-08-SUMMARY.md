---
phase: 14-multi-tenancy-security-1-0-release
plan: 08
subsystem: fuzzing
tags: [fuzz, wasm, sec-05]
requirements: [SEC-05]
key-files:
  created: [crates/ostro-fuzz-targets/tests/wasm_targets.rs, fuzz/corpus/evil_guest/, fuzz/corpus/smith_component/, fuzz/regressions/evil_guest/spinning_initialize]
  modified: [crates/ostro-fuzz-targets/src/evil_guest.rs, crates/ostro-fuzz-targets/src/smith.rs, crates/ostro-engine/src/runtime.rs]
completed: 2026-11-27
---

# Phase 14 Plan 08: Wasm fuzz targets Summary

evil_guest: malformed WIT values through every ostro:runtime 0.1.0 import. smith_component: wasm-smith components. Both run against the real engine, with a budget invariant checked after every stage.

## Results
- Template: `wit_component::dummy_module` (Legacy/Sync mangling) was used. The WAT fallback was not needed.
- evil_guest: 200/200 random inputs encode (threshold 90%). The valid-call seed lifts without a trap and reaches the journal. The host then rejects zero arguments on protocol rules (`wait` on an empty op list); the test accepts Completed, Evicted or ProtocolViolation. The five malformed cases each fail the attempt with the expected trap: OOB string, discriminant 7, 2^31 list, invalid UTF-8, and realloc returning 0xFFFF_FFF0. A `spin` flag (bit 7 of the call-count byte) makes an export loop forever.
- smith_component: 200/200 encode and 73/200 reach instantiation (threshold 30%). Each input is prepared uncached, so nothing persists between iterations.
- Seeds: 20 per target plus 1 regression. Both targets replay in 14-07's `corpus_replay`.
- Smoke on dell-4 (`OSTRO_FUZZ_SECS=60 bash scripts/fuzz-smoke.sh evil_guest smith_component`):
  - evil_guest: 36 execs, cov 57562, ok.
  - smith_component: 31 execs, cov 81084, ok.
  - Speed is under 1 exec/s because the ASan build compiles each component with Cranelift. No crashes.
- Long run, not done: `OSTRO_FUZZ_SECS=1800 bash scripts/fuzz-smoke.sh evil_guest smith_component` (or the 14-07 nightly workflow).

## Finding (SEC-05), fixed
`Runtime::calibrate` (crates/ostro-engine/src/runtime.rs) instantiated with `set_fuel(u64::MAX)` and `set_epoch_deadline(u64::MAX)`. wit-component calls `_initialize` from its fixup module's start function, so the guest runs during instantiation. A looping `_initialize` therefore hung admission forever while holding the pool permit: deploy-time describe and every activation. smith_component found this; it hung the wasm_targets test binary on dell-4 for 6 h. The fix bounds calibration by `activation_fuel` and `activation_timeout`; a failed calibration falls back to the conservative weight. The fix is 8 lines. The regression test is `evil_spinning_initialize_is_bounded` (with a 120 s watchdog), and the regression input is `fuzz/regressions/evil_guest/spinning_initialize`.

## Deviations
- wasm-smith is built without its `wasmparser` feature (14-01 manifest), so `available_imports` and `exports` would panic. The smith module is therefore generated without imports or exports and re-encoded with `wasm_encoder::reencode`: the template imports are prepended with shifted function indices, and each export is bound either to a smith function of the right type or to a generated caller. No manifest change. Enabling `wasm-smith/wasmparser` later would allow the direct config.
- The FuzzSink implements the async_trait methods in their expanded form, because the crate has no async-trait dependency.

## Gate
gate incomplete — consolidated gate pending on dell-4. Verified on dell-4:
- clippy -D warnings on ostro-fuzz-targets and ostro-engine
- ostro-fuzz-targets unit tests, wasm_targets and corpus_replay
- ostro-engine pool, core_budget, runtime_cache and sandbox
- fuzz smoke at 60 s per target

Not verified: the full ci-local (all other ostro-engine tests, the rest of the workspace, musl) and the long fuzz runs.
