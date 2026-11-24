---
phase: 13-cluster-membership-routing-upgrades
plan: 07
subsystem: ostro-engine (WIT linking, validation, AOT/InstancePre caches)
tags: [wit, linker, semver, eng-10, upgrade, cache-key, fixture]
requires:
  - phase: 13-cluster-membership-routing-upgrades
    provides: "13-01 LinkerSet stub (WitLine, WitSelection, select signature, WIT_0_1_LATEST), wit-next-fixture feature; JOURNAL_IFACE at host/state.rs:565"
provides:
  - "ostro_engine::host::linker_set::{select, select_lines, select_from_surface, line_from_bytes, introduced_in, supported_lines, ostro_iface, parse_version, split_ostro, LinkerSet::{new, linker, for_component, lines, with_fixture_0_1_1}, WitLine::{V0_1, V0_2, of, of_version, parse}, FUNCS_0_1_0, FUNCS_0_1_ADDED, LEVEL_0_1_0}"
  - "ostro_engine::validate::ValidateError {MixedWitLines, UnsupportedWitLine, BadWitVersion, NoWitLine, UnknownWitFunction, Unparsable} (From for EngineError::Validation); ValidatedComponent.selection"
  - "ostro_engine::host::state::journal_iface() (derived from WIT_0_1_LATEST; replaces JOURNAL_IFACE)"
  - "Runtime holds a LinkerSet: Runtime::with_linkers, Runtime::linkers, Prepared.wit (WitSelection); L2 keyed by (line, sha)"
  - "cache::cache_key_for(engine, sha, line), AotCache::component_for(sha, wasm, line)"
  - "feature wit-next-fixture: host::fixture_0_1_1 (host at ostro:runtime@0.1.1), host::next_fixture (ostro:runtime@0.2.0 line)"
  - "fixture guests p13-wit011, p13-wit011-lite, p13-wit020, p13-wit-mixed (guests/ workspace)"
affects: [13-14, 13-20]
tech-stack:
  added: []
  patterns:
    - "one Linker<HostState> per WIT major line; within a line only the newest 0.1.N is defined (Wasmtime semver alternate key resolves older imports/exports)"
    - "min level = max introduced_in over imported functions (component type), not the package version"
    - "hand-written interface names derive from WIT_0_1_LATEST via ostro_iface()"
    - "test-only bindgen hosts behind a feature, remapping unchanged interfaces with `with` to the 0.1.0 bindings"
key-files:
  created:
    - crates/ostro-engine/src/host/fixture_0_1_1.rs
    - crates/ostro-engine/src/host/next_fixture.rs
    - crates/ostro-engine/fixtures/wit-0.1.1/runtime.wit
    - crates/ostro-engine/fixtures/wit-next/runtime.wit
    - crates/ostro-engine/tests/common/wit_fixtures.rs
    - crates/ostro-engine/tests/linker_set.rs
    - guests/p13-wit011/{Cargo.toml,src/lib.rs}
    - guests/p13-wit011-lite/{Cargo.toml,src/lib.rs}
    - guests/p13-wit020/{Cargo.toml,src/lib.rs}
    - guests/p13-wit-mixed/{Cargo.toml,src/lib.rs,wit/mixed.wit}
  modified:
    - crates/ostro-engine/src/host/{linker_set,state,mod}.rs
    - crates/ostro-engine/src/{validate,cache,runtime}.rs
key-decisions:
  - "Fixture components are real guests in the guests/ workspace (wit_bindgen::generate! over the frozen wit/ or the test-only fixture packages), not wit_component::dummy_module: ostro-engine has no wit-component dependency and Cargo files are 13-01-only (STOP rule). Real guests also run activations."
  - "The 0.1 cache key is byte-identical to the pre-13-07 key; any other line salts the digest part with sha256(compat ‖ 'wit=' ‖ line). No artefact is shared across lines, no recompile on upgrade, and tests/cache.rs's key-format pin stays untouched."
  - "Unknown ostro:runtime function on the selected line = upload error (UnknownWitFunction), not a silent level 1. A production build rejects the 0.1.1 fixture guest that calls note-hint."
  - "LinkerSet::default() keeps the 13-01 meaning (describes the build; no linkers) because ostro-server p13/mod.rs (13-02-only) uses LinkerSet::default().lines() for NodeVersion.wit_lines; LinkerSet::new(engine) builds the linkers."
  - "The 0.1.1 fixture host generates context and journal fresh at 0.1.1 and remaps types/effects/log with bindgen `with` to the 0.1.0 modules (they stay defined at @0.1.0); the journal is the interface the overrides shadow, so the trap the fix closes is covered without type conversions."
  - "The 0.2 fixture omits the effects interface (in addition to the one removed function and one changed signature) to keep the test-only host small."
  - "Only 0.1 has an activation driver: Runtime validates a 0.2 module on its linker (with the feature) and then refuses it ('no activation driver'); 0.2 runs through next_fixture::bindings directly in the test."
requirements-completed: [ENG-10]
duration: "~2h code + 4h ci-local"
completed: 2026-11-24
---

# Phase 13 Plan 07: LinkerSet by WIT major line Summary

**The engine now links one `Linker<HostState>` per WIT major line. Within the 0.1 line only the newest `0.1.N` is defined, and Wasmtime's semver alternate key resolves older imports and the `workflows` export to it. `select()` reads a component's imports, rejects mixed lines, and computes the minimum feature level from the imported functions. The hard-coded `JOURNAL_IFACE` is gone. A test-only host built at `ostro:runtime@0.1.1` runs an unmodified 0.1.0 SDK guest with the fuel-aware journal overrides, and a test-only `ostro:runtime@0.2.0` line links side by side. Both cache levels key on the line.**

## `select()` API (for 13-14)

```rust
// ostro_engine::host::linker_set
pub const WIT_0_1_LATEST: &str = "0.1.0";
pub const OSTRO_PACKAGE: &str = "ostro:runtime";
pub const LEVEL_0_1_0: u32 = 1;
pub fn ostro_iface(iface: &str, version: &str) -> String;      // "ostro:runtime/journal@0.1.0"
pub fn parse_version(v: &str) -> Option<(u32, u32, u32)>;      // strict x.y.z, no semver dep
pub struct WitLine { pub major: u32, pub minor: u32 }          // V0_1, V0_2; of(), of_version(), parse("0.1"); Display "0.1"
pub struct WitSelection { pub line: WitLine, pub min_level: u32 }
pub fn select(component: &Component, engine: &Engine) -> Result<WitSelection, EngineError>;  // 13-01 signature
pub fn select_lines(component: &Component, engine: &Engine) -> Result<WitSelection, ValidateError>;
pub fn select_from_surface(imports: &[(String, Vec<String>)], exports: &[String], supported: &[WitLine]) -> Result<WitSelection, ValidateError>;
pub fn line_from_bytes(wasm: &[u8]) -> Result<WitLine, ValidateError>;   // pre-compile (wasmparser, top-level names)
pub fn introduced_in(line: WitLine, iface: &str, func: &str) -> Option<u32>;
pub fn supported_lines() -> Vec<WitLine>;                       // [0.1] (+ 0.2 with wit-next-fixture)
pub struct LinkerSet;  // Default = no linkers (describes the build); new(engine), linker(line), for_component(c, e), lines()
#[cfg(feature = "wit-next-fixture")] LinkerSet::with_fixture_0_1_1(engine)

// ostro_engine::validate
pub enum ValidateError { MixedWitLines(Vec<String>), UnsupportedWitLine(String), BadWitVersion(String),
                         NoWitLine, UnknownWitFunction { line, import, func }, Unparsable(String) }
impl From<ValidateError> for EngineError   // -> EngineError::Validation(vec![msg])
pub struct ValidatedComponent { .., pub selection: WitSelection }
```

Rules: every `ostro:runtime/*` import name and export name must be on one line (WASI and other packages are ignored). Mixed lines → `MixedWitLines(["0.1","0.2"])` (13-14: 400). A line this build does not link → `UnsupportedWitLine("0.2")` (400). The level is the max `introduced_in` over the functions in the imported instance types, or 1 if the guest imports none. An unknown function on the line → `UnknownWitFunction` (400). `validate_loaded` reports a selection error after the allow-list violations, and it skips `NoWitLine` when "missing required export" already covers it. `Runtime::prepare_cached` returns `Prepared.wit`, so 13-14's upload gate can use it directly.

## `introduced_in` table

| Line | Interface | Functions | Level |
|------|-----------|-----------|-------|
| 0.1 | context | info, now, random-u64, random-bytes, uuid-v7, is-replaying, patched, set-status, continue-as-new-suggested | 1 |
| 0.1 | journal | `[method]step.seq`, `[method]step.complete`, begin-step | 1 |
| 0.1 | effects | http-fetch, sql-query, object-store, sleep, sleep-until, wait-signal, start-child, create-await-token, cancellation, cancel-op, wait | 1 |
| 0.1 | log | emit | 1 |
| 0.1 (`FUNCS_0_1_ADDED`, feature only) | journal | note-hint (0.1.1 fixture) | 2 |
| 0.2 (feature only) | context / journal / log | every fixture function | 2 |

`introduced_in_table_matches_the_frozen_wit` (lib test) parses `wit/ostro-runtime-0.1.0/runtime.wit` and pins `FUNCS_0_1_0` to it. A real `0.1.N` adds its functions to `FUNCS_0_1_ADDED` and bumps `WIT_0_1_LATEST`. `latest_matches_the_wit_dir` ties `WIT_0_1_LATEST` to `wit/`.

## Fixture approach

The plan asked for `wit_component::dummy_module` + `ComponentEncoder`. ostro-engine has no `wit-component` dependency, and adding one is a Cargo change (13-01-only). So the fixtures are **real guests** in the `guests/` workspace (`members = ["*"]`, so `guests/Cargo.toml` is untouched), loaded through `tests/common/wit_fixtures.rs`:

| Guest | WIT | Ostro imports | Selection |
|-------|-----|---------------|-----------|
| `trivial-steps` (existing SDK guest) | wit/ 0.1.0 | `@0.1.0` incl. journal | {0.1, 1} |
| `p13-wit011` | fixtures/wit-0.1.1 | `@0.1.1`, calls `journal.note-hint` | {0.1, 2} with the feature; `UnknownWitFunction` without |
| `p13-wit011-lite` | fixtures/wit-0.1.1 | `@0.1.1`, only 0.1.0 functions | {0.1, 1} |
| `p13-wit020` | fixtures/wit-next (0.2.0) | `@0.2.0` | {0.2, 2} with the feature; `UnsupportedWitLine("0.2")` without |
| `p13-wit-mixed` | wit/ + wit-next + its own world | `log@0.1.0` + `log@0.2.0` | `MixedWitLines` |

wit-component imports only the functions a guest uses, so `p13-wit011-lite` really imports no `note-hint`. That makes the function-level rule observable.

`fixtures/wit-0.1.1/runtime.wit` is the frozen 0.1.0 text with `@0.1.1` and one added function (`fixture_0_1_1_is_frozen_plus_one_function` pins it). `wit/` and `wit/FROZEN.toml` are untouched. `fixtures/wit-next/runtime.wit` is `ostro:runtime@0.2.0`: `run-info` gains `region` (changed `context.info` signature), `continue-as-new-suggested` is removed, and there is no `effects`.

## Export-lookup result

Wasmtime 50.0.2 resolves **both directions** inside a line with no alias instance (so the D-P13 open-question fallback is unused):
- `p13-wit011-lite` (imports `@0.1.1`, exports `workflows@0.1.1`) validates, instantiates and **runs** on the 0.1.0 production host. Its imports resolve through the `@0.1` alternate key, and the runtime's 0.1.0 `WorkflowPre` finds `workflows@0.1.1`.
- On the 0.1.1 fixture host, the 0.1.0 `trivial-steps` guest runs 3 steps to completion. The fixture's bindgen `begin-step` / `[method]step.complete` return a protocol violation if called, so this proves the overrides at `journal@0.1.1` reached the 0.1.0 guest. `p13-wit011` runs on the same host, and its `workflows@0.1.1` export is found by the `@0.1.0` lookup.
- The trap: `journal_overrides_cover_0_1_0_guest` places the overrides at the stale exact `journal@0.1.0` on the 0.1.1 host. The 0.1.0 guest then fails `instantiate_pre`, because the exact name wins and lacks the `step` resource.
- Lines never cross: the 0.2 guest does not link on the 0.1 linker, and the 0.1.0 guest does not link on the 0.2 linker.

## Cache key change

- L1 AOT key: `cache_key_for(engine, sha, line)`. On **0.1 the key is byte-identical to the pre-13-07 key** (`hex(sha)-<16 hex of compat_digest>`), so existing artefacts stay valid and an upgrade does not recompile. On any other line the 16-hex digest part is `sha256(compat_digest ‖ len ‖ "wit=<line>")`. `cache_key(engine, sha)` and `AotCache::component(sha, wasm)` are the 0.1 wrappers. `cache_key_includes_line`: the same bytes compiled for 0.1 and 0.2 give two artefacts and 2 compiles, and a repeat 0.1 load is a hit.
- L2 `InstancePre` LRU: keyed by `(WitLine, sha)`. The hit path (`prepared(sha)`, `prepare_cached` hit) looks up `(0.1, sha)` without parsing, since a sha has exactly one line and only 0.1 is executable. On a miss the line comes from `line_from_bytes` before compile, and validation must select the same line.
- This deviates from the plan's "key = old key + line component": the line is folded into the digest instead of appended. The 0.1 key is then unchanged, which the plan allowed for ("stable apart from the added component"), and `tests/cache.rs::cache_key_stable` (hex + 16-char digest format pin) needs no edit.

## Task commits

1. `d665ecd` `test(13-07)`: fixture guests, fixture WIT packages, `tests/common/wit_fixtures.rs`, `tests/linker_set.rs`
2. `41557c7` `feat(13-07)`: LinkerSet by WIT major line, `select()` + `introduced_in`, `journal_iface()`, 0.1.1 fixture host, 0.2 fixture line, cache key by line

The two tasks share `linker_set.rs`, `validate.rs` and `runtime.rs` (`LinkerSet::new` builds the 0.2 line under the feature, and the selection rules are shared). So the plan's Task 1 / Task 2 split became a test commit plus an implementation commit.

## Verification

- `cargo test -p ostro-engine --locked --test linker_set`: 10 passed. With `--features wit-next-fixture`: 12 passed.
- Full engine suite at the implementation tree: `cargo test -p ostro-engine --locked` ran 34 binaries, 341 passed. `--features wit-next-fixture` ran 34 binaries, 344 passed. The replay corpus (6) is green in both runs. `--lib` 109, including `introduced_in_table_matches_the_frozen_wit` and `versions_parse_by_hand`.
- `cargo clippy -p ostro-engine --all-targets --locked -- -D warnings`: clean with and without `wit-next-fixture`.
- `git status --porcelain -- wit/` is empty (wit/ and FROZEN.toml untouched).
- `bash scripts/ci-local.sh --no-musl --no-polyglot` at 41557c7 (main, including concurrent 13-0x commits up to 20ddfb0): **all steps passed** (19:13 to 23:22 UTC, under the Phase 12 campaign load). PG/MySQL/S3 legs are SKIP (human_needed, env unset); the mutant matrix bit 11/11.

## Deviations from Plan

1. **[Rule 3 - Blocking] Fixtures are real guests, not `dummy_module`.** ostro-engine has no `wit-component` dependency, and adding one is a Cargo change that is 13-01-only (STOP rule). The plan allowed a fallback ("hand-written WAT core modules"), but wasmtime here has no `wat` feature either. Real guests also run activations. Files: `guests/p13-*`, `tests/common/wit_fixtures.rs` (included with `#[path]`, so `tests/common/mod.rs` is untouched).
2. **[Adaptation] Cache key:** the line is folded into the digest on non-0.1 lines, and the 0.1 key is unchanged (see above).
3. **[Adaptation] `LinkerSet::default()`** stays a no-linker build description, because ostro-server `p13/mod.rs` (13-02-only) calls `LinkerSet::default().lines()`. `LinkerSet::new(engine)` builds the linkers.
4. **[Adaptation] `ValidateError`** is a new enum in `validate.rs` (there was none; 13-01 mapped `select` to `EngineError`). `select` keeps the 13-01 signature, and `select_lines` gives the typed error.
5. **[Scope] 0.2 fixture omits `effects`** (plus the planned removed function and changed signature) to keep the test-only host small.
6. **[Adaptation] Paths:** the plan's `/tmp/ostro-wt-13-07` / `/tmp/ostro-target-13-07` became the orchestrator's scratchpad worktree and `target/slot-2`.

## Handover

- **13-14:** call `linker_set::select(&component, engine)` or use `Prepared.wit` from `Runtime::prepare_cached`. Mixed / unsupported / unknown-function errors arrive as `EngineError::Validation` with the `ValidateError` text (→ 400). Store `min_level` in ext 101 when finalized ≥ 2. Fixture components for server tests: the guests `p13-wit011` (level 2), `p13-wit011-lite` (level 1), `p13-wit-mixed` (mixed) and `p13-wit020` (0.2) are in the `guests/` workspace. ostro-server tests can load them with their existing guest builder, by name. Without `wit-next-fixture` (server builds), `p13-wit011` is rejected as an unknown function, not as level 2. For a level-2 module in a server test, enable `ostro-engine/wit-next-fixture` in the test or use the lite guest for level 1.
- **13-20:** the `wit-next-fixture` step should run `cargo test -p ostro-engine --features wit-next-fixture --locked` and `cargo clippy -p ostro-engine --features wit-next-fixture --all-targets --locked -- -D warnings`. ci.yml hunk (not edited here, coordinator-owned):

```yaml
      - name: WIT fixture lines (13-07, ENG-10)
        run: |
          cargo test -p ostro-engine --features wit-next-fixture --locked
          cargo clippy -p ostro-engine --features wit-next-fixture --all-targets --locked -- -D warnings
```

- A real `0.1.N`: add `wit/ostro-runtime-0.1.N`, switch `host/bindings.rs` to it, bump `WIT_0_1_LATEST` and list the new functions in `FUNCS_0_1_ADDED` with their level. Every hand-written interface name follows from the constant (`no_versioned_iface_literal_in_host` guards `src/host/`).
