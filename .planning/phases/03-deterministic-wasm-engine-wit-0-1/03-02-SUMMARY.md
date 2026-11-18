---
phase: 03-deterministic-wasm-engine-wit-0-1
plan: 02
subsystem: engine
tags: [journal, codec, postcard, sha2, command-hash, golden-vectors, proptest, tdd]
requires:
  - "03-01: ostro_types::journal contract types"
provides:
  - "ostro_types::journal::{encode_event, decode_event, encode_result, decode_result}"
  - "ostro_types::journal::{canonical_args, command_hash, command_args, CodecError, EventKind::from_code}"
  - "crates/ostro-types/tests/golden/journal_v1.txt (golden vectors v1)"
affects: [03-05, 03-07, 03-08, phase-2-ostro-types, phase-4]
tech-stack:
  added: []
  patterns:
    - "Single private Canon writer defines every hashed primitive layout"
    - "Golden file + OSTRO_BLESS=1 regeneration (atomic tmp+rename)"
key-files:
  created: []
  modified:
    - crates/ostro-types/src/journal.rs
    - crates/ostro-types/tests/journal_golden.rs
    - crates/ostro-types/tests/golden/journal_v1.txt
decisions:
  - "Header names are ASCII-lowercased (to_ascii_lowercase), not Unicode-lowercased, so the hash cannot change with Rust's Unicode tables"
  - "SQL bytea inside the params digest is an inline length-prefixed blob; absent HTTP/object body hashes as the empty body"
  - "Event payload NaN bits are kept exactly (ENG-04); only the command hash canonicalises NaN"
  - "command_args(&EffectRequest) is the caller-facing builder; it keeps header names only"
  - "Golden lines carry a name column: `event <kind> <name> <hex>`, `result <name> <ver+hex>`, `hash <kind-name> <hex>`"
metrics:
  duration: 15min
  completed: 2026-11-18
---

# Phase 3 Plan 02: Journal Codec + Command Hash v1 Summary

This plan adds the byte-stable journal event codec, the recorded-result codec and a hand-specified per-kind command hash (v1) to `ostro_types::journal`. A committed golden vector file pins all three.

## What was built

- **Event codec:** `encode_event(&Event) -> (u32, Vec<u8>)` produces `[EVENT_CODEC_VER] ‖ postcard((ts_nanos, payload))`. `seq` is not in the bytes because Phase 2 carries it. `decode_event(seq, kind, bytes)` rejects an unknown kind, a version byte other than 1, truncated or empty input and trailing bytes. It also adds `EventKind::from_code`.
- **Result codec:** `encode_result` / `decode_result<T>` check the version and reject trailing bytes.
- **Command hash:** `canonical_args` writes everything through one private `Canon` writer: strings with a length prefix, digested blobs, LE integers, NaN-canonicalised f64 and counted lists. `command_hash(hash_ver, args)` returns `Result`, so `hash_ver != 1` gives `UnsupportedVersion`. The hash is `sha256("ostro/cmd" ‖ ver ‖ kind ‖ args)[..16]`.
- **`command_args(&EffectRequest)`** builds the hash input for an effect request. It keeps only the hashed fields, so header values are never included.
- **`CodecError`** has the variants UnknownKind, UnsupportedVersion, Malformed and KindMismatch. Display and Error are written by hand; no thiserror.
- **Module docs:** both layout tables (primitives, and per kind which fields are hashed and which are not), plus the rule that changing a mirror field or variant means bumping the codec version.
- **Golden file:** 51 lines, made up of the header, 22 events (every EventKind, including HTTP/SQL/object effects and a SQL row with 1.5 and a NaN with a non-default payload), 14 results (every OpOutput and every Failure case) and 14 hashes (one per CommandKind, all distinct).
- **Removed** the `use postcard as _; use sha2 as _;` anchors that 03-01 added.

## Verification

- `cargo test -p ostro-types --test journal_golden`: all 10 tests pass (3 golden, hash semantics, canonical layout, decode_rejects and 4 proptests). The full `cargo test -p ostro-types` run also passes.
- After a second run without `OSTRO_BLESS`, `git diff --exit-code crates/ostro-types/tests/golden/` is clean.
- `cargo clippy -p ostro-types --all-targets -- -D warnings` is clean, as is `cargo fmt -p ostro-types --check`.
- TDD order: RED bd1ea5e (did not compile because the API was missing; the golden file had only its header), then GREEN 133901a.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Clippy `manual_is_multiple_of` in the test hex helper**
- Fixed with `is_multiple_of(2)` before the GREEN commit (133901a).

**2. [Rule 2 - Missing] Added beyond the plan's API list**
- `command_args(&EffectRequest)`: the plan's test needs "the builder used by callers takes names only".
- `EventKind::from_code`.
- Extra tests: `canonical_args_layout_is_hand_specified`, and `prop_sql_hash_nan_canonical`, which checks that any two NaNs hash equal while any other differing bit patterns hash differently.

No refactor commit was needed.

### Concurrency notes
- Other executors had uncommitted work in the tree: ostro-engine, ostro-log, ostro-partition, ostro-testkit, Cargo.toml/lock and nextest.toml. Verification was scoped to `-p ostro-types`. I staged only my three files.

## Known Stubs

None.

## Commits

- bd1ea5e test(03-02): add failing golden + property tests for journal codec and command hash
- 133901a feat(03-02): journal event/result codec and command hash v1 with golden vectors

## Self-Check: PASSED
