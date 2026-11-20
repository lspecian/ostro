---
phase: 06-effects-bridge-connectors-secrets
plan: 02
subsystem: effects-bridge
tags: [contract, dependencies, skeleton, format, goldens]
requires:
  - "06-01 Phase-6 gate (6984651) and decisions D-FX1..9, P-1..21"
  - "08-01 landed first (40047c3, bab9ed6, d4d150d): ostro-objstore skeleton, rusty-s3/url/futures pins, InvalidCode placeholders 13..21"
provides:
  - "Every Phase 6 workspace pin and member dependency (the only Phase 6 plan touching Cargo files)"
  - "ostro-bridge full module tree: contract files kinds/connector/meta_view/origin/failpoints, QuotaSpec, SecretProvider; stubs for 06-04/05/07/10/11/12/13"
  - "ostro-testkit features mock-http / db / canary (stubs for 06-10/06-11/06-19)"
  - "Cmd::CompleteEffect (13), Intent::RegisterEffect (9) / CancelEffect (10), InvalidCode::BadEffectCompletion (15), ostro_types::live"
  - "command::complete_effect_payload (B2)"
affects: [06-03, 06-04, 06-05, 06-06, 06-07, 06-10, 06-11, 06-12, 06-13, 06-14, 06-16, 06-19, 08-03]
tech-stack:
  added: [sqlx 0.9.1, hickory-resolver 0.26.3, ipnet, aws-lc-rs (direct), zeroize, secrecy 0.10.3, governor 0.10.4, aho-corasick, vaultrs 0.8.0, kube 5.1.0, k8s-openapi 0.29.0, base64 (direct)]
  patterns:
    - "dep anchors (`mod dep_anchors { use x as _; }`) keep cargo 1.100's unused-dependency lint quiet for deps declared ahead of their owning plan (same pattern as 08-01)"
    - "InvalidCode placeholder renamed in place (P-15)"
key-files:
  created:
    - crates/ostro-types/src/live.rs
    - crates/ostro-bridge/src/{kinds,connector,meta_view,origin,failpoints,ratelimit}.rs
    - crates/ostro-bridge/src/secrets/providers/mod.rs
    - crates/ostro-bridge/src/{driver,registry,outcome,config}.rs (stubs)
    - crates/ostro-bridge/src/{egress,http,sql,object,secrets}/mod.rs (stubs)
    - crates/ostro-bridge/src/secrets/providers/{sealed,env,file,chain,cache,vault,k8s}.rs (stubs)
    - crates/ostro-testkit/src/{mock_provider,db,canary}.rs (stubs)
  modified:
    - Cargo.toml
    - Cargo.lock
    - crates/ostro-bridge/Cargo.toml
    - crates/ostro-bridge/src/lib.rs
    - crates/ostro-testkit/Cargo.toml
    - crates/ostro-testkit/src/lib.rs
    - crates/ostro-server/Cargo.toml
    - crates/ostro-server/src/lib.rs
    - scripts/ci-local.sh
    - .github/workflows/ci.yml
    - crates/ostro-types/src/{lib,command}.rs
    - crates/ostro-types/tests/command.rs
    - crates/ostro-types/tests/golden/commands.txt
    - crates/ostro-partition/src/apply/{mod,outcome,run}.rs
    - crates/ostro-partition/examples/gen_format_corpus.rs
key-decisions:
  - "ostro-objstore is owned by Phase 8: 08-01 created it (40047c3) before 06-02 ran; 06-02 left it untouched. 08-03 defines the trait; 06-12 adds presign/put_with/PutOpts/PresignOpts with default impls"
  - "BadEffectCompletion takes index 15 by renaming 08-01's never-emitted ReservedP6_15 placeholder in place (P-15); ReservedP5_13/14 already existed"
  - "governor pinned with default-features = false, features = [\"std\"] at workspace level (a member cannot turn off a workspace dep's defaults without a cargo warning)"
  - "aws-lc-rs pinned with rustls' exact feature set (aws-lc-sys + prebuilt-nasm): still one aws-lc-sys build"
  - "Golden base is d4d150d (main after 08-01), not 2718a6d: the plan was rebased onto 08-01 before the first commit"
metrics:
  duration: "~75 min (incl. one rebase onto 08-01 and ci-local)"
  completed: 2026-11-20
---

# Phase 6 Plan 02: Dependencies, Skeletons and Contract Summary

**Every Phase 6 dependency is pinned and declared. ostro-bridge has its full module tree: the contract files (W8 kinds, `Connector`/`SendGate`/`CompletionSink`, `MetaView`, `Origin`/`OriginPattern`, `bridge_fail!`, `QuotaSpec`, `SecretProvider`) plus owner-tagged stubs. ostro-testkit has the mock-http/db/canary features, and `ostro-bridge` is in the failpoint suites. On the format side, ostro-types gains `Cmd::CompleteEffect` (13, whose outcome is the encoded `EffectCompleted` event, B2), `Intent::RegisterEffect`/`CancelEffect` (9/10) and the never-journaled `live` module; `InvalidCode::BadEffectCompletion` sits at index 15. Goldens are append-only. ostro-objstore was already created by Phase 8 (08-01), so it was left alone.**

## Base commit

- `target/06-02-base.sha` = **`d4d150df7a2aefda99517a5a912f7cc3922a6db5`**. Later plans should use this value for golden diffs.
- The plan started on `2718a6d`. 08-01 then landed three commits (`40047c3`, `bab9ed6`, `d4d150d`), so the work was rebased onto `d4d150d` before the first 06-02 commit, and the base SHA was reset to match.

## ostro-objstore owner

**Created by Phase 8 (08-01, commit `40047c3`).** It is a skeleton: `lib.rs` holds only dependency anchors, and 08-03 owns the trait. 06-02 skipped its objstore block and did not touch the crate. ostro-bridge depends on it by path.

The Phase 6 trait additions (`put_with`/`PutOpts`, `presign`/`PresignMethod`/`PresignOpts`, all with default impls) now fall to **06-12**, layered on top of 08-03's signatures.

## `complete_effect_payload` signature

```rust
// ostro_types::command
pub fn complete_effect_payload(
    op: u64,
    outcome: &crate::journal::RecordedResult,
    completed_at_nanos: u64,
) -> Vec<u8>
// = encode_event(&Event{ seq: 0 (not encoded), ts_nanos: completed_at_nanos,
//                       body: EventBody::EffectCompleted(EffectCompleted{op, outcome}) }).1
```

`CompleteEffect{run_id: bytes 1, op: u64 2, outcome: Option<PayloadRef> 3, completed_at_ms: u64 4}`. Apply (06-03) stores the outcome as `JournalEvent{seq: next_seq, kind: 65, payload: outcome}` verbatim and never decodes it. There is no codec field.

The test `complete_effect_payload_is_the_encoded_effect_completed_event` checks that `decode_event(any seq, 65, bytes)` gives `EffectCompleted{op, outcome}` with `ts == completed_at`, and that the bytes equal `encode_event(..).1`.

Other contract items:
- `Derived::register_effect(op, kind: u32, scheduled_at_ms)` and `Derived::cancel_effect(op)`.
- `ostro_types::live::{EffectKind{Http=0,Sql=1,Object=2}, EffectRetry, LiveEffect}`. `LiveEffect` has a manual `Debug` that prints header names but `"<value>"` for values, body lengths only, SQL param counts only and a redacted idempotency key. Canary tests cover it, and nothing in `live` derives Serialize.

## Goldens

- Three lines are appended to `commands.txt` **after 08-01's three `schedule_fire_*` lines**: `complete_effect_inline` (payload = `complete_effect_payload(12, Ok(Http 201), 9_100 ms)`), `complete_effect_blob` and `events_appended_effects` (RegisterEffect ×2 + CancelEffect).
- `git diff d4d150d -- commands.txt` contains only `+` lines. `golden_frozen` is green, and its 22-line pin is unchanged (not extended).
- `old_bytes_decode_unchanged_after_phase6` re-decodes and re-encodes every pre-06-02 line, including 08-01's, byte-identically.
- `EVENT_CODEC_VER` is still 1.

## Temporary reject arms (owner: 06-03)

| Site | Arm | Until |
|------|-----|-------|
| `ostro-partition/src/apply/mod.rs` | `Cmd::CompleteEffect(_)` → `Rejected(Invalid(NotImplemented))` | 06-03 |
| `ostro-partition/src/apply/run.rs` `check_intents` | `Intent::RegisterEffect(_) \| Intent::CancelEffect(_)` → `Invalid(BadIntent)` | 06-03 |
| `ostro-partition/src/apply/run.rs` apply loop | `Intent::RegisterEffect(_) \| Intent::CancelEffect(_) => {}` (unreachable: `check_intents` rejects first) | 06-03 |

Each arm is commented `// Phase 6: implemented in 06-03` (or `rejected by check_intents until 06-03`).

## Dependency / duplicate report

- **Workspace pins added:**
  - sqlx `=0.9.1`: no default features; runtime-tokio, tls-rustls-aws-lc-rs-native-roots, postgres, mysql, json, time, bigdecimal, uuid.
  - hickory-resolver 0.26.3: tokio, system-config.
  - ipnet 2.12.2.
  - aws-lc-rs 1.19: `aws-lc-sys` + `prebuilt-nasm`, which matches what rustls enables, so there is still one aws-lc-sys build.
  - zeroize 1.9.2 (derive), secrecy 0.10.3, governor 0.10.4 (std only), aho-corasick 1.
  - vaultrs 0.8.0 (rustls).
  - kube 5.1.0 (client, rustls-tls, aws-lc-rs) and k8s-openapi 0.29.0 (v1_33).
  - base64 0.22 (= the locked 0.22.1).
- **Reused 08-01 pins:** rusty-s3 `=0.12.0`, url 2.5.8, futures-core/futures-util 0.3.34.
- **Unchanged:** the reqwest workspace features (only its comment changed) and deny.toml.
- **Cargo.lock:** 98 new package versions.
- **Normal graph** (`cargo tree --workspace -e normal --locked > target/06-02-tree.txt`):
  - no `chrono`;
  - no libsqlite3-sys, sqlx-macros or sqlx-sqlite (they appear in Cargo.lock only as unactivated optional deps);
  - no webpki-roots, openssl-sys, native-tls or rsa.
- **`cargo deny check`:** `advisories ok, bans ok, licenses ok, sources ok` on the stock config.
- **New duplicate warnings** (warn-level only; none is a deny-listed crate):
  - thiserror 1.0.69 / 2.0.21 (+ thiserror-impl), via vaultrs → rustify. Expected.
  - syn 1.0.109 (third copy alongside 2.x/3.x) and synstructure 0.12/0.14, via rustify_derive.
  - core-foundation 0.9/0.10 and windows-result/windows-strings: target-specific (hickory system-config).
  - object 0.39/0.40.
- **Duplicates already present at d4d150d:** jiff 0.2/1.0 (08-01's jiff02 and croner; kube's jiff 0.2 unifies with it) and tower-http 0.6/0.7 (reqwest).
- **No new duplicate** of tonic, tonic-prost, prost, prost-types, hyper, http, axum, opentelemetry or wasmtime.
- **kube/k8s-openapi build cost:** after `cargo clean -p k8s-openapi -p kube -p kube-client -p kube-core` (357 MiB of debug artifacts), rebuilding them plus ostro-bridge takes about 10 s wall time on this machine (debug, parallel). Building with `--no-default-features` drops them, and also vaultrs, from the bridge.

## Verification

- **Task 2 verify:**
  - `cargo test -p ostro-types --locked`: every binary ok; `command` has 40 passed.
  - `golden_frozen`: ok.
  - Golden diff has no `-` lines; `EVENT_CODEC_VER: u8 = 1`.
  - `cargo test -p ostro-partition --locked`: 258 passed, 0 failed.
  - Workspace build and clippy `-D warnings`: 0 warnings.
- **Task 1 verify:**
  - workspace build `--locked`, `-p ostro-bridge --no-default-features`, the `--features failpoints` and `--all-features` builds: 0 warnings;
  - `cargo test -p ostro-bridge --lib --locked`: 13 passed;
  - `cargo test -p ostro-bridge --features failpoints --locked -- --test-threads=1`: 13 passed;
  - clippy `-p ostro-bridge -p ostro-objstore -p ostro-testkit --all-features --all-targets -D warnings`, clippy on the bridge `--no-default-features`, clippy on `-p ostro-server -p ostro --features failpoints`, and workspace clippy: all clean;
  - `cargo deny check`: ok; no chrono in the tree;
  - both `grep`s for `ostro-bridge` in ci-local.sh and ci.yml pass.
- **WIT:** `git diff --stat d4d150d -- wit/` is empty.
- **Gate:** `bash scripts/ci-local.sh --no-musl` at 6c9db25 (worktree, `CARGO_TARGET_DIR=target/wt-06-02`): **all steps passed**. Timings: clippy 12s, build 27s, cargo test 629s, semantics sleepers 73s, failpoint list sync PASS (now includes ostro-bridge), failpoint suites 861s, server campaign smoke (20 kills) 408s, cargo deny / vendored / deny fixtures / forbidden crates / workflow validation PASS; musl steps SKIP (--no-musl).

## Deviations from Plan

### Auto-fixed issues

**1. [Rule 3 - Blocking] Rebased onto 08-01 (landed mid-plan)**
- **Found during:** Task 1 (before the first commit).
- **Issue:** 08-01 committed `ostro-objstore`, rusty-s3/url/futures pins, InvalidCode placeholders 13..21, `Cmd::ScheduleFire` (27) and three golden lines.
- **Fix:**
  - Re-applied both tasks on `d4d150d` and reset the base SHA.
  - Dropped the duplicate rusty-s3/url/futures pins and skipped the objstore block.
  - Renamed `ReservedP6_15` → `BadEffectCompletion` instead of declaring `ReservedP5_13/14` (already present).
  - Merged command.rs doc/oneof lines so both phases' tags (13 and 27) are listed.
  - Appended the Phase 6 goldens after the Phase 8 lines.

**2. [Rule 3 - Blocking] 08-01's `phase8_golden_lines_are_appended_last` asserted the file ends at 24 lines**
- **Fix:** it now asserts `len >= 24` and checks the `21..24` window. The intent is unchanged: the Phase 8 lines follow the Phase 5 lines. A new `phase6_golden_lines_follow_phase8` test checks `24..27`. Edited in `crates/ostro-types/tests/command.rs` (commit 3ca83b2).

**3. [Rule 3 - Blocking] cargo 1.100 `unused dependency` manifest warnings**
- **Issue:** the build must be warning-free, but deps declared ahead of their owning plan trigger cargo 1.100's `unused dependency` manifest warnings.
- **Fix:** added private `dep_anchors` modules, the same pattern 08-01 used:
  - in `ostro-bridge/src/lib.rs` (besides the `pub mod` list the plan prescribes);
  - in `ostro-testkit/src/lib.rs`, cfg'd per feature;
  - in `ostro-server/src/lib.rs`, a one-module addition outside files_modified.

**4. [Wave rule] One-line compile arms outside files_modified**
- `apply/run.rs` (the two arms above).
- `examples/gen_format_corpus.rs` (intent names).

**5. Task order**
- Task 2 (types contract) was committed before Task 1, because `connector.rs` needs `ostro_types::live`. Each commit builds on its own.

**6. ostro/Cargo.toml unchanged**
- The binary's `failpoints` already forwards to `ostro-server/failpoints`, which now includes `ostro-bridge/failpoints`.
- `ostro-server` was already in FAILPOINT_CRATES (`:failpoints_phase5`) and `ostro` in FAILPOINT_LINT_ONLY, so only `ostro-bridge` was added.

### Not done here (outside this plan's files)

- RESERVATIONS.md rows for Cmd 13 / Intent 9–10 / InvalidCode 15 still read `planned`. 08-01 flipped its own rows in a docs commit. A docs-only follow-up, or 06-03, should flip the P6 rows to `exists`.

## Commits

| Task | Commit | Description |
|------|--------|-------------|
| 2 | 3ca83b2 | feat(06-02): Phase 6 format contract: Cmd::CompleteEffect (13), Intent RegisterEffect/CancelEffect (9/10), InvalidCode 15 |
| 1 | 6c9db25 | chore(06-02): declare every Phase 6 dependency, ostro-bridge contract skeleton, testkit features, failpoint chain |

## Self-Check: PASSED

- FOUND: crates/ostro-types/src/live.rs, crates/ostro-bridge/src/{connector,kinds,meta_view,origin,failpoints,ratelimit}.rs, crates/ostro-bridge/src/secrets/providers/mod.rs, crates/ostro-testkit/src/{mock_provider,db,canary}.rs
- FOUND: commits 3ca83b2 and 6c9db25 on main
