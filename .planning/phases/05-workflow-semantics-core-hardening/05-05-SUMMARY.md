---
phase: 05-workflow-semantics-core-hardening
plan: 05
subsystem: wit
tags: [wit, freeze, witcheck, semver-check, wit-parser, golden, D14, WIT-07]

requires:
  - phase: 05-workflow-semantics-core-hardening
    provides: "05-01 contract and appended goldens (21 command vectors, 25 journal kinds ending in 97 signal_matched)"
provides:
  - "crates/ostro-witcheck: check_additive(prev_dir, new_dir) = wit_component::semver_check + wit-parser structural diff; check_chain(wit_root) for ostro-runtime-0.1.N"
  - "8 WIT fixtures (2 additive, 6 breaking) generated from runtime.wit, bless-checked"
  - "wit_freeze.rs: frozen_0_1_0_is_byte_identical (pre-freeze report mode; enforces once wit/FROZEN.toml exists) + 0.1.N chain test"
  - "ostro-types/tests/golden_frozen.rs: append-only (line_count, sha256) prefix pins"
  - "docs/wit-0.1-freeze-review.md (D14, USER REVIEW) + doc-comment-only runtime.wit semantics (Phase 5, P6 W1-W8, P7 F4, P9 PF-W1..W9 + PF-F3, P14 F10)"
affects: [05-06, 05-17, 05-19, 05-20, phase-6, phase-7, phase-9, phase-14]

tech-stack:
  added: ["wit-component =0.261.0 (feature semver-check)", "wit-parser =0.261.0", "wat 1.261.0 / wast 261.0.0 (lock-pinned, transitive)", "unicode-width 0.2.3 (transitive)"]
  patterns:
    - "Two-checker additivity: semver_check (wasmparser type-check) AND a readable structural diff; both must accept"
    - "Fixtures generated from the real WIT by an anchored edit table; OSTRO_BLESS=1 rewrites, the test fails on drift"
    - "Sectioned golden prefix pins (journal_v1.txt event/result/hash sections pinned separately)"

key-files:
  created:
    - crates/ostro-witcheck/Cargo.toml
    - crates/ostro-witcheck/src/lib.rs
    - crates/ostro-witcheck/tests/semver_fixtures.rs
    - crates/ostro-witcheck/tests/wit_freeze.rs
    - crates/ostro-witcheck/tests/wit-fixtures/{base,add-func-context,add-interface-imported,add-record-field,add-variant-case,change-param-type,remove-func,rename-interface,add-workflows-export}/runtime.wit
    - crates/ostro-types/tests/golden_frozen.rs
    - docs/wit-0.1-freeze-review.md
  modified:
    - Cargo.toml
    - Cargo.lock
    - deny.toml
    - wit/ostro-runtime-0.1.0/runtime.wit

key-decisions:
  - "check_additive requires BOTH semver_check and the structural diff to accept; the structural diff supplies readable messages and the version/package-name rules"
  - "wat/wast lock-pinned to 1.261.0/261.0.0 (cargo picked 1.263, which duplicated wasm-encoder/wasmparser); deny.toml now bans multiple versions of wit-component, wit-parser, wasmparser and wasm-encoder"
  - "journal_v1.txt is pinned per section (header/event/result/hash) because new kinds land at the end of the event section, mid-file"
  - "Same package name+version in one Resolve panics in wit-parser; the semver leg is skipped (LoadFailed) and the structural leg rejects the non-increasing version"
  - "D14 outcome recorded: no record/variant/enum change; every candidate has a named 0.1.N additive path (USER REVIEW)"

requirements-completed: []

duration: ~75min
completed: 2026-11-19
---

# Phase 5 Plan 05: WIT freeze tooling, pins and pre-freeze review

**`ostro-witcheck` proves an `ostro:runtime@0.1.N` package is additive. It runs `wit_component::semver_check` together with a wit-parser structural diff, validated by 2 positive and 6 negative fixtures. The WIT byte-identity pin is in pre-freeze mode and the golden prefix pins are enforcing. `runtime.wit` gained only doc comments, which cover every Phase 5/6/7/9/14 semantics item the plan lists.**

## Performance

- **Base commit:** `8dee57950b3ae39ffb5da253f7bac967c8c2964c` (written to `target/05-05-base.sha` before any change).
- **Started:** 2026-11-19T06:43Z.
- **Tasks:** 3/3.
- **Files:** 4 modified, 16 created (9 of them fixtures).

## Task commits

1. **Task 1, ostro-witcheck and fixtures:** `48512ca`.
2. **Task 2, byte-identity and golden prefix pins:** `c695f1b`.
3. **Task 3, D14 review and WIT doc comments (+ fixture re-bless):** `0e8cf0c`.

## semver_check spike result

**semver_check works directly; the fallback was not needed to load packages.** wit-component 0.261.0 `semver_check` loads `base` (0.1.0) and each 0.1.1 candidate into **one** `Resolve` via `push_dir` + `select_world(&[pkg], Some("workflow"))` with no trouble; the 2024 two-versions report does not reproduce. The one failure mode found: pushing the **same** name and version twice makes wit-parser panic. That case is guarded by comparing package names first.

The structural diff was built anyway, as plan and fallback both specify, and `check_additive` requires both checkers to accept. Which checker catches which class (pinned by `semver_check_classes`, printed by `fixture_table_verdicts`):

| Fixture | Expected | semver_check | structural diff (message) |
|---|---|---|---|
| add-func-context | additive | accept | accept |
| add-interface-imported | additive | accept | accept |
| add-record-field | breaking | REJECT | REJECT: `type types.run-info changed shape` |
| add-variant-case | breaking | REJECT | REJECT: `type types.failure changed shape` |
| change-param-type | breaking | REJECT | REJECT: `function effects.sleep changed signature` |
| remove-func | breaking | REJECT | REJECT: `function context.is-replaying removed or renamed` |
| rename-interface | breaking | REJECT | REJECT: `interface log removed or renamed` + world import removed |
| add-workflows-export | breaking | REJECT | REJECT: `exported interface workflows gained function query` |

Both checkers catch every negative class. semver_check's messages are raw wasmparser errors, so the structural diff supplies the readable list, plus two rules semver_check ignores by design: same package name, and a semver-compatible increasing version (0.1.N, N strictly greater).

## Pins

- Current `runtime.wit` sha256 (after the doc comments): `3aaa4253a284909e06bbbb037a7d358ff3ee65d8810ba0adbbe34d6945fc0c86`. The test prints it; 05-20 records it in `wit/FROZEN.toml`.
- Golden prefix pins (post-05-01 contents):

| File | Section | Lines | Note |
|---|---|---|---|
| commands.txt | all | 22 | header + 21 vectors |
| journal_v1.txt | `#` | 1 | header |
| journal_v1.txt | `event` | 27 | **25 kinds but 27 lines**: kind 18 has three vectors (http/sql/object); last line `event 97 signal_matched` |
| journal_v1.txt | `result` | 14 | |
| journal_v1.txt | `hash` | 14 | |
| routing.txt | all | 49 | header + 48 vectors |

## WIT doc-comment items and locations

All are in `wit/ostro-runtime-0.1.0/runtime.wit`; line numbers are at `0e8cf0c`. Each has outcome "doc comment only" and a row in `docs/wit-0.1-freeze-review.md`.

| Item | Location (line) |
|---|---|
| File header: byte-pin, 0.1.N path, link to the review | header `//` comment, L6-8 |
| **Phase 5** FIFO signals, buffering, oldest-op first, timeout, always evicts / never hot-parked | `effects.wait-signal`, L368-373 |
| **Phase 5** cancel-op → `cancelled` unless already completed (D5) | `effects.cancel-op`, L398-401 |
| **Phase 5** patched memo + unqueried markers ignored (D6) | `context.patched`, L157-163 |
| **Phase 5** step-local RNG (D7) | `context.random-bytes`, L145-150; `context` interface doc, L127-129 |
| **Phase 5** await token one-shot, timeout, always evicts | `effects.create-await-token`, L388-391; `effects.await-token`, L331-334 |
| **Phase 5** `run-info.deadline` = creation + run timeout, hard stop (D2) | `types.run-info.deadline`, L116-118 |
| **Phase 5** `run-info.module-hash` | L109-110 |
| **D11** SDK cancellation surfacing is SDK policy | `effects.cancellation`, L395-396 |
| **P6 W1** secret templating, no redirects, origin scope, `[REDACTED]` | `effects.header`, L222-231 (+ `http-request` pointer, L236) |
| **P6 W2** derived `Idempotency-Key` | `effects.http-request.idempotency-key`, L244-248 |
| **P6 W3** redact applies to responses too | `effects.http-request.redact`, L254-257 |
| **P6 W4** per-attempt timeout, retryable conditions, `effect_outcome_unknown` | `http-request.timeout-ms`, L242-243; `http-request.retry`, L249-253 |
| **P6 W5** any status is success, `response_too_large` | `effects.http-response`, L260-264 |
| **P6 W6** exactly-once SQL, `sql_not_transactional`, at-least-once when false | `effects.sql-request.exactly-once`, L289-294 |
| **P6 W7** object op values and encodings, presign expiry, `unsupported_op` | `effects.object-request.op`, L308-313 |
| **P6 W8** reserved error kinds (+ Phase 5 kinds and the Phase 9 reserved names) | `types.error-info`, L44-51 |
| **P7 F4** `attempt` not replay-stable; logging/metrics only, never for control flow | `types.run-info.attempt`, L105-108; record doc, L97-98 |
| **P9 PF-W1** open-step rule | `journal` interface doc, L173-178; `journal.step`, L194 |
| **P9 PF-W2** `wait` semantics, `WaitResolved` | `effects.wait`, L402-413 |
| **P9 PF-W3** opaque op ids, 2^64-1, reserved range, `bigint` | `types.op-id`, L28-34; `effects.cancellation`, L395 |
| **P9 PF-W4** UTF-8 scalar values, lone surrogates, `invalid_string` | `types` interface doc, L14-17 |
| **P9 PF-W5** JSON payload convention | `types.payload`, L19-23 |
| **P9 PF-W6** retry none, max-attempts, coefficient clamp, fuel portability, `random-bytes` ≤ 65536 | `types.retry-policy`, L69-74; `types.step-options`, L82-94; `context.random-bytes`, L142-144 |
| **P9 PF-W7** child contract (outcome mapping, CAN chain, cancel cascade, detached, deterministic ids) | `effects.start-child`, L374-387; `child-options.detached`, L322-327 |
| **P9 PF-F3** attached children get a graceful cancel at any parent terminal status | `effects.start-child`, L384-386 |
| **P9 PF-W8** fresh instance, trap-ends-activation, no imports at pre-init, `describe` pure/unique | `workflows` interface doc, L432-437; `workflows.describe`, L464 |
| **P9 PF-W9** uuid-v7 8-4-4-4-12, stream sharing | `context.uuid-v7`, L151-154; `context.random-u64`, L137-139 |
| **P14 F10** `run-info.tenant` (none = default, immutable, inherited) | L111-113 |
| **P14 F10** unqualified names (+ grammar) | `run-info.workflow`, L101-103; `workflow-meta.name`, L442-443; world doc, L472-473 |
| **P14 F10** in-tenant resolution, no cross-tenant address | world doc, L473-474; `start-child`, `continuation`, `sql-request`, `object-request`, `header` |
| **P14 F10** quotas delay, never fail | world doc, L474-475 |
| `log.emit` allowed in an open step | `log` interface doc, L417 |

The D14 shape check: with comment lines removed, the file is byte-identical to the base (`diff` of `grep -v '^\s*//'` on both sides is empty, a stricter check than the plan's verify, which passes as well).

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] wat/wast resolved to 1.263, duplicating wasm-encoder/wasmparser**
- **Found during:** Task 1.
- **Issue:** `wit-component`'s `semver-check` feature pulls `wat ^1.261.0`; Cargo chose 1.263.0, which brought wasm-encoder/wasmparser 0.263 next to wasmtime's 0.261.
- **Fix:** `cargo update -p wat --precise 1.261.0`, which also brought wast to 261.0.0. Added `deny-multiple-versions` bans for wit-component, wit-parser, wasmparser and wasm-encoder, so a future `cargo update` that splits them fails `cargo deny`. The new unavoidable `unicode-width` 0.1/0.2 duplicate (warn-level) is noted in deny.toml.
- **Commit:** `48512ca`.

**2. [Rule 1 - Bug] wit-parser panics on the same package version pushed twice**
- **Found during:** Task 1.
- **Fix:** `check_additive_report` compares package names (with versions) first and skips the semver leg when they are equal. Test: `same_version_is_rejected_without_panicking`.
- **Commit:** `48512ca`.

**3. [Plan detail] journal_v1.txt pinned per section, not as one file prefix**
- **Issue:** the file is rendered as header, then events, results, hashes, so an appended kind lands mid-file and a whole-file prefix pin would reject a legitimate append.
- **Fix:** one pin per section. The event section has 27 lines (25 kinds), not the "25 lines" in the hand-off.

**Total deviations:** 3, all inside plan-owned files. **Impact:** none on scope.

## Issues Encountered

Fixtures are generated in-test. The first `OSTRO_BLESS=1` run raced the other tests in the same binary, so the bless instruction now filters to `fixtures_are_generated`.

## Hand-off notes for 05-20 (freeze flip)

1. **Create `wit/FROZEN.toml`:**
   ```toml
   [ostro-runtime-0_1_0]
   sha256 = "<sha256sum wit/ostro-runtime-0.1.0/runtime.wit>"
   ```
   The value is `3aaa4253…0c86` at `0e8cf0c` if nothing changes before 05-20. `cargo test -p ostro-witcheck --test wit_freeze -- --nocapture` prints the current value. Once the file exists, `frozen_0_1_0_is_byte_identical` enforces it.
2. **Re-pin `crates/ostro-types/tests/golden_frozen.rs`** to the final line counts and hashes (`grep '^event ' journal_v1.txt | sha256sum` etc.; whole-file `sha256sum` for commands.txt and routing.txt). Later Phase 5 plans (05-08/05-09/05-22…) may append vectors, and the current pins allow that.
3. **If runtime.wit doc comments change before the freeze** (a user override of D14/D7), run `OSTRO_BLESS=1 cargo test -p ostro-witcheck --test semver_fixtures -- fixtures_are_generated`, because `base/` must equal the real file.
4. Tag `wit/ostro-runtime-v0.1.0` locally (RESERVATIONS §11).
5. `docs/wit-0.1-freeze-review.md` is marked "adopted default, USER REVIEW". 05-20 records the user's decision or keeps it as the default.
6. Post-freeze, a new package goes into `wit/ostro-runtime-0.1.N/` (version `0.1.N` in the `package` line); `runtime_0_1_chain_is_additive` checks it automatically. No ci-local edit is needed: the tests run in `cargo test --workspace`.
7. **D17:** a Wasmtime 51 bump must move `wit-component`/`wit-parser` (workspace pins) and re-pin `wat`/`wast` to match. The deny bans catch a split.

## Notes for STATE.md (owned by 05-07; for the orchestrator)

- Decision: WIT additivity = semver_check AND structural diff (05-05).
- D14 review recorded in `docs/wit-0.1-freeze-review.md` (USER REVIEW item still open).
- WIT-07 stays **Pending** until 05-20 creates FROZEN.toml; the tooling is complete.

## Next Phase Readiness

- 05-06 (SDK README) can mirror the `attempt` (F4) and D11 wording from runtime.wit.
- 05-20 only creates FROZEN.toml, re-pins and tags.

## Gate

`bash scripts/ci-local.sh --no-musl`, run in a detached worktree at `0e8cf0c` with `CARGO_TARGET_DIR=target/wt-05-05`, so other plans' uncommitted edits were excluded:

```
toolchain 1.100.0                    PASS  0s
cargo fmt --check                    PASS  0s
cargo clippy -D warnings             PASS  32s
cargo build (warning-free)           PASS  54s
sdk workspace fmt + clippy           PASS  2s
sdk workspace test + wasm build      PASS  22s
guests build (wasm32-wasip2)         PASS  11s
cargo test                           FAIL  584s   (ostro-partition shuffler_exactly_once only)
failpoint suites                     FAIL  463s   (ostro-partition rebuild_digest only)
cargo deny check                     PASS  1s
check-vendored.sh                    PASS  1s
check-deny-fixtures.sh               PASS  1s
no chronik/tokio-uring/prometheus    PASS  0s
workflow validation                  PASS  0s
```

**Both failures are the same timing flake, outside this plan's files.** `crates/ostro-partition/tests/common/mod.rs:243` panics with `node open timed out: Elapsed(())`: a fixed timeout `T` around `Node::open`. During the run, other wave-2 agents were compiling in parallel, and the load average was 15-24 on 12 cores. Isolated re-runs:

| Test | Result |
|---|---|
| `rebuild_digest` (`--features failpoints --test-threads=1`) | **PASS** (3/3, 282s) |
| `shuffler_exactly_once` run 1 | `exactly_once_heavy_faults` and `exactly_once_under_flaky_acks_and_restarts` FAIL (node open timed out) |
| `shuffler_exactly_once` run 2 (`--test-threads=1`) | `exactly_once_heavy_faults` PASS; `exactly_once_under_flaky_acks_and_restarts` FAIL (node open timed out, load 18-24) |

05-05 does not touch ostro-partition. The only plausible interactions are the new deny bans, which are build-time only, and Cargo.lock, whose only additions are wat, wast, unicode-width and ostro-witcheck, none of them used by ostro-partition. The failure is a timeout, not an assertion. **Follow-up for the orchestrator / 05-02:** re-run `cargo test -p ostro-partition --test shuffler_exactly_once` on a quiet machine. If it still times out there, 05-02's partition changes (`811c89d`/`8dee579` are in this tree) slowed `Node::open`.

Plan-level checks, all green: `cargo test -p ostro-witcheck -p ostro-types --locked`, `cargo deny check`, `cargo tree -d -p ostro-witcheck` (no duplicates), the Task 3 verify command, the engine build and clippy, and the SDK wasm build and clippy.

## Self-Check: PASSED

All created files are present at HEAD, and commits 48512ca, c695f1b and 0e8cf0c are ancestors of HEAD.
