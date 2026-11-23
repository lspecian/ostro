---
phase: 10-developer-cli-test-kits
verified: 2026-11-23T06:21:47Z
status: human_needed
score: 4/4 success criteria verified (7/7 requirements satisfied; remaining items are by-design human/user items)
human_verification:
  - test: "USER DECISION D-P10-13/14: dev-mode module replacement (a) dev-only server replace/auto-number, (b) watch rewrites versions in a staged copy, (c) keep + document (current)"
    expected: "A choice is recorded. Today, a save that changes code without bumping the workflow version rebuilds, gets a 409 with a bump hint, and keeps the active version."
    why_human: "Product/UX policy. The current behaviour follows the write-once (name, version) invariant and is documented in getting-started 'Changing a workflow'."
  - test: "USER DECISION / follow-up plan: fsync-free ephemeral mode for the macOS and disk-fallback `ostro dev --ephemeral` 1 s startup budget"
    expected: "An approach is chosen and planned. Today macOS measures 2.7-3.4 s, and Linux with /dev/shm < 512 MiB uses the disk temp dir."
    why_human: "Design decision across ostro-log/partition/meta/blob. The budget is CLI-01 (Phase 4), not a Phase 10 requirement."
  - test: "USER REVIEW D-P10-6: auto-skip trigger (idle + >=1 result waiter + 20 ms quiescence re-check; leases stay real time)"
    expected: "Accepted or amended"
    why_human: "Flagged for user review in 10-DECISIONS"
  - test: "USER REVIEW D-P10-11: crate-scoped notify CC0-1.0 exception in deny.toml"
    expected: "Accepted"
    why_human: "Licence policy decision"
  - test: "macOS clean run: `bash scripts/clean-machine.sh all --local` (and the --docker variant) on a Mac without Xcode CLT; record timings and the ephemeral startup time"
    expected: "3 CLEAN lines, each under 600 s"
    why_human: "Needs macOS hardware"
  - test: "Remote CI: push, then confirm the ci.yml `polyglot` (incl. clients) and `clean-machine` jobs are green (6 CLEAN lines)"
    expected: "Green jobs"
    why_human: "Needs a push to the remote and GitHub runners; the verifier does not push"
  - test: "Publishing: ostro-client to PyPI/npm/crates.io, Go module tag clients/go/v0.1.0, ostro-build image, Rust SDK crates (commands in 10-20-SUMMARY)"
    expected: "Packages published; the template path deps switch to crates.io"
    why_human: "Credentials and an irreversible external action"
---

# Phase 10: Developer CLI & Test Kits Verification Report

**Phase Goal:** A developer goes from an empty directory to a deployed, running workflow in any of the three languages with one toolchain-free CLI, and can test workflows locally with time skipping.
**Verified:** 2026-11-23T06:21:47Z at main 57b96be
**Status:** human_needed. Every automated check passes. The remaining items are by-design: user decisions/reviews, macOS hardware, remote CI and publishing.
**Re-verification:** No. This is the initial verification.

## Goal Achievement

### Observable Truths (ROADMAP Success Criteria)

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | In Rust, Python and TS, `ostro init → deploy → run` works on a clean machine with no pre-installed toolchain (hermetic or `--docker`) in < 10 min | ✓ VERIFIED (Linux local mode). Container runs were done locally; macOS and remote CI are human_needed | `crates/ostro/tests/clean_machine.rs::local_all` drives `scripts/clean-machine.sh`, which fails any language over 600 s (line 138). 10-20 results: rust 41.3 s, python 13.2 s, ts 16.7 s, with no toolchain on PATH and a fresh cache. Rust used rustup-init plus the zig shim because no cc was present. ubuntu:24.04 and debian:12 container modes were run locally at 10-19. ci.yml has a `clean-machine` job (line 648). Not re-run here (network, ~75 s); the gate ran it twice. |
| 2 | `ostro deploy` builds when needed, validates imports, uploads idempotently; `ostro dev --watch` rebuilds and redeploys on save | ✓ VERIFIED (with a user decision pending on unbumped edits; see judgement (a)) | **Re-run here:** `deploy_build` 5 passed and `dev_watch` 5 passed. The watch test showed: initial deploy 10.5 s; edit → "new runs use v2" in 1.26 s; ignore-list noise produced no builds; a broken edit gave "build failed; keeping v2"; coalescing did 2 builds for 6 edits. `deploy_build::project_deploy_is_idempotent` step 4 asserts that an edit without a version bump gets a 409 with "bump the workflow's version" and that v2 stays active. |
| 3 | `ostro replay <run\|--sample N> --module new.wasm` catches a deliberate non-deterministic change in each language and exits non-zero (CI gate) | ✓ VERIFIED | **Re-run here:** `cargo test -p ostro --features polyglot --test replay_cli` gave 6 passed: `rust_replay_gate`, `python_replay_gate`, `ts_replay_gate` (each expects exit 4 on swapped steps v2 with a divergence seq, and exit 0 on patched v3), plus `corpus_offline`. `output::exit_code(Diverged) == 4` is unit-asserted in `crates/ostro-cli/src/commands/replay.rs`. |
| 4 | Each language's harness runs a multi-day-sleep workflow against `ostro dev --ephemeral` in seconds via time skipping; TS, Python, Go and Rust clients can start, query and signal runs | ✓ VERIFIED | **Re-run here:** Rust kit 9 passed ("3-day sleep_for 59.2 ms", "signal_then_sleep 65.8 ms"); Go `ostrotest` ok (`TestThreeDaySleepAndSignal`); TS `node --test` pass 26 / fail 0; Python (uv, pytest 8.4.2) 25 passed in 8.1 s; `time_skipping_auto` 9 passed (the race_gate_1000 run is from 10-20); `OSTRO_REQUIRE_ALL_CLIENTS=1` contract 5 passed, python/ts/go 16 routes each with 0 missing, clients rust/python/ts/go. The Rust/Python/TS templates ship three-day-sleep tests (`template_tests` 5 passed at 10-20). |

**Score:** 4/4 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `crates/ostro-build/` (toolchains.toml, fetch/extract/cache/toolchain, lang builders, templates, sdk_bundle) | Hermetic toolchain cache + builders + init templates | ✓ VERIFIED | Has 13 test files (toolchain_cache, build_rust(_hermetic), build_polyglot, templates_build, …). Used by `ostro build/deploy/init`. |
| `crates/ostro-cli/src/commands/deploy.rs`, `src/watch.rs` | Build-if-stale deploy, idempotent activation, watch loop | ✓ VERIFIED | `deploy_project` is shared by deploy and watch. The 409 hint is at deploy.rs:279. |
| `crates/ostro-cli/src/commands/replay.rs` + `ostro_engine::replay_check` + server `/v1/modules/{sha}/replay` | Replay CI gate | ✓ VERIFIED | Exercised end-to-end by replay_cli. |
| `crates/ostro-server/src/devtime.rs` (DevClock, skip barrier, auto-skip), `/v1/dev/time[/skip]` | Server-side time skipping | ✓ VERIFIED | time_skipping(_auto) green. CLOCK_GATE had 0 hits at 10-20. |
| `crates/ostro-server/src/ephemeral.rs` | /dev/shm-if-room ephemeral dir (eph-fix) | ✓ VERIFIED | 3 unit tests + `ephemeral_base`. `dev_startup` enforces 1 s whenever shm is used. |
| `crates/ostro-client/` (client, typed, testing) | Rust client + kit | ✓ VERIFIED | kit/contract/typed tests |
| `clients/python` (client, routes, testing, pytest_plugin), `clients/ts` (client, routes, testing), `clients/go` (ostro, ostrotest) | Zero-dependency clients + kits | ✓ VERIFIED | All four kits drain stdout after the ready line (Rust testing.rs:461, Python testing.py:211, TS testing.js:166, Go ostrotest.go:261/273), so stdout log volume cannot block the server. |
| `scripts/clean-machine.sh`, ci.yml `polyglot` + `clean-machine` jobs, `docs/{getting-started,testing,replay,toolchains,cli-json}.md` | SC1 harness + docs | ✓ VERIFIED | Present. The remote jobs have not run yet (human_needed). |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `ostro deploy DIR` | `ostro_build::build` → upload → activate | `deploy_project` | WIRED | deploy_build tests |
| `ostro dev --watch` | `deploy_project` | notify debouncer + build thread | WIRED | dev_watch (v1→v2→broken→v3→v5) |
| `ostro replay` | server replay endpoint → `replay_check` | ostro-client | WIRED | replay_cli: exit 4 / exit 0 |
| Kits (Rust/Py/TS/Go) | `ostro dev --ephemeral --time-skipping` + `/v1/dev/skip` | spawn + frozen ready line | WIRED | All four kit suites green here |
| Clients (4) | OpenAPI routes | route tables | WIRED | contract with OSTRO_REQUIRE_ALL_CLIENTS=1: 0 missing |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|-------------|-------------|--------|----------|
| CLI-03 | 10-08, 10-15, 10-17, 10-19 | `ostro init --lang` with pinned toolchains | ✓ SATISFIED | init snapshots, templates_build, template_tests, clean_machine local_all |
| CLI-04 | 10-07, 10-12, 10-15 | Hermetic `ostro build` for Rust/Python/TS | ✓ SATISFIED | toolchain_cache, build_rust_hermetic, build_polyglot, parity, ostro-built conformance |
| CLI-05 | 10-16 | `deploy [--activate]` builds if needed, idempotent | ✓ SATISFIED | deploy_build (re-run: 5 passed) |
| CLI-07 | 10-11 | replay detects non-determinism (CI gate) | ✓ SATISFIED | replay_cli (re-run: 6 passed) |
| CLI-08 | 10-16 | `dev --watch` rebuilds and redeploys on save | ✓ SATISFIED (policy decision pending, see (a)) | dev_watch (re-run: 5 passed) |
| SDK-06 | 10-10, 10-13, 10-17 | Per-language time-skipping harness | ✓ SATISFIED | 4 kits re-run green; time_skipping_auto green |
| SDK-07 | 10-09, 10-14 | TS/Python/Go/Rust clients | ✓ SATISFIED | contract (all 4), typed, e2e; publishing is human_needed |

No orphaned requirements: REQUIREMENTS maps exactly these 7 IDs to Phase 10. CLI-01, CLI-02 and CLI-06 belong to Phases 4 and 5.

### Judgements on the open items

**(a) A dev-mode redeploy needs a workflow version bump. Does this break SC2? No. It is a policy decision for the user (human_needed), not a gap.**
- On every save, `--watch` rebuilds and tries to redeploy.
- When the save changes the declared version, the new version goes live in about 1.3 s (tested).
- When the module changes but its version does not, the server refuses the upload with a 409, because (name, version) is write-once. The watch loop prints a bump hint and keeps the active version.
- That refusal comes from the platform's determinism invariant: the same version must mean the same code, which is what SC3's replay gate depends on. It is documented in getting-started "Changing a workflow".
- SC2's mechanism (rebuild, then redeploy on save) is fully delivered. Whether dev mode should relax the invariant (options a/b) is a UX choice recorded in STATE and 10-DECISIONS.
- Caveat for the user: read literally, an unbumped save does not redeploy. If you count that case as part of SC2, option (b) closes it without touching the server.

**(b) The ephemeral startup budget on macOS and the disk fallback. Does this break CLI-01 or an SC? It does not break any Phase 10 SC or requirement. It is a known platform caveat on CLI-01, a Phase 4 requirement.**
- On Linux with ≥ 512 MiB free in /dev/shm (every normal host), `dev_startup` still asserts the 1 s budget, and it passes.
- The relaxed 15 s budget applies only where there is no usable tmpfs. That means macOS (2.7–3.4 s on the CI runner) and containers with a small shm.
- Before eph-fix, the small-shm case failed outright with ENOSPC. Falling back to disk is strictly better.
- SC4 asks for "in seconds", and that holds even on the fallback.
- The fix (an fsync-free ephemeral mode across 4–5 crates) needs its own plan. It belongs on the follow-up list with a user decision on the approach.
- CLI-01's tick should be read as "on hosts with tmpfs" until that plan lands.

**(c) About 1600 lines of stdout log noise before the ready line: ℹ️ Info / follow-up, not a gap.**
- The ready line is frozen (D-P10-9), and the `ready_line` test is green.
- All four kits match the only line that fits the pattern, and they keep draining stdout afterwards, so the noise cannot fill a pipe and stall the server.
- It does hurt the human UX of `ostro dev`. Moving the logs to stderr is a small server change, already recorded in STATE.

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| (scan of ostro-build/src, ostro-client/src, ostro-cli deploy/replay/init/build/watch, devtime.rs, ephemeral.rs, clients/*) | — | TODO/FIXME/todo!/unimplemented!/"scaffold pending" | — | None found |
| `ostro dev` server logging | — | INFO logs on stdout before the ready line | ℹ️ Info | UX only; the kits drain stdout (see (c)) |
| `crates/ostro-build/tests/build_polyglot.rs:61` | 61 | Cold-cache npmrc race (`ts_offline_after_warm`) seen once at the gate | ⚠️ Warning | Test flake, likely fixed by ci-fix6 5067bf7. Re-check on a cold cache. |
| musl binary | — | 56.2 MB of the 60 MB gate (3.8 MB headroom) | ℹ️ Info | Take a cargo-bloat breakdown before the next large addition |

Other recorded follow-ups, none blocking: the wasm32 guard should move into the `ostro_sdk::export!` macro; the OpenAPI descriptions of `RunStatusDto::Scheduled` and bulk retry are stale; Python/TS builds are not bit-reproducible (D-P9-3 residual); the image tag does not track Dockerfile changes within a version.

### Verifier test runs (this verification, worktree at 57b96be, target/slot-2)

| Command | Result |
|---------|--------|
| `cargo test -p ostro --locked --test deploy_build --test dev_watch -- --test-threads=1` | 5 + 5 passed |
| `cargo test -p ostro --locked --features polyglot --test replay_cli` | 6 passed |
| `cargo test -p ostro-server --locked --test time_skipping_auto` | 9 passed, 1 ignored (race_gate_1000, run at 10-20) |
| `OSTRO_REQUIRE_ALL_CLIENTS=1 cargo test -p ostro-client --locked --test contract` | 5 passed; python/ts/go 16 routes, 0 missing |
| `cargo test -p ostro-client --locked --features testing --test kit` | 9 passed; 3-day sleep 59 ms |
| `go test ./ostrotest/` (clients/go) | ok |
| `node --test 'clients/ts/test/**/*.test.js'` | pass 26, fail 0 |
| `uv run --python 3.12 --with pytest==8.4.2 python -m pytest tests -q` (clients/python) | 25 passed in 8.1 s |

Note: the first contract/kit attempt panicked with "no ostro binary" because the test harness needs `cargo build -p ostro` first, and that build had not run yet. After the build, both suites passed. This is not a defect.

### Human Verification Required

1. **USER DECISION: dev-mode module replacement (D-P10-13/14).** Choose (a), (b) or (c); see judgement (a).
2. **USER DECISION / follow-up plan: fsync-free ephemeral mode** for the macOS and disk-fallback startup budget; see judgement (b).
3. **USER REVIEW D-P10-6:** the auto-skip trigger.
4. **USER REVIEW D-P10-11:** the notify CC0-1.0 deny exception.
5. **macOS clean run:** `bash scripts/clean-machine.sh all --local` (and `--docker` with `OSTRO_LINUX_BIN`). Expected: 3 CLEAN lines, each under 600 s, plus the recorded ephemeral startup time.
6. **Remote CI:** push and confirm the `polyglot` and `clean-machine` jobs are green (6 CLEAN lines).
7. **Publishing:** ostro-client (PyPI/npm/crates.io), the Go tag `clients/go/v0.1.0`, the `ostro-build` image, and the Rust SDK crates. The exact commands are in 10-20-SUMMARY.

### Gaps Summary

There are no real gaps. All four success criteria and all seven Phase 10 requirements have working code, and real tests prove them. This verification re-ran the key suites in all four languages, and they are green. The open items are by-design human or user items (decisions, reviews, macOS hardware, remote CI, publishing) plus follow-ups that do not block Phase 10: the stdout log noise, the cold-cache flake re-check, and musl headroom.

---

_Verified: 2026-11-23T06:21:47Z_
_Verifier: Claude (gsd-verifier)_
