---
phase: 10-developer-cli-test-kits
plan: 20
subsystem: planning-gate
tags: [gate, reservations, requirements, validation, decisions, cli, test-kits]
requires:
  - phase: 10-developer-cli-test-kits
    provides: "10-01..10-19 SUMMARYs + eph-fix-SUMMARY"
provides:
  - "PHASE-10 GATE passed: full ci-local with musl at a6525c2 (one cold-cache flake, green on rerun), gate-only suites green, SC1 (local) / SC2 / SC3 / SC4 evidence, Phase 10 suites re-checked at 820ab8a"
  - "RESERVATIONS: 15 Phase 10 §11 rows exists (P10); RESERVATIONS CHECK PASS, no P10 ordinal"
  - "REQUIREMENTS: CLI-03, CLI-07, SDK-06, SDK-07 ticked; CLI-03/04/05/07/08 + SDK-06/07 traceability with evidence"
  - "10-VALIDATION complete (nyquist_compliant: true); 10-DECISIONS Fold-in (10-20); STATE outcome + human_needed"
affects: [phase-10-verification, phase-12, phase-13, phase-14]
tech-stack:
  added: []
  patterns: ["gate run in a dedicated worktree; docs committed from a second worktree with CAS (ancestry asserted), never moving the ci-local worktree"]
key-files:
  created:
    - .planning/phases/10-developer-cli-test-kits/10-20-SUMMARY.md
  modified:
    - .planning/RESERVATIONS.md
    - .planning/REQUIREMENTS.md
    - .planning/STATE.md
    - .planning/ROADMAP.md
    - .planning/phases/10-developer-cli-test-kits/10-VALIDATION.md
    - .planning/phases/10-developer-cli-test-kits/10-DECISIONS.md
key-decisions:
  - "The one ci-local failure (build_polyglot::ts_offline_after_warm, npmrc ENOENT on a fresh cold toolchain cache) is a flake, not a regression: the whole 'ostro-build polyglot' step passed on rerun at a6525c2 and again at 820ab8a"
  - "D-P10-6 is superseded by the code on lease deadlines: they stay real time (clock-exempt); the skip barrier guarantees no skip while a lease is held"
  - "Phase 10 coordination concern closed in STATE (Phase 11 complete, all Phase 10 plans complete)"
requirements-completed: [CLI-03, CLI-04, CLI-05, CLI-07, CLI-08, SDK-06, SDK-07]
duration: ~6h (ci-local 4h42m under parallel load; gate-only + recheck ~20 min)
completed: 2026-11-23
---

# Phase 10 Plan 20: PHASE-10 GATE Summary

**The Phase 10 gate passed.** The full ci-local run with musl passed all 37 steps apart from one: `build_polyglot::ts_offline_after_warm` failed once on a cold toolchain cache, and the whole step passed on rerun. The musl binary is 56.2 MB, under the 60 MB gate. Every gate-only suite is green, and the SC1 (local mode), SC2, SC3 and SC4 evidence is recorded below. Phase 10 allocated no ordinal. Requirements CLI-03/04/05/07/08 and SDK-06/07 are ticked with evidence.

## Gate table

All runs used the worktree at a6525c2 unless marked otherwise, with `CARGO_TARGET_DIR=target/slot-2`, `nice -n 10` and `CARGO_BUILD_JOBS=4`. The 12-06/12-09 gates and the ci-fix5/6 runs were going in parallel (load 8–10).

| Command | Result | Duration |
|---------|--------|----------|
| `bash scripts/ci-local.sh` (with musl, polyglot block, clients, size gate) | 36 PASS, 1 FAIL (ostro-build polyglot, see below), 3 SKIP (external PG/MySQL/S3 unset; human_needed as in every gate) | 4h42m (01:06–05:48 UTC) |
| rerun of the "ostro-build polyglot" step (`check_ostro_build_polyglot`, the same function) | PASS: every ostro-build suite with `--features polyglot`, then `parity`, `replay_cli` and `template_tests` | 232 s |
| no "scaffold pending" / "not present yet" in the ci-local output (10-02) | `grep` finds 0 lines | — |
| musl binary size | **56.2 MB** (56,239,472 bytes). That is +33.5 MB over Phase 4's 22.7 MB, leaving 3.8 MB of headroom | — |
| `cargo deny check` | PASS (also a ci-local step) | 2 s |
| `cargo tree -i <c> --workspace --locked -e normal,build` for chrono, bincode, webpki-roots, dirs-sys, option-ext, ring, openssl-sys | All absent from normal/build edges. Without `-e`, chrono still shows up, but only through the Phase 6 `s3s-fs` dev-dependency of ostro-objstore. ring is in Cargo.lock but has no host edge. Both were already in the 2c8642a lock, before Phase 10 | ~5 s |
| `grep -n CC0-1.0 deny.toml` | Exactly one exception, `{ allow = ["CC0-1.0"], crate = "notify" }` (line 21). Line 17 is an older comment | — |
| `cargo test -p ostro-engine --test replay_corpus --locked` | 6 passed | 42 s |
| `cargo test -p ostro-witcheck --locked` | 8 passed | 10 s |
| `git diff --exit-code ca8a62e HEAD -- wit/` | empty | — |
| `git diff --exit-code --diff-filter=MDR ca8a62e HEAD -- tests/replay-corpus` | empty | — |
| strict CLOCK_GATE (10-06 / VALIDATION gate 9) | **0 hits** at a6525c2 and at 820ab8a | <1 s |
| `cargo test -p ostro-build --features polyglot --locked --test build_rust_hermetic -- --nocapture` | 1 passed (`hermetic_build_without_cc_uses_rustup_init_and_zig`) | 13 s |
| `cargo test -p ostro-server --locked --test time_skipping_auto -- --ignored race_gate_1000 --nocapture --test-threads=1` | 1 passed. signal_before_await: 1000 iterations, p50 64 ms, p99 116 ms. start_then_await: 1000 iterations, p50 63 ms, p99 115 ms. Zero leases after every skip | 137 s |
| `cargo test -p ostro --locked --test clean_machine -- --ignored local_all --nocapture --test-threads=1` | 1 passed, 3 CLEAN lines (SC1 below) | 74 s |
| `OSTRO_REQUIRE_ALL_CLIENTS=1 cargo test -p ostro-client --locked --test contract -- --nocapture` | 5 passed. python/ts/go each have 16 routes with 0 missing. The coverage table names the clients rust, python, ts, go. No "pending" line | 11 s |
| **Recheck at 820ab8a** (main moved during the gate: ci-fix6 + Phase 12 code) | All green: `cargo build -p ostro`; the full ostro-build polyglot step; `time_skipping` + `time_skipping_auto`; `deploy_build`, `dev_watch`, `time_skipping_cli`, `ready_line`, `json_contract`, `cli_verbs`; ostro-client `kit`, `contract`, `typed`; the python/ts/go client steps; CLOCK_GATE | 216 + 52 + 116 + 2 + 14 s |

### ci-local summary (a6525c2)

```
toolchain 1.100.0                    PASS  0s
cargo fmt --check                    PASS  3s
cargo clippy -D warnings             PASS  137s
cargo build (warning-free)           PASS  119s
sdk workspace fmt + clippy           PASS  34s
sdk workspace test + wasm build      PASS  43s
guests build (wasm32-wasip2)         PASS  36s
order-saga example                   PASS  33s
external: OSTRO_TEST_PG_URL          SKIP  -
external: OSTRO_TEST_MYSQL_URL       SKIP  -
external: OSTRO_TEST_S3_URL          SKIP  -
cargo test                           PASS  5150s
polyglot toolchains present          PASS  1s
python sdk tests                     PASS  2s
ts sdk tests                         PASS  5s
polyglot conformance                 PASS  244s
clippy (polyglot feature)            PASS  33s
clients python                       PASS  10s
clients ts                           PASS  2s
ostro-build polyglot                 FAIL  147s   <- cold-cache flake; rerun PASS (232 s)
clients go                           PASS  7s
semantics sleepers (serial)          PASS  40s
failpoint list sync (ci.yml)         PASS  1s
failpoint suites                     PASS  5003s
server campaign smoke (20 kills)     PASS  1014s
phase 7 failpoint legs + manifests   PASS  53s
cargo deny check                     PASS  2s
check-vendored.sh                    PASS  2s
check-deny-fixtures.sh               PASS  0s
no chronik/tokio-uring/prometheus    PASS  1s
one partition log path (12-08)       PASS  0s
workflow validation                  PASS  2s
zig + cargo-zigbuild present         PASS  1s
musl release (static, mimalloc)      PASS  616s
musl binary size < 60 MB             PASS  0s   size=56.2 MB (56239472 bytes) delta_vs_P4=+33.5 MB
musl zigbuild test                   PASS  4188s
```

**The flake:** `thread 'ts_offline_after_warm' panicked at crates/ostro-build/tests/build_polyglot.rs:61:57: ts offline: warm-up: writing …/target/ostro-toolchains-test/state/npm/npmrc: No such file or directory`. This was the first run on a fresh worktree, so the toolchain cache was cold, and `ts_template_builds` was installing TS state at the same time (`--test-threads=2`). The rerun with a warm cache passed, and so did the 820ab8a recheck. ci-fix6 5067bf7 ("atomic writes use a per-call temp name, not just the pid") probably addresses this race. It is listed as a follow-up to re-check on a cold cache. Because of the failure, `parity`, `replay_cli` and `template_tests` did not run in that ci-local pass; they ran in the rerun.

## SC evidence

- **SC1 (local mode; container and macOS human_needed):** `clean_machine local_all` with no toolchain on PATH and a fresh toolchain dir per run:
  ```
  CLEAN rust local OK total=41.3 doctor=0.0 init=0.0 dev=0.2 deploy=40.7 run=0.1
  CLEAN python local OK total=13.2 doctor=0.0 init=0.0 dev=0.2 deploy=12.5 run=0.1
  CLEAN ts local OK total=16.7 doctor=0.0 init=0.0 dev=0.2 deploy=16.1 run=0.1
  ```
  - Rust installs rustup 1.29.3 into the cache and, because there is no C compiler, uses the zig shim as the host linker.
  - All three languages finish well under 600 s.
  - 10-19 also ran the ubuntu:24.04/debian:12 container modes locally on Docker Desktop WSL. The remote CI job and macOS are still human_needed.
- **SC2:** `cargo test -p ostro --test deploy_build --test dev_watch -- --test-threads=1`
  - `deploy_build`: 5 passed (`project_deploy_is_idempotent`, `wasm_form_keeps_the_05_14_shape`, `no_build_and_build_failures`).
  - `dev_watch`: 5 passed (`watch_redeploys_on_save_ignores_noise_and_survives_broken_edits`, `watch_with_time_skipping_deploys_and_stays_idle`, `watch_without_a_project_exits_2`).
- **SC3:** `cargo test -p ostro --features polyglot --test replay_cli`: `rust_replay_gate`, `python_replay_gate`, `ts_replay_gate` and `corpus_offline` pass (6 passed, 39 s). Each gate expects exit 4 on v2 and 0 on v3.
- **SC4:**
  - `cargo test -p ostro-client --features testing --test kit -- --nocapture`: 9 passed. Printed wall times: `3-day sleep_for 58.97 ms`, `signal_then_sleep 65.67 ms`.
  - pytest kit + e2e: 25 passed in 8.4 s.
  - node: `# pass 26 # fail 0`.
  - go: `ok …/clients/go/ostro`, `ok …/clients/go/ostrotest`.
  - `cargo test -p ostro --features polyglot --test template_tests`: rust/python/ts template tests passed (5 passed, 52.6 s).
  - The TS/Python/Go/Rust clients pass the e2e scenario (client steps + contract).

## Reservations script output (`10-20-rescheck.py ca8a62e df056f3`)

```
OK unchanged  oneof Cmd (17 entries)
OK unchanged  oneof derived::Intent (11 entries)
OK unchanged  oneof outbox_msg::Kind (6 entries)
OK unchanged  oneof dlq_update::Op (3 entries)
OK unchanged  StartRun tags (25 entries)
OK unchanged  TerminalStatus (6 entries)
OK unchanged  EventKind (25 entries)
OK unchanged  RejectReason (16 entries)
OK unchanged  InvalidCode (31 entries)
OK unchanged  MetaKind (17 entries)
OK unchanged  MetaRecord (17 entries)
OK unchanged  MetaCommand (7 entries)
OK unchanged  TABLES (17 entries)
OK unchanged  EXT tags (2 entries)

commits touching ordinal files since base: 1
     46928ae feat(12-05): ElectLeader/AlterIsr/ResignLeader validated at meta apply

Phase 10 names: code hit -> §11 row
OK       ostro-build / ostro-client / clients/{python,ts,go} / deploy/build-image
OK       /v1/dev/time, /v1/dev/time/skip, /replay, time_skipping_disabled, sample_too_large
OK       OSTRO_BIN, OSTRO_TOOLCHAIN_DIR, OSTRO_OFFLINE, OSTRO_TOOLCHAIN_MIRROR,
         OSTRO_POLYGLOT_BUILDER, OSTRO_DOCKER, OSTRO_LINUX_BIN
OK       ostro_dev_, ostro.manifest, time_skipping ([dev]), ostro-build: (image tag)
OK       CLI verbs init, build, replay, toolchain, doctor, completions
P10 ordinal rows in RESERVATIONS.md: 0 (F7 expects 0)
RESERVATIONS CHECK: PASS
```

The script compares the extracted ordinal spaces at the 10-01 baseline and at HEAD. It also attributes every commit that touched an ordinal file; no `(10-NN)` commit did. 12-05 touched `ostro-meta/src/command.rs` without changing any variant order. Each Phase 10 name has a code hit and a §11 row; the name lines above are condensed from the per-name output.

## Requirements

| Req | Status | Evidence |
|-----|--------|----------|
| CLI-01, CLI-02 | already ticked (Phase 4) | unchanged. eph-fix keeps `--ephemeral` on /dev/shm when at least 512 MiB is free; macOS has no tmpfs, so a follow-up is open |
| CLI-03 | ticked | 10-08 init snapshots; 10-15 templates_build; 10-17 template_tests; local no-toolchain run passed; container + macOS runs human_needed |
| CLI-04 | evidence added | 10-07 toolchain_cache; 10-12 build_rust + build_rust_hermetic; 10-15 build_polyglot + parity + ostro-built conformance |
| CLI-05 | evidence added | 10-16 deploy_build |
| CLI-06 | already ticked (Phase 5) | unchanged; the `cli_verbs` snapshots are green at 820ab8a |
| CLI-07 | ticked | 10-11 replay_cli: rust/python/ts gates |
| CLI-08 | evidence added | 10-16 dev_watch |
| SDK-06 | ticked | 10-10/10-13/10-17: time_skipping, time_skipping_auto + race_gate_1000, kits, template_tests; CLOCK_GATE 0 |
| SDK-07 | ticked | 10-09/10-14: contract (all four clients) + typed + e2e |

## Human-needed (exact commands)

1. **Publish ostro-client.** Review the 10-14 dry-run artefacts first.
   - PyPI: `cd clients/python && uv build && uv publish`
   - npm: `cd clients/ts && npm publish` (check `npm pack --dry-run --json` first)
   - crates.io: set `publish = true` in `crates/ostro-client/Cargo.toml`, then `cargo publish -p ostro-client`
2. **Tag the Go module:** `git tag clients/go/v0.1.0 && git push origin clients/go/v0.1.0`
3. **Publish the `ostro-build` image:** `ostro build --docker` builds it locally from `deploy/build-image/Dockerfile`. To publish, run `docker build -t <registry>/ostro-build:<version> - < deploy/build-image/Dockerfile && docker push <registry>/ostro-build:<version>` and ship its NOTICE.
4. **Publish the Rust SDK crates:** `cargo publish -p ostro-sdk-macros && cargo publish -p ostro-sdk`, from `sdks/rust` and after setting `publish = true`. The template's path dependency then switches to crates.io.
5. **Clean-machine container runs:**
   ```
   cargo zigbuild --release --locked -p ostro --target x86_64-unknown-linux-musl
   bash scripts/clean-machine.sh all --image ubuntu:24.04 --ostro target/x86_64-unknown-linux-musl/release/ostro
   bash scripts/clean-machine.sh all --image debian:12   --ostro target/x86_64-unknown-linux-musl/release/ostro
   ```
   10-19 ran these locally. Repeating them in remote CI is part of item 7.
6. **macOS clean run.** Use a darwin `ostro` on a Mac without Xcode CLT:
   - `bash scripts/clean-machine.sh all --local`
   - `OSTRO_LINUX_BIN=<linux-musl ostro> bash scripts/clean-machine.sh all --local --docker --ostro <darwin ostro>`
   - Record the timings and the `ostro dev --ephemeral` startup time.
7. **Remote CI jobs:** push and confirm that the ci.yml `polyglot` job (including the clients) and the `clean-machine` job are green. The step summary should have 6 CLEAN lines.
8. **USER REVIEW D-P10-6:** the auto-skip trigger (idle + at least one result waiter + a 20 ms quiescence re-check). Note that lease deadlines stay real time (see the Fold-in).
9. **USER REVIEW D-P10-11:** the crate-scoped notify CC0-1.0 exception in deny.toml.
10. **USER DECISION:** dev-mode module replacement. Today a changed module needs a workflow version bump before `deploy` / `dev --watch` will redeploy it. The options (a), (b) and (c) are listed in STATE and 10-DECISIONS.
11. **Follow-up plan:** an fsync-free ephemeral mode, so `ostro dev --ephemeral` meets the 1 s startup budget on macOS and on the disk fallback.

## Open issues / follow-ups (recorded in STATE)

- `ostro dev` prints about 1600 INFO lines on stdout before the ready line (10-17). The logs should go to stderr.
- The `ostro_sdk::export!` macro should carry the wasm32 guard itself (10-17).
- The OpenAPI description of `RunStatusDto::Scheduled` does not match how the server uses it for suspended runs (10-14).
- The OpenAPI description of the bulk `retry` action is still "Retry (resume) failed runs." (10-18). The snapshot is frozen, so this belongs to the API owner.
- musl size headroom is 3.8 MB; take a cargo-bloat breakdown before the next large addition (10-19).
- The `build_polyglot` cold-cache race needs a re-check on a fresh cache after ci-fix6.
- Inherited, still open: Python/TS builds are not reproducible (D-P9-3 residual); the image tag does not change when the Dockerfile changes within one version; clean-machine.sh portability to macOS is untested.

## Deviations from Plan

1. **Worktree, target dir and commits** follow the executor brief rather than `/tmp/ostro-wt-10-20` and `index-10-20`:
   - two scratchpad worktrees: the gate worktree, which was not moved while ci-local ran, and a docs worktree;
   - `target/slot-2`;
   - worktree-index commits with a CAS onto main and the ancestry asserted.
2. **The banned-crate check uses `-e normal,build`.** The plan's literal `cargo tree -i chrono` without `-e` exits 0 because of the Phase 6 `s3s-fs` dev-dependency, as 10-02 already noted. The truth being checked ("normal tree") holds.
3. **The reservations script was written fresh** (`10-20-rescheck.py`). It does a baseline-vs-HEAD comparison of every ordinal space plus a name check. Earlier gates did not commit their scripts.
4. **A targeted recheck ran at 820ab8a**, because main moved during the 4h42m ci-local run (ci-fix6 code plus Phase 12 code). The full ci-local was not re-run at the new head.
5. **The CLI-04/05/08 checkboxes were already ticked** before this plan. Their traceability rows got evidence added; the checkboxes are unchanged.
6. **The ROADMAP Phase 10 checkbox is not ticked;** the coordinator does that after verification. Only the 10-20 plan line is ticked.

## Commits

- 2d00798 docs(10-20): Phase 10 reservations flip and requirement ticks
- df056f3 docs(10-20): Phase 10 validation, decisions fold-in and state
- (this commit) docs(10-20): complete PHASE-10 GATE plan

## Self-Check: PASSED

- FOUND: 2d00798 and df056f3 on main
- FOUND: `nyquist_compliant: true` in 10-VALIDATION.md; "Fold-in (10-20)" in 10-DECISIONS.md; "exists (P10" in RESERVATIONS.md; "[HUMAN] Phase 10" in STATE.md
- FOUND: `- [x] **CLI-03**`, `**CLI-04**`, `**CLI-05**`, `**CLI-07**`, `**CLI-08**`, `**SDK-06**`, `**SDK-07**` in REQUIREMENTS.md
