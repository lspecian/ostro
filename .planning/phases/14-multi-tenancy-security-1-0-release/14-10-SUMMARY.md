---
phase: 14-multi-tenancy-security-1-0-release
plan: 10
subsystem: fault knobs (SkewClock, test endpoints) + ostro-jepsen clock / disk nemeses (QA-05)
tags: [p14, jepsen, nemesis, clock-skew, disk-stall, failpoints, test-endpoints, qa-05]
requires:
  - phase: 14-01
    provides: "Clock inventory (wall now_ms only, no monotonic method), failpoint inventory (env at start only), lease clock source (Instant, not the injected Clock)"
  - phase: 14-01b
    provides: "p14::wrap_clock -> test_hooks::wrap_clock at the clock construction"
  - phase: 14-09
    provides: "run::Nemesis / NemesisKind::Custom, RunSpec, run(), history + report files, checkers"
provides:
  - "ostro_partition::clock::{SkewClock, SkewSettings, SkewSnapshot, MonoSource, MAX_RATE_PPM} + Clock::skew() (failpoints only)"
  - "p14::test_hooks::{wrap_clock, mount, PATHS}: GET/POST /v1/admin/test/clock, POST /v1/admin/test/failpoints (failpoints builds; default builds answer 404)"
  - "ostro_jepsen::nemesis::clock::{ClockNemesis, ClockFault, bump, strobe, drift, reset, post_hook, choose_node}"
  - "ostro_jepsen::nemesis::disk::{DiskNemesis, SITES, stall, clear}"
  - "tests: ostro-partition skew_clock (7), ostro-server p14_test_hooks (1 default + 4 failpoints), ostro-jepsen nemesis_faults (3, #[ignore])"
affects: [14-22, 14-24, 14-27]
tech-stack:
  added: []
  patterns:
    - "test-only knobs exist only under cfg(feature = \"failpoints\"); default builds answer a REST 404 on the same paths"
    - "the skew handle is found through a failpoints-only default method Clock::skew() (no global registry; one SkewClock per server)"
key-files:
  created:
    - crates/ostro-partition/tests/skew_clock.rs
    - crates/ostro-server/tests/p14_test_hooks.rs
    - crates/ostro-jepsen/tests/nemesis_faults.rs
  modified:
    - crates/ostro-partition/src/clock.rs
    - crates/ostro-server/src/p14/test_hooks.rs
    - crates/ostro-server/src/server.rs (one call-site line, see Deviations: missing hook)
    - crates/ostro-jepsen/src/nemesis/{clock,disk}.rs
key-decisions:
  - "Drift moves the wall clock too: wall = inner + offset + strobe + (drift accumulated since the last reset). Nothing in the server reads a monotonic value from the injected Clock (14-01), so a monotonic-only drift would have no effect on a node"
  - "Clock::skew() (failpoints-only default method returning None) finds the handle on P14Ctx.clock; a DevClock-wrapped clock answers 412 (not skewable)"
  - "Default builds mount explicit 404 routes on both paths: without them an unknown POST falls through to the gRPC router, which answers HTTP 200 with grpc-status 12"
  - "Disk-stall sites are the existing failpoints wal::fsync, state::commit, state::persist (the plan's wal::before_fsync / state::before_persist do not exist)"
  - "The D-P14-20 negative control is inapplicable (14-01): the test asserts from the source that the lease still compares Instant values and prints the inapplicability; it fails if the lease ever reads an injectable clock"
requirements-completed: []
duration: ~2.5h
completed: 2026-11-27
---

# Phase 14 Plan 10: Fault knobs (clock skew, disk stalls) Summary

**Clock skew and disk stalls are now injectable into running failpoints processes. Every server's injected clock is wrapped in a `SkewClock`, and two test endpoints are added: `/v1/admin/test/clock` and `/v1/admin/test/failpoints`. On top of them sit the seeded clock and disk nemeses. Local 120 s runs: in-bounds clock skew (seed 1410) found 0 RYW violations over 18,730 recorded reads and every checker passed; disk stalls of 100..2,000 ms (seed 1411) lost no acked run and crashed no node, and mean write latency was 395.5 ms inside the stall windows against 6.7 ms outside. The D-P14-20 negative control does not apply, because the lease reads `Instant::now()`. With these, all four QA-05 fault classes (partitions, kill -9, clock skew, disk stalls) are available to 14-22's gate runs.**

**STOP / orchestrator decision needed: missing hook.** The 14-01b seam has no place to mount routes for 14-10. The plan says "through the existing admin router extension point recorded by 14-01", but no such point exists: 14-01 recorded none, and `server.rs` / `p14/mod.rs` are 14-01b-only ("a missing hook -> STOP"). This work therefore lives on branch `wip/14-10`, not on main. Commit de83f7a is a separate commit that adds one call site to `server.rs`, right after the Phase 12 status router:

```rust
    // Phase 14 (14-10): the failpoints-only test endpoints `/v1/admin/test/{clock,failpoints}`
    // (a default build mounts nothing).
    app = p14::test_hooks::mount(app, &p14_ctx);
```

If that line is approved, landing is a fast-forward of main to `wip/14-10` (CAS). Without it, the failpoints endpoint tests and the nemeses cannot work.

QA-05 is not ticked here (14-27's, D-P14-35).

## Commits (branch wip/14-10, on main 757271d)

| Task | Commit | What |
|------|--------|------|
| hook | de83f7a | `server.rs`: `p14::test_hooks::mount(app, &p14_ctx)` (needs approval, see above) |
| 1 | e1ade0d | SkewClock (clock.rs) + test endpoints (test_hooks.rs) + `skew_clock` + `p14_test_hooks` |
| 2 | ecfab92 | clock + disk nemeses + `nemesis_faults` |

## Endpoint contract (failpoints builds only)

Both endpoints require a system admin: `tenant = None` plus the `admin` scope, checked with the server's own authenticator (`p14::authenticator`). The responses are:

- `[auth] mode = "none"`: the dev principal is admitted.
- Token mode, missing or invalid bearer: 401.
- Token mode, any other principal (tenant admin, system `read`): 403 `missing_scope`.

The endpoints are mounted next to `/v1/admin/replication`, outside the ostro-api router. They are therefore not in the 14-15 route x scope matrix; they authenticate themselves.

- `GET /v1/admin/test/clock` returns the current state: `{offset_ms, strobe_ms, strobe_period_ms, mono_rate, mono_rate_ppm, wall_ms, inner_wall_ms, mono_ns, inner_mono_ns}`.
- `POST /v1/admin/test/clock` takes `{reset?, offset_ms?, strobe_ms?, strobe_period_ms?, mono_rate?}`:
  - `reset` is applied first, then every field that is present.
  - `mono_rate` is a fraction (0.05 = 5% fast), limited to ±0.5.
  - A strobe needs a period > 0.
  - Unknown fields or values out of range return 400 and change nothing.
  - The answer is the new state.
  - If the clock is not skewable (dev time skipping wraps it), the answer is 412.
- `POST /v1/admin/test/failpoints` takes `{name, action}`:
  - It calls `fail::cfg(name, action)` (`sleep(ms)`, `return`, `1%return`, ...); `action = "off"` calls `fail::remove`.
  - A bad action or an empty name returns 400.
  - The answer is `{name, action, active: [{name, action}]}`.
- In a default build both paths answer a REST 404 `not_found` for every method. Explicit 404 routes are needed because an unknown POST would otherwise fall through to the gRPC router, which answers HTTP 200 with grpc-status 12.

## SkewClock semantics (failpoints only, `ostro_partition::clock`)

- **Wall clock** = inner wall + `offset_ms` + strobe + the drift accumulated since the last reset.
  - The strobe is a triangle wave: 0, then +amp at a quarter period, then -amp at three quarters.
  - Offset changes apply exactly as given.
- **Monotonic clock** = `anchor_skewed + (inner_mono - anchor_inner) * (1 + rate)`.
  - Every rate change re-anchors.
  - Reset sets rate, offset and strobe to 0 and re-anchors, keeping the accumulated monotonic offset. The wall clock returns to the inner value.
  - The rate is limited to ±50%, so the clock never decreases (proptest, 256 cases).
- **No skew set:** `now_ms()` returns the inner value unchanged, and `subscribe_changes` / `skip_barrier` pass through.
- **Finding the handle:** `Clock::skew()` is a failpoints-only default method that returns `None`; `SkewClock` returns itself. The endpoints use it to find the handle on `P14Ctx::clock`. There is no global registry, so concurrent in-process servers stay independent.
- **Why drift also moves the wall clock:** nothing in the server reads a monotonic value from the injected `Clock`, so drift that changed only the monotonic value would have no effect on a node.
- **Limits** (for 14-24's threat model): skew moves only the node's injected clock (leader timers, cron, dispatcher leases, timestamps). OS timers (tokio sleeps, TCP keepalive) and the D-P13-15 read lease (`Instant`) are not skewed.

## Failpoint sites used (disk nemesis)

The nemesis uses three existing failpoints with the `sleep(ms)` action, which stalls and then lets the I/O proceed:

- `wal::fsync`: group-commit fsync in the vendored WAL.
- `state::commit`: state-store batch commit.
- `state::persist`: state-store `persist(SyncAll)`.

The plan named `wal::before_fsync` and `state::before_persist`; neither exists. The stall length is 100..=2,000 ms.

## Nemeses

- **Clock** (`nemesis/clock.rs`):
  - Faults `clock:bump` (±1..60,000 ms), `clock:strobe` (amplitude 100..5,000 ms, period 500..5,000 ms) and `clock:drift` (±5%). `ClockNemesis::menu()` returns the three in-bounds faults.
  - `clock:negative_control` (-30%) runs only when constructed explicitly.
  - Change and node are a pure function of the slot's `pick`; the node is the leader of partition `pick % P` when it is running.
  - Heal resets every node the nemesis touched.
  - Standalone `bump` / `strobe` / `drift` / `reset` functions are also exported.
- **Disk** (`nemesis/disk.rs`):
  - `disk:stall` picks its site and duration from `pick`, preferring the partition leader.
  - Heal sets the sites `off` and fails the run if a stalled node is no longer running.
  - Standalone `stall` / `clear` functions are also exported.

## Test results

Local runs, slot-3 target dir, nice 10, 4 build jobs. The failpoint binary is the debug build `target/slot-3/debug/ostro` (via `OSTRO_FAILPOINT_BIN`), and reports were written to a scratch `OSTRO_JEPSEN_OUT`.

- `ostro-partition --features failpoints --test skew_clock`: 7/7 passed (bump, strobe, drift ±5%/±1%, re-anchor, rate limits, identity, proptest monotonicity).
- `ostro-server --features failpoints --test p14_test_hooks --test-threads=1`: 4/4 passed.
  - Skew set / validate / reset.
  - A +30 s offset fired a 60 s `sleep_for` timer **30.05 s** after start.
  - `wal::fsync=sleep(200)`: start latency went from 1.7 ms to **202.2 ms**, and back under 200 ms after `off`.
  - Token mode: 401 / 401 / 403 / 403 / 200.
- `ostro-server --test p14_test_hooks` (default build): 1/1 passed (404 on both paths).
- Clippy `-D warnings`: clean for ostro-partition + ostro-server (default and failpoints) and ostro-jepsen (all targets). Own files are rustfmt-clean, and the clock gate grep on the diff found nothing.
- `nemesis_faults`, `OSTRO_JEPSEN_SECS=120` (on base a3bc6db; tree identical to the rebased commits apart from main's unrelated changes): **3/3 passed in 352 s**.

```
(a) seed 1410 | 120 s | PASS  faults: clock:bump 7, clock:drift 4, clock:strobe 4
    ops: start/ok 1117 (info 5), signal/ok 2012, result/ok 3825, cancel/ok 26
    lost: PASS (runs_checked=1117, completed=1091, cancelled=26)
    duplicate: PASS (journals_checked=1117, records_compared=250923)
    fencing: PASS (tokens_checked=3155, commands_reapplied=252180, epochs=4)
    ryw (p13_ryw, 4 sessions, no tokens): 0 violations; writes acked 12387,
        reads recorded 18730, unavailable 336
(b) seed 1411 | 120 s | PASS  faults: disk:stall 14 (state::commit 7, state::persist 4, wal::fsync 3)
    ops: start/ok 851, signal/ok 1538, result/ok 2980, cancel/ok 20
    lost: PASS (runs_checked=851, completed=831); duplicate: PASS; fencing: PASS (tokens 2409)
    write latency: inside 14 stall windows mean 395.5 ms (441 ops), outside 6.7 ms (1948 ops)
    final heal: restarted [] (no node crashed)
(c) negative control (-30% drift > lease_margin 0.2): INAPPLICABLE (asserted from source)
```

- Re-check after the rebase onto fda88ac (then rebased again onto 757271d, p14-fixups-2, whose server.rs hunk is elsewhere; re-checked there: clippy -D warnings ostro-server + ostro-jepsen with failpoints, p14_test_hooks failpoints 4/4): the Task 1 verify passed again (7, 4 and 1 tests, both clippy runs, clock gate), and a 45 s `nemesis_faults` re-run gave 3/3 PASS in 152 s: (a) seed 1410, 454 acked runs checked, RYW 0 violations over 6,124 reads; (b) seed 1411, 346 runs checked, 4 stalls, latency 625.6 ms inside the windows vs 9.9 ms outside; (c) inapplicable.

## Negative-control result (D-P14-20, W2)

The control is **inapplicable**, as 14-01 recorded. `route/consistency.rs` compares `Instant` values: `NodeReadEnv::now()` is `Instant::now()` and the anchor comes from the raft driver. A SkewClock over the injected `Clock` therefore cannot reach the lease.

- `negative_control_drift_above_lease_margin` asserts this from the source and prints the reason.
- If the lease ever reads an injectable clock, the test fails and the control has to be run for real: `ClockFault::NegativeControl` must report ≥ 1 RYW violation, labelled "assumption violated (expected)".
- 14-24 must document that the harness does not exercise lease safety under real clock drift, and the timer limitation above.

## Gate

**Gate incomplete: cluster withdrawn; consolidated gate pending on dell-4.** The plan-level `ostro-k8s-ci --parallel` could not run. Unverified:

- Every ci-local step beyond the targeted tests above: full `cargo test`, failpoint suites (incl. the whole ostro-partition failpoint suite that now contains `skew_clock`), musl build, size gate, lint group, tier C.
- The plan's exact Task 2 command with `--release` and `target/failpoints/ostro`. Locally the test harness ran in debug against the debug failpoint binary, to spare CPU.
- Long Jepsen runs (beyond 120 s) and other seeds; any run with `OSTRO_JEPSEN_TOKEN` / auth on (14-22).
- The musl binary size: the default build adds only two 404 routes and the explicit `mount` call. All knobs are behind `cfg(feature = "failpoints")`.

## Deviations from Plan

1. **[Missing hook - STOP rule] `server.rs` call site** (de83f7a): one line, kept on `wip/14-10` for approval (see top).
2. **[Rule 1] Default builds answer an explicit 404** on both paths. Without those routes an unknown POST answers HTTP 200 (gRPC unimplemented), which would fail the plan's "both endpoints 404" truth.
3. **Failpoint sites:** `wal::fsync` / `state::commit` / `state::persist` instead of the plan's names, which do not exist (existing names only, per the plan).
4. **Drift scales the wall clock too** (see the semantics section). Nothing reads a monotonic value from the injected clock.
5. **`Clock::skew()`:** a failpoints-only default trait method (clock.rs is 14-10's file) instead of a stored global handle.
6. **RYW sessions (a)** are new code in the test, because 14-09's `run()` has no RYW checker. They start crash-saga runs under their own key prefix (`ryw-<seed>-…`). The lost checker counts their keys in `keys_in_log` but checks only the workload's acknowledged runs. The 13-08 oracle is used unchanged.
7. **Test harness built in debug** for the local runs (see Gate).

## Workflow hunks (for 14-27, ci-local + ci.yml "Failpoint suites")

The failpoints half of `p14_test_hooks` runs only with `--features failpoints`. Add it to the failpoint suites:

```
FAILPOINT_CRATES=(... ostro-server:audit_journal ostro-server:p14_test_hooks)
# ci.yml Failpoint suites, same order:
cargo test -p ostro-server --features failpoints --test p14_test_hooks --locked -- --test-threads=1
```

`skew_clock` is already covered by the `ostro-partition` entry. `nemesis_faults` is `#[ignore]`; 14-22 wires the Jepsen runs.

## Product anomalies

None found (no STOP for anomalies).

## Self-Check

PASSED: all 8 plan files exist on `wip/14-10` (ecfab92); commits de83f7a, e1ade0d and ecfab92 are on `wip/14-10`, based on main 757271d (not on main: hook approval pending); `cfg(feature = "failpoints")` is in test_hooks.rs, `mono_rate` in nemesis/clock.rs, and `test_hooks::mount` is called from server.rs. The `wrap_clock` key link (p14/mod.rs -> `test_hooks::wrap_clock`) is unchanged from 14-01b.
