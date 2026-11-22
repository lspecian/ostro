---
phase: 10-developer-cli-test-kits
plan: 10
subsystem: dev-time-skipping
tags: [sdk-06, devclock, time-skipping, dispatcher, skip-barrier, cli, build-info]
requires:
  - phase: 10-developer-cli-test-kits
    provides: "10-05 dev endpoints (409 stub), 10-06 clock-exempt sweep + logic-time follow-ups, 10-03 p10 spawned-binary helpers"
  - phase: 08-triggers-bottomless-observability
    provides: "08-04 injected clock seam: ServerOptions::clock / with_clock, NodeConfig.clock, ServiceCtx/ControlCtx clock"
provides:
  - "ostro_server::devclock::DevClock (both clock traits from base + growing offset, skip_by/skip_to/skip_guarded, watch notifier, barrier)"
  - "ostro_partition::Clock::subscribe_changes / skip_barrier defaults (None in production)"
  - "dispatcher clock-change wake + grant read guard + DispatcherStats::granting gauge (10-13 idle snapshot input)"
  - "[dev] time_skipping (off|auto|manual) + ServerConfig::validate_time_skipping"
  - "GET /v1/dev/time, POST /v1/dev/time/skip implemented; ServerHandle::dev_clock()"
  - "ostro dev --time-skipping[=auto|manual]; ostro build-info --json"
  - "guests/p10-time; crates/ostro-server/tests/time_skipping.rs"
affects: [10-13, 10-14, 10-17, 10-20]
tech-stack:
  added: []
  patterns:
    - "dev-only clock hooks as default trait methods returning None (zero production change)"
    - "skip barrier: tokio RwLock, grants read, skips write; check closure re-snapshots under the write guard"
key-files:
  created:
    - crates/ostro-server/src/devclock.rs
    - crates/ostro-partition/tests/dispatcher_clock.rs
    - crates/ostro-server/tests/time_skipping.rs
    - crates/ostro/tests/time_skipping_cli.rs
    - crates/ostro/tests/build_info.rs
    - guests/p10-time/Cargo.toml
    - guests/p10-time/src/lib.rs
  modified:
    - crates/ostro-partition/src/clock.rs
    - crates/ostro-partition/src/leader/dispatcher.rs
    - crates/ostro-server/src/lib.rs
    - crates/ostro-server/src/server.rs
    - crates/ostro-server/src/config.rs
    - crates/ostro-server/src/control/mod.rs
    - crates/ostro-server/src/control/dev.rs
    - crates/ostro-server/src/sink.rs
    - crates/ostro-server/src/executor.rs
    - crates/ostro/src/main.rs
key-decisions:
  - "Seam override used: start_inner wraps the ServerOptions clock (the F5 seam) in a DevClock when [dev] time_skipping != off and rebinds `clock` before any consumer; RuntimeOptions.clock is set to the same Arc. Off: no DevClock is built and the code path is the HEAD one"
  - "Time skipping implies an embedded-only node: `ostro dev --time-skipping` turns the worker listener off unless OSTRO_WORKERS__ENABLED is set explicitly (then exit 2). Without this the flag could never pass (workers.enabled defaults to true in both profiles)"
  - "Refusals exit 2 (new usage_error helper in main.rs); other config errors keep exit 1"
  - "DispatcherStats::granting is a new gauge (no granting counter existed): incremented under the read guard, decremented when the grant task ends (drop guard, also on panic/cancel)"
  - "The executor-level attempt-failure retry (ExecOutcome::Retry, retry_backoff) still sleeps in real time; step backoffs (EnqueueReady ready_at) follow the dev clock via the dispatcher wake. Lease TTL / heartbeat / self-fence stay real time (10-06)"
  - "Manual skips in 10-10 always apply (check returns the requested target); `idle`/`next_due_ms` are false/None until 10-13"
requirements-completed: []
duration: ~2h40m
completed: 2026-11-22
---

# Phase 10 Plan 10: DevClock time skipping (server half 1) Summary

**A dev-only `DevClock` (base + growing offset) is injected through the Phase 8 clock seam and `RuntimeOptions.clock`. The dispatcher wakes on clock jumps and grants under a skip barrier. Manual skips work through `POST /v1/dev/time/skip`. `ostro dev --ephemeral --time-skipping[=auto|manual]` enables it, with exit-2 refusals. `ostro build-info --json` exists.**

SDK-06 stays open: auto skipping (10-13) and the kits (10-14/10-17) are still to come.

## Commits
- 41d4c95 feat(10-10): DevClock and dispatcher clock-change wake
- c196987 feat(10-10): wire DevClock through the clock seam, [dev] config, ostro dev --time-skipping
- 7291022 fix(10-10): child-repair retention reads the journal clock, not SystemTime (10-06 follow-up 1)
- 5d13fa1 test(10-10): manual time-skipping suite with the p10-time guest

## Seam override API used
- `ServerOptions::clock` / `with_clock` (08-04) is still the injection root.
- In `server::start_inner`, right after the options are destructured, `cfg.validate_time_skipping(server_profile)` runs. The profile is `opts.tls_profile`, or else `TlsProfile::of(&cfg)`. This happens before the data directory is resolved, so a refused start creates nothing.
- When the mode is on, the code does `let clock: Arc<dyn Clock> = DevClock::new(clock, mode)`. Every later consumer gets this one instance: `InitCluster`, the P7 seam, the guest log sink, `NodeConfig.clock`, the services and `ControlCtx`.
- `runtime_opts.clock = dev.clone()` makes journal timestamps and `ServerControlPlane::now_ms` use the same clock.
- `ServerControlPlane::with_dev(Option<Arc<DevClock>>)` sets the control plane's dev clock.
- `ServerHandle::dev_clock()` exposes it to tests and to 10-13.

## Dispatcher hunk (partition `leader/dispatcher.rs`)
- `let mut clock_rx = self.ctx.clock.subscribe_changes();` runs before the loop.
- A new `tokio::select!` arm, `r = clock_changed(&mut clock_rx)`, returns `Flow::Continue`; a closed channel sets `clock_rx = None`. The top of the loop then re-runs `promote_due(now)` and `unpark_due(now)` and recomputes `next_deadline()` from the new time.
- `dispatch()` and `regrant_after_failure()` take `ctx.clock.skip_barrier()`. Inside the spawned grant task, each one does the following:
  1. takes `read_owned()`;
  2. counts the grant with `Granting::new(stats)`, which bumps `DispatcherStats::granting`;
  3. appends `AttemptGranted`;
  4. drops the guard.

  The retry path guards only the grant itself, never the backoff. A "no nested acquisition" comment sits at both sites.
- The `next_deadline` clock-exempt comment changed from "logic-time follow-up (10-10)" to "real-time sleep bridge; clock jumps wake the loop (10-10)". This resolves 10-06 follow-up 2.
- Existing dispatcher tests pass unchanged: dispatcher (7), dispatcher_flow (17) and the full `cargo test -p ostro-partition`.

## Config validation messages (`ServerConfig::validate_time_skipping`, exit 2 from the binary)
- server profile: `--time-skipping ([dev] time_skipping) is dev only: `ostro server` refuses it; use `ostro dev --ephemeral --time-skipping``
- not ephemeral: `--time-skipping needs --ephemeral: skipped time is written into the journal, so it is refused on a persistent data directory`
- worker listener: `--time-skipping cannot be used with a worker listener ([workers] enabled = true): only embedded executors follow the dev clock`
- remote placement: `--time-skipping cannot be used with remote placement ([workers] default_placement = "workers"|"auto"): only embedded executors follow the dev clock`

The binary prints each one as `ostro: error: invalid configuration: <message>`. With time skipping on, `ostro dev` also prints this to stderr: `time skipping: <mode> (dev only; data is ephemeral)`. The ready line is unchanged.

## Per-scenario wall times (manual suite, `--test-threads=1`, load ~12)
| Scenario | Wall time after the skip |
|----------|--------------------------|
| sleep_for 3 d, one 72h skip → Completed | 45.9 ms |
| backoff_step, attempt 2 after a 1h skip | 10.8 ms |
| backoff_step, attempt 3 after a 1h skip | 12.4 ms |
| backoff_step, start to Completed | 45.2 ms |
| wait_forever (48h run timeout), 49h skip → TimedOut (`run_timeout`) | 33.2 ms |
| signal_then_sleep, 1 d skip → Completed | 45.4 ms |
| partition `dispatcher_clock`: 1 h backoff dispatched after a clock jump | < 1 s (asserted) |

The whole `time_skipping` binary took 22 s; most of that is server starts and the guest build.

## Lease/activation count source for the D-W26 assertion
- **Counts used:** `ServerHandle::dispatcher_stats()`. `leases` is the existing per-loop-turn lease gauge; `granting` is the new grant-in-flight gauge.
- **Before each skip:** `wait_parked` requires all of the following:
  - the run's history has the parking event;
  - `leases == 0` and `granting == 0`;
  - the run is not terminal.
- **At the skip:** the test skips through `DevClock::skip_guarded`. Inside the `check` closure, which runs under the barrier's write guard, it asserts `leases == 0` and `granting == 0`.
- **Deviation:** the plan said "asserted again immediately after each skip". The assertion is made at the skip instant, under the guard, instead. Right after a skip the woken runs take leases legitimately, so a check after the skip would race.
- Lease TTLs stay real-time (`Instant`; 10-06), so a skip cannot expire a lease anyway. D-P10-6's note that D-W26 puts leases on the injected clock does not match the code at HEAD.

## Shared-file bases
Bases moved twice while I worked:
- **12-08** renamed `Rf1Log` to `PartitionLog` and edited `dispatcher.rs`, `control/mod.rs`, `sink.rs` and `executor.rs`.
- **10-16** added `--watch` to `main.rs`.
- **ci-fix5** landed commits too.

My diff re-applied cleanly with `git apply -3` everywhere except `main.rs`. There I kept both sides: `watch` and `time_skipping` in `Command::Dev`, and `apply_time_skipping` runs before `run_server(..., watch)`. Each commit was then cherry-picked onto the current main and landed with a CAS `update-ref` after the ancestry assert.

## Tests run
| Command | Result |
|---------|--------|
| `cargo test -p ostro-partition --locked` (full, before the rebase) | exit 0, no failures |
| `cargo test -p ostro-partition --test dispatcher_clock --test dispatcher --test dispatcher_flow --test timers` (after the rebase) | 2 + 7 + 17 + 15 passed |
| `cargo test -p ostro-server --lib` | 166 passed (devclock 5, dev_section_tests 2) |
| `cargo test -p ostro-server --test time_skipping -- --test-threads=1 --nocapture` | 7 passed |
| `cargo test -p ostro-server --test config --test p11_contract --test dev_surface` | 44 + 10 + 1 passed |
| `cargo test -p ostro-server --test sink --test children_resume --test children_detached --test executor --test semantics_e2e` | 19 + 3 + 9 + 8 + 18 passed |
| `cargo test -p ostro-server --test leases --test executor_phase5 --test flow_control --test schedules` | all passed |
| `cargo test -p ostro --test time_skipping_cli --test build_info -- --test-threads=1` | 8 + 4 passed |
| `cargo test -p ostro --test cli --test dev_startup --test server_config -- --test-threads=1` | all passed |
| `cargo fmt --all --check`; `cargo clippy --workspace --all-targets --locked -- -D warnings` | clean |
| Plan clock grep gate | no hits |

**Full `scripts/ci-local.sh --no-musl` was not run** (it takes about 3 h, and 4 parallel agents share the machine).

## Deviations from Plan
1. **[Rule 3 - Blocking] Worker listener turned off by the flag.** `workers.enabled` defaults to true in the dev profile. Without this change `--time-skipping` would always be refused. An explicit `OSTRO_WORKERS__ENABLED` keeps the listener on and is refused instead.
2. **[Rule 1 - Bug, coordinator follow-up] Child-repair retention clock.** `sink.rs` and `executor.rs` are outside files_modified. `PartitionSink::with_clock` now carries the runtime clock, so `plan_child_repairs` compares journal time with journal time.
3. **One guest, four workflows.** `guests/p10-time` is a single component that exports four workflows (`export!` takes several), because each workflow needs its own run-timeout attribute. `backoff_step` counts failures with workflow time (`ctx.now() - t0` in hours), not with an attempt number. The SDK has no attempt accessor, and guest statics reset on every requeue.
4. **Skips through the dev clock directly.** The suite skips in process via `ServerHandle::dev_clock().skip_guarded`, so it can assert lease counts under the barrier. The REST skip API is covered separately: past target, accumulation, 400 and 409.
5. **Test layout.** The dispatcher wake test is a new partition integration file, `tests/dispatcher_clock.rs`, rather than an edit to the shared `tests/dispatcher.rs`. The `[dev]` config tests are a `#[cfg(test)]` module appended to `config.rs`.
6. **Worktree and target dir.** Per the executor brief, I used a scratchpad worktree and `target/slot-2`, not `/tmp/ostro-wt-10-10`.

## Open issues / for later plans
- **10-13:** idle detection and the auto trigger (D-P10-6 [USER REVIEW]). `mode = auto` is accepted and reported today, but nothing skips automatically yet. `DevTimeInfo.idle` and `next_due_ms` are placeholders; the inputs exist (`DispatcherStats::{leases, granting}`, `DevClock::skip_guarded`).
- **Executor-level retry backoff** (`ExecOutcome::Retry`, `[executor] retry_backoff`) still sleeps in tokio real time. Step backoffs are unaffected. 10-13 can route it through the clock if needed.
- **macOS `ostro dev --ephemeral` 1 s budget:** there is no tmpfs on macOS. An in-memory or fsync-free ephemeral mode did not fall out of this plan naturally: the state store and log stay on disk. Recorded for 10-13.
- **Metrics:** the `ostro_dev_*` series register only when a DevClock exists. The Prometheus recorder is process-global, so absence is not asserted inside one test binary.

## Self-Check: PASSED
- Files exist: `devclock.rs`, `dispatcher_clock.rs`, `time_skipping.rs` (server), `time_skipping_cli.rs`, `build_info.rs`, `guests/p10-time/{Cargo.toml,src/lib.rs}`.
- Commits 41d4c95, c196987, 7291022 and 5d13fa1 are on main.
