---
status: awaiting_human_verify
trigger: "outbound_traceparent passes but logs `ERROR ostro_partition::machine: apply thread panicked partition=2` / `partition apply machine failed reason=\"apply thread panicked\"` (main 3d8fbde, full cargo test -p ostro-server)"
created: 2026-11-21T08:20:00Z
updated: 2026-11-21T11:55:00Z
---

## Current Focus

hypothesis: CONFIRMED - see Resolution
test: full ostro-partition + ostro-server suites with the fix, grep for the panic line
expecting: zero "apply thread panicked" / "being shutdown" lines, no regressions
next_action: caller confirms; then archive to resolved/

## Symptoms

expected: no apply-thread panic during any test (crash() or graceful shutdown)
actual: `thread 'ostro-apply-p2' panicked at tokio-1.54.1/src/runtime/time/entry.rs:534:9: A Tokio 1.x context was found, but it is being shutdown.` then `apply thread panicked partition=2`
errors: see above
reproduction: `RUST_BACKTRACE=1 cargo test -p ostro-server --test outbound_traceparent --locked -- --nocapture` (11/13 runs)
started: since outbound_traceparent (08-21) uses crash(); mechanism predates it (any crash() with a dirty partition)

## Eliminated

## Evidence

- timestamp: 2026-11-21T08:40:00Z
  checked: 13 sequential runs of outbound_traceparent with RUST_BACKTRACE=1 --nocapture
  found: run1 (cold build) and runs 8, 12 clean; 10/12 warm runs show the panic, always right after "effect spans" (i.e. at s.crash() step (d))
  implication: reproducible, tied to crash()

- timestamp: 2026-11-21T08:42:00Z
  checked: backtrace (scratchpad dbg-panic-run2.log)
  found: TimerEntry::poll_elapsed (entry.rs:534) <- Sleep::poll <- Machine::wait select! (machine.rs:464) <- Handle::block_on (machine.rs:463) <- run_inner (389) <- run <- spawn closure catch_unwind (164)
  implication: not an apply-logic panic; the apply thread's idle wait polls a timer on the shut-down server runtime

- timestamp: 2026-11-21T08:43:00Z
  checked: machine.rs wait(), tests/common/mod.rs crash()
  found: wait() arms `sleep_until(last_ckpt + checkpoint_every)` only `if dirty`; crash() calls rt.shutdown_background() while apply threads (std threads) are blocked in rt.block_on(...). Partitions 0,1,3 (clean, no sleep armed) logged "apply machine stopped" normally; p2 (just applied the in-flight effect) panicked.
  implication: classification (b) shutdown artifact, but in the machine, not the harness: any runtime shutdown while a dirty apply thread waits panics

- timestamp: 2026-11-21T08:44:00Z
  checked: production shutdown (ServerHandle::shutdown -> Node::shutdown -> MachineHandle::shutdown joins threads) and ostro main run_server
  found: graceful path joins apply threads before the runtime drops, so a clean shutdown does not hit it. But if shutdown bails early (e.g. ServerError::Invariant "node still referenced", Node never shut down) or Server::start fails after spawning machines, run_server returns and drops the multi-thread runtime while apply threads live -> same panic on exit for dirty partitions.
  implication: production degraded-shutdown paths can hit it too; fix in machine.rs, not only the harness

- timestamp: 2026-11-21T09:00:00Z
  checked: new deterministic test crates/ostro-partition/tests/apply_runtime_shutdown.rs (dirty machine, own multi-thread runtime, shutdown_background, assert health) on unfixed code
  found: FAILED with the identical panic (`ostro-apply-p0` at tokio entry.rs:534, health = Failed { reason: "apply thread panicked" })
  implication: root cause confirmed in isolation, independent of ostro-server and the bridge

- timestamp: 2026-11-21T09:10:00Z
  checked: effects_seam.rs (the only other server test using crash()), 3 runs on unfixed code
  found: 0 panics (its crashes happen when no partition is inside the checkpoint_every window after an apply)
  implication: outbound_traceparent is the only server test that reliably hits it; the mechanism is generic

- timestamp: 2026-11-21T09:30:00Z
  checked: fix applied; regression test x6, outbound_traceparent x12 (--nocapture, RUST_BACKTRACE=1)
  found: regression test passes 6/6; outbound_traceparent 12/12 ok with 0 "panicked", 0 "being shutdown", 0 ERROR lines; p2 now logs "apply machine stopped" after crash() like the others
  implication: fix verified against the original symptom

- timestamp: 2026-11-21T11:50:00Z
  checked: regression suites with the fix (load avg 16-54 throughout, other agents building)
  found: clippy -p ostro-partition --all-targets -D warnings clean. ostro-partition: all binaries pass except snapshot_tail and trim_safety sc4_random, both "node open timed out: Elapsed(())" (OPEN_T 120 s) - snapshot_tail fails IDENTICALLY on unfixed main in the same conditions (baseline run, 2 timeouts), trim_safety failure is the same fresh-store open timeout (known: node-open-stall.md, fsync-bound fresh fjall opens). ostro-server full suite: 316 passed, 17 failed - all "node open timed out" / "server start timed out" (leases, lifecycle_api) plus a timing assertion in guest_logs during a load spike to 54; the three binaries rerun sequentially: guest_logs 4/4, leases 4/4, lifecycle_api 13/13. Across the whole server suite: 0 "apply thread panicked", 0 "being shutdown".
  implication: no regression attributable to the fix; failures are the pre-existing load-dependent open timeouts

## Resolution

root_cause: Machine::wait (crates/ostro-partition/src/machine.rs) ran `self.rt.block_on(select!{..., sleep_until(checkpoint deadline), if dirty})` on the SERVER runtime handle from the std apply thread. When that runtime shuts down while the thread is alive (TestServer::crash() -> Runtime::shutdown_background; in production any early-exit path that drops the runtime before Node::shutdown joins the machines), the time driver shutdown fires the pending TimerEntry and Sleep::poll panics ("A Tokio 1.x context was found, but it is being shutdown"). catch_unwind turned it into Failed("apply thread panicked"). Only dirty partitions (applied within checkpoint_every of the crash) arm the timer, hence partition=2 only and ~10/12 runs. Classification (b) shutdown artifact - but located in the machine, reachable outside the harness, so fixed in the machine.
fix: The apply thread builds a private time-only current-thread tokio runtime (on the thread, outside any async context; build failure -> Failed, not a panic) and Machine::wait block_ons on it. The waited channels (watch/mpsc) are runtime-agnostic; log reads still use the server runtime. The checkpoint deadline is no longer tied to the server runtime's lifetime, so runtime shutdown cannot panic the apply thread; it keeps idling until its handles drop and then stops gracefully as before.
verification: regression test red before (identical panic) / green after (6/6); outbound_traceparent 12/12 clean (was 10/12 runs panicking); full ostro-server suite 0 apply panics; failing binaries rerun green; remaining partition failures are pre-existing load open-timeouts reproduced on unfixed main
files_changed: [crates/ostro-partition/src/machine.rs, crates/ostro-partition/tests/apply_runtime_shutdown.rs]
