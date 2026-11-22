---
phase: 10-developer-cli-test-kits
plan: 06
subsystem: clock-gate
tags: [clock, f5-gate, annotations, devclock]
requires: ["10-01"]
provides:
  - "strict CLOCK_GATE (F5) at zero un-annotated hits across ostro-server/ostro-partition/ostro-backup"
affects: ["10-10", "10-13", "10-20"]
tech-stack:
  added: []
  patterns: ["same-line trailing `// clock-exempt: <kind>` annotation (block comment where rustfmt would move a trailing one)"]
key-files:
  created: []
  modified:
    - crates/ostro-partition/src/lib.rs
    - crates/ostro-partition/src/node.rs
    - crates/ostro-partition/src/machine.rs
    - crates/ostro-partition/src/leader/shuffler.rs
    - crates/ostro-partition/src/leader/dispatcher.rs
    - crates/ostro-server/src/executor.rs
    - crates/ostro-server/src/server.rs
    - crates/ostro-server/src/effects.rs
    - crates/ostro-server/src/sink.rs
    - crates/ostro-server/src/tls/acme.rs
    - crates/ostro-server/src/tls/ca.rs
    - crates/ostro-server/src/workers/spill.rs
    - crates/ostro-backup/src/uploader.rs
decisions:
  - "Strict CLOCK_GATE scope widened to all 13 files (coordinator: 10-06 owns the 33 Phase 2-9 hits found by the 11-18 gate), not only the 8 files in files_modified"
  - "Two logic-time reads (sink.rs:533 child-retention now_ms, dispatcher.rs:1701 clock-ms -> tokio deadline bridge) annotated as `logic-time follow-up (10-10)`, not changed"
metrics:
  duration: ~25min
  completed: 2026-11-22
---

# Phase 10 Plan 06: Clock-exempt sweep Summary

Comment-only `// clock-exempt:` annotations on all 33 real-time reads in ostro-server, ostro-partition
and ostro-backup. The strict F5 CLOCK_GATE goes from 33 hits to 0. Behaviour is unchanged.

## Gate counts

| Gate | Before (base e3f989a) | After |
|------|-----------------------|-------|
| strict CLOCK_GATE | 33 hits / 13 files | 0 |
| SCOPED_CLOCK_GATE (Phase 10-owned files) | 24 hits | 0 |

CLOCK_GATE output after the change: empty (exit 0).

## Site classification

Kinds: (a) real-time cadence/deadline by design, (b) latency/duration metric, (c) startup/diagnostic
timing, (d) logic-time read (follow-up), (s) default production clock / seam plumbing.

| file:line | kind | reason |
|-----------|------|--------|
| ostro-partition/src/lib.rs:40 | s | re-export of the default `SystemClock` |
| ostro-partition/src/node.rs:33 | s | default clock import |
| ostro-partition/src/node.rs:97 | s | doc reference |
| ostro-partition/src/node.rs:109 | s | default production clock for the injected seam (`NodeConfig::new`) |
| ostro-partition/src/machine.rs:140 | a | checkpoint-interval cadence base |
| ostro-partition/src/machine.rs:526 | b | apply latency metric |
| ostro-partition/src/machine.rs:581 | b | checkpoint latency metric |
| ostro-partition/src/leader/shuffler.rs:132 | a | internal retry backoff (block comment: rustfmt moves a trailing one after `{`) |
| ostro-partition/src/leader/shuffler.rs:143 | a | internal retry backoff |
| ostro-partition/src/leader/dispatcher.rs:362 | a | lease TTL renew (real time by design) |
| ostro-partition/src/leader/dispatcher.rs:641 | a | lease TTL base |
| ostro-partition/src/leader/dispatcher.rs:647 | a | lease TTL relative now |
| ostro-partition/src/leader/dispatcher.rs:1701 | d | clock ms -> tokio deadline bridge; follow-up (10-10) |
| ostro-server/src/executor.rs:520 | a | heartbeat cadence |
| ostro-server/src/executor.rs:532 | a | lease self-fence deadline |
| ostro-server/src/executor.rs:541 | a | heartbeat cadence |
| ostro-server/src/executor.rs:770 | a | lease TTL base (`LeaseState`) |
| ostro-server/src/server.rs:298,321,326,375,418,484 | c | startup step timings (`StartupTimings`) |
| ostro-server/src/server.rs:976 | c | shutdown timing |
| ostro-server/src/effects.rs:338,471 | c | tracing-hook span timing only |
| ostro-server/src/sink.rs:533 | d | child-repair retention `now_ms`; follow-up (10-10) |
| ostro-server/src/tls/acme.rs:46,92 | s | fallback clock import / doc reference |
| ostro-server/src/tls/acme.rs:99 | s | default production clock for the injected seam (fallback when `set_clock` wasn't called) |
| ostro-server/src/tls/ca.rs:53 | a | X.509 validity timestamps checked by peers (must be wall time) |
| ostro-server/src/workers/spill.rs:369 | b | CPU-load sampling wall delta |
| ostro-backup/src/uploader.rs:481 | a | tokio interval cadence for upload rounds |

`control/watch.rs` had no hits at base (it already uses the injected clock or tokio timers).

## Logic-time follow-ups for 10-10

1. **ostro-server/src/sink.rs:533**: `plan_child_repairs` gets `now_ms` from `SystemTime::now()`
   and compares it with `last_closed_ms(journal) + retention_ms`. The journal timestamps come from the
   injected clock, so under DevClock time skipping the W-b child-retention limit would mix the two
   time bases. It should read the node's injected `Clock::now_ms()`.
2. **ostro-partition/src/leader/dispatcher.rs:1701**: turns the next injected-clock wake-up (`at_clock`)
   into a real `tokio::time::Instant` deadline (`Instant::now() + (at - clock.now_ms())`). With a
   skipping DevClock, the dispatcher sleeps in real time unless it is also woken when the clock advances.
   10-10 needs a clock-advance notification here (or must confirm its DevClock design already covers it).

Dispatcher and executor lease TTLs, heartbeats and self-fence deadlines stay in real time on purpose.
They protect against real process stalls, and time skipping must not expire leases.

## Hits outside files_modified

None remain. The 6 files outside the plan's list (effects.rs, sink.rs, tls/acme.rs, tls/ca.rs,
workers/spill.rs, plus ostro-backup uploader.rs) were annotated here on coordinator direction
(see Deviations).

## Deviations from Plan

1. **[Scope - coordinator direction] Annotated files outside `files_modified`.** The plan assigned
   effects/sink/tls/spill/uploader hits to in-flight phases. The coordinator reported that 11-18 and
   11-VERIFICATION found these 33 hits, assigned all of them to 10-06, and asked for the strict gate to pass.
   The diffs are still comment-only.
2. **[Rule 3 - Blocking] Block comment at shuffler.rs:132.** rustfmt moves a trailing `//` comment after
   `if ... {` into the block, which breaks the same-line grep. I used an inline `/* clock-exempt: ... */`
   instead, so the plan's `// clock-exempt:` strip regex doesn't apply to that one line. A manual check
   confirms the line is otherwise byte-identical.
3. Pre-existing preceding-line `// clock-exempt:` comments (effects, server:297, acme, ca, spill) were
   kept, and the gate-visible trailing annotation was added on the read line itself.
4. Worktree and target dir follow the executor brief (scratchpad worktree, target/slot-2) rather than
   `/tmp/ostro-wt-10-06`.

## Verification

- Comment-only diff check (B2): every `+` line carries `clock-exempt:`, and after stripping the annotation
  the `+` lines equal the `-` lines.
- `cargo fmt --all --check`: clean.
- `cargo clippy -p ostro-partition -p ostro-server -p ostro-backup --all-targets --locked -- -D warnings`: clean.
- `cargo test -p ostro-partition -p ostro-server -p ostro-backup --locked`: 1063 passed, 0 failed, 7 ignored (123 test binaries, exit 0)
- `cargo check --workspace --all-targets` after rebase onto main: clean (on top of 728bdea; no overlap with 12-03/12-10/10-11/10-12)

## Self-Check: PASSED

- Commits 727b415, 2924068 are present on the rebased branch
- The 13 modified files carry clock-exempt annotations, and the strict CLOCK_GATE output is empty
