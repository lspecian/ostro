---
status: resolved
trigger: "crates/ostro-partition/tests/dispatcher.rs::pipelined_grants_never_overlap_and_rejections_are_fenced intermittently fails assert_eq!(completed.len(), RUNS) (999 vs 1000 in ci-local; 980 vs 1000 in 1/15 isolated reruns)"
created: 2026-11-20T00:00:00Z
updated: 2026-11-20T12:00:00Z
---

## Current Focus

hypothesis: CONFIRMED H1 (test race); fixed in the test
test: done
expecting: n/a
next_action: none (resolved)

## Symptoms

expected: every run recorded exactly once in executor's `completed` map after all 1000 runs are Completed in the store
actual: completed.len() is 999 / 980 on occasional runs
errors: assertion `left == right` failed, left 999/980 right 1000 (dispatcher.rs ~904)
reproduction: cargo test -p ostro-partition --test dispatcher pipelined_grants (about 1 in 15)
started: unknown; passed in Phase 5 gate runs

## Eliminated

- hypothesis: H2 product bug - a run completes via a path where no executor job gets ApplyOutcome::Accepted (job panic on unexpected outcome / Err after the append applied, aborted job future, duplicate or rejected grant whose job still completes)
  evidence: dispatcher catches job panics via JoinSet and retries, so a panic would hide a recording; instrumented the `other =>` arm and slow submits: 0 hits over 66 runs (incl. 36 under parallel load). Rf1 append has no timeout path (errors fail the partition). Job futures are aborted only at shutdown. In the reproducing run all 1000 runs were recorded exactly once 2.8 ms later, and overlaps stayed 0.
  timestamp: 2026-11-20
- hypothesis: regression from 06-03 / 08-07 / 08-08 / 08-09
  evidence: `git log 0ec6391..main -- machine.rs acks.rs handle.rs` is empty: the commit -> applied_tx -> acks.resolve -> oneshot wake path and the test body are unchanged since 0ec6391 (05-16, where the test was introduced). The race window is pure scheduling latency, so flake rate tracks host load (4-5 parallel agents now), not code. Bisect skipped: a 1-in-15..36 flake under variable load gives no reliable signal per commit, and the mechanism was observed directly.
  timestamp: 2026-11-20

## Evidence

- timestamp: 2026-11-20
  checked: crates/ostro-partition/src/machine.rs apply_once, acks.rs, handle.rs submit_with_offsets
  found: apply commits the store batch (run status Completed becomes readable by read_run), then applied_tx.send_replace, then acks.resolve sends the outcome over a oneshot. The executor task must then be woken by tokio, return from submit_one, and only then insert into the test's `completed` map. A whole apply chunk (many completions) resolves at once.
  implication: the test's status-poll loop can see 1000/1000 Completed while some executor tasks have not yet run their recording; window = scheduling latency, grows under CPU load.
- timestamp: 2026-11-20
  checked: 50 isolated instrumented runs (print completed.len() at poll exit, then wait for 1000)
  found: at_exit=1000 every time; extra wait 1-40 us (no repro on idle-ish host)
  implication: race window normally microseconds
- timestamp: 2026-11-20
  checked: 36 instrumented runs, 3 concurrent processes (load)
  found: one run with `at_exit=986 final=1000 waited=2.78ms`, stats expired=0 rejected=341 react=0, no unexpected outcomes, test (with the wait) passed: every run exactly once, 0 overlaps, log checks all green
  implication: the original assertion would have failed (986 vs 1000) purely because the map was read 2.8 ms too early - matches the reported 999/980 shape

- timestamp: 2026-11-20
  checked: deterministic delay injection (Accepted arm sleeps 300 ms before recording for runs with id byte 17 % 50 == 0)
  found: original test fails at dispatcher.rs:905 `left: 997 right: 1000` (same assertion, same shape as reported); fixed test passes
  implication: the reported failure is fully explained by reading the map before the executor records
- timestamp: 2026-11-20
  checked: negative control on the fixed test (Accepted arm skips recording for runs with id byte 17 == 7)
  found: fixed test fails `left: 998 right: 1000: every completed run is recorded by its executor job`
  implication: the fix does not weaken the exactly-once check; a completion without an executor recording still fails

## Resolution

root_cause: test race, not a product bug and not a regression. Apply commits `Completed` (readable via read_run) before acks.resolve wakes the accepting job's submit_one; the test's status-poll loop could see 1000/1000 Completed and read its `completed` map while a chunk of executor tasks had not yet recorded (ms-scale under CPU load). Present since the test's introduction in 0ec6391 (05-16); the commit->ack path is unchanged since. Flake rate rose with host load (parallel agents).
fix: test tracks executor jobs in flight (incremented in execute, decremented after recording) and, after the status poll, waits (bounded by T) until all RUNS are recorded and no job is in flight before asserting exactly-once. A panicking job (dispatcher swallows and retries) or a completion with no executor recording never satisfies the wait and fails the assertions.
verification: delay injection - original fails (997/1000), fixed passes; negative control (skip recording) - fixed fails (998/1000); fixed test 85/85 passes (45 in 3 concurrent loops + 40 final-version in 2 concurrent loops); full `--test dispatcher` 17/17; clippy -p ostro-partition --all-targets -D warnings clean; cargo fmt --check clean
files_changed: [crates/ostro-partition/tests/dispatcher.rs]
