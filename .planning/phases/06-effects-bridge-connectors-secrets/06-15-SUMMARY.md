---
phase: 06-effects-bridge-connectors-secrets
plan: 15
subsystem: server-effect-seam-e2e
tags: [e2e, effects, bridge, b1, b2, crash, failpoints, fx-01, fx-03, fx-06]
requires:
  - "06-09 fx-http / fx-canary guests (I/O contracts)"
  - "06-10 MockProvider + derive_key"
  - "06-14 server effect seam + test helpers (start_server_with_bridge, crash, deploy_named, put_*)"
provides:
  - "crates/ostro-server/tests/effects_seam.rs: in-process seam e2e (6 tests; 8 with --features failpoints)"
affects: [06-17, 06-19]
tech-stack:
  added: []
  patterns:
    - "Eviction observed through executor job records (ActivationOutcome::Evicted{HotWaitElapsed}), because a run waiting on an in-flight effect re-activates and never becomes Scheduled"
    - "Offline replay (ostro_engine::testkit::replay_journal on a fresh Runtime) to prove a completed journal replays without provider contact"
    - "Test-local detached TCP forwarder so the mock's /pay handler survives the client vanishing in a crash"
key-files:
  created:
    - crates/ostro-server/tests/effects_seam.rs
  modified: []
key-decisions:
  - "Effect ops and keys come from the journal (EffectScheduled seq) through ostro_bridge::http::derive_key, so the tests check the real derived Idempotency-Key"
  - "B1 is asserted on the raw journal right after restart (repair_unsupported has run) and again after completion: no EffectCompleted with kind unsupported; the outcome carries the provider's idempotent-replayed header"
  - "B2 blob path is checked on the stored journal row (query::journal_rows -> PayloadRef Kind::Blob)"
requirements-completed: [FX-01, FX-03, FX-06]
metrics:
  duration: ~40 min
  completed: 2026-11-20
---

# Phase 6 Plan 15: Server seam e2e (in-process) Summary

**The 06-14 seam now has end-to-end tests in a real in-process server against a loopback MockProvider. They cover intent before I/O, journal-only delivery with scrubbing, the B2 1 MiB blob outcome, the B1 re-drive after a crash with no unsupported repair, the crash wake of a run evicted with an effect in flight, and the regressions for children and replay. Everything passed on the first run against the product code; nothing in the product had to change.**

## Tests (`crates/ostro-server/tests/effects_seam.rs`)

| Test | Feature | What it proves |
|------|---------|----------------|
| `intent_before_io` | - | `EffectScheduled.ts_nanos` ≤ the first moment the mock was seen holding the request. One request carried the derived key, one charge, outcome http 200 |
| `intent_before_io_failpoint` | failpoints | `bridge::http::before_send=return(wait:1500)`: `EffectScheduled` is in the journal while `requests_to("/pay") == 0`, and still 0 with no completion 300 ms later. After release: exactly 1 request, 1 charge, guest sees 200 |
| `journal_delivered` | - | fx-canary posts `X-Api-Key: {{secret:tok}}` (sealed `_/tok`, grant on the mock origin) to `/echo`. The guest's `observed.body` bytes equal the journaled `EffectCompleted` Http body. The echoed `x-api-key` is `[REDACTED]`, and the canary appears in no journal event, no outcome bytes and not in the stored result |
| `large_outcome_blob` (B2) | - | http-target `max_response_bytes` = 2 MiB, GET `/big?bytes=1048576`. The guest body is 1,048,576 bytes with matching sha256. The `EffectCompleted` journal row is `Kind::Blob` (len > 1 MiB). An offline replay of the journal returns `Completed` with output == stored result, appends 0 events, and `/big` is still requested only once |
| `children_still_unsupported` | - | wit-surface `surface`: `ChildCompleted` is `NonRetryable{kind: "unsupported", message: CHILDREN_UNSUPPORTED}`. Its 3 effects went through the bridge, and none of them completed as unsupported |
| `completed_journal_no_contact` | - | fx-http `[GET /echo, POST /pay?delay_ms=3000]`. At least one `HotWaitElapsed` eviction happens while `/pay` is in flight, and every re-activation serves `/echo` from the journal: `/echo` 1, `/pay` 1, total 2. An offline replay of the finished journal leaves `total_requests()` unchanged and appends 0 events |
| `uncompleted_effect_redriven_not_repaired` (B1) | failpoints | `bridge::effect::before_complete_submit=return(hang)`, POST `/pay`, wait for `charges(key) == 1`, then `crash()`, failpoint `off`, `restart()`. Right after restart the journal has no unsupported `EffectCompleted`. The run completes Ok with exactly one `EffectCompleted` for the op (http 200, `idempotent-replayed` header). `requests(key) == 2`, `requests_to("/pay") == 2` (same key on both), `charges(key) == 1` |
| `evicted_inflight_wakes` | - | POST `/pay?delay_ms=5000` with a retry policy. Wait for a `HotWaitElapsed` eviction (asserted < 4.5 s after the request), then `crash()` before the provider responds and `restart()`. The LeaderAnnounced wake re-activates the run, the replay re-drives with the same key, and the run completes with http 200 and `charges(key) == 1` |

Journal dumps from the two crash tests (`--nocapture`):

```
B1:      #1 EffectScheduled /pay   #2 EffectCompleted op 1 -> http 200   #3 WaitResolved [1]   #4 RunCompleted
evicted: #1 EffectScheduled /pay?delay_ms=5000   #2 EffectCompleted op 1 -> http 200 (~5.9 s)   #3 WaitResolved   #4 RunCompleted
```

## Verification

- `cargo test -p ostro-server --test effects_seam --locked -- --test-threads=1`: **test result: ok. 6 passed** (32 s).
- `cargo test -p ostro-server --features failpoints --test effects_seam --locked -- --test-threads=1 --nocapture`: **test result: ok. 8 passed** (14 s).
- `cargo clippy -p ostro-server --all-targets --all-features --locked -- -D warnings`: clean. `cargo clippy -p ostro-server --all-targets --locked -- -D warnings`: clean. `cargo fmt -p ostro-server --check`: clean.
- **Not run:** `bash scripts/ci-local.sh --no-musl`. Three other executors and a full ci-local run were active at the same time. This plan adds a single test file and touches no product code, Cargo files or shared files.

## Deviations from Plan

**1. [Plan defect] An evicted run with an in-flight effect is never `Scheduled`.**
- The plan expects the activation to suspend so that the run becomes `Scheduled`. It does not. An in-flight effect is hot-waitable (`host/effects.rs` `hot_waitable_op`). When the window elapses, the activation evicts with `HotWaitElapsed`, and the executor maps that to `Reactivate` (`executor.rs`). The run stays `Running` and parks again.
- My first attempt waited for `RunStatus::Scheduled` and timed out with the run already `Completed`.
- The tests now look for the eviction itself: a finished job record with `ActivationOutcome::Evicted{HotWaitElapsed}` (`TestServer::jobs_of`). The rest is unchanged: the crash happens while `/pay` is in flight and after an eviction, followed by the restart wake and the single charge.
- This is product behaviour as designed (ENG-09), not a bug.

**2. [Rule 3 - Blocking, test harness] The mock's `/pay` key stayed in progress forever after a crash.**
- When the client connection closes, the MockProvider's hyper server drops the `/pay` handler future. In `evicted_inflight_wakes`, `crash()` closes that connection mid-sleep, so the key never reached `Done`. Every re-drive got 409 `in_progress`, and the run finished after the 40-attempt retry budget with http 409 (`EffectCompleted op 1 -> http 409`).
- The bridge behaved correctly throughout: same key, retried, one charge.
- `crates/ostro-testkit` is outside this plan's files, so the test file now has a small `detached_proxy` TCP forwarder in front of the mock. It keeps the upstream connection open until the upstream response ends, which models a provider that finishes a request whose client vanished.
- With it, the re-drive gets 409 while the first request is still sleeping, retries, and then gets the replayed 200.
- Only `evicted_inflight_wakes` uses the proxy. The egress rule is the proxy's loopback origin.

**3. Replay checks use an offline re-activation.**
- "Replaying the run (evict + re-activate)" and "forced re-activation" of an already completed run cannot be triggered in the server: a terminal run is never dispatched.
- `large_outcome_blob` and `completed_journal_no_contact` replay the stored journal on a fresh engine `Runtime` (`ostro_engine::testkit::replay_journal`, `FakeSink`, no effect driver). They assert identical output, 0 appended events and no provider contact.
- `completed_journal_no_contact` also covers the in-server path: real `HotWaitElapsed` re-activations replay a completed `/echo` without re-sending it.

**4. The non-failpoint intent check uses an upper bound.** The MockProvider records no receive timestamps, so the test compares the `EffectScheduled` ts against the wall time of the first poll that sees `requests_to("/pay") > 0`. The strict ordering is proven by the failpoint variant.

**5. TDD commits.** The plan marks both tasks TDD. The product code already existed (06-14), so this is test-only work. Both tasks landed in one commit (`test(06-15)`), which keeps the CAS commits down while other plans run in parallel.

## Commits

| Task | Commit | Description |
|------|--------|-------------|
| 1+2 | f1a1eca | test(06-15): in-process effect seam e2e (intent before I/O, journal delivery, B1, B2, crash wake) |

## Self-Check: PASSED

- FOUND: crates/ostro-server/tests/effects_seam.rs on main (792 lines)
- FOUND: f1a1eca on main
