---
phase: 10-developer-cli-test-kits
plan: 13
subsystem: dev-time-skipping
tags: [sdk-06, devtime, auto-skip, idle-detector, skip-barrier, d-w26]
requires:
  - phase: 10-developer-cli-test-kits
    provides: "10-10 DevClock (skip_guarded barrier, dispatcher clock wake, granting gauge), guests/p10-time, tests/time_skipping.rs harness"
provides:
  - "ostro_server::devtime::DevTime (idle snapshot, fenced store view, auto_step, spawn_auto, ApiWriteCounter layer body)"
  - "DispatcherStats::idle_stats -> IdleStats {leases, granting}"
  - "ostro_partition::leader::timers::earliest_due_ms(reader, exclude_expire, scan_limit)"
  - "RunWatchers::pending_waiters / pending_guard"
  - "GET /v1/dev/time idle + next_due_ms; lease-safe POST /v1/dev/time/skip (5 s busy wait, 503 busy)"
  - "DevClock::attach_time / time / set_skip_hook (test hook at the skip instant)"
  - "ServerHandle::dev_time; auto loop joined first at shutdown"
affects: [10-14, 10-17, 10-20]
tech-stack:
  added: []
  patterns:
    - "idle inputs read from the store (source of truth), fenced by per-partition applied offsets == log_end; scan cached per fence"
    - "auto skip = idle snapshot -> 20 ms quiescence -> identical snapshot -> atomics-only re-check under the skip barrier's write guard"
key-files:
  created:
    - crates/ostro-server/src/devtime.rs
    - crates/ostro-server/tests/time_skipping_auto.rs
  modified:
    - crates/ostro-partition/src/leader/dispatcher.rs
    - crates/ostro-partition/src/leader/timers.rs
    - crates/ostro-server/src/control/watch.rs
    - crates/ostro-server/src/control/dev.rs
    - crates/ostro-server/src/devclock.rs
    - crates/ostro-server/src/lib.rs
    - crates/ostro-server/src/server.rs
    - crates/ostro-server/tests/time_skipping.rs
    - crates/ostro/tests/dev_watch.rs
key-decisions:
  - "Idle is read from the store (ready/outbox/timers tables) plus atomics, not from the dispatcher's or timer service's in-memory state: the apply thread publishes `applied` before it broadcasts actions, so an in-memory view always lags; the store is exact at a given applied offset"
  - "Every store read is fenced: applied == log_end per partition before and after the scan; the barrier re-check compares the offsets (watch borrows) instead of re-reading the store (W-A)"
  - "The timer window cannot provide the skip target (it only reaches now + 60 s); earliest_due_ms scans the timers table in due order, EXPIRE_OP skipped, at most 4096 rows"
  - "A new dispatcher lease is reported to the shared leases gauge before its grant task spawns, so a grant is always visible as a lease, a granting count, or both"
  - "Executor-level attempt-failure retry backoff ([executor] retry_backoff) stays deliberately real time: the lease is held through it, so the node is busy and no skip (auto or manual) crosses it (D-W26)"
  - "Lease TTL / heartbeat stay real time (10-06); D-P10-6's 'lease deadlines on the injected clock' is superseded: skips can never expire a lease, and no skip happens while one is held"
  - "Schedules (Phase 8 cron) are not skip targets: they live in meta; they fire as the clock passes them under their catch-up policy"
requirements-completed: []
duration: ~3h
completed: 2026-11-23
---

# Phase 10 Plan 13: Automatic time skipping (server half 2) Summary

**An idle detector (`DevTime`) and the D-P10-6 auto trigger: an `ostro dev --ephemeral --time-skipping` node fast-forwards its dev clock to the next due timer or step backoff when it is fully idle and a result long-poll is waiting. Manual skips are lease-safe. A 3-day sleep completes through `result --wait` in about 60 ms of wall time. The race suite passes 100 iterations by default and 1000 in the gate.**

SDK-06 server side is complete. The per-language kits (10-14/10-17) remain.

## Commits
- 0ab9421 feat(10-13): DevTime idle detector, waiter count and lease-safe manual skip
- 97e4d93 feat(10-13): auto time skipping on idle with a pending result waiter; race suite

## Idle input sources
| Input | Source (file / fn) | Read |
|-------|--------------------|------|
| Apply caught up (`applied == log_end`) | `ostro-partition/src/machine.rs` `MachineHandle::applied()` (watch) + `ostro-log` `ReplicatedLog::log_end()` | `DevTime::fence` |
| Ready work queued / earliest future `ready_at` | `ostro-partition/src/query.rs` `ready_entries` (the `ready` table) | `DevTime::store_view` |
| Outbox / shuffler pending | `reader.prefix(Table::Outbox, &[], 1)` (rows stay until `OutboxTruncated` applies after delivery) | `DevTime::store_view` |
| Earliest timer, `EXPIRE_OP` excluded | `ostro-partition/src/leader/timers.rs` `earliest_due_ms` (the `timers` table) | `DevTime::store_view` |
| Leases / grants in flight | `ostro-partition/src/leader/dispatcher.rs` `DispatcherStats::idle_stats()` (`leases`, `granting` atomics) | `DevTime::counters` |
| Effects in flight | `ostro-bridge/src/driver.rs` `Bridge::inflight()` (registry length; a short std mutex, no await) | `DevTime::counters` |
| Mutating API requests in flight | `ostro-server/src/devtime.rs` `count_api_writes` layer (REST POST/PUT/PATCH/DELETE; gRPC by method name, reads `Get*/List*/Await*/Fetch*/Watch*/History/Check/Describe*` excluded; `/v1/dev/*` excluded), installed in `server.rs` only with time skipping on | `DevTime::counters` |
| Result waiters | `ostro-server/src/control/watch.rs` `RunWatchers::pending_waiters()` (RAII `pending_guard` around each `await_result` with `wait > 0`) | `DevTime::counters` |

The store scan is cached per applied offsets, so a parked node polled every 10 ms reads the store once per apply. The auto loop also does no store read at all without a waiter. (A first version scanned on every poll; the scans walk tombstones that grow with run history, and idle CPU rose linearly. With the cache, idle CPU stays flat over 500 iterations.)

**Skip protocol (auto):**
1. Cheap gate: waiters ≥ 1, no activation, API write or effect.
2. Full snapshot `s1`: idle, waiter, `next_due > now`.
3. Sleep 20 ms (real time).
4. Snapshot `s2`: still idle, same target, same offsets.
5. `skip_guarded`: under the write guard, re-check counters, waiter and offsets == `s2` (atomics and watches only), then skip to `next_due`.

**Manual:** wait up to 5 s for zero activations and API writes (else 503 `busy: N activations in flight; retry`, with the API write count appended when non-zero). Then `skip_guarded` re-checks the activations under the guard. If it finds one, it goes back to waiting within the same deadline.

**Schedules as skip targets:** no (see key decisions).

## Wall times (`--test-threads=1`, load ~9)
| Scenario | Wall |
|----------|------|
| auto `sleep_for` 3 d via `result?wait=30s` | 60–63 ms |
| auto `backoff_step` (2 × 1 h backoffs, 3 attempts) | 71–76 ms |
| auto `wait_forever` → `timed_out` at 48 h | 53 ms |
| `time_skipping_auto` default suite (9 tests) | 29 s |
| 10-10 manual suite, unchanged | 2.3 s |

## Race results
| Run | signal_before_await p50 / p99 / max | start_then_await p50 / p99 / max |
|-----|------------------------------------|----------------------------------|
| 100 iterations (default suite) | 72.9 / 81.5 / 83.4 ms | 64.7 / 68.0 / 117.3 ms |
| 1000 iterations (`race_gate_1000`, ignored) | 71.2 / 125.2 / 187.8 ms | 69.9 / 133.6 / 203.9 ms |

The gate took 156 s in total. Every iteration completed with the right payload. In each one the signal was matched before the sleep's timer, and `TimerStarted.due == RunStarted.accepted_at + 1 d (±1 s)` with `accepted_at ≤ TimerStarted.ts`. D-W26 held: the DevClock skip hook recorded `IdleStats {leases: 0, granting: 0}` at every skip in every test (about 2000 skips in the gate).

Gate command (also for 10-VALIDATION): `cargo test -p ostro-server --locked --test time_skipping_auto -- --ignored race_gate_1000 --nocapture`.

## Tests run
| Command | Result |
|---------|--------|
| `cargo test -p ostro-server --test time_skipping_auto -- --test-threads=1` | 9 passed, 1 ignored |
| `cargo test -p ostro-server --test time_skipping_auto -- --ignored race_gate_1000` | 1 passed (156 s) |
| `cargo test -p ostro-server --test time_skipping -- --test-threads=1` | 7 passed |
| `cargo test -p ostro-server --lib -- devtime devclock control::watch` | 9 passed |
| `cargo test -p ostro-partition --locked` | all binaries ok, no panic (slow proptests: snapshot_tail 740 s, trim_safety 492 s) |
| `cargo test -p ostro-server --test leases --test flow_control --test dev_surface --test executor --test executor_phase5` | 1 + 10 + 4 + 12 + 8 passed |
| `cargo test -p ostro --test time_skipping_cli --test dev_watch -- --test-threads=1` | 8 + 5 passed (incl. the new watch + time-skipping test) |
| `cargo clippy -p ostro-server -p ostro-partition --all-targets -- -D warnings`; `cargo fmt --all --check` | clean |
| Plan clock grep gate | no hits |

**Full `scripts/ci-local.sh --no-musl` was not run** (it takes about 3 h, and 4 parallel agents share the machine).

## Deviations from Plan
1. **[Rule 1 - Design] Store-based idle inputs instead of dispatcher/timer `watch` publications.**
   - The plan asked the dispatcher to publish `ready`/`earliest ready_at`, and the timer service its window minimum, through `watch` channels.
   - Both would lag the store. The apply thread publishes `applied`, then acks, then broadcasts the actions, so an in-memory view is behind by one broadcast.
   - The timer window also cannot see multi-day timers (it spans `now + 60 s`).
   - I read the tables instead, fenced by applied offsets. The barrier check stays atomics/watches-only (W-A): it compares offsets, it does not re-read the store.
   - `dispatcher.rs` gets `IdleStats`/`idle_stats()` and an earlier lease report. `timers.rs` gets the read-only `earliest_due_ms`.
2. **[Rule 1 - Bug] Idle CPU growth (found by the 1000-iteration gate).**
   - Polling scans every 10 ms slowed iterations from ~70 ms to ~4.5 s after ~1500 runs.
   - Fixed with the cheap waiter gate and the per-fence cache (see above).
3. **D-W26 assertion "immediately after every skip".** As in 10-10, the hook runs at the skip instant under the write guard, right before the offset moves. Right after a skip, the woken runs legitimately take leases.
4. **Files outside `files_modified`:**
   - `devclock.rs`: `attach_time`/`time`/`set_skip_hook`; `dev.rs` reaches `DevTime` through the control plane's `DevClock`.
   - `tests/time_skipping.rs`: the 10-10 assertion `idle == false` became `idle == true` for an empty node.
   - `crates/ostro/tests/dev_watch.rs`: the cheap `--watch` + `--time-skipping` test the coordinator asked for.
5. **`api_write_in_flight_blocks_skip` uses a real slow request**: raw TCP, headers first, body after 1.5 s. It needs no test-only handler.
6. **Worktree and target dir** follow the executor brief (scratchpad worktree, `target/slot-2`).

## Idle inputs not observed (follow-ups)
- **Schedules (cron):** not skip targets (meta-backed). 10-17 docs/testing.md should say that schedules fire as skipped time passes them, under their catch-up policy.
- **Timer scan limit:** if more than 4096 `EXPIRE_OP` rows precede the first real timer in one partition, `earliest_due_ms` returns `None` and the node does not auto-skip. This is safe but stalls. It is unlikely in a dev session (retention timers fire as days are skipped).
- **Residual window:**
  - A dispatcher entry with no store row (a stale entry after a cancel) could start a grant right at a skip.
  - The grant waits on the barrier, then runs at the new time. Apply rejects stale grants.
  - It was never observed in about 2200 skips.
- **macOS `ostro dev --ephemeral` 1 s startup budget (no tmpfs):** not addressed here.
  - An fsync-free ephemeral mode needs a durability switch threaded through the WAL (`ostro-log` `partition_log.rs`, `recovery.rs`), the partition store and checkpoints (`ostro-partition` `store/mod.rs`, `machine.rs` `persist(SyncAll)`, `node.rs`, `leader/snapshot.rs`, `store/snapshot.rs`), meta (`ostro-meta` `handle.rs`) and blobs (`ostro-blob` `lib.rs`), plus the `[node]`/dev config.
  - That is five crates outside this plan's `files_modified`, with crash-safety review in each. It needs its own plan.
  - Skipping fsync is safe for ephemeral data, which is disposable.
- **`--watch` + `--time-skipping`:** covered by `dev_watch.rs::watch_with_time_skipping_deploys_and_stays_idle` (needs the wasm32-wasip2 toolchain like the rest of `dev_watch.rs`).

## Self-Check: PASSED
- Files exist: devtime.rs, tests/time_skipping_auto.rs, 10-13-SUMMARY.md.
- Commits 0ab9421 and 97e4d93 are on main.
