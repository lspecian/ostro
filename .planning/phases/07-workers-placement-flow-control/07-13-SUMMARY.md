---
phase: 07-workers-placement-flow-control
plan: 13
subsystem: ostro-server workers (auto placement spill)
tags: [spill, hysteresis, placement, auto, getrusage, load-sampler, WORK-03]
requires:
  - phase: 07-workers-placement-flow-control
    provides: "07-02 SpillHandle/start fixed signatures + [workers.spill] config + meta SpillPolicy; 07-05 QueueGauges (ready_unblocked, oldest_ready_at_ms, running_on_workers); 07-08 meta policy reads; 07-09 registry (workers enabled)"
provides:
  - "SpillThresholds (D-W9 defaults, from_config, with_policy) and the pure SpillController state machine"
  - "LoadSampler trait, ProcessSampler (getrusage CPU, embedded slots, Wasm budget, lag/backlog/workers from QueueGauges), CpuMeter"
  - "SpillHandle: spilling(q), note_overflow(q, now_ms), last_overflow(q), changed() watch, set/remove, spilling_queues()"
  - "SpillService::tick + spill::run + spill::start (1 s sampler), spill::auto_queues"
  - "ostro_placement_transitions_total{queue,to}, ostro_placement_spilling{queue}"
affects: [07-12, 07-16, 07-18, 07-22]
tech-stack:
  added: []
  patterns:
    - "Pure state machine fed (sample, now_ms) from the injected clock; the service owns no time source of its own"
    - "Run loop sleeps to a deadline computed from the injected clock (no interval); a lagging deadline skips ahead"
key-files:
  created:
    - crates/ostro-server/tests/placement_spill.rs
  modified:
    - crates/ostro-server/src/workers/spill.rs
key-decisions:
  - "QueuePolicy.spill replaces the node defaults wholesale (its fields are non-optional u8/u64)"
  - "backlog = QueueGauge.ready_unblocked (admissible work), so a paused or flow-blocked queue counts as empty"
  - "Overflow window = the last cooldown_ms before the sample (an attempt at t blocks exit until t + cooldown_ms has passed)"
  - "Candidate queues = QueueGauges keys plus the queues already tracked; auto = QueuePolicy.placement == PLACEMENT_AUTO, or [workers] default_placement = auto when the policy has no placement"
  - "spill::start returns None when [workers] enabled = false"
  - "Added SpillHandle::changed() -> watch::Receiver<u64> (transition counter) for NodePlacement::changed(); the fixed 07-02 signatures are unchanged"
metrics:
  duration: ~45 min
  completed: 2026-11-21
---

# Phase 7 Plan 13: Spill controller Summary

**`auto` queues now have a per-queue hysteresis controller. It is fed once a second by real signals: getrusage CPU over `available_parallelism`, embedded slot use, the Wasm memory budget, and queue lag/backlog/worker leases from `QueueGauges`. Entry uses the D-W9 thresholds; exit follows the B6 rule (minimum dwell, a calm window below entry × hysteresis, and workers drained or no backlog). The per-queue state is published through `SpillHandle::spilling(q)` with a `changed()` watch, transitions are logged and counted, and every window is measured on the injected clock.**

## Commits

1. `03159aa` feat(07-13): spill controller state machine
2. `639d945` feat(07-13): load sampler and spill service

## Thresholds as implemented

| Signal | Entry | Exit line (× 0.85) | Notes |
|---|---|---|---|
| CPU | > 70 % for ≥ `sustain_ms` (10 s) | < 59.5 % | a dip at or below 70 % resets the sustain timer |
| Embedded slots | > 90 % | < 76.5 % | immediate |
| Wasm memory | > 85 % | < 72.25 % | immediate |
| Queue lag | > 5 000 ms | < 4 250 ms | immediate |

- Entry: any signal triggers. When several fire, the reported signal follows the order slots, mem, lag, cpu.
- Exit (`Spilling → Embedded`) needs all of the following:
  - `now − spilling_since ≥ cooldown_ms` (30 s);
  - every signal below its exit line continuously for `cooldown_ms`, where any excursion restarts the window;
  - `(running_on_workers == 0 && no overflow attempt in the last cooldown_ms) || backlog == 0`.
- Exit lines are compared strictly (`<`), entry lines strictly (`>`).
- Node defaults come from `[workers.spill]` (`SpillThresholds::from_config`, equal to D-W9 by default; tested). A `QueuePolicy.spill` replaces them for that queue. A threshold change keeps the current state and windows.

## getrusage wrapper

`process_cpu_time()` in `spill.rs` holds the only `unsafe` block. It does `MaybeUninit::<libc::rusage>::zeroed()` + `libc::getrusage(RUSAGE_SELF, ptr)` and calls `assume_init` only on rc 0, with a SAFETY comment. It returns `ru_utime + ru_stime` as a `Duration`. `tv_sec`/`tv_usec` go through `u64::try_from`, so the code works on both Linux (i64 usec) and macOS (i32 usec).

## Sampler accuracy notes

- `CpuMeter` computes `Δ(user+sys) / (Δwall × available_parallelism)`, clamped to 0..=1. The first sample covers the time since construction.
  - The wall denominator is `std::time::Instant` (`// clock-exempt: process CPU-time delta (getrusage)`). It is only the denominator of the ratio, never a spill window.
  - The kernel may account user/sys time at tick granularity. Over a 1 s period the reading should be within a few percent (not measured precisely; the test only asserts > 0 after a 200 ms busy thread). The value is normalised, so on a 32-core box one fully busy thread reads about 3 %.
  - The signal is process-wide (embedded Wasm and server threads alike). Every `auto` queue sees the same value.
- Slots: `1 − min(available, total) / total`, where total is `[executor] slots`. 0 slots gives 0.
- Memory: `budget.used() / budget.limit()` of `runtime.parked().budget()`. A 0 limit gives 0.
- Lag: `clock.now_ms() − min(oldest_ready_at_ms)` across partitions, 0 when nothing is ready.
- Backlog: summed `ready_unblocked`. Running on workers: summed `running_on_workers`.
- The process-wide signals and the gauge snapshot are read once per tick (`LoadSampler::begin`). Per-queue values come from that snapshot.

## Transition metrics and logs

- `ostro_placement_transitions_total{queue, to = spilling|embedded}`: counter, +1 per transition, including the forced clear when a spilling queue leaves `auto`.
- `ostro_placement_spilling{queue}`: gauge, 0/1, set on every transition and to 0 when a queue is dropped.
- Logging: one `info!("placement spill transition")` per transition. It carries queue, to, signal (`cpu|slots|mem|lag|cooldown`) and the sample values (cpu, slots, mem, lag_ms, backlog, running_on_workers).

## Service shape

- `spill::start(ctx)` runs when `[workers] enabled`.
  - It builds `ProcessSampler(ctx.clock, shared.gauges, shared.embedded_slots, [executor] slots, runtime.parked().budget())`.
  - It spawns `run(SpillService, auto, ctx.clock, ctx.shutdown, 1 s)`.
- Each tick:
  - finds the candidate queues (gauge keys plus tracked queues);
  - runs `auto_queues` (one `meta.read`);
  - drops controllers for queues that left `auto` (`SpillHandle::remove`, which clears overflow and bumps the watch if the queue was spilling);
  - then `begin()`, plus one sample and one `observe` per auto queue.
- `SpillHandle::last_overflow(q)` is merged into each sample.
- The loop sleeps to `next_ms` computed from the injected clock (`next += period`; if behind, `next = now`; minimum 1 ms). `grep "interval(" spill.rs` is empty.
- With no auto queues a tick costs one gauge snapshot plus a meta read of the candidate names. The sampler is not called.

## Tests

`tests/placement_spill.rs` has 20 tests, all green. The controller cases drive `ManualClock` in 1 s ticks.
- Spike: exactly 2 transitions, with exit at calm + 30 s.
- Zero workers under overload for 3 × cool-down: 1 transition.
- Steady 95 % slots, no backlog: ≤ 1 transition (enter only).
- Sustained overload with workers for 3 × cool-down: 1 transition, still Spilling. After the drain it exits once.
- Calm signals with backlog and busy workers: stays Spilling. It exits a cool-down after the last overflow attempt.
- Minimum dwell: spikes of 1, 2, 5 and 29 s.
- CPU oscillating 65/75 % every 2 s for 2 min: 0 transitions.
- CPU enters exactly at sustain.
- Slots oscillation 95/80 after entry: exits exactly at clean + 30 s.
- An excursion restarts the window.
- Per-queue overrides: entry, exit line, cool-down and sustain.
- Exit lines are entry × 0.85 for each of the 4 signals.
- `CpuMeter` reads > 0 after a 200 ms busy thread.
- `ProcessSampler` reads slots, memory, lag, backlog and workers across 2 partitions.
- `SpillHandle` state, overflow and watch.
- Service tick flips the handle and bumps the watch exactly twice.
- The service merges overflow attempts from the handle (exit at t = 90 for the last attempt at t = 59).
- A queue leaving `auto` is dropped.
- The spawned `run` loop reaches enter + exit, with the watch at 2.
- `auto_queues` over a real `MetaHandle`: policy placement, `default_placement = auto`, overrides.

`cargo test -p ostro-server --lib -- workers::spill`: 3 unit tests (exit lines, config defaults, immediate signals).

## Deviations from Plan

1. **[Plan adaptation] Service test time.** ostro-server has no tokio `test-util`, so `tokio::time::pause()` is unavailable. It is also disallowed alone by F5. The service test therefore uses a `ManualClock` that a scripted sampler advances by 1 s per tick, and the run loop gets a 1 ms period. Every deadline is then already due, and the loop runs at its 1 ms floor while the controller sees 1 s clock steps.
2. **[Plan adaptation] `LoadSampler::begin()`** was added as a default no-op hook, so process-wide signals (CPU delta, slots, memory, gauge snapshot) are read once per tick, not once per queue. Without it the getrusage delta between consecutive queues would be about 0.
3. **[Interface addition] `SpillHandle::changed()`, `last_overflow()`, `set()`, `remove()`, `spilling_queues()`** and `pub fn auto_queues` are new. The fixed 07-02 signatures (`new`, `spilling`, `note_overflow`, `start`) are unchanged.

## Hand-offs

- **07-12 (NodePlacement):**
  - Merge `ctx.shared.spill.changed()` (a `watch::Receiver<u64>` transition counter) into `Placement::changed()`.
  - Call `spill.note_overflow(queue, ctx.clock.now_ms())` on every overflow attempt (spilling, embedded full, a worker wanted), whether or not a credit was found.
  - `spilling(queue)` is one ArcSwap load plus a map lookup and is safe on the reserve path.
- **07-16 (demand):** `spill.spilling(q)` / `spilling_queues()`.
- **07-18 (e2e), deterministic spill triggers:**
  - Slots: `[executor] slots = 1` (or 2), a `QueuePolicy { placement: Some(PLACEMENT_AUTO), spill: Some(SpillPolicy { cpu_pct: 100, slots_pct: 50, mem_pct: 100, lag_ms: u64::MAX / 2, sustain_ms: 0, cooldown_ms: 2_000, hysteresis_pct: 50 }) }`, and more concurrent long runs than slots. Spill happens on the first 1 s tick that sees the slot full. With no backlog and drained workers, exit comes ~2 s after the last calm sample.
  - Lag: a small `lag_ms` (e.g. 200) with a capacity-1 queue.
  - Disable the CPU trigger with `cpu_pct: 100` (the signal is clamped to ≤ 1.0 and the comparison is strict).
  - Assert on `ostro_placement_transitions_total{queue,to}` or `SpillHandle::spilling(q)`.
  - The service always ticks every 1 s of real time (`SAMPLE_PERIOD`). With a `ManualClock` injected into the server, windows advance only when the test advances the clock.
- **07-22:** clock grep: `spill.rs` reads time only via `clock.now_ms()`. The one `Instant::now` is the `clock-exempt` getrusage wall denominator, and the one `unsafe` is the getrusage wrapper.

## Self-Check: PASSED

- FOUND: crates/ostro-server/src/workers/spill.rs, crates/ostro-server/tests/placement_spill.rs
- FOUND: 03159aa, 639d945
