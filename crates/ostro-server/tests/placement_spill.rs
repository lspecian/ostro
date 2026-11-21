//! Spill hysteresis (plan 07-13, WORK-03, D-W9, plan-check B6).
//!
//! The controller tests feed one fake sample per second; time comes from a partition
//! `ManualClock` (Phase 10 F5), never from the wall clock.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use ostro_engine::limits::NodeMemoryBudget;
use ostro_meta::records::{PLACEMENT_AUTO, PLACEMENT_EMBEDDED};
use ostro_meta::{MetaCommand, MetaHandle, MetaRecord, QueuePolicy, SpillPolicy};
use ostro_partition::{Clock, ManualClock, QueueGauge, QueueGauges};
use ostro_server::workers::spill::{
    self, CpuMeter, LoadSample, LoadSampler, ProcessSampler, Signal, SpillController, SpillHandle,
    SpillService, SpillState, SpillThresholds, Transition,
};
use ostro_types::PartitionId;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

const T0: u64 = 1_700_000_000_000;
const COOLDOWN: u64 = 30_000;

/// Run `secs` one-second ticks: sample `f(second)` and observe it at the clock's time.
fn drive(
    c: &mut SpillController,
    clock: &ManualClock,
    secs: u64,
    mut f: impl FnMut(u64) -> LoadSample,
) -> Vec<Transition> {
    let mut out = Vec::new();
    for s in 0..secs {
        if let Some(t) = c.observe(f(s), clock.now_ms()) {
            out.push(t);
        }
        clock.advance(1_000);
    }
    out
}

fn ctl() -> (SpillController, ManualClock) {
    (
        SpillController::new(SpillThresholds::default()),
        ManualClock::new(T0),
    )
}

fn overload(clock: &ManualClock, running_on_workers: u64) -> LoadSample {
    LoadSample {
        cpu: 0.5,
        slots: 1.0,
        backlog: 25,
        running_on_workers,
        last_overflow_ms: Some(clock.now_ms()),
        ..Default::default()
    }
}

fn idle() -> LoadSample {
    LoadSample {
        cpu: 0.1,
        slots: 0.1,
        ..Default::default()
    }
}

#[test]
fn spike_causes_exactly_two_transitions() {
    let (mut c, clock) = ctl();
    let ts = drive(&mut c, &clock, 120, |s| {
        if (10..15).contains(&s) {
            LoadSample {
                slots: 1.0,
                backlog: 3,
                ..Default::default()
            }
        } else {
            idle()
        }
    });
    assert_eq!(ts.len(), 2, "{ts:?}");
    assert_eq!(
        (ts[0].to, ts[0].signal),
        (SpillState::Spilling, Signal::Slots)
    );
    assert_eq!(ts[0].at_ms, T0 + 10_000);
    assert_eq!(
        (ts[1].to, ts[1].signal),
        (SpillState::Embedded, Signal::Cooldown)
    );
    // Calm from second 15: exit exactly one full cool-down later.
    assert_eq!(ts[1].at_ms, T0 + 15_000 + COOLDOWN);
    assert_eq!(c.state(), SpillState::Embedded);
}

#[test]
fn zero_workers_under_overload_enters_once() {
    let (mut c, clock) = ctl();
    let ts = drive(&mut c, &clock, 3 * COOLDOWN / 1_000, |_| {
        overload(&clock, 0)
    });
    assert_eq!(ts.len(), 1, "{ts:?}");
    assert!(c.spilling());
}

#[test]
fn steady_high_slots_without_backlog_never_exits() {
    let (mut c, clock) = ctl();
    let ts = drive(&mut c, &clock, 3 * COOLDOWN / 1_000, |_| LoadSample {
        cpu: 0.2,
        slots: 0.95,
        ..Default::default()
    });
    assert!(ts.len() <= 1, "{ts:?}");
    assert!(ts.iter().all(|t| t.to == SpillState::Spilling));
}

#[test]
fn sustained_overload_with_workers_stays_spilling_then_exits_once() {
    let (mut c, clock) = ctl();
    let ts = drive(&mut c, &clock, 3 * COOLDOWN / 1_000, |_| {
        overload(&clock, 4)
    });
    assert_eq!(ts.len(), 1, "{ts:?}");
    assert!(c.spilling(), "B6: still spilling under sustained overload");

    // The overload ends: workers drain over 10 s, then nothing is left.
    let ts = drive(&mut c, &clock, 120, |s| LoadSample {
        cpu: 0.1,
        slots: 0.5,
        running_on_workers: 10u64.saturating_sub(s),
        ..Default::default()
    });
    assert_eq!(ts.len(), 1, "{ts:?}");
    assert_eq!(ts[0].to, SpillState::Embedded);
    assert!(!c.spilling());
}

#[test]
fn calm_signals_with_backlog_and_busy_workers_keep_spilling() {
    let (mut c, clock) = ctl();
    assert_eq!(drive(&mut c, &clock, 1, |_| overload(&clock, 2)).len(), 1);
    // Signals calm, but workers still serve a backlog: condition 3 fails.
    let ts = drive(&mut c, &clock, 3 * COOLDOWN / 1_000, |_| LoadSample {
        cpu: 0.1,
        slots: 0.5,
        backlog: 7,
        running_on_workers: 3,
        last_overflow_ms: Some(clock.now_ms()),
        ..Default::default()
    });
    assert!(ts.is_empty(), "{ts:?}");
    // Overflow stops and the workers drain: exit once the last attempt is a cool-down old.
    let drained_at = clock.now_ms();
    let ts = drive(&mut c, &clock, 60, |_| LoadSample {
        cpu: 0.1,
        slots: 0.5,
        backlog: 7,
        last_overflow_ms: Some(drained_at - 1_000),
        ..Default::default()
    });
    assert_eq!(ts.len(), 1, "{ts:?}");
    assert!(ts[0].at_ms >= drained_at - 1_000 + COOLDOWN);
}

#[test]
fn exit_never_before_minimum_dwell() {
    for spike_secs in [1u64, 2, 5, 29] {
        let (mut c, clock) = ctl();
        let ts = drive(&mut c, &clock, 200, |s| {
            if s < spike_secs {
                LoadSample {
                    lag_ms: 60_000,
                    ..Default::default()
                }
            } else {
                LoadSample::default()
            }
        });
        assert_eq!(ts.len(), 2, "{ts:?}");
        assert!(ts[1].at_ms - ts[0].at_ms >= COOLDOWN, "{ts:?}");
    }
}

#[test]
fn cpu_oscillation_around_entry_never_transitions() {
    let (mut c, clock) = ctl();
    // 65 % and 75 % alternating every 2 s for 2 minutes: never 10 s above 70 %.
    let ts = drive(&mut c, &clock, 120, |s| LoadSample {
        cpu: if (s / 2) % 2 == 0 { 0.65 } else { 0.75 },
        ..Default::default()
    });
    assert!(ts.is_empty(), "{ts:?}");
    assert!(!c.spilling());
}

#[test]
fn cpu_enters_only_after_sustain() {
    let (mut c, clock) = ctl();
    let ts = drive(&mut c, &clock, 30, |_| LoadSample {
        cpu: 0.8,
        ..Default::default()
    });
    assert_eq!(ts.len(), 1);
    assert_eq!(ts[0].signal, Signal::Cpu);
    assert_eq!(ts[0].at_ms, T0 + 10_000);
}

#[test]
fn slots_oscillation_after_entry_stays_until_clean_cooldown() {
    let (mut c, clock) = ctl();
    // 95 % / 80 % every 2 s for a minute: 80 % is above the 76.5 % exit line.
    let ts = drive(&mut c, &clock, 60, |s| LoadSample {
        slots: if (s / 2) % 2 == 0 { 0.95 } else { 0.80 },
        ..Default::default()
    });
    assert_eq!(ts.len(), 1, "{ts:?}");
    assert!(c.spilling());
    let clean_from = clock.now_ms();
    let ts = drive(&mut c, &clock, 60, |_| LoadSample {
        slots: 0.5,
        ..Default::default()
    });
    assert_eq!(ts.len(), 1, "{ts:?}");
    assert_eq!(ts[0].at_ms, clean_from + COOLDOWN);
}

#[test]
fn any_excursion_restarts_the_exit_window() {
    let (mut c, clock) = ctl();
    drive(&mut c, &clock, 1, |_| overload(&clock, 0));
    // Calm for 20 s, one 2 s excursion above the exit line (not the entry line), calm.
    let start = clock.now_ms();
    let ts = drive(&mut c, &clock, 120, |s| LoadSample {
        slots: if (20..22).contains(&s) { 0.8 } else { 0.3 },
        ..Default::default()
    });
    assert_eq!(ts.len(), 1, "{ts:?}");
    assert_eq!(ts[0].at_ms, start + 22_000 + COOLDOWN);
}

#[test]
fn per_queue_overrides_change_thresholds() {
    let policy = SpillPolicy {
        cpu_pct: 50,
        slots_pct: 40,
        mem_pct: 60,
        lag_ms: 1_000,
        sustain_ms: 2_000,
        cooldown_ms: 5_000,
        hysteresis_pct: 50,
    };
    let th = SpillThresholds::default().with_policy(Some(&policy));
    assert_eq!(th.slots_pct, 40);
    assert_eq!(th.cooldown_ms, 5_000);
    assert_eq!(
        SpillThresholds::default().with_policy(None),
        SpillThresholds::default()
    );

    // 45 % slots: no spill under D-W9, a spill under the override.
    let sample = LoadSample {
        slots: 0.45,
        ..Default::default()
    };
    let clock = ManualClock::new(T0);
    let mut dflt = SpillController::new(SpillThresholds::default());
    assert!(drive(&mut dflt, &clock, 10, |_| sample).is_empty());
    let mut over = SpillController::new(th);
    let ts = drive(&mut over, &clock, 10, |_| sample);
    assert_eq!(ts.len(), 1);

    // Exit line is 40 % × 0.5 = 20 %: 25 % keeps it spilling, 15 % exits after 5 s.
    assert!(
        drive(&mut over, &clock, 20, |_| LoadSample {
            slots: 0.25,
            ..Default::default()
        })
        .is_empty()
    );
    let from = clock.now_ms();
    let ts = drive(&mut over, &clock, 20, |_| LoadSample {
        slots: 0.15,
        ..Default::default()
    });
    assert_eq!(ts.len(), 1);
    assert_eq!(ts[0].at_ms, from + 5_000);

    // CPU sustain override: 2 s at 55 %.
    let mut cpu = SpillController::new(th);
    let start = clock.now_ms();
    let ts = drive(&mut cpu, &clock, 5, |_| LoadSample {
        cpu: 0.55,
        ..Default::default()
    });
    assert_eq!(ts.len(), 1);
    assert_eq!(ts[0].at_ms, start + 2_000);
}

#[test]
fn exit_lines_are_entry_times_085_for_each_signal() {
    // Just above each exit line keeps the queue spilling; just below lets it go.
    type Mk = fn(f64) -> LoadSample;
    let cases: [(Mk, f64); 4] = [
        (
            |v| LoadSample {
                cpu: v,
                ..Default::default()
            },
            0.70 * 0.85,
        ),
        (
            |v| LoadSample {
                slots: v,
                ..Default::default()
            },
            0.90 * 0.85,
        ),
        (
            |v| LoadSample {
                mem: v,
                ..Default::default()
            },
            0.85 * 0.85,
        ),
        (
            |v| LoadSample {
                lag_ms: v as u64,
                ..Default::default()
            },
            5_000.0 * 0.85,
        ),
    ];
    for (mk, line) in cases {
        let eps = if line > 1.0 { 10.0 } else { 0.005 };
        let (mut c, clock) = ctl();
        drive(&mut c, &clock, 1, |_| LoadSample {
            slots: 1.0,
            ..Default::default()
        });
        assert!(drive(&mut c, &clock, 100, |_| mk(line + eps)).is_empty());
        assert_eq!(drive(&mut c, &clock, 100, |_| mk(line - eps)).len(), 1);
    }
}

// ---------------------------------------------------------------------------------
// Task 2: signals, handle, service
// ---------------------------------------------------------------------------------

#[test]
fn cpu_meter_is_a_fraction_and_rises_under_load() {
    let mut m = CpuMeter::new();
    let idle = m.sample();
    assert!((0.0..=1.0).contains(&idle), "{idle}");
    let busy = std::thread::spawn(|| {
        // clock-exempt: test busy loop length.
        #[allow(clippy::disallowed_methods)]
        let start = std::time::Instant::now();
        let mut x = 0u64;
        while start.elapsed() < Duration::from_millis(200) {
            x = std::hint::black_box(x.wrapping_mul(6364136223846793005).wrapping_add(1));
        }
        x
    });
    busy.join().expect("busy thread");
    let v = m.sample();
    assert!(v > 0.0 && v <= 1.0, "cpu after a 200 ms busy loop: {v}");
}

#[test]
fn process_sampler_reads_slots_memory_lag_and_gauges() {
    let clock = ManualClock::new(T0);
    let gauges = Arc::new(QueueGauges::new());
    let slots = Arc::new(Semaphore::new(4));
    let budget = NodeMemoryBudget::new(1_000);
    let _held = Arc::clone(&slots)
        .try_acquire_many_owned(3)
        .expect("permits");
    gauges.set(
        PartitionId(0),
        "q",
        QueueGauge {
            ready: 9,
            ready_unblocked: 7,
            oldest_ready_at_ms: Some(T0 - 6_000),
            running_embedded: 3,
            running_on_workers: 2,
        },
    );
    gauges.set(
        PartitionId(1),
        "q",
        QueueGauge {
            ready_unblocked: 1,
            oldest_ready_at_ms: Some(T0 - 2_000),
            ..QueueGauge::default()
        },
    );
    let mut s = ProcessSampler::new(
        Arc::new(clock.clone()),
        Arc::clone(&gauges),
        slots,
        4,
        Some(Arc::clone(&budget)),
    );
    s.begin();
    let q = s.sample("q");
    assert!((q.slots - 0.75).abs() < 1e-9, "{q:?}");
    assert_eq!(q.mem, 0.0);
    assert_eq!(q.lag_ms, 6_000, "oldest across partitions");
    assert_eq!(q.backlog, 8);
    assert_eq!(q.running_on_workers, 2);
    assert!((0.0..=1.0).contains(&q.cpu));
    let other = s.sample("other");
    assert_eq!((other.lag_ms, other.backlog), (0, 0));
}

#[test]
fn handle_tracks_state_overflow_and_bumps_watch() {
    let h = SpillHandle::new();
    let rx = h.changed();
    assert!(!h.spilling("q"));
    assert!(h.set("q", true));
    assert!(!h.set("q", true), "no change, no bump");
    assert!(h.spilling("q"));
    assert_eq!(h.spilling_queues(), vec!["q".to_owned()]);
    h.note_overflow("q", 50);
    h.note_overflow("q", 20);
    assert_eq!(h.last_overflow("q"), Some(50));
    assert!(h.remove("q"));
    assert!(!h.spilling("q"));
    assert_eq!(h.last_overflow("q"), None);
    assert_eq!(*rx.borrow(), 2);
}

/// Scripted samples by tick number; optionally advances a `ManualClock` 1 s per tick.
struct Script {
    tick: Arc<AtomicU64>,
    clock: Option<ManualClock>,
    f: fn(u64) -> LoadSample,
}

impl LoadSampler for Script {
    fn begin(&mut self) {
        let t = self.tick.fetch_add(1, Ordering::SeqCst);
        if let (Some(c), true) = (&self.clock, t > 0) {
            c.advance(1_000);
        }
    }

    fn sample(&mut self, _queue: &str) -> LoadSample {
        (self.f)(self.tick.load(Ordering::SeqCst) - 1)
    }
}

fn spike(t: u64) -> LoadSample {
    if (10..15).contains(&t) {
        LoadSample {
            slots: 1.0,
            backlog: 4,
            ..Default::default()
        }
    } else {
        idle()
    }
}

fn auto_q() -> BTreeMap<String, SpillThresholds> {
    BTreeMap::from([("q".to_owned(), SpillThresholds::default())])
}

#[test]
fn service_tick_flips_handle_and_bumps_watch_twice_for_a_spike() {
    let clock = ManualClock::new(T0);
    let handle = Arc::new(SpillHandle::new());
    let rx = handle.changed();
    let script = Script {
        tick: Arc::new(AtomicU64::new(0)),
        clock: None,
        f: spike,
    };
    let mut svc = SpillService::new(script, Arc::clone(&handle), Arc::new(clock.clone()));
    let mut seen = Vec::new();
    for t in 0..120u64 {
        for (q, tr) in svc.tick(&auto_q()) {
            seen.push((q, tr.to));
        }
        if t == 12 {
            assert!(handle.spilling("q"));
        }
        clock.advance(1_000);
    }
    assert_eq!(
        seen,
        vec![
            ("q".to_owned(), SpillState::Spilling),
            ("q".to_owned(), SpillState::Embedded)
        ]
    );
    assert!(!handle.spilling("q"));
    assert_eq!(*rx.borrow(), 2);
}

#[test]
fn service_merges_overflow_attempts_from_the_handle() {
    let clock = ManualClock::new(T0);
    let handle = Arc::new(SpillHandle::new());
    let script = Script {
        tick: Arc::new(AtomicU64::new(0)),
        clock: None,
        // Enter on lag at tick 0, then calm with a backlog and nothing on workers.
        f: |t| LoadSample {
            lag_ms: if t == 0 { 60_000 } else { 0 },
            backlog: 5,
            ..Default::default()
        },
    };
    let mut svc = SpillService::new(script, Arc::clone(&handle), Arc::new(clock.clone()));
    let mut exit_at = None;
    for t in 0..200u64 {
        // Placement keeps attempting overflow for the first 60 s.
        if t < 60 {
            handle.note_overflow("q", clock.now_ms());
        }
        for (_, tr) in svc.tick(&auto_q()) {
            if tr.to == SpillState::Embedded {
                exit_at = Some(t);
            }
        }
        clock.advance(1_000);
    }
    // Last attempt at t = 59: exit once it is a full cool-down old (t = 90).
    assert_eq!(exit_at, Some(90));
}

#[test]
fn queue_leaving_auto_is_dropped_and_stops_spilling() {
    let clock = ManualClock::new(T0);
    let handle = Arc::new(SpillHandle::new());
    let script = Script {
        tick: Arc::new(AtomicU64::new(0)),
        clock: None,
        f: |_| LoadSample {
            slots: 1.0,
            ..Default::default()
        },
    };
    let mut svc = SpillService::new(script, Arc::clone(&handle), Arc::new(clock.clone()));
    assert_eq!(svc.tick(&auto_q()).len(), 1);
    assert!(handle.spilling("q"));
    assert!(svc.tick(&BTreeMap::new()).is_empty());
    assert!(!handle.spilling("q"));
    assert!(svc.queues().is_empty());
}

#[tokio::test]
async fn run_loop_spills_and_returns_on_the_injected_clock() {
    let clock = ManualClock::new(T0);
    let ticks = Arc::new(AtomicU64::new(0));
    let handle = Arc::new(SpillHandle::new());
    let rx = handle.changed();
    let script = Script {
        tick: Arc::clone(&ticks),
        clock: Some(clock.clone()),
        f: spike,
    };
    let dyn_clock: Arc<dyn Clock> = Arc::new(clock.clone());
    let svc = SpillService::new(script, Arc::clone(&handle), Arc::clone(&dyn_clock));
    let stop = CancellationToken::new();
    // A 1 ms period with a script that moves the clock 1 s per tick: every deadline is
    // already due, so the loop skips ahead and paces itself at its 1 ms floor (80 ticks
    // take ~80 ms of real time) while the controller sees 1 s steps of the clock.
    let task = tokio::spawn(spill::run(
        svc,
        |_: &[String]| auto_q(),
        dyn_clock,
        stop.clone(),
        Duration::from_millis(1),
    ));
    tokio::time::timeout(Duration::from_secs(60), async {
        while ticks.load(Ordering::SeqCst) < 80 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("80 ticks");
    stop.cancel();
    task.await.expect("run loop");
    assert_eq!(*rx.borrow(), 2, "enter + exit");
    assert!(!handle.spilling("q"));
}

async fn put_policy(meta: &MetaHandle, p: QueuePolicy) {
    let record = MetaRecord::QueuePolicy(p);
    let key = record.key();
    let expected_version = meta.view().read(|s| s.get(&key).map(|v| v.version));
    let r = meta
        .propose(MetaCommand::Put {
            key,
            expected_version,
            record,
        })
        .await
        .expect("propose");
    assert!(matches!(r, ostro_meta::MetaResult::Ok { .. }), "{r:?}");
}

#[tokio::test]
async fn auto_queues_follow_policy_placement_and_overrides() {
    let dir = tempfile::tempdir().expect("tempdir");
    let meta = MetaHandle::start(dir.path(), 1).await.expect("meta");
    let over = SpillPolicy {
        cpu_pct: 50,
        slots_pct: 60,
        mem_pct: 70,
        lag_ms: 1_000,
        sustain_ms: 1_000,
        cooldown_ms: 2_000,
        hysteresis_pct: 80,
    };
    put_policy(
        &meta,
        QueuePolicy {
            queue: "a".into(),
            placement: Some(PLACEMENT_AUTO),
            spill: Some(over.clone()),
            ..QueuePolicy::default()
        },
    )
    .await;
    put_policy(
        &meta,
        QueuePolicy {
            queue: "e".into(),
            placement: Some(PLACEMENT_EMBEDDED),
            ..QueuePolicy::default()
        },
    )
    .await;
    let names = || ["a", "e", "none"].map(String::from);
    let d = SpillThresholds::default();
    let got = spill::auto_queues(meta.view(), false, d, names());
    assert_eq!(
        got,
        BTreeMap::from([("a".to_owned(), d.with_policy(Some(&over)))])
    );
    // `[workers] default_placement = auto`: queues without a placement are auto too.
    let got = spill::auto_queues(meta.view(), true, d, names());
    assert_eq!(got.keys().collect::<Vec<_>>(), ["a", "none"]);
    assert_eq!(got["none"], d);
    drop(meta);
}
