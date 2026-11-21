//! Spill hysteresis (plan 07-13, WORK-03, D-W9, plan-check B6).
//!
//! The controller tests feed one fake sample per second; time comes from a partition
//! `ManualClock` (Phase 10 F5), never from the wall clock.

use ostro_meta::SpillPolicy;
use ostro_partition::{Clock, ManualClock};
use ostro_server::workers::spill::{
    LoadSample, Signal, SpillController, SpillState, SpillThresholds, Transition,
};

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
