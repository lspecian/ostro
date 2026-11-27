//! 14-10 Task 1 (D-P14-20/34): the failpoints-only `SkewClock` over the injected clock:
//! byte-identical without skew, wall bumps, strobe (triangle wave), monotonic rate drift
//! within ±5%, re-anchoring on every rate change, reset keeping the accumulated monotonic
//! offset, and a non-decreasing monotonic clock across any change sequence (proptest).
#![cfg(feature = "failpoints")]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use ostro_partition::Clock;
use ostro_partition::clock::{MAX_RATE_PPM, ManualClock, MonoSource, SkewClock, SkewSettings};
use proptest::prelude::*;

const MS: u64 = 1_000_000;

/// A skew clock over a manual wall clock and a manual monotonic source (ns).
struct Rig {
    wall: ManualClock,
    mono: Arc<AtomicU64>,
    clock: SkewClock,
}

impl Rig {
    fn new() -> Self {
        let wall = ManualClock::new(1_700_000_000_000);
        let mono = Arc::new(AtomicU64::new(5 * MS));
        let src: MonoSource = {
            let m = Arc::clone(&mono);
            Arc::new(move || m.load(Ordering::SeqCst))
        };
        let clock = SkewClock::with_monotonic(Arc::new(wall.clone()), src);
        Rig { wall, mono, clock }
    }

    /// Real time passes: both inner clocks move by `ms`.
    fn advance(&self, ms: u64) {
        self.wall.advance(ms);
        self.mono.fetch_add(ms * MS, Ordering::SeqCst);
    }

    fn mono_ms(&self) -> u64 {
        u64::try_from(self.clock.monotonic().as_millis()).unwrap()
    }
}

#[test]
fn without_skew_the_wall_clock_is_the_inner_clock() {
    let r = Rig::new();
    for step in [0, 1, 7, 1_000, 59_999] {
        r.advance(step);
        assert_eq!(r.clock.now_ms(), r.wall.now_ms());
    }
    assert_eq!(r.mono_ms(), r.mono.load(Ordering::SeqCst) / MS);
    let s = r.clock.snapshot();
    assert_eq!((s.offset_ms, s.strobe_ms, s.mono_rate_ppm), (0, 0, 0));
    assert_eq!(s.wall_ms, s.inner_wall_ms);
    // The skip hooks of the inner clock pass through (none for a manual clock).
    assert!(r.clock.subscribe_changes().is_none());
    assert!(r.clock.skip_barrier().is_none());
    assert!(r.clock.skew().is_some());
    assert!(r.wall.skew().is_none());
}

#[test]
fn bumps_move_the_wall_clock_only() {
    let r = Rig::new();
    let mono0 = r.mono_ms();
    r.clock.bump_ms(30_000);
    assert_eq!(r.clock.now_ms(), r.wall.now_ms() + 30_000);
    r.clock.bump_ms(-90_000);
    assert_eq!(r.clock.now_ms(), r.wall.now_ms() - 60_000);
    r.clock
        .apply(&SkewSettings {
            offset_ms: Some(5_000),
            ..SkewSettings::default()
        })
        .unwrap();
    assert_eq!(r.clock.now_ms(), r.wall.now_ms() + 5_000);
    assert_eq!(
        r.mono_ms(),
        mono0,
        "a wall bump never touches the monotonic clock"
    );
    r.clock.reset();
    assert_eq!(r.clock.now_ms(), r.wall.now_ms());
}

#[test]
fn strobe_is_a_triangle_wave_around_the_offset() {
    let r = Rig::new();
    r.clock
        .apply(&SkewSettings {
            offset_ms: Some(1_000),
            strobe_ms: Some(400),
            strobe_period_ms: Some(1_000),
            ..SkewSettings::default()
        })
        .unwrap();
    let base = |r: &Rig| r.wall.now_ms() + 1_000;
    assert_eq!(r.clock.now_ms(), base(&r));
    r.advance(250); // quarter: +amp
    assert_eq!(r.clock.now_ms(), base(&r) + 400);
    r.advance(250); // half: 0
    assert_eq!(r.clock.now_ms(), base(&r));
    r.advance(250); // three quarters: -amp
    assert_eq!(r.clock.now_ms(), base(&r) - 400);
    r.advance(125);
    assert_eq!(r.clock.now_ms(), base(&r) - 200);
    r.advance(125); // full period: 0 again
    assert_eq!(r.clock.now_ms(), base(&r));
    // A strobe needs a period.
    let err = r
        .clock
        .apply(&SkewSettings {
            strobe_ms: Some(10),
            strobe_period_ms: Some(0),
            ..SkewSettings::default()
        })
        .unwrap_err();
    assert!(err.contains("strobe_period_ms"), "{err}");
    r.clock
        .apply(&SkewSettings {
            strobe_ms: Some(0),
            ..SkewSettings::default()
        })
        .unwrap();
    r.advance(250);
    assert_eq!(r.clock.now_ms(), base(&r));
}

#[test]
fn drift_within_five_percent_scales_both_clocks() {
    for ppm in [50_000i64, -50_000, 10_000, -10_000] {
        let r = Rig::new();
        let (w0, m0) = (r.clock.now_ms(), r.mono_ms());
        r.clock.set_rate_ppm(ppm);
        r.advance(10_000);
        let want = 10_000 + 10_000 * ppm / 1_000_000;
        let dm = i64::try_from(r.mono_ms() - m0).unwrap();
        let dw = i64::try_from(r.clock.now_ms() - w0).unwrap();
        assert_eq!(dm, want, "monotonic at {ppm} ppm");
        assert_eq!(dw, want, "wall at {ppm} ppm");
        // Reset: no further drift; the monotonic clock keeps its accumulated offset, the
        // wall clock returns to the inner value.
        r.clock.reset();
        let m1 = r.mono_ms();
        assert_eq!(r.clock.now_ms(), r.wall.now_ms());
        r.advance(1_000);
        assert_eq!(r.mono_ms(), m1 + 1_000);
        assert_eq!(r.clock.now_ms(), r.wall.now_ms());
    }
}

#[test]
fn every_rate_change_reanchors_without_a_jump() {
    let r = Rig::new();
    r.clock.set_rate_ppm(50_000);
    r.advance(1_000);
    let before = r.mono_ms();
    r.clock.set_rate_ppm(-50_000);
    assert_eq!(r.mono_ms(), before, "a rate change must not move the clock");
    r.advance(1_000);
    assert_eq!(r.mono_ms(), before + 950);
    r.clock.set_rate_ppm(0);
    assert_eq!(r.mono_ms(), before + 950);
    r.advance(1_000);
    assert_eq!(r.mono_ms(), before + 1_950);
}

#[test]
fn rate_bounds_are_enforced() {
    let r = Rig::new();
    let err = r
        .clock
        .apply(&SkewSettings {
            mono_rate_ppm: Some(MAX_RATE_PPM + 1),
            ..SkewSettings::default()
        })
        .unwrap_err();
    assert!(err.contains("mono_rate"), "{err}");
    assert_eq!(
        r.clock.snapshot().mono_rate_ppm,
        0,
        "an invalid change applies nothing"
    );
    assert_eq!(
        r.clock.set_rate_ppm(-2_000_000).mono_rate_ppm,
        -MAX_RATE_PPM
    );
}

#[derive(Clone, Debug)]
enum Op {
    Advance(u64),
    Rate(i64),
    Bump(i64),
    Strobe(u64, u64),
    Reset,
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        (0u64..5_000).prop_map(Op::Advance),
        (-MAX_RATE_PPM..=MAX_RATE_PPM).prop_map(Op::Rate),
        (-60_000i64..=60_000).prop_map(Op::Bump),
        (0u64..2_000, 1u64..10_000).prop_map(|(a, p)| Op::Strobe(a, p)),
        Just(Op::Reset),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn monotonic_never_decreases(ops in proptest::collection::vec(op(), 1..64)) {
        let r = Rig::new();
        let mut last = r.clock.monotonic();
        for o in ops {
            match o {
                Op::Advance(ms) => r.advance(ms),
                Op::Rate(ppm) => { r.clock.set_rate_ppm(ppm); }
                Op::Bump(ms) => { r.clock.bump_ms(ms); }
                Op::Strobe(a, p) => {
                    r.clock.apply(&SkewSettings {
                        strobe_ms: Some(a),
                        strobe_period_ms: Some(p),
                        ..SkewSettings::default()
                    }).unwrap();
                }
                Op::Reset => { r.clock.reset(); }
            }
            let now = r.clock.monotonic();
            prop_assert!(now >= last, "monotonic went back: {last:?} -> {now:?}");
            last = now;
        }
        // After a reset the wall clock is the inner clock again.
        r.clock.reset();
        prop_assert_eq!(r.clock.now_ms(), r.wall.now_ms());
    }
}
