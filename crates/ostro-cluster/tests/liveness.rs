//! 12-10 Task 1: the liveness accumulator on a manual clock (D-P12-9). Conservative on
//! samples (never dead on one missed sample), fast on confirmed suspects, and a new
//! controller seeds suspicion instead of trusting its first sample.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use ostro_cluster::liveness::{Liveness, LivenessConfig, LivenessHandle, NodeState, SuspectSink};
use ostro_partition::ManualClock;

const T: Duration = Duration::from_millis(3000);

fn setup() -> (ManualClock, Liveness) {
    let clock = ManualClock::new(1_000_000);
    let mut l = Liveness::new(Arc::new(clock.clone()), T);
    l.track([1, 2, 3]);
    (clock, l)
}

fn ms(d: Duration) -> u64 {
    d.as_millis() as u64
}

#[test]
fn defaults_derive_from_the_session_timeout() {
    let c = LivenessConfig::new(T);
    assert_eq!(c.tick, Duration::from_millis(500));
    assert_eq!(c.sample_window, Duration::from_millis(1000));
    assert_eq!(c.confirm_window, Duration::from_millis(750));
    assert_eq!(c.seed_window, Duration::from_millis(1000));
}

#[test]
fn one_missed_sample_is_not_dead() {
    let (clock, mut l) = setup();
    let tick = ms(l.config().tick);
    l.observe(&[1, 2, 3]);
    clock.advance(tick);
    l.observe(&[1, 2]);
    assert_eq!(l.state(3), NodeState::Live);
    clock.advance(tick);
    l.observe(&[1, 2, 3]);
    assert_eq!(l.state(3), NodeState::Live);
    // Several misses inside the window are still not dead.
    for _ in 0..5 {
        clock.advance(tick);
        l.observe(&[1, 2]);
        assert_ne!(l.state(3), NodeState::Dead);
    }
}

#[test]
fn unseen_for_the_session_timeout_is_dead() {
    let (clock, mut l) = setup();
    let tick = ms(l.config().tick);
    l.observe(&[1, 2, 3]);
    let mut elapsed = 0;
    while elapsed + tick <= ms(T) {
        clock.advance(tick);
        elapsed += tick;
        l.observe(&[1, 2]);
        assert_ne!(l.state(3), NodeState::Dead, "dead after only {elapsed} ms");
    }
    clock.advance(tick);
    l.observe(&[1, 2]);
    assert_eq!(l.state(3), NodeState::Dead);
    let universe = BTreeSet::from([1, 2, 3]);
    assert_eq!(l.live_set(&universe), BTreeSet::from([1, 2]));
    // Seen again: live at once.
    clock.advance(tick);
    l.observe(&[1, 2, 3]);
    assert_eq!(l.state(3), NodeState::Live);
}

#[test]
fn suspect_absent_from_the_confirming_sample_is_dead_within_one_tick() {
    let (clock, mut l) = setup();
    l.observe(&[1, 2, 3]);
    l.report_suspect(1, 3);
    assert_eq!(l.state(3), NodeState::Suspect);
    // A report alone never kills: no confirmation is due yet and the node stays live.
    assert_eq!(l.confirm_due(), None);
    let universe = BTreeSet::from([1, 2, 3]);
    assert!(l.live_set(&universe).contains(&3));
    // A confirmation taken too early settles nothing.
    l.confirm(&[1, 2]);
    assert_eq!(l.state(3), NodeState::Suspect);
    // confirm_window later (< one tick) the confirming sample is due.
    let w = l.config().confirm_window;
    assert!(w <= l.config().tick * 2);
    clock.advance(ms(w));
    assert_eq!(l.confirm_due(), Some(w));
    l.confirm(&[1, 2]);
    assert_eq!(l.state(3), NodeState::Dead);
    assert_eq!(l.confirm_due(), None);
    // A later regular sample that includes it revives it.
    clock.advance(ms(l.config().tick));
    l.observe(&[1, 2, 3]);
    assert_eq!(l.state(3), NodeState::Live);
}

/// 12-18: the controller's poll takes the confirming sample (`within = confirm_window`) and
/// then a regular one (`within = sample_window`, wider). A node killed just before the
/// suspect report was still heard inside the wider window, so the regular sample used to
/// revive it at once and detection waited for a later report (about 2.3 s instead of
/// `confirm_window` plus a tick on the failover runs).
#[test]
fn a_wider_sample_right_after_the_confirmation_does_not_revive() {
    let (clock, mut l) = setup();
    l.observe(&[1, 2, 3]);
    l.report_suspect(1, 3);
    clock.advance(ms(l.config().confirm_window));
    l.confirm(&[1, 2]);
    assert_eq!(l.state(3), NodeState::Dead);
    // Same poll: node 3's last contact predates the report but lies inside sample_window.
    l.observe(&[1, 2, 3]);
    assert_eq!(
        l.state(3),
        NodeState::Dead,
        "revived by contact older than the report"
    );
    let stale = ms(l.config().sample_window) - ms(l.config().confirm_window);
    clock.advance(stale - 1);
    l.observe(&[1, 2, 3]);
    assert_eq!(l.state(3), NodeState::Dead);
    // From then on the regular window starts after the confirmation: presence is new contact.
    clock.advance(1);
    l.observe(&[1, 2, 3]);
    assert_eq!(l.state(3), NodeState::Live);
}

#[test]
fn suspect_present_in_the_confirming_sample_is_cleared() {
    let (clock, mut l) = setup();
    l.observe(&[1, 2, 3]);
    l.report_suspect(2, 3);
    clock.advance(ms(l.config().confirm_window));
    l.confirm(&[1, 2, 3]);
    assert_eq!(l.state(3), NodeState::Live);
    assert_eq!(l.confirm_due(), None);
    // A regular sample showing the suspect also clears it.
    l.report_suspect(2, 1);
    l.observe(&[1, 2, 3]);
    assert_eq!(l.state(1), NodeState::Live);
    assert_eq!(l.next_confirm_ms(), None);
}

#[test]
fn a_new_controller_seeds_suspicion_and_never_kills_on_its_first_sample() {
    let (clock, mut l) = setup();
    let tick = ms(l.config().tick);
    l.observe(&[1, 2, 3]);
    clock.advance(tick);
    l.on_became_controller();
    // The first sample misses node 3 (a new leader has not heard from it yet).
    l.observe(&[1, 2]);
    assert_eq!(l.state(3), NodeState::Live, "first sample must not kill");
    // Node 2 appears late but within the seeding window: never suspect.
    let seed = ms(l.config().seed_window);
    let mut t = 0;
    while t + tick < seed {
        clock.advance(tick);
        t += tick;
        l.observe(&[1]);
        assert_ne!(l.state(3), NodeState::Dead);
        assert_eq!(l.state(3), NodeState::Live);
    }
    l.observe(&[1, 2]);
    // Past the seeding window node 3, absent from every sample, is suspect...
    clock.advance(tick);
    l.observe(&[1, 2]);
    assert_eq!(l.state(3), NodeState::Suspect);
    assert_eq!(l.state(2), NodeState::Live);
    // ...and the confirming sample settles it.
    clock.advance(ms(l.config().confirm_window));
    assert!(l.confirm_due().is_some());
    l.confirm(&[1, 2]);
    assert_eq!(l.state(3), NodeState::Dead);
}

#[test]
fn a_new_controller_forgets_old_verdicts() {
    let (clock, mut l) = setup();
    l.observe(&[1, 2, 3]);
    l.report_suspect(1, 3);
    clock.advance(ms(l.config().confirm_window));
    l.confirm(&[1, 2]);
    assert_eq!(l.state(3), NodeState::Dead);
    l.on_became_controller();
    assert_eq!(l.state(3), NodeState::Live);
}

#[test]
fn suspect_sink_reports_reach_the_detector() {
    let clock = ManualClock::new(5_000);
    let h = LivenessHandle::new(Liveness::new(Arc::new(clock.clone()), T));
    h.lock().track([1, 2, 3]);
    h.lock().observe(&[1, 2, 3]);
    let sink: Arc<dyn SuspectSink> = Arc::new(h.clone());
    sink.report(2, 3, Some(7));
    assert_eq!(h.lock().state(3), NodeState::Suspect);
    clock.advance(ms(LivenessConfig::new(T).confirm_window));
    h.lock().confirm(&[1, 2]);
    assert_eq!(h.lock().state(3), NodeState::Dead);
}
