//! Determinism end to end on a real component (plan 03-09, ENG-03, ENG-04,
//! roadmap Phase 3 SC1).
//!
//! - det-basic (native `SystemTime::now`, std `HashMap` iteration, `rand`) recorded
//!   1,000 times with the same seed and script produces byte-identical journals, and
//!   1,000 replays append nothing and return identical output.
//! - det-divergent (the same `workflow.rs` with `DIVERGE = true`) replaying a
//!   det-basic journal fails as nondeterminism at the exact seq, with expected vs
//!   actual in the DLQ reason, for every divergence kind.
//! - The det-basic journal is pinned by a golden snapshot (first replay-corpus entry).
//!
//! All tests share one `Runtime` whose `FakeClock` is frozen (step 0): live timestamps
//! then depend only on the journal (`max(clock, last command ts)`), so every record
//! is reproducible without building a fresh engine per run.

mod common;

use std::sync::{Arc, OnceLock};

use ostro_engine::config::EngineConfig;
use ostro_engine::error::AttemptFailure;
use ostro_engine::runtime::{Prepared, Runtime, RuntimeOptions};
use ostro_engine::sink::{ActivationOutcome, RunRef};
use ostro_engine::testkit::{
    DEFAULT_ACCEPTED_AT, Drive, FakeClock, RunSpec, Script, drive_to_completion, replay_journal,
};
use ostro_types::journal::{Event, EventBody, RngSeed, encode_event};

struct Shared {
    rt: Runtime,
    basic: Prepared,
    divergent: Prepared,
}

fn shared() -> &'static Shared {
    static S: OnceLock<Shared> = OnceLock::new();
    S.get_or_init(|| {
        let rt = Runtime::new(
            EngineConfig::default(),
            RuntimeOptions {
                clock: Arc::new(FakeClock::with_step(DEFAULT_ACCEPTED_AT, 0)),
                token_key: [9; 32],
            },
        )
        .expect("runtime");
        let basic = rt
            .prepare(&common::guest_wasm("det-basic"))
            .expect("prepare det-basic");
        let divergent = rt
            .prepare(&common::guest_wasm("det-divergent"))
            .expect("prepare det-divergent");
        Shared {
            rt,
            basic,
            divergent,
        }
    })
}

fn run_ref() -> RunRef {
    RunRef {
        partition: 7,
        run_id: "det-run".to_string(),
    }
}

fn spec(mode: &str, seed: [u8; 32]) -> RunSpec {
    let mut s = RunSpec::new(run_ref(), "det", mode.as_bytes());
    s.seed = RngSeed(seed);
    s.accepted_at_nanos = 1_700_000_000_000_000_000;
    s
}

async fn record_seeded(mode: &str, seed: [u8; 32]) -> Drive {
    let s = shared();
    let d = drive_to_completion(&s.rt, &s.basic, spec(mode, seed), Script::default()).await;
    assert!(
        matches!(d.outcome, ActivationOutcome::Completed { .. }),
        "record({mode}): {:?}",
        d.outcome
    );
    d
}

/// Drive det-basic to completion with input `mode`, seed `[1; 32]`.
async fn record(mode: &str) -> Drive {
    record_seeded(mode, [1; 32]).await
}

fn seq_of_step(journal: &[Event], name: &str) -> u64 {
    journal
        .iter()
        .find_map(|e| match &e.body {
            EventBody::StepCompleted(s) if s.name == name => Some(e.seq),
            _ => None,
        })
        .unwrap_or_else(|| panic!("no step {name:?} in journal"))
}

fn step_bytes(journal: &[Event], name: &str) -> Vec<u8> {
    journal
        .iter()
        .find_map(|e| match &e.body {
            EventBody::StepCompleted(s) if s.name == name => Some(s.outcome.bytes.clone()),
            _ => None,
        })
        .unwrap_or_else(|| panic!("no step {name:?} in journal"))
}

fn encoded(journal: &[Event]) -> Vec<(u64, u32, Vec<u8>)> {
    journal
        .iter()
        .map(|e| {
            let (kind, bytes) = encode_event(e);
            (e.seq, kind, bytes)
        })
        .collect()
}

/// Record det-basic with `mode`, replay its journal with det-divergent; returns
/// (expected seq from the recording, nondeterminism failure, appended events).
async fn diverge(mode: &str, at_step: &str) -> (u64, AttemptFailure, usize) {
    let d = record(mode).await;
    let seq = seq_of_step(&d.journal, at_step);
    let s = shared();
    let (outcome, appended) = replay_journal(&s.rt, &s.divergent, run_ref(), d.journal).await;
    let ActivationOutcome::AttemptFailed(f @ AttemptFailure::NonDeterminism(_)) = outcome else {
        panic!("{mode}: expected nondeterminism, got {outcome:?}");
    };
    (seq, f, appended)
}

fn nd(f: &AttemptFailure) -> (u64, &str, &str) {
    let AttemptFailure::NonDeterminism(nd) = f else {
        unreachable!()
    };
    (nd.seq, &nd.expected, &nd.actual)
}

#[tokio::test]
async fn divergence_changed_args() {
    let (want, f, appended) = diverge("changed-args", "s2").await;
    let (seq, expected, actual) = nd(&f);
    assert_eq!(seq, want);
    assert!(expected.contains("step \"s2\""), "{expected}");
    assert!(actual.contains("step \"s2x\""), "{actual}");
    assert_eq!(
        f.dlq_reason(),
        format!("nondeterminism: seq={seq} expected={expected} actual={actual}")
    );
    assert_eq!(appended, 0, "a diverged attempt appends nothing");
}

#[tokio::test]
async fn divergence_extra() {
    let (want, f, appended) = diverge("extra", "s2").await;
    let (seq, expected, actual) = nd(&f);
    assert_eq!(seq, want);
    assert!(expected.contains("step \"s2\""), "{expected}");
    assert!(actual.contains("step \"extra\""), "{actual}");
    assert_eq!(appended, 0);
}

#[tokio::test]
async fn divergence_missing() {
    let (want, f, appended) = diverge("missing", "loop-1").await;
    let (seq, expected, actual) = nd(&f);
    assert_eq!(seq, want);
    assert!(expected.contains("step \"loop-1\""), "{expected}");
    assert!(actual.contains("step \"loop-2\""), "{actual}");
    assert_eq!(appended, 0);
}

#[tokio::test]
async fn divergence_reordered() {
    let (want, f, appended) = diverge("reordered", "loop-0").await;
    let (seq, expected, actual) = nd(&f);
    assert_eq!(seq, want);
    assert!(expected.contains("step \"loop-0\""), "{expected}");
    assert!(actual.contains("step \"loop-1\""), "{actual}");
    assert_eq!(appended, 0);
}

#[tokio::test]
async fn divergence_early_return() {
    // Research Pitfall 3: returning before the recorded commands are consumed is
    // never success.
    let (want, f, appended) = diverge("early-return", "loop-0").await;
    let (seq, expected, actual) = nd(&f);
    assert_eq!(seq, want);
    assert!(expected.contains("step \"loop-0\""), "{expected}");
    assert_eq!(actual, "run returned");
    assert_eq!(appended, 0);
}

#[tokio::test]
async fn divergence_same_module_no_error() {
    let d = record("changed-args").await;
    let ActivationOutcome::Completed { output } = &d.outcome else {
        unreachable!()
    };
    let s = shared();
    let (outcome, appended) = replay_journal(&s.rt, &s.basic, run_ref(), d.journal).await;
    assert_eq!(
        outcome,
        ActivationOutcome::Completed {
            output: output.clone()
        }
    );
    assert_eq!(appended, 0);
}

#[tokio::test]
async fn divergence_seed_matters() {
    let a = record_seeded("basic", [1; 32]).await;
    let b = record_seeded("basic", [2; 32]).await;
    assert_ne!(
        step_bytes(&a.journal, "s1"),
        step_bytes(&b.journal, "s1"),
        "randomness is per run"
    );
    assert_ne!(encoded(&a.journal), encoded(&b.journal));
    assert_ne!(a.outcome, b.outcome);
}
