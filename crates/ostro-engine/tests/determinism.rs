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
//! then depend only on the journal (`max(clock, last command ts, max journal ts)`), so
//! every record is reproducible without building a fresh engine per run.

mod common;

use std::sync::{Arc, OnceLock};
use std::time::Instant;

use ostro_engine::config::EngineConfig;
use ostro_engine::error::AttemptFailure;
use ostro_engine::runtime::{Prepared, Runtime, RuntimeOptions};
use ostro_engine::sink::{ActivationOutcome, RunRef};
use ostro_engine::testkit::{
    DEFAULT_ACCEPTED_AT, Drive, FakeClock, RunSpec, Script, drive_to_completion, replay_journal,
};
use ostro_types::journal::{Event, EventBody, RngSeed, encode_event};
use sha2::{Digest, Sha256};

struct Shared {
    rt: Runtime,
    basic: Prepared,
    divergent: Prepared,
    /// det-basic as committed in the replay corpus (fixed module bytes, see the golden test).
    corpus_basic: Prepared,
}

fn shared() -> &'static Shared {
    static S: OnceLock<Shared> = OnceLock::new();
    S.get_or_init(|| {
        let rt = Runtime::new(
            EngineConfig::default(),
            RuntimeOptions {
                clock: Arc::new(FakeClock::with_step(DEFAULT_ACCEPTED_AT, 0)),
                token_key: [9; 32],
                ..RuntimeOptions::default()
            },
        )
        .expect("runtime");
        let basic = rt
            .prepare(&common::guest_wasm("det-basic"))
            .expect("prepare det-basic");
        let divergent = rt
            .prepare(&common::guest_wasm("det-divergent"))
            .expect("prepare det-divergent");
        let corpus_wasm = common::repo_root().join("tests/replay-corpus/det-basic/module.wasm");
        let corpus_basic = rt
            .prepare(
                &std::fs::read(&corpus_wasm)
                    .unwrap_or_else(|e| panic!("read {}: {e}", corpus_wasm.display())),
            )
            .expect("prepare corpus det-basic");
        Shared {
            rt,
            basic,
            divergent,
            corpus_basic,
        }
    })
}

fn iters() -> usize {
    std::env::var("OSTRO_DET_ITERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1000)
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
    record_with(&shared().basic, mode, seed).await
}

async fn record_with(module: &Prepared, mode: &str, seed: [u8; 32]) -> Drive {
    let s = shared();
    let d = drive_to_completion(&s.rt, module, spec(mode, seed), Script::default()).await;
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

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
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

#[tokio::test]
async fn thousand_records() {
    let n = iters();
    let first = record("basic").await;
    let want = encoded(&first.journal);
    let start = Instant::now();
    for i in 0..n {
        let d = record("basic").await;
        assert_eq!(d.outcome, first.outcome, "run {i}: outcome differs");
        assert!(encoded(&d.journal) == want, "run {i}: journal bytes differ");
    }
    let el = start.elapsed();
    println!(
        "thousand_records: {n} records in {el:?} ({:?}/run)",
        el / n.max(1) as u32
    );
}

#[tokio::test]
async fn thousand_replays() {
    let n = iters();
    let d = record("basic").await;
    let ActivationOutcome::Completed { output } = &d.outcome else {
        unreachable!()
    };
    let want = ActivationOutcome::Completed {
        output: output.clone(),
    };
    let s = shared();
    let start = Instant::now();
    for i in 0..n {
        let (outcome, appended) =
            replay_journal(&s.rt, &s.basic, run_ref(), d.journal.clone()).await;
        assert_eq!(outcome, want, "replay {i}");
        assert_eq!(appended, 0, "replay {i} appended events");
    }
    let el = start.elapsed();
    println!(
        "thousand_replays: {n} replays in {el:?} ({:?}/replay)",
        el / n.max(1) as u32
    );
}

/// First replay-corpus entry. QA-02 (Phase 5) replays committed journals across
/// versions and architectures. A change here means hashing, the journal codec, the
/// RNG streams, the WASI shim or the guest toolchain changed: investigate, never
/// blindly re-bless. If this snapshot differs between x86_64 and aarch64 in CI, that
/// is a real determinism bug.
///
/// The pinned recording runs the committed corpus module
/// (`tests/replay-corpus/det-basic/module.wasm`): `RunStarted.module_hash` is the module's
/// sha256, and a fresh guest build is not byte-reproducible across build hosts (cargo
/// mixes the host triple into the `-C metadata` of proc-macro / build-script dependencies,
/// and so into every dependent crate's StableCrateId, and a plain build embeds the host's
/// cargo / rustup paths). The freshly built det-basic must then journal the same events
/// byte for byte except `RunStarted` (whose only build-dependent field is `module_hash`),
/// so guest source or guest toolchain changes are still caught.
#[tokio::test]
async fn golden_det_basic_journal() {
    let s = shared();
    let d = record_with(&s.corpus_basic, "basic", [1; 32]).await;
    // Logical time is monotonic over every journaled event, inputs included (03-11):
    // with the frozen clock, events after TimerFired are stamped at its fire time.
    let ts: Vec<u64> = d.journal.iter().map(|e| e.ts_nanos).collect();
    assert!(
        ts.windows(2).all(|w| w[0] <= w[1]),
        "journal ts decreased: {ts:?}"
    );
    let text = golden_lines(&d.journal);
    insta::assert_snapshot!("golden_det_basic_journal", text);

    let fresh = record("basic").await;
    let EventBody::RunStarted(rs) = &fresh.journal[0].body else {
        panic!(
            "fresh journal starts with {:?}",
            fresh.journal[0].body.kind()
        )
    };
    assert_eq!(rs.module_hash, s.basic.module_hash_hex);
    let tail = |t: &str| t.lines().skip(1).map(str::to_owned).collect::<Vec<_>>();
    assert_eq!(
        tail(&golden_lines(&fresh.journal)),
        tail(&text),
        "freshly built det-basic journals differently from the committed module"
    );
}

/// The golden snapshot's line format: seq, kind, command hash, ts, encoded length, digest.
fn golden_lines(journal: &[Event]) -> String {
    let mut text = String::new();
    for e in journal {
        let (_, bytes) = encode_event(e);
        let hash = e
            .body
            .command_meta()
            .map_or_else(|| "-".to_string(), |m| hex(&m.hash.0));
        let digest = Sha256::digest(&bytes);
        text.push_str(&format!(
            "{} {:?} {} {} {} {}\n",
            e.seq,
            e.body.kind(),
            hash,
            e.ts_nanos,
            bytes.len(),
            hex(&digest[..8])
        ));
    }
    text
}
