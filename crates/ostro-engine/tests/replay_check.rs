//! `ostro_engine::replay_check` + `replay_corpus` as the `ostro replay` library (plan
//! 10-11, CLI-07):
//!
//! - every committed corpus entry loads through `load_corpus` and its journal survives a
//!   `journal.bin` encode/decode round trip;
//! - a terminal entry is `Ok{0}`; the same journal cut before its terminal event is
//!   `OkFrontier{appended >= 1}`;
//! - a module whose step order differs (det-basic's journal with the `reordered` input,
//!   replayed by `guests/det-divergent`) is `Diverged` at the first mismatching seq, with
//!   the expected and actual commands in the detail; the original module still replays
//!   the same edited journal `Ok` (det-basic ignores its input);
//! - a corrupted `journal.bin` is a decode error from the loader.

mod common;

use std::path::PathBuf;
use std::sync::Arc;

use ostro_engine::config::EngineConfig;
use ostro_engine::replay_check::{ReplayVerdict, replay_check};
use ostro_engine::replay_corpus::{
    CorpusEntry, decode_journal_bin, encode_journal_bin, load_corpus, load_corpus_entry,
};
use ostro_engine::runtime::{Runtime, RuntimeOptions};
use ostro_engine::testkit::FakeClock;
use ostro_types::journal::{EventBody, EventKind};

fn corpus() -> PathBuf {
    common::repo_root().join("tests/replay-corpus")
}

fn runtime(e: &CorpusEntry) -> Runtime {
    let last_ts = e.journal.iter().map(|e| e.ts_nanos).max().unwrap_or(0);
    Runtime::new(
        EngineConfig::default(),
        RuntimeOptions {
            clock: Arc::new(FakeClock::with_step(last_ts + 3_600_000_000_000, 0)),
            token_key: e.token_key,
            ..RuntimeOptions::default()
        },
    )
    .expect("runtime")
}

fn det_basic() -> CorpusEntry {
    load_corpus_entry(&corpus().join("det-basic")).expect("det-basic entry")
}

#[test]
fn corpus_loads_through_the_library() {
    let entries = load_corpus(&corpus()).expect("load corpus");
    assert!(entries.len() >= 24, "{} entries", entries.len());
    let mut names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
    let sorted = {
        let mut s = names.clone();
        s.sort();
        s
    };
    assert_eq!(names, sorted, "sorted by name");
    names.dedup();
    assert_eq!(names.len(), entries.len());
    for e in &entries {
        let again = decode_journal_bin(&encode_journal_bin(&e.journal)).expect("round trip");
        assert_eq!(again, e.journal, "{}", e.name);
        assert!(
            matches!(e.journal[0].body, EventBody::RunStarted(_)),
            "{}",
            e.name
        );
    }
}

#[tokio::test]
async fn terminal_ok_and_truncated_frontier() {
    let e = det_basic();
    let rt = runtime(&e);
    let p = rt.prepare(&e.module).expect("prepare");
    assert_eq!(
        replay_check(&rt, &p, e.run.clone(), &e.journal).await,
        ReplayVerdict::Ok { appended: 0 }
    );
    let cut = &e.journal[..e.journal.len() - 1];
    let v = replay_check(&rt, &p, e.run.clone(), cut).await;
    assert!(
        matches!(v, ReplayVerdict::OkFrontier { appended } if appended >= 1),
        "{v:?}"
    );
}

#[tokio::test]
async fn reordered_module_diverges_at_the_first_mismatch() {
    let e = det_basic();
    let mut journal = e.journal.clone();
    let EventBody::RunStarted(rs) = &mut journal[0].body else {
        panic!("seq 0 is RunStarted");
    };
    rs.input.data = b"reordered".to_vec();
    // The first mismatch: det-divergent runs loop-1 where the journal has loop-0.
    let want = journal
        .iter()
        .find(|ev| matches!(&ev.body, EventBody::StepCompleted(s) if s.name == "loop-0"))
        .map(|ev| ev.seq)
        .expect("loop-0 in det-basic");
    let rt = runtime(&e);

    let divergent = rt
        .prepare(&common::guest_wasm("det-divergent"))
        .expect("prepare det-divergent");
    match replay_check(&rt, &divergent, e.run.clone(), &journal).await {
        ReplayVerdict::Diverged { seq, detail } => {
            assert_eq!(seq, want, "{detail}");
            assert!(
                detail.contains("expected") && detail.contains("actual"),
                "{detail}"
            );
            assert!(
                detail.contains("loop-0") && detail.contains("loop-1"),
                "{detail}"
            );
        }
        other => panic!("want Diverged at {want}, got {other:?}"),
    }
    // A non-terminal prefix past the mismatch diverges at the same seq.
    let v = replay_check(
        &rt,
        &divergent,
        e.run.clone(),
        &journal[..journal.len() - 1],
    )
    .await;
    assert!(
        matches!(&v, ReplayVerdict::Diverged { seq, .. } if *seq == want),
        "{v:?}"
    );

    // The recording module ignores its input: the edited journal still replays.
    let original = rt.prepare(&e.module).expect("prepare det-basic");
    assert_eq!(
        replay_check(&rt, &original, e.run.clone(), &journal).await,
        ReplayVerdict::Ok { appended: 0 }
    );
    assert_eq!(
        journal.last().map(|e| e.body.kind()),
        Some(EventKind::RunCompleted)
    );
}

#[test]
fn corrupted_journal_bin_is_a_decode_error() {
    let src = corpus().join("det-basic");
    let mut bin = std::fs::read(src.join("journal.bin")).expect("read");
    // Truncate inside the last record.
    bin.truncate(bin.len() - 3);
    let err = decode_journal_bin(&bin).expect_err("truncated");
    assert!(err.contains("truncated"), "{err}");
    // A length prefix pointing at garbage: not a JournalEvent.
    let garbage = [4u8, 0, 0, 0, 0xff, 0xff, 0xff, 0xff];
    assert!(decode_journal_bin(&garbage).is_err());

    // Through the loader (an entry copied and corrupted): the sha check catches it first.
    let t = tempfile::tempdir().expect("tempdir");
    let dst = t.path().join("det-basic");
    std::fs::create_dir_all(&dst).expect("mkdir");
    for f in ostro_engine::replay_corpus::ENTRY_FILES {
        std::fs::copy(src.join(f), dst.join(f)).expect("copy");
    }
    std::fs::write(dst.join("journal.bin"), &bin).expect("write");
    let err = load_corpus_entry(&dst).expect_err("corrupted");
    assert!(err.contains("journal_sha256 mismatch"), "{err}");
    assert!(
        load_corpus(t.path())
            .unwrap_err()
            .starts_with("det-basic: ")
    );
}
