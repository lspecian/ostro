//! QA-02 replay corpus (plan 05-12): every committed entry under `tests/replay-corpus/`
//! replays on the current engine exactly as it was recorded.
//!
//! An entry is a directory `tests/replay-corpus/<entry>/` with exactly three files:
//! - `module.wasm`: the exact component bytes the journal was recorded with;
//! - `journal.bin`: repeated `u32 LE length ‖ JournalEvent protobuf` (the bytes of the
//!   partition `Events` rows, payloads always inline);
//! - `entry.toml`: metadata (see [`Entry`] and `tests/replay-corpus/README.md`).
//!
//! For each entry the test verifies the sha256s and the event count, prepares the module
//! through the production linker (`Runtime::prepare`), and activates it over the **whole**
//! journal with a recording sink, on a runtime built with the entry's token key and the
//! entry's `partition`/`run_id`, so replayed await-token minting and run info reproduce
//! byte-identically. The outcome kind must equal `expected`, the terminal payload (result,
//! failure or continuation) must equal the journaled terminal event's and hash to
//! `terminal_payload_sha256`, nothing may be appended and nothing may diverge.
//!
//! The test is part of the default `cargo test --workspace` run, so it runs on every CI leg
//! (linux-x86_64-musl, linux-aarch64-musl, macos-aarch64, linux-x86_64-gnu): that is the
//! cross-architecture determinism proof and the Wasmtime-bump gate. Entries are never
//! re-recorded: a failure here is a determinism or compatibility bug, never a reason to
//! re-bless (README).
//!
//! `OSTRO_REPLAY_CORPUS=<dir>` replays another directory instead (checking an exported
//! entry before it is committed).

mod common;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use ostro_engine::config::EngineConfig;
use ostro_engine::replay_check::{ReplayVerdict, journaled_outcome, replay_check};
use ostro_engine::replay_corpus::load_corpus_entry;
use ostro_engine::runtime::{Runtime, RuntimeOptions};
use ostro_engine::sink::{ActivationOutcome, RunRef};
use ostro_engine::testkit::FakeClock;
use ostro_types::PayloadRef;
use ostro_types::command::JournalEvent;
use ostro_types::journal::{Event, EventBody, Failure, decode_event, encode_event, encode_result};
use ostro_types::payload::payload_ref;
use prost::Message;
use serde::Deserialize;
use sha2::{Digest, Sha256};

/// The three files of an entry; anything else in an entry directory is an error.
const ENTRY_FILES: [&str; 3] = ["entry.toml", "journal.bin", "module.wasm"];

/// `entry.toml`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    name: String,
    workflow: String,
    /// Run input when it is JSON (informational; the journal's `RunStarted` is the truth).
    #[serde(default)]
    input_json: Option<String>,
    /// `completed` | `failed` | `continued_as_new` | `cancelled`.
    expected: String,
    partition: u16,
    run_id: String,
    token_key_hex: String,
    terminal_payload_sha256: String,
    journal_events: u64,
    module_sha256: String,
    journal_sha256: String,
    recorded_with: RecordedWith,
}

/// `[recorded_with]` (informational only).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)]
struct RecordedWith {
    ostro: String,
    wasmtime: String,
    target: String,
    date: String,
    #[serde(default)]
    tool: Option<String>,
    #[serde(default)]
    command: Option<String>,
}

/// A verified entry: metadata, module bytes and the decoded journal.
#[derive(Debug, Clone)]
struct Loaded {
    entry: Entry,
    module: Vec<u8>,
    journal: Vec<Event>,
}

fn corpus_dir() -> PathBuf {
    std::env::var_os("OSTRO_REPLAY_CORPUS")
        .map(PathBuf::from)
        .unwrap_or_else(|| common::repo_root().join("tests/replay-corpus"))
}

/// Entry directories, sorted by name (the README is the only regular file allowed).
fn entry_dirs(root: &Path) -> Vec<PathBuf> {
    let Ok(rd) = std::fs::read_dir(root) else {
        return Vec::new();
    };
    let mut dirs: Vec<PathBuf> = rd
        .map(|e| e.expect("read corpus dir").path())
        .filter(|p| p.is_dir())
        .collect();
    dirs.sort();
    dirs
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn sha_hex(b: &[u8]) -> String {
    hex(&Sha256::digest(b))
}

fn unhex32(s: &str) -> Result<[u8; 32], String> {
    if s.len() != 64 {
        return Err(format!("expected 64 hex chars, got {}", s.len()));
    }
    let mut out = [0u8; 32];
    for (i, o) in out.iter_mut().enumerate() {
        *o = u8::from_str_radix(&s[2 * i..2 * i + 2], 16).map_err(|e| format!("hex: {e}"))?;
    }
    Ok(out)
}

/// Decode `journal.bin` into events (contiguous seqs from 0, inline payloads, canonical
/// protobuf bytes).
fn decode_journal(bin: &[u8]) -> Result<Vec<Event>, String> {
    let mut out = Vec::new();
    let mut pos = 0usize;
    while pos < bin.len() {
        let len_bytes: [u8; 4] = bin
            .get(pos..pos + 4)
            .and_then(|s| s.try_into().ok())
            .ok_or_else(|| format!("truncated length prefix at byte {pos}"))?;
        let len = u32::from_le_bytes(len_bytes) as usize;
        pos += 4;
        let msg = bin
            .get(pos..pos + len)
            .ok_or_else(|| format!("truncated record at byte {pos} (len {len})"))?;
        pos += len;
        let je = JournalEvent::decode(msg).map_err(|e| format!("record {}: {e}", out.len()))?;
        if je.encode_to_vec() != msg {
            return Err(format!("record {}: not canonical protobuf", out.len()));
        }
        let seq = out.len() as u64;
        if je.seq != seq {
            return Err(format!("record {}: seq {} (want {seq})", out.len(), je.seq));
        }
        let bytes = match je.payload.and_then(|p| p.kind) {
            Some(payload_ref::Kind::Inline(b)) => b,
            other => return Err(format!("seq {seq}: payload must be inline, got {other:?}")),
        };
        let ev = decode_event(seq, je.kind, &bytes).map_err(|e| format!("seq {seq}: {e}"))?;
        if encode_event(&ev) != (je.kind, bytes.to_vec()) {
            return Err(format!(
                "seq {seq}: event bytes do not re-encode identically"
            ));
        }
        out.push(ev);
    }
    Ok(out)
}

/// Encode events as `journal.bin` (used by the negative controls only; entries are written
/// by `examples/record_corpus.rs` and ostro-server's `examples/export_run.rs`).
fn encode_journal(journal: &[Event]) -> Vec<u8> {
    let mut out = Vec::new();
    for ev in journal {
        let (kind, bytes) = encode_event(ev);
        let msg = JournalEvent {
            seq: ev.seq,
            kind,
            payload: Some(PayloadRef::inline(Bytes::from(bytes))),
            ..Default::default()
        }
        .encode_to_vec();
        out.extend_from_slice(&(msg.len() as u32).to_le_bytes());
        out.extend_from_slice(&msg);
    }
    out
}

/// Read and verify an entry directory: file set, toml, sha256s, event count, journal codec.
///
/// The checks are the library loader's ([`ostro_engine::replay_corpus::load_corpus_entry`],
/// plan 10-11, shared with `ostro replay --corpus`). On top of it this test parses the
/// metadata strictly (`deny_unknown_fields`) and decodes the same bytes independently
/// with prost: the two decoders must agree, and the metadata must match the library's.
fn load(dir: &Path) -> Result<Loaded, String> {
    let lib = load_corpus_entry(dir)?;
    let toml_text =
        std::fs::read_to_string(dir.join("entry.toml")).map_err(|e| format!("entry.toml: {e}"))?;
    let entry: Entry = toml::from_str(&toml_text).map_err(|e| format!("entry.toml: {e}"))?;
    let bin = std::fs::read(dir.join("journal.bin")).map_err(|e| format!("journal: {e}"))?;
    let journal = decode_journal(&bin)?;
    if journal != lib.journal {
        return Err("the library journal decoder disagrees with prost".to_owned());
    }
    if (
        entry.name.as_str(),
        entry.workflow.as_str(),
        entry.expected.as_str(),
        entry.partition,
        entry.run_id.as_str(),
    ) != (
        lib.name.as_str(),
        lib.workflow.as_str(),
        lib.expected.as_str(),
        lib.run.partition,
        lib.run.run_id.as_str(),
    ) || unhex32(&entry.token_key_hex)? != lib.token_key
        || entry.module_sha256 != sha_hex(&lib.module)
        || entry.journal_sha256 != sha_hex(&bin)
        || entry.input_json.as_ref().is_some_and(|i| {
            !matches!(&journal[0].body, EventBody::RunStarted(rs) if rs.input.data == i.as_bytes())
        })
    {
        return Err("the library entry.toml reader disagrees with toml".to_owned());
    }
    Ok(Loaded {
        entry,
        module: lib.module,
        journal,
    })
}

/// The journaled terminal outcome: `(expected kind, payload bytes)`. Completed hashes the
/// output bytes; failed / cancelled / continued-as-new hash the `encode_result` (postcard,
/// RESULT_CODEC_VER) bytes of the `Failure` / `Continuation`.
fn terminal_of(outcome: &ActivationOutcome) -> Option<(&'static str, Vec<u8>)> {
    Some(match outcome {
        ActivationOutcome::Completed { output } => ("completed", output.data.clone()),
        ActivationOutcome::Failed {
            failure: f @ Failure::Cancelled,
        } => ("cancelled", encode_result(f).bytes),
        ActivationOutcome::Failed { failure } => ("failed", encode_result(failure).bytes),
        ActivationOutcome::ContinuedAsNew { continuation } => {
            ("continued_as_new", encode_result(continuation).bytes)
        }
        _ => return None,
    })
}

/// Replay `l` over its whole journal through [`ostro_engine::replay_check`] (the verdict
/// logic: outcome compare and appended count); `Ok` iff the verdict is `Ok{appended: 0}`
/// and the outcome kind and terminal payload hash are as recorded.
async fn replay(l: &Loaded) -> Result<(), String> {
    let e = &l.entry;
    let last_ts = l.journal.iter().map(|e| e.ts_nanos).max().unwrap_or(0);
    let rt = Runtime::new(
        EngineConfig::default(),
        RuntimeOptions {
            // Frozen an hour past the last journaled event: a faithful replay stamps
            // nothing live, so the node clock must not matter.
            clock: Arc::new(FakeClock::with_step(last_ts + 3_600_000_000_000, 0)),
            token_key: unhex32(&e.token_key_hex)?,
            ..RuntimeOptions::default()
        },
    )
    .map_err(|err| format!("runtime: {err}"))?;
    let p = rt
        .prepare(&l.module)
        .map_err(|err| format!("prepare (production linker): {err}"))?;
    let run = RunRef {
        partition: e.partition,
        run_id: e.run_id.clone(),
    };
    match replay_check(&rt, &p, run, &l.journal).await {
        ReplayVerdict::Ok { appended: 0 } => {}
        ReplayVerdict::Ok { appended } | ReplayVerdict::OkFrontier { appended } => {
            return Err(format!(
                "replay appended {appended} event(s): the journal does not end in its terminal event"
            ));
        }
        ReplayVerdict::Diverged { seq, detail } => {
            return Err(format!(
                "nondeterminism / divergence at seq {seq}: {detail}"
            ));
        }
        ReplayVerdict::Error(err) => return Err(format!("replay error: {err}")),
    }
    let journaled = journaled_outcome(&l.journal)
        .ok_or_else(|| "journal does not end in a terminal event".to_owned())?;
    let Some((kind, bytes)) = terminal_of(&journaled) else {
        return Err(format!("journaled outcome is not terminal: {journaled:?}"));
    };
    if kind != e.expected {
        return Err(format!(
            "outcome kind {kind:?} != expected {:?} ({journaled:?})",
            e.expected
        ));
    }
    let got = sha_hex(&bytes);
    if got != e.terminal_payload_sha256 {
        return Err(format!(
            "terminal_payload_sha256 mismatch: replayed {got}, entry.toml {}",
            e.terminal_payload_sha256
        ));
    }
    Ok(())
}

async fn check_dir(dir: &Path) -> Result<Loaded, String> {
    let l = load(dir)?;
    replay(&l).await?;
    Ok(l)
}

#[tokio::test]
async fn replay_corpus_entries() {
    let root = corpus_dir();
    let dirs = entry_dirs(&root);
    if dirs.is_empty() {
        println!(
            "replay-corpus: no entries under {} (bootstrap: nothing to replay)",
            root.display()
        );
        return;
    }
    let mut failures = Vec::new();
    let mut total_events = 0u64;
    for dir in &dirs {
        let name = dir
            .file_name()
            .expect("name")
            .to_string_lossy()
            .into_owned();
        match check_dir(dir).await {
            Ok(l) => {
                total_events += l.entry.journal_events;
                let w = &l.entry.recorded_with;
                println!(
                    "replay-corpus: {name}: ok ({} events, {}; recorded {} {} on {})",
                    l.entry.journal_events, l.entry.expected, w.ostro, w.date, w.target
                );
            }
            Err(e) => failures.push(format!("{name}: {e}")),
        }
    }
    assert!(
        failures.is_empty(),
        "replay-corpus failures (determinism/compat bugs: never re-record, see README):\n{}",
        failures.join("\n")
    );
    println!(
        "replay-corpus: {} entries, {total_events} events replayed",
        dirs.len()
    );
}

// ------------------------------------------------------------------------ negative controls

/// The entry the negative controls copy: `det-basic` if present, else the first.
fn control_entry() -> Option<PathBuf> {
    let dirs = entry_dirs(&corpus_dir());
    dirs.iter()
        .find(|d| d.ends_with("det-basic"))
        .or_else(|| dirs.first())
        .cloned()
}

fn copy_entry(src: &Path) -> (tempfile::TempDir, PathBuf) {
    let t = tempfile::tempdir().expect("tempdir");
    let dst = t.path().join(src.file_name().expect("name"));
    std::fs::create_dir_all(&dst).expect("mkdir");
    for f in ENTRY_FILES {
        std::fs::copy(src.join(f), dst.join(f)).expect("copy");
    }
    (t, dst)
}

/// Rewrite `entry.toml` so it describes the files again (count and sha256s); simulates a
/// re-recorded entry whose metadata is self-consistent.
fn reseal(dir: &Path, events: usize) {
    let text = std::fs::read_to_string(dir.join("entry.toml")).expect("read toml");
    let journal_sha = sha_hex(&std::fs::read(dir.join("journal.bin")).expect("read journal"));
    let mut out = String::new();
    for line in text.lines() {
        if line.starts_with("journal_events = ") {
            out.push_str(&format!("journal_events = {events}\n"));
        } else if line.starts_with("journal_sha256 = ") {
            out.push_str(&format!("journal_sha256 = \"{journal_sha}\"\n"));
        } else {
            out.push_str(line);
            out.push('\n');
        }
    }
    std::fs::write(dir.join("entry.toml"), out).expect("write toml");
}

#[tokio::test]
async fn negative_flipped_journal_byte_is_a_sha_mismatch() {
    let Some(src) = control_entry() else {
        println!("replay-corpus: empty, negative control skipped");
        return;
    };
    let (_t, dir) = copy_entry(&src);
    check_dir(&dir).await.expect("the pristine copy replays");
    let path = dir.join("journal.bin");
    let mut bin = std::fs::read(&path).expect("read");
    let mid = bin.len() / 2;
    bin[mid] ^= 0x01;
    std::fs::write(&path, &bin).expect("write");
    let err = check_dir(&dir).await.expect_err("a flipped byte must fail");
    assert!(err.contains("journal_sha256 mismatch"), "{err}");

    // Same for the module.
    let (_t2, dir) = copy_entry(&src);
    let path = dir.join("module.wasm");
    let mut wasm = std::fs::read(&path).expect("read");
    let last = wasm.len() - 1;
    wasm[last] ^= 0x80;
    std::fs::write(&path, &wasm).expect("write");
    let err = check_dir(&dir)
        .await
        .expect_err("a flipped module byte must fail");
    assert!(err.contains("module_sha256 mismatch"), "{err}");
}

#[tokio::test]
async fn negative_truncated_journal_appends_or_diverges() {
    let Some(src) = control_entry() else {
        println!("replay-corpus: empty, negative control skipped");
        return;
    };
    let (_t, dir) = copy_entry(&src);
    let l = load(&dir).expect("load");
    let mut journal = l.journal.clone();
    journal.pop().expect("non-empty journal");

    // Truncated without resealing: the metadata no longer matches.
    std::fs::write(dir.join("journal.bin"), encode_journal(&journal)).expect("write");
    let err = check_dir(&dir)
        .await
        .expect_err("truncation must be detected");
    assert!(err.contains("journal_sha256 mismatch"), "{err}");

    // Even with self-consistent metadata (a re-recorded entry), the replay detects that the
    // journal lost its terminal event: the engine re-runs the tail and appends it.
    reseal(&dir, journal.len());
    let err = check_dir(&dir)
        .await
        .expect_err("a truncated journal must not replay clean");
    assert!(
        err.contains("appended") || err.contains("nondeterminism") || err.contains("terminal"),
        "{err}"
    );
}

#[tokio::test]
async fn negative_wrong_expected_or_payload_sha_is_reported() {
    let Some(src) = control_entry() else {
        println!("replay-corpus: empty, negative control skipped");
        return;
    };
    let (_t, dir) = copy_entry(&src);
    let mut l = load(&dir).expect("load");
    l.entry.terminal_payload_sha256 = "00".repeat(32);
    let err = replay(&l).await.expect_err("wrong payload sha");
    assert!(err.contains("terminal_payload_sha256 mismatch"), "{err}");
    let mut l = load(&dir).expect("load");
    l.entry.expected = if l.entry.expected == "completed" {
        "failed".into()
    } else {
        "completed".into()
    };
    let err = replay(&l).await.expect_err("wrong expected kind");
    assert!(err.contains("!= expected"), "{err}");
    // A different token key changes nothing for a journal without tokens, but a different
    // run id is visible to guests that read run info; either way the metadata is checked.
    let mut l = load(&dir).expect("load");
    l.entry.token_key_hex = "zz".repeat(32);
    assert!(
        replay(&l).await.is_err(),
        "bad token key hex must be an error"
    );
}

// ------------------------------------------------------------------- replay_check verdicts

/// Runtime + prepared module of a loaded entry, for direct [`replay_check`] calls.
fn runtime_of(l: &Loaded) -> (Runtime, ostro_engine::runtime::Prepared, RunRef) {
    let last_ts = l.journal.iter().map(|e| e.ts_nanos).max().unwrap_or(0);
    let rt = Runtime::new(
        EngineConfig::default(),
        RuntimeOptions {
            clock: Arc::new(FakeClock::with_step(last_ts + 3_600_000_000_000, 0)),
            token_key: unhex32(&l.entry.token_key_hex).expect("token key"),
            ..RuntimeOptions::default()
        },
    )
    .expect("runtime");
    let p = rt.prepare(&l.module).expect("prepare");
    let run = RunRef {
        partition: l.entry.partition,
        run_id: l.entry.run_id.clone(),
    };
    (rt, p, run)
}

/// Terminal entries are `Ok{appended: 0}`; a truncated (non-terminal) journal is
/// `OkFrontier` (live progress past the end, refused by the check); a journal whose command
/// identity no longer matches what the module issues is `Diverged` at that seq.
#[tokio::test]
async fn replay_check_verdicts_ok_frontier_and_diverged() {
    let Some(src) = control_entry() else {
        println!("replay-corpus: empty, replay_check verdicts skipped");
        return;
    };
    let l = load(&src).expect("load");
    let (rt, p, run) = runtime_of(&l);
    assert_eq!(
        replay_check(&rt, &p, run.clone(), &l.journal).await,
        ReplayVerdict::Ok { appended: 0 }
    );

    // Every proper prefix (non-terminal) is a frontier.
    for cut in 1..l.journal.len() {
        let v = replay_check(&rt, &p, run.clone(), &l.journal[..cut]).await;
        assert!(
            matches!(v, ReplayVerdict::OkFrontier { .. }),
            "prefix 0..{cut}: {v:?}"
        );
    }
    let v = replay_check(&rt, &p, run.clone(), &l.journal[..l.journal.len() - 1]).await;
    assert!(
        matches!(v, ReplayVerdict::OkFrontier { appended } if appended >= 1),
        "truncated journal re-runs its tail: {v:?}"
    );

    // Divergence: flip the command hash of the first StepCompleted.
    let mut j = l.journal.clone();
    let seq = j
        .iter_mut()
        .find_map(|e| match &mut e.body {
            EventBody::StepCompleted(s) => {
                s.meta.hash.0[0] ^= 0xff;
                Some(e.seq)
            }
            _ => None,
        })
        .expect("a StepCompleted in the control entry");
    for journal in [&j[..], &j[..j.len() - 1]] {
        let v = replay_check(&rt, &p, run.clone(), journal).await;
        assert!(
            matches!(&v, ReplayVerdict::Diverged { seq: s, .. } if *s == seq),
            "diverged at {seq}: {v:?}"
        );
    }

    // Malformed journals are errors, never verdicts about the module.
    assert!(matches!(
        replay_check(&rt, &p, run.clone(), &[]).await,
        ReplayVerdict::Error(_)
    ));
    assert!(matches!(
        replay_check(&rt, &p, run, &l.journal[1..]).await,
        ReplayVerdict::Error(_)
    ));
}

// ---------------------------------------------------------------- det-basic golden snapshot

/// The `det-basic` entry is the golden det-basic journal (determinism.rs) as committed bytes:
/// rendering its journal in the snapshot's line format must give the snapshot verbatim. The
/// snapshot itself is never edited here.
#[test]
fn det_basic_entry_equals_golden_snapshot() {
    let dir = corpus_dir().join("det-basic");
    if !dir.is_dir() {
        println!("replay-corpus: no det-basic entry, golden check skipped");
        return;
    }
    let l = load(&dir).expect("load det-basic");
    let mut text = String::new();
    for e in &l.journal {
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
    let snap = std::fs::read_to_string(
        common::repo_root()
            .join("crates/ostro-engine/tests/snapshots/determinism__golden_det_basic_journal.snap"),
    )
    .expect("read golden snapshot");
    // insta header: `---\n...\n---\n` then the body.
    let body = snap.splitn(3, "---\n").nth(2).expect("snapshot body");
    assert_eq!(
        text.trim_end(),
        body.trim_end(),
        "det-basic entry != golden"
    );
}
