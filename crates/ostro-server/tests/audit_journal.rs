//! The node-local, hash-chained audit journal (D-P14-11/12, 14-04): record format, chain,
//! segments, torn-tail recovery, retention, verification, the group-committing writer with
//! fail-closed permits, coalescing, the chain-head and OTLP exports, and F8 (node open,
//! backup and blob GC ignore `<data>/audit/`).
//!
//! Failpoint cases (`audit::writer_stall`, `audit::before_fsync`, `audit::mutant::
//! skip_chain`) need `--features failpoints` and `--test-threads=1`.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use opentelemetry::logs::{AnyValue, Severity};
use opentelemetry_sdk::logs::{InMemoryLogExporter, SdkLoggerProvider};
use ostro_partition::Clock;
use ostro_partition::clock::ManualClock;
use ostro_server::audit::chain;
use ostro_server::audit::export::{AuditExport, ExportItem, OtlpExport};
use ostro_server::audit::journal::{self, Record};
use ostro_server::audit::segment;
use ostro_server::audit::verify::{verify_dir, verify_dir_against};
use ostro_server::audit::writer::{AuditWriter, WriterConfig};
use ostro_types::audit::{ActorKind, AuditEvent, AuditOutcome, AuditSink, CoalesceKey};
use proptest::prelude::*;
use tempfile::TempDir;

// ---------------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------------

fn tmp() -> TempDir {
    let shm = Path::new("/dev/shm");
    if shm.is_dir()
        && let Ok(d) = tempfile::Builder::new()
            .prefix("ostro-audit-")
            .tempdir_in(shm)
    {
        return d;
    }
    tempfile::Builder::new()
        .prefix("ostro-audit-")
        .tempdir()
        .expect("tempdir")
}

fn cfg(dir: &Path, clock: &Arc<ManualClock>) -> WriterConfig {
    let mut c = WriterConfig::new(dir, 1, 7);
    c.group_commit = Duration::ZERO;
    c.reserve_timeout = Duration::from_millis(200);
    let clk: Arc<dyn Clock> = clock.clone();
    c.clock = Some(clk);
    c
}

fn ev(i: u64) -> AuditEvent {
    let mut e = AuditEvent::new("run.cancel", "otk_a", ActorKind::Token, format!("run-{i}"));
    e.tenant = Some("acme".into());
    e.request_id = Some(format!("req-{i}"));
    e.details.insert("i".into(), i.to_string());
    e
}

async fn commit(w: &AuditWriter, e: AuditEvent) {
    let p = w.reserve().expect("reserve");
    p.commit(e).await.expect("commit");
}

async fn write_n(w: &AuditWriter, n: u64) {
    for i in 0..n {
        commit(w, ev(i)).await;
    }
}

/// Every record of the journal, oldest first.
fn records(dir: &Path) -> Vec<Record> {
    let mut out = Vec::new();
    for (_, p) in segment::list_segments(dir).expect("list") {
        let bytes = fs::read(&p).expect("read");
        for line in bytes.split(|b| *b == b'\n').filter(|l| !l.is_empty()) {
            out.push(Record::parse_line(line).expect("valid line"));
        }
    }
    out
}

fn files(dir: &Path) -> Vec<PathBuf> {
    segment::list_segments(dir)
        .expect("list")
        .into_iter()
        .map(|(_, p)| p)
        .collect()
}

fn lines(p: &Path) -> Vec<Vec<u8>> {
    fs::read(p)
        .expect("read")
        .split(|b| *b == b'\n')
        .filter(|l| !l.is_empty())
        .map(<[u8]>::to_vec)
        .collect()
}

fn write_lines(p: &Path, ls: &[Vec<u8>]) {
    let mut out = Vec::new();
    for l in ls {
        out.extend_from_slice(l);
        out.push(b'\n');
    }
    fs::write(p, out).expect("write");
}

/// A closed single-segment journal with `n` API records (seq 2..=n+1 after segment_open).
async fn journal_with(n: u64) -> TempDir {
    let d = tmp();
    let clock = Arc::new(ManualClock::new(1_000));
    let w = AuditWriter::open(cfg(d.path(), &clock)).expect("open");
    write_n(&w, n).await;
    w.close();
    d
}

fn copy_dir(from: &Path) -> TempDir {
    let d = tmp();
    for e in fs::read_dir(from).expect("read_dir") {
        let e = e.expect("entry");
        fs::copy(e.path(), d.path().join(e.file_name())).expect("copy");
    }
    d
}

// ---------------------------------------------------------------------------------
// Task 1: chain, tamper, rotation, torn tail, retention, F8
// ---------------------------------------------------------------------------------

#[tokio::test]
async fn chain_of_1000_records_verifies() {
    let d = tmp();
    let clock = Arc::new(ManualClock::new(5_000));
    let w = AuditWriter::open(cfg(d.path(), &clock)).expect("open");
    write_n(&w, 1000).await;
    let (seq, hash) = w.head();
    w.close();
    let r = verify_dir(d.path()).expect("verify");
    assert_eq!(r.records, 1001, "segment_open + 1000");
    assert_eq!((r.first_seq, r.last_seq), (1, 1001));
    assert_eq!((r.last_seq, r.last_hash.clone()), (seq, hash.clone()));
    verify_dir_against(d.path(), Some((seq, &hash))).expect("head matches");
    let recs = records(d.path());
    assert_eq!(recs[0].action, journal::SEGMENT_OPEN);
    assert_eq!(
        recs[0].prev_hash_bytes(),
        Some(chain::genesis(1, 7)),
        "the first record chains from genesis(node, incarnation)"
    );
    for pair in recs.windows(2) {
        assert_eq!(pair[1].prev_hash, pair[0].hash);
        assert_eq!(pair[1].seq, pair[0].seq + 1);
    }
    let r5 = &recs[5];
    assert_eq!((r5.v, r5.ts_ms, r5.node, r5.incarnation), (1, 5_000, 1, 7));
    assert_eq!(r5.tenant.as_deref(), Some("acme"));
    assert_eq!(r5.outcome, AuditOutcome::Ok);
}

#[tokio::test]
async fn chain_continues_across_restarts() {
    let d = tmp();
    let clock = Arc::new(ManualClock::new(1));
    for inc in 1..=3u64 {
        let mut c = cfg(d.path(), &clock);
        c.incarnation = inc;
        let w = AuditWriter::open(c).expect("open");
        write_n(&w, 5).await;
        w.close();
    }
    let r = verify_dir(d.path()).expect("verify");
    assert_eq!(r.segments, 3, "one segment per incarnation");
    assert_eq!(r.records, 18);
    let incs: Vec<u64> = records(d.path()).iter().map(|r| r.incarnation).collect();
    assert_eq!(&incs[..7], &[1, 1, 1, 1, 1, 1, 2]);
}

#[tokio::test]
async fn tamper_flip_one_byte_names_the_record() {
    let base = journal_with(50).await;
    for k in [2u64, 17, 51] {
        let d = copy_dir(base.path());
        let p = &files(d.path())[0];
        let mut ls = lines(p);
        let line = &mut ls[usize::try_from(k - 1).unwrap()];
        // Flip a byte inside the target value ("run-<i>").
        let at = line.windows(4).position(|w| w == b"run-").expect("target") + 1;
        line[at] ^= 0x01;
        write_lines(p, &ls);
        let e = verify_dir(d.path()).expect_err("flipped byte");
        assert_eq!(e.seq, k, "{e}");
    }
}

#[tokio::test]
async fn tamper_delete_swap_insert_are_detected() {
    let base = journal_with(30).await;
    let k = 10u64;
    let ki = usize::try_from(k - 1).unwrap();

    // Delete record k.
    let d = copy_dir(base.path());
    let p = &files(d.path())[0];
    let mut ls = lines(p);
    ls.remove(ki);
    write_lines(p, &ls);
    let e = verify_dir(d.path()).expect_err("deleted");
    assert_eq!(e.seq, k, "{e}");

    // Swap records k and k+1.
    let d = copy_dir(base.path());
    let p = &files(d.path())[0];
    let mut ls = lines(p);
    ls.swap(ki, ki + 1);
    write_lines(p, &ls);
    let e = verify_dir(d.path()).expect_err("swapped");
    assert_eq!(e.seq, k, "{e}");

    // Insert a naive forgery (a copy of record k-1 with another target) before k.
    let d = copy_dir(base.path());
    let p = &files(d.path())[0];
    let mut ls = lines(p);
    let forged = String::from_utf8(ls[ki - 1].clone())
        .unwrap()
        .replace("run-", "rUn-");
    ls.insert(ki, forged.into_bytes());
    write_lines(p, &ls);
    let e = verify_dir(d.path()).expect_err("forged");
    assert_eq!(e.seq, k, "{e}");

    // A chain-consistent forgery at seq k still breaks the link to the original k.
    let d = copy_dir(base.path());
    let p = &files(d.path())[0];
    let mut ls = lines(p);
    let prev = Record::parse_line(&ls[ki - 1]).unwrap();
    let orig = Record::parse_line(&ls[ki]).unwrap();
    let mut fe = AuditEvent::new("token.create", "otk_evil", ActorKind::Token, "otk_x");
    fe.details = BTreeMap::new();
    let forged = Record::seal(
        fe,
        journal::Stamp {
            seq: orig.seq,
            ts_ms: orig.ts_ms,
            node: orig.node,
            incarnation: orig.incarnation,
        },
        None,
        &prev.hash_bytes().unwrap(),
    );
    let mut fl = forged.to_line();
    fl.pop();
    ls.insert(ki, fl);
    write_lines(p, &ls);
    let e = verify_dir(d.path()).expect_err("chain-consistent forgery");
    assert_eq!(e.seq, k + 1, "{e}");

    // Truncating the end is only visible against an exported head (W8).
    let d = copy_dir(base.path());
    let p = &files(d.path())[0];
    let all = lines(p);
    let last = Record::parse_line(all.last().unwrap()).unwrap();
    write_lines(p, &all[..all.len() - 1]);
    verify_dir(d.path()).expect("a shorter chain is still a chain");
    let e = verify_dir_against(d.path(), Some((last.seq, &last.hash))).expect_err("head");
    assert_eq!(e.seq, last.seq, "{e}");
}

#[derive(Clone, Debug)]
enum Mutation {
    Flip { line: usize, at: usize, bit: u8 },
    Delete { line: usize },
    Swap { a: usize, b: usize },
    Duplicate { line: usize },
    Garbage { line: usize },
}

fn mutation() -> impl Strategy<Value = Mutation> {
    prop_oneof![
        (any::<usize>(), any::<usize>(), 0u8..8).prop_map(|(line, at, bit)| Mutation::Flip {
            line,
            at,
            bit
        }),
        any::<usize>().prop_map(|line| Mutation::Delete { line }),
        (any::<usize>(), any::<usize>()).prop_map(|(a, b)| Mutation::Swap { a, b }),
        any::<usize>().prop_map(|line| Mutation::Duplicate { line }),
        any::<usize>().prop_map(|line| Mutation::Garbage { line }),
    ]
}

/// Two segments (4 KiB rotation) of a small journal, built once.
fn prop_journal() -> &'static (TempDir, u64, String) {
    static J: std::sync::OnceLock<(TempDir, u64, String)> = std::sync::OnceLock::new();
    J.get_or_init(|| {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let d = tmp();
            let clock = Arc::new(ManualClock::new(1_000));
            let mut c = cfg(d.path(), &clock);
            c.segment_bytes = 4096;
            let w = AuditWriter::open(c).expect("open");
            write_n(&w, 24).await;
            let (seq, hash) = w.head();
            w.close();
            assert!(files(d.path()).len() >= 2);
            (d, seq, hash)
        })
    })
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 256, ..ProptestConfig::default() })]

    #[test]
    fn tamper_any_single_mutation_is_detected(m in mutation(), file_pick in any::<usize>()) {
        let (base, seq, hash) = prop_journal();
        let d = copy_dir(base.path());
        let fs_ = files(d.path());
        let p = &fs_[file_pick % fs_.len()];
        let mut ls = lines(p);
        let n = ls.len();
        match m {
            Mutation::Flip { line, at, bit } => {
                // Flip any bit of the line or of its newline.
                let l = line % n;
                let mut bytes = ls[l].clone();
                bytes.push(b'\n');
                let i = at % bytes.len();
                bytes[i] ^= 1 << bit;
                let mut all = Vec::new();
                for (j, x) in ls.iter().enumerate() {
                    if j == l {
                        all.extend_from_slice(&bytes);
                    } else {
                        all.extend_from_slice(x);
                        all.push(b'\n');
                    }
                }
                fs::write(p, all).unwrap();
            }
            Mutation::Delete { line } => {
                ls.remove(line % n);
                write_lines(p, &ls);
            }
            Mutation::Swap { a, b } => {
                let (a, b) = (a % n, b % n);
                let b = if a == b { (b + 1) % n } else { b };
                ls.swap(a, b);
                write_lines(p, &ls);
            }
            Mutation::Duplicate { line } => {
                let l = line % n;
                let dup = ls[l].clone();
                ls.insert(l, dup);
                write_lines(p, &ls);
            }
            Mutation::Garbage { line } => {
                ls.insert(line % (n + 1), b"{\"v\":1,\"seq\":0}".to_vec());
                write_lines(p, &ls);
            }
        }
        prop_assert!(verify_dir_against(d.path(), Some((*seq, hash))).is_err());
    }
}

#[tokio::test]
async fn rotate_at_small_segment_bytes() {
    let d = tmp();
    let clock = Arc::new(ManualClock::new(1_000));
    let mut c = cfg(d.path(), &clock);
    c.segment_bytes = 4096;
    let w = AuditWriter::open(c).expect("open");
    write_n(&w, 200).await;
    w.close();
    let fs_ = segment::list_segments(d.path()).expect("list");
    assert!(fs_.len() >= 5, "several segments: {}", fs_.len());
    for (seq, p) in &fs_ {
        let first = Record::parse_line(&lines(p)[0]).expect("first");
        assert_eq!(first.action, journal::SEGMENT_OPEN);
        assert_eq!(first.seq, *seq, "named by its first seq");
        assert_eq!(
            p.file_name().unwrap().to_str().unwrap(),
            format!("audit-{seq:020}.jsonl")
        );
        assert_eq!(
            first.detail(journal::D_PREV_SEGMENT_LAST_HASH),
            Some(first.prev_hash.as_str())
        );
        let len = fs::metadata(p).unwrap().len();
        assert!(len < 4096 + 1024, "rotates at segment_bytes: {len}");
    }
    let r = verify_dir(d.path()).expect("verify across files");
    assert_eq!(r.segments, fs_.len() as u64);
    assert_eq!(r.records, 200 + fs_.len() as u64);
}

#[tokio::test]
async fn torn_tail_mid_line_is_truncated_and_recorded() {
    let d = journal_with(20).await;
    let p = files(d.path())[0].clone();
    let bytes = fs::read(&p).unwrap();
    let last_nl = bytes[..bytes.len() - 1]
        .iter()
        .rposition(|b| *b == b'\n')
        .unwrap();
    let keep = last_nl + 1 + (bytes.len() - last_nl) / 2;
    fs::write(&p, &bytes[..keep]).unwrap();
    let cut = (keep - (last_nl + 1)) as u64;
    assert!(verify_dir(d.path()).is_err(), "torn before recovery");

    let clock = Arc::new(ManualClock::new(9_000));
    let mut c = cfg(d.path(), &clock);
    c.incarnation = 8;
    let w = AuditWriter::open(c).expect("reopen");
    w.flush().await;
    w.close();
    let r = verify_dir(d.path()).expect("verify after recovery");
    let recs = records(d.path());
    let rec = recs
        .iter()
        .find(|r| r.action == journal::AUDIT_RECOVERED)
        .expect("audit_recovered");
    assert_eq!(
        rec.detail(journal::D_TRUNCATED_BYTES),
        Some(cut.to_string().as_str())
    );
    assert_eq!(rec.detail(journal::D_LAST_GOOD_SEQ), Some("20"));
    assert_eq!(rec.ts_ms, 9_000);
    assert_eq!(r.segments, 2);
    assert!(
        !recs.iter().any(|r| r.target == "run-19"),
        "the torn record is gone"
    );
}

#[tokio::test]
async fn torn_tail_garbage_is_truncated() {
    for garbage in [
        &b"{\"v\":1,\"seq\":22,\"ts"[..],
        b"garbage\n\0\0\0",
        b"\n\n",
    ] {
        let d = journal_with(5).await;
        let p = files(d.path())[0].clone();
        let mut bytes = fs::read(&p).unwrap();
        bytes.extend_from_slice(garbage);
        fs::write(&p, &bytes).unwrap();
        let clock = Arc::new(ManualClock::new(2_000));
        let w = AuditWriter::open(cfg(d.path(), &clock)).expect("reopen");
        w.flush().await;
        w.close();
        verify_dir(d.path()).expect("verify");
        let rec = records(d.path())
            .into_iter()
            .find(|r| r.action == journal::AUDIT_RECOVERED)
            .expect("audit_recovered");
        assert_eq!(
            rec.detail(journal::D_TRUNCATED_BYTES),
            Some(garbage.len().to_string().as_str())
        );
        assert_eq!(rec.detail(journal::D_LAST_GOOD_SEQ), Some("6"));
    }
}

#[tokio::test]
async fn torn_segment_open_drops_the_file() {
    let d = journal_with(5).await;
    // A crash right after rotation: the next segment holds half a segment_open.
    let next = d.path().join(segment::segment_name(7));
    fs::write(&next, b"{\"v\":1,\"seq\":7,\"ts_ms\":1").unwrap();
    let clock = Arc::new(ManualClock::new(2_000));
    let w = AuditWriter::open(cfg(d.path(), &clock)).expect("reopen");
    w.flush().await;
    w.close();
    let r = verify_dir(d.path()).expect("verify");
    assert_eq!(r.segments, 2);
    let recs = records(d.path());
    let rec = recs
        .iter()
        .find(|r| r.action == journal::AUDIT_RECOVERED)
        .expect("audit_recovered");
    assert_eq!(rec.detail(journal::D_LAST_GOOD_SEQ), Some("6"));
    assert_eq!(rec.seq, 8, "segment_open 7 then audit_recovered 8");
}

#[tokio::test]
async fn torn_tail_logic_refuses_a_modified_middle() {
    let d = journal_with(10).await;
    let p = files(d.path())[0].clone();
    let mut ls = lines(&p);
    ls[4][20] ^= 0x01;
    write_lines(&p, &ls);
    let clock = Arc::new(ManualClock::new(2_000));
    let e = AuditWriter::open(cfg(d.path(), &clock)).expect_err("modified journal");
    assert_eq!(e.kind(), std::io::ErrorKind::InvalidData, "{e}");
    assert_eq!(lines(&p).len(), 11, "nothing was truncated");
}

#[tokio::test]
async fn retention_by_age_trims_whole_segments_and_verifies_from_the_anchor() {
    let d = tmp();
    let clock = Arc::new(ManualClock::new(1_000));
    let mut c = cfg(d.path(), &clock);
    c.segment_bytes = 4096;
    c.retention_ms = 10_000;
    let w = AuditWriter::open(c.clone()).expect("open");
    write_n(&w, 60).await;
    let before = files(d.path()).len();
    assert!(before >= 3);
    w.tick().await;
    assert_eq!(files(d.path()).len(), before, "nothing is old yet");
    clock.advance(20_000);
    // Rotate into a fresh segment written after the cut-off.
    while files(d.path()).len() == before {
        commit(&w, ev(1_000)).await;
    }
    w.tick().await;
    let after = files(d.path());
    assert_eq!(
        after.len(),
        2,
        "only the segment overlapping the window and the active one"
    );
    let r = verify_dir(d.path()).expect("verify from the anchor");
    assert!(r.first_seq > 1);
    let recs = records(d.path());
    let trim = recs
        .iter()
        .find(|r| r.action == journal::RETENTION_TRIM)
        .expect("retention_trim");
    assert_eq!(
        trim.detail(journal::D_FIRST_KEPT_SEQ),
        Some(r.first_seq.to_string().as_str())
    );
    assert_eq!(
        trim.detail(journal::D_FIRST_KEPT_HASH),
        Some(recs[0].hash.as_str())
    );
    w.close();
    // The anchor survives a restart.
    let w = AuditWriter::open(c).expect("reopen");
    w.flush().await;
    w.close();
    verify_dir(d.path()).expect("verify after restart");

    // Without the anchor record the trimmed journal does not verify.
    let p = files(d.path()).last().unwrap().clone();
    let _ = p;
    let d2 = copy_dir(d.path());
    for f in files(d2.path()) {
        let ls: Vec<Vec<u8>> = lines(&f)
            .into_iter()
            .filter(|l| !String::from_utf8_lossy(l).contains("\"retention_trim\""))
            .collect();
        write_lines(&f, &ls);
    }
    assert!(verify_dir(d2.path()).is_err());
}

#[tokio::test]
async fn retention_by_max_bytes_keeps_the_newest_segments() {
    let d = tmp();
    let clock = Arc::new(ManualClock::new(1_000));
    let mut c = cfg(d.path(), &clock);
    c.segment_bytes = 4096;
    c.max_bytes = 3 * 4096;
    let w = AuditWriter::open(c).expect("open");
    write_n(&w, 150).await;
    assert!(files(d.path()).len() > 4);
    w.tick().await;
    w.close();
    let fs_ = files(d.path());
    let total: u64 = fs_.iter().map(|p| fs::metadata(p).unwrap().len()).sum();
    assert!(
        total <= 3 * 4096 + 2 * 1024,
        "bounded: {total} in {} files",
        fs_.len()
    );
    let r = verify_dir(d.path()).expect("verify");
    assert!(r.first_seq > 1);
    assert_eq!(r.last_seq, records(d.path()).last().unwrap().seq);
}

/// F8: node open, `ostro backup` and blob GC ignore `<data>/audit/`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ignored_by_node_open_backup_and_blob_gc() {
    use ostro_server::config::ServerConfig;
    let d = tmp();
    let mut c = ServerConfig::server_defaults();
    c.api.listen = "127.0.0.1:0".parse().unwrap();
    c.cluster.listen = "127.0.0.1:0".parse().unwrap();
    c.node.data_dir = d.path().to_path_buf();
    c.node.partitions = 2;
    let start = || async {
        tokio::time::timeout(
            Duration::from_secs(120),
            ostro_server::server::start(c.clone()),
        )
        .await
        .expect("start timed out")
        .expect("server start")
    };
    let h = start().await;
    h.shutdown().await.expect("shutdown");
    let audit = d.path().join("audit");
    fs::write(audit.join("operator-notes.txt"), b"not a segment").unwrap();
    let snap = |dir: &Path| -> BTreeMap<String, Vec<u8>> {
        fs::read_dir(dir)
            .unwrap()
            .map(|e| {
                let e = e.unwrap();
                (
                    e.file_name().to_string_lossy().into_owned(),
                    fs::read(e.path()).unwrap(),
                )
            })
            .collect()
    };
    let before = snap(&audit);
    assert_eq!(
        before.len(),
        2,
        "one segment + the foreign file: {:?}",
        before.keys()
    );
    verify_dir(&audit).expect("the server's journal verifies");

    // Offline backup: nothing from audit/ in the archive.
    let out = tmp();
    let tar = out.path().join("b.tar");
    let clock: Arc<dyn Clock> = Arc::new(ostro_partition::clock::SystemClock);
    ostro_backup::archive::backup_offline(d.path(), c.node.node_id, clock, &tar)
        .await
        .expect("offline backup");
    let unpacked = tmp();
    ostro_backup::archive::unpack(fs::File::open(&tar).unwrap(), unpacked.path()).unwrap();
    let mut stack = vec![unpacked.path().to_path_buf()];
    while let Some(dir) = stack.pop() {
        for e in fs::read_dir(&dir).unwrap() {
            let e = e.unwrap();
            let name = e.file_name().to_string_lossy().into_owned();
            assert!(!name.starts_with("audit"), "backup contains {name}");
            if e.file_type().unwrap().is_dir() {
                stack.push(e.path());
            }
        }
    }

    // Blob GC with an empty mark set and a cut-off in the future.
    let blobs = ostro_blob::BlobStore::open(d.path()).expect("blob store");
    let far = std::time::SystemTime::now() + Duration::from_secs(86_400);
    blobs
        .sweep(&std::collections::HashSet::new(), far)
        .expect("sweep");
    assert_eq!(snap(&audit), before, "backup and GC left audit/ alone");

    // Node open again: the journal continues in a new segment.
    let h = start().await;
    h.shutdown().await.expect("shutdown");
    let r = verify_dir(&audit).expect("verify after restart");
    assert_eq!(r.segments, 2);
    assert!(audit.join("operator-notes.txt").is_file());
}

// ---------------------------------------------------------------------------------
// Task 2: writer, coalescing, export
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writer_group_commit_batches_concurrent_committers() {
    let d = tmp();
    let clock = Arc::new(ManualClock::new(1_000));
    let mut c = cfg(d.path(), &clock);
    c.group_commit = Duration::from_millis(20);
    let w = AuditWriter::open(c).expect("open");
    w.flush().await;
    let s0 = w.stats();
    let mut tasks = Vec::new();
    for i in 0..32 {
        let w = Arc::clone(&w);
        tasks.push(tokio::spawn(async move { commit(&w, ev(i)).await }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    let s1 = w.stats();
    let recs = s1.records - s0.records;
    let fsyncs = s1.fsyncs - s0.fsyncs;
    println!(
        "group commit: {recs} records / {fsyncs} fsyncs ({} batches)",
        s1.batches - s0.batches
    );
    assert_eq!(recs, 32);
    assert!(fsyncs < recs, "batched: {fsyncs} fsyncs for {recs} records");
    w.close();
    assert_eq!(verify_dir(d.path()).unwrap().records, 33);
}

#[tokio::test]
async fn writer_commit_resolves_only_when_durable() {
    let d = tmp();
    let clock = Arc::new(ManualClock::new(1_000));
    let mut c = cfg(d.path(), &clock);
    c.group_commit = Duration::from_millis(50);
    let w = AuditWriter::open(c).expect("open");
    let p = w.reserve().expect("reserve");
    let mut e = ev(42);
    e.outcome = AuditOutcome::Failed("boom".into());
    p.commit(e).await.expect("commit");
    // Visible on disk the moment commit resolved (no close or flush).
    let recs = records(d.path());
    let last = recs.last().unwrap();
    assert_eq!(last.target, "run-42");
    assert_eq!(last.outcome, AuditOutcome::Failed("boom".into()));
    assert_eq!(w.head(), (last.seq, last.hash.clone()));
    w.close();
}

#[tokio::test]
async fn writer_full_queue_fails_reserve_after_the_timeout() {
    let d = tmp();
    let clock = Arc::new(ManualClock::new(1_000));
    let mut c = cfg(d.path(), &clock);
    c.queue = 2;
    let w = AuditWriter::open(c).expect("open");
    w.flush().await;
    let before = records(d.path()).len();
    let a = w.reserve().expect("1");
    let b = w.reserve().expect("2");
    let t = Instant::now();
    let e = w.reserve().expect_err("queue full");
    let waited = t.elapsed();
    assert!(waited >= Duration::from_millis(190), "waited {waited:?}");
    assert!(waited < Duration::from_secs(5), "waited {waited:?}");
    assert!(e.0.contains("queue full"), "{e}");
    assert_eq!(records(d.path()).len(), before, "nothing written");
    drop(a);
    let c3 = w.reserve().expect("a dropped permit frees its slot");
    c3.commit(ev(3)).await.expect("commit");
    b.commit(ev(2)).await.expect("commit");
    w.close();
    assert!(w.reserve().is_err(), "a closed writer fails closed");
    assert_eq!(records(d.path()).len(), before + 2);
}

#[tokio::test]
async fn writer_coalesces_per_key_and_window() {
    let d = tmp();
    let clock = Arc::new(ManualClock::new(10_000));
    let mut c = cfg(d.path(), &clock);
    c.secret_coalesce_ms = 60_000;
    c.auth_failure_coalesce_ms = 30_000;
    let w = AuditWriter::open(c).expect("open");
    let secret = CoalesceKey::SecretAccess {
        run: "r1".into(),
        module: "m".into(),
        secret: "db".into(),
        origin: "http".into(),
    };
    let auth = CoalesceKey::AuthFailure {
        token_id: "malformed".into(),
        client_ip: "10.0.0.1".into(),
    };
    let sev = || AuditEvent::new("secret.access", "system", ActorKind::System, "db");
    let aev = || AuditEvent::new("auth.failure", "anonymous", ActorKind::Anonymous, "");
    for _ in 0..5 {
        w.record_coalesced(sev(), secret.clone());
        clock.advance(1_000);
    }
    w.record_coalesced(aev(), auth.clone());
    w.record_coalesced(aev(), auth.clone());
    w.tick().await;
    let n0 = records(d.path()).len();
    assert!(
        !records(d.path()).iter().any(|r| r.count.is_some()),
        "nothing before the window ends"
    );
    clock.advance(30_000); // auth window (30 s) over, secret window (60 s) not yet
    w.tick().await;
    let recs = records(d.path());
    let a: Vec<&Record> = recs.iter().filter(|r| r.action == "auth.failure").collect();
    assert_eq!(a.len(), 1);
    assert_eq!(a[0].count, Some(2));
    assert_eq!(
        recs.iter().filter(|r| r.action == "secret.access").count(),
        0
    );
    clock.advance(30_000);
    w.tick().await;
    let recs = records(d.path());
    let s: Vec<&Record> = recs
        .iter()
        .filter(|r| r.action == "secret.access")
        .collect();
    assert_eq!(s.len(), 1);
    assert_eq!(s[0].count, Some(5));
    assert_eq!(s[0].first_ts_ms, Some(10_000));
    assert_eq!(s[0].last_ts_ms, Some(14_000));
    assert_eq!(recs.len(), n0 + 2);
    // flush() writes a pending window early.
    for _ in 0..3 {
        w.record_coalesced(sev(), secret.clone());
    }
    w.flush().await;
    let recs = records(d.path());
    assert_eq!(recs.last().unwrap().count, Some(3));
    w.close();
    verify_dir(d.path()).expect("verify");
}

fn mem_provider() -> (SdkLoggerProvider, InMemoryLogExporter) {
    let exp = InMemoryLogExporter::default();
    let p = SdkLoggerProvider::builder()
        .with_simple_exporter(exp.clone())
        .build();
    (p, exp)
}

fn attr<'a>(r: &'a opentelemetry_sdk::logs::SdkLogRecord, k: &str) -> Option<&'a AnyValue> {
    r.attributes_iter()
        .find(|(key, _)| key.as_str() == k)
        .map(|(_, v)| v)
}

async fn settle<F: Fn() -> bool>(f: F) {
    for _ in 0..200 {
        if f() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn head_export_once_per_interval_and_at_shutdown() {
    let d = tmp();
    let clock = Arc::new(ManualClock::new(100_000));
    let (p, exp) = mem_provider();
    let mut c = cfg(d.path(), &clock);
    c.head_export_interval_ms = 1_000;
    c.export = Some(Arc::new(OtlpExport::with_provider(p)));
    let w = AuditWriter::open(c).expect("open");
    let heads = || {
        exp.get_emitted_logs()
            .unwrap()
            .into_iter()
            .filter(|l| l.record.event_name() == Some("ostro.audit.head"))
            .count()
    };
    w.tick().await;
    assert_eq!(w.stats().heads, 0, "the interval has not elapsed");
    clock.advance(1_000);
    w.tick().await;
    w.tick().await;
    assert_eq!(w.stats().heads, 1);
    commit(&w, ev(1)).await;
    clock.advance(999);
    w.tick().await;
    assert_eq!(w.stats().heads, 1);
    clock.advance(1);
    w.tick().await;
    assert_eq!(w.stats().heads, 2);
    w.flush().await; // shutdown export
    assert_eq!(w.stats().heads, 3);
    settle(|| heads() == 3).await;
    let logs = exp.get_emitted_logs().unwrap();
    let last = logs
        .iter()
        .rev()
        .find(|l| l.record.event_name() == Some("ostro.audit.head"))
        .unwrap();
    let (seq, hash) = w.head();
    assert_eq!(
        attr(&last.record, "ostro.audit.seq"),
        Some(&AnyValue::Int(i64::try_from(seq).unwrap()))
    );
    assert_eq!(
        attr(&last.record, "ostro.audit.hash"),
        Some(&AnyValue::from(hash))
    );
    assert_eq!(heads(), 3);
    w.close();
}

#[tokio::test]
async fn export_records_as_otlp_logrecords() {
    let d = tmp();
    let clock = Arc::new(ManualClock::new(1_700_000_000_000));
    let (p, exp) = mem_provider();
    let mut c = cfg(d.path(), &clock);
    c.export = Some(Arc::new(OtlpExport::with_provider(p)));
    let w = AuditWriter::open(c).expect("open");
    let mut e = AuditEvent::new("module.upload", "otk_a", ActorKind::Token, "abc123");
    e.tenant = Some("acme".into());
    e.outcome = AuditOutcome::Denied("signature_required".into());
    e.details.insert("key_id".into(), "k1".into());
    commit(&w, e).await;
    let find = || {
        exp.get_emitted_logs().unwrap().into_iter().find(|l| {
            l.record.event_name() == Some("ostro.audit")
                && l.record.body() == Some(&AnyValue::from("module.upload"))
        })
    };
    settle(|| find().is_some()).await;
    let l = find().expect("exported");
    let r = &l.record;
    assert_eq!(r.severity_number(), Some(Severity::Info));
    let s = |k: &str| attr(r, k).cloned();
    assert_eq!(
        s("ostro.audit.action"),
        Some(AnyValue::from("module.upload"))
    );
    assert_eq!(s("ostro.audit.actor"), Some(AnyValue::from("otk_a")));
    assert_eq!(s("ostro.audit.actor_kind"), Some(AnyValue::from("token")));
    assert_eq!(s("ostro.audit.tenant"), Some(AnyValue::from("acme")));
    assert_eq!(s("ostro.audit.outcome"), Some(AnyValue::from("denied")));
    assert_eq!(
        s("ostro.audit.outcome_reason"),
        Some(AnyValue::from("signature_required"))
    );
    assert_eq!(s("ostro.audit.detail.key_id"), Some(AnyValue::from("k1")));
    assert_eq!(s("ostro.audit.incarnation"), Some(AnyValue::from("7")));
    assert!(s("ostro.audit.details").is_none(), "details are flattened");
    let (seq, hash) = w.head();
    assert_eq!(
        s("ostro.audit.seq"),
        Some(AnyValue::Int(i64::try_from(seq).unwrap()))
    );
    assert_eq!(s("ostro.audit.hash"), Some(AnyValue::from(hash)));
    w.close();
}

/// An export target that blocks until released.
struct Gate {
    open: Mutex<bool>,
    cv: std::sync::Condvar,
    seen: Mutex<u64>,
}

impl AuditExport for Gate {
    fn emit(&self, _item: &ExportItem) {
        let mut g = self.open.lock().unwrap();
        while !*g {
            g = self.cv.wait(g).unwrap();
        }
        *self.seen.lock().unwrap() += 1;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn export_full_channel_drops_the_copy_never_the_record() {
    let d = tmp();
    let clock = Arc::new(ManualClock::new(1_000));
    let gate = Arc::new(Gate {
        open: Mutex::new(false),
        cv: std::sync::Condvar::new(),
        seen: Mutex::new(0),
    });
    let mut c = cfg(d.path(), &clock);
    c.group_commit = Duration::from_millis(2);
    let g: Arc<dyn AuditExport> = gate.clone();
    c.export = Some(g);
    let w = AuditWriter::open(c).expect("open");
    let mut tasks = Vec::new();
    for t in 0..32u64 {
        let w = Arc::clone(&w);
        tasks.push(tokio::spawn(async move {
            for i in 0..40u64 {
                commit(&w, ev(t * 1000 + i)).await;
            }
        }));
    }
    for t in tasks {
        tokio::time::timeout(Duration::from_secs(60), t)
            .await
            .expect("the writer never waits on export")
            .unwrap();
    }
    let s = w.stats();
    assert!(s.export_dropped > 0, "{s:?}");
    *gate.open.lock().unwrap() = true;
    gate.cv.notify_all();
    w.close();
    let r = verify_dir(d.path()).expect("verify");
    assert_eq!(r.records, 1 + 32 * 40, "every journal record kept");
    let seen = *gate.seen.lock().unwrap();
    let s = w.stats();
    assert_eq!(
        seen + s.export_dropped,
        r.records + s.heads,
        "offered (records + chain heads) = exported + dropped"
    );
}

/// The seam: build_ctx opens the writer, start hands it the clock, before_shutdown flushes,
/// the services stop closes it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn writer_seam_open_start_flush_stop() {
    use ostro_meta::MetaHandle;
    use ostro_server::config::ServerConfig;
    use ostro_server::p14::{self, P14Init};
    let d = tmp();
    let meta = Arc::new(MetaHandle::start(d.path(), 1).await.expect("meta"));
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(77_000));
    let ctx = p14::build_ctx(P14Init {
        cfg: Arc::new(ServerConfig::server_defaults()),
        meta,
        clock,
        node_id: 1,
        data_dir: d.path().to_path_buf(),
        level_max: ostro_types::compat::level_max(),
    })
    .expect("build_ctx");
    let sink = p14::audit_sink(&ctx);
    let permit = sink.reserve().expect("reserve before start");
    let services = p14::start(Arc::clone(&ctx)).await.expect("start");
    permit.commit(ev(1)).await.expect("commit");
    p14::before_shutdown(&ctx).await;
    services.stop().await;
    assert!(sink.reserve().is_err(), "closed after the services stopped");
    let audit = d.path().join("audit");
    let r = verify_dir(&audit).expect("verify");
    assert_eq!(r.records, 2);
    let recs = records(&audit);
    assert_eq!(recs[1].ts_ms, 77_000, "the injected clock");
    assert_eq!(recs[1].incarnation, ctx.incarnation);

    // `[audit] enabled = false`: no journal.
    let d2 = tmp();
    let meta = Arc::new(MetaHandle::start(d2.path(), 1).await.expect("meta"));
    let mut cfg = ServerConfig::server_defaults();
    cfg.audit.enabled = false;
    let ctx = p14::build_ctx(P14Init {
        cfg: Arc::new(cfg),
        meta,
        clock: Arc::new(ManualClock::new(1)),
        node_id: 1,
        data_dir: d2.path().to_path_buf(),
        level_max: ostro_types::compat::level_max(),
    })
    .expect("build_ctx");
    p14::audit_sink(&ctx)
        .reserve()
        .expect("noop")
        .commit(ev(1))
        .await
        .expect("noop commit");
    assert!(!d2.path().join("audit").exists());
}

// ---------------------------------------------------------------------------------
// Failpoints (`--features failpoints`, `--test-threads=1`)
// ---------------------------------------------------------------------------------

#[cfg(feature = "failpoints")]
mod failpoints {
    use super::*;

    #[tokio::test]
    async fn failpoint_writer_stall_fails_reserve_and_writes_nothing() {
        let _s = fail::FailScenario::setup();
        let d = tmp();
        let clock = Arc::new(ManualClock::new(1_000));
        let w = AuditWriter::open(cfg(d.path(), &clock)).expect("open");
        w.flush().await;
        let before = records(d.path()).len();
        fail::cfg("audit::writer_stall", "return").unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let t = Instant::now();
        let e = w.reserve().expect_err("stalled writer");
        assert!(
            t.elapsed() >= Duration::from_millis(190),
            "{:?}",
            t.elapsed()
        );
        assert!(e.0.contains("stalled"), "{e}");
        assert_eq!(records(d.path()).len(), before, "nothing written");
        fail::remove("audit::writer_stall");
        tokio::time::sleep(Duration::from_millis(100)).await;
        commit(&w, ev(1)).await;
        w.close();
        assert_eq!(records(d.path()).len(), before + 1);
    }

    #[tokio::test]
    async fn failpoint_before_fsync_poisons_until_restart() {
        let _s = fail::FailScenario::setup();
        let d = tmp();
        let clock = Arc::new(ManualClock::new(1_000));
        let w = AuditWriter::open(cfg(d.path(), &clock)).expect("open");
        commit(&w, ev(1)).await;
        fail::cfg("audit::before_fsync", "return").unwrap();
        let p = w.reserve().expect("reserve");
        let e = p.commit(ev(2)).await.expect_err("fsync EIO");
        assert!(e.0.contains("audit::before_fsync"), "{e}");
        fail::remove("audit::before_fsync");
        assert!(w.is_poisoned());
        let e = w.reserve().expect_err("poisoned: fail closed");
        assert!(e.0.contains("failed"), "{e}");
        w.close();
        // A restart recovers (the unsynced record may or may not be there; the chain holds).
        let w = AuditWriter::open(cfg(d.path(), &clock)).expect("reopen");
        commit(&w, ev(3)).await;
        w.close();
        verify_dir(d.path()).expect("verify");
    }

    #[tokio::test]
    async fn mutant_skip_chain_is_caught_by_verify() {
        let _s = fail::FailScenario::setup();
        let d = tmp();
        let clock = Arc::new(ManualClock::new(1_000));
        let w = AuditWriter::open(cfg(d.path(), &clock)).expect("open");
        commit(&w, ev(1)).await;
        commit(&w, ev(2)).await;
        fail::cfg("audit::mutant::skip_chain", "1*return").unwrap();
        commit(&w, ev(3)).await;
        fail::remove("audit::mutant::skip_chain");
        commit(&w, ev(4)).await;
        w.close();
        let e = verify_dir(d.path()).expect_err("wrong prev_hash");
        assert_eq!(e.seq, 4, "segment_open, run-1, run-2, then the mutant: {e}");
    }
}
