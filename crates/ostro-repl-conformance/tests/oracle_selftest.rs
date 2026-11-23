//! 12-03 Task 3: every oracle passes on a correct fixture and fails, naming the culprit, on
//! a deliberately damaged one. Fixtures are written with the real formats: partition logs
//! through the log type behind `ostro_partition::roles::PartitionLogHandle`, state stores
//! through `PartitionStore`, chunks and manifests through `ostro-backup`, meta logs through
//! a replayed `MetaState`.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use ostro_backup::chunk::{chunk_key, chunk_sha, encode as encode_chunk};
use ostro_backup::manifest::{self, Manifest, PartitionCut};
use ostro_log::ReplicatedLog;
use ostro_meta::ext::ElectionInfo;
use ostro_meta::{MetaCommand, MetaKey, MetaRecord, MetaState, PartitionAssignment};
use ostro_objstore::{LocalFs, ObjectStore};
use ostro_partition::roles::open_partition_log_for_tests;
use ostro_partition::store::{MetaUpdate, PartitionStore, StoreConfig, Table};
use ostro_repl_conformance::harness::inproc::{Ack, InProcCluster};
use ostro_repl_conformance::harness::{ClusterHarness, ClusterSpec, DataDirView, MetaLogEntry};
use ostro_repl_conformance::oracle::{
    cluster_formed, disk, election, la_rule, objstore, replica, standby, status,
};
use ostro_types::command::{StartRun, encode_command};
use ostro_types::{Command, Epoch, Offset, PartitionCount, PartitionId, RunId};
use serde_json::json;
use tempfile::TempDir;

// ---------------------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------------------

fn start_run(run: &RunId) -> Bytes {
    encode_command(&Command::start_run(StartRun {
        run_id: run.as_bytes().to_vec(),
        workflow: "smoke".into(),
        ..Default::default()
    }))
}

fn la(epoch: u32, node: u64) -> Bytes {
    encode_command(&Command::leader_announced(epoch, node, 0))
}

/// Write `batches` (`(epoch, commands)`, one envelope each) as partition `p`'s log under
/// `<dir>/wal`.
async fn write_log(dir: &Path, p: PartitionId, batches: Vec<(u32, Vec<Bytes>)>) {
    let (node, log) = open_partition_log_for_tests(&dir.join("wal"), p, 1, Offset(0))
        .await
        .expect("open log");
    // The fixture log opens unassigned (12-08): lead it the RF=1 way first.
    let first = batches.first().map_or(1, |(e, _)| *e);
    log.become_leader(Epoch(first), ostro_log::ReplicaSet::solo(1))
        .expect("become leader");
    for (e, cmds) in batches {
        let r = log.append(Epoch(e), cmds).await.expect("append");
        log.wait_committed(r.last).await.expect("committed");
    }
    node.shutdown().await;
}

fn view(node: u64, dir: &TempDir) -> DataDirView {
    DataDirView {
        node,
        root: dir.path().to_path_buf(),
    }
}

fn copy_tree(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).expect("mkdir");
    for e in std::fs::read_dir(from).expect("read_dir") {
        let e = e.expect("entry");
        let dst = to.join(e.file_name());
        if e.file_type().expect("type").is_dir() {
            copy_tree(&e.path(), &dst);
        } else {
            std::fs::copy(e.path(), dst).expect("copy");
        }
    }
}

/// Replay `cmds` over a fresh state, as `read_meta_log` does.
fn replay(cmds: Vec<MetaCommand>) -> Vec<MetaLogEntry> {
    let mut state = MetaState::default();
    cmds.into_iter()
        .enumerate()
        .map(|(i, command)| {
            let index = i as u64 + 1;
            let applied_result = state.apply(index, command.clone());
            MetaLogEntry {
                index,
                command,
                applied_result,
            }
        })
        .collect()
}

fn init(partitions: u16) -> MetaCommand {
    MetaCommand::InitCluster {
        cluster_id: "conf-0".into(),
        node_id: 1,
        partitions,
        created_at_ms: 0,
        token_key: [3; 32],
    }
}

fn ack(run: &RunId) -> Ack {
    Ack {
        run_id: run.to_string(),
        partition: run.partition().0,
    }
}

// ---------------------------------------------------------------------------------------
// disk
// ---------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disk_names_the_acked_run_missing_from_a_required_copy() {
    let p = PartitionId(1);
    let (r1, r2) = (RunId::new_v7(p), RunId::new_v7(p));
    let full = tempfile::tempdir().expect("dir");
    write_log(
        full.path(),
        p,
        vec![
            (1, vec![la(1, 1), start_run(&r1)]),
            (1, vec![start_run(&r2)]),
        ],
    )
    .await;
    let short = tempfile::tempdir().expect("dir");
    write_log(short.path(), p, vec![(1, vec![la(1, 1), start_run(&r1)])]).await;
    let copy = tempfile::tempdir().expect("dir");
    copy_tree(&full.path().join("wal"), &copy.path().join("wal"));

    let acks = [ack(&r1), ack(&r2)];
    let ok = disk::acked_on_disk(&[view(1, &full), view(3, &copy)], &acks, 2);
    assert!(ok.is_ok(), "{ok}");

    let bad = disk::acked_on_disk(&[view(1, &full), view(2, &short)], &acks, 2);
    assert!(!bad.is_ok());
    assert_eq!(bad.violations.len(), 1, "{bad}");
    assert!(bad.mentions(&r2.to_string()), "{bad}");
    assert!(!bad.mentions(&r1.to_string()), "{bad}");
    assert!(bad.mentions("[1]"), "names the holder: {bad}");
    let f = bad.into_result().expect_err("failure");
    assert_eq!(f.oracle, "disk");

    // The scan is measured: three records, contiguous offsets, durable end 3.
    let scan = disk::scan_partition(&view(1, &full), p).expect("scan");
    assert_eq!(scan.durable_end(), Some(Offset(3)));
    assert!(scan.stopped.is_none());
}

// ---------------------------------------------------------------------------------------
// replica
// ---------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replica_reports_a_flipped_envelope_byte_with_partition_and_offset() {
    let p = PartitionId(2);
    let r = RunId::new_v7(p);
    let a = tempfile::tempdir().expect("dir");
    write_log(
        a.path(),
        p,
        vec![
            (1, vec![la(1, 1)]),
            (1, vec![start_run(&r)]),
            (1, vec![start_run(&RunId::new_v7(p))]),
        ],
    )
    .await;
    let b = tempfile::tempdir().expect("dir");
    copy_tree(&a.path().join("wal"), &b.path().join("wal"));
    let hw = Offset(3);
    let ok = replica::identical_below_hw(&[view(1, &a), view(2, &b)], p, hw);
    assert!(ok.is_ok(), "{ok}");

    // Fixture copy of node 2's envelope at offset 1 with one byte flipped.
    let s1 = disk::scan_partition(&view(1, &a), p).expect("scan");
    let mut s2 = disk::scan_partition(&view(2, &b), p).expect("scan");
    let mut env = s2.records[1].envelope.to_vec();
    let last = env.len() - 1;
    env[last] ^= 0x01;
    s2.records[1].envelope = Bytes::from(env);
    let bad = replica::compare_below_hw(&[s1.clone(), s2], hw);
    assert!(bad.mentions("partition 2 offset 1"), "{bad}");
    assert!(bad.mentions("node 1 and node 2"), "{bad}");

    // Above HW nothing is compared.
    let ok = replica::compare_below_hw(
        &[s1, disk::scan_partition(&view(2, &b), p).expect("scan")],
        Offset(1),
    );
    assert!(ok.is_ok(), "{ok}");

    // On disk: a damaged byte in node 2's last record stops its verified scan below HW.
    let seg = std::fs::read_dir(b.path().join("wal/p/2"))
        .expect("segs")
        .map(|e| e.expect("e").path())
        .find(|p| std::fs::metadata(p).map(|m| m.len() > 0).unwrap_or(false))
        .expect("non-empty segment");
    let mut bytes = std::fs::read(&seg).expect("read");
    let n = bytes.len();
    bytes[n - 5] ^= 0xff;
    std::fs::write(&seg, bytes).expect("write");
    let bad = replica::identical_below_hw(&[view(1, &a), view(2, &b)], p, hw);
    assert!(
        bad.mentions("node 2 partition 2: verified log ends at 2 below HW 3"),
        "{bad}"
    );
}

// ---------------------------------------------------------------------------------------
// standby
// ---------------------------------------------------------------------------------------

fn store_with(dir: &Path, p: PartitionId, applied: u64, value: &[u8]) {
    let count = PartitionCount::new(4).expect("count");
    let (store, meta) = PartitionStore::open(dir, p, count, &StoreConfig::default()).expect("open");
    let mut b = store.batch();
    b.insert(Table::Runs, b"run-1".to_vec(), value.to_vec());
    b.commit(&MetaUpdate {
        applied: Offset(applied),
        state_hash: [7; 32],
        cursor: meta.cursor.clone(),
        inbound_seen_changed: Vec::new(),
    })
    .expect("commit");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standby_reports_a_diverging_row_by_table() {
    let p = PartitionId(0);
    let count = PartitionCount::new(4).expect("count");
    let (a, b, c, d) = (
        tempfile::tempdir().expect("a"),
        tempfile::tempdir().expect("b"),
        tempfile::tempdir().expect("c"),
        tempfile::tempdir().expect("d"),
    );
    store_with(a.path(), p, 5, b"v1");
    store_with(b.path(), p, 5, b"v1");
    store_with(c.path(), p, 5, b"v2"); // same applied offset and state_hash, other row
    store_with(d.path(), p, 6, b"v1");
    let ok = standby::equal_state(&[view(1, &a), view(2, &b)], p, count);
    assert!(ok.is_ok(), "{ok}");
    let bad = standby::equal_state(&[view(1, &a), view(3, &c)], p, count);
    assert!(bad.mentions("node 1 and node 3"), "{bad}");
    assert!(bad.mentions("\"runs\""), "names the table: {bad}");
    let bad = standby::equal_state(&[view(1, &a), view(4, &d)], p, count);
    assert!(bad.mentions("not at an equal applied offset"), "{bad}");
    // The node's own dir is untouched (opened on a copy): it still opens here.
    drop(PartitionStore::open(a.path(), p, count, &StoreConfig::default()).expect("reopen"));
}

// ---------------------------------------------------------------------------------------
// la_rule
// ---------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn la_rule_reports_an_announcement_by_the_wrong_node() {
    let p = PartitionId(1);
    let meta = replay(vec![
        init(4),
        MetaCommand::SetPartitionLeader {
            partition: 1,
            epoch: 1,
            leader: 1,
            isr: vec![1],
        },
    ]);
    let good = tempfile::tempdir().expect("dir");
    write_log(
        good.path(),
        p,
        vec![(1, vec![la(1, 1), start_run(&RunId::new_v7(p))])],
    )
    .await;
    let ok = la_rule::check(&view(1, &good), p, &meta);
    assert!(ok.is_ok(), "{ok}");

    let wrong = tempfile::tempdir().expect("dir");
    write_log(wrong.path(), p, vec![(1, vec![la(1, 2)])]).await;
    let bad = la_rule::check(&view(2, &wrong), p, &meta);
    assert!(
        bad.mentions("LeaderAnnounced by node 2 but meta's leader for epoch 1 is node 1"),
        "{bad}"
    );

    // An epoch without an announcement, and an epoch meta never committed.
    let missing = tempfile::tempdir().expect("dir");
    write_log(
        missing.path(),
        p,
        vec![
            (1, vec![la(1, 1)]),
            (2, vec![start_run(&RunId::new_v7(p))]),
            (3, vec![la(3, 1)]),
        ],
    )
    .await;
    let bad = la_rule::check(&view(1, &missing), p, &meta);
    assert!(
        bad.mentions("epoch 2 (offset 1): first command is not LeaderAnnounced"),
        "{bad}"
    );
    assert!(bad.mentions("no leader for epoch 3"), "{bad}");
}

// ---------------------------------------------------------------------------------------
// election
// ---------------------------------------------------------------------------------------

fn placement() -> Vec<MetaCommand> {
    vec![
        init(1),
        MetaCommand::Put {
            key: MetaKey::Partition(0),
            expected_version: Some(0),
            record: MetaRecord::Partition(PartitionAssignment {
                partition: 0,
                leader: 1,
                epoch: 1,
                isr: vec![1, 2],
                replicas: vec![1, 2, 3],
                isr_version: 1,
                min_insync: 2,
                ext: Vec::new(),
            }),
        },
    ]
}

fn elect(epoch: u32, leader: u64, isr: Vec<u64>) -> MetaCommand {
    MetaCommand::ElectLeader {
        partition: 0,
        epoch,
        leader,
        isr,
        expected_isr_version: 1,
        info: ElectionInfo::default(),
    }
}

#[test]
fn election_flags_out_of_isr_proposals_replacements_and_late_set_leader() {
    let ok = election::isr_only(&replay([placement(), vec![elect(2, 2, vec![2])]].concat()));
    assert!(ok.is_ok(), "{ok}");
    // An RF=1 log that never bootstraps a replicated cluster keeps SetPartitionLeader.
    let rf1 = election::isr_only(&replay(vec![
        init(1),
        MetaCommand::SetPartitionLeader {
            partition: 0,
            epoch: 1,
            leader: 1,
            isr: vec![1],
        },
    ]));
    assert!(rf1.is_ok(), "{rf1}");

    // (a) a proposal (applied or rejected) electing a node outside the published ISR.
    let log = replay([placement(), vec![elect(2, 3, vec![3])]].concat());
    let bad = election::isr_only(&log);
    assert!(
        bad.mentions("elects node 3 outside the published ISR [1, 2]"),
        "{bad}"
    );

    // (a) an offline election whose ISR adds a non-member.
    let bad = election::isr_only(&replay(
        [placement(), vec![elect(2, 0, vec![1, 2, 3])]].concat(),
    ));
    assert!(
        bad.mentions("not a subset of the published ISR [1, 2]"),
        "{bad}"
    );
    assert!(
        !bad.mentions("outside the published ISR"),
        "leader 0 is excepted: {bad}"
    );

    // (b) a second Put(Partition) after the bootstrap placement.
    let mut again = placement();
    again.push(placement().remove(1));
    let bad = election::isr_only(&replay(again));
    assert!(bad.mentions("is not the bootstrap placement"), "{bad}");

    // (c) SetPartitionLeader after cluster bootstrap.
    let bad = election::isr_only(&replay(
        [
            placement(),
            vec![MetaCommand::SetPartitionLeader {
                partition: 0,
                epoch: 5,
                leader: 2,
                isr: vec![2],
            }],
        ]
        .concat(),
    ));
    assert!(
        bad.mentions("SetPartitionLeader partition 0 epoch 5 after cluster bootstrap"),
        "{bad}"
    );
}

// ---------------------------------------------------------------------------------------
// objstore
// ---------------------------------------------------------------------------------------

async fn put_manifest(
    store: &Arc<dyn ObjectStore>,
    generation: u64,
    cut: u64,
    key: &str,
    sha: [u8; 32],
) {
    let m = Manifest {
        format: manifest::FORMAT,
        cluster_id: "conf-0".into(),
        node_id: 1,
        incarnation: [0; 16],
        generation,
        prev_manifest_sha256: [0; 32],
        cut_at_ms: 0,
        cut: vec![PartitionCut {
            p: 0,
            cut_offset: cut,
            chain_start: 0,
            last_chunk_key: Some(key.to_owned()),
            last_chunk_sha256: Some(sha),
            snapshot_key: None,
            snapshot_sha256: None,
            snapshot_offset: None,
            verify_offset: 0,
            verify_state_hash: [0; 32],
        }],
        meta_snapshot_key: None,
        meta_snapshot_sha256: None,
        ostro_version: "test".into(),
    };
    store
        .put(manifest::CURRENT_KEY, manifest::encode(&m).into())
        .await
        .expect("put manifest");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn objstore_reports_a_chunk_beyond_hw() {
    let p = PartitionId(0);
    let dir = tempfile::tempdir().expect("dir");
    write_log(
        dir.path(),
        p,
        vec![
            (1, vec![la(1, 1)]),
            (1, vec![start_run(&RunId::new_v7(p))]),
            (1, vec![start_run(&RunId::new_v7(p))]),
        ],
    )
    .await;
    let scan = disk::scan_partition(&view(1, &dir), p).expect("scan");
    assert_eq!(scan.records.len(), 3);
    let bucket = tempfile::tempdir().expect("bucket");
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFs::new(bucket.path()).expect("localfs"));
    let mut prev = [0u8; 32];
    let mut heads = Vec::new();
    for r in &scan.records {
        let (h, bytes) = encode_chunk(0, r.epoch.0, prev, &[&r.envelope[..]]).expect("chunk");
        let key = chunk_key(0, h.epoch, h.base, h.end);
        prev = chunk_sha(&bytes);
        store.put(&key, bytes.into()).await.expect("put chunk");
        heads.push((key, prev, h.end));
    }
    // HW = end of the second record; the head names the second chunk.
    let hw = Offset(heads[1].2);
    put_manifest(&store, 1, heads[1].2, &heads[1].0, heads[1].1).await;
    let ok = objstore::chain_equals_committed(bucket.path(), 0, &scan.records, hw).await;
    assert!(ok.is_ok(), "{ok}");
    // The third chunk exists (unreachable) above HW.
    let bad = objstore::nothing_above(bucket.path(), 0, hw).await;
    assert!(
        bad.mentions(&format!("{} (epoch 1) ends at 3 above HW 2", heads[2].0)),
        "{bad}"
    );
    assert!(
        objstore::nothing_above(bucket.path(), 0, Offset(3))
            .await
            .is_ok()
    );

    // The head moves to the chunk beyond HW: the reachable chain exceeds the committed log.
    put_manifest(&store, 2, heads[2].2, &heads[2].0, heads[2].1).await;
    let bad = objstore::chain_equals_committed(bucket.path(), 0, &scan.records, hw).await;
    assert!(bad.mentions("ends at 3 beyond HW 2"), "{bad}");
    assert!(
        bad.mentions("holds offset 2, which is not in the committed log below HW 2"),
        "{bad}"
    );
    // A committed log that differs from the chain.
    let mut other = scan.records.clone();
    let mut env = other[0].envelope.to_vec();
    let last = env.len() - 1;
    env[last] ^= 1;
    other[0].envelope = Bytes::from(env);
    let bad = objstore::chain_equals_committed(bucket.path(), 0, &other, Offset(3)).await;
    assert!(bad.mentions("envelope at offset 0 differs"), "{bad}");
}

// ---------------------------------------------------------------------------------------
// status
// ---------------------------------------------------------------------------------------

#[test]
fn status_reports_an_isr_member_with_nothing_on_disk() {
    let admin = json!({ "partitions": [ { "partition": 0, "isr": [1, 2] } ] });
    let leos: BTreeMap<u64, Offset> = [(1, Offset(10)), (2, Offset(10))].into();
    assert!(status::honest(&admin, 0, &[2, 1], &leos, Offset(10)).is_ok());

    let zero: BTreeMap<u64, Offset> = [(1, Offset(10)), (2, Offset(0))].into();
    let bad = status::honest(&admin, 0, &[1, 2], &zero, Offset(10));
    assert!(
        bad.mentions("node 2 is reported in the ISR but its measured durable LEO 0 < HW 10"),
        "{bad}"
    );

    let bad = status::honest(&admin, 0, &[1], &leos, Offset(10));
    assert!(
        bad.mentions("reported ISR [1, 2] != published ISR [1]"),
        "{bad}"
    );
    let bad = status::honest(&serde_json::Value::Null, 0, &[1], &leos, Offset(10));
    assert!(bad.mentions("reports no ISR"), "{bad}");
}

// ---------------------------------------------------------------------------------------
// cluster_formed
// ---------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_formed_passes_formed_clusters_and_names_mismatches() {
    let spec = ClusterSpec::single();
    let c = InProcCluster::start(spec.clone()).await.expect("start");
    let ok = cluster_formed::check_within(&c, &spec, Duration::from_secs(20)).await;
    assert!(ok.is_ok(), "{ok}");
    c.stop().await;

    // Two nodes, rf 2: since 12-14 they form one cluster, and the oracle passes...
    let spec = ClusterSpec {
        nodes: 2,
        rf: 2,
        min_insync: 1,
        partitions: 2,
        ..ClusterSpec::default()
    };
    let c = InProcCluster::start(spec.clone()).await.expect("start");
    let ok = cluster_formed::check_within(&c, &spec, Duration::from_secs(30)).await;
    assert!(ok.is_ok(), "{ok}");
    // ...and fails, naming the culprit, against a spec the cluster does not meet.
    let wrong = ClusterSpec {
        rf: 3,
        ..spec.clone()
    };
    let bad = cluster_formed::check_within(&c, &wrong, Duration::from_secs(1)).await;
    assert!(bad.mentions("expected rf 3"), "{bad}");
    assert_eq!(
        bad.clone().into_result().expect_err("red").oracle,
        "cluster_formed"
    );
    c.stop().await;
}

// ---------------------------------------------------------------------------------------
// disk: trimmed-log rule (user-approved 2026-11-23)
// ---------------------------------------------------------------------------------------

/// A verified scan of `node` holding exactly the given `(base, commands)` records.
fn scan_of(node: u64, p: PartitionId, records: Vec<(u64, Vec<Bytes>)>) -> disk::LogScan {
    disk::LogScan {
        node,
        partition: p,
        records: records
            .into_iter()
            .map(|(base, commands)| disk::ScannedRecord {
                base: Offset(base),
                end: Offset(base + commands.len() as u64),
                epoch: Epoch(1),
                envelope: Bytes::new(),
                commands,
            })
            .collect(),
        stopped: None,
    }
}

#[test]
fn disk_trimmed_copy_may_evidence_a_run_by_its_state_store() {
    let p = PartitionId(1);
    let (r1, r2) = (RunId::new_v7(p), RunId::new_v7(p));
    let acks = [ack(&r1), ack(&r2)];
    // Node 1: untrimmed, holds both. Node 2: trimmed at offset 2 (r1 lived below it).
    let n1 = scan_of(
        1,
        p,
        vec![
            (0, vec![la(1, 1), start_run(&r1)]),
            (2, vec![start_run(&r2)]),
        ],
    );
    let n2 = scan_of(2, p, vec![(2, vec![start_run(&r2)])]);
    assert!(n2.is_trimmed() && !n1.is_trimmed());

    // r1 in node 2's state store: both runs on 2 nodes -> pass.
    let ok = disk::acked_on_scans(&[n1.clone(), n2.clone()], &acks, 2, |n, _, r| {
        n == 2 && *r == r1
    });
    assert!(ok.is_ok(), "{ok}");

    // r1 missing from node 2's state store too -> FAIL naming r1 only.
    let bad = disk::acked_on_scans(&[n1.clone(), n2.clone()], &acks, 2, |_, _, _| false);
    assert_eq!(bad.violations.len(), 1, "{bad}");
    assert!(
        bad.mentions(&r1.to_string()) && !bad.mentions(&r2.to_string()),
        "{bad}"
    );
    assert_eq!(bad.into_result().expect_err("failure").oracle, "disk");
}

#[test]
fn disk_untrimmed_copy_never_consults_the_state_store() {
    let p = PartitionId(1);
    let (r1, r2) = (RunId::new_v7(p), RunId::new_v7(p));
    let acks = [ack(&r1), ack(&r2)];
    // Node 2 is untrimmed (starts at 0) but is missing r2: a state-store hit must not count.
    let n1 = scan_of(
        1,
        p,
        vec![(0, vec![la(1, 1), start_run(&r1), start_run(&r2)])],
    );
    let n2 = scan_of(2, p, vec![(0, vec![la(1, 1), start_run(&r1)])]);
    let bad = disk::acked_on_scans(&[n1, n2], &acks, 2, |_, _, _| true);
    assert_eq!(bad.violations.len(), 1, "{bad}");
    assert!(
        bad.mentions(&r2.to_string()) && bad.mentions("[1]"),
        "{bad}"
    );
}
