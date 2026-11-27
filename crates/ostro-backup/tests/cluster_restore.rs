//! Whole-cluster restore of a multi-member bucket (plan 14-30, D-P14-45, BACK-05).
//!
//! The bucket is synthetic but written by the real cluster-mode uploader: one 3-partition
//! node with cross-partition signal traffic, uploaded by a fake leader set that leads
//! different partitions in different rounds (as three single-partition leaders would), so
//! the partition heads end at deliberately skewed points (p2 early, p0 later, p1 latest).
//! Metadata snapshots and `meta/head` are written by the test (the metadata uploader is
//! covered by 14-29's `meta_head` / `cluster_meta_head`).
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use common::{
    Restored, assert_consistent, keys, local_store, node_cfg, open, run_on, shutdown, signals_cmd,
    start_and_grant, submit_ok, up_cfg,
};
use ostro_backup::chunk;
use ostro_backup::cluster::{self, Head};
use ostro_backup::meta_head::{self, MetaHead};
use ostro_backup::restore::{self, At, RestoreError, RestoreOptions, RestoreReport, RestoreSource};
use ostro_backup::uploader::{Uploader, UploaderClusterDeps, meta_key};
use ostro_meta::ext::{
    MEMBER_ACTIVE, MEMBER_REMOVED, ReplicationPolicy, get_member_state, get_replication_policy,
    put_replication_policy,
};
use ostro_meta::{
    MetaCommand, MetaHandle, MetaKey, MetaRecord, MetaResult, MetaState, ModuleRecord,
    PartitionAssignment, WorkflowVersion,
};
use ostro_objstore::ObjectStore;
use ostro_partition::roles::PartitionRoles;
use ostro_partition::{Node, StartMode, SystemClock};
use ostro_types::{AttemptToken, Epoch, PartitionId, RunId};
use tokio::sync::watch;

const P: u16 = 3;
const SEED: u64 = 7;
const CLUSTER_ID: &str = "c-restore-test";
const MODULE: &[u8] = b"\0asm-cluster-restore-test-module";

type Led = Arc<Vec<(PartitionId, Epoch)>>;

struct FakeRoles(watch::Sender<Led>);

impl PartitionRoles for FakeRoles {
    fn led(&self) -> watch::Receiver<Led> {
        self.0.subscribe()
    }
}

fn set_led(r: &FakeRoles, led: &[u16]) {
    r.0.send_replace(Arc::new(
        led.iter().map(|p| (PartitionId(*p), Epoch(1))).collect(),
    ));
}

/// One all-to-all round of cross-partition signals, then wait until every outbox drained.
async fn signal_round(node: &Node, runs: &[(RunId, AttemptToken)], round: usize) {
    let mut n = 0u64;
    for (i, (r, t)) in runs.iter().enumerate() {
        let targets: Vec<(RunId, String)> = runs
            .iter()
            .enumerate()
            .filter(|(j, (x, _))| *j != i && x.partition() != r.partition())
            .map(|(_, (x, _))| {
                n += 1;
                (*x, format!("r{round}-{n}"))
            })
            .collect();
        submit_ok(node, r.partition(), vec![signals_cmd(r, *t, &targets)]).await;
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let drained = (0..P).all(|p| {
            let m = node
                .handle(PartitionId(p))
                .expect("ready")
                .store()
                .read_meta()
                .expect("meta");
            m.cursor.outbox_next_seq.iter().all(|(q, next)| {
                m.cursor.outbox_truncated.get(q).copied().unwrap_or(0) + 1 == *next
            })
        });
        if drained {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "outboxes never drained"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
}

fn sha(b: &[u8]) -> [u8; 32] {
    chunk::chunk_sha(b)
}

fn hexs(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn ok(state: &mut MetaState, index: u64, cmd: MetaCommand) {
    let what = format!("{cmd:?}");
    let r = state.apply(index, cmd);
    assert!(matches!(r, MetaResult::Ok { .. }), "{what}: {r:?}");
}

/// A 3-member metadata state. `with_workflow`: registers `wf` v1 (the open runs' pin) with
/// its module. `epoch`: partition assignment epoch (0 = the InitCluster default).
fn meta_state(index: u64, with_workflow: bool, epoch: u32) -> MetaState {
    let mut s = MetaState::default();
    ok(
        &mut s,
        index,
        MetaCommand::InitCluster {
            cluster_id: CLUSTER_ID.into(),
            node_id: 1,
            partitions: P,
            created_at_ms: 1,
            token_key: [9; 32],
        },
    );
    for id in [2u64, 3] {
        ok(
            &mut s,
            index,
            MetaCommand::Put {
                key: MetaKey::Member(id),
                expected_version: None,
                record: MetaRecord::Member(ostro_meta::Member {
                    node_id: id,
                    addr: format!("127.0.0.1:{}", 7000 + id),
                    joined_at_ms: 2,
                    cluster_addr: None,
                    repl_addr: None,
                    ext: Vec::new(),
                }),
            },
        );
    }
    let mut ci = s.cluster().expect("cluster").clone();
    ci.value.ext = put_replication_policy(
        &ci.value.ext,
        &ReplicationPolicy {
            rf: 3,
            min_insync: 2,
            max_voters: 3,
        },
    )
    .expect("ext");
    ok(
        &mut s,
        index,
        MetaCommand::Put {
            key: MetaKey::Cluster,
            expected_version: Some(ci.version),
            record: MetaRecord::Cluster(ci.value),
        },
    );
    if epoch > 0 {
        for p in 0..P {
            ok(
                &mut s,
                index,
                MetaCommand::Put {
                    key: MetaKey::Partition(p),
                    expected_version: Some(0),
                    record: MetaRecord::Partition(PartitionAssignment {
                        partition: p,
                        leader: 1 + u64::from(p),
                        epoch,
                        isr: vec![1, 2, 3],
                        replicas: vec![1, 2, 3],
                        isr_version: 3,
                        min_insync: 2,
                        ext: Vec::new(),
                    }),
                },
            );
        }
    }
    if with_workflow {
        let m = sha(MODULE);
        ok(
            &mut s,
            index,
            MetaCommand::Put {
                key: MetaKey::Module(m),
                expected_version: None,
                record: MetaRecord::Module(ModuleRecord {
                    sha256: m,
                    size: MODULE.len() as u64,
                    language: None,
                    workflows: vec![("wf".into(), 1)],
                    uploaded_at_ms: 3,
                    ext: Vec::new(),
                }),
            },
        );
        ok(
            &mut s,
            index,
            MetaCommand::Put {
                key: MetaKey::Workflow("wf".into(), 1),
                expected_version: None,
                record: MetaRecord::Workflow(WorkflowVersion {
                    name: "wf".into(),
                    version: 1,
                    module_sha256: m,
                    created_at_ms: 3,
                    ..WorkflowVersion::default()
                }),
            },
        );
    }
    s
}

async fn put_meta(store: &Arc<dyn ObjectStore>, index: u64, s: &MetaState) -> MetaHead {
    let bytes = ostro_meta::encode_snapshot(s);
    store
        .put(&meta_key(index), bytes.clone().into())
        .await
        .expect("put meta");
    MetaHead {
        term: 4,
        index,
        key: meta_key(index),
        sha256: sha(&bytes),
        at_ms: 1_000 + index,
        members: 3,
        partitions: P,
    }
}

/// Which metadata the bucket gets.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Meta {
    /// `meta/head` (index 20) covers the open runs.
    CoveringHead,
    /// `meta/head` (index 20, epoch 1) lacks the workflow; index 10 (epoch 0) covers it.
    OlderCovers,
    /// No metadata at all (a pre-1.0 cluster bucket).
    None,
}

struct Bucket {
    _remote: tempfile::TempDir,
    store: Arc<dyn ObjectStore>,
}

/// Run the workload and upload a skewed cluster bucket (module docs). With `p1_snapshot`,
/// partition 1 takes a snapshot right before the last round (uploaded with it).
async fn build_bucket(work: &Path, meta: Meta, p1_snapshot: bool) -> Bucket {
    let remote = tempfile::tempdir().expect("remote");
    let store = local_store(remote.path(), true);
    let node = open(node_cfg(work, P)).await;
    let (tx, _) = watch::channel(Arc::new(Vec::new()));
    let roles = Arc::new(FakeRoles(tx));
    let (_mtx, members) = watch::channel(3usize);
    let up = Arc::new(
        Uploader::new(Arc::clone(&node), None, Arc::new(SystemClock), up_cfg()).with_cluster(
            UploaderClusterDeps {
                roles: Arc::clone(&roles) as Arc<dyn PartitionRoles>,
                members,
            },
        ),
    );
    let mut runs = Vec::new();
    for i in 0..6u16 {
        let r = run_on(i % P);
        let t = start_and_grant(&node, &r).await;
        runs.push((r, t));
    }
    // A: everything, with a snapshot per partition; every partition led.
    signal_round(&node, &runs, 0).await;
    for p in 0..P {
        node.snapshot_now(PartitionId(p)).await.expect("snapshot");
    }
    set_led(&roles, &[0, 1, 2]);
    up.round(&store).await.expect("round A");
    // B: only p0's leader uploads.
    signal_round(&node, &runs, 1).await;
    set_led(&roles, &[0]);
    up.round(&store).await.expect("round B");
    // C: only p1's leader uploads.
    signal_round(&node, &runs, 2).await;
    if p1_snapshot {
        node.snapshot_now(PartitionId(1)).await.expect("snapshot");
    }
    set_led(&roles, &[1]);
    up.round(&store).await.expect("round C");
    drop(up);
    shutdown(node).await;

    store
        .put(
            &format!("blobs/{}", hexs(&sha(MODULE))),
            MODULE.to_vec().into(),
        )
        .await
        .expect("module blob");
    match meta {
        Meta::CoveringHead => {
            let h = put_meta(&store, 20, &meta_state(20, true, 1)).await;
            store
                .put(meta_head::META_HEAD_KEY, meta_head::encode(&h).into())
                .await
                .expect("meta/head");
        }
        Meta::OlderCovers => {
            put_meta(&store, 10, &meta_state(10, true, 0)).await;
            let h = put_meta(&store, 20, &meta_state(20, false, 1)).await;
            store
                .put(meta_head::META_HEAD_KEY, meta_head::encode(&h).into())
                .await
                .expect("meta/head");
        }
        Meta::None => {}
    }
    Bucket {
        _remote: remote,
        store,
    }
}

async fn heads(store: &Arc<dyn ObjectStore>) -> BTreeMap<u16, Head> {
    let mut out = BTreeMap::new();
    for p in 0..P {
        let (h, _) = cluster::read_head(store, p)
            .await
            .expect("read")
            .expect("head");
        out.insert(p, h);
    }
    out
}

/// The highest envelope epoch of `p`'s chain.
async fn chain_max_epoch(store: &Arc<dyn ObjectStore>, h: &Head) -> u32 {
    let Some(k) = &h.last_chunk_key else {
        return 0;
    };
    chunk::walk_back(store, k, h.last_chunk_sha256.expect("sha"), h.chain_start)
        .await
        .expect("walk")
        .iter()
        .flat_map(|c| c.chunk.envelopes.iter())
        .map(|e| {
            ostro_types::envelope::peek_header(e)
                .expect("header")
                .epoch
                .0
        })
        .max()
        .unwrap_or(0)
}

async fn restore_to(
    store: &Arc<dyn ObjectStore>,
    dir: &Path,
) -> Result<RestoreReport, RestoreError> {
    let mut opts = RestoreOptions::new(dir);
    opts.node_id = Some(SEED);
    restore::restore(RestoreSource::Store(Arc::clone(store)), opts).await
}

/// The restored partitions' cursors and digests (Inspect open of the data dir).
async fn restored(dir: &Path) -> Vec<Restored> {
    let mut cfg = node_cfg(dir, P);
    cfg.mode = StartMode::Inspect;
    cfg.backup_enabled = false;
    cfg.node_id = SEED;
    let node = open(cfg).await;
    let out = (0..P)
        .map(|p| {
            let h = node.handle(PartitionId(p)).expect("ready");
            let m = h.store().read_meta().expect("meta");
            Restored {
                p,
                next: m.cursor.outbox_next_seq,
                truncated: m.cursor.outbox_truncated,
                seen: m.inbound_seen,
                digest: h.store().digest().expect("digest"),
                applied: m.applied.0,
            }
        })
        .collect();
    shutdown(node).await;
    out
}

async fn restored_meta(dir: &Path) -> MetaState {
    let h = MetaHandle::start(&dir.join("meta"), SEED)
        .await
        .expect("meta start");
    let s = h.view().read(MetaState::clone);
    h.stop().await;
    s
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn skewed_bucket_restores_to_a_consistent_lowered_cut_as_a_seed() {
    let root = tempfile::tempdir().expect("root");
    let b = build_bucket(&root.path().join("work"), Meta::CoveringHead, false).await;
    let hs = heads(&b.store).await;
    assert!(
        hs[&2].end < hs[&0].end && hs[&0].end < hs[&1].end || hs[&2].end < hs[&1].end,
        "skewed heads: {:?}",
        hs.iter().map(|(p, h)| (p, h.end)).collect::<Vec<_>>()
    );
    let dir = root.path().join("seed");
    let rep = restore_to(&b.store, &dir).await.expect("cluster restore");
    let c = rep.cluster.clone().expect("cluster report");
    println!(
        "cluster restore: {}",
        serde_json::to_string(&c).expect("json")
    );
    // Cuts: within [floor, head end], p1 (newest head, saw p2's late messages) lowered.
    for cp in &c.partitions {
        assert_eq!(cp.head_end, hs[&cp.p].end);
        assert!(cp.floor <= cp.cut && cp.cut <= cp.head_end, "{cp:?}");
        assert_eq!(cp.dropped, cp.head_end - cp.cut);
        assert_eq!(cp.lowered_reason.is_some(), cp.cut < cp.head_end, "{cp:?}");
    }
    let p1 = &c.partitions[1];
    assert!(p1.cut < p1.head_end, "p1 lowered: {p1:?}");
    assert_eq!(c.partitions[2].cut, hs[&2].end, "p2 (oldest head) kept");
    assert_eq!(c.open_runs, 6);
    assert!(c.meta_from_head);
    assert_eq!((c.meta_index, c.meta_term), (20, Some(4)));
    assert_eq!(rep.node_id, SEED);
    assert_eq!(rep.cluster_id, CLUSTER_ID);
    let json = serde_json::to_value(&rep).expect("json");
    assert!(json["cluster"]["partitions"][1]["dropped"].as_u64() > Some(0));

    // Pitfall 1 on the restored data dir, and the report's per-partition offsets.
    let r = restored(&dir).await;
    assert_consistent(&r);
    for x in &r {
        assert_eq!(x.applied, c.partitions[usize::from(x.p)].cut);
        assert_eq!(rep.partitions[usize::from(x.p)].cut_offset, x.applied);
    }
    assert!(!ostro_backup::cluster_restore::scan_dir(&dir).exists());

    // Metadata names only the seed.
    let m = restored_meta(&dir).await;
    let state = |id: u64| {
        get_member_state(&m.member(id).expect("member").value.ext)
            .expect("ext")
            .map(|s| s.state)
    };
    assert_eq!(state(SEED), Some(MEMBER_ACTIVE));
    let seed = &m.member(SEED).expect("seed").value;
    assert!(seed.addr.is_empty() && seed.cluster_addr.is_none() && seed.repl_addr.is_none());
    for id in [1, 2, 3] {
        assert_eq!(state(id), Some(MEMBER_REMOVED), "member {id}");
    }
    let mut removed = c.removed_members.clone();
    removed.sort_unstable();
    assert_eq!(removed, vec![1, 2, 3]);
    for p in 0..P {
        let a = &m.partition(p).expect("assignment").value;
        let want_epoch = 1u32
            .max(hs[&p].epoch)
            .max(chain_max_epoch(&b.store, &hs[&p]).await);
        assert_eq!(a.replicas, vec![SEED], "p{p}");
        assert_eq!(a.isr, vec![SEED], "p{p}");
        assert_eq!((a.leader, a.min_insync, a.isr_version), (0, 1, 4), "p{p}");
        assert!(
            a.epoch >= want_epoch,
            "p{p}: epoch {} < {want_epoch}",
            a.epoch
        );
        assert_eq!(a.epoch, c.partitions[usize::from(p)].epoch);
    }
    let ci = &m.cluster().expect("cluster").value;
    assert_eq!(ci.cluster_id, CLUSTER_ID);
    assert_eq!(
        get_replication_policy(&ci.ext).expect("ext").map(|r| r.rf),
        Some(3),
        "ReplicationPolicy kept"
    );
    assert!(m.workflow("wf", 1).is_some());
    let toml = std::fs::read_to_string(dir.join("node.toml")).expect("node.toml");
    assert!(
        toml.contains(&format!("cluster_id = \"{CLUSTER_ID}\"")),
        "{toml}"
    );
    assert!(toml.contains("restored_from = "), "{toml}");
    assert!(dir.join("blobs").exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn at_is_rejected_for_cluster_buckets() {
    let root = tempfile::tempdir().expect("root");
    let b = build_bucket(&root.path().join("work"), Meta::CoveringHead, false).await;
    for at in [At::Generation(1), At::TimeMs(5_000)] {
        let dir = root.path().join("seed");
        let mut opts = RestoreOptions::new(&dir);
        opts.at = Some(at);
        let err = restore::restore(RestoreSource::Store(Arc::clone(&b.store)), opts)
            .await
            .expect_err("--at refused");
        assert!(
            err.to_string()
                .contains("--at is not supported for cluster buckets in 1.0 (no common time base)"),
            "{err}"
        );
        assert!(!dir.exists(), "nothing written");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cut_below_a_floor_is_no_consistent_cut() {
    let root = tempfile::tempdir().expect("root");
    let b = build_bucket(&root.path().join("work"), Meta::CoveringHead, true).await;
    // Re-chain p1 at its newest snapshot (as a leader does when its log start passed the
    // remote tail, 12-15 W4): the chain cannot reach below it any more.
    let snaps = keys(&b.store, &ostro_backup::uploader::snap_prefix(1)).await;
    let (_, off, s) = snaps
        .iter()
        .filter_map(|k| ostro_backup::uploader::parse_snap_key(k))
        .max_by_key(|(_, o, _)| *o)
        .expect("p1 snapshot");
    let key = ostro_backup::uploader::snap_key(1, off, &s);
    let (cur, etag) = cluster::read_head(&b.store, 1)
        .await
        .expect("read")
        .expect("head");
    let rechained = Head {
        last_chunk_key: None,
        last_chunk_sha256: None,
        chain_start: off,
        end: off,
        snapshot: Some(cluster::HeadSnapshot {
            key,
            offset: off,
            sha256: s,
            state_hash: None,
        }),
        ..cur
    };
    let put = cluster::put_head(&b.store, 1, &rechained, etag.as_deref())
        .await
        .expect("put head");
    assert!(matches!(put, cluster::HeadPut::Written(_)), "{put:?}");
    let dir = root.path().join("seed");
    let err = restore_to(&b.store, &dir).await.expect_err("no cut");
    match &err {
        RestoreError::NoConsistentCut { partitions } => {
            assert!(partitions.contains(&1), "{partitions:?}");
        }
        other => panic!("expected NoConsistentCut, got {other:?}"),
    }
    assert!(err.to_string().contains("no consistent cut"), "{err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_older_covering_meta_snapshot_is_chosen_and_the_epoch_follows_the_log() {
    let root = tempfile::tempdir().expect("root");
    let b = build_bucket(&root.path().join("work"), Meta::OlderCovers, false).await;
    let hs = heads(&b.store).await;
    let dir = root.path().join("seed");
    let rep = restore_to(&b.store, &dir).await.expect("cluster restore");
    let c = rep.cluster.clone().expect("cluster report");
    assert!(!c.meta_from_head, "meta/head lacks the workflow");
    assert_eq!((c.meta_index, c.meta_term), (10, None));
    assert_eq!(rep.meta_index, Some(10));
    let m = restored_meta(&dir).await;
    assert!(m.workflow("wf", 1).is_some());
    for p in 0..P {
        // The chosen snapshot's assignments are at epoch 0; the log and the heads are later.
        let log = chain_max_epoch(&b.store, &hs[&p]).await;
        let want = hs[&p].epoch.max(log);
        assert!(
            want > 0,
            "p{p}: the log carries elections after the old snapshot"
        );
        let a = &m.partition(p).expect("assignment").value;
        assert_eq!(
            a.epoch, want,
            "p{p}: rewritten epoch = the highest restored epoch"
        );
        // The controller's first election uses epoch + 1: above every restored epoch.
        assert!(a.epoch + 1 > log && a.epoch + 1 > hs[&p].epoch);
        assert_eq!((a.leader, a.isr.clone()), (0, vec![SEED]));
    }
    assert_consistent(&restored(&dir).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_covering_meta_snapshot_is_refused() {
    let root = tempfile::tempdir().expect("root");
    let b = build_bucket(&root.path().join("work"), Meta::None, false).await;
    let dir = root.path().join("seed");
    let err = restore_to(&b.store, &dir).await.expect_err("no meta");
    assert!(
        matches!(err, RestoreError::ClusterBucketUnsupported(_)),
        "{err:?}"
    );
    assert!(
        err.to_string().contains(
            "no metadata snapshot in the bucket; the metadata leader uploads one in 1.0+"
        ),
        "{err}"
    );
    // A metadata snapshot without the open runs' workflow is not enough either.
    let h = put_meta(&b.store, 30, &meta_state(30, false, 1)).await;
    b.store
        .put(meta_head::META_HEAD_KEY, meta_head::encode(&h).into())
        .await
        .expect("meta/head");
    let dir2 = root.path().join("seed2");
    let err = restore_to(&b.store, &dir2).await.expect_err("not covering");
    assert!(
        err.to_string()
            .contains("no metadata snapshot covers the module pins of 6 non-terminal runs"),
        "{err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_single_member_manifest_bucket_takes_the_phase8_path() {
    let root = tempfile::tempdir().expect("root");
    let remote = tempfile::tempdir().expect("remote");
    let store = local_store(remote.path(), true);
    let node = open(node_cfg(&root.path().join("work"), 2)).await;
    common::start_runs(&node, 0, 4).await;
    common::start_runs(&node, 1, 4).await;
    let up = common::uploader(&node, up_cfg());
    up.round(&store).await.expect("round");
    drop(up);
    shutdown(node).await;
    assert!(common::current_manifest(&store).await.is_some());
    let dir = root.path().join("restored");
    let rep = restore::restore(
        RestoreSource::Store(Arc::clone(&store)),
        RestoreOptions::new(&dir),
    )
    .await
    .expect("phase 8 restore");
    assert!(rep.cluster.is_none());
    let json = serde_json::to_value(&rep).expect("json");
    assert!(json.get("cluster").is_none(), "no cluster field: {json}");
    assert!(rep.generation >= 1);
}

/// Mutant bite: with `repl::mutant::restore_skip_fixpoint` every partition is restored at its
/// raw head end and the final Pitfall 1 check fails on the skewed bucket.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restore_skip_fixpoint_mutant_bites() {
    let root = tempfile::tempdir().expect("root");
    let b = build_bucket(&root.path().join("work"), Meta::CoveringHead, false).await;
    let scenario = fail::FailScenario::setup();
    fail::cfg("repl::mutant::restore_skip_fixpoint", "return").expect("cfg");
    let err = restore_to(&b.store, &root.path().join("mutant"))
        .await
        .expect_err("raw heads are inconsistent");
    scenario.teardown();
    assert!(matches!(err, RestoreError::Inconsistent(_)), "{err:?}");
    // Without the mutant the same bucket restores.
    restore_to(&b.store, &root.path().join("clean"))
        .await
        .expect("clean restore");
}
