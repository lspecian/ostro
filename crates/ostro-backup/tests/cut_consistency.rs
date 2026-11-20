//! Cut consistency (Pitfall 1, plan 08-13): restoring at any manifest generation of a
//! cross-partition signal workload gives consistent `outbox_next_seq` / `outbox_truncated` /
//! `inbound_seen` for every partition pair, including when a snapshot lands between reading
//! the cut and writing the manifest (B3).
//!
//! The workload sends signals between runs on different partitions; the shuffler delivers
//! them concurrently with the uploader's rounds, so cuts land with messages in flight, half
//! delivered and truncated. Cases: 24 (`OSTRO_CUT_CASES` overrides).
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{
    assert_consistent, current_manifest, keys, local_store, node_cfg, open, run_on, shutdown,
    signals_cmd, simulate_restore, start_and_grant, submit_ok, up_cfg, uploader,
};
use ostro_types::{AttemptToken, PartitionId, RunId};
use proptest::prelude::*;

const P: u16 = 3;
/// Runs in the workload: two per partition.
const RUNS: usize = 6;
const MAX_ROUNDS: usize = 3;

#[derive(Clone, Debug)]
enum Op {
    /// Run `src` signals runs `targets`.
    Send { src: usize, targets: Vec<usize> },
    /// Take a snapshot of partition `p`.
    Snapshot(u16),
    /// Let the shuffler run for a few ms.
    Pause(u64),
    /// Upload round + simulated restore at the new manifest.
    Round,
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        6 => (0..RUNS, prop::collection::vec(0..RUNS, 1..4))
            .prop_map(|(src, targets)| Op::Send { src, targets }),
        1 => (0..P).prop_map(Op::Snapshot),
        2 => (0u64..30).prop_map(Op::Pause),
        2 => Just(Op::Round),
    ]
}

fn cases() -> u32 {
    std::env::var("OSTRO_CUT_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(24)
}

async fn setup(node: &ostro_partition::Node) -> Vec<(RunId, AttemptToken)> {
    let mut runs = Vec::new();
    for i in 0..RUNS {
        let r = run_on((i % P as usize) as u16);
        let t = start_and_grant(node, &r).await;
        runs.push((r, t));
    }
    runs
}

async fn send(
    node: &ostro_partition::Node,
    runs: &[(RunId, AttemptToken)],
    src: usize,
    targets: &[usize],
    n: &mut u64,
) {
    let (r, t) = runs[src];
    let ts: Vec<(RunId, String)> = targets
        .iter()
        .map(|&i| {
            *n += 1;
            (runs[i].0, format!("s{n}"))
        })
        .collect();
    submit_ok(node, r.partition(), vec![signals_cmd(&r, t, &ts)]).await;
}

async fn run_case(ops: Vec<Op>) {
    let dir = tempfile::tempdir().expect("tempdir");
    let remote = tempfile::tempdir().expect("remote");
    let store = local_store(remote.path(), true);
    let node = open(node_cfg(dir.path(), P)).await;
    let up = uploader(&node, up_cfg());
    let runs = setup(&node).await;
    let mut n = 0u64;
    let mut rounds = 0;
    for op in ops {
        match op {
            Op::Send { src, targets } => send(&node, &runs, src, &targets, &mut n).await,
            Op::Snapshot(p) => {
                node.snapshot_now(PartitionId(p)).await.expect("snapshot");
            }
            Op::Pause(ms) => tokio::time::sleep(Duration::from_millis(ms)).await,
            Op::Round if rounds < MAX_ROUNDS => {
                rounds += 1;
                if let Some(m) = up.round(&store).await.expect("round").manifest {
                    assert_consistent(&simulate_restore(&store, &m, P).await);
                }
            }
            Op::Round => {}
        }
    }
    // Final round on whatever is in flight.
    up.round(&store).await.expect("final round");
    let m = current_manifest(&store).await.expect("a manifest");
    assert_consistent(&simulate_restore(&store, &m, P).await);
    drop(up);
    shutdown(node).await;
}

proptest! {
    #![proptest_config(ProptestConfig { cases: cases(), failure_persistence: None, ..ProptestConfig::default() })]
    #[test]
    fn restore_at_any_manifest_is_cut_consistent(ops in prop::collection::vec(op(), 1..16)) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .expect("runtime");
        rt.block_on(run_case(ops));
    }
}

/// B3: a snapshot taken after the cut was read but before the manifest is written is uploaded
/// but not referenced; validation passes, the simulated restore is consistent, and the next
/// round references it once its cut covers it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_between_cut_and_manifest_is_held_back() {
    let dir = tempfile::tempdir().expect("tempdir");
    let remote = tempfile::tempdir().expect("remote");
    let store = local_store(remote.path(), true);
    let node = open(node_cfg(dir.path(), 2)).await;
    let up = uploader(&node, up_cfg());
    let runs = setup2(&node).await;
    let mut n = 0;
    for _ in 0..5 {
        send(&node, &runs, 0, &[1, 2, 3], &mut n).await;
        send(&node, &runs, 1, &[0, 2], &mut n).await;
    }
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    up.set_after_cut_hook(Some(tx));
    let task = {
        let up = Arc::clone(&up);
        let store = Arc::clone(&store);
        tokio::spawn(async move { up.round(&store).await })
    };
    let resume = rx.recv().await.expect("round reached the hook");
    // While the round is paused after its cut: more traffic and a snapshot of partition 0.
    for _ in 0..5 {
        send(&node, &runs, 0, &[1, 3], &mut n).await;
    }
    let snap = node
        .snapshot_now(PartitionId(0))
        .await
        .expect("snapshot")
        .expect("new snapshot");
    resume.send(()).expect("resume");
    up.set_after_cut_hook(None);
    let r = task.await.expect("join").expect("round");
    let cut0 = r.cut[0].0;
    assert!(
        snap.applied_offset.0 > cut0,
        "the snapshot is beyond the cut"
    );
    let m = r.manifest.expect("manifest");
    m.validate().expect("valid");
    let e0 = m.partition(0).expect("p0");
    assert_ne!(e0.snapshot_offset, Some(snap.applied_offset.0), "held back");
    assert!(e0.snapshot_offset.is_none_or(|s| s <= e0.cut_offset));
    // ... but already uploaded.
    let uploaded: Vec<u64> = keys(&store, "p/0000/snap/")
        .await
        .iter()
        .map(|k| ostro_backup::uploader::parse_snap_key(k).expect("key").1)
        .collect();
    assert!(uploaded.contains(&snap.applied_offset.0));
    assert_consistent(&simulate_restore(&store, &m, 2).await);
    // Next round: the cut covers the snapshot; the manifest references it.
    let r2 = up.round(&store).await.expect("round 2");
    let m2 = r2.manifest.expect("manifest 2");
    let e0 = m2.partition(0).expect("p0");
    assert_eq!(e0.snapshot_offset, Some(snap.applied_offset.0));
    assert!(e0.verify_offset >= snap.applied_offset.0);
    assert_consistent(&simulate_restore(&store, &m2, 2).await);
    drop(up);
    shutdown(node).await;
}

async fn setup2(node: &ostro_partition::Node) -> Vec<(RunId, AttemptToken)> {
    let mut runs = Vec::new();
    for i in 0..4u16 {
        let r = run_on(i % 2);
        let t = start_and_grant(node, &r).await;
        runs.push((r, t));
    }
    runs
}
