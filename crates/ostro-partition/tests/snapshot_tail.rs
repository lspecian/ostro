//! Snapshot + tail == full replay (plan 08-09, STOR-06 under trimming): a generated workload,
//! a forced snapshot at a random offset k, more load, then the state is deleted and rebuilt
//! twice: from the snapshot plus the log tail `[k, HW)`, and by full replay from offset 0
//! (snapshots removed). Digest, rolling hash and applied offset must match each other and
//! the live state. 64 cases (`OSTRO_SNAPSHOT_TAIL_CASES` overrides).
#![allow(clippy::disallowed_methods)]

mod common;

use std::path::Path;

use bytes::Bytes;
use common::{OPEN_T, T, run_on, start_run};
use ostro_log::ReplicatedLog;
use ostro_partition::{Node, NodeConfig, PartitionSlot, StartMode};
use ostro_types::command::ConflictPolicy;
use ostro_types::{Command, Offset, PartitionCount, PartitionId, PayloadRef};
use proptest::prelude::*;
use tokio::time::timeout;

const P0: PartitionId = PartitionId(0);

fn cfg(dir: &Path, mode: StartMode) -> NodeConfig {
    let mut cfg = NodeConfig::new(dir.to_path_buf(), PartitionCount::new(1).expect("non-zero"));
    cfg.mode = mode;
    cfg.snapshots.interval = std::time::Duration::from_secs(3600);
    cfg.snapshots.min_interval = std::time::Duration::from_secs(3600);
    cfg.wal.enable_rotation = true;
    cfg.wal.rotation_size_bytes = 4 * 1024;
    cfg
}

async fn open(dir: &Path, mode: StartMode) -> Node {
    timeout(OPEN_T, Node::open(cfg(dir, mode)))
        .await
        .expect("node open timed out")
        .expect("node open")
}

/// One generated command: a `StartRun` with a varying queue, priority, input size and an
/// idempotency key from a small space (so duplicates and conflicts happen).
#[derive(Clone, Debug)]
struct Gen {
    key: Option<u8>,
    policy: u8,
    queue: u8,
    input_len: usize,
    priority: i32,
}

fn gen_cmd() -> impl Strategy<Value = Gen> {
    (
        prop::option::weighted(0.5, 0u8..8),
        0u8..3,
        0u8..3,
        0usize..512,
        -2i32..3,
    )
        .prop_map(|(key, policy, queue, input_len, priority)| Gen {
            key,
            policy,
            queue,
            input_len,
            priority,
        })
}

fn command(g: &Gen, i: usize) -> Command {
    let mut s = start_run(&run_on(0));
    s.queue = format!("q{}", g.queue);
    s.priority = g.priority;
    s.input = Some(PayloadRef::inline(Bytes::from(vec![i as u8; g.input_len])));
    if let Some(k) = g.key {
        s.idempotency_key = vec![b'k', k];
        s.conflict_policy = if g.policy == 1 {
            ConflictPolicy::Reject
        } else {
            ConflictPolicy::ReturnExisting
        } as i32;
    }
    Command::start_run(s)
}

async fn apply(node: &Node, cmds: &[Gen], base: usize, chunk: usize) {
    let h = node.handle(P0).expect("ready");
    for (ci, part) in cmds.chunks(chunk.max(1)).enumerate() {
        let batch: Vec<Command> = part
            .iter()
            .enumerate()
            .map(|(j, g)| command(g, base + ci * chunk + j))
            .collect();
        timeout(T, h.submit(batch))
            .await
            .expect("submit timed out")
            .expect("submit");
    }
}

/// `(applied, state_hash, digest)` of a Ready partition.
fn state(node: &Node) -> (Offset, [u8; 32], [u8; 32]) {
    let slots = node.slots();
    let h = match &slots[0] {
        PartitionSlot::Ready(h) => h,
        PartitionSlot::Failed { reason, .. } => panic!("partition failed: {reason}"),
    };
    let m = h.store().read_meta().expect("meta");
    (m.applied, m.state_hash, h.store().digest().expect("digest"))
}

async fn run_case(before: Vec<Gen>, after: Vec<Gen>, chunk: usize) {
    let tmp = ostro_testkit::fs::tempdir();
    let dir = tmp.path();
    let node = open(dir, StartMode::Leader).await;
    apply(&node, &before, 0, chunk).await;
    let snap = node
        .snapshot_now(P0)
        .await
        .expect("snapshot")
        .expect("new snapshot");
    apply(&node, &after, before.len(), chunk).await;
    let hw = *node
        .handle(P0)
        .expect("ready")
        .log()
        .high_watermark()
        .borrow();
    timeout(
        T,
        node.handle(P0)
            .expect("ready")
            .machine()
            .wait_applied(Offset(hw.0 - 1)),
    )
    .await
    .expect("apply timed out")
    .expect("applied");
    let live = state(&node);
    node.shutdown().await.expect("shutdown");

    // Snapshot + tail.
    let state_dir = dir.join("state").join("p0000");
    std::fs::remove_dir_all(&state_dir).expect("rm state");
    let node = open(dir, StartMode::Inspect).await;
    assert_eq!(node.snapshot_installed(P0), Some(snap.applied_offset));
    let from_snapshot = state(&node);
    node.shutdown().await.expect("shutdown");

    // Full replay.
    std::fs::remove_dir_all(&state_dir).expect("rm state");
    std::fs::remove_dir_all(dir.join("snapshots")).expect("rm snapshots");
    let node = open(dir, StartMode::Inspect).await;
    assert_eq!(node.snapshot_installed(P0), None);
    let full = state(&node);
    node.shutdown().await.expect("shutdown");

    assert_eq!(
        from_snapshot, full,
        "snapshot at {} + tail != full replay",
        snap.applied_offset
    );
    assert_eq!(full, live, "full replay != live state");
}

fn cases() -> u32 {
    std::env::var("OSTRO_SNAPSHOT_TAIL_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(64)
}

proptest! {
    #![proptest_config(ProptestConfig { cases: cases(), max_shrink_iters: 16, ..ProptestConfig::default() })]

    #[test]
    fn snapshot_plus_tail_equals_full_replay(
        before in prop::collection::vec(gen_cmd(), 0..120),
        after in prop::collection::vec(gen_cmd(), 1..120),
        chunk in 1usize..40,
    ) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");
        rt.block_on(run_case(before, after, chunk));
    }
}
