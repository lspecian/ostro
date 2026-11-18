//! Shared helpers for ostro-partition integration tests (plans 02-11+): open a single-partition
//! stack (state store + RF=1 log + apply machine + handle) until `Node` exists.
#![allow(dead_code, clippy::disallowed_methods, clippy::disallowed_types)]

pub mod manual_hw;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use ostro_blob::BlobStore;
use ostro_log::{LogNode, LogNodeConfig, ReplicatedLog, Rf1Log, Role};
use ostro_partition::{
    ApplyOutcome, Cursor, MachineConfig, MachineHandle, PartitionError, PartitionHandle,
    PartitionStore, PendingAcks, StoreConfig, apply_chunk,
};
use ostro_types::command::{ConflictPolicy, StartRun};
use ostro_types::{Command, Epoch, Offset, PartitionCount, PartitionId, PayloadRef, RunId};
use tokio::time::timeout;

/// Generous timeout for anything that should happen "soon".
pub const T: Duration = Duration::from_secs(10);

/// Partition count used by the single-partition helpers.
pub fn pc() -> PartitionCount {
    PartitionCount::new(1).expect("non-zero")
}

pub const P0: PartitionId = PartitionId(0);

/// One partition: the log node, the raw RF=1 log and the handle (whose log may wrap it).
pub struct TestPartition<L: ReplicatedLog = Rf1Log> {
    pub node: LogNode,
    pub rf1: Arc<Rf1Log>,
    pub handle: PartitionHandle<L>,
}

impl<L: ReplicatedLog> TestPartition<L> {
    /// Graceful machine shutdown (final checkpoint), then WAL shutdown. Everything is dropped
    /// before returning, so the directory can be reopened.
    pub async fn close(self) -> Result<Offset, PartitionError> {
        let TestPartition { node, rf1, handle } = self;
        let r = handle.machine().clone().shutdown().await;
        node.shutdown().await;
        drop(handle);
        drop(rf1);
        drop(node);
        r
    }

    pub fn applied(&self) -> Offset {
        *self.handle.machine().applied().borrow()
    }

    pub fn durable(&self) -> Offset {
        *self.handle.machine().durable_checkpoint().borrow()
    }
}

/// Open the single partition under `dir` (store, LogNode P=1 with floor = applied, machine,
/// catch-up), set epoch = latest + 1 and submit `LeaderAnnounced`.
pub async fn open_single(dir: &Path, cfg: MachineConfig) -> TestPartition {
    open_with(dir, cfg, true, |l| l).await
}

/// Like [`open_single`], wrapping the RF=1 log with `wrap`. Without `announce`, the epoch is
/// set to the log's latest epoch and nothing is appended.
pub async fn open_with<L, F>(dir: &Path, cfg: MachineConfig, announce: bool, wrap: F) -> TestPartition<L>
where
    L: ReplicatedLog,
    F: FnOnce(Arc<Rf1Log>) -> Arc<L>,
{
    let (store, meta) =
        PartitionStore::open(dir, P0, pc(), &StoreConfig::default()).expect("open store");
    let (node, mut logs) = LogNode::open(LogNodeConfig::new(dir.join("wal"), pc(), 1), &[meta.applied])
        .await
        .expect("open log node");
    let rf1 = logs.remove(0).expect("recover partition log");
    let log = wrap(Arc::clone(&rf1));
    let store = Arc::new(store);
    let acks = Arc::new(PendingAcks::new());
    let machine = MachineHandle::spawn(
        Arc::clone(&store),
        Arc::clone(&log),
        Cursor::from_meta(&meta),
        cfg,
        tokio::runtime::Handle::current(),
        Arc::clone(&acks),
    );
    let blobs = Arc::new(BlobStore::open(dir).expect("open blob store"));
    let handle = PartitionHandle::new(P0, log, store, machine, acks, blobs);

    // Catch up to the recovered HW.
    let hw = *handle.log().high_watermark().borrow();
    let mut applied = handle.machine().applied();
    timeout(T, applied.wait_for(|a| *a >= hw))
        .await
        .expect("catch-up timed out")
        .expect("machine gone");

    let latest = rf1.epochs().latest().map_or(0, |e| e.0);
    let epoch = if announce { Epoch(latest + 1) } else { Epoch(latest) };
    handle.set_epoch(epoch);
    rf1.set_role(Role::Leader { epoch });
    if announce {
        let o = timeout(T, handle.submit_one(Command::leader_announced(epoch.0, 1, 0)))
            .await
            .expect("announce timed out")
            .expect("announce");
        assert_eq!(o, ApplyOutcome::Accepted);
    }
    TestPartition { node, rf1, handle }
}

/// Digest of a fresh store rebuilt from the log `[0, HW)`.
pub async fn rebuild_digest<L: ReplicatedLog>(log: &L) -> [u8; 32] {
    let dir = ostro_testkit::fs::tempdir();
    let (store, meta) =
        PartitionStore::open(dir.path(), P0, pc(), &StoreConfig::default()).expect("open");
    let mut cur = Cursor::from_meta(&meta);
    let hw = *log.high_watermark().borrow();
    while cur.next < hw {
        let batches = log.read(cur.next, hw, 1 << 20).await.expect("read");
        assert!(!batches.is_empty(), "short read below HW");
        let mut b = store.batch();
        let mut actions = Vec::new();
        apply_chunk(&mut b, &mut cur, &batches, &mut actions).expect("apply");
        let mu = cur.meta_update();
        b.commit(&mu).expect("commit");
    }
    store.digest().expect("digest")
}

/// A fresh run id on partition 0.
pub fn new_run() -> RunId {
    RunId::new_v7(P0)
}

pub fn start_run(run: &RunId) -> StartRun {
    StartRun {
        run_id: run.as_bytes().to_vec(),
        workflow: "wf".into(),
        workflow_version: 1,
        queue: "q".into(),
        input: Some(PayloadRef::inline(Bytes::from_static(b"in"))),
        idempotency_key: vec![],
        conflict_policy: ConflictPolicy::ReturnExisting as i32,
        concurrency_key: String::new(),
        priority: 0,
        ready_at_ms: 100,
        requested_at_ms: 100,
        parent: None,
        rng_seed: vec![7; 32],
    }
}

pub fn start_cmd(run: &RunId) -> Command {
    Command::start_run(start_run(run))
}

/// Fast-checkpoint-free config (tests opt into cadence explicitly).
pub fn cfg() -> MachineConfig {
    MachineConfig::default()
}
