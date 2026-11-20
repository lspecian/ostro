//! Shared helpers for ostro-partition integration tests (plans 02-11+): open a single-partition
//! stack (state store + RF=1 log + apply machine + handle) until `Node` exists.
#![allow(dead_code, clippy::disallowed_methods, clippy::disallowed_types)]

pub mod flaky_router;
pub mod gen_effects;
pub mod gen_lifecycle;
pub mod gen_waits;
pub mod manual_hw;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use ostro_blob::BlobStore;
use ostro_log::{LogNode, LogNodeConfig, ReplicatedLog, Rf1Log, Role};
use ostro_partition::{
    ApplyOutcome, Cursor, MachineConfig, MachineHandle, PartitionError, PartitionHandle,
    PartitionHealth, PartitionStore, PendingAcks, StoreConfig, apply_chunk,
};
use ostro_types::command::{ConflictPolicy, StartRun};
use ostro_types::{
    AttemptToken, Command, Epoch, Offset, PartitionCount, PartitionId, PayloadRef, RunId,
};
use tokio::time::timeout;

/// Driver view of one run, handed to the `rebuild_digest` generator hooks
/// ([`gen_waits`], [`gen_lifecycle`], [`gen_effects`]). `next_seq` is re-read from live state just before the
/// hook runs (inputs journaled by apply move it).
#[derive(Clone, Debug)]
pub struct GenRun {
    pub partition: u16,
    pub id: RunId,
    /// The driver's last granted token (`None` = never granted by the driver).
    pub token: Option<AttemptToken>,
    pub next_seq: u64,
}

/// Generous timeout for anything that should happen "soon".
pub const T: Duration = Duration::from_secs(10);

/// Timeout for `Node::open` in the harness.
///
/// Nearly all of a fresh open is spent creating the fjall state stores. Creating one issues about
/// 295 fsyncs, one after another on that partition's open thread, because fjall rewrites its meta
/// keyspace for every keyspace it creates. Partitions open in parallel, so a fresh open takes about
/// 295 fsync latencies. Reopening an existing store takes 10-60 ms. Log recovery, catch-up and
/// `LeaderAnnounced` stay well under a second even under load.
///
/// Measured on WSL2 ext4 (debug, 2026-11-19): fresh opens took 1.1-2.6 s at load average 7-13 with
/// no added I/O. At load average 30-58 (dd writers plus parallel test binaries) they took 6-11 s,
/// and strace showed 6.7 s of an 8.4 s state stage spent in fsync (about 23 ms each). One open took
/// 10.3 s during an I/O burst at load 10. ci-local runs alongside other cargo builds pushed opens
/// past 30 s.
///
/// The test runs saw no hang: 678 traced opens under a 300 s limit all finished. So the limit only
/// has to absorb fsync latency. 120 s allows a 400 ms mean fsync, and a real hang still fails.
pub const OPEN_T: Duration = Duration::from_secs(120);

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
pub async fn open_with<L, F>(
    dir: &Path,
    cfg: MachineConfig,
    announce: bool,
    wrap: F,
) -> TestPartition<L>
where
    L: ReplicatedLog,
    F: FnOnce(Arc<Rf1Log>) -> Arc<L>,
{
    let (store, meta) =
        PartitionStore::open(dir, P0, pc(), &StoreConfig::default()).expect("open store");
    let (node, mut logs) = LogNode::open(
        LogNodeConfig::new(dir.join("wal"), pc(), 1),
        &[meta.applied],
    )
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
    let mut health = handle.machine().health();
    timeout(T, health.wait_for(|h| *h != PartitionHealth::Starting))
        .await
        .expect("machine never left Starting")
        .expect("machine gone");
    assert_eq!(*health.borrow(), PartitionHealth::Healthy);

    let latest = rf1.epochs().latest().map_or(0, |e| e.0);
    let epoch = if announce {
        Epoch(latest + 1)
    } else {
        Epoch(latest)
    };
    handle.set_epoch(epoch);
    rf1.set_role(Role::Leader { epoch });
    if announce {
        let o = timeout(
            T,
            handle.submit_one(Command::leader_announced(epoch.0, 1, 0)),
        )
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
        ..Default::default()
    }
}

pub fn start_cmd(run: &RunId) -> Command {
    Command::start_run(start_run(run))
}

/// Fast-checkpoint-free config (tests opt into cadence explicitly).
pub fn cfg() -> MachineConfig {
    MachineConfig::default()
}

/// Open a node with `p` partitions under `dir` in `mode` (default configs otherwise).
pub async fn open_node(
    dir: &Path,
    p: u16,
    mode: ostro_partition::StartMode,
) -> ostro_partition::Node {
    let mut cfg = ostro_partition::NodeConfig::new(
        dir.to_path_buf(),
        PartitionCount::new(p).expect("non-zero"),
    );
    cfg.mode = mode;
    timeout(OPEN_T, ostro_partition::Node::open(cfg))
        .await
        .expect("node open timed out")
        .expect("node open")
}

/// Recursively copy `src` into `dst` (created).
pub fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).expect("mkdir");
    for e in std::fs::read_dir(src).expect("read_dir") {
        let e = e.expect("entry");
        let to = dst.join(e.file_name());
        if e.file_type().expect("file type").is_dir() {
            copy_dir(&e.path(), &to);
        } else {
            std::fs::copy(e.path(), &to).expect("copy");
        }
    }
}

/// A fresh run id on partition `p`.
pub fn run_on(p: u16) -> RunId {
    RunId::new_v7(PartitionId(p))
}

// ---------------------------------------------------------------- multi-partition helpers
// (plan 02-13: shuffler / exactly-once suites)

/// Open a node with `p` partitions under `dir` in Leader mode, letting `f` adjust the config
/// (router wrapper, shuffler tuning, ...).
pub async fn open_node_cfg(
    dir: &Path,
    p: u16,
    f: impl FnOnce(&mut ostro_partition::NodeConfig),
) -> ostro_partition::Node {
    let mut cfg = ostro_partition::NodeConfig::new(
        dir.to_path_buf(),
        PartitionCount::new(p).expect("non-zero"),
    );
    f(&mut cfg);
    timeout(OPEN_T, ostro_partition::Node::open(cfg))
        .await
        .expect("node open timed out")
        .expect("node open")
}

/// Shuffler tuned for tests: short idle poll.
pub fn fast_shuffler(cfg: &mut ostro_partition::NodeConfig) {
    cfg.leader.shuffler.idle_poll = Duration::from_millis(20);
}

pub fn handle_of(node: &ostro_partition::Node, p: PartitionId) -> &PartitionHandle<Rf1Log> {
    node.handle(p).expect("partition ready")
}

/// Start `run` (on its own partition) and grant it an attempt under the current epoch.
pub async fn start_and_grant(
    node: &ostro_partition::Node,
    run: &RunId,
) -> ostro_types::AttemptToken {
    let h = handle_of(node, run.partition());
    let o = timeout(T, h.submit_one(start_cmd(run)))
        .await
        .expect("start timed out")
        .expect("start");
    assert_eq!(o, ApplyOutcome::Accepted, "start {run}");
    grant(node, run).await
}

/// Grant the next attempt of an existing run.
pub async fn grant(node: &ostro_partition::Node, run: &RunId) -> ostro_types::AttemptToken {
    let h = handle_of(node, run.partition());
    timeout(T, h.grant_attempt(*run))
        .await
        .expect("grant timed out")
        .expect("grant")
}

/// `EventsAppended` (no journal events) carrying one `SendMessage` per `kinds` entry.
pub fn send_cmd(
    run: &RunId,
    t: ostro_types::AttemptToken,
    kinds: Vec<ostro_types::command::outbox_msg::Kind>,
) -> Command {
    use ostro_types::command::{
        AttemptTokenMsg, Derived, EventsAppended, OutboxMsg, SendMessage, derived,
    };
    Command::events_appended(EventsAppended {
        run_id: run.as_bytes().to_vec(),
        token: Some(AttemptTokenMsg {
            epoch: t.epoch.0,
            n: t.n,
        }),
        from_seq: 0,
        events: vec![],
        derived: kinds
            .into_iter()
            .map(|k| Derived {
                intent: Some(derived::Intent::SendMessage(SendMessage {
                    msg: Some(OutboxMsg { kind: Some(k) }),
                })),
            })
            .collect(),
    })
}

pub fn signal_kind(target: &RunId, name: &str) -> ostro_types::command::outbox_msg::Kind {
    ostro_types::command::outbox_msg::Kind::Signal(ostro_types::command::Signal {
        target_run: target.as_bytes().to_vec(),
        name: name.into(),
        payload: Some(PayloadRef::inline(Bytes::from_static(b"p"))),
    })
}

pub fn cancel_kind(target: &RunId) -> ostro_types::command::outbox_msg::Kind {
    ostro_types::command::outbox_msg::Kind::CancelRun(ostro_types::command::CancelRun {
        target_run: target.as_bytes().to_vec(),
        reason: "stop".into(),
    })
}

/// Submit and require `Accepted`.
pub async fn submit_ok(node: &ostro_partition::Node, p: PartitionId, cmds: Vec<Command>) {
    let h = handle_of(node, p);
    let out = timeout(T, h.submit(cmds))
        .await
        .expect("submit timed out")
        .expect("submit");
    for o in out {
        assert_eq!(o, ApplyOutcome::Accepted);
    }
}

/// Decoded legacy inbox entries (`InboxMsg`, key run ‖ 0xFFFF ‖ n) of `run`, in inbox order.
/// Signal rows (plan 05-02 `SignalSlot`) are skipped: see [`signal_names`].
pub fn inbox(
    node: &ostro_partition::Node,
    run: &RunId,
) -> Vec<ostro_partition::store::values::InboxMsg> {
    use ostro_partition::store::keys::decode_signal_inbox_key;
    handle_of(node, run.partition())
        .reader()
        .prefix(ostro_partition::Table::Inbox, run.as_bytes(), usize::MAX)
        .expect("inbox")
        .into_iter()
        .filter(|(k, _)| decode_signal_inbox_key(k).is_none())
        .map(|(_, v)| ostro_partition::store::values::decode(&v).expect("decode inbox"))
        .collect()
}

/// Names of the signals delivered to `run` and not yet matched by a wait, in arrival order:
/// the run's `SignalSlot` inbox rows (plan 05-02; `Pending` before seq 0, else `Journaled`)
/// ordered by their inbox counter `n`. Exactly one row per delivered signal, so the
/// shuffler exactly-once suites can count them.
pub fn signal_names(node: &ostro_partition::Node, run: &RunId) -> Vec<String> {
    use ostro_partition::store::keys::decode_signal_inbox_key;
    let mut rows: Vec<(u64, String)> = handle_of(node, run.partition())
        .reader()
        .prefix(ostro_partition::Table::Inbox, run.as_bytes(), usize::MAX)
        .expect("inbox")
        .into_iter()
        .filter_map(|(k, _)| decode_signal_inbox_key(&k).map(|(_, name, n)| (n, name)))
        .collect();
    rows.sort();
    rows.into_iter().map(|(_, name)| name).collect()
}

/// How many cancel requests took effect on `run`: legacy `CancelRequested` inbox rows plus
/// journaled `CancelRequested` events (kind 69, the 05-08 path). 05-08 journals nothing for
/// a run whose journal is empty (`next_seq == 0`, only the flag is set), so a cancelled run
/// with neither counts once.
pub fn cancel_count(node: &ostro_partition::Node, run: &RunId) -> usize {
    use ostro_types::journal::EventKind;
    let h = handle_of(node, run.partition());
    let legacy = inbox(node, run)
        .into_iter()
        .filter(|m| {
            matches!(
                m,
                ostro_partition::store::values::InboxMsg::CancelRequested { .. }
            )
        })
        .count();
    let journaled = ostro_partition::query::journal_rows(&h.reader(), run, 0, usize::MAX)
        .expect("journal")
        .into_iter()
        .filter(|(_, kind, _)| *kind == EventKind::CancelRequested.code())
        .count();
    let rec = h.read_run(run).expect("read run");
    match (legacy + journaled, rec) {
        (0, Some(r)) if r.cancel_requested && r.next_seq == 0 => 1,
        (n, _) => n,
    }
}

/// Outbox rows over every ready partition.
pub fn outbox_rows(node: &ostro_partition::Node) -> usize {
    node.slots()
        .iter()
        .filter_map(|s| match s {
            ostro_partition::PartitionSlot::Ready(h) => Some(h),
            _ => None,
        })
        .map(|h| {
            h.reader()
                .prefix(ostro_partition::Table::Outbox, &[], usize::MAX)
                .expect("outbox")
                .len()
        })
        .sum()
}

/// Poll `f` every 10 ms until it holds; panics with `what` after `limit`.
pub async fn wait_until(limit: Duration, what: &str, mut f: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + limit;
    loop {
        if f() {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out after {limit:?} waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Wait until every outbox of `node` is empty.
pub async fn wait_drained(node: &ostro_partition::Node, limit: Duration) {
    wait_until(limit, "outboxes to drain", || outbox_rows(node) == 0).await;
}
