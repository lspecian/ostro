// Vendored from chronik-stream@08715f4 crates/chronik-raft/src/driver.rs; modified by Ostro (see VENDORED.md).
//! Event-driven Raft driver.
//!
//! One OS thread owns the `RawNode` and its [`RaftLogStore`]. It wakes for a
//! proposal, an incoming message or the next tick deadline, never on a fixed
//! polling interval, so a commit costs a network round trip and an fsync
//! instead of two or three tick boundaries. Running on its own thread keeps it
//! clear of request handling on the async runtimes (issue #58).
//!
//! A proposal returns the state machine's **result**, produced when the entry
//! is applied on the proposing node. Callers can therefore tell a command that
//! was rejected by validation from one that succeeded, and know that the
//! change is visible locally when the future resolves.
//!
//! Proposals are tracked by an id carried in the entry's `context`
//! (`node_id`, `sequence`). If this node stops being leader before an accepted
//! proposal is applied, the proposal fails with [`ProposeError::OutcomeUnknown`]:
//! the entry may still commit under the next leader. Commands are validated at
//! apply time, so retrying one is safe.

use std::collections::{HashMap, VecDeque};
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use prost::Message as ProstMessage;
use raft::eraftpb::{ConfChange, ConfChangeV2, ConfState, Entry, EntryType, Message, Snapshot};
use raft::{Config, RawNode, StateRole, Storage};
use tokio::sync::{oneshot, watch};

use crate::log_store::{LogStoreOptions, RaftLogStore};

/// Ostro (Phase 13, 13-05): a committed entry this state machine must not apply, e.g.
/// one written by a newer binary. The driver stops before `applied` advances: the entry
/// is neither applied nor counted, every pending proposal fails with
/// [`ProposeError::Fatal`], and the message is published in [`RaftStatus::fatal`] (and
/// [`RaftStatus::failed`]). A restart replays the same entry and halts again until the
/// binary is upgraded. See VENDORED.md.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FatalApply(pub String);

impl std::fmt::Display for FatalApply {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for FatalApply {}

/// The replicated state machine. Every method runs on the driver thread.
pub trait StateMachine: Send + 'static {
    /// Apply a committed command. Must be deterministic: every node applies
    /// the same commands in the same order and must reach the same state.
    /// The returned bytes are delivered to the proposer.
    ///
    /// Ostro: `Err(FatalApply)` halts the driver before `applied` advances
    /// (see [`FatalApply`]).
    fn apply(&mut self, index: u64, term: u64, command: &[u8]) -> Result<Vec<u8>, FatalApply>;

    /// Serialize the applied state.
    fn snapshot(&self) -> Vec<u8>;

    /// Replace the state with a snapshot taken at `index`.
    fn restore(&mut self, index: u64, data: &[u8]) -> Result<(), String>;

    /// Called after a membership change is applied.
    fn on_conf_change(&mut self, _index: u64, _conf_state: &ConfState) {}
}

/// Delivers outgoing Raft messages. Must not block the driver thread.
pub trait Outbox: Send + 'static {
    fn send(&mut self, messages: Vec<Message>);
}

#[derive(Debug, Clone)]
pub struct DriverConfig {
    pub id: u64,
    pub tick_interval: Duration,
    pub election_tick: usize,
    pub heartbeat_tick: usize,
    /// Take a snapshot and compact the log after this many applied entries.
    pub snapshot_every: u64,
    pub log_store: LogStoreOptions,
}

impl DriverConfig {
    pub fn new(id: u64) -> Self {
        Self {
            id,
            tick_interval: Duration::from_millis(100),
            election_tick: 20,
            heartbeat_tick: 3,
            snapshot_every: 10_000,
            log_store: LogStoreOptions::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProposeError {
    /// This node is not the leader. `leader` is the leader it knows of.
    NotLeader { leader: Option<u64> },
    /// Raft refused the proposal (e.g. a leadership transfer is in progress).
    Dropped,
    /// Accepted, but this node lost leadership before it was applied here.
    /// The command may or may not take effect.
    OutcomeUnknown,
    /// The driver has stopped.
    Stopped,
    /// Ostro: the driver halted on a [`FatalApply`] (the entry was not applied).
    Fatal(String),
}

impl std::fmt::Display for ProposeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProposeError::NotLeader { leader: Some(l) } => write!(f, "not the raft leader (leader is {})", l),
            ProposeError::NotLeader { leader: None } => write!(f, "not the raft leader (no leader known)"),
            ProposeError::Dropped => write!(f, "proposal dropped by raft"),
            ProposeError::OutcomeUnknown => write!(f, "leadership lost before the proposal was applied"),
            ProposeError::Stopped => write!(f, "raft driver stopped"),
            ProposeError::Fatal(m) => write!(f, "raft driver halted: {}", m),
        }
    }
}

impl std::error::Error for ProposeError {}

/// What a successful proposal returns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Applied {
    pub index: u64,
    pub term: u64,
    /// The state machine's result for a command; the encoded `ConfState` for
    /// a membership change.
    pub result: Vec<u8>,
}

/// Ostro (13-05): the leader's replication progress of one peer.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PeerProgress {
    pub id: u64,
    /// Highest log index known replicated on the peer.
    pub matched: u64,
    /// Next log index the leader sends.
    pub next_idx: u64,
    /// Index of a snapshot in flight to the peer (0 = none).
    pub pending_snapshot: u64,
    /// The peer answered within the last election timeout (check-quorum window).
    pub recent_active: bool,
}

/// A snapshot of the driver's view, published after every cycle.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RaftStatus {
    pub id: u64,
    pub term: u64,
    pub leader_id: Option<u64>,
    pub is_leader: bool,
    pub commit: u64,
    pub applied: u64,
    /// Voters of the (incoming) configuration.
    pub voters: Vec<u64>,
    pub learners: Vec<u64>,
    /// Set when the driver stopped on a storage failure (or a [`FatalApply`]).
    pub failed: Option<String>,
    /// Ostro (13-05): voters of the outgoing configuration; non-empty in joint consensus.
    pub voters_outgoing: Vec<u64>,
    /// Ostro (13-05): voters that become learners when the joint configuration is left.
    pub learners_next: Vec<u64>,
    /// Ostro (13-05): raft leaves the joint configuration by itself once it applied.
    pub auto_leave: bool,
    /// Ostro (13-05): a membership change was proposed and is not applied yet.
    pub pending_conf: bool,
    /// Ostro (13-05): per-peer progress, on the leader only (empty elsewhere).
    pub progress: Vec<PeerProgress>,
    /// Ostro (13-05): set when the driver halted on a [`FatalApply`].
    pub fatal: Option<String>,
}

type Reply = oneshot::Sender<Result<Applied, ProposeError>>;
type ReadReply = oneshot::Sender<Result<u64, ProposeError>>;

enum Event {
    Propose { data: Vec<u8>, reply: Reply },
    ConfChange { cc: ConfChangeV2, reply: Reply },
    Step(Message),
    Unreachable(u64),
    SnapshotFailed(u64),
    Campaign,
    SampleLive { within: Duration, reply: oneshot::Sender<Option<Vec<u64>>> },
    // Ostro (13-05): leadership transfer, linearizable reads, snapshot on demand.
    TransferLeader { to: u64, reply: oneshot::Sender<Result<(), ProposeError>> },
    ReadIndex { reply: ReadReply },
    SnapshotNow,
    Stop,
}

/// Ostro (13-05): `ReadIndex` request contexts. A user read and an internal lease round
/// are told apart by the first byte; the rest is a per-driver sequence.
const CTX_READ: u8 = 1;
const CTX_LEASE: u8 = 2;
/// Ostro (13-05): a follower keeps running lease rounds while its anchor was asked for
/// within this window (the first ask starts them).
const LEASE_DEMAND_WINDOW: Duration = Duration::from_secs(10);

/// Handle to a running driver. Cheap to clone.
#[derive(Clone)]
pub struct RaftHandle {
    tx: Sender<Event>,
    status: watch::Receiver<RaftStatus>,
    /// Milliseconds since `epoch` when this node last heard from (or was) the leader; 0 = never.
    leader_contact: Arc<AtomicU64>,
    epoch: Instant,
    /// Ostro (13-05): the lease anchor, microseconds since `epoch` + 1; 0 = none.
    lease_anchor: Arc<AtomicU64>,
    /// Ostro (13-05): milliseconds since `epoch` + 1 when the anchor was last asked for.
    lease_wanted: Arc<AtomicU64>,
}

impl RaftHandle {
    /// Propose a command; resolves once it is applied on this node.
    pub async fn propose(&self, data: Vec<u8>) -> Result<Applied, ProposeError> {
        let (reply, rx) = oneshot::channel();
        self.tx.send(Event::Propose { data, reply }).map_err(|_| ProposeError::Stopped)?;
        rx.await.unwrap_or(Err(ProposeError::Stopped))
    }

    /// Propose a membership change; resolves once it is applied on this node.
    pub async fn propose_conf_change(&self, cc: ConfChangeV2) -> Result<Applied, ProposeError> {
        let (reply, rx) = oneshot::channel();
        self.tx.send(Event::ConfChange { cc, reply }).map_err(|_| ProposeError::Stopped)?;
        rx.await.unwrap_or(Err(ProposeError::Stopped))
    }

    /// Hand an incoming message to the driver. Never blocks.
    pub fn step(&self, msg: Message) {
        let _ = self.tx.send(Event::Step(msg));
    }

    /// Tell raft a peer could not be reached, so it stops pipelining to it.
    pub fn report_unreachable(&self, peer: u64) {
        let _ = self.tx.send(Event::Unreachable(peer));
    }

    /// Tell raft a snapshot could not be delivered, so it retries instead of
    /// waiting on the peer forever.
    pub fn report_snapshot_failed(&self, peer: u64) {
        let _ = self.tx.send(Event::SnapshotFailed(peer));
    }

    /// On the leader: this node plus every voter or learner it heard from within
    /// `within`. `None` on a follower, whose view of liveness is not the
    /// cluster's. A sample, not a verdict: callers accumulate over a window.
    pub async fn sample_live(&self, within: Duration) -> Option<Vec<u64>> {
        let (reply, rx) = oneshot::channel();
        self.tx.send(Event::SampleLive { within, reply }).ok()?;
        rx.await.ok().flatten()
    }

    /// Start an election now instead of waiting for the election timeout.
    pub fn campaign(&self) {
        let _ = self.tx.send(Event::Campaign);
    }

    /// How long ago this node last heard from the leader, or was leader
    /// itself. `None` if it never has since starting.
    pub fn leader_contact_age(&self) -> Option<Duration> {
        match self.leader_contact.load(Ordering::Relaxed) {
            0 => None,
            ms => Some(self.epoch.elapsed().saturating_sub(Duration::from_millis(ms))),
        }
    }

    pub fn status(&self) -> RaftStatus {
        self.status.borrow().clone()
    }

    /// Ostro (13-05): ask the leader to hand leadership to `to` (raft `MsgTransferLeader`:
    /// the leader brings `to` up to date and sends it `MsgTimeoutNow`). Resolves once the
    /// transfer started; watch [`RaftHandle::status`] for the new leader. `NotLeader` on a
    /// follower. Proposals are dropped while a transfer is in progress.
    pub async fn transfer_leader(&self, to: u64) -> Result<(), ProposeError> {
        let (reply, rx) = oneshot::channel();
        self.tx.send(Event::TransferLeader { to, reply }).map_err(|_| ProposeError::Stopped)?;
        rx.await.unwrap_or(Err(ProposeError::Stopped))
    }

    /// Ostro (13-05): a linearizable read index (raft `ReadIndex`, `ReadOnlyOption::Safe`).
    /// The leader confirms its leadership with a heartbeat round acknowledged by a quorum
    /// and answers its commit index at the time of the call; once this node applied that
    /// index, every write acknowledged before the call is visible locally. Works on
    /// followers (forwarded to the leader). Fails with `NotLeader { leader: None }` while no
    /// leader is known and `Dropped` when the leader changes, the request is lost, or it is
    /// not answered within two election timeouts, so callers never hang.
    pub async fn read_index(&self) -> Result<u64, ProposeError> {
        let (reply, rx) = oneshot::channel();
        self.tx.send(Event::ReadIndex { reply }).map_err(|_| ProposeError::Stopped)?;
        rx.await.unwrap_or(Err(ProposeError::Stopped))
    }

    /// Ostro (13-05): the metadata lease anchor (monotonic, never wall time): the send
    /// instant of the newest `ReadIndex` lease round this node issued that came back
    /// resolved. On the leader that is a heartbeat round acknowledged by a quorum of
    /// voters; on a follower it is a round trip through the leader, which itself confirmed
    /// a quorum, so the leader heard from this node no earlier than the anchor. A one-voter
    /// leader returns now. `None` without a leader, right after a leader or term change,
    /// and after a check-quorum step-down; it stops advancing as soon as the rounds stop
    /// resolving (cut off). A follower runs lease rounds only while asked for its anchor
    /// (within the last 10 s; the first ask starts them and may return `None`).
    pub fn lease_anchor(&self) -> Option<Instant> {
        let now_ms = self.epoch.elapsed().as_millis() as u64 + 1;
        self.lease_wanted.store(now_ms, Ordering::Relaxed);
        {
            let s = self.status.borrow();
            if s.is_leader && s.voters.len() == 1 && s.voters_outgoing.is_empty() && s.fatal.is_none() && s.failed.is_none() {
                return Some(Instant::now());
            }
        }
        match self.lease_anchor.load(Ordering::Relaxed) {
            0 => None,
            us => Some(self.epoch + Duration::from_micros(us - 1)),
        }
    }

    /// Ostro (13-05): [`RaftHandle::lease_anchor`] on the leader, `None` on any other node.
    pub fn leader_lease_anchor(&self) -> Option<Instant> {
        if !self.status.borrow().is_leader {
            return None;
        }
        self.lease_anchor()
    }

    /// Ostro (13-05): take a snapshot of the applied state now and compact the log behind
    /// it (tests and operators; normally every `snapshot_every` entries).
    pub fn snapshot_now(&self) {
        let _ = self.tx.send(Event::SnapshotNow);
    }

    pub fn subscribe(&self) -> watch::Receiver<RaftStatus> {
        self.status.clone()
    }

    /// Resolve once this node has applied `index`. Fails if the driver stops.
    pub async fn wait_applied(&self, index: u64) -> Result<(), ProposeError> {
        let mut rx = self.status.clone();
        loop {
            {
                let s = rx.borrow_and_update();
                if s.applied >= index {
                    return Ok(());
                }
                if let Some(f) = &s.fatal {
                    return Err(ProposeError::Fatal(f.clone()));
                }
                if s.failed.is_some() {
                    return Err(ProposeError::Stopped);
                }
            }
            rx.changed().await.map_err(|_| ProposeError::Stopped)?;
        }
    }

    /// Resolve once some node is leader (this one or another).
    pub async fn wait_for_leader(&self) -> Result<u64, ProposeError> {
        let mut rx = self.status.clone();
        loop {
            if let Some(l) = rx.borrow_and_update().leader_id {
                return Ok(l);
            }
            rx.changed().await.map_err(|_| ProposeError::Stopped)?;
        }
    }
}

/// A running driver; dropping it stops the thread.
pub struct RaftDriver {
    handle: RaftHandle,
    thread: Option<JoinHandle<()>>,
    /// Ostro (P4): the store, for tests (state-file write counts).
    #[cfg(test)]
    store: RaftLogStore,
}

impl RaftDriver {
    /// Open the log in `dir` and start driving it.
    ///
    /// `initial_voters` bootstraps a brand-new cluster: when the log is empty
    /// and holds no snapshot, it is initialized with a snapshot at index 1
    /// whose configuration is `initial_voters` and whose data is the empty
    /// state machine's snapshot. Every founding node must pass the same set.
    /// A node joining an existing cluster passes an empty set and starts
    /// blank; the leader brings it up to date with a snapshot.
    pub fn start<S: StateMachine, O: Outbox>(
        dir: impl Into<PathBuf>,
        config: DriverConfig,
        initial_voters: &[u64],
        mut state_machine: S,
        outbox: O,
    ) -> io::Result<Self> {
        let store = RaftLogStore::open(dir.into(), config.log_store.clone())?;
        let (snap_index, _) = store.snapshot_index_term();
        let blank = snap_index == 0 && store.last_index().unwrap_or(0) == 0;
        if blank && !initial_voters.is_empty() {
            let mut snap = Snapshot::default();
            snap.data = state_machine.snapshot().into();
            let meta = snap.mut_metadata();
            meta.index = 1;
            meta.term = 1;
            meta.mut_conf_state().voters = initial_voters.to_vec();
            store
                .apply_snapshot(snap)?
                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("bootstrap snapshot: {}", e)))?;
        }
        // Rebuild the state machine from the snapshot; raft re-delivers every
        // committed entry after it.
        let (snap_index, _) = store.snapshot_index_term();
        if let Some(data) = store.snapshot_data() {
            state_machine
                .restore(snap_index, &data)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("restoring snapshot: {}", e)))?;
        }

        // Ostro (P4): entries up to the commit index persisted before this open are
        // re-applied by raft (the restart replay); their conf changes are not written to
        // the state file one by one (see `apply_conf_change`).
        let replay_until = store.initial_state().map(|s| s.hard_state.commit).unwrap_or(0);

        let raft_config = Config {
            id: config.id,
            election_tick: config.election_tick,
            heartbeat_tick: config.heartbeat_tick,
            applied: snap_index,
            check_quorum: true,
            pre_vote: true,
            max_size_per_msg: 1024 * 1024,
            max_inflight_msgs: 256,
            ..Default::default()
        };
        raft_config.validate().map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
        let logger = slog::Logger::root(slog::Drain::fuse(slog_stdlog::StdLog), slog::o!("raft_id" => config.id));
        let node = RawNode::new(&raft_config, store.clone(), &logger)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("raft: {}", e)))?;

        let (tx, rx) = mpsc::channel();
        let (status_tx, status_rx) = watch::channel(RaftStatus { id: config.id, applied: snap_index, ..Default::default() });
        let leader_contact = Arc::new(AtomicU64::new(0));
        let lease_anchor = Arc::new(AtomicU64::new(0));
        let lease_wanted = Arc::new(AtomicU64::new(0));
        let epoch = Instant::now();
        let heartbeat_every = config.tick_interval * config.heartbeat_tick.max(1) as u32;
        let read_timeout = config.tick_interval * (config.election_tick.max(1) as u32) * 2;
        let mut core = DriverCore {
            id: config.id,
            node,
            store,
            sm: state_machine,
            outbox,
            pending: HashMap::new(),
            next_seq: 1,
            applied: snap_index,
            snapshot_every: config.snapshot_every,
            status: status_tx,
            leader_contact: leader_contact.clone(),
            last_heard: HashMap::new(),
            fatal: None,
            epoch,
            read_waiters: HashMap::new(),
            deferred_reads: VecDeque::new(),
            next_read: 1,
            seen_leader: (0, 0, false),
            lease_anchor: lease_anchor.clone(),
            lease_wanted: lease_wanted.clone(),
            lease_inflight: None,
            lease_last_issue: None,
            heartbeat_every,
            read_timeout,
            force_snapshot: false,
            halted: None,
            replay_until,
            conf_dirty: false,
        };
        #[cfg(test)]
        let test_store = core.store.clone();
        core.publish_status(None);
        let tick = config.tick_interval;
        let thread = std::thread::Builder::new()
            .name(format!("raft-{}", config.id))
            .spawn(move || core.run(rx, tick))?;
        Ok(Self {
            handle: RaftHandle { tx, status: status_rx, leader_contact, epoch, lease_anchor, lease_wanted },
            thread: Some(thread),
            #[cfg(test)]
            store: test_store,
        })
    }

    pub fn handle(&self) -> RaftHandle {
        self.handle.clone()
    }

    /// Stop the driver and wait for its thread.
    pub fn stop(mut self) {
        self.shutdown();
    }

    fn shutdown(&mut self) {
        let _ = self.handle.tx.send(Event::Stop);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

impl Drop for RaftDriver {
    fn drop(&mut self) {
        self.shutdown();
    }
}

struct Pending {
    reply: Reply,
}

struct DriverCore<S: StateMachine, O: Outbox> {
    id: u64,
    node: RawNode<RaftLogStore>,
    store: RaftLogStore,
    sm: S,
    outbox: O,
    pending: HashMap<u64, Pending>,
    next_seq: u64,
    applied: u64,
    snapshot_every: u64,
    status: watch::Sender<RaftStatus>,
    leader_contact: Arc<AtomicU64>,
    /// When each peer was last heard from.
    last_heard: HashMap<u64, Instant>,
    /// Set when this node must stop: storage it cannot trust, or a log it lost.
    fatal: Option<String>,
    epoch: Instant,
    // ---- Ostro (13-05) ----
    /// Outstanding user `ReadIndex` requests by context, with their deadline.
    read_waiters: HashMap<Vec<u8>, (ReadReply, Instant)>,
    /// Reads waiting for this new leader's first commit in its term (raft drops a
    /// `ReadIndex` before that).
    deferred_reads: VecDeque<(ReadReply, Instant)>,
    next_read: u64,
    /// (term, leader id, is leader) seen at the end of the last cycle.
    seen_leader: (u64, u64, bool),
    lease_anchor: Arc<AtomicU64>,
    lease_wanted: Arc<AtomicU64>,
    /// The lease round in flight: (context sequence, send instant).
    lease_inflight: Option<(u64, Instant)>,
    lease_last_issue: Option<Instant>,
    heartbeat_every: Duration,
    read_timeout: Duration,
    force_snapshot: bool,
    /// Set by a `FatalApply`: the cycle stops with this message.
    halted: Option<String>,
    // ---- Ostro (P4, p13-campaign) ----
    /// The commit index persisted before this open: conf changes at or below it are
    /// replayed, and their configuration is kept in memory until the replay is done.
    replay_until: u64,
    /// The in-memory configuration is newer than the state file.
    conf_dirty: bool,
}

/// Fails the driver: raft must not continue on storage it cannot trust.
struct Fatal(String);

impl From<io::Error> for Fatal {
    fn from(e: io::Error) -> Self {
        Fatal(format!("raft storage: {}", e))
    }
}

impl<S: StateMachine, O: Outbox> DriverCore<S, O> {
    fn run(&mut self, rx: Receiver<Event>, tick: Duration) {
        let mut next_tick = Instant::now() + tick;
        loop {
            let timeout = next_tick.saturating_duration_since(Instant::now());
            match rx.recv_timeout(timeout) {
                Ok(Event::Stop) | Err(RecvTimeoutError::Disconnected) => break,
                Ok(ev) => {
                    if self.handle_event(ev) {
                        break;
                    }
                    // Batch whatever else is already queued into this cycle.
                    let mut stop = false;
                    while let Ok(ev) = rx.try_recv() {
                        if matches!(ev, Event::Stop) || self.handle_event(ev) {
                            stop = true;
                            break;
                        }
                    }
                    if stop {
                        break;
                    }
                }
                Err(RecvTimeoutError::Timeout) => {}
            }
            let now = Instant::now();
            if now >= next_tick {
                self.node.tick();
                next_tick = now + tick;
            }
            if self.node.raft.state == StateRole::Leader {
                self.touch_leader_contact();
            }
            self.track_leader_change();
            self.issue_reads();
            let result = match self.fatal.take() {
                Some(msg) => Err(Fatal(msg)),
                None => self.process_ready(),
            };
            if let Err(Fatal(msg)) = result {
                tracing::error!("Raft node {} stopping: {}", self.id, msg);
                match self.halted.take() {
                    // Ostro (13-05): a FatalApply; `applied` did not advance past the entry.
                    Some(h) => {
                        self.fail_pending(ProposeError::Fatal(h.clone()));
                        self.fail_reads(ProposeError::Fatal(h.clone()));
                        self.lease_anchor.store(0, Ordering::Relaxed);
                        self.publish_status_full(Some(msg), Some(h));
                    }
                    None => {
                        self.fail_pending(ProposeError::Stopped);
                        self.fail_reads(ProposeError::Stopped);
                        self.lease_anchor.store(0, Ordering::Relaxed);
                        self.publish_status(Some(msg));
                    }
                }
                return;
            }
            self.track_leader_change();
        }
        self.fail_pending(ProposeError::Stopped);
        self.fail_reads(ProposeError::Stopped);
    }

    /// Ostro (13-05): on a term or leader change, drop every outstanding read (its answer
    /// may never come) and the lease: a new leader must earn it again.
    fn track_leader_change(&mut self) {
        let now = (self.node.raft.term, self.node.raft.leader_id, self.node.raft.state == StateRole::Leader);
        if now != self.seen_leader {
            self.seen_leader = now;
            self.lease_anchor.store(0, Ordering::Relaxed);
            self.lease_inflight = None;
            self.lease_last_issue = None;
            for (_, (reply, _)) in self.read_waiters.drain() {
                let _ = reply.send(Err(ProposeError::Dropped));
            }
            if !now.2 {
                for (reply, _) in self.deferred_reads.drain(..) {
                    let _ = reply.send(Err(ProposeError::Dropped));
                }
            }
        }
    }

    /// Ostro (13-05): whether raft accepts a `ReadIndex` here now. A leader must have
    /// committed an entry in its term first; a follower needs a known leader.
    fn reads_accepted(&self) -> bool {
        let r = &self.node.raft;
        if r.state == StateRole::Leader {
            r.raft_log.term(r.raft_log.committed).map(|t| t == r.term).unwrap_or(false)
        } else {
            r.leader_id != 0
        }
    }

    fn read_ctx(&mut self, kind: u8) -> (u64, Vec<u8>) {
        let seq = self.next_read;
        self.next_read += 1;
        let mut ctx = Vec::with_capacity(9);
        ctx.push(kind);
        ctx.extend_from_slice(&seq.to_le_bytes());
        (seq, ctx)
    }

    /// Ostro (13-05): expire overdue reads, issue deferred ones, and run the lease round.
    fn issue_reads(&mut self) {
        let now = Instant::now();
        let expired: Vec<Vec<u8>> =
            self.read_waiters.iter().filter(|(_, (_, deadline))| *deadline <= now).map(|(k, _)| k.clone()).collect();
        for k in expired {
            if let Some((reply, _)) = self.read_waiters.remove(&k) {
                let _ = reply.send(Err(ProposeError::Dropped));
            }
        }
        while let Some((_, deadline)) = self.deferred_reads.front() {
            if *deadline > now {
                break;
            }
            let (reply, _) = self.deferred_reads.pop_front().unwrap();
            let _ = reply.send(Err(ProposeError::Dropped));
        }
        if !self.reads_accepted() {
            return;
        }
        while let Some((reply, deadline)) = self.deferred_reads.pop_front() {
            let (_, ctx) = self.read_ctx(CTX_READ);
            self.node.read_index(ctx.clone());
            self.read_waiters.insert(ctx, (reply, deadline));
        }
        // Lease rounds: always on a multi-voter leader, on a follower while asked for.
        let leader = self.node.raft.state == StateRole::Leader;
        if leader && self.node.raft.prs().is_singleton() {
            return;
        }
        if !leader {
            let wanted = self.lease_wanted.load(Ordering::Relaxed);
            let age_ms = (self.epoch.elapsed().as_millis() as u64 + 1).saturating_sub(wanted);
            if wanted == 0 || Duration::from_millis(age_ms) > LEASE_DEMAND_WINDOW {
                return;
            }
        }
        if let Some((_, sent)) = self.lease_inflight {
            if now.duration_since(sent) < self.read_timeout {
                return;
            }
            self.lease_inflight = None;
        }
        if self.lease_last_issue.map(|t| now.duration_since(t) < self.heartbeat_every).unwrap_or(false) {
            return;
        }
        let (seq, ctx) = self.read_ctx(CTX_LEASE);
        self.node.read_index(ctx);
        self.lease_inflight = Some((seq, now));
        self.lease_last_issue = Some(now);
    }

    fn fail_reads(&mut self, err: ProposeError) {
        for (_, (reply, _)) in self.read_waiters.drain() {
            let _ = reply.send(Err(err.clone()));
        }
        for (reply, _) in self.deferred_reads.drain(..) {
            let _ = reply.send(Err(err.clone()));
        }
    }

    /// Ostro (13-05): answer the `ReadState`s of a `Ready`.
    fn resolve_reads(&mut self, states: Vec<raft::ReadState>) {
        for rs in states {
            let ctx = rs.request_ctx;
            match ctx.first() {
                Some(&CTX_LEASE) if ctx.len() == 9 => {
                    let seq = u64::from_le_bytes(ctx[1..].try_into().unwrap());
                    if let Some((want, sent)) = self.lease_inflight {
                        if want == seq {
                            self.lease_inflight = None;
                            let us = sent.saturating_duration_since(self.epoch).as_micros() as u64 + 1;
                            self.lease_anchor.fetch_max(us, Ordering::Relaxed);
                        }
                    }
                }
                _ => {
                    if let Some((reply, _)) = self.read_waiters.remove(&ctx) {
                        let _ = reply.send(Ok(rs.index));
                    }
                }
            }
        }
    }

    /// Returns true to stop.
    fn handle_event(&mut self, ev: Event) -> bool {
        match ev {
            Event::Stop => return true,
            Event::Propose { data, reply } => {
                if let Some(reply) = self.check_leader(reply) {
                    let seq = self.next_seq();
                    match self.node.propose(self.context(seq), data) {
                        Ok(()) => {
                            self.pending.insert(seq, Pending { reply });
                        }
                        Err(_) => {
                            let _ = reply.send(Err(ProposeError::Dropped));
                        }
                    }
                }
            }
            Event::ConfChange { cc, reply } => {
                if let Some(reply) = self.check_leader(reply) {
                    let seq = self.next_seq();
                    match self.node.propose_conf_change(self.context(seq), cc) {
                        Ok(()) => {
                            self.pending.insert(seq, Pending { reply });
                        }
                        Err(_) => {
                            let _ = reply.send(Err(ProposeError::Dropped));
                        }
                    }
                }
            }
            Event::Step(msg) => {
                // A heartbeat committing beyond our log means this node lost
                // entries it had acknowledged (its data was wiped or restored
                // from an old copy). raft-rs would abort the process; stop the
                // catalog with the remedy instead.
                if msg.get_msg_type() == raft::eraftpb::MessageType::MsgHeartbeat
                    && msg.commit > self.node.raft.raft_log.last_index()
                {
                    self.fatal = Some(format!(
                        "leader {} has committed through {} but this node's catalog log ends at {}: \
                         this node lost raft state it had acknowledged (data wiped or restored). \
                         Node {}'s metadata Raft log is missing or blank while it is a configured voter: \
                         restore the meta dir from backup, or wipe the whole data dir. Remove it with \
                         `ostro cluster remove <id> --force`, wipe its data dir, and add it back with \
                         `ostro cluster join` (it gets a new node id).",
                        msg.from, msg.commit, self.node.raft.raft_log.last_index(), self.id
                    ));
                    return false; // the loop reports `fatal` and stops
                }
                let from = msg.from;
                if from != 0 {
                    self.last_heard.insert(from, Instant::now());
                }
                let term = msg.term;
                if let Err(e) = self.node.step(msg) {
                    tracing::debug!("Raft node {}: step: {}", self.id, e);
                }
                if from != 0 && from == self.node.raft.leader_id && term == self.node.raft.term {
                    self.touch_leader_contact();
                }
            }
            Event::Unreachable(peer) => self.node.report_unreachable(peer),
            Event::SnapshotFailed(peer) => self.node.report_snapshot(peer, raft::SnapshotStatus::Failure),
            Event::SampleLive { within, reply } => {
                let live = (self.node.raft.state == StateRole::Leader).then(|| {
                    let mut ids = vec![self.id];
                    // Ostro: voters ∪ learners (raft heartbeats reach learners too), so the
                    // controller sees a dead learner node. See VENDORED.md.
                    let conf = self.node.raft.prs().conf();
                    let cs = conf.to_conf_state();
                    let voters = conf.voters().ids();
                    for id in voters.iter().chain(cs.learners.iter().copied()).chain(cs.learners_next.iter().copied()) {
                        if id != self.id && self.last_heard.get(&id).map(|t| t.elapsed() <= within).unwrap_or(false) {
                            ids.push(id);
                        }
                    }
                    ids.sort_unstable();
                    ids.dedup();
                    ids
                });
                let _ = reply.send(live);
            }
            Event::Campaign => {
                let _ = self.node.campaign();
            }
            Event::TransferLeader { to, reply } => {
                if self.node.raft.state != StateRole::Leader {
                    let leader = Some(self.node.raft.leader_id).filter(|&l| l != 0);
                    let _ = reply.send(Err(ProposeError::NotLeader { leader }));
                } else if to == self.id {
                    let _ = reply.send(Ok(()));
                } else if !self.node.raft.prs().conf().voters().contains(to) {
                    // raft ignores a transfer to a learner or a stranger.
                    let _ = reply.send(Err(ProposeError::Dropped));
                } else {
                    self.node.transfer_leader(to);
                    let _ = reply.send(Ok(()));
                }
            }
            Event::ReadIndex { reply } => {
                let deadline = Instant::now() + self.read_timeout;
                let r = &self.node.raft;
                if r.state != StateRole::Leader && r.leader_id == 0 {
                    let _ = reply.send(Err(ProposeError::NotLeader { leader: None }));
                } else if self.reads_accepted() {
                    let (_, ctx) = self.read_ctx(CTX_READ);
                    self.node.read_index(ctx.clone());
                    self.read_waiters.insert(ctx, (reply, deadline));
                } else {
                    self.deferred_reads.push_back((reply, deadline));
                }
            }
            Event::SnapshotNow => self.force_snapshot = true,
        }
        false
    }

    fn check_leader(&self, reply: Reply) -> Option<Reply> {
        if self.node.raft.state == StateRole::Leader {
            Some(reply)
        } else {
            let leader = Some(self.node.raft.leader_id).filter(|&l| l != 0);
            let _ = reply.send(Err(ProposeError::NotLeader { leader }));
            None
        }
    }

    fn next_seq(&mut self) -> u64 {
        let s = self.next_seq;
        self.next_seq += 1;
        s
    }

    fn context(&self, seq: u64) -> Vec<u8> {
        let mut c = Vec::with_capacity(16);
        c.extend_from_slice(&self.id.to_le_bytes());
        c.extend_from_slice(&seq.to_le_bytes());
        c
    }

    /// The pending sequence an entry answers, if this node proposed it.
    fn own_seq(&self, entry: &Entry) -> Option<u64> {
        let c = &entry.context;
        if c.len() != 16 || u64::from_le_bytes(c[..8].try_into().unwrap()) != self.id {
            return None;
        }
        Some(u64::from_le_bytes(c[8..].try_into().unwrap()))
    }

    fn process_ready(&mut self) -> Result<(), Fatal> {
        if !self.node.has_ready() {
            // Ostro (13-05): `snapshot_now` without anything else to do.
            if self.force_snapshot {
                self.maybe_snapshot()?;
            }
            return Ok(());
        }
        let mut ready = self.node.ready();

        // Ostro (13-05): ReadIndex answers.
        let states = ready.take_read_states();
        if !states.is_empty() {
            self.resolve_reads(states);
        }

        // A leader may send before persisting (raft thesis §10.2.1).
        if !ready.messages().is_empty() {
            self.outbox.send(ready.take_messages());
        }

        if !ready.snapshot().is_empty() {
            let snap = ready.snapshot().clone();
            let index = snap.get_metadata().index;
            match self.store.apply_snapshot(snap.clone())? {
                Ok(()) => {
                    self.sm
                        .restore(index, &snap.data)
                        .map_err(|e| Fatal(format!("restoring snapshot at {}: {}", index, e)))?;
                    self.applied = index;
                    self.sm.on_conf_change(index, snap.get_metadata().get_conf_state());
                    tracing::info!("Raft node {}: installed snapshot at index {}", self.id, index);
                }
                Err(e) => tracing::warn!("Raft node {}: snapshot at {} not installed: {}", self.id, index, e),
            }
        }

        let committed = ready.take_committed_entries();
        self.apply_entries(committed)?;

        self.store.append(ready.entries())?;
        if let Some(hs) = ready.hs() {
            self.store.set_hardstate(hs.clone())?;
        }
        if !ready.persisted_messages().is_empty() {
            self.outbox.send(ready.take_persisted_messages());
        }
        let lost_leadership = ready.ss().map(|ss| ss.raft_state != StateRole::Leader).unwrap_or(false);

        let mut light = self.node.advance(ready);
        if let Some(commit) = light.commit_index() {
            self.store.set_commit(commit)?;
        }
        if !light.messages().is_empty() {
            self.outbox.send(light.take_messages());
        }
        let committed = light.take_committed_entries();
        self.apply_entries(committed)?;
        self.node.advance_apply();

        if lost_leadership {
            self.fail_pending(ProposeError::OutcomeUnknown);
        }
        self.maybe_snapshot()?;
        self.publish_status(None);
        Ok(())
    }

    fn apply_entries(&mut self, entries: Vec<Entry>) -> Result<(), Fatal> {
        for entry in entries {
            if entry.index <= self.applied {
                continue;
            }
            let result = match entry.get_entry_type() {
                EntryType::EntryNormal => {
                    // An empty normal entry is the new leader's no-op.
                    if entry.data.is_empty() {
                        None
                    } else {
                        match self.sm.apply(entry.index, entry.term, &entry.data) {
                            Ok(r) => Some(r),
                            // Ostro (13-05): halt before `applied` advances (FatalApply).
                            Err(FatalApply(m)) => {
                                self.halted = Some(m.clone());
                                return Err(Fatal(format!(
                                    "state machine refused entry {} (term {}): {}",
                                    entry.index, entry.term, m
                                )));
                            }
                        }
                    }
                }
                EntryType::EntryConfChange | EntryType::EntryConfChangeV2 => Some(self.apply_conf_change(&entry)?),
            };
            self.applied = entry.index;
            if let (Some(seq), Some(result)) = (self.own_seq(&entry), result) {
                if let Some(p) = self.pending.remove(&seq) {
                    let _ = p.reply.send(Ok(Applied { index: entry.index, term: entry.term, result }));
                }
            }
        }
        // Ostro (P4): the replay is done: write the configuration it reached, once.
        if self.conf_dirty && self.applied >= self.replay_until {
            self.store.flush_state()?;
            self.conf_dirty = false;
        }
        Ok(())
    }

    fn apply_conf_change(&mut self, entry: &Entry) -> Result<Vec<u8>, Fatal> {
        let applied = if entry.get_entry_type() == EntryType::EntryConfChangeV2 {
            let cc = ConfChangeV2::decode(entry.data.as_ref())
                .map_err(|e| Fatal(format!("undecodable conf change at {}: {}", entry.index, e)))?;
            self.node.apply_conf_change(&cc)
        } else {
            let cc = ConfChange::decode(entry.data.as_ref())
                .map_err(|e| Fatal(format!("undecodable conf change at {}: {}", entry.index, e)))?;
            self.node.apply_conf_change(&cc)
        };
        match applied {
            Ok(cs) => {
                // Ostro (P4, p13-campaign): a replayed conf change (committed before this
                // open) only updates the in-memory configuration; `apply_entries` writes
                // the state file once the replay is done. Writing and fsyncing the state
                // file for each one made a restart take seconds to minutes on a busy disk
                // after many membership changes (up to `snapshot_every` entries replayed).
                if entry.index <= self.replay_until {
                    self.store.set_conf_state_deferred(cs.clone());
                    self.conf_dirty = true;
                } else if self.conf_dirty {
                    self.store.set_conf_state_deferred(cs.clone());
                } else {
                    self.store.set_conf_state(cs.clone())?;
                }
                self.sm.on_conf_change(entry.index, &cs);
                tracing::info!(
                    "Raft node {}: membership at {} is voters {:?} learners {:?}",
                    self.id,
                    entry.index,
                    cs.voters,
                    cs.learners
                );
                Ok(cs.encode_to_vec())
            }
            // Raft rejects an invalid change deterministically on every node;
            // the proposer learns it from an empty result.
            Err(e) => {
                tracing::warn!("Raft node {}: conf change at {} rejected: {}", self.id, entry.index, e);
                Ok(Vec::new())
            }
        }
    }

    fn maybe_snapshot(&mut self) -> Result<(), Fatal> {
        let (snap_index, _) = self.store.snapshot_index_term();
        let due = self.applied >= snap_index + self.snapshot_every;
        let asked = (self.store.snapshot_requested() || self.force_snapshot) && self.applied > snap_index;
        self.force_snapshot = false;
        if !(due || asked) || self.applied <= snap_index {
            return Ok(());
        }
        let cs = self.node.raft.prs().conf().to_conf_state();
        self.store.create_snapshot(self.applied, cs, self.sm.snapshot())?;
        tracing::debug!("Raft node {}: snapshot at {}, log compacted", self.id, self.applied);
        Ok(())
    }

    fn touch_leader_contact(&self) {
        let ms = self.epoch.elapsed().as_millis() as u64;
        self.leader_contact.store(ms.max(1), Ordering::Relaxed);
    }

    fn fail_pending(&mut self, err: ProposeError) {
        for (_, p) in self.pending.drain() {
            let _ = p.reply.send(Err(err.clone()));
        }
    }

    fn publish_status(&self, failed: Option<String>) {
        self.publish_status_full(failed, None)
    }

    fn publish_status_full(&self, failed: Option<String>, fatal: Option<String>) {
        let cs = self.node.raft.prs().conf().to_conf_state();
        let is_leader = self.node.raft.state == StateRole::Leader;
        // Ostro (13-05): per-peer progress on the leader.
        let mut progress: Vec<PeerProgress> = if is_leader {
            self.node
                .raft
                .prs()
                .iter()
                .map(|(&id, pr)| PeerProgress {
                    id,
                    matched: pr.matched,
                    next_idx: pr.next_idx,
                    pending_snapshot: pr.pending_snapshot,
                    recent_active: pr.recent_active,
                })
                .collect()
        } else {
            Vec::new()
        };
        progress.sort_by_key(|p| p.id);
        let mut voters = cs.voters;
        voters.sort_unstable();
        let mut learners = cs.learners;
        learners.sort_unstable();
        let mut voters_outgoing = cs.voters_outgoing;
        voters_outgoing.sort_unstable();
        let mut learners_next = cs.learners_next;
        learners_next.sort_unstable();
        let status = RaftStatus {
            id: self.id,
            term: self.node.raft.term,
            leader_id: Some(self.node.raft.leader_id).filter(|&l| l != 0),
            is_leader,
            commit: self.node.raft.raft_log.committed,
            applied: self.applied,
            voters,
            learners,
            failed,
            voters_outgoing,
            learners_next,
            auto_leave: cs.auto_leave,
            pending_conf: self.node.raft.pending_conf_index > self.applied,
            progress,
            fatal,
        };
        self.status.send_if_modified(|cur| {
            if *cur != status {
                *cur = status;
                true
            } else {
                false
            }
        });
    }
}

/// An `Outbox` backed by a channel, for transports that send from an async task.
pub struct ChannelOutbox(pub tokio::sync::mpsc::UnboundedSender<Vec<Message>>);

impl Outbox for ChannelOutbox {
    fn send(&mut self, messages: Vec<Message>) {
        let _ = self.0.send(messages);
    }
}

// Ostro (13-05): membership, ReadIndex, transfer, lease and FatalApply tests; they reuse
// the `tests` harness below (its items are `pub(super)` for this).
#[cfg(test)]
#[path = "membership_tests.rs"]
mod membership_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeSet, HashSet};
    use std::sync::{Arc, Mutex};
    use tempfile::TempDir;

    /// `create:<name>` succeeds once per name; the state is shared so tests can read it.
    #[derive(Default, Clone)]
    pub(super) struct Names(Arc<Mutex<(BTreeSet<String>, Vec<String>)>>);

    impl Names {
        pub(super) fn set(&self) -> BTreeSet<String> {
            self.0.lock().unwrap().0.clone()
        }
        pub(super) fn applied_log(&self) -> Vec<String> {
            self.0.lock().unwrap().1.clone()
        }
    }

    impl StateMachine for Names {
        fn apply(&mut self, _index: u64, _term: u64, command: &[u8]) -> Result<Vec<u8>, FatalApply> {
            let cmd = String::from_utf8(command.to_vec()).unwrap();
            // Ostro (13-05): `halt:<x>` is an entry this state machine refuses.
            if let Some(why) = cmd.strip_prefix("halt:") {
                return Err(FatalApply(why.to_string()));
            }
            let mut st = self.0.lock().unwrap();
            st.1.push(cmd.clone());
            let name = cmd.strip_prefix("create:").expect("create command").to_string();
            if st.0.insert(name) { Ok(b"created".to_vec()) } else { Ok(b"exists".to_vec()) }
        }
        fn snapshot(&self) -> Vec<u8> {
            serde_json::to_vec(&self.0.lock().unwrap().0).unwrap()
        }
        fn restore(&mut self, _index: u64, data: &[u8]) -> Result<(), String> {
            let set: BTreeSet<String> = serde_json::from_slice(data).map_err(|e| e.to_string())?;
            let mut st = self.0.lock().unwrap();
            st.0 = set;
            st.1.clear();
            Ok(())
        }
    }

    #[derive(Default)]
    pub(super) struct Net {
        handles: HashMap<u64, RaftHandle>,
        cut: HashSet<(u64, u64)>,
        // Ostro (13-05): one-way delay of every message (lease tests).
        pub(super) delay: Duration,
    }

    #[derive(Clone, Default)]
    pub(super) struct Router(pub(super) Arc<Mutex<Net>>);

    impl Router {
        pub(super) fn outbox(&self, from: u64) -> RouterOutbox {
            RouterOutbox { router: self.clone(), from }
        }
        pub(super) fn register(&self, id: u64, h: RaftHandle) {
            self.0.lock().unwrap().handles.insert(id, h);
        }
        pub(super) fn unregister(&self, id: u64) {
            self.0.lock().unwrap().handles.remove(&id);
        }
        pub(super) fn isolate(&self, id: u64, peers: &[u64]) {
            let mut n = self.0.lock().unwrap();
            for &p in peers {
                n.cut.insert((id, p));
                n.cut.insert((p, id));
            }
        }
        pub(super) fn heal(&self) {
            self.0.lock().unwrap().cut.clear();
        }
    }

    pub(super) struct RouterOutbox {
        router: Router,
        from: u64,
    }

    impl Outbox for RouterOutbox {
        fn send(&mut self, messages: Vec<Message>) {
            let n = self.router.0.lock().unwrap();
            let delay = n.delay;
            for m in messages {
                if n.cut.contains(&(self.from, m.to)) {
                    continue;
                }
                if let Some(h) = n.handles.get(&m.to) {
                    if delay.is_zero() {
                        h.step(m);
                    } else {
                        let h = h.clone();
                        std::thread::spawn(move || {
                            std::thread::sleep(delay);
                            h.step(m);
                        });
                    }
                }
            }
        }
    }

    // Ostro: election_tick 10 -> 50 (election timeout 100-200 ms -> 500-1000 ms). Under
    // heavy machine load a driver thread stalled on fsync for >200 ms let followers depose
    // a healthy leader mid-test, failing an `unwrap()` with OutcomeUnknown. Heartbeats
    // stay at 20 ms. Test-only timeout; see VENDORED.md.
    pub(super) fn config(id: u64) -> DriverConfig {
        DriverConfig {
            id,
            tick_interval: Duration::from_millis(10),
            election_tick: 50,
            heartbeat_tick: 2,
            snapshot_every: 10_000,
            log_store: LogStoreOptions::default(),
        }
    }

    pub(super) struct Cluster {
        pub(super) router: Router,
        pub(super) dirs: HashMap<u64, TempDir>,
        pub(super) drivers: HashMap<u64, RaftDriver>,
        pub(super) sms: HashMap<u64, Names>,
        pub(super) ids: Vec<u64>,
        pub(super) cfg: fn(u64) -> DriverConfig,
    }

    impl Cluster {
        pub(super) fn new(ids: &[u64], cfg: fn(u64) -> DriverConfig) -> Self {
            let mut c = Cluster {
                router: Router::default(),
                dirs: ids.iter().map(|&i| (i, TempDir::new().unwrap())).collect(),
                drivers: HashMap::new(),
                sms: HashMap::new(),
                ids: ids.to_vec(),
                cfg,
            };
            for &id in ids {
                c.start(id);
            }
            c
        }

        pub(super) fn start(&mut self, id: u64) {
            let sm = Names::default();
            let d = RaftDriver::start(
                self.dirs[&id].path(),
                (self.cfg)(id),
                &self.ids,
                sm.clone(),
                self.router.outbox(id),
            )
            .unwrap();
            self.router.register(id, d.handle());
            self.sms.insert(id, sm);
            self.drivers.insert(id, d);
        }

        pub(super) fn stop(&mut self, id: u64) {
            self.router.unregister(id);
            if let Some(d) = self.drivers.remove(&id) {
                d.stop();
            }
        }

        pub(super) fn h(&self, id: u64) -> RaftHandle {
            self.drivers[&id].handle()
        }

        pub(super) async fn leader(&self) -> u64 {
            // Ostro: 10 s -> 30 s hang guard (slower elections, loaded machines).
            let deadline = Instant::now() + Duration::from_secs(30);
            loop {
                let leaders: Vec<u64> =
                    self.drivers.iter().filter(|(_, d)| d.handle().status().is_leader).map(|(&i, _)| i).collect();
                if leaders.len() == 1 {
                    return leaders[0];
                }
                assert!(Instant::now() < deadline, "no single leader elected");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }

        /// Wait until every running node has applied `index`.
        pub(super) async fn settle(&self, index: u64) {
            for d in self.drivers.values() {
                // Ostro: 10 s -> 30 s hang guard (loaded machines).
                tokio::time::timeout(Duration::from_secs(30), d.handle().wait_applied(index))
                    .await
                    .expect("node did not catch up")
                    .unwrap();
            }
        }
    }

    pub(super) async fn create(h: &RaftHandle, name: &str) -> Result<Applied, ProposeError> {
        h.propose(format!("create:{}", name).into_bytes()).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn single_node_returns_results_and_survives_restart() {
        let mut c = Cluster::new(&[1], config);
        c.leader().await;
        let a = create(&c.h(1), "a").await.unwrap();
        assert_eq!(a.result, b"created");
        assert_eq!(create(&c.h(1), "a").await.unwrap().result, b"exists");
        c.stop(1);
        c.start(1);
        c.leader().await;
        // The restarted node replays its log before serving.
        assert_eq!(create(&c.h(1), "a").await.unwrap().result, b"exists");
        assert_eq!(c.sms[&1].set(), BTreeSet::from(["a".to_string()]));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn followers_refuse_and_name_the_leader() {
        let c = Cluster::new(&[1, 2, 3], config);
        let leader = c.leader().await;
        let follower = *c.ids.iter().find(|&&i| i != leader).unwrap();
        // The follower learns who leads from the first heartbeat.
        c.h(follower).wait_for_leader().await.unwrap();
        assert_eq!(
            create(&c.h(follower), "x").await,
            Err(ProposeError::NotLeader { leader: Some(leader) })
        );
        let r = create(&c.h(leader), "x").await.unwrap();
        c.settle(r.index).await;
        for id in &c.ids {
            assert_eq!(c.sms[id].set(), BTreeSet::from(["x".to_string()]), "node {}", id);
        }
    }

    /// Event-driven: a commit must not wait for tick boundaries. With 100 ms
    /// ticks, the old loop needed 200–300 ms per commit.
    #[tokio::test(flavor = "multi_thread")]
    async fn commits_do_not_wait_for_ticks() {
        fn slow_ticks(id: u64) -> DriverConfig {
            DriverConfig { tick_interval: Duration::from_millis(100), election_tick: 10, ..config(id) }
        }
        let c = Cluster::new(&[1, 2, 3], slow_ticks);
        let leader = c.leader().await;
        let h = c.h(leader);
        let start = Instant::now();
        for i in 0..100 {
            create(&h, &format!("n{}", i)).await.unwrap();
        }
        let per = start.elapsed() / 100;
        assert!(per < Duration::from_millis(60), "{:?} per sequential commit", per);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_creates_succeed_once_per_name() {
        let c = Cluster::new(&[1, 2, 3], config);
        let h = c.h(c.leader().await);
        let mut tasks = Vec::new();
        for i in 0..200 {
            let h = h.clone();
            tasks.push(tokio::spawn(async move { (i % 20, create(&h, &format!("t{}", i % 20)).await.unwrap()) }));
        }
        let mut created: HashMap<u64, usize> = HashMap::new();
        let mut max_index = 0;
        for t in tasks {
            let (name, r) = t.await.unwrap();
            max_index = max_index.max(r.index);
            if r.result == b"created" {
                *created.entry(name).or_default() += 1;
            }
        }
        assert_eq!(created.len(), 20);
        assert!(created.values().all(|&n| n == 1), "{:?}", created);
        c.settle(max_index).await;
        let want = c.sms[&1].set();
        for id in &c.ids {
            assert_eq!(c.sms[id].set(), want);
            assert_eq!(c.sms[id].applied_log(), c.sms[&1].applied_log(), "same order on node {}", id);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn leader_failover_and_rejoin() {
        let mut c = Cluster::new(&[1, 2, 3], config);
        let old = c.leader().await;
        create(&c.h(old), "before").await.unwrap();
        c.stop(old);
        let new = c.leader().await;
        assert_ne!(new, old);
        let r = create(&c.h(new), "after").await.unwrap();
        c.start(old);
        c.settle(r.index).await;
        let want = BTreeSet::from(["before".to_string(), "after".to_string()]);
        for id in &c.ids {
            assert_eq!(c.sms[id].set(), want, "node {}", id);
        }
    }

    /// A leader cut off from the majority steps down (check-quorum); its
    /// in-flight proposal fails as OutcomeUnknown and is never applied
    /// anywhere, because the majority elected a leader without it.
    #[tokio::test(flavor = "multi_thread")]
    async fn isolated_leader_steps_down_and_its_proposal_is_lost() {
        let c = Cluster::new(&[1, 2, 3], config);
        let old = c.leader().await;
        let others: Vec<u64> = c.ids.iter().copied().filter(|&i| i != old).collect();
        c.router.isolate(old, &others);

        let stranded = tokio::time::timeout(Duration::from_secs(10), create(&c.h(old), "stranded")).await.unwrap();
        assert_eq!(stranded, Err(ProposeError::OutcomeUnknown));

        // The majority elects one of its own and keeps going.
        let new = loop {
            let l = others.iter().copied().find(|&i| c.h(i).status().is_leader);
            if let Some(l) = l {
                break l;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        let r = create(&c.h(new), "majority").await.unwrap();
        c.router.heal();
        c.settle(r.index).await;
        for id in &c.ids {
            assert_eq!(c.sms[id].set(), BTreeSet::from(["majority".to_string()]), "node {}", id);
        }
    }

    /// A node down long enough for the leader to compact past it catches up
    /// by snapshot, then by log.
    #[tokio::test(flavor = "multi_thread")]
    async fn lagging_node_catches_up_by_snapshot() {
        fn snappy(id: u64) -> DriverConfig {
            DriverConfig { snapshot_every: 20, ..config(id) }
        }
        let mut c = Cluster::new(&[1, 2, 3], snappy);
        let leader = c.leader().await;
        let down = *c.ids.iter().find(|&&i| i != leader).unwrap();
        c.stop(down);
        let mut last = 0;
        for i in 0..100 {
            last = create(&c.h(leader), &format!("k{}", i)).await.unwrap().index;
        }
        c.start(down);
        c.settle(last).await;
        assert_eq!(c.sms[&down].set().len(), 100);
        assert_eq!(c.sms[&down].set(), c.sms[&leader].set());
        // It never replayed the compacted commands one by one.
        assert!(c.sms[&down].applied_log().len() < 100, "{} replayed", c.sms[&down].applied_log().len());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_whole_cluster_restart_keeps_everything() {
        let mut c = Cluster::new(&[1, 2, 3], config);
        let h = c.h(c.leader().await);
        for i in 0..30 {
            create(&h, &format!("r{}", i)).await.unwrap();
        }
        for id in c.ids.clone() {
            c.stop(id);
        }
        for id in c.ids.clone() {
            c.start(id);
        }
        let h = c.h(c.leader().await);
        let r = create(&h, "r0").await.unwrap();
        assert_eq!(r.result, b"exists");
        c.settle(r.index).await;
        for id in &c.ids {
            assert_eq!(c.sms[id].set().len(), 30, "node {}", id);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn leader_samples_live_peers_and_followers_do_not() {
        let mut c = Cluster::new(&[1, 2, 3], config);
        let leader = c.leader().await;
        let window = Duration::from_millis(300);
        tokio::time::sleep(window).await;
        assert_eq!(c.h(leader).sample_live(window).await, Some(vec![1, 2, 3]));
        let follower = *c.ids.iter().find(|&&i| i != leader).unwrap();
        assert_eq!(c.h(follower).sample_live(window).await, None);
        c.stop(follower);
        tokio::time::sleep(window * 2).await;
        let mut want: Vec<u64> = c.ids.iter().copied().filter(|&i| i != follower).collect();
        want.sort();
        assert_eq!(c.h(leader).sample_live(window).await, Some(want));
    }

    /// Ostro: `sample_live` covers learners as well as voters (a dead learner node must be
    /// visible to the controller). Also records what the first sample of a newly elected
    /// leader contains: `last_heard` survives role changes, so it still holds contacts from
    /// the node's follower days (a just-killed old leader appears in a wide window), and
    /// learners appear only once they answered the new leader's first heartbeat.
    #[tokio::test(flavor = "multi_thread")]
    async fn sample_live_includes_learners() {
        use raft::eraftpb::{ConfChangeSingle, ConfChangeType};

        async fn until(what: &str, mut f: impl FnMut() -> bool) {
            let deadline = Instant::now() + Duration::from_secs(30);
            while !f() {
                assert!(Instant::now() < deadline, "timed out waiting for {}", what);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
        async fn sample(h: &RaftHandle, within: Duration) -> Vec<u64> {
            h.sample_live(within).await.unwrap_or_default()
        }
        fn start_learner(c: &mut Cluster, id: u64) {
            let sm = Names::default();
            let d = RaftDriver::start(c.dirs[&id].path(), config(id), &[], sm.clone(), c.router.outbox(id)).unwrap();
            c.router.register(id, d.handle());
            c.sms.insert(id, sm);
            c.drivers.insert(id, d);
        }

        let window = Duration::from_millis(300);
        let mut c = Cluster::new(&[1, 2, 3], config);
        let leader = c.leader().await;
        c.dirs.insert(4, TempDir::new().unwrap());
        start_learner(&mut c, 4);
        let mut single = ConfChangeSingle::default();
        single.set_change_type(ConfChangeType::AddLearnerNode);
        single.node_id = 4;
        let mut cc = ConfChangeV2::default();
        cc.changes = vec![single];
        c.h(leader).propose_conf_change(cc).await.unwrap();
        // The status is published after the apply cycle that resolved the proposal.
        until("learner 4 in the leader's status", || c.h(leader).status().learners == vec![4]).await;

        // The learner answers heartbeats, so it shows up in the leader's samples.
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if sample(&c.h(leader), window).await.contains(&4) {
                break;
            }
            assert!(Instant::now() < deadline, "learner never sampled live");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(sample(&c.h(leader), window).await, vec![1, 2, 3, 4]);

        // Stopped, it drops out once `within` has passed.
        c.stop(4);
        tokio::time::sleep(window * 2).await;
        assert_eq!(sample(&c.h(leader), window).await, vec![1, 2, 3]);

        // Back, then the leader dies: the new leader's first samples.
        start_learner(&mut c, 4);
        let deadline = Instant::now() + Duration::from_secs(30);
        while !sample(&c.h(leader), window).await.contains(&4) {
            assert!(Instant::now() < deadline, "restarted learner never sampled live");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        c.stop(leader);
        let voters: Vec<u64> = c.ids.iter().copied().filter(|&i| i != leader).collect();
        let mut new = 0;
        until("a new leader", || {
            new = voters.iter().copied().find(|&i| c.h(i).status().is_leader).unwrap_or(0);
            new != 0
        })
        .await;
        let wide = sample(&c.h(new), Duration::from_secs(10)).await;
        let narrow = sample(&c.h(new), window).await;
        eprintln!("first samples on new leader {}: wide(10s) {:?}, narrow(300ms) {:?}", new, wide, narrow);
        let other = *voters.iter().find(|&&i| i != new).unwrap();
        // The surviving voter granted its vote moments ago.
        assert!(wide.contains(&new) && wide.contains(&other), "{:?}", wide);
        assert!(narrow.contains(&new) && narrow.contains(&other), "{:?}", narrow);
        // Follower-era contact: the dead old leader is still "heard" within a wide window.
        assert!(wide.contains(&leader), "{:?}", wide);
        assert!(!narrow.contains(&leader), "{:?}", narrow);
        // The learner joins the new leader's samples after answering its heartbeats.
        let deadline = Instant::now() + Duration::from_secs(30);
        while !sample(&c.h(new), window).await.contains(&4) {
            assert!(Instant::now() < deadline, "learner never sampled live by the new leader");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// A voter whose log was wiped must stop with the remedy, not abort the
    /// process (raft-rs panics when a heartbeat commits beyond its log).
    #[tokio::test(flavor = "multi_thread")]
    async fn a_wiped_voter_stops_with_the_remedy_instead_of_panicking() {
        let mut c = Cluster::new(&[1, 2, 3], config);
        let leader = c.leader().await;
        let mut last = 0;
        for i in 0..20 {
            last = create(&c.h(leader), &format!("w{}", i)).await.unwrap().index;
        }
        c.settle(last).await;
        let victim = *c.ids.iter().find(|&&i| i != leader).unwrap();
        c.stop(victim);
        c.dirs.insert(victim, TempDir::new().unwrap());
        c.start(victim);
        let mut status = c.h(victim).subscribe();
        let failed = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Some(f) = status.borrow_and_update().failed.clone() {
                    return f;
                }
                status.changed().await.unwrap();
            }
        })
        .await
        .expect("the wiped node never reported a failure");
        assert!(failed.contains("lost raft state"), "{}", failed);
        // Ostro: the remedy is Ostro's (no Chronik commands or env vars).
        assert!(failed.contains("metadata Raft log is missing or blank while it is a configured voter"), "{}", failed);
        assert!(failed.contains("restore the meta dir from backup, or wipe the whole data dir"), "{}", failed);
        assert!(failed.contains("ostro cluster remove"), "{}", failed);
        // Ostro (13-05): the real procedure, now that `ostro cluster join` exists.
        assert!(
            failed.contains(
                "Remove it with `ostro cluster remove <id> --force`, wipe its data dir, and add it back with \
                 `ostro cluster join` (it gets a new node id)."
            ),
            "{}",
            failed
        );
        assert!(!failed.contains("in Phase 13"), "{}", failed);
        assert!(!failed.contains("CHRONIK") && !failed.contains(concat!("remove", "-node")), "{}", failed);
        // The rest of the cluster keeps working.
        create(&c.h(leader), "after-wipe").await.unwrap();
    }
}
