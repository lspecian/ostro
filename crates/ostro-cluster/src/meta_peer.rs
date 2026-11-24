//! The metadata Raft group's network side (12-05, D-P12-2 / D-P12-13).
//!
//! - [`MetaPeerService`]: the `ostro.cluster.v1.MetaPeer` service. `Raft` decodes every
//!   `RaftBatch` entry into an `eraftpb::Message` and steps it into the local driver;
//!   `Propose` runs a forwarded proposal on this node (the leader; a follower answers
//!   `not_leader` with the leader hint); `ReportSuspect` hands the report to a
//!   [`SuspectSink`]. Callers are authorized by node identity only
//!   (`authz::require_node`) and must be metadata voters.
//! - [`PeerOutbox`]: the driver [`Outbox`]. One bounded queue and one sender task per
//!   peer; `send` never blocks the driver thread. A failed stream reports
//!   `report_unreachable` (and `report_snapshot_failed` when it carried a snapshot) and
//!   reconnects with exponential backoff; a full queue drops its oldest non-snapshot
//!   message (Raft retransmits). Since 13-05 a message to a node that is not yet a peer
//!   (a learner added after start) opens a queue for it on the fly.
//! - [`GrpcForwarder`]: the `ProposeForwarder` over `MetaPeer.Propose`.
//!
//! A single node has zero peers: the outbox sends nothing, and the service is still
//! servable (D-P12-13: the cluster transport is live in every mode).
//!
//! Phase 13 (13-05): the encode/decode limit defaults to 256 MiB (`[cluster]
//! meta_max_message_bytes`, Pitfall 4) so a whole metadata snapshot reaches a new learner in
//! one `MsgSnapshot`; callers are authorized as the static voters **or** any member of the
//! current Raft configuration (voters, learners, outgoing voters); and
//! [`RaftSlot::meta_lease_anchor`] exposes the metadata lease anchor (D-P13-15).
//!
//! The driver's [`RaftHandle`] exists only once the metadata group started, while the
//! service must already be serving (peers' messages elect the leader), so both reach it
//! through a late-bound [`RaftSlot`].

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use ostro_meta::handle::{ForwardFuture, ProposeForwarder};
use ostro_meta::{Applied, Outbox, ProposeError, RaftHandle, decode_command};
use ostro_proto::cluster::v1::meta_peer_client::MetaPeerClient;
use ostro_proto::cluster::v1::meta_peer_server::{MetaPeer, MetaPeerServer};
use ostro_proto::cluster::v1::{
    ProposeRequest, ProposeResponse, RaftAck, RaftBatch, SuspectAck, SuspectReport,
};
use ostro_raft_driver::raft::eraftpb::{Message, MessageType};
use prost::Message as _;
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_stream::wrappers::ReceiverStream;
use tonic::service::interceptor::InterceptedService;
use tonic::{Request, Response, Status, Streaming};

use crate::authz::require_node;
use crate::liveness::SuspectSink;
use crate::transport::ClusterTransport;
use crate::transport::tcp::PeerIdentity;

/// Default encode/decode limit of MetaPeer messages on both client and server: a Raft
/// batch may carry a whole metadata snapshot (`MsgSnapshot`). 256 MiB since 13-05
/// (`[cluster] meta_max_message_bytes`, D-P13-26; was 64 MiB).
pub const DEFAULT_META_MAX_MESSAGE: usize = 256 * 1024 * 1024;

/// Gauge: bytes of the last metadata snapshot handed to a peer stream.
pub const META_SNAPSHOT_BYTES_GAUGE: &str = "ostro_cluster_meta_snapshot_bytes";

/// `ProposeResponse.error_kind`: this node is not the leader (`leader_hint` = the leader,
/// 0 = unknown). Nothing was applied.
pub const ERROR_NOT_LEADER: &str = "not_leader";
/// `ProposeResponse.error_kind`: raft dropped the proposal. Nothing was applied.
pub const ERROR_DROPPED: &str = "dropped";
/// `ProposeResponse.error_kind`: leadership was lost before the proposal applied.
pub const ERROR_OUTCOME_UNKNOWN: &str = "outcome_unknown";
/// `ProposeResponse.error_kind`: the leader's driver stopped (outcome unknown).
pub const ERROR_STOPPED: &str = "stopped";

/// A late-bound [`RaftHandle`], set once the metadata group started. Cheap to clone.
#[derive(Clone, Default)]
pub struct RaftSlot(Arc<OnceLock<RaftHandle>>);

impl std::fmt::Debug for RaftSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RaftSlot")
            .field("bound", &self.0.get().is_some())
            .finish()
    }
}

impl RaftSlot {
    /// An empty slot.
    pub fn new() -> Self {
        Self::default()
    }

    /// Bind the handle; `false` when one was already bound (it is kept).
    pub fn set(&self, raft: RaftHandle) -> bool {
        self.0.set(raft).is_ok()
    }

    /// The handle, once bound.
    pub fn get(&self) -> Option<&RaftHandle> {
        self.0.get()
    }

    /// The metadata lease anchor (13-05, D-P13-15) of this node:
    /// `RaftHandle::lease_anchor` (the send instant of the newest lease round confirmed
    /// through a quorum-acknowledged leader; `now` on a one-voter leader; `None` before the
    /// group started, without a leader, or right after a leader change). On the meta leader
    /// it is the leader's own quorum-acked heartbeat round, on any other node a round trip
    /// through the leader. clock-exempt: monotonic lease anchor, never wall time.
    pub fn meta_lease_anchor(&self) -> Option<std::time::Instant> {
        self.0.get().and_then(RaftHandle::lease_anchor)
    }
}

impl std::fmt::Debug for MetaPeerService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetaPeerService")
            .field("node_id", &self.node_id)
            .field("voters", &self.voters)
            .field("bound", &self.raft.get().is_some())
            .finish_non_exhaustive()
    }
}

/// A [`SuspectSink`] that ignores reports (until the 12-10 controller supplies one).
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopSuspectSink;

impl SuspectSink for NoopSuspectSink {
    fn report(&self, _reporter: u64, _suspect: u64, _partition: Option<u16>) {}
}

/// The `MetaPeer` service of one node.
pub struct MetaPeerService {
    node_id: u64,
    voters: Vec<u64>,
    raft: RaftSlot,
    suspects: Arc<dyn SuspectSink>,
}

impl MetaPeerService {
    /// The service of node `node_id` in the static voter set `voters`.
    pub fn new(
        node_id: u64,
        voters: Vec<u64>,
        raft: RaftSlot,
        suspects: Arc<dyn SuspectSink>,
    ) -> Self {
        MetaPeerService {
            node_id,
            voters,
            raft,
            suspects,
        }
    }

    /// The tonic server: `identity` annotates callers (use
    /// [`crate::transport::tcp::TcpTransport::identity_interceptor`]), and `max_message`
    /// bounds encoding and decoding (normally [`DEFAULT_META_MAX_MESSAGE`]).
    pub fn into_server(
        self,
        identity: PeerIdentity,
        max_message: usize,
    ) -> InterceptedService<MetaPeerServer<MetaPeerService>, PeerIdentity> {
        InterceptedService::new(
            MetaPeerServer::new(self)
                .max_decoding_message_size(max_message)
                .max_encoding_message_size(max_message),
            identity,
        )
    }

    /// The calling node, which must be a metadata voter, or (13-05) a member of the current
    /// Raft configuration (a learner added after start, an outgoing voter).
    fn voter<T>(&self, req: &Request<T>) -> Result<u64, Status> {
        let peer = require_node(req)?;
        let member = self.raft.get().is_some_and(|raft| {
            let s = raft.status();
            s.voters.contains(&peer)
                || s.learners.contains(&peer)
                || s.voters_outgoing.contains(&peer)
                || s.learners_next.contains(&peer)
        });
        if !self.voters.contains(&peer) && !member {
            return Err(Status::permission_denied(format!(
                "node {peer} is not a metadata peer of node {} (voters {:?})",
                self.node_id, self.voters
            )));
        }
        Ok(peer)
    }
}

fn not_leader(hint: Option<u64>) -> ProposeResponse {
    ProposeResponse {
        error_kind: ERROR_NOT_LEADER.into(),
        leader_hint: hint.unwrap_or(0),
        ..ProposeResponse::default()
    }
}

#[tonic::async_trait]
impl MetaPeer for MetaPeerService {
    async fn raft(&self, req: Request<Streaming<RaftBatch>>) -> Result<Response<RaftAck>, Status> {
        let peer = self.voter(&req)?;
        if peer == self.node_id {
            return Err(Status::permission_denied(format!(
                "node {peer} may not send raft messages to itself"
            )));
        }
        let mut stream = req.into_inner();
        // Ends the stream once the local driver stopped, so the peer's outbox notices,
        // reports unreachable and reconnects (to this node once it restarted).
        let mut stopped: Option<Pin<Box<dyn Future<Output = ()> + Send>>> = None;
        loop {
            if stopped.is_none()
                && let Some(raft) = self.raft.get()
            {
                let mut rx = raft.subscribe();
                stopped = Some(Box::pin(async move { while rx.changed().await.is_ok() {} }));
            }
            let next = match stopped.as_mut() {
                Some(stop) => tokio::select! {
                    m = stream.message() => m?,
                    () = stop => return Err(Status::unavailable("metadata raft stopped")),
                },
                None => stream.message().await?,
            };
            let Some(batch) = next else {
                break;
            };
            if batch.from != peer {
                return Err(Status::permission_denied(format!(
                    "raft batch from node {} on a connection authenticated as node {peer}",
                    batch.from
                )));
            }
            // Not started yet: drop (raft retransmits once the group runs).
            let raft = self.raft.get();
            for bytes in batch.messages {
                let msg = Message::decode(bytes.as_slice())
                    .map_err(|e| Status::invalid_argument(format!("raft message: {e}")))?;
                if msg.from != peer || msg.to != self.node_id {
                    return Err(Status::permission_denied(format!(
                        "raft message {} -> {} on a connection from node {peer} to node {}",
                        msg.from, msg.to, self.node_id
                    )));
                }
                if let Some(raft) = raft {
                    raft.step(msg);
                }
            }
        }
        Ok(Response::new(RaftAck {}))
    }

    async fn propose(
        &self,
        req: Request<ProposeRequest>,
    ) -> Result<Response<ProposeResponse>, Status> {
        self.voter(&req)?;
        let command = req.into_inner().command;
        decode_command(&command)
            .map_err(|e| Status::invalid_argument(format!("meta command: {e}")))?;
        let Some(raft) = self.raft.get() else {
            return Ok(Response::new(not_leader(None)));
        };
        let status = raft.status();
        if !status.is_leader {
            return Ok(Response::new(not_leader(status.leader_id)));
        }
        let resp = match raft.propose(command).await {
            Ok(Applied {
                index,
                term,
                result,
            }) => ProposeResponse {
                index,
                term,
                result,
                ..ProposeResponse::default()
            },
            Err(ProposeError::NotLeader { leader }) => not_leader(leader),
            Err(e) => ProposeResponse {
                error_kind: match e {
                    ProposeError::Dropped => ERROR_DROPPED,
                    ProposeError::Stopped | ProposeError::Fatal(_) => ERROR_STOPPED,
                    _ => ERROR_OUTCOME_UNKNOWN,
                }
                .into(),
                leader_hint: raft.status().leader_id.unwrap_or(0),
                ..ProposeResponse::default()
            },
        };
        Ok(Response::new(resp))
    }

    async fn report_suspect(
        &self,
        req: Request<SuspectReport>,
    ) -> Result<Response<SuspectAck>, Status> {
        let peer = require_node(&req)?;
        let r = req.into_inner();
        if r.reporter != peer {
            return Err(Status::permission_denied(format!(
                "suspect report by node {} on a connection authenticated as node {peer}",
                r.reporter
            )));
        }
        let partition = r.partition.and_then(|p| u16::try_from(p).ok());
        self.suspects.report(r.reporter, r.suspect, partition);
        Ok(Response::new(SuspectAck {}))
    }
}

// ---- outbox -------------------------------------------------------------------------------

/// [`PeerOutbox`] tuning.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OutboxConfig {
    /// Messages queued per peer before the oldest non-snapshot message is dropped.
    pub queue_cap: usize,
    /// Messages per `RaftBatch`.
    pub batch_max: usize,
    /// First reconnect delay after a failed stream.
    pub backoff_min: Duration,
    /// Reconnect delay cap (doubling from `backoff_min`).
    pub backoff_max: Duration,
    /// Client encode/decode limit.
    pub max_message: usize,
}

impl Default for OutboxConfig {
    fn default() -> Self {
        OutboxConfig {
            queue_cap: 4096,
            batch_max: 256,
            backoff_min: Duration::from_millis(50),
            backoff_max: Duration::from_secs(2),
            max_message: DEFAULT_META_MAX_MESSAGE,
        }
    }
}

fn is_snapshot(m: &Message) -> bool {
    m.msg_type == MessageType::MsgSnapshot as i32
}

/// One peer's bounded queue.
#[derive(Default)]
struct PeerQueue {
    q: Mutex<VecDeque<Message>>,
    notify: Notify,
    dropped: AtomicU64,
}

impl PeerQueue {
    /// Enqueue `m`; on overflow drop the oldest non-snapshot message, or `m` itself when
    /// every queued message is a snapshot. Returns the dropped message, if any.
    fn push(&self, m: Message, cap: usize) -> Option<Message> {
        let mut q = self.q.lock().unwrap_or_else(|e| e.into_inner());
        let mut dropped = None;
        if q.len() >= cap.max(1) {
            match q.iter().position(|x| !is_snapshot(x)) {
                Some(i) => dropped = q.remove(i),
                None if !is_snapshot(&m) => {
                    drop(q);
                    self.dropped.fetch_add(1, Ordering::Relaxed);
                    return Some(m);
                }
                None => dropped = q.pop_front(),
            }
        }
        q.push_back(m);
        drop(q);
        if dropped.is_some() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
        self.notify.notify_one();
        dropped
    }

    fn drain(&self, max: usize) -> Vec<Message> {
        let mut q = self.q.lock().unwrap_or_else(|e| e.into_inner());
        let n = q.len().min(max.max(1));
        q.drain(..n).collect()
    }
}

struct Shared {
    node_id: u64,
    raft: RaftSlot,
    closed: AtomicBool,
    transport: Arc<dyn ClusterTransport>,
    cfg: OutboxConfig,
    /// Runtime for the sender tasks of peers added on the fly (13-05); `send` runs on the
    /// driver thread, outside any runtime.
    runtime: Option<tokio::runtime::Handle>,
}

impl Shared {
    fn unreachable(&self, peer: u64, snapshot: bool) {
        if let Some(raft) = self.raft.get() {
            raft.report_unreachable(peer);
            if snapshot {
                raft.report_snapshot_failed(peer);
            }
        }
    }
}

/// The metadata driver's [`Outbox`] over `MetaPeer.Raft` client streams.
pub struct PeerOutbox {
    shared: Arc<Shared>,
    peers: HashMap<u64, Arc<PeerQueue>>,
    tasks: Vec<JoinHandle<()>>,
}

impl std::fmt::Debug for PeerOutbox {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut peers: Vec<u64> = self.peers.keys().copied().collect();
        peers.sort_unstable();
        f.debug_struct("PeerOutbox")
            .field("node_id", &self.shared.node_id)
            .field("peers", &peers)
            .finish_non_exhaustive()
    }
}

impl PeerOutbox {
    /// An outbox from `node_id` to `peers` (this node is skipped), spawning one sender
    /// task per peer on the current tokio runtime. `raft` is bound once the group started
    /// (unreachable reports before that are dropped). Zero peers: sends nothing.
    pub fn new(
        node_id: u64,
        peers: &[u64],
        transport: Arc<dyn ClusterTransport>,
        raft: RaftSlot,
        cfg: OutboxConfig,
    ) -> Self {
        let shared = Arc::new(Shared {
            node_id,
            raft,
            closed: AtomicBool::new(false),
            transport,
            cfg,
            runtime: tokio::runtime::Handle::try_current().ok(),
        });
        let mut map = HashMap::new();
        let mut tasks = Vec::new();
        for &peer in peers {
            if peer == node_id || peer == 0 || map.contains_key(&peer) {
                continue;
            }
            let q = Arc::new(PeerQueue::default());
            tasks.push(tokio::spawn(run_peer(
                Arc::clone(&shared),
                peer,
                Arc::clone(&q),
            )));
            map.insert(peer, q);
        }
        PeerOutbox {
            shared,
            peers: map,
            tasks,
        }
    }

    /// Messages dropped so far because `peer`'s queue was full.
    pub fn dropped(&self, peer: u64) -> u64 {
        self.peers
            .get(&peer)
            .map_or(0, |q| q.dropped.load(Ordering::Relaxed))
    }
}

impl PeerOutbox {
    /// A queue and sender task for `peer`, a node that joined the configuration after this
    /// outbox was built (13-05: a new learner). `None` for this node, node 0, or without a
    /// runtime.
    fn add_peer(&mut self, peer: u64) -> Option<Arc<PeerQueue>> {
        if peer == self.shared.node_id || peer == 0 {
            return None;
        }
        let rt = self.shared.runtime.as_ref()?;
        let q = Arc::new(PeerQueue::default());
        self.tasks
            .push(rt.spawn(run_peer(Arc::clone(&self.shared), peer, Arc::clone(&q))));
        self.peers.insert(peer, Arc::clone(&q));
        tracing::info!(peer, "meta raft peer added to the outbox");
        Some(q)
    }
}

impl Outbox for PeerOutbox {
    fn send(&mut self, messages: Vec<Message>) {
        for m in messages {
            let to = m.to;
            if !self.peers.contains_key(&to) {
                self.add_peer(to);
            }
            match self.peers.get(&to) {
                Some(q) => {
                    if let Some(lost) = q.push(m, self.shared.cfg.queue_cap)
                        && is_snapshot(&lost)
                    {
                        self.shared.unreachable(to, true);
                    }
                }
                None => tracing::debug!(to, "meta raft message to a non-peer dropped"),
            }
        }
    }
}

impl Drop for PeerOutbox {
    fn drop(&mut self) {
        self.shared.closed.store(true, Ordering::SeqCst);
        for t in &self.tasks {
            t.abort();
        }
    }
}

/// Aborts the wrapped task when dropped.
struct AbortOnDrop<T>(JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// One peer's sender: open a `Raft` client stream, feed it batches from the queue, and on
/// failure report unreachable, back off and reconnect.
async fn run_peer(shared: Arc<Shared>, peer: u64, q: Arc<PeerQueue>) {
    let cfg = shared.cfg;
    let mut backoff = cfg.backoff_min;
    while !shared.closed.load(Ordering::SeqCst) {
        // Connect only when there is something to send.
        if q.q.lock().unwrap_or_else(|e| e.into_inner()).is_empty() {
            q.notify.notified().await;
            continue;
        }
        let started = Instant::now(); // clock-exempt: connection lifetime for the reconnect backoff, not replication time
        let mut snapshot_sent = false;
        let failure = match shared.transport.channel(peer) {
            Err(e) => e.to_string(),
            Ok(channel) => {
                let mut client = MetaPeerClient::new(channel)
                    .max_encoding_message_size(cfg.max_message)
                    .max_decoding_message_size(cfg.max_message);
                let (tx, rx) = mpsc::channel::<RaftBatch>(8);
                let mut call = AbortOnDrop(tokio::spawn(async move {
                    client.raft(ReceiverStream::new(rx)).await
                }));
                pump(&shared, &q, tx, &mut call.0, &mut snapshot_sent).await
            }
        };
        if shared.closed.load(Ordering::SeqCst) {
            return;
        }
        tracing::debug!(peer, %failure, "meta raft stream failed");
        shared.unreachable(peer, snapshot_sent);
        if started.elapsed() >= cfg.backoff_max {
            backoff = cfg.backoff_min;
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(cfg.backoff_max);
    }
}

/// Feed `tx` until the stream fails; returns why.
async fn pump(
    shared: &Shared,
    q: &PeerQueue,
    tx: mpsc::Sender<RaftBatch>,
    call: &mut JoinHandle<Result<Response<RaftAck>, Status>>,
    snapshot_sent: &mut bool,
) -> String {
    let ended = |r: Result<Result<Response<RaftAck>, Status>, tokio::task::JoinError>| match r {
        Ok(Ok(_)) => "stream closed by the peer".to_owned(),
        Ok(Err(s)) => format!("{}: {}", s.code(), s.message()),
        Err(e) => format!("stream task: {e}"),
    };
    loop {
        let batch = q.drain(shared.cfg.batch_max);
        if batch.is_empty() {
            tokio::select! {
                () = q.notify.notified() => continue,
                r = &mut *call => return ended(r),
            }
        }
        if let Some(snap) = batch.iter().find(|m| is_snapshot(m)) {
            *snapshot_sent = true;
            let bytes = snap.snapshot.as_ref().map_or(0, |s| s.data.len());
            metrics::gauge!(META_SNAPSHOT_BYTES_GAUGE).set(bytes as f64);
        }
        let rb = RaftBatch {
            from: shared.node_id,
            messages: batch.iter().map(|m| m.encode_to_vec()).collect(),
        };
        tokio::select! {
            r = tx.send(rb) => if r.is_err() {
                return "stream closed".to_owned();
            },
            r = &mut *call => return ended(r),
        }
    }
}

// ---- forwarder ----------------------------------------------------------------------------

/// [`ProposeForwarder`] over `MetaPeer.Propose`.
///
/// Mapping: success → `Applied`; `not_leader` → `NotLeader { leader: hint }` (nothing
/// applied); `dropped` → `Dropped`; `outcome_unknown` / `stopped` / unknown kinds and every
/// RPC failure (the request may have reached the leader) → `OutcomeUnknown`; an unknown
/// node (no address) → `NotLeader { leader: None }`.
pub struct GrpcForwarder {
    transport: Arc<dyn ClusterTransport>,
    max_message: usize,
    timeout: Duration,
}

impl std::fmt::Debug for GrpcForwarder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcForwarder")
            .field("max_message", &self.max_message)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

impl GrpcForwarder {
    /// A forwarder with the default message limit and a 10 s per-call timeout.
    pub fn new(transport: Arc<dyn ClusterTransport>) -> Self {
        GrpcForwarder {
            transport,
            max_message: DEFAULT_META_MAX_MESSAGE,
            timeout: Duration::from_secs(10),
        }
    }

    /// Override the message limit and per-call timeout.
    pub fn with_limits(mut self, max_message: usize, timeout: Duration) -> Self {
        self.max_message = max_message;
        self.timeout = timeout;
        self
    }

    async fn call(&self, leader: u64, cmd: Vec<u8>) -> Result<Applied, ProposeError> {
        let channel = self
            .transport
            .channel(leader)
            .map_err(|_| ProposeError::NotLeader { leader: None })?;
        let mut client = MetaPeerClient::new(channel)
            .max_encoding_message_size(self.max_message)
            .max_decoding_message_size(self.max_message);
        let mut req = Request::new(ProposeRequest { command: cmd });
        req.set_timeout(self.timeout);
        let r = match client.propose(req).await {
            Ok(r) => r.into_inner(),
            Err(s) => {
                tracing::debug!(leader, code = ?s.code(), msg = s.message(), "forwarded meta proposal failed");
                return Err(ProposeError::OutcomeUnknown);
            }
        };
        match r.error_kind.as_str() {
            "" => Ok(Applied {
                index: r.index,
                term: r.term,
                result: r.result,
            }),
            ERROR_NOT_LEADER => Err(ProposeError::NotLeader {
                leader: (r.leader_hint != 0).then_some(r.leader_hint),
            }),
            ERROR_DROPPED => Err(ProposeError::Dropped),
            _ => Err(ProposeError::OutcomeUnknown),
        }
    }
}

impl ProposeForwarder for GrpcForwarder {
    fn forward(&self, leader: u64, cmd: Vec<u8>) -> ForwardFuture<'_> {
        Box::pin(self.call(leader, cmd))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg(t: MessageType, index: u64) -> Message {
        Message {
            msg_type: t as i32,
            to: 2,
            index,
            ..Message::default()
        }
    }

    #[test]
    fn full_queue_drops_oldest_non_snapshot() {
        let q = PeerQueue::default();
        assert!(q.push(msg(MessageType::MsgSnapshot, 1), 3).is_none());
        assert!(q.push(msg(MessageType::MsgAppend, 2), 3).is_none());
        assert!(q.push(msg(MessageType::MsgAppend, 3), 3).is_none());
        let lost = q.push(msg(MessageType::MsgHeartbeat, 4), 3).unwrap();
        assert_eq!(lost.index, 2, "oldest non-snapshot message dropped");
        let left: Vec<u64> = q.drain(10).iter().map(|m| m.index).collect();
        assert_eq!(left, [1, 3, 4]);
        assert_eq!(q.dropped.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn full_queue_of_snapshots() {
        let q = PeerQueue::default();
        assert!(q.push(msg(MessageType::MsgSnapshot, 1), 1).is_none());
        // A non-snapshot message is dropped rather than a snapshot.
        assert_eq!(q.push(msg(MessageType::MsgAppend, 2), 1).unwrap().index, 2);
        // A newer snapshot replaces the older one.
        assert_eq!(
            q.push(msg(MessageType::MsgSnapshot, 3), 1).unwrap().index,
            1
        );
        assert_eq!(q.drain(10).iter().map(|m| m.index).collect::<Vec<_>>(), [3]);
        assert_eq!(q.drain(10).len(), 0);
    }

    /// `ostro_meta::sm::required_level` mirrors the compat registry (ostro-meta cannot
    /// depend on ostro-types).
    #[test]
    fn meta_required_level_matches_the_compat_registry() {
        use ostro_types::compat::{Feature, feature_for_meta_variant};
        let alter = ostro_meta::MetaCommand::AlterReplicas {
            partition: 0,
            epoch: 1,
            expected_isr_version: 0,
            replicas: vec![1],
            isr: vec![1],
            min_insync: 1,
            reassignment: None,
        };
        let bytes = ostro_meta::encode_command(&alter);
        // `[META_FORMAT] ‖ varint(variant)`: AlterReplicas is variant 8.
        assert_eq!(bytes[1], 8);
        assert_eq!(feature_for_meta_variant(8), Some(Feature::AlterReplicas));
        assert_eq!(
            ostro_meta::sm::required_level(&alter),
            Feature::AlterReplicas.introduced_at()
        );
        for v in 0..8 {
            assert_eq!(feature_for_meta_variant(v), None, "variant {v}");
        }
        let put = ostro_meta::MetaCommand::Delete {
            key: ostro_meta::MetaKey::Queue("q".into()),
            expected_version: 1,
        };
        assert_eq!(ostro_meta::sm::required_level(&put), 1);
    }

    #[test]
    fn drain_respects_batch_max() {
        let q = PeerQueue::default();
        for i in 0..5 {
            q.push(msg(MessageType::MsgAppend, i), 100);
        }
        assert_eq!(q.drain(2).len(), 2);
        assert_eq!(q.drain(10).len(), 3);
    }
}
