//! The worker registry (plan 07-09, WORK-01): connected workers and their sessions (one
//! per worker and node; a renewing worker briefly holds two), labels, absolute credit,
//! the remote jobs attached to each session, lease renewal and abandonment.
//!
//! - **Credit.** A worker reports its free slots absolutely (`Credit`, `Heartbeat`). The
//!   assignable credit of a session is `min(reported, slots - attached) - outstanding`,
//!   where `outstanding` counts [`CreditGuard`]s handed out by [`WorkerRegistry::pick`] and
//!   not yet attached or dropped. A draining session has none.
//! - **Jobs.** [`WorkerRegistry::attach_job`] turns a credit into a [`RemoteJobHandle`]:
//!   the handle sends `ServerMsg`s to the job's current session and yields
//!   [`RemoteEvent`]s (`Done`, `GuestLogs`, `CancelEffect`, `Abandoned`); `Append` and
//!   `StartEffect` RPCs reach it through [`RemoteJobHandle::rpcs`]. Dropping the handle
//!   detaches the job and returns its slot.
//! - **Leases.** Each `Heartbeat` lease `(run, token)` renews the matching attached job's
//!   dispatcher lease ([`LeaseRenewer::renew`]); a successful `Append` renews too (the
//!   service calls [`WorkerRegistry::renew_job`]). A lease the session does not hold is
//!   answered with `LeaseLost`, except a lease held by another session of the same worker:
//!   that is a certificate renewal (07-07 moves activations to the new session and lists
//!   them in its first heartbeat), so the job moves to the new session.
//! - **Abandonment.** When a session ends, every job still attached to it gets
//!   [`RemoteEvent::Abandoned`] at once (the dispatcher maps it to
//!   `ExecOutcome::Abandoned`), except after a worker `Drain` with a live sibling session of
//!   the same worker, where the jobs move to the sibling (renewal, never revoked).
//! - **Wake-ups.** [`WorkerRegistry::changed`] bumps on every credit change (session open
//!   and close, credit, heartbeat, attach, detach, guard drop, drain): the source of
//!   `Placement::changed` for 07-12.
//! - **Versions (13-14, D-P13-18).** Each session records its [`WorkerVersion`] (feature
//!   level, WIT lines, event codec) from `Hello`; [`WorkerView::version`] carries it to the
//!   placement predicate ([`crate::workers::placement::compatible`]).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use ostro_engine::host::linker_set::{OSTRO_PACKAGE, WitLine, parse_version};
use ostro_log::PartitionLog;
use ostro_partition::{LeaseRenewer, PartitionHandle};
use ostro_types::{AttemptToken, Epoch, RunId};
use ostro_worker_proto::v1;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;
use tonic::Status;

use crate::config::WorkersSection;

/// A session id (16 random bytes, `Welcome.session_id`).
pub type SessionId = Vec<u8>;

/// Server -> worker stream items.
pub type SessionTx = mpsc::UnboundedSender<Result<v1::ServerMsg, Status>>;

/// Who a session belongs to (from its client certificate).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PeerIdentity {
    pub cluster_id: String,
    /// Join token id of a join-issued certificate; `None` = external CA (D-W25).
    pub token_id: Option<String>,
    pub worker_id: String,
}

impl PeerIdentity {
    /// A certificate from `[cluster.tls] client_ca` (`ostro://<cluster>/worker/ext/<id>`).
    pub fn is_external(&self) -> bool {
        self.token_id.is_none()
    }
}

/// A connected session, as listed by [`WorkerRegistry::sessions`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerInfo {
    pub session_id: SessionId,
    pub worker_id: String,
    pub token_id: Option<String>,
    pub labels: BTreeMap<String, String>,
    /// `Hello.slots`.
    pub slots: u32,
    /// Assignable credit now.
    pub free: u32,
    /// Attached jobs.
    pub leases: u32,
    pub memory_bytes: u64,
    /// Injected-clock ms at `Welcome`.
    pub connected_at_ms: u64,
    pub draining: bool,
}

/// What [`WorkerRegistry::pick`]'s predicate sees of a candidate session.
#[derive(Debug)]
pub struct WorkerView<'a> {
    pub worker_id: &'a str,
    pub labels: &'a BTreeMap<String, String>,
    pub free: u32,
    /// The session's advertised versions (13-14).
    pub version: &'a WorkerVersion,
}

/// What a worker session can run (13-14, D-P13-18), from its `Hello`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WorkerVersion {
    /// `Hello.feature_level`, or (0 = an older worker) the level of `Hello.ostro_version`
    /// ([`level_of_ostro_version`]); 0 when neither gives one.
    pub level: u32,
    /// WIT major lines from `Hello.wit_packages` (`ostro:runtime@x.y.z` entries).
    pub wit_lines: Vec<WitLine>,
    /// `Hello.event_codec_ver`.
    pub event_codec_ver: u32,
}

impl WorkerVersion {
    /// The versions `hello` advertises; `Err` (the GoAway reason) when the feature level is
    /// absent and `ostro_version` names no level.
    pub fn from_hello(hello: &v1::Hello) -> Result<Self, String> {
        let v = Self::lenient(hello);
        if v.level == 0 {
            return Err(format!(
                "version mismatch: Hello carries no feature_level and ostro_version {:?} \
                 names no feature level",
                hello.ostro_version
            ));
        }
        Ok(v)
    }

    /// [`Self::from_hello`] with level 0 (never compatible) instead of an error.
    pub fn lenient(hello: &v1::Hello) -> Self {
        let level = if hello.feature_level != 0 {
            hello.feature_level
        } else {
            level_of_ostro_version(&hello.ostro_version).unwrap_or(0)
        };
        let mut wit_lines: Vec<WitLine> = hello
            .wit_packages
            .iter()
            .filter_map(|p| {
                let (pkg, ver) = p.split_once('@')?;
                if pkg == OSTRO_PACKAGE {
                    WitLine::of_version(ver)
                } else {
                    None
                }
            })
            .collect();
        wit_lines.sort();
        wit_lines.dedup();
        Self {
            level,
            wit_lines,
            event_codec_ver: hello.event_codec_ver,
        }
    }
}

/// The feature level of a worker that predates `Hello.feature_level` (D-P13-18), from its
/// `ostro_version` (`x.y.z`, an optional `-pre` / `+build` suffix ignored): every `0.1.z`
/// release before Phase 13 is the baseline level `LEVEL_MIN`. `0.0.z` and unparsable
/// versions name no level (the session is refused). A later minor always sends
/// `feature_level`; one that does not is assumed to be at the baseline.
pub fn level_of_ostro_version(v: &str) -> Option<u32> {
    let core = v.split(['-', '+']).next().unwrap_or(v);
    let (major, minor, _) = parse_version(core)?;
    match (major, minor) {
        (0, 0) => None,
        _ => Some(ostro_types::compat::LEVEL_MIN),
    }
}

/// Something that happened to a remote job.
#[derive(Debug)]
pub enum RemoteEvent {
    Done(v1::Done),
    GuestLogs(v1::GuestLogs),
    CancelEffect(v1::CancelEffect),
    /// The session died (stream loss, revocation, shutdown): the worker self-fenced.
    Abandoned {
        reason: String,
    },
}

/// One reassembled `Append` batch: event `i` has seq `from_seq + i`; each event is
/// `(EventKind code, encode_event bytes)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendBatch {
    pub from_seq: u64,
    pub events: Vec<(u32, Vec<u8>)>,
}

/// An RPC routed to the job's handler (07-12).
#[derive(Debug)]
pub enum JobRpc {
    Append {
        batch: AppendBatch,
        reply: oneshot::Sender<Result<v1::AppendResult, Status>>,
    },
    StartEffect {
        req: v1::StartEffectRequest,
        reply: oneshot::Sender<Result<v1::StartEffectResponse, Status>>,
    },
}

/// What a remote job is attached with.
pub struct AttachSpec {
    pub run: RunId,
    pub token: AttemptToken,
    pub lease: LeaseRenewer,
    pub handle: PartitionHandle<PartitionLog>,
    /// The run's module (`RunRecord.module_sha`): `FetchBlob` may serve it.
    pub module_sha: Option<[u8; 32]>,
}

/// Why `attach_job` failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AttachError {
    #[error("the worker session is gone")]
    SessionGone,
    #[error("the session already holds a job for this run")]
    Duplicate,
}

/// Why a session was refused at open.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OpenRefused {
    #[error("external worker {0} already holds a session")]
    ExternalBusy(String),
}

/// Session controls handed to the service task.
#[derive(Clone, Debug)]
pub struct SessionCtl {
    pub id: SessionId,
    /// Cancelled when the registry closes the session ([`WorkerRegistry::close_where`]).
    pub close: CancellationToken,
    reason: Arc<Mutex<Option<String>>>,
}

impl SessionCtl {
    /// Why the registry closed the session (`GoAway.reason`).
    pub fn reason(&self) -> Option<String> {
        lock(&self.reason).clone()
    }
}

/// Lease-scoped access of a bulk RPC (see [`WorkerRegistry::authorize`]).
#[derive(Clone)]
pub struct JobAccess {
    pub job_id: u64,
    pub run: RunId,
    pub token: AttemptToken,
    pub handle: PartitionHandle<PartitionLog>,
    pub module_sha: Option<[u8; 32]>,
}

impl std::fmt::Debug for JobAccess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobAccess")
            .field("job_id", &self.job_id)
            .field("run", &self.run)
            .field("token", &self.token)
            .finish_non_exhaustive()
    }
}

/// Why a bulk RPC was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AuthzError {
    #[error("unknown session")]
    NoSession,
    #[error("the session belongs to another identity")]
    NotOwner,
    #[error("the session holds no lease for this run and token")]
    NoLease,
}

struct Session {
    peer: PeerIdentity,
    version: WorkerVersion,
    labels: BTreeMap<String, String>,
    slots: u32,
    memory_bytes: u64,
    connected_at_ms: u64,
    reported_free: u32,
    outstanding: u32,
    draining: bool,
    /// The worker sent `Drain` (renewal or shutdown).
    worker_drained: bool,
    tx: SessionTx,
    ctl: SessionCtl,
    jobs: HashSet<u64>,
    last_assigned: u64,
}

impl Session {
    fn free(&self) -> u32 {
        if self.draining {
            return 0;
        }
        let cap = self
            .slots
            .saturating_sub(u32::try_from(self.jobs.len()).unwrap_or(u32::MAX));
        self.reported_free.min(cap).saturating_sub(self.outstanding)
    }

    fn send(&self, msg: v1::server_msg::Msg) -> bool {
        self.tx.send(Ok(v1::ServerMsg { msg: Some(msg) })).is_ok()
    }

    fn info(&self, id: &SessionId) -> WorkerInfo {
        WorkerInfo {
            session_id: id.clone(),
            worker_id: self.peer.worker_id.clone(),
            token_id: self.peer.token_id.clone(),
            labels: self.labels.clone(),
            slots: self.slots,
            free: self.free(),
            leases: u32::try_from(self.jobs.len()).unwrap_or(u32::MAX),
            memory_bytes: self.memory_bytes,
            connected_at_ms: self.connected_at_ms,
            draining: self.draining,
        }
    }
}

struct JobSlot {
    /// `None` once abandoned.
    session: Option<SessionId>,
    run: RunId,
    token: AttemptToken,
    lease: LeaseRenewer,
    handle: PartitionHandle<PartitionLog>,
    module_sha: Option<[u8; 32]>,
    /// Blob refs served to the worker (sha -> len).
    blobs: HashMap<[u8; 32], u64>,
    events: mpsc::UnboundedSender<RemoteEvent>,
    rpc: Option<mpsc::UnboundedSender<JobRpc>>,
}

#[derive(Default)]
struct Inner {
    sessions: HashMap<SessionId, Session>,
    jobs: HashMap<u64, JobSlot>,
    next_job: u64,
    assign_seq: u64,
}

impl Inner {
    fn totals(&self) -> (usize, u64) {
        (
            self.sessions.len(),
            self.sessions.values().map(|s| u64::from(s.free())).sum(),
        )
    }
}

struct Shared {
    state: Mutex<Inner>,
    changed: watch::Sender<u64>,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Bump `changed` and publish the gauges (after the lock is released).
    fn bump(&self) {
        let (connected, free) = self.lock().totals();
        metrics::gauge!("ostro_workers_connected").set(connected as f64);
        metrics::gauge!("ostro_worker_slots_free").set(free as f64);
        self.changed.send_modify(|v| *v = v.wrapping_add(1));
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

fn token_of(t: Option<&v1::Token>) -> Option<AttemptToken> {
    t.map(|t| AttemptToken {
        epoch: Epoch(t.epoch),
        n: t.n,
    })
}

/// The wire form of a token.
pub fn wire_token(t: AttemptToken) -> v1::Token {
    v1::Token {
        epoch: t.epoch.0,
        n: t.n,
    }
}

/// The wire form of a run.
pub fn wire_run(run: &RunId) -> v1::RunRef {
    v1::RunRef {
        run_id: run.as_bytes().to_vec(),
        partition: u32::from(run.partition().0),
    }
}

/// Connected workers of this node.
pub struct WorkerRegistry {
    shared: Arc<Shared>,
    max_frame_bytes: u64,
}

impl std::fmt::Debug for WorkerRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (connected, free) = self.shared.lock().totals();
        f.debug_struct("WorkerRegistry")
            .field("connected", &connected)
            .field("slots_free", &free)
            .finish_non_exhaustive()
    }
}

impl WorkerRegistry {
    /// An empty registry configured from `[workers]`.
    pub fn new(cfg: &WorkersSection) -> Self {
        Self {
            shared: Arc::new(Shared {
                state: Mutex::new(Inner::default()),
                changed: watch::channel(0).0,
            }),
            max_frame_bytes: cfg.max_frame_bytes,
        }
    }

    /// Connected sessions.
    pub fn connected(&self) -> usize {
        self.shared.lock().sessions.len()
    }

    /// Assignable worker credit over every session.
    pub fn slots_free(&self) -> u64 {
        self.shared.lock().totals().1
    }

    /// `[workers] max_frame_bytes`.
    pub fn max_frame_bytes(&self) -> u64 {
        self.max_frame_bytes
    }

    /// Bumped whenever worker credit may have appeared or disappeared.
    pub fn changed(&self) -> watch::Receiver<u64> {
        self.shared.changed.subscribe()
    }

    /// Every session, sorted by (worker id, connected at).
    pub fn sessions(&self) -> Vec<WorkerInfo> {
        let g = self.shared.lock();
        let mut v: Vec<WorkerInfo> = g.sessions.iter().map(|(id, s)| s.info(id)).collect();
        v.sort_by(|a, b| {
            (a.worker_id.as_str(), a.connected_at_ms, &a.session_id).cmp(&(
                b.worker_id.as_str(),
                b.connected_at_ms,
                &b.session_id,
            ))
        });
        v
    }

    /// The identity owning `session`.
    pub fn session_peer(&self, session: &[u8]) -> Option<PeerIdentity> {
        self.shared
            .lock()
            .sessions
            .get(session)
            .map(|s| s.peer.clone())
    }

    /// Whether `peer` holds at least one live session.
    pub fn has_session(&self, peer: &PeerIdentity) -> bool {
        self.shared
            .lock()
            .sessions
            .values()
            .any(|s| &s.peer == peer)
    }

    // ------------------------------------------------------------------ session lifecycle

    /// Register a session after a valid `Hello`.
    pub fn open_session(
        &self,
        peer: PeerIdentity,
        hello: &v1::Hello,
        now_ms: u64,
        tx: SessionTx,
    ) -> Result<SessionCtl, OpenRefused> {
        let mut id = vec![0u8; 16];
        if getrandom::fill(&mut id).is_err() {
            // Unique per process is enough for a session id; fall back to a counter mix.
            let n = now_ms.to_be_bytes();
            id[..8].copy_from_slice(&n);
        }
        let ctl = SessionCtl {
            id: id.clone(),
            close: CancellationToken::new(),
            reason: Arc::new(Mutex::new(None)),
        };
        {
            let mut g = self.shared.lock();
            if peer.is_external()
                && g.sessions
                    .values()
                    .any(|s| s.peer == peer && !s.ctl.close.is_cancelled())
            {
                return Err(OpenRefused::ExternalBusy(peer.worker_id));
            }
            let seq = g.assign_seq;
            g.sessions.insert(
                id,
                Session {
                    peer,
                    // The service refuses a Hello without a level before opening; a
                    // direct caller's unparsable Hello gets level 0 (never compatible).
                    version: WorkerVersion::lenient(hello),
                    labels: hello
                        .labels
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect(),
                    slots: hello.slots,
                    memory_bytes: hello.memory_bytes,
                    connected_at_ms: now_ms,
                    reported_free: 0,
                    outstanding: 0,
                    draining: false,
                    worker_drained: false,
                    tx,
                    ctl: ctl.clone(),
                    jobs: HashSet::new(),
                    last_assigned: seq,
                },
            );
        }
        self.shared.bump();
        Ok(ctl)
    }

    /// The session ended: abandon (or, after a worker `Drain`, hand to a live sibling
    /// session of the same worker) every attached job.
    pub fn close_session(&self, id: &[u8], reason: &str) {
        let mut abandoned = Vec::new();
        {
            let mut g = self.shared.lock();
            let Some(s) = g.sessions.remove(id) else {
                return;
            };
            s.ctl.close.cancel();
            let sibling = if s.worker_drained {
                g.sessions
                    .iter()
                    .filter(|(_, o)| o.peer.worker_id == s.peer.worker_id && !o.worker_drained)
                    .map(|(sid, _)| sid.clone())
                    .next()
            } else {
                None
            };
            for job in s.jobs {
                match &sibling {
                    Some(sid) => {
                        if let Some(slot) = g.jobs.get_mut(&job) {
                            slot.session = Some(sid.clone());
                        }
                        if let Some(o) = g.sessions.get_mut(sid) {
                            o.jobs.insert(job);
                        }
                    }
                    None => {
                        if let Some(slot) = g.jobs.get_mut(&job) {
                            slot.session = None;
                            abandoned.push(slot.events.clone());
                        }
                    }
                }
            }
        }
        for tx in abandoned {
            let _ = tx.send(RemoteEvent::Abandoned {
                reason: reason.to_owned(),
            });
        }
        self.shared.bump();
    }

    /// Close every session matching `pred` with `GoAway{reason}`; returns how many.
    pub fn close_where(&self, reason: &str, pred: impl Fn(&PeerIdentity) -> bool) -> usize {
        let g = self.shared.lock();
        let mut n = 0;
        for s in g.sessions.values().filter(|s| pred(&s.peer)) {
            *lock(&s.ctl.reason) = Some(reason.to_owned());
            s.ctl.close.cancel();
            n += 1;
        }
        n
    }

    /// Close every session whose advertised versions match `pred` with `GoAway{reason}`
    /// (13-14: sessions too old for the finalized level); returns how many.
    pub fn close_where_version(
        &self,
        reason: &str,
        pred: impl Fn(&WorkerVersion) -> bool,
    ) -> usize {
        let g = self.shared.lock();
        let mut n = 0;
        for s in g
            .sessions
            .values()
            .filter(|s| pred(&s.version) && !s.ctl.close.is_cancelled())
        {
            *lock(&s.ctl.reason) = Some(reason.to_owned());
            s.ctl.close.cancel();
            n += 1;
        }
        n
    }

    /// The advertised versions of `session`.
    pub fn session_version(&self, session: &[u8]) -> Option<WorkerVersion> {
        self.shared
            .lock()
            .sessions
            .get(session)
            .map(|s| s.version.clone())
    }

    /// Close the sessions whose certificate carries join token `token_id`.
    pub fn close_token(&self, token_id: &str) -> usize {
        self.close_where("token revoked", |p| p.token_id.as_deref() == Some(token_id))
    }

    /// Stop assigning to `worker_id`'s sessions. `None` = unknown worker; `Some(true)` =
    /// already draining. There is no server -> worker `Drain` message in
    /// `ostro.worker.v1` (adding a `ServerMsg` variant would break 07-07's exhaustive
    /// match), so in-flight work finishes normally.
    pub fn drain(&self, worker_id: &str) -> Option<bool> {
        let r = {
            let mut g = self.shared.lock();
            let mut found = false;
            let mut already = true;
            for s in g
                .sessions
                .values_mut()
                .filter(|s| s.peer.worker_id == worker_id)
            {
                found = true;
                already &= s.draining;
                s.draining = true;
            }
            found.then_some(already)
        };
        if r.is_some() {
            self.shared.bump();
        }
        r
    }

    // ------------------------------------------------------------------ worker messages

    /// `Credit{free_slots}` (absolute).
    pub fn credit(&self, id: &[u8], free: u32) {
        let changed = {
            let mut g = self.shared.lock();
            match g.sessions.get_mut(id) {
                Some(s) if s.reported_free != free => {
                    s.reported_free = free;
                    true
                }
                _ => false,
            }
        };
        if changed {
            self.shared.bump();
        }
    }

    /// The worker sent `Drain` on this session: no new work; its jobs stay leased (a
    /// renewing worker lists them in the new session's heartbeat).
    pub fn worker_drain(&self, id: &[u8]) {
        {
            let mut g = self.shared.lock();
            if let Some(s) = g.sessions.get_mut(id) {
                s.draining = true;
                s.worker_drained = true;
            }
        }
        self.shared.bump();
    }

    /// `Heartbeat`: credit, then renew every listed lease (see the module docs). Returns
    /// the number of leases renewed.
    pub fn heartbeat(&self, id: &[u8], hb: &v1::Heartbeat) -> usize {
        let mut renewed = 0;
        {
            let mut g = self.shared.lock();
            let Some(worker_id) = g.sessions.get(id).map(|s| s.peer.worker_id.clone()) else {
                return 0;
            };
            if let Some(s) = g.sessions.get_mut(id) {
                s.reported_free = hb.free_slots;
            }
            for l in &hb.leases {
                let run = l
                    .run
                    .as_ref()
                    .and_then(|r| RunId::from_bytes(&r.run_id).ok());
                let token = token_of(l.token.as_ref());
                let (Some(run), Some(token)) = (run, token) else {
                    continue;
                };
                // Held by a session of this worker (this one, or a renewal sibling)?
                let found = g.jobs.iter().find_map(|(jid, slot)| {
                    let sid = slot.session.as_ref()?;
                    (slot.run == run && slot.token == token).then(|| (*jid, sid.clone()))
                });
                let ok = match found {
                    Some((jid, sid)) if sid.as_slice() == id => {
                        g.jobs.get(&jid).is_some_and(|slot| slot.lease.renew())
                    }
                    Some((jid, sid))
                        if g.sessions
                            .get(&sid)
                            .is_some_and(|o| o.peer.worker_id == worker_id) =>
                    {
                        // Renewal hand-off: move the job to this session.
                        if let Some(o) = g.sessions.get_mut(&sid) {
                            o.jobs.remove(&jid);
                        }
                        if let Some(s) = g.sessions.get_mut(id) {
                            s.jobs.insert(jid);
                        }
                        match g.jobs.get_mut(&jid) {
                            Some(slot) => {
                                slot.session = Some(id.to_vec());
                                slot.lease.renew()
                            }
                            None => false,
                        }
                    }
                    _ => false,
                };
                if ok {
                    renewed += 1;
                } else if let Some(s) = g.sessions.get(id) {
                    s.send(v1::server_msg::Msg::LeaseLost(v1::LeaseLost {
                        run: l.run.clone(),
                        token: l.token,
                    }));
                }
            }
        }
        self.shared.bump();
        renewed
    }

    /// Route a job message (`Done`, `GuestLogs`, `CancelEffect`) of session `id`. Returns
    /// `false` when the session holds no such job.
    pub fn job_event(
        &self,
        id: &[u8],
        run: Option<&v1::RunRef>,
        token: Option<&v1::Token>,
        ev: RemoteEvent,
    ) -> bool {
        let run = run.and_then(|r| RunId::from_bytes(&r.run_id).ok());
        let (Some(run), Some(token)) = (run, token_of(token)) else {
            return false;
        };
        let tx = {
            let g = self.shared.lock();
            g.jobs
                .values()
                .find(|s| s.session.as_deref() == Some(id) && s.run == run && s.token == token)
                .map(|s| s.events.clone())
        };
        tx.is_some_and(|tx| tx.send(ev).is_ok())
    }

    // ------------------------------------------------------------------ placement API

    /// The matching session with the most assignable credit (ties: least recently
    /// assigned) as a [`CreditGuard`]; `None` when no session matches or has credit.
    pub fn pick(&self, pred: impl Fn(&WorkerView<'_>) -> bool) -> Option<CreditGuard> {
        let guard = {
            let mut g = self.shared.lock();
            let best = g
                .sessions
                .iter()
                .filter_map(|(id, s)| {
                    let free = s.free();
                    let view = WorkerView {
                        worker_id: &s.peer.worker_id,
                        labels: &s.labels,
                        free,
                        version: &s.version,
                    };
                    (free > 0 && pred(&view)).then(|| (free, s.last_assigned, id.clone()))
                })
                .max_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)))
                .map(|(_, _, id)| id)?;
            g.assign_seq += 1;
            let seq = g.assign_seq;
            let s = g.sessions.get_mut(&best)?;
            s.outstanding += 1;
            s.last_assigned = seq;
            CreditGuard {
                shared: Arc::clone(&self.shared),
                session: best,
                worker_id: s.peer.worker_id.clone(),
                armed: true,
            }
        };
        self.shared.bump();
        Some(guard)
    }

    /// Attach a job to the credit's session.
    pub fn attach_job(
        &self,
        mut credit: CreditGuard,
        spec: AttachSpec,
    ) -> Result<RemoteJobHandle, AttachError> {
        let (tx, rx) = mpsc::unbounded_channel();
        let job_id = {
            let mut g = self.shared.lock();
            if let Some(s) = g.sessions.get_mut(&credit.session) {
                s.outstanding = s.outstanding.saturating_sub(1);
            }
            credit.armed = false;
            if !g.sessions.contains_key(&credit.session) {
                drop(g);
                self.shared.bump();
                return Err(AttachError::SessionGone);
            }
            let dup = g.jobs.values().any(|j| {
                j.session.as_deref() == Some(credit.session.as_slice()) && j.run == spec.run
            });
            if dup {
                drop(g);
                self.shared.bump();
                return Err(AttachError::Duplicate);
            }
            g.next_job += 1;
            let job_id = g.next_job;
            g.jobs.insert(
                job_id,
                JobSlot {
                    session: Some(credit.session.clone()),
                    run: spec.run,
                    token: spec.token,
                    lease: spec.lease,
                    handle: spec.handle,
                    module_sha: spec.module_sha,
                    blobs: HashMap::new(),
                    events: tx,
                    rpc: None,
                },
            );
            if let Some(s) = g.sessions.get_mut(&credit.session) {
                s.jobs.insert(job_id);
            }
            job_id
        };
        self.shared.bump();
        Ok(RemoteJobHandle {
            shared: Arc::clone(&self.shared),
            job_id,
            run: spec.run,
            token: spec.token,
            worker_id: credit.worker_id.clone(),
            events: rx,
        })
    }

    // ------------------------------------------------------------------ bulk RPC access

    /// Lease-scoped authorization (W6): `peer` must own `session` and the session must
    /// hold a job for `(run, token)`.
    pub fn authorize(
        &self,
        session: &[u8],
        peer: &PeerIdentity,
        run: Option<&v1::RunRef>,
        token: Option<&v1::Token>,
    ) -> Result<JobAccess, AuthzError> {
        let g = self.shared.lock();
        let s = g.sessions.get(session).ok_or(AuthzError::NoSession)?;
        if &s.peer != peer {
            return Err(AuthzError::NotOwner);
        }
        let run = run
            .and_then(|r| RunId::from_bytes(&r.run_id).ok())
            .ok_or(AuthzError::NoLease)?;
        let token = token_of(token).ok_or(AuthzError::NoLease)?;
        s.jobs
            .iter()
            .filter_map(|jid| g.jobs.get(jid).map(|j| (*jid, j)))
            .find(|(_, j)| j.run == run && j.token == token)
            .map(|(job_id, j)| JobAccess {
                job_id,
                run,
                token,
                handle: j.handle.clone(),
                module_sha: j.module_sha,
            })
            .ok_or(AuthzError::NoLease)
    }

    /// Remember a blob ref served to the job (`FetchBlob` may then serve it).
    pub fn note_blob(&self, job_id: u64, sha: [u8; 32], len: u64) {
        if let Some(j) = self.shared.lock().jobs.get_mut(&job_id) {
            j.blobs.insert(sha, len);
        }
    }

    /// The length of a blob ref already served to the job.
    pub fn known_blob(&self, job_id: u64, sha: &[u8; 32]) -> Option<u64> {
        self.shared
            .lock()
            .jobs
            .get(&job_id)
            .and_then(|j| j.blobs.get(sha).copied())
    }

    /// The job's RPC handler, if one is registered ([`RemoteJobHandle::rpcs`]).
    pub fn rpc_handler(&self, job_id: u64) -> Option<mpsc::UnboundedSender<JobRpc>> {
        self.shared
            .lock()
            .jobs
            .get(&job_id)
            .and_then(|j| j.rpc.clone())
            .filter(|tx| !tx.is_closed())
    }

    /// Renew the job's lease (a successful `Append`); `false` once revoked.
    pub fn renew_job(&self, job_id: u64) -> bool {
        self.shared
            .lock()
            .jobs
            .get(&job_id)
            .is_some_and(|j| j.lease.renew())
    }
}

/// One unit of worker credit; dropping it returns the credit.
pub struct CreditGuard {
    shared: Arc<Shared>,
    session: SessionId,
    worker_id: String,
    armed: bool,
}

impl CreditGuard {
    pub fn session_id(&self) -> &[u8] {
        &self.session
    }

    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }
}

impl std::fmt::Debug for CreditGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CreditGuard")
            .field("worker_id", &self.worker_id)
            .finish_non_exhaustive()
    }
}

impl Drop for CreditGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if let Some(s) = self.shared.lock().sessions.get_mut(&self.session) {
            s.outstanding = s.outstanding.saturating_sub(1);
        }
        self.shared.bump();
    }
}

/// A job attached to a worker session; dropping it detaches the job.
pub struct RemoteJobHandle {
    shared: Arc<Shared>,
    job_id: u64,
    run: RunId,
    token: AttemptToken,
    worker_id: String,
    events: mpsc::UnboundedReceiver<RemoteEvent>,
}

impl std::fmt::Debug for RemoteJobHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteJobHandle")
            .field("job_id", &self.job_id)
            .field("run", &self.run)
            .field("worker_id", &self.worker_id)
            .finish_non_exhaustive()
    }
}

impl RemoteJobHandle {
    pub fn job_id(&self) -> u64 {
        self.job_id
    }

    pub fn run(&self) -> RunId {
        self.run
    }

    pub fn token(&self) -> AttemptToken {
        self.token
    }

    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    /// The session currently holding the job (`None` once abandoned).
    pub fn session_id(&self) -> Option<SessionId> {
        self.shared
            .lock()
            .jobs
            .get(&self.job_id)
            .and_then(|j| j.session.clone())
    }

    /// Send a message to the job's current session; `false` if it is gone.
    pub fn send(&self, msg: v1::server_msg::Msg) -> bool {
        let g = self.shared.lock();
        g.jobs
            .get(&self.job_id)
            .and_then(|j| j.session.as_ref())
            .and_then(|sid| g.sessions.get(sid))
            .is_some_and(|s| s.send(msg))
    }

    /// The next event (`None` only after the registry dropped the job, which it never does
    /// while the handle lives).
    pub async fn next_event(&mut self) -> Option<RemoteEvent> {
        self.events.recv().await
    }

    /// A pending event, without waiting.
    pub fn try_event(&mut self) -> Option<RemoteEvent> {
        self.events.try_recv().ok()
    }

    /// Register this job's `Append` / `StartEffect` handler (replaces an earlier one).
    /// Until one is registered those RPCs answer `UNIMPLEMENTED`.
    pub fn rpcs(&self) -> mpsc::UnboundedReceiver<JobRpc> {
        let (tx, rx) = mpsc::unbounded_channel();
        if let Some(j) = self.shared.lock().jobs.get_mut(&self.job_id) {
            j.rpc = Some(tx);
        }
        rx
    }

    /// Renew the job's lease; `false` once revoked.
    pub fn renew(&self) -> bool {
        self.shared
            .lock()
            .jobs
            .get(&self.job_id)
            .is_some_and(|j| j.lease.renew())
    }
}

impl Drop for RemoteJobHandle {
    fn drop(&mut self) {
        {
            let mut g = self.shared.lock();
            if let Some(slot) = g.jobs.remove(&self.job_id)
                && let Some(sid) = slot.session
                && let Some(s) = g.sessions.get_mut(&sid)
            {
                s.jobs.remove(&self.job_id);
            }
        }
        self.shared.bump();
    }
}
