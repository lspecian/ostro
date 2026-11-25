//! [`MetaHandle`]: starts the metadata Raft group (a lone voter, or a static 3/5-voter
//! group since Phase 12, D-P12-2) and offers propose / read / watch with read-your-writes
//! (META-01).
//!
//! A multi-voter group is wired by the caller through [`MetaStart`]: the [`Outbox`] that
//! carries Raft messages to the peers and a [`ProposeForwarder`] that runs a proposal on
//! the current leader (ostro-cluster implements both over `MetaPeer`; ostro-meta stays
//! transport-free).
//!
//! Phase 13 (13-05) adds the membership and consistency surface: [`MetaHandle::read_index_barrier`]
//! (linearizable meta reads), [`MetaHandle::transfer_leader`], [`MetaHandle::propose_conf_change`],
//! [`MetaHandle::conf_state`], [`MetaHandle::raft_detail`] (the full driver status with
//! per-peer progress), [`MetaHandle::lease_anchor`], [`MetaHandle::start_joiner`] (a blank node
//! that joins as a learner) and [`MetaHandle::start_with_options`] (level emulation).

use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use ostro_raft_driver::raft::eraftpb::{ConfChangeV2, ConfState, Message};
use tokio::time::Instant;

use crate::command::{self, DecodeError, MetaCommand, MetaResult, decode_result, encode_command};
use crate::records::ClusterInfo;
use crate::sm::{MetaSm, MetaView};
use crate::{
    Applied, DriverConfig, NullOutbox, Outbox, ProposeError, RaftDriver, RaftHandle, RaftStatus,
};

/// How long `start` waits for this node to become leader and catch up. A hang guard, not a
/// latency target (a lone voter campaigns at once and normally leads in tens of ms): 5 s
/// fired spuriously on a heavily loaded machine (load average 15-30, fsync stalls of
/// seconds), failing an otherwise healthy start, so it is 30 s. A real hang still fails.
const START_TIMEOUT: Duration = Duration::from_secs(30);
/// Retries of a proposal whose outcome is unknown (leadership lost mid-flight).
const OUTCOME_UNKNOWN_RETRIES: u32 = 5;
const OUTCOME_UNKNOWN_BACKOFF: Duration = Duration::from_millis(50);
/// How long a proposal failed with `Stopped` waits for the driver to publish why.
const STOPPED_REASON_WAIT: Duration = Duration::from_secs(1);
/// Multi-voter: how long a proposal keeps retrying while no leader is known or the
/// leader moved (an election takes `election_tick` .. `2 * election_tick` ticks), and
/// how long a forwarded proposal waits for the local apply (read-your-writes).
const PROPOSE_DEADLINE: Duration = Duration::from_secs(30);
/// Multi-voter: pause between retries while no leader is known / the leader moved.
const LEADER_RETRY_BACKOFF: Duration = Duration::from_millis(100);
/// Default bound of [`MetaHandle::read_index_barrier`] (an election plus a heartbeat round).
const READ_BARRIER_DEADLINE: Duration = Duration::from_secs(10);
/// How long [`MetaHandle::transfer_leader`] waits for the target to lead.
const TRANSFER_DEADLINE: Duration = Duration::from_secs(10);

/// Extra start options ([`MetaHandle::start_with_options`], 13-05).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetaOptions {
    /// fsync the Raft log store (only tests turn this off).
    pub sync: bool,
    /// Level emulation (`ostro_types::compat::level_max()` under the `level-emulation`
    /// feature): a committed command needing a higher feature level halts this voter.
    /// `None`: no limit (the real binary supports every command it can decode).
    pub level_max: Option<u32>,
}

impl Default for MetaOptions {
    fn default() -> Self {
        MetaOptions {
            sync: true,
            level_max: None,
        }
    }
}

/// The future returned by [`ProposeForwarder::forward`].
pub type ForwardFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Applied, ProposeError>> + Send + 'a>>;

/// Runs an encoded proposal on the metadata leader (12-05; ostro-cluster's
/// `GrpcForwarder` over `MetaPeer.Propose`).
///
/// The result carries the leader's `Applied { index, term, result }`; [`MetaHandle`]
/// then waits until the local node applied `index` (read-your-writes). Errors keep the
/// driver's meaning: `NotLeader { leader }` (the target is not the leader: nothing was
/// applied), `Dropped`, `OutcomeUnknown` (it may or may not take effect; transport
/// failures after the request may have been sent belong here) and `Stopped`.
pub trait ProposeForwarder: Send + Sync + 'static {
    /// Propose `cmd` (an encoded [`MetaCommand`]) on `leader`.
    fn forward(&self, leader: u64, cmd: Vec<u8>) -> ForwardFuture<'_>;
}

/// Raft timing of the metadata group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TickConfig {
    /// Driver tick.
    pub interval: Duration,
    /// Election timeout in ticks (randomized in `[election_tick, 2 * election_tick)`).
    pub election_tick: usize,
    /// Leader heartbeat in ticks (`< election_tick`).
    pub heartbeat_tick: usize,
}

impl Default for TickConfig {
    /// The vendored driver's defaults (100 ms, election 20, heartbeat 3), which the
    /// single-voter [`MetaHandle::start`] has always used.
    fn default() -> Self {
        let d = DriverConfig::new(1);
        TickConfig {
            interval: d.tick_interval,
            election_tick: d.election_tick,
            heartbeat_tick: d.heartbeat_tick,
        }
    }
}

/// How to start the metadata group ([`MetaHandle::start_with`]).
pub struct MetaStart {
    /// This node's id (a member of `voters`).
    pub node_id: u64,
    /// The static voter set; every founding node passes the same set. `[node_id]` is the
    /// single-voter group (immediate campaign, as [`MetaHandle::start`]).
    pub voters: Vec<u64>,
    /// Carries Raft messages to the other voters. Must not block (driver thread).
    pub outbox: Box<dyn Outbox>,
    /// Runs proposals on the leader when this node is a follower. `None`: a follower's
    /// proposal fails with `NotLeader`.
    pub forwarder: Option<Arc<dyn ProposeForwarder>>,
    /// Raft timing.
    pub tick: TickConfig,
}

impl std::fmt::Debug for MetaStart {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetaStart")
            .field("node_id", &self.node_id)
            .field("voters", &self.voters)
            .field("forwarder", &self.forwarder.is_some())
            .field("tick", &self.tick)
            .finish_non_exhaustive()
    }
}

/// Adapts the boxed caller outbox to the driver's generic `Outbox` parameter.
struct DynOutbox(Box<dyn Outbox>);

impl Outbox for DynOutbox {
    fn send(&mut self, messages: Vec<Message>) {
        self.0.send(messages);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MetaError {
    #[error("starting metadata raft: {0}")]
    Start(String),
    #[error("metadata proposal failed: {0}")]
    Propose(String),
    #[error("decoding metadata result: {0}")]
    Decode(String),
    #[error("metadata raft did not elect a leader / catch up in time")]
    Timeout,
    #[error("metadata command rejected: {0}")]
    Rejected(String),
    /// 13-05: the metadata (an entry, a snapshot or a result) is in a newer meta format:
    /// a newer Ostro finalized this cluster.
    #[error(
        "metadata written by a newer Ostro (meta format {found}, this build reads {supported}): this cluster was finalized by a newer Ostro; upgrade this binary"
    )]
    NewerFormat { found: u8, supported: u8 },
    /// 13-05: a leader-only operation ran on a follower; `hint` is the leader it knows.
    #[error("not the metadata leader (leader hint {hint:?})")]
    NotLeader { hint: Option<u64> },
}

impl MetaError {
    /// Whether the call failed only because no metadata leader was reachable: a
    /// leader-only call on a follower, a timeout, or a proposal that gave up while the
    /// leader was unknown / unreachable, moved, refused it during a transfer or lost
    /// leadership before applying it. Callers whose command is idempotent (InitCluster,
    /// a CAS-guarded upsert) may retry these (p13-bugs2: boot proposals).
    pub fn is_leader_unavailable(&self) -> bool {
        match self {
            MetaError::NotLeader { .. } | MetaError::Timeout => true,
            MetaError::Propose(m) => {
                m.starts_with(NOT_LEADER_PREFIX)
                    || *m == ProposeError::Dropped.to_string()
                    || *m == ProposeError::OutcomeUnknown.to_string()
            }
            _ => false,
        }
    }
}

/// The start of `ProposeError::NotLeader`'s message (with or without a leader hint).
const NOT_LEADER_PREFIX: &str = "not the raft leader";

fn decode_err(e: DecodeError) -> MetaError {
    match e {
        DecodeError::NewerFormat { found, supported } => {
            MetaError::NewerFormat { found, supported }
        }
        e => MetaError::Decode(e.to_string()),
    }
}

/// A point-in-time view of the metadata Raft group, for the `ostro_raft_*` gauges
/// (OBS-02, plan 08-16). Read from the driver's published status (a watch value), never
/// from the driver thread.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetaRaftStatus {
    /// Current Raft term.
    pub term: u64,
    /// Highest log index known committed.
    pub commit_index: u64,
    /// Highest log index applied to the metadata state machine.
    pub applied_index: u64,
    /// This node leads the metadata group.
    pub is_leader: bool,
}

/// A running metadata group member.
pub struct MetaHandle {
    /// `Some` until [`MetaHandle::stop`]; dropping the handle also stops the
    /// driver (blocking the dropping thread on the join).
    driver: Option<RaftDriver>,
    raft: RaftHandle,
    view: MetaView,
    node_id: u64,
    /// More than one voter: proposals wait out elections and are forwarded.
    multi: bool,
    forwarder: Option<Arc<dyn ProposeForwarder>>,
}

impl std::fmt::Debug for MetaHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetaHandle")
            .field("status", &self.raft.status())
            .finish_non_exhaustive()
    }
}

impl MetaHandle {
    /// Open (or create) the metadata Raft log in `dir` as a single voter
    /// `node_id`, campaign immediately and return once this node is leader and
    /// has applied every committed entry (so reads see all durable records).
    ///
    /// Campaigning right away is a locked decision: a lone voter otherwise waits
    /// out the randomized election timeout (~2-4 s with the default 100 ms tick
    /// and election_tick 20) instead of ~25-60 ms. The log store keeps its
    /// default `sync = true`, including for ephemeral data dirs.
    ///
    /// A data dir written in the pre-Phase-5 meta format fails with
    /// [`MetaError::Start`] carrying [`DecodeError::PreFreezeFormat`]'s message
    /// ("delete the dev data dir"), whether the old data is in the snapshot or
    /// in the log.
    ///
    /// Tests/examples only; production uses [`MetaHandle::start_with`] with the cluster
    /// `PeerOutbox` (12-14), so the cluster transport is live in every mode (D-P12-13).
    /// This is `start_with` with no peers ([`NullOutbox`]), no forwarder and the default
    /// ticks.
    pub async fn start(dir: &Path, node_id: u64) -> Result<MetaHandle, MetaError> {
        Self::start_with(
            dir,
            MetaStart {
                node_id,
                voters: vec![node_id],
                outbox: Box::new(NullOutbox),
                forwarder: None,
                tick: TickConfig::default(),
            },
        )
        .await
    }

    /// Open (or create) the metadata Raft log in `dir` as member `start.node_id` of the
    /// static voter group `start.voters`.
    ///
    /// - `voters == [node_id]`: exactly [`MetaHandle::start`]'s behaviour (immediate
    ///   campaign; returns once leader and caught up).
    /// - More voters: returns once the local log is replayed (applied ≥ the persisted
    ///   commit index), without waiting for an election, because the peers' messages may
    ///   only start flowing once the caller wired its transport to [`MetaHandle::raft`].
    ///   Call [`MetaHandle::wait_ready`] to wait for a leader.
    ///
    /// The pre-Phase-5 format check of [`MetaHandle::start`] applies to both.
    pub async fn start_with(dir: &Path, start: MetaStart) -> Result<MetaHandle, MetaError> {
        Self::start_inner(dir, start, MetaOptions::default(), false).await
    }

    /// [`MetaHandle::start_with`] with [`MetaOptions`] (13-05: level emulation).
    pub async fn start_with_options(
        dir: &Path,
        start: MetaStart,
        options: MetaOptions,
    ) -> Result<MetaHandle, MetaError> {
        Self::start_inner(dir, start, options, false).await
    }

    /// Start a node that **joins** an existing metadata group (13-05, for 13-09): a blank
    /// `dir` starts with no configuration at all (no bootstrap snapshot) and waits for the
    /// leader to add it as a learner and send it a snapshot. `start.voters` are the known
    /// members (the outbox / forwarder targets) and need not contain `start.node_id`. A
    /// `dir` that already holds a log (a restarted learner or voter) starts from its own
    /// persisted configuration, as [`MetaHandle::start_with`] does. Returns without waiting
    /// for a leader; [`MetaHandle::wait_ready`] waits for one.
    pub async fn start_joiner(
        dir: &Path,
        start: MetaStart,
        options: MetaOptions,
    ) -> Result<MetaHandle, MetaError> {
        Self::start_inner(dir, start, options, true).await
    }

    /// [`MetaHandle::start_with`] over a Raft log store that does not fsync (the vendored
    /// driver's `LogStoreOptions::sync = false`, "only tests turn this off").
    ///
    /// Test-only (ci-fix7): election and failover tests assert Raft timing bounds derived
    /// from the tick configuration. On macOS CI one `F_FULLFSYNC` of the log store stalled
    /// for seconds inside that window (a candidate's HardState or the new leader's first
    /// append), so the bound measured the runner's disk instead of the election. Durability
    /// is covered by the meta persistence and failpoint suites, which keep fsync on.
    #[doc(hidden)]
    pub async fn start_with_unsynced_log(
        dir: &Path,
        start: MetaStart,
    ) -> Result<MetaHandle, MetaError> {
        let options = MetaOptions {
            sync: false,
            ..MetaOptions::default()
        };
        Self::start_inner(dir, start, options, false).await
    }

    async fn start_inner(
        dir: &Path,
        start: MetaStart,
        options: MetaOptions,
        joiner: bool,
    ) -> Result<MetaHandle, MetaError> {
        let MetaOptions { sync, level_max } = options;
        let MetaStart {
            node_id,
            voters,
            outbox,
            forwarder,
            tick,
        } = start;
        if node_id == 0 || voters.contains(&0) {
            return Err(MetaError::Start("node id 0 is reserved".into()));
        }
        if !joiner && !voters.contains(&node_id) {
            return Err(MetaError::Start(format!(
                "node {node_id} is not one of the metadata voters {voters:?}"
            )));
        }
        for (i, v) in voters.iter().enumerate() {
            if voters[..i].contains(v) {
                return Err(MetaError::Start(format!(
                    "metadata voters {voters:?} list node {v} twice"
                )));
            }
        }
        let multi = joiner || voters.len() > 1;
        let config = DriverConfig {
            tick_interval: tick.interval,
            election_tick: tick.election_tick,
            heartbeat_tick: tick.heartbeat_tick,
            log_store: ostro_raft_driver::LogStoreOptions {
                sync,
                ..ostro_raft_driver::LogStoreOptions::default()
            },
            ..DriverConfig::new(node_id)
        };
        let (sm, view) = MetaSm::with_level_max(level_max.unwrap_or(u32::MAX));
        let newer = view.clone();
        let initial: &[u64] = if joiner { &[] } else { &voters };
        let driver = RaftDriver::start(dir, config, initial, sm, DynOutbox(outbox)).map_err(
            |e| match newer.newer_format() {
                Some(found) => MetaError::NewerFormat {
                    found,
                    supported: crate::META_FORMAT,
                },
                None => MetaError::Start(e.to_string()),
            },
        )?;
        let raft = driver.handle();
        if !multi {
            raft.campaign();
        }
        let handle = MetaHandle {
            driver: Some(driver),
            raft,
            view,
            node_id,
            multi,
            forwarder,
        };
        let ready = async {
            if multi {
                handle.wait_replayed().await
            } else {
                handle.wait_leader_caught_up().await
            }
        };
        match tokio::time::timeout(START_TIMEOUT, ready).await {
            Ok(Ok(())) => match handle.view.pre_freeze_format() {
                // A replayed log entry predates the Phase 5 meta format (D20).
                Some(found) => {
                    handle.stop().await;
                    Err(MetaError::Start(
                        DecodeError::PreFreezeFormat { found }.to_string(),
                    ))
                }
                None => Ok(handle),
            },
            Ok(Err(e)) => {
                let newer = handle.view.newer_format();
                handle.stop().await;
                match newer {
                    Some(found) => Err(MetaError::NewerFormat {
                        found,
                        supported: crate::META_FORMAT,
                    }),
                    None => Err(e),
                }
            }
            Err(_) => {
                handle.stop().await;
                Err(MetaError::Timeout)
            }
        }
    }

    /// Wait until some node leads the group and this node applied everything it knows
    /// committed (multi-voter readiness; immediate for a started single voter).
    /// [`MetaError::Timeout`] after 30 s.
    pub async fn wait_ready(&self) -> Result<(), MetaError> {
        let ready = async {
            let mut rx = self.raft.subscribe();
            loop {
                {
                    let s = rx.borrow_and_update();
                    if let Some(f) = &s.failed {
                        return Err(MetaError::Start(f.clone()));
                    }
                    if s.leader_id.is_some() && s.applied >= s.commit {
                        return Ok(());
                    }
                }
                rx.changed()
                    .await
                    .map_err(|_| MetaError::Start(ProposeError::Stopped.to_string()))?;
            }
        };
        tokio::time::timeout(START_TIMEOUT, ready)
            .await
            .map_err(|_| MetaError::Timeout)?
    }

    /// Wait until the entries committed before this start are applied locally.
    async fn wait_replayed(&self) -> Result<(), MetaError> {
        let s = self.raft.status();
        if let Some(f) = s.failed {
            return Err(MetaError::Start(f));
        }
        self.raft
            .wait_applied(s.commit)
            .await
            .map_err(|e| MetaError::Start(e.to_string()))
    }

    /// Bootstrap a brand-new metadata group in the empty directory `dir` from a metadata
    /// snapshot (`[META_FORMAT] ‖ postcard(MetaState)`, as uploaded by the bottomless
    /// uploader as `meta/<index>.msnap`), then start it like [`MetaHandle::start`].
    ///
    /// Native raft install (08-18 spike, research Open Question 5): the snapshot is written
    /// into the Raft log store at `index` (term 1, configuration = {`node_id`}) exactly as a
    /// follower would install a leader's snapshot, so the restored state is byte-identical,
    /// including every record's `updated_index`, the applied index is `index`, and new
    /// proposals continue after it (the new term's empty entry is `index + 1`). No leader is
    /// needed: the lone voter campaigns on start.
    ///
    /// Refuses a directory that already holds a Raft log or snapshot, and a snapshot that
    /// does not decode (nothing is written then). `index` 0 is treated as 1 (raft reserves 0
    /// for "no snapshot").
    pub async fn bootstrap_from_snapshot(
        dir: &Path,
        node_id: u64,
        index: u64,
        snapshot: &[u8],
    ) -> Result<MetaHandle, MetaError> {
        command::decode_snapshot(snapshot)
            .map_err(|e| MetaError::Start(format!("metadata snapshot: {e}")))?;
        let dir_buf = dir.to_path_buf();
        let data = snapshot.to_vec();
        tokio::task::spawn_blocking(move || install_bootstrap(&dir_buf, node_id, index, data))
            .await
            .map_err(|e| MetaError::Start(format!("bootstrap task: {e}")))??;
        Self::start(dir, node_id).await
    }

    async fn wait_leader_caught_up(&self) -> Result<(), MetaError> {
        let mut rx = self.raft.subscribe();
        let commit = loop {
            {
                let s = rx.borrow_and_update();
                if let Some(f) = &s.failed {
                    return Err(MetaError::Start(f.clone()));
                }
                if s.is_leader {
                    break s.commit;
                }
            }
            rx.changed()
                .await
                .map_err(|_| MetaError::Start(ProposeError::Stopped.to_string()))?;
        };
        self.raft
            .wait_applied(commit)
            .await
            .map_err(|e| MetaError::Start(e.to_string()))
    }

    /// Propose `cmd` and return its verdict once applied locally; afterwards
    /// [`MetaView::read`] observes the change (read-your-writes).
    ///
    /// `ProposeError::OutcomeUnknown` is retried a bounded number of times. That
    /// is safe because every command is CAS-guarded: a retry of a command that
    /// did take effect cannot apply twice (it reports `Conflict`/`StaleVersion`
    /// instead, or `Ok` for idempotent commands).
    ///
    /// Multi-voter (12-05): on a follower the proposal goes to the leader through the
    /// [`ProposeForwarder`], then waits until this node applied the leader's index. While
    /// no leader is known, the leader moved, or the outcome is unknown (failover), it
    /// retries for up to 30 s.
    pub async fn propose(&self, cmd: MetaCommand) -> Result<MetaResult, MetaError> {
        let bytes = encode_command(&cmd);
        let deadline = Instant::now() + PROPOSE_DEADLINE;
        let mut attempt = 0;
        loop {
            let outcome = match self.raft.propose(bytes.clone()).await {
                Err(ProposeError::NotLeader { leader: Some(l) }) if l != self.node_id => {
                    match &self.forwarder {
                        Some(f) => f.forward(l, bytes.clone()).await.map(|a| (a, true)),
                        None => Err(ProposeError::NotLeader { leader: Some(l) }),
                    }
                }
                other => other.map(|a| (a, false)),
            };
            match outcome {
                Ok((applied, forwarded)) => {
                    if forwarded {
                        self.wait_applied_until(applied.index, deadline).await?;
                    }
                    return decode_result(&applied.result).map_err(decode_err);
                }
                // Multi-voter: a failover (or a forwarded call that lost its leader) can take
                // an election timeout, so retries run until the deadline instead of a count.
                Err(ProposeError::OutcomeUnknown)
                    if attempt < OUTCOME_UNKNOWN_RETRIES
                        || (self.multi && Instant::now() < deadline) =>
                {
                    attempt += 1;
                    tracing::warn!(attempt, "metadata proposal outcome unknown; retrying");
                    tokio::time::sleep(if self.multi {
                        LEADER_RETRY_BACKOFF
                    } else {
                        OUTCOME_UNKNOWN_BACKOFF
                    })
                    .await;
                }
                Err(ProposeError::NotLeader { leader })
                    if self.multi && Instant::now() < deadline =>
                {
                    tracing::debug!(?leader, "metadata leader unknown or moved; retrying");
                    tokio::time::sleep(LEADER_RETRY_BACKOFF).await;
                }
                Err(ProposeError::Stopped) => {
                    return Err(self.stopped_err().await);
                }
                Err(e) => return Err(self.halted_or(e)),
            }
        }
    }

    /// A stopped driver: [`MetaError::NewerFormat`] after a newer-format halt, else
    /// `Propose` with the published failure.
    async fn stopped_err(&self) -> MetaError {
        match self.view.newer_format() {
            Some(found) => MetaError::NewerFormat {
                found,
                supported: crate::META_FORMAT,
            },
            None => MetaError::Propose(self.stopped_reason().await),
        }
    }

    /// A newer-format halt as [`MetaError::NewerFormat`], anything else as `Propose`.
    fn halted_or(&self, e: ProposeError) -> MetaError {
        match self.view.newer_format() {
            Some(found) => MetaError::NewerFormat {
                found,
                supported: crate::META_FORMAT,
            },
            None => MetaError::Propose(e.to_string()),
        }
    }

    // ---- Phase 13 (13-05): membership + consistency surface ---------------------------

    /// A linearizable read barrier (D-P13-15): a raft `ReadIndex` on the local driver (the
    /// leader confirms leadership with a quorum heartbeat round; a follower asks the
    /// leader), then wait until this node applied that index. Afterwards [`MetaView::read`]
    /// observes every write acknowledged before the call, on any node. Returns the index.
    /// Bounded by 10 s ([`MetaHandle::read_index_barrier_until`] takes a caller deadline);
    /// retries across elections until then.
    ///
    /// A lone voter in a one-voter configuration answers from its own commit index without
    /// a round (it is the quorum). A cut-off old leader cannot pass the barrier: it never
    /// gets the quorum acknowledgement (unlike a plain local read, which it serves until
    /// check-quorum steps it down).
    pub async fn read_index_barrier(&self) -> Result<u64, MetaError> {
        self.read_index_barrier_until(Instant::now() + READ_BARRIER_DEADLINE)
            .await
    }

    /// [`MetaHandle::read_index_barrier`] bounded by `deadline` ([`MetaError::Timeout`]).
    pub async fn read_index_barrier_until(&self, deadline: Instant) -> Result<u64, MetaError> {
        let index = loop {
            let s = self.raft.status();
            if s.is_leader && s.voters == [self.node_id] && s.voters_outgoing.is_empty() {
                break s.commit;
            }
            match tokio::time::timeout_at(deadline, self.raft.read_index()).await {
                Err(_) => return Err(MetaError::Timeout),
                Ok(Ok(index)) => break index,
                Ok(Err(ProposeError::NotLeader { .. } | ProposeError::Dropped)) => {
                    if Instant::now() >= deadline {
                        return Err(MetaError::Timeout);
                    }
                    tokio::time::sleep(
                        LEADER_RETRY_BACKOFF
                            .min(deadline.saturating_duration_since(Instant::now())),
                    )
                    .await;
                }
                Ok(Err(ProposeError::Stopped)) => {
                    return Err(self.stopped_err().await);
                }
                Ok(Err(e)) => return Err(self.halted_or(e)),
            }
        };
        self.wait_applied_until(index, deadline).await?;
        Ok(index)
    }

    /// Hand metadata leadership to voter `to` and wait (up to 10 s) until it leads.
    /// Leader only ([`MetaError::NotLeader`] elsewhere); `to` must be a voter
    /// ([`MetaError::Rejected`]). [`MetaError::Timeout`] when leadership did not move
    /// (raft aborts a transfer after an election timeout; proposals resume).
    pub async fn transfer_leader(&self, to: u64) -> Result<(), MetaError> {
        match self.raft.transfer_leader(to).await {
            Ok(()) => {}
            Err(ProposeError::NotLeader { leader }) => {
                return Err(MetaError::NotLeader { hint: leader });
            }
            Err(ProposeError::Dropped) => {
                return Err(MetaError::Rejected(format!(
                    "node {to} is not a metadata voter"
                )));
            }
            Err(e) => return Err(self.halted_or(e)),
        }
        let deadline = Instant::now() + TRANSFER_DEADLINE;
        let mut rx = self.raft.subscribe();
        let moved = async {
            loop {
                {
                    let s = rx.borrow_and_update();
                    if s.leader_id == Some(to) && (to != self.node_id || s.is_leader) {
                        return Ok(());
                    }
                    if let Some(f) = &s.failed {
                        return Err(MetaError::Propose(f.clone()));
                    }
                }
                if rx.changed().await.is_err() {
                    return Err(MetaError::Propose(ProposeError::Stopped.to_string()));
                }
            }
        };
        tokio::time::timeout_at(deadline, moved)
            .await
            .map_err(|_| MetaError::Timeout)?
    }

    /// Propose a membership change (learner add, promotion, joint consensus, or an empty
    /// `ConfChangeV2` to leave a joint configuration). Leader only: on a follower
    /// [`MetaError::NotLeader`] with the leader hint. Resolves once applied locally; the
    /// result carries the encoded new `ConfState`, and [`MetaHandle::conf_state`] already
    /// shows it. A change raft refuses (e.g. one while
    /// another is pending, or leaving a non-joint configuration) is [`MetaError::Rejected`].
    pub async fn propose_conf_change(&self, cc: ConfChangeV2) -> Result<Applied, MetaError> {
        match self.raft.propose_conf_change(cc).await {
            Ok(applied) if applied.result.is_empty() => Err(MetaError::Rejected(
                "membership change refused by raft".into(),
            )),
            Ok(applied) => {
                // The driver publishes its status after the cycle that answered; wait
                // for it so `conf_state` / `raft_detail` show the new membership.
                self.wait_applied_until(applied.index, Instant::now() + PROPOSE_DEADLINE)
                    .await?;
                Ok(applied)
            }
            Err(ProposeError::NotLeader { leader }) => Err(MetaError::NotLeader { hint: leader }),
            Err(ProposeError::Stopped) => Err(self.stopped_err().await),
            Err(e) => Err(self.halted_or(e)),
        }
    }

    /// The membership this node applied: voters, learners, the outgoing voters of a joint
    /// configuration, the learners-to-be and `auto_leave`.
    pub fn conf_state(&self) -> ConfState {
        let s = self.raft.status();
        ConfState {
            voters: s.voters,
            learners: s.learners,
            voters_outgoing: s.voters_outgoing,
            learners_next: s.learners_next,
            auto_leave: s.auto_leave,
        }
    }

    /// The driver's full status (13-05): term, leader, commit / applied, membership incl.
    /// joint state and `pending_conf`, per-peer progress on the leader, failures. The
    /// existing [`MetaHandle::raft_status`] keeps its small `Copy` view for the gauges.
    pub fn raft_detail(&self) -> RaftStatus {
        self.raft.status()
    }

    /// The metadata lease anchor (`RaftHandle::lease_anchor`, D-P13-15; monotonic, never
    /// wall time): the send instant of this node's newest quorum-confirmed lease round,
    /// `now` on a one-voter leader, `None` without a confirmed leader.
    pub fn lease_anchor(&self) -> Option<std::time::Instant> {
        self.raft.lease_anchor()
    }

    /// Read-your-writes after a forwarded proposal: wait until `index` is applied here.
    async fn wait_applied_until(&self, index: u64, deadline: Instant) -> Result<(), MetaError> {
        match tokio::time::timeout_at(deadline, self.raft.wait_applied(index)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(ProposeError::Stopped)) => Err(self.stopped_err().await),
            Ok(Err(e)) => Err(MetaError::Propose(e.to_string())),
            Err(_) => Err(MetaError::Timeout),
        }
    }

    /// The driver's storage failure (or a `FatalApply` halt, 13-05), if it stopped on one (`RaftStatus::failed`, e.g.
    /// `raft storage: No space left on device (os error 28)`). A stopped metadata group
    /// never recovers in place: the node must restart, which re-opens the log.
    pub fn failure(&self) -> Option<String> {
        self.raft.status().failed
    }

    /// `"stopped"`, plus the storage failure that stopped the driver when there is one.
    /// The driver fails pending proposals just before it publishes the failure, so this
    /// waits briefly for it (the status channel closes when the driver thread exits).
    async fn stopped_reason(&self) -> String {
        let mut rx = self.raft.subscribe();
        let failed = async {
            loop {
                if let Some(f) = rx.borrow_and_update().failed.clone() {
                    return Some(f);
                }
                if rx.changed().await.is_err() {
                    return rx.borrow().failed.clone();
                }
            }
        };
        match tokio::time::timeout(STOPPED_REASON_WAIT, failed).await {
            Ok(Some(f)) => format!("{}: {f}", ProposeError::Stopped),
            _ => ProposeError::Stopped.to_string(),
        }
    }

    /// Propose `InitCluster` and return the stored [`ClusterInfo`]. Idempotent
    /// for the same partition count (the existing record is returned, its
    /// cluster id and token key untouched); a different count is
    /// [`MetaError::Rejected`].
    pub async fn init_cluster(
        &self,
        cluster_id: &str,
        node_id: u64,
        partitions: u16,
        now_ms: u64,
        token_key: [u8; 32],
    ) -> Result<ClusterInfo, MetaError> {
        let cmd = MetaCommand::InitCluster {
            cluster_id: cluster_id.to_string(),
            node_id,
            partitions,
            created_at_ms: now_ms,
            token_key,
        };
        match self.propose(cmd).await? {
            MetaResult::Ok { .. } => self
                .view
                .read(|s| s.cluster().map(|c| c.value.clone()))
                .ok_or_else(|| {
                    MetaError::Rejected("cluster record missing after InitCluster".into())
                }),
            MetaResult::Invalid(msg) => Err(MetaError::Rejected(msg)),
            other => Err(MetaError::Rejected(format!(
                "unexpected InitCluster result {other:?}"
            ))),
        }
    }

    pub fn view(&self) -> &MetaView {
        &self.view
    }

    pub fn is_leader(&self) -> bool {
        self.raft.status().is_leader
    }

    /// Term, commit / applied index and leadership of the metadata group (read-only).
    pub fn raft_status(&self) -> MetaRaftStatus {
        let s = self.raft.status();
        MetaRaftStatus {
            term: s.term,
            commit_index: s.commit,
            applied_index: s.applied,
            is_leader: s.is_leader,
        }
    }

    /// The underlying Raft handle (status, subscribe, `step` for incoming peer messages,
    /// `report_unreachable`, `sample_live`, `leader_contact_age`).
    pub fn raft(&self) -> &RaftHandle {
        &self.raft
    }

    /// This node's id.
    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    /// Stop the driver, joining its thread off the async runtime.
    pub async fn stop(mut self) {
        if let Some(driver) = self.driver.take() {
            let _ = tokio::task::spawn_blocking(move || driver.stop()).await;
        }
    }
}

/// Write the bootstrap snapshot into an empty Raft log store (blocking).
fn install_bootstrap(dir: &Path, node_id: u64, index: u64, data: Vec<u8>) -> Result<(), MetaError> {
    use ostro_raft_driver::raft::eraftpb::Snapshot;
    use ostro_raft_driver::{LogStoreOptions, RaftLogStore};

    let start = |e: std::io::Error| MetaError::Start(format!("metadata log store: {e}"));
    let store = RaftLogStore::open(dir, LogStoreOptions::default()).map_err(start)?;
    if store.snapshot_index_term() != (0, 0) || store.entry_count() > 0 {
        return Err(MetaError::Start(format!(
            "{}: metadata log already exists; bootstrap needs an empty directory",
            dir.display()
        )));
    }
    let mut snap = Snapshot {
        data: data.into(),
        ..Snapshot::default()
    };
    let meta = snap.mut_metadata();
    meta.index = index.max(1);
    meta.term = 1;
    meta.mut_conf_state().voters = vec![node_id];
    store
        .apply_snapshot(snap)
        .map_err(start)?
        .map_err(|e| MetaError::Start(format!("bootstrap snapshot: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// p13-bugs2: the boot retry classifies exactly the "no reachable leader" failures,
    /// pinned against the driver's messages.
    #[test]
    fn leader_unavailable_classification() {
        let propose = |e: ProposeError| MetaError::Propose(e.to_string());
        for e in [
            propose(ProposeError::NotLeader { leader: None }),
            propose(ProposeError::NotLeader { leader: Some(4) }),
            propose(ProposeError::Dropped),
            propose(ProposeError::OutcomeUnknown),
            MetaError::NotLeader { hint: None },
            MetaError::Timeout,
        ] {
            assert!(e.is_leader_unavailable(), "{e}");
        }
        for e in [
            propose(ProposeError::Stopped),
            propose(ProposeError::Fatal("apply".into())),
            MetaError::Rejected("x".into()),
            MetaError::Start("x".into()),
            MetaError::NewerFormat {
                found: 9,
                supported: 1,
            },
        ] {
            assert!(!e.is_leader_unavailable(), "{e}");
        }
    }

    #[tokio::test]
    async fn raft_status_reports_the_lone_leader() {
        let dir = tempfile::tempdir().expect("tempdir");
        let h = MetaHandle::start(dir.path(), 1).await.expect("meta starts");
        let s = h.raft_status();
        assert!(s.is_leader, "{s:?}");
        assert!(s.term >= 1, "{s:?}");
        assert!(s.commit_index >= 1, "{s:?}");
        assert!(s.applied_index <= s.commit_index, "{s:?}");
        h.stop().await;
    }
}
