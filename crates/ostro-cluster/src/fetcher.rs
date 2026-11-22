//! Follower fetch loop (12-11, research Pattern 3): the
//! [`FollowerDriver`](ostro_partition::roles::FollowerDriver) implementation.
//!
//! [`FetcherDriver::start`] spawns one task per followed partition. Each round:
//!
//! 1. builds a `FetchRequest` (`leader_epoch` = the followed epoch, `fetch_offset` = LEO,
//!    `last_fetched_epoch` = the newest epoch in the log, seeded history included for an
//!    anchored empty log, `durable_leo` = the fsynced end);
//! 2. on a transport failure reports the leader as suspect when the connection was refused
//!    or reset ([`SuspectSink`], `MetaPeer.ReportSuspect` wired by 12-14) and backs off
//!    50 ms → 1 s;
//! 3. on `FENCED_LEADER_EPOCH` / `UNKNOWN_LEADER_EPOCH` / `NOT_LEADER` / `NOT_REPLICA` backs
//!    off and retries: the role lifecycle (12-13) restarts the fetcher with the next
//!    assignment's leader and epoch;
//! 4. on `diverging{epoch, end}` truncates to [`follower_truncation_point`] and refetches
//!    (KIP-279 iteration; the `repl::mutant::no_truncate` failpoint ignores the answer);
//! 5. on `SNAPSHOT_REQUIRED` runs [`install_from_leader`] and refetches;
//! 6. otherwise verifies every envelope (CRC, partition, `base == LEO`, epoch non-decreasing
//!    and not above the followed epoch), fetches every blob the commands reference that the
//!    local store lacks (F11, `Replication.FetchBlob`), `append_replica`s the verbatim bytes
//!    (failpoint `fetch::before_append`), waits until they are durable (failpoint
//!    `fetch::before_durable`), and adopts the leader's HW (`set_follower_hw`, capped at the
//!    local durable end by the log).
//!
//! v1 fetches again only after the previous batch is durable (D-P12-23), so `fetch_offset ==
//! durable_leo` on every request. The `repl::mutant::count_nondurable_leo` failpoint (the
//! follower half; the tracker half is 12-04's) reports `durable_leo = LEO` and does not wait.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures_util::future::BoxFuture;
use ostro_blob::BlobStore;
use ostro_log::replica::divergence::follower_truncation_point;
use ostro_log::{RawBatch, ReplicatedLog};
use ostro_partition::roles::{FollowerDriver, FollowerTask, PartitionLogHandle};
use ostro_proto::cluster::v1::replication_client::ReplicationClient;
use ostro_proto::cluster::v1::{FetchError, FetchRequest, FetchResponse};
use ostro_types::envelope;
use ostro_types::{Epoch, Offset, PartitionId};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::blob;
use crate::fetch::{
    DEFAULT_FETCH_MAX_BYTES, DEFAULT_FETCH_MAX_WAIT, DEFAULT_FETCH_READ_TIMEOUT,
    fetch_decode_limit, mutant,
};
use crate::install::{InstallCtx, install_from_leader};
use crate::liveness::SuspectSink;
use crate::transport::ClusterTransport;

/// Follower fetch tuning.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FetcherConfig {
    /// `max_wait_ms` sent with every request (`fetch_max_wait`).
    pub max_wait: Duration,
    /// `max_bytes` sent with every request (`fetch_max_bytes`).
    pub max_bytes: usize,
    /// The leader's `fetch_read_timeout`; the RPC deadline is `max_wait + read_timeout + 1 s`.
    pub read_timeout: Duration,
    /// Largest envelope of the cluster (cluster-uniform, F1); sizes the decode limit.
    pub max_envelope_bytes: usize,
    /// First retry delay.
    pub backoff_min: Duration,
    /// Retry delay cap.
    pub backoff_max: Duration,
}

impl Default for FetcherConfig {
    fn default() -> Self {
        Self {
            max_wait: DEFAULT_FETCH_MAX_WAIT,
            max_bytes: DEFAULT_FETCH_MAX_BYTES,
            read_timeout: DEFAULT_FETCH_READ_TIMEOUT,
            max_envelope_bytes: envelope::DEFAULT_MAX_ENVELOPE_BYTES,
            backoff_min: Duration::from_millis(50),
            backoff_max: Duration::from_secs(1),
        }
    }
}

/// A test hook over every fetch response before it is processed.
pub type ResponseHook = Arc<dyn Fn(&mut FetchResponse) + Send + Sync>;
/// A test hook awaited before each blob fetch (gets the sha).
pub type BlobHook = Arc<dyn Fn([u8; 32]) -> BoxFuture<'static, ()> + Send + Sync>;

/// Test hooks (`#[doc(hidden)]` on the setter).
#[derive(Clone, Default)]
pub struct FetcherHooks {
    /// Mutate a response before processing (e.g. corrupt an envelope).
    pub on_response: Option<ResponseHook>,
    /// Delay before fetching a blob.
    pub before_blob_fetch: Option<BlobHook>,
}

/// Counters of one driver (all fetchers it started).
#[derive(Debug, Default)]
pub struct FetcherStats {
    /// Successful fetch round trips.
    pub fetches: AtomicU64,
    /// Envelopes appended.
    pub envelopes: AtomicU64,
    /// Responses rejected by verification (nothing appended).
    pub rejected: AtomicU64,
    /// Truncations after a diverging answer.
    pub truncations: AtomicU64,
    /// Blobs fetched before an append.
    pub blobs_fetched: AtomicU64,
    /// Transport failures.
    pub transport_errors: AtomicU64,
    /// Suspect reports sent.
    pub suspects: AtomicU64,
    /// Snapshot installs attempted.
    pub installs: AtomicU64,
}

/// Starts follower fetch loops.
pub struct FetcherDriver {
    node_id: u64,
    transport: Arc<dyn ClusterTransport>,
    blobs: Arc<BlobStore>,
    suspects: Arc<dyn SuspectSink>,
    install: Arc<InstallCtx>,
    cfg: FetcherConfig,
    hooks: FetcherHooks,
    stats: Arc<FetcherStats>,
}

impl fmt::Debug for FetcherDriver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FetcherDriver")
            .field("node_id", &self.node_id)
            .field("cfg", &self.cfg)
            .finish_non_exhaustive()
    }
}

impl FetcherDriver {
    /// A driver for node `node_id`, fetching over `transport`, storing blobs in `blobs`,
    /// reporting refused/reset leaders to `suspects`, installing snapshots with `install`.
    pub fn new(
        node_id: u64,
        transport: Arc<dyn ClusterTransport>,
        blobs: Arc<BlobStore>,
        suspects: Arc<dyn SuspectSink>,
        install: Arc<InstallCtx>,
        cfg: FetcherConfig,
    ) -> Self {
        Self {
            node_id,
            transport,
            blobs,
            suspects,
            install,
            cfg,
            hooks: FetcherHooks::default(),
            stats: Arc::new(FetcherStats::default()),
        }
    }

    /// Test hooks.
    #[doc(hidden)]
    pub fn with_hooks(mut self, hooks: FetcherHooks) -> Self {
        self.hooks = hooks;
        self
    }

    /// Counters of every fetcher this driver started.
    pub fn stats(&self) -> Arc<FetcherStats> {
        Arc::clone(&self.stats)
    }
}

impl FollowerDriver for FetcherDriver {
    fn start(
        &self,
        p: PartitionId,
        epoch: Epoch,
        leader: u64,
        log: Arc<PartitionLogHandle>,
    ) -> Box<dyn FollowerTask> {
        let cancel = CancellationToken::new();
        let fetcher = Fetcher {
            node_id: self.node_id,
            p,
            epoch,
            leader,
            log,
            transport: Arc::clone(&self.transport),
            blobs: Arc::clone(&self.blobs),
            suspects: Arc::clone(&self.suspects),
            install: Arc::clone(&self.install),
            cfg: self.cfg,
            hooks: self.hooks.clone(),
            stats: Arc::clone(&self.stats),
            cancel: cancel.clone(),
        };
        let join = tokio::spawn(fetcher.run());
        Box::new(RunningFetcher { cancel, join })
    }
}

struct RunningFetcher {
    cancel: CancellationToken,
    join: JoinHandle<()>,
}

impl FollowerTask for RunningFetcher {
    fn stop(self: Box<Self>) -> BoxFuture<'static, ()> {
        self.cancel.cancel();
        Box::pin(async move {
            let _ = self.join.await;
        })
    }
}

/// Why a fetch response could not be appended.
#[derive(Debug, thiserror::Error)]
enum RoundError {
    #[error("transport: {0}")]
    Transport(String),
    #[error("rpc: {0}")]
    Rpc(tonic::Status),
    #[error("verify: {0}")]
    Verify(String),
    #[error("blob: {0}")]
    Blob(String),
    #[error("log: {0}")]
    Log(String),
    #[error("leader answered {0:?}")]
    Refused(FetchError),
    #[error("install: {0}")]
    Install(String),
}

struct Fetcher {
    node_id: u64,
    p: PartitionId,
    epoch: Epoch,
    leader: u64,
    log: Arc<PartitionLogHandle>,
    transport: Arc<dyn ClusterTransport>,
    blobs: Arc<BlobStore>,
    suspects: Arc<dyn SuspectSink>,
    install: Arc<InstallCtx>,
    cfg: FetcherConfig,
    hooks: FetcherHooks,
    stats: Arc<FetcherStats>,
    cancel: CancellationToken,
}

/// Connection refused / reset (the leader process is gone): worth a suspect report.
fn is_connection_failure(s: &tonic::Status) -> bool {
    if s.code() != tonic::Code::Unavailable {
        return false;
    }
    let mut text = s.message().to_ascii_lowercase();
    let mut src: Option<&(dyn std::error::Error + 'static)> = std::error::Error::source(s);
    while let Some(e) = src {
        text.push(' ');
        text.push_str(&e.to_string().to_ascii_lowercase());
        src = e.source();
    }
    text.contains("refused")
        || text.contains("reset")
        || text.contains("broken pipe")
        || text.contains("tcp connect error")
        || text.contains("connection error")
}

impl Fetcher {
    async fn run(self) {
        info!(
            partition = self.p.0,
            epoch = self.epoch.0,
            leader = self.leader,
            "fetcher started"
        );
        let mut backoff = self.cfg.backoff_min;
        let mut client = None;
        loop {
            if self.cancel.is_cancelled() {
                break;
            }
            let round = tokio::select! {
                () = self.cancel.cancelled() => break,
                r = self.round(&mut client) => r,
            };
            match round {
                Ok(()) => backoff = self.cfg.backoff_min,
                Err(e) => {
                    match &e {
                        RoundError::Rpc(_) | RoundError::Transport(_) => client = None,
                        _ => {}
                    }
                    debug!(partition = self.p.0, leader = self.leader, error = %e, "fetch round failed");
                    tokio::select! {
                        () = self.cancel.cancelled() => break,
                        () = tokio::time::sleep(backoff) => {}
                    }
                    backoff = (backoff * 2).min(self.cfg.backoff_max);
                }
            }
        }
        info!(
            partition = self.p.0,
            leader = self.leader,
            "fetcher stopped"
        );
    }

    fn decode_limit(&self) -> usize {
        fetch_decode_limit(self.cfg.max_envelope_bytes, self.cfg.max_bytes)
    }

    fn client(
        &self,
        slot: &mut Option<ReplicationClient<tonic::transport::Channel>>,
    ) -> Result<ReplicationClient<tonic::transport::Channel>, RoundError> {
        if let Some(c) = slot {
            return Ok(c.clone());
        }
        let channel = self
            .transport
            .channel(self.leader)
            .map_err(|e| RoundError::Transport(e.to_string()))?;
        let limit = self.decode_limit();
        let c = ReplicationClient::new(channel)
            .max_decoding_message_size(limit)
            .max_encoding_message_size(limit);
        *slot = Some(c.clone());
        Ok(c)
    }

    fn request(&self) -> FetchRequest {
        let leo = self.log.log_end();
        let durable = if mutant(ostro_log::mutants::COUNT_NONDURABLE_LEO) {
            leo
        } else {
            self.log.durable_end()
        };
        FetchRequest {
            partition: u32::from(self.p.0),
            replica_id: self.node_id,
            leader_epoch: self.epoch.0,
            fetch_offset: leo.0,
            last_fetched_epoch: self.log.latest_epoch().map_or(0, |e| e.0),
            durable_leo: durable.0,
            max_bytes: u32::try_from(self.cfg.max_bytes).unwrap_or(u32::MAX),
            max_wait_ms: u32::try_from(self.cfg.max_wait.as_millis()).unwrap_or(u32::MAX),
            min_bytes: 1,
        }
    }

    async fn round(
        &self,
        slot: &mut Option<ReplicationClient<tonic::transport::Channel>>,
    ) -> Result<(), RoundError> {
        let mut client = self.client(slot)?;
        let mut req = tonic::Request::new(self.request());
        req.set_timeout(self.cfg.max_wait + self.cfg.read_timeout + Duration::from_secs(1));
        let mut resp = match client.fetch(req).await {
            Ok(r) => r.into_inner(),
            Err(s) => {
                self.stats.transport_errors.fetch_add(1, Ordering::Relaxed);
                if is_connection_failure(&s) {
                    self.stats.suspects.fetch_add(1, Ordering::Relaxed);
                    self.suspects
                        .report(self.node_id, self.leader, Some(self.p.0));
                }
                return Err(RoundError::Rpc(s));
            }
        };
        self.stats.fetches.fetch_add(1, Ordering::Relaxed);
        if let Some(h) = &self.hooks.on_response {
            h(&mut resp);
        }
        let error = FetchError::try_from(resp.error).unwrap_or(FetchError::NotLeader);
        match error {
            FetchError::None => {}
            FetchError::SnapshotRequired => {
                self.stats.installs.fetch_add(1, Ordering::Relaxed);
                info!(
                    partition = self.p.0,
                    leader = self.leader,
                    "leader requires a snapshot install"
                );
                return install_from_leader(&self.install, self.p, self.leader)
                    .await
                    .map(|_| ())
                    .map_err(|e| RoundError::Install(e.to_string()));
            }
            other => return Err(RoundError::Refused(other)),
        }
        if let Some(div) = resp.diverging
            && !mutant(ostro_log::mutants::NO_TRUNCATE)
        {
            let own_leo = self.log.log_end();
            let point = follower_truncation_point(
                &self.log.epochs(),
                own_leo,
                Epoch(div.epoch),
                Offset(div.end_offset),
            );
            info!(
                partition = self.p.0,
                diverging_epoch = div.epoch,
                leader_end = div.end_offset,
                own_leo = %own_leo,
                truncate_to = %point,
                "diverging follower log: truncating"
            );
            self.log
                .truncate_to(point)
                .await
                .map_err(|e| RoundError::Log(e.to_string()))?;
            self.stats.truncations.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        if !resp.envelopes.is_empty() {
            self.append(std::mem::take(&mut resp.envelopes)).await?;
        }
        self.log.set_follower_hw(Offset(resp.high_watermark));
        Ok(())
    }

    /// Verify, fetch blobs, append, wait durable.
    async fn append(&self, envelopes: Vec<Vec<u8>>) -> Result<(), RoundError> {
        let mut next = self.log.log_end();
        let mut min_epoch = self.log.latest_epoch().unwrap_or_default();
        let mut batches: Vec<RawBatch> = Vec::new();
        let mut shas: Vec<[u8; 32]> = Vec::new();
        for env in envelopes {
            let view = match envelope::verify(&env) {
                Ok(v) => v,
                Err(e) => {
                    self.stats.rejected.fetch_add(1, Ordering::Relaxed);
                    return Err(RoundError::Verify(format!("envelope at {next}: {e}")));
                }
            };
            let h = view.header;
            let reject = |why: String| {
                self.stats.rejected.fetch_add(1, Ordering::Relaxed);
                RoundError::Verify(why)
            };
            if h.partition != self.p {
                return Err(reject(format!("envelope of partition {}", h.partition.0)));
            }
            if h.base != next {
                return Err(reject(format!("envelope base {} != LEO {next}", h.base)));
            }
            if h.epoch < min_epoch || h.epoch > self.epoch {
                return Err(reject(format!(
                    "envelope epoch {} outside [{}, {}]",
                    h.epoch.0, min_epoch.0, self.epoch.0
                )));
            }
            for sha in blob::blob_refs_of(view.commands.iter().map(|c| c.as_ref())) {
                if !shas.contains(&sha) {
                    shas.push(sha);
                }
            }
            let end = h.base + u64::from(h.cmd_count);
            let bytes = bytes::Bytes::from(env);
            match batches.last_mut() {
                Some(last) if last.epoch == h.epoch.0 && last.end == h.base => {
                    last.end = end;
                    last.envelopes.push(bytes);
                }
                _ => batches.push(RawBatch {
                    base: h.base,
                    end,
                    epoch: h.epoch.0,
                    envelopes: vec![bytes],
                }),
            }
            min_epoch = h.epoch;
            next = end;
        }
        // F11: every referenced blob is local before the envelope can become durable here.
        for sha in shas {
            if blob::has_blob(&self.blobs, &sha) {
                continue;
            }
            if let Some(h) = &self.hooks.before_blob_fetch {
                h(sha).await;
            }
            let channel = self
                .transport
                .channel(self.leader)
                .map_err(|e| RoundError::Transport(e.to_string()))?;
            blob::fetch_blob_into(channel, &self.blobs, sha)
                .await
                .map_err(|e| RoundError::Blob(e.to_string()))?;
            self.stats.blobs_fetched.fetch_add(1, Ordering::Relaxed);
        }
        #[cfg(feature = "failpoints")]
        fail::fail_point!("fetch::before_append");
        let count: u64 = batches.iter().map(|b| b.envelopes.len() as u64).sum();
        let new_leo = self
            .log
            .append_replica(batches)
            .await
            .map_err(|e| RoundError::Log(e.to_string()))?;
        self.stats.envelopes.fetch_add(count, Ordering::Relaxed);
        #[cfg(feature = "failpoints")]
        fail::fail_point!("fetch::before_durable");
        if mutant(ostro_log::mutants::COUNT_NONDURABLE_LEO) {
            // Mutant: fetch again (reporting the LEO) without waiting for durability.
            return Ok(());
        }
        let mut durable = self.log.durable_end_watch();
        let mut health = self.log.health();
        tokio::select! {
            r = durable.wait_for(|d| *d >= new_leo) => {
                r.map_err(|_| RoundError::Log("durable watch closed".into()))?;
            }
            r = health.wait_for(|h| !matches!(h, ostro_log::LogHealth::Healthy)) => {
                let _ = r;
                warn!(partition = self.p.0, "follower log failed while waiting for durability");
                return Err(RoundError::Log("log failed".into()));
            }
        }
        Ok(())
    }
}
