//! [`MetaHandle`]: starts the single-voter metadata Raft group and offers
//! propose / read / watch with read-your-writes on one node (META-01).

use std::path::Path;
use std::time::Duration;

use crate::command::{DecodeError, MetaCommand, MetaResult, decode_result, encode_command};
use crate::records::ClusterInfo;
use crate::sm::{MetaSm, MetaView};
use crate::{DriverConfig, NullOutbox, ProposeError, RaftDriver, RaftHandle};

/// How long `start` waits for this node to become leader and catch up.
const START_TIMEOUT: Duration = Duration::from_secs(5);
/// Retries of a proposal whose outcome is unknown (leadership lost mid-flight).
const OUTCOME_UNKNOWN_RETRIES: u32 = 5;
const OUTCOME_UNKNOWN_BACKOFF: Duration = Duration::from_millis(50);

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
}

/// A running single-voter metadata group.
pub struct MetaHandle {
    /// `Some` until [`MetaHandle::stop`]; dropping the handle also stops the
    /// driver (blocking the dropping thread on the join).
    driver: Option<RaftDriver>,
    raft: RaftHandle,
    view: MetaView,
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
    pub async fn start(dir: &Path, node_id: u64) -> Result<MetaHandle, MetaError> {
        let (sm, view) = MetaSm::new();
        let driver = RaftDriver::start(dir, DriverConfig::new(node_id), &[node_id], sm, NullOutbox)
            .map_err(|e| MetaError::Start(e.to_string()))?;
        let raft = driver.handle();
        raft.campaign();
        let handle = MetaHandle {
            driver: Some(driver),
            raft,
            view,
        };
        match tokio::time::timeout(START_TIMEOUT, handle.wait_leader_caught_up()).await {
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
                handle.stop().await;
                Err(e)
            }
            Err(_) => {
                handle.stop().await;
                Err(MetaError::Timeout)
            }
        }
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
    pub async fn propose(&self, cmd: MetaCommand) -> Result<MetaResult, MetaError> {
        let bytes = encode_command(&cmd);
        let mut attempt = 0;
        loop {
            match self.raft.propose(bytes.clone()).await {
                Ok(applied) => {
                    return decode_result(&applied.result)
                        .map_err(|e| MetaError::Decode(e.to_string()));
                }
                Err(ProposeError::OutcomeUnknown) if attempt < OUTCOME_UNKNOWN_RETRIES => {
                    attempt += 1;
                    tracing::warn!(attempt, "metadata proposal outcome unknown; retrying");
                    tokio::time::sleep(OUTCOME_UNKNOWN_BACKOFF).await;
                }
                Err(e) => return Err(MetaError::Propose(e.to_string())),
            }
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

    /// The underlying Raft handle (status, subscribe).
    pub fn raft(&self) -> &RaftHandle {
        &self.raft
    }

    /// Stop the driver, joining its thread off the async runtime.
    pub async fn stop(mut self) {
        if let Some(driver) = self.driver.take() {
            let _ = tokio::task::spawn_blocking(move || driver.stop()).await;
        }
    }
}
