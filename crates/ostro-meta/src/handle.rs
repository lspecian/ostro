//! [`MetaHandle`]: starts the single-voter metadata Raft group and offers
//! propose / read / watch with read-your-writes on one node (META-01).

use std::path::Path;
use std::time::Duration;

use crate::command::{self, DecodeError, MetaCommand, MetaResult, decode_result, encode_command};
use crate::records::ClusterInfo;
use crate::sm::{MetaSm, MetaView};
use crate::{DriverConfig, NullOutbox, ProposeError, RaftDriver, RaftHandle};

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
                Err(ProposeError::Stopped) => {
                    return Err(MetaError::Propose(self.stopped_reason().await));
                }
                Err(e) => return Err(MetaError::Propose(e.to_string())),
            }
        }
    }

    /// The driver's storage failure, if it stopped on one (`RaftStatus::failed`, e.g.
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
