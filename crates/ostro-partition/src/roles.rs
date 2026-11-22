//! Phase 12 role contracts (12-02): how a partition learns its role from meta and
//! hands follower work to the cluster layer.
//!
//! - [`PartitionLogHandle`]: the one concrete log type, named in exactly one alias line
//!   (12-08 renames the type by changing that line). Code outside `ostro-log` names the
//!   log only through this alias or the [`ReplicatedLog`](ostro_log::ReplicatedLog) trait.
//! - [`AssignmentSource`]: the meta-published assignment of every partition (12-10).
//! - [`FollowerDriver`] / [`FollowerTask`]: the fetcher loop for a followed partition (12-11).
//! - [`PartitionRoles`]: which partitions this node leads (bottomless uploader, 12-15).
//! - [`PartitionControl`] / [`InstallGuard`]: quiesce a partition for a snapshot install
//!   (12-12 / 12-13).
//!
//! The role lifecycle that drives these lands in 12-13 (`replica_services`).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use futures_util::future::BoxFuture;
use ostro_log::{LogError, LogNode, LogNodeConfig};
use ostro_types::{Epoch, Offset, PartitionCount, PartitionId};
use tokio::sync::watch;

use crate::handle::PartitionError;
use crate::store::StoreConfig;

/// The concrete partition log handle `Node` holds. The only line that names the type.
pub type PartitionLogHandle = ostro_log::Rf1Log;

/// Test/fixture constructor next to the alias (W6): opens a one-node log for
/// `partition` (partition count `partition + 1`) under `wal_dir`, anchored at `floor`
/// when empty. Keep the returned [`LogNode`] alive while using the log and call
/// [`LogNode::shutdown`] at the end. Fixtures use this, never the type name. The log opens
/// unassigned (role `Offline`, HW = `floor`); call `become_leader` / `become_follower`.
#[doc(hidden)]
pub async fn open_partition_log_for_tests(
    wal_dir: &Path,
    partition: PartitionId,
    node_id: u64,
    floor: Offset,
) -> Result<(LogNode, Arc<PartitionLogHandle>), LogError> {
    let n = partition
        .0
        .checked_add(1)
        .and_then(PartitionCount::new)
        .ok_or_else(|| LogError::Failed(format!("partition {} out of range", partition.0)))?;
    let cfg = LogNodeConfig::new(wal_dir.to_path_buf(), n, node_id);
    let floors = vec![floor; usize::from(n.get())];
    let (node, mut logs) = LogNode::open(cfg, &floors).await?;
    let log = logs.swap_remove(usize::from(partition.0))?;
    Ok((node, log))
}

/// One partition's meta-published assignment, as the role lifecycle sees it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionAssignmentView {
    /// The partition.
    pub partition: PartitionId,
    /// The leader (0 = none / offline).
    pub leader: u64,
    /// Leader epoch (0 = unplaced / offline, D-P12-14).
    pub epoch: Epoch,
    /// Every assigned replica.
    pub replicas: Vec<u64>,
    /// The published ISR.
    pub isr: Vec<u64>,
    /// CAS token of `isr`.
    pub isr_version: u64,
    /// Minimum ISR size.
    pub min_insync: u8,
}

/// The meta-published assignments plus the two requests a partition makes of the
/// controller (12-10).
pub trait AssignmentSource: Send + Sync + 'static {
    /// Every partition's current assignment; changes on each meta apply that touches one.
    fn subscribe(&self) -> watch::Receiver<Arc<Vec<PartitionAssignmentView>>>;

    /// This node's id.
    fn local_node(&self) -> u64;

    /// Ask for a fresh term for this node at an epoch `>= min_epoch` (12-10
    /// `request_fresh_term`; a no-op at epoch 0). Resolves once proposed.
    fn request_term(&self, p: PartitionId, min_epoch: Epoch) -> BoxFuture<'_, ()>;

    /// Step down as leader of `p` at `epoch` (`ResignLeader`; used when the log turns
    /// `LogHealth::Failed`, D-P12-9). Resolves once proposed.
    fn resign(&self, p: PartitionId, epoch: Epoch) -> BoxFuture<'_, ()>;
}

/// Starts the fetcher for a followed partition (12-11).
pub trait FollowerDriver: Send + Sync + 'static {
    /// Follow `leader` at `epoch`, appending into `log`.
    fn start(
        &self,
        p: PartitionId,
        epoch: Epoch,
        leader: u64,
        log: Arc<PartitionLogHandle>,
    ) -> Box<dyn FollowerTask>;
}

/// A running fetcher.
pub trait FollowerTask: Send {
    /// Stop fetching; resolves once the task no longer touches the log.
    fn stop(self: Box<Self>) -> BoxFuture<'static, ()>;
}

/// The partitions this node currently leads, with their epochs.
pub trait PartitionRoles: Send + Sync + 'static {
    /// `(partition, epoch)` of every led partition.
    fn led(&self) -> watch::Receiver<Arc<Vec<(PartitionId, Epoch)>>>;
}

/// Partition lifecycle control for a snapshot install (D-P12-8).
pub trait PartitionControl: Send + Sync + 'static {
    /// Stop the partition's apply machine and services and close its state store, so the
    /// installer may reset the log and replace the state directory.
    fn quiesce_for_install(
        &self,
        p: PartitionId,
    ) -> BoxFuture<'_, Result<InstallGuard, PartitionError>>;
}

type ReopenFn = Box<dyn FnOnce() -> BoxFuture<'static, Result<(), PartitionError>> + Send>;

/// A quiesced partition: its store is closed, its log is idle. Dropping the guard
/// without [`reopen`](Self::reopen) leaves the partition stopped.
pub struct InstallGuard {
    /// The data dir root that holds `state/` (Phase 8 `install_snapshot`'s `data_dir`).
    pub state_root: PathBuf,
    /// The partition's log (for `reset_to`).
    pub log: Arc<PartitionLogHandle>,
    /// The store tuning to reopen with.
    pub store_cfg: StoreConfig,
    reopen: ReopenFn,
}

impl InstallGuard {
    /// Build a guard; `reopen` reopens the store and restarts the partition. It must
    /// also run the derived-keyspace rebuild hook the normal startup runs
    /// (`PartitionStore::start_run_index_rebuild`), since `x_*` keyspaces are outside the
    /// snapshot's digest scope.
    pub fn new(
        state_root: PathBuf,
        log: Arc<PartitionLogHandle>,
        store_cfg: StoreConfig,
        reopen: impl FnOnce() -> BoxFuture<'static, Result<(), PartitionError>> + Send + 'static,
    ) -> Self {
        Self {
            state_root,
            log,
            store_cfg,
            reopen: Box::new(reopen),
        }
    }

    /// Reopen the partition (after the install, or after an aborted one).
    pub fn reopen(self) -> BoxFuture<'static, Result<(), PartitionError>> {
        (self.reopen)()
    }
}

impl std::fmt::Debug for InstallGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InstallGuard")
            .field("state_root", &self.state_root)
            .field(
                "partition",
                &ostro_log::ReplicatedLog::partition(&*self.log),
            )
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use ostro_log::ReplicatedLog;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[tokio::test]
    async fn fixture_log_opens_and_guard_reopens() {
        let dir = tempfile::tempdir().unwrap();
        let (node, log) =
            open_partition_log_for_tests(&dir.path().join("wal"), PartitionId(2), 1, Offset(5))
                .await
                .unwrap();
        assert_eq!(log.partition(), PartitionId(2));
        assert_eq!(log.log_start(), Offset(5));
        // The fixture opens unassigned (role Offline); lead it the RF=1 way.
        log.become_leader(Epoch(1), ostro_log::ReplicaSet::solo(1))
            .unwrap();
        let r = log
            .append(Epoch(1), vec![Bytes::from_static(b"a")])
            .await
            .unwrap();
        assert_eq!(r.base, Offset(5));
        log.wait_committed(r.last).await.unwrap();

        let reopened = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&reopened);
        let guard = InstallGuard::new(
            dir.path().to_path_buf(),
            Arc::clone(&log),
            StoreConfig::default(),
            move || {
                Box::pin(async move {
                    flag.store(true, Ordering::SeqCst);
                    Ok(())
                })
            },
        );
        assert!(format!("{guard:?}").contains("InstallGuard"));
        guard.reopen().await.unwrap();
        assert!(reopened.load(Ordering::SeqCst));
        node.shutdown().await;
    }

    struct Fixed(watch::Sender<Arc<Vec<PartitionAssignmentView>>>);
    impl AssignmentSource for Fixed {
        fn subscribe(&self) -> watch::Receiver<Arc<Vec<PartitionAssignmentView>>> {
            self.0.subscribe()
        }
        fn local_node(&self) -> u64 {
            1
        }
        fn request_term(&self, _p: PartitionId, _min_epoch: Epoch) -> BoxFuture<'_, ()> {
            Box::pin(async {})
        }
        fn resign(&self, _p: PartitionId, _epoch: Epoch) -> BoxFuture<'_, ()> {
            Box::pin(async {})
        }
    }

    #[tokio::test]
    async fn assignment_source_is_object_safe() {
        let view = PartitionAssignmentView {
            partition: PartitionId(0),
            leader: 1,
            epoch: Epoch(1),
            replicas: vec![1],
            isr: vec![1],
            isr_version: 1,
            min_insync: 1,
        };
        let src: Arc<dyn AssignmentSource> =
            Arc::new(Fixed(watch::channel(Arc::new(vec![view.clone()])).0));
        assert_eq!(src.subscribe().borrow()[0], view);
        assert_eq!(src.local_node(), 1);
        src.request_term(PartitionId(0), Epoch(2)).await;
        src.resign(PartitionId(0), Epoch(1)).await;
        fn _object_safe(
            _: &dyn FollowerDriver,
            _: Box<dyn FollowerTask>,
            _: &dyn PartitionRoles,
            _: &dyn PartitionControl,
        ) {
        }
    }
}
