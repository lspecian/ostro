//! The `ReplicatedLog` trait: the crate boundary between `ostro-log` and `ostro-partition`
//! (STOR-04, research Pattern 1).
//!
//! RF=1 ([`Rf1Log`](crate::Rf1Log)) implements it now with ISR = {self}; the Phase 12 cluster
//! implements the same trait. Offsets are **exclusive ends** everywhere (HW, LEO, log start
//! follow Kafka semantics) and `log_start <= HW <= LEO` always holds.

use std::future::Future;

use bytes::Bytes;
use ostro_types::envelope;
use ostro_types::{Epoch, Offset, PartitionId};
use tokio::sync::watch;

use crate::barrier::TrimPermit;
use crate::epoch_cache::EpochCache;
use crate::error::LogError;
use crate::verify::VerifiedBatch;

/// Position of an appended batch (fixed in the log, not necessarily durable yet).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct AppendReceipt {
    /// First offset of the batch.
    pub base: Offset,
    /// Last offset of the batch (inclusive).
    pub last: Offset,
    /// Epoch stamped on the envelope.
    pub epoch: Epoch,
}

/// In-sync replica set (node ids). RF=1: exactly the local node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Isr(pub Vec<u64>);

/// This replica's role for the partition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Role {
    /// Accepting appends under `epoch`.
    Leader {
        /// Current leader epoch.
        epoch: Epoch,
    },
    /// Following a leader of `epoch` (Phase 12).
    Follower {
        /// Leader epoch being followed.
        epoch: Epoch,
    },
    /// Not serving (not yet elected, or failed).
    Offline,
}

/// Partition log health. `Failed` is terminal until the node reopens the log (STOR-07).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogHealth {
    /// Serving.
    Healthy,
    /// Fail-stop after an I/O or verification failure.
    Failed {
        /// What failed.
        reason: String,
    },
}

/// Raw envelopes of consecutive batches written under one epoch (bottomless upload, P8-D3).
///
/// `envelopes` are the stored OSTR envelope bytes, one per log batch, in offset order,
/// byte-identical to `envelope::encode` of the batch's commands. Batch boundaries are
/// preserved (one element per batch, never merged or split), and a `RawBatch` never spans
/// an epoch change: [`ReplicatedLog::read_envelopes`] starts a new one when the epoch moves.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RawBatch {
    /// First offset of the first envelope.
    pub base: Offset,
    /// Exclusive end of the last envelope.
    pub end: Offset,
    /// Leader epoch every envelope here was written under.
    pub epoch: u32,
    /// The envelopes, one per batch, contiguous: each starts where the previous ended.
    pub envelopes: Vec<Bytes>,
}

/// Append one batch's envelope (`[base, end)`, written under `epoch`) to `out`, extending
/// the last [`RawBatch`] when it has the same epoch and ends at `base`.
pub(crate) fn push_raw(
    out: &mut Vec<RawBatch>,
    epoch: Epoch,
    base: Offset,
    end: Offset,
    env: Bytes,
) {
    if let Some(last) = out.last_mut()
        && last.epoch == epoch.0
        && last.end == base
    {
        last.end = end;
        last.envelopes.push(env);
        return;
    }
    out.push(RawBatch {
        base,
        end,
        epoch: epoch.0,
        envelopes: vec![env],
    });
}

/// Snapshot of the leader-epoch history: each epoch with the first offset it wrote, ascending.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EpochHistory(pub Vec<(Epoch, Offset)>);

impl EpochHistory {
    /// The latest epoch in the log, if any batch was written.
    pub fn latest(&self) -> Option<Epoch> {
        self.0.last().map(|(e, _)| *e)
    }

    /// Where an **earlier** epoch `e` ended (the start of the next known epoch).
    ///
    /// `None` for the latest epoch (it has not ended; its end is the current LEO, see
    /// [`end_offset_for_epoch_at`](Self::end_offset_for_epoch_at)) and for epochs newer than
    /// the history or older than the retained history (Kafka UNDEFINED).
    pub fn end_offset_for_epoch(&self, e: Epoch) -> Option<Offset> {
        if self.latest() == Some(e) {
            return None;
        }
        self.cache()
            .end_offset_for_epoch(e, Offset::ZERO)
            .map(|(_, o)| o)
    }

    /// Kafka `OffsetForLeaderEpoch` answer given the log end offset: `(epoch, end)` or
    /// `None` (UNDEFINED).
    pub fn end_offset_for_epoch_at(&self, e: Epoch, log_end: Offset) -> Option<(Epoch, Offset)> {
        self.cache().end_offset_for_epoch(e, log_end)
    }

    fn cache(&self) -> EpochCache {
        EpochCache::from_pairs(&self.0)
    }
}

/// A partition's replicated log.
pub trait ReplicatedLog: Send + Sync + 'static {
    /// The partition.
    fn partition(&self) -> PartitionId;

    /// Leader append. Under the sequencer: reject `epoch < current` ([`LogError::StaleEpoch`]),
    /// assign `[base, last]`, build the envelope, enqueue to the WAL. Returns once the batch's
    /// position is fixed (enqueued), **not** once it is durable; see
    /// [`wait_committed`](Self::wait_committed).
    fn append(
        &self,
        epoch: Epoch,
        cmds: Vec<Bytes>,
    ) -> impl Future<Output = Result<AppendReceipt, LogError>> + Send;

    /// Resolves `Ok` only once HW > `last`; `Err` if the partition failed or `last` was
    /// truncated away. Never `Ok` for data that is not durable on the ISR.
    fn wait_committed(&self, last: Offset) -> impl Future<Output = Result<(), LogError>> + Send;

    /// High watermark (exclusive), published after durability.
    fn high_watermark(&self) -> watch::Receiver<Offset>;

    /// Log end offset (exclusive).
    fn log_end(&self) -> Offset;

    /// First retained offset.
    fn log_start(&self) -> Offset;

    /// Verified batches fully inside `[from, min(upto, HW))`, starting at `from` (a batch
    /// boundary), up to about `max_bytes` (at least one batch when one fits).
    fn read(
        &self,
        from: Offset,
        upto: Offset,
        max_bytes: usize,
    ) -> impl Future<Output = Result<Vec<VerifiedBatch>, LogError>> + Send;

    /// The stored envelope bytes of whole batches inside `[from, HW)`, starting at `from` (a
    /// batch boundary), up to about `max_bytes` (at least one batch when one fits), grouped
    /// into [`RawBatch`]es that never span an epoch change. Every batch is verified like
    /// [`read`](Self::read). HW-capped: never returns data that is not committed.
    ///
    /// The default re-encodes [`read`](Self::read)'s verified batches (envelope encoding is
    /// deterministic, so this is byte-identical to the stored bytes); implementations that
    /// hold the raw bytes return them directly ([`Rf1Log`](crate::Rf1Log) does).
    fn read_envelopes(
        &self,
        from: Offset,
        max_bytes: usize,
    ) -> impl Future<Output = Result<Vec<RawBatch>, LogError>> + Send {
        async move {
            let batches = self.read(from, Offset(u64::MAX), max_bytes).await?;
            let p = self.partition();
            let mut out = Vec::new();
            for b in batches {
                let env = Bytes::from(envelope::encode(p, b.epoch, b.base, &b.commands));
                push_raw(&mut out, b.epoch, b.base, b.end(), env);
            }
            Ok(out)
        }
    }

    /// Leader-epoch history snapshot.
    fn epochs(&self) -> EpochHistory;

    /// In-sync replicas (RF=1: `{self}`, never assumed).
    fn isr(&self) -> Isr;

    /// Role watch.
    fn role(&self) -> watch::Receiver<Role>;

    /// Health watch.
    fn health(&self) -> watch::Receiver<LogHealth>;

    /// Drop the suffix at and above `end`; returns the new LEO (may be below `end` when
    /// `end` falls inside a batch).
    fn truncate_to(&self, end: Offset) -> impl Future<Output = Result<Offset, LogError>> + Send;

    /// Delete whole segments entirely below the permit's floor; returns the new log start.
    fn trim_prefix(
        &self,
        permit: TrimPermit,
    ) -> impl Future<Output = Result<Offset, LogError>> + Send;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_history_queries() {
        let h = EpochHistory(vec![(Epoch(1), Offset(0)), (Epoch(3), Offset(10))]);
        assert_eq!(h.latest(), Some(Epoch(3)));
        assert_eq!(h.end_offset_for_epoch(Epoch(1)), Some(Offset(10)));
        assert_eq!(h.end_offset_for_epoch(Epoch(2)), Some(Offset(10)));
        assert_eq!(h.end_offset_for_epoch(Epoch(3)), None);
        assert_eq!(h.end_offset_for_epoch(Epoch(9)), None);
        assert_eq!(
            h.end_offset_for_epoch_at(Epoch(3), Offset(25)),
            Some((Epoch(3), Offset(25)))
        );
        assert_eq!(EpochHistory::default().latest(), None);
    }
}
