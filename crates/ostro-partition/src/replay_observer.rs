//! Per-offset replay observer (plan 14-29, D-P14-45): a read-only, additive hook for
//! whole-cluster restore and tests.
//!
//! Restoring a cluster bucket needs, per partition, the cross-partition cursors
//! (`outbox_next_seq[->q]`, `outbox_truncated[->q]`, `inbound_seen[<-q]`) **at every offset** of
//! the replayed chain, to search a cut that satisfies Phase 8 Pitfall 1 for every pair
//! (`ostro_backup::cluster_cut`). [`replay_with_observer`] replays `[applied, upto)` of a log
//! into its store exactly like the apply machine does (verified batches, [`apply_command`] in
//! offset order into one `ApplyBatch` per read chunk, committed with `Cursor::meta_update`,
//! actions discarded as in Inspect mode) and hands the cursor to a [`ReplayObserver`] after
//! every applied command. Apply semantics, the state digest, `TABLES` and the storage format
//! are untouched: the observer only reads the cursor.
//!
//! [`crate::Node::open`] runs it in [`crate::StartMode::Inspect`] when
//! [`crate::NodeConfig::replay_observer`] yields an observer for the partition (after the
//! Inspect term raised the HW to the durable end, before the apply machine starts, which then
//! finds nothing left to replay). Without an observer nothing changes.
//!
//! Offsets reported to the observer are cut positions: after applying the command at offset
//! `o` the observer sees `at = o + 1 = cursor.next`; [`ReplayObserver::start`] reports the
//! state the replay starts from (`at = applied`).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};

use ostro_log::ReplicatedLog;
use ostro_types::{Offset, PartitionId};

use crate::apply::{Cursor, apply_command};
use crate::store::PartitionStore;

/// Observes a replay command by command.
pub trait ReplayObserver: Send {
    /// The state the replay starts from (`at` = the store's applied offset). Defaults to
    /// [`Self::after_apply`].
    fn start(&mut self, at: Offset, cursor: &Cursor) {
        self.after_apply(at, cursor);
    }

    /// The cursor after applying every command below `at` (`at == cursor.next`).
    fn after_apply(&mut self, at: Offset, cursor: &Cursor);
}

/// Yields the observer of a partition's Inspect-mode replay (`None` = no observer).
pub type ReplayObserverFactory =
    Arc<dyn Fn(PartitionId) -> Option<Box<dyn ReplayObserver>> + Send + Sync>;

/// The cross-partition cursors at one cut position.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CursorSample {
    /// The cut position (`cursor.next`).
    pub at: u64,
    /// `outbox_next_seq` by target partition.
    pub next: BTreeMap<u16, u64>,
    /// `outbox_truncated` by target partition.
    pub truncated: BTreeMap<u16, u64>,
    /// `inbound_seen` by source partition.
    pub seen: BTreeMap<u16, u64>,
}

impl CursorSample {
    /// The sample of `cursor` at `at`.
    pub fn of(at: Offset, cursor: &Cursor) -> Self {
        Self {
            at: at.0,
            next: cursor.meta.outbox_next_seq.clone(),
            truncated: cursor.meta.outbox_truncated.clone(),
            seen: cursor.inbound_seen.clone(),
        }
    }

    fn same_cursors(&self, cursor: &Cursor) -> bool {
        self.next == cursor.meta.outbox_next_seq
            && self.truncated == cursor.meta.outbox_truncated
            && self.seen == cursor.inbound_seen
    }
}

/// An observer that records a [`CursorSample`] at the start and whenever a cross-partition
/// cursor changed. Clones share the samples.
#[derive(Clone, Debug, Default)]
pub struct CursorRecorder {
    samples: Arc<Mutex<Vec<CursorSample>>>,
}

impl CursorRecorder {
    /// An empty recorder.
    pub fn new() -> Self {
        Self::default()
    }

    /// The samples recorded so far, ascending by `at`.
    pub fn samples(&self) -> Vec<CursorSample> {
        self.samples
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl ReplayObserver for CursorRecorder {
    fn start(&mut self, at: Offset, cursor: &Cursor) {
        self.samples
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(CursorSample::of(at, cursor));
    }

    fn after_apply(&mut self, at: Offset, cursor: &Cursor) {
        let mut s = self.samples.lock().unwrap_or_else(PoisonError::into_inner);
        if s.last().is_some_and(|l| l.same_cursors(cursor)) {
            return;
        }
        s.push(CursorSample::of(at, cursor));
    }
}

/// One [`CursorRecorder`] per partition, created on demand by [`Self::factory`].
#[derive(Clone, Debug, Default)]
pub struct CursorRecorders {
    by_partition: Arc<Mutex<BTreeMap<u16, CursorRecorder>>>,
}

impl CursorRecorders {
    /// No recorders yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// A factory for [`crate::NodeConfig::replay_observer`] recording every partition.
    pub fn factory(&self) -> ReplayObserverFactory {
        let me = self.clone();
        Arc::new(move |p: PartitionId| {
            let r = me
                .by_partition
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .entry(p.0)
                .or_default()
                .clone();
            Some(Box::new(r) as Box<dyn ReplayObserver>)
        })
    }

    /// The samples of partition `p` (empty if it was not replayed).
    pub fn samples(&self, p: PartitionId) -> Vec<CursorSample> {
        self.by_partition
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&p.0)
            .map(CursorRecorder::samples)
            .unwrap_or_default()
    }
}

/// Replay `[applied, upto)` of `log` into `store` (see the module docs), reporting the cursor
/// to `observer` after every command, then checkpoint. Returns the final cursor.
pub async fn replay_with_observer<L: ReplicatedLog>(
    store: &PartitionStore,
    log: &L,
    upto: Offset,
    max_chunk_bytes: usize,
    observer: &mut dyn ReplayObserver,
) -> Result<Cursor, String> {
    let meta = store
        .read_meta()
        .map_err(|e| format!("observed replay: read meta: {e}"))?;
    let mut cur = Cursor::from_meta(&meta);
    observer.start(cur.next, &cur);
    while cur.next < upto {
        let batches = log
            .read(cur.next, upto, max_chunk_bytes)
            .await
            .map_err(|e| format!("observed replay: log read [{}, {upto}): {e}", cur.next))?;
        let Some(first) = batches.first() else {
            return Err(format!(
                "observed replay: empty log read below {upto} at {}",
                cur.next
            ));
        };
        if first.base != cur.next {
            return Err(format!(
                "observed replay: log read returned base {}, expected {}",
                first.base, cur.next
            ));
        }
        let mut b = store.batch();
        let mut actions = Vec::new();
        for vb in &batches {
            for (off, bytes) in vb.commands_with_offsets() {
                apply_command(&mut b, &mut cur, off, vb.epoch, bytes, &mut actions)
                    .map_err(|e| format!("observed replay: apply failed at {off}: {e}"))?;
                // Replayed actions are never emitted (Inspect).
                actions.clear();
                observer.after_apply(cur.next, &cur);
            }
        }
        let mu = cur.meta_update();
        b.commit(&mu)
            .map_err(|e| format!("observed replay: state commit failed: {e}"))?;
    }
    store
        .checkpoint(cur.next)
        .map_err(|e| format!("observed replay: checkpoint failed: {e}"))?;
    Ok(cur)
}
