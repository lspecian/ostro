//! Oracle `standby` (12-03): replicas of a partition that applied the same prefix hold the
//! same state.
//!
//! Each node's state store (`<data>/state/pNNNN`) is copied to a temp dir and opened there
//! (fjall has no read-only mode; a copy never disturbs the node and never takes its lock).
//! Measured per node: `applied_offset` and `state_hash` from the store's meta keyspace, the
//! logical digest (`PartitionStore::digest`, STOR-06) and a sha256 per table over every row in
//! key order (the meta row `checkpointed_offset` excluded, as in the logical digest), so a
//! divergence names its table.
//!
//! Strict by design: nodes at different `applied_offset`s are reported too ("not at an equal
//! applied offset"); a scenario waits until the replicas quiesce at the same offset before it
//! calls the oracle.

use std::collections::BTreeMap;

use ostro_partition::store::meta::K_CHECKPOINTED;
use ostro_partition::store::{PartitionStore, StoreConfig, TABLES, Table, state_dir};
use ostro_types::{Offset, PartitionCount, PartitionId};
use sha2::{Digest, Sha256};

use super::{Report, copy_dir};
use crate::harness::DataDirView;

/// One node's measured state of a partition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StateDigest {
    /// Node id.
    pub node: u64,
    /// `applied_offset`.
    pub applied: Offset,
    /// Rolling `state_hash`.
    pub state_hash: [u8; 32],
    /// `PartitionStore::digest()`.
    pub logical: [u8; 32],
    /// sha256 per table name.
    pub tables: BTreeMap<&'static str, [u8; 32]>,
}

/// Measure partition `p` of `view` (on a copy of its state dir).
pub fn measure(
    view: &DataDirView,
    p: PartitionId,
    count: PartitionCount,
) -> Result<StateDigest, String> {
    let src = state_dir(&view.root, p);
    if !src.is_dir() {
        return Err(format!("{} does not exist", src.display()));
    }
    let tmp = tempfile::tempdir().map_err(|e| format!("tempdir: {e}"))?;
    copy_dir(&src, &state_dir(tmp.path(), p))
        .map_err(|e| format!("copy {}: {e}", src.display()))?;
    let (store, meta) = PartitionStore::open(tmp.path(), p, count, &StoreConfig::default())
        .map_err(|e| format!("open copy of {}: {e}", src.display()))?;
    let logical = store.digest().map_err(|e| format!("digest: {e}"))?;
    let reader = store.reader();
    let mut tables = BTreeMap::new();
    for t in TABLES {
        let rows = reader
            .prefix(t, &[], usize::MAX)
            .map_err(|e| format!("read {}: {e}", t.name()))?;
        let mut h = Sha256::new();
        for (k, v) in rows {
            if t == Table::Meta && k == K_CHECKPOINTED {
                continue;
            }
            h.update((k.len() as u64).to_le_bytes());
            h.update(&k);
            h.update((v.len() as u64).to_le_bytes());
            h.update(&v);
        }
        tables.insert(t.name(), h.finalize().into());
    }
    drop(reader);
    drop(store);
    Ok(StateDigest {
        node: view.node,
        applied: meta.applied,
        state_hash: meta.state_hash,
        logical,
        tables,
    })
}

/// Every view's state of partition `p` is equal to the first view's.
pub fn equal_state(views: &[DataDirView], p: PartitionId, count: PartitionCount) -> Report {
    let mut report = Report::new("standby");
    let mut digests = Vec::new();
    for v in views {
        match measure(v, p, count) {
            Ok(d) => digests.push(d),
            Err(e) => report.violation(format!("node {} partition {}: {e}", v.node, p.0)),
        }
    }
    report.violations.extend(compare(&digests, p).violations);
    report
}

/// Compare measured digests of one partition.
pub fn compare(digests: &[StateDigest], p: PartitionId) -> Report {
    let mut report = Report::new("standby");
    let Some(first) = digests.first() else {
        return report;
    };
    for d in &digests[1..] {
        if d.applied != first.applied {
            report.violation(format!(
                "partition {}: node {} applied {} and node {} applied {}: not at an equal applied offset",
                p.0, first.node, first.applied.0, d.node, d.applied.0
            ));
            continue;
        }
        if d.state_hash != first.state_hash {
            report.violation(format!(
                "partition {} at applied {}: state_hash differs between node {} and node {}",
                p.0, d.applied.0, first.node, d.node
            ));
        }
        let differing: Vec<&str> = first
            .tables
            .iter()
            .filter(|(t, h)| d.tables.get(*t) != Some(*h))
            .map(|(t, _)| *t)
            .collect();
        if d.logical != first.logical || !differing.is_empty() {
            report.violation(format!(
                "partition {} at applied {}: logical state differs between node {} and node {} (tables {differing:?})",
                p.0, d.applied.0, first.node, d.node
            ));
        }
    }
    report
}
