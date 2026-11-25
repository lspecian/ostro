//! Apply-outcome streams (13-18): the [`OutcomeRecord`] producer for
//! `oracle::p13_upgrade::no_reject_continue` (13-08 / 13-10 handover: nothing built them).
//!
//! [`partition_outcomes`] replays one node's verified partition WAL (a [`LogScan`], the
//! measured on-disk bytes) through this build's partition apply ([`apply_command`], the
//! function every replica's apply loop runs) on a scratch store, in offset order, with each
//! record's envelope epoch. Every applied command yields one record with its verdict. A
//! fail-stop (`StoreError`, e.g. `NewerFormat`: a command written by a newer Ostro, P13-F1
//! rule 2) ends the stream, as it halts the real partition; [`OutcomeStream::halted`] says
//! where and why.
//!
//! The replay is this process's apply: `ostro_types::compat::level_max()` and the
//! `upgrade::mutant::reject_and_continue` failpoint are process-wide, exactly as for the
//! in-process nodes. A trimmed log (retention cut its head, or the copy came from a
//! snapshot install) cannot be replayed from offset 0 and yields no stream.

use ostro_partition::apply::{Action, Cursor, apply_command};
use ostro_partition::{PartitionStore, StoreConfig};
use ostro_types::{Offset, PartitionCount};

use crate::oracle::disk::LogScan;
use crate::oracle::p13_upgrade::OutcomeRecord;

/// One node's partition outcome stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutcomeStream {
    /// One record per applied command, in offset order.
    pub records: Vec<OutcomeRecord>,
    /// `(offset, error)` of the fail-stop that ended the stream, if any.
    pub halted: Option<(u64, String)>,
}

/// Replay `scan` (module docs). `count` is the cluster's partition count. `Err` when the
/// log is trimmed or the scratch store cannot be opened.
pub fn partition_outcomes(scan: &LogScan, count: PartitionCount) -> Result<OutcomeStream, String> {
    if scan.is_trimmed() {
        return Err(format!(
            "node {} partition {}: log starts at {:?}, cannot replay from 0",
            scan.node,
            scan.partition.0,
            scan.log_start()
        ));
    }
    let dir = tempfile::tempdir().map_err(|e| format!("scratch dir: {e}"))?;
    let (store, meta) =
        PartitionStore::open(dir.path(), scan.partition, count, &StoreConfig::default())
            .map_err(|e| format!("scratch store: {e}"))?;
    let mut cur = Cursor::from_meta(&meta);
    let mut b = store.batch();
    let mut actions: Vec<Action> = Vec::new();
    let mut out = OutcomeStream {
        records: Vec::new(),
        halted: None,
    };
    for rec in &scan.records {
        for (i, bytes) in rec.commands.iter().enumerate() {
            let off: Offset = rec.base + i as u64;
            match apply_command(&mut b, &mut cur, off, rec.epoch, bytes, &mut actions) {
                Ok(outcome) => out.records.push(OutcomeRecord {
                    node: scan.node,
                    partition: Some(scan.partition.0),
                    offset: off.0,
                    command: bytes.clone(),
                    outcome,
                }),
                Err(e) => {
                    out.halted = Some((off.0, e.to_string()));
                    return Ok(out);
                }
            }
            actions.clear();
        }
    }
    Ok(out)
}
