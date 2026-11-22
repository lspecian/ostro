//! Exhaustive blob-reference visitor over every [`Table`] (plan 08-02).
//!
//! Used by the snapshot `BlobRefs` frame (P8-D1), the blob GC mark phase (P8-D8, 08-19)
//! and the bottomless uploader (08-13).
//!
//! **BlobRef invariant (Phase 8 F5, docs/storage-format.md §14.2).** A `BlobRef` appears only
//! in a `PayloadRef` field of a prost message or of a documented state value. It is never
//! inferred from opaque payload bytes, so this visitor decodes only prost `PayloadRef`
//! fields and documented postcard values, and never looks inside journal event payloads,
//! inline payload bytes or module code.
//!
//! [`visit_row`] matches on [`Table`] **without a `_` arm**: adding a keyspace does not
//! compile until its blob references are declared here.
//!
//! Where the references live, per table:
//!
//! | table | refs |
//! |---|---|
//! | `meta` | none (fixed ASCII keys, integers, hashes, postcard `CursorMeta`) |
//! | `runs` | none. `RunRecord.module_sha` names a *module*, which meta `Module` records own (module blobs are never collected by run state; Phase 13 owns module GC) |
//! | `events` | prost `JournalEvent.payload` (`PayloadRef`) |
//! | `payloads` | the prost `PayloadRef` row itself (run input / result) |
//! | `ready`, `timers` | none |
//! | `inbox` | signal rows: postcard `SignalSlot::Pending.event` (prost `PayloadRef`); legacy rows: `InboxMsg::ChildCompleted.result` (prost `PayloadRef` encoding, empty = none) |
//! | `dedupe` | none (run id) |
//! | `outbox` | prost `OutboxMsg`: `StartChild.start` / `CronStart.start` / `StartRun` (`input`, `run_started.payload`), `ChildCompleted.result` and `.event`, `Signal.payload`; `CancelRun` none |
//! | `inbound_seen`, `children` | none |
//! | `dlq` | none (no rows written) |
//! | `effects` | none (Phase 6 `EffectRow{kind, scheduled_at_ms}` carries no payload; RESERVATIONS §6) |
//! | `running`, `waits`, `signal_seen` | none |
//! | `schedule_state` | `0x01` rows: `ScheduleStateRow.buffered.start` (prost `StartRun`: `input`, `run_started.payload`); `0x02` rows none |

use std::collections::BTreeSet;

use fjall::Readable;
use ostro_types::PayloadRef;
use ostro_types::command::{JournalEvent, OutboxMsg, StartRun, outbox_msg};
use ostro_types::payload::payload_ref;
use prost::Message;

use super::keys::{self, SCHEDULE_RUN, SCHEDULE_STATE};
use super::values::{self, InboxMsg, ScheduleStateRow, SignalSlot};
use super::{PartitionStore, StoreError, TABLES, Table};

fn codec(table: Table, what: &str, e: impl std::fmt::Display) -> StoreError {
    StoreError::Codec(format!("blob-ref visitor: {} {what}: {e}", table.name()))
}

/// Pushes the sha256 of a blob `PayloadRef` (inline and absent refs push nothing).
fn push_ref(
    table: Table,
    p: Option<&PayloadRef>,
    out: &mut Vec<[u8; 32]>,
) -> Result<(), StoreError> {
    if let Some(PayloadRef {
        kind: Some(payload_ref::Kind::Blob(b)),
    }) = p
    {
        let sha = <[u8; 32]>::try_from(b.sha256.as_slice()).map_err(|_| {
            codec(
                table,
                "BlobRef",
                format!("sha256 length {}", b.sha256.len()),
            )
        })?;
        out.push(sha);
    }
    Ok(())
}

fn push_start(
    table: Table,
    s: Option<&StartRun>,
    out: &mut Vec<[u8; 32]>,
) -> Result<(), StoreError> {
    if let Some(s) = s {
        push_ref(table, s.input.as_ref(), out)?;
        push_ref(
            table,
            s.run_started.as_ref().and_then(|e| e.payload.as_ref()),
            out,
        )?;
    }
    Ok(())
}

fn push_outbox(table: Table, m: &OutboxMsg, out: &mut Vec<[u8; 32]>) -> Result<(), StoreError> {
    match &m.kind {
        None => Ok(()),
        Some(outbox_msg::Kind::StartChild(c)) => push_start(table, c.start.as_ref(), out),
        Some(outbox_msg::Kind::ChildCompleted(c)) => {
            push_ref(table, c.result.as_ref(), out)?;
            push_ref(table, c.event.as_ref(), out)
        }
        Some(outbox_msg::Kind::Signal(s)) => push_ref(table, s.payload.as_ref(), out),
        Some(outbox_msg::Kind::CancelRun(_)) => Ok(()),
        Some(outbox_msg::Kind::CronStart(c)) => push_start(table, c.start.as_ref(), out),
        Some(outbox_msg::Kind::StartRun(s)) => push_start(table, Some(s), out),
    }
}

/// Appends to `out` the sha256 of every blob referenced by one state row (duplicates
/// allowed; see the module table). Errors if a row that can carry refs does not decode:
/// silently skipping it could let GC delete a live blob.
pub fn visit_row(
    table: Table,
    key: &[u8],
    value: &[u8],
    out: &mut Vec<[u8; 32]>,
) -> Result<(), StoreError> {
    match table {
        Table::Meta
        | Table::Runs
        | Table::Ready
        | Table::Timers
        | Table::Dedupe
        | Table::InboundSeen
        | Table::Children
        | Table::Dlq
        | Table::Effects
        | Table::Running
        | Table::Waits
        | Table::SignalSeen => Ok(()),
        Table::Events => {
            let e = JournalEvent::decode(value).map_err(|e| codec(table, "JournalEvent", e))?;
            push_ref(table, e.payload.as_ref(), out)
        }
        Table::Payloads => {
            let p = PayloadRef::decode(value).map_err(|e| codec(table, "PayloadRef", e))?;
            push_ref(table, Some(&p), out)
        }
        Table::Inbox => {
            if keys::decode_signal_inbox_key(key).is_some() {
                match values::decode::<SignalSlot>(value)? {
                    SignalSlot::Journaled { .. } => Ok(()),
                    SignalSlot::Pending { event, .. } => push_ref(table, Some(&event), out),
                }
            } else {
                match values::decode::<InboxMsg>(value)? {
                    InboxMsg::ChildCompleted { result, .. } if !result.is_empty() => {
                        let p = PayloadRef::decode(result.as_slice())
                            .map_err(|e| codec(table, "ChildCompleted.result", e))?;
                        push_ref(table, Some(&p), out)
                    }
                    InboxMsg::ChildCompleted { .. }
                    | InboxMsg::CancelRequested { .. }
                    | InboxMsg::TimerFired { .. } => Ok(()),
                }
            }
        }
        Table::Outbox => {
            let m = OutboxMsg::decode(value).map_err(|e| codec(table, "OutboxMsg", e))?;
            push_outbox(table, &m, out)
        }
        Table::ScheduleState => match key.first() {
            Some(&SCHEDULE_STATE) => {
                let row = values::decode::<ScheduleStateRow>(value)?;
                match row.buffered {
                    Some(b) => {
                        let s = StartRun::decode(b.start.as_slice())
                            .map_err(|e| codec(table, "buffered StartRun", e))?;
                        push_start(table, Some(&s), out)
                    }
                    None => Ok(()),
                }
            }
            Some(&SCHEDULE_RUN) => Ok(()),
            _ => Err(codec(table, "key", "unknown key shape")),
        },
    }
}

impl PartitionStore {
    /// Every blob referenced by the current state, read from ONE fjall snapshot (the blob GC
    /// mark set for this partition, P8-D8).
    pub fn blob_refs_snapshot(&self) -> Result<BTreeSet<[u8; 32]>, StoreError> {
        let snap = self.gate.snapshot(&self.db);
        let mut out = Vec::new();
        let mut set = BTreeSet::new();
        for t in TABLES {
            for g in snap.iter(self.ks(t)) {
                let (k, v) = g.into_inner()?;
                visit_row(t, &k, &v, &mut out)?;
                set.extend(out.drain(..));
            }
        }
        Ok(set)
    }
}
