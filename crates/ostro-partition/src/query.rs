//! Typed read helpers over a [`StoreReader`] for the dispatcher and the API (plan 04-12).
//!
//! All functions are synchronous fjall reads. Callers on an async runtime should use
//! `spawn_blocking` for the scans (`running_runs`, `ready_entries`, `journal_rows`,
//! `runs_page`). None of them is used by apply.
//!
//! - [`running_runs`]: the `running` index (in-flight runs and their current token), the
//!   leader's rebuild source for leases (DISP-01).
//! - [`ready_entries`]: every `ready` row of every queue, in key order (queue, priority
//!   descending, ready time, run).
//! - [`journal_rows`]: a run's journal from a sequence number on.
//! - [`run_record`], [`run_payload`]: one run's record and its input/result payload refs.
//! - [`runs_page`]: runs in key order (partition, then uuid v7 = creation time order).

use ostro_types::command::JournalEvent;
use ostro_types::{AttemptToken, Epoch, PayloadRef, RunId};
use prost::Message;

use crate::apply::run;
use crate::store::keys::{decode_ready_key, event_key, payload_key, run_key};
use crate::store::values::{self, ReadyValue, RunRecord};
use crate::store::{StoreError, StoreReader, Table};

/// `payloads` kind of a run's input.
pub const PAYLOAD_INPUT: u8 = run::PAYLOAD_INPUT;
/// `payloads` kind of a run's result (written by `CompleteRun`).
pub const PAYLOAD_RESULT: u8 = run::PAYLOAD_RESULT;

const RUN_LEN: usize = 18;

/// One decoded `ready` row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadyEntryRow {
    pub queue: String,
    pub priority: i32,
    pub ready_at_ms: u64,
    pub run: RunId,
    pub concurrency_key: String,
}

fn codec(what: &str, e: impl std::fmt::Display) -> StoreError {
    StoreError::Codec(format!("{what}: {e}"))
}

fn run_id(what: &str, k: &[u8]) -> Result<RunId, StoreError> {
    RunId::from_bytes(k).map_err(|e| codec(what, e))
}

/// Every run in the `running` keyspace with its current attempt token, in run id order.
/// Bounded by the number of in-flight runs.
pub fn running_runs(r: &StoreReader) -> Result<Vec<(RunId, AttemptToken)>, StoreError> {
    r.prefix(Table::Running, &[], usize::MAX)?
        .into_iter()
        .map(|(k, v)| {
            let (epoch, n): (u32, u32) = values::decode(&v)?;
            Ok((
                run_id("running key", &k)?,
                AttemptToken {
                    epoch: Epoch(epoch),
                    n,
                },
            ))
        })
        .collect()
}

/// Up to `limit` ready rows across all queues, in key order: queues by their key encoding
/// (`u16 BE len ‖ utf8`, so shorter names first, not alphabetical), then per queue priority
/// descending, ready time, run id.
pub fn ready_entries(r: &StoreReader, limit: usize) -> Result<Vec<ReadyEntryRow>, StoreError> {
    r.prefix(Table::Ready, &[], limit)?
        .into_iter()
        .map(|(k, v)| {
            let (queue, priority, ready_at_ms, run) =
                decode_ready_key(&k).ok_or_else(|| codec("ready key", "malformed"))?;
            let rv: ReadyValue = values::decode(&v)?;
            Ok(ReadyEntryRow {
                queue,
                priority,
                ready_at_ms,
                run,
                concurrency_key: rv.concurrency_key,
            })
        })
        .collect()
}

/// Up to `limit` journal rows of `run` with `seq >= from_seq`, in seq order, as
/// `(seq, kind, payload)`. A stored event without a payload yields an empty inline ref
/// (the engine always sets one).
pub fn journal_rows(
    r: &StoreReader,
    run: &RunId,
    from_seq: u64,
    limit: usize,
) -> Result<Vec<(u64, u32, PayloadRef)>, StoreError> {
    let from = event_key(run, from_seq);
    // Every event key of `run` is RUN_LEN + 8 bytes, so run ‖ 0xff * 9 is above all of them.
    let mut to = run.as_bytes().to_vec();
    to.extend_from_slice(&[0xff; 9]);
    r.range(Table::Events, &from, &to, limit)?
        .into_iter()
        .map(|(_, v)| {
            let ev = JournalEvent::decode(&*v).map_err(|e| codec("journal event", e))?;
            let payload = ev
                .payload
                .unwrap_or_else(|| PayloadRef::inline(Default::default()));
            Ok((ev.seq, ev.kind, payload))
        })
        .collect()
}

/// The run record, if the run exists in this partition.
pub fn run_record(r: &StoreReader, run: &RunId) -> Result<Option<RunRecord>, StoreError> {
    r.get(Table::Runs, &run_key(run))?
        .map(|v| values::decode(&v))
        .transpose()
}

/// The payload ref of kind [`PAYLOAD_INPUT`] or [`PAYLOAD_RESULT`], if stored.
pub fn run_payload(
    r: &StoreReader,
    run: &RunId,
    kind: u8,
) -> Result<Option<PayloadRef>, StoreError> {
    r.get(Table::Payloads, &payload_key(run, kind))?
        .map(|v| PayloadRef::decode(&*v).map_err(|e| codec("payload ref", e)))
        .transpose()
}

/// Up to `limit` runs in key order, strictly after `after` (from the start if `None`).
/// Within a partition, key order is uuid v7 order, i.e. creation time order.
pub fn runs_page(
    r: &StoreReader,
    after: Option<RunId>,
    limit: usize,
) -> Result<Vec<(RunId, RunRecord)>, StoreError> {
    let rows = match after {
        None => r.prefix(Table::Runs, &[], limit)?,
        Some(a) => {
            // The smallest key above `a` is a ‖ 0x00; every run key is RUN_LEN bytes, so
            // 0xff * (RUN_LEN + 1) is above all of them.
            let mut from = run_key(&a);
            from.push(0);
            r.range(Table::Runs, &from, &[0xff; RUN_LEN + 1], limit)?
        }
    };
    rows.into_iter()
        .map(|(k, v)| Ok((run_id("run key", &k)?, values::decode(&v)?)))
        .collect()
}
