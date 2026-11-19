//! Reading a run's journal back from the partition store as engine [`Event`]s.
//!
//! Rows come from `ostro_partition::query::journal_rows` (`(seq, kind, PayloadRef)`); blob
//! payloads are fetched (length and sha256 verified) and every row is decoded with
//! `ostro_types::journal::decode_event`. Nothing is skipped: an unknown kind, a bad codec
//! version or a missing blob is an error that names the run and the seq.

use ostro_log::ReplicatedLog;
use ostro_partition::{PartitionHandle, query};
use ostro_types::RunId;
use ostro_types::journal::{Event, decode_event};

/// Rows read per store scan.
const PAGE: usize = 1024;

/// Errors reading a journal.
#[derive(Debug, thiserror::Error)]
pub enum JournalReadError {
    #[error("run {run}: reading journal rows: {source}")]
    Store {
        run: RunId,
        #[source]
        source: ostro_partition::StoreError,
    },
    #[error("run {run} seq {seq}: payload: {source}")]
    Payload {
        run: RunId,
        seq: u64,
        #[source]
        source: ostro_partition::PartitionError,
    },
    #[error("run {run} seq {seq}: decode: {source}")]
    Decode {
        run: RunId,
        seq: u64,
        #[source]
        source: ostro_types::journal::CodecError,
    },
    #[error("run {run}: journal has seq {found} where {expected} was expected")]
    Gap {
        run: RunId,
        expected: u64,
        found: u64,
    },
    #[error("run {run}: journal read task failed: {msg}")]
    Task { run: RunId, msg: String },
}

async fn rows<L: ReplicatedLog>(
    handle: &PartitionHandle<L>,
    run: RunId,
    from_seq: u64,
    limit: usize,
) -> Result<Vec<(u64, u32, ostro_types::PayloadRef)>, JournalReadError> {
    let reader = handle.reader();
    tokio::task::spawn_blocking(move || query::journal_rows(&reader, &run, from_seq, limit))
        .await
        .map_err(|e| JournalReadError::Task {
            run,
            msg: e.to_string(),
        })?
        .map_err(|source| JournalReadError::Store { run, source })
}

async fn decode<L: ReplicatedLog>(
    handle: &PartitionHandle<L>,
    run: RunId,
    (seq, kind, payload): (u64, u32, ostro_types::PayloadRef),
) -> Result<Event, JournalReadError> {
    let bytes = handle
        .fetch_payload(&payload)
        .await
        .map_err(|source| JournalReadError::Payload { run, seq, source })?;
    decode_event(seq, kind, &bytes).map_err(|source| JournalReadError::Decode { run, seq, source })
}

/// The whole journal of `run` in seq order, starting at seq 0. Rows must be contiguous.
pub async fn read_journal<L: ReplicatedLog>(
    handle: &PartitionHandle<L>,
    run: &RunId,
) -> Result<Vec<Event>, JournalReadError> {
    read_journal_from(handle, run, 0).await
}

/// The journal of `run` from `from_seq` to its end, in seq order (contiguous).
pub async fn read_journal_from<L: ReplicatedLog>(
    handle: &PartitionHandle<L>,
    run: &RunId,
    from_seq: u64,
) -> Result<Vec<Event>, JournalReadError> {
    let run = *run;
    let mut out = Vec::new();
    let mut next = from_seq;
    loop {
        let page = rows(handle, run, next, PAGE).await?;
        let n = page.len();
        for row in page {
            if row.0 != next {
                return Err(JournalReadError::Gap {
                    run,
                    expected: next,
                    found: row.0,
                });
            }
            out.push(decode(handle, run, row).await?);
            next += 1;
        }
        if n < PAGE {
            return Ok(out);
        }
    }
}

/// The journal event of `run` at `seq`, if stored (04-16's input bridge reads single rows
/// announced by `Action::InputAppended`).
pub async fn read_event<L: ReplicatedLog>(
    handle: &PartitionHandle<L>,
    run: &RunId,
    seq: u64,
) -> Result<Option<Event>, JournalReadError> {
    let run = *run;
    match rows(handle, run, seq, 1).await?.pop() {
        Some(row) if row.0 == seq => Ok(Some(decode(handle, run, row).await?)),
        _ => Ok(None),
    }
}
