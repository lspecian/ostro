//! `FetchJournal`: journal rows `[from, to)` in pages of at most `max_frame_bytes`
//! (WORK-02, Pattern 3).
//!
//! Rows must be contiguous from `from`. Blob-backed rows (`BlobRef{sha, len}`) are resolved
//! with [`crate::blob_fetch`] (length + sha256 verified) and every row is decoded with
//! `ostro_types::journal::decode_event`, as `ostro-server`'s `journal_io` does.

use ostro_types::journal::{Event, decode_event};
use ostro_worker_proto::v1;

use crate::Link;
use crate::blob_fetch::{self, FetchError};

/// Fetch and decode journal events `[from, to)`.
pub(crate) async fn fetch(
    link: &Link,
    run: &v1::RunRef,
    token: &v1::Token,
    from: u64,
    to: u64,
) -> Result<Vec<Event>, FetchError> {
    if from >= to {
        return Ok(Vec::new());
    }
    let mut client = link.client.clone();
    let req = v1::FetchJournalRequest {
        session_id: link.session_id.clone(),
        run: Some(run.clone()),
        token: Some(*token),
        from_seq: from,
        to_seq: to,
    };
    let mut stream = client
        .fetch_journal(req)
        .await
        .map_err(|s| FetchError::from_status(&s))?
        .into_inner();
    let mut out = Vec::with_capacity(usize::try_from(to - from).unwrap_or(0));
    let mut next = from;
    'pages: while let Some(page) = stream
        .message()
        .await
        .map_err(|s| FetchError::from_status(&s))?
    {
        for row in page.rows {
            if row.seq != next {
                return Err(FetchError::Integrity(format!(
                    "journal has seq {} where {next} was expected",
                    row.seq
                )));
            }
            if next >= to {
                return Err(FetchError::Integrity(format!(
                    "journal row {} beyond the requested end {to}",
                    row.seq
                )));
            }
            let bytes = match row.payload {
                Some(v1::journal_row::Payload::Inline(b)) => b,
                Some(v1::journal_row::Payload::Blob(r)) => {
                    let sha: [u8; 32] = r.sha.as_slice().try_into().map_err(|_| {
                        FetchError::Integrity(format!("seq {}: blob sha is not 32 bytes", row.seq))
                    })?;
                    blob_fetch::fetch(link, run, token, &sha, Some(r.len)).await?
                }
                None => {
                    return Err(FetchError::Integrity(format!(
                        "seq {}: row without payload",
                        row.seq
                    )));
                }
            };
            let ev = decode_event(row.seq, row.kind, &bytes)
                .map_err(|e| FetchError::Decode(format!("seq {}: {e}", row.seq)))?;
            out.push(ev);
            next += 1;
        }
        if page.last {
            break 'pages;
        }
    }
    if next != to {
        return Err(FetchError::Integrity(format!(
            "journal ended at seq {next}, expected {to}"
        )));
    }
    Ok(out)
}
