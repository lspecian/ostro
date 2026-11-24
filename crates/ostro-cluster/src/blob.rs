//! `Replication.FetchBlob` / `Replication.PushBlob` (12-11, F11, D-P12-22): payload and
//! module blobs by sha256 between nodes.
//!
//! - [`fetch_blob`] (server) streams a stored blob in [`BLOB_CHUNK`] chunks after checking
//!   its sha256; `NOT_FOUND` when absent. [`fetch_blob_into`] (client) reassembles the stream,
//!   verifies the sha256 and only then stores it, so a follower never holds a wrong blob
//!   under a sha.
//! - [`push_blob`] (server) receives, verifies and stores a blob; [`push_to`] (client) pushes
//!   one local blob to several peers (module upload, 12-14).
//! - [`command_blob_refs`] walks a decoded prost `Command` for every `PayloadRef::Blob`
//!   (the BlobRef invariant: every BlobRef of a log command sits in a `PayloadRef` field of
//!   the prost Command tree). The match arms are exhaustive (no `_`), so a new command
//!   variant does not compile until its references are declared here.
//!
//! The service layer (12-14) runs `authz::require_node` before calling these.

use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use futures_util::{Stream, StreamExt};
use ostro_blob::{BlobRef, BlobStore, hex};
use ostro_proto::cluster::v1::replication_client::ReplicationClient;
use ostro_proto::cluster::v1::{BlobChunk, FetchBlobRequest, PushBlobAck};
use ostro_types::PayloadRef;
use ostro_types::command::{Cmd, Command, JournalEvent, OutboxMsg, StartRun, derived, outbox_msg};
use ostro_types::payload::payload_ref;
use prost::Message;
use sha2::{Digest, Sha256};
use tonic::Status;
use tonic::transport::Channel;

use crate::transport::ClusterTransport;

/// Chunk size of a blob stream (1 MiB).
pub const BLOB_CHUNK: usize = 1 << 20;

/// Largest blob a node accepts over `PushBlob` / `FetchBlob` (1 GiB).
pub const MAX_BLOB_BYTES: u64 = 1 << 30;

/// gRPC message limit for blob streams: one chunk plus framing.
pub const BLOB_MESSAGE_LIMIT: usize = BLOB_CHUNK + 64 * 1024;

/// A boxed server stream of blob chunks.
pub type BlobStream = Pin<Box<dyn Stream<Item = Result<BlobChunk, Status>> + Send + 'static>>;

/// Blob transfer failure (client side).
#[derive(Debug, thiserror::Error)]
pub enum BlobTransferError {
    /// The peer does not hold the blob.
    #[error("blob {0} not found on the peer")]
    NotFound(String),
    /// The RPC failed.
    #[error("blob rpc: {0}")]
    Rpc(#[from] Status),
    /// The received bytes do not hash to the requested sha256 (or the stream was malformed).
    #[error("blob {sha}: {reason}")]
    Corrupt {
        /// Requested sha256 (hex).
        sha: String,
        /// What was wrong.
        reason: String,
    },
    /// The local store failed.
    #[error("blob store: {0}")]
    Store(String),
    /// No channel to the peer.
    #[error("transport: {0}")]
    Transport(String),
}

fn sha_of(bytes: &[u8]) -> Result<[u8; 32], Status> {
    <[u8; 32]>::try_from(bytes).map_err(|_| {
        Status::invalid_argument(format!("sha256 must be 32 bytes, got {}", bytes.len()))
    })
}

/// Path of the blob `sha` in `store` (the path depends on the sha only).
fn path_of(store: &BlobStore, sha: &[u8; 32]) -> PathBuf {
    store.path_of(&BlobRef {
        sha256: *sha,
        len: 0,
    })
}

/// Whether `store` holds the blob `sha`.
pub fn has_blob(store: &BlobStore, sha: &[u8; 32]) -> bool {
    path_of(store, sha).is_file()
}

/// Read and verify a local blob by sha (blocking: call from `spawn_blocking`).
fn read_verified(store: &BlobStore, sha: &[u8; 32]) -> Result<Option<Vec<u8>>, String> {
    let bytes = match std::fs::read(path_of(store, sha)) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.to_string()),
    };
    let actual: [u8; 32] = Sha256::digest(&bytes).into();
    if &actual != sha {
        return Err(format!(
            "stored blob {} hashes to {}",
            hex(sha),
            hex(&actual)
        ));
    }
    Ok(Some(bytes))
}

/// Split `bytes` into the chunk messages of one blob stream (one empty `last` chunk for an
/// empty blob).
fn chunks(sha: [u8; 32], bytes: &[u8]) -> Vec<BlobChunk> {
    if bytes.is_empty() {
        return vec![BlobChunk {
            sha256: sha.to_vec(),
            offset: 0,
            bytes: Vec::new(),
            last: true,
        }];
    }
    let n = bytes.len().div_ceil(BLOB_CHUNK);
    bytes
        .chunks(BLOB_CHUNK)
        .enumerate()
        .map(|(i, c)| BlobChunk {
            sha256: sha.to_vec(),
            offset: (i * BLOB_CHUNK) as u64,
            bytes: c.to_vec(),
            last: i + 1 == n,
        })
        .collect()
}

/// `FetchBlob` server: stream the blob named by `req.sha256` from `store`.
pub async fn fetch_blob(
    store: Arc<BlobStore>,
    req: FetchBlobRequest,
) -> Result<BlobStream, Status> {
    let sha = sha_of(&req.sha256)?;
    let s = Arc::clone(&store);
    let bytes = tokio::task::spawn_blocking(move || read_verified(&s, &sha))
        .await
        .map_err(|e| Status::internal(format!("blob read task: {e}")))?
        .map_err(Status::data_loss)?
        .ok_or_else(|| Status::not_found(format!("blob {} not found", hex(&sha))))?;
    let items: Vec<Result<BlobChunk, Status>> = chunks(sha, &bytes).into_iter().map(Ok).collect();
    Ok(Box::pin(futures_util::stream::iter(items)))
}

/// Reassemble a chunk stream of the blob `expect` (or, when `None`, of the sha the first
/// chunk names) and verify its sha256.
async fn collect_chunks<S>(
    mut stream: S,
    expect: Option<[u8; 32]>,
) -> Result<([u8; 32], Vec<u8>), BlobTransferError>
where
    S: Stream<Item = Result<BlobChunk, Status>> + Unpin,
{
    let mut sha = expect;
    let mut buf = Vec::new();
    let corrupt = |sha: Option<[u8; 32]>, reason: String| BlobTransferError::Corrupt {
        sha: sha.map(|s| hex(&s)).unwrap_or_default(),
        reason,
    };
    let mut done = false;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        if done {
            return Err(corrupt(sha, "chunk after the last one".into()));
        }
        let named = <[u8; 32]>::try_from(chunk.sha256.as_slice())
            .map_err(|_| corrupt(sha, "chunk sha256 is not 32 bytes".into()))?;
        match sha {
            None => sha = Some(named),
            Some(s) if s != named => {
                return Err(corrupt(sha, format!("chunk names blob {}", hex(&named))));
            }
            Some(_) => {}
        }
        if chunk.offset != buf.len() as u64 {
            return Err(corrupt(
                sha,
                format!("chunk offset {} != received {}", chunk.offset, buf.len()),
            ));
        }
        if buf.len() as u64 + chunk.bytes.len() as u64 > MAX_BLOB_BYTES {
            return Err(corrupt(sha, format!("blob exceeds {MAX_BLOB_BYTES} bytes")));
        }
        buf.extend_from_slice(&chunk.bytes);
        done = chunk.last;
    }
    let Some(sha) = sha else {
        return Err(corrupt(None, "empty stream".into()));
    };
    if !done {
        return Err(corrupt(
            Some(sha),
            "stream ended before the last chunk".into(),
        ));
    }
    let actual: [u8; 32] = Sha256::digest(&buf).into();
    if actual != sha {
        return Err(corrupt(
            Some(sha),
            format!("received bytes hash to {}", hex(&actual)),
        ));
    }
    Ok((sha, buf))
}

async fn put(store: &Arc<BlobStore>, bytes: Vec<u8>) -> Result<BlobRef, BlobTransferError> {
    let s = Arc::clone(store);
    tokio::task::spawn_blocking(move || s.put(&bytes))
        .await
        .map_err(|e| BlobTransferError::Store(format!("put task: {e}")))?
        .map_err(|e| BlobTransferError::Store(e.to_string()))
}

/// `FetchBlob` client: fetch `sha` over `channel` into `store` (verified before the put).
/// A blob already held locally is not fetched.
pub async fn fetch_blob_into(
    channel: Channel,
    store: &Arc<BlobStore>,
    sha: [u8; 32],
) -> Result<(), BlobTransferError> {
    if has_blob(store, &sha) {
        return Ok(());
    }
    let mut client = ReplicationClient::new(channel)
        .max_decoding_message_size(BLOB_MESSAGE_LIMIT)
        .max_encoding_message_size(BLOB_MESSAGE_LIMIT);
    let stream = match client
        .fetch_blob(FetchBlobRequest {
            sha256: sha.to_vec(),
        })
        .await
    {
        Ok(r) => r.into_inner(),
        Err(s) if s.code() == tonic::Code::NotFound => {
            return Err(BlobTransferError::NotFound(hex(&sha)));
        }
        Err(s) => return Err(s.into()),
    };
    let (_, bytes) = collect_chunks(stream, Some(sha)).await?;
    put(store, bytes).await?;
    Ok(())
}

/// `PushBlob` server: receive one blob, verify its sha256 and store it.
/// `stored = false` when the blob was already held.
pub async fn push_blob<S>(store: Arc<BlobStore>, stream: S) -> Result<PushBlobAck, Status>
where
    S: Stream<Item = Result<BlobChunk, Status>> + Unpin,
{
    let (sha, bytes) = collect_chunks(stream, None).await.map_err(|e| match e {
        BlobTransferError::Rpc(s) => s,
        other => Status::invalid_argument(other.to_string()),
    })?;
    if has_blob(&store, &sha) {
        return Ok(PushBlobAck {
            sha256: sha.to_vec(),
            stored: false,
        });
    }
    put(&store, bytes)
        .await
        .map_err(|e| Status::internal(e.to_string()))?;
    Ok(PushBlobAck {
        sha256: sha.to_vec(),
        stored: true,
    })
}

/// `PushBlob` client: push the local blob `sha` to every peer in `peers`. One result per
/// peer, in order (`Ok(stored)`).
pub async fn push_to(
    transport: &dyn ClusterTransport,
    peers: &[u64],
    store: &Arc<BlobStore>,
    sha: [u8; 32],
) -> Vec<(u64, Result<bool, BlobTransferError>)> {
    let s = Arc::clone(store);
    let bytes = match tokio::task::spawn_blocking(move || read_verified(&s, &sha)).await {
        Ok(Ok(Some(b))) => b,
        Ok(Ok(None)) => {
            return peers
                .iter()
                .map(|&p| (p, Err(BlobTransferError::NotFound(hex(&sha)))))
                .collect();
        }
        Ok(Err(e)) => {
            return peers
                .iter()
                .map(|&p| (p, Err(BlobTransferError::Store(e.clone()))))
                .collect();
        }
        Err(e) => {
            return peers
                .iter()
                .map(|&p| (p, Err(BlobTransferError::Store(format!("read task: {e}")))))
                .collect();
        }
    };
    let mut out = Vec::with_capacity(peers.len());
    for &peer in peers {
        let r = async {
            let channel = transport
                .channel(peer)
                .map_err(|e| BlobTransferError::Transport(e.to_string()))?;
            let mut client = ReplicationClient::new(channel)
                .max_decoding_message_size(BLOB_MESSAGE_LIMIT)
                .max_encoding_message_size(BLOB_MESSAGE_LIMIT);
            let ack = client
                .push_blob(futures_util::stream::iter(chunks(sha, &bytes)))
                .await?
                .into_inner();
            Ok(ack.stored)
        }
        .await;
        out.push((peer, r));
    }
    out
}

// ---- BlobRef walk over the prost Command tree (F11). ----

fn push_ref(p: Option<&PayloadRef>, out: &mut Vec<[u8; 32]>) {
    if let Some(PayloadRef {
        kind: Some(payload_ref::Kind::Blob(b)),
    }) = p
        && let Ok(sha) = <[u8; 32]>::try_from(b.sha256.as_slice())
        && !out.contains(&sha)
    {
        out.push(sha);
    }
}

fn push_event(e: Option<&JournalEvent>, out: &mut Vec<[u8; 32]>) {
    if let Some(e) = e {
        push_ref(e.payload.as_ref(), out);
    }
}

fn push_start(s: Option<&StartRun>, out: &mut Vec<[u8; 32]>) {
    if let Some(s) = s {
        push_ref(s.input.as_ref(), out);
        push_event(s.run_started.as_ref(), out);
        for e in &s.rerun_prefix {
            push_ref(e.payload.as_ref(), out);
        }
    }
}

fn push_outbox(m: Option<&OutboxMsg>, out: &mut Vec<[u8; 32]>) {
    let Some(kind) = m.and_then(|m| m.kind.as_ref()) else {
        return;
    };
    match kind {
        outbox_msg::Kind::StartChild(c) => push_start(c.start.as_ref(), out),
        outbox_msg::Kind::ChildCompleted(c) => {
            push_ref(c.result.as_ref(), out);
            push_ref(c.event.as_ref(), out);
        }
        outbox_msg::Kind::Signal(s) => push_ref(s.payload.as_ref(), out),
        outbox_msg::Kind::CancelRun(_) => {}
        outbox_msg::Kind::CronStart(c) => push_start(c.start.as_ref(), out),
        outbox_msg::Kind::StartRun(s) => push_start(Some(s), out),
    }
}

/// Every blob sha256 a command references (deduplicated, in tree order).
pub fn command_blob_refs(cmd: &Command) -> Vec<[u8; 32]> {
    let mut out = Vec::new();
    let Some(c) = cmd.cmd.as_ref() else {
        return out;
    };
    match c {
        Cmd::LeaderAnnounced(_)
        | Cmd::AttemptGranted(_)
        | Cmd::TimerFired(_)
        | Cmd::OutboxTruncated(_)
        | Cmd::RequestCancel(_)
        | Cmd::TerminateRun(_)
        | Cmd::RetryRun(_)
        | Cmd::PauseRun(_)
        | Cmd::ResumeRun(_)
        | Cmd::DlqUpdate(_) => {}
        // Phase 13 (13-01): module shas only, no payload blobs (modules sync separately).
        Cmd::RepinRun(_) => {}
        Cmd::StartRun(s) => push_start(Some(s), &mut out),
        Cmd::EventsAppended(e) => {
            for ev in &e.events {
                push_ref(ev.payload.as_ref(), &mut out);
            }
            for d in &e.derived {
                let Some(intent) = d.intent.as_ref() else {
                    continue;
                };
                match intent {
                    derived::Intent::SendMessage(m) => push_outbox(m.msg.as_ref(), &mut out),
                    derived::Intent::CompleteRun(r) => {
                        push_ref(r.result.as_ref(), &mut out);
                        push_ref(r.parent_event.as_ref(), &mut out);
                    }
                    derived::Intent::ScheduleTimer(_)
                    | derived::Intent::CancelTimer(_)
                    | derived::Intent::EnqueueReady(_)
                    | derived::Intent::Suspend(_)
                    | derived::Intent::RegisterWait(_)
                    | derived::Intent::CancelWait(_)
                    | derived::Intent::RegisterEffect(_)
                    | derived::Intent::CancelEffect(_)
                    | derived::Intent::ChildJournal(_) => {}
                }
            }
        }
        Cmd::InboundBatch(b) => {
            for m in &b.msgs {
                push_outbox(m.msg.as_ref(), &mut out);
            }
        }
        Cmd::SignalRun(s) => push_ref(s.event.as_ref(), &mut out),
        Cmd::CompleteToken(t) => push_ref(t.event.as_ref(), &mut out),
        Cmd::ScheduleFire(f) => push_start(f.start.as_ref(), &mut out),
        Cmd::CompleteEffect(e) => push_ref(e.outcome.as_ref(), &mut out),
    }
    out
}

/// Every blob sha256 the encoded commands reference. An undecodable command references
/// nothing (apply rejects it as invalid, so it never needs a blob).
pub fn blob_refs_of<'a>(cmds: impl IntoIterator<Item = &'a [u8]>) -> Vec<[u8; 32]> {
    let mut out = Vec::new();
    for bytes in cmds {
        if let Ok(cmd) = Command::decode(bytes) {
            for sha in command_blob_refs(&cmd) {
                if !out.contains(&sha) {
                    out.push(sha);
                }
            }
        }
    }
    out
}
