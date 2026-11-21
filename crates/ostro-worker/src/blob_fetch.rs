//! `FetchBlob`: modules and payload blobs, lease-authorized and chunked (Pattern 3).
//!
//! The chunks are concatenated and verified exactly like `ostro-server`'s `journal_io`:
//! the total length must equal the announced `total_len` (and the `BlobRef.len` when the
//! caller knows it) and `sha256(bytes)` must equal the requested sha. A mismatch is an
//! error; nothing unverified is ever handed to the engine or cached.

use sha2::{Digest, Sha256};

use crate::Link;
use ostro_worker_proto::v1;

/// Upper bound of a single blob (the server's `[workers] max_append_bytes` default; a
/// module or payload above this is refused before it is buffered).
pub const MAX_BLOB_BYTES: u64 = 256 * 1024 * 1024;

/// Fetch failures.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FetchError {
    /// No live session.
    #[error("no session")]
    NoSession,
    /// The server refused (lease not held: the activation is fenced).
    #[error("fenced by the server: {0}")]
    Fenced(String),
    /// Transport / server error.
    #[error("rpc failed: {0}")]
    Rpc(String),
    /// Length or sha256 mismatch, malformed page.
    #[error("integrity: {0}")]
    Integrity(String),
    /// A journal row did not decode.
    #[error("decode: {0}")]
    Decode(String),
}

impl FetchError {
    pub(crate) fn from_status(s: &tonic::Status) -> Self {
        match s.code() {
            tonic::Code::PermissionDenied | tonic::Code::FailedPrecondition => {
                Self::Fenced(s.message().to_owned())
            }
            c => Self::Rpc(format!("{c}: {}", s.message())),
        }
    }
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// Fetch the blob `sha` for `(run, token)` and verify it.
pub(crate) async fn fetch(
    link: &Link,
    run: &v1::RunRef,
    token: &v1::Token,
    sha: &[u8; 32],
    expected_len: Option<u64>,
) -> Result<Vec<u8>, FetchError> {
    let mut client = link.client.clone();
    let req = v1::FetchBlobRequest {
        session_id: link.session_id.clone(),
        run: Some(run.clone()),
        token: Some(*token),
        sha: sha.to_vec(),
        offset: 0,
    };
    let mut stream = client
        .fetch_blob(req)
        .await
        .map_err(|s| FetchError::from_status(&s))?
        .into_inner();
    let mut out: Vec<u8> = Vec::new();
    let mut total: Option<u64> = None;
    let mut done = false;
    while let Some(chunk) = stream
        .message()
        .await
        .map_err(|s| FetchError::from_status(&s))?
    {
        if total.is_none() {
            if chunk.total_len > MAX_BLOB_BYTES {
                return Err(FetchError::Integrity(format!(
                    "blob {} announces {} bytes (limit {MAX_BLOB_BYTES})",
                    hex(sha),
                    chunk.total_len
                )));
            }
            total = Some(chunk.total_len);
            out.reserve(usize::try_from(chunk.total_len).unwrap_or(0));
        }
        out.extend_from_slice(&chunk.data);
        if out.len() as u64 > total.unwrap_or(0) {
            return Err(FetchError::Integrity(format!(
                "blob {}: more bytes than the announced {}",
                hex(sha),
                total.unwrap_or(0)
            )));
        }
        if chunk.last {
            done = true;
            break;
        }
    }
    let total = total.unwrap_or(0);
    if !done || out.len() as u64 != total {
        return Err(FetchError::Integrity(format!(
            "blob {}: got {} of {total} bytes",
            hex(sha),
            out.len()
        )));
    }
    if let Some(len) = expected_len
        && len != total
    {
        return Err(FetchError::Integrity(format!(
            "blob {}: length {total}, expected {len}",
            hex(sha)
        )));
    }
    let got: [u8; 32] = Sha256::digest(&out).into();
    if &got != sha {
        return Err(FetchError::Integrity(format!(
            "blob sha256 mismatch: requested {}, received {}",
            hex(sha),
            hex(&got)
        )));
    }
    Ok(out)
}
