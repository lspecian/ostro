//! Payload references (STOR-14): inline bytes up to [`INLINE_PAYLOAD_LIMIT`], otherwise a
//! content-addressed blob reference. Hand-written prost message embedded in partition commands.

use bytes::Bytes;

/// Largest payload stored inline in a command (256 KiB). Larger payloads go to the blob store.
pub const INLINE_PAYLOAD_LIMIT: usize = 256 * 1024;

/// A payload: inline bytes or a blob reference.
#[derive(Clone, PartialEq, prost::Message)]
pub struct PayloadRef {
    /// The payload variant (`None` only for malformed input).
    #[prost(oneof = "payload_ref::Kind", tags = "1, 2")]
    pub kind: Option<payload_ref::Kind>,
}

/// Oneof variants of [`PayloadRef`].
pub mod payload_ref {
    /// Payload variant.
    #[derive(Clone, PartialEq, prost::Oneof)]
    pub enum Kind {
        /// Bytes stored in the command itself.
        #[prost(bytes = "bytes", tag = "1")]
        Inline(bytes::Bytes),
        /// Content-addressed blob.
        #[prost(message, tag = "2")]
        Blob(super::BlobRef),
    }
}

/// A content-addressed blob: SHA-256 of the content and its length.
#[derive(Clone, PartialEq, prost::Message)]
pub struct BlobRef {
    /// SHA-256 digest (32 bytes).
    #[prost(bytes = "vec", tag = "1")]
    pub sha256: Vec<u8>,
    /// Content length in bytes.
    #[prost(uint64, tag = "2")]
    pub len: u64,
}

impl PayloadRef {
    /// An inline payload.
    pub fn inline(b: Bytes) -> Self {
        Self {
            kind: Some(payload_ref::Kind::Inline(b)),
        }
    }

    /// A blob reference.
    pub fn blob(sha256: [u8; 32], len: u64) -> Self {
        Self {
            kind: Some(payload_ref::Kind::Blob(BlobRef {
                sha256: sha256.to_vec(),
                len,
            })),
        }
    }

    /// Length of an inline payload; `None` for blobs.
    pub fn inline_len(&self) -> Option<usize> {
        match &self.kind {
            Some(payload_ref::Kind::Inline(b)) => Some(b.len()),
            _ => None,
        }
    }

    /// Whether this is an inline payload larger than [`INLINE_PAYLOAD_LIMIT`].
    pub fn is_oversize_inline(&self) -> bool {
        self.inline_len().is_some_and(|n| n > INLINE_PAYLOAD_LIMIT)
    }
}
