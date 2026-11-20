//! Error type shared by every backend.

/// Result alias for object-store operations.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// An object-store error. Messages never contain credentials.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The key is not acceptable to this backend (empty, absolute, `..`, NUL, ...).
    #[error("invalid object key {key:?}: {reason}")]
    InvalidKey {
        /// The rejected key.
        key: String,
        /// Why it was rejected.
        reason: &'static str,
    },
    /// The object does not exist (for operations that require it, e.g. `get_range`).
    #[error("object not found: {key}")]
    NotFound {
        /// The missing key.
        key: String,
    },
    /// The requested byte range is empty-inverted or starts at or past the end of the object.
    #[error("invalid range {start}..{end} for {key} (size {size:?})")]
    InvalidRange {
        /// The key.
        key: String,
        /// Requested start.
        start: u64,
        /// Requested end (exclusive).
        end: u64,
        /// Object size, when known.
        size: Option<u64>,
    },
    /// Bad configuration or URL (unsupported scheme, `http://` without `allow_http`, ...).
    #[error("object store configuration: {0}")]
    Config(String),
    /// Local filesystem error.
    #[error("object store I/O on {path}: {source}")]
    Io {
        /// The path involved.
        path: String,
        /// The underlying error.
        #[source]
        source: std::io::Error,
    },
    /// Transport error after retries (connect, timeout, body read).
    #[error("object store transport: {0}")]
    Transport(#[source] reqwest::Error),
    /// The server answered with an unexpected status (after retries for 5xx/429).
    #[error("object store {op} {key}: HTTP {status}: {body}")]
    Status {
        /// The operation (`put`, `get`, `list`, ...).
        op: &'static str,
        /// The key or prefix.
        key: String,
        /// HTTP status code.
        status: u16,
        /// A (truncated) response body, usually the S3 XML error.
        body: String,
    },
    /// The server's response could not be parsed.
    #[error("object store {op}: malformed response: {detail}")]
    Protocol {
        /// The operation.
        op: &'static str,
        /// What was wrong.
        detail: String,
    },
    /// A `put_multipart` body stream yielded an error.
    #[error("object store body stream: {0}")]
    Body(String),
}

impl Error {
    pub(crate) fn io(path: impl std::fmt::Display, source: std::io::Error) -> Self {
        Self::Io {
            path: path.to_string(),
            source,
        }
    }
}
