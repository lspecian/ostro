//! Webhook idempotency-key extractors and run-input selectors (P8-D12, TRIG-01).
//!
//! The meta `Webhook.idempotency` string is `header:<name>`, `json:<pointer>` (RFC 6901) or
//! `none`; `Webhook.input_ref` is `""` (the whole body) or a JSON pointer.
//!
//! Every function that reads the body takes a [`VerifiedDelivery`], which only
//! [`crate::webhook::verify`] can produce: the body is never parsed before its signature has
//! been checked. Error `Display`s never include header or body content.

use std::fmt;

use crate::webhook::{VerifiedDelivery, VerifierKind, header};

/// Longest accepted idempotency key, in bytes.
pub const MAX_KEY_LEN: usize = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ExtractError {
    /// The extractor or selector string is not valid.
    #[error("invalid extractor syntax (expected header:<name>, json:<pointer> or none)")]
    Syntax,
    /// The body is not valid JSON.
    #[error("webhook body is not valid JSON")]
    BadJson,
    /// The pointer resolves to an object, array, boolean or null.
    #[error("idempotency key is not a string or number")]
    NotScalar,
    /// The key is longer than [`MAX_KEY_LEN`] bytes.
    #[error("idempotency key longer than {MAX_KEY_LEN} bytes")]
    TooLong,
    /// The header value is not UTF-8.
    #[error("idempotency header is not valid UTF-8")]
    NotUtf8,
    /// The input pointer does not resolve.
    #[error("input JSON pointer does not resolve")]
    PointerNotFound,
}

/// How to derive a delivery's idempotency key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IdempotencyExtractor {
    /// A request header (name stored lower case).
    Header(String),
    /// A JSON pointer into the body.
    JsonPointer(String),
    /// No key: every delivery is distinct.
    None,
}

fn is_header_name(s: &str) -> bool {
    !s.is_empty()
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b))
}

fn is_pointer(s: &str) -> bool {
    s.starts_with('/')
}

impl IdempotencyExtractor {
    /// Parse `header:<name>`, `json:<pointer>` or `none`.
    pub fn parse(s: &str) -> Result<Self, ExtractError> {
        if let Some(name) = s.strip_prefix("header:") {
            if !is_header_name(name) {
                return Err(ExtractError::Syntax);
            }
            return Ok(Self::Header(name.to_ascii_lowercase()));
        }
        if let Some(ptr) = s.strip_prefix("json:") {
            if !is_pointer(ptr) {
                return Err(ExtractError::Syntax);
            }
            return Ok(Self::JsonPointer(ptr.to_string()));
        }
        match s {
            "none" => Ok(Self::None),
            _ => Err(ExtractError::Syntax),
        }
    }

    /// Extract the key. `Ok(None)` when the header or pointer is absent (or empty), or for
    /// [`IdempotencyExtractor::None`].
    pub fn extract(
        &self,
        proof: &VerifiedDelivery,
        headers: &[(String, Vec<u8>)],
        body: &[u8],
    ) -> Result<Option<String>, ExtractError> {
        let key = match self {
            Self::None => return Ok(None),
            Self::Header(name) => match header(headers, name) {
                None => return Ok(None),
                Some(raw) => std::str::from_utf8(raw)
                    .map_err(|_| ExtractError::NotUtf8)?
                    .trim()
                    .to_string(),
            },
            Self::JsonPointer(ptr) => match json_pointer_string(proof, body, ptr)? {
                None => return Ok(None),
                Some(k) => k,
            },
        };
        if key.len() > MAX_KEY_LEN {
            return Err(ExtractError::TooLong);
        }
        Ok(if key.is_empty() { None } else { Some(key) })
    }
}

impl fmt::Display for IdempotencyExtractor {
    /// The meta string form, inverse of [`IdempotencyExtractor::parse`].
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Header(n) => write!(f, "header:{n}"),
            Self::JsonPointer(p) => write!(f, "json:{p}"),
            Self::None => f.write_str("none"),
        }
    }
}

/// Vendor default extractor: Stripe `json:/id`, GitHub `header:x-github-delivery`, Slack
/// `json:/event_id`, Standard Webhooks `header:webhook-id`; `none` otherwise.
pub fn default_for(kind: VerifierKind) -> IdempotencyExtractor {
    match kind {
        VerifierKind::Stripe => IdempotencyExtractor::JsonPointer("/id".into()),
        VerifierKind::GitHub => IdempotencyExtractor::Header("x-github-delivery".into()),
        VerifierKind::Slack => IdempotencyExtractor::JsonPointer("/event_id".into()),
        VerifierKind::StandardWebhooks => IdempotencyExtractor::Header("webhook-id".into()),
        VerifierKind::GenericHmac | VerifierKind::None => IdempotencyExtractor::None,
    }
}

/// Resolve a JSON pointer in a verified body to a string (strings as-is, numbers in their
/// JSON text form). `Ok(None)` if the pointer does not resolve. Usable for the meta
/// `Webhook.run_ref` (run id / token) as well as idempotency keys.
pub fn json_pointer_string(
    _proof: &VerifiedDelivery,
    body: &[u8],
    pointer: &str,
) -> Result<Option<String>, ExtractError> {
    let v: serde_json::Value = serde_json::from_slice(body).map_err(|_| ExtractError::BadJson)?;
    match v.pointer(pointer) {
        None => Ok(None),
        Some(serde_json::Value::String(s)) => Ok(Some(s.clone())),
        Some(serde_json::Value::Number(n)) => Ok(Some(n.to_string())),
        Some(_) => Err(ExtractError::NotScalar),
    }
}

/// Which bytes become the run input (meta `Webhook.input_ref`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InputSelector {
    /// The raw body, untouched.
    Body,
    /// The JSON value at this pointer, re-serialized.
    JsonPointer(String),
}

impl InputSelector {
    /// `""` = [`InputSelector::Body`], otherwise a JSON pointer starting with `/`.
    pub fn from_meta(input_ref: &str) -> Result<Self, ExtractError> {
        if input_ref.is_empty() {
            Ok(Self::Body)
        } else if is_pointer(input_ref) {
            Ok(Self::JsonPointer(input_ref.to_string()))
        } else {
            Err(ExtractError::Syntax)
        }
    }

    /// The meta string form, inverse of [`InputSelector::from_meta`].
    pub fn to_meta(&self) -> &str {
        match self {
            Self::Body => "",
            Self::JsonPointer(p) => p,
        }
    }

    /// Produce the run input bytes from a verified body.
    pub fn select(&self, _proof: &VerifiedDelivery, body: &[u8]) -> Result<Vec<u8>, ExtractError> {
        match self {
            Self::Body => Ok(body.to_vec()),
            Self::JsonPointer(ptr) => {
                let v: serde_json::Value =
                    serde_json::from_slice(body).map_err(|_| ExtractError::BadJson)?;
                let sel = v.pointer(ptr).ok_or(ExtractError::PointerNotFound)?;
                serde_json::to_vec(sel).map_err(|_| ExtractError::BadJson)
            }
        }
    }
}
