//! Layer 3: the only place in ostro-bridge that builds a `reqwest::Client` (FX-08).
//!
//! Both clients:
//! - `.no_proxy()`: reqwest reads `HTTP(S)_PROXY`/`ALL_PROXY` by default, and a proxy
//!   would resolve names on our behalf, bypassing every guard (Pitfall 3);
//! - `redirect::Policy::none()`: a reqwest redirect policy has no module context, so the
//!   HTTP connector follows redirects itself and re-checks every hop (P-5);
//! - `.referer(false)`, and no gzip/brotli/zstd/deflate (the features are not compiled in:
//!   no decompression bombs, and the byte cap counts wire bytes);
//! - the `[egress] connect_timeout`.
//!
//! The guarded client additionally resolves through [`EgressResolver`], which pins every
//! connection to vetted addresses. The operator client resolves normally: object-store and
//! other operator-configured endpoints are trusted (SeaweedFS on localhost is legitimate).

use bytes::{Bytes, BytesMut};
use reqwest::redirect::Policy;

use super::config::EgressConfig;
use super::resolver::EgressResolver;

fn base(cfg: &EgressConfig) -> reqwest::ClientBuilder {
    reqwest::Client::builder()
        .no_proxy()
        .redirect(Policy::none())
        .referer(false)
        .connect_timeout(cfg.connect_timeout)
}

/// The guarded client: [`base`] plus the vetting resolver.
pub(crate) fn guarded(
    cfg: &EgressConfig,
    resolver: EgressResolver,
) -> Result<reqwest::Client, reqwest::Error> {
    base(cfg).dns_resolver(resolver).build()
}

/// The operator client: [`base`] with the system resolver (rustls, default roots).
pub(crate) fn operator(cfg: &EgressConfig) -> Result<reqwest::Client, reqwest::Error> {
    base(cfg).build()
}

/// The body exceeded the cap.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("response body exceeds {cap} bytes")]
pub struct TooLarge {
    /// The cap.
    pub cap: u64,
    /// Bytes seen before giving up (`Content-Length` when it alone exceeded the cap).
    pub seen: u64,
}

/// Reading a capped body failed.
#[derive(Debug, thiserror::Error)]
pub enum ReadError {
    /// Over the cap (`response_too_large`).
    #[error(transparent)]
    TooLarge(#[from] TooLarge),
    /// The body stream failed (`http_transport`).
    #[error("reading the response body failed: {0}")]
    Body(#[from] reqwest::Error),
}

/// Streams `resp`'s body into memory, failing as soon as more than `cap` bytes arrive
/// (at most one chunk past the cap is ever buffered). A `Content-Length` above the cap
/// fails before any body byte is read.
pub async fn read_capped(mut resp: reqwest::Response, cap: u64) -> Result<Bytes, ReadError> {
    if let Some(len) = resp.content_length()
        && len > cap
    {
        return Err(TooLarge { cap, seen: len }.into());
    }
    let mut buf = BytesMut::new();
    while let Some(chunk) = resp.chunk().await? {
        let seen = buf.len() as u64 + chunk.len() as u64;
        if seen > cap {
            return Err(TooLarge { cap, seen }.into());
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(buf.freeze())
}
