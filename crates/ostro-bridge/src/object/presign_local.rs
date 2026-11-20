//! Ostro-served presigned URLs for the local backend (06-12, FX-05).
//!
//! `{public_base_url}/v1/objects/{connection}/{key}?exp=<unix seconds>&sig=<hex>` where
//! `sig = HMAC-SHA256(k, "ostro/presign/v1\n" + method + "\n" + connection + "\n" + key + "\n"
//! + exp)` and `k` is the sealed reserved secret `__ostro/presign/<connection>` (32 random bytes,
//! created on first use through a [`PresignKeyStore`]). The route (ostro-api, 06-08/06-16) is
//! unauthenticated: the signature is the authentication. `key` is the guest-visible key (the
//! connection `prefix` is applied when the request is served).
//!
//! Verification recomputes the MAC and compares it in constant time (`Mac::verify_slice`),
//! then rejects `exp < now`. Connection names and keys containing `\n` are refused, so the
//! signed string is unambiguous.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use hmac::{Hmac, KeyInit as _, Mac as _};
use ostro_objstore::PresignMethod;
use ostro_types::journal::Failure;
use sha2::Sha256;

use crate::secrets::SecretValue;
use crate::secrets::resolver::PRESIGN_PREFIX;

/// Domain separator of the signed string.
pub const DOMAIN: &str = "ostro/presign/v1";
/// REST path prefix of the route (RESERVATIONS: `/v1/objects/{connection}/{key}`).
pub const ROUTE_PREFIX: &str = "/v1/objects";
/// Length of a generated presign key.
pub const KEY_LEN: usize = 32;

type HmacSha256 = Hmac<Sha256>;

/// The reserved sealed-secret name of `connection`'s presign key.
pub fn key_name(connection: &str) -> String {
    format!("{PRESIGN_PREFIX}{connection}")
}

/// Why a presigned request was refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum VerifyError {
    /// The URL's `exp` is in the past.
    #[error("presigned URL expired")]
    Expired,
    /// The signature does not match.
    #[error("presigned URL signature mismatch")]
    BadSignature,
    /// The signature is not hex, or a field cannot be signed (`\n` in connection/key).
    #[error("malformed presigned URL")]
    Malformed,
}

fn mac(
    key: &[u8],
    method: PresignMethod,
    connection: &str,
    obj_key: &str,
    exp: u64,
) -> Option<HmacSha256> {
    if connection.contains('\n') || obj_key.contains('\n') {
        return None;
    }
    let mut m = HmacSha256::new_from_slice(key).ok()?;
    for part in [DOMAIN, method.as_str(), connection, obj_key] {
        m.update(part.as_bytes());
        m.update(b"\n");
    }
    m.update(exp.to_string().as_bytes());
    Some(m)
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(char::from(DIGITS[usize::from(b >> 4)]));
        s.push(char::from(DIGITS[usize::from(b & 0xf)]));
    }
    s
}

fn unhex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    let digit = |c: u8| match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    };
    s.as_bytes()
        .chunks(2)
        .map(|p| Some((digit(p[0])? << 4) | digit(p[1])?))
        .collect()
}

/// The hex signature of (`method`, `connection`, `obj_key`, `exp`). `None` when a field
/// contains `\n`.
pub fn sign(
    key: &[u8],
    method: PresignMethod,
    connection: &str,
    obj_key: &str,
    exp: u64,
) -> Option<String> {
    Some(hex(&mac(key, method, connection, obj_key, exp)?
        .finalize()
        .into_bytes()))
}

/// Verifies `sig` (hex) for (`method`, `connection`, `obj_key`, `exp`) at unix time `now`.
/// The MAC is checked first, in constant time; then `exp < now` is [`VerifyError::Expired`].
pub fn verify(
    key: &[u8],
    method: PresignMethod,
    connection: &str,
    obj_key: &str,
    exp: u64,
    sig: &str,
    now: u64,
) -> Result<(), VerifyError> {
    let sig = unhex(sig).ok_or(VerifyError::Malformed)?;
    let m = mac(key, method, connection, obj_key, exp).ok_or(VerifyError::Malformed)?;
    m.verify_slice(&sig)
        .map_err(|_| VerifyError::BadSignature)?;
    if exp < now {
        return Err(VerifyError::Expired);
    }
    Ok(())
}

/// `{base}/v1/objects/{connection}/{key segments}?exp=&sig=`, each path segment
/// percent-encoded. `None` if `base` is not an absolute http(s) URL.
pub fn url(base: &str, connection: &str, obj_key: &str, exp: u64, sig: &str) -> Option<url::Url> {
    let mut u = url::Url::parse(base).ok()?;
    if !matches!(u.scheme(), "http" | "https") {
        return None;
    }
    u.set_query(None);
    u.set_fragment(None);
    {
        let mut segs = u.path_segments_mut().ok()?;
        segs.pop_if_empty()
            .push("v1")
            .push("objects")
            .push(connection);
        for s in obj_key.split('/') {
            segs.push(s);
        }
    }
    u.query_pairs_mut()
        .append_pair("exp", &exp.to_string())
        .append_pair("sig", sig);
    Some(u)
}

/// Persists newly generated presign keys (06-14: seals them as `__ostro/presign/<connection>`
/// through the meta log; tests: [`MemoryPresignKeys`]).
#[async_trait]
pub trait PresignKeyStore: Send + Sync {
    /// Stores `key` under the reserved name `name` **if absent** and returns the key now in
    /// force (the existing one when another writer won).
    async fn create(&self, name: &str, key: SecretValue) -> Result<SecretValue, Failure>;
}

/// In-memory [`PresignKeyStore`] (tests, dev).
#[derive(Default)]
pub struct MemoryPresignKeys {
    keys: Mutex<HashMap<String, SecretValue>>,
}

impl std::fmt::Debug for MemoryPresignKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let n = self.keys.lock().map(|k| k.len()).unwrap_or_default();
        f.debug_struct("MemoryPresignKeys")
            .field("keys", &n)
            .finish()
    }
}

impl MemoryPresignKeys {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of stored keys.
    pub fn len(&self) -> usize {
        self.keys.lock().map(|k| k.len()).unwrap_or_default()
    }

    /// Whether no key is stored.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[async_trait]
impl PresignKeyStore for MemoryPresignKeys {
    async fn create(&self, name: &str, key: SecretValue) -> Result<SecretValue, Failure> {
        let mut keys = self.keys.lock().unwrap_or_else(|p| p.into_inner());
        Ok(keys.entry(name.to_owned()).or_insert(key).clone())
    }
}

/// 32 fresh random bytes (aws-lc-rs).
pub fn generate_key() -> Result<SecretValue, Failure> {
    let mut k = vec![0u8; KEY_LEN];
    aws_lc_rs::rand::fill(&mut k).map_err(|_| {
        crate::outcome::retryable(
            crate::outcome::INTERNAL_ERROR,
            "random key generation failed",
        )
    })?;
    Ok(SecretValue::new(k))
}

#[cfg(test)]
mod tests {
    use super::*;

    const K: &[u8] = b"0123456789abcdef0123456789abcdef";

    #[test]
    fn sign_verify_tamper_expiry() {
        let sig = sign(K, PresignMethod::Get, "files", "a/b.txt", 1_000).unwrap();
        assert_eq!(sig.len(), 64);
        verify(K, PresignMethod::Get, "files", "a/b.txt", 1_000, &sig, 999).unwrap();
        verify(
            K,
            PresignMethod::Get,
            "files",
            "a/b.txt",
            1_000,
            &sig,
            1_000,
        )
        .unwrap();
        assert_eq!(
            verify(
                K,
                PresignMethod::Get,
                "files",
                "a/b.txt",
                1_000,
                &sig,
                1_001
            ),
            Err(VerifyError::Expired)
        );
        for (m, c, k, e) in [
            (PresignMethod::Put, "files", "a/b.txt", 1_000),
            (PresignMethod::Get, "files2", "a/b.txt", 1_000),
            (PresignMethod::Get, "files", "a/b.txT", 1_000),
            (PresignMethod::Get, "files", "a/b.txt", 1_001),
        ] {
            assert_eq!(
                verify(K, m, c, k, e, &sig, 0),
                Err(VerifyError::BadSignature),
                "{m:?} {c} {k} {e}"
            );
        }
        assert_eq!(
            verify(
                b"other-key",
                PresignMethod::Get,
                "files",
                "a/b.txt",
                1_000,
                &sig,
                0
            ),
            Err(VerifyError::BadSignature)
        );
        assert_eq!(
            verify(K, PresignMethod::Get, "files", "a/b.txt", 1_000, "zz", 0),
            Err(VerifyError::Malformed)
        );
        assert!(sign(K, PresignMethod::Get, "fi\nles", "k", 1).is_none());
        // Upper-case hex verifies too.
        verify(
            K,
            PresignMethod::Get,
            "files",
            "a/b.txt",
            1_000,
            &sig.to_uppercase(),
            0,
        )
        .unwrap();
    }

    #[test]
    fn signature_vector_is_stable() {
        // HMAC-SHA256(K, "ostro/presign/v1\nGET\nfiles\na/b.txt\n1000"), cross-checked with
        // Python: hmac.new(K, msg, hashlib.sha256).hexdigest().
        let sig = sign(K, PresignMethod::Get, "files", "a/b.txt", 1_000).unwrap();
        assert_eq!(
            sig,
            "fa56f9e794b7bc84d0440d6fd8690d17f8aa57a2bd3789b36d0a04900c567570"
        );
    }

    #[test]
    fn url_shape() {
        let u = url("http://127.0.0.1:8080/", "my files", "a/b c.txt", 42, "ab").unwrap();
        assert_eq!(
            u.as_str(),
            "http://127.0.0.1:8080/v1/objects/my%20files/a/b%20c.txt?exp=42&sig=ab"
        );
        let u = url("https://ostro.example/base", "f", "k", 1, "00").unwrap();
        assert_eq!(
            u.as_str(),
            "https://ostro.example/base/v1/objects/f/k?exp=1&sig=00"
        );
        assert!(url("ftp://x", "f", "k", 1, "00").is_none());
        assert!(url("not a url", "f", "k", 1, "00").is_none());
    }

    #[tokio::test]
    async fn memory_store_first_writer_wins() {
        let s = MemoryPresignKeys::new();
        let a = generate_key().unwrap();
        let b = generate_key().unwrap();
        assert_ne!(a.expose(), b.expose());
        let got_a = s.create("__ostro/presign/f", a.clone()).await.unwrap();
        let got_b = s.create("__ostro/presign/f", b).await.unwrap();
        assert_eq!(got_a.expose(), a.expose());
        assert_eq!(got_b.expose(), a.expose());
        assert_eq!(s.len(), 1);
        assert!(!format!("{s:?}").contains("presign/f"));
    }
}
