//! Phase 14 (14-03): the API token codec (D-P14-4).
//!
//! A bearer is `ostro_<id>_<secret>`:
//! - `id` = `otk_` + 16 lowercase RFC 4648 base32 characters (10 random bytes, 80 bits);
//! - `secret` = 52 lowercase base32 characters (32 random bytes, canonical: the 4 trailing
//!   bits of the last character are zero).
//!
//! The alphabet is `a-z2-7`, unpadded. Randomness comes from the OS through aws-lc-rs
//! ([`aws_lc_rs::rand::SystemRandom`]). Meta stores only `sha256(decoded secret bytes)`
//! ([`hash`]); [`verify`] compares in constant time
//! ([`aws_lc_rs::constant_time::verify_slices_are_equal`]). [`parse`] rejects every
//! malformed bearer before any meta lookup.

use std::fmt;

use aws_lc_rs::rand::SecureRandom;
use sha2::{Digest, Sha256};

/// Every bearer starts with this.
pub const BEARER_PREFIX: &str = "ostro_";
/// API token ids start with this (RESERVATIONS id prefix `otk_`).
pub const ID_PREFIX: &str = "otk_";
/// Base32 characters after [`ID_PREFIX`].
pub const ID_CHARS: usize = 16;
/// Random bytes behind an id.
pub const ID_BYTES: usize = 10;
/// Base32 characters of a secret.
pub const SECRET_CHARS: usize = 52;
/// Random bytes behind a secret.
pub const SECRET_BYTES: usize = 32;
/// Anything longer is refused before it is looked at (D-P14-4: > 128 bytes).
pub const MAX_BEARER_LEN: usize = 128;
/// Id prefixes of the non-API tokens stored in the same meta keyspace.
const OTHER_PREFIXES: [&str; 2] = ["wkr_", "njt_"];

const ALPHABET: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";

/// An API token id (`otk_` + 16 base32 characters).
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TokenId(String);

impl TokenId {
    /// The id text (the meta key).
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The id text, owned.
    pub fn into_string(self) -> String {
        self.0
    }
}

impl fmt::Debug for TokenId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("TokenId").field(&self.0).finish()
    }
}

impl fmt::Display for TokenId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A token secret (52 base32 characters). `Debug` never shows it.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    /// The secret text. Only for building the bearer shown once to its owner.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(<redacted>)")
    }
}

/// A full bearer (`ostro_<id>_<secret>`). `Debug` never shows it.
#[derive(Clone, PartialEq, Eq)]
pub struct Bearer(String);

impl Bearer {
    /// The bearer text (printed once, written to `admin.token`).
    pub fn expose(&self) -> &str {
        &self.0
    }

    /// The bearer of `id` and `secret`.
    pub fn of(id: &TokenId, secret: &Secret) -> Bearer {
        Bearer(format!("{BEARER_PREFIX}{}_{}", id.0, secret.0))
    }
}

impl fmt::Debug for Bearer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Bearer(<redacted>)")
    }
}

/// Why a bearer was refused by [`parse`]. Never echoed to clients beyond "invalid token".
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ParseError {
    /// Longer than [`MAX_BEARER_LEN`].
    #[error("bearer longer than {MAX_BEARER_LEN} bytes")]
    TooLong,
    /// Does not start with `ostro_`.
    #[error("bearer does not start with {BEARER_PREFIX:?}")]
    Prefix,
    /// The id part is not `otk_` + 16 base32 characters.
    #[error("malformed token id")]
    Id,
    /// The id belongs to a worker / node join token (`wkr_`, `njt_`).
    #[error("not an API token")]
    NotApiToken,
    /// The secret part is not 52 canonical base32 characters.
    #[error("malformed token secret")]
    Secret,
}

/// The OS random source could not fill a buffer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("OS random source unavailable")]
pub struct RngError;

/// Generate a fresh token: id, secret and the bearer built from them.
pub fn generate(rng: &impl SecureRandom) -> Result<(TokenId, Secret, Bearer), RngError> {
    let mut id = [0u8; ID_BYTES];
    let mut secret = [0u8; SECRET_BYTES];
    rng.fill(&mut id).map_err(|_| RngError)?;
    rng.fill(&mut secret).map_err(|_| RngError)?;
    let id = TokenId(format!("{ID_PREFIX}{}", base32_encode(&id)));
    let secret = Secret(base32_encode(&secret));
    let bearer = Bearer::of(&id, &secret);
    Ok((id, secret, bearer))
}

/// Generate with the OS random source ([`aws_lc_rs::rand::SystemRandom`]).
pub fn generate_os() -> Result<(TokenId, Secret, Bearer), RngError> {
    generate(&aws_lc_rs::rand::SystemRandom::new())
}

/// Parse a bearer: exact prefix, `rsplit_once('_')`, exact lengths and alphabet, canonical
/// secret encoding. No lookup happens before this succeeds.
pub fn parse(bearer: &str) -> Result<(TokenId, Secret), ParseError> {
    if bearer.len() > MAX_BEARER_LEN {
        return Err(ParseError::TooLong);
    }
    let rest = bearer
        .strip_prefix(BEARER_PREFIX)
        .ok_or(ParseError::Prefix)?;
    let (id, secret) = rest.rsplit_once('_').ok_or(ParseError::Id)?;
    check_id(id)?;
    if secret.len() != SECRET_CHARS || base32_decode(secret).is_none() {
        return Err(ParseError::Secret);
    }
    Ok((TokenId(id.to_owned()), Secret(secret.to_owned())))
}

/// Whether `id` is a well-formed API token id (`otk_` + 16 base32 characters).
pub fn is_api_token_id(id: &str) -> bool {
    check_id(id).is_ok()
}

fn check_id(id: &str) -> Result<(), ParseError> {
    if OTHER_PREFIXES.iter().any(|p| id.starts_with(p)) {
        return Err(ParseError::NotApiToken);
    }
    let body = id.strip_prefix(ID_PREFIX).ok_or(ParseError::Id)?;
    if body.len() != ID_CHARS || !body.bytes().all(|c| ALPHABET.contains(&c)) {
        return Err(ParseError::Id);
    }
    Ok(())
}

/// The stored hash: `sha256(decoded secret bytes)`.
pub fn hash(secret: &Secret) -> [u8; 32] {
    // A `Secret` only comes from `generate` / `parse`, so it always decodes; fall back to the
    // text bytes (never equal to a real hash) rather than panicking.
    let bytes = base32_decode(&secret.0).unwrap_or_else(|| secret.0.as_bytes().to_vec());
    Sha256::digest(&bytes).into()
}

/// Whether `secret` matches the stored hash (constant time).
pub fn verify(stored: &[u8; 32], secret: &Secret) -> bool {
    aws_lc_rs::constant_time::verify_slices_are_equal(stored, &hash(secret)).is_ok()
}

/// Lowercase RFC 4648 base32 without padding.
pub fn base32_encode(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len().div_ceil(5) * 8);
    let (mut acc, mut bits) = (0u32, 0u32);
    for &b in data {
        acc = ((acc << 8) | u32::from(b)) & 0xFFFF;
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            out.push(char::from(ALPHABET[((acc >> bits) & 31) as usize]));
        }
    }
    if bits > 0 {
        out.push(char::from(ALPHABET[((acc << (5 - bits)) & 31) as usize]));
    }
    out
}

/// Inverse of [`base32_encode`]: `None` for any character outside `a-z2-7`, an impossible
/// length (`len % 8` of 1, 3 or 6) or non-zero trailing bits (non-canonical encodings).
pub fn base32_decode(s: &str) -> Option<Vec<u8>> {
    if matches!(s.len() % 8, 1 | 3 | 6) {
        return None;
    }
    let mut out = Vec::with_capacity(s.len() * 5 / 8);
    let (mut acc, mut bits) = (0u32, 0u32);
    for c in s.bytes() {
        let v = ALPHABET.iter().position(|&a| a == c)? as u32;
        acc = ((acc << 5) | v) & 0xFFFF;
        bits += 5;
        if bits >= 8 {
            bits -= 8;
            out.push(((acc >> bits) & 0xFF) as u8);
        }
    }
    if acc & ((1 << bits) - 1) != 0 {
        return None;
    }
    Some(out)
}
