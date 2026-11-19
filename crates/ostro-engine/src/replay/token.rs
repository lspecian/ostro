//! Await tokens (WIT-05 `create-await-token`).
//!
//! `"ot1_" + base32_lower_nopad(partition BE(2) || len(run_id) u8 || run_id ||
//! op BE(8) || hmac_sha256(key, those bytes)[..10])`. The token routes an external
//! completion to its partition, run and op; the truncated HMAC (80 bits) authenticates
//! it. The base32 decoder accepts only canonical RFC 4648 encodings (lowercase, no
//! padding, zero trailing bits), so every token has exactly one spelling.
//!
//! The server's token-completion API (05-13) uses [`parse_token`] / [`verify_token`]
//! (re-exported as `ostro_engine::token::{parse, verify}`). A token is a bearer
//! credential for one op: its body must never appear in URLs, logs or error messages
//! ([`TokenError`] and [`TokenParts`] carry no token text or MAC bytes).

use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

const PREFIX: &str = "ot1_";
const TAG_LEN: usize = 10;
const ALPHABET: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";

/// Why a token was rejected. Never includes the token text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TokenError {
    /// The token does not start with `ot1_`.
    #[error("not an ostro await token (bad prefix)")]
    BadPrefix,
    /// Not canonical base32, or the decoded layout is inconsistent.
    #[error("malformed await token")]
    Malformed,
    /// Well formed, but the HMAC does not verify under the key.
    #[error("await token MAC does not verify")]
    BadMac,
}

/// Decoded token fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenParts {
    pub partition: u16,
    pub run_id: String,
    pub op: u64,
}

fn mac(key: &[u8; 32], body: &[u8]) -> HmacSha256 {
    let mut m = <HmacSha256 as KeyInit>::new_from_slice(key).expect("HMAC accepts any key length");
    m.update(body);
    m
}

/// Mint a token for `(partition, run_id, op)`.
///
/// # Panics
///
/// If `run_id` is longer than 255 bytes (engine-generated run ids are far shorter).
pub fn mint(partition: u16, run_id: &str, op: u64, key: &[u8; 32]) -> String {
    let len = u8::try_from(run_id.len()).expect("run id longer than 255 bytes");
    let mut bytes = Vec::with_capacity(2 + 1 + run_id.len() + 8 + TAG_LEN);
    bytes.extend_from_slice(&partition.to_be_bytes());
    bytes.push(len);
    bytes.extend_from_slice(run_id.as_bytes());
    bytes.extend_from_slice(&op.to_be_bytes());
    let tag = mac(key, &bytes).finalize().into_bytes();
    bytes.extend_from_slice(&tag[..TAG_LEN]);
    format!("{PREFIX}{}", base32_encode(&bytes))
}

/// Decoded body and tag of a well-formed token.
fn split(token: &str) -> Result<(TokenParts, Vec<u8>, usize), TokenError> {
    let encoded = token.strip_prefix(PREFIX).ok_or(TokenError::BadPrefix)?;
    let bytes = base32_decode(encoded).ok_or(TokenError::Malformed)?;
    let len = usize::from(*bytes.get(2).ok_or(TokenError::Malformed)?);
    if bytes.len() != 2 + 1 + len + 8 + TAG_LEN {
        return Err(TokenError::Malformed);
    }
    let partition = u16::from_be_bytes([bytes[0], bytes[1]]);
    let run_id = std::str::from_utf8(&bytes[3..3 + len])
        .map_err(|_| TokenError::Malformed)?
        .to_owned();
    let op = u64::from_be_bytes(
        bytes[3 + len..3 + len + 8]
            .try_into()
            .map_err(|_| TokenError::Malformed)?,
    );
    let body_len = 3 + len + 8;
    Ok((
        TokenParts {
            partition,
            run_id,
            op,
        },
        bytes,
        body_len,
    ))
}

/// Parse a token's fields **without** checking its HMAC (routing only; never trust
/// the result before [`verify_token`]).
pub fn parse_token(token: &str) -> Result<TokenParts, TokenError> {
    split(token).map(|(parts, _, _)| parts)
}

/// Parse `token` and check its HMAC under `key` in constant time; returns its fields
/// only if it verifies.
pub fn verify_token(key: &[u8; 32], token: &str) -> Result<TokenParts, TokenError> {
    let (parts, bytes, body_len) = split(token)?;
    let (body, tag) = bytes.split_at(body_len);
    mac(key, body)
        .verify_truncated_left(tag)
        .map_err(|_| TokenError::BadMac)?;
    Ok(parts)
}

/// Option form of [`parse_token`] (Phase 3 callers).
pub fn parse(token: &str) -> Option<TokenParts> {
    parse_token(token).ok()
}

/// Bool form of [`verify_token`] (Phase 3 callers; note the argument order).
pub fn verify(token: &str, key: &[u8; 32]) -> bool {
    verify_token(key, token).is_ok()
}

/// RFC 4648 base32, lowercase alphabet, no padding.
fn base32_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(5) * 8);
    let (mut acc, mut bits) = (0u32, 0u32);
    for &b in bytes {
        acc = ((acc << 8) | u32::from(b)) & 0xffff;
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

/// Inverse of [`base32_encode`]; rejects non-alphabet chars, impossible lengths and
/// non-zero trailing bits.
fn base32_decode(s: &str) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(s.len() * 5 / 8);
    let (mut acc, mut bits) = (0u32, 0u32);
    for c in s.bytes() {
        let v = ALPHABET.iter().position(|&a| a == c)? as u32;
        acc = ((acc << 5) | v) & 0xffff;
        bits += 5;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
        }
    }
    // Leftover bits must be fewer than 5 (otherwise a whole char was redundant) and zero.
    if bits >= 5 || acc & ((1 << bits) - 1) != 0 {
        return None;
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: [u8; 32] = [9; 32];

    #[test]
    fn base32_rfc4648_vectors() {
        let v = [
            ("", ""),
            ("f", "my"),
            ("fo", "mzxq"),
            ("foo", "mzxw6"),
            ("foob", "mzxw6yq"),
            ("fooba", "mzxw6ytb"),
            ("foobar", "mzxw6ytboi"),
        ];
        for (plain, enc) in v {
            assert_eq!(base32_encode(plain.as_bytes()), enc);
            assert_eq!(base32_decode(enc).as_deref(), Some(plain.as_bytes()));
        }
    }

    #[test]
    fn base32_rejects_noncanonical() {
        // Invalid lengths, non-alphabet chars, uppercase and non-zero trailing bits.
        for bad in ["m", "mzx", "mzxw6y", "MZXW6", "mzxw1", "mz", "mzxr"] {
            assert_eq!(base32_decode(bad), None, "{bad}");
        }
    }

    /// Golden token computed independently (Python hmac/hashlib/base64).
    #[test]
    fn await_token_golden() {
        assert_eq!(
            mint(3, "run-01", 42, &KEY),
            "ot1_aabqm4tvnywtamiaaaaaaaaaaavdc46cczcvk4rr32fq"
        );
    }

    #[test]
    fn await_token_round_trip() {
        let t = mint(65_535, "01JABCDEF", u64::MAX - 1, &KEY);
        assert!(t.starts_with("ot1_"));
        assert_eq!(
            parse(&t),
            Some(TokenParts {
                partition: 65_535,
                run_id: "01JABCDEF".into(),
                op: u64::MAX - 1,
            })
        );
        assert!(verify(&t, &KEY));
        assert!(!verify(&t, &[8; 32]));
    }

    #[test]
    fn await_token_tamper_detected() {
        let t = mint(3, "run-01", 42, &KEY);
        let alphabet = "abcdefghijklmnopqrstuvwxyz234567";
        for i in 4..t.len() {
            let c = t.as_bytes()[i] as char;
            for r in alphabet.chars().filter(|r| *r != c) {
                let mut s = t.clone();
                s.replace_range(i..=i, &r.to_string());
                assert!(
                    parse(&s).is_none() || !verify(&s, &KEY),
                    "tampered token accepted: {s}"
                );
            }
        }
        assert_eq!(parse("ot2_aabqm4tvnywtamia"), None);
        assert_eq!(parse("ot1_"), None);
        assert!(!verify("garbage", &KEY));
    }

    /// Result API used by the server (05-13): golden vector, tampered MAC, bad prefix,
    /// wrong key; errors carry no token text.
    #[test]
    fn token_result_api() {
        let t = "ot1_aabqm4tvnywtamiaaaaaaaaaaavdc46cczcvk4rr32fq";
        let want = TokenParts {
            partition: 3,
            run_id: "run-01".into(),
            op: 42,
        };
        assert_eq!(parse_token(t), Ok(want.clone()));
        assert_eq!(verify_token(&KEY, t), Ok(want));
        assert_eq!(verify_token(&[8; 32], t), Err(TokenError::BadMac));

        // Flip one MAC bit (last bytes of the body) keeping canonical base32.
        let mut bytes = base32_decode(&t[4..]).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 1;
        let tampered = format!("ot1_{}", base32_encode(&bytes));
        assert!(parse_token(&tampered).is_ok(), "fields still parse");
        assert_eq!(verify_token(&KEY, &tampered), Err(TokenError::BadMac));

        for bad in ["ot2_aabqm4tvnywtamia", "OT1_aabq", "aabqm4tv", ""] {
            assert_eq!(parse_token(bad), Err(TokenError::BadPrefix), "{bad}");
            assert_eq!(verify_token(&KEY, bad), Err(TokenError::BadPrefix));
        }
        for bad in ["ot1_", "ot1_m", "ot1_MZXW6", "ot1_aabqm4tvnywtamia"] {
            assert_eq!(parse_token(bad), Err(TokenError::Malformed), "{bad}");
        }
        for e in [
            TokenError::BadPrefix,
            TokenError::Malformed,
            TokenError::BadMac,
        ] {
            assert!(!e.to_string().contains("ot1_"));
        }
    }
}
