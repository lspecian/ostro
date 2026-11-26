//! Phase 14 (14-05, SEC-03, D-P14-10): module signature verification.
//!
//! Pure functions: no I/O, no clock (timestamps are passed in by the caller). Two formats:
//!
//! - **minisign** (primary): legacy `Ed` (Ed25519 over the bytes) and prehashed `ED`
//!   (Ed25519 over BLAKE2b-512 of the bytes), both with the trusted-comment global
//!   signature, verified by `minisign-verify` (`allow_legacy = true`). Key ids are matched
//!   before any verification is attempted.
//! - **cosign keyed**: ECDSA P-256 / SHA-256 over the module bytes, as a raw base64 ASN.1 DER
//!   signature (the `.sig` file `cosign sign-blob --key` writes) or a Sigstore bundle v0.3 JSON
//!   (`verificationMaterial.publicKey.hint`, `messageSignature.signature`, optional
//!   `messageSignature.messageDigest` checked against sha256 of the bytes), verified with
//!   aws-lc-rs. Keyless bundles (certificate / transparency log) are refused
//!   ([`SigError::KeylessUnsupported`]); the `sigstore` crate is not used.
//!
//! Trusted keys come from a minisign public key (`RW…` base64 or the two-line `.pub` file) or
//! a cosign PEM `SubjectPublicKeyInfo` for P-256. Every parser bounds its input
//! ([`MAX_SIG_BYTES`], [`MAX_KEY_BYTES`], [`MAX_COMMENT_BYTES`], [`MAX_SIGS`]) and returns a
//! typed [`SigError`] on malformed input; nothing here panics on arbitrary bytes (proptest in
//! `tests/p14_signing.rs`, fuzzed by 14-07 `signature_parse`).
//!
//! Persistence (ext 113 / 115 / 116): [`ModuleSigEvidence::to_module_sig`] produces the
//! [`ext::ModuleSig`] 14-19 stores; [`reverify`] re-checks stored signatures over the blob at
//! activation with keys selected by [`key_fingerprint`] (D-P14-10 B1(c)). The stored `sig` is
//! the normalised `.minisig` text for minisign algs and the raw DER bytes for cosign.

use aws_lc_rs::digest;
use aws_lc_rs::encoding::AsDer;
use aws_lc_rs::signature::{ECDSA_P256_SHA256_ASN1, UnparsedPublicKey};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use ostro_meta::ext;

/// Largest accepted signature input (one `.minisig`, cosign `.sig` or bundle), in bytes.
pub const MAX_SIG_BYTES: usize = 8 * 1024;
/// Largest accepted trusted-key text, in bytes.
pub const MAX_KEY_BYTES: usize = 8 * 1024;
/// Largest accepted comment (minisign untrusted/trusted comment, key comment, bundle hint).
pub const MAX_COMMENT_BYTES: usize = 1024;
/// Most signatures accepted for one module.
pub const MAX_SIGS: usize = 4;

const MINISIGN_LEGACY: [u8; 2] = *b"Ed";
const MINISIGN_PREHASHED: [u8; 2] = *b"ED";
const UNTRUSTED_PREFIX: &str = "untrusted comment:";
const TRUSTED_PREFIX: &str = "trusted comment: ";
const PEM_BEGIN: &str = "-----BEGIN PUBLIC KEY-----";
const PEM_END: &str = "-----END PUBLIC KEY-----";
const BUNDLE_MEDIA_TYPES: [&str; 2] = [
    "application/vnd.dev.sigstore.bundle.v0.3+json",
    "application/vnd.dev.sigstore.bundle+json;version=0.3",
];
/// Shortest / longest ASN.1 DER ECDSA P-256 signature (SEQUENCE of two INTEGERs ≤ 33 bytes).
const DER_SIG_MIN: usize = 8;
const DER_SIG_MAX: usize = 72;

/// Signature algorithm; the discriminants are the ext 113 `SIG_ALG_*` values.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Alg {
    /// minisign legacy `Ed`.
    MinisignEd = ext::SIG_ALG_MINISIGN_ED,
    /// minisign prehashed `ED` (BLAKE2b-512, then Ed25519).
    MinisignEdPrehashed = ext::SIG_ALG_MINISIGN_ED_PREHASHED,
    /// cosign keyed ECDSA P-256 / SHA-256.
    CosignP256 = ext::SIG_ALG_COSIGN_P256,
}

impl Alg {
    /// The ext 113 byte.
    pub fn as_u8(self) -> u8 {
        self as u8
    }

    /// Parse an ext 113 byte (unknown values → `None`).
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            ext::SIG_ALG_MINISIGN_ED => Some(Self::MinisignEd),
            ext::SIG_ALG_MINISIGN_ED_PREHASHED => Some(Self::MinisignEdPrehashed),
            ext::SIG_ALG_COSIGN_P256 => Some(Self::CosignP256),
            _ => None,
        }
    }

    /// The key family that verifies this algorithm.
    pub fn family(self) -> KeyFamily {
        match self {
            Self::MinisignEd | Self::MinisignEdPrehashed => KeyFamily::Minisign,
            Self::CosignP256 => KeyFamily::Cosign,
        }
    }
}

/// Key family of a [`TrustedKey`]. A minisign key verifies both minisign algorithms.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum KeyFamily {
    /// Ed25519 minisign key (`public_key` = 32 raw bytes, `key_id` = 8 bytes as in the file).
    Minisign,
    /// P-256 cosign key (`public_key` = SPKI DER, `key_id` = first 8 bytes of its sha256).
    Cosign,
}

impl KeyFamily {
    /// The alg byte of the key in ext 115/116 `TrustedKey.alg` and in [`key_fingerprint`]:
    /// `SIG_ALG_MINISIGN_ED` (minisign public keys always carry `Ed`) or `SIG_ALG_COSIGN_P256`.
    pub fn alg_byte(self) -> u8 {
        match self {
            Self::Minisign => ext::SIG_ALG_MINISIGN_ED,
            Self::Cosign => ext::SIG_ALG_COSIGN_P256,
        }
    }
}

/// A trusted signing key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TrustedKey {
    /// Minisign or cosign.
    pub alg_family: KeyFamily,
    /// minisign: the 8 key-id bytes in file order; cosign: `sha256(SPKI DER)[..8]`.
    pub key_id: Vec<u8>,
    /// minisign: 32-byte Ed25519 key; cosign: canonical SPKI DER.
    pub public_key: Vec<u8>,
    /// minisign: the untrusted comment of the `.pub` file (prefix stripped); cosign: empty.
    pub comment: String,
}

impl TrustedKey {
    /// The ext 115/116 record.
    pub fn to_meta(&self) -> ext::TrustedKey {
        ext::TrustedKey {
            alg: self.alg_family.alg_byte(),
            key_id: self.key_id.clone(),
            public_key: self.public_key.clone(),
            comment: self.comment.clone(),
        }
    }

    /// From an ext 115/116 record, re-validating the key material (unknown alg, wrong sizes,
    /// an invalid P-256 key or a cosign key id that does not match the key → `Malformed`).
    pub fn from_meta(k: &ext::TrustedKey) -> Result<Self, SigError> {
        if k.comment.len() > MAX_COMMENT_BYTES {
            return Err(SigError::TooLarge("key comment"));
        }
        match k.alg {
            ext::SIG_ALG_MINISIGN_ED | ext::SIG_ALG_MINISIGN_ED_PREHASHED => {
                if k.key_id.len() != 8 || k.public_key.len() != 32 {
                    return Err(malformed(
                        "minisign key needs an 8-byte id and a 32-byte key",
                    ));
                }
                Ok(Self {
                    alg_family: KeyFamily::Minisign,
                    key_id: k.key_id.clone(),
                    public_key: k.public_key.clone(),
                    comment: k.comment.clone(),
                })
            }
            ext::SIG_ALG_COSIGN_P256 => {
                if k.public_key.len() > MAX_KEY_BYTES {
                    return Err(SigError::TooLarge("trusted key"));
                }
                let mut key = cosign_key_from_der(&k.public_key)?;
                if key.key_id != k.key_id {
                    return Err(malformed("cosign key id does not match the key"));
                }
                key.comment = k.comment.clone();
                Ok(key)
            }
            other => Err(malformed(format!("unknown key alg {other}"))),
        }
    }
}

/// Signature verification error. Never carries secret material.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum SigError {
    /// An input exceeds a limit (which one is named).
    #[error("too large: {0}")]
    TooLarge(&'static str),
    /// The input is not a parseable signature, bundle or key.
    #[error("malformed: {0}")]
    Malformed(String),
    /// No trusted key matches the signature's key id (minisign) or family (cosign), or no
    /// trusted key has the stored fingerprint ([`reverify`]).
    #[error("no trusted key matches the signature")]
    UnknownKey,
    /// A matching key was found but the signature does not verify.
    #[error("signature does not verify")]
    BadSignature,
    /// The bundle's `messageDigest` is not sha256 of the module bytes.
    #[error("bundle message digest does not match the module")]
    DigestMismatch,
    /// A keyless (certificate / transparency log) Sigstore bundle.
    #[error("keyless sigstore bundles are not supported (keyed cosign only)")]
    KeylessUnsupported,
    /// No signature was supplied.
    #[error("no signature supplied")]
    Unsigned,
}

impl SigError {
    /// Rank when no signature verifies: the most specific failure is reported.
    fn rank(&self) -> u8 {
        match self {
            Self::Unsigned => 0,
            Self::UnknownKey => 1,
            Self::Malformed(_) | Self::KeylessUnsupported | Self::TooLarge(_) => 2,
            Self::BadSignature => 3,
            Self::DigestMismatch => 4,
        }
    }
}

fn malformed(msg: impl Into<String>) -> SigError {
    SigError::Malformed(msg.into())
}

/// A parsed signature ([`parse_signature`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParsedSig {
    alg: Alg,
    key_id: Vec<u8>,
    body: Body,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Body {
    /// Normalised four-line `.minisig` text (accepted by `minisign_verify::Signature::decode`).
    Minisign { text: String },
    /// DER signature, optional bundle digest and hint.
    Cosign {
        der: Vec<u8>,
        digest: Option<[u8; 32]>,
        hint: Option<String>,
    },
}

impl ParsedSig {
    /// The signature algorithm.
    pub fn alg(&self) -> Alg {
        self.alg
    }

    /// minisign: the 8-byte key id from the signature; cosign: empty (keys are tried by family).
    pub fn key_id(&self) -> &[u8] {
        &self.key_id
    }

    /// The bundle's `verificationMaterial.publicKey.hint`, if any.
    pub fn hint(&self) -> Option<&str> {
        match &self.body {
            Body::Cosign { hint, .. } => hint.as_deref(),
            Body::Minisign { .. } => None,
        }
    }

    /// The bytes persisted in ext 113 `ModuleSig.sig`: the normalised `.minisig` text or the
    /// DER signature.
    pub fn stored_bytes(&self) -> &[u8] {
        match &self.body {
            Body::Minisign { text } => text.as_bytes(),
            Body::Cosign { der, .. } => der,
        }
    }
}

/// One verified signature: what 14-19 persists in ext 113.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModuleSigEvidence {
    /// Algorithm of the signature.
    pub alg: Alg,
    /// Key id of the verifying key ([`TrustedKey::key_id`]).
    pub key_id: Vec<u8>,
    /// [`key_fingerprint`] of the verifying key.
    pub key_fingerprint: [u8; 32],
    /// [`ParsedSig::stored_bytes`].
    pub sig: Vec<u8>,
}

impl ModuleSigEvidence {
    /// The ext 113 record (`added_at_ms` from the injected clock, `added_by` = token id or
    /// `system`).
    pub fn to_module_sig(&self, added_at_ms: u64, added_by: &str) -> ext::ModuleSig {
        ext::ModuleSig {
            alg: self.alg.as_u8(),
            key_fingerprint: self.key_fingerprint,
            key_id: self.key_id.clone(),
            sig: self.sig.clone(),
            added_at_ms,
            added_by: added_by.to_string(),
        }
    }
}

// ---------------------------------------------------------------------------------------
// Keys
// ---------------------------------------------------------------------------------------

/// Parse a trusted key: a minisign public key (`RW…` base64, or the two-line `.pub` file with
/// its `untrusted comment:` line) or a cosign PEM `SubjectPublicKeyInfo` for P-256.
pub fn parse_trusted_key(text: &str) -> Result<TrustedKey, SigError> {
    if text.len() > MAX_KEY_BYTES {
        return Err(SigError::TooLarge("trusted key"));
    }
    let t = text.trim();
    if t.starts_with(PEM_BEGIN) {
        return parse_cosign_pem(t);
    }
    let mut lines = t.lines().map(str::trim).filter(|l| !l.is_empty());
    let first = lines.next().ok_or_else(|| malformed("empty key"))?;
    let (comment, b64) = if let Some(c) = first.strip_prefix(UNTRUSTED_PREFIX) {
        let c = c.trim();
        if c.len() > MAX_COMMENT_BYTES {
            return Err(SigError::TooLarge("key comment"));
        }
        let b64 = lines
            .next()
            .ok_or_else(|| malformed("minisign key line missing"))?;
        (c.to_string(), b64)
    } else {
        (String::new(), first)
    };
    if lines.next().is_some() {
        return Err(malformed("unexpected lines after the minisign key"));
    }
    // minisign-verify validates the encoding and algorithm; base64 gives us the bytes.
    minisign_verify::PublicKey::from_base64(b64)
        .map_err(|e| malformed(format!("minisign key: {e}")))?;
    let bin = B64
        .decode(b64)
        .map_err(|_| malformed("minisign key: invalid base64"))?;
    if bin.len() != 42 || (bin[..2] != MINISIGN_LEGACY && bin[..2] != MINISIGN_PREHASHED) {
        return Err(malformed("minisign key: wrong length or algorithm"));
    }
    Ok(TrustedKey {
        alg_family: KeyFamily::Minisign,
        key_id: bin[2..10].to_vec(),
        public_key: bin[10..42].to_vec(),
        comment,
    })
}

fn parse_cosign_pem(t: &str) -> Result<TrustedKey, SigError> {
    let rest = t
        .strip_prefix(PEM_BEGIN)
        .ok_or_else(|| malformed("PEM header"))?;
    let end = rest
        .find(PEM_END)
        .ok_or_else(|| malformed("PEM footer missing"))?;
    if !rest[end + PEM_END.len()..].trim().is_empty() {
        return Err(malformed("data after the PEM footer"));
    }
    let body: String = rest[..end]
        .chars()
        .filter(|c| !c.is_ascii_whitespace())
        .collect();
    let der = B64
        .decode(body.as_bytes())
        .map_err(|_| malformed("PEM: invalid base64"))?;
    cosign_key_from_der(&der)
}

/// Validate a P-256 SPKI DER key and canonicalise it.
fn cosign_key_from_der(der: &[u8]) -> Result<TrustedKey, SigError> {
    // A SubjectPublicKeyInfo is a DER SEQUENCE; a bare SEC1 point (0x04 …) is not accepted.
    if der.first() != Some(&0x30) {
        return Err(malformed("cosign key: not a SubjectPublicKeyInfo"));
    }
    let parsed = UnparsedPublicKey::new(&ECDSA_P256_SHA256_ASN1, der)
        .parse()
        .map_err(|_| malformed("cosign key: not a P-256 public key"))?;
    let canonical = parsed
        .as_der()
        .map_err(|_| malformed("cosign key: cannot encode"))?
        .as_ref()
        .to_vec();
    let id = sha256(&canonical);
    Ok(TrustedKey {
        alg_family: KeyFamily::Cosign,
        key_id: id[..8].to_vec(),
        public_key: canonical,
        comment: String::new(),
    })
}

/// Display form of a key id: minisign = the 8-byte id read as a little-endian u64 in upper-case
/// hex (as `minisign`/`rsign` print it); cosign = lower-case hex of `sha256(SPKI DER)[..8]`.
pub fn key_id_hex(k: &TrustedKey) -> String {
    match k.alg_family {
        KeyFamily::Minisign => k.key_id.iter().rev().map(|b| format!("{b:02X}")).collect(),
        KeyFamily::Cosign => k.key_id.iter().map(|b| format!("{b:02x}")).collect(),
    }
}

/// `sha256(alg byte ‖ public_key)` with [`KeyFamily::alg_byte`]: the key identity stored in
/// ext 113 `ModuleSig.key_fingerprint`. Two keys with the same 8-byte minisign key id but
/// different public keys have different fingerprints.
pub fn key_fingerprint(k: &TrustedKey) -> [u8; 32] {
    let mut ctx = digest::Context::new(&digest::SHA256);
    ctx.update(&[k.alg_family.alg_byte()]);
    ctx.update(&k.public_key);
    to32(ctx.finish().as_ref())
}

// ---------------------------------------------------------------------------------------
// Signatures
// ---------------------------------------------------------------------------------------

/// Parse one signature: a `.minisig` file, a cosign raw base64 DER signature, or a Sigstore
/// bundle v0.3 JSON (keyed only).
pub fn parse_signature(raw: &[u8]) -> Result<ParsedSig, SigError> {
    if raw.len() > MAX_SIG_BYTES {
        return Err(SigError::TooLarge("signature"));
    }
    let text = std::str::from_utf8(raw).map_err(|_| malformed("signature is not UTF-8 text"))?;
    let t = text.trim();
    if t.is_empty() {
        return Err(malformed("empty signature"));
    }
    if t.starts_with(UNTRUSTED_PREFIX) {
        parse_minisig(t)
    } else if t.starts_with('{') || t.starts_with('[') {
        parse_bundle(t)
    } else {
        let der = B64
            .decode(t)
            .map_err(|_| malformed("cosign signature: invalid base64"))?;
        cosign_sig(der, None, None)
    }
}

fn parse_minisig(t: &str) -> Result<ParsedSig, SigError> {
    let lines: Vec<&str> = t.lines().collect();
    if lines.len() < 4 {
        return Err(malformed("minisig: expected 4 lines"));
    }
    if lines[4..].iter().any(|l| !l.trim().is_empty()) {
        return Err(malformed(
            "minisig: unexpected lines after the global signature",
        ));
    }
    let untrusted = lines[0]
        .strip_prefix(UNTRUSTED_PREFIX)
        .ok_or_else(|| malformed("minisig: untrusted comment"))?;
    if untrusted.len() > MAX_COMMENT_BYTES {
        return Err(SigError::TooLarge("untrusted comment"));
    }
    let trusted = lines[2]
        .strip_prefix(TRUSTED_PREFIX)
        .ok_or_else(|| malformed("minisig: trusted comment"))?;
    if trusted.len() > MAX_COMMENT_BYTES {
        return Err(SigError::TooLarge("trusted comment"));
    }
    let (sig_b64, global_b64) = (lines[1].trim(), lines[3].trim());
    let text = format!("{}\n{sig_b64}\n{}\n{global_b64}\n", lines[0], lines[2]);
    // minisign-verify is the authority on the format; base64 gives us the key id and alg.
    minisign_verify::Signature::decode(&text).map_err(|e| malformed(format!("minisig: {e}")))?;
    let bin = B64
        .decode(sig_b64)
        .map_err(|_| malformed("minisig: invalid base64"))?;
    if bin.len() != 74 {
        return Err(malformed("minisig: wrong signature length"));
    }
    B64.decode(global_b64)
        .ok()
        .filter(|g| g.len() == 64)
        .ok_or_else(|| malformed("minisig: invalid global signature"))?;
    let alg = match [bin[0], bin[1]] {
        MINISIGN_LEGACY => Alg::MinisignEd,
        MINISIGN_PREHASHED => Alg::MinisignEdPrehashed,
        _ => return Err(malformed("minisig: unsupported algorithm")),
    };
    Ok(ParsedSig {
        alg,
        key_id: bin[2..10].to_vec(),
        body: Body::Minisign { text },
    })
}

fn parse_bundle(t: &str) -> Result<ParsedSig, SigError> {
    let v: serde_json::Value =
        serde_json::from_str(t).map_err(|e| malformed(format!("bundle: invalid JSON: {e}")))?;
    let obj = v
        .as_object()
        .ok_or_else(|| malformed("bundle: not an object"))?;
    let media = obj
        .get("mediaType")
        .and_then(|m| m.as_str())
        .ok_or_else(|| malformed("bundle: mediaType missing"))?;
    if !BUNDLE_MEDIA_TYPES.contains(&media) {
        return Err(malformed("bundle: unsupported mediaType"));
    }
    let vm = obj
        .get("verificationMaterial")
        .and_then(|m| m.as_object())
        .ok_or_else(|| malformed("bundle: verificationMaterial missing"))?;
    let hint = match vm.get("publicKey") {
        Some(pk) => {
            let pk = pk
                .as_object()
                .ok_or_else(|| malformed("bundle: publicKey is not an object"))?;
            match pk.get("hint") {
                None | Some(serde_json::Value::Null) => None,
                Some(serde_json::Value::String(h)) if h.len() <= MAX_COMMENT_BYTES => {
                    Some(h.clone())
                }
                Some(serde_json::Value::String(_)) => return Err(SigError::TooLarge("hint")),
                Some(_) => return Err(malformed("bundle: hint is not a string")),
            }
        }
        None => {
            let keyless = ["certificate", "x509CertificateChain", "tlogEntries"]
                .iter()
                .any(|k| vm.contains_key(*k));
            return Err(if keyless {
                SigError::KeylessUnsupported
            } else {
                malformed("bundle: no publicKey verification material")
            });
        }
    };
    let ms = obj
        .get("messageSignature")
        .and_then(|m| m.as_object())
        .ok_or_else(|| malformed("bundle: messageSignature missing (DSSE is not supported)"))?;
    let sig_b64 = ms
        .get("signature")
        .and_then(|s| s.as_str())
        .ok_or_else(|| malformed("bundle: messageSignature.signature missing"))?;
    let der = B64
        .decode(sig_b64)
        .map_err(|_| malformed("bundle: signature is not base64"))?;
    let digest = match ms.get("messageDigest") {
        None | Some(serde_json::Value::Null) => None,
        Some(md) => {
            if md.get("algorithm").and_then(|a| a.as_str()) != Some("SHA2_256") {
                return Err(malformed(
                    "bundle: messageDigest.algorithm must be SHA2_256",
                ));
            }
            let d = md
                .get("digest")
                .and_then(|d| d.as_str())
                .and_then(|d| B64.decode(d).ok())
                .filter(|d| d.len() == 32)
                .ok_or_else(|| malformed("bundle: messageDigest.digest must be 32 bytes"))?;
            Some(to32(&d))
        }
    };
    cosign_sig(der, digest, hint)
}

fn cosign_sig(
    der: Vec<u8>,
    digest: Option<[u8; 32]>,
    hint: Option<String>,
) -> Result<ParsedSig, SigError> {
    if !(DER_SIG_MIN..=DER_SIG_MAX).contains(&der.len()) || der[0] != 0x30 {
        return Err(malformed(
            "cosign signature: not an ASN.1 DER ECDSA P-256 signature",
        ));
    }
    Ok(ParsedSig {
        alg: Alg::CosignP256,
        key_id: Vec::new(),
        body: Body::Cosign { der, digest, hint },
    })
}

// ---------------------------------------------------------------------------------------
// Verification
// ---------------------------------------------------------------------------------------

/// Verify `sigs` over `bytes` against `keys`. Valid if at least one signature verifies with a
/// trusted key; the evidence lists every verified signature (in input order, duplicates
/// dropped) and none of the failed ones. If none verifies, the most specific failure is
/// returned (`DigestMismatch` > `BadSignature` > `Malformed` > `UnknownKey`); no signature at
/// all is `Unsigned`, more than [`MAX_SIGS`] is `TooLarge`.
pub fn verify_module(
    bytes: &[u8],
    sigs: &[ParsedSig],
    keys: &[TrustedKey],
) -> Result<Vec<ModuleSigEvidence>, SigError> {
    if sigs.len() > MAX_SIGS {
        return Err(SigError::TooLarge("signature count"));
    }
    collect(sigs.iter().map(|s| verify_one(bytes, s, keys.iter())))
}

/// Activation re-check (D-P14-10 B1(c)): every stored signature is re-verified over `bytes`
/// with a key of the stored fingerprint from `keys` (the activating tenant's effective keys).
/// A key with the same key id but another fingerprint is never used. Same result rules as
/// [`verify_module`].
pub fn reverify(
    bytes: &[u8],
    stored: &[ext::ModuleSig],
    keys: &[TrustedKey],
) -> Result<Vec<ModuleSigEvidence>, SigError> {
    if stored.len() > MAX_SIGS {
        return Err(SigError::TooLarge("signature count"));
    }
    collect(stored.iter().map(|s| reverify_one(bytes, s, keys)))
}

fn collect(
    results: impl Iterator<Item = Result<ModuleSigEvidence, SigError>>,
) -> Result<Vec<ModuleSigEvidence>, SigError> {
    let mut ok: Vec<ModuleSigEvidence> = Vec::new();
    let mut worst = SigError::Unsigned;
    for r in results {
        match r {
            Ok(ev) => {
                if !ok.contains(&ev) {
                    ok.push(ev);
                }
            }
            Err(e) => {
                if e.rank() > worst.rank() {
                    worst = e;
                }
            }
        }
    }
    if ok.is_empty() { Err(worst) } else { Ok(ok) }
}

fn reverify_one(
    bytes: &[u8],
    stored: &ext::ModuleSig,
    keys: &[TrustedKey],
) -> Result<ModuleSigEvidence, SigError> {
    if stored.sig.len() > MAX_SIG_BYTES {
        return Err(SigError::TooLarge("stored signature"));
    }
    let alg = Alg::from_u8(stored.alg)
        .ok_or_else(|| malformed(format!("stored signature: unknown alg {}", stored.alg)))?;
    let parsed = match alg.family() {
        KeyFamily::Minisign => parse_signature(&stored.sig)?,
        KeyFamily::Cosign => cosign_sig(stored.sig.clone(), None, None)?,
    };
    if parsed.alg != alg {
        return Err(malformed(
            "stored signature: alg does not match the signature",
        ));
    }
    let matching = keys
        .iter()
        .filter(|k| k.alg_family == alg.family() && key_fingerprint(k) == stored.key_fingerprint);
    verify_one(bytes, &parsed, matching)
}

fn verify_one<'k>(
    bytes: &[u8],
    sig: &ParsedSig,
    keys: impl Iterator<Item = &'k TrustedKey>,
) -> Result<ModuleSigEvidence, SigError> {
    match &sig.body {
        Body::Minisign { text } => {
            // Key ids are matched before any verification.
            let candidates: Vec<&TrustedKey> = keys
                .filter(|k| k.alg_family == KeyFamily::Minisign && k.key_id == sig.key_id)
                .collect();
            if candidates.is_empty() {
                return Err(SigError::UnknownKey);
            }
            let decoded = minisign_verify::Signature::decode(text)
                .map_err(|e| malformed(format!("minisig: {e}")))?;
            for k in candidates {
                let Some(pk) = minisign_public_key(k) else {
                    continue;
                };
                if pk.verify(bytes, &decoded, true).is_ok() {
                    return Ok(evidence(sig, k));
                }
            }
            Err(SigError::BadSignature)
        }
        Body::Cosign { der, digest, hint } => {
            if let Some(d) = digest
                && *d != sha256(bytes)
            {
                return Err(SigError::DigestMismatch);
            }
            let mut candidates: Vec<&TrustedKey> =
                keys.filter(|k| k.alg_family == KeyFamily::Cosign).collect();
            if candidates.is_empty() {
                return Err(SigError::UnknownKey);
            }
            // The hint (cosign: base64 sha256 of the key DER) only orders the attempts.
            if let Some(h) = hint {
                candidates.sort_by_key(|k| B64.encode(sha256(&k.public_key)) != *h);
            }
            for k in candidates {
                if UnparsedPublicKey::new(&ECDSA_P256_SHA256_ASN1, &k.public_key)
                    .verify(bytes, der)
                    .is_ok()
                {
                    return Ok(evidence(sig, k));
                }
            }
            Err(SigError::BadSignature)
        }
    }
}

fn minisign_public_key(k: &TrustedKey) -> Option<minisign_verify::PublicKey> {
    if k.key_id.len() != 8 || k.public_key.len() != 32 {
        return None;
    }
    let mut bin = Vec::with_capacity(42);
    bin.extend_from_slice(&MINISIGN_LEGACY);
    bin.extend_from_slice(&k.key_id);
    bin.extend_from_slice(&k.public_key);
    minisign_verify::PublicKey::from_base64(&B64.encode(bin)).ok()
}

fn evidence(sig: &ParsedSig, k: &TrustedKey) -> ModuleSigEvidence {
    ModuleSigEvidence {
        alg: sig.alg,
        key_id: k.key_id.clone(),
        key_fingerprint: key_fingerprint(k),
        sig: sig.stored_bytes().to_vec(),
    }
}

fn sha256(b: &[u8]) -> [u8; 32] {
    to32(digest::digest(&digest::SHA256, b).as_ref())
}

fn to32(b: &[u8]) -> [u8; 32] {
    let mut out = [0u8; 32];
    let n = b.len().min(32);
    out[..n].copy_from_slice(&b[..n]);
    out
}
