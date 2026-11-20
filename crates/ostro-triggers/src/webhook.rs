//! Webhook signature verifiers (P8-D12, TRIG-01).
//!
//! Pure functions: no I/O and no clock reads. The caller passes the current time (`now_unix`,
//! seconds) from the injected `Clock`. Verification runs on the **raw body bytes**; nothing in
//! this module parses the body before the signature has been checked.
//!
//! Every signature comparison goes through [`Mac::verify_slice`] (constant time). This module
//! deliberately contains no equality operators at all; `tests/webhook_verify.rs` greps for that.
//!
//! A successful [`verify`] returns a [`VerifiedDelivery`] token. It cannot be built any other
//! way, and every body-parsing helper ([`slack_challenge`], [`crate::extract`]) takes it, so the
//! type system enforces verify-before-parse.
//!
//! | kind | headers | signed content | MAC / encoding | replay window |
//! |------|---------|----------------|----------------|---------------|
//! | Stripe | `stripe-signature: t=…,v1=…[,v1=…]` | `{t}.{body}` | HMAC-SHA256 hex, secret used as-is | ±tolerance |
//! | GitHub | `x-hub-signature-256: sha256=…` | `{body}` | HMAC-SHA256 hex | none |
//! | Slack | `x-slack-request-timestamp`, `x-slack-signature: v0=…` | `v0:{ts}:{body}` | HMAC-SHA256 hex | ±tolerance |
//! | Standard Webhooks | `webhook-id`, `webhook-timestamp`, `webhook-signature: v1,… v1,…` | `{id}.{ts}.{body}` | HMAC-SHA256 base64, secret `whsec_<base64>` | ±tolerance |
//! | generic HMAC | configurable | `{body}` or `{ts}.{body}` | SHA-256 (or opt-in legacy SHA-1), hex or base64 | optional |

use hmac::digest::{KeyInit, Mac};
use hmac::{Hmac, SimpleHmac};
use sha2::Sha256;

/// Meta `Webhook.verifier` ordinals, mirrored from `ostro-meta` `records.rs` (this crate does not
/// depend on ostro-meta; a test checks the values match).
pub const VERIFIER_NONE: u8 = 0;
pub const VERIFIER_STRIPE: u8 = 1;
pub const VERIFIER_GITHUB: u8 = 2;
pub const VERIFIER_SLACK: u8 = 3;
pub const VERIFIER_GENERIC_HMAC: u8 = 4;
pub const VERIFIER_STANDARD_WEBHOOKS: u8 = 5;

/// Default replay window in seconds (Stripe, Slack, Standard Webhooks).
pub const DEFAULT_TOLERANCE_S: i64 = 300;

/// Verifier kind, one-to-one with the meta `Webhook.verifier` byte.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum VerifierKind {
    None,
    Stripe,
    GitHub,
    Slack,
    GenericHmac,
    StandardWebhooks,
}

impl VerifierKind {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            VERIFIER_NONE => Some(Self::None),
            VERIFIER_STRIPE => Some(Self::Stripe),
            VERIFIER_GITHUB => Some(Self::GitHub),
            VERIFIER_SLACK => Some(Self::Slack),
            VERIFIER_GENERIC_HMAC => Some(Self::GenericHmac),
            VERIFIER_STANDARD_WEBHOOKS => Some(Self::StandardWebhooks),
            _ => None,
        }
    }

    pub fn as_u8(self) -> u8 {
        match self {
            Self::None => VERIFIER_NONE,
            Self::Stripe => VERIFIER_STRIPE,
            Self::GitHub => VERIFIER_GITHUB,
            Self::Slack => VERIFIER_SLACK,
            Self::GenericHmac => VERIFIER_GENERIC_HMAC,
            Self::StandardWebhooks => VERIFIER_STANDARD_WEBHOOKS,
        }
    }
}

/// Generic-HMAC hash function. SHA-1 is legacy and opt-in (config name `legacy_sha1`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HmacAlgorithm {
    Sha256,
    LegacySha1,
}

/// Generic-HMAC signature encoding in the header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SigEncoding {
    Hex,
    Base64,
}

/// Generic-HMAC signed content.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SignedTemplate {
    /// `{body}`.
    Body,
    /// `{ts}.{body}`, `ts` being the timestamp header value exactly as received.
    TsDotBody,
}

/// Generic-HMAC options.
///
/// Stored in the meta `Webhook.verifier_config` blob as a JSON object (format owned by Phase 8,
/// 08-06): `{"header": "x-sig", "prefix": "sha256=", "algorithm": "sha256" | "legacy_sha1",
/// "encoding": "hex" | "base64", "timestamp_header": "x-ts", "template": "{body}" |
/// "{ts}.{body}"}`. Only `header` is required. Unknown keys are rejected. The tolerance comes
/// from `Webhook.tolerance_s` (0 = no replay window).
///
/// Valid combinations: no timestamp header with `{body}`, or a timestamp header with
/// `{ts}.{body}` (optionally with a tolerance). A timestamp header with `{body}` would be an
/// unsigned timestamp and is rejected as [`HookError::Config`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GenericHmacSpec {
    /// Signature header name (lower case).
    pub header: String,
    /// Required literal prefix before the encoded signature, e.g. `sha256=`.
    pub prefix: Option<String>,
    pub algorithm: HmacAlgorithm,
    pub encoding: SigEncoding,
    /// Timestamp header name (lower case).
    pub timestamp_header: Option<String>,
    pub template: SignedTemplate,
    /// Replay window in seconds; `None` = not checked.
    pub tolerance_s: Option<i64>,
}

impl GenericHmacSpec {
    fn validate(&self) -> Result<(), HookError> {
        if self.header.is_empty() || !is_header_name(&self.header) {
            return Err(HookError::Config);
        }
        if let Some(ts) = &self.timestamp_header
            && (ts.is_empty() || !is_header_name(ts))
        {
            return Err(HookError::Config);
        }
        match (&self.timestamp_header, self.template) {
            (None, SignedTemplate::Body) | (Some(_), SignedTemplate::TsDotBody) => {}
            _ => return Err(HookError::Config),
        }
        match self.tolerance_s {
            None => {}
            Some(t) if t > 0 && self.timestamp_header.is_some() => {}
            Some(_) => return Err(HookError::Config),
        }
        Ok(())
    }

    /// Parse the `verifier_config` JSON blob (see the type docs).
    pub fn parse_config(config: &[u8], tolerance_s: u32) -> Result<Self, HookError> {
        let v: serde_json::Value = serde_json::from_slice(config).map_err(|_| HookError::Config)?;
        let obj = v.as_object().ok_or(HookError::Config)?;
        let mut spec = GenericHmacSpec {
            header: String::new(),
            prefix: None,
            algorithm: HmacAlgorithm::Sha256,
            encoding: SigEncoding::Hex,
            timestamp_header: None,
            template: SignedTemplate::Body,
            tolerance_s: if tolerance_s > 0 {
                Some(i64::from(tolerance_s))
            } else {
                None
            },
        };
        for (key, value) in obj {
            let s = value.as_str().ok_or(HookError::Config)?;
            match key.as_str() {
                "header" => spec.header = s.to_ascii_lowercase(),
                "prefix" => spec.prefix = Some(s.to_string()),
                "algorithm" => {
                    spec.algorithm = match s {
                        "sha256" => HmacAlgorithm::Sha256,
                        "legacy_sha1" => HmacAlgorithm::LegacySha1,
                        _ => return Err(HookError::Config),
                    }
                }
                "encoding" => {
                    spec.encoding = match s {
                        "hex" => SigEncoding::Hex,
                        "base64" => SigEncoding::Base64,
                        _ => return Err(HookError::Config),
                    }
                }
                "timestamp_header" => spec.timestamp_header = Some(s.to_ascii_lowercase()),
                "template" => {
                    spec.template = match s {
                        "{body}" => SignedTemplate::Body,
                        "{ts}.{body}" => SignedTemplate::TsDotBody,
                        _ => return Err(HookError::Config),
                    }
                }
                _ => return Err(HookError::Config),
            }
        }
        spec.validate()?;
        Ok(spec)
    }

    /// Encode as the `verifier_config` JSON blob (inverse of [`Self::parse_config`]).
    pub fn to_config(&self) -> Vec<u8> {
        let mut m = serde_json::Map::new();
        m.insert("header".into(), self.header.clone().into());
        if let Some(p) = &self.prefix {
            m.insert("prefix".into(), p.clone().into());
        }
        let alg = match self.algorithm {
            HmacAlgorithm::Sha256 => "sha256",
            HmacAlgorithm::LegacySha1 => "legacy_sha1",
        };
        m.insert("algorithm".into(), alg.into());
        let enc = match self.encoding {
            SigEncoding::Hex => "hex",
            SigEncoding::Base64 => "base64",
        };
        m.insert("encoding".into(), enc.into());
        if let Some(t) = &self.timestamp_header {
            m.insert("timestamp_header".into(), t.clone().into());
        }
        let tpl = match self.template {
            SignedTemplate::Body => "{body}",
            SignedTemplate::TsDotBody => "{ts}.{body}",
        };
        m.insert("template".into(), tpl.into());
        serde_json::to_vec(&serde_json::Value::Object(m)).unwrap_or_default()
    }
}

/// A verifier, built from the meta `Webhook` record with [`VerifierSpec::from_meta`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VerifierSpec {
    /// No verification (unauthenticated hook). Whether to allow it is the caller's policy.
    None,
    Stripe {
        tolerance_s: i64,
    },
    GitHub,
    Slack {
        tolerance_s: i64,
    },
    StandardWebhooks {
        tolerance_s: i64,
    },
    GenericHmac(GenericHmacSpec),
}

impl VerifierSpec {
    /// Map the meta `Webhook` fields one-to-one: `verifier` (u8 ordinal), `verifier_config`
    /// (generic HMAC JSON, empty for every other kind) and `tolerance_s` (0 = the verifier
    /// default: 300 s for Stripe, Slack and Standard Webhooks, no window for generic HMAC; must
    /// be 0 for GitHub and None, which carry no timestamp).
    ///
    /// Takes the fields rather than `&ostro_meta::Webhook` because ostro-triggers is a pure
    /// crate without an ostro-meta dependency; callers pass
    /// `(w.verifier, &w.verifier_config, w.tolerance_s)`.
    pub fn from_meta(
        verifier: u8,
        verifier_config: &[u8],
        tolerance_s: u32,
    ) -> Result<Self, HookError> {
        let kind = VerifierKind::from_u8(verifier).ok_or(HookError::Config)?;
        let tol = if tolerance_s > 0 {
            i64::from(tolerance_s)
        } else {
            DEFAULT_TOLERANCE_S
        };
        if !matches!(kind, VerifierKind::GenericHmac) && !verifier_config.is_empty() {
            return Err(HookError::Config);
        }
        if matches!(kind, VerifierKind::None | VerifierKind::GitHub) && tolerance_s > 0 {
            return Err(HookError::Config);
        }
        Ok(match kind {
            VerifierKind::None => Self::None,
            VerifierKind::GitHub => Self::GitHub,
            VerifierKind::Stripe => Self::Stripe { tolerance_s: tol },
            VerifierKind::Slack => Self::Slack { tolerance_s: tol },
            VerifierKind::StandardWebhooks => Self::StandardWebhooks { tolerance_s: tol },
            VerifierKind::GenericHmac => {
                Self::GenericHmac(GenericHmacSpec::parse_config(verifier_config, tolerance_s)?)
            }
        })
    }

    /// Inverse of [`Self::from_meta`]: `(verifier, verifier_config, tolerance_s)`.
    pub fn to_meta(&self) -> (u8, Vec<u8>, u32) {
        let tol = |t: i64| u32::try_from(t).unwrap_or(u32::MAX);
        match self {
            Self::None => (VERIFIER_NONE, Vec::new(), 0),
            Self::GitHub => (VERIFIER_GITHUB, Vec::new(), 0),
            Self::Stripe { tolerance_s } => (VERIFIER_STRIPE, Vec::new(), tol(*tolerance_s)),
            Self::Slack { tolerance_s } => (VERIFIER_SLACK, Vec::new(), tol(*tolerance_s)),
            Self::StandardWebhooks { tolerance_s } => {
                (VERIFIER_STANDARD_WEBHOOKS, Vec::new(), tol(*tolerance_s))
            }
            Self::GenericHmac(g) => (
                VERIFIER_GENERIC_HMAC,
                g.to_config(),
                g.tolerance_s.map(tol).unwrap_or(0),
            ),
        }
    }

    pub fn kind(&self) -> VerifierKind {
        match self {
            Self::None => VerifierKind::None,
            Self::Stripe { .. } => VerifierKind::Stripe,
            Self::GitHub => VerifierKind::GitHub,
            Self::Slack { .. } => VerifierKind::Slack,
            Self::StandardWebhooks { .. } => VerifierKind::StandardWebhooks,
            Self::GenericHmac(_) => VerifierKind::GenericHmac,
        }
    }
}

/// Verification failure. `Display` never includes a header value or the secret.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum HookError {
    /// A signature or timestamp header is present but unparsable.
    #[error("malformed webhook signature header")]
    Malformed,
    /// A required header is absent. Generic HMAC reports `"signature"` / `"timestamp"`.
    #[error("missing webhook header `{0}`")]
    MissingHeader(&'static str),
    /// The timestamp is outside the replay window.
    #[error("webhook timestamp outside the replay window")]
    Stale,
    /// No signature matched.
    #[error("webhook signature mismatch")]
    BadSignature,
    /// The verifier spec or the secret is invalid.
    #[error("invalid webhook verifier configuration")]
    Config,
}

/// Proof that a delivery passed [`verify`]. Zero-size, not constructible outside this module
/// (`PartialEq` only so tests can compare `Result`s; it carries no data).
#[derive(Debug, PartialEq, Eq)]
pub struct VerifiedDelivery {
    _private: (),
}

/// Verify a delivery. `headers` carries lower-cased names (matching is ASCII case-insensitive
/// anyway) and raw values; `body` is the raw request body; `now_unix` is seconds since the epoch
/// from the injected clock.
pub fn verify(
    spec: &VerifierSpec,
    secret: &[u8],
    headers: &[(String, Vec<u8>)],
    body: &[u8],
    now_unix: i64,
) -> Result<VerifiedDelivery, HookError> {
    let ok = VerifiedDelivery { _private: () };
    if matches!(spec, VerifierSpec::None) {
        return Ok(ok);
    }
    if secret.is_empty() {
        return Err(HookError::Config);
    }
    match spec {
        VerifierSpec::None => {}
        VerifierSpec::Stripe { tolerance_s } => {
            verify_stripe(secret, headers, body, now_unix, *tolerance_s)?
        }
        VerifierSpec::GitHub => verify_github(secret, headers, body)?,
        VerifierSpec::Slack { tolerance_s } => {
            verify_slack(secret, headers, body, now_unix, *tolerance_s)?
        }
        VerifierSpec::StandardWebhooks { tolerance_s } => {
            verify_standard(secret, headers, body, now_unix, *tolerance_s)?
        }
        VerifierSpec::GenericHmac(g) => verify_generic(g, secret, headers, body, now_unix)?,
    }
    Ok(ok)
}

/// Slack `url_verification`: returns the `challenge` to echo, if the (verified) body is one.
pub fn slack_challenge(_proof: &VerifiedDelivery, body: &[u8]) -> Option<String> {
    let v: serde_json::Value = serde_json::from_slice(body).ok()?;
    if !matches!(v.get("type")?.as_str()?, "url_verification") {
        return None;
    }
    Some(v.get("challenge")?.as_str()?.to_string())
}

/// First header with this name (ASCII case-insensitive).
pub fn header<'a>(headers: &'a [(String, Vec<u8>)], name: &str) -> Option<&'a [u8]> {
    headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_slice())
}

// ---------------------------------------------------------------------------------------------

fn is_header_name(s: &str) -> bool {
    s.bytes()
        .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b))
}

fn header_str<'a>(
    headers: &'a [(String, Vec<u8>)],
    name: &'static str,
    report: &'static str,
) -> Result<&'a str, HookError> {
    let raw = header(headers, name).ok_or(HookError::MissingHeader(report))?;
    std::str::from_utf8(raw)
        .map(str::trim)
        .map_err(|_| HookError::Malformed)
}

fn dyn_header_str<'a>(
    headers: &'a [(String, Vec<u8>)],
    name: &str,
    report: &'static str,
) -> Result<&'a str, HookError> {
    let raw = header(headers, name).ok_or(HookError::MissingHeader(report))?;
    std::str::from_utf8(raw)
        .map(str::trim)
        .map_err(|_| HookError::Malformed)
}

/// Parse a decimal unix timestamp (seconds) and check the replay window.
fn check_ts(ts: &str, now: i64, tolerance_s: Option<i64>) -> Result<(), HookError> {
    if ts.is_empty() || !ts.bytes().all(|b| b.is_ascii_digit() || matches!(b, b'-')) {
        return Err(HookError::Malformed);
    }
    let t: i64 = ts.parse().map_err(|_| HookError::Malformed)?;
    if let Some(tol) = tolerance_s {
        let skew = (i128::from(now) - i128::from(t)).abs();
        if skew > i128::from(tol) {
            return Err(HookError::Stale);
        }
    }
    Ok(())
}

/// Constant-time MAC check over the concatenation of `parts`.
fn mac_verify(
    alg: HmacAlgorithm,
    key: &[u8],
    parts: &[&[u8]],
    tag: &[u8],
) -> Result<(), HookError> {
    let res = match alg {
        HmacAlgorithm::Sha256 => {
            let mut m =
                <Hmac<Sha256> as KeyInit>::new_from_slice(key).map_err(|_| HookError::Config)?;
            for p in parts {
                Mac::update(&mut m, p);
            }
            m.verify_slice(tag)
        }
        HmacAlgorithm::LegacySha1 => {
            let mut m = <SimpleHmac<sha1::Sha1> as KeyInit>::new_from_slice(key)
                .map_err(|_| HookError::Config)?;
            for p in parts {
                Mac::update(&mut m, p);
            }
            m.verify_slice(tag)
        }
    };
    res.map_err(|_| HookError::BadSignature)
}

fn verify_stripe(
    secret: &[u8],
    headers: &[(String, Vec<u8>)],
    body: &[u8],
    now: i64,
    tolerance_s: i64,
) -> Result<(), HookError> {
    let hv = header_str(headers, "stripe-signature", "stripe-signature")?;
    let mut t: Option<&str> = None;
    let mut sigs: Vec<&str> = Vec::new();
    for part in hv.split(',') {
        // Only `t` and `v1` are used. `v0` (test events) and `secret`/`destination`/`payload`
        // (unsigned debug metadata) and any unknown entry are ignored.
        match part.trim().split_once('=') {
            Some(("t", v)) => {
                if t.replace(v).is_some() {
                    return Err(HookError::Malformed);
                }
            }
            Some(("v1", v)) => sigs.push(v),
            _ => {}
        }
    }
    let t = t.ok_or(HookError::Malformed)?;
    check_ts(t, now, Some(tolerance_s))?;
    for s in sigs {
        let Some(raw) = hex_decode(s) else { continue };
        // The endpoint secret (`whsec_…`) is used as-is, not base64-decoded.
        if mac_verify(
            HmacAlgorithm::Sha256,
            secret,
            &[t.as_bytes(), b".", body],
            &raw,
        )
        .is_ok()
        {
            return Ok(());
        }
    }
    Err(HookError::BadSignature)
}

fn verify_github(
    secret: &[u8],
    headers: &[(String, Vec<u8>)],
    body: &[u8],
) -> Result<(), HookError> {
    let hv = header_str(headers, "x-hub-signature-256", "x-hub-signature-256")?;
    let enc = hv.strip_prefix("sha256=").ok_or(HookError::Malformed)?;
    let raw = hex_decode(enc).ok_or(HookError::Malformed)?;
    mac_verify(HmacAlgorithm::Sha256, secret, &[body], &raw)
}

fn verify_slack(
    secret: &[u8],
    headers: &[(String, Vec<u8>)],
    body: &[u8],
    now: i64,
    tolerance_s: i64,
) -> Result<(), HookError> {
    let ts = header_str(
        headers,
        "x-slack-request-timestamp",
        "x-slack-request-timestamp",
    )?;
    let sig = header_str(headers, "x-slack-signature", "x-slack-signature")?;
    check_ts(ts, now, Some(tolerance_s))?;
    let enc = sig.strip_prefix("v0=").ok_or(HookError::Malformed)?;
    let raw = hex_decode(enc).ok_or(HookError::Malformed)?;
    mac_verify(
        HmacAlgorithm::Sha256,
        secret,
        &[b"v0:", ts.as_bytes(), b":", body],
        &raw,
    )
}

fn verify_standard(
    secret: &[u8],
    headers: &[(String, Vec<u8>)],
    body: &[u8],
    now: i64,
    tolerance_s: i64,
) -> Result<(), HookError> {
    let encoded = secret.strip_prefix(b"whsec_").unwrap_or(secret);
    let key = std::str::from_utf8(encoded)
        .ok()
        .and_then(b64_decode)
        .filter(|k| !k.is_empty())
        .ok_or(HookError::Config)?;
    let id = header_str(headers, "webhook-id", "webhook-id")?;
    let ts = header_str(headers, "webhook-timestamp", "webhook-timestamp")?;
    let sigs = header_str(headers, "webhook-signature", "webhook-signature")?;
    check_ts(ts, now, Some(tolerance_s))?;
    let mut entries = 0usize;
    for entry in sigs.split_ascii_whitespace() {
        let Some((version, enc)) = entry.split_once(',') else {
            continue;
        };
        entries += 1;
        if !matches!(version, "v1") {
            continue;
        }
        let Some(raw) = b64_decode(enc) else { continue };
        let parts: [&[u8]; 5] = [id.as_bytes(), b".", ts.as_bytes(), b".", body];
        if mac_verify(HmacAlgorithm::Sha256, &key, &parts, &raw).is_ok() {
            return Ok(());
        }
    }
    if entries > 0 {
        Err(HookError::BadSignature)
    } else {
        Err(HookError::Malformed)
    }
}

fn verify_generic(
    g: &GenericHmacSpec,
    secret: &[u8],
    headers: &[(String, Vec<u8>)],
    body: &[u8],
    now: i64,
) -> Result<(), HookError> {
    g.validate()?;
    let sig = dyn_header_str(headers, &g.header, "signature")?;
    let ts = match &g.timestamp_header {
        Some(name) => {
            let ts = dyn_header_str(headers, name, "timestamp")?;
            check_ts(ts, now, g.tolerance_s)?;
            Some(ts)
        }
        None => None,
    };
    let enc = match &g.prefix {
        Some(p) => sig.strip_prefix(p.as_str()).ok_or(HookError::Malformed)?,
        None => sig,
    };
    let raw = match g.encoding {
        SigEncoding::Hex => hex_decode(enc),
        SigEncoding::Base64 => b64_decode(enc),
    }
    .ok_or(HookError::Malformed)?;
    match (g.template, ts) {
        (SignedTemplate::TsDotBody, Some(ts)) => {
            mac_verify(g.algorithm, secret, &[ts.as_bytes(), b".", body], &raw)
        }
        (SignedTemplate::Body, None) => mac_verify(g.algorithm, secret, &[body], &raw),
        _ => Err(HookError::Config),
    }
}

// ---------------------------------------------------------------------------------------------
// Local hex / base64 decoders (no encoding crate is a dependency of this crate; P8-D21 forbids
// new dependencies in wave 2).

fn hex_val(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

/// Decode hex (either case). `None` on odd length, a non-hex digit or empty input.
pub(crate) fn hex_decode(s: &str) -> Option<Vec<u8>> {
    let b = s.as_bytes();
    if b.is_empty() || !b.len().is_multiple_of(2) {
        return None;
    }
    b.as_chunks::<2>()
        .0
        .iter()
        .map(|[hi, lo]| Some((hex_val(*hi)? << 4) | hex_val(*lo)?))
        .collect()
}

fn b64_val(c: u8) -> Option<u32> {
    match c {
        b'A'..=b'Z' => Some(u32::from(c - b'A')),
        b'a'..=b'z' => Some(u32::from(c - b'a') + 26),
        b'0'..=b'9' => Some(u32::from(c - b'0') + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    }
}

/// Decode standard-alphabet base64 (RFC 4648 §4), padding optional. `None` on invalid input,
/// non-canonical trailing bits or empty input.
pub(crate) fn b64_decode(s: &str) -> Option<Vec<u8>> {
    let b = s.trim_end_matches('=').as_bytes();
    let pad = s.len() - b.len();
    // Padding, when present, must complete the final quantum; a lone sextet is never valid.
    if b.is_empty()
        || pad > 2
        || (pad > 0 && !(b.len() + pad).is_multiple_of(4))
        || matches!(b.len() % 4, 1)
    {
        return None;
    }
    let mut out = Vec::with_capacity(b.len() * 3 / 4);
    for chunk in b.chunks(4) {
        let mut n: u32 = 0;
        for (i, &c) in chunk.iter().enumerate() {
            n |= b64_val(c)? << (18 - 6 * i);
        }
        match chunk.len() {
            4 => out.extend_from_slice(&[(n >> 16) as u8, (n >> 8) as u8, n as u8]),
            3 => {
                if n & 0xff > 0 {
                    return None;
                }
                out.extend_from_slice(&[(n >> 16) as u8, (n >> 8) as u8]);
            }
            2 => {
                if n & 0xffff > 0 {
                    return None;
                }
                out.push((n >> 16) as u8);
            }
            _ => return None,
        }
    }
    Some(out)
}

// ---------------------------------------------------------------------------------------------
// Minimal SHA-1 (FIPS 180-4) behind the `digest` 0.11 traits, for opt-in legacy generic HMAC.
// The `sha1` crate is not a dependency of this crate and wave 2 adds none (P8-D21). Used only
// through `hmac::SimpleHmac`, so comparisons still go through `Mac::verify_slice`.
mod sha1 {
    use hmac::digest::common::BlockSizeUser;
    use hmac::digest::consts::{U20, U64};
    use hmac::digest::{FixedOutput, HashMarker, Output, OutputSizeUser, Update};

    #[derive(Clone)]
    pub struct Sha1 {
        h: [u32; 5],
        buf: [u8; 64],
        buf_len: usize,
        total: u64,
    }

    impl Default for Sha1 {
        fn default() -> Self {
            Self {
                h: [
                    0x6745_2301,
                    0xEFCD_AB89,
                    0x98BA_DCFE,
                    0x1032_5476,
                    0xC3D2_E1F0,
                ],
                buf: [0; 64],
                buf_len: 0,
                total: 0,
            }
        }
    }

    impl Sha1 {
        fn compress(h: &mut [u32; 5], block: &[u8]) {
            let mut w = [0u32; 80];
            for (i, word) in block.as_chunks::<4>().0.iter().enumerate() {
                w[i] = u32::from_be_bytes(*word);
            }
            for i in 16..80 {
                w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
            }
            let [mut a, mut b, mut c, mut d, mut e] = *h;
            for (i, wi) in w.iter().enumerate() {
                let (f, k) = match i {
                    0..=19 => ((b & c) | (!b & d), 0x5A82_7999),
                    20..=39 => (b ^ c ^ d, 0x6ED9_EBA1),
                    40..=59 => ((b & c) | (b & d) | (c & d), 0x8F1B_BCDC),
                    _ => (b ^ c ^ d, 0xCA62_C1D6),
                };
                let t = a
                    .rotate_left(5)
                    .wrapping_add(f)
                    .wrapping_add(e)
                    .wrapping_add(k)
                    .wrapping_add(*wi);
                e = d;
                d = c;
                c = b.rotate_left(30);
                b = a;
                a = t;
            }
            for (x, y) in h.iter_mut().zip([a, b, c, d, e]) {
                *x = x.wrapping_add(y);
            }
        }

        fn absorb(&mut self, mut data: &[u8]) {
            self.total = self.total.wrapping_add(data.len() as u64);
            if self.buf_len > 0 {
                let take = (64 - self.buf_len).min(data.len());
                self.buf[self.buf_len..self.buf_len + take].copy_from_slice(&data[..take]);
                self.buf_len += take;
                data = &data[take..];
                if self.buf_len < 64 {
                    return;
                }
                let block = self.buf;
                Self::compress(&mut self.h, &block);
                self.buf_len = 0;
            }
            let (blocks, rest) = data.as_chunks::<64>();
            for block in blocks {
                Self::compress(&mut self.h, block);
            }
            self.buf[..rest.len()].copy_from_slice(rest);
            self.buf_len = rest.len();
        }

        pub(super) fn digest_bytes(mut self) -> [u8; 20] {
            let bits = self.total.wrapping_mul(8);
            let mut pad = [0u8; 72];
            pad[0] = 0x80;
            let pad_len = if self.buf_len < 56 {
                56 - self.buf_len
            } else {
                120 - self.buf_len
            };
            // `absorb` adds to `total`; length was captured above.
            self.absorb(&pad[..pad_len]);
            self.absorb(&bits.to_be_bytes());
            let mut out = [0u8; 20];
            for (o, w) in out.as_chunks_mut::<4>().0.iter_mut().zip(self.h) {
                o.copy_from_slice(&w.to_be_bytes());
            }
            out
        }
    }

    impl HashMarker for Sha1 {}

    impl BlockSizeUser for Sha1 {
        type BlockSize = U64;
    }

    impl OutputSizeUser for Sha1 {
        type OutputSize = U20;
    }

    impl Update for Sha1 {
        fn update(&mut self, data: &[u8]) {
            self.absorb(data);
        }
    }

    impl FixedOutput for Sha1 {
        fn finalize_into(self, out: &mut Output<Self>) {
            out.copy_from_slice(&self.digest_bytes());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sha1_hex(data: &[u8]) -> String {
        let mut s = sha1::Sha1::default();
        hmac::digest::Update::update(&mut s, data);
        s.digest_bytes()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    }

    #[test]
    fn sha1_fips_vectors() {
        assert_eq!(sha1_hex(b""), "da39a3ee5e6b4b0d3255bfef95601890afd80709");
        assert_eq!(sha1_hex(b"abc"), "a9993e364706816aba3e25717850c26c9cd0d89d");
        assert_eq!(
            sha1_hex(b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"),
            "84983e441c3bd26ebaae4aa1f95129e5e54670f1"
        );
        assert_eq!(
            sha1_hex(&vec![b'a'; 1_000_000]),
            "34aa973cd4c4daa4f61eeb2bdbad27316534016f"
        );
    }

    #[test]
    fn sha1_incremental_matches_one_shot() {
        let data: Vec<u8> = (0..300u32).map(|i| (i * 7) as u8).collect();
        for split in [0, 1, 55, 56, 63, 64, 65, 128, 299] {
            let mut s = sha1::Sha1::default();
            hmac::digest::Update::update(&mut s, &data[..split]);
            hmac::digest::Update::update(&mut s, &data[split..]);
            let got: String = s
                .digest_bytes()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect();
            assert_eq!(got, sha1_hex(&data));
        }
    }

    #[test]
    fn hmac_sha1_rfc2202() {
        // Case 1: key 0x0b * 20, "Hi There"; case 6: 80-byte key (longer than the block).
        let tag = hex_decode("b617318655057264e28bc0b6fb378c8ef146be00").unwrap();
        mac_verify(HmacAlgorithm::LegacySha1, &[0x0b; 20], &[b"Hi There"], &tag).unwrap();
        let tag = hex_decode("aa4ae5e15272d00e95705637ce8a3b55ed402112").unwrap();
        mac_verify(
            HmacAlgorithm::LegacySha1,
            &[0xaa; 80],
            &[b"Test Using Larger Than Block-Size Key - Hash Key First"],
            &tag,
        )
        .unwrap();
    }

    #[test]
    fn hex_decoding() {
        assert_eq!(hex_decode("00ffAb"), Some(vec![0x00, 0xff, 0xab]));
        assert_eq!(hex_decode(""), None);
        assert_eq!(hex_decode("abc"), None);
        assert_eq!(hex_decode("zz"), None);
    }

    #[test]
    fn base64_decoding() {
        let pad2 = concat!("YQ", "=", "=");
        assert_eq!(b64_decode(pad2), Some(b"a".to_vec()));
        assert_eq!(b64_decode("YQ"), Some(b"a".to_vec()));
        assert_eq!(b64_decode("YWI="), Some(b"ab".to_vec()));
        assert_eq!(b64_decode("YWI"), Some(b"ab".to_vec()));
        assert_eq!(b64_decode("YWJj"), Some(b"abc".to_vec()));
        assert_eq!(b64_decode("+/+/"), Some(vec![0xfb, 0xff, 0xbf]));
        assert_eq!(b64_decode(""), None);
        assert_eq!(b64_decode("Y"), None);
        assert_eq!(b64_decode("YR"), None); // non-canonical trailing bits
        assert_eq!(b64_decode("YW-j"), None); // URL alphabet is not standard
        assert_eq!(b64_decode("YWJj="), None); // padding on a full quantum
        assert_eq!(b64_decode(concat!("Y", "=", "=", "=")), None);
    }
}
