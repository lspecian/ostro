//! Idempotency keys (FX-02, W2, P-11).
//!
//! # Persistent contract
//!
//! The host-derived key of an HTTP effect is
//!
//! ```text
//! "ostro-" + lower-hex( sha256( b"ostro/idem/v1" || run_id (UTF-8) || op (u64 little-endian) )[..16] )
//! ```
//!
//! 38 ASCII characters. **This formula is frozen once shipped**: a provider deduplicates on
//! the key, so changing it would make every in-flight effect re-drive with a new key and
//! defeat the dedupe (a double charge). The test vectors below pin it.
//!
//! - The key never includes the attempt (Pitfall 5): every attempt, retry, re-drive and
//!   resume of the same `(run, op)` sends the same key. A retry-as-new or continue-as-new run
//!   has a new run id, so new keys.
//! - FX-02's "(run, op, name)": WIT `http-request` has no name field (D14); the op seq is the
//!   deterministic per-run name, and SDKs pass semantic keys (`ctx.idempotency_key(name)`,
//!   P-11) through the `idempotency-key` override.
//! - Length limits: Stripe accepts keys of up to 255 characters, Adyen 64; Square (body
//!   located) 45. 38 characters fits them all.
//!
//! # Which key is sent
//!
//! 1. The guest's `idempotency-key` override, verbatim (it replaces any guest-set header of
//!    the configured name, so the header is never duplicated).
//! 2. Otherwise, if the guest set the configured header itself, that value is the key and
//!    is sent once, as the guest wrote it.
//! 3. Otherwise the derived key.
//!
//! The header name comes from the destination's `http-target` config
//! (`idempotency_header`, default `Idempotency-Key`); it can be suppressed, in which case no
//! key header is added (the key is still computed: it appears in `effect_outcome_unknown`
//! details).

use sha2::{Digest, Sha256};

use ostro_types::journal::HttpRequest;

/// Domain separator of the derived key.
pub const KEY_DOMAIN: &[u8] = b"ostro/idem/v1";

/// Prefix of every derived key.
pub const KEY_PREFIX: &str = "ostro-";

/// Length of a derived key (`"ostro-"` + 32 hex).
pub const KEY_LEN: usize = 38;

/// The default key header name.
pub const DEFAULT_HEADER: &str = "Idempotency-Key";

/// The host-derived key of effect `op` of run `run_id` (see the module docs).
pub fn derive_key(run_id: &str, op: u64) -> String {
    let mut h = Sha256::new();
    h.update(KEY_DOMAIN);
    h.update(run_id.as_bytes());
    h.update(op.to_le_bytes());
    let digest = h.finalize();
    let mut out = String::with_capacity(KEY_LEN);
    out.push_str(KEY_PREFIX);
    for b in &digest[..16] {
        out.push(char::from(HEX[usize::from(b >> 4)]));
        out.push(char::from(HEX[usize::from(b & 0x0f)]));
    }
    out
}

const HEX: &[u8; 16] = b"0123456789abcdef";

/// Where the effective key came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeySource {
    /// Host-derived ([`derive_key`]).
    Derived,
    /// The guest's `idempotency-key` field.
    Override,
    /// A guest-set header of the configured name.
    GuestHeader,
}

/// The key of a request and its source.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EffectiveKey {
    /// The key value.
    pub key: String,
    /// Where it came from.
    pub source: KeySource,
}

/// The key `req` carries, given the configured header name (`None` = suppressed).
pub fn effective_key(
    req: &HttpRequest,
    header: Option<&str>,
    run_id: &str,
    op: u64,
) -> EffectiveKey {
    if let Some(k) = &req.idempotency_key {
        return EffectiveKey {
            key: k.clone(),
            source: KeySource::Override,
        };
    }
    if let Some(name) = header
        && let Some(h) = req
            .headers
            .iter()
            .find(|h| h.name.eq_ignore_ascii_case(name))
    {
        return EffectiveKey {
            key: h.value.clone(),
            source: KeySource::GuestHeader,
        };
    }
    EffectiveKey {
        key: derive_key(run_id, op),
        source: KeySource::Derived,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ostro_types::journal::Header;

    fn req(headers: Vec<Header>, key: Option<&str>) -> HttpRequest {
        HttpRequest {
            method: "POST".into(),
            url: "https://api.example.com/pay".into(),
            headers,
            body: None,
            timeout_ms: None,
            idempotency_key: key.map(str::to_owned),
            redact: vec![],
        }
    }

    /// Pinned vectors (cross-checked with Python hashlib). Never change them.
    #[test]
    fn derived_key_vectors_are_pinned() {
        let uuid = "0197a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b";
        let cases = [
            (uuid, 1u64, "ostro-bde26f8232abe4e7e94943f970fc2c33"),
            (
                uuid,
                0x0102_0304_0506_0708,
                "ostro-07d79386f8bb969ca49792f371a64344",
            ),
            ("run-1", 1, "ostro-6ac4a780ae09b9770d9fb056ef9a5364"),
            (
                "run-1",
                0x0102_0304_0506_0708,
                "ostro-25d328cd03c15ed4fc3e117a5e82626f",
            ),
        ];
        for (run, op, want) in cases {
            let k = derive_key(run, op);
            assert_eq!(k, want, "run {run} op {op}");
            assert_eq!(k.len(), KEY_LEN);
        }
    }

    #[test]
    fn derived_key_differs_by_run_and_op() {
        assert_ne!(derive_key("r", 1), derive_key("r", 2));
        assert_ne!(derive_key("r1", 1), derive_key("r2", 1));
        assert_eq!(derive_key("r", 7), derive_key("r", 7));
    }

    #[test]
    fn override_is_used_verbatim() {
        let r = req(
            vec![Header {
                name: "idempotency-key".into(),
                value: "guest-header".into(),
            }],
            Some("Order#42 / custom"),
        );
        let k = effective_key(&r, Some(DEFAULT_HEADER), "run", 1);
        assert_eq!(k.key, "Order#42 / custom");
        assert_eq!(k.source, KeySource::Override);
    }

    #[test]
    fn guest_header_is_the_key_when_no_override() {
        let r = req(
            vec![Header {
                name: "IDEMPOTENCY-KEY".into(),
                value: "guest-header".into(),
            }],
            None,
        );
        let k = effective_key(&r, Some(DEFAULT_HEADER), "run", 1);
        assert_eq!(k.key, "guest-header");
        assert_eq!(k.source, KeySource::GuestHeader);
        // Another configured name: the guest header is just a header.
        let k = effective_key(&r, Some("PayPal-Request-Id"), "run", 1);
        assert_eq!(k.source, KeySource::Derived);
        assert_eq!(k.key, derive_key("run", 1));
    }

    #[test]
    fn suppressed_header_still_derives() {
        let k = effective_key(&req(vec![], None), None, "run", 3);
        assert_eq!(k.key, derive_key("run", 3));
        assert_eq!(k.source, KeySource::Derived);
    }
}
