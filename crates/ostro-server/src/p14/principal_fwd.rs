//! Phase 14 (14-15): principal propagation over Phase 13 forwarding (D-P14-6).
//!
//! Authorization happens once, at the ingress node (the guard, `p14/authz.rs`). A call the
//! router forwards (one hop) carries the caller's principal in `ostro-principal`
//! ([`encode`]: the 14-01 `p1;<token_id>;<tenant|*>;<scopes>` format; a token's secret never
//! travels):
//!
//! - **forwarder** (`route/peer.rs::stamp_principal`, called by the router's `forward` for
//!   every forwarded call and fan-out leg): the header is the current principal, never a
//!   value the client sent (the client listener drops `ostro-principal`, see
//!   `ostro_api::request_meta::from_headers`); an internal caller without a principal
//!   (`Anonymous`: cron, webhooks, bulk and other in-process consumers) sends none;
//! - **receiver** (the node API on the cluster listener, `route::node_api_router`, after
//!   the mTLS node-identity check): [`ingest`] decodes the header and the call runs inside
//!   `PRINCIPAL.scope(decoded)` with its kind preserved (`Token` for `otk_` ids); a missing
//!   header is an internal call ([`Principal::system`]); a malformed one is refused. The
//!   node API is not guarded: the principal only serves node-local lookups (bulk jobs,
//!   audit queries, logging) and is never re-authorized or widened from the header.

use ostro_api::ApiError;
use ostro_api::auth::Principal;
use ostro_api::request_meta::MAX_TOKEN_LEN;

/// Tracing target of the receiver's per-call event (`from_node`, `principal`, `kind`).
pub const TRACE_TARGET: &str = "ostro::p14::principal_fwd";

/// The header value a forwarder attaches for `p`.
pub fn encode(p: &Principal) -> String {
    p.encode_forwarded()
}

/// The principal of a forwarded call from a trusted node.
pub fn decode(v: &str) -> Result<Principal, String> {
    if v.len() > MAX_TOKEN_LEN {
        return Err(format!("longer than {MAX_TOKEN_LEN} bytes"));
    }
    Principal::decode_forwarded(v)
}

/// The principal a node-API call runs as: the forwarded one, [`Principal::system`] when the
/// trusted caller sent none (an internal call), `invalid_argument` for a malformed value.
pub fn ingest(header: Option<&str>) -> Result<Principal, ApiError> {
    match header {
        None => Ok(Principal::system()),
        Some(v) => decode(v.trim()).map_err(|e| {
            ApiError::invalid_argument(format!("header ostro-principal: {e}"))
                .with_details(serde_json::json!({ "header": "ostro-principal" }))
        }),
    }
}

#[cfg(test)]
mod tests {
    use ostro_api::auth::{PrincipalKind, Scope, ScopeSet};

    use super::*;

    #[test]
    fn round_trip_keeps_the_kind() {
        let t = Principal::token(
            "otk_abcdefghijklmnop",
            Some("acme".into()),
            ScopeSet::of(&[Scope::Read, Scope::Trigger]),
        );
        for p in [t, Principal::system(), Principal::dev()] {
            let back = ingest(Some(&encode(&p))).expect("decode");
            assert_eq!(back, p);
        }
        let back = ingest(Some(&encode(&Principal::token(
            "otk_abcdefghijklmnop",
            None,
            ScopeSet::admin(),
        ))))
        .expect("decode");
        assert_eq!(back.kind, PrincipalKind::Token);
        assert_eq!(back.tenant, None);
    }

    #[test]
    fn missing_is_system_and_malformed_is_refused() {
        assert_eq!(ingest(None).expect("system"), Principal::system());
        for bad in [
            "",
            "p2;otk_a;*;read",
            "p1;otk_a;*",
            "p1;otk_a;*;root",
            "garbage",
        ] {
            assert!(ingest(Some(bad)).is_err(), "{bad:?}");
        }
        let long = format!("p1;otk_{};*;read", "a".repeat(300));
        assert!(ingest(Some(&long)).is_err());
    }
}
