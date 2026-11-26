//! Phase 14: implemented in 14-15 (principal propagation over Phase 13 forwarding,
//! D-P14-6): the `ostro-principal` header on forwarded calls and its node-API ingest.

use ostro_api::auth::Principal;

/// The header value a forwarder attaches for `p`. Stub: `Principal::encode_forwarded`.
pub fn encode(p: &Principal) -> String {
    p.encode_forwarded()
}

/// The principal of a forwarded call from a trusted node. Stub: decode only.
pub fn decode(v: &str) -> Result<Principal, String> {
    Principal::decode_forwarded(v)
}
