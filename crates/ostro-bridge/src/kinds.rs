//! Guest-visible `ErrorInfo.kind` values produced by the bridge (W8 in the frozen
//! `runtime.wit` doc comments, plus the SQL kinds). These strings are part of the guest
//! contract: never rename one.

/// The request may or may not have reached the destination, and re-sending is unsafe.
pub const EFFECT_OUTCOME_UNKNOWN: &str = "effect_outcome_unknown";
/// The destination is not allowed by the module's egress policy (or resolves to a blocked
/// address).
pub const EGRESS_DENIED: &str = "egress_denied";
/// A `{{secret:NAME}}` template names a secret the module holds no grant for (for this
/// origin or location).
pub const SECRET_NOT_GRANTED: &str = "secret_not_granted";
/// A granted secret is not found in any provider.
pub const SECRET_NOT_FOUND: &str = "secret_not_found";
/// The rate limit of the target was not available within `rate_max_wait`.
pub const RATE_LIMITED: &str = "rate_limited";
/// The response exceeds the configured size cap.
pub const RESPONSE_TOO_LARGE: &str = "response_too_large";
/// The named connection does not exist.
pub const CONNECTION_UNKNOWN: &str = "connection_unknown";
/// An exactly-once SQL statement cannot run inside a transaction (e.g. DDL with implicit
/// commit).
pub const SQL_NOT_TRANSACTIONAL: &str = "sql_not_transactional";
/// The exactly-once protocol found evidence that the statement may have run twice.
pub const SQL_EXACTLY_ONCE_VIOLATED: &str = "sql_exactly_once_violated";
/// The connector does not support the requested operation.
pub const UNSUPPORTED_OP: &str = "unsupported_op";
/// A transport-level HTTP failure (connect, TLS, protocol, refused encoding).
pub const HTTP_TRANSPORT: &str = "http_transport";
/// The database returned an error.
pub const SQL_ERROR: &str = "sql_error";
/// A SQL value has a type the WIT `sql-value` mapping cannot represent.
pub const SQL_UNSUPPORTED_TYPE: &str = "sql_unsupported_type";

/// Every kind above.
pub const ALL: &[&str] = &[
    EFFECT_OUTCOME_UNKNOWN,
    EGRESS_DENIED,
    SECRET_NOT_GRANTED,
    SECRET_NOT_FOUND,
    RATE_LIMITED,
    RESPONSE_TOO_LARGE,
    CONNECTION_UNKNOWN,
    SQL_NOT_TRANSACTIONAL,
    SQL_EXACTLY_ONCE_VIOLATED,
    UNSUPPORTED_OP,
    HTTP_TRANSPORT,
    SQL_ERROR,
    SQL_UNSUPPORTED_TYPE,
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kinds_are_unique_snake_case() {
        let mut seen = std::collections::BTreeSet::new();
        for k in ALL {
            assert!(seen.insert(*k), "duplicate {k}");
            assert!(
                k.bytes().all(|b| b.is_ascii_lowercase() || b == b'_'),
                "{k}"
            );
        }
        assert_eq!(ALL.len(), 13);
    }
}
