//! [`ClientError`]: one error type for the typed surface ([`crate::typed`]), built from
//! the REST error body `{"error": {"code", "message", "details"}}`.

use serde_json::Value;

use crate::client::CallError;

/// A failed typed call.
///
/// - `status`: the HTTP status when the server answered (`None` for local or transport
///   failures);
/// - `code`: the server's error code (`not_found`, `conflict`, `invalid_argument`, ...),
///   `unreachable` when the server could not be reached or timed out, `usage` for bad local
///   input, `decode` when a 2xx body did not have the expected shape;
/// - `message`: the human-readable message;
/// - `details`: the server's `details` object (`Value::Null` when absent). The machine
///   reason, if any, is `details["reason"]` (see [`ClientError::reason`]).
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
#[error("{}", self.render())]
pub struct ClientError {
    /// HTTP status, when the server answered.
    pub status: Option<u16>,
    /// Error code.
    pub code: String,
    /// Human-readable message.
    pub message: String,
    /// The server's `details` object, or `Value::Null`.
    pub details: Value,
}

impl ClientError {
    /// `details.reason`, when the server gave one (e.g. `time_skipping_disabled`).
    pub fn reason(&self) -> Option<&str> {
        self.details.get("reason").and_then(Value::as_str)
    }

    fn render(&self) -> String {
        match (self.status, self.reason()) {
            (Some(s), Some(r)) => format!("{} (HTTP {s} {}, reason {r})", self.message, self.code),
            (Some(s), None) => format!("{} (HTTP {s} {})", self.message, self.code),
            (None, _) => format!("{} ({})", self.message, self.code),
        }
    }
}

impl From<CallError> for ClientError {
    fn from(e: CallError) -> Self {
        match e {
            CallError::Usage(message) => Self {
                status: None,
                code: "usage".to_owned(),
                message,
                details: Value::Null,
            },
            CallError::Transport(message) => Self {
                status: None,
                code: "unreachable".to_owned(),
                message,
                details: Value::Null,
            },
            CallError::Api { status, body } => {
                let err = &body["error"];
                Self {
                    status: Some(status),
                    code: err["code"]
                        .as_str()
                        .map_or_else(|| format!("http_{status}"), str::to_owned),
                    message: err["message"]
                        .as_str()
                        .unwrap_or("request failed")
                        .to_owned(),
                    details: err.get("details").cloned().unwrap_or(Value::Null),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn from_api_body() {
        let e = ClientError::from(CallError::Api {
            status: 409,
            body: json!({"error": {"code": "conflict", "message": "time skipping is off",
                "details": {"reason": "time_skipping_disabled"}}}),
        });
        assert_eq!(e.status, Some(409));
        assert_eq!(e.code, "conflict");
        assert_eq!(e.reason(), Some("time_skipping_disabled"));
        assert_eq!(
            e.to_string(),
            "time skipping is off (HTTP 409 conflict, reason time_skipping_disabled)"
        );
        let e = ClientError::from(CallError::Api {
            status: 404,
            body: json!({"error": {"code": "not_found", "message": "no such run"}}),
        });
        assert_eq!(e.details, Value::Null);
        assert_eq!(e.to_string(), "no such run (HTTP 404 not_found)");
    }

    #[test]
    fn from_transport_and_usage() {
        let e = ClientError::from(CallError::Transport(
            "cannot reach the Ostro server at http://h:1/".to_owned(),
        ));
        assert_eq!(e.status, None);
        assert_eq!(e.code, "unreachable");
        assert_eq!(
            e.to_string(),
            "cannot reach the Ostro server at http://h:1/ (unreachable)"
        );
        let e = ClientError::from(CallError::Usage("bad".to_owned()));
        assert_eq!(e.code, "usage");
    }
}
