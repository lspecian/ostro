//! Outcome encoding and guest-visible failure helpers (06-07).
//!
//! A connector result is a `Result<OpOutput, Failure>`. The driver encodes it **once** with
//! [`encode_outcome`] (byte-identical to `ostro_types::journal::encode_result`) and submits the
//! [`RecordedResult`] through the [`CompletionSink`](crate::connector::CompletionSink).
//!
//! `details` payloads are UTF-8 JSON (`content_type = "application/json"`). ostro-types has no
//! `Payload` constructor, so [`json_payload`] builds the struct literal. Callers put only names,
//! kinds and counts in `details`, never secret material, header values or bodies.

use ostro_types::journal::{ErrorInfo, Failure, OpOutput, Payload, RecordedResult, encode_result};

/// `ErrorInfo.kind` of an effect whose connector panicked (W11). Not a W8 kind (so not in
/// [`crate::kinds`]); 06-20 lists it in the docs error table.
pub const INTERNAL_ERROR: &str = "internal_error";

/// The message of an [`INTERNAL_ERROR`] completion.
pub const CONNECTOR_PANICKED: &str = "bridge connector panicked";

/// Content type of [`json_payload`] details.
pub const JSON_CONTENT_TYPE: &str = "application/json";

/// Encodes a connector result as the recorded effect outcome (`encode_result(r)`).
pub fn encode_outcome(r: &Result<OpOutput, Failure>) -> RecordedResult {
    encode_result(r)
}

/// A UTF-8 JSON [`Payload`].
pub fn json_payload(v: &serde_json::Value) -> Payload {
    Payload {
        content_type: JSON_CONTENT_TYPE.to_owned(),
        data: v.to_string().into_bytes(),
    }
}

/// `Failure::NonRetryable{kind, message, details}`; `details` becomes a JSON payload.
pub fn non_retryable(
    kind: &str,
    message: impl Into<String>,
    details: Option<serde_json::Value>,
) -> Failure {
    Failure::NonRetryable(ErrorInfo {
        kind: kind.to_owned(),
        message: message.into(),
        details: details.as_ref().map(json_payload),
    })
}

/// `Failure::Retryable{kind, message}` without details.
pub fn retryable(kind: &str, message: impl Into<String>) -> Failure {
    Failure::Retryable(ErrorInfo {
        kind: kind.to_owned(),
        message: message.into(),
        details: None,
    })
}

/// The `outcome` metric label of a result: `ok`, the failure kind, `timeout` or `cancelled`.
pub fn outcome_label(r: &Result<OpOutput, Failure>) -> String {
    match r {
        Ok(_) => "ok".to_owned(),
        Err(Failure::Retryable(e) | Failure::NonRetryable(e)) => e.kind.clone(),
        Err(Failure::Timeout) => "timeout".to_owned(),
        Err(Failure::Cancelled) => "cancelled".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ostro_types::journal::{HttpResponse, decode_result};

    fn ok() -> Result<OpOutput, Failure> {
        Ok(OpOutput::Http(HttpResponse {
            status: 201,
            headers: vec![],
            body: b"hi".to_vec(),
        }))
    }

    #[test]
    fn outcome_encoding_equals_encode_result() {
        for r in [
            ok(),
            Err(non_retryable(
                crate::kinds::EGRESS_DENIED,
                "denied",
                Some(serde_json::json!({"host": "x"})),
            )),
            Err(retryable(crate::kinds::RATE_LIMITED, "slow down")),
            Err(Failure::Timeout),
        ] {
            let enc = encode_outcome(&r);
            assert_eq!(enc, encode_result(&r));
            let back: Result<OpOutput, Failure> = decode_result(&enc).unwrap();
            assert_eq!(back, r);
        }
    }

    #[test]
    fn outcome_details_are_json_payload() {
        let f = non_retryable("k", "m", Some(serde_json::json!({"names": ["a"], "n": 2})));
        let Failure::NonRetryable(e) = f else {
            panic!("non-retryable expected")
        };
        let d = e.details.unwrap();
        assert_eq!(d.content_type, "application/json");
        let v: serde_json::Value = serde_json::from_slice(&d.data).unwrap();
        assert_eq!(v["n"], 2);
        assert!(
            non_retryable("k", "m", None)
                == Failure::NonRetryable(ErrorInfo {
                    kind: "k".into(),
                    message: "m".into(),
                    details: None
                })
        );
        let Failure::Retryable(r) = retryable("rate_limited", "x") else {
            panic!("retryable expected")
        };
        assert_eq!((r.kind.as_str(), r.details), ("rate_limited", None));
    }

    #[test]
    fn outcome_labels() {
        assert_eq!(outcome_label(&ok()), "ok");
        assert_eq!(
            outcome_label(&Err(retryable("rate_limited", "x"))),
            "rate_limited"
        );
        assert_eq!(outcome_label(&Err(Failure::Timeout)), "timeout");
        assert_eq!(outcome_label(&Err(Failure::Cancelled)), "cancelled");
        assert!(!crate::kinds::ALL.contains(&INTERNAL_ERROR));
    }
}
