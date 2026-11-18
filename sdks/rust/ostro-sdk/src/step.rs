//! Durable steps: JSON payload codec (the step runner lands with the registry).

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::bindings::ostro::runtime::types::Payload;
use crate::error::StepError;

/// Content type of every SDK-encoded payload.
pub const JSON: &str = "application/json";

#[allow(dead_code)] // consumed by the step runner / registry (Task 2)
pub(crate) fn json_payload(data: Vec<u8>) -> Payload {
    Payload {
        content_type: JSON.to_string(),
        data,
    }
}

/// Encode a value as an `application/json` payload.
#[allow(dead_code)] // consumed by the step runner / registry (Task 2)
pub(crate) fn encode_value<T: Serialize + ?Sized>(v: &T) -> Result<Payload, StepError> {
    Ok(json_payload(serde_json::to_vec(v)?))
}

/// Decode a JSON payload. Failures are non-retryable `"invalid_json"` errors.
pub(crate) fn decode_payload<T: DeserializeOwned>(p: &Payload) -> Result<T, StepError> {
    Ok(serde_json::from_slice(&p.data)?)
}
