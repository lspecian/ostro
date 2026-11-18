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

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};
    use std::time::Duration;

    use super::*;
    use crate::bindings::ostro::runtime::journal::CompletionAck;
    use crate::bindings::ostro::runtime::types::{ErrorInfo, Failure};

    /// Fake step handle: answers `complete` from a script of acks and records outcomes.
    struct FakeStep {
        acks: RefCell<Vec<CompletionAck>>,
        completed: RefCell<Vec<Result<Payload, Failure>>>,
    }

    impl FakeStep {
        fn new(acks: Vec<CompletionAck>) -> Self {
            Self {
                acks: RefCell::new(acks),
                completed: RefCell::new(Vec::new()),
            }
        }
    }

    impl StepHandle for &FakeStep {
        fn complete(&self, outcome: Result<&Payload, &Failure>) -> CompletionAck {
            self.completed
                .borrow_mut()
                .push(outcome.map(Clone::clone).map_err(Clone::clone));
            self.acks.borrow_mut().remove(0)
        }
    }

    #[test]
    fn replayed_ok_decodes_without_running_body() {
        let calls = Cell::new(0);
        let start: StepStartOf<&FakeStep> = StepStartOf::Replayed(Ok(json_payload(b"42".to_vec())));
        let out: Result<u32, StepError> = drive_step(start, || {
            calls.set(calls.get() + 1);
            Ok(7)
        });
        assert_eq!(out, Ok(42));
        assert_eq!(calls.get(), 0);
    }

    #[test]
    fn replayed_err_returns_recorded_failure() {
        let start: StepStartOf<&FakeStep> = StepStartOf::Replayed(Err(Failure::Timeout));
        let out: Result<u32, StepError> = drive_step(start, || panic!("body must not run"));
        assert!(out.unwrap_err().is_timeout());
    }

    #[test]
    fn replayed_garbage_is_non_retryable_invalid_json() {
        let start: StepStartOf<&FakeStep> =
            StepStartOf::Replayed(Ok(json_payload(b"not json".to_vec())));
        let err = drive_step::<u32, _>(start, || Ok(1)).unwrap_err();
        assert_eq!(err.kind, crate::error::INVALID_JSON);
        assert!(!err.retryable);
    }

    #[test]
    fn execute_records_json_and_returns_journaled_value() {
        let h = FakeStep::new(vec![CompletionAck::Recorded]);
        let out = drive_step(StepStartOf::Execute(&h), || Ok(vec!["a".to_string()]));
        assert_eq!(out, Ok(vec!["a".to_string()]));
        let done = h.completed.borrow();
        assert_eq!(done.len(), 1);
        let p = done[0].as_ref().unwrap();
        assert_eq!(p.content_type, JSON);
        assert_eq!(p.data, br#"["a"]"#);
    }

    #[test]
    fn retry_now_reruns_body_until_recorded() {
        let h = FakeStep::new(vec![
            CompletionAck::RetryNow,
            CompletionAck::RetryNow,
            CompletionAck::Recorded,
        ]);
        let attempt = Cell::new(0u32);
        let out = drive_step(StepStartOf::Execute(&h), || {
            attempt.set(attempt.get() + 1);
            if attempt.get() < 3 {
                Err(StepError::retryable("flaky", format!("attempt {}", attempt.get())))
            } else {
                Ok(attempt.get())
            }
        });
        assert_eq!(out, Ok(3));
        assert_eq!(attempt.get(), 3);
        let done = h.completed.borrow();
        assert_eq!(done.len(), 3);
        match &done[0] {
            Err(Failure::Retryable(ErrorInfo { kind, .. })) => assert_eq!(kind, "flaky"),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn recorded_failure_is_returned() {
        let h = FakeStep::new(vec![CompletionAck::Recorded]);
        let out: Result<u32, StepError> =
            drive_step(StepStartOf::Execute(&h), || Err(StepError::non_retryable("bad", "x")));
        assert_eq!(out, Err(StepError::non_retryable("bad", "x")));
        assert!(matches!(h.completed.borrow()[0], Err(Failure::NonRetryable(_))));
    }

    #[test]
    fn options_convert_to_wit() {
        let o = StepOptions::default().to_wit();
        assert!(o.retry.is_none() && o.timeout_ms.is_none() && o.fuel.is_none());

        let o = StepOptions::builder()
            .retry(5, Duration::from_millis(100), Duration::from_secs(10), 2.0, ["bad_input"])
            .timeout(Duration::from_secs(3))
            .fuel(1_000_000)
            .build()
            .to_wit();
        let r = o.retry.expect("retry");
        assert_eq!(r.max_attempts, 5);
        assert_eq!(r.initial_interval_ms, 100);
        assert_eq!(r.max_interval_ms, 10_000);
        assert_eq!(r.backoff_coefficient, 2.0);
        assert_eq!(r.non_retryable_kinds, vec!["bad_input".to_string()]);
        assert_eq!(o.timeout_ms, Some(3000));
        assert_eq!(o.fuel, Some(1_000_000));
    }
}
