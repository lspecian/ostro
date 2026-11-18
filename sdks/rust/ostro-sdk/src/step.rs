//! Durable steps: the typed step runner, step options and the JSON payload codec.
//!
//! A step body runs at most once to a recorded completion. On replay the recorded outcome
//! is returned without running the body. Outputs are journaled as `application/json`.

use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::bindings::ostro::runtime::journal::{self, CompletionAck, Step, StepStart};
use crate::bindings::ostro::runtime::types::{
    Failure, Payload, RetryPolicy as WitRetryPolicy, StepOptions as WitStepOptions,
};
use crate::ctx::duration_ms;
use crate::error::StepError;

/// Content type of every SDK-encoded payload.
pub const JSON: &str = "application/json";

pub(crate) fn json_payload(data: Vec<u8>) -> Payload {
    Payload {
        content_type: JSON.to_string(),
        data,
    }
}

/// Encode a value as an `application/json` payload.
pub(crate) fn encode_value<T: Serialize + ?Sized>(v: &T) -> Result<Payload, StepError> {
    Ok(json_payload(serde_json::to_vec(v)?))
}

/// Decode a JSON payload. Failures are non-retryable `"invalid_json"` errors.
///
/// An empty payload decodes as JSON `null` (so `()` / `Option` inputs need no body).
pub(crate) fn decode_payload<T: DeserializeOwned>(p: &Payload) -> Result<T, StepError> {
    let data: &[u8] = if p.data.is_empty() { b"null" } else { &p.data };
    Ok(serde_json::from_slice(data)?)
}

/// Exponential-backoff retry policy for a step.
#[derive(Debug, Clone, PartialEq)]
pub struct RetryPolicy {
    /// Maximum attempts including the first.
    pub max_attempts: u32,
    /// Delay before the first retry.
    pub initial_interval: Duration,
    /// Upper bound for the delay.
    pub max_interval: Duration,
    /// Multiplier applied to the delay after each attempt.
    pub backoff_coefficient: f64,
    /// Error kinds that are never retried.
    pub non_retryable_kinds: Vec<String>,
}

/// Options for a durable step. `StepOptions::default()` uses the host defaults.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct StepOptions {
    /// Retry policy (host default when `None`).
    pub retry: Option<RetryPolicy>,
    /// Step timeout (host default when `None`).
    pub timeout: Option<Duration>,
    /// Fuel budget for the step body (host default when `None`).
    pub fuel: Option<u64>,
}

impl StepOptions {
    /// Start building step options.
    pub fn builder() -> StepOptionsBuilder {
        StepOptionsBuilder::default()
    }

    /// Convert to the WIT `step-options` record.
    pub(crate) fn to_wit(&self) -> WitStepOptions {
        WitStepOptions {
            retry: self.retry.as_ref().map(|r| WitRetryPolicy {
                max_attempts: r.max_attempts,
                initial_interval_ms: duration_ms(r.initial_interval),
                max_interval_ms: duration_ms(r.max_interval),
                backoff_coefficient: r.backoff_coefficient,
                non_retryable_kinds: r.non_retryable_kinds.clone(),
            }),
            timeout_ms: self.timeout.map(duration_ms),
            fuel: self.fuel,
        }
    }
}

/// Builder for [`StepOptions`].
#[derive(Debug, Clone, Default)]
pub struct StepOptionsBuilder {
    opts: StepOptions,
}

impl StepOptionsBuilder {
    /// Set an exponential-backoff retry policy.
    pub fn retry<I, S>(
        mut self,
        max_attempts: u32,
        initial: Duration,
        max: Duration,
        coefficient: f64,
        non_retryable: I,
    ) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.opts.retry = Some(RetryPolicy {
            max_attempts,
            initial_interval: initial,
            max_interval: max,
            backoff_coefficient: coefficient,
            non_retryable_kinds: non_retryable.into_iter().map(Into::into).collect(),
        });
        self
    }

    /// Set the step timeout.
    pub fn timeout(mut self, d: Duration) -> Self {
        self.opts.timeout = Some(d);
        self
    }

    /// Set the step fuel budget.
    pub fn fuel(mut self, fuel: u64) -> Self {
        self.opts.fuel = Some(fuel);
        self
    }

    /// Finish.
    pub fn build(self) -> StepOptions {
        self.opts
    }
}

/// The part of a WIT `step` resource the runner needs (abstracted for host-side tests).
pub(crate) trait StepHandle {
    fn complete(&self, outcome: Result<&Payload, &Failure>) -> CompletionAck;
}

impl StepHandle for Step {
    fn complete(&self, outcome: Result<&Payload, &Failure>) -> CompletionAck {
        Step::complete(self, outcome)
    }
}

/// `journal::step-start`, generic over the handle.
pub(crate) enum StepStartOf<H> {
    Replayed(Result<Payload, Failure>),
    Execute(H),
}

/// Encode a body outcome for `step.complete`. A serialisation failure is recorded as a
/// non-retryable `"invalid_json"` failure.
fn encode_outcome<T: Serialize>(out: &Result<T, StepError>) -> Result<Payload, Failure> {
    match out {
        Ok(v) => encode_value(v).map_err(|e| e.to_failure()),
        Err(e) => Err(e.to_failure()),
    }
}

fn decode_outcome<T: DeserializeOwned>(r: &Result<Payload, Failure>) -> Result<T, StepError> {
    match r {
        Ok(p) => decode_payload(p),
        Err(f) => Err(f.clone().into()),
    }
}

/// The step state machine: replay or execute, looping while the host answers `retry-now`.
///
/// The returned value is always decoded from the journaled bytes, so live execution and
/// replay return identical values. The handle is always completed (never dropped
/// uncompleted on the success path; the engine does not rely on drop).
pub(crate) fn drive_step<T, H>(
    start: StepStartOf<H>,
    mut body: impl FnMut() -> Result<T, StepError>,
) -> Result<T, StepError>
where
    T: Serialize + DeserializeOwned,
    H: StepHandle,
{
    match start {
        StepStartOf::Replayed(r) => decode_outcome(&r),
        StepStartOf::Execute(h) => loop {
            let encoded = encode_outcome(&body());
            match h.complete(encoded.as_ref()) {
                CompletionAck::Recorded => return decode_outcome(&encoded),
                CompletionAck::RetryNow => continue,
            }
        },
    }
}

/// Run (or replay) the durable step `name`.
///
/// - Replay: the recorded outcome is decoded and returned; `body` is not called.
/// - Execute: `body` runs, its outcome is journaled as JSON (or as a failure), and while
///   the host answers `retry-now` (a retryable failure retried in-activation) `body` is
///   run again. The value returned is decoded from the journaled bytes.
///
/// A recorded payload that cannot be decoded as `T` yields a non-retryable
/// `"invalid_json"` error.
pub fn run_step<T>(
    name: &str,
    opts: &StepOptions,
    body: impl FnMut() -> Result<T, StepError>,
) -> Result<T, StepError>
where
    T: Serialize + DeserializeOwned,
{
    let start = match journal::begin_step(name, &opts.to_wit()) {
        StepStart::Replayed(r) => StepStartOf::Replayed(r),
        StepStart::Execute(step) => StepStartOf::Execute(step),
    };
    drive_step(start, body)
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
                .push(outcome.cloned().map_err(Clone::clone));
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
                Err(StepError::retryable(
                    "flaky",
                    format!("attempt {}", attempt.get()),
                ))
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
        let out: Result<u32, StepError> = drive_step(StepStartOf::Execute(&h), || {
            Err(StepError::non_retryable("bad", "x"))
        });
        assert_eq!(out, Err(StepError::non_retryable("bad", "x")));
        assert!(matches!(
            h.completed.borrow()[0],
            Err(Failure::NonRetryable(_))
        ));
    }

    #[test]
    fn options_convert_to_wit() {
        let o = StepOptions::default().to_wit();
        assert!(o.retry.is_none() && o.timeout_ms.is_none() && o.fuel.is_none());

        let o = StepOptions::builder()
            .retry(
                5,
                Duration::from_millis(100),
                Duration::from_secs(10),
                2.0,
                ["bad_input"],
            )
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
