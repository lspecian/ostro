//! The deterministic workflow context.
//!
//! Every value a [`Ctx`] returns is journaled or derived from journaled data, so it is
//! identical on every replay. Rust std is deterministic too: `SystemTime::now()`, std
//! `HashMap` seeds and `rand` (via `getrandom`) go through Ostro's deterministic WASI shim
//! (WF-07), and `Instant` is logical time measured from the run's `accepted_at`. Prefer the
//! explicit `Ctx` methods; they make the determinism visible in code review.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::bindings::ostro::runtime::effects;
use crate::bindings::ostro::runtime::log as wit_log;
use crate::bindings::ostro::runtime::types::Datetime;
use crate::bindings::ostro::runtime::context;
use crate::error::WorkflowError;
use crate::ops::{self, Op};

pub use crate::bindings::ostro::runtime::effects::{Header, HttpRequest, HttpResponse};

/// Identity and metadata of the current run, stable across replays.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunInfo {
    /// Run id (plain, without partition).
    pub run_id: String,
    /// Workflow name.
    pub workflow: String,
    /// Workflow version.
    pub workflow_version: u32,
    /// Run attempt (1-based).
    pub attempt: u32,
    /// Hash of the module executing the run.
    pub module_hash: String,
    /// Tenant, if any.
    pub tenant: Option<String>,
    /// Parent run id for child runs.
    pub parent_run_id: Option<String>,
    /// Run deadline derived from the run timeout, if any.
    pub deadline: Option<SystemTime>,
    /// Run id this run was continued from via continue-as-new, if any.
    pub continued_from: Option<String>,
}

/// Log level for [`Ctx::log`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Level {
    /// Trace.
    Trace,
    /// Debug.
    Debug,
    /// Info.
    Info,
    /// Warn.
    Warn,
    /// Error.
    Error,
}

impl From<Level> for wit_log::Level {
    fn from(l: Level) -> Self {
        match l {
            Level::Trace => wit_log::Level::Trace,
            Level::Debug => wit_log::Level::Debug,
            Level::Info => wit_log::Level::Info,
            Level::Warn => wit_log::Level::Warn,
            Level::Error => wit_log::Level::Error,
        }
    }
}

pub(crate) fn to_system_time(d: Datetime) -> SystemTime {
    UNIX_EPOCH + Duration::new(d.seconds, d.nanoseconds)
}

pub(crate) fn to_datetime(t: SystemTime) -> Datetime {
    // Times before the epoch clamp to the epoch.
    let d = t.duration_since(UNIX_EPOCH).unwrap_or_default();
    Datetime {
        seconds: d.as_secs(),
        nanoseconds: d.subsec_nanos(),
    }
}

pub(crate) fn duration_ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// The deterministic run context, handed to [`Workflow::run`](crate::Workflow::run).
///
/// Zero-sized; constructed only by the SDK runtime.
#[derive(Debug)]
pub struct Ctx {
    _p: (),
}

impl Ctx {
    #[allow(dead_code)] // consumed by the step runner / registry (Task 2)
    pub(crate) fn new() -> Self {
        Self { _p: () }
    }

    /// Metadata of the current run.
    pub fn info(&self) -> RunInfo {
        let i = context::info();
        RunInfo {
            run_id: i.run_id,
            workflow: i.workflow,
            workflow_version: i.workflow_version,
            attempt: i.attempt,
            module_hash: i.module_hash,
            tenant: i.tenant,
            parent_run_id: i.parent_run_id,
            deadline: i.deadline.map(to_system_time),
            continued_from: i.continued_from,
        }
    }

    /// Deterministic (logical, journaled) workflow time.
    pub fn now(&self) -> SystemTime {
        to_system_time(context::now())
    }

    /// Deterministic random `u64` from the run's journaled seed.
    pub fn random_u64(&self) -> u64 {
        context::random_u64()
    }

    /// `n` deterministic random bytes from the run's journaled seed.
    pub fn random_bytes(&self, n: u32) -> Vec<u8> {
        context::random_bytes(n)
    }

    /// Deterministic UUIDv7 (time from [`Ctx::now`], randomness from the run seed).
    pub fn uuid_v7(&self) -> String {
        context::uuid_v7()
    }

    /// True while the guest is re-executing journaled history.
    pub fn is_replaying(&self) -> bool {
        context::is_replaying()
    }

    /// Versioning gate: true if `change_id` is active for this run.
    pub fn patched(&self, change_id: &str) -> bool {
        context::patched(change_id)
    }

    /// Publish a JSON status document visible to clients (journaled).
    pub fn set_status<T: Serialize + ?Sized>(&self, status: &T) -> Result<(), WorkflowError> {
        let json = serde_json::to_string(status)?;
        context::set_status(&json);
        Ok(())
    }

    /// True when the journal is large enough that the workflow should continue-as-new.
    pub fn continue_as_new_suggested(&self) -> bool {
        context::continue_as_new_suggested()
    }

    /// Emit a log line. Not journaled; suppressed while replaying.
    pub fn log(&self, level: Level, message: &str) {
        self.log_with(level, message, &[]);
    }

    /// Emit a log line with structured fields. Not journaled; suppressed while replaying.
    pub fn log_with(&self, level: Level, message: &str, fields: &[(&str, &str)]) {
        if context::is_replaying() {
            return;
        }
        let fields: Vec<(String, String)> = fields
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        wit_log::emit(level.into(), message, &fields);
    }

    /// Log at info level (suppressed on replay).
    pub fn log_info(&self, message: &str) {
        self.log(Level::Info, message);
    }

    /// Log at warn level (suppressed on replay).
    pub fn log_warn(&self, message: &str) {
        self.log(Level::Warn, message);
    }

    /// Durable timer: completes after `d` of workflow time.
    pub fn sleep(&self, d: Duration) -> Op<()> {
        Op::new(effects::sleep(duration_ms(d)), ops::decode_unit)
    }

    /// Durable timer: completes at workflow time `at`.
    pub fn sleep_until(&self, at: SystemTime) -> Op<()> {
        Op::new(effects::sleep_until(to_datetime(at)), ops::decode_unit)
    }

    /// Outbound HTTP request executed by the host.
    ///
    /// Phase 4 note: the single-node server does not bridge effects yet; this op
    /// completes with a non-retryable `"unsupported"` failure until Phase 6.
    pub fn http_fetch(&self, req: &HttpRequest) -> Op<HttpResponse> {
        Op::new(effects::http_fetch(req), ops::decode_http)
    }

    /// Wait for a named signal whose payload is JSON-decoded into `T`.
    pub fn wait_signal<T: DeserializeOwned>(&self, name: &str, timeout: Option<Duration>) -> Op<T> {
        Op::new(
            effects::wait_signal(name, timeout.map(duration_ms)),
            ops::decode_signal::<T>,
        )
    }

    /// Op that completes once a graceful cancel of this run is journaled.
    pub fn cancellation(&self) -> Op<()> {
        Op::new(effects::cancellation(), ops::decode_unit)
    }

    /// Cancel a pending op; it completes with a `"cancelled"` error.
    pub fn cancel<T>(&self, op: &Op<T>) {
        effects::cancel_op(op.id());
    }
}

/// Log at info level through a [`Ctx`] with `format!` arguments (suppressed on replay).
///
/// ```ignore
/// ostro_sdk::info!(ctx, "charged {} cents", amount);
/// ```
#[macro_export]
macro_rules! info {
    ($ctx:expr, $($arg:tt)+) => {
        ($ctx).log($crate::Level::Info, &::std::format!($($arg)+))
    };
}

/// Log at warn level through a [`Ctx`] with `format!` arguments (suppressed on replay).
#[macro_export]
macro_rules! warn {
    ($ctx:expr, $($arg:tt)+) => {
        ($ctx).log($crate::Level::Warn, &::std::format!($($arg)+))
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn datetime_roundtrip() {
        let t = UNIX_EPOCH + Duration::new(1_700_000_000, 123_456_789);
        let d = to_datetime(t);
        assert_eq!((d.seconds, d.nanoseconds), (1_700_000_000, 123_456_789));
        assert_eq!(to_system_time(d), t);
        let before = UNIX_EPOCH - Duration::from_secs(5);
        let d = to_datetime(before);
        assert_eq!((d.seconds, d.nanoseconds), (0, 0));
    }

    #[test]
    fn duration_ms_saturates() {
        assert_eq!(duration_ms(Duration::from_millis(1500)), 1500);
        assert_eq!(duration_ms(Duration::MAX), u64::MAX);
    }
}
