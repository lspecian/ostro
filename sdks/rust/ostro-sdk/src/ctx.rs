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

use crate::bindings::ostro::runtime::context;
use crate::bindings::ostro::runtime::effects;
use crate::bindings::ostro::runtime::log as wit_log;
use crate::bindings::ostro::runtime::types::Datetime;
use crate::children::{self, ChildOptions};
use crate::effects::{HttpClient, ObjectClient, SqlBuilder};
use crate::error::StepError;
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
    /// Activation attempt (1-based). It increases on every re-dispatch (failover, lease
    /// expiry, worker loss, restart) and is **not** stable across replays: use it for
    /// logging and metrics only, never for control flow.
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

/// An await token: an external system completes [`op`](Self::op) by presenting
/// [`token`](Self::token) (`POST /v1/tokens/complete`). The completion payload is
/// JSON-decoded into `T`.
///
/// One-shot: the first completion wins and later completions are rejected. The optional
/// timeout completes the op with `Err(StepError::timeout())`. Waiting on a token op always
/// evicts the activation (never hot-parked), so a long wait holds no memory.
#[must_use = "hand the token to the external system and wait on the op"]
pub struct AwaitToken<T> {
    token: String,
    op: Op<T>,
}

impl<T> std::fmt::Debug for AwaitToken<T> {
    // The token is a bearer credential: never printed.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AwaitToken")
            .field("op", &self.op)
            .finish_non_exhaustive()
    }
}

impl<T> AwaitToken<T> {
    /// The opaque token string. Treat it as a secret bearer credential: keep it out of
    /// URLs and logs.
    pub fn token(&self) -> &str {
        &self.token
    }

    /// The op that completes when the token is presented.
    pub fn op(self) -> Op<T> {
        self.op
    }

    /// Split into `(token, op)`.
    pub fn into_parts(self) -> (String, Op<T>) {
        (self.token, self.op)
    }
}

/// Map a random `u64` to an `f64` in `[0, 1)`: the top 53 bits scaled by `2^-53`
/// (`(u >> 11) as f64 * 2^-53`). Exact (no rounding), so every SDK gets the same bits;
/// pinned by `conformance/vectors/random_f64.json`.
pub fn f64_from_u64(u: u64) -> f64 {
    (u >> 11) as f64 * (1.0 / (1u64 << 53) as f64)
}

/// The deterministic run context, handed to [`Workflow::run`](crate::Workflow::run).
///
/// Zero-sized; constructed only by the SDK runtime.
#[derive(Debug)]
pub struct Ctx {
    _p: (),
}

impl Ctx {
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

    /// Deterministic random `f64` in `[0, 1)`: [`f64_from_u64`] of [`Ctx::random_u64`]
    /// (`(u >> 11) * 2^-53`), identical on every replay and in every SDK.
    pub fn random_f64(&self) -> f64 {
        f64_from_u64(context::random_u64())
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

    /// Build the error that ends this run with continue-as-new: a fresh run of the same
    /// workflow starts with `input`. Return it: `return Err(ctx.continue_as_new(&next));`.
    ///
    /// Use [`ContinueAsNew`](crate::ContinueAsNew) (`Err(ContinueAsNew::new(&input)?.workflow("w").into())`) to
    /// continue as another workflow or pin a version. If `input` cannot be JSON-encoded the
    /// result is a non-retryable `invalid_json` error and the run fails.
    pub fn continue_as_new<T: Serialize + ?Sized>(&self, input: &T) -> WorkflowError {
        WorkflowError::continue_as_new(input)
    }

    /// Run `f` shielded from cancellation (D11): waits inside it (`Op::wait`, `join!`,
    /// `select!`) wait for their ops only and do not return `Err(cancelled)` when the run
    /// is cancelled. Use it for cleanup and compensation after a cancel:
    ///
    /// ```ignore
    /// match work(ctx) {
    ///     Err(e) if e.is_cancelled() => ctx.shielded(|| release(ctx))?,
    ///     r => r?,
    /// }
    /// ```
    ///
    /// Sections nest. Shielding does not stop a terminate or the run timeout (hard stops
    /// with no guest code).
    pub fn shielded<R>(&self, f: impl FnOnce() -> R) -> R {
        ops::shielded(f)
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
    /// Prefer the [`Ctx::http`] builder; this takes a hand-built WIT record.
    pub fn http_fetch(&self, req: &HttpRequest) -> Op<HttpResponse> {
        Op::new(effects::http_fetch(req), ops::decode_http)
    }

    /// Builder for an outbound HTTP request: `ctx.http().post(url).header(..).send()`.
    /// See [`crate::effects`] for the semantics (templating, redirects, redaction, keys).
    pub fn http(&self) -> HttpClient {
        HttpClient::new()
    }

    /// SQL statements against the named connection:
    /// `ctx.sql("orders").exactly_once().query(stmt, params)`.
    pub fn sql(&self, connection: &str) -> SqlBuilder {
        SqlBuilder::new(connection)
    }

    /// Object-store operations against the named connection: `ctx.object("blobs").get(key)`.
    pub fn object(&self, connection: &str) -> ObjectClient {
        ObjectClient::new(connection)
    }

    /// Deterministic idempotency key for `name` in this run (P-11): 38 chars
    /// (`ostro-` + 32 hex), stable across replay and attempts, different for every run.
    /// Pass it to [`HttpBuilder::idempotency_key`](crate::effects::HttpBuilder::idempotency_key)
    /// and embed it in the body for providers that read it from there.
    pub fn idempotency_key(&self, name: &str) -> String {
        crate::effects::idempotency_key_for(&context::info().run_id, name)
    }

    /// Wait for a named signal whose payload is JSON-decoded into `T`.
    ///
    /// Signals are FIFO: one signal satisfies one wait, signals sent before the wait are
    /// buffered per name, and several waits on one name are served oldest op first. The
    /// timeout completes the op with `Err(StepError::timeout())`. Signal waits always evict
    /// the activation (never hot-parked).
    pub fn wait_signal<T: DeserializeOwned>(&self, name: &str, timeout: Option<Duration>) -> Op<T> {
        Op::new(
            effects::wait_signal(name, timeout.map(duration_ms)),
            ops::decode_signal::<T>,
        )
    }

    /// Create an await token whose completion payload is JSON-decoded into `T`. See
    /// [`AwaitToken`].
    pub fn create_await_token<T: DeserializeOwned>(
        &self,
        timeout: Option<Duration>,
    ) -> AwaitToken<T> {
        let t = effects::create_await_token(timeout.map(duration_ms));
        AwaitToken {
            token: t.token,
            op: Op::new(t.op, ops::decode_token::<T>),
        }
    }

    /// Start a child workflow and return its op; the child's output is JSON-decoded into
    /// `T` when the op is waited on. See [`crate::children`] for the outcome contract.
    ///
    /// `workflow` is an unqualified name resolved in this run's tenant; `input` is sent as
    /// `application/json`. Child run ids derive from (parent run, op), so replay and
    /// re-delivery never start a second child. With [`ChildOptions::detached`] use
    /// `T = ChildRun`.
    ///
    /// ```ignore
    /// let out: Receipt = ctx.start_child("charge", &order, ChildOptions::new()).wait()?;
    /// let run: ChildRun = ctx.start_child("audit", &order, ChildOptions::new().detached()).wait()?;
    /// ```
    ///
    /// # Panics
    ///
    /// If `input` cannot be JSON-encoded (for example a map with non-string keys). Use
    /// [`Ctx::try_start_child`] to get that as an error instead.
    pub fn start_child<T: DeserializeOwned, I: Serialize + ?Sized>(
        &self,
        workflow: &str,
        input: &I,
        opts: ChildOptions,
    ) -> Op<T> {
        match self.try_start_child(workflow, input, opts) {
            Ok(op) => op,
            Err(e) => panic!("start_child({workflow:?}): input is not JSON-encodable: {e}"),
        }
    }

    /// [`Ctx::start_child`] that returns a non-retryable `invalid_json` error (and starts
    /// nothing) when `input` cannot be JSON-encoded.
    pub fn try_start_child<T: DeserializeOwned, I: Serialize + ?Sized>(
        &self,
        workflow: &str,
        input: &I,
        opts: ChildOptions,
    ) -> Result<Op<T>, StepError> {
        let payload = crate::step::encode_value(input)?;
        Ok(Op::new(
            effects::start_child(workflow, &payload, &opts.to_wit()),
            children::decode_child::<T>,
        ))
    }

    /// Op that completes once a graceful cancel of this run is journaled.
    ///
    /// Waiting on it directly (or passing it to `select!`/`join!`) handles cancellation
    /// explicitly; such waits are not wrapped with the implicit cancellation check.
    pub fn cancellation(&self) -> Op<()> {
        Op::new(effects::cancellation(), ops::decode_unit)
    }

    /// Cancel a pending op; waiting on it then returns `Err(StepError::cancelled())`,
    /// unless it already completed earlier in journal order (the first completion wins).
    /// Use it on `select!` losers that should not complete later. On a child op it also
    /// requests a graceful cancel of the child run and its descendants.
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
