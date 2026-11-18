//! Durable operation handles (`Op<T>`) and the `join!` / `select!` wait macros.
//!
//! Every effect call (`Ctx::sleep`, `Ctx::http_fetch`, ...) allocates an op id
//! deterministically, in call order, and returns an [`Op`]. Nothing blocks until the op is
//! waited on: [`Op::wait`] waits for one op, [`join!`](crate::join) waits for all of several
//! (`wait-mode::all`) and [`select!`](crate::select) for the first of several
//! (`wait-mode::any`). Completions come back in journaled order, identical on every replay.

use std::marker::PhantomData;

use crate::bindings::ostro::runtime::effects::{self, OpOutput, WaitMode};
use crate::error::StepError;

pub use crate::bindings::ostro::runtime::effects::Completion;

/// Raw WIT op id.
pub type OpId = u64;

/// Decoder from a successful op output to the typed result.
type Decode<T> = fn(OpOutput) -> Result<T, StepError>;

/// A pending durable operation that resolves to `T`.
#[must_use = "an op does nothing observable until it is waited on (or cancelled)"]
pub struct Op<T> {
    id: OpId,
    decode: Decode<T>,
    _t: PhantomData<fn() -> T>,
}

impl<T> std::fmt::Debug for Op<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Op").field("id", &self.id).finish()
    }
}

impl<T> Op<T> {
    pub(crate) fn new(id: OpId, decode: Decode<T>) -> Self {
        Self {
            id,
            decode,
            _t: PhantomData,
        }
    }

    /// The raw op id (deterministic per run).
    pub fn id(&self) -> OpId {
        self.id
    }

    /// Block (durably) until this op completes and return its typed result.
    pub fn wait(self) -> Result<T, StepError> {
        let mut completions = effects::wait(&[self.id], WaitMode::All);
        self.__take(&mut completions)
    }

    /// Decode a completion of this op (e.g. the one returned by [`select!`](crate::select)).
    ///
    /// Returns a non-retryable `"protocol"` error if the completion belongs to another op.
    pub fn decode(&self, completion: Completion) -> Result<T, StepError> {
        if completion.op != self.id {
            return Err(StepError::non_retryable(
                "protocol",
                format!(
                    "completion for op {} decoded as op {}",
                    completion.op, self.id
                ),
            ));
        }
        match completion.outcome {
            Ok(out) => (self.decode)(out),
            Err(f) => Err(f.into()),
        }
    }

    /// Remove this op's completion from `completions` and decode it.
    #[doc(hidden)]
    pub fn __take(self, completions: &mut Vec<Completion>) -> Result<T, StepError> {
        match completions.iter().position(|c| c.op == self.id) {
            Some(i) => {
                let c = completions.remove(i);
                self.decode(c)
            }
            None => Err(StepError::non_retryable(
                "protocol",
                format!("wait returned no completion for op {}", self.id),
            )),
        }
    }
}

fn unexpected<T>(what: &str, out: &OpOutput) -> Result<T, StepError> {
    Err(StepError::non_retryable(
        "protocol",
        format!("expected {what} output, got {out:?}"),
    ))
}

/// Timer / cancellation ops resolve to `()`.
pub(crate) fn decode_unit(out: OpOutput) -> Result<(), StepError> {
    match out {
        OpOutput::Timer | OpOutput::CancelRequested => Ok(()),
        other => unexpected("timer", &other),
    }
}

/// HTTP ops resolve to the response.
pub(crate) fn decode_http(
    out: OpOutput,
) -> Result<crate::bindings::ostro::runtime::effects::HttpResponse, StepError> {
    match out {
        OpOutput::Http(r) => Ok(r),
        other => unexpected("http", &other),
    }
}

/// Signal ops resolve to the JSON-decoded signal payload.
pub(crate) fn decode_signal<T: serde::de::DeserializeOwned>(out: OpOutput) -> Result<T, StepError> {
    match out {
        OpOutput::Signal(p) => crate::step::decode_payload(&p),
        other => unexpected("signal", &other),
    }
}

/// Wait on raw op ids. Used by `join!` / `select!`.
#[doc(hidden)]
pub fn __wait(ids: &[OpId], any: bool) -> Vec<Completion> {
    let mode = if any { WaitMode::Any } else { WaitMode::All };
    effects::wait(ids, mode)
}

/// Index (into `ids`) of the op the first completion belongs to.
#[doc(hidden)]
pub fn __first(ids: &[OpId], mut completions: Vec<Completion>) -> (usize, Completion) {
    assert!(
        !completions.is_empty(),
        "ostro: wait(any) returned no completions"
    );
    let c = completions.remove(0);
    let idx = ids
        .iter()
        .position(|id| *id == c.op)
        .expect("ostro: wait(any) returned a completion for an op that was not awaited");
    (idx, c)
}

/// Wait for **all** ops and return a tuple of their typed results, in argument order.
///
/// Each argument is evaluated exactly once, left to right, then a single
/// `wait(ops, all)` is issued.
///
/// ```ignore
/// let (a, b) = ostro_sdk::join!(ctx.sleep(s1), ctx.sleep(s2));
/// a?; b?;
/// ```
#[macro_export]
macro_rules! join {
    (@acc [$($b:ident = $e:expr;)*]) => {{
        $( let $b = $e; )*
        let mut __ostro_c = $crate::ops::__wait(&[$($crate::ops::Op::id(&$b)),*], false);
        ( $( $crate::ops::Op::__take($b, &mut __ostro_c), )* )
    }};
    (@acc [$($acc:tt)*] $e:expr $(, $rest:expr)*) => {
        $crate::join!(@acc [$($acc)* __ostro_op = $e;] $($rest),*)
    };
    ($($e:expr),+ $(,)?) => {
        $crate::join!(@acc [] $($e),+)
    };
}

/// Wait for the **first** of several ops (`wait-mode::any`).
///
/// Arguments are op values or places (they are borrowed, not consumed, so the losing ops
/// can still be waited on or cancelled). Returns `(index, completion)` where `index` is the
/// position of the winning argument; decode it with the matching op's [`Op::decode`].
///
/// ```ignore
/// let timer = ctx.sleep(Duration::from_secs(60));
/// let reply = ctx.wait_signal::<String>("reply", None);
/// match ostro_sdk::select!(timer, reply) {
///     (0, _) => { /* timed out */ }
///     (_, c) => { let text = reply.decode(c)?; }
/// }
/// ```
#[macro_export]
macro_rules! select {
    (@acc [$($b:ident = $e:expr;)*]) => {{
        $( let $b = &$e; )*
        let __ostro_ids = [$($crate::ops::Op::id($b)),*];
        let __ostro_c = $crate::ops::__wait(&__ostro_ids, true);
        $crate::ops::__first(&__ostro_ids, __ostro_c)
    }};
    (@acc [$($acc:tt)*] $e:expr $(, $rest:expr)*) => {
        $crate::select!(@acc [$($acc)* __ostro_op = $e;] $($rest),*)
    };
    ($($e:expr),+ $(,)?) => {
        $crate::select!(@acc [] $($e),+)
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bindings::ostro::runtime::types::{ErrorInfo, Failure};

    fn timer_op(id: OpId) -> Op<()> {
        Op::new(id, decode_unit)
    }

    fn done(op: OpId) -> Completion {
        Completion {
            op,
            outcome: Ok(OpOutput::Timer),
        }
    }

    #[test]
    fn take_finds_and_removes_matching_completion() {
        let mut cs = vec![done(7), done(3)];
        assert_eq!(timer_op(3).__take(&mut cs), Ok(()));
        assert_eq!(cs.len(), 1);
        assert_eq!(cs[0].op, 7);
        let err = timer_op(9).__take(&mut cs).unwrap_err();
        assert_eq!(err.kind, "protocol");
    }

    #[test]
    fn decode_maps_failures_and_wrong_outputs() {
        let op = timer_op(1);
        let c = Completion {
            op: 1,
            outcome: Err(Failure::Retryable(ErrorInfo {
                kind: "k".into(),
                message: "m".into(),
                details: None,
            })),
        };
        assert_eq!(op.decode(c), Err(StepError::retryable("k", "m")));
        let c = Completion {
            op: 1,
            outcome: Ok(OpOutput::Object(vec![1])),
        };
        assert_eq!(op.decode(c).unwrap_err().kind, "protocol");
        assert_eq!(op.decode(done(2)).unwrap_err().kind, "protocol");
        let c = Completion {
            op: 1,
            outcome: Err(Failure::Cancelled),
        };
        assert!(op.decode(c).unwrap_err().is_cancelled());
    }

    #[test]
    fn signal_decodes_json() {
        let out = OpOutput::Signal(crate::step::json_payload(br#"{"a":1}"#.to_vec()));
        let v: serde_json::Value = decode_signal(out).unwrap();
        assert_eq!(v["a"], 1);
    }

    #[test]
    fn first_reports_winning_index() {
        let (i, c) = __first(&[10, 11, 12], vec![done(12), done(10)]);
        assert_eq!(i, 2);
        assert_eq!(c.op, 12);
    }

    // `join!` hygiene: each argument binds its own `__ostro_op` and is evaluated once,
    // in order. Checked via the bindings only (no wait call) on the host target.
    #[test]
    fn join_binding_hygiene_evaluates_each_arg_once_in_order() {
        let mut order = Vec::new();
        let mut mk = |id: OpId| {
            order.push(id);
            timer_op(id)
        };
        macro_rules! bind_only {
            (@acc [$($b:ident = $e:expr;)*]) => {{
                $( let $b = $e; )*
                [$(Op::id(&$b)),*]
            }};
            (@acc [$($acc:tt)*] $e:expr $(, $rest:expr)*) => {
                bind_only!(@acc [$($acc)* __ostro_op = $e;] $($rest),*)
            };
            ($($e:expr),+) => { bind_only!(@acc [] $($e),+) };
        }
        let ids = bind_only!(mk(4), mk(5), mk(6));
        assert_eq!(ids, [4, 5, 6]);
        assert_eq!(order, vec![4, 5, 6]);
    }
}
