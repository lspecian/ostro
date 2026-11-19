//! Durable operation handles (`Op<T>`) and the `join!` / `select!` wait macros.
//!
//! Every effect call (`Ctx::sleep`, `Ctx::http_fetch`, ...) allocates an op id
//! deterministically, in call order, and returns an [`Op`]. Nothing blocks until the op is
//! waited on: [`Op::wait`] waits for one op, [`join!`](crate::join) waits for all of several
//! (`wait-mode::all`) and [`select!`](crate::select) for the first of several
//! (`wait-mode::any`). Completions come back in journaled order, identical on every replay.
//!
//! # Cancellation (D11)
//!
//! SDK waits observe a graceful cancel of the run by default. `Op::wait`, `join!` and
//! `select!` add the run's cancellation op ([`CANCELLATION`]) to the waited list
//! (`wait([op, CANCELLATION], any)`), and an op that has not completed before the cancel was
//! journaled resolves to `Err(StepError::cancelled())`. An op that completed first still
//! returns its own result (first completion in journal order wins).
//!
//! Inside [`Ctx::shielded`](crate::Ctx::shielded) waits use the plain op list and ignore
//! cancellation, which is how cleanup code (compensations, releases) runs to completion
//! after a cancel. A wait that already includes the cancellation op (for example
//! `ctx.cancellation().wait()`, or `select!` with `ctx.cancellation()` as an argument) is
//! not wrapped a second time.
//!
//! The cancellation op is part of the journaled wait command, so an SDK wait's command
//! hash covers `[op, CANCELLATION]` (or the plain list when shielded). Moving a wait into
//! or out of a `shielded` section therefore changes the journal like any other code change.

use std::cell::Cell;
use std::marker::PhantomData;

use crate::bindings::ostro::runtime::effects::{self, OpOutput, WaitMode};
use crate::bindings::ostro::runtime::types::Failure;
use crate::error::StepError;

pub use crate::bindings::ostro::runtime::effects::Completion;

/// Raw WIT op id.
pub type OpId = u64;

/// The run's cancellation op id: `effects.cancellation()` always returns 2^64-1 (frozen by
/// WIT 0.1, see `types.op-id`). It completes with `cancel-requested` once a graceful cancel
/// is journaled.
pub const CANCELLATION: OpId = u64::MAX;

thread_local! {
    /// Nesting depth of [`shielded`] sections on this (single) guest thread.
    static SHIELD_DEPTH: Cell<u32> = const { Cell::new(0) };
}

/// Restores the shield depth on scope exit, including unwinding.
struct ShieldGuard;

impl Drop for ShieldGuard {
    fn drop(&mut self) {
        SHIELD_DEPTH.with(|d| d.set(d.get().saturating_sub(1)));
    }
}

/// Run `f` with cancellation surfacing disabled (see [`Ctx::shielded`](crate::Ctx::shielded)).
pub(crate) fn shielded<R>(f: impl FnOnce() -> R) -> R {
    SHIELD_DEPTH.with(|d| d.set(d.get().saturating_add(1)));
    let _guard = ShieldGuard;
    f()
}

/// True inside a [`Ctx::shielded`](crate::Ctx::shielded) section.
pub fn is_shielded() -> bool {
    SHIELD_DEPTH.with(Cell::get) > 0
}

/// True if `c` is the completion of the run's cancellation op (what `select!` returns when
/// the run was cancelled before any of its arguments completed).
pub fn is_cancellation(c: &Completion) -> bool {
    c.op == CANCELLATION
}

/// The op list an SDK wait actually issues: `ids` plus [`CANCELLATION`], or `None` when the
/// wait must not observe cancellation (shielded, or `ids` already contains it).
fn with_cancellation(ids: &[OpId]) -> Option<Vec<OpId>> {
    if is_shielded() || ids.contains(&CANCELLATION) {
        return None;
    }
    let mut v = Vec::with_capacity(ids.len() + 1);
    v.extend_from_slice(ids);
    v.push(CANCELLATION);
    Some(v)
}

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
    ///
    /// Observes cancellation (D11): if a graceful cancel of the run is journaled before this
    /// op completes, returns `Err(StepError::cancelled())` (the op itself stays open; cancel
    /// it with [`Ctx::cancel`](crate::Ctx::cancel) if it should not complete later). Inside
    /// [`Ctx::shielded`](crate::Ctx::shielded) it waits for the op only.
    pub fn wait(self) -> Result<T, StepError> {
        match with_cancellation(&[self.id]) {
            None => {
                let mut completions = effects::wait(&[self.id], WaitMode::All);
                self.__take(&mut completions)
            }
            Some(ids) => self.take_first(effects::wait(&ids, WaitMode::Any)),
        }
    }

    /// Decode the first completion of a `wait([self, CANCELLATION], any)`.
    fn take_first(self, completions: Vec<Completion>) -> Result<T, StepError> {
        match completions.into_iter().next() {
            Some(c) => self.decode(c),
            None => Err(StepError::non_retryable(
                "protocol",
                format!("wait returned no completion for op {}", self.id),
            )),
        }
    }

    /// Decode a completion of this op (e.g. the one returned by [`select!`](crate::select)).
    ///
    /// The cancellation op's completion decodes as `Err(StepError::cancelled())` for any op
    /// (that is what `select!` returns when the run was cancelled first). Any other
    /// completion of a different op is a non-retryable `"protocol"` error.
    pub fn decode(&self, completion: Completion) -> Result<T, StepError> {
        if completion.op == CANCELLATION && self.id != CANCELLATION {
            return Err(StepError::cancelled());
        }
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

/// Await-token ops resolve to the JSON-decoded completion payload.
pub(crate) fn decode_token<T: serde::de::DeserializeOwned>(out: OpOutput) -> Result<T, StepError> {
    match out {
        OpOutput::Token(p) => crate::step::decode_payload(&p),
        other => unexpected("token", &other),
    }
}

/// Signal ops resolve to the JSON-decoded signal payload.
pub(crate) fn decode_signal<T: serde::de::DeserializeOwned>(out: OpOutput) -> Result<T, StepError> {
    match out {
        OpOutput::Signal(p) => crate::step::decode_payload(&p),
        other => unexpected("signal", &other),
    }
}

/// Wait on raw op ids with no cancellation handling (the plain WIT `wait`).
#[doc(hidden)]
pub fn __wait(ids: &[OpId], any: bool) -> Vec<Completion> {
    let mode = if any { WaitMode::Any } else { WaitMode::All };
    effects::wait(ids, mode)
}

/// `join!`: one completion per id. Observes cancellation unless shielded.
#[doc(hidden)]
pub fn __join(ids: &[OpId]) -> Vec<Completion> {
    if with_cancellation(ids).is_none() {
        return effects::wait(ids, WaitMode::All);
    }
    join_observing(ids, |w| effects::wait(w, WaitMode::Any))
}

/// `join!` with cancellation: repeat `wait(pending + [CANCELLATION], any)` until every op
/// has completed or the cancellation op shows up. Ops that completed before the cancel
/// (in journal order) keep their own result; the rest resolve to `Err(cancelled)`.
///
/// Each round is one journaled wait, so the number of `WaitResolved` events depends on how
/// the completions arrived; that is journaled, so replay takes the same rounds.
fn join_observing(
    ids: &[OpId],
    mut wait: impl FnMut(&[OpId]) -> Vec<Completion>,
) -> Vec<Completion> {
    let mut out = Vec::with_capacity(ids.len());
    let mut pending: Vec<OpId> = ids.to_vec();
    while !pending.is_empty() {
        let mut w = pending.clone();
        w.push(CANCELLATION);
        let completions = wait(&w);
        if completions.is_empty() {
            // Not produced by a conforming host; `__take` reports the missing ops.
            break;
        }
        for c in completions {
            if c.op == CANCELLATION {
                out.extend(pending.drain(..).map(|op| Completion {
                    op,
                    outcome: Err(Failure::Cancelled),
                }));
                break;
            }
            if let Some(i) = pending.iter().position(|p| *p == c.op) {
                pending.remove(i);
                out.push(c);
            }
        }
    }
    out
}

/// `select!`: the first completion among `ids`. Observes cancellation unless shielded.
#[doc(hidden)]
pub fn __select(ids: &[OpId]) -> (usize, Completion) {
    let completions = match with_cancellation(ids) {
        None => effects::wait(ids, WaitMode::Any),
        Some(w) => effects::wait(&w, WaitMode::Any),
    };
    __first(ids, completions)
}

/// Index (into `ids`) of the op the first completion belongs to; `ids.len()` if it is the
/// cancellation op added by [`__select`].
#[doc(hidden)]
pub fn __first(ids: &[OpId], mut completions: Vec<Completion>) -> (usize, Completion) {
    assert!(
        !completions.is_empty(),
        "ostro: wait(any) returned no completions"
    );
    let c = completions.remove(0);
    let idx = match ids.iter().position(|id| *id == c.op) {
        Some(i) => i,
        None if c.op == CANCELLATION => ids.len(),
        None => panic!("ostro: wait(any) returned a completion for an op that was not awaited"),
    };
    (idx, c)
}

/// Wait for **all** ops and return a tuple of their typed results, in argument order.
///
/// Each argument is evaluated exactly once, left to right. Inside
/// [`Ctx::shielded`](crate::Ctx::shielded) a single `wait(ops, all)` is issued. Otherwise
/// the join observes cancellation (D11): it waits `any` over the pending ops plus the
/// cancellation op until all have completed, and every op still pending when a cancel is
/// journaled yields `Err(StepError::cancelled())`.
///
/// ```ignore
/// let (a, b) = ostro_sdk::join!(ctx.sleep(s1), ctx.sleep(s2));
/// a?; b?;
/// ```
#[macro_export]
macro_rules! join {
    (@acc [$($b:ident = $e:expr;)*]) => {{
        $( let $b = $e; )*
        let mut __ostro_c = $crate::ops::__join(&[$($crate::ops::Op::id(&$b)),*]);
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
/// can still be waited on or cancelled with [`Ctx::cancel`](crate::Ctx::cancel)). Returns
/// `(index, completion)` where `index` is the position of the winning argument; decode it
/// with the matching op's [`Op::decode`].
///
/// Cancellation (D11): unless shielded, the cancellation op is added to the wait. If the
/// run is cancelled before any argument completes, `index` is the number of arguments and
/// the completion is the cancellation op's ([`is_cancellation`] is true); decoding it with
/// any op yields `Err(StepError::cancelled())`, so a catch-all arm that decodes propagates
/// the cancel. Passing `ctx.cancellation()` as an argument handles it explicitly instead.
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
        $crate::ops::__select(&__ostro_ids)
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
    fn token_decodes_json() {
        let out = OpOutput::Token(crate::step::json_payload(br#""ok""#.to_vec()));
        assert_eq!(decode_token::<String>(out), Ok("ok".to_string()));
        assert_eq!(
            decode_token::<String>(OpOutput::Timer).unwrap_err().kind,
            "protocol"
        );
    }

    #[test]
    fn first_reports_winning_index() {
        let (i, c) = __first(&[10, 11, 12], vec![done(12), done(10)]);
        assert_eq!(i, 2);
        assert_eq!(c.op, 12);
        // The cancellation op added by select! reports index == len.
        let (i, c) = __first(&[10, 11], vec![cancel_requested(), done(10)]);
        assert_eq!(i, 2);
        assert!(is_cancellation(&c));
        assert!(timer_op(10).decode(c).unwrap_err().is_cancelled());
    }

    fn cancel_requested() -> Completion {
        Completion {
            op: CANCELLATION,
            outcome: Ok(OpOutput::CancelRequested),
        }
    }

    #[test]
    fn wait_list_adds_cancellation_unless_shielded_or_present() {
        assert_eq!(with_cancellation(&[4]), Some(vec![4, CANCELLATION]));
        assert_eq!(with_cancellation(&[4, 5]), Some(vec![4, 5, CANCELLATION]));
        // Not double-wrapped.
        assert_eq!(with_cancellation(&[CANCELLATION]), None);
        assert_eq!(with_cancellation(&[4, CANCELLATION]), None);
        assert!(!is_shielded());
        let inner = shielded(|| {
            assert!(is_shielded());
            let nested = shielded(|| with_cancellation(&[4]));
            assert!(is_shielded(), "still shielded after the nested section");
            (nested, with_cancellation(&[4]))
        });
        assert_eq!(inner, (None, None));
        assert!(!is_shielded());
        // The depth is restored when the shielded closure unwinds.
        let r = std::panic::catch_unwind(|| shielded(|| panic!("boom")));
        assert!(r.is_err());
        assert!(!is_shielded());
    }

    #[test]
    fn take_first_cancellation_wins_only_if_first() {
        assert!(
            timer_op(4)
                .take_first(vec![cancel_requested(), done(4)])
                .unwrap_err()
                .is_cancelled()
        );
        assert_eq!(
            timer_op(4).take_first(vec![done(4), cancel_requested()]),
            Ok(())
        );
        assert_eq!(timer_op(4).take_first(vec![]).unwrap_err().kind, "protocol");
        // The cancellation op itself decodes as unit.
        let c = Op::new(CANCELLATION, decode_unit);
        assert_eq!(c.decode(cancel_requested()), Ok(()));
    }

    #[test]
    fn join_observing_rounds_and_cancellation() {
        // All complete over two rounds; the waited list shrinks and always ends in CANCELLATION.
        let mut rounds = Vec::new();
        let mut script = vec![vec![done(2)], vec![done(1)]].into_iter();
        let out = join_observing(&[1, 2], |w| {
            rounds.push(w.to_vec());
            script.next().expect("round")
        });
        assert_eq!(
            rounds,
            vec![vec![1, 2, CANCELLATION], vec![1, CANCELLATION]]
        );
        let mut out2 = out.clone();
        assert_eq!(timer_op(1).__take(&mut out2), Ok(()));
        assert_eq!(timer_op(2).__take(&mut out2), Ok(()));

        // Op 2 completes before the cancel, op 1 does not: 2 keeps its result, 1 is cancelled.
        let mut out = join_observing(&[1, 2], |_| vec![done(2), cancel_requested(), done(1)]);
        assert_eq!(out.len(), 2);
        assert!(timer_op(1).__take(&mut out).unwrap_err().is_cancelled());
        assert_eq!(timer_op(2).__take(&mut out), Ok(()));
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
