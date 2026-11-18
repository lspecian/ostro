//! Workflow registration: the [`Workflow`] trait and the [`export!`](crate::export) macro.
//!
//! Registration is explicit — `ostro_sdk::export!(A, B, ...)` once per component — and
//! implements the WIT `workflows` export: `describe()` lists every workflow's metadata and
//! `run(name, input)` dispatches by name. There is no constructor/linker-section magic.

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::bindings::exports::ostro::runtime::workflows::{RunResult, WorkflowMeta};
use crate::bindings::ostro::runtime::types::{ErrorInfo, Failure, Payload};
use crate::ctx::Ctx;
use crate::error::{StepError, WorkflowError};
use crate::step::{decode_payload, encode_value};

/// Error kind returned by `run` for a workflow name the component does not export.
pub const UNKNOWN_WORKFLOW: &str = "unknown_workflow";

/// A durable workflow.
///
/// `run` is re-executed from the start on every activation; all non-determinism must
/// come from [`Ctx`], steps or effects.
pub trait Workflow {
    /// Workflow name (unique within the component).
    const NAME: &'static str;
    /// Workflow version.
    const VERSION: u32 = 1;
    /// Default task queue, if any.
    const DEFAULT_QUEUE: Option<&'static str> = None;
    /// Default run timeout in milliseconds, if any.
    const RUN_TIMEOUT_MS: Option<u64> = None;
    /// JSON-decoded input (an empty payload decodes as `null`).
    type Input: DeserializeOwned;
    /// JSON-encoded output.
    type Output: Serialize;
    /// The workflow body.
    fn run(ctx: &Ctx, input: Self::Input) -> Result<Self::Output, WorkflowError>;
}

/// A type-erased `run` entry point.
#[doc(hidden)]
pub type RunFn = fn(Payload) -> Result<RunResult, Failure>;

/// A dispatch table entry: `(name, run)`.
#[doc(hidden)]
pub type Entry = (&'static str, RunFn);

/// Metadata of `W` for `describe()`.
#[doc(hidden)]
pub fn __describe<W: Workflow>() -> WorkflowMeta {
    WorkflowMeta {
        name: W::NAME.to_string(),
        version: W::VERSION,
        default_queue: W::DEFAULT_QUEUE.map(str::to_string),
        run_timeout_ms: W::RUN_TIMEOUT_MS,
    }
}

/// Decode the input, run `W` and encode its output.
#[doc(hidden)]
pub fn __run<W: Workflow>(input: Payload) -> Result<RunResult, Failure> {
    let input: W::Input = decode_payload(&input).map_err(|e: StepError| e.to_failure())?;
    let ctx = Ctx::new();
    let output = W::run(&ctx, input).map_err(|e| e.to_failure())?;
    let payload = encode_value(&output).map_err(|e| e.to_failure())?;
    Ok(RunResult::Completed(payload))
}

/// Dispatch `run(name, input)` over a table built by `export!`.
#[doc(hidden)]
pub fn __dispatch(name: &str, table: &[Entry], input: Payload) -> Result<RunResult, Failure> {
    match table.iter().find(|(n, _)| *n == name) {
        Some((_, run)) => run(input),
        None => Err(Failure::NonRetryable(ErrorInfo {
            kind: UNKNOWN_WORKFLOW.to_string(),
            message: format!("this component does not export workflow {name:?}"),
            details: None,
        })),
    }
}

/// Register the component's workflows and export the WIT `workflows` interface.
///
/// Call exactly once per component (crate-type `cdylib`), at module level:
///
/// ```ignore
/// ostro_sdk::export!(Greeter, Billing);
/// ```
#[macro_export]
macro_rules! export {
    ($($wf:ty),+ $(,)?) => {
        #[doc(hidden)]
        struct __OstroComponent;

        impl $crate::bindings::exports::ostro::runtime::workflows::Guest for __OstroComponent {
            fn describe() -> ::std::vec::Vec<
                $crate::bindings::exports::ostro::runtime::workflows::WorkflowMeta,
            > {
                ::std::vec![$($crate::registry::__describe::<$wf>()),+]
            }

            fn run(
                name: ::std::string::String,
                input: $crate::bindings::ostro::runtime::types::Payload,
            ) -> ::std::result::Result<
                $crate::bindings::exports::ostro::runtime::workflows::RunResult,
                $crate::bindings::ostro::runtime::types::Failure,
            > {
                let table: &[$crate::registry::Entry] = &[
                    $((
                        <$wf as $crate::registry::Workflow>::NAME,
                        $crate::registry::__run::<$wf> as $crate::registry::RunFn,
                    )),+
                ];
                $crate::registry::__dispatch(&name, table, input)
            }
        }

        $crate::bindings::__export_world!(__OstroComponent with_types_in $crate::bindings);
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bindings::exports::ostro::runtime::workflows::RunResult;
    use crate::bindings::ostro::runtime::types::{Failure, Payload};
    use crate::step::json_payload;
    use crate::{Ctx, WorkflowError};

    struct Double;
    impl Workflow for Double {
        const NAME: &'static str = "double";
        const VERSION: u32 = 3;
        const DEFAULT_QUEUE: Option<&'static str> = Some("math");
        const RUN_TIMEOUT_MS: Option<u64> = Some(60_000);
        type Input = u64;
        type Output = u64;
        fn run(_ctx: &Ctx, input: u64) -> Result<u64, WorkflowError> {
            if input == 0 {
                return Err(WorkflowError::non_retryable("zero", "no zeros"));
            }
            Ok(input * 2)
        }
    }

    struct Unit;
    impl Workflow for Unit {
        const NAME: &'static str = "unit";
        type Input = ();
        type Output = ();
        fn run(_ctx: &Ctx, _input: ()) -> Result<(), WorkflowError> {
            Ok(())
        }
    }

    fn completed(r: Result<RunResult, Failure>) -> Vec<u8> {
        match r {
            Ok(RunResult::Completed(p)) => {
                assert_eq!(p.content_type, crate::step::JSON);
                p.data
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    fn non_retryable_kind(r: Result<RunResult, Failure>) -> String {
        match r {
            Err(Failure::NonRetryable(i)) => i.kind,
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn describe_reports_consts() {
        let m = __describe::<Double>();
        assert_eq!(m.name, "double");
        assert_eq!(m.version, 3);
        assert_eq!(m.default_queue.as_deref(), Some("math"));
        assert_eq!(m.run_timeout_ms, Some(60_000));
        let m = __describe::<Unit>();
        assert_eq!(
            (m.version, m.default_queue, m.run_timeout_ms),
            (1, None, None)
        );
    }

    #[test]
    fn run_decodes_input_and_encodes_output() {
        assert_eq!(
            completed(__run::<Double>(json_payload(b"21".to_vec()))),
            b"42"
        );
        // Empty input decodes as JSON null (e.g. `()` inputs).
        assert_eq!(completed(__run::<Unit>(json_payload(Vec::new()))), b"null");
    }

    #[test]
    fn run_maps_errors() {
        assert_eq!(
            non_retryable_kind(__run::<Double>(json_payload(b"0".to_vec()))),
            "zero"
        );
        assert_eq!(
            non_retryable_kind(__run::<Double>(json_payload(b"\"x\"".to_vec()))),
            crate::error::INVALID_JSON
        );
    }

    #[test]
    fn dispatch_by_name_and_unknown() {
        let table: &[Entry] = &[
            (Double::NAME, __run::<Double> as RunFn),
            (Unit::NAME, __run::<Unit> as RunFn),
        ];
        let p = || json_payload(b"5".to_vec());
        assert_eq!(completed(__dispatch("double", table, p())), b"10");
        assert_eq!(
            completed(__dispatch(
                "unit",
                table,
                Payload {
                    content_type: String::new(),
                    data: vec![]
                }
            )),
            b"null"
        );
        assert_eq!(
            non_retryable_kind(__dispatch("nope", table, p())),
            UNKNOWN_WORKFLOW
        );
    }
}
