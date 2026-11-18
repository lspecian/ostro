//! Workflow registration: the [`Workflow`] trait and the [`export!`](crate::export) macro.

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
        assert_eq!((m.version, m.default_queue, m.run_timeout_ms), (1, None, None));
    }

    #[test]
    fn run_decodes_input_and_encodes_output() {
        assert_eq!(completed(__run::<Double>(json_payload(b"21".to_vec()))), b"42");
        // Empty input decodes as JSON null (e.g. `()` inputs).
        assert_eq!(completed(__run::<Unit>(json_payload(Vec::new()))), b"null");
    }

    #[test]
    fn run_maps_errors() {
        assert_eq!(non_retryable_kind(__run::<Double>(json_payload(b"0".to_vec()))), "zero");
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
        assert_eq!(completed(__dispatch("unit", table, Payload { content_type: String::new(), data: vec![] })), b"null");
        assert_eq!(non_retryable_kind(__dispatch("nope", table, p())), UNKNOWN_WORKFLOW);
    }
}
