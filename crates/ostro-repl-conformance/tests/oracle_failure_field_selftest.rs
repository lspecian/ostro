//! 13-18 (held for approval): `RunResult::from_json` reads the run's `failure` field.
//!
//! `GET /v1/runs/{id}` carries a failed run's detail in `failure` (a JSON string); the old
//! reading looked at `error` only, so a run live at retire time that failed `module_gone`
//! passed `no_module_gone_for_live` (13-15 folded `failure` in by hand in its scenario).

use ostro_repl_conformance::oracle::p13_gc::{RetireEvent, RunResult, no_module_gone_for_live};
use serde_json::json;

fn event() -> RetireEvent {
    RetireEvent {
        module_sha: "aa".repeat(32),
        non_terminal_runs: vec!["run-live".into()],
    }
}

/// A real-shaped answer of a run that failed `module_gone` (13-17 executor).
fn gone(run: &str) -> serde_json::Value {
    json!({
        "run_id": run,
        "status": "failed",
        "failure": "{\"reason_code\":\"module_gone\",\"kind\":\"module_gone\",\"message\":\"module aa.. is gone\"}",
    })
}

#[test]
fn a_module_gone_failure_of_a_live_run_is_caught() {
    let r = RunResult::from_json(&gone("run-live"));
    // (1) The old reading (`error` only) saw no failure: the oracle passed a violation.
    assert!(
        json!(null) == gone("run-live")["error"],
        "the run answer has no `error` field: the old reading found nothing"
    );
    // (2) New: the failure is read, the oracle flags the live run.
    assert!(r.is_module_gone(), "{r:?}");
    let bad = no_module_gone_for_live(&[event()], &[r]);
    assert!(!bad.is_ok(), "{bad}");
    assert!(bad.mentions("run-live"), "{bad}");

    // (3) Still strict, not over-eager: a run that was not live at retire time may fail
    // module_gone (loud, allowed); other failures and completed runs never count.
    assert!(
        no_module_gone_for_live(&[event()], &[RunResult::from_json(&gone("run-later"))]).is_ok()
    );
    let other = RunResult::from_json(&json!({
        "run_id": "run-live", "status": "failed",
        "failure": "{\"reason_code\":\"step_failed\",\"kind\":\"step\",\"message\":\"s3\"}",
    }));
    assert!(!other.is_module_gone());
    assert!(no_module_gone_for_live(&[event()], &[other]).is_ok());
    let done = RunResult::from_json(&json!({ "run_id": "run-live", "status": "completed" }));
    assert_eq!(done.error, None);
    assert!(no_module_gone_for_live(&[event()], &[done]).is_ok());
    // An `error` field still wins (older answers).
    let legacy = RunResult::from_json(&json!({
        "run_id": "run-live", "status": "failed", "error": "module_gone: x", "failure": "other",
    }));
    assert_eq!(legacy.error.as_deref(), Some("module_gone: x"));
}
