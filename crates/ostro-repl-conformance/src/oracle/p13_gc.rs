//! Module GC oracle (13-08; scenarios in 13-15 / 13-17, fixes in 13-18).
//!
//! [`no_module_gone_for_live`]: no run that was non-terminal when its module was retired
//! ever fails with `module_gone`. Both inputs are client measurements: the scenario lists
//! the module's non-terminal runs (`GET /v1/runs`) right before it retires the module
//! (`DELETE /v1/modules/{sha}`), and reads every run's final answer (`GET /v1/runs/{id}`).

use std::collections::BTreeMap;

use serde_json::Value;

use super::Report;

/// The reason code of a run that failed because its module was garbage-collected.
pub const MODULE_GONE: &str = "module_gone";

/// One module retirement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetireEvent {
    /// The retired module (hex sha256).
    pub module_sha: String,
    /// Runs of that module that were not terminal at retire time.
    pub non_terminal_runs: Vec<String>,
}

/// A run's final answer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunResult {
    /// Run id.
    pub run_id: String,
    /// Its status (`completed`, `failed`, …).
    pub status: String,
    /// The failure detail as returned (reason code / message), if any.
    pub error: Option<String>,
}

impl RunResult {
    /// From a `GET /v1/runs/{id}` body (`run_id`, `status`, and the `error` object or string
    /// rendered as text).
    pub fn from_json(v: &Value) -> Self {
        let error = match &v["error"] {
            Value::Null => None,
            Value::String(s) => Some(s.clone()),
            other => Some(other.to_string()),
        };
        Self {
            run_id: v["run_id"].as_str().unwrap_or_default().to_owned(),
            status: v["status"].as_str().unwrap_or_default().to_owned(),
            error,
        }
    }

    /// Whether the run failed with `module_gone`.
    pub fn is_module_gone(&self) -> bool {
        self.error
            .as_deref()
            .is_some_and(|e| e.contains(MODULE_GONE))
    }
}

/// No run listed live at a retirement fails with `module_gone`.
pub fn no_module_gone_for_live(retire_events: &[RetireEvent], run_results: &[RunResult]) -> Report {
    let mut report = Report::new("p13_gc");
    let mut live: BTreeMap<&str, &str> = BTreeMap::new();
    for ev in retire_events {
        for run in &ev.non_terminal_runs {
            live.entry(run.as_str()).or_insert(ev.module_sha.as_str());
        }
    }
    for r in run_results {
        if r.is_module_gone()
            && let Some(sha) = live.get(r.run_id.as_str())
        {
            report.violation(format!(
                "run {} was live when module {sha} was retired but ended {} with {}",
                r.run_id,
                r.status,
                r.error.as_deref().unwrap_or_default()
            ));
        }
    }
    report
}
