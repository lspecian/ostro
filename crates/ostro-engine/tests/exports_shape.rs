//! ENG-1 (plan 09-07): deploy validation accepts componentize-py's extra `exports` export
//! only with its exact `init` shape, next to `ostro:runtime/workflows@0.1.*`.
//!
//! Rust probes stand in for Python here (always run, no Python toolchain):
//! - `exports-shape-ok`: the exact shape (its WIT is a copy of `jco wit` of a real
//!   componentize-py 0.25.1 build) validates with zero violations.
//! - `exports-shape-bad`: same name, `init`'s `stub-wasi` is `u32`: "unexpected type".
//! - the ok probe with every `exports` string renamed to `exportz` (same type, other name)
//!   is an ordinary disallowed extra export, as before.
//! - the ok probe with its workflows export renamed: missing required export, and the
//!   `exports` export alone is not allowed.
//!
//! The ok probe's `abandon-*` workflows are the 09-07 replay regression: a step begun and
//! left open (or dropped) journals nothing live, and replaying that journal must give the
//! same outcome with no nondeterminism.

mod common;

use std::sync::{Arc, OnceLock};

use ostro_engine::config::{EngineConfig, PoolConfig};
use ostro_engine::error::EngineError;
use ostro_engine::runtime::{Prepared, Runtime, RuntimeOptions};
use ostro_engine::sink::{ActivationOutcome, RunRef};
use ostro_engine::testkit::{
    DEFAULT_ACCEPTED_AT, FakeClock, RunSpec, Script, drive_to_completion, replay_journal,
};
use ostro_engine::validate::{COMPONENTIZE_PY_EXPORT, REQUIRED_EXPORT};
use ostro_types::journal::EventKind;

fn rt() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        Runtime::new(
            EngineConfig {
                pool: PoolConfig {
                    enabled: false,
                    ..PoolConfig::default()
                },
                ..EngineConfig::default()
            },
            RuntimeOptions {
                clock: Arc::new(FakeClock::with_step(DEFAULT_ACCEPTED_AT, 0)),
                token_key: [9; 32],
                ..RuntimeOptions::default()
            },
        )
        .expect("runtime")
    })
}

fn violations(res: Result<Prepared, EngineError>) -> Vec<String> {
    match res {
        Err(EngineError::Validation(v)) => v,
        Err(other) => panic!("expected Validation error, got {other:?}"),
        Ok(p) => panic!("expected rejection, got {p:?}"),
    }
}

/// Replace every occurrence of `from` with the same-length `to`.
fn rename(wasm: &[u8], from: &[u8], to: &[u8]) -> Vec<u8> {
    assert_eq!(from.len(), to.len());
    let mut out = wasm.to_vec();
    let mut i = 0;
    let mut hits = 0;
    while i + from.len() <= out.len() {
        if &out[i..i + from.len()] == from {
            out[i..i + from.len()].copy_from_slice(to);
            hits += 1;
            i += from.len();
        } else {
            i += 1;
        }
    }
    assert!(hits > 0, "{:?} not found", String::from_utf8_lossy(from));
    out
}

#[test]
fn exact_shape_validates() {
    let wasm = common::guest_wasm("exports-shape-ok");
    let p = rt().prepare(&wasm).expect("exports-shape-ok must validate");
    assert!(!p.module_hash_hex.is_empty());

    // The production linker's view: both exports recorded.
    let v =
        ostro_engine::validate::validate_component(rt().engine().engine(), rt().linker(), &wasm)
            .expect("validate");
    assert!(
        v.exports.iter().any(|e| e == COMPONENTIZE_PY_EXPORT),
        "{v:?}"
    );
    assert!(
        v.exports
            .iter()
            .any(|e| e.starts_with(&format!("{REQUIRED_EXPORT}@0.1."))),
        "{v:?}"
    );
}

#[test]
fn wrong_shape_rejected() {
    let v = violations(rt().prepare(&common::guest_wasm("exports-shape-bad")));
    assert_eq!(v.len(), 1, "{v:?}");
    assert!(
        v[0].starts_with("export exports has an unexpected type"),
        "{v:?}"
    );
    assert!(v[0].contains("stub-wasi: u32"), "{v:?}");
}

#[test]
fn other_extra_export_still_rejected() {
    let wasm = rename(
        &common::guest_wasm("exports-shape-ok"),
        b"exports",
        b"exportz",
    );
    let v = violations(rt().prepare(&wasm));
    assert_eq!(v.len(), 1, "{v:?}");
    assert!(
        v[0].starts_with("export exportz is not allowed: Ostro workflows export"),
        "{v:?}"
    );
}

#[test]
fn missing_workflows_export_still_rejected() {
    let wasm = rename(
        &common::guest_wasm("exports-shape-ok"),
        b"ostro:runtime/workflows",
        b"ostro:runtime/workflowz",
    );
    let v = violations(rt().prepare(&wasm));
    assert!(
        v.iter()
            .any(|m| m.contains("missing required export ostro:runtime/workflows@0.1.*")),
        "{v:?}"
    );
    // Without the workflows export the componentize-py allowance does not apply.
    assert!(
        v.iter()
            .any(|m| m.starts_with("export exports is not allowed")),
        "{v:?}"
    );
    assert!(
        v.iter()
            .any(|m| m.starts_with("export ostro:runtime/workflowz@0.1.0 is not allowed")),
        "{v:?}"
    );
}

async fn prepared() -> Arc<Prepared> {
    rt().prepare_cached(&common::guest_wasm("exports-shape-ok"))
        .await
        .expect("prepare")
}

fn run_ref() -> RunRef {
    RunRef {
        partition: 1,
        run_id: "run-shape-1".to_string(),
    }
}

/// Live run to completion, then a replay of its journal: same outcome, nothing appended.
async fn live_then_replay(workflow: &str, steps_completed: usize) {
    let p = prepared().await;
    let d = drive_to_completion(
        rt(),
        &p,
        RunSpec::new(run_ref(), workflow, br#""in""#),
        Script::new(),
    )
    .await;
    let ActivationOutcome::Completed { output } = &d.outcome else {
        panic!("{workflow}: expected Completed, got {:?}", d.outcome);
    };
    assert_eq!(output.data, br#""in""#);
    let kinds: Vec<EventKind> = d.journal.iter().map(|e| e.body.kind()).collect();
    let steps = kinds
        .iter()
        .filter(|k| **k == EventKind::StepCompleted)
        .count();
    assert_eq!(steps, steps_completed, "{workflow}: {kinds:?}");
    assert_eq!(kinds.first(), Some(&EventKind::RunStarted));
    assert_eq!(kinds.last(), Some(&EventKind::RunCompleted));

    let (outcome, appended) = replay_journal(rt(), &p, run_ref(), d.journal.clone()).await;
    assert_eq!(
        outcome, d.outcome,
        "{workflow}: replay outcome must be identical"
    );
    assert_eq!(appended, 0, "{workflow}: replay must append nothing");
}

#[tokio::test]
async fn shape_probe_runs_and_replays() {
    live_then_replay("exports-shape-ok", 1).await;
}

/// 09-07 regression: begin a step, return with it still open.
#[tokio::test]
async fn abandoned_open_step_replays() {
    live_then_replay("abandon-open", 0).await;
}

/// 09-07 regression: begin a step, drop it, then complete another step.
#[tokio::test]
async fn abandoned_dropped_step_replays() {
    live_then_replay("abandon-drop", 1).await;
}
