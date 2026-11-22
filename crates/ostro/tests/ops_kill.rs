//! PHASE-11 GATE (plan 11-18): the Phase 11 kill tests on the real binary built with
//! `--features failpoints` (`FAILPOINTS` env var, plan 05-18). Each test arms one Phase 11
//! failpoint, lets the node reach it, `kill -9`s the process, restarts it on the same data
//! dir without the failpoint and checks the recovery:
//!
//! - `kill_rerun_before_rearm`: `rerun::before_rearm=return` skips the re-arm of the open
//!   timer a rerun copied; after kill + restart the timer still fires and the rerun
//!   completes (one completion per step).
//! - `kill_hook_after_start`: `dlq::hook_after_start=return` starts the on_failure hook run
//!   but skips the `HookStarted` mark; after kill + restart the catch-up finds the same hook
//!   run (dedupe `__onfail/<run>/0`): exactly one hook.
//! - `kill_bulk_after_op`: `bulk::after_op=25*off->return` stops a bulk cancel after 26
//!   ops; after kill + restart the job resumes from its last checkpoint and finishes with
//!   every run cancelled and `cancel_requested` journaled once per run.
//!
//! Run: `cargo test -p ostro --features failpoints --test ops_kill -- --test-threads=1`
//! (FAILPOINT_CRATES entry `ostro:ops_kill`).
#![cfg(feature = "failpoints")]
#![cfg(unix)]

mod common;
#[path = "common/p11.rs"]
mod p11;

use std::time::{Duration, Instant};

use common::*;
use p11::*;
use reqwest::StatusCode;
use serde_json::{Value, json};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kill_rerun_before_rearm() {
    let _g = SERIAL.lock().await;
    let fp = [("FAILPOINTS", "rerun::before_rearm=return")];
    let mut n = P11Node::start(&Profile::Dev, &fp).await;
    n.deploy("p11-rerun");
    let (src, st) = n.run_wait("pipeline", r#"{"fail_at":3,"sleep_between_ms":300}"#, &[]);
    assert_eq!(st, "failed");
    let sh = n.api.history(&src).await;
    let timer = events_of(&sh, "timer_started")[0]["seq"]
        .as_u64()
        .expect("timer seq");
    let cut = timer + 1;
    n.deploy("p11-rerun-v2");
    let t0 = Instant::now();
    let rr = n.ok(&["rerun", &src, "--from-seq", &cut.to_string()]);
    let run = rr["new_run_id"].as_str().expect("new_run_id").to_owned();
    // The copied timer is long past due: re-armed it would fire at once. It does not.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let r = n.api.run(&run).await;
    assert!(
        !is_terminal(r["status"].as_str().unwrap()),
        "no re-arm, the rerun must still be open: {r}"
    );
    assert_eq!(
        n.api.history(&run).await.len() as u64,
        cut,
        "no live event after the cut"
    );
    n.kill9();
    let t1 = Instant::now();
    n.boot(&Profile::Dev, &[]).await;
    let done = n.ok(&["result", &run, "--wait"]);
    assert_eq!(done["status"], "completed", "{done}");
    assert_eq!(done["result"], 5, "{done}");
    let h = n.api.history(&run).await;
    assert_eq!(events_of(&h, "timer_fired").len(), 1, "{h:?}");
    for s in ["s1", "s2", "s3", "s4", "s5"] {
        assert_eq!(step_completions(&h, s), 1, "{s}");
    }
    eprintln!(
        "kill_rerun_before_rearm: blocked {:?}, restart to completion {:?}",
        t1 - t0,
        t1.elapsed()
    );
    n.stop();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kill_hook_after_start() {
    let _g = SERIAL.lock().await;
    let fp = [("FAILPOINTS", "dlq::hook_after_start=return")];
    let mut n = P11Node::start(&Profile::Dev, &fp).await;
    n.deploy("p11-hook");
    let (st, v) = n
        .rest(
            "PUT",
            "/v1/workflows/w/failure-policy",
            Some(json!({"on_failure": {"workflow": "on_fail", "include_input": true}})),
        )
        .await;
    assert!(st.is_success(), "{st} {v}");
    let (a, st) = n.run_wait("w", r#"{"fail":true,"msg":"crash"}"#, &[]);
    assert_eq!(st, "failed");
    let first = poll("hook run started", T, || async {
        list_all(n.addr, "workflow=on_fail")
            .await
            .into_iter()
            .next()
    })
    .await;
    let first = first["run_id"].as_str().unwrap().to_owned();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let (_, page) = n.rest("GET", "/v1/dlq", None).await;
    let e = page["entries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["run_id"] == a.as_str())
        .cloned()
        .expect("DLQ entry");
    assert!(
        e["hook_run_id"].is_null(),
        "the failpoint skipped HookStarted: {e}"
    );
    n.kill9();
    n.boot(&Profile::Dev, &[]).await;
    let hook = poll("HookStarted after restart", T, || async {
        let (_, page) = n.rest("GET", "/v1/dlq", None).await;
        page["entries"]
            .as_array()
            .and_then(|es| es.iter().find(|e| e["run_id"] == a.as_str()).cloned())
            .and_then(|e| e["hook_run_id"].as_str().map(str::to_owned))
    })
    .await;
    assert_eq!(hook, first, "the catch-up found the started hook run");
    tokio::time::sleep(Duration::from_secs(1)).await;
    let hooks = list_all(n.addr, "workflow=on_fail").await;
    assert_eq!(hooks.len(), 1, "exactly one hook: {hooks:?}");
    let h = n.api.wait_terminal(&hook, T).await;
    assert_eq!(h["status"], "completed", "{h}");
    n.stop();
}

fn progress(v: &Value) -> u64 {
    ["done", "skipped", "failed_count"]
        .iter()
        .map(|k| v[*k].as_u64().unwrap_or(0))
        .sum()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kill_bulk_after_op() {
    let _g = SERIAL.lock().await;
    let bulk_env = [
        ("OSTRO_BULK__CHECKPOINT_OPS", "10"),
        ("OSTRO_BULK__BURST", "1"),
    ];
    let mut env = bulk_env.to_vec();
    env.push(("FAILPOINTS", "bulk::after_op=25*off->return"));
    let mut n = P11Node::start(&Profile::Dev, &env).await;
    n.deploy("signals");
    let mut runs = Vec::new();
    for _ in 0..60 {
        let (st, v) = n
            .api
            .start_http(
                "signals",
                json!({"input": {"mode": "long_wait"}, "tags": ["ca"]}),
            )
            .await;
        assert!(st.is_success(), "{st} {v}");
        runs.push(v["run_id"].as_str().unwrap().to_owned());
    }
    for r in &runs {
        poll("run activated", T, || async {
            (n.api.history(r).await.len() >= 2).then_some(())
        })
        .await;
    }
    let (st, v) = n
        .rest(
            "POST",
            "/v1/bulk",
            Some(json!({"action": "cancel", "filter": {"tags": ["ca"]},
                        "rate_per_sec": 200, "max_in_flight": 1})),
        )
        .await;
    assert!(st.is_success(), "{st} {v}");
    let id = v["job_id"].as_str().expect("job_id").to_owned();
    // Stalled: no progress for 1 s after at least 26 ops.
    let mut last = (u64::MAX, Instant::now());
    let deadline = Instant::now() + T;
    let stalled = loop {
        let (st, j) = n.rest("GET", &format!("/v1/bulk/{id}"), None).await;
        assert_eq!(st, StatusCode::OK, "{j}");
        let p = progress(&j);
        if p != last.0 {
            last = (p, Instant::now());
        } else if p >= 26 && last.1.elapsed() > Duration::from_secs(1) {
            break p;
        }
        assert!(Instant::now() < deadline, "job did not stall: {j}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert!(stalled < 60, "the failpoint stopped the runner early");
    let file = n.data_dir().join("bulk").join(format!("{id}.json"));
    let on_disk: Value =
        serde_json::from_slice(&std::fs::read(&file).expect("job file")).expect("job file json");
    assert_eq!(on_disk["state"], "running", "{on_disk}");
    n.kill9();
    n.boot(&Profile::Dev, &bulk_env).await;
    let (code, done, err) = n.json(&["bulk", "status", &id, "--watch", "--interval", "200ms"]);
    assert_eq!(code, 0, "{done}\n{err}");
    assert_eq!(done["state"], "done", "{done}");
    assert_eq!(
        done["done"].as_u64().unwrap() + done["skipped"].as_u64().unwrap(),
        60,
        "{done}"
    );
    assert_eq!(done["failed_count"], 0, "{done}");
    for r in &runs {
        let fin = n.api.wait_terminal(r, T).await;
        assert_eq!(fin["status"], "cancelled", "{fin}");
        let h = n.api.history(r).await;
        assert_eq!(
            events_of(&h, "cancel_requested").len(),
            1,
            "cancel journaled once for {r}"
        );
    }
    eprintln!(
        "kill_bulk_after_op: stalled at {stalled} ops, checkpoint done {} skipped {}",
        on_disk["done"], on_disk["skipped"]
    );
    n.stop();
}
