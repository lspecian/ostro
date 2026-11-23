//! Manual-mode time skipping (plan 10-10, SDK-06, D-P10-6) on the real in-process server
//! with `guests/p10-time` (`sleep_for`, `backoff_step`, `wait_forever` with a 48h run
//! timeout, `signal_then_sleep`):
//!
//! - a 3-day sleep completes within 5 s after one 72h skip; the journal's
//!   `RunStarted.accepted_at`, `TimerStarted.fire_at` and the `TimerFired` timestamp agree
//!   with the dev clock (Pitfall 9);
//! - a step failing twice with a 1 h backoff runs again right after each 1 h skip
//!   (dispatcher clock wake) and completes after 3 attempts;
//! - a 48h run timeout fires after a 49h skip (`timed_out`);
//! - a signal delivered before the wait is consumed, then the 1 day sleep is skipped;
//! - `to_ms` in the past skips 0, consecutive skips accumulate, time never goes back;
//! - with time skipping off the endpoints answer 409 and a 3-day sleep stays pending.
//!
//! D-W26 safety: every skip waits until the run is parked (no lease, no grant in flight)
//! and asserts zero leases and zero grants **at the skip instant**, inside the skip
//! barrier's write guard (`DevClock::skip_guarded`'s check). Right after a skip the
//! woken runs take leases, as they should.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use common::{ServerOpts, TestServer, server_config, tmpfs_dir};
use ostro_partition::store::values::RunStatus;
use ostro_server::config::TimeSkipping;
use ostro_types::RunId;
use ostro_types::journal::{EventBody, EventKind};
use reqwest::StatusCode;
use serde_json::{Value, json};

const HOUR_MS: u64 = 3_600_000;
const DAY_MS: u64 = 24 * HOUR_MS;
const LIMIT: Duration = Duration::from_secs(60);

/// An ephemeral dev-profile server with `[dev] time_skipping = mode`.
async fn start(mode: TimeSkipping) -> TestServer {
    let dir = tmpfs_dir();
    let mut cfg = server_config(
        dir.path(),
        &ServerOpts {
            partitions: 2,
            ephemeral: true,
        },
    );
    // The dev profile (TlsProfile::of: auto sealing key), embedded execution only.
    cfg.secrets.auto_sealing_key = true;
    cfg.workers.enabled = false;
    cfg.dev.time_skipping = mode;
    drop(dir);
    let s = TestServer::start_cfg(cfg, None).await;
    s.wait_ready(Duration::from_secs(20)).await;
    s
}

async fn deploy(s: &TestServer) {
    s.deploy_guest("p10-time", None).await;
}

fn leases(s: &TestServer) -> u64 {
    s.h().dispatcher_stats().leases.load(Ordering::SeqCst)
}

fn granting(s: &TestServer) -> u64 {
    s.h().dispatcher_stats().granting.load(Ordering::SeqCst)
}

/// Wait until `run`'s history contains `kind` and the node holds no lease and no grant.
async fn wait_parked(s: &TestServer, run: &RunId, kind: EventKind, n: usize) {
    let deadline = Instant::now() + LIMIT;
    loop {
        let kinds = s.history_kinds(run).await;
        if kinds.iter().filter(|k| **k == kind).count() >= n
            && leases(s) == 0
            && granting(s) == 0
            && !s.run_record(run).status.is_terminal()
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{run} never parked on {kind:?} x{n}: {kinds:?}, leases {}",
            leases(s)
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Skip by `ms` in process through the skip barrier, asserting no lease and no grant at
/// the skip instant (D-W26). Returns the ms skipped.
async fn skip_by(s: &TestServer, ms: u64) -> u64 {
    let dev = Arc::clone(s.h().dev_clock().expect("time skipping on"));
    let stats = Arc::clone(s.h().dispatcher_stats());
    assert_eq!(
        stats.leases.load(Ordering::SeqCst),
        0,
        "lease before the skip"
    );
    dev.skip_guarded(|now| {
        assert_eq!(stats.leases.load(Ordering::SeqCst), 0, "lease at the skip");
        assert_eq!(
            stats.granting.load(Ordering::SeqCst),
            0,
            "grant at the skip"
        );
        Some(now + ms)
    })
    .await
}

async fn post_skip(s: &TestServer, body: Value) -> Value {
    let (st, v) = s.post_json("/v1/dev/time/skip", &body).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    v
}

async fn dev_time(s: &TestServer) -> Value {
    let (st, v) = s.get_json("/v1/dev/time").await;
    assert_eq!(st, StatusCode::OK, "{v}");
    v
}

fn u(v: &Value, k: &str) -> u64 {
    v[k].as_u64().unwrap_or_else(|| panic!("{k} in {v}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_day_sleep_completes_after_one_skip_with_consistent_timestamps() {
    let s = start(TimeSkipping::Manual).await;
    deploy(&s).await;
    let t = dev_time(&s).await;
    assert_eq!(t["mode"], "manual", "{t}");
    assert_eq!(u(&t, "offset_ms"), 0, "{t}");
    let run = s
        .start_rest("sleep_for", json!({"input": {"ms": 3 * DAY_MS}}))
        .await;
    wait_parked(&s, &run, EventKind::TimerStarted, 1).await;
    let wall = Instant::now();
    let skipped = skip_by(&s, 3 * DAY_MS).await;
    assert_eq!(skipped, 3 * DAY_MS);
    s.wait_status(&run, RunStatus::Completed, Duration::from_secs(5))
        .await;
    let took = wall.elapsed();
    eprintln!("[wall] sleep_for 3d after a 72h skip: {took:?}");
    assert!(took < Duration::from_secs(5), "{took:?}");
    let out: Value =
        serde_json::from_slice(&s.result_bytes(&run).await.expect("result")).expect("json result");
    assert_eq!(out["slept"], 3 * DAY_MS, "{out}");

    // Pitfall 9: one clock for partitions and the engine.
    let now_ms = u(&dev_time(&s).await, "now_ms");
    let journal = s.journal(&run).await;
    let accepted = journal
        .iter()
        .find_map(|e| match &e.body {
            EventBody::RunStarted(r) => Some(r.accepted_at_nanos / 1_000_000),
            _ => None,
        })
        .expect("RunStarted");
    let due = journal
        .iter()
        .find_map(|e| match &e.body {
            EventBody::TimerStarted(t) => Some(t.fire_at_nanos / 1_000_000),
            _ => None,
        })
        .expect("TimerStarted");
    let fired = journal
        .iter()
        .find(|e| matches!(e.body, EventBody::TimerFired(_)))
        .map(|e| e.ts_nanos / 1_000_000)
        .expect("TimerFired");
    let want_due = accepted + 3 * DAY_MS;
    assert!(
        due.abs_diff(want_due) <= 1_000,
        "due {due} vs accepted + 3d {want_due}"
    );
    assert!(fired >= due, "TimerFired ts {fired} before due {due}");
    assert!(
        fired <= now_ms,
        "TimerFired ts {fired} after dev now {now_ms}"
    );
    assert!(
        now_ms >= accepted + 3 * DAY_MS,
        "dev now {now_ms} is not 3 days after {accepted}"
    );
    assert_eq!(leases(&s), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_hour_step_backoff_resolves_after_each_skip() {
    let s = start(TimeSkipping::Manual).await;
    deploy(&s).await;
    let run = s
        .start_rest("backoff_step", json!({"input": {"fail_times": 2}}))
        .await;
    let total = Instant::now();
    for failures in 1..=2 {
        wait_parked(&s, &run, EventKind::StepAttemptFailed, failures).await;
        let wall = Instant::now();
        assert_eq!(skip_by(&s, HOUR_MS).await, HOUR_MS);
        // The next attempt runs within 1 s of the skip (dispatcher clock wake).
        let deadline = wall + Duration::from_secs(1);
        loop {
            let kinds = s.history_kinds(&run).await;
            let fails = kinds
                .iter()
                .filter(|k| **k == EventKind::StepAttemptFailed)
                .count();
            if fails > failures || kinds.contains(&EventKind::StepCompleted) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "attempt {} did not run within 1 s of the skip: {kinds:?}",
                failures + 1
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        eprintln!(
            "[wall] backoff_step attempt {} after a 1h skip: {:?}",
            failures + 1,
            wall.elapsed()
        );
    }
    s.wait_status(&run, RunStatus::Completed, Duration::from_secs(5))
        .await;
    eprintln!("[wall] backoff_step total: {:?}", total.elapsed());
    let kinds = s.history_kinds(&run).await;
    assert_eq!(
        kinds
            .iter()
            .filter(|k| **k == EventKind::StepAttemptFailed)
            .count(),
        2,
        "{kinds:?}"
    );
    let out: Value =
        serde_json::from_slice(&s.result_bytes(&run).await.expect("result")).expect("json result");
    assert_eq!(out["hours"], 2, "{out}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn run_timeout_of_48h_fires_after_a_49h_skip() {
    let s = start(TimeSkipping::Manual).await;
    deploy(&s).await;
    let run = s.start_rest("wait_forever", json!({"input": {}})).await;
    wait_parked(&s, &run, EventKind::SignalWaitStarted, 1).await;
    let wall = Instant::now();
    assert_eq!(skip_by(&s, 49 * HOUR_MS).await, 49 * HOUR_MS);
    let rec = s
        .wait_status(&run, RunStatus::TimedOut, Duration::from_secs(5))
        .await;
    eprintln!(
        "[wall] wait_forever timed out after a 49h skip: {:?}",
        wall.elapsed()
    );
    assert_eq!(rec.terminal_reason, "run_timeout");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn signal_before_the_wait_then_a_skipped_day_sleep() {
    let s = start(TimeSkipping::Manual).await;
    deploy(&s).await;
    let run = s
        .start_rest("signal_then_sleep", json!({"input": {}}))
        .await;
    let (st, v) = s
        .post_json(
            &format!("/v1/runs/{run}/signals/go"),
            &json!({"payload": {"n": 7}}),
        )
        .await;
    assert!(st.is_success(), "signal: {st} {v}");
    wait_parked(&s, &run, EventKind::TimerStarted, 1).await;
    let wall = Instant::now();
    assert_eq!(skip_by(&s, DAY_MS).await, DAY_MS);
    s.wait_status(&run, RunStatus::Completed, Duration::from_secs(5))
        .await;
    eprintln!(
        "[wall] signal_then_sleep after a 1d skip: {:?}",
        wall.elapsed()
    );
    let out: Value =
        serde_json::from_slice(&s.result_bytes(&run).await.expect("result")).expect("json result");
    assert_eq!(out, json!({"n": 7}));
    let kinds = s.history_kinds(&run).await;
    let sig = kinds
        .iter()
        .position(|k| *k == EventKind::SignalMatched)
        .expect("SignalMatched");
    let timer = kinds
        .iter()
        .position(|k| *k == EventKind::TimerStarted)
        .expect("TimerStarted");
    assert!(
        sig < timer,
        "the signal is consumed before the sleep: {kinds:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn skip_api_never_moves_back_and_accumulates() {
    let s = start(TimeSkipping::Manual).await;
    let t0 = dev_time(&s).await;
    assert_eq!(t0["mode"], "manual");
    // 10-13: an empty node is idle, with nothing due.
    assert_eq!(t0["idle"], true, "{t0}");
    assert!(t0["next_due_ms"].is_null(), "{t0}");
    // In the past: nothing.
    let v = post_skip(&s, json!({"to_ms": 1_000})).await;
    assert_eq!(u(&v, "skipped_ms"), 0, "{v}");
    assert_eq!(u(&v, "offset_ms"), 0, "{v}");
    let v1 = post_skip(&s, json!({"by": "1h"})).await;
    let s1 = u(&v1, "skipped_ms");
    // `by` is exact (ci-fix6: it used to lose the ms the wall clock ticked mid-skip).
    assert_eq!(s1, HOUR_MS, "{v1}");
    let v2 = post_skip(&s, json!({"by": "2h"})).await;
    let off = u(&v2, "offset_ms");
    assert_eq!(off, 3 * HOUR_MS, "{v2}");
    assert!(
        u(&v2, "now_ms") >= u(&v1, "now_ms") + 2 * HOUR_MS - 50,
        "{v1} {v2}"
    );
    let target = u(&v2, "now_ms") + DAY_MS;
    let v3 = post_skip(&s, json!({"to_ms": target})).await;
    assert!(u(&v3, "now_ms") >= target, "{v3}");
    let t = dev_time(&s).await;
    assert!(u(&t, "now_ms") >= target, "{t}");
    assert!(u(&t, "offset_ms") >= off + DAY_MS - 100, "{t}");
    // Bad bodies are 400.
    let (st, v) = s.post_json("/v1/dev/time/skip", &json!({})).await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");
    // Metrics: only exported with time skipping on.
    let m = s
        .http
        .get(s.url("/metrics"))
        .send()
        .await
        .expect("GET /metrics")
        .text()
        .await
        .expect("body");
    assert!(m.contains("ostro_dev_time_offset_ms"), "{m}");
    assert!(m.contains("ostro_dev_time_skips_total"), "{m}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn time_skipping_off_answers_409_and_sleeps_stay_pending() {
    let s = start(TimeSkipping::Off).await;
    assert!(s.h().dev_clock().is_none());
    deploy(&s).await;
    let run = s
        .start_rest("sleep_for", json!({"input": {"ms": 3 * DAY_MS}}))
        .await;
    wait_parked(&s, &run, EventKind::TimerStarted, 1).await;
    for (st, v) in [
        s.get_json("/v1/dev/time").await,
        s.post_json("/v1/dev/time/skip", &json!({"by": "72h"}))
            .await,
    ] {
        assert_eq!(st, StatusCode::CONFLICT, "{v}");
        assert_eq!(v["error"]["details"]["reason"], "time_skipping_disabled");
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!s.run_record(&run).status.is_terminal());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_refuses_time_skipping_outside_ephemeral_dev() {
    // Persistent data dir.
    let dir = tmpfs_dir();
    let mut cfg = server_config(dir.path(), &ServerOpts::default());
    cfg.secrets.auto_sealing_key = true;
    cfg.workers.enabled = false;
    cfg.dev.time_skipping = TimeSkipping::Auto;
    let e = ostro_server::server::start(cfg.clone())
        .await
        .expect_err("persistent dir refused");
    assert!(e.to_string().contains("--ephemeral"), "{e}");
    // Server profile.
    cfg.node.ephemeral = true;
    let e = ostro_server::server::start_with_options(
        cfg.clone(),
        ostro_server::ServerOptions::default()
            .with_tls_profile(ostro_server::tls::TlsProfile::Server),
    )
    .await
    .expect_err("server profile refused");
    assert!(e.to_string().contains("ostro server"), "{e}");
    // Worker listener.
    cfg.workers.enabled = true;
    let e = ostro_server::server::start(cfg)
        .await
        .expect_err("worker listener refused");
    assert!(e.to_string().contains("worker listener"), "{e}");
}
