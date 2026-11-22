//! Automatic time skipping (plan 10-13, SDK-06, D-P10-6) on the real in-process server with
//! `guests/p10-time`: an idle node with a pending result long-poll fast-forwards to the
//! next due timer or step backoff.
//!
//! - a 3-day sleep completes through `GET /result?wait=30s` in < 5 s of wall time;
//! - without a waiter an idle node does not skip (offset 0 after 2 s);
//! - races: a signal sent before the await is delivered before the sleep target, and a
//!   start immediately followed by an await never sees a skip before `RunStarted`
//!   (`OSTRO_SKIP_RACE_ITERS` iterations each, default 100; the ignored gate runs 1000);
//! - a step backoff (2 x 1 h) and a 48 h run timeout pass through auto skips;
//! - manual mode never auto-skips; a manual skip waits for in-flight API writes and
//!   answers 503 busy when they outlast 5 s;
//! - an open mutating API request holds every auto skip back.
//!
//! D-W26: a skip hook on the dev clock records the dispatcher's lease and grant counts at
//! every skip (under the barrier's write guard, at the skip instant); every test asserts
//! they were all zero.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common::{ServerOpts, TestServer, server_config, tmpfs_dir};
use ostro_partition::leader::dispatcher::IdleStats;
use ostro_server::config::TimeSkipping;
use ostro_types::RunId;
use ostro_types::journal::{EventBody, EventKind};
use reqwest::StatusCode;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const HOUR_MS: u64 = 3_600_000;
const DAY_MS: u64 = 24 * HOUR_MS;

/// A started server plus the skips its dev clock recorded: `(now, target, stats)`.
struct Auto {
    s: TestServer,
    skips: Arc<Mutex<Vec<(u64, u64, IdleStats)>>>,
}

impl Auto {
    async fn start(mode: TimeSkipping) -> Self {
        let dir = tmpfs_dir();
        let mut cfg = server_config(
            dir.path(),
            &ServerOpts {
                partitions: 2,
                ephemeral: true,
            },
        );
        cfg.secrets.auto_sealing_key = true;
        cfg.workers.enabled = false;
        cfg.dev.time_skipping = mode;
        drop(dir);
        let s = TestServer::start_cfg(cfg, None).await;
        s.wait_ready(Duration::from_secs(20)).await;
        let skips = Arc::new(Mutex::new(Vec::new()));
        let rec = Arc::clone(&skips);
        let stats = Arc::clone(s.h().dispatcher_stats());
        s.h()
            .dev_clock()
            .expect("time skipping on")
            .set_skip_hook(Some(Arc::new(move |now, target| {
                rec.lock()
                    .expect("lock")
                    .push((now, target, stats.idle_stats()));
            })));
        s.deploy_guest("p10-time", None).await;
        Self { s, skips }
    }

    fn skip_count(&self) -> usize {
        self.skips.lock().expect("lock").len()
    }

    /// D-W26: zero leases and grants at every skip so far.
    fn assert_skips_clean(&self) {
        let skips = self.skips.lock().expect("lock");
        for (now, target, st) in skips.iter() {
            assert_eq!(
                *st,
                IdleStats::default(),
                "activation at the skip {now} -> {target}"
            );
        }
    }

    async fn dev_time(&self) -> Value {
        let (st, v) = self.s.get_json("/v1/dev/time").await;
        assert_eq!(st, StatusCode::OK, "{v}");
        v
    }

    async fn offset(&self) -> u64 {
        u(&self.dev_time().await, "offset_ms")
    }

    /// `GET /v1/runs/{run}/result?wait=…`.
    async fn await_result(&self, run: &RunId, wait: &str) -> Value {
        let (st, v) = self
            .s
            .get_json(&format!("/v1/runs/{run}/result?wait={wait}"))
            .await;
        assert_eq!(st, StatusCode::OK, "{v}");
        v
    }
}

fn u(v: &Value, k: &str) -> u64 {
    v[k].as_u64().unwrap_or_else(|| panic!("{k} in {v}"))
}

fn iters() -> usize {
    std::env::var("OSTRO_SKIP_RACE_ITERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100)
}

fn percentiles(name: &str, mut v: Vec<Duration>) {
    v.sort();
    let p = |q: f64| v[((v.len() as f64 - 1.0) * q).round() as usize];
    eprintln!(
        "[race] {name}: {} iterations, p50 {:?}, p99 {:?}, max {:?}",
        v.len(),
        p(0.50),
        p(0.99),
        v[v.len() - 1]
    );
}

/// A `POST /v1/workflows/sleep_for/runs` whose body arrives `hold` after its headers (an API write in flight
/// for `hold`). Returns the response status line.
async fn slow_post(s: &TestServer, hold: Duration) -> String {
    let body = br#"{"input":{"ms":1}}"#;
    let mut c = tokio::net::TcpStream::connect(s.addr())
        .await
        .expect("connect");
    let head = format!(
        "POST /v1/workflows/sleep_for/runs HTTP/1.1\r\nhost: {}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        s.addr(),
        body.len()
    );
    c.write_all(head.as_bytes()).await.expect("headers");
    c.write_all(&body[..1]).await.expect("first byte");
    c.flush().await.expect("flush");
    tokio::time::sleep(hold).await;
    c.write_all(&body[1..]).await.expect("body");
    let mut out = Vec::new();
    c.read_to_end(&mut out).await.expect("response");
    String::from_utf8_lossy(&out)
        .lines()
        .next()
        .unwrap_or("")
        .to_owned()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_three_day_sleep() {
    let a = Auto::start(TimeSkipping::Auto).await;
    let run =
        a.s.start_rest("sleep_for", json!({"input": {"ms": 3 * DAY_MS}}))
            .await;
    let wall = Instant::now();
    let r = a.await_result(&run, "30s").await;
    let took = wall.elapsed();
    eprintln!("[wall] auto sleep_for 3d via result --wait: {took:?}");
    assert_eq!(r["completed"], true, "{r}");
    assert_eq!(r["status"], "completed", "{r}");
    assert_eq!(r["result"]["slept"], 3 * DAY_MS, "{r}");
    assert!(took < Duration::from_secs(5), "{took:?}");
    let t = a.dev_time().await;
    assert!(u(&t, "offset_ms") >= 3 * DAY_MS - 5_000, "{t}");
    assert_eq!(t["mode"], "auto");
    assert!(a.skip_count() >= 1);
    a.assert_skips_clean();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_waiter_no_skip() {
    let a = Auto::start(TimeSkipping::Auto).await;
    // An empty node is idle with nothing due.
    let t = a.dev_time().await;
    assert_eq!(t["idle"], true, "{t}");
    assert!(t["next_due_ms"].is_null(), "{t}");
    let run =
        a.s.start_rest("sleep_for", json!({"input": {"ms": 3 * DAY_MS}}))
            .await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(a.offset().await, 0);
    assert!(!a.s.run_record(&run).status.is_terminal());
    assert_eq!(a.skip_count(), 0);
    // The detector sees the sleeper: idle, due in about 3 days.
    let t = a.dev_time().await;
    assert_eq!(t["idle"], true, "{t}");
    let due = u(&t, "next_due_ms");
    let now = u(&t, "now_ms");
    assert!(
        due > now + 3 * DAY_MS - 10_000 && due <= now + 3 * DAY_MS,
        "{t}"
    );
}

/// `iters` rounds of: start `signal_then_sleep`, signal `go`, await at once.
async fn signal_before_await(a: &Auto, iters: usize) {
    let mut lat = Vec::with_capacity(iters);
    for i in 0..iters {
        let t = Instant::now();
        let run =
            a.s.start_rest("signal_then_sleep", json!({"input": {}}))
                .await;
        let (st, v) =
            a.s.post_json(
                &format!("/v1/runs/{run}/signals/go"),
                &json!({"payload": {"n": i}}),
            )
            .await;
        assert!(st.is_success(), "signal {i}: {st} {v}");
        let r = a.await_result(&run, "30s").await;
        lat.push(t.elapsed());
        assert_eq!(r["completed"], true, "iteration {i}: {r}");
        assert_eq!(r["result"], json!({"n": i}), "iteration {i}: {r}");
        let kinds = a.s.history_kinds(&run).await;
        let sig = kinds
            .iter()
            .position(|k| *k == EventKind::SignalMatched)
            .unwrap_or_else(|| panic!("iteration {i}: no SignalMatched: {kinds:?}"));
        let timer = kinds
            .iter()
            .position(|k| *k == EventKind::TimerStarted)
            .unwrap_or_else(|| panic!("iteration {i}: no TimerStarted: {kinds:?}"));
        assert!(sig < timer, "iteration {i}: {kinds:?}");
    }
    percentiles("signal_before_await", lat);
    a.assert_skips_clean();
}

/// `iters` rounds of: start `sleep_for 1d`, await immediately; the timer's due is
/// `RunStarted.accepted_at + 1 d` (no skip happened before RunStarted applied).
async fn start_then_await(a: &Auto, iters: usize) {
    let mut lat = Vec::with_capacity(iters);
    for i in 0..iters {
        let t = Instant::now();
        let run =
            a.s.start_rest("sleep_for", json!({"input": {"ms": DAY_MS}}))
                .await;
        let r = a.await_result(&run, "30s").await;
        lat.push(t.elapsed());
        assert_eq!(r["completed"], true, "iteration {i}: {r}");
        let journal = a.s.journal(&run).await;
        let accepted = journal
            .iter()
            .find_map(|e| match &e.body {
                EventBody::RunStarted(r) => Some(r.accepted_at_nanos / 1_000_000),
                _ => None,
            })
            .expect("RunStarted");
        let (timer_ts, due) = journal
            .iter()
            .find_map(|e| match &e.body {
                EventBody::TimerStarted(t) => {
                    Some((e.ts_nanos / 1_000_000, t.fire_at_nanos / 1_000_000))
                }
                _ => None,
            })
            .expect("TimerStarted");
        assert!(
            accepted <= timer_ts,
            "iteration {i}: accepted {accepted} after TimerStarted {timer_ts}"
        );
        assert!(
            due.abs_diff(accepted + DAY_MS) <= 1_000,
            "iteration {i}: due {due} vs accepted + 1d {}",
            accepted + DAY_MS
        );
    }
    percentiles("start_then_await", lat);
    a.assert_skips_clean();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn signal_before_await_race() {
    let a = Auto::start(TimeSkipping::Auto).await;
    let wall = Instant::now();
    signal_before_await(&a, iters()).await;
    eprintln!("[wall] signal_before_await total: {:?}", wall.elapsed());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn start_then_await_race() {
    let a = Auto::start(TimeSkipping::Auto).await;
    let wall = Instant::now();
    start_then_await(&a, iters()).await;
    eprintln!("[wall] start_then_await total: {:?}", wall.elapsed());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "1000-iteration race gate (10-VALIDATION); run explicitly"]
async fn race_gate_1000() {
    let a = Auto::start(TimeSkipping::Auto).await;
    let wall = Instant::now();
    signal_before_await(&a, 1_000).await;
    start_then_await(&a, 1_000).await;
    eprintln!("[wall] race gate total: {:?}", wall.elapsed());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backoff_auto() {
    let a = Auto::start(TimeSkipping::Auto).await;
    let run =
        a.s.start_rest("backoff_step", json!({"input": {"fail_times": 2}}))
            .await;
    let wall = Instant::now();
    let r = a.await_result(&run, "30s").await;
    eprintln!("[wall] auto backoff_step: {:?}", wall.elapsed());
    assert_eq!(r["completed"], true, "{r}");
    assert_eq!(r["result"]["hours"], 2, "{r}");
    let kinds = a.s.history_kinds(&run).await;
    assert_eq!(
        kinds
            .iter()
            .filter(|k| **k == EventKind::StepAttemptFailed)
            .count(),
        2,
        "3 attempts: {kinds:?}"
    );
    assert!(a.offset().await >= 2 * HOUR_MS - 5_000);
    a.assert_skips_clean();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn run_timeout_auto() {
    let a = Auto::start(TimeSkipping::Auto).await;
    let run = a.s.start_rest("wait_forever", json!({"input": {}})).await;
    let wall = Instant::now();
    let r = a.await_result(&run, "30s").await;
    eprintln!(
        "[wall] auto wait_forever (48h run timeout): {:?}",
        wall.elapsed()
    );
    assert_eq!(r["completed"], true, "{r}");
    assert_eq!(r["status"], "timed_out", "{r}");
    assert!(a.offset().await >= 48 * HOUR_MS - 5_000);
    a.assert_skips_clean();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn manual_mode_no_auto() {
    let a = Auto::start(TimeSkipping::Manual).await;
    let run =
        a.s.start_rest("sleep_for", json!({"input": {"ms": 3 * DAY_MS}}))
            .await;
    let r = a.await_result(&run, "2s").await;
    assert_eq!(r["completed"], false, "{r}");
    assert_eq!(a.offset().await, 0);
    assert_eq!(a.skip_count(), 0);
    let (st, v) =
        a.s.post_json("/v1/dev/time/skip", &json!({"by": "72h"}))
            .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    let r = a.await_result(&run, "30s").await;
    assert_eq!(r["completed"], true, "{r}");
    assert_eq!(a.skip_count(), 1);
    a.assert_skips_clean();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn manual_skip_waits_for_api_writes_then_503() {
    let a = Auto::start(TimeSkipping::Manual).await;
    // A 1 s write: the skip waits for it, then applies.
    let s2 = a.s.url("/v1/dev/time/skip");
    let http = a.s.http.clone();
    let post = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let t = Instant::now();
        let r = http
            .post(s2)
            .json(&json!({"by": "1h"}))
            .send()
            .await
            .expect("skip");
        (r.status(), t.elapsed())
    });
    let line = slow_post(&a.s, Duration::from_secs(1)).await;
    assert!(line.contains(" 201 "), "{line}");
    let (st, took) = post.await.expect("join");
    assert_eq!(st, StatusCode::OK);
    assert!(
        took >= Duration::from_millis(500),
        "skip did not wait: {took:?}"
    );
    assert!(a.offset().await >= HOUR_MS - 1_000);
    // A 6 s write outlasts the 5 s wait: 503 busy.
    let s2 = a.s.url("/v1/dev/time/skip");
    let http = a.s.http.clone();
    let post = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let r = http
            .post(s2)
            .json(&json!({"by": "1h"}))
            .send()
            .await
            .expect("skip");
        let st = r.status();
        (st, r.json::<Value>().await.expect("json"))
    });
    let _ = slow_post(&a.s, Duration::from_secs(6)).await;
    let (st, v) = post.await.expect("join");
    assert_eq!(st, StatusCode::SERVICE_UNAVAILABLE, "{v}");
    assert!(
        v.to_string().contains("busy: 0 activations in flight"),
        "{v}"
    );
    a.assert_skips_clean();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn api_write_in_flight_blocks_skip() {
    let a = Auto::start(TimeSkipping::Auto).await;
    let run =
        a.s.start_rest("sleep_for", json!({"input": {"ms": 3 * DAY_MS}}))
            .await;
    // Let the run park first.
    let deadline = Instant::now() + Duration::from_secs(20);
    while !a
        .s
        .history_kinds(&run)
        .await
        .contains(&EventKind::TimerStarted)
    {
        assert!(Instant::now() < deadline, "never parked");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let hold = Duration::from_millis(1_500);
    let slow = slow_post(&a.s, hold);
    let waiter = async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let t = Instant::now();
        let r = a.await_result(&run, "30s").await;
        (r, t.elapsed())
    };
    let probe = async {
        tokio::time::sleep(Duration::from_millis(1_000)).await;
        a.offset().await
    };
    let (line, (r, took), off_mid) = tokio::join!(slow, waiter, probe);
    assert!(line.contains(" 201 "), "{line}");
    assert_eq!(off_mid, 0, "skipped while an API write was in flight");
    assert_eq!(r["completed"], true, "{r}");
    assert!(
        took >= hold - Duration::from_millis(300),
        "completed before the write ended: {took:?}"
    );
    a.assert_skips_clean();
}
