//! WF-04 "a sleeping run holds no memory" through the real in-process server (plan 05-15).
//!
//! 1,000 runs of `timeouts` `sleep_ms` 1 h are started over REST. Once every one is
//! `Scheduled` (listed by status) the node holds, for all of them together:
//! - 0 live activations and 0 parked activations, memory budget 0 (engine);
//! - 0 in-flight executor jobs and 0 rows in the durable `running` keyspace (no
//!   dispatcher lease, no granted attempt);
//! - 0 durable timer rows due within the timer service's look-ahead span (60 s), so the
//!   in-memory timer window holds none of the 1,000 rows (<= `window_max` trivially);
//! - a bounded RSS growth per sleeper (`/proc/self/status` VmRSS, Linux only).
//!
//! A second test starts 1,000 runs sleeping 2 s and expects all of them to complete. It is
//! named so that it runs after the RSS test (libtest orders by name), so its freed heap
//! cannot hide the sleepers' growth.
//!
//! Both tests are `#[ignore]`d in the parallel workspace run and executed by a dedicated
//! serial CI step (scripts/ci-local.sh "semantics sleepers (serial)" and ci.yml):
//! `cargo test -p ostro-server --test semantics_sleepers --locked -- --ignored --test-threads=1`.
//! RSS is process-wide, so nothing else may run in this process at the same time.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{ServerOpts, TestServer, now_ms, rss_bytes};
use ostro_partition::store::values::RunStatus;
use serde_json::json;
use tokio::sync::Semaphore;
use tokio::time::sleep;

const RUNS: usize = 1_000;
/// Measured on WSL2 (debug, P=4); see the 05-15 SUMMARY. The plan's initial bound.
const RSS_PER_SLEEPER_MAX: u64 = 64 * 1024;
/// The timer service look-ahead (`TimerConfig::default().window_span_ms`).
const WINDOW_SPAN_MS: u64 = 60_000;
const WINDOW_MAX: usize = 10_000;

async fn server() -> TestServer {
    // hot_wait 50 ms: a sleep evicts at once instead of parking for the default window.
    let s = TestServer::start_with(ServerOpts::default(), |c| {
        c.engine.hot_wait = Duration::from_millis(50).into();
    })
    .await;
    s.wait_ready(Duration::from_secs(10)).await;
    s.deploy_guest("timeouts", None).await;
    s
}

/// Start `RUNS` `sleep_ms(n)` runs over REST, 32 requests in flight.
async fn start_many(s: &Arc<TestServer>, n_ms: u64) -> Vec<String> {
    let gate = Arc::new(Semaphore::new(32));
    let mut tasks = Vec::with_capacity(RUNS);
    for i in 0..RUNS {
        let s = Arc::clone(s);
        let gate = Arc::clone(&gate);
        tasks.push(tokio::spawn(async move {
            let _p = gate.acquire_owned().await.expect("gate");
            let id = s
                .start_rest(
                    "timeouts",
                    json!({"input": {"mode": "sleep_ms", "n": n_ms}, "tags": [format!("s{i}")]}),
                )
                .await;
            id.to_string()
        }));
    }
    let mut ids = Vec::with_capacity(RUNS);
    for t in tasks {
        ids.push(t.await.expect("start task"));
    }
    ids
}

/// Poll the status list until it holds `n` runs (every page).
async fn wait_listed(s: &TestServer, status: &str, n: usize, limit: Duration) -> Duration {
    let t0 = Instant::now();
    loop {
        let got = s
            .list_all(&format!("workflow=timeouts&status={status}"))
            .await
            .len();
        if got >= n {
            return t0.elapsed();
        }
        assert!(
            t0.elapsed() < limit,
            "only {got}/{n} runs {status} after {limit:?}"
        );
        sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "serial: dedicated ci step"]
async fn sleepers_hold_no_memory() {
    let s = Arc::new(server().await);
    // Warm up: compile/instantiate caches, allocator pools, HTTP clients. A few full runs
    // so the baseline includes everything that is per-process, not per-sleeper.
    for _ in 0..3 {
        let id = s
            .start_rest("timeouts", json!({"input": {"mode": "sleep_ms", "n": 10}}))
            .await;
        s.wait_status(&id, RunStatus::Completed, Duration::from_secs(30))
            .await;
    }
    let base = rss_bytes();

    let t0 = Instant::now();
    let ids = start_many(&s, 3_600_000).await;
    let started = t0.elapsed();
    let listed = wait_listed(&s, "scheduled", RUNS, Duration::from_secs(120)).await;

    // Every record agrees: suspended on its timer, journal started (the last attempt
    // token stays in the record; the `running` keyspace below is the grant probe).
    for id in &ids {
        let rec = s.run_record(&id.parse().expect("id"));
        assert_eq!(rec.status, RunStatus::Scheduled, "{id}");
        assert!(rec.next_seq > 0, "{id}: never activated");
    }
    // Let in-flight bookkeeping (job records, eviction drops) settle.
    let t1 = Instant::now();
    while s.h().runtime().live_activations() > 0 || s.in_flight_jobs() > 0 {
        assert!(
            t1.elapsed() < Duration::from_secs(10),
            "live {} in-flight {}",
            s.h().runtime().live_activations(),
            s.in_flight_jobs()
        );
        sleep(Duration::from_millis(20)).await;
    }

    let rt = s.h().runtime();
    assert_eq!(rt.live_activations(), 0, "live activations");
    assert_eq!(rt.parked().parked_count(), 0, "parked activations");
    assert_eq!(rt.parked().budget().used(), 0, "memory budget in use");
    assert_eq!(s.in_flight_jobs(), 0, "executor jobs (dispatcher leases)");
    assert_eq!(
        s.running_rows(),
        0,
        "durable running rows (granted attempts)"
    );
    let (timers, near) = s.timer_rows(now_ms() + WINDOW_SPAN_MS);
    assert!(
        timers >= RUNS,
        "one durable timer row per sleeper ({timers})"
    );
    assert_eq!(
        near, 0,
        "no sleeper timer is due within the window span, so the window holds none"
    );
    assert!(near <= WINDOW_MAX);

    match (base, rss_bytes()) {
        (Some(before), Some(after)) => {
            let delta = after.saturating_sub(before);
            let per = delta / RUNS as u64;
            println!(
                "WF-04: {RUNS} sleepers: started in {started:?}, all scheduled after +{listed:?}; \
                 RSS {before} -> {after} bytes, delta {delta} = {per} bytes/sleeper \
                 (bound {RSS_PER_SLEEPER_MAX}); durable timer rows {timers}"
            );
            assert!(
                per <= RSS_PER_SLEEPER_MAX,
                "RSS grew {per} bytes per sleeper (bound {RSS_PER_SLEEPER_MAX})"
            );
        }
        _ => println!("WF-04: RSS check skipped (no /proc/self/status on this platform)"),
    }

    let mut s = Arc::try_unwrap(s).unwrap_or_else(|_| panic!("server still shared"));
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "serial: dedicated ci step"]
async fn wake_all_short_sleepers() {
    let s = Arc::new(server().await);
    let t0 = Instant::now();
    let ids = start_many(&s, 2_000).await;
    let started = t0.elapsed();
    let done = wait_listed(&s, "completed", RUNS, Duration::from_secs(30)).await;
    println!("WF-04: {RUNS} 2 s sleepers started in {started:?}, all completed after +{done:?}");
    for id in &ids {
        let b = s
            .result_bytes(&id.parse().expect("id"))
            .await
            .expect("result");
        assert_eq!(b, br#""slept""#, "{id}");
    }
    assert_eq!(s.h().runtime().live_activations(), 0);
    let mut s = Arc::try_unwrap(s).unwrap_or_else(|_| panic!("server still shared"));
    s.shutdown().await;
}
