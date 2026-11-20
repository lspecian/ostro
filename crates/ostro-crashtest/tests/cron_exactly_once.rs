//! SC2 (plan 08-20): cron exactly-once across >= 20 `kill -9` restarts.
//!
//! A spawned release server (the crash-target pattern: the 05-19 `ostro dev` failpoint
//! binary, started without FAILPOINTS) runs a `* * * * * *` schedule in `America/New_York`
//! (overlap AllowAll, catch-up One). The loop runs the server 2-5 s, kills it with SIGKILL
//! (confirmed by waitpid), keeps it down 0-4 s and restarts it, at least 20 times. Then
//! the data dir is opened offline (Inspect mode) and every run with a
//! `__cron/<schedule>/<fire_time>` idempotency key is collected from every partition:
//!
//! - each fire_time has exactly one run (no duplicate fire after any restart);
//! - every fire_time is a whole second;
//! - every second between the first and the last fire has a run, except inside a downtime
//!   window (2 s before the kill .. 2 s after the restarted server answered `/readyz`);
//! - inside each downtime window the missing seconds form at most one contiguous gap:
//!   catch-up One fires only the latest missed instant, then the steady state resumes.
//!
//! `#[ignore]` (crash profile):
//!
//! ```text
//! cargo nextest run -P crash -p ostro-crashtest --run-ignored only --locked cron_exactly_once
//! ```
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ostro_crashtest::campaign::PARTITIONS;
use ostro_crashtest::server_target::{KillOutcome, ServerTarget, deploy_crash_saga, failpoint_bin};
use ostro_crashtest::{Rng, open_node_within};
use ostro_partition::node::StartMode;
use ostro_partition::query::runs_page;
use ostro_types::PartitionId;
use serde_json::json;

const SCHEDULE: &str = "tick-ny";
const RESTARTS: usize = 20;

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after 1970")
        .as_millis() as u64
}

async fn spawn(
    bin: &std::path::Path,
    data: &std::path::Path,
    http: &reqwest::Client,
) -> ServerTarget {
    let mut last = String::new();
    for _ in 0..5 {
        match ServerTarget::spawn(bin, data, None, http, Duration::from_secs(60)).await {
            Ok(s) => return s,
            Err(e) => last = e,
        }
    }
    panic!("server did not start: {last}");
}

/// The missing seconds in `[lo, hi]` (both whole seconds) as contiguous runs.
fn gaps(fires: &BTreeMap<u64, usize>, lo: u64, hi: u64) -> Vec<(u64, u64)> {
    let mut out: Vec<(u64, u64)> = Vec::new();
    let mut t = lo;
    while t <= hi {
        if !fires.contains_key(&t) {
            match out.last_mut() {
                Some(g) if g.1 + 1000 == t => g.1 = t,
                _ => out.push((t, t)),
            }
        }
        t += 1000;
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "spawns the release server and kills it >= 20 times; crash profile"]
async fn cron_exactly_once() {
    let bin = failpoint_bin();
    let http = reqwest::Client::new();
    let dir = tempfile::tempdir().expect("tempdir");
    let data = dir.path().join("data");
    let mut rng = Rng::new(0x0820_c201);

    let mut srv = spawn(&bin, &data, &http).await;
    deploy_crash_saga(&http, &srv.base)
        .await
        .expect("deploy crash-saga");
    let body = json!({
        "name": SCHEDULE,
        "workflow": "crash-saga",
        "cron": "* * * * * *",
        "timezone": "America/New_York",
        "input": {"id": "cron", "steps": 3, "fail_pattern": "", "sleep_ms": 0,
                  "signal_timeout_ms": 1},
        "overlap": "allow_all",
        "catchup": "one",
    });
    let r = http
        .post(format!("{}/v1/schedules", srv.base))
        .json(&body)
        .send()
        .await
        .expect("POST schedule");
    assert_eq!(
        r.status().as_u16(),
        201,
        "{}",
        r.text().await.unwrap_or_default()
    );

    // (kill_ms, ready_ms) of every downtime.
    let mut downs: Vec<(u64, u64)> = Vec::new();
    let mut kills = 0usize;
    while kills < RESTARTS {
        tokio::time::sleep(Duration::from_millis(rng.range(2_000, 5_000))).await;
        let kill_ms = now_ms();
        match srv.kill9().await.expect("kill") {
            KillOutcome::Killed => kills += 1,
            KillOutcome::AlreadyExited => panic!("server exited on its own:\n{}", srv.tail(30)),
        }
        // Stay down 0-4 s, so most downtimes miss fires and exercise catch-up One.
        tokio::time::sleep(Duration::from_millis(rng.range(0, 4_000))).await;
        srv = spawn(&bin, &data, &http).await;
        let ready_ms = now_ms();
        downs.push((kill_ms, ready_ms));
        println!("kill {kills}: down {} ms", ready_ms.saturating_sub(kill_ms));
    }
    // Run a little longer, then the final kill (the data dir must be closed for Inspect).
    tokio::time::sleep(Duration::from_secs(3)).await;
    let end_ms = now_ms();
    assert_eq!(srv.kill9().await.expect("final kill"), KillOutcome::Killed);

    let node = open_node_within(
        &data,
        PARTITIONS,
        StartMode::Inspect,
        Duration::from_secs(120),
    )
    .await
    .expect("inspect open");
    let prefix = format!("__cron/{SCHEDULE}/");
    let mut fires: BTreeMap<u64, usize> = BTreeMap::new();
    let mut manual = 0usize;
    for p in 0..PARTITIONS {
        let h = node.handle(PartitionId(p)).expect("partition ready");
        for (_, rec) in runs_page(&h.reader(), None, usize::MAX).expect("runs") {
            let Ok(key) = String::from_utf8(rec.idempotency_key) else {
                continue;
            };
            let Some(rest) = key.strip_prefix(&prefix) else {
                continue;
            };
            if rest.starts_with("manual/") {
                manual += 1;
                continue;
            }
            let t: u64 = rest.parse().expect("fire time");
            *fires.entry(t).or_insert(0) += 1;
        }
    }
    let _ = node.shutdown().await;

    let dups: Vec<_> = fires.iter().filter(|(_, n)| **n != 1).collect();
    assert!(
        dups.is_empty(),
        "fire times with more than one run: {dups:?}"
    );
    assert_eq!(manual, 0);
    assert!(fires.keys().all(|t| t % 1000 == 0), "non-second fire times");
    let first = *fires.keys().next().expect("at least one fire");
    let last = *fires.keys().next_back().expect("fires");
    assert!(
        last + 5_000 >= end_ms,
        "no fires near the end: last {last}, end {end_ms}"
    );

    let windows: Vec<(u64, u64)> = downs
        .iter()
        .map(|(k, r)| (k.saturating_sub(2_000), r + 2_000))
        .collect();
    let all_gaps = gaps(&fires, first, last);
    let mut per_window = vec![0usize; windows.len()];
    for g in &all_gaps {
        let w = windows
            .iter()
            .position(|(lo, hi)| g.0 >= *lo && g.1 <= *hi)
            .unwrap_or_else(|| {
                panic!(
                    "gap {}..={} ms ({} fires missing) outside every downtime window {windows:?}",
                    g.0,
                    g.1,
                    (g.1 - g.0) / 1000 + 1
                )
            });
        per_window[w] += 1;
    }
    for (i, n) in per_window.iter().enumerate() {
        assert!(
            *n <= 1,
            "downtime {i} {:?}: {n} separate gaps (catch-up One allows one)",
            windows[i]
        );
    }
    let missed: u64 = all_gaps.iter().map(|g| (g.1 - g.0) / 1000 + 1).sum();
    let down_s: u64 = downs.iter().map(|(k, r)| r - k).sum::<u64>() / 1000;
    println!(
        "cron_exactly_once: kills={kills} fires={} unique=all span={} s missed_in_downtime={missed} \
         downtime_total={down_s} s gaps={} windows_with_gap={}",
        fires.len(),
        (last - first) / 1000 + 1,
        all_gaps.len(),
        per_window.iter().filter(|n| **n > 0).count()
    );
}
