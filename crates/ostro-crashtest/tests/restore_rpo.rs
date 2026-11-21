//! SC3 (plan 08-20): kill -9 under load, `ostro restore` from the `file://` bucket, RPO.
//!
//! Each of [`ITERS`] iterations starts a fresh release server (the 05-19 failpoint binary,
//! unarmed) with `[backup] url = file://<tmp>/bucket`, `seal_interval = 1s` and small
//! snapshot thresholds, drives crash-saga starts from 6 concurrent clients (each
//! acknowledged start is recorded with its ack time), kills the server with SIGKILL at a
//! random point 3-7 s in, and runs `ostro restore --from file://<bucket> --data-dir <new>
//! --json`. Asserted per iteration:
//!
//! - (a) the restore exits 0: the manifest validates, every partition's state hash matches
//!   at its verify offset, the cut is reached and the cross-partition check passes;
//! - (b) every start acknowledged before the cut (`ack_ms + 50 ms < cut_at_ms`; the ack
//!   arrives after the apply, the cut is read before `cut_at_ms`) is present in the restored
//!   data dir (opened offline in Inspect mode);
//! - (c) RPO per P8-D23 (as revised by 08-23): the **data-loss window** (ack time of the
//!   newest acknowledged start missing after the restore - `cut_at_ms`; 0 when nothing is
//!   missing) is at most `seal_interval` + [`LOSS_SLACK_MS`] (500 ms). This is the
//!   end-to-end RPO of SC3/BACK-02. Also asserted: the restored manifest's **cut lag** `min(kill, start of the
//!   in-flight round) - manifest write time` (= `kill - cut_at_ms - upload latency - the
//!   in-flight round's time`) is at most `seal_interval` + 500 ms scheduling slack. The
//!   manifest write time is the mtime of `manifest/current`; the in-flight round's start is
//!   bounded by the first bucket object written after it (an over-estimate of the lag, so
//!   the assertion is conservative). Reported separately: the **upload latency** (manifest
//!   write time - `cut_at_ms`, plus every round's `ostro_backup_upload_seconds` sampled from
//!   `/metrics`);
//! - (d) every partition's restored `cut_offset` is at most the killed source's recovered
//!   applied offset (uploads only below HW); the live leader's metrics show chunk bytes
//!   uploaded, and opening the killed data dir offline (Inspect mode, closed barriers)
//!   writes nothing to the bucket.
//!
//! The last iteration also runs the backup file round trip: `ostro backup --to f.tar
//! --server <live>` before the kill, then `ostro restore --from f.tar` into a fresh dir.
//!
//! `#[ignore]` (crash profile):
//!
//! ```text
//! cargo nextest run -P crash -p ostro-crashtest --run-ignored only --locked restore_rpo
//! ```
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ostro_crashtest::campaign::PARTITIONS;
use ostro_crashtest::server_target::{KillOutcome, ServerTarget, deploy_crash_saga, failpoint_bin};
use ostro_crashtest::{Rng, open_node_within};
use ostro_partition::node::StartMode;
use ostro_partition::query::runs_page;
use ostro_types::PartitionId;
use serde_json::{Value, json};

const ITERS: usize = 10;
const SEAL_MS: u64 = 1_000;
const SLACK_MS: u64 = 500;
/// Slack of the data-loss window on top of `SEAL_MS` (08-23). It covers what the uploader
/// does not control: the ack is timestamped by the client after the HTTP response (after the
/// apply, so later than the commit the cut can include), the scheduling of the uploader
/// task and the fixed-cadence tick on a shared multi-thread runtime, and the fsyncs of the
/// manifest PUT on a loaded machine. Do not widen it to make a loaded run pass: a failure
/// caused only by load is recorded and re-run on a quiet machine (08-VALIDATION).
const LOSS_SLACK_MS: u64 = 500;

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after 1970")
        .as_millis() as u64
}

fn mtime_ms(p: &Path) -> u64 {
    std::fs::metadata(p)
        .and_then(|m| m.modified())
        .map(|t| t.duration_since(UNIX_EPOCH).expect("mtime").as_millis() as u64)
        .unwrap_or(0)
}

/// Every regular file under `root`: relative path -> (len, mtime ms).
fn listing(root: &Path) -> BTreeMap<PathBuf, (u64, u64)> {
    let mut out = BTreeMap::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&d) else {
            continue;
        };
        for e in rd.flatten() {
            let p = e.path();
            match e.file_type() {
                Ok(t) if t.is_dir() => stack.push(p),
                Ok(t) if t.is_file() => {
                    let len = e.metadata().map(|m| m.len()).unwrap_or(0);
                    let rel = p.strip_prefix(root).expect("under root").to_path_buf();
                    out.insert(rel, (len, mtime_ms(&p)));
                }
                _ => {}
            }
        }
    }
    out
}

fn env(bucket: &Path) -> Vec<(String, String)> {
    [
        ("OSTRO_BACKUP__URL", format!("file://{}", bucket.display())),
        ("OSTRO_BACKUP__SEAL_INTERVAL", format!("{SEAL_MS}ms")),
        ("OSTRO_SNAPSHOTS__ENABLED", "true".into()),
        ("OSTRO_SNAPSHOTS__INTERVAL", "2s".into()),
        ("OSTRO_SNAPSHOTS__MIN_INTERVAL", "1s".into()),
        ("OSTRO_SNAPSHOTS__BYTES", "65536".into()),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_owned(), v))
    .collect()
}

/// `bin args...` with every inherited `OSTRO_*` / FAILPOINTS removed.
fn ostro(bin: &Path, args: &[&str]) -> std::process::Output {
    let mut cmd = Command::new(bin);
    for (k, _) in std::env::vars_os() {
        if k.to_string_lossy().starts_with("OSTRO_") {
            cmd.env_remove(&k);
        }
    }
    cmd.env_remove("FAILPOINTS")
        .args(args)
        .output()
        .unwrap_or_else(|e| panic!("spawn ostro {args:?}: {e}"))
}

fn restore(bin: &Path, from: &str, to: &Path) -> Value {
    let out = ostro(
        bin,
        &[
            "restore",
            "--from",
            from,
            "--data-dir",
            &to.display().to_string(),
            "--json",
        ],
    );
    assert!(
        out.status.success(),
        "ostro restore --from {from} failed ({}):\n{}\n{}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).expect("restore report JSON")
}

/// Run ids in every partition of a stopped data dir (Inspect mode), plus each partition's
/// applied offset.
async fn runs_and_applied(data: &Path) -> (HashSet<String>, Vec<u64>) {
    let node = open_node_within(
        data,
        PARTITIONS,
        StartMode::Inspect,
        Duration::from_secs(120),
    )
    .await
    .unwrap_or_else(|e| panic!("inspect open {}: {e}", data.display()));
    let mut ids = HashSet::new();
    let mut applied = Vec::new();
    for p in 0..PARTITIONS {
        let h = node.handle(PartitionId(p)).expect("partition ready");
        applied.push(h.store().read_meta().expect("meta").applied.0);
        for (id, _) in runs_page(&h.reader(), None, usize::MAX).expect("runs") {
            ids.insert(id.to_string());
        }
    }
    let _ = node.shutdown().await;
    (ids, applied)
}

/// `(sum, count)` of `ostro_backup_upload_seconds` and the chunk upload bytes.
fn parse_metrics(text: &str) -> (f64, u64, f64) {
    let mut sum = 0.0;
    let mut count = 0;
    let mut chunk_bytes = 0.0;
    for l in text.lines() {
        let val = |l: &str| {
            l.rsplit(' ')
                .next()
                .and_then(|v| v.parse::<f64>().ok())
                .unwrap_or(0.0)
        };
        if l.starts_with("ostro_backup_upload_seconds_sum") {
            sum = val(l);
        } else if l.starts_with("ostro_backup_upload_seconds_count") {
            count = val(l) as u64;
        } else if l.starts_with("ostro_backup_upload_bytes_total") && l.contains("kind=\"chunk\"") {
            chunk_bytes = val(l);
        }
    }
    (sum, count, chunk_bytes)
}

fn pct(v: &mut [f64], q: f64) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.sort_by(|a, b| a.partial_cmp(b).expect("finite"));
    v[((v.len() - 1) as f64 * q).round() as usize]
}

struct Iter {
    cut_lag_ms: u64,
    upload_ms: u64,
    loss_ms: i64,
    acked: usize,
    missing_after_cut: usize,
    round_ms: Vec<f64>,
}

async fn iteration(i: usize, bin: &Path, rng: &mut Rng, tar_round_trip: bool) -> Iter {
    let dir = tempfile::tempdir().expect("tempdir");
    let data = dir.path().join("data");
    let bucket = dir.path().join("bucket");
    let http = reqwest::Client::new();
    let env = env(&bucket);
    let mut srv = ServerTarget::spawn_env(bin, &data, None, &env, &http, Duration::from_secs(60))
        .await
        .unwrap_or_else(|e| panic!("spawn: {e}"));
    deploy_crash_saga(&http, &srv.base).await.expect("deploy");

    // Load: 6 clients, every 2xx start recorded with its ack time.
    let acks: Arc<Mutex<Vec<(String, u64)>>> = Arc::default();
    let stop = Arc::new(AtomicBool::new(false));
    let seq = Arc::new(AtomicU64::new(0));
    let mut tasks = Vec::new();
    for _ in 0..6 {
        let (http, base, acks, stop, seq) = (
            http.clone(),
            srv.base.clone(),
            Arc::clone(&acks),
            Arc::clone(&stop),
            Arc::clone(&seq),
        );
        tasks.push(tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                let key = format!("rpo{i}-{}", seq.fetch_add(1, Ordering::Relaxed));
                let body = json!({"idempotency_key": key, "input": {"id": key, "steps": 3,
                    "fail_pattern": "", "sleep_ms": 0, "signal_timeout_ms": 1}});
                let r = http
                    .post(format!("{base}/v1/workflows/crash-saga/runs"))
                    .json(&body)
                    .timeout(Duration::from_secs(10))
                    .send()
                    .await;
                let Ok(r) = r else {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    continue;
                };
                if r.status().is_success() {
                    let v: Value = r.json().await.unwrap_or(Value::Null);
                    let at = now_ms();
                    if let Some(id) = v["run_id"].as_str() {
                        acks.lock().expect("acks").push((id.to_owned(), at));
                    }
                }
            }
        }));
    }
    // Upload-round samples from /metrics.
    let samples: Arc<Mutex<Vec<(f64, u64, f64)>>> = Arc::default();
    let scraper = {
        let (http, base, samples, stop) = (
            http.clone(),
            srv.base.clone(),
            Arc::clone(&samples),
            Arc::clone(&stop),
        );
        tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                if let Ok(r) = http
                    .get(format!("{base}/metrics"))
                    .timeout(Duration::from_secs(2))
                    .send()
                    .await
                    && let Ok(t) = r.text().await
                {
                    samples.lock().expect("samples").push(parse_metrics(&t));
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        })
    };

    let run_for = Duration::from_millis(rng.range(3_000, 7_000));
    tokio::time::sleep(run_for).await;
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while !bucket.join("manifest/current").is_file() {
        assert!(
            std::time::Instant::now() < deadline,
            "no manifest within 30 s"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    if tar_round_trip {
        let tar = dir.path().join("f.tar");
        let out = ostro(
            bin,
            &[
                "backup",
                "--to",
                &tar.display().to_string(),
                "--server",
                &srv.base,
                "--json",
            ],
        );
        assert!(
            out.status.success(),
            "ostro backup failed: {}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        let rep = restore(
            bin,
            &tar.display().to_string(),
            &dir.path().join("from-tar"),
        );
        let (ids, _) = runs_and_applied(&dir.path().join("from-tar")).await;
        println!(
            "backup file round trip: generation {} partitions {} runs {} restore {} ms",
            rep["generation"],
            rep["partitions"].as_array().map_or(0, Vec::len),
            ids.len(),
            rep["durations"]["total_ms"]
        );
        assert_eq!(
            rep["partitions"].as_array().map_or(0, Vec::len),
            usize::from(PARTITIONS)
        );
    }
    let kill_ms = now_ms();
    assert_eq!(srv.kill9().await.expect("kill"), KillOutcome::Killed);
    stop.store(true, Ordering::Relaxed);
    for t in tasks {
        t.abort();
    }
    scraper.abort();

    // (d) the leader uploaded chunks; an offline (Inspect, closed-barrier) open of the
    // killed data dir writes nothing to the bucket.
    let samples = samples.lock().expect("samples").clone();
    let chunk_bytes = samples.iter().map(|s| s.2).fold(0.0, f64::max);
    assert!(chunk_bytes > 0.0, "leader metrics show no chunk upload");
    let before = listing(&bucket);
    let (_, src_applied) = runs_and_applied(&data).await;
    assert_eq!(
        listing(&bucket),
        before,
        "an Inspect open wrote to the bucket"
    );

    // (a) restore.
    let restored = dir.path().join("restored");
    let rep = restore(bin, &format!("file://{}", bucket.display()), &restored);
    let cut_at_ms = rep["cut_at_ms"].as_u64().expect("cut_at_ms");
    let generation = rep["generation"].as_u64().expect("generation");
    let parts = rep["partitions"].as_array().expect("partitions");
    assert_eq!(parts.len(), usize::from(PARTITIONS));
    for pr in parts {
        let p = pr["p"].as_u64().expect("p") as usize;
        let cut = pr["cut_offset"].as_u64().expect("cut_offset");
        assert!(
            cut <= src_applied[p],
            "p{p}: cut {cut} above the source's applied {}",
            src_applied[p]
        );
        assert!(
            pr["verify_state_hash"]
                .as_str()
                .is_some_and(|h| h.len() == 64)
        );
    }

    // (c) cut lag and upload latency.
    // The restore read `manifest/current` (written first in the round; `manifest/gen/<g>`
    // follows only for the first generation and when a snapshot changed).
    let current = bucket.join("manifest/current");
    let manifest_ms = mtime_ms(&current);
    assert!(manifest_ms > 0, "missing {}", current.display());
    let first_after = listing(&bucket)
        .iter()
        .filter(|(p, _)| !p.starts_with("manifest") && !p.starts_with("LOCK"))
        .map(|(_, (_, m))| *m)
        .filter(|m| *m > manifest_ms)
        .min();
    let inflight_start = first_after.map_or(kill_ms, |m| m.min(kill_ms));
    let cut_lag_ms = inflight_start.saturating_sub(manifest_ms);
    let upload_ms = manifest_ms.saturating_sub(cut_at_ms);
    let mut round_ms = Vec::new();
    for w in samples.windows(2) {
        let (dc, ds) = (w[1].1.saturating_sub(w[0].1), w[1].0 - w[0].0);
        if dc > 0 {
            round_ms.push(ds / dc as f64 * 1000.0);
        }
    }

    // (b) every start acked before the cut is restored; the loss window.
    let (ids, _) = runs_and_applied(&restored).await;
    let acks = acks.lock().expect("acks").clone();
    let mut missing_after_cut = 0;
    let mut newest_missing: Option<u64> = None;
    for (id, at) in &acks {
        if ids.contains(id) {
            continue;
        }
        assert!(
            at + 50 >= cut_at_ms,
            "run {id} acked at {at} (cut_at_ms {cut_at_ms}) is missing after the restore"
        );
        missing_after_cut += 1;
        newest_missing = Some(newest_missing.map_or(*at, |m| m.max(*at)));
    }
    let loss_ms = newest_missing.map_or(0, |m| m as i64 - cut_at_ms as i64);
    println!(
        "iter {i}: gen {generation} acked {} missing(after cut) {missing_after_cut} cut_lag {cut_lag_ms} ms \
         upload {upload_ms} ms loss_window {loss_ms} ms kill-cut {} ms rounds {}",
        acks.len(),
        kill_ms.saturating_sub(cut_at_ms),
        round_ms.len()
    );
    assert!(
        cut_lag_ms <= SEAL_MS + SLACK_MS,
        "iter {i}: cut lag {cut_lag_ms} ms > seal {SEAL_MS} ms + {SLACK_MS} ms slack"
    );
    Iter {
        cut_lag_ms,
        upload_ms,
        loss_ms,
        acked: acks.len(),
        missing_after_cut,
        round_ms,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "spawns the release server, kills it and restores 10 times; crash profile"]
async fn restore_rpo() {
    let bin = failpoint_bin();
    let mut rng = Rng::new(0x0820_0e70);
    let mut its = Vec::new();
    for i in 0..ITERS {
        its.push(iteration(i, &bin, &mut rng, i + 1 == ITERS).await);
    }
    let mut lag: Vec<f64> = its.iter().map(|x| x.cut_lag_ms as f64).collect();
    let mut up: Vec<f64> = its.iter().map(|x| x.upload_ms as f64).collect();
    let mut loss: Vec<f64> = its.iter().map(|x| x.loss_ms as f64).collect();
    let mut rounds: Vec<f64> = its.iter().flat_map(|x| x.round_ms.clone()).collect();
    println!(
        "restore_rpo: iterations {ITERS} seal {SEAL_MS} ms acked {} missing_after_cut {}",
        its.iter().map(|x| x.acked).sum::<usize>(),
        its.iter().map(|x| x.missing_after_cut).sum::<usize>()
    );
    println!(
        "  cut lag (asserted <= seal + {SLACK_MS} ms): p50 {:.0} ms max {:.0} ms",
        pct(&mut lag, 0.5),
        pct(&mut lag, 1.0)
    );
    println!(
        "  upload latency of the restored round: p50 {:.0} ms max {:.0} ms; all rounds \
         (ostro_backup_upload_seconds, {} samples): p50 {:.0} ms max {:.0} ms",
        pct(&mut up, 0.5),
        pct(&mut up, 1.0),
        rounds.len(),
        pct(&mut rounds, 0.5),
        pct(&mut rounds, 1.0)
    );
    println!(
        "  data-loss window (newest lost ack - cut_at_ms, asserted <= seal + {LOSS_SLACK_MS} ms): \
         p50 {:.0} ms max {:.0} ms",
        pct(&mut loss, 0.5),
        pct(&mut loss, 1.0)
    );
    // The end-to-end RPO (P8-D23 as revised by 08-23), checked after every iteration ran so
    // a failure still prints the full distribution above.
    let over: Vec<(usize, i64)> = its
        .iter()
        .enumerate()
        .filter(|(_, x)| x.loss_ms > (SEAL_MS + LOSS_SLACK_MS) as i64)
        .map(|(i, x)| (i, x.loss_ms))
        .collect();
    assert!(
        over.is_empty(),
        "data-loss window above seal {SEAL_MS} ms + {LOSS_SLACK_MS} ms slack in (iteration, ms): {over:?}"
    );
}
