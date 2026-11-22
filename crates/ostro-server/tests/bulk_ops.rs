//! Bulk operations (plan 11-13, OPS-03, D-P11-20) against an in-process server:
//!
//! - dry run (estimate + ≤ 20 samples, nothing changed), cancel, and a second job over the
//!   same filter that only skips;
//! - the snapshot bound: a bulk rerun of 50 tagged runs creates exactly 50 runs (dedupe
//!   `__bulk/<job>/<run>`, wrapped by the rerun as `__rerun/<run>/__bulk/<job>/<run>`) and
//!   terminates;
//! - GCRA pacing ([`Pacer`] over a manual clock), backpressure (`paused_backpressure` while
//!   the target partition's ready backlog is over `backlog_high`), 429 `retry_after_ms`
//!   honoured on restarts, signal `request_id = <job>/<run>` (sha-shortened), requeue
//!   marks, pause / resume;
//! - limits (`bulk_too_many_jobs`, `bulk_too_many_matches`), job cancel;
//! - checkpoints (tmp + rename: never a partial file; cadence `checkpoint_ops`), the
//!   processed set across a restart, finished jobs listed after a restart, unknown file
//!   versions ignored;
//! - (feature `failpoints`, `--test-threads=1`, filter `crash`) crashes at
//!   `bulk::after_op` and `bulk::before_checkpoint`, resumed without a duplicated effect.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;
#[path = "common/p11.rs"]
mod p11;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use ostro_partition::ManualClock;
use ostro_partition::store::values::RunStatus;
use ostro_server::config::BulkSection;
use ostro_server::services::bulk::{Pacer, signal_request_id};
use ostro_types::RunId;
use p11::P11Server;
use reqwest::StatusCode;
use serde_json::{Value, json};
use tokio::time::{Instant, sleep};

const LIMIT: Duration = Duration::from_secs(90);

async fn server_p(
    partitions: u16,
    bulk: impl FnOnce(&mut BulkSection) + Send + 'static,
) -> P11Server {
    let s = P11Server::builder()
        .partitions(partitions)
        .with_bulk(bulk)
        .with_config(|c| {
            c.engine.hot_wait = Duration::from_millis(50).into();
        })
        .start()
        .await;
    s.wait_ready(Duration::from_secs(20)).await;
    s
}

async fn server(bulk: impl FnOnce(&mut BulkSection) + Send + 'static) -> P11Server {
    server_p(4, bulk).await
}

/// `n` `signals` runs (`mode`) tagged `tag`, each waiting (journal started).
async fn signal_runs(s: &P11Server, n: usize, mode: &str, tag: &str) -> Vec<RunId> {
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        out.push(
            s.start_rest("signals", json!({"input": {"mode": mode}, "tags": [tag]}))
                .await,
        );
    }
    for r in &out {
        s.wait_record(r, LIMIT, |rec| rec.next_seq > 0).await;
    }
    out
}

/// `n` failed `w` runs (p11-hook) tagged `tag` (extra body keys from `extra`).
async fn failed_w(s: &P11Server, n: usize, tag: &str, extra: Value) -> Vec<RunId> {
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let mut body = json!({"input": {"fail": true, "msg": format!("m{i}")}, "tags": [tag]});
        if let (Value::Object(b), Value::Object(e)) = (&mut body, extra.clone()) {
            b.extend(e);
        }
        out.push(s.start_rest("w", body).await);
    }
    for r in &out {
        let rec = s
            .wait_record(r, LIMIT, |rec| rec.status.is_terminal())
            .await;
        assert_eq!(rec.status, RunStatus::Failed);
    }
    out
}

/// `POST /v1/bulk` expecting 201; the job id.
async fn create(s: &P11Server, body: Value) -> String {
    let (st, v) = s.bulk_create(&body).await;
    assert_eq!(st, StatusCode::CREATED, "{v}");
    let id = v["job_id"].as_str().expect("job_id").to_owned();
    assert!(id.starts_with("blk_"), "{id}");
    id
}

async fn job(s: &P11Server, id: &str) -> Value {
    let (st, v) = s.get_json(&format!("/v1/bulk/{id}")).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    v
}

fn n(v: &Value, k: &str) -> u64 {
    v[k].as_u64().unwrap_or_else(|| panic!("{k}: {v}"))
}

async fn wait_job(s: &P11Server, id: &str, what: &str, f: impl Fn(&Value) -> bool) -> Value {
    let deadline = Instant::now() + LIMIT;
    loop {
        let v = job(s, id).await;
        if f(&v) {
            return v;
        }
        assert!(Instant::now() < deadline, "job {id}: {what}: {v}");
        sleep(Duration::from_millis(20)).await;
    }
}

fn job_file(s: &P11Server, id: &str) -> Value {
    let p = s.data_dir().join("bulk").join(format!("{id}.json"));
    let b = std::fs::read(&p).unwrap_or_else(|e| panic!("{}: {e}", p.display()));
    serde_json::from_slice(&b).expect("job file json")
}

async fn activate(s: &P11Server, name: &str, version: u32) {
    let r = s
        .http
        .put(s.url(&format!("/v1/workflows/{name}/active")))
        .json(&json!({ "version": version }))
        .send()
        .await
        .expect("PUT active");
    assert!(r.status().is_success(), "activate: {}", r.status());
}

async fn scrape(s: &P11Server) -> String {
    s.http
        .get(s.url("/metrics"))
        .send()
        .await
        .expect("GET /metrics")
        .text()
        .await
        .expect("body")
}

fn metric_at_least(body: &str, series: &str, min: f64) -> bool {
    body.lines().any(|l| {
        l.strip_prefix(series)
            .and_then(|rest| rest.trim().parse::<f64>().ok())
            .is_some_and(|v| v >= min)
    })
}

// ---------------------------------------------------------------------------------
// Task 1
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dry_run_cancel_and_second_job_skips() {
    let s = server(|_| {}).await;
    s.deploy_guest("signals", None).await;
    let runs = signal_runs(&s, 30, "long_wait", "bc").await;
    signal_runs(&s, 3, "long_wait", "other").await;

    // Dry run: estimate and 20 samples, nothing created or changed.
    let (st, v) = s
        .bulk_create(&json!({"action": "cancel", "filter": {"tags": ["bc"]}, "dry_run": true}))
        .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["matched_estimate"], 30, "{v}");
    assert!(v.get("job_id").is_none_or(Value::is_null), "{v}");
    let sample = v["sample"].as_array().expect("sample");
    assert_eq!(sample.len(), 20);
    let ids: BTreeSet<String> = runs.iter().map(ToString::to_string).collect();
    assert!(sample.iter().all(|r| ids.contains(r.as_str().expect("id"))));
    let (st, v) = s.get_json("/v1/bulk").await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(v["jobs"].as_array().expect("jobs").len(), 0, "{v}");
    assert!(runs.iter().all(|r| !s.run_record(r).cancel_requested));

    // Cancel all 30.
    let id = create(
        &s,
        json!({"action": "cancel", "filter": {"tags": ["bc"]}, "rate_per_sec": 200}),
    )
    .await;
    let v = s.bulk_wait(&id, LIMIT).await;
    assert_eq!(v["state"], "done", "{v}");
    assert_eq!(
        (n(&v, "matched"), n(&v, "done"), n(&v, "skipped")),
        (30, 30, 0),
        "{v}"
    );
    assert_eq!(n(&v, "failed_count"), 0, "{v}");
    assert_eq!(v["action"], "cancel");
    assert!(v["finished_at_ms"].as_u64().is_some() && v.get("eta_ms").is_none_or(Value::is_null));
    for r in &runs {
        s.wait_record(r, LIMIT, |rec| rec.status == RunStatus::Cancelled)
            .await;
    }
    let f = job_file(&s, &id);
    assert_eq!(f["version"], 1);
    assert_eq!(f["state"], "done");
    assert_eq!(f["processed"].as_array().expect("processed").len(), 30);

    // Same filter again: nothing left to cancel.
    let id2 = create(
        &s,
        json!({"action": "cancel", "filter": {"tags": ["bc"]}, "rate_per_sec": 200}),
    )
    .await;
    let v = s.bulk_wait(&id2, LIMIT).await;
    assert_eq!((n(&v, "done"), n(&v, "skipped")), (0, 30), "{v}");

    let (_, v) = s.get_json("/v1/bulk").await;
    let listed: Vec<&str> = v["jobs"]
        .as_array()
        .expect("jobs")
        .iter()
        .map(|j| j["job_id"].as_str().expect("id"))
        .collect();
    assert_eq!(listed, vec![id.as_str(), id2.as_str()]);
    let (st, _) = s
        .get_json("/v1/bulk/blk_0190c0de-0000-7000-8000-000000000000")
        .await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    let m = scrape(&s).await;
    assert!(
        metric_at_least(
            &m,
            "ostro_bulk_ops_total{action=\"cancel\",result=\"done\"}",
            30.0
        ),
        "{m}"
    );
    // `ostro_bulk_jobs{state}` is an absolute gauge set by each node's registry, and the
    // in-process test servers share one global recorder (last writer wins), so only the
    // series is asserted here; the job states themselves are checked through the API above.
    assert!(m.contains("ostro_bulk_jobs{state=\"done\"}"), "{m}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rerun_respects_the_snapshot_bound() {
    let s = server(|_| {}).await;
    s.deploy_guest("p11-rerun", None).await;
    let mut sources = Vec::new();
    for _ in 0..50 {
        sources.push(
            s.start_rest("pipeline", json!({"input": {"fail_at": 3}, "tags": ["rr"]}))
                .await,
        );
    }
    for r in &sources {
        let rec = s
            .wait_record(r, LIMIT, |rec| rec.status.is_terminal())
            .await;
        assert_eq!(rec.status, RunStatus::Failed);
    }
    s.deploy_guest("p11-rerun-v2", None).await;
    activate(&s, "pipeline", 2).await;

    // Only the tag: the new runs carry it too, the snapshot bound keeps them out.
    let id = create(
        &s,
        json!({
            "action": {"rerun": {"from": {"step": {"name": "s3"}}}},
            "filter": {"tags": ["rr"]},
            "rate_per_sec": 100,
        }),
    )
    .await;
    let v = s.bulk_wait(&id, LIMIT).await;
    assert_eq!(v["state"], "done", "{v}");
    assert_eq!(
        (n(&v, "matched"), n(&v, "done"), n(&v, "failed_count")),
        (50, 50, 0),
        "{v}"
    );

    let all = s.list_all("tag=rr").await;
    assert_eq!(all.len(), 100, "exactly 50 new runs");
    let src: BTreeSet<String> = sources.iter().map(ToString::to_string).collect();
    let marker = format!("__bulk/{id}/");
    let mut lineage = BTreeSet::new();
    for r in all.iter().filter(|r| !src.contains(*r)) {
        let run: RunId = r.parse().expect("id");
        let rec = s
            .wait_record(&run, LIMIT, |rec| rec.status.is_terminal())
            .await;
        assert_eq!(rec.status, RunStatus::Completed, "rerun on v2 completes");
        let key = String::from_utf8_lossy(&rec.idempotency_key).into_owned();
        let retry_of = rec.retry_of.expect("retry_of").to_string();
        assert!(
            src.contains(&retry_of),
            "a rerun of a source, not of a rerun"
        );
        assert_eq!(key, format!("__rerun/{retry_of}/{marker}{retry_of}"));
        lineage.insert(retry_of);
    }
    assert_eq!(lineage, src, "one rerun per source");
}

#[test]
fn pacing_spreads_ops_with_the_phase7_gcra() {
    // rate 10/s, burst 2, 40 ops, logical time only.
    let clock = ManualClock::new(1_700_000_000_000);
    let mut pacer = Pacer::new(10, 2, Arc::new(clock.clone()));
    let t0 = clock.changed().borrow().to_owned();
    let mut starts = Vec::new();
    while starts.len() < 40 {
        match pacer.try_admit() {
            None => starts.push(clock.changed().borrow().to_owned() - t0),
            Some(d) => clock.advance(u64::try_from(d.as_millis()).expect("ms")),
        }
    }
    assert_eq!(&starts[..2], &[0, 0], "the burst starts at once");
    for w in starts[1..].windows(2) {
        assert!(
            w[1] - w[0] >= 100,
            "after the burst ≥ 1/rate apart: {starts:?}"
        );
    }
    assert!(starts[39] >= 3_800, "total {} ms", starts[39]);
    eprintln!(
        "pacing: 40 ops at 10/s burst 2 -> last start at {} ms",
        starts[39]
    );
}

#[test]
fn signal_request_ids_respect_the_cap() {
    let job = "blk_0190c0de-0000-7000-8000-000000000000";
    let run = "p0003-0190c0de-0000-7000-8000-000000000001";
    let id = signal_request_id(job, run);
    assert!(id.len() <= 64, "{id}");
    assert_eq!(id, signal_request_id(job, run), "deterministic");
    assert_ne!(
        id,
        signal_request_id(job, "p0003-0190c0de-0000-7000-8000-000000000002")
    );
    assert_eq!(signal_request_id("blk_x", "r1"), "blk_x/r1");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backpressure_pauses_until_the_backlog_drains() {
    let s = server_p(1, |b| b.backlog_high = 5).await;
    s.deploy_guest("p11-hook", None).await;
    let failed = failed_w(&s, 3, "bp", json!({})).await;
    let (st, v) = s.post_json("/v1/queues/held/pause", &Value::Null).await;
    assert!(st.is_success(), "{v}");
    for _ in 0..10 {
        s.start_rest("w", json!({"input": {}, "queue": "held"}))
            .await;
    }
    let id = create(&s, json!({"action": "retry", "filter": {"tags": ["bp"]}})).await;
    let v = wait_job(&s, &id, "paused_backpressure", |v| {
        v["state"] == "paused_backpressure"
    })
    .await;
    assert_eq!(n(&v, "done"), 0, "{v}");
    sleep(Duration::from_millis(500)).await;
    let v = job(&s, &id).await;
    assert_eq!(
        (v["state"].as_str(), n(&v, "done")),
        (Some("paused_backpressure"), 0)
    );
    assert_eq!(job_file(&s, &id)["state"], "paused_backpressure");

    let (st, v) = s.post_json("/v1/queues/held/resume", &Value::Null).await;
    assert!(st.is_success(), "{v}");
    let v = s.bulk_wait(&id, LIMIT).await;
    assert_eq!(
        (v["state"].as_str(), n(&v, "done")),
        (Some("done"), 3),
        "{v}"
    );
    let all = s.list_all("tag=bp").await;
    assert_eq!(
        all.len(),
        6,
        "one restart per failed run: {all:?} / {failed:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restarts_honour_429_retry_after() {
    let s = server(|_| {}).await;
    s.deploy_guest("p11-hook", None).await;
    failed_w(&s, 6, "rl", json!({"queue": "rl"})).await;
    let (st, v) = s
        .put_json(
            "/v1/queues/rl/flow",
            &json!({"rate_limit": {"limit": 2, "period_ms": 1000, "burst": 1}}),
        )
        .await;
    assert!(st.is_success(), "{v}");
    let t0 = Instant::now();
    let id = create(
        &s,
        json!({"action": "retry", "filter": {"tags": ["rl"]}, "rate_per_sec": 100}),
    )
    .await;
    let v = s.bulk_wait(&id, LIMIT).await;
    let took = t0.elapsed();
    assert_eq!(v["state"], "done", "{v}");
    assert_eq!((n(&v, "done"), n(&v, "failed_count")), (6, 0), "{v}");
    assert!(
        took >= Duration::from_millis(2_000),
        "6 starts at 2/s took {took:?}"
    );
    let m = scrape(&s).await;
    assert!(
        metric_at_least(
            &m,
            "ostro_bulk_ops_total{action=\"retry\",result=\"rate_limited\"}",
            1.0
        ),
        "{m}"
    );
    assert_eq!(
        s.list_all("tag=rl").await.len(),
        12,
        "exactly one restart each"
    );
    eprintln!("429 restarts: 6 runs at queue limit 2/s in {took:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn signal_uses_a_per_run_request_id() {
    let s = server(|_| {}).await;
    s.deploy_guest("signals", None).await;
    let runs = signal_runs(&s, 5, "fifo", "sg").await;
    let id = create(
        &s,
        json!({"action": {"signal": {"name": "x", "payload": 1}}, "filter": {"tags": ["sg"]}}),
    )
    .await;
    let v = s.bulk_wait(&id, LIMIT).await;
    assert_eq!((n(&v, "done"), n(&v, "skipped")), (5, 0), "{v}");
    for r in &runs {
        let rid = signal_request_id(&id, &r.to_string());
        assert!(rid.len() <= 64);
        let (st, b) = s
            .post_json(
                &format!("/v1/runs/{r}/signals/x"),
                &json!({"payload": 2, "request_id": rid}),
            )
            .await;
        assert!(st.is_success(), "{b}");
        assert_eq!(
            b["duplicate"], true,
            "the job already used this request id: {b}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn requeue_marks_and_pause_resume() {
    let s = server(|_| {}).await;
    s.deploy_guest("p11-hook", None).await;
    s.deploy_guest("signals", None).await;
    let failed = failed_w(&s, 3, "rq", json!({})).await;
    let id = create(
        &s,
        json!({"action": {"requeue": {"mode": "restart"}}, "filter": {"tags": ["rq"], "dlq": true}}),
    )
    .await;
    let v = s.bulk_wait(&id, LIMIT).await;
    assert_eq!((n(&v, "matched"), n(&v, "done")), (3, 3), "{v}");
    for r in &failed {
        let row = ostro_server::control::p11_dlq::dlq_row(&s.handle_of(r).reader(), r)
            .expect("dlq row")
            .expect("row written");
        assert!(row.requeued_to.is_some(), "Requeued mark on {r}");
    }
    // The sources left the DLQ (the restarts, same input, fail again and are members).
    let (st, v) = s.dlq_list("").await;
    assert_eq!(st, StatusCode::OK);
    let src: BTreeSet<String> = failed.iter().map(ToString::to_string).collect();
    assert!(
        v["entries"]
            .as_array()
            .expect("entries")
            .iter()
            .all(|e| !src.contains(e["run_id"].as_str().expect("id"))),
        "{v}"
    );

    let runs = signal_runs(&s, 3, "long_wait", "pr").await;
    let id = create(&s, json!({"action": "pause", "filter": {"tags": ["pr"]}})).await;
    let v = s.bulk_wait(&id, LIMIT).await;
    assert_eq!(n(&v, "done"), 3, "{v}");
    for r in &runs {
        let (_, b) = s.get_json(&format!("/v1/runs/{r}")).await;
        assert!(!b["pause"].is_null(), "paused: {b}");
    }
    let id = create(&s, json!({"action": "resume", "filter": {"tags": ["pr"]}})).await;
    let v = s.bulk_wait(&id, LIMIT).await;
    assert_eq!(n(&v, "done"), 3, "{v}");
    for r in &runs {
        let (_, b) = s.get_json(&format!("/v1/runs/{r}")).await;
        assert!(b["pause"].is_null(), "resumed: {b}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn job_and_match_limits_and_job_cancel() {
    let s = server(|b| {
        b.max_matches = 100;
        b.burst = 1;
    })
    .await;
    s.deploy_guest("p11-hook", None).await;
    for _ in 0..101 {
        s.start_rest("w", json!({"input": {}, "tags": ["mm"]}))
            .await;
    }
    for dry in [true, false] {
        let (st, v) = s
            .bulk_create(&json!({"action": "cancel", "filter": {"tags": ["mm"]}, "dry_run": dry}))
            .await;
        assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");
        assert_eq!(
            v["error"]["details"]["reason"], "bulk_too_many_matches",
            "{v}"
        );
        assert_eq!(v["error"]["details"]["max"], 100, "{v}");
    }
    s.deploy_guest("signals", None).await;
    signal_runs(&s, 10, "long_wait", "jb").await;
    let mut jobs = Vec::new();
    for _ in 0..4 {
        jobs.push(
            create(
                &s,
                json!({"action": "pause", "filter": {"tags": ["jb"]}, "rate_per_sec": 1, "max_in_flight": 1}),
            )
            .await,
        );
    }
    let (st, v) = s
        .bulk_create(&json!({"action": "pause", "filter": {"tags": ["jb"]}}))
        .await;
    assert_eq!(st, StatusCode::TOO_MANY_REQUESTS, "{v}");
    assert_eq!(v["error"]["code"], "rate_limited", "{v}");
    assert_eq!(v["error"]["details"]["reason"], "bulk_too_many_jobs", "{v}");
    assert!(
        v["error"]["details"]["retry_after_ms"].as_u64().is_some(),
        "{v}"
    );

    // eta while running; cancel stops within one op.
    let v = wait_job(&s, &jobs[0], "first op", |v| {
        n(v, "done") + n(v, "skipped") >= 1
    })
    .await;
    assert!(v["eta_ms"].as_u64().is_some_and(|e| e > 0), "{v}");
    for j in &jobs {
        let (st, v) = s
            .post_json(&format!("/v1/bulk/{j}/cancel"), &Value::Null)
            .await;
        assert_eq!(st, StatusCode::OK, "{v}");
        assert_eq!(v["state"], "cancelled", "{v}");
    }
    sleep(Duration::from_millis(300)).await;
    let before: Vec<u64> = futures_progress(&s, &jobs).await;
    sleep(Duration::from_millis(1_500)).await;
    let after: Vec<u64> = futures_progress(&s, &jobs).await;
    assert_eq!(before, after, "no op after cancel");
    assert!(after.iter().all(|p| *p < 10), "{after:?}");
    assert_eq!(job_file(&s, &jobs[0])["state"], "cancelled");
    // A fifth job fits now.
    let id = create(
        &s,
        json!({"action": "resume", "filter": {"tags": ["jb"]}, "rate_per_sec": 100}),
    )
    .await;
    s.bulk_wait(&id, LIMIT).await;
}

async fn futures_progress(s: &P11Server, jobs: &[String]) -> Vec<u64> {
    let mut out = Vec::new();
    for j in jobs {
        let v = job(s, j).await;
        out.push(n(&v, "done") + n(&v, "skipped") + n(&v, "failed_count"));
    }
    out
}

// ---------------------------------------------------------------------------------
// Task 2
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn checkpoints_are_atomic_and_follow_the_op_cadence() {
    let s = server(|b| {
        b.checkpoint_ops = 10;
        b.checkpoint_every = Duration::from_secs(3600).into();
        b.burst = 1;
    })
    .await;
    s.deploy_guest("p11-hook", None).await;
    let mut runs = Vec::new();
    for _ in 0..40 {
        runs.push(
            s.start_rest("w", json!({"input": {}, "tags": ["ck"]}))
                .await,
        );
    }
    for r in &runs {
        s.wait_record(r, LIMIT, |rec| rec.status.is_terminal())
            .await;
    }
    let id = create(
        &s,
        json!({"action": "pause", "filter": {"tags": ["ck"]}, "rate_per_sec": 20, "max_in_flight": 1}),
    )
    .await;
    // Written at creation (synchronously).
    let path = s.data_dir().join("bulk").join(format!("{id}.json"));
    assert!(path.exists(), "job file at creation");
    let mut seen = BTreeSet::new();
    let mut reads = 0u64;
    let deadline = Instant::now() + LIMIT;
    loop {
        let b = std::fs::read(&path).expect("never missing");
        let f: Value = serde_json::from_slice(&b).expect("never a partial file");
        reads += 1;
        assert_eq!(f["version"], 1);
        let progress = n(&f, "done") + n(&f, "skipped") + n(&f, "failed_count");
        if f["state"] == "done" {
            assert_eq!(progress, 40, "{f}");
            break;
        }
        seen.insert(progress);
        assert!(Instant::now() < deadline, "job not done");
        tokio::task::yield_now().await;
    }
    assert!(
        seen.iter().all(|p| p % 10 == 0),
        "running checkpoints every 10 ops: {seen:?}"
    );
    assert!(seen.len() >= 3, "{seen:?}");
    eprintln!("checkpoint reads: {reads}, running progress seen: {seen:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn processed_set_survives_restart_and_finished_jobs_stay_listed() {
    let mut s = server(|b| {
        b.checkpoint_ops = 1;
        b.burst = 1;
    })
    .await;
    s.deploy_guest("signals", None).await;
    signal_runs(&s, 20, "fifo", "ps").await;
    let id = create(
        &s,
        json!({"action": {"signal": {"name": "x", "payload": 7}}, "filter": {"tags": ["ps"]},
               "rate_per_sec": 4, "max_in_flight": 1}),
    )
    .await;
    wait_job(&s, &id, "6 ops", |v| n(v, "done") >= 6).await;
    let dir = s.data_dir().join("bulk");
    s.shutdown().await;

    // Re-deliver every run: the cursor restarts from the beginning.
    let path = dir.join(format!("{id}.json"));
    let mut f: Value = serde_json::from_slice(&std::fs::read(&path).expect("read")).expect("json");
    assert_eq!(f["state"], "running", "{f}");
    let processed = f["processed"].as_array().expect("processed").len();
    assert!(processed >= 6, "{f}");
    f["cursor"] = Value::Null;
    f["selection_done"] = json!(false);
    f["pending_in_page"] = json!([]);
    std::fs::write(&path, serde_json::to_vec(&f).expect("enc")).expect("write");
    // A file of another version is ignored (WARN).
    let mut bogus = f.clone();
    bogus["version"] = json!(2);
    bogus["job_id"] = json!("blk_bogus");
    std::fs::write(
        dir.join("blk_bogus.json"),
        serde_json::to_vec(&bogus).expect("enc"),
    )
    .expect("write");

    s.restart().await;
    s.wait_ready(Duration::from_secs(20)).await;
    let v = s.bulk_wait(&id, LIMIT).await;
    assert_eq!(v["state"], "done", "{v}");
    assert_eq!(
        (n(&v, "done"), n(&v, "skipped"), n(&v, "failed_count")),
        (20, 0, 0),
        "processed runs were not operated on again: {v}"
    );

    s.restart().await;
    s.wait_ready(Duration::from_secs(20)).await;
    // 11-18: the bulk API waits for the service's reload of `<data>/bulk`, so the first
    // listing right after readiness already has the job (no polling).
    let (_, l) = s.get_json("/v1/bulk").await;
    assert!(s.h().control().p11().expect("p11").bulk.is_loaded());
    let jobs = l["jobs"].as_array().expect("jobs");
    assert_eq!(jobs.len(), 1, "{l}");
    assert_eq!(jobs[0]["job_id"], id.as_str());
    assert_eq!(jobs[0]["state"], "done");
    assert_eq!(jobs[0]["done"], 20);
}

// ---------------------------------------------------------------------------------
// Crash resume (feature `failpoints`, `--test-threads=1`)
// ---------------------------------------------------------------------------------

#[cfg(feature = "failpoints")]
async fn wait_stalled(s: &P11Server, id: &str, at_least: u64) -> u64 {
    let deadline = Instant::now() + LIMIT;
    let mut last = (u64::MAX, Instant::now());
    loop {
        let v = job(s, id).await;
        let p = n(&v, "done") + n(&v, "skipped") + n(&v, "failed_count");
        if p != last.0 {
            last = (p, Instant::now());
        } else if p >= at_least && last.1.elapsed() > Duration::from_millis(500) {
            return p;
        }
        assert!(Instant::now() < deadline, "job {id} did not stall: {v}");
        sleep(Duration::from_millis(20)).await;
    }
}

#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_after_op_resumes_from_the_cursor_without_duplicates() {
    use ostro_types::journal::EventKind;
    let scenario = fail::FailScenario::setup();
    let mut s = server(|b| {
        b.checkpoint_ops = 10;
        b.burst = 1;
    })
    .await;
    s.deploy_guest("signals", None).await;
    let runs = signal_runs(&s, 60, "long_wait", "ca").await;
    fail::cfg("bulk::after_op", "25*off->return").expect("cfg");
    let t0 = Instant::now();
    let id = create(
        &s,
        json!({"action": "cancel", "filter": {"tags": ["ca"]}, "rate_per_sec": 200, "max_in_flight": 1}),
    )
    .await;
    let stalled = wait_stalled(&s, &id, 26).await;
    let crash_phase = t0.elapsed();
    let on_disk = job_file(&s, &id);
    assert_eq!(on_disk["state"], "running");
    let ckpt = n(&on_disk, "done") + n(&on_disk, "skipped");
    assert!(
        ckpt < stalled,
        "the crash lost the progress after the last checkpoint"
    );
    fail::remove("bulk::after_op");
    let t1 = Instant::now();
    s.restart().await;
    s.wait_ready(Duration::from_secs(20)).await;
    let v = s.bulk_wait(&id, LIMIT).await;
    let resume_phase = t1.elapsed();
    assert_eq!(v["state"], "done", "{v}");
    assert_eq!(n(&v, "done") + n(&v, "skipped"), 60, "{v}");
    assert_eq!(n(&v, "failed_count"), 0, "{v}");
    for r in &runs {
        s.wait_record(r, LIMIT, |rec| rec.status == RunStatus::Cancelled)
            .await;
        let j = s.journal(r).await;
        assert_eq!(
            common::count(&j, EventKind::CancelRequested),
            1,
            "cancel journaled once for {r}"
        );
    }
    eprintln!(
        "crash after op 26 (checkpoint at {ckpt}): crash phase {crash_phase:?}, restart to done {resume_phase:?}"
    );
    scenario.teardown();
}

#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_before_checkpoint_keeps_one_restart_per_run() {
    let scenario = fail::FailScenario::setup();
    let mut s = server(|b| {
        b.checkpoint_ops = 3;
        b.burst = 1;
    })
    .await;
    s.deploy_guest("p11-hook", None).await;
    let sources = failed_w(&s, 12, "cb", json!({})).await;
    // Evaluation 1 is the runner's start checkpoint; the first op checkpoint crashes.
    fail::cfg("bulk::before_checkpoint", "1*off->return").expect("cfg");
    let t0 = Instant::now();
    let id = create(
        &s,
        json!({"action": "retry", "filter": {"tags": ["cb"]}, "rate_per_sec": 100, "max_in_flight": 1}),
    )
    .await;
    let stalled = wait_stalled(&s, &id, 3).await;
    let crash_phase = t0.elapsed();
    assert_eq!(
        n(&job_file(&s, &id), "done"),
        0,
        "nothing checkpointed after the start"
    );
    fail::remove("bulk::before_checkpoint");
    let t1 = Instant::now();
    s.restart().await;
    s.wait_ready(Duration::from_secs(20)).await;
    let v = s.bulk_wait(&id, LIMIT).await;
    let resume_phase = t1.elapsed();
    assert_eq!(
        (v["state"].as_str(), n(&v, "done")),
        (Some("done"), 12),
        "{v}"
    );
    let mut by_source: std::collections::BTreeMap<String, usize> = Default::default();
    for r in s.list_all("tag=cb").await {
        let run: RunId = r.parse().expect("id");
        if let Some(src) = s.run_record(&run).retry_of {
            assert_eq!(
                String::from_utf8_lossy(&s.run_record(&run).idempotency_key),
                format!("__bulk/{id}/{src}")
            );
            *by_source.entry(src.to_string()).or_default() += 1;
        }
    }
    let want: std::collections::BTreeMap<String, usize> =
        sources.iter().map(|r| (r.to_string(), 1)).collect();
    assert_eq!(by_source, want, "exactly one restart per source run");
    eprintln!(
        "crash before checkpoint after {stalled} ops: crash phase {crash_phase:?}, restart to done {resume_phase:?}"
    );
    scenario.teardown();
}
