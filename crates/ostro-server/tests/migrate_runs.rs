//! `POST /v1/runs/migrate` (13-17; CLUS-07, D-P13-21) on the real single-node server with
//! `guests/p10-replay-v{1,2,3}` (`replay_demo`: v1 steps `a`, `b`, then a sleep; v2 swaps
//! the steps, so a v1 journal past the first step diverges; v3 swaps them behind
//! `patched("reorder")`, so v1 journals replay clean):
//!
//! - dry run against v3: every sleeping run passed, nothing submitted (pins unchanged);
//!   the real run repins all of them, and they complete on v3;
//! - against v2: runs past the first step fail with the divergence seq and keep v1; a run
//!   still waiting for its start delay (journal = `RunStarted`) passes and is repinned;
//! - `limit` split over the partitions, `filter` by tag;
//! - a journal that grows between the replay and the submit is `seq_mismatch`; a second
//!   migrate repins it; failpoints build: under `migrate::mutant::skip_seq_check` the grown
//!   journal is repinned (the mutant bites);
//! - a hot-parked run (`[engine] hot_wait` longer than its sleep) is `busy`
//!   (`hot_parked_run_is_busy`);
//! - an emulated level-1 node answers 409 `feature_not_enabled` for a submitting migrate,
//!   while a dry run works; a retired target is 409 `module_retired`; unknown versions 404.
//!
//! The 3-node in-process cases (any receiving node, followers, the report identical at
//! every node) are the tier-B scenarios in `ostro-repl-conformance/tests/migrate.rs`.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{ServerOpts, TestServer, server_config, tmpfs_dir};
use ostro_partition::store::values::RunStatus;
use ostro_server::control::migrate::{BeforeSubmitHook, limit_share, set_before_submit_hook};
use ostro_types::journal::{EventBody, EventKind};
use ostro_types::{PartitionCount, PartitionId, RunId};
use reqwest::StatusCode;
use serde_json::{Value, json};

const LIMIT: Duration = Duration::from_secs(90);

/// The before-submit hook is process-wide: tests that install it run one at a time.
static HOOK_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn hex(sha: &[u8; 32]) -> String {
    sha.iter().map(|b| format!("{b:02x}")).collect()
}

fn reason(v: &Value) -> &str {
    v["error"]["details"]["reason"].as_str().unwrap_or("")
}

async fn server(hot_wait: Duration) -> TestServer {
    let s = TestServer::start_with(ServerOpts::default(), |c| {
        c.engine.hot_wait = hot_wait.into();
    })
    .await;
    s.wait_ready(Duration::from_secs(30)).await;
    s
}

/// `(v1, v2, v3)` shas; v1 is active.
async fn deploy(s: &TestServer) -> ([u8; 32], [u8; 32], [u8; 32]) {
    let v1 = s.deploy_guest("p10-replay-v1", None).await;
    let v2 = s.deploy_guest("p10-replay-v2", None).await;
    let v3 = s.deploy_guest("p10-replay-v3", None).await;
    (v1, v2, v3)
}

async fn migrate(s: &TestServer, body: Value) -> (StatusCode, Value) {
    s.post_json("/v1/runs/migrate", &body).await
}

/// A submitting migrate, retried while the single node has not finalized level 2 yet
/// (it finalizes once its NodeVersion is advertised).
async fn migrate_ok(s: &TestServer, body: Value) -> Value {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let (st, v) = migrate(s, body.clone()).await;
        if st == StatusCode::OK {
            return v;
        }
        assert!(
            reason(&v) == "feature_not_enabled" && Instant::now() < deadline,
            "migrate {body}: {st} {v}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn ids(v: &Value, field: &str) -> BTreeSet<String> {
    v[field]
        .as_array()
        .unwrap_or_else(|| panic!("{field}: {v}"))
        .iter()
        .map(|x| {
            x.as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| x["run_id"].as_str().expect("run_id").to_owned())
        })
        .collect()
}

fn set(runs: &[RunId]) -> BTreeSet<String> {
    runs.iter().map(RunId::to_string).collect()
}

/// A run sleeping `sleep_s` after both steps (suspended once its timer is journaled).
async fn sleeping(s: &TestServer, sleep_s: u64, tags: &[&str]) -> RunId {
    let run = s
        .start_rest(
            "replay_demo",
            json!({"input": {"sleep_s": sleep_s}, "tags": tags}),
        )
        .await;
    let deadline = Instant::now() + LIMIT;
    while !s
        .history_kinds(&run)
        .await
        .contains(&EventKind::TimerStarted)
    {
        assert!(Instant::now() < deadline, "run {run} never slept");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    run
}

async fn suspended(s: &TestServer, run: &RunId) {
    s.wait_status(run, RunStatus::Scheduled, LIMIT).await;
}

async fn first_step_seq(s: &TestServer, run: &RunId) -> u64 {
    s.journal(run)
        .await
        .iter()
        .find(|e| matches!(e.body, EventBody::StepCompleted(_)))
        .map(|e| e.seq)
        .expect("a StepCompleted")
}

fn body(from: u32, to: u32, dry_run: bool) -> Value {
    json!({"workflow": "replay_demo", "from": {"version": from}, "to_version": to,
           "dry_run": dry_run})
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn passing_runs_repinned_then_complete_on_the_new_version() {
    let s = server(Duration::from_millis(50)).await;
    let (v1, _v2, v3) = deploy(&s).await;
    let mut runs = Vec::new();
    for _ in 0..3 {
        runs.push(sleeping(&s, 25, &[]).await);
    }
    for r in &runs {
        suspended(&s, r).await;
    }

    // Dry run: every run passes, nothing is submitted.
    let (st, dry) = migrate(&s, body(1, 3, true)).await;
    assert_eq!(st, StatusCode::OK, "{dry}");
    assert_eq!(ids(&dry, "passed"), set(&runs), "{dry}");
    for f in [
        "repinned",
        "failed",
        "busy",
        "seq_mismatch",
        "already_on_target",
    ] {
        assert!(ids(&dry, f).is_empty(), "{f}: {dry}");
    }
    for r in &runs {
        assert_eq!(
            s.run_record(r).module_sha,
            Some(v1),
            "dry run changed a pin"
        );
    }
    // Same answer again (deterministic selection and verdicts).
    let (_, dry2) = migrate(&s, body(1, 3, true)).await;
    assert_eq!(dry, dry2);

    // Real run.
    let rep = migrate_ok(&s, body(1, 3, false)).await;
    assert_eq!(ids(&rep, "passed"), set(&runs), "{rep}");
    assert_eq!(ids(&rep, "repinned"), set(&runs), "{rep}");
    for r in &runs {
        assert_eq!(s.run_record(r).module_sha, Some(v3), "{r} repinned");
    }
    // A second migrate from v1 submits nothing: the runs are already on v3 and the report
    // is the same complete set (p13-follow: idempotent report).
    let again = migrate_ok(&s, body(1, 3, false)).await;
    assert_eq!(ids(&again, "already_on_target"), set(&runs), "{again}");
    assert_eq!(ids(&again, "passed"), set(&runs), "{again}");
    assert_eq!(ids(&again, "repinned"), set(&runs), "{again}");
    // They wake on v3 and complete (v3 keeps the old order for an unpatched journal).
    for r in &runs {
        let rec = s.wait_status(r, RunStatus::Completed, LIMIT).await;
        assert_eq!(rec.module_sha, Some(v3));
        let out: Value =
            serde_json::from_slice(&s.result_bytes(r).await.expect("result")).expect("json result");
        assert_eq!(out, json!(["a", "b"]), "{r}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn diverging_runs_never_repinned() {
    let s = server(Duration::from_millis(50)).await;
    let (v1, v2, _v3) = deploy(&s).await;
    let mut diverging = Vec::new();
    for _ in 0..2 {
        diverging.push(sleeping(&s, 600, &[]).await);
    }
    for r in &diverging {
        suspended(&s, r).await;
    }
    // Waiting for a 10 min start delay: the journal is only RunStarted, which v2 replays.
    let delayed = s
        .start_rest(
            "replay_demo",
            json!({"input": {"sleep_s": 1}, "delay_ms": 600_000}),
        )
        .await;
    assert_eq!(s.run_record(&delayed).next_seq, 1);

    let (st, dry) = migrate(&s, body(1, 2, true)).await;
    assert_eq!(st, StatusCode::OK, "{dry}");
    assert_eq!(ids(&dry, "passed"), set(&[delayed]), "{dry}");
    assert_eq!(ids(&dry, "failed"), set(&diverging), "{dry}");
    for f in dry["failed"].as_array().expect("failed") {
        let run: RunId = f["run_id"].as_str().expect("id").parse().expect("run id");
        assert_eq!(f["seq"], first_step_seq(&s, &run).await, "{f}");
        assert!(!f["detail"].as_str().unwrap_or("").is_empty(), "{f}");
    }

    let rep = migrate_ok(&s, body(1, 2, false)).await;
    assert_eq!(ids(&rep, "repinned"), set(&[delayed]), "{rep}");
    assert_eq!(ids(&rep, "failed"), set(&diverging), "{rep}");
    assert_eq!(s.run_record(&delayed).module_sha, Some(v2));
    for r in &diverging {
        assert_eq!(
            s.run_record(r).module_sha,
            Some(v1),
            "diverging run kept v1"
        );
    }
}

/// p13-follow: a migrate retried after a partial success (a leg answered 503 after other
/// legs repinned) reports the complete set. Here the first call moves part of the runs (a
/// `filter`); the same call again reports them as `already_on_target` (also in `passed` /
/// `repinned`) and submits nothing; the unfiltered call reports them next to the newly
/// repinned ones; a dry run lists them in `passed` and `already_on_target` but never in
/// `repinned`. A run started directly on the target version is never reported (it was not
/// migrated), nor is a moved run outside the filter.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retried_migrate_reports_runs_already_on_the_target() {
    let s = server(Duration::from_millis(50)).await;
    let (_v1, _v2, v3) = deploy(&s).await;
    let mut first = Vec::new();
    for _ in 0..2 {
        first.push(sleeping(&s, 600, &["first"]).await);
    }
    let mut rest = Vec::new();
    for _ in 0..2 {
        rest.push(sleeping(&s, 600, &[]).await);
    }
    let native = s
        .start_rest(
            "replay_demo",
            json!({"input": {"sleep_s": 600}, "version": 3}),
        )
        .await;
    for r in first.iter().chain(&rest) {
        suspended(&s, r).await;
    }
    assert_eq!(s.run_record(&native).module_sha, Some(v3), "started on v3");

    let mut filtered = body(1, 3, false);
    filtered["filter"] = json!("first");
    let rep = migrate_ok(&s, filtered.clone()).await;
    assert_eq!(ids(&rep, "repinned"), set(&first), "{rep}");
    assert!(ids(&rep, "already_on_target").is_empty(), "{rep}");

    // The same call again (a retry): the same report, nothing submitted.
    let again = migrate_ok(&s, filtered.clone()).await;
    assert_eq!(ids(&again, "repinned"), set(&first), "{again}");
    assert_eq!(ids(&again, "passed"), set(&first), "{again}");
    assert_eq!(ids(&again, "already_on_target"), set(&first), "{again}");

    // Dry run of the whole migration: the moved runs are passed + already on target.
    let all: Vec<RunId> = first.iter().chain(&rest).copied().collect();
    let (st, dry) = migrate(&s, body(1, 3, true)).await;
    assert_eq!(st, StatusCode::OK, "{dry}");
    assert_eq!(ids(&dry, "passed"), set(&all), "{dry}");
    assert_eq!(ids(&dry, "already_on_target"), set(&first), "{dry}");
    assert!(ids(&dry, "repinned").is_empty(), "{dry}");

    // The whole migration: the complete set, `native` never listed.
    let full = migrate_ok(&s, body(1, 3, false)).await;
    assert_eq!(ids(&full, "passed"), set(&all), "{full}");
    assert_eq!(ids(&full, "repinned"), set(&all), "{full}");
    assert_eq!(ids(&full, "already_on_target"), set(&first), "{full}");
    assert!(ids(&full, "failed").is_empty(), "{full}");
    for r in &all {
        assert_eq!(s.run_record(r).module_sha, Some(v3), "{r} on v3");
    }

    // A moved run outside the filter is not listed.
    let only_first = migrate_ok(&s, filtered).await;
    assert_eq!(ids(&only_first, "repinned"), set(&first), "{only_first}");
    assert_eq!(ids(&only_first, "already_on_target"), set(&first), "{only_first}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn filter_and_limit() {
    let s = server(Duration::from_millis(50)).await;
    let (_v1, _v2, _v3) = deploy(&s).await;
    let mut tagged = Vec::new();
    for _ in 0..2 {
        tagged.push(sleeping(&s, 600, &["batch-a", "x"]).await);
    }
    let other = sleeping(&s, 600, &["batch-b"]).await;
    for r in tagged.iter().chain([&other]) {
        suspended(&s, r).await;
    }
    let mut b = body(1, 3, true);
    b["filter"] = json!("batch-a,x");
    let (st, v) = migrate(&s, b).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(ids(&v, "passed"), set(&tagged), "{v}");

    // limit 1 over 4 partitions: only partition 0 has a share.
    let count = PartitionCount::new(4).expect("count");
    assert_eq!(limit_share(Some(1), PartitionId(0), count), 1);
    assert_eq!(limit_share(Some(1), PartitionId(1), count), 0);
    assert_eq!(limit_share(Some(6), PartitionId(1), count), 2);
    assert_eq!(limit_share(Some(6), PartitionId(2), count), 1);
    assert_eq!(limit_share(None, PartitionId(3), count), usize::MAX);
    let mut b = body(1, 3, true);
    b["limit"] = json!(4);
    let (_, v) = migrate(&s, b).await;
    let all = set(&[tagged[0], tagged[1], other]);
    let mut per_p = [0usize; 4];
    for r in &all {
        let run: RunId = r.parse().expect("id");
        per_p[usize::from(run.partition().0)] += 1;
    }
    // limit 4 over 4 partitions: at most one run per partition.
    let want: usize = per_p.iter().map(|n| (*n).min(1)).sum();
    assert_eq!(ids(&v, "passed").len(), want, "{v}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_growth_is_seq_mismatch_then_repinned() {
    let _l = HOOK_LOCK.lock().await;
    let s = Arc::new(server(Duration::from_millis(50)).await);
    let (v1, _v2, v3) = deploy(&s).await;
    let run = sleeping(&s, 600, &[]).await;
    suspended(&s, &run).await;
    install_signal_hook(&s, run);
    let rep = migrate_ok(&s, body(1, 3, false)).await;
    set_before_submit_hook(None);
    assert_eq!(ids(&rep, "passed"), set(&[run]), "{rep}");
    assert_eq!(ids(&rep, "seq_mismatch"), set(&[run]), "{rep}");
    assert!(ids(&rep, "repinned").is_empty(), "{rep}");
    assert_eq!(s.run_record(&run).module_sha, Some(v1));
    // Dry-run again / migrate again: the grown journal replays and the run is repinned.
    let rep = repin_eventually(&s, &run).await;
    assert_eq!(ids(&rep, "repinned"), set(&[run]), "{rep}");
    assert_eq!(s.run_record(&run).module_sha, Some(v3));
}

/// Migrate until `run` is repinned (it may be busy while it consumes the signal).
async fn repin_eventually(s: &TestServer, run: &RunId) -> Value {
    let deadline = Instant::now() + LIMIT;
    loop {
        let rep = migrate_ok(s, body(1, 3, false)).await;
        if ids(&rep, "repinned").contains(&run.to_string()) {
            return rep;
        }
        assert!(Instant::now() < deadline, "never repinned: {rep}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Before the submit: signal `run` and wait until its journal grew and it is suspended.
fn install_signal_hook(s: &Arc<TestServer>, run: RunId) {
    let s2 = Arc::clone(s);
    let hook: BeforeSubmitHook = Arc::new(move |passed: Vec<String>| {
        let s = Arc::clone(&s2);
        Box::pin(async move {
            if !passed.contains(&run.to_string()) {
                return;
            }
            let before = s.run_record(&run).next_seq;
            let (st, v) = s
                .post_json(
                    &format!("/v1/runs/{run}/signals/poke"),
                    &json!({"payload": 1}),
                )
                .await;
            assert_eq!(st, StatusCode::OK, "signal: {v}");
            // Grown, and not leased (a woken run suspends again after the hot wait), so
            // the submit meets the seq check and nothing else.
            s.wait_record(&run, LIMIT, |r| {
                r.next_seq > before && r.status == RunStatus::Scheduled
            })
            .await;
        })
    });
    set_before_submit_hook(Some(hook));
}

/// Plan check W15 on the server: a run parked in the engine's hot wait keeps its lease, so
/// its repin is `busy`; it stays on v1.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hot_parked_run_is_busy() {
    let s = server(Duration::from_secs(60)).await;
    let (v1, _v2, _v3) = deploy(&s).await;
    let run = sleeping(&s, 30, &[]).await;
    // Parked in process: still Running with its lease row.
    let rec = s.run_record(&run);
    assert_eq!(rec.status, RunStatus::Running, "hot-parked");
    let rep = migrate_ok(&s, body(1, 3, false)).await;
    assert_eq!(ids(&rep, "passed"), set(&[run]), "{rep}");
    assert_eq!(ids(&rep, "busy"), set(&[run]), "{rep}");
    assert_eq!(s.run_record(&run).module_sha, Some(v1));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refusals() {
    let s = server(Duration::from_millis(50)).await;
    let (_v1, _v2, v3) = deploy(&s).await;
    // Unknown versions.
    let (st, v) = migrate(&s, body(9, 3, true)).await;
    assert_eq!(st, StatusCode::NOT_FOUND, "{v}");
    let (st, v) = migrate(&s, body(1, 9, true)).await;
    assert_eq!(st, StatusCode::NOT_FOUND, "{v}");
    // Same module.
    let (st, v) = migrate(&s, body(3, 3, true)).await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");
    // Malformed source sha / zero limit (adapter checks).
    let (st, _) = migrate(
        &s,
        json!({"workflow": "replay_demo", "from": {"sha": "zz"}, "to_version": 3}),
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    // A retired target: refused for dry runs and real runs.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let r = s
            .http
            .delete(s.url(&format!("/v1/modules/{}", hex(&v3))))
            .send()
            .await
            .expect("DELETE");
        if r.status().is_success() {
            break;
        }
        let v: Value = r.json().await.unwrap_or(Value::Null);
        assert!(
            reason(&v) == "feature_not_enabled" && Instant::now() < deadline,
            "retire v3: {v}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    for dry in [true, false] {
        let (st, v) = migrate(&s, body(1, 3, dry)).await;
        assert_eq!(st, StatusCode::CONFLICT, "{v}");
        assert_eq!(reason(&v), "module_retired", "{v}");
    }
}

/// An emulated level-1 node (finalized at 1): a submitting migrate is 409
/// `feature_not_enabled` and submits nothing; the dry run works.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn level_1_refuses_submission_dry_run_works() {
    let dir = tmpfs_dir();
    let mut cfg = server_config(dir.path(), &ServerOpts::default());
    cfg.engine.hot_wait = Duration::from_millis(50).into();
    let h = tokio::time::timeout(
        common::T,
        ostro_server::server::start_with_options(
            cfg.clone(),
            ostro_server::ServerOptions::default().with_level_max_override(Some(1)),
        ),
    )
    .await
    .expect("start timed out")
    .expect("start");
    let s = TestServer {
        handle: Some(h),
        cfg,
        hooks: Arc::default(),
        http: reqwest::Client::new(),
        dir: Some(dir),
    };
    s.wait_ready(Duration::from_secs(30)).await;
    let (v1, _v2, _v3) = deploy(&s).await;
    let run = sleeping(&s, 600, &[]).await;
    suspended(&s, &run).await;
    let (st, v) = migrate(&s, body(1, 3, false)).await;
    assert_eq!(st, StatusCode::CONFLICT, "{v}");
    assert_eq!(reason(&v), "feature_not_enabled", "{v}");
    assert_eq!(v["error"]["details"]["feature"], "repin_run", "{v}");
    assert_eq!(s.run_record(&run).module_sha, Some(v1));
    let (st, v) = migrate(&s, body(1, 3, true)).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(ids(&v, "passed"), set(&[run]), "{v}");
}

#[cfg(feature = "failpoints")]
mod failpoints {
    use super::*;
    use ostro_server::control::migrate::MUTANT_SKIP_SEQ_CHECK;

    struct Cfg(&'static str);
    impl Cfg {
        fn set(name: &'static str) -> Self {
            fail::cfg(name, "return").expect("fail cfg");
            Self(name)
        }
    }
    impl Drop for Cfg {
        fn drop(&mut self) {
            fail::remove(self.0);
        }
    }

    /// Under `migrate::mutant::skip_seq_check` the journal that grew after the replay is
    /// repinned anyway (the compare-and-set is defeated): the mutant bites.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn skip_seq_check_mutant_bites() {
        let _l = HOOK_LOCK.lock().await;
        let s = Arc::new(server(Duration::from_millis(50)).await);
        let (_v1, _v2, v3) = deploy(&s).await;
        let run = sleeping(&s, 600, &[]).await;
        suspended(&s, &run).await;
        install_signal_hook(&s, run);
        let rep = {
            let _m = Cfg::set(MUTANT_SKIP_SEQ_CHECK);
            migrate_ok(&s, body(1, 3, false)).await
        };
        set_before_submit_hook(None);
        assert!(
            ids(&rep, "seq_mismatch").is_empty(),
            "mutant: no seq_mismatch: {rep}"
        );
        assert_eq!(ids(&rep, "repinned"), set(&[run]), "mutant bites: {rep}");
        assert_eq!(s.run_record(&run).module_sha, Some(v3));
    }
}
