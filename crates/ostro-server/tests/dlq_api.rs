//! DLQ server side (plan 11-12, OPS-04, D-P11-11/12) with the `guests/p11-hook` component:
//!
//! - the view (`GET /v1/dlq`): guest failure, run timeout and nondeterminism listed newest
//!   first with reasons, `resumable` and tags; reason / tag / workflow filters; pagination;
//! - dismiss (`dlq_not_member` for a non-member) and requeue (resume, restart with dedupe
//!   `__dlq/<run>/1` and an idempotent re-issue, rerun delegation), and episodes (a
//!   dismissed run that is resumed and fails again is back);
//! - failure policies in meta ext tags 144/145 (unknown tags preserved, RunPolicy unchanged);
//! - the `on_failure` hook service: exactly one hook per failure episode with the
//!   documented input, the loop guard, a disabled queue, catch-up after a restart (window)
//!   and while the index is rebuilding (scan fallback), metrics, and (feature `failpoints`,
//!   `--test-threads=1`) a crash between the hook start and `HookStarted`.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;
#[path = "common/p11.rs"]
mod p11;

use std::sync::Arc;
use std::time::Duration;

use ostro_meta::ext::{ext_get, ext_put};
use ostro_meta::{MetaKey, MetaRecord};
use ostro_partition::store::values::{DlqRow, RunRecord, RunStatus};
use ostro_server::control::p11_dlq::dlq_row;
use ostro_server::onfailure::{EXT_DLQ_POLICY, EXT_ON_FAILURE};
use ostro_server::services::dlq_hook;
use ostro_types::RunId;
use p11::P11Server;
use reqwest::StatusCode;
use serde_json::{Value, json};
use tokio::time::{Instant, sleep};
use tokio_util::sync::CancellationToken;

const LIMIT: Duration = Duration::from_secs(60);

async fn server_with(
    f: impl FnOnce(&mut ostro_server::config::ServerConfig) + Send + 'static,
) -> P11Server {
    let s = P11Server::builder()
        .with_config(|c| {
            // Sleeps evict quickly, so the nondeterminism replay happens promptly.
            c.engine.hot_wait = Duration::from_millis(50).into();
            f(c);
        })
        .start()
        .await;
    s.wait_ready(Duration::from_secs(20)).await;
    s.deploy_guest("p11-hook", None).await;
    s
}

async fn server() -> P11Server {
    server_with(|_| {}).await
}

async fn terminal(s: &P11Server, run: &RunId) -> RunRecord {
    s.wait_record(run, LIMIT, |r| r.status.is_terminal()).await
}

/// A failed `w` run (guest step error with `msg`).
async fn fail_w(s: &P11Server, msg: &str, extra: Value) -> RunId {
    let mut body = json!({"input": {"fail": true, "msg": msg}});
    if let (Value::Object(b), Value::Object(e)) = (&mut body, extra) {
        b.extend(e);
    }
    let run = s.start_rest("w", body).await;
    let rec = terminal(s, &run).await;
    assert_eq!(rec.status, RunStatus::Failed, "w fails");
    run
}

async fn timed_out(s: &P11Server) -> RunId {
    let run = s.start_rest("w_slow", json!({"input": null})).await;
    let rec = terminal(s, &run).await;
    assert_eq!(rec.status, RunStatus::TimedOut);
    run
}

/// A nondeterminism failure: `patch-v1` journals step A and a sleep; the replay on
/// `patch-v3` (module override) diverges. The override stays set (resumes fail again)
/// until [`fix`].
async fn nondet(s: &P11Server) -> RunId {
    s.deploy_guest("patch-v1", None).await;
    let v3 = s.deploy_guest("patch-v3", Some("beta")).await;
    let run = s.start_rest("patch-demo", json!({"input": null})).await;
    s.wait_record(&run, LIMIT, |r| {
        r.next_seq > 0 && r.status == RunStatus::Scheduled
    })
    .await;
    s.hooks
        .module_override
        .set(Some(Arc::new(move |r: &RunId| (*r == run).then_some(v3))));
    let rec = terminal(s, &run).await;
    assert_eq!(rec.status, RunStatus::Failed, "the v3 replay must diverge");
    assert!(rec.resumable);
    run
}

fn fix(s: &P11Server) {
    s.hooks.module_override.set(None);
}

fn row(s: &P11Server, run: &RunId) -> Option<DlqRow> {
    dlq_row(&s.handle_of(run).reader(), run).expect("dlq row")
}

fn ids(page: &Value) -> Vec<String> {
    page["entries"]
        .as_array()
        .expect("entries")
        .iter()
        .map(|e| e["run_id"].as_str().expect("id").to_owned())
        .collect()
}

fn entry<'a>(page: &'a Value, run: &RunId) -> Option<&'a Value> {
    page["entries"]
        .as_array()
        .expect("entries")
        .iter()
        .find(|e| e["run_id"] == run.to_string())
}

async fn dlq(s: &P11Server, q: &str) -> Value {
    let (st, v) = s.dlq_list(q).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    v
}

async fn put_failure_policy(s: &P11Server, scope: &str, name: &str, body: Value) -> Value {
    let r = s
        .http
        .put(s.url(&format!("/v1/{scope}/{name}/failure-policy")))
        .json(&body)
        .send()
        .await
        .expect("PUT");
    let st = r.status();
    let v: Value = r.json().await.expect("json");
    assert_eq!(st, StatusCode::OK, "{v}");
    v
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
// Task 1: view, dismiss, requeue, policy
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn view_lists_failures_newest_first_with_reasons_and_filters() {
    let s = server().await;
    let a = fail_w(&s, "guest says no", json!({"tags": ["blue"]})).await;
    let b = timed_out(&s).await;
    let c = nondet(&s).await;
    fix(&s);
    // Completed runs are never members.
    let ok = s.start_rest("w", json!({"input": {}})).await;
    assert_eq!(terminal(&s, &ok).await.status, RunStatus::Completed);

    let page = dlq(&s, "").await;
    assert_eq!(
        ids(&page),
        vec![c.to_string(), b.to_string(), a.to_string()],
        "newest first: {page}"
    );
    let ea = entry(&page, &a).expect("a");
    assert_eq!(ea["reason"], "workflow_failed: guest says no", "{ea}");
    assert_eq!(ea["resumable"], false);
    assert_eq!(ea["status"], "failed");
    assert_eq!(ea["workflow"], "w");
    assert_eq!(ea["tags"], json!(["blue"]));
    assert!(ea["failed_at_ms"].as_u64().expect("ms") > 0);
    let eb = entry(&page, &b).expect("b");
    assert_eq!(eb["reason"], "run_timeout", "{eb}");
    assert_eq!(eb["status"], "timed_out");
    assert_eq!(eb["resumable"], true);
    let ec = entry(&page, &c).expect("c");
    assert!(
        ec["reason"]
            .as_str()
            .expect("reason")
            .starts_with("nondeterminism"),
        "{ec}"
    );
    assert_eq!(ec["resumable"], true);

    assert_eq!(
        ids(&dlq(&s, "reason=nondeterminism").await),
        vec![c.to_string()]
    );
    assert_eq!(ids(&dlq(&s, "tags=blue").await), vec![a.to_string()]);
    assert_eq!(ids(&dlq(&s, "workflow=w").await), vec![a.to_string()]);

    // Pagination: 2 + 1.
    let p1 = dlq(&s, "limit=2").await;
    assert_eq!(ids(&p1), vec![c.to_string(), b.to_string()]);
    let t = p1["next_page_token"].as_str().expect("token");
    assert!(t.starts_with("i1.d."), "{t}");
    let p2 = dlq(&s, &format!("limit=2&page_token={t}")).await;
    assert_eq!(ids(&p2), vec![a.to_string()]);
    assert!(p2.get("next_page_token").is_none(), "{p2}");

    // While the index is rebuilding the scan serves the same members (oldest first).
    for h in s.partitions() {
        h.store().delete_run_index_marker().expect("delete marker");
    }
    let mut got = ids(&dlq(&s, "").await);
    got.sort();
    let mut want = vec![a.to_string(), b.to_string(), c.to_string()];
    want.sort();
    assert_eq!(got, want);
    let (st, v) = s.dlq_list(&format!("page_token={t}")).await;
    assert_eq!(st, StatusCode::SERVICE_UNAVAILABLE, "{v}");
    for h in s.partitions() {
        h.store().start_run_index_rebuild();
        h.store().join_run_index_rebuild();
    }
    assert_eq!(
        dlq(&s, "").await["entries"].as_array().map(Vec::len),
        Some(3)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dismiss_hides_entries_and_non_members_conflict() {
    let s = server().await;
    let a = fail_w(&s, "flaky", json!({})).await;
    let (st, v) = s
        .post_json(
            &format!("/v1/dlq/{a}/dismiss"),
            &json!({"reason": "known flake"}),
        )
        .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert!(v["dismissed_at_ms"].as_u64().expect("at") > 0);
    assert_eq!(v["dismiss_reason"], "known flake");
    let r = row(&s, &a).expect("row");
    assert_eq!(
        Some(r.episode_completed_at_ms),
        s.run_record(&a).completed_at_ms,
        "marks carry the current episode"
    );
    // Dismissing again is idempotent (last mark wins).
    let (st, _) = s
        .post_json(&format!("/v1/dlq/{a}/dismiss"), &json!({"reason": "again"}))
        .await;
    assert_eq!(st, StatusCode::OK);

    assert!(
        entry(&dlq(&s, "").await, &a).is_none(),
        "dismissed drops out"
    );
    let all = dlq(&s, "include_dismissed=true").await;
    let e = entry(&all, &a).expect("listed with include_dismissed");
    assert_eq!(e["dismiss_reason"], "again");
    // 11-11's search hides dismissed runs too (both orders).
    for q in ["dlq=true", "dlq=true&order=newest"] {
        let listed = s.list_all(q).await;
        assert!(!listed.contains(&a.to_string()), "{q}: {listed:?}");
    }

    let ok = s.start_rest("w", json!({"input": {}})).await;
    terminal(&s, &ok).await;
    let (st, v) = s
        .post_json(&format!("/v1/dlq/{ok}/dismiss"), &json!({"reason": "x"}))
        .await;
    assert_eq!(st, StatusCode::CONFLICT, "{v}");
    assert_eq!(v["error"]["details"]["reason"], "dlq_not_member", "{v}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dismiss_episode_resumed_and_refailed_run_is_back() {
    let s = server().await;
    let c = nondet(&s).await;
    let (st, _) = s
        .post_json(&format!("/v1/dlq/{c}/dismiss"), &json!({"reason": "later"}))
        .await;
    assert_eq!(st, StatusCode::OK);
    assert!(entry(&dlq(&s, "").await, &c).is_none());
    let first = s.run_record(&c).completed_at_ms;
    // Resume (Phase 5 retry) while the divergent module is still in place: fails again.
    let (st, v) = s
        .post_json(&format!("/v1/runs/{c}/retry"), &json!({}))
        .await;
    assert!(st.is_success(), "{st} {v}");
    let rec = s
        .wait_record(&c, LIMIT, |r| {
            r.status == RunStatus::Failed && r.completed_at_ms != first
        })
        .await;
    assert_eq!(rec.resumed_count, 1);
    let page = dlq(&s, "").await;
    let e = entry(&page, &c).expect("a new episode is a member again");
    assert!(e.get("dismissed_at_ms").is_none(), "{e}");
    fix(&s);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn requeue_resume_restart_and_rerun_delegation() {
    let s = server().await;

    // Non-resumable (guest failure): auto = restart with dedupe __dlq/<run>/1.
    let a = fail_w(&s, "restart me", json!({"tags": ["keep"]})).await;
    let (st, v) = s
        .post_json(&format!("/v1/dlq/{a}/requeue"), &json!({"dry_run": true}))
        .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["mode"], "restart");
    assert!(v.get("new_run_id").is_none());
    let (st, v) = s
        .post_json(&format!("/v1/dlq/{a}/requeue"), &json!({"mode": "auto"}))
        .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["mode"], "restart");
    let new: RunId = v["new_run_id"]
        .as_str()
        .expect("new run")
        .parse()
        .expect("id");
    let nrec = s.run_record(&new);
    assert_eq!(
        String::from_utf8_lossy(&nrec.idempotency_key),
        format!("__dlq/{a}/1")
    );
    assert_eq!(nrec.retry_of, Some(a));
    assert_eq!(nrec.tags, vec!["keep".to_owned()]);
    // Re-issuing returns the same new run.
    let (st, v2) = s
        .post_json(&format!("/v1/dlq/{a}/requeue"), &json!({}))
        .await;
    assert_eq!(st, StatusCode::OK, "{v2}");
    assert_eq!(v2["new_run_id"], v["new_run_id"]);
    assert!(
        entry(&dlq(&s, "").await, &a).is_none(),
        "requeued drops out"
    );
    let all = dlq(&s, "include_dismissed=true").await;
    assert_eq!(entry(&all, &a).expect("a")["requeued_to"], new.to_string());
    let (st, v) = s
        .post_json(&format!("/v1/dlq/{a}/requeue"), &json!({"mode": "resume"}))
        .await;
    assert_eq!(st, StatusCode::OK, "the existing requeue is returned: {v}");

    // Resume mode on a non-resumable member is a conflict.
    let b = fail_w(&s, "not resumable", json!({})).await;
    let (st, v) = s
        .post_json(&format!("/v1/dlq/{b}/requeue"), &json!({"mode": "resume"}))
        .await;
    assert_eq!(st, StatusCode::CONFLICT, "{v}");

    // Rerun delegates to the rerun control plane: 501 before 11-10, 2xx after.
    let (st, v) = s
        .post_json(
            &format!("/v1/dlq/{b}/requeue"),
            &json!({"mode": {"rerun": {"from": {"seq": 1}}}}),
        )
        .await;
    if st == StatusCode::NOT_IMPLEMENTED {
        assert_eq!(v["error"]["details"]["lands_in"], "11-10", "{v}");
    } else {
        assert!(st.is_success(), "{st} {v}");
        assert_eq!(v["mode"], "rerun");
    }

    // Resumable (nondeterminism): auto = resume, same id; after the fix it completes.
    let c = nondet(&s).await;
    // Fix-deploy first (the override goes away), then requeue.
    fix(&s);
    let (st, v) = s
        .post_json(&format!("/v1/dlq/{c}/requeue"), &json!({"mode": "auto"}))
        .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["mode"], "resume");
    assert_eq!(v["run_id"], c.to_string());
    assert!(v.get("new_run_id").is_none());
    let rec = terminal(&s, &c).await;
    assert_eq!(
        rec.status,
        RunStatus::Completed,
        "resumed on the fixed module"
    );
    assert!(entry(&dlq(&s, "include_dismissed=true").await, &c).is_none());

    let body = scrape(&s).await;
    assert!(
        metric_at_least(&body, "ostro_dlq_requeues_total{mode=\"restart\"}", 1.0),
        "{body}"
    );
    assert!(metric_at_least(
        &body,
        "ostro_dlq_requeues_total{mode=\"resume\"}",
        1.0
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn policy_round_trips_through_ext_tags() {
    let s = server().await;
    // A RunPolicy and an unknown ext tag 150 on the workflow's record.
    let r = s
        .http
        .put(s.url("/v1/workflows/w/policy"))
        .json(&json!({"retention": "1h"}))
        .send()
        .await
        .expect("PUT policy");
    assert!(r.status().is_success(), "{}", r.status());
    let cur = s
        .h()
        .meta()
        .view()
        .read(|st| st.workflow_policy("w").map(|v| v.value.clone()))
        .expect("record");
    let mut rec = cur.clone();
    rec.ext = ext_put(&rec.ext, 150, b"future").expect("ext");
    common::put_meta(
        &s,
        MetaKey::WorkflowPolicy("w".into()),
        MetaRecord::WorkflowPolicy(rec),
    )
    .await;

    let put = put_failure_policy(
        &s,
        "workflows",
        "w",
        json!({"on_failure": {"workflow": "on_fail", "include_input": true}}),
    )
    .await;
    assert_eq!(put["on_failure"]["workflow"], "on_fail");
    assert_eq!(put["on_failure"]["include_input"], true);
    let (st, got) = s.get_json("/v1/workflows/w/failure-policy").await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(got, put);
    let (_, rp) = s.get_json("/v1/workflows/w/policy").await;
    assert_eq!(rp["retention_ms"], 3_600_000, "RunPolicy unchanged: {rp}");
    let stored = s
        .h()
        .meta()
        .view()
        .read(|st| st.workflow_policy("w").map(|v| v.value.clone()))
        .expect("record");
    assert!(ext_get(&stored.ext, EXT_ON_FAILURE).is_some());
    assert_eq!(ext_get(&stored.ext, 150), Some(&b"future"[..]));
    assert_eq!(stored.retention_ms, Some(3_600_000));

    // Stale version tag.
    let r = s
        .http
        .put(s.url("/v1/workflows/w/failure-policy"))
        .json(&json!({"version_tag": 1}))
        .send()
        .await
        .expect("PUT");
    assert_eq!(r.status(), StatusCode::CONFLICT);

    // Queue: DLQ policy via tag 145; a disabled queue has no members.
    let q = put_failure_policy(
        &s,
        "queues",
        "qoff",
        json!({"dlq": {"enabled": false, "hook_catchup_ms": 5000}}),
    )
    .await;
    assert_eq!(q["dlq"], json!({"enabled": false, "hook_catchup_ms": 5000}));
    let (_, gq) = s.get_json("/v1/queues/qoff/failure-policy").await;
    assert_eq!(gq, q);
    let qrec = s
        .h()
        .meta()
        .view()
        .read(|st| st.queue_policy("qoff").map(|v| v.value.clone()))
        .expect("queue record");
    assert!(ext_get(&qrec.ext, EXT_DLQ_POLICY).is_some());
    let off = fail_w(&s, "quiet", json!({"queue": "qoff"})).await;
    assert!(entry(&dlq(&s, "include_dismissed=true").await, &off).is_none());

    // Unknown workflow: 404.
    let (st, _) = s.get_json("/v1/workflows/nope/failure-policy").await;
    assert_eq!(st, StatusCode::NOT_FOUND);
}

// ---------------------------------------------------------------------------------
// Task 2: on_failure hook service
// ---------------------------------------------------------------------------------

async fn hook_runs(s: &P11Server, workflow: &str) -> Vec<String> {
    s.list_all(&format!("workflow={workflow}")).await
}

/// Wait until `run`'s DLQ row names a hook run for the current episode.
async fn wait_hook(s: &P11Server, run: &RunId) -> RunId {
    let deadline = Instant::now() + LIMIT;
    loop {
        let rec = s.run_record(run);
        if let Some(r) = row(s, run)
            && Some(r.episode_completed_at_ms) == rec.completed_at_ms
            && let Some(h) = r.hook_run
        {
            return RunId::from_bytes(&h).expect("hook id");
        }
        assert!(Instant::now() < deadline, "no HookStarted for {run}");
        sleep(Duration::from_millis(20)).await;
    }
}

async fn hook_policy(s: &P11Server, workflow: &str, hook: &str) {
    put_failure_policy(
        s,
        "workflows",
        workflow,
        json!({"on_failure": {"workflow": hook, "include_input": true}}),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hook_fires_once_with_documented_input() {
    let s = server().await;
    hook_policy(&s, "w", "on_fail").await;
    let a = fail_w(&s, "hooked", json!({"tags": ["t1"]})).await;
    let hook = wait_hook(&s, &a).await;
    let hrec = terminal(&s, &hook).await;
    assert_eq!(hrec.status, RunStatus::Completed);
    assert!(
        hrec.tags.contains(&format!("__ostro/on_failure_of={a}")),
        "{:?}",
        hrec.tags
    );
    assert_eq!(
        String::from_utf8_lossy(&hrec.idempotency_key),
        format!("__onfail/{a}/0")
    );
    let out: Value =
        serde_json::from_slice(&s.result_bytes(&hook).await.expect("result")).expect("json");
    let arec = s.run_record(&a);
    assert_eq!(
        out,
        json!({
            "run_id": a.to_string(),
            "workflow": "w",
            "version": 1,
            "status": "failed",
            "reason": "workflow_failed: hooked",
            "failed_at_ms": arec.completed_at_ms.expect("completed"),
            "tags": ["t1"],
            "input": {"fail": true, "msg": "hooked"},
        })
    );
    let page = dlq(&s, "").await;
    assert_eq!(
        entry(&page, &a).expect("a")["hook_run_id"],
        hook.to_string()
    );
    sleep(Duration::from_secs(1)).await;
    assert_eq!(hook_runs(&s, "on_fail").await, vec![hook.to_string()]);
    let body = scrape(&s).await;
    assert!(
        metric_at_least(&body, "ostro_dlq_hooks_total{result=\"started\"}", 1.0),
        "{body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hook_loop_guard_stops_after_one_failed_hook() {
    let s = server().await;
    hook_policy(&s, "w", "always_fail").await;
    hook_policy(&s, "on_fail", "always_fail").await;
    hook_policy(&s, "always_fail", "always_fail").await;
    let a = fail_w(&s, "loop?", json!({})).await;
    let hook = wait_hook(&s, &a).await;
    assert_eq!(terminal(&s, &hook).await.status, RunStatus::Failed);
    sleep(Duration::from_secs(2)).await;
    assert_eq!(hook_runs(&s, "always_fail").await, vec![hook.to_string()]);
    assert!(row(&s, &hook).is_none(), "a hook run never gets a hook");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hook_per_failure_episode_and_disabled_queue() {
    let s = server().await;
    s.deploy_guest("patch-v1", None).await;
    hook_policy(&s, "patch-demo", "on_fail").await;
    let c = nondet(&s).await;
    let h1 = wait_hook(&s, &c).await;
    let (st, v) = s
        .post_json(&format!("/v1/runs/{c}/retry"), &json!({}))
        .await;
    assert!(st.is_success(), "{st} {v}");
    let first = s.run_record(&c).completed_at_ms;
    s.wait_record(&c, LIMIT, |r| {
        r.status == RunStatus::Failed && r.completed_at_ms != first && r.resumed_count == 1
    })
    .await;
    let h2 = wait_hook(&s, &c).await;
    assert_ne!(h1, h2, "a second episode gets a second hook");
    assert_eq!(
        String::from_utf8_lossy(&s.run_record(&h2).idempotency_key),
        format!("__onfail/{c}/1")
    );
    fix(&s);

    // Queue-level hook on a queue whose DLQ policy is disabled: no hook.
    put_failure_policy(
        &s,
        "queues",
        "qoff",
        json!({"on_failure": {"workflow": "on_fail"},
               "dlq": {"enabled": false, "hook_catchup_ms": 1000}}),
    )
    .await;
    let off = fail_w(&s, "quiet", json!({"queue": "qoff"})).await;
    sleep(Duration::from_millis(1500)).await;
    assert!(row(&s, &off).is_none());
    let mut runs = hook_runs(&s, "on_fail").await;
    runs.sort();
    let mut want = vec![h1.to_string(), h2.to_string()];
    want.sort();
    assert_eq!(runs, want);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hook_catch_up_after_restart_honours_the_window() {
    let mut s = server_with(|c| {
        c.dlq.hooks_enabled = false;
        c.dlq.hook_catchup = Duration::from_secs(3).into();
    })
    .await;
    hook_policy(&s, "w", "on_fail").await;
    let old = fail_w(&s, "old", json!({})).await;
    sleep(Duration::from_millis(4_000)).await;
    let new = fail_w(&s, "new", json!({})).await;
    sleep(Duration::from_millis(300)).await;
    assert!(row(&s, &new).is_none(), "hooks disabled");
    s.cfg.dlq.hooks_enabled = true;
    s.restart().await;
    s.wait_ready(Duration::from_secs(20)).await;
    let hook = wait_hook(&s, &new).await;
    sleep(Duration::from_secs(1)).await;
    assert!(row(&s, &old).is_none(), "outside the catch-up window");
    assert_eq!(hook_runs(&s, "on_fail").await, vec![hook.to_string()]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hook_catch_up_falls_back_to_the_scan_while_rebuilding() {
    let s = server_with(|c| c.dlq.hooks_enabled = false).await;
    hook_policy(&s, "w", "on_fail").await;
    let a = fail_w(&s, "rebuilding", json!({})).await;
    for h in s.partitions() {
        h.store().delete_run_index_marker().expect("delete marker");
    }
    let p11 = s.h().control().p11().expect("p11 ctx");
    let t0 = Instant::now();
    let stats =
        dlq_hook::catch_up(&p11, Duration::from_millis(300), &CancellationToken::new()).await;
    assert_eq!(stats.scan_fallbacks, 4, "{stats:?}");
    assert_eq!(stats.started, 1, "{stats:?}");
    assert!(t0.elapsed() >= Duration::from_millis(300));
    let hook = wait_hook(&s, &a).await;
    // A second pass finds HookStarted and drives nothing.
    for h in s.partitions() {
        h.store().start_run_index_rebuild();
        h.store().join_run_index_rebuild();
    }
    let again =
        dlq_hook::catch_up(&p11, Duration::from_millis(300), &CancellationToken::new()).await;
    assert_eq!(again.driven, 0, "{again:?}");
    assert_eq!(hook_runs(&s, "on_fail").await, vec![hook.to_string()]);
}

#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hook_exactly_once_across_a_crash_after_start() {
    let scenario = fail::FailScenario::setup();
    let mut s = server().await;
    hook_policy(&s, "w", "on_fail").await;
    fail::cfg("dlq::hook_after_start", "return").expect("cfg");
    let t0 = Instant::now();
    let a = fail_w(&s, "crash", json!({})).await;
    let deadline = Instant::now() + LIMIT;
    while hook_runs(&s, "on_fail").await.is_empty() {
        assert!(Instant::now() < deadline, "hook run not started");
        sleep(Duration::from_millis(20)).await;
    }
    sleep(Duration::from_millis(300)).await;
    assert!(
        row(&s, &a).is_none_or(|r| r.hook_run.is_none()),
        "the failpoint skipped HookStarted"
    );
    let blocked = t0.elapsed();
    fail::remove("dlq::hook_after_start");
    let t1 = Instant::now();
    s.restart().await;
    s.wait_ready(Duration::from_secs(20)).await;
    let hook = wait_hook(&s, &a).await;
    eprintln!(
        "failpoint phase {blocked:?}; restart to HookStarted {:?}",
        t1.elapsed()
    );
    assert_eq!(hook_runs(&s, "on_fail").await, vec![hook.to_string()]);
    scenario.teardown();
}
