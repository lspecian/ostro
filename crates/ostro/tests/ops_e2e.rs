//! PHASE-11 GATE (plan 11-18): ROADMAP Phase 11 success criteria 1-3 through the spawned
//! `ostro` binary (CLI verbs + REST), no in-process server anywhere.
//!
//! - `sc1_*`: a failed run is in the DLQ with its reason; its `on_failure` hook ran exactly
//!   once; `ostro dlq requeue --mode restart` starts a linked run; after deploying the
//!   fixed module, `ostro rerun --from-step s3` completes without re-running s1/s2 (one
//!   `step_completed` per step) and the copied prefix is identical to the source.
//! - `sc2_*`: `ostro bulk retry` and `ostro bulk rerun` over a tag filter on the real bulk
//!   service: paced at `--rate 20` after the burst, bounded by the job's snapshot (the
//!   runs a job creates carry the same tag and are not processed), finished; tag search
//!   pages are bounded and newest first.
//! - `sc3_*`: `ostro server` with lowered `[limits]`: history / children limit failures
//!   are resumable `limit_exceeded` runs, a large input is 413 with details, a rerun prefix
//!   over `max_history_events` is 413 `rerun_prefix_too_large`; after a restart with the
//!   defaults, `ostro retry` and `ostro dlq requeue --mode resume` complete the runs;
//!   `GET /v1/limits` equals `docs/limits.md` under the default config.
//!
//! The kill tests (rerun re-arm, on_failure exactly once, bulk resume) are in `ops_kill.rs`
//! (feature `failpoints`).

mod common;
#[path = "common/p11.rs"]
mod p11;

use std::collections::BTreeSet;
use std::time::Duration;

use common::*;
use p11::*;
use reqwest::StatusCode;
use serde_json::{Value, json};

const PIPELINE: &str = "pipeline";

/// Wait for every run in `ids` to reach a terminal status; returns their final JSON.
async fn wait_all_terminal(n: &P11Node, ids: &[String]) -> Vec<Value> {
    let mut out = Vec::new();
    for id in ids {
        out.push(n.api.wait_terminal(id, T).await);
    }
    out
}

/// The DLQ entry of `run` (default listing), if listed.
async fn dlq_entry(n: &P11Node, run: &str, query: &str) -> Option<Value> {
    let (st, page) = n
        .rest("GET", &format!("/v1/dlq?limit=200{query}"), None)
        .await;
    assert_eq!(st, StatusCode::OK, "{page}");
    page["entries"]
        .as_array()
        .expect("entries")
        .iter()
        .find(|e| e["run_id"] == run)
        .cloned()
}

fn has_tag(run: &Value, tag: &str) -> bool {
    run["tags"]
        .as_array()
        .is_some_and(|t| t.iter().any(|x| x == tag))
}

// ---------------------------------------------------------------------------------
// SC1: DLQ + on_failure + requeue + fix-and-rerun
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sc1_dlq_on_failure_requeue_and_fixed_rerun() {
    let _g = SERIAL.lock().await;
    let n = P11Node::start(&Profile::Dev, &[]).await;
    n.deploy("p11-hook");
    n.deploy("p11-rerun");
    let (st, v) = n
        .rest(
            "PUT",
            "/v1/workflows/pipeline/failure-policy",
            Some(json!({"on_failure": {"workflow": "on_fail", "include_input": true}})),
        )
        .await;
    assert!(st.is_success(), "{st} {v}");

    // A run that fails at s3 (non-retryable guest failure).
    let (src, status) = n.run_wait(PIPELINE, r#"{"fail_at":3}"#, &["sc1"]);
    assert_eq!(status, "failed");

    // In the DLQ with its reason, and the hook recorded for this episode.
    let entry = poll("DLQ entry with hook_run_id", T, || async {
        dlq_entry(&n, &src, "")
            .await
            .filter(|e| e["hook_run_id"].is_string())
    })
    .await;
    let reason = entry["reason"].as_str().expect("reason");
    assert_eq!(reason, "workflow_failed: s3 fails", "{entry}");
    let hook = entry["hook_run_id"].as_str().unwrap().to_owned();
    let cli = n.ok(&["dlq", "ls"]);
    assert!(ids(&cli, "entries").contains(&src), "{cli}");

    // The hook run: tagged with the failed run, completed, input as documented.
    let h = n.api.wait_terminal(&hook, T).await;
    assert_eq!(h["status"], "completed", "{h}");
    assert_eq!(h["workflow"], "on_fail");
    assert!(has_tag(&h, &format!("__ostro/on_failure_of={src}")), "{h}");
    let res = n.ok(&["result", &hook]);
    assert_eq!(res["result"]["run_id"], src.as_str(), "{res}");
    assert_eq!(res["result"]["workflow"], PIPELINE, "{res}");
    assert_eq!(res["result"]["status"], "failed", "{res}");
    assert_eq!(res["result"]["input"], json!({"fail_at": 3}), "{res}");
    // Exactly once: no second hook for this run after a grace period.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let hooks: Vec<Value> = list_all(n.addr, "workflow=on_fail")
        .await
        .into_iter()
        .filter(|r| has_tag(r, &format!("__ostro/on_failure_of={src}")))
        .collect();
    assert_eq!(hooks.len(), 1, "{hooks:?}");
    assert_eq!(hooks[0]["run_id"], hook.as_str());

    // Requeue (restart): a new linked run; the source leaves the default DLQ view.
    let dry = n.ok(&["dlq", "requeue", &src, "--dry-run"]);
    assert_eq!(
        dry["mode"], "restart",
        "auto picks restart for a guest failure: {dry}"
    );
    let rq = n.ok(&["dlq", "requeue", &src, "--mode", "restart"]);
    assert_eq!(rq["mode"], "restart", "{rq}");
    let restarted = rq["new_run_id"].as_str().expect("new_run_id").to_owned();
    assert_ne!(restarted, src);
    let r = n.api.wait_terminal(&restarted, T).await;
    assert_eq!(r["status"], "failed", "v1 still fails: {r}");
    assert!(has_tag(&r, "sc1"), "tags copied: {r}");
    assert!(
        dlq_entry(&n, &src, "").await.is_none(),
        "requeued source still listed"
    );
    let all = dlq_entry(&n, &src, "&include_dismissed=true")
        .await
        .expect("listed with include_dismissed");
    assert_eq!(all["requeued_to"], restarted.as_str(), "{all}");

    // Fix: v2 never fails at s3. Rerun the source from s3.
    n.deploy("p11-rerun-v2");
    let rr = n.ok(&["rerun", &src, "--from-step", "s3"]);
    let rerun = rr["new_run_id"].as_str().expect("new_run_id").to_owned();
    assert_eq!(rr["version"], 2, "{rr}");
    let cut = rr["cut_seq"].as_u64().expect("cut_seq");
    let done = n.ok(&["result", &rerun, "--wait"]);
    assert_eq!(done["status"], "completed", "{done}");
    assert_eq!(done["result"], 5, "{done}");

    // s1/s2 were not re-executed: exactly one completion per step in the rerun journal.
    let rh = n.api.history(&rerun).await;
    for s in ["s1", "s2", "s3", "s4", "s5"] {
        assert_eq!(step_completions(&rh, s), 1, "{s}: {rh:?}");
    }
    // The copied prefix (1..cut) equals the source's events after blob resolution.
    let sh = n.api.history(&src).await;
    let cut_us = usize::try_from(cut).unwrap();
    assert!(
        cut_us >= 3 && cut_us < sh.len(),
        "cut {cut} of {}",
        sh.len()
    );
    for i in 1..cut_us {
        assert_eq!(rh[i]["seq"], sh[i]["seq"]);
        assert_eq!(rh[i]["kind"], sh[i]["kind"], "seq {i}");
        assert_eq!(rh[i]["details"], sh[i]["details"], "seq {i}");
    }
    assert_eq!(rh[0]["kind"], "run_started");
    let info = n.api.run(&rerun).await;
    assert!(
        has_tag(&info, &format!("__ostro/rerun_from={cut}")),
        "{info}"
    );
    eprintln!("SC1: source {src}, hook {hook}, restart {restarted}, rerun {rerun} (cut {cut})");
    let mut n = n;
    n.stop();
}

// ---------------------------------------------------------------------------------
// SC2: bulk retry + bulk rerun, pacing, snapshot bound, tag search pages
// ---------------------------------------------------------------------------------

const BULK_RUNS: usize = 60;
const RATE: u64 = 20;
const BURST: usize = 10;

/// Runs of `list` carrying `tag`, sorted by creation time.
fn created_sorted(list: &[Value]) -> Vec<u64> {
    let mut v: Vec<u64> = list
        .iter()
        .map(|r| r["created_at_ms"].as_u64().expect("created_at_ms"))
        .collect();
    v.sort_unstable();
    v
}

/// The pacing check on op timestamps: after the burst, `k` ops span at least
/// `0.9 × (k-1) / rate`; returns (span ms, count of gaps shorter than 1/rate).
fn assert_paced(what: &str, ts: &[u64]) -> (u64, usize) {
    let after = &ts[BURST.min(ts.len())..];
    assert!(after.len() >= 10, "{what}: too few ops after the burst");
    let span = after.last().unwrap() - after.first().unwrap();
    let min_span = (after.len() as u64 - 1) * 1000 / RATE * 9 / 10;
    let short = after
        .windows(2)
        .filter(|w| w[1] - w[0] < 1000 / RATE)
        .count();
    assert!(
        span >= min_span,
        "{what}: {} ops after the burst span {span} ms < {min_span} ms",
        after.len()
    );
    (span, short)
}

/// `ostro bulk <args> --json` -> the job id; then `ostro bulk status --watch` to the end.
fn bulk_run(n: &P11Node, args: &[&str]) -> Value {
    let v = n.ok(args);
    let job = v["job_id"]
        .as_str()
        .unwrap_or_else(|| panic!("{v}"))
        .to_owned();
    assert!(job.starts_with("blk_"), "{job}");
    let (code, done, err) = n.json(&["bulk", "status", &job, "--watch", "--interval", "200ms"]);
    assert_eq!(code, 0, "{done}\n{err}");
    assert_eq!(done["state"], "done", "{done}");
    done
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sc2_bulk_retry_and_rerun_paced_bounded_and_searchable() {
    let _g = SERIAL.lock().await;
    let n = P11Node::start(&Profile::Dev, &[]).await;
    n.deploy("p11-rerun");
    let mut src = Vec::new();
    for _ in 0..BULK_RUNS {
        let (st, v) = n
            .api
            .start_http(
                PIPELINE,
                json!({"input": {"fail_at": 3}, "tags": ["batch"]}),
            )
            .await;
        assert!(st.is_success(), "{st} {v}");
        src.push(v["run_id"].as_str().expect("run_id").to_owned());
    }
    for r in wait_all_terminal(&n, &src).await {
        assert_eq!(r["status"], "failed", "{r}");
    }

    // Dry run: estimate, no job.
    let dry = n.ok(&["bulk", "retry", "--tag", "batch", "--dry-run"]);
    assert_eq!(dry["matched_estimate"], BULK_RUNS, "{dry}");
    assert!(dry["job_id"].is_null(), "{dry}");

    // Bulk retry: guest failures are not resumable, so each op restarts a linked run that
    // carries the same tag. The snapshot bound keeps those out of the job.
    let retry = bulk_run(
        &n,
        &[
            "bulk",
            "retry",
            "--tag",
            "batch",
            "--rate",
            &RATE.to_string(),
        ],
    );
    assert_eq!(retry["matched"], BULK_RUNS, "{retry}");
    assert_eq!(retry["done"], BULK_RUNS, "{retry}");
    assert_eq!(retry["failed_count"], 0, "{retry}");
    let all = list_all(n.addr, "tags=batch").await;
    assert_eq!(all.len(), 2 * BULK_RUNS, "exactly one restart per source");
    let srcset: BTreeSet<&str> = src.iter().map(String::as_str).collect();
    let restarts: Vec<Value> = all
        .iter()
        .filter(|r| !srcset.contains(r["run_id"].as_str().unwrap()))
        .cloned()
        .collect();
    let (span_retry, short_retry) = assert_paced("bulk retry", &created_sorted(&restarts));
    let restart_ids: Vec<String> = restarts
        .iter()
        .map(|r| r["run_id"].as_str().unwrap().to_owned())
        .collect();
    wait_all_terminal(&n, &restart_ids).await;

    // Fix, then bulk rerun from s3 over the same tag: the snapshot is the 120 runs that
    // exist at job creation; the 120 reruns it creates (same tag) are not processed.
    n.deploy("p11-rerun-v2");
    let before = list_all(n.addr, "tags=batch").await.len();
    let rerun = bulk_run(
        &n,
        &[
            "bulk",
            "rerun",
            "--tag",
            "batch",
            "--from-step",
            "s3",
            "--rate",
            &RATE.to_string(),
        ],
    );
    assert_eq!(rerun["matched"], before, "{rerun}");
    assert_eq!(rerun["done"], before, "{rerun}");
    assert_eq!(rerun["failed_count"], 0, "{rerun}");
    let all = list_all(n.addr, "tags=batch").await;
    assert_eq!(all.len(), 2 * before, "exactly one rerun per matched run");
    let reruns: Vec<Value> = all
        .iter()
        .filter(|r| {
            r["tags"].as_array().is_some_and(|t| {
                t.iter().any(|x| {
                    x.as_str()
                        .is_some_and(|s| s.starts_with("__ostro/rerun_from="))
                })
            })
        })
        .cloned()
        .collect();
    assert_eq!(reruns.len(), before, "rerun lineage tags");
    let (span_rerun, short_rerun) = assert_paced("bulk rerun", &created_sorted(&reruns));
    let rerun_ids: Vec<String> = reruns
        .iter()
        .map(|r| r["run_id"].as_str().unwrap().to_owned())
        .collect();
    for r in wait_all_terminal(&n, &rerun_ids).await {
        assert_eq!(r["status"], "completed", "rerun on v2: {r}");
    }

    // Tag search: bounded pages, newest first, no duplicates, complete.
    let mut seen = BTreeSet::new();
    let mut last = u64::MAX;
    let mut pages = 0;
    let mut token: Option<String> = None;
    loop {
        let mut path = "/v1/runs?tags=batch&order=newest&limit=25".to_owned();
        if let Some(t) = &token {
            path.push_str(&format!("&page_token={t}"));
        }
        let (st, p) = n.rest("GET", &path, None).await;
        assert_eq!(st, StatusCode::OK, "{p}");
        let runs = p["runs"].as_array().expect("runs");
        assert!(runs.len() <= 25, "page over the limit");
        for r in runs {
            let c = r["created_at_ms"].as_u64().unwrap();
            assert!(c <= last, "not newest first: {c} after {last}");
            last = c;
            assert!(seen.insert(r["run_id"].as_str().unwrap().to_owned()));
        }
        pages += 1;
        match p["next_page_token"].as_str() {
            Some(t) => token = Some(t.to_owned()),
            None => break,
        }
    }
    assert_eq!(seen.len(), 2 * before);
    assert!(pages >= (2 * before).div_ceil(25), "{pages} pages");
    // The CLI form of the same search.
    let v = n.ok(&["ls", "--tag", "batch", "--newest", "--limit", "25"]);
    assert_eq!(v["runs"].as_array().map(Vec::len), Some(25), "{v}");

    eprintln!(
        "SC2: retry {BULK_RUNS} ops, span after burst {span_retry} ms ({short_retry} short gaps); \
         rerun {before} ops, span after burst {span_rerun} ms ({short_rerun} short gaps); \
         search {pages} pages of <= 25"
    );
    let mut n = n;
    n.stop();
}

// ---------------------------------------------------------------------------------
// SC3: limits
// ---------------------------------------------------------------------------------

const LOW_LIMITS: &str = "[limits]\nmax_history_events = 20\nmax_children_per_run = 3\n\
                          max_payload_bytes = \"64KiB\"\n";

/// `name -> default` rows of the `docs/limits.md` table (the 11-11 `limits_doc` parser).
fn doc_rows() -> Vec<(String, u64)> {
    let text = std::fs::read_to_string(repo_root().join("docs/limits.md")).expect("limits.md");
    let mut out = Vec::new();
    for line in text.lines() {
        let cols: Vec<&str> = line.split('|').map(str::trim).collect();
        if cols.len() < 4 || !cols[0].is_empty() {
            continue;
        }
        let Some(name) = cols[1].strip_prefix('`').and_then(|c| c.strip_suffix('`')) else {
            continue;
        };
        let value: u64 = cols[2].parse().expect("integer default");
        out.push((name.to_owned(), value));
    }
    out
}

fn limit_rows(v: &Value) -> Vec<(String, u64)> {
    v["limits"]
        .as_array()
        .expect("limits")
        .iter()
        .map(|l| {
            (
                l["name"].as_str().unwrap().to_owned(),
                l["value"].as_u64().unwrap(),
            )
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sc3_limits_errors_resumable_failures_and_published_table() {
    let _g = SERIAL.lock().await;
    let defaults = Profile::Server(String::new());
    let low = Profile::Server(LOW_LIMITS.to_owned());

    // Defaults: the published table, and a 32-event source for the rerun-prefix bound.
    let mut n = P11Node::start(&defaults, &[]).await;
    let lim = n.ok(&["limits"]);
    assert_eq!(
        limit_rows(&lim),
        doc_rows(),
        "GET /v1/limits == docs/limits.md"
    );
    n.deploy("p11-limits");
    let (long_src, st) = n.run_wait("limits-history", r#"{"n":30,"end":"fail"}"#, &[]);
    assert_eq!(st, "failed");
    n.stop();

    // Lowered limits.
    n.boot(&low, &[]).await;
    let lim = limit_rows(&n.ok(&["limits"]));
    for (k, want) in [
        ("max_history_events", 20),
        ("max_children_per_run", 3),
        ("max_payload_bytes", 65536),
    ] {
        assert!(lim.contains(&(k.to_owned(), want)), "{k}: {lim:?}");
    }
    // History: Failed, resumable, limit_exceeded.
    let mut hist = Vec::new();
    for _ in 0..2 {
        let (id, st) = n.run_wait("limits-history", r#"{"n":30}"#, &[]);
        assert_eq!(st, "failed");
        hist.push(id);
    }
    for id in &hist {
        let e = poll("history-limit DLQ entry", T, || dlq_entry(&n, id, "")).await;
        let reason = e["reason"].as_str().unwrap();
        assert!(
            reason.starts_with("limit_exceeded: history_events 21 > 20"),
            "{reason}"
        );
        let dry = n.ok(&["retry", id, "--dry-run"]);
        assert_eq!(
            dry["mode"], "resumed",
            "a limit failure is resumable: {dry}"
        );
        assert_eq!(
            n.api.history(id).await.len(),
            20,
            "journal stops at the limit"
        );
    }
    // Children: 4 > 3 (children are bridged at this HEAD: Phase 9).
    let (kids, st) = n.run_wait("limits-children", r#"{"n":4}"#, &[]);
    assert_eq!(st, "failed");
    let e = poll("children-limit DLQ entry", T, || dlq_entry(&n, &kids, "")).await;
    assert!(
        e["reason"]
            .as_str()
            .unwrap()
            .starts_with("limit_exceeded: children"),
        "{e}"
    );
    // Payload: a 100 KiB input against 64 KiB is 413 with details, no run.
    let big = "x".repeat(100 * 1024);
    let (st, body) = n
        .api
        .start_http("limits-history", json!({"input": {"n": 1, "pad": big}}))
        .await;
    assert_eq!(st, StatusCode::PAYLOAD_TOO_LARGE, "{body}");
    let d = &body["error"]["details"];
    assert_eq!(d["reason"], "limit_exceeded", "{body}");
    assert_eq!(d["limit"], "payload_bytes", "{body}");
    assert_eq!(d["max"], 65536, "{body}");
    assert!(d["actual"].as_u64().unwrap() > 100 * 1024, "{body}");
    // Rerun prefix over max_history_events: 413 rerun_prefix_too_large.
    let (code, body, err) = n.json(&["rerun", &long_src, "--from-seq", "25"]);
    assert_eq!(code, 1, "{body}\n{err}");
    let d = &body["error"]["details"];
    assert_eq!(d["reason"], "rerun_prefix_too_large", "{body}");
    assert_eq!(d["limit"], "history_events", "{body}");
    assert_eq!(d["max"], 20, "{body}");
    assert!(err.contains("rerun_prefix_too_large"), "{err}");
    let (st, raw) = n
        .rest(
            "POST",
            &format!("/v1/runs/{long_src}/rerun"),
            Some(json!({"from": {"seq": 25}})),
        )
        .await;
    assert_eq!(st, StatusCode::PAYLOAD_TOO_LARGE, "{raw}");
    n.stop();

    // Defaults again: `ostro retry` and `ostro dlq requeue --mode resume` complete them.
    n.boot(&defaults, &[]).await;
    let r = n.ok(&["retry", &hist[0]]);
    assert_eq!(r["mode"], "resumed", "{r}");
    assert_eq!(r["run_id"], hist[0].as_str(), "{r}");
    let rq = n.ok(&["dlq", "requeue", &hist[1], "--mode", "resume"]);
    assert_eq!(rq["mode"], "resume", "{rq}");
    for id in &hist {
        let done = n.ok(&["result", id, "--wait"]);
        assert_eq!(done["status"], "completed", "{done}");
        assert_eq!(done["result"], 30, "{done}");
        assert_eq!(n.api.history(id).await.len(), 32, "{id}");
    }
    let r = n.ok(&["retry", &kids]);
    assert_eq!(r["mode"], "resumed", "{r}");
    let done = n.ok(&["result", &kids, "--wait"]);
    assert_eq!(done["status"], "completed", "{done}");
    n.stop();
}
