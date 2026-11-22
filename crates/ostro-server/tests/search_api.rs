//! Indexed run search (plan 11-11, OPS-05 / D-P11-9/10) against the real server:
//! newest-first listings from `x_run_index` (k-way merge over partitions, `i1.` page
//! tokens, budget), multi-tag AND, DLQ selection, the scan fallback while the index is
//! rebuilding, the unchanged default path, and the history `attempt` field (D-P11-18).
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;
#[path = "common/p11.rs"]
mod p11;

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use ostro_api::domain::{ListOrder, ListRunsRequest, RunStatus as ApiStatus};
use ostro_partition::store::derived::IndexState;
use ostro_partition::store::values::RunStatus;
use ostro_types::RunId;
use p11::P11Server;
use reqwest::StatusCode;
use serde_json::{Value, json};

const LIMIT: Duration = Duration::from_secs(120);
const RUNS: usize = 300;

/// A listed row: `(run id, created_at_ms)`.
type Row = (String, u64);

/// Newest-first order key of a listed row: created desc, uuid desc, partition asc.
fn newest_key(r: &Row) -> (std::cmp::Reverse<u64>, std::cmp::Reverse<u128>, u16) {
    let id: RunId = r.0.parse().expect("run id");
    (
        std::cmp::Reverse(r.1),
        std::cmp::Reverse(id.uuid().as_u128()),
        id.partition().0,
    )
}

fn rows(page: &Value) -> Vec<Row> {
    page["runs"]
        .as_array()
        .expect("runs")
        .iter()
        .map(|r| {
            (
                r["run_id"].as_str().expect("id").to_owned(),
                r["created_at_ms"].as_u64().expect("created_at_ms"),
            )
        })
        .collect()
}

/// Every page of `GET /v1/runs?{query}&limit={limit}`: `(rows, pages, tokens)`.
async fn list_pages(s: &P11Server, query: &str, limit: u32) -> (Vec<Row>, usize, Vec<String>) {
    let mut out = Vec::new();
    let mut tokens = Vec::new();
    let mut pages = 0;
    let mut token: Option<String> = None;
    loop {
        let mut path = format!("/v1/runs?limit={limit}&{query}");
        if let Some(t) = &token {
            path.push_str(&format!("&page_token={t}"));
        }
        let (st, v) = s.get_json(&path).await;
        assert_eq!(st, StatusCode::OK, "{path}: {v}");
        pages += 1;
        out.extend(rows(&v));
        match v["next_page_token"].as_str() {
            Some(t) => {
                tokens.push(t.to_owned());
                token = Some(t.to_owned());
            }
            None => break,
        }
        assert!(pages < 500, "runaway pagination for {query}");
    }
    let unique: HashSet<&String> = out.iter().map(|r| &r.0).collect();
    assert_eq!(unique.len(), out.len(), "duplicates in {query}");
    (out, pages, tokens)
}

fn assert_newest_first(rows: &[Row], what: &str) {
    for w in rows.windows(2) {
        assert!(
            newest_key(&w[0]) < newest_key(&w[1]),
            "{what}: not strictly newest first: {:?} then {:?}",
            w[0],
            w[1]
        );
    }
}

fn ids(rows: &[Row]) -> HashSet<String> {
    rows.iter().map(|r| r.0.clone()).collect()
}

struct Fixture {
    s: P11Server,
    /// Every run with its tags.
    all: Vec<(RunId, Vec<String>)>,
    /// Failed or timed-out runs.
    dlq: HashSet<String>,
}

async fn fixture() -> Fixture {
    let s = P11Server::builder().start().await;
    s.wait_ready(Duration::from_secs(20)).await;
    s.deploy_guest("sdk-basic", None).await; // sdk-echo
    s.deploy_guest("p11-limits", None).await;
    let timeouts = s.deploy_guest("timeouts", None).await;

    let mut all = Vec::new();
    for i in 0..RUNS {
        let mut tags = vec![format!("m{}", i % 7)];
        if i % 5 < 2 {
            tags.push("blue".into());
            if i % 3 == 0 {
                tags.push("big".into());
            }
        }
        let run = s
            .start_rest("sdk-echo", json!({"input": {"i": i}, "tags": tags}))
            .await;
        all.push((run, tags));
    }
    let mut dlq = HashSet::new();
    for _ in 0..6 {
        let run = s
            .start_rest("limits-history", json!({"input": {"n": 0, "end": "fail"}}))
            .await;
        all.push((run, vec![]));
        dlq.insert(run.to_string());
    }
    for _ in 0..2 {
        let run = s
            .start_raw(
                "timeouts",
                1,
                br#"{"mode":"sleep_forever"}"#,
                &timeouts,
                |r| {
                    r.run_timeout_ms = 300;
                },
            )
            .await;
        all.push((run, vec![]));
        dlq.insert(run.to_string());
    }
    for (run, _) in &all {
        let rec = s.wait_record(run, LIMIT, |r| r.status.is_terminal()).await;
        let want_dlq = dlq.contains(&run.to_string());
        assert_eq!(
            matches!(rec.status, RunStatus::Failed | RunStatus::TimedOut),
            want_dlq,
            "{run}: {rec:?}"
        );
    }
    Fixture { s, all, dlq }
}

fn tagged(f: &Fixture, want: &[&str]) -> HashSet<String> {
    f.all
        .iter()
        .filter(|(_, t)| want.iter().all(|w| t.iter().any(|x| x == w)))
        .map(|(r, _)| r.to_string())
        .collect()
}

fn index_states(s: &P11Server) -> Vec<IndexState> {
    s.partitions()
        .iter()
        .map(|h| h.store().run_index_state())
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn indexed_search_pagination_budget_fallback_and_history() {
    let mut f = fixture().await;
    let s = &f.s;
    let total = f.all.len();
    assert!(
        index_states(s).iter().all(|st| *st == IndexState::Ready),
        "fresh stores have a ready index"
    );

    // Everything, newest first (All prefix), is the reverse of the full scan order.
    let (everything, _, tokens) = list_pages(s, "order=newest", 100).await;
    assert_eq!(everything.len(), total);
    assert_newest_first(&everything, "order=newest");
    assert!(tokens.iter().all(|t| t.starts_with("i1.a.")), "{tokens:?}");
    let (newest20, pages, _) = {
        let (st, v) = s.get_json("/v1/runs?order=newest&limit=20").await;
        assert_eq!(st, StatusCode::OK, "{v}");
        assert!(v["next_page_token"].as_str().is_some());
        (rows(&v), 1, ())
    };
    assert_eq!(pages, 1);
    assert_eq!(newest20, everything[..20].to_vec());

    // tags=blue newest first: 120 runs over exactly 3 pages of 50.
    let blue = tagged(&f, &["blue"]);
    assert_eq!(blue.len(), 120);
    let (got, pages, tokens) = list_pages(s, "tags=blue&order=newest", 50).await;
    assert_eq!(pages, 3, "{tokens:?}");
    assert_eq!(got.len(), 120);
    assert_eq!(ids(&got), blue);
    assert_newest_first(&got, "tags=blue");
    assert!(tokens.iter().all(|t| t.starts_with("i1.t.")), "{tokens:?}");
    // The legacy `tag` parameter is the same query.
    let (legacy, _, _) = list_pages(s, "tag=blue&order=newest", 50).await;
    assert_eq!(legacy, got);

    // Multi-tag AND: exactly the 40, newest (index) and oldest (scan).
    let big = tagged(&f, &["blue", "big"]);
    assert_eq!(big.len(), 40);
    let (got, _, _) = list_pages(s, "tags=blue&tags=big&order=newest", 7).await;
    assert_eq!(ids(&got), big);
    assert_newest_first(&got, "blue+big");
    let (oldest, _, tokens) = list_pages(s, "tags=blue&tags=big", 7).await;
    assert_eq!(ids(&oldest), big);
    assert!(tokens.iter().all(|t| t.starts_with("c1.")), "{tokens:?}");
    let mut rev = got.clone();
    rev.reverse();
    // Oldest order is (uuid v7, partition); the fixture's runs are all uuid v7 and were
    // started one after another, so it is exactly the reverse of newest first.
    assert_eq!(ids(&oldest), ids(&rev));

    // DLQ: only failed / timed-out runs, newest first; the oldest order lists the same.
    let (got, _, _) = list_pages(s, "dlq=true&order=newest", 3).await;
    assert_eq!(ids(&got), f.dlq);
    assert_newest_first(&got, "dlq");
    for r in &got {
        let (_, v) = s.get_json(&format!("/v1/runs/{}", r.0)).await;
        assert!(
            matches!(v["status"].as_str(), Some("failed" | "timed_out")),
            "{v}"
        );
    }
    let (got, _, _) = list_pages(s, "dlq=true", 3).await;
    assert_eq!(ids(&got), f.dlq);
    // Terminal-status prefix and non-terminal status (All prefix + filter).
    let (got, _, _) = list_pages(s, "status=completed&order=newest", 100).await;
    assert_eq!(got.len(), RUNS);
    let (got, _, _) = list_pages(s, "status=running&order=newest", 100).await;
    assert!(got.is_empty());
    // A filter combined with a prefix: workflow prefix + tag filter.
    let (got, _, _) = list_pages(s, "workflow=sdk-echo&order=newest", 100).await;
    assert_eq!(got.len(), RUNS);

    // Tokens: garbage and a token of another query are 400.
    let (st, _) = s
        .get_json("/v1/runs?order=newest&page_token=i1.t.1.garbage")
        .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    let (_, p) = s.get_json("/v1/runs?order=newest&limit=5").await;
    let all_token = p["next_page_token"].as_str().expect("token").to_owned();
    let (st, v) = s
        .get_json(&format!(
            "/v1/runs?tags=blue&order=newest&page_token={all_token}"
        ))
        .await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");

    // Budget: a sparse query returns partial pages with tokens instead of scanning all.
    let ctx = s.h().control().p11().expect("p11 context");
    let req =
        |token: Option<String>, tags: Vec<String>, status: Option<ApiStatus>| ListRunsRequest {
            tags,
            status,
            order: ListOrder::Newest,
            limit: 100,
            page_token: token,
            ..Default::default()
        };
    let mut calls = 0;
    let mut token = None;
    loop {
        let page = ostro_server::control::p11_search::list_runs_indexed_with_budget(
            &ctx,
            &req(token.clone(), vec![], Some(ApiStatus::Running)),
            100,
        )
        .await
        .expect("served")
        .expect("page");
        calls += 1;
        assert!(page.runs.is_empty());
        match page.next_page_token {
            Some(t) => token = Some(t),
            None => break,
        }
        assert!(calls < 50, "budget pages never end");
    }
    assert_eq!(calls, total.div_ceil(100), "{total} rows / budget 100");
    let mut got = Vec::new();
    let mut token = None;
    let mut first = true;
    loop {
        let page = ostro_server::control::p11_search::list_runs_indexed_with_budget(
            &ctx,
            &req(token.clone(), vec!["blue".into(), "big".into()], None),
            100,
        )
        .await
        .expect("served")
        .expect("page");
        if first {
            assert!(
                page.runs.len() < 40 && page.next_page_token.is_some(),
                "budget 100 over 120 blue rows yields a partial page"
            );
            first = false;
        }
        got.extend(page.runs.into_iter().map(|r| r.run_id));
        match page.next_page_token {
            Some(t) => token = Some(t),
            None => break,
        }
    }
    let set: HashSet<String> = got.iter().cloned().collect();
    assert_eq!(set.len(), got.len());
    assert_eq!(set, big);

    // The default path is untouched: no new params -> the scan, oldest first.
    let cp = s.h().control().clone();
    let (st, rest) = s.get_json("/v1/runs?limit=50&tag=blue").await;
    assert_eq!(st, StatusCode::OK);
    let scan = ostro_server::control::list::list_runs_with_budget(
        &cp,
        ListRunsRequest {
            tag: Some("blue".into()),
            limit: 50,
            ..Default::default()
        },
        ostro_server::control::list::SCAN_BUDGET,
    )
    .await
    .expect("scan");
    let rest_ids: Vec<String> = rows(&rest).into_iter().map(|r| r.0).collect();
    let scan_ids: Vec<String> = scan.runs.into_iter().map(|r| r.run_id).collect();
    assert_eq!(rest_ids, scan_ids);
    assert_eq!(
        rest["next_page_token"].as_str(),
        scan.next_page_token.as_deref()
    );
    assert!(
        rest["next_page_token"]
            .as_str()
            .is_some_and(|t| t.starts_with("c1."))
    );

    // Index rebuilding: delete every marker (the index reports Rebuilding until a rebuild
    // writes it). The same queries are answered by the scan fallback.
    let (_, p) = s.get_json("/v1/runs?tags=blue&order=newest&limit=10").await;
    let index_token = p["next_page_token"].as_str().expect("token").to_owned();
    for h in s.partitions() {
        h.store().delete_run_index_marker().expect("delete marker");
    }
    assert!(
        index_states(s)
            .iter()
            .all(|st| *st == IndexState::Rebuilding)
    );
    let (fallback_blue, _, tokens) = list_pages(s, "tags=blue&order=newest", 50).await;
    assert_eq!(ids(&fallback_blue), blue);
    assert!(tokens.iter().all(|t| t.starts_with("c1.")), "{tokens:?}");
    let (fallback_dlq, _, _) = list_pages(s, "dlq=true&order=newest", 50).await;
    assert_eq!(ids(&fallback_dlq), f.dlq);
    let (fallback_big, _, _) = list_pages(s, "tags=blue&tags=big&order=newest", 50).await;
    assert_eq!(ids(&fallback_big), big);
    let (fallback_all, _, _) = list_pages(s, "order=newest", 100).await;
    assert_eq!(ids(&fallback_all), ids(&everything));
    // An index token cannot be resumed by the scan: 503 index_rebuilding.
    let (st, v) = s
        .get_json(&format!(
            "/v1/runs?tags=blue&order=newest&page_token={index_token}"
        ))
        .await;
    assert_eq!(st, StatusCode::SERVICE_UNAVAILABLE, "{v}");
    assert_eq!(v["error"]["details"]["reason"], "index_rebuilding", "{v}");

    // Rebuild, then the index serves again with identical results.
    for h in s.partitions() {
        h.store().start_run_index_rebuild();
        h.store().join_run_index_rebuild();
    }
    assert!(index_states(s).iter().all(|st| *st == IndexState::Ready));
    let (after, _, tokens) = list_pages(s, "tags=blue&order=newest", 50).await;
    assert!(tokens.iter().all(|t| t.starts_with("i1.t.")));
    assert_eq!(ids(&after), ids(&fallback_blue));
    assert_newest_first(&after, "after rebuild");
    let (after_all, _, _) = list_pages(s, "order=newest", 100).await;
    assert_eq!(after_all, everything);

    // History: `attempt` is either absent (apply-originated events, or a sink that does
    // not stamp it) or the non-zero `epoch << 32 | n`; never 0.
    let mut kinds: HashMap<String, usize> = HashMap::new();
    for (run, _) in f.all.iter().take(3) {
        let (st, h) = s.get_json(&format!("/v1/runs/{run}/history")).await;
        assert_eq!(st, StatusCode::OK, "{h}");
        for e in h["events"].as_array().expect("events") {
            *kinds
                .entry(e["kind"].as_str().unwrap_or("?").to_owned())
                .or_default() += 1;
            if let Some(a) = e.get("attempt") {
                assert!(a.as_u64().is_some_and(|a| a != 0), "{e}");
            }
        }
    }
    assert!(kinds.contains_key("run_started"), "{kinds:?}");

    f.s.shutdown().await;
}
