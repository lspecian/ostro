//! API-04 (history + list) against the real server (plan 04-18): decoded journal history
//! with paging, and filtered run listing across P=16 partitions with page tokens and a
//! scan budget, over REST and gRPC with identical results.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::collections::HashSet;
use std::time::{Duration, Instant};

use common::{ServerOpts, TestServer, guest_wasm, now_ms};
use ostro_api::domain::{ListRunsRequest, RunStatus};
use ostro_proto::api::v1 as pb;
use ostro_proto::api::v1::runs_client::RunsClient;
use reqwest::StatusCode;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::time::sleep;
use tonic::transport::Channel;

const LIMIT: Duration = Duration::from_secs(60);

fn sha_hex(b: &[u8]) -> String {
    Sha256::digest(b)
        .iter()
        .map(|x| format!("{x:02x}"))
        .collect()
}

async fn server(partitions: u16) -> TestServer {
    let s = TestServer::start(ServerOpts {
        partitions,
        ..ServerOpts::default()
    })
    .await;
    s.wait_ready(Duration::from_secs(10)).await;
    s
}

async fn deploy(s: &TestServer, guest: &str) {
    let wasm = guest_wasm(guest);
    let sha = sha_hex(&wasm);
    let r = s
        .http
        .put(s.url(&format!("/v1/modules/{sha}")))
        .body(wasm)
        .send()
        .await
        .expect("PUT module");
    assert!(r.status().is_success(), "deploy {guest}: {}", r.status());
}

async fn start(s: &TestServer, workflow: &str, body: Value) -> String {
    let r = s
        .http
        .post(s.url(&format!("/v1/workflows/{workflow}/runs")))
        .json(&body)
        .send()
        .await
        .expect("POST run");
    assert_eq!(r.status(), StatusCode::CREATED);
    let v: Value = r.json().await.expect("json");
    v["run_id"].as_str().expect("run_id").to_owned()
}

async fn get_json(s: &TestServer, path: &str) -> (StatusCode, Value) {
    let r = s.http.get(s.url(path)).send().await.expect("GET");
    let st = r.status();
    (st, r.json().await.expect("json"))
}

async fn wait_completed(s: &TestServer, ids: &[String]) {
    let t0 = Instant::now();
    for id in ids {
        loop {
            let (st, run) = get_json(s, &format!("/v1/runs/{id}")).await;
            assert_eq!(st, StatusCode::OK);
            match run["status"].as_str() {
                Some("completed") => break,
                Some("failed" | "cancelled") => panic!("run failed: {run}"),
                _ => {}
            }
            assert!(t0.elapsed() < LIMIT, "not completed in time: {run}");
            sleep(Duration::from_millis(20)).await;
        }
    }
}

// ---------------------------------------------------------------------------------
// History
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn history_rest_and_grpc() {
    let mut s = server(4).await;
    deploy(&s, "sdk-basic").await;
    let id = start(&s, "sdk-basic", json!({"input": {"name": "h", "n": 3}})).await;
    wait_completed(&s, std::slice::from_ref(&id)).await;

    let (st, page) = get_json(&s, &format!("/v1/runs/{id}/history")).await;
    assert_eq!(st, StatusCode::OK, "{page}");
    let events = page["events"].as_array().expect("events").clone();
    assert_eq!(page["next_from_seq"], Value::Null);
    assert!(events.len() >= 6, "{page}");
    for (i, e) in events.iter().enumerate() {
        assert_eq!(e["seq"], i as u64, "contiguous seqs: {e}");
        assert!(e["ts_ms"].as_u64().is_some_and(|t| t > 0), "{e}");
        assert!(e["ts"].as_str().is_some_and(|t| t.ends_with('Z')), "{e}");
        assert!(e["summary"].as_str().is_some_and(|t| !t.is_empty()), "{e}");
    }
    assert_eq!(events[0]["kind"], "run_started");
    assert_eq!(events[0]["details"]["workflow"], "sdk-basic");
    assert_eq!(events[0]["details"]["input"], json!({"name": "h", "n": 3}));
    let last = events.last().expect("last");
    assert_eq!(last["kind"], "run_completed");
    assert_eq!(
        last["details"]["output"],
        json!({"greeting": "hello, h", "sum": 6, "steps": 2})
    );
    let steps: Vec<&Value> = events
        .iter()
        .filter(|e| e["kind"] == "step_completed")
        .collect();
    assert_eq!(steps.len(), 2, "{page}");
    assert_eq!(steps[0]["details"]["outcome"]["ok"], "hello, h", "{page}");
    assert_eq!(steps[1]["details"]["outcome"]["ok"], 6, "{page}");
    for st in &steps {
        assert!(st["details"]["name"].is_string(), "{st}");
    }
    let status = events
        .iter()
        .find(|e| e["kind"] == "status_published")
        .expect("status_published");
    assert_eq!(status["details"]["status"], json!({"stage": "done"}));
    assert!(events.iter().any(|e| e["kind"] == "timer_started"));
    assert!(events.iter().any(|e| e["kind"] == "timer_fired"));

    // Paging: limit 3 walks the same events exactly once.
    let mut paged = Vec::new();
    let mut from = 0u64;
    loop {
        let (st, p) = get_json(
            &s,
            &format!("/v1/runs/{id}/history?from_seq={from}&limit=3"),
        )
        .await;
        assert_eq!(st, StatusCode::OK);
        let evs = p["events"].as_array().expect("events");
        assert!(evs.len() <= 3);
        paged.extend(evs.iter().cloned());
        match p["next_from_seq"].as_u64() {
            Some(n) => {
                assert_eq!(n, from + 3);
                from = n;
            }
            None => break,
        }
    }
    assert_eq!(paged, events);

    // gRPC: identical events (ts_ms, kind, summary, details).
    let mut runs = RunsClient::new(s.channel().await);
    let g = runs
        .history(pb::RunHistoryRequest {
            run_id: id.clone(),
            from_seq: 0,
            limit: 0,
        })
        .await
        .expect("history")
        .into_inner();
    assert_eq!(g.next_from_seq, None);
    assert_eq!(g.events.len(), events.len());
    for (ge, re) in g.events.iter().zip(&events) {
        assert_eq!(re["seq"], ge.seq);
        assert_eq!(re["kind"], ge.kind.as_str());
        assert_eq!(re["ts_ms"], ge.ts_ms);
        assert_eq!(re["summary"], ge.summary.as_str());
        let d: Value = serde_json::from_slice(&ge.details_json).expect("details json");
        assert_eq!(re["details"], d);
    }
    let g2 = runs
        .history(pb::RunHistoryRequest {
            run_id: id.clone(),
            from_seq: 2,
            limit: 2,
        })
        .await
        .expect("history page")
        .into_inner();
    assert_eq!(
        g2.events.iter().map(|e| e.seq).collect::<Vec<_>>(),
        vec![2, 3]
    );
    assert_eq!(g2.next_from_seq, Some(4));

    // Unknown run 404, malformed id 400.
    let mut other: Vec<char> = id.chars().collect();
    let l = other.len() - 1;
    other[l] = if other[l] == '0' { '4' } else { '0' };
    let other: String = other.into_iter().collect();
    let (st, _) = get_json(&s, &format!("/v1/runs/{other}/history")).await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    let (st, _) = get_json(&s, "/v1/runs/nope/history").await;
    assert_eq!(st, StatusCode::BAD_REQUEST);

    s.shutdown().await;
}

// ---------------------------------------------------------------------------------
// List
// ---------------------------------------------------------------------------------

const WORKFLOWS: [&str; 3] = ["sdk-basic", "sdk-echo", "sdk-native"];

fn input(workflow: &str, i: usize) -> Value {
    match workflow {
        "sdk-basic" => json!({"name": format!("r{i}"), "n": 1}),
        _ => json!({"i": i}),
    }
}

struct Started {
    id: String,
    workflow: &'static str,
    queue: &'static str,
    tags: Vec<String>,
}

/// Every page of a REST listing with `query` (without limit/page_token) and `limit`.
async fn rest_list_all(s: &TestServer, query: &str, limit: u32) -> (Vec<String>, usize) {
    let mut ids = Vec::new();
    let mut pages = 0;
    let mut token: Option<String> = None;
    loop {
        let mut url = format!("/v1/runs?limit={limit}");
        if !query.is_empty() {
            url.push('&');
            url.push_str(query);
        }
        if let Some(t) = &token {
            url.push_str(&format!("&page_token={t}"));
        }
        let (st, page) = get_json(s, &url).await;
        assert_eq!(st, StatusCode::OK, "{url}: {page}");
        pages += 1;
        let runs = page["runs"].as_array().expect("runs");
        assert!(runs.len() <= limit as usize);
        for r in runs {
            ids.push(r["run_id"].as_str().expect("id").to_owned());
            assert!(r["created_at"].as_str().is_some_and(|t| t.ends_with('Z')));
        }
        match page["next_page_token"].as_str() {
            Some(t) => token = Some(t.to_owned()),
            None => break,
        }
        assert!(pages < 1000, "runaway pagination");
    }
    let unique: HashSet<&String> = ids.iter().collect();
    assert_eq!(unique.len(), ids.len(), "duplicates in {query}");
    (ids, pages)
}

async fn grpc_list_all(runs: &mut RunsClient<Channel>, req: pb::ListRunsRequest) -> Vec<String> {
    let mut ids = Vec::new();
    let mut req = req;
    loop {
        let page = runs.list(req.clone()).await.expect("list").into_inner();
        ids.extend(page.runs.iter().map(|r| r.run_id.clone()));
        match page.next_page_token {
            Some(t) => req.page_token = Some(t),
            None => return ids,
        }
    }
}

fn expected(all: &[Started], f: impl Fn(&Started) -> bool) -> Vec<String> {
    all.iter().filter(|r| f(r)).map(|r| r.id.clone()).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn list_filters_pagination_and_budget() {
    let mut s = server(16).await;
    deploy(&s, "sdk-basic").await; // sdk-basic + sdk-echo
    deploy(&s, "sdk-native").await;

    let mut all = Vec::new();
    let mut boundary = 0;
    for i in 0..60usize {
        if i == 30 {
            sleep(Duration::from_millis(50)).await;
            boundary = now_ms();
            sleep(Duration::from_millis(50)).await;
        }
        let workflow = WORKFLOWS[i % 3];
        let queue = if i % 2 == 0 { "qa" } else { "qb" };
        let tags = vec!["all".to_owned(), format!("m5-{}", i % 5)];
        let id = start(
            &s,
            workflow,
            json!({"input": input(workflow, i), "queue": queue, "tags": tags}),
        )
        .await;
        all.push(Started {
            id,
            workflow,
            queue,
            tags,
        });
    }
    let ids: Vec<String> = all.iter().map(|r| r.id.clone()).collect();
    let partitions: HashSet<u16> = ids
        .iter()
        .map(|i| i.parse::<ostro_types::RunId>().expect("id").partition().0)
        .collect();
    assert!(partitions.len() > 4, "runs should spread over partitions");
    wait_completed(&s, &ids).await;

    // Unfiltered, limit 7: every run exactly once, oldest first (= start order).
    let (listed, pages) = rest_list_all(&s, "", 7).await;
    assert_eq!(listed, ids);
    assert!(pages >= 9, "{pages} pages");

    let boundary_rfc = jiff::Timestamp::from_millisecond(i64::try_from(boundary).expect("ms"))
        .expect("ts")
        .to_string();
    let cases: Vec<(String, Vec<String>, pb::ListRunsRequest)> = vec![
        (
            "workflow=sdk-echo".into(),
            expected(&all, |r| r.workflow == "sdk-echo"),
            pb::ListRunsRequest {
                workflow: Some("sdk-echo".into()),
                ..Default::default()
            },
        ),
        (
            "status=completed".into(),
            ids.clone(),
            pb::ListRunsRequest {
                status: Some(pb::RunStatus::Completed as i32),
                ..Default::default()
            },
        ),
        (
            "status=running".into(),
            vec![],
            pb::ListRunsRequest {
                status: Some(pb::RunStatus::Running as i32),
                ..Default::default()
            },
        ),
        (
            "queue=qa".into(),
            expected(&all, |r| r.queue == "qa"),
            pb::ListRunsRequest {
                queue: Some("qa".into()),
                ..Default::default()
            },
        ),
        (
            "tag=m5-2".into(),
            expected(&all, |r| r.tags.iter().any(|t| t == "m5-2")),
            pb::ListRunsRequest {
                tag: Some("m5-2".into()),
                ..Default::default()
            },
        ),
        (
            format!("created_after={boundary_rfc}"),
            ids[30..].to_vec(),
            pb::ListRunsRequest {
                created_after_ms: Some(boundary),
                ..Default::default()
            },
        ),
        (
            format!("created_before={boundary_rfc}"),
            ids[..30].to_vec(),
            pb::ListRunsRequest {
                created_before_ms: Some(boundary),
                ..Default::default()
            },
        ),
        (
            "workflow=sdk-basic&queue=qb".into(),
            expected(&all, |r| r.workflow == "sdk-basic" && r.queue == "qb"),
            pb::ListRunsRequest {
                workflow: Some("sdk-basic".into()),
                queue: Some("qb".into()),
                ..Default::default()
            },
        ),
    ];
    let mut runs = RunsClient::new(s.channel().await);
    for (query, want, greq) in cases {
        let (got, _) = rest_list_all(&s, &query, 7).await;
        assert_eq!(got, want, "REST {query}");
        let g = grpc_list_all(&mut runs, pb::ListRunsRequest { limit: 7, ..greq }).await;
        assert_eq!(g, want, "gRPC {query}");
    }

    // REST and gRPC rows agree field by field.
    let (_, rest_page) = get_json(&s, "/v1/runs?limit=5&workflow=sdk-native").await;
    let g = runs
        .list(pb::ListRunsRequest {
            workflow: Some("sdk-native".into()),
            limit: 5,
            ..Default::default()
        })
        .await
        .expect("list")
        .into_inner();
    assert_eq!(
        rest_page["next_page_token"],
        g.next_page_token.clone().expect("more")
    );
    for (r, gr) in rest_page["runs"]
        .as_array()
        .expect("runs")
        .iter()
        .zip(&g.runs)
    {
        assert_eq!(r["run_id"], gr.run_id.as_str());
        assert_eq!(r["workflow"], gr.workflow.as_str());
        assert_eq!(r["version"], gr.version);
        assert_eq!(r["queue"], gr.queue.as_str());
        assert_eq!(r["status"], "completed");
        assert_eq!(gr.status, pb::RunStatus::Completed as i32);
        assert_eq!(r["created_at_ms"], gr.created_at_ms);
        assert_eq!(r["tags"], json!(gr.tags));
    }

    // A token from page 1 still continues correctly after new runs are started.
    let (_, p1) = get_json(&s, "/v1/runs?limit=7&workflow=sdk-echo").await;
    let mut seen: Vec<String> = p1["runs"]
        .as_array()
        .expect("runs")
        .iter()
        .map(|r| r["run_id"].as_str().expect("id").to_owned())
        .collect();
    let mut token = p1["next_page_token"].as_str().expect("token").to_owned();
    let mut fresh = Vec::new();
    for i in 0..3 {
        fresh.push(start(&s, "sdk-echo", json!({"input": {"late": i}})).await);
    }
    loop {
        let (st, p) = get_json(
            &s,
            &format!("/v1/runs?limit=7&workflow=sdk-echo&page_token={token}"),
        )
        .await;
        assert_eq!(st, StatusCode::OK, "{p}");
        seen.extend(
            p["runs"]
                .as_array()
                .expect("runs")
                .iter()
                .map(|r| r["run_id"].as_str().expect("id").to_owned()),
        );
        match p["next_page_token"].as_str() {
            Some(t) => token = t.to_owned(),
            None => break,
        }
    }
    let mut want = expected(&all, |r| r.workflow == "sdk-echo");
    want.extend(fresh.iter().cloned());
    assert_eq!(seen, want);

    // Garbage page token: 400 over both transports.
    let (st, _) = get_json(&s, "/v1/runs?page_token=garbage").await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    let e = runs
        .list(pb::ListRunsRequest {
            page_token: Some("c1.nope".into()),
            ..Default::default()
        })
        .await
        .expect_err("bad token");
    assert_eq!(e.code(), tonic::Code::InvalidArgument);

    // Scan budget: a filter that matches nothing returns empty pages with tokens until
    // the scan is exhausted (budget 25 over 63 runs = 3 calls).
    let cp = s.h().control().clone();
    let mut calls = 0;
    let mut token = None;
    loop {
        let page = ostro_server::control::list::list_runs_with_budget(
            &cp,
            ListRunsRequest {
                workflow: Some("no-such-workflow".into()),
                limit: 10,
                page_token: token.clone(),
                ..Default::default()
            },
            25,
        )
        .await
        .expect("list");
        calls += 1;
        assert!(page.runs.is_empty());
        match page.next_page_token {
            Some(t) => token = Some(t),
            None => break,
        }
        assert!(calls < 10, "budget pages never end");
    }
    assert_eq!(calls, 3);
    // Budget-limited pages of a matching filter still yield every match exactly once.
    let mut got = Vec::new();
    let mut token = None;
    loop {
        let page = ostro_server::control::list::list_runs_with_budget(
            &cp,
            ListRunsRequest {
                status: Some(RunStatus::Completed),
                tag: Some("m5-0".into()),
                limit: 100,
                page_token: token.clone(),
                ..Default::default()
            },
            10,
        )
        .await
        .expect("list");
        got.extend(page.runs.into_iter().map(|r| r.run_id));
        match page.next_page_token {
            Some(t) => token = Some(t),
            None => break,
        }
    }
    assert_eq!(got, expected(&all, |r| r.tags.iter().any(|t| t == "m5-0")));

    drop(cp);
    s.shutdown().await;
}
