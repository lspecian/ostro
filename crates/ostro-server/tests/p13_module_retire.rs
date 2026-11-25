//! Module retirement and the per-node reference source (13-15, CLUS-07 / D-P13-20) on the
//! real single-node server with `guests/p11-rerun{,-v2}` (`pipeline` v1 / v2):
//!
//! - retire refusal while a version is active (409 `module_active` naming it), 404 for an
//!   unknown module, success after another version is activated (Module + WorkflowVersion
//!   carry ext 102), idempotent second retire (same `retired_at_ms`);
//! - a retired module refuses explicit-version starts and activations (409
//!   `module_retired`); starts on the active version keep working;
//! - `LeaderModuleRefs`: a non-terminal run references its module, a Failed (retryable)
//!   run only with `protect_resumable`, a Completed run never; an asked partition at a
//!   stale epoch is reported uncovered.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::time::Duration;

use common::{ServerOpts, TestServer};
use ostro_cluster::contracts::ModuleRefsSource;
use ostro_meta::ext::get_retired;
use ostro_partition::store::values::RunStatus;
use ostro_types::{Epoch, PartitionId, RunId};
use reqwest::StatusCode;
use serde_json::{Value, json};

const LIMIT: Duration = Duration::from_secs(90);

async fn server() -> TestServer {
    let s = TestServer::start_with(ServerOpts::default(), |c| {
        c.engine.hot_wait = Duration::from_millis(50).into();
    })
    .await;
    s.wait_ready(Duration::from_secs(30)).await;
    s
}

fn hex(sha: &[u8; 32]) -> String {
    sha.iter().map(|b| format!("{b:02x}")).collect()
}

fn reason(v: &Value) -> &str {
    v["error"]["details"]["reason"].as_str().unwrap_or("")
}

async fn delete(s: &TestServer, sha: &str) -> (StatusCode, Value) {
    let r = s
        .http
        .delete(s.url(&format!("/v1/modules/{sha}")))
        .send()
        .await
        .expect("DELETE module");
    let st = r.status();
    let b = r.bytes().await.expect("body");
    (st, serde_json::from_slice(&b).unwrap_or(Value::Null))
}

/// DELETE, waiting out the feature gate (a fresh single node finalizes level 2 once its
/// NodeVersion is advertised).
async fn retire(s: &TestServer, sha: &str) -> (StatusCode, Value) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let (st, v) = delete(s, sha).await;
        if reason(&v) != "feature_not_enabled" || tokio::time::Instant::now() >= deadline {
            return (st, v);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn activate(s: &TestServer, name: &str, version: u32) -> (StatusCode, Value) {
    let r = s
        .http
        .put(s.url(&format!("/v1/workflows/{name}/active")))
        .json(&json!({ "version": version }))
        .send()
        .await
        .expect("PUT active");
    let st = r.status();
    let b = r.bytes().await.expect("body");
    (st, serde_json::from_slice(&b).unwrap_or(Value::Null))
}

fn run_id(v: &Value) -> RunId {
    v["run_id"]
        .as_str()
        .expect("run_id")
        .parse()
        .expect("run id")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retire_refusals_idempotence_and_start_refusal() {
    let s = server().await;
    let v1 = s.deploy_guest("p11-rerun", None).await;
    let (st, v) = retire(&s, &hex(&v1)).await;
    assert_eq!(st, StatusCode::CONFLICT, "active version: {v}");
    assert_eq!(reason(&v), "module_active", "{v}");
    assert_eq!(
        v["error"]["details"]["workflows"],
        json!([{"name": "pipeline", "version": 1}])
    );
    let (st, v) = delete(&s, &"ab".repeat(32)).await;
    assert_eq!(st, StatusCode::NOT_FOUND, "{v}");

    let v2 = s.deploy_guest("p11-rerun-v2", None).await;
    let (st, v) = activate(&s, "pipeline", 2).await;
    assert!(st.is_success(), "activate v2: {st} {v}");
    // v2 is active now, so v2 is the refused one.
    let (st, v) = retire(&s, &hex(&v2)).await;
    assert_eq!(
        (st, reason(&v)),
        (StatusCode::CONFLICT, "module_active"),
        "{v}"
    );

    let (st, first) = retire(&s, &hex(&v1)).await;
    assert_eq!(st, StatusCode::OK, "{first}");
    assert_eq!(first["sha256"], json!(hex(&v1)));
    assert_eq!(
        first["versions"],
        json!([{"name": "pipeline", "version": 1}])
    );
    let at = first["retired_at_ms"].as_u64().expect("retired_at_ms");
    assert!(at > 0);
    let (st, again) = delete(&s, &hex(&v1)).await;
    assert_eq!(st, StatusCode::OK, "idempotent: {again}");
    assert_eq!(again, first, "a second retire reports the first one");

    // Both records carry ext 102 with the same time.
    let (m, w) = s.h().meta().view().read(|st| {
        (
            st.module(&v1).map(|m| m.value.ext.clone()),
            st.workflow("pipeline", 1).map(|w| w.value.ext.clone()),
        )
    });
    for ext in [m.expect("module"), w.expect("workflow")] {
        let r = get_retired(&ext).expect("decodes").expect("tag 102");
        assert_eq!(r.retired_at_ms, at);
    }
    let w2 = s
        .h()
        .meta()
        .view()
        .read(|st| st.workflow("pipeline", 2).map(|w| w.value.ext.clone()))
        .expect("v2");
    assert_eq!(get_retired(&w2).expect("decodes"), None, "v2 untouched");

    // A retired module takes no new runs or activations.
    let (st, v) = s
        .post_start("pipeline", &json!({ "input": {}, "version": 1 }))
        .await;
    assert_eq!(
        (st, reason(&v)),
        (StatusCode::CONFLICT, "module_retired"),
        "{v}"
    );
    let (st, v) = activate(&s, "pipeline", 1).await;
    assert_eq!(
        (st, reason(&v)),
        (StatusCode::CONFLICT, "module_retired"),
        "{v}"
    );
    // The active version still starts.
    let (st, v) = s.post_start("pipeline", &json!({ "input": {} })).await;
    assert_eq!(st, StatusCode::CREATED, "{v}");
    let rec = s
        .wait_record(&run_id(&v), LIMIT, |r| r.status.is_terminal())
        .await;
    assert_eq!(rec.status, RunStatus::Completed);
    assert_eq!(rec.module_sha, Some(v2));
}

/// Every `(partition, epoch)` this node leads, once all `n` are led.
async fn led_all(s: &TestServer, n: usize) -> Vec<(PartitionId, Epoch)> {
    let deadline = tokio::time::Instant::now() + LIMIT;
    loop {
        let led = s.h().node().roles().led().borrow().as_ref().clone();
        if led.len() == n {
            return led;
        }
        assert!(tokio::time::Instant::now() < deadline, "led: {led:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn module_refs_follow_run_status_and_coverage() {
    let s = server().await;
    let sha = s.deploy_guest("p11-rerun", None).await;
    let other = [0x5a; 32];
    let refs = std::sync::Arc::clone(&s.h().p13().expect("p13 ctx").module_refs);
    let led = led_all(&s, 4).await;
    let ask = |protect: bool| {
        let refs = std::sync::Arc::clone(&refs);
        let led = led.clone();
        async move {
            let a = refs
                .referenced(vec![sha, other], led, protect)
                .await
                .expect("refs");
            assert!(a.uncovered.is_empty(), "every led partition covered: {a:?}");
            a.referenced
        }
    };

    // A Completed run never references its module.
    let (st, v) = s.post_start("pipeline", &json!({ "input": {} })).await;
    assert_eq!(st, StatusCode::CREATED, "{v}");
    let rec = s
        .wait_record(&run_id(&v), LIMIT, |r| r.status.is_terminal())
        .await;
    assert_eq!(rec.status, RunStatus::Completed);
    assert!(ask(true).await.is_empty());
    assert!(ask(false).await.is_empty());

    // A Failed run (retry_run accepts it) only with protection.
    let (st, v) = s
        .post_start("pipeline", &json!({ "input": { "fail_at": 3 } }))
        .await;
    assert_eq!(st, StatusCode::CREATED, "{v}");
    let rec = s
        .wait_record(&run_id(&v), LIMIT, |r| r.status.is_terminal())
        .await;
    assert_eq!(rec.status, RunStatus::Failed);
    assert_eq!(ask(true).await, vec![sha]);
    assert!(ask(false).await.is_empty());

    // A non-terminal (sleeping) run always.
    let (st, v) = s
        .post_start(
            "pipeline",
            &json!({ "input": { "sleep_between_ms": 600_000 } }),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED, "{v}");
    let sleeping = run_id(&v);
    assert!(!s.run_record(&sleeping).status.is_terminal());
    assert_eq!(ask(false).await, vec![sha]);
    assert_eq!(ask(true).await, vec![sha]);

    // A stale epoch is uncovered; the other partitions still answer.
    let (p, e) = led
        .iter()
        .copied()
        .find(|(p, _)| *p == sleeping.partition())
        .expect("led");
    let stale = (p, Epoch(e.0 + 7));
    let rest: Vec<_> = led.iter().copied().filter(|x| x.0 != p).collect();
    let mut asked = rest.clone();
    asked.push(stale);
    let a = refs
        .referenced(vec![sha], asked, false)
        .await
        .expect("refs");
    assert_eq!(a.uncovered, vec![stale]);
    assert!(
        a.referenced.is_empty(),
        "the sleeping run's partition was not scanned: {a:?}"
    );
    // Asking nothing answers nothing.
    let a = refs
        .referenced(vec![], led.clone(), true)
        .await
        .expect("refs");
    assert!(a.referenced.is_empty() && a.uncovered.is_empty(), "{a:?}");
}
