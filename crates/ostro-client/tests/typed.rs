//! Typed surface (SDK-07, 10-09).
//!
//! - Offline: every typed method calls a `(method, path)` listed in `typed::ROUTES`, and every
//!   listed route is called by some method (a recording loopback server).
//! - Spawned `ostro dev --ephemeral` (the binary from `OSTRO_BIN` or
//!   `${CARGO_TARGET_DIR:-target}/debug/ostro`; build it with `cargo build -p ostro`):
//!   upload, activate, start, get, list, history, result, signal, cancel, terminate,
//!   ClientError for an unknown run, and the dev clock / replay routes, adapting to what the
//!   server supports (409 `time_skipping_disabled` until 10-10, replay 501 until 10-11).

mod common_p10;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::extract::{Request, State};
use ostro_client::typed::{PolicyTarget, ROUTES, ReplaySelect, RunFilter};
use ostro_client::{ClientError, StartRun, TypedClient};
use serde_json::{Value, json};

type Seen = Arc<Mutex<Vec<(String, String)>>>;

async fn record(State(seen): State<Seen>, req: Request) -> axum::Json<Value> {
    seen.lock().unwrap_or_else(|e| e.into_inner()).push((
        req.method().as_str().to_owned(),
        req.uri().path().to_owned(),
    ));
    axum::Json(json!({}))
}

/// `path` (concrete) matches `template` segment by segment; `{..}` matches one segment.
fn matches(template: &str, path: &str) -> bool {
    let t: Vec<&str> = template.split('/').collect();
    let p: Vec<&str> = path.split('/').collect();
    t.len() == p.len()
        && t.iter()
            .zip(&p)
            .all(|(t, p)| (t.starts_with('{') && t.ends_with('}') && !p.is_empty()) || t == p)
}

#[tokio::test]
async fn every_typed_method_calls_a_listed_route() {
    let seen: Seen = Arc::default();
    let app = Router::new().fallback(record).with_state(Arc::clone(&seen));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    let c = TypedClient::new(&format!("http://{addr}"), Duration::from_secs(5)).expect("client");

    // Answers are `{}`: most typed decodes fail, which is fine; only the requests matter.
    let sha = "ab".repeat(32);
    let _ = c.upload_module(b"\0asm".to_vec(), Some("m"), None).await;
    let _ = c.activate("w", 1).await;
    let _ = c.workflows().await;
    let _ = c.start_run(&StartRun::new("w", json!({}))).await;
    let _ = c.list_runs(&RunFilter::default()).await;
    let _ = c.get_run("r").await;
    let _ = c.history("r", Some(1), Some(10)).await;
    let _ = c.result("r", None).await;
    let _ = c.signal("r", "s", json!(1), Some("q")).await;
    let _ = c.cancel("r", None).await;
    let _ = c.terminate("r", Some("why")).await;
    let _ = c.retry("r").await;
    let _ = c.complete_token("t", json!(null)).await;
    let _ = c.dev_time().await;
    let _ = c.dev_skip_by(Duration::from_secs(60)).await;
    let _ = c.dev_skip_to(1).await;
    let _ = c
        .replay(&sha, &ReplaySelect::Runs(vec!["r".into()]), false)
        .await;
    let _ = c.rerun("r", &json!({})).await;
    let _ = c.rerun_cuts("r").await;
    let _ = c.dlq(&[]).await;
    let _ = c.requeue_dlq("r", &json!({})).await;
    let _ = c.dismiss_dlq("r", &json!({})).await;
    let _ = c.create_bulk(&json!({})).await;
    let _ = c.bulk_jobs().await;
    let _ = c.bulk_job("j").await;
    let _ = c.cancel_bulk("j").await;
    let _ = c.limits().await;
    let _ = c.modules(Some(1), None).await;
    let _ = c.queues().await;
    let _ = c.partitions().await;
    let _ = c.failure_policy(PolicyTarget::Workflow, "w").await;
    let _ = c
        .set_failure_policy(PolicyTarget::Workflow, "w", &json!({}), Some("v"))
        .await;
    let _ = c.failure_policy(PolicyTarget::Queue, "q").await;
    let _ = c
        .set_failure_policy(PolicyTarget::Queue, "q", &json!({}), None)
        .await;

    let seen = seen.lock().unwrap_or_else(|e| e.into_inner()).clone();
    let unlisted: Vec<_> = seen
        .iter()
        .filter(|(m, p)| !ROUTES.iter().any(|(rm, rt)| rm == m && matches(rt, p)))
        .collect();
    assert!(
        unlisted.is_empty(),
        "requests not in typed::ROUTES: {unlisted:?}"
    );
    let unused: Vec<_> = ROUTES
        .iter()
        .filter(|(rm, rt)| !seen.iter().any(|(m, p)| rm == m && matches(rt, p)))
        .collect();
    assert!(
        unused.is_empty(),
        "typed::ROUTES entries no method calls: {unused:?}"
    );
}

fn assert_status(e: &ClientError, status: u16) {
    assert_eq!(e.status, Some(status), "{e:?}");
    assert!(!e.code.is_empty(), "{e:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_calls_against_ostro_dev() {
    let basic = common_p10::guest_wasm("sdk-basic");
    let signals = common_p10::guest_wasm("signals");
    let srv = common_p10::DevServer::spawn(&[]);
    assert!(srv.ready.ephemeral, "{:?}", srv.ready);
    let c = TypedClient::new(&srv.url(), Duration::from_secs(30)).expect("client");
    let wait = Some(Duration::from_secs(30));

    // Modules and workflows.
    let up = c
        .upload_module(basic.clone(), Some("basic"), None)
        .await
        .expect("upload");
    assert!(up.created, "{up:?}");
    assert!(
        up.module
            .workflows
            .iter()
            .any(|w| w.name == "sdk-basic" && w.version == 1),
        "{up:?}"
    );
    let again = c.upload_module(basic, None, None).await.expect("re-upload");
    assert!(!again.created);
    let wf = c.activate("sdk-basic", 1).await.expect("activate");
    assert_eq!(wf.name, "sdk-basic");
    assert_eq!(wf.active_version, Some(1), "{wf:?}");
    let all = c.workflows().await.expect("workflows");
    assert!(all.iter().any(|w| w.name == "sdk-basic"), "{all:?}");

    // Start, get, result, history, list.
    let mut req = StartRun::new("sdk-basic", json!({"name": "typed", "n": 3}));
    req.tags = vec!["typed".into()];
    let started = c.start_run(&req).await.expect("start");
    assert!(started.created);
    let run = c.get_run(&started.run_id).await.expect("get");
    assert_eq!(run.run_id, started.run_id);
    assert_eq!(run.workflow, "sdk-basic");
    assert_eq!(run.version, 1);
    assert_eq!(run.module_sha256, up.module.sha256);
    let res = c.result(&started.run_id, wait).await.expect("result");
    assert!(res.completed, "{res:?}");
    assert_eq!(res.status.as_deref(), Some("completed"), "{res:?}");
    assert!(res.output.is_some(), "{res:?}");
    let hist = c
        .history(&started.run_id, None, None)
        .await
        .expect("history");
    assert!(
        hist.events.iter().any(|e| e["kind"] == "step_completed"),
        "no step_completed event: {:?}",
        hist.events
    );
    let page = c
        .list_runs(&RunFilter {
            workflow: Some("sdk-basic".into()),
            tags: vec!["typed".into()],
            ..RunFilter::default()
        })
        .await
        .expect("list");
    assert!(
        page.runs.iter().any(|r| r.run_id == started.run_id),
        "{page:?}"
    );

    // Signal a waiting run; its result is the payload.
    c.upload_module(signals, Some("signals"), None)
        .await
        .expect("upload signals");
    let s = c
        .start_run(&StartRun::new("signals", json!({"mode": "long_wait"})))
        .await
        .expect("start signals");
    let sig = c
        .signal(&s.run_id, "x", json!({"v": 1}), Some("req-1"))
        .await
        .expect("signal");
    assert!(sig.accepted && !sig.duplicate, "{sig:?}");
    let res = c.result(&s.run_id, wait).await.expect("signals result");
    assert!(res.completed, "{res:?}");
    assert_eq!(res.output, Some(json!({"v": 1})), "{res:?}");

    // Cancel and terminate waiting runs.
    let a = c
        .start_run(&StartRun::new("signals", json!({"mode": "long_wait"})))
        .await
        .expect("start a");
    let out = c
        .cancel(&a.run_id, Some("typed test"))
        .await
        .expect("cancel");
    assert!(!out.already_requested, "{out:?}");
    let res = c.result(&a.run_id, wait).await.expect("cancelled result");
    assert_eq!(res.status.as_deref(), Some("cancelled"), "{res:?}");
    let b = c
        .start_run(&StartRun::new("signals", json!({"mode": "long_wait"})))
        .await
        .expect("start b");
    let out = c.terminate(&b.run_id, None).await.expect("terminate");
    assert!(!out.status.is_empty(), "{out:?}");

    // ClientError from the REST error body: a malformed id is 400 invalid_argument, a
    // well-formed unknown one (one character of a real id changed) 404 not_found.
    let e = c
        .get_run("no-such-run")
        .await
        .expect_err("malformed run id");
    assert_status(&e, 400);
    assert_eq!(e.code, "invalid_argument", "{e:?}");
    let mut unknown: Vec<char> = started.run_id.chars().collect();
    unknown[10] = if unknown[10] == '0' { '1' } else { '0' };
    let unknown: String = unknown.into_iter().collect();
    let e = c.get_run(&unknown).await.expect_err("unknown run");
    assert_status(&e, 404);
    assert_eq!(e.code, "not_found", "{e:?}");

    // Dev clock: off on a plain `ostro dev --ephemeral` (409 time_skipping_disabled).
    match c.dev_time().await {
        Err(e) => {
            assert_status(&e, 409);
            assert_eq!(e.reason(), Some("time_skipping_disabled"), "{e:?}");
            let e = c
                .dev_skip_by(Duration::from_secs(3600))
                .await
                .expect_err("skip off");
            assert_eq!(e.reason(), Some("time_skipping_disabled"), "{e:?}");
            println!("dev time: off (409 time_skipping_disabled)");
        }
        Ok(t) => {
            // A server that reports the clock must say whether skipping is on.
            println!("dev time: {t:?}");
            assert!(
                ["off", "auto", "manual"].contains(&t.mode.as_str()),
                "{t:?}"
            );
        }
    }

    // Replay: 501 until 10-11 fills the server side; afterwards a report.
    let sel = ReplaySelect::Runs(vec![started.run_id.clone()]);
    match c.replay(&up.module.sha256, &sel, false).await {
        Ok(r) => {
            assert_eq!(r.module_sha256, up.module.sha256);
            assert_eq!(r.verdicts.len(), 1, "{r:?}");
            println!("replay: {:?}", r.summary);
        }
        Err(e) => {
            assert_status(&e, 501);
            assert_eq!(e.code, "unimplemented", "{e:?}");
            println!("replay: pending (501 unimplemented, 10-11)");
        }
    }
    drop(srv);
}

/// Once `ostro dev` accepts `--time-skipping` (10-10), the dev clock answers and skips.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dev_clock_with_time_skipping_when_supported() {
    let help = common_p10::command()
        .args(["dev", "--help"])
        .output()
        .expect("ostro dev --help");
    if !String::from_utf8_lossy(&help.stdout).contains("--time-skipping") {
        println!("dev clock: pending (`ostro dev --time-skipping` arrives with 10-10)");
        return;
    }
    let srv = common_p10::DevServer::spawn(&["--time-skipping"]);
    let c = TypedClient::new(&srv.url(), Duration::from_secs(30)).expect("client");
    let t = c.dev_time().await.expect("dev time with --time-skipping");
    assert_ne!(t.mode, "off", "{t:?}");
    let s = c
        .dev_skip_by(Duration::from_secs(3600))
        .await
        .expect("skip");
    assert!(s.skipped_ms >= 3_600_000, "{s:?}");
    assert!(s.offset_ms >= t.offset_ms + 3_600_000, "{s:?} vs {t:?}");
}
