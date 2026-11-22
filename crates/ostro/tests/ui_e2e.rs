//! PHASE-11 GATE (plan 11-18): ROADMAP Phase 11 success criteria 4 and 5 over HTTP against
//! the spawned `ostro` binary (UI-01..03). No browser and no JavaScript: requests are sent
//! the way htmx and a no-JS form send them, and every state change is observed through
//! `/v1`. Real-browser checks (rendering, swaps, keyboard, CSP console) are human_needed
//! (11-VALIDATION.md "Manual-Only").
//!
//! - `ui_pages_and_actions_change_real_state` (`ostro dev`): `/` redirects to `/ui/`;
//!   every page and fragment renders real data with the D-P11-15 headers; terminate,
//!   cancel, retry, pause + resume (run and queue), rerun-from-step (preview, then
//!   submit), DLQ requeue and bulk create + cancel change real state through same-origin
//!   requests carrying the CSRF token; a cross-site post and a foreign `Host` are 403.
//! - `ui_inventory_views_on_a_live_server_with_a_worker` (`ostro server`, TLS, one real
//!   `ostro worker`, a cron schedule): all six inventory views show live data.

mod common;
#[path = "common/p11.rs"]
mod p11;
#[path = "common/p7.rs"]
mod p7;

use std::time::Duration;

use common::*;
use p11::*;
use reqwest::StatusCode;
use serde_json::{Value, json};

const PIPELINE: &str = "pipeline";
/// Input of a run that parks in a one-hour durable sleep after s2.
const PARKED: &str = r#"{"sleep_between_ms":3600000}"#;

async fn status_of(n: &P11Node, id: &str) -> Value {
    n.api.run(id).await
}

async fn wait_status(n: &P11Node, id: &str, want: &str) -> Value {
    n.api.wait_run(id, T, |r| r["status"] == want).await
}

/// The new run id of an htmx action answered with `HX-Redirect: /ui/runs/<id>`.
fn redirected_run(r: &UiResp, what: &str) -> String {
    assert_eq!(r.status, StatusCode::OK, "{what}: {}", r.body);
    r.header("hx-redirect")
        .and_then(|l| l.strip_prefix("/ui/runs/"))
        .unwrap_or_else(|| panic!("{what}: no HX-Redirect to a run: {:?}", r.headers))
        .to_owned()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ui_pages_and_actions_change_real_state() {
    let _g = SERIAL.lock().await;
    let mut n = P11Node::start(&Profile::Dev, &[]).await;
    let base = format!("http://{}", n.addr);

    // `/` -> 303 /ui/ (UI enabled on the loopback dev listener, D-P11-17).
    let raw = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let r = raw.get(format!("{base}/")).send().await.unwrap();
    assert_eq!(r.status(), StatusCode::SEE_OTHER);
    assert_eq!(r.headers()["location"], "/ui/");
    let ui = UiClient::connect(&base, None).await;

    // Real runs.
    n.deploy("p11-rerun");
    let (failed, st) = n.run_wait(PIPELINE, r#"{"fail_at":3}"#, &["ui"]);
    assert_eq!(st, "failed");
    let (failed2, _) = n.run_wait(PIPELINE, r#"{"fail_at":2}"#, &["ui"]);
    let (completed, _) = n.run_wait(PIPELINE, "{}", &["ui"]);
    let parked: Vec<String> = (0..3)
        .map(|_| n.run_nowait(PIPELINE, PARKED, &["ui-parked"]))
        .collect();
    for id in &parked {
        poll("parked run in its sleep", T, || async {
            let h = n.api.history(id).await;
            (!events_of(&h, "timer_started").is_empty()).then_some(())
        })
        .await;
    }
    let sha = status_of(&n, &failed).await["module_sha256"]
        .as_str()
        .unwrap()
        .to_owned();

    // SC4: every page renders real data with the security headers.
    for (path, needles) in [
        ("/ui/", vec![failed.as_str(), completed.as_str()]),
        ("/ui/runs", vec![failed.as_str(), parked[0].as_str()]),
        ("/ui/runs?status=failed", vec![failed.as_str()]),
        (
            &format!("/ui/runs/{failed}") as &str,
            vec![failed.as_str(), PIPELINE, "s3 fails"],
        ),
        (
            &format!("/ui/runs/{completed}"),
            vec![completed.as_str(), "completed"],
        ),
        ("/ui/dlq", vec![failed.as_str(), failed2.as_str()]),
        ("/ui/bulk", vec!["Bulk"]),
        ("/ui/modules", vec![sha.as_str()]),
        ("/ui/workflows", vec![PIPELINE]),
        ("/ui/queues", vec!["default"]),
        ("/ui/schedules", vec![]),
        ("/ui/workers", vec![]),
        (
            "/ui/partitions",
            vec!["class=\"inventory partitions\"", "badge healthy"],
        ),
        (
            &format!("/ui/runs/{failed}/rerun"),
            vec![failed.as_str(), "name=\"step\""],
        ),
    ] {
        let r = ui.get(path).await;
        r.assert_page(path);
        for needle in needles {
            assert!(
                r.body.contains(needle),
                "{path} lacks {needle:?}:\n{}",
                r.body
            );
        }
    }
    for path in [
        format!("/ui/runs/{failed}/timeline"),
        format!("/ui/runs/{failed}/logs"),
        format!("/ui/runs/{failed}/status"),
        "/ui/partitions/fragment".to_owned(),
    ] {
        let r = ui.get_fragment(&path).await;
        r.assert_page(&path);
    }
    let tl = ui
        .get_fragment(&format!("/ui/runs/{failed}/timeline"))
        .await;
    for s in ["s1", "s2", "s3"] {
        assert!(tl.body.contains(s), "timeline lacks {s}:\n{}", tl.body);
    }
    let asset = ui.get("/ui/assets/vendor/htmx-2.0.11.min.js").await;
    asset.assert_page("htmx asset");

    // Security: a cross-site post and a foreign Host are refused, nothing changes.
    let r = ui
        .post(
            Via::CrossSite,
            &format!("/ui/runs/{}/terminate", parked[0]),
            &[("reason", "x")],
        )
        .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.body);
    r.assert_headers("cross-site 403");
    let r = ui.get_with_host("/ui/", "evil.example").await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.body);
    assert!(!is_terminal(
        status_of(&n, &parked[0]).await["status"].as_str().unwrap()
    ));
    // A mutation without any token is refused too.
    let anon = raw
        .post(format!("{base}/ui/runs/{}/cancel", parked[0]))
        .header("sec-fetch-site", "same-origin")
        .header("content-type", "application/x-www-form-urlencoded")
        .body("")
        .send()
        .await
        .unwrap();
    assert_eq!(anon.status(), StatusCode::FORBIDDEN);

    // SC5: every action changes real state.
    // terminate
    let r = ui
        .post(
            Via::Htmx,
            &format!("/ui/runs/{}/terminate", parked[0]),
            &[("reason", "ui-e2e")],
        )
        .await;
    assert_eq!(r.status, StatusCode::OK, "terminate: {}", r.body);
    wait_status(&n, &parked[0], "terminated").await;
    // cancel (graceful: the guest is woken and the run ends cancelled)
    let r = ui
        .post(
            Via::Form,
            &format!("/ui/runs/{}/cancel", parked[1]),
            &[("reason", "ui-e2e")],
        )
        .await;
    assert_eq!(r.status, StatusCode::SEE_OTHER, "cancel form: {}", r.body);
    wait_status(&n, &parked[1], "cancelled").await;
    // pause + resume a run
    let r = ui
        .post(Via::Htmx, &format!("/ui/runs/{}/pause", parked[2]), &[])
        .await;
    assert_eq!(r.status, StatusCode::OK, "pause: {}", r.body);
    n.api
        .wait_run(&parked[2], T, |r| r["pause"].is_object())
        .await;
    let r = ui
        .post(Via::Htmx, &format!("/ui/runs/{}/resume", parked[2]), &[])
        .await;
    assert_eq!(r.status, StatusCode::OK, "resume: {}", r.body);
    n.api
        .wait_run(&parked[2], T, |r| r["pause"].is_null())
        .await;
    // pause + resume a queue
    for (action, want) in [("pause", true), ("resume", false)] {
        let r = ui
            .post(Via::Htmx, &format!("/ui/queues/default/{action}"), &[])
            .await;
        assert_eq!(r.status, StatusCode::OK, "queue {action}: {}", r.body);
        poll("queue paused flag", T, || async {
            let (_, v) = n.rest("GET", "/v1/queues", None).await;
            v["queues"]
                .as_array()?
                .iter()
                .find(|q| q["name"] == "default")
                .filter(|q| q["paused"] == want)
                .map(|_| ())
        })
        .await;
    }
    // retry a failed run (guest failure: restart as a new linked run)
    let before = list_all(n.addr, "tags=ui").await.len();
    let r = ui
        .post(Via::Htmx, &format!("/ui/runs/{failed2}/retry"), &[])
        .await;
    let retried = redirected_run(&r, "retry");
    assert_ne!(retried, failed2);
    n.api.wait_terminal(&retried, T).await;
    assert_eq!(list_all(n.addr, "tags=ui").await.len(), before + 1);

    // rerun-from-step: preview (dry run: verdict, no run), then submit on the fix.
    n.deploy("p11-rerun-v2");
    let before = list_all(n.addr, "workflow=pipeline").await.len();
    let r = ui
        .post(
            Via::Htmx,
            &format!("/ui/runs/{failed}/rerun/preview"),
            &[("step", "s3")],
        )
        .await;
    assert_eq!(r.status, StatusCode::OK, "rerun preview: {}", r.body);
    r.assert_headers("rerun preview");
    for needle in [
        "<dt>Verdict</dt><dd>ok",
        "<dt>Cut</dt><dd>seq 3</dd>",
        "pipeline@2",
    ] {
        assert!(
            r.body.contains(needle),
            "preview lacks {needle}:\n{}",
            r.body
        );
    }
    assert_eq!(
        list_all(n.addr, "workflow=pipeline").await.len(),
        before,
        "a preview created a run"
    );
    let r = ui
        .post(
            Via::Htmx,
            &format!("/ui/runs/{failed}/rerun"),
            &[("step", "s3")],
        )
        .await;
    let rerun = redirected_run(&r, "rerun submit");
    let done = n.api.wait_terminal(&rerun, T).await;
    assert_eq!(done["status"], "completed", "{done}");
    assert_eq!(done["version"], 2, "{done}");
    let h = n.api.history(&rerun).await;
    assert_eq!(step_completions(&h, "s1"), 1);
    let page = ui.get(&format!("/ui/runs/{rerun}")).await;
    page.assert_page("rerun detail");
    assert!(page.body.contains("rerun"), "{}", page.body);

    // DLQ requeue (restart) of the rerun source (a rerun does not touch its DLQ entry).
    let r = ui
        .post(
            Via::Htmx,
            &format!("/ui/dlq/{failed}/requeue"),
            &[("mode", "restart")],
        )
        .await;
    let requeued = redirected_run(&r, "dlq requeue");
    assert_ne!(requeued, failed);
    poll("source left the DLQ view", T, || async {
        let (_, p) = n.rest("GET", "/v1/dlq", None).await;
        (!p["entries"]
            .as_array()?
            .iter()
            .any(|e| e["run_id"] == failed.as_str()))
        .then_some(())
    })
    .await;

    // bulk: preview, create (cancel over a tag), progress page, done; then a slow job
    // cancelled from the UI.
    let targets: Vec<String> = (0..4)
        .map(|_| n.run_nowait(PIPELINE, PARKED, &["ui-bulk"]))
        .collect();
    for id in &targets {
        poll("bulk target parked", T, || async {
            let h = n.api.history(id).await;
            (!events_of(&h, "timer_started").is_empty()).then_some(())
        })
        .await;
    }
    let fields = [
        ("tags", "ui-bulk"),
        ("action", "cancel"),
        ("rate", "50"),
        ("reason", ""),
        ("payload", ""),
    ];
    let r = ui.post(Via::Form, "/ui/bulk/preview", &fields).await;
    r.assert_page("bulk preview");
    assert!(
        r.body.contains(&targets[0]) || r.body.contains('4'),
        "{}",
        r.body
    );
    let (_, jobs) = n.rest("GET", "/v1/bulk", None).await;
    assert_eq!(
        jobs["jobs"].as_array().map(Vec::len),
        Some(0),
        "a preview created a job"
    );
    let r = ui.post(Via::Htmx, "/ui/bulk", &fields).await;
    assert_eq!(r.status, StatusCode::OK, "bulk create: {}", r.body);
    let job = r
        .header("hx-redirect")
        .and_then(|l| l.strip_prefix("/ui/bulk/"))
        .expect("HX-Redirect to the job")
        .to_owned();
    let page = ui.get(&format!("/ui/bulk/{job}")).await;
    page.assert_page("bulk job page");
    assert!(page.body.contains(&job));
    poll("bulk job done", T, || async {
        let (_, j) = n.rest("GET", &format!("/v1/bulk/{job}"), None).await;
        (j["state"] == "done").then_some(())
    })
    .await;
    for id in &targets {
        wait_status(&n, id, "cancelled").await;
    }
    let prog = ui.get_fragment(&format!("/ui/bulk/{job}/progress")).await;
    prog.assert_page("bulk progress");

    let slow: Vec<String> = (0..5)
        .map(|_| n.run_nowait(PIPELINE, PARKED, &["ui-slow"]))
        .collect();
    let r = ui
        .post(
            Via::Htmx,
            "/ui/bulk",
            &[
                ("tags", "ui-slow"),
                ("action", "terminate"),
                ("reason", "slow"),
                ("rate", "1"),
            ],
        )
        .await;
    let slow_job = r
        .header("hx-redirect")
        .and_then(|l| l.strip_prefix("/ui/bulk/"))
        .unwrap_or_else(|| panic!("slow bulk create: {} {}", r.status, r.body))
        .to_owned();
    let r = ui
        .post(Via::Htmx, &format!("/ui/bulk/{slow_job}/cancel"), &[])
        .await;
    assert_eq!(r.status, StatusCode::OK, "bulk cancel: {}", r.body);
    poll("bulk job cancelled", T, || async {
        let (_, j) = n.rest("GET", &format!("/v1/bulk/{slow_job}"), None).await;
        (j["state"] == "cancelled").then_some(j)
    })
    .await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let mut alive = 0;
    for id in &slow {
        if !is_terminal(status_of(&n, id).await["status"].as_str().unwrap()) {
            alive += 1;
        }
    }
    assert!(
        alive >= 3,
        "the cancelled job kept terminating runs ({alive} alive)"
    );
    n.stop();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ui_inventory_views_on_a_live_server_with_a_worker() {
    let _g = SERIAL.lock().await;
    let c = p7::P7Cluster::start(p7::P7Opts::default()).await;
    let _w = c.spawn_worker(p7::WorkerOpts::new("w-ui", 2));
    c.wait_workers(1).await;
    c.api.deploy("p11-rerun").await;
    let id = c.api.start(PIPELINE, json!({"input": {}})).await;
    c.api.wait_terminal(&id, p7::P7_T).await;
    let (st, v) = c
        .api
        .post(
            "/v1/schedules",
            json!({"name": "nightly-ui", "workflow": PIPELINE, "cron": "0 0 3 * * *",
                   "input": {}}),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED, "{v}");
    let (_, run) = c.api.get(&format!("/v1/runs/{id}")).await;
    let sha = run["module_sha256"].as_str().unwrap().to_owned();

    let ui = UiClient::connect(&c.api.base, Some(&c.ca_pem)).await;
    for (path, needles) in [
        ("/ui/modules", vec![sha.as_str()]),
        ("/ui/workflows", vec![PIPELINE]),
        ("/ui/queues", vec!["default"]),
        ("/ui/schedules", vec!["nightly-ui", "0 0 3 * * *"]),
        ("/ui/workers", vec!["w-ui"]),
        (
            "/ui/partitions",
            vec!["class=\"inventory partitions\"", "badge healthy"],
        ),
        ("/ui/", vec![id.as_str()]),
    ] {
        let r = ui.get(path).await;
        r.assert_page(path);
        for needle in needles {
            assert!(
                r.body.contains(needle),
                "{path} lacks {needle:?}:\n{}",
                r.body
            );
        }
    }
    let parts = ui.get("/ui/partitions").await;
    assert!(
        parts.body.matches("badge healthy").count() >= 4,
        "{}",
        parts.body
    );
    // An inventory action on the live server: pause + resume the schedule.
    for (action, want) in [("pause", true), ("resume", false)] {
        let r = ui
            .post(
                Via::Htmx,
                &format!("/ui/schedules/nightly-ui/{action}"),
                &[],
            )
            .await;
        assert_eq!(r.status, StatusCode::OK, "schedule {action}: {}", r.body);
        let (st, s) = c.api.get("/v1/schedules/nightly-ui").await;
        assert_eq!(st, StatusCode::OK, "{s}");
        assert_eq!(s["paused"], want, "{s}");
    }
}
