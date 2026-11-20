//! TRIG-02/TRIG-03 (08-14): cron schedules end to end on the real server with a
//! `ManualClock` as the one injected clock.
//!
//! - Overlap Skip / BufferOne / AllowAll / CancelOther / TerminateOther against a
//!   long-running guest (the Phase 5 `signals` guest in `long_wait` mode).
//! - Catch-up Skip / One / All after downtime (stop the server, advance the clock, start).
//! - Pause stops fires and keeps the watermark; resume catches up per policy.
//! - Trigger-now starts a run without moving the watermark; a repeated `request_id` returns
//!   the same run.
//! - CRUD over REST and gRPC: invalid cron / unknown time zone / unknown workflow = 400 with
//!   a message, an existing name or a stale `version_tag` = 409.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{ServerOpts, TestServer, now_ms, server_config, tmpfs_dir};
use ostro_partition::clock::ManualClock;
use ostro_partition::leader::cron::schedule_state;
use ostro_partition::query::runs_page;
use ostro_partition::store::values::{RunStatus, ScheduleStateRow};
use ostro_proto::api::v1 as pb;
use ostro_server::config::ServerConfig;
use ostro_server::executor::ExecutorHooks;
use ostro_server::server::{ServerHandle, ServerOptions, start_with_options};
use ostro_types::routing::schedule_owner;
use ostro_types::{PartitionCount, RunId};
use reqwest::StatusCode;
use serde_json::{Value, json};
use tokio::time::{Instant, sleep, timeout};

const LIMIT: Duration = Duration::from_secs(30);
const EVERY_10S: &str = "*/10 * * * * *";

/// A minute-aligned instant a little in the future: the fake "now" of a test.
fn t0() -> u64 {
    (now_ms() / 60_000 + 2) * 60_000
}

async fn boot(cfg: &ServerConfig, clock: &ManualClock) -> ServerHandle {
    timeout(
        Duration::from_secs(120),
        start_with_options(
            cfg.clone(),
            ServerOptions::default().with_clock(Arc::new(clock.clone())),
        ),
    )
    .await
    .expect("server start timed out")
    .expect("server start")
}

async fn start(clock: &ManualClock, tweak: impl FnOnce(&mut ServerConfig)) -> TestServer {
    let dir = tmpfs_dir();
    let mut cfg = server_config(dir.path(), &ServerOpts::default());
    cfg.engine.hot_wait = Duration::from_millis(50).into();
    tweak(&mut cfg);
    let handle = boot(&cfg, clock).await;
    let s = TestServer {
        handle: Some(handle),
        cfg,
        hooks: Arc::new(ExecutorHooks::default()),
        http: reqwest::Client::new(),
        dir: Some(dir),
    };
    s.wait_ready(Duration::from_secs(30)).await;
    s.deploy_guest("signals", None).await;
    s
}

/// Graceful stop, then start on the same data dir with the same clock.
async fn restart(s: &mut TestServer, clock: &ManualClock) {
    s.shutdown().await;
    s.http = reqwest::Client::new();
    s.handle = Some(boot(&s.cfg, clock).await);
    s.wait_ready(Duration::from_secs(30)).await;
}

fn pc(s: &TestServer) -> PartitionCount {
    PartitionCount::new(s.cfg.node.partitions).expect("non-zero")
}

/// One run started by schedule `name`: `fire` is `Some(fire_time)` for a cron fire, `None`
/// for a manual one.
#[derive(Clone, Debug)]
struct SchedRun {
    fire: Option<u64>,
    run: RunId,
}

/// Every run schedule `name` started (read from its owner partition), cron fires by fire
/// time first, then manual runs.
fn runs_of(s: &TestServer, name: &str) -> Vec<SchedRun> {
    let owner = schedule_owner(name, pc(s));
    let h = s.h().node().handle(owner).expect("owner partition").clone();
    let prefix = format!("__cron/{name}/");
    let mut out: Vec<SchedRun> = runs_page(&h.reader(), None, usize::MAX)
        .expect("runs")
        .into_iter()
        .filter_map(|(run, rec)| {
            let key = String::from_utf8(rec.idempotency_key).ok()?;
            let rest = key.strip_prefix(&prefix)?;
            assert!(
                rec.tags.contains(&format!("ostro.schedule={name}")),
                "schedule tag on {run}: {:?}",
                rec.tags
            );
            Some(SchedRun {
                fire: if rest.starts_with("manual/") {
                    None
                } else {
                    Some(rest.parse().expect("fire time"))
                },
                run,
            })
        })
        .collect();
    out.sort_by_key(|r| (r.fire.is_none(), r.fire));
    out
}

fn fires(s: &TestServer, name: &str) -> Vec<u64> {
    runs_of(s, name).iter().filter_map(|r| r.fire).collect()
}

fn manual(s: &TestServer, name: &str) -> Vec<RunId> {
    runs_of(s, name)
        .iter()
        .filter(|r| r.fire.is_none())
        .map(|r| r.run)
        .collect()
}

fn state(s: &TestServer, name: &str) -> Option<ScheduleStateRow> {
    let owner = schedule_owner(name, pc(s));
    let h = s.h().node().handle(owner).expect("owner partition").clone();
    schedule_state(&h.reader(), name).expect("schedule_state")
}

async fn wait_for(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + LIMIT;
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        sleep(Duration::from_millis(20)).await;
    }
}

/// The driver polls every 50 ms; let it run a few rounds.
async fn settle() {
    sleep(Duration::from_millis(500)).await;
}

fn sched(name: &str, overlap: &str, catchup: &str) -> Value {
    json!({
        "name": name,
        "workflow": "signals",
        "cron": EVERY_10S,
        "input": {"mode": "long_wait"},
        "overlap": overlap,
        "catchup": catchup,
    })
}

async fn create(s: &TestServer, body: &Value) -> Value {
    let (st, v) = s.create_schedule(body).await;
    assert_eq!(st, StatusCode::CREATED, "create {body}: {v}");
    v
}

async fn signal(s: &TestServer, run: &RunId) {
    let (st, v) = s
        .post_json(
            &format!("/v1/runs/{run}/signals/x"),
            &json!({"payload": "go"}),
        )
        .await;
    assert_eq!(st, StatusCode::OK, "signal {run}: {v}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn overlap_policies_against_a_long_running_run() {
    let t0 = t0();
    let clock = ManualClock::new(t0);
    let s = start(&clock, |_| {}).await;
    let policies = [
        ("ov-skip", "skip"),
        ("ov-buffer", "buffer_one"),
        ("ov-allow", "allow_all"),
        ("ov-cancel", "cancel_other"),
        ("ov-terminate", "terminate_other"),
    ];
    for (name, overlap) in policies {
        let v = create(&s, &sched(name, overlap, "one")).await;
        assert_eq!(v["overlap"], overlap, "{v}");
        assert_eq!(v["next_fire_at_ms"], t0 + 10_000, "{v}");
    }
    settle().await;
    clock.set(t0 + 10_000);
    for (name, _) in policies {
        wait_for(&format!("first fire of {name}"), || {
            fires(&s, name) == vec![t0 + 10_000]
        })
        .await;
    }
    // Every first run waits for a signal: the second fire overlaps.
    for (name, _) in policies {
        let first = runs_of(&s, name)[0].run;
        assert!(!s.run_record(&first).status.is_terminal(), "{name} live");
    }
    clock.set(t0 + 20_000);
    for (name, _) in policies {
        wait_for(&format!("second fire of {name} applied"), || {
            state(&s, name).is_some_and(|r| r.watermark_ms == t0 + 20_000)
        })
        .await;
    }
    settle().await;

    // Skip: no second run.
    assert_eq!(fires(&s, "ov-skip"), vec![t0 + 10_000]);
    let row = state(&s, "ov-skip").expect("row");
    assert_eq!((row.fired, row.skipped), (1, 1), "{row:?}");

    // AllowAll: both run.
    assert_eq!(fires(&s, "ov-allow"), vec![t0 + 10_000, t0 + 20_000]);
    let allow = runs_of(&s, "ov-allow");
    assert!(!s.run_record(&allow[0].run).status.is_terminal());

    // CancelOther: the first run is cancelled gracefully, the second starts.
    assert_eq!(fires(&s, "ov-cancel"), vec![t0 + 10_000, t0 + 20_000]);
    let first = runs_of(&s, "ov-cancel")[0].run;
    let rec = s
        .wait_record(&first, LIMIT, |r| r.status.is_terminal())
        .await;
    assert_eq!(rec.status, RunStatus::Cancelled, "{rec:?}");
    assert_eq!(rec.cancel_reason, "schedule_overlap");

    // TerminateOther: the first run is terminated, the second starts.
    assert_eq!(fires(&s, "ov-terminate"), vec![t0 + 10_000, t0 + 20_000]);
    let first = runs_of(&s, "ov-terminate")[0].run;
    let rec = s.wait_status(&first, RunStatus::Terminated, LIMIT).await;
    assert_eq!(rec.terminal_reason, "schedule_overlap");

    // BufferOne: held back until the first run ends, then started.
    assert_eq!(fires(&s, "ov-buffer"), vec![t0 + 10_000]);
    let row = state(&s, "ov-buffer").expect("row");
    assert_eq!(
        row.buffered.as_ref().map(|b| b.fire_time_ms),
        Some(t0 + 20_000),
        "{row:?}"
    );
    let first = runs_of(&s, "ov-buffer")[0].run;
    signal(&s, &first).await;
    s.wait_status(&first, RunStatus::Completed, LIMIT).await;
    wait_for("buffered fire started", || {
        fires(&s, "ov-buffer") == vec![t0 + 10_000, t0 + 20_000]
    })
    .await;
    assert!(state(&s, "ov-buffer").expect("row").buffered.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catch_up_policies_after_downtime() {
    let t0 = t0();
    let clock = ManualClock::new(t0);
    let mut s = start(&clock, |c| c.triggers.cron_catchup_cap = 3).await;
    create(&s, &sched("cu-skip", "allow_all", "skip")).await;
    create(&s, &sched("cu-one", "allow_all", "one")).await;
    let mut all = sched("cu-all", "allow_all", "all");
    all["catchup_window_ms"] = json!(60_000);
    create(&s, &all).await;
    settle().await;
    clock.set(t0 + 10_000);
    for n in ["cu-skip", "cu-one", "cu-all"] {
        wait_for(&format!("first fire of {n}"), || {
            fires(&s, n) == vec![t0 + 10_000]
        })
        .await;
    }
    s.shutdown().await;
    // 5 minutes (+3 s) of downtime: 30 missed fires.
    let now = t0 + 10_000 + 303_000;
    let latest = now - now % 10_000;
    clock.set(now);
    restart(&mut s, &clock).await;
    wait_for("catch-up One", || {
        fires(&s, "cu-one") == vec![t0 + 10_000, latest]
    })
    .await;
    wait_for("catch-up All (3 newest within the window)", || {
        fires(&s, "cu-all") == vec![t0 + 10_000, latest - 20_000, latest - 10_000, latest]
    })
    .await;
    wait_for("catch-up Skip advances", || {
        state(&s, "cu-skip").is_some_and(|r| r.watermark_ms == latest)
    })
    .await;
    settle().await;
    assert_eq!(
        fires(&s, "cu-skip"),
        vec![t0 + 10_000],
        "Skip starts nothing"
    );
    let (st, v) = s.get_json("/v1/schedules/cu-skip").await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["last_fire_at_ms"], latest, "{v}");
    assert_eq!(v["next_fire_at_ms"], latest + 10_000, "{v}");
    // Steady state continues after catch-up.
    clock.set(latest + 10_000);
    wait_for("steady fire after catch-up", || {
        fires(&s, "cu-skip") == vec![t0 + 10_000, latest + 10_000]
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pause_resume_and_trigger_now() {
    let t0 = t0();
    let clock = ManualClock::new(t0);
    let s = start(&clock, |_| {}).await;
    create(&s, &sched("pr", "allow_all", "one")).await;
    settle().await;
    clock.set(t0 + 10_000);
    wait_for("first fire", || fires(&s, "pr") == vec![t0 + 10_000]).await;

    // Pause: no fires, the watermark stays.
    let (st, v) = s.post_json("/v1/schedules/pr/pause", &Value::Null).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["paused"], true);
    assert_eq!(v["next_fire_at_ms"], Value::Null, "{v}");
    let (st, v2) = s.post_json("/v1/schedules/pr/pause", &Value::Null).await;
    assert_eq!(st, StatusCode::OK, "idempotent pause: {v2}");
    assert_eq!(v2["version_tag"], v["version_tag"], "no second write");
    settle().await;
    clock.set(t0 + 60_000);
    settle().await;
    assert_eq!(fires(&s, "pr"), vec![t0 + 10_000], "paused: no fires");
    assert_eq!(state(&s, "pr").expect("row").watermark_ms, t0 + 10_000);

    // Trigger-now (also while paused): a run, the watermark does not move; a repeated
    // request_id returns the same run.
    let (st, v) = s
        .post_json("/v1/schedules/pr/trigger", &json!({"request_id": "r-1"}))
        .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    let run1 = v["run_id"].as_str().expect("run_id").to_owned();
    let (st, v) = s
        .post_json("/v1/schedules/pr/trigger", &json!({"request_id": "r-1"}))
        .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["run_id"], run1.as_str(), "same request_id, same run");
    let (st, v) = s.post_json("/v1/schedules/pr/trigger", &json!({})).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    let run2 = v["run_id"].as_str().expect("run_id").to_owned();
    assert_ne!(run1, run2, "no request_id: a fresh run");
    let mut m: Vec<String> = manual(&s, "pr").iter().map(RunId::to_string).collect();
    m.sort();
    let mut want = vec![run1.clone(), run2.clone()];
    want.sort();
    assert_eq!(m, want);
    assert_eq!(
        state(&s, "pr").expect("row").watermark_ms,
        t0 + 10_000,
        "manual fires never move the watermark"
    );
    let (_, v) = s.get_json("/v1/schedules/pr").await;
    assert_eq!(v["last_fire_at_ms"], t0 + 10_000, "{v}");

    // Resume at t0+65s: catch-up One fires the latest missed fire (t0+60s).
    clock.set(t0 + 65_000);
    let (st, v) = s.post_json("/v1/schedules/pr/resume", &Value::Null).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["paused"], false);
    wait_for("resume catch-up", || {
        fires(&s, "pr") == vec![t0 + 10_000, t0 + 60_000]
    })
    .await;
    clock.set(t0 + 70_000);
    wait_for("steady fire after resume", || {
        fires(&s, "pr") == vec![t0 + 10_000, t0 + 60_000, t0 + 70_000]
    })
    .await;

    // Unknown schedule.
    let (st, _) = s.post_json("/v1/schedules/nope/trigger", &json!({})).await;
    assert_eq!(st, StatusCode::NOT_FOUND);
}

fn reason(v: &Value) -> &str {
    v["error"]["details"]["reason"].as_str().unwrap_or_default()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crud_rest_and_grpc_with_validation_and_cas() {
    let clock = ManualClock::new(t0());
    let s = start(&clock, |_| {}).await;
    let body = json!({
        "name": "crud",
        "workflow": "signals",
        "cron": "0 0 9 * * MON#2",
        "timezone": "Europe/Berlin",
        "input": {"mode": "long_wait"},
        "overlap": "skip",
        "catchup": "skip",
    });
    let v = create(&s, &body).await;
    let tag = v["version_tag"].as_u64().expect("version_tag");
    assert!(
        v["next_fire_at_ms"].as_u64().expect("next") > now_ms(),
        "{v}"
    );
    assert_eq!(v["last_fire_at_ms"], Value::Null);
    assert_eq!(v["input"], json!({"mode": "long_wait"}));

    let (st, v) = s.create_schedule(&body).await;
    assert_eq!(st, StatusCode::CONFLICT, "existing name: {v}");

    for (bad, why) in [
        (json!({"cron": "not a cron"}), "invalid cron expression"),
        (json!({"cron": "0 0 0 30 2 *"}), "never fires"),
        (
            json!({"timezone": "Mars/Olympus_Mons"}),
            "invalid time zone",
        ),
        (json!({"workflow": "no-such-workflow"}), "does not exist"),
        (json!({"version": 9}), "version 9 does not exist"),
        (json!({"start_at_ms": 10_000, "end_at_ms": 5_000}), ""),
    ] {
        let mut b = body.clone();
        b["name"] = json!("bad");
        for (k, v) in bad.as_object().expect("object") {
            b[k] = v.clone();
        }
        let (st, v) = s.create_schedule(&b).await;
        assert_eq!(st, StatusCode::BAD_REQUEST, "{bad}: {v}");
        let msg = v["error"]["message"].as_str().unwrap_or_default();
        assert!(msg.contains(why), "{bad}: {msg}");
    }
    let (st, _) = s.get_json("/v1/schedules/bad").await;
    assert_eq!(st, StatusCode::NOT_FOUND);

    // Update: stale tag 409, current tag 200 (new tag).
    let mut upd = body.clone();
    upd["cron"] = json!("0 */30 * * * *");
    upd["version_tag"] = json!(tag + 100);
    let (st, v) = s.put_json("/v1/schedules/crud", &upd).await;
    assert_eq!(st, StatusCode::CONFLICT, "{v}");
    assert_eq!(reason(&v), "version_mismatch", "{v}");
    upd["version_tag"] = json!(tag);
    let (st, v) = s.put_json("/v1/schedules/crud", &upd).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["cron"], "0 */30 * * * *");
    let tag2 = v["version_tag"].as_u64().expect("tag");
    assert!(tag2 > tag);
    let mut bad = upd.clone();
    bad["cron"] = json!("61 * * * * *");
    bad["version_tag"] = json!(tag2);
    let (st, v) = s.put_json("/v1/schedules/crud", &bad).await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");

    let (st, v) = s.get_json("/v1/schedules").await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(v["schedules"].as_array().expect("list").len(), 1, "{v}");

    // Delete: stale tag 409, then ok, then 404.
    let (st, v) = s
        .delete_json(&format!("/v1/schedules/crud?version_tag={}", tag2 + 1))
        .await;
    assert_eq!(st, StatusCode::CONFLICT, "{v}");
    assert_eq!(reason(&v), "version_mismatch", "{v}");
    let (st, v) = s
        .delete_json(&format!("/v1/schedules/crud?version_tag={tag2}"))
        .await;
    assert!(st.is_success(), "{st}: {v}");
    let (st, _) = s.get_json("/v1/schedules/crud").await;
    assert_eq!(st, StatusCode::NOT_FOUND);

    // gRPC: the same controller.
    let mut c = pb::schedules_client::SchedulesClient::new(s.channel().await);
    let created = c
        .create(pb::CreateScheduleRequest {
            schedule: Some(pb::Schedule {
                name: "g".into(),
                workflow: "signals".into(),
                cron: "*/15 * * * * *".into(),
                timezone: "America/New_York".into(),
                input_json: br#"{"mode":"long_wait"}"#.to_vec(),
                overlap: pb::OverlapPolicy::AllowAll as i32,
                catchup: pb::CatchupPolicy::One as i32,
                ..Default::default()
            }),
        })
        .await
        .expect("grpc create")
        .into_inner();
    assert!(created.version_tag.is_some());
    assert!(created.next_fire_at_ms.is_some());
    let e = c
        .create(pb::CreateScheduleRequest {
            schedule: Some(pb::Schedule {
                name: "g2".into(),
                workflow: "signals".into(),
                cron: "*/15 * * * * *".into(),
                timezone: "Nowhere/City".into(),
                ..Default::default()
            }),
        })
        .await
        .expect_err("unknown tz");
    assert_eq!(e.code(), tonic::Code::InvalidArgument, "{e:?}");
    let got = c
        .get(pb::GetScheduleRequest { name: "g".into() })
        .await
        .expect("grpc get")
        .into_inner();
    assert_eq!(got.timezone, "America/New_York");
    let listed = c
        .list(pb::ListSchedulesRequest {})
        .await
        .expect("grpc list")
        .into_inner();
    assert_eq!(listed.schedules.len(), 1);
    let t = c
        .trigger(pb::TriggerScheduleRequest {
            name: "g".into(),
            request_id: Some("grpc-1".into()),
        })
        .await
        .expect("grpc trigger")
        .into_inner();
    let t2 = c
        .trigger(pb::TriggerScheduleRequest {
            name: "g".into(),
            request_id: Some("grpc-1".into()),
        })
        .await
        .expect("grpc trigger again")
        .into_inner();
    assert_eq!(t.run_id, t2.run_id);
    let paused = c
        .pause(pb::PauseScheduleRequest { name: "g".into() })
        .await
        .expect("grpc pause")
        .into_inner();
    assert!(paused.paused);
    let e = c
        .delete(pb::DeleteScheduleRequest {
            name: "g".into(),
            version_tag: Some(created.version_tag.expect("tag")),
        })
        .await
        .expect_err("stale delete");
    assert_eq!(e.code(), tonic::Code::AlreadyExists, "{e:?}");
    c.delete(pb::DeleteScheduleRequest {
        name: "g".into(),
        version_tag: paused.version_tag,
    })
    .await
    .expect("grpc delete");
    assert!(
        c.list(pb::ListSchedulesRequest {})
            .await
            .expect("list")
            .into_inner()
            .schedules
            .is_empty()
    );
}
