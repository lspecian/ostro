//! Phase 8 SC1 / SC2 end to end (plan 08-20).
//!
//! - `stripe_round_trip` (SC1): a signed Stripe `checkout.session.completed` webhook starts
//!   the `hook-order` guest, keyed by the session id; a second signed Stripe webhook
//!   (`payment_intent.succeeded`-shaped, same object id) signals `paid` to that run, which
//!   completes with `{session, paid_at, payment}`. A duplicate delivery of either hook
//!   starts or applies nothing new; exactly one run exists.
//! - `github_slack_hmac_hooks` (SC1): GitHub, Slack and generic-HMAC hooks each start one
//!   run; tampered deliveries are 401 and start none; the Slack challenge is echoed.
//! - `cron_cli_policies` (SC2 / TRIG-03): through the spawned `ostro schedules ...` CLI
//!   against the test server: a 1 s schedule with overlap Skip keeps one live run;
//!   trigger-now is refused by Skip while a run is live and starts a manual run while
//!   paused; pause stops fires; resume catches up the latest missed instant (One); and a
//!   1 s AllowAll / One schedule fires exactly once for the whole downtime after a
//!   stop/start, then continues every second.
//!
//! The kill -9 half of SC2 is `crates/ostro-crashtest/tests/cron_exactly_once.rs`.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::path::PathBuf;
use std::process::Command;
use std::sync::OnceLock;
use std::time::Duration;

use common::phase8::{Phase8Server, hex, hmac_sha256, sign_github, sign_slack, sign_stripe};
use common::{TestServer, now_ms, repo_root};
use ostro_meta::records::{SealedSecret, TOKEN_KEY_PLAINTEXT_ID};
use ostro_meta::{MetaCommand, MetaKey, MetaRecord, MetaResult};
use ostro_partition::query::runs_page;
use ostro_types::routing::schedule_owner;
use ostro_types::{PartitionCount, RunId};
use reqwest::StatusCode;
use serde_json::{Value, json};
use tokio::time::{Instant, sleep};

const LIMIT: Duration = Duration::from_secs(30);

type Headers = Vec<(String, String)>;

async fn server() -> Phase8Server {
    let s = Phase8Server::builder()
        .with_config(|c| c.engine.hot_wait = Duration::from_millis(50).into())
        .start()
        .await;
    s.wait_ready(Duration::from_secs(10)).await;
    s
}

/// A `plaintext-v0` sealed meta secret.
async fn put_secret(s: &TestServer, name: &str, value: &[u8]) {
    let r = s
        .h()
        .meta()
        .propose(MetaCommand::Put {
            key: MetaKey::Secret(name.into()),
            expected_version: None,
            record: MetaRecord::Secret(SealedSecret {
                name: name.into(),
                ciphertext: value.to_vec(),
                key_id: TOKEN_KEY_PLAINTEXT_ID.into(),
                ext: Vec::new(),
            }),
        })
        .await
        .expect("propose secret");
    assert!(matches!(r, MetaResult::Ok { .. }), "{r:?}");
}

async fn create_hook(s: &TestServer, w: Value) {
    let (st, v) = s.create_webhook(&w).await;
    assert_eq!(st, StatusCode::CREATED, "create {w}: {v}");
}

async fn deliver(s: &TestServer, name: &str, h: &Headers, body: &[u8]) -> (StatusCode, Value) {
    let (st, _, b) = s.deliver_hook(name, h, body).await;
    let v = serde_json::from_slice(&b).unwrap_or_else(|_| Value::String(format!("{b:?}")));
    (st, v)
}

async fn deliver_ok(s: &TestServer, name: &str, h: &Headers, body: &[u8]) -> Value {
    let (st, v) = deliver(s, name, h, body).await;
    assert_eq!(st, StatusCode::OK, "deliver {name}: {v}");
    v
}

fn now_s() -> i64 {
    i64::try_from(now_ms() / 1000).expect("secs")
}

fn with(mut h: Headers, k: &str, v: &str) -> Headers {
    h.push((k.into(), v.into()));
    h
}

fn tamper(body: &[u8]) -> Vec<u8> {
    let mut b = body.to_vec();
    let i = b.iter().position(|c| *c == b'1').expect("a '1' to flip");
    b[i] = b'2';
    b
}

async fn result_json(s: &TestServer, run_id: &str) -> Value {
    serde_json::from_slice(
        &s.result_bytes(&run_id.parse().expect("run id"))
            .await
            .expect("result"),
    )
    .expect("json result")
}

// ---------------------------------------------------------------------------------
// SC1: Stripe checkout -> payment round trip
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stripe_round_trip() {
    let mut s = server().await;
    s.deploy_guest("hook-order", None).await;
    let secret = b"whsec_sc1_round_trip".to_vec();
    put_secret(&s, "_/stripe", &secret).await;
    // Webhook A: start hook-order with the checkout session, one run per session id.
    create_hook(
        &s,
        json!({"name": "checkout", "verifier": {"kind": "stripe", "secret_ref": "stripe"},
        "action": {"kind": "start", "workflow": "hook-order", "input": "/data/object"},
        "idempotency": "json:/data/object/id"}),
    )
    .await;
    // Webhook B: signal `paid` to the run started for the same object id.
    create_hook(
        &s,
        json!({"name": "payment", "verifier": {"kind": "stripe", "secret_ref": "stripe"},
        "action": {"kind": "signal", "workflow": "hook-order", "name": "paid",
                   "run_by": {"idempotency_pointer": "/data/object/id"}}}),
    )
    .await;

    let now = now_s();
    let checkout = br#"{"id":"evt_1Checkout","object":"event","type":"checkout.session.completed","created":1700000001,"data":{"object":{"id":"cs_test_a1","object":"checkout.session","amount_total":4200,"currency":"eur"}}}"#;
    let v = deliver_ok(
        &s,
        "checkout",
        &sign_stripe(&secret, now, checkout),
        checkout,
    )
    .await;
    assert_eq!(v["created"], true, "{v}");
    assert_eq!(v["duplicate"], false, "{v}");
    let run_id = v["run_id"].as_str().expect("run_id").to_owned();
    // Duplicate delivery of A (Stripe retries re-sign with a new timestamp): same run.
    let v = deliver_ok(
        &s,
        "checkout",
        &sign_stripe(&secret, now + 3, checkout),
        checkout,
    )
    .await;
    assert_eq!(v["run_id"], run_id.as_str(), "{v}");
    assert_eq!(v["created"], false, "{v}");
    assert_eq!(v["duplicate"], true, "{v}");
    // The run is waiting for `paid` with the session recorded.
    let deadline = Instant::now() + LIMIT;
    loop {
        let (_, r) = s.get_json(&format!("/v1/runs/{run_id}")).await;
        if r["published_status"]["session"] == "cs_test_a1" {
            assert!(r["completed_at_ms"].is_null(), "{r}");
            break;
        }
        assert!(Instant::now() < deadline, "no session status: {r}");
        sleep(Duration::from_millis(20)).await;
    }

    let paid = br#"{"id":"evt_2Payment","object":"event","type":"payment_intent.succeeded","created":1700000123,"data":{"object":{"id":"cs_test_a1","object":"payment_intent","amount_received":4200}}}"#;
    let v = deliver_ok(&s, "payment", &sign_stripe(&secret, now, paid), paid).await;
    assert_eq!(v["run_id"], run_id.as_str(), "{v}");
    assert_eq!(v["duplicate"], false, "{v}");
    // Immediate redelivery of B: SignalSeen (200 duplicate) while the run is live, or
    // 409 run_terminal if it already completed. Either way nothing new is applied.
    let redeliver_b = |at: i64| {
        let s = &s;
        let h = sign_stripe(&secret, at, paid);
        async move {
            let (st, v) = deliver(s, "payment", &h, paid).await;
            match st {
                StatusCode::OK => assert_eq!(v["duplicate"], true, "{v}"),
                StatusCode::CONFLICT => {
                    assert_eq!(v["error"]["details"]["reason"], "run_terminal", "{v}")
                }
                other => panic!("redelivery of B: {other} {v}"),
            }
        }
    };
    redeliver_b(now + 1).await;
    s.wait_run_status(&run_id, "completed", LIMIT).await;
    // Redelivery of B after completion: nothing applied (409 run_terminal).
    redeliver_b(now + 5).await;
    // Redelivery of A after completion: still the same run, nothing started.
    let v = deliver_ok(
        &s,
        "checkout",
        &sign_stripe(&secret, now + 6, checkout),
        checkout,
    )
    .await;
    assert_eq!(v["run_id"], run_id.as_str(), "{v}");
    assert_eq!(v["created"], false, "{v}");

    assert_eq!(
        result_json(&s, &run_id).await,
        json!({"session": "cs_test_a1", "paid_at": 1_700_000_123, "payment": "cs_test_a1"})
    );
    assert_eq!(
        s.list_all("workflow=hook-order").await,
        vec![run_id.clone()]
    );
    // Exactly one `paid` signal reached the journal (one SignalPayload).
    let kinds = s.history_kinds(&run_id.parse().expect("id")).await;
    let signals = kinds
        .iter()
        .filter(|k| format!("{k:?}") == "SignalPayload")
        .count();
    assert_eq!(signals, 1, "{kinds:?}");
    // Tampered payment for another session: 401, nothing applied.
    let (st, _) = deliver(
        &s,
        "payment",
        &sign_stripe(&secret, now, paid),
        &tamper(paid),
    )
    .await;
    assert_eq!(st, StatusCode::UNAUTHORIZED);
    s.shutdown().await;
}

// ---------------------------------------------------------------------------------
// SC1: GitHub, Slack, generic HMAC
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn github_slack_hmac_hooks() {
    let mut s = server().await;
    s.deploy_guest("sdk-basic", None).await;
    let gh = b"gh-sc1-secret".to_vec();
    let slack = b"slack-sc1-signing-secret-0123456".to_vec();
    let hm = b"generic-sc1-hmac".to_vec();
    put_secret(&s, "_/gh", &gh).await;
    put_secret(&s, "_/slack", &slack).await;
    put_secret(&s, "_/hm", &hm).await;
    let start = json!({"kind": "start", "workflow": "sdk-echo", "input": "body"});
    create_hook(
        &s,
        json!({"name": "gh", "action": start, "verifier": {"kind": "github", "secret_ref": "gh"}}),
    )
    .await;
    create_hook(
        &s,
        json!({"name": "slack", "action": start,
               "verifier": {"kind": "slack", "secret_ref": "slack"}}),
    )
    .await;
    create_hook(
        &s,
        json!({"name": "hm", "action": start,
               "verifier": {"kind": "hmac", "secret_ref": "hm", "header": "x-hub-sig",
                            "prefix": "sha256=", "encoding": "hex"},
               "idempotency": "header:x-delivery-id"}),
    )
    .await;

    let now = now_s();
    let gh_body = br#"{"action":"opened","pull_request":{"number":1}}"#.to_vec();
    let slack_body = br#"{"type":"event_callback","event_id":"Ev1SC1","event":{"n":1}}"#.to_vec();
    let hm_body = br#"{"order":1,"total":10}"#.to_vec();
    let gh_h = with(sign_github(&gh, &gh_body), "x-github-delivery", "gh-sc1-1");
    let slack_h = sign_slack(&slack, now, &slack_body);
    let hm_h: Headers = vec![
        (
            "x-hub-sig".into(),
            format!("sha256={}", hex(&hmac_sha256(&hm, &[&hm_body]))),
        ),
        ("x-delivery-id".into(), "hm-1".into()),
    ];
    let mut runs = Vec::new();
    for (name, h, body) in [
        ("gh", &gh_h, &gh_body),
        ("slack", &slack_h, &slack_body),
        ("hm", &hm_h, &hm_body),
    ] {
        // Tampered first: 401, no run.
        let (st, v) = deliver(&s, name, h, &tamper(body)).await;
        assert_eq!(st, StatusCode::UNAUTHORIZED, "{name}: {v}");
        let v = deliver_ok(&s, name, h, body).await;
        assert_eq!(v["created"], true, "{name}: {v}");
        let id = v["run_id"].as_str().expect("run_id").to_owned();
        let again = deliver_ok(&s, name, h, body).await;
        assert_eq!(again["run_id"], id.as_str(), "{name}");
        assert_eq!(again["duplicate"], true, "{name}");
        // Tampered after the first delivery: still 401.
        let (st, _) = deliver(&s, name, h, &tamper(body)).await;
        assert_eq!(st, StatusCode::UNAUTHORIZED, "{name}");
        runs.push((id, body.clone()));
    }
    for (id, body) in &runs {
        s.wait_run_status(id, "completed", LIMIT).await;
        let want: Value = serde_json::from_slice(body).expect("json");
        assert_eq!(result_json(&s, id).await, want);
    }
    let mut listed = s.list_all("workflow=sdk-echo").await;
    listed.sort();
    let mut want: Vec<String> = runs.iter().map(|r| r.0.clone()).collect();
    want.sort();
    assert_eq!(listed, want, "exactly one run per hook");

    // Slack url_verification: echoed as text/plain after verification, no run.
    let challenge = br#"{"type":"url_verification","token":"t","challenge":"sc1-challenge-value"}"#;
    let (st, ct, b) = s
        .deliver_hook("slack", &sign_slack(&slack, now, challenge), challenge)
        .await;
    assert_eq!(st, StatusCode::OK);
    assert!(ct.starts_with("text/plain"), "{ct}");
    assert_eq!(&b[..], b"sc1-challenge-value");
    let (st, _, _) = s
        .deliver_hook("slack", &sign_slack(b"wrong", now, challenge), challenge)
        .await;
    assert_eq!(st, StatusCode::UNAUTHORIZED);
    assert_eq!(s.list_all("workflow=sdk-echo").await.len(), 3);
    s.shutdown().await;
}

// ---------------------------------------------------------------------------------
// SC2 / TRIG-03 through the CLI
// ---------------------------------------------------------------------------------

/// The `ostro` binary of this build (same target dir, target triple and profile), built
/// once.
///
/// The layout is read off this test binary's own path: the nearest ancestor holding
/// `CACHEDIR.TAG` is the target dir, and the first component below it is either the
/// profile (`debug` / `release`) or, for a `--target <triple>` build such as the musl CI
/// legs (`cargo-zigbuild test --target ...`), the triple followed by the profile. Building
/// with the same `--target` reuses the artifacts this test run already built (and the
/// zig linker env that `cargo-zigbuild` passes down); building for the host instead would
/// compile the whole workspace a second time, which filled the CI runner's disk.
fn ostro_bin() -> PathBuf {
    static BIN: OnceLock<PathBuf> = OnceLock::new();
    BIN.get_or_init(|| {
        let exe = std::env::current_exe().expect("current exe");
        let target = exe
            .ancestors()
            .skip(1)
            .find(|d| d.join("CACHEDIR.TAG").is_file())
            .unwrap_or_else(|| panic!("no target dir (CACHEDIR.TAG) above {}", exe.display()))
            .to_path_buf();
        let rel: Vec<String> = exe
            .strip_prefix(&target)
            .expect("exe under target dir")
            .components()
            .map(|c| c.as_os_str().to_string_lossy().into_owned())
            .collect();
        let (triple, profile) = match rel.first().map(String::as_str) {
            Some("debug") | Some("release") => (None, rel[0].clone()),
            Some(t) => (
                Some(t.to_owned()),
                rel.get(1).cloned().expect("profile dir"),
            ),
            None => panic!("test exe directly in the target dir"),
        };
        let mut profile_dir = target.clone();
        let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
        let mut cmd = Command::new(cargo);
        cmd.current_dir(repo_root())
            .args(["build", "-p", "ostro", "--locked"])
            .env("CARGO_TARGET_DIR", &target);
        if let Some(t) = &triple {
            cmd.args(["--target", t]);
            profile_dir.push(t);
        }
        match profile.as_str() {
            "release" => {
                cmd.arg("--release");
            }
            "debug" => {}
            other => panic!("unexpected profile dir {other:?} in {}", exe.display()),
        }
        profile_dir.push(&profile);
        let out = cmd.output().expect("spawn cargo build -p ostro");
        assert!(
            out.status.success(),
            "cargo build -p ostro failed:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
        profile_dir.join("ostro")
    })
    .clone()
}

/// `ostro schedules <args> --server <s> --json`: (exit code, parsed stdout, stderr).
fn cli(s: &TestServer, args: &[&str]) -> (i32, Value, String) {
    let mut cmd = Command::new(ostro_bin());
    for (k, _) in std::env::vars_os() {
        if k.to_string_lossy().starts_with("OSTRO_") {
            cmd.env_remove(&k);
        }
    }
    let out = cmd
        .arg("schedules")
        .args(args)
        .args(["--server", &format!("http://{}", s.addr()), "--json"])
        .output()
        .expect("spawn ostro schedules");
    let body = serde_json::from_slice(&out.stdout).unwrap_or(Value::Null);
    (
        out.status.code().unwrap_or(-1),
        body,
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

fn cli_ok(s: &TestServer, args: &[&str]) -> Value {
    let (code, v, err) = cli(s, args);
    assert_eq!(code, 0, "ostro schedules {args:?}: {v}\n{err}");
    v
}

/// Cron fire times and manual runs of schedule `name` (owner partition, idempotency keys).
fn runs_of(s: &TestServer, name: &str) -> (Vec<(u64, RunId)>, Vec<RunId>) {
    let pc = PartitionCount::new(s.cfg.node.partitions).expect("partitions");
    let h = s
        .h()
        .node()
        .handle(schedule_owner(name, pc))
        .expect("owner")
        .clone();
    let prefix = format!("__cron/{name}/");
    let mut fires = Vec::new();
    let mut manual = Vec::new();
    for (run, rec) in runs_page(&h.reader(), None, usize::MAX).expect("runs") {
        let Ok(key) = String::from_utf8(rec.idempotency_key) else {
            continue;
        };
        let Some(rest) = key.strip_prefix(&prefix) else {
            continue;
        };
        if rest.starts_with("manual/") {
            manual.push(run);
        } else {
            fires.push((rest.parse().expect("fire time"), run));
        }
    }
    fires.sort();
    (fires, manual)
}

async fn wait_for(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + LIMIT;
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        sleep(Duration::from_millis(20)).await;
    }
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

fn live(s: &TestServer, runs: &[RunId]) -> usize {
    runs.iter()
        .filter(|r| {
            let rec = s.run_record(r);
            !rec.status.is_terminal()
        })
        .count()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cron_cli_policies() {
    let bin = ostro_bin();
    assert!(bin.is_file(), "{}", bin.display());
    let mut s = server().await;
    s.deploy_guest("signals", None).await;
    s.deploy_guest("sdk-basic", None).await;

    // 1 s schedule, overlap Skip, against a guest that waits for a signal.
    let v = cli_ok(
        &s,
        &[
            "create",
            "--name",
            "skip1",
            "--workflow",
            "signals",
            "--cron",
            "* * * * * *",
            "--timezone",
            "America/New_York",
            "--input-json",
            r#"{"mode":"long_wait"}"#,
            "--overlap",
            "skip",
            "--catchup",
            "one",
        ],
    );
    assert_eq!(v["overlap"], "skip", "{v}");
    let v = cli_ok(&s, &["get", "skip1"]);
    assert_eq!(v["overlap"], "skip", "{v}");
    assert_eq!(v["catchup"], "one", "{v}");
    assert_eq!(v["timezone"], "America/New_York", "{v}");
    let ls = cli_ok(&s, &["ls"]);
    assert!(ls.to_string().contains("skip1"), "{ls}");
    // Fires keep coming every second, but Skip keeps exactly one live run.
    wait_for("first cron run", || !runs_of(&s, "skip1").0.is_empty()).await;
    sleep(Duration::from_millis(2_500)).await;
    let (fires, manual) = runs_of(&s, "skip1");
    assert_eq!(
        fires.len(),
        1,
        "overlap Skip started more than one run: {fires:?}"
    );
    assert!(manual.is_empty());
    let first = fires[0].1;
    // Trigger-now while a run is live: refused by Skip (409 schedule_overlap).
    let (code, body, err) = cli(&s, &["trigger", "skip1"]);
    assert_eq!(code, 1, "{body}\n{err}");
    assert!(format!("{body}{err}").contains("409"), "{body}\n{err}");

    // Pause: no fires; trigger-now still works and starts a manual run.
    let v = cli_ok(&s, &["pause", "skip1"]);
    assert_eq!(v["paused"], true, "{v}");
    signal(&s, &first).await;
    wait_for("first run done", || live(&s, &[first]) == 0).await;
    sleep(Duration::from_millis(2_500)).await;
    assert_eq!(runs_of(&s, "skip1").0.len(), 1, "a paused schedule fired");
    let v = cli_ok(&s, &["trigger", "skip1", "--request-id", "sc2-manual-1"]);
    let manual_run: RunId = v["run_id"].as_str().expect("run_id").parse().expect("id");
    let (fires, manual) = runs_of(&s, "skip1");
    assert_eq!((fires.len(), manual), (1, vec![manual_run]));
    // A retried trigger with the same request id while that run is still live returns the
    // same run: the manual dedupe row is checked before overlap Skip (was 409).
    let v = cli_ok(&s, &["trigger", "skip1", "--request-id", "sc2-manual-1"]);
    assert_eq!(
        v["run_id"],
        manual_run.to_string(),
        "retry while live: same run"
    );
    signal(&s, &manual_run).await;
    wait_for("manual run done", || live(&s, &[manual_run]) == 0).await;
    // And after it finished, too.
    let v = cli_ok(&s, &["trigger", "skip1", "--request-id", "sc2-manual-1"]);
    assert_eq!(
        v["run_id"],
        manual_run.to_string(),
        "same request id, same run"
    );
    assert_eq!(runs_of(&s, "skip1").1, vec![manual_run]);

    // Resume: catch-up One fires the latest missed instant (one run), then Skip holds.
    let before_resume = now_ms();
    let v = cli_ok(&s, &["resume", "skip1"]);
    assert_eq!(v["paused"], false, "{v}");
    wait_for("catch-up fire", || runs_of(&s, "skip1").0.len() == 2).await;
    sleep(Duration::from_millis(2_000)).await;
    let (fires, _) = runs_of(&s, "skip1");
    assert_eq!(fires.len(), 2, "{fires:?}");
    assert!(
        fires[1].0 + 2_000 >= before_resume,
        "catch-up fired an old instant: {} vs resume at {before_resume}",
        fires[1].0
    );
    signal(&s, &fires[1].1).await;
    let v = cli_ok(&s, &["delete", "skip1"]);
    assert!(v.is_null() || v.is_object(), "{v}");

    // Catch-up One after a stop/start: AllowAll, a guest that completes at once.
    cli_ok(
        &s,
        &[
            "create",
            "--name",
            "every1",
            "--workflow",
            "sdk-echo",
            "--cron",
            "* * * * * *",
            "--input-json",
            r#"{"n":1}"#,
            "--overlap",
            "allow-all",
            "--catchup",
            "one",
        ],
    );
    wait_for("three fires", || runs_of(&s, "every1").0.len() >= 3).await;
    s.shutdown().await;
    let down_from = now_ms();
    sleep(Duration::from_secs(4)).await;
    s.restart().await;
    let up_at = now_ms();
    wait_for("fires after restart", || {
        runs_of(&s, "every1")
            .0
            .iter()
            .any(|(t, _)| *t > up_at + 1_000)
    })
    .await;
    let (fires, _) = runs_of(&s, "every1");
    let times: Vec<u64> = fires.iter().map(|f| f.0).collect();
    let mut dedup = times.clone();
    dedup.dedup();
    assert_eq!(dedup, times, "duplicate fire times");
    // Inside the downtime (a second after the stop .. a second before the restart): at
    // most the one catch-up instant.
    let inside = times
        .iter()
        .filter(|t| **t > down_from + 1_000 && **t + 1_000 < up_at)
        .count();
    assert!(
        inside <= 1,
        "catch-up One fired {inside} missed instants: {times:?}"
    );
    // After the restart the schedule fires every second again.
    let after: Vec<u64> = times.iter().copied().filter(|t| *t > up_at).collect();
    for w in after.windows(2) {
        assert_eq!(w[1] - w[0], 1_000, "gap after restart: {after:?}");
    }
    cli_ok(&s, &["delete", "every1"]);
    s.shutdown().await;
}
