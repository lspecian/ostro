//! QA-01 whole-server kill campaign (plan 05-19). All `#[ignore]`: they build and spawn
//! the failpoint-enabled release binary and take minutes to hours.
//!
//! ```text
//! # CI smoke (ci-local + ci.yml), always from a wiped state dir:
//! rm -rf target/campaign-smoke
//! export OSTRO_CAMPAIGN_STATE_DIR=target/campaign-smoke OSTRO_CAMPAIGN_FAULTS=20
//! cargo test --release -p ostro-crashtest --features failpoints --test server_campaign \
//!     --locked -- --ignored --nocapture campaign
//! cargo test --release -p ostro-crashtest --features failpoints --test server_campaign \
//!     --locked -- --ignored --nocapture report
//!
//! # 05-20 gate (batches of shards; each batch resumes where the last stopped):
//! export OSTRO_CAMPAIGN_STATE_DIR=target/campaign-gate OSTRO_CAMPAIGN_FAULTS=10000 \
//!        OSTRO_CAMPAIGN_SHARDS=20 OSTRO_CAMPAIGN_PARALLEL=6
//! OSTRO_CAMPAIGN_SHARD_IDS=0-5 cargo test ... campaign   # then 6-11, 12-17, 18-19
//! cargo test ... report
//!
//! # Offline re-check of finished shards with the current checker (p8-gate), then the
//! # retry of a rebuild that failed only on a Node::open timeout (same exports as above;
//! # the replaced shard-NN.json files go to $OSTRO_CAMPAIGN_STATE_DIR/$OSTRO_CAMPAIGN_SUPERSEDED):
//! OSTRO_CAMPAIGN_SUPERSEDED=superseded-pre-recheck cargo test ... recheck
//! OSTRO_CAMPAIGN_SHARD_IDS=10 OSTRO_CAMPAIGN_SUPERSEDED=superseded-shard10-open-timeout \
//!     cargo test ... retry_rebuild
//! ```
//!
//! See `ostro_crashtest::campaign` for the variables, the iteration shape and the checks.

use std::collections::BTreeMap;
use std::time::Duration;

use ostro_crashtest::campaign::{self, CampaignCfg, history, run_campaign};
use ostro_crashtest::check::{crash_saga_expected, embedded_request_id};
use ostro_crashtest::server_target::{ServerTarget, deploy_crash_saga, failpoint_bin};
use serde_json::{Value, json};

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "spawns the failpoint server and kills it; run with --ignored"]
async fn campaign() {
    let cfg = CampaignCfg::from_env().unwrap_or_else(|e| panic!("{e}"));
    match run_campaign(&cfg).await {
        Ok(shards) => {
            for s in shards {
                println!(
                    "shard {:02}: kills {} ({:.2} s/kill) runs {} final {:?}",
                    s.shard,
                    s.kills,
                    s.kill_secs / s.kills.max(1) as f64,
                    s.runs_acked,
                    s.final_summary.as_ref().map(|f| &f.statuses)
                );
            }
        }
        Err(e) => panic!("campaign failed:\n{e}"),
    }
}

#[test]
#[ignore = "reads the campaign state dir; run with --ignored after `campaign`"]
fn report() {
    let cfg = CampaignCfg::from_env().unwrap_or_else(|e| panic!("{e}"));
    let (text, pass) = campaign::report(&cfg).unwrap_or_else(|e| panic!("{e}"));
    println!("{text}");
    assert!(pass, "campaign report: FAIL");
}

fn superseded_dir(cfg: &CampaignCfg) -> std::path::PathBuf {
    cfg.state_dir.join(
        std::env::var("OSTRO_CAMPAIGN_SUPERSEDED")
            .unwrap_or_else(|_| "superseded-pre-recheck".into()),
    )
}

/// Re-run the final check of every finished shard in `OSTRO_CAMPAIGN_SHARD_IDS` offline
/// (`campaign::recheck_shard`); unfinished shards are skipped.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "reads the campaign state dir and spawns the server on copies; run with --ignored"]
async fn recheck() {
    let cfg = CampaignCfg::from_env().unwrap_or_else(|e| panic!("{e}"));
    let sup = superseded_dir(&cfg);
    let mut bad = Vec::new();
    for &id in &cfg.shard_ids {
        match campaign::load_shard(&campaign::shard_path(&cfg.state_dir, id), &cfg) {
            Ok(Some(s)) if s.final_checked => {}
            other => {
                println!(
                    "shard {id:02}: skipped (not finished: {:?})",
                    other.map(|s| s.map(|s| s.kills))
                );
                continue;
            }
        }
        match campaign::recheck_shard(&cfg, id, &sup).await {
            Ok((s, vs)) => {
                println!(
                    "shard {id:02}: rechecked runs {} violations {} tally {:?}",
                    s.runs_acked,
                    vs.len(),
                    s.tally
                );
                for x in &vs {
                    println!("shard {id:02}:   {x}");
                }
                if !vs.is_empty() {
                    bad.push(id);
                }
            }
            Err(e) => {
                println!("shard {id:02}: recheck error: {e}");
                bad.push(id);
            }
        }
    }
    assert!(bad.is_empty(), "shards with violations or errors: {bad:?}");
}

/// `campaign::retry_rebuild_timeout` for every shard in `OSTRO_CAMPAIGN_SHARD_IDS`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "reads the campaign state dir; run with --ignored"]
async fn retry_rebuild() {
    let cfg = CampaignCfg::from_env().unwrap_or_else(|e| panic!("{e}"));
    let sup = superseded_dir(&cfg);
    for &id in &cfg.shard_ids {
        let s = campaign::retry_rebuild_timeout(&cfg, id, &sup)
            .await
            .unwrap_or_else(|e| panic!("shard {id}: {e}"));
        println!(
            "shard {id:02}: {}",
            s.anomalies.last().map_or("", String::as_str)
        );
    }
}

/// Cross-check `crash_saga_expected` against the real guest on a live server: a plain
/// completion with both signals, timeouts (null go/extra) and a compensation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "spawns the failpoint server; run with --ignored"]
async fn expected_matches_guest() {
    let http = reqwest::Client::new();
    let dir = tempfile::tempdir().expect("tempdir");
    let data = dir.path().join("data");
    let srv = ServerTarget::spawn(
        &failpoint_bin(),
        &data,
        None,
        &http,
        Duration::from_secs(60),
    )
    .await
    .unwrap_or_else(|e| panic!("{e}"));
    deploy_crash_saga(&http, &srv.base).await.expect("deploy");
    let cases = [
        (
            json!({"id": "e1", "steps": 4, "fail_pattern": "0123", "sleep_ms": 20, "signal_timeout_ms": 600000}),
            true,
        ),
        (
            json!({"id": "e2", "steps": 3, "fail_pattern": "", "sleep_ms": 10, "signal_timeout_ms": 150}),
            false,
        ),
        (
            json!({"id": "e3", "steps": 6, "fail_pattern": "01x0x1", "sleep_ms": 10, "signal_timeout_ms": 600000}),
            false,
        ),
        (
            json!({"id": "e4", "steps": 8, "fail_pattern": "3", "sleep_ms": 2200, "signal_timeout_ms": 600000}),
            true,
        ),
    ];
    let mut runs = BTreeMap::new();
    for (input, signals) in &cases {
        let r: Value = http
            .post(format!("{}/v1/workflows/crash-saga/runs", srv.base))
            .json(&json!({"input": input}))
            .send()
            .await
            .expect("start")
            .json()
            .await
            .expect("json");
        let id = r["run_id"].as_str().expect("run_id").to_owned();
        let (mut go, mut extra) = (Value::Null, Value::Null);
        if *signals {
            for (name, slot) in [("go", &mut go), ("extra", &mut extra)] {
                let p = json!({"request_id": format!("{id}-{name}"), "v": name});
                let st = http
                    .post(format!("{}/v1/runs/{id}/signals/{name}", srv.base))
                    .json(&json!({"payload": p, "request_id": format!("{id}-{name}")}))
                    .send()
                    .await
                    .expect("signal")
                    .status();
                assert_eq!(st, 200);
                *slot = p;
            }
        }
        runs.insert(
            id,
            crash_saga_expected(input, &go, &extra).expect("valid input"),
        );
    }
    for (id, want) in &runs {
        let mut got = Value::Null;
        for _ in 0..600 {
            got = http
                .get(format!("{}/v1/runs/{id}", srv.base))
                .send()
                .await
                .expect("get")
                .json()
                .await
                .expect("json");
            if got["status"] == "completed" || got["status"] == "failed" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(got["status"], "completed", "{got}");
        assert_eq!(&got["result"], want, "run {id}");
        let h = history(&http, &srv.base, id)
            .await
            .expect("history")
            .expect("exists");
        for e in h.iter().filter(|e| e.kind == "signal_payload") {
            assert!(
                embedded_request_id(&e.details["payload"]).is_some(),
                "{e:?}"
            );
        }
    }
}
