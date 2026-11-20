//! SC3 against S3 (plan 08-20): the bottomless backup + `ostro restore` flow once against an
//! S3-compatible store (SeaweedFS in CI, P8-D25), through the real binary.
//!
//! A server with `[backup] url = s3://<bucket>/<unique prefix>` (endpoint, path style,
//! `allow_http`) runs a small workload and is killed with SIGKILL. `ostro restore --from
//! s3://...` rebuilds a fresh data dir (manifest validated, every partition's state hash
//! checked at its verify offset, cross-partition consistency), and a server started on the
//! restored dir serves every run that completed before the cut, with its result.
//!
//! Env-gated on the Phase 6 convention `OSTRO_TEST_S3_URL` / `_ACCESS_KEY` / `_SECRET_KEY` /
//! `_BUCKET` (the CI "Start SeaweedFS" step). Unset: prints `SKIP (human_needed)` and passes.
#![cfg(unix)]

mod common;

use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common::{Api, SERIAL, Spawned, T, command, dev, free_addr, guest_wasm, http, run_to_end};
use reqwest::StatusCode;
use serde_json::{Value, json};

const RUN_LIMIT: Duration = Duration::from_secs(60);

struct S3 {
    endpoint: String,
    access_key: String,
    secret_key: String,
    url: String,
}

fn s3_env() -> Option<S3> {
    let Ok(endpoint) = std::env::var("OSTRO_TEST_S3_URL") else {
        println!("SKIP (human_needed): OSTRO_TEST_S3_URL unset (CI SeaweedFS step / Docker host)");
        return None;
    };
    let var = |n: &str| {
        std::env::var(n).unwrap_or_else(|_| panic!("{n} must be set with OSTRO_TEST_S3_URL"))
    };
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    Some(S3 {
        endpoint,
        access_key: var("OSTRO_TEST_S3_ACCESS_KEY"),
        secret_key: var("OSTRO_TEST_S3_SECRET_KEY"),
        url: format!(
            "s3://{}/bottomless-s3/{}-{nanos}",
            var("OSTRO_TEST_S3_BUCKET"),
            std::process::id()
        ),
    })
}

async fn spawn(cwd: &Path, data: &Path, addr: &str, s3: Option<&S3>) -> Spawned {
    let mut cmd = dev(
        cwd,
        &[
            "--data-dir",
            data.to_str().expect("utf-8"),
            "--listen",
            addr,
        ],
    );
    cmd.env("OSTRO_NODE__PARTITIONS", "4")
        .env("OSTRO_LOG__LEVEL", "warn");
    if let Some(s3) = s3 {
        cmd.env("OSTRO_BACKUP__URL", &s3.url)
            .env("OSTRO_BACKUP__ENDPOINT", &s3.endpoint)
            .env("OSTRO_BACKUP__REGION", "us-east-1")
            .env("OSTRO_BACKUP__URL_STYLE", "path")
            .env("OSTRO_BACKUP__ALLOW_HTTP", "true")
            .env("OSTRO_BACKUP__ACCESS_KEY_ID", &s3.access_key)
            .env("OSTRO_BACKUP__SECRET_ACCESS_KEY", &s3.secret_key)
            .env("OSTRO_BACKUP__SEAL_INTERVAL", "300ms");
    }
    let s = Spawned::spawn(cmd);
    s.wait_readyz(&http(), addr.parse().expect("addr")).await;
    s
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bottomless_s3_restore_round_trip() {
    let Some(s3) = s3_env() else {
        return;
    };
    let wasm = guest_wasm("sdk-basic");
    let _serial = SERIAL.lock().await;
    let tmp = tempfile::tempdir().expect("tmp");
    let src = tmp.path().join("src");

    let addr = free_addr().to_string();
    let mut a = spawn(tmp.path(), &src, &addr, Some(&s3)).await;
    let api = Api::new(addr.parse().expect("addr"));
    let (st, body) = api.upload(&wasm).await;
    assert_eq!(st, StatusCode::CREATED, "{body}");
    let mut runs = Vec::new();
    for i in 0..8 {
        runs.push(
            api.start_new(
                "sdk-basic",
                json!({"input": {"name": format!("s3-{i}"), "n": 2}}),
            )
            .await,
        );
    }
    for r in &runs {
        let v = api.wait_terminal(r, RUN_LIMIT).await;
        assert_eq!(v["status"], "completed", "{v}");
    }
    // Several seal intervals after the last completion, then SIGKILL.
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    let text = api
        .http
        .get(api.url("/metrics"))
        .send()
        .await
        .expect("GET /metrics")
        .text()
        .await
        .expect("metrics text");
    assert!(
        text.contains("ostro_backup_upload_seconds"),
        "the leader uploaded nothing: no ostro_backup_upload_seconds"
    );
    let kill_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_millis() as u64;
    a.signal("KILL");
    a.wait_exit(T);

    // Restore from S3 (credentials from the environment, never flags).
    let restored = tmp.path().join("restored");
    let mut cmd = command();
    cmd.args(["restore", "--from", &s3.url, "--data-dir"])
        .arg(&restored)
        .args([
            "--endpoint",
            &s3.endpoint,
            "--region",
            "us-east-1",
            "--json",
        ])
        .env("OSTRO_BACKUP_ACCESS_KEY_ID", &s3.access_key)
        .env("OSTRO_BACKUP_SECRET_ACCESS_KEY", &s3.secret_key);
    if s3.endpoint.starts_with("http://") {
        cmd.arg("--allow-http");
    }
    let (st, out, err) = run_to_end(cmd);
    assert!(st.success(), "restore {}: {st:?}\n{out}\n{err}", s3.url);
    let rep: Value = serde_json::from_str(&out).unwrap_or_else(|e| panic!("report ({e}): {out}"));
    let parts = rep["partitions"].as_array().expect("partitions");
    assert_eq!(parts.len(), 4, "{rep}");
    for p in parts {
        assert!(
            p["verify_state_hash"]
                .as_str()
                .is_some_and(|h| h.len() == 64),
            "{p}"
        );
    }
    assert!(rep["meta_index"].as_u64().is_some(), "{rep}");
    let cut_at_ms = rep["cut_at_ms"].as_u64().expect("cut_at_ms");
    println!(
        "bottomless_s3: generation {} kill - cut_at_ms {} ms, restore total {} ms",
        rep["generation"],
        kill_ms.saturating_sub(cut_at_ms),
        rep["durations"]["total_ms"]
    );
    assert!(
        !out.contains(&s3.secret_key) && !err.contains(&s3.secret_key),
        "the restore output leaks the secret key"
    );

    // A server on the restored dir serves every run completed before the cut.
    let addr = free_addr().to_string();
    let mut b = spawn(tmp.path(), &restored, &addr, None).await;
    let api = Api::new(addr.parse().expect("addr"));
    for r in &runs {
        let v = api.run(r).await;
        assert_eq!(v["status"], "completed", "{v}");
    }
    b.stop_clean("INT");
}
