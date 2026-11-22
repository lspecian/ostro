//! `ostro dev --time-skipping[=auto|manual]` (plan 10-10, SDK-06, D-P10-6), spawned binary:
//!
//! - without `--ephemeral`: exit 2, the message names `--ephemeral`;
//! - `ostro server` with `OSTRO_DEV__TIME_SKIPPING=auto`: exit 2;
//! - an explicit worker listener (`OSTRO_WORKERS__ENABLED=true`): exit 2;
//! - `--ephemeral --time-skipping`: the frozen ready line parses, a stderr notice names the
//!   mode, `GET /v1/dev/time` is 200 with mode `auto`; `=manual` gives `manual`;
//! - `ostro dev --ephemeral` alone: 409 `time_skipping_disabled`.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

#[path = "common/p10.rs"]
mod p10;

use std::time::{Duration, Instant};

use p10::{http_get, run_cli_in, spawn_dev};
use serde_json::Value;

#[test]
fn time_skipping_without_ephemeral_exits_2() {
    let cwd = tempfile::tempdir().expect("tempdir");
    let (code, out, err) = run_cli_in(
        cwd.path(),
        &["dev", "--listen", "127.0.0.1:0", "--time-skipping"],
        &[],
    );
    assert_eq!(code, 2, "stdout {out}\nstderr {err}");
    assert!(err.contains("--ephemeral"), "{err}");
    assert!(err.contains("--time-skipping"), "{err}");
    assert!(out.is_empty(), "no ready line: {out}");
    assert!(
        !cwd.path().join(".ostro").exists(),
        "no data dir was created"
    );
}

#[test]
fn server_with_dev_time_skipping_env_exits_2() {
    let cwd = tempfile::tempdir().expect("tempdir");
    let data = cwd.path().join("data");
    let (code, out, err) = run_cli_in(
        cwd.path(),
        &[
            "server",
            "--listen",
            "127.0.0.1:0",
            "--data-dir",
            data.to_str().expect("utf8"),
        ],
        &[("OSTRO_DEV__TIME_SKIPPING", "auto")],
    );
    assert_eq!(code, 2, "stdout {out}\nstderr {err}");
    assert!(err.contains("ostro server"), "{err}");
    assert!(!data.exists(), "no data dir was created");
}

#[test]
fn explicit_worker_listener_exits_2() {
    let cwd = tempfile::tempdir().expect("tempdir");
    let (code, out, err) = run_cli_in(
        cwd.path(),
        &[
            "dev",
            "--ephemeral",
            "--listen",
            "127.0.0.1:0",
            "--time-skipping=manual",
        ],
        &[("OSTRO_WORKERS__ENABLED", "true")],
    );
    assert_eq!(code, 2, "stdout {out}\nstderr {err}");
    assert!(err.contains("worker listener"), "{err}");
}

#[test]
fn bad_time_skipping_value_is_a_usage_error() {
    let cwd = tempfile::tempdir().expect("tempdir");
    let (code, _, err) = run_cli_in(
        cwd.path(),
        &["dev", "--ephemeral", "--time-skipping=sometimes"],
        &[],
    );
    assert_eq!(code, 2, "{err}");
}

fn dev_time(addr: std::net::SocketAddr) -> (u16, Value) {
    let (st, body) = http_get(addr, "/v1/dev/time");
    (st, serde_json::from_str(&body).unwrap_or(Value::Null))
}

/// Wait for a stderr line containing `needle` (the notice is printed before startup).
fn stderr_has(dev: &p10::DevProc, needle: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut all = String::new();
    loop {
        all.push_str(&dev.stderr_so_far());
        if all.contains(needle) || Instant::now() > deadline {
            return all;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn ephemeral_time_skipping_auto_and_manual() {
    let dev = spawn_dev(&["--time-skipping"], &[]);
    assert_eq!(dev.ready.name, "ostro dev", "{:?}", dev.ready);
    assert!(dev.ready.ephemeral, "{:?}", dev.ready);
    let err = stderr_has(&dev, "time skipping: auto");
    assert!(
        err.contains("time skipping: auto (dev only; data is ephemeral)"),
        "{err}"
    );
    let (st, v) = dev_time(dev.addr);
    assert_eq!(st, 200, "{v}");
    assert_eq!(v["mode"], "auto", "{v}");
    assert_eq!(v["offset_ms"], 0, "{v}");
    drop(dev);

    let dev = spawn_dev(&["--time-skipping=manual"], &[]);
    let (st, v) = dev_time(dev.addr);
    assert_eq!(st, 200, "{v}");
    assert_eq!(v["mode"], "manual", "{v}");
}

#[test]
fn ephemeral_without_time_skipping_is_409() {
    let dev = spawn_dev(&[], &[]);
    let (st, v) = dev_time(dev.addr);
    assert_eq!(st, 409, "{v}");
    assert_eq!(
        v["error"]["details"]["reason"], "time_skipping_disabled",
        "{v}"
    );
    assert!(!dev.stderr_so_far().contains("time skipping:"));
}
