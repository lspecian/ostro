//! `ostro backup` / `ostro restore` through the real binary (BACK-02/03, plan 08-18):
//!
//! (a) a server with `[backup] url = file://<bucket>` runs a workload, is killed with
//!     SIGKILL, `ostro restore --from file://<bucket>` rebuilds a new data directory, and a
//!     server started on it serves every run that completed before the cut;
//! (b) `ostro backup --to f.tar` (online, `GET /v1/admin/backup`) then `ostro restore --from
//!     f.tar`;
//! (c) `ostro backup --data-dir <stopped dir> --to g.tar` (offline) then restore.
//!
//! The server is idle after its runs complete, so all three restores reproduce the same
//! per-partition digests, and restoring the same source twice is deterministic.
#![cfg(unix)]

mod common;

use std::path::Path;
use std::time::Duration;

use common::{Api, SERIAL, Spawned, T, command, dev, free_addr, guest_wasm, http, run_to_end};
use reqwest::StatusCode;
use serde_json::{Value, json};

const P: &str = "4";
const RUN_LIMIT: Duration = Duration::from_secs(60);

async fn spawn(cwd: &Path, data: &Path, addr: &str, bucket: Option<&Path>) -> Spawned {
    let mut cmd = dev(
        cwd,
        &[
            "--data-dir",
            data.to_str().expect("utf-8"),
            "--listen",
            addr,
        ],
    );
    cmd.env("OSTRO_NODE__PARTITIONS", P)
        .env("OSTRO_LOG__LEVEL", "warn");
    if let Some(b) = bucket {
        cmd.env("OSTRO_BACKUP__URL", format!("file://{}", b.display()))
            .env("OSTRO_BACKUP__SEAL_INTERVAL", "200ms");
    }
    let s = Spawned::spawn(cmd);
    s.wait_readyz(&http(), addr.parse().expect("addr")).await;
    s
}

/// `ostro restore --json` -> the report.
fn restore(from: &str, data: &Path, extra: &[&str]) -> Value {
    let mut cmd = command();
    cmd.args(["restore", "--from", from, "--data-dir"])
        .arg(data)
        .args(["--json"])
        .args(extra);
    let (st, out, err) = run_to_end(cmd);
    assert!(st.success(), "restore {from}: {st:?}\n{out}\n{err}");
    serde_json::from_str(&out).unwrap_or_else(|e| panic!("report json ({e}): {out}"))
}

fn digests(report: &Value) -> Vec<(u64, u64, String)> {
    report["partitions"]
        .as_array()
        .expect("partitions")
        .iter()
        .map(|p| {
            (
                p["p"].as_u64().expect("p"),
                p["cut_offset"].as_u64().expect("cut"),
                p["digest"].as_str().expect("digest").to_owned(),
            )
        })
        .collect()
}

fn backup(args: &[&str]) -> (bool, String, String) {
    let mut cmd = command();
    cmd.arg("backup").args(args);
    let (st, out, err) = run_to_end(cmd);
    (st.success(), out, err)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bucket_online_and_offline_round_trips() {
    let wasm = guest_wasm("sdk-basic");
    let _serial = SERIAL.lock().await;
    let tmp = tempfile::tempdir().expect("tmp");
    let (src, bucket) = (tmp.path().join("src"), tmp.path().join("bucket"));
    std::fs::create_dir_all(&bucket).expect("bucket");

    // A server backing up to file://bucket, with a small workload.
    let addr = free_addr().to_string();
    let mut a = spawn(tmp.path(), &src, &addr, Some(&bucket)).await;
    let api = Api::new(addr.parse().expect("addr"));
    let (st, body) = api.upload(&wasm).await;
    assert_eq!(st, StatusCode::CREATED, "{body}");
    let mut runs = Vec::new();
    for i in 0..6 {
        runs.push(
            api.start_new(
                "sdk-basic",
                json!({"input": {"name": format!("r{i}"), "n": 3}}),
            )
            .await,
        );
    }
    for r in &runs {
        let v = api.wait_terminal(r, RUN_LIMIT).await;
        assert_eq!(v["status"], "completed", "{v}");
    }

    // (b) Online backup while the server runs.
    let f_tar = tmp.path().join("f.tar");
    let (ok, out, err) = backup(&[
        "--to",
        f_tar.to_str().expect("utf-8"),
        "--server",
        &format!("http://{addr}"),
        "--json",
    ]);
    assert!(ok, "online backup: {out}\n{err}");
    let v: Value = serde_json::from_str(&out).expect("backup json");
    assert_eq!(v["mode"], "online");
    assert!(v["bytes"].as_u64().is_some_and(|b| b > 0), "{v}");
    assert!(f_tar.is_file());

    // Two seal intervals and more, then SIGKILL (the bottomless cut covers the idle log).
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert!(bucket.join("manifest/current").is_file());
    a.signal("KILL");
    a.wait_exit(T);

    // (a) Restore from the bucket, twice: deterministic and verified.
    let url = format!("file://{}", bucket.display());
    let r1_dir = tmp.path().join("r1");
    let r1 = restore(&url, &r1_dir, &[]);
    let r1b = restore(&url, &tmp.path().join("r1b"), &[]);
    assert_eq!(digests(&r1), digests(&r1b), "restore is deterministic");
    assert_eq!(r1["partitions"].as_array().map(Vec::len), Some(4));
    assert!(r1["meta_index"].as_u64().is_some(), "{r1}");
    assert_ne!(r1["incarnation"], r1["source_incarnation"]);
    // Human output: the table and the takeover hint.
    let mut cmd = command();
    cmd.args(["restore", "--from", &url, "--data-dir"])
        .arg(tmp.path().join("r1c"))
        .arg("--takeover");
    let (st, out, err) = run_to_end(cmd);
    assert!(st.success(), "{out}\n{err}");
    assert!(
        out.contains("STATE_HASH") && out.contains("takeover = true"),
        "{out}"
    );

    // A server on the restored directory serves every run started (and completed) before
    // the cut, with its result.
    let addr = free_addr().to_string();
    let mut b = spawn(tmp.path(), &r1_dir, &addr, None).await;
    let api = Api::new(addr.parse().expect("addr"));
    for r in &runs {
        let v = api.run(r).await;
        assert_eq!(v["status"], "completed", "{v}");
    }
    let (st, wf) = api.get("/v1/workflows").await;
    assert_eq!(st, StatusCode::OK);
    assert!(wf.to_string().contains("sdk-basic"), "meta restored: {wf}");
    b.stop_clean("INT");

    // (b) Restore the online backup file.
    let r2 = restore(f_tar.to_str().expect("utf-8"), &tmp.path().join("r2"), &[]);

    // (c) Offline backup of the killed source directory, then restore.
    let g_tar = tmp.path().join("g.tar");
    let (ok, out, err) = backup(&[
        "--data-dir",
        src.to_str().expect("utf-8"),
        "--to",
        g_tar.to_str().expect("utf-8"),
    ]);
    assert!(ok, "offline backup: {out}\n{err}");
    assert!(out.contains("offline backup"), "{out}");
    let r3 = restore(g_tar.to_str().expect("utf-8"), &tmp.path().join("r3"), &[]);

    assert_eq!(
        digests(&r2),
        digests(&r3),
        "online and offline backups agree"
    );
    assert_eq!(digests(&r1), digests(&r3), "bucket and backup files agree");

    // Refusals: a non-empty directory, a missing file.
    let mut cmd = command();
    cmd.args(["restore", "--from", &url, "--data-dir"])
        .arg(&r1_dir);
    let (st, _, err) = run_to_end(cmd);
    assert_eq!(st.code(), Some(1), "{err}");
    assert!(err.contains("not empty"), "{err}");
    let mut cmd = command();
    cmd.args(["restore", "--from"])
        .arg(tmp.path().join("missing.tar"))
        .arg("--data-dir")
        .arg(tmp.path().join("r4"));
    let (st, _, err) = run_to_end(cmd);
    assert_eq!(st.code(), Some(1), "{err}");
    assert!(err.contains("not a file"), "{err}");
}

#[test]
fn help_lists_backup_and_restore() {
    let mut cmd = command();
    cmd.arg("--help");
    let (st, out, _) = run_to_end(cmd);
    assert!(st.success());
    for verb in ["backup", "restore"] {
        assert!(
            out.lines()
                .any(|l| l.split_whitespace().next() == Some(verb)),
            "--help lacks {verb}:\n{out}"
        );
    }
    let mut cmd = command();
    cmd.args(["restore", "--help"]);
    let (_, out, _) = run_to_end(cmd);
    for flag in ["--from", "--data-dir", "--at", "--takeover", "--json"] {
        assert!(out.contains(flag), "restore --help lacks {flag}:\n{out}");
    }
    assert!(
        !out.contains("--secret"),
        "no flag takes secret material:\n{out}"
    );
    // Usage errors are exit 2.
    let mut cmd = command();
    cmd.args(["restore", "--from", "x"]);
    assert_eq!(run_to_end(cmd).0.code(), Some(2));
    let mut cmd = command();
    cmd.args(["restore", "--from", "x", "--data-dir", "d", "--at", "soon"]);
    assert_eq!(run_to_end(cmd).0.code(), Some(2));
}
