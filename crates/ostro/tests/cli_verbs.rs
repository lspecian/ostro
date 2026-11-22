//! CLI-06 (plan 05-14): every client verb of the real `ostro` binary against a spawned
//! `ostro dev --ephemeral`, over REST.
//!
//! - `--json` output is snapshot-tested (insta; run ids, timestamps, shas, sizes and meta
//!   record versions redacted). Human output is checked for key facts only.
//! - `--dry-run` on run / signal / cancel / terminate / retry / token complete changes
//!   nothing: run list, history length and run status are compared before and after.
//! - Exit codes: 0 ok, 1 API or connection error (JSON error body under `--json`),
//!   2 usage, 3 when `run --wait` / `result --wait` ends in a status other than
//!   `completed`.

mod common;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use common::*;
use serde_json::{Value, json};

/// One spawned dev server (P=4 for speed) plus a work dir for guest files.
struct Dev {
    srv: Spawned,
    addr: SocketAddr,
    work: tempfile::TempDir,
}

impl Dev {
    async fn start() -> Self {
        let work = tempfile::tempdir().expect("tempdir");
        let addr = free_addr();
        let a = addr.to_string();
        let mut cmd = dev(work.path(), &["--ephemeral", "--listen", &a]);
        cmd.env("OSTRO_NODE__PARTITIONS", "4")
            .env("OSTRO_LOG__LEVEL", "warn");
        let srv = Spawned::spawn(cmd);
        srv.wait_readyz(&http(), addr).await;
        Self { srv, addr, work }
    }

    fn cli(&self, args: &[&str]) -> (i32, String, String) {
        run_cli(self.addr, args)
    }

    fn json(&self, args: &[&str]) -> (i32, Value, String) {
        cli_json(self.addr, args)
    }

    /// `json` asserting exit 0.
    fn ok(&self, args: &[&str]) -> Value {
        let (code, body, err) = self.json(args);
        assert_eq!(code, 0, "{args:?}: {body}\n{err}");
        body
    }

    /// `ostro deploy <guest> --activate`.
    fn deploy(&self, guest: &str) -> Value {
        let f = write_guest(self.work.path(), guest);
        self.ok(&["deploy", f.to_str().unwrap(), "--activate"])
    }

    /// Start a run and return its id.
    fn start_run(&self, workflow: &str, input: &str) -> String {
        let b = self.ok(&["run", workflow, "--input", input]);
        assert_eq!(b["created"], true, "{b}");
        b["run_id"].as_str().expect("run_id").to_owned()
    }

    fn status(&self, run: &str) -> String {
        self.ok(&["inspect", run])["status"]
            .as_str()
            .expect("status")
            .to_owned()
    }

    fn history_len(&self, run: &str) -> usize {
        self.ok(&["history", run, "--limit", "1000"])["events"]
            .as_array()
            .expect("events")
            .len()
    }

    fn history_kinds(&self, run: &str) -> Vec<String> {
        self.ok(&["history", run, "--limit", "1000"])["events"]
            .as_array()
            .expect("events")
            .iter()
            .map(|e| e["kind"].as_str().unwrap_or_default().to_owned())
            .collect()
    }

    /// Wait until the run's (status, history length) is stable over three reads 200 ms
    /// apart and its history shows a wait, so a dry run is compared against a run that
    /// is parked, not one still making progress.
    fn settled(&self, run: &str) -> (String, usize) {
        let deadline = Instant::now() + T;
        let mut last: Option<(String, usize)> = None;
        let mut stable = 0;
        loop {
            let kinds = self.history_kinds(run);
            let cur = (self.status(run), kinds.len());
            let waiting = kinds
                .iter()
                .any(|k| k.contains("wait") || k.contains("token"));
            if waiting && last.as_ref() == Some(&cur) {
                stable += 1;
                if stable >= 2 {
                    return cur;
                }
            } else {
                stable = 0;
            }
            last = Some(cur);
            assert!(
                Instant::now() < deadline,
                "run {run} did not settle: {last:?} {kinds:?}"
            );
            std::thread::sleep(Duration::from_millis(200));
        }
    }

    fn run_ids(&self, workflow: &str) -> Vec<String> {
        self.ok(&["ls", "--workflow", workflow, "--limit", "1000"])["runs"]
            .as_array()
            .expect("runs")
            .iter()
            .map(|r| r["run_id"].as_str().expect("run_id").to_owned())
            .collect()
    }

    fn stop(mut self) {
        self.srv.stop_clean("INT");
    }
}

#[test]
fn help_lists_every_verb() {
    let mut cmd = command();
    cmd.arg("--help");
    let (st, out, err) = run_to_end(cmd);
    assert!(st.success(), "{err}");
    for verb in [
        "dev",
        "server",
        "openapi",
        "build-info",
        "run",
        "ls",
        "inspect",
        "history",
        "signal",
        "cancel",
        "terminate",
        "retry",
        "result",
        "token",
        "deploy",
    ] {
        assert!(
            out.lines()
                .any(|l| l.split_whitespace().next() == Some(verb)),
            "--help lacks {verb}:\n{out}"
        );
    }
    // Connection flags belong to the client verbs only.
    let mut cmd = command();
    cmd.args(["dev", "--json"]);
    let (st, _, _) = run_to_end(cmd);
    assert_eq!(st.code(), Some(2));
}

#[test]
fn usage_and_connection_errors() {
    // Nothing listens on a fresh free port.
    let addr = free_addr();
    let (code, out, err) = run_cli(addr, &["ls"]);
    assert_eq!(code, 1, "{out}{err}");
    assert!(err.contains("cannot reach the Ostro server"), "{err}");
    assert_eq!(err.lines().count(), 1, "{err}");

    let (code, _, err) = run_cli(addr, &["run", "w", "--retention", "7d"]);
    assert_eq!(code, 2);
    assert!(err.contains("168h"), "{err}");
    let (code, _, err) = run_cli(addr, &["signal", "r1", "x", "--payload", "not json"]);
    assert_eq!(code, 2);
    assert!(err.contains("not valid JSON"), "{err}");
    // https:// is accepted since 07-22 (SEC-01); an unreachable https server is a
    // transport error (exit 1), a bad --ca-file a usage error (exit 2).
    let https = format!("https://{addr}");
    let (code, _, err) = run_cli(addr, &["ls", "--server", https.as_str()]);
    assert_eq!(code, 1, "{err}");
    assert!(
        err.contains("cannot reach the Ostro server at https://"),
        "{err}"
    );
    let (code, _, err) = run_cli(
        addr,
        &[
            "ls",
            "--server",
            "https://x",
            "--ca-file",
            "/nonexistent/ca.pem",
        ],
    );
    assert_eq!(code, 2, "{err}");
    assert!(err.contains("--ca-file"), "{err}");
    let (code, _, err) = run_cli(addr, &["deploy", "/nonexistent/x.wasm"]);
    assert_eq!(code, 2, "{err}");
    assert!(err.contains("cannot read"), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn deploy_run_ls_inspect_history() {
    let _g = SERIAL.lock().await;
    let d = Dev::start().await;

    // deploy --activate: sdk-basic exports sdk-basic and sdk-echo.
    let f = write_guest(d.work.path(), "sdk-basic");
    let fs = f.to_str().unwrap();
    let up = d.ok(&["deploy", fs, "--activate"]);
    assert_eq!(up["upload"]["created"], true, "{up}");
    assert_eq!(
        up["upload"]["module"]["sha256"],
        sha_hex(&std::fs::read(&f).unwrap())
    );
    insta::assert_snapshot!("deploy_activate", redacted(&up, &[]));
    // Re-deploy: unchanged (200), human output says so.
    let (code, out, err) = d.cli(&["deploy", fs]);
    assert_eq!(code, 0, "{err}");
    assert!(out.contains("unchanged"), "{out}");
    assert!(
        out.contains("sdk-basic@1") && out.contains("sdk-echo@1"),
        "{out}"
    );

    // run --wait --json: start + result.
    let r = d.ok(&[
        "run",
        "sdk-basic",
        "--input",
        r#"{"name":"cli","n":4}"#,
        "--wait",
    ]);
    assert_eq!(r["result"]["status"], "completed", "{r}");
    assert_eq!(r["start"]["run_id"], r["result"]["run_id"]);
    insta::assert_snapshot!("run_wait", redacted(&r, &[]));

    // Input from @file; a run with tags, retention and an idempotency key.
    let input = d.work.path().join("echo.json");
    std::fs::write(&input, r#"{"hello":"file"}"#).unwrap();
    let at = format!("@{}", input.display());
    let e = d.ok(&[
        "run",
        "sdk-echo",
        "--input",
        &at,
        "--tag",
        "env=test",
        "--retention",
        "48h",
        "--idempotency-key",
        "echo-1",
        "--wait=30s",
    ]);
    assert_eq!(e["result"]["result"], json!({"hello": "file"}), "{e}");
    let echo = e["start"]["run_id"].as_str().unwrap().to_owned();

    let ls = d.ok(&["ls", "--workflow", "sdk-echo"]);
    insta::assert_snapshot!("ls", redacted(&ls, &[]));
    let tagged = d.ok(&["ls", "--tag", "env=test", "--status", "completed"]);
    assert_eq!(tagged["runs"].as_array().unwrap().len(), 1, "{tagged}");
    let ins = d.ok(&["inspect", &echo]);
    assert_eq!(ins["retention_ms"], 48 * 3600 * 1000, "{ins}");
    insta::assert_snapshot!("inspect", redacted(&ins, &[]));
    let mut hist = d.ok(&["history", &echo]);
    // 11-07/11-11: sink-stamped events carry `attempt = epoch << 32 | n`. The leader
    // epoch is not part of the contract under test, so keep `n` and mask the epoch.
    for ev in hist["events"].as_array_mut().unwrap() {
        if let Some(a) = ev.get("attempt").and_then(Value::as_u64) {
            assert!(a >> 32 >= 1, "attempt without an epoch: {a}");
            ev["attempt"] = json!(format!("[epoch]<<32|{}", a & 0xffff_ffff));
        }
    }
    insta::assert_snapshot!("history", redacted(&hist, &[(&echo, "[run_id]")]));
    let page = d.ok(&["history", &echo, "--limit", "1"]);
    assert_eq!(page["events"].as_array().unwrap().len(), 1);
    assert_eq!(page["next_from_seq"], 1, "{page}");

    // Human output: key facts only.
    let (code, out, _) = d.cli(&["ls", "--workflow", "sdk-echo"]);
    assert_eq!(code, 0);
    assert!(out.contains(&echo) && out.contains("completed"), "{out}");
    let (code, out, _) = d.cli(&["inspect", &echo]);
    assert_eq!(code, 0);
    assert!(out.contains("retries") && out.contains("env=test"), "{out}");
    let (code, out, _) = d.cli(&["history", &echo]);
    assert_eq!(code, 0);
    assert!(out.contains("SEQ") && out.contains("run_started"), "{out}");

    // run --dry-run: a new run would be created; nothing is.
    let before = d.run_ids("sdk-echo");
    let dry = d.ok(&["run", "sdk-echo", "--input", "1", "--dry-run", "--wait"]);
    insta::assert_snapshot!("run_dry_run", redacted(&dry, &[]));
    assert_eq!(dry, json!({"run_id": "", "created": true, "dry_run": true}));
    // An existing idempotency key reports the existing run.
    let dry = d.ok(&[
        "run",
        "sdk-echo",
        "--idempotency-key",
        "echo-1",
        "--dry-run",
    ]);
    assert_eq!(dry["run_id"], echo.as_str(), "{dry}");
    assert_eq!(dry["created"], false);
    let (code, out, _) = d.cli(&["run", "sdk-echo", "--dry-run"]);
    assert_eq!(code, 0);
    assert!(out.contains("dry run"), "{out}");
    assert_eq!(d.run_ids("sdk-echo"), before, "dry runs created a run");

    // --conflict reject on a reused key: 409, exit 1, JSON error body.
    let (code, body, err) = d.json(&[
        "run",
        "sdk-echo",
        "--idempotency-key",
        "echo-1",
        "--conflict",
        "reject",
    ]);
    assert_eq!(code, 1, "{body}");
    assert_eq!(body["error"]["details"]["existing_run_id"], echo.as_str());
    assert!(err.starts_with("ostro: error: "), "{err}");
    insta::assert_snapshot!(
        "run_conflict_reject",
        redacted(&body, &[(&echo, "[run_id]")])
    );

    // Unknown (well-formed) run: 404, exit 1 with the error body; a malformed id is 400.
    let (code, body, _) = d.json(&["inspect", &"0".repeat(29)]);
    assert_eq!(code, 1);
    insta::assert_snapshot!("inspect_not_found", redacted(&body, &[]));
    let (code, body, _) = d.json(&["history", "no-such-run"]);
    assert_eq!(code, 1);
    assert_eq!(body["error"]["code"], "invalid_argument", "{body}");

    // Tenant-qualified names travel as one %2F segment: an unknown tenant workflow is a
    // clean 404, not a routing error.
    let (code, body, _) = d.json(&["run", "acme/sdk-echo", "--tenant", "acme"]);
    assert_eq!(code, 1, "{body}");
    assert_eq!(body["error"]["code"], "not_found", "{body}");

    d.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn signals_fifo_dedupe_and_result() {
    let _g = SERIAL.lock().await;
    let d = Dev::start().await;
    d.deploy("signals");

    let run = d.start_run("signals", r#"{"mode":"fifo"}"#);
    let sub = [(run.as_str(), "[run_id]")];
    let before = d.settled(&run);

    // Dry run: validated, nothing delivered.
    let b = d.ok(&["signal", &run, "x", "--payload", "0", "--dry-run"]);
    assert_eq!(
        b,
        json!({"accepted": true, "duplicate": false, "dry_run": true})
    );
    let (code, out, _) = d.cli(&["signal", &run, "x", "--dry-run"]);
    assert_eq!(code, 0);
    assert!(out.contains("dry run"), "{out}");
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(d.settled(&run), before, "signal --dry-run changed the run");

    // Three deliveries; the repeated request id is acknowledged as a duplicate.
    let b = d.ok(&["signal", &run, "x", "--payload", "1", "--request-id", "r1"]);
    insta::assert_snapshot!("signal_first", redacted(&b, &sub));
    let b = d.ok(&["signal", &run, "x", "--payload", "1", "--request-id", "r1"]);
    assert_eq!(b["duplicate"], true, "{b}");
    insta::assert_snapshot!("signal_duplicate", redacted(&b, &sub));
    let (code, out, _) = d.cli(&["signal", &run, "x", "--payload", "2"]);
    assert_eq!(code, 0);
    assert!(out.contains("sent"), "{out}");
    let pf = d.work.path().join("p3.json");
    std::fs::write(&pf, "3").unwrap();
    let at = format!("@{}", pf.display());
    d.ok(&["signal", &run, "x", "--payload", &at]);

    let (code, res, err) = d.json(&["result", &run, "--wait"]);
    assert_eq!(code, 0, "{res}\n{err}");
    assert_eq!(res["result"], json!([1, 2, 3]), "{res}");
    insta::assert_snapshot!("signal_result", redacted(&res, &sub));
    let (code, out, _) = d.cli(&["result", &run]);
    assert_eq!(code, 0);
    assert!(
        out.contains("completed") && out.contains("[1,2,3]"),
        "{out}"
    );

    // Signalling a finished run: 409 run_terminal.
    let (code, body, _) = d.json(&["signal", &run, "x"]);
    assert_eq!(code, 1);
    assert_eq!(body["error"]["details"]["reason"], "run_terminal", "{body}");

    d.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn cancel_terminate_retry() {
    let _g = SERIAL.lock().await;
    let d = Dev::start().await;
    d.deploy("signals");
    let wait = r#"{"mode":"long_wait"}"#;

    // --- cancel ---
    let a = d.start_run("signals", wait);
    let sa = [(a.as_str(), "[run_id]")];
    let before = d.settled(&a);
    let b = d.ok(&["cancel", &a, "--dry-run"]);
    assert_eq!(b, json!({"already_requested": false, "dry_run": true}));
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(d.settled(&a), before, "cancel --dry-run changed the run");
    let b = d.ok(&["cancel", &a, "--reason", "bye"]);
    insta::assert_snapshot!("cancel", redacted(&b, &sa));
    // The guest observes the cancellation: result --wait ends cancelled -> exit 3.
    let (code, res, _) = d.json(&["result", &a, "--wait=30s"]);
    assert_eq!(code, 3, "{res}");
    assert_eq!(res["status"], "cancelled", "{res}");
    let (code, body, err) = d.json(&["cancel", &a]);
    assert_eq!(code, 1, "{body}");
    assert!(err.contains("run_terminal"), "{err}");
    insta::assert_snapshot!("cancel_terminal_conflict", redacted(&body, &sa));

    // --- terminate ---
    let t = d.start_run("signals", wait);
    let st = [(t.as_str(), "[run_id]")];
    let before = d.settled(&t);
    let b = d.ok(&["terminate", &t, "--dry-run", "--reason", "x"]);
    assert_eq!(b["dry_run"], true, "{b}");
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(d.settled(&t), before, "terminate --dry-run changed the run");
    let b = d.ok(&["terminate", &t, "--reason", "stop"]);
    insta::assert_snapshot!("terminate", redacted(&b, &st));
    assert_eq!(d.status(&t), "terminated");
    let (code, out, err) = d.cli(&["terminate", &t]);
    assert_eq!(code, 1, "{out}");
    assert!(
        err.starts_with("ostro: error: ") && err.contains("409"),
        "{err}"
    );

    // --- retry: resume in place (Terminated is resumable, D1) ---
    let hist = d.history_len(&t);
    let b = d.ok(&["retry", &t, "--dry-run"]);
    assert_eq!(
        b,
        json!({"mode": "resumed", "run_id": t.as_str(), "dry_run": true})
    );
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(
        d.status(&t),
        "terminated",
        "retry --dry-run resumed the run"
    );
    assert_eq!(d.history_len(&t), hist, "retry --dry-run changed history");
    let b = d.ok(&["retry", &t]);
    assert_eq!(b["run_id"], t.as_str(), "{b}");
    insta::assert_snapshot!("retry_resumed", redacted(&b, &st));
    d.settled(&t);
    d.ok(&["signal", &t, "x", "--payload", r#""go""#]);
    let (code, res, _) = d.json(&["result", &t, "--wait"]);
    assert_eq!(code, 0, "{res}");
    assert_eq!(res["result"], "go", "{res}");
    // Retrying a completed run: 409 run_completed.
    let (code, body, _) = d.json(&["retry", &t]);
    assert_eq!(code, 1, "{body}");
    insta::assert_snapshot!("retry_completed_conflict", redacted(&body, &st));

    // --- retry: restart as a new linked run (the cancelled run ended in a guest
    // terminal event, D1) ---
    let (code, out, _) = d.cli(&["retry", &a, "--dry-run"]);
    assert_eq!(code, 0);
    assert!(out.contains("would be restarted"), "{out}");
    let b = d.ok(&["retry", &a]);
    assert_eq!(b["mode"], "restarted", "{b}");
    let new = b["run_id"].as_str().unwrap().to_owned();
    assert_ne!(new, a);
    // Leave nothing running: terminate the restarted run (human output).
    let (code, out, _) = d.cli(&["terminate", &new]);
    assert_eq!(code, 0);
    assert!(out.contains("terminated"), "{out}");

    d.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn failed_run_exits_3() {
    let _g = SERIAL.lock().await;
    let d = Dev::start().await;
    d.deploy("signals");

    let (code, r, _) = d.json(&["run", "signals", "--input", r#"{"mode":"bogus"}"#, "--wait"]);
    assert_eq!(code, 3, "{r}");
    assert_eq!(r["result"]["status"], "failed", "{r}");
    let run = r["start"]["run_id"].as_str().unwrap().to_owned();
    insta::assert_snapshot!(
        "run_wait_failed",
        redacted(&r, &[(run.as_str(), "[run_id]")])
    );
    let (code, out, _) = d.cli(&["result", &run, "--wait"]);
    assert_eq!(code, 3);
    assert!(out.contains("failed") && out.contains("bad_mode"), "{out}");
    // Without --wait, reading a finished failed result is not an error.
    let (code, _, _) = d.cli(&["result", &run]);
    assert_eq!(code, 0);
    // A wait that ends before the run finishes is "not completed" too.
    let pending = d.start_run("signals", r#"{"mode":"long_wait"}"#);
    let (code, res, _) = d.json(&["result", &pending, "--wait=300ms"]);
    assert_eq!(code, 3, "{res}");
    assert_eq!(res["completed"], false, "{res}");

    d.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn token_complete() {
    let _g = SERIAL.lock().await;
    let d = Dev::start().await;
    d.deploy("tokens");

    let run = d.start_run("tokens", "{}");
    let sub = [(run.as_str(), "[run_id]")];
    // The guest publishes its token in the run status.
    let deadline = Instant::now() + T;
    let token = loop {
        let b = d.ok(&["inspect", &run]);
        if let Some(t) = b["published_status"]["token"].as_str() {
            break t.to_owned();
        }
        assert!(Instant::now() < deadline, "no token published: {b}");
        std::thread::sleep(Duration::from_millis(50));
    };
    let before = d.settled(&run);

    let b = d.ok(&["token", "complete", &token, "--payload", "1", "--dry-run"]);
    insta::assert_snapshot!("token_dry_run", redacted(&b, &sub));
    let (code, out, _) = d.cli(&["token", "complete", &token, "--dry-run"]);
    assert_eq!(code, 0);
    assert!(!out.contains(&token), "token echoed: {out}");
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(d.settled(&run), before, "token --dry-run changed the run");

    let pf: PathBuf = d.work.path().join("done.json");
    std::fs::write(&pf, r#"{"ok": true}"#).unwrap();
    let at = format!("@{}", pf.display());
    let (code, out, err) = d.cli(&["token", "complete", &token, "--payload", &at]);
    assert_eq!(code, 0, "{err}");
    assert!(!out.contains(&token), "token echoed: {out}");
    assert!(out.contains(&token[..8]) && out.contains(&run), "{out}");
    let (code, res, _) = d.json(&["result", &run, "--wait"]);
    assert_eq!(code, 0, "{res}");
    assert_eq!(res["result"], json!({"ok": true}), "{res}");
    insta::assert_snapshot!("token_result", redacted(&res, &sub));

    // A second completion is rejected; the token is never echoed in the error.
    let (code, body, err) = d.json(&["token", "complete", &token]);
    assert_eq!(code, 1, "{body}");
    assert!(!err.contains(&token) && !body.to_string().contains(&token));
    let (code, body, _) = d.json(&["token", "complete", "ot1_garbage"]);
    assert_eq!(code, 1);
    assert_eq!(
        body["error"]["details"]["reason"], "token_invalid",
        "{body}"
    );

    d.stop();
}
