//! 11-14: the Phase 11 run-operation verbs of the real `ostro` binary (CLI-06 contract:
//! human output by default, `--json` = the REST body, exit codes 0 ok / 1 API error /
//! 2 usage / 3 not completed).
//!
//! - `rerun_*`, `dlq_*`, `limits_*`, `ls_*`: against a spawned `ostro dev --ephemeral`
//!   with `guests/p11-rerun` (v1 fails at a chosen step) and `guests/p11-rerun-v2` (the
//!   fix). `--json` bodies are snapshot-tested with ids and times redacted.
//! - `bulk_*`: against an in-process `ostro_api::router` over the `ostro_api::fake`
//!   fakes (D-P11-24 e): request bodies are asserted on what the fake received.
//! - `help_*`: `--help` of every new verb is snapshot-tested. `ostro completions` does
//!   not exist at this HEAD (Phase 10 10-18), so there is no completions assertion.

mod common;
#[path = "common/p11.rs"]
mod p11;

use common::*;
use p11::*;
use serde_json::Value;

const PIPELINE: &str = "pipeline";

// ---------------------------------------------------------------------------------
// help
// ---------------------------------------------------------------------------------

fn help(args: &[&str]) -> String {
    let mut cmd = command();
    cmd.args(args).arg("--help");
    let (st, out, err) = run_to_end(cmd);
    assert!(st.success(), "{args:?}: {err}");
    out
}

#[test]
fn help_rerun_dlq_limits() {
    insta::assert_snapshot!("help_rerun", help(&["rerun"]));
    insta::assert_snapshot!("help_dlq", help(&["dlq"]));
    insta::assert_snapshot!("help_dlq_ls", help(&["dlq", "ls"]));
    insta::assert_snapshot!("help_dlq_requeue", help(&["dlq", "requeue"]));
    insta::assert_snapshot!("help_dlq_dismiss", help(&["dlq", "dismiss"]));
    insta::assert_snapshot!("help_limits", help(&["limits"]));
    insta::assert_snapshot!("help_ls", help(&["ls"]));
    let top = help(&[]);
    for verb in ["rerun", "dlq", "limits"] {
        assert!(
            top.lines()
                .any(|l| l.split_whitespace().next() == Some(verb)),
            "--help lacks {verb}:\n{top}"
        );
    }
}

#[test]
fn rerun_usage_errors_exit_2() {
    let addr = free_addr();
    for args in [
        &["rerun", "r1"][..],
        &["rerun", "r1", "--from-step", "s3", "--from-seq", "4"],
        &["rerun", "r1", "--from-step", "s3#0"],
        &["rerun", "r1", "--from-seq", "4", "--version", "newest"],
        &["dlq", "dismiss", "r1"],
        &["dlq", "requeue", "r1", "--mode", "rerun"],
        &["dlq", "requeue", "r1", "--from-seq", "3"],
    ] {
        let (code, _, err) = run_cli(addr, args);
        assert_eq!(code, 2, "{args:?}: {err}");
    }
}

// ---------------------------------------------------------------------------------
// rerun + ls search (spawned dev server)
// ---------------------------------------------------------------------------------

#[tokio::test]
async fn rerun_and_ls_search_flags() {
    let _g = SERIAL.lock().await;
    let d = P11Dev::start().await;
    d.deploy("p11-rerun");

    // v1 fails at s3: the source (terminal, a DLQ member).
    let (src, st) = d.run_wait(PIPELINE, r#"{"fail_at":3}"#, &["src"]);
    assert_eq!(st, "failed");
    // Search fixtures: two runs tagged blue+big, one blue only.
    let (a, _) = d.run_wait(PIPELINE, "{}", &["blue", "big"]);
    let (b, _) = d.run_wait(PIPELINE, "{}", &["blue", "big"]);
    let (c, _) = d.run_wait(PIPELINE, "{}", &["blue"]);

    // ls --tag blue --tag big --newest: AND of both tags, newest first.
    let v = d.ok(&["ls", "--tag", "blue", "--tag", "big", "--newest"]);
    assert_eq!(ids(&v, "runs"), vec![b.clone(), a.clone()], "{v}");
    let v = d.ok(&["ls", "--tag", "blue", "--tag", "big"]);
    assert_eq!(ids(&v, "runs"), vec![a.clone(), b.clone()], "{v}");
    let v = d.ok(&["ls", "--tag", "blue", "--newest"]);
    assert_eq!(
        ids(&v, "runs"),
        vec![c.clone(), b.clone(), a.clone()],
        "{v}"
    );
    // ls --dlq: only the failed source.
    let v = d.until(&["ls", "--dlq"], |v| !ids(v, "runs").is_empty());
    assert_eq!(ids(&v, "runs"), vec![src.clone()], "{v}");
    let (code, out, err) = d.cli(&["ls", "--dlq"]);
    assert_eq!(code, 0, "{err}");
    assert!(out.contains(&src) && out.contains("failed"), "{out}");

    // Fix: deploy and activate v2.
    d.deploy("p11-rerun-v2");
    let runs_before = ids(
        &d.ok(&["ls", "--workflow", PIPELINE, "--limit", "1000"]),
        "runs",
    );

    // Dry run: verdict, no run created.
    let dry = d.ok(&["rerun", &src, "--from-step", "s3", "--dry-run"]);
    assert!(dry["new_run_id"].is_null(), "{dry}");
    insta::assert_snapshot!(
        "rerun_dry_run",
        redacted(&dry, &[(src.as_str(), "[source]")])
    );
    let (code, out, err) = d.cli(&["rerun", &src, "--from-step", "s3", "--dry-run"]);
    assert_eq!(code, 0, "{err}");
    assert!(out.starts_with("dry run: rerun of"), "{out}");
    assert!(out.contains("verdict"), "{out}");
    let runs_after = ids(
        &d.ok(&["ls", "--workflow", PIPELINE, "--limit", "1000"]),
        "runs",
    );
    assert_eq!(runs_before, runs_after, "a dry run created a run");

    // Real rerun from s3 on the active v2: a new run that completes with 5.
    let r = d.ok(&[
        "rerun",
        &src,
        "--from-step",
        "s3",
        "--idempotency-key",
        "k1",
    ]);
    let new = r["new_run_id"].as_str().expect("new_run_id").to_owned();
    assert_ne!(new, src);
    assert_eq!(r["version"], 2, "{r}");
    insta::assert_snapshot!(
        "rerun_json",
        redacted(
            &r,
            &[(src.as_str(), "[source]"), (new.as_str(), "[new_run]")]
        )
    );
    let done = d.ok(&["result", &new, "--wait"]);
    assert_eq!(done["status"], "completed", "{done}");
    assert_eq!(done["result"], 5, "{done}");
    // Same key: the same run, `existing`; human output says so.
    let (code, out, err) = d.cli(&[
        "rerun",
        &src,
        "--from-step",
        "s3",
        "--idempotency-key",
        "k1",
    ]);
    assert_eq!(code, 0, "{err}");
    assert!(
        out.contains("existing rerun") && out.contains(&new),
        "{out}"
    );
    // Human success line.
    let (code, out, err) = d.cli(&["rerun", &src, "--from-seq", "1", "--version", "2"]);
    assert_eq!(code, 0, "{err}");
    assert!(out.starts_with("started rerun "), "{out}");

    // An unsafe cut (past the terminal event): 409 rerun_cut_unsafe, exit 1, the
    // reason details on stderr, the error body on stdout under --json.
    let hist = d.ok(&["history", &src, "--limit", "1000"]);
    let last = hist["events"]
        .as_array()
        .and_then(|e| e.last())
        .and_then(|e| e["seq"].as_u64())
        .expect("last seq");
    let past = (last + 1).to_string();
    let (code, body, err) = d.json(&["rerun", &src, "--from-seq", &past]);
    assert_eq!(code, 1, "{body}\n{err}");
    assert_eq!(
        body["error"]["details"]["reason"], "rerun_cut_unsafe",
        "{body}"
    );
    assert!(err.contains("rerun_cut_unsafe"), "{err}");
    insta::assert_snapshot!(
        "rerun_cut_unsafe",
        redacted(&body, &[(src.as_str(), "[source]")])
    );
    let (code, out, err) = d.cli(&["rerun", &src, "--from-seq", &past]);
    assert_eq!(code, 1, "{out}");
    assert!(out.is_empty(), "{out}");
    assert!(err.contains("reason rerun_cut_unsafe"), "{err}");
    let nearest = &body["error"]["details"]["nearest_safe_seq"];
    if let Some(n) = nearest.as_u64() {
        assert!(
            err.contains(&format!("nearest safe cut: --from-seq {n}")),
            "{err}"
        );
    } else {
        assert!(err.contains("no safe cut"), "{err}");
    }
    // A running / unknown source.
    let (code, _, err) = d.cli(&["rerun", "p0-nope", "--from-seq", "1"]);
    assert_eq!(code, 1, "{err}");

    d.stop();
}

// ---------------------------------------------------------------------------------
// DLQ + limits (spawned dev server)
// ---------------------------------------------------------------------------------

#[tokio::test]
async fn dlq_ls_dismiss_requeue_and_limits() {
    let _g = SERIAL.lock().await;
    let d = P11Dev::start().await;
    d.deploy("p11-rerun");

    let (a, st) = d.run_wait(PIPELINE, r#"{"fail_at":3}"#, &["team=a"]);
    assert_eq!(st, "failed");
    let (b, st) = d.run_wait(PIPELINE, r#"{"fail_at":2}"#, &["team=b"]);
    assert_eq!(st, "failed");
    let (ok, _) = d.run_wait(PIPELINE, "{}", &[]);

    let v = d.until(&["dlq", "ls"], |v| ids(v, "entries").len() == 2);
    assert_eq!(ids(&v, "entries"), vec![b.clone(), a.clone()], "{v}");
    let subst = [(a.as_str(), "[run_a]"), (b.as_str(), "[run_b]")];
    insta::assert_snapshot!("dlq_ls_json", redacted(&v, &subst));
    // Human table.
    let (code, out, err) = d.cli(&["dlq", "ls"]);
    assert_eq!(code, 0, "{err}");
    insta::assert_snapshot!("dlq_ls_table", mask_times(&mask(&out, &subst)));
    // Filters.
    let v = d.ok(&["dlq", "ls", "--tag", "team=a"]);
    assert_eq!(ids(&v, "entries"), vec![a.clone()], "{v}");
    let v = d.ok(&["dlq", "ls", "--workflow", "nope"]);
    assert!(ids(&v, "entries").is_empty(), "{v}");
    let (code, out, _) = d.cli(&["dlq", "ls", "--workflow", "nope"]);
    assert_eq!(code, 0);
    assert_eq!(out.trim(), "no DLQ entries");

    // Dismiss a: hidden by default, shown with --all.
    let (code, out, err) = d.cli(&["dlq", "dismiss", &a, "--reason", "flake"]);
    assert_eq!(code, 0, "{err}");
    assert_eq!(out.trim(), format!("dismissed {a} from the DLQ (flake)"));
    let v = d.ok(&["dlq", "ls"]);
    assert_eq!(ids(&v, "entries"), vec![b.clone()], "{v}");
    let v = d.ok(&["dlq", "ls", "--all"]);
    assert_eq!(ids(&v, "entries"), vec![b.clone(), a.clone()], "{v}");
    let e = v["entries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["run_id"] == a.as_str())
        .unwrap();
    assert_eq!(e["dismiss_reason"], "flake", "{e}");
    let (code, out, _) = d.cli(&["dlq", "ls", "--all"]);
    assert_eq!(code, 0);
    assert!(out.contains("dismissed: flake"), "{out}");
    // Dismissing a non-member: 409 dlq_not_member, exit 1.
    let (code, body, err) = d.json(&["dlq", "dismiss", &ok, "--reason", "x"]);
    assert_eq!(code, 1, "{body}\n{err}");
    assert_eq!(
        body["error"]["details"]["reason"], "dlq_not_member",
        "{body}"
    );
    assert!(err.contains("dlq_not_member"), "{err}");

    // Requeue b (auto: a guest failure is not resumable -> restart).
    let (code, out, err) = d.cli(&["dlq", "requeue", &b, "--dry-run"]);
    assert_eq!(code, 0, "{err}");
    assert_eq!(
        out.trim(),
        format!("dry run: {b} would be requeued (mode restart)")
    );
    let r = d.ok(&["dlq", "requeue", &b]);
    assert_eq!(r["mode"], "restart", "{r}");
    let new = r["new_run_id"].as_str().expect("new_run_id").to_owned();
    insta::assert_snapshot!(
        "dlq_requeue_json",
        redacted(&r, &[(b.as_str(), "[run_b]"), (new.as_str(), "[new_run]")])
    );
    let v = d.until(&["dlq", "ls"], |v| !ids(v, "entries").contains(&b));
    assert!(!ids(&v, "entries").contains(&b), "{v}");
    let (code, out, _) = d.cli(&["dlq", "ls", "--all"]);
    assert_eq!(code, 0);
    assert!(out.contains(&format!("requeued -> {new}")), "{out}");

    // Limits: --json equals GET /v1/limits; the human table has one row per limit.
    let lim = d.ok(&["limits"]);
    let rest: Value = http()
        .get(format!("http://{}/v1/limits", d.addr))
        .send()
        .await
        .expect("GET /v1/limits")
        .json()
        .await
        .expect("json");
    assert_eq!(lim, rest);
    let n = lim["limits"].as_array().expect("limits").len();
    assert!(n >= 14, "{lim}");
    insta::assert_snapshot!("limits_json", serde_json::to_string_pretty(&lim).unwrap());
    let (code, out, err) = d.cli(&["limits"]);
    assert_eq!(code, 0, "{err}");
    assert_eq!(out.lines().count(), n + 1, "{out}");
    insta::assert_snapshot!("limits_table", out);

    d.stop();
}
