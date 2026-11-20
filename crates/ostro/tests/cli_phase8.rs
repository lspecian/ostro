//! 08-12: the Phase 8 verbs (`ostro schedules`, `ostro webhooks`) of the real `ostro`
//! binary: help lists every verb, clap rejects bad arguments with exit 2, and one round
//! trip against a spawned `ostro dev --ephemeral`.
//!
//! The server's schedule and webhook controllers may still be stubs (08-14 / 08-15 land
//! in the same wave). The round trip therefore accepts either success or the mapped
//! "not implemented on this server" error (exit 1). A panic, a malformed request (400)
//! or any other error fails. 08-20 drives the full flow end to end.

mod common;

use common::*;

fn help(args: &[&str]) -> String {
    let mut cmd = command();
    cmd.args(args).arg("--help");
    let (st, out, err) = run_to_end(cmd);
    assert!(st.success(), "{args:?} --help: {err}");
    out
}

fn lists(out: &str, verb: &str) -> bool {
    out.lines()
        .any(|l| l.split_whitespace().next() == Some(verb))
}

#[test]
fn help_lists_every_phase8_verb() {
    let top = help(&[]);
    for verb in ["schedules", "webhooks"] {
        assert!(lists(&top, verb), "--help lacks {verb}:\n{top}");
    }
    let s = help(&["schedules"]);
    for verb in [
        "ls", "get", "create", "update", "delete", "pause", "resume", "trigger",
    ] {
        assert!(lists(&s, verb), "schedules --help lacks {verb}:\n{s}");
    }
    let w = help(&["webhooks"]);
    for verb in ["ls", "get", "create", "update", "delete"] {
        assert!(lists(&w, verb), "webhooks --help lacks {verb}:\n{w}");
    }
    let c = help(&["webhooks", "create"]);
    assert!(c.contains("--secret-ref"), "{c}");
    assert!(
        !c.contains("--secret "),
        "a flag taking secret material: {c}"
    );
}

#[test]
fn argument_validation_is_clap_exit_2() {
    // Nothing needs to listen: clap rejects these before any request.
    let addr = free_addr();
    for args in [
        &[
            "schedules",
            "create",
            "--name",
            "a",
            "--workflow",
            "w",
            "--cron",
            "@daily",
            "--overlap",
            "sometimes",
        ][..],
        &[
            "schedules",
            "create",
            "--name",
            "a",
            "--workflow",
            "w",
            "--cron",
            "@daily",
            "--catchup",
            "most",
        ][..],
        &["schedules", "create", "--name", "a", "--workflow", "w"][..],
        &["schedules", "update", "a"][..],
        &[
            "schedules",
            "create",
            "--name",
            "a",
            "--workflow",
            "w",
            "--cron",
            "@daily",
            "--catchup-window",
            "7d",
        ][..],
        &[
            "webhooks",
            "create",
            "--name",
            "h",
            "--action",
            "start",
            "--verifier",
            "md5",
        ][..],
        &[
            "webhooks",
            "create",
            "--name",
            "h",
            "--action",
            "start",
            "--verifier",
            "stripe",
            "--secret",
            "whsec_x",
        ][..],
        &[
            "webhooks",
            "create",
            "--name",
            "h",
            "--action",
            "start",
            "--verifier",
            "none",
            "--idempotency",
            "cookie:x",
        ][..],
        &["schedules", "bogus"][..],
    ] {
        let (code, out, err) = run_cli(addr, args);
        assert_eq!(code, 2, "{args:?}: {out}\n{err}");
    }
    // A local usage check after parsing (no --secret-ref) is exit 2 as well.
    let (code, _, err) = run_cli(
        addr,
        &[
            "webhooks",
            "create",
            "--name",
            "h",
            "--action",
            "start",
            "--workflow",
            "w",
            "--verifier",
            "github",
        ],
    );
    assert_eq!(code, 2, "{err}");
    assert!(err.contains("--secret-ref"), "{err}");
    // Connection refused: exit 1.
    let (code, _, err) = run_cli(addr, &["schedules", "ls"]);
    assert_eq!(code, 1, "{err}");
}

/// Success, or the mapped "not implemented" answer of a stub controller.
fn ok_or_unimplemented(code: i32, body: &serde_json::Value, err: &str, what: &str) -> bool {
    if code == 0 {
        return true;
    }
    assert_eq!(code, 1, "{what}: {body}\n{err}");
    assert!(
        err.contains("not implemented on this server"),
        "{what}: unexpected error: {body}\n{err}"
    );
    assert!(
        !err.contains("HTTP 400"),
        "{what}: malformed request: {err}"
    );
    false
}

#[tokio::test(flavor = "multi_thread")]
async fn round_trip_against_ostro_dev() {
    let work = tempfile::tempdir().expect("tempdir");
    let addr = free_addr();
    let a = addr.to_string();
    let mut cmd = dev(work.path(), &["--ephemeral", "--listen", &a]);
    cmd.env("OSTRO_NODE__PARTITIONS", "4")
        .env("OSTRO_LOG__LEVEL", "warn");
    let mut srv = Spawned::spawn(cmd);
    srv.wait_readyz(&http(), addr).await;

    let (code, body, err) = cli_json(
        addr,
        &[
            "schedules",
            "create",
            "--name",
            "smoke",
            "--workflow",
            "greet",
            "--cron",
            "@daily",
        ],
    );
    if ok_or_unimplemented(code, &body, &err, "schedules create") {
        assert_eq!(body["name"], "smoke", "{body}");
        let (code, ls, err) = cli_json(addr, &["schedules", "ls"]);
        assert_eq!(code, 0, "{ls}\n{err}");
        let names: Vec<&str> = ls["schedules"]
            .as_array()
            .unwrap_or_else(|| panic!("{ls}"))
            .iter()
            .filter_map(|s| s["name"].as_str())
            .collect();
        assert!(names.contains(&"smoke"), "{ls}");
    } else {
        // Stub: `ls --json` prints the error body as JSON (exit 1, mapped message).
        let (code, ls, err) = cli_json(addr, &["schedules", "ls"]);
        ok_or_unimplemented(code, &ls, &err, "schedules ls");
        assert!(ls["error"].is_object(), "{ls}");
    }

    let (code, body, err) = cli_json(addr, &["webhooks", "ls"]);
    if ok_or_unimplemented(code, &body, &err, "webhooks ls") {
        assert!(body["webhooks"].is_array(), "{body}");
    }

    srv.stop_clean("INT");
}
