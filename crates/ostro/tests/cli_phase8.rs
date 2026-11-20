//! 08-12: the Phase 8 verbs (`ostro schedules`, `ostro webhooks`) of the real `ostro`
//! binary: help lists every verb, clap rejects bad arguments with exit 2, and one round
//! trip against a spawned `ostro dev --ephemeral`.
//!
//! The round trip runs the success path of every verb against the real controllers (08-14
//! schedules, 08-15 webhooks): it deploys the `signals` guest, seals a webhook secret with
//! `ostro secret set --from-env`, and checks that no CLI output and no server log line
//! carries the secret value. 08-20 drives deliveries and fires end to end.

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

const CANARY: &str = "cnry-hook-7c3e9a1b5d2f4086";

/// `ostro <args> --json` asserting exit 0; stdout parsed. The canary never appears.
fn ok(addr: std::net::SocketAddr, args: &[&str]) -> serde_json::Value {
    let (code, body, err) = cli_json(addr, args);
    assert_eq!(code, 0, "{args:?}: {body}\n{err}");
    assert!(
        !body.to_string().contains(CANARY) && !err.contains(CANARY),
        "{args:?} leaks the secret: {body}\n{err}"
    );
    body
}

/// `ostro <args> --json` expecting exit 1 and an API error body with `status`.
fn fails(addr: std::net::SocketAddr, args: &[&str], status: u64) -> serde_json::Value {
    let (code, body, err) = cli_json(addr, args);
    assert_eq!(code, 1, "{args:?}: {body}\n{err}");
    assert!(
        !err.contains("not implemented on this server"),
        "{args:?}: {err}"
    );
    assert!(body["error"].is_object(), "{args:?}: {body}\n{err}");
    let text = format!("{body}\n{err}");
    assert!(
        text.contains(&status.to_string()),
        "{args:?}: HTTP {status} expected: {text}"
    );
    assert!(!text.contains(CANARY), "{args:?} leaks the secret: {text}");
    body
}

fn tag(v: &serde_json::Value) -> String {
    v["version_tag"]
        .as_u64()
        .unwrap_or_else(|| panic!("version_tag in {v}"))
        .to_string()
}

fn names(v: &serde_json::Value, list: &str) -> Vec<String> {
    v[list]
        .as_array()
        .unwrap_or_else(|| panic!("{list} in {v}"))
        .iter()
        .filter_map(|x| x["name"].as_str().map(str::to_owned))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn round_trip_against_ostro_dev() {
    let work = tempfile::tempdir().expect("tempdir");
    let addr = free_addr();
    let a = addr.to_string();
    let mut cmd = dev(work.path(), &["--ephemeral", "--listen", &a]);
    cmd.env("OSTRO_NODE__PARTITIONS", "4")
        .env("OSTRO_LOG__LEVEL", "debug");
    let mut srv = Spawned::spawn(cmd);
    srv.wait_readyz(&http(), addr).await;

    // A workflow for the schedule and the webhook to reference.
    let guest = write_guest(work.path(), "signals");
    ok(addr, &["deploy", guest.to_str().unwrap(), "--activate"]);

    // ---- schedules ----
    // An unknown workflow is a 400 from the controller, not a stub answer.
    let e = fails(
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
        400,
    );
    assert!(e.to_string().contains("does not exist"), "{e}");

    let created = ok(
        addr,
        &[
            "schedules",
            "create",
            "--name",
            "smoke",
            "--workflow",
            "signals",
            "--cron",
            "@daily",
            "--input-json",
            r#"{"mode":"fifo"}"#,
            "--overlap",
            "skip",
        ],
    );
    assert_eq!(created["name"], "smoke", "{created}");
    assert_eq!(created["workflow"], "signals", "{created}");
    assert_eq!(created["paused"], false, "{created}");
    // A duplicate name is a 409.
    fails(
        addr,
        &[
            "schedules",
            "create",
            "--name",
            "smoke",
            "--workflow",
            "signals",
            "--cron",
            "@daily",
        ],
        409,
    );

    let got = ok(addr, &["schedules", "get", "smoke"]);
    assert_eq!(got, created);
    let ls = ok(addr, &["schedules", "ls"]);
    assert_eq!(names(&ls, "schedules"), ["smoke"], "{ls}");

    let updated = ok(
        addr,
        &[
            "schedules",
            "update",
            "smoke",
            "--version-tag",
            &tag(&got),
            "--overlap",
            "allow-all",
        ],
    );
    assert_eq!(updated["overlap"], "allow_all", "{updated}");
    // The old tag is stale now: 409.
    fails(
        addr,
        &[
            "schedules",
            "update",
            "smoke",
            "--version-tag",
            &tag(&got),
            "--overlap",
            "skip",
        ],
        409,
    );

    let paused = ok(addr, &["schedules", "pause", "smoke"]);
    assert_eq!(paused["paused"], true, "{paused}");
    assert_eq!(ok(addr, &["schedules", "get", "smoke"])["paused"], true);
    let resumed = ok(addr, &["schedules", "resume", "smoke"]);
    assert_eq!(resumed["paused"], false, "{resumed}");

    // Trigger-now starts a run; a repeated request id returns the same run.
    let t1 = ok(
        addr,
        &["schedules", "trigger", "smoke", "--request-id", "r-1"],
    );
    let run = t1["run_id"]
        .as_str()
        .unwrap_or_else(|| panic!("{t1}"))
        .to_owned();
    let t2 = ok(
        addr,
        &["schedules", "trigger", "smoke", "--request-id", "r-1"],
    );
    assert_eq!(t2["run_id"], run.as_str(), "{t2}");
    let info = ok(addr, &["inspect", &run]);
    assert_eq!(info["workflow"], "signals", "{info}");
    fails(addr, &["schedules", "trigger", "nope"], 404);

    let current = ok(addr, &["schedules", "get", "smoke"]);
    ok(
        addr,
        &[
            "schedules",
            "delete",
            "smoke",
            "--version-tag",
            &tag(&current),
        ],
    );
    fails(addr, &["schedules", "get", "smoke"], 404);
    assert!(names(&ok(addr, &["schedules", "ls"]), "schedules").is_empty());

    // ---- webhooks ----
    // The verifier secret is sealed from the environment (never argv).
    let mut set = command();
    set.env("OSTRO_SERVER", format!("http://{addr}"))
        .env("HOOK_SECRET_VALUE", CANARY)
        .args([
            "secret",
            "set",
            "gh-hook",
            "--from-env",
            "HOOK_SECRET_VALUE",
            "--json",
        ])
        .stdin(std::process::Stdio::null());
    let (st, out, err) = run_to_end(set);
    assert!(st.success(), "{out}\n{err}");
    assert!(
        !out.contains(CANARY) && !err.contains(CANARY),
        "{out}\n{err}"
    );

    // A secret_ref naming no sealed secret is a 400.
    fails(
        addr,
        &[
            "webhooks",
            "create",
            "--name",
            "gh",
            "--action",
            "start",
            "--workflow",
            "signals",
            "--verifier",
            "github",
            "--secret-ref",
            "no-such-secret",
        ],
        400,
    );
    let hook = ok(
        addr,
        &[
            "webhooks",
            "create",
            "--name",
            "gh",
            "--action",
            "start",
            "--workflow",
            "signals",
            "--verifier",
            "github",
            "--secret-ref",
            "gh-hook",
        ],
    );
    assert_eq!(hook["name"], "gh", "{hook}");
    assert_eq!(hook["verifier"]["kind"], "github", "{hook}");
    assert_eq!(hook["verifier"]["secret_ref"], "gh-hook", "{hook}");
    assert_eq!(ok(addr, &["webhooks", "get", "gh"]), hook);
    assert_eq!(names(&ok(addr, &["webhooks", "ls"]), "webhooks"), ["gh"]);

    let upd = ok(
        addr,
        &[
            "webhooks",
            "update",
            "gh",
            "--version-tag",
            &tag(&hook),
            "--idempotency",
            "header:x-github-delivery",
        ],
    );
    assert!(upd.to_string().contains("x-github-delivery"), "{upd}");
    assert_eq!(upd["verifier"]["secret_ref"], "gh-hook", "{upd}");
    fails(
        addr,
        &[
            "webhooks",
            "update",
            "gh",
            "--version-tag",
            &tag(&hook),
            "--idempotency",
            "none",
        ],
        409,
    );

    ok(
        addr,
        &["webhooks", "delete", "gh", "--version-tag", &tag(&upd)],
    );
    fails(addr, &["webhooks", "get", "gh"], 404);
    assert!(names(&ok(addr, &["webhooks", "ls"]), "webhooks").is_empty());

    srv.stop_clean("INT");
    // The server never logged the secret value (debug level).
    let log = srv.stderr();
    assert!(!log.contains(CANARY), "the server log leaks the secret");
}
