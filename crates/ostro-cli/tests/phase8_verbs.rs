//! 08-12: `ostro schedules` / `ostro webhooks` against an in-process
//! `ostro_api::router_with(FakeControlPlane, FakePhase8)` on 127.0.0.1:0.
//!
//! The verbs are parsed with clap exactly as the `ostro` binary embeds them and run
//! through `schedules::render` / `webhooks::render`, which return the text the verb
//! prints on stdout, so the tests can scan it (secret material) and check exit codes
//! via the shared `Out::error` mapping.

use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use ostro_api::fake::{FakeControlPlane, FakePhase8, fake_module, sha256_hex};
use ostro_api::{ApiConfig, ApiError, ApiExtensions, ControlPlane};
use ostro_cli::ClientCommand;
use ostro_cli::client::{CallError, Client};
use ostro_cli::commands::{schedules, webhooks};
use ostro_cli::output::{Out, Outcome, exit_code};
use serde_json::{Value, json};

#[derive(Debug, Parser)]
#[command(name = "ostro")]
struct Cli {
    #[command(subcommand)]
    cmd: ClientCommand,
}

/// A dummy secret value: the CLI has no way to send it, the fake never stores it, and
/// it must never appear in any output.
const DUMMY_SECRET: &str = "whsec_DUMMY_SECRET_VALUE_0123456789";

struct Srv {
    url: String,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Srv {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve(app: axum::Router) -> Srv {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    Srv {
        url: format!("http://{addr}"),
        task,
    }
}

/// The fake surface with workflow `report@1` registered (and active).
async fn fake() -> (Srv, Client) {
    let cp = Arc::new(FakeControlPlane::new());
    let p8 = FakePhase8::new(cp.clone());
    let dyn_cp: Arc<dyn ControlPlane> = cp;
    let srv = serve(ostro_api::router_with(
        dyn_cp,
        ApiConfig::default(),
        p8.extensions(),
    ))
    .await;
    let c = Client::new(&srv.url, Duration::from_secs(10)).expect("client");
    let wasm = fake_module(&[("report", 1)]);
    c.upload_module(&sha256_hex(&wasm), wasm.to_vec(), None, None)
        .await
        .expect("upload fake module");
    (srv, c)
}

fn parse(args: &[&str]) -> ClientCommand {
    let mut v = vec!["ostro"];
    v.extend_from_slice(args);
    Cli::try_parse_from(v)
        .unwrap_or_else(|e| panic!("{args:?}: {e}"))
        .cmd
}

/// Run a verb: Ok(stdout) or Err(error).
async fn exec(c: &Client, args: &[&str]) -> Result<String, CallError> {
    let json = args.contains(&"--json");
    match parse(args) {
        ClientCommand::Schedules { command } => schedules::render(c, json, &command).await,
        ClientCommand::Webhooks { command } => webhooks::render(c, json, &command).await,
        other => panic!("not a Phase 8 verb: {other:?}"),
    }
}

async fn ok(c: &Client, args: &[&str]) -> String {
    let out = exec(c, args)
        .await
        .unwrap_or_else(|e| panic!("{args:?}: {}", e.message()));
    assert!(!out.contains(DUMMY_SECRET), "{args:?} printed the secret");
    out
}

async fn ok_json(c: &Client, args: &[&str]) -> Value {
    let mut v = args.to_vec();
    v.push("--json");
    let out = ok(c, &v).await;
    serde_json::from_str(&out).unwrap_or_else(|e| panic!("{args:?}: not JSON ({e}): {out}"))
}

/// Expect a failure; returns (exit code, message).
async fn err(c: &Client, args: &[&str]) -> (u8, String) {
    match exec(c, args).await {
        Ok(out) => panic!("{args:?} succeeded: {out}"),
        Err(e) => {
            let code = exit_code(Out { json: false }.error(&e));
            (code, e.message())
        }
    }
}

#[tokio::test]
async fn schedule_crud_with_cas() {
    let (_srv, c) = fake().await;

    assert_eq!(ok(&c, &["schedules", "ls"]).await, "no schedules");

    let created = ok(
        &c,
        &[
            "schedules",
            "create",
            "--name",
            "nightly",
            "--workflow",
            "report",
            "--cron",
            "0 0 * * *",
            "--timezone",
            "Europe/Lisbon",
            "--overlap",
            "buffer-one",
            "--catchup",
            "one",
            "--catchup-window",
            "2h",
            "--input-json",
            r#"{"k":1}"#,
        ],
    )
    .await;
    assert!(created.starts_with("schedule nightly created"), "{created}");
    assert!(created.contains("buffer_one"), "{created}");

    // ls: table and JSON
    let table = ok(&c, &["schedules", "ls"]).await;
    let mut lines = table.lines();
    assert!(lines.next().unwrap().starts_with("NAME"), "{table}");
    let row = lines.next().expect("one row");
    for want in ["nightly", "report", "0 0 * * *", "Europe/Lisbon", "false"] {
        assert!(row.contains(want), "{want}: {table}");
    }
    let ls = ok_json(&c, &["schedules", "ls"]).await;
    assert_eq!(ls["schedules"].as_array().unwrap().len(), 1, "{ls}");

    // get
    let got = ok_json(&c, &["schedules", "get", "nightly"]).await;
    assert_eq!(got["catchup_window_ms"], 7_200_000);
    assert_eq!(got["input"], json!({"k": 1}));
    let tag = got["version_tag"].as_u64().expect("version_tag");
    let human = ok(&c, &["schedules", "get", "nightly"]).await;
    assert!(human.contains("version_tag:"), "{human}");

    // update with a stale tag: exit 1, the server's 409 in the message
    let stale = (tag + 7).to_string();
    let (code, msg) = err(
        &c,
        &[
            "schedules",
            "update",
            "nightly",
            "--version-tag",
            &stale,
            "--cron",
            "@hourly",
        ],
    )
    .await;
    assert_ne!(code, 0);
    assert!(msg.contains("409"), "{msg}");
    assert!(msg.contains("version_mismatch"), "{msg}");

    // fresh tag: applied, other fields kept
    let tag_s = tag.to_string();
    let upd = ok_json(
        &c,
        &[
            "schedules",
            "update",
            "nightly",
            "--version-tag",
            &tag_s,
            "--cron",
            "@hourly",
        ],
    )
    .await;
    assert_eq!(upd["cron"], "@hourly");
    assert_eq!(upd["timezone"], "Europe/Lisbon");
    assert_eq!(upd["overlap"], "buffer_one");
    let new_tag = upd["version_tag"].as_u64().unwrap();
    assert!(new_tag > tag);

    // delete with the stale tag fails, with the fresh one succeeds
    let (code, msg) = err(
        &c,
        &["schedules", "delete", "nightly", "--version-tag", &tag_s],
    )
    .await;
    assert_ne!(code, 0);
    assert!(msg.contains("409"), "{msg}");
    let del = ok(
        &c,
        &[
            "schedules",
            "delete",
            "nightly",
            "--version-tag",
            &new_tag.to_string(),
        ],
    )
    .await;
    assert_eq!(del, "schedule nightly deleted");

    // 404 after delete
    let (code, msg) = err(&c, &["schedules", "get", "nightly"]).await;
    assert_eq!(code, 1);
    assert!(msg.contains("404"), "{msg}");

    // 400 validation from the server (bad name)
    let (code, msg) = err(
        &c,
        &[
            "schedules",
            "create",
            "--name",
            "bad name!",
            "--workflow",
            "report",
            "--cron",
            "@daily",
        ],
    )
    .await;
    assert_eq!(code, 1);
    assert!(msg.contains("400"), "{msg}");
}

#[tokio::test]
async fn pause_resume_trigger() {
    let (_srv, c) = fake().await;
    ok(
        &c,
        &[
            "schedules",
            "create",
            "--name",
            "s1",
            "--workflow",
            "report",
            "--cron",
            "@daily",
        ],
    )
    .await;

    let p = ok_json(&c, &["schedules", "pause", "s1"]).await;
    assert_eq!(p["paused"], true);
    assert_eq!(
        ok_json(&c, &["schedules", "get", "s1"]).await["paused"],
        true
    );
    let human = ok(&c, &["schedules", "resume", "s1"]).await;
    assert!(human.starts_with("schedule s1 resumed"), "{human}");
    assert_eq!(
        ok_json(&c, &["schedules", "get", "s1"]).await["paused"],
        false
    );

    let t = ok(&c, &["schedules", "trigger", "s1"]).await;
    let run = t
        .strip_prefix("schedule s1 triggered: run ")
        .unwrap_or_else(|| panic!("{t}"));
    assert!(!run.is_empty() && run != "-", "{t}");

    // --request-id dedupes: the same run twice
    let a = ok_json(&c, &["schedules", "trigger", "s1", "--request-id", "r-1"]).await;
    let b = ok_json(&c, &["schedules", "trigger", "s1", "--request-id", "r-1"]).await;
    assert!(a["run_id"].is_string(), "{a}");
    assert_eq!(a["run_id"], b["run_id"]);

    let (code, msg) = err(&c, &["schedules", "trigger", "nope"]).await;
    assert_eq!(code, 1);
    assert!(msg.contains("404"), "{msg}");
}

#[tokio::test]
async fn webhook_crud_never_prints_secrets() {
    let (_srv, c) = fake().await;
    assert_eq!(ok(&c, &["webhooks", "ls"]).await, "no webhooks");

    let created = ok(
        &c,
        &[
            "webhooks",
            "create",
            "--name",
            "stripe-in",
            "--action",
            "start",
            "--workflow",
            "report",
            "--verifier",
            "stripe",
            "--secret-ref",
            "stripe-key",
            "--tolerance",
            "5m",
            "--idempotency",
            "json:/id",
        ],
    )
    .await;
    assert!(created.contains("POST /hooks/stripe-in"), "{created}");
    assert!(created.contains("stripe-key"), "{created}");

    ok(
        &c,
        &[
            "webhooks",
            "create",
            "--name",
            "gh",
            "--action",
            "signal",
            "--workflow",
            "report",
            "--run-id-pointer",
            "/run",
            "--signal",
            "push",
            "--verifier",
            "github",
            "--secret-ref",
            "gh-key",
        ],
    )
    .await;

    let table = ok(&c, &["webhooks", "ls"]).await;
    assert_eq!(table.lines().count(), 3, "{table}");
    assert!(table.contains("SECRET_REF"), "{table}");
    assert!(table.contains("report signal push"), "{table}");

    let w = ok_json(&c, &["webhooks", "get", "stripe-in"]).await;
    assert_eq!(w["verifier"]["secret_ref"], "stripe-key");
    assert_eq!(w["verifier"]["tolerance_s"], 300);
    assert_eq!(w["idempotency"], "json:/id");
    let tag = w["version_tag"].as_u64().unwrap();

    // stale update -> 409; fresh update keeps the action and changes the secret ref
    let (code, msg) = err(
        &c,
        &[
            "webhooks",
            "update",
            "stripe-in",
            "--version-tag",
            "99",
            "--secret-ref",
            "k2",
        ],
    )
    .await;
    assert_ne!(code, 0);
    assert!(msg.contains("409"), "{msg}");
    let u = ok_json(
        &c,
        &[
            "webhooks",
            "update",
            "stripe-in",
            "--version-tag",
            &tag.to_string(),
            "--secret-ref",
            "k2",
        ],
    )
    .await;
    assert_eq!(u["verifier"]["secret_ref"], "k2");
    assert_eq!(u["action"]["workflow"], "report");

    let d = ok(&c, &["webhooks", "delete", "gh"]).await;
    assert_eq!(d, "webhook gh deleted");
    let (code, msg) = err(&c, &["webhooks", "get", "gh"]).await;
    assert_eq!(code, 1);
    assert!(msg.contains("404"), "{msg}");

    // A missing --secret-ref is a local usage error (exit 2), nothing sent.
    let (code, msg) = err(
        &c,
        &[
            "webhooks",
            "create",
            "--name",
            "x",
            "--action",
            "start",
            "--workflow",
            "report",
            "--verifier",
            "slack",
        ],
    )
    .await;
    assert_eq!(code, 2);
    assert!(msg.contains("--secret-ref"), "{msg}");

    // There is no flag that takes secret material.
    let e = Cli::try_parse_from([
        "ostro",
        "webhooks",
        "create",
        "--name",
        "x",
        "--action",
        "start",
        "--workflow",
        "w",
        "--verifier",
        "stripe",
        "--secret",
        DUMMY_SECRET,
    ])
    .unwrap_err();
    assert_eq!(e.exit_code(), 2);
}

/// A misbehaving server that returns secret material in a webhook body: the CLI
/// strips it from both human and JSON output.
#[tokio::test]
async fn secret_fields_from_a_server_are_scrubbed() {
    let hook = json!({"name": "h", "action": {"kind": "start", "workflow": "w", "input": "body"},
        "verifier": {"kind": "stripe", "secret_ref": "stripe-key", "secret": DUMMY_SECRET},
        "signing_secret": DUMMY_SECRET, "version_tag": 1});
    let list = json!({"webhooks": [hook.clone()]});
    let app = axum::Router::new()
        .route(
            "/v1/webhooks",
            axum::routing::get(move || {
                let l = list.clone();
                async move { axum::Json(l) }
            }),
        )
        .route(
            "/v1/webhooks/{name}",
            axum::routing::get(move || {
                let h = hook.clone();
                async move { axum::Json(h) }
            }),
        );
    let srv = serve(app).await;
    let c = Client::new(&srv.url, Duration::from_secs(10)).unwrap();
    for args in [
        &["webhooks", "ls"][..],
        &["webhooks", "ls", "--json"][..],
        &["webhooks", "get", "h"][..],
        &["webhooks", "get", "h", "--json"][..],
    ] {
        let out = ok(&c, args).await;
        assert!(out.contains("stripe-key"), "{args:?}: {out}");
    }
}

/// 503 from a controller stub and 404 from a disabled API both read "not implemented
/// on this server" (exit 1).
#[tokio::test]
async fn unimplemented_server_is_reported_clearly() {
    async fn stub() -> Result<(), ApiError> {
        Err(ApiError::unavailable("not yet implemented (08-14)"))
    }
    let app = axum::Router::new()
        .route("/v1/schedules", axum::routing::get(stub).post(stub))
        .route("/v1/webhooks", axum::routing::get(stub));
    let srv = serve(app).await;
    let c = Client::new(&srv.url, Duration::from_secs(10)).unwrap();
    for args in [
        &["schedules", "ls"][..],
        &[
            "schedules",
            "create",
            "--name",
            "a",
            "--workflow",
            "w",
            "--cron",
            "@daily",
        ][..],
        &["webhooks", "ls"][..],
    ] {
        let (code, msg) = err(&c, args).await;
        assert_eq!(code, 1, "{args:?}");
        assert!(
            msg.contains("not implemented on this server"),
            "{args:?}: {msg}"
        );
        assert!(msg.contains("503"), "{args:?}: {msg}");
    }

    // Phase 8 slots absent (an older or reduced server): 404 "not enabled".
    let cp: Arc<dyn ControlPlane> = Arc::new(FakeControlPlane::new());
    let srv = serve(ostro_api::router_with(
        cp,
        ApiConfig::default(),
        ApiExtensions::default(),
    ))
    .await;
    let c = Client::new(&srv.url, Duration::from_secs(10)).unwrap();
    for args in [&["schedules", "ls"][..], &["webhooks", "get", "x"][..]] {
        let (code, msg) = err(&c, args).await;
        assert_eq!(code, 1, "{args:?}");
        assert!(
            msg.contains("not implemented on this server"),
            "{args:?}: {msg}"
        );
    }
}

#[test]
fn outcome_mapping_is_shared() {
    assert_eq!(exit_code(Outcome::Error), 1);
    assert_eq!(exit_code(Outcome::Usage), 2);
}
