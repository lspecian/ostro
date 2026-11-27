//! 14-18: `ostro audit ls` against an in-process `ostro_api::router_with` over the 14-02
//! `FakeAudit` on 127.0.0.1:0, and the `ostro audit verify` argument shape.
//!
//! Each test records a transcript (`$ ostro <args>`, the output or the error line, the exit
//! code from the shared `Out::error` mapping) and compares it with an insta snapshot: the
//! human table, the JSON page, filters, paging with `--cursor`, a 503 `audit_unavailable`
//! and usage errors. The offline chain verification itself (`ostro audit verify
//! --data-dir`, run by the `ostro` binary through `ostro_server::audit::verify::offline`) is
//! exercised on a good and a tampered journal in `ostro-server/tests/audit_log.rs`; this
//! crate has no ostro-server dependency (no Cargo edits in Phase 14 plans).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use ostro_api::fake::{FakeAudit, FakeControlPlane};
use ostro_api::phase14::AuditRecordView;
use ostro_api::{ApiConfig, ApiError, ApiExtensions, ControlPlane};
use ostro_cli::ClientCommand;
use ostro_cli::client::{CallError, Client};
use ostro_cli::commands::audit::{self, AuditCmd};
use ostro_cli::output::{Out, Outcome, exit_code};
use serde_json::json;

#[derive(Debug, Parser)]
#[command(name = "ostro")]
struct Cli {
    #[command(subcommand)]
    cmd: ClientCommand,
}

struct Fx {
    url: String,
    task: tokio::task::JoinHandle<()>,
    c: Client,
    audit: Arc<FakeAudit>,
}

impl Drop for Fx {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn record(
    ts_ms: u64,
    node: u64,
    action: &str,
    actor: &str,
    tenant: Option<&str>,
    target: &str,
    outcome: &str,
) -> AuditRecordView {
    AuditRecordView {
        seq: 0,
        ts_ms,
        node,
        actor: actor.into(),
        actor_kind: if actor.starts_with("otk_") {
            "token".into()
        } else {
            "system".into()
        },
        tenant: tenant.map(str::to_owned),
        action: action.into(),
        target: target.into(),
        outcome: outcome.into(),
        request_id: None,
        details: BTreeMap::new(),
        hash: "ab".repeat(32),
    }
}

async fn fx() -> Fx {
    let cp: Arc<dyn ControlPlane> = Arc::new(FakeControlPlane::new());
    let audit = FakeAudit::new();
    let mut ext = ApiExtensions::default();
    audit.install(&mut ext);
    // 2026-11-27T10:00:00Z and on.
    let t0 = 1_795_773_600_000;
    audit.push(record(
        t0,
        1,
        "module.upload",
        "otk_admin",
        None,
        &"cd".repeat(32),
        "ok",
    ));
    audit.push(record(
        t0 + 1_000,
        2,
        "run.cancel",
        "otk_ops",
        Some("acme"),
        "r-1",
        "ok",
    ));
    audit.push(record(
        t0 + 2_000,
        1,
        "run.cancel",
        "otk_ops",
        Some("acme"),
        "r-2",
        "failed:not_found",
    ));
    let mut coalesced = record(
        t0 + 3_000,
        3,
        "auth.failure",
        "malformed",
        None,
        "",
        "denied:malformed",
    );
    coalesced.actor_kind = "anonymous".into();
    coalesced.details.insert("count".into(), "7".into());
    audit.push(coalesced);
    audit.push(record(
        t0 + 4_000,
        2,
        "secret.set",
        "otk_admin",
        Some("beta"),
        "db",
        "ok",
    ));
    let app = ostro_api::router_with(cp, ApiConfig::default(), ext);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    let url = format!("http://{addr}");
    let c = Client::new(&url, Duration::from_secs(10)).expect("client");
    Fx {
        url,
        task,
        c,
        audit,
    }
}

impl Fx {
    async fn sh(&self, args: &[&str]) -> String {
        let (out, code) = self.exec(args).await;
        let out = out.replace(&self.url, "http://SERVER");
        format!("$ ostro {}\n{out}\n[exit {code}]\n", args.join(" "))
    }

    async fn exec(&self, args: &[&str]) -> (String, u8) {
        let json = args.contains(&"--json");
        let mut v = vec!["ostro"];
        v.extend_from_slice(args);
        let cmd = match Cli::try_parse_from(v) {
            Ok(c) => c.cmd,
            Err(e) => return (format!("usage: {}", e.kind()), 2),
        };
        let r: Result<String, CallError> = match &cmd {
            ClientCommand::Audit(c) => audit::render(&self.c, json, c).await,
            other => panic!("not an audit verb: {other:?}"),
        };
        match r {
            Ok(s) => (s, exit_code(Outcome::Ok)),
            Err(e) => {
                let code = exit_code(Out { json: false }.error(&e));
                (format!("error: {}", e.message()), code)
            }
        }
    }

    async fn transcript(&self, cmds: &[&[&str]]) -> String {
        let mut t = String::new();
        for a in cmds {
            t.push_str(&self.sh(a).await);
        }
        t
    }
}

#[tokio::test]
async fn audit_ls_human() {
    let f = fx().await;
    let t = f
        .transcript(&[
            &["audit", "ls"],
            &["audit", "ls", "--action", "run.cancel"],
            &["audit", "ls", "--tenant", "acme", "--actor", "otk_ops"],
            &[
                "audit",
                "ls",
                "--action",
                "secret.*",
                "--since",
                "1795773604000",
            ],
            &["audit", "ls", "--action", "token.create"],
        ])
        .await;
    insta::assert_snapshot!("audit_ls_human", t);
}

#[tokio::test]
async fn audit_ls_json_and_paging() {
    let f = fx().await;
    let t = f
        .transcript(&[
            &["audit", "ls", "--limit", "2", "--json"],
            &["audit", "ls", "--limit", "2"],
            &["audit", "ls", "--limit", "2", "--cursor", "2"],
            &["audit", "ls", "--limit", "2", "--cursor", "4"],
        ])
        .await;
    insta::assert_snapshot!("audit_ls_json_and_paging", t);
}

#[tokio::test]
async fn audit_ls_errors_and_verify_shape() {
    let f = fx().await;
    f.audit.fail(
        "query",
        Some(
            ApiError::unavailable("audit journal unavailable: writer stalled")
                .with_details(json!({ "reason": "audit_unavailable", "retry_after_ms": 1000 })),
        ),
    );
    let t = f
        .transcript(&[
            &["audit", "ls"],
            &["audit", "ls", "--limit", "many"],
            &["audit", "verify"],
            &["audit", "verify", "--data-dir", "/var/lib/ostro"],
        ])
        .await;
    insta::assert_snapshot!("audit_ls_errors_and_verify_shape", t);
    // `verify` carries its data dir to the binary's offline verifier and needs no server.
    let cli = Cli::try_parse_from(["ostro", "audit", "verify", "--data-dir", "/d"]).expect("parse");
    match cli.cmd {
        ClientCommand::Audit(c @ AuditCmd::Verify(_)) => {
            assert!(c.conn().is_none());
            let AuditCmd::Verify(a) = c else {
                unreachable!()
            };
            assert_eq!(a.data_dir, std::path::PathBuf::from("/d"));
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn relative_since_is_sent_as_unix_ms() {
    let cli = Cli::try_parse_from(["ostro", "audit", "ls", "--since", "1h", "--until", "5m"])
        .expect("parse");
    let ClientCommand::Audit(AuditCmd::Ls(a)) = cli.cmd else {
        panic!("ls")
    };
    let f = a.filter(10_000_000);
    assert_eq!(f.since.as_deref(), Some("6400000"));
    assert_eq!(f.until.as_deref(), Some("9700000"));
}
