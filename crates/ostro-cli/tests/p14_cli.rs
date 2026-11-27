//! 14-17: `ostro tenant …`, `ostro auth token …` and `ostro auth whoami` against an
//! in-process `ostro_api::router_with(FakeControlPlane, FakeTenants + FakeTokens)` (14-02
//! fakes) on 127.0.0.1:0 (Phase 11/13 pattern).
//!
//! The verbs are parsed with clap exactly as the `ostro` binary embeds them and run through
//! their `render` functions; each test records a transcript (`$ ostro <args>`, the stdout text
//! or the error line, the exit code from the shared `Out::error` mapping) and compares it with
//! an insta snapshot, human and `--json`.

use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use ostro_api::fake::{FakeControlPlane, FakeP14};
use ostro_api::{ApiConfig, ApiExtensions, ControlPlane};
use ostro_cli::ClientCommand;
use ostro_cli::client::{CallError, Client};
use ostro_cli::commands::{auth, tenant};
use ostro_cli::output::{Out, Outcome, exit_code};

#[derive(Debug, Parser)]
#[command(name = "ostro")]
struct Cli {
    #[command(subcommand)]
    cmd: ClientCommand,
}

struct Fx {
    url: String,
    c: Client,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Fx {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn fx() -> Fx {
    let cp: Arc<dyn ControlPlane> = Arc::new(FakeControlPlane::new());
    let mut ext = ApiExtensions::default();
    FakeP14::new().install(&mut ext);
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
    Fx { url, c, task }
}

impl Fx {
    /// One verb as a transcript entry (stdout, `[stderr]` lines, exit code).
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
            Err(e) => {
                let first = e.to_string().lines().next().unwrap_or_default().to_owned();
                return (
                    format!("usage: {first}"),
                    u8::try_from(e.exit_code()).unwrap_or(2),
                );
            }
        };
        let r: Result<(String, Outcome), CallError> = match &cmd {
            ClientCommand::Tenant(c) => tenant::render(&self.c, json, c)
                .await
                .map(|s| (s, Outcome::Ok)),
            ClientCommand::Auth(c) => auth::render(&self.c, json, c).await.map(|(s, bearer)| {
                let s = if bearer {
                    format!("{s}\n[stderr] {}", auth::STORE_HINT)
                } else {
                    s
                };
                (s, Outcome::Ok)
            }),
            other => panic!("not a 14-17 verb: {other:?}"),
        };
        match r {
            Ok((s, o)) => (s, exit_code(o)),
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
async fn tenant_verbs_human() {
    let f = fx().await;
    let t = f
        .transcript(&[
            &[
                "tenant",
                "create",
                "acme",
                "--weight",
                "2",
                "--max-concurrency",
                "8",
                "--memory",
                "512MiB",
                "--event-rate",
                "100/1s+10",
            ],
            &["tenant", "create", "beta"],
            &["tenant", "create", "acme"],
            &["tenant", "ls"],
            &["tenant", "get", "acme"],
            &[
                "tenant",
                "set",
                "acme",
                "--fuel-per-s",
                "1000",
                "--clear",
                "max-concurrency",
            ],
            &["tenant", "set", "acme"],
            &["tenant", "rm", "acme"],
            &["tenant", "suspend", "acme"],
            &["tenant", "suspend", "acme"],
            &["tenant", "resume", "beta"],
            &["tenant", "rm", "acme"],
            &["tenant", "get", "acme"],
        ])
        .await;
    insta::assert_snapshot!("tenant_human", t);
}

#[tokio::test]
async fn tenant_verbs_json() {
    let f = fx().await;
    let t = f
        .transcript(&[
            &["tenant", "create", "acme", "--fuel-burst", "500", "--json"],
            &["tenant", "ls", "--json"],
            &["tenant", "get", "acme", "--json"],
            &["tenant", "set", "acme", "--weight", "3", "--json"],
            &["tenant", "suspend", "acme", "--json"],
            &["tenant", "rm", "acme", "--json"],
            &["tenant", "get", "nope", "--json"],
        ])
        .await;
    insta::assert_snapshot!("tenant_json", t);
}

#[tokio::test]
async fn tenant_flag_errors_are_usage_errors() {
    let f = fx().await;
    let t = f
        .transcript(&[
            &["tenant", "create", "_"],
            &["tenant", "create", "Acme"],
            &["tenant", "create", "acme", "--memory", "12XB"],
            &["tenant", "create", "acme", "--memory", "0"],
            &["tenant", "create", "acme", "--max-concurrency", "0"],
            &["tenant", "create", "acme", "--event-rate", "100"],
            &["tenant", "create", "acme", "--event-rate", "10/1d"],
            &["tenant", "set", "acme", "--clear", "colour"],
            &[
                "tenant", "set", "acme", "--weight", "2", "--clear", "weight",
            ],
        ])
        .await;
    for entry in t.split("$ ostro ").filter(|e| !e.is_empty()) {
        assert!(entry.contains("[exit 2]"), "{entry}");
    }
    assert!(t.contains("default tenant"), "{t}");
    assert!(t.contains("invalid size unit"), "{t}");
    insta::assert_snapshot!("tenant_usage", t);
}

#[tokio::test]
async fn token_verbs_human_and_json() {
    let f = fx().await;
    let t = f
        .transcript(&[
            &[
                "auth",
                "token",
                "create",
                "--name",
                "ci",
                "--scope",
                "deploy",
                "--scope",
                "read",
                "--tenant",
                "acme",
                "--ttl",
                "720h",
                "--description",
                "CI deployer",
            ],
            &[
                "auth",
                "token",
                "create",
                "--name",
                "root",
                "--scope",
                "admin",
                "--system",
                "--no-expiry",
                "--json",
            ],
            &["auth", "token", "ls"],
            &["auth", "token", "ls", "--tenant", "acme", "--json"],
            &["auth", "token", "rotate", "otk_aaaaaaaaaaaaaaab"],
            &[
                "auth",
                "token",
                "rotate",
                "otk_aaaaaaaaaaaaaaac",
                "--keep-old",
                "--json",
            ],
            &["auth", "token", "revoke", "otk_aaaaaaaaaaaaaaab"],
            &["auth", "token", "revoke", "otk_aaaaaaaaaaaaaaab", "--json"],
            &["auth", "token", "revoke", "otk_nope"],
            &["auth", "whoami"],
            &["auth", "whoami", "--json"],
        ])
        .await;
    insta::assert_snapshot!("token_verbs", t);

    // The bearer appears only in the create / rotate outputs.
    for entry in t.split("$ ostro ").filter(|e| !e.is_empty()) {
        let cmd = entry.lines().next().unwrap_or_default();
        let has_bearer = entry.contains("ostro_otk_");
        let mints = cmd.starts_with("auth token create") || cmd.starts_with("auth token rotate");
        assert_eq!(has_bearer, mints, "{entry}");
        assert_eq!(entry.contains(auth::STORE_HINT), mints, "{entry}");
    }
}

#[tokio::test]
async fn token_flag_errors_are_usage_errors() {
    let f = fx().await;
    let t = f
        .transcript(&[
            &["auth", "token", "create", "--scope", "read"],
            &["auth", "token", "create", "--name", "x"],
            &["auth", "token", "create", "--name", "x", "--scope", "root"],
            &[
                "auth", "token", "create", "--name", "x", "--scope", "read", "--tenant", "acme",
                "--system",
            ],
            &[
                "auth",
                "token",
                "create",
                "--name",
                "x",
                "--scope",
                "read",
                "--ttl",
                "1h",
                "--no-expiry",
            ],
            &[
                "auth", "token", "create", "--name", "x", "--scope", "read", "--ttl", "7d",
            ],
            &["auth", "token", "revoke"],
        ])
        .await;
    for entry in t.split("$ ostro ").filter(|e| !e.is_empty()) {
        assert!(entry.contains("[exit 2]"), "{entry}");
    }
    insta::assert_snapshot!("token_usage", t);
}
