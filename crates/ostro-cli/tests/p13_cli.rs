//! 13-16: `ostro cluster …`, `ostro migrate-runs`, `ostro modules {rm, gc}` against an
//! in-process `ostro_api::router_with(FakeControlPlane, FakeCluster + FakeModuleLifecycle)`
//! (13-03 fakes) on 127.0.0.1:0.
//!
//! The verbs are parsed with clap exactly as the `ostro` binary embeds them and run through
//! their `render` functions; each test records a transcript (`$ ostro <args>`, the stdout
//! text or the error line, the exit code from the shared `Out::error` mapping) and compares
//! it with an insta snapshot. Every verb has at least one error path (409 / 412 / 503 / 404 /
//! usage).

use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use ostro_api::fake::{FakeCluster, FakeControlPlane, FakeMigrateOutcome, FakeModuleLifecycle};
use ostro_api::{ApiConfig, ApiError, ApiExtensions, ControlPlane, ErrorCode};
use ostro_cli::ClientCommand;
use ostro_cli::client::{CallError, Client};
use ostro_cli::commands::{cluster, migrate_runs, modules_lc};
use ostro_cli::output::{Out, Outcome, exit_code};
use serde_json::json;

#[derive(Debug, Parser)]
#[command(name = "ostro")]
struct Cli {
    #[command(subcommand)]
    cmd: ClientCommand,
}

struct Srv {
    url: String,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Srv {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct Fx {
    srv: Srv,
    c: Client,
    cluster: Arc<FakeCluster>,
    modules: Arc<FakeModuleLifecycle>,
}

const SHA_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const SHA_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

async fn fx() -> Fx {
    let cp: Arc<dyn ControlPlane> = Arc::new(FakeControlPlane::new());
    let cluster = FakeCluster::new();
    let modules = FakeModuleLifecycle::new();
    let mut ext = ApiExtensions::default();
    cluster.install(&mut ext);
    modules.install(&mut ext);
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
        srv: Srv { url, task },
        c,
        cluster,
        modules,
    }
}

impl Fx {
    /// Run one verb; the transcript entry. The server URL is redacted (random port).
    async fn sh(&self, args: &[&str]) -> String {
        let (out, code) = self.exec(args).await;
        let out = out.replace(&self.srv.url, "http://SERVER");
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
        let r: Result<(String, Outcome), CallError> = match &cmd {
            ClientCommand::Cluster { command } => cluster::render(&self.c, json, command)
                .await
                .map(|s| (s, Outcome::Ok)),
            ClientCommand::MigrateRuns(a) => migrate_runs::render(&self.c, json, a).await,
            ClientCommand::Modules { command } => modules_lc::render(&self.c, json, command)
                .await
                .map(|s| (s, Outcome::Ok)),
            other => panic!("not a Phase 13 verb: {other:?}"),
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

    /// Init + one joined node (node 2) through a node token.
    async fn two_nodes(&self) {
        self.c
            .cluster_init(&json!({ "advertise": "10.0.0.1:7701" }))
            .await
            .expect("init");
        let t = self
            .c
            .create_node_token(&json!({}))
            .await
            .expect("token")
            .body;
        self.c
            .cluster_join(&json!({
                "token": t["token"],
                "csr_pem": "csr",
                "advertise": { "api": "10.0.0.2:7700", "cluster": "10.0.0.2:7701" },
                "node_version": {
                    "ostro_version": "0.1.0", "level_min": 1, "level_max": 2,
                    "wasmtime": "50.0.2", "event_codec_ver": 1, "wit_lines": ["0.1"]
                },
            }))
            .await
            .expect("join");
        self.cluster.promote(2);
    }
}

fn feature_not_enabled() -> ApiError {
    ApiError::conflict("AlterReplicas needs feature level 2; finalized level is 1").with_details(
        json!({
            "reason": "feature_not_enabled", "feature": "AlterReplicas", "needed": 2, "finalized": 1
        }),
    )
}

fn not_leader() -> ApiError {
    ApiError::new(ErrorCode::Unavailable, "not the meta leader; retry")
        .with_details(json!({ "reason": "not_leader", "leader": 2 }))
}

#[tokio::test]
async fn cluster_init() {
    let f = fx().await;
    let t = f
        .transcript(&[
            &["cluster", "init", "--advertise-api", "10.0.0.1:7700"],
            &["cluster", "init", "--rf", "3"],
            &[
                "cluster",
                "init",
                "--advertise-cluster",
                "10.0.0.1:7701",
                "--rf",
                "5",
            ],
            &["cluster", "init", "--json"],
        ])
        .await;
    insta::assert_snapshot!("cluster_init", t);
    let f = fx().await;
    f.cluster.fail(
        "init",
        Some(
            ApiError::conflict("`ostro cluster init` needs the server profile")
                .with_details(json!({ "reason": "not_supported_in_profile" })),
        ),
    );
    let (out, code) = f.exec(&["cluster", "init"]).await;
    assert_eq!(code, 1, "{out}");
    assert!(out.contains("not_supported_in_profile"), "{out}");
}

#[tokio::test]
async fn cluster_status() {
    let f = fx().await;
    let before = f.transcript(&[&["cluster", "status"]]).await;
    f.two_nodes().await;
    let mut t = before;
    t.push_str(
        &f.transcript(&[&["cluster", "status"], &["cluster", "status", "--json"]])
            .await,
    );
    f.cluster.fail("status", Some(not_leader()));
    t.push_str(&f.transcript(&[&["cluster", "status"]]).await);
    insta::assert_snapshot!("cluster_status", t);
}

#[tokio::test]
async fn cluster_token() {
    let f = fx().await;
    let t = f
        .transcript(&[
            &["cluster", "token", "create", "--node"],
            &["cluster", "init"],
            &["cluster", "token", "create"],
            &["cluster", "token", "create", "--node", "--ttl", "1h"],
            &["cluster", "token", "create", "--node", "--json"],
        ])
        .await;
    insta::assert_snapshot!("cluster_token", t);
}

#[tokio::test]
async fn cluster_remove_and_drain() {
    let f = fx().await;
    f.two_nodes().await;
    let t = f
        .transcript(&[
            &["cluster", "drain", "9"],
            &["cluster", "drain", "2"],
            &["cluster", "remove", "2", "--force"],
            &["cluster", "remove", "2", "--json"],
            &["cluster", "remove", "1"],
        ])
        .await;
    insta::assert_snapshot!("cluster_remove_drain", t);
}

#[tokio::test]
async fn cluster_reassign_and_rebalance() {
    let f = fx().await;
    f.two_nodes().await;
    let mut t = f
        .transcript(&[
            &["cluster", "reassign", "--partition", "2", "--to", "1,2"],
            &["cluster", "reassign", "--partition", "2", "--to", "2,1"],
            &["cluster", "reassign", "--partition", "2", "--cancel"],
            &["cluster", "reassign", "--partition", "2", "--cancel"],
            &["cluster", "reassign", "--partition", "2"],
            &["cluster", "rebalance"],
        ])
        .await;
    f.cluster.fail("reassign", Some(feature_not_enabled()));
    f.cluster.fail("rebalance", Some(feature_not_enabled()));
    t.push_str(
        &f.transcript(&[
            &["cluster", "reassign", "--partition", "1", "--to", "1,2"],
            &["cluster", "rebalance", "--json"],
        ])
        .await,
    );
    insta::assert_snapshot!("cluster_reassign_rebalance", t);
}

#[tokio::test]
async fn cluster_upgrade() {
    let f = fx().await;
    f.two_nodes().await;
    let mut t = f
        .transcript(&[
            &["cluster", "upgrade", "status"],
            &["cluster", "upgrade", "hold"],
            &["cluster", "upgrade", "finalize"],
            &["cluster", "upgrade", "release"],
            &["cluster", "upgrade", "finalize"],
            &["cluster", "upgrade", "status", "--json"],
        ])
        .await;
    f.cluster.fail("upgrade_finalize", Some(not_leader()));
    t.push_str(&f.transcript(&[&["cluster", "upgrade", "finalize"]]).await);
    insta::assert_snapshot!("cluster_upgrade", t);
}

#[tokio::test]
async fn cluster_join_is_the_binarys() {
    let f = fx().await;
    let (out, code) = f
        .exec(&[
            "cluster",
            "join",
            "https://seed:7700",
            "--token",
            "njt_x",
            "--ca-hash",
            "sha256:ab",
        ])
        .await;
    assert_eq!(code, 2, "{out}");
}

#[tokio::test]
async fn migrate_runs() {
    let f = fx().await;
    f.modules.add_run("r1", "order", FakeMigrateOutcome::Pass);
    f.modules.add_run("r2", "order", FakeMigrateOutcome::Busy);
    f.modules
        .add_run("r3", "order", FakeMigrateOutcome::SeqMismatch);
    f.modules.add_run("r4", "other", FakeMigrateOutcome::Pass);
    let mut t = f
        .transcript(&[
            &[
                "migrate-runs",
                "--workflow",
                "order",
                "--from",
                "1",
                "--to",
                "2",
                "--dry-run",
                "--filter",
                "tag=eu",
                "--filter",
                "vip",
            ],
            &[
                "migrate-runs",
                "--workflow",
                "order",
                "--from",
                SHA_A,
                "--to",
                "2",
            ],
            &[
                "migrate-runs",
                "--workflow",
                "order",
                "--from",
                "1",
                "--to",
                "2",
                "--filter",
                "status=running",
            ],
        ])
        .await;
    let m = f.modules.migrations();
    assert_eq!(m[0].filter.as_deref(), Some("eu,vip"));
    assert!(m[0].dry_run);
    assert_eq!(
        m[1].from,
        ostro_api::phase13::VersionOrSha::Sha(SHA_A.to_owned())
    );
    f.modules.add_run(
        "r0",
        "order",
        FakeMigrateOutcome::Fail {
            seq: 7,
            detail: "step `charge` diverged".into(),
        },
    );
    t.push_str(
        &f.transcript(&[
            &[
                "migrate-runs",
                "--workflow",
                "order",
                "--from",
                "1",
                "--to",
                "2",
                "--limit",
                "2",
            ],
            &[
                "migrate-runs",
                "--workflow",
                "order",
                "--from",
                "1",
                "--to",
                "2",
                "--limit",
                "1",
                "--json",
            ],
        ])
        .await,
    );
    f.modules.fail(
        "migrate_runs",
        Some(
            ApiError::conflict("workflow order v2 is retired")
                .with_details(json!({ "reason": "module_retired" })),
        ),
    );
    t.push_str(
        &f.transcript(&[&[
            "migrate-runs",
            "--workflow",
            "order",
            "--from",
            "1",
            "--to",
            "2",
        ]])
        .await,
    );
    insta::assert_snapshot!("migrate_runs", t);
}

#[tokio::test]
async fn modules_rm() {
    let f = fx().await;
    f.modules
        .add_module(SHA_A, &[("order", 1), ("order", 2)], false);
    f.modules.add_module(SHA_B, &[("order", 3)], true);
    let t = f
        .transcript(&[
            &["modules", "rm", SHA_A],
            &["modules", "rm", SHA_A],
            &["modules", "rm", SHA_B],
            &["modules", "rm", "cccc"],
            &["modules", "rm", SHA_A, "--json"],
        ])
        .await;
    insta::assert_snapshot!("modules_rm", t);
}

#[tokio::test]
async fn modules_gc() {
    let f = fx().await;
    f.modules.add_module(SHA_A, &[("order", 1)], false);
    f.modules.add_module(SHA_B, &[("order", 2)], false);
    let mut t = f.transcript(&[&["modules", "gc"]]).await;
    f.c.retire_module(SHA_A).await.expect("retire a");
    f.c.retire_module(SHA_B).await.expect("retire b");
    f.modules.set_referenced(SHA_B, true);
    t.push_str(
        &f.transcript(&[&["modules", "gc"], &["modules", "gc", "--json"]])
            .await,
    );
    f.modules.fail("run_module_gc", Some(feature_not_enabled()));
    t.push_str(&f.transcript(&[&["modules", "gc"]]).await);
    insta::assert_snapshot!("modules_gc", t);
}
