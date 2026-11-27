//! 14-19: `ostro security …` and `ostro deploy --signature` against an in-process
//! `ostro_api::router_with(FakeControlPlane, FakeP14)` (14-02 fakes) on 127.0.0.1:0, as
//! `p14_cli.rs` does for 14-17.
//!
//! Security verbs run through `security::render` and are compared as insta transcripts
//! (`$ ostro <args>`, stdout or the error line, the exit code). `deploy --signature` is run
//! through `deploy::deploy`; the fake control plane records the decoded
//! `Ostro-Module-Signature` headers of the last upload. Oversized / too many signature files
//! are refused locally, before any request.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use clap::Parser;
use ostro_api::fake::{FakeControlPlane, FakeP14, fake_module, sha256_hex};
use ostro_api::{ApiConfig, ApiExtensions, ControlPlane};
use ostro_cli::ClientCommand;
use ostro_cli::client::{CallError, Client};
use ostro_cli::commands::{deploy, security};
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
    cp: Arc<FakeControlPlane>,
    dir: tempfile::TempDir,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Fx {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn fx() -> Fx {
    let cp = Arc::new(FakeControlPlane::new());
    let mut ext = ApiExtensions::default();
    FakeP14::new().install(&mut ext);
    let app = ostro_api::router_with(
        Arc::clone(&cp) as Arc<dyn ControlPlane>,
        ApiConfig::default(),
        ext,
    );
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
        c,
        cp,
        dir: tempfile::tempdir().expect("tempdir"),
        task,
    }
}

/// A fixed minisign public key line (key id bytes 1..=8, key 0x09 x 32).
fn minisign_key() -> String {
    let mut raw = b"Ed".to_vec();
    raw.extend_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]);
    raw.extend_from_slice(&[9; 32]);
    B64.encode(raw)
}

const COSIGN_PEM: &str = "-----BEGIN PUBLIC KEY-----\nMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEfakefakefakefakefakefakefake\n-----END PUBLIC KEY-----\n";

impl Fx {
    fn file(&self, name: &str, body: &[u8]) -> String {
        let p = self.dir.path().join(name);
        std::fs::write(&p, body).expect("write");
        p.display().to_string()
    }

    fn rel(&self, s: &str) -> String {
        s.replace(&self.url, "http://SERVER")
            .replace(&self.dir.path().display().to_string(), "TMP")
    }

    async fn sh(&self, args: &[&str]) -> String {
        let (out, code) = self.exec(args).await;
        format!(
            "$ ostro {}\n{}\n[exit {code}]\n",
            self.rel(&args.join(" ")),
            self.rel(&out)
        )
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
            ClientCommand::Security(c) => security::render(&self.c, json, c)
                .await
                .map(|s| (s, Outcome::Ok)),
            ClientCommand::Deploy(a) => deploy::deploy(&self.c, Out { json }, a)
                .await
                .map(|o| ("(deploy output on stdout)".to_owned(), o)),
            other => panic!("not a 14-19 verb: {other:?}"),
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
async fn security_verbs_human() {
    let f = fx().await;
    let key = minisign_key();
    let pub_file = f.file(
        "m.pub",
        format!("untrusted comment: minisign public key 0807060504030201\n{key}\n").as_bytes(),
    );
    let pem = f.file("cosign.pub", COSIGN_PEM.as_bytes());
    let t = f
        .transcript(&[
            &["security", "policy", "get"],
            &[
                "security",
                "trust",
                "add",
                "--minisign",
                &key,
                "--comment",
                "ci",
            ],
            &["security", "trust", "add", "--minisign", &pub_file],
            &[
                "security", "trust", "add", "--cosign", &pem, "--tenant", "acme",
            ],
            &["security", "policy", "get", "--tenant", "other"],
            &["security", "trust", "rm", "0807060504030201"],
            &["security", "trust", "rm", "0807060504030201"],
            // The fake replaces the whole policy on PUT (the server keeps the keys when
            // `trusted_keys` is empty, see p14/security_api.rs).
            &["security", "policy", "set", "--require-signed", "true"],
            &["security", "policy", "get"],
            &[
                "security",
                "policy",
                "set",
                "--require-signed",
                "true",
                "--tenant",
                "acme",
            ],
            &["security", "trust", "add", "--minisign", "not-a-key"],
        ])
        .await;
    insta::assert_snapshot!("security_human", t);
}

#[tokio::test]
async fn security_verbs_json() {
    let f = fx().await;
    let key = minisign_key();
    let t = f
        .transcript(&[
            &["security", "trust", "add", "--minisign", &key, "--json"],
            &[
                "security",
                "policy",
                "set",
                "--require-signed",
                "false",
                "--json",
            ],
            &["security", "policy", "get", "--json"],
        ])
        .await;
    insta::assert_snapshot!("security_json", t);
}

#[tokio::test]
async fn security_usage() {
    let f = fx().await;
    let t = f
        .transcript(&[
            &["security", "trust", "add"],
            &[
                "security",
                "trust",
                "add",
                "--minisign",
                "x",
                "--cosign",
                "y",
            ],
            &["security", "policy", "set"],
            &["security", "policy", "set", "--require-signed", "maybe"],
        ])
        .await;
    insta::assert_snapshot!("security_usage", t);
}

fn wasm(dir: &Path) -> (String, String) {
    let m = fake_module(&[("orders", 1)]);
    let p = dir.join("orders.wasm");
    std::fs::write(&p, &m).expect("write");
    (p.display().to_string(), sha256_hex(&m))
}

#[tokio::test]
async fn deploy_signature_flag() {
    let f = fx().await;
    let (module, _sha) = wasm(f.dir.path());
    let s1 = f.file("a.minisig", b"untrusted comment: one\nRWQ...\n");
    let s2 = f.file("b.sig", b"MEUCIQ...");
    let (out, code) = f
        .exec(&["deploy", &module, "--signature", &s1, "--signature", &s2])
        .await;
    assert_eq!(code, 0, "{out}");
    assert_eq!(
        f.cp.last_upload_signatures(),
        vec![
            b"untrusted comment: one\nRWQ...\n".to_vec(),
            b"MEUCIQ...".to_vec()
        ],
        "each file is sent in its own Ostro-Module-Signature header"
    );

    // Refused locally: an oversized file, too many files, the project form.
    let big = f.file("big.sig", &vec![b'A'; 8 * 1024 + 1]);
    let empty = f.file("empty.sig", b"");
    let many: Vec<String> = (0..5).map(|i| f.file(&format!("s{i}.sig"), b"x")).collect();
    let mut five: Vec<&str> = vec!["deploy", &module];
    for m in &many {
        five.extend(["--signature", m.as_str()]);
    }
    let proj = f.dir.path().join("proj");
    std::fs::create_dir_all(&proj).expect("proj");
    let proj = proj.display().to_string();
    let t = f
        .transcript(&[
            &["deploy", &module, "--signature", &big],
            &["deploy", &module, "--signature", &empty],
            &five,
            &["deploy", &proj, "--signature", &s1],
        ])
        .await;
    insta::assert_snapshot!("deploy_signature_refused", t);
    assert_eq!(f.cp.last_upload_signatures().len(), 2, "no upload was made");
}
