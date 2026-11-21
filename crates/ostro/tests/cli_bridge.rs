//! 06-16: the bridge admin verbs (`ostro secret|grant|connection|egress`) of the real
//! `ostro` binary against a spawned `ostro dev --ephemeral` (dev profile: an automatic
//! sealing key), every verb with `--json`.
//!
//! Secret values are write-only: `secret set` reads them from stdin / `--from-file` /
//! `--from-env`, never from argv. The test checks the CLI's argv (`/proc/<pid>/cmdline`,
//! what a process listing shows) while it waits for stdin, and every stdout/stderr of the
//! CLI and the server's own log for the canary values.

mod common;

use std::io::Write;
use std::net::SocketAddr;
use std::process::Stdio;
use std::time::Duration;

use common::*;
use serde_json::{Value, json};

const CANARY_STDIN: &str = "cnry-stdin-4e1b7a93c2d05f68";
const CANARY_ENV: &str = "cnry-env-9a6c3e1f7b2d4085";
const CANARY_FILE: &str = "cnry-file-1d5f9b3a7e2c6048";

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
            .env("OSTRO_LOG__LEVEL", "debug");
        let srv = Spawned::spawn(cmd);
        srv.wait_readyz(&http(), addr).await;
        Self { srv, addr, work }
    }

    /// `ostro <args> --json` asserting exit 0; stdout parsed.
    fn ok(&self, args: &[&str]) -> Value {
        let (code, body, err) = cli_json(self.addr, args);
        assert_eq!(code, 0, "{args:?}: {body}\n{err}");
        body
    }

    /// `ostro <args> --json` expecting exit 1 (an API error body on stdout).
    fn fails(&self, args: &[&str]) -> Value {
        let (code, body, err) = cli_json(self.addr, args);
        assert_eq!(code, 1, "{args:?}: {body}\n{err}");
        body
    }

    /// Run the CLI with `stdin` piped in. Before writing it, the process's argv is read
    /// from `/proc` (it blocks on stdin, so it is still alive). Returns (exit code,
    /// stdout, stderr, argv as seen by a process listing).
    fn piped(&self, args: &[&str], stdin: &[u8]) -> (i32, String, String, String) {
        let mut cmd = command();
        cmd.env("OSTRO_SERVER", format!("http://{}", self.addr))
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = cmd.spawn().expect("spawn ostro");
        // Right after fork the child may not have exec'd yet (empty or the test's own
        // argv): poll until the process shows the CLI's arguments.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let argv = loop {
            let a = process_argv(child.id());
            if a.contains(args[0]) || std::time::Instant::now() > deadline {
                break a;
            }
            std::thread::sleep(Duration::from_millis(5));
        };
        {
            let mut w = child.stdin.take().expect("stdin");
            w.write_all(stdin).expect("write stdin");
        }
        let out = child.wait_with_output().expect("wait");
        (
            out.status.code().unwrap_or(-1),
            String::from_utf8_lossy(&out.stdout).into_owned(),
            String::from_utf8_lossy(&out.stderr).into_owned(),
            argv,
        )
    }
}

/// The live argv of `pid` as other local users would see it. Linux: `/proc/<pid>/cmdline`.
/// Elsewhere (macOS has no procfs): `ps -ww -o args=`, which reads the same kernel argv
/// (`KERN_PROCARGS2`). Empty when the process is gone or not yet visible.
fn process_argv(pid: u32) -> String {
    #[cfg(target_os = "linux")]
    {
        std::fs::read(format!("/proc/{pid}/cmdline"))
            .map(|b| String::from_utf8_lossy(&b).replace('\0', " "))
            .unwrap_or_default()
    }
    #[cfg(not(target_os = "linux"))]
    {
        std::process::Command::new("ps")
            .args(["-ww", "-o", "args=", "-p", &pid.to_string()])
            .stdin(Stdio::null())
            .stderr(Stdio::null())
            .output()
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
            .unwrap_or_default()
    }
}

fn names(v: &Value, list: &str, field: &str) -> Vec<String> {
    v[list]
        .as_array()
        .unwrap_or_else(|| panic!("{list} in {v}"))
        .iter()
        .map(|x| x[field].as_str().expect("name").to_owned())
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bridge_verbs_round_trip_without_leaking_values() {
    let _g = SERIAL.lock().await;
    let d = Dev::start().await;
    let mut transcript = String::new();

    // --- secret set: stdin (default), --from-env, --from-file ---
    let (code, out, err, argv) = d.piped(
        &["secret", "set", "stripe/live", "--json"],
        format!("{CANARY_STDIN}\n").as_bytes(),
    );
    assert_eq!(code, 0, "{out}\n{err}");
    assert!(argv.contains("secret set stripe/live"), "argv {argv:?}");
    assert!(!argv.contains(CANARY_STDIN), "argv {argv:?}");
    let v: Value = serde_json::from_str(&out).expect("json");
    assert_eq!(v["name"], "stripe/live");
    assert!(
        v["key_id"].as_str().unwrap().starts_with("aes256gcm:"),
        "{v}"
    );
    transcript.push_str(&out);
    transcript.push_str(&err);

    let mut cmd = command();
    cmd.env("OSTRO_SERVER", format!("http://{}", d.addr))
        .env("STRIPE_TEST_VALUE", CANARY_ENV)
        .args([
            "secret",
            "set",
            "stripe/test",
            "--tenant",
            "acme",
            "--from-env",
            "STRIPE_TEST_VALUE",
            "--json",
        ])
        .stdin(Stdio::null());
    let (st, out, err) = run_to_end(cmd);
    assert!(st.success(), "{out}\n{err}");
    assert_eq!(
        serde_json::from_str::<Value>(&out).unwrap()["tenant"],
        "acme"
    );
    transcript.push_str(&out);
    transcript.push_str(&err);

    let f = d.work.path().join("value.txt");
    std::fs::write(&f, format!("{CANARY_FILE}\n")).unwrap();
    let (code, out, err) = run_cli(
        d.addr,
        &[
            "secret",
            "set",
            "github/token",
            "--from-file",
            f.to_str().unwrap(),
        ],
    );
    assert_eq!(code, 0, "{out}\n{err}");
    assert!(out.contains("secret github/token set"), "{out}");
    transcript.push_str(&out);
    transcript.push_str(&err);

    // A value on argv is a usage error; a too-short value is refused by the server.
    let (code, _, _) = run_cli(d.addr, &["secret", "set", "x", "short-value-on-argv"]);
    assert_eq!(code, 2);
    let (code, out, err, _) = d.piped(&["secret", "set", "short", "--json"], b"7-bytes");
    assert_eq!(code, 1, "{out}\n{err}");
    assert!(out.contains("secret_too_short"), "{out}");
    assert!(
        !out.contains("7-bytes") && !err.contains("7-bytes"),
        "{out}\n{err}"
    );
    let (code, out, err, _) = d.piped(&["secret", "set", "__ostro/x", "--json"], b"long-enough-1");
    assert_eq!(code, 1, "{out}\n{err}");
    assert!(out.contains("secret_reserved"), "{out}");

    let v = d.ok(&["secret", "ls"]);
    assert_eq!(
        names(&v, "secrets", "name"),
        ["github/token", "stripe/live"]
    );
    transcript.push_str(&v.to_string());
    let v = d.ok(&["secret", "ls", "--tenant", "acme"]);
    assert_eq!(names(&v, "secrets", "name"), ["stripe/test"]);
    transcript.push_str(&v.to_string());
    let (code, human, err) = run_cli(d.addr, &["secret", "ls"]);
    assert_eq!(code, 0, "{err}");
    assert!(
        human.contains("NAME") && human.contains("stripe/live"),
        "{human}"
    );
    transcript.push_str(&human);
    assert_eq!(d.ok(&["secret", "rm", "github/token"]), Value::Null);
    let v = d.fails(&["secret", "rm", "github/token"]);
    assert_eq!(v["error"]["code"], "not_found", "{v}");

    // --- grant ---
    let v = d.ok(&[
        "grant",
        "add",
        "--module",
        "acme/billing",
        "--secret",
        "stripe/live",
        "--origin",
        "https://API.stripe.com",
        "--origin",
        "https://*.example.com:8443",
        "--body",
    ]);
    assert_eq!(v["id"], "secret:acme/billing:stripe/live");
    assert_eq!(
        v["origins"],
        json!(["https://api.stripe.com", "https://*.example.com:8443"])
    );
    d.ok(&[
        "grant",
        "add",
        "--module",
        "other",
        "--secret",
        "s1",
        "--origin",
        "https://a.example",
    ]);
    let v = d.ok(&["grant", "ls", "--module", "acme/billing"]);
    assert_eq!(
        names(&v, "grants", "id"),
        ["secret:acme/billing:stripe/live"]
    );
    let v = d.ok(&["grant", "ls"]);
    assert_eq!(v["grants"].as_array().unwrap().len(), 2);
    assert_eq!(d.ok(&["grant", "rm", "secret:other:s1"]), Value::Null);
    let v = d.fails(&["grant", "rm", "token:abc"]);
    assert_eq!(v["error"]["code"], "invalid_argument", "{v}");
    transcript.push_str(&d.ok(&["grant", "ls"]).to_string());

    // --- connection (config from a file and from stdin) ---
    let root = d.work.path().join("objects");
    let cfg = d.work.path().join("files.json");
    std::fs::write(
        &cfg,
        json!({"v": 1, "backend": "local", "root": root}).to_string(),
    )
    .unwrap();
    let v = d.ok(&[
        "connection",
        "set",
        "files",
        "--kind",
        "object",
        "--config-file",
        cfg.to_str().unwrap(),
    ]);
    assert_eq!(v["kind"], "object");
    let pg = json!({"v": 1, "host": "127.0.0.1", "database": "app", "user": "app",
                    "password": "{{secret:db/pass}}"});
    let (code, out, err, _) = d.piped(
        &[
            "connection",
            "set",
            "orders-db",
            "--kind",
            "sql-postgres",
            "--config-file",
            "-",
            "--json",
        ],
        pg.to_string().as_bytes(),
    );
    assert_eq!(code, 0, "{out}\n{err}");
    let target_cfg = d.work.path().join("target.json");
    std::fs::write(&target_cfg, r#"{"v":1}"#).unwrap();
    let v = d.ok(&[
        "connection",
        "set",
        "https://api.stripe.com",
        "--kind",
        "http-target",
        "--config-file",
        target_cfg.to_str().unwrap(),
    ]);
    assert_eq!(v["name"], "https://api.stripe.com");
    let v = d.ok(&["connection", "get", "orders-db"]);
    assert_eq!(v["config"], pg, "templates come back verbatim");
    let v = d.ok(&["connection", "ls"]);
    assert_eq!(
        names(&v, "connections", "name"),
        ["files", "https://api.stripe.com", "orders-db"]
    );
    transcript.push_str(&v.to_string());
    assert_eq!(
        d.ok(&["connection", "rm", "https://api.stripe.com"]),
        Value::Null
    );
    let v = d.fails(&["connection", "get", "https://api.stripe.com"]);
    assert_eq!(v["error"]["code"], "not_found", "{v}");

    // --- egress ---
    let v = d.ok(&[
        "egress",
        "set",
        "acme/billing",
        "--allow",
        "https://api.stripe.com",
        "--allow",
        "connection:orders-db",
    ]);
    assert_eq!(
        v["allow"],
        json!(["https://api.stripe.com", "connection:orders-db"])
    );
    let v = d.fails(&["egress", "set", "m", "--allow", "ftp://x.example"]);
    assert_eq!(v["error"]["code"], "invalid_argument", "{v}");
    let v = d.ok(&["egress", "get", "acme/billing"]);
    assert_eq!(v["module"], "acme/billing");
    let v = d.ok(&["egress", "ls"]);
    assert_eq!(names(&v, "policies", "module"), ["acme/billing"]);
    let (code, human, _) = run_cli(d.addr, &["egress", "ls"]);
    assert_eq!(code, 0);
    assert!(human.contains("connection:orders-db"), "{human}");
    assert_eq!(d.ok(&["egress", "rm", "acme/billing"]), Value::Null);
    let v = d.fails(&["egress", "get", "acme/billing"]);
    assert_eq!(v["error"]["code"], "not_found", "{v}");

    // No canary anywhere the CLI printed, nor in the server's own (debug) log.
    // Give the server's log reader a moment to collect the last lines.
    std::thread::sleep(Duration::from_secs(1));
    let server_log = d.srv.stderr() + &d.srv.stdout_lines().join("\n");
    for c in [CANARY_STDIN, CANARY_ENV, CANARY_FILE] {
        assert!(!transcript.contains(c), "{c} printed by the CLI");
        assert!(!server_log.contains(c), "{c} in the server log");
    }
}
