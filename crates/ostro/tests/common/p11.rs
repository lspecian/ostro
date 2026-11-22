//! Phase 11 CLI harness (plan 11-14): a spawned `ostro dev --ephemeral` for the rerun /
//! DLQ / limits / search verbs, an in-process API server over the `ostro_api::fake`
//! fakes for the bulk verbs, and snapshot masking helpers.
//!
//! Include it next to the shared harness (it uses `crate::common`):
//!
//! ```ignore
//! mod common;
//! #[path = "common/p11.rs"]
//! mod p11;
//! ```
#![allow(dead_code)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::Request;
use axum::middleware::Next;
use ostro_api::fake::{FakeControlPlane, FakeOps};
use ostro_api::phase11::OpsApi;
use ostro_api::{ApiConfig, ApiExtensions};
use serde_json::Value;

use crate::common::*;

/// One spawned dev server (P=4) plus a work dir for guest files.
pub struct P11Dev {
    pub srv: Spawned,
    pub addr: SocketAddr,
    pub work: tempfile::TempDir,
}

impl P11Dev {
    pub async fn start() -> Self {
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

    pub fn cli(&self, args: &[&str]) -> (i32, String, String) {
        run_cli(self.addr, args)
    }

    pub fn json(&self, args: &[&str]) -> (i32, Value, String) {
        cli_json(self.addr, args)
    }

    /// `json` asserting exit 0.
    pub fn ok(&self, args: &[&str]) -> Value {
        let (code, body, err) = self.json(args);
        assert_eq!(code, 0, "{args:?}: {body}\n{err}");
        body
    }

    /// `ostro deploy <guest> --activate`.
    pub fn deploy(&self, guest: &str) -> Value {
        let f = write_guest(self.work.path(), guest);
        self.ok(&["deploy", f.to_str().unwrap(), "--activate"])
    }

    /// `ostro run <workflow> --input <input> [--tag t]... --wait --json`: the run id and
    /// the final status (exit 0 when completed, 3 otherwise).
    pub fn run_wait(&self, workflow: &str, input: &str, tags: &[&str]) -> (String, String) {
        let mut args = vec!["run", workflow, "--input", input, "--wait"];
        for t in tags {
            args.extend(["--tag", t]);
        }
        let (code, b, err) = self.json(&args);
        let status = b["result"]["status"]
            .as_str()
            .unwrap_or_default()
            .to_owned();
        let want = if status == "completed" { 0 } else { 3 };
        assert_eq!(code, want, "{args:?}: {b}\n{err}");
        (
            b["start"]["run_id"].as_str().expect("run_id").to_owned(),
            status,
        )
    }

    /// Poll `args` (a `--json` verb) until `f` holds, for at most [`T`].
    pub fn until(&self, args: &[&str], f: impl Fn(&Value) -> bool) -> Value {
        let deadline = Instant::now() + T;
        loop {
            let v = self.ok(args);
            if f(&v) {
                return v;
            }
            assert!(Instant::now() < deadline, "{args:?} never matched: {v}");
            std::thread::sleep(Duration::from_millis(200));
        }
    }

    pub fn stop(mut self) {
        self.srv.stop_clean("INT");
    }
}

/// The `run_id` values of a `runs` / `entries` array.
pub fn ids(v: &Value, key: &str) -> Vec<String> {
    v[key]
        .as_array()
        .unwrap_or_else(|| panic!("no {key} in {v}"))
        .iter()
        .map(|r| r["run_id"].as_str().expect("run_id").to_owned())
        .collect()
}

/// Replace every RFC 3339 whole-second UTC timestamp (`2026-11-22T10:00:00Z`) with
/// `[time]` (padded to the same width so table columns stay aligned).
pub fn mask_times(s: &str) -> String {
    const PAT: &[u8] = b"dddd-dd-ddTdd:dd:ddZ";
    let b = s.as_bytes();
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    while i < b.len() {
        let hit = b.len() - i >= PAT.len()
            && PAT.iter().zip(&b[i..]).all(|(p, c)| match p {
                b'd' => c.is_ascii_digit(),
                p => p == c,
            });
        if hit {
            out.push_str(&format!("{:<width$}", "[time]", width = PAT.len()));
            i += PAT.len();
        } else {
            let ch = s[i..].chars().next().expect("char");
            out.push(ch);
            i += ch.len_utf8();
        }
    }
    out
}

/// Replace each `(needle, label)` (label padded to the needle's width so tables stay
/// aligned).
pub fn mask(s: &str, subst: &[(&str, &str)]) -> String {
    let mut out = s.to_owned();
    for (needle, label) in subst {
        if !needle.is_empty() {
            let padded = format!("{label:<width$}", width = needle.len());
            out = out.replace(needle, &padded);
        }
    }
    out
}

/// In-process API server: `ostro_api::router_with(FakeControlPlane, ApiConfig, ext)` with
/// [`FakeOps`] in `ext.ops`, on its own thread and runtime (the CLI processes are spawned
/// synchronously by the tests). Every `GET /v1/bulk/{job}` first advances that job by
/// one op (`done += 1`), so `ostro bulk status --watch` sees progress and a terminal
/// state.
pub struct FakeApi {
    pub addr: SocketAddr,
    pub ops: Arc<FakeOps>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl FakeApi {
    pub fn start() -> Self {
        let ops = FakeOps::new();
        let mut ext = ApiExtensions::default();
        ops.install(&mut ext);
        let advancing = Arc::clone(&ops);
        let router =
            ostro_api::router_with(Arc::new(FakeControlPlane::new()), ApiConfig::default(), ext)
                .layer(axum::middleware::from_fn(
                    move |req: Request, next: Next| {
                        let ops = Arc::clone(&advancing);
                        async move {
                            if req.method() == axum::http::Method::GET
                                && let Some(job) = req.uri().path().strip_prefix("/v1/bulk/")
                                && !job.contains('/')
                                && let Ok(j) = ops.get_bulk(job.to_owned()).await
                            {
                                ops.set_bulk_progress(&j.job_id, j.done + 1, j.skipped);
                            }
                            next.run(req).await
                        }
                    },
                ));
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        listener.set_nonblocking(true).expect("nonblocking");
        let addr = listener.local_addr().expect("addr");
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let thread = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            rt.block_on(async move {
                let l = tokio::net::TcpListener::from_std(listener).expect("listener");
                axum::serve(l, router)
                    .with_graceful_shutdown(async {
                        let _ = rx.await;
                    })
                    .await
                    .expect("serve");
            });
        });
        Self {
            addr,
            ops,
            shutdown: Some(tx),
            thread: Some(thread),
        }
    }

    pub fn cli(&self, args: &[&str]) -> (i32, String, String) {
        run_cli(self.addr, args)
    }

    pub fn json(&self, args: &[&str]) -> (i32, Value, String) {
        cli_json(self.addr, args)
    }
}

impl Drop for FakeApi {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}
