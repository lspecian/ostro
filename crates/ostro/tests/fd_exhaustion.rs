//! p14-fixups-4 (14-23 soak finding 1) through the real binary:
//!
//! - under a low open-file limit (`ulimit -n 384`, soft = hard, so the startup raise cannot
//!   lift it) a flood of idle client connections (up to 600, as many as the accept backlog
//!   takes) does not exhaust the server's descriptors: the automatic `[api]
//!   max_connections` (half the limit = 192) leaves the rest in the accept backlog, the
//!   node's fd count stays at most base + cap and below the limit, no EMFILE is logged,
//!   and once the flood closes the node still serves reads and metadata writes;
//! - failpoints build: a metadata Raft storage failure (`meta::log_append=return` through the
//!   runtime failpoint endpoint, the same path as EMFILE / ENOSPC / EIO) makes the process
//!   exit non-zero with the reason on stderr instead of answering 503 forever.
#![cfg(target_os = "linux")]

mod common;

use std::net::SocketAddr;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

use common::{SERIAL, Spawned, bin, free_addr, http};
use serde_json::{Value, json};

const NOFILE: usize = 384;
/// The automatic `[api] max_connections` under `NOFILE`: half of it.
const CAP: usize = NOFILE / 2;

/// `ostro server` on `addr` / `dir` (P=4, auth none), under `ulimit -n nofile` when given.
fn server_cmd(dir: &Path, addr: SocketAddr, nofile: Option<usize>) -> Command {
    let mut cmd = match nofile {
        Some(n) => {
            let mut c = Command::new("sh");
            c.arg("-c")
                .arg(format!("ulimit -n {n} && exec \"$0\" \"$@\""))
                .arg(bin());
            c
        }
        None => Command::new(bin()),
    };
    for (k, _) in std::env::vars_os() {
        if k.to_string_lossy().starts_with("OSTRO_") {
            cmd.env_remove(&k);
        }
    }
    cmd.env_remove("RUST_LOG")
        .env_remove("FAILPOINTS")
        .env("OSTRO_CLUSTER__LISTEN", "127.0.0.1:0")
        .env("OSTRO_AUTH__MODE", "none")
        .env("OSTRO_NODE__PARTITIONS", "4")
        .current_dir(dir)
        .arg("server")
        .arg("--data-dir")
        .arg(dir.join("data"))
        .arg("--listen")
        .arg(addr.to_string());
    cmd
}

fn open_fds(pid: u32) -> usize {
    std::fs::read_dir(format!("/proc/{pid}/fd")).map_or(0, |d| d.count())
}

async fn post(addr: SocketAddr, path: &str, body: &Value) -> (u16, String) {
    match http()
        .post(format!("http://{addr}{path}"))
        .json(body)
        .send()
        .await
    {
        Ok(r) => (r.status().as_u16(), r.text().await.unwrap_or_default()),
        Err(e) => (0, e.to_string()),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connection_flood_under_a_low_fd_limit_keeps_the_node_serving() {
    let _g = SERIAL.lock().await;
    // The flood's client sockets live in this process: lift its own soft limit first.
    let _ = ostro_server::tls::conn_limit::raise_nofile_soft_limit();
    println!(
        "test process soft nofile: {:?}",
        ostro_server::tls::conn_limit::soft_nofile()
    );
    let tmp = tempfile::tempdir().expect("tmp");
    let addr = free_addr();
    let client = http();
    let mut s = Spawned::spawn(server_cmd(tmp.path(), addr, Some(NOFILE)));
    s.wait_readyz(&client, addr).await;
    let pid = s.pid();
    let base = open_fds(pid);
    println!("server fds after start: {base} (limit {NOFILE})");
    assert!(base < NOFILE / 2, "baseline {base} fds leaves no room");

    // The flood: idle connections until the kernel stops completing handshakes (accept
    // backlog full) or 600. Those past the cap wait in the backlog without a descriptor.
    let somaxconn = std::fs::read_to_string("/proc/sys/net/core/somaxconn").unwrap_or_default();
    let mut flood = Vec::new();
    let mut misses = 0;
    let mut last_err = String::new();
    while flood.len() < 600 && misses < 10 {
        match tokio::time::timeout(
            Duration::from_millis(500),
            tokio::net::TcpStream::connect(addr),
        )
        .await
        {
            Ok(Ok(c)) => {
                flood.push(c);
                misses = 0;
            }
            Ok(Err(e)) => {
                last_err = e.to_string();
                misses += 1;
            }
            Err(_) => {
                last_err = "connect timed out (backlog full)".into();
                misses += 1;
            }
        }
    }
    let mut peak = 0;
    let t0 = Instant::now();
    while t0.elapsed() < Duration::from_secs(3) {
        peak = peak.max(open_fds(pid));
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    println!(
        "flood: {} connections open (stopped: {last_err:?}; somaxconn {}); server fds: base \
         {base}, peak {peak}, cap {CAP}",
        flood.len(),
        somaxconn.trim()
    );
    assert!(
        flood.len() > CAP + 16,
        "the flood must pass the automatic cap {CAP}"
    );
    // Without the cap every flood connection would hold a server descriptor.
    assert!(
        peak <= base + CAP + 16,
        "peak {peak} fds: more than base {base} + cap {CAP} connections"
    );
    assert!(
        peak < NOFILE - 8,
        "peak {peak} fds reaches the limit {NOFILE}"
    );
    drop(flood);

    // Still serving: a read and a metadata write (the Raft driver is alive).
    s.wait_readyz(&client, addr).await;
    let (st, body) = post(addr, "/v1/tenants", &json!({ "id": "acme" })).await;
    assert!(
        (200..300).contains(&st),
        "tenant create after the flood: {st} {body}"
    );
    let err = s.stderr();
    assert!(
        !err.contains("Too many open files"),
        "EMFILE under the cap:\n{err}"
    );
    s.stop_clean("TERM");
}

#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stopped_metadata_raft_driver_exits_non_zero() {
    let _g = SERIAL.lock().await;
    let tmp = tempfile::tempdir().expect("tmp");
    let addr = free_addr();
    let client = http();
    let mut s = Spawned::spawn(server_cmd(tmp.path(), addr, None));
    s.wait_readyz(&client, addr).await;
    let (st, body) = post(
        addr,
        "/v1/admin/test/failpoints",
        &json!({ "name": "meta::log_append", "action": "return" }),
    )
    .await;
    assert_eq!(st, 200, "failpoint: {body}");
    // A metadata write reaches the Raft log append; the driver stops.
    let (st, body) = post(addr, "/v1/tenants", &json!({ "id": "acme" })).await;
    assert!(!(200..300).contains(&st), "cannot commit: {st} {body}");
    let status = s.wait_exit(common::T + Duration::from_secs(60));
    let err = s.stderr();
    assert_eq!(
        status.code(),
        Some(1),
        "exit status {status:?}; stderr:\n{err}"
    );
    assert!(
        err.contains("ostro: error: metadata raft driver stopped (raft storage:"),
        "stderr:\n{err}"
    );
}
