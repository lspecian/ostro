//! p14-fixups-4 (14-23 soak finding 1): fd exhaustion and a stopped metadata Raft driver.
//!
//! - `[api] max_connections` caps the client listener: with a cap of 4, idle connections
//!   beyond it are not accepted (a request on a fifth connection waits) until one closes;
//! - failpoints build: a metadata Raft storage failure (`meta::log_append=return`, the same
//!   path as EMFILE / ENOSPC / EIO) fail-stops the server: `ServerHandle::wait` returns,
//!   `ServerHandle::fatal` names the stopped driver (the binary then exits non-zero), and
//!   the shutdown that follows finishes.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::time::Duration;

use common::{ServerOpts, TestServer};
use reqwest::StatusCode;

const LIMIT: Duration = Duration::from_secs(60);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn max_connections_caps_the_client_listener() {
    let mut s = TestServer::start_with(ServerOpts::default(), |c| {
        c.api.max_connections = 4;
    })
    .await;
    s.wait_ready(LIMIT).await;
    // The readiness poll's pooled keep-alive connection holds a slot: drop it.
    s.http = reqwest::Client::new();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let addr = s.addr();
    // Four idle connections take every permit (accepted, no request sent).
    let mut idle = Vec::new();
    for _ in 0..4 {
        idle.push(tokio::net::TcpStream::connect(addr).await.expect("connect"));
    }
    // Let the listener accept them.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let fresh = reqwest::Client::builder()
        .no_proxy()
        .pool_max_idle_per_host(0)
        .build()
        .expect("client");
    let url = s.url("/readyz");
    // One request on a fifth connection: it stays pending while the cap is reached.
    let mut pending = tokio::spawn(async move { fresh.get(&url).send().await });
    let early = tokio::time::timeout(Duration::from_secs(2), &mut pending).await;
    assert!(
        early.is_err(),
        "a fifth connection must wait at the cap: {early:?}"
    );
    // The oldest (accepted) one closes: the waiting request is served.
    drop(idle.remove(0));
    let r = tokio::time::timeout(Duration::from_secs(20), pending)
        .await
        .expect("served once a connection closed")
        .expect("task")
        .expect("readyz");
    assert_eq!(r.status(), StatusCode::OK);
    drop(idle);
    s.shutdown().await;
}

#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stopped_metadata_raft_driver_fail_stops_the_server() {
    use serde_json::json;

    let mut s = TestServer::start(ServerOpts::default()).await;
    s.wait_ready(LIMIT).await;
    assert!(s.handle.as_ref().expect("handle").fatal().is_none());
    fail::cfg("meta::log_append", "return").expect("failpoint");
    // Any metadata proposal reaches the log append and stops the driver.
    let (st, v) = s.post_json("/v1/tenants", &json!({ "id": "acme" })).await;
    assert_ne!(st, StatusCode::OK, "the proposal cannot commit: {v}");
    let h = s.handle.as_ref().expect("handle");
    tokio::time::timeout(LIMIT, h.wait())
        .await
        .expect("the server fail-stops after the metadata driver stopped");
    let reason = h.fatal().expect("a fail-stop reason");
    fail::remove("meta::log_append");
    assert!(
        reason.contains("metadata raft driver stopped") && reason.contains("raft storage"),
        "{reason}"
    );
    // The binary shuts down (flushing what it can) and exits non-zero; the shutdown ends.
    tokio::time::timeout(Duration::from_secs(90), s.shutdown())
        .await
        .expect("shutdown after a fail-stop finishes");
}
