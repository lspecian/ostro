//! Transport failures of the production gRPC client are `unavailable` (13-03; the 13-12
//! router re-routes a reason-less `unavailable` from a peer once and then answers a
//! retryable 503). A peer that dies mid-call used to surface as tonic's client-side
//! `Unknown "transport error"`, which the table maps to `internal` (a 500): the 13-13
//! `ryw_through_failover` scenario saw it on a start forwarded to a leader being killed.
//!
//! The server here accepts the call and never answers; a TCP relay in front of it is
//! dropped while the call is in flight (both sockets close, as when the peer process dies).

use std::net::SocketAddr;
use std::time::Duration;

use ostro_api::grpc_client::GrpcControlPlane;
use ostro_api::{ControlPlane, ErrorCode};
use tonic::transport::Channel;

/// An h2c server whose every request hangs (the call is in flight until the link dies).
async fn hanging_server() -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let app = axum::Router::new().fallback(|| async {
        tokio::time::sleep(Duration::from_secs(3600)).await;
        "never"
    });
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    (addr, task)
}

/// A TCP relay to `upstream`; aborting the returned task (and the per-connection tasks it
/// owns) closes both sides of every relayed connection.
async fn relay(upstream: SocketAddr) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind relay");
    let addr = listener.local_addr().expect("addr");
    let task = tokio::spawn(async move {
        let mut conns = tokio::task::JoinSet::new();
        loop {
            let Ok((mut inbound, _)) = listener.accept().await else {
                return;
            };
            conns.spawn(async move {
                let Ok(mut outbound) = tokio::net::TcpStream::connect(upstream).await else {
                    return;
                };
                let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
            });
        }
    });
    (addr, task)
}

#[tokio::test]
async fn peer_dying_mid_call_is_unavailable() {
    let (server, server_task) = hanging_server().await;
    for round in 0..5 {
        let (front, relay_task) = relay(server).await;
        let ch = Channel::from_shared(format!("http://{front}"))
            .expect("uri")
            .connect()
            .await
            .expect("h2c connect");
        let client = GrpcControlPlane::new(ch);
        let call = tokio::spawn(async move { client.get_run("run-x".into()).await });
        // The request is on the wire (the server holds it), then the link dies.
        tokio::time::sleep(Duration::from_millis(200 + 50 * round)).await;
        relay_task.abort();
        let e = tokio::time::timeout(Duration::from_secs(10), call)
            .await
            .expect("a dead link fails the call, it does not hang")
            .expect("call task")
            .expect_err("the server never answers");
        assert_eq!(
            e.code,
            ErrorCode::Unavailable,
            "round {round}: a transport failure must be retryable, got {e:?}"
        );
        assert_eq!(
            e.reason(),
            None,
            "round {round}: reason-less (the router's transport rule): {e:?}"
        );
    }
    server_task.abort();
}

#[test]
fn client_side_transport_statuses_are_unavailable() {
    use ostro_api::grpc_client::status_to_error;
    // What tonic builds for a hyper / io failure below the gRPC layer.
    let io = std::io::Error::new(std::io::ErrorKind::ConnectionReset, "connection reset");
    let s = tonic::Status::from_error(Box::new(io));
    assert_eq!(s.code(), tonic::Code::Unknown, "{s:?}");
    let e = status_to_error(&s);
    assert_eq!(e.code, ErrorCode::Unavailable, "{e:?}");
    // A server's answer (decoded from the trailers: no source error) keeps the table.
    let e = status_to_error(&tonic::Status::unknown("server said unknown"));
    assert_eq!(e.code, ErrorCode::Internal, "{e:?}");
    let e = status_to_error(&tonic::Status::internal("boom"));
    assert_eq!(e.code, ErrorCode::Internal, "{e:?}");
}
