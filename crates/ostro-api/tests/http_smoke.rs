//! One-port mux smoke test: HTTP/1.1 `GET /healthz` and a gRPC health check are both
//! answered by `ostro_api::router()` on the same ephemeral listener.

use ostro_proto::health::v1::{CheckRequest, health_client::HealthClient};

#[tokio::test]
async fn http1_and_grpc_on_one_listener() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, ostro_api::router())
            .await
            .expect("serve");
    });

    // (a) HTTP/1.1 health endpoint.
    let resp = reqwest::Client::builder()
        .http1_only()
        .build()
        .expect("client")
        .get(format!("http://{addr}/healthz"))
        .send()
        .await
        .expect("GET /healthz");
    assert_eq!(resp.version(), reqwest::Version::HTTP_11);
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(resp.text().await.expect("body"), "ok");

    // (b) gRPC over h2c prior-knowledge on the same port.
    let mut client = HealthClient::connect(format!("http://{addr}"))
        .await
        .expect("grpc connect");
    let status = client
        .check(CheckRequest::default())
        .await
        .expect("grpc check")
        .into_inner()
        .status;
    assert_eq!(status, "SERVING");

    server.abort();
}
