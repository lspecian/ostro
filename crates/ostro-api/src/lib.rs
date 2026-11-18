//! ostro-api: HTTP + gRPC API surface (skeleton).

use ostro_proto::health::v1::{
    CheckRequest, CheckResponse,
    health_server::{Health, HealthServer},
};

/// gRPC health service. Always reports `SERVING` in the skeleton.
#[derive(Debug, Default, Clone, Copy)]
pub struct HealthSvc;

#[tonic::async_trait]
impl Health for HealthSvc {
    async fn check(
        &self,
        _request: tonic::Request<CheckRequest>,
    ) -> Result<tonic::Response<CheckResponse>, tonic::Status> {
        Ok(tonic::Response::new(CheckResponse {
            status: "SERVING".to_owned(),
        }))
    }
}

/// The process-wide TLS crypto provider: rustls backed by aws-lc-rs.
pub fn tls_provider() -> rustls::crypto::CryptoProvider {
    rustls::crypto::aws_lc_rs::default_provider()
}

/// Build the combined HTTP router: tonic gRPC services plus `GET /healthz`.
pub fn router() -> axum::Router {
    let grpc = tonic::service::Routes::new(HealthServer::new(HealthSvc)).into_axum_router();
    axum::Router::new()
        .route("/healthz", axum::routing::get(|| async { "ok" }))
        .merge(grpc)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn router_builds() {
        let _r: axum::Router = router();
    }

    #[tokio::test]
    async fn health_check_serving() {
        use ostro_proto::health::v1::{CheckRequest, health_server::Health};
        let resp = HealthSvc
            .check(tonic::Request::new(CheckRequest::default()))
            .await
            .expect("check");
        assert_eq!(resp.into_inner().status, "SERVING");
    }

    #[test]
    fn aws_lc_rs_provider_has_cipher_suites() {
        let p = tls_provider();
        assert!(!p.cipher_suites.is_empty());
    }
}
