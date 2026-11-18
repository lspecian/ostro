//! ostro-api: HTTP + gRPC API surface.
//!
//! [`domain::ControlPlane`] is the transport-neutral contract every adapter calls.

use std::sync::Arc;

pub mod domain;
pub mod error;
#[cfg(any(test, feature = "fake"))]
pub mod fake;
mod openapi;
pub mod rest;

pub use domain::ControlPlane;
pub use error::{ApiError, ErrorCode};
pub use openapi::openapi;

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

/// HTTP API limits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ApiConfig {
    /// Body limit of `PUT /v1/modules/{sha256}` only.
    pub max_module_bytes: usize,
    /// Body limit of every other route.
    pub max_request_bytes: usize,
}

impl Default for ApiConfig {
    /// 64 MiB modules, 4 MiB requests (matches ostro-server's `[api]` defaults).
    fn default() -> Self {
        ApiConfig {
            max_module_bytes: 64 * 1024 * 1024,
            max_request_bytes: 4 * 1024 * 1024,
        }
    }
}

/// The UI shell page with the server version filled in.
pub fn ui_shell_html() -> String {
    include_str!("ui_shell.html").replace("{{version}}", env!("CARGO_PKG_VERSION"))
}

/// Build the combined one-port router: REST (`/v1/...`), `/openapi.json`, `/healthz`,
/// `/readyz`, the UI shell at `/`, and the tonic gRPC services.
pub fn router(cp: Arc<dyn ControlPlane>, cfg: ApiConfig) -> axum::Router {
    let (rest, _doc) = rest::api_routes(cfg.max_module_bytes).split_for_parts();
    let grpc = tonic::service::Routes::new(HealthServer::new(HealthSvc)).into_axum_router();
    rest.with_state(cp)
        // Route-level limits (the module upload) are inner layers and win over this one.
        .layer(axum::extract::DefaultBodyLimit::max(cfg.max_request_bytes))
        .layer(tower_http::trace::TraceLayer::new_for_http())
        .merge(grpc)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn router_builds() {
        let cp: Arc<dyn ControlPlane> = Arc::new(fake::FakeControlPlane::new());
        let _r: axum::Router = router(cp, ApiConfig::default());
    }

    #[test]
    fn ui_shell_has_version_and_openapi_link() {
        let html = ui_shell_html();
        assert!(html.contains(env!("CARGO_PKG_VERSION")));
        assert!(!html.contains("{{version}}"));
        assert!(html.contains("/openapi.json"));
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
