//! ostro-api: HTTP + gRPC API surface.
//!
//! [`domain::ControlPlane`] is the transport-neutral contract every adapter calls.

use std::sync::Arc;

pub mod domain;
pub mod error;
#[cfg(any(test, feature = "fake"))]
pub mod fake;
pub mod grpc;
mod openapi;
pub mod phase11;
pub mod phase7;
pub mod phase8;
pub mod rest;

pub use domain::ControlPlane;
pub use error::{ApiError, ErrorCode};
pub use openapi::openapi;
pub use phase8::ApiExtensions;

use ostro_proto::api::v1::{
    connectors_server::ConnectorsServer, modules_server::ModulesServer,
    queues_server::QueuesServer, runs_server::RunsServer, secrets_server::SecretsServer,
    tokens_server::TokensServer, workers_server::WorkersServer, workflows_server::WorkflowsServer,
};
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

/// HTTP API limits and router options.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApiConfig {
    /// Body limit of `PUT /v1/modules/{sha256}` only.
    pub max_module_bytes: usize,
    /// Body limit of every other route.
    pub max_request_bytes: usize,
    /// Largest run input / signal / await-completion payload accepted before proposal
    /// (`[limits] max_payload_bytes`, D-P11-13; enforced from 11-09). 4 MiB by default.
    pub max_payload_bytes: usize,
    /// When set, `GET /` answers `303 See Other` to this location instead of the shell
    /// page (D-P11-17; implemented by 11-09). `None` by default.
    pub root_redirect: Option<String>,
}

impl Default for ApiConfig {
    /// 64 MiB modules, 4 MiB requests (matches ostro-server's `[api]` defaults), 4 MiB
    /// payloads (`[limits]`), no root redirect.
    fn default() -> Self {
        ApiConfig {
            max_module_bytes: 64 * 1024 * 1024,
            max_request_bytes: 4 * 1024 * 1024,
            max_payload_bytes: 4 * 1024 * 1024,
            root_redirect: None,
        }
    }
}

/// Bytes allowed on top of `max_module_bytes` for the rest of an `UploadModuleRequest`
/// (sha256, language, protobuf tags/lengths).
const GRPC_UPLOAD_HEADROOM: usize = 4 * 1024;

/// The UI shell page with the server version filled in.
pub fn ui_shell_html() -> String {
    include_str!("ui_shell.html").replace("{{version}}", env!("CARGO_PKG_VERSION"))
}

/// Build the combined one-port router: REST (`/v1/...`), `/openapi.json`, `/healthz`,
/// `/readyz`, the UI shell at `/`, and the tonic gRPC services (`ostro.health.v1.Health`,
/// `ostro.api.v1.{Modules,Workflows,Runs,Tokens,Secrets,Connectors,Queues,Workers}`) under their
/// `/<package>.<Service>/` paths. The Phase 6 presign route
/// (`/v1/objects/{connection}/{key}`, [`rest::object_routes`]) is mounted outside the
/// REST middleware stack.
///
/// gRPC size limits mirror REST: the Modules server's decode limit is raised from
/// tonic's 4 MiB default to `cfg.max_module_bytes` (plus framing headroom; the module
/// bytes themselves are checked exactly by [`grpc::GrpcModules`]);
/// Workflows, Runs and Tokens use `cfg.max_request_bytes`. Over-limit messages answer
/// `RESOURCE_EXHAUSTED`.
///
/// Equivalent to [`router_with`] with no Phase 8 extensions (their REST routes answer
/// 404, their gRPC services are not mounted).
pub fn router(cp: Arc<dyn ControlPlane>, cfg: ApiConfig) -> axum::Router {
    router_with(cp, cfg, ApiExtensions::default())
}

/// [`router`] plus the Phase 8 surface ([`phase8`], 08-10): schedules, webhooks,
/// `POST /hooks/{name}` (route-level body limit `ext.hook_body_limit`), run logs and the
/// admin backup, merged via `phase8::router(ext)`; gRPC `ostro.api.v1.{Schedules,
/// Webhooks,RunLogs}` for the slots that are present.
///
/// Phase 11 (11-09): the [`phase11`] REST routes (rerun, DLQ, bulk, limits, failure
/// policies, inventory; 404 while `ext.ops` / `ext.inventory` is `None`), the payload
/// cap `cfg.max_payload_bytes` on start / signal / token completion, and `GET /` →
/// `303 cfg.root_redirect` when set.
pub fn router_with(cp: Arc<dyn ControlPlane>, cfg: ApiConfig, ext: ApiExtensions) -> axum::Router {
    let (rest, _doc) = rest::api_routes(cfg.max_module_bytes).split_for_parts();
    let grpc = tonic::service::Routes::new(HealthServer::new(HealthSvc))
        .add_service(
            ModulesServer::new(grpc::GrpcModules::new(cp.clone(), cfg.max_module_bytes))
                .max_decoding_message_size(
                    cfg.max_module_bytes.saturating_add(GRPC_UPLOAD_HEADROOM),
                ),
        )
        .add_service(
            WorkflowsServer::new(grpc::GrpcWorkflows(cp.clone()))
                .max_decoding_message_size(cfg.max_request_bytes),
        )
        .add_service(
            RunsServer::new(grpc::GrpcRuns(cp.clone()))
                .max_decoding_message_size(cfg.max_request_bytes),
        )
        .add_service(
            TokensServer::new(grpc::GrpcTokens(cp.clone()))
                .max_decoding_message_size(cfg.max_request_bytes),
        )
        // Phase 6 (06-08): bridge administration.
        .add_service(
            SecretsServer::new(grpc::GrpcSecrets(cp.clone()))
                .max_decoding_message_size(cfg.max_request_bytes),
        )
        .add_service(
            ConnectorsServer::new(grpc::GrpcConnectors(cp.clone()))
                .max_decoding_message_size(cfg.max_request_bytes),
        )
        // Phase 7 (07-11): queue flow/pause/demand, worker admin and join.
        .add_service(
            QueuesServer::new(grpc::GrpcQueues(cp.clone()))
                .max_decoding_message_size(cfg.max_request_bytes),
        )
        .add_service(
            WorkersServer::new(grpc::GrpcWorkers(cp.clone()))
                .max_decoding_message_size(cfg.max_request_bytes),
        );
    let grpc = phase8::add_grpc_services(grpc, &ext, cfg.max_request_bytes)
        .into_axum_router()
        .layer(axum::middleware::map_response(grpc::map_size_limit_status));
    // The local presign route (06-08) sits outside the API middleware stack below: API
    // authentication, when added there, must never wrap it (the signature is the
    // authorisation), and it has no trace layer (its query carries the signature).
    let (objects, _doc) = rest::object_routes().split_for_parts();
    let objects = objects
        .with_state(cp.clone())
        .layer(axum::extract::DefaultBodyLimit::max(cfg.max_request_bytes));
    // Phase 11 (11-09): payload cap and root redirect for the core handlers.
    let opts = phase11::RestOptions::from_config(&cfg);
    rest.with_state(cp)
        .layer(axum::Extension(opts))
        .merge(phase11::router(ext.clone()))
        .merge(phase8::router(ext))
        // Route-level limits (the module upload, the hook body) are inner layers and win
        // over this one.
        .layer(axum::extract::DefaultBodyLimit::max(cfg.max_request_bytes))
        .layer(tower_http::trace::TraceLayer::new_for_http())
        .merge(objects)
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
    fn router_with_phase8_extensions_builds() {
        let cp = Arc::new(fake::FakeControlPlane::new());
        let ext = phase8::fake::FakePhase8::new(cp.clone()).extensions();
        let _r: axum::Router = router_with(cp, ApiConfig::default(), ext);
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
