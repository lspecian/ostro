//! The OpenAPI document, derived from the same route definitions the server mounts.

use std::sync::OnceLock;

/// OpenAPI document of the REST API (built once from [`crate::rest::api_routes`]).
///
/// Needs no [`crate::ControlPlane`]: the routes are split before state is attached.
pub fn openapi() -> utoipa::openapi::OpenApi {
    static DOC: OnceLock<utoipa::openapi::OpenApi> = OnceLock::new();
    DOC.get_or_init(|| {
        crate::rest::api_routes(crate::ApiConfig::default().max_module_bytes)
            .split_for_parts()
            .1
    })
    .clone()
}
