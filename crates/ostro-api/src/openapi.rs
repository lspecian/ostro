//! The OpenAPI document, derived from the same route definitions the server mounts.

use std::sync::OnceLock;

/// OpenAPI document of the REST API (built once from [`crate::rest::api_routes`]).
///
/// Needs no [`crate::ControlPlane`]: the routes are split before state is attached.
pub fn openapi() -> utoipa::openapi::OpenApi {
    static DOC: OnceLock<utoipa::openapi::OpenApi> = OnceLock::new();
    DOC.get_or_init(|| {
        let mut doc = crate::rest::api_routes(crate::ApiConfig::default().max_module_bytes)
            .split_for_parts()
            .1;
        // Phase 8 (08-10): paths and schemas only (its base document has no tags).
        doc.merge(
            crate::phase8::api_routes(crate::phase8::DEFAULT_HOOK_BODY_LIMIT)
                .split_for_parts()
                .1,
        );
        // Phase 11 (11-09): paths and schemas only (tags live in the base document).
        doc.merge(crate::phase11::api_routes().split_for_parts().1);
        // Phase 13 (13-03): cluster / module lifecycle paths and schemas (join included).
        doc.merge(crate::phase13_adapters::api_routes().split_for_parts().1);
        doc.merge(crate::phase13_adapters::join_routes().split_for_parts().1);
        // Phase 6 (06-08): the presign route, mounted outside the API middleware.
        doc.merge(crate::rest::object_routes().split_for_parts().1);
        doc
    })
    .clone()
}
