//! The OpenAPI document is pinned by a snapshot and lists every REST route.

#[test]
fn openapi_document() {
    let doc = ostro_api::openapi();
    let pretty = serde_json::to_string_pretty(&doc).expect("serialize");
    insta::assert_snapshot!(pretty);
}

#[test]
fn every_route_is_documented() {
    let doc = serde_json::to_value(ostro_api::openapi()).expect("serialize");
    let paths = doc["paths"].as_object().expect("paths");
    for (path, method) in [
        ("/v1/modules/{sha256}", "put"),
        ("/v1/modules/{sha256}", "get"),
        ("/v1/workflows", "get"),
        ("/v1/workflows/{name}/active", "put"),
        ("/v1/workflows/{name}/runs", "post"),
        ("/v1/runs", "get"),
        ("/v1/runs/{run_id}", "get"),
        ("/v1/runs/{run_id}/history", "get"),
        ("/v1/runs/{run_id}/signals/{name}", "post"),
        ("/v1/runs/{run_id}/cancel", "post"),
        ("/v1/runs/{run_id}/terminate", "post"),
        ("/v1/runs/{run_id}/retry", "post"),
        ("/v1/runs/{run_id}/result", "get"),
        ("/v1/tokens/complete", "post"),
        ("/v1/workflows/{name}/policy", "put"),
        ("/v1/workflows/{name}/policy", "get"),
        ("/v1/queues/{name}/policy", "put"),
        ("/v1/queues/{name}/policy", "get"),
        ("/healthz", "get"),
        ("/readyz", "get"),
        ("/openapi.json", "get"),
        ("/", "get"),
    ] {
        assert!(
            paths.get(path).and_then(|p| p.get(method)).is_some(),
            "{method} {path} missing from OpenAPI"
        );
    }
    // Await tokens never appear in a path or query parameter (access logs).
    for (path, item) in paths {
        assert!(!path.contains("token}"), "token in path: {path}");
        for op in item.as_object().expect("path item").values() {
            for p in op["parameters"].as_array().into_iter().flatten() {
                assert_ne!(p["name"], "token", "token parameter on {path}");
            }
        }
    }
    assert_eq!(doc["info"]["title"], "Ostro API");
    assert_eq!(doc["info"]["version"], env!("CARGO_PKG_VERSION"));
}
