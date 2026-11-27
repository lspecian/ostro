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

/// Phase 14 (14-02): the D-P14-29 paths, the `bearerAuth` scheme applied globally, and an
/// explicit empty `security` on exactly the unauthenticated operations.
#[test]
fn phase14_paths_and_bearer_security() {
    let doc = serde_json::to_value(ostro_api::openapi()).expect("serialize");
    let paths = doc["paths"].as_object().expect("paths");
    for (path, method) in [
        ("/v1/tenants", "get"),
        ("/v1/tenants", "post"),
        ("/v1/tenants/{id}", "get"),
        ("/v1/tenants/{id}", "patch"),
        ("/v1/tenants/{id}", "delete"),
        ("/v1/tenants/{id}/suspend", "post"),
        ("/v1/tenants/{id}/resume", "post"),
        ("/v1/auth/tokens", "get"),
        ("/v1/auth/tokens", "post"),
        ("/v1/auth/tokens/{id}", "delete"),
        ("/v1/auth/tokens/{id}/rotate", "post"),
        ("/v1/auth/whoami", "get"),
        ("/v1/audit", "get"),
        ("/v1/security/modules/policy", "get"),
        ("/v1/security/modules/policy", "put"),
        ("/v1/security/modules/keys", "post"),
        ("/v1/security/modules/keys/{key_id}", "delete"),
        ("/v1/modules/{sha}/signatures", "post"),
        ("/v1/security/sealing", "get"),
        ("/v1/security/sealing/reseal", "post"),
        ("/hooks/{tenant}/{name}", "post"),
    ] {
        assert!(
            paths.get(path).and_then(|p| p.get(method)).is_some(),
            "{method} {path} missing from OpenAPI"
        );
    }
    let scheme = &doc["components"]["securitySchemes"]["bearerAuth"];
    assert_eq!(scheme["type"], "http");
    assert_eq!(scheme["scheme"], "bearer");
    assert_eq!(doc["security"], serde_json::json!([{ "bearerAuth": [] }]));
    let upload = &paths["/v1/modules/{sha256}"]["put"]["parameters"];
    assert!(
        upload
            .as_array()
            .expect("params")
            .iter()
            .any(|p| p["name"] == "Ostro-Module-Signature" && p["in"] == "header"),
        "{upload}"
    );
    let mut open = Vec::new();
    for (path, item) in paths {
        for (method, op) in item.as_object().expect("item") {
            if op.get("security") == Some(&serde_json::json!([])) {
                open.push(format!("{method} {path}"));
            }
        }
    }
    open.sort();
    assert_eq!(
        open,
        [
            "get /",
            "get /healthz",
            "get /readyz",
            "get /v1/objects/{connection}/{key}",
            "post /hooks/{name}",
            "post /hooks/{tenant}/{name}",
            "post /v1/cluster/join",
            "post /v1/workers/join",
            "put /v1/objects/{connection}/{key}",
        ]
    );
}
