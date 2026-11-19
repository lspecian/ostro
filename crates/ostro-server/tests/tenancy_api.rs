//! D23 tenant-qualified names (Phase 14 F1) and the deployed module name (F4) against the
//! real server (plan 05-24): the tenant-id and resource-name grammar on start and upload
//! (400 over REST and gRPC), tenant-qualified storage keys for workflow versions, runs,
//! queues and dedupe rows, `WorkflowVersion.module`, cross-tenant re-upload of the same
//! bytes, and the two-tenant collision test (same workflow name, same idempotency key, two
//! runs).
//!
//! Tenant runs are not driven to completion here: until 05-11 strips the `<tenant>/`
//! prefix for the journal, the guest is invoked with the qualified name.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::time::Duration;

use common::{ServerOpts, TestServer, guest_wasm};
use ostro_partition::Table;
use ostro_partition::store::keys::dedupe_key;
use ostro_proto::api::v1 as pb;
use ostro_proto::api::v1::{modules_client::ModulesClient, runs_client::RunsClient};
use ostro_types::RunId;
use reqwest::StatusCode;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tonic::Code;

fn sha_hex(b: &[u8]) -> String {
    Sha256::digest(b)
        .iter()
        .map(|x| format!("{x:02x}"))
        .collect()
}

async fn server() -> TestServer {
    let s = TestServer::start(ServerOpts::default()).await;
    s.wait_ready(Duration::from_secs(5)).await;
    s
}

/// `PUT /v1/modules/{sha}` with an optional query string.
async fn put_module(s: &TestServer, wasm: &[u8], query: &str) -> (StatusCode, Value) {
    let sha = sha_hex(wasm);
    let r = s
        .http
        .put(s.url(&format!("/v1/modules/{sha}{query}")))
        .header("content-type", "application/wasm")
        .body(wasm.to_vec())
        .send()
        .await
        .expect("PUT module");
    let st = r.status();
    (st, r.json().await.expect("json body"))
}

async fn start(s: &TestServer, workflow: &str, body: Value) -> (StatusCode, Value) {
    let r = s
        .http
        .post(s.url(&format!("/v1/workflows/{workflow}/runs")))
        .json(&body)
        .send()
        .await
        .expect("POST run");
    let st = r.status();
    (st, r.json().await.expect("json body"))
}

async fn get(s: &TestServer, run_id: &str) -> Value {
    let r = s
        .http
        .get(s.url(&format!("/v1/runs/{run_id}")))
        .send()
        .await
        .expect("GET run");
    assert_eq!(r.status(), StatusCode::OK);
    r.json().await.expect("json body")
}

/// The `WorkflowVersion` record of `(name, version)` in meta.
fn version(s: &TestServer, name: &str, v: u32) -> Option<ostro_meta::WorkflowVersion> {
    s.h()
        .meta()
        .view()
        .read(|st| st.workflow(name, v).map(|w| w.value.clone()))
}

fn active(s: &TestServer, name: &str) -> Option<u32> {
    s.h()
        .meta()
        .view()
        .read(|st| st.active(name).map(|a| a.value.version))
}

fn assert_400(st: StatusCode, body: &Value, field: &str) {
    assert_eq!(st, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["code"], "invalid_argument", "{body}");
    assert_eq!(body["error"]["details"]["field"], field, "{body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upload_registers_qualified_versions_with_module_name() {
    let mut s = server().await;
    let wasm = guest_wasm("sdk-basic");
    let sha: [u8; 32] = Sha256::digest(&wasm).into();

    // Bad tenant / name: 400, nothing registered.
    for (q, field) in [
        ("?tenant=_", "tenant"),
        ("?tenant=Acme", "tenant"),
        ("?tenant=a/b", "tenant"),
        ("?name=a/b", "name"),
        ("?name=x:y", "name"),
        ("?name=__z", "name"),
    ] {
        let (st, body) = put_module(&s, &wasm, q).await;
        assert_400(st, &body, field);
    }
    assert!(s.h().meta().view().read(|st| st.module(&sha).is_none()));

    // Default tenant, named: bare keys, module = "mod-a".
    let (st, body) = put_module(&s, &wasm, "?name=mod-a").await;
    assert_eq!(st, StatusCode::CREATED, "{body}");
    let w = version(&s, "sdk-basic", 1).expect("default-tenant version");
    assert_eq!(w.module, "mod-a");
    assert_eq!(w.module_sha256, sha);
    assert_eq!(active(&s, "sdk-basic"), Some(1));

    // Same bytes, tenant acme: no recompile, no new blob, created=false, but acme's
    // versions are registered (qualified) and auto-activated, with module "acme/mod-a".
    let stats = s.h().runtime().cache_stats().expect("aot cache");
    let (st, body) = put_module(&s, &wasm, "?tenant=acme&name=mod-a").await;
    assert_eq!(st, StatusCode::OK, "{body}");
    assert_eq!(body["created"], false);
    assert_eq!(s.h().runtime().cache_stats().expect("aot cache"), stats);
    for wf in ["acme/sdk-basic", "acme/sdk-echo"] {
        let w = version(&s, wf, 1).unwrap_or_else(|| panic!("{wf} registered"));
        assert_eq!(w.module, "acme/mod-a");
        assert_eq!(w.module_sha256, sha);
        assert_eq!(active(&s, wf), Some(1));
    }
    // The default tenant's records are untouched.
    assert_eq!(
        version(&s, "sdk-basic", 1).expect("default").module,
        "mod-a"
    );
    // The (name-less) module record lists every tenant's qualified pairs.
    let names: Vec<String> = body["module"]["workflows"]
        .as_array()
        .expect("workflows")
        .iter()
        .map(|w| w["name"].as_str().expect("name").to_owned())
        .collect();
    assert_eq!(
        names,
        ["acme/sdk-basic", "acme/sdk-echo", "sdk-basic", "sdk-echo"]
    );

    // Re-upload by acme with the same name: a no-op. With a different name: 409
    // (write-once bindings).
    let (st, body) = put_module(&s, &wasm, "?tenant=acme&name=mod-a").await;
    assert_eq!(st, StatusCode::OK, "{body}");
    let (st, body) = put_module(&s, &wasm, "?tenant=acme&name=other").await;
    assert_eq!(st, StatusCode::CONFLICT, "{body}");
    assert_eq!(
        body["error"]["details"]["existing_module_name"],
        "acme/mod-a"
    );
    // Without a name the existing bindings are simply kept.
    let (st, body) = put_module(&s, &wasm, "?tenant=acme").await;
    assert_eq!(st, StatusCode::OK, "{body}");

    // Tenant beta over gRPC, no name: module stays empty.
    let mut modules = ModulesClient::new(s.channel().await);
    let r = modules
        .upload(pb::UploadModuleRequest {
            sha256: sha_hex(&wasm),
            wasm: wasm.clone(),
            language: None,
            tenant: Some("beta".into()),
            name: None,
        })
        .await
        .expect("grpc upload")
        .into_inner();
    assert!(!r.created);
    assert_eq!(version(&s, "beta/sdk-basic", 1).expect("beta").module, "");
    // gRPC grammar errors: tenant `_`, bad names.
    for (tenant, name) in [
        (Some("_"), None),
        (Some("Beta"), None),
        (None, Some("a/b")),
        (None, Some("x:y")),
        (None, Some("__z")),
    ] {
        let e = modules
            .upload(pb::UploadModuleRequest {
                sha256: sha_hex(&wasm),
                wasm: wasm.clone(),
                language: None,
                tenant: tenant.map(str::to_owned),
                name: name.map(str::to_owned),
            })
            .await
            .expect_err("invalid");
        assert_eq!(
            e.code(),
            Code::InvalidArgument,
            "{tenant:?} {name:?}: {e:?}"
        );
    }

    // Survives a restart.
    s.restart().await;
    s.wait_ready(Duration::from_secs(5)).await;
    assert_eq!(
        version(&s, "acme/sdk-basic", 1).expect("acme").module,
        "acme/mod-a"
    );
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_tenants_same_workflow_and_key_never_collide() {
    let mut s = server().await;
    let wasm = guest_wasm("sdk-basic");
    for q in ["", "?tenant=acme&name=m", "?tenant=beta&name=m"] {
        let (st, body) = put_module(&s, &wasm, q).await;
        assert!(st.is_success(), "{q}: {body}");
    }
    let input = json!({"name": "t", "n": 1});

    let mut ids = Vec::new();
    for tenant in [Some("acme"), Some("beta"), None] {
        let mut body = json!({"input": input, "idempotency_key": "k"});
        if let Some(t) = tenant {
            body["tenant"] = json!(t);
        }
        let (st, res) = start(&s, "sdk-basic", body).await;
        assert_eq!(st, StatusCode::CREATED, "{tenant:?}: {res}");
        assert_eq!(res["created"], true);
        ids.push(res["run_id"].as_str().expect("run_id").to_owned());
    }
    let [acme, beta, default] = [&ids[0], &ids[1], &ids[2]];
    assert_ne!(acme, beta);
    assert_ne!(acme, default);
    assert_ne!(beta, default);

    // The second start of each tenant with `k` returns that tenant's run.
    for (tenant, want) in [(Some("acme"), acme), (Some("beta"), beta), (None, default)] {
        let mut body = json!({"input": input, "idempotency_key": "k"});
        if let Some(t) = tenant {
            body["tenant"] = json!(t);
        }
        let (st, res) = start(&s, "sdk-basic", body).await;
        assert_eq!(st, StatusCode::OK, "{tenant:?}: {res}");
        assert_eq!(res["created"], false);
        assert_eq!(res["run_id"].as_str(), Some(want.as_str()), "{tenant:?}");
    }
    // An already-qualified workflow with the matching tenant is the same key.
    let mut runs = RunsClient::new(s.channel().await);
    let r = runs
        .start(pb::StartRunRequest {
            workflow: "acme/sdk-basic".into(),
            input_json: br#"{"name":"t","n":1}"#.to_vec(),
            tenant: Some("acme".into()),
            idempotency_key: Some("k".into()),
            ..Default::default()
        })
        .await
        .expect("grpc start")
        .into_inner();
    assert!(!r.created);
    assert_eq!(&r.run_id, acme);

    // Records carry the storage form; the tenant is a separate field.
    for (id, tenant, workflow, queue) in [
        (acme, json!("acme"), "acme/sdk-basic", "acme/default"),
        (beta, json!("beta"), "beta/sdk-basic", "beta/default"),
        (default, Value::Null, "sdk-basic", "default"),
    ] {
        let run = get(&s, id).await;
        assert_eq!(run["workflow"], workflow, "{run}");
        assert_eq!(run["queue"], queue, "{run}");
        assert_eq!(run["tenant"], tenant, "{run}");
    }

    // Three distinct dedupe rows, each on the partition of its own run.
    for (id, workflow) in [
        (acme, "acme/sdk-basic"),
        (beta, "beta/sdk-basic"),
        (default, "sdk-basic"),
    ] {
        let run: RunId = id.parse().expect("run id");
        let row = s
            .handle_of(&run)
            .reader()
            .get(Table::Dedupe, &dedupe_key(workflow, b"k"))
            .expect("read dedupe");
        assert!(row.is_some(), "dedupe row of {workflow}");
    }

    // An explicit queue is qualified too.
    let (st, res) = start(
        &s,
        "sdk-basic",
        json!({"input": input, "tenant": "acme", "queue": "q1"}),
    )
    .await;
    assert_eq!(st, StatusCode::CREATED, "{res}");
    let run = get(&s, res["run_id"].as_str().expect("id")).await;
    assert_eq!(run["queue"], "acme/q1", "{run}");

    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn start_rejects_bad_names_and_tenants() {
    let mut s = server().await;
    let wasm = guest_wasm("sdk-basic");
    let (st, body) = put_module(&s, &wasm, "").await;
    assert_eq!(st, StatusCode::CREATED, "{body}");
    let input = json!({"name": "t", "n": 1});

    // REST: the tenant `_` and bad tenant ids; bad queue names (the workflow is a path
    // segment, so `a/b` cannot reach it over REST: see gRPC below).
    for (body, field) in [
        (json!({"input": input, "tenant": "_"}), "tenant"),
        (json!({"input": input, "tenant": "Acme"}), "tenant"),
        (json!({"input": input, "tenant": "a:b"}), "tenant"),
        (json!({"input": input, "queue": "a/b"}), "queue"),
        (json!({"input": input, "queue": "x:y"}), "queue"),
        (json!({"input": input, "queue": "__z"}), "queue"),
        // A queue qualified with another tenant.
        (
            json!({"input": input, "tenant": "acme", "queue": "beta/q"}),
            "queue",
        ),
    ] {
        let (st, res) = start(&s, "sdk-basic", body.clone()).await;
        assert_400(st, &res, field);
    }
    for workflow in ["x:y", "__z"] {
        let (st, res) = start(&s, workflow, json!({"input": input})).await;
        assert_400(st, &res, "workflow");
    }

    // gRPC: bad workflow names (incl. one qualified with a tenant other than the
    // request's), the tenant `_`.
    let mut runs = RunsClient::new(s.channel().await);
    for (workflow, tenant) in [
        ("a/b", None),
        ("x:y", None),
        ("__z", None),
        ("acme/sdk-basic", None),
        ("acme/sdk-basic", Some("beta")),
        ("sdk-basic", Some("_")),
    ] {
        let e = runs
            .start(pb::StartRunRequest {
                workflow: workflow.into(),
                input_json: br#"{"name":"t","n":1}"#.to_vec(),
                tenant: tenant.map(str::to_owned),
                ..Default::default()
            })
            .await
            .expect_err("invalid");
        assert_eq!(
            e.code(),
            Code::InvalidArgument,
            "{workflow} {tenant:?}: {e:?}"
        );
    }

    // A valid tenant whose workflow was never uploaded in that tenant: 404 (no fallback
    // to the default tenant's workflow).
    let (st, res) = start(&s, "sdk-basic", json!({"input": input, "tenant": "acme"})).await;
    assert_eq!(st, StatusCode::NOT_FOUND, "{res}");

    // Default-tenant behaviour is unchanged: an empty tenant is the default tenant, and the
    // run completes as before.
    let (st, res) = start(&s, "sdk-basic", json!({"input": input, "tenant": ""})).await;
    assert_eq!(st, StatusCode::CREATED, "{res}");
    let id = res["run_id"].as_str().expect("id").to_owned();
    let t0 = std::time::Instant::now();
    let run = loop {
        let run = get(&s, &id).await;
        if run["status"] == "completed" {
            break run;
        }
        assert!(
            t0.elapsed() < Duration::from_secs(30),
            "not completed: {run}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(run["workflow"], "sdk-basic");
    assert_eq!(run["queue"], "default");
    assert_eq!(run["tenant"], Value::Null);

    s.shutdown().await;
}
