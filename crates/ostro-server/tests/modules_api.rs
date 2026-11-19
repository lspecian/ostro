//! API-02 against the real server (plan 04-17): module upload by sha256 (verify, store,
//! validate + AOT, register workflows), idempotent re-upload without recompiling,
//! immutable (name, version) bindings, first-version auto-activation, CAS activation,
//! over REST and gRPC on the same port, and metadata persistence across a restart.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::time::Duration;

use common::{ServerOpts, TestServer, guest_wasm};
use ostro_meta::{ApiToken, MetaCommand, MetaKey, MetaRecord, MetaResult};
use ostro_proto::api::v1 as pb;
use ostro_proto::api::v1::{modules_client::ModulesClient, workflows_client::WorkflowsClient};
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

/// `wasm` plus a trailing custom section: a different sha, the same `describe()`.
fn with_custom_section(wasm: &[u8]) -> Vec<u8> {
    let name = b"ostro-test";
    let data = b"different module, same workflows";
    let mut body = vec![u8::try_from(name.len()).expect("len")];
    body.extend_from_slice(name);
    body.extend_from_slice(data);
    let mut out = wasm.to_vec();
    out.push(0); // custom section id
    out.push(u8::try_from(body.len()).expect("short section")); // LEB128 (< 128)
    out.extend_from_slice(&body);
    out
}

async fn server() -> TestServer {
    let s = TestServer::start(ServerOpts::default()).await;
    s.wait_ready(Duration::from_secs(5)).await;
    s
}

// ---------------------------------------------------------------------------------
// REST
// ---------------------------------------------------------------------------------

async fn put_module(s: &TestServer, sha: &str, body: Vec<u8>) -> (StatusCode, Value) {
    let r = s
        .http
        .put(s.url(&format!("/v1/modules/{sha}?language=rust")))
        .header("content-type", "application/wasm")
        .body(body)
        .send()
        .await
        .expect("PUT module");
    let st = r.status();
    (st, r.json().await.expect("json body"))
}

async fn workflows(s: &TestServer) -> Value {
    let r = s
        .http
        .get(s.url("/v1/workflows"))
        .send()
        .await
        .expect("GET workflows");
    assert_eq!(r.status(), StatusCode::OK);
    r.json().await.expect("json")
}

fn workflow<'a>(list: &'a Value, name: &str) -> &'a Value {
    list["workflows"]
        .as_array()
        .expect("array")
        .iter()
        .find(|w| w["name"] == name)
        .unwrap_or_else(|| panic!("workflow {name} missing in {list}"))
}

async fn activate(s: &TestServer, name: &str, body: Value) -> (StatusCode, Value) {
    let r = s
        .http
        .put(s.url(&format!("/v1/workflows/{name}/active")))
        .json(&body)
        .send()
        .await
        .expect("PUT active");
    let st = r.status();
    (st, r.json().await.expect("json"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rest_modules_and_workflows() {
    let mut s = server().await;
    let v1 = guest_wasm("sdk-basic");
    let v1_sha = sha_hex(&v1);

    // Upload: 201, workflows from describe(), first version auto-activates.
    let (st, body) = put_module(&s, &v1_sha, v1.clone()).await;
    assert_eq!(st, StatusCode::CREATED, "{body}");
    assert_eq!(body["created"], true);
    assert_eq!(body["module"]["sha256"], v1_sha);
    assert_eq!(body["module"]["size_bytes"], v1.len());
    assert_eq!(body["module"]["language"], "rust");
    assert_eq!(
        body["module"]["workflows"],
        json!([{"name": "sdk-basic", "version": 1}, {"name": "sdk-echo", "version": 1}])
    );
    let list = workflows(&s).await;
    let basic = workflow(&list, "sdk-basic");
    assert_eq!(basic["active_version"], 1);
    assert_eq!(basic["versions"][0]["module_sha256"], v1_sha);
    assert_eq!(workflow(&list, "sdk-echo")["active_version"], 1);
    let r = s
        .http
        .get(s.url(&format!("/v1/modules/{v1_sha}")))
        .send()
        .await
        .expect("GET module");
    assert_eq!(r.status(), StatusCode::OK);

    // The module is in the node's blob store and prepared (warm L2).
    let sha: [u8; 32] = Sha256::digest(&v1).into();
    assert!(s.h().runtime().prepared(&sha).is_some());
    let blob = s
        .h()
        .control()
        .blobs()
        .expect("blobs")
        .get(&ostro_blob::BlobRef {
            sha256: sha,
            len: v1.len() as u64,
        })
        .expect("blob stored");
    assert_eq!(blob.len(), v1.len());

    // Re-upload: 200, created=false, no recompile, nothing new in L2.
    let stats = s.h().runtime().cache_stats().expect("aot cache");
    let l2 = s.h().runtime().instance_pre_len();
    let (st, body) = put_module(&s, &v1_sha, v1.clone()).await;
    assert_eq!(st, StatusCode::OK, "{body}");
    assert_eq!(body["created"], false);
    assert_eq!(s.h().runtime().cache_stats().expect("aot cache"), stats);
    assert_eq!(s.h().runtime().instance_pre_len(), l2);

    // Wrong sha in the path -> 400; a 20 MB body with a wrong sha -> 400 (not 413).
    let (st, body) = put_module(&s, &sha_hex(b"other"), v1.clone()).await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["details"]["actual_sha256"], v1_sha);
    let (st, body) = put_module(&s, &"ab".repeat(16).repeat(2), vec![7u8; 20 << 20]).await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{body}");
    let (st, _) = put_module(&s, "not-hex", v1.clone()).await;
    assert_eq!(st, StatusCode::BAD_REQUEST);

    // Non-component bytes with the right sha -> 400 with the validation message.
    let junk = b"definitely not a wasm component".to_vec();
    let (st, body) = put_module(&s, &sha_hex(&junk), junk.clone()).await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["code"], "invalid_argument");
    let msg = body["error"]["message"].as_str().expect("message");
    assert!(msg.contains("component"), "{msg}");
    let r = s
        .http
        .get(s.url(&format!("/v1/modules/{}", sha_hex(&junk))))
        .send()
        .await
        .expect("GET");
    assert_eq!(r.status(), StatusCode::NOT_FOUND);
    // A component preamble followed by garbage fails in the compiler: still 400.
    let mut bad = vec![0x00, 0x61, 0x73, 0x6d, 0x0d, 0x00, 0x01, 0x00];
    bad.extend_from_slice(b"\xff\xffgarbage section");
    let (st, body) = put_module(&s, &sha_hex(&bad), bad).await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{body}");
    assert!(
        body["error"]["message"]
            .as_str()
            .expect("message")
            .starts_with("invalid Wasm component"),
        "{body}"
    );
    // A core module (not a component) -> 400 with the engine's message.
    let core = vec![0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00];
    let (st, body) = put_module(&s, &sha_hex(&core), core).await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{body}");
    assert!(
        body["error"]["message"]
            .as_str()
            .expect("message")
            .contains("not a component"),
        "{body}"
    );

    // A different module describing (sdk-basic, 1) -> 409 naming the existing sha.
    let dup = with_custom_section(&v1);
    let dup_sha = sha_hex(&dup);
    let (st, body) = put_module(&s, &dup_sha, dup).await;
    assert_eq!(st, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["details"]["existing_module_sha256"], v1_sha);
    assert!(
        body["error"]["message"]
            .as_str()
            .expect("message")
            .contains(&v1_sha)
    );
    let r = s
        .http
        .get(s.url(&format!("/v1/modules/{dup_sha}")))
        .send()
        .await
        .expect("GET");
    assert_eq!(r.status(), StatusCode::NOT_FOUND, "nothing recorded");

    // v2: 201, active stays 1.
    let v2 = guest_wasm("sdk-basic-v2");
    let v2_sha = sha_hex(&v2);
    let (st, body) = put_module(&s, &v2_sha, v2).await;
    assert_eq!(st, StatusCode::CREATED, "{body}");
    assert_eq!(
        body["module"]["workflows"],
        json!([{"name": "sdk-basic", "version": 2}])
    );
    let list = workflows(&s).await;
    let basic = workflow(&list, "sdk-basic");
    assert_eq!(basic["active_version"], 1);
    assert_eq!(basic["versions"].as_array().expect("versions").len(), 2);
    let rv = basic["record_version"].as_u64().expect("record_version");

    // CAS activation: stale -> 412, current -> 200, omitted -> 200, unknown version -> 404.
    let (st, body) = activate(
        &s,
        "sdk-basic",
        json!({"version": 2, "expected_version": rv - 1}),
    )
    .await;
    assert_eq!(st, StatusCode::PRECONDITION_FAILED, "{body}");
    assert_eq!(body["error"]["details"]["record_version"], rv);
    let (st, body) = activate(
        &s,
        "sdk-basic",
        json!({"version": 2, "expected_version": rv}),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "{body}");
    assert_eq!(body["active_version"], 2);
    assert_eq!(body["record_version"], rv + 1);
    let (st, body) = activate(&s, "sdk-basic", json!({"version": 1})).await;
    assert_eq!(st, StatusCode::OK, "{body}");
    assert_eq!(body["active_version"], 1);
    let (st, body) = activate(&s, "sdk-basic", json!({"version": 2})).await;
    assert_eq!(st, StatusCode::OK, "{body}");
    assert_eq!(body["active_version"], 2);
    let (st, _) = activate(&s, "sdk-basic", json!({"version": 9})).await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    let (st, _) = activate(&s, "no-such-workflow", json!({"version": 1})).await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    assert_eq!(
        workflow(&workflows(&s).await, "sdk-basic")["active_version"],
        2
    );

    // Runs are 04-18's: the placeholder answers 503.
    let r = s
        .http
        .post(s.url("/v1/workflows/sdk-basic/runs"))
        .json(&json!({"input": {}}))
        .send()
        .await
        .expect("POST run");
    assert_eq!(r.status(), StatusCode::SERVICE_UNAVAILABLE);

    s.shutdown().await;
}

// ---------------------------------------------------------------------------------
// gRPC (same scenario, same port)
// ---------------------------------------------------------------------------------

fn upload_req(sha: &str, wasm: Vec<u8>) -> pb::UploadModuleRequest {
    pb::UploadModuleRequest {
        sha256: sha.to_string(),
        wasm,
        language: Some("rust".into()),
    }
}

fn refs(m: &pb::ModuleInfo) -> Vec<(String, u32)> {
    m.workflows
        .iter()
        .map(|w| (w.name.clone(), w.version))
        .collect()
}

async fn grpc_workflow(
    c: &mut WorkflowsClient<tonic::transport::Channel>,
    name: &str,
) -> pb::WorkflowInfo {
    c.list(pb::ListWorkflowsRequest {})
        .await
        .expect("list")
        .into_inner()
        .workflows
        .into_iter()
        .find(|w| w.name == name)
        .unwrap_or_else(|| panic!("workflow {name} missing"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn grpc_modules_and_workflows() {
    let mut s = server().await;
    let ch = s.channel().await;
    let mut modules = ModulesClient::new(ch.clone()).max_encoding_message_size(64 << 20);
    let mut wf = WorkflowsClient::new(ch);
    let v1 = guest_wasm("sdk-basic");
    let v1_sha = sha_hex(&v1);

    let r = modules
        .upload(upload_req(&v1_sha, v1.clone()))
        .await
        .expect("upload")
        .into_inner();
    assert!(r.created);
    let m = r.module.expect("module");
    assert_eq!(m.sha256, v1_sha);
    assert_eq!(
        refs(&m),
        vec![("sdk-basic".to_string(), 1), ("sdk-echo".to_string(), 1)]
    );
    assert_eq!(
        grpc_workflow(&mut wf, "sdk-basic").await.active_version,
        Some(1)
    );

    let stats = s.h().runtime().cache_stats().expect("aot cache");
    let r = modules
        .upload(upload_req(&v1_sha, v1.clone()))
        .await
        .expect("re-upload")
        .into_inner();
    assert!(!r.created);
    assert_eq!(s.h().runtime().cache_stats().expect("aot cache"), stats);

    let e = modules
        .upload(upload_req(&sha_hex(b"x"), v1.clone()))
        .await
        .expect_err("wrong sha");
    assert_eq!(e.code(), Code::InvalidArgument);
    let e = modules
        .upload(upload_req(&sha_hex(b"x"), vec![1u8; 20 << 20]))
        .await
        .expect_err("20 MB wrong sha");
    assert_eq!(e.code(), Code::InvalidArgument, "{e:?}");
    let junk = b"\0asm junk".to_vec();
    let e = modules
        .upload(upload_req(&sha_hex(&junk), junk))
        .await
        .expect_err("junk");
    assert_eq!(e.code(), Code::InvalidArgument);

    let dup = with_custom_section(&v1);
    let e = modules
        .upload(upload_req(&sha_hex(&dup), dup.clone()))
        .await
        .expect_err("dup binding");
    assert_eq!(e.code(), Code::AlreadyExists, "{e:?}");
    let details = ostro_api::grpc::status_details(&e).expect("details");
    assert_eq!(details["existing_module_sha256"], v1_sha);
    let e = modules
        .get(pb::GetModuleRequest {
            sha256: sha_hex(&dup),
        })
        .await
        .expect_err("not recorded");
    assert_eq!(e.code(), Code::NotFound);

    let v2 = guest_wasm("sdk-basic-v2");
    let r = modules
        .upload(upload_req(&sha_hex(&v2), v2))
        .await
        .expect("v2")
        .into_inner();
    assert!(r.created);
    let basic = grpc_workflow(&mut wf, "sdk-basic").await;
    assert_eq!(basic.active_version, Some(1));
    let rv = basic.record_version;

    let e = wf
        .activate(pb::ActivateWorkflowRequest {
            name: "sdk-basic".into(),
            version: 2,
            expected_version: Some(rv - 1),
        })
        .await
        .expect_err("stale");
    assert_eq!(e.code(), Code::FailedPrecondition);
    let w = wf
        .activate(pb::ActivateWorkflowRequest {
            name: "sdk-basic".into(),
            version: 2,
            expected_version: Some(rv),
        })
        .await
        .expect("activate")
        .into_inner();
    assert_eq!(w.active_version, Some(2));
    let w = wf
        .activate(pb::ActivateWorkflowRequest {
            name: "sdk-basic".into(),
            version: 1,
            expected_version: None,
        })
        .await
        .expect("activate omitted")
        .into_inner();
    assert_eq!(w.active_version, Some(1));
    let e = wf
        .activate(pb::ActivateWorkflowRequest {
            name: "sdk-basic".into(),
            version: 7,
            expected_version: None,
        })
        .await
        .expect_err("unknown version");
    assert_eq!(e.code(), Code::NotFound);

    s.shutdown().await;
}

// ---------------------------------------------------------------------------------
// Persistence
// ---------------------------------------------------------------------------------

type Snapshot = (
    Vec<(String, u64)>,
    Vec<(String, u32, u64)>,
    Vec<(String, u32, u64)>,
    Option<u64>,
);

fn snapshot(s: &TestServer) -> Snapshot {
    s.h().meta().view().read(|m| {
        (
            m.modules()
                .map(|r| (ostro_blob::hex(&r.value.sha256), r.version))
                .collect(),
            m.workflows()
                .map(|r| (r.value.name.clone(), r.value.version, r.version))
                .collect(),
            ["sdk-basic", "sdk-echo"]
                .iter()
                .filter_map(|n| m.active(n))
                .map(|r| (r.value.name.clone(), r.value.version, r.version))
                .collect(),
            m.token("tok-1").map(|t| t.version),
        )
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metadata_survives_restart() {
    let mut s = server().await;
    let v1 = guest_wasm("sdk-basic");
    let v2 = guest_wasm("sdk-basic-v2");
    assert_eq!(
        put_module(&s, &sha_hex(&v1), v1.clone()).await.0,
        StatusCode::CREATED
    );
    assert_eq!(
        put_module(&s, &sha_hex(&v2), v2).await.0,
        StatusCode::CREATED
    );
    assert_eq!(
        activate(&s, "sdk-basic", json!({"version": 2})).await.0,
        StatusCode::OK
    );
    let res = s
        .h()
        .meta()
        .propose(MetaCommand::Put {
            key: MetaKey::Token("tok-1".into()),
            expected_version: None,
            record: MetaRecord::Token(ApiToken {
                id: "tok-1".into(),
                hash: [3; 32],
                scopes: vec!["runs:write".into()],
                tenant: None,
                created_at_ms: 1,
            }),
        })
        .await
        .expect("token put");
    assert_eq!(res, MetaResult::Ok { version: 1 });

    let before = snapshot(&s);
    assert_eq!(before.0.len(), 2);
    assert_eq!(before.1.len(), 3);
    assert_eq!(before.2.len(), 2);
    assert_eq!(before.3, Some(1));
    let list_before = workflows(&s).await;

    s.restart().await;
    s.wait_ready(Duration::from_secs(5)).await;
    assert_eq!(snapshot(&s), before, "records and versions unchanged");
    assert_eq!(workflows(&s).await, list_before);
    assert_eq!(
        workflow(&workflows(&s).await, "sdk-basic")["active_version"],
        2
    );
    // Re-upload after restart is still a no-op.
    let (st, body) = put_module(&s, &sha_hex(&v1), v1).await;
    assert_eq!(st, StatusCode::OK, "{body}");
    assert_eq!(body["created"], false);
    s.shutdown().await;
}
