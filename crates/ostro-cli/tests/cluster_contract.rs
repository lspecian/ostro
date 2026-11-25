//! 13-16: the `ostro_client::cluster` contract — every D-P13-23 method of the typed client
//! against an in-process `ostro_api::router_with` with the 13-03 fakes (`FakeCluster`,
//! `FakeModuleLifecycle`), decoding into the client's types, plus the Phase 10 error mapping
//! (`ClientError{status, code, reason}`).
//!
//! The plan names `crates/ostro-client/tests/cluster_contract.rs`; ostro-client has no
//! `ostro-api` dev-dependency and Cargo manifests are 13-01-only, so the contract runs here,
//! where `ostro-api` (feature `fake`) already is a dev-dependency (13-16 SUMMARY).

use std::sync::Arc;
use std::time::Duration;

use ostro_api::fake::{FakeCluster, FakeControlPlane, FakeMigrateOutcome, FakeModuleLifecycle};
use ostro_api::{ApiConfig, ApiError, ApiExtensions, ControlPlane};
use ostro_client::TypedClient;
use ostro_client::cluster::{
    InitRequest, MigrateRunsRequest, NodeTokenRequest, ReassignRequest, VersionOrSha,
};
use serde_json::json;

struct Fx {
    c: TypedClient,
    cluster: Arc<FakeCluster>,
    modules: Arc<FakeModuleLifecycle>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Fx {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn fx() -> Fx {
    let cp: Arc<dyn ControlPlane> = Arc::new(FakeControlPlane::new());
    let cluster = FakeCluster::new();
    let modules = FakeModuleLifecycle::new();
    let mut ext = ApiExtensions::default();
    cluster.install(&mut ext);
    modules.install(&mut ext);
    let app = ostro_api::router_with(cp, ApiConfig::default(), ext);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    let c = TypedClient::new(&format!("http://{addr}"), Duration::from_secs(10)).expect("client");
    Fx {
        c,
        cluster,
        modules,
        task,
    }
}

#[tokio::test]
async fn cluster_methods_round_trip() {
    let f = fx().await;
    // Before init: status works, the rest is 412.
    let st = f.c.cluster_status().await.expect("status");
    assert_eq!(st.members.len(), 1);
    assert!(st.policy.is_none());
    let e =
        f.c.create_node_token(&NodeTokenRequest::default())
            .await
            .expect_err("token before init");
    assert_eq!(
        (e.status, e.code.as_str()),
        (Some(412), "precondition_failed")
    );

    let st =
        f.c.cluster_init(&InitRequest {
            advertise: Some("10.0.0.1:7701".into()),
            rf: Some(3),
            min_insync: None,
        })
        .await
        .expect("init");
    assert_eq!(st.voters, vec![1]);
    let p = st.policy.expect("policy");
    assert_eq!((p.rf, p.min_insync, p.max_voters), (3, 2, 5));
    assert_eq!(st.members[0].cluster_addr.as_deref(), Some("10.0.0.1:7701"));
    // Idempotent.
    let again =
        f.c.cluster_init(&InitRequest::default())
            .await
            .expect("re-init");
    assert_eq!(again.policy.expect("policy").rf, 3);

    let t =
        f.c.create_node_token(&NodeTokenRequest { ttl_secs: Some(60) })
            .await
            .expect("token");
    assert!(
        t.id.starts_with("njt_") && t.token.starts_with(&t.id),
        "{t:?}"
    );
    assert!(t.ca_hash.starts_with("sha256:"));
    let joined =
        f.c.raw()
            .cluster_join(&json!({
                "token": t.token,
                "csr_pem": "csr",
                "advertise": { "api": "10.0.0.2:7700", "cluster": "10.0.0.2:7701" },
                "node_version": {
                    "ostro_version": "0.1.0", "level_min": 1, "level_max": 2,
                    "wasmtime": "50.0.2", "event_codec_ver": 1, "wit_lines": ["0.1"]
                },
            }))
            .await
            .expect("join");
    assert_eq!(joined.body["node_id"], json!(2));
    f.cluster.promote(2);

    let st = f.c.cluster_drain(2).await.expect("drain");
    assert_eq!(st.members[1].state, "draining");
    let st =
        f.c.cluster_reassign(&ReassignRequest {
            partition: 3,
            replicas: vec![1, 2],
            cancel: false,
        })
        .await
        .expect("reassign");
    assert_eq!(st.reassignments[0].partition, 3);
    assert_eq!(st.reassignments[0].target, vec![1, 2]);
    let e =
        f.c.cluster_reassign(&ReassignRequest {
            partition: 3,
            replicas: vec![2, 1],
            cancel: false,
        })
        .await
        .expect_err("in flight");
    assert_eq!(
        (e.status, e.reason()),
        (Some(409), Some("reassignment_in_progress"))
    );
    let st =
        f.c.cluster_reassign(&ReassignRequest {
            partition: 3,
            replicas: vec![],
            cancel: true,
        })
        .await
        .expect("cancel");
    assert!(st.reassignments.is_empty());
    f.c.cluster_rebalance().await.expect("rebalance");

    let u = f.c.upgrade_status().await.expect("upgrade status");
    assert_eq!((u.finalized, u.common_level_max), (1, 2));
    let u = f.c.upgrade_hold().await.expect("hold");
    assert_eq!(u.hold, Some(1));
    let e = f.c.upgrade_finalize().await.expect_err("held");
    assert_eq!((e.status, e.reason()), (Some(409), Some("held")));
    f.c.upgrade_release().await.expect("release");
    let u = f.c.upgrade_finalize().await.expect("finalize");
    assert_eq!(u.finalized, 2);

    let st = f.c.cluster_remove(2, true).await.expect("remove");
    assert_eq!(st.members[1].state, "removed");
    let e = f.c.cluster_remove(1, false).await.expect_err("last voter");
    assert_eq!((e.status, e.reason()), (Some(409), Some("quorum")));
    let e = f.c.cluster_drain(9).await.expect_err("unknown");
    assert_eq!((e.status, e.code.as_str()), (Some(404), "not_found"));

    // Error mapping of an injected 503 not_leader.
    f.cluster.fail(
        "status",
        Some(
            ApiError::unavailable("not the meta leader")
                .with_details(json!({ "reason": "not_leader" })),
        ),
    );
    let e = f.c.cluster_status().await.expect_err("503");
    assert_eq!(
        (e.status, e.code.as_str(), e.reason()),
        (Some(503), "unavailable", Some("not_leader"))
    );
}

#[tokio::test]
async fn module_lifecycle_methods_round_trip() {
    let f = fx().await;
    let a = "aa".repeat(32);
    let b = "bb".repeat(32);
    f.modules.add_module(&a, &[("order", 1)], false);
    f.modules.add_module(&b, &[("order", 2)], true);
    let r = f.c.retire_module(&a).await.expect("retire");
    assert_eq!(r.sha256, a);
    assert_eq!(r.versions[0].name, "order");
    let again = f.c.retire_module(&a).await.expect("idempotent");
    assert_eq!(again.retired_at_ms, r.retired_at_ms);
    let e = f.c.retire_module(&b).await.expect_err("active");
    assert_eq!((e.status, e.reason()), (Some(409), Some("module_active")));
    let g = f.c.run_module_gc().await.expect("gc");
    assert_eq!(g.deleted, vec![a.clone()]);
    assert!(!g.postponed);

    f.modules.add_run("r1", "order", FakeMigrateOutcome::Pass);
    f.modules.add_run(
        "r2",
        "order",
        FakeMigrateOutcome::Fail {
            seq: 4,
            detail: "diverged".into(),
        },
    );
    let m =
        f.c.migrate_runs(&MigrateRunsRequest {
            workflow: "order".into(),
            from: VersionOrSha::Version(1),
            to_version: 2,
            filter: Some("eu".into()),
            limit: None,
            dry_run: true,
        })
        .await
        .expect("migrate");
    assert_eq!(m.passed, vec!["r1"]);
    assert!(m.repinned.is_empty(), "dry run");
    assert_eq!((m.failed[0].run_id.as_str(), m.failed[0].seq), ("r2", 4));
    let got = f.modules.migrations();
    assert_eq!(got[0].filter.as_deref(), Some("eu"));
    assert_eq!(got[0].from, ostro_api::phase13::VersionOrSha::Version(1));
}
