//! Phase 14 seam skeleton (14-01b): the seam links, the default principal is Anonymous, the
//! trigger classification, the passthrough stubs and the Kafka trigger stub.

use std::sync::Arc;

use ostro_api::auth::{self, Principal, PrincipalKind};
use ostro_meta::MetaHandle;
use ostro_partition::clock::ManualClock;
use ostro_partition::{Clock, PartitionRoles};
use ostro_server::config::{AuthMode, RawTable, ServerConfig};
use ostro_server::p14::{self, IngressKind, P14Init, RegisterDecision, TriggerKind};
use ostro_types::{Epoch, PartitionId};
use tokio::sync::watch;

struct NoRoles(watch::Sender<Arc<Vec<(PartitionId, Epoch)>>>);

impl PartitionRoles for NoRoles {
    fn led(&self) -> watch::Receiver<Arc<Vec<(PartitionId, Epoch)>>> {
        self.0.subscribe()
    }
}

fn roles() -> Arc<dyn PartitionRoles> {
    Arc::new(NoRoles(watch::channel(Arc::new(Vec::new())).0))
}

async fn ctx(cfg: ServerConfig, dir: &tempfile::TempDir) -> Arc<p14::P14Ctx> {
    let meta = Arc::new(MetaHandle::start(dir.path(), 1).await.expect("meta"));
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(1_000));
    p14::build_ctx(P14Init {
        cfg: Arc::new(cfg),
        meta,
        clock: p14::wrap_clock(clock),
        node_id: 1,
        data_dir: dir.path().to_path_buf(),
        level_max: ostro_types::compat::level_max(),
    })
    .expect("build_ctx")
}

#[test]
fn current_principal_outside_any_scope_is_anonymous() {
    let p = auth::current();
    assert!(p.is_anonymous());
    assert_eq!(p.kind, PrincipalKind::Anonymous);
}

#[tokio::test]
async fn scoped_principal_is_visible_and_ends_with_the_scope() {
    let inner = auth::scope(Principal::dev(), async { auth::current() }).await;
    assert_eq!(inner.kind, PrincipalKind::Dev);
    assert!(auth::current().is_anonymous());
}

#[test]
fn trigger_kind_comes_from_the_idempotency_key_prefix() {
    assert_eq!(
        TriggerKind::of_key(Some("__kafka/x/t/0/1")),
        Some(TriggerKind::Kafka)
    );
    assert_eq!(
        TriggerKind::of_key(Some("__hook/github/delivery-1")),
        Some(TriggerKind::Webhook)
    );
    assert_eq!(
        TriggerKind::of_key(Some("__cron/nightly/manual/00ff")),
        Some(TriggerKind::Cron)
    );
    assert_eq!(TriggerKind::of_key(Some("order-42")), None);
    assert_eq!(TriggerKind::of_key(Some("")), None);
    assert_eq!(TriggerKind::of_key(None), None);
    assert_eq!(
        ostro_server::control::webhooks::HOOK_KEY_PREFIX,
        "__hook/",
        "the webhook marker the classification reads"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seam_links_and_every_stub_passes_through() {
    let dir = tempfile::tempdir().expect("tempdir");
    let c = ctx(ServerConfig::server_defaults(), &dir).await;
    assert_eq!(c.node_id, 1);
    assert_eq!(c.clock.now_ms(), 1_000, "wrap_clock is the identity");
    assert!(c.blobs().is_none() && c.roles().is_none());
    c.attach_node(None, roles());
    assert!(c.roles().is_some());

    // Gates allow, evidence is empty, registration proceeds, ingress admits.
    let sha = [7u8; 32];
    let ev = p14::upload_gate(&c, Some("acme"), sha, b"\0asm", &[])
        .await
        .expect("upload gate");
    assert!(ev.is_empty());
    let mut ext = Vec::new();
    p14::evidence_ext(&ev, &mut ext);
    assert!(ext.is_empty());
    p14::merge_evidence(&c, sha, ev).await.expect("merge");
    p14::version_gate(&c, None, "wf", 2, sha)
        .await
        .expect("version gate");
    p14::activation_gate(&c, None, sha)
        .await
        .expect("activation gate");
    assert_eq!(
        p14::register_existing(&c, sha, Some("acme")).await,
        RegisterDecision::Proceed
    );
    for kind in [
        IngressKind::Start {
            dedupe_hit: false,
            trigger: TriggerKind::of_key(Some("__kafka/k1/t/0/7")),
        },
        IngressKind::Signal,
        IngressKind::TokenComplete,
    ] {
        p14::admit_ingress(&c, kind, Some("acme"))
            .await
            .expect("admit");
    }
    p14::worker_event(
        &c,
        p14::WorkerAuditEvent {
            action: p14::WorkerAction::Join,
            token_id: "wjt_x".into(),
            worker_id: Some("w1".into()),
            outcome: ostro_types::audit::AuditOutcome::Ok,
        },
    );
    // The audit sink is the no-op sink until 14-04: a permit is always available.
    assert!(p14::audit_sink(&c).reserve().is_ok());
    // Services start and stop; the shutdown hook runs.
    let services = p14::start(Arc::clone(&c)).await.expect("start");
    p14::before_shutdown(&c).await;
    services.stop().await;
    assert!(c.shutdown.is_cancelled());

    // 14-03: token mode (the server profile) authenticates against meta: no bearer → 401.
    let res = p14::authenticator(&c).authenticate(None).await;
    assert_eq!(res, Err(ostro_api::auth::AuthError::Missing));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_triggers_is_none_without_kafka_and_refuses_a_kafka_list() {
    let dir = tempfile::tempdir().expect("tempdir");
    let c = ctx(ServerConfig::server_defaults(), &dir).await;
    let routed: Arc<dyn ostro_api::ControlPlane> =
        Arc::new(ostro_api::fake::FakeControlPlane::default());
    let got = p14::start_triggers(&c, Arc::clone(&routed), roles())
        .await
        .expect("empty list");
    assert!(got.is_none());

    let dir2 = tempfile::tempdir().expect("tempdir");
    let mut cfg = ServerConfig::server_defaults();
    let mut table = config::Map::new();
    table.insert("name".to_owned(), config::Value::from("orders"));
    cfg.triggers
        .kafka
        .push(RawTable(config::Value::from(table)));
    let c2 = ctx(cfg, &dir2).await;
    let err = p14::start_triggers(&c2, routed, roles())
        .await
        .expect_err("a Kafka list before 14-32");
    assert!(
        err.to_string().contains("Kafka triggers land in 14-32"),
        "{err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn guard_passes_through_and_fills_no_phase14_slot() {
    let dir = tempfile::tempdir().expect("tempdir");
    let c = ctx(ServerConfig::server_defaults(), &dir).await;
    let routed: Arc<dyn ostro_api::ControlPlane> =
        Arc::new(ostro_api::fake::FakeControlPlane::default());
    let mut ext = ostro_api::ApiExtensions::default();
    p14::fill_extensions(&c, &mut ext);
    // 14-17 fills the tenants and API-token slots.
    assert!(ext.tenants.is_some() && ext.api_tokens.is_some());
    assert!(ext.audit.is_none() && ext.security.is_none());
    let g = p14::wrap_ingress(&c, Arc::clone(&routed), ext);
    // 14-03: the guard wraps the routed instance (deny-by-default is tested in p14_guard).
    assert!(!Arc::ptr_eq(&g.control, &routed), "guarded since 14-03");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ui_scope_layer_runs_handlers_as_dev_in_both_modes_until_14_21() {
    use axum::routing::get;
    for mode in [AuthMode::None, AuthMode::Token] {
        let layer = p14::UiScopeLayer::for_mode(mode, Arc::new(ostro_ui::auth::AllowAll));
        assert_eq!(layer.mode(), mode);
        let app = layer.apply(axum::Router::new().route(
            "/ui/who",
            get(|| async { format!("{:?}", auth::current().kind) }),
        ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move { axum::serve(listener, app).await });
        let body = reqwest::get(format!("http://{addr}/ui/who"))
            .await
            .expect("get")
            .text()
            .await
            .expect("body");
        assert_eq!(body, "Dev", "{mode:?}");
        server.abort();
    }
}
