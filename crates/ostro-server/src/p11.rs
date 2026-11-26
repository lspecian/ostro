//! The single Phase 11 wiring seam (D-P11-23, plan 11-04). **Final after 11-04**: later
//! Phase 11 plans edit only `control/p11_*.rs`, `services/{dlq_hook,bulk}.rs`,
//! [`crate::rerun`] and [`crate::onfailure`]; Phase 13 edits only the body of
//! [`p11_feature_enabled`].
//!
//! `server.rs` calls [`wire`] once, where it used to build the API router. `wire`:
//!
//! 1. builds the one [`P11Ctx`] (Phase 8 [`ControlCtx`], the executor's engine
//!    [`Runtime`], [`ostro_engine::RunLimits`] from `[limits]`, the node's
//!    [`BulkRegistry`] and the config) behind an `Arc`;
//! 2. stores a `Weak` of it in `ServerControlPlane::p11` (W7), so control-plane methods
//!    (`list_runs` -> [`crate::control::p11_search`]) reach it through
//!    `ServerControlPlane::p11()`;
//! 3. hands it to the services seam ([`Services::attach_p11`]), which starts the Phase 11
//!    services ([`crate::services::dlq_hook`], [`crate::services::bulk`]) and joins them at
//!    shutdown; the [`BulkRegistry`] those services see is `P11Ctx::bulk`, the same `Arc`
//!    the bulk controllers use;
//! 4. fills `ApiConfig::max_payload_bytes` (`[limits] max_payload_bytes`) and
//!    `ApiConfig::root_redirect` (`/ui/` when the UI is enabled), and the
//!    `ApiExtensions::{ops, inventory}` slots with [`P11Controllers`] (on top of the Phase 8
//!    slots from [`crate::control::phase8_extensions`]);
//! 5. builds the API router (`ostro_api::router_with`) and merges the `ostro_ui` router
//!    (`/ui`, D-P11-17) onto it, so both share the one port.
//!
//! Phase 13 (13-02, deviation from "final after 11-04"): `wire` takes the Phase 13
//! extension filler (`p13::fill_extensions`), applied after the Phase 8/11 slots, and the API
//! and UI use `ControlCtx::routed` (the routed control plane, D-P13-14).
//!
//! # Ownership
//!
//! `P11Ctx::cc` holds the control plane strongly (`ControlCtx::partitions`), so the
//! control plane holds only a `Weak<P11Ctx>`: a strong reference there would be a cycle
//! that keeps meta alive after shutdown. The strong owners are the [`P11Controllers`] in
//! the router (dropped when the API stops serving) and the Phase 11 service tasks (joined
//! by `Services::shutdown`). The node is reached weakly through `ControlCtx::node()`.

use std::sync::{Arc, OnceLock, Weak};

use ostro_api::phase11::{
    BulkCreateRequest, BulkCreateResponse, BulkJobView, DlqDismissRequest, DlqEntry,
    DlqListRequest, DlqPage, DlqRequeueRequest, DlqRequeueResponse, FailurePolicy, InventoryApi,
    LimitsView, ListModulesRequest, ModulePage, OpsApi, PartitionView, PolicyScope, QueueView,
    RerunCutsView, RerunRequest, RerunResponse,
};
use ostro_api::{ApiConfig, ApiError, ApiExtensions, ControlPlane};
use ostro_engine::runtime::Runtime;
use tracing::warn;

use crate::config::ServerConfig;
use crate::control::{ControlCtx, p11_bulk, p11_dlq, p11_inventory, p11_rerun, phase8_extensions};
use crate::services::Services;
use crate::services::bulk::BulkRegistry;

/// Everything a Phase 11 controller or service reaches. Cheap to clone.
#[derive(Clone)]
pub struct P11Ctx {
    /// Phase 8 controller context (node, meta, blobs, guest logs, partition client,
    /// clock, data dir).
    pub cc: ControlCtx,
    /// The executor's engine runtime (cluster token key) for the rerun replay check
    /// (11-10).
    pub runtime: Arc<Runtime>,
    /// `[limits]` (D-P11-13).
    pub limits: ostro_engine::RunLimits,
    /// The node's bulk job registry (11-13 fills its internals).
    pub bulk: Arc<BulkRegistry>,
    /// The effective configuration.
    pub cfg: Arc<ServerConfig>,
}

impl std::fmt::Debug for P11Ctx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("P11Ctx")
            .field("cc", &self.cc)
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

/// The new Phase 11 encodings the server emits (D-P11-30).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum P11Feature {
    /// `Cmd::DlqUpdate` (45).
    DlqUpdate,
    /// `StartRun.rerun_prefix` / `rerun_from_seq` (40/41).
    RerunPrefix,
    /// `JournalEvent.attempt` (4).
    JournalAttempt,
    /// Ext tags 144 (`OnFailure`) / 145 (`DlqPolicy`) on the policy records.
    FailurePolicyExt,
}

impl P11Feature {
    /// Every feature, in declaration order.
    pub const ALL: [P11Feature; 4] = [
        P11Feature::DlqUpdate,
        P11Feature::RerunPrefix,
        P11Feature::JournalAttempt,
        P11Feature::FailurePolicyExt,
    ];
}

/// Single gate for every new Phase 11 emission (D-P11-30, W13). Returns true.
///
/// **Phase 13 obligation:** replace the body with the cluster-version / proposal-gating
/// check so mixed-version clusters never see these encodings early.
pub fn p11_feature_enabled(f: P11Feature) -> bool {
    let _ = f;
    true
}

/// The Phase 11 API controllers: every method delegates to a `control::p11_*` free
/// function taking `&P11Ctx`.
#[derive(Clone, Debug)]
pub struct P11Controllers(pub Arc<P11Ctx>);

#[async_trait::async_trait]
impl OpsApi for P11Controllers {
    async fn rerun_run(&self, req: RerunRequest) -> Result<RerunResponse, ApiError> {
        p11_rerun::rerun_run(&self.0, req).await
    }

    async fn analyze_rerun(&self, run_id: String) -> Result<RerunCutsView, ApiError> {
        p11_rerun::analyze_rerun(&self.0, run_id).await
    }

    async fn list_dlq(&self, req: DlqListRequest) -> Result<DlqPage, ApiError> {
        p11_dlq::list_dlq(&self.0, req).await
    }

    async fn requeue_dlq(&self, req: DlqRequeueRequest) -> Result<DlqRequeueResponse, ApiError> {
        p11_dlq::requeue_dlq(&self.0, req).await
    }

    async fn dismiss_dlq(&self, req: DlqDismissRequest) -> Result<DlqEntry, ApiError> {
        p11_dlq::dismiss_dlq(&self.0, req).await
    }

    async fn create_bulk(&self, req: BulkCreateRequest) -> Result<BulkCreateResponse, ApiError> {
        p11_bulk::create_bulk(&self.0, req).await
    }

    async fn list_bulk(&self) -> Result<Vec<BulkJobView>, ApiError> {
        p11_bulk::list_bulk(&self.0).await
    }

    async fn get_bulk(&self, job_id: String) -> Result<BulkJobView, ApiError> {
        p11_bulk::get_bulk(&self.0, job_id).await
    }

    async fn cancel_bulk(&self, job_id: String) -> Result<BulkJobView, ApiError> {
        p11_bulk::cancel_bulk(&self.0, job_id).await
    }

    async fn limits(&self) -> Result<LimitsView, ApiError> {
        p11_inventory::limits(&self.0).await
    }

    async fn get_failure_policy(&self, scope: PolicyScope) -> Result<FailurePolicy, ApiError> {
        p11_dlq::get_failure_policy(&self.0, scope).await
    }

    async fn set_failure_policy(
        &self,
        scope: PolicyScope,
        p: FailurePolicy,
    ) -> Result<FailurePolicy, ApiError> {
        p11_dlq::set_failure_policy(&self.0, scope, p).await
    }
}

#[async_trait::async_trait]
impl InventoryApi for P11Controllers {
    async fn list_modules(&self, req: ListModulesRequest) -> Result<ModulePage, ApiError> {
        p11_inventory::list_modules(&self.0, req).await
    }

    async fn list_queues(&self) -> Result<Vec<QueueView>, ApiError> {
        p11_inventory::list_queues(&self.0).await
    }

    async fn list_partitions(&self) -> Result<Vec<PartitionView>, ApiError> {
        p11_inventory::list_partitions(&self.0).await
    }
}

/// The API extension slots: the Phase 8 ones ([`phase8_extensions`]) plus `ops` and
/// `inventory` filled with [`P11Controllers`] over `ctx`.
pub fn extensions(cc: &ControlCtx, ctx: &Arc<P11Ctx>) -> ApiExtensions {
    let controllers = Arc::new(P11Controllers(Arc::clone(ctx)));
    ApiExtensions {
        ops: Some(Arc::clone(&controllers) as Arc<dyn OpsApi>),
        inventory: Some(controllers as Arc<dyn InventoryApi>),
        ..phase8_extensions(cc)
    }
}

/// `[ui]` as the UI router sees it: [`crate::config::UiSection::effective_enabled`] for
/// the configured API listener, the allowed hosts and the listen address.
pub fn ui_config(cfg: &ServerConfig) -> ostro_ui::UiConfig {
    ostro_ui::UiConfig {
        enabled: cfg.ui.effective_enabled(cfg.api.listen),
        allowed_hosts: cfg.ui.allowed_hosts.clone(),
        bound_addr: Some(cfg.api.listen),
        ..ostro_ui::UiConfig::default()
    }
}

/// Phase 14 (14-01b, D-P14-33): what the client-facing routers receive, built by
/// `server.rs` from the routed control plane and slots (`p14::wrap_ingress`, `p14::ui_auth`,
/// `p14::ui_scope_layer`).
pub struct ApiFront {
    /// The guarded control plane and slots (REST, gRPC, UI).
    pub guarded: crate::p14::Guarded,
    /// The UI's authentication.
    pub ui_auth: Arc<dyn ostro_ui::auth::UiAuth>,
    /// The `/ui` principal scope.
    pub ui_layer: crate::p14::UiScopeLayer,
}

/// Build the Phase 11 context and the one-port router (see the module docs). `router` is
/// the base the API and UI routers are merged onto (`server.rs` passes an empty one).
/// `front` (Phase 14) turns the routed control plane and slots into what REST, gRPC and the
/// UI serve; in-process consumers keep `cc.routed`.
#[allow(clippy::too_many_arguments)]
pub fn wire(
    cc: &ControlCtx,
    runtime: Arc<Runtime>,
    services: &Services,
    cp_p11: &Arc<OnceLock<Weak<P11Ctx>>>,
    api_cfg: &mut ApiConfig,
    router: axum::Router,
    cfg: &Arc<ServerConfig>,
    p13_extensions: &dyn Fn(&mut ApiExtensions),
    front: &dyn Fn(Arc<dyn ControlPlane>, ApiExtensions) -> ApiFront,
) -> axum::Router {
    let limits = cfg.limits.to_run_limits();
    let bulk = BulkRegistry::new(cc.data_dir.clone(), cfg.bulk.clone(), Arc::clone(&cc.clock));
    let ctx = Arc::new(P11Ctx {
        cc: cc.clone(),
        runtime,
        limits,
        bulk,
        cfg: Arc::clone(cfg),
    });
    if cp_p11.set(Arc::downgrade(&ctx)).is_err() {
        warn!("Phase 11 context was already set on the control plane; keeping the first");
    }
    services.attach_p11(&ctx);

    let ui = ui_config(cfg);
    if ui.enabled && !cfg.api.listen.ip().is_loopback() {
        warn!(
            listen = %cfg.api.listen,
            "the embedded UI (/ui) and the REST API are unauthenticated until Phase 14; \
             they are served on a non-loopback listener"
        );
    }
    api_cfg.max_payload_bytes = usize::try_from(ctx.limits.max_payload_bytes).unwrap_or(usize::MAX);
    api_cfg.root_redirect = ui.enabled.then(|| "/ui/".to_owned());

    let mut ext = extensions(cc, &ctx);
    // Phase 13 (13-02): the Phase 13 slots and routing over every slot, then the routed
    // control plane for the API and the UI.
    p13_extensions(&mut ext);
    // Phase 14 (14-01b): the guarded instances for the client-facing routers.
    let ApiFront {
        guarded,
        ui_auth,
        ui_layer,
    } = front(Arc::clone(&cc.routed), ext);
    let cp = guarded.control;
    let ext = guarded.ext;
    let api = ostro_api::router_with(Arc::clone(&cp), api_cfg.clone(), ext.clone());
    let ui_router = ui_layer.apply(ostro_ui::router(ostro_ui::UiDeps {
        cp,
        ext,
        cfg: ui,
        auth: ui_auth,
    }));
    router.merge(api).merge(ui_router)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_feature_is_enabled_until_phase_13() {
        for f in P11Feature::ALL {
            assert!(p11_feature_enabled(f), "{f:?}");
        }
    }

    #[test]
    fn ui_config_follows_the_listener() {
        let mut cfg = ServerConfig::server_defaults();
        assert!(!ui_config(&cfg).enabled, "0.0.0.0 server profile");
        cfg.api.listen = "127.0.0.1:7700".parse().expect("addr");
        assert!(ui_config(&cfg).enabled, "loopback server profile");
        cfg.ui.enabled = Some(false);
        assert!(!ui_config(&cfg).enabled, "explicit off");
        let dev = ServerConfig::dev_defaults();
        assert!(ui_config(&dev).enabled, "dev profile");
    }
}
