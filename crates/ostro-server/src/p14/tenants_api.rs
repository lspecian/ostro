//! Phase 14 (14-17, D-P14-8): `TenantsApi`, the tenant lifecycle (system admin).
//!
//! | Method | Meta write |
//! |---|---|
//! | `create_tenant` | create-only `Put Tenant{id, created_at_ms, quotas…, suspended: false}` (409 if it exists) |
//! | `update_tenant` | CAS `Put` of weight / quotas (`null` clears a quota) |
//! | `suspend_tenant` / `resume_tenant` | CAS `Put` flipping `suspended` (idempotent: no write when unchanged) |
//! | `delete_tenant` | CAS `Delete`, only when suspended and empty, else 409 `tenant_not_empty` |
//!
//! "Empty" = no active workflow version (`<t>/…` actives), no token of the tenant, no
//! sealed secret `<t>/…`, and no non-terminal run: a fan-out listing through the routed
//! control plane (`ListRunsRequest.tenant = t`, one status at a time, limit 1).
//!
//! Authorization (system admin) is the guard's; these impls validate ids and quota values
//! (`> 0`, rate period `> 0`) before any meta write. The default tenant `_` exists implicitly
//! and is never creatable (D23 grammar). Times come from the injected clock.
//!
//! The routed control plane is bound by the guard ([`bind_routed`], called from
//! `guard::wrap`): the seam's `fill_extensions` only sees the context.

use std::sync::{Arc, Mutex, Weak};

use ostro_api::domain::{ListRunsRequest, RunStatus};
use ostro_api::error::reason;
use ostro_api::phase7::RateSpecDto;
use ostro_api::phase14::{TenantInfo, TenantPatch, TenantSpec, TenantsApi};
use ostro_api::{ApiError, ApiExtensions, ControlPlane};
use ostro_meta::command::{MetaCommand, MetaKey, MetaRecord, MetaResult};
use ostro_meta::{MetaError, RateSpec, Tenant};

use super::P14Ctx;

/// CAS attempts before a concurrent-update 503.
const CAS_RETRIES: usize = 8;

/// Fill `ApiExtensions.tenants`.
pub fn install(ctx: &Arc<P14Ctx>, ext: &mut ApiExtensions) {
    ext.tenants = Some(Arc::new(TenantsService::new(ctx)) as Arc<dyn TenantsApi>);
}

// ---------------------------------------------------------------------------------
// Routed control plane binding
// ---------------------------------------------------------------------------------

type Binding = (Weak<P14Ctx>, Weak<dyn ControlPlane>);

static ROUTED: Mutex<Vec<Binding>> = Mutex::new(Vec::new());

fn bindings() -> std::sync::MutexGuard<'static, Vec<Binding>> {
    ROUTED.lock().unwrap_or_else(|p| p.into_inner())
}

/// Bind the routed (unguarded) control plane of `ctx`'s server: `delete_tenant` counts the
/// tenant's non-terminal runs through it. Held weakly; entries of dropped contexts are
/// pruned on every bind.
pub fn bind_routed(ctx: &Arc<P14Ctx>, routed: &Arc<dyn ControlPlane>) {
    let mut b = bindings();
    b.retain(|(c, _)| c.strong_count() > 0 && !std::ptr::eq(c.as_ptr(), Arc::as_ptr(ctx)));
    b.push((Arc::downgrade(ctx), Arc::downgrade(routed)));
}

/// The routed control plane bound to `ctx`, if any (and still alive).
pub fn routed_of(ctx: &Arc<P14Ctx>) -> Option<Arc<dyn ControlPlane>> {
    bindings()
        .iter()
        .find(|(c, _)| std::ptr::eq(c.as_ptr(), Arc::as_ptr(ctx)))
        .and_then(|(_, r)| r.upgrade())
}

// ---------------------------------------------------------------------------------
// Conversions and validation
// ---------------------------------------------------------------------------------

/// `Tenant` record -> wire.
pub fn info_of(t: &Tenant) -> TenantInfo {
    TenantInfo {
        spec: TenantSpec {
            id: t.id.clone(),
            weight: t.weight,
            max_concurrency: t.max_concurrency,
            fuel_per_s: t.fuel_per_s,
            fuel_burst: t.fuel_burst,
            memory_bytes: t.memory_bytes,
            event_rate: t.event_rate.as_ref().map(|r| RateSpecDto {
                limit: r.limit,
                period_ms: r.period_ms,
                burst: r.burst,
            }),
        },
        suspended: t.suspended,
        created_at_ms: t.created_at_ms,
    }
}

fn rate_to_meta(r: &RateSpecDto) -> RateSpec {
    RateSpec {
        limit: r.limit,
        period_ms: r.period_ms,
        burst: r.burst,
    }
}

fn field_error(field: &str, message: impl Into<String>) -> ApiError {
    ApiError::invalid_argument(format!("{field}: {}", message.into()))
        .with_details(serde_json::json!({ "field": field }))
}

/// The tenant id grammar (`_` named explicitly: the default tenant always exists).
pub fn check_id(id: &str) -> Result<(), ApiError> {
    if id == "_" {
        return Err(field_error(
            "id",
            "`_` is the default tenant: it always exists and cannot be created or changed",
        ));
    }
    if !ostro_meta::is_tenant_id(id) {
        return Err(field_error(
            "id",
            format!("{id:?} is not a tenant id (^[a-z0-9][a-z0-9-]{{0,62}}$)"),
        ));
    }
    Ok(())
}

fn positive<T: PartialEq + Default>(field: &str, v: Option<&T>) -> Result<(), ApiError> {
    match v {
        Some(x) if *x == T::default() => {
            Err(field_error(field, "must be > 0 (omit it for no limit)"))
        }
        _ => Ok(()),
    }
}

/// Quota values of a record about to be written: every set value `> 0`, rate period `> 0`.
pub fn check_quotas(t: &Tenant) -> Result<(), ApiError> {
    positive("weight", t.weight.as_ref())?;
    positive("max_concurrency", t.max_concurrency.as_ref())?;
    positive("fuel_per_s", t.fuel_per_s.as_ref())?;
    positive("fuel_burst", t.fuel_burst.as_ref())?;
    positive("memory_bytes", t.memory_bytes.as_ref())?;
    if let Some(r) = &t.event_rate {
        if r.limit == 0 {
            return Err(field_error("event_rate.limit", "must be > 0"));
        }
        if r.period_ms == 0 {
            return Err(field_error("event_rate.period_ms", "must be > 0"));
        }
    }
    Ok(())
}

fn apply_patch(t: &mut Tenant, p: &TenantPatch) {
    if let Some(v) = p.weight {
        t.weight = v;
    }
    if let Some(v) = p.max_concurrency {
        t.max_concurrency = v;
    }
    if let Some(v) = p.fuel_per_s {
        t.fuel_per_s = v;
    }
    if let Some(v) = p.fuel_burst {
        t.fuel_burst = v;
    }
    if let Some(v) = p.memory_bytes {
        t.memory_bytes = v;
    }
    if let Some(v) = &p.event_rate {
        t.event_rate = v.as_ref().map(rate_to_meta);
    }
}

fn meta_unavailable(e: MetaError) -> ApiError {
    ApiError::unavailable(format!("metadata unavailable: {e}"))
}

fn not_found(id: &str) -> ApiError {
    ApiError::not_found(format!("tenant {id} not found"))
}

fn not_empty(id: &str, why: &str) -> ApiError {
    ApiError::conflict(format!("tenant {id} cannot be deleted: {why}"))
        .with_details(serde_json::json!({ "reason": reason::TENANT_NOT_EMPTY, "tenant": id }))
}

fn concurrent() -> ApiError {
    ApiError::unavailable("the tenant is being updated concurrently; retry")
}

// ---------------------------------------------------------------------------------
// The service
// ---------------------------------------------------------------------------------

/// `TenantsApi` over the cluster metadata. Holds the context weakly (503 once released).
pub struct TenantsService {
    ctx: Weak<P14Ctx>,
}

impl TenantsService {
    /// The service of `ctx`.
    pub fn new(ctx: &Arc<P14Ctx>) -> Self {
        Self {
            ctx: Arc::downgrade(ctx),
        }
    }

    fn ctx(&self) -> Result<Arc<P14Ctx>, ApiError> {
        self.ctx
            .upgrade()
            .ok_or_else(|| ApiError::unavailable("the server is shutting down"))
    }

    fn read(ctx: &P14Ctx, id: &str) -> Option<(u64, Tenant)> {
        ctx.meta
            .view()
            .read(|s| s.tenant(id).map(|v| (v.version, v.value.clone())))
    }

    /// CAS-update tenant `id` with `f` (`None` from `f` = no change, nothing written).
    async fn update(
        &self,
        id: &str,
        f: impl Fn(&Tenant) -> Option<Tenant> + Send,
    ) -> Result<TenantInfo, ApiError> {
        check_id(id)?;
        let ctx = self.ctx()?;
        for _ in 0..CAS_RETRIES {
            let (version, cur) = Self::read(&ctx, id).ok_or_else(|| not_found(id))?;
            let Some(next) = f(&cur) else {
                return Ok(info_of(&cur));
            };
            check_quotas(&next)?;
            let res = ctx
                .meta
                .propose(MetaCommand::Put {
                    key: MetaKey::Tenant(id.to_owned()),
                    expected_version: Some(version),
                    record: MetaRecord::Tenant(next.clone()),
                })
                .await
                .map_err(meta_unavailable)?;
            match res {
                MetaResult::Ok { .. } => return Ok(info_of(&next)),
                MetaResult::NotFound => return Err(not_found(id)),
                MetaResult::StaleVersion { .. } | MetaResult::Conflict { .. } => {}
                MetaResult::Invalid(m) => {
                    return Err(ApiError::invalid_argument(format!(
                        "metadata rejected the tenant: {m}"
                    )));
                }
            }
        }
        Err(concurrent())
    }

    /// Why `id` is not empty (`None` = empty). Cheap meta checks first, then the run
    /// fan-out.
    async fn occupancy(ctx: &Arc<P14Ctx>, id: &str) -> Result<Option<String>, ApiError> {
        let prefix = format!("{id}/");
        let meta_reason = ctx.meta.view().read(|s| {
            // Actives are keyed by the (qualified) workflow name of a registered version.
            if let Some(w) = s
                .workflows()
                .filter(|w| w.value.name.starts_with(&prefix))
                .find(|w| s.active(&w.value.name).is_some())
            {
                return Some(format!("workflow {} has an active version", w.value.name));
            }
            let tokens = s
                .tokens()
                .filter(|t| t.value.tenant.as_deref() == Some(id))
                .count();
            if tokens > 0 {
                return Some(format!("{tokens} API token(s) belong to it"));
            }
            let secrets = s
                .secrets()
                .filter(|x| x.value.name.starts_with(&prefix))
                .count();
            if secrets > 0 {
                return Some(format!("{secrets} secret(s) belong to it"));
            }
            None
        });
        if meta_reason.is_some() {
            return Ok(meta_reason);
        }
        let routed = routed_of(ctx)
            .ok_or_else(|| ApiError::unavailable("the run listing is not available yet; retry"))?;
        for status in [RunStatus::Scheduled, RunStatus::Ready, RunStatus::Running] {
            let page = routed
                .list_runs(ListRunsRequest {
                    status: Some(status),
                    tenant: Some(id.to_owned()),
                    limit: 1,
                    ..ListRunsRequest::default()
                })
                .await?;
            if let Some(r) = page.runs.first() {
                return Ok(Some(format!(
                    "run {} is not terminal ({status:?})",
                    r.run_id
                )));
            }
        }
        Ok(None)
    }
}

#[async_trait::async_trait]
impl TenantsApi for TenantsService {
    async fn create_tenant(&self, spec: TenantSpec) -> Result<TenantInfo, ApiError> {
        check_id(&spec.id)?;
        let ctx = self.ctx()?;
        let record = Tenant {
            id: spec.id.clone(),
            created_at_ms: ctx.clock.now_ms(),
            weight: spec.weight,
            max_concurrency: spec.max_concurrency,
            fuel_per_s: spec.fuel_per_s,
            fuel_burst: spec.fuel_burst,
            memory_bytes: spec.memory_bytes,
            event_rate: spec.event_rate.as_ref().map(rate_to_meta),
            suspended: false,
            ext: Vec::new(),
        };
        check_quotas(&record)?;
        let res = ctx
            .meta
            .propose(MetaCommand::Put {
                key: MetaKey::Tenant(spec.id.clone()),
                expected_version: None,
                record: MetaRecord::Tenant(record.clone()),
            })
            .await
            .map_err(meta_unavailable)?;
        match res {
            MetaResult::Ok { .. } => Ok(info_of(&record)),
            MetaResult::Conflict { .. } | MetaResult::StaleVersion { .. } => Err(
                ApiError::conflict(format!("tenant {} already exists", spec.id))
                    .with_details(serde_json::json!({ "tenant": spec.id })),
            ),
            MetaResult::NotFound => Err(not_found(&spec.id)),
            MetaResult::Invalid(m) => Err(ApiError::invalid_argument(format!(
                "metadata rejected the tenant: {m}"
            ))),
        }
    }

    async fn get_tenant(&self, id: String) -> Result<TenantInfo, ApiError> {
        check_id(&id)?;
        let ctx = self.ctx()?;
        Self::read(&ctx, &id)
            .map(|(_, t)| info_of(&t))
            .ok_or_else(|| not_found(&id))
    }

    async fn list_tenants(&self) -> Result<Vec<TenantInfo>, ApiError> {
        let ctx = self.ctx()?;
        // MetaState keeps tenants in a BTreeMap: sorted by id.
        Ok(ctx
            .meta
            .view()
            .read(|s| s.tenants().map(|t| info_of(&t.value)).collect()))
    }

    async fn update_tenant(&self, id: String, patch: TenantPatch) -> Result<TenantInfo, ApiError> {
        self.update(&id, |cur| {
            let mut next = cur.clone();
            apply_patch(&mut next, &patch);
            (next != *cur).then_some(next)
        })
        .await
    }

    async fn suspend_tenant(&self, id: String) -> Result<TenantInfo, ApiError> {
        self.update(&id, |cur| {
            (!cur.suspended).then(|| Tenant {
                suspended: true,
                ..cur.clone()
            })
        })
        .await
    }

    async fn resume_tenant(&self, id: String) -> Result<TenantInfo, ApiError> {
        self.update(&id, |cur| {
            cur.suspended.then(|| Tenant {
                suspended: false,
                ..cur.clone()
            })
        })
        .await
    }

    async fn delete_tenant(&self, id: String) -> Result<(), ApiError> {
        check_id(&id)?;
        let ctx = self.ctx()?;
        for _ in 0..CAS_RETRIES {
            let (version, cur) = Self::read(&ctx, &id).ok_or_else(|| not_found(&id))?;
            if !cur.suspended {
                return Err(not_empty(&id, "suspend it first"));
            }
            if let Some(why) = Self::occupancy(&ctx, &id).await? {
                return Err(not_empty(&id, &why));
            }
            let res = ctx
                .meta
                .propose(MetaCommand::Delete {
                    key: MetaKey::Tenant(id.clone()),
                    expected_version: version,
                })
                .await
                .map_err(meta_unavailable)?;
            match res {
                MetaResult::Ok { .. } => return Ok(()),
                MetaResult::NotFound => return Err(not_found(&id)),
                MetaResult::StaleVersion { .. } | MetaResult::Conflict { .. } => {}
                MetaResult::Invalid(m) => {
                    return Err(ApiError::invalid_argument(format!(
                        "metadata rejected the delete: {m}"
                    )));
                }
            }
        }
        Err(concurrent())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tenant() -> Tenant {
        Tenant {
            id: "acme".into(),
            ..Tenant::default()
        }
    }

    #[test]
    fn ids() {
        assert!(check_id("acme").is_ok());
        assert!(check_id("a-1").is_ok());
        let long = "a".repeat(64);
        for bad in ["_", "", "Acme", "-a", "a/b", long.as_str()] {
            assert!(check_id(bad).is_err(), "{bad:?}");
        }
        assert!(
            check_id("_")
                .unwrap_err()
                .message
                .contains("default tenant")
        );
    }

    #[test]
    fn quotas_must_be_positive() {
        assert!(check_quotas(&tenant()).is_ok());
        let cases: [fn(&mut Tenant); 7] = [
            |t| t.weight = Some(0),
            |t| t.max_concurrency = Some(0),
            |t| t.fuel_per_s = Some(0),
            |t| t.fuel_burst = Some(0),
            |t| t.memory_bytes = Some(0),
            |t| {
                t.event_rate = Some(RateSpec {
                    limit: 0,
                    period_ms: 1,
                    burst: 0,
                })
            },
            |t| {
                t.event_rate = Some(RateSpec {
                    limit: 1,
                    period_ms: 0,
                    burst: 0,
                })
            },
        ];
        for f in cases {
            let mut t = tenant();
            f(&mut t);
            assert!(check_quotas(&t).is_err(), "{t:?}");
        }
    }

    #[test]
    fn patch_sets_clears_and_keeps() {
        let mut t = Tenant {
            weight: Some(2),
            max_concurrency: Some(4),
            ..tenant()
        };
        apply_patch(
            &mut t,
            &TenantPatch {
                weight: Some(Some(3)),
                max_concurrency: Some(None),
                event_rate: Some(Some(RateSpecDto {
                    limit: 10,
                    period_ms: 1000,
                    burst: 0,
                })),
                ..TenantPatch::default()
            },
        );
        assert_eq!(t.weight, Some(3));
        assert_eq!(t.max_concurrency, None);
        assert_eq!(t.event_rate.as_ref().map(|r| r.limit), Some(10));
        assert_eq!(t.memory_bytes, None);
    }
}
