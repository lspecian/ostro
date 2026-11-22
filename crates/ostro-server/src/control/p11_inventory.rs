//! Limits and inventory views (Phase 11, D-P11-13, D-P11-19, plan 11-11): `GET /v1/limits`,
//! `/v1/modules`, `/v1/queues`, `/v1/partitions`. Called by [`crate::p11::P11Controllers`].
//!
//! # Limits
//!
//! [`limits`] returns one row per limit in `docs/limits.md` (same names, same order). The
//! configurable ones carry the effective value (`[limits]`, `[api]`); the others are the
//! code constants. `tests/limits_doc.rs` keeps the doc, this endpoint and the constants
//! equal.
//!
//! # Modules
//!
//! Every meta `Module` record, newest upload first (ties: sha ascending), paginated with
//! the token `m1.<uploaded_at_ms>.<sha hex>` (the last module returned). `workflows` are
//! the distinct workflow names registered from the module.
//!
//! # Queues
//!
//! The known queue names: meta queue configs, workflow default queues, `default`, the
//! queues with dispatcher gauges (Phase 7) and every queue with ready or running runs.
//! `paused` and `flow` come from the queue's meta `QueuePolicy` (none = not paused, no
//! flow), `ready` / `running` are counted from the partitions' `ready` rows and running
//! runs, and `demand` is the Phase 7 worker demand snapshot.
//!
//! # Partitions (D-P11-19: only what the node exposes)
//!
//! | Field | Source |
//! |---|---|
//! | `id` | slot index |
//! | `role` | `leader` for a started slot (RF1: this node leads every partition it started), `failed` for a slot that failed to start |
//! | `leader` | `[node] node_id` for a started slot |
//! | `epoch` | `PartitionHandle::epoch` |
//! | `applied_offset` | `MachineHandle::applied` |
//! | `log_end_offset` | `ReplicatedLog::log_end` |
//! | `ready` / `running` | `ready` rows / `running` rows of the store |
//! | `healthy` | `MachineHandle::health == Healthy` |
//! | `detail` | `starting`, the failure reason, or the start failure |
//!
//! A failed slot reports only `id`, `role`, `healthy = false` and `detail`.

use std::collections::{BTreeMap, BTreeSet};

use ostro_api::ApiError;
use ostro_api::domain::{MAX_PAGE_LIMIT, MAX_RESULT_WAIT_MS, effective_limit};
use ostro_api::phase11::{
    LimitView, LimitsView, ListModulesRequest, ModulePage, ModuleSummary, PartitionView, QueueView,
};
use ostro_log::replicated::ReplicatedLog;
use ostro_partition::machine::PartitionHealth;
use ostro_partition::node::PartitionSlot;
use ostro_partition::query;
use ostro_types::command::{
    MAX_REASON_LEN, MAX_REQUEST_ID_LEN, MAX_RUN_TAG_LEN, MAX_RUN_TAGS, MAX_SIGNAL_NAME_LEN,
};

use super::p7_flow::flow_of;
use super::runs::{store_error, task_error};
use crate::p11::P11Ctx;
use crate::workers::demand;

/// Module page token prefix (format version 1).
const MODULE_TOKEN_PREFIX: &str = "m1.";

fn row(
    name: &str,
    value: u64,
    unit: &str,
    configurable: bool,
    enforced: &str,
    error: &str,
) -> LimitView {
    LimitView {
        name: name.to_owned(),
        value,
        unit: unit.to_owned(),
        configurable,
        enforced: enforced.to_owned(),
        error: error.to_owned(),
    }
}

fn n(v: usize) -> u64 {
    u64::try_from(v).unwrap_or(u64::MAX)
}

/// The published limits (same rows as `docs/limits.md`).
pub async fn limits(ctx: &P11Ctx) -> Result<LimitsView, ApiError> {
    const RUN_FAILED: &str = "run failed (resumable), reason limit_exceeded";
    const TOO_LARGE: &str = "413 payload_too_large / gRPC RESOURCE_EXHAUSTED";
    const INVALID: &str = "400 invalid_argument";
    let l = &ctx.limits;
    let api = &ctx.cfg.api;
    Ok(LimitsView {
        limits: vec![
            row(
                "max_history_events",
                l.max_history_events,
                "events per run",
                true,
                "engine, live appends only",
                RUN_FAILED,
            ),
            row(
                "max_children_per_run",
                l.max_children_per_run,
                "ChildStarted per run",
                true,
                "engine, live appends only",
                RUN_FAILED,
            ),
            row(
                "max_payload_bytes",
                l.max_payload_bytes,
                "bytes",
                true,
                "engine, live appends only; API (start input, signal and token payloads)",
                "engine: run failed (resumable), reason limit_exceeded; API: 413 payload_too_large / gRPC RESOURCE_EXHAUSTED",
            ),
            row(
                "max_request_bytes",
                api.max_request_bytes,
                "bytes",
                true,
                "API (REST body, gRPC message)",
                TOO_LARGE,
            ),
            row(
                "max_module_bytes",
                api.max_module_bytes,
                "bytes",
                true,
                "API (module upload)",
                TOO_LARGE,
            ),
            row(
                "MAX_CONTEXT_BYTES",
                n(ostro_engine::host::context::MAX_CONTEXT_BYTES),
                "bytes",
                false,
                "engine (set-status JSON and random-bytes length)",
                "attempt fails protocol_violation",
            ),
            row(
                "MAX_RUN_TAGS",
                n(MAX_RUN_TAGS),
                "tags per run",
                false,
                "API and apply (StartRun)",
                INVALID,
            ),
            row(
                "MAX_RUN_TAG_LEN",
                n(MAX_RUN_TAG_LEN),
                "bytes per tag",
                false,
                "API and apply (StartRun)",
                INVALID,
            ),
            row(
                "MAX_SIGNAL_NAME_LEN",
                n(MAX_SIGNAL_NAME_LEN),
                "bytes",
                false,
                "API and apply",
                INVALID,
            ),
            row(
                "MAX_REASON_LEN",
                n(MAX_REASON_LEN),
                "bytes",
                false,
                "API and apply",
                INVALID,
            ),
            row(
                "MAX_REQUEST_ID_LEN",
                n(MAX_REQUEST_ID_LEN),
                "bytes",
                false,
                "API and apply",
                INVALID,
            ),
            row(
                "MAX_PAGE_LIMIT",
                u64::from(MAX_PAGE_LIMIT),
                "items per page",
                false,
                "API (list endpoints)",
                "none: a larger page size is capped",
            ),
            row(
                "MAX_RESULT_WAIT_MS",
                MAX_RESULT_WAIT_MS,
                "milliseconds",
                false,
                "API (result long-poll)",
                "none: a longer wait is capped",
            ),
            row(
                "MAX_RERUN_PREFIX_BYTES",
                n(super::p11_rerun::MAX_RERUN_PREFIX_BYTES),
                "bytes (encoded rerun StartRun)",
                false,
                "API (rerun, after offloading copied payloads over 1 KiB to blobs; prefix also capped at max_history_events)",
                "413 payload_too_large, details.reason rerun_prefix_too_large",
            ),
        ],
    })
}

fn hex(b: &[u8; 32]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// Module order key: newest upload first, then sha ascending.
type ModuleKey = (std::cmp::Reverse<u64>, String);

fn decode_module_token(t: &str) -> Result<ModuleKey, ApiError> {
    let bad = || ApiError::invalid_argument("page_token: malformed");
    let (at, sha) = t
        .strip_prefix(MODULE_TOKEN_PREFIX)
        .and_then(|r| r.split_once('.'))
        .ok_or_else(bad)?;
    let at: u64 = at.parse().map_err(|_| bad())?;
    if sha.len() != 64 || !sha.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(bad());
    }
    Ok((std::cmp::Reverse(at), sha.to_owned()))
}

/// Stored modules, newest first, paginated.
pub async fn list_modules(ctx: &P11Ctx, req: ListModulesRequest) -> Result<ModulePage, ApiError> {
    let limit = effective_limit(req.limit) as usize;
    let after = req
        .page_token
        .as_deref()
        .filter(|t| !t.is_empty())
        .map(decode_module_token)
        .transpose()?;
    let mut all: Vec<(ModuleKey, ModuleSummary)> = ctx.cc.meta.view().read(|s| {
        s.modules()
            .map(|m| {
                let sha = hex(&m.value.sha256);
                let workflows: BTreeSet<String> =
                    m.value.workflows.iter().map(|(w, _)| w.clone()).collect();
                (
                    (std::cmp::Reverse(m.value.uploaded_at_ms), sha.clone()),
                    ModuleSummary {
                        sha256: sha,
                        size_bytes: m.value.size,
                        uploaded_at_ms: (m.value.uploaded_at_ms != 0)
                            .then_some(m.value.uploaded_at_ms),
                        workflows: workflows.into_iter().collect(),
                    },
                )
            })
            .collect()
    });
    all.sort_by(|a, b| a.0.cmp(&b.0));
    let mut rest = all
        .into_iter()
        .filter(|(k, _)| after.as_ref().is_none_or(|a| k > a))
        .peekable();
    let mut modules = Vec::new();
    let mut last = None;
    while modules.len() < limit {
        let Some((k, m)) = rest.next() else { break };
        last = Some(k);
        modules.push(m);
    }
    let next_page_token = match (rest.peek(), last) {
        (Some(_), Some((at, sha))) => Some(format!("{MODULE_TOKEN_PREFIX}{}.{sha}", at.0)),
        _ => None,
    };
    Ok(ModulePage {
        modules,
        next_page_token,
    })
}

/// Per-partition `(queue -> ready, queue -> running)` counts.
type Counts = (BTreeMap<String, u64>, BTreeMap<String, u64>);

/// Ready / running counts per queue over every started partition.
async fn queue_counts(ctx: &P11Ctx) -> Result<Counts, ApiError> {
    let node = ctx.cc.node()?;
    let readers: Vec<_> = node
        .slots()
        .iter()
        .filter_map(|s| match s {
            PartitionSlot::Ready(h) => Some(h.reader()),
            PartitionSlot::Failed { .. } => None,
        })
        .collect();
    drop(node);
    tokio::task::spawn_blocking(move || {
        let mut ready: BTreeMap<String, u64> = BTreeMap::new();
        let mut running: BTreeMap<String, u64> = BTreeMap::new();
        for r in &readers {
            for e in query::ready_entries(r, usize::MAX)? {
                *ready.entry(e.queue).or_default() += 1;
            }
            for (run, _) in query::running_runs(r)? {
                if let Some(rec) = query::run_record(r, &run)? {
                    *running.entry(rec.queue).or_default() += 1;
                }
            }
        }
        Ok::<_, ostro_partition::StoreError>((ready, running))
    })
    .await
    .map_err(task_error)?
    .map_err(store_error)
}

/// Queues with counts, sorted by name.
pub async fn list_queues(ctx: &P11Ctx) -> Result<Vec<QueueView>, ApiError> {
    let (ready, running) = queue_counts(ctx).await?;
    let mut names: BTreeSet<String> = ctx.cc.meta.view().read(|s| {
        s.queues()
            .map(|q| q.value.name.clone())
            .chain(s.workflows().filter_map(|w| w.value.default_queue.clone()))
            .collect()
    });
    names.insert("default".to_owned());
    names.extend(ready.keys().cloned());
    names.extend(running.keys().cloned());
    let p7 = &ctx.cc.partitions.p7;
    let demands = demand::snapshot_all(p7, names.iter().cloned());
    names.extend(demands.keys().cloned());
    let policies: BTreeMap<String, ostro_meta::QueuePolicy> = ctx.cc.meta.view().read(|s| {
        names
            .iter()
            .filter_map(|n| s.queue_policy(n).map(|p| (n.clone(), p.value.clone())))
            .collect()
    });
    let mut out = Vec::with_capacity(names.len());
    for name in names {
        let policy = policies.get(&name);
        let flow = match policy {
            Some(p) => Some(
                serde_json::to_value(flow_of(p))
                    .map_err(|e| ApiError::internal(format!("queue {name} flow: {e}")))?,
            ),
            None => None,
        };
        out.push(QueueView {
            paused: policy.is_some_and(|p| p.paused),
            ready: ready.get(&name).copied().unwrap_or(0),
            running: running.get(&name).copied().unwrap_or(0),
            demand: demands.get(&name).map(|d| d.demand),
            flow,
            name,
        });
    }
    Ok(out)
}

/// Partitions with role and health (see the module docs for the field list).
pub async fn list_partitions(ctx: &P11Ctx) -> Result<Vec<PartitionView>, ApiError> {
    let node = ctx.cc.node()?;
    let node_id = ctx.cfg.node.node_id;
    struct Live {
        id: u16,
        epoch: u64,
        applied: u64,
        log_end: u64,
        health: PartitionHealth,
        reader: ostro_partition::store::StoreReader,
    }
    let mut live = Vec::new();
    let mut failed = Vec::new();
    for (i, slot) in node.slots().iter().enumerate() {
        let id = u16::try_from(i).unwrap_or(u16::MAX);
        match slot {
            PartitionSlot::Ready(h) => live.push(Live {
                id,
                epoch: u64::from(h.epoch().0),
                applied: h.machine().applied().borrow().0,
                log_end: h.log().log_end().0,
                health: h.machine().health().borrow().clone(),
                reader: h.reader(),
            }),
            PartitionSlot::Failed { partition, reason } => failed.push(PartitionView {
                id: partition.0,
                role: "failed".to_owned(),
                leader: None,
                epoch: None,
                applied_offset: None,
                log_end_offset: None,
                ready: None,
                running: None,
                healthy: false,
                detail: Some(format!("failed to start: {reason}")),
            }),
        }
    }
    drop(node);
    let mut out = tokio::task::spawn_blocking(move || {
        let mut out = Vec::with_capacity(live.len());
        for p in live {
            let ready = query::ready_entries(&p.reader, usize::MAX)?.len();
            let running = query::running_runs(&p.reader)?.len();
            let (healthy, detail) = match p.health {
                PartitionHealth::Healthy => (true, None),
                PartitionHealth::Starting => (false, Some("starting".to_owned())),
                PartitionHealth::Failed { reason } => (false, Some(reason)),
            };
            out.push(PartitionView {
                id: p.id,
                role: "leader".to_owned(),
                leader: Some(node_id),
                epoch: Some(p.epoch),
                applied_offset: Some(p.applied),
                log_end_offset: Some(p.log_end),
                ready: Some(n(ready)),
                running: Some(n(running)),
                healthy,
                detail,
            });
        }
        Ok::<_, ostro_partition::StoreError>(out)
    })
    .await
    .map_err(task_error)?
    .map_err(store_error)?;
    out.extend(failed);
    out.sort_by_key(|p| p.id);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn module_token_round_trip_and_garbage() {
        let sha = "ab".repeat(32);
        let k = decode_module_token(&format!("m1.17.{sha}")).expect("ok");
        assert_eq!(k, (std::cmp::Reverse(17), sha));
        for bad in ["", "m1.", "m1.x.y", "m1.1.abc", "m2.1.00"] {
            assert!(decode_module_token(bad).is_err(), "{bad:?}");
        }
    }
}
