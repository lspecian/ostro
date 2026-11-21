//! Queue flow configuration and queue / workflow pause (plan 07-14; DISP-03..05, OPS-01).
//!
//! Every operation is a read-modify-CAS of the 05-07 meta records:
//!
//! | Route | Record | Fields written |
//! |---|---|---|
//! | `PUT /v1/queues/{q}/flow` | `QueuePolicy` | the Phase 7 F2 fields (placement, selector, spill, concurrency, weight, key_concurrency, throttles, rate limits, on_retries_exhausted, paused) |
//! | `POST /v1/queues/{q}/pause\|resume` | `QueuePolicy` | `paused` |
//! | `POST /v1/workflows/{w}/pause\|resume` | `WorkflowPolicy` | `paused` |
//!
//! The Phase 5 fields (`retention_ms`, `schedule_to_start_ms`, written by
//! `/v1/{queues,workflows}/{name}/policy`) and `ext` are always carried over from the
//! current record. Nothing else is needed for the dispatchers: the 07-08 flow watcher
//! refreshes its policy view on every meta apply and bumps `changed()`, so a pause stops new
//! grants and a resume wakes the dispatchers without polling.
//!
//! Versions: `QueueFlowInfo.version` is the meta record version (0 = no record). A `PUT`
//! with `expected_version` that does not match answers 412 `PreconditionFailed` with
//! `details.current_version`; without it the write is last-writer-wins (re-read on a lost
//! race). Pause and resume are idempotent (`changed = false` on a repeat) and re-read on a
//! lost race.
//!
//! Names follow D23 like the Phase 5 policy routes ([`super::policy::policy_key`]): bare =
//! the default tenant, `acme/q` = tenant-qualified. Queues are implicit; workflows must
//! exist (404 otherwise).

use ostro_api::ApiError;
use ostro_api::phase7::{
    ExhaustAction, PauseState, Placement, QueueFlow, QueueFlowInfo, RateSpecDto, SpillPolicyDto,
    require_name,
};
use ostro_meta::records::{
    ON_EXHAUSTED_FAIL, ON_EXHAUSTED_PAUSE, PLACEMENT_AUTO, PLACEMENT_EMBEDDED, PLACEMENT_WORKERS,
    RateSpec, SpillPolicy,
};
use ostro_meta::{
    MetaCommand, MetaError, MetaKey, MetaRecord, MetaResult, QueuePolicy, WorkflowPolicy,
};

use super::ServerControlPlane;
use super::policy::policy_key;

/// CAS attempts before a concurrently updated record answers 503.
const CAS_RETRIES: usize = 8;

fn meta_unavailable(e: MetaError) -> ApiError {
    ApiError::unavailable(format!("metadata unavailable: {e}"))
}

// ---------------------------------------------------------------------------------
// Conversions (meta F2 fields <-> API types)
// ---------------------------------------------------------------------------------

fn placement_to_meta(p: Placement) -> u8 {
    match p {
        Placement::Embedded => PLACEMENT_EMBEDDED,
        Placement::Workers => PLACEMENT_WORKERS,
        Placement::Auto => PLACEMENT_AUTO,
    }
}

fn placement_from_meta(v: u8) -> Option<Placement> {
    match v {
        PLACEMENT_EMBEDDED => Some(Placement::Embedded),
        PLACEMENT_WORKERS => Some(Placement::Workers),
        PLACEMENT_AUTO => Some(Placement::Auto),
        _ => None,
    }
}

fn exhaust_to_meta(a: ExhaustAction) -> u8 {
    match a {
        ExhaustAction::Fail => ON_EXHAUSTED_FAIL,
        ExhaustAction::Pause => ON_EXHAUSTED_PAUSE,
    }
}

fn exhaust_from_meta(v: u8) -> Option<ExhaustAction> {
    match v {
        ON_EXHAUSTED_FAIL => Some(ExhaustAction::Fail),
        ON_EXHAUSTED_PAUSE => Some(ExhaustAction::Pause),
        _ => None,
    }
}

fn rate_to_meta(r: &RateSpecDto) -> RateSpec {
    RateSpec {
        limit: r.limit,
        period_ms: r.period_ms,
        burst: r.burst,
    }
}

fn rate_from_meta(r: &RateSpec) -> RateSpecDto {
    RateSpecDto {
        limit: r.limit,
        period_ms: r.period_ms,
        burst: r.burst,
    }
}

fn spill_to_meta(s: &SpillPolicyDto) -> SpillPolicy {
    SpillPolicy {
        cpu_pct: s.cpu_pct,
        slots_pct: s.slots_pct,
        mem_pct: s.mem_pct,
        lag_ms: s.lag_ms,
        sustain_ms: s.sustain_ms,
        cooldown_ms: s.cooldown_ms,
        hysteresis_pct: s.hysteresis_pct,
    }
}

fn spill_from_meta(s: &SpillPolicy) -> SpillPolicyDto {
    SpillPolicyDto {
        cpu_pct: s.cpu_pct,
        slots_pct: s.slots_pct,
        mem_pct: s.mem_pct,
        lag_ms: s.lag_ms,
        sustain_ms: s.sustain_ms,
        cooldown_ms: s.cooldown_ms,
        hysteresis_pct: s.hysteresis_pct,
    }
}

/// The API view of a stored policy's F2 fields.
pub(crate) fn flow_of(p: &QueuePolicy) -> QueueFlow {
    QueueFlow {
        placement: p.placement.and_then(placement_from_meta),
        selector: p.selector.clone(),
        spill: p.spill.as_ref().map(spill_from_meta),
        concurrency: p.concurrency,
        weight: p.weight,
        key_concurrency: p.key_concurrency,
        throttle: p.throttle.as_ref().map(rate_from_meta),
        key_throttle: p.key_throttle.as_ref().map(rate_from_meta),
        rate_limit: p.rate_limit.as_ref().map(rate_from_meta),
        key_rate_limit: p.key_rate_limit.as_ref().map(rate_from_meta),
        on_retries_exhausted: p.on_retries_exhausted.and_then(exhaust_from_meta),
        paused: p.paused,
    }
}

/// `cur` with every F2 field replaced by `flow`; the Phase 5 fields and `ext` are kept.
pub(crate) fn apply_flow(key: &str, cur: Option<QueuePolicy>, flow: &QueueFlow) -> QueuePolicy {
    let cur = cur.unwrap_or_default();
    QueuePolicy {
        queue: key.to_owned(),
        retention_ms: cur.retention_ms,
        schedule_to_start_ms: cur.schedule_to_start_ms,
        placement: flow.placement.map(placement_to_meta),
        selector: flow.selector.clone(),
        spill: flow.spill.as_ref().map(spill_to_meta),
        concurrency: flow.concurrency,
        weight: flow.weight,
        key_concurrency: flow.key_concurrency,
        throttle: flow.throttle.as_ref().map(rate_to_meta),
        key_throttle: flow.key_throttle.as_ref().map(rate_to_meta),
        rate_limit: flow.rate_limit.as_ref().map(rate_to_meta),
        key_rate_limit: flow.key_rate_limit.as_ref().map(rate_to_meta),
        on_retries_exhausted: flow.on_retries_exhausted.map(exhaust_to_meta),
        paused: flow.paused,
        ext: cur.ext,
    }
}

// ---------------------------------------------------------------------------------
// Queue flow
// ---------------------------------------------------------------------------------

fn current_queue(cp: &ServerControlPlane, key: &str) -> Option<(u64, QueuePolicy)> {
    cp.meta()
        .view()
        .read(|s| s.queue_policy(key).map(|v| (v.version, v.value.clone())))
}

fn flow_info(queue: &str, cur: Option<(u64, QueuePolicy)>) -> QueueFlowInfo {
    match cur {
        Some((version, p)) => QueueFlowInfo {
            queue: queue.to_owned(),
            version,
            flow: flow_of(&p),
        },
        None => QueueFlowInfo {
            queue: queue.to_owned(),
            version: 0,
            flow: QueueFlow::default(),
        },
    }
}

fn stale(queue: &str, current: u64, expected: u64) -> ApiError {
    ApiError::precondition_failed(format!(
        "queue {queue} flow is at version {current}, not {expected}"
    ))
    .with_details(serde_json::json!({ "current_version": current }))
}

pub(crate) async fn get_queue_flow(
    cp: &ServerControlPlane,
    queue: String,
) -> Result<QueueFlowInfo, ApiError> {
    require_name("queue name", &queue)?;
    let key = policy_key("queue", &queue)?;
    Ok(flow_info(&queue, current_queue(cp, &key)))
}

pub(crate) async fn put_queue_flow(
    cp: &ServerControlPlane,
    queue: String,
    flow: QueueFlow,
    expected_version: Option<u64>,
) -> Result<QueueFlowInfo, ApiError> {
    require_name("queue name", &queue)?;
    flow.validate()?;
    let key = policy_key("queue", &queue)?;
    for _ in 0..CAS_RETRIES {
        let cur = current_queue(cp, &key);
        let version = cur.as_ref().map(|(v, _)| *v);
        if let Some(expected) = expected_version
            && expected != version.unwrap_or(0)
        {
            return Err(stale(&queue, version.unwrap_or(0), expected));
        }
        let record = apply_flow(&key, cur.map(|(_, p)| p), &flow);
        let res = cp
            .meta()
            .propose(MetaCommand::Put {
                key: MetaKey::QueuePolicy(key.clone()),
                expected_version: version,
                record: MetaRecord::QueuePolicy(record),
            })
            .await
            .map_err(meta_unavailable)?;
        match res {
            MetaResult::Ok { .. } => return Ok(flow_info(&queue, current_queue(cp, &key))),
            MetaResult::StaleVersion { current } | MetaResult::Conflict { current } => {
                // Another writer won between the read and the CAS.
                if let Some(expected) = expected_version {
                    return Err(stale(&queue, current, expected));
                }
            }
            MetaResult::NotFound => {}
            MetaResult::Invalid(msg) => {
                return Err(ApiError::invalid_argument(format!(
                    "metadata rejected the flow: {msg}"
                )));
            }
        }
    }
    Err(ApiError::unavailable(
        "the queue flow is being updated concurrently; retry",
    ))
}

// ---------------------------------------------------------------------------------
// Pause / resume
// ---------------------------------------------------------------------------------

/// Read-modify-CAS of a `paused` flag. `current` reads `(version, paused, record)`,
/// `build` returns the record with `paused` set. Returns whether the flag changed.
async fn set_paused<R>(
    cp: &ServerControlPlane,
    key: MetaKey,
    paused: bool,
    current: impl Fn() -> Option<(u64, bool, R)>,
    build: impl Fn(Option<R>) -> MetaRecord,
) -> Result<bool, ApiError> {
    for _ in 0..CAS_RETRIES {
        let cur = current();
        let was = cur.as_ref().is_some_and(|(_, p, _)| *p);
        if was == paused {
            return Ok(false);
        }
        let expected = cur.as_ref().map(|(v, _, _)| *v);
        let record = build(cur.map(|(_, _, r)| r));
        let res = cp
            .meta()
            .propose(MetaCommand::Put {
                key: key.clone(),
                expected_version: expected,
                record,
            })
            .await
            .map_err(meta_unavailable)?;
        match res {
            MetaResult::Ok { .. } => return Ok(true),
            MetaResult::StaleVersion { .. }
            | MetaResult::Conflict { .. }
            | MetaResult::NotFound => {}
            MetaResult::Invalid(msg) => {
                return Err(ApiError::invalid_argument(format!(
                    "metadata rejected the policy: {msg}"
                )));
            }
        }
    }
    Err(ApiError::unavailable(
        "the policy is being updated concurrently; retry",
    ))
}

async fn queue_paused(
    cp: &ServerControlPlane,
    queue: String,
    paused: bool,
) -> Result<PauseState, ApiError> {
    require_name("queue name", &queue)?;
    let key = policy_key("queue", &queue)?;
    let changed = set_paused(
        cp,
        MetaKey::QueuePolicy(key.clone()),
        paused,
        || current_queue(cp, &key).map(|(v, p)| (v, p.paused, p)),
        |cur: Option<QueuePolicy>| {
            MetaRecord::QueuePolicy(QueuePolicy {
                queue: key.clone(),
                paused,
                ..cur.unwrap_or_default()
            })
        },
    )
    .await?;
    Ok(PauseState {
        name: queue,
        paused,
        changed,
    })
}

pub(crate) async fn pause_queue(
    cp: &ServerControlPlane,
    queue: String,
) -> Result<PauseState, ApiError> {
    queue_paused(cp, queue, true).await
}

pub(crate) async fn resume_queue(
    cp: &ServerControlPlane,
    queue: String,
) -> Result<PauseState, ApiError> {
    queue_paused(cp, queue, false).await
}

async fn workflow_paused(
    cp: &ServerControlPlane,
    workflow: String,
    paused: bool,
) -> Result<PauseState, ApiError> {
    require_name("workflow name", &workflow)?;
    let key = policy_key("workflow", &workflow)?;
    let exists = cp
        .meta()
        .view()
        .read(|s| s.workflows().any(|w| w.value.name == key));
    if !exists {
        return Err(ApiError::not_found(format!("workflow {key} not found")));
    }
    let changed = set_paused(
        cp,
        MetaKey::WorkflowPolicy(key.clone()),
        paused,
        || {
            cp.meta().view().read(|s| {
                s.workflow_policy(&key)
                    .map(|v| (v.version, v.value.paused, v.value.clone()))
            })
        },
        |cur: Option<WorkflowPolicy>| {
            MetaRecord::WorkflowPolicy(WorkflowPolicy {
                workflow: key.clone(),
                paused,
                ..cur.unwrap_or_default()
            })
        },
    )
    .await?;
    Ok(PauseState {
        name: workflow,
        paused,
        changed,
    })
}

pub(crate) async fn pause_workflow(
    cp: &ServerControlPlane,
    workflow: String,
) -> Result<PauseState, ApiError> {
    workflow_paused(cp, workflow, true).await
}

pub(crate) async fn resume_workflow(
    cp: &ServerControlPlane,
    workflow: String,
) -> Result<PauseState, ApiError> {
    workflow_paused(cp, workflow, false).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn full_flow() -> QueueFlow {
        QueueFlow {
            placement: Some(Placement::Auto),
            selector: vec!["zone in (a,b)".into(), "!gpu".into()],
            spill: Some(SpillPolicyDto {
                cpu_pct: 70,
                slots_pct: 80,
                mem_pct: 90,
                lag_ms: 1000,
                sustain_ms: 2000,
                cooldown_ms: 3000,
                hysteresis_pct: 50,
            }),
            concurrency: Some(4),
            weight: Some(3),
            key_concurrency: Some(1),
            throttle: Some(RateSpecDto {
                limit: 5,
                period_ms: 1000,
                burst: 2,
            }),
            key_throttle: Some(RateSpecDto {
                limit: 1,
                period_ms: 60_000,
                burst: 0,
            }),
            rate_limit: Some(RateSpecDto {
                limit: 100,
                period_ms: 1000,
                burst: 10,
            }),
            key_rate_limit: Some(RateSpecDto {
                limit: 2,
                period_ms: 500,
                burst: 1,
            }),
            on_retries_exhausted: Some(ExhaustAction::Pause),
            paused: true,
        }
    }

    #[test]
    fn flow_round_trips_through_the_meta_record() {
        let flow = full_flow();
        let rec = apply_flow("q", None, &flow);
        assert_eq!(rec.placement, Some(PLACEMENT_AUTO));
        assert_eq!(rec.on_retries_exhausted, Some(ON_EXHAUSTED_PAUSE));
        assert_eq!(flow_of(&rec), flow);
        for p in [Placement::Embedded, Placement::Workers, Placement::Auto] {
            assert_eq!(placement_from_meta(placement_to_meta(p)), Some(p));
        }
        for a in [ExhaustAction::Fail, ExhaustAction::Pause] {
            assert_eq!(exhaust_from_meta(exhaust_to_meta(a)), Some(a));
        }
    }

    #[test]
    fn apply_flow_preserves_phase5_fields_and_ext() {
        let cur = QueuePolicy {
            queue: "q".into(),
            retention_ms: Some(7),
            schedule_to_start_ms: Some(9),
            concurrency: Some(99),
            ext: vec![1, 2, 3],
            ..QueuePolicy::default()
        };
        let rec = apply_flow("q", Some(cur), &QueueFlow::default());
        assert_eq!(rec.retention_ms, Some(7));
        assert_eq!(rec.schedule_to_start_ms, Some(9));
        assert_eq!(rec.ext, vec![1, 2, 3]);
        // Every flow field is replaced (absent = cleared).
        assert_eq!(rec.concurrency, None);
    }
}
