//! Cron schedule controller (08-14, TRIG-02/TRIG-03, P8-D10): `ostro_api::phase8::ScheduleApi`
//! over meta `Schedule` records (CAS on the record version) and the partitions
//! (`ScheduleFire{kind: Manual}` for trigger-now), plus [`MetaScheduleSource`], the
//! meta-backed [`ScheduleSource`] the owner partitions' cron drivers read.
//!
//! # Validation (create / update)
//!
//! - name, workflow, cron required and syntax (API layer, shared by REST and gRPC);
//! - cron expression, time zone and bounds with `ostro_triggers::cron::CronSpec::parse_bounded`
//!   (seconds field optional, `L`/`#`/`W`; unknown zone or never-firing expression = 400 with
//!   the engine's message);
//! - the workflow exists in meta (and the pinned version, when given) = 400 otherwise;
//! - overlap / catch-up map to the meta u8 constants ([`overlap_to_meta`] /
//!   [`catchup_to_meta`], 05-07 numbering).
//!
//! # CAS
//!
//! Create is `Put(expected = None)`: an existing name is 409. Update and delete take the
//! `version_tag` (body / `?version_tag=` / `If-Match`): stale = 409 `version_mismatch`.
//! Update keeps the stored `created_at_ms` (same incarnation, W4: the watermark carries over)
//! and `ext`. Pause / resume flip `paused` with a CAS read-modify-write (retried on a
//! concurrent change; idempotent). Delete leaves the owner partition's `schedule_state` row
//! orphaned: harmless, because the driver's floor is `max(watermark, created_at_ms)` and a
//! re-created schedule is a new incarnation (W6).
//!
//! # Trigger-now
//!
//! `ScheduleFire{kind: Manual, fire_time_ms: now, request_id}` on the owner partition
//! (`schedule_owner(name, P)`); `request_id` is the given string's bytes or 16 random bytes,
//! the run's idempotency key `__cron/<name>/manual/<hex>` (`ReturnExisting`: a repeated
//! `request_id` returns the same run). Manual fires never move the watermark; they are
//! subject to the overlap policy like any fire (an overlap skip is 409 `schedule_overlap`, a
//! buffered fire returns the run id that will start when the live run ends), except that a
//! repeated `request_id` is answered by its dedupe row first (apply returns
//! `Duplicate { existing }`), so a retry while its own run is live returns that run.
//!
//! # Schedule source
//!
//! [`MetaScheduleSource`] is a **subscription**: it waits on the meta applied-index watch
//! (`MetaView::subscribe`), re-reads the schedule set after every applied entry and publishes
//! a new `Arc<Vec<ScheduleSpec>>` only when the set changed. Its `build_start` builds the run
//! like the API start path: the workflow name qualified per D23, the pinned or active
//! version, the version's default queue, the module pin, the run timeout, the D9 policy
//! (`[runs]` / workflow / queue policies), the pre-encoded `RunStarted` (D24) and the tag
//! `ostro.schedule=<name>`.

use std::sync::{Arc, Mutex, PoisonError};

use bytes::Bytes;
use ostro_api::ApiError;
use ostro_api::phase8::{
    CatchupPolicyDto, OverlapPolicyDto, ScheduleApi, ScheduleDto, version_mismatch,
};
use ostro_log::PartitionLog;
use ostro_meta::records::{Schedule, WorkflowVersion};
use ostro_meta::{MetaCommand, MetaError, MetaKey, MetaRecord, MetaResult, MetaView};
use ostro_partition::ApplyOutcome;
use ostro_partition::PartitionHandle;
use ostro_partition::leader::cron::{
    FireRequest, ScheduleSource, ScheduleSpec, StartError, StartFuture, incarnation_watermark,
    next_fire_after, parse_spec, schedule_state, schedule_tag,
};
use ostro_types::command::{
    ConflictPolicy, ScheduleFire, ScheduleFireKind, StartRun, cron_manual_idempotency_key,
};
use ostro_types::routing::schedule_owner;
use ostro_types::{Command, RunId};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::debug;

use super::runs::{DEFAULT_QUEUE, attach_run_started, partition_error};
use super::{ControlCtx, names, policy};
use crate::config::RunsSection;
use crate::retention::{RunPolicy, resolve_run_policy};

/// Wire overlap policy -> meta `OVERLAP_*`.
pub fn overlap_to_meta(p: OverlapPolicyDto) -> u8 {
    p.as_u8()
}

/// Meta `OVERLAP_*` -> wire overlap policy (`Internal` on an out-of-range record).
pub fn overlap_from_meta(v: u8) -> Result<OverlapPolicyDto, ApiError> {
    OverlapPolicyDto::from_u8(v)
        .ok_or_else(|| ApiError::internal(format!("stored schedule has overlap {v}")))
}

/// Wire catch-up policy -> meta `CATCHUP_*`.
pub fn catchup_to_meta(p: CatchupPolicyDto) -> u8 {
    p.as_u8()
}

/// Meta `CATCHUP_*` -> wire catch-up policy (`Internal` on an out-of-range record).
pub fn catchup_from_meta(v: u8) -> Result<CatchupPolicyDto, ApiError> {
    CatchupPolicyDto::from_u8(v)
        .ok_or_else(|| ApiError::internal(format!("stored schedule has catchup {v}")))
}

/// The driver's view of a meta schedule record at version `version`.
pub fn schedule_spec(s: &Schedule, version: u64) -> ScheduleSpec {
    ScheduleSpec {
        name: s.name.clone(),
        workflow: s.workflow.clone(),
        version: s.version,
        cron: s.cron.clone(),
        timezone: s.timezone.clone(),
        input: s.input.clone(),
        paused: s.paused,
        overlap: s.overlap,
        catchup: s.catchup,
        catchup_window_ms: s.catchup_window_ms,
        start_at_ms: s.start_at_ms,
        end_at_ms: s.end_at_ms,
        created_at_ms: s.created_at_ms,
        meta_version: version,
    }
}

fn all_specs(meta: &MetaView) -> Vec<ScheduleSpec> {
    meta.read(|s| {
        s.schedules()
            .map(|v| schedule_spec(&v.value, v.version))
            .collect()
    })
}

// ---------------------------------------------------------------------------------
// Start building (shared by the driver's source and trigger-now)
// ---------------------------------------------------------------------------------

/// The workflow version a fire runs on (pinned, else active).
fn resolve_workflow(
    meta: &MetaView,
    key: &str,
    version: Option<u32>,
) -> Result<WorkflowVersion, String> {
    meta.read(|s| match version {
        Some(v) => s
            .workflow(key, v)
            .map(|w| w.value.clone())
            .ok_or_else(|| format!("workflow {key} version {v} not found")),
        None => match s.active(key) {
            Some(a) => s
                .workflow(key, a.value.version)
                .map(|w| w.value.clone())
                .ok_or_else(|| format!("active version of {key} is not registered")),
            None => Err(format!("workflow {key} has no active version")),
        },
    })
}

/// The `StartRun` of a fire of `spec`, built like an API start (see the module docs).
pub async fn build_schedule_start(
    meta: &MetaView,
    runs: &RunsSection,
    handle: &PartitionHandle<PartitionLog>,
    spec: &ScheduleSpec,
    req: FireRequest,
) -> Result<StartRun, StartError> {
    let skip = |e: ApiError| StartError::Skip(e.message);
    let workflow = policy::policy_key("workflow", &spec.workflow).map_err(skip)?;
    let (tenant, _) = names::split_qualified(&workflow);
    let tenant = tenant.map(str::to_owned);
    let wf = resolve_workflow(meta, &workflow, spec.version).map_err(StartError::Skip)?;
    let local_queue = wf.default_queue.as_deref().unwrap_or(DEFAULT_QUEUE);
    let queue =
        names::qualify_request_name("queue", tenant.as_deref(), local_queue).map_err(skip)?;
    let (retention_ms, schedule_to_start_ms) =
        resolve_run_policy(meta, runs, &workflow, &queue, RunPolicy::default());
    let mut seed = [0u8; 32];
    getrandom::fill(&mut seed)
        .map_err(|e| StartError::Retry(format!("OS random source unavailable: {e}")))?;
    let input_bytes = Bytes::from(spec.input.clone());
    let input = handle
        .prepare_payload(input_bytes.clone())
        .await
        .map_err(|e| StartError::Retry(e.to_string()))?;
    let start = StartRun {
        run_id: req.run.as_bytes().to_vec(),
        workflow,
        workflow_version: wf.version,
        queue,
        input: Some(input),
        idempotency_key: req.idempotency_key.into_bytes(),
        conflict_policy: ConflictPolicy::ReturnExisting as i32,
        concurrency_key: String::new(),
        priority: 0,
        ready_at_ms: req.now_ms,
        requested_at_ms: req.now_ms,
        parent: None,
        rng_seed: seed.to_vec(),
        module_sha: wf.module_sha256.to_vec(),
        tags: vec![schedule_tag(&spec.name)],
        tenant: tenant.unwrap_or_default(),
        traceparent: String::new(),
        run_timeout_ms: wf.run_timeout_ms.unwrap_or(0),
        schedule_to_start_ms,
        retention_ms,
        retry_of: Vec::new(),
        continued_from: Vec::new(),
        run_started: None,
        ..Default::default()
    };
    attach_run_started(handle, start, &input_bytes)
        .await
        .map_err(|e| StartError::Retry(e.message))
}

// ---------------------------------------------------------------------------------
// MetaScheduleSource
// ---------------------------------------------------------------------------------

/// The meta-backed schedule set (see the module docs). Built before `Node::open` and set as
/// `NodeConfig::schedule_source`.
pub struct MetaScheduleSource {
    tx: Arc<watch::Sender<Arc<Vec<ScheduleSpec>>>>,
    meta: MetaView,
    runs: RunsSection,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl std::fmt::Debug for MetaScheduleSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetaScheduleSource").finish_non_exhaustive()
    }
}

impl MetaScheduleSource {
    /// Publish the current schedule set and follow meta from now on (spawns the watcher on
    /// the current tokio runtime; it stops when the source is dropped or meta stops).
    pub fn start(meta: MetaView, runs: RunsSection) -> Arc<Self> {
        let tx = Arc::new(watch::channel(Arc::new(all_specs(&meta))).0);
        let task = {
            let tx = Arc::clone(&tx);
            let meta = meta.clone();
            let mut applied = meta.subscribe();
            tokio::spawn(async move {
                while applied.changed().await.is_ok() {
                    let next = all_specs(&meta);
                    let changed = tx.send_if_modified(|cur| {
                        if **cur == next {
                            false
                        } else {
                            *cur = Arc::new(next);
                            true
                        }
                    });
                    if changed {
                        debug!(
                            schedules = tx.borrow().len(),
                            "schedule set changed; cron drivers notified"
                        );
                    }
                }
            })
        };
        Arc::new(Self {
            tx,
            meta,
            runs,
            task: Mutex::new(Some(task)),
        })
    }

    /// The set currently published.
    pub fn current(&self) -> Arc<Vec<ScheduleSpec>> {
        Arc::clone(&self.tx.borrow())
    }
}

impl Drop for MetaScheduleSource {
    fn drop(&mut self) {
        if let Some(t) = self
            .task
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        {
            t.abort();
        }
    }
}

impl ScheduleSource for MetaScheduleSource {
    fn subscribe(&self) -> watch::Receiver<Arc<Vec<ScheduleSpec>>> {
        self.tx.subscribe()
    }

    fn build_start<'a>(
        &'a self,
        handle: &'a PartitionHandle<PartitionLog>,
        spec: &'a ScheduleSpec,
        req: FireRequest,
    ) -> StartFuture<'a> {
        Box::pin(build_schedule_start(
            &self.meta, &self.runs, handle, spec, req,
        ))
    }
}

// ---------------------------------------------------------------------------------
// Controller
// ---------------------------------------------------------------------------------

/// `ScheduleApi` for the server.
#[derive(Debug)]
pub struct ScheduleController {
    ctx: ControlCtx,
}

fn meta_unavailable(e: MetaError) -> ApiError {
    ApiError::unavailable(format!("metadata unavailable: {e}"))
}

fn not_found(name: &str) -> ApiError {
    ApiError::not_found(format!("schedule {name} not found"))
}

/// Reason of a manual fire skipped by the overlap policy (409).
pub const OVERLAP_SKIPPED: &str = "schedule_overlap";

impl ScheduleController {
    /// Over the shared controller context.
    pub fn new(ctx: ControlCtx) -> Self {
        Self { ctx }
    }

    /// The shared controller context.
    pub fn ctx(&self) -> &ControlCtx {
        &self.ctx
    }

    fn stored(&self, name: &str) -> Option<(u64, Schedule)> {
        self.ctx
            .meta
            .view()
            .read(|s| s.schedule(name).map(|v| (v.version, v.value.clone())))
    }

    /// DTO -> record (validated). `created_at_ms` / `ext` are filled by the caller.
    fn record_from_dto(&self, d: &ScheduleDto) -> Result<Schedule, ApiError> {
        parse_spec(&ScheduleSpec {
            cron: d.cron.clone(),
            timezone: d.timezone.clone(),
            start_at_ms: d.start_at_ms,
            end_at_ms: d.end_at_ms,
            ..ScheduleSpec::default()
        })
        .map_err(|e| ApiError::invalid_argument(e.to_string()))?;
        let key = policy::policy_key("workflow", &d.workflow)
            .map_err(|e| ApiError::invalid_argument(e.message))?;
        let found = self.ctx.meta.view().read(|s| match d.version {
            Some(v) => s.workflow(&key, v).is_some(),
            None => s.workflows().any(|w| w.value.name == key),
        });
        if !found {
            return Err(ApiError::invalid_argument(match d.version {
                Some(v) => format!("workflow {:?} version {v} does not exist", d.workflow),
                None => format!("workflow {:?} does not exist", d.workflow),
            }));
        }
        let input = serde_json::to_vec(&d.input)
            .map_err(|e| ApiError::invalid_argument(format!("input: {e}")))?;
        Ok(Schedule {
            name: d.name.clone(),
            workflow: d.workflow.clone(),
            version: d.version,
            cron: d.cron.clone(),
            input,
            paused: d.paused,
            timezone: d.timezone.clone(),
            overlap: overlap_to_meta(d.overlap),
            catchup: catchup_to_meta(d.catchup),
            catchup_window_ms: d.catchup_window_ms.unwrap_or(0),
            start_at_ms: d.start_at_ms,
            end_at_ms: d.end_at_ms,
            created_at_ms: 0,
            ext: Vec::new(),
        })
    }

    /// The durable watermark of `s`'s incarnation (`None` when nothing fired yet).
    async fn last_fire(&self, s: &Schedule) -> Option<u64> {
        let owner = schedule_owner(&s.name, self.ctx.partitions.partition_count());
        let reader = self.ctx.partitions.partition(owner).ok()?.reader();
        let name = s.name.clone();
        let row = tokio::task::spawn_blocking(move || schedule_state(&reader, &name))
            .await
            .ok()?
            .ok()?;
        Some(incarnation_watermark(row.as_ref(), s.created_at_ms)).filter(|w| *w > 0)
    }

    async fn to_dto(&self, s: &Schedule, version: u64) -> Result<ScheduleDto, ApiError> {
        let last = self.last_fire(s).await;
        let next = if s.paused {
            None
        } else {
            parse_spec(&schedule_spec(s, version)).ok().and_then(|c| {
                let floor = last.unwrap_or(0).max(s.created_at_ms);
                next_fire_after(&c, floor.max(self.ctx.now_ms()))
            })
        };
        Ok(ScheduleDto {
            name: s.name.clone(),
            workflow: s.workflow.clone(),
            version: s.version,
            cron: s.cron.clone(),
            timezone: s.timezone.clone(),
            input: serde_json::from_slice(&s.input).unwrap_or(serde_json::Value::Null),
            paused: s.paused,
            overlap: overlap_from_meta(s.overlap)?,
            catchup: catchup_from_meta(s.catchup)?,
            catchup_window_ms: Some(s.catchup_window_ms).filter(|w| *w > 0),
            start_at_ms: s.start_at_ms,
            end_at_ms: s.end_at_ms,
            version_tag: Some(version),
            next_fire_at_ms: next,
            last_fire_at_ms: last,
        })
    }

    /// Store `s` with CAS `expected` (None = create).
    async fn put(&self, s: Schedule, expected: Option<u64>) -> Result<ScheduleDto, ApiError> {
        let name = s.name.clone();
        let res = self
            .ctx
            .meta
            .propose(MetaCommand::Put {
                key: MetaKey::Schedule(name.clone()),
                expected_version: expected,
                record: MetaRecord::Schedule(s.clone()),
            })
            .await
            .map_err(meta_unavailable)?;
        match res {
            MetaResult::Ok { version } => self.to_dto(&s, version).await,
            MetaResult::Conflict { .. } => Err(ApiError::conflict(format!(
                "schedule {name} already exists"
            ))),
            MetaResult::StaleVersion { current } => {
                Err(version_mismatch("schedule", &name, current))
            }
            MetaResult::NotFound => Err(not_found(&name)),
            MetaResult::Invalid(m) => Err(ApiError::invalid_argument(format!(
                "metadata rejected the schedule: {m}"
            ))),
        }
    }

    /// Pause / resume: CAS read-modify-write of `paused`, retried on a concurrent change.
    async fn set_paused(&self, name: &str, paused: bool) -> Result<ScheduleDto, ApiError> {
        for _ in 0..8 {
            let (version, mut s) = self.stored(name).ok_or_else(|| not_found(name))?;
            if s.paused == paused {
                return self.to_dto(&s, version).await;
            }
            s.paused = paused;
            match self.put(s, Some(version)).await {
                Err(e) if is_version_mismatch(&e) => continue,
                r => return r,
            }
        }
        Err(ApiError::unavailable(format!(
            "schedule {name} keeps changing; retry"
        )))
    }
}

fn is_version_mismatch(e: &ApiError) -> bool {
    e.details
        .as_ref()
        .and_then(|d| d.get("reason"))
        .and_then(|r| r.as_str())
        == Some(ostro_api::phase8::reason::VERSION_MISMATCH)
}

#[async_trait::async_trait]
impl ScheduleApi for ScheduleController {
    async fn list_schedules(&self) -> Result<Vec<ScheduleDto>, ApiError> {
        let all: Vec<(u64, Schedule)> = self.ctx.meta.view().read(|s| {
            s.schedules()
                .map(|v| (v.version, v.value.clone()))
                .collect()
        });
        let mut out = Vec::with_capacity(all.len());
        for (v, s) in &all {
            out.push(self.to_dto(s, *v).await?);
        }
        Ok(out)
    }

    async fn get_schedule(&self, name: String) -> Result<ScheduleDto, ApiError> {
        let (v, s) = self.stored(&name).ok_or_else(|| not_found(&name))?;
        self.to_dto(&s, v).await
    }

    async fn create_schedule(&self, d: ScheduleDto) -> Result<ScheduleDto, ApiError> {
        let mut s = self.record_from_dto(&d)?;
        s.created_at_ms = self.ctx.now_ms();
        self.put(s, None).await
    }

    async fn update_schedule(&self, d: ScheduleDto) -> Result<ScheduleDto, ApiError> {
        let (current, old) = self.stored(&d.name).ok_or_else(|| not_found(&d.name))?;
        if let Some(t) = d.version_tag
            && t != current
        {
            return Err(version_mismatch("schedule", &d.name, current));
        }
        let mut s = self.record_from_dto(&d)?;
        // Same incarnation: the watermark (exactly-once) carries over (W4).
        s.created_at_ms = old.created_at_ms;
        s.ext = old.ext;
        self.put(s, Some(d.version_tag.unwrap_or(current))).await
    }

    async fn delete_schedule(
        &self,
        name: String,
        version_tag: Option<u64>,
    ) -> Result<(), ApiError> {
        let (current, _) = self.stored(&name).ok_or_else(|| not_found(&name))?;
        if let Some(t) = version_tag
            && t != current
        {
            return Err(version_mismatch("schedule", &name, current));
        }
        let res = self
            .ctx
            .meta
            .propose(MetaCommand::Delete {
                key: MetaKey::Schedule(name.clone()),
                expected_version: version_tag.unwrap_or(current),
            })
            .await
            .map_err(meta_unavailable)?;
        match res {
            MetaResult::Ok { .. } => Ok(()),
            MetaResult::NotFound => Err(not_found(&name)),
            MetaResult::StaleVersion { current } | MetaResult::Conflict { current } => {
                Err(version_mismatch("schedule", &name, current))
            }
            MetaResult::Invalid(m) => Err(ApiError::invalid_argument(m)),
        }
    }

    async fn pause_schedule(&self, name: String) -> Result<ScheduleDto, ApiError> {
        self.set_paused(&name, true).await
    }

    async fn resume_schedule(&self, name: String) -> Result<ScheduleDto, ApiError> {
        self.set_paused(&name, false).await
    }

    async fn trigger_schedule(
        &self,
        name: String,
        request_id: Option<String>,
    ) -> Result<String, ApiError> {
        let (version, s) = self.stored(&name).ok_or_else(|| not_found(&name))?;
        let spec = schedule_spec(&s, version);
        let request_id = match request_id {
            Some(r) => r.into_bytes(),
            None => {
                let mut b = [0u8; 16];
                getrandom::fill(&mut b).map_err(|e| {
                    ApiError::internal(format!("OS random source unavailable: {e}"))
                })?;
                b.to_vec()
            }
        };
        let cp = &self.ctx.partitions;
        let owner = schedule_owner(&name, cp.partition_count());
        let handle = &cp.partition(owner)?;
        let run = RunId::new_v7(owner);
        let now = self.ctx.now_ms();
        let start = build_schedule_start(
            self.ctx.meta.view(),
            &self.ctx.config.runs,
            handle,
            &spec,
            FireRequest {
                run,
                idempotency_key: cron_manual_idempotency_key(&name, &request_id),
                fire_time_ms: now,
                now_ms: now,
            },
        )
        .await
        .map_err(|e| match e {
            StartError::Skip(m) => ApiError::precondition_failed(m),
            StartError::Retry(m) => ApiError::unavailable(m),
        })?;
        let cmd = ScheduleFire {
            schedule: name.clone(),
            fire_time_ms: now,
            kind: ScheduleFireKind::Manual as i32,
            start: Some(start),
            overlap: u32::from(s.overlap),
            request_id,
            skipped: 0,
            schedule_created_at_ms: s.created_at_ms,
        };
        let outcome = handle
            .submit_one(Command::schedule_fire(cmd))
            .await
            .map_err(partition_error)?;
        match outcome {
            ApplyOutcome::Accepted => {
                let reader = handle.reader();
                let n = name.clone();
                let row = tokio::task::spawn_blocking(move || schedule_state(&reader, &n))
                    .await
                    .map_err(super::runs::task_error)?
                    .map_err(super::runs::store_error)?;
                let started = row.as_ref().is_some_and(|r| r.last_run == Some(run));
                let buffered = row
                    .as_ref()
                    .and_then(|r| r.buffered.as_ref())
                    .is_some_and(|b| b.fire_time_ms == now);
                if started || buffered {
                    Ok(run.to_string())
                } else {
                    Err(ApiError::conflict(format!(
                        "schedule {name}: the previous run is still running and the overlap policy skips this fire"
                    ))
                    .with_details(serde_json::json!({ "reason": OVERLAP_SKIPPED })))
                }
            }
            ApplyOutcome::Duplicate {
                existing: Some(existing),
            } => Ok(existing.to_string()),
            ApplyOutcome::Duplicate { existing: None } => Err(ApiError::conflict(format!(
                "schedule {name} changed while triggering; retry"
            ))),
            ApplyOutcome::Rejected(r) => Err(partition_error(
                ostro_partition::PartitionError::Rejected(r),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ostro_meta::records::{
        CATCHUP_ALL, CATCHUP_ONE, CATCHUP_SKIP, OVERLAP_ALLOW_ALL, OVERLAP_BUFFER_ONE,
        OVERLAP_CANCEL_OTHER, OVERLAP_SKIP, OVERLAP_TERMINATE_OTHER,
    };
    use ostro_triggers::cron::{CatchupPolicy, OverlapPolicy};

    #[test]
    fn overlap_mapping_matches_meta_and_triggers() {
        for (dto, meta, trig) in [
            (OverlapPolicyDto::Skip, OVERLAP_SKIP, OverlapPolicy::Skip),
            (
                OverlapPolicyDto::BufferOne,
                OVERLAP_BUFFER_ONE,
                OverlapPolicy::BufferOne,
            ),
            (
                OverlapPolicyDto::AllowAll,
                OVERLAP_ALLOW_ALL,
                OverlapPolicy::AllowAll,
            ),
            (
                OverlapPolicyDto::CancelOther,
                OVERLAP_CANCEL_OTHER,
                OverlapPolicy::CancelOther,
            ),
            (
                OverlapPolicyDto::TerminateOther,
                OVERLAP_TERMINATE_OTHER,
                OverlapPolicy::TerminateOther,
            ),
        ] {
            assert_eq!(overlap_to_meta(dto), meta);
            assert_eq!(overlap_from_meta(meta).expect("known"), dto);
            assert_eq!(OverlapPolicy::try_from(meta).expect("triggers"), trig);
        }
        assert!(overlap_from_meta(5).is_err());
    }

    #[test]
    fn catchup_mapping_matches_meta_and_triggers() {
        for (dto, meta, trig) in [
            (CatchupPolicyDto::Skip, CATCHUP_SKIP, CatchupPolicy::Skip),
            (CatchupPolicyDto::One, CATCHUP_ONE, CatchupPolicy::One),
            (CatchupPolicyDto::All, CATCHUP_ALL, CatchupPolicy::All),
        ] {
            assert_eq!(catchup_to_meta(dto), meta);
            assert_eq!(catchup_from_meta(meta).expect("known"), dto);
            assert_eq!(CatchupPolicy::try_from(meta).expect("triggers"), trig);
        }
        assert!(catchup_from_meta(3).is_err());
    }

    #[test]
    fn spec_copies_every_record_field() {
        let s = Schedule {
            name: "n".into(),
            workflow: "w".into(),
            version: Some(3),
            cron: "*/5 * * * * *".into(),
            input: b"{}".to_vec(),
            paused: true,
            timezone: "Europe/Berlin".into(),
            overlap: OVERLAP_BUFFER_ONE,
            catchup: CATCHUP_ALL,
            catchup_window_ms: 60_000,
            start_at_ms: Some(1),
            end_at_ms: Some(2),
            created_at_ms: 77,
            ext: Vec::new(),
        };
        let spec = schedule_spec(&s, 9);
        assert_eq!(
            spec,
            ScheduleSpec {
                name: "n".into(),
                workflow: "w".into(),
                version: Some(3),
                cron: "*/5 * * * * *".into(),
                timezone: "Europe/Berlin".into(),
                input: b"{}".to_vec(),
                paused: true,
                overlap: OVERLAP_BUFFER_ONE,
                catchup: CATCHUP_ALL,
                catchup_window_ms: 60_000,
                start_at_ms: Some(1),
                end_at_ms: Some(2),
                created_at_ms: 77,
                meta_version: 9,
            }
        );
    }
}
