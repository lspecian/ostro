//! Node-local bulk operation jobs (Phase 11, OPS-03, D-P11-20). Declared by 11-04
//! (D-P11-23); implemented by **11-13**.
//!
//! The one [`BulkRegistry`] per node is built by `p11::wire` and held as
//! `P11Ctx::bulk`; the bulk controllers (`control::p11_bulk`) and the service task
//! started here ([`spawn`], through `Services::attach_p11`) share that `Arc`.
//!
//! # A job
//!
//! `POST /v1/bulk` creates `blk_<uuidv7>`: an action (`cancel | terminate | retry | rerun |
//! requeue | pause | resume | signal`) over a `ListRunsRequest`-style filter.
//!
//! - **Snapshot bound.** The selection always carries `created_before_ms =
//!   job.created_at_ms` (or the request's earlier bound), so runs the job itself creates
//!   (restarts, reruns) are never selected. A selected run whose record shows it was created
//!   by this job (dedupe key containing `__bulk/<job>/`, or `retry_of` a run the job already
//!   processed) is skipped as well.
//! - **Pinned cursor.** At creation the cursor mode is pinned: `index` when every
//!   partition's run index is `Ready` (newest first through
//!   [`crate::control::p11_search::list_runs_indexed`]), else `scan` (oldest first through
//!   the control plane's scan). The mode is stored in the job file and never changes: a job
//!   pinned to `index` that finds an index `Rebuilding` waits for `Ready`.
//! - **Processed set.** Every run id the job finished (done, skipped or failed) is recorded
//!   and persisted with the checkpoint; a run already in the set is never operated on again,
//!   even if a resumed cursor re-delivers it.
//! - **Pacing.** Each op is admitted by a private [`Pacer`] over Phase 7's hand-rolled GCRA
//!   ([`crate::flow::limits::Gcra`]; rate `rate_per_sec` per 1 s, burst `[bulk] burst`),
//!   with at most `max_in_flight` ops running at once. Time comes only from the injected
//!   clock; waits use the runtime timer.
//! - **Backpressure.** Before an op, the target partition's ready backlog (the store's
//!   `ready` rows, the accessor `/v1/queues` uses) is compared with `[bulk] backlog_high`;
//!   while it is higher the job is `paused_backpressure`. A 429 `rate_limited` from a start
//!   (restart / rerun / requeue) waits `retry_after_ms` and retries the same run; it is not a
//!   failure.
//! - **Per-run idempotency.** cancel / terminate / pause / resume / retry-resume are natural
//!   (409 on a run no longer eligible counts as `skipped`); signal uses `request_id =
//!   <job>/<run>` (sha-shortened to the 64-byte cap, [`signal_request_id`]), and a duplicate
//!   counts as done; restart and rerun use dedupe `__bulk/<job>/<run>` (a rerun's key is
//!   wrapped by 11-10 as `__rerun/<run>/__bulk/<job>/<run>`); requeue writes the DLQ mark.
//!
//! # Job file
//!
//! `<data>/bulk/<job>.json` ([`JobFile`], `version: 1`), written tmp + fsync + rename (+ a
//! directory fsync) at creation, on state changes, and while running every `[bulk]
//! checkpoint_every` (1 s) or `checkpoint_ops` (500) finished ops. It is node-local, not
//! part of any snapshot or backup. Unfinished jobs (`pending`, `running`,
//! `paused_backpressure`) resume from their cursor at service start ([`BulkRegistry::
//! resume_all`]); finished jobs stay listed. Files of another version are ignored with a
//! WARN.
//!
//! # Failpoints and metrics
//!
//! `bulk::after_op` (after an op's outcome is recorded) and `bulk::before_checkpoint`
//! (before a running job's checkpoint write) simulate a crash when they fire: the job's
//! runner stops at once, without writing anything further. Metrics:
//! `ostro_bulk_ops_total{action,result}` (`done | skipped | failed | rate_limited`) and
//! `ostro_bulk_jobs{state}`.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::Duration;

use bytes::Bytes;
use ostro_api::domain::{
    CancelRequest, ListOrder, ListRunsRequest, RetryRequest, RunPage, SignalRequest,
    TerminateRequest,
};
use ostro_api::phase11::{
    BulkAction, BulkCreateRequest, BulkCreateResponse, BulkFailure, BulkFilter, BulkJobView,
    DlqRequeueRequest, RerunRequest, bulk_too_many_jobs, reason, reason_error,
};
use ostro_api::{ApiError, ControlPlane, ErrorCode};
use ostro_meta::RateSpec;
use ostro_partition::store::derived::IndexState;
use ostro_partition::store::values::{RunRecord, RunStatus};
use ostro_partition::{Clock, query};
use ostro_types::command::{self as cmd, StartRun};
use ostro_types::routing::partition_for_key;
use ostro_types::{PartitionId, PayloadRef, RunId};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::{Semaphore, mpsc};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::ServiceCtx;
use crate::config::BulkSection;
use crate::control::lifecycle::read_record;
use crate::control::runs::{store_error, task_error};
use crate::control::{p11_dlq, p11_rerun, p11_search};
use crate::flow::limits::{Gcra, ms_to_ns, ns_to_ms_ceil};
use crate::onfailure;
use crate::p11::P11Ctx;

/// Job file format version.
pub const JOB_FILE_VERSION: u32 = 1;
/// Subdirectory of the node data directory holding the job files.
pub const BULK_DIR: &str = "bulk";
/// Job id prefix.
pub const JOB_ID_PREFIX: &str = "blk_";
/// Dedupe-key prefix of runs a job creates (`__bulk/<job>/<run>`).
pub const DEDUPE_PREFIX: &str = "__bulk/";
/// Runs read per selection page.
pub const SELECTION_PAGE: u32 = 500;
/// Failures kept with reasons (the count goes on).
pub const MAX_FAILURES: usize = 100;
/// Samples returned by a dry run.
pub const MAX_SAMPLES: usize = 20;
/// Highest accepted `max_in_flight`.
pub const MAX_IN_FLIGHT_CAP: u32 = 64;
/// How long a cached backlog reading is trusted.
const BACKLOG_TTL_MS: u64 = 250;
/// Wait between backlog re-checks while paused, and between selection retries.
const RETRY_WAIT: Duration = Duration::from_millis(250);
/// `Unavailable` answers an op retries before it counts as failed.
const UNAVAILABLE_RETRIES: u32 = 20;
/// `retry_after_ms` assumed when a 429 carries none.
const DEFAULT_RETRY_AFTER_MS: u64 = 1_000;

// ---------------------------------------------------------------------------------
// Pacing
// ---------------------------------------------------------------------------------

/// A private GCRA limiter for one job's op starts (Phase 7's [`Gcra`], D-W24): `rate` ops
/// per 1 s period with burst `burst`, time from the injected clock.
pub struct Pacer {
    gcra: Gcra,
    tat_ns: u64,
    clock: Arc<dyn Clock>,
}

impl std::fmt::Debug for Pacer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pacer")
            .field("gcra", &self.gcra)
            .field("tat_ns", &self.tat_ns)
            .finish_non_exhaustive()
    }
}

impl Pacer {
    /// `rate` and `burst` are normalised to at least 1.
    pub fn new(rate_per_sec: u32, burst: u32, clock: Arc<dyn Clock>) -> Self {
        let spec = RateSpec {
            limit: rate_per_sec.max(1),
            period_ms: 1_000,
            burst: burst.max(1),
        };
        let gcra = Gcra::from_spec(&spec).unwrap_or(Gcra {
            t_ns: 1_000_000_000,
            tau_ns: 0,
        });
        Self {
            gcra,
            tat_ns: 0,
            clock,
        }
    }

    /// `None`: the op may start now (charged). `Some(d)`: wait `d`, then ask again.
    pub fn try_admit(&mut self) -> Option<Duration> {
        let now = ms_to_ns(self.clock.now_ms());
        match self.gcra.conforms(self.tat_ns, now) {
            Ok(()) => {
                self.tat_ns = self.gcra.advance(self.tat_ns, now);
                None
            }
            Err(at) => Some(Duration::from_millis(
                ns_to_ms_ceil(at.saturating_sub(now)).max(1),
            )),
        }
    }

    /// Wait (runtime timer) until an op may start; `false` when `stop` fired first.
    pub async fn admit(&mut self, stop: &CancellationToken) -> bool {
        loop {
            match self.try_admit() {
                None => return true,
                Some(d) => {
                    tokio::select! {
                        biased;
                        _ = stop.cancelled() => return false,
                        _ = tokio::time::sleep(d) => {}
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------------
// Job file and state
// ---------------------------------------------------------------------------------

/// `<data>/bulk/<job>.json` (format version 1).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct JobFile {
    /// [`JOB_FILE_VERSION`].
    pub version: u32,
    /// The creating request.
    pub spec: BulkCreateRequest,
    /// `blk_<uuidv7>`.
    pub job_id: String,
    /// Creation time (injected clock); the snapshot bound.
    pub created_at_ms: u64,
    /// `pending` | `running` | `paused_backpressure` | `done` | `cancelled` | `failed`.
    pub state: String,
    /// Pinned at creation: `index` or `scan`; never changes on resume.
    pub cursor_mode: String,
    /// Selection page token (in `cursor_mode`) after the current page.
    pub cursor: Option<String>,
    /// Runs of the current page not finished yet (including in-flight ones).
    pub pending_in_page: Vec<String>,
    /// Run ids done, skipped or failed (the per-job processed set).
    pub processed: Vec<String>,
    /// Ops applied.
    pub done: u64,
    /// Runs no longer eligible.
    pub skipped: u64,
    /// The first [`MAX_FAILURES`] failures.
    pub failed: Vec<BulkFailure>,
    /// Every failure.
    pub failed_count: u64,
    /// Runs matched at creation.
    pub matched: u64,
    /// Effective ops per second.
    pub rate_per_sec: u32,
    /// Effective concurrent ops.
    pub max_in_flight: u32,
    /// Every selection page was read.
    #[serde(default)]
    pub selection_done: bool,
    /// First start of the runner.
    #[serde(default)]
    pub started_at_ms: Option<u64>,
    /// Finish (done / cancelled / failed).
    #[serde(default)]
    pub finished_at_ms: Option<u64>,
}

fn is_active(state: &str) -> bool {
    matches!(state, "pending" | "running" | "paused_backpressure")
}

/// The action's wire name.
pub fn action_name(a: &BulkAction) -> &'static str {
    match a {
        BulkAction::Cancel => "cancel",
        BulkAction::Terminate { .. } => "terminate",
        BulkAction::Retry => "retry",
        BulkAction::Rerun { .. } => "rerun",
        BulkAction::Requeue { .. } => "requeue",
        BulkAction::Pause => "pause",
        BulkAction::Resume => "resume",
        BulkAction::Signal { .. } => "signal",
    }
}

impl JobFile {
    fn view(&self) -> BulkJobView {
        let finished = self.done + self.skipped + self.failed_count;
        let eta_ms = is_active(&self.state).then(|| {
            let remaining = self.matched.saturating_sub(finished);
            remaining.saturating_mul(1_000) / u64::from(self.rate_per_sec.max(1))
        });
        BulkJobView {
            job_id: self.job_id.clone(),
            state: self.state.clone(),
            action: action_name(&self.spec.action).to_owned(),
            matched: self.matched,
            done: self.done,
            skipped: self.skipped,
            failed: self.failed.clone(),
            failed_count: self.failed_count,
            rate_per_sec: self.rate_per_sec,
            created_at_ms: self.created_at_ms,
            started_at_ms: self.started_at_ms,
            finished_at_ms: self.finished_at_ms,
            eta_ms,
        }
    }
}

/// In-memory job state (the file plus derived sets).
struct JobState {
    file: JobFile,
    processed: HashSet<String>,
    /// Not yet dispatched (front = next).
    queue: VecDeque<String>,
    /// Dispatched, outcome pending.
    in_flight: BTreeSet<String>,
    dirty: bool,
}

impl JobState {
    fn from_file(file: JobFile) -> Self {
        let processed = file.processed.iter().cloned().collect();
        let queue = file.pending_in_page.iter().cloned().collect();
        Self {
            file,
            processed,
            queue,
            in_flight: BTreeSet::new(),
            dirty: false,
        }
    }

    /// The file as it is checkpointed (pending = in flight + queued).
    fn snapshot(&self) -> JobFile {
        let mut f = self.file.clone();
        f.pending_in_page = self
            .in_flight
            .iter()
            .cloned()
            .chain(self.queue.iter().cloned())
            .collect();
        f
    }
}

struct Job {
    id: String,
    state: Mutex<JobState>,
    /// Serialises file writes (the latest state is written under it).
    write: tokio::sync::Mutex<()>,
    /// `POST /v1/bulk/{id}/cancel`.
    cancel: CancellationToken,
    /// A runner is attached (at most one).
    running: AtomicBool,
}

impl Job {
    fn new(file: JobFile) -> Arc<Self> {
        Arc::new(Self {
            id: file.job_id.clone(),
            state: Mutex::new(JobState::from_file(file)),
            write: tokio::sync::Mutex::new(()),
            cancel: CancellationToken::new(),
            running: AtomicBool::new(false),
        })
    }

    fn lock(&self) -> MutexGuard<'_, JobState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn view(&self) -> BulkJobView {
        self.lock().file.view()
    }
}

// ---------------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------------

/// The node's bulk jobs.
pub struct BulkRegistry {
    data_dir: PathBuf,
    cfg: BulkSection,
    clock: Arc<dyn Clock>,
    jobs: Mutex<BTreeMap<String, Arc<Job>>>,
    /// The service task's job queue (set by [`spawn`]).
    tx: OnceLock<mpsc::UnboundedSender<Arc<Job>>>,
    resumed: AtomicBool,
    /// `true` once [`BulkRegistry::resume_all`] has reloaded `<data>/bulk/` (11-18): the
    /// API waits for it, so a listing right after a restart is never missing a job.
    loaded: tokio::sync::watch::Sender<bool>,
}

/// Longest wait of an API call for the initial job reload (it reads a few small files).
const LOAD_WAIT: std::time::Duration = std::time::Duration::from_secs(10);

impl std::fmt::Debug for BulkRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BulkRegistry")
            .field("data_dir", &self.data_dir)
            .field("cfg", &self.cfg)
            .finish_non_exhaustive()
    }
}

impl BulkRegistry {
    /// Over the node data directory (`data_dir`; the jobs live in `<data_dir>/bulk/`),
    /// `[bulk]` and the injected clock. Touches no disk.
    pub fn new(data_dir: PathBuf, cfg: BulkSection, clock: Arc<dyn Clock>) -> Arc<Self> {
        Arc::new(Self {
            data_dir,
            cfg,
            clock,
            jobs: Mutex::new(BTreeMap::new()),
            tx: OnceLock::new(),
            resumed: AtomicBool::new(false),
            loaded: tokio::sync::watch::channel(false).0,
        })
    }

    /// The node data directory.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// `<data_dir>/bulk`.
    pub fn bulk_dir(&self) -> PathBuf {
        self.data_dir.join(BULK_DIR)
    }

    /// `<data_dir>/bulk/<job>.json`.
    pub fn job_path(&self, job_id: &str) -> PathBuf {
        self.bulk_dir().join(format!("{job_id}.json"))
    }

    /// `[bulk]`.
    pub fn config(&self) -> &BulkSection {
        &self.cfg
    }

    /// The injected clock (every pacing / checkpoint time read goes through it).
    pub fn clock(&self) -> &Arc<dyn Clock> {
        &self.clock
    }

    fn jobs(&self) -> MutexGuard<'_, BTreeMap<String, Arc<Job>>> {
        self.jobs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn active_jobs(&self) -> usize {
        self.jobs()
            .values()
            .filter(|j| is_active(&j.lock().file.state))
            .count()
    }

    fn too_many_jobs(&self) -> ApiError {
        bulk_too_many_jobs(u64::from(self.cfg.max_jobs), 1_000)
    }

    fn publish_job_gauges(&self) {
        let mut by: BTreeMap<&'static str, f64> = [
            "pending",
            "running",
            "paused_backpressure",
            "done",
            "cancelled",
            "failed",
        ]
        .into_iter()
        .map(|s| (s, 0.0))
        .collect();
        for j in self.jobs().values() {
            let st = j.lock().file.state.clone();
            if let Some(n) = by.iter_mut().find(|(k, _)| **k == st).map(|(_, v)| v) {
                *n += 1.0;
            }
        }
        for (state, n) in by {
            metrics::gauge!("ostro_bulk_jobs", "state" => state).set(n);
        }
    }

    /// Create a job (or count it on a dry run).
    pub async fn create(
        &self,
        ctx: &P11Ctx,
        req: BulkCreateRequest,
    ) -> Result<BulkCreateResponse, ApiError> {
        let rate = req.rate_per_sec.unwrap_or(self.cfg.default_rate);
        if rate == 0 {
            return Err(ApiError::invalid_argument(
                "rate_per_sec must be at least 1",
            ));
        }
        let rate = rate.min(self.cfg.max_rate.max(1));
        let in_flight = req
            .max_in_flight
            .unwrap_or(self.cfg.max_in_flight)
            .clamp(1, MAX_IN_FLIGHT_CAP);
        if !req.dry_run && self.active_jobs() >= self.cfg.max_jobs as usize {
            return Err(self.too_many_jobs());
        }
        let created_at_ms = self.clock.now_ms();
        let mode = if index_ready(ctx).await {
            CursorMode::Index
        } else {
            CursorMode::Scan
        };
        let max = self.cfg.max_matches;
        let (matched, sample) = count_matches(ctx, &req.filter, created_at_ms, mode, max).await?;
        if matched > max {
            return Err(reason_error(
                ErrorCode::InvalidArgument,
                reason::BULK_TOO_MANY_MATCHES,
                format!("the filter matches more than bulk.max_matches ({max}) runs"),
                serde_json::json!({
                    "limit": "bulk_matches",
                    "max": max,
                    "actual": matched,
                }),
            ));
        }
        if req.dry_run {
            return Ok(BulkCreateResponse {
                job_id: None,
                matched_estimate: matched,
                sample,
            });
        }
        let Some(tx) = self.tx.get() else {
            return Err(ApiError::unavailable("the bulk service is not running"));
        };
        let job_id = format!("{JOB_ID_PREFIX}{}", uuid::Uuid::now_v7());
        let file = JobFile {
            version: JOB_FILE_VERSION,
            spec: BulkCreateRequest {
                rate_per_sec: Some(rate),
                max_in_flight: Some(in_flight),
                ..req
            },
            job_id: job_id.clone(),
            created_at_ms,
            state: "pending".to_owned(),
            cursor_mode: mode.as_str().to_owned(),
            cursor: None,
            pending_in_page: Vec::new(),
            processed: Vec::new(),
            done: 0,
            skipped: 0,
            failed: Vec::new(),
            failed_count: 0,
            matched,
            rate_per_sec: rate,
            max_in_flight: in_flight,
            selection_done: false,
            started_at_ms: None,
            finished_at_ms: None,
        };
        let job = Job::new(file);
        {
            let mut jobs = self.jobs();
            let active = jobs
                .values()
                .filter(|j| is_active(&j.lock().file.state))
                .count();
            if active >= self.cfg.max_jobs as usize {
                return Err(self.too_many_jobs());
            }
            jobs.insert(job_id.clone(), Arc::clone(&job));
        }
        if let Err(e) = self.write_job(&job).await {
            self.jobs().remove(&job_id);
            return Err(ApiError::internal(format!("bulk job file: {e}")));
        }
        if tx.send(Arc::clone(&job)).is_err() {
            self.jobs().remove(&job_id);
            return Err(ApiError::unavailable("the bulk service is shutting down"));
        }
        self.publish_job_gauges();
        info!(job = %job_id, matched, rate, mode = mode.as_str(), "bulk job created");
        Ok(BulkCreateResponse {
            job_id: Some(job_id),
            matched_estimate: matched,
            sample,
        })
    }

    /// Wait until the persisted jobs are reloaded ([`BulkRegistry::resume_all`]), at most
    /// [`LOAD_WAIT`]. Returns at once when no bulk service runs (nothing will reload) or
    /// the reload is done. The bulk API calls this before every create / list / get /
    /// cancel, so they see the jobs of the previous run as soon as the server is ready.
    pub async fn wait_loaded(&self) {
        if self.tx.get().is_none() {
            return;
        }
        let mut rx = self.loaded.subscribe();
        if tokio::time::timeout(LOAD_WAIT, rx.wait_for(|done| *done))
            .await
            .is_err()
        {
            warn!("bulk job reload still running after {LOAD_WAIT:?}; answering without it");
        }
    }

    /// Whether the initial reload is done (tests).
    pub fn is_loaded(&self) -> bool {
        *self.loaded.borrow()
    }

    /// Every job on this node (creation order).
    pub fn list(&self) -> Vec<BulkJobView> {
        let jobs: Vec<Arc<Job>> = self.jobs().values().cloned().collect();
        jobs.iter().map(|j| j.view()).collect()
    }

    /// One job.
    pub fn get(&self, id: &str) -> Option<BulkJobView> {
        let job = self.jobs().get(id).cloned();
        job.map(|j| j.view())
    }

    /// Cancel a job: no new op starts (the runner stops within one op); a finished job is
    /// returned unchanged.
    pub async fn cancel(&self, id: &str) -> Result<BulkJobView, ApiError> {
        let job = self
            .jobs()
            .get(id)
            .cloned()
            .ok_or_else(|| ApiError::not_found(format!("bulk job {id} not found")))?;
        let changed = {
            let mut st = job.lock();
            if is_active(&st.file.state) {
                st.file.state = "cancelled".to_owned();
                st.file.finished_at_ms = Some(self.clock.now_ms());
                st.dirty = true;
                true
            } else {
                false
            }
        };
        if changed {
            job.cancel.cancel();
            if let Err(e) = self.write_job(&job).await {
                warn!(job = %id, error = %e, "bulk job file not written on cancel");
            }
            self.publish_job_gauges();
        }
        Ok(job.view())
    }

    /// Reload `<data>/bulk/*.json` (once): every job is listed again; jobs in state
    /// `pending`, `running` or `paused_backpressure` are handed to the runner and resume
    /// from their cursor. Files of another version or that do not parse are ignored (WARN).
    pub async fn resume_all(&self, ctx: &P11Ctx) {
        let _ = ctx;
        if self.resumed.swap(true, Ordering::SeqCst) {
            return;
        }
        let dir = self.bulk_dir();
        let files = match tokio::task::spawn_blocking(move || load_dir(&dir)).await {
            Ok(f) => f,
            Err(e) => {
                warn!(error = %e, "bulk job reload task failed");
                self.loaded.send_replace(true);
                return;
            }
        };
        let mut resumed = 0usize;
        for file in files {
            let id = file.job_id.clone();
            let active = is_active(&file.state);
            let job = Job::new(file);
            if active {
                job.lock().file.state = "pending".to_owned();
            }
            self.jobs().insert(id.clone(), Arc::clone(&job));
            if active {
                match self.tx.get() {
                    Some(tx) if tx.send(job).is_ok() => resumed += 1,
                    _ => warn!(job = %id, "bulk job not resumed: the service is not running"),
                }
            }
        }
        if resumed > 0 {
            info!(resumed, "bulk jobs resumed");
        }
        self.loaded.send_replace(true);
        self.publish_job_gauges();
    }

    /// Write `job`'s current state (tmp + fsync + rename + dir fsync).
    async fn write_job(&self, job: &Arc<Job>) -> std::io::Result<()> {
        let _w = job.write.lock().await;
        let bytes = {
            let mut st = job.lock();
            st.dirty = false;
            serde_json::to_vec_pretty(&st.snapshot()).map_err(std::io::Error::other)?
        };
        let dir = self.bulk_dir();
        let id = job.id.clone();
        tokio::task::spawn_blocking(move || write_atomic(&dir, &id, &bytes))
            .await
            .map_err(std::io::Error::other)?
    }
}

/// Write `<dir>/<id>.json` through `<id>.json.tmp`: write, fsync, rename, fsync the
/// directory.
fn write_atomic(dir: &Path, id: &str, bytes: &[u8]) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    let tmp = dir.join(format!("{id}.json.tmp"));
    let dst = dir.join(format!("{id}.json"));
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, &dst)?;
    std::fs::File::open(dir)?.sync_all()
}

/// The version-1 job files of `dir` (creation order); leftovers `*.json.tmp` are removed.
fn load_dir(dir: &Path) -> Vec<JobFile> {
    let rd = match std::fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Vec::new(),
        Err(e) => {
            warn!(dir = %dir.display(), error = %e, "bulk job directory unreadable");
            return Vec::new();
        }
    };
    let mut out = Vec::new();
    for entry in rd.flatten() {
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.ends_with(".json.tmp") {
            let _ = std::fs::remove_file(&path);
            continue;
        }
        if !name.ends_with(".json") {
            continue;
        }
        let bytes = match std::fs::read(&path) {
            Ok(b) => b,
            Err(e) => {
                warn!(file = %path.display(), error = %e, "bulk job file unreadable; ignored");
                continue;
            }
        };
        let version = serde_json::from_slice::<serde_json::Value>(&bytes)
            .ok()
            .and_then(|v| v.get("version").and_then(serde_json::Value::as_u64));
        if version != Some(u64::from(JOB_FILE_VERSION)) {
            warn!(
                file = %path.display(),
                ?version,
                "bulk job file of an unknown version; ignored"
            );
            continue;
        }
        match serde_json::from_slice::<JobFile>(&bytes) {
            Ok(f) if f.job_id.starts_with(JOB_ID_PREFIX) => out.push(f),
            Ok(f) => warn!(file = %path.display(), job = %f.job_id, "bulk job id invalid; ignored"),
            Err(e) => warn!(file = %path.display(), error = %e, "bulk job file invalid; ignored"),
        }
    }
    out.sort_by(|a, b| a.job_id.cmp(&b.job_id));
    out
}

// ---------------------------------------------------------------------------------
// Selection
// ---------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CursorMode {
    Index,
    Scan,
}

impl CursorMode {
    fn as_str(self) -> &'static str {
        match self {
            CursorMode::Index => "index",
            CursorMode::Scan => "scan",
        }
    }

    fn parse(s: &str) -> Self {
        if s == "index" {
            CursorMode::Index
        } else {
            CursorMode::Scan
        }
    }
}

/// Every started partition's run index is `Ready`.
async fn index_ready(ctx: &P11Ctx) -> bool {
    let cp = &ctx.cc.partitions;
    for p in cp.partition_count().ids() {
        let Ok(h) = cp.partition(p) else {
            return false;
        };
        let reader = h.reader();
        let ready = tokio::task::spawn_blocking(move || query::index_state(&reader))
            .await
            .is_ok_and(|s| s == IndexState::Ready);
        if !ready {
            return false;
        }
    }
    true
}

/// The job filter as a listing request: the snapshot bound is always applied.
fn list_request(
    f: &BulkFilter,
    created_at_ms: u64,
    mode: CursorMode,
    token: Option<String>,
) -> ListRunsRequest {
    ListRunsRequest {
        workflow: f.workflow.clone(),
        status: f.status.map(Into::into),
        queue: f.queue.clone(),
        tag: f.tags.first().cloned(),
        created_after_ms: f.created_after_ms,
        created_before_ms: Some(
            f.created_before_ms
                .map_or(created_at_ms, |b| b.min(created_at_ms)),
        ),
        limit: SELECTION_PAGE,
        page_token: token,
        tags: f.tags.clone(),
        order: match mode {
            CursorMode::Index => ListOrder::Newest,
            CursorMode::Scan => ListOrder::Oldest,
        },
        dlq: f.dlq,
        // Phase 14 (14-02 field): bulk selection keeps its pre-tenant filter; tenant scoping of
        // bulk jobs is 14-16's.
        tenant: None,
    }
}

/// Why a page could not be read now.
enum PageError {
    /// The pinned index is rebuilding (or the node is busy): retry later.
    Retry(String),
    /// Give up (the job fails).
    Fatal(ApiError),
}

/// One selection page in the pinned mode.
async fn select_page(
    ctx: &P11Ctx,
    req: &ListRunsRequest,
    mode: CursorMode,
) -> Result<RunPage, PageError> {
    let classify = |e: ApiError| {
        if e.code == ErrorCode::Unavailable {
            PageError::Retry(e.message)
        } else {
            PageError::Fatal(e)
        }
    };
    match mode {
        CursorMode::Scan => ctx
            .cc
            .partitions
            .list_runs(req.clone())
            .await
            .map_err(classify),
        CursorMode::Index => {
            // Pinned to the index: never let the search fall back to the scan.
            if !index_ready(ctx).await {
                return Err(PageError::Retry("run index rebuilding".to_owned()));
            }
            let page = match p11_search::list_runs_indexed(ctx, req).await {
                Some(r) => r.map_err(classify)?,
                None => {
                    return Err(PageError::Fatal(ApiError::internal(
                        "the indexed search declined a newest-first request",
                    )));
                }
            };
            if page
                .next_page_token
                .as_deref()
                .is_some_and(|t| !t.starts_with("i1."))
            {
                return Err(PageError::Retry("run index rebuilding".to_owned()));
            }
            Ok(page)
        }
    }
}

/// Count the matches (up to `max + 1`) and the first [`MAX_SAMPLES`] ids.
async fn count_matches(
    ctx: &P11Ctx,
    filter: &BulkFilter,
    created_at_ms: u64,
    mode: CursorMode,
    max: u64,
) -> Result<(u64, Vec<String>), ApiError> {
    let mut token = None;
    let mut n = 0u64;
    let mut sample = Vec::new();
    let mut retries = 0u32;
    loop {
        let req = list_request(filter, created_at_ms, mode, token.clone());
        let page = match select_page(ctx, &req, mode).await {
            Ok(p) => p,
            Err(PageError::Retry(msg)) if retries < 40 => {
                retries += 1;
                let _ = msg;
                tokio::time::sleep(RETRY_WAIT).await;
                continue;
            }
            Err(PageError::Retry(msg)) => return Err(ApiError::unavailable(msg)),
            Err(PageError::Fatal(e)) => return Err(e),
        };
        for r in page.runs {
            n += 1;
            if sample.len() < MAX_SAMPLES {
                sample.push(r.run_id);
            }
            if n > max {
                return Ok((n, sample));
            }
        }
        match page.next_page_token {
            Some(t) if !t.is_empty() => token = Some(t),
            _ => return Ok((n, sample)),
        }
    }
}

// ---------------------------------------------------------------------------------
// Per-run ops
// ---------------------------------------------------------------------------------

/// What one op did.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Outcome {
    Done,
    Skipped,
    Failed(String),
    /// Stopped (cancel / shutdown) before the op completed: the run stays pending.
    Interrupted,
}

/// The signal `request_id` of `(job, run)`: `<job>/<run>`, or `blk~` + the first 60 hex
/// digits of its SHA-256 when that exceeds the 64-byte cap.
pub fn signal_request_id(job_id: &str, run_id: &str) -> String {
    let full = format!("{job_id}/{run_id}");
    if full.len() <= ostro_api::domain::MAX_REQUEST_ID_LEN {
        return full;
    }
    let digest = Sha256::digest(full.as_bytes());
    let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
    format!("blk~{}", &hex[..60])
}

/// The dedupe key of a run a job creates from `run_id`.
pub fn dedupe_key(job_id: &str, run_id: &str) -> String {
    format!("{DEDUPE_PREFIX}{job_id}/{run_id}")
}

/// Run one op, retrying 429s (after `retry_after_ms`) and short unavailability.
async fn execute(
    ctx: &P11Ctx,
    job_id: &str,
    action: &BulkAction,
    run_id: &str,
    job: &Job,
    stop: &CancellationToken,
) -> Outcome {
    let name = action_name(action);
    let mut unavailable = 0u32;
    loop {
        let wait = match op_once(ctx, job_id, action, run_id, job).await {
            Err(e) if e.code == ErrorCode::RateLimited => {
                metrics::counter!("ostro_bulk_ops_total", "action" => name, "result" => "rate_limited")
                    .increment(1);
                Duration::from_millis(e.retry_after_ms().unwrap_or(DEFAULT_RETRY_AFTER_MS).max(1))
            }
            Err(e) if e.code == ErrorCode::Unavailable && unavailable < UNAVAILABLE_RETRIES => {
                unavailable += 1;
                RETRY_WAIT
            }
            Ok(o) => return o,
            Err(e) => return classify_error(action, e),
        };
        tokio::select! {
            biased;
            _ = stop.cancelled() => return Outcome::Interrupted,
            _ = tokio::time::sleep(wait) => {}
        }
    }
}

/// An op error that is not retried: a run no longer eligible is skipped, the rest fail.
fn classify_error(action: &BulkAction, e: ApiError) -> Outcome {
    match e.code {
        ErrorCode::NotFound => Outcome::Skipped,
        ErrorCode::Conflict => match (action, e.reason()) {
            (
                BulkAction::Rerun { .. } | BulkAction::Requeue { .. },
                Some(reason::RERUN_CUT_UNSAFE | reason::RERUN_NONDETERMINISTIC),
            ) => Outcome::Failed(error_text(&e)),
            _ => Outcome::Skipped,
        },
        _ => Outcome::Failed(error_text(&e)),
    }
}

fn error_text(e: &ApiError) -> String {
    match e.reason() {
        Some(r) => format!("{}: {} ({r})", e.code.as_str(), e.message),
        None => format!("{}: {}", e.code.as_str(), e.message),
    }
}

/// Whether `rec` was created by this job (lineage): its dedupe key carries the job's
/// prefix, or it restarts / reruns a run the job already processed.
fn created_by_job(job_id: &str, rec: &RunRecord, job: &Job) -> bool {
    let marker = format!("{DEDUPE_PREFIX}{job_id}/");
    let key = String::from_utf8_lossy(&rec.idempotency_key);
    if key.contains(&marker) {
        return true;
    }
    rec.retry_of
        .as_ref()
        .is_some_and(|r| job.lock().processed.contains(&r.to_string()))
}

async fn op_once(
    ctx: &P11Ctx,
    job_id: &str,
    action: &BulkAction,
    run_id: &str,
    job: &Job,
) -> Result<Outcome, ApiError> {
    let cp = &ctx.cc.partitions;
    // Phase 13 (13-02): the action itself goes through the routed control plane.
    let routed = &ctx.cc.routed;
    let Ok(run) = run_id.parse::<RunId>() else {
        return Ok(Outcome::Failed(format!("invalid run id {run_id:?}")));
    };
    let handle = &cp.partition(run.partition())?;
    let rec = read_record(handle, run).await?;
    if created_by_job(job_id, &rec, job) {
        return Ok(Outcome::Skipped);
    }
    let reason_text = Some(format!("bulk {job_id}"));
    match action {
        BulkAction::Cancel => {
            let r = routed
                .cancel_run(CancelRequest {
                    run_id: run_id.to_owned(),
                    reason: reason_text,
                    dry_run: false,
                })
                .await?;
            Ok(if r.already_requested {
                Outcome::Skipped
            } else {
                Outcome::Done
            })
        }
        BulkAction::Terminate { reason } => {
            routed
                .terminate_run(TerminateRequest {
                    run_id: run_id.to_owned(),
                    reason: Some(if reason.is_empty() {
                        format!("bulk {job_id}")
                    } else {
                        reason.clone()
                    }),
                    dry_run: false,
                })
                .await?;
            Ok(Outcome::Done)
        }
        BulkAction::Retry => {
            if !rec.status.is_terminal()
                || matches!(rec.status, RunStatus::Completed | RunStatus::ContinuedAsNew)
            {
                return Ok(Outcome::Skipped);
            }
            if rec.resumable {
                routed
                    .retry_run(RetryRequest {
                        run_id: run_id.to_owned(),
                        dry_run: false,
                    })
                    .await?;
                return Ok(Outcome::Done);
            }
            restart_with_key(ctx, run, rec, &dedupe_key(job_id, run_id)).await?;
            Ok(Outcome::Done)
        }
        BulkAction::Rerun { from, version } => {
            p11_rerun::rerun_run(
                ctx,
                RerunRequest {
                    run_id: run_id.to_owned(),
                    from: from.clone(),
                    version: version.clone(),
                    idempotency_key: Some(dedupe_key(job_id, run_id)),
                    dry_run: false,
                },
            )
            .await?;
            Ok(Outcome::Done)
        }
        BulkAction::Requeue { mode } => {
            p11_dlq::requeue_dlq(
                ctx,
                DlqRequeueRequest {
                    run_id: run_id.to_owned(),
                    mode: mode.clone(),
                    dry_run: false,
                },
            )
            .await?;
            Ok(Outcome::Done)
        }
        BulkAction::Pause => {
            routed.pause_run(run_id.to_owned(), reason_text).await?;
            Ok(Outcome::Done)
        }
        BulkAction::Resume => {
            routed.resume_run(run_id.to_owned()).await?;
            Ok(Outcome::Done)
        }
        BulkAction::Signal { name, payload } => {
            routed
                .signal_run(SignalRequest {
                    run_id: run_id.to_owned(),
                    name: name.clone(),
                    payload: payload.clone(),
                    request_id: Some(signal_request_id(job_id, run_id)),
                    dry_run: false,
                })
                .await?;
            // A duplicate (the job already delivered it before a restart) is done too.
            Ok(Outcome::Done)
        }
    }
}

/// The Phase 5 restart copy with the job's dedupe key: the new run is placed by
/// `partition_for_key(workflow, key)`, the start goes through the StartRun rate limit
/// (Phase 7, D-W21: 429 is retried by the caller), and a repeat returns the existing run.
/// p13-follow: the StartRun goes to that partition's leader wherever it is
/// (`p13::keyed_start`; the rate limit is admitted there).
async fn restart_with_key(
    ctx: &P11Ctx,
    old: RunId,
    rec: RunRecord,
    key: &str,
) -> Result<RunId, ApiError> {
    let cp = &ctx.cc.partitions;
    // Phase 13 (13-18): a retired module takes no new run (as a start).
    crate::rerun::check_restart_module(ctx.cc.meta.view(), rec.module_sha.as_ref())?;
    let old_handle = &cp.partition(old.partition())?;
    let reader = old_handle.reader();
    let input = tokio::task::spawn_blocking(move || {
        query::run_payload(&reader, &old, query::PAYLOAD_INPUT)
    })
    .await
    .map_err(task_error)?
    .map_err(store_error)?;
    let input = input.unwrap_or_else(|| PayloadRef::inline(Bytes::from_static(b"null")));
    let p = partition_for_key(&rec.workflow, key.as_bytes(), cp.partition_count());
    let run = RunId::new(p, uuid::Uuid::now_v7());
    let mut seed = [0u8; 32];
    getrandom::fill(&mut seed)
        .map_err(|e| ApiError::internal(format!("OS random source unavailable: {e}")))?;
    let now = ctx.cc.now_ms();
    let start = StartRun {
        run_id: run.as_bytes().to_vec(),
        workflow: rec.workflow,
        workflow_version: rec.workflow_version,
        queue: rec.queue,
        input: Some(input),
        idempotency_key: key.as_bytes().to_vec(),
        conflict_policy: cmd::ConflictPolicy::ReturnExisting as i32,
        concurrency_key: rec.concurrency_key,
        priority: rec.priority,
        ready_at_ms: now,
        requested_at_ms: now,
        parent: None,
        rng_seed: seed.to_vec(),
        module_sha: rec.module_sha.map(|s| s.to_vec()).unwrap_or_default(),
        // Hook-run lineage tags are not copied onto the restart (D-P11-6).
        tags: rec
            .tags
            .into_iter()
            .filter(|t| !t.starts_with(onfailure::ON_FAILURE_OF_TAG))
            .collect(),
        tenant: rec.tenant.unwrap_or_default(),
        traceparent: rec.traceparent.unwrap_or_default(),
        run_timeout_ms: rec.run_timeout_ms.unwrap_or(0),
        schedule_to_start_ms: rec.schedule_to_start_ms,
        retention_ms: rec.retention_ms,
        retry_of: old.as_bytes().to_vec(),
        continued_from: Vec::new(),
        run_started: None,
        ..Default::default()
    };
    let ks = crate::p13::keyed_start::KeyedStart {
        start,
        rate_limit: true,
        prefix_bound: None,
    };
    match crate::p13::keyed_start::submit(cp, old_handle, ks).await? {
        crate::p13::keyed_start::Started::Run { run, .. } => Ok(run),
        crate::p13::keyed_start::Started::Rejected(e) => {
            Err(ApiError::invalid_argument(format!("restart rejected: {e}")))
        }
    }
}

// ---------------------------------------------------------------------------------
// Runner
// ---------------------------------------------------------------------------------

/// A failpoint fired: stop without writing anything further.
struct Crashed;

/// Failpoint `bulk::after_op`.
fn after_op_fires() -> bool {
    #[cfg(feature = "failpoints")]
    if fail::eval("bulk::after_op", |_| ()).is_some() {
        warn!("bulk::after_op (injected): the bulk runner stops as if the node crashed");
        return true;
    }
    false
}

/// Failpoint `bulk::before_checkpoint`.
fn before_checkpoint_fires() -> bool {
    #[cfg(feature = "failpoints")]
    if fail::eval("bulk::before_checkpoint", |_| ()).is_some() {
        warn!("bulk::before_checkpoint (injected): the bulk runner stops as if the node crashed");
        return true;
    }
    false
}

struct Runner {
    reg: Arc<BulkRegistry>,
    ctx: Arc<P11Ctx>,
    job: Arc<Job>,
    /// Job cancel or service shutdown.
    stop: CancellationToken,
    shutdown: CancellationToken,
    action: BulkAction,
    filter: BulkFilter,
    created_at_ms: u64,
    mode: CursorMode,
    ops_since_checkpoint: u64,
    last_checkpoint_ms: u64,
    /// partition -> (checked at, over the high-water mark).
    backlog: HashMap<u16, (u64, bool)>,
}

impl Runner {
    fn now(&self) -> u64 {
        self.reg.clock.now_ms()
    }

    fn set_state(&self, state: &str) -> bool {
        let mut st = self.job.lock();
        if st.file.state == "cancelled" || st.file.state == state {
            return false;
        }
        st.file.state = state.to_owned();
        st.dirty = true;
        true
    }

    /// A periodic / transition checkpoint of a running job (through the failpoint).
    async fn checkpoint(&mut self) -> Result<(), Crashed> {
        if before_checkpoint_fires() {
            return Err(Crashed);
        }
        self.ops_since_checkpoint = 0;
        self.last_checkpoint_ms = self.now();
        if let Err(e) = self.reg.write_job(&self.job).await {
            warn!(job = %self.job.id, error = %e, "bulk checkpoint failed; retried at the next one");
        }
        self.reg.publish_job_gauges();
        Ok(())
    }

    async fn maybe_checkpoint(&mut self) -> Result<(), Crashed> {
        let every = u64::try_from(self.reg.cfg.checkpoint_every.get().as_millis())
            .unwrap_or(u64::MAX)
            .max(1);
        let ops_due = self.ops_since_checkpoint >= self.reg.cfg.checkpoint_ops.max(1);
        let time_due =
            self.now().saturating_sub(self.last_checkpoint_ms) >= every && self.job.lock().dirty;
        if ops_due || time_due {
            self.checkpoint().await?;
        }
        Ok(())
    }

    /// Record an op's outcome.
    fn record(&mut self, run_id: String, outcome: Outcome) -> Result<(), Crashed> {
        let name = action_name(&self.action);
        let mut st = self.job.lock();
        st.in_flight.remove(&run_id);
        let result = match outcome {
            Outcome::Interrupted => {
                st.queue.push_front(run_id);
                return Ok(());
            }
            Outcome::Done => {
                st.file.done += 1;
                "done"
            }
            Outcome::Skipped => {
                st.file.skipped += 1;
                "skipped"
            }
            Outcome::Failed(reason) => {
                st.file.failed_count += 1;
                if st.file.failed.len() < MAX_FAILURES {
                    st.file.failed.push(BulkFailure {
                        run_id: run_id.clone(),
                        reason,
                    });
                }
                "failed"
            }
        };
        if st.processed.insert(run_id.clone()) {
            st.file.processed.push(run_id);
        }
        st.dirty = true;
        drop(st);
        metrics::counter!("ostro_bulk_ops_total", "action" => name, "result" => result)
            .increment(1);
        self.ops_since_checkpoint += 1;
        if after_op_fires() {
            return Err(Crashed);
        }
        Ok(())
    }

    /// The next run to operate on (skipping processed and in-flight ids).
    fn next_run(&self) -> Option<String> {
        let mut st = self.job.lock();
        while let Some(id) = st.queue.pop_front() {
            if !st.processed.contains(&id) && !st.in_flight.contains(&id) {
                return Some(id);
            }
        }
        None
    }

    /// Read the next selection page into the queue. `Ok(false)`: stopped or failed.
    async fn fetch_page(&mut self) -> Result<bool, Crashed> {
        loop {
            if self.stop.is_cancelled() {
                return Ok(false);
            }
            let token = self.job.lock().file.cursor.clone();
            let req = list_request(&self.filter, self.created_at_ms, self.mode, token);
            match select_page(&self.ctx, &req, self.mode).await {
                Ok(page) => {
                    let mut st = self.job.lock();
                    for r in page.runs {
                        if !st.processed.contains(&r.run_id) {
                            st.queue.push_back(r.run_id);
                        }
                    }
                    match page.next_page_token.filter(|t| !t.is_empty()) {
                        Some(t) => st.file.cursor = Some(t),
                        None => st.file.selection_done = true,
                    }
                    st.dirty = true;
                    return Ok(true);
                }
                Err(PageError::Retry(msg)) => {
                    tracing::debug!(job = %self.job.id, %msg, "bulk selection waits");
                    self.maybe_checkpoint().await?;
                    tokio::select! {
                        biased;
                        _ = self.stop.cancelled() => return Ok(false),
                        _ = tokio::time::sleep(RETRY_WAIT) => {}
                    }
                }
                Err(PageError::Fatal(e)) => {
                    warn!(job = %self.job.id, error = %e, "bulk selection failed; the job fails");
                    let mut st = self.job.lock();
                    if st.file.state != "cancelled" {
                        st.file.state = "failed".to_owned();
                    }
                    st.file.failed_count += 1;
                    if st.file.failed.len() < MAX_FAILURES {
                        st.file.failed.push(BulkFailure {
                            run_id: String::new(),
                            reason: format!("selection: {}", error_text(&e)),
                        });
                    }
                    st.dirty = true;
                    return Ok(false);
                }
            }
        }
    }

    /// Whether `p`'s ready backlog exceeds `[bulk] backlog_high` (cached briefly).
    async fn over_backlog(&mut self, p: PartitionId) -> bool {
        let now = self.now();
        if let Some((at, over)) = self.backlog.get(&p.0)
            && now.saturating_sub(*at) < BACKLOG_TTL_MS
        {
            return *over;
        }
        let high = self.reg.cfg.backlog_high;
        let over = match self.ctx.cc.partitions.partition(p) {
            Ok(h) => {
                let reader = h.reader();
                let limit = usize::try_from(high.saturating_add(1)).unwrap_or(usize::MAX);
                tokio::task::spawn_blocking(move || query::ready_entries(&reader, limit))
                    .await
                    .ok()
                    .and_then(Result::ok)
                    .is_some_and(|rows| rows.len() as u64 > high)
            }
            Err(_) => false,
        };
        self.backlog.insert(p.0, (now, over));
        over
    }

    /// Wait while the target partition is over its backlog. `Ok(false)`: stopped.
    async fn wait_backlog(&mut self, run_id: &str) -> Result<bool, Crashed> {
        let Ok(run) = run_id.parse::<RunId>() else {
            return Ok(true);
        };
        let mut paused = false;
        while self.over_backlog(run.partition()).await {
            if !paused {
                paused = true;
                info!(job = %self.job.id, partition = run.partition().0, "bulk job paused: ready backlog over backlog_high");
                if self.set_state("paused_backpressure") {
                    self.checkpoint().await?;
                }
            } else {
                self.maybe_checkpoint().await?;
            }
            tokio::select! {
                biased;
                _ = self.stop.cancelled() => return Ok(false),
                _ = tokio::time::sleep(RETRY_WAIT) => {}
            }
            self.backlog.remove(&run.partition().0);
        }
        if paused && self.set_state("running") {
            info!(job = %self.job.id, "bulk job resumed: backlog drained");
            self.checkpoint().await?;
        }
        Ok(true)
    }

    async fn run(mut self) {
        let (rate, in_flight) = {
            let st = self.job.lock();
            (st.file.rate_per_sec, st.file.max_in_flight)
        };
        let mut pacer = Pacer::new(rate, self.reg.cfg.burst, Arc::clone(&self.reg.clock));
        {
            let now = self.now();
            let mut st = self.job.lock();
            if st.file.state == "cancelled" {
                return;
            }
            st.file.state = "running".to_owned();
            st.file.started_at_ms.get_or_insert(now);
            st.dirty = true;
        }
        if self.checkpoint().await.is_err() {
            return;
        }
        let sem = Arc::new(Semaphore::new(in_flight.max(1) as usize));
        let mut ops: JoinSet<(String, Outcome)> = JoinSet::new();
        let mut selection_ok = true;
        'outer: loop {
            while let Some(r) = ops.try_join_next() {
                if let Ok((id, o)) = r
                    && self.record(id, o).is_err()
                {
                    return;
                }
            }
            if self.maybe_checkpoint().await.is_err() {
                return;
            }
            if self.stop.is_cancelled() {
                break;
            }
            let run_id = match self.next_run() {
                Some(id) => id,
                None => {
                    if !self.job.lock().file.selection_done {
                        match self.fetch_page().await {
                            Ok(true) => continue,
                            Ok(false) => {
                                selection_ok = self.stop.is_cancelled()
                                    || self.job.lock().file.state != "failed";
                                break;
                            }
                            Err(Crashed) => return,
                        }
                    }
                    if ops.is_empty() {
                        break;
                    }
                    if let Some(Ok((id, o))) = ops.join_next().await
                        && self.record(id, o).is_err()
                    {
                        return;
                    }
                    continue;
                }
            };
            // Hold the id as in flight while waiting, so a checkpoint keeps it pending.
            self.job.lock().in_flight.insert(run_id.clone());
            let requeue = |job: &Job, id: String| {
                let mut st = job.lock();
                st.in_flight.remove(&id);
                st.queue.push_front(id);
            };
            match self.wait_backlog(&run_id).await {
                Ok(true) => {}
                Ok(false) => {
                    requeue(&self.job, run_id);
                    break;
                }
                Err(Crashed) => return,
            }
            if !pacer.admit(&self.stop).await {
                requeue(&self.job, run_id);
                break;
            }
            let permit = tokio::select! {
                biased;
                _ = self.stop.cancelled() => {
                    requeue(&self.job, run_id);
                    break 'outer;
                }
                p = Arc::clone(&sem).acquire_owned() => match p {
                    Ok(p) => p,
                    Err(_) => {
                        requeue(&self.job, run_id);
                        break 'outer;
                    }
                },
            };
            let ctx = Arc::clone(&self.ctx);
            let job_id = self.job.id.clone();
            let action = self.action.clone();
            let stop = self.stop.clone();
            let job = Arc::clone(&self.job);
            ops.spawn(async move {
                let _permit = permit;
                let o = execute(&ctx, &job_id, &action, &run_id, &job, &stop).await;
                (run_id, o)
            });
        }
        // Let in-flight ops finish (they stop at their next wait once `stop` fired).
        while let Some(r) = ops.join_next().await {
            if let Ok((id, o)) = r
                && self.record(id, o).is_err()
            {
                return;
            }
        }
        let now = self.now();
        let final_state = {
            let mut st = self.job.lock();
            if self.job.cancel.is_cancelled() || st.file.state == "cancelled" {
                st.file.state = "cancelled".to_owned();
            } else if !selection_ok || st.file.state == "failed" {
                st.file.state = "failed".to_owned();
            } else if self.shutdown.is_cancelled() {
                // Interrupted by the node stopping: resumes at the next start.
                if st.file.state == "paused_backpressure" {
                    st.file.state = "running".to_owned();
                }
            } else {
                st.file.state = "done".to_owned();
            }
            if !is_active(&st.file.state) {
                st.file.finished_at_ms.get_or_insert(now);
            }
            st.dirty = true;
            st.file.state.clone()
        };
        if self.checkpoint().await.is_ok() {
            let v = self.job.view();
            info!(
                job = %self.job.id,
                state = %final_state,
                done = v.done,
                skipped = v.skipped,
                failed = v.failed_count,
                "bulk job stopped"
            );
        }
    }
}

async fn run_job(
    reg: Arc<BulkRegistry>,
    ctx: Arc<P11Ctx>,
    job: Arc<Job>,
    shutdown: CancellationToken,
) {
    if job.running.swap(true, Ordering::SeqCst) {
        return;
    }
    let (spec, created_at_ms, mode) = {
        let st = job.lock();
        (
            st.file.spec.clone(),
            st.file.created_at_ms,
            CursorMode::parse(&st.file.cursor_mode),
        )
    };
    let stop = CancellationToken::new();
    {
        let stop = stop.clone();
        let cancel = job.cancel.clone();
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = cancel.cancelled() => {}
                _ = shutdown.cancelled() => {}
                _ = stop.cancelled() => {}
            }
            stop.cancel();
        });
    }
    let now = reg.clock.now_ms();
    let runner = Runner {
        reg,
        ctx,
        job: Arc::clone(&job),
        stop: stop.clone(),
        shutdown,
        action: spec.action,
        filter: spec.filter,
        created_at_ms,
        mode,
        ops_since_checkpoint: 0,
        last_checkpoint_ms: now,
        backlog: HashMap::new(),
    };
    runner.run().await;
    stop.cancel();
    job.running.store(false, Ordering::SeqCst);
}

/// The bulk service task: resumes the persisted jobs, then runs every job handed to it
/// (one runner each) until `ServiceCtx::shutdown`, when the runners checkpoint and are
/// joined.
pub fn spawn(svc: &ServiceCtx, p11: &Arc<P11Ctx>) -> Option<JoinHandle<()>> {
    let reg = Arc::clone(&p11.bulk);
    let (tx, mut rx) = mpsc::unbounded_channel::<Arc<Job>>();
    if reg.tx.set(tx).is_err() {
        warn!("bulk service already started for this registry");
        return None;
    }
    let ctx = Arc::clone(p11);
    let shutdown = svc.shutdown.clone();
    Some(tokio::spawn(async move {
        reg.resume_all(&ctx).await;
        let mut set = JoinSet::new();
        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => break,
                job = rx.recv() => match job {
                    Some(job) => {
                        set.spawn(run_job(
                            Arc::clone(&reg),
                            Arc::clone(&ctx),
                            job,
                            shutdown.clone(),
                        ));
                    }
                    None => break,
                },
                Some(_) = set.join_next(), if !set.is_empty() => {}
            }
        }
        rx.close();
        while set.join_next().await.is_some() {}
    }))
}
