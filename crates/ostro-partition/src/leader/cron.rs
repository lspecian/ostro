//! Cron driver of a schedule's owner partition (P8-D10, plan 08-14).
//!
//! Contract (plan 08-01):
//! - [`ScheduleSpec`]: one schedule as the driver sees it (copied from the meta `Schedule`
//!   record by the server's [`ScheduleSource`]).
//! - [`ScheduleSource`]: a watch channel of the full schedule set; the driver filters it by
//!   `ostro_types::routing::schedule_owner(name, P) == partition`.
//! - [`CronConfig`]: driver knobs (`[triggers]`, P8-D22).
//!
//! # Driver (08-14)
//!
//! One leader service per partition ([`spawn`], started by `leader::spawn_all` when the node
//! has a schedule source). It drives only the schedules this partition owns and that are not
//! paused, and proposes `Cmd::ScheduleFire`; overlap is evaluated in apply (08-07), catch-up
//! here. Every time read goes through the injected [`crate::clock::Clock`]; waits are short
//! real-time polls (at most [`MAX_SLEEP`]) so a `ManualClock` drives them.
//!
//! **Activation** (leadership start, a new schedule, a new incarnation W4, resume): read the
//! durable watermark from `schedule_state` ([`schedule_state`]; 0 when absent or when the row
//! belongs to another incarnation) and call `ostro_triggers::cron::plan_catchup` with
//! `floor = max(watermark, created_at_ms)` (W6), so a brand-new or re-created schedule never
//! backfills:
//! - `Skip`: one `ScheduleFire{kind: Advance}` to `latest_at_or_before(now)`;
//! - `One`: one `Fire` at `latest_at_or_before(now)`;
//! - `All`: the newest [`CronConfig::catchup_cap`] fires in the catch-up window.
//!
//! Catch-up never enumerates from the watermark (B5): `latest_at_or_before` and the backward
//! `fires_in` walk cost O(cap) however long the downtime was.
//!
//! **Steady state**: a `BTreeSet<(due_ms, name)>` window, seeded per schedule with
//! `next_after(max(floor, now))`. When an entry is due and the following fire is still in the
//! future, it is submitted as a `Fire`; when the driver is behind by more than one fire (a
//! stalled task, a clock jump), the catch-up path runs instead, so the policy also applies to
//! a live driver that missed fires.
//!
//! **Spec changes** (same incarnation, still active) re-seed the schedule from its floor: a
//! cron, time-zone or bounds change applies to future fires only, and the watermark keeps
//! exactly-once. **Pause** drops the schedule from the window (the watermark stays).
//!
//! **Submission**: the run id is minted on this partition, the `StartRun` is built by
//! [`ScheduleSource::build_start`] (the server resolves the workflow version, queue, module
//! and policies; the default builds a minimal start), with idempotency key
//! `cron_idempotency_key(name, fire_time)` and tag `ostro.schedule=<name>`. Every
//! `ScheduleFire` carries `schedule_created_at_ms` (W4). `NotLeader` / a failed partition stop
//! the driver; other submit errors are retried with backoff and the same command (idempotent
//! by watermark + key). `ostro_cron_fires_total{schedule,outcome}` counts the outcomes
//! (`started`, `skipped`, `buffered`, `advanced`, `duplicate`, `rejected`, `start_error`).

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use ostro_log::Rf1Log;
use ostro_triggers::cron::{CatchupPolicy, CronSpec, plan_catchup};
use ostro_types::command::{
    ConflictPolicy, MAX_RUN_TAG_LEN, ScheduleFire, ScheduleFireKind, StartRun, cron_idempotency_key,
};
use ostro_types::routing::schedule_owner;
use ostro_types::{Command, RunId};
use tokio::sync::watch;
use tracing::{debug, error, info, warn};

use super::LeaderCtx;
use crate::apply::ApplyOutcome;
use crate::handle::{PartitionError, PartitionHandle};
use crate::store::keys::schedule_state_key;
use crate::store::values::{self, ScheduleStateRow};
use crate::store::{StoreError, StoreReader, Table};

/// One schedule, as the cron driver sees it. Field meanings and the numeric `overlap` /
/// `catchup` mappings are those of the meta `Schedule` record (05-07).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ScheduleSpec {
    /// Schedule name (the owner partition is derived from it).
    pub name: String,
    /// Workflow to start.
    pub workflow: String,
    /// Pinned workflow version (`None` = the active version at fire time).
    pub version: Option<u32>,
    /// Cron expression.
    pub cron: String,
    /// IANA zone name; `""` = UTC.
    pub timezone: String,
    /// Run input bytes.
    pub input: Vec<u8>,
    /// Paused schedules do not fire.
    pub paused: bool,
    /// Overlap policy: 0 Skip, 1 BufferOne, 2 AllowAll, 3 CancelOther, 4 TerminateOther.
    pub overlap: u8,
    /// Catch-up policy: 0 Skip, 1 One, 2 All.
    pub catchup: u8,
    /// How far back missed fires are considered (0 = default).
    pub catchup_window_ms: u64,
    /// No fire before this time (unix ms).
    pub start_at_ms: Option<u64>,
    /// No fire after this time (unix ms).
    pub end_at_ms: Option<u64>,
    /// Meta `Schedule.created_at_ms`: identifies the schedule incarnation (W4).
    pub created_at_ms: u64,
    /// Meta log index of the record version this spec was read from.
    pub meta_version: u64,
}

/// What the driver asks [`ScheduleSource::build_start`] to build.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FireRequest {
    /// The run id (already routed to the schedule's owner partition).
    pub run: RunId,
    /// The run's idempotency key (`__cron/<name>/<fire_time_ms>`, or the manual key).
    pub idempotency_key: String,
    /// Nominal fire time (ms); the request time for a manual fire.
    pub fire_time_ms: u64,
    /// Current time from the injected clock (`requested_at_ms` / `ready_at_ms`).
    pub now_ms: u64,
}

/// Why a `StartRun` could not be built.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum StartError {
    /// Transient (payload write, ...): the driver retries the same fire.
    #[error("retryable: {0}")]
    Retry(String),
    /// Permanent for this fire (the workflow does not exist, ...): the fire is dropped.
    #[error("{0}")]
    Skip(String),
}

/// The future returned by [`ScheduleSource::build_start`].
pub type StartFuture<'a> = Pin<Box<dyn Future<Output = Result<StartRun, StartError>> + Send + 'a>>;

/// The schedule set, as a watch channel (meta-backed, implemented in ostro-server by 08-14).
pub trait ScheduleSource: Send + Sync + 'static {
    /// A receiver of the current full schedule set; it changes whenever a schedule does.
    fn subscribe(&self) -> watch::Receiver<Arc<Vec<ScheduleSpec>>>;

    /// The `StartRun` a fire of `spec` submits (inside `ScheduleFire.start`). The default is
    /// [`default_start`]; the server resolves the workflow version, queue, module, policies
    /// and the pre-encoded `RunStarted` like an API start.
    fn build_start<'a>(
        &'a self,
        handle: &'a PartitionHandle<Rf1Log>,
        spec: &'a ScheduleSpec,
        req: FireRequest,
    ) -> StartFuture<'a> {
        Box::pin(default_start(handle, spec, req))
    }
}

/// Cron driver configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CronConfig {
    /// Most missed fires replayed by catch-up `All` (default 100).
    pub catchup_cap: u32,
}

impl Default for CronConfig {
    fn default() -> Self {
        Self { catchup_cap: 100 }
    }
}

/// The cron driver as configured on a node: knobs plus the schedule source
/// (`LeaderConfig::cron`, filled from `NodeConfig::{cron, schedule_source}`).
#[derive(Clone)]
pub struct CronService {
    pub config: CronConfig,
    pub source: Arc<dyn ScheduleSource>,
}

impl std::fmt::Debug for CronService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CronService")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

/// Run tag carried by every run a schedule starts (RESERVATIONS §11): `ostro.schedule=<name>`,
/// cut to [`MAX_RUN_TAG_LEN`] bytes (at a char boundary) for very long names.
pub fn schedule_tag(name: &str) -> String {
    let mut t = format!("ostro.schedule={name}");
    if t.len() > MAX_RUN_TAG_LEN {
        let mut cut = MAX_RUN_TAG_LEN;
        while !t.is_char_boundary(cut) {
            cut -= 1;
        }
        t.truncate(cut);
    }
    t
}

/// Queue used by [`default_start`].
pub const DEFAULT_START_QUEUE: &str = "default";

/// A minimal `StartRun` for a fire: the spec's workflow (version or 1), queue
/// [`DEFAULT_START_QUEUE`], the input bytes, `ReturnExisting` on the idempotency key, the
/// schedule tag and an rng seed derived from the run id. Used by sources without metadata.
pub async fn default_start(
    handle: &PartitionHandle<Rf1Log>,
    spec: &ScheduleSpec,
    req: FireRequest,
) -> Result<StartRun, StartError> {
    let input = handle
        .prepare_payload(Bytes::from(spec.input.clone()))
        .await
        .map_err(|e| StartError::Retry(e.to_string()))?;
    let seed = blake3::hash(req.run.as_bytes());
    Ok(StartRun {
        run_id: req.run.as_bytes().to_vec(),
        workflow: spec.workflow.clone(),
        workflow_version: spec.version.unwrap_or(1),
        queue: DEFAULT_START_QUEUE.to_owned(),
        input: Some(input),
        idempotency_key: req.idempotency_key.into_bytes(),
        conflict_policy: ConflictPolicy::ReturnExisting as i32,
        ready_at_ms: req.now_ms,
        requested_at_ms: req.now_ms,
        rng_seed: seed.as_bytes().to_vec(),
        tags: vec![schedule_tag(&spec.name)],
        ..Default::default()
    })
}

/// The `schedule_state` watermark row of `name`, if any (one point read).
pub fn schedule_state(r: &StoreReader, name: &str) -> Result<Option<ScheduleStateRow>, StoreError> {
    r.get(Table::ScheduleState, &schedule_state_key(name))?
        .map(|v| values::decode(&v))
        .transpose()
}

/// The watermark of the incarnation `created_at_ms` in `row`: 0 when there is no row or the
/// row belongs to another incarnation (an orphan left by a delete, W4/W6).
pub fn incarnation_watermark(row: Option<&ScheduleStateRow>, created_at_ms: u64) -> u64 {
    match row {
        Some(r) if r.schedule_created_at_ms == created_at_ms => r.watermark_ms,
        _ => 0,
    }
}

/// Parse `spec`'s cron expression with its time zone and bounds.
pub fn parse_spec(spec: &ScheduleSpec) -> Result<CronSpec, ostro_triggers::cron::CronError> {
    CronSpec::parse_bounded(&spec.cron, &spec.timezone, spec.start_at_ms, spec.end_at_ms)
}

/// The next fire strictly after `after_ms` (0 when `after_ms` is out of the i64 range).
pub fn next_fire_after(cron: &CronSpec, after_ms: u64) -> Option<u64> {
    let after = i64::try_from(after_ms).ok()?;
    cron.next_after(after).and_then(|t| u64::try_from(t).ok())
}

/// Longest real-time sleep between clock checks (ManualClock tests rely on this poll).
pub const MAX_SLEEP: Duration = Duration::from_millis(50);
/// First retry delay after a transient submit error (doubles up to [`MAX_RETRY`]).
const RETRY: Duration = Duration::from_millis(50);
const MAX_RETRY: Duration = Duration::from_secs(1);

/// Start the cron driver of `ctx.partition`. Runs until `ctx.shutdown` turns true or the
/// partition stops accepting this leader's commands.
pub fn spawn(ctx: LeaderCtx, svc: CronService) -> tokio::task::JoinHandle<()> {
    let rx = svc.source.subscribe();
    tokio::spawn(async move {
        Driver::new(ctx, svc).run(rx).await;
    })
}

/// One tracked (owned) schedule.
struct Tracked {
    spec: ScheduleSpec,
    /// `None` when the stored expression does not parse (never scheduled).
    cron: Option<Arc<CronSpec>>,
    /// Highest fire time handled by this incarnation (durable watermark or a submitted fire),
    /// never below `created_at_ms`.
    floor: u64,
    /// The schedule's entry in the window, if any.
    next: Option<u64>,
}

/// The driver stops (leadership lost, partition failed, shutdown).
struct Stop;

struct Driver {
    ctx: LeaderCtx,
    cfg: CronConfig,
    source: Arc<dyn ScheduleSource>,
    tracked: BTreeMap<String, Tracked>,
    window: BTreeSet<(u64, String)>,
}

impl Driver {
    fn new(ctx: LeaderCtx, svc: CronService) -> Self {
        Self {
            ctx,
            cfg: svc.config,
            source: svc.source,
            tracked: BTreeMap::new(),
            window: BTreeSet::new(),
        }
    }

    fn p(&self) -> u16 {
        self.ctx.partition.0
    }

    async fn run(mut self, mut rx: watch::Receiver<Arc<Vec<ScheduleSpec>>>) {
        let mut shutdown = self.ctx.shutdown.clone();
        let clock = Arc::clone(&self.ctx.clock);
        info!(partition = self.p(), "cron driver started");
        let mut source_open = true;
        // The first pass reconciles the current set; later passes only after a change (a
        // change observed by `changed()` below is marked seen, hence the flag).
        let mut dirty = true;
        loop {
            if *shutdown.borrow() {
                break;
            }
            if dirty || (source_open && rx.has_changed().unwrap_or(false)) {
                dirty = false;
                let set = Arc::clone(&rx.borrow_and_update());
                if self.reconcile(&set).await.is_err() {
                    return;
                }
                continue;
            }
            let now = clock.now_ms();
            if let Some((due, name)) = self.window.first().cloned()
                && due <= now
            {
                self.window.remove(&(due, name.clone()));
                if let Some(t) = self.tracked.get_mut(&name) {
                    t.next = None;
                }
                if self.on_due(&name, due, now).await.is_err() {
                    return;
                }
                continue;
            }
            let delay = self
                .window
                .first()
                .map_or(MAX_SLEEP, |(d, _)| {
                    Duration::from_millis(d.saturating_sub(now))
                })
                .min(MAX_SLEEP);
            tokio::select! {
                _ = shutdown.changed() => {}
                r = rx.changed(), if source_open => {
                    if r.is_ok() {
                        dirty = true;
                    } else {
                        debug!(partition = self.p(), "schedule source closed; keeping the last set");
                        source_open = false;
                    }
                }
                () = tokio::time::sleep(delay) => {}
            }
        }
        info!(partition = self.p(), "cron driver stopped");
    }

    /// Bring the tracked set in line with the source's full set.
    async fn reconcile(&mut self, set: &[ScheduleSpec]) -> Result<(), Stop> {
        let count = self.ctx.count;
        let me = self.ctx.partition;
        let owned: BTreeMap<&str, &ScheduleSpec> = set
            .iter()
            .filter(|s| schedule_owner(&s.name, count) == me)
            .map(|s| (s.name.as_str(), s))
            .collect();
        // Deleted (or no longer owned) schedules leave the window.
        let gone: Vec<String> = self
            .tracked
            .keys()
            .filter(|n| !owned.contains_key(n.as_str()))
            .cloned()
            .collect();
        for n in gone {
            self.untrack(&n);
            debug!(partition = self.p(), schedule = %n, "schedule removed");
        }
        for (name, spec) in owned {
            let prev = self.tracked.get(name);
            if prev.is_some_and(|t| t.spec == *spec) {
                continue;
            }
            let activate = match prev {
                None => !spec.paused,
                Some(t) => {
                    !spec.paused && (t.spec.paused || t.spec.created_at_ms != spec.created_at_ms)
                }
            };
            if spec.paused {
                // Pause: drop the window entry, keep the floor (the watermark stays durable).
                let floor = prev.map_or(spec.created_at_ms, |t| t.floor);
                self.untrack(name);
                self.tracked.insert(
                    name.to_owned(),
                    Tracked {
                        spec: spec.clone(),
                        cron: None,
                        floor,
                        next: None,
                    },
                );
                continue;
            }
            if activate {
                self.activate(spec).await?;
            } else {
                // Same incarnation, still active: future fires follow the new spec.
                let floor = prev.map_or(spec.created_at_ms, |t| t.floor);
                self.untrack(name);
                let cron = self.parse(spec);
                self.tracked.insert(
                    name.to_owned(),
                    Tracked {
                        spec: spec.clone(),
                        cron,
                        floor,
                        next: None,
                    },
                );
                let now = self.ctx.clock.now_ms();
                self.seed(name, now);
            }
        }
        Ok(())
    }

    fn untrack(&mut self, name: &str) {
        if let Some(t) = self.tracked.remove(name)
            && let Some(d) = t.next
        {
            self.window.remove(&(d, name.to_owned()));
        }
    }

    fn parse(&self, spec: &ScheduleSpec) -> Option<Arc<CronSpec>> {
        match parse_spec(spec) {
            Ok(c) => Some(Arc::new(c)),
            Err(e) => {
                warn!(partition = self.p(), schedule = %spec.name, error = %e, "schedule has an invalid cron spec; not scheduled");
                None
            }
        }
    }

    /// Put `name`'s next fire after `max(floor, now)` in the window.
    fn seed(&mut self, name: &str, now: u64) {
        let Some(t) = self.tracked.get_mut(name) else {
            return;
        };
        if let Some(d) = t.next.take() {
            self.window.remove(&(d, name.to_owned()));
        }
        let Some(cron) = &t.cron else { return };
        if let Some(next) = next_fire_after(cron, t.floor.max(now)) {
            t.next = Some(next);
            self.window.insert((next, name.to_owned()));
        }
    }

    /// Activation: read the durable watermark, run catch-up, seed the window.
    async fn activate(&mut self, spec: &ScheduleSpec) -> Result<(), Stop> {
        let name = spec.name.clone();
        self.untrack(&name);
        let reader = self.ctx.handle.reader();
        let row = {
            let n = name.clone();
            tokio::task::spawn_blocking(move || schedule_state(&reader, &n))
                .await
                .map_err(|e| e.to_string())
                .and_then(|r| r.map_err(|e| e.to_string()))
        };
        let watermark = match row {
            Ok(row) => incarnation_watermark(row.as_ref(), spec.created_at_ms),
            Err(e) => {
                if self.ctx.handle.machine().check_healthy().is_err() {
                    error!(partition = self.p(), error = %e, "schedule_state read failed; partition failing, cron driver stops");
                    return Err(Stop);
                }
                // The floor still protects exactly-once in apply; start from created_at.
                warn!(partition = self.p(), schedule = %name, error = %e, "schedule_state read failed; using created_at as floor");
                0
            }
        };
        let cron = self.parse(spec);
        self.tracked.insert(
            name.clone(),
            Tracked {
                spec: spec.clone(),
                cron,
                floor: watermark.max(spec.created_at_ms),
                next: None,
            },
        );
        let now = self.ctx.clock.now_ms();
        self.catch_up(&name, now).await?;
        self.seed(&name, now);
        debug!(partition = self.p(), schedule = %name, watermark, "schedule activated");
        Ok(())
    }

    /// A window entry is due.
    async fn on_due(&mut self, name: &str, due: u64, now: u64) -> Result<(), Stop> {
        let Some(t) = self.tracked.get(name) else {
            return Ok(());
        };
        let Some(cron) = t.cron.clone() else {
            return Ok(());
        };
        let behind = next_fire_after(&cron, due).is_some_and(|n| n <= now);
        if behind {
            // More than one fire missed while running: the catch-up policy decides.
            self.catch_up(name, now).await?;
        } else if due > t.floor {
            self.fire(name, due, ScheduleFireKind::Fire, 0).await?;
        }
        self.seed(name, now);
        Ok(())
    }

    /// Plan and submit catch-up for `name` at `now` (floor = max(watermark, created_at)).
    async fn catch_up(&mut self, name: &str, now: u64) -> Result<(), Stop> {
        let Some(t) = self.tracked.get(name) else {
            return Ok(());
        };
        let Some(cron) = t.cron.clone() else {
            return Ok(());
        };
        let policy = CatchupPolicy::try_from(t.spec.catchup).unwrap_or(CatchupPolicy::Skip);
        let to_i64 = |v: u64| i64::try_from(v).unwrap_or(i64::MAX);
        let plan = plan_catchup(
            &cron,
            to_i64(t.floor),
            to_i64(t.spec.created_at_ms),
            to_i64(now),
            policy,
            t.spec.catchup_window_ms,
            usize::try_from(self.cfg.catchup_cap.max(1)).unwrap_or(usize::MAX),
        );
        if plan.advance_to.is_none() && plan.fire.is_empty() {
            return Ok(());
        }
        info!(
            partition = self.p(),
            schedule = %name,
            policy = ?policy,
            fires = plan.fire.len(),
            advance_to = ?plan.advance_to,
            skipped = plan.skipped,
            "cron catch-up"
        );
        if let Some(a) = plan.advance_to.and_then(|a| u64::try_from(a).ok()) {
            self.fire(name, a, ScheduleFireKind::Advance, plan.skipped)
                .await?;
        }
        let mut skipped = plan.skipped;
        for f in plan.fire.iter().filter_map(|f| u64::try_from(*f).ok()) {
            self.fire(name, f, ScheduleFireKind::Fire, skipped).await?;
            skipped = 0;
        }
        Ok(())
    }

    /// Submit one `ScheduleFire` (Fire or Advance) at `ft` and move the in-memory floor.
    async fn fire(
        &mut self,
        name: &str,
        ft: u64,
        kind: ScheduleFireKind,
        skipped: u32,
    ) -> Result<(), Stop> {
        let Some(spec) = self.tracked.get(name).map(|t| t.spec.clone()) else {
            return Ok(());
        };
        let run = RunId::new_v7(self.ctx.partition);
        let mut delay = RETRY;
        let start = if kind == ScheduleFireKind::Advance {
            None
        } else {
            loop {
                let req = FireRequest {
                    run,
                    idempotency_key: cron_idempotency_key(name, ft),
                    fire_time_ms: ft,
                    now_ms: self.ctx.clock.now_ms(),
                };
                match self.source.build_start(&self.ctx.handle, &spec, req).await {
                    Ok(s) => break Some(s),
                    Err(StartError::Skip(why)) => {
                        warn!(partition = self.p(), schedule = %name, fire_time = ft, reason = %why, "cron fire dropped: no run can be started");
                        count(name, "start_error");
                        self.bump_floor(name, ft);
                        return Ok(());
                    }
                    Err(StartError::Retry(why)) => {
                        warn!(partition = self.p(), schedule = %name, fire_time = ft, reason = %why, "cron fire start build failed; retrying");
                        self.backoff(&mut delay).await?;
                    }
                }
            }
        };
        let cmd = ScheduleFire {
            schedule: name.to_owned(),
            fire_time_ms: ft,
            kind: kind as i32,
            start,
            overlap: u32::from(spec.overlap),
            request_id: Vec::new(),
            skipped,
            schedule_created_at_ms: spec.created_at_ms,
        };
        let outcome = loop {
            match self
                .ctx
                .handle
                .submit_one(Command::schedule_fire(cmd.clone()))
                .await
            {
                Ok(o) => break o,
                Err(e) => {
                    if matches!(e, PartitionError::NotLeader | PartitionError::Failed(_))
                        || self.ctx.handle.machine().check_healthy().is_err()
                    {
                        error!(partition = self.p(), error = %e, "cron fire submit failed; cron driver stops");
                        return Err(Stop);
                    }
                    warn!(partition = self.p(), schedule = %name, error = %e, "cron fire submit failed; retrying");
                    self.backoff(&mut delay).await?;
                }
            }
        };
        let label = match outcome {
            ApplyOutcome::Accepted if kind == ScheduleFireKind::Advance => "advanced",
            ApplyOutcome::Accepted => self.classify(name, &run, ft).await,
            ApplyOutcome::Duplicate { .. } => "duplicate",
            ApplyOutcome::Rejected(ref r) => {
                warn!(partition = self.p(), schedule = %name, fire_time = ft, reason = ?r, "ScheduleFire rejected");
                "rejected"
            }
        };
        debug!(partition = self.p(), schedule = %name, fire_time = ft, kind = ?kind, outcome = label, "cron fire applied");
        count(name, label);
        self.bump_floor(name, ft);
        Ok(())
    }

    fn bump_floor(&mut self, name: &str, ft: u64) {
        if let Some(t) = self.tracked.get_mut(name) {
            t.floor = t.floor.max(ft);
        }
    }

    /// An accepted `Fire`: started (our run is `last_run`), buffered or skipped by overlap.
    async fn classify(&self, name: &str, run: &RunId, ft: u64) -> &'static str {
        let reader = self.ctx.handle.reader();
        let n = name.to_owned();
        let row = tokio::task::spawn_blocking(move || schedule_state(&reader, &n))
            .await
            .ok()
            .and_then(Result::ok)
            .flatten();
        match row {
            Some(r) if r.last_run == Some(*run) => "started",
            Some(r) if r.buffered.as_ref().is_some_and(|b| b.fire_time_ms == ft) => "buffered",
            Some(_) => "skipped",
            None => "started",
        }
    }

    /// Sleep `delay` (doubling it up to [`MAX_RETRY`]); `Err` on shutdown.
    async fn backoff(&self, delay: &mut Duration) -> Result<(), Stop> {
        let mut shutdown = self.ctx.shutdown.clone();
        if *shutdown.borrow() {
            return Err(Stop);
        }
        tokio::select! {
            _ = shutdown.wait_for(|v| *v) => return Err(Stop),
            () = tokio::time::sleep(*delay) => {}
        }
        *delay = (*delay * 2).min(MAX_RETRY);
        Ok(())
    }
}

fn count(schedule: &str, outcome: &'static str) {
    metrics::counter!(
        "ostro_cron_fires_total",
        "schedule" => schedule.to_owned(),
        "outcome" => outcome
    )
    .increment(1);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tag_is_bounded() {
        assert_eq!(schedule_tag("nightly"), "ostro.schedule=nightly");
        let long = "x".repeat(200);
        assert_eq!(schedule_tag(&long).len(), MAX_RUN_TAG_LEN);
    }

    #[test]
    fn watermark_is_incarnation_aware() {
        let row = ScheduleStateRow {
            schedule_created_at_ms: 10,
            watermark_ms: 99,
            ..Default::default()
        };
        assert_eq!(incarnation_watermark(Some(&row), 10), 99);
        assert_eq!(incarnation_watermark(Some(&row), 11), 0);
        assert_eq!(incarnation_watermark(None, 10), 0);
    }
}
