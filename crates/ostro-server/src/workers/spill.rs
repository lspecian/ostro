//! Spill hysteresis for `auto` queues (plan 07-13, WORK-03, D-W9, research Pattern 7).
//!
//! Every `auto` queue has a [`SpillController`]: a two-state machine (`Embedded`,
//! `Spilling`) fed one [`LoadSample`] per second.
//!
//! - **Enter** (`Embedded → Spilling`): process CPU above `cpu_pct` for `sustain_ms`, or
//!   embedded slot use above `slots_pct`, or Wasm memory above `mem_pct`, or queue lag
//!   above `lag_ms` (the last three immediately).
//! - **Exit** (`Spilling → Embedded`, plan-check B6), all of:
//!   1. at least `cooldown_ms` spent in `Spilling` (minimum dwell);
//!   2. every signal below `entry × hysteresis_pct / 100` for the last full
//!      `cooldown_ms` (any excursion restarts the window);
//!   3. either nothing runs on workers and no overflow attempt happened in the last
//!      `cooldown_ms`, or the queue has no admissible backlog.
//!
//!   Condition 2 is always required, so a queue never leaves `Spilling` while one of its
//!   entry conditions still holds, with or without workers.
//!
//! Time is the injected clock's milliseconds (D-W26, Phase 10 F5): the controller only
//! sees the `now_ms` it is given.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arc_swap::ArcSwap;
use ostro_engine::limits::NodeMemoryBudget;
use ostro_meta::records::PLACEMENT_AUTO;
use ostro_meta::{MetaView, SpillPolicy};
use ostro_partition::{Clock, QueueGauge, QueueGauges};
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::config::{PlacementSetting, SpillSection};
use crate::p7::P7Ctx;

/// Spill thresholds of one queue (D-W9 defaults, `[workers.spill]`, `QueuePolicy.spill`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SpillThresholds {
    /// Process CPU entry threshold, % of the available parallelism.
    pub cpu_pct: u32,
    /// Embedded slot use entry threshold, %.
    pub slots_pct: u32,
    /// Wasm memory budget entry threshold, %.
    pub mem_pct: u32,
    /// Queue lag entry threshold.
    pub lag_ms: u64,
    /// How long the CPU signal must stay above `cpu_pct`.
    pub sustain_ms: u64,
    /// Minimum dwell in `Spilling` and the exit window.
    pub cooldown_ms: u64,
    /// Exit thresholds are `entry × hysteresis_pct / 100`.
    pub hysteresis_pct: u32,
}

impl Default for SpillThresholds {
    /// D-W9: 70 % / 90 % / 85 % / 5 s, × 0.85, sustain 10 s, cool-down 30 s.
    fn default() -> Self {
        Self {
            cpu_pct: 70,
            slots_pct: 90,
            mem_pct: 85,
            lag_ms: 5_000,
            sustain_ms: 10_000,
            cooldown_ms: 30_000,
            hysteresis_pct: 85,
        }
    }
}

fn ms(d: std::time::Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

impl SpillThresholds {
    /// The node defaults from `[workers.spill]`.
    pub fn from_config(s: &SpillSection) -> Self {
        Self {
            cpu_pct: s.cpu_pct,
            slots_pct: s.slots_pct,
            mem_pct: s.mem_pct,
            lag_ms: ms(s.lag.0),
            sustain_ms: ms(s.sustain.0),
            cooldown_ms: ms(s.cooldown.0),
            hysteresis_pct: s.hysteresis_pct,
        }
    }

    /// The queue's thresholds: `QueuePolicy.spill` replaces the node defaults wholesale.
    pub fn with_policy(self, p: Option<&SpillPolicy>) -> Self {
        match p {
            None => self,
            Some(p) => Self {
                cpu_pct: u32::from(p.cpu_pct),
                slots_pct: u32::from(p.slots_pct),
                mem_pct: u32::from(p.mem_pct),
                lag_ms: p.lag_ms,
                sustain_ms: p.sustain_ms,
                cooldown_ms: p.cooldown_ms,
                hysteresis_pct: u32::from(p.hysteresis_pct),
            },
        }
    }

    fn entry_frac(pct: u32) -> f64 {
        f64::from(pct) / 100.0
    }

    fn exit_frac(&self, pct: u32) -> f64 {
        f64::from(pct) * f64::from(self.hysteresis_pct) / 10_000.0
    }

    /// Exit threshold of the lag signal, ms.
    fn exit_lag_ms(&self) -> u64 {
        u64::try_from(u128::from(self.lag_ms) * u128::from(self.hysteresis_pct) / 100)
            .unwrap_or(u64::MAX)
    }

    /// Which signal is over its entry threshold (CPU ignored: it needs `sustain_ms`).
    fn immediate_trigger(&self, s: &LoadSample) -> Option<Signal> {
        if s.slots > Self::entry_frac(self.slots_pct) {
            Some(Signal::Slots)
        } else if s.mem > Self::entry_frac(self.mem_pct) {
            Some(Signal::Mem)
        } else if s.lag_ms > self.lag_ms {
            Some(Signal::Lag)
        } else {
            None
        }
    }

    /// Every signal below its exit threshold.
    fn calm(&self, s: &LoadSample) -> bool {
        s.cpu < self.exit_frac(self.cpu_pct)
            && s.slots < self.exit_frac(self.slots_pct)
            && s.mem < self.exit_frac(self.mem_pct)
            && s.lag_ms < self.exit_lag_ms()
    }
}

/// One observation of a queue's load.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct LoadSample {
    /// Process CPU, 0..=1 of the available parallelism.
    pub cpu: f64,
    /// Embedded slot use, 0..=1.
    pub slots: f64,
    /// Wasm memory budget use, 0..=1.
    pub mem: f64,
    /// Now minus the queue's oldest `ready_at_ms` (0 when nothing is ready).
    pub lag_ms: u64,
    /// Admissible ready entries of the queue (`QueueGauge::ready_unblocked`).
    pub backlog: u64,
    /// Leases of the queue running on workers.
    pub running_on_workers: u64,
    /// Last overflow attempt (placement wanted a worker), injected-clock ms.
    pub last_overflow_ms: Option<u64>,
}

/// Spill state of a queue.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum SpillState {
    #[default]
    Embedded,
    Spilling,
}

impl SpillState {
    /// Metric label.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Embedded => "embedded",
            Self::Spilling => "spilling",
        }
    }
}

/// What caused a transition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Signal {
    Cpu,
    Slots,
    Mem,
    Lag,
    /// Exit: every signal calm for the cool-down and the workers drained (or no backlog).
    Cooldown,
}

impl Signal {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cpu => "cpu",
            Self::Slots => "slots",
            Self::Mem => "mem",
            Self::Lag => "lag",
            Self::Cooldown => "cooldown",
        }
    }
}

/// A state change returned by [`SpillController::observe`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Transition {
    pub to: SpillState,
    pub signal: Signal,
    pub at_ms: u64,
}

/// The per-queue hysteresis state machine (pure: no clock, no I/O).
#[derive(Clone, Debug)]
pub struct SpillController {
    th: SpillThresholds,
    state: SpillState,
    /// Embedded: since when CPU has been above its entry threshold.
    cpu_over_since: Option<u64>,
    /// Spilling: when the state was entered.
    spilling_since: u64,
    /// Spilling: since when every signal has been calm.
    calm_since: Option<u64>,
}

impl SpillController {
    pub fn new(th: SpillThresholds) -> Self {
        Self {
            th,
            state: SpillState::Embedded,
            cpu_over_since: None,
            spilling_since: 0,
            calm_since: None,
        }
    }

    pub fn state(&self) -> SpillState {
        self.state
    }

    pub fn spilling(&self) -> bool {
        self.state == SpillState::Spilling
    }

    pub fn thresholds(&self) -> SpillThresholds {
        self.th
    }

    /// Replace the thresholds (policy change); the state and windows are kept.
    pub fn set_thresholds(&mut self, th: SpillThresholds) {
        self.th = th;
    }

    /// Feed one sample taken at `now_ms`; returns the transition it caused, if any.
    pub fn observe(&mut self, s: LoadSample, now_ms: u64) -> Option<Transition> {
        match self.state {
            SpillState::Embedded => {
                let cpu_over = s.cpu > SpillThresholds::entry_frac(self.th.cpu_pct);
                if cpu_over {
                    self.cpu_over_since.get_or_insert(now_ms);
                } else {
                    self.cpu_over_since = None;
                }
                let cpu_sustained = self
                    .cpu_over_since
                    .is_some_and(|t| now_ms.saturating_sub(t) >= self.th.sustain_ms);
                let signal = self
                    .th
                    .immediate_trigger(&s)
                    .or(cpu_sustained.then_some(Signal::Cpu))?;
                self.state = SpillState::Spilling;
                self.spilling_since = now_ms;
                self.calm_since = None;
                self.cpu_over_since = None;
                Some(Transition {
                    to: SpillState::Spilling,
                    signal,
                    at_ms: now_ms,
                })
            }
            SpillState::Spilling => {
                if self.th.calm(&s) {
                    self.calm_since.get_or_insert(now_ms);
                } else {
                    self.calm_since = None;
                }
                let cd = self.th.cooldown_ms;
                let dwell = now_ms.saturating_sub(self.spilling_since) >= cd;
                let calm = self
                    .calm_since
                    .is_some_and(|t| now_ms.saturating_sub(t) >= cd);
                let window_start = now_ms.saturating_sub(cd);
                let no_overflow = s.last_overflow_ms.is_none_or(|t| t < window_start);
                let drained = (s.running_on_workers == 0 && no_overflow) || s.backlog == 0;
                if !(dwell && calm && drained) {
                    return None;
                }
                self.state = SpillState::Embedded;
                self.calm_since = None;
                self.cpu_over_since = None;
                Some(Transition {
                    to: SpillState::Embedded,
                    signal: Signal::Cooldown,
                    at_ms: now_ms,
                })
            }
        }
    }
}

// ---------------------------------------------------------------------------------
// Signals
// ---------------------------------------------------------------------------------

/// Where [`SpillService`] gets its samples from.
pub trait LoadSampler: Send {
    /// Called once per tick before the per-queue samples (refresh process-wide signals).
    fn begin(&mut self) {}
    /// The load of `queue` (`last_overflow_ms` is merged with [`SpillHandle`]'s by the
    /// service).
    fn sample(&mut self, queue: &str) -> LoadSample;
}

/// Process user + system CPU time (`getrusage(RUSAGE_SELF)`), `None` if the call fails.
fn process_cpu_time() -> Option<Duration> {
    // SAFETY: `getrusage` only writes a `struct rusage` through the pointer, which points
    // at a zero-initialised, properly aligned `MaybeUninit<rusage>` owned by this frame;
    // `RUSAGE_SELF` is always a valid `who`. On success (0) every field is written, and a
    // zeroed `rusage` is a valid value anyway, so `assume_init` is sound.
    let ru = unsafe {
        let mut ru = std::mem::MaybeUninit::<libc::rusage>::zeroed();
        (libc::getrusage(libc::RUSAGE_SELF, ru.as_mut_ptr()) == 0).then(|| ru.assume_init())
    }?;
    let tv = |t: libc::timeval| {
        let s = u64::try_from(t.tv_sec).unwrap_or(0);
        let us = u64::try_from(t.tv_usec).unwrap_or(0);
        Duration::from_secs(s) + Duration::from_micros(us)
    };
    Some(tv(ru.ru_utime) + tv(ru.ru_stime))
}

/// Process CPU utilisation between two calls, as a fraction of `available_parallelism`.
#[derive(Debug)]
pub struct CpuMeter {
    parallelism: f64,
    last: Option<(std::time::Instant, Duration)>,
}

impl Default for CpuMeter {
    fn default() -> Self {
        Self::new()
    }
}

impl CpuMeter {
    /// Starts measuring now (the first [`sample`](Self::sample) covers the time since).
    pub fn new() -> Self {
        let parallelism = std::thread::available_parallelism().map_or(1, |n| n.get());
        let mut m = Self {
            parallelism: parallelism as f64,
            last: None,
        };
        m.last = m.read();
        m
    }

    #[allow(clippy::disallowed_methods)]
    fn read(&self) -> Option<(std::time::Instant, Duration)> {
        // clock-exempt: process CPU-time delta (getrusage); the wall delta is its
        // denominator, never used for spill windows.
        let wall = std::time::Instant::now();
        process_cpu_time().map(|cpu| (wall, cpu))
    }

    /// Δ(user + system CPU) / (Δwall × available_parallelism), clamped to 0..=1.
    pub fn sample(&mut self) -> f64 {
        let Some(now) = self.read() else { return 0.0 };
        let Some(prev) = self.last.replace(now) else {
            return 0.0;
        };
        let wall = now.0.saturating_duration_since(prev.0).as_secs_f64();
        if wall <= 0.0 {
            return 0.0;
        }
        let cpu = now.1.saturating_sub(prev.1).as_secs_f64();
        (cpu / (wall * self.parallelism)).clamp(0.0, 1.0)
    }
}

/// The node's real signals: getrusage CPU, embedded slot use, the engine's Wasm memory
/// budget and the queue lag / backlog / worker leases from [`QueueGauges`].
pub struct ProcessSampler {
    clock: Arc<dyn Clock>,
    gauges: Arc<QueueGauges>,
    slots: Arc<Semaphore>,
    slots_total: usize,
    budget: Option<Arc<NodeMemoryBudget>>,
    cpu_meter: CpuMeter,
    // Per tick.
    cpu: f64,
    slot_use: f64,
    mem: f64,
    now_ms: u64,
    snapshot: BTreeMap<String, QueueGauge>,
}

impl std::fmt::Debug for ProcessSampler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProcessSampler")
            .field("slots_total", &self.slots_total)
            .field("cpu", &self.cpu)
            .field("slots", &self.slot_use)
            .field("mem", &self.mem)
            .finish_non_exhaustive()
    }
}

impl ProcessSampler {
    /// `slots` is the embedded slot semaphore with `slots_total` permits (`[executor]
    /// slots`); `budget` the engine's Wasm memory budget (`None` = signal 0).
    pub fn new(
        clock: Arc<dyn Clock>,
        gauges: Arc<QueueGauges>,
        slots: Arc<Semaphore>,
        slots_total: usize,
        budget: Option<Arc<NodeMemoryBudget>>,
    ) -> Self {
        Self {
            clock,
            gauges,
            slots,
            slots_total,
            budget,
            cpu_meter: CpuMeter::new(),
            cpu: 0.0,
            slot_use: 0.0,
            mem: 0.0,
            now_ms: 0,
            snapshot: BTreeMap::new(),
        }
    }

    fn ratio(used: u64, limit: u64) -> f64 {
        if limit == 0 {
            0.0
        } else {
            (used as f64 / limit as f64).clamp(0.0, 1.0)
        }
    }
}

impl LoadSampler for ProcessSampler {
    fn begin(&mut self) {
        self.cpu = self.cpu_meter.sample();
        let total = self.slots_total;
        let free = self.slots.available_permits().min(total);
        self.slot_use = Self::ratio((total - free) as u64, total as u64);
        self.mem = self
            .budget
            .as_ref()
            .map_or(0.0, |b| Self::ratio(b.used(), b.limit()));
        self.now_ms = self.clock.now_ms();
        self.snapshot = self.gauges.snapshot();
    }

    fn sample(&mut self, queue: &str) -> LoadSample {
        let g = self.snapshot.get(queue).copied().unwrap_or_default();
        LoadSample {
            cpu: self.cpu,
            slots: self.slot_use,
            mem: self.mem,
            lag_ms: g
                .oldest_ready_at_ms
                .map_or(0, |t| self.now_ms.saturating_sub(t)),
            backlog: g.ready_unblocked,
            running_on_workers: g.running_on_workers,
            last_overflow_ms: None,
        }
    }
}

// ---------------------------------------------------------------------------------
// Shared state
// ---------------------------------------------------------------------------------

/// Spill state shared by placement (07-12) and demand (07-16).
///
/// `spilling` is read on the dispatcher's reserve path: one atomic load plus a map
/// lookup. Overflow attempts go through a small mutex. Every transition bumps
/// [`changed`](Self::changed), which `NodePlacement` merges into `Placement::changed()`.
pub struct SpillHandle {
    spilling: ArcSwap<BTreeMap<String, bool>>,
    /// Serialises writers of `spilling`.
    write: Mutex<()>,
    overflow: Mutex<BTreeMap<String, u64>>,
    changed: watch::Sender<u64>,
}

impl std::fmt::Debug for SpillHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpillHandle")
            .field("spilling", &self.spilling_queues())
            .field("version", &*self.changed.borrow())
            .finish()
    }
}

impl Default for SpillHandle {
    fn default() -> Self {
        Self {
            spilling: ArcSwap::from_pointee(BTreeMap::new()),
            write: Mutex::new(()),
            overflow: Mutex::new(BTreeMap::new()),
            changed: watch::channel(0).0,
        }
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl SpillHandle {
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether `queue` currently spills to workers.
    pub fn spilling(&self, queue: &str) -> bool {
        self.spilling.load().get(queue).copied().unwrap_or(false)
    }

    /// Placement found the embedded slots full for `queue` at `now_ms` and wanted a worker
    /// (injected clock, B6), whether or not it found one.
    pub fn note_overflow(&self, queue: &str, now_ms: u64) {
        let mut m = lock(&self.overflow);
        match m.get_mut(queue) {
            Some(t) => *t = (*t).max(now_ms),
            None => {
                m.insert(queue.to_owned(), now_ms);
            }
        }
    }

    /// The last overflow attempt of `queue`.
    pub fn last_overflow(&self, queue: &str) -> Option<u64> {
        lock(&self.overflow).get(queue).copied()
    }

    /// Bumped (a transition counter) on every spill transition.
    pub fn changed(&self) -> watch::Receiver<u64> {
        self.changed.subscribe()
    }

    /// Queues currently spilling.
    pub fn spilling_queues(&self) -> Vec<String> {
        self.spilling
            .load()
            .iter()
            .filter(|(_, s)| **s)
            .map(|(q, _)| q.clone())
            .collect()
    }

    /// Set `queue`'s state; bumps [`changed`](Self::changed) and returns `true` when it
    /// changed. Used by the spill service (and tests forcing a state).
    pub fn set(&self, queue: &str, spilling: bool) -> bool {
        let _g = lock(&self.write);
        let cur = self.spilling.load_full();
        if cur.get(queue).copied().unwrap_or(false) == spilling {
            return false;
        }
        let mut next = (*cur).clone();
        if spilling {
            next.insert(queue.to_owned(), true);
        } else {
            next.remove(queue);
        }
        self.spilling.store(Arc::new(next));
        self.changed.send_modify(|v| *v = v.wrapping_add(1));
        true
    }

    /// Forget `queue` (it left `auto`): not spilling, no overflow record.
    pub fn remove(&self, queue: &str) -> bool {
        lock(&self.overflow).remove(queue);
        self.set(queue, false)
    }
}

// ---------------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------------

/// How often the spill service samples (D-W9).
pub const SAMPLE_PERIOD: Duration = Duration::from_secs(1);

/// One controller per `auto` queue over a [`LoadSampler`].
pub struct SpillService<S> {
    sampler: S,
    handle: Arc<SpillHandle>,
    clock: Arc<dyn Clock>,
    queues: BTreeMap<String, SpillController>,
}

impl<S> std::fmt::Debug for SpillService<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpillService")
            .field("queues", &self.queues.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

impl<S: LoadSampler> SpillService<S> {
    pub fn new(sampler: S, handle: Arc<SpillHandle>, clock: Arc<dyn Clock>) -> Self {
        Self {
            sampler,
            handle,
            clock,
            queues: BTreeMap::new(),
        }
    }

    /// Queues with a controller (the `auto` queues of the last tick).
    pub fn queues(&self) -> Vec<String> {
        self.queues.keys().cloned().collect()
    }

    /// One evaluation: `auto` maps every `auto` queue to its thresholds. Queues no longer
    /// in it are dropped (and stop spilling). Returns the transitions applied.
    pub fn tick(&mut self, auto: &BTreeMap<String, SpillThresholds>) -> Vec<(String, Transition)> {
        let gone: Vec<String> = self
            .queues
            .keys()
            .filter(|q| !auto.contains_key(*q))
            .cloned()
            .collect();
        for q in gone {
            self.queues.remove(&q);
            if self.handle.remove(&q) {
                info!(queue = %q, "queue left auto placement; spill cleared");
                transition_metrics(&q, SpillState::Embedded, true);
            }
            metrics::gauge!("ostro_placement_spilling", "queue" => q).set(0.0);
        }
        if auto.is_empty() {
            return Vec::new();
        }
        self.sampler.begin();
        let now = self.clock.now_ms();
        let mut out = Vec::new();
        for (q, th) in auto {
            let c = self
                .queues
                .entry(q.clone())
                .or_insert_with(|| SpillController::new(*th));
            if c.thresholds() != *th {
                c.set_thresholds(*th);
            }
            let mut s = self.sampler.sample(q);
            s.last_overflow_ms = s.last_overflow_ms.max(self.handle.last_overflow(q));
            if let Some(t) = c.observe(s, now) {
                let spilling = t.to == SpillState::Spilling;
                self.handle.set(q, spilling);
                info!(
                    queue = %q,
                    to = t.to.as_str(),
                    signal = t.signal.as_str(),
                    cpu = s.cpu,
                    slots = s.slots,
                    mem = s.mem,
                    lag_ms = s.lag_ms,
                    backlog = s.backlog,
                    running_on_workers = s.running_on_workers,
                    "placement spill transition"
                );
                transition_metrics(q, t.to, true);
                out.push((q.clone(), t));
            }
        }
        out
    }
}

fn transition_metrics(queue: &str, to: SpillState, counted: bool) {
    if counted {
        metrics::counter!(
            "ostro_placement_transitions_total",
            "queue" => queue.to_owned(),
            "to" => to.as_str()
        )
        .increment(1);
    }
    let v = if to == SpillState::Spilling { 1.0 } else { 0.0 };
    metrics::gauge!("ostro_placement_spilling", "queue" => queue.to_owned()).set(v);
}

/// Run `svc` every `period` until `shutdown`: `auto(tracked)` returns the current `auto`
/// queues (given the queues tracked so far). The deadline comes from `clock`; tokio
/// only sleeps until it (no `interval`, F5).
pub async fn run<S: LoadSampler>(
    mut svc: SpillService<S>,
    mut auto: impl FnMut(&[String]) -> BTreeMap<String, SpillThresholds> + Send,
    clock: Arc<dyn Clock>,
    shutdown: CancellationToken,
    period: Duration,
) {
    let period_ms = u64::try_from(period.as_millis()).unwrap_or(u64::MAX).max(1);
    let mut next_ms = clock.now_ms();
    loop {
        let queues = auto(&svc.queues());
        svc.tick(&queues);
        next_ms = next_ms.saturating_add(period_ms);
        let now = clock.now_ms();
        if next_ms < now {
            // Fell behind (or the clock jumped): skip the missed ticks.
            next_ms = now;
        }
        let wait = Duration::from_millis((next_ms - now).max(1));
        tokio::select! {
            biased;
            () = shutdown.cancelled() => break,
            () = tokio::time::sleep(wait) => {}
        }
    }
}

/// The `auto` queues among `names`, with their thresholds.
pub fn auto_queues(
    meta: &MetaView,
    default_auto: bool,
    defaults: SpillThresholds,
    names: impl IntoIterator<Item = String>,
) -> BTreeMap<String, SpillThresholds> {
    let names: BTreeSet<String> = names.into_iter().collect();
    meta.read(|s| {
        names
            .into_iter()
            .filter_map(|q| {
                let p = s.queue_policy(&q).map(|v| &v.value);
                let auto = match p.and_then(|p| p.placement) {
                    Some(pl) => pl == PLACEMENT_AUTO,
                    None => default_auto,
                };
                auto.then(|| {
                    let th = defaults.with_policy(p.and_then(|p| p.spill.as_ref()));
                    (q, th)
                })
            })
            .collect()
    })
}

/// Start the spill sampler (every [`SAMPLE_PERIOD`]); `None` when `[workers] enabled =
/// false` (nothing can spill). Candidate queues are those with dispatcher gauges plus the
/// ones already tracked; `auto` comes from `QueuePolicy.placement` or `[workers]
/// default_placement`.
pub fn start(ctx: &P7Ctx) -> Option<JoinHandle<()>> {
    let workers = &ctx.config.workers;
    if !workers.enabled {
        return None;
    }
    let shared = &ctx.shared;
    let slots_total = usize::try_from(ctx.config.executor.slots).unwrap_or(usize::MAX);
    let sampler = ProcessSampler::new(
        Arc::clone(&ctx.clock),
        Arc::clone(&shared.gauges),
        Arc::clone(&shared.embedded_slots),
        slots_total,
        Some(Arc::clone(ctx.runtime.parked().budget())),
    );
    let svc = SpillService::new(sampler, Arc::clone(&shared.spill), Arc::clone(&ctx.clock));
    let meta = ctx.meta.clone();
    let gauges = Arc::clone(&shared.gauges);
    let default_auto = workers.default_placement == PlacementSetting::Auto;
    let defaults = SpillThresholds::from_config(&workers.spill);
    let auto = move |tracked: &[String]| {
        let names = gauges.snapshot().into_keys().chain(tracked.iter().cloned());
        auto_queues(&meta, default_auto, defaults, names)
    };
    Some(tokio::spawn(run(
        svc,
        auto,
        Arc::clone(&ctx.clock),
        ctx.shutdown.clone(),
        SAMPLE_PERIOD,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exit_thresholds_are_entry_times_hysteresis() {
        let th = SpillThresholds::default();
        assert!((th.exit_frac(th.cpu_pct) - 0.595).abs() < 1e-9);
        assert!((th.exit_frac(th.slots_pct) - 0.765).abs() < 1e-9);
        assert!((th.exit_frac(th.mem_pct) - 0.7225).abs() < 1e-9);
        assert_eq!(th.exit_lag_ms(), 4_250);
    }

    #[test]
    fn config_defaults_match_dw9() {
        assert_eq!(
            SpillThresholds::from_config(&SpillSection::default()),
            SpillThresholds::default()
        );
    }

    #[test]
    fn immediate_signals_enter_without_sustain() {
        for (s, sig) in [
            (
                LoadSample {
                    slots: 0.95,
                    ..Default::default()
                },
                Signal::Slots,
            ),
            (
                LoadSample {
                    mem: 0.9,
                    ..Default::default()
                },
                Signal::Mem,
            ),
            (
                LoadSample {
                    lag_ms: 6_000,
                    ..Default::default()
                },
                Signal::Lag,
            ),
        ] {
            let mut c = SpillController::new(SpillThresholds::default());
            let t = c.observe(s, 1_000).expect("enters");
            assert_eq!((t.to, t.signal), (SpillState::Spilling, sig));
        }
    }
}
