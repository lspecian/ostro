//! The node-level guest log sink (08-17, OBS-03, P8-D16).
//!
//! Every live guest log line (the engine drops lines while replaying and bounds each
//! activation to 1000 lines / 1 MiB) reaches [`GuestLogSink::emit`] through the
//! activation sink (`PartitionSink::guest_log`). The sink applies, in order:
//!
//! 1. the **node** token bucket (`node_rate_per_sec`, default 5,000 lines/s, burst one
//!    second's worth) -> drop reason `node_rate`;
//! 2. the **per-run** token bucket (`rate_per_sec` 50/s, `burst` 500) -> `run_rate`;
//! 3. the **per-run lifetime cap** (`run_lines_cap` 10,000 lines / `run_bytes_cap`
//!    10 MiB) -> `run_cap`, with one `warn` node-log line per run when the cap is first
//!    hit.
//!
//! A line is only charged against a bucket when it passes all three checks, so a capped
//! run does not drain the node bucket. An accepted line is
//!
//! - (a) pushed to the run's **ring buffer** (the last `ring_lines`, default 1,000). Runs
//!   form an LRU under `ring_budget_bytes` (default 64 MiB): when the node total exceeds
//!   the budget, the least recently written run loses its whole ring (a single run larger
//!   than the budget loses its oldest lines instead). Served by
//!   `GET /v1/runs/{id}/logs` (`control/run_logs.rs`). Non-durable: logs are never
//!   journaled;
//! - (b) written to the node log as `tracing` target `ostro::guest` at the guest's level,
//!   with `run_id`, `workflow`, `attempt` and `guest.fields` (the guest's key/values as
//!   one JSON object string: `tracing` field names are static, so dynamic keys cannot be
//!   real fields; under `LogFormat::Json` the line is
//!   `{"fields":{"message":..,"run_id":..,"workflow":..,"attempt":..,"guest.fields":"{\"k\":\"v\"}"},..}`);
//!   the OTLP log bridge excludes `ostro::guest` (W4), so (b) is not exported twice;
//! - (c) exported as one **OTLP LogRecord** through the server's logger provider (scope
//!   `ostro.guest`): body = message, severity from the level, attributes = every guest
//!   field (string values) plus `run.id`, `workflow`, `attempt` (int), and the
//!   activation span's trace/span ids.
//!
//! Metrics: `ostro_guest_log_lines_total{level}` and
//! `ostro_guest_log_dropped_total{reason}` with reasons `activation_bound` (engine),
//! `node_rate`, `run_rate`, `run_cap`. Labels never carry the run id.
//!
//! Limiter state is per run id and in memory: lifetime caps **reset on restart** (P8-D16,
//! best effort). A run's state is forgotten once it has had no line for
//! [`RUN_STATE_IDLE_MS`] and its ring is empty, or when more than [`MAX_RUN_STATES`]
//! runs are tracked (least recently seen first).
//!
//! Every time read goes through the injected [`Clock`] (Phase 10 F5).

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use opentelemetry::logs::{AnyValue, LogRecord as _, Logger as _, LoggerProvider as _, Severity};
use opentelemetry::{SpanId, TraceFlags, TraceId};
use opentelemetry_sdk::logs::{SdkLogger, SdkLoggerProvider};
use ostro_engine::sink::{GuestLogRecord, guest_log_metrics as m};
use ostro_partition::clock::Clock;

use crate::config::GuestLogsSection;

/// OTLP instrumentation scope of guest LogRecords.
pub const OTLP_SCOPE: &str = "ostro.guest";
/// A run's limiter state is forgotten after this long without a line (once its ring is
/// empty).
pub const RUN_STATE_IDLE_MS: u64 = 60 * 60 * 1000;
/// Hard bound on tracked runs (limiter + ring state).
pub const MAX_RUN_STATES: usize = 100_000;
/// Fixed per-line overhead counted against the ring byte budget.
pub const RING_LINE_OVERHEAD: u64 = 64;
/// Emits between idle sweeps.
const SWEEP_EVERY: u64 = 4096;

/// Why a line was not accepted (or that it was).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    Accepted,
    NodeRate,
    RunRate,
    RunCap,
}

impl Verdict {
    /// The `reason` label of a drop (`None` for an accepted line).
    pub fn reason(self) -> Option<&'static str> {
        match self {
            Verdict::Accepted => None,
            Verdict::NodeRate => Some(m::REASON_NODE_RATE),
            Verdict::RunRate => Some(m::REASON_RUN_RATE),
            Verdict::RunCap => Some(m::REASON_RUN_CAP),
        }
    }
}

/// Level rank (`trace` 0 .. `error` 4; unknown levels rank as `info`).
pub fn level_rank(level: &str) -> u8 {
    match level {
        "trace" => 0,
        "debug" => 1,
        "warn" => 3,
        "error" => 4,
        _ => 2,
    }
}

fn severity(level: &str) -> (Severity, &'static str) {
    match level {
        "trace" => (Severity::Trace, "TRACE"),
        "debug" => (Severity::Debug, "DEBUG"),
        "warn" => (Severity::Warn, "WARN"),
        "error" => (Severity::Error, "ERROR"),
        _ => (Severity::Info, "INFO"),
    }
}

/// One line kept in a run's ring buffer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RingLine {
    pub ts_ms: u64,
    pub level: &'static str,
    pub message: String,
    pub fields: Vec<(String, String)>,
    pub attempt: u32,
}

impl RingLine {
    fn cost(&self) -> u64 {
        let f: usize = self.fields.iter().map(|(k, v)| k.len() + v.len()).sum();
        (self.message.len() + f) as u64 + RING_LINE_OVERHEAD
    }
}

/// A run's recent lines as served by `GET /v1/runs/{id}/logs`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RunLogs {
    /// Lines, oldest first.
    pub lines: Vec<RingLine>,
    /// Lines of this run dropped by rate limits / caps, plus lines lost to ring
    /// eviction (best effort, since the last restart).
    pub dropped: u64,
    /// The node has no state for this run (unknown, idle long enough to be forgotten, or
    /// the node restarted).
    pub unknown: bool,
    /// The run's ring was evicted (wholly or partly) under the node byte budget.
    pub evicted: bool,
}

/// Per-run accounting (tests, docs).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RunStats {
    pub accepted: u64,
    pub accepted_bytes: u64,
    pub dropped_node_rate: u64,
    pub dropped_run_rate: u64,
    pub dropped_run_cap: u64,
    pub ring_evicted: u64,
}

impl RunStats {
    fn dropped(&self) -> u64 {
        self.dropped_node_rate + self.dropped_run_rate + self.dropped_run_cap + self.ring_evicted
    }
}

/// A token bucket over the injected clock (ms resolution).
#[derive(Clone, Copy, Debug)]
struct Bucket {
    tokens: f64,
    last_ms: u64,
    rate: f64,
    burst: f64,
}

impl Bucket {
    fn new(rate_per_sec: u32, burst: u32, now_ms: u64) -> Self {
        let burst = f64::from(burst.max(1));
        Self {
            tokens: burst,
            last_ms: now_ms,
            rate: f64::from(rate_per_sec),
            burst,
        }
    }

    fn refill(&mut self, now_ms: u64) {
        if now_ms > self.last_ms {
            let dt = (now_ms - self.last_ms) as f64 / 1000.0;
            self.tokens = (self.tokens + dt * self.rate).min(self.burst);
            self.last_ms = now_ms;
        }
    }

    fn ready(&mut self, now_ms: u64) -> bool {
        self.refill(now_ms);
        self.tokens >= 1.0
    }

    fn take(&mut self) {
        self.tokens -= 1.0;
    }
}

#[derive(Debug)]
struct RunState {
    bucket: Bucket,
    stats: RunStats,
    capped: bool,
    ring: VecDeque<RingLine>,
    ring_bytes: u64,
    /// Key in `Inner::ring_lru` while the ring is non-empty.
    ring_tick: Option<u64>,
    /// Key in `Inner::seen_lru`.
    seen_tick: u64,
    seen_ms: u64,
}

#[derive(Debug)]
struct Inner {
    node: Bucket,
    runs: HashMap<String, RunState>,
    /// Runs with a non-empty ring, by last write.
    ring_lru: BTreeMap<u64, String>,
    /// Every tracked run, by last line (accepted or not).
    seen_lru: BTreeMap<u64, String>,
    ring_bytes: u64,
    tick: u64,
    emits: u64,
}

impl Inner {
    fn next_tick(&mut self) -> u64 {
        self.tick += 1;
        self.tick
    }

    /// Drop whole rings, least recently written first, until within `budget`; the run
    /// `current` only loses its oldest lines (it was just written).
    fn enforce_budget(&mut self, budget: u64, current: &str) {
        while self.ring_bytes > budget {
            let Some((&tick, run)) = self.ring_lru.iter().next() else {
                break;
            };
            let run = run.clone();
            let Some(st) = self.runs.get_mut(&run) else {
                self.ring_lru.remove(&tick);
                continue;
            };
            if run == current {
                while self.ring_bytes > budget && st.ring.len() > 1 {
                    if let Some(l) = st.ring.pop_front() {
                        let c = l.cost();
                        st.ring_bytes -= c;
                        self.ring_bytes -= c;
                        st.stats.ring_evicted += 1;
                    }
                }
                break;
            }
            self.ring_lru.remove(&tick);
            st.ring_tick = None;
            st.stats.ring_evicted += st.ring.len() as u64;
            self.ring_bytes -= st.ring_bytes;
            st.ring.clear();
            st.ring_bytes = 0;
        }
    }

    fn forget(&mut self, run: &str) {
        if let Some(st) = self.runs.remove(run) {
            if let Some(t) = st.ring_tick {
                self.ring_lru.remove(&t);
            }
            self.seen_lru.remove(&st.seen_tick);
            self.ring_bytes -= st.ring_bytes;
        }
    }

    /// Forget idle runs with an empty ring, and the least recently seen runs beyond
    /// [`MAX_RUN_STATES`].
    fn sweep(&mut self, now_ms: u64, force: bool) {
        let over = self.runs.len().saturating_sub(MAX_RUN_STATES);
        if !force && over == 0 {
            return;
        }
        let mut victims = Vec::new();
        for run in self.seen_lru.values() {
            if victims.len() < over {
                victims.push(run.clone());
                continue;
            }
            let Some(st) = self.runs.get(run) else {
                continue;
            };
            if now_ms.saturating_sub(st.seen_ms) < RUN_STATE_IDLE_MS {
                // seen_lru is ordered by last line: everything after is newer.
                break;
            }
            if st.ring.is_empty() {
                victims.push(run.clone());
            }
        }
        for v in victims {
            self.forget(&v);
        }
    }
}

/// The node's guest log sink (module docs).
pub struct GuestLogSink {
    cfg: GuestLogsSection,
    clock: Arc<dyn Clock>,
    logger: Option<SdkLogger>,
    inner: Mutex<Inner>,
}

impl std::fmt::Debug for GuestLogSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GuestLogSink")
            .field("cfg", &self.cfg)
            .field("otlp", &self.logger.is_some())
            .finish_non_exhaustive()
    }
}

impl GuestLogSink {
    /// A sink with the `[guest_logs]` limits, the system clock and no OTLP export.
    pub fn new(cfg: &GuestLogsSection) -> Self {
        // server.rs replaces this default through `with_clock` (the injected clock).
        let clock = Arc::new(ostro_partition::clock::SystemClock); // clock-exempt: default only
        Self::build(cfg.clone(), clock, None)
    }

    fn build(cfg: GuestLogsSection, clock: Arc<dyn Clock>, logger: Option<SdkLogger>) -> Self {
        let now = clock.now_ms();
        let node = Bucket::new(cfg.node_rate_per_sec, cfg.node_rate_per_sec, now);
        Self {
            cfg,
            clock,
            logger,
            inner: Mutex::new(Inner {
                node,
                runs: HashMap::new(),
                ring_lru: BTreeMap::new(),
                seen_lru: BTreeMap::new(),
                ring_bytes: 0,
                tick: 0,
                emits: 0,
            }),
        }
    }

    /// Read time (bucket refill, idle sweep, ring timestamps fallback) through `clock`,
    /// the server's one injectable clock.
    pub fn with_clock(self, clock: Arc<dyn Clock>) -> Self {
        Self::build(self.cfg, clock, self.logger)
    }

    /// Export accepted lines as OTLP LogRecords through `provider` (the server's
    /// `TelemetryGuard::logger_provider`; `None` = no OTLP export).
    pub fn with_logger_provider(self, provider: Option<&SdkLoggerProvider>) -> Self {
        let logger = provider.map(|p| p.logger(OTLP_SCOPE));
        Self::build(self.cfg, self.clock, logger)
    }

    /// The `[guest_logs]` limits in force.
    pub fn config(&self) -> &GuestLogsSection {
        &self.cfg
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Accept or drop one live guest line (module docs); returns the verdict.
    pub fn emit(&self, rec: GuestLogRecord) -> Verdict {
        let now = self.clock.now_ms();
        let size = rec.size() as u64;
        let (verdict, cap_hit) = self.admit(&rec, size, now);
        match verdict.reason() {
            Some(reason) => m::dropped(reason),
            None => m::line(rec.level),
        }
        if cap_hit {
            tracing::warn!(
                run_id = %rec.run_id,
                workflow = %rec.workflow,
                lines_cap = self.cfg.run_lines_cap,
                bytes_cap = self.cfg.run_bytes_cap,
                "guest log lifetime cap reached for this run; further lines are dropped"
            );
        }
        if verdict == Verdict::Accepted {
            self.node_log(&rec);
            self.export(&rec, now);
        }
        verdict
    }

    /// Limits + ring under the lock. Returns the verdict and whether this line is the
    /// first one over the run's lifetime cap.
    fn admit(&self, rec: &GuestLogRecord, size: u64, now: u64) -> (Verdict, bool) {
        let cfg = &self.cfg;
        let mut g = self.lock();
        let inner = &mut *g;
        inner.emits += 1;
        if inner.emits.is_multiple_of(SWEEP_EVERY) {
            inner.sweep(now, true);
        }
        let seen_tick = inner.next_tick();
        if !inner.runs.contains_key(&rec.run_id) {
            inner.runs.insert(
                rec.run_id.clone(),
                RunState {
                    bucket: Bucket::new(cfg.rate_per_sec, cfg.burst, now),
                    stats: RunStats::default(),
                    capped: false,
                    ring: VecDeque::new(),
                    ring_bytes: 0,
                    ring_tick: None,
                    seen_tick,
                    seen_ms: now,
                },
            );
            inner.seen_lru.insert(seen_tick, rec.run_id.clone());
            inner.sweep(now, false);
        }
        let Some(st) = inner.runs.get_mut(&rec.run_id) else {
            return (Verdict::RunRate, false);
        };
        inner.seen_lru.remove(&st.seen_tick);
        st.seen_tick = seen_tick;
        st.seen_ms = now;
        inner.seen_lru.insert(seen_tick, rec.run_id.clone());

        let node_ok = inner.node.ready(now);
        let run_ok = st.bucket.ready(now);
        let over_cap = st.stats.accepted >= cfg.run_lines_cap
            || st.stats.accepted_bytes.saturating_add(size) > cfg.run_bytes_cap;
        let mut cap_hit = false;
        let verdict = if !node_ok {
            st.stats.dropped_node_rate += 1;
            Verdict::NodeRate
        } else if !run_ok {
            st.stats.dropped_run_rate += 1;
            Verdict::RunRate
        } else if over_cap {
            st.stats.dropped_run_cap += 1;
            cap_hit = !st.capped;
            st.capped = true;
            Verdict::RunCap
        } else {
            inner.node.take();
            st.bucket.take();
            st.stats.accepted += 1;
            st.stats.accepted_bytes += size;
            Verdict::Accepted
        };
        if verdict != Verdict::Accepted {
            return (verdict, cap_hit);
        }
        // Ring push.
        let line = RingLine {
            ts_ms: if rec.ts_ms == 0 { now } else { rec.ts_ms },
            level: rec.level,
            message: rec.message.clone(),
            fields: rec.fields.clone(),
            attempt: rec.attempt,
        };
        let cost = line.cost();
        st.ring.push_back(line);
        st.ring_bytes += cost;
        inner.ring_bytes += cost;
        let keep = cfg.ring_lines.max(1) as usize;
        while st.ring.len() > keep {
            if let Some(old) = st.ring.pop_front() {
                let c = old.cost();
                st.ring_bytes -= c;
                inner.ring_bytes -= c;
            }
        }
        if let Some(t) = st.ring_tick.take() {
            inner.ring_lru.remove(&t);
        }
        inner.tick += 1;
        let ring_tick = inner.tick;
        st.ring_tick = Some(ring_tick);
        inner.ring_lru.insert(ring_tick, rec.run_id.clone());
        inner.enforce_budget(cfg.ring_budget_bytes, &rec.run_id);
        (Verdict::Accepted, false)
    }

    /// (b) the node log line.
    fn node_log(&self, rec: &GuestLogRecord) {
        let fields = rec.fields_json();
        let run = rec.run_id.as_str();
        let workflow = rec.workflow.as_str();
        let attempt = rec.attempt;
        let message = rec.message.as_str();
        macro_rules! ev {
            ($lvl:expr) => {
                tracing::event!(
                    target: "ostro::guest",
                    $lvl,
                    run_id = %run,
                    workflow = %workflow,
                    attempt,
                    guest.fields = %fields,
                    "{message}"
                )
            };
        }
        match level_rank(rec.level) {
            0 => ev!(tracing::Level::TRACE),
            1 => ev!(tracing::Level::DEBUG),
            3 => ev!(tracing::Level::WARN),
            4 => ev!(tracing::Level::ERROR),
            _ => ev!(tracing::Level::INFO),
        }
    }

    /// (c) the OTLP LogRecord.
    fn export(&self, rec: &GuestLogRecord, now_ms: u64) {
        let Some(logger) = &self.logger else {
            return;
        };
        let mut lr = logger.create_log_record();
        let ts = if rec.ts_ms == 0 { now_ms } else { rec.ts_ms };
        lr.set_timestamp(UNIX_EPOCH + Duration::from_millis(ts));
        lr.set_observed_timestamp(epoch_ms(now_ms));
        let (sev, text) = severity(rec.level);
        lr.set_severity_number(sev);
        lr.set_severity_text(text);
        lr.set_body(AnyValue::from(rec.message.clone()));
        lr.add_attributes(
            rec.fields
                .iter()
                .map(|(k, v)| (k.clone(), AnyValue::from(v.clone()))),
        );
        lr.add_attribute("run.id", rec.run_id.clone());
        lr.add_attribute("workflow", rec.workflow.clone());
        lr.add_attribute("attempt", i64::from(rec.attempt));
        if let (Some(t), Some(s)) = (rec.trace_id, rec.span_id) {
            lr.set_trace_context(
                TraceId::from_bytes(t),
                SpanId::from_bytes(s),
                Some(TraceFlags::SAMPLED),
            );
        }
        logger.emit(lr);
    }

    /// The run's most recent `limit` lines at or above `min_rank`, oldest first.
    pub fn run_logs(&self, run_id: &str, limit: usize, min_rank: u8) -> RunLogs {
        let g = self.lock();
        let Some(st) = g.runs.get(run_id) else {
            return RunLogs {
                unknown: true,
                ..RunLogs::default()
            };
        };
        let mut lines: Vec<RingLine> = st
            .ring
            .iter()
            .rev()
            .filter(|l| level_rank(l.level) >= min_rank)
            .take(limit)
            .cloned()
            .collect();
        lines.reverse();
        RunLogs {
            lines,
            dropped: st.stats.dropped(),
            unknown: false,
            evicted: st.stats.ring_evicted > 0,
        }
    }

    /// The run's accounting since this node started (`None` when not tracked).
    pub fn run_stats(&self, run_id: &str) -> Option<RunStats> {
        self.lock().runs.get(run_id).map(|s| s.stats)
    }

    /// Forget a run's limiter and ring state (e.g. after it was deleted).
    pub fn forget_run(&self, run_id: &str) {
        self.lock().forget(run_id);
    }

    /// Bytes held by all ring buffers.
    pub fn ring_bytes(&self) -> u64 {
        self.lock().ring_bytes
    }

    /// Runs currently tracked.
    pub fn tracked_runs(&self) -> usize {
        self.lock().runs.len()
    }
}

fn epoch_ms(ms: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(ms)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ostro_partition::clock::ManualClock;

    fn rec(run: &str, msg: &str) -> GuestLogRecord {
        GuestLogRecord {
            run_id: run.to_owned(),
            workflow: "wf".to_owned(),
            attempt: 1,
            level: "info",
            message: msg.to_owned(),
            fields: vec![("k".to_owned(), "v".to_owned())],
            ts_ms: 0,
            trace_id: None,
            span_id: None,
        }
    }

    fn sink(cfg: GuestLogsSection) -> (GuestLogSink, ManualClock) {
        let clock = ManualClock::new(1_000_000);
        let s = GuestLogSink::new(&cfg).with_clock(Arc::new(clock.clone()));
        (s, clock)
    }

    fn unlimited() -> GuestLogsSection {
        GuestLogsSection {
            rate_per_sec: 1_000_000,
            burst: 1_000_000,
            node_rate_per_sec: 1_000_000,
            ..GuestLogsSection::default()
        }
    }

    fn count(s: &GuestLogSink, run: &str, n: usize) -> (usize, Vec<Verdict>) {
        let mut ok = 0;
        let mut drops = Vec::new();
        for i in 0..n {
            match s.emit(rec(run, &format!("l{i}"))) {
                Verdict::Accepted => ok += 1,
                v => drops.push(v),
            }
        }
        (ok, drops)
    }

    #[test]
    fn guest_logs_run_bucket_burst_then_refill() {
        let (s, clock) = sink(GuestLogsSection::default());
        let (ok, drops) = count(&s, "r1", 2_000);
        assert_eq!(ok, 500, "burst 500");
        assert!(drops.iter().all(|v| *v == Verdict::RunRate));
        clock.advance(1_000);
        let (ok, _) = count(&s, "r1", 2_000);
        assert_eq!(ok, 50, "50 lines/s refill");
        clock.advance(100);
        let (ok, _) = count(&s, "r1", 100);
        assert_eq!(ok, 5, "partial refill");
        // A second run has its own bucket.
        let (ok, _) = count(&s, "r2", 600);
        assert_eq!(ok, 500);
        let st = s.run_stats("r1").expect("tracked");
        assert_eq!(st.accepted, 555);
        assert_eq!(st.dropped_run_rate, 2_000 + 2_000 + 100 - 555);
    }

    #[test]
    fn guest_logs_node_bucket_checked_first() {
        let cfg = GuestLogsSection {
            node_rate_per_sec: 100,
            ..GuestLogsSection::default()
        };
        let (s, clock) = sink(cfg);
        let (a, _) = count(&s, "a", 80);
        let (b, drops) = count(&s, "b", 80);
        assert_eq!(a + b, 100, "node burst = one second's worth");
        assert!(drops.iter().all(|v| *v == Verdict::NodeRate));
        clock.advance(500);
        let (c, _) = count(&s, "c", 200);
        assert_eq!(c, 50);
    }

    #[test]
    fn guest_logs_lifetime_caps_lines_and_bytes() {
        let cfg = GuestLogsSection {
            run_lines_cap: 10_000,
            ..unlimited()
        };
        let (s, _) = sink(cfg);
        let (ok, drops) = count(&s, "r", 20_000);
        assert_eq!(ok, 10_000);
        assert_eq!(drops.len(), 10_000);
        assert!(drops.iter().all(|v| *v == Verdict::RunCap));
        assert_eq!(s.run_stats("r").map(|x| x.dropped_run_cap), Some(10_000));

        let cfg = GuestLogsSection {
            run_bytes_cap: 1_000,
            ..unlimited()
        };
        let (s, _) = sink(cfg);
        let mut r = rec("big", "");
        r.message = "x".repeat(98); // + "k" + "v" = 100 bytes
        let mut ok = 0;
        for _ in 0..20 {
            if s.emit(r.clone()) == Verdict::Accepted {
                ok += 1;
            }
        }
        assert_eq!(ok, 10, "10 MiB analogue: 1000 bytes / 100");
    }

    #[test]
    fn guest_logs_ring_keeps_last_lines_in_order() {
        let cfg = GuestLogsSection {
            ring_lines: 1_000,
            ..unlimited()
        };
        let (s, _) = sink(cfg);
        count(&s, "r", 1_500);
        let l = s.run_logs("r", 100, 0);
        assert_eq!(l.lines.len(), 100);
        assert_eq!(l.lines[0].message, "l1400");
        assert_eq!(l.lines[99].message, "l1499");
        assert_eq!(s.run_logs("r", 5_000, 0).lines.len(), 1_000);
        assert!(!l.evicted && !l.unknown);
        let u = s.run_logs("nope", 10, 0);
        assert!(u.unknown && u.lines.is_empty());
    }

    #[test]
    fn guest_logs_level_filter() {
        let (s, _) = sink(unlimited());
        for (i, lvl) in ["trace", "debug", "info", "warn", "error"]
            .iter()
            .enumerate()
        {
            let mut r = rec("r", &format!("m{i}"));
            r.level = lvl;
            s.emit(r);
        }
        let got: Vec<&str> = s
            .run_logs("r", 10, level_rank("warn"))
            .lines
            .iter()
            .map(|l| l.level)
            .collect();
        assert_eq!(got, ["warn", "error"]);
    }

    #[test]
    fn guest_logs_ring_lru_evicts_least_recently_written_run() {
        // Each line costs 1 + 2 + overhead = 67 bytes ("lN" message is 2-3 bytes).
        let line = rec("x", "l0");
        let per = (line.size() as u64) + RING_LINE_OVERHEAD;
        let cfg = GuestLogsSection {
            ring_budget_bytes: per * 25,
            ..unlimited()
        };
        let (s, _) = sink(cfg);
        for i in 0..10 {
            s.emit(rec("a", &format!("l{i}")));
        }
        for i in 0..10 {
            s.emit(rec("b", &format!("l{i}")));
        }
        // a written again: b is now least recently written.
        s.emit(rec("a", "l9"));
        for i in 0..5 {
            s.emit(rec("c", &format!("l{i}")));
        }
        // 11 + 10 + 5 = 26 lines > 25: b (whole ring) goes.
        let b = s.run_logs("b", 100, 0);
        assert!(b.lines.is_empty() && b.evicted, "{b:?}");
        assert_eq!(b.dropped, 10);
        assert_eq!(s.run_logs("a", 100, 0).lines.len(), 11);
        assert_eq!(s.run_logs("c", 100, 0).lines.len(), 5);
        assert!(s.ring_bytes() <= per * 25);

        // One run alone over the budget keeps its newest lines.
        let (s, _) = sink(GuestLogsSection {
            ring_budget_bytes: per * 5,
            ..unlimited()
        });
        for i in 0..8 {
            s.emit(rec("solo", &format!("l{i}")));
        }
        let solo = s.run_logs("solo", 100, 0);
        assert_eq!(solo.lines.len(), 5);
        assert_eq!(solo.lines[4].message, "l7");
        assert!(solo.evicted);
    }

    #[test]
    fn guest_logs_idle_runs_are_forgotten() {
        let cfg = GuestLogsSection {
            ring_budget_bytes: 1, // every ring is evicted right away (except the newest)
            ..unlimited()
        };
        let (s, clock) = sink(cfg);
        s.emit(rec("old", "x"));
        s.emit(rec("new", "x"));
        assert_eq!(s.tracked_runs(), 2);
        clock.advance(RUN_STATE_IDLE_MS + 1);
        // The sweep runs on the next new run (and every SWEEP_EVERY emits).
        s.lock().sweep(clock.now_ms(), true);
        assert_eq!(
            s.tracked_runs(),
            1,
            "old (empty ring) forgotten, new keeps its ring"
        );
        assert!(s.run_logs("old", 10, 0).unknown);
        s.forget_run("new");
        assert_eq!(s.tracked_runs(), 0);
        assert_eq!(s.ring_bytes(), 0);
    }

    #[test]
    fn guest_logs_fields_round_trip() {
        let (s, _) = sink(unlimited());
        let mut r = rec("r", "hello");
        r.fields = vec![
            ("order".into(), "42".into()),
            ("quote\"d".into(), "line\nbreak\t\u{1}".into()),
        ];
        s.emit(r.clone());
        let got = &s.run_logs("r", 1, 0).lines[0];
        assert_eq!(got.fields, r.fields);
        let v: serde_json::Value = serde_json::from_str(&r.fields_json()).expect("valid json");
        assert_eq!(v["order"], "42");
        assert_eq!(v["quote\"d"], "line\nbreak\t\u{1}");
        let empty = rec("r", "m");
        assert_eq!(
            GuestLogRecord {
                fields: vec![],
                ..empty
            }
            .fields_json(),
            "{}"
        );
    }

    #[test]
    fn guest_logs_cap_warns_once() {
        let cfg = GuestLogsSection {
            run_lines_cap: 3,
            ..unlimited()
        };
        let (s, _) = sink(cfg);
        let (ok, drops) = count(&s, "r", 6);
        assert_eq!(ok, 3);
        assert_eq!(drops, vec![Verdict::RunCap; 3]);
        // capped is sticky: no second warn (state check; the warn itself is a node log).
        assert!(s.lock().runs["r"].capped);
    }
}
