//! Cron evaluation (P8-D9, plan 08-05): next occurrence, latest occurrence, bounded range
//! enumeration and catch-up planning, all timezone-aware.
//!
//! # Engine
//! croner 4.0.3 (jiff backend, never chrono) evaluates the expression. croner speaks jiff 0.2,
//! so a private adapter turns `i64` unix milliseconds into `jiff02::Zoned` values in a zone
//! parsed from the same TZif bytes as the jiff 1.0 zone ([`crate::tz`]). **No jiff 0.2 or
//! croner type appears in this module's public API**: times are `i64` unix ms (or
//! `jiff::Timestamp` 1.0), zones are names.
//!
//! # Syntax
//! croner's: 5 fields (`min hour dom month dow`, seconds = 0), 6 fields (a leading seconds
//! field) or 7 (a trailing year). `L` (last day / last weekday of month), `#` (nth weekday),
//! `W` (nearest weekday), month and weekday names, `@daily`-style nicknames. When both
//! day-of-month and day-of-week are restricted, either may match (vixie-cron).
//!
//! # DST (croner 4.0.3 rules, pinned by `tests/cron.rs`)
//! - **Gap** (spring forward): a fixed-time job (single second, minute and hour) whose wall
//!   time does not exist that day fires **once, at the first valid instant after the gap**
//!   (`0 30 2 * * *` in New York on 2026-03-08 fires at 03:00 EDT). Interval/wildcard jobs skip
//!   the missing occurrences. Never an extra fire.
//! - **Overlap** (fall back): a fixed-time job fires **once, at the first occurrence** (EDT).
//!   Interval/wildcard jobs fire in both passes (`0 */30 * * * *` fires at 01:00 and 01:30
//!   EDT, then 01:00 and 01:30 EST).
//!
//! # Fire set and bounds
//! A schedule's fire set is the sequence produced by iterating [`next_after`]. Fires are whole
//! seconds. [`CronSpec`] bounds (`start_at_ms`, `end_at_ms`, inclusive) clip the set.
//!
//! # Catch-up ([`plan_catchup`], B5/W6)
//! The floor is always `max(watermark, created_at_ms)`, so a re-created schedule (or a fresh
//! one) never backfills. Work is bounded no matter how far back the floor is: Skip/One compute
//! the latest fire `<= now` directly, All walks back from `now` at most `cap` fires. The
//! driver's steady state then restarts at `next_after(max(floor, now))` (08-14).
//!
//! Pure: no clock reads, no I/O after [`CronSpec::parse`] (which may read
//! `/usr/share/zoneinfo`).

use std::fmt;

use crate::tz::{DualZone, TzError};

/// Fires counted exactly by the `skipped` estimates of [`plan_catchup`] before extrapolating.
pub const SKIP_COUNT_BUDGET: u32 = 256;

/// A cron validation error.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum CronError {
    /// The expression does not parse, or it can never fire.
    #[error("invalid cron expression {expr:?}: {reason}")]
    Expr {
        /// The expression as given.
        expr: String,
        /// Why it was rejected.
        reason: String,
    },
    /// The time-zone name is malformed or unknown.
    #[error("invalid time zone {name:?}: {reason}")]
    Timezone {
        /// The zone name as given.
        name: String,
        /// Why it was rejected.
        reason: String,
    },
    /// `start_at_ms` / `end_at_ms` are inconsistent (end before start) or out of range.
    #[error("invalid schedule bounds: {0}")]
    Bounds(String),
    /// An overlap or catch-up policy byte outside its range.
    #[error("invalid {kind} policy value {value}")]
    Policy {
        /// `"overlap"` or `"catchup"`.
        kind: &'static str,
        /// The rejected value.
        value: u8,
    },
}

impl From<TzError> for CronError {
    fn from(e: TzError) -> Self {
        let name = match &e {
            TzError::BadName(n) | TzError::Unknown(n) => n.clone(),
            TzError::Invalid { name, .. } => name.clone(),
        };
        CronError::Timezone {
            name,
            reason: e.to_string(),
        }
    }
}

/// Overlap policy, with the meta `Schedule.overlap` numbering (05-07 `OVERLAP_*`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum OverlapPolicy {
    /// `OVERLAP_SKIP` (0): skip the fire while the previous run is live.
    Skip = 0,
    /// `OVERLAP_BUFFER_ONE` (1): buffer one fire until the previous run ends.
    BufferOne = 1,
    /// `OVERLAP_ALLOW_ALL` (2): start every fire.
    AllowAll = 2,
    /// `OVERLAP_CANCEL_OTHER` (3): cancel the previous run, then start.
    CancelOther = 3,
    /// `OVERLAP_TERMINATE_OTHER` (4): terminate the previous run, then start.
    TerminateOther = 4,
}

impl OverlapPolicy {
    /// The meta byte.
    pub const fn as_u8(self) -> u8 {
        self as u8
    }
}

impl TryFrom<u8> for OverlapPolicy {
    type Error = CronError;
    fn try_from(v: u8) -> Result<Self, CronError> {
        Ok(match v {
            0 => Self::Skip,
            1 => Self::BufferOne,
            2 => Self::AllowAll,
            3 => Self::CancelOther,
            4 => Self::TerminateOther,
            value => {
                return Err(CronError::Policy {
                    kind: "overlap",
                    value,
                });
            }
        })
    }
}

/// Catch-up policy, with the meta `Schedule.catchup` numbering (05-07 `CATCHUP_*`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum CatchupPolicy {
    /// `CATCHUP_SKIP` (0): fire none of the missed instants; advance the watermark.
    Skip = 0,
    /// `CATCHUP_ONE` (1): fire the latest missed instant only.
    One = 1,
    /// `CATCHUP_ALL` (2): fire the newest missed instants within the window, up to the cap.
    All = 2,
}

impl CatchupPolicy {
    /// The meta byte.
    pub const fn as_u8(self) -> u8 {
        self as u8
    }
}

impl TryFrom<u8> for CatchupPolicy {
    type Error = CronError;
    fn try_from(v: u8) -> Result<Self, CronError> {
        Ok(match v {
            0 => Self::Skip,
            1 => Self::One,
            2 => Self::All,
            value => {
                return Err(CronError::Policy {
                    kind: "catchup",
                    value,
                });
            }
        })
    }
}

/// A parsed schedule: expression, zone (both jiff majors, same TZif bytes) and bounds.
/// Parse once, evaluate many times.
#[derive(Clone)]
pub struct CronSpec {
    expr: String,
    tz_name: String,
    cron: croner::Cron,
    zone: DualZone,
    start_at_ms: Option<i64>,
    end_at_ms: Option<i64>,
}

impl fmt::Debug for CronSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CronSpec")
            .field("expr", &self.expr)
            .field("timezone", &self.tz_name)
            .field("start_at_ms", &self.start_at_ms)
            .field("end_at_ms", &self.end_at_ms)
            .finish()
    }
}

impl CronSpec {
    /// Parses `expr` in zone `timezone` (`""` = UTC), unbounded.
    ///
    /// Errors: [`CronError::Expr`] when croner rejects the expression or it never fires
    /// (for example `0 0 0 30 2 *`); [`CronError::Timezone`] for a malformed or unknown zone.
    pub fn parse(expr: &str, timezone: &str) -> Result<Self, CronError> {
        let cron = croner::parser::CronParser::builder()
            .seconds(croner::parser::Seconds::Optional)
            .build()
            .parse(expr)
            .map_err(|e| CronError::Expr {
                expr: expr.to_owned(),
                reason: e.to_string(),
            })?;
        let zone = DualZone::load(timezone)?;
        let spec = Self {
            expr: expr.to_owned(),
            tz_name: timezone.to_owned(),
            cron,
            zone,
            start_at_ms: None,
            end_at_ms: None,
        };
        // An expression that parses but has no instant at all (Feb 30) is a validation error.
        // Probe from the unix epoch; croner gives up at its year limit, so this is bounded.
        if spec.raw_next_after(0).is_none() && spec.raw_prev_at_or_before(0).is_none() {
            return Err(CronError::Expr {
                expr: expr.to_owned(),
                reason: "the expression never fires".to_owned(),
            });
        }
        Ok(spec)
    }

    /// Parses with bounds (unix ms, inclusive). [`CronError::Bounds`] when
    /// `end_at_ms < start_at_ms` or a bound exceeds `i64::MAX`.
    pub fn parse_bounded(
        expr: &str,
        timezone: &str,
        start_at_ms: Option<u64>,
        end_at_ms: Option<u64>,
    ) -> Result<Self, CronError> {
        let bounds = check_bounds(start_at_ms, end_at_ms)?;
        let mut spec = Self::parse(expr, timezone)?;
        (spec.start_at_ms, spec.end_at_ms) = bounds;
        Ok(spec)
    }

    /// The expression as given.
    pub fn expr(&self) -> &str {
        &self.expr
    }

    /// The zone name as given (`""` = UTC).
    pub fn timezone(&self) -> &str {
        &self.tz_name
    }

    /// The zone, as a jiff 1.0 `TimeZone` (built from the same TZif bytes croner uses).
    pub fn time_zone(&self) -> &jiff::tz::TimeZone {
        &self.zone.v1
    }

    /// Where the zone's TZif bytes came from (`None` for UTC).
    pub fn tz_source(&self) -> Option<crate::tz::TzSource> {
        self.zone.source
    }

    /// The inclusive start bound (unix ms).
    pub fn start_at_ms(&self) -> Option<i64> {
        self.start_at_ms
    }

    /// The inclusive end bound (unix ms).
    pub fn end_at_ms(&self) -> Option<i64> {
        self.end_at_ms
    }

    /// The next fire strictly after `after_ms` (unix ms), within the bounds. See [`next_after`].
    pub fn next_after(&self, after_ms: i64) -> Option<i64> {
        let after = match self.start_at_ms {
            // fire >= start  <=>  fire > start - 1 (integer ms)
            Some(start) => after_ms.max(start.saturating_sub(1)),
            None => after_ms,
        };
        let next = self.raw_next_after(after)?;
        match self.end_at_ms {
            Some(end) if next > end => None,
            _ => Some(next),
        }
    }

    /// [`Self::next_after`] on jiff 1.0 timestamps.
    pub fn next_after_ts(&self, after: jiff::Timestamp) -> Option<jiff::Timestamp> {
        self.next_after(after.as_millisecond())
            .and_then(|ms| jiff::Timestamp::from_millisecond(ms).ok())
    }

    /// The latest fire `<= t_ms`, within the bounds. See [`latest_at_or_before`].
    pub fn latest_at_or_before(&self, t_ms: i64) -> Option<i64> {
        let t = match self.end_at_ms {
            Some(end) => t_ms.min(end),
            None => t_ms,
        };
        if let Some(start) = self.start_at_ms
            && t < start
        {
            return None;
        }
        let latest = self.raw_latest_at_or_before(t)?;
        match self.start_at_ms {
            Some(start) if latest < start => None,
            _ => Some(latest),
        }
    }

    /// The fires in `(from_exclusive, to_inclusive]`, increasing, keeping the **last** `cap`.
    /// See [`fires_in`].
    pub fn fires_in(&self, from_exclusive: i64, to_inclusive: i64, cap: usize) -> (Vec<i64>, bool) {
        let mut out = Vec::new();
        let mut t = to_inclusive;
        let truncated = loop {
            let Some(p) = self.latest_at_or_before(t).filter(|&p| p > from_exclusive) else {
                break false;
            };
            if out.len() == cap {
                break true;
            }
            out.push(p);
            t = p - 1;
        };
        out.reverse();
        (out, truncated)
    }

    /// The number of fires in `(from_exclusive, to_inclusive]`: exact up to
    /// [`SKIP_COUNT_BUDGET`], beyond that extrapolated from the density of the counted prefix
    /// (saturating at `u32::MAX`).
    pub fn count_fires(&self, from_exclusive: i64, to_inclusive: i64) -> u32 {
        let mut n: u32 = 0;
        let mut t = from_exclusive;
        while let Some(next) = self.next_after(t).filter(|&f| f <= to_inclusive) {
            n += 1;
            t = next;
            if n == SKIP_COUNT_BUDGET {
                // Is there anything left at all?
                if self.next_after(t).is_none_or(|f| f > to_inclusive) {
                    return n;
                }
                let counted = (t - from_exclusive).max(1) as f64;
                let total = (to_inclusive - from_exclusive) as f64;
                let estimate = f64::from(n) * total / counted;
                return if estimate >= f64::from(u32::MAX) {
                    u32::MAX
                } else {
                    (estimate as u32).max(n + 1)
                };
            }
        }
        n
    }

    // ---- adapter (jiff 0.2 + croner, private) ----

    /// `i64` unix ms (floored to the second) as a croner-ready jiff 0.2 `Zoned`.
    fn zoned02(&self, ms: i64) -> Option<jiff02::Zoned> {
        let secs = ms.div_euclid(1000);
        jiff02::Timestamp::from_second(secs)
            .ok()
            .map(|ts| ts.to_zoned(self.zone.v02.clone()))
    }

    /// The next fire strictly after `after_ms`, ignoring bounds. Fires are whole seconds, so
    /// starting croner's exclusive search at `floor(after_ms)` is exact.
    fn raw_next_after(&self, after_ms: i64) -> Option<i64> {
        let z = self.zoned02(after_ms)?;
        let next = self.cron.find_next_occurrence(&z, false).ok()?;
        Some(next.timestamp().as_millisecond())
    }

    /// croner's previous occurrence `<= t_ms` (inclusive at `floor(t_ms)`), ignoring bounds.
    fn raw_prev_at_or_before(&self, t_ms: i64) -> Option<i64> {
        let z = self.zoned02(t_ms)?;
        let prev = self.cron.find_previous_occurrence(&z, true).ok()?;
        Some(prev.timestamp().as_millisecond())
    }

    /// The latest member of the forward fire set `<= t_ms`, ignoring bounds.
    ///
    /// croner's backward search proposes a candidate `p`; it is accepted only if the forward
    /// search agrees (`p` is a forward fire and the next forward fire is past `t_ms`), so the
    /// result is always consistent with [`Self::next_after`] iteration even around DST
    /// transitions. Otherwise an expanding backward window (1 min, doubling) is scanned forward.
    /// Both paths do bounded work regardless of how far back the previous fire is.
    fn raw_latest_at_or_before(&self, t_ms: i64) -> Option<i64> {
        if let Some(p) = self.raw_prev_at_or_before(t_ms)
            && self.raw_next_after(p - 1) == Some(p)
            && self.raw_next_after(p).is_none_or(|n| n > t_ms)
        {
            return Some(p);
        }
        self.latest_by_window(t_ms)
    }

    /// Fallback for [`Self::raw_latest_at_or_before`]: windows of 1 min, 2 min, 4 min, ...
    /// before `t_ms`, each scanned forward with `next_after`; stops at croner's lower year
    /// limit.
    fn latest_by_window(&self, t_ms: i64) -> Option<i64> {
        // croner searches no earlier than year 1; jiff 0.2 no earlier than -9999.
        const LOWEST_MS: i64 = -62_135_596_800_000; // 0001-01-01T00:00:00Z
        let mut width: i64 = 60_000;
        loop {
            let lo = t_ms.saturating_sub(width).max(LOWEST_MS);
            if let Some(first) = self.raw_next_after(lo).filter(|&f| f <= t_ms) {
                let mut last = first;
                while let Some(n) = self.raw_next_after(last).filter(|&f| f <= t_ms) {
                    last = n;
                }
                return Some(last);
            }
            if lo == LOWEST_MS {
                return None;
            }
            width = width.saturating_mul(2);
        }
    }
}

fn check_bounds(
    start_at_ms: Option<u64>,
    end_at_ms: Option<u64>,
) -> Result<(Option<i64>, Option<i64>), CronError> {
    let conv = |what: &str, v: Option<u64>| {
        v.map(|v| {
            i64::try_from(v).map_err(|_| CronError::Bounds(format!("{what} {v} is out of range")))
        })
        .transpose()
    };
    let start = conv("start_at_ms", start_at_ms)?;
    let end = conv("end_at_ms", end_at_ms)?;
    if let (Some(s), Some(e)) = (start, end)
        && e < s
    {
        return Err(CronError::Bounds(format!(
            "end_at_ms {e} is before start_at_ms {s}"
        )));
    }
    Ok((start, end))
}

/// Parses `expr` in `timezone` and returns its next fire strictly after `after_ms` (unix ms,
/// UTC). `Ok(None)` when no later fire exists (croner's year limit).
pub fn next_after_expr(
    expr: &str,
    timezone: &str,
    after_ms: i64,
) -> Result<Option<i64>, CronError> {
    Ok(CronSpec::parse(expr, timezone)?.next_after(after_ms))
}

/// The next fire of `spec` strictly after `after_ms` (unix ms, UTC), honouring the zone's DST
/// rules, the seconds field, `L`/`#`/`W` and the bounds. `None` when there is none.
pub fn next_after(spec: &CronSpec, after_ms: i64) -> Option<i64> {
    spec.next_after(after_ms)
}

/// The latest fire of `spec` that is `<= t_ms`, within the bounds. Bounded work regardless of
/// how long ago it was.
pub fn latest_at_or_before(spec: &CronSpec, t_ms: i64) -> Option<i64> {
    spec.latest_at_or_before(t_ms)
}

/// The fires of `spec` in `(from_exclusive, to_inclusive]`, increasing, keeping the **last**
/// `cap` of them: truncation drops the oldest, never the newest. The flag is `true` when older
/// fires were dropped. Work is `O(cap)`, independent of the range length.
pub fn fires_in(
    spec: &CronSpec,
    from_exclusive: i64,
    to_inclusive: i64,
    cap: usize,
) -> (Vec<i64>, bool) {
    spec.fires_in(from_exclusive, to_inclusive, cap)
}

/// What catch-up does for one schedule (see [`plan_catchup`]).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CatchupPlan {
    /// Fire times to submit as `ScheduleFire`, increasing.
    pub fire: Vec<i64>,
    /// Advance the watermark to this fire time without starting a run (policy Skip).
    pub advance_to: Option<i64>,
    /// Missed fires that will not run. Exact up to [`SKIP_COUNT_BUDGET`], extrapolated beyond
    /// (saturating at `u32::MAX`); for metrics and logs only.
    pub skipped: u32,
}

/// Plans catch-up for a schedule whose durable watermark is `watermark` (the last fire time
/// handled, 0 when none) at time `now`.
///
/// The floor is `max(watermark, created_at_ms)` (W6): nothing at or before it is ever planned,
/// so a re-created schedule with an orphaned watermark, or a brand-new one, never backfills.
/// - [`CatchupPolicy::Skip`]: `latest = latest_at_or_before(now)`; if it is past the floor,
///   `advance_to = latest`, no fire, `skipped` = the missed fires.
/// - [`CatchupPolicy::One`]: if `latest` is past the floor, `fire = [latest]` (the fire itself
///   moves the watermark), `skipped` = the missed fires before it.
/// - [`CatchupPolicy::All`]: the fires in `(max(floor, now - window_ms), now]`, keeping the
///   newest `cap`; `skipped` = every missed fire older than the first planned one (the first
///   fire moves the watermark past them), or every missed fire when none is planned. `window_ms == 0` means no window (the Phase 8
///   default): only the floor and the cap limit it.
///
/// The driver's steady state then restarts at `next_after(max(floor, now))`.
pub fn plan_catchup(
    spec: &CronSpec,
    watermark: i64,
    created_at_ms: i64,
    now: i64,
    policy: CatchupPolicy,
    window_ms: u64,
    cap: usize,
) -> CatchupPlan {
    let floor = watermark.max(created_at_ms);
    if now <= floor {
        return CatchupPlan::default();
    }
    match policy {
        CatchupPolicy::Skip | CatchupPolicy::One => {
            let Some(latest) = spec.latest_at_or_before(now).filter(|&l| l > floor) else {
                return CatchupPlan::default();
            };
            if policy == CatchupPolicy::Skip {
                CatchupPlan {
                    fire: Vec::new(),
                    advance_to: Some(latest),
                    skipped: spec.count_fires(floor, latest),
                }
            } else {
                CatchupPlan {
                    fire: vec![latest],
                    advance_to: None,
                    skipped: spec.count_fires(floor, latest - 1),
                }
            }
        }
        CatchupPolicy::All => {
            let lo = if window_ms == 0 {
                floor
            } else {
                let window = i64::try_from(window_ms).unwrap_or(i64::MAX);
                floor.max(now.saturating_sub(window))
            };
            let (fire, _) = spec.fires_in(lo, now, cap);
            let skipped = match fire.first() {
                Some(&first) => spec.count_fires(floor, first - 1),
                None => spec.count_fires(floor, now),
            };
            CatchupPlan {
                fire,
                advance_to: None,
                skipped,
            }
        }
    }
}
