//! Cron driver of a schedule's owner partition (P8-D10). Phase 8: implemented in 08-14.
//!
//! This file holds the **contract** (plan 08-01):
//! - [`ScheduleSpec`]: one schedule as the driver sees it (copied from the meta `Schedule`
//!   record by the server's [`ScheduleSource`]).
//! - [`ScheduleSource`]: a watch channel of the full schedule set; the driver filters it by
//!   `ostro_types::routing::schedule_owner(name, P) == partition`.
//! - [`CronConfig`]: driver knobs (`[triggers]`, P8-D22).
//!
//! The driver proposes `Cmd::ScheduleFire`; overlap is evaluated in apply (08-07), catch-up by
//! the driver (at most [`CronConfig::catchup_cap`] fires). Every time read goes through the
//! injected `crate::clock::Clock`.

use std::sync::Arc;

use tokio::sync::watch;

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

/// The schedule set, as a watch channel (meta-backed, implemented in ostro-server by 08-14).
pub trait ScheduleSource: Send + Sync + 'static {
    /// A receiver of the current full schedule set; it changes whenever a schedule does.
    fn subscribe(&self) -> watch::Receiver<Arc<Vec<ScheduleSpec>>>;
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
