//! Ordered node startup (STOR-03, STOR-08, STOR-09, STOR-10; research Pattern 3, Pitfall 8).
//!
//! Per partition, in this order (Leader mode):
//! 1. `StateOpened`: open the state store and read `applied_offset` (the durable floor);
//! 2. `LogRecovered`: verified log scan with that floor; a torn tail is cut **before** the
//!    shared WAL exists, so before any append;
//! 3. `Validated`: the state is neither ahead of the log nor below its start;
//! 4. `CaughtUp`: replay `[applied, HW)` with actions discarded;
//! 5. `TermStarted`: `LeaderAnnounced{epoch}` appended and applied (the first record of the
//!    term, `epoch = max(source floor, latest log epoch + 1)`);
//! 6. `BarrierOpened`: the partition's [`ReadyBarrier`](ostro_log::ReadyBarrier) opens (trim
//!    permits become mintable) and the handle becomes routable;
//! 7. `LeaderServicesStarted`: after **every** partition reached step 6.
//!
//! Inspect mode stops after step 4: no append, the barrier stays closed, no services.
//! A failure in any step marks only that partition `Failed` (`PartitionFailed`).

use std::sync::Mutex;

/// One startup step, recorded in order of occurrence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StartupEvent {
    NodeFileChecked,
    StateOpened { p: u16, applied: u64 },
    LogRecovered { p: u16, leo: u64, torn: bool },
    Validated { p: u16 },
    CaughtUp { p: u16, applied: u64 },
    TermStarted { p: u16, epoch: u32, offset: u64 },
    BarrierOpened { p: u16 },
    LeaderServicesStarted { p: u16 },
    PartitionFailed { p: u16, reason: String },
}

impl StartupEvent {
    /// The partition the event belongs to (`None` for node-level events).
    pub fn partition(&self) -> Option<u16> {
        match self {
            StartupEvent::NodeFileChecked => None,
            StartupEvent::StateOpened { p, .. }
            | StartupEvent::LogRecovered { p, .. }
            | StartupEvent::Validated { p }
            | StartupEvent::CaughtUp { p, .. }
            | StartupEvent::TermStarted { p, .. }
            | StartupEvent::BarrierOpened { p }
            | StartupEvent::LeaderServicesStarted { p }
            | StartupEvent::PartitionFailed { p, .. } => Some(*p),
        }
    }
}

/// Append-only startup event log (shared by the per-partition startup tasks).
#[derive(Debug, Default)]
pub(crate) struct StartupLog {
    events: Mutex<Vec<StartupEvent>>,
}

impl StartupLog {
    pub(crate) fn push(&self, e: StartupEvent) {
        tracing::info!(event = ?e, "startup");
        self.events
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(e);
    }

    pub(crate) fn snapshot(&self) -> Vec<StartupEvent> {
        self.events
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }
}
