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

use tokio::task::JoinHandle;

use crate::config::SpillSection;
use crate::p7::P7Ctx;
use ostro_meta::SpillPolicy;

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

/// Spill state shared by placement and demand.
#[derive(Debug, Default)]
pub struct SpillHandle {
    _private: (),
}

impl SpillHandle {
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether `queue` currently spills to workers.
    ///
    /// Phase 7 stub (07-02): never.
    pub fn spilling(&self, queue: &str) -> bool {
        let _ = queue;
        false
    }

    /// Placement found the embedded slots full for `queue` at `now_ms` (injected clock, B6).
    ///
    /// Phase 7 stub (07-02): no-op.
    pub fn note_overflow(&self, queue: &str, now_ms: u64) {
        let _ = (queue, now_ms);
    }
}

/// Start the spill sampler; `None` when nothing runs.
///
/// Phase 7 stub (07-02): `None`.
pub fn start(ctx: &P7Ctx) -> Option<JoinHandle<()>> {
    let _ = ctx;
    None
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
