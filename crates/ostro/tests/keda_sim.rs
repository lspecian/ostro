//! SC5 local proof (plan 07-20, WORK-05, D-W22): a KEDA simulator drives real
//! `ostro worker` processes from the demand endpoint, 0 → N → 0.
//!
//! No Docker, kind or k3d exist here and the reachable microk8s cluster is off-limits, so
//! this file implements KEDA's metrics-api scaler math itself ([`Scaler`]) and acts on
//! the result by spawning or SIGTERMing `ostro worker` processes (the simulator never
//! shells out to kubectl or helm). A real ScaledObject run
//! (`deploy/keda/scaledobject-metrics-api.yaml`) is human_needed (07-22).
//!
//! # KEDA metrics-api semantics reproduced
//!
//! - **Activation** (0 → 1): the scaler is active only when the metric is strictly
//!   greater than `activationTargetValue` (`demand > activation_target`).
//! - **Scaling** (1 → N): `desired = ceil(demand / targetValue)`, clamped to
//!   `[max(minReplicaCount, 1), maxReplicaCount]` while active.
//! - **Cool-down** (N → 0): only after the scaler has been inactive for `cooldownPeriod`
//!   since the last active poll. Until then the replica count stays where it is.
//! - **N → M (M > 0)** follows `desired` immediately. The HPA's own scale-down
//!   stabilisation window (default 300 s) is out of scope: following `desired` at once is
//!   the harsher case for exactly-once completion, because every scale-down drains a
//!   worker with runs on it.
#![cfg(unix)]
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::time::{Duration, Instant};

/// KEDA's metrics-api scaler plus the cool-down of the ScaledObject controller.
#[derive(Clone, Debug)]
pub struct Scaler {
    /// `targetValue`: metric value per replica.
    pub target: f64,
    /// `activationTargetValue`: active only when the metric is strictly greater.
    pub activation_target: f64,
    /// `minReplicaCount`.
    pub min: u32,
    /// `maxReplicaCount`.
    pub max: u32,
    /// `cooldownPeriod`: inactive time before N → 0.
    pub cooldown: Duration,
    /// The last poll at which the scaler was active.
    last_active: Option<Instant>,
}

impl Scaler {
    pub fn new(
        target: f64,
        activation_target: f64,
        min: u32,
        max: u32,
        cooldown: Duration,
    ) -> Self {
        assert!(target > 0.0, "targetValue must be > 0");
        assert!(max >= 1 && min <= max, "replica bounds");
        Self {
            target,
            activation_target,
            min,
            max,
            cooldown,
            last_active: None,
        }
    }

    /// The replica count KEDA would ask for at `now`, given the polled `demand` and the
    /// `current` replica count.
    pub fn desired(&mut self, demand: f64, current: u32, now: Instant) -> u32 {
        if demand > self.activation_target {
            self.last_active = Some(now);
            let raw = (demand / self.target).ceil();
            // `raw` is finite and ≥ 0 here; saturate before the integer clamp.
            let raw = if raw >= f64::from(u32::MAX) {
                u32::MAX
            } else {
                raw as u32
            };
            return raw.clamp(self.min.max(1), self.max);
        }
        // Inactive: only minReplicaCount (0 here) after the cool-down; until then KEDA
        // leaves the scale target alone.
        if current > self.min {
            let cooled = match self.last_active {
                Some(t) => now.saturating_duration_since(t) >= self.cooldown,
                // Never active since the simulator started: KEDA scales to min at once.
                None => true,
            };
            if cooled { self.min } else { current }
        } else {
            self.min
        }
    }
}

#[cfg(test)]
mod scaler_tests {
    use super::*;

    fn s() -> Scaler {
        Scaler::new(8.0, 0.0, 0, 4, Duration::from_secs(5))
    }

    #[test]
    fn scaler_activation_is_strictly_greater() {
        let t = Instant::now();
        let mut sc = s();
        assert_eq!(
            sc.desired(0.0, 0, t),
            0,
            "demand 0, activation 0 stays at 0"
        );
        let mut sc = Scaler::new(8.0, 3.0, 0, 4, Duration::from_secs(5));
        assert_eq!(sc.desired(3.0, 0, t), 0, "equal to activation is inactive");
        assert_eq!(sc.desired(3.5, 0, t), 1, "above activation activates");
    }

    #[test]
    fn scaler_ceil_math() {
        let t = Instant::now();
        let mut sc = s();
        assert_eq!(sc.desired(9.0, 0, t), 2, "ceil(9 / 8) = 2");
        assert_eq!(sc.desired(8.0, 2, t), 1, "ceil(8 / 8) = 1");
        assert_eq!(sc.desired(0.5, 1, t), 1, "fractional demand rounds up");
        assert_eq!(sc.desired(16.0, 1, t), 2);
        assert_eq!(sc.desired(17.0, 2, t), 3);
    }

    #[test]
    fn scaler_clamps_to_max_and_min() {
        let t = Instant::now();
        let mut sc = s();
        assert_eq!(sc.desired(1_000.0, 1, t), 4, "clamped to max");
        assert_eq!(sc.desired(f64::MAX, 1, t), 4, "huge demand saturates");
        let mut sc = Scaler::new(8.0, 0.0, 2, 4, Duration::from_secs(5));
        assert_eq!(sc.desired(1.0, 0, t), 2, "clamped up to min while active");
    }

    #[test]
    fn scaler_scales_to_zero_only_after_cooldown() {
        let t = Instant::now();
        let mut sc = s();
        assert_eq!(sc.desired(20.0, 0, t), 3);
        // Inactive polls inside the cool-down keep the current count.
        for ms in [500u64, 1_000, 4_999] {
            assert_eq!(
                sc.desired(0.0, 3, t + Duration::from_millis(ms)),
                3,
                "at +{ms} ms"
            );
        }
        assert_eq!(
            sc.desired(0.0, 3, t + Duration::from_secs(5)),
            0,
            "cool-down passed"
        );
        // A re-activation restarts the window.
        assert_eq!(sc.desired(1.0, 0, t + Duration::from_secs(6)), 1);
        assert_eq!(sc.desired(0.0, 1, t + Duration::from_secs(10)), 1);
        assert_eq!(sc.desired(0.0, 1, t + Duration::from_secs(11)), 0);
    }

    #[test]
    fn scaler_scale_up_from_zero_is_immediate() {
        let t = Instant::now();
        let mut sc = s();
        assert_eq!(sc.desired(30.0, 0, t), 4, "0 → max in one poll");
        let mut sc = Scaler::new(2.0, 0.0, 0, 4, Duration::from_secs(5));
        assert_eq!(sc.desired(1.0, 0, t), 1);
    }

    #[test]
    fn scaler_scale_down_between_nonzero_counts_is_immediate() {
        let t = Instant::now();
        let mut sc = Scaler::new(2.0, 0.0, 0, 4, Duration::from_secs(5));
        assert_eq!(sc.desired(30.0, 0, t), 4);
        assert_eq!(sc.desired(3.0, 4, t + Duration::from_millis(500)), 2);
        assert_eq!(sc.desired(1.0, 2, t + Duration::from_secs(1)), 1);
    }
}
