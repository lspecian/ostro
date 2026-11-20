//! Per-node, per-target rate limiting (FX-09, D-FX7) over governor. 06-02 defined
//! [`QuotaSpec`], which [`crate::connector::Target`] carries; 06-07 adds the limiter.
//!
//! # Design
//!
//! One governor **direct** (GCRA) limiter per target key (the origin for HTTP, the connection
//! name for SQL/object), held in a `Mutex<BTreeMap>` and rebuilt when the quota of that key
//! changes. A target with no quota and no default is unlimited. Limits are per node in Phase 6
//! (D-FX7); cluster-wide buckets come in Phase 7/13.
//!
//! [`RateLimiter::acquire`] polls the limiter and sleeps on the tokio timer until the next cell,
//! under `tokio::time::timeout(max_wait)`. On timeout it fails `Retryable{kind: rate_limited}`
//! and increments `ostro_effect_rate_limited_total{target}`.
//!
//! governor is built with `default-features = false, features = ["std"]` (06-02), so its
//! `jitter` feature (and with it `until_ready_with_jitter`/`Jitter`) is not available, and its
//! clock is the std monotonic clock. The wait loop therefore lives here and adds a small
//! deterministic jitter (up to 5 ms, spread by a per-limiter counter) to avoid thundering herds.

use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use governor::clock::{Clock, DefaultClock};
use governor::{DefaultDirectRateLimiter, Quota};
use ostro_types::journal::Failure;

use crate::connector::Target;
use crate::kinds::RATE_LIMITED;
use crate::outcome::retryable;

/// The counter incremented when a permit was not available within `max_wait`.
pub const RATE_LIMITED_METRIC: &str = "ostro_effect_rate_limited_total";

/// A rate quota: `per_second` sustained, with `burst` cells.
#[derive(Clone, Debug, PartialEq)]
pub struct QuotaSpec {
    /// Sustained rate, in requests per second (at least 1).
    pub per_second: u32,
    /// Burst size (at least 1).
    pub burst: u32,
}

/// A quota string that is not `<n>/s` or `<n>/m` with `n >= 1`.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("invalid rate {0:?}: expected <n>/s or <n>/m with n >= 1")]
pub struct QuotaParseError(pub String);

impl QuotaSpec {
    /// Parses `"100/s"` or `"6000/m"`. A per-minute rate is converted to per second (rounded
    /// up, at least 1). The burst defaults to the per-second rate (at least 1); use
    /// [`QuotaSpec::parse_with_burst`] for an explicit `burst` key.
    pub fn parse(s: &str) -> Result<Self, QuotaParseError> {
        let err = || QuotaParseError(s.to_owned());
        let (n, unit) = s.trim().split_once('/').ok_or_else(err)?;
        let n: u32 = n.trim().parse().map_err(|_| err())?;
        if n == 0 {
            return Err(err());
        }
        let per_second = match unit.trim() {
            "s" => n,
            "m" => n.div_ceil(60),
            _ => return Err(err()),
        };
        Ok(Self {
            per_second,
            burst: per_second.max(1),
        })
    }

    /// Parses `rate` like [`QuotaSpec::parse`] and overrides the burst with `burst` when set
    /// (a zero burst is an error). Connection configs carry `"rate"` and `"burst"` keys.
    pub fn parse_with_burst(rate: &str, burst: Option<u32>) -> Result<Self, QuotaParseError> {
        let mut q = Self::parse(rate)?;
        if let Some(b) = burst {
            if b == 0 {
                return Err(QuotaParseError(format!("{rate} (burst 0)")));
            }
            q.burst = b;
        }
        Ok(q)
    }

    /// The governor quota: one cell every `1/per_second`, `burst` cells of capacity.
    pub fn to_quota(&self) -> Quota {
        let rate = NonZeroU32::new(self.per_second.max(1)).unwrap_or(NonZeroU32::MIN);
        let burst = NonZeroU32::new(self.burst.max(1)).unwrap_or(NonZeroU32::MIN);
        Quota::per_second(rate).allow_burst(burst)
    }
}

/// Per-target rate limiter (shared by every connector on the node).
pub struct RateLimiter {
    default: Option<QuotaSpec>,
    clock: DefaultClock,
    limiters: Mutex<BTreeMap<String, (QuotaSpec, Arc<DefaultDirectRateLimiter>)>>,
    jitter_seq: AtomicU64,
}

impl std::fmt::Debug for RateLimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RateLimiter")
            .field("default", &self.default)
            .field("targets", &self.targets())
            .finish()
    }
}

impl RateLimiter {
    /// A limiter applying `default` to targets without their own quota (`None` = unlimited).
    pub fn new(default: Option<QuotaSpec>) -> Self {
        Self {
            default,
            clock: DefaultClock::default(),
            limiters: Mutex::new(BTreeMap::new()),
            jitter_seq: AtomicU64::new(0),
        }
    }

    /// The default quota.
    pub fn default_quota(&self) -> Option<&QuotaSpec> {
        self.default.as_ref()
    }

    /// Keys of the targets that currently hold a limiter.
    pub fn targets(&self) -> Vec<String> {
        self.lock().keys().cloned().collect()
    }

    fn lock(
        &self,
    ) -> std::sync::MutexGuard<'_, BTreeMap<String, (QuotaSpec, Arc<DefaultDirectRateLimiter>)>>
    {
        self.limiters.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// The limiter of `target`, built or rebuilt for its effective quota; `None` = unlimited.
    fn limiter_for(&self, target: &Target) -> Option<Arc<DefaultDirectRateLimiter>> {
        let Some(quota) = target.quota.as_ref().or(self.default.as_ref()) else {
            // Unlimited now: forget any stale limiter so a later quota starts fresh.
            self.lock().remove(&target.key);
            return None;
        };
        let mut map = self.lock();
        match map.get(&target.key) {
            Some((q, l)) if q == quota => Some(Arc::clone(l)),
            _ => {
                let l = Arc::new(DefaultDirectRateLimiter::direct(quota.to_quota()));
                map.insert(target.key.clone(), (quota.clone(), Arc::clone(&l)));
                Some(l)
            }
        }
    }

    /// Waits for a permit of `target` for at most `max_wait`. Fails
    /// `Retryable{kind: rate_limited}` (and counts `ostro_effect_rate_limited_total{target}`) if
    /// none became available in time.
    pub async fn acquire(&self, target: &Target, max_wait: Duration) -> Result<(), Failure> {
        let Some(limiter) = self.limiter_for(target) else {
            return Ok(());
        };
        if limiter.check().is_ok() {
            return Ok(());
        }
        let wait = async {
            loop {
                match limiter.check() {
                    Ok(()) => return,
                    Err(not_until) => {
                        let base = not_until.wait_time_from(self.clock.now());
                        let jitter = self.jitter_seq.fetch_add(1, Ordering::Relaxed) % 5_000;
                        tokio::time::sleep(base + Duration::from_micros(jitter)).await;
                    }
                }
            }
        };
        match tokio::time::timeout(max_wait, wait).await {
            Ok(()) => Ok(()),
            Err(_) => {
                metrics::counter!(RATE_LIMITED_METRIC, "target" => target.key.clone()).increment(1);
                tracing::debug!(target = %target.key, ?max_wait, "effect rate limited");
                Err(retryable(
                    RATE_LIMITED,
                    format!(
                        "rate limit of target {} not available within {} ms",
                        target.key,
                        max_wait.as_millis()
                    ),
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ratelimit_parses_per_second_and_per_minute() {
        assert_eq!(
            QuotaSpec::parse("100/s"),
            Ok(QuotaSpec {
                per_second: 100,
                burst: 100
            })
        );
        assert_eq!(
            QuotaSpec::parse("6000/m"),
            Ok(QuotaSpec {
                per_second: 100,
                burst: 100
            })
        );
        assert_eq!(
            QuotaSpec::parse(" 10/s "),
            Ok(QuotaSpec {
                per_second: 10,
                burst: 10
            })
        );
        let slow = QuotaSpec::parse("30/m").unwrap();
        assert_eq!((slow.per_second, slow.burst), (1, 1));
        for bad in ["", "100", "0/s", "-1/s", "10/h", "x/s", "10/"] {
            assert!(QuotaSpec::parse(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn ratelimit_explicit_burst() {
        assert_eq!(
            QuotaSpec::parse_with_burst("5/s", Some(20)),
            Ok(QuotaSpec {
                per_second: 5,
                burst: 20
            })
        );
        assert_eq!(QuotaSpec::parse_with_burst("5/s", None).unwrap().burst, 5);
        assert!(QuotaSpec::parse_with_burst("5/s", Some(0)).is_err());
        let q = QuotaSpec::parse_with_burst("5/s", Some(7))
            .unwrap()
            .to_quota();
        assert_eq!(q.burst_size().get(), 7);
        assert_eq!(q.replenish_interval(), Duration::from_millis(200));
    }

    #[tokio::test]
    async fn ratelimit_rebuilds_on_quota_change_and_forgets_unlimited() {
        let rl = RateLimiter::new(None);
        let mut t = Target {
            key: "https://a.example".into(),
            quota: Some(QuotaSpec {
                per_second: 1,
                burst: 1,
            }),
        };
        rl.acquire(&t, Duration::ZERO).await.unwrap();
        assert!(rl.acquire(&t, Duration::from_millis(5)).await.is_err());
        t.quota = Some(QuotaSpec {
            per_second: 1,
            burst: 2,
        });
        rl.acquire(&t, Duration::ZERO).await.unwrap();
        t.quota = None;
        for _ in 0..100 {
            rl.acquire(&t, Duration::ZERO).await.unwrap();
        }
        assert!(rl.targets().is_empty());
    }
}
