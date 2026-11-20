//! Per-target rate limits (FX-09, 06-07): quota + burst within a window, target isolation,
//! bounded wait -> `rate_limited`, and the `ostro_effect_rate_limited_total{target}` counter.
//!
//! Real clock: governor uses the std monotonic clock and ostro-bridge's dev tokio has no
//! `test-util` (so no `tokio::time::pause`); the bounds below are generous instead.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};
use ostro_bridge::connector::Target;
use ostro_bridge::ratelimit::{QuotaSpec, RateLimiter};
use ostro_types::journal::Failure;

// ---- minimal global metrics recorder (counters only) ----

static COUNTERS: LazyLock<Mutex<BTreeMap<String, Arc<AtomicU64>>>> =
    LazyLock::new(|| Mutex::new(BTreeMap::new()));

fn key_string(key: &Key) -> String {
    let mut labels: Vec<String> = key
        .labels()
        .map(|l| format!("{}={}", l.key(), l.value()))
        .collect();
    labels.sort();
    format!("{}{{{}}}", key.name(), labels.join(","))
}

struct TestRecorder;

impl Recorder for TestRecorder {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> Counter {
        let c = Arc::clone(COUNTERS.lock().unwrap().entry(key_string(key)).or_default());
        Counter::from_arc(c)
    }
    fn register_gauge(&self, _: &Key, _: &Metadata<'_>) -> Gauge {
        Gauge::noop()
    }
    fn register_histogram(&self, _: &Key, _: &Metadata<'_>) -> Histogram {
        Histogram::noop()
    }
}

fn install_recorder() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| metrics::set_global_recorder(TestRecorder).expect("recorder"));
}

fn counter(name_and_labels: &str) -> u64 {
    COUNTERS
        .lock()
        .unwrap()
        .get(name_and_labels)
        .map_or(0, |c| c.load(Ordering::Relaxed))
}

fn target(key: &str, per_second: u32, burst: u32) -> Target {
    Target {
        key: key.to_owned(),
        quota: Some(QuotaSpec { per_second, burst }),
    }
}

fn assert_rate_limited(r: Result<(), Failure>) {
    match r {
        Err(Failure::Retryable(e)) => assert_eq!(e.kind, "rate_limited"),
        other => panic!("expected rate_limited, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quota_plus_burst_bounds_a_window_and_targets_are_isolated() {
    install_recorder();
    let rl = Arc::new(RateLimiter::new(None));
    let a = target("https://a.quota.example", 5, 5);
    let start = Instant::now();

    let mut set = tokio::task::JoinSet::new();
    for _ in 0..20 {
        let (rl, a) = (Arc::clone(&rl), a.clone());
        set.spawn(async move {
            let r = rl.acquire(&a, Duration::from_millis(1_500)).await;
            (r.is_ok(), start.elapsed())
        });
    }

    // While target A is saturated, target B (same quota) is unaffected: its burst is
    // available immediately.
    tokio::time::sleep(Duration::from_millis(20)).await;
    let b = target("https://b.quota.example", 5, 5);
    let b_start = Instant::now();
    for _ in 0..5 {
        rl.acquire(&b, Duration::ZERO)
            .await
            .expect("target B unaffected");
    }
    assert!(b_start.elapsed() < Duration::from_millis(200));

    let mut ok_first_second = 0;
    let mut ok_total = 0;
    let mut limited = 0;
    while let Some(j) = set.join_next().await {
        let (ok, at) = j.unwrap();
        if ok {
            ok_total += 1;
            if at < Duration::from_secs(1) {
                ok_first_second += 1;
            }
        } else {
            limited += 1;
        }
    }
    // GCRA 5/s with burst 5: 5 immediately, then one per 200 ms -> at most quota + burst = 10
    // inside the first second; at least the burst.
    assert!(
        (5..=10).contains(&ok_first_second),
        "{ok_first_second} acquired in the first second"
    );
    // By 1.5 s at most 5 + 1.5 * 5 (+1 for the boundary) succeeded; the rest timed out.
    assert!(ok_total <= 13, "{ok_total} acquired within max_wait");
    assert_eq!(ok_total + limited, 20);
    assert!(limited >= 7, "{limited} rate_limited");
    assert_eq!(
        counter("ostro_effect_rate_limited_total{target=https://a.quota.example}"),
        limited as u64
    );
    assert_eq!(
        counter("ostro_effect_rate_limited_total{target=https://b.quota.example}"),
        0
    );
}

#[tokio::test]
async fn bounded_wait_fails_rate_limited_and_counts() {
    install_recorder();
    let rl = RateLimiter::new(None);
    let t = target("pg-main", 1, 1);
    rl.acquire(&t, Duration::from_millis(50)).await.unwrap();
    let before = counter("ostro_effect_rate_limited_total{target=pg-main}");
    let started = Instant::now();
    assert_rate_limited(rl.acquire(&t, Duration::from_millis(50)).await);
    let waited = started.elapsed();
    assert!(waited >= Duration::from_millis(45), "{waited:?}");
    assert!(waited < Duration::from_millis(900), "{waited:?}");
    assert_eq!(
        counter("ostro_effect_rate_limited_total{target=pg-main}"),
        before + 1
    );
}

#[tokio::test]
async fn waiting_within_max_wait_succeeds() {
    let rl = RateLimiter::new(None);
    let t = target("s3-wait", 10, 1);
    rl.acquire(&t, Duration::ZERO).await.unwrap();
    let started = Instant::now();
    // Next cell after ~100 ms; max_wait 1 s.
    rl.acquire(&t, Duration::from_secs(1)).await.unwrap();
    let waited = started.elapsed();
    assert!(waited >= Duration::from_millis(80), "{waited:?}");
    assert!(waited < Duration::from_millis(900), "{waited:?}");
}

#[tokio::test]
async fn unlimited_without_quota_and_default_applies() {
    let unlimited = RateLimiter::new(None);
    let none = Target {
        key: "https://free.example".into(),
        quota: None,
    };
    for _ in 0..1_000 {
        unlimited.acquire(&none, Duration::ZERO).await.unwrap();
    }

    let defaulted = RateLimiter::new(Some(QuotaSpec {
        per_second: 1,
        burst: 2,
    }));
    defaulted.acquire(&none, Duration::ZERO).await.unwrap();
    defaulted.acquire(&none, Duration::ZERO).await.unwrap();
    assert_rate_limited(defaulted.acquire(&none, Duration::from_millis(10)).await);
    // A target's own quota wins over the default.
    let own = target("https://own.example", 100, 100);
    for _ in 0..50 {
        defaulted.acquire(&own, Duration::ZERO).await.unwrap();
    }
}
