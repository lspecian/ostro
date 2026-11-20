//! Bridge driver core (06-07) with fake connector, gate and sink: dedupe/attach, gate abandon,
//! completion retry, cancellation, panic -> internal_error (W11), outcome bytes, unsupported
//! kinds, rate limits in the pipeline, shutdown and metrics.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};
use ostro_bridge::config::ConnectorsConfig;
use ostro_bridge::connector::{
    CompletionError, CompletionSink, Connector, EffectCtx, GateDecision, SendGate, Target,
};
use ostro_bridge::driver::{Bridge, EffectBase};
use ostro_bridge::outcome::{encode_outcome, non_retryable};
use ostro_bridge::ratelimit::{QuotaSpec, RateLimiter};
use ostro_types::journal::{
    EffectRequest, Failure, HttpRequest, HttpResponse, OpOutput, RecordedResult, SqlRequest,
    decode_result, encode_result,
};
use ostro_types::live::{EffectKind, LiveEffect};

// ---- minimal global metrics recorder ----

static METRICS: LazyLock<Mutex<BTreeMap<String, Arc<AtomicU64>>>> =
    LazyLock::new(|| Mutex::new(BTreeMap::new()));

fn key_string(key: &Key) -> String {
    let mut labels: Vec<String> = key
        .labels()
        .map(|l| format!("{}={}", l.key(), l.value()))
        .collect();
    labels.sort();
    format!("{}{{{}}}", key.name(), labels.join(","))
}

fn handle(key: &Key) -> Arc<AtomicU64> {
    Arc::clone(METRICS.lock().unwrap().entry(key_string(key)).or_default())
}

struct HistCount(Arc<AtomicU64>);

impl metrics::HistogramFn for HistCount {
    fn record(&self, _: f64) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

struct TestRecorder;

impl Recorder for TestRecorder {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> Counter {
        Counter::from_arc(handle(key))
    }
    fn register_gauge(&self, key: &Key, _: &Metadata<'_>) -> Gauge {
        Gauge::from_arc(handle(key))
    }
    fn register_histogram(&self, key: &Key, _: &Metadata<'_>) -> Histogram {
        Histogram::from_arc(Arc::new(HistCount(handle(key))))
    }
}

fn install_recorder() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| metrics::set_global_recorder(TestRecorder).expect("recorder"));
}

fn metric(name_and_labels: &str) -> u64 {
    METRICS
        .lock()
        .unwrap()
        .get(name_and_labels)
        .map_or(0, |c| c.load(Ordering::Relaxed))
}

fn metric_exists(name_and_labels: &str) -> bool {
    METRICS.lock().unwrap().contains_key(name_and_labels)
}

// ---- fakes ----

#[derive(Default)]
struct FakeConnector {
    delay: Duration,
    panic: bool,
    cancel_in_execute: bool,
    target: Option<Target>,
    result: Option<Result<OpOutput, Failure>>,
    executions: AtomicUsize,
}

fn ok_output() -> Result<OpOutput, Failure> {
    Ok(OpOutput::Http(HttpResponse {
        status: 201,
        headers: vec![],
        body: b"created".to_vec(),
    }))
}

#[async_trait]
impl Connector for FakeConnector {
    fn kind(&self) -> EffectKind {
        EffectKind::Http
    }

    fn target(&self, _: &EffectCtx, _: &LiveEffect) -> Option<Target> {
        self.target.clone()
    }

    async fn execute(
        &self,
        ctx: &EffectCtx,
        _: &LiveEffect,
        _: &dyn SendGate,
    ) -> Result<OpOutput, Failure> {
        self.executions.fetch_add(1, Ordering::SeqCst);
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        if self.panic {
            panic!("fake connector bug");
        }
        if self.cancel_in_execute {
            ctx.cancel.cancel();
        }
        self.result.clone().unwrap_or_else(ok_output)
    }
}

struct FakeGate {
    decision: Mutex<GateDecision>,
    checks: AtomicUsize,
}

impl FakeGate {
    fn new(d: GateDecision) -> Arc<Self> {
        Arc::new(Self {
            decision: Mutex::new(d),
            checks: AtomicUsize::new(0),
        })
    }
}

#[async_trait]
impl SendGate for FakeGate {
    async fn check(&self, _: u16, _: &str, _: u64) -> GateDecision {
        self.checks.fetch_add(1, Ordering::SeqCst);
        *self.decision.lock().unwrap()
    }
}

#[derive(Default)]
struct FakeSink {
    completions: Mutex<Vec<(u16, String, u64, RecordedResult)>>,
    fail_unavailable: AtomicUsize,
    dropped: bool,
    attempts: AtomicUsize,
}

impl FakeSink {
    fn failing(n: usize) -> Arc<Self> {
        let s = Self::default();
        s.fail_unavailable.store(n, Ordering::SeqCst);
        Arc::new(s)
    }

    fn count(&self) -> usize {
        self.completions.lock().unwrap().len()
    }

    fn outcome(&self, i: usize) -> Result<OpOutput, Failure> {
        decode_result(&self.completions.lock().unwrap()[i].3).unwrap()
    }
}

#[async_trait]
impl CompletionSink for FakeSink {
    async fn complete(
        &self,
        partition: u16,
        run_id: &str,
        op: u64,
        outcome: RecordedResult,
    ) -> Result<(), CompletionError> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        if self
            .fail_unavailable
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            return Err(CompletionError::Unavailable("not leader yet".into()));
        }
        if self.dropped {
            return Err(CompletionError::Dropped("run terminal".into()));
        }
        self.completions
            .lock()
            .unwrap()
            .push((partition, run_id.to_owned(), op, outcome));
        Ok(())
    }
}

// ---- helpers ----

fn base(run: &str) -> EffectBase {
    EffectBase {
        cluster_id: "c1".into(),
        partition: 3,
        run_id: run.into(),
        attempt: 1,
        module: "_/m".into(),
        tenant: None,
    }
}

fn http(op: u64, timeout_ms: Option<u64>) -> LiveEffect {
    LiveEffect {
        op,
        request: EffectRequest::Http(HttpRequest {
            method: "POST".into(),
            url: "https://api.example.com/charge".into(),
            headers: vec![],
            body: Some(b"{}".to_vec()),
            timeout_ms,
            idempotency_key: None,
            redact: vec![],
        }),
        http_retry: None,
        scheduled_ts_nanos: 0,
        redrive: false,
    }
}

fn sql(op: u64) -> LiveEffect {
    LiveEffect {
        op,
        request: EffectRequest::Sql(SqlRequest {
            connection: "pg".into(),
            statement: "select 1".into(),
            params: vec![],
            exactly_once: false,
        }),
        http_retry: None,
        scheduled_ts_nanos: 0,
        redrive: false,
    }
}

fn bridge(
    conn: Arc<FakeConnector>,
    gate: Arc<FakeGate>,
    sink: Arc<FakeSink>,
    rate_max_wait: Duration,
) -> Bridge {
    install_recorder();
    let mut b = Bridge::new(
        ConnectorsConfig::default(),
        rate_max_wait,
        gate,
        sink,
        Arc::new(RateLimiter::new(None)),
    );
    b.register(conn);
    b
}

async fn wait_until(what: &str, timeout: Duration, mut cond: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while !cond() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

fn assert_failure_kind(r: &Result<OpOutput, Failure>, kind: &str) {
    match r {
        Err(Failure::NonRetryable(e) | Failure::Retryable(e)) => assert_eq!(e.kind, kind),
        other => panic!("expected failure {kind}, got {other:?}"),
    }
}

// ---- tests ----

#[tokio::test]
async fn double_start_attaches_one_execution_one_completion() {
    let conn = Arc::new(FakeConnector {
        delay: Duration::from_millis(100),
        ..Default::default()
    });
    let sink = Arc::new(FakeSink::default());
    let b = bridge(
        Arc::clone(&conn),
        FakeGate::new(GateDecision::Proceed),
        Arc::clone(&sink),
        Duration::from_secs(30),
    );
    let attach_before = metric("ostro_effect_attach_total{}");
    b.start(base("r-dup"), http(1, None));
    b.start(base("r-dup"), http(1, None));
    assert_eq!(b.inflight(), 1);
    wait_until("completion", Duration::from_secs(5), || sink.count() == 1).await;
    wait_until("entry removed", Duration::from_secs(5), || {
        b.inflight() == 0
    })
    .await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(conn.executions.load(Ordering::SeqCst), 1);
    assert_eq!(sink.count(), 1);
    assert!(metric("ostro_effect_attach_total{}") > attach_before);
    let (p, run, op, _) = sink.completions.lock().unwrap()[0].clone();
    assert_eq!((p, run.as_str(), op), (3, "r-dup", 1));
}

#[tokio::test]
async fn abandon_means_no_execution_and_no_completion() {
    let conn = Arc::new(FakeConnector::default());
    let gate = FakeGate::new(GateDecision::Abandon);
    let sink = Arc::new(FakeSink::default());
    let b = bridge(
        Arc::clone(&conn),
        Arc::clone(&gate),
        Arc::clone(&sink),
        Duration::from_secs(30),
    );
    b.start(base("r-abandon"), http(1, None));
    wait_until("task end", Duration::from_secs(5), || b.inflight() == 0).await;
    assert_eq!(gate.checks.load(Ordering::SeqCst), 1);
    assert_eq!(conn.executions.load(Ordering::SeqCst), 0);
    assert_eq!(sink.attempts.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn unavailable_is_retried_with_backoff_then_completes_once() {
    let conn = Arc::new(FakeConnector::default());
    let sink = FakeSink::failing(3);
    let b = bridge(
        Arc::clone(&conn),
        FakeGate::new(GateDecision::Proceed),
        Arc::clone(&sink),
        Duration::from_secs(30),
    );
    let t0 = Instant::now();
    b.start(base("r-unavail"), http(7, None));
    wait_until("completion", Duration::from_secs(5), || sink.count() == 1).await;
    // 100 + 200 + 400 ms of backoff.
    assert!(
        t0.elapsed() >= Duration::from_millis(650),
        "{:?}",
        t0.elapsed()
    );
    assert_eq!(sink.attempts.load(Ordering::SeqCst), 4);
    wait_until("entry removed", Duration::from_secs(5), || {
        b.inflight() == 0
    })
    .await;
    assert_eq!(sink.count(), 1);
    assert_eq!(conn.executions.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn dropped_completion_ends_the_task() {
    let sink = Arc::new(FakeSink {
        dropped: true,
        ..Default::default()
    });
    let b = bridge(
        Arc::new(FakeConnector::default()),
        FakeGate::new(GateDecision::Proceed),
        Arc::clone(&sink),
        Duration::from_secs(30),
    );
    b.start(base("r-dropped"), http(1, None));
    wait_until("task end", Duration::from_secs(5), || b.inflight() == 0).await;
    assert_eq!(sink.attempts.load(Ordering::SeqCst), 1);
    assert_eq!(sink.count(), 0);
}

#[tokio::test]
async fn cancel_mid_execution_submits_nothing() {
    let conn = Arc::new(FakeConnector {
        delay: Duration::from_secs(10),
        ..Default::default()
    });
    let sink = Arc::new(FakeSink::default());
    let b = bridge(
        Arc::clone(&conn),
        FakeGate::new(GateDecision::Proceed),
        Arc::clone(&sink),
        Duration::from_secs(30),
    );
    b.start(base("r-cancel"), http(1, None));
    b.start(base("r-cancel"), http(2, None));
    b.start(base("r-other"), http(1, None));
    wait_until("executing", Duration::from_secs(5), || {
        conn.executions.load(Ordering::SeqCst) == 3
    })
    .await;
    b.cancel(3, "r-cancel", 1);
    wait_until("op 1 gone", Duration::from_secs(5), || b.inflight() == 2).await;
    b.cancel_run(3, "r-cancel");
    wait_until("run gone", Duration::from_secs(5), || b.inflight() == 1).await;
    b.cancel_run(3, "r-other");
    wait_until("all gone", Duration::from_secs(5), || b.inflight() == 0).await;
    assert_eq!(sink.attempts.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn connector_cancelling_its_token_means_no_completion() {
    let conn = Arc::new(FakeConnector {
        cancel_in_execute: true,
        ..Default::default()
    });
    let sink = Arc::new(FakeSink::default());
    let b = bridge(
        Arc::clone(&conn),
        FakeGate::new(GateDecision::Proceed),
        Arc::clone(&sink),
        Duration::from_secs(30),
    );
    b.start(base("r-late-abandon"), http(1, None));
    wait_until("task end", Duration::from_secs(5), || b.inflight() == 0).await;
    assert_eq!(conn.executions.load(Ordering::SeqCst), 1);
    assert_eq!(sink.attempts.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn panic_completes_internal_error_once_and_removes_entry() {
    let conn = Arc::new(FakeConnector {
        panic: true,
        ..Default::default()
    });
    let sink = Arc::new(FakeSink::default());
    let b = bridge(
        Arc::clone(&conn),
        FakeGate::new(GateDecision::Proceed),
        Arc::clone(&sink),
        Duration::from_secs(30),
    );
    b.start(base("r-panic"), http(4, None));
    wait_until("completion", Duration::from_secs(5), || sink.count() == 1).await;
    wait_until("entry removed", Duration::from_secs(5), || {
        b.inflight() == 0
    })
    .await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(sink.count(), 1, "exactly one completion, no respawn");
    assert_eq!(conn.executions.load(Ordering::SeqCst), 1);
    match sink.outcome(0) {
        Err(Failure::NonRetryable(e)) => {
            assert_eq!(e.kind, "internal_error");
            assert_eq!(e.message, "bridge connector panicked");
        }
        other => panic!("expected internal_error, got {other:?}"),
    }
}

#[tokio::test]
async fn encoded_bytes_equal_encode_result() {
    let failure: Result<OpOutput, Failure> = Err(non_retryable(
        "egress_denied",
        "denied",
        Some(serde_json::json!({"host": "10.0.0.1"})),
    ));
    for (run, result) in [("r-bytes-ok", ok_output()), ("r-bytes-err", failure)] {
        let conn = Arc::new(FakeConnector {
            result: Some(result.clone()),
            ..Default::default()
        });
        let sink = Arc::new(FakeSink::default());
        let b = bridge(
            conn,
            FakeGate::new(GateDecision::Proceed),
            Arc::clone(&sink),
            Duration::from_secs(30),
        );
        b.start(base(run), http(1, None));
        wait_until("completion", Duration::from_secs(5), || sink.count() == 1).await;
        let got = sink.completions.lock().unwrap()[0].3.clone();
        assert_eq!(got, encode_result(&result));
        assert_eq!(got, encode_outcome(&result));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hundred_distinct_ops_complete_and_drain() {
    let conn = Arc::new(FakeConnector {
        delay: Duration::from_millis(10),
        ..Default::default()
    });
    let sink = Arc::new(FakeSink::default());
    let b = bridge(
        Arc::clone(&conn),
        FakeGate::new(GateDecision::Proceed),
        Arc::clone(&sink),
        Duration::from_secs(30),
    );
    for op in 0..100 {
        b.start(base("r-many"), http(op, None));
    }
    wait_until("100 completions", Duration::from_secs(10), || {
        sink.count() == 100
    })
    .await;
    wait_until("drained", Duration::from_secs(5), || b.inflight() == 0).await;
    let mut ops: Vec<u64> = sink
        .completions
        .lock()
        .unwrap()
        .iter()
        .map(|c| c.2)
        .collect();
    ops.sort_unstable();
    assert_eq!(ops, (0..100).collect::<Vec<_>>());
    assert_eq!(conn.executions.load(Ordering::SeqCst), 100);
}

#[tokio::test]
async fn unknown_kind_completes_unsupported_op() {
    let sink = Arc::new(FakeSink::default());
    let b = bridge(
        Arc::new(FakeConnector::default()),
        FakeGate::new(GateDecision::Proceed),
        Arc::clone(&sink),
        Duration::from_secs(30),
    );
    b.start(base("r-sql"), sql(2));
    wait_until("completion", Duration::from_secs(5), || sink.count() == 1).await;
    assert_failure_kind(&sink.outcome(0), "unsupported_op");
}

#[tokio::test]
async fn rate_limit_in_pipeline_bounded_by_effect_timeout() {
    let conn = Arc::new(FakeConnector {
        target: Some(Target {
            key: "https://api.example.com".into(),
            quota: Some(QuotaSpec {
                per_second: 1,
                burst: 1,
            }),
        }),
        ..Default::default()
    });
    let sink = Arc::new(FakeSink::default());
    // rate_max_wait 30 s, but the effect's timeout_ms (60 ms) bounds the wait.
    let b = bridge(
        Arc::clone(&conn),
        FakeGate::new(GateDecision::Proceed),
        Arc::clone(&sink),
        Duration::from_secs(30),
    );
    let t0 = Instant::now();
    b.start(base("r-rate"), http(1, Some(60)));
    wait_until("first", Duration::from_secs(5), || sink.count() == 1).await;
    b.start(base("r-rate"), http(2, Some(60)));
    wait_until("second", Duration::from_secs(5), || sink.count() == 2).await;
    assert!(
        t0.elapsed() < Duration::from_millis(900),
        "{:?}",
        t0.elapsed()
    );
    assert!(sink.outcome(0).is_ok());
    assert_failure_kind(&sink.outcome(1), "rate_limited");
    assert_eq!(
        conn.executions.load(Ordering::SeqCst),
        1,
        "rate-limited op never executes"
    );
    assert!(metric("ostro_effect_rate_limited_total{target=https://api.example.com}") >= 1);
}

#[tokio::test]
async fn task_survives_caller_and_bridge_drop() {
    let conn = Arc::new(FakeConnector {
        delay: Duration::from_millis(50),
        ..Default::default()
    });
    let sink = Arc::new(FakeSink::default());
    let b = bridge(
        conn,
        FakeGate::new(GateDecision::Proceed),
        Arc::clone(&sink),
        Duration::from_secs(30),
    );
    // The caller is a short-lived task; the bridge handle is dropped right after.
    tokio::spawn(async move {
        b.start(base("r-orphan"), http(1, None));
    })
    .await
    .unwrap();
    wait_until("completion", Duration::from_secs(5), || sink.count() == 1).await;
}

#[tokio::test]
async fn shutdown_cancels_after_grace_and_refuses_new_starts() {
    let conn = Arc::new(FakeConnector {
        delay: Duration::from_secs(10),
        ..Default::default()
    });
    let sink = Arc::new(FakeSink::default());
    let b = bridge(
        Arc::clone(&conn),
        FakeGate::new(GateDecision::Proceed),
        Arc::clone(&sink),
        Duration::from_secs(30),
    );
    b.start(base("r-shutdown"), http(1, None));
    wait_until("executing", Duration::from_secs(5), || {
        conn.executions.load(Ordering::SeqCst) == 1
    })
    .await;
    let t0 = Instant::now();
    b.shutdown(Duration::from_millis(50)).await;
    assert!(t0.elapsed() >= Duration::from_millis(50));
    assert!(t0.elapsed() < Duration::from_secs(5));
    assert_eq!(b.inflight(), 0);
    b.start(base("r-shutdown"), http(2, None));
    assert_eq!(b.inflight(), 0);
    assert_eq!(sink.attempts.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn metrics_are_recorded() {
    let sink = Arc::new(FakeSink::default());
    let b = bridge(
        Arc::new(FakeConnector::default()),
        FakeGate::new(GateDecision::Proceed),
        Arc::clone(&sink),
        Duration::from_secs(30),
    );
    let ok_before = metric("ostro_effect_completed_total{kind=http,outcome=ok}");
    let dur_before = metric("ostro_effect_duration_seconds{kind=http}");
    b.start(base("r-metrics"), http(1, None));
    wait_until("completion", Duration::from_secs(5), || sink.count() == 1).await;
    wait_until("drained", Duration::from_secs(5), || b.inflight() == 0).await;
    assert!(metric("ostro_effect_completed_total{kind=http,outcome=ok}") > ok_before);
    assert!(metric("ostro_effect_duration_seconds{kind=http}") > dur_before);
    assert!(metric_exists("ostro_effect_inflight{}"));
}

#[cfg(feature = "failpoints")]
mod failpoints {
    use super::*;

    #[tokio::test]
    async fn before_complete_submit_error_drops_and_wait_delays() {
        let scenario = fail::FailScenario::setup();

        fail::cfg("bridge::effect::before_complete_submit", "return(error)").unwrap();
        let sink = Arc::new(FakeSink::default());
        let b = bridge(
            Arc::new(FakeConnector::default()),
            FakeGate::new(GateDecision::Proceed),
            Arc::clone(&sink),
            Duration::from_secs(30),
        );
        b.start(base("r-fp-error"), http(1, None));
        wait_until("task end", Duration::from_secs(5), || b.inflight() == 0).await;
        assert_eq!(sink.attempts.load(Ordering::SeqCst), 0);

        fail::cfg("bridge::effect::before_complete_submit", "return(wait:150)").unwrap();
        let t0 = Instant::now();
        b.start(base("r-fp-wait"), http(1, None));
        wait_until("completion", Duration::from_secs(5), || sink.count() == 1).await;
        assert!(
            t0.elapsed() >= Duration::from_millis(140),
            "{:?}",
            t0.elapsed()
        );

        fail::cfg("bridge::effect::before_complete_submit", "return(hang)").unwrap();
        b.start(base("r-fp-hang"), http(1, None));
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(b.inflight(), 1, "hung before submit");
        b.cancel_run(3, "r-fp-hang");
        wait_until("hang cancelled", Duration::from_secs(5), || {
            b.inflight() == 0
        })
        .await;
        assert_eq!(sink.count(), 1);

        fail::remove("bridge::effect::before_complete_submit");
        scenario.teardown();
    }
}
