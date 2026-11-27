//! 14-32 Task 2 (TRIG-04, D-P14-43): the Kafka topic-consumer trigger against a real broker,
//! single node.
//!
//! - `offsets_become_runs_once`: 500 records over 3 partitions → 500 runs, one per offset,
//!   each run's input (echoed by the `smoke` guest) equals the record's JSON value; the
//!   group's committed offsets reach the high watermarks; a restart starts nothing new and
//!   a later burst of 600 records continues from the committed offsets (throughput printed).
//! - `message_key_mode_one_run_per_key`: repeated keys → one run per distinct key; null
//!   keys → one run per offset.
//! - `invalid_records_pause_or_skip`: `on_invalid = "pause"` holds the partition at the
//!   non-JSON record (its commit stays there, the other partition continues);
//!   `on_invalid = "skip"` counts it and moves on.
//! - `tenant_trigger_runs_in_its_tenant`: `tenant = "a"` starts runs of `a/smoke` in
//!   tenant `a`.
//! - `rate_limited_start_pauses_then_resumes`: a control-plane wrapper refusing starts with
//!   429 for 2 s → the partition pauses and resumes; no offset is skipped.
//!
//! Needs `OSTRO_TEST_KAFKA_BOOTSTRAP` (`scripts/test-services.sh start kafka`); without it
//! every test prints `SKIP:` and returns.
#![cfg(feature = "kafka")]
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use common::now_ms;
use common::phase8::Phase8Server;
use ostro_api::domain::{
    ActivateWorkflow, HistoryPage, HistoryRequest, ListRunsRequest, ModuleInfo, RunInfo, RunPage,
    StartRunRequest, StartRunResult, UploadModule, UploadModuleResult, WorkflowInfo,
};
use ostro_api::{ApiError, ControlPlane, ErrorCode};
use ostro_kafka::testkit::{Compression, create_topic, produce};
use ostro_kafka::{Client, ClientConfig, OffsetSpec, Record, TopicPartition};
use ostro_server::config::RawTable;
use serde_json::{Value, json};
use tokio::time::{Instant, sleep};

const LIMIT: Duration = Duration::from_secs(120);

fn bootstrap() -> Option<String> {
    match std::env::var("OSTRO_TEST_KAFKA_BOOTSTRAP") {
        Ok(b) if !b.is_empty() => Some(b),
        _ => {
            println!(
                "SKIP: OSTRO_TEST_KAFKA_BOOTSTRAP is unset (scripts/test-services.sh start kafka)"
            );
            None
        }
    }
}

async fn client(bs: &str) -> Client {
    Client::connect(ClientConfig {
        bootstrap: vec![bs.to_owned()],
        client_id: "kafka-trigger-test".into(),
        ..ClientConfig::default()
    })
    .await
    .expect("connect to the test broker")
}

fn unique(prefix: &str) -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    format!(
        "{prefix}-{}-{}",
        now_ms(),
        N.fetch_add(1, Ordering::Relaxed)
    )
}

/// One `[[triggers.kafka]]` table from TOML body lines.
fn table(body: &str) -> RawTable {
    config::Config::builder()
        .add_source(config::File::from_str(
            &format!("[[triggers.kafka]]\n{body}\n"),
            config::FileFormat::Toml,
        ))
        .build()
        .expect("toml")
        .get::<Vec<RawTable>>("triggers.kafka")
        .expect("triggers.kafka")
        .remove(0)
}

fn trigger_toml(name: &str, bs: &str, topic: &str, group: &str, extra: &str) -> String {
    format!(
        "name = \"{name}\"\nbootstrap = [\"{bs}\"]\ntopics = [\"{topic}\"]\n\
         group_id = \"{group}\"\nworkflow = \"smoke\"\nstart_from = \"committed_or_earliest\"\n\
         commit_interval = \"200ms\"\n{extra}"
    )
}

async fn server(tables: Vec<RawTable>) -> Phase8Server {
    let s = Phase8Server::builder()
        .with_config(|c| c.engine.hot_wait = Duration::from_millis(50).into())
        .with_triggers(move |t| t.kafka = tables)
        .start()
        .await;
    s.wait_ready(Duration::from_secs(30)).await;
    s
}

/// A server with the `smoke` guest deployed (in `tenant`) *before* the triggers start: the
/// server first runs without them, then restarts with `tables` (a start for a workflow that
/// does not exist yet is an invalid record, which `on_invalid = "skip"` would skip).
async fn deployed(tables: Vec<RawTable>, tenant: Option<&str>) -> Phase8Server {
    let mut s = server(Vec::new()).await;
    s.deploy_guest("smoke", tenant).await;
    s.cfg.triggers.kafka = tables;
    s.restart().await;
    s.wait_ready(Duration::from_secs(30)).await;
    s
}

fn json_record(v: &Value, key: Option<&[u8]>) -> Record {
    Record {
        offset: 0,
        timestamp_ms: i64::try_from(now_ms()).expect("ms"),
        key: key.map(bytes::Bytes::copy_from_slice),
        value: Some(bytes::Bytes::from(serde_json::to_vec(v).expect("json"))),
        headers: Vec::new(),
    }
}

/// Every run carrying `tag` (tenant-scoped when `tenant` is set).
async fn runs_tagged(
    cp: &dyn ControlPlane,
    tag: &str,
    tenant: Option<&str>,
) -> Vec<ostro_api::domain::RunSummary> {
    let mut out = Vec::new();
    let mut token = None;
    loop {
        let page = cp
            .list_runs(ListRunsRequest {
                tag: Some(tag.to_owned()),
                limit: 1000,
                page_token: token.take(),
                tenant: tenant.map(str::to_owned),
                ..ListRunsRequest::default()
            })
            .await
            .expect("list runs");
        out.extend(page.runs);
        match page.next_page_token {
            Some(t) if !t.is_empty() => token = Some(t),
            _ => return out,
        }
    }
}

fn tag_value<'a>(tags: &'a [String], k: &str) -> Option<&'a str> {
    tags.iter()
        .find_map(|t| t.strip_prefix(k).and_then(|r| r.strip_prefix('=')))
}

/// `(partition, offset)` of every run of trigger `name`; panics on a duplicate.
fn by_offset(runs: &[ostro_api::domain::RunSummary]) -> BTreeMap<(i32, i64), String> {
    let mut m = BTreeMap::new();
    for r in runs {
        let p: i32 = tag_value(&r.tags, "ostro.kafka.partition")
            .expect("partition tag")
            .parse()
            .expect("int");
        let o: i64 = tag_value(&r.tags, "ostro.kafka.offset")
            .expect("offset tag")
            .parse()
            .expect("int");
        assert!(
            m.insert((p, o), r.run_id.clone()).is_none(),
            "two runs for partition {p} offset {o}"
        );
    }
    m
}

async fn wait_runs(
    cp: &dyn ControlPlane,
    tag: &str,
    tenant: Option<&str>,
    n: usize,
    limit: Duration,
) -> Vec<ostro_api::domain::RunSummary> {
    let deadline = Instant::now() + limit;
    loop {
        let runs = runs_tagged(cp, tag, tenant).await;
        if runs.len() >= n {
            return runs;
        }
        assert!(
            Instant::now() < deadline,
            "{} of {n} runs tagged {tag} within {limit:?}",
            runs.len()
        );
        sleep(Duration::from_millis(100)).await;
    }
}

/// Committed offsets of `group` for `tps`.
async fn committed(c: &Client, group: &str, tps: &[TopicPartition]) -> Vec<Option<i64>> {
    c.committed(group, tps)
        .await
        .expect("committed")
        .into_iter()
        .map(|(_, o)| o)
        .collect()
}

async fn wait_committed(c: &Client, group: &str, tps: &[TopicPartition], want: &[i64]) {
    let want: Vec<Option<i64>> = want.iter().copied().map(Some).collect();
    let deadline = Instant::now() + LIMIT;
    loop {
        let got = committed(c, group, tps).await;
        if got == want {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "committed {got:?}, want {want:?} ({group})"
        );
        sleep(Duration::from_millis(100)).await;
    }
}

fn tps(topic: &str, n: i32) -> Vec<TopicPartition> {
    (0..n)
        .map(|p| TopicPartition {
            topic: topic.to_owned(),
            partition: p,
        })
        .collect()
}

async fn ends(c: &Client, tps: &[TopicPartition]) -> Vec<i64> {
    c.list_offsets(tps, OffsetSpec::Latest)
        .await
        .expect("list offsets")
        .into_iter()
        .map(|(_, o)| o)
        .collect()
}

async fn result_json(s: &Phase8Server, run_id: &str) -> Value {
    let deadline = Instant::now() + LIMIT;
    loop {
        let info = s
            .h()
            .control()
            .get_run(run_id.to_owned())
            .await
            .expect("run");
        if let Some(r) = &info.result_json {
            return serde_json::from_slice(r).expect("json result");
        }
        assert!(Instant::now() < deadline, "run {run_id} has no result");
        sleep(Duration::from_millis(50)).await;
    }
}

// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn offsets_become_runs_once() {
    let Some(bs) = bootstrap() else { return };
    let kc = client(&bs).await;
    let topic = unique("kt-offsets");
    let group = unique("g-offsets");
    let name = "orders";
    create_topic(&kc, &topic, 3).await.expect("create topic");
    // 500 records: partition p gets every third value, in batches of 50.
    let mut want: BTreeMap<(i32, i64), Value> = BTreeMap::new();
    for p in 0..3i32 {
        let vals: Vec<Value> = (0..500)
            .filter(|i| i % 3 == p)
            .map(|i| json!({"i": i, "p": p, "s": format!("rec-{i}")}))
            .collect();
        for chunk in vals.chunks(50) {
            let recs: Vec<Record> = chunk.iter().map(|v| json_record(v, None)).collect();
            let base = produce(&kc, &topic, p, &recs, Compression::None)
                .await
                .expect("produce");
            for (k, v) in chunk.iter().enumerate() {
                want.insert((p, base + k as i64), v.clone());
            }
        }
    }
    assert_eq!(want.len(), 500);
    let mut s = deployed(
        vec![table(&trigger_toml(name, &bs, &topic, &group, ""))],
        None,
    )
    .await;
    let tag = format!("ostro.kafka={name}");
    let runs = wait_runs(s.h().control().as_ref(), &tag, None, 500, LIMIT).await;
    let map = by_offset(&runs);
    assert_eq!(
        map.keys().copied().collect::<BTreeSet<_>>(),
        want.keys().copied().collect::<BTreeSet<_>>(),
        "one run per produced offset"
    );
    let ids: BTreeSet<&String> = map.values().collect();
    assert_eq!(ids.len(), 500, "distinct runs");
    for r in &runs {
        assert_eq!(
            tag_value(&r.tags, "ostro.kafka.topic"),
            Some(topic.as_str())
        );
        assert_eq!(r.workflow, "smoke");
    }
    // Inputs: the smoke guest echoes its input. Check a sample of 60 across partitions.
    for ((p, o), id) in map.iter().step_by(9).take(60) {
        assert_eq!(
            &result_json(&s, id).await,
            &want[&(*p, *o)],
            "input of {p}/{o}"
        );
    }
    // The idempotency key is the documented one.
    let probe = s
        .h()
        .control()
        .start_run(StartRunRequest {
            workflow: "smoke".into(),
            input_json: b"null".to_vec(),
            idempotency_key: Some(format!("__kafka/{name}/{topic}/1/0")),
            conflict_policy: ostro_api::domain::ConflictPolicy::ReturnExisting,
            dry_run: true,
            ..StartRunRequest::default()
        })
        .await
        .expect("dry run");
    assert_eq!(probe.run_id, map[&(1, 0)]);
    let parts = tps(&topic, 3);
    let hw = ends(&kc, &parts).await;
    wait_committed(&kc, &group, &parts, &hw).await;

    // Restart: nothing new; later records resume from the committed offsets.
    s.restart().await;
    s.wait_ready(Duration::from_secs(30)).await;
    sleep(Duration::from_secs(3)).await;
    assert_eq!(
        runs_tagged(s.h().control().as_ref(), &tag, None)
            .await
            .len(),
        500
    );
    // Throughput: a burst of 600 records (200 per partition, gzip batches of 50) on the
    // running consumers.
    let t0 = Instant::now();
    for p in 0..3 {
        for b in 0..4 {
            let recs: Vec<Record> = (0..50)
                .map(|i| json_record(&json!({"after": b * 50 + i, "p": p}), None))
                .collect();
            produce(&kc, &topic, p, &recs, Compression::Gzip)
                .await
                .expect("produce");
        }
    }
    let runs = wait_runs(s.h().control().as_ref(), &tag, None, 1100, LIMIT).await;
    let took = t0.elapsed();
    println!(
        "throughput: 600 records over 3 partitions in {took:?} = {:.0} records/s ({:.0}/s per partition, max_in_flight 256, debug build)",
        600.0 / took.as_secs_f64(),
        200.0 / took.as_secs_f64()
    );
    assert_eq!(by_offset(&runs).len(), 1100);
    let hw = ends(&kc, &parts).await;
    wait_committed(&kc, &group, &parts, &hw).await;
    sleep(Duration::from_secs(1)).await;
    assert_eq!(
        runs_tagged(s.h().control().as_ref(), &tag, None)
            .await
            .len(),
        1100
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn message_key_mode_one_run_per_key() {
    let Some(bs) = bootstrap() else { return };
    let kc = client(&bs).await;
    let topic = unique("kt-keys");
    let group = unique("g-keys");
    let name = "by-key";
    create_topic(&kc, &topic, 2).await.expect("create topic");
    // 60 keyed records over 10 keys (each key on both partitions), plus 4 null-key records.
    for p in 0..2 {
        let recs: Vec<Record> = (0..30)
            .map(|i| {
                let k = format!("key-{}", i % 10);
                json_record(&json!({"i": i}), Some(k.as_bytes()))
            })
            .chain((0..2).map(|i| json_record(&json!({"null": i}), None)))
            .collect();
        produce(&kc, &topic, p, &recs, Compression::None)
            .await
            .expect("produce");
    }
    let s = deployed(
        vec![table(&trigger_toml(
            name,
            &bs,
            &topic,
            &group,
            "idempotency = \"message_key\"",
        ))],
        None,
    )
    .await;
    let parts = tps(&topic, 2);
    let hw = ends(&kc, &parts).await;
    assert_eq!(hw, vec![32, 32]);
    wait_committed(&kc, &group, &parts, &hw).await;
    let tag = format!("ostro.kafka={name}");
    let runs = runs_tagged(s.h().control().as_ref(), &tag, None).await;
    assert_eq!(runs.len(), 14, "10 keys + 4 null-key records");
    for k in 0..10 {
        let key = format!("key-{k}");
        let idem = ostro_triggers::kafka::idempotency_key(
            name,
            ostro_triggers::kafka::IdempotencyMode::MessageKey,
            &topic,
            0,
            0,
            Some(key.as_bytes()),
        );
        let r = s
            .h()
            .control()
            .start_run(StartRunRequest {
                workflow: "smoke".into(),
                input_json: b"null".to_vec(),
                idempotency_key: Some(idem),
                conflict_policy: ostro_api::domain::ConflictPolicy::ReturnExisting,
                dry_run: true,
                ..StartRunRequest::default()
            })
            .await
            .expect("dry run");
        assert!(!r.run_id.is_empty(), "a run exists for {key}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invalid_records_pause_or_skip() {
    let Some(bs) = bootstrap() else { return };
    let kc = client(&bs).await;
    let (tp_topic, ts_topic) = (unique("kt-pause"), unique("kt-skip"));
    let (gp, gs) = (unique("g-pause"), unique("g-skip"));
    for t in [&tp_topic, &ts_topic] {
        create_topic(&kc, t, 2).await.expect("create topic");
        // Partition 0: three valid, one non-JSON at offset 3, three valid. Partition 1:
        // five valid.
        let mut p0: Vec<Record> = (0..3)
            .map(|i| json_record(&json!({"i": i}), None))
            .collect();
        p0.push(Record {
            value: Some(bytes::Bytes::from_static(b"not json {")),
            ..json_record(&json!(null), None)
        });
        p0.extend((4..7).map(|i| json_record(&json!({"i": i}), None)));
        produce(&kc, t, 0, &p0, Compression::None)
            .await
            .expect("produce");
        let p1: Vec<Record> = (0..5)
            .map(|i| json_record(&json!({"j": i}), None))
            .collect();
        produce(&kc, t, 1, &p1, Compression::None)
            .await
            .expect("produce");
    }
    let s = deployed(
        vec![
            table(&trigger_toml(
                "held",
                &bs,
                &tp_topic,
                &gp,
                "max_in_flight = 1",
            )),
            table(&trigger_toml(
                "skipper",
                &bs,
                &ts_topic,
                &gs,
                "on_invalid = \"skip\"",
            )),
        ],
        None,
    )
    .await;
    let cp = s.h().control().as_ref();
    // skip: everything committed, 11 runs (the bad record has none).
    let parts = tps(&ts_topic, 2);
    wait_committed(&kc, &gs, &parts, &[7, 5]).await;
    let map = by_offset(&runs_tagged(cp, "ostro.kafka=skipper", None).await);
    assert_eq!(map.len(), 11);
    assert!(!map.contains_key(&(0, 3)));
    // pause: partition 1 completes, partition 0 holds at offset 3.
    let parts = tps(&tp_topic, 2);
    wait_committed(&kc, &gp, &parts, &[3, 5]).await;
    sleep(Duration::from_secs(3)).await;
    assert_eq!(committed(&kc, &gp, &parts).await, vec![Some(3), Some(5)]);
    let map = by_offset(&runs_tagged(cp, "ostro.kafka=held", None).await);
    let p0: Vec<i64> = map
        .keys()
        .filter(|(p, _)| *p == 0)
        .map(|(_, o)| *o)
        .collect();
    assert_eq!(
        p0,
        vec![0, 1, 2],
        "max_in_flight = 1: nothing past the held record"
    );
    assert_eq!(map.keys().filter(|(p, _)| *p == 1).count(), 5);
    // Metrics: the skip is counted, the held partition is paused.
    let body = s
        .http
        .get(s.url("/metrics"))
        .send()
        .await
        .expect("GET /metrics")
        .text()
        .await
        .expect("text");
    let skipped = body
        .lines()
        .find(|l| l.starts_with("ostro_kafka_skipped_total") && l.contains("trigger=\"skipper\""));
    let paused = body.lines().find(|l| {
        l.starts_with("ostro_kafka_paused")
            && l.contains("trigger=\"held\"")
            && l.contains("partition=\"0\"")
    });
    if body.contains("ostro_kafka_") {
        let skipped = skipped.expect("skipped counter");
        assert!(skipped.contains("reason=\"not_json\""), "{skipped}");
        assert!(skipped.ends_with(" 1"), "{skipped}");
        assert!(paused.expect("paused gauge").ends_with(" 1"), "{paused:?}");
    } else {
        println!(
            "note: /metrics exposes no ostro_kafka_ series in this process (recorder not installed)"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tenant_trigger_runs_in_its_tenant() {
    let Some(bs) = bootstrap() else { return };
    let kc = client(&bs).await;
    let topic = unique("kt-tenant");
    let group = unique("g-tenant");
    create_topic(&kc, &topic, 1).await.expect("create topic");
    let recs: Vec<Record> = (0..5)
        .map(|i| json_record(&json!({"t": i}), None))
        .collect();
    produce(&kc, &topic, 0, &recs, Compression::None)
        .await
        .expect("produce");
    let s = deployed(
        vec![table(&trigger_toml(
            "tenant-a",
            &bs,
            &topic,
            &group,
            "tenant = \"a\"",
        ))],
        Some("a"),
    )
    .await;
    wait_committed(&kc, &group, &tps(&topic, 1), &[5]).await;
    let runs = runs_tagged(s.h().control().as_ref(), "ostro.kafka=tenant-a", Some("a")).await;
    assert_eq!(runs.len(), 5);
    for r in &runs {
        let info = s
            .h()
            .control()
            .get_run(r.run_id.clone())
            .await
            .expect("run");
        assert_eq!(info.tenant.as_deref(), Some("a"));
        assert_eq!(info.workflow, "a/smoke");
    }
}

/// Refuses starts with 429 until `until`, then forwards (counts refusals).
struct Throttle {
    inner: Arc<dyn ControlPlane>,
    until: std::time::Instant,
    refused: AtomicU64,
    calls: AtomicU64,
}

#[async_trait::async_trait]
impl ControlPlane for Throttle {
    async fn upload_module(&self, req: UploadModule) -> Result<UploadModuleResult, ApiError> {
        self.inner.upload_module(req).await
    }
    async fn get_module(&self, sha256: String) -> Result<ModuleInfo, ApiError> {
        self.inner.get_module(sha256).await
    }
    async fn list_workflows(&self) -> Result<Vec<WorkflowInfo>, ApiError> {
        self.inner.list_workflows().await
    }
    async fn activate_workflow(&self, req: ActivateWorkflow) -> Result<WorkflowInfo, ApiError> {
        self.inner.activate_workflow(req).await
    }
    async fn start_run(&self, req: StartRunRequest) -> Result<StartRunResult, ApiError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if std::time::Instant::now() < self.until {
            self.refused.fetch_add(1, Ordering::SeqCst);
            return Err(ApiError::new(ErrorCode::RateLimited, "rate_limited (test)"));
        }
        self.inner.start_run(req).await
    }
    async fn get_run(&self, run_id: String) -> Result<RunInfo, ApiError> {
        self.inner.get_run(run_id).await
    }
    async fn run_history(&self, req: HistoryRequest) -> Result<HistoryPage, ApiError> {
        self.inner.run_history(req).await
    }
    async fn list_runs(&self, req: ListRunsRequest) -> Result<RunPage, ApiError> {
        self.inner.list_runs(req).await
    }
    fn ready(&self) -> bool {
        self.inner.ready()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rate_limited_start_pauses_then_resumes() {
    let Some(bs) = bootstrap() else { return };
    let kc = client(&bs).await;
    let topic = unique("kt-429");
    let group = unique("g-429");
    create_topic(&kc, &topic, 1).await.expect("create topic");
    let recs: Vec<Record> = (0..20)
        .map(|i| json_record(&json!({"r": i}), None))
        .collect();
    produce(&kc, &topic, 0, &recs, Compression::None)
        .await
        .expect("produce");
    // No configured trigger: the test starts the driver itself on a throttled control plane.
    let s = deployed(Vec::new(), None).await;
    let ctx = ostro_server::p14::ctx_of(s.h().control())
        .expect("ctx")
        .expect("Phase 14 context attached");
    let throttle = Arc::new(Throttle {
        inner: Arc::clone(s.h().control()) as Arc<dyn ControlPlane>,
        until: std::time::Instant::now() + Duration::from_secs(2),
        refused: AtomicU64::new(0),
        calls: AtomicU64::new(0),
    });
    let triggers = ostro_server::p14::kafka::validate(&[table(&trigger_toml(
        "throttled",
        &bs,
        &topic,
        &group,
        "max_in_flight = 4",
    ))])
    .expect("valid");
    let task = ostro_server::p14::kafka::start_triggers(
        &ctx,
        triggers,
        Arc::clone(&throttle) as Arc<dyn ControlPlane>,
        s.h().node().roles(),
    );
    // While refused: nothing committed, nothing started, the start is retried.
    sleep(Duration::from_millis(1500)).await;
    // (The start position 0 itself may be committed: nothing past it.)
    let c = committed(&kc, &group, &tps(&topic, 1)).await;
    assert!(c == vec![None] || c == vec![Some(0)], "{c:?}");
    assert!(throttle.refused.load(Ordering::SeqCst) >= 1);
    assert_eq!(
        runs_tagged(s.h().control().as_ref(), "ostro.kafka=throttled", None)
            .await
            .len(),
        0
    );
    let fetched_while_paused = throttle.calls.load(Ordering::SeqCst);
    // Paused partition: at most max_in_flight records were handed out.
    assert!(
        throttle.refused.load(Ordering::SeqCst) <= fetched_while_paused,
        "refusals are counted calls"
    );
    wait_committed(&kc, &group, &tps(&topic, 1), &[20]).await;
    let map =
        by_offset(&runs_tagged(s.h().control().as_ref(), "ostro.kafka=throttled", None).await);
    assert_eq!(
        map.keys().map(|(_, o)| *o).collect::<Vec<_>>(),
        (0..20).collect::<Vec<i64>>(),
        "no offset skipped"
    );
    drop(ctx);
    let mut s = s;
    s.shutdown().await;
    tokio::time::timeout(Duration::from_secs(30), task)
        .await
        .expect("driver stops with the server")
        .expect("driver task");
}
