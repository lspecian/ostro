//! 14-32 Task 2 (TRIG-04, D-P14-43, tier C): Kafka consumption follows the leader of the
//! owner partition across a controlled handoff and a `kill -9`.
//!
//! Three static processes (failpoint binary, `OSTRO_FAILPOINT_BIN`) run one
//! `[[triggers.kafka]]` over a 2-partition topic while a producer keeps writing records.
//! Kafka partition 0's owner partition (`kafka_owner(name, topic, 0, P)`) is led by node L:
//! L gets SIGTERM (its partitions are handed off, its consumer stops and commits) and is
//! restarted; then the new owner leader is `kill -9`ed and restarted. Afterwards every
//! produced offset of both partitions has exactly one run (no offset skipped, no duplicate
//! run) and the group's committed offsets reach the high watermarks.
//!
//! Not `#[ignore]`: without `OSTRO_TEST_KAFKA_BOOTSTRAP` (until 14-27 wires the test
//! services into ci-local), the failpoint binary or loopback aliases it prints `SKIP:` and
//! returns.

use std::collections::BTreeMap;
use std::time::Duration;

use ostro_kafka::testkit::{Compression, create_topic, produce};
use ostro_kafka::{Client, ClientConfig, OffsetSpec, Record, TopicPartition};
use ostro_repl_conformance::harness::process::ProcCluster;
use ostro_repl_conformance::harness::{ApiClient, ClusterHarness};
use ostro_repl_conformance::scenarios::process::skip_reason;
use ostro_repl_conformance::scenarios::process_p13::{HEAL_T, start_static, wait_healthy};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::time::{Instant, sleep};

const NAME: &str = "failover";
const KPARTS: i32 = 2;
const LIMIT: Duration = Duration::from_secs(180);

/// `ostro_triggers::kafka::kafka_owner` (this crate does not depend on ostro-triggers): the
/// first 8 bytes of `sha256("__kafka/" ‖ name ‖ 0 ‖ topic ‖ 0 ‖ kpartition BE)` mod P.
fn kafka_owner(name: &str, topic: &str, kp: i32, p: u16) -> u16 {
    let mut h = Sha256::new();
    h.update(b"__kafka/");
    h.update(name.as_bytes());
    h.update([0u8]);
    h.update(topic.as_bytes());
    h.update([0u8]);
    h.update(kp.to_be_bytes());
    let d = h.finalize();
    let mut b = [0u8; 8];
    b.copy_from_slice(&d[..8]);
    (u64::from_be_bytes(b) % u64::from(p.max(1))) as u16
}

#[test]
fn owner_formula_matches_the_pinned_vectors() {
    // The vectors pinned in crates/ostro-triggers/tests/kafka.rs.
    assert_eq!(kafka_owner("orders", "orders", 0, 16), 14);
    assert_eq!(kafka_owner("orders", "orders", 1, 16), 2);
    assert_eq!(kafka_owner("billing", "invoices.v2", 7, 64), 18);
}

async fn kclient(bs: &str) -> Client {
    Client::connect(ClientConfig {
        bootstrap: vec![bs.to_owned()],
        client_id: "kafka-failover-test".into(),
        ..ClientConfig::default()
    })
    .await
    .expect("connect to the test broker")
}

fn record(v: &Value) -> Record {
    Record {
        value: Some(bytes::Bytes::from(serde_json::to_vec(v).expect("json"))),
        ..Record::default()
    }
}

/// Add the trigger to every node's config file (the rendered `kafka = []` line).
fn configure(c: &ProcCluster, bs: &str, topic: &str, group: &str) {
    let inline = format!(
        "kafka = [{{ name = \"{NAME}\", bootstrap = [\"{bs}\"], topics = [\"{topic}\"], \
         group_id = \"{group}\", workflow = \"smoke\", start_from = \"committed_or_earliest\", \
         commit_interval = \"200ms\", max_in_flight = 32 }}]"
    );
    for i in c.node_ids() {
        let path = c.config_path(i);
        let text = std::fs::read_to_string(path).expect("read config");
        assert_eq!(
            text.matches("kafka = []").count(),
            1,
            "one rendered kafka line in {}",
            path.display()
        );
        std::fs::write(path, text.replace("kafka = []", &inline)).expect("write config");
    }
}

/// `(partition, offset) -> run ids` of every run tagged `ostro.kafka=<NAME>`.
async fn runs(api: &ApiClient) -> BTreeMap<(i64, i64), Vec<String>> {
    let mut out: BTreeMap<(i64, i64), Vec<String>> = BTreeMap::new();
    let mut token: Option<String> = None;
    loop {
        let mut path = format!("/v1/runs?tag=ostro.kafka%3D{NAME}&limit=1000");
        if let Some(t) = &token {
            path.push_str(&format!("&page_token={t}"));
        }
        let (st, v) = api.get_json(&path).await.expect("list runs");
        assert!(st.is_success(), "list runs: {st} {v}");
        for r in v["runs"].as_array().expect("runs") {
            let tags: Vec<&str> = r["tags"]
                .as_array()
                .expect("tags")
                .iter()
                .filter_map(Value::as_str)
                .collect();
            let get = |k: &str| -> i64 {
                tags.iter()
                    .find_map(|t| t.strip_prefix(k))
                    .expect("tag")
                    .parse()
                    .expect("int")
            };
            out.entry((get("ostro.kafka.partition="), get("ostro.kafka.offset=")))
                .or_default()
                .push(r["run_id"].as_str().expect("id").to_owned());
        }
        match v["next_page_token"].as_str() {
            Some(t) if !t.is_empty() => token = Some(t.to_owned()),
            _ => return out,
        }
    }
}

/// A running node's client.
fn any_client(c: &mut ProcCluster) -> ApiClient {
    let id = c
        .node_ids()
        .into_iter()
        .find(|&i| c.is_running(i))
        .expect("a running node");
    c.client(id)
}

/// The owner partition's leader, once it is set and not `not`.
async fn owner_leader(c: &ProcCluster, owner: u16, not: Option<u64>) -> u64 {
    let deadline = Instant::now() + HEAL_T;
    loop {
        let a = c.assignment(owner).await;
        if a.leader != 0 && Some(a.leader) != not {
            return a.leader;
        }
        assert!(
            Instant::now() < deadline,
            "partition {owner} has no new leader (now {}, not {not:?})",
            a.leader
        );
        sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn consumption_follows_the_owner_leader() {
    let bs = match std::env::var("OSTRO_TEST_KAFKA_BOOTSTRAP") {
        Ok(b) if !b.is_empty() => b,
        _ => {
            println!(
                "SKIP: OSTRO_TEST_KAFKA_BOOTSTRAP is unset (scripts/test-services.sh start kafka)"
            );
            return;
        }
    };
    if let Some(r) = skip_reason() {
        println!("{r}");
        return;
    }
    let kc = kclient(&bs).await;
    let stamp = std::process::id();
    let topic = format!("kf-{stamp}-{}", rand::random::<u32>());
    let group = format!("g-{topic}");
    create_topic(&kc, &topic, KPARTS)
        .await
        .expect("create topic");

    let mut c = start_static(1432, None).await.expect("static cluster");
    c.client(1)
        .deploy_test_module()
        .await
        .expect("deploy smoke");
    let p = c
        .client(1)
        .partition_count()
        .await
        .expect("partition count");
    configure(&c, &bs, &topic, &group);
    // Load the trigger config everywhere (kill -9 + restart of all three: data is durable).
    let ids = c.node_ids();
    c.restart_many(&ids)
        .await
        .expect("restart with the trigger");
    wait_healthy(&c, &[], HEAL_T, "trigger configured")
        .await
        .expect("healthy");

    // A producer keeps writing to both Kafka partitions.
    let stop = tokio_util_stop();
    let producer = {
        let bs = bs.clone();
        let topic = topic.clone();
        let stop = stop.clone();
        tokio::spawn(async move {
            let kc = kclient(&bs).await;
            let mut n = 0u64;
            while !stop.load(std::sync::atomic::Ordering::SeqCst) {
                for kp in 0..KPARTS {
                    let recs: Vec<Record> =
                        (0..5).map(|i| record(&json!({"n": n, "i": i}))).collect();
                    produce(&kc, &topic, kp, &recs, Compression::None)
                        .await
                        .expect("produce");
                }
                n += 1;
                sleep(Duration::from_millis(100)).await;
            }
            n
        })
    };

    let owner = kafka_owner(NAME, &topic, 0, p);
    let api = any_client(&mut c);
    let deadline = Instant::now() + LIMIT;
    while runs(&api).await.len() < 50 {
        assert!(
            Instant::now() < deadline,
            "no consumption before the handoff"
        );
        sleep(Duration::from_millis(200)).await;
    }

    // 1. Controlled handoff: SIGTERM the owner's leader, then bring it back.
    let l1 = owner_leader(&c, owner, None).await;
    println!("owner partition {owner} (of {p}) led by node {l1}: SIGTERM");
    c.stop_gracefully(l1).await.expect("graceful stop");
    let l2 = owner_leader(&c, owner, Some(l1)).await;
    let before = runs(&any_client(&mut c)).await.len();
    let deadline = Instant::now() + LIMIT;
    while runs(&any_client(&mut c)).await.len() < before + 20 {
        assert!(
            Instant::now() < deadline,
            "consumption did not resume on node {l2}"
        );
        sleep(Duration::from_millis(200)).await;
    }
    c.restart(l1).await.expect("restart");
    wait_healthy(&c, &[], HEAL_T, "after handoff")
        .await
        .expect("healthy");

    // 2. Crash: kill -9 the current owner leader, then bring it back.
    let l3 = owner_leader(&c, owner, None).await;
    println!("owner partition {owner} led by node {l3}: kill -9");
    c.kill9(l3).await.expect("kill");
    let l4 = owner_leader(&c, owner, Some(l3)).await;
    let before = runs(&any_client(&mut c)).await.len();
    let deadline = Instant::now() + LIMIT;
    while runs(&any_client(&mut c)).await.len() < before + 20 {
        assert!(
            Instant::now() < deadline,
            "consumption did not resume on node {l4} after the crash"
        );
        sleep(Duration::from_millis(200)).await;
    }
    c.restart(l3).await.expect("restart");
    wait_healthy(&c, &[], HEAL_T, "after crash")
        .await
        .expect("healthy");

    // Stop producing; everything produced gets exactly one run.
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    let rounds = producer.await.expect("producer");
    let parts: Vec<TopicPartition> = (0..KPARTS)
        .map(|kp| TopicPartition {
            topic: topic.clone(),
            partition: kp,
        })
        .collect();
    let ends: Vec<i64> = kc
        .list_offsets(&parts, OffsetSpec::Latest)
        .await
        .expect("ends")
        .into_iter()
        .map(|(_, o)| o)
        .collect();
    let total: i64 = ends.iter().sum();
    assert_eq!(
        total,
        i64::try_from(rounds).expect("n") * 5 * i64::from(KPARTS)
    );
    let api = any_client(&mut c);
    let deadline = Instant::now() + LIMIT;
    let got = loop {
        let got = runs(&api).await;
        let committed: Vec<Option<i64>> = kc
            .committed(&group, &parts)
            .await
            .expect("committed")
            .into_iter()
            .map(|(_, o)| o)
            .collect();
        if got.len() as i64 >= total
            && committed == ends.iter().copied().map(Some).collect::<Vec<_>>()
        {
            break got;
        }
        assert!(
            Instant::now() < deadline,
            "{} of {total} offsets have runs; committed {committed:?}, ends {ends:?}",
            got.len()
        );
        sleep(Duration::from_millis(300)).await;
    };
    for kp in 0..KPARTS {
        for o in 0..ends[kp as usize] {
            let ids = got.get(&(i64::from(kp), o));
            assert_eq!(
                ids.map(Vec::len),
                Some(1),
                "partition {kp} offset {o}: runs {ids:?}"
            );
        }
    }
    assert_eq!(got.len() as i64, total, "no run without a record");
    println!(
        "kafka_failover: {total} records over {KPARTS} partitions, exactly one run each \
         across a handoff ({l1} -> {l2}) and a kill -9 ({l3} -> {l4})"
    );
    c.stop().await;
}

fn tokio_util_stop() -> std::sync::Arc<std::sync::atomic::AtomicBool> {
    std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false))
}
