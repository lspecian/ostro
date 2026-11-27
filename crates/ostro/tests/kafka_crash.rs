//! 14-32 Task 3 (TRIG-04, D-P14-43): `kill -9` crash test of the Kafka trigger against a real
//! broker. `#[ignore]`:
//!
//! ```text
//! bash scripts/fetch-tools.sh kafka jre && bash scripts/test-services.sh start kafka
//! eval "$(bash scripts/test-services.sh env)"
//! OSTRO_KAFKA_CRASH_ITERS=20 cargo test -p ostro --features failpoints --locked \
//!   --test kafka_crash -- --ignored --test-threads=1 --nocapture
//! ```
//!
//! A producer writes records to a 3-partition topic continuously while a spawned
//! `ostro server` (failpoint build) with one `[[triggers.kafka]]` consumes it. Each of
//! `OSTRO_KAFKA_CRASH_ITERS` iterations (default 20) SIGKILLs the server at a random point;
//! every third iteration runs with `FAILPOINTS=kafka::after_start_before_commit=return(hang)`,
//! so every record whose run start is durable hangs before it counts as done and the kill
//! lands between a durable start and its offset commit. The server restarts on the same data
//! dir. At the end (producer stopped, a clean server, quiet period) it asserts:
//!
//! - every produced offset has exactly one run (runs listed by the `ostro.kafka=<name>` tag,
//!   keyed by their partition/offset tags), and no run exists without a record;
//! - the group's committed offsets equal the high watermarks;
//! - the crash invariants at the trigger level (the Phase 5 checker's "0 lost accepted runs,
//!   0 duplicate journal entries"): every run completed with its record's value as result,
//!   and the sampled histories have unique, increasing sequence numbers with exactly one
//!   `run_started` and one `run_completed` event.
//!
//! Without `OSTRO_TEST_KAFKA_BOOTSTRAP` it prints `SKIP:` and returns.
#![cfg(feature = "failpoints")]
#![cfg(unix)]

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use common::{Api, Spawned, events_of, free_addr, guest_wasm, server};
use ostro_kafka::testkit::{Compression, create_topic, produce};
use ostro_kafka::{Client, ClientConfig, OffsetSpec, Record, TopicPartition};
use serde_json::{Value, json};

const NAME: &str = "crash";
const KPARTS: i32 = 3;
const FAILPOINT: &str = "kafka::after_start_before_commit=return(hang)";
const READY_T: Duration = Duration::from_secs(120);
const FINISH_T: Duration = Duration::from_secs(300);
/// Histories checked in full (a sample when there are more runs).
const HISTORY_SAMPLE: usize = 300;

/// xorshift64* (no rand dev-dependency in this crate).
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.next() % (hi - lo)
    }
}

async fn kclient(bs: &str) -> Client {
    Client::connect(ClientConfig {
        bootstrap: vec![bs.to_owned()],
        client_id: "kafka-crash-test".into(),
        ..ClientConfig::default()
    })
    .await
    .expect("connect to the test broker")
}

fn write_config(dir: &Path, addr: SocketAddr, bs: &str, topic: &str, group: &str) -> String {
    let path = dir.join("node.toml");
    let text = format!(
        r#"[node]
data_dir = "{data}"
partitions = 4

[api]
listen = "{addr}"

[log]
level = "info"

[[triggers.kafka]]
name = "{NAME}"
bootstrap = ["{bs}"]
topics = ["{topic}"]
group_id = "{group}"
workflow = "smoke"
start_from = "committed_or_earliest"
max_in_flight = 64
commit_interval = "250ms"
"#,
        data = dir.join("data").display()
    );
    std::fs::write(&path, text).expect("write node.toml");
    path.display().to_string()
}

fn spawn(dir: &Path, cfg: &str, failpoints: bool) -> Spawned {
    let mut cmd = server(dir, &["-c", cfg]);
    if failpoints {
        cmd.env("FAILPOINTS", FAILPOINT);
    } else {
        cmd.env_remove("FAILPOINTS");
    }
    Spawned::spawn(cmd)
}

/// `(partition, offset) -> run ids` of the trigger's runs.
async fn runs(api: &Api) -> BTreeMap<(i64, i64), Vec<String>> {
    let mut out: BTreeMap<(i64, i64), Vec<String>> = BTreeMap::new();
    let mut token: Option<String> = None;
    loop {
        let mut path = format!("/v1/runs?tag=ostro.kafka%3D{NAME}&limit=1000");
        if let Some(t) = &token {
            path.push_str(&format!("&page_token={t}"));
        }
        let (st, v) = api.get(&path).await;
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

async fn committed(kc: &Client, group: &str, parts: &[TopicPartition]) -> Vec<Option<i64>> {
    kc.committed(group, parts)
        .await
        .expect("committed")
        .into_iter()
        .map(|(_, o)| o)
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "kill -9 crash test against a Kafka broker (scripts/test-services.sh start kafka)"]
async fn kafka_crash_exactly_one_run_per_offset() {
    let bs = match std::env::var("OSTRO_TEST_KAFKA_BOOTSTRAP") {
        Ok(b) if !b.is_empty() => b,
        _ => {
            println!(
                "SKIP: OSTRO_TEST_KAFKA_BOOTSTRAP is unset (scripts/test-services.sh start kafka)"
            );
            return;
        }
    };
    let iters: usize = std::env::var("OSTRO_KAFKA_CRASH_ITERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20);
    let seed: u64 = std::env::var("OSTRO_KAFKA_CRASH_SEED")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(1, |d| d.as_nanos() as u64)
                | 1
        });
    println!("kafka_crash: {iters} iterations, seed {seed} (OSTRO_KAFKA_CRASH_SEED)");
    let mut rng = Rng(seed);

    let kc = kclient(&bs).await;
    let topic = format!("kc-{}-{}", std::process::id(), seed % 1_000_000);
    let group = format!("g-{topic}");
    create_topic(&kc, &topic, KPARTS)
        .await
        .expect("create topic");
    let parts: Vec<TopicPartition> = (0..KPARTS)
        .map(|p| TopicPartition {
            topic: topic.clone(),
            partition: p,
        })
        .collect();

    let tmp = tempfile::tempdir().expect("tmp");
    let addr = free_addr();
    let cfg = write_config(tmp.path(), addr, &bs, &topic, &group);
    let api = Api::new(addr);

    // Producer: 3 records per partition every 250 ms; the value records (partition, seq).
    let stop = Arc::new(AtomicBool::new(false));
    let producer = {
        let (bs, topic, stop) = (bs.clone(), topic.clone(), Arc::clone(&stop));
        tokio::spawn(async move {
            let kc = kclient(&bs).await;
            let mut values: BTreeMap<(i64, i64), Value> = BTreeMap::new();
            let mut seq = 0u64;
            while !stop.load(Ordering::SeqCst) {
                for p in 0..KPARTS {
                    let vals: Vec<Value> = (0..3)
                        .map(|i| json!({"p": p, "seq": seq, "i": i}))
                        .collect();
                    let recs: Vec<Record> = vals
                        .iter()
                        .map(|v| Record {
                            value: Some(serde_json::to_vec(v).expect("json").into()),
                            ..Record::default()
                        })
                        .collect();
                    let base = produce(&kc, &topic, p, &recs, Compression::None)
                        .await
                        .expect("produce");
                    for (k, v) in vals.into_iter().enumerate() {
                        values.insert((i64::from(p), base + k as i64), v);
                    }
                }
                seq += 1;
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            values
        })
    };

    let mut hung_kills = 0usize;
    for it in 0..iters {
        let fp = it % 3 == 2;
        let mut s = spawn(tmp.path(), &cfg, fp);
        tokio::task::block_in_place(|| s.ready_line(READY_T));
        s.wait_readyz(&api.http, addr).await;
        if it == 0 {
            let (st, body) = api.upload(&guest_wasm("smoke")).await;
            assert!(st.is_success(), "deploy smoke: {st} {body}");
        }
        let before = committed(&kc, &group, &parts).await;
        let wait = if fp {
            Duration::from_millis(rng.range(1500, 3000))
        } else {
            Duration::from_millis(rng.range(200, 4000))
        };
        tokio::time::sleep(wait).await;
        if fp {
            // The failpoint holds every durable start before its commit: runs exist past the
            // committed offsets, which have not moved since the start.
            let after = committed(&kc, &group, &parts).await;
            assert_eq!(before, after, "nothing is committed while starts are held");
            let past: usize = runs(&api)
                .await
                .keys()
                .filter(|(p, o)| *o >= after[*p as usize].unwrap_or(0))
                .count();
            assert!(
                past > 0,
                "durable starts past the committed offsets {after:?}"
            );
            println!("iteration {it}: {past} durable starts past the committed offsets");
            hung_kills += 1;
        }
        s.signal("KILL");
        let st = tokio::task::block_in_place(|| s.wait_exit(Duration::from_secs(30)));
        assert!(!st.success(), "killed: {st:?}");
        println!(
            "iteration {it}: kill after {wait:?}{}",
            if fp {
                " (held at kafka::after_start_before_commit)"
            } else {
                ""
            }
        );
    }

    // Stop producing; a clean server drains everything.
    stop.store(true, Ordering::SeqCst);
    let values = producer.await.expect("producer");
    let mut s = spawn(tmp.path(), &cfg, false);
    tokio::task::block_in_place(|| s.ready_line(READY_T));
    s.wait_readyz(&api.http, addr).await;
    let ends: Vec<i64> = kc
        .list_offsets(&parts, OffsetSpec::Latest)
        .await
        .expect("ends")
        .into_iter()
        .map(|(_, o)| o)
        .collect();
    let total: usize = values.len();
    assert_eq!(
        ends.iter().sum::<i64>(),
        i64::try_from(total).expect("n"),
        "every produced record is in the log"
    );
    let want_commit: Vec<Option<i64>> = ends.iter().copied().map(Some).collect();
    let t0 = Instant::now();
    loop {
        let got = runs(&api).await.len();
        let c = committed(&kc, &group, &parts).await;
        if got >= total && c == want_commit {
            break;
        }
        assert!(
            t0.elapsed() < FINISH_T,
            "{got} of {total} offsets have runs; committed {c:?}, ends {ends:?}; stderr: {}",
            s.stderr()
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    // Quiet period: nothing changes.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let got = runs(&api).await;
    assert_eq!(committed(&kc, &group, &parts).await, want_commit);
    let keys: BTreeSet<(i64, i64)> = got.keys().copied().collect();
    let want: BTreeSet<(i64, i64)> = values.keys().copied().collect();
    assert_eq!(keys, want, "runs exist for exactly the produced offsets");
    let dups: Vec<_> = got.iter().filter(|(_, ids)| ids.len() != 1).collect();
    assert!(dups.is_empty(), "offsets with more than one run: {dups:?}");

    // Trigger-level crash invariants: every run completes with its record's value; sampled
    // histories have one start, one completion and unique increasing sequence numbers.
    let mut checked = 0usize;
    let step = (total / HISTORY_SAMPLE).max(1);
    for (i, (k, ids)) in got.iter().enumerate() {
        let id = &ids[0];
        let run = api.wait_terminal(id, Duration::from_secs(120)).await;
        assert_eq!(run["status"], "completed", "run {id} ({k:?}): {run}");
        if i % step == 0 {
            assert_eq!(run["result"], values[k], "run {id} result = record value");
            let h = api.history(id).await;
            let seqs: Vec<u64> = h.iter().filter_map(|e| e["seq"].as_u64()).collect();
            assert!(
                seqs.windows(2).all(|w| w[0] < w[1]),
                "run {id}: journal sequence not strictly increasing: {seqs:?}"
            );
            assert_eq!(events_of(&h, "run_started").len(), 1, "run {id}");
            assert_eq!(events_of(&h, "run_completed").len(), 1, "run {id}");
            checked += 1;
        }
    }
    s.stop_clean("TERM");
    println!(
        "kafka_crash: {iters} kills ({hung_kills} between a durable start and its commit), \
         {total} records over {KPARTS} partitions, {} runs = exactly one per offset, \
         0 lost, 0 duplicate; {checked} histories checked",
        got.len()
    );
}
