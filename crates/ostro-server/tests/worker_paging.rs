//! WORK-02 frame limit end to end (plan 07-12): with `[workers] max_frame_bytes = 64 KiB`
//! a `long-journal` run of 2,000 steps with one 3 MiB step result and one 3 MiB signal
//! input executes on a worker, every bulk message on the worker channel carries at most
//! 64 KiB of payload in either direction, and the journal is identical to the same run
//! executed embedded.
//!
//! Message sizes come from the 07-09 recorder ([`WireStats`]) on a second cluster listener
//! over the server's own registry and join service (joins advertise it). Recorded sizes
//! are payload bytes (append chunks, blob chunks, journal rows + 24 B per inline row /
//! 64 B per blob ref); both sides budget `max_frame_bytes - FRAME_RESERVE` (256 B) for the
//! payload, so the encoded message stays within `max_frame_bytes`: the framing overhead
//! constant is [`FRAME_OVERHEAD`].
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;
#[path = "common/p7_workers.rs"]
mod p7_workers;

use std::sync::Arc;
use std::time::Duration;

use common::{ServerOpts, TestServer, guest_wasm};
use ostro_meta::records::PLACEMENT_WORKERS;
use ostro_meta::{MetaCommand, MetaRecord, MetaResult, QueuePolicy};
use ostro_partition::store::values::RunStatus;
use ostro_server::workers::service::WireDir;
use ostro_types::RunId;
use ostro_types::journal::EventKind;
use p7_workers::{ServerWorkers, journal_shape, wire_listener};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

const LIMIT: Duration = Duration::from_secs(300);
const FRAME: u64 = 64 * 1024;
/// Bytes of a message outside its payload budget (`FRAME_RESERVE` on both sides).
const FRAME_OVERHEAD: usize = 256;
const MIB: u64 = 1024 * 1024;

async fn deploy(s: &TestServer, guest: &str) -> [u8; 32] {
    let wasm = guest_wasm(guest);
    let sha: [u8; 32] = Sha256::digest(&wasm).into();
    let hex: String = sha.iter().map(|b| format!("{b:02x}")).collect();
    let r = s
        .http
        .put(s.url(&format!("/v1/modules/{hex}")))
        .header("content-type", "application/wasm")
        .body(wasm)
        .send()
        .await
        .expect("PUT module");
    assert!(r.status().is_success(), "deploy {guest}: {}", r.status());
    sha
}

async fn workers_queue(s: &TestServer, queue: &str) {
    let record = MetaRecord::QueuePolicy(QueuePolicy {
        queue: queue.into(),
        placement: Some(PLACEMENT_WORKERS),
        ..QueuePolicy::default()
    });
    let meta = s.h().meta();
    let key = record.key();
    let expected_version = meta.view().read(|st| st.get(&key).map(|v| v.version));
    let r = meta
        .propose(MetaCommand::Put {
            key,
            expected_version,
            record,
        })
        .await
        .expect("meta put");
    assert!(matches!(r, MetaResult::Ok { .. }), "{r:?}");
}

async fn run_one(s: &TestServer, sha: &[u8; 32], queue: &str, big_input: &Value) -> RunId {
    let input = json!({ "steps": 2000, "result_bytes": 3 * MIB, "wait_input": true });
    let q = queue.to_owned();
    let run = s
        .start_raw(
            "long-journal",
            1,
            input.to_string().as_bytes(),
            sha,
            move |st| st.queue = q,
        )
        .await;
    // Signal only once the run waits for it. A signal posted earlier is buffered and its
    // SignalPayload lands wherever the post races the run's steps (right after RunStarted
    // when the worker has not picked the run up yet), so the embedded and remote journal
    // shapes would differ by timing alone.
    let deadline = tokio::time::Instant::now() + LIMIT;
    while !s
        .journal(&run)
        .await
        .iter()
        .any(|e| e.body.kind() == EventKind::SignalWaitStarted)
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "{run}: no SignalWaitStarted within {LIMIT:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let (st, v) = s
        .post_json(
            &format!("/v1/runs/{run}/signals/input"),
            &json!({ "payload": big_input }),
        )
        .await;
    assert_eq!(st, reqwest::StatusCode::OK, "signal: {v}");
    run
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn long_journal_runs_within_the_frame_limit() {
    let s = TestServer::start_with(ServerOpts::default(), |c| {
        c.workers.max_frame_bytes = FRAME;
    })
    .await;
    s.wait_ready(Duration::from_secs(20)).await;
    let stop = CancellationToken::new();
    let (_addr, wire) = wire_listener(
        Arc::new(s.cfg.clone()),
        s.h().p7(),
        s.h().meta().view().clone(),
        Arc::clone(s.h().clock()),
        s.cfg.executor.lease_ttl.into(),
        stop.clone(),
    );
    let sha = deploy(&s, "long-journal").await;
    workers_queue(&s, "wq").await;
    let ws = ServerWorkers::new(Arc::clone(s.h().p7())).await;
    let _w = ws.spawn("w1", 1, &[]).await;
    // A 3 MiB JSON string (the signal input).
    let big_input = Value::String("i".repeat(usize::try_from(3 * MIB - 16).expect("len")));
    let a = run_one(&s, &sha, "default", &big_input).await;
    let b = run_one(&s, &sha, "wq", &big_input).await;
    for r in [&a, &b] {
        let rec = s.wait_record(r, LIMIT, |r| r.status.is_terminal()).await;
        assert_eq!(rec.status, RunStatus::Completed, "{rec:?}");
    }
    let ja = s.journal(&a).await;
    let jb = s.journal(&b).await;
    assert!(ja.len() > 2000, "journal of {} events", ja.len());
    assert_eq!(journal_shape(&ja), journal_shape(&jb), "embedded == remote");
    assert_eq!(s.result_bytes(&a).await, s.result_bytes(&b).await);
    let out: Value =
        serde_json::from_slice(&s.result_bytes(&b).await.expect("result")).expect("json");
    assert_eq!(out["steps"], 2000);
    assert_eq!(out["big_len"], 3 * MIB);
    assert!(out["input_len"].as_u64().expect("input_len") >= 3 * MIB - 16);
    // Every recorded bulk message within the frame limit, both directions.
    let records = wire.records();
    let budget = usize::try_from(FRAME).expect("frame") - FRAME_OVERHEAD;
    for r in &records {
        assert!(
            r.bytes <= budget,
            "{:?} {} message of {} payload bytes > {budget} (max_frame_bytes {FRAME} - overhead {FRAME_OVERHEAD})",
            r.dir,
            r.rpc,
            r.bytes
        );
    }
    let count = |dir: WireDir, rpc: &str| {
        records
            .iter()
            .filter(|r| r.dir == dir && r.rpc == rpc)
            .count()
    };
    // The 3 MiB step result went up in >= 48 chunks; pages and blob chunks came down.
    assert!(
        count(WireDir::In, "append") >= 48,
        "append chunks: {}",
        count(WireDir::In, "append")
    );
    assert!(count(WireDir::Out, "fetch_journal") >= 2, "journal pages");
    assert!(count(WireDir::Out, "fetch_blob") >= 48, "blob chunks");
    let max_in = wire.max(WireDir::In, "append");
    let max_out = wire
        .max(WireDir::Out, "fetch_journal")
        .max(wire.max(WireDir::Out, "fetch_blob"));
    eprintln!(
        "07-12 paging: {} records, max append chunk {max_in} B, max page/chunk {max_out} B, overhead constant {FRAME_OVERHEAD} B",
        records.len()
    );
    stop.cancel();
    let mut s = s;
    s.shutdown().await;
}
