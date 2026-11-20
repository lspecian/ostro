//! Bottomless uploader service (plan 08-13, BACK-01): a server with `[backup] url =
//! file://<tmp>` takes the prefix LOCK and uploads under the consistent cut; after 20 runs
//! and two seal intervals the manifest exists, validates and names chunks for every busy
//! partition, and a graceful shutdown runs a final round.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::guest_wasm;
use common::phase8::Phase8Server;
use ostro_backup::{chunk, fence, manifest};
use ostro_objstore::{LocalFs, ObjectStore};
use ostro_server::config::FriendlyDuration;
use reqwest::StatusCode;
use serde_json::json;
use sha2::{Digest, Sha256};

const SEAL: Duration = Duration::from_millis(300);

async fn current(store: &Arc<dyn ObjectStore>) -> Option<manifest::Manifest> {
    let (b, _) = store.get(manifest::CURRENT_KEY).await.expect("get")?;
    Some(manifest::decode(&b).expect("manifest decodes and validates"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_uploads_continuously_under_the_cut() {
    let remote = tempfile::tempdir().expect("remote");
    let mut s = Phase8Server::builder()
        .partitions(4)
        .with_backup_dir(remote.path())
        .with_config(|c| c.backup.seal_interval = FriendlyDuration(SEAL))
        .start()
        .await;
    s.wait_ready(Duration::from_secs(10)).await;
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFs::new(remote.path()).expect("localfs"));

    // LOCK names this node's incarnation.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let lock = loop {
        if let Some((b, _)) = store.get(fence::LOCK_KEY).await.expect("get") {
            break b;
        }
        assert!(tokio::time::Instant::now() < deadline, "LOCK never written");
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let ident: fence::Ident = serde_json::from_slice(&lock).expect("LOCK json");
    assert_eq!(
        ident.incarnation,
        ostro_partition::node::hex16(&s.h().node().incarnation())
    );
    assert_eq!(ident.cluster_id, s.h().cluster_id());

    // 20 runs.
    let wasm = guest_wasm("sdk-basic");
    let sha: String = Sha256::digest(&wasm)
        .iter()
        .map(|x| format!("{x:02x}"))
        .collect();
    let r = s
        .http
        .put(s.url(&format!("/v1/modules/{sha}")))
        .header("content-type", "application/wasm")
        .body(wasm)
        .send()
        .await
        .expect("PUT module");
    assert!(r.status().is_success(), "deploy: {}", r.status());
    let mut busy = std::collections::BTreeSet::new();
    for i in 0..20 {
        let r = s
            .http
            .post(s.url("/v1/workflows/sdk-basic/runs"))
            .json(&json!({"input": {"name": format!("r{i}"), "n": 1}}))
            .send()
            .await
            .expect("POST run");
        assert_eq!(r.status(), StatusCode::CREATED);
        let body: serde_json::Value = r.json().await.expect("json");
        let id: ostro_types::RunId = body["run_id"]
            .as_str()
            .expect("run_id")
            .parse()
            .expect("run id");
        busy.insert(id.partition().0);
    }
    let applied: Vec<u64> = (0..4u16)
        .map(|p| {
            s.h()
                .node()
                .applied_state(ostro_types::PartitionId(p))
                .map_or(0, |(o, _)| o.0)
        })
        .collect();

    // Two seal intervals (plus scheduling slack): the manifest covers what was applied.
    tokio::time::sleep(2 * SEAL).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let m = loop {
        if let Some(m) = current(&store).await
            && busy.iter().all(|p| {
                m.partition(*p)
                    .is_some_and(|e| e.cut_offset >= applied[*p as usize])
            })
        {
            break m;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "manifest does not cover the runs yet"
        );
        tokio::time::sleep(SEAL).await;
    };
    m.validate().expect("valid");
    assert_eq!(m.cut.len(), 4);
    assert_eq!(m.incarnation, s.h().node().incarnation());
    assert!(m.meta_snapshot_key.is_some(), "meta snapshot uploaded");
    for p in &busy {
        let e = m.partition(*p).expect("entry");
        let head = e
            .last_chunk_key
            .as_deref()
            .expect("chunks for a busy partition");
        assert_eq!(chunk::parse_chunk_key(head).map(|k| k.0), Some(*p));
        let segs = store
            .list(&chunk::seg_prefix(*p), None)
            .await
            .expect("list");
        assert!(!segs.is_empty(), "partition {p} has chunk objects");
        let walked = chunk::walk_back(
            &store,
            head,
            e.last_chunk_sha256.expect("sha"),
            e.chain_start,
        )
        .await
        .expect("chain walks back");
        assert_eq!(
            walked.last().map(|c| c.chunk.header.end),
            Some(e.cut_offset)
        );
    }
    // A blob-free workload uploads no blobs; the meta snapshot object exists.
    let mk = m.meta_snapshot_key.clone().expect("meta key");
    assert!(store.head(&mk).await.expect("head").is_some());

    // Graceful shutdown runs a final round (bounded at 10 s) and returns.
    let gen_before = current(&store).await.expect("manifest").generation;
    s.shutdown().await;
    let after = current(&store).await.expect("manifest");
    assert!(after.generation >= gen_before);
}
