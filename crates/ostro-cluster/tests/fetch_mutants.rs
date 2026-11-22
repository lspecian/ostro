//! Mutant bites for the 12-11 sites (D-P12-19): each property holds with the mutant off,
//! is violated with it on, and holds again after `fail::remove`.
//!
//! - `repl::mutant::skip_epoch_check` (fetch.rs): a stale-epoch fetch must be FENCED.
//! - `repl::mutant::no_truncate` (fetcher.rs): a divergent follower must truncate before
//!   its HW covers the divergent records.
//!
//! The follower half of `repl::mutant::count_nondurable_leo` (fetcher.rs) is proven in
//! tier C by 12-17.
#![cfg(feature = "failpoints")]

mod support_fetch;
mod support_meta;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use ostro_cluster::fetch::{FetchConfig, FetchCtx, fetch_decode_limit, handle_fetch};
use ostro_cluster::fetcher::{FetcherConfig, FetcherDriver};
use ostro_cluster::install::InstallCtx;
use ostro_log::mutants::{NO_TRUNCATE, SKIP_EPOCH_CHECK};
use ostro_log::{ReplicaSet, ReplicatedLog};
use ostro_partition::SystemClock;
use ostro_partition::roles::FollowerDriver;
use ostro_proto::cluster::v1::{FetchError, FetchRequest};
use ostro_types::{Epoch, Offset};
use support_fetch::*;

fn set(replicas: &[u64], isr: &[u64], min_insync: u8) -> ReplicaSet {
    ReplicaSet {
        replicas: replicas.to_vec(),
        isr: isr.to_vec(),
        isr_version: 0,
        min_insync,
    }
}

#[tokio::test]
async fn skip_epoch_check_serves_a_stale_epoch_fetch() {
    let fx = LogFx::open(1).await;
    fx.log
        .become_leader(Epoch(2), set(&[1, 2], &[1], 1))
        .unwrap();
    append(&fx.log, Epoch(2), vec![cmd(0)]).await;
    let a = assignments(view(1, 2, &[1, 2], &[1]));
    let ctx = FetchCtx::new(
        Arc::clone(&fx.log),
        a.subscribe(),
        Arc::new(SystemClock),
        FetchConfig::default(),
    );
    let stale = || FetchRequest {
        partition: 0,
        replica_id: 2,
        leader_epoch: 1,
        fetch_offset: 0,
        last_fetched_epoch: 0,
        durable_leo: 0,
        max_bytes: 1 << 20,
        max_wait_ms: 0,
        min_bytes: 1,
    };
    let fenced = |r: &ostro_proto::cluster::v1::FetchResponse| {
        r.error == FetchError::FencedLeaderEpoch as i32 && r.envelopes.is_empty()
    };

    let r = handle_fetch(&ctx, 2, stale()).await.unwrap();
    assert!(fenced(&r), "property holds without the mutant");

    fail::cfg(SKIP_EPOCH_CHECK, "return").unwrap();
    let r = handle_fetch(&ctx, 2, stale()).await.unwrap();
    fail::remove(SKIP_EPOCH_CHECK);
    assert!(!fenced(&r), "the mutant bites: a stale epoch got data");
    assert_eq!(r.envelopes.len(), 1);

    let r = handle_fetch(&ctx, 2, stale()).await.unwrap();
    assert!(fenced(&r), "holds again after fail::remove");
    fx.shutdown().await;
}

/// Run a divergent follower (epoch 1: a, b, x, y never committed) against a leader
/// (epoch 1: a, b; epoch 2: c, d) until the follower adopts the leader's HW; returns
/// whether the follower's records below its HW equal the leader's.
async fn divergent_prefix_matches() -> bool {
    let f = LogFx::open(2).await;
    f.log
        .become_leader(Epoch(1), set(&[2, 9], &[2, 9], 2))
        .unwrap();
    for i in [0u64, 1, 100, 101] {
        append(&f.log, Epoch(1), vec![cmd(i)]).await;
    }
    let l = LogFx::open(1).await;
    l.log
        .become_leader(Epoch(1), set(&[1, 2], &[1], 1))
        .unwrap();
    for i in [0u64, 1] {
        append(&l.log, Epoch(1), vec![cmd(i)]).await;
    }
    l.log
        .become_leader(Epoch(2), set(&[1, 2], &[1], 1))
        .unwrap();
    for i in [2u64, 3] {
        append(&l.log, Epoch(2), vec![cmd(i)]).await;
    }
    f.log.become_follower(Epoch(2), 1).unwrap();

    let ca = test_ca();
    let a = assignments(view(1, 2, &[1, 2], &[1]));
    let ctx = Arc::new(FetchCtx::new(
        Arc::clone(&l.log),
        a.subscribe(),
        Arc::new(SystemClock),
        FetchConfig {
            max_wait: Duration::from_millis(100),
            ..FetchConfig::default()
        },
    ));
    let limit = fetch_decode_limit(ostro_types::envelope::DEFAULT_MAX_ENVELOPE_BYTES, 8 << 20);
    let srv = ReplServer::start(&ca, 1, Some(ctx), Arc::clone(&l.blobs), limit).await;
    let driver = FetcherDriver::new(
        2,
        node_transport(&ca, 2, 1, srv.addr),
        Arc::clone(&f.blobs),
        Arc::new(Suspects::default()),
        Arc::new(InstallCtx::default()),
        FetcherConfig {
            max_wait: Duration::from_millis(100),
            ..FetcherConfig::default()
        },
    );
    let task = driver.start(P, Epoch(2), 1, Arc::clone(&f.log));
    let lhw = *l.log.high_watermark().borrow();
    assert_eq!(lhw, Offset(4));
    let flog = Arc::clone(&f.log);
    wait_until(
        "follower HW reaches the leader HW",
        Duration::from_secs(10),
        || *flog.high_watermark().borrow() == lhw,
    )
    .await;
    task.stop().await;
    let fhw = *f.log.high_watermark().borrow();
    let mut theirs = envelopes(&l.log, Offset(0)).await;
    let mut ours = envelopes(&f.log, Offset(0)).await;
    let n = usize::try_from(fhw.0).unwrap();
    theirs.truncate(n);
    ours.truncate(n);
    let truncations = driver.stats().truncations.load(Ordering::Relaxed);
    srv.stop().await;
    l.shutdown().await;
    f.shutdown().await;
    eprintln!("truncations: {truncations}");
    ours == theirs
}

#[tokio::test]
async fn no_truncate_leaves_divergent_records_below_the_hw() {
    assert!(divergent_prefix_matches().await, "property holds");
    fail::cfg(NO_TRUNCATE, "return").unwrap();
    let bitten = !divergent_prefix_matches().await;
    fail::remove(NO_TRUNCATE);
    assert!(
        bitten,
        "the mutant bites: divergent records below the follower HW"
    );
    assert!(
        divergent_prefix_matches().await,
        "holds again after fail::remove"
    );
}
