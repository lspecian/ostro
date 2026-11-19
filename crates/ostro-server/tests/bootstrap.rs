//! Server bootstrap (plan 04-17, META-01/META-03/CLI-01): readiness, health on one port,
//! restart identity and epochs, partition-count mismatch, ephemeral data dirs, stale
//! partition-leader rejection, startup timings.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::collections::BTreeMap;
use std::time::Duration;

use common::{ServerOpts, TestServer, server_config, tmpfs_dir};
use ostro_meta::{MetaCommand, MetaResult};
use ostro_proto::health::v1::{CheckRequest, health_client::HealthClient};
use ostro_server::{Server, ServerError};

fn epochs(s: &TestServer) -> BTreeMap<u16, u32> {
    s.h().meta().view().read(|m| {
        m.partitions()
            .map(|a| (a.value.partition, a.value.epoch))
            .collect()
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn boots_ready_with_health_on_one_port() {
    let s = TestServer::start(ServerOpts::default()).await;
    let took = s.wait_ready(Duration::from_secs(2)).await;
    println!("readyz 200 after {took:?}; timings {:?}", s.h().timings());
    assert!(s.h().ready());

    let r = s.http.get(s.url("/healthz")).send().await.expect("healthz");
    assert_eq!(r.status(), reqwest::StatusCode::OK);

    let mut health = HealthClient::new(s.channel().await);
    let st = health
        .check(CheckRequest::default())
        .await
        .expect("grpc health")
        .into_inner()
        .status;
    assert_eq!(st, "SERVING");

    // Every partition's term is recorded in meta, led by this node.
    let e = epochs(&s);
    assert_eq!(e.len(), 4);
    for (p, epoch) in &e {
        assert!(*epoch >= 1, "partition {p} epoch {epoch}");
        let h = s
            .h()
            .node()
            .handle(ostro_types::PartitionId(*p))
            .expect("ready");
        assert_eq!(h.epoch().0, *epoch);
    }
    let t = s.h().timings();
    assert!(t.total >= t.node_open && t.total >= t.meta_leader);
    assert!(t.total > Duration::ZERO);

    let mut s = s;
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_keeps_identity_and_increases_epochs() {
    let mut s = TestServer::start(ServerOpts::default()).await;
    s.wait_ready(Duration::from_secs(2)).await;
    let cluster = s.h().cluster_id().to_string();
    let key = s.h().token_key();
    assert_eq!(s.h().meta().view().read(|m| m.token_key()), Some(key));
    let before = epochs(&s);

    s.restart().await;
    s.wait_ready(Duration::from_secs(2)).await;
    assert_eq!(s.h().cluster_id(), cluster, "cluster id survives restart");
    assert_eq!(s.h().token_key(), key, "token key survives restart");
    let after = epochs(&s);
    assert_eq!(before.len(), after.len());
    for (p, e) in &before {
        assert!(after[p] > *e, "partition {p}: epoch {e} -> {}", after[p]);
    }
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partition_count_mismatch_is_fatal_and_names_both() {
    let dir = tmpfs_dir();
    let mut s =
        TestServer::start_cfg(server_config(dir.path(), &ServerOpts::default()), None).await;
    s.shutdown().await;

    let opts = ServerOpts {
        partitions: 8,
        ..ServerOpts::default()
    };
    let err = Server::start(server_config(dir.path(), &opts))
        .await
        .expect_err("P mismatch must fail");
    let msg = err.to_string();
    assert!(
        matches!(
            err,
            ServerError::PartitionCountMismatch {
                stored: 4,
                configured: 8
            }
        ),
        "{msg}"
    );
    assert!(msg.contains('4') && msg.contains('8'), "{msg}");

    // Same P again starts fine (InitCluster repeat is accepted).
    let mut s =
        TestServer::start_cfg(server_config(dir.path(), &ServerOpts::default()), None).await;
    s.wait_ready(Duration::from_secs(2)).await;
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ephemeral_dir_on_tmpfs_removed_on_shutdown() {
    let unused = tmpfs_dir();
    let opts = ServerOpts {
        ephemeral: true,
        ..ServerOpts::default()
    };
    let mut s = TestServer::start_cfg(server_config(unused.path(), &opts), None).await;
    s.wait_ready(Duration::from_secs(2)).await;
    let dir = s.h().data_dir().to_path_buf();
    assert!(s.h().is_ephemeral());
    assert!(dir.is_dir(), "{} exists while running", dir.display());
    assert!(dir.join("meta").is_dir());
    assert_ne!(dir, unused.path(), "ephemeral ignores node.data_dir");
    let shm = std::path::Path::new("/dev/shm");
    if shm.is_dir() && tempfile::tempdir_in(shm).is_ok() {
        assert!(dir.starts_with(shm), "{} not on /dev/shm", dir.display());
    } else {
        assert!(dir.starts_with(std::env::temp_dir()));
    }
    // The configured directory is untouched.
    assert_eq!(std::fs::read_dir(unused.path()).expect("ls").count(), 0);

    s.shutdown().await;
    assert!(!dir.exists(), "{} removed after shutdown", dir.display());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_partition_leader_is_rejected() {
    let mut s = TestServer::start(ServerOpts::default()).await;
    s.wait_ready(Duration::from_secs(2)).await;
    s.restart().await; // epochs >= 2
    let before = epochs(&s);
    let p0 = before[&0];
    assert!(p0 >= 2);

    let res = s
        .h()
        .meta()
        .propose(MetaCommand::SetPartitionLeader {
            partition: 0,
            epoch: p0 - 1,
            leader: 1,
            isr: vec![1],
        })
        .await
        .expect("propose");
    assert_eq!(
        res,
        MetaResult::StaleVersion {
            current: u64::from(p0)
        }
    );
    assert_eq!(epochs(&s), before, "meta keeps the higher epoch");
    s.shutdown().await;
}
