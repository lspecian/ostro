//! 12-05 Task 3: the metadata group over the real mTLS cluster transport.
//!
//! - three `MetaHandle`s on loopback elect a leader, forward a follower's proposal through
//!   `MetaPeer.Propose`, survive the leader's stop and converge after its restart;
//! - `MetaPeer` authorizes by node identity only: worker certificates, other clusters'
//!   node certificates, non-members and spoofed batch senders are refused;
//! - a single node with zero peers serves `Propose` locally.

mod support_meta;

use std::sync::Arc;
use std::time::{Duration, Instant};

use ostro_cluster::meta_peer::{ERROR_NOT_LEADER, GrpcForwarder};
use ostro_cluster::transport::ClusterTransport;
use ostro_meta::handle::ProposeForwarder;
use ostro_meta::{
    MetaCommand, MetaKey, MetaRecord, MetaResult, QueueConfig, decode_result, encode_command,
    encode_snapshot,
};
use ostro_proto::cluster::v1::meta_peer_client::MetaPeerClient;
use ostro_proto::cluster::v1::{ProposeRequest, RaftBatch, SuspectReport};
use support_meta::{CLUSTER, MetaCluster, TestCa, client_transport};
use tonic::Code;

fn queue(name: &str, at: u64) -> MetaCommand {
    MetaCommand::Put {
        key: MetaKey::Queue(name.into()),
        expected_version: None,
        record: MetaRecord::Queue(QueueConfig {
            name: name.into(),
            max_concurrency: Some(2),
            created_at_ms: at,
            ext: Vec::new(),
        }),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_voters_over_mtls_elect_forward_fail_over_and_restart() {
    let mut c = MetaCluster::start(&[1, 2, 3]).await;
    let t0 = Instant::now();
    let leader = c.leader(Duration::from_secs(30)).await;
    eprintln!("initial election: leader {leader} after {:?}", t0.elapsed());

    // A follower's proposals are forwarded over MetaPeer.Propose, read-your-writes.
    let f = c.follower(leader);
    let info = c
        .handle(f)
        .init_cluster(CLUSTER, 1, 4, 10, [1; 32])
        .await
        .expect("init via follower");
    assert_eq!(info.partitions, 4);
    let r = c.handle(f).propose(queue("q1", 11)).await.expect("put");
    assert_eq!(r, MetaResult::Ok { version: 1 });
    assert!(c.handle(f).view().read(|s| s.queue("q1").is_some()));

    // Stop the leader: a new one within 5 s, proposals continue.
    c.stop(leader).await;
    let t1 = Instant::now();
    let new_leader = c.leader(Duration::from_secs(5)).await;
    eprintln!(
        "failover: {leader} -> {new_leader} after {:?}",
        t1.elapsed()
    );
    assert_ne!(new_leader, leader);
    let other = c.follower(new_leader);
    let r = c.handle(other).propose(queue("q2", 12)).await.expect("put");
    assert_eq!(r, MetaResult::Ok { version: 1 });
    assert!(c.handle(other).view().read(|s| s.queue("q2").is_some()));

    // Restart the old leader on its address: it rejoins and converges.
    c.restart(leader).await;
    c.handle(leader).wait_ready().await.expect("ready");
    let r = c
        .handle(leader)
        .propose(queue("q3", 13))
        .await
        .expect("put");
    assert_eq!(r, MetaResult::Ok { version: 1 });
    c.settle().await;
    let snap = c.handle(new_leader).view().read(encode_snapshot);
    for id in [1, 2, 3] {
        assert_eq!(
            c.handle(id).view().read(encode_snapshot),
            snap,
            "node {id} state differs"
        );
    }
    c.stop_all().await;
}

/// Calls `Propose` and `Raft` on `target` presenting `tls`; returns both status codes.
async fn probe(
    tls: ostro_cluster::transport::tcp::NodeTls,
    target: u64,
    addr: std::net::SocketAddr,
    claimed_from: u64,
) -> (Result<String, Code>, Result<(), Code>) {
    let t = client_transport(CLUSTER, tls, target, addr);
    let mut client = MetaPeerClient::new(t.channel(target).unwrap());
    let cmd = encode_command(&queue("probe", 1));
    let propose = client
        .propose(ProposeRequest { command: cmd })
        .await
        .map(|r| r.into_inner().error_kind)
        .map_err(|s| s.code());
    let batch = RaftBatch {
        from: claimed_from,
        messages: Vec::new(),
    };
    let raft = client
        .raft(tokio_stream::iter(vec![batch]))
        .await
        .map(|_| ())
        .map_err(|s| s.code());
    (propose, raft)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn meta_peer_authorizes_by_node_identity_only() {
    let mut c = MetaCluster::start(&[1, 2, 3]).await;
    let leader = c.leader(Duration::from_secs(30)).await;
    let target = c.follower(leader);
    let addr = c.addrs[&target];
    let denied = |code: &Code| matches!(code, Code::PermissionDenied | Code::Unauthenticated);

    // A worker certificate of this cluster's CA.
    let (p, r) = probe(c.ca.worker("tok1", "w1"), target, addr, 2).await;
    eprintln!("worker cert: propose {p:?}, raft {r:?}");
    assert!(denied(&p.clone().unwrap_err()), "{p:?}");
    assert!(denied(&r.unwrap_err()), "{r:?}");

    // A node certificate naming another cluster (same CA).
    let (p, r) = probe(c.ca.node_of("other-cluster", 2), target, addr, 2).await;
    eprintln!("other cluster id: propose {p:?}, raft {r:?}");
    assert!(denied(&p.clone().unwrap_err()), "{p:?}");
    assert!(denied(&r.unwrap_err()), "{r:?}");

    // A node of this cluster that is not a metadata voter.
    let (p, r) = probe(c.ca.node(9), target, addr, 9).await;
    eprintln!("non-member node: propose {p:?}, raft {r:?}");
    assert_eq!(p, Err(Code::PermissionDenied));
    assert_eq!(r, Err(Code::PermissionDenied));

    // A voter claiming to send another voter's raft batch.
    let spoofer = if target == 2 { 3 } else { 2 };
    let other = c
        .voters
        .iter()
        .copied()
        .find(|&v| v != target && v != spoofer)
        .unwrap();
    let (p, r) = probe(c.ca.node(spoofer), target, addr, other).await;
    eprintln!("spoofed batch: propose {p:?}, raft {r:?}");
    assert_eq!(
        p,
        Ok(ERROR_NOT_LEADER.to_owned()),
        "a voter may propose (follower answers not_leader)"
    );
    assert_eq!(r, Err(Code::PermissionDenied));

    // A certificate from another cluster's CA fails the TLS handshake.
    let foreign = TestCa::new("other-cluster");
    let (p, r) = probe(foreign.node(2), target, addr, 2).await;
    eprintln!("foreign CA: propose {p:?}, raft {r:?}");
    assert!(p.is_err() && r.is_err());

    // ReportSuspect: the reporter must be the authenticated node.
    let t = client_transport(CLUSTER, c.ca.node(spoofer), target, addr);
    let mut client = MetaPeerClient::new(t.channel(target).unwrap());
    let ok = client
        .report_suspect(SuspectReport {
            reporter: spoofer,
            suspect: leader,
            partition: Some(3),
            reason: "test".into(),
        })
        .await;
    assert!(ok.is_ok(), "{ok:?}");
    let spoofed = client
        .report_suspect(SuspectReport {
            reporter: other,
            suspect: leader,
            partition: None,
            reason: "test".into(),
        })
        .await;
    assert_eq!(spoofed.unwrap_err().code(), Code::PermissionDenied);
    let t = client_transport(CLUSTER, c.ca.worker("tok1", "w1"), target, addr);
    let mut client = MetaPeerClient::new(t.channel(target).unwrap());
    let worker = client
        .report_suspect(SuspectReport {
            reporter: spoofer,
            suspect: leader,
            partition: None,
            reason: "test".into(),
        })
        .await;
    assert_eq!(worker.unwrap_err().code(), Code::PermissionDenied);

    c.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_node_with_zero_peers_serves_propose_locally() {
    let mut c = MetaCluster::start(&[1]).await;
    assert!(c.handle(1).is_leader(), "a lone voter leads on start");
    let r = c
        .handle(1)
        .propose(queue("solo", 1))
        .await
        .expect("local put");
    assert_eq!(r, MetaResult::Ok { version: 1 });

    // The MetaPeer service is live: Propose over mTLS runs on the lone leader.
    let t = client_transport(CLUSTER, c.ca.node(1), 1, c.addrs[&1]);
    let fwd = GrpcForwarder::new(t as Arc<dyn ClusterTransport>);
    let applied = fwd
        .forward(1, encode_command(&queue("solo2", 2)))
        .await
        .expect("propose over MetaPeer");
    assert_eq!(
        decode_result(&applied.result).unwrap(),
        MetaResult::Ok { version: 1 }
    );
    c.handle(1)
        .raft()
        .wait_applied(applied.index)
        .await
        .unwrap();
    assert!(c.handle(1).view().read(|s| s.queue("solo2").is_some()));
    c.stop_all().await;
}
