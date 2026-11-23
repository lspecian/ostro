//! 12-03 Task 1: the test PKI and the in-memory cluster transport with its seeded fault
//! layer. Real tonic/prost traffic over `tokio::io::duplex`; `ostro.health.v1` and a dummy
//! `ostro.cluster.v1.MetaPeer` stand in for the cluster services.

use std::sync::Arc;
use std::time::{Duration, Instant};

use ostro_cluster::transport::{ClusterIncoming, ClusterTransport, PeerNode};
use ostro_proto::cluster::v1 as cpb;
use ostro_proto::cluster::v1::meta_peer_client::MetaPeerClient;
use ostro_proto::cluster::v1::meta_peer_server::{MetaPeer, MetaPeerServer};
use ostro_proto::health::v1 as hpb;
use ostro_proto::health::v1::health_client::HealthClient;
use ostro_proto::health::v1::health_server::{Health, HealthServer};
use ostro_repl_conformance::harness::certs::TestPki;
use ostro_repl_conformance::harness::fault::{Fault, MemNet, RpcKind, serve_memory};
use ostro_server::tls::{NodeIdentity, WorkerIdentity};
use tonic::transport::Channel;
use tonic::{Code, Request, Response, Status};

/// Health answers with the caller identity the transport injected.
struct Echo;

#[tonic::async_trait]
impl Health for Echo {
    async fn check(
        &self,
        req: Request<hpb::CheckRequest>,
    ) -> Result<Response<hpb::CheckResponse>, Status> {
        let peer = req
            .extensions()
            .get::<PeerNode>()
            .map_or("none".to_owned(), |p| p.0.to_string());
        Ok(Response::new(hpb::CheckResponse {
            status: format!("ok from {peer}"),
        }))
    }
}

struct Meta;

#[tonic::async_trait]
impl MetaPeer for Meta {
    async fn raft(
        &self,
        req: Request<tonic::Streaming<cpb::RaftBatch>>,
    ) -> Result<Response<cpb::RaftAck>, Status> {
        let mut s = req.into_inner();
        while s.message().await?.is_some() {}
        Ok(Response::new(cpb::RaftAck {}))
    }

    async fn propose(
        &self,
        req: Request<cpb::ProposeRequest>,
    ) -> Result<Response<cpb::ProposeResponse>, Status> {
        Ok(Response::new(cpb::ProposeResponse {
            index: req.into_inner().command.len() as u64,
            ..Default::default()
        }))
    }

    async fn report_suspect(
        &self,
        _req: Request<cpb::SuspectReport>,
    ) -> Result<Response<cpb::SuspectAck>, Status> {
        Ok(Response::new(cpb::SuspectAck {}))
    }
}

/// A net with nodes 1 and 2; node 2 serves Health + MetaPeer. Returns node 1's channel to 2.
fn net(seed: u64) -> (Arc<MemNet>, Channel) {
    let net = MemNet::new(seed);
    let t1 = net.transport(1);
    let t2 = net.transport(2);
    let ClusterIncoming::Memory(rx) = t2.incoming() else {
        panic!("memory listener expected");
    };
    let routes =
        tonic::service::Routes::new(HealthServer::new(Echo)).add_service(MetaPeerServer::new(Meta));
    tokio::spawn(serve_memory(rx, routes));
    let ch = t1.channel(2).expect("channel");
    (net, ch)
}

/// Every call carries a deadline, as every production cluster RPC does (`set_timeout` /
/// `tokio::time::timeout` in ostro-cluster). Without one a call can hang forever: when a
/// cut lands while the call is being dispatched, tonic's `Reconnect` still sees the dying
/// hyper connection as ready and the request is parked in that connection's dispatch queue,
/// released (`Cancelled "connection closed"`) only when the channel's next call drops the
/// old connection. ci-fix6 reproduced it in about 1 of 5000 isolate cycles under load; the
/// linux-aarch64-musl job hung 2h50m on `isolate_cuts_both_directions` that way.
const CALL_DEADLINE: Duration = Duration::from_secs(10);
/// The deadline of calls over a cut link: well inside the "fails fast" bound below.
const CUT_DEADLINE: Duration = Duration::from_secs(1);
/// Hang guard per test (CI runs `cargo test`, which has no per-test timeout).
const TEST_LIMIT: Duration = Duration::from_secs(60);

async fn guarded<F: std::future::Future>(f: F) -> F::Output {
    tokio::time::timeout(TEST_LIMIT, f) // clock-exempt: test hang guard
        .await
        .expect("test exceeded its 60 s hang guard")
}

fn req<T>(msg: T, deadline: Duration) -> Request<T> {
    let mut r = Request::new(msg);
    r.set_timeout(deadline);
    r
}

async fn check(ch: &Channel) -> Result<String, Status> {
    check_within(ch, CALL_DEADLINE).await
}

async fn check_within(ch: &Channel, deadline: Duration) -> Result<String, Status> {
    HealthClient::new(ch.clone())
        .check(req(hpb::CheckRequest {}, deadline))
        .await
        .map(|r| r.into_inner().status)
}

async fn raft(ch: &Channel) -> Result<(), Status> {
    let batches = futures_util::stream::iter(vec![cpb::RaftBatch {
        from: 1,
        messages: vec![vec![1, 2, 3]],
    }]);
    MetaPeerClient::new(ch.clone())
        .raft(req(batches, CALL_DEADLINE))
        .await
        .map(|_| ())
}

async fn propose(ch: &Channel) -> Result<u64, Status> {
    MetaPeerClient::new(ch.clone())
        .propose(req(
            cpb::ProposeRequest {
                command: vec![0; 5],
            },
            CALL_DEADLINE,
        ))
        .await
        .map(|r| r.into_inner().index)
}

#[test]
fn pki_node_and_worker_identities() {
    let pki = TestPki::new("conf-7").expect("pki");
    let node = pki
        .node_cert(3, &["127.0.0.3".parse().expect("ip")])
        .expect("node cert");
    let id = NodeIdentity::from_der(&node.cert_der).expect("node identity");
    assert_eq!((id.cluster_id.as_str(), id.node_id), ("conf-7", 3));
    assert!(node.cert_pem.contains("BEGIN CERTIFICATE"));
    assert!(node.key_pem.contains("PRIVATE KEY"));

    let worker = pki.worker_cert("tok1", "w1").expect("worker cert");
    assert!(
        NodeIdentity::from_der(&worker.cert_der).is_err(),
        "a worker certificate is not a node identity"
    );
    let w = WorkerIdentity::from_der(&worker.cert_der).expect("worker identity");
    assert_eq!((w.token_id.as_str(), w.worker_id.as_str()), ("tok1", "w1"));

    // The CA pair is importable by the Phase 7 cluster CA (what [cluster.tls] ca_* point at).
    let dir = tempfile::tempdir().expect("tempdir");
    let (crt, key) = pki.write_ca(dir.path()).expect("write ca");
    let ca = ostro_server::tls::ClusterCa::import_pem(
        &dir.path().join("imported"),
        "conf-7",
        &std::fs::read_to_string(crt).expect("crt"),
        &std::fs::read_to_string(key).expect("key"),
    )
    .expect("import");
    assert_eq!(ca.cluster_id(), "conf-7");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unary_call_carries_peer_identity() {
    guarded(async {
        let (_net, ch) = net(1);
        assert_eq!(check(&ch).await.expect("check"), "ok from 1");
        assert_eq!(propose(&ch).await.expect("propose"), 5);
        raft(&ch).await.expect("raft stream");
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cut_fails_fast_and_heal_restores() {
    guarded(async {
        let (net, ch) = net(2);
        check(&ch).await.expect("connected");
        // Cut with a live connection: the pipe resets and reconnects are refused.
        net.faults().partition(1, 2);
        for _ in 0..3 {
            let t = Instant::now();
            let e = tokio::time::timeout(Duration::from_secs(5), check_within(&ch, CUT_DEADLINE))
                .await
                .expect("a cut link fails fast, it does not hang")
                .expect_err("cut");
            // The code depends on where the reset catches the call (a race, seen on both musl
            // CI legs): before dispatch it is Unavailable; on the old connection it is
            // Cancelled (hyper `Canceled: connection closed`, or the call deadline when the
            // request was parked on the dying connection, see CALL_DEADLINE) or,
            // mid-exchange, Unknown "transport error" carrying the fault layer's
            // ConnectionReset. All are the transport failing, never a handler answer.
            let transport = match e.code() {
                Code::Unavailable | Code::Cancelled => true,
                Code::Unknown => e.message() == "transport error",
                _ => false,
            };
            assert!(transport, "{e:?}");
            assert!(t.elapsed() < Duration::from_secs(2), "{:?}", t.elapsed());
        }
        // A fresh channel: connect refused.
        let fresh = net.channel_as(1, 2);
        let e = check(&fresh).await.expect_err("refused");
        assert_eq!(e.code(), Code::Unavailable, "{e:?}");
        let msg = format!("{e:?}");
        assert!(
            msg.contains("refused") || msg.contains("reset") || msg.contains("transport"),
            "{msg}"
        );
        net.faults().heal_all();
        let mut ok = false;
        for _ in 0..50 {
            if check(&ch).await.is_ok() {
                ok = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(ok, "heal restores the link");
        check(&fresh).await.expect("fresh channel after heal");
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn isolate_cuts_both_directions() {
    guarded(async {
        let (net, ch) = net(3);
        check(&ch).await.expect("connected");
        net.faults().isolate(2);
        assert!(net.faults().link_cut(1, 2) && net.faults().link_cut(2, 1));
        assert!(!net.faults().link_cut(1, 3));
        check_within(&ch, CUT_DEADLINE).await.expect_err("isolated");
        net.faults().heal(2);
        assert!(!net.faults().link_cut(1, 2));
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blackhole_times_out_at_the_client_deadline() {
    guarded(async {
        let (net, ch) = net(4);
        check(&ch).await.expect("connected");
        net.faults().set(1, 2, RpcKind::Any, Fault::Blackhole);
        let t = Instant::now();
        let r = tokio::time::timeout(Duration::from_millis(300), check(&ch)).await; // clock-exempt: test deadline
        assert!(r.is_err(), "a blackholed call never answers: {r:?}");
        assert!(t.elapsed() >= Duration::from_millis(300));
        net.faults().clear_link(1, 2);
        check(&ch).await.expect("cleared");
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delay_adds_latency() {
    guarded(async {
        let (net, ch) = net(5);
        check(&ch).await.expect("connected");
        net.faults().set(1, 2, RpcKind::Any, Fault::Delay(200));
        let t = Instant::now();
        check(&ch).await.expect("delayed");
        assert!(
            t.elapsed() >= Duration::from_millis(200),
            "{:?}",
            t.elapsed()
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_on_raft_leaves_propose_working() {
    guarded(async {
        let (net, ch) = net(6);
        net.faults().set(1, 2, RpcKind::Raft, Fault::Drop(1.0));
        for _ in 0..5 {
            let e = raft(&ch).await.expect_err("raft dropped");
            assert_eq!(e.code(), Code::Unavailable);
            assert!(e.message().contains("dropped"), "{e:?}");
            assert_eq!(propose(&ch).await.expect("propose unaffected"), 5);
        }
        check(&ch).await.expect("health unaffected");
        // The reverse direction is a different link.
        assert!(!net.faults().link_cut(2, 1));
        net.faults().heal(1);
        raft(&ch).await.expect("healed");
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn same_seed_same_drop_sequence() {
    guarded(async {
        async fn pattern(seed: u64) -> Vec<bool> {
            let (net, ch) = net(seed);
            net.faults().set(1, 2, RpcKind::Any, Fault::Drop(0.5));
            let mut out = Vec::new();
            for _ in 0..32 {
                out.push(check(&ch).await.is_ok());
            }
            out
        }
        let a = pattern(42).await;
        let b = pattern(42).await;
        assert_eq!(a, b, "same seed, same sequence");
        assert!(a.iter().any(|x| *x) && a.iter().any(|x| !*x), "{a:?}");
    })
    .await;
}
