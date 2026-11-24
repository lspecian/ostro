//! 13-05 Task 3 (Pitfall 4): a metadata snapshot larger than 4 MiB reaches a new learner
//! over the real loopback-mTLS `MetaPeer` transport.
//!
//! A 3-voter group holds > 4 MiB of metadata (sealed secrets), compacts its log
//! (`snapshot_now`), and adds a joiner (`MetaHandle::start_joiner`) as a learner through
//! `propose_conf_change`: the leader's outbox opens a stream to the new node on the fly,
//! the learner is authorized as a configuration member, and it catches up from one
//! `MsgSnapshot`. With the limit forced to 4 MiB the same learner never catches up (the
//! failed stream is reported with `report_snapshot_failed` and retried), which proves the
//! limit is what matters. Run with `--test-threads=1`.

mod support_meta;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ostro_cluster::address_book::{AddressBook, PeerEntry};
use ostro_cluster::meta_peer::{
    DEFAULT_META_MAX_MESSAGE, GrpcForwarder, MetaPeerService, NoopSuspectSink, OutboxConfig,
    PeerOutbox, RaftSlot,
};
use ostro_cluster::transport::ClusterTransport;
use ostro_cluster::transport::tcp::TcpTransport;
use ostro_meta::handle::{MetaOptions, MetaStart};
use ostro_meta::{MetaCommand, MetaHandle, MetaKey, MetaRecord, SealedSecret, encode_snapshot};
use ostro_raft_driver::raft::eraftpb::{ConfChangeSingle, ConfChangeType, ConfChangeV2};
use support_meta::{TestCa, ticks};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tonic::transport::server::TcpIncoming;

const VOTERS: [u64; 3] = [1, 2, 3];
const LEARNER: u64 = 4;
const FOUR_MIB: usize = 4 * 1024 * 1024;

struct Node {
    handle: MetaHandle,
    slot: RaftSlot,
    shutdown: CancellationToken,
    server: tokio::task::JoinHandle<()>,
}

impl Node {
    async fn start(
        ca: &TestCa,
        id: u64,
        dir: &std::path::Path,
        listener: TcpListener,
        book: Arc<AddressBook>,
        max_message: usize,
        joiner: bool,
    ) -> Node {
        let transport = Arc::new(TcpTransport::new(&ca.cluster_id, id, ca.node(id), book).unwrap());
        let slot = RaftSlot::new();
        let svc =
            MetaPeerService::new(id, VOTERS.to_vec(), slot.clone(), Arc::new(NoopSuspectSink));
        let shutdown = CancellationToken::new();
        let router = Server::builder()
            .tls_config(transport.server_tls())
            .unwrap()
            .add_service(svc.into_server(transport.identity_interceptor(), max_message));
        let sd = shutdown.clone();
        let server = tokio::spawn(async move {
            let incoming = TcpIncoming::from(listener).with_nodelay(Some(true));
            let _ = router
                .serve_with_incoming_shutdown(incoming, sd.cancelled_owned())
                .await;
        });
        let dyn_transport: Arc<dyn ClusterTransport> = transport.clone();
        let outbox = PeerOutbox::new(
            id,
            &VOTERS,
            Arc::clone(&dyn_transport),
            slot.clone(),
            OutboxConfig {
                max_message,
                ..OutboxConfig::default()
            },
        );
        let start = MetaStart {
            node_id: id,
            voters: VOTERS.to_vec(),
            outbox: Box::new(outbox),
            forwarder: Some(Arc::new(
                GrpcForwarder::new(dyn_transport).with_limits(max_message, Duration::from_secs(10)),
            )),
            tick: ticks(),
        };
        let opts = MetaOptions {
            sync: false,
            ..MetaOptions::default()
        };
        let handle = if joiner {
            MetaHandle::start_joiner(dir, start, opts).await
        } else {
            MetaHandle::start_with_options(dir, start, opts).await
        }
        .expect("meta start");
        assert!(slot.set(handle.raft().clone()));
        Node {
            handle,
            slot,
            shutdown,
            server,
        }
    }

    async fn stop(self) {
        self.handle.stop().await;
        self.shutdown.cancel();
        let mut server = self.server;
        if tokio::time::timeout(Duration::from_secs(5), &mut server)
            .await
            .is_err()
        {
            server.abort();
        }
    }
}

struct Group {
    ca: TestCa,
    book: Arc<AddressBook>,
    listeners: HashMap<u64, TcpListener>,
    dirs: HashMap<u64, TempDir>,
    nodes: HashMap<u64, Node>,
    max_message: usize,
}

impl Group {
    async fn new(max_message: usize) -> Group {
        let ca = TestCa::new("c-13-05");
        let mut listeners = HashMap::new();
        let mut addrs: HashMap<u64, SocketAddr> = HashMap::new();
        for id in [1, 2, 3, LEARNER] {
            let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
            addrs.insert(id, l.local_addr().unwrap());
            listeners.insert(id, l);
        }
        // The learner's address is known up front (13-09 upserts it from its Member record).
        let book = Arc::new(AddressBook::from_peers(addrs.iter().map(|(&id, a)| {
            PeerEntry {
                id,
                addr: a.to_string(),
                repl_addr: None,
            }
        })));
        let mut g = Group {
            ca,
            book,
            listeners,
            dirs: [1, 2, 3, LEARNER]
                .into_iter()
                .map(|i| (i, TempDir::new().unwrap()))
                .collect(),
            nodes: HashMap::new(),
            max_message,
        };
        for id in VOTERS {
            g.start(id, false).await;
        }
        for n in g.nodes.values() {
            n.handle.wait_ready().await.expect("ready");
        }
        g
    }

    async fn start(&mut self, id: u64, joiner: bool) {
        let l = self.listeners.remove(&id).unwrap();
        let n = Node::start(
            &self.ca,
            id,
            self.dirs[&id].path(),
            l,
            Arc::clone(&self.book),
            self.max_message,
            joiner,
        )
        .await;
        self.nodes.insert(id, n);
    }

    async fn leader(&self) -> u64 {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(n) = self.nodes.values().find(|n| n.handle.is_leader()) {
                return n.handle.node_id();
            }
            assert!(Instant::now() < deadline, "no leader");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn stop_all(&mut self) {
        let ids: Vec<u64> = self.nodes.keys().copied().collect();
        for id in ids {
            if let Some(n) = self.nodes.remove(&id) {
                n.stop().await;
            }
        }
    }
}

fn secret(i: usize) -> MetaCommand {
    MetaCommand::Put {
        key: MetaKey::Secret(format!("big-{i}")),
        expected_version: None,
        record: MetaRecord::Secret(SealedSecret {
            name: format!("big-{i}"),
            // Incompressible-looking bytes: no codec makes this small.
            ciphertext: (0..256 * 1024)
                .map(|j| ((i * 31 + j * 7) % 251) as u8)
                .collect(),
            key_id: "k1".into(),
            ext: Vec::new(),
        }),
    }
}

fn add_learner() -> ConfChangeV2 {
    let mut s = ConfChangeSingle::default();
    s.set_change_type(ConfChangeType::AddLearnerNode);
    s.node_id = LEARNER;
    ConfChangeV2 {
        changes: vec![s],
        ..Default::default()
    }
}

/// Fill > 4 MiB of metadata, compact the leader's log, start the joiner and add it as a
/// learner. Returns (leader, the leader's applied index after the conf change).
async fn grow_and_add_learner(g: &mut Group) -> (u64, u64) {
    let leader = g.leader().await;
    for i in 0..20 {
        g.nodes[&leader]
            .handle
            .propose(secret(i))
            .await
            .expect("put secret");
    }
    let snap_len = g.nodes[&leader]
        .handle
        .view()
        .read(|s| encode_snapshot(s).len());
    eprintln!("meta snapshot: {snap_len} bytes");
    assert!(snap_len > FOUR_MIB, "{snap_len}");
    g.nodes[&leader].handle.raft().snapshot_now();
    tokio::time::sleep(Duration::from_millis(500)).await;
    g.start(LEARNER, true).await;
    let applied = g.nodes[&leader]
        .handle
        .propose_conf_change(add_learner())
        .await
        .expect("learner added");
    assert_eq!(g.nodes[&leader].handle.conf_state().learners, vec![LEARNER]);
    (leader, applied.index)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn learner_catches_up_from_a_meta_snapshot_above_4_mib() {
    let mut g = Group::new(DEFAULT_META_MAX_MESSAGE).await;
    let (leader, index) = grow_and_add_learner(&mut g).await;
    let t = Instant::now();
    tokio::time::timeout(
        Duration::from_secs(60),
        g.nodes[&LEARNER].handle.raft().wait_applied(index),
    )
    .await
    .expect("the learner did not catch up within 60 s")
    .expect("applied");
    eprintln!("learner caught up after {:?}", t.elapsed());
    let n = g.nodes[&LEARNER].handle.view().read(|s| {
        (0..20)
            .filter(|i| s.get(&MetaKey::Secret(format!("big-{i}"))).is_some())
            .count()
    });
    assert_eq!(n, 20);
    // The leader's progress shows it matched once the snapshot's answer arrived.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let p = g.nodes[&leader].handle.raft_detail().progress;
        let lp = p
            .iter()
            .find(|p| p.id == LEARNER)
            .expect("learner progress")
            .clone();
        if lp.matched >= index && lp.pending_snapshot == 0 {
            break;
        }
        assert!(Instant::now() < deadline, "{lp:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // And a read barrier on the learner works over the transport.
    g.nodes[&LEARNER]
        .handle
        .read_index_barrier()
        .await
        .expect("learner barrier");
    // The first ask starts the learner's lease rounds; one resolves through the leader.
    let deadline = Instant::now() + Duration::from_secs(30);
    while g.nodes[&LEARNER].slot.meta_lease_anchor().is_none() {
        assert!(
            Instant::now() < deadline,
            "the learner never got a lease anchor"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    g.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn with_a_4_mib_limit_the_learner_never_catches_up() {
    let mut g = Group::new(FOUR_MIB).await;
    let (leader, index) = grow_and_add_learner(&mut g).await;
    let r = tokio::time::timeout(
        Duration::from_secs(15),
        g.nodes[&LEARNER].handle.raft().wait_applied(index),
    )
    .await;
    assert!(r.is_err(), "the learner caught up through a 4 MiB limit");
    let applied = g.nodes[&LEARNER].handle.view().applied();
    assert!(applied < index, "learner applied {applied} of {index}");
    // The leader keeps retrying the snapshot (pending or probing), never matched.
    let p = g.nodes[&leader].handle.raft_detail().progress;
    let lp = p
        .iter()
        .find(|p| p.id == LEARNER)
        .expect("learner progress");
    assert!(lp.matched < index, "{lp:?}");
    g.stop_all().await;
}
