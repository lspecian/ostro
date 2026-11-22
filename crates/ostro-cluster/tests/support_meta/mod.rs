//! Test support for the MetaPeer suite (12-05): an inline rcgen cluster CA, node / worker
//! certificates, and metadata nodes served over real loopback mTLS.

#![allow(dead_code)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ostro_cluster::address_book::{AddressBook, PeerEntry};
use ostro_cluster::meta_peer::{
    DEFAULT_META_MAX_MESSAGE, GrpcForwarder, MetaPeerService, NoopSuspectSink, OutboxConfig,
    PeerOutbox, RaftSlot,
};
use ostro_cluster::transport::ClusterTransport;
use ostro_cluster::transport::tcp::{NodeTls, TcpTransport};
use ostro_meta::MetaHandle;
use ostro_meta::handle::{MetaStart, TickConfig};
use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa,
    Issuer, KeyPair, KeyUsagePurpose, SanType,
};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tonic::transport::server::TcpIncoming;

pub const CLUSTER: &str = "c-12-05";

/// A throwaway cluster CA.
pub struct TestCa {
    pub cluster_id: String,
    pub ca_pem: String,
    issuer: Issuer<'static, KeyPair>,
}

impl TestCa {
    pub fn new(cluster_id: &str) -> TestCa {
        let mut p = CertificateParams::default();
        let mut dn = DistinguishedName::new();
        dn.push(DnType::CommonName, format!("ostro cluster {cluster_id}"));
        p.distinguished_name = dn;
        p.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
        p.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
            KeyUsagePurpose::DigitalSignature,
        ];
        let key = KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
        let cert = p.self_signed(&key).unwrap();
        TestCa {
            cluster_id: cluster_id.to_owned(),
            ca_pem: cert.pem(),
            issuer: Issuer::new(p, key),
        }
    }

    fn leaf(&self, uri: &str, ekus: &[ExtendedKeyUsagePurpose]) -> NodeTls {
        let mut p = CertificateParams::default();
        let mut dn = DistinguishedName::new();
        dn.push(DnType::CommonName, uri);
        p.distinguished_name = dn;
        p.subject_alt_names = vec![
            SanType::DnsName("localhost".try_into().unwrap()),
            SanType::IpAddress([127, 0, 0, 1].into()),
            SanType::URI(uri.try_into().unwrap()),
        ];
        p.is_ca = IsCa::ExplicitNoCa;
        p.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        p.extended_key_usages = ekus.to_vec();
        p.use_authority_key_identifier_extension = true;
        let key = KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
        let cert = p.signed_by(&key, &self.issuer).unwrap();
        NodeTls {
            cert_chain_pem: format!("{}{}", cert.pem(), self.ca_pem),
            key_pem: key.serialize_pem(),
            ca_pem: self.ca_pem.clone(),
        }
    }

    /// A node certificate (`ostro://<cluster>/node/<id>`, serverAuth + clientAuth).
    pub fn node(&self, id: u64) -> NodeTls {
        self.node_of(&self.cluster_id.clone(), id)
    }

    /// A node certificate naming another cluster, signed by this CA.
    pub fn node_of(&self, cluster_id: &str, id: u64) -> NodeTls {
        self.leaf(
            &format!("ostro://{cluster_id}/node/{id}"),
            &[
                ExtendedKeyUsagePurpose::ServerAuth,
                ExtendedKeyUsagePurpose::ClientAuth,
            ],
        )
    }

    /// A worker client certificate (`ostro://<cluster>/worker/<token>/<worker>`).
    pub fn worker(&self, token: &str, worker: &str) -> NodeTls {
        self.leaf(
            &format!("ostro://{}/worker/{token}/{worker}", self.cluster_id),
            &[ExtendedKeyUsagePurpose::ClientAuth],
        )
    }
}

/// Meta ticks for the suite: the `[replication]` defaults (100 ms, election 10,
/// heartbeat 2).
pub fn ticks() -> TickConfig {
    TickConfig {
        interval: Duration::from_millis(100),
        election_tick: 10,
        heartbeat_tick: 2,
    }
}

/// One running metadata node: listener + MetaPeer service + MetaHandle.
pub struct MetaNode {
    pub id: u64,
    pub addr: SocketAddr,
    pub handle: MetaHandle,
    pub transport: Arc<TcpTransport>,
    shutdown: CancellationToken,
    server: tokio::task::JoinHandle<()>,
}

impl MetaNode {
    /// Serve `listener` and start the metadata member `id` of `voters` in `dir`.
    pub async fn start(
        ca: &TestCa,
        id: u64,
        voters: &[u64],
        dir: &Path,
        listener: TcpListener,
        book: Arc<AddressBook>,
    ) -> MetaNode {
        let addr = listener.local_addr().unwrap();
        let transport = Arc::new(TcpTransport::new(&ca.cluster_id, id, ca.node(id), book).unwrap());
        let slot = RaftSlot::new();
        let svc =
            MetaPeerService::new(id, voters.to_vec(), slot.clone(), Arc::new(NoopSuspectSink));
        let shutdown = CancellationToken::new();
        let router = Server::builder()
            .tls_config(transport.server_tls())
            .unwrap()
            .add_service(
                svc.into_server(transport.identity_interceptor(), DEFAULT_META_MAX_MESSAGE),
            );
        let sd = shutdown.clone();
        let server = tokio::spawn(async move {
            let incoming = TcpIncoming::from(listener).with_nodelay(Some(true));
            if let Err(e) = router
                .serve_with_incoming_shutdown(incoming, sd.cancelled_owned())
                .await
            {
                eprintln!("server error: {e}");
            }
        });
        let dyn_transport: Arc<dyn ClusterTransport> = transport.clone();
        let outbox = PeerOutbox::new(
            id,
            voters,
            Arc::clone(&dyn_transport),
            slot.clone(),
            OutboxConfig::default(),
        );
        let handle = MetaHandle::start_with(
            dir,
            MetaStart {
                node_id: id,
                voters: voters.to_vec(),
                outbox: Box::new(outbox),
                forwarder: Some(Arc::new(GrpcForwarder::new(dyn_transport))),
                tick: ticks(),
            },
        )
        .await
        .expect("meta start_with");
        assert!(slot.set(handle.raft().clone()));
        MetaNode {
            id,
            addr,
            handle,
            transport,
            shutdown,
            server,
        }
    }

    pub async fn stop(self) {
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

/// A static group of metadata nodes on loopback.
pub struct MetaCluster {
    pub ca: TestCa,
    pub voters: Vec<u64>,
    pub book: Arc<AddressBook>,
    pub addrs: HashMap<u64, SocketAddr>,
    pub dirs: HashMap<u64, TempDir>,
    pub nodes: HashMap<u64, MetaNode>,
}

impl MetaCluster {
    pub async fn start(voters: &[u64]) -> MetaCluster {
        let ca = TestCa::new(CLUSTER);
        let mut listeners = HashMap::new();
        let mut addrs = HashMap::new();
        for &id in voters {
            let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
            addrs.insert(id, l.local_addr().unwrap());
            listeners.insert(id, l);
        }
        let book = Arc::new(AddressBook::from_peers(voters.iter().map(|&id| {
            PeerEntry {
                id,
                addr: addrs[&id].to_string(),
                repl_addr: None,
            }
        })));
        let mut c = MetaCluster {
            ca,
            voters: voters.to_vec(),
            book,
            addrs,
            dirs: voters
                .iter()
                .map(|&i| (i, TempDir::new().unwrap()))
                .collect(),
            nodes: HashMap::new(),
        };
        for &id in voters {
            let l = listeners.remove(&id).unwrap();
            c.start_node(id, l).await;
        }
        c
    }

    async fn start_node(&mut self, id: u64, listener: TcpListener) {
        let node = MetaNode::start(
            &self.ca,
            id,
            &self.voters,
            self.dirs[&id].path(),
            listener,
            Arc::clone(&self.book),
        )
        .await;
        self.nodes.insert(id, node);
    }

    /// Restart a stopped node on its old address.
    pub async fn restart(&mut self, id: u64) {
        let deadline = Instant::now() + Duration::from_secs(10);
        let listener = loop {
            match TcpListener::bind(self.addrs[&id]).await {
                Ok(l) => break l,
                Err(e) => {
                    assert!(Instant::now() < deadline, "rebind {}: {e}", self.addrs[&id]);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        };
        self.start_node(id, listener).await;
    }

    pub async fn stop(&mut self, id: u64) {
        if let Some(n) = self.nodes.remove(&id) {
            n.stop().await;
        }
    }

    pub async fn stop_all(&mut self) {
        let ids: Vec<u64> = self.nodes.keys().copied().collect();
        for id in ids {
            self.stop(id).await;
        }
    }

    /// The single leader every running node agrees on.
    pub async fn leader(&self, within: Duration) -> u64 {
        let deadline = Instant::now() + within;
        loop {
            let leaders: Vec<u64> = self
                .nodes
                .values()
                .filter(|n| n.handle.is_leader())
                .map(|n| n.id)
                .collect();
            if let [l] = leaders[..]
                && self
                    .nodes
                    .values()
                    .all(|n| n.handle.raft().status().leader_id == Some(l))
            {
                return l;
            }
            assert!(
                Instant::now() < deadline,
                "no agreed leader within {within:?}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    pub fn follower(&self, leader: u64) -> u64 {
        *self.nodes.keys().find(|&&i| i != leader).unwrap()
    }

    pub fn handle(&self, id: u64) -> &MetaHandle {
        &self.nodes[&id].handle
    }

    /// Wait until every running node applied the highest index any node applied.
    pub async fn settle(&self) {
        let target = self
            .nodes
            .values()
            .map(|n| n.handle.view().applied())
            .max()
            .unwrap();
        for n in self.nodes.values() {
            tokio::time::timeout(
                Duration::from_secs(30),
                n.handle.raft().wait_applied(target),
            )
            .await
            .expect("settle timeout")
            .expect("applied");
        }
    }
}

/// A client transport presenting `tls` (any identity) towards `addr` as node `target`.
pub fn client_transport(
    cluster_id: &str,
    tls: NodeTls,
    target: u64,
    addr: SocketAddr,
) -> Arc<TcpTransport> {
    let book = Arc::new(AddressBook::from_peers([PeerEntry {
        id: target,
        addr: addr.to_string(),
        repl_addr: None,
    }]));
    Arc::new(TcpTransport::new(cluster_id, 999, tls, book).unwrap())
}
