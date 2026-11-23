//! Phase 12 server wiring (12-14, research Patterns 4-5, D-P12-13/14/15/16/21/22):
//! [`ClusterServices`] owns the node's cluster side. Every server runs it, a single node
//! included (RF=1 is replicas `[self]` on the same path, SC4).
//!
//! # Boot order ([`ClusterServices::start`], before any partition opens)
//!
//! 1. `bind_node_id` (node.toml), before meta opens.
//! 2. Probe the persisted meta read-only (`identity::probe_meta`) and resolve the cluster id:
//!    configured → node.toml → the CA's stored id → `ClusterInfo.cluster_id` → fresh (blank
//!    meta only); disagreeing sources are fatal.
//! 3. Joiner check: blank meta over existing partition data fail-stops (wiped-voter remedy
//!    with peers, "meta dir missing" without). Blank meta without partition data bootstraps
//!    with `voters = self ∪ peers`.
//! 4. Meta voter check: a persisted ConfState whose voters differ from `self ∪ peers` is fatal
//!    ("grow via Phase 13").
//! 5. `bind_cluster_id`, then TLS: the shared CA (`[cluster.tls] ca_cert_file/ca_key_file`,
//!    `ClusterCa::import_pem`) or this node's own CA, and the node certificate.
//! 6. The cluster listener, bound in every profile: `MetaPeer` + `Replication` (+ the
//!    Phase 7 Execute service once it attaches), mTLS on `[cluster] listen`, or the
//!    in-memory incoming of the injected transport (`ServerOptions::cluster_transport`).
//! 7. Meta: `MetaHandle::start_with(voters, PeerOutbox, GrpcForwarder, [replication] ticks)`,
//!    `RaftSlot::set`, `wait_ready`, then the live voters must equal the configured set.
//! 8. `InitCluster` with the bound id (idempotent), `ClusterInfo.cluster_id` must equal it
//!    (W2); the own `Member` record is upserted.
//! 9. The controller (bootstrap placement of epoch-0 partitions, failover) runs on every
//!    node and acts only as meta leader; the meta-driven [`MetaAssignments`] source and the
//!    self-demotion watchdog start.
//!
//! The server then opens the node with that source and [`ClusterServices::follower_driver`],
//! and calls [`ClusterServices::attach_node`]: fetch / snapshot contexts are bound into the
//! `Replication` service, the follower `FetcherDriver` (with the snapshot installer) becomes
//! live, one `IsrPublisher` runs per led partition, and the module-blob sync task starts.
//!
//! # Self-demotion
//!
//! A node whose meta contact (`leader_contact_age`) is older than `session_timeout` masks
//! every partition it leads as leaderless in its assignment view, so the role drivers step
//! down locally (clients get 503 fast; zombie safety does not depend on it, research
//! Pattern 4). Contact back: the real view returns and the node asks for a fresh term.
//! Counted by `ostro_repl_self_demotions_total`.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use futures_util_shim::BoxFuture;
use ostro_blob::BlobStore;
use ostro_cluster::address_book::{AddressBook, PeerEntry};
use ostro_cluster::config::ReplicationTuning;
use ostro_cluster::controller::{self, Controller};
use ostro_cluster::fetch::FetchCtx;
use ostro_cluster::fetcher::FetcherDriver;
use ostro_cluster::identity::{self, BootError, ClusterIdSource};
use ostro_cluster::install::InstallCtx;
use ostro_cluster::isr_publisher::{IsrMeta, IsrPublisher};
use ostro_cluster::liveness::{Liveness, LivenessHandle};
use ostro_cluster::meta_peer::{
    DEFAULT_META_MAX_MESSAGE, GrpcForwarder, MetaPeerService, OutboxConfig, PeerOutbox, RaftSlot,
};
use ostro_cluster::service::{self as repl_service, ListenerIo, ReplicationService};
use ostro_cluster::snapshot::{Exporter, SnapshotCtx, SnapshotServeConfig};
use ostro_cluster::status::{self, MetaSummary, PartitionInput};
use ostro_cluster::transport::tcp::{NodeTls, PeerIdentity, TcpTransport};
use ostro_cluster::transport::{ClusterIncoming, ClusterTransport};
use ostro_log::ReplicatedLog as _;
use ostro_meta::handle::MetaStart;
use ostro_meta::{MetaHandle, MetaState};
use ostro_partition::roles::{
    AssignmentSource, FollowerDriver, FollowerTask, PartitionAssignmentView, PartitionLogHandle,
};
use ostro_partition::{Clock, Node, PartitionRole};
use ostro_types::{Epoch, PartitionCount, PartitionId};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::service::Routes;
use tonic::transport::{Certificate, Identity, ServerTlsConfig};
use tracing::{debug, info, warn};

use crate::config::ServerConfig;
use crate::server::ServerError;
use crate::tls::ca::ClusterCa;
use crate::workers::service::{LateExecute, listener_client_roots};

/// `BoxFuture` without a `futures` dependency in this crate.
mod futures_util_shim {
    pub type BoxFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;
}

/// The `[replication]` section as the cluster layer's tuning.
pub fn tuning(cfg: &ServerConfig) -> ReplicationTuning {
    let r = &cfg.replication;
    ReplicationTuning {
        session_timeout: r.session_timeout.get(),
        replica_lag_max: r.replica_lag_max.get(),
        meta_tick_interval: r.meta_tick_interval.get(),
        meta_election_tick: r.meta_election_tick as usize,
        meta_heartbeat_tick: r.meta_heartbeat_tick as usize,
        fetch_max_wait: r.fetch_max_wait.get(),
        fetch_read_timeout: r.fetch_read_timeout.get(),
        fetch_max_bytes: usize::try_from(r.fetch_max_bytes.get()).unwrap_or(usize::MAX),
        max_envelope_bytes: ostro_types::envelope::DEFAULT_MAX_ENVELOPE_BYTES,
        rf: r.rf,
        min_insync: r.min_insync,
    }
}

fn boot(e: BootError) -> ServerError {
    ServerError::Config(e.to_string())
}

/// Whether `data_dir` already holds partition data (a state store or WAL segment).
fn partition_data_exists(data_dir: &Path) -> bool {
    let non_empty = |d: PathBuf| {
        std::fs::read_dir(d)
            .map(|mut rd| rd.next().is_some())
            .unwrap_or(false)
    };
    non_empty(data_dir.join("state")) || non_empty(data_dir.join("wal"))
}

/// Inputs of [`ClusterServices::start`].
pub struct ClusterStart<'a> {
    pub cfg: &'a ServerConfig,
    pub data_dir: &'a Path,
    pub count: PartitionCount,
    pub clock: Arc<dyn Clock>,
    /// The injected transport (conformance harness); `None` = mTLS TCP.
    pub transport: Option<Arc<dyn ClusterTransport>>,
    /// The bound API address (the `Member.addr` record).
    pub api_addr: SocketAddr,
}

/// What [`ClusterServices::start`] measured.
#[derive(Clone, Copy, Debug, Default)]
pub struct ClusterTimings {
    /// Identity + TLS + listener + meta start until meta is ready.
    pub meta_ready: Duration,
    /// `InitCluster` + member upsert.
    pub init_cluster: Duration,
}

/// The node's cluster side (see the module docs).
pub struct ClusterServices {
    node_id: u64,
    cluster_id: String,
    token_key: [u8; 32],
    meta: Arc<MetaHandle>,
    voters: Vec<u64>,
    count: PartitionCount,
    clock: Arc<dyn Clock>,
    tuning: ReplicationTuning,
    data_dir: PathBuf,
    transport: Arc<dyn ClusterTransport>,
    assignments: Arc<MetaAssignments>,
    follower: Arc<LateFollower>,
    replication: ReplicationService,
    execute: LateExecute,
    listener_addr: SocketAddr,
    liveness: LivenessHandle,
    /// Cluster tasks that hold meta (controller, publishers, sync, views): stopped before
    /// the node.
    tasks: Mutex<Vec<JoinHandle<()>>>,
    tasks_stop: CancellationToken,
    /// The listener(s): stopped after meta.
    listener_tasks: Mutex<Vec<JoinHandle<()>>>,
    listener_stop: CancellationToken,
    /// Measured startup steps.
    pub timings: ClusterTimings,
}

impl std::fmt::Debug for ClusterServices {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterServices")
            .field("node_id", &self.node_id)
            .field("cluster_id", &self.cluster_id)
            .field("voters", &self.voters)
            .field("listener", &self.listener_addr)
            .finish_non_exhaustive()
    }
}

impl ClusterServices {
    /// Steps 1-9 of the boot order (module docs). On error nothing keeps running.
    pub async fn start(s: ClusterStart<'_>) -> Result<ClusterServices, ServerError> {
        let ClusterStart {
            cfg,
            data_dir,
            count,
            clock,
            transport: injected,
            api_addr,
        } = s;
        // clock-exempt: startup step durations are reported, never used for logic.
        let t0 = std::time::Instant::now(); // clock-exempt: startup timing
        let node_id = cfg.node.node_id;
        let tuning = tuning(cfg);
        let peers: Vec<u64> = cfg.cluster.peers.iter().map(|p| p.id).collect();
        let mut voters = peers.clone();
        voters.push(node_id);
        voters.sort_unstable();

        // 1. node id before meta opens.
        ostro_partition::bind_node_id(data_dir, node_id)?;

        // 2.-4. identity and voter checks, read-only.
        let meta_dir = data_dir.join("meta");
        let probe = identity::probe_meta(&meta_dir).map_err(boot)?;
        let tls_dir = data_dir.join("tls");
        let node_toml = ostro_partition::bound_cluster_id(data_dir)?;
        let stored_ca = identity::stored_ca_cluster_id(&tls_dir);
        let source = identity::resolve_cluster_id(
            cfg.cluster.cluster_id.as_deref(),
            node_toml.as_deref(),
            stored_ca.as_deref(),
            probe.cluster_id.as_deref(),
            probe.blank,
        )
        .map_err(boot)?;
        if identity::is_joiner(probe.blank, partition_data_exists(data_dir)) {
            return Err(boot(identity::joiner_error(!peers.is_empty(), &meta_dir)));
        }
        identity::check_meta_voters(probe.conf.clone(), &voters).map_err(boot)?;
        let cluster_id = match &source {
            ClusterIdSource::GenerateFresh => uuid::Uuid::now_v7().to_string(),
            other => other.id().unwrap_or_default().to_owned(),
        };
        ostro_partition::bind_cluster_id(data_dir, &cluster_id)?;
        info!(cluster_id = %cluster_id, source = ?source, voters = ?voters, "cluster identity bound");

        // 5. TLS: the shared CA (peers) or this node's own CA, and the node certificate.
        let ca = match (&cfg.cluster.tls.ca_cert_file, &cfg.cluster.tls.ca_key_file) {
            (Some(crt), Some(key)) => {
                let read = |p: &PathBuf| {
                    std::fs::read_to_string(p).map_err(|e| {
                        ServerError::Config(format!("[cluster.tls] {}: {e}", p.display()))
                    })
                };
                ClusterCa::import_pem(&tls_dir, &cluster_id, &read(crt)?, &read(key)?)
                    .map_err(|e| ServerError::Config(format!("[cluster.tls] CA: {e}")))?
            }
            _ => ClusterCa::load_or_create(&tls_dir, &cluster_id)
                .map_err(|e| ServerError::Config(format!("cluster CA: {e}")))?,
        };
        let node_cert = ca
            .load_or_issue_node_cert(node_id, &cfg.cluster.advertise)
            .map_err(|e| ServerError::Config(format!("node certificate: {e}")))?;
        let ca_pem = ca.cert_pem();

        // 6. transport + the always-bound cluster listener.
        let transport: Arc<dyn ClusterTransport> = match injected {
            Some(t) => t,
            None => {
                let book = Arc::new(AddressBook::from_peers(cfg.cluster.peers.iter().map(|p| {
                    PeerEntry {
                        id: p.id,
                        addr: p.addr.clone(),
                        repl_addr: p.repl_addr.clone(),
                    }
                })));
                Arc::new(
                    TcpTransport::new(
                        &cluster_id,
                        node_id,
                        NodeTls {
                            cert_chain_pem: node_cert.chain_pem(),
                            key_pem: node_cert.key_pem(),
                            ca_pem: ca_pem.clone(),
                        },
                        book,
                    )
                    .map_err(|e| ServerError::Config(format!("cluster transport: {e}")))?,
                )
            }
        };
        let identity = PeerIdentity::new(&cluster_id);
        let slot = RaftSlot::new();
        let liveness =
            LivenessHandle::new(Liveness::with_config(Arc::clone(&clock), tuning.liveness()));
        let replication = ReplicationService::new();
        let execute = LateExecute::new();
        let limit = tuning.replication_message_limit();
        let routes = Routes::new(repl_service::replication_server(
            replication.clone(),
            identity.clone(),
            limit,
        ))
        .add_service(
            MetaPeerService::new(
                node_id,
                voters.clone(),
                slot.clone(),
                Arc::new(liveness.clone()),
            )
            .into_server(identity.clone(), DEFAULT_META_MAX_MESSAGE),
        )
        .add_service(execute.clone());
        let tls_config = || -> Result<ServerTlsConfig, ServerError> {
            let (roots, _) = listener_client_roots(cfg, &ca_pem)?;
            Ok(ServerTlsConfig::new()
                .identity(Identity::from_pem(
                    node_cert.chain_pem(),
                    node_cert.key_pem(),
                ))
                .client_ca_root(Certificate::from_pem(roots)))
        };
        let listener_stop = CancellationToken::new();
        let keepalive = Some(cfg.workers.keepalive.get());
        let mut listener_tasks = Vec::new();
        let (io, listener_addr) = match transport.incoming() {
            ClusterIncoming::Memory(rx) => (ListenerIo::Memory(rx), cfg.cluster.listen),
            ClusterIncoming::Tcp => {
                let l = std::net::TcpListener::bind(cfg.cluster.listen).map_err(|source| {
                    ServerError::Bind {
                        addr: cfg.cluster.listen,
                        source,
                    }
                })?;
                let addr = l.local_addr().map_err(|source| ServerError::Bind {
                    addr: cfg.cluster.listen,
                    source,
                })?;
                (ListenerIo::Tcp(l, tls_config()?), addr)
            }
        };
        let is_tcp = matches!(io, ListenerIo::Tcp(..));
        listener_tasks.push(
            repl_service::serve(io, routes, keepalive, listener_stop.clone())
                .map_err(|e| ServerError::Config(format!("cluster listener: {e}")))?,
        );
        // RP-10: an optional second listener serving only Replication.
        if let (Some(addr), true) = (cfg.cluster.replication_listen, is_tcp) {
            let l = std::net::TcpListener::bind(addr)
                .map_err(|source| ServerError::Bind { addr, source })?;
            let routes = Routes::new(repl_service::replication_server(
                replication.clone(),
                identity.clone(),
                limit,
            ));
            listener_tasks.push(
                repl_service::serve(
                    ListenerIo::Tcp(l, tls_config()?),
                    routes,
                    keepalive,
                    listener_stop.clone(),
                )
                .map_err(|e| ServerError::Config(format!("replication listener: {e}")))?,
            );
        }
        info!(addr = %listener_addr, tcp = is_tcp, "cluster listener bound (MetaPeer, Replication)");
        let stop_listener = |tasks: Vec<JoinHandle<()>>, stop: CancellationToken| async move {
            stop.cancel();
            for t in tasks {
                let _ = t.await;
            }
        };

        // 7. meta over the live transport (single node included: zero peers).
        let outbox = PeerOutbox::new(
            node_id,
            &peers,
            Arc::clone(&transport),
            slot.clone(),
            OutboxConfig::default(),
        );
        let started = MetaHandle::start_with(
            &meta_dir,
            MetaStart {
                node_id,
                voters: voters.clone(),
                outbox: Box::new(outbox),
                forwarder: Some(Arc::new(GrpcForwarder::new(Arc::clone(&transport)))),
                tick: tuning.tick(),
            },
        )
        .await;
        let meta = match started {
            Ok(m) => m,
            Err(e) => {
                stop_listener(listener_tasks, listener_stop).await;
                return Err(e.into());
            }
        };
        slot.set(meta.raft().clone());
        let ready = async {
            meta.wait_ready().await.map_err(ServerError::from)?;
            identity::check_voters_after_ready(&meta.raft().status().voters, &voters).map_err(boot)
        };
        if let Err(e) = ready.await {
            meta.stop().await;
            stop_listener(listener_tasks, listener_stop).await;
            return Err(e);
        }
        let meta_ready = t0.elapsed();

        // 8. InitCluster with the bound id, then the own Member record.
        let t = std::time::Instant::now(); // clock-exempt: startup timing
        let init = async {
            let token_key =
                crate::server::init_cluster(&meta, &cluster_id, node_id, count, clock.now_ms())
                    .await?;
            let stored = meta
                .view()
                .read(|s| s.cluster().map(|c| c.value.cluster_id.clone()));
            identity::check_cluster_info(stored.as_deref(), &cluster_id).map_err(boot)?;
            let cluster_addr = if is_tcp {
                Some(listener_addr.to_string())
            } else {
                Some(format!("mem://{node_id}"))
            };
            let me = ostro_meta::Member {
                node_id,
                addr: api_addr.to_string(),
                joined_at_ms: clock.now_ms(),
                cluster_addr,
                repl_addr: cfg.cluster.replication_listen.map(|a| a.to_string()),
                ext: Vec::new(),
            };
            if let Err(e) = ostro_cluster::members::upsert_self(&meta, me).await {
                warn!(error = %e, "own member record not updated");
            }
            Ok::<_, ServerError>(token_key)
        };
        let token_key = match init.await {
            Ok(k) => k,
            Err(e) => {
                meta.stop().await;
                stop_listener(listener_tasks, listener_stop).await;
                return Err(e);
            }
        };
        let init_cluster = t.elapsed();
        let meta = Arc::new(meta);

        // 9. controller, assignment source, self-demotion watchdog.
        let tasks_stop = CancellationToken::new();
        let mut tasks = Vec::new();
        tasks.push(tokio::spawn(
            Controller::new(
                Arc::clone(&meta),
                Arc::new(liveness.clone()),
                tuning.controller(voters.len()),
                Arc::clone(&clock),
            )
            .run(tasks_stop.clone()),
        ));
        let assignments = MetaAssignments::start(
            node_id,
            &meta,
            count,
            Arc::clone(&clock),
            tuning.session_timeout,
            tasks_stop.clone(),
            &mut tasks,
        );
        Ok(ClusterServices {
            node_id,
            cluster_id,
            token_key,
            meta,
            voters,
            count,
            clock,
            tuning,
            data_dir: data_dir.to_path_buf(),
            transport,
            assignments,
            follower: Arc::new(LateFollower::default()),
            replication,
            execute,
            listener_addr,
            liveness,
            tasks: Mutex::new(tasks),
            tasks_stop,
            listener_tasks: Mutex::new(listener_tasks),
            listener_stop,
            timings: ClusterTimings {
                meta_ready,
                init_cluster,
            },
        })
    }

    /// The bound cluster id.
    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    /// The await-token key stored in meta.
    pub fn token_key(&self) -> [u8; 32] {
        self.token_key
    }

    /// The metadata handle.
    pub fn meta(&self) -> &Arc<MetaHandle> {
        &self.meta
    }

    /// The configured metadata voters (self ∪ peers).
    pub fn voters(&self) -> &[u64] {
        &self.voters
    }

    /// The node-to-node transport.
    pub fn transport(&self) -> &Arc<dyn ClusterTransport> {
        &self.transport
    }

    /// The controller's liveness view.
    pub fn liveness(&self) -> &LivenessHandle {
        &self.liveness
    }

    /// The meta-driven assignment source for `NodeConfig::assignments`.
    pub fn assignments(&self) -> Arc<dyn AssignmentSource> {
        Arc::clone(&self.assignments) as Arc<dyn AssignmentSource>
    }

    /// The assignment watch (the same views the role drivers see).
    pub fn assignment_watch(&self) -> watch::Receiver<Arc<Vec<PartitionAssignmentView>>> {
        self.assignments.subscribe()
    }

    /// The follower driver for `NodeConfig::follower_driver` (live after
    /// [`ClusterServices::attach_node`]).
    pub fn follower_driver(&self) -> Arc<dyn FollowerDriver> {
        Arc::clone(&self.follower) as Arc<dyn FollowerDriver>
    }

    /// The Phase 7 Execute slot and the listener address (for `P7Ctx::cluster_listener`).
    pub fn execute_slot(&self) -> (LateExecute, SocketAddr) {
        (self.execute.clone(), self.listener_addr)
    }

    /// Whether this node is currently self-demoted (meta contact lost).
    pub fn self_demoted(&self) -> bool {
        *self.assignments.demoted.borrow()
    }

    /// Bind the node's partitions: fetch / snapshot contexts into `Replication`, the live
    /// `FetcherDriver`, one `IsrPublisher` per led partition, the module-blob sync task.
    pub fn attach_node(&self, node: &Arc<Node>) {
        let mut fetch = Vec::new();
        let mut snaps = Vec::new();
        let mut blobs: Option<Arc<BlobStore>> = None;
        for p in self.count.ids() {
            match node.handle(p) {
                Some(h) => {
                    blobs.get_or_insert_with(|| Arc::clone(h.blobs()));
                    let log: Arc<PartitionLogHandle> = Arc::clone(h.log());
                    fetch.push(Some(Arc::new(FetchCtx::new(
                        Arc::clone(&log),
                        self.assignment_watch(),
                        Arc::clone(&self.clock),
                        self.tuning.fetch(),
                    ))));
                    snaps.push(Some(Arc::new(SnapshotCtx::new(
                        log,
                        self.count,
                        &self.data_dir,
                        self.assignment_watch(),
                        Exporter::None,
                        Arc::clone(&self.clock),
                        SnapshotServeConfig::default(),
                    ))));
                }
                None => {
                    fetch.push(None);
                    snaps.push(None);
                }
            }
        }
        let Some(blobs) = blobs else {
            warn!("no partition started; replication stays unbound");
            return;
        };
        let weak = Arc::downgrade(node);
        let prepare: repl_service::SnapshotPrepare = Arc::new(move |p: PartitionId| {
            let weak = weak.clone();
            Box::pin(async move {
                let Some(node) = weak.upgrade() else {
                    return;
                };
                ensure_snapshot(&node, p).await;
            })
        });
        self.replication
            .bind(fetch, snaps, Arc::clone(&blobs), Some(prepare));
        let install = Arc::new(InstallCtx::new(
            self.node_id,
            self.data_dir.clone(),
            self.count,
            Arc::clone(&self.transport),
            node.control(),
        ));
        let driver = FetcherDriver::new(
            self.node_id,
            Arc::clone(&self.transport),
            Arc::clone(&blobs),
            Arc::new(SuspectReporter {
                meta: Arc::downgrade(&self.meta),
                transport: Arc::clone(&self.transport),
                liveness: self.liveness.clone(),
            }),
            install,
            self.tuning.fetcher(),
        );
        self.follower.set(Arc::new(driver));
        let mut tasks = self.tasks.lock().unwrap_or_else(|e| e.into_inner());
        for p in self.count.ids() {
            let Some(role) = node.role(p) else {
                continue;
            };
            tasks.push(tokio::spawn(run_isr_publisher(
                p,
                role,
                Arc::downgrade(node),
                Arc::clone(&self.meta),
                self.assignment_watch(),
                Arc::clone(&self.clock),
                self.tuning,
                self.tasks_stop.clone(),
            )));
        }
        tasks.push(tokio::spawn(run_module_sync(
            Arc::downgrade(&self.meta),
            blobs,
            Arc::clone(&self.transport),
            self.node_id,
            self.tasks_stop.clone(),
        )));
    }

    /// The `GET /v1/admin/replication` document.
    pub fn status_document(&self, node: &Node) -> serde_json::Value {
        let st = self.meta.raft().status();
        let meta = MetaSummary {
            leader: st.leader_id.unwrap_or(0),
            term: st.term,
            voters: st.voters,
            learners: st.learners,
        };
        let records: BTreeMap<u16, ostro_meta::records::PartitionAssignment> =
            self.meta.view().read(|s: &MetaState| {
                s.partitions()
                    .map(|v| (v.value.partition, v.value.clone()))
                    .collect()
            });
        let parts: Vec<PartitionInput> = self
            .count
            .ids()
            .map(|p| {
                let rec = records.get(&p.0);
                PartitionInput {
                    partition: p.0,
                    status: node.handle(p).map(|h| {
                        use ostro_log::ReplicatedLog as _;
                        h.log().replica_status()
                    }),
                    leader: rec.map_or(0, |r| r.leader),
                    replicas: rec.map(|r| r.replicas.clone()).unwrap_or_default(),
                }
            })
            .collect();
        status::document(self.node_id, &meta, &parts, self.clock.now_ms())
    }

    /// The server-local `GET /v1/admin/replication` route (like `/metrics`).
    pub fn status_router(self: &Arc<Self>, node: &Arc<Node>) -> axum::Router {
        let cs = Arc::downgrade(self);
        let node = Arc::downgrade(node);
        axum::Router::new().route(
            "/v1/admin/replication",
            axum::routing::get(move || {
                let (cs, node) = (cs.clone(), node.clone());
                async move {
                    use axum::response::IntoResponse as _;
                    match (cs.upgrade(), node.upgrade()) {
                        (Some(cs), Some(node)) => {
                            axum::Json(cs.status_document(&node)).into_response()
                        }
                        _ => (
                            axum::http::StatusCode::SERVICE_UNAVAILABLE,
                            "the node is shutting down",
                        )
                            .into_response(),
                    }
                }
            }),
        )
    }

    /// Stop the tasks that hold meta or partition logs (before `Node::shutdown`).
    pub async fn stop_tasks(&self) {
        self.tasks_stop.cancel();
        self.follower.clear();
        self.replication.unbind();
        let tasks = std::mem::take(&mut *self.tasks.lock().unwrap_or_else(|e| e.into_inner()));
        for t in tasks {
            if let Err(e) = t.await
                && e.is_panic()
            {
                warn!(error = %e, "a cluster task panicked");
            }
        }
    }

    /// Stop the cluster listener(s) (after meta stopped).
    pub async fn stop_listener(&self) {
        self.listener_stop.cancel();
        let tasks = std::mem::take(
            &mut *self
                .listener_tasks
                .lock()
                .unwrap_or_else(|e| e.into_inner()),
        );
        for t in tasks {
            let _ = t.await;
        }
    }

    /// Stop everything but meta, and hand meta back (startup failure paths and shutdown).
    pub async fn into_meta(self) -> (Arc<MetaHandle>, ClusterRemainder) {
        self.stop_tasks().await;
        let ClusterServices {
            meta,
            listener_tasks,
            listener_stop,
            assignments,
            ..
        } = self;
        assignments.close();
        (
            meta,
            ClusterRemainder {
                listener_tasks: listener_tasks.into_inner().unwrap_or_default(),
                listener_stop,
            },
        )
    }
}

/// What remains of [`ClusterServices`] after [`ClusterServices::into_meta`]: the listener,
/// stopped after meta.
pub struct ClusterRemainder {
    listener_tasks: Vec<JoinHandle<()>>,
    listener_stop: CancellationToken,
}

impl ClusterRemainder {
    /// Stop the listener(s).
    pub async fn stop(self) {
        self.listener_stop.cancel();
        for t in self.listener_tasks {
            let _ = t.await;
        }
    }
}

/// Make sure `p` has a local snapshot covering its log start before a fresh
/// `FetchSnapshot` stream (the snapshot context never exports by itself, so it never pins a
/// state store that a later install would need to close).
async fn ensure_snapshot(node: &Node, p: PartitionId) {
    let Some(h) = node.handle(p) else {
        return;
    };
    let log_start = h.log().log_start();
    drop(h);
    let covered = node
        .local_snapshots(p)
        .await
        .iter()
        .any(|s| s.applied_offset >= log_start);
    if !covered && let Err(e) = node.snapshot_now(p).await {
        warn!(partition = p.0, error = %e, "on-demand snapshot for a follower failed");
    }
}

// ---------------------------------------------------------------------------------------
// Assignment source
// ---------------------------------------------------------------------------------------

/// The meta-driven [`AssignmentSource`]: a watch of every partition's
/// `PartitionAssignment`, re-derived on each meta apply; fresh-term and resign requests go to
/// the controller helpers (12-10). While self-demoted, partitions this node leads are
/// published leaderless (module docs).
pub struct MetaAssignments {
    node_id: u64,
    meta: Weak<MetaHandle>,
    clock: Arc<dyn Clock>,
    views: watch::Sender<Arc<Vec<PartitionAssignmentView>>>,
    demoted: watch::Sender<bool>,
    stop: CancellationToken,
}

impl std::fmt::Debug for MetaAssignments {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetaAssignments")
            .field("node_id", &self.node_id)
            .field("demoted", &*self.demoted.borrow())
            .finish_non_exhaustive()
    }
}

fn views_of(
    s: &MetaState,
    count: PartitionCount,
    me: u64,
    demoted: bool,
) -> Vec<PartitionAssignmentView> {
    count
        .ids()
        .map(|p| match s.partition(p.0) {
            Some(v) => {
                let a = &v.value;
                let leader = if demoted && a.leader == me {
                    0
                } else {
                    a.leader
                };
                PartitionAssignmentView {
                    partition: p,
                    leader,
                    epoch: Epoch(a.epoch),
                    replicas: a.replicas.clone(),
                    isr: a.isr.clone(),
                    isr_version: a.isr_version,
                    min_insync: a.min_insync,
                }
            }
            None => PartitionAssignmentView {
                partition: p,
                leader: 0,
                epoch: Epoch(0),
                replicas: Vec::new(),
                isr: Vec::new(),
                isr_version: 0,
                min_insync: 1,
            },
        })
        .collect()
}

impl MetaAssignments {
    fn start(
        node_id: u64,
        meta: &Arc<MetaHandle>,
        count: PartitionCount,
        clock: Arc<dyn Clock>,
        session_timeout: Duration,
        stop: CancellationToken,
        tasks: &mut Vec<JoinHandle<()>>,
    ) -> Arc<Self> {
        let initial = meta.view().read(|s| views_of(s, count, node_id, false));
        let src = Arc::new(MetaAssignments {
            node_id,
            meta: Arc::downgrade(meta),
            clock,
            views: watch::channel(Arc::new(initial)).0,
            demoted: watch::channel(false).0,
            stop: stop.clone(),
        });
        // Views: every meta apply and every demotion change.
        let view = meta.view().clone();
        let mut applied = view.subscribe();
        let mut demoted = src.demoted.subscribe();
        let tx = src.views.clone();
        let s = stop.clone();
        tasks.push(tokio::spawn(async move {
            loop {
                let d = *demoted.borrow_and_update();
                let next = view.read(|st| views_of(st, count, node_id, d));
                tx.send_if_modified(|cur| {
                    if **cur == next {
                        false
                    } else {
                        *cur = Arc::new(next);
                        true
                    }
                });
                tokio::select! {
                    _ = s.cancelled() => return,
                    r = applied.changed() => if r.is_err() { return },
                    r = demoted.changed() => if r.is_err() { return },
                }
            }
        }));
        // Self-demotion watchdog.
        let raft = meta.raft().clone();
        let demoted_tx = src.demoted.clone();
        let period = (session_timeout / 4).max(Duration::from_millis(20));
        tasks.push(tokio::spawn(async move {
            let mut tick = tokio::time::interval(period);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = stop.cancelled() => return,
                    _ = tick.tick() => {}
                }
                let lost = raft
                    .leader_contact_age()
                    .is_some_and(|age| age > session_timeout);
                let was = *demoted_tx.borrow();
                if lost != was {
                    demoted_tx.send_replace(lost);
                    if lost {
                        metrics::counter!("ostro_repl_self_demotions_total").increment(1);
                        warn!(
                            node = node_id,
                            ?session_timeout,
                            "lost metadata contact: led partitions step down (self-demotion)"
                        );
                    } else {
                        info!(
                            node = node_id,
                            "metadata contact back: partitions may lead again"
                        );
                    }
                }
            }
        }));
        src
    }

    fn close(&self) {
        self.stop.cancel();
    }
}

impl AssignmentSource for MetaAssignments {
    fn subscribe(&self) -> watch::Receiver<Arc<Vec<PartitionAssignmentView>>> {
        self.views.subscribe()
    }

    fn local_node(&self) -> u64 {
        self.node_id
    }

    fn request_term(&self, p: PartitionId, min_epoch: Epoch) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            if *self.demoted.borrow() {
                return;
            }
            let Some(meta) = self.meta.upgrade() else {
                return;
            };
            let req =
                controller::request_fresh_term(&meta, self.node_id, p.0, min_epoch.0, &*self.clock);
            tokio::select! {
                _ = self.stop.cancelled() => {}
                r = req => match r {
                    Ok(r) => debug!(partition = p.0, min_epoch = min_epoch.0, result = ?r, "fresh term requested"),
                    Err(e) => warn!(partition = p.0, error = %e, "fresh term request failed"),
                },
            }
        })
    }

    fn resign(&self, p: PartitionId, epoch: Epoch) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            let Some(meta) = self.meta.upgrade() else {
                return;
            };
            let req = controller::resign(&meta, self.node_id, p.0, epoch.0, &*self.clock);
            tokio::select! {
                _ = self.stop.cancelled() => {}
                r = req => if let Err(e) = r {
                    warn!(partition = p.0, error = %e, "resign failed");
                },
            }
        })
    }
}

// ---------------------------------------------------------------------------------------
// Follower driver (bound after Node::open) and suspect reports
// ---------------------------------------------------------------------------------------

/// The node's `FollowerDriver`: the 12-11 `FetcherDriver` once [`ClusterServices::attach_node`]
/// set it (it needs the node's blob store and install control). A follower started earlier
/// waits for it.
#[derive(Default)]
struct LateFollower {
    inner: watch::Sender<Option<Arc<FetcherDriver>>>,
}

impl LateFollower {
    fn set(&self, d: Arc<FetcherDriver>) {
        self.inner.send_replace(Some(d));
    }

    fn clear(&self) {
        self.inner.send_replace(None);
    }
}

struct PendingFollower {
    stop: CancellationToken,
    join: JoinHandle<()>,
}

impl FollowerTask for PendingFollower {
    fn stop(self: Box<Self>) -> futures_util_shim::BoxFuture<'static, ()> {
        Box::pin(async move {
            self.stop.cancel();
            let _ = self.join.await;
        })
    }
}

impl FollowerDriver for LateFollower {
    fn start(
        &self,
        p: PartitionId,
        epoch: Epoch,
        leader: u64,
        log: Arc<PartitionLogHandle>,
    ) -> Box<dyn FollowerTask> {
        let mut rx = self.inner.subscribe();
        let stop = CancellationToken::new();
        let s = stop.clone();
        let join = tokio::spawn(async move {
            let driver = loop {
                if let Some(d) = rx.borrow_and_update().clone() {
                    break d;
                }
                tokio::select! {
                    _ = s.cancelled() => return,
                    r = rx.changed() => if r.is_err() { return },
                }
            };
            let task = driver.start(p, epoch, leader, log);
            s.cancelled().await;
            task.stop().await;
        });
        Box::new(PendingFollower { stop, join })
    }
}

/// The follower's suspect reports: to the local liveness when this node leads meta, else
/// `MetaPeer.ReportSuspect` to the current meta leader.
struct SuspectReporter {
    meta: Weak<MetaHandle>,
    transport: Arc<dyn ClusterTransport>,
    liveness: LivenessHandle,
}

impl ostro_cluster::liveness::SuspectSink for SuspectReporter {
    fn report(&self, reporter: u64, suspect: u64, partition: Option<u16>) {
        let Some(meta) = self.meta.upgrade() else {
            return;
        };
        let st = meta.raft().status();
        if st.is_leader {
            self.liveness.report_suspect(reporter, suspect);
            return;
        }
        let Some(leader) = st.leader_id.filter(|l| *l != suspect) else {
            return;
        };
        let Ok(ch) = self.transport.channel(leader) else {
            return;
        };
        tokio::spawn(async move {
            let _ = repl_service::report_suspect(
                ch,
                reporter,
                suspect,
                partition,
                "fetch connection refused or reset",
            )
            .await;
        });
    }
}

// ---------------------------------------------------------------------------------------
// ISR publishers and module-blob sync
// ---------------------------------------------------------------------------------------

/// One `IsrPublisher` per partition while this node leads it or takes it over (the
/// takeover's `LeaderAnnounced` may wait for `min_insync`, which only an ISR expansion can
/// satisfy); stopped when the leading epoch changes or ends.
#[allow(clippy::too_many_arguments)]
async fn run_isr_publisher(
    p: PartitionId,
    mut role: watch::Receiver<PartitionRole>,
    node: Weak<Node>,
    meta: Arc<MetaHandle>,
    assignments: watch::Receiver<Arc<Vec<PartitionAssignmentView>>>,
    clock: Arc<dyn Clock>,
    tuning: ReplicationTuning,
    stop: CancellationToken,
) {
    let mut running: Option<(Epoch, CancellationToken, JoinHandle<()>)> = None;
    loop {
        let leading = role.borrow_and_update().leading_epoch();
        if running
            .as_ref()
            .is_some_and(|(e, _, j)| Some(*e) != leading || j.is_finished())
            && let Some((_, tok, join)) = running.take()
        {
            tok.cancel();
            let _ = join.await;
        }
        if running.is_none()
            && let Some(e) = leading
            && let Some(log) = node
                .upgrade()
                .and_then(|n| n.handle(p))
                .map(|h| Arc::clone(h.log()))
        {
            let tok = stop.child_token();
            let isr_meta: Arc<dyn IsrMeta> = Arc::clone(&meta) as Arc<dyn IsrMeta>;
            let join = tokio::spawn({
                let tok = tok.clone();
                let assignments = assignments.clone();
                let clock = Arc::clone(&clock);
                async move {
                    let _ = IsrPublisher::run(
                        p,
                        log,
                        isr_meta,
                        assignments,
                        clock,
                        tuning.isr_publisher(),
                        tok,
                    )
                    .await;
                }
            });
            running = Some((e, tok, join));
        }
        tokio::select! {
            _ = stop.cancelled() => break,
            r = role.changed() => if r.is_err() { break },
            // A publisher that exited by itself (lost leadership) is restarted on the next
            // role change; poll so a same-epoch exit is noticed too.
            _ = tokio::time::sleep(Duration::from_millis(500)) => {}
        }
    }
    if let Some((_, tok, join)) = running {
        tok.cancel();
        let _ = join.await;
    }
}

/// The module-blob sync task (D-P12-22): every `Module` record whose blob is missing locally
/// is fetched from a member (`FetchBlob`, sha-verified by the blob store) so a node that
/// becomes a leader already holds every module.
async fn run_module_sync(
    meta: Weak<MetaHandle>,
    blobs: Arc<BlobStore>,
    transport: Arc<dyn ClusterTransport>,
    node_id: u64,
    stop: CancellationToken,
) {
    let Some(view) = meta.upgrade().map(|m| m.view().clone()) else {
        return;
    };
    let mut applied = view.subscribe();
    let mut retry = tokio::time::interval(Duration::from_secs(2));
    retry.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        let (missing, members): (Vec<[u8; 32]>, Vec<u64>) = view.read(|s| {
            (
                s.modules()
                    .map(|m| m.value.sha256)
                    .filter(|sha| !ostro_cluster::blob::has_blob(&blobs, sha))
                    .collect(),
                s.members()
                    .map(|m| m.value.node_id)
                    .filter(|n| *n != node_id)
                    .collect(),
            )
        });
        for sha in missing {
            for m in &members {
                let Ok(ch) = transport.channel(*m) else {
                    continue;
                };
                match ostro_cluster::blob::fetch_blob_into(ch, &blobs, sha).await {
                    Ok(()) => {
                        debug!(member = m, "module blob synced");
                        break;
                    }
                    Err(e) => debug!(member = m, error = %e, "module blob not fetched"),
                }
            }
        }
        tokio::select! {
            _ = stop.cancelled() => return,
            r = applied.changed() => if r.is_err() { return },
            _ = retry.tick() => {}
        }
    }
}

/// [`crate::control::LeaderHints`] over the node's roles and meta: a partition this node
/// does not lead (follower, offline, taking over, self-demoted) answers with the leader meta
/// names and its API address (`Member.addr`).
pub struct NodeLeaderHints {
    node: Weak<Node>,
    view: ostro_meta::MetaView,
}

impl crate::control::LeaderHints for NodeLeaderHints {
    fn not_leader(&self, p: PartitionId) -> Option<ostro_api::ApiError> {
        let node = self.node.upgrade()?;
        let role = node.role(p).map(|r| *r.borrow())?;
        // A leader stepping down leaves the led set first; its role changes only after its
        // services stopped (which can take the dispatcher's grace period).
        let stepping_down = || {
            !ostro_partition::roles::PartitionRoles::led(&*node)
                .borrow()
                .iter()
                .any(|(q, _)| *q == p)
        };
        if matches!(role, PartitionRole::Leader { .. }) && !stepping_down() {
            return None;
        }
        let (leader, addr) = self.view.read(|s| {
            let leader = s.partition(p.0).map(|a| a.value.leader).filter(|l| *l != 0);
            let addr = leader
                .and_then(|l| s.member(l))
                .map(|m| m.value.addr.clone())
                .filter(|a| !a.is_empty());
            (leader, addr)
        });
        let leader = leader.or_else(|| node.router().leader_hint(p).filter(|l| *l != 0));
        Some(crate::control::not_leader_error(p, leader, addr))
    }
}

impl ClusterServices {
    /// The API's leader hints for `node`.
    pub fn leader_hints(&self, node: &Arc<Node>) -> Arc<dyn crate::control::LeaderHints> {
        Arc::new(NodeLeaderHints {
            node: Arc::downgrade(node),
            view: self.meta.view().clone(),
        })
    }
}

/// [`crate::control::ModulePush`] over the cluster transport (D-P12-22).
struct ClusterModulePush {
    transport: Arc<dyn ClusterTransport>,
    targets: Box<dyn Fn() -> (usize, Vec<u64>) + Send + Sync>,
}

impl crate::control::ModulePush for ClusterModulePush {
    fn push<'a>(
        &'a self,
        blobs: &'a Arc<BlobStore>,
        sha: [u8; 32],
    ) -> futures_util_shim::BoxFuture<'a, Result<(), String>> {
        Box::pin(async move {
            let (k, candidates) = (self.targets)();
            if k == 0 {
                return Ok(());
            }
            let results =
                ostro_cluster::blob::push_to(&*self.transport, &candidates, blobs, sha).await;
            let ok = results.iter().filter(|(_, r)| r.is_ok()).count();
            if ok >= k {
                return Ok(());
            }
            let errs: Vec<String> = results
                .iter()
                .filter_map(|(n, r)| r.as_ref().err().map(|e| format!("node {n}: {e}")))
                .collect();
            Err(format!(
                "{ok} of the {k} required peers stored the blob ({})",
                errs.join("; ")
            ))
        })
    }
}

impl ClusterServices {
    /// The API's module-blob replication (`k = min(min_insync, live members) − 1`).
    pub fn module_push(self: &Arc<Self>) -> Arc<dyn crate::control::ModulePush> {
        let me = Arc::downgrade(self);
        Arc::new(ClusterModulePush {
            transport: Arc::clone(&self.transport),
            targets: Box::new(move || match me.upgrade() {
                Some(cs) => cs.module_push_plan(),
                None => (0, Vec::new()),
            }),
        })
    }

    /// `(k, candidates)`: how many peers must store a module blob, and which peers to try
    /// (live peers when this node leads meta and knows liveness, else every other voter).
    pub fn module_push_plan(&self) -> (usize, Vec<u64>) {
        let min_insync = usize::from(
            self.tuning
                .effective_min_insync(self.tuning.effective_rf(self.voters.len())),
        );
        let universe: std::collections::BTreeSet<u64> = self.voters.iter().copied().collect();
        let others = |set: Vec<u64>| -> Vec<u64> {
            set.into_iter().filter(|n| *n != self.node_id).collect()
        };
        let live = if self.meta.is_leader() {
            others(
                self.liveness
                    .lock()
                    .live_set(&universe)
                    .into_iter()
                    .collect(),
            )
        } else {
            others(self.voters.clone())
        };
        let k = min_insync.min(live.len() + 1).saturating_sub(1);
        (k, live)
    }
}

/// The role of `p` on `node`, for the API's not-leader answers.
pub fn role_of(node: &Node, p: PartitionId) -> Option<PartitionRole> {
    node.role(p).map(|r| *r.borrow())
}
