//! ostro-cluster: Phase 12 replication below the control plane (CLUS-01..04).
//!
//! Every module is declared here once by 12-02, so later plans own disjoint files and never
//! edit this list:
//!
//! | module | owner | contents |
//! |--------|-------|----------|
//! | [`transport`] | 12-02 / 12-05 | [`transport::ClusterTransport`], [`transport::PeerNode`]; `tcp` = mTLS TCP transport (12-05) |
//! | [`authz`] | 12-05 | `require_node`: the calling node id from the peer certificate |
//! | [`address_book`] | 12-05 | node id → cluster/replication address |
//! | [`meta_peer`] | 12-05 | `MetaPeer` service (Raft batches, Propose forwarding, ReportSuspect) |
//! | [`controller`] | 12-10 | partition controller on the meta leader (placement, elections, ISR) |
//! | [`liveness`] | 12-10 | liveness sampling + [`liveness::SuspectSink`] |
//! | [`bootstrap`] | 12-10 | static-cluster bootstrap placement (epoch 0 → 1) |
//! | [`members`] | 12-10 | own `Member` upsert |
//! | [`fetch`] | 12-11 | leader side of `Replication.Fetch` |
//! | [`fetcher`] | 12-11 | follower fetch loop (`FollowerDriver`) |
//! | [`blob`] | 12-11 | `FetchBlob` / `PushBlob` |
//! | [`isr_publisher`] | 12-11 | leader-side `AlterIsr` proposals |
//! | [`snapshot`] | 12-12 | `FetchSnapshot` streaming |
//! | [`install`] | 12-12 | follower snapshot install (reset log, then install state) |
//! | [`service`] | 12-14 | the tonic `Replication` service |
//! | [`status`] | 12-14 | `GET /v1/admin/replication` document |
//! | [`config`] | 12-14 | cluster runtime config derived from `[cluster]` / `[replication]` |
//! | [`identity`] | 12-14 | cluster-id resolution and meta voter checks |
//!
//! Phase 13 modules, declared once by 13-01 (this list is final after 13-01):
//!
//! | module | owner | contents |
//! |--------|-------|----------|
//! | [`contracts`] | 13-01 (final) | `LevelSource`, `HandoffClient`, `HandoffExecutor`, `ModuleRefsSource`, `ClusterStatusView` |
//! | [`compat`] | 13-04 | boot / running fences, feature gate, NodeVersion advertiser |
//! | [`upgrade`] | 13-04 | finalization decision, auto-finalizer, finalize / hold / release |
//! | [`membership`] | 13-06 (`reconcile`), 13-09 (`service`, `remove`) | dynamic membership |
//! | [`placement`] | 13-06 (`plan`, `reassign`, `leaders`), 13-10 (`controller`) | reassignment |
//! | [`handoff`] | 13-11 | `LocalHandoff`, `NodeOpsHandoffClient` |
//! | [`shutdown`] | 13-11 | controlled shutdown |

pub mod address_book;
pub mod authz;
pub mod blob;
pub mod bootstrap;
pub mod compat;
pub mod config;
pub mod contracts;
pub mod controller;
pub mod fetch;
pub mod fetcher;
pub mod handoff;
pub mod identity;
pub mod install;
pub mod isr_publisher;
pub mod liveness;
pub mod members;
pub mod membership;
pub mod meta_peer;
pub mod placement;
pub mod service;
pub mod shutdown;
pub mod snapshot;
pub mod status;
pub mod transport;
pub mod upgrade;

// Dependencies declared once by plan 12-01 for later Phase 12 plans (12-05 meta peer/authz,
// 12-10 controller, 12-11 fetch, 12-12 install, 12-14 service). Anchored so cargo's
// unused-dependency lint stays quiet until the owning modules consume them.
mod dep_anchors {
    use ostro_meta as _;
    use ostro_partition as _;
    use ostro_proto as _;
    use x509_parser as _;
}
