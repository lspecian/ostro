//! The Phase 13 harness API (13-02, D-P13-25): [`P13Harness`] extends [`ClusterHarness`]
//! with single-node start, cluster promotion, joins through the real join flow, removal,
//! drain, emulated rolling upgrades and the cluster status. 13-08 implements it for
//! [`super::inproc::InProcCluster`] (tier B) and [`super::process::ProcCluster`] (tier C);
//! the Phase 13 scenarios code against it.

use std::future::Future;

use super::{ClusterHarness, ClusterSpec, HarnessError};

/// How [`P13Harness::add_node`] starts a joining node.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NodeOpts {
    /// Emulated `level_max` (tier B: `ServerOptions::level_max_override`; tier C:
    /// `OSTRO_TEST_LEVEL_MAX`); `None` = this build's level.
    pub level_max: Option<u32>,
    /// Pass `--advertise-*` addresses to the join (tier C) / set `[cluster] advertise`.
    pub advertise: bool,
}

/// A cluster harness with dynamic membership and upgrades (Phase 13).
pub trait P13Harness: ClusterHarness {
    /// Start one server-profile node (`spec.nodes == 1`, no `[[cluster.peers]]`, cluster
    /// listener bound, `[cluster] advertise` set).
    fn start_single(spec: ClusterSpec) -> impl Future<Output = Result<Self, HarnessError>>
    where
        Self: Sized;

    /// `POST /v1/cluster/init` on `node`; the status document.
    fn cluster_init(
        &mut self,
        node: u64,
    ) -> impl Future<Output = Result<serde_json::Value, HarnessError>>;

    /// A new node through the real join flow (`POST /v1/cluster/tokens`, the joiner, then
    /// the server); its allocated id.
    fn add_node(&mut self, opts: NodeOpts) -> impl Future<Output = Result<u64, HarnessError>>;

    /// `POST /v1/cluster/members/{id}/remove`, then wait (bounded) until `id` is Removed.
    fn remove(&mut self, id: u64, force: bool) -> impl Future<Output = Result<(), HarnessError>>;

    /// `POST /v1/cluster/members/{id}/drain`, then wait (bounded) until `id` holds no
    /// replica.
    fn drain(&mut self, id: u64) -> impl Future<Output = Result<(), HarnessError>>;

    /// Restart `node` from its data dir at an emulated level (`None` = this build's).
    fn upgrade(
        &mut self,
        node: u64,
        level_max: Option<u32>,
    ) -> impl Future<Output = Result<(), HarnessError>>;

    /// `GET /v1/cluster/status` on `node`.
    fn cluster_status(
        &self,
        node: u64,
    ) -> impl Future<Output = Result<serde_json::Value, HarnessError>>;
}
