//! Phase 13: implemented in 13-09 (the marker-first joiner, the boot rule and both voter
//! checks; D-P13-9, D-P13-29).
//!
//! Fixed signatures (called from [`super`]). The stubs keep the Phase 12 behaviour exactly:
//! no marker is ever read, so no node is a joiner; [`boot_mode`] is the 12-14 boot rule and
//! [`voter_check`] the two 12-14 voter checks; `ostro cluster join` reports that it is not
//! implemented yet.

use std::path::Path;

use ostro_cluster::identity::{self, BootError};
use ostro_meta::MetaView;

use super::{
    BootMode, ConfView, DataDirProbe, JoinArgs, JoinMarker, JoinPrepared, JoinedTls,
    VoterCheckPhase,
};
use crate::config::ServerConfig;

/// Read and validate `<data>/cluster-seeds.toml`; a marker with an incomplete `<data>/tls`
/// is refused ("re-run `ostro cluster join`"). Stub: no marker.
pub fn join_identity(data_dir: &Path) -> Result<Option<JoinMarker>, BootError> {
    let _ = data_dir;
    Ok(None)
}

/// Whether `<data>/tls` holds a complete joined-node certificate set. Stub: false.
pub fn tls_complete(data_dir: &Path) -> bool {
    let _ = data_dir;
    false
}

/// A joined node's certificate set from `<data>/tls`. Stub: `None` (the Phase 12 CA path).
pub fn joined_tls(data_dir: &Path) -> Result<Option<JoinedTls>, BootError> {
    let _ = data_dir;
    Ok(None)
}

/// The boot rule. Stub: the Phase 12 rule (blank meta over partition data fail-stops with the
/// wiped-voter remedy when peers are configured, "meta dir missing" otherwise; blank meta
/// bootstraps; non-blank meta restarts).
pub fn boot_mode(cfg: &ServerConfig, probe: &DataDirProbe) -> Result<BootMode, BootError> {
    if identity::is_joiner(probe.meta_blank, probe.has_partition_data) {
        return Err(identity::joiner_error(
            !cfg.cluster.peers.is_empty(),
            &probe.meta_dir,
        ));
    }
    Ok(if probe.meta_blank {
        BootMode::Fresh
    } else {
        BootMode::Existing
    })
}

/// Whether dynamic membership is in use. Stub: false.
pub fn members_known(data_dir: &Path, view: Option<&MetaView>) -> bool {
    let _ = (data_dir, view);
    false
}

/// The configured voter set: self ∪ `[[cluster.peers]]`.
fn configured_voters(cfg: &ServerConfig) -> Vec<u64> {
    let mut v: Vec<u64> = cfg.cluster.peers.iter().map(|p| p.id).collect();
    v.push(cfg.node.node_id);
    v.sort_unstable();
    v
}

/// Both meta voter checks. Stub: the Phase 12 rules (PreStart: a non-blank persisted
/// ConfState must list exactly self ∪ peers; PostReady: the live voters must).
pub fn voter_check(
    view: &ConfView,
    cfg: &ServerConfig,
    members_known: bool,
    phase: VoterCheckPhase,
) -> Result<(), BootError> {
    let _ = members_known;
    let expected = configured_voters(cfg);
    match phase {
        VoterCheckPhase::PreStart => identity::check_meta_voters(Some(view.clone()), &expected),
        VoterCheckPhase::PostReady => identity::check_voters_after_ready(&view.voters, &expected),
    }
}

/// `ostro cluster join` (D-P13-9): handshake with the seed, then the marker, `tls/` and
/// node.toml, in that order. Stub: not implemented.
pub async fn run_join(args: JoinArgs) -> Result<JoinPrepared, String> {
    let _ = args;
    Err("`ostro cluster join` is implemented in 13-09".to_owned())
}
