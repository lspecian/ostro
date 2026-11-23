//! Cluster-id resolution and meta voter checks (12-14, D-P12-14 / D-P12-16). All of it runs
//! before any partition data is opened, and before TLS and meta start.
//!
//! - [`resolve_cluster_id`]: configured `[cluster] cluster_id` → `node.toml` → the existing
//!   CA's stored id ([`cluster_id_from_ca_pem`]) → `ClusterInfo.cluster_id` of a non-blank
//!   persisted meta ([`probe_meta`]) → a fresh id, only when meta is blank. Two present
//!   sources that disagree are fatal.
//! - [`is_joiner`]: blank meta over existing partition data fail-stops ([`joiner_error`]).
//! - [`check_meta_voters`]: a non-blank meta whose persisted voters differ from self ∪
//!   configured peers is fatal; [`check_voters_after_ready`] repeats it on the live group.
//!
//! [`probe_meta`] reads the vendored Raft log store's files (`state`, `snapshot`,
//! `log/*.seg`) without opening the store, so nothing is written or repaired.

use std::fs;
use std::io;
use std::path::Path;

use ostro_meta::{MetaCommand, decode_command, decode_snapshot};
use ostro_raft_driver::raft::eraftpb::{ConfState, Entry, HardState, Snapshot};
use prost::Message;

/// Where the bound cluster id comes from, in D-P12-14 source order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClusterIdSource {
    /// `[cluster] cluster_id`.
    Configured(String),
    /// `node.toml`.
    NodeToml(String),
    /// The existing CA certificate's subject CN (`ostro cluster <id>`).
    StoredCa(String),
    /// `ClusterInfo.cluster_id` of a non-blank persisted meta.
    Meta(String),
    /// Nothing present and meta is blank: generate a fresh id.
    GenerateFresh,
}

impl ClusterIdSource {
    /// The id, `None` for [`ClusterIdSource::GenerateFresh`].
    pub fn id(&self) -> Option<&str> {
        match self {
            ClusterIdSource::Configured(s)
            | ClusterIdSource::NodeToml(s)
            | ClusterIdSource::StoredCa(s)
            | ClusterIdSource::Meta(s) => Some(s),
            ClusterIdSource::GenerateFresh => None,
        }
    }
}

/// The voters and learners of a persisted meta Raft `ConfState`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ConfStateView {
    /// Voter node ids.
    pub voters: Vec<u64>,
    /// Learner node ids.
    pub learners: Vec<u64>,
}

/// The remedy text for a blank meta over existing data when peers are configured (the same
/// remedy the vendored driver prints for a wiped voter, 12-10).
pub const WIPED_VOTER_REMEDY: &str = "this node's metadata Raft log is missing or blank while \
     it is a configured voter and its partition data exists (data wiped or restored): restore \
     the meta dir from backup, or wipe the whole data dir. Node removal and rejoin arrive with \
     `ostro cluster remove` / `ostro cluster join` in Phase 13";

/// A fatal identity / bootstrap check failure (the message names the remedy).
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BootError {
    /// Two present cluster-id sources disagree.
    #[error(
        "cluster id conflict: {first} says {first_id:?} but {second} says {second_id:?}; \
         fix the configuration or point the node at the right data dir"
    )]
    ClusterIdConflict {
        /// The first source.
        first: &'static str,
        /// Its id.
        first_id: String,
        /// The disagreeing source.
        second: &'static str,
        /// Its id.
        second_id: String,
    },
    /// Meta holds data but no source names the cluster id.
    #[error(
        "the metadata Raft log exists but no cluster id is known (no [cluster] cluster_id, \
         node.toml, CA or ClusterInfo); set [cluster] cluster_id"
    )]
    NoClusterId,
    /// Blank meta over existing partition data, peers configured.
    #[error("{WIPED_VOTER_REMEDY}")]
    WipedVoter,
    /// Blank meta over existing partition data, single node.
    #[error(
        "meta dir missing: {meta_dir} is blank but the data dir holds partition data; restore \
         the meta dir from backup, or wipe the whole data dir"
    )]
    MetaDirMissing {
        /// The meta dir.
        meta_dir: String,
    },
    /// The persisted voters differ from self ∪ configured peers.
    #[error(
        "changing the voter set: the metadata Raft log has voters {persisted:?} but the \
         configuration says {expected:?}; grow via Phase 13 `ostro cluster` commands"
    )]
    VoterMismatch {
        /// Persisted voters.
        persisted: Vec<u64>,
        /// Configured voters.
        expected: Vec<u64>,
    },
    /// After meta became ready its voters differ from the configured set.
    #[error(
        "the running metadata group has voters {actual:?} but the configuration says \
         {expected:?}; grow via Phase 13 `ostro cluster` commands"
    )]
    VotersAfterReady {
        /// Live voters.
        actual: Vec<u64>,
        /// Configured voters.
        expected: Vec<u64>,
    },
    /// `ClusterInfo.cluster_id` differs from the bound id after meta became ready.
    #[error(
        "cluster id mismatch: the metadata says {stored:?} but this node is bound to {bound:?}"
    )]
    ClusterInfoMismatch {
        /// `ClusterInfo.cluster_id`.
        stored: String,
        /// The bound id.
        bound: String,
    },
    /// The persisted meta files could not be read.
    #[error("reading the persisted metadata in {dir}: {reason}")]
    MetaRead {
        /// The meta dir.
        dir: String,
        /// What failed.
        reason: String,
    },
}

/// Resolve the cluster id: configured → node.toml → stored CA → persisted meta → fresh
/// (only when meta is blank). Two present sources that disagree are fatal.
pub fn resolve_cluster_id(
    configured: Option<&str>,
    node_toml: Option<&str>,
    stored_ca: Option<&str>,
    persisted_cluster_info: Option<&str>,
    meta_blank: bool,
) -> Result<ClusterIdSource, BootError> {
    let present: Vec<(&'static str, &str)> = [
        ("[cluster] cluster_id", configured),
        ("node.toml", node_toml),
        ("the cluster CA", stored_ca),
        ("the metadata ClusterInfo", persisted_cluster_info),
    ]
    .into_iter()
    .filter_map(|(n, v)| v.filter(|v| !v.trim().is_empty()).map(|v| (n, v)))
    .collect();
    if let Some((first, first_id)) = present.first()
        && let Some((second, second_id)) = present.iter().find(|(_, v)| v != first_id)
    {
        return Err(BootError::ClusterIdConflict {
            first,
            first_id: (*first_id).to_owned(),
            second,
            second_id: (*second_id).to_owned(),
        });
    }
    let own = |v: Option<&str>| v.filter(|v| !v.trim().is_empty()).map(str::to_owned);
    if let Some(v) = own(configured) {
        return Ok(ClusterIdSource::Configured(v));
    }
    if let Some(v) = own(node_toml) {
        return Ok(ClusterIdSource::NodeToml(v));
    }
    if let Some(v) = own(stored_ca) {
        return Ok(ClusterIdSource::StoredCa(v));
    }
    if let Some(v) = own(persisted_cluster_info) {
        return Ok(ClusterIdSource::Meta(v));
    }
    if meta_blank {
        Ok(ClusterIdSource::GenerateFresh)
    } else {
        Err(BootError::NoClusterId)
    }
}

/// A blank meta over existing partition data is a joiner / wiped voter (fail-stop).
pub fn is_joiner(meta_blank: bool, partition_data_exists: bool) -> bool {
    meta_blank && partition_data_exists
}

/// The fail-stop for a joiner: the wiped-voter remedy with peers, "meta dir missing"
/// without.
pub fn joiner_error(has_peers: bool, meta_dir: &Path) -> BootError {
    if has_peers {
        BootError::WipedVoter
    } else {
        BootError::MetaDirMissing {
            meta_dir: meta_dir.display().to_string(),
        }
    }
}

fn sorted(v: &[u64]) -> Vec<u64> {
    let mut v = v.to_vec();
    v.sort_unstable();
    v.dedup();
    v
}

/// A non-blank meta whose voters differ from `expected` (self ∪ configured peers) is fatal
/// ("grow via Phase 13"). `None` (or an empty persisted voter set) = blank meta.
pub fn check_meta_voters(
    persisted: Option<ConfStateView>,
    expected: &[u64],
) -> Result<(), BootError> {
    let Some(cs) = persisted else {
        return Ok(());
    };
    if cs.voters.is_empty() {
        return Ok(());
    }
    let (p, e) = (sorted(&cs.voters), sorted(expected));
    if p != e {
        return Err(BootError::VoterMismatch {
            persisted: p,
            expected: e,
        });
    }
    Ok(())
}

/// After meta is ready: the live voters must equal the configured set.
pub fn check_voters_after_ready(actual: &[u64], expected: &[u64]) -> Result<(), BootError> {
    let (a, e) = (sorted(actual), sorted(expected));
    if a != e {
        return Err(BootError::VotersAfterReady {
            actual: a,
            expected: e,
        });
    }
    Ok(())
}

/// After meta is ready: `ClusterInfo.cluster_id` must equal the bound id (W2).
pub fn check_cluster_info(stored: Option<&str>, bound: &str) -> Result<(), BootError> {
    match stored {
        Some(s) if s != bound => Err(BootError::ClusterInfoMismatch {
            stored: s.to_owned(),
            bound: bound.to_owned(),
        }),
        _ => Ok(()),
    }
}

/// The cluster id stored in a CA certificate (PEM): its subject CN `ostro cluster <id>`.
/// `None` when the PEM holds no parseable certificate or another CN shape.
pub fn cluster_id_from_ca_pem(pem: &str) -> Option<String> {
    let (_, pem) = x509_parser::pem::parse_x509_pem(pem.as_bytes()).ok()?;
    let cert = pem.parse_x509().ok()?;
    let cn = cert.subject().iter_common_name().next()?.as_str().ok()?;
    cn.strip_prefix("ostro cluster ")
        .filter(|id| !id.is_empty())
        .map(str::to_owned)
}

/// The cluster id of `<tls_dir>/ca.crt`, if the file exists.
pub fn stored_ca_cluster_id(tls_dir: &Path) -> Option<String> {
    let pem = fs::read_to_string(tls_dir.join("ca.crt")).ok()?;
    cluster_id_from_ca_pem(&pem)
}

/// What the persisted metadata Raft log holds (read without opening the store).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MetaProbe {
    /// No Raft state was ever persisted (no `state` / `snapshot` file).
    pub blank: bool,
    /// The persisted `ConfState` (from `state`, else from the snapshot metadata).
    pub conf: Option<ConfStateView>,
    /// `ClusterInfo.cluster_id` (snapshot state, else the first committed `InitCluster`).
    pub cluster_id: Option<String>,
}

const STATE_MAGIC: &[u8; 4] = b"CRS1";
const SNAPSHOT_MAGIC: &[u8; 4] = b"CRN1";

fn read_opt(path: &Path) -> io::Result<Option<Vec<u8>>> {
    match fs::read(path) {
        Ok(b) => Ok(Some(b)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

fn le_u32(b: &[u8], at: usize) -> Option<usize> {
    let s = b.get(at..at + 4)?;
    Some(u32::from_le_bytes([s[0], s[1], s[2], s[3]]) as usize)
}

/// Probe `meta_dir` read-only (see [`MetaProbe`]).
pub fn probe_meta(meta_dir: &Path) -> Result<MetaProbe, BootError> {
    let bad = |reason: String| BootError::MetaRead {
        dir: meta_dir.display().to_string(),
        reason,
    };
    let io_err = |e: io::Error| bad(e.to_string());
    let state = read_opt(&meta_dir.join("state")).map_err(io_err)?;
    let snapshot = read_opt(&meta_dir.join("snapshot")).map_err(io_err)?;
    let mut probe = MetaProbe {
        blank: state.is_none() && snapshot.is_none(),
        ..MetaProbe::default()
    };
    let mut commit = 0;
    if let Some(b) = &state {
        // CRS1 [len][HardState][len][ConfState][crc]
        if b.get(..4) != Some(STATE_MAGIC.as_slice()) {
            return Err(bad("state: bad magic".into()));
        }
        let hs_len = le_u32(b, 4).ok_or_else(|| bad("state: truncated".into()))?;
        let hs = b
            .get(8..8 + hs_len)
            .ok_or_else(|| bad("state: truncated".into()))?;
        let hs = HardState::decode(hs).map_err(|e| bad(format!("state: {e}")))?;
        commit = hs.commit;
        let at = 8 + hs_len;
        let cs_len = le_u32(b, at).ok_or_else(|| bad("state: truncated".into()))?;
        let cs = b
            .get(at + 4..at + 4 + cs_len)
            .ok_or_else(|| bad("state: truncated".into()))?;
        let cs = ConfState::decode(cs).map_err(|e| bad(format!("state: {e}")))?;
        probe.conf = Some(ConfStateView {
            voters: cs.voters,
            learners: cs.learners,
        });
    }
    let mut snapshot_index = 0;
    if let Some(b) = &snapshot {
        if b.get(..4) != Some(SNAPSHOT_MAGIC.as_slice()) {
            return Err(bad("snapshot: bad magic".into()));
        }
        let body = snapshot_body(b).ok_or_else(|| bad("snapshot: truncated".into()))?;
        let snap = Snapshot::decode(body).map_err(|e| bad(format!("snapshot: {e}")))?;
        if let Some(meta) = &snap.metadata {
            snapshot_index = meta.index;
            if probe.conf.as_ref().is_none_or(|c| c.voters.is_empty())
                && let Some(cs) = &meta.conf_state
            {
                probe.conf = Some(ConfStateView {
                    voters: cs.voters.clone(),
                    learners: cs.learners.clone(),
                });
            }
        }
        if snapshot_index > 0
            && !snap.data.is_empty()
            && let Ok(state) = decode_snapshot(&snap.data)
        {
            probe.cluster_id = state.cluster().map(|c| c.value.cluster_id.clone());
        }
    }
    if probe.cluster_id.is_none() && commit > snapshot_index {
        probe.cluster_id = first_init_cluster(meta_dir, snapshot_index, commit).map_err(io_err)?;
    }
    Ok(probe)
}

/// The snapshot file body: `CRN1`, an 8-byte length, the encoded `Snapshot`, a CRC.
fn snapshot_body(b: &[u8]) -> Option<&[u8]> {
    let s = b.get(4..12)?;
    let len = u64::from_le_bytes([s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7]]);
    b.get(12..12usize.checked_add(usize::try_from(len).ok()?)?)
}

/// Scan the log segments in index order for the first committed `InitCluster` above the
/// snapshot (it is one of the first entries of a cluster's log).
fn first_init_cluster(meta_dir: &Path, after: u64, commit: u64) -> io::Result<Option<String>> {
    let mut segs: Vec<(u64, std::path::PathBuf)> = Vec::new();
    match fs::read_dir(meta_dir.join("log")) {
        Ok(rd) => {
            for d in rd {
                let path = d?.path();
                if let Some(first) = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .and_then(|n| n.strip_suffix(".seg"))
                    .and_then(|n| n.parse::<u64>().ok())
                {
                    segs.push((first, path));
                }
            }
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    }
    segs.sort();
    for (_, path) in segs {
        let b = fs::read(&path)?;
        let mut at = 0;
        while let Some(len) = le_u32(&b, at) {
            let Some(body) = b.get(at + 8..at + 8 + len) else {
                break; // torn tail
            };
            at += 8 + len;
            let Ok(e) = Entry::decode(body) else {
                break;
            };
            if e.index <= after || e.index > commit || e.entry_type != 0 || e.data.is_empty() {
                continue;
            }
            if let Ok(MetaCommand::InitCluster { cluster_id, .. }) = decode_command(&e.data) {
                return Ok(Some(cluster_id));
            }
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_order_and_conflicts() {
        assert_eq!(
            resolve_cluster_id(Some("a"), None, Some("a"), None, false).unwrap(),
            ClusterIdSource::Configured("a".into())
        );
        assert_eq!(
            resolve_cluster_id(None, Some("b"), None, Some("b"), false).unwrap(),
            ClusterIdSource::NodeToml("b".into())
        );
        assert_eq!(
            resolve_cluster_id(None, None, Some("c"), None, false).unwrap(),
            ClusterIdSource::StoredCa("c".into())
        );
        assert_eq!(
            resolve_cluster_id(None, None, None, Some("d"), false).unwrap(),
            ClusterIdSource::Meta("d".into())
        );
        assert_eq!(
            resolve_cluster_id(None, None, None, None, true).unwrap(),
            ClusterIdSource::GenerateFresh
        );
        assert!(matches!(
            resolve_cluster_id(None, None, None, None, false),
            Err(BootError::NoClusterId)
        ));
        let e = resolve_cluster_id(Some("a"), Some("b"), None, None, true).unwrap_err();
        assert!(e.to_string().contains("conflict"), "{e}");
        let e = resolve_cluster_id(None, Some("a"), Some("x"), None, false).unwrap_err();
        assert!(matches!(e, BootError::ClusterIdConflict { .. }));
    }

    #[test]
    fn voter_checks() {
        check_meta_voters(None, &[1, 2, 3]).unwrap();
        check_meta_voters(
            Some(ConfStateView {
                voters: vec![3, 1, 2],
                learners: vec![],
            }),
            &[1, 2, 3],
        )
        .unwrap();
        let e = check_meta_voters(
            Some(ConfStateView {
                voters: vec![1],
                learners: vec![],
            }),
            &[1, 2, 3],
        )
        .unwrap_err();
        assert!(e.to_string().contains("grow via Phase 13"), "{e}");
        assert!(check_voters_after_ready(&[1, 2], &[2, 1]).is_ok());
        assert!(check_voters_after_ready(&[1], &[2, 1]).is_err());
        assert!(check_cluster_info(Some("a"), "b").is_err());
        assert!(check_cluster_info(Some("a"), "a").is_ok());
        assert!(is_joiner(true, true) && !is_joiner(true, false) && !is_joiner(false, true));
        assert!(
            joiner_error(true, Path::new("m"))
                .to_string()
                .contains("ostro cluster")
        );
        assert!(
            joiner_error(false, Path::new("m"))
                .to_string()
                .contains("meta dir missing")
        );
    }

    #[test]
    fn blank_probe() {
        let d = tempfile::tempdir().unwrap();
        let p = probe_meta(&d.path().join("meta")).unwrap();
        assert!(p.blank && p.conf.is_none() && p.cluster_id.is_none());
    }
}
