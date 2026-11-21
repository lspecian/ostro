//! `self-signed` mode: `<data_dir>/tls/dev.{crt,key}`, a leaf issued by the cluster CA
//! with the CA in the chain so workers can pin `--ca-hash` (D-W20).
//!
//! SANs: the hostname, `localhost`, `127.0.0.1`, `::1` and the listen IP (unless it is
//! unspecified). Unlike node certificates the dev cert carries no `ostro://` identity.

use std::collections::BTreeSet;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};

use rcgen::ExtendedKeyUsagePurpose;

use super::TlsError;
use super::ca::{CertifiedKeyPair, ClusterCa, LEAF_LIFETIME, San};

/// The machine's hostname, when it is a usable DNS name.
pub fn hostname() -> Option<String> {
    let raw = std::fs::read_to_string("/proc/sys/kernel/hostname")
        .or_else(|_| std::fs::read_to_string("/etc/hostname"))
        .ok()
        .or_else(|| std::env::var("HOSTNAME").ok())?;
    match San::from_host(raw.trim()) {
        Some(San::Dns(h)) => Some(h),
        _ => None,
    }
}

/// The dev cert SANs for a listener bound to `listen` (plus `extra` names).
pub fn sans(listen: SocketAddr, extra: &[String]) -> Vec<San> {
    let mut set: BTreeSet<San> = extra.iter().filter_map(|s| San::from_host(s)).collect();
    if let Some(h) = hostname() {
        set.insert(San::Dns(h));
    }
    set.insert(San::Dns("localhost".to_owned()));
    set.insert(San::Ip(IpAddr::from([127, 0, 0, 1])));
    set.insert(San::Ip(IpAddr::from(Ipv6Addr::LOCALHOST)));
    if !listen.ip().is_unspecified() {
        set.insert(San::Ip(listen.ip()));
    }
    set.into_iter().collect()
}

/// Issue a dev leaf with `sans` (serverAuth, 1 y); chain = [leaf, CA].
pub fn issue(ca: &ClusterCa, sans: &[San]) -> Result<CertifiedKeyPair, TlsError> {
    ca.issue_leaf(
        "ostro dev",
        sans,
        &[ExtendedKeyUsagePurpose::ServerAuth],
        LEAF_LIFETIME,
    )
}

/// Reuse `<ca dir>/dev.{crt,key}` when it still matches `sans` and is valid for more than
/// 30 days (a stable fingerprint across restarts), else issue and persist a new one.
pub fn load_or_issue(ca: &ClusterCa, sans: &[San]) -> Result<CertifiedKeyPair, TlsError> {
    ca.load_or_issue("dev", sans, || issue(ca, sans))
}
