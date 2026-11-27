//! `POST /v1/cluster/init` (13-16, D-P13-11): online promotion of a running single
//! server-profile node to a cluster that other nodes can join. No restart, no export, no
//! leadership change: the partitions stay led by this node, and RF rises later through the
//! placement controller (13-10) as members join, `min_insync` only in its Shrink step.
//!
//! # Preconditions (checked in this order)
//!
//! 1. Server profile: the dev profile (`TlsProfile::of`, the profile that auto-generates
//!    the sealing key) and `--ephemeral` answer 409 `not_supported_in_profile`.
//! 2. `rf` ∈ {1, 3, 5} (default 3), `1 ≤ min_insync ≤ rf` (default `rf / 2 + 1`), else 400.
//! 3. Already a cluster (the `ReplicationPolicy`, written last by init, is present, or more
//!    than one non-Removed member exists): 200 with the current status, nothing written.
//! 4. An advertised cluster address: the request's `advertise`, else the first
//!    `[cluster] advertise` entry (with the listener port when it names only a host); none
//!    → 400. The cluster listener itself is always bound (D-P12-13).
//!
//! # Steps (each idempotent, so an init cut short is completed by running it again)
//!
//! 1. Own `Member`: `cluster_addr` = the advertised address, `MemberState` Active and this
//!    binary's `NodeVersion` (read-modify-write with CAS; unknown ext tags kept; a Removed
//!    member is refused).
//! 2. `join::ensure_cluster_ca`: `__ostro/cluster-ca` from `<data>/tls/ca.{crt,key}`
//!    (create-only). Failpoint `member::init_after_ca` stops init right after it.
//! 3. `ClusterInfo` ext `ClusterVersion` = `{finalized: level_max}` when absent (later
//!    raises go through 13-04's `upgrade::*`). A node bootstrapped on a blank data dir
//!    already holds it (`upgrade::stamp_fresh` at boot, docs/compat.md §1); only data from
//!    before that rule reaches this write.
//! 4. Node certificate: hostname SANs are not part of node-to-node authentication (the
//!    `ostro://<cluster>/node/<id>` URI is), and the cluster listener's identity is fixed at
//!    boot from `[cluster] advertise`, so an advertised host the current certificate does
//!    not cover is reported (warning + `details` in the log) rather than hot-swapped; add it
//!    to `[cluster] advertise` so workers that dial that name verify it after a restart.
//! 5. `ClusterInfo` ext `ReplicationPolicy{rf, min_insync, max_voters: 5}`: last, so its
//!    presence means "initialised" (precondition 3). It switches on RF raise and balancing
//!    in the placement controller (13-10).
//!
//! The cluster id is the one this node resolved at boot (12-14 order); init never changes
//! it (D-P13-29d).

use ostro_api::ApiError;
use ostro_api::phase13::{ClusterStatus, InitRequest};
use ostro_cluster::membership::service::member_state_of;
use ostro_meta::ext::{
    ClusterVersion, MEMBER_ACTIVE, MEMBER_REMOVED, MemberState, ReplicationPolicy,
    get_cluster_version, get_member_state, get_node_version, get_replication_policy,
    put_cluster_version, put_member_state, put_node_version, put_replication_policy,
};
use ostro_meta::{MetaCommand, MetaKey, MetaRecord, MetaResult, MetaState};
use tracing::{info, warn};

use super::P13Ctx;
use crate::tls::TlsProfile;
use crate::tls::ca::San;

/// `details.reason` of an init refused in the dev profile or on an ephemeral node.
pub const REASON_NOT_SUPPORTED_IN_PROFILE: &str = "not_supported_in_profile";
/// The failpoint right after the CA step (crash-and-rerun test).
pub const FP_INIT_AFTER_CA: &str = "member::init_after_ca";
/// Default and maximum meta voters of a promoted cluster (D-P13-8).
pub const MAX_VOTERS: u8 = 5;
/// CAS attempts of one read-modify-write step.
const CAS_ATTEMPTS: usize = 16;

#[cfg(feature = "failpoints")]
fn failpoint(name: &str) -> Result<(), ApiError> {
    match fail::eval(name, |_| ()) {
        Some(()) => Err(ApiError::unavailable(format!("failpoint {name}"))),
        None => Ok(()),
    }
}

#[cfg(not(feature = "failpoints"))]
#[inline(always)]
fn failpoint(_name: &str) -> Result<(), ApiError> {
    Ok(())
}

/// The validated replication policy of a request (precondition 2).
pub fn policy_of(r: &InitRequest) -> Result<ReplicationPolicy, ApiError> {
    let rf = r.rf.unwrap_or(3);
    if ![1, 3, 5].contains(&rf) {
        return Err(ApiError::invalid_argument(format!(
            "rf must be 1, 3 or 5, got {rf}"
        )));
    }
    let min_insync = r.min_insync.unwrap_or(rf / 2 + 1);
    if min_insync == 0 || min_insync > rf {
        return Err(ApiError::invalid_argument(format!(
            "min_insync must be between 1 and rf ({rf}), got {min_insync}"
        )));
    }
    Ok(ReplicationPolicy {
        rf,
        min_insync,
        max_voters: MAX_VOTERS,
    })
}

/// Whether the cluster is already initialised (precondition 3): a `ReplicationPolicy`
/// exists, or more than one non-Removed member.
pub fn already_cluster(s: &MetaState) -> bool {
    let policy = s
        .cluster()
        .is_some_and(|c| matches!(get_replication_policy(&c.value.ext), Ok(Some(_))));
    let members = s
        .members()
        .filter(|m| member_state_of(&m.value) != MEMBER_REMOVED)
        .count();
    policy || members > 1
}

/// The advertised cluster address (precondition 4): `requested` when non-empty, else the
/// first `[cluster] advertise` entry, completed with `listener_port` when it is a bare host.
pub fn advertise_of(
    requested: Option<&str>,
    configured: &[String],
    listener_port: u16,
) -> Option<String> {
    if let Some(a) = requested.map(str::trim).filter(|a| !a.is_empty()) {
        return Some(a.to_owned());
    }
    let first = configured
        .iter()
        .map(|a| a.trim())
        .find(|a| !a.is_empty())?;
    if first.contains("://") || first.parse::<std::net::SocketAddr>().is_ok() {
        return Some(first.to_owned());
    }
    match first.parse::<std::net::IpAddr>() {
        Ok(std::net::IpAddr::V6(ip)) => return Some(format!("[{ip}]:{listener_port}")),
        Ok(ip) => return Some(format!("{ip}:{listener_port}")),
        Err(_) => {}
    }
    let host_port = first
        .split_once(':')
        .is_some_and(|(h, p)| !h.is_empty() && p.parse::<u16>().is_ok());
    Some(if host_port {
        first.to_owned()
    } else {
        format!("{first}:{listener_port}")
    })
}

/// The `cluster_addr` a (re)starting TCP node writes into its own Member record (13-18, the
/// 13-16 handover): the configured `[cluster] advertise` ([`advertise_of`]) when set; else
/// the address init or the join stored, while the bound listener is a wildcard
/// (`0.0.0.0` / `::`) or listens on the stored port (a restart must not replace an
/// advertised host with the bind address); else the bound listener.
pub fn self_cluster_addr(
    configured: &[String],
    stored: Option<&str>,
    bound: std::net::SocketAddr,
) -> String {
    if let Some(a) = advertise_of(None, configured, bound.port()) {
        return a;
    }
    let keep = stored.map(str::trim).filter(|s| !s.is_empty()).filter(|s| {
        bound.ip().is_unspecified()
            || s.rsplit_once(':')
                .and_then(|(_, port)| port.parse::<u16>().ok())
                .is_some_and(|port| port == bound.port())
    });
    match keep {
        Some(s) => s.to_owned(),
        None => bound.to_string(),
    }
}

/// Promote this node to a cluster (module docs).
pub async fn init(ctx: &P13Ctx, r: InitRequest) -> Result<ClusterStatus, ApiError> {
    // 1. profile.
    if TlsProfile::of(&ctx.cfg) == TlsProfile::Dev || ctx.cfg.node.ephemeral {
        return Err(ApiError::conflict(
            "`ostro cluster init` needs the server profile on a persistent data directory \
             (refused in `ostro dev` and with --ephemeral)",
        )
        .with_details(serde_json::json!({ "reason": REASON_NOT_SUPPORTED_IN_PROFILE })));
    }
    // 2. policy.
    let policy = policy_of(&r)?;
    // 3. already a cluster: idempotent.
    ctx.meta
        .read_index_barrier()
        .await
        .map_err(|e| ApiError::unavailable(format!("metadata read barrier: {e}")))?;
    if ctx.meta.view().read(already_cluster) {
        return super::status::status(ctx).await;
    }
    // 4. advertise.
    let port = ctx.cluster.execute_slot().1.port();
    let advertise = advertise_of(r.advertise.as_deref(), &ctx.cfg.cluster.advertise, port)
        .ok_or_else(|| {
            ApiError::invalid_argument(
                "an advertised cluster address is required: pass `advertise` (`ostro cluster \
                 init --advertise-cluster HOST:PORT`) or set `[cluster] advertise`",
            )
        })?;
    info!(node = ctx.node_id(), %advertise, rf = policy.rf, min_insync = policy.min_insync, "cluster init");

    put_own_member(ctx, &advertise).await?;
    super::join::ensure_cluster_ca(ctx).await?;
    if ctx.meta.view().read(super::join::stored_ca).is_none() {
        return Err(ApiError::unavailable(format!(
            "this node holds no usable CA under {}; the cluster CA could not be stored",
            ctx.cfg.node.data_dir.join("tls").display()
        )));
    }
    failpoint(FP_INIT_AFTER_CA)?;
    edit_cluster(ctx, |ext| match get_cluster_version(ext) {
        Ok(Some(_)) => Ok(None),
        Ok(None) => put_cluster_version(
            ext,
            &ClusterVersion {
                finalized: ctx.level_max,
                hold: None,
                finalized_at_ms: ctx.clock.now_ms(),
            },
        )
        .map(Some)
        .map_err(|e| ApiError::internal(format!("cluster ext: {e}"))),
        Err(e) => Err(ApiError::internal(format!(
            "undecodable ClusterVersion (never overwritten): {e}"
        ))),
    })
    .await?;
    check_node_cert(ctx, &advertise);
    edit_cluster(ctx, |ext| match get_replication_policy(ext) {
        Ok(Some(_)) => Ok(None),
        _ => put_replication_policy(ext, &policy)
            .map(Some)
            .map_err(|e| ApiError::internal(format!("cluster ext: {e}"))),
    })
    .await?;
    ctx.placement_trigger.notify();
    info!(
        node = ctx.node_id(),
        cluster_id = %ctx.cluster.cluster_id(),
        "this node is now a cluster; other nodes can join"
    );
    super::status::status(ctx).await
}

/// Step 1: own Member Active with NodeVersion and the advertised cluster address.
async fn put_own_member(ctx: &P13Ctx, advertise: &str) -> Result<(), ApiError> {
    let id = ctx.node_id();
    for _ in 0..CAS_ATTEMPTS {
        let Some(cur) = ctx.meta.view().read(|s| s.member(id).cloned()) else {
            return Err(ApiError::unavailable(format!(
                "node {id} has no member record yet; retry once it finished starting"
            )));
        };
        let state = member_state_of(&cur.value);
        if state == MEMBER_REMOVED {
            return Err(ApiError::conflict(format!("node {id} was removed")));
        }
        let mut record = cur.value.clone();
        let version = ctx.node_version();
        let same = record.cluster_addr.as_deref() == Some(advertise)
            && matches!(get_member_state(&record.ext), Ok(Some(s)) if s.state == MEMBER_ACTIVE)
            && get_node_version(&record.ext)
                .ok()
                .flatten()
                .is_some_and(|v| v.level_max == version.level_max);
        if same {
            return Ok(());
        }
        record.cluster_addr = Some(advertise.to_owned());
        if state != MEMBER_ACTIVE || !matches!(get_member_state(&record.ext), Ok(Some(_))) {
            record.ext = put_member_state(
                &record.ext,
                &MemberState {
                    state: MEMBER_ACTIVE,
                    changed_at_ms: ctx.clock.now_ms(),
                },
            )
            .map_err(|e| ApiError::internal(format!("member ext: {e}")))?;
        }
        record.ext = put_node_version(&record.ext, &version)
            .map_err(|e| ApiError::internal(format!("member ext: {e}")))?;
        let res = ctx
            .meta
            .propose(MetaCommand::Put {
                key: MetaKey::Member(id),
                expected_version: Some(cur.version),
                record: MetaRecord::Member(record),
            })
            .await
            .map_err(|e| ApiError::unavailable(format!("updating the member record: {e}")))?;
        match res {
            MetaResult::Ok { .. } => return Ok(()),
            MetaResult::StaleVersion { .. } | MetaResult::Conflict { .. } => continue,
            other => {
                return Err(ApiError::internal(format!(
                    "updating the member record: {other:?}"
                )));
            }
        }
    }
    Err(ApiError::unavailable(
        "the member record kept changing; retry `ostro cluster init`",
    ))
}

/// Read-modify-write of the ClusterInfo ext with CAS retries; `edit` returns `None` for
/// "already so".
async fn edit_cluster(
    ctx: &P13Ctx,
    edit: impl Fn(&[u8]) -> Result<Option<Vec<u8>>, ApiError>,
) -> Result<(), ApiError> {
    for _ in 0..CAS_ATTEMPTS {
        let Some(cur) = ctx.meta.view().read(|s| s.cluster().cloned()) else {
            return Err(ApiError::unavailable("the cluster record is not there yet"));
        };
        let Some(ext) = edit(&cur.value.ext)? else {
            return Ok(());
        };
        let mut record = cur.value.clone();
        record.ext = ext;
        let res = ctx
            .meta
            .propose(MetaCommand::Put {
                key: MetaKey::Cluster,
                expected_version: Some(cur.version),
                record: MetaRecord::Cluster(record),
            })
            .await
            .map_err(|e| ApiError::unavailable(format!("updating the cluster record: {e}")))?;
        match res {
            MetaResult::Ok { .. } => return Ok(()),
            MetaResult::StaleVersion { .. } | MetaResult::Conflict { .. } => continue,
            other => {
                return Err(ApiError::internal(format!(
                    "updating the cluster record: {other:?}"
                )));
            }
        }
    }
    Err(ApiError::unavailable(
        "the cluster record kept changing; retry `ostro cluster init`",
    ))
}

/// Step 4 (module docs): whether the node certificate (SANs = `[cluster] advertise` plus
/// the loopback names, `ClusterCa::node_sans`) names the advertised host. An address that
/// is not a host name (e.g. `mem://`) counts as covered.
pub fn node_cert_covers(configured: &[String], advertise: &str) -> bool {
    let Some(want) = San::from_host(advertise) else {
        return true;
    };
    let loopback = [
        San::Dns("localhost".to_owned()),
        San::Ip(std::net::IpAddr::from([127, 0, 0, 1])),
        San::Ip(std::net::IpAddr::from(std::net::Ipv6Addr::LOCALHOST)),
    ];
    loopback.contains(&want)
        || configured
            .iter()
            .filter_map(|a| San::from_host(a))
            .any(|s| s == want)
}

fn check_node_cert(ctx: &P13Ctx, advertise: &str) {
    if !node_cert_covers(&ctx.cfg.cluster.advertise, advertise) {
        warn!(
            node = ctx.node_id(),
            %advertise,
            "the node certificate does not name the advertised host; add it to [cluster] \
             advertise (workers verify it) — node-to-node mTLS uses the node identity URI and \
             is unaffected"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 13-18: a restart keeps the advertised address (13-16 handover).
    #[test]
    fn self_cluster_addr_keeps_the_advertised_address() {
        let wild: std::net::SocketAddr = "0.0.0.0:7701".parse().unwrap();
        let lo: std::net::SocketAddr = "127.0.0.1:7701".parse().unwrap();
        let other: std::net::SocketAddr = "127.0.0.1:7801".parse().unwrap();
        // `[cluster] advertise` wins (bare host completed with the bound port).
        assert_eq!(
            self_cluster_addr(&["node-a.example".into()], Some("10.0.0.5:7701"), wild),
            "node-a.example:7701"
        );
        // A wildcard bind never replaces the stored advertised address.
        assert_eq!(
            self_cluster_addr(&[], Some("10.0.0.5:7701"), wild),
            "10.0.0.5:7701"
        );
        // Same port on a concrete bind: still the advertised host.
        assert_eq!(
            self_cluster_addr(&[], Some("node-a.example:7701"), lo),
            "node-a.example:7701"
        );
        // The listener moved to another port: the bound address.
        assert_eq!(
            self_cluster_addr(&[], Some("10.0.0.5:7701"), other),
            "127.0.0.1:7801"
        );
        // Nothing stored (first boot of a static node): the bound address.
        assert_eq!(self_cluster_addr(&[], None, lo), "127.0.0.1:7701");
        assert_eq!(self_cluster_addr(&[], Some(""), wild), "0.0.0.0:7701");
    }

    #[test]
    fn policy_defaults_and_bounds() {
        let p = policy_of(&InitRequest::default()).unwrap();
        assert_eq!((p.rf, p.min_insync, p.max_voters), (3, 2, 5));
        let p = policy_of(&InitRequest {
            rf: Some(5),
            ..InitRequest::default()
        })
        .unwrap();
        assert_eq!((p.rf, p.min_insync), (5, 3));
        let p = policy_of(&InitRequest {
            rf: Some(1),
            ..InitRequest::default()
        })
        .unwrap();
        assert_eq!((p.rf, p.min_insync), (1, 1));
        for (rf, mi) in [
            (Some(2), None),
            (Some(4), None),
            (Some(3), Some(4)),
            (None, Some(0)),
        ] {
            assert!(
                policy_of(&InitRequest {
                    rf,
                    min_insync: mi,
                    advertise: None
                })
                .is_err(),
                "{rf:?} {mi:?}"
            );
        }
    }

    #[test]
    fn advertise_sources() {
        let none: Vec<String> = vec![];
        assert_eq!(
            advertise_of(Some("h:1"), &none, 7701).as_deref(),
            Some("h:1")
        );
        assert_eq!(advertise_of(Some("  "), &none, 7701), None);
        assert_eq!(advertise_of(None, &none, 7701), None);
        let cfg = |s: &str| vec![s.to_owned()];
        assert_eq!(
            advertise_of(None, &cfg("node1.example"), 7701).as_deref(),
            Some("node1.example:7701")
        );
        assert_eq!(
            advertise_of(None, &cfg("node1.example:9000"), 7701).as_deref(),
            Some("node1.example:9000")
        );
        assert_eq!(
            advertise_of(None, &cfg("10.0.0.1"), 7701).as_deref(),
            Some("10.0.0.1:7701")
        );
        assert_eq!(
            advertise_of(None, &cfg("::1"), 7701).as_deref(),
            Some("[::1]:7701")
        );
        assert_eq!(
            advertise_of(None, &cfg("[::1]:9"), 7701).as_deref(),
            Some("[::1]:9")
        );
        assert_eq!(
            advertise_of(None, &cfg("mem://1"), 7701).as_deref(),
            Some("mem://1")
        );
    }

    #[test]
    fn cert_coverage() {
        let cfg = vec!["node1.example".to_owned()];
        assert!(node_cert_covers(&cfg, "node1.example:7701"));
        assert!(node_cert_covers(&cfg, "127.0.0.1:7701"));
        assert!(node_cert_covers(&cfg, "mem://1"));
        assert!(!node_cert_covers(&cfg, "10.0.0.9:7701"));
        assert!(!node_cert_covers(&[], "node1.example:7701"));
    }
}
