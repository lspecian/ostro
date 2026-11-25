//! The marker-first joiner, the boot rule and both voter checks (13-09; D-P13-9, D-P13-29).
//!
//! # `ostro cluster join` ([`run_join`] / [`join_and_prepare`])
//!
//! 1. A marker already in the data dir means a previous join got that far: resume with its
//!    node id (`resume_node_id`; the seed re-signs only for a still-Joining id).
//! 2. `POST <seed>/v1/cluster/join {token, csr, advertise, node_version, resume_node_id?}`
//!    on the seed's client listener: `https://` verifies the server chain through the CA
//!    whose sha256 is `--ca-hash` (the D-W20 bootstrap pin); `http://` (tests, plaintext
//!    listeners) checks the pin against the returned CA instead. The CA in the answer must
//!    match the pin either way, and `finalized_level` must lie in
//!    `[LEVEL_MIN, level_max]`.
//! 3. Then, **in this order** (each step durable before the next):
//!    1. the marker `<data>/cluster-seeds.toml {node_id, cluster_id, seeds}` (tmp, fsync,
//!       rename, dir fsync) — failpoint `member::join_before_marker` just before;
//!    2. `<data>/tls/joined/{ca.pem,node.pem,node.key}` (key 0600) — failpoint
//!       `member::join_after_marker` just before;
//!    3. node.toml `node_id` / `cluster_id` — failpoint `member::join_after_node_toml` just
//!       after.
//!
//!    (`tls/joined/` keeps the joined set apart from the Phase 7 `tls/node.{crt,key}` that
//!    `tls::init` issues from the cluster CA.)
//! 4. Discard-and-rejoin (NW1): a resume refused because the id is Removed (`member_removed`)
//!    while meta is blank and no partition data exists deletes node.toml, `tls/joined/` and
//!    then the marker, and joins again for a new id.
//!
//! # Boot ([`join_identity`], [`boot_mode`], [`voter_check`])
//!
//! - [`join_identity`] runs first in boot: reads the marker, refuses a marker whose joined TLS
//!   set is incomplete ("re-run `ostro cluster join`") before any TLS load.
//! - [`boot_mode`]: blank meta over partition data → the Phase 12 fail-stop (marker or not);
//!   blank meta + marker → `Joiner` (node.toml completed from the marker if missing); blank
//!   meta, no data, no marker → `Fresh` (configured peers alone never make a joiner);
//!   otherwise `Existing`.
//! - [`voter_check`]: once dynamic membership is in use ([`members_known`]: the marker, a
//!   MemberState in the live view, or one in the persisted meta) the persisted / live
//!   ConfState is authoritative and `[[cluster.peers]]` only seeds (drift logged once); a
//!   node whose own id is Removed (or that is no longer in a non-empty configuration)
//!   refuses to start. Before that, both Phase 12 rules apply unchanged.

use std::io::{self, Write as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use ostro_api::phase13::{Advertise, JoinRequest, JoinResponse, NodeVersionDto};
use ostro_cluster::address_book::Seed;
use ostro_cluster::identity::{self, BootError};
use ostro_meta::MetaView;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, RootCertStore, SignatureScheme};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{info, warn};

use super::{
    BootMode, ConfView, DataDirProbe, JoinArgs, JoinMarker, JoinPrepared, JoinedTls,
    VoterCheckPhase,
};
use crate::config::ServerConfig;
use crate::tls::ca::write_atomic;

/// The join marker file in the data dir (RESERVATIONS §11).
pub const MARKER_FILE: &str = "cluster-seeds.toml";
/// The joined certificate set, under the data dir.
pub const JOINED_TLS_DIR: &str = "tls/joined";
const JOINED_CA: &str = "ca.pem";
const JOINED_CERT: &str = "node.pem";
const JOINED_KEY: &str = "node.key";
/// `details.reason` of a resume refused for a tombstoned id (from 13-09's `join.rs`).
const REASON_MEMBER_REMOVED: &str = super::join::REASON_MEMBER_REMOVED;

// ------------------------------------------------------------------------------------
// The marker and the joined TLS set
// ------------------------------------------------------------------------------------

#[derive(Deserialize)]
struct MarkerFile {
    node_id: u64,
    cluster_id: String,
    #[serde(default)]
    seeds: Vec<SeedFile>,
}

#[derive(Deserialize)]
struct SeedFile {
    id: u64,
    addr: String,
    #[serde(default)]
    repl_addr: Option<String>,
}

fn toml_str(s: &str) -> String {
    // A JSON string literal is a valid TOML basic string.
    serde_json::to_string(s).unwrap_or_else(|_| "\"\"".to_owned())
}

/// The marker's TOML text.
pub fn marker_toml(m: &JoinMarker) -> String {
    let mut s = format!(
        "# Written by `ostro cluster join` (do not edit). Removing it makes this node no longer\n\
         # a joiner.\nnode_id = {}\ncluster_id = {}\n",
        m.node_id,
        toml_str(&m.cluster_id)
    );
    for seed in &m.seeds {
        s.push_str(&format!(
            "\n[[seeds]]\nid = {}\naddr = {}\n",
            seed.id,
            toml_str(&seed.addr)
        ));
        if let Some(r) = &seed.repl_addr {
            s.push_str(&format!("repl_addr = {}\n", toml_str(r)));
        }
    }
    s
}

/// Parse the marker's TOML text.
pub fn parse_marker(text: &str) -> Result<JoinMarker, String> {
    let f: MarkerFile = config::Config::builder()
        .add_source(config::File::from_str(text, config::FileFormat::Toml))
        .build()
        .and_then(|c| c.try_deserialize())
        .map_err(|e| e.to_string())?;
    if f.node_id == 0 {
        return Err("node_id 0 is reserved".into());
    }
    if f.cluster_id.is_empty() {
        return Err("cluster_id is empty".into());
    }
    Ok(JoinMarker {
        node_id: f.node_id,
        cluster_id: f.cluster_id,
        seeds: f
            .seeds
            .into_iter()
            .map(|s| Seed {
                id: s.id,
                addr: s.addr,
                repl_addr: s.repl_addr,
            })
            .collect(),
    })
}

/// Read `<data>/cluster-seeds.toml` (`None` when absent).
pub fn read_marker(data_dir: &Path) -> Result<Option<JoinMarker>, BootError> {
    let path = data_dir.join(MARKER_FILE);
    let bad = |reason: String| BootError::BadJoinMarker {
        path: path.display().to_string(),
        reason,
    };
    match std::fs::read_to_string(&path) {
        Ok(text) => parse_marker(&text).map(Some).map_err(bad),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(bad(e.to_string())),
    }
}

fn io_other(e: impl std::fmt::Display) -> io::Error {
    io::Error::other(e.to_string())
}

/// Write the marker durably (tmp, fsync, rename, dir fsync).
pub fn write_marker(data_dir: &Path, m: &JoinMarker) -> io::Result<()> {
    std::fs::create_dir_all(data_dir)?;
    write_atomic(
        &data_dir.join(MARKER_FILE),
        marker_toml(m).as_bytes(),
        0o644,
    )
    .map_err(io_other)
}

/// Write `<data>/tls/joined/{ca.pem,node.pem,node.key}` durably (key 0600, the key file
/// last so a complete set always has its key).
pub fn write_joined_tls(data_dir: &Path, t: &JoinedTls) -> io::Result<()> {
    let dir = data_dir.join(JOINED_TLS_DIR);
    std::fs::create_dir_all(&dir)?;
    write_atomic(&dir.join(JOINED_CA), t.ca_pem.as_bytes(), 0o644).map_err(io_other)?;
    write_atomic(&dir.join(JOINED_CERT), t.cert_chain_pem.as_bytes(), 0o644).map_err(io_other)?;
    write_atomic(&dir.join(JOINED_KEY), t.key_pem.as_bytes(), 0o600).map_err(io_other)
}

/// Bind node.toml `node_id` / `cluster_id` (durable; refuses a node.toml naming others).
pub fn write_identity(data_dir: &Path, node_id: u64, cluster_id: &str) -> Result<(), String> {
    ostro_partition::bind_node_id(data_dir, node_id).map_err(|e| e.to_string())?;
    ostro_partition::bind_cluster_id(data_dir, cluster_id).map_err(|e| e.to_string())
}

/// Whether `data_dir` holds partition data (a state store or WAL segment), as `cluster.rs`
/// decides it.
fn partition_data_exists(data_dir: &Path) -> bool {
    let non_empty = |d: PathBuf| {
        std::fs::read_dir(d)
            .map(|mut rd| rd.next().is_some())
            .unwrap_or(false)
    };
    non_empty(data_dir.join("state")) || non_empty(data_dir.join("wal"))
}

fn fsync_dir(dir: &Path) -> io::Result<()> {
    std::fs::File::open(dir)?.sync_all()
}

/// Discard a join (NW1): node.toml, `tls/joined/`, then the marker (deleted last, so a crash
/// in between still resumes or discards again).
pub fn discard_join_state(data_dir: &Path) -> io::Result<()> {
    let rm = |p: PathBuf| match std::fs::remove_file(&p) {
        Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e),
        _ => Ok(()),
    };
    rm(data_dir.join("node.toml"))?;
    match std::fs::remove_dir_all(data_dir.join(JOINED_TLS_DIR)) {
        Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(e),
        _ => {}
    }
    if data_dir.join("tls").exists() {
        fsync_dir(&data_dir.join("tls"))?;
    }
    fsync_dir(data_dir)?;
    rm(data_dir.join(MARKER_FILE))?;
    fsync_dir(data_dir)
}

/// Read and validate `<data>/cluster-seeds.toml`; a marker with an incomplete joined TLS set
/// is refused ("re-run `ostro cluster join`") before any TLS load.
pub fn join_identity(data_dir: &Path) -> Result<Option<JoinMarker>, BootError> {
    let Some(m) = read_marker(data_dir)? else {
        return Ok(None);
    };
    if !tls_complete(data_dir) {
        return Err(BootError::JoinIncomplete {
            data_dir: data_dir.display().to_string(),
        });
    }
    Ok(Some(m))
}

/// Whether `<data>/tls/joined` holds a complete joined-node certificate set.
pub fn tls_complete(data_dir: &Path) -> bool {
    let dir = data_dir.join(JOINED_TLS_DIR);
    [JOINED_CA, JOINED_CERT, JOINED_KEY]
        .iter()
        .all(|f| std::fs::metadata(dir.join(f)).is_ok_and(|m| m.is_file() && m.len() > 0))
}

/// A joined node's certificate set from `<data>/tls/joined` (`None` = the Phase 12 CA path).
pub fn joined_tls(data_dir: &Path) -> Result<Option<JoinedTls>, BootError> {
    if !tls_complete(data_dir) {
        return Ok(None);
    }
    let dir = data_dir.join(JOINED_TLS_DIR);
    let read = |f: &str| {
        std::fs::read_to_string(dir.join(f)).map_err(|e| BootError::BadJoinMarker {
            path: dir.join(f).display().to_string(),
            reason: e.to_string(),
        })
    };
    Ok(Some(JoinedTls {
        ca_pem: read(JOINED_CA)?,
        cert_chain_pem: read(JOINED_CERT)?,
        key_pem: read(JOINED_KEY)?,
    }))
}

/// Import `__ostro/cluster-ca` from meta into `<data>/tls/ca.{crt,key}` when this node has no
/// CA key yet (a joined node), so the Phase 7 TLS state (worker joins, the client listener's
/// self-signed certificate) uses the cluster CA instead of creating its own. Called by
/// `p7::init` before `tls::init`; a node that already has `ca.key`, or a meta without the
/// secret, is left alone.
pub fn import_cluster_ca(data_dir: &Path, cluster_id: &str, view: &MetaView) {
    let dir = data_dir.join("tls");
    if dir.join(crate::tls::ca::ClusterCa::KEY_FILE).exists() || !tls_complete(data_dir) {
        return;
    }
    let Some(ca) = view.read(super::join::stored_ca) else {
        return;
    };
    match crate::tls::ca::ClusterCa::import_pem(&dir, cluster_id, &ca.cert_pem, &ca.key_pem) {
        Ok(_) => info!("joined node: cluster CA imported from metadata for the Phase 7 TLS state"),
        Err(e) => warn!(error = %e, "joined node: importing the cluster CA failed"),
    }
}

// ------------------------------------------------------------------------------------
// Boot rule and voter checks
// ------------------------------------------------------------------------------------

/// The boot rule (D-P13-29b).
pub fn boot_mode(cfg: &ServerConfig, probe: &DataDirProbe) -> Result<BootMode, BootError> {
    if identity::is_joiner(probe.meta_blank, probe.has_partition_data) {
        // Blank meta over partition data: the Phase 12 fail-stop, marker or not.
        return Err(identity::joiner_error(
            !cfg.cluster.peers.is_empty(),
            &probe.meta_dir,
        ));
    }
    if !probe.meta_blank {
        return Ok(BootMode::Existing);
    }
    match &probe.join_marker {
        Some(m) => {
            if !probe.tls_complete {
                return Err(BootError::JoinIncomplete {
                    data_dir: probe.data_dir.display().to_string(),
                });
            }
            // node.toml is completed from the marker if a crash came before step 3.
            if let Err(e) = write_identity(&probe.data_dir, m.node_id, &m.cluster_id) {
                return Err(BootError::BadJoinMarker {
                    path: probe.data_dir.join(MARKER_FILE).display().to_string(),
                    reason: format!("completing node.toml: {e}"),
                });
            }
            Ok(BootMode::Joiner)
        }
        None => Ok(BootMode::Fresh),
    }
}

/// Whether dynamic membership is in use: the join marker, a MemberState in the live view,
/// or (PreStart, `view = None`) a MemberState in the persisted meta.
pub fn members_known(data_dir: &Path, view: Option<&MetaView>) -> bool {
    if data_dir.join(MARKER_FILE).exists() {
        return true;
    }
    match view {
        Some(v) => v.read(ostro_cluster::membership::service::dynamic_membership),
        None => identity::probe_members(&data_dir.join("meta"))
            .map(|p| p.dynamic)
            .unwrap_or(false),
    }
}

/// The configured voter set: self ∪ `[[cluster.peers]]`.
fn configured_voters(cfg: &ServerConfig) -> Vec<u64> {
    let mut v: Vec<u64> = cfg.cluster.peers.iter().map(|p| p.id).collect();
    v.push(cfg.node.node_id);
    v.sort_unstable();
    v.dedup();
    v
}

static DRIFT_LOGGED: AtomicBool = AtomicBool::new(false);

/// Both meta voter checks (D-P13-29a).
pub fn voter_check(
    view: &ConfView,
    cfg: &ServerConfig,
    members_known: bool,
    phase: VoterCheckPhase,
) -> Result<(), BootError> {
    let expected = configured_voters(cfg);
    if !members_known {
        return match phase {
            VoterCheckPhase::PreStart => identity::check_meta_voters(Some(view.clone()), &expected),
            VoterCheckPhase::PostReady => {
                identity::check_voters_after_ready(&view.voters, &expected)
            }
        };
    }
    let me = cfg.node.node_id;
    let in_conf = view.voters.contains(&me) || view.learners.contains(&me);
    let conf_empty = view.voters.is_empty() && view.learners.is_empty();
    if phase == VoterCheckPhase::PreStart {
        let removed = identity::probe_members(&cfg.node.data_dir.join("meta"))
            .map(|p| p.removed.contains(&me))
            .unwrap_or(false);
        if removed {
            return Err(BootError::NodeRemoved { node_id: me });
        }
    }
    if !conf_empty && !in_conf {
        return Err(BootError::NodeRemoved { node_id: me });
    }
    let mut actual = view.voters.clone();
    actual.sort_unstable();
    if !conf_empty && actual != expected && !DRIFT_LOGGED.swap(true, Ordering::Relaxed) {
        info!(
            voters = ?actual,
            configured = ?expected,
            "dynamic membership in use: the metadata configuration is authoritative; \
             [[cluster.peers]] only seeds the address book (logged once)"
        );
    }
    Ok(())
}

// ------------------------------------------------------------------------------------
// `ostro cluster join`
// ------------------------------------------------------------------------------------

/// What [`join_and_prepare`] needs (resolved from [`JoinArgs`] by [`run_join`]).
#[derive(Clone, Debug)]
pub struct JoinPlan {
    /// The data dir to prepare.
    pub data_dir: PathBuf,
    /// Seed API URL (`https://…` or `http://…`).
    pub seed: String,
    /// `njt_<id>.<secret>` (never logged).
    pub token: String,
    /// `sha256:<hex>` CA pin.
    pub ca_hash: String,
    /// Advertised addresses.
    pub advertise: Advertise,
    /// This binary's highest feature level.
    pub level_max: u32,
}

#[cfg(feature = "failpoints")]
fn fail_here(name: &str) -> Result<(), String> {
    match fail::eval(name, |_| ()) {
        Some(()) => Err(format!("failpoint {name}")),
        None => Ok(()),
    }
}

#[cfg(not(feature = "failpoints"))]
#[inline(always)]
fn fail_here(_name: &str) -> Result<(), String> {
    Ok(())
}

fn parse_pin(h: &str) -> Result<[u8; 32], String> {
    let hex = h
        .strip_prefix("sha256:")
        .ok_or_else(|| "--ca-hash: expected sha256:<64 hex>".to_owned())?;
    if hex.len() != 64 {
        return Err("--ca-hash: expected sha256:<64 hex>".into());
    }
    let mut out = [0u8; 32];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&hex[2 * i..2 * i + 2], 16)
            .map_err(|_| "--ca-hash: expected sha256:<64 hex>".to_owned())?;
    }
    Ok(out)
}

fn key_and_csr(cn: &str) -> Result<(String, String), String> {
    let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256)
        .map_err(|e| format!("key generation: {e}"))?;
    let mut params =
        rcgen::CertificateParams::new(Vec::<String>::new()).map_err(|e| format!("csr: {e}"))?;
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, cn);
    let csr = params
        .serialize_request(&key)
        .and_then(|c| c.pem())
        .map_err(|e| format!("csr: {e}"))?;
    Ok((key.serialize_pem(), csr))
}

fn node_version(level_max: u32) -> NodeVersionDto {
    NodeVersionDto {
        ostro_version: env!("CARGO_PKG_VERSION").to_owned(),
        level_min: ostro_types::compat::LEVEL_MIN,
        level_max,
        wasmtime: ostro_engine::cache::WASMTIME_VERSION.to_owned(),
        event_codec_ver: u32::from(ostro_types::journal::EVENT_CODEC_VER),
        wit_lines: ostro_engine::host::linker_set::LinkerSet::default()
            .lines()
            .iter()
            .map(ToString::to_string)
            .collect(),
    }
}

/// The server's answer to a join POST.
enum JoinAnswer {
    Ok(Box<JoinResponse>),
    /// `(status, reason, message)`.
    Refused(u16, Option<String>, String),
}

async fn post_join(
    plan: &JoinPlan,
    pin: [u8; 32],
    req: &JoinRequest,
) -> Result<JoinAnswer, String> {
    let body = serde_json::to_vec(req).map_err(|e| e.to_string())?;
    let (status, resp) = http_post_json(&plan.seed, "/v1/cluster/join", &body, pin).await?;
    let scrub = |s: String| s.replace(&plan.token, "[token]");
    if (200..300).contains(&status) {
        let r: JoinResponse =
            serde_json::from_slice(&resp).map_err(|e| format!("join response: {e}"))?;
        return Ok(JoinAnswer::Ok(Box::new(r)));
    }
    let v: serde_json::Value = serde_json::from_slice(&resp).unwrap_or_default();
    let err = v.get("error").unwrap_or(&v);
    let reason = err
        .pointer("/details/reason")
        .and_then(|r| r.as_str())
        .map(str::to_owned);
    let msg = err
        .get("message")
        .and_then(|m| m.as_str())
        .map(str::to_owned)
        .unwrap_or_else(|| String::from_utf8_lossy(&resp).into_owned());
    Ok(JoinAnswer::Refused(status, reason, scrub(msg)))
}

/// Join through the seed and prepare `plan.data_dir` (marker, `tls/joined/`, node.toml, in
/// that order). Resumes the marker's id when one exists.
pub async fn join_and_prepare(plan: &JoinPlan) -> Result<JoinPrepared, String> {
    let pin = parse_pin(&plan.ca_hash)?;
    let data_dir = &plan.data_dir;
    std::fs::create_dir_all(data_dir).map_err(|e| format!("{}: {e}", data_dir.display()))?;
    let mut resume = read_marker(data_dir)
        .map_err(|e| e.to_string())?
        .map(|m| m.node_id);
    let meta_blank = identity::probe_meta(&data_dir.join("meta"))
        .map(|p| p.blank)
        .unwrap_or(true);
    if resume.is_none() && !meta_blank {
        return Err(format!(
            "{} already holds a cluster node's metadata; join only blank data dirs",
            data_dir.display()
        ));
    }
    for attempt in 0..2 {
        let (key_pem, csr_pem) = key_and_csr("ostro joining node")?;
        let req = JoinRequest {
            token: plan.token.clone(),
            csr_pem,
            advertise: plan.advertise.clone(),
            node_version: node_version(plan.level_max),
            resume_node_id: resume,
        };
        let r = match post_join(plan, pin, &req).await? {
            JoinAnswer::Ok(r) => *r,
            JoinAnswer::Refused(_, Some(reason), _)
                if reason == REASON_MEMBER_REMOVED
                    && attempt == 0
                    && meta_blank
                    && !partition_data_exists(data_dir) =>
            {
                warn!(
                    node = ?resume,
                    "the marker's node id was removed (orphan tombstone); discarding the join \
                     state and joining as a new node"
                );
                discard_join_state(data_dir).map_err(|e| format!("discarding the join: {e}"))?;
                resume = None;
                continue;
            }
            JoinAnswer::Refused(status, _, msg) => {
                return Err(format!("the seed refused the join (HTTP {status}): {msg}"));
            }
        };
        let ca_der = rustls_pki_types::pem::PemObject::from_pem_slice(r.ca_cert_pem.as_bytes())
            .map(|d: CertificateDer<'static>| d)
            .map_err(|_| "join response: unreadable CA certificate".to_owned())?;
        if <[u8; 32]>::from(Sha256::digest(ca_der.as_ref())) != pin {
            return Err("the cluster CA in the join response does not match --ca-hash".into());
        }
        let lo = ostro_types::compat::LEVEL_MIN;
        if !(lo <= r.finalized_level && r.finalized_level <= plan.level_max) {
            return Err(format!(
                "the cluster is finalized at feature level {} but this binary runs {lo}..={}",
                r.finalized_level, plan.level_max
            ));
        }
        if let Some(id) = resume
            && id != r.node_id
        {
            return Err(format!(
                "the seed resumed node {} but the marker names node {id}",
                r.node_id
            ));
        }
        let marker = JoinMarker {
            node_id: r.node_id,
            cluster_id: r.cluster_id.clone(),
            seeds: r
                .seeds
                .iter()
                .map(|s| Seed {
                    id: s.id,
                    addr: s.addr.clone(),
                    repl_addr: s.repl_addr.clone(),
                })
                .collect(),
        };
        fail_here("member::join_before_marker")?;
        write_marker(data_dir, &marker).map_err(|e| format!("writing the join marker: {e}"))?;
        fail_here("member::join_after_marker")?;
        write_joined_tls(
            data_dir,
            &JoinedTls {
                ca_pem: r.ca_cert_pem.clone(),
                cert_chain_pem: r.node_cert_pem.clone(),
                key_pem,
            },
        )
        .map_err(|e| format!("writing the joined TLS files: {e}"))?;
        write_identity(data_dir, r.node_id, &r.cluster_id)?;
        fail_here("member::join_after_node_toml")?;
        info!(node_id = r.node_id, cluster_id = %r.cluster_id, "join prepared");
        return Ok(JoinPrepared {
            node_id: r.node_id,
            cluster_id: r.cluster_id,
            data_dir: data_dir.clone(),
        });
    }
    Err("the join was refused twice".into())
}

fn non_wildcard(addr: std::net::SocketAddr) -> Option<String> {
    (!addr.ip().is_unspecified() && addr.port() != 0).then(|| addr.to_string())
}

/// `ostro cluster join` (D-P13-9): resolve the data dir and advertised addresses from the
/// arguments and the server configuration, then [`join_and_prepare`]. The binary starts the
/// server afterwards unless `--no-start`.
pub async fn run_join(args: JoinArgs) -> Result<JoinPrepared, String> {
    let cfg = match &args.config {
        Some(path) => crate::config::load_layered(
            ServerConfig::server_defaults(),
            Some(path.as_path()),
            true,
            &crate::config::EnvSource::Process,
            &crate::config::Overrides::default(),
        )
        .map_err(|e| e.to_string())?,
        None => ServerConfig::server_defaults(),
    };
    let data_dir = args
        .data_dir
        .clone()
        .unwrap_or_else(|| cfg.node.data_dir.clone());
    let advertise = Advertise {
        api: args
            .advertise_api
            .clone()
            .or_else(|| non_wildcard(cfg.api.listen))
            .unwrap_or_default(),
        cluster: args
            .advertise_cluster
            .clone()
            .or_else(|| non_wildcard(cfg.cluster.listen))
            .unwrap_or_default(),
        repl: args
            .advertise_repl
            .clone()
            .or_else(|| cfg.cluster.replication_listen.and_then(non_wildcard)),
    };
    if advertise.cluster.is_empty() {
        warn!(
            "no cluster address to advertise (--advertise-cluster or a non-wildcard \
             [cluster] listen); the metadata leader cannot reach this node until it does"
        );
    }
    join_and_prepare(&JoinPlan {
        data_dir,
        seed: args.seed.clone(),
        token: args.token.clone(),
        ca_hash: args.ca_hash.clone(),
        advertise,
        level_max: ostro_types::compat::level_max(),
    })
    .await
}

// ------------------------------------------------------------------------------------
// A minimal HTTP/1.1 client (no new dependency): one POST, `Connection: close`.
// ------------------------------------------------------------------------------------

const PIN_NOT_IN_CHAIN: &str = "no certificate in the server chain matches --ca-hash";

/// Trust the presented chain only through the certificate whose sha256 equals the pin
/// (the worker's D-W20 bootstrap verifier).
#[derive(Debug)]
struct PinnedCaVerifier {
    pin: [u8; 32],
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for PinnedCaVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let matches =
            |c: &CertificateDer<'_>| <[u8; 32]>::from(Sha256::digest(c.as_ref())) == self.pin;
        let Some(ca) = std::iter::once(end_entity)
            .chain(intermediates.iter())
            .find(|c| matches(c))
        else {
            return Err(rustls::Error::General(PIN_NOT_IN_CHAIN.into()));
        };
        let mut roots = RootCertStore::empty();
        roots
            .add(ca.clone().into_owned())
            .map_err(|e| rustls::Error::General(format!("pinned CA: {e}")))?;
        let inner = rustls::client::WebPkiServerVerifier::builder_with_provider(
            Arc::new(roots),
            Arc::clone(&self.provider),
        )
        .build()
        .map_err(|e| rustls::Error::General(format!("pinned CA verifier: {e}")))?;
        inner.verify_server_cert(end_entity, intermediates, server_name, ocsp_response, now)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// `(https, host, port, path prefix)` of `http(s)://host[:port][/prefix]`.
fn parse_url(url: &str) -> Result<(bool, String, u16, String), String> {
    let (https, rest) = if let Some(r) = url.strip_prefix("https://") {
        (true, r)
    } else if let Some(r) = url.strip_prefix("http://") {
        (false, r)
    } else {
        return Err(format!("seed URL {url:?}: expected https://host:port"));
    };
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], rest[i..].trim_end_matches('/')),
        None => (rest, ""),
    };
    let (host, port) = if let Some(r) = authority.strip_prefix('[') {
        let (h, p) = r
            .split_once(']')
            .ok_or_else(|| format!("seed URL {url:?}: bad IPv6 host"))?;
        (h.to_owned(), p.strip_prefix(':').unwrap_or(""))
    } else {
        match authority.rsplit_once(':') {
            Some((h, p)) => (h.to_owned(), p),
            None => (authority.to_owned(), ""),
        }
    };
    let port = if port.is_empty() {
        if https { 443 } else { 80 }
    } else {
        port.parse()
            .map_err(|_| format!("seed URL {url:?}: bad port"))?
    };
    if host.is_empty() {
        return Err(format!("seed URL {url:?}: no host"));
    }
    Ok((https, host, port, path.to_owned()))
}

/// Decode a `Transfer-Encoding: chunked` body.
fn dechunk(mut b: &[u8]) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    loop {
        let i = b
            .windows(2)
            .position(|w| w == b"\r\n")
            .ok_or("chunked body: truncated")?;
        let size_txt = std::str::from_utf8(&b[..i]).map_err(|_| "chunked body: bad size")?;
        let size = usize::from_str_radix(size_txt.split(';').next().unwrap_or("").trim(), 16)
            .map_err(|_| "chunked body: bad size")?;
        b = &b[i + 2..];
        if size == 0 {
            return Ok(out);
        }
        if b.len() < size + 2 {
            return Err("chunked body: truncated".into());
        }
        out.extend_from_slice(&b[..size]);
        b = &b[size + 2..];
    }
}

async fn exchange<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(
    mut s: S,
    request: &[u8],
) -> Result<Vec<u8>, String> {
    s.write_all(request).await.map_err(|e| e.to_string())?;
    s.flush().await.map_err(|e| e.to_string())?;
    let mut buf = Vec::new();
    // A peer that closes without close_notify still delivered the whole response.
    match s.read_to_end(&mut buf).await {
        Ok(_) => {}
        Err(e) if !buf.is_empty() && e.kind() == io::ErrorKind::UnexpectedEof => {}
        Err(e) => return Err(e.to_string()),
    }
    Ok(buf)
}

/// POST `body` (JSON) to `<base><path>`; `(status, body)`.
async fn http_post_json(
    base: &str,
    path: &str,
    body: &[u8],
    pin: [u8; 32],
) -> Result<(u16, Vec<u8>), String> {
    let (https, host, port, prefix) = parse_url(base)?;
    let host_header = if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    };
    let mut req = Vec::new();
    write!(
        req,
        "POST {prefix}{path} HTTP/1.1\r\nHost: {host_header}\r\nContent-Type: application/json\r\n\
         Accept: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .map_err(|e| e.to_string())?;
    req.extend_from_slice(body);
    let connect = tokio::net::TcpStream::connect((host.as_str(), port));
    let tcp = tokio::time::timeout(Duration::from_secs(10), connect)
        .await
        .map_err(|_| format!("connecting to {host_header}: timed out"))?
        .map_err(|e| format!("connecting to {host_header}: {e}"))?;
    let io = async {
        if https {
            let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
            let tls = rustls::ClientConfig::builder_with_provider(Arc::clone(&provider))
                .with_safe_default_protocol_versions()
                .map_err(|e| format!("TLS config: {e}"))?
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(PinnedCaVerifier { pin, provider }))
                .with_no_client_auth();
            let name = ServerName::try_from(host.clone())
                .map_err(|e| format!("seed host {host:?}: {e}"))?;
            let s = tokio_rustls::TlsConnector::from(Arc::new(tls))
                .connect(name, tcp)
                .await
                .map_err(|e| format!("TLS to {host_header}: {e}"))?;
            exchange(s, &req).await
        } else {
            exchange(tcp, &req).await
        }
    };
    let raw = tokio::time::timeout(Duration::from_secs(60), io)
        .await
        .map_err(|_| format!("join request to {host_header}: timed out"))??;
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or("malformed HTTP response")?;
    let head = std::str::from_utf8(&raw[..split]).map_err(|_| "malformed HTTP response")?;
    let status: u16 = head
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or("malformed HTTP status line")?;
    let chunked = head.lines().any(|l| {
        let l = l.to_ascii_lowercase();
        l.starts_with("transfer-encoding:") && l.contains("chunked")
    });
    let body = &raw[split + 4..];
    let body = if chunked {
        dechunk(body)?
    } else {
        body.to_vec()
    };
    Ok((status, body))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn marker_round_trips() {
        let m = JoinMarker {
            node_id: 4,
            cluster_id: "c-1".into(),
            seeds: vec![
                Seed {
                    id: 1,
                    addr: "10.0.0.1:17701".into(),
                    repl_addr: None,
                },
                Seed {
                    id: 2,
                    addr: "mem://2".into(),
                    repl_addr: Some("10.0.0.2:17702".into()),
                },
            ],
        };
        assert_eq!(parse_marker(&marker_toml(&m)).unwrap(), m);
        assert!(parse_marker("node_id = 0\ncluster_id = \"x\"\n").is_err());
    }

    #[test]
    fn urls_and_chunks() {
        assert_eq!(
            parse_url("http://127.0.0.1:7700").unwrap(),
            (false, "127.0.0.1".into(), 7700, String::new())
        );
        assert_eq!(
            parse_url("https://[::1]:7/x/").unwrap(),
            (true, "::1".into(), 7, "/x".into())
        );
        assert!(parse_url("ftp://h").is_err());
        assert_eq!(
            dechunk(b"3\r\nabc\r\n2\r\nde\r\n0\r\n\r\n").unwrap(),
            b"abcde"
        );
        assert_eq!(
            parse_pin(&format!("sha256:{}", "ab".repeat(32))).unwrap(),
            [0xab; 32]
        );
    }
}
