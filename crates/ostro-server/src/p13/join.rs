//! The server side of the node join handshake (13-09, D-P13-9 / D-P13-10).
//!
//! - **Node join tokens** ([`create_node_token`]): the Phase 7 `ApiToken` record
//!   `{id: "njt_<base32 10>", hash: sha256(secret), scopes: ["node:join"], expires_at_ms}`,
//!   create-only; the secret (`njt_<id>.<secret>`) is returned once and never logged. The
//!   response carries the CA pin (`sha256:<hex>` of the cluster CA from
//!   `__ostro/cluster-ca`).
//! - **Join** ([`handle_join`], any member serves it; meta writes are forwarded to the meta
//!   leader by `MetaHandle`): token check (constant-time hash, scope, expiry against the
//!   injected clock, present = not revoked; unknown / expired / revoked / wrong scope are one
//!   indistinguishable `join_token_invalid`), the joiner's level range must contain the
//!   finalized level, then either a **resume** (`resume_node_id`: re-sign for a still-Joining
//!   id, refreshing its `MemberState.changed_at_ms`; Active → `member_active`, Removed →
//!   `member_removed`) or an **allocation**: create-only `Put Member{node_id = max(all ids
//!   incl. tombstones) + 1, MemberState Joining, NodeVersion}` retried on Conflict. The CSR
//!   is signed with the CA from `__ostro/cluster-ca` (SAN URI `ostro://<cluster>/node/<id>`
//!   assigned by the server, never taken from the CSR, plus the advertised hosts and the
//!   loopback names; serverAuth + clientAuth; 1 year).
//! - **CA secret** ([`ensure_cluster_ca`]): `SealedSecret "__ostro/cluster-ca"` holding the
//!   CA certificate and key PEMs, written create-only from this node's
//!   `<data>/tls/ca.{crt,key}` (the shared CA of a static cluster, the node's own CA, or the
//!   CA a joined node imported); never overwritten, never logged. p14-fixups-2: sealed with
//!   aes256gcm when an operator sealing key is configured (`p14::reseal::cluster_store`;
//!   `p14::reseal` re-seals a legacy `plaintext-v0` record at startup), `plaintext-v0`
//!   otherwise; [`stored_ca`] reads both. Only the CA certificate travels to a joiner
//!   (`JoinResponse.ca_cert_pem`); the key is opened by the member that signs the CSR.

use std::collections::BTreeSet;
use std::net::IpAddr;
use std::time::Duration;

use ostro_api::ApiError;
use ostro_api::phase7::REASON_JOIN_TOKEN_INVALID;
use ostro_api::phase13::{JoinRequest, JoinResponse, NodeTokenRequest, NodeTokenResponse, SeedDto};
use ostro_cluster::membership::service::member_state_of;
use ostro_meta::ext::{
    MEMBER_JOINING, MEMBER_REMOVED, MemberState, NodeVersion, put_member_state, put_node_version,
};
use ostro_meta::{
    ApiToken, Member, MetaCommand, MetaKey, MetaRecord, MetaResult, MetaState, SealedSecret,
    TOKEN_KEY_PLAINTEXT_ID,
};
use rcgen::{
    CertificateParams, CertificateSigningRequestParams, DistinguishedName, DnType,
    ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair, KeyUsagePurpose, SanType, SerialNumber,
};
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, CertificateSigningRequestDer};
use sha2::{Digest, Sha256};
use tracing::{info, warn};
use x509_parser::oid_registry::{OID_EC_P256, OID_KEY_TYPE_EC_PUBLIC_KEY, OID_SIG_ED25519};
use x509_parser::prelude::{FromDer, X509CertificationRequest};

use super::P13Ctx;
use crate::p14::reseal::{SystemSecretError, cluster_store, open_system_secret};
use crate::tls::ca::{ClusterCa, LEAF_LIFETIME, NodeIdentity, San, pem_encode, sha256_fingerprint};

/// The secret holding the cluster CA (RESERVATIONS §11, D-P13-10).
pub const CLUSTER_CA_SECRET: &str = "__ostro/cluster-ca";
/// Node join token id prefix (RESERVATIONS §11).
pub const NODE_TOKEN_PREFIX: &str = "njt_";
/// Node join token scope (RESERVATIONS §11).
pub const NODE_JOIN_SCOPE: &str = "node:join";
/// `details.reason` when a resume names a tombstoned id (the joiner then discards its
/// marker and allocates a new id, NW1).
pub const REASON_MEMBER_REMOVED: &str = "member_removed";
/// `details.reason` when a resume names an Active id.
pub const REASON_MEMBER_ACTIVE: &str = "member_active";
/// `details.reason` when the joiner's level range does not contain the finalized level.
pub const REASON_LEVEL_MISMATCH: &str = "level_mismatch";

const TOKEN_ID_CHARS: usize = 10;
const SECRET_CHARS: usize = 32;
const ALPHABET: &[u8; 32] = b"0123456789abcdefghjkmnpqrstvwxyz";
const BACKDATE_SECS: i64 = 60;

fn random_base32(n: usize) -> Result<String, ApiError> {
    let mut b = vec![0u8; n];
    getrandom::fill(&mut b).map_err(|e| ApiError::internal(format!("OS RNG: {e}")))?;
    Ok(b.iter()
        .map(|x| ALPHABET[usize::from(x & 31)] as char)
        .collect())
}

fn sha256(b: &[u8]) -> [u8; 32] {
    Sha256::digest(b).into()
}

fn ct_eq(a: &[u8; 32], b: &[u8; 32]) -> bool {
    let mut d = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        d |= x ^ y;
    }
    std::hint::black_box(d) == 0
}

/// The cluster CA as stored in `__ostro/cluster-ca`. `Debug` never shows the key.
#[derive(Clone)]
pub struct StoredCa {
    /// CA certificate (PEM).
    pub cert_pem: String,
    /// CA private key (PEM).
    pub key_pem: String,
}

impl std::fmt::Debug for StoredCa {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoredCa").finish_non_exhaustive()
    }
}

impl StoredCa {
    fn encode(&self) -> Vec<u8> {
        let mut s = self.cert_pem.trim_end().to_owned();
        s.push('\n');
        s.push_str(self.key_pem.trim_end());
        s.push('\n');
        s.into_bytes()
    }

    fn decode(b: &[u8]) -> Option<Self> {
        let s = std::str::from_utf8(b).ok()?;
        let end = "-----END CERTIFICATE-----";
        let at = s.find(end)? + end.len();
        let (cert, key) = s.split_at(at);
        let key = key.trim();
        (key.contains("PRIVATE KEY") && cert.contains("BEGIN CERTIFICATE")).then(|| Self {
            cert_pem: format!("{}\n", cert.trim()),
            key_pem: format!("{key}\n"),
        })
    }

    /// The CA certificate DER.
    pub fn cert_der(&self) -> Option<CertificateDer<'static>> {
        CertificateDer::from_pem_slice(self.cert_pem.as_bytes()).ok()
    }

    /// `sha256:<hex>` of the CA certificate DER (the joiner's pin).
    pub fn ca_hash(&self) -> Option<String> {
        self.cert_der().map(|d| sha256_fingerprint(&d))
    }
}

/// The CA stored in meta: `Ok(None)` when absent. A `plaintext-v0` record is read as is; an
/// aes256gcm record opens with `store` (the node's `p14::reseal::cluster_store`).
pub fn stored_ca(
    state: &MetaState,
    store: Option<&ostro_bridge::secrets::SealedStore>,
) -> Result<Option<StoredCa>, SystemSecretError> {
    let Some(v) = open_system_secret(state, CLUSTER_CA_SECRET, store)? else {
        return Ok(None);
    };
    StoredCa::decode(v.expose())
        .map(Some)
        .ok_or_else(|| SystemSecretError::Malformed {
            name: CLUSTER_CA_SECRET.to_owned(),
            reason: "not a certificate PEM followed by a private key PEM".into(),
        })
}

/// Whether `__ostro/cluster-ca` exists (sealed or not).
pub fn has_stored_ca(state: &MetaState) -> bool {
    state
        .get(&MetaKey::Secret(CLUSTER_CA_SECRET.to_owned()))
        .is_some()
}

fn sealed_store(ctx: &P13Ctx) -> Result<Option<ostro_bridge::secrets::SealedStore>, ApiError> {
    cluster_store(&ctx.cfg)
        .map_err(|e| ApiError::unavailable(format!("this node's sealing key: {e}")))
}

/// [`stored_ca`] with this node's sealing key; a CA it cannot open is `unavailable`.
pub fn read_stored_ca(ctx: &P13Ctx) -> Result<Option<StoredCa>, ApiError> {
    let store = sealed_store(ctx)?;
    ctx.meta
        .view()
        .read(|s| stored_ca(s, store.as_ref()))
        .map_err(|e| ApiError::unavailable(e.to_string()))
}

/// Store this node's CA (`<data>/tls/ca.{crt,key}`) as `__ostro/cluster-ca` if absent
/// (create-only, never overwrite), sealed when an operator sealing key is configured. A
/// node without a CA key stores nothing.
pub async fn ensure_cluster_ca(ctx: &P13Ctx) -> Result<(), ApiError> {
    if ctx.meta.view().read(has_stored_ca) {
        return Ok(());
    }
    let dir = ctx.cfg.node.data_dir.join("tls");
    let (Ok(cert_pem), Ok(key_pem)) = (
        std::fs::read_to_string(dir.join(ClusterCa::CERT_FILE)),
        std::fs::read_to_string(dir.join(ClusterCa::KEY_FILE)),
    ) else {
        return Ok(());
    };
    let ca = StoredCa { cert_pem, key_pem };
    if ca.cert_der().is_none() || KeyPair::from_pem(&ca.key_pem).is_err() {
        warn!("this node's CA files are unusable; __ostro/cluster-ca not stored");
        return Ok(());
    }
    let record = match sealed_store(ctx)? {
        Some(store) => store
            .seal(CLUSTER_CA_SECRET, &ca.encode())
            .map_err(|e| ApiError::internal(format!("sealing the cluster CA: {e}")))?,
        None => SealedSecret {
            name: CLUSTER_CA_SECRET.to_owned(),
            ciphertext: ca.encode(),
            key_id: TOKEN_KEY_PLAINTEXT_ID.to_owned(),
            ext: Vec::new(),
        },
    };
    let res = ctx
        .meta
        .propose(MetaCommand::Put {
            key: MetaKey::Secret(CLUSTER_CA_SECRET.to_owned()),
            expected_version: None,
            record: MetaRecord::Secret(record),
        })
        .await
        .map_err(|e| ApiError::unavailable(format!("storing the cluster CA: {e}")))?;
    match res {
        MetaResult::Ok { .. } => {
            info!(
                node = ctx.node_id(),
                "cluster CA stored in metadata ({CLUSTER_CA_SECRET})"
            );
            Ok(())
        }
        MetaResult::Conflict { .. } | MetaResult::StaleVersion { .. } => Ok(()),
        other => Err(ApiError::internal(format!(
            "storing the cluster CA: {other:?}"
        ))),
    }
}

/// The stored CA, storing this node's first when absent.
async fn cluster_ca(ctx: &P13Ctx) -> Result<StoredCa, ApiError> {
    if let Some(ca) = read_stored_ca(ctx)? {
        return Ok(ca);
    }
    ensure_cluster_ca(ctx).await?;
    read_stored_ca(ctx)?.ok_or_else(|| {
        ApiError::unavailable(format!(
            "no cluster CA in metadata ({CLUSTER_CA_SECRET}); start a node that holds the CA \
             (`ostro cluster init` or the shared [cluster.tls] CA files)"
        ))
    })
}

/// `POST /v1/cluster/tokens`: a `njt_` node join token (scope `node:join`).
pub async fn create_node_token(
    ctx: &P13Ctx,
    r: NodeTokenRequest,
) -> Result<NodeTokenResponse, ApiError> {
    let ttl = r
        .ttl_secs
        .map(Duration::from_secs)
        .unwrap_or_else(|| ctx.cfg.workers.join_token_ttl.get());
    if ttl.is_zero() {
        return Err(ApiError::invalid_argument("token ttl must be > 0"));
    }
    let ca_hash = cluster_ca(ctx)
        .await?
        .ca_hash()
        .ok_or_else(|| ApiError::internal("the stored cluster CA is unreadable"))?;
    let ttl_ms = u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX);
    for _ in 0..4 {
        let id = format!("{NODE_TOKEN_PREFIX}{}", random_base32(TOKEN_ID_CHARS)?);
        let secret = random_base32(SECRET_CHARS)?;
        let now = ctx.clock.now_ms();
        let expires_at_ms = now.saturating_add(ttl_ms);
        let res = ctx
            .meta
            .propose(MetaCommand::Put {
                key: MetaKey::Token(id.clone()),
                expected_version: None,
                record: MetaRecord::Token(ApiToken {
                    id: id.clone(),
                    hash: sha256(secret.as_bytes()),
                    scopes: vec![NODE_JOIN_SCOPE.to_owned()],
                    tenant: None,
                    created_at_ms: now,
                    expires_at_ms: Some(expires_at_ms),
                    ext: Vec::new(),
                }),
            })
            .await
            .map_err(|e| ApiError::unavailable(format!("storing the node token: {e}")))?;
        match res {
            MetaResult::Ok { .. } => {
                info!(token_id = %id, "node join token created");
                return Ok(NodeTokenResponse {
                    token: format!("{id}.{secret}"),
                    id,
                    expires_at_ms,
                    ca_hash,
                });
            }
            MetaResult::Conflict { .. } => continue,
            other => {
                return Err(ApiError::internal(format!(
                    "storing the node token: {other:?}"
                )));
            }
        }
    }
    Err(ApiError::internal("could not allocate a unique token id"))
}

fn invalid_token() -> ApiError {
    ApiError::invalid_argument("node join token is invalid, revoked or expired")
        .with_details(serde_json::json!({ "reason": REASON_JOIN_TOKEN_INVALID }))
}

/// Check `njt_<id>.<secret>` (constant-time hash, scope, expiry; present = not revoked).
fn verify_token(state: &MetaState, token: &str, now_ms: u64) -> Result<String, ApiError> {
    let (id, secret) = token.split_once('.').ok_or_else(invalid_token)?;
    if !id.starts_with(NODE_TOKEN_PREFIX) || secret.is_empty() {
        return Err(invalid_token());
    }
    let hash = sha256(secret.as_bytes());
    let ok = state.token(id).is_some_and(|t| {
        let hash_ok = ct_eq(&t.value.hash, &hash);
        let scope_ok = t.value.scopes.iter().any(|s| s == NODE_JOIN_SCOPE);
        let live = t.value.expires_at_ms.is_none_or(|e| now_ms < e);
        hash_ok & scope_ok & live
    });
    if ok {
        Ok(id.to_owned())
    } else {
        Err(invalid_token())
    }
}

/// A CSR as DER (PEM or raw DER).
fn csr_der(csr: &str) -> Result<Vec<u8>, ApiError> {
    let b = csr.as_bytes();
    let (_, pem) = x509_parser::pem::parse_x509_pem(b)
        .map_err(|_| ApiError::invalid_argument("csr: not a PEM certificate request"))?;
    if !pem.label.contains("CERTIFICATE REQUEST") {
        return Err(ApiError::invalid_argument(format!(
            "csr: PEM label {:?} is not a certificate request",
            pem.label
        )));
    }
    Ok(pem.contents)
}

fn unix_now() -> i64 {
    // clock-exempt: X.509 validity timestamps are checked by peers against real time.
    #[allow(clippy::disallowed_methods)]
    let now = std::time::SystemTime::now(); // clock-exempt: real-time: X.509 validity
    now.duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

fn san_rcgen(s: &San) -> Result<SanType, ApiError> {
    let bad = |what: &str| {
        ApiError::invalid_argument(format!("advertised name {what:?} is not a valid SAN"))
    };
    Ok(match s {
        San::Dns(d) => SanType::DnsName(d.as_str().try_into().map_err(|_| bad(d))?),
        San::Ip(ip) => SanType::IpAddress(*ip),
        San::Uri(u) => SanType::URI(u.as_str().try_into().map_err(|_| bad(u))?),
    })
}

/// Sign the joiner's CSR as the node certificate of `node_id` (chain PEM: leaf + CA).
pub fn sign_node_csr(
    ca: &StoredCa,
    cluster_id: &str,
    node_id: u64,
    csr_pem: &str,
    advertise: &[&str],
) -> Result<String, ApiError> {
    let der = csr_der(csr_pem)?;
    {
        let (_, req) = X509CertificationRequest::from_der(&der)
            .map_err(|_| ApiError::invalid_argument("csr: unparseable"))?;
        let spki = &req.certification_request_info.subject_pki;
        let alg = &spki.algorithm.algorithm;
        let ok = if *alg == OID_KEY_TYPE_EC_PUBLIC_KEY {
            spki.algorithm
                .parameters
                .as_ref()
                .and_then(|p| p.as_oid().ok())
                .is_some_and(|c| c == OID_EC_P256)
        } else {
            *alg == OID_SIG_ED25519
        };
        if !ok {
            return Err(ApiError::invalid_argument(
                "csr: key must be ECDSA P-256 or Ed25519",
            ));
        }
    }
    let csr = CertificateSigningRequestParams::from_der(&CertificateSigningRequestDer::from(
        der.as_slice(),
    ))
    .map_err(|e| match e {
        rcgen::Error::InvalidCertificationRequestSignature => {
            ApiError::invalid_argument("csr: signature does not verify")
        }
        other => ApiError::invalid_argument(format!("csr: unusable: {other}")),
    })?;
    let ca_der = ca
        .cert_der()
        .ok_or_else(|| ApiError::internal("the stored cluster CA certificate is unreadable"))?;
    let key = KeyPair::from_pem(&ca.key_pem)
        .map_err(|_| ApiError::internal("the stored cluster CA key is unreadable"))?;
    let issuer = Issuer::from_ca_cert_der(&ca_der, key)
        .map_err(|e| ApiError::internal(format!("cluster CA: {e}")))?;
    let mut sans: BTreeSet<San> = advertise.iter().filter_map(|a| San::from_host(a)).collect();
    sans.insert(San::Dns("localhost".to_owned()));
    sans.insert(San::Ip(IpAddr::from([127, 0, 0, 1])));
    sans.insert(San::Ip(IpAddr::from(std::net::Ipv6Addr::LOCALHOST)));
    // Exactly one identity URI, assigned here (never from the CSR).
    sans.insert(San::Uri(NodeIdentity::uri(cluster_id, node_id)));
    let mut p = CertificateParams::default();
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, format!("ostro node {node_id}"));
    p.distinguished_name = dn;
    p.subject_alt_names = sans.iter().map(san_rcgen).collect::<Result<_, _>>()?;
    p.is_ca = IsCa::ExplicitNoCa;
    p.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    p.extended_key_usages = vec![
        ExtendedKeyUsagePurpose::ServerAuth,
        ExtendedKeyUsagePurpose::ClientAuth,
    ];
    p.use_authority_key_identifier_extension = true;
    let mut serial = [0u8; 16];
    getrandom::fill(&mut serial).map_err(|e| ApiError::internal(format!("OS RNG: {e}")))?;
    serial[0] &= 0x7f;
    p.serial_number = Some(SerialNumber::from_slice(&serial));
    let now = unix_now();
    let life = i64::try_from(LEAF_LIFETIME.as_secs()).unwrap_or(i64::MAX / 4);
    let t = |secs: i64| {
        x509_parser::time::ASN1Time::from_timestamp(secs)
            .map(|t| t.to_datetime())
            .map_err(|_| ApiError::internal("certificate time out of range"))
    };
    p.not_before = t(now - BACKDATE_SECS)?;
    p.not_after = t(now.saturating_add(life))?;
    let cert = p
        .signed_by(&csr.public_key, &issuer)
        .map_err(|e| ApiError::internal(format!("node certificate signing: {e}")))?;
    let mut chain = pem_encode("CERTIFICATE", cert.der());
    chain.push_str(&pem_encode("CERTIFICATE", &ca_der));
    Ok(chain)
}

fn node_version_of(r: &JoinRequest, now_ms: u64) -> NodeVersion {
    let v = &r.node_version;
    NodeVersion {
        ostro_version: v.ostro_version.clone(),
        level_min: v.level_min,
        level_max: v.level_max,
        wasmtime: v.wasmtime.clone(),
        event_codec_ver: v.event_codec_ver,
        wit_lines: v.wit_lines.clone(),
        reported_at_ms: now_ms,
    }
}

/// The seeds handed to a joiner: every non-removed member with a cluster address.
fn seeds_of(state: &MetaState, except: u64) -> Vec<SeedDto> {
    state
        .members()
        .filter(|m| m.value.node_id != except && member_state_of(&m.value) != MEMBER_REMOVED)
        .filter_map(|m| {
            m.value
                .cluster_addr
                .clone()
                .filter(|a| !a.is_empty())
                .map(|addr| SeedDto {
                    id: m.value.node_id,
                    addr,
                    repl_addr: m.value.repl_addr.clone(),
                })
        })
        .collect()
}

/// Allocate a fresh node id: create-only `Put Member{max + 1, Joining}`, retried on
/// Conflict.
async fn allocate(ctx: &P13Ctx, r: &JoinRequest, now: u64) -> Result<u64, ApiError> {
    let ext = put_member_state(
        &[],
        &MemberState {
            state: MEMBER_JOINING,
            changed_at_ms: now,
        },
    )
    .and_then(|e| put_node_version(&e, &node_version_of(r, now)))
    .map_err(|e| ApiError::internal(format!("member ext: {e}")))?;
    for _ in 0..16 {
        let id = ctx
            .meta
            .view()
            .read(|s| s.members().map(|m| m.value.node_id).max().unwrap_or(0))
            + 1;
        let record = Member {
            node_id: id,
            addr: r.advertise.api.clone(),
            joined_at_ms: now,
            cluster_addr: Some(r.advertise.cluster.clone()).filter(|a| !a.is_empty()),
            repl_addr: r.advertise.repl.clone().filter(|a| !a.is_empty()),
            ext: ext.clone(),
        };
        let res = ctx
            .meta
            .propose(MetaCommand::Put {
                key: MetaKey::Member(id),
                expected_version: None,
                record: MetaRecord::Member(record),
            })
            .await
            .map_err(|e| ApiError::unavailable(format!("allocating a node id: {e}")))?;
        match res {
            MetaResult::Ok { .. } => return Ok(id),
            MetaResult::Conflict { .. } | MetaResult::StaleVersion { .. } => continue,
            other => {
                return Err(ApiError::internal(format!(
                    "allocating a node id: {other:?}"
                )));
            }
        }
    }
    Err(ApiError::unavailable(
        "node id allocation kept conflicting; retry the join",
    ))
}

/// Resume `id` (a still-Joining member): refresh `MemberState.changed_at_ms` so the orphan
/// clock restarts, allocate nothing.
async fn resume(ctx: &P13Ctx, id: u64, now: u64) -> Result<(), ApiError> {
    let st = ctx
        .meta
        .view()
        .read(|s| s.member(id).map(|m| member_state_of(&m.value)));
    match st {
        None => Err(ApiError::not_found(format!(
            "node {id} is not a member; remove the join marker and join again"
        ))),
        Some(MEMBER_REMOVED) => Err(ApiError::precondition_failed(format!(
            "node {id} was removed (tombstone); join again as a new node"
        ))
        .with_details(serde_json::json!({ "reason": REASON_MEMBER_REMOVED, "node_id": id }))),
        Some(MEMBER_JOINING) => refresh_joining(ctx, id, now).await,
        Some(_) => Err(ApiError::precondition_failed(format!(
            "node {id} already joined (active); it cannot be re-certified through a join"
        ))
        .with_details(serde_json::json!({ "reason": REASON_MEMBER_ACTIVE, "node_id": id }))),
    }
}

async fn refresh_joining(ctx: &P13Ctx, id: u64, now: u64) -> Result<(), ApiError> {
    for _ in 0..8 {
        let Some(cur) = ctx.meta.view().read(|s| s.member(id).cloned()) else {
            return Err(ApiError::not_found(format!("node {id} is not a member")));
        };
        if member_state_of(&cur.value) != MEMBER_JOINING {
            return Err(ApiError::conflict(format!(
                "node {id} changed state during the resume; retry"
            )));
        }
        let mut record = cur.value.clone();
        record.ext = put_member_state(
            &record.ext,
            &MemberState {
                state: MEMBER_JOINING,
                changed_at_ms: now,
            },
        )
        .map_err(|e| ApiError::internal(format!("member ext: {e}")))?;
        let res = ctx
            .meta
            .propose(MetaCommand::Put {
                key: MetaKey::Member(id),
                expected_version: Some(cur.version),
                record: MetaRecord::Member(record),
            })
            .await
            .map_err(|e| ApiError::unavailable(format!("resuming node {id}: {e}")))?;
        match res {
            MetaResult::Ok { .. } => return Ok(()),
            MetaResult::StaleVersion { .. } | MetaResult::Conflict { .. } => continue,
            other => return Err(ApiError::internal(format!("resuming node {id}: {other:?}"))),
        }
    }
    Err(ApiError::unavailable(
        "the member record kept changing; retry",
    ))
}

/// `POST /v1/cluster/join`: admit a joining node (any member serves it).
pub async fn handle_join(ctx: &P13Ctx, r: JoinRequest) -> Result<JoinResponse, ApiError> {
    let now = ctx.clock.now_ms();
    let token_id = ctx.meta.view().read(|s| verify_token(s, &r.token, now))?;
    let (finalized, cluster_id, partitions) = ctx.meta.view().read(|s| {
        (
            ostro_cluster::compat::finalized_level(s),
            s.cluster().map(|c| c.value.cluster_id.clone()),
            s.cluster().map_or(0, |c| c.value.partitions),
        )
    });
    let cluster_id =
        cluster_id.ok_or_else(|| ApiError::unavailable("the cluster is not initialized"))?;
    let (lo, hi) = (r.node_version.level_min, r.node_version.level_max);
    if !(lo <= finalized && finalized <= hi) {
        return Err(ApiError::precondition_failed(format!(
            "the joining binary supports feature levels {lo}..={hi} but the cluster is \
             finalized at level {finalized}; use a matching Ostro version"
        ))
        .with_details(serde_json::json!({
            "reason": REASON_LEVEL_MISMATCH, "finalized": finalized, "level_min": lo, "level_max": hi,
        })));
    }
    let ca = cluster_ca(ctx).await?;
    // Validate the CSR before any meta write.
    csr_der(&r.csr_pem)?;
    let node_id = match r.resume_node_id {
        Some(id) => {
            resume(ctx, id, now).await?;
            id
        }
        None => allocate(ctx, &r, now).await?,
    };
    let mut names: Vec<&str> = vec![r.advertise.api.as_str(), r.advertise.cluster.as_str()];
    if let Some(repl) = &r.advertise.repl {
        names.push(repl);
    }
    let node_cert_pem = sign_node_csr(&ca, &cluster_id, node_id, &r.csr_pem, &names)?;
    let seeds = ctx.meta.view().read(|s| seeds_of(s, node_id));
    info!(
        token_id = %token_id,
        node_id,
        resumed = r.resume_node_id.is_some(),
        "node join admitted"
    );
    Ok(JoinResponse {
        node_id,
        cluster_id,
        partitions,
        ca_cert_pem: ca.cert_pem.clone(),
        node_cert_pem,
        seeds,
        finalized_level: finalized,
    })
}
