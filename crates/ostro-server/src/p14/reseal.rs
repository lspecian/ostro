//! Phase 14 (14-19, D-P14-30): system-secret sealing at startup, plus the
//! `[security.modules]` policy seed (`security_api::seed_task`, which needs the same
//! "after start, on the meta leader" slot).
//!
//! # What is sealed now
//!
//! On the meta leader, when a sealing key resolves (`OSTRO_SEALING_KEY`, `[secrets]
//! sealing_key_file`, or the dev `<data>/sealing.key`), [`run`]:
//!
//! - creates `__ostro/ui-session-key` (32 random bytes) sealed with aes256gcm if absent;
//! - re-seals it from `plaintext-v0` to aes256gcm if it was stored in plaintext (14-21
//!   creates it as `plaintext-v0` in token mode); like the secrets below it is cluster-wide
//!   (a UI session cookie verifies on every node), so (14-21) it is sealed only with an
//!   operator-supplied key, never the per-node dev key;
//! - (p14-fixups-2) re-seals `__ostro/token-key` and `__ostro/cluster-ca` from `plaintext-v0`
//!   to aes256gcm, **only with an operator-supplied key** (`OSTRO_SEALING_KEY` or `[secrets]
//!   sealing_key_file`, [`cluster_ring`]); they are never created here (InitCluster and
//!   `p13::join::ensure_cluster_ca` create them);
//!
//! each under CAS, idempotent (an aes256gcm record is left alone), with ext 117
//! `SecretTimes` (`created_at_ms` / `rotated_at_ms`, injected clock; only once
//! `compat::require(Feature::SecretTimesExt)` passes) and an audit record `system.reseal`
//! through `p14::audit_sink`.
//!
//! # Cluster-wide system secrets: every node needs the same sealing key
//!
//! [`CLUSTER_WIDE`] secrets are read by every node: the await-token key at every boot
//! (`cluster.rs`, `server::init_cluster`) and on token completion / worker appends, the
//! cluster CA by joins (`p13::join`, the member signing the joiner's CSR), by a joined
//! node's Phase 7 TLS import (`p13::joiner::import_cluster_ca`) and by `ostro restore`.
//! Their readers go through [`open_system_secret`] / [`token_key`], which read legacy
//! `plaintext-v0` records as raw bytes and open aes256gcm records with the node's
//! [`cluster_ring`]. Once sealed, **every node of the cluster must be configured with the
//! same sealing key** (as Phase 6 sealed secrets already require): a node without it, or
//! with another key, refuses to boot with [`SystemSecretError`] naming the secret and the
//! key id. The dev auto-generated `<data>/sealing.key` is per node, so it never seals them
//! (a second node or a joiner could not open them); they stay `plaintext-v0` then.
//!
//! The join handshake itself ships only the CA *certificate* (`JoinResponse.ca_cert_pem`,
//! public) to the joiner; the CA key stays in meta and is opened by the serving member. A
//! joiner opens the sealed records only after its metadata caught up, with its own
//! configured key (it fails its boot with the clear error above without it).
//!
//! Without a sealing key and with `[auth] mode = "token"`, a WARN is logged once.
//!
//! # Key rotation (p14-key-rotation)
//!
//! A node loads old sealing keys next to the current one (`OSTRO_SEALING_KEY_OLD`, `[secrets]
//! old_sealing_key_files`; `SealingKeyRing::load`): every record names its key
//! (`aes256gcm:<kid>`), so records sealed with an old key keep opening, and every new seal
//! uses the current key. The startup [`run`] never re-keys (during a rolling key change the
//! other nodes may not hold the new key yet). The explicit step is [`reseal_all`]
//! (`POST /v1/security/sealing/reseal`, `ostro secret reseal`): it re-seals every stored
//! record that is not on the current key, under CAS, idempotently, and audits one
//! `system.reseal` record with the counts. [`sealing_status`] tells the operator when no
//! record needs an old key any more. The [`RESEAL_NOW`] system secrets (all cluster-wide since
//! 14-21) keep the operator-key-only rule: with only the dev auto key they are skipped, never
//! sealed.

use std::sync::Arc;
use std::time::Duration;

use std::collections::BTreeMap;

use ostro_api::ApiError;
use ostro_api::phase14::{ResealResult, SealingKeyUse, SealingStatus};
use ostro_bridge::secrets::{SealedStore, SealingKeyRing, SecretValue};
use ostro_meta::ext::{SecretTimes, get_secret_times, put_secret_times};
use ostro_meta::records::{SealedSecret, TOKEN_KEY_PLAINTEXT_ID, TOKEN_KEY_SECRET};
use ostro_meta::{MetaCommand, MetaKey, MetaRecord, MetaResult, MetaState};
use ostro_types::audit::{ActorKind, AuditEvent};
use ostro_types::compat::Feature;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::P14Ctx;
use crate::p13::join::CLUSTER_CA_SECRET;

/// The UI session signing key (14-21 reads it).
pub const UI_SESSION_KEY_SECRET: &str = "__ostro/ui-session-key";
/// System secrets sealed at startup.
pub const RESEAL_NOW: [&str; 3] = [UI_SESSION_KEY_SECRET, TOKEN_KEY_SECRET, CLUSTER_CA_SECRET];
/// Cluster-wide system secrets (module docs): re-sealed only with an operator-supplied key
/// ([`cluster_ring`]), never created by [`run`].
pub const CLUSTER_WIDE: [&str; 2] = [TOKEN_KEY_SECRET, CLUSTER_CA_SECRET];
/// Audit action of a reseal / creation.
pub const AUDIT_ACTION: &str = "system.reseal";
/// CAS attempts per secret.
const CAS_RETRIES: usize = 8;
/// How often a non-leader re-checks for leadership.
const LEADER_POLL: Duration = Duration::from_secs(1);

/// What happened to one secret.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SecretOutcome {
    /// Created sealed.
    Created,
    /// Re-sealed from `plaintext-v0`.
    Resealed,
    /// Already aes256gcm: untouched.
    AlreadySealed,
    /// A [`CLUSTER_WIDE`] secret not written yet (nothing to seal; never created here), or
    /// (re-key) a record deleted meanwhile.
    Absent,
    /// (Re-key) moved from an old key to the current key.
    Rekeyed,
    /// (Re-key) a `plaintext-v0` record that is not a [`RESEAL_NOW`] system secret: left
    /// alone.
    Skipped,
}

/// What [`seal_record`] does with an aes256gcm record under another key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    /// Startup: create / seal plaintext; any aes256gcm record is left alone.
    Startup,
    /// Explicit reseal: also move records from an old key to the current key; never create.
    Rekey,
}

/// The result of [`run`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ResealReport {
    /// No sealing key resolved: nothing was changed.
    pub no_key: bool,
    /// The no-key WARN was logged (token mode).
    pub warned: bool,
    /// Per secret of [`RESEAL_NOW`].
    pub secrets: Vec<(String, SecretOutcome)>,
}

/// The node's sealing key ring, if one resolves (the bridge's rules; the dev key file once
/// the bridge created it).
pub fn load_ring(ctx: &P14Ctx) -> Option<SealingKeyRing> {
    try_load_ring(ctx).ok().flatten()
}

/// [`load_ring`] with the loading error (a configured key that is unreadable or invalid).
pub fn try_load_ring(ctx: &P14Ctx) -> Result<Option<SealingKeyRing>, String> {
    let mut secrets = ctx.cfg.secrets.to_bridge().map_err(|e| e.to_string())?;
    let dev_key = ctx
        .data_dir
        .join(ostro_bridge::secrets::sealed::DEV_KEY_FILE);
    if secrets.sealing_key_file.is_none() && dev_key.is_file() {
        secrets.sealing_key_file = Some(dev_key);
    }
    SealingKeyRing::load(&secrets, &ctx.data_dir, false).map_err(|e| e.to_string())
}

/// The operator-supplied sealing key ring (`OSTRO_SEALING_KEY`, else `[secrets]
/// sealing_key_file`), never the dev auto key: the ring of the [`CLUSTER_WIDE`] secrets
/// (module docs). `Ok(None)` = no key configured; a configured but unreadable or invalid
/// key is an error.
pub fn cluster_ring(cfg: &crate::config::ServerConfig) -> Result<Option<SealingKeyRing>, String> {
    let secrets = cfg.secrets.to_bridge().map_err(|e| e.to_string())?;
    SealingKeyRing::load(&secrets, &cfg.node.data_dir, false).map_err(|e| e.to_string())
}

/// [`cluster_ring`] as a store.
pub fn cluster_store(cfg: &crate::config::ServerConfig) -> Result<Option<SealedStore>, String> {
    Ok(cluster_ring(cfg)?.map(|r| SealedStore::new(Arc::new(r))))
}

/// An opener for `ostro restore` (`RestoreOptions::open_secret`): the operator key from
/// `OSTRO_SEALING_KEY` (the restore has no config file). `Ok(None)` = the variable is unset.
pub fn env_secret_opener() -> Result<Option<ostro_backup::restore::SecretOpener>, String> {
    let ring = SealingKeyRing::load(
        &ostro_bridge::secrets::SecretsConfig::default(),
        std::path::Path::new("."),
        false,
    )
    .map_err(|e| e.to_string())?;
    Ok(ring.map(|r| {
        let store = SealedStore::new(Arc::new(r));
        ostro_backup::restore::SecretOpener(Arc::new(move |rec: &SealedSecret| {
            store
                .open(rec)
                .map(|v| v.expose().to_vec())
                .map_err(|e| format!("cannot be opened with OSTRO_SEALING_KEY: {e}"))
        }))
    }))
}

/// A system secret that exists but cannot be read on this node.
#[derive(Debug, thiserror::Error)]
pub enum SystemSecretError {
    /// Sealed, and this node has no sealing key.
    #[error(
        "{name} is sealed ({key_id}) and this node has no sealing key: every node of a cluster \
         needs the same sealing key (set OSTRO_SEALING_KEY or [secrets] sealing_key_file)"
    )]
    NoKey {
        /// The secret.
        name: String,
        /// Its `key_id`.
        key_id: String,
    },
    /// Sealed under a key this node does not hold, or tampered.
    #[error(
        "{name} ({key_id}) cannot be opened with this node's sealing key: {reason}; every node \
         of a cluster needs the same sealing key (OSTRO_SEALING_KEY or [secrets] \
         sealing_key_file)"
    )]
    Open {
        /// The secret.
        name: String,
        /// Its `key_id`.
        key_id: String,
        /// The opening failure (never key material).
        reason: String,
    },
    /// Opened, but the value has the wrong shape.
    #[error("{name} is malformed: {reason}")]
    Malformed {
        /// The secret.
        name: String,
        /// What is wrong.
        reason: String,
    },
}

/// Read system secret `name`: `Ok(None)` when absent; a `plaintext-v0` record is returned
/// as is (clusters created before sealing); an aes256gcm record is opened with `store`.
pub fn open_system_secret(
    state: &MetaState,
    name: &str,
    store: Option<&SealedStore>,
) -> Result<Option<SecretValue>, SystemSecretError> {
    let Some(v) = state.get(&MetaKey::Secret(name.to_owned())) else {
        return Ok(None);
    };
    let MetaRecord::Secret(rec) = &v.value else {
        return Err(SystemSecretError::Malformed {
            name: name.to_owned(),
            reason: "not a secret record".into(),
        });
    };
    open_record(name, rec, store).map(Some)
}

/// [`open_system_secret`] over a record already read.
pub fn open_record(
    name: &str,
    rec: &SealedSecret,
    store: Option<&SealedStore>,
) -> Result<SecretValue, SystemSecretError> {
    if rec.key_id == TOKEN_KEY_PLAINTEXT_ID {
        return Ok(SecretValue::from_slice(&rec.ciphertext));
    }
    let Some(store) = store else {
        return Err(SystemSecretError::NoKey {
            name: name.to_owned(),
            key_id: rec.key_id.clone(),
        });
    };
    store.open(rec).map_err(|e| SystemSecretError::Open {
        name: name.to_owned(),
        key_id: rec.key_id.clone(),
        reason: e.to_string(),
    })
}

/// The await-token key (`__ostro/token-key`, 32 bytes): `Ok(None)` before InitCluster.
pub fn token_key(
    state: &MetaState,
    store: Option<&SealedStore>,
) -> Result<Option<[u8; 32]>, SystemSecretError> {
    let Some(v) = open_system_secret(state, TOKEN_KEY_SECRET, store)? else {
        return Ok(None);
    };
    let key: [u8; 32] = v
        .expose()
        .try_into()
        .map_err(|_| SystemSecretError::Malformed {
            name: TOKEN_KEY_SECRET.to_owned(),
            reason: format!("{} bytes, expected 32", v.expose().len()),
        })?;
    Ok(Some(key))
}

async fn audit(ctx: &P14Ctx, name: &str, outcome: SecretOutcome, key_id: &str) {
    let mut ev = AuditEvent::new(AUDIT_ACTION, "system", ActorKind::System, name);
    ev.details.insert(
        "result".into(),
        match outcome {
            SecretOutcome::Created => "created",
            SecretOutcome::Resealed => "resealed",
            SecretOutcome::AlreadySealed => "already_sealed",
            SecretOutcome::Absent => "absent",
            SecretOutcome::Rekeyed => "rekeyed",
            SecretOutcome::Skipped => "skipped",
        }
        .into(),
    );
    ev.details.insert("key_id".into(), key_id.to_owned());
    match ctx.audit.reserve() {
        Ok(permit) => {
            if let Err(e) = permit.commit(ev).await {
                warn!(secret = name, error = ?e, "system.reseal audit record not committed");
            }
        }
        Err(e) => warn!(secret = name, error = ?e, "system.reseal audit unavailable"),
    }
}

/// Seal (create or re-seal) one system secret.
pub async fn reseal_one(
    ctx: &P14Ctx,
    store: &SealedStore,
    name: &str,
) -> Result<SecretOutcome, String> {
    seal_record(ctx, store, name, Mode::Startup).await
}

/// Re-seal one stored record with `store`'s current key (the explicit reseal step): a record
/// already on the current key is untouched; one under an old key is opened and sealed again
/// (same value, ext kept, `rotated_at_ms` stamped); a `plaintext-v0` [`RESEAL_NOW`] secret is
/// sealed; nothing is created. Not audited per record ([`reseal_all`] audits the pass).
pub async fn rekey_one(
    ctx: &P14Ctx,
    store: &SealedStore,
    name: &str,
) -> Result<SecretOutcome, String> {
    seal_record(ctx, store, name, Mode::Rekey).await
}

async fn seal_record(
    ctx: &P14Ctx,
    store: &SealedStore,
    name: &str,
    mode: Mode,
) -> Result<SecretOutcome, String> {
    let times_ok =
        ostro_cluster::compat::require(ctx.meta.view(), Feature::SecretTimesExt, ctx.level_max)
            .is_ok();
    for _ in 0..CAS_RETRIES {
        let cur = ctx
            .meta
            .view()
            .read(|s| s.get(&MetaKey::Secret(name.to_owned())));
        let now = ctx.clock.now_ms();
        let (expected, mut rec, outcome) = match cur {
            None if mode == Mode::Rekey || CLUSTER_WIDE.contains(&name) => {
                return Ok(SecretOutcome::Absent);
            }
            None => {
                let mut v = [0u8; 32];
                getrandom::fill(&mut v).map_err(|e| format!("OS random source: {e}"))?;
                let sealed = store.seal(name, &v);
                v.fill(0);
                let mut rec = sealed.map_err(|e| e.to_string())?;
                if times_ok {
                    rec.ext = put_secret_times(
                        &rec.ext,
                        &SecretTimes {
                            created_at_ms: now,
                            rotated_at_ms: None,
                        },
                    )
                    .map_err(|e| e.to_string())?;
                }
                (None, rec, SecretOutcome::Created)
            }
            Some(v) => {
                let MetaRecord::Secret(old) = v.value else {
                    return Err(format!("{name} is not a secret record"));
                };
                let plaintext = old.key_id == TOKEN_KEY_PLAINTEXT_ID;
                if !plaintext
                    && (mode == Mode::Startup || old.key_id == store.ring().current_key_id())
                {
                    return Ok(SecretOutcome::AlreadySealed);
                }
                if plaintext && mode == Mode::Rekey && !RESEAL_NOW.contains(&name) {
                    return Ok(SecretOutcome::Skipped);
                }
                let sealed = if plaintext {
                    store.seal(name, &old.ciphertext)
                } else {
                    // The record's AAD binds its stored name: open it under that name.
                    let value = store.open(&old).map_err(|e| e.to_string())?;
                    store.seal(name, value.expose())
                };
                let mut rec = sealed.map_err(|e| e.to_string())?;
                // Keep every other ext tag; stamp the rotation time.
                rec.ext = old.ext.clone();
                if times_ok {
                    let t = get_secret_times(&old.ext).ok().flatten();
                    rec.ext = put_secret_times(
                        &rec.ext,
                        &SecretTimes {
                            created_at_ms: t.map_or(now, |t| t.created_at_ms),
                            rotated_at_ms: Some(now),
                        },
                    )
                    .map_err(|e| e.to_string())?;
                }
                let outcome = if plaintext {
                    SecretOutcome::Resealed
                } else {
                    SecretOutcome::Rekeyed
                };
                (Some(v.version), rec, outcome)
            }
        };
        rec.name = name.to_owned();
        let key_id = rec.key_id.clone();
        match ctx
            .meta
            .propose(MetaCommand::Put {
                key: MetaKey::Secret(name.to_owned()),
                expected_version: expected,
                record: MetaRecord::Secret(SealedSecret { ..rec }),
            })
            .await
            .map_err(|e| format!("metadata unavailable: {e}"))?
        {
            MetaResult::Ok { .. } => {
                info!(secret = name, ?outcome, key_id = %key_id, "secret sealed (aes256gcm)");
                if mode == Mode::Startup {
                    audit(ctx, name, outcome, &key_id).await;
                }
                return Ok(outcome);
            }
            MetaResult::StaleVersion { .. } | MetaResult::Conflict { .. } => {}
            other => return Err(format!("metadata rejected {name}: {other:?}")),
        }
    }
    Err(format!("{name}: lost the CAS {CAS_RETRIES} times"))
}

/// Seal the [`RESEAL_NOW`] system secrets (call on the meta leader). The
/// [`CLUSTER_WIDE`] ones only with [`cluster_ring`]'s key (module docs).
pub async fn run(ctx: &P14Ctx) -> ResealReport {
    let mut report = ResealReport::default();
    let Some(ring) = load_ring(ctx) else {
        report.no_key = true;
        if ctx.cfg.auth.mode.as_str() == "token" {
            report.warned = true;
            warn!(
                "no sealing key configured: system secrets ({}) stay unsealed in metadata; set \
                 OSTRO_SEALING_KEY or [secrets] sealing_key_file (the same key on every node)",
                RESEAL_NOW.join(", ")
            );
        }
        return report;
    };
    // 14-21: the UI session key is cluster-wide too (a cookie issued by one node must
    // verify on every node), so the per-node dev key never seals it either; without an
    // operator key it stays `plaintext-v0` (created by `ui_auth::ensure_key`).
    drop(ring);
    let cluster = match cluster_store(&ctx.cfg) {
        Ok(c) => c,
        Err(e) => {
            warn!(error = %e, "sealing key unusable for the cluster-wide system secrets");
            None
        }
    };
    if cluster.is_none() {
        info!(
            "{} stay unsealed: only an operator-supplied sealing key (OSTRO_SEALING_KEY or \
             [secrets] sealing_key_file, the same on every node) seals them, never the dev \
             auto-generated key",
            RESEAL_NOW.join(", ")
        );
    }
    for name in RESEAL_NOW {
        let Some(st) = &cluster else { continue };
        match reseal_one(ctx, st, name).await {
            Ok(o) => report.secrets.push((name.to_owned(), o)),
            Err(e) => warn!(secret = name, error = %e, "system secret not sealed"),
        }
    }
    report
}

// ---- key rotation (p14-key-rotation) --------------------------------------------------

/// Where one record's `key_id` stands against `ring`.
fn key_state(ring: Option<&SealingKeyRing>, key_id: &str) -> &'static str {
    if key_id == TOKEN_KEY_PLAINTEXT_ID {
        return "plaintext";
    }
    match ring {
        Some(r) if r.current_key_id() == key_id => "current",
        Some(r) if r.holds(key_id) => "old",
        _ => "unknown",
    }
}

/// Whether record `name` is sealed only with the operator key: the [`RESEAL_NOW`] system
/// secrets ([`CLUSTER_WIDE`] plus, since 14-21, the UI session key).
fn operator_only(name: &str) -> bool {
    RESEAL_NOW.contains(&name)
}

/// The two rings the records are judged against: the node ring ([`try_load_ring`]) for user
/// records, the operator ring ([`cluster_ring`]) for the [`operator_only`] ones.
struct Rings {
    node: Option<Arc<SealingKeyRing>>,
    cluster: Option<Arc<SealingKeyRing>>,
}

impl Rings {
    fn load(ctx: &P14Ctx) -> Result<Self, String> {
        Ok(Self {
            node: try_load_ring(ctx)?.map(Arc::new),
            cluster: cluster_ring(&ctx.cfg)?.map(Arc::new),
        })
    }

    fn for_record(&self, name: &str) -> Option<&SealingKeyRing> {
        if operator_only(name) {
            self.cluster.as_deref()
        } else {
            self.node.as_deref()
        }
    }
}

fn status_of(ctx: &P14Ctx, rings: &Rings) -> SealingStatus {
    let records: Vec<(String, String)> = ctx.meta.view().read(|s| {
        s.secrets()
            .map(|v| (v.value.name.clone(), v.value.key_id.clone()))
            .collect()
    });
    let mut st = SealingStatus {
        node: ctx.node_id,
        current_key_id: rings.node.as_deref().map(SealingKeyRing::current_key_id),
        old_key_ids: rings
            .node
            .as_deref()
            .map(SealingKeyRing::old_key_ids)
            .unwrap_or_default(),
        ..SealingStatus::default()
    };
    let mut keys: BTreeMap<(String, &'static str), u64> = BTreeMap::new();
    for (name, key_id) in &records {
        let state = key_state(rings.for_record(name), key_id);
        match state {
            "current" => st.current += 1,
            "old" => st.old += 1,
            "plaintext" => st.plaintext += 1,
            _ => st.unknown += 1,
        }
        *keys.entry((key_id.clone(), state)).or_default() += 1;
    }
    st.records = records.len() as u64;
    st.keys = keys
        .into_iter()
        .map(|((key_id, state), records)| SealingKeyUse {
            key_id,
            state: state.to_owned(),
            records,
        })
        .collect();
    st.old_keys_removable = st.old == 0;
    st
}

/// `GET /v1/security/sealing`: the stored records per key, against this node's key ring.
pub fn sealing_status(ctx: &P14Ctx) -> Result<SealingStatus, ApiError> {
    let rings = Rings::load(ctx).map_err(|e| {
        ApiError::precondition_failed(format!("sealing key unusable: {e}"))
            .with_details(serde_json::json!({ "reason": "sealing_key_invalid" }))
    })?;
    Ok(status_of(ctx, &rings))
}

/// `POST /v1/security/sealing/reseal`: re-seal every stored record with the current key
/// ([`rekey_one`] per record; the [`RESEAL_NOW`] system secrets only with the operator key), then
/// audit one `system.reseal` record with the counts. Idempotent: a second pass finds every
/// record current. Runs on any node (proposals are forwarded to the meta leader).
pub async fn reseal_all(ctx: &P14Ctx) -> Result<ResealResult, ApiError> {
    let rings = Rings::load(ctx).map_err(|e| {
        ApiError::precondition_failed(format!("sealing key unusable: {e}"))
            .with_details(serde_json::json!({ "reason": "sealing_key_invalid" }))
    })?;
    let Some(node_ring) = &rings.node else {
        return Err(
            ApiError::precondition_failed(crate::control::bridge::SEALING_KEY_HINT).with_details(
                serde_json::json!({ "reason": crate::bridge_services::SEALING_KEY_MISSING }),
            ),
        );
    };
    // Judge the newest records, not a lagging local view.
    ctx.meta
        .read_index_barrier()
        .await
        .map_err(|e| ApiError::unavailable(format!("metadata unavailable: {e}")))?;
    let key_id = node_ring.current_key_id();
    let node_store = SealedStore::new(Arc::clone(node_ring));
    let cluster_store = rings
        .cluster
        .as_ref()
        .map(|r| SealedStore::new(Arc::clone(r)));
    let names: Vec<String> = ctx
        .meta
        .view()
        .read(|s| s.secrets().map(|v| v.value.name.clone()).collect());
    let mut out = ResealResult {
        key_id: key_id.clone(),
        records: names.len() as u64,
        ..ResealResult::default()
    };
    for name in &names {
        let store = if operator_only(name) {
            match &cluster_store {
                Some(c) => c,
                None => {
                    out.skipped += 1;
                    continue;
                }
            }
        } else {
            &node_store
        };
        match rekey_one(ctx, store, name).await {
            Ok(SecretOutcome::Rekeyed | SecretOutcome::Resealed | SecretOutcome::Created) => {
                out.resealed += 1;
            }
            Ok(SecretOutcome::AlreadySealed) => out.already_current += 1,
            Ok(SecretOutcome::Skipped) => out.skipped += 1,
            Ok(SecretOutcome::Absent) => out.records = out.records.saturating_sub(1),
            Err(e) => {
                warn!(secret = %name, error = %e, "secret not re-sealed");
                out.failed.push(format!("{name}: {e}"));
            }
        }
    }
    let mut ev = AuditEvent::new(AUDIT_ACTION, "system", ActorKind::System, "secrets");
    for (k, v) in [
        ("result", "reseal".to_owned()),
        ("key_id", key_id),
        ("records", out.records.to_string()),
        ("resealed", out.resealed.to_string()),
        ("already_current", out.already_current.to_string()),
        ("skipped", out.skipped.to_string()),
        ("failed", out.failed.len().to_string()),
    ] {
        ev.details.insert(k.into(), v);
    }
    let permit = ctx
        .audit
        .reserve()
        .map_err(|e| ApiError::unavailable(format!("audit log unavailable: {e:?}")))?;
    permit
        .commit(ev)
        .await
        .map_err(|e| ApiError::unavailable(format!("system.reseal audit not committed: {e:?}")))?;
    info!(
        records = out.records,
        resealed = out.resealed,
        already_current = out.already_current,
        skipped = out.skipped,
        failed = out.failed.len(),
        "reseal pass done"
    );
    out.status = status_of(ctx, &rings);
    Ok(out)
}

/// The startup task: the policy seed (`security_api::seed_task`) and, once this node is the
/// meta leader, [`run`].
pub fn start(ctx: &Arc<P14Ctx>, stop: CancellationToken) -> Option<JoinHandle<()>> {
    let seed = super::security_api::seed_task(ctx, stop.clone());
    let weak = Arc::downgrade(ctx);
    Some(tokio::spawn(async move {
        loop {
            let Some(ctx) = weak.upgrade() else { break };
            if ctx.meta.is_leader() {
                run(&ctx).await;
                break;
            }
            drop(ctx);
            tokio::select! {
                () = stop.cancelled() => break,
                () = tokio::time::sleep(LEADER_POLL) => {}
            }
        }
        if let Some(seed) = seed {
            let _ = seed.await;
        }
    }))
}
