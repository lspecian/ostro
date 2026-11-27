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
//! - re-seals it from `plaintext-v0` to aes256gcm if it was stored in plaintext;
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

use std::sync::Arc;
use std::time::Duration;

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
    /// A [`CLUSTER_WIDE`] secret not written yet (nothing to seal; never created here).
    Absent,
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
    let mut secrets = ctx.cfg.secrets.to_bridge().ok()?;
    let dev_key = ctx
        .data_dir
        .join(ostro_bridge::secrets::sealed::DEV_KEY_FILE);
    if secrets.sealing_key_file.is_none() && dev_key.is_file() {
        secrets.sealing_key_file = Some(dev_key);
    }
    SealingKeyRing::load(&secrets, &ctx.data_dir, false)
        .ok()
        .flatten()
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
            None if CLUSTER_WIDE.contains(&name) => return Ok(SecretOutcome::Absent),
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
                if old.key_id != TOKEN_KEY_PLAINTEXT_ID {
                    return Ok(SecretOutcome::AlreadySealed);
                }
                let mut rec = store
                    .seal(name, &old.ciphertext)
                    .map_err(|e| e.to_string())?;
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
                (Some(v.version), rec, SecretOutcome::Resealed)
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
                info!(secret = name, ?outcome, "system secret sealed (aes256gcm)");
                audit(ctx, name, outcome, &key_id).await;
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
    let store = SealedStore::new(Arc::new(ring));
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
            CLUSTER_WIDE.join(" and ")
        );
    }
    for name in RESEAL_NOW {
        let st = if CLUSTER_WIDE.contains(&name) {
            match &cluster {
                Some(c) => c,
                None => continue,
            }
        } else {
            &store
        };
        match reseal_one(ctx, st, name).await {
            Ok(o) => report.secrets.push((name.to_owned(), o)),
            Err(e) => warn!(secret = name, error = %e, "system secret not sealed"),
        }
    }
    report
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
