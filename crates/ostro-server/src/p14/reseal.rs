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
//!
//! each under CAS, idempotent (an aes256gcm record is left alone), with ext 117
//! `SecretTimes` (`created_at_ms` / `rotated_at_ms`, injected clock; only once
//! `compat::require(Feature::SecretTimesExt)` passes) and an audit record `system.reseal`
//! through `p14::audit_sink`.
//!
//! # Deferred: `__ostro/token-key` and `__ostro/cluster-ca`
//!
//! [`DEFERRED`] secrets stay `plaintext-v0`. Their readers decode the record bytes
//! directly, without a sealing key ring: `MetaState::token_key()` (ostro-meta) used by
//! `server.rs`, `cluster.rs`, `control/lifecycle.rs` and `workers/remote_executor.rs`, and
//! `p13::join::stored_ca` / `p13::joiner::import_cluster_ca`. Sealing them before those
//! readers open aes256gcm records would break await tokens and node joins, and `server.rs`
//! is outside this plan (Phase 14 shared-file rule). The reseal routine below is generic;
//! adding a name to [`RESEAL_NOW`] is the whole change once the readers are converted.
//!
//! Without a sealing key and with `[auth] mode = "token"`, a WARN is logged once.

use std::sync::Arc;
use std::time::Duration;

use ostro_bridge::secrets::{SealedStore, SealingKeyRing};
use ostro_meta::ext::{SecretTimes, get_secret_times, put_secret_times};
use ostro_meta::records::{SealedSecret, TOKEN_KEY_PLAINTEXT_ID, TOKEN_KEY_SECRET};
use ostro_meta::{MetaCommand, MetaKey, MetaRecord, MetaResult};
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
pub const RESEAL_NOW: [&str; 1] = [UI_SESSION_KEY_SECRET];
/// System secrets left `plaintext-v0` until their readers open sealed records (see the
/// module docs).
pub const DEFERRED: [&str; 2] = [TOKEN_KEY_SECRET, CLUSTER_CA_SECRET];
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

async fn audit(ctx: &P14Ctx, name: &str, outcome: SecretOutcome, key_id: &str) {
    let mut ev = AuditEvent::new(AUDIT_ACTION, "system", ActorKind::System, name);
    ev.details.insert(
        "result".into(),
        match outcome {
            SecretOutcome::Created => "created",
            SecretOutcome::Resealed => "resealed",
            SecretOutcome::AlreadySealed => "already_sealed",
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

/// Seal the [`RESEAL_NOW`] system secrets (call on the meta leader).
pub async fn run(ctx: &P14Ctx) -> ResealReport {
    let mut report = ResealReport::default();
    let Some(ring) = load_ring(ctx) else {
        report.no_key = true;
        if ctx.cfg.auth.mode.as_str() == "token" {
            report.warned = true;
            warn!(
                "no sealing key configured: system secrets ({}) stay unsealed in metadata; set \
                 OSTRO_SEALING_KEY or [secrets] sealing_key_file",
                RESEAL_NOW
                    .iter()
                    .chain(DEFERRED.iter())
                    .copied()
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
        return report;
    };
    let store = SealedStore::new(Arc::new(ring));
    for name in RESEAL_NOW {
        match reseal_one(ctx, &store, name).await {
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
