//! Phase 14 (14-03): admin bootstrap, online and offline (D-P14-5).
//!
//! **Online** ([`ensure_admin`], from `p14::start` on every node, `[auth] mode = "token"`
//! only): when no unexpired system-admin token (`tenant = None`, scopes ⊇ `admin`, an `otk_`
//! id) exists, the **meta leader** creates exactly one:
//!
//! - from `OSTRO_BOOTSTRAP_TOKEN` or `[auth] bootstrap_token_file` (an operator-supplied
//!   bearer, Helm-friendly; it must parse; only its id and hash are stored), else
//! - a generated bearer written to `<data>/admin.token` (mode 0600, tmp + fsync + rename)
//!   **before** the meta write; the log names the path, never the secret.
//!
//! The record carries ext 112 `ApiTokenMeta{name: "bootstrap-admin", created_by: "system"}`
//! when `compat::require(ApiTokenMetaExt)` allows it, no expiry, and is written with a
//! create-only `Put` (`expected_version: None`): a `Conflict` (the same supplied id created
//! by a concurrent leader) counts as success. A process-wide lock plus a re-check under it
//! make concurrent calls on one node create one token; leadership makes nodes exclusive. A
//! node that is not the meta leader while no admin exists keeps a small watcher task (1 s
//! period, stops with `P14Ctx::shutdown`) that bootstraps if it becomes leader, so a leader
//! change before the bootstrap is covered. Restarts find the token and do nothing.
//!
//! **Offline** ([`offline`]: `ostro auth bootstrap --data-dir <d> [--expires <dur>]`): on a
//! stopped single node. Takes the data dir `LOCK` (refuses while a server holds it), probes
//! the persisted meta configuration (refuses unless exactly one voter and no learners: "use
//! `ostro auth token create` against the running cluster"), opens meta through the
//! single-member path (`MetaHandle::start(<data>/meta, <the voter>)`, the same path the
//! offline backup uses, here on the real directory because it writes), proposes a new
//! system-admin token through the local one-voter raft and prints the bearer once on
//! stdout.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};
use std::time::Duration;

use ostro_meta::command::{MetaCommand, MetaKey, MetaRecord, MetaResult};
use ostro_meta::ext::{ApiTokenMeta, put_api_token_meta};
use ostro_meta::sm::MetaView;
use ostro_meta::{ApiToken, MetaHandle};
use ostro_partition::Clock;
use ostro_types::compat::Feature;
use tracing::{info, warn};

use super::tokens::{self, Bearer, Secret, TokenId};
use super::{P14Ctx, P14Error};
use crate::config::{AuthMode, FriendlyDuration};

/// `<data>/admin.token`: the generated bootstrap bearer (0600).
pub const ADMIN_TOKEN_FILE: &str = "admin.token";
/// Environment variable holding an operator-supplied bootstrap bearer.
pub const BOOTSTRAP_TOKEN_ENV: &str = "OSTRO_BOOTSTRAP_TOKEN";
/// ext 112 `name` of the online bootstrap token.
pub const BOOTSTRAP_NAME: &str = "bootstrap-admin";
/// ext 112 `name` of a token created by `ostro auth bootstrap`.
pub const OFFLINE_BOOTSTRAP_NAME: &str = "offline-bootstrap-admin";
/// ext 112 `created_by` of bootstrap tokens.
pub const CREATED_BY_SYSTEM: &str = "system";
/// How often a non-leader re-checks while no system admin exists.
const WATCH_PERIOD: Duration = Duration::from_secs(1);

/// `ostro auth bootstrap` arguments (the `ostro` binary intercepts the CLI variant).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BootstrapArgs {
    /// The stopped node's data directory (`--data-dir`).
    pub data_dir: PathBuf,
    /// The server configuration file (`--config`). Must exist when given; nothing in it
    /// changes the offline bootstrap (the data dir is explicit).
    pub config: Option<PathBuf>,
    /// Token lifetime (`--expires`, a friendly duration); `None` = no expiry.
    pub expires: Option<String>,
}

/// Where the bootstrap bearer comes from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BootstrapSource {
    /// An operator-supplied bearer (`OSTRO_BOOTSTRAP_TOKEN` or `bootstrap_token_file`).
    Supplied {
        /// `env` or the file path (logs).
        origin: String,
        /// The parsed id.
        id: TokenId,
        /// The parsed secret (only its hash is stored).
        secret: Secret,
    },
    /// Generate one and write it to this file.
    Generate(PathBuf),
}

/// Whether `t` is an unexpired system-admin API token at `now_ms`.
pub fn is_system_admin(t: &ApiToken, now_ms: u64) -> bool {
    t.tenant.is_none()
        && tokens::is_api_token_id(&t.id)
        && t.scopes.iter().any(|s| s == "admin")
        && ostro_api::auth::ScopeSet::parse(&t.scopes).is_ok()
        && t.expires_at_ms.is_none_or(|e| e > now_ms)
}

/// Whether `view` holds an unexpired system-admin token at `now_ms`.
pub fn has_system_admin(view: &MetaView, now_ms: u64) -> bool {
    view.read(|s| s.tokens().any(|t| is_system_admin(&t.value, now_ms)))
}

/// The bootstrap source for `env` (the `OSTRO_BOOTSTRAP_TOKEN` value), the configured
/// `bootstrap_token_file` and the data dir. A supplied bearer that does not parse fails
/// startup (the secret is never printed).
pub fn resolve_source(
    env: Option<&str>,
    file: Option<&Path>,
    data_dir: &Path,
) -> Result<BootstrapSource, P14Error> {
    let (origin, raw) = if let Some(v) = env.filter(|v| !v.trim().is_empty()) {
        (BOOTSTRAP_TOKEN_ENV.to_owned(), v.trim().to_owned())
    } else if let Some(path) = file {
        let raw = std::fs::read_to_string(path).map_err(|e| {
            P14Error::new(format!(
                "[auth] bootstrap_token_file {}: {e}",
                path.display()
            ))
        })?;
        (path.display().to_string(), raw.trim().to_owned())
    } else {
        return Ok(BootstrapSource::Generate(data_dir.join(ADMIN_TOKEN_FILE)));
    };
    let (id, secret) = tokens::parse(&raw).map_err(|e| {
        P14Error::new(format!(
            "the bootstrap token from {origin} is not a valid Ostro API bearer ({e}); expected ostro_otk_<16 base32>_<52 base32>"
        ))
    })?;
    Ok(BootstrapSource::Supplied { origin, id, secret })
}

/// Online bootstrap (see the module docs). Token mode only; a no-op when a system admin
/// exists.
pub async fn ensure_admin(ctx: &Arc<P14Ctx>) -> Result<(), P14Error> {
    if ctx.cfg.auth.mode != AuthMode::Token {
        return Ok(());
    }
    let env = std::env::var(BOOTSTRAP_TOKEN_ENV).ok();
    let source = resolve_source(
        env.as_deref(),
        ctx.cfg.auth.bootstrap_token_file.as_deref(),
        &ctx.data_dir,
    )?;
    match try_bootstrap(ctx, &source).await? {
        Attempt::Done => Ok(()),
        Attempt::NotLeader => {
            let weak = Arc::downgrade(ctx);
            let stop = ctx.shutdown.clone();
            tokio::spawn(watch(weak, stop, source));
            Ok(())
        }
    }
}

/// Run [`ensure_admin`]'s single attempt for `source` and report whether a system admin
/// exists afterwards (tests; the watcher uses the same path).
pub async fn bootstrap_once(ctx: &Arc<P14Ctx>, source: &BootstrapSource) -> Result<bool, P14Error> {
    Ok(matches!(try_bootstrap(ctx, source).await?, Attempt::Done))
}

enum Attempt {
    /// A system admin exists (found or created).
    Done,
    /// None exists and this node is not the meta leader.
    NotLeader,
}

/// One process-wide bootstrap at a time (concurrent calls → one token).
static BOOTSTRAP_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn try_bootstrap(ctx: &P14Ctx, source: &BootstrapSource) -> Result<Attempt, P14Error> {
    let _guard = BOOTSTRAP_LOCK.lock().await;
    if has_system_admin(ctx.meta.view(), ctx.clock.now_ms()) {
        return Ok(Attempt::Done);
    }
    if !ctx.meta.is_leader() {
        return Ok(Attempt::NotLeader);
    }
    for _ in 0..4 {
        let (id, secret, file) = match source {
            BootstrapSource::Supplied { id, secret, .. } => (id.clone(), secret.clone(), None),
            BootstrapSource::Generate(path) => {
                let (id, secret, bearer) = tokens::generate_os()
                    .map_err(|e| P14Error::new(format!("bootstrap token: {e}")))?;
                write_bearer(path, &bearer)?;
                (id, secret, Some(path.clone()))
            }
        };
        let record = admin_record(
            &id,
            &secret,
            BOOTSTRAP_NAME,
            ctx.meta.view(),
            ctx.level_max,
            ctx.clock.now_ms(),
            None,
        )?;
        let res = ctx
            .meta
            .propose(MetaCommand::Put {
                key: MetaKey::Token(id.as_str().to_owned()),
                expected_version: None,
                record: MetaRecord::Token(record),
            })
            .await;
        match res {
            Ok(MetaResult::Ok { .. }) => {
                match (&file, source) {
                    (Some(path), _) => info!(
                        token_id = id.as_str(),
                        "bootstrap admin token written to {}",
                        path.display()
                    ),
                    (None, BootstrapSource::Supplied { origin, .. }) => info!(
                        token_id = id.as_str(),
                        "bootstrap admin token created from {origin}"
                    ),
                    (None, BootstrapSource::Generate(_)) => {}
                }
                return Ok(Attempt::Done);
            }
            Ok(MetaResult::Conflict { .. }) => {
                if matches!(source, BootstrapSource::Supplied { .. }) {
                    // The same supplied id, created by a concurrent leader.
                    return Ok(Attempt::Done);
                }
                // A generated id collided (~2^-80): try another one.
                continue;
            }
            Ok(other) => {
                return Err(P14Error::new(format!(
                    "bootstrap admin token: meta refused the record: {other:?}"
                )));
            }
            Err(e) if e.is_leader_unavailable() => {
                if let Some(path) = &file {
                    let _ = std::fs::remove_file(path);
                }
                return Ok(Attempt::NotLeader);
            }
            Err(e) => {
                return Err(P14Error::new(format!("bootstrap admin token: {e}")));
            }
        }
    }
    Err(P14Error::new(
        "bootstrap admin token: could not allocate a unique token id",
    ))
}

async fn watch(
    ctx: Weak<P14Ctx>,
    stop: tokio_util::sync::CancellationToken,
    source: BootstrapSource,
) {
    loop {
        tokio::select! {
            () = stop.cancelled() => return,
            () = tokio::time::sleep(WATCH_PERIOD) => {}
        }
        let Some(ctx) = ctx.upgrade() else { return };
        match try_bootstrap(&ctx, &source).await {
            Ok(Attempt::Done) => return,
            Ok(Attempt::NotLeader) => {}
            Err(e) => warn!(error = %e, "bootstrap admin token: retrying"),
        }
    }
}

/// The system-admin `ApiToken` for `id`/`secret` (no tenant, scopes `[admin]`); ext 112
/// only when the cluster's finalized level allows it.
fn admin_record(
    id: &TokenId,
    secret: &Secret,
    name: &str,
    view: &MetaView,
    level_max: u32,
    now_ms: u64,
    expires_at_ms: Option<u64>,
) -> Result<ApiToken, P14Error> {
    let ext = if ostro_cluster::compat::require(view, Feature::ApiTokenMetaExt, level_max).is_ok() {
        put_api_token_meta(
            &[],
            &ApiTokenMeta {
                name: name.to_owned(),
                description: String::new(),
                created_by: CREATED_BY_SYSTEM.to_owned(),
                rotated_from: None,
            },
        )
        .map_err(|e| P14Error::new(format!("encoding ApiTokenMeta: {e}")))?
    } else {
        Vec::new()
    };
    Ok(ApiToken {
        id: id.as_str().to_owned(),
        hash: tokens::hash(secret),
        scopes: vec!["admin".to_owned()],
        tenant: None,
        created_at_ms: now_ms,
        expires_at_ms,
        ext,
    })
}

/// Write `bearer` (plus a newline) to `path` with mode 0600: tmp + fsync + rename + dir fsync.
fn write_bearer(path: &Path, bearer: &Bearer) -> Result<(), P14Error> {
    let mut text = bearer.expose().to_owned();
    text.push('\n');
    crate::tls::ca::write_atomic(path, text.as_bytes(), 0o600)
        .map_err(|e| P14Error::new(format!("writing the bootstrap admin token: {e}")))
}

// ---------------------------------------------------------------------------------
// Offline
// ---------------------------------------------------------------------------------

/// Offline `ostro auth bootstrap --data-dir` on a stopped single node: creates a system-admin
/// token and prints the bearer once on stdout.
pub fn offline(args: BootstrapArgs) -> Result<(), P14Error> {
    // The offline CLI has no server clock: created_at / expiry use wall time.
    let clock: Arc<dyn Clock> = Arc::new(ostro_partition::SystemClock); // clock-exempt: offline CLI
    let bearer = offline_with_clock(&args, clock)?;
    let mut out = std::io::stdout().lock();
    writeln!(out, "{}", bearer.expose())
        .and_then(|()| out.flush())
        .map_err(|e| P14Error::new(format!("writing the bearer to stdout: {e}")))
}

/// [`offline`] with an explicit clock; returns the bearer instead of printing it.
pub fn offline_with_clock(args: &BootstrapArgs, clock: Arc<dyn Clock>) -> Result<Bearer, P14Error> {
    if let Some(cfg) = &args.config
        && !cfg.is_file()
    {
        return Err(P14Error::new(format!(
            "configuration file {} not found",
            cfg.display()
        )));
    }
    let ttl = match &args.expires {
        None => None,
        Some(s) => {
            let d: FriendlyDuration = s
                .parse()
                .map_err(|e| P14Error::new(format!("--expires {s:?}: {e}")))?;
            let ms = u64::try_from(d.get().as_millis()).unwrap_or(u64::MAX);
            if ms == 0 {
                return Err(P14Error::new("--expires must be > 0"));
            }
            Some(ms)
        }
    };
    let data_dir = args.data_dir.as_path();
    let meta_dir = data_dir.join("meta");
    if !meta_dir.is_dir() {
        return Err(P14Error::new(format!(
            "{} has no metadata (meta/): start the server once first",
            data_dir.display()
        )));
    }
    let _lock = lock(data_dir)?;
    let probe = ostro_cluster::identity::probe_meta(&meta_dir)
        .map_err(|e| P14Error::new(format!("reading {}: {e}", meta_dir.display())))?;
    let conf = probe.conf.unwrap_or_default();
    let node_id = match (conf.voters.as_slice(), conf.learners.is_empty()) {
        ([only], true) => *only,
        ([], _) => {
            return Err(P14Error::new(format!(
                "{} has no metadata configuration: start the server once first",
                data_dir.display()
            )));
        }
        _ => {
            return Err(P14Error::new(format!(
                "{} belongs to a multi-node cluster (voters {:?}, learners {:?}): use `ostro auth token create` against the running cluster",
                data_dir.display(),
                conf.voters,
                conf.learners
            )));
        }
    };
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| P14Error::new(format!("starting a runtime: {e}")))?;
    rt.block_on(async move {
        let meta = MetaHandle::start(&meta_dir, node_id)
            .await
            .map_err(|e| P14Error::new(format!("opening metadata: {e}")))?;
        let res = propose_offline(&meta, clock.as_ref(), ttl).await;
        meta.stop().await;
        res
    })
}

async fn propose_offline(
    meta: &MetaHandle,
    clock: &dyn Clock,
    ttl_ms: Option<u64>,
) -> Result<Bearer, P14Error> {
    for _ in 0..4 {
        let (id, secret, bearer) =
            tokens::generate_os().map_err(|e| P14Error::new(format!("bootstrap token: {e}")))?;
        let now = clock.now_ms();
        let record = admin_record(
            &id,
            &secret,
            OFFLINE_BOOTSTRAP_NAME,
            meta.view(),
            ostro_types::compat::level_max(),
            now,
            ttl_ms.map(|t| now.saturating_add(t)),
        )?;
        match meta
            .propose(MetaCommand::Put {
                key: MetaKey::Token(id.as_str().to_owned()),
                expected_version: None,
                record: MetaRecord::Token(record),
            })
            .await
            .map_err(|e| P14Error::new(format!("storing the token: {e}")))?
        {
            MetaResult::Ok { .. } => return Ok(bearer),
            MetaResult::Conflict { .. } => continue,
            other => {
                return Err(P14Error::new(format!(
                    "storing the token: meta refused the record: {other:?}"
                )));
            }
        }
    }
    Err(P14Error::new("could not allocate a unique token id"))
}

/// The data dir `LOCK` (refuses while a running server holds it).
fn lock(data_dir: &Path) -> Result<std::fs::File, P14Error> {
    let path = data_dir.join(crate::server::LOCK_FILE);
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .map_err(|e| P14Error::new(format!("{}: {e}", path.display())))?;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(std::fs::TryLockError::WouldBlock) => Err(P14Error::new(format!(
            "{} is in use by a running server: stop it, or use `ostro auth token create` against the running cluster",
            data_dir.display()
        ))),
        Err(std::fs::TryLockError::Error(e)) => {
            Err(P14Error::new(format!("{}: {e}", path.display())))
        }
    }
}
