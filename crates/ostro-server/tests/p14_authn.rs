//! 14-03 Task 2: the meta-backed authenticator and the admin bootstrap (online + offline).

use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use ostro_api::auth::{AuthError, Authenticator, PrincipalKind, Scope, ScopeSet};
use ostro_meta::ext::{ClusterVersion, get_api_token_meta, put_cluster_version};
use ostro_meta::{ApiToken, ClusterInfo, MetaCommand, MetaHandle, MetaKey, MetaRecord, MetaResult};
use ostro_partition::Clock;
use ostro_partition::clock::ManualClock;
use ostro_server::config::{AuthMode, ServerConfig};
use ostro_server::p14::authn::{self, MetaAuthenticator};
use ostro_server::p14::bootstrap::{
    self, ADMIN_TOKEN_FILE, BOOTSTRAP_NAME, BootstrapArgs, BootstrapSource, OFFLINE_BOOTSTRAP_NAME,
};
use ostro_server::p14::{self, P14Init, tokens};
use tokio::time::timeout;

const T: Duration = Duration::from_secs(60);

/// A fresh token stored in `meta`; returns its bearer.
async fn put_token(
    meta: &MetaHandle,
    scopes: &[&str],
    tenant: Option<&str>,
    expires_at_ms: Option<u64>,
) -> String {
    let (id, secret, bearer) = tokens::generate_os().expect("generate");
    let rec = ApiToken {
        id: id.as_str().to_owned(),
        hash: tokens::hash(&secret),
        scopes: scopes.iter().map(|s| (*s).to_owned()).collect(),
        tenant: tenant.map(str::to_owned),
        created_at_ms: 1,
        expires_at_ms,
        ext: Vec::new(),
    };
    put_raw(meta, rec).await;
    bearer.expose().to_owned()
}

async fn put_raw(meta: &MetaHandle, rec: ApiToken) {
    let res = meta
        .propose(MetaCommand::Put {
            key: MetaKey::Token(rec.id.clone()),
            expected_version: None,
            record: MetaRecord::Token(rec),
        })
        .await
        .expect("propose");
    assert!(matches!(res, MetaResult::Ok { .. }), "{res:?}");
}

fn system_admins(meta: &MetaHandle) -> Vec<ApiToken> {
    meta.view().read(|s| {
        s.tokens()
            .filter(|t| bootstrap::is_system_admin(&t.value, 0))
            .map(|t| t.value.clone())
            .collect()
    })
}

fn token_count(meta: &MetaHandle) -> usize {
    meta.view().read(|s| s.tokens().count())
}

fn bootstrap_name(t: &ApiToken) -> Option<String> {
    get_api_token_meta(&t.ext).expect("ext").map(|m| m.name)
}

async fn set_finalized(meta: &MetaHandle, finalized: u32) {
    let cur = meta
        .view()
        .read(|s| s.cluster().cloned())
        .expect("cluster info");
    let mut info: ClusterInfo = cur.value;
    info.ext = put_cluster_version(
        &info.ext,
        &ClusterVersion {
            finalized,
            hold: None,
            finalized_at_ms: 1,
        },
    )
    .expect("ext 98");
    let r = meta
        .propose(MetaCommand::Put {
            key: MetaKey::Cluster,
            expected_version: Some(cur.version),
            record: MetaRecord::Cluster(info),
        })
        .await
        .expect("propose");
    assert!(matches!(r, MetaResult::Ok { .. }), "{r:?}");
}

// ---------------------------------------------------------------------------------
// MetaAuthenticator over a local meta view
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn authenticator_accepts_valid_and_rejects_everything_else() {
    let dir = tempfile::tempdir().expect("tempdir");
    let meta = MetaHandle::start(dir.path(), 1).await.expect("meta");
    let clock = ManualClock::new(10_000);
    let a = MetaAuthenticator::new(
        meta.view().clone(),
        Arc::new(clock.clone()),
        AuthMode::Token,
    );

    let read_acme = put_token(&meta, &["read", "trigger"], Some("acme"), None).await;
    let sys_admin = put_token(&meta, &["admin"], None, None).await;
    let default_tenant = put_token(&meta, &["deploy"], Some("_"), None).await;
    let expiring = put_token(&meta, &["read"], None, Some(20_000)).await;
    let join = put_token(&meta, &["worker:join"], None, None).await;
    let mixed = put_token(&meta, &["read", "node:join"], None, None).await;
    let typo = put_token(&meta, &["reed"], None, None).await;
    let applied = meta.view().applied();

    // Valid tokens → their tenant and scopes.
    let p = a.authenticate(Some(&read_acme)).await.expect("valid");
    assert_eq!(p.kind, PrincipalKind::Token);
    assert_eq!(p.tenant.as_deref(), Some("acme"));
    assert_eq!(p.scopes, ScopeSet::of(&[Scope::Read, Scope::Trigger]));
    let (id, _) = tokens::parse(&read_acme).unwrap();
    assert_eq!(p.token_id, id.as_str());
    assert_eq!(a.last_seen(id.as_str()), Some(10_000));
    let p = a.authenticate(Some(&sys_admin)).await.expect("admin");
    assert!(p.tenant.is_none() && p.scopes == ScopeSet::admin());
    let p = a
        .authenticate(Some(&default_tenant))
        .await
        .expect("default");
    assert_eq!(p.tenant.as_deref(), Some("_"));

    // Wrong secret (same id), unknown id.
    let (id, secret) = tokens::parse(&read_acme).unwrap();
    let mut wrong = secret.expose().as_bytes().to_vec();
    wrong[0] = if wrong[0] == b'a' { b'b' } else { b'a' };
    let wrong = format!(
        "ostro_{}_{}",
        id.as_str(),
        String::from_utf8(wrong).unwrap()
    );
    assert_eq!(a.authenticate(Some(&wrong)).await, Err(AuthError::Invalid));
    let (_, _, unknown) = tokens::generate_os().unwrap();
    assert_eq!(
        a.authenticate(Some(unknown.expose())).await,
        Err(AuthError::Invalid)
    );

    // Expiry through the injected clock: valid before, expired at and after.
    assert!(a.authenticate(Some(&expiring)).await.is_ok());
    clock.set(19_999);
    assert!(a.authenticate(Some(&expiring)).await.is_ok());
    clock.set(20_000);
    assert_eq!(
        a.authenticate(Some(&expiring)).await,
        Err(AuthError::Expired)
    );
    // An expired token with a wrong secret is just invalid (no expiry leak).
    let (eid, esecret) = tokens::parse(&expiring).unwrap();
    let mut w = esecret.expose().as_bytes().to_vec();
    w[3] = if w[3] == b'a' { b'b' } else { b'a' };
    let w = format!("ostro_{}_{}", eid.as_str(), String::from_utf8(w).unwrap());
    assert_eq!(a.authenticate(Some(&w)).await, Err(AuthError::Invalid));

    // Non-API scopes and unknown scope strings are never API principals.
    for b in [&join, &mixed, &typo] {
        assert_eq!(a.authenticate(Some(b)).await, Err(AuthError::NotApiToken));
    }
    // Non-otk ids and malformed bearers fail before the lookup.
    let (_, s, _) = tokens::generate_os().unwrap();
    for prefix in ["njt_", "wkr_"] {
        let b = format!("ostro_{prefix}aaaaaaaaaaaaaaaa_{}", s.expose());
        assert_eq!(a.authenticate(Some(&b)).await, Err(AuthError::NotApiToken));
    }
    for b in ["", "Bearer x", "ostro_otk_short_secret", "wkr_abc.def"] {
        assert_eq!(a.authenticate(Some(b)).await, Err(AuthError::Malformed));
    }
    assert_eq!(a.authenticate(None).await, Err(AuthError::Missing));

    // Error reasons leak nothing beyond invalid / expired.
    assert_eq!(authn::reason(&AuthError::Expired), "expired_token");
    assert_eq!(authn::reason(&AuthError::NotApiToken), "invalid_token");
    assert_eq!(authn::reason(&AuthError::Malformed), "invalid_token");
    assert_eq!(authn::reason(&AuthError::Invalid), "invalid_token");

    // Never a meta write.
    assert_eq!(
        meta.view().applied(),
        applied,
        "authentication wrote to meta"
    );
    meta.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mode_none_is_the_dev_principal() {
    let dir = tempfile::tempdir().expect("tempdir");
    let meta = MetaHandle::start(dir.path(), 1).await.expect("meta");
    let a = MetaAuthenticator::new(
        meta.view().clone(),
        Arc::new(ManualClock::new(1)),
        AuthMode::None,
    );
    for b in [None, Some("garbage")] {
        let p = a.authenticate(b).await.expect("dev");
        assert_eq!(p.kind, PrincipalKind::Dev);
    }
    // The factory: none → AllowDev, token → MetaAuthenticator (missing bearer refused).
    let mut cfg = ServerConfig::server_defaults();
    cfg.auth.mode = AuthMode::None;
    let c = ctx_on(cfg, Arc::new(meta), dir.path());
    let p = p14::authenticator(&c)
        .authenticate(None)
        .await
        .expect("dev");
    assert_eq!(p.kind, PrincipalKind::Dev);
    let dir2 = tempfile::tempdir().expect("tempdir");
    let c2 = ctx(ServerConfig::server_defaults(), &dir2).await;
    assert_eq!(
        p14::authenticator(&c2).authenticate(None).await,
        Err(AuthError::Missing)
    );
}

#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lookup_failpoint_is_unavailable() {
    let dir = tempfile::tempdir().expect("tempdir");
    let meta = MetaHandle::start(dir.path(), 1).await.expect("meta");
    let a = MetaAuthenticator::new(
        meta.view().clone(),
        Arc::new(ManualClock::new(1)),
        AuthMode::Token,
    );
    let b = put_token(&meta, &["read"], None, None).await;
    let scenario = fail::FailScenario::setup();
    fail::cfg("auth::lookup", "return(store down)").expect("cfg");
    match a.authenticate(Some(&b)).await {
        Err(AuthError::Unavailable(m)) => assert_eq!(m, "store down"),
        other => panic!("expected Unavailable, got {other:?}"),
    }
    // Malformed bearers still fail before the lookup site.
    assert_eq!(a.authenticate(Some("x")).await, Err(AuthError::Malformed));
    fail::remove("auth::lookup");
    scenario.teardown();
    assert!(a.authenticate(Some(&b)).await.is_ok());
    meta.stop().await;
}

// ---------------------------------------------------------------------------------
// Online bootstrap on a bare P14Ctx
// ---------------------------------------------------------------------------------

fn ctx_on(cfg: ServerConfig, meta: Arc<MetaHandle>, data: &Path) -> Arc<p14::P14Ctx> {
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(5_000));
    p14::build_ctx(P14Init {
        cfg: Arc::new(cfg),
        meta,
        clock,
        node_id: 1,
        data_dir: data.to_path_buf(),
        level_max: ostro_types::compat::level_max(),
    })
    .expect("build_ctx")
}

async fn ctx(cfg: ServerConfig, dir: &tempfile::TempDir) -> Arc<p14::P14Ctx> {
    let meta_dir = dir.path().join("meta");
    let meta = Arc::new(MetaHandle::start(&meta_dir, 1).await.expect("meta"));
    ctx_on(cfg, meta, dir.path())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_ensure_admin_creates_one_token_in_a_0600_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let c = ctx(ServerConfig::server_defaults(), &dir).await;
    let (a, b, d) = tokio::join!(
        bootstrap::ensure_admin(&c),
        bootstrap::ensure_admin(&c),
        bootstrap::ensure_admin(&c)
    );
    a.and(b).and(d).expect("ensure_admin");
    let admins = system_admins(&c.meta);
    assert_eq!(admins.len(), 1, "{admins:?}");
    // A bare meta group never ran InitCluster: no finalized level, so the ext 112 emitter is
    // gated off (compat::require) and the record carries no ext. The server test below sees
    // the ext.
    assert_eq!(bootstrap_name(&admins[0]), None);
    assert_eq!(admins[0].expires_at_ms, None);
    assert_eq!(admins[0].created_at_ms, 5_000, "injected clock");
    let path = dir.path().join(ADMIN_TOKEN_FILE);
    let mode = std::fs::metadata(&path)
        .expect("admin.token")
        .permissions()
        .mode();
    assert_eq!(mode & 0o777, 0o600, "{mode:o}");
    let bearer = std::fs::read_to_string(&path).expect("read");
    let a = MetaAuthenticator::new(c.meta.view().clone(), Arc::clone(&c.clock), AuthMode::Token);
    let p = a
        .authenticate(Some(bearer.trim()))
        .await
        .expect("bootstrap bearer");
    assert!(p.tenant.is_none() && p.scopes == ScopeSet::admin());
    assert_eq!(p.token_id, admins[0].id);

    // Again: nothing new, the file is untouched.
    bootstrap::ensure_admin(&c).await.expect("again");
    assert_eq!(system_admins(&c.meta).len(), 1);
    assert_eq!(std::fs::read_to_string(&path).unwrap(), bearer);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bootstrap_record_carries_ext_112_once_finalized_at_level_2() {
    let dir = tempfile::tempdir().expect("tempdir");
    let c = ctx(ServerConfig::server_defaults(), &dir).await;
    let r = c
        .meta
        .propose(MetaCommand::InitCluster {
            cluster_id: "c-test".into(),
            node_id: 1,
            partitions: 1,
            created_at_ms: 1,
            token_key: [3; 32],
        })
        .await
        .expect("init");
    assert!(matches!(r, MetaResult::Ok { .. }), "{r:?}");
    set_finalized(&c.meta, 2).await;
    bootstrap::ensure_admin(&c).await.expect("bootstrap");
    let admins = system_admins(&c.meta);
    assert_eq!(admins.len(), 1);
    let m = get_api_token_meta(&admins[0].ext)
        .unwrap()
        .expect("ext 112");
    assert_eq!(m.name, BOOTSTRAP_NAME);
    assert_eq!(m.created_by, "system");
    assert_eq!(m.rotated_from, None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ensure_admin_is_a_no_op_in_mode_none_and_with_an_existing_admin() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut cfg = ServerConfig::server_defaults();
    cfg.auth.mode = AuthMode::None;
    let c = ctx(cfg, &dir).await;
    bootstrap::ensure_admin(&c).await.expect("none");
    assert_eq!(token_count(&c.meta), 0);
    assert!(!dir.path().join(ADMIN_TOKEN_FILE).exists());

    let dir2 = tempfile::tempdir().expect("tempdir");
    let c2 = ctx(ServerConfig::server_defaults(), &dir2).await;
    put_token(&c2.meta, &["admin", "read"], None, None).await;
    bootstrap::ensure_admin(&c2).await.expect("existing");
    assert_eq!(token_count(&c2.meta), 1);
    assert!(!dir2.path().join(ADMIN_TOKEN_FILE).exists());

    // A tenant admin or an expired system admin does not count.
    let dir3 = tempfile::tempdir().expect("tempdir");
    let c3 = ctx(ServerConfig::server_defaults(), &dir3).await;
    put_token(&c3.meta, &["admin"], Some("acme"), None).await;
    put_token(&c3.meta, &["admin"], None, Some(4_000)).await;
    bootstrap::ensure_admin(&c3).await.expect("bootstrap");
    assert_eq!(
        system_admins(&c3.meta).len(),
        2,
        "expired one + bootstrap (now=0 filter)"
    );
    assert!(dir3.path().join(ADMIN_TOKEN_FILE).exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn supplied_bearer_sources() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (_, _, env_bearer) = tokens::generate_os().unwrap();
    let (_, _, file_bearer) = tokens::generate_os().unwrap();
    let file = dir.path().join("bootstrap.txt");
    std::fs::write(&file, format!("{}\n", file_bearer.expose())).unwrap();

    // The env wins over the file; the file over generation.
    match bootstrap::resolve_source(Some(env_bearer.expose()), Some(&file), dir.path()).unwrap() {
        BootstrapSource::Supplied { origin, id, .. } => {
            assert_eq!(origin, bootstrap::BOOTSTRAP_TOKEN_ENV);
            assert_eq!(id, tokens::parse(env_bearer.expose()).unwrap().0);
        }
        other => panic!("{other:?}"),
    }
    match bootstrap::resolve_source(None, Some(&file), dir.path()).unwrap() {
        BootstrapSource::Supplied { id, .. } => {
            assert_eq!(id, tokens::parse(file_bearer.expose()).unwrap().0);
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(
        bootstrap::resolve_source(Some("  "), None, dir.path()).unwrap(),
        BootstrapSource::Generate(dir.path().join(ADMIN_TOKEN_FILE))
    );
    // An invalid supplied bearer fails without echoing it.
    let bad = "ostro_otk_notavalidtokenatall_secretsecret";
    let err = bootstrap::resolve_source(Some(bad), None, dir.path()).unwrap_err();
    assert!(!err.to_string().contains(bad), "{err}");
    assert!(err.to_string().contains("OSTRO_BOOTSTRAP_TOKEN"), "{err}");
    assert!(
        bootstrap::resolve_source(None, Some(&dir.path().join("missing")), dir.path()).is_err()
    );

    // The env-supplied bearer authenticates after a bootstrap with it; a second leader with
    // the same supplied id (Conflict) is success and creates nothing.
    let c = ctx(ServerConfig::server_defaults(), &dir).await;
    let src = bootstrap::resolve_source(Some(env_bearer.expose()), None, dir.path()).unwrap();
    assert!(bootstrap::bootstrap_once(&c, &src).await.unwrap());
    let a = MetaAuthenticator::new(c.meta.view().clone(), Arc::clone(&c.clock), AuthMode::Token);
    let p = a
        .authenticate(Some(env_bearer.expose()))
        .await
        .expect("env bearer");
    assert!(p.tenant.is_none() && p.scopes == ScopeSet::admin());
    assert!(!dir.path().join(ADMIN_TOKEN_FILE).exists());
    let stored = system_admins(&c.meta);
    assert_eq!(stored.len(), 1);
    // Only the hash is stored.
    let (_, secret) = tokens::parse(env_bearer.expose()).unwrap();
    assert_eq!(stored[0].hash, tokens::hash(&secret));
}

// ---------------------------------------------------------------------------------
// In-process server: startup bootstrap, restart, file source, offline bootstrap
// ---------------------------------------------------------------------------------

fn server_cfg(dir: &Path) -> ServerConfig {
    let mut cfg = ServerConfig::server_defaults();
    cfg.api.listen = "127.0.0.1:0".parse().unwrap();
    cfg.cluster.listen = "127.0.0.1:0".parse().unwrap();
    cfg.node.data_dir = dir.to_path_buf();
    cfg.node.partitions = 2;
    cfg
}

async fn boot(cfg: ServerConfig) -> ostro_server::ServerHandle {
    timeout(T, ostro_server::server::start(cfg))
        .await
        .expect("server start timed out")
        .expect("server start")
}

fn auth_of(h: &ostro_server::ServerHandle) -> MetaAuthenticator {
    MetaAuthenticator::new(
        h.meta().view().clone(),
        Arc::clone(h.clock()),
        AuthMode::Token,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_bootstraps_once_across_restarts() {
    let dir = tempfile::tempdir().expect("tempdir");
    let h = boot(server_cfg(dir.path())).await;
    let path = dir.path().join(ADMIN_TOKEN_FILE);
    let bearer = std::fs::read_to_string(&path).expect("admin.token");
    let mode = std::fs::metadata(&path).unwrap().permissions().mode();
    assert_eq!(mode & 0o777, 0o600);
    tokens::parse(bearer.trim()).expect("admin.token parses");
    let p = auth_of(&h)
        .authenticate(Some(bearer.trim()))
        .await
        .expect("auth");
    assert!(p.tenant.is_none() && p.scopes == ScopeSet::admin());
    let admins = system_admins(h.meta());
    assert_eq!(admins.len(), 1);
    // A fresh cluster is finalized at level 1: ext 112 is gated off (D-P14-28).
    assert_eq!(bootstrap_name(&admins[0]), None);
    h.shutdown().await.expect("shutdown");

    let h = boot(server_cfg(dir.path())).await;
    let admins = system_admins(h.meta());
    assert_eq!(admins.len(), 1, "restart created another token: {admins:?}");
    assert_eq!(std::fs::read_to_string(&path).unwrap(), bearer);
    assert!(auth_of(&h).authenticate(Some(bearer.trim())).await.is_ok());

    // Offline bootstrap refuses while the server holds the data dir.
    let args = BootstrapArgs {
        data_dir: dir.path().to_path_buf(),
        config: None,
        expires: None,
    };
    let err =
        bootstrap::offline_with_clock(&args, Arc::new(ManualClock::new(1))).expect_err("locked");
    assert!(
        err.to_string().contains("in use by a running server"),
        "{err}"
    );
    h.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_uses_the_bootstrap_token_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let secret_dir = tempfile::tempdir().expect("tempdir");
    let (_, _, bearer) = tokens::generate_os().unwrap();
    let file = secret_dir.path().join("admin-bearer");
    std::fs::write(&file, bearer.expose()).unwrap();
    let mut cfg = server_cfg(dir.path());
    cfg.auth.bootstrap_token_file = Some(file);
    let h = boot(cfg).await;
    assert!(!dir.path().join(ADMIN_TOKEN_FILE).exists());
    let p = auth_of(&h)
        .authenticate(Some(bearer.expose()))
        .await
        .expect("file bearer");
    assert!(p.tenant.is_none() && p.scopes == ScopeSet::admin());
    h.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn offline_bootstrap_on_a_stopped_node() {
    let dir = tempfile::tempdir().expect("tempdir");
    let h = boot(server_cfg(dir.path())).await;
    // Finalized at level 2: the offline record carries ext 112.
    set_finalized(h.meta(), 2).await;
    h.shutdown().await.expect("shutdown");

    let args = BootstrapArgs {
        data_dir: dir.path().to_path_buf(),
        config: None,
        expires: Some("1h".into()),
    };
    let now = ostro_partition::SystemClock.now_ms();
    let clock = ManualClock::new(now);
    let bearer =
        tokio::task::spawn_blocking(move || bootstrap::offline_with_clock(&args, Arc::new(clock)))
            .await
            .unwrap()
            .expect("offline bootstrap");

    let h = boot(server_cfg(dir.path())).await;
    let p = auth_of(&h)
        .authenticate(Some(bearer.expose()))
        .await
        .expect("offline bearer");
    assert!(p.tenant.is_none() && p.scopes == ScopeSet::admin());
    let (id, _) = tokens::parse(bearer.expose()).unwrap();
    let rec = h
        .meta()
        .view()
        .read(|s| s.token(id.as_str()).map(|v| v.value.clone()))
        .expect("record");
    assert_eq!(rec.expires_at_ms, Some(now + 3_600_000));
    assert_eq!(
        bootstrap_name(&rec).as_deref(),
        Some(OFFLINE_BOOTSTRAP_NAME)
    );
    // The online bootstrap token is still the only other admin.
    assert_eq!(system_admins(h.meta()).len(), 2);
    h.shutdown().await.expect("shutdown");

    // Bad inputs.
    let empty = tempfile::tempdir().expect("tempdir");
    let err = bootstrap::offline_with_clock(
        &BootstrapArgs {
            data_dir: empty.path().to_path_buf(),
            config: None,
            expires: None,
        },
        Arc::new(ManualClock::new(1)),
    )
    .expect_err("no meta");
    assert!(err.to_string().contains("no metadata"), "{err}");
    let err = bootstrap::offline_with_clock(
        &BootstrapArgs {
            data_dir: dir.path().to_path_buf(),
            config: None,
            expires: Some("soon".into()),
        },
        Arc::new(ManualClock::new(1)),
    )
    .expect_err("bad expiry");
    assert!(err.to_string().contains("--expires"), "{err}");
}
