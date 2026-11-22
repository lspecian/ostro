//! 10-07: the verified toolchain cache against the local fixture server (no real network):
//! sha verification before extraction, safe extraction, atomic install, cache hits, offline
//! and mirror modes, the lock, size cap and timeouts, prune and stale tmp cleanup.

#[path = "common/fixture_server.rs"]
mod fixture_server;

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use fixture_server::{FixtureServer, GOOD_PREFIX, TOOL_SCRIPT, sha256_hex};
use ostro_build::cache::{self, Cache};
use ostro_build::fetch::FetchOptions;
use ostro_build::toolchain::{ArchiveKind, Pin, Tool, ToolchainError, ToolchainSet, ensure};

const TEST_TIMEOUT: Duration = Duration::from_secs(30);

fn opts() -> FetchOptions {
    let connect = Duration::from_secs(5);
    FetchOptions {
        offline: false,
        mirror: None,
        client: reqwest::Client::builder()
            .no_proxy()
            .connect_timeout(connect)
            .build()
            .expect("client"),
        connect_timeout: connect,
        idle_timeout: Duration::from_secs(10),
        total_timeout: Duration::from_secs(20),
    }
}

fn pin(srv: &FixtureServer, name: &str, kind: ArchiveKind, strip: Option<&str>) -> Pin {
    Pin {
        tool: Tool::Uv,
        version: "1.0".into(),
        url: srv.url(&format!("dist/{name}")),
        sha256: srv
            .shas
            .get(name)
            .cloned()
            .unwrap_or_else(|| "0".repeat(64)),
        kind,
        strip: strip.map(str::to_string),
        max_bytes: None,
    }
}

fn good(srv: &FixtureServer) -> Pin {
    pin(srv, "good.tar.gz", ArchiveKind::TarGz, Some(GOOD_PREFIX))
}

fn temp_cache() -> (tempfile::TempDir, Cache) {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = Cache::with_root(dir.path().join("cache"));
    (dir, cache)
}

/// Every name in `tools/` (tmp dirs included).
fn tools_listing(cache: &Cache) -> Vec<String> {
    let mut v: Vec<String> = std::fs::read_dir(cache.tools_dir())
        .map(|rd| {
            rd.flatten()
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect()
        })
        .unwrap_or_default();
    v.sort();
    v
}

fn mode(p: &Path) -> u32 {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::metadata(p).expect("metadata").permissions().mode() & 0o7777
}

async fn within<T>(f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(TEST_TIMEOUT, f)
        .await
        .expect("test timed out")
}

#[tokio::test]
async fn good_targz_installs_then_cache_hit_makes_no_request() {
    within(async {
        let srv = FixtureServer::spawn_default().await;
        let (_d, cache) = temp_cache();
        let p = good(&srv);
        let dir = ensure(&cache, &p, &opts()).await.expect("install");
        assert_eq!(
            dir,
            cache.tools_dir().join(format!("uv-1.0-{}", &p.sha256[..8]))
        );
        let tool = dir.join("bin/tool");
        assert_eq!(std::fs::read(&tool).unwrap(), TOOL_SCRIPT);
        assert_eq!(mode(&tool) & 0o111, 0o111, "executable bit kept");
        assert!(dir.join("README").is_file());
        assert_eq!(
            std::fs::read_to_string(dir.join(".ok")).unwrap().trim(),
            p.sha256
        );
        assert!(cache.is_installed(&p));
        assert_eq!(srv.counts.total(), 1);

        let again = ensure(&cache, &p, &opts()).await.expect("cache hit");
        assert_eq!(again, dir);
        assert_eq!(srv.counts.total(), 1, "cache hit makes no request");
        assert_eq!(tools_listing(&cache), vec![Cache::entry_name(&p)]);
    })
    .await;
}

#[tokio::test]
async fn sha_mismatch_fails_and_leaves_nothing() {
    within(async {
        let srv = FixtureServer::spawn_default().await;
        let (_d, cache) = temp_cache();
        let p = pin(&srv, "badsha.tar.gz", ArchiveKind::TarGz, Some(GOOD_PREFIX));
        let err = ensure(&cache, &p, &opts()).await.unwrap_err();
        match err {
            ToolchainError::ShaMismatch {
                expected, actual, ..
            } => {
                assert_eq!(expected, p.sha256);
                assert_eq!(actual, sha256_hex(&srv.files["badsha.tar.gz"]));
            }
            other => panic!("expected ShaMismatch, got {other:?}"),
        }
        assert!(!cache.entry_dir(&p).exists());
        assert!(
            tools_listing(&cache).is_empty(),
            "{:?}",
            tools_listing(&cache)
        );
    })
    .await;
}

#[tokio::test]
async fn unsafe_archives_are_rejected() {
    within(async {
        let srv = FixtureServer::spawn_default().await;
        for (name, bad_entry, why) in [
            ("traversal.tar.gz", "../evil", "`..`"),
            ("abs.tar.gz", "/tmp/evil", "absolute"),
            ("symlink-escape.tar.gz", "bin/x", "symlink target escapes"),
            ("symlink-chain.tar.gz", "b", "symlink target escapes"),
            (
                "hardlink-escape.tar.gz",
                "bin/passwd",
                "hardlink target escapes",
            ),
        ] {
            let (d, cache) = temp_cache();
            let p = pin(&srv, name, ArchiveKind::TarGz, None);
            let err = ensure(&cache, &p, &opts()).await.unwrap_err();
            match &err {
                ToolchainError::UnsafeArchive { entry, reason } => {
                    assert_eq!(entry, bad_entry, "{name}");
                    assert!(reason.contains(why), "{name}: {reason}");
                }
                other => panic!("{name}: expected UnsafeArchive, got {other:?}"),
            }
            assert!(!cache.entry_dir(&p).exists(), "{name}");
            assert!(
                tools_listing(&cache).is_empty(),
                "{name}: {:?}",
                tools_listing(&cache)
            );
            // nothing was written next to the cache root
            let siblings: Vec<_> = std::fs::read_dir(d.path())
                .unwrap()
                .flatten()
                .map(|e| e.file_name())
                .collect();
            assert_eq!(siblings, vec![std::ffi::OsString::from("cache")], "{name}");
            let top: Vec<_> = std::fs::read_dir(&cache.root)
                .unwrap()
                .flatten()
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .filter(|n| n != ".lock" && n != "tools")
                .collect();
            assert!(top.is_empty(), "{name}: {top:?}");
        }
    })
    .await;
}

#[tokio::test]
async fn internal_symlink_is_allowed() {
    within(async {
        let srv = FixtureServer::spawn_default().await;
        let (_d, cache) = temp_cache();
        let p = pin(&srv, "symlink-ok.tar.gz", ArchiveKind::TarGz, None);
        let dir = ensure(&cache, &p, &opts()).await.expect("install");
        let npm = dir.join("bin/npm");
        assert_eq!(
            std::fs::read_link(&npm).unwrap(),
            PathBuf::from("../lib/npm-cli.js")
        );
        let resolved = std::fs::canonicalize(&npm).unwrap();
        assert!(resolved.starts_with(std::fs::canonicalize(&dir).unwrap()));
        assert_eq!(
            std::fs::read(&npm).unwrap(),
            b"console.log('npm');\n".to_vec()
        );
    })
    .await;
}

#[tokio::test]
async fn tar_xz_binary_and_wheel_install() {
    within(async {
        let srv = FixtureServer::spawn_default().await;
        let (_d, cache) = temp_cache();

        let xz = pin(&srv, "good.tar.xz", ArchiveKind::TarXz, Some(GOOD_PREFIX));
        let dir = ensure(&cache, &xz, &opts()).await.expect("xz");
        assert_eq!(std::fs::read(dir.join("bin/tool")).unwrap(), TOOL_SCRIPT);
        assert_eq!(mode(&dir.join("bin/tool")) & 0o111, 0o111);

        let mut bin = pin(&srv, "tool.bin", ArchiveKind::Binary, None);
        bin.tool = Tool::RustupInit;
        let dir = ensure(&cache, &bin, &opts()).await.expect("binary");
        assert_eq!(std::fs::read(dir.join("tool.bin")).unwrap(), TOOL_SCRIPT);
        assert_eq!(mode(&dir.join("tool.bin")), 0o755);

        let mut whl = pin(&srv, "tool.whl", ArchiveKind::Wheel, None);
        whl.tool = Tool::ComponentizePy;
        let dir = ensure(&cache, &whl, &opts()).await.expect("wheel");
        assert_eq!(
            std::fs::read(dir.join("tool.whl")).unwrap(),
            srv.files["tool.whl"]
        );
        let mut names: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        assert_eq!(names, vec![".ok", "tool.whl"], "a wheel is never extracted");
    })
    .await;
}

#[tokio::test]
async fn offline_mode_never_requests() {
    within(async {
        let srv = FixtureServer::spawn_default().await;
        let (_d, cache) = temp_cache();
        let mut offline = opts();
        offline.offline = true;

        let p = good(&srv);
        let err = ensure(&cache, &p, &offline).await.unwrap_err();
        assert!(matches!(err, ToolchainError::Offline { .. }), "{err:?}");
        assert!(err.to_string().contains("offline and not cached"), "{err}");
        assert_eq!(srv.counts.total(), 0);

        ensure(&cache, &p, &opts()).await.expect("online install");
        assert_eq!(srv.counts.total(), 1);
        let dir = ensure(&cache, &p, &offline).await.expect("offline cached");
        assert!(dir.join("bin/tool").is_file());
        assert_eq!(srv.counts.total(), 1);
    })
    .await;
}

#[tokio::test]
async fn mirror_rewrites_url_and_sha_stays_authoritative() {
    within(async {
        let origin = FixtureServer::spawn_default().await;
        let mirror = FixtureServer::spawn_default().await;
        let tampered = FixtureServer::spawn_tampered().await;

        let (_d, cache) = temp_cache();
        let p = good(&origin);
        let mut o = opts();
        o.mirror = Some(format!("{}/mirror/", mirror.base_url));
        ensure(&cache, &p, &o).await.expect("mirror install");
        assert_eq!(origin.counts.total(), 0);
        assert_eq!(mirror.counts.get("good.tar.gz"), 1);

        let (_d2, cache2) = temp_cache();
        let mut o = opts();
        o.mirror = Some(tampered.base_url.clone());
        let err = ensure(&cache2, &p, &o).await.unwrap_err();
        assert!(matches!(err, ToolchainError::ShaMismatch { .. }), "{err:?}");
        assert_eq!(tampered.counts.get("good.tar.gz"), 1);
        assert_eq!(origin.counts.total(), 0);
        assert!(tools_listing(&cache2).is_empty());
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_installs_download_once() {
    within(async {
        let srv = FixtureServer::spawn_default().await;
        let (_d, cache) = temp_cache();
        let p = good(&srv);
        let o = opts();
        let (a, b) = tokio::join!(ensure(&cache, &p, &o), ensure(&cache, &p, &o));
        assert_eq!(a.expect("a"), b.expect("b"));
        assert_eq!(srv.counts.total(), 1);

        // two Cache handles on one root, from two tasks: serialised by the process mutex
        let (_d2, c1) = temp_cache();
        let c2 = Cache::with_root(c1.root.clone());
        let xz = pin(&srv, "good.tar.xz", ArchiveKind::TarXz, Some(GOOD_PREFIX));
        let (p1, p2, o1, o2) = (xz.clone(), xz.clone(), opts(), opts());
        let t1 = tokio::spawn(async move { ensure(&c1, &p1, &o1).await });
        let t2 = tokio::spawn(async move { ensure(&c2, &p2, &o2).await });
        let (r1, r2) = (t1.await.unwrap(), t2.await.unwrap());
        assert_eq!(r1.expect("t1"), r2.expect("t2"));
        assert_eq!(srv.counts.get("good.tar.xz"), 1);

        // lock is reentrant across sequential holders (no leaked guard)
        let c3 = Cache::with_root(_d2.path().join("cache"));
        drop(c3.lock().await.expect("lock 1"));
        drop(c3.lock().await.expect("lock 2"));
    })
    .await;
}

#[tokio::test]
async fn size_cap_aborts_and_cleans_up() {
    within(async {
        let srv = FixtureServer::spawn_default().await;
        let (_d, cache) = temp_cache();
        let mut p = pin(&srv, "big.bin", ArchiveKind::Binary, None);
        p.max_bytes = Some(1024 * 1024);
        let err = ensure(&cache, &p, &opts()).await.unwrap_err();
        assert!(
            matches!(err, ToolchainError::TooLarge { limit, .. } if limit == 1024 * 1024),
            "{err:?}"
        );
        assert!(
            tools_listing(&cache).is_empty(),
            "{:?}",
            tools_listing(&cache)
        );
    })
    .await;
}

#[tokio::test]
async fn read_idle_timeout_fires() {
    within(async {
        let srv = FixtureServer::spawn_default().await;
        let (_d, cache) = temp_cache();
        let p = pin(&srv, "slow.bin", ArchiveKind::Binary, None);
        let mut o = opts();
        o.idle_timeout = Duration::from_secs(1);
        let start = Instant::now();
        let err = ensure(&cache, &p, &o).await.unwrap_err();
        assert!(
            matches!(
                err,
                ToolchainError::Timeout {
                    what: "read-idle",
                    ..
                }
            ),
            "{err:?}"
        );
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "{:?}",
            start.elapsed()
        );
        assert!(tools_listing(&cache).is_empty());

        let mut o = opts();
        o.total_timeout = Duration::from_millis(500);
        let err = ensure(&cache, &p, &o).await.unwrap_err();
        assert!(
            matches!(err, ToolchainError::Timeout { what: "total", .. }),
            "{err:?}"
        );
    })
    .await;
}

#[tokio::test]
async fn http_error_is_reported() {
    within(async {
        let srv = FixtureServer::spawn_default().await;
        let (_d, cache) = temp_cache();
        let p = pin(&srv, "missing.tar.gz", ArchiveKind::TarGz, None);
        let err = ensure(&cache, &p, &opts()).await.unwrap_err();
        assert!(
            matches!(&err, ToolchainError::Http { detail, .. } if detail.contains("404")),
            "{err:?}"
        );
    })
    .await;
}

#[tokio::test]
async fn prune_removes_only_stale_tools_unless_state() {
    within(async {
        let srv = FixtureServer::spawn_default().await;
        let (_d, cache) = temp_cache();
        let p = good(&srv);
        let kept = ensure(&cache, &p, &opts()).await.expect("install");
        let stale = cache.tools_dir().join("uv-0.9-deadbeef");
        std::fs::create_dir_all(stale.join("bin")).unwrap();
        let venv = cache.state_dir("uv-venv");
        std::fs::create_dir_all(venv.join("lib")).unwrap();

        let keep = ToolchainSet {
            id: "test".into(),
            pins: vec![p.clone()],
        };
        let removed = cache::prune(&cache, &keep, false).await.expect("prune");
        assert_eq!(removed, vec![stale.clone()]);
        assert!(!stale.exists());
        assert!(kept.join("bin/tool").is_file());
        assert!(venv.is_dir(), "state untouched without --state");

        let removed = cache::prune(&cache, &keep, true)
            .await
            .expect("prune --state");
        assert_eq!(removed, vec![venv.clone()]);
        assert!(!venv.exists());
        assert!(kept.join("bin/tool").is_file());

        let names: Vec<_> = cache::entries(&cache).into_iter().map(|e| e.name).collect();
        assert_eq!(names, vec![Cache::entry_name(&p)]);
    })
    .await;
}

#[tokio::test]
async fn interrupted_install_leftover_is_cleaned() {
    within(async {
        let srv = FixtureServer::spawn_default().await;
        let (_d, cache) = temp_cache();
        // a pid above Linux's PID_MAX_LIMIT (2^22) is never alive
        let leftover = cache.tools_dir().join(".tmp-4194399-0");
        std::fs::create_dir_all(leftover.join("entry/bin")).unwrap();
        std::fs::write(leftover.join("download"), b"partial").unwrap();
        // an incomplete final entry (no .ok) is replaced
        let p = good(&srv);
        std::fs::create_dir_all(cache.entry_dir(&p).join("junk")).unwrap();
        assert!(!cache.is_installed(&p));

        let dir = ensure(&cache, &p, &opts()).await.expect("install");
        assert!(!leftover.exists(), "stale tmp dir removed");
        assert!(!dir.join("junk").exists());
        assert!(dir.join("bin/tool").is_file());
        assert_eq!(tools_listing(&cache), vec![Cache::entry_name(&p)]);
    })
    .await;
}
