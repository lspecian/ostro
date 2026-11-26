//! Service: local blob garbage collection plus delayed remote deletion (P8-D8, plan 08-19).
//!
//! When `[blob_gc] enabled` (the default), every `interval` (default `1h`) after the node
//! opened (every readiness barrier is open before [`super::Services::start`]):
//!
//! 1. `gc_start` = the injected clock;
//! 2. **mark** = ⋃ `PartitionStore::blob_refs_snapshot()` (one fjall snapshot per partition,
//!    exhaustive visitor) ∪ every meta `Module` sha (module blobs are never deleted; Phase 13
//!    owns module GC) ∪ the `.blobrefs` sidecars of every retained local snapshot. Any
//!    failure (a partition without a handle, an unreadable sidecar) aborts the pass: the
//!    sweep never runs on an incomplete mark;
//! 3. `BlobStore::sweep(mark, gc_start - grace)` (default grace `1h`). Puts hold the GC lock
//!    in shared mode and touch the mtime of an existing blob, so a blob re-put between the
//!    mark and the sweep is young and kept;
//! 4. with `[backup] url` set: the deleted shas go to the remote deletion ledger
//!    (`<data>/backup/gc-ledger`), remote blobs that are neither local nor in the ledger are
//!    adopted, and, if this node still owns the prefix (`LOCK` names its incarnation), the
//!    ledger deletes remote copies older than the retention window (the age of the oldest
//!    retained manifest) unless the blob is local again. A watcher task re-uploads a
//!    remotely deleted blob that is put again ([`ostro_backup::remote_gc::ReputGuard`]).
//!
//! Metrics: `ostro_blob_gc_deleted_total`, `ostro_blob_gc_bytes_freed_total`,
//! `ostro_blob_gc_duration_seconds`. Every pass is logged at info.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ostro_backup::fence::{self, Ident};
use ostro_backup::remote_gc::{self, Ledger, RunReport};
use ostro_blob::{BlobStore, SweepReport};
use ostro_meta::MetaView;
use ostro_objstore::ObjectStore;
use ostro_partition::Clock;
use ostro_partition::Node;
use ostro_partition::leader::snapshot::BLOBREFS_EXT;
use ostro_types::PartitionId;
use tokio::sync::{Mutex, broadcast};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::ServiceCtx;

/// `ostro_blob_gc_deleted_total`: local blobs deleted.
pub const METRIC_DELETED: &str = "ostro_blob_gc_deleted_total";
/// `ostro_blob_gc_bytes_freed_total`: bytes of the deleted local blobs.
pub const METRIC_BYTES_FREED: &str = "ostro_blob_gc_bytes_freed_total";
/// `ostro_blob_gc_duration_seconds`: duration of one pass.
pub const METRIC_DURATION: &str = "ostro_blob_gc_duration_seconds";

/// Start the service; `None` when `[blob_gc] enabled = false`.
pub fn spawn(ctx: &ServiceCtx) -> Option<JoinHandle<()>> {
    let section = ctx.config.blob_gc.clone();
    if !section.enabled {
        return None;
    }
    let ctx = ctx.clone();
    Some(tokio::spawn(async move {
        let backup = ctx.config.backup.to_backup_config();
        let remote = match backup.url.as_deref() {
            None => None,
            Some(url) => match ostro_objstore::open_url(url, &fence::store_options(&backup)) {
                Ok(store) => Some(store),
                Err(e) => {
                    warn!(url = %url, error = %e, "blob gc: cannot open the backup store; local GC only");
                    None
                }
            },
        };
        let gc = match BlobGc::new(
            Arc::clone(&ctx.blobs),
            ctx.data_dir.clone(),
            section.grace.get(),
            Arc::clone(&ctx.clock),
            remote,
        ) {
            Ok(gc) => Arc::new(gc),
            Err(e) => {
                warn!(error = %e, "blob gc disabled: cannot open the deletion ledger");
                return;
            }
        };
        let watcher = gc.spawn_reput_watcher(ctx.shutdown.clone());
        let interval = section.interval.get().max(Duration::from_secs(1));
        info!(?interval, grace = ?section.grace.get(), "blob gc started");
        loop {
            tokio::select! {
                () = ctx.shutdown.cancelled() => break,
                () = tokio::time::sleep(interval) => {}
            }
            if let Err(e) = gc.pass(&ctx.node, ctx.meta.view()).await {
                warn!(error = %e, "blob gc pass failed; retried next interval");
            }
        }
        if let Some(w) = watcher {
            let _ = w.await;
        }
    }))
}

/// The remote half (bottomless on).
struct Remote {
    store: Arc<dyn ObjectStore>,
    ledger: Mutex<Ledger>,
}

/// One GC pass's outcome.
#[derive(Clone, Debug, Default)]
pub struct PassReport {
    /// Size of the mark set.
    pub marked: usize,
    /// The local sweep.
    pub sweep: SweepReport,
    /// Remote blobs adopted into the ledger.
    pub adopted: usize,
    /// The remote ledger run (`None`: backup off, no manifest yet, or the prefix is not ours).
    pub remote: Option<RunReport>,
    /// Wall time of the pass (metric only).
    pub duration: Duration,
}

/// Blob GC state: the store, the grace, the clock and the optional remote ledger.
pub struct BlobGc {
    blobs: Arc<BlobStore>,
    grace: Duration,
    clock: Arc<dyn Clock>,
    remote: Option<Remote>,
    /// Serialises passes (the service loop and tests).
    pass_lock: Mutex<()>,
}

impl std::fmt::Debug for BlobGc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlobGc")
            .field("grace", &self.grace)
            .field("remote", &self.remote.is_some())
            .finish_non_exhaustive()
    }
}

impl BlobGc {
    /// Build the GC; opens the ledger under `data_dir` when `remote` is set.
    pub fn new(
        blobs: Arc<BlobStore>,
        data_dir: PathBuf,
        grace: Duration,
        clock: Arc<dyn Clock>,
        remote: Option<Arc<dyn ObjectStore>>,
    ) -> Result<Self, String> {
        let remote = match remote {
            Some(store) => Some(Remote {
                store,
                ledger: Mutex::new(Ledger::open(&data_dir).map_err(|e| e.to_string())?),
            }),
            None => None,
        };
        Ok(Self {
            blobs,
            grace,
            clock,
            remote,
            pass_lock: Mutex::new(()),
        })
    }

    /// Watch blob puts and re-upload blobs whose remote copy this process deleted (see
    /// [`ostro_backup::remote_gc::ReputGuard`]). `None` when backup is off.
    pub fn spawn_reput_watcher(&self, shutdown: CancellationToken) -> Option<JoinHandle<()>> {
        let remote = self.remote.as_ref()?;
        let store = Arc::clone(&remote.store);
        let guard = remote.ledger.try_lock().ok()?.guard();
        let blobs = Arc::clone(&self.blobs);
        let mut rx = blobs.subscribe_puts();
        Some(tokio::spawn(async move {
            loop {
                let ev = tokio::select! {
                    () = shutdown.cancelled() => break,
                    ev = rx.recv() => ev,
                };
                let r = match ev {
                    Ok(sha) => guard.on_put(&store, &blobs, &sha).await.map(usize::from),
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        guard.on_lagged(&store, &blobs).await
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                };
                if let Err(e) = r {
                    warn!(error = %e, "blob gc: re-upload of a re-put blob failed");
                }
            }
        }))
    }

    /// The mark set (see the module docs). Errors abort the pass.
    pub async fn mark(&self, node: &Node, meta: &MetaView) -> Result<HashSet<[u8; 32]>, String> {
        let mut mark: HashSet<[u8; 32]> = HashSet::new();
        let partitions = node.log_cut().len() as u16;
        for p in 0..partitions {
            let pid = PartitionId(p);
            let handle = node
                .handle(pid)
                .ok_or_else(|| format!("partition {p} has no local handle; mark incomplete"))?;
            let store = Arc::clone(handle.store());
            let refs = tokio::task::spawn_blocking(move || store.blob_refs_snapshot())
                .await
                .map_err(|e| format!("partition {p} mark task: {e}"))?
                .map_err(|e| format!("partition {p} mark: {e}"))?;
            mark.extend(refs);
            for s in node.local_snapshots(pid).await {
                let sidecar = s.path.with_extension(BLOBREFS_EXT);
                match tokio::fs::read(&sidecar).await {
                    Ok(b) => mark.extend(
                        parse_blobrefs(&b)
                            .ok_or_else(|| format!("{}: malformed", sidecar.display()))?,
                    ),
                    // Pruned since the listing: no longer a retained snapshot.
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(format!("{}: {e}", sidecar.display())),
                }
            }
        }
        meta.read(|s| mark.extend(s.modules().map(|m| m.value.sha256)));
        Ok(mark)
    }

    /// One full pass: mark, sweep, ledger record, remote run.
    pub async fn pass(&self, node: &Node, meta: &MetaView) -> Result<PassReport, String> {
        let _serial = self.pass_lock.lock().await;
        #[allow(clippy::disallowed_methods, clippy::disallowed_types)]
        let started = std::time::Instant::now(); // clock-exempt: pass duration metric only
        let gc_start = self.clock.now_ms();
        let older_than = (UNIX_EPOCH + Duration::from_millis(gc_start))
            .checked_sub(self.grace)
            .unwrap_or(SystemTime::UNIX_EPOCH);
        let mark = self.mark(node, meta).await?;
        let marked = mark.len();
        let blobs = Arc::clone(&self.blobs);
        let sweep = tokio::task::spawn_blocking(move || blobs.sweep(&mark, older_than))
            .await
            .map_err(|e| format!("sweep task: {e}"))?
            .map_err(|e| format!("sweep: {e}"))?;
        metrics::counter!(METRIC_DELETED).increment(sweep.deleted);
        metrics::counter!(METRIC_BYTES_FREED).increment(sweep.bytes_freed);
        let mut report = PassReport {
            marked,
            sweep,
            ..PassReport::default()
        };
        if let Some(remote) = &self.remote {
            let mut ledger = remote.ledger.lock().await;
            ledger
                .record(&report.sweep.deleted_shas, gc_start)
                .map_err(|e| format!("ledger record: {e}"))?;
            match self.remote_pass(remote, &mut ledger, node).await {
                Ok((adopted, run)) => {
                    report.adopted = adopted;
                    report.remote = run;
                }
                // The local part is done; remote deletion retries next pass.
                Err(e) => warn!(error = %e, "blob gc: remote pass failed"),
            }
        }
        report.duration = started.elapsed();
        metrics::histogram!(METRIC_DURATION).record(report.duration.as_secs_f64());
        info!(
            marked = report.marked,
            scanned = report.sweep.scanned,
            deleted = report.sweep.deleted,
            bytes_freed = report.sweep.bytes_freed,
            kept_young = report.sweep.kept_young,
            unknown = report.sweep.unknown,
            remote_deleted = report.remote.as_ref().map_or(0, |r| r.deleted),
            ledger = report.remote.as_ref().map(|r| r.remaining),
            duration_ms = report.duration.as_millis() as u64,
            "blob gc pass"
        );
        Ok(report)
    }

    async fn remote_pass(
        &self,
        remote: &Remote,
        ledger: &mut Ledger,
        node: &Node,
    ) -> Result<(usize, Option<RunReport>), String> {
        // Never touch a prefix another incarnation owns (it was taken over, or we were fenced).
        let ours = match remote.store.get(fence::LOCK_KEY).await {
            Ok(Some((b, _))) => serde_json::from_slice::<Ident>(&b).is_ok_and(|id| {
                id.incarnation == ostro_partition::node::hex16(&node.incarnation())
            }),
            Ok(None) => false,
            Err(e) => return Err(format!("LOCK read: {e}")),
        };
        if !ours {
            return Ok((0, None));
        }
        let now = self.clock.now_ms();
        let adopted = ledger
            .adopt_orphans(&remote.store, &self.blobs, now)
            .await
            .map_err(|e| e.to_string())?;
        let Some(window) = remote_gc::retention_window(&remote.store, now)
            .await
            .map_err(|e| e.to_string())?
        else {
            return Ok((adopted, None));
        };
        let run = ledger
            .run(&remote.store, window, now, &self.blobs)
            .await
            .map_err(|e| e.to_string())?;
        Ok((adopted, Some(run)))
    }
}

/// `.blobrefs` sidecar: `u32 LE n`, then `n` 32-byte digests (08-09).
fn parse_blobrefs(b: &[u8]) -> Option<Vec<[u8; 32]>> {
    let n = u32::from_le_bytes(b.get(..4)?.try_into().ok()?) as usize;
    let body = b.get(4..)?;
    if body.len() != n.checked_mul(32)? {
        return None;
    }
    let (digests, rest) = body.as_chunks::<32>();
    rest.is_empty().then(|| digests.to_vec())
}

#[cfg(test)]
mod tests {
    //! Service-level GC: a real in-process server (1 partition, snapshots on, `[runs]
    //! retention = 1s`, the periodic task off so the test drives passes itself).
    #![allow(clippy::disallowed_methods, clippy::disallowed_types)]

    use std::collections::BTreeSet;
    use std::time::Instant;

    use ostro_partition::store::values::RunStatus;
    use ostro_types::RunId;
    use serde_json::json;
    use sha2::{Digest as _, Sha256};

    use super::*;
    use crate::config::{FriendlyDuration, ServerConfig};
    use crate::server::{ServerHandle, start};

    const LIMIT: Duration = Duration::from_secs(60);

    /// Build the `guests/` workspace (once per process) and read the named guest.
    fn guest_wasm(name: &str) -> Vec<u8> {
        static BUILT: std::sync::OnceLock<()> = std::sync::OnceLock::new();
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let target_dir = root.join("target/guests");
        BUILT.get_or_init(|| {
            let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
            let out = std::process::Command::new(cargo)
                .args([
                    "build",
                    "--release",
                    "--target",
                    "wasm32-wasip2",
                    "--manifest-path",
                ])
                .arg(root.join("guests/Cargo.toml"))
                .arg("--target-dir")
                .arg(&target_dir)
                .env_remove("RUSTFLAGS")
                .env_remove("CARGO_ENCODED_RUSTFLAGS")
                .env_remove("CARGO_BUILD_TARGET")
                .env_remove("CARGO_TARGET_DIR")
                .env_remove("RUSTC_WRAPPER")
                .output()
                .expect("spawn cargo for the guests build");
            assert!(
                out.status.success(),
                "guests build failed:\n{}",
                String::from_utf8_lossy(&out.stderr)
            );
        });
        let path = target_dir
            .join("wasm32-wasip2/release")
            .join(format!("{}.wasm", name.replace('-', "_")));
        std::fs::read(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
    }

    struct Fx {
        h: ServerHandle,
        http: reqwest::Client,
        gc: BlobGc,
        blobs: Arc<BlobStore>,
        module: [u8; 32],
        _dir: tempfile::TempDir,
    }

    impl Fx {
        fn url(&self, path: &str) -> String {
            format!("http://{}{path}", self.h.local_addr())
        }

        async fn start_run(&self, name: String) -> RunId {
            let r = self
                .http
                .post(self.url("/v1/workflows/sdk-basic/runs"))
                .json(&json!({"input": {"name": name, "n": 3}}))
                .send()
                .await
                .expect("POST run");
            assert!(r.status().is_success(), "start: {}", r.status());
            let v: serde_json::Value = r.json().await.expect("json");
            v["run_id"]
                .as_str()
                .expect("run_id")
                .parse()
                .expect("run id")
        }

        fn status(&self, run: &RunId) -> Option<RunStatus> {
            self.h
                .node()
                .handle(run.partition())
                .expect("handle")
                .read_run(run)
                .expect("read run")
                .map(|r| r.status)
        }

        async fn wait_completed(&self, run: &RunId) {
            let t0 = Instant::now(); // clock-exempt: test wait deadline
            while self.status(run) != Some(RunStatus::Completed) {
                assert!(t0.elapsed() < LIMIT, "{run}: {:?}", self.status(run));
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }

        async fn wait_expired(&self, run: &RunId) {
            let t0 = Instant::now(); // clock-exempt: test wait deadline
            while self.status(run).is_some() {
                assert!(t0.elapsed() < LIMIT, "{run} never expired");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }

        /// Every blob except the module.
        fn payload_blobs(&self) -> BTreeSet<[u8; 32]> {
            let mut all: BTreeSet<_> = self.blobs.list().expect("list").into_iter().collect();
            all.remove(&self.module);
            all
        }

        fn has(&self, sha: &[u8; 32]) -> bool {
            self.blobs.contains(&ostro_blob::BlobRef {
                sha256: *sha,
                len: 0,
            })
        }

        async fn pass(&self) -> PassReport {
            self.gc
                .pass(self.h.node(), self.h.meta().view())
                .await
                .expect("gc pass")
        }
    }

    async fn fx() -> Fx {
        fx_with(None).await
    }

    async fn fx_with(remote: Option<Arc<dyn ObjectStore>>) -> Fx {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut cfg = ServerConfig::server_defaults();
        cfg.api.listen = "127.0.0.1:0".parse().expect("addr");
        cfg.cluster.listen = "127.0.0.1:0".parse().expect("addr");
        cfg.node.data_dir = dir.path().to_path_buf();
        cfg.node.partitions = 1;
        // Phase 14 (14-03): this loopback test server keeps the open API.
        cfg.auth.mode = crate::config::AuthMode::None;
        cfg.runs.retention = FriendlyDuration(Duration::from_secs(1));
        cfg.blob_gc.enabled = false;
        cfg.snapshots.enabled = true;
        cfg.snapshots.interval = FriendlyDuration(Duration::from_secs(3600));
        cfg.snapshots.bytes_threshold = u64::MAX;
        cfg.snapshots.keep = 2;
        let h = start(cfg).await.expect("server start");
        assert!(
            !h.services().running().contains(&"blob_gc"),
            "[blob_gc] enabled = false starts no task"
        );
        let http = reqwest::Client::new();
        let wasm = guest_wasm("sdk-basic");
        let module: [u8; 32] = Sha256::digest(&wasm).into();
        let r = http
            .put(format!(
                "http://{}/v1/modules/{}",
                h.local_addr(),
                ostro_blob::hex(&module)
            ))
            .header("content-type", "application/wasm")
            .body(wasm)
            .send()
            .await
            .expect("PUT module");
        assert!(r.status().is_success(), "deploy: {}", r.status());
        let blobs = Arc::clone(h.node().handle(PartitionId(0)).expect("p0").blobs());
        let gc = BlobGc::new(
            Arc::clone(&blobs),
            dir.path().to_path_buf(),
            Duration::ZERO,
            Arc::clone(h.clock()),
            remote,
        )
        .expect("gc");
        Fx {
            h,
            http,
            gc,
            blobs,
            module,
            _dir: dir,
        }
    }

    #[test]
    fn blobrefs_sidecar_parses() {
        let mut b = 2u32.to_le_bytes().to_vec();
        b.extend([1u8; 32]);
        b.extend([2u8; 32]);
        assert_eq!(parse_blobrefs(&b), Some(vec![[1; 32], [2; 32]]));
        assert_eq!(parse_blobrefs(&b[..b.len() - 1]), None);
        assert_eq!(parse_blobrefs(&0u32.to_le_bytes()), Some(vec![]));
        assert_eq!(parse_blobrefs(&[1, 0]), None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn blob_gc_reclaims_expired_run_blobs_and_keeps_modules_and_snapshot_refs() {
        let fx = fx().await;
        assert!(fx.has(&fx.module), "modules live in the blob store");

        // 1. A run with a large input (offloaded to a blob) completes.
        let run1 = fx.start_run("a".repeat(300 * 1024)).await;
        fx.wait_completed(&run1).await;
        let b1 = fx.payload_blobs();
        assert!(!b1.is_empty(), "the large input was offloaded to a blob");

        // While the run exists its blobs are marked, even with grace 0.
        let rep = fx.pass().await;
        assert_eq!(rep.sweep.deleted, 0, "{rep:?}");
        assert!(b1.iter().all(|s| fx.has(s)));
        assert!(rep.marked > b1.len(), "payload blobs + the module");

        // 2. Expired (retention 1 s): one pass with grace 0 reclaims them; the module stays.
        fx.wait_expired(&run1).await;
        let rep = fx.pass().await;
        assert_eq!(rep.sweep.deleted as usize, b1.len(), "{rep:?}");
        assert!(rep.sweep.bytes_freed >= 300 * 1024);
        assert!(b1.iter().all(|s| !fx.has(s)), "expired run blobs reclaimed");
        assert!(fx.has(&fx.module), "a module blob is never deleted");
        assert!(rep.remote.is_none(), "no backup, no remote pass");

        // 3. A blob referenced by a retained snapshot is kept until that snapshot is pruned.
        let run2 = fx.start_run("b".repeat(400 * 1024)).await;
        fx.wait_completed(&run2).await;
        let b2 = fx.payload_blobs();
        assert!(!b2.is_empty());
        let p = run2.partition();
        let s1 =
            fx.h.node()
                .snapshot_now(p)
                .await
                .expect("snapshot")
                .expect("state changed since the last snapshot");
        fx.wait_expired(&run2).await;
        let rep = fx.pass().await;
        assert_eq!(
            rep.sweep.deleted, 0,
            "kept by the snapshot's blobrefs: {rep:?}"
        );
        assert!(b2.iter().all(|s| fx.has(s)));

        // Two newer snapshots (keep = 2) prune s1.
        fx.h.node()
            .snapshot_now(p)
            .await
            .expect("snapshot")
            .expect("the expiry changed the state");
        let run3 = fx.start_run("c".into()).await;
        fx.wait_completed(&run3).await;
        fx.h.node()
            .snapshot_now(p)
            .await
            .expect("snapshot")
            .expect("run 3 changed the state");
        let t0 = Instant::now(); // clock-exempt: test wait deadline
        while fx
            .h
            .node()
            .local_snapshots(p)
            .await
            .iter()
            .any(|s| s.applied_offset == s1.applied_offset)
        {
            assert!(t0.elapsed() < LIMIT, "s1 never pruned");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(!s1.path.exists());
        let rep = fx.pass().await;
        assert!(rep.sweep.deleted as usize >= b2.len(), "{rep:?}");
        assert!(
            b2.iter().all(|s| !fx.has(s)),
            "reclaimed once s1 was pruned"
        );
        assert!(fx.has(&fx.module));
        fx.h.shutdown().await.expect("shutdown");
    }

    fn manifest_at(cut_at_ms: u64) -> bytes::Bytes {
        ostro_backup::manifest::encode(&ostro_backup::manifest::Manifest {
            format: ostro_backup::manifest::FORMAT,
            cluster_id: "c".into(),
            node_id: 1,
            incarnation: [0; 16],
            generation: 1,
            prev_manifest_sha256: [0; 32],
            cut_at_ms,
            cut: Vec::new(),
            meta_snapshot_key: None,
            meta_snapshot_sha256: None,
            ostro_version: "test".into(),
        })
        .into()
    }

    /// Bottomless on: deleted shas reach the ledger; remote copies are deleted only when the
    /// prefix is ours (LOCK) and the oldest manifest was cut after the local deletion.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn remote_copies_follow_the_ledger_window_and_the_lock() {
        let remote_dir = tempfile::tempdir().expect("remote");
        let store: Arc<dyn ObjectStore> =
            Arc::new(ostro_objstore::LocalFs::new(remote_dir.path()).expect("localfs"));
        let fx = fx_with(Some(Arc::clone(&store))).await;
        let run = fx.start_run("r".repeat(300 * 1024)).await;
        fx.wait_completed(&run).await;
        let b = fx.payload_blobs();
        assert!(!b.is_empty());
        // What the uploader would have done.
        for sha in &b {
            let bytes = std::fs::read(fx.blobs.path_of(&ostro_blob::BlobRef {
                sha256: *sha,
                len: 0,
            }))
            .expect("read blob");
            store
                .put(&ostro_backup::uploader::blob_key(sha), bytes.into())
                .await
                .expect("upload");
        }
        let remote_has = |sha: [u8; 32]| {
            let store = Arc::clone(&store);
            async move {
                store
                    .head(&ostro_backup::uploader::blob_key(&sha))
                    .await
                    .expect("head")
                    .is_some()
            }
        };
        fx.wait_expired(&run).await;

        // No LOCK of ours: local GC runs and the ledger is written, nothing remote is touched.
        let rep = fx.pass().await;
        assert_eq!(rep.sweep.deleted as usize, b.len());
        assert!(rep.remote.is_none());
        let ledger = std::fs::read_to_string(fx._dir.path().join(remote_gc::LEDGER_PATH))
            .expect("ledger written");
        assert_eq!(ledger.lines().count(), b.len());
        for sha in &b {
            assert!(remote_has(*sha).await);
        }

        // Our LOCK, a manifest cut before the deletion: inside the window, kept.
        let ident = Ident::new("c", 1, fx.h.node().incarnation());
        store
            .put(
                fence::LOCK_KEY,
                serde_json::to_vec(&ident).expect("json").into(),
            )
            .await
            .expect("LOCK");
        store
            .put(ostro_backup::manifest::CURRENT_KEY, manifest_at(1))
            .await
            .expect("manifest");
        let rep = fx.pass().await;
        let r = rep.remote.expect("remote pass ran");
        assert_eq!((r.due, r.deleted, r.remaining), (0, 0, b.len()));
        for sha in &b {
            assert!(remote_has(*sha).await);
        }

        // The oldest restore point is now cut after the deletion: the copies go.
        tokio::time::sleep(Duration::from_millis(5)).await;
        store
            .put(
                ostro_backup::manifest::CURRENT_KEY,
                manifest_at(fx.h.clock().now_ms()),
            )
            .await
            .expect("manifest");
        tokio::time::sleep(Duration::from_millis(5)).await;
        let rep = fx.pass().await;
        let r = rep.remote.expect("remote pass ran");
        assert_eq!((r.deleted, r.remaining), (b.len(), 0), "{r:?}");
        for sha in &b {
            assert!(!remote_has(*sha).await);
        }
        fx.h.shutdown().await.expect("shutdown");
    }
}
