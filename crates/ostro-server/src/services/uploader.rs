//! Service: bottomless uploader (plan 08-13, P8-D3/D4).
//!
//! When `[backup] url` is set (08-04 then also set `NodeConfig.backup_enabled`, so the upload
//! floors start at `Some(log_start)`): open the store with `ostro_objstore::open_url`, acquire
//! the prefix `LOCK` (`takeover` from `[backup] takeover` / `--takeover`), and run
//! [`ostro_backup::uploader::Uploader`] until shutdown, then one final round bounded at 10 s.
//! Rounds start on a fixed cadence of `seal_interval / 2` (from round start) and upload up
//! to `[backup] upload_concurrency` partitions at once (0 = `min(P, 8)`), so the data-loss
//! window stays within `seal_interval` (P8-D23 as revised by 08-23). The startup log names the URL (a prefix, never credentials: those
//! live in separate config fields and `open_url` rejects userinfo).
//!
//! A failed LOCK (another incarnation owns the prefix) or a fenced round stops uploading and
//! sets `ostro_backup_fenced = 1`; the node keeps serving (local trimming stays held at the
//! upload floor, which the `ostro_backup_upload_floor_lag` alert catches).

use std::sync::Arc;
use std::time::Duration;

use ostro_backup::fence::{self, FenceError};
use ostro_backup::uploader::{METRIC_FENCED, Uploader, UploaderClusterDeps, UploaderConfig};
use ostro_meta::MetaView;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use super::ServiceCtx;

/// Bound of the final round on graceful shutdown.
const FINAL_ROUND: Duration = Duration::from_secs(10);
/// Retry period of a failed LOCK acquisition caused by a store error.
const LOCK_RETRY: Duration = Duration::from_secs(5);

/// Start the service; `None` when backup is off.
pub fn spawn(ctx: &ServiceCtx) -> Option<JoinHandle<()>> {
    let cfg = ctx.config.backup.to_backup_config();
    let url = cfg.url.clone()?;
    let ctx = ctx.clone();
    Some(tokio::spawn(async move {
        let store = match ostro_objstore::open_url(&url, &fence::store_options(&cfg)) {
            Ok(s) => s,
            Err(e) => {
                error!(url = %url, error = %e, "bottomless backup disabled: cannot open the object store");
                return;
            }
        };
        let cluster_id = ctx
            .meta
            .view()
            .read(|s| s.cluster().map(|c| c.value.cluster_id.clone()))
            .unwrap_or_default();
        let ucfg = UploaderConfig::from_backup(&cfg, cluster_id, ctx.config.node.node_id);
        // Phase 12 (12-15/12-14, D-P12-20): leader-only per-partition uploads with
        // epoch-fenced heads, single node included; LOCK and manifest/current only while the
        // cluster has one member (D-P12-3). The member count follows meta's Member records.
        let members = member_count_watch(ctx.meta.view().clone(), ctx.shutdown.clone());
        let up = Arc::new(
            Uploader::new(
                Arc::clone(&ctx.node),
                Some(ctx.meta.view().clone()),
                Arc::clone(&ctx.clock),
                ucfg,
            )
            .with_cluster(UploaderClusterDeps {
                roles: ctx.node.roles(),
                members: members.clone(),
            }),
        );
        metrics::gauge!(METRIC_FENCED).set(0.0);
        // LOCK (P8-D4).
        loop {
            let n = *members.borrow();
            match fence::acquire_if_single_member(&store, up.ident(), cfg.takeover, n).await {
                Ok(_) => break,
                Err(FenceError::Store(e)) => {
                    warn!(error = %e, "bottomless LOCK: object store error; retrying");
                    tokio::select! {
                        () = ctx.shutdown.cancelled() => return,
                        () = tokio::time::sleep(LOCK_RETRY) => {}
                    }
                }
                Err(e) => {
                    metrics::gauge!(METRIC_FENCED).set(1.0);
                    error!(url = %url, error = %e, "bottomless backup fenced at startup; not uploading");
                    return;
                }
            }
        }
        info!(
            url = %url,
            seal_interval = ?cfg.seal_interval,
            round_interval = ?up.config().round_interval(),
            upload_concurrency = up.config().concurrency(ctx.node.log_cut().len() as u16),
            incarnation = %up.ident().incarnation,
            "bottomless uploader started"
        );
        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        let runner = tokio::spawn(Arc::clone(&up).run(Arc::clone(&store), stop_rx));
        ctx.shutdown.cancelled().await;
        let _ = stop_tx.send(true);
        if let Err(e) = runner.await {
            warn!(error = %e, "uploader task failed");
        }
        if up.is_fenced() {
            return;
        }
        match tokio::time::timeout(FINAL_ROUND, up.round(&store)).await {
            Ok(Ok(r)) => info!(
                generation = r.manifest.as_ref().map(|m| m.generation),
                "bottomless final round done"
            ),
            Ok(Err(e)) => warn!(error = %e, "bottomless final round failed"),
            Err(_) => warn!("bottomless final round timed out after {FINAL_ROUND:?}"),
        }
    }))
}

/// The number of `Member` records in meta, kept current until `stop`.
fn member_count_watch(
    view: MetaView,
    stop: tokio_util::sync::CancellationToken,
) -> tokio::sync::watch::Receiver<usize> {
    let count = |v: &MetaView| v.read(|s| s.members().count());
    let (tx, rx) = tokio::sync::watch::channel(count(&view));
    tokio::spawn(async move {
        let mut applied = view.subscribe();
        loop {
            tokio::select! {
                () = stop.cancelled() => return,
                r = applied.changed() => if r.is_err() { return },
            }
            let n = count(&view);
            tx.send_if_modified(|cur| {
                let changed = *cur != n;
                *cur = n;
                changed
            });
        }
    });
    rx
}
