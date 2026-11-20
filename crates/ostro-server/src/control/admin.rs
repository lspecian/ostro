//! Admin controller: `ostro_api::phase8::AdminApi`, the online backup (`GET
//! /v1/admin/backup`, P8-D7, plan 08-18).
//!
//! One backup = a fresh snapshot per partition (so a restore replays only a short tail),
//! then one bottomless upload round of the running node into a temporary LocalFs store
//! under a fresh fence identity (`publish_floor = false`, `retention = false`: the live
//! node's trimming and any configured bucket are untouched), packed as a tar
//! ([`ostro_backup::archive::backup_node_to_tar`]) and streamed as `application/x-tar`.
//! The staging directory is removed before the body streams (the open file keeps the tar
//! readable). `ostro restore --from <file.tar>` restores it through the same code path as a
//! bucket.

use std::sync::Arc;

use axum::body::Body;
use ostro_api::ApiError;
use ostro_api::phase8::{AdminApi, BackupStream};
use ostro_backup::archive::backup_node_to_tar;
use ostro_types::PartitionId;

use super::ControlCtx;

/// `AdminApi` for the server.
#[derive(Debug)]
pub struct AdminController {
    ctx: ControlCtx,
}

impl AdminController {
    /// Over the shared controller context.
    pub fn new(ctx: ControlCtx) -> Self {
        Self { ctx }
    }

    /// The shared controller context.
    pub fn ctx(&self) -> &ControlCtx {
        &self.ctx
    }
}

#[async_trait::async_trait]
impl AdminApi for AdminController {
    async fn backup(&self) -> Result<BackupStream, ApiError> {
        let node = self.ctx.node()?;
        for p in 0..node.log_cut().len() {
            let pid = PartitionId(p as u16);
            if node.handle(pid).is_none() {
                continue;
            }
            if let Err(e) = node.snapshot_now(pid).await {
                tracing::warn!(partition = p, error = %e, "online backup: snapshot failed; the backup replays a longer tail");
            }
        }
        let staging = tempfile::Builder::new()
            .prefix("ostro-online-backup-")
            .tempdir()
            .map_err(|e| ApiError::internal(format!("backup staging directory: {e}")))?;
        let out = staging.path().join("ostro-backup.tar");
        let report = backup_node_to_tar(
            node,
            Some(self.ctx.meta.view().clone()),
            Arc::clone(&self.ctx.clock),
            self.ctx.config.node.node_id,
            false,
            &out,
        )
        .await
        .map_err(|e| ApiError::unavailable(format!("backup failed: {e}")))?;
        let file = tokio::fs::File::open(&out)
            .await
            .map_err(|e| ApiError::internal(format!("backup file: {e}")))?;
        // Unix: the open descriptor keeps the unlinked tar readable while it streams.
        drop(staging);
        tracing::info!(
            generation = report.manifest.generation,
            entries = report.entries,
            bytes = report.bytes,
            "online backup streamed"
        );
        let body = Body::from_stream(tokio_util::io::ReaderStream::new(file));
        Ok(BackupStream::new(body))
    }
}
