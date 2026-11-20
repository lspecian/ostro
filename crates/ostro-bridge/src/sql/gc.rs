//! Batched GC of `ostro_effects` token rows.
//!
//! Every [`GC_INTERVAL`] the task deletes, per built pool, token rows older than the retention
//! (`[connectors] sql_effects_retention`, default 168 h, at least the run retention, D9) in
//! batches of [`GC_BATCH`] until a batch comes back short. Only connections used since the
//! node started have a pool, so only those are collected (a later use collects the rest).
//! A re-drive older than the retention minus 1 h never relies on a token row (W12), so GC
//! cannot turn a re-drive into a duplicate.

use std::sync::Arc;
use std::time::Duration;

use sqlx::AssertSqlSafe;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::SqlConnector;
use super::config::Dialect;
use super::pool::{DbPool, PoolEntry};

/// Rows deleted per statement.
pub const GC_BATCH: u64 = 1000;
/// Interval between GC passes.
pub const GC_INTERVAL: Duration = Duration::from_secs(600);

/// The batched delete statement of `table`.
pub fn gc_sql(dialect: Dialect, table: &str) -> String {
    match dialect {
        Dialect::Postgres => format!(
            "DELETE FROM {table} WHERE effect_key IN (SELECT effect_key FROM {table} \
             WHERE created_at < now() - $1 * interval '1 second' LIMIT {GC_BATCH})"
        ),
        Dialect::Mysql => format!(
            "DELETE FROM {table} WHERE created_at < NOW(6) - INTERVAL ? SECOND LIMIT {GC_BATCH}"
        ),
    }
}

/// Deletes the token rows of `entry` older than `retention`; returns the count.
pub async fn gc_entry(entry: &PoolEntry, retention: Duration) -> Result<u64, sqlx::Error> {
    let sql = gc_sql(entry.cfg.dialect, &entry.cfg.effects_table);
    let mut total = 0;
    loop {
        let n = match &entry.pool {
            DbPool::Pg(p) => sqlx::query(AssertSqlSafe(sql.clone()))
                .bind(retention.as_secs_f64())
                .execute(p)
                .await?
                .rows_affected(),
            DbPool::My(p) => sqlx::query(AssertSqlSafe(sql.clone()))
                .bind(i64::try_from(retention.as_secs()).unwrap_or(i64::MAX))
                .execute(p)
                .await?
                .rows_affected(),
        };
        total += n;
        if n < GC_BATCH {
            return Ok(total);
        }
    }
}

/// The running GC task. Dropping the handle cancels it.
#[derive(Debug)]
pub struct GcHandle {
    cancel: CancellationToken,
    join: Option<JoinHandle<()>>,
}

impl GcHandle {
    /// Cancels the task and waits for it to finish.
    pub async fn stop(mut self) {
        self.cancel.cancel();
        if let Some(j) = self.join.take() {
            let _ = j.await;
        }
    }
}

impl Drop for GcHandle {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

impl SqlConnector {
    /// One GC pass over every built pool; returns the rows deleted. Errors are logged and
    /// the next connection is collected.
    pub async fn gc_once(&self, retention: Duration) -> u64 {
        let mut total = 0;
        for entry in self.pools.entries().await {
            match gc_entry(&entry, retention).await {
                Ok(n) => total += n,
                Err(e) => tracing::warn!(
                    connection = %entry.name,
                    error = %entry.redact(&e.to_string()),
                    "sql: token-row GC failed"
                ),
            }
        }
        total
    }

    /// Spawns the GC task: a pass every [`GC_INTERVAL`] until the handle is stopped or
    /// dropped.
    pub fn spawn_gc(self: &Arc<Self>, retention: Duration) -> GcHandle {
        let cancel = CancellationToken::new();
        let token = cancel.clone();
        let this = Arc::clone(self);
        let join = tokio::spawn(async move {
            loop {
                tokio::select! {
                    () = token.cancelled() => return,
                    () = tokio::time::sleep(GC_INTERVAL) => {}
                }
                tokio::select! {
                    () = token.cancelled() => return,
                    n = this.gc_once(retention) => {
                        if n > 0 {
                            tracing::debug!(rows = n, "sql: token rows collected");
                        }
                    }
                }
            }
        });
        GcHandle {
            cancel,
            join: Some(join),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gc_statements() {
        assert_eq!(
            gc_sql(Dialect::Postgres, "ostro_effects"),
            "DELETE FROM ostro_effects WHERE effect_key IN (SELECT effect_key FROM ostro_effects \
             WHERE created_at < now() - $1 * interval '1 second' LIMIT 1000)"
        );
        assert_eq!(
            gc_sql(Dialect::Mysql, "fx"),
            "DELETE FROM fx WHERE created_at < NOW(6) - INTERVAL ? SECOND LIMIT 1000"
        );
    }
}
