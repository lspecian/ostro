//! Member self-registration (12-10, D-P12-13): every node upserts its own
//! `Member{addr, cluster_addr, repl_addr}` record once meta is ready, and only when one of
//! those addresses changed (a restart with the same config writes nothing).

use ostro_meta::{MetaCommand, MetaError, MetaHandle, MetaKey, MetaRecord, MetaResult};

/// CAS races (another writer between our read and our Put) retried before giving up.
const UPSERT_ATTEMPTS: usize = 5;

/// Upsert `me` (keyed by `me.node_id`). An existing record keeps its `joined_at_ms` and
/// `ext`; nothing is proposed when `addr`, `cluster_addr` and `repl_addr` already match.
/// Returns whether a record was written.
pub async fn upsert_self(meta: &MetaHandle, me: ostro_meta::Member) -> Result<bool, MetaError> {
    for _ in 0..UPSERT_ATTEMPTS {
        let current = meta.view().read(|s| s.member(me.node_id).cloned());
        let (expected, record) = match current {
            Some(cur)
                if cur.value.addr == me.addr
                    && cur.value.cluster_addr == me.cluster_addr
                    && cur.value.repl_addr == me.repl_addr =>
            {
                return Ok(false);
            }
            Some(cur) => (
                Some(cur.version),
                ostro_meta::Member {
                    node_id: me.node_id,
                    addr: me.addr.clone(),
                    joined_at_ms: cur.value.joined_at_ms,
                    cluster_addr: me.cluster_addr.clone(),
                    repl_addr: me.repl_addr.clone(),
                    ext: cur.value.ext.clone(),
                },
            ),
            None => (None, me.clone()),
        };
        let cmd = MetaCommand::Put {
            key: MetaKey::Member(me.node_id),
            expected_version: expected,
            record: MetaRecord::Member(record),
        };
        match meta.propose(cmd).await? {
            MetaResult::Ok { .. } => {
                tracing::info!(node = me.node_id, addr = %me.addr, "member record updated");
                return Ok(true);
            }
            MetaResult::StaleVersion { .. }
            | MetaResult::Conflict { .. }
            | MetaResult::NotFound => {
                continue;
            }
            MetaResult::Invalid(msg) => return Err(MetaError::Rejected(msg)),
        }
    }
    Err(MetaError::Rejected(format!(
        "member {} record kept changing under the upsert",
        me.node_id
    )))
}
