//! Member self-registration (12-10, D-P12-13): every node upserts its own
//! `Member{addr, cluster_addr, repl_addr}` record once meta is ready, and only when one of
//! those addresses changed (a restart with the same config writes nothing).
//!
//! 13-09 (D-P13-8, plan check W10): the upsert is a read-modify-write of the current record
//! under CAS. Every `ext` entry (NodeVersion, MemberState and any tag this binary does not
//! know) is kept byte for byte, and a record whose MemberState is Removed is never written:
//! [`upsert_self`] answers [`MetaError::Rejected`] naming the tombstone (the boot path's
//! PostReady voter check refuses to start such a node before it gets here).

use ostro_meta::ext::{MEMBER_REMOVED, get_member_state};
use ostro_meta::{MetaCommand, MetaError, MetaHandle, MetaKey, MetaRecord, MetaResult};

/// Whether `m` carries the Removed tombstone (MemberState ext 97).
pub fn is_removed(m: &ostro_meta::Member) -> bool {
    matches!(get_member_state(&m.ext), Ok(Some(s)) if s.state == MEMBER_REMOVED)
}

/// The refusal for a tombstoned node.
pub fn removed_error(node_id: u64) -> MetaError {
    MetaError::Rejected(format!(
        "node {node_id} was removed from the cluster (tombstone); node ids are never reused: \
         wipe the data dir and join again as a new node"
    ))
}

/// CAS races (another writer between our read and our Put) retried before giving up.
const UPSERT_ATTEMPTS: usize = 5;

/// Upsert `me` (keyed by `me.node_id`). An existing record keeps its `joined_at_ms` and
/// `ext`; nothing is proposed when `addr`, `cluster_addr` and `repl_addr` already match.
/// Returns whether a record was written.
pub async fn upsert_self(meta: &MetaHandle, me: ostro_meta::Member) -> Result<bool, MetaError> {
    for _ in 0..UPSERT_ATTEMPTS {
        let current = meta.view().read(|s| s.member(me.node_id).cloned());
        if current.as_ref().is_some_and(|c| is_removed(&c.value)) {
            return Err(removed_error(me.node_id));
        }
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
