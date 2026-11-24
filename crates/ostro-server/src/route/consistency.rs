//! Phase 13: implemented in 13-13 (read-your-writes, D-P13-15): partition reads under a
//! leader lease (or a full barrier with `[api] strict_reads`), meta reads behind a raft
//! ReadIndex barrier, and the `ostro-consistency` tokens.
//!
//! Fixed signatures, called by the 13-12 router's local branch (direct and forwarded-in
//! calls). Stubs: every read is served, no token is produced.

use ostro_api::ApiError;
use ostro_types::PartitionId;

use crate::p13::P13Ctx;

/// Before a partition read on this node. Stub: Ok.
pub async fn before_partition_read(ctx: &P13Ctx, p: PartitionId) -> Result<(), ApiError> {
    let _ = (ctx, p);
    Ok(())
}

/// Before a meta read on this node. Stub: Ok.
pub async fn before_meta_read(ctx: &P13Ctx) -> Result<(), ApiError> {
    let _ = ctx;
    Ok(())
}

/// The consistency token after a partition write (`p<P>:<epoch>:<offset>`). Stub: none.
pub fn token_after_partition_write(ctx: &P13Ctx, p: PartitionId) -> Option<String> {
    let _ = (ctx, p);
    None
}

/// The consistency token after a meta write (`m:<index>`). Stub: none.
pub fn token_after_meta_write(ctx: &P13Ctx) -> Option<String> {
    let _ = ctx;
    None
}
