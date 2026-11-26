//! Phase 14: implemented in 14-06 (`TenantFlow`: per-tenant concurrency, memory, fuel debt
//! and suspension above the queue level of the node flow control, D-P14-8).

/// Per-tenant dispatch admission. Stub: admits everything.
#[derive(Debug, Default)]
pub struct TenantFlow {}

impl TenantFlow {
    /// An empty tenant flow.
    pub fn new() -> Self {
        Self {}
    }
}
