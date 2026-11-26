//! Phase 14: implemented in 14-10 (fault knobs: the failpoints-only SkewClock, the runtime
//! failpoint endpoint).

use std::sync::Arc;

use ostro_partition::Clock;

/// Stub: identity.
pub fn wrap_clock(clock: Arc<dyn Clock>) -> Arc<dyn Clock> {
    clock
}
