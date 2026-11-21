//! Phase 7: implemented in 07-10.
//!
//! `acme` mode over instant-acme (D-W6): account, order, HTTP-01 / TLS-ALPN-01
//! challenge, finalize, cache under `[api.tls.acme] cache_dir`, renewal at 2/3 of the
//! lifetime scheduled from the injected clock.
//!
//! Contract fixed by 07-06 (the listener calls it in `acme` mode, inside the runtime,
//! before serving): validate `[api.tls.acme]`, install a cached certificate if any, and
//! return the task that orders / renews certificates (it installs them with
//! [`ReloadableResolver::swap`](super::ReloadableResolver::swap) and TLS-ALPN-01
//! challenge certs with `set_challenge`) and stops on `shutdown`. The listener serves
//! while that task runs, so TLS-ALPN-01 validation reaches it.

use std::sync::Arc;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::{TlsError, TlsState};

/// Start ACME for the client listener (stub until 07-10: always an error).
pub fn start(
    state: Arc<TlsState>,
    shutdown: CancellationToken,
) -> Result<JoinHandle<()>, TlsError> {
    let _ = (state, shutdown);
    Err(TlsError::Config(
        "[api.tls] mode = acme: ACME lands in 07-10".to_owned(),
    ))
}
