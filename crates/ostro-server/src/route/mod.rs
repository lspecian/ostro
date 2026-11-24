//! Phase 13 routing (API-08, D-P13-14): implemented in 13-12; the consistency hooks in
//! [`consistency`] in 13-13.
//!
//! The seam ([`crate::p13`]) calls three fixed entry points: [`wrap_control_plane`] at the
//! single control-plane construction site (every API, UI and in-process consumer gets the
//! result), [`wrap_extensions`] over the filled `ApiExtensions` slots and
//! [`node_api_services`] for the cluster listener. The start path reads the forwarded
//! partition pin through [`table::pinned_partition`]. Stubs: pass-through, no services.
//!
//! | file | contents (13-12) |
//! |------|------------------|
//! | [`table`] | route classes, the partition of a call, the honoured `ostro-partition` pin |
//! | [`peer`] | `PeerControlPlane` over the 13-03 gRPC client and the cluster transport |
//! | [`fanout`] | partition / node fan-out with complete-or-error merging |
//! | [`consistency`] | lease / barrier / token hooks (13-13) |

pub mod consistency;
pub mod fanout;
pub mod peer;
pub mod table;

use std::sync::Arc;

use ostro_api::{ApiExtensions, ControlPlane};
use tonic::service::Routes;

use crate::control::ServerControlPlane;
use crate::p13::P13Ctx;

/// The routed control plane over `local`. Stub: `local` itself.
pub fn wrap_control_plane(
    ctx: &Arc<P13Ctx>,
    local: Arc<ServerControlPlane>,
) -> Arc<dyn ControlPlane> {
    let _ = ctx;
    local
}

/// Route every filled extension slot (Phase 7/8/11/13 traits). Stub: unchanged.
pub fn wrap_extensions(ctx: &Arc<P13Ctx>, ext: &mut ApiExtensions) {
    let _ = (ctx, ext);
}

/// The `ostro.api.v1.*` services the cluster listener serves to node identities (forwarded
/// calls). Stub: none.
pub fn node_api_services(ctx: &Arc<P13Ctx>) -> Vec<Routes> {
    let _ = ctx;
    Vec::new()
}
