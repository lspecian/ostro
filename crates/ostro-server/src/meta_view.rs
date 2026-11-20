//! [`MetaHandleView`]: the bridge's [`ostro_bridge::meta_view::MetaView`] over the live,
//! replicated meta state (06-14).
//!
//! Every call is one read of the current meta state through [`ostro_meta::MetaView::read`]
//! (a shared `RwLock` read plus a clone of the matching records). There is no cache, so a
//! grant, egress policy, connection or sealed secret written through the meta log is seen by
//! the very next effect. Cost per call:
//!
//! | method | cost |
//! |---|---|
//! | `sealed_secret`, `egress_policy`, `connection` | one `BTreeMap` point lookup + clone |
//! | `grants_for` | a scan of every grant record (filtered by subject) + clones |
//!
//! Grant counts are operator-sized (one per module x secret), so the scan stays cheap; the
//! HTTP connector calls `grants_for` once per templated request.

use ostro_bridge::meta_view::MetaView as BridgeMetaView;
use ostro_meta::records::{Connection, EgressPolicy, Grant, SealedSecret};
use ostro_meta::{MetaKey, MetaRecord, MetaView};

/// [`BridgeMetaView`] reading the live meta state (no cache; see the module docs).
#[derive(Clone)]
pub struct MetaHandleView {
    meta: MetaView,
}

impl std::fmt::Debug for MetaHandleView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetaHandleView").finish_non_exhaustive()
    }
}

impl MetaHandleView {
    /// A view over `meta` (a clone of the meta handle's view).
    pub fn new(meta: MetaView) -> Self {
        Self { meta }
    }

    fn record(&self, key: MetaKey) -> Option<MetaRecord> {
        self.meta.read(|s| s.get(&key)).map(|v| v.value)
    }
}

impl BridgeMetaView for MetaHandleView {
    fn sealed_secret(&self, name: &str) -> Option<SealedSecret> {
        match self.record(MetaKey::Secret(name.to_owned()))? {
            MetaRecord::Secret(s) => Some(s),
            _ => None,
        }
    }

    fn grants_for(&self, module: &str) -> Vec<Grant> {
        let subject = format!("module:{module}");
        self.meta.read(|s| {
            s.grants()
                .filter(|g| g.value.subject == subject)
                .map(|g| g.value.clone())
                .collect()
        })
    }

    fn egress_policy(&self, module: &str) -> Option<EgressPolicy> {
        match self.record(MetaKey::Egress(module.to_owned()))? {
            MetaRecord::Egress(p) => Some(p),
            _ => None,
        }
    }

    fn connection(&self, name: &str) -> Option<Connection> {
        match self.record(MetaKey::Connection(name.to_owned()))? {
            MetaRecord::Connection(c) => Some(c),
            _ => None,
        }
    }
}
