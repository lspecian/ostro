//! Read-only view of the meta records the bridge needs (06-02, P-3). The server implements it
//! over the replicated meta state (06-14); tests use [`StaticMetaView`].
//!
//! Module names are tenant-qualified deployed names `<tenant>/<name>` (D23). A grant's subject
//! is `module:<module>`; an egress policy is keyed by the module name.

use std::collections::BTreeMap;

use ostro_meta::records::{Connection, EgressPolicy, Grant, SealedSecret};

/// Meta records used by egress, secrets and connectors.
pub trait MetaView: Send + Sync {
    /// The sealed secret named `name` (`<tenant|_>/<name>`).
    fn sealed_secret(&self, name: &str) -> Option<SealedSecret>;
    /// Every grant whose subject is `module:<module>`.
    fn grants_for(&self, module: &str) -> Vec<Grant>;
    /// The egress policy of `module`; `None` = no egress.
    fn egress_policy(&self, module: &str) -> Option<EgressPolicy>;
    /// The connection named `name`.
    fn connection(&self, name: &str) -> Option<Connection>;
}

/// `BTreeMap`-backed [`MetaView`] for tests.
#[derive(Clone, Debug, Default)]
pub struct StaticMetaView {
    secrets: BTreeMap<String, SealedSecret>,
    grants: BTreeMap<String, Grant>,
    egress: BTreeMap<String, EgressPolicy>,
    connections: BTreeMap<String, Connection>,
}

impl StaticMetaView {
    /// An empty view.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds (or replaces) a sealed secret, keyed by its name.
    pub fn insert_sealed_secret(mut self, s: SealedSecret) -> Self {
        self.secrets.insert(s.name.clone(), s);
        self
    }

    /// Adds (or replaces) a grant, keyed by its id.
    pub fn insert_grant(mut self, g: Grant) -> Self {
        self.grants.insert(g.id.clone(), g);
        self
    }

    /// Adds (or replaces) an egress policy, keyed by its module name.
    pub fn insert_egress_policy(mut self, p: EgressPolicy) -> Self {
        self.egress.insert(p.name.clone(), p);
        self
    }

    /// Adds (or replaces) a connection, keyed by its name.
    pub fn insert_connection(mut self, c: Connection) -> Self {
        self.connections.insert(c.name.clone(), c);
        self
    }
}

impl MetaView for StaticMetaView {
    fn sealed_secret(&self, name: &str) -> Option<SealedSecret> {
        self.secrets.get(name).cloned()
    }

    fn grants_for(&self, module: &str) -> Vec<Grant> {
        let subject = format!("module:{module}");
        self.grants
            .values()
            .filter(|g| g.subject == subject)
            .cloned()
            .collect()
    }

    fn egress_policy(&self, module: &str) -> Option<EgressPolicy> {
        self.egress.get(module).cloned()
    }

    fn connection(&self, name: &str) -> Option<Connection> {
        self.connections.get(name).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn static_view_lookups() {
        let v = StaticMetaView::new()
            .insert_grant(Grant {
                id: "secret:t/a:k".into(),
                subject: "module:t/a".into(),
                scopes: vec!["origin:https://api.example.com".into()],
                ext: vec![],
            })
            .insert_grant(Grant {
                id: "secret:t/b:k".into(),
                subject: "module:t/b".into(),
                scopes: vec![],
                ext: vec![],
            })
            .insert_egress_policy(EgressPolicy {
                name: "t/a".into(),
                allow: vec!["https://api.example.com".into()],
                ext: vec![],
            })
            .insert_connection(Connection {
                name: "db".into(),
                kind: "sql-postgres".into(),
                config: b"{\"v\":1}".to_vec(),
                ext: vec![],
            })
            .insert_sealed_secret(SealedSecret {
                name: "t/k".into(),
                ciphertext: vec![1],
                key_id: "plaintext-v0".into(),
                ext: vec![],
            });
        let view: &dyn MetaView = &v;
        assert_eq!(view.grants_for("t/a").len(), 1);
        assert_eq!(view.grants_for("t/a")[0].id, "secret:t/a:k");
        assert!(view.grants_for("t/c").is_empty());
        assert!(view.egress_policy("t/a").is_some());
        assert!(view.egress_policy("t/b").is_none());
        assert_eq!(view.connection("db").unwrap().kind, "sql-postgres");
        assert!(view.connection("nope").is_none());
        assert!(view.sealed_secret("t/k").is_some());
    }
}
