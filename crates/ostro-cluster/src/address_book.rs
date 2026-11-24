//! Node id → cluster / replication address (12-05).
//!
//! Seeded from `[[cluster.peers]]` (empty for a single node); `upsert` / `upsert_member`
//! let 12-10 / Phase 13 add addresses learned from meta `Member` records. Reads are cheap
//! clones under a read lock; [`crate::transport::tcp::TcpTransport`] re-dials a node whose
//! address changed.

use std::collections::BTreeMap;
use std::sync::RwLock;

use ostro_meta::{Member, MetaView};

/// The addresses of one node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerAddr {
    /// `host:port` of the node's cluster listener (MetaPeer, Replication).
    pub cluster_addr: String,
    /// `host:port` of a separate replication listener, when the node has one.
    pub repl_addr: Option<String>,
}

/// One `[[cluster.peers]]` entry (ostro-server's `PeerConfig`, without the dependency).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerEntry {
    /// Node id (non-zero).
    pub id: u64,
    /// Cluster listener `host:port`.
    pub addr: String,
    /// Replication listener `host:port`, if separate.
    pub repl_addr: Option<String>,
}

/// One joiner seed (`<data>/cluster-seeds.toml`, D-P13-9/29): same shape as a static peer.
pub type Seed = PeerEntry;

/// Node id → addresses.
#[derive(Debug, Default)]
pub struct AddressBook {
    inner: RwLock<BTreeMap<u64, PeerAddr>>,
}

impl AddressBook {
    /// An empty book (single node).
    pub fn new() -> Self {
        Self::default()
    }

    /// A book holding the static peers.
    pub fn from_peers(peers: impl IntoIterator<Item = PeerEntry>) -> Self {
        let book = Self::new();
        for p in peers {
            book.upsert(p.id, p.addr, p.repl_addr);
        }
        book
    }

    /// Phase 13 (13-01 constructor stub; 13-09 fills it from the meta `Member` records): a
    /// book seeded from `seeds` that will follow `view`. Until 13-09 it delegates to the
    /// Phase 12 static book ([`AddressBook::from_peers`]) and ignores `view`.
    pub fn from_meta(view: MetaView, seeds: Vec<Seed>) -> Self {
        let _ = view;
        Self::from_peers(seeds)
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, BTreeMap<u64, PeerAddr>> {
        self.inner.read().unwrap_or_else(|e| e.into_inner())
    }

    /// The addresses of `id`.
    pub fn get(&self, id: u64) -> Option<PeerAddr> {
        self.read().get(&id).cloned()
    }

    /// The cluster listener address of `id`.
    pub fn cluster_addr(&self, id: u64) -> Option<String> {
        self.read().get(&id).map(|a| a.cluster_addr.clone())
    }

    /// The replication address of `id`: its `repl_addr`, else its cluster address.
    pub fn repl_addr(&self, id: u64) -> Option<String> {
        self.read().get(&id).map(|a| {
            a.repl_addr
                .clone()
                .unwrap_or_else(|| a.cluster_addr.clone())
        })
    }

    /// Every known node id, ascending.
    pub fn ids(&self) -> Vec<u64> {
        self.read().keys().copied().collect()
    }

    /// Number of known nodes.
    pub fn len(&self) -> usize {
        self.read().len()
    }

    /// No node is known (single node).
    pub fn is_empty(&self) -> bool {
        self.read().is_empty()
    }

    /// Insert or replace the addresses of `id`; `true` when something changed. Id 0 and an
    /// empty address are ignored.
    pub fn upsert(&self, id: u64, cluster_addr: String, repl_addr: Option<String>) -> bool {
        if id == 0 || cluster_addr.is_empty() {
            return false;
        }
        let new = PeerAddr {
            cluster_addr,
            repl_addr: repl_addr.filter(|a| !a.is_empty()),
        };
        let mut map = self.inner.write().unwrap_or_else(|e| e.into_inner());
        if map.get(&id) == Some(&new) {
            return false;
        }
        map.insert(id, new);
        true
    }

    /// Upsert from a meta `Member` record (its `cluster_addr`; members without one are
    /// skipped). `true` when something changed.
    pub fn upsert_member(&self, m: &Member) -> bool {
        match &m.cluster_addr {
            Some(addr) => self.upsert(m.node_id, addr.clone(), m.repl_addr.clone()),
            None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peers_members_and_fallbacks() {
        let book = AddressBook::from_peers([
            PeerEntry {
                id: 2,
                addr: "10.0.0.2:7001".into(),
                repl_addr: None,
            },
            PeerEntry {
                id: 3,
                addr: "10.0.0.3:7001".into(),
                repl_addr: Some("10.0.0.3:7002".into()),
            },
        ]);
        assert_eq!(book.ids(), [2, 3]);
        assert_eq!(book.repl_addr(2).as_deref(), Some("10.0.0.2:7001"));
        assert_eq!(book.repl_addr(3).as_deref(), Some("10.0.0.3:7002"));
        assert_eq!(book.cluster_addr(4), None);
        assert!(!book.upsert(2, "10.0.0.2:7001".into(), None), "unchanged");
        assert!(!book.upsert(0, "x:1".into(), None));
        assert!(!book.upsert(5, String::new(), None));
        let m = Member {
            node_id: 4,
            addr: "api".into(),
            joined_at_ms: 0,
            cluster_addr: Some("10.0.0.4:7001".into()),
            repl_addr: None,
            ext: Vec::new(),
        };
        assert!(book.upsert_member(&m));
        assert_eq!(book.cluster_addr(4).as_deref(), Some("10.0.0.4:7001"));
        assert!(!book.upsert_member(&Member {
            cluster_addr: None,
            ..m
        }));
        assert_eq!(book.len(), 3);
        assert!(AddressBook::new().is_empty());
    }
}
