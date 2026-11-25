//! Node id → cluster / replication address (12-05; dynamic since 13-09, D-P13-8/9).
//!
//! Seeded from `[[cluster.peers]]` (empty for a single node) and, on a joiner, from the join
//! marker's seeds. Since 13-09 the book follows the meta `Member` records
//! ([`AddressBook::sync_members`], [`AddressBook::follow`]): a node's `cluster_addr` /
//! `repl_addr` in meta replaces its seed address and later changes apply live.
//!
//! **Configured peers are pinned** ([`AddressBook::from_peers`]): an id listed in
//! `[[cluster.peers]]` keeps its configured address even when its Member record names
//! another one. The configured address is the operator's route from *this* node (a proxy, a
//! NAT address, the tier-C chaos proxy links); a difference is logged once per id and never
//! applied. Ids that are not configured (joined nodes, seeds) follow meta.
//!
//! Reads are cheap clones under a read lock; [`crate::transport::tcp::TcpTransport`] re-dials
//! a node whose address changed.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, RwLock};

use ostro_meta::{Member, MetaState, MetaView};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

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
    /// Ids from `[[cluster.peers]]`: their configured address wins over meta.
    pinned: RwLock<BTreeSet<u64>>,
    /// Pinned ids whose meta address differs (logged once each).
    drift_logged: RwLock<BTreeSet<u64>>,
}

impl AddressBook {
    /// An empty book (single node).
    pub fn new() -> Self {
        Self::default()
    }

    /// A book holding the static peers (pinned: their configured address wins over meta).
    pub fn from_peers(peers: impl IntoIterator<Item = PeerEntry>) -> Self {
        let book = Self::new();
        book.add_pinned(peers);
        book
    }

    /// Add configured peers (pinned) to this book.
    pub fn add_pinned(&self, peers: impl IntoIterator<Item = PeerEntry>) {
        for p in peers {
            if self.upsert(p.id, p.addr, p.repl_addr) || self.get(p.id).is_some() {
                self.pinned
                    .write()
                    .unwrap_or_else(|e| e.into_inner())
                    .insert(p.id);
            }
        }
    }

    /// Add seeds (not pinned: a Member record replaces them).
    pub fn add_seeds(&self, seeds: impl IntoIterator<Item = Seed>) {
        for s in seeds {
            if !self.is_pinned(s.id) {
                self.upsert(s.id, s.addr, s.repl_addr);
            }
        }
    }

    /// A book seeded from `seeds` (not pinned) that takes every Member address already in
    /// `view`. Call [`AddressBook::follow`] to keep it current.
    pub fn from_meta(view: MetaView, seeds: Vec<Seed>) -> Self {
        let book = Self::new();
        book.add_seeds(seeds);
        view.read(|s| book.sync_members(s));
        book
    }

    /// Whether `id` is a configured (pinned) peer.
    pub fn is_pinned(&self, id: u64) -> bool {
        self.pinned
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .contains(&id)
    }

    /// Apply every Member record of `state` (except `skip`, normally this node): unpinned
    /// ids take the meta address; a pinned id whose meta address differs is logged once.
    /// Returns the ids whose address changed.
    pub fn sync_members_except(&self, state: &MetaState, skip: Option<u64>) -> Vec<u64> {
        let mut changed = Vec::new();
        for m in state.members() {
            let m = &m.value;
            if Some(m.node_id) == skip {
                continue;
            }
            let Some(addr) = m.cluster_addr.as_deref().filter(|a| !a.is_empty()) else {
                continue;
            };
            if self.is_pinned(m.node_id) {
                let cur = self.get(m.node_id);
                let differs = cur.as_ref().is_some_and(|c| {
                    c.cluster_addr != addr || c.repl_addr.as_deref() != m.repl_addr.as_deref()
                });
                if differs {
                    let first = self
                        .drift_logged
                        .write()
                        .unwrap_or_else(|e| e.into_inner())
                        .insert(m.node_id);
                    if first {
                        tracing::warn!(
                            node = m.node_id,
                            configured = %cur.map(|c| c.cluster_addr).unwrap_or_default(),
                            meta = %addr,
                            "[[cluster.peers]] address differs from the node's Member record; \
                             keeping the configured address (logged once)"
                        );
                    }
                }
                continue;
            }
            if self.upsert_member(m) {
                changed.push(m.node_id);
            }
        }
        changed
    }

    /// [`AddressBook::sync_members_except`] without a skipped id.
    pub fn sync_members(&self, state: &MetaState) -> Vec<u64> {
        self.sync_members_except(state, None)
    }

    /// Follow `view`: re-sync on every applied meta change until `stop` (`skip` = this
    /// node, whose own record never feeds its book).
    pub fn follow(
        self: &Arc<Self>,
        view: MetaView,
        skip: Option<u64>,
        stop: CancellationToken,
    ) -> JoinHandle<()> {
        let book = Arc::clone(self);
        tokio::spawn(async move {
            let mut applied = view.subscribe();
            loop {
                let changed = view.read(|s| book.sync_members_except(s, skip));
                if !changed.is_empty() {
                    tracing::info!(nodes = ?changed, "address book updated from meta");
                }
                tokio::select! {
                    _ = stop.cancelled() => return,
                    r = applied.changed() => if r.is_err() { return },
                }
            }
        })
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
