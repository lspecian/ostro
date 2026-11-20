//! A small TTL cache for the remote providers (Vault, Kubernetes), owned by 06-13.
//!
//! - Values are stored as [`Zeroizing<Vec<u8>>`]: an entry that expires, is replaced, is
//!   cleared or is dropped with the cache has its bytes zeroized.
//! - [`TtlCache::get`] returns a fresh [`SecretBox`] copy of a live entry. An expired entry is
//!   removed (and zeroized) on lookup. Expired entries are also swept on every insert, so the
//!   cache stays bounded by the working set of names; on top of that, at most
//!   [`MAX_ENTRIES`] values are held (inserts beyond it are skipped, never an error).
//! - A zero TTL disables the cache.
//! - Only found values are cached; misses and errors always go to the backend.
//!
//! The clock is `std::time::Instant`. Tests drive it through the crate-private `*_at`
//! methods (tokio's paused clock needs the `test-util` feature, which this crate does not
//! enable).

use std::collections::HashMap;
use std::fmt;
use std::hash::Hash;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use secrecy::SecretBox;
use zeroize::Zeroizing;

/// Upper bound on cached values per cache.
pub const MAX_ENTRIES: usize = 4096;

struct Entry {
    value: Zeroizing<Vec<u8>>,
    expires: Instant,
}

/// A name -> secret-bytes cache with a fixed TTL.
pub struct TtlCache<K> {
    ttl: Duration,
    map: Mutex<HashMap<K, Entry>>,
}

impl<K> fmt::Debug for TtlCache<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let n = self.map.lock().map(|m| m.len()).unwrap_or(0);
        f.debug_struct("TtlCache")
            .field("ttl", &self.ttl)
            .field("entries", &n)
            .finish()
    }
}

impl<K: Eq + Hash + Clone> TtlCache<K> {
    /// An empty cache whose entries live for `ttl` (zero disables caching).
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            map: Mutex::new(HashMap::new()),
        }
    }

    /// The configured TTL.
    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    /// A copy of the live value for `key`, if any.
    pub fn get(&self, key: &K) -> Option<SecretBox<[u8]>> {
        self.get_at(key, Instant::now())
    }

    /// Caches `value` for `key` (replacing and zeroizing any previous value).
    pub fn insert(&self, key: K, value: Zeroizing<Vec<u8>>) {
        self.insert_at(key, value, Instant::now());
    }

    /// Removes (and zeroizes) every entry.
    pub fn clear(&self) {
        self.lock().clear();
    }

    /// Removes (and zeroizes) the entry for `key`.
    pub fn remove(&self, key: &K) {
        self.lock().remove(key);
    }

    /// Sweeps (and zeroizes) expired entries; returns how many were removed.
    pub fn evict_expired(&self) -> usize {
        self.evict_expired_at(Instant::now())
    }

    /// Number of stored entries (live or not yet swept).
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// Whether no entries are stored.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<K, Entry>> {
        // A panic while holding the lock cannot leave an entry half-written (every update
        // is a single map operation), so a poisoned map is still consistent.
        self.map.lock().unwrap_or_else(|p| p.into_inner())
    }

    pub(crate) fn get_at(&self, key: &K, now: Instant) -> Option<SecretBox<[u8]>> {
        let mut map = self.lock();
        match map.get(key) {
            Some(e) if now < e.expires => Some(SecretBox::new(e.value.as_slice().into())),
            Some(_) => {
                map.remove(key);
                None
            }
            None => None,
        }
    }

    pub(crate) fn insert_at(&self, key: K, value: Zeroizing<Vec<u8>>, now: Instant) {
        if self.ttl.is_zero() {
            return;
        }
        let Some(expires) = now.checked_add(self.ttl) else {
            return;
        };
        let mut map = self.lock();
        map.retain(|_, e| now < e.expires);
        if map.len() >= MAX_ENTRIES && !map.contains_key(&key) {
            return;
        }
        map.insert(key, Entry { value, expires });
    }

    pub(crate) fn evict_expired_at(&self, now: Instant) -> usize {
        let mut map = self.lock();
        let before = map.len();
        map.retain(|_, e| now < e.expires);
        before - map.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use secrecy::ExposeSecret;

    fn z(b: &[u8]) -> Zeroizing<Vec<u8>> {
        Zeroizing::new(b.to_vec())
    }

    #[test]
    fn hit_until_ttl_then_expired_and_removed() {
        let c = TtlCache::<String>::new(Duration::from_secs(30));
        let t0 = Instant::now();
        assert!(c.get_at(&"k".into(), t0).is_none());
        c.insert_at("k".into(), z(b"cached-secret-1"), t0);
        let v = c.get_at(&"k".into(), t0 + Duration::from_secs(29)).unwrap();
        assert_eq!(v.expose_secret(), b"cached-secret-1");
        assert!(
            c.get_at(&"k".into(), t0 + Duration::from_secs(30))
                .is_none()
        );
        assert!(
            c.is_empty(),
            "the expired entry is dropped (and zeroized) on lookup"
        );
    }

    #[test]
    fn insert_replaces_and_sweeps_expired() {
        let c = TtlCache::<String>::new(Duration::from_secs(10));
        let t0 = Instant::now();
        c.insert_at("a".into(), z(b"value-a-1"), t0);
        c.insert_at("a".into(), z(b"value-a-2"), t0);
        assert_eq!(
            c.get_at(&"a".into(), t0).unwrap().expose_secret(),
            b"value-a-2"
        );
        c.insert_at("b".into(), z(b"value-b"), t0 + Duration::from_secs(5));
        assert_eq!(c.len(), 2);
        // At t0+11 "a" is expired; inserting "c" sweeps it.
        c.insert_at("c".into(), z(b"value-c"), t0 + Duration::from_secs(11));
        assert_eq!(c.len(), 2);
        assert!(
            c.get_at(&"a".into(), t0 + Duration::from_secs(11))
                .is_none()
        );
        assert_eq!(c.evict_expired_at(t0 + Duration::from_secs(16)), 1);
        assert_eq!(c.len(), 1);
        c.clear();
        assert!(c.is_empty());
    }

    #[test]
    fn zero_ttl_disables_and_cap_is_enforced() {
        let off = TtlCache::<String>::new(Duration::ZERO);
        off.insert("k".into(), z(b"never-cached"));
        assert!(off.get(&"k".into()).is_none());

        let c = TtlCache::<usize>::new(Duration::from_secs(60));
        let t0 = Instant::now();
        for i in 0..MAX_ENTRIES + 10 {
            c.insert_at(i, z(b"v"), t0);
        }
        assert_eq!(c.len(), MAX_ENTRIES);
        // Replacing an existing key still works at the cap.
        c.insert_at(0, z(b"w"), t0);
        assert_eq!(c.get_at(&0, t0).unwrap().expose_secret(), b"w");
        c.remove(&0);
        assert!(c.get_at(&0, t0).is_none());
    }

    #[test]
    fn debug_shows_no_values() {
        let c = TtlCache::<String>::new(Duration::from_secs(30));
        c.insert("k".into(), z(b"canary-cache-value"));
        let d = format!("{c:?}");
        assert!(!d.contains("canary"), "{d}");
        assert!(d.contains("entries: 1"), "{d}");
    }
}
