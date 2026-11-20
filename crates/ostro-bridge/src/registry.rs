//! `EffectRegistry`: the in-memory `(partition, run, op) -> task` map of in-flight effects
//! (06-07). The [`Bridge`](crate::driver::Bridge) inserts an entry before it spawns an effect
//! task and the task removes its own entry when it ends.
//!
//! Every method takes the mutex for a short, synchronous critical section; nothing here is held
//! across an `.await`.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use ostro_types::live::EffectKind;
use tokio_util::sync::CancellationToken;

/// The gauge of live registry entries.
pub const INFLIGHT_METRIC: &str = "ostro_effect_inflight";

/// Registry key: `(partition, run_id, op)`.
pub type EffectKey = (u16, String, u64);

/// One in-flight effect task.
#[derive(Clone, Debug)]
pub struct TaskEntry {
    /// Cancels the task (and is the `EffectCtx.cancel` the connector sees).
    pub cancel: CancellationToken,
    /// When the task was started.
    pub started_at: Instant,
    /// The effect family.
    pub kind: EffectKind,
    /// Generation id: a task removes only the entry it created.
    pub id: u64,
}

/// Outcome of [`EffectRegistry::try_insert`].
#[derive(Debug)]
pub enum Insert {
    /// A new entry: spawn the task with this id and token.
    New {
        /// Entry generation id.
        id: u64,
        /// The task's cancellation token.
        cancel: CancellationToken,
    },
    /// A live (not cancelled) task already runs for the key: attach, start nothing.
    Attached,
}

/// The in-flight effect map.
#[derive(Debug, Default)]
pub struct EffectRegistry {
    entries: Mutex<BTreeMap<EffectKey, TaskEntry>>,
    next_id: AtomicU64,
}

impl EffectRegistry {
    /// An empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<EffectKey, TaskEntry>> {
        self.entries.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn publish(len: usize) {
        metrics::gauge!(INFLIGHT_METRIC).set(len as f64);
    }

    /// Inserts an entry for `key` unless a live one exists. A cancelled entry whose task has not
    /// exited yet is replaced (a re-drive after a cancel must run again).
    pub fn try_insert(&self, key: EffectKey, kind: EffectKind) -> Insert {
        let mut map = self.lock();
        if let Some(e) = map.get(&key)
            && !e.cancel.is_cancelled()
        {
            return Insert::Attached;
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let cancel = CancellationToken::new();
        map.insert(
            key,
            TaskEntry {
                cancel: cancel.clone(),
                started_at: Instant::now(),
                kind,
                id,
            },
        );
        Self::publish(map.len());
        Insert::New { id, cancel }
    }

    /// Removes the entry of `key` if it is still generation `id`. Returns whether it did.
    pub fn remove(&self, key: &EffectKey, id: u64) -> bool {
        let mut map = self.lock();
        let removed = match map.get(key) {
            Some(e) if e.id == id => map.remove(key).is_some(),
            _ => false,
        };
        Self::publish(map.len());
        removed
    }

    /// The entry of `key`, if any.
    pub fn get(&self, partition: u16, run_id: &str, op: u64) -> Option<TaskEntry> {
        self.lock()
            .get(&(partition, run_id.to_owned(), op))
            .cloned()
    }

    /// Cancels the task of `(partition, run_id, op)`. Returns whether one was found.
    pub fn cancel(&self, partition: u16, run_id: &str, op: u64) -> bool {
        match self.lock().get(&(partition, run_id.to_owned(), op)) {
            Some(e) => {
                e.cancel.cancel();
                true
            }
            None => false,
        }
    }

    /// Cancels every task of `run_id` on `partition`. Returns how many were found.
    pub fn cancel_run(&self, partition: u16, run_id: &str) -> usize {
        let lo = (partition, run_id.to_owned(), 0);
        let hi = (partition, run_id.to_owned(), u64::MAX);
        let map = self.lock();
        let mut n = 0;
        for e in map.range(lo..=hi).map(|(_, e)| e) {
            e.cancel.cancel();
            n += 1;
        }
        n
    }

    /// Cancels every task.
    pub fn cancel_all(&self) -> usize {
        let map = self.lock();
        for e in map.values() {
            e.cancel.cancel();
        }
        map.len()
    }

    /// Number of live entries.
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// Whether no entry is live.
    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(run: &str, op: u64) -> EffectKey {
        (1, run.to_owned(), op)
    }

    #[test]
    fn registry_attach_replace_and_generation_remove() {
        let r = EffectRegistry::new();
        let Insert::New { id: a, cancel } = r.try_insert(key("r", 1), EffectKind::Http) else {
            panic!("new expected")
        };
        assert!(matches!(
            r.try_insert(key("r", 1), EffectKind::Http),
            Insert::Attached
        ));
        // A cancelled entry is replaced; the old task's remove is then a no-op.
        cancel.cancel();
        let Insert::New { id: b, .. } = r.try_insert(key("r", 1), EffectKind::Http) else {
            panic!("replacement expected")
        };
        assert_ne!(a, b);
        assert!(!r.remove(&key("r", 1), a));
        assert_eq!(r.len(), 1);
        assert!(r.remove(&key("r", 1), b));
        assert!(r.is_empty());
    }

    #[test]
    fn registry_cancel_run_is_scoped() {
        let r = EffectRegistry::new();
        for (run, op) in [("r", 1), ("r", 2), ("r2", 1), ("q", 9)] {
            r.try_insert(key(run, op), EffectKind::Sql);
        }
        r.try_insert((2, "r".into(), 1), EffectKind::Sql);
        assert_eq!(r.cancel_run(1, "r"), 2);
        assert!(r.get(1, "r", 1).unwrap().cancel.is_cancelled());
        assert!(r.get(1, "r", 2).unwrap().cancel.is_cancelled());
        assert!(!r.get(1, "r2", 1).unwrap().cancel.is_cancelled());
        assert!(!r.get(2, "r", 1).unwrap().cancel.is_cancelled());
        assert!(r.cancel(1, "q", 9));
        assert!(!r.cancel(1, "q", 10));
        assert_eq!(r.cancel_all(), 5);
    }
}
