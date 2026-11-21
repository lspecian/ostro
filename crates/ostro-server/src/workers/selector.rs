//! Queue placement and label selectors (plan 07-12, WORK-04).
//!
//! `QueuePolicy.placement` (`embedded` | `workers` | `auto`, default `[workers]
//! default_placement`) and `QueuePolicy.selector` (Kubernetes-style label selector entries,
//! ANDed; parsed by [`ostro_api::phase7::Selector`], 07-11) decide where a queue's runs may
//! execute. [`PlacementPolicies`] compiles them once per queue and meta version: the cache
//! is dropped whenever the meta applied index moves, so a policy edit takes effect on the
//! next reservation without polling.
//!
//! A stored selector that does not parse (meta validates lengths only) matches **no**
//! worker: such a queue never reaches a worker it was not meant for (a warning is logged
//! once per compile).

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, PoisonError};

use ostro_api::phase7::Selector;
use ostro_meta::records::{PLACEMENT_AUTO, PLACEMENT_EMBEDDED, PLACEMENT_WORKERS};
use ostro_meta::{MetaView, QueuePolicy};
use tracing::warn;

use crate::config::PlacementSetting;

/// The compiled placement of one queue.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueuePlacement {
    pub placement: PlacementSetting,
    /// `None` = the stored selector does not parse (matches nothing).
    pub selector: Option<Selector>,
}

impl QueuePlacement {
    /// `placement` with an empty selector (every worker matches).
    pub fn new(placement: PlacementSetting) -> Self {
        Self {
            placement,
            selector: Some(Selector::default()),
        }
    }

    /// Compile a queue's policy (`None` = no policy) over the node default.
    pub fn from_policy(p: Option<&QueuePolicy>, default: PlacementSetting) -> Self {
        let Some(p) = p else {
            return Self::new(default);
        };
        let placement = match p.placement {
            Some(PLACEMENT_EMBEDDED) => PlacementSetting::Embedded,
            Some(PLACEMENT_WORKERS) => PlacementSetting::Workers,
            Some(PLACEMENT_AUTO) => PlacementSetting::Auto,
            _ => default,
        };
        let selector = match compile(&p.selector) {
            Ok(s) => Some(s),
            Err(e) => {
                warn!(queue = %p.queue, error = %e, "queue selector does not parse; no worker matches it");
                None
            }
        };
        Self {
            placement,
            selector,
        }
    }

    /// Whether a worker with `labels` may run this queue's work.
    pub fn matches(&self, labels: &BTreeMap<String, String>) -> bool {
        self.selector.as_ref().is_some_and(|s| matches(s, labels))
    }
}

/// Compile selector entries (ANDed); the empty list matches every worker.
pub fn compile<S: AsRef<str>>(entries: &[S]) -> Result<Selector, String> {
    Selector::from_entries(entries).map_err(|e| e.to_string())
}

/// Whether `labels` satisfy `selector`.
pub fn matches(selector: &Selector, labels: &BTreeMap<String, String>) -> bool {
    selector.matches(labels)
}

/// Where a queue's placement comes from (meta in production, a fake in unit tests).
pub trait PolicySource: Send + Sync + 'static {
    fn queue(&self, queue: &str) -> Arc<QueuePlacement>;
}

struct Cache {
    applied: u64,
    queues: HashMap<String, Arc<QueuePlacement>>,
}

/// Per-queue compiled placements over the meta view, cached per meta applied index.
pub struct PlacementPolicies {
    meta: MetaView,
    default: PlacementSetting,
    cache: Mutex<Cache>,
}

impl std::fmt::Debug for PlacementPolicies {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PlacementPolicies")
            .field("default", &self.default)
            .finish_non_exhaustive()
    }
}

impl PlacementPolicies {
    pub fn new(meta: MetaView, default: PlacementSetting) -> Self {
        Self {
            meta,
            default,
            cache: Mutex::new(Cache {
                applied: 0,
                queues: HashMap::new(),
            }),
        }
    }
}

impl PolicySource for PlacementPolicies {
    fn queue(&self, queue: &str) -> Arc<QueuePlacement> {
        let applied = self.meta.applied();
        let mut c = self.cache.lock().unwrap_or_else(PoisonError::into_inner);
        if c.applied != applied {
            c.applied = applied;
            c.queues.clear();
        }
        if let Some(p) = c.queues.get(queue) {
            return Arc::clone(p);
        }
        let p = Arc::new(self.meta.read(|s| {
            QueuePlacement::from_policy(s.queue_policy(queue).map(|v| &v.value), self.default)
        }));
        c.queues.insert(queue.to_owned(), Arc::clone(&p));
        p
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn labels(kv: &[(&str, &str)]) -> BTreeMap<String, String> {
        kv.iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    fn m(entries: &[&str], l: &BTreeMap<String, String>) -> bool {
        matches(&compile(entries).expect("selector parses"), l)
    }

    #[test]
    fn selector_equality() {
        let gpu = labels(&[("gpu", "true"), ("zone", "a")]);
        assert!(m(&["gpu=true"], &gpu));
        assert!(m(&["gpu==true"], &gpu));
        assert!(!m(&["gpu=false"], &gpu));
        assert!(!m(&["gpu=true"], &labels(&[("zone", "a")])));
    }

    #[test]
    fn selector_inequality() {
        let a = labels(&[("zone", "a")]);
        assert!(!m(&["zone!=a"], &a));
        assert!(m(&["zone!=b"], &a));
        // Kubernetes semantics: a missing key satisfies `!=`.
        assert!(m(&["gpu!=true"], &a));
    }

    #[test]
    fn selector_set_based() {
        let a = labels(&[("zone", "a")]);
        assert!(m(&["zone in (a,b)"], &a));
        assert!(!m(&["zone in (b,c)"], &a));
        assert!(m(&["zone notin (b,c)"], &a));
        assert!(!m(&["zone notin (a)"], &a));
        assert!(m(&["region notin (eu)"], &a));
    }

    #[test]
    fn selector_existence_and_negated_existence() {
        let a = labels(&[("gpu", "true")]);
        assert!(m(&["gpu"], &a));
        assert!(!m(&["tpu"], &a));
        assert!(m(&["!tpu"], &a));
        assert!(!m(&["!gpu"], &a));
    }

    #[test]
    fn selector_and_of_several() {
        let a = labels(&[("gpu", "true"), ("zone", "a")]);
        assert!(m(&["gpu=true", "zone=a"], &a));
        assert!(m(&["gpu=true,zone in (a,b)"], &a));
        assert!(!m(&["gpu=true", "zone=b"], &a));
        assert!(!m(&["gpu=true,!zone"], &a));
    }

    #[test]
    fn selector_empty_matches_all() {
        let none: [&str; 0] = [];
        assert!(m(&none, &labels(&[])));
        assert!(m(&none, &labels(&[("x", "y")])));
        assert!(QueuePlacement::new(PlacementSetting::Workers).matches(&labels(&[])));
    }

    #[test]
    fn policy_compiles_placement_and_selector() {
        let mut p = QueuePolicy {
            queue: "q".into(),
            ..QueuePolicy::default()
        };
        // No placement: the node default.
        let qp = QueuePlacement::from_policy(Some(&p), PlacementSetting::Auto);
        assert_eq!(qp.placement, PlacementSetting::Auto);
        p.placement = Some(PLACEMENT_WORKERS);
        p.selector = vec!["gpu=true".into()];
        let qp = QueuePlacement::from_policy(Some(&p), PlacementSetting::Embedded);
        assert_eq!(qp.placement, PlacementSetting::Workers);
        assert!(qp.matches(&labels(&[("gpu", "true")])));
        assert!(!qp.matches(&labels(&[("gpu", "false")])));
        p.placement = Some(PLACEMENT_EMBEDDED);
        assert_eq!(
            QueuePlacement::from_policy(Some(&p), PlacementSetting::Workers).placement,
            PlacementSetting::Embedded
        );
        p.placement = Some(PLACEMENT_AUTO);
        assert_eq!(
            QueuePlacement::from_policy(Some(&p), PlacementSetting::Embedded).placement,
            PlacementSetting::Auto
        );
        // No policy at all: the default with an empty selector.
        let qp = QueuePlacement::from_policy(None, PlacementSetting::Workers);
        assert_eq!(qp, QueuePlacement::new(PlacementSetting::Workers));
    }

    #[test]
    fn unparsable_selector_matches_nothing() {
        let p = QueuePolicy {
            queue: "q".into(),
            placement: Some(PLACEMENT_WORKERS),
            selector: vec!["gpu in (".into()],
            ..QueuePolicy::default()
        };
        let qp = QueuePlacement::from_policy(Some(&p), PlacementSetting::Embedded);
        assert!(qp.selector.is_none());
        assert!(!qp.matches(&labels(&[])));
        assert!(!qp.matches(&labels(&[("gpu", "true")])));
    }
}
