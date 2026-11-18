//! `FlakyRouter` (plan 02-13): wraps the node's [`InProcessRouter`] and, per `deliver` call,
//! decides with a seeded RNG to
//! - deliver normally,
//! - deliver and then return `Err` (lost ack: the shuffler resends, the target sees a
//!   duplicate), or
//! - return `Err` without delivering (lost request).
//!
//! The probabilities live in a shared [`FlakyControl`] that survives node restarts (each
//! `Node::open` calls the wrapper again with a fresh `InProcessRouter`).

use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use ostro_partition::node::RouterWrapper;
use ostro_partition::{InProcessRouter, PartitionError, PartitionRouter, RouteFuture};
use ostro_types::PartitionId;
use ostro_types::command::InboundBatch;

const PPM: u32 = 1_000_000;

/// Shared knobs and counters of every [`FlakyRouter`] built from it.
#[derive(Debug)]
pub struct FlakyControl {
    rng: Mutex<u64>,
    lost_ack_ppm: AtomicU32,
    lost_req_ppm: AtomicU32,
    pub delivered: AtomicUsize,
    pub lost_acks: AtomicUsize,
    pub lost_reqs: AtomicUsize,
}

impl FlakyControl {
    pub fn new(seed: u64) -> Arc<Self> {
        Arc::new(Self {
            rng: Mutex::new(seed),
            lost_ack_ppm: AtomicU32::new(0),
            lost_req_ppm: AtomicU32::new(0),
            delivered: AtomicUsize::new(0),
            lost_acks: AtomicUsize::new(0),
            lost_reqs: AtomicUsize::new(0),
        })
    }

    /// Probabilities in `[0, 1]` (their sum is capped at 1).
    pub fn set(&self, p_lost_ack: f64, p_lost_req: f64) {
        let ppm = |p: f64| (p.clamp(0.0, 1.0) * f64::from(PPM)) as u32;
        let req = ppm(p_lost_req);
        let ack = ppm(p_lost_ack).min(PPM - req);
        self.lost_req_ppm.store(req, Ordering::Relaxed);
        self.lost_ack_ppm.store(ack, Ordering::Relaxed);
    }

    /// A [`NodeConfig::router_wrapper`](ostro_partition::NodeConfig) producing
    /// [`FlakyRouter`]s that share this control.
    pub fn wrapper(self: &Arc<Self>) -> RouterWrapper {
        let ctl = Arc::clone(self);
        Arc::new(move |inner| {
            Arc::new(FlakyRouter {
                inner,
                ctl: Arc::clone(&ctl),
            }) as Arc<dyn PartitionRouter>
        })
    }

    fn draw(&self) -> u32 {
        let mut s = self.rng.lock().unwrap_or_else(|p| p.into_inner());
        *s = s.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = *s;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        ((z ^ (z >> 31)) % u64::from(PPM)) as u32
    }
}

/// See the module docs.
pub struct FlakyRouter {
    inner: Arc<InProcessRouter>,
    ctl: Arc<FlakyControl>,
}

impl PartitionRouter for FlakyRouter {
    fn deliver(&self, target: PartitionId, batch: InboundBatch) -> RouteFuture<'_> {
        let r = self.ctl.draw();
        let req = self.ctl.lost_req_ppm.load(Ordering::Relaxed);
        let ack = self.ctl.lost_ack_ppm.load(Ordering::Relaxed);
        Box::pin(async move {
            if r < req {
                self.ctl.lost_reqs.fetch_add(1, Ordering::Relaxed);
                return Err(PartitionError::Failed("flaky router: request lost".into()));
            }
            let out = self.inner.deliver(target, batch).await;
            if r < req.saturating_add(ack) {
                self.ctl.lost_acks.fetch_add(1, Ordering::Relaxed);
                return Err(PartitionError::Failed("flaky router: ack lost".into()));
            }
            if out.is_ok() {
                self.ctl.delivered.fetch_add(1, Ordering::Relaxed);
            }
            out
        })
    }
}
