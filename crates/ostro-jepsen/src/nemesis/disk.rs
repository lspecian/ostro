//! Disk-stall nemesis (14-10, D-P14-19): fsync / state-store persistence stalls through the
//! failpoints-only test endpoint `POST /v1/admin/test/failpoints` (`p14/test_hooks.rs`),
//! with the `fail` crate's `sleep(ms)` action: the failpoint thread sleeps, then the I/O
//! proceeds normally (a stall, never an error).
//!
//! Sites ([`SITES`], the existing Phase 2/5 failpoints, 14-01 inventory): `wal::fsync`
//! (the vendored WAL's group-commit fsync, every append), `state::commit` (the state-store
//! batch commit of apply) and `state::persist` (the state-store fsync). The node prefers the
//! leader of a partition; site and duration (100..=2,000 ms by default) are a pure function
//! of the slot's `pick`. Heal sets every touched site `off` and requires every touched node
//! to still run (a stall must never crash a node).

use std::sync::{Mutex, PoisonError};

use ostro_repl_conformance::harness::ClusterHarness;
use ostro_repl_conformance::harness::process::ProcCluster;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde_json::{Value, json};

use super::clock::{choose_node, post_hook};
use crate::run::{LocalBoxFuture, Nemesis};

/// The failpoint endpoint.
pub const FAILPOINTS_PATH: &str = "/v1/admin/test/failpoints";
/// The stall sites (existing failpoint names).
pub const SITES: [&str; 3] = ["wal::fsync", "state::commit", "state::persist"];

/// Stall `site` on `node` for `ms` per hit.
pub async fn stall(
    c: &ProcCluster,
    token: Option<&str>,
    node: u64,
    site: &str,
    ms: u64,
) -> Result<Value, String> {
    post_hook(
        &c.client(node),
        token,
        FAILPOINTS_PATH,
        &json!({ "name": site, "action": format!("sleep({ms})") }),
    )
    .await
}

/// Remove every stall site on `node`.
pub async fn clear(c: &ProcCluster, token: Option<&str>, node: u64) -> Result<(), String> {
    for site in SITES {
        post_hook(
            &c.client(node),
            token,
            FAILPOINTS_PATH,
            &json!({ "name": site, "action": "off" }),
        )
        .await?;
    }
    Ok(())
}

/// The disk-stall nemesis (a [`Nemesis`] for `NemesisKind::Custom`).
#[derive(Debug)]
pub struct DiskNemesis {
    /// Stall per hit, (min, max) ms.
    pub stall_ms: (u64, u64),
    /// Sites to choose from.
    pub sites: Vec<&'static str>,
    token: Option<String>,
    touched: Mutex<Vec<u64>>,
}

impl DiskNemesis {
    /// Every site, 100..=2,000 ms.
    pub fn new(token: Option<String>) -> Self {
        Self {
            stall_ms: (100, 2_000),
            sites: SITES.to_vec(),
            token,
            touched: Mutex::new(Vec::new()),
        }
    }

    /// The (site, ms) of choice `pick` (a pure function of it).
    pub fn change(&self, pick: u64) -> (&'static str, u64) {
        let mut r = StdRng::seed_from_u64(pick ^ 0x0064_6973_6b21);
        let site = self.sites[r.random_range(0..self.sites.len().max(1))];
        let (lo, hi) = (self.stall_ms.0, self.stall_ms.1.max(self.stall_ms.0));
        (site, r.random_range(lo..=hi))
    }

    fn touched(&self) -> std::sync::MutexGuard<'_, Vec<u64>> {
        self.touched.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Nemesis for DiskNemesis {
    fn name(&self) -> String {
        "disk:stall".to_owned()
    }

    fn inject<'a>(
        &'a self,
        c: &'a mut ProcCluster,
        pick: u64,
    ) -> LocalBoxFuture<'a, Result<String, String>> {
        Box::pin(async move {
            let (node, why) = choose_node(c, pick).ok_or("no running node")?;
            let (site, ms) = self.change(pick);
            self.touched().push(node);
            stall(c, self.token.as_deref(), node, site, ms).await?;
            Ok(format!("{why} {node}: {site}=sleep({ms})"))
        })
    }

    fn heal<'a>(&'a self, c: &'a mut ProcCluster) -> LocalBoxFuture<'a, Result<String, String>> {
        Box::pin(async move {
            let mut nodes = std::mem::take(&mut *self.touched());
            nodes.sort_unstable();
            nodes.dedup();
            for n in &nodes {
                if !c.is_running(*n) {
                    return Err(format!(
                        "node {n} is not running after a disk stall (crash)"
                    ));
                }
                clear(c, self.token.as_deref(), *n).await?;
            }
            Ok(format!("cleared {nodes:?}"))
        })
    }
}
