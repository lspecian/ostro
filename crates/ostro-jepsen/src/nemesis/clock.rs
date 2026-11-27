//! Clock-skew nemesis (14-10, D-P14-20) through the failpoints-only test endpoint
//! `POST /v1/admin/test/clock` (`p14/test_hooks.rs`): each node's injected clock is a
//! `SkewClock` in a failpoints build, so skew is injected in-process (static musl binaries
//! cannot use libfaketime).
//!
//! | fault | effect on one node |
//! |-------|--------------------|
//! | [`ClockFault::Bump`] | wall offset of ±1..=`max_bump_ms` (default ±60 s) |
//! | [`ClockFault::Strobe`] | triangle-wave wall offset, amplitude 100..=5,000 ms, period 500..=5,000 ms |
//! | [`ClockFault::Drift`] | monotonic (and wall) rate drift within ±`max_drift` (default ±5%) |
//! | [`ClockFault::NegativeControl`] | 30% slow drift (above `lease_margin` 0.2); only when explicitly requested |
//!
//! The values and the node are a pure function of the slot's `pick` (seeded schedule); the
//! node prefers the leader of a partition (`pick % partitions`). Heal resets every node
//! the nemesis touched (`{reset: true}`: offset, strobe and rate back to 0; the monotonic
//! clock keeps its accumulated offset and never jumps).
//!
//! What skew reaches: the node's injected clock (leader timers, cron, dispatcher leases,
//! command timestamps). OS timers (tokio sleeps, TCP keepalive) are not skewed, and the
//! D-P13-15 read lease is measured on `Instant` (14-01), so the negative control cannot
//! violate it: see `tests/nemesis_faults.rs`.

use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use ostro_repl_conformance::harness::process::ProcCluster;
use ostro_repl_conformance::harness::{ApiClient, ClusterHarness};
use ostro_repl_conformance::scenarios::process::tools;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde_json::{Value, json};

use crate::run::{LocalBoxFuture, Nemesis};
use crate::workload::pause;

/// The clock endpoint.
pub const CLOCK_PATH: &str = "/v1/admin/test/clock";
/// Default bound of a wall bump (ms).
pub const MAX_BUMP_MS: i64 = 60_000;
/// Default bound of the drift (fraction; ±5%).
pub const MAX_DRIFT: f64 = 0.05;
/// The negative control's drift: 30% slow, above `lease_margin` (0.2).
pub const NEGATIVE_CONTROL_RATE: f64 = -0.30;

/// `POST path` with `body` on one node (3 attempts, 5 s each); the JSON answer.
pub async fn post_hook(
    api: &ApiClient,
    token: Option<&str>,
    path: &str,
    body: &Value,
) -> Result<Value, String> {
    let mut last = String::new();
    for attempt in 0..3 {
        if attempt > 0 {
            pause(Duration::from_millis(300)).await;
        }
        let mut rb = api
            .http
            .post(format!("{}{path}", api.base_url))
            .json(body)
            .timeout(Duration::from_secs(5));
        if let Some(t) = token {
            rb = rb.bearer_auth(t);
        }
        match rb.send().await {
            Ok(r) => {
                let st = r.status();
                let b = r.bytes().await.unwrap_or_default();
                let v: Value = serde_json::from_slice(&b).unwrap_or(Value::Null);
                if st.is_success() {
                    return Ok(v);
                }
                last = format!("http {st}: {v}");
                if st.is_client_error() {
                    break;
                }
            }
            Err(e) => last = e.to_string(),
        }
    }
    Err(format!("POST {}{path}: {last}", api.base_url))
}

/// Set a wall offset of `ms` on `node` (replacing the previous offset).
pub async fn bump(
    c: &ProcCluster,
    token: Option<&str>,
    node: u64,
    ms: i64,
) -> Result<Value, String> {
    post_hook(
        &c.client(node),
        token,
        CLOCK_PATH,
        &json!({ "offset_ms": ms }),
    )
    .await
}

/// A triangle-wave wall offset on `node`.
pub async fn strobe(
    c: &ProcCluster,
    token: Option<&str>,
    node: u64,
    amplitude_ms: u64,
    period_ms: u64,
) -> Result<Value, String> {
    post_hook(
        &c.client(node),
        token,
        CLOCK_PATH,
        &json!({ "strobe_ms": amplitude_ms, "strobe_period_ms": period_ms.max(1) }),
    )
    .await
}

/// Monotonic rate drift `mono_rate` (fraction) on `node`.
pub async fn drift(
    c: &ProcCluster,
    token: Option<&str>,
    node: u64,
    mono_rate: f64,
) -> Result<Value, String> {
    post_hook(
        &c.client(node),
        token,
        CLOCK_PATH,
        &json!({ "mono_rate": mono_rate }),
    )
    .await
}

/// No skew on `node` any more.
pub async fn reset(c: &ProcCluster, token: Option<&str>, node: u64) -> Result<Value, String> {
    post_hook(
        &c.client(node),
        token,
        CLOCK_PATH,
        &json!({ "reset": true }),
    )
    .await
}

/// A clock fault kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ClockFault {
    /// Wall offset bump.
    Bump,
    /// Oscillating wall offset.
    Strobe,
    /// Rate drift within bounds.
    Drift,
    /// Drift above the lease margin (explicit only).
    NegativeControl,
}

impl ClockFault {
    /// The in-bounds faults (the default menu; never the negative control).
    pub const IN_BOUNDS: [ClockFault; 3] =
        [ClockFault::Bump, ClockFault::Strobe, ClockFault::Drift];

    /// Schedule / report name.
    pub fn name(self) -> &'static str {
        match self {
            ClockFault::Bump => "clock:bump",
            ClockFault::Strobe => "clock:strobe",
            ClockFault::Drift => "clock:drift",
            ClockFault::NegativeControl => "clock:negative_control",
        }
    }
}

/// The clock nemesis of one fault kind (a [`Nemesis`] for `NemesisKind::Custom`).
#[derive(Debug)]
pub struct ClockNemesis {
    fault: ClockFault,
    /// Bound of a bump (ms).
    pub max_bump_ms: i64,
    /// Bound of the drift (fraction).
    pub max_drift: f64,
    token: Option<String>,
    touched: Mutex<Vec<u64>>,
}

impl ClockNemesis {
    /// `fault` with the default bounds. [`ClockFault::NegativeControl`] is accepted here only
    /// because the caller asked for it by name.
    pub fn new(fault: ClockFault, token: Option<String>) -> Self {
        Self {
            fault,
            max_bump_ms: MAX_BUMP_MS,
            max_drift: MAX_DRIFT,
            token,
            touched: Mutex::new(Vec::new()),
        }
    }

    /// The in-bounds menu (bump, strobe, drift) as `NemesisKind::Custom` entries.
    pub fn menu(token: Option<String>) -> Vec<crate::run::NemesisKind> {
        ClockFault::IN_BOUNDS
            .into_iter()
            .map(|f| {
                crate::run::NemesisKind::Custom(std::sync::Arc::new(Self::new(f, token.clone())))
            })
            .collect()
    }

    /// The fault.
    pub fn fault(&self) -> ClockFault {
        self.fault
    }

    /// The change for choice `pick` (a pure function of it): `(body, description)`.
    pub fn change(&self, pick: u64) -> (Value, String) {
        let mut r = StdRng::seed_from_u64(pick ^ 0x636c_6f63_6b21);
        match self.fault {
            ClockFault::Bump => {
                let max = self.max_bump_ms.max(1);
                let mut ms = r.random_range(1..=max);
                if r.random_bool(0.5) {
                    ms = -ms;
                }
                (json!({ "offset_ms": ms }), format!("offset {ms:+} ms"))
            }
            ClockFault::Strobe => {
                let amp: u64 = r.random_range(100..=5_000);
                let period: u64 = r.random_range(500..=5_000);
                (
                    json!({ "strobe_ms": amp, "strobe_period_ms": period }),
                    format!("strobe ±{amp} ms / {period} ms"),
                )
            }
            ClockFault::Drift => {
                let ppm = (self.max_drift.abs() * 1e6) as i64;
                let rate = r.random_range(-ppm..=ppm) as f64 / 1e6;
                (
                    json!({ "mono_rate": rate }),
                    format!("drift {:+.3}%", rate * 100.0),
                )
            }
            ClockFault::NegativeControl => (
                json!({ "mono_rate": NEGATIVE_CONTROL_RATE }),
                format!(
                    "drift {:+.0}% (negative control, above lease_margin 0.2)",
                    NEGATIVE_CONTROL_RATE * 100.0
                ),
            ),
        }
    }

    fn touched(&self) -> std::sync::MutexGuard<'_, Vec<u64>> {
        self.touched.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The node for choice `pick`: the leader of partition `pick % partitions` when it runs,
/// else a running node.
pub fn choose_node(c: &ProcCluster, pick: u64) -> Option<(u64, String)> {
    let live = tools::live(c);
    if live.is_empty() {
        return None;
    }
    let p = (pick % u64::from(c.spec().partitions.max(1))) as u16;
    let l = tools::published(c, p).leader;
    if live.contains(&l) {
        return Some((l, format!("partition {p} leader")));
    }
    let n = live[(pick % live.len() as u64) as usize];
    Some((n, "node".to_owned()))
}

impl Nemesis for ClockNemesis {
    fn name(&self) -> String {
        self.fault.name().to_owned()
    }

    fn inject<'a>(
        &'a self,
        c: &'a mut ProcCluster,
        pick: u64,
    ) -> LocalBoxFuture<'a, Result<String, String>> {
        Box::pin(async move {
            let (node, why) = choose_node(c, pick).ok_or("no running node")?;
            let (body, what) = self.change(pick);
            self.touched().push(node);
            let v = post_hook(&c.client(node), self.token.as_deref(), CLOCK_PATH, &body).await?;
            let skew =
                v["wall_ms"].as_i64().unwrap_or(0) - v["inner_wall_ms"].as_i64().unwrap_or(0);
            Ok(format!("{why} {node}: {what} (wall skew now {skew:+} ms)"))
        })
    }

    fn heal<'a>(&'a self, c: &'a mut ProcCluster) -> LocalBoxFuture<'a, Result<String, String>> {
        Box::pin(async move {
            let mut nodes = std::mem::take(&mut *self.touched());
            nodes.sort_unstable();
            nodes.dedup();
            let mut done = Vec::new();
            for n in nodes {
                if c.pid(n).is_none() {
                    continue; // a restarted process starts without skew
                }
                let v = reset(c, self.token.as_deref(), n).await?;
                done.push(format!(
                    "node {n} mono {} ns",
                    v["mono_ns"].as_u64().unwrap_or(0)
                ));
            }
            Ok(format!("reset {}", done.join(", ")))
        })
    }
}
