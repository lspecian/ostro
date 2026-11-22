//! SC5 local proof (plan 07-20, WORK-05, D-W22): a KEDA simulator drives real
//! `ostro worker` processes from the demand endpoint, 0 → N → 0.
//!
//! No Docker, kind or k3d exist here and the reachable microk8s cluster is off-limits, so
//! this file implements KEDA's metrics-api scaler math itself ([`Scaler`]) and acts on
//! the result by spawning or SIGTERMing `ostro worker` processes (the simulator never
//! shells out to kubectl or helm). A real ScaledObject run
//! (`deploy/keda/scaledobject-metrics-api.yaml`) is human_needed (07-22).
//!
//! # KEDA metrics-api semantics reproduced
//!
//! - **Activation** (0 → 1): the scaler is active only when the metric is strictly
//!   greater than `activationTargetValue` (`demand > activation_target`).
//! - **Scaling** (1 → N): `desired = ceil(demand / targetValue)`, clamped to
//!   `[max(minReplicaCount, 1), maxReplicaCount]` while active.
//! - **Cool-down** (N → 0): only after the scaler has been inactive for `cooldownPeriod`
//!   since the last active poll. Until then the replica count stays where it is.
//! - **N → M (M > 0)** follows `desired` immediately. The HPA's own scale-down
//!   stabilisation window (default 300 s) is out of scope: following `desired` at once is
//!   the harsher case for exactly-once completion, because every scale-down drains a
//!   worker with runs on it.
#![cfg(unix)]
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;
#[path = "common/p7.rs"]
mod p7;

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use common::{events_of, is_terminal, step_completions};
use p7::{P7_T, P7Cluster, P7Opts, WorkerOpts, WorkerProc};
use serde_json::{Value, json};

/// KEDA's metrics-api scaler plus the cool-down of the ScaledObject controller.
#[derive(Clone, Debug)]
pub struct Scaler {
    /// `targetValue`: metric value per replica.
    pub target: f64,
    /// `activationTargetValue`: active only when the metric is strictly greater.
    pub activation_target: f64,
    /// `minReplicaCount`.
    pub min: u32,
    /// `maxReplicaCount`.
    pub max: u32,
    /// `cooldownPeriod`: inactive time before N → 0.
    pub cooldown: Duration,
    /// The last poll at which the scaler was active.
    last_active: Option<Instant>,
}

impl Scaler {
    pub fn new(
        target: f64,
        activation_target: f64,
        min: u32,
        max: u32,
        cooldown: Duration,
    ) -> Self {
        assert!(target > 0.0, "targetValue must be > 0");
        assert!(max >= 1 && min <= max, "replica bounds");
        Self {
            target,
            activation_target,
            min,
            max,
            cooldown,
            last_active: None,
        }
    }

    /// The replica count KEDA would ask for at `now`, given the polled `demand` and the
    /// `current` replica count.
    pub fn desired(&mut self, demand: f64, current: u32, now: Instant) -> u32 {
        if demand > self.activation_target {
            self.last_active = Some(now);
            let raw = (demand / self.target).ceil();
            // `raw` is finite and ≥ 0 here; saturate before the integer clamp.
            let raw = if raw >= f64::from(u32::MAX) {
                u32::MAX
            } else {
                raw as u32
            };
            return raw.clamp(self.min.max(1), self.max);
        }
        // Inactive: only minReplicaCount (0 here) after the cool-down; until then KEDA
        // leaves the scale target alone.
        if current > self.min {
            let cooled = match self.last_active {
                Some(t) => now.saturating_duration_since(t) >= self.cooldown,
                // Never active since the simulator started: KEDA scales to min at once.
                None => true,
            };
            if cooled { self.min } else { current }
        } else {
            self.min
        }
    }
}

#[cfg(test)]
mod scaler_tests {
    use super::*;

    fn s() -> Scaler {
        Scaler::new(8.0, 0.0, 0, 4, Duration::from_secs(5))
    }

    #[test]
    fn scaler_activation_is_strictly_greater() {
        let t = Instant::now();
        let mut sc = s();
        assert_eq!(
            sc.desired(0.0, 0, t),
            0,
            "demand 0, activation 0 stays at 0"
        );
        let mut sc = Scaler::new(8.0, 3.0, 0, 4, Duration::from_secs(5));
        assert_eq!(sc.desired(3.0, 0, t), 0, "equal to activation is inactive");
        assert_eq!(sc.desired(3.5, 0, t), 1, "above activation activates");
    }

    #[test]
    fn scaler_ceil_math() {
        let t = Instant::now();
        let mut sc = s();
        assert_eq!(sc.desired(9.0, 0, t), 2, "ceil(9 / 8) = 2");
        assert_eq!(sc.desired(8.0, 2, t), 1, "ceil(8 / 8) = 1");
        assert_eq!(sc.desired(0.5, 1, t), 1, "fractional demand rounds up");
        assert_eq!(sc.desired(16.0, 1, t), 2);
        assert_eq!(sc.desired(17.0, 2, t), 3);
    }

    #[test]
    fn scaler_clamps_to_max_and_min() {
        let t = Instant::now();
        let mut sc = s();
        assert_eq!(sc.desired(1_000.0, 1, t), 4, "clamped to max");
        assert_eq!(sc.desired(f64::MAX, 1, t), 4, "huge demand saturates");
        let mut sc = Scaler::new(8.0, 0.0, 2, 4, Duration::from_secs(5));
        assert_eq!(sc.desired(1.0, 0, t), 2, "clamped up to min while active");
    }

    #[test]
    fn scaler_scales_to_zero_only_after_cooldown() {
        let t = Instant::now();
        let mut sc = s();
        assert_eq!(sc.desired(20.0, 0, t), 3);
        // Inactive polls inside the cool-down keep the current count.
        for ms in [500u64, 1_000, 4_999] {
            assert_eq!(
                sc.desired(0.0, 3, t + Duration::from_millis(ms)),
                3,
                "at +{ms} ms"
            );
        }
        assert_eq!(
            sc.desired(0.0, 3, t + Duration::from_secs(5)),
            0,
            "cool-down passed"
        );
        // A re-activation restarts the window.
        assert_eq!(sc.desired(1.0, 0, t + Duration::from_secs(6)), 1);
        assert_eq!(sc.desired(0.0, 1, t + Duration::from_secs(10)), 1);
        assert_eq!(sc.desired(0.0, 1, t + Duration::from_secs(11)), 0);
    }

    #[test]
    fn scaler_scale_up_from_zero_is_immediate() {
        let t = Instant::now();
        let mut sc = s();
        assert_eq!(sc.desired(30.0, 0, t), 4, "0 → max in one poll");
        let mut sc = Scaler::new(2.0, 0.0, 0, 4, Duration::from_secs(5));
        assert_eq!(sc.desired(1.0, 0, t), 1);
    }

    #[test]
    fn scaler_scale_down_between_nonzero_counts_is_immediate() {
        let t = Instant::now();
        let mut sc = Scaler::new(2.0, 0.0, 0, 4, Duration::from_secs(5));
        assert_eq!(sc.desired(30.0, 0, t), 4);
        assert_eq!(sc.desired(3.0, 4, t + Duration::from_millis(500)), 2);
        assert_eq!(sc.desired(1.0, 2, t + Duration::from_secs(1)), 1);
    }
}

// ---------------------------------------------------------------------------------
// The control loop against real workers
// ---------------------------------------------------------------------------------

/// Simulated KEDA polling interval (`pollingInterval`).
const POLL: Duration = Duration::from_millis(500);
/// Runs in the burst.
const RUNS: usize = 30;
/// Long runs submitted with the burst: `timeouts` `sleep_ms` runs that stay parked hot
/// (leased) on a worker for [`LONG_MS`]. When the short runs are done, demand falls to
/// these few and the scaler drops N → M > 0 while they are leased on workers, so the
/// SIGTERM drain evicts in-flight activations (the exactly-once case under scale-down).
/// A long `cpu-burn` cannot do this: its wall time is not predictable (`ms: 1500` ran in
/// well under a second here) and `ms: 4000` exhausts the fuel budget.
const LONG_RUNS: usize = 3;
/// Sleep of a long run; `[engine] hot_wait` is set above it so the run stays parked.
const LONG_MS: u64 = 6_000;
/// `--slots` of every simulated replica (= `targetValue`, as the manifests require).
const SLOTS: u32 = 2;
/// The queue the ScaledObject scales on.
const QUEUE: &str = "heavy";

/// One poll of the simulator.
#[derive(Clone, Debug)]
struct Tick {
    t: Duration,
    demand: u64,
    running_on_workers: u64,
    backlog: u64,
    desired: u32,
    /// Live (not draining) workers after acting on `desired`.
    current: u32,
    draining: usize,
    open_runs: usize,
}

/// The simulated scale target: live replicas plus replicas draining after a SIGTERM.
/// Every child is reaped on drop (the `P7Proc` drop guard kills what is still alive).
struct Pool<'a> {
    cluster: &'a P7Cluster,
    live: Vec<WorkerProc>,
    draining: Vec<WorkerProc>,
    spawned: u32,
    terminated: u32,
}

impl<'a> Pool<'a> {
    fn new(cluster: &'a P7Cluster) -> Self {
        Self {
            cluster,
            live: Vec::new(),
            draining: Vec::new(),
            spawned: 0,
            terminated: 0,
        }
    }

    fn current(&self) -> u32 {
        u32::try_from(self.live.len()).expect("replicas")
    }

    /// Scale to `desired`: spawn new replicas, or SIGTERM the newest (KEDA/HPA pick a
    /// pod; newest-first is the Deployment controller's default for equal pods).
    fn scale_to(&mut self, desired: u32) {
        while self.current() < desired {
            self.spawned += 1;
            let id = format!("keda-w{}", self.spawned);
            let w = self
                .cluster
                .spawn_worker(WorkerOpts::new(&id, SLOTS).label("pool", QUEUE));
            self.live.push(w);
        }
        while self.current() > desired {
            let w = self.live.pop().expect("live worker");
            w.sigterm();
            self.terminated += 1;
            self.draining.push(w);
        }
    }

    /// Reap drained replicas that exited; every drain must exit cleanly.
    fn reap(&mut self) {
        let mut still = Vec::new();
        for mut w in self.draining.drain(..) {
            match w.try_exit() {
                Some(st) => assert!(
                    st.success(),
                    "worker {} drain exit {st}:\n{}",
                    w.id,
                    w.output()
                ),
                None => still.push(w),
            }
        }
        self.draining = still;
    }
}

fn u(v: &Value, k: &str) -> u64 {
    v[k].as_u64()
        .unwrap_or_else(|| panic!("demand field {k}: {v}"))
}

fn print_timeline(timeline: &[Tick]) {
    eprintln!(
        "keda_sim timeline (t, demand, running_on_workers, backlog, desired, current, draining, open runs):"
    );
    eprintln!(
        "| t (s) | demand | on workers | backlog | desired | current | draining | open runs |"
    );
    eprintln!("|---|---|---|---|---|---|---|---|");
    for k in timeline {
        eprintln!(
            "| {:.1} | {} | {} | {} | {} | {} | {} | {} |",
            k.t.as_secs_f64(),
            k.demand,
            k.running_on_workers,
            k.backlog,
            k.desired,
            k.current,
            k.draining,
            k.open_runs
        );
    }
}

/// SC5 (WORK-05): a burst on a `workers` queue takes the pool 0 → N (N ≥ 2) → 0, every
/// run completes exactly once despite the scale-down drains, and demand stays > 0 while
/// runs execute on workers, so the pool never scales in to zero under load.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn zero_to_n_to_zero() {
    let t0 = Instant::now();
    let c = P7Cluster::start(P7Opts {
        hot_wait: Some(Duration::from_secs(15)),
        env: vec![("RUST_LOG".into(), "info".into())],
        ..P7Opts::default()
    })
    .await;
    c.api.deploy("cpu-burn").await;
    c.api.deploy("timeouts").await;
    let flow = c
        .api
        .set_flow(
            QUEUE,
            json!({"placement": "workers", "selector": [format!("pool={QUEUE}")]}),
        )
        .await;
    assert_eq!(flow["placement"], "workers", "{flow}");
    assert!(
        c.api.workers().await.is_empty(),
        "no worker before the burst"
    );
    let d0 = c.api.demand(QUEUE).await;
    assert_eq!(u(&d0, "demand"), 0, "idle queue: {d0}");

    let mut scaler = Scaler::new(f64::from(SLOTS), 0.0, 0, 4, Duration::from_secs(5));
    let mut pool = Pool::new(&c);

    // The burst: 30 short runs (plus the long tail) submitted at once.
    let mut submit = tokio::task::JoinSet::new();
    for i in 0..RUNS + LONG_RUNS {
        let api = c.api.clone();
        let long = i >= RUNS;
        submit.spawn(async move {
            let id = if long {
                let input = json!({"mode": "sleep_ms", "n": LONG_MS});
                api.start("timeouts", json!({"input": input, "queue": QUEUE}))
                    .await
            } else {
                api.start("cpu-burn", json!({"input": {"ms": 500}, "queue": QUEUE}))
                    .await
            };
            (id, long)
        });
    }
    let mut ids: Vec<(String, bool)> = Vec::new();
    while let Some(id) = submit.join_next().await {
        ids.push(id.expect("submit task"));
    }
    let burst_at = Instant::now();
    let mut open: BTreeSet<String> = ids.iter().map(|(id, _)| id.clone()).collect();
    let long_ids: BTreeSet<String> = ids
        .iter()
        .filter(|(_, long)| *long)
        .map(|(id, _)| id.clone())
        .collect();

    let mut timeline: Vec<Tick> = Vec::new();
    let mut peak = 0u32;
    let mut scaled_up_at: Option<Duration> = None;
    let mut all_done_at: Option<Instant> = None;
    let mut zero_at: Option<Instant> = None;
    let mut next = Instant::now();

    loop {
        // One KEDA poll: read the metric, decide, act.
        // The metrics-api trigger's URL, `valueLocation: "demand"`.
        let (st, d) = c.api.get(&format!("/v1/queues/{QUEUE}/demand")).await;
        assert_eq!(st, reqwest::StatusCode::OK, "demand endpoint: {d}");
        let demand = u(&d, "demand");
        let on_workers = u(&d, "running_on_workers");
        let now = Instant::now();
        #[allow(clippy::cast_precision_loss)]
        let desired = scaler.desired(demand as f64, pool.current(), now);

        // D-W12 / Pitfall 4: runs executing on workers keep demand above zero.
        if on_workers > 0 {
            assert!(
                demand > 0,
                "demand 0 with {on_workers} runs on workers: {d}"
            );
        }
        pool.scale_to(desired);
        pool.reap();

        // Which runs are still open (terminal ones leave the set).
        let mut finished = Vec::new();
        for id in &open {
            if let Some(r) = c.api.run(id).await
                && r["status"].as_str().is_some_and(is_terminal)
            {
                finished.push(id.clone());
            }
        }
        for id in finished {
            open.remove(&id);
        }
        if open.is_empty() && all_done_at.is_none() {
            all_done_at = Some(Instant::now());
        }

        let current = pool.current();
        peak = peak.max(current);
        if current >= 2 && scaled_up_at.is_none() {
            scaled_up_at = Some(burst_at.elapsed());
        }
        timeline.push(Tick {
            t: burst_at.elapsed(),
            demand,
            running_on_workers: on_workers,
            backlog: u(&d, "backlog"),
            desired,
            current,
            draining: pool.draining.len(),
            open_runs: open.len(),
        });

        // Never scaled in to zero while runnable work is open (once the pool has come up).
        // A long run evicted from its hot park by a scale-down drain finishes its sleep as a
        // cold timer: it is neither ready nor on a worker, so demand is 0 and the scaler may
        // legitimately reach 0 workers until the timer fires (then scale from zero). The
        // short cpu-burn runs never park, so none of them may be open at zero workers.
        if current == 0 && !open.is_empty() && peak > 0 {
            let short_open: Vec<&String> =
                open.iter().filter(|id| !long_ids.contains(*id)).collect();
            if !short_open.is_empty() || on_workers > 0 {
                print_timeline(&timeline);
                panic!(
                    "pool at 0 with {} open runs, {} of them short cpu-burn runs \
                     (demand {demand}, on workers {on_workers}): {short_open:?}",
                    open.len(),
                    short_open.len()
                );
            }
        }
        if scaled_up_at.is_none() && burst_at.elapsed() > Duration::from_secs(5) {
            print_timeline(&timeline);
            panic!("pool did not reach 2 workers within 5 s of the burst");
        }

        // After the drain: the pool reaches 0 within cool-down + slack and stays there 3 s.
        if let Some(done) = all_done_at {
            if current == 0 && pool.draining.is_empty() {
                let z = *zero_at.get_or_insert(now);
                if now.duration_since(z) >= Duration::from_secs(3) {
                    break;
                }
            } else {
                assert!(zero_at.is_none(), "pool scaled back up after reaching zero");
                if done.elapsed() > scaler.cooldown + Duration::from_secs(15) {
                    print_timeline(&timeline);
                    panic!("pool not at 0 within cool-down + 15 s of the last run");
                }
            }
        }
        assert!(t0.elapsed() < P7_T * 2, "keda_sim hang guard");

        next += POLL;
        let now = Instant::now();
        if next > now {
            tokio::time::sleep(next - now).await;
        } else {
            next = now;
        }
    }
    print_timeline(&timeline);

    let all_done = all_done_at.expect("runs done");
    let zero = zero_at.expect("zero reached");
    let to_zero = zero.duration_since(all_done);
    assert!(peak >= 2, "peak {peak}");
    assert!(
        to_zero >= scaler.cooldown - POLL,
        "scaled to 0 {to_zero:?} after the last run, before the {:?} cool-down",
        scaler.cooldown
    );
    // The last 3 s of polls saw zero workers and zero demand.
    let tail: Vec<&Tick> = timeline
        .iter()
        .filter(|k| k.t >= zero.duration_since(burst_at))
        .collect();
    assert!(tail.len() >= 6, "{} zero polls", tail.len());
    for k in tail {
        assert_eq!((k.current, k.demand, k.draining), (0, 0, 0), "{k:?}");
    }
    assert!(
        c.api.workers().await.is_empty() || {
            // Drained sessions leave the registry asynchronously; give them a moment.
            let deadline = Instant::now() + Duration::from_secs(10);
            loop {
                if c.api.workers().await.is_empty() {
                    break true;
                }
                if Instant::now() > deadline {
                    break false;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        },
        "workers still registered after scale to zero"
    );

    // Exactly once: one RunStarted, one terminal event, burn completed once, per run.
    let out = c.server().output();
    let mut extra_activations = 0usize;
    let mut moved = 0usize;
    for (id, long) in &ids {
        let r = c.api.wait_terminal(id, P7_T).await;
        assert_eq!(r["status"], "completed", "{id}: {r}");
        let h = c.api.history(id).await;
        assert_eq!(
            events_of(&h, "run_started").len(),
            1,
            "{id}: one RunStarted"
        );
        let terminal = events_of(&h, "run_completed").len()
            + events_of(&h, "run_failed").len()
            + events_of(&h, "run_cancelled").len();
        assert_eq!(terminal, 1, "{id}: one terminal event");
        assert_eq!(events_of(&h, "run_completed").len(), 1, "{id}");
        if *long {
            assert_eq!(r["result"], "slept", "{id}: {r}");
        } else {
            assert_eq!(step_completions(&h, "burn"), 1, "{id}: burn completed once");
        }
        let activations = out.matches(&format!("remote activate run={id} ")).count();
        assert!(activations >= 1, "{id} never activated on a worker");
        extra_activations += activations - 1;
        // A run activated on more than one worker was evicted by a drain and re-granted.
        let needle = format!("remote activate run={id} ");
        let on: BTreeSet<&str> = out
            .lines()
            .filter(|l| l.contains(&needle))
            .filter_map(|l| l.split("worker=").nth(1))
            .map(|w| w.split_whitespace().next().unwrap_or(w))
            .collect();
        if on.len() > 1 {
            moved += 1;
        }
    }

    eprintln!(
        "keda_sim::zero_to_n_to_zero: peak {peak} workers, >= 2 workers at {:?} after the burst, \
         last run done at {:?}, zero at {:?} ({to_zero:?} after the last run), \
         spawned {}, SIGTERMed {}, extra activations (drain evictions, wake-ups) {extra_activations}, runs moved to another worker by a drain {moved}, total {:?}",
        scaled_up_at.expect("scaled up"),
        all_done.duration_since(burst_at),
        zero.duration_since(burst_at),
        pool.spawned,
        pool.terminated,
        t0.elapsed()
    );
    assert!(
        pool.terminated >= 2,
        "scale-down drained {} workers",
        pool.terminated
    );
    drop(pool);
}
