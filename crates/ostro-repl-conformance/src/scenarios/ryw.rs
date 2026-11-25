//! Phase 13 `ryw` scenarios (tier B, 13-13; API-08 read half, D-P13-15/16): a client always
//! reads its own acknowledged writes, whichever node a load balancer sends each request to,
//! through leader moves, failover and zombie leaders. Measured by the 13-08 session checker
//! ([`RywChecker`]) over client observations (REST answers) only.
//!
//! Workload: `SESSIONS` client sessions on a 3-node RF=3 cluster, each request to a node
//! picked at random among the nodes the "load balancer" currently lists. Writes: keyed
//! starts (`smoke` runs complete at once, `signals` `long_wait` runs stay running), signals
//! with a `request_id` (to waiting runs, name `ryw` so they stay waiting), terminate (the
//! hard stop: terminal at acknowledgement; a graceful cancel is only a request and has no
//! read-observable effect at its acknowledgement), workflow activation and a per-session
//! queue policy. Reads: get run, history, list runs (load only), list workflows, get the
//! session's queue policy, and a signal probe (re-sending every acked `request_id` of a run:
//! the run must answer `duplicate: true` for each, i.e. its dedupe state shows the signal).
//! `503` answers and transport errors are availability, never recorded as observations.
//!
//! - `ryw_across_nodes_steady`: 2,000 operations, 0 violations.
//! - `ryw_across_nodes_through_moves`: the same while leaders move by graceful handoffs
//!   (13-11 `NodeOps.HandOff`) and Phase 12 `ResignLeader`.
//! - `ryw_through_failover`: a partition leader (not the meta leader) is killed mid-run.
//! - `zombie_leader_never_serves_stale`: a partition leader keeps hearing the cluster but
//!   nothing it sends arrives (outbound links cut; a brief inbound blip puts it in raft probe
//!   state, so it never learns the commits after it, including its replacement): it keeps
//!   believing it leads and never self-demotes, a successor is elected, and it must stop
//!   serving reads within `session_timeout × (1 − lease_margin)` (+ the bounded anchor wait).
//! - `zombie_serve_without_lease_mutant_bites` (`--features failpoints`): the same zombie
//!   under `route::mutant::serve_without_lease`: the checker must report a stale read.
//! - `tokens_skip_barrier`: sessions echoing `ostro-read-after` see 0 violations with fewer
//!   read barriers than without tokens.
//! - `await_across_handoff` / `await_across_leader_change`: `GET /v1/runs/{id}/result?wait=`
//!   issued at a non-leader before a graceful handoff / a resignation of the run's
//!   partition returns the result before its deadline.
//! - `zombie_asymmetric`: the partition leader can still send to the meta leader but every
//!   metadata message to it is dropped: its lease expires from its last confirmed round and
//!   it stops serving before any successor exists (0 violations).
//! - `zombie_writes_are_retryable` (p13-fixes): runs started through the successor of a
//!   probe-paused zombie; signal, cancel, retry and terminate sent to the zombie answer a
//!   retryable `503` (its write hook refuses without the lease), never `404` from its stale
//!   run record.
//! - `strict_reads_mode`: the steady workload with `[api] strict_reads = true` on every
//!   node (latency recorded for the D-P13-15 review).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::future::BoxFuture;
use ostro_proto::cluster::v1::HandOffRequest;
use ostro_proto::cluster::v1::node_ops_client::NodeOpsClient;
use ostro_types::{Epoch, PartitionCount, PartitionId};
use serde_json::{Value, json};
use tokio::time::Instant;

use super::placement::support::{
    CHANGE_T, SESSION, STEP, fail, harness, live, meta_leader, pause, poll, poll_async, published,
    published_all, wait_assignment, wait_meta_leader,
};
use super::{Scenario, ScenarioResult, Tier};
use crate::harness::fault::{Fault, RpcKind};
use crate::harness::inproc::{InProcCluster, guest_wasm};
use crate::harness::{ApiClient, ClusterHarness, ClusterSpec};
use crate::oracle::p13_ryw::{Effect, Observation, RywChecker, Session};

type Run = for<'a> fn(&'a mut InProcCluster) -> BoxFuture<'a, ScenarioResult>;

/// Deadline of one scenario.
const DEADLINE: Duration = Duration::from_secs(420);
/// Client sessions.
const SESSIONS: usize = 6;
/// Bound on one client request.
const REQ_T: Duration = Duration::from_secs(10);
/// `[api] lease_margin` of the tier-B nodes (the default).
const MARGIN: f64 = 0.2;
/// Header names (ostro_api::request_meta).
const H_CONSISTENCY: &str = "ostro-consistency";
const H_READ_AFTER: &str = "ostro-read-after";

/// Every `ryw` scenario.
pub fn all() -> Vec<Scenario> {
    let s = |name, run: Run| Scenario {
        name,
        group: "ryw",
        tier: Tier::B,
        deadline: DEADLINE,
        spec: rf3,
        run,
    };
    #[allow(unused_mut)]
    let mut v = vec![
        s("ryw_across_nodes_steady", ryw_across_nodes_steady),
        s(
            "ryw_across_nodes_through_moves",
            ryw_across_nodes_through_moves,
        ),
        s("ryw_through_failover", ryw_through_failover),
        s(
            "zombie_leader_never_serves_stale",
            zombie_leader_never_serves_stale,
        ),
        s("tokens_skip_barrier", tokens_skip_barrier),
        s("await_across_handoff", await_across_handoff),
        s("await_across_leader_change", await_across_leader_change),
        s("zombie_asymmetric", zombie_asymmetric),
        s("strict_reads_mode", strict_reads_mode),
        s("zombie_writes_are_retryable", zombie_writes_are_retryable),
    ];
    #[cfg(feature = "failpoints")]
    v.push(s(
        "zombie_serve_without_lease_mutant_bites",
        zombie_serve_without_lease_mutant_bites,
    ));
    v
}

fn rf3() -> ClusterSpec {
    ClusterSpec {
        seed: 1313,
        ..ClusterSpec::default()
    }
}

// ---------------------------------------------------------------------------------
// HTTP
// ---------------------------------------------------------------------------------

/// One answer (`None` from [`call`] = no answer: transport error or timeout).
#[derive(Debug)]
struct Resp {
    st: u16,
    body: Value,
    token: Option<String>,
}

async fn call(
    api: &ApiClient,
    method: reqwest::Method,
    path: &str,
    body: Option<Value>,
    read_after: Option<&str>,
    limit: Duration,
) -> Option<Resp> {
    let mut req = api
        .http
        .request(method, format!("{}{path}", api.base_url))
        .timeout(limit);
    if let Some(b) = body {
        req = req.json(&b);
    }
    if let Some(t) = read_after {
        req = req.header(H_READ_AFTER, t);
    }
    let r = req.send().await.ok()?;
    let st = r.status().as_u16();
    let token = r
        .headers()
        .get(H_CONSISTENCY)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let b = r.bytes().await.ok()?;
    Some(Resp {
        st,
        body: serde_json::from_slice(&b).unwrap_or(Value::Null),
        token,
    })
}

/// `PUT /v1/modules/{sha}` of guest `name`.
async fn put_module(api: &ApiClient, name: &str) -> bool {
    use sha2::{Digest, Sha256};
    let wasm = harness(&format!("guest {name}"), guest_wasm(name));
    let sha: [u8; 32] = Sha256::digest(&wasm).into();
    let hex: String = sha.iter().map(|b| format!("{b:02x}")).collect();
    api.http
        .put(format!("{}/v1/modules/{hex}", api.base_url))
        .header("content-type", "application/wasm")
        .body(wasm)
        .send()
        .await
        .is_ok_and(|r| r.status().is_success())
}

/// Deploy `smoke` and `signals` and wait until every node's (barrier-backed) workflow list
/// shows both active.
async fn deploy(c: &InProcCluster) -> ScenarioResult {
    let first = wait_meta_leader(c).await?;
    let mut order = vec![first];
    order.extend(live(c).into_iter().filter(|n| *n != first));
    for name in ["smoke", "signals"] {
        let order = &order;
        let done = poll_async(CHANGE_T, || async move {
            for n in order {
                if put_module(&c.client(*n), name).await {
                    return Some(());
                }
            }
            None
        })
        .await;
        if done.is_none() {
            return Err(fail("availability", format!("guest {name} not deployed")));
        }
    }
    for n in live(c) {
        let api = c.client(n);
        let seen = poll_async(CHANGE_T, || {
            let api = api.clone();
            async move {
                let r = call(
                    &api,
                    reqwest::Method::GET,
                    "/v1/workflows",
                    None,
                    None,
                    REQ_T,
                )
                .await?;
                let list = r.body.get("workflows").cloned().unwrap_or(r.body);
                let active = |w: &str| {
                    list.as_array().is_some_and(|a| {
                        a.iter()
                            .any(|x| x["name"] == w && x["active_version"].as_u64().is_some())
                    })
                };
                (r.st == 200 && active("smoke") && active("signals")).then_some(())
            }
        })
        .await;
        if seen.is_none() {
            return Err(fail(
                "availability",
                format!("node {n}: workflows not visible"),
            ));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------------
// The session workload
// ---------------------------------------------------------------------------------

/// What the load balancer may send requests to (scenarios edit it).
type Lb = Arc<Mutex<Vec<u64>>>;

/// Workload knobs.
#[derive(Clone)]
struct LoadCfg {
    sessions: usize,
    ops_per_session: usize,
    tokens: bool,
    /// A label unique to this run (idempotency keys, queue names, tags).
    tag: String,
    seed: u64,
    /// Pause between a session's operations.
    think: Duration,
}

/// What the workload measured.
#[derive(Debug, Default)]
struct LoadReport {
    ops: usize,
    writes_acked: usize,
    reads_recorded: usize,
    unavailable: usize,
    unexpected: Vec<String>,
    read_lat: Vec<Duration>,
    write_lat: Vec<Duration>,
}

impl LoadReport {
    fn merge(&mut self, o: LoadReport) {
        self.ops += o.ops;
        self.writes_acked += o.writes_acked;
        self.reads_recorded += o.reads_recorded;
        self.unavailable += o.unavailable;
        self.unexpected.extend(o.unexpected);
        self.read_lat.extend(o.read_lat);
        self.write_lat.extend(o.write_lat);
    }

    fn summary(&mut self) -> String {
        self.read_lat.sort_unstable();
        self.write_lat.sort_unstable();
        format!(
            "{} ops, {} writes acked, {} reads recorded, {} unavailable, {} unexpected; read p50 {:?} p99 {:?}, write p50 {:?} p99 {:?}",
            self.ops,
            self.writes_acked,
            self.reads_recorded,
            self.unavailable,
            self.unexpected.len(),
            pct(&self.read_lat, 0.5),
            pct(&self.read_lat, 0.99),
            pct(&self.write_lat, 0.5),
            pct(&self.write_lat, 0.99),
        )
    }
}

fn pct(sorted: &[Duration], q: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    sorted[(((sorted.len() - 1) as f64) * q).round() as usize]
}

/// One run a session started.
#[derive(Clone, Debug)]
struct RunRec {
    id: String,
    p: u16,
    /// A `signals` run that was not terminated (may be signalled / terminated).
    waiting: bool,
    terminal_acked: bool,
    /// A terminate whose answer was lost (it may have applied).
    terminal_maybe: bool,
    signals: Vec<String>,
}

struct SessionState {
    s: Session,
    /// Signal acks and probes live in a twin session: a `GET /v1/runs/{id}` cannot show
    /// signals, so run reads and signal probes are judged separately.
    sig: Session,
    rng: u64,
    runs: Vec<RunRec>,
    queue: String,
    n: u64,
    /// Last partition / meta tokens (sent as `ostro-read-after` when `tokens`).
    ptoken: BTreeMap<u16, String>,
    mtoken: Option<String>,
}

impl SessionState {
    fn next(&mut self) -> u64 {
        let mut x = self.rng;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng = x;
        x
    }

    fn pick(&mut self, n: usize) -> usize {
        (self.next() % n.max(1) as u64) as usize
    }
}

/// Run the session workload against the nodes `lb` lists; every observation goes to
/// `checker`.
async fn run_load(
    clients: BTreeMap<u64, ApiClient>,
    lb: Lb,
    cfg: LoadCfg,
    checker: RywChecker,
) -> LoadReport {
    let clients = Arc::new(clients);
    let mut tasks = Vec::new();
    for i in 0..cfg.sessions {
        let (clients, lb, cfg, checker) = (
            Arc::clone(&clients),
            Arc::clone(&lb),
            cfg.clone(),
            checker.clone(),
        );
        tasks.push(tokio::spawn(async move {
            let mut st = SessionState {
                s: checker.session(format!("{}-s{i}", cfg.tag)),
                sig: checker.session(format!("{}-s{i}-sig", cfg.tag)),
                rng: cfg.seed ^ (0x9e37_79b9_7f4a_7c15u64.wrapping_mul(i as u64 + 1)) | 1,
                runs: Vec::new(),
                queue: format!("{}-q{i}", cfg.tag),
                n: 0,
                ptoken: BTreeMap::new(),
                mtoken: None,
            };
            let mut rep = LoadReport::default();
            for _ in 0..cfg.ops_per_session {
                let nodes = lb.lock().unwrap().clone();
                if nodes.is_empty() {
                    pause(STEP).await;
                    continue;
                }
                let node = nodes[st.pick(nodes.len())];
                let api = clients[&node].clone();
                one_op(&api, &mut st, &cfg, &mut rep).await;
                rep.ops += 1;
                if !cfg.think.is_zero() {
                    pause(cfg.think).await;
                }
            }
            rep
        }));
    }
    let mut out = LoadReport::default();
    for t in tasks {
        out.merge(t.await.expect("harness: session task"));
    }
    out
}

/// Classify a non-recorded answer. A forwarded call whose peer dies mid-call is a
/// retryable 503 (the router re-routes it once); a `500` such as the former
/// `internal "transport error"` is unexpected (p13-fixes regression check).
fn other_answer(rep: &mut LoadReport, what: &str, r: &Option<Resp>) {
    match r {
        None => rep.unavailable += 1,
        Some(r) if r.st == 503 || r.st == 504 => rep.unavailable += 1,
        Some(r) => rep
            .unexpected
            .push(format!("{what}: HTTP {}: {}", r.st, r.body)),
    }
}

fn run_partition(id: &str) -> u16 {
    id.parse::<ostro_types::RunId>()
        .map(|r| r.partition().0)
        .unwrap_or(0)
}

async fn one_op(api: &ApiClient, st: &mut SessionState, cfg: &LoadCfg, rep: &mut LoadReport) {
    use reqwest::Method;
    let roll = st.pick(100);
    let have_runs = !st.runs.is_empty();
    // clock-exempt: client-side latency measurement.
    let t0 = Instant::now();
    match roll {
        // Start: smoke (completes) or signals long_wait (stays running).
        _ if roll < 28 || !have_runs => {
            let signals = roll.is_multiple_of(3);
            st.n += 1;
            let key = format!("{}-k{}-{}", cfg.tag, st.s.id(), st.n);
            let (wf, input) = if signals {
                ("signals", json!({ "mode": "long_wait" }))
            } else {
                ("smoke", json!({ "k": key }))
            };
            let r = call(
                api,
                Method::POST,
                &format!("/v1/workflows/{wf}/runs"),
                Some(json!({ "input": input, "idempotency_key": key, "tags": [cfg.tag] })),
                None,
                REQ_T,
            )
            .await;
            match &r {
                Some(x) if (200..300).contains(&x.st) => {
                    let id = x.body["run_id"].as_str().unwrap_or_default().to_owned();
                    let p = run_partition(&id);
                    if let Some(t) = &x.token {
                        st.ptoken.insert(p, t.clone());
                    }
                    st.s.ack(Effect::RunExists { run_id: id.clone() });
                    st.runs.push(RunRec {
                        id,
                        p,
                        waiting: signals,
                        terminal_acked: false,
                        terminal_maybe: false,
                        signals: Vec::new(),
                    });
                    rep.writes_acked += 1;
                    rep.write_lat.push(t0.elapsed());
                }
                _ => other_answer(rep, "start", &r),
            }
        }
        // Get run.
        _ if roll < 50 => {
            let i = st.pick(st.runs.len());
            let run = st.runs[i].clone();
            let tok = cfg.tokens.then(|| st.ptoken.get(&run.p).cloned()).flatten();
            let ticket = st.s.begin_read();
            let r = call(
                api,
                Method::GET,
                &format!("/v1/runs/{}", run.id),
                None,
                tok.as_deref(),
                REQ_T,
            )
            .await;
            match &r {
                Some(x) if x.st == 200 || x.st == 404 => {
                    st.s.observe_issued(
                        ticket,
                        Observation::run_from_json(&run.id, x.st == 200, &x.body, Vec::new()),
                    );
                    rep.reads_recorded += 1;
                    rep.read_lat.push(t0.elapsed());
                }
                _ => other_answer(rep, "get run", &r),
            }
        }
        // History (found only; recorded for runs whose only acked effect is the start).
        _ if roll < 58 => {
            let i = st.pick(st.runs.len());
            let run = st.runs[i].clone();
            let tok = cfg.tokens.then(|| st.ptoken.get(&run.p).cloned()).flatten();
            let ticket = st.s.begin_read();
            let r = call(
                api,
                Method::GET,
                &format!("/v1/runs/{}/history", run.id),
                None,
                tok.as_deref(),
                REQ_T,
            )
            .await;
            match &r {
                Some(x) if x.st == 200 || x.st == 404 => {
                    if !run.terminal_acked {
                        st.s.observe_issued(
                            ticket,
                            Observation::Run {
                                run_id: run.id.clone(),
                                found: x.st == 200,
                                status: None,
                                signals: Vec::new(),
                            },
                        );
                        rep.reads_recorded += 1;
                    }
                    rep.read_lat.push(t0.elapsed());
                }
                _ => other_answer(rep, "history", &r),
            }
        }
        // Signal a waiting run (name `ryw`: it keeps waiting for `x`).
        _ if roll < 66 => {
            let waiting: Vec<usize> = (0..st.runs.len())
                .filter(|i| {
                    let r = &st.runs[*i];
                    r.waiting && !r.terminal_maybe && r.signals.len() < 3
                })
                .collect();
            if waiting.is_empty() {
                return;
            }
            let i = waiting[st.pick(waiting.len())];
            st.n += 1;
            let rid = format!("{}-r{}", st.s.id(), st.n);
            let id = st.runs[i].id.clone();
            let r = call(
                api,
                Method::POST,
                &format!("/v1/runs/{id}/signals/ryw"),
                Some(json!({ "payload": { "rid": rid }, "request_id": rid })),
                None,
                REQ_T,
            )
            .await;
            match &r {
                Some(x) if (200..300).contains(&x.st) => {
                    if let Some(t) = &x.token {
                        st.ptoken.insert(st.runs[i].p, t.clone());
                    }
                    st.sig.ack(Effect::SignalVisible {
                        run_id: id,
                        request_id: rid.clone(),
                    });
                    st.runs[i].signals.push(rid);
                    rep.writes_acked += 1;
                    rep.write_lat.push(t0.elapsed());
                }
                _ => other_answer(rep, "signal", &r),
            }
        }
        // Signal probe: every acked request id of a run must be a duplicate now.
        _ if roll < 70 => {
            let cands: Vec<usize> = (0..st.runs.len())
                .filter(|i| {
                    let r = &st.runs[*i];
                    !r.signals.is_empty() && !r.terminal_acked && !r.terminal_maybe
                })
                .collect();
            if cands.is_empty() {
                return;
            }
            let k = st.pick(cands.len());
            let run = st.runs[cands[k]].clone();
            let ticket = st.sig.begin_read();
            let mut seen = Vec::new();
            for rid in &run.signals {
                let r = call(
                    api,
                    Method::POST,
                    &format!("/v1/runs/{}/signals/ryw", run.id),
                    Some(json!({ "payload": { "rid": rid }, "request_id": rid })),
                    None,
                    REQ_T,
                )
                .await;
                match &r {
                    Some(x) if (200..300).contains(&x.st) => {
                        if x.body["duplicate"] == true {
                            seen.push(rid.clone());
                        }
                    }
                    _ => {
                        other_answer(rep, "signal probe", &r);
                        return;
                    }
                }
            }
            st.sig.observe_issued(
                ticket,
                Observation::Run {
                    run_id: run.id.clone(),
                    found: true,
                    status: None,
                    signals: seen,
                },
            );
            rep.reads_recorded += 1;
        }
        // Terminate a waiting run (hard stop: terminal at acknowledgement).
        _ if roll < 74 => {
            let waiting: Vec<usize> = (0..st.runs.len())
                .filter(|i| st.runs[*i].waiting && !st.runs[*i].terminal_maybe)
                .collect();
            if waiting.is_empty() {
                return;
            }
            let i = waiting[st.pick(waiting.len())];
            let id = st.runs[i].id.clone();
            st.runs[i].terminal_maybe = true;
            let r = call(
                api,
                Method::POST,
                &format!("/v1/runs/{id}/terminate"),
                Some(json!({ "reason": "ryw" })),
                None,
                REQ_T,
            )
            .await;
            match &r {
                Some(x) if (200..300).contains(&x.st) => {
                    if let Some(t) = &x.token {
                        st.ptoken.insert(st.runs[i].p, t.clone());
                    }
                    st.s.ack(Effect::RunTerminal { run_id: id });
                    st.runs[i].terminal_acked = true;
                    st.runs[i].waiting = false;
                    rep.writes_acked += 1;
                    rep.write_lat.push(t0.elapsed());
                }
                _ => other_answer(rep, "terminate", &r),
            }
        }
        // Queue policy write (one key per session).
        _ if roll < 82 => {
            let hours = 1 + st.pick(500) as u64;
            let r = call(
                api,
                Method::PUT,
                &format!("/v1/queues/{}/policy", st.queue),
                Some(json!({ "retention": format!("{hours}h") })),
                None,
                REQ_T,
            )
            .await;
            match &r {
                Some(x) if (200..300).contains(&x.st) => {
                    if x.token.is_some() {
                        st.mtoken = x.token.clone();
                    }
                    st.s.ack(Effect::Policy {
                        key: st.queue.clone(),
                        value: x.body["retention_ms"].clone(),
                    });
                    rep.writes_acked += 1;
                    rep.write_lat.push(t0.elapsed());
                }
                _ => other_answer(rep, "put policy", &r),
            }
        }
        // Queue policy read.
        _ if roll < 90 => {
            let tok = cfg.tokens.then(|| st.mtoken.clone()).flatten();
            let ticket = st.s.begin_read();
            let r = call(
                api,
                Method::GET,
                &format!("/v1/queues/{}/policy", st.queue),
                None,
                tok.as_deref(),
                REQ_T,
            )
            .await;
            match &r {
                Some(x) if x.st == 200 => {
                    let v = &x.body["retention_ms"];
                    st.s.observe_issued(
                        ticket,
                        Observation::Policy {
                            key: st.queue.clone(),
                            value: (!v.is_null()).then(|| v.clone()),
                        },
                    );
                    rep.reads_recorded += 1;
                    rep.read_lat.push(t0.elapsed());
                }
                _ => other_answer(rep, "get policy", &r),
            }
        }
        // List runs (load; fans out).
        _ if roll < 94 => {
            let r = call(
                api,
                Method::GET,
                &format!("/v1/runs?tag={}&limit=10", cfg.tag),
                None,
                None,
                REQ_T,
            )
            .await;
            match &r {
                Some(x) if x.st == 200 => rep.read_lat.push(t0.elapsed()),
                _ => other_answer(rep, "list runs", &r),
            }
        }
        // Activate smoke v1.
        _ if roll < 97 => {
            let r = call(
                api,
                Method::PUT,
                "/v1/workflows/smoke/active",
                Some(json!({ "version": 1 })),
                None,
                REQ_T,
            )
            .await;
            match &r {
                Some(x) if (200..300).contains(&x.st) => {
                    if x.token.is_some() {
                        st.mtoken = x.token.clone();
                    }
                    st.s.ack(Effect::ActiveVersion {
                        workflow: "smoke".into(),
                        version: 1,
                    });
                    rep.writes_acked += 1;
                    rep.write_lat.push(t0.elapsed());
                }
                _ => other_answer(rep, "activate", &r),
            }
        }
        // List workflows (active version).
        _ => {
            let tok = cfg.tokens.then(|| st.mtoken.clone()).flatten();
            let ticket = st.s.begin_read();
            let r = call(
                api,
                Method::GET,
                "/v1/workflows",
                None,
                tok.as_deref(),
                REQ_T,
            )
            .await;
            match &r {
                Some(x) if x.st == 200 => {
                    let list = x.body.get("workflows").cloned().unwrap_or(x.body.clone());
                    let version = list.as_array().and_then(|a| {
                        a.iter()
                            .find(|w| w["name"] == "smoke")
                            .and_then(|w| w["active_version"].as_u64())
                            .map(|v| v as u32)
                    });
                    st.s.observe_issued(
                        ticket,
                        Observation::ActiveVersion {
                            workflow: "smoke".into(),
                            version,
                        },
                    );
                    rep.reads_recorded += 1;
                    rep.read_lat.push(t0.elapsed());
                }
                _ => other_answer(rep, "list workflows", &r),
            }
        }
    }
}

// ---------------------------------------------------------------------------------
// Scenario plumbing
// ---------------------------------------------------------------------------------

fn clients(c: &InProcCluster) -> BTreeMap<u64, ApiClient> {
    c.node_ids().into_iter().map(|n| (n, c.client(n))).collect()
}

fn count(c: &InProcCluster) -> PartitionCount {
    PartitionCount::new(c.spec().partitions).expect("partitions > 0")
}

fn tag(prefix: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    format!("{prefix}{}", N.fetch_add(1, Ordering::Relaxed))
}

fn load_cfg(c: &InProcCluster, tagp: &str, ops_total: usize, tokens: bool) -> LoadCfg {
    LoadCfg {
        sessions: SESSIONS,
        ops_per_session: ops_total.div_ceil(SESSIONS),
        tokens,
        tag: tag(tagp),
        seed: c.spec().seed,
        think: Duration::ZERO,
    }
}

/// The checker's verdict (0 violations), with the workload summary in the detail.
fn verdict(name: &str, checker: &RywChecker, rep: &mut LoadReport) -> ScenarioResult {
    let r = checker.check();
    let summary = rep.summary();
    println!("{name}: {summary}");
    if !r.is_ok() {
        let shown: Vec<&String> = r.violations.iter().take(10).collect();
        return Err(fail(
            "p13_ryw",
            format!("{} stale reads ({summary}): {shown:?}", r.violations.len()),
        ));
    }
    if rep.reads_recorded == 0 || rep.writes_acked == 0 {
        return Err(fail(
            "p13_ryw",
            format!("the workload measured nothing ({summary})"),
        ));
    }
    Ok(())
}

/// Fail on answers that are neither success nor availability errors.
fn no_unexpected(rep: &LoadReport) -> ScenarioResult {
    if rep.unexpected.is_empty() {
        return Ok(());
    }
    let shown: Vec<&String> = rep.unexpected.iter().take(10).collect();
    Err(fail(
        "availability",
        format!("{} unexpected answers: {shown:?}", rep.unexpected.len()),
    ))
}

/// Run a full RF=3 ISR everywhere before measuring.
async fn full_isr(c: &InProcCluster) -> ScenarioResult {
    for p in 0..c.spec().partitions {
        wait_assignment(c, p, "full ISR", CHANGE_T * 2, |a| {
            a.leader != 0 && a.isr.len() == 3
        })
        .await?;
    }
    Ok(())
}

async fn prepare(c: &InProcCluster) -> ScenarioResult {
    deploy(c).await?;
    full_isr(c).await
}

/// `NodeOps.HandOff` of `p` from `leader` to `to` (as node `caller`).
async fn hand_off(
    c: &InProcCluster,
    caller: u64,
    leader: u64,
    p: u16,
    to: u64,
    epoch: u32,
) -> Result<(), String> {
    let mut client = NodeOpsClient::new(c.channel_as(caller, leader));
    let req = HandOffRequest {
        partition: u32::from(p),
        to,
        epoch,
    };
    match tokio::time::timeout(Duration::from_secs(60), client.hand_off(req)).await {
        Ok(Ok(r)) => {
            let r = r.into_inner();
            if r.moved {
                Ok(())
            } else {
                Err(format!("not moved: {r:?}"))
            }
        }
        Ok(Err(s)) => Err(format!("{:?}: {}", s.code(), s.message())),
        Err(_) => Err("no answer".into()),
    }
}

/// `ResignLeader` of `p` by its leader.
async fn resign(c: &InProcCluster, leader: u64, p: u16, epoch: u32) {
    let h = c
        .handle(leader)
        .and_then(|h| h.p13())
        .cloned()
        .expect("harness: leader running");
    h.cluster
        .assignments()
        .resign(PartitionId(p), Epoch(epoch))
        .await;
}

// ---------------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------------

fn ryw_across_nodes_steady(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        prepare(c).await?;
        let checker = RywChecker::new();
        let lb: Lb = Arc::new(Mutex::new(c.node_ids()));
        let cfg = load_cfg(c, "steady", 2000, false);
        let mut rep = run_load(clients(c), lb, cfg, checker.clone()).await;
        no_unexpected(&rep)?;
        verdict("ryw_across_nodes_steady", &checker, &mut rep)
    })
}

fn ryw_across_nodes_through_moves(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        prepare(c).await?;
        let checker = RywChecker::new();
        let lb: Lb = Arc::new(Mutex::new(c.node_ids()));
        let cfg = load_cfg(c, "moves", 1500, false);
        let load = tokio::spawn(run_load(clients(c), lb, cfg, checker.clone()));
        let (mut handoffs, mut resigns, mut refused) = (0usize, 0usize, Vec::new());
        let mut i = 0usize;
        while !load.is_finished() {
            i += 1;
            let p = (i % c.spec().partitions as usize) as u16;
            let a = published(c, p);
            if a.leader == 0 || a.isr.len() < 3 {
                pause(STEP * 4).await;
                continue;
            }
            if i.is_multiple_of(2) {
                let to = *a.isr.iter().find(|n| **n != a.leader).expect("isr of 3");
                let caller = meta_leader(c).unwrap_or(a.leader);
                match hand_off(c, caller, a.leader, p, to, a.epoch).await {
                    Ok(()) => handoffs += 1,
                    Err(e) => refused.push(format!("p{p}: {e}")),
                }
            } else {
                resign(c, a.leader, p, a.epoch).await;
                resigns += 1;
            }
            let _ = wait_assignment(c, p, "re-led", CHANGE_T, |x| {
                x.leader != 0 && x.epoch > a.epoch
            })
            .await;
            pause(Duration::from_millis(400)).await;
        }
        let mut rep = load.await.expect("harness: load task");
        println!(
            "ryw_across_nodes_through_moves: {handoffs} handoffs, {resigns} resignations, {} refused handoffs {refused:?}",
            refused.len()
        );
        if handoffs + resigns < 4 {
            return Err(fail(
                "handoff",
                format!("only {handoffs} handoffs and {resigns} resignations happened"),
            ));
        }
        no_unexpected(&rep)?;
        verdict("ryw_across_nodes_through_moves", &checker, &mut rep)
    })
}

fn ryw_through_failover(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        prepare(c).await?;
        let meta = wait_meta_leader(c).await?;
        let a = published_all(c)
            .into_iter()
            .find(|a| a.leader != 0 && a.leader != meta)
            .ok_or_else(|| fail("placement", "no partition led off the meta leader"))?;
        let victim = a.leader;
        let checker = RywChecker::new();
        let lb: Lb = Arc::new(Mutex::new(c.node_ids()));
        let cfg = load_cfg(c, "failover", 1500, false);
        let load = tokio::spawn(run_load(clients(c), Arc::clone(&lb), cfg, checker.clone()));
        pause(Duration::from_millis(1500)).await;
        // The load balancer stops sending to the victim, then it dies.
        lb.lock().unwrap().retain(|n| *n != victim);
        c.kill(victim).await;
        wait_assignment(c, a.partition, "failed over", CHANGE_T, |x| {
            x.leader != 0 && x.leader != victim && x.epoch > a.epoch
        })
        .await?;
        let mut rep = load.await.expect("harness: load task");
        no_unexpected(&rep)?;
        verdict("ryw_through_failover", &checker, &mut rep)
    })
}

/// Set up the probe-paused zombie (module docs): partition `p` led by `zombie` (a meta
/// follower). Returns the cut instant.
async fn make_zombie(c: &InProcCluster, zombie: u64) -> Instant {
    let meta = meta_leader(c).expect("harness: meta leader");
    let faults = c.net().faults();
    // Nothing the zombie sends arrives anywhere (raft, ISR publication, forwards, fetches).
    for x in c.node_ids().into_iter().filter(|n| *n != zombie) {
        faults.set(zombie, x, RpcKind::Any, Fault::Cut);
    }
    // clock-exempt: test measurement
    let cut = Instant::now();
    // A short blip of the meta leader's stream to the zombie: the leader marks it
    // unreachable (raft probe state); after the reconnect it sends one probe append and,
    // with no answer ever arriving, stays paused (heartbeats keep the zombie's contact).
    pause(Duration::from_millis(50)).await;
    faults.set(meta, zombie, RpcKind::Any, Fault::Cut);
    pause(Duration::from_millis(120)).await;
    faults.clear_link(meta, zombie);
    cut
}

/// The zombie scenario body; `mutant` = run it under `serve_without_lease`.
async fn zombie_body(c: &mut InProcCluster, mutant: bool) -> ScenarioResult {
    prepare(c).await?;
    let meta = wait_meta_leader(c).await?;
    let a = published_all(c)
        .into_iter()
        .find(|a| a.leader != 0 && a.leader != meta && a.isr.len() == 3)
        .ok_or_else(|| fail("placement", "no partition led off the meta leader"))?;
    let (p, zombie) = (a.partition, a.leader);
    let others: Vec<u64> = c.node_ids().into_iter().filter(|n| *n != zombie).collect();
    let zapi = c.client(zombie);
    // A run on p the zombie has (the probe target).
    let probe_run = {
        let key = super::placement::support::key_for(c, p, "zprobe");
        let r = call(
            &c.client(others[0]),
            reqwest::Method::POST,
            "/v1/workflows/smoke/runs",
            Some(json!({ "input": {}, "idempotency_key": key })),
            None,
            REQ_T,
        )
        .await
        .ok_or_else(|| fail("availability", "probe run not started"))?;
        r.body["run_id"].as_str().unwrap_or_default().to_owned()
    };
    // The zombie serves it now.
    let ok = poll_async(CHANGE_T, || {
        let (zapi, id) = (zapi.clone(), probe_run.clone());
        async move {
            let r = call(
                &zapi,
                reqwest::Method::GET,
                &format!("/v1/runs/{id}"),
                None,
                None,
                REQ_T,
            )
            .await?;
            (r.st == 200).then_some(())
        }
    })
    .await;
    if ok.is_none() {
        return Err(fail("availability", "the leader does not serve its run"));
    }
    #[cfg(feature = "failpoints")]
    if mutant {
        harness(
            "mutant",
            fail::cfg(
                ostro_server::route::consistency::MUTANT_SERVE_WITHOUT_LEASE,
                "return",
            ),
        );
    }
    #[cfg(not(feature = "failpoints"))]
    let _ = mutant;

    let cut = make_zombie(c, zombie).await;
    // Probe the zombie: when does it stop serving?
    let probe = {
        let (zapi, id) = (zapi.clone(), probe_run.clone());
        tokio::spawn(async move {
            let mut last_served: Option<Duration> = None;
            let mut first_refused: Option<Duration> = None;
            let end = cut + Duration::from_secs(6);
            while Instant::now() < end {
                let sent = Instant::now();
                let r = call(
                    &zapi,
                    reqwest::Method::GET,
                    &format!("/v1/runs/{id}"),
                    None,
                    None,
                    Duration::from_secs(5),
                )
                .await;
                match r {
                    Some(x) if x.st == 200 => last_served = Some(sent - cut),
                    Some(x) if x.st == 503 => {
                        first_refused.get_or_insert(sent - cut);
                    }
                    _ => {}
                }
                pause(Duration::from_millis(20)).await;
            }
            (last_served, first_refused)
        })
    };
    // A successor is elected (the zombie is silent).
    let moved = wait_assignment(c, p, "successor elected", CHANGE_T, |x| {
        x.leader != 0 && x.leader != zombie && x.epoch > a.epoch
    })
    .await;
    // Sessions write through the other nodes and read through every node.
    let checker = RywChecker::new();
    let lb: Lb = Arc::new(Mutex::new(c.node_ids()));
    let mut cfg = load_cfg(c, "zombie", 600, false);
    cfg.sessions = 4;
    let load = run_load(clients(c), Arc::clone(&lb), cfg, checker.clone());
    // Plus write-then-read pairs on p: start via another node, read via the zombie.
    let pairs = async {
        let s = checker.session("zombie-pairs");
        let mut n = 0usize;
        for i in 0..40 {
            let key = super::placement::support::key_for(c, p, "zpair");
            let r = call(
                &c.client(others[i % others.len()]),
                reqwest::Method::POST,
                "/v1/workflows/smoke/runs",
                Some(json!({ "input": {}, "idempotency_key": key })),
                None,
                REQ_T,
            )
            .await;
            let Some(r) = r.filter(|r| (200..300).contains(&r.st)) else {
                continue;
            };
            let id = r.body["run_id"].as_str().unwrap_or_default().to_owned();
            s.ack(Effect::RunExists { run_id: id.clone() });
            let t = s.begin_read();
            if let Some(x) = call(
                &zapi,
                reqwest::Method::GET,
                &format!("/v1/runs/{id}"),
                None,
                None,
                REQ_T,
            )
            .await
                && (x.st == 200 || x.st == 404)
            {
                s.observe_issued(
                    t,
                    Observation::run_from_json(&id, x.st == 200, &x.body, vec![]),
                );
                n += 1;
            }
        }
        n
    };
    let (mut rep, zombie_reads) = tokio::join!(load, pairs);
    let (last_served, first_refused) = probe.await.expect("harness: probe task");
    #[cfg(feature = "failpoints")]
    if mutant {
        fail::remove(ostro_server::route::consistency::MUTANT_SERVE_WITHOUT_LEASE);
    }
    c.net().faults().heal(zombie);
    let moved = moved?;
    let limit = SESSION.mul_f64(1.0 - MARGIN);
    println!(
        "zombie (mutant {mutant}): p{p} leader {zombie} -> {} (epoch {} -> {}); zombie last served +{last_served:?}, first refused +{first_refused:?} after the cut (lease {limit:?}); {zombie_reads} reads answered by the zombie after writes elsewhere",
        moved.leader, a.epoch, moved.epoch
    );
    if !rep.unexpected.is_empty() {
        println!(
            "zombie (mutant {mutant}): unexpected answers (the zombie's own failed writes): {:?}",
            rep.unexpected.iter().take(5).collect::<Vec<_>>()
        );
    }
    let report = checker.check();
    if mutant {
        return if report.is_ok() {
            Err(fail(
                "mutant",
                format!(
                    "route::mutant::serve_without_lease did not bite: 0 stale reads ({}; zombie reads {zombie_reads}, last served +{last_served:?})",
                    rep.summary()
                ),
            ))
        } else {
            println!(
                "mutant bit: {} stale reads, e.g. {:?}",
                report.violations.len(),
                report.violations.first()
            );
            Ok(())
        };
    }
    if let Some(ls) = last_served {
        // The bounded anchor wait (a quarter of the lease) + one probe round.
        let bound = limit + limit / 4 + Duration::from_millis(150);
        if ls > bound {
            return Err(fail(
                "lease",
                format!("the zombie served a read +{ls:?} after the cut (bound {bound:?})"),
            ));
        }
    }
    if first_refused.is_none() {
        return Err(fail("lease", "the zombie never refused a read"));
    }
    verdict("zombie_leader_never_serves_stale", &checker, &mut rep)
}

/// An idempotency key whose `signals` start lands on `p` (the start path's own rule).
fn signals_key_for(c: &InProcCluster, p: u16, tag: &str) -> String {
    let n = count(c);
    (0u64..)
        .map(|i| format!("{tag}-{}-{i}", c.cluster_id()))
        .find(|k| {
            ostro_server::route::table::key_partition("signals", None, k, n)
                .is_some_and(|q| q.0 == p)
        })
        .expect("some key lands on every partition")
}

/// Runs started through the successor, written to through the zombie (p13-fixes): the
/// zombie's run record does not have them, and a write that read it before proposing
/// answered `404 not_found` for the client's own acknowledged run. With the write hook
/// (leading + lease) the zombie refuses, the router re-routes once (the zombie's view never
/// moves) and answers `503` + `Retry-After`.
async fn zombie_writes_body(c: &mut InProcCluster) -> ScenarioResult {
    use reqwest::Method;
    prepare(c).await?;
    let meta = wait_meta_leader(c).await?;
    let a = published_all(c)
        .into_iter()
        .find(|a| a.leader != 0 && a.leader != meta && a.isr.len() == 3)
        .ok_or_else(|| fail("placement", "no partition led off the meta leader"))?;
    let (p, zombie) = (a.partition, a.leader);
    let others: Vec<u64> = c.node_ids().into_iter().filter(|n| *n != zombie).collect();
    let zapi = c.client(zombie);
    make_zombie(c, zombie).await;
    let moved = wait_assignment(c, p, "successor elected", CHANGE_T, |x| {
        x.leader != 0 && x.leader != zombie && x.epoch > a.epoch
    })
    .await;
    let out = async {
        let moved = moved?;
        // Waiting runs on p, started (and acknowledged) through the other nodes.
        let mut runs = Vec::new();
        for i in 0..4 {
            let key = signals_key_for(c, p, &format!("zwrite{i}"));
            let started = poll_async(CHANGE_T, || {
                let (api, key) = (c.client(others[i % others.len()]), key.clone());
                async move {
                    let r = call(
                        &api,
                        Method::POST,
                        "/v1/workflows/signals/runs",
                        Some(json!({ "input": { "mode": "long_wait" }, "idempotency_key": key })),
                        None,
                        REQ_T,
                    )
                    .await?;
                    ((200..300).contains(&r.st))
                        .then(|| r.body["run_id"].as_str().unwrap_or_default().to_owned())
                }
            })
            .await
            .ok_or_else(|| fail("availability", "start through the successor"))?;
            if run_partition(&started) != p {
                return Err(fail("harness", format!("{started} is not on p{p}")));
            }
            runs.push(started);
        }
        let (mut refused, mut answered) = (0usize, Vec::new());
        for (i, id) in runs.iter().enumerate() {
            let writes = [
                (
                    "signal",
                    format!("/v1/runs/{id}/signals/ryw"),
                    json!({ "payload": { "rid": i }, "request_id": format!("zw-{i}") }),
                ),
                ("cancel", format!("/v1/runs/{id}/cancel"), json!({})),
                ("retry", format!("/v1/runs/{id}/retry"), json!({})),
                (
                    "terminate",
                    format!("/v1/runs/{id}/terminate"),
                    json!({ "reason": "zombie" }),
                ),
            ];
            for (what, path, body) in writes {
                let r = call(&zapi, Method::POST, &path, Some(body), None, REQ_T).await;
                match r {
                    Some(x) if x.st == 503 => {
                        refused += 1;
                        if x.body["error"]["details"]["retry_after_ms"].as_u64().is_none() {
                            return Err(fail(
                                "availability",
                                format!("{what} via the zombie: 503 without retry_after_ms: {}", x.body),
                            ));
                        }
                    }
                    Some(x) if x.st == 404 => {
                        return Err(fail(
                            "zombie_write",
                            format!(
                                "{what} of {id} via the zombie answered 404 for a run acknowledged through node {} (epoch {}): {}",
                                moved.leader, moved.epoch, x.body
                            ),
                        ));
                    }
                    Some(x) => answered.push(format!("{what}: {} {}", x.st, x.body)),
                    None => answered.push(format!("{what}: no answer")),
                }
            }
        }
        println!(
            "zombie writes: p{p} leader {zombie} -> {} (epoch {} -> {}); {refused} of {} writes via the zombie refused with 503; other answers {answered:?}",
            moved.leader,
            a.epoch,
            moved.epoch,
            runs.len() * 4
        );
        if refused == 0 {
            return Err(fail(
                "harness",
                format!("the zombie never refused a write (no zombie window?): {answered:?}"),
            ));
        }
        // Every other answer is from a write that reached the successor (the zombie's view
        // caught up): never a 5xx other than 503.
        if let Some(bad) = answered.iter().find(|a| a.contains(": 5")) {
            return Err(fail("availability", format!("unexpected answer: {bad}")));
        }
        Ok(())
    }
    .await;
    c.net().faults().heal(zombie);
    out
}

fn zombie_writes_are_retryable(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(zombie_writes_body(c))
}

fn zombie_leader_never_serves_stale(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(zombie_body(c, false))
}

#[cfg(feature = "failpoints")]
fn zombie_serve_without_lease_mutant_bites(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(zombie_body(c, true))
}

fn tokens_skip_barrier(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        prepare(c).await?;
        let lb: Lb = Arc::new(Mutex::new(c.node_ids()));
        let stats = ostro_server::route::consistency::stats;
        // Without tokens.
        let plain = RywChecker::new();
        let b0 = stats().barriers;
        let mut rep_plain = run_load(
            clients(c),
            Arc::clone(&lb),
            load_cfg(c, "notok", 900, false),
            plain.clone(),
        )
        .await;
        let barriers_plain = stats().barriers - b0;
        // With tokens.
        let tok = RywChecker::new();
        let b1 = stats().barriers;
        let s1 = stats().token_skips;
        let mut rep_tok = run_load(
            clients(c),
            Arc::clone(&lb),
            load_cfg(c, "tok", 900, true),
            tok.clone(),
        )
        .await;
        let barriers_tok = stats().barriers - b1;
        let skips = stats().token_skips - s1;
        println!(
            "tokens_skip_barrier: barriers without tokens {barriers_plain}, with tokens {barriers_tok} ({skips} skipped)"
        );
        no_unexpected(&rep_plain)?;
        no_unexpected(&rep_tok)?;
        verdict("tokens_skip_barrier (no tokens)", &plain, &mut rep_plain)?;
        verdict("tokens_skip_barrier (tokens)", &tok, &mut rep_tok)?;
        if barriers_tok >= barriers_plain || skips == 0 {
            return Err(fail(
                "tokens",
                format!(
                    "tokens did not save barriers: {barriers_tok} with vs {barriers_plain} without ({skips} skips)"
                ),
            ));
        }
        Ok(())
    })
}

/// `await_result` at a non-leader across a move of the run's partition.
async fn await_across(c: &mut InProcCluster, handoff: bool) -> ScenarioResult {
    prepare(c).await?;
    let a = published_all(c)
        .into_iter()
        .find(|a| a.leader != 0 && a.isr.len() == 3)
        .ok_or_else(|| fail("placement", "no partition with a full ISR"))?;
    let (p, leader) = (a.partition, a.leader);
    let caller = *c
        .node_ids()
        .iter()
        .find(|n| **n != leader)
        .expect("three nodes");
    let capi = c.client(caller);
    // A waiting run on p.
    let key = super::placement::support::key_for(c, p, "await");
    let count = count(c);
    let key = if ostro_types::routing::partition_for_key("signals", key.as_bytes(), count).0 == p {
        key
    } else {
        // key_for routes the smoke workflow; find a key for `signals` on p.
        let mut k = 0u64;
        loop {
            k += 1;
            let cand = format!("await-{}-{k}", c.cluster_id());
            if ostro_types::routing::partition_for_key("signals", cand.as_bytes(), count).0 == p {
                break cand;
            }
        }
    };
    let r = call(
        &capi,
        reqwest::Method::POST,
        "/v1/workflows/signals/runs",
        Some(json!({ "input": { "mode": "long_wait" }, "idempotency_key": key })),
        None,
        REQ_T,
    )
    .await
    .filter(|r| (200..300).contains(&r.st))
    .ok_or_else(|| fail("availability", "waiting run not started"))?;
    let run = r.body["run_id"].as_str().unwrap_or_default().to_owned();
    if run_partition(&run) != p {
        return Err(fail("placement", format!("run {run} not on partition {p}")));
    }
    // The long-poll, issued at the non-leader before the move.
    let wait = Duration::from_secs(30);
    let poll_task = {
        let (capi, run) = (capi.clone(), run.clone());
        tokio::spawn(async move {
            let t0 = Instant::now();
            let r = call(
                &capi,
                reqwest::Method::GET,
                &format!("/v1/runs/{run}/result?wait=30s"),
                None,
                None,
                wait + Duration::from_secs(15),
            )
            .await;
            (t0.elapsed(), r)
        })
    };
    pause(Duration::from_millis(500)).await;
    let a = published(c, p);
    if handoff {
        let to = *a.isr.iter().find(|n| **n != a.leader).expect("isr of 3");
        let caller_node = meta_leader(c).unwrap_or(a.leader);
        hand_off(c, caller_node, a.leader, p, to, a.epoch)
            .await
            .map_err(|e| fail("handoff", format!("p{p}: {e}")))?;
    } else {
        resign(c, a.leader, p, a.epoch).await;
    }
    let moved = wait_assignment(c, p, "moved", CHANGE_T, |x| {
        x.leader != 0 && x.epoch > a.epoch
    })
    .await?;
    pause(Duration::from_millis(300)).await;
    // Complete the run (signal `x`) through any node, retried across the move.
    let done = poll_async(CHANGE_T, || {
        let (api, run) = (c.client(moved.leader), run.clone());
        async move {
            let r = call(
                &api,
                reqwest::Method::POST,
                &format!("/v1/runs/{run}/signals/x"),
                Some(json!({ "payload": "done", "request_id": "await-x" })),
                None,
                REQ_T,
            )
            .await?;
            (200..300).contains(&r.st).then_some(())
        }
    })
    .await;
    if done.is_none() {
        return Err(fail(
            "availability",
            "the completing signal was not accepted",
        ));
    }
    let (took, r) = poll_task.await.expect("harness: long-poll task");
    let name = if handoff {
        "await_across_handoff"
    } else {
        "await_across_leader_change"
    };
    let r = r.ok_or_else(|| fail("await", format!("{name}: no answer after {took:?}")))?;
    println!(
        "{name}: p{p} {} -> {} (epoch {} -> {}), caller {caller}: HTTP {} after {took:?}: {}",
        a.leader, moved.leader, a.epoch, moved.epoch, r.st, r.body
    );
    if r.st != 200 || r.body["completed"] != true || r.body["result"] != "done" {
        return Err(fail(
            "await",
            format!("{name}: HTTP {} {} after {took:?}", r.st, r.body),
        ));
    }
    if took >= wait {
        return Err(fail(
            "await",
            format!("{name}: answered after {took:?}, past the {wait:?} deadline"),
        ));
    }
    Ok(())
}

fn await_across_handoff(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(await_across(c, true))
}

fn await_across_leader_change(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(await_across(c, false))
}

fn zombie_asymmetric(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        prepare(c).await?;
        let meta = wait_meta_leader(c).await?;
        let a = published_all(c)
            .into_iter()
            .find(|a| a.leader != 0 && a.leader != meta && a.isr.len() == 3)
            .ok_or_else(|| fail("placement", "no partition led off the meta leader"))?;
        let (p, zombie) = (a.partition, a.leader);
        let zapi = c.client(zombie);
        let key = super::placement::support::key_for(c, p, "asym");
        let probe_run = call(
            &zapi,
            reqwest::Method::POST,
            "/v1/workflows/smoke/runs",
            Some(json!({ "input": {}, "idempotency_key": key })),
            None,
            REQ_T,
        )
        .await
        .filter(|r| (200..300).contains(&r.st))
        .ok_or_else(|| fail("availability", "probe run not started"))?
        .body["run_id"]
            .as_str()
            .unwrap_or_default()
            .to_owned();
        // Nothing reaches the zombie on links the others open (their metadata messages,
        // i.e. the answers to its lease rounds, and their fetches); it still sends
        // everything (and gets the answers of the RPCs it opens itself).
        let faults = c.net().faults();
        for x in c.node_ids().into_iter().filter(|n| *n != zombie) {
            faults.set(x, zombie, RpcKind::Any, Fault::Cut);
        }
        let cut = Instant::now();
        let checker = RywChecker::new();
        let lb: Lb = Arc::new(Mutex::new(c.node_ids()));
        let mut cfg = load_cfg(c, "asym", 600, false);
        cfg.sessions = 4;
        let load = run_load(clients(c), lb, cfg, checker.clone());
        let probe = async {
            let mut last_served = None;
            let mut first_refused = None;
            while Instant::now() < cut + Duration::from_secs(5) {
                let sent = Instant::now();
                match call(
                    &zapi,
                    reqwest::Method::GET,
                    &format!("/v1/runs/{probe_run}"),
                    None,
                    None,
                    Duration::from_secs(5),
                )
                .await
                {
                    Some(x) if x.st == 200 => last_served = Some(sent - cut),
                    Some(x) if x.st == 503 => {
                        first_refused.get_or_insert((sent - cut, sent.elapsed(), x.body));
                    }
                    _ => {}
                }
                pause(Duration::from_millis(20)).await;
            }
            (last_served, first_refused)
        };
        let (mut rep, (last_served, first_refused)) = tokio::join!(load, probe);
        // A successor, if any, came only after the zombie stopped serving.
        let after = published(c, p);
        let successor = after.leader != zombie && after.leader != 0 && after.epoch > a.epoch;
        c.net().faults().heal(zombie);
        let limit = SESSION.mul_f64(1.0 - MARGIN);
        println!(
            "zombie_asymmetric: p{p} leader {zombie}: last served +{last_served:?}, first refused +{first_refused:?} (lease {limit:?}); successor elected: {successor} ({} at epoch {})",
            after.leader, after.epoch
        );
        let bound = limit + limit / 4 + Duration::from_millis(150);
        if let Some(ls) = last_served
            && ls > bound
        {
            return Err(fail(
                "lease",
                format!(
                    "the zombie served a read +{ls:?} after losing meta acks (bound {bound:?})"
                ),
            ));
        }
        if first_refused.is_none() {
            return Err(fail("lease", "the zombie never refused a read"));
        }
        verdict("zombie_asymmetric", &checker, &mut rep)
    })
}

fn strict_reads_mode(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        // Every node restarts with strict reads (one at a time, full ISR in between).
        for n in c.node_ids() {
            c.config_mut(n).api.strict_reads = true;
            harness(&format!("restart {n}"), c.restart(n).await);
            wait_meta_leader(c).await?;
            let up = poll(CHANGE_T * 2, || {
                published_all(c)
                    .iter()
                    .all(|a| a.leader != 0 && a.isr.len() == 3)
                    .then_some(())
            })
            .await;
            if up.is_none() {
                return Err(fail(
                    "availability",
                    format!("no full ISR after restarting {n}"),
                ));
            }
        }
        prepare(c).await?;
        let checker = RywChecker::new();
        let lb: Lb = Arc::new(Mutex::new(c.node_ids()));
        let b0 = ostro_server::route::consistency::stats().barriers;
        let mut rep = run_load(
            clients(c),
            lb,
            load_cfg(c, "strict", 1200, false),
            checker.clone(),
        )
        .await;
        let barriers = ostro_server::route::consistency::stats().barriers - b0;
        println!("strict_reads_mode: {barriers} barriers");
        no_unexpected(&rep)?;
        verdict("strict_reads_mode", &checker, &mut rep)
    })
}
