//! The Jepsen workload (14-09, D-P14-19): N client tasks (default 8) drive the crash-saga
//! guest (`guests/crash-saga`, the 05-19 crash-campaign workflow) through random nodes
//! (Phase 13 any-node routing), and every request is recorded in the [`History`] with its
//! outcome and, for acknowledged writes, the `ostro-consistency` token.
//!
//! Each client loops over fresh runs. A run's input is a pure function of the seed and its
//! idempotency key ([`plan_input`]), so its expected result is known up front
//! ([`expected_output`]):
//!
//! 1. **start** with the idempotency key, retried with the same key (and input) until it is
//!    acknowledged or [`START_GIVE_UP`] passes;
//! 2. runs without a fatal step get the `go` and `extra` signals (fixed request ids, embedded
//!    in the payload), each retried until acknowledged or answered 409 (`run_terminal`);
//!    [`CANCEL_PCT`] percent get a cancel instead of `extra` (a cancelled run is then allowed
//!    to end `cancelled`);
//! 3. **result** polls until the run is terminal or [`POLL_GIVE_UP`] passes (the quiescence
//!    phase of the run checks every acknowledged run anyway).
//!
//! Outcomes: 2xx = `ok`; a 4xx answer or a request that never connected = `fail`; a timeout,
//! a broken connection or a 5xx = `info` (the write may have been applied). With a bearer
//! token (`OSTRO_JEPSEN_TOKEN`, 14-22 with auth on) every request carries
//! `Authorization: Bearer <token>`.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ostro_repl_conformance::harness::ApiClient;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde_json::{Value, json};
use tokio::task::JoinSet;

use crate::checker::fnv64;
use crate::history::{ClientOp, History, OpKind, Outcome};

/// The workflow (and guest) name.
pub const WORKFLOW: &str = "crash-saga";
/// Default client tasks.
pub const DEFAULT_CLIENTS: usize = 8;
/// Bound on one request.
pub const OP_TIMEOUT: Duration = Duration::from_secs(5);
/// Bound on the retries of one start (same key) or one signal (same request id).
pub const START_GIVE_UP: Duration = Duration::from_secs(30);
/// Bound on a client's result polls of one run.
pub const POLL_GIVE_UP: Duration = Duration::from_secs(20);
/// Percent of signalled runs that get a cancel instead of the `extra` signal.
pub const CANCEL_PCT: u32 = 3;
/// Pause between retries / polls.
const RETRY_PAUSE: Duration = Duration::from_millis(200);

/// The input of run `key` (deterministic in `seed` and `key`): 3..=5 steps, retryable
/// failures `'0'..'2'`, one fatal `'x'` step in 8% of runs, a durable sleep (10% beyond the
/// hot-wait threshold), signal waits that never time out.
pub fn plan_input(seed: u64, key: &str) -> Value {
    let mut r = StdRng::seed_from_u64(seed ^ fnv64(key.as_bytes()));
    let steps: usize = r.random_range(3..=5);
    let mut pattern: Vec<char> = (0..steps)
        .map(|_| match r.random_range(0..100) {
            0..70 => '0',
            70..90 => '1',
            _ => '2',
        })
        .collect();
    if r.random_range(0..100) < 8 {
        let k = r.random_range(0..steps);
        pattern[k] = 'x';
    }
    let sleep_ms: u64 = if r.random_range(0..100) < 10 {
        r.random_range(1500..2200)
    } else {
        r.random_range(10..300)
    };
    json!({
        "id": key,
        "steps": steps,
        "fail_pattern": pattern.into_iter().collect::<String>(),
        "sleep_ms": sleep_ms,
        "signal_timeout_ms": 600_000,
    })
}

/// Whether the run of `input` hits a fatal step (it then compensates and completes before
/// any signal wait, so the workload sends it no signals).
pub fn has_fatal_step(input: &Value) -> bool {
    input["fail_pattern"]
        .as_str()
        .is_some_and(|p| p.contains('x'))
}

/// The `request_id` of signal `name` of run `key`.
pub fn request_id(key: &str, name: &str) -> String {
    format!("{key}-{name}")
}

/// The payload of signal `name` of run `key` (embeds its request id).
pub fn signal_payload(key: &str, name: &str) -> Value {
    json!({"request_id": request_id(key, name), "signal": name, "key": key})
}

/// The documented crash-saga output (guests/crash-saga/src/lib.rs) of a run that completed
/// without a cancel, given that the workload signalled `go` and `extra` with
/// [`signal_payload`]: `compensated` after a fatal step `k`, else `completed` with every
/// step's result and both payloads.
pub fn expected_output(input: &Value) -> Value {
    let id = input["id"].as_str().unwrap_or("");
    let steps = input["steps"].as_u64().unwrap_or(0);
    let pattern: Vec<char> = input["fail_pattern"]
        .as_str()
        .unwrap_or("")
        .chars()
        .collect();
    match (0..steps as usize).find(|i| pattern.get(*i) == Some(&'x')) {
        Some(k) => json!({
            "id": id,
            "status": "compensated",
            "failed_at": k,
            "undone": (0..k).rev().collect::<Vec<_>>(),
        }),
        None => json!({
            "id": id,
            "status": "completed",
            "results": (0..steps).map(|i| format!("{id}-{i}")).collect::<Vec<_>>(),
            "go": signal_payload(id, "go"),
            "extra": signal_payload(id, "extra"),
        }),
    }
}

/// One node the clients may talk to.
#[derive(Clone, Debug)]
pub struct Target {
    /// Node id.
    pub node: u64,
    /// Its API client.
    pub api: ApiClient,
}

/// Workload settings.
#[derive(Clone, Debug)]
pub struct WorkloadConfig {
    /// The run's seed (inputs, per-client RNGs).
    pub seed: u64,
    /// Client tasks.
    pub clients: usize,
    /// Bearer token for every request.
    pub token: Option<String>,
}

/// What one HTTP request ended with.
#[derive(Debug)]
enum Answer {
    /// 2xx: status, body, consistency token.
    Ok(u16, Value, Option<String>),
    /// 4xx (definite refusal).
    Refused(u16, Value),
    /// Never connected (definitely not applied).
    NoConnect(String),
    /// Timeout / broken connection / 5xx (indeterminate).
    Unknown(String),
}

impl Answer {
    fn outcome(&self) -> Outcome {
        match self {
            Answer::Ok(..) => Outcome::Ok,
            Answer::Refused(..) | Answer::NoConnect(_) => Outcome::Fail,
            Answer::Unknown(_) => Outcome::Info,
        }
    }

    fn token(&self) -> Option<String> {
        match self {
            Answer::Ok(_, _, t) => t.clone(),
            _ => None,
        }
    }

    fn remark(&self) -> String {
        match self {
            Answer::Ok(st, ..) => format!("http {st}"),
            Answer::Refused(st, v) => format!(
                "http {st} {}",
                v["error"]["details"]["reason"]
                    .as_str()
                    .or(v["error"]["code"].as_str())
                    .unwrap_or("")
            ),
            Answer::NoConnect(e) => format!("no connection: {e}"),
            Answer::Unknown(e) => format!("unknown: {e}"),
        }
    }
}

struct Shared {
    cfg: WorkloadConfig,
    targets: Vec<Target>,
    history: Arc<History>,
    stop_new: AtomicBool,
    stop_all: AtomicBool,
    expected: Mutex<BTreeMap<String, Value>>,
    runs: AtomicU64,
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl Shared {
    async fn send(&self, t: &Target, post: Option<&Value>, path: &str) -> Answer {
        let url = format!("{}{path}", t.api.base_url);
        let mut rb = match post {
            Some(b) => t.api.http.post(url).json(b),
            None => t.api.http.get(url),
        };
        rb = rb.timeout(OP_TIMEOUT);
        if let Some(tok) = &self.cfg.token {
            rb = rb.bearer_auth(tok);
        }
        let r = match rb.send().await {
            Ok(r) => r,
            Err(e) if e.is_connect() => return Answer::NoConnect(e.to_string()),
            Err(e) => return Answer::Unknown(e.to_string()),
        };
        let st = r.status().as_u16();
        let token = r
            .headers()
            .get("ostro-consistency")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let body: Value = match r.bytes().await {
            Ok(b) => serde_json::from_slice(&b).unwrap_or(Value::Null),
            Err(e) if (200..300).contains(&st) => {
                return Answer::Unknown(format!("http {st}, body lost: {e}"));
            }
            Err(_) => Value::Null,
        };
        match st {
            200..=299 => Answer::Ok(st, body, token),
            400..=499 => Answer::Refused(st, body),
            _ => Answer::Unknown(format!(
                "http {st} {}",
                body["error"]["details"]["reason"]
                    .as_str()
                    .or(body["error"]["code"].as_str())
                    .unwrap_or("")
            )),
        }
    }

    /// One recorded request.
    #[allow(clippy::too_many_arguments)]
    async fn op(
        &self,
        client: u32,
        t: &Target,
        kind: OpKind,
        path: &str,
        body: Option<&Value>,
        run_id: Option<&str>,
        key: &str,
        detail: Option<String>,
    ) -> Answer {
        let op_id = self.history.next_op_id();
        let invoke_ms = self.history.now_ms();
        let a = self.send(t, body, path).await;
        let complete_ms = self.history.now_ms();
        let run = match (&a, kind) {
            (Answer::Ok(_, v, _), OpKind::Start) => v["run_id"].as_str().map(str::to_owned),
            _ => run_id.map(str::to_owned),
        };
        let mut d = detail.unwrap_or_default();
        let observed = match (&a, kind) {
            (Answer::Ok(_, v, _), OpKind::Result) => v["status"].as_str().unwrap_or("").to_owned(),
            (Answer::Ok(..), _) => String::new(),
            _ => a.remark(),
        };
        if !observed.is_empty() {
            if !d.is_empty() {
                d.push(' ');
            }
            d.push_str(&observed);
        }
        self.history.record(ClientOp {
            op_id,
            client,
            kind,
            node: t.node,
            invoke_ms,
            complete_ms,
            outcome: a.outcome(),
            run_id: run,
            idem_key: key.to_owned(),
            consistency_token: if kind == OpKind::Result {
                None
            } else {
                a.token()
            },
            detail: (!d.is_empty()).then_some(d),
        });
        a
    }

    fn pick<'a>(&'a self, rng: &mut StdRng) -> &'a Target {
        &self.targets[rng.random_range(0..self.targets.len())]
    }

    fn stopped(&self) -> bool {
        self.stop_all.load(Ordering::Relaxed)
    }

    /// Start `key` until acknowledged (`Some(run id)`) or given up.
    async fn start(
        &self,
        client: u32,
        rng: &mut StdRng,
        key: &str,
        input: &Value,
    ) -> Option<String> {
        let body = json!({ "input": input, "idempotency_key": key });
        let path = format!("/v1/workflows/{WORKFLOW}/runs");
        let t0 = Instant::now(); // clock-exempt: harness timing
        while !self.stopped() && t0.elapsed() < START_GIVE_UP {
            let t = self.pick(rng);
            if let Answer::Ok(_, v, _) = self
                .op(
                    client,
                    t,
                    OpKind::Start,
                    &path,
                    Some(&body),
                    None,
                    key,
                    None,
                )
                .await
                && let Some(id) = v["run_id"].as_str()
            {
                return Some(id.to_owned());
            }
            pause(RETRY_PAUSE).await;
        }
        None
    }

    /// A write retried until acknowledged or a terminal refusal (409 / 404); whether it was
    /// acknowledged.
    #[allow(clippy::too_many_arguments)]
    async fn write(
        &self,
        client: u32,
        rng: &mut StdRng,
        kind: OpKind,
        path: &str,
        body: &Value,
        run: &str,
        key: &str,
        detail: &str,
    ) -> bool {
        let t0 = Instant::now(); // clock-exempt: harness timing
        while !self.stopped() && t0.elapsed() < START_GIVE_UP {
            let t = self.pick(rng);
            match self
                .op(
                    client,
                    t,
                    kind,
                    path,
                    Some(body),
                    Some(run),
                    key,
                    Some(detail.to_owned()),
                )
                .await
            {
                Answer::Ok(..) => return true,
                Answer::Refused(409, _) => return false,
                _ => pause(RETRY_PAUSE).await,
            }
        }
        false
    }

    /// Poll `run` until terminal or given up.
    async fn poll(&self, client: u32, rng: &mut StdRng, run: &str, key: &str) {
        let path = format!("/v1/runs/{run}");
        let t0 = Instant::now(); // clock-exempt: harness timing
        while !self.stopped() && t0.elapsed() < POLL_GIVE_UP {
            let t = self.pick(rng);
            if let Answer::Ok(_, v, _) = self
                .op(client, t, OpKind::Result, &path, None, Some(run), key, None)
                .await
                && v["status"]
                    .as_str()
                    .is_some_and(|s| crate::checker::lost::TERMINAL.contains(&s))
            {
                return;
            }
            pause(RETRY_PAUSE * 2).await;
        }
    }

    async fn client_loop(self: Arc<Self>, client: u32) {
        let mut rng =
            StdRng::seed_from_u64(self.cfg.seed ^ (u64::from(client) << 40) ^ 0x6a65_7073);
        let mut n = 0u64;
        while !self.stop_new.load(Ordering::Relaxed) && !self.stopped() {
            let key = format!("j{}-c{client}-{n}", self.cfg.seed);
            n += 1;
            let input = plan_input(self.cfg.seed, &key);
            lock(&self.expected).insert(key.clone(), expected_output(&input));
            let Some(run) = self.start(client, &mut rng, &key, &input).await else {
                continue;
            };
            self.runs.fetch_add(1, Ordering::Relaxed);
            if !has_fatal_step(&input) {
                let cancel = rng.random_range(0..100) < CANCEL_PCT;
                for name in ["go", "extra"] {
                    if name == "extra" && cancel {
                        let path = format!("/v1/runs/{run}/cancel");
                        if self
                            .write(
                                client,
                                &mut rng,
                                OpKind::Cancel,
                                &path,
                                &json!({}),
                                &run,
                                &key,
                                "cancel",
                            )
                            .await
                        {
                            break;
                        }
                    }
                    let rid = request_id(&key, name);
                    let body = json!({"payload": signal_payload(&key, name), "request_id": rid});
                    let path = format!("/v1/runs/{run}/signals/{name}");
                    let detail = format!("{name}/{rid}");
                    self.write(
                        client,
                        &mut rng,
                        OpKind::Signal,
                        &path,
                        &body,
                        &run,
                        &key,
                        &detail,
                    )
                    .await;
                }
            }
            self.poll(client, &mut rng, &run, &key).await;
        }
    }
}

/// Sleep (harness pacing).
pub async fn pause(d: Duration) {
    // clock-exempt: harness timing
    tokio::time::sleep(d).await;
}

/// The running workload.
pub struct Workload {
    shared: Arc<Shared>,
    tasks: JoinSet<()>,
}

impl std::fmt::Debug for Workload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Workload")
            .field("clients", &self.shared.cfg.clients)
            .field("runs", &self.runs())
            .finish_non_exhaustive()
    }
}

impl Workload {
    /// Start `cfg.clients` client tasks over `targets`, recording into `history`.
    pub fn start(cfg: WorkloadConfig, targets: Vec<Target>, history: Arc<History>) -> Self {
        assert!(!targets.is_empty(), "workload needs at least one target");
        let shared = Arc::new(Shared {
            cfg,
            targets,
            history,
            stop_new: AtomicBool::new(false),
            stop_all: AtomicBool::new(false),
            expected: Mutex::new(BTreeMap::new()),
            runs: AtomicU64::new(0),
        });
        let mut tasks = JoinSet::new();
        for c in 0..shared.cfg.clients {
            let sh = Arc::clone(&shared);
            tasks.spawn(sh.client_loop(u32::try_from(c).unwrap_or(u32::MAX)));
        }
        Self { shared, tasks }
    }

    /// Runs acknowledged so far.
    pub fn runs(&self) -> u64 {
        self.shared.runs.load(Ordering::Relaxed)
    }

    /// Stop starting new runs; the clients finish the run they are on (signals, polls).
    pub fn stop_new(&self) {
        self.shared.stop_new.store(true, Ordering::Relaxed);
    }

    /// Wait (bounded by `limit`) for the clients to finish their current run, then stop
    /// every request. Returns the expected result per idempotency key.
    pub async fn finish(mut self, limit: Duration) -> BTreeMap<String, Value> {
        self.stop_new();
        let drained = tokio::time::timeout(limit, async {
            while self.tasks.join_next().await.is_some() {}
        })
        .await;
        if drained.is_err() {
            self.shared.stop_all.store(true, Ordering::Relaxed);
            while self.tasks.join_next().await.is_some() {}
        }
        lock(&self.shared.expected).clone()
    }
}
