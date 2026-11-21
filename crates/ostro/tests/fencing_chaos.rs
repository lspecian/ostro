//! QA-03 fencing chaos campaign (plan 07-21, D-W11): pay-flow runs against the testkit
//! `MockProvider` `/pay` API through spawned `ostro server` + `ostro worker` processes
//! (the 07-16 `P7Cluster` harness) under a seeded fault mix.
//!
//! ```text
//! # quick feedback (not ignored; 100 runs, 1 shard, every fault kind once):
//! cargo test -p ostro --features failpoints --test fencing_chaos --locked -- chaos_smoke
//!
//! # full campaign (resumable: shards with a shard-NN.json are skipped), then the report:
//! OSTRO_CHAOS_RUNS=10000 OSTRO_CHAOS_SHARDS=20 OSTRO_CHAOS_PARALLEL=4 \
//!     bash scripts/fencing-chaos.sh
//! ```
//!
//! # Environment
//!
//! | Variable | Default | Meaning |
//! |---|---|---|
//! | `OSTRO_CHAOS_RUNS` | 10000 | runs over all shards |
//! | `OSTRO_CHAOS_SHARDS` | 20 | shards (one cluster each) |
//! | `OSTRO_CHAOS_PARALLEL` | 4 | shards running at once |
//! | `OSTRO_CHAOS_STATE_DIR` | `target/fencing-chaos` (repo root) | shard files, evidence, report |
//! | `OSTRO_CHAOS_SEED` | `0x0721c4a05eed` | campaign seed (shard seed = splitmix(seed + shard)) |
//! | `OSTRO_CHAOS_SHARD_IDS` | all | `a-b` or `a,b,c`: run only these shards |
//! | `OSTRO_CHAOS_SUBMIT_SECS` | 60 | runs of a shard are submitted evenly over this time |
//!
//! # Shard
//!
//! One server (`lease_ttl` 3 s, `hot_wait` 5 s, 2 embedded slots, quick spill knobs), three
//! workers (4 slots each), one `MockProvider` behind a recording proxy (below). Three of
//! four runs go to the `chaos-w` queue (`placement = workers`), one of four to `chaos-a`
//! (`placement = auto`, embedded first, spilling to the workers). Every run is started
//! with a client idempotency key, so a start retried across a leader kill never creates a
//! second run. A seeded fault driver injects one fault every 2-5 s while runs are pending:
//!
//! - `worker_kill9`: kill -9 a random worker, restart it (same worker id);
//! - `worker_sigstop`: SIGSTOP a random worker for `lease_ttl` + 0-2 s, then SIGCONT;
//! - `worker_failpoint`: replace a random worker (kill -9) by one started with
//!   `FAILPOINTS=worker::<site>=<action>`: `3*return(error)` (self-disarming) or
//!   `2*return(hang)` (the worker is killed and replaced by a clean one after a 2-4 s window);
//! - `server_failpoint`: kill -9 the server and restart it with
//!   `FAILPOINTS=<executor::|dispatcher::site>=<skip>*off-><n>*return` (a burst after `skip`
//!   evaluations; the next restart is clean). This is also a leader kill;
//! - `leader_kill`: kill -9 the server and restart it on the same data dir and ports.
//!
//! # Recording proxy
//!
//! The MockProvider has counters but no request log, so the guest's URL points at a small
//! HTTP/1.1 proxy in this file. Per request it records the `Idempotency-Key`, the arrival
//! time, the response status and `idempotent-replayed`, and rewrites `POST /pay` to
//! `/pay?delay_ms=<50..400>` (the mock sleeps inside its in-progress state, so a request in
//! flight across a leader kill is real and a concurrent resend gets 409). The upstream
//! request always runs to completion, also when the leader that sent it was killed.
//!
//! # Checks (D-W11)
//!
//! Per shard, after every run is terminal and the system was quiet for 2 s:
//!
//! - `charges(key) == 1` for every key (`double_charges` counts keys with more than one);
//! - `lost`: a run that is not `completed`, has no `run_completed` exactly once, has a
//!   result other than `{status: 200, idempotency_key}`, or whose key has no charge;
//! - **fenced sends** (W5): a mock request for key K is fenced when a StartEffect for K's run
//!   was rejected by a live leader (worker log: `StartEffect rejected`, or `StartEffect
//!   failed` with a non-transport code) in the same leader incarnation before the request
//!   (no leader kill between them), and the request is not authorised by (a) a StartEffect
//!   for K's run accepted in that incarnation (worker debug log `effect started on the
//!   leader`), (b) a request for K before that incarnation began (a re-drive of an effect in
//!   flight at a leader kill), or (c) the previous request for K in that incarnation having
//!   been answered 409/503 (a bridge retry of the running effect);
//! - **duplicate attribution** (W5): every key with `requests(key) > 1` is attributed to
//!   (a) `leader_kill`: a leader kill (`leader_kill` or `server_failpoint`) with timestamp in
//!   `[first request - KILL_SLACK, effect_completed]`, or (b) `retry_409`: an extra request
//!   whose previous request for K was answered 409 **and** the leader logged an
//!   `http effect attempt` with status 409 for the run's effect op within 2 s of that
//!   answer (the bridge's attempt line carries the op but not the run id: this correlation
//!   is by op and time). Anything else is `unattributed`;
//! - `raw_duplicates_without_leader_kill`: extra requests for K with no leader kill in
//!   `[first request of K - KILL_SLACK, the extra request]` (worker and executor zombies
//!   alone must never produce a second raw request).
//!
//! `KILL_SLACK` (250 ms) covers a request the killed leader wrote into the socket just
//! before SIGKILL but the proxy read just after it.
//!
//! The shard writes `shard-NN.json`; a failing shard also keeps its evidence under
//! `shard-NN/` (server and worker logs, proxy request log, fault log). `report`
//! aggregates every shard file into `report.json` and prints a summary table.
#![cfg(all(unix, feature = "failpoints"))]
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;
#[path = "common/p7.rs"]
mod p7;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use p7::{P7Api, P7Cluster, P7Opts, WorkerOpts, WorkerProc};
use serde_json::{Value, json};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// A leader kill this long before a key's first request still counts for that key.
const KILL_SLACK_MS: u64 = 250;
/// `[executor] lease_ttl` of every shard.
const LEASE_TTL: Duration = Duration::from_secs(3);
/// Workers per shard.
const WORKERS: usize = 3;
/// Slots per worker.
const WORKER_SLOTS: u32 = 4;
/// Quiet time after the last run became terminal (late duplicates would show up here).
const QUIET: Duration = Duration::from_secs(2);
/// The payment secret of `pay-flow` (07-16 renamed it to a valid lowercase name).
const SECRET: &str = "pay_token";

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------------
// Seeded RNG (splitmix64)
// ---------------------------------------------------------------------------------

#[derive(Clone)]
struct Rng(u64);

fn splitmix(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        splitmix(self.0)
    }

    /// Uniform in `[lo, hi]`.
    fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.next() % (hi - lo + 1)
    }

    fn pick<'a, T>(&mut self, xs: &'a [T]) -> &'a T {
        &xs[(self.next() % xs.len() as u64) as usize]
    }
}

// ---------------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct ChaosCfg {
    runs: usize,
    shards: usize,
    parallel: usize,
    state_dir: PathBuf,
    seed: u64,
    shard_ids: Option<Vec<usize>>,
    submit_secs: u64,
}

fn env_num<T: std::str::FromStr>(k: &str, d: T) -> T {
    match std::env::var(k) {
        Ok(v) if !v.trim().is_empty() => v
            .trim()
            .parse()
            .unwrap_or_else(|_| panic!("{k}={v}: not a number")),
        _ => d,
    }
}

fn parse_seed(s: &str) -> u64 {
    let s = s.trim();
    match s.strip_prefix("0x") {
        Some(h) => u64::from_str_radix(h, 16).expect("OSTRO_CHAOS_SEED hex"),
        None => s.parse().expect("OSTRO_CHAOS_SEED"),
    }
}

fn parse_ids(s: &str) -> Vec<usize> {
    let mut out = Vec::new();
    for part in s.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        match part.split_once('-') {
            Some((a, b)) => {
                let (a, b): (usize, usize) = (a.parse().expect("ids"), b.parse().expect("ids"));
                out.extend(a..=b);
            }
            None => out.push(part.parse().expect("ids")),
        }
    }
    out
}

impl ChaosCfg {
    fn from_env() -> Self {
        let state = std::env::var("OSTRO_CHAOS_STATE_DIR")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| "target/fencing-chaos".into());
        let state = PathBuf::from(state);
        let state_dir = if state.is_absolute() {
            state
        } else {
            common::repo_root().join(state)
        };
        let cfg = Self {
            runs: env_num("OSTRO_CHAOS_RUNS", 10_000),
            shards: env_num("OSTRO_CHAOS_SHARDS", 20),
            parallel: env_num("OSTRO_CHAOS_PARALLEL", 4).max(1),
            state_dir,
            seed: std::env::var("OSTRO_CHAOS_SEED")
                .ok()
                .filter(|s| !s.trim().is_empty())
                .map(|s| parse_seed(&s))
                .unwrap_or(0x0721_C4A0_5EED),
            shard_ids: std::env::var("OSTRO_CHAOS_SHARD_IDS")
                .ok()
                .filter(|s| !s.trim().is_empty())
                .map(|s| parse_ids(&s)),
            submit_secs: env_num("OSTRO_CHAOS_SUBMIT_SECS", 60),
        };
        assert!(cfg.shards > 0 && cfg.runs >= cfg.shards, "{cfg:?}");
        cfg
    }

    /// Runs of shard `i` (the remainder goes to the first shards).
    fn runs_of(&self, i: usize) -> usize {
        self.runs / self.shards + usize::from(i < self.runs % self.shards)
    }

    fn shard_seed(&self, i: usize) -> u64 {
        splitmix(self.seed.wrapping_add(i as u64))
    }

    fn shard_file(&self, i: usize) -> PathBuf {
        self.state_dir.join(format!("shard-{i:02}.json"))
    }
}

// ---------------------------------------------------------------------------------
// Recording proxy in front of the MockProvider
// ---------------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct ReqRec {
    key: Option<String>,
    path: String,
    t_ms: u64,
    done_ms: u64,
    status: u16,
    replayed: bool,
}

struct Proxy {
    addr: SocketAddr,
    log: Arc<Mutex<Vec<ReqRec>>>,
    errors: Arc<AtomicU64>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Proxy {
    async fn start(upstream: SocketAddr, seed: u64) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
        let addr = listener.local_addr().expect("proxy addr");
        let log = Arc::new(Mutex::new(Vec::new()));
        let errors = Arc::new(AtomicU64::new(0));
        let ctr = Arc::new(AtomicU64::new(seed));
        let (l2, e2) = (Arc::clone(&log), Arc::clone(&errors));
        let task = tokio::spawn(async move {
            while let Ok((sock, _)) = listener.accept().await {
                let (l, e, c) = (Arc::clone(&l2), Arc::clone(&e2), Arc::clone(&ctr));
                tokio::spawn(async move {
                    if proxy_conn(sock, upstream, &l, &c).await.is_err() {
                        e.fetch_add(1, Ordering::Relaxed);
                    }
                });
            }
        });
        Self {
            addr,
            log,
            errors,
            task,
        }
    }

    fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    fn records(&self) -> Vec<ReqRec> {
        self.log.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

/// One parsed HTTP/1.1 message head.
struct Head {
    first: String,
    headers: Vec<(String, String)>,
}

impl Head {
    fn get(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    fn encode(&self) -> Vec<u8> {
        let mut s = format!("{}\r\n", self.first);
        for (k, v) in &self.headers {
            s.push_str(&format!("{k}: {v}\r\n"));
        }
        s.push_str("\r\n");
        s.into_bytes()
    }
}

/// Read a message head (`None` on a clean EOF before any byte); `buf` keeps the bytes read
/// past the head.
async fn read_head<R: AsyncRead + Unpin>(
    r: &mut R,
    buf: &mut Vec<u8>,
) -> std::io::Result<Option<Head>> {
    loop {
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            let head = String::from_utf8_lossy(&buf[..pos]).into_owned();
            buf.drain(..pos + 4);
            let mut lines = head.split("\r\n");
            let first = lines.next().unwrap_or_default().to_owned();
            let headers = lines
                .filter_map(|l| l.split_once(':'))
                .map(|(k, v)| (k.trim().to_owned(), v.trim().to_owned()))
                .collect();
            return Ok(Some(Head { first, headers }));
        }
        let mut tmp = [0u8; 8192];
        let n = r.read(&mut tmp).await?;
        if n == 0 {
            if buf.is_empty() {
                return Ok(None);
            }
            return Err(std::io::Error::other("eof inside a message head"));
        }
        buf.extend_from_slice(&tmp[..n]);
    }
}

/// Read a `Content-Length` body (0 when absent; chunked bodies are refused).
async fn read_body<R: AsyncRead + Unpin>(
    r: &mut R,
    buf: &mut Vec<u8>,
    head: &Head,
) -> std::io::Result<Vec<u8>> {
    if head
        .get("transfer-encoding")
        .is_some_and(|v| v.to_ascii_lowercase().contains("chunked"))
    {
        return Err(std::io::Error::other("chunked bodies are not supported"));
    }
    let len: usize = head
        .get("content-length")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    while buf.len() < len {
        let mut tmp = [0u8; 8192];
        let n = r.read(&mut tmp).await?;
        if n == 0 {
            return Err(std::io::Error::other("eof inside a body"));
        }
        buf.extend_from_slice(&tmp[..n]);
    }
    Ok(buf.drain(..len).collect())
}

async fn proxy_conn(
    mut client: TcpStream,
    upstream: SocketAddr,
    log: &Mutex<Vec<ReqRec>>,
    ctr: &AtomicU64,
) -> std::io::Result<()> {
    let mut up: Option<TcpStream> = None;
    let (mut cbuf, mut ubuf) = (Vec::new(), Vec::new());
    loop {
        let Some(mut head) = read_head(&mut client, &mut cbuf).await? else {
            return Ok(());
        };
        let body = read_body(&mut client, &mut cbuf, &head).await?;
        let t_ms = now_ms();
        let key = head.get("idempotency-key").map(str::to_owned);
        let mut parts = head.first.splitn(3, ' ');
        let (method, target, version) = (
            parts.next().unwrap_or_default().to_owned(),
            parts.next().unwrap_or_default().to_owned(),
            parts.next().unwrap_or("HTTP/1.1").to_owned(),
        );
        let path = target.split('?').next().unwrap_or_default().to_owned();
        if method == "POST" && path == "/pay" {
            let delay = 50 + splitmix(ctr.fetch_add(1, Ordering::Relaxed)) % 351;
            head.first = format!("{method} /pay?delay_ms={delay} {version}");
        }
        // The upstream exchange always completes (the provider finishes a request whose
        // client vanished), so its status is recorded even when the leader was killed.
        let resp = async {
            if up.is_none() {
                up = Some(TcpStream::connect(upstream).await?);
                ubuf.clear();
            }
            let u = up.as_mut().expect("upstream");
            let mut msg = head.encode();
            msg.extend_from_slice(&body);
            u.write_all(&msg).await?;
            let rh = read_head(u, &mut ubuf)
                .await?
                .ok_or_else(|| std::io::Error::other("upstream closed"))?;
            let rb = read_body(u, &mut ubuf, &rh).await?;
            Ok::<_, std::io::Error>((rh, rb))
        }
        .await;
        let (rh, rb) = match resp {
            Ok(x) => x,
            Err(e) => {
                log.lock().unwrap_or_else(|e| e.into_inner()).push(ReqRec {
                    key,
                    path,
                    t_ms,
                    done_ms: now_ms(),
                    status: 0,
                    replayed: false,
                });
                return Err(e);
            }
        };
        let status: u16 = rh
            .first
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        log.lock().unwrap_or_else(|e| e.into_inner()).push(ReqRec {
            key,
            path,
            t_ms,
            done_ms: now_ms(),
            status,
            replayed: rh.get("idempotent-replayed") == Some("true"),
        });
        let mut out = rh.encode();
        out.extend_from_slice(&rb);
        client.write_all(&out).await?;
    }
}

// ---------------------------------------------------------------------------------
// Log parsing
// ---------------------------------------------------------------------------------

/// Days since 1970-01-01 of a civil date (Howard Hinnant's algorithm).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// `YYYY-MM-DDTHH:MM:SS[.frac]Z` -> epoch ms (UTC only: what tracing writes).
fn rfc3339_ms(s: &str) -> Option<u64> {
    let s = s.trim();
    let b = s.as_bytes();
    if b.len() < 20 || b[4] != b'-' || b[10] != b'T' {
        return None;
    }
    let n = |r: std::ops::Range<usize>| s.get(r)?.parse::<i64>().ok();
    let (y, mo, d) = (n(0..4)?, n(5..7)?, n(8..10)?);
    let (h, mi, se) = (n(11..13)?, n(14..16)?, n(17..19)?);
    let rest = s.get(19..)?;
    let rest = rest.strip_suffix('Z')?;
    let ms = match rest.strip_prefix('.') {
        Some(frac) => {
            let digits: String = frac.chars().take(3).collect();
            let v: i64 = digits.parse().ok()?;
            v * 10_i64.pow(3 - digits.len() as u32)
        }
        None => 0,
    };
    let days = days_from_civil(y, mo, d);
    u64::try_from(((days * 24 + h) * 60 + mi) * 60_000 + se * 1000 + ms).ok()
}

fn strip_ansi(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut it = s.chars().peekable();
    while let Some(c) = it.next() {
        if c == '\u{1b}' {
            if it.peek() == Some(&'[') {
                it.next();
                for c2 in it.by_ref() {
                    if c2.is_ascii_alphabetic() {
                        break;
                    }
                }
            }
            continue;
        }
        out.push(c);
    }
    out
}

fn field<'a>(line: &'a str, name: &str) -> Option<&'a str> {
    let pat = format!("{name}=");
    let mut from = 0;
    while let Some(i) = line[from..].find(&pat) {
        let at = from + i;
        if at == 0 || line.as_bytes()[at - 1] == b' ' {
            let v = &line[at + pat.len()..];
            return Some(v.split_whitespace().next().unwrap_or(""));
        }
        from = at + pat.len();
    }
    None
}

/// A worker's view of one StartEffect.
#[derive(Clone, Debug)]
struct EffectLog {
    run: String,
    t_ms: u64,
    worker: String,
    accepted: bool,
    line: String,
}

/// StartEffect outcomes from a worker's compact log.
fn parse_worker_log(worker: &str, lines: &[String]) -> Vec<EffectLog> {
    let mut out = Vec::new();
    for raw in lines {
        let l = strip_ansi(raw);
        let accepted = l.contains("effect started on the leader");
        let rejected = l.contains("StartEffect rejected")
            || (l.contains("StartEffect failed") && {
                let lower = l.to_ascii_lowercase();
                !(lower.contains("unavailable")
                    || lower.contains("transport")
                    || lower.contains("cancel")
                    || lower.contains("deadline")
                    || lower.contains("unknown error")
                    || lower.contains("connection"))
            });
        if !(accepted || rejected) {
            continue;
        }
        let (Some(t), Some(run)) = (
            l.split_whitespace().next().and_then(rfc3339_ms),
            field(&l, "run"),
        ) else {
            continue;
        };
        out.push(EffectLog {
            run: run.to_owned(),
            t_ms: t,
            worker: worker.to_owned(),
            accepted,
            line: l.clone(),
        });
    }
    out
}

/// One leader-side `http effect attempt` (bridge debug line, JSON log format).
#[derive(Clone, Debug)]
struct AttemptLog {
    t_ms: u64,
    op: u64,
    status: u64,
    attempt: u64,
}

fn parse_server_log(lines: &[String]) -> Vec<AttemptLog> {
    let mut out = Vec::new();
    for l in lines {
        if !l.contains("http effect attempt") {
            continue;
        }
        let Ok(v) = serde_json::from_str::<Value>(l) else {
            continue;
        };
        let f = &v["fields"];
        if f["message"] != "http effect attempt" {
            continue;
        }
        let (Some(t), Some(op)) = (
            v["timestamp"].as_str().and_then(rfc3339_ms),
            f["op"].as_u64(),
        ) else {
            continue;
        };
        out.push(AttemptLog {
            t_ms: t,
            op,
            status: f["status"].as_u64().unwrap_or(0),
            attempt: f["attempt"].as_u64().unwrap_or(0),
        });
    }
    out
}

// ---------------------------------------------------------------------------------
// Faults
// ---------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Kind {
    WorkerKill9,
    WorkerSigstop,
    WorkerFailpoint,
    ServerFailpoint,
    LeaderKill,
}

impl Kind {
    const ALL: [Kind; 5] = [
        Kind::WorkerKill9,
        Kind::WorkerSigstop,
        Kind::WorkerFailpoint,
        Kind::ServerFailpoint,
        Kind::LeaderKill,
    ];

    fn name(self) -> &'static str {
        match self {
            Kind::WorkerKill9 => "worker_kill9",
            Kind::WorkerSigstop => "worker_sigstop",
            Kind::WorkerFailpoint => "worker_failpoint",
            Kind::ServerFailpoint => "server_failpoint",
            Kind::LeaderKill => "leader_kill",
        }
    }

    fn kills_leader(self) -> bool {
        matches!(self, Kind::ServerFailpoint | Kind::LeaderKill)
    }
}

#[derive(Clone, Debug)]
struct Fault {
    kind: Kind,
    /// Signal / restart time (ms since the epoch); for a leader kill the SIGKILL time.
    t_ms: u64,
    /// End of the fault (SIGCONT, restart ready, window end).
    end_ms: u64,
    target: String,
    detail: String,
}

impl Fault {
    fn json(&self) -> Value {
        json!({
            "kind": self.kind.name(),
            "t_ms": self.t_ms,
            "end_ms": self.end_ms,
            "target": self.target,
            "detail": self.detail,
        })
    }
}

const WORKER_FPS: [&str; 4] = [
    "worker::before_append",
    "worker::before_done",
    "worker::before_start_effect",
    "worker::after_start_effect",
];
const SERVER_FPS: [&str; 4] = [
    "executor::before_append",
    "executor::after_append_before_done",
    "dispatcher::before_grant",
    "dispatcher::before_ack",
];

/// Worker slots plus the logs of every process that ever ran in them.
struct Workers {
    procs: Vec<WorkerProc>,
    ids: Vec<String>,
    /// (worker id, lines) of processes already gone.
    logs: Vec<(String, Vec<String>)>,
}

fn worker_opts(id: &str, fp: Option<&str>) -> WorkerOpts {
    let mut w = WorkerOpts::new(id, WORKER_SLOTS).label("pool", "chaos");
    w.rust_log = Some("info,ostro_worker::effect_proxy=debug".into());
    w.env.push(("NO_COLOR".into(), "1".into()));
    if let Some(fp) = fp {
        w.env.push(("FAILPOINTS".into(), fp.into()));
    }
    w
}

impl Workers {
    fn retire(&mut self, i: usize) {
        let p = &self.procs[i];
        let mut lines = p.stdout();
        lines.extend(p.stderr());
        self.logs.push((self.ids[i].clone(), lines));
    }

    /// kill -9 slot `i` and start a new process in it (same worker id).
    fn replace(&mut self, c: &P7Cluster, i: usize, fp: Option<&str>) {
        self.procs[i].kill9();
        self.retire(i);
        self.procs[i] = c.spawn_worker(worker_opts(&self.ids[i], fp));
    }

    fn all_logs(&self) -> Vec<(String, Vec<String>)> {
        let mut out = self.logs.clone();
        for (i, p) in self.procs.iter().enumerate() {
            let mut lines = p.stdout();
            lines.extend(p.stderr());
            out.push((self.ids[i].clone(), lines));
        }
        out
    }
}

/// Server lines of every incarnation.
struct ServerLogs(Vec<String>);

impl ServerLogs {
    fn take(&mut self, c: &P7Cluster) {
        if let Some(s) = c.server.as_ref() {
            self.0.extend(s.stdout());
            self.0.extend(s.stderr());
        }
    }
}

async fn leader_restart(
    c: &mut P7Cluster,
    logs: &mut ServerLogs,
    failpoints: Option<String>,
) -> (u64, u64) {
    logs.take(c);
    c.opts.env.retain(|(k, _)| k != "FAILPOINTS");
    if let Some(fp) = failpoints {
        c.opts.env.push(("FAILPOINTS".into(), fp));
    }
    let t = now_ms();
    c.kill9_server();
    c.restart_server().await;
    let ready = now_ms();
    // Do not carry the failpoints into the next restart.
    c.opts.env.retain(|(k, _)| k != "FAILPOINTS");
    (t, ready)
}

async fn inject(
    kind: Kind,
    rng: &mut Rng,
    c: &mut P7Cluster,
    ws: &mut Workers,
    logs: &mut ServerLogs,
) -> Fault {
    match kind {
        Kind::WorkerKill9 => {
            let i = (rng.next() % ws.procs.len() as u64) as usize;
            let t = now_ms();
            tokio::task::block_in_place(|| ws.replace(c, i, None));
            Fault {
                kind,
                t_ms: t,
                end_ms: now_ms(),
                target: ws.ids[i].clone(),
                detail: "kill -9, restarted".into(),
            }
        }
        Kind::WorkerSigstop => {
            let i = (rng.next() % ws.procs.len() as u64) as usize;
            let extra = rng.range(0, 2000);
            let t = now_ms();
            ws.procs[i].sigstop();
            tokio::time::sleep(LEASE_TTL + Duration::from_millis(extra)).await;
            ws.procs[i].sigcont();
            Fault {
                kind,
                t_ms: t,
                end_ms: now_ms(),
                target: ws.ids[i].clone(),
                detail: format!("SIGSTOP {} ms", LEASE_TTL.as_millis() as u64 + extra),
            }
        }
        Kind::WorkerFailpoint => {
            let i = (rng.next() % ws.procs.len() as u64) as usize;
            let site = *rng.pick(&WORKER_FPS);
            let hang = rng.next().is_multiple_of(2);
            let action = if hang {
                "2*return(hang)"
            } else {
                "3*return(error)"
            };
            let fp = format!("{site}={action}");
            let t = now_ms();
            tokio::task::block_in_place(|| ws.replace(c, i, Some(&fp)));
            let window = rng.range(2000, 4000);
            tokio::time::sleep(Duration::from_millis(window)).await;
            if hang {
                // Hung activations hold their leases (heartbeats continue): end the window.
                tokio::task::block_in_place(|| ws.replace(c, i, None));
            }
            Fault {
                kind,
                t_ms: t,
                end_ms: now_ms(),
                target: ws.ids[i].clone(),
                detail: format!(
                    "{fp}; window {window} ms{}",
                    if hang { ", then replaced" } else { "" }
                ),
            }
        }
        Kind::ServerFailpoint => {
            let site = *rng.pick(&SERVER_FPS);
            let skip = rng.range(5, 40);
            let n = rng.range(1, 4);
            let fp = format!("{site}={skip}*off->{n}*return");
            let (t, ready) = leader_restart(c, logs, Some(fp.clone())).await;
            Fault {
                kind,
                t_ms: t,
                end_ms: ready,
                target: "server".into(),
                detail: format!("kill -9, restarted with {fp}"),
            }
        }
        Kind::LeaderKill => {
            let (t, ready) = leader_restart(c, logs, None).await;
            Fault {
                kind,
                t_ms: t,
                end_ms: ready,
                target: "server".into(),
                detail: "kill -9, restarted".into(),
            }
        }
    }
}

// ---------------------------------------------------------------------------------
// Runs
// ---------------------------------------------------------------------------------

/// Start a run with a client idempotency key, retrying across leader restarts.
async fn start_run(api: &P7Api, key: &str, queue: &str, url: &str) -> Option<String> {
    let deadline = Instant::now() + Duration::from_secs(120);
    let body = json!({
        "input": {"url": url},
        "queue": queue,
        "idempotency_key": key,
    });
    loop {
        let r = api
            .http
            .post(api.url("/v1/workflows/pay-flow/runs"))
            .json(&body)
            .send()
            .await;
        if let Ok(r) = r
            && r.status().is_success()
            && let Ok(v) = r.json::<Value>().await
            && let Some(id) = v["run_id"].as_str()
        {
            return Some(id.to_owned());
        }
        if Instant::now() > deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn get_json(api: &P7Api, path: &str) -> Option<Value> {
    let r = api.http.get(api.url(path)).send().await.ok()?;
    if !r.status().is_success() {
        return None;
    }
    r.json().await.ok()
}

async fn history(api: &P7Api, id: &str) -> Option<Vec<Value>> {
    let mut out = Vec::new();
    let mut from = 0u64;
    loop {
        let p = get_json(
            api,
            &format!("/v1/runs/{id}/history?from_seq={from}&limit=100"),
        )
        .await?;
        out.extend(p["events"].as_array()?.iter().cloned());
        match p["next_from_seq"].as_u64() {
            Some(n) => from = n,
            None => return Some(out),
        }
    }
}

async fn put_ok(api: &P7Api, path: &str, body: Value) {
    let (st, v) = api.put(path, body).await;
    assert!(st.is_success(), "PUT {path}: {st} {v}");
}

#[derive(Clone, Debug, Default)]
struct RunInfo {
    id: String,
    start_key: String,
    queue: String,
    status: String,
    key: Option<String>,
    result_status: Option<u64>,
    completed_events: usize,
    effect_op: Option<u64>,
    effect_completed_ms: Option<u64>,
    latency_ms: Option<u64>,
}

// ---------------------------------------------------------------------------------
// One shard
// ---------------------------------------------------------------------------------

struct ShardSpec {
    shard: usize,
    runs: usize,
    seed: u64,
    submit_secs: u64,
    /// `Some(order)`: inject exactly these faults (smoke); `None`: seeded mix until done.
    script: Option<Vec<Kind>>,
    gap_ms: (u64, u64),
    out_dir: PathBuf,
}

fn chaos_opts() -> P7Opts {
    P7Opts {
        lease_ttl: Some(LEASE_TTL),
        heartbeat: Some(Duration::from_millis(1000)),
        embedded_slots: Some(2),
        hot_wait: Some(Duration::from_secs(5)),
        spill: vec![
            ("slots_pct".into(), "50".into()),
            ("lag".into(), "\"500ms\"".into()),
            ("sustain".into(), "\"1s\"".into()),
            ("cooldown".into(), "\"2s\"".into()),
        ],
        mock: true,
        extra_toml: "[log]\nformat = \"json\"\n\n[secrets]\nauto_sealing_key = true\n\n".into(),
        env: vec![("RUST_LOG".into(), "info,ostro_bridge::http=debug".into())],
        ..P7Opts::default()
    }
}

async fn run_shard(spec: ShardSpec) -> Value {
    let t_start = Instant::now();
    let mut rng = Rng(spec.seed);
    let mut c = P7Cluster::start(chaos_opts()).await;
    let proxy = Proxy::start(c.mock().addr(), spec.seed).await;
    let url = proxy.base_url();
    let mut logs = ServerLogs(Vec::new());

    // Module, secret, grant, egress, queues.
    let wasm = common::guest_wasm("pay-flow");
    let sha = common::sha_hex(&wasm);
    let r = c
        .api
        .http
        .put(c.api.url(&format!("/v1/modules/{sha}?name=pay-flow")))
        .header("content-type", "application/wasm")
        .body(wasm)
        .send()
        .await
        .expect("PUT module");
    assert!(r.status().is_success(), "deploy pay-flow: {}", r.status());
    put_ok(
        &c.api,
        &format!("/v1/secrets/{SECRET}"),
        json!({"value": format!("chaos-secret-{}", spec.seed)}),
    )
    .await;
    put_ok(
        &c.api,
        "/v1/grants",
        json!({"module": "pay-flow", "secret": SECRET, "origins": [url], "body": false}),
    )
    .await;
    put_ok(&c.api, "/v1/egress/pay-flow", json!({"allow": [url]})).await;
    c.api
        .set_flow(
            "chaos-w",
            json!({"placement": "workers", "selector": ["pool=chaos"]}),
        )
        .await;
    c.api
        .set_flow(
            "chaos-a",
            json!({"placement": "auto", "selector": ["pool=chaos"]}),
        )
        .await;

    let ids: Vec<String> = (0..WORKERS)
        .map(|i| format!("s{:02}-w{i}", spec.shard))
        .collect();
    let procs = ids
        .iter()
        .map(|id| c.spawn_worker(worker_opts(id, None)))
        .collect();
    let mut ws = Workers {
        procs,
        ids,
        logs: Vec::new(),
    };
    c.wait_workers(WORKERS).await;

    // Submitter: runs spread evenly over submit_secs.
    let runs: Arc<Mutex<Vec<RunInfo>>> = Arc::new(Mutex::new(Vec::new()));
    let submitted = Arc::new(AtomicBool::new(false));
    let api = c.api.clone();
    let submitter = {
        let (runs, submitted, url) = (Arc::clone(&runs), Arc::clone(&submitted), url.clone());
        let (n, shard, secs) = (spec.runs, spec.shard, spec.submit_secs);
        tokio::spawn(async move {
            let t0 = Instant::now();
            let mut tasks = tokio::task::JoinSet::new();
            for i in 0..n {
                let due = Duration::from_millis(secs * 1000 * i as u64 / n as u64);
                if let Some(wait) = due.checked_sub(t0.elapsed()) {
                    tokio::time::sleep(wait).await;
                }
                let queue = if i % 4 == 3 { "chaos-a" } else { "chaos-w" };
                let start_key = format!("chaos-s{shard:02}-r{i:05}");
                let (api, url, runs) = (api.clone(), url.clone(), Arc::clone(&runs));
                tasks.spawn(async move {
                    let id = start_run(&api, &start_key, queue, &url).await;
                    runs.lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .push(RunInfo {
                            id: id.unwrap_or_default(),
                            start_key,
                            queue: queue.into(),
                            ..RunInfo::default()
                        });
                });
            }
            while tasks.join_next().await.is_some() {}
            submitted.store(true, Ordering::Release);
        })
    };

    // Poller: every pending run until terminal.
    let done = Arc::new(AtomicBool::new(false));
    let terminal: Arc<Mutex<HashMap<String, Value>>> = Arc::new(Mutex::new(HashMap::new()));
    let poller = {
        let (runs, submitted, done, terminal) = (
            Arc::clone(&runs),
            Arc::clone(&submitted),
            Arc::clone(&done),
            Arc::clone(&terminal),
        );
        let api = c.api.clone();
        tokio::spawn(async move {
            loop {
                let all_submitted = submitted.load(Ordering::Acquire);
                let pending: Vec<String> = {
                    let rs = runs.lock().unwrap_or_else(|e| e.into_inner());
                    let t = terminal.lock().unwrap_or_else(|e| e.into_inner());
                    rs.iter()
                        .filter(|r| !r.id.is_empty() && !t.contains_key(&r.id))
                        .map(|r| r.id.clone())
                        .collect()
                };
                if all_submitted && pending.is_empty() {
                    done.store(true, Ordering::Release);
                    return;
                }
                for chunk in pending.chunks(16) {
                    let mut set = tokio::task::JoinSet::new();
                    for id in chunk {
                        let (api, id) = (api.clone(), id.clone());
                        set.spawn(async move {
                            let v = get_json(&api, &format!("/v1/runs/{id}")).await;
                            (id, v)
                        });
                    }
                    while let Some(Ok((id, v))) = set.join_next().await {
                        if let Some(v) = v
                            && v["status"].as_str().is_some_and(common::is_terminal)
                        {
                            terminal
                                .lock()
                                .unwrap_or_else(|e| e.into_inner())
                                .insert(id, v);
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        })
    };

    // Fault driver.
    let mut faults: Vec<Fault> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(spec.submit_secs + 300);
    let mut script = spec.script.clone().map(|s| s.into_iter());
    loop {
        let gap = rng.range(spec.gap_ms.0, spec.gap_ms.1);
        tokio::time::sleep(Duration::from_millis(gap)).await;
        let kind = match script.as_mut() {
            Some(s) => match s.next() {
                Some(k) => k,
                None => break,
            },
            None => {
                if done.load(Ordering::Acquire) || Instant::now() > deadline {
                    break;
                }
                // Leader kills are the heaviest fault: about one in five.
                *rng.pick(&Kind::ALL)
            }
        };
        let f = inject(kind, &mut rng, &mut c, &mut ws, &mut logs).await;
        eprintln!(
            "fencing_chaos shard {:02}: +{:>5.1}s {} {} ({})",
            spec.shard,
            t_start.elapsed().as_secs_f64(),
            f.kind.name(),
            f.target,
            f.detail
        );
        faults.push(f);
    }
    // Wait for every run to be terminal (bounded), then a quiet period.
    let wait_deadline = Instant::now() + Duration::from_secs(spec.submit_secs + 300);
    while !done.load(Ordering::Acquire) && Instant::now() < wait_deadline {
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let timed_out = !done.load(Ordering::Acquire);
    poller.abort();
    let _ = submitter.await;
    tokio::time::sleep(QUIET).await;

    // Collect every run's record and history.
    let mut runs = runs.lock().unwrap_or_else(|e| e.into_inner()).clone();
    runs.sort_by(|a, b| a.start_key.cmp(&b.start_key));
    for r in runs.iter_mut().filter(|r| !r.id.is_empty()) {
        let Some(v) = get_json(&c.api, &format!("/v1/runs/{}", r.id)).await else {
            continue;
        };
        r.status = v["status"].as_str().unwrap_or_default().to_owned();
        r.key = v["result"]["idempotency_key"].as_str().map(str::to_owned);
        r.result_status = v["result"]["status"].as_u64();
        if let (Some(a), Some(b)) = (v["created_at_ms"].as_u64(), v["completed_at_ms"].as_u64()) {
            r.latency_ms = Some(b.saturating_sub(a));
        }
        if let Some(h) = history(&c.api, &r.id).await {
            r.completed_events = h.iter().filter(|e| e["kind"] == "run_completed").count();
            r.effect_op = h
                .iter()
                .find(|e| e["kind"] == "effect_scheduled")
                .and_then(|e| e["seq"].as_u64());
            r.effect_completed_ms = h
                .iter()
                .find(|e| e["kind"] == "effect_completed")
                .and_then(|e| e["ts_ms"].as_u64());
        }
    }
    logs.take(&c);
    let worker_logs = ws.all_logs();
    let charges: HashMap<String, usize> = runs
        .iter()
        .filter_map(|r| r.key.clone())
        .map(|k| {
            let n = c.mock().charges(&k);
            (k, n)
        })
        .collect();
    let mock_requests: HashMap<String, usize> = runs
        .iter()
        .filter_map(|r| r.key.clone())
        .map(|k| {
            let n = c.mock().requests(&k);
            (k, n)
        })
        .collect();
    let reqs = proxy.records();
    let proxy_errors = proxy.errors.load(Ordering::Relaxed);
    drop(ws);
    drop(c);

    let mut effects = Vec::new();
    for (w, lines) in &worker_logs {
        effects.extend(parse_worker_log(w, lines));
    }
    let attempts = parse_server_log(&logs.0);
    let report = analyse(
        &spec,
        &runs,
        &faults,
        &reqs,
        &charges,
        &mock_requests,
        &effects,
        &attempts,
        timed_out,
        proxy_errors,
        t_start.elapsed(),
    );
    if report["pass"] != true {
        let dir = spec.out_dir.join(format!("shard-{:02}", spec.shard));
        std::fs::create_dir_all(&dir).expect("evidence dir");
        std::fs::write(dir.join("server.log"), logs.0.join("\n")).expect("server.log");
        for (i, (w, lines)) in worker_logs.iter().enumerate() {
            std::fs::write(dir.join(format!("worker-{i:02}-{w}.log")), lines.join("\n"))
                .expect("worker log");
        }
        let rq: Vec<String> = reqs
            .iter()
            .map(|r| {
                json!({"key": r.key, "path": r.path, "t_ms": r.t_ms, "done_ms": r.done_ms,
                       "status": r.status, "replayed": r.replayed})
                .to_string()
            })
            .collect();
        std::fs::write(dir.join("requests.jsonl"), rq.join("\n")).expect("requests");
        let fl: Vec<String> = faults.iter().map(|f| f.json().to_string()).collect();
        std::fs::write(dir.join("faults.jsonl"), fl.join("\n")).expect("faults");
        let rs: Vec<String> = runs
            .iter()
            .map(|r| {
                json!({"id": r.id, "start_key": r.start_key, "queue": r.queue,
                       "status": r.status, "key": r.key, "result_status": r.result_status,
                       "completed_events": r.completed_events, "effect_op": r.effect_op,
                       "effect_completed_ms": r.effect_completed_ms})
                .to_string()
            })
            .collect();
        std::fs::write(dir.join("runs.jsonl"), rs.join("\n")).expect("runs");
    }
    report
}

// ---------------------------------------------------------------------------------
// Analysis
// ---------------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
fn analyse(
    spec: &ShardSpec,
    runs: &[RunInfo],
    faults: &[Fault],
    reqs: &[ReqRec],
    charges: &HashMap<String, usize>,
    mock_requests: &HashMap<String, usize>,
    effects: &[EffectLog],
    attempts: &[AttemptLog],
    timed_out: bool,
    proxy_errors: u64,
    elapsed: Duration,
) -> Value {
    let mut failures: Vec<String> = Vec::new();
    if timed_out {
        failures.push("not every run reached a terminal state in time".into());
    }
    // Leader incarnations: kill times and ready times.
    let kills: Vec<(u64, u64)> = faults
        .iter()
        .filter(|f| f.kind.kills_leader())
        .map(|f| (f.t_ms, f.end_ms))
        .collect();
    let kill_in = |a: u64, b: u64| kills.iter().any(|&(k, _)| k >= a && k <= b);
    // Incarnation index of time t: number of kills at or before t.
    let incarnation = |t: u64| kills.iter().filter(|&&(k, _)| k <= t).count();
    // Whether t lies while a leader was up (not between a kill and its restart).
    let leader_up = |t: u64| !kills.iter().any(|&(k, r)| t >= k && t < r);

    let by_run: HashMap<&str, &RunInfo> = runs.iter().map(|r| (r.id.as_str(), r)).collect();
    let key_run: HashMap<&str, &RunInfo> = runs
        .iter()
        .filter_map(|r| r.key.as_deref().map(|k| (k, r)))
        .collect();

    // ---- runs, charges, lost
    let mut lost = 0u64;
    let mut completed = 0u64;
    let mut charges_one = 0u64;
    let mut double_charges = 0u64;
    let mut latencies: Vec<u64> = Vec::new();
    for r in runs {
        if r.id.is_empty() {
            lost += 1;
            failures.push(format!("{}: start never accepted", r.start_key));
            continue;
        }
        let ok_status = r.status == "completed";
        if ok_status {
            completed += 1;
        }
        let ch = r
            .key
            .as_ref()
            .and_then(|k| charges.get(k))
            .copied()
            .unwrap_or(0);
        if ch == 1 {
            charges_one += 1;
        }
        if ch > 1 {
            double_charges += 1;
            failures.push(format!("run {} key {:?}: {ch} charges", r.id, r.key));
        }
        if !ok_status || r.completed_events != 1 || r.result_status != Some(200) || ch == 0 {
            lost += 1;
            failures.push(format!(
                "run {} lost: status {:?}, run_completed x{}, result status {:?}, charges {ch}",
                r.id, r.status, r.completed_events, r.result_status
            ));
        }
        if let Some(l) = r.latency_ms {
            latencies.push(l);
        }
    }
    let distinct: HashSet<&str> = key_run.keys().copied().collect();
    if distinct.len() != runs.iter().filter(|r| r.key.is_some()).count() {
        failures.push("two runs share an idempotency key".into());
    }
    // Keys the provider saw that belong to no run of this shard.
    let orphan: HashSet<&str> = reqs
        .iter()
        .filter_map(|r| r.key.as_deref())
        .filter(|k| !key_run.contains_key(k))
        .collect();
    if !orphan.is_empty() {
        failures.push(format!("{} provider keys belong to no run", orphan.len()));
    }

    // ---- per-key request lists
    let mut per_key: BTreeMap<&str, Vec<&ReqRec>> = BTreeMap::new();
    for r in reqs.iter().filter(|r| r.path == "/pay") {
        if let Some(k) = r.key.as_deref() {
            per_key.entry(k).or_default().push(r);
        }
    }
    for v in per_key.values_mut() {
        v.sort_by_key(|r| r.t_ms);
    }
    // Proxy request counts agree with the MockProvider's own counters.
    for (k, n) in mock_requests {
        let seen = per_key.get(k.as_str()).map_or(0, Vec::len);
        if seen != *n {
            failures.push(format!(
                "key {k}: proxy saw {seen} requests, MockProvider counted {n}"
            ));
        }
    }

    // ---- fenced sends
    let mut effects_by_run: HashMap<&str, Vec<&EffectLog>> = HashMap::new();
    for e in effects {
        effects_by_run.entry(e.run.as_str()).or_default().push(e);
    }
    let mut fenced_sends = 0u64;
    let mut rejections = 0u64;
    let mut fenced_detail: Vec<Value> = Vec::new();
    for e in effects.iter().filter(|e| !e.accepted && leader_up(e.t_ms)) {
        if by_run.contains_key(e.run.as_str()) {
            rejections += 1;
        }
    }
    for (k, list) in &per_key {
        let Some(run) = key_run.get(k) else { continue };
        let evs = effects_by_run.get(run.id.as_str());
        for (i, r) in list.iter().enumerate() {
            let inc = incarnation(r.t_ms);
            let rejected_before = evs.and_then(|evs| {
                evs.iter().find(|e| {
                    !e.accepted
                        && leader_up(e.t_ms)
                        && e.t_ms <= r.t_ms
                        && incarnation(e.t_ms) == inc
                })
            });
            let Some(rejection) = rejected_before else {
                continue;
            };
            let accepted_in_inc =
                evs.is_some_and(|evs| evs.iter().any(|e| e.accepted && incarnation(e.t_ms) == inc));
            let inc_start = if inc == 0 { 0 } else { kills[inc - 1].0 };
            let redrive = list[..i].iter().any(|p| p.t_ms < inc_start);
            let retry = i > 0
                && incarnation(list[i - 1].t_ms) == inc
                && matches!(list[i - 1].status, 409 | 503 | 0);
            if !(accepted_in_inc || redrive || retry) {
                fenced_sends += 1;
                fenced_detail.push(json!({"key": k, "run": run.id, "t_ms": r.t_ms,
                    "rejected_by": rejection.worker, "rejection": rejection.line}));
            }
        }
    }
    if fenced_sends > 0 {
        failures.push(format!("{fenced_sends} fenced sends: {fenced_detail:?}"));
    }

    // ---- duplicates
    let mut raw_duplicates = 0u64;
    let mut dup_no_kill = 0u64;
    let mut by_leader_kill = 0u64;
    let mut by_retry_409 = 0u64;
    let mut unattributed = 0u64;
    let mut dup_keys: Vec<Value> = Vec::new();
    for (k, list) in &per_key {
        if list.len() < 2 {
            continue;
        }
        let run = key_run.get(k);
        let first = list[0].t_ms;
        let completed_at = run.and_then(|r| r.effect_completed_ms).unwrap_or(u64::MAX);
        let op = run.and_then(|r| r.effect_op);
        let key_kill = kill_in(first.saturating_sub(KILL_SLACK_MS), completed_at);
        let mut causes = Vec::new();
        for i in 1..list.len() {
            raw_duplicates += 1;
            let r = list[i];
            if !kill_in(first.saturating_sub(KILL_SLACK_MS), r.t_ms) {
                dup_no_kill += 1;
            }
            let prev = list[i - 1];
            let retry = prev.status == 409
                && attempts.iter().any(|a| {
                    a.status == 409
                        && Some(a.op) == op
                        && a.t_ms + 2000 >= prev.done_ms
                        && a.t_ms <= prev.done_ms + 2000
                });
            let cause = if key_kill {
                by_leader_kill += 1;
                "leader_kill"
            } else if retry {
                by_retry_409 += 1;
                "retry_409"
            } else {
                unattributed += 1;
                "unattributed"
            };
            causes.push(cause);
        }
        dup_keys.push(json!({
            "key": k,
            "run": run.map(|r| r.id.clone()),
            "requests": list.len(),
            "statuses": list.iter().map(|r| r.status).collect::<Vec<_>>(),
            "t_ms": list.iter().map(|r| r.t_ms).collect::<Vec<_>>(),
            "effect_completed_ms": run.and_then(|r| r.effect_completed_ms),
            "causes": causes,
        }));
    }
    if unattributed > 0 {
        failures.push(format!("{unattributed} unattributed duplicate requests"));
    }
    if dup_no_kill > 0 {
        failures.push(format!(
            "{dup_no_kill} raw duplicates without a leader kill in their window"
        ));
    }
    if proxy_errors > 0 {
        // A broken client connection (leader killed) is expected; record it only.
        eprintln!(
            "fencing_chaos shard {:02}: {proxy_errors} proxy connections ended with an error",
            spec.shard
        );
    }

    let mut faults_by: BTreeMap<&str, u64> = Kind::ALL.iter().map(|k| (k.name(), 0)).collect();
    for f in faults {
        *faults_by.entry(f.kind.name()).or_default() += 1;
    }
    latencies.sort_unstable();
    let median = latencies.get(latencies.len() / 2).copied().unwrap_or(0);
    let max = latencies.last().copied().unwrap_or(0);
    failures.truncate(200);
    json!({
        "shard": spec.shard,
        "seed": format!("{:#x}", spec.seed),
        "pass": failures.is_empty(),
        "runs": runs.len(),
        "completed": completed,
        "charges_eq_1": charges_one,
        "double_charges": double_charges,
        "lost": lost,
        "fenced_sends": fenced_sends,
        "start_effect_rejections": rejections,
        "start_effect_accepts": effects.iter().filter(|e| e.accepted).count(),
        "raw_duplicates": raw_duplicates,
        "raw_duplicates_without_leader_kill": dup_no_kill,
        "unattributed_duplicates": unattributed,
        "duplicates_by_cause": {
            "leader_kill": by_leader_kill,
            "retry_409": by_retry_409,
            "unattributed": unattributed,
        },
        "provider_requests": reqs.iter().filter(|r| r.path == "/pay").count(),
        "provider_409": reqs.iter().filter(|r| r.status == 409).count(),
        "provider_replays": reqs.iter().filter(|r| r.replayed).count(),
        "bridge_409_attempt_lines": attempts.iter().filter(|a| a.status == 409).count(),
        "bridge_retry_attempt_lines": attempts.iter().filter(|a| a.attempt > 1).count(),
        "proxy_errors": proxy_errors,
        "faults": faults_by,
        "fault_log": faults.iter().map(Fault::json).collect::<Vec<_>>(),
        "duplicate_keys": dup_keys,
        "latency_ms": {"median": median, "max": max},
        "duration_s": elapsed.as_secs_f64(),
        "failures": failures,
    })
}

// ---------------------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------------------

fn sum(shards: &[Value], k: &str) -> u64 {
    shards.iter().map(|s| s[k].as_u64().unwrap_or(0)).sum()
}

fn aggregate(cfg: &ChaosCfg) -> (Value, String) {
    let mut shards = Vec::new();
    let mut missing = Vec::new();
    for i in 0..cfg.shards {
        match std::fs::read(cfg.shard_file(i)) {
            Ok(b) => shards.push(serde_json::from_slice::<Value>(&b).expect("shard json")),
            Err(_) => missing.push(i),
        }
    }
    let mut faults: BTreeMap<String, u64> =
        Kind::ALL.iter().map(|k| (k.name().into(), 0)).collect();
    let mut causes: BTreeMap<String, u64> = BTreeMap::new();
    let mut medians = Vec::new();
    let mut max_latency = 0;
    for s in &shards {
        if let Some(o) = s["faults"].as_object() {
            for (k, v) in o {
                *faults.entry(k.clone()).or_default() += v.as_u64().unwrap_or(0);
            }
        }
        if let Some(o) = s["duplicates_by_cause"].as_object() {
            for (k, v) in o {
                *causes.entry(k.clone()).or_default() += v.as_u64().unwrap_or(0);
            }
        }
        medians.push(s["latency_ms"]["median"].as_u64().unwrap_or(0));
        max_latency = max_latency.max(s["latency_ms"]["max"].as_u64().unwrap_or(0));
    }
    medians.sort_unstable();
    let failed: Vec<Value> = shards
        .iter()
        .filter(|s| s["pass"] != true)
        .map(|s| json!({"shard": s["shard"], "seed": s["seed"], "failures": s["failures"]}))
        .collect();
    let pass = failed.is_empty() && missing.is_empty();
    let report = json!({
        "pass": pass,
        "seed": format!("{:#x}", cfg.seed),
        "shards": shards.len(),
        "shards_expected": cfg.shards,
        "missing_shards": missing,
        "runs": sum(&shards, "runs"),
        "completed": sum(&shards, "completed"),
        "charges_eq_1": sum(&shards, "charges_eq_1"),
        "double_charges": sum(&shards, "double_charges"),
        "lost": sum(&shards, "lost"),
        "fenced_sends": sum(&shards, "fenced_sends"),
        "start_effect_rejections": sum(&shards, "start_effect_rejections"),
        "raw_duplicates": sum(&shards, "raw_duplicates"),
        "raw_duplicates_without_leader_kill": sum(&shards, "raw_duplicates_without_leader_kill"),
        "unattributed_duplicates": sum(&shards, "unattributed_duplicates"),
        "duplicates_by_cause": causes,
        "provider_requests": sum(&shards, "provider_requests"),
        "provider_409": sum(&shards, "provider_409"),
        "faults": faults,
        "latency_ms": {
            "median_of_shard_medians": medians.get(medians.len() / 2).copied().unwrap_or(0),
            "max": max_latency,
        },
        "duration_s_total": shards.iter().map(|s| s["duration_s"].as_f64().unwrap_or(0.0)).sum::<f64>(),
        "failed_shards": failed,
    });
    let mut t = String::new();
    t.push_str("QA-03 fencing chaos report (D-W11)\n");
    t.push_str(&format!(
        "shards {}/{} seed {}  verdict {}\n",
        shards.len(),
        cfg.shards,
        report["seed"].as_str().unwrap_or(""),
        if pass { "PASS" } else { "FAIL" }
    ));
    for k in [
        "runs",
        "completed",
        "charges_eq_1",
        "double_charges",
        "lost",
        "fenced_sends",
        "start_effect_rejections",
        "raw_duplicates",
        "raw_duplicates_without_leader_kill",
        "unattributed_duplicates",
        "provider_requests",
        "provider_409",
    ] {
        t.push_str(&format!("  {k:<38} {}\n", report[k]));
    }
    t.push_str("  faults by kind:\n");
    for (k, v) in &faults {
        t.push_str(&format!("    {k:<36} {v}\n"));
    }
    t.push_str("  duplicates by cause:\n");
    for (k, v) in &causes {
        t.push_str(&format!("    {k:<36} {v}\n"));
    }
    t.push_str(&format!(
        "  run latency ms: median (of shard medians) {}, max {}\n",
        report["latency_ms"]["median_of_shard_medians"], max_latency
    ));
    if !missing.is_empty() {
        t.push_str(&format!("  missing shards: {missing:?}\n"));
    }
    for f in &failed {
        t.push_str(&format!(
            "  FAILED shard {} seed {}\n",
            f["shard"], f["seed"]
        ));
    }
    (report, t)
}

// ---------------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------------

/// Spawned clusters are heavy: one test at a time per process.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn print_shard(s: &Value) {
    eprintln!(
        "fencing_chaos shard {:02}: pass {} runs {} completed {} charges==1 {} double {} lost {} \
         fenced {} StartEffect accepted {} rejected {} provider requests {} (409 {}, replays {}) \
         raw dups {} (no kill {}, unattributed {}) faults {} in {:.0}s",
        s["shard"].as_u64().unwrap_or(0),
        s["pass"],
        s["runs"],
        s["completed"],
        s["charges_eq_1"],
        s["double_charges"],
        s["lost"],
        s["fenced_sends"],
        s["start_effect_accepts"],
        s["start_effect_rejections"],
        s["provider_requests"],
        s["provider_409"],
        s["provider_replays"],
        s["raw_duplicates"],
        s["raw_duplicates_without_leader_kill"],
        s["unattributed_duplicates"],
        s["faults"],
        s["duration_s"].as_f64().unwrap_or(0.0),
    );
    if let Some(f) = s["failures"].as_array() {
        for l in f.iter().take(20) {
            eprintln!("    {l}");
        }
    }
}

/// 100 runs, one shard, every fault kind once (quick feedback; not ignored).
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn chaos_smoke() {
    let _g = SERIAL.lock().await;
    let out = std::env::temp_dir().join(format!("ostro-chaos-smoke-{}", std::process::id()));
    std::fs::create_dir_all(&out).expect("smoke dir");
    let s = run_shard(ShardSpec {
        shard: 0,
        runs: 100,
        seed: 0x0721_5A0E,
        submit_secs: 30,
        script: Some(Kind::ALL.to_vec()),
        gap_ms: (1500, 2500),
        out_dir: out.clone(),
    })
    .await;
    print_shard(&s);
    for k in Kind::ALL {
        assert_eq!(s["faults"][k.name()], 1, "{} exercised once: {s}", k.name());
    }
    assert_eq!(s["runs"], 100);
    assert_eq!(
        s["pass"], true,
        "chaos smoke failed (evidence in {out:?}): {s:#}"
    );
    let _ = std::fs::remove_dir_all(&out);
}

/// The full campaign: every pending shard (resumable).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "QA-03 campaign: spawns clusters for a long time; run via scripts/fencing-chaos.sh"]
async fn campaign() {
    let _g = SERIAL.lock().await;
    let cfg = ChaosCfg::from_env();
    std::fs::create_dir_all(&cfg.state_dir).expect("state dir");
    let ids: Vec<usize> = cfg
        .shard_ids
        .clone()
        .unwrap_or_else(|| (0..cfg.shards).collect())
        .into_iter()
        .filter(|&i| i < cfg.shards)
        .collect();
    let pending: Vec<usize> = ids
        .into_iter()
        .filter(|&i| {
            let done = cfg.shard_file(i).exists();
            if done {
                eprintln!("fencing_chaos: shard {i:02} already done, skipped");
            }
            !done
        })
        .collect();
    eprintln!(
        "fencing_chaos: {} runs in {} shards, {} pending, parallel {}, seed {:#x}, state {:?}",
        cfg.runs,
        cfg.shards,
        pending.len(),
        cfg.parallel,
        cfg.seed,
        cfg.state_dir
    );
    let sem = Arc::new(tokio::sync::Semaphore::new(cfg.parallel));
    let mut set = tokio::task::JoinSet::new();
    for i in pending {
        let (sem, cfg) = (Arc::clone(&sem), cfg.clone());
        set.spawn(async move {
            let _permit = sem.acquire_owned().await.expect("semaphore");
            let spec = ShardSpec {
                shard: i,
                runs: cfg.runs_of(i),
                seed: cfg.shard_seed(i),
                submit_secs: cfg.submit_secs,
                script: None,
                gap_ms: (2000, 5000),
                out_dir: cfg.state_dir.clone(),
            };
            let s = run_shard(spec).await;
            write_atomic(
                &cfg.shard_file(i),
                &serde_json::to_vec_pretty(&s).expect("json"),
            );
            print_shard(&s);
            s["pass"] == true
        });
    }
    let mut failed = 0;
    while let Some(r) = set.join_next().await {
        match r {
            Ok(true) => {}
            Ok(false) => failed += 1,
            Err(e) => panic!("shard task panicked: {e}"),
        }
    }
    assert_eq!(failed, 0, "{failed} shard(s) failed; see the shard files");
}

fn write_atomic(path: &Path, bytes: &[u8]) {
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, bytes).expect("write shard");
    std::fs::rename(&tmp, path).expect("rename shard");
}

/// Aggregate the shard files into `report.json` (+ a printed summary).
#[test]
#[ignore = "reads the campaign state dir; run after `campaign`"]
fn report() {
    let cfg = ChaosCfg::from_env();
    let (r, text) = aggregate(&cfg);
    std::fs::create_dir_all(&cfg.state_dir).expect("state dir");
    std::fs::write(
        cfg.state_dir.join("report.json"),
        serde_json::to_string_pretty(&r).expect("json"),
    )
    .expect("report.json");
    std::fs::write(cfg.state_dir.join("report.txt"), &text).expect("report.txt");
    println!("{text}");
    assert_eq!(r["pass"], true, "fencing chaos report: FAIL");
}

#[test]
fn log_parsers() {
    assert_eq!(rfc3339_ms("1970-01-01T00:00:01.5Z"), Some(1500));
    assert_eq!(
        rfc3339_ms("2026-11-21T00:00:00.000123Z"),
        Some(1_795_219_200_000)
    );
    let w = parse_worker_log(
        "w0",
        &[
            "\u{1b}[2m2026-11-21T00:00:00.010Z\u{1b}[0m DEBUG ostro_worker::effect_proxy: effect started on the leader run=r1 op=3".into(),
            "2026-11-21T00:00:00.020Z  WARN ostro_worker::effect_proxy: StartEffect failed run=r2 op=3 code=The system is not in a state required for the operation's execution msg=the lease of this token is no longer held".into(),
            "2026-11-21T00:00:00.030Z  WARN ostro_worker::effect_proxy: StartEffect failed run=r3 op=3 code=The service is currently unavailable msg=x".into(),
        ],
    );
    assert_eq!(w.len(), 2, "{w:?}");
    assert!(w[0].accepted && w[0].run == "r1" && w[0].t_ms == 1_795_219_200_010);
    assert!(!w[1].accepted && w[1].run == "r2");
    let a = parse_server_log(&[r#"{"timestamp":"2026-11-21T00:00:00.5Z","level":"DEBUG","fields":{"message":"http effect attempt","origin":"http://x","status":409,"attempt":2,"op":3},"target":"ostro_bridge::http::send"}"#.into()]);
    assert_eq!(a.len(), 1);
    assert_eq!((a[0].op, a[0].status, a[0].attempt), (3, 409, 2));
}
