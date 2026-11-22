//! QA-01 whole-server kill campaign: sharded, resumable runner + aggregate report (05-19).
//!
//! # Environment
//!
//! | variable | default | meaning |
//! |---|---|---|
//! | `OSTRO_CAMPAIGN_FAULTS` | 20 | total counted faults (= kills) over all shards |
//! | `OSTRO_CAMPAIGN_SHARDS` | 1 | shard count (the 05-20 gate uses 20) |
//! | `OSTRO_CAMPAIGN_SHARD_IDS` | all | subset to run now, e.g. `0-4` or `0,3,7-9` |
//! | `OSTRO_CAMPAIGN_PARALLEL` | min(6, cores/2) | shards run at once |
//! | `OSTRO_CAMPAIGN_STATE_DIR` | `target/campaign` | state dir (relative = repo root). Smoke: `target/campaign-smoke` (wiped first), timing: `target/campaign-timing` (wiped first), gate: `target/campaign-gate` (started clean by 05-20). Never share one between run types. |
//! | `OSTRO_CAMPAIGN_SEED` | 0x0519 | campaign seed |
//! | `OSTRO_CAMPAIGN_RATE` | 8 | new runs per second per shard |
//! | `OSTRO_CAMPAIGN_PHASE8` | off | `1`: Phase 8 scenarios on (see below); part of the resume identity |
//!
//! # One iteration = one counted fault, always ending in `kill -9`
//!
//! Each iteration picks a variant from its own seed: **plain** (60%) kills the clean server
//! at a uniformly random time (20..3000 ms) under load; **failpoint** (30%) restarts the
//! server with one site armed (`1%return`, `sleep` kill windows, 5% for the rarely hit blob
//! sites) and kills it 0..300 ms after the first injected fault shows up in its log, or after
//! 300..3000 ms; **enospc** (10%) does the same with `return(enospc)` at a WAL or meta write
//! site. `kills` grows only after waitpid reports death by SIGKILL. Injected faults seen in
//! the log before the kill are counted separately as `failpoint_hits`.
//!
//! After every kill the shard restarts **clean** (no FAILPOINTS), checks (a) every acked run
//! exists and (c) the journal invariants of every run not yet verified terminal, under load.
//! A plain next iteration reuses that server; a faulted one kills it (uncounted,
//! `extra_kills`) and restarts with its FAILPOINTS. Every 25 kills the dead data dir goes
//! through `rebuild_and_compare` (d). At the shard's end the generator stops, every intent is
//! driven to an ack (or a 409 rejection), and once every run is terminal (b) checks status
//! and result of every acked run, then (c) again and a final rebuild.
//!
//! `shard-NN.json` is rewritten (atomically) after every iteration and the next run resumes
//! from it. It records the run identity (`shards`, `faults`, `seed`); the runner refuses to
//! resume a file whose identity differs from the current environment.
//!
//! # Phase 8 scenarios (plan 08-20, `OSTRO_CAMPAIGN_PHASE8=1`)
//!
//! Every server of the shard runs with [`phase8_env`]: snapshots every second (32 KiB
//! threshold), bottomless backup into `<shard>/bucket` every 500 ms, blob GC every 2 s. The
//! variant mix becomes plain 50%, failpoint 18%, enospc 7% and **phase8** 25%, one entry of
//! [`PHASE8_MENU`]:
//!
//! - kill during snapshot: `snapshot::before_rename`, `snapshot::before_dir_fsync`
//!   (`return`), and a `sleep` window at `before_rename`;
//! - kill during trim: `trim::after_delete` (the first retention pass comes after the fixed
//!   60 s leader interval, so this one waits up to [`TRIM_WAIT`]);
//! - kill during upload: `upload::before_manifest` (`return`, and a `sleep` window);
//! - kill during GC: `blobgc::before_unlink` (`sleep` window) after [`plant_orphan_blobs`]
//!   planted 20 unreferenced, old blobs, so the first GC pass has unlink work;
//! - kill during install: `snapshot::install_before_swap` (`1*return`: the first install
//!   fails, startup falls back to the next older local snapshot or a full replay) after
//!   [`drop_partition_states`] removed every `state/pNNNN`, so startup installs the newest
//!   local snapshot of each partition. With a plain `return` every candidate fails, a
//!   trimmed partition legitimately reports `snapshot required` on the armed server and
//!   the armed start never reaches its ready line (five 60 s spawn attempts, then a plain
//!   fallback), so the scenario arms one failure only.
//!
//! Extra invariants after a phase8 kill: the clean restart never logs `snapshot required`
//! (a trimmed log always keeps a valid local snapshot), and the dead data dir goes through
//! `rebuild_and_compare` (digest equality of the live state and newest snapshot + log
//! tail) after **every** phase8 kill, not only every 25 kills. At the shard's end the bucket
//! is restored with `ostro restore --from file://<shard>/bucket` into a fresh directory; the
//! restore must pass its own state-hash and consistency checks.
//!
//! # Timing (05-19 measurements, WSL2, 12 cores, release failpoint binary)
//!
//! - Timing run (plan shape): 200 kills = 4 shards x 50, parallel 4, wiped
//!   `target/campaign-timing`: 2 min 15 s wall, 2.0-2.5 s per kill per shard (about 1,550
//!   kills/hour per shard, 5,300/hour aggregate), PASS.
//! - Gate-shaped batch: 3,000 kills = 6 shards x 500, parallel 6 (`target/campaign-timing500`,
//!   load average about 25 from other work on the machine): 28 min 16 s wall, 3.1-3.3 s per
//!   kill averaged over a shard (about 1,100-1,150 kills/hour per shard; it grows from about 2 s
//!   to about 3.5 s as a shard accumulates about 6,000 runs, because every post-kill check
//!   lists all runs), final settle + check 22-64 s per shard, peak harness + 6 servers RSS
//!   about 4 GB, 375 MB data per shard, PASS.
//! - Projection for the 05-20 gate (20 shards x 500, parallel 6): 4 batches (6+6+6+2) of
//!   about 28 min each, so about 1 h 55 min wall. With `OSTRO_CAMPAIGN_SHARD_IDS` batches of
//!   6 (0-5, 6-11, 12-17, 18-19), every batch fits a 30 min window and resumes cleanly if it
//!   is cut off.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::check::{
    Event, Kind, Ledger, RunView, Tally, Violation, check_exists, check_final, check_journal,
    is_terminal,
};
use crate::server_target::{
    KillOutcome, ServerTarget, deploy_crash_saga, failpoint_bin, kill_stale, repo_root,
};
use crate::workload::{
    AckLog, TRIM_WAIT, WORKFLOW, Workload, drop_partition_states, phase8_env, plant_orphan_blobs,
};
use crate::{Rng, rebuild_and_compare};

/// Measured timing, kept next to the code that 05-20 will run (filled in by 05-19).
pub const TIMING_NOTE: &str =
    "05-19 timing run: see 05-19-SUMMARY.md (200 kills, 4 shards x 50, parallel 4)";

/// Server partitions (`ostro dev` default).
pub const PARTITIONS: u16 = 16;
const READY_LIMIT: Duration = Duration::from_secs(60);
const SETTLE_LIMIT: Duration = Duration::from_secs(300);
const REBUILD_EVERY: u64 = 25;

// ------------------------------------------------------------------------ config

#[derive(Clone, Debug)]
pub struct CampaignCfg {
    pub faults: u64,
    pub shards: u32,
    pub shard_ids: Vec<u32>,
    pub parallel: usize,
    pub state_dir: PathBuf,
    pub seed: u64,
    pub rate: f64,
    /// Phase 8 scenarios (`OSTRO_CAMPAIGN_PHASE8=1`, plan 08-20).
    pub phase8: bool,
}

fn env_u64(name: &str, default: u64) -> Result<u64, String> {
    match std::env::var(name) {
        Ok(v) if !v.trim().is_empty() => {
            let v = v.trim();
            let parsed = match v.strip_prefix("0x") {
                Some(h) => u64::from_str_radix(h, 16),
                None => v.parse(),
            };
            parsed.map_err(|e| format!("{name}={v}: {e}"))
        }
        _ => Ok(default),
    }
}

/// `0-4`, `1,3`, `0,3,7-9` -> sorted unique ids, each `< shards`.
pub fn parse_shard_ids(s: &str, shards: u32) -> Result<Vec<u32>, String> {
    let mut out = BTreeSet::new();
    for part in s.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        let (a, b) = match part.split_once('-') {
            Some((a, b)) => (a.trim(), b.trim()),
            None => (part, part),
        };
        let a: u32 = a.parse().map_err(|e| format!("shard id {a:?}: {e}"))?;
        let b: u32 = b.parse().map_err(|e| format!("shard id {b:?}: {e}"))?;
        if a > b || b >= shards {
            return Err(format!("shard range {part:?} outside 0..{shards}"));
        }
        out.extend(a..=b);
    }
    Ok(out.into_iter().collect())
}

/// Kill quota of `shard`: `faults / shards`, the remainder spread over the first shards.
pub fn quota(faults: u64, shards: u32, shard: u32) -> u64 {
    let s = u64::from(shards);
    faults / s + u64::from(u64::from(shard) < faults % s)
}

/// Resolve a state dir: relative paths are relative to the repo root.
pub fn resolve_state_dir(p: &str) -> PathBuf {
    let p = PathBuf::from(p);
    if p.is_absolute() {
        p
    } else {
        repo_root().join(p)
    }
}

impl CampaignCfg {
    pub fn from_env() -> Result<Self, String> {
        let faults = env_u64("OSTRO_CAMPAIGN_FAULTS", 20)?;
        let shards = u32::try_from(env_u64("OSTRO_CAMPAIGN_SHARDS", 1)?)
            .map_err(|e| format!("OSTRO_CAMPAIGN_SHARDS: {e}"))?;
        if shards == 0 || faults == 0 {
            return Err("OSTRO_CAMPAIGN_SHARDS and OSTRO_CAMPAIGN_FAULTS must be >= 1".into());
        }
        let shard_ids = match std::env::var("OSTRO_CAMPAIGN_SHARD_IDS") {
            Ok(s) if !s.trim().is_empty() => parse_shard_ids(&s, shards)?,
            _ => (0..shards).collect(),
        };
        let cores = std::thread::available_parallelism().map_or(2, |n| n.get());
        let parallel = env_u64("OSTRO_CAMPAIGN_PARALLEL", (cores / 2).clamp(1, 6) as u64)? as usize;
        let state_dir = resolve_state_dir(
            &std::env::var("OSTRO_CAMPAIGN_STATE_DIR").unwrap_or_else(|_| "target/campaign".into()),
        );
        let seed = env_u64("OSTRO_CAMPAIGN_SEED", 0x0519)?;
        let rate = std::env::var("OSTRO_CAMPAIGN_RATE")
            .ok()
            .and_then(|v| v.trim().parse::<f64>().ok())
            .unwrap_or(8.0);
        let phase8 = std::env::var("OSTRO_CAMPAIGN_PHASE8")
            .is_ok_and(|v| matches!(v.trim(), "1" | "true" | "yes" | "on"));
        Ok(Self {
            faults,
            shards,
            shard_ids,
            parallel: parallel.max(1),
            state_dir,
            seed,
            rate,
            phase8,
        })
    }
}

// --------------------------------------------------------------------- shard state

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ByVariant {
    pub plain: u64,
    pub failpoint: u64,
    pub enospc: u64,
    #[serde(default)]
    pub phase8: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct FinalSummary {
    pub runs: u64,
    pub statuses: BTreeMap<String, u64>,
    pub signals_acked: u64,
    pub duplicate_start_acks: u64,
    pub secs: f64,
}

/// `shard-NN.json`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ShardState {
    // identity (resume guard)
    pub shards: u32,
    pub faults: u64,
    pub seed: u64,
    /// Phase 8 scenarios on (resume identity, plan 08-20).
    #[serde(default)]
    pub phase8: bool,
    pub shard: u32,
    pub quota: u64,
    // progress
    pub kills: u64,
    pub by_variant: ByVariant,
    pub by_site: BTreeMap<String, u64>,
    pub failpoint_hits: u64,
    pub extra_kills: u64,
    pub self_exits: u64,
    pub spawn_retries: u64,
    pub variant_fallbacks: u64,
    pub iterations: u64,
    pub last_seed: u64,
    pub rebuilds: u64,
    pub checks: u64,
    pub runs_acked: u64,
    pub tally: Tally,
    pub failures: Vec<String>,
    pub anomalies: Vec<String>,
    pub final_checked: bool,
    pub final_summary: Option<FinalSummary>,
    /// Seconds spent in kill iterations (all resumes), excluding the final settle.
    pub kill_secs: f64,
    pub max_restart_ms: u64,
    /// Phase 8: `ostro restore` of the shard's bucket at the shard's end passed.
    #[serde(default)]
    pub restore_checked: bool,
}

pub fn shard_path(state_dir: &Path, shard: u32) -> PathBuf {
    state_dir.join(format!("shard-{shard:02}.json"))
}

/// Load `shard-NN.json` if present, refusing a file from a different campaign identity.
pub fn load_shard(path: &Path, cfg: &CampaignCfg) -> Result<Option<ShardState>, String> {
    let bytes = match std::fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(format!("read {}: {e}", path.display())),
    };
    let s: ShardState =
        serde_json::from_slice(&bytes).map_err(|e| format!("parse {}: {e}", path.display()))?;
    if s.shards != cfg.shards
        || s.faults != cfg.faults
        || s.seed != cfg.seed
        || s.phase8 != cfg.phase8
    {
        return Err(format!(
            "refusing to resume {}: it belongs to SHARDS={} FAULTS={} SEED={:#x} PHASE8={}, this \
             run is SHARDS={} FAULTS={} SEED={:#x} PHASE8={}; wipe the state dir {} (or pick \
             another OSTRO_CAMPAIGN_STATE_DIR)",
            path.display(),
            s.shards,
            s.faults,
            s.seed,
            s.phase8,
            cfg.shards,
            cfg.faults,
            cfg.seed,
            cfg.phase8,
            cfg.state_dir.display()
        ));
    }
    Ok(Some(s))
}

pub fn save_shard(path: &Path, s: &ShardState) -> Result<(), String> {
    let tmp = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec_pretty(s).map_err(|e| e.to_string())?;
    {
        use std::io::Write;
        let mut f = std::fs::File::create(&tmp).map_err(|e| format!("{}: {e}", tmp.display()))?;
        f.write_all(&bytes).map_err(|e| e.to_string())?;
        f.sync_all().map_err(|e| e.to_string())?;
    }
    std::fs::rename(&tmp, path).map_err(|e| format!("rename {}: {e}", path.display()))
}

// ----------------------------------------------------------------------- variants

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Variant {
    Plain,
    Failpoint,
    Enospc,
    /// A Phase 8 storage scenario ([`PHASE8_MENU`], plan 08-20).
    Phase8,
}

/// Failpoint menu (05-18 sites + Phase 2 storage sites).
pub const FAILPOINT_MENU: &[&str] = &[
    "wal::write=1%return",
    "wal::fsync=1%return",
    "wal::dir_fsync=1%return",
    "state::commit=1%return",
    "state::persist=1%return",
    "blob::fsync=5%return",
    "blob::before_rename=5%return",
    "meta::log_append=1%return",
    "meta::fsync=1%return",
    "aot::write=return",
    "executor::before_append=1%return",
    "executor::after_append_before_done=1%return",
    "dispatcher::before_grant=1%return",
    "dispatcher::before_ack=1%return",
    "executor::after_append_before_done=5%sleep(150)",
    "executor::before_append=5%sleep(150)",
    "dispatcher::before_ack=5%sleep(150)",
];

/// ENOSPC menu: WAL and meta write sites (`return(enospc)` = half write + ENOSPC).
pub const ENOSPC_MENU: &[&str] = &[
    "wal::write=1%return(enospc)",
    "meta::log_append=1%return(enospc)",
    "meta::fsync=1%return(enospc)",
];

/// Phase 8 scenarios (plan 08-20): kills during snapshot, trim, upload, blob GC and
/// snapshot install. See [`Phase8Setup`] / [`phase8_setup`] for the per-site preparation and
/// kill window.
pub const PHASE8_MENU: &[&str] = &[
    "snapshot::before_rename=return",
    "snapshot::before_dir_fsync=return",
    "snapshot::before_rename=sleep(300)",
    "trim::after_delete=return",
    "upload::before_manifest=return",
    "upload::before_manifest=sleep(300)",
    "blobgc::before_unlink=sleep(200)",
    "snapshot::install_before_swap=1*return",
];

/// What a Phase 8 scenario prepares while the server is down.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase8Setup {
    None,
    /// [`plant_orphan_blobs`] (20 unreferenced, old blobs).
    PlantOrphans,
    /// [`drop_partition_states`] (startup installs local snapshots).
    DropStates,
}

/// `(setup, kill deadline range after ready in ms)` of a [`PHASE8_MENU`] entry. The kill
/// comes 0..300 ms after the first injected-fault log line, or at the deadline.
pub fn phase8_setup(spec: &str) -> (Phase8Setup, (u64, u64)) {
    if spec.starts_with("trim::") {
        // The first retention pass runs one fixed 60 s interval after the leader starts.
        let t = TRIM_WAIT.as_millis() as u64;
        (Phase8Setup::None, (t, t + 1))
    } else if spec.starts_with("blobgc::") {
        // The first GC pass runs 2 s after start; 20 unlinks x 200 ms follow.
        (Phase8Setup::PlantOrphans, (2_200, 6_000))
    } else if spec.starts_with("snapshot::install") {
        (Phase8Setup::DropStates, (300, 3_000))
    } else {
        // The first snapshot needs one 1 s interval; uploads every 500 ms.
        (Phase8Setup::None, (1_200, 4_000))
    }
}

/// [`pick_variant`], or with Phase 8 scenarios on: plain 50%, failpoint 18%, enospc 7%,
/// phase8 25%. With `phase8 == false` it is exactly [`pick_variant`] (the 05-19 mix).
pub fn pick_variant_cfg(
    seed: u64,
    shard: u32,
    iter: u64,
    phase8: bool,
) -> (u64, Variant, Option<&'static str>) {
    if !phase8 {
        return pick_variant(seed, shard, iter);
    }
    let s = seed ^ (u64::from(shard) << 40) ^ iter.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ 0x0820;
    let mut r = Rng::new(s);
    match r.below(100) {
        0..50 => (s, Variant::Plain, None),
        50..68 => (
            s,
            Variant::Failpoint,
            Some(FAILPOINT_MENU[r.below(FAILPOINT_MENU.len() as u64) as usize]),
        ),
        68..75 => (
            s,
            Variant::Enospc,
            Some(ENOSPC_MENU[r.below(ENOSPC_MENU.len() as u64) as usize]),
        ),
        _ => (
            s,
            Variant::Phase8,
            Some(PHASE8_MENU[r.below(PHASE8_MENU.len() as u64) as usize]),
        ),
    }
}

/// The variant and FAILPOINTS spec of iteration `iter` of `shard`.
pub fn pick_variant(seed: u64, shard: u32, iter: u64) -> (u64, Variant, Option<&'static str>) {
    let s = seed ^ (u64::from(shard) << 40) ^ iter.wrapping_mul(0x9e37_79b9_7f4a_7c15);
    let mut r = Rng::new(s);
    match r.below(100) {
        0..60 => (s, Variant::Plain, None),
        60..90 => (
            s,
            Variant::Failpoint,
            Some(FAILPOINT_MENU[r.below(FAILPOINT_MENU.len() as u64) as usize]),
        ),
        _ => (
            s,
            Variant::Enospc,
            Some(ENOSPC_MENU[r.below(ENOSPC_MENU.len() as u64) as usize]),
        ),
    }
}

// ------------------------------------------------------------------------ checks

async fn get_json(http: &reqwest::Client, url: &str) -> Result<(u16, Value), String> {
    let mut last = String::new();
    for i in 0..8u64 {
        match http.get(url).timeout(Duration::from_secs(20)).send().await {
            Ok(r) => {
                let st = r.status().as_u16();
                if st < 500 {
                    let v = r.json::<Value>().await.unwrap_or(Value::Null);
                    return Ok((st, v));
                }
                last = format!("{st} {}", r.text().await.unwrap_or_default());
            }
            Err(e) => last = e.to_string(),
        }
        tokio::time::sleep(Duration::from_millis(100 * (i + 1))).await;
    }
    Err(format!("GET {url}: {last}"))
}

/// Every crash-saga run (`run_id -> status`).
pub async fn list_runs(
    http: &reqwest::Client,
    base: &str,
) -> Result<BTreeMap<String, String>, String> {
    let mut out = BTreeMap::new();
    let mut token: Option<String> = None;
    loop {
        let mut url = format!("{base}/v1/runs?workflow={WORKFLOW}&limit=1000");
        if let Some(t) = &token {
            url.push_str("&page_token=");
            url.push_str(t);
        }
        let (st, v) = get_json(http, &url).await?;
        if st != 200 {
            return Err(format!("list runs: {st} {v}"));
        }
        for r in v["runs"].as_array().into_iter().flatten() {
            if let (Some(id), Some(s)) = (r["run_id"].as_str(), r["status"].as_str()) {
                out.insert(id.to_owned(), s.to_owned());
            }
        }
        match v["next_page_token"].as_str() {
            Some(t) if !t.is_empty() => token = Some(t.to_owned()),
            _ => return Ok(out),
        }
    }
}

/// A run's whole journal via the history API (`None` = 404).
pub async fn history(
    http: &reqwest::Client,
    base: &str,
    run_id: &str,
) -> Result<Option<Vec<Event>>, String> {
    let mut out = Vec::new();
    let mut from = 0u64;
    loop {
        let (st, v) = get_json(
            http,
            &format!("{base}/v1/runs/{run_id}/history?limit=1000&from_seq={from}"),
        )
        .await?;
        match st {
            200 => {}
            404 => return Ok(None),
            _ => return Err(format!("history {run_id}: {st} {v}")),
        }
        for e in v["events"].as_array().into_iter().flatten() {
            out.push(serde_json::from_value::<Event>(e.clone()).map_err(|e| e.to_string())?);
        }
        match v["next_from_seq"].as_u64() {
            Some(n) => from = n,
            None => return Ok(Some(out)),
        }
    }
}

/// `GET /v1/runs/{id}` (`None` = 404).
pub async fn get_run(
    http: &reqwest::Client,
    base: &str,
    run_id: &str,
) -> Result<Option<RunView>, String> {
    let (st, v) = get_json(http, &format!("{base}/v1/runs/{run_id}")).await?;
    match st {
        200 => serde_json::from_value(v)
            .map(Some)
            .map_err(|e| e.to_string()),
        404 => Ok(None),
        _ => Err(format!("get {run_id}: {st} {v}")),
    }
}

fn check_error(run_id: &str, e: String) -> Violation {
    Violation {
        kind: Kind::CheckError,
        run_id: run_id.to_owned(),
        detail: e,
    }
}

/// (a) + (c) after a restart. `verified` holds runs whose terminal journal already passed
/// (c); they are only re-checked for existence.
async fn check_after_restart(
    http: &reqwest::Client,
    base: &str,
    ledger: &Ledger,
    verified: &mut HashSet<String>,
    anomalies: &mut Vec<String>,
) -> Vec<Violation> {
    let listed = match list_runs(http, base).await {
        Ok(l) => l,
        Err(e) => return vec![check_error("", e)],
    };
    let mut existing: BTreeSet<String> = listed.keys().cloned().collect();
    // A run missing from the list is confirmed with GET before it counts as lost.
    for (id, _) in ledger.acked() {
        if !existing.contains(id) {
            match get_run(http, base, id).await {
                Ok(Some(_)) => {
                    anomalies.push(format!("list omitted acked run {id} (GET found it)"));
                    existing.insert(id.to_owned());
                }
                Ok(None) => {}
                Err(e) => return vec![check_error(id, e)],
            }
        }
    }
    let mut out = check_exists(ledger, &existing);
    let todo: Vec<(String, bool)> = ledger
        .acked()
        .filter(|(id, _)| !verified.contains(*id) && existing.contains(*id))
        .map(|(id, _)| {
            (
                id.to_owned(),
                listed.get(id).is_some_and(|s| is_terminal(s)),
            )
        })
        .collect();
    let results = par_map(todo, 16, {
        let http = http.clone();
        let base = base.to_owned();
        move |(id, terminal)| {
            let http = http.clone();
            let base = base.clone();
            async move {
                let h = history(&http, &base, &id).await;
                (id, terminal, h)
            }
        }
    })
    .await;
    for (id, terminal, h) in results {
        match h {
            Ok(Some(events)) => {
                let vs = check_journal(&id, &events, ledger.get(&id));
                if vs.is_empty() && terminal {
                    verified.insert(id);
                }
                out.extend(vs);
            }
            Ok(None) => out.push(Violation {
                kind: Kind::LostAckedRun,
                run_id: id.clone(),
                detail: "listed but history 404".into(),
            }),
            Err(e) => out.push(check_error(&id, e)),
        }
    }
    out
}

/// Map `f` over `items` with at most `n` in flight.
async fn par_map<T, R, F, Fut>(items: Vec<T>, n: usize, f: F) -> Vec<R>
where
    T: Send + 'static,
    R: Send + 'static,
    F: Fn(T) -> Fut,
    Fut: std::future::Future<Output = R> + Send + 'static,
{
    let mut set = tokio::task::JoinSet::new();
    let mut out = Vec::with_capacity(items.len());
    for it in items {
        if set.len() >= n
            && let Some(r) = set.join_next().await
        {
            out.push(r.expect("check task"));
        }
        set.spawn(f(it));
    }
    while let Some(r) = set.join_next().await {
        out.push(r.expect("check task"));
    }
    out
}

// ------------------------------------------------------------------------ runner

struct Shard {
    cfg: CampaignCfg,
    id: u32,
    dir: PathBuf,
    data: PathBuf,
    state_path: PathBuf,
    bin: PathBuf,
    http: reqwest::Client,
    st: ShardState,
    /// `OSTRO_*` config overrides of every server of the shard (Phase 8: [`phase8_env`]).
    env: Vec<(String, String)>,
}

impl Shard {
    fn log(&self, msg: &str) {
        println!("[shard {:02}] {msg}", self.id);
    }

    fn save(&self) -> Result<(), String> {
        save_shard(&self.state_path, &self.st)
    }

    /// Spawn with `fp` (retrying a server that exits before ready up to 5 times).
    async fn spawn(&mut self, fp: Option<&str>) -> Result<ServerTarget, String> {
        let mut last = String::new();
        for _ in 0..5 {
            match ServerTarget::spawn_env(
                &self.bin,
                &self.data,
                fp,
                &self.env,
                &self.http,
                READY_LIMIT,
            )
            .await
            {
                Ok(s) => {
                    return Ok(s);
                }
                Err(e) => {
                    self.st.spawn_retries += 1;
                    last = e;
                }
            }
        }
        Err(last)
    }

    async fn spawn_clean(&mut self) -> Result<ServerTarget, String> {
        let t = Instant::now();
        let s = self
            .spawn(None)
            .await
            .map_err(|e| format!("clean restart failed: {e}"))?;
        self.st.max_restart_ms = self.st.max_restart_ms.max(t.elapsed().as_millis() as u64);
        Ok(s)
    }

    fn record(&mut self, vs: Vec<Violation>, when: &str) -> bool {
        if vs.is_empty() {
            return true;
        }
        self.st.tally.add(&vs);
        for x in vs.iter().take(50) {
            self.st.failures.push(format!("{when}: {x}"));
        }
        if vs.len() > 50 {
            self.st
                .failures
                .push(format!("{when}: ... {} more", vs.len() - 50));
        }
        for x in &vs {
            self.log(&format!("VIOLATION {when}: {x}"));
        }
        false
    }

    async fn run(mut self) -> Result<ShardState, String> {
        if self.st.final_checked {
            self.log("already complete");
            return Ok(self.st);
        }
        for pid in kill_stale(&self.data) {
            self.log(&format!(
                "killed stale server pid {pid} from an earlier harness"
            ));
        }
        let log = Arc::new(
            AckLog::open(&self.dir.join("acks.jsonl")).map_err(|e| format!("ack log: {e}"))?,
        );
        let wl = Workload::new(
            self.cfg.seed ^ u64::from(self.id),
            format!("s{:02}", self.id),
            Arc::clone(&log),
            self.http.clone(),
        );
        let handles = wl.spawn(self.cfg.rate, 8);
        let r = self.drive(&log, &wl).await;
        wl.stop();
        for h in handles {
            let _ = h.await;
        }
        self.st.anomalies.extend(
            wl.anomalies
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .drain(..)
                .take(100),
        );
        self.st.runs_acked = log.ledger().acked().count() as u64;
        self.save()?;
        r.map(|()| self.st)
    }

    async fn drive(&mut self, log: &Arc<AckLog>, wl: &Arc<Workload>) -> Result<(), String> {
        let mut verified = HashSet::new();
        let mut srv = self.spawn_clean().await?;
        deploy_crash_saga(&self.http, &srv.base).await?;
        wl.set_target(Some(srv.base.clone()));
        wl.set_generating(true);
        // Resume: check what an earlier harness left behind.
        if self.st.iterations > 0 {
            let vs = check_after_restart(
                &self.http,
                &srv.base,
                &log.ledger(),
                &mut verified,
                &mut self.st.anomalies,
            )
            .await;
            self.st.checks += 1;
            if !self.record(vs, "resume check") {
                self.save()?;
                return Err(format!("shard {}: violations, see shard file", self.id));
            }
        }
        let t_loop = Instant::now();
        let secs_base = self.st.kill_secs;
        while self.st.kills < self.st.quota {
            let iter = self.st.iterations;
            let (iseed, mut variant, mut spec) =
                pick_variant_cfg(self.cfg.seed, self.id, iter, self.cfg.phase8);
            let mut rng = Rng::new(iseed ^ 0x6b);
            let mut window = (300, 3000);
            if variant != Variant::Plain {
                wl.set_target(None);
                if let KillOutcome::Killed = srv.kill9().await? {
                    self.st.extra_kills += 1;
                }
                if variant == Variant::Phase8 {
                    let (setup, w) = phase8_setup(spec.unwrap_or_default());
                    window = w;
                    let prepared = match setup {
                        Phase8Setup::None => Ok(()),
                        Phase8Setup::PlantOrphans => {
                            plant_orphan_blobs(&self.data, 20, iseed).map(|_| ())
                        }
                        Phase8Setup::DropStates => drop_partition_states(&self.data).map(|_| ()),
                    };
                    prepared.map_err(|e| format!("phase8 setup for {spec:?}: {e}"))?;
                }
                match self.spawn(spec).await {
                    Ok(s) => srv = s,
                    Err(e) => {
                        self.log(&format!("cannot start with {spec:?} ({e}); plain fallback"));
                        self.st.variant_fallbacks += 1;
                        variant = Variant::Plain;
                        spec = None;
                        srv = self.spawn_clean().await?;
                    }
                }
                wl.set_target(Some(srv.base.clone()));
            }
            // Wait for the kill point.
            match variant {
                Variant::Plain => {
                    let at = srv.ready_at + Duration::from_millis(rng.range(20, 3000));
                    tokio::time::sleep_until(at.into()).await;
                }
                _ => {
                    let deadline =
                        srv.ready_at + Duration::from_millis(rng.range(window.0, window.1));
                    let after_hit = Duration::from_millis(rng.range(0, 300));
                    loop {
                        if srv.fault_lines() > 0 {
                            tokio::time::sleep(after_hit).await;
                            break;
                        }
                        if Instant::now() >= deadline || srv.exited() {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                }
            }
            let hits = srv.fault_lines();
            let outcome = srv.kill9().await?;
            wl.set_target(None);
            self.st.iterations += 1;
            self.st.last_seed = iseed;
            match outcome {
                KillOutcome::Killed => {
                    self.st.kills += 1;
                    match variant {
                        Variant::Plain => self.st.by_variant.plain += 1,
                        Variant::Failpoint => self.st.by_variant.failpoint += 1,
                        Variant::Enospc => self.st.by_variant.enospc += 1,
                        Variant::Phase8 => self.st.by_variant.phase8 += 1,
                    }
                    *self
                        .st
                        .by_site
                        .entry(spec.unwrap_or("plain").to_owned())
                        .or_insert(0) += 1;
                    self.st.failpoint_hits += hits;
                }
                KillOutcome::AlreadyExited => {
                    self.st.self_exits += 1;
                    self.log(&format!(
                        "server exited on its own under {spec:?} (not counted); tail:\n{}",
                        srv.tail(15)
                    ));
                }
            }
            // Phase 8: digest-after-rebuild after every phase8 kill (snapshot / trim / upload /
            // GC / install leave the dead data dir in their most delicate state).
            if outcome == KillOutcome::Killed
                && (self.st.kills.is_multiple_of(REBUILD_EVERY) || variant == Variant::Phase8)
            {
                self.st.rebuilds += 1;
                if let Err(e) = rebuild_and_compare(&self.data, PARTITIONS).await {
                    let vs = vec![check_error(
                        "",
                        format!("rebuild_and_compare after kill {}: {e}", self.st.kills),
                    )];
                    self.record(vs, "rebuild");
                    self.save()?;
                    return Err(format!("shard {}: rebuild mismatch: {e}", self.id));
                }
            }
            srv = self.spawn_clean().await?;
            wl.set_target(Some(srv.base.clone()));
            if self.cfg.phase8 && !self.record(snapshot_required(&srv), "clean restart") {
                self.save()?;
                return Err(format!(
                    "shard {}: 'snapshot required' with a valid snapshot",
                    self.id
                ));
            }
            let vs = check_after_restart(
                &self.http,
                &srv.base,
                &log.ledger(),
                &mut verified,
                &mut self.st.anomalies,
            )
            .await;
            self.st.checks += 1;
            self.st.kill_secs = secs_base + t_loop.elapsed().as_secs_f64();
            if !self.record(
                vs,
                &format!("after kill {} (iter {iter}, {spec:?})", self.st.kills),
            ) {
                self.save()?;
                return Err(format!("shard {}: violations, see shard file", self.id));
            }
            self.save()?;
            if self.st.kills.is_multiple_of(10) {
                self.log(&format!(
                    "kills {}/{} ({:.2} s/kill, acked runs {}, pending {})",
                    self.st.kills,
                    self.st.quota,
                    self.st.kill_secs / self.st.kills.max(1) as f64,
                    log.ledger().acked().count(),
                    wl.pending()
                ));
            }
        }
        self.finish(log, wl, srv).await
    }

    /// Shard end: settle, (b) + (c) for every acked run, final rebuild.
    async fn finish(
        &mut self,
        log: &Arc<AckLog>,
        wl: &Arc<Workload>,
        mut srv: ServerTarget,
    ) -> Result<(), String> {
        let t = Instant::now();
        wl.set_generating(false);
        let deadline = Instant::now() + SETTLE_LIMIT;
        while wl.pending() > 0 {
            if Instant::now() > deadline {
                return Err(format!(
                    "shard {}: {} requests still pending after the settle limit",
                    self.id,
                    wl.pending()
                ));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let ledger = log.ledger();
        let mut listed;
        loop {
            listed = list_runs(&self.http, &srv.base).await?;
            let open = ledger
                .acked()
                .filter(|(id, _)| listed.get(*id).is_some_and(|s| !is_terminal(s)))
                .count();
            if open == 0 || Instant::now() > deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        let mut anomalies = Vec::new();
        let mut verified = HashSet::new();
        let mut vs = check_after_restart(
            &self.http,
            &srv.base,
            &ledger,
            &mut verified,
            &mut anomalies,
        )
        .await;
        self.st.anomalies.extend(anomalies);
        let (final_vs, mut summary) = final_run_checks(&self.http, &srv.base, &ledger).await;
        vs.extend(final_vs);
        // Dedupe (check_after_restart and the final pass both check journals).
        let mut seen = HashSet::new();
        vs.retain(|x| seen.insert(format!("{x}")));
        if let KillOutcome::Killed = srv.kill9().await? {
            self.st.extra_kills += 1;
        }
        let ok = self.record(vs, "final");
        self.st.rebuilds += 1;
        if let Err(e) = rebuild_and_compare(&self.data, PARTITIONS).await {
            self.record(
                vec![check_error("", format!("final rebuild_and_compare: {e}"))],
                "final rebuild",
            );
            self.save()?;
            return Err(format!("shard {}: final rebuild mismatch: {e}", self.id));
        }
        if self.cfg.phase8 {
            match restore_bucket(&self.bin, &self.dir) {
                Ok(msg) => {
                    self.log(&msg);
                    self.st.restore_checked = true;
                }
                Err(e) => {
                    self.record(
                        vec![check_error("", format!("restore of the bucket: {e}"))],
                        "final restore",
                    );
                    self.save()?;
                    return Err(format!("shard {}: restore failed: {e}", self.id));
                }
            }
        }
        summary.secs = t.elapsed().as_secs_f64();
        self.log(&format!(
            "final: runs {} statuses {:?} ({:.1} s)",
            summary.runs, summary.statuses, summary.secs
        ));
        self.st.final_summary = Some(summary);
        self.st.final_checked = true;
        self.save()?;
        if ok {
            Ok(())
        } else {
            Err(format!(
                "shard {}: final check violations, see shard file",
                self.id
            ))
        }
    }
}

/// (b) + (c) for every acked run of `ledger` on a settled server: status and result
/// ([`check_final`]) and the journal invariants ([`check_journal`]).
async fn final_run_checks(
    http: &reqwest::Client,
    base: &str,
    ledger: &Ledger,
) -> (Vec<Violation>, FinalSummary) {
    let mut vs = Vec::new();
    let ids: Vec<String> = ledger.acked().map(|(id, _)| id.to_owned()).collect();
    let views = par_map(ids, 16, {
        let http = http.clone();
        let base = base.to_owned();
        move |id| {
            let http = http.clone();
            let base = base.clone();
            async move {
                let v = get_run(&http, &base, &id).await;
                let h = history(&http, &base, &id).await;
                (id, v, h)
            }
        }
    })
    .await;
    let mut summary = FinalSummary::default();
    for (id, view, h) in views {
        let l = ledger.get(&id).expect("acked run in ledger");
        match (view, h) {
            (Ok(Some(view)), Ok(Some(events))) => {
                *summary.statuses.entry(view.status.clone()).or_insert(0) += 1;
                vs.extend(check_final(&id, l, &view, &events));
                vs.extend(check_journal(&id, &events, Some(l)));
            }
            (Ok(None), _) | (_, Ok(None)) => {} // reported by check_exists
            (Err(e), _) | (_, Err(e)) => vs.push(check_error(&id, e)),
        }
        summary.runs += 1;
        summary.signals_acked += l.signals_acked.len() as u64;
        summary.duplicate_start_acks += l.start_acks.len().saturating_sub(1) as u64;
    }
    (vs, summary)
}

// ------------------------------------------------------------------- offline re-check

/// Copy `shard-NN.json` into `superseded` (kept as evidence; never overwritten).
fn supersede_state(cfg: &CampaignCfg, id: u32, superseded: &Path) -> Result<(), String> {
    std::fs::create_dir_all(superseded)
        .map_err(|e| format!("mkdir {}: {e}", superseded.display()))?;
    let src = shard_path(&cfg.state_dir, id);
    let dst = shard_path(superseded, id);
    if !dst.exists() {
        std::fs::copy(&src, &dst).map_err(|e| format!("copy {}: {e}", src.display()))?;
    }
    Ok(())
}

/// Offline re-check of a finished shard with the current checker (p8-gate). The shard's
/// data dir is copied (the original stays untouched), a clean server (no FAILPOINTS, no
/// `OSTRO_*` overrides, so no bucket) serves the copy, and the shard-end checks run again
/// against the stored ack log: (a) + (c) as after a restart, then (b) + (c) per acked run.
/// Only a shard whose recorded violations all came from its final check is re-checked;
/// the new final violations replace them in `shard-NN.json` (the old file is kept in
/// `superseded`). Returns the updated state and the violations found.
pub async fn recheck_shard(
    cfg: &CampaignCfg,
    id: u32,
    superseded: &Path,
) -> Result<(ShardState, Vec<Violation>), String> {
    let path = shard_path(&cfg.state_dir, id);
    let mut st = load_shard(&path, cfg)?.ok_or_else(|| format!("no {}", path.display()))?;
    if !st.final_checked {
        return Err(format!("shard {id} has no final check to re-run"));
    }
    if st
        .failures
        .iter()
        .any(|f| !f.starts_with("final: ") || f.contains(" more"))
        || st.tally.total() != st.failures.len() as u64
    {
        return Err(format!(
            "shard {id}: recorded violations are not all from the final check ({:?})",
            st.tally
        ));
    }
    let dir = cfg.state_dir.join(format!("shard-{id:02}"));
    let tmp = tempfile::tempdir_in(&cfg.state_dir).map_err(|e| e.to_string())?;
    let st_cp = std::process::Command::new("cp")
        .arg("-a")
        .arg(dir.join("data"))
        .arg(tmp.path().join("data"))
        .status()
        .map_err(|e| format!("cp: {e}"))?;
    if !st_cp.success() {
        return Err(format!("cp -a {}/data failed", dir.display()));
    }
    let ledger = Ledger::from_records(
        &crate::workload::read_records(&dir.join("acks.jsonl")).map_err(|e| e.to_string())?,
    );
    let http = reqwest::Client::builder()
        .pool_max_idle_per_host(16)
        .build()
        .map_err(|e| e.to_string())?;
    let mut srv = ServerTarget::spawn(
        &failpoint_bin(),
        &tmp.path().join("data"),
        None,
        &http,
        READY_LIMIT,
    )
    .await?;
    let mut anomalies = Vec::new();
    let mut vs = check_after_restart(
        &http,
        &srv.base,
        &ledger,
        &mut HashSet::new(),
        &mut anomalies,
    )
    .await;
    let (final_vs, summary) = final_run_checks(&http, &srv.base, &ledger).await;
    vs.extend(final_vs);
    let mut seen = HashSet::new();
    vs.retain(|x| seen.insert(format!("{x}")));
    let _ = srv.kill9().await?;
    supersede_state(cfg, id, superseded)?;
    let before = st.tally.clone();
    st.tally = Tally::default();
    st.tally.add(&vs);
    st.failures = vs.iter().take(50).map(|x| format!("final: {x}")).collect();
    if vs.len() > 50 {
        st.failures
            .push(format!("final: ... {} more", vs.len() - 50));
    }
    st.anomalies.extend(anomalies);
    st.anomalies.push(format!(
        "recheck: final check re-run offline (wait winner from the journal + lost-wake \
         check) on a copy of the data dir: runs {} statuses {:?}; tally before {before:?}",
        summary.runs, summary.statuses
    ));
    save_shard(&path, &st)?;
    Ok((st, vs))
}

/// Retry a shard's `rebuild_and_compare` that failed only because a `Node::open` timed out
/// (a harness timeout under host load, not a digest mismatch). The dead data dir is still
/// the one the failed rebuild saw (the shard stopped right there). On success the timeout
/// failure is removed from `shard-NN.json` (the old file is kept in `superseded`), so the
/// shard can resume its remaining kills; a mismatch or another timeout is returned as `Err`.
pub async fn retry_rebuild_timeout(
    cfg: &CampaignCfg,
    id: u32,
    superseded: &Path,
) -> Result<ShardState, String> {
    let path = shard_path(&cfg.state_dir, id);
    let mut st = load_shard(&path, cfg)?.ok_or_else(|| format!("no {}", path.display()))?;
    let [f] = st.failures.as_slice() else {
        return Err(format!(
            "shard {id}: expected exactly one failure, got {:?}",
            st.failures
        ));
    };
    if st.final_checked
        || !f.starts_with("rebuild: CheckError")
        || !f.ends_with("timed out")
        || st.tally.total() != 1
        || st.tally.other_violations != 1
    {
        return Err(format!("shard {id}: not a rebuild open timeout: {f}"));
    }
    let f = f.clone();
    let data = cfg.state_dir.join(format!("shard-{id:02}")).join("data");
    let t = Instant::now();
    rebuild_and_compare(&data, PARTITIONS).await?;
    supersede_state(cfg, id, superseded)?;
    st.failures.clear();
    st.tally = Tally::default();
    st.anomalies.push(format!(
        "retry: {f:?} was a Node::open timeout under host load; rebuild_and_compare of the \
         same dead data dir re-run on a quiet host passed in {:.1} s",
        t.elapsed().as_secs_f64()
    ));
    save_shard(&path, &st)?;
    Ok(st)
}

/// Phase 8 invariant: a clean restart never reports `snapshot required` (retention keeps a
/// valid local snapshot at or below `log_start`, so every partition can rebuild).
fn snapshot_required(srv: &ServerTarget) -> Vec<Violation> {
    srv.tail(400)
        .lines()
        .filter(|l| l.contains("snapshot required"))
        .take(3)
        .map(|l| check_error("", format!("clean restart logged: {l}")))
        .collect()
}

/// `ostro restore --from file://<shard>/bucket --data-dir <fresh> --json` with the shard's
/// (failpoint, unarmed) binary; Ok = exit 0 (manifest, state hashes at every verify offset
/// and the cross-partition consistency check all passed).
pub fn restore_bucket(bin: &Path, shard_dir: &Path) -> Result<String, String> {
    let bucket = shard_dir.join("bucket");
    if !bucket.join("manifest/current").is_file() {
        return Err(format!("no manifest/current in {}", bucket.display()));
    }
    let target = shard_dir.join("restored");
    let _ = std::fs::remove_dir_all(&target);
    let mut cmd = std::process::Command::new(bin);
    for (k, _) in std::env::vars_os() {
        if k.to_string_lossy().starts_with("OSTRO_") {
            cmd.env_remove(&k);
        }
    }
    let out = cmd
        .env_remove("FAILPOINTS")
        .arg("restore")
        .arg("--from")
        .arg(format!("file://{}", bucket.display()))
        .arg("--data-dir")
        .arg(&target)
        .arg("--json")
        .output()
        .map_err(|e| format!("spawn restore: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "ostro restore exited {}: {}{}",
            out.status,
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        ));
    }
    let v: Value = serde_json::from_slice(&out.stdout).map_err(|e| {
        format!(
            "restore report: {e}: {}",
            String::from_utf8_lossy(&out.stdout)
        )
    })?;
    let parts = v["partitions"].as_array().map_or(0, Vec::len);
    let _ = std::fs::remove_dir_all(&target);
    Ok(format!(
        "restore of the bucket ok: generation {} partitions {parts} total {} ms",
        v["generation"], v["durations"]["total_ms"]
    ))
}

/// Run (or resume) one shard to completion.
pub async fn run_shard(cfg: &CampaignCfg, id: u32) -> Result<ShardState, String> {
    let dir = cfg.state_dir.join(format!("shard-{id:02}"));
    std::fs::create_dir_all(&dir).map_err(|e| format!("mkdir {}: {e}", dir.display()))?;
    let state_path = shard_path(&cfg.state_dir, id);
    let st = match load_shard(&state_path, cfg)? {
        Some(s) => s,
        None => ShardState {
            shards: cfg.shards,
            faults: cfg.faults,
            seed: cfg.seed,
            phase8: cfg.phase8,
            shard: id,
            quota: quota(cfg.faults, cfg.shards, id),
            ..ShardState::default()
        },
    };
    let env = if cfg.phase8 {
        phase8_env(&dir)
    } else {
        Vec::new()
    };
    let http = reqwest::Client::builder()
        .pool_max_idle_per_host(16)
        .build()
        .map_err(|e| e.to_string())?;
    let shard = Shard {
        cfg: cfg.clone(),
        id,
        data: dir.join("data"),
        dir,
        state_path,
        bin: failpoint_bin(),
        http,
        st,
        env,
    };
    shard.run().await
}

/// Run every configured shard, `parallel` at a time. `Err` lists the failed shards.
pub async fn run_campaign(cfg: &CampaignCfg) -> Result<Vec<ShardState>, String> {
    std::fs::create_dir_all(&cfg.state_dir).map_err(|e| e.to_string())?;
    // Identity guard before anything starts.
    for &id in &cfg.shard_ids {
        load_shard(&shard_path(&cfg.state_dir, id), cfg)?;
    }
    let bin = failpoint_bin();
    crate::server_target::crash_saga_wasm();
    println!(
        "campaign: faults {} shards {} ids {:?} parallel {} seed {:#x} rate {}/s phase8 {} state {} bin {}",
        cfg.faults,
        cfg.shards,
        cfg.shard_ids,
        cfg.parallel,
        cfg.seed,
        cfg.rate,
        cfg.phase8,
        cfg.state_dir.display(),
        bin.display()
    );
    let sem = Arc::new(tokio::sync::Semaphore::new(cfg.parallel));
    let mut set = tokio::task::JoinSet::new();
    for &id in &cfg.shard_ids {
        let cfg = cfg.clone();
        let sem = Arc::clone(&sem);
        set.spawn(async move {
            let _p = sem.acquire_owned().await.expect("semaphore");
            (id, run_shard(&cfg, id).await)
        });
    }
    let mut ok = Vec::new();
    let mut errs = Vec::new();
    while let Some(r) = set.join_next().await {
        match r.map_err(|e| e.to_string()) {
            Ok((_, Ok(s))) => ok.push(s),
            Ok((id, Err(e))) => errs.push(format!("shard {id}: {e}")),
            Err(e) => errs.push(e),
        }
    }
    if errs.is_empty() {
        ok.sort_by_key(|s| s.shard);
        Ok(ok)
    } else {
        Err(errs.join("\n"))
    }
}

// ------------------------------------------------------------------------ report

/// Aggregate every `shard-NN.json` of the state dir. Returns the report text and whether
/// the campaign passed (no violation, no failure, every shard final-checked, kills >=
/// `cfg.faults`).
pub fn report(cfg: &CampaignCfg) -> Result<(String, bool), String> {
    let mut shards = Vec::new();
    for id in 0..cfg.shards {
        if let Some(s) = load_shard(&shard_path(&cfg.state_dir, id), cfg)? {
            shards.push(s);
        }
    }
    let mut tally = Tally::default();
    let (mut kills, mut hits, mut extra, mut rebuilds, mut runs) = (0u64, 0u64, 0u64, 0u64, 0u64);
    let mut by = ByVariant::default();
    let mut sites: BTreeMap<String, u64> = BTreeMap::new();
    let mut failures = Vec::new();
    let mut incomplete = Vec::new();
    let mut table = String::from(
        "shard  kills  plain  fp  enospc  hits  extra  rebuilds  runs  kill_s  s/kill  final\n",
    );
    for s in &shards {
        tally.merge(&s.tally);
        kills += s.kills;
        hits += s.failpoint_hits;
        extra += s.extra_kills;
        rebuilds += s.rebuilds;
        runs += s.runs_acked;
        by.plain += s.by_variant.plain;
        by.failpoint += s.by_variant.failpoint;
        by.enospc += s.by_variant.enospc;
        by.phase8 += s.by_variant.phase8;
        if cfg.phase8 && s.final_checked && !s.restore_checked {
            failures.push(format!("shard {}: bucket restore not checked", s.shard));
        }
        for (k, n) in &s.by_site {
            *sites.entry(k.clone()).or_insert(0) += n;
        }
        failures.extend(s.failures.iter().map(|f| format!("shard {}: {f}", s.shard)));
        if !s.final_checked {
            incomplete.push(s.shard);
        }
        table.push_str(&format!(
            "{:>5}  {:>5}  {:>5}  {:>2}  {:>6}  {:>4}  {:>5}  {:>8}  {:>4}  {:>6.0}  {:>6.2}  {}\n",
            s.shard,
            s.kills,
            s.by_variant.plain,
            s.by_variant.failpoint,
            s.by_variant.enospc,
            s.failpoint_hits,
            s.extra_kills,
            s.rebuilds,
            s.runs_acked,
            s.kill_secs,
            s.kill_secs / s.kills.max(1) as f64,
            if s.final_checked { "ok" } else { "pending" }
        ));
    }
    let missing = cfg.shards as usize - shards.len();
    let mut text = format!(
        "kills={kills} lost_acked_runs={} duplicate_journal_entries={} non_terminal_runs={} \
         lost_acked_signals={} wrong_results={} other_violations={}\n\
         faults_target={} shards={}/{} incomplete={incomplete:?} by_variant=plain:{} failpoint:{} \
         enospc:{} phase8:{} failpoint_hits={hits} extra_kills={extra} rebuilds={rebuilds} \
         acked_runs={runs} phase8_scenarios={}\n",
        tally.lost_acked_runs,
        tally.duplicate_journal_entries,
        tally.non_terminal_runs,
        tally.lost_acked_signals,
        tally.wrong_results,
        tally.other_violations,
        cfg.faults,
        shards.len(),
        cfg.shards,
        by.plain,
        by.failpoint,
        by.enospc,
        by.phase8,
        cfg.phase8,
    );
    text.push_str(&table);
    text.push_str("by site:\n");
    for (k, n) in &sites {
        text.push_str(&format!("  {n:>6}  {k}\n"));
    }
    for f in failures.iter().take(100) {
        text.push_str(&format!("FAILURE {f}\n"));
    }
    let pass = tally.total() == 0
        && failures.is_empty()
        && incomplete.is_empty()
        && missing == 0
        && kills >= cfg.faults;
    text.push_str(if pass {
        "RESULT: PASS\n"
    } else {
        "RESULT: FAIL\n"
    });
    Ok((text, pass))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(dir: &Path, shards: u32, faults: u64, seed: u64) -> CampaignCfg {
        CampaignCfg {
            faults,
            shards,
            shard_ids: (0..shards).collect(),
            parallel: 1,
            state_dir: dir.to_path_buf(),
            seed,
            rate: 1.0,
            phase8: false,
        }
    }

    #[test]
    fn quotas_sum_to_faults() {
        for (f, s) in [(20, 1), (10_000, 20), (203, 4), (3, 5)] {
            let sum: u64 = (0..s).map(|i| quota(f, s, i)).sum();
            assert_eq!(sum, f, "faults {f} shards {s}");
        }
        assert_eq!(quota(10_000, 20, 7), 500);
    }

    #[test]
    fn shard_ids_parse() {
        assert_eq!(parse_shard_ids("0-4", 20).unwrap(), vec![0, 1, 2, 3, 4]);
        assert_eq!(parse_shard_ids("0,3,7-9", 20).unwrap(), vec![0, 3, 7, 8, 9]);
        assert!(parse_shard_ids("5-25", 20).is_err());
        assert!(parse_shard_ids("4-2", 20).is_err());
    }

    #[test]
    fn variants_are_deterministic_and_mixed() {
        let mut n = [0u32; 3];
        for i in 0..1000 {
            let a = pick_variant(1, 2, i);
            assert_eq!(a, pick_variant(1, 2, i));
            n[a.1 as usize] += 1;
        }
        assert!(n.iter().all(|&c| c > 50), "{n:?}");
    }

    #[test]
    fn phase8_variants_cover_the_menu_and_keep_the_05_19_mix_when_off() {
        let mut sites = BTreeSet::new();
        let mut n = [0u32; 4];
        for i in 0..4000 {
            assert_eq!(pick_variant_cfg(3, 1, i, false), pick_variant(3, 1, i));
            let a = pick_variant_cfg(3, 1, i, true);
            assert_eq!(a, pick_variant_cfg(3, 1, i, true));
            n[a.1 as usize] += 1;
            if a.1 == Variant::Phase8 {
                sites.insert(a.2.expect("spec"));
            }
        }
        assert!(n.iter().all(|&c| c > 100), "{n:?}");
        assert_eq!(sites.len(), PHASE8_MENU.len(), "{sites:?}");
        for site in [
            "snapshot::before_rename",
            "snapshot::before_dir_fsync",
            "trim::after_delete",
            "upload::before_manifest",
            "blobgc::before_unlink",
            "snapshot::install_before_swap",
        ] {
            assert!(PHASE8_MENU.iter().any(|m| m.starts_with(site)), "{site}");
        }
        assert_eq!(
            phase8_setup("blobgc::before_unlink=sleep(200)").0,
            Phase8Setup::PlantOrphans
        );
        assert_eq!(
            phase8_setup("snapshot::install_before_swap=return").0,
            Phase8Setup::DropStates
        );
        assert!(phase8_setup("trim::after_delete=return").1.0 >= 60_000);
    }

    #[test]
    fn phase8_is_part_of_the_resume_identity() {
        let d = tempfile::tempdir().unwrap();
        let st = ShardState {
            shards: 1,
            faults: 20,
            seed: 7,
            phase8: true,
            quota: 20,
            ..ShardState::default()
        };
        let p = shard_path(d.path(), 0);
        save_shard(&p, &st).unwrap();
        let mut c = cfg(d.path(), 1, 20, 7);
        let e = load_shard(&p, &c).unwrap_err();
        assert!(
            e.contains("PHASE8=true") && e.contains("wipe the state dir"),
            "{e}"
        );
        c.phase8 = true;
        assert_eq!(load_shard(&p, &c).unwrap(), Some(st));
    }

    #[test]
    fn refuses_to_resume_a_different_identity() {
        let d = tempfile::tempdir().unwrap();
        let c = cfg(d.path(), 4, 200, 7);
        let st = ShardState {
            shards: 4,
            faults: 200,
            seed: 7,
            shard: 1,
            quota: 50,
            kills: 12,
            ..ShardState::default()
        };
        let p = shard_path(d.path(), 1);
        save_shard(&p, &st).unwrap();
        assert_eq!(load_shard(&p, &c).unwrap(), Some(st));
        for other in [
            cfg(d.path(), 20, 200, 7),
            cfg(d.path(), 4, 20, 7),
            cfg(d.path(), 4, 200, 8),
        ] {
            let e = load_shard(&p, &other).unwrap_err();
            assert!(
                e.contains("refusing to resume") && e.contains("wipe the state dir"),
                "{e}"
            );
        }
        // The report refuses too (it reads every shard file through the same guard).
        assert!(report(&cfg(d.path(), 4, 20, 7)).is_err());
    }

    #[test]
    fn report_fails_until_complete() {
        let d = tempfile::tempdir().unwrap();
        let c = cfg(d.path(), 2, 10, 1);
        let mut st = ShardState {
            shards: 2,
            faults: 10,
            seed: 1,
            shard: 0,
            quota: 5,
            kills: 5,
            final_checked: true,
            ..ShardState::default()
        };
        save_shard(&shard_path(d.path(), 0), &st).unwrap();
        let (t, pass) = report(&c).unwrap();
        assert!(!pass, "{t}");
        st.shard = 1;
        save_shard(&shard_path(d.path(), 1), &st).unwrap();
        let (t, pass) = report(&c).unwrap();
        assert!(pass, "{t}");
        assert!(
            t.contains("kills=10 lost_acked_runs=0 duplicate_journal_entries=0"),
            "{t}"
        );
        st.tally.duplicate_journal_entries = 1;
        save_shard(&shard_path(d.path(), 1), &st).unwrap();
        let (t, pass) = report(&c).unwrap();
        assert!(!pass && t.contains("duplicate_journal_entries=1"), "{t}");
    }
}
