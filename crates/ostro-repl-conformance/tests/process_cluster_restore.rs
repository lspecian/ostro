//! 14-30 Task 2 (BACK-05, D-P14-45, tier C): a whole 3-node cluster is rebuilt from its
//! bottomless backups and grows back to RF 3. `#[ignore]`:
//!
//! ```text
//! cargo build -p ostro --features failpoints --target-dir target/failpoints --locked
//! bash scripts/fetch-tools.sh weed && bash scripts/test-services.sh start s3
//! eval "$(bash scripts/test-services.sh env)"
//! cargo test -p ostro-repl-conformance --locked --test process_cluster_restore \
//!   -- --ignored --nocapture --test-threads=1
//! bash scripts/test-services.sh stop
//! ```
//!
//! (`OSTRO_FAILPOINT_BIN=<target dir>/debug/ostro` when the binary lives elsewhere.)
//!
//! Scenario (both variants share it): a static 3-node RF 3 cluster with `[backup]` pointed at
//! a unique prefix (`file://` in `file_bucket`; `s3://$OSTRO_TEST_S3_BUCKET/<unique>` on the
//! `OSTRO_TEST_S3_URL` endpoint, SeaweedFS, in `s3_bucket`, which prints `SKIP:` when the
//! variables are unset) runs the continuous cross-partition workload (`process_p13::Load`:
//! steps, durable sleeps and timers, signal waits, parents starting attached children on
//! other partitions) until every partition head and `meta/head` are in the bucket; then all
//! three nodes are killed with SIGKILL mid-load. `ostro restore --from <prefix> --data-dir
//! <seed> --node-id 1 --json` rebuilds one seed (the harness numbers nodes from 1, so the
//! seed keeps id 1; 2 and 3 become Removed tombstones). The seed starts alone on a new
//! bottomless prefix, `ostro cluster init` (idempotent) and two `ostro cluster join`s follow,
//! and every partition must reach RF 3 with a full ISR (bound 120 s; the D-P14-36 fallback
//! would be an explicit reassign, recorded when taken). Checks:
//!
//! - (a) every acknowledged run whose `StartRun` offset (from the old nodes' logs) is below
//!   its partition's cut is present and completes exactly once (journal check, `outcomes`);
//! - (b) acknowledged runs at or above the cut are absent (404) and counted as the RPO;
//! - (c) the Phase 12 final checks (replica identity below the HW, acked on disk, LA rule,
//!   ISR-only elections) over the seed and the joined nodes;
//! - (d) non-terminal runs at the cut complete after the restore (signal waits re-signalled);
//! - (e) new runs start and complete on the grown cluster;
//! - (f) the old ids 2 and 3 are Removed tombstones and the joiners got fresh ids;
//! - (g) the restored cluster writes to its new prefix only (the old prefix is unchanged).
//!
//! Report: `$OSTRO_P13_REPORT_DIR/process_cluster_restore/report.txt`.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use ostro_objstore::{Credentials, ObjectStore, StoreOptions, UrlStyle};
use ostro_repl_conformance::harness::inproc::Ack;
use ostro_repl_conformance::harness::p13::{NodeOpts, P13Harness};
use ostro_repl_conformance::harness::process::{ProcCluster, ProcOptions, failpoint_bin};
use ostro_repl_conformance::harness::{ClusterHarness, ClusterSpec};
use ostro_repl_conformance::scenarios::process::skip_reason;
use ostro_repl_conformance::scenarios::process::tools::{
    live, published_all, scan, wait_meta_leader,
};
use ostro_repl_conformance::scenarios::process_p13::{
    HEAL_T, Kind, Load, ScenarioFailure, Started, WORKLOAD_GUESTS, deploy_guests, final_checks,
    get, host_line, outcomes, post, spec3, status, wait_healthy, write_report,
};
use ostro_types::{Cmd, Offset};
use serde_json::{Value, json};
use tokio::time::Instant;

const TEST: &str = "process_cluster_restore";
const SEED: u64 = 1;
const RF_T: Duration = Duration::from_secs(120);
const RUN_T: Duration = Duration::from_secs(120);
const BUCKET_T: Duration = Duration::from_secs(90);

fn f(oracle: &'static str, detail: impl Into<String>) -> ScenarioFailure {
    ScenarioFailure {
        oracle,
        detail: detail.into(),
    }
}

/// Where the backups go.
struct Backend {
    label: &'static str,
    old_url: String,
    new_url: String,
    /// Server environment except the URL (`OSTRO_BACKUP__*`).
    server_env: Vec<(String, String)>,
    /// Extra `ostro restore` arguments and environment.
    restore_args: Vec<String>,
    restore_env: Vec<(String, String)>,
    store: StoreOptions,
    _dir: Option<tempfile::TempDir>,
}

impl Backend {
    fn file() -> Self {
        let dir = tempfile::Builder::new()
            .prefix("ostro-cluster-restore-")
            .tempdir()
            .expect("bucket dir");
        let url = |n: &str| format!("file://{}", dir.path().join(n).display());
        for n in ["old", "new"] {
            std::fs::create_dir_all(dir.path().join(n)).expect("mkdir");
        }
        Self {
            label: "file://",
            old_url: url("old"),
            new_url: url("new"),
            server_env: vec![("OSTRO_BACKUP__SEAL_INTERVAL".to_owned(), "300ms".to_owned())],
            restore_args: Vec::new(),
            restore_env: Vec::new(),
            store: StoreOptions::default(),
            _dir: Some(dir),
        }
    }

    fn s3() -> Option<Self> {
        let Ok(endpoint) = std::env::var("OSTRO_TEST_S3_URL") else {
            println!(
                "SKIP: OSTRO_TEST_S3_URL unset (start SeaweedFS with scripts/test-services.sh start s3)"
            );
            return None;
        };
        let var = |n: &str| {
            std::env::var(n).unwrap_or_else(|_| panic!("{n} must be set with OSTRO_TEST_S3_URL"))
        };
        let (key, secret, bucket) = (
            var("OSTRO_TEST_S3_ACCESS_KEY"),
            var("OSTRO_TEST_S3_SECRET_KEY"),
            var("OSTRO_TEST_S3_BUCKET"),
        );
        let unique = format!(
            "{}-{}",
            std::process::id(),
            std::time::SystemTime::now() // clock-exempt: unique test prefix
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        );
        let pair = |k: &str, v: &str| (k.to_owned(), v.to_owned());
        Some(Self {
            label: "s3 (SeaweedFS)",
            old_url: format!("s3://{bucket}/cluster-restore/{unique}/old"),
            new_url: format!("s3://{bucket}/cluster-restore/{unique}/new"),
            server_env: vec![
                pair("OSTRO_BACKUP__SEAL_INTERVAL", "300ms"),
                pair("OSTRO_BACKUP__ENDPOINT", &endpoint),
                pair("OSTRO_BACKUP__REGION", "us-east-1"),
                pair("OSTRO_BACKUP__URL_STYLE", "path"),
                pair("OSTRO_BACKUP__ALLOW_HTTP", "true"),
                pair("OSTRO_BACKUP__ACCESS_KEY_ID", &key),
                pair("OSTRO_BACKUP__SECRET_ACCESS_KEY", &secret),
            ],
            restore_args: vec![
                "--endpoint".into(),
                endpoint.clone(),
                "--region".into(),
                "us-east-1".into(),
                "--allow-http".into(),
            ],
            restore_env: vec![
                pair("OSTRO_BACKUP_ACCESS_KEY_ID", &key),
                pair("OSTRO_BACKUP_SECRET_ACCESS_KEY", &secret),
            ],
            store: StoreOptions {
                region: Some("us-east-1".into()),
                endpoint: Some(endpoint),
                url_style: UrlStyle::Path,
                credentials: Some(Credentials {
                    access_key_id: key,
                    secret_access_key: secret,
                    session_token: None,
                }),
                allow_http: true,
                ..StoreOptions::default()
            },
            _dir: None,
        })
    }

    fn env(&self, url: &str) -> Vec<(String, String)> {
        let mut e = self.server_env.clone();
        e.push(("OSTRO_BACKUP__URL".to_owned(), url.to_owned()));
        e
    }

    fn open(&self, url: &str) -> Arc<dyn ObjectStore> {
        ostro_objstore::open_url(url, &self.store).expect("open bucket")
    }
}

async fn keys(store: &Arc<dyn ObjectStore>) -> BTreeMap<String, u64> {
    store
        .list("", None)
        .await
        .expect("list bucket")
        .into_iter()
        .map(|m| (m.key, m.size))
        .collect()
}

/// Wait until every partition head and `meta/head` are in the bucket.
async fn wait_bucket(store: &Arc<dyn ObjectStore>, partitions: u16) -> Result<(), ScenarioFailure> {
    let t0 = Instant::now(); // clock-exempt: test timeout
    loop {
        let k = keys(store).await;
        let missing: Vec<String> = (0..partitions)
            .map(|p| format!("p/{p:04}/head"))
            .chain(std::iter::once("meta/head".to_owned()))
            .filter(|x| !k.contains_key(x))
            .collect();
        if missing.is_empty() {
            return Ok(());
        }
        if t0.elapsed() > BUCKET_T {
            return Err(f(
                "bottomless",
                format!("missing in the bucket after {BUCKET_T:?}: {missing:?}"),
            ));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// The `StartRun` offset of every run in the old nodes' logs (the first copy that has it).
fn start_offsets(old: &ProcCluster) -> BTreeMap<Vec<u8>, (u16, Offset)> {
    let mut out = BTreeMap::new();
    for n in 1..=3u64 {
        for p in 0..old.spec().partitions {
            let Ok(s) = scan(old, n, p) else { continue };
            for (off, bytes) in s.commands() {
                if let Ok(cmd) = ostro_types::command::decode_command(bytes)
                    && let Some(Cmd::StartRun(st)) = &cmd.cmd
                {
                    out.entry(st.run_id.clone()).or_insert((p, off));
                }
            }
        }
    }
    out
}

fn kind_of(workflow: &str) -> Option<Kind> {
    [
        Kind::Steps,
        Kind::Hot,
        Kind::Signal,
        Kind::Probe,
        Kind::Child,
    ]
    .into_iter()
    .find(|k| k.workflow() == workflow)
}

/// `GET /v1/runs/{id}` on any live node: `Some(run)` / `None` on 404.
async fn run_doc(c: &ProcCluster, id: &str) -> Result<Option<Value>, ScenarioFailure> {
    let path = format!("/v1/runs/{id}");
    let t0 = Instant::now(); // clock-exempt: test timeout
    loop {
        for n in live(c) {
            match get(c, n, &path).await {
                Some((200, v)) => return Ok(Some(v)),
                Some((404, _)) => return Ok(None),
                _ => {}
            }
        }
        if t0.elapsed() > RUN_T {
            return Err(f("availability", format!("GET {path}: no answer")));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Every partition at RF 3 with a full ISR.
async fn wait_rf3(c: &ProcCluster) -> Result<Duration, String> {
    let t0 = Instant::now(); // clock-exempt: measurement
    let mut last = String::new();
    while t0.elapsed() < RF_T {
        let parts = published_all(c);
        if parts
            .iter()
            .all(|a| a.replicas.len() == 3 && a.isr.len() == 3 && a.leader != 0)
        {
            return Ok(t0.elapsed());
        }
        last = format!(
            "{:?}",
            parts
                .iter()
                .map(|a| (a.partition, a.leader, a.replicas.clone(), a.isr.clone()))
                .collect::<Vec<_>>()
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Err(last)
}

fn run_restore(b: &Backend, data_dir: &Path, out: &mut String) -> Result<Value, ScenarioFailure> {
    let bin = failpoint_bin().ok_or_else(|| f("harness", "no ostro binary"))?;
    let mut cmd = std::process::Command::new(&bin);
    for (k, _) in std::env::vars_os() {
        if k.to_string_lossy().starts_with("OSTRO_") {
            cmd.env_remove(&k);
        }
    }
    cmd.env_remove("FAILPOINTS")
        .arg("restore")
        .arg("--from")
        .arg(&b.old_url)
        .arg("--data-dir")
        .arg(data_dir)
        .arg("--node-id")
        .arg(SEED.to_string())
        .arg("--json")
        .args(&b.restore_args);
    for (k, v) in &b.restore_env {
        cmd.env(k, v);
    }
    let t0 = Instant::now(); // clock-exempt: measurement
    let o = cmd
        .output()
        .map_err(|e| f("restore", format!("spawn ostro restore: {e}")))?;
    let took = t0.elapsed();
    if !o.status.success() {
        return Err(f(
            "restore",
            format!(
                "ostro restore failed ({}):\n{}\n{}",
                o.status,
                String::from_utf8_lossy(&o.stdout),
                String::from_utf8_lossy(&o.stderr)
            ),
        ));
    }
    let v: Value = serde_json::from_slice(&o.stdout).map_err(|e| {
        f(
            "restore",
            format!("report: {e}: {}", String::from_utf8_lossy(&o.stdout)),
        )
    })?;
    let _ = writeln!(
        out,
        "restore: {took:?} wall ({} ms in the report), meta index {}, blobs {}",
        v["durations"]["total_ms"], v["meta_index"], v["blobs"]
    );
    Ok(v)
}

/// The cut of each partition from the restore report.
fn cuts(report: &Value) -> Result<BTreeMap<u16, (u64, u64)>, ScenarioFailure> {
    let parts = report["cluster"]["partitions"]
        .as_array()
        .ok_or_else(|| f("restore", format!("no cluster report: {report}")))?;
    let mut m = BTreeMap::new();
    for p in parts {
        let id = p["p"].as_u64().unwrap_or(u64::MAX);
        m.insert(
            u16::try_from(id).map_err(|_| f("restore", "bad partition"))?,
            (
                p["cut"].as_u64().unwrap_or(0),
                p["head_end"].as_u64().unwrap_or(0),
            ),
        );
    }
    Ok(m)
}

/// `toml` without the `[cluster.tls]` `ca_cert_file` / `ca_key_file` entries (inline-table
/// `"ca_cert_file" = "..."` pairs as the harness writes them, or `ca_cert_file = "..."` lines).
fn without_ca_import(toml: &str) -> String {
    let mut out = String::with_capacity(toml.len());
    for line in toml.lines() {
        let t = line.trim_start();
        if t.starts_with("ca_cert_file") || t.starts_with("ca_key_file") {
            continue;
        }
        if t.starts_with("tls = {") && line.contains("\"ca_cert_file\"") {
            let inner = t
                .trim_start_matches("tls = {")
                .trim_end()
                .trim_end_matches('}');
            let kept: Vec<&str> = inner
                .split(", ")
                .map(str::trim)
                .filter(|kv| {
                    !kv.is_empty()
                        && !kv.starts_with("\"ca_cert_file\"")
                        && !kv.starts_with("\"ca_key_file\"")
                })
                .collect();
            out.push_str(&format!("tls = {{ {} }}\n", kept.join(", ")));
            continue;
        }
        out.push_str(line);
        out.push('\n');
    }
    out
}

async fn wipe(b: &Backend, url: &str) {
    let s = b.open(url);
    let k: Vec<String> = keys(&s).await.into_keys().collect();
    if !k.is_empty() {
        s.delete_many(&k).await.expect("wipe the new prefix");
    }
}

#[allow(clippy::too_many_lines)]
async fn scenario(b: &Backend, seed: u64, out: &mut String) -> Result<(), ScenarioFailure> {
    // ---- the original cluster
    let old_spec = spec3(seed);
    let partitions = old_spec.partitions;
    let mut old = ProcCluster::start_with(
        old_spec,
        ProcOptions {
            env: b.env(&b.old_url),
            ..ProcOptions::default()
        },
    )
    .await
    .map_err(|e| f("harness", format!("start the 3-node cluster: {e}")))?;
    let r = original(b, &mut old, out).await;
    let (acks, old_keys) = match r {
        Ok(x) => x,
        Err(e) => {
            for n in old.node_ids() {
                println!("--- old node {n} log tail ---\n{}", old.log_tail(n, 40));
            }
            old.stop().await;
            return Err(e);
        }
    };
    let starts = start_offsets(&old);
    old.stop().await;

    // ---- the seed cluster (node 1; first boot replaced by the restored data dir)
    let mut spec = ClusterSpec {
        seed,
        ..ClusterSpec::single()
    };
    spec.partitions = partitions;
    let mut c = ProcCluster::start_single_with(
        spec,
        ProcOptions {
            env: b.env(&b.new_url),
            ..ProcOptions::default()
        },
        None,
    )
    .await
    .map_err(|e| f("harness", format!("start the seed node: {e}")))?;
    let r = restored(b, &mut c, &acks, &starts, &old_keys, out).await;
    if r.is_err() {
        for n in c.node_ids() {
            println!("--- node {n} log tail ---\n{}", c.log_tail(n, 60));
        }
        if std::env::var_os("OSTRO_KEEP_FAILED").is_some() {
            let kept = c.stop_keeping_dirs();
            println!(
                "kept the failed cluster's directory: {}",
                kept.keep().display()
            );
            return r;
        }
    }
    c.stop().await;
    r
}

/// Run the original cluster under load, wait for the bucket, kill every node mid-load.
/// Returns the acknowledged runs and the old prefix's keys right after the kill.
async fn original(
    b: &Backend,
    old: &mut ProcCluster,
    out: &mut String,
) -> Result<(Vec<Ack>, BTreeMap<String, u64>), ScenarioFailure> {
    wait_healthy(old, &[], HEAL_T, "start").await?;
    deploy_guests(old, &WORKLOAD_GUESTS).await?;
    let load = Load::start(old, &[], false);
    load.wait_acked(30, RUN_T).await?;
    let store = b.open(&b.old_url);
    wait_bucket(&store, old.spec().partitions).await?;
    // More load after the first complete upload, so the heads trail the live logs.
    load.wait_acked(20, RUN_T).await?;
    let t_kill = Instant::now(); // clock-exempt: measurement
    for n in 1..=3 {
        old.kill9(n)
            .await
            .map_err(|e| f("harness", format!("kill -9 node {n}: {e}")))?;
    }
    let acks = load.snapshot();
    drop(load);
    let old_keys = keys(&store).await;
    let _ = writeln!(
        out,
        "original: 3 nodes RF 3, {} acknowledged runs, all killed (SIGKILL) mid-load in {:?}; bucket {} ({} objects)",
        acks.len(),
        t_kill.elapsed(),
        b.label,
        old_keys.len()
    );
    Ok((acks, old_keys))
}

async fn restored(
    b: &Backend,
    c: &mut ProcCluster,
    acks: &[Ack],
    starts: &BTreeMap<Vec<u8>, (u16, Offset)>,
    old_keys: &BTreeMap<String, u64>,
    out: &mut String,
) -> Result<(), ScenarioFailure> {
    // Replace the fresh node's data dir with the restored seed and its prefix with nothing.
    c.kill9(SEED)
        .await
        .map_err(|e| f("harness", format!("kill the fresh node: {e}")))?;
    let data_dir: PathBuf = c.config(SEED).node.data_dir.clone();
    std::fs::remove_dir_all(&data_dir).map_err(|e| f("harness", format!("wipe: {e}")))?;
    wipe(b, &b.new_url).await;
    let report = run_restore(b, &data_dir, out)?;
    if report["cluster"]["ca_kept"] == true {
        // The restored data dir holds the cluster's CA (from metadata): the operator starts
        // the seed without a different CA import (docs/cluster-restore.md), otherwise the
        // seed's own certificate and the ones it issues to joiners would not share a CA.
        let path = c.config_path(SEED).to_path_buf();
        let toml = std::fs::read_to_string(&path).map_err(|e| f("harness", e.to_string()))?;
        let edited = without_ca_import(&toml);
        if edited == toml {
            return Err(f(
                "harness",
                "no [cluster.tls] CA import in the seed config",
            ));
        }
        std::fs::write(&path, edited).map_err(|e| f("harness", e.to_string()))?;
        let _ = writeln!(
            out,
            "seed config: the CA import removed (the kept CA is used)"
        );
    }
    let cut = cuts(&report)?;
    let rpo: u64 = cut.values().map(|(c, e)| e - c).sum();
    let _ = writeln!(
        out,
        "cut per partition (cut / head end): {:?}; dropped offsets {rpo}; removed members {}; ca kept {}",
        cut, report["cluster"]["removed_members"], report["cluster"]["ca_kept"]
    );
    let removed: Vec<u64> = report["cluster"]["removed_members"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_u64)
        .collect();
    if removed != [2, 3] {
        return Err(f(
            "restore",
            format!("expected members 2 and 3 removed, got {removed:?}"),
        ));
    }

    // Seed alone, then init (idempotent) and two joins.
    let t_start = Instant::now(); // clock-exempt: measurement
    c.restart(SEED)
        .await
        .map_err(|e| f("restore", format!("the restored seed did not start: {e}")))?;
    wait_meta_leader(c).await?;
    let _ = writeln!(out, "seed ready after {:?}", t_start.elapsed());
    let st = c
        .cluster_init(SEED)
        .await
        .map_err(|e| f("restore", format!("cluster init on the seed: {e}")))?;
    let _ = writeln!(out, "init: policy {}", st["policy"]);
    let t_join = Instant::now(); // clock-exempt: measurement
    let mut joined = Vec::new();
    for _ in 0..2 {
        let t = Instant::now(); // clock-exempt: measurement
        let id = c
            .add_node(NodeOpts {
                level_max: None,
                advertise: true,
            })
            .await
            .map_err(|e| f("restore", format!("cluster join: {e}")))?;
        let _ = writeln!(out, "join: node {id} ready after {:?}", t.elapsed());
        joined.push(id);
    }
    let rf = match wait_rf3(c).await {
        Ok(d) => format!("RF 3 / full ISR {d:?} after the second join"),
        Err(last) => {
            return Err(f(
                "restore",
                format!("RF 3 not reached within {RF_T:?} after the joins: {last}"),
            ));
        }
    };
    let _ = writeln!(
        out,
        "{rf}; time-to-RF-3 from the first join {:?}",
        t_join.elapsed()
    );

    // (f) tombstones and fresh ids.
    let st = status(c, SEED)
        .await
        .ok_or_else(|| f("availability", "no cluster status"))?;
    let state_of = |id: u64| {
        st["members"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|m| m["node_id"].as_u64() == Some(id))
            .and_then(|m| m["state"].as_str().map(str::to_owned))
    };
    for id in [2, 3] {
        if state_of(id).as_deref() != Some("removed") {
            return Err(f(
                "restore",
                format!("old member {id} is not a Removed tombstone: {st}"),
            ));
        }
    }
    if joined.iter().any(|j| [2, 3].contains(j)) {
        return Err(f("restore", format!("a removed id was reused: {joined:?}")));
    }

    // (a)/(b)/(d): acknowledged runs vs the cut.
    let mut below = Vec::new();
    let mut above = 0usize;
    let mut unknown = 0usize;
    let mut present_above = Vec::new();
    for a in acks {
        let id = a.id();
        let expect = starts
            .get(id.as_bytes().as_slice())
            .map(|(p, off)| off.0 < cut.get(p).map_or(0, |(c, _)| *c));
        let doc = run_doc(c, &a.run_id).await?;
        match (expect, doc) {
            (Some(true), Some(v)) => below.push((a.clone(), v)),
            (Some(true), None) => {
                return Err(f(
                    "restore",
                    format!(
                        "run {} (p{}) is below the cut but missing",
                        a.run_id, a.partition
                    ),
                ));
            }
            (Some(false), None) => above += 1,
            (Some(false), Some(_)) => present_above.push(a.run_id.clone()),
            (None, Some(v)) => {
                unknown += 1;
                below.push((a.clone(), v));
            }
            (None, None) => {
                unknown += 1;
                above += 1;
            }
        }
    }
    if !present_above.is_empty() {
        return Err(f(
            "restore",
            format!("runs above the cut are present: {present_above:?}"),
        ));
    }
    let mut runs: Vec<Started> = Vec::new();
    let mut open = 0usize;
    for (a, v) in &below {
        let wf = v["workflow"].as_str().unwrap_or_default();
        let kind = kind_of(wf).ok_or_else(|| f("restore", format!("unknown workflow {wf}")))?;
        let terminal = ostro_repl_conformance::harness::inproc::TERMINAL
            .contains(&v["status"].as_str().unwrap_or_default());
        if !terminal {
            open += 1;
            if kind == Kind::Signal {
                for n in live(c) {
                    let path = format!("/v1/runs/{}/signals/x", a.run_id);
                    if post(c, n, &path, &json!({ "payload": "go" }))
                        .await
                        .is_some_and(|(s, _)| (200..300).contains(&s) || s == 409)
                    {
                        break;
                    }
                }
            }
        }
        runs.push(Started {
            ack: a.clone(),
            kind,
            key: String::new(),
        });
    }
    let _ = writeln!(
        out,
        "acknowledged runs: {} below the cut present ({open} non-terminal at the cut), {above} above the cut absent (RPO), {unknown} without a StartRun in the old logs",
        below.len()
    );
    let oc = outcomes(c, &runs).await;
    let _ = writeln!(out, "restored runs: {}", oc.line());
    oc.verdict()?;

    // (e) new runs on the grown cluster.
    let load = Load::start(c, &[], false);
    load.wait_acked(10, RUN_T).await?;
    let res = load.finish(RUN_T).await?;
    let _ = writeln!(out, "new {}", res.line());
    res.verdict()?;
    let oc = outcomes(c, &res.runs).await;
    let _ = writeln!(out, "new runs: {}", oc.line());
    oc.verdict()?;

    // (c) Phase 12 final checks over the seed and the joined nodes.
    let mut nodes = vec![SEED];
    nodes.extend(&joined);
    let mut checked: Vec<Ack> = runs.iter().map(|r| r.ack.clone()).collect();
    checked.extend(res.acks());
    let fc = final_checks(c, &nodes, &checked).await?;
    let _ = writeln!(out, "final checks: {fc}");

    // (g) new prefix only.
    let now_old = keys(&b.open(&b.old_url)).await;
    if &now_old != old_keys {
        return Err(f(
            "bottomless",
            "the restored cluster wrote to the old prefix".to_owned(),
        ));
    }
    let new_keys = keys(&b.open(&b.new_url)).await;
    let heads = (0..c.spec().partitions)
        .filter(|p| new_keys.contains_key(&format!("p/{p:04}/head")))
        .count();
    let _ = writeln!(
        out,
        "uploads: old prefix unchanged ({} objects), new prefix {} objects with {heads} partition heads",
        now_old.len(),
        new_keys.len()
    );
    if heads == 0 {
        return Err(f("bottomless", "the restored cluster uploaded nothing"));
    }
    Ok(())
}

async fn run(b: Backend, seed: u64, name: &str) {
    if let Some(skip) = skip_reason() {
        println!("{skip}");
        return;
    }
    let t0 = Instant::now(); // clock-exempt: measurement
    let mut out = String::new();
    let _ = writeln!(
        out,
        "{TEST}::{name} (14-30, tier C): 3 nodes + load -> SIGKILL all -> ostro restore (seed) -> init -> 2 joins -> RF 3; backend {}\n{}",
        b.label,
        host_line()
    );
    let r = scenario(&b, seed, &mut out).await;
    let _ = writeln!(
        out,
        "result: {}; elapsed {:?}",
        match &r {
            Ok(()) => "PASS".to_owned(),
            Err(e) => format!("FAIL {e}"),
        },
        t0.elapsed()
    );
    println!("{out}");
    write_report(&format!("{TEST}/{name}"), &out);
    r.expect(name);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "tier C: needs the failpoint binary (cargo build -p ostro --features failpoints --target-dir target/failpoints)"]
async fn file_bucket() {
    run(Backend::file(), 3001, "file_bucket").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "tier C: needs the failpoint binary and SeaweedFS (OSTRO_TEST_S3_URL, scripts/test-services.sh)"]
async fn s3_bucket() {
    let Some(b) = Backend::s3() else {
        return;
    };
    run(b, 3002, "s3_bucket").await;
}
