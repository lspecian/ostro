//! `ostro replay` (CLI-07, D-P10-7): replay recorded runs against a (new) module and
//! report divergence (exit 4).
//!
//! Selectors (exactly one): run ids (positional `RUN...` merged with `--run`),
//! `--sample N [--workflow W] [--status S]`, or `--corpus DIR` (offline, 05-12 entries).
//! `--module` is required unless `--corpus` is given.
//!
//! - **Remote** (run ids / `--sample`): the module is uploaded idempotently with
//!   `PUT /v1/modules/{sha}` (named after the file stem, as `ostro deploy` names it, and
//!   never activated), then `POST /v1/modules/{sha}/replay` replays the selected journals
//!   **on the server** with the cluster token key; no journal or key reaches the CLI.
//! - **Offline** (`--corpus DIR`): every entry of a 05-12 replay corpus is loaded with
//!   `ostro_engine::replay_corpus` and replayed locally with
//!   `ostro_engine::replay_check` on a runtime built with the entry's own token key,
//!   against the entry's `module.wasm` or `--module`. No server is contacted.
//!
//! Output: a table (run, workflow, verdict, seq, detail) and a summary line, or with
//! `--json` the `ostro.replay.result` envelope (`module_sha256`, `verdicts`, `summary`;
//! offline also `corpus`).
//!
//! Exit codes ([`outcome_of`]): 4 ([`Outcome::Diverged`]) when any run diverged, or was
//! skipped (its workflow is not exported by the module) without `--allow-missing`;
//! otherwise 1 when any verdict is `error` (or the call failed); 0 when every verdict is
//! `ok` / `ok_frontier` (or an allowed `skipped`); 2 on usage errors. A divergence is a
//! finding about the module, so it wins over errors of other runs.
//!
//! [`run`] is called outside any async runtime, so it uses [`super::block_on`].

use std::collections::hash_map::Entry;
use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::{ArgGroup, Args};
use ostro_engine::config::EngineConfig;
use ostro_engine::replay_check::{ReplayVerdict, replay_check};
use ostro_engine::replay_corpus::{CorpusEntry, load_corpus};
use ostro_engine::runtime::{Prepared, Runtime, RuntimeOptions};
use serde_json::{Value, json};

use super::deploy::sha256_hex;
use super::{Conn, StatusArg, block_on, report};
use crate::client::CallError;
use crate::output::{Out, Outcome, local_json, s, table};

/// `ostro replay [RUN...] [--run ID]... | --sample N [--workflow W] [--status S] |
/// --corpus DIR, [--module PATH] [--allow-missing]` plus the connection flags.
#[derive(Debug, Clone, Args)]
#[command(group(
    ArgGroup::new("selector")
        .args(["runs", "run", "sample", "corpus"])
        .required(true)
        .multiple(true)
))]
pub struct ReplayArgs {
    /// Run ids to replay (merged with `--run`).
    #[arg(value_name = "RUN", conflicts_with_all = ["sample", "corpus"])]
    pub runs: Vec<String>,
    /// A run id to replay (repeatable; merged with the positional ids).
    #[arg(long, value_name = "ID", conflicts_with_all = ["sample", "corpus"])]
    pub run: Vec<String>,
    /// Replay the newest N runs of each workflow the module describes.
    #[arg(long, value_name = "N", conflicts_with = "corpus")]
    pub sample: Option<u32>,
    /// With --sample: only this workflow.
    // `requires` alone is waived by clap when a conflicting selector is present, so the
    // other selectors are listed as conflicts too.
    #[arg(long, requires = "sample", conflicts_with_all = ["runs", "run", "corpus"])]
    pub workflow: Option<String>,
    /// With --sample: only runs in this status.
    #[arg(
        long,
        value_enum,
        requires = "sample",
        conflicts_with_all = ["runs", "run", "corpus"]
    )]
    pub status: Option<StatusArg>,
    /// Replay a 05-12 replay corpus directory offline (no server).
    #[arg(long, value_name = "DIR")]
    pub corpus: Option<PathBuf>,
    /// The Wasm component to replay with (required unless --corpus).
    #[arg(long, value_name = "PATH", required_unless_present = "corpus")]
    pub module: Option<PathBuf>,
    /// Do not count runs of workflows the module does not export as failures.
    #[arg(long)]
    pub allow_missing: bool,
    #[command(flatten)]
    pub conn: Conn,
}

impl ReplayArgs {
    /// The explicit run ids: positional ones first, then `--run`.
    pub fn run_ids(&self) -> Vec<String> {
        self.runs.iter().chain(&self.run).cloned().collect()
    }

    /// The `POST /v1/modules/{sha}/replay` body of a remote replay.
    pub fn request_body(&self) -> Value {
        let mut b = match self.sample {
            Some(n) => {
                let mut b = json!({ "sample": n });
                if let Some(w) = &self.workflow {
                    b["workflow"] = json!(w);
                }
                if let Some(st) = self.status {
                    b["status"] = json!(st.as_str());
                }
                b
            }
            None => json!({ "runs": self.run_ids() }),
        };
        b["allow_missing"] = json!(self.allow_missing);
        b
    }
}

/// Verdict counts (the server's `summary` shape).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Counts {
    pub ok: u64,
    pub ok_frontier: u64,
    pub diverged: u64,
    pub skipped: u64,
    pub error: u64,
}

impl Counts {
    /// Read a `summary` object (missing fields count 0).
    pub fn from_summary(v: &Value) -> Self {
        let n = |k: &str| v[k].as_u64().unwrap_or(0);
        Counts {
            ok: n("ok"),
            ok_frontier: n("ok_frontier"),
            diverged: n("diverged"),
            skipped: n("skipped"),
            error: n("error"),
        }
    }

    /// Count the `verdict` fields of `verdicts`.
    pub fn of(verdicts: &[Value]) -> Self {
        let mut c = Counts::default();
        for v in verdicts {
            match v["verdict"].as_str() {
                Some("ok") => c.ok += 1,
                Some("ok_frontier") => c.ok_frontier += 1,
                Some("diverged") => c.diverged += 1,
                Some("skipped") => c.skipped += 1,
                _ => c.error += 1,
            }
        }
        c
    }

    fn total(&self) -> u64 {
        self.ok + self.ok_frontier + self.diverged + self.skipped + self.error
    }

    fn to_json(self) -> Value {
        json!({
            "ok": self.ok,
            "ok_frontier": self.ok_frontier,
            "diverged": self.diverged,
            "skipped": self.skipped,
            "error": self.error,
        })
    }
}

/// The exit outcome of a replay (see the module docs).
pub fn outcome_of(c: &Counts, allow_missing: bool) -> Outcome {
    if c.diverged > 0 || (c.skipped > 0 && !allow_missing) {
        Outcome::Diverged
    } else if c.error > 0 {
        Outcome::Error
    } else {
        Outcome::Ok
    }
}

/// Print the verdicts (table + summary line, or the JSON envelope) and map the exit code.
fn finish(out: Out, mut body: Value, allow_missing: bool) -> Outcome {
    let verdicts: Vec<Value> = body["verdicts"].as_array().cloned().unwrap_or_default();
    let counts = match body.get("summary") {
        Some(sum) if sum.is_object() => Counts::from_summary(sum),
        _ => Counts::of(&verdicts),
    };
    body["summary"] = counts.to_json();
    if out.json {
        println!("{}", local_json("replay", &body));
    } else {
        let rows: Vec<Vec<String>> = verdicts
            .iter()
            .map(|v| {
                // offline rows are named by their corpus entry
                let run = v.get("entry").unwrap_or(&v["run_id"]);
                vec![
                    s(run),
                    s(&v["workflow"]),
                    s(&v["verdict"]),
                    s(&v["seq"]),
                    s(&v["detail"]),
                ]
            })
            .collect();
        if !rows.is_empty() {
            println!(
                "{}",
                table(&["RUN", "WORKFLOW", "VERDICT", "SEQ", "DETAIL"], &rows)
            );
        }
        println!(
            "replay: {} run(s): {} ok, {} ok_frontier, {} diverged, {} skipped, {} error",
            counts.total(),
            counts.ok,
            counts.ok_frontier,
            counts.diverged,
            counts.skipped,
            counts.error
        );
    }
    let o = outcome_of(&counts, allow_missing);
    match o {
        Outcome::Diverged if counts.diverged > 0 => {
            eprintln!("ostro: replay: {} run(s) diverged", counts.diverged);
        }
        Outcome::Diverged => eprintln!(
            "ostro: replay: {} run(s) skipped (workflow not exported by the module; \
             --allow-missing accepts them)",
            counts.skipped
        ),
        Outcome::Error => eprintln!(
            "ostro: replay: {} run(s) could not be checked",
            counts.error
        ),
        _ => {}
    }
    o
}

fn read_module(p: &Path) -> Result<Vec<u8>, CallError> {
    std::fs::read(p).map_err(|e| CallError::Usage(format!("cannot read {}: {e}", p.display())))
}

/// Run `ostro replay`.
pub fn run(a: &ReplayArgs) -> Outcome {
    match &a.corpus {
        Some(dir) => block_on(corpus(a, dir)),
        None => block_on(remote(a)),
    }
}

/// Remote mode: upload (no activation) and replay on the server.
async fn remote(a: &ReplayArgs) -> Outcome {
    let out = a.conn.out();
    let Some(path) = &a.module else {
        eprintln!("ostro: error: --module is required unless --corpus is given");
        return Outcome::Usage;
    };
    let res = async {
        let wasm = read_module(path)?;
        let sha = sha256_hex(&wasm);
        let name = path
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .filter(|n| !n.is_empty());
        let c = a.conn.client()?;
        c.upload_module(&sha, wasm, name.as_deref(), None).await?;
        c.replay_module(&sha, &a.request_body()).await
    }
    .await;
    match res {
        Ok(r) => {
            let body = json!({
                "module_sha256": r.body["module_sha256"],
                "verdicts": r.body["verdicts"],
                "summary": r.body["summary"],
            });
            finish(out, body, a.allow_missing)
        }
        Err(e) => report(out, &e),
    }
}

/// One verdict row of an offline replay.
fn corpus_row(e: &CorpusEntry, v: ReplayVerdict) -> Value {
    let (verdict, seq, detail, appended) = match v {
        ReplayVerdict::Ok { appended } => ("ok", None, None, appended),
        ReplayVerdict::OkFrontier { appended } => ("ok_frontier", None, None, appended),
        ReplayVerdict::Diverged { seq, detail } => ("diverged", Some(seq), Some(detail), 0),
        ReplayVerdict::Error(m) => ("error", None, Some(m), 0),
    };
    json!({
        "entry": e.name,
        "run_id": e.run.run_id,
        "workflow": e.workflow,
        "verdict": verdict,
        "seq": seq,
        "detail": detail,
        "appended": appended,
    })
}

/// A local runtime with `token_key` (the system clock: a faithful replay stamps nothing
/// live).
fn runtime_for(token_key: [u8; 32]) -> Result<Runtime, String> {
    Runtime::new(
        EngineConfig::default(),
        RuntimeOptions {
            token_key,
            ..RuntimeOptions::default()
        },
    )
    .map_err(|e| format!("engine: {e}"))
}

/// A prepared module and the workflow names it describes.
struct Module {
    prepared: Arc<Prepared>,
    exported: BTreeSet<String>,
}

async fn prepare(rt: &Runtime, wasm: &[u8]) -> Result<Module, String> {
    let prepared = Arc::new(rt.prepare(wasm).map_err(|e| format!("prepare: {e}"))?);
    let exported = rt
        .describe(&prepared)
        .await
        .map_err(|e| format!("describe: {e}"))?
        .into_iter()
        .map(|m| m.name)
        .collect();
    Ok(Module { prepared, exported })
}

/// Offline mode: replay every corpus entry locally.
async fn corpus(a: &ReplayArgs, dir: &Path) -> Outcome {
    let out = a.conn.out();
    let override_wasm = match a.module.as_deref().map(read_module).transpose() {
        Ok(w) => w,
        Err(e) => return out.error(&e),
    };
    let entries = match load_corpus(dir) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("ostro: error: corpus {}: {e}", dir.display());
            return Outcome::Error;
        }
    };
    // One runtime per distinct token key; one prepared module per (key, wasm).
    let mut runtimes: HashMap<[u8; 32], Runtime> = HashMap::new();
    let mut modules: HashMap<([u8; 32], String), Module> = HashMap::new();
    let mut verdicts = Vec::with_capacity(entries.len());
    for e in &entries {
        let wasm = override_wasm.as_deref().unwrap_or(&e.module);
        let key = (e.token_key, sha256_hex(wasm));
        let rt = match runtimes.entry(e.token_key) {
            Entry::Occupied(o) => o.into_mut(),
            Entry::Vacant(v) => match runtime_for(e.token_key) {
                Ok(rt) => v.insert(rt),
                Err(m) => {
                    verdicts.push(corpus_row(e, ReplayVerdict::Error(m)));
                    continue;
                }
            },
        };
        let m = match modules.entry(key) {
            Entry::Occupied(o) => o.into_mut(),
            Entry::Vacant(v) => match prepare(rt, wasm).await {
                Ok(m) => v.insert(m),
                Err(m) => {
                    verdicts.push(corpus_row(e, ReplayVerdict::Error(m)));
                    continue;
                }
            },
        };
        if !m.exported.contains(&e.workflow) {
            let mut row = corpus_row(e, ReplayVerdict::Error(String::new()));
            row["verdict"] = json!("skipped");
            row["detail"] = json!("workflow not exported by module");
            verdicts.push(row);
            continue;
        }
        let v = replay_check(rt, &m.prepared, e.run.clone(), &e.journal).await;
        verdicts.push(corpus_row(e, v));
    }
    let body = json!({
        "corpus": dir.display().to_string(),
        "module_sha256": override_wasm.as_deref().map(sha256_hex),
        "verdicts": verdicts,
    });
    finish(out, body, a.allow_missing)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::tests::parse;
    use crate::commands::{ClientCommand, run as run_cmd};

    fn args(v: &[&str]) -> ReplayArgs {
        let mut all = vec!["replay"];
        all.extend_from_slice(v);
        match parse(&all) {
            Ok(ClientCommand::Replay(a)) => a,
            other => panic!("{all:?}: {other:?}"),
        }
    }

    fn counts(ok: u64, okf: u64, div: u64, skip: u64, err: u64) -> Counts {
        Counts {
            ok,
            ok_frontier: okf,
            diverged: div,
            skipped: skip,
            error: err,
        }
    }

    #[test]
    fn replay_exit_code_mapping() {
        use Outcome::*;
        assert_eq!(outcome_of(&counts(3, 1, 0, 0, 0), false), Ok);
        assert_eq!(outcome_of(&counts(0, 0, 0, 0, 0), false), Ok);
        assert_eq!(outcome_of(&counts(3, 0, 1, 0, 0), false), Diverged);
        assert_eq!(outcome_of(&counts(3, 0, 0, 1, 0), false), Diverged);
        assert_eq!(outcome_of(&counts(3, 0, 0, 1, 0), true), Ok);
        assert_eq!(outcome_of(&counts(3, 0, 0, 0, 1), false), Error);
        assert_eq!(outcome_of(&counts(3, 0, 0, 1, 1), true), Error);
        // a divergence wins over errors of other runs
        assert_eq!(outcome_of(&counts(0, 0, 1, 0, 1), true), Diverged);
        assert_eq!(crate::output::exit_code(Diverged), 4);
    }

    #[test]
    fn replay_counts_from_verdicts_and_summary() {
        let vs = vec![
            json!({"verdict": "ok"}),
            json!({"verdict": "ok_frontier"}),
            json!({"verdict": "diverged", "seq": 2}),
            json!({"verdict": "skipped"}),
            json!({"verdict": "error"}),
            json!({"verdict": "bogus"}),
        ];
        assert_eq!(Counts::of(&vs), counts(1, 1, 1, 1, 2));
        let sum = json!({"ok": 4, "ok_frontier": 0, "diverged": 2, "skipped": 1, "error": 0});
        assert_eq!(Counts::from_summary(&sum), counts(4, 0, 2, 1, 0));
        assert_eq!(Counts::from_summary(&sum).to_json(), sum);
    }

    #[test]
    fn replay_request_bodies() {
        let a = args(&["r1", "--run", "r2", "--module", "m.wasm"]);
        assert_eq!(
            a.request_body(),
            json!({"runs": ["r1", "r2"], "allow_missing": false})
        );
        let a = args(&[
            "--sample",
            "5",
            "--workflow",
            "w",
            "--status",
            "timed_out",
            "--module",
            "m.wasm",
            "--allow-missing",
        ]);
        assert_eq!(
            a.request_body(),
            json!({"sample": 5, "workflow": "w", "status": "timed_out", "allow_missing": true})
        );
        let a = args(&["--sample", "1", "--module", "m.wasm"]);
        assert_eq!(
            a.request_body(),
            json!({"sample": 1, "allow_missing": false})
        );
    }

    #[test]
    fn replay_unreadable_inputs() {
        // An unreadable module is a usage error (as `ostro deploy`), before any request.
        let cmd = parse(&[
            "replay",
            "r1",
            "--module",
            "/nonexistent/m.wasm",
            "--server",
            "http://127.0.0.1:9",
        ])
        .unwrap();
        assert_eq!(run_cmd(cmd), Outcome::Usage);
        let cmd = parse(&[
            "replay",
            "--corpus",
            "/nonexistent/corpus",
            "--module",
            "/nonexistent/m.wasm",
        ])
        .unwrap();
        assert_eq!(run_cmd(cmd), Outcome::Usage);
        // A missing corpus directory is an error (exit 1), not a usage error.
        let cmd = parse(&["replay", "--corpus", "/nonexistent/corpus"]).unwrap();
        assert_eq!(run_cmd(cmd), Outcome::Error);
    }

    #[test]
    fn replay_empty_corpus_is_ok() {
        let d = tempfile::tempdir().unwrap();
        let cmd = parse(&["replay", "--corpus", d.path().to_str().unwrap()]).unwrap();
        assert_eq!(run_cmd(cmd), Outcome::Ok);
    }
}
