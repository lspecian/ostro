//! Run reports (14-09): `target/jepsen/<seed>/{history.jsonl, nemesis.jsonl, schedule.json,
//! report.json, report.txt}`.
//!
//! The root is the **workspace root's** `target/jepsen` (`CARGO_MANIFEST_DIR/../..`,
//! canonicalised), never the test's working directory; `OSTRO_JEPSEN_OUT` overrides it.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::checker::Verdict;
use crate::history::{History, write_jsonl};

/// Environment override of the report root.
pub const OUT_ENV: &str = "OSTRO_JEPSEN_OUT";

/// The workspace root (canonicalised when possible).
pub fn workspace_root() -> PathBuf {
    let p = Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../.."));
    p.canonicalize().unwrap_or_else(|_| p.to_path_buf())
}

/// `OSTRO_JEPSEN_OUT`, else `<workspace>/target/jepsen`.
pub fn out_root() -> PathBuf {
    match std::env::var_os(OUT_ENV) {
        Some(d) if !d.is_empty() => PathBuf::from(d),
        _ => workspace_root().join("target").join("jepsen"),
    }
}

/// The report directory of `seed` under [`out_root`].
pub fn seed_dir(seed: u64) -> PathBuf {
    out_root().join(seed.to_string())
}

/// What a run did and what the checkers found (`report.json`).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RunReport {
    /// The seed (schedule, workload inputs).
    pub seed: u64,
    /// Fault phase length (s).
    pub duration_secs: u64,
    /// Cluster processes.
    pub nodes: usize,
    /// Partitions.
    pub partitions: u16,
    /// The `ostro` binary under test.
    pub binary: String,
    /// Client ops by `kind/outcome`.
    pub ops: BTreeMap<String, u64>,
    /// Faults executed by kind.
    pub faults: BTreeMap<String, u64>,
    /// Phase timings (ms): warm-up, faults, heal, quiescence, export, checks.
    pub timings_ms: BTreeMap<String, u64>,
    /// One verdict per checker (plus `harness` when the run itself failed).
    pub verdicts: Vec<Verdict>,
    /// Every verdict passed.
    pub passed: bool,
    /// Free-form remarks.
    pub notes: Vec<String>,
}

impl RunReport {
    /// The verdict of `checker`.
    pub fn verdict(&self, checker: &str) -> Option<&Verdict> {
        self.verdicts.iter().find(|v| v.checker == checker)
    }

    /// The human-readable report (`report.txt`).
    pub fn text(&self) -> String {
        let mut s = String::new();
        let _ = writeln!(
            s,
            "ostro-jepsen run: seed {} | {} s of faults | {} nodes, {} partitions | {}",
            self.seed,
            self.duration_secs,
            self.nodes,
            self.partitions,
            if self.passed { "PASS" } else { "FAIL" }
        );
        let _ = writeln!(s, "binary: {}", self.binary);
        let _ = writeln!(s, "ops: {:?}", self.ops);
        let _ = writeln!(s, "faults: {:?}", self.faults);
        let _ = writeln!(s, "timings (ms): {:?}", self.timings_ms);
        for v in &self.verdicts {
            let _ = writeln!(s, "{}", v.line());
            for x in v.violations.iter().take(50) {
                let _ = writeln!(s, "  VIOLATION {x}");
            }
            if v.violations.len() > 50 {
                let _ = writeln!(s, "  ... {} more", v.violations.len() - 50);
            }
            for n in v.notes.iter().take(20) {
                let _ = writeln!(s, "  note: {n}");
            }
        }
        for n in &self.notes {
            let _ = writeln!(s, "note: {n}");
        }
        s
    }
}

/// Write `report.json` and `report.txt` into `dir` (and `history.jsonl` / `nemesis.jsonl`
/// from `history` when it was not streamed there already).
pub fn write(dir: &Path, report: &RunReport, history: Option<&History>) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    if let Some(h) = history {
        write_jsonl(&dir.join("history.jsonl"), &h.ops())?;
        write_jsonl(&dir.join("nemesis.jsonl"), &h.nemesis())?;
    }
    let json = serde_json::to_vec_pretty(report).map_err(std::io::Error::other)?;
    std::fs::write(dir.join("report.json"), json)?;
    std::fs::write(dir.join("report.txt"), report.text())
}
