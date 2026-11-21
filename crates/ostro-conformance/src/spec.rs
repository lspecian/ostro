//! Language-neutral conformance scenarios (see `conformance/spec/SCHEMA.md`).
//!
//! A scenario lives in `conformance/spec/<leg>/<ID>.json` and is listed in
//! `conformance/spec/catalogue.json`. The catalogue is written once (plan 09-02) and never
//! edited afterwards: an entry is *pending* exactly when its spec file does not exist yet,
//! so owner plans only add spec files.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::OnceLock;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Which harness runs a scenario.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Leg {
    /// `ostro-engine` testkit, scripted inputs (09-11).
    Engine,
    /// In-process `ostro-server` (children, API signals/tokens, effects; 09-13).
    Server,
    /// Spawned failpoint-enabled `ostro` binary with kill -9 (09-18).
    Crash,
}

impl Leg {
    /// Directory name under `conformance/spec/`.
    pub fn dir(self) -> &'static str {
        match self {
            Leg::Engine => "engine",
            Leg::Server => "server",
            Leg::Crash => "crash",
        }
    }
}

/// Guest language / build mode a scenario applies to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Lang {
    #[serde(rename = "rust")]
    Rust,
    #[serde(rename = "py")]
    Py,
    /// TypeScript on QuickJS (default backend, D-P9-1).
    #[serde(rename = "ts")]
    Ts,
    /// TypeScript on StarlingMonkey (fallback backend, smoke subset only).
    #[serde(rename = "ts-sm")]
    TsSm,
}

/// Catalogue entry kind (W6): leg runners iterate `scenario` only; the `rebuild` entry is
/// run by 09-17's `rebuild.rs`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    Scenario,
    Rebuild,
}

/// One scenario file.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Spec {
    pub id: String,
    pub title: String,
    pub leg: Leg,
    /// Base workflow name (`conf_steps`); the harness maps it per language
    /// (`rs_`/`py_`/`ts_` prefix, D-P9-23).
    pub workflow: String,
    #[serde(default)]
    pub input: Value,
    /// Run seed (decimal u64 as a string); harness default when absent.
    #[serde(default)]
    pub seed: Option<String>,
    /// Fixed run id (UUID string); harness default when absent.
    #[serde(default)]
    pub run_id: Option<String>,
    /// Module variant to start on (`v2`); default is the base module.
    #[serde(default)]
    pub variant: Option<String>,
    #[serde(default)]
    pub script: Vec<ScriptStep>,
    pub expect: Expect,
    /// Restricts the languages (default: the catalogue entry's languages).
    #[serde(default)]
    pub languages: Option<Vec<Lang>>,
}

/// A scripted input, delivered in order when the run is waiting (D-P9-22).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "input", rename_all = "snake_case")]
pub enum ScriptStep {
    /// Deliver a signal. `before_start` buffers it before the first activation.
    Signal {
        name: String,
        #[serde(default)]
        payload: Value,
        #[serde(default)]
        before_start: bool,
    },
    /// Complete the first open await token with `payload`.
    Token {
        #[serde(default)]
        payload: Value,
    },
    /// Request graceful cancellation of the run.
    Cancel {},
    /// Fire every pending timer at its due time.
    FireTimers {},
    /// Re-activate on a different module variant (patching / non-determinism scenarios).
    SwapModule { variant: String },
    /// Server leg: one control-plane API operation (`op` plus operation-specific fields).
    Api {
        op: String,
        #[serde(flatten)]
        args: Map<String, Value>,
    },
    /// Crash leg: kill -9 the node at a named point (failpoint or phase).
    Kill { at: String },
}

/// Expected outcome.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Expect {
    /// Terminal status: `completed`, `failed`, `cancelled`, `terminated`, `continued_as_new`.
    pub status: String,
    /// Canonical-JSON result (compared per D-P9-21).
    #[serde(default)]
    pub result: Option<Value>,
    /// Failure kind for a failed run.
    #[serde(default)]
    pub failure_kind: Option<String>,
    /// Normalised journal projection (SCHEMA.md vocabulary).
    #[serde(default)]
    pub projection: Option<Vec<String>>,
    /// Scenario-specific checks (e.g. `differs_across_seeds`).
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// One catalogue line.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogueEntry {
    pub id: String,
    pub leg: Leg,
    pub kind: Kind,
    pub title: String,
    /// The plan that writes the spec file (`09-NN`).
    pub owner: String,
    pub languages: Vec<Lang>,
}

impl CatalogueEntry {
    /// Path of the spec file (`conformance/spec/<leg>/<ID>.json`).
    pub fn spec_path(&self) -> PathBuf {
        spec_path(self.leg, &self.id)
    }

    /// True while the owner plan has not written the spec file.
    pub fn is_pending(&self) -> bool {
        !self.spec_path().exists()
    }
}

/// `conformance/spec/catalogue.json`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Catalogue {
    pub entries: Vec<CatalogueEntry>,
}

impl Catalogue {
    pub fn get(&self, id: &str) -> Option<&CatalogueEntry> {
        self.entries.iter().find(|e| e.id == id)
    }
}

/// Spec errors.
#[derive(Debug, thiserror::Error)]
pub enum SpecError {
    #[error("read {path}: {source}")]
    Io {
        path: String,
        source: std::io::Error,
    },
    #[error("parse {path}: {source}")]
    Parse {
        path: String,
        source: serde_json::Error,
    },
    #[error("unknown scenario id {0}")]
    UnknownId(String),
    #[error("{path}: {msg}")]
    Mismatch { path: String, msg: String },
}

/// `<repo>/conformance/spec`.
pub fn spec_root() -> PathBuf {
    crate::repo_root().join("conformance/spec")
}

/// `<repo>/conformance/spec/<leg>/<ID>.json`.
pub fn spec_path(leg: Leg, id: &str) -> PathBuf {
    spec_root().join(leg.dir()).join(format!("{id}.json"))
}

fn read_json<T: serde::de::DeserializeOwned>(path: &std::path::Path) -> Result<T, SpecError> {
    let p = path.display().to_string();
    let bytes = std::fs::read(path).map_err(|source| SpecError::Io {
        path: p.clone(),
        source,
    })?;
    serde_json::from_slice(&bytes).map_err(|source| SpecError::Parse { path: p, source })
}

/// Reads and parses the catalogue.
pub fn load_catalogue() -> Result<Catalogue, SpecError> {
    read_json(&spec_root().join("catalogue.json"))
}

/// The catalogue, loaded once per process (panics if it does not parse).
pub fn catalogue() -> &'static Catalogue {
    static CAT: OnceLock<Catalogue> = OnceLock::new();
    CAT.get_or_init(|| load_catalogue().unwrap_or_else(|e| panic!("catalogue: {e}")))
}

/// Loads `conformance/spec/<leg>/<ID>.json` for a catalogue id and checks that the file's
/// `id` and `leg` match the catalogue.
pub fn load_spec(id: &str) -> Result<Spec, SpecError> {
    let entry = catalogue()
        .get(id)
        .ok_or_else(|| SpecError::UnknownId(id.to_owned()))?;
    let path = entry.spec_path();
    let spec: Spec = read_json(&path)?;
    if spec.id != entry.id || spec.leg != entry.leg {
        return Err(SpecError::Mismatch {
            path: path.display().to_string(),
            msg: format!(
                "file has id {} leg {:?}, catalogue has id {} leg {:?}",
                spec.id, spec.leg, entry.id, entry.leg
            ),
        });
    }
    Ok(spec)
}

/// Catalogue entries of `kind == scenario` for a leg (W6: the rebuild entry is excluded).
pub fn scenarios(leg: Leg) -> impl Iterator<Item = &'static CatalogueEntry> {
    catalogue()
        .entries
        .iter()
        .filter(move |e| e.leg == leg && e.kind == Kind::Scenario)
}

/// Ids whose spec file does not exist yet.
pub fn pending_ids() -> Vec<String> {
    catalogue()
        .entries
        .iter()
        .filter(|e| e.is_pending())
        .map(|e| e.id.clone())
        .collect()
}

/// Pending ids grouped by owner plan.
pub fn pending_by_owner() -> BTreeMap<String, Vec<String>> {
    let mut m: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for e in catalogue().entries.iter().filter(|e| e.is_pending()) {
        m.entry(e.owner.clone()).or_default().push(e.id.clone());
    }
    m
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_research_example() {
        let s: Spec = serde_json::from_str(
            r#"{"id":"C-SIG-01","title":"two signals","workflow":"conf_signals","input":{"n":2},
            "leg":"engine",
            "script":[{"input":"signal","name":"go","payload":{"v":1},"before_start":true},
                      {"input":"signal","name":"go","payload":{"v":2}},
                      {"input":"cancel"},{"input":"fire_timers"},
                      {"input":"swap_module","variant":"v2"},
                      {"input":"api","op":"terminate","target":"child"},
                      {"input":"kill","at":"child::start_child_after_flush"},
                      {"input":"token","payload":1}],
            "expect":{"status":"completed","result":{"got":[{"v":1},{"v":2}]},
                      "projection":["step:prepare","signal:go#1","signal:go#2","terminal:completed"],
                      "differs_across_seeds":true}}"#,
        )
        .unwrap();
        assert_eq!(s.script.len(), 8);
        assert!(matches!(
            &s.script[0],
            ScriptStep::Signal {
                before_start: true,
                ..
            }
        ));
        match &s.script[5] {
            ScriptStep::Api { op, args } => {
                assert_eq!(op, "terminate");
                assert_eq!(args["target"], "child");
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(s.expect.extra["differs_across_seeds"], true);
        assert_eq!(s.languages, None);
    }

    #[test]
    fn rejects_unknown_top_level_field() {
        let r: Result<Spec, _> = serde_json::from_str(
            r#"{"id":"C-X-01","title":"t","leg":"engine","workflow":"w","expect":{"status":"completed"},"bogus":1}"#,
        );
        assert!(r.is_err());
    }

    #[test]
    fn lang_names() {
        let v: Vec<Lang> = serde_json::from_str(r#"["rust","py","ts","ts-sm"]"#).unwrap();
        assert_eq!(v, vec![Lang::Rust, Lang::Py, Lang::Ts, Lang::TsSm]);
    }
}
