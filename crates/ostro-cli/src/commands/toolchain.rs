//! `ostro toolchain install|list|prune` and `ostro doctor` (CLI-08): the hermetic
//! build toolchains and an environment check.
//!
//! Plan 10-03 fixed the flag set; plan 10-07 implements the verbs on top of
//! `ostro_build::{cache, toolchain}`. Every verb takes its environment through [`Env`] so
//! tests never mutate the process environment.

use std::path::{Path, PathBuf};

use clap::{ArgGroup, Args, Subcommand, ValueEnum};
use ostro_build::Lang;
use ostro_build::cache::{self, Cache};
use ostro_build::fetch::{self, FetchOptions};
use ostro_build::toolchain::{
    self as tc, Pin, Platform, Tool, ToolchainError, ToolchainSet, ensure,
};
use serde_json::{Value, json};

use crate::commands::block_on;
use crate::output::{Outcome, local_json};

/// A toolchain language.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ToolchainLang {
    Rust,
    Python,
    Ts,
}

/// `ostro toolchain <install|list|prune>`.
#[derive(Debug, Clone, Args)]
pub struct ToolchainArgs {
    #[command(subcommand)]
    pub command: ToolchainCommand,
}

/// The toolchain subcommands.
#[derive(Debug, Clone, Subcommand)]
pub enum ToolchainCommand {
    /// Install the pinned toolchain for one language or all of them.
    Install(InstallArgs),
    /// List installed toolchains.
    List(ListArgs),
    /// Remove toolchains that are no longer pinned.
    Prune(PruneArgs),
}

/// `ostro toolchain install [--lang L | --all] [--json]`.
#[derive(Debug, Clone, Args)]
#[command(group(ArgGroup::new("which").args(["lang", "all"]).multiple(false)))]
pub struct InstallArgs {
    /// Language whose toolchain to install.
    #[arg(long, value_enum)]
    pub lang: Option<ToolchainLang>,
    /// Install every language's toolchain.
    #[arg(long)]
    pub all: bool,
    /// Print an `ostro.toolchain.result` JSON object.
    #[arg(long)]
    pub json: bool,
}

/// `ostro toolchain list [--json]`.
#[derive(Debug, Clone, Args)]
pub struct ListArgs {
    /// Print an `ostro.toolchain.result` JSON object.
    #[arg(long)]
    pub json: bool,
}

/// `ostro toolchain prune [--state] [--json]`.
#[derive(Debug, Clone, Args)]
pub struct PruneArgs {
    /// Also remove cached state (downloads, build caches).
    #[arg(long)]
    pub state: bool,
    /// Print an `ostro.toolchain.result` JSON object.
    #[arg(long)]
    pub json: bool,
}

/// `ostro doctor [--json]`.
#[derive(Debug, Clone, Args)]
pub struct DoctorArgs {
    /// Print an `ostro.doctor.result` JSON object.
    #[arg(long)]
    pub json: bool,
}

impl ToolchainLang {
    fn lang(self) -> Lang {
        match self {
            ToolchainLang::Rust => Lang::Rust,
            ToolchainLang::Python => Lang::Python,
            ToolchainLang::Ts => Lang::Ts,
        }
    }
}

/// The environment a verb reads (`OSTRO_TOOLCHAIN_DIR`, `OSTRO_OFFLINE`,
/// `OSTRO_TOOLCHAIN_MIRROR`, `OSTRO_DOCKER`, `XDG_CACHE_HOME`, `HOME`).
pub struct Env {
    lookup: Box<Lookup>,
}

type Lookup = dyn Fn(&str) -> Option<String> + Send + Sync;

impl Env {
    /// The process environment.
    pub fn process() -> Env {
        Env {
            lookup: Box::new(|k| std::env::var(k).ok()),
        }
    }

    /// A fixed set of variables (tests).
    pub fn from_pairs(pairs: &[(&str, &str)]) -> Env {
        let owned: Vec<(String, String)> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Env {
            lookup: Box::new(move |k| owned.iter().find(|(n, _)| n == k).map(|(_, v)| v.clone())),
        }
    }

    fn get(&self, k: &str) -> Option<String> {
        (self.lookup)(k).filter(|v| !v.is_empty())
    }
}

/// Run `ostro toolchain ...`.
pub fn run(a: &ToolchainArgs) -> Outcome {
    let env = Env::process();
    let ((outcome, report), json) = match &a.command {
        ToolchainCommand::Install(i) => (install_report(i, &env), i.json),
        ToolchainCommand::List(l) => (list_report(&env), l.json),
        ToolchainCommand::Prune(p) => (prune_report(p, &env), p.json),
    };
    emit("toolchain", json, outcome, report)
}

/// Run `ostro doctor` (exit 0 even with warnings).
pub fn doctor(a: &DoctorArgs) -> Outcome {
    let (outcome, report) = doctor_report(&Env::process());
    emit("doctor", a.json, outcome, report)
}

/// A verb result: the outcome plus the report (`Err` = a message for stderr).
pub type Report = (Outcome, Body);
/// `Ok((json body, human text))` or an error message for stderr.
pub type Body = Result<(Value, String), String>;

fn emit(kind: &str, json: bool, outcome: Outcome, report: Body) -> Outcome {
    match report {
        Ok((body, human)) => {
            if json {
                println!("{}", local_json(kind, &body));
            } else {
                print!("{human}");
            }
        }
        Err(msg) => eprintln!("ostro: error: {msg}"),
    }
    outcome
}

/// Cache, platform and pinned set from `env`.
fn context(env: &Env) -> Result<(Cache, Platform, ToolchainSet), ToolchainError> {
    let cache = Cache::resolve_with(|k| env.get(k), std::env::consts::OS)?;
    let platform = Platform::detect()?;
    let set = tc::toolchain_set(&platform)?;
    Ok((cache, platform, set))
}

fn fetch_options(env: &Env) -> Result<FetchOptions, ToolchainError> {
    FetchOptions::from_env_with(|k| env.get(k))
}

/// The pins `install` covers: `--all` every pin, `--lang L` that language's tools, no flag
/// every language's tools (zig only when no `cc` is on PATH).
fn selected_pins(a: &InstallArgs, set: &ToolchainSet, cc_present: bool) -> Vec<Pin> {
    if a.all {
        return set.pins.clone();
    }
    let langs = match a.lang {
        Some(l) => vec![l.lang()],
        None => vec![Lang::Rust, Lang::Python, Lang::Ts],
    };
    let mut tools: Vec<Tool> = Vec::new();
    for l in langs {
        for t in tc::tools_for_with(l, cc_present) {
            if !tools.contains(&t) {
                tools.push(t);
            }
        }
    }
    tools
        .into_iter()
        .filter_map(|t| set.pin(t).cloned())
        .collect()
}

fn path_str(p: &Path) -> String {
    p.display().to_string()
}

/// `ostro toolchain install` (progress on stderr).
pub fn install_report(a: &InstallArgs, env: &Env) -> Report {
    let (cache, platform, set) = match context(env) {
        Ok(c) => c,
        Err(e) => return (Outcome::Error, Err(e.to_string())),
    };
    let opts = match fetch_options(env) {
        Ok(o) => o,
        Err(e) => return (Outcome::Error, Err(e.to_string())),
    };
    let pins = selected_pins(a, &set, tc::cc_available());
    let mut rows: Vec<Value> = Vec::new();
    let mut human = format!("cache: {}\n", path_str(&cache.root));
    let mut failed = false;
    let outcome = block_on(async {
        for pin in &pins {
            let label = format!("{} {}", pin.tool, pin.version);
            let was_cached = cache.is_installed(pin);
            if !was_cached && !opts.offline {
                eprintln!("ostro: installing {label} ...");
            }
            match ensure(&cache, pin, &opts).await {
                Ok(dir) => {
                    let status = if was_cached { "cached" } else { "installed" };
                    human.push_str(&format!("  {label:<24} {status:<10} {}\n", path_str(&dir)));
                    rows.push(json!({
                        "tool": pin.tool.name(),
                        "version": pin.version,
                        "status": status,
                        "path": path_str(&dir),
                    }));
                }
                Err(e) => {
                    failed = true;
                    let msg = e.to_string();
                    if msg.starts_with(&label) {
                        eprintln!("ostro: error: {msg}");
                    } else {
                        eprintln!("ostro: error: {label}: {msg}");
                    }
                    human.push_str(&format!("  {label:<24} error      {e}\n"));
                    rows.push(json!({
                        "tool": pin.tool.name(),
                        "version": pin.version,
                        "status": "error",
                        "error": e.to_string(),
                    }));
                }
            }
        }
        if failed { Outcome::Error } else { Outcome::Ok }
    });
    let body = json!({
        "action": "install",
        "cache_root": path_str(&cache.root),
        "platform": platform.key(),
        "toolchain_set": set.id,
        "offline": opts.offline,
        "tools": rows,
    });
    (outcome, Ok((body, human)))
}

/// Per-pin installed state.
fn tool_rows(cache: &Cache, set: &ToolchainSet) -> (Vec<Value>, String) {
    let mut rows = Vec::new();
    let mut human = String::new();
    for pin in &set.pins {
        let installed = cache.is_installed(pin);
        let dir = cache.entry_dir(pin);
        let label = format!("{} {}", pin.tool, pin.version);
        human.push_str(&format!(
            "  {label:<24} {}\n",
            if installed {
                format!("installed  {}", path_str(&dir))
            } else {
                "not installed".to_string()
            }
        ));
        rows.push(json!({
            "tool": pin.tool.name(),
            "version": pin.version,
            "entry": Cache::entry_name(pin),
            "installed": installed,
            "path": path_str(&dir),
        }));
    }
    (rows, human)
}

/// `ostro toolchain list`.
pub fn list_report(env: &Env) -> Report {
    let (cache, platform, set) = match context(env) {
        Ok(c) => c,
        Err(e) => return (Outcome::Error, Err(e.to_string())),
    };
    let (rows, tools_human) = tool_rows(&cache, &set);
    let pinned: Vec<String> = set.pins.iter().map(Cache::entry_name).collect();
    let unknown: Vec<Value> = cache::entries(&cache)
        .into_iter()
        .filter(|e| !pinned.contains(&e.name))
        .map(|e| json!({"name": e.name, "path": path_str(&e.path), "complete": e.sha256.is_some()}))
        .collect();
    let mut human = format!(
        "cache: {}\ntoolchain set: {} ({})\n{tools_human}",
        path_str(&cache.root),
        set.id,
        platform.key()
    );
    if !unknown.is_empty() {
        human.push_str("not in the current set (removed by `ostro toolchain prune`):\n");
        for u in &unknown {
            human.push_str(&format!("  {}\n", u["name"].as_str().unwrap_or_default()));
        }
    }
    let body = json!({
        "action": "list",
        "cache_root": path_str(&cache.root),
        "platform": platform.key(),
        "toolchain_set": set.id,
        "tools": rows,
        "unknown": unknown,
    });
    (Outcome::Ok, Ok((body, human)))
}

/// `ostro toolchain prune [--state]`.
pub fn prune_report(a: &PruneArgs, env: &Env) -> Report {
    let (cache, _platform, set) = match context(env) {
        Ok(c) => c,
        Err(e) => return (Outcome::Error, Err(e.to_string())),
    };
    let mut result: Result<Vec<PathBuf>, ToolchainError> = Ok(Vec::new());
    block_on(async {
        result = cache::prune(&cache, &set, a.state).await;
        Outcome::Ok
    });
    match result {
        Ok(removed) => {
            let mut human = format!("cache: {}\n", path_str(&cache.root));
            if removed.is_empty() {
                human.push_str("nothing to remove\n");
            }
            for r in &removed {
                human.push_str(&format!("  removed {}\n", path_str(r)));
            }
            let body = json!({
                "action": "prune",
                "cache_root": path_str(&cache.root),
                "toolchain_set": set.id,
                "state": a.state,
                "removed": removed.iter().map(|p| path_str(p)).collect::<Vec<_>>(),
            });
            (Outcome::Ok, Ok((body, human)))
        }
        Err(e) => (Outcome::Error, Err(e.to_string())),
    }
}

/// `root` can be created and written.
fn writable(root: &Path) -> bool {
    if std::fs::create_dir_all(root).is_err() {
        return false;
    }
    let probe = root.join(format!(".doctor-{}", std::process::id()));
    let ok = std::fs::write(&probe, b"ok").is_ok();
    let _ = std::fs::remove_file(&probe);
    ok
}

/// The docker CLI: `OSTRO_DOCKER` (a path or a name on PATH), else `docker` on PATH.
fn docker_path(env: &Env) -> Option<PathBuf> {
    match env.get("OSTRO_DOCKER") {
        Some(d) if d.contains('/') => Some(PathBuf::from(d)).filter(|p| p.is_file()),
        Some(d) => tc::which(&d),
        None => tc::which("docker"),
    }
}

/// `ostro doctor`: never fails on warnings.
pub fn doctor_report(env: &Env) -> Report {
    let mut warnings: Vec<String> = Vec::new();
    let cache = Cache::resolve_with(|k| env.get(k), std::env::consts::OS);
    let platform = Platform::detect();
    let set = platform.as_ref().ok().map(tc::toolchain_set);

    let (cache_root, cache_writable) = match &cache {
        Ok(c) => {
            let w = writable(&c.root);
            if !w {
                warnings.push(format!(
                    "the toolchain cache {} is not writable",
                    path_str(&c.root)
                ));
            }
            (Value::String(path_str(&c.root)), w)
        }
        Err(e) => {
            warnings.push(e.to_string());
            (Value::Null, false)
        }
    };
    let platform_key = match &platform {
        Ok(p) => Value::String(p.key()),
        Err(e) => {
            warnings.push(e.to_string());
            Value::Null
        }
    };
    let (set_id, tools, tools_human) = match (&cache, set) {
        (Ok(c), Some(Ok(s))) => {
            let (rows, h) = tool_rows(c, &s);
            (Value::String(s.id.clone()), rows, h)
        }
        (_, Some(Err(e))) => {
            warnings.push(e.to_string());
            (Value::Null, Vec::new(), String::new())
        }
        _ => (Value::Null, Vec::new(), String::new()),
    };

    let cc = ["cc", "gcc", "clang"].iter().find_map(|c| tc::which(c));
    if cc.is_none() {
        warnings.push("no C compiler on PATH: Rust builds use the hermetic zig cc".into());
    }
    let docker = docker_path(env);
    let glibc = tc::glibc_version();
    if let Some(v) = &glibc
        && !tc::glibc_at_least_2_28(v)
    {
        warnings.push(format!(
            "glibc {v} is older than 2.28: hermetic Python/TS toolchains need it, use --docker"
        ));
    }
    let musl = tc::musl_host();
    if musl {
        warnings
            .push("musl host: hermetic Python/TS toolchains are unsupported, use --docker".into());
    }
    let offline = env
        .get("OSTRO_OFFLINE")
        .is_some_and(|v| fetch::is_truthy(&v));
    let mirror = env.get("OSTRO_TOOLCHAIN_MIRROR");

    let opt = |v: &Option<PathBuf>| v.as_deref().map(path_str);
    let mut human = String::from("ostro doctor\n");
    human.push_str(&format!(
        "  cache root:     {} ({})\n",
        cache_root.as_str().unwrap_or("unresolved"),
        if cache_writable {
            "writable"
        } else {
            "not writable"
        }
    ));
    human.push_str(&format!(
        "  platform:       {}\n",
        platform_key.as_str().unwrap_or("unsupported")
    ));
    human.push_str(&format!(
        "  toolchain set:  {}\n",
        set_id.as_str().unwrap_or("-")
    ));
    human.push_str(&tools_human);
    human.push_str(&format!(
        "  cc:             {}\n",
        opt(&cc).unwrap_or_else(|| "missing: Rust builds use hermetic zig".into())
    ));
    human.push_str(&format!(
        "  docker:         {}\n",
        opt(&docker).unwrap_or_else(|| "missing (--docker unavailable)".into())
    ));
    human.push_str(&format!(
        "  glibc:          {}\n",
        glibc.as_deref().unwrap_or("n/a")
    ));
    human.push_str(&format!(
        "  musl host:      {}\n",
        if musl { "yes" } else { "no" }
    ));
    human.push_str(&format!(
        "  OSTRO_OFFLINE:  {}\n",
        if offline { "on" } else { "off" }
    ));
    human.push_str(&format!(
        "  OSTRO_TOOLCHAIN_MIRROR: {}\n",
        mirror.as_deref().unwrap_or("(unset)")
    ));
    if !warnings.is_empty() {
        human.push_str("warnings:\n");
        for w in &warnings {
            human.push_str(&format!("  - {w}\n"));
        }
    }
    let body = json!({
        "cache_root": cache_root,
        "cache_writable": cache_writable,
        "platform": platform_key,
        "toolchain_set": set_id,
        "tools": tools,
        "cc": {"present": cc.is_some(), "path": opt(&cc)},
        "docker": {"present": docker.is_some(), "path": opt(&docker)},
        "glibc": glibc,
        "musl": musl,
        "offline": offline,
        "mirror": mirror,
        "warnings": warnings,
    });
    (Outcome::Ok, Ok((body, human)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env_for(dir: &Path, extra: &[(&str, &str)]) -> Env {
        let root = dir.to_string_lossy().into_owned();
        let mut pairs: Vec<(&str, &str)> = vec![("OSTRO_TOOLCHAIN_DIR", root.as_str())];
        pairs.extend_from_slice(extra);
        Env::from_pairs(&pairs)
    }

    fn body(r: Report) -> (Outcome, Value) {
        let (o, r) = r;
        (o, r.expect("report").0)
    }

    fn current_set() -> ToolchainSet {
        tc::toolchain_set(&Platform::detect().unwrap()).unwrap()
    }

    /// Fake a completed install of `pin` (no network).
    fn fake_install(cache: &Cache, pin: &Pin) {
        let dir = cache.entry_dir(pin);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(".ok"), format!("{}\n", pin.sha256)).unwrap();
    }

    #[test]
    fn toolchain_list_reports_pins_and_unknown_entries() {
        let d = tempfile::tempdir().unwrap();
        let env = env_for(d.path(), &[]);
        let cache = Cache::with_root(d.path());
        let set = current_set();
        let uv = set.pin(Tool::Uv).unwrap().clone();
        fake_install(&cache, &uv);
        std::fs::create_dir_all(cache.tools_dir().join("old-0.1-abcdef01")).unwrap();

        let (o, b) = body(list_report(&env));
        assert_eq!(o, Outcome::Ok);
        assert_eq!(b["action"], "list");
        assert_eq!(b["cache_root"], d.path().display().to_string());
        assert_eq!(b["toolchain_set"], set.id);
        let tools = b["tools"].as_array().unwrap();
        assert_eq!(tools.len(), set.pins.len());
        let uv_row = tools.iter().find(|t| t["tool"] == "uv").unwrap();
        assert_eq!(uv_row["installed"], true);
        assert_eq!(uv_row["version"], "0.7.15");
        assert!(
            tools
                .iter()
                .filter(|t| t["tool"] != "uv")
                .all(|t| t["installed"] == false)
        );
        assert_eq!(b["unknown"][0]["name"], "old-0.1-abcdef01");
        assert_eq!(b["unknown"][0]["complete"], false);

        let s = local_json("toolchain", &b);
        assert!(
            s.starts_with("{\n  \"kind\": \"ostro.toolchain.result\",\n  \"version\": 1,"),
            "{s}"
        );
    }

    #[test]
    fn toolchain_prune_keeps_state_unless_asked() {
        let d = tempfile::tempdir().unwrap();
        let env = env_for(d.path(), &[]);
        let cache = Cache::with_root(d.path());
        let set = current_set();
        let node = set.pin(Tool::Node).unwrap().clone();
        fake_install(&cache, &node);
        let stale = cache.tools_dir().join("node-1.0.0-00000000");
        std::fs::create_dir_all(&stale).unwrap();
        let state = cache.state_dir("jco");
        std::fs::create_dir_all(&state).unwrap();

        let p = PruneArgs {
            state: false,
            json: true,
        };
        let (o, b) = body(prune_report(&p, &env));
        assert_eq!(o, Outcome::Ok);
        assert_eq!(b["removed"], json!([stale.display().to_string()]));
        assert!(state.is_dir());
        assert!(cache.is_installed(&node));

        let p = PruneArgs {
            state: true,
            json: false,
        };
        let (o, b) = body(prune_report(&p, &env));
        assert_eq!(o, Outcome::Ok);
        assert_eq!(b["removed"], json!([state.display().to_string()]));
        assert!(!state.exists());
        assert!(cache.is_installed(&node));
    }

    #[test]
    fn toolchain_install_offline_uses_cache_and_never_downloads() {
        let d = tempfile::tempdir().unwrap();
        let env = env_for(d.path(), &[("OSTRO_OFFLINE", "1")]);
        let cache = Cache::with_root(d.path());
        let set = current_set();

        // uncached + offline: exit 1, a clear error per tool, no request possible
        let a = InstallArgs {
            lang: Some(ToolchainLang::Ts),
            all: false,
            json: true,
        };
        let (o, b) = body(install_report(&a, &env));
        assert_eq!(o, Outcome::Error);
        assert_eq!(b["offline"], true);
        assert_eq!(b["tools"][0]["tool"], "node");
        assert_eq!(b["tools"][0]["status"], "error");
        assert!(
            b["tools"][0]["error"]
                .as_str()
                .unwrap()
                .contains("offline and not cached")
        );

        // cached + offline: ok
        fake_install(&cache, set.pin(Tool::Node).unwrap());
        let (o, b) = body(install_report(&a, &env));
        assert_eq!(o, Outcome::Ok);
        assert_eq!(b["tools"][0]["status"], "cached");

        let a = InstallArgs {
            lang: Some(ToolchainLang::Python),
            all: false,
            json: false,
        };
        let (o, b) = body(install_report(&a, &env));
        assert_eq!(o, Outcome::Error);
        let names: Vec<_> = b["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["tool"].clone())
            .collect();
        assert_eq!(names, vec![json!("uv"), json!("componentize-py")]);
    }

    #[test]
    fn toolchain_install_selection() {
        let set = current_set();
        let pick = |lang, all, cc| -> Vec<Tool> {
            selected_pins(
                &InstallArgs {
                    lang,
                    all,
                    json: false,
                },
                &set,
                cc,
            )
            .into_iter()
            .map(|p| p.tool)
            .collect()
        };
        assert_eq!(
            pick(Some(ToolchainLang::Rust), false, true),
            vec![Tool::RustupInit]
        );
        assert_eq!(
            pick(Some(ToolchainLang::Rust), false, false),
            vec![Tool::RustupInit, Tool::Zig]
        );
        assert_eq!(pick(None, true, true).len(), set.pins.len());
        assert_eq!(
            pick(None, false, true),
            vec![Tool::RustupInit, Tool::Uv, Tool::ComponentizePy, Tool::Node]
        );
    }

    #[test]
    fn toolchain_doctor_reports_environment() {
        let d = tempfile::tempdir().unwrap();
        let fake_docker = d.path().join("fake-docker");
        std::fs::write(&fake_docker, b"#!/bin/sh\n").unwrap();
        let docker = fake_docker.to_string_lossy().into_owned();
        let root = d.path().join("cache");
        let env = env_for(
            &root,
            &[
                ("OSTRO_DOCKER", docker.as_str()),
                ("OSTRO_TOOLCHAIN_MIRROR", "http://m.local"),
            ],
        );
        let (o, b) = doctor_report(&env);
        let b = b.unwrap().0;
        assert_eq!(o, Outcome::Ok);
        assert_eq!(b["cache_root"], root.display().to_string());
        assert_eq!(b["cache_writable"], true);
        assert_eq!(b["platform"], Platform::detect().unwrap().key());
        assert_eq!(b["toolchain_set"], tc::set_id());
        assert_eq!(b["docker"]["present"], true);
        assert_eq!(b["docker"]["path"], docker);
        assert_eq!(b["mirror"], "http://m.local");
        assert_eq!(b["offline"], false);
        assert_eq!(b["cc"]["present"], tc::cc_available());
        assert!(
            b["tools"]
                .as_array()
                .unwrap()
                .iter()
                .all(|t| t["installed"] == false)
        );
        if cfg!(target_os = "linux") && !tc::musl_host() {
            assert!(b["glibc"].is_string(), "{b}");
        }
        let s = local_json("doctor", &b);
        assert!(
            s.starts_with("{\n  \"kind\": \"ostro.doctor.result\",\n  \"version\": 1,"),
            "{s}"
        );

        // a missing OSTRO_DOCKER path is reported as absent, still exit 0
        let env = env_for(&root, &[("OSTRO_DOCKER", "/nonexistent/docker")]);
        let (o, b) = doctor_report(&env);
        assert_eq!(o, Outcome::Ok);
        assert_eq!(b.unwrap().0["docker"]["present"], false);
    }
}
