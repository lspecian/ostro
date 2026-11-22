//! `ostro dev --watch [DIR]` (D-P10-14, CLI-08): rebuild and redeploy on change.
//!
//! [`run_watch`] runs on its own task next to the in-process server:
//!
//! - one build + deploy at start, then one per debounced batch of changes
//!   (notify-debouncer-full, 250 ms), through [`deploy_project`], the code path of
//!   `ostro deploy [DIR]`;
//! - paths in `ostro_build::pipeline::IGNORE` (`target/`, `.git/`, `*.swp`, `4913`, ...)
//!   and the caller's extra patterns never trigger a build. Ignored top-level
//!   directories are not even watched (no inotify watches on `target/`);
//! - builds are serialised: changes that arrive during a build set a dirty flag, which
//!   gives exactly one follow-up build;
//! - the build itself runs on a dedicated thread, so the server is never blocked;
//! - a build, validation or deploy failure prints the diagnostics with
//!   `ostro dev: build failed; keeping v<N>` and the loop goes on: the server and the
//!   last active version keep serving;
//! - each deploy prints `ostro dev: deployed <workflow> v<N>; new runs use v<N>`.

use std::collections::BTreeSet;
use std::future::Future;
use std::path::{Component, Path, PathBuf};
use std::time::{Duration, Instant};

use notify_debouncer_full::notify::{EventKind, RecursiveMode};
use notify_debouncer_full::{DebounceEventResult, new_debouncer};
use ostro_build::pipeline::{ignored_dir, ignored_file, is_ignored};

use crate::client::Client;
use crate::commands::deploy::{DeployError, DeployOptions, DeployReport, deploy_project};

/// The default debounce window (D-P10-14).
pub const DEBOUNCE_MS: u64 = 250;

/// Options of one watch loop.
#[derive(Debug, Clone)]
pub struct WatchOptions {
    /// Project directory to watch and build.
    pub dir: PathBuf,
    /// Extra ignore patterns on top of the pipeline's built-in list (the same syntax:
    /// `name/` a directory at any depth, `*suffix`, `prefix*`, or an exact file name).
    pub ignore: Vec<String>,
    /// Debounce window in milliseconds.
    pub debounce_ms: u64,
}

impl WatchOptions {
    /// Watch `dir` with the default debounce and no extra ignores.
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        WatchOptions {
            dir: dir.into(),
            ignore: Vec::new(),
            debounce_ms: DEBOUNCE_MS,
        }
    }
}

/// `Ok` when `dir` holds an Ostro project (the usage check `ostro dev --watch` runs before
/// it starts the server; an error is exit 2).
pub fn check_project(dir: &Path) -> Result<(), String> {
    ostro_build::project::load(dir)
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// `true` when `pat` (an extra ignore pattern) matches the component `name`.
fn matches_extra(pat: &str, name: &str) -> bool {
    let pat = pat.strip_suffix('/').unwrap_or(pat);
    if let Some(suffix) = pat.strip_prefix('*') {
        name.ends_with(suffix)
    } else if let Some(prefix) = pat.strip_suffix('*') {
        name.starts_with(prefix)
    } else {
        name == pat
    }
}

/// `true` when a change of `path` (under `root`) should trigger a build: inside the
/// project, not under an ignored directory (any component, the last one included: a
/// created or removed `target/` is not a source change), not an ignored file name, and
/// not matched by an extra pattern.
pub fn relevant(root: &Path, path: &Path, extra: &[String]) -> bool {
    let Ok(rel) = path.strip_prefix(root) else {
        return false;
    };
    if rel.as_os_str().is_empty() || is_ignored(rel) {
        return false;
    }
    !rel.components().any(|c| match c {
        Component::Normal(n) => {
            let n = n.to_string_lossy();
            ignored_dir(&n) || ignored_file(&n) || extra.iter().any(|p| matches_extra(p, &n))
        }
        _ => true,
    })
}

/// The versions `new runs use`: `v2` for one workflow (or when all share a version), else
/// `a v1, b v2`.
fn versions_text(active: &[(String, u64)]) -> String {
    let distinct: BTreeSet<u64> = active.iter().map(|(_, v)| *v).collect();
    match (active.len(), distinct.len()) {
        (0, _) => "nothing deployed yet".to_owned(),
        (_, 1) => format!("v{}", active[0].1),
        _ => active
            .iter()
            .map(|(n, v)| format!("{n} v{v}"))
            .collect::<Vec<_>>()
            .join(", "),
    }
}

/// Print the outcome of one build + deploy and update the active versions.
fn report(res: Result<DeployReport, DeployError>, active: &mut Vec<(String, u64)>, took: Duration) {
    let ms = took.as_millis();
    match res {
        Ok(r) => {
            if r.build.up_to_date && !r.created && r.activated_refs.is_empty() {
                if r.activate {
                    *active = r.workflows.clone();
                }
                eprintln!(
                    "ostro dev: up to date; new runs use {}",
                    versions_text(active)
                );
                return;
            }
            if !r.activate {
                for (wf, v) in &r.workflows {
                    eprintln!(
                        "ostro dev: uploaded {wf} v{v} ({ms} ms); not activated ([deploy] activate = false)"
                    );
                }
                return;
            }
            for (wf, v) in &r.workflows {
                eprintln!("ostro dev: deployed {wf} v{v}; new runs use v{v} ({ms} ms)");
            }
            *active = r.workflows;
        }
        Err(e) => {
            let what = match e {
                DeployError::Call(_) => "deploy failed",
                DeployError::Build(_) | DeployError::NotBuilt(_) => "build failed",
            };
            let keeping = if active.is_empty() {
                "nothing deployed yet".to_owned()
            } else {
                format!("keeping {}", versions_text(active))
            };
            eprintln!("ostro dev: {what}; {keeping}");
            eprintln!("ostro dev: {}", e.message());
        }
    }
}

/// Start watching `root`: the root itself (non-recursive) and every top-level directory
/// that is not ignored (recursive). Returns the watched directories.
fn watch_tree<W>(
    deb: &mut notify_debouncer_full::Debouncer<W, notify_debouncer_full::RecommendedCache>,
    root: &Path,
    extra: &[String],
    watched: &mut BTreeSet<PathBuf>,
) -> Result<(), String>
where
    W: notify_debouncer_full::notify::Watcher,
{
    if watched.insert(root.to_path_buf()) {
        deb.watch(root, RecursiveMode::NonRecursive)
            .map_err(|e| format!("watching {}: {e}", root.display()))?;
    }
    let entries =
        std::fs::read_dir(root).map_err(|e| format!("reading {}: {e}", root.display()))?;
    for e in entries.flatten() {
        let p = e.path();
        // `file_type` does not follow symlinks: symlinked dirs are not watched (nor staged).
        if e.file_type().is_ok_and(|t| t.is_dir()) {
            watch_dir(deb, root, &p, extra, watched);
        }
    }
    Ok(())
}

/// Watch the top-level directory `dir` recursively unless it is ignored or watched.
fn watch_dir<W>(
    deb: &mut notify_debouncer_full::Debouncer<W, notify_debouncer_full::RecommendedCache>,
    root: &Path,
    dir: &Path,
    extra: &[String],
    watched: &mut BTreeSet<PathBuf>,
) where
    W: notify_debouncer_full::notify::Watcher,
{
    if dir.parent() != Some(root) || !relevant(root, dir, extra) || watched.contains(dir) {
        return;
    }
    match deb.watch(dir, RecursiveMode::Recursive) {
        Ok(()) => {
            watched.insert(dir.to_path_buf());
        }
        Err(e) => eprintln!("ostro dev: cannot watch {}: {e}", dir.display()),
    }
}

/// Runs the watch loop against `server` until `shutdown` resolves. Errors are setup
/// failures (the watcher could not start); build and deploy failures are printed and the
/// loop goes on.
pub async fn run_watch(
    opts: WatchOptions,
    server: String,
    shutdown: impl Future<Output = ()> + Send,
) -> Result<(), String> {
    let root =
        std::fs::canonicalize(&opts.dir).map_err(|e| format!("{}: {e}", opts.dir.display()))?;
    check_project(&root)?;
    let client = Client::new(&server, Duration::from_secs(30)).map_err(|e| e.message())?;
    let deploy = DeployOptions {
        dir: root.clone(),
        ..DeployOptions::default()
    };
    let name = ostro_build::project::load(&root)
        .map(|p| p.manifest.project.name)
        .map_err(|e| e.to_string())?;

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Vec<PathBuf>>();
    let (filter_root, extra) = (root.clone(), opts.ignore.clone());
    let mut deb = new_debouncer(
        Duration::from_millis(opts.debounce_ms.max(1)),
        None,
        move |res: DebounceEventResult| match res {
            Ok(events) => {
                let paths: Vec<PathBuf> = events
                    .iter()
                    .filter(|e| !matches!(e.event.kind, EventKind::Access(_)))
                    .flat_map(|e| e.event.paths.iter())
                    .filter(|p| relevant(&filter_root, p, &extra))
                    .cloned()
                    .collect();
                if !paths.is_empty() {
                    let _ = tx.send(paths);
                }
            }
            Err(errs) => {
                for e in errs {
                    eprintln!("ostro dev: watch error: {e}");
                }
            }
        },
    )
    .map_err(|e| format!("starting the file watcher: {e}"))?;
    let mut watched = BTreeSet::new();
    watch_tree(&mut deb, &root, &opts.ignore, &mut watched)?;
    eprintln!(
        "ostro dev: watching {} (debounce {} ms)",
        root.display(),
        opts.debounce_ms
    );

    let mut active: Vec<(String, u64)> = Vec::new();
    let mut shutdown = std::pin::pin!(shutdown);
    // The initial build, then one build per batch; `dirty` = changes arrived meanwhile.
    let mut dirty = true;
    loop {
        if dirty {
            eprintln!("ostro dev: building {name}");
            let t0 = Instant::now();
            let res = tokio::select! {
                r = deploy_project(&client, &deploy) => r,
                () = &mut shutdown => return Ok(()),
            };
            report(res, &mut active, t0.elapsed());
            // Everything queued during the build collapses into one follow-up build.
            dirty = false;
            while let Ok(paths) = rx.try_recv() {
                for p in &paths {
                    watch_dir(&mut deb, &root, p, &opts.ignore, &mut watched);
                }
                dirty = true;
            }
            continue;
        }
        tokio::select! {
            () = &mut shutdown => return Ok(()),
            m = rx.recv() => match m {
                Some(paths) => {
                    for p in &paths {
                        watch_dir(&mut deb, &root, p, &opts.ignore, &mut watched);
                    }
                    dirty = true;
                }
                None => return Err("the file watcher stopped".to_owned()),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relevance_follows_the_pipeline_ignore_set() {
        let root = Path::new("/p");
        let yes = |p: &str| relevant(root, Path::new(p), &[]);
        assert!(yes("/p/src/lib.rs"));
        assert!(yes("/p/ostro.toml"));
        assert!(yes("/p/Cargo.lock"));
        assert!(yes("/p/src/nested/mod.rs"));
        for no in [
            "/p",
            "/elsewhere/x.rs",
            "/p/target",
            "/p/target/ostro/x.wasm",
            "/p/.git/index",
            "/p/.ostro/data",
            "/p/node_modules/a/index.js",
            "/p/src/__pycache__/a.pyc",
            "/p/src/.lib.rs.swp",
            "/p/src/4913",
            "/p/src/lib.rs~",
            "/p/src/.#lib.rs",
            "/p/.venv",
        ] {
            assert!(!yes(no), "{no}");
        }
        let extra = vec!["*.log".to_owned(), "scratch/".to_owned()];
        assert!(!relevant(root, Path::new("/p/build.log"), &extra));
        assert!(!relevant(root, Path::new("/p/scratch/a.rs"), &extra));
        assert!(relevant(root, Path::new("/p/src/a.rs"), &extra));
    }

    #[test]
    fn version_texts() {
        assert_eq!(versions_text(&[]), "nothing deployed yet");
        assert_eq!(versions_text(&[("hello".into(), 2)]), "v2");
        assert_eq!(versions_text(&[("a".into(), 3), ("b".into(), 3)]), "v3");
        assert_eq!(
            versions_text(&[("a".into(), 1), ("b".into(), 2)]),
            "a v1, b v2"
        );
    }

    #[test]
    fn check_project_is_a_usage_check() {
        let d = tempfile::tempdir().unwrap();
        let e = check_project(d.path()).unwrap_err();
        assert!(e.contains("no Ostro project"), "{e}");
    }
}
