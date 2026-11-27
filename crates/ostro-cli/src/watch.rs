//! `ostro dev --watch [DIR]` (D-P10-14, CLI-08): rebuild and redeploy on change.
//!
//! [`run_watch`] runs on its own task next to the in-process server:
//!
//! - one build + deploy at start, then one per debounced batch of changes
//!   ([`debounce`]: 250 ms quiet window, 2 s max latency), through [`deploy_project`],
//!   the code path of `ostro deploy [DIR]`;
//! - the watcher is our own (D-P14-47, replaces `notify`): [`inotify`] over `libc` on
//!   Linux (falls back to [`poll`] when inotify limits are reached), [`poll`] elsewhere;
//! - paths in `ostro_build::pipeline::IGNORE` (`target/`, `.git/`, `*.swp`, `4913`, ...)
//!   and the caller's extra patterns never trigger a build. Ignored directories are not
//!   even watched or walked (no inotify watches on `target/`);
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

use std::sync::Arc;

use ostro_build::pipeline::{ignored_dir, ignored_file, is_ignored};

use crate::client::Client;
use crate::commands::deploy::{DeployError, DeployOptions, DeployReport, deploy_project};

pub mod debounce;
#[cfg(target_os = "linux")]
pub mod inotify;
pub mod poll;

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

/// The path filter a backend applies: which directories are watched (walked) and which
/// changed paths are reported.
pub(crate) type Filter = Arc<dyn Fn(&Path) -> bool + Send + Sync>;

/// Which backend to start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    /// inotify on Linux, polling elsewhere.
    Native,
    /// mtime/size/inode polling (the test-only way to pick it on Linux).
    #[cfg_attr(not(test), allow(dead_code))]
    Poll,
}

/// A running watcher backend (stops when dropped).
pub(crate) enum Backend {
    #[cfg(target_os = "linux")]
    Inotify {
        _watcher: inotify::Watcher,
    },
    Poll {
        _watcher: poll::Watcher,
    },
}

/// Start a backend over `root`. On Linux, `Kind::Native` falls back to polling (with one
/// `ostro dev: inotify unavailable (<err>); polling` line) when inotify limits are hit.
pub(crate) fn start_backend(
    kind: Kind,
    root: &Path,
    filter: Filter,
    tx: tokio::sync::mpsc::UnboundedSender<debounce::Event>,
) -> Result<Backend, String> {
    #[cfg(target_os = "linux")]
    if kind == Kind::Native {
        match inotify::Watcher::start(root, filter.clone(), tx.clone()) {
            Ok(w) => return Ok(Backend::Inotify { _watcher: w }),
            Err(e) if inotify::is_limit(&e) => {
                eprintln!("ostro dev: inotify unavailable ({e}); polling");
            }
            Err(e) => return Err(format!("starting the file watcher: {e}")),
        }
    }
    let _ = kind;
    poll::Watcher::start(root, filter, tx, poll::INTERVAL)
        .map(|w| Backend::Poll { _watcher: w })
        .map_err(|e| format!("starting the file watcher: {e}"))
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

    // watcher events -> debounce (quiet window + max latency) -> batches -> build loop
    let (events, mut rx) = debounce::spawn(
        Duration::from_millis(opts.debounce_ms.max(1)),
        Duration::from_millis(debounce::MAX_LATENCY_MS),
    );
    let (filter_root, extra) = (root.clone(), opts.ignore.clone());
    let filter: Filter = Arc::new(move |p: &Path| relevant(&filter_root, p, &extra));
    let _backend = start_backend(Kind::Native, &root, filter, events)?;
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
            while let Ok(batch) = rx.try_recv() {
                dirty |= !batch.is_empty();
            }
            continue;
        }
        tokio::select! {
            () = &mut shutdown => return Ok(()),
            m = rx.recv() => match m {
                Some(batch) => dirty = !batch.is_empty(),
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

    // ---- watcher backends + debouncer (D-P14-47): the same scenarios for both ----

    use debounce::Batch;
    use tokio::sync::mpsc::UnboundedReceiver;

    struct Rig {
        _dir: tempfile::TempDir,
        root: PathBuf,
        kind: Kind,
        _backend: Backend,
        rx: UnboundedReceiver<Batch>,
    }

    impl Rig {
        async fn start(kind: Kind) -> Rig {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path().canonicalize().unwrap();
            std::fs::create_dir_all(root.join("src")).unwrap();
            std::fs::create_dir_all(root.join("target/debug")).unwrap();
            std::fs::create_dir_all(root.join(".git")).unwrap();
            std::fs::write(root.join("src/lib.rs"), "fn a() {}").unwrap();
            // polling: a quiet window longer than the interval keeps one change one batch
            let quiet = match kind {
                Kind::Native => 100,
                Kind::Poll => 450,
            };
            let (tx, rx) = debounce::spawn(
                Duration::from_millis(quiet),
                Duration::from_millis(debounce::MAX_LATENCY_MS),
            );
            let fr = root.clone();
            let filter: Filter = Arc::new(move |p: &Path| relevant(&fr, p, &[]));
            let backend = start_backend(kind, &root, filter, tx).unwrap();
            Rig {
                _dir: dir,
                root,
                kind,
                _backend: backend,
                rx,
            }
        }

        fn p(&self, rel: &str) -> PathBuf {
            self.root.join(rel)
        }

        /// The batches of one change: the first within 5 s, then whatever follows
        /// within a settle window.
        async fn batches(&mut self) -> Vec<Batch> {
            let first = tokio::time::timeout(Duration::from_secs(5), self.rx.recv())
                .await
                .unwrap_or_else(|_| panic!("{:?}: no batch within 5 s", self.kind))
                .unwrap();
            let mut out = vec![first];
            out.extend(self.quiet_batches().await);
            out
        }

        /// Batches arriving within the settle window (expected: none).
        async fn quiet_batches(&mut self) -> Vec<Batch> {
            let settle = Duration::from_millis(match self.kind {
                Kind::Native => 600,
                Kind::Poll => 1_500,
            });
            let mut out = Vec::new();
            let deadline = tokio::time::Instant::now() + settle;
            while let Ok(Some(b)) = tokio::time::timeout_at(deadline, self.rx.recv()).await {
                out.push(b);
            }
            out
        }
    }

    fn one_batch(kind: Kind, what: &str, got: &[Batch], must: &[PathBuf]) {
        assert_eq!(got.len(), 1, "{kind:?} {what}: {got:?}");
        for p in must {
            assert!(
                got[0].paths.contains(p),
                "{kind:?} {what}: {p:?} not in {got:?}"
            );
        }
    }

    async fn scenarios(kind: Kind) {
        let mut r = Rig::start(kind).await;
        // let a polling snapshot / the inotify watches settle
        assert!(r.quiet_batches().await.is_empty(), "{kind:?}: setup noise");

        std::fs::write(r.p("src/lib.rs"), "fn a() { /* edited */ }").unwrap();
        let b = r.batches().await;
        one_batch(kind, "modify", &b, &[r.p("src/lib.rs")]);

        std::fs::write(r.p("src/new.rs"), "fn n() {}").unwrap();
        let b = r.batches().await;
        one_batch(kind, "create", &b, &[r.p("src/new.rs")]);

        std::fs::remove_file(r.p("src/new.rs")).unwrap();
        let b = r.batches().await;
        one_batch(kind, "delete", &b, &[r.p("src/new.rs")]);

        std::fs::rename(r.p("src/lib.rs"), r.p("src/lib2.rs")).unwrap();
        let b = r.batches().await;
        one_batch(kind, "rename", &b, &[r.p("src/lib.rs"), r.p("src/lib2.rs")]);

        // noise: ignored directories and file names never give a batch
        std::fs::write(r.p("target/debug/noise.o"), "x").unwrap();
        std::fs::create_dir_all(r.p("target/new-dir")).unwrap();
        std::fs::write(r.p("target/new-dir/x"), "x").unwrap();
        std::fs::write(r.p(".git/index"), "x").unwrap();
        std::fs::write(r.p("src/.lib2.rs.swp"), "x").unwrap();
        std::fs::write(r.p("src/4913"), "x").unwrap();
        std::fs::remove_file(r.p("src/4913")).unwrap();
        let b = r.quiet_batches().await;
        assert!(b.is_empty(), "{kind:?} noise: {b:?}");

        // a new subdirectory with a file written immediately: the file is reported
        std::fs::create_dir_all(r.p("src/sub")).unwrap();
        std::fs::write(r.p("src/sub/m.rs"), "fn m() {}").unwrap();
        let b = r.batches().await;
        one_batch(kind, "new dir", &b, &[r.p("src/sub/m.rs")]);

        // the new directory is watched: a later edit in it is reported
        std::fs::write(r.p("src/sub/m.rs"), "fn m() { 1; }").unwrap();
        let b = r.batches().await;
        one_batch(kind, "edit in new dir", &b, &[r.p("src/sub/m.rs")]);

        // a directory rename inside the project: the moved tree is still watched
        std::fs::rename(r.p("src/sub"), r.p("src/moved")).unwrap();
        let b = r.batches().await;
        one_batch(kind, "dir rename", &b, &[r.p("src/moved")]);
        std::fs::write(r.p("src/moved/m.rs"), "fn m() { 2; }").unwrap();
        let b = r.batches().await;
        one_batch(kind, "edit after dir rename", &b, &[r.p("src/moved/m.rs")]);
        assert!(
            !b[0].paths.iter().any(|p| p.starts_with(r.p("src/sub"))),
            "{kind:?}: stale path after the rename: {b:?}"
        );

        // 200 rapid writes: one or two batches
        for i in 0..200 {
            std::fs::write(r.p("src/lib2.rs"), format!("fn a() {{ {i}; }}")).unwrap();
        }
        let b = r.batches().await;
        assert!((1..=2).contains(&b.len()), "{kind:?} rapid writes: {b:?}");
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn watch_inotify_backend_scenarios() {
        scenarios(Kind::Native).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn watch_poll_backend_scenarios() {
        scenarios(Kind::Poll).await;
    }
}
