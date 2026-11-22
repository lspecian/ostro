//! The source set of a project and the staging directory (`target/ostro/stage/`).
//!
//! [`source_files`] is the one definition of "the sources" (hashed by
//! [`crate::pipeline::input_hash`], copied by [`stage`]):
//! - Rust: `Cargo.toml`, `Cargo.lock`, `build.rs`, `rust-toolchain.toml`,
//!   `rust-toolchain`, `src/**`;
//! - Python: `**/*.py` except under the top-level `tests/`, plus `pyproject.toml`,
//!   `uv.lock`, `requirements*.txt`;
//! - TypeScript: `src/**`, `package.json`, `package-lock.json`, `tsconfig.json`;
//! - always `ostro.toml` and the `[project] entry` when it names a file (Python: also
//!   `<entry>.py`).
//!
//! Paths matching [`crate::pipeline::IGNORE`] are never included. Symlinked directories are
//! not followed; symlinked files are read through.
//!
//! Owner: 10-12.

use std::fs;
use std::io;
use std::path::Path;
use std::time::{Duration, SystemTime};

use crate::pipeline::{BuildError, HostEnv, ignored_dir, ignored_file, is_ignored};
use crate::project::{Lang, MANIFEST_FILE, Project};

/// The project's source files as sorted, `/`-separated paths relative to the root.
pub fn source_files(project: &Project) -> io::Result<Vec<String>> {
    let root = &project.root;
    let lang = project.manifest.project.lang;
    let mut out = std::collections::BTreeSet::new();
    let top = |name: &str, out: &mut std::collections::BTreeSet<String>| {
        if root.join(name).is_file() && !is_ignored(Path::new(name)) {
            out.insert(name.to_owned());
        }
    };
    top(MANIFEST_FILE, &mut out);
    match lang {
        Lang::Rust => {
            for f in [
                "Cargo.toml",
                "Cargo.lock",
                "build.rs",
                "rust-toolchain.toml",
                "rust-toolchain",
            ] {
                top(f, &mut out);
            }
            walk(root, "src", &mut |_| true, &mut out)?;
        }
        Lang::Python => {
            for f in ["pyproject.toml", "uv.lock"] {
                top(f, &mut out);
            }
            for e in read_dir_sorted(root)? {
                let name = e.0;
                if e.1.is_file()
                    && name.starts_with("requirements")
                    && name.ends_with(".txt")
                    && !ignored_file(&name)
                {
                    out.insert(name);
                }
            }
            walk(
                root,
                "",
                &mut |rel: &str| rel.ends_with(".py") && !rel.starts_with("tests/"),
                &mut out,
            )?;
        }
        Lang::Ts => {
            for f in ["package.json", "package-lock.json", "tsconfig.json"] {
                top(f, &mut out);
            }
            walk(root, "src", &mut |_| true, &mut out)?;
        }
    }
    let entry = project.manifest.project.entry().to_owned();
    if !entry.is_empty() && !entry.contains("..") && !Path::new(&entry).is_absolute() {
        top(&entry, &mut out);
        if lang == Lang::Python {
            top(&format!("{entry}.py"), &mut out);
        }
    }
    Ok(out.into_iter().collect())
}

/// `(name, file type following symlinks)` of the entries of `dir`, sorted by name.
fn read_dir_sorted(dir: &Path) -> io::Result<Vec<(String, fs::Metadata)>> {
    let mut v = Vec::new();
    for e in fs::read_dir(dir)? {
        let e = e?;
        let name = e.file_name().to_string_lossy().into_owned();
        let ft = e.file_type()?;
        if ft.is_symlink() {
            // Read file symlinks through; never follow directory symlinks.
            match fs::metadata(e.path()) {
                Ok(m) if m.is_file() => v.push((name, m)),
                _ => {}
            }
        } else {
            v.push((name, e.metadata()?));
        }
    }
    v.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(v)
}

/// Add every file under `root/rel` (recursively) whose relative path passes `keep`.
fn walk(
    root: &Path,
    rel: &str,
    keep: &mut dyn FnMut(&str) -> bool,
    out: &mut std::collections::BTreeSet<String>,
) -> io::Result<()> {
    let dir = if rel.is_empty() {
        root.to_path_buf()
    } else {
        root.join(rel)
    };
    if !dir.is_dir() {
        return Ok(());
    }
    for (name, meta) in read_dir_sorted(&dir)? {
        let child = if rel.is_empty() {
            name.clone()
        } else {
            format!("{rel}/{name}")
        };
        if meta.is_dir() {
            if !ignored_dir(&name) {
                walk(root, &child, keep, out)?;
            }
        } else if meta.is_file() && !ignored_file(&name) && keep(&child) {
            out.insert(child);
        }
    }
    Ok(())
}

/// `SOURCE_DATE_EPOCH`: the host variable when set (and an integer), else the commit time
/// of `HEAD` when the project is in a git work tree (`git log -1 --format=%ct`), else 0.
pub fn source_date_epoch(project: &Project, host: HostEnv) -> u64 {
    if let Some(v) = host("SOURCE_DATE_EPOCH").and_then(|v| v.trim().parse::<u64>().ok()) {
        return v;
    }
    let out = std::process::Command::new("git")
        .arg("-C")
        .arg(&project.root)
        .args(["log", "-1", "--format=%ct"])
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .stdin(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .output();
    match out {
        Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout)
            .trim()
            .parse()
            .unwrap_or(0),
        _ => 0,
    }
}

/// Recreate `dest` and copy the project's [`source_files`] into it; every staged file and
/// directory gets mtime = `source_date_epoch`.
pub fn stage(project: &Project, dest: &Path, source_date_epoch: u64) -> Result<(), BuildError> {
    let io = |what: &str, p: &Path, e: io::Error| {
        BuildError::io(format!("staging: {what} {}", p.display()), e)
    };
    match fs::remove_dir_all(dest) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(io("removing", dest, e)),
    }
    fs::create_dir_all(dest).map_err(|e| io("creating", dest, e))?;
    let files = source_files(project).map_err(|e| io("listing", &project.root, e))?;
    let mtime = SystemTime::UNIX_EPOCH + Duration::from_secs(source_date_epoch);
    let mut dirs = std::collections::BTreeSet::new();
    for rel in &files {
        let to = dest.join(rel);
        if let Some(parent) = to.parent() {
            fs::create_dir_all(parent).map_err(|e| io("creating", parent, e))?;
            let mut p = Path::new(rel.as_str()).parent();
            while let Some(d) = p.filter(|d| !d.as_os_str().is_empty()) {
                dirs.insert(d.to_path_buf());
                p = d.parent();
            }
        }
        let from = project.root.join(rel);
        fs::copy(&from, &to).map_err(|e| io("copying", &from, e))?;
        set_mtime(&to, mtime).map_err(|e| io("setting the mtime of", &to, e))?;
    }
    // Deepest directories first (BTreeSet order reversed: children sort after parents).
    for d in dirs.iter().rev() {
        let p = dest.join(d);
        set_mtime(&p, mtime).map_err(|e| io("setting the mtime of", &p, e))?;
    }
    set_mtime(dest, mtime).map_err(|e| io("setting the mtime of", dest, e))?;
    Ok(())
}

fn set_mtime(path: &Path, t: SystemTime) -> io::Result<()> {
    let f = if path.is_dir() {
        fs::File::open(path)?
    } else {
        fs::OpenOptions::new().write(true).open(path)?
    };
    f.set_modified(t)
}
