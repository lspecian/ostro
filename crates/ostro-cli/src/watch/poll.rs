//! Polling backend of the `ostro dev --watch` watcher (D-P14-47): the default off Linux,
//! the fallback on Linux when inotify limits are reached.
//!
//! Every [`INTERVAL`] a blocking thread snapshots `path -> (mtime, size, file id)` of the
//! accepted tree (ignored directories are never walked) and reports created, removed and
//! changed paths. Directories count only when they appear or disappear: a directory's
//! own mtime moves whenever an entry (even an ignored one) is created in it.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime};

use tokio::sync::mpsc::UnboundedSender;

use super::Filter;
use super::debounce::Event;

/// The polling interval.
pub const INTERVAL: Duration = Duration::from_millis(300);

/// What a snapshot remembers of one path.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Entry {
    Dir,
    File {
        mtime: Option<SystemTime>,
        size: u64,
        id: u64,
    },
}

/// A file id: the inode on Unix (a replaced file is a change even with equal mtime and
/// size); 0 elsewhere (mtime + size only).
fn file_id(m: &std::fs::Metadata) -> u64 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        m.ino()
    }
    #[cfg(not(unix))]
    {
        let _ = m;
        0
    }
}

/// Snapshot of the accepted tree under `root` (the root itself excluded).
fn snapshot(root: &Path, filter: &Filter) -> BTreeMap<PathBuf, Entry> {
    let mut out = BTreeMap::new();
    walk(root, filter, &mut out);
    out
}

fn walk(dir: &Path, filter: &Filter, out: &mut BTreeMap<PathBuf, Entry>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for e in entries.flatten() {
        let p = e.path();
        if !filter(&p) {
            continue;
        }
        // symlink_metadata: symlinked dirs are not walked (as the inotify backend)
        let Ok(m) = std::fs::symlink_metadata(&p) else {
            continue;
        };
        if m.is_dir() {
            out.insert(p.clone(), Entry::Dir);
            walk(&p, filter, out);
        } else {
            out.insert(
                p,
                Entry::File {
                    mtime: m.modified().ok(),
                    size: m.len(),
                    id: file_id(&m),
                },
            );
        }
    }
}

/// The paths that differ between two snapshots.
fn diff(old: &BTreeMap<PathBuf, Entry>, new: &BTreeMap<PathBuf, Entry>) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for (p, e) in new {
        if old.get(p) != Some(e) {
            out.push(p.clone());
        }
    }
    for p in old.keys() {
        if !new.contains_key(p) {
            out.push(p.clone());
        }
    }
    out
}

/// A running polling watcher; dropping it stops the thread (within one interval).
pub struct Watcher {
    stop: Arc<AtomicBool>,
}

impl Watcher {
    /// Start polling `root` every `interval`; events go to `tx`.
    pub(crate) fn start(
        root: &Path,
        filter: Filter,
        tx: UnboundedSender<Event>,
        interval: Duration,
    ) -> std::io::Result<Self> {
        let stop = Arc::new(AtomicBool::new(false));
        let (root, flag) = (root.to_path_buf(), stop.clone());
        let mut prev = snapshot(&root, &filter);
        std::thread::Builder::new()
            .name("ostro-dev-poll".to_owned())
            .spawn(move || {
                while !flag.load(Ordering::Relaxed) {
                    std::thread::sleep(interval);
                    if flag.load(Ordering::Relaxed) {
                        return;
                    }
                    let next = snapshot(&root, &filter);
                    for p in diff(&prev, &next) {
                        if tx.send(Event::Changed(p)).is_err() {
                            return;
                        }
                    }
                    prev = next;
                }
            })?;
        Ok(Watcher { stop })
    }
}

impl Drop for Watcher {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshots_skip_rejected_dirs_and_diff_by_content() {
        let d = tempfile::tempdir().unwrap();
        let root = d.path().to_path_buf();
        std::fs::create_dir_all(root.join("src")).unwrap();
        std::fs::create_dir_all(root.join("target/deep")).unwrap();
        std::fs::write(root.join("src/a.rs"), "a").unwrap();
        std::fs::write(root.join("target/deep/x"), "x").unwrap();
        let f: Filter = Arc::new(|p: &Path| !p.ends_with("target"));
        let s1 = snapshot(&root, &f);
        assert!(s1.contains_key(&root.join("src/a.rs")));
        assert!(!s1.keys().any(|p| p.starts_with(root.join("target"))));
        std::fs::write(root.join("src/a.rs"), "longer").unwrap();
        std::fs::write(root.join("src/b.rs"), "b").unwrap();
        let s2 = snapshot(&root, &f);
        let mut changed = diff(&s1, &s2);
        changed.sort();
        assert_eq!(changed, vec![root.join("src/a.rs"), root.join("src/b.rs")]);
        std::fs::remove_file(root.join("src/b.rs")).unwrap();
        assert_eq!(diff(&s2, &snapshot(&root, &f)), vec![root.join("src/b.rs")]);
    }
}
