//! Linux backend of the `ostro dev --watch` watcher (D-P14-47): inotify over `libc`.
//!
//! - `inotify_init1(IN_NONBLOCK | IN_CLOEXEC)`, read through `tokio::io::unix::AsyncFd`;
//! - one watch per non-ignored directory under the root (walked at start). A directory
//!   created or moved into the tree gets watches for its whole subtree, and its files are
//!   reported as changed (closes the create-then-write race). A directory moved away
//!   loses its watches (a move inside the tree re-adds them under the new name);
//! - mask [`MASK`]; `IN_Q_OVERFLOW` rescans the tree (adding any missing watch) and
//!   emits one [`Event::Rescan`]; `IN_IGNORED` drops the watch mapping;
//! - a directory's own metadata change (`IN_ATTRIB | IN_ISDIR`) is not a source change;
//! - the event buffer is parsed with explicit bounds (16-byte `inotify_event` header +
//!   `len` name bytes), never by casting past the read length;
//! - the inotify fd is an `OwnedFd`: dropping the watcher aborts its task, which closes it.

use std::collections::{BTreeMap, HashMap};
use std::ffi::{CString, OsStr};
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};

use tokio::io::unix::AsyncFd;
use tokio::sync::mpsc::UnboundedSender;

use super::Filter;
use super::debounce::Event;

/// The events every directory watch asks for.
pub const MASK: u32 = libc::IN_CLOSE_WRITE
    | libc::IN_CREATE
    | libc::IN_DELETE
    | libc::IN_MOVED_FROM
    | libc::IN_MOVED_TO
    | libc::IN_DELETE_SELF
    | libc::IN_MOVE_SELF
    | libc::IN_ATTRIB;

/// Size of the fixed `struct inotify_event` header (wd, mask, cookie, len).
const HEADER: usize = 16;

/// One parsed `inotify_event`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RawEvent {
    pub wd: i32,
    pub mask: u32,
    pub name: Vec<u8>,
}

/// Parse a buffer returned by `read(2)` on an inotify fd. A truncated trailing record
/// (which the kernel never produces) is dropped, never read past `buf.len()`.
pub(crate) fn parse(buf: &[u8]) -> Vec<RawEvent> {
    let mut out = Vec::new();
    let mut off = 0usize;
    while let Some(hdr) = buf.get(off..off + HEADER) {
        let word = |i: usize| [hdr[i], hdr[i + 1], hdr[i + 2], hdr[i + 3]];
        let wd = i32::from_ne_bytes(word(0));
        let mask = u32::from_ne_bytes(word(4));
        let len = u32::from_ne_bytes(word(12)) as usize;
        let Some(name) = off
            .checked_add(HEADER)
            .and_then(|s| Some(s..s.checked_add(len)?))
            .and_then(|r| buf.get(r))
        else {
            break;
        };
        // the name is NUL-padded to an alignment boundary
        let end = name.iter().position(|&b| b == 0).unwrap_or(name.len());
        out.push(RawEvent {
            wd,
            mask,
            name: name[..end].to_vec(),
        });
        off += HEADER + len;
    }
    out
}

/// The watch state: the inotify fd and the watch-descriptor <-> directory maps.
pub(crate) struct State {
    fd: OwnedFd,
    root: PathBuf,
    filter: Filter,
    by_wd: HashMap<i32, PathBuf>,
    by_path: BTreeMap<PathBuf, i32>,
}

/// `true` for the errors that mean "inotify limits reached" (the caller falls back to
/// polling): `EMFILE` (max_user_instances / fds) and `ENOSPC` (max_user_watches).
pub(crate) fn is_limit(e: &io::Error) -> bool {
    matches!(e.raw_os_error(), Some(libc::EMFILE) | Some(libc::ENOSPC))
}

impl State {
    /// A new inotify instance with watches on `root` and every directory under it that
    /// `filter` accepts. Any `add_watch` failure on the initial walk is returned.
    pub(crate) fn new(root: &Path, filter: Filter) -> io::Result<State> {
        // SAFETY: inotify_init1 takes only flags and returns a new fd or -1.
        let raw = unsafe { libc::inotify_init1(libc::IN_NONBLOCK | libc::IN_CLOEXEC) };
        if raw < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: `raw` is a freshly created fd that nothing else owns.
        let fd = unsafe { OwnedFd::from_raw_fd(raw) };
        let mut st = State {
            fd,
            root: root.to_path_buf(),
            filter,
            by_wd: HashMap::new(),
            by_path: BTreeMap::new(),
        };
        st.add(root)?;
        let mut files = Vec::new();
        st.add_tree(root, &mut files, true)?;
        Ok(st)
    }

    /// Add (or refresh) the watch of one directory.
    fn add(&mut self, dir: &Path) -> io::Result<()> {
        let c = CString::new(dir.as_os_str().as_bytes())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        // SAFETY: the fd is a live inotify fd owned by `self`; `c` is a valid NUL-terminated
        // path that outlives the call.
        let wd = unsafe {
            libc::inotify_add_watch(
                self.fd.as_raw_fd(),
                c.as_ptr(),
                MASK | libc::IN_ONLYDIR | libc::IN_DONT_FOLLOW,
            )
        };
        if wd < 0 {
            return Err(io::Error::last_os_error());
        }
        // the same inode keeps its wd: a re-add after a rename updates the path
        if let Some(old) = self.by_wd.insert(wd, dir.to_path_buf())
            && old != dir
        {
            self.by_path.remove(&old);
        }
        self.by_path.insert(dir.to_path_buf(), wd);
        Ok(())
    }

    /// Watch every accepted directory below `dir` (not `dir` itself) and collect the
    /// accepted files. With `strict`, the first `add_watch` error is returned; otherwise
    /// errors are printed and the walk goes on.
    fn add_tree(&mut self, dir: &Path, files: &mut Vec<PathBuf>, strict: bool) -> io::Result<()> {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return Ok(());
        };
        for e in entries.flatten() {
            let p = e.path();
            // `file_type` does not follow symlinks: symlinked dirs are not watched.
            let Ok(t) = e.file_type() else { continue };
            if !(self.filter)(&p) {
                continue;
            }
            if t.is_dir() {
                match self.add(&p) {
                    Ok(()) => {}
                    Err(err) if strict => return Err(err),
                    Err(err) => {
                        eprintln!("ostro dev: cannot watch {}: {err}", p.display());
                        continue;
                    }
                }
                self.add_tree(&p, files, strict)?;
            } else {
                files.push(p);
            }
        }
        Ok(())
    }

    /// Drop the watches of `dir` and every directory below it.
    fn remove_tree(&mut self, dir: &Path) {
        let gone: Vec<(PathBuf, i32)> = self
            .by_path
            .range(dir.to_path_buf()..)
            .take_while(|(p, _)| p.starts_with(dir))
            .map(|(p, wd)| (p.clone(), *wd))
            .collect();
        for (p, wd) in gone {
            self.by_path.remove(&p);
            self.by_wd.remove(&wd);
            // SAFETY: the fd is a live inotify fd owned by `self`; a stale wd only makes
            // the call fail with EINVAL, which is ignored.
            unsafe {
                libc::inotify_rm_watch(self.fd.as_raw_fd(), wd);
            }
        }
    }

    /// Handle one read buffer; returns the events to report.
    pub(crate) fn handle(&mut self, buf: &[u8]) -> Vec<Event> {
        let mut out = Vec::new();
        for ev in parse(buf) {
            if ev.mask & libc::IN_Q_OVERFLOW != 0 {
                let root = self.root.clone();
                let mut files = Vec::new();
                let _ = self.add_tree(&root, &mut files, false);
                out.push(Event::Rescan);
                continue;
            }
            if ev.mask & libc::IN_IGNORED != 0 {
                if let Some(p) = self.by_wd.remove(&ev.wd)
                    && self.by_path.get(&p) == Some(&ev.wd)
                {
                    self.by_path.remove(&p);
                }
                continue;
            }
            let Some(dir) = self.by_wd.get(&ev.wd).cloned() else {
                continue;
            };
            if ev.name.is_empty() {
                // DELETE_SELF / MOVE_SELF / ATTRIB of the watched directory itself: the
                // parent's watch reports the entry; IN_IGNORED follows a delete.
                continue;
            }
            let path = dir.join(OsStr::from_bytes(&ev.name));
            if !(self.filter)(&path) {
                continue;
            }
            let is_dir = ev.mask & libc::IN_ISDIR != 0;
            if is_dir {
                if ev.mask & libc::IN_ATTRIB != 0 {
                    // a directory's own metadata change is not a source change
                    continue;
                }
                if ev.mask & (libc::IN_CREATE | libc::IN_MOVED_TO) != 0 {
                    match self.add(&path) {
                        Ok(()) => {
                            let mut files = Vec::new();
                            let _ = self.add_tree(&path, &mut files, false);
                            out.extend(files.into_iter().map(Event::Changed));
                        }
                        Err(e) => eprintln!("ostro dev: cannot watch {}: {e}", path.display()),
                    }
                }
                if ev.mask & libc::IN_MOVED_FROM != 0 {
                    self.remove_tree(&path);
                }
            }
            out.push(Event::Changed(path));
        }
        out
    }

    /// The number of watched directories (tests).
    #[cfg(test)]
    pub(crate) fn watch_count(&self) -> usize {
        self.by_wd.len()
    }
}

/// A running inotify watcher; dropping it stops the reader task and closes the fd.
pub struct Watcher {
    task: tokio::task::JoinHandle<()>,
}

impl Watcher {
    /// Start watching `root`; events go to `tx`. Errors are setup failures ([`is_limit`]
    /// tells the caller to fall back to polling).
    pub(crate) fn start(
        root: &Path,
        filter: Filter,
        tx: UnboundedSender<Event>,
    ) -> io::Result<Self> {
        let st = State::new(root, filter)?;
        Self::spawn(st, tx)
    }

    /// Run the reader task over an existing state.
    pub(crate) fn spawn(st: State, tx: UnboundedSender<Event>) -> io::Result<Self> {
        // SAFETY: `State::as_raw_fd` always returns the fd of its private `OwnedFd`, which
        // is never replaced and stays open until the `State` (owned by the AsyncFd) drops.
        let afd = unsafe { AsyncFd::register(st) }?;
        let task = tokio::spawn(async move {
            let mut afd = afd;
            let mut buf = vec![0u8; 64 * 1024];
            loop {
                let Ok(mut guard) = afd.readable_mut().await else {
                    return;
                };
                let res = guard.try_io(|inner| {
                    let fd = inner.get_ref().fd.as_raw_fd();
                    // SAFETY: `buf` is a live, writable allocation of `buf.len()` bytes; the
                    // kernel writes at most that many.
                    let n = unsafe { libc::read(fd, buf.as_mut_ptr().cast(), buf.len()) };
                    if n < 0 {
                        Err(io::Error::last_os_error())
                    } else {
                        Ok(n as usize)
                    }
                });
                match res {
                    Ok(Ok(n)) => {
                        let events = guard.get_inner_mut().handle(&buf[..n]);
                        for ev in events {
                            if tx.send(ev).is_err() {
                                return;
                            }
                        }
                    }
                    Ok(Err(e)) => {
                        eprintln!("ostro dev: watch error: {e}");
                        return;
                    }
                    Err(_would_block) => continue,
                }
            }
        });
        Ok(Watcher { task })
    }
}

impl AsRawFd for State {
    fn as_raw_fd(&self) -> std::os::fd::RawFd {
        self.fd.as_raw_fd()
    }
}

impl Drop for Watcher {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(wd: i32, mask: u32, name: &[u8], pad: usize) -> Vec<u8> {
        let len = name.len() + pad;
        let mut v = Vec::new();
        v.extend_from_slice(&wd.to_ne_bytes());
        v.extend_from_slice(&mask.to_ne_bytes());
        v.extend_from_slice(&0u32.to_ne_bytes());
        v.extend_from_slice(&(len as u32).to_ne_bytes());
        v.extend_from_slice(name);
        v.extend(std::iter::repeat_n(0u8, pad));
        v
    }

    #[test]
    fn the_parser_respects_the_read_length() {
        let mut buf = record(1, libc::IN_CREATE, b"a.rs", 12);
        buf.extend(record(2, libc::IN_DELETE, b"", 0));
        buf.extend(record(3, libc::IN_CLOSE_WRITE, b"longer-name.rs", 2));
        let all = parse(&buf);
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].name, b"a.rs");
        assert_eq!(all[1].wd, 2);
        assert!(all[1].name.is_empty());
        assert_eq!(all[2].name, b"longer-name.rs");
        // every truncation: never a panic, only complete records
        for cut in 0..buf.len() {
            let got = parse(&buf[..cut]);
            assert!(got.len() <= 3);
            assert_eq!(got[..], all[..got.len()]);
        }
        // a header whose len points past the end
        let mut bad = record(1, libc::IN_CREATE, b"x", 0);
        bad[12..16].copy_from_slice(&u32::MAX.to_ne_bytes());
        assert!(parse(&bad).is_empty());
    }

    #[test]
    fn overflow_rescans_and_emits_one_rescan_event() {
        let d = tempfile::tempdir().unwrap();
        let root = d.path().canonicalize().unwrap();
        std::fs::create_dir_all(root.join("src")).unwrap();
        let f: Filter = std::sync::Arc::new(|_| true);
        let mut st = State::new(&root, f).unwrap();
        assert_eq!(st.watch_count(), 2);
        // a directory that appeared while the queue overflowed
        std::fs::create_dir_all(root.join("src/new")).unwrap();
        let buf = record(-1, libc::IN_Q_OVERFLOW, b"", 0);
        assert_eq!(st.handle(&buf), vec![Event::Rescan]);
        assert_eq!(st.watch_count(), 3, "the rescan watches the new directory");
        // IN_IGNORED drops the mapping
        let wd = *st.by_path.get(&root.join("src/new")).unwrap();
        assert!(st.handle(&record(wd, libc::IN_IGNORED, b"", 0)).is_empty());
        assert_eq!(st.watch_count(), 2);
        // a directory's own metadata change is not reported
        let wd = *st.by_path.get(&root.join("src")).unwrap();
        let attr = record(wd, libc::IN_ATTRIB | libc::IN_ISDIR, b"new", 5);
        assert!(st.handle(&attr).is_empty());
    }
}
