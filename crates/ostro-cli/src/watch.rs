//! `ostro dev --watch [DIR]` (D-P10-14): rebuild and redeploy on change.
//!
//! Stub (10-02); 10-16 implements the loop: an in-process server plus build/deploy through
//! the deploy path, notify-debouncer-full (250 ms), the shared `pipeline::IGNORE` list,
//! serialised builds with one follow-up, the last good version kept on failure.

use std::path::PathBuf;

/// Options of one watch loop.
#[derive(Debug, Clone)]
pub struct WatchOptions {
    /// Project directory to watch and build.
    pub dir: PathBuf,
    /// Extra ignore patterns on top of the pipeline's built-in list.
    pub ignore: Vec<String>,
    /// Debounce window in milliseconds.
    pub debounce_ms: u64,
}

/// Runs the watch loop against `server` until interrupted.
pub async fn run_watch(opts: WatchOptions, server: String) -> Result<(), String> {
    let _ = (opts, server);
    Err("not yet implemented (10-16)".to_string())
}
