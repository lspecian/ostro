//! The spawned, failpoint-enabled `ostro dev` server (plan 05-19).
//!
//! - [`failpoint_bin`] builds `cargo build --release -p ostro --features failpoints
//!   --target-dir target/failpoints --locked` once per process (or uses `OSTRO_FAILPOINT_BIN`)
//!   and asserts that `build-info` ends in `failpoints=on`.
//! - [`crash_saga_wasm`] builds crash-saga once, path-neutrally (`scripts/corpus-guest-build.sh`,
//!   `target/guests-corpus`).
//! - [`ServerTarget::spawn`] runs `ostro dev --data-dir <dir> --listen 127.0.0.1:0` with
//!   `FAILPOINTS` set (or removed), parses the ready line for the bound address and waits
//!   for `/readyz`. stdout/stderr are drained by threads into a ring buffer, and lines that
//!   report an injected fault are counted ([`ServerTarget::fault_lines`]).
//! - [`ServerTarget::kill9`] sends SIGKILL and confirms with `waitpid` that the process died
//!   of signal 9 (only then does the campaign count a kill).

use std::collections::VecDeque;
use std::io::{BufRead, BufReader};
use std::net::SocketAddr;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, mpsc};
use std::time::{Duration, Instant};

/// Repository root (two levels above this crate).
pub fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("repo root")
        .to_path_buf()
}

fn cargo() -> std::ffi::OsString {
    std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into())
}

/// The failpoint-enabled release binary (built once per process).
pub fn failpoint_bin() -> PathBuf {
    static BIN: OnceLock<PathBuf> = OnceLock::new();
    BIN.get_or_init(|| {
        let bin = match std::env::var_os("OSTRO_FAILPOINT_BIN") {
            Some(b) => PathBuf::from(b),
            None => {
                let root = repo_root();
                let out = Command::new(cargo())
                    .current_dir(&root)
                    .args([
                        "build",
                        "--release",
                        "-p",
                        "ostro",
                        "--features",
                        "failpoints",
                        "--target-dir",
                        "target/failpoints",
                        "--locked",
                    ])
                    .env_remove("CARGO_TARGET_DIR")
                    .output()
                    .expect("spawn cargo build (failpoint binary)");
                assert!(
                    out.status.success(),
                    "failpoint binary build failed:\n{}",
                    String::from_utf8_lossy(&out.stderr)
                );
                root.join("target/failpoints/release/ostro")
            }
        };
        let info = Command::new(&bin)
            .arg("build-info")
            .env_remove("FAILPOINTS")
            .output()
            .unwrap_or_else(|e| panic!("run {} build-info: {e}", bin.display()));
        let info = String::from_utf8_lossy(&info.stdout);
        assert!(
            info.contains("failpoints=on"),
            "{} is not a failpoint build: {info}",
            bin.display()
        );
        bin
    })
    .clone()
}

/// The crash-saga component, built once per process into `target/guests-corpus` by
/// `scripts/corpus-guest-build.sh` (path-neutral: `--remap-path-prefix` for the repository,
/// cargo home, rustup home and target dir). Runs exported from a campaign data dir become
/// replay-corpus entries, so the deployed module must not embed local build paths. Its own
/// target dir keeps these flags from invalidating the plain `target/guests` build.
pub fn crash_saga_wasm() -> Vec<u8> {
    static WASM: OnceLock<Vec<u8>> = OnceLock::new();
    WASM.get_or_init(|| {
        let root = repo_root();
        let target = root.join("target/guests-corpus");
        let out = Command::new("bash")
            .arg(root.join("scripts/corpus-guest-build.sh"))
            .arg(root.join("guests/Cargo.toml"))
            .arg(&target)
            .args(["-p", "crash-saga"])
            .env("CARGO", cargo())
            .output()
            .expect("spawn scripts/corpus-guest-build.sh (guests)");
        assert!(
            out.status.success(),
            "guests build failed:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
        let p = target.join("wasm32-wasip2/release/crash_saga.wasm");
        std::fs::read(&p).unwrap_or_else(|e| panic!("read {}: {e}", p.display()))
    })
    .clone()
}

/// Lowercase hex sha256.
pub fn sha256_hex(b: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let d: [u8; 32] = Sha256::digest(b).into();
    d.iter().map(|x| format!("{x:02x}")).collect()
}

/// A log line that reports an injected fault (EIO / ENOSPC text of the failpoint sites).
pub fn is_fault_line(line: &str) -> bool {
    line.contains("(injected)")
        || line.contains("injected failure")
        || line.contains("os error 5)")
        || line.contains("os error 28)")
        || line.contains("Input/output error")
        || line.contains("No space left on device")
}

const RING: usize = 400;

/// Outcome of [`ServerTarget::kill9`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KillOutcome {
    /// waitpid reported death by SIGKILL.
    Killed,
    /// The process had already exited on its own (not a counted kill).
    AlreadyExited,
}

/// One spawned `ostro dev` process.
pub struct ServerTarget {
    child: Option<Child>,
    pub addr: SocketAddr,
    pub base: String,
    pub ready_at: Instant,
    pub failpoints: Option<String>,
    log: Arc<Mutex<VecDeque<String>>>,
    faults: Arc<AtomicU64>,
}

impl ServerTarget {
    /// Spawn on `data_dir` and wait for the ready line, then (clean servers only) until
    /// `/readyz` is 200. `limit` elapsing or the process exiting returns `Err` with the log
    /// tail (the process is then gone). An armed server (`failpoints` set) only needs its
    /// ready line: an injected fault may legitimately fail a partition during recovery,
    /// which keeps `/readyz` at 503 until the next (clean) restart.
    pub async fn spawn(
        bin: &Path,
        data_dir: &Path,
        failpoints: Option<&str>,
        http: &reqwest::Client,
        limit: Duration,
    ) -> Result<Self, String> {
        Self::spawn_env(bin, data_dir, failpoints, &[], http, limit).await
    }

    /// [`Self::spawn`] with extra environment variables, set after the inherited `OSTRO_*`
    /// and `RUST_LOG` are stripped: `OSTRO_<SECTION>__<KEY>` config overrides such as the
    /// Phase 8 campaign's tiny snapshot / backup / blob GC settings (plan 08-20).
    pub async fn spawn_env(
        bin: &Path,
        data_dir: &Path,
        failpoints: Option<&str>,
        env: &[(String, String)],
        http: &reqwest::Client,
        limit: Duration,
    ) -> Result<Self, String> {
        std::fs::create_dir_all(data_dir).map_err(|e| format!("mkdir data dir: {e}"))?;
        let mut cmd = Command::new(bin);
        for (k, _) in std::env::vars_os() {
            if k.to_string_lossy().starts_with("OSTRO_") {
                cmd.env_remove(&k);
            }
        }
        cmd.env_remove("RUST_LOG");
        for (k, v) in env {
            cmd.env(k, v);
        }
        match failpoints {
            Some(fp) => cmd.env("FAILPOINTS", fp),
            None => cmd.env_remove("FAILPOINTS"),
        };
        cmd.arg("dev")
            .arg("--data-dir")
            .arg(data_dir)
            .arg("--listen")
            .arg("127.0.0.1:0")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = cmd
            .spawn()
            .map_err(|e| format!("spawn {}: {e}", bin.display()))?;
        let log = Arc::new(Mutex::new(VecDeque::with_capacity(RING)));
        let faults = Arc::new(AtomicU64::new(0));
        let (tx, rx) = mpsc::channel::<SocketAddr>();
        let drain = |r: Box<dyn std::io::Read + Send>, tx: Option<mpsc::Sender<SocketAddr>>| {
            let log = Arc::clone(&log);
            let faults = Arc::clone(&faults);
            std::thread::spawn(move || {
                for line in BufReader::new(r).lines() {
                    let Ok(line) = line else { break };
                    if let Some(tx) = &tx
                        && let Some(a) = parse_ready(&line)
                    {
                        let _ = tx.send(a);
                    }
                    if is_fault_line(&line) {
                        faults.fetch_add(1, Ordering::Relaxed);
                    }
                    let mut g = log.lock().unwrap_or_else(|e| e.into_inner());
                    if g.len() == RING {
                        g.pop_front();
                    }
                    g.push_back(line);
                }
            });
        };
        drain(Box::new(child.stdout.take().expect("stdout")), Some(tx));
        drain(Box::new(child.stderr.take().expect("stderr")), None);
        let deadline = Instant::now() + limit;
        let addr = loop {
            match rx.try_recv() {
                Ok(a) => break a,
                Err(mpsc::TryRecvError::Disconnected) | Err(mpsc::TryRecvError::Empty) => {}
            }
            if let Ok(Some(st)) = child.try_wait() {
                tokio::time::sleep(Duration::from_millis(50)).await;
                return Err(format!(
                    "server exited before ready ({st}); log tail:\n{}",
                    tail_of(&log, 30)
                ));
            }
            if Instant::now() > deadline {
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!(
                    "no ready line within {limit:?}; log tail:\n{}",
                    tail_of(&log, 30)
                ));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        let base = format!("http://{addr}");
        let mut t = ServerTarget {
            child: Some(child),
            addr,
            base: base.clone(),
            ready_at: Instant::now(),
            failpoints: failpoints.map(str::to_owned),
            log,
            faults,
        };
        while failpoints.is_none() {
            if let Ok(r) = http.get(format!("{base}/readyz")).send().await
                && r.status() == reqwest::StatusCode::OK
            {
                break;
            }
            if let Some(c) = t.child.as_mut()
                && let Ok(Some(st)) = c.try_wait()
            {
                return Err(format!(
                    "server exited before /readyz ({st}); log tail:\n{}",
                    t.tail(30)
                ));
            }
            if Instant::now() > deadline {
                let _ = t.kill9().await;
                return Err(format!(
                    "/readyz not 200 within {limit:?}; log tail:\n{}",
                    t.tail(30)
                ));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        t.ready_at = Instant::now();
        Ok(t)
    }

    pub fn pid(&self) -> Option<u32> {
        self.child.as_ref().map(Child::id)
    }

    /// Lines reporting an injected fault so far.
    pub fn fault_lines(&self) -> u64 {
        self.faults.load(Ordering::Relaxed)
    }

    /// Has the process exited on its own?
    pub fn exited(&mut self) -> bool {
        self.child
            .as_mut()
            .is_none_or(|c| matches!(c.try_wait(), Ok(Some(_))))
    }

    /// The last `n` log lines.
    pub fn tail(&self, n: usize) -> String {
        tail_of(&self.log, n)
    }

    /// SIGKILL + waitpid. `Killed` only when the wait status says signal 9.
    pub async fn kill9(&mut self) -> Result<KillOutcome, String> {
        let Some(mut child) = self.child.take() else {
            return Ok(KillOutcome::AlreadyExited);
        };
        if let Ok(Some(_)) = child.try_wait() {
            return Ok(KillOutcome::AlreadyExited);
        }
        let _ = child.kill();
        let st = tokio::task::spawn_blocking(move || child.wait())
            .await
            .map_err(|e| format!("wait task: {e}"))?
            .map_err(|e| format!("waitpid: {e}"))?;
        match st.signal() {
            Some(9) => Ok(KillOutcome::Killed),
            _ if st.code().is_some() => Ok(KillOutcome::AlreadyExited),
            other => Err(format!("unexpected wait status {st} (signal {other:?})")),
        }
    }
}

impl Drop for ServerTarget {
    fn drop(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

fn tail_of(log: &Mutex<VecDeque<String>>, n: usize) -> String {
    let g = log.lock().unwrap_or_else(|e| e.into_inner());
    let skip = g.len().saturating_sub(n);
    g.iter().skip(skip).cloned().collect::<Vec<_>>().join("\n")
}

/// `NAME listening on http://ADDR (data: ...) ready in N ms` -> ADDR.
pub fn parse_ready(line: &str) -> Option<SocketAddr> {
    let (_, rest) = line.split_once(" listening on http://")?;
    let (addr, rest) = rest.split_once(" (data: ")?;
    rest.contains(") ready in ").then_some(())?;
    addr.parse().ok()
}

/// Kill every `ostro dev` process left behind by an earlier (killed) harness on
/// `data_dir`: any process whose command line holds `--data-dir <data_dir>`. Returns the
/// killed pids. The server takes no exclusive lock on its data dir, so a resumed shard
/// must never start next to an orphan.
pub fn kill_stale(data_dir: &Path) -> Vec<u32> {
    let mut needle = b"--data-dir\0".to_vec();
    needle.extend_from_slice(data_dir.as_os_str().as_encoded_bytes());
    needle.push(0);
    let mut killed = Vec::new();
    let Ok(rd) = std::fs::read_dir("/proc") else {
        return killed;
    };
    for e in rd.flatten() {
        let Some(pid) = e.file_name().to_str().and_then(|s| s.parse::<u32>().ok()) else {
            continue;
        };
        let Ok(cmdline) = std::fs::read(format!("/proc/{pid}/cmdline")) else {
            continue;
        };
        if !cmdline
            .windows(needle.len())
            .any(|w| w == needle.as_slice())
        {
            continue;
        }
        if Command::new("kill")
            .arg("-9")
            .arg(pid.to_string())
            .status()
            .is_ok_and(|s| s.success())
        {
            for _ in 0..100 {
                if !Path::new(&format!("/proc/{pid}")).exists() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            killed.push(pid);
        }
    }
    killed
}

/// `PUT /v1/modules/{sha}` of crash-saga (the first version auto-activates).
pub async fn deploy_crash_saga(http: &reqwest::Client, base: &str) -> Result<(), String> {
    let wasm = crash_saga_wasm();
    let sha = sha256_hex(&wasm);
    let r = http
        .put(format!("{base}/v1/modules/{sha}"))
        .header("content-type", "application/wasm")
        .body(wasm)
        .timeout(Duration::from_secs(120))
        .send()
        .await
        .map_err(|e| format!("PUT module: {e}"))?;
    if !r.status().is_success() {
        let st = r.status();
        return Err(format!(
            "PUT module: {st} {}",
            r.text().await.unwrap_or_default()
        ));
    }
    Ok(())
}
