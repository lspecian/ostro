//! Spawned-binary harness (plan 04-19): run the real `ostro` binary, parse its ready
//! line, poll `/readyz`, send signals, wait for exit.
//!
//! The binary is `CARGO_BIN_EXE_ostro` unless `OSTRO_BIN` names another one (e.g. a
//! release build, to record release startup numbers with the same tests).
#![allow(dead_code)]

use std::io::{BufRead, BufReader, Read};
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// One spawned server at a time per test process, so parallel test threads do not skew
/// the startup numbers (tokio mutex: guards are held across awaits).
pub static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Generous limit for anything that is not the asserted startup budget.
pub const T: Duration = Duration::from_secs(30);

/// The binary under test.
pub fn bin() -> PathBuf {
    std::env::var_os("OSTRO_BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_ostro")))
}

/// Startup budget: 1 s, overridable with `OSTRO_STARTUP_BUDGET_MS` on slow machines.
pub fn startup_budget() -> Duration {
    let ms = std::env::var("OSTRO_STARTUP_BUDGET_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(1000);
    Duration::from_millis(ms)
}

/// A currently free loopback port (bind :0, read, drop).
pub fn free_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind :0");
    l.local_addr().expect("local addr").port()
}

/// `127.0.0.1:<free port>`.
pub fn free_addr() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], free_port()))
}

/// The binary as a `Command`, with every inherited `OSTRO_*` variable removed (only
/// the ones a test sets apply) and `RUST_LOG` cleared (the profile level applies).
pub fn command() -> Command {
    let mut cmd = Command::new(bin());
    for (k, _) in std::env::vars_os() {
        if k.to_string_lossy().starts_with("OSTRO_") {
            cmd.env_remove(&k);
        }
    }
    cmd.env_remove("RUST_LOG");
    cmd
}

/// Run to completion (`--print-config`, failing starts): (status, stdout, stderr).
pub fn run_to_end(mut cmd: Command) -> (ExitStatus, String, String) {
    let out = cmd.output().expect("run ostro");
    (
        out.status,
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

/// The parsed ready line:
/// `NAME listening on http://ADDR (data: DIR[, ephemeral]) ready in N ms`.
#[derive(Debug, Clone)]
pub struct ReadyLine {
    pub name: String,
    pub addr: SocketAddr,
    pub data_dir: PathBuf,
    pub ephemeral: bool,
    pub ms: u64,
}

impl ReadyLine {
    pub fn parse(line: &str) -> Option<Self> {
        let (name, rest) = line.split_once(" listening on http://")?;
        let (addr, rest) = rest.split_once(" (data: ")?;
        let (data, rest) = rest.split_once(") ready in ")?;
        let ms = rest.strip_suffix(" ms")?.trim().parse().ok()?;
        let (data_dir, ephemeral) = match data.strip_suffix(", ephemeral") {
            Some(d) => (d, true),
            None => (data, false),
        };
        Some(Self {
            name: name.to_owned(),
            addr: addr.parse().ok()?,
            data_dir: PathBuf::from(data_dir),
            ephemeral,
            ms,
        })
    }
}

/// A spawned `ostro dev` / `ostro server`; killed on drop if still running.
pub struct Spawned {
    child: Child,
    pub spawned_at: Instant,
    ready_rx: mpsc::Receiver<ReadyLine>,
    stdout: Arc<Mutex<Vec<String>>>,
    stderr: Arc<Mutex<String>>,
}

impl Spawned {
    /// Spawn `cmd` (from [`command`]) with stdout/stderr drained by threads.
    pub fn spawn(mut cmd: Command) -> Self {
        cmd.stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let spawned_at = Instant::now();
        let mut child = cmd.spawn().expect("spawn ostro");
        let (tx, ready_rx) = mpsc::channel();
        let stdout = Arc::new(Mutex::new(Vec::new()));
        let stderr = Arc::new(Mutex::new(String::new()));
        let out = child.stdout.take().expect("stdout");
        let lines = Arc::clone(&stdout);
        std::thread::spawn(move || {
            for line in BufReader::new(out).lines() {
                let Ok(line) = line else { break };
                if let Some(r) = ReadyLine::parse(&line) {
                    let _ = tx.send(r);
                }
                lines.lock().unwrap_or_else(|e| e.into_inner()).push(line);
            }
        });
        let mut err = child.stderr.take().expect("stderr");
        let buf = Arc::clone(&stderr);
        std::thread::spawn(move || {
            let mut s = String::new();
            let _ = err.read_to_string(&mut s);
            buf.lock().unwrap_or_else(|e| e.into_inner()).push_str(&s);
        });
        Self {
            child,
            spawned_at,
            ready_rx,
            stdout,
            stderr,
        }
    }

    pub fn pid(&self) -> u32 {
        self.child.id()
    }

    /// Wait for the ready line on stdout.
    pub fn ready_line(&self, limit: Duration) -> ReadyLine {
        match self.ready_rx.recv_timeout(limit) {
            Ok(r) => r,
            Err(e) => panic!(
                "no ready line within {limit:?} ({e}); stderr: {}",
                self.stderr()
            ),
        }
    }

    /// Poll `GET http://addr/readyz` every 10 ms until 200; returns the time since spawn.
    pub async fn wait_readyz(&self, http: &reqwest::Client, addr: SocketAddr) -> Duration {
        let url = format!("http://{addr}/readyz");
        let deadline = self.spawned_at + T;
        loop {
            if let Ok(r) = http.get(&url).send().await
                && r.status() == reqwest::StatusCode::OK
            {
                return self.spawned_at.elapsed();
            }
            assert!(
                Instant::now() < deadline,
                "/readyz not 200 within {T:?}; stderr: {}",
                self.stderr()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Send a signal by name (`INT`, `TERM`) with `kill(1)`.
    pub fn signal(&self, sig: &str) {
        let st = Command::new("kill")
            .arg(format!("-{sig}"))
            .arg(self.pid().to_string())
            .status()
            .expect("run kill");
        assert!(st.success(), "kill -{sig} failed");
    }

    /// Wait for the process to exit (polling; panics after `limit`).
    pub fn wait_exit(&mut self, limit: Duration) -> ExitStatus {
        let deadline = Instant::now() + limit;
        loop {
            if let Some(st) = self.child.try_wait().expect("try_wait") {
                return st;
            }
            assert!(
                Instant::now() < deadline,
                "process did not exit within {limit:?}; stderr: {}",
                self.stderr()
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// Signal, wait, assert a clean exit.
    pub fn stop_clean(&mut self, sig: &str) {
        self.signal(sig);
        let st = self.wait_exit(T);
        assert!(
            st.success(),
            "exit after SIG{sig}: {st:?}; stderr: {}",
            self.stderr()
        );
    }

    pub fn stderr(&self) -> String {
        self.stderr
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub fn stdout_lines(&self) -> Vec<String> {
        self.stdout
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
}

impl Drop for Spawned {
    fn drop(&mut self) {
        if let Ok(None) = self.child.try_wait() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

/// HTTP client without proxies (loopback only).
pub fn http() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("reqwest client")
}

/// `ostro dev` with the given extra args, run from `cwd`.
pub fn dev(cwd: &Path, args: &[&str]) -> Command {
    let mut cmd = command();
    cmd.current_dir(cwd).arg("dev").args(args);
    cmd
}

/// `ostro server` with the given extra args, run from `cwd`.
pub fn server(cwd: &Path, args: &[&str]) -> Command {
    let mut cmd = command();
    cmd.current_dir(cwd).arg("server").args(args);
    cmd
}

/// min / median of a non-empty sample.
pub fn min_median(mut v: Vec<Duration>) -> (Duration, Duration) {
    v.sort();
    (v[0], v[v.len() / 2])
}
