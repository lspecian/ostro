//! Phase 10 spawned-binary helpers (plan 10-03): the frozen ready-line parser (D-P10-9),
//! a spawned `ostro dev --ephemeral --listen 127.0.0.1:0`, and CLI runners with a
//! working directory (the local verbs `init` / `build` / `toolchain` / `doctor` /
//! `replay --corpus` act on the cwd).
//!
//! Standalone (it does not use `crate::common`), so a test includes only this file:
//!
//! ```ignore
//! #[path = "common/p10.rs"]
//! mod p10;
//! ```
//!
//! Every spawned process gets a scrubbed environment: inherited `OSTRO_*` variables and
//! `RUST_LOG` are removed, then the test's `env` pairs apply.
#![allow(dead_code)]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use serde_json::Value;

/// Hang guard for a spawned dev server to print its ready line.
pub const READY_TIMEOUT: Duration = Duration::from_secs(60);

/// The binary under test: `OSTRO_BIN` if set (e.g. a release build), else
/// `CARGO_BIN_EXE_ostro`.
pub fn ostro_bin() -> PathBuf {
    std::env::var_os("OSTRO_BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_ostro")))
}

/// `ostro` as a `Command` with inherited `OSTRO_*` and `RUST_LOG` removed and `env`
/// applied, stdin closed.
pub fn ostro_cmd(env: &[(&str, &str)]) -> Command {
    let mut cmd = Command::new(ostro_bin());
    for (k, _) in std::env::vars_os() {
        if k.to_string_lossy().starts_with("OSTRO_") {
            cmd.env_remove(&k);
        }
    }
    cmd.env_remove("RUST_LOG");
    for (k, v) in env {
        cmd.env(k, v);
    }
    cmd.stdin(Stdio::null());
    cmd
}

// ---------------------------------------------------------------------------------
// The frozen ready line (D-P10-9)
// ---------------------------------------------------------------------------------

/// A parsed ready line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ready {
    pub name: String,
    pub addr: String,
    pub data: String,
    pub ephemeral: bool,
    pub ms: u64,
}

/// The literal between the name and the address.
const LISTENING: &str = " listening on http://";

/// Parse the frozen ready line (no regex crate).
///
/// **The D-P10-9 regex as written does not match the binary's line.** It is
/// `^(\S+) listening on http://(\S+) \(data: (.*?)(, ephemeral)?\) ready in (\d+) ms$`,
/// but the frozen `{name}` is `ostro dev` / `ostro server` (crates/ostro/src/main.rs),
/// which contains a space. This parser is equivalent to the regex with the name group
/// widened to a lazy `(.+?)`, the line itself being the frozen contract:
///
/// `^(.+?) listening on http://(\S+) \(data: (.*?)(, ephemeral)?\) ready in (\d+) ms$`
///
/// - `name` is the shortest non-empty prefix (no newline) before a ` listening on
///   http://` after which the rest matches (later occurrences are tried in order);
/// - `addr` is a non-empty run of non-whitespace followed by ` (data: `;
/// - the tail is anchored at the end: ` ms`, a non-empty digit run, `) ready in `;
/// - what remains is `data`, minus a trailing `, ephemeral` (the lazy `.*?` prefers
///   the optional group), and contains no newline (`.`).
///
/// Differences: digits are ASCII only, and a value that overflows `u64` is rejected
/// (the regex would match it; no real start time does).
pub fn parse_ready_line(line: &str) -> Option<Ready> {
    let mut from = 0;
    while let Some(i) = line[from..].find(LISTENING) {
        let at = from + i;
        let name = &line[..at];
        if !name.is_empty()
            && !name.contains('\n')
            && let Some(r) = parse_after_name(name, &line[at + LISTENING.len()..])
        {
            return Some(r);
        }
        from = at + 1;
    }
    None
}

/// `(\S+) \(data: (.*?)(, ephemeral)?\) ready in (\d+) ms$` after the name.
fn parse_after_name(name: &str, rest: &str) -> Option<Ready> {
    // (\S+) \(data:
    let ws = rest.find(char::is_whitespace)?;
    let (addr, rest) = rest.split_at(ws);
    if addr.is_empty() {
        return None;
    }
    let rest = rest.strip_prefix(" (data: ")?;
    // \) ready in (\d+) ms$
    let rest = rest.strip_suffix(" ms")?;
    let digits_at = rest
        .char_indices()
        .rev()
        .take_while(|(_, c)| c.is_ascii_digit())
        .last()
        .map(|(i, _)| i)?;
    let (rest, digits) = rest.split_at(digits_at);
    let ms: u64 = digits.parse().ok()?;
    let middle = rest.strip_suffix(") ready in ")?;
    // (.*?)(, ephemeral)?
    if middle.contains('\n') {
        return None;
    }
    let (data, ephemeral) = match middle.strip_suffix(", ephemeral") {
        Some(d) => (d, true),
        None => (middle, false),
    };
    Some(Ready {
        name: name.to_owned(),
        addr: addr.to_owned(),
        data: data.to_owned(),
        ephemeral,
        ms,
    })
}

// ---------------------------------------------------------------------------------
// Spawned `ostro dev`
// ---------------------------------------------------------------------------------

/// A spawned `ostro dev --ephemeral --listen 127.0.0.1:0`, killed on drop.
pub struct DevProc {
    /// The bound address from the ready line.
    pub addr: SocketAddr,
    /// The data directory from the ready line.
    pub data: PathBuf,
    pub child: Child,
    /// `ready in N ms` from the ready line.
    pub ready_ms: u64,
    /// The whole ready line.
    pub ready: Ready,
    /// stderr, one line per message (drained by a thread until exit).
    pub stderr_rx: mpsc::Receiver<String>,
    /// stdout lines after the ready line.
    pub stdout_rx: mpsc::Receiver<String>,
    /// The process's working directory (removed on drop).
    pub cwd: tempfile::TempDir,
}

impl DevProc {
    /// `http://<addr>`.
    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// The stderr lines received so far.
    pub fn stderr_so_far(&self) -> String {
        drain(&self.stderr_rx)
    }

    /// Run `ostro <args>` against this server (`OSTRO_SERVER` set) in its cwd.
    pub fn cli(&self, args: &[&str]) -> (i32, String, String) {
        let server = self.url();
        run_cli_in(self.cwd.path(), args, &[("OSTRO_SERVER", &server)])
    }

    /// [`DevProc::cli`] with `--json` appended, stdout parsed.
    pub fn cli_json(&self, args: &[&str]) -> (i32, Value, String) {
        let server = self.url();
        cli_json_in(self.cwd.path(), args, &[("OSTRO_SERVER", &server)])
    }
}

impl Drop for DevProc {
    fn drop(&mut self) {
        if let Ok(None) = self.child.try_wait() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn drain(rx: &mpsc::Receiver<String>) -> String {
    let mut s = String::new();
    while let Ok(l) = rx.try_recv() {
        s.push_str(&l);
        s.push('\n');
    }
    s
}

/// Spawn `ostro dev --ephemeral --listen 127.0.0.1:0 <args>` (cwd: a fresh temp dir,
/// env scrubbed then `env` applied) and wait up to [`READY_TIMEOUT`] for the ready
/// line. Panics (after killing the child) with its stderr if the line never comes or
/// the process exits first.
pub fn spawn_dev(args: &[&str], env: &[(&str, &str)]) -> DevProc {
    spawn_dev_mode(true, args, env)
}

/// [`spawn_dev`] without `--ephemeral`: `ostro dev --listen 127.0.0.1:0 <args>`, data in
/// the default `./.ostro` of the fresh temp cwd (a persistent dev node, removed with the
/// cwd on drop).
pub fn spawn_dev_persistent(args: &[&str], env: &[(&str, &str)]) -> DevProc {
    spawn_dev_mode(false, args, env)
}

fn spawn_dev_mode(ephemeral: bool, args: &[&str], env: &[(&str, &str)]) -> DevProc {
    let cwd = tempfile::tempdir().expect("tempdir");
    let mut cmd = ostro_cmd(env);
    cmd.current_dir(cwd.path()).arg("dev");
    if ephemeral {
        cmd.arg("--ephemeral");
    }
    cmd.args(["--listen", "127.0.0.1:0"])
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = cmd.spawn().expect("spawn ostro dev");

    let (out_tx, out_rx) = mpsc::channel::<String>();
    let stdout = child.stdout.take().expect("stdout");
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            let Ok(line) = line else { break };
            if out_tx.send(line).is_err() {
                break;
            }
        }
    });
    let (err_tx, err_rx) = mpsc::channel::<String>();
    let stderr = child.stderr.take().expect("stderr");
    std::thread::spawn(move || {
        let mut r = BufReader::new(stderr);
        let mut buf = Vec::new();
        loop {
            buf.clear();
            match r.read_until(b'\n', &mut buf) {
                Ok(0) | Err(_) => break,
                Ok(_) => {
                    let line = String::from_utf8_lossy(&buf);
                    if err_tx.send(line.trim_end_matches('\n').to_owned()).is_err() {
                        break;
                    }
                }
            }
        }
    });

    let deadline = Instant::now() + READY_TIMEOUT;
    let mut seen = Vec::new();
    let ready = loop {
        let left = deadline.saturating_duration_since(Instant::now());
        match out_rx.recv_timeout(left.max(Duration::from_millis(1))) {
            Ok(line) => match parse_ready_line(&line) {
                Some(r) => break r,
                None => seen.push(line),
            },
            Err(e) => {
                let _ = child.kill();
                let st = child.wait();
                // give the stderr thread a moment to see EOF
                std::thread::sleep(Duration::from_millis(50));
                panic!(
                    "ostro dev {args:?}: no ready line within {READY_TIMEOUT:?} ({e}); \
                     exit {st:?}\nstdout:\n{}\nstderr:\n{}",
                    seen.join("\n"),
                    drain(&err_rx)
                );
            }
        }
    };
    let addr: SocketAddr = ready
        .addr
        .parse()
        .unwrap_or_else(|e| panic!("ready line address {:?}: {e}", ready.addr));
    DevProc {
        addr,
        data: PathBuf::from(&ready.data),
        child,
        ready_ms: ready.ms,
        ready,
        stderr_rx: err_rx,
        stdout_rx: out_rx,
        cwd,
    }
}

// ---------------------------------------------------------------------------------
// CLI runners
// ---------------------------------------------------------------------------------

/// Run `ostro <args>` in `cwd` (env scrubbed, then `env`): (exit code, stdout, stderr).
/// A signal death is exit code -1.
pub fn run_cli_in(cwd: &Path, args: &[&str], env: &[(&str, &str)]) -> (i32, String, String) {
    let out = ostro_cmd(env)
        .current_dir(cwd)
        .args(args)
        .output()
        .expect("run ostro");
    (
        out.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

/// [`run_cli_in`] with `--json` appended; stdout parsed as JSON (Null when empty).
pub fn cli_json_in(cwd: &Path, args: &[&str], env: &[(&str, &str)]) -> (i32, Value, String) {
    let mut v = args.to_vec();
    v.push("--json");
    let (code, out, err) = run_cli_in(cwd, &v, env);
    let body = if out.trim().is_empty() {
        Value::Null
    } else {
        serde_json::from_str(&out)
            .unwrap_or_else(|e| panic!("stdout of {v:?} is not JSON ({e}): {out}\n{err}"))
    };
    (code, body, err)
}

/// A minimal blocking HTTP/1.1 GET (`Connection: close`): (status, body). Enough for
/// health / listing checks without an async runtime.
pub fn http_get(addr: SocketAddr, path: &str) -> (u16, String) {
    let mut s = TcpStream::connect_timeout(&addr, Duration::from_secs(5)).expect("connect");
    s.set_read_timeout(Some(Duration::from_secs(30)))
        .expect("read timeout");
    write!(
        s,
        "GET {path} HTTP/1.1\r\nHost: {addr}\r\nAccept: application/json\r\nConnection: close\r\n\r\n"
    )
    .expect("write request");
    let mut raw = Vec::new();
    s.read_to_end(&mut raw).expect("read response");
    let text = String::from_utf8_lossy(&raw).into_owned();
    let status = text
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .unwrap_or_else(|| panic!("bad HTTP response: {text}"));
    let body = text
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_owned())
        .unwrap_or_default();
    (status, body)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ready_line_accepts() {
        let r = parse_ready_line(
            "ostro-dev listening on http://127.0.0.1:41234 (data: /tmp/.tmpAb, ephemeral) ready in 87 ms",
        )
        .unwrap();
        assert_eq!(
            r,
            Ready {
                name: "ostro-dev".into(),
                addr: "127.0.0.1:41234".into(),
                data: "/tmp/.tmpAb".into(),
                ephemeral: true,
                ms: 87,
            }
        );
        let r =
            parse_ready_line("ostro listening on http://[::1]:7700 (data: ./.ostro) ready in 0 ms")
                .unwrap();
        assert_eq!(r.addr, "[::1]:7700");
        assert_eq!(r.data, "./.ostro");
        assert!(!r.ephemeral);
        assert_eq!(r.ms, 0);
        // data may contain spaces, parentheses and the tail's own words
        let r = parse_ready_line(
            "n listening on http://a:1 (data: /x (y) ready in 3 ms, ephemeral, ephemeral) ready in 12 ms",
        )
        .unwrap();
        assert_eq!(r.data, "/x (y) ready in 3 ms, ephemeral");
        assert!(r.ephemeral);
        assert_eq!(r.ms, 12);
        // the names the binary prints (D-P10-9's `\S+` would reject them)
        let r = parse_ready_line(
            "ostro dev listening on http://127.0.0.1:40253 (data: /dev/shm/ostro-ECIARX, ephemeral) ready in 205 ms",
        )
        .unwrap();
        assert_eq!(r.name, "ostro dev");
        assert_eq!(r.addr, "127.0.0.1:40253");
        assert_eq!(r.data, "/dev/shm/ostro-ECIARX");
        assert!(r.ephemeral);
        assert_eq!(r.ms, 205);
        let r = parse_ready_line(
            "ostro server listening on http://0.0.0.0:7700 (data: /var/lib/ostro) ready in 1234 ms",
        )
        .unwrap();
        assert_eq!(r.name, "ostro server");
        assert!(!r.ephemeral);
        // the shortest name for which the rest matches: an earlier occurrence whose
        // address part fails is skipped
        let r = parse_ready_line(
            "a listening on http://x y listening on http://h:1 (data: d) ready in 2 ms",
        )
        .unwrap();
        assert_eq!(r.name, "a listening on http://x y");
        assert_eq!(r.addr, "h:1");
        // empty data (.*? may match nothing)
        let r = parse_ready_line("n listening on http://a:1 (data: , ephemeral) ready in 5 ms")
            .unwrap();
        assert_eq!(r.data, "");
        assert!(r.ephemeral);
    }

    #[test]
    fn ready_line_rejects() {
        for bad in [
            "",
            " listening on http://a:1 (data: d) ready in 1 ms",
            "\nn listening on http://a:1 (data: d) ready in 1 ms",
            "n listening on https://a:1 (data: d) ready in 1 ms",
            "n listening on http://a:1 (sha256 fingerprint ab) (data: d) ready in 1 ms",
            "n listening on http:// (data: d) ready in 1 ms",
            "n listening on http://a:1 (data: d) ready in ms",
            "n listening on http://a:1 (data: d) ready in 1ms",
            "n listening on http://a:1 (data: d) ready in 1 ms ",
            "n listening on http://a:1 (data: d) ready in -1 ms",
            "n listening on http://a:1 (data: d ready in 1 ms",
            "n listening on http://a:1 (data: d)\nready in 1 ms",
            "n listening on http://a:1 (data: d\n) ready in 1 ms",
            "n listening on http://a:1 (data: d) ready in 1 ms\n",
            "server ready",
        ] {
            assert_eq!(parse_ready_line(bad), None, "{bad:?}");
        }
    }
}
