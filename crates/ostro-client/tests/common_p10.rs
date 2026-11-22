//! Spawned-binary helpers for the ostro-client tests (10-09): locate the `ostro` binary,
//! read its OpenAPI document, build guests, spawn `ostro dev --ephemeral` and parse its
//! frozen ready line (D-P10-9).
//!
//! Minimal copies of crates/ostro/tests/common helpers: ostro-client cannot dev-depend on
//! the binary crate, and `CARGO_BIN_EXE_ostro` is only set for the binary's own crate.
//! Included by the test targets with `mod common_p10;` (and compiled once on its own as an
//! empty test target).
#![allow(dead_code)]

use std::io::{BufRead, BufReader, Read};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

/// Repository root (`crates/ostro-client/../..`).
pub fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// The `ostro` binary: `OSTRO_BIN`, else the one in this test binary's own profile dir.
///
/// `cargo test --workspace` builds the `ostro` bin next to this run's artifacts: the
/// profile dir is `<target>/debug`, or `<target>/<triple>/debug` for a `--target` build
/// such as the musl CI legs (`cargo-zigbuild test --target ...`), and the test exe lives
/// somewhere below it (`deps/`, or `build/<pkg>/<hash>/out/` with the newer build-dir
/// layout). The nearest ancestor of the exe holding an `ostro` file is that dir.
/// Panics with the build command when it does not exist.
pub fn ostro_bin() -> PathBuf {
    let name = format!("ostro{}", std::env::consts::EXE_SUFFIX);
    let path = match std::env::var_os("OSTRO_BIN") {
        Some(p) => PathBuf::from(p),
        None => {
            let exe = std::env::current_exe().expect("current exe");
            exe.ancestors()
                .skip(1)
                .map(|d| d.join(&name))
                .find(|p| p.is_file())
                .unwrap_or_else(|| {
                    panic!(
                        "no ostro binary in any directory above {}: build it first with \
                         `cargo build -p ostro` (same CARGO_TARGET_DIR and --target) or point \
                         OSTRO_BIN at one",
                        exe.display()
                    )
                })
        }
    };
    assert!(
        path.is_file(),
        "the ostro binary {} does not exist: point OSTRO_BIN at a built one",
        path.display()
    );
    path
}

/// The binary as a `Command` with every inherited `OSTRO_*` variable and `RUST_LOG` removed.
pub fn command() -> Command {
    let mut cmd = Command::new(ostro_bin());
    for (k, _) in std::env::vars_os() {
        if k.to_string_lossy().starts_with("OSTRO_") {
            cmd.env_remove(&k);
        }
    }
    cmd.env_remove("RUST_LOG");
    cmd
}

/// `ostro openapi`: the server's OpenAPI document.
pub fn openapi_doc() -> serde_json::Value {
    let out = command()
        .arg("openapi")
        .output()
        .expect("run ostro openapi");
    assert!(
        out.status.success(),
        "ostro openapi failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).expect("ostro openapi prints JSON")
}

/// Build the `guests/` workspace once per process (release, wasm32-wasip2, into
/// `target/guests`, the directory the other suites share) and return the named guest.
pub fn guest_wasm(name: &str) -> Vec<u8> {
    static BUILT: OnceLock<()> = OnceLock::new();
    let root = repo_root();
    let target_dir = root.join("target/guests");
    BUILT.get_or_init(|| {
        let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
        let out = Command::new(cargo)
            .args([
                "build",
                "--release",
                "--target",
                "wasm32-wasip2",
                "--manifest-path",
            ])
            .arg(root.join("guests/Cargo.toml"))
            .arg("--target-dir")
            .arg(&target_dir)
            .env_remove("RUSTFLAGS")
            .env_remove("CARGO_ENCODED_RUSTFLAGS")
            .env_remove("CARGO_BUILD_TARGET")
            .env_remove("CARGO_TARGET_DIR")
            .env_remove("RUSTC_WRAPPER")
            .output()
            .expect("failed to spawn cargo for guests build");
        assert!(
            out.status.success(),
            "guests build failed:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
    });
    let path = target_dir
        .join("wasm32-wasip2/release")
        .join(format!("{}.wasm", name.replace('-', "_")));
    std::fs::read(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// The frozen ready line (D-P10-9):
/// `^(.+?) listening on http://(\S+) \(data: (.*?)(, ephemeral)?\) ready in (\d+) ms$`.
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
        let (data, rest) = rest.rsplit_once(") ready in ")?;
        let ms = rest.strip_suffix(" ms")?;
        if name.is_empty() || ms.is_empty() || !ms.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        let (data_dir, ephemeral) = match data.strip_suffix(", ephemeral") {
            Some(d) => (d, true),
            None => (data, false),
        };
        Some(Self {
            name: name.to_owned(),
            addr: addr.parse().ok()?,
            data_dir: PathBuf::from(data_dir),
            ephemeral,
            ms: ms.parse().ok()?,
        })
    }
}

/// A spawned `ostro dev --ephemeral`; killed on drop.
pub struct DevServer {
    child: Child,
    pub ready: ReadyLine,
    stderr: Arc<Mutex<String>>,
    _cwd: tempfile::TempDir,
}

impl DevServer {
    /// Spawn `ostro dev --ephemeral --listen 127.0.0.1:0 <extra>` in a fresh directory and
    /// wait (up to 60 s) for the ready line.
    pub fn spawn(extra: &[&str]) -> Self {
        let cwd = tempfile::tempdir().expect("cwd");
        let mut cmd = command();
        cmd.current_dir(cwd.path())
            .args(["dev", "--ephemeral", "--listen", "127.0.0.1:0"])
            .args(extra)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = cmd.spawn().expect("spawn ostro dev");
        let (tx, rx) = mpsc::channel();
        let out = child.stdout.take().expect("stdout");
        std::thread::spawn(move || {
            for line in BufReader::new(out).lines() {
                let Ok(line) = line else { break };
                if let Some(r) = ReadyLine::parse(&line) {
                    let _ = tx.send(r);
                }
            }
        });
        let stderr = Arc::new(Mutex::new(String::new()));
        let mut err = child.stderr.take().expect("stderr");
        let buf = Arc::clone(&stderr);
        std::thread::spawn(move || {
            let mut s = String::new();
            let _ = err.read_to_string(&mut s);
            buf.lock().unwrap_or_else(|e| e.into_inner()).push_str(&s);
        });
        let ready = match rx.recv_timeout(Duration::from_secs(60)) {
            Ok(r) => r,
            Err(e) => {
                let _ = child.kill();
                let _ = child.wait();
                std::thread::sleep(Duration::from_millis(100));
                panic!(
                    "no ready line from ostro dev within 60 s ({e}); stderr: {}",
                    stderr.lock().unwrap_or_else(|e| e.into_inner())
                );
            }
        };
        Self {
            child,
            ready,
            stderr,
            _cwd: cwd,
        }
    }

    /// `http://ADDR` of the server.
    pub fn url(&self) -> String {
        format!("http://{}", self.ready.addr)
    }

    /// Everything the server wrote to stderr so far.
    pub fn stderr(&self) -> String {
        self.stderr
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// The data directory from the ready line.
    pub fn data_dir(&self) -> &Path {
        &self.ready.data_dir
    }
}

impl Drop for DevServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn ready_line_parses_the_frozen_format() {
    let r = ReadyLine::parse(
        "ostro dev listening on http://127.0.0.1:41234 (data: /tmp/x y, ephemeral) ready in 12 ms",
    )
    .expect("parse");
    assert_eq!(r.name, "ostro dev");
    assert_eq!(r.addr.port(), 41234);
    assert_eq!(r.data_dir, PathBuf::from("/tmp/x y"));
    assert!(r.ephemeral);
    assert_eq!(r.ms, 12);
    assert!(
        ReadyLine::parse("ostro dev listening on http://127.0.0.1:1 (data: d) ready in x ms")
            .is_none()
    );
}
