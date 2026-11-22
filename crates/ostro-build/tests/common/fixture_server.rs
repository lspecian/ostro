//! Local fixture HTTP server for the toolchain download / extraction tests (10-02; used by
//! 10-07). Every archive is generated in code, deterministically (mtime 0, uid/gid 0, gzip
//! mtime 0), so two server instances publish identical bytes and shas (mirror tests).
//!
//! Include it from a test target with
//! `#[path = "common/fixture_server.rs"] mod fixture_server;`.
//!
//! Files (served at `<base_url>/<name>`; any directory prefix is ignored, so
//! `<base_url>/dist/v1/good.tar.gz` serves `good.tar.gz` too, for the mirror rewrite):
//!
//! | name | content |
//! |---|---|
//! | `good.tar.gz`, `good.tar.xz` | `tool-1.0/bin/tool` (0755 shell script), `tool-1.0/README` |
//! | `badsha.tar.gz` | a valid archive; its [`Shas`] entry is the sha of `good.tar.gz` (a wrong pin) |
//! | `traversal.tar.gz` | `../evil` |
//! | `abs.tar.gz` | `/tmp/evil` |
//! | `symlink-escape.tar.gz` | `bin/x -> ../../outside` |
//! | `symlink-chain.tar.gz` | `a -> b`, `b -> ../../outside` |
//! | `symlink-ok.tar.gz` | `lib/npm-cli.js`, `bin/npm -> ../lib/npm-cli.js` |
//! | `hardlink-escape.tar.gz` | `bin/passwd` hardlink to `../../etc/passwd` |
//! | `tool.bin` | a single executable script (ArchiveKind::Binary) |
//! | `tool.whl` | opaque bytes (wheels are verified, never extracted) |
//! | `big.bin` | [`BIG_BIN_BYTES`] bytes (larger than a test size cap) |
//! | `slow.bin` | headers plus one [`SLOW_FIRST_CHUNK`]-byte chunk, then stalls forever |
//!
//! Unknown names are 404 (and still counted).

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::io::Write as _;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{Path, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use sha2::{Digest, Sha256};

/// Top-level directory of the good archives (a pin's `strip` prefix).
pub const GOOD_PREFIX: &str = "tool-1.0";
/// The script inside the good archives and `tool.bin`.
pub const TOOL_SCRIPT: &[u8] = b"#!/bin/sh\necho ostro-fixture-tool\n";
/// Size of `big.bin`.
pub const BIG_BIN_BYTES: usize = 2 * 1024 * 1024;
/// Size of the one chunk `slow.bin` sends before it stalls.
pub const SLOW_FIRST_CHUNK: usize = 16;

/// Pinned sha256 (lower-case hex) per file name. `badsha.tar.gz` maps to the sha of
/// `good.tar.gz`, so a pin built from it never matches. `slow.bin` has no entry.
pub type Shas = BTreeMap<&'static str, String>;

/// Per-server request counters, keyed by file name.
#[derive(Debug, Default)]
pub struct RequestCounts {
    counts: Mutex<BTreeMap<String, u64>>,
}

impl RequestCounts {
    /// Requests for `name` so far.
    pub fn get(&self, name: &str) -> u64 {
        self.counts.lock().unwrap().get(name).copied().unwrap_or(0)
    }

    /// All requests so far.
    pub fn total(&self) -> u64 {
        self.counts.lock().unwrap().values().sum()
    }

    fn hit(&self, name: &str) {
        *self
            .counts
            .lock()
            .unwrap()
            .entry(name.to_string())
            .or_insert(0) += 1;
    }
}

/// Handle on a running fixture server (it runs until the test runtime shuts down).
pub struct FixtureServer {
    /// `http://127.0.0.1:<port>` (no trailing slash).
    pub base_url: String,
    /// Pinned shas (see [`Shas`]).
    pub shas: Shas,
    /// Request counters.
    pub counts: Arc<RequestCounts>,
    /// The bytes served per name (`slow.bin` excluded).
    pub files: Arc<BTreeMap<&'static str, Vec<u8>>>,
}

#[derive(Clone)]
struct AppState {
    files: Arc<BTreeMap<&'static str, Vec<u8>>>,
    counts: Arc<RequestCounts>,
}

impl FixtureServer {
    /// Starts a server on `127.0.0.1:0` and returns `(base_url, shas, request_counts)`.
    pub async fn start() -> (String, Shas, Arc<RequestCounts>) {
        let s = Self::spawn(false).await;
        (s.base_url, s.shas, s.counts)
    }

    /// Like [`FixtureServer::start`], with the full handle.
    pub async fn spawn_default() -> FixtureServer {
        Self::spawn(false).await
    }

    /// A server that publishes the same [`Shas`] as a normal one but serves every file with
    /// its last byte flipped (a mirror serving different bytes).
    pub async fn spawn_tampered() -> FixtureServer {
        Self::spawn(true).await
    }

    async fn spawn(tamper: bool) -> FixtureServer {
        let (mut files, shas) = generate();
        if tamper {
            for bytes in files.values_mut() {
                if let Some(last) = bytes.last_mut() {
                    *last ^= 0xff;
                }
            }
        }
        let files = Arc::new(files);
        let counts = Arc::new(RequestCounts::default());
        let state = AppState {
            files: files.clone(),
            counts: counts.clone(),
        };
        let app = Router::new()
            .route("/{*path}", get(serve))
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fixture server");
        let addr = listener.local_addr().expect("fixture server addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("fixture server");
        });
        FixtureServer {
            base_url: format!("http://{addr}"),
            shas,
            counts,
            files,
        }
    }

    /// `<base_url>/<name>`.
    pub fn url(&self, name: &str) -> String {
        format!("{}/{name}", self.base_url)
    }
}

async fn serve(State(state): State<AppState>, Path(path): Path<String>) -> Response {
    let name = path.rsplit('/').next().unwrap_or_default().to_string();
    state.counts.hit(&name);
    if name == "slow.bin" {
        let first = futures_util::stream::once(async {
            Ok::<_, std::io::Error>(Bytes::from(vec![b's'; SLOW_FIRST_CHUNK]))
        });
        let stalled = futures_util::stream::pending::<Result<Bytes, std::io::Error>>();
        let body = Body::from_stream(futures_util::StreamExt::chain(first, stalled));
        return ([(header::CONTENT_TYPE, "application/octet-stream")], body).into_response();
    }
    match state.files.get(name.as_str()) {
        Some(bytes) => (
            [(header::CONTENT_TYPE, "application/octet-stream")],
            bytes.clone(),
        )
            .into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

/// Lower-case hex sha256.
pub fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// One tar entry; paths and link targets are written raw (no validation), so unsafe
/// archives can be built.
pub enum Entry {
    /// A regular file with a mode.
    File(&'static str, &'static [u8], u32),
    /// A directory (mode 0755).
    Dir(&'static str),
    /// A symlink `path -> target`.
    Symlink(&'static str, &'static str),
    /// A hardlink `path` to `target`.
    Hardlink(&'static str, &'static str),
}

/// An uncompressed tar of `entries` (deterministic headers).
pub fn tar_bytes(entries: &[Entry]) -> Vec<u8> {
    let mut builder = tar::Builder::new(Vec::new());
    for entry in entries {
        let mut h = tar::Header::new_gnu();
        let (path, data, link): (&str, &[u8], Option<&str>) = match entry {
            Entry::File(p, d, mode) => {
                h.set_entry_type(tar::EntryType::Regular);
                h.set_mode(*mode);
                (p, d, None)
            }
            Entry::Dir(p) => {
                h.set_entry_type(tar::EntryType::Directory);
                h.set_mode(0o755);
                (p, &[], None)
            }
            Entry::Symlink(p, t) => {
                h.set_entry_type(tar::EntryType::Symlink);
                h.set_mode(0o777);
                (p, &[], Some(t))
            }
            Entry::Hardlink(p, t) => {
                h.set_entry_type(tar::EntryType::Link);
                h.set_mode(0o644);
                (p, &[], Some(t))
            }
        };
        let gnu = h.as_gnu_mut().expect("gnu header");
        assert!(path.len() < gnu.name.len(), "fixture path too long: {path}");
        gnu.name[..path.len()].copy_from_slice(path.as_bytes());
        if let Some(t) = link {
            assert!(t.len() < gnu.linkname.len(), "fixture link too long: {t}");
            gnu.linkname[..t.len()].copy_from_slice(t.as_bytes());
        }
        h.set_size(data.len() as u64);
        h.set_mtime(0);
        h.set_uid(0);
        h.set_gid(0);
        h.set_cksum();
        builder.append(&h, data).expect("append fixture entry");
    }
    builder.into_inner().expect("finish fixture tar")
}

/// gzip (mtime 0, best compression).
pub fn gz(bytes: &[u8]) -> Vec<u8> {
    let mut enc = flate2::GzBuilder::new()
        .mtime(0)
        .write(Vec::new(), flate2::Compression::best());
    enc.write_all(bytes).expect("gzip fixture");
    enc.finish().expect("finish gzip fixture")
}

/// xz (preset 6).
pub fn xz(bytes: &[u8]) -> Vec<u8> {
    let mut enc = lzma_rust2::XzWriter::new(Vec::new(), lzma_rust2::XzOptions::with_preset(6))
        .expect("xz writer");
    enc.write_all(bytes).expect("xz fixture");
    enc.finish().expect("finish xz fixture")
}

fn good_entries() -> Vec<Entry> {
    vec![
        Entry::Dir("tool-1.0/"),
        Entry::Dir("tool-1.0/bin/"),
        Entry::File("tool-1.0/bin/tool", TOOL_SCRIPT, 0o755),
        Entry::File("tool-1.0/README", b"ostro fixture tool\n", 0o644),
    ]
}

/// Generates every served file and the pinned shas.
pub fn generate() -> (BTreeMap<&'static str, Vec<u8>>, Shas) {
    let good = tar_bytes(&good_entries());
    let mut files: BTreeMap<&'static str, Vec<u8>> = BTreeMap::new();
    files.insert("good.tar.gz", gz(&good));
    files.insert("good.tar.xz", xz(&good));
    files.insert(
        "badsha.tar.gz",
        gz(&tar_bytes(&[Entry::File(
            "tool-1.0/bin/tool",
            b"#!/bin/sh\necho tampered\n",
            0o755,
        )])),
    );
    files.insert(
        "traversal.tar.gz",
        gz(&tar_bytes(&[Entry::File("../evil", b"evil\n", 0o644)])),
    );
    files.insert(
        "abs.tar.gz",
        gz(&tar_bytes(&[Entry::File("/tmp/evil", b"evil\n", 0o644)])),
    );
    files.insert(
        "symlink-escape.tar.gz",
        gz(&tar_bytes(&[
            Entry::Dir("bin/"),
            Entry::Symlink("bin/x", "../../outside"),
        ])),
    );
    files.insert(
        "symlink-chain.tar.gz",
        gz(&tar_bytes(&[
            Entry::Symlink("a", "b"),
            Entry::Symlink("b", "../../outside"),
        ])),
    );
    files.insert(
        "symlink-ok.tar.gz",
        gz(&tar_bytes(&[
            Entry::Dir("lib/"),
            Entry::File("lib/npm-cli.js", b"console.log('npm');\n", 0o644),
            Entry::Dir("bin/"),
            Entry::Symlink("bin/npm", "../lib/npm-cli.js"),
        ])),
    );
    files.insert(
        "hardlink-escape.tar.gz",
        gz(&tar_bytes(&[
            Entry::Dir("bin/"),
            Entry::Hardlink("bin/passwd", "../../etc/passwd"),
        ])),
    );
    files.insert("tool.bin", TOOL_SCRIPT.to_vec());
    files.insert("tool.whl", b"PK\x03\x04ostro-fixture-wheel".to_vec());
    files.insert(
        "big.bin",
        (0..BIG_BIN_BYTES).map(|i| (i % 251) as u8).collect(),
    );

    let mut shas: Shas = files
        .iter()
        .map(|(name, bytes)| (*name, sha256_hex(bytes)))
        .collect();
    let good_sha = shas["good.tar.gz"].clone();
    shas.insert("badsha.tar.gz", good_sha);
    (files, shas)
}
