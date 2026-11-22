//! Bundles the SDK sources into `$OUT_DIR/sdk_bundle.tar` (D-P10-6, plan 10-08), embedded
//! by `src/sdk_bundle.rs` and materialised by `ostro init` / `ostro build` into
//! `<project>/target/ostro/sdk/`.
//!
//! The tar is byte-reproducible: entries are sorted by path, every header has mtime 0,
//! uid/gid 0, empty user/group names and mode 0644 (0755 when the source file is
//! executable), GNU format, regular files only. Excluded everywhere: `target/`,
//! `node_modules/`, `__pycache__/`, `tests/`, `*.pyc` and hidden files.
//!
//! Bundle layout (tar path ← repo path):
//! - `rust/ostro-sdk/{Cargo.toml,src/**,wit/runtime.wit}` ← `sdks/rust/ostro-sdk`
//! - `rust/ostro-sdk-macros/{Cargo.toml,src/**}` ← `sdks/rust/ostro-sdk-macros`
//! - `rust/ostro-client/{Cargo.toml,src/**}` ← `crates/ostro-client` (manifest rewritten:
//!   no `[lints]`, no `[dev-dependencies]`, `publish = false`; any `workspace = true`
//!   fails the build)
//! - `python/ostro/**` ← `sdks/python/ostro`
//! - `ts/{package.json,rolldown.ostro.mjs,src/**,wit/runtime.wit}` ← `sdks/ts`
//! - `toolchain/{package.json,package-lock.json,rolldown.ostro.mjs}` ←
//!   `sdks/scripts/toolchain` (byte-identical, D-P10-3)
//! - `clients/python/ostro_client/**`, `clients/ts/{package.json,src/**}` ← `clients/…`
//!   (the Phase 10 test kits; included once 10-04 has created them)

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

/// Directory names never bundled.
const EXCLUDED_DIRS: [&str; 4] = ["target", "node_modules", "__pycache__", "tests"];

/// One bundled file.
struct Entry {
    bytes: Vec<u8>,
    exec: bool,
}

struct Bundle {
    root: PathBuf,
    entries: BTreeMap<String, Entry>,
}

impl Bundle {
    /// Add one file (must exist) as `tar_path`.
    fn file(&mut self, repo_rel: &str, tar_path: &str) {
        let path = self.root.join(repo_rel);
        let bytes = fs::read(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        println!("cargo:rerun-if-changed={}", path.display());
        let exec = is_exec(&path);
        self.insert(tar_path.to_owned(), Entry { bytes, exec });
    }

    /// Add a directory tree (must exist) under `tar_prefix`.
    fn tree(&mut self, repo_rel: &str, tar_prefix: &str) {
        let dir = self.root.join(repo_rel);
        assert!(dir.is_dir(), "bundle source {} is missing", dir.display());
        self.walk(&dir, tar_prefix);
    }

    /// Add a directory tree if it exists (the client kits arrive with plan 10-04).
    fn optional_tree(&mut self, repo_rel: &str, tar_prefix: &str) {
        if self.root.join(repo_rel).is_dir() {
            self.tree(repo_rel, tar_prefix);
        }
    }

    fn optional_file(&mut self, repo_rel: &str, tar_path: &str) {
        if self.root.join(repo_rel).is_file() {
            self.file(repo_rel, tar_path);
        }
    }

    fn walk(&mut self, dir: &Path, tar_prefix: &str) {
        println!("cargo:rerun-if-changed={}", dir.display());
        let mut names: Vec<(String, PathBuf)> = fs::read_dir(dir)
            .unwrap_or_else(|e| panic!("read_dir {}: {e}", dir.display()))
            .map(|e| {
                let e = e.unwrap_or_else(|err| panic!("read_dir {}: {err}", dir.display()));
                let name = e
                    .file_name()
                    .into_string()
                    .unwrap_or_else(|n| panic!("non-UTF-8 file name {n:?} in {}", dir.display()));
                (name, e.path())
            })
            .collect();
        names.sort();
        for (name, path) in names {
            if name.starts_with('.') {
                continue;
            }
            let ft = fs::symlink_metadata(&path)
                .unwrap_or_else(|e| panic!("stat {}: {e}", path.display()))
                .file_type();
            let tar_path = format!("{tar_prefix}/{name}");
            if ft.is_dir() {
                if !EXCLUDED_DIRS.contains(&name.as_str()) {
                    self.walk(&path, &tar_path);
                }
            } else if ft.is_file() {
                if name.ends_with(".pyc") {
                    continue;
                }
                let bytes =
                    fs::read(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
                println!("cargo:rerun-if-changed={}", path.display());
                let exec = is_exec(&path);
                self.insert(tar_path, Entry { bytes, exec });
            } else {
                panic!("bundle source {} is not a regular file", path.display());
            }
        }
    }

    fn insert(&mut self, tar_path: String, entry: Entry) {
        let prev = self.entries.insert(tar_path.clone(), entry);
        assert!(prev.is_none(), "duplicate bundle path {tar_path}");
    }
}

#[cfg(unix)]
fn is_exec(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    fs::metadata(path)
        .map(|m| m.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
}

#[cfg(not(unix))]
fn is_exec(_path: &Path) -> bool {
    false
}

/// `crates/ostro-client/Cargo.toml` as a standalone (bundled) manifest.
fn client_manifest(src: &str) -> Vec<u8> {
    let mut t: toml::Table = toml::from_str(src).expect("parse crates/ostro-client/Cargo.toml");
    t.remove("lints");
    t.remove("dev-dependencies");
    let pkg = t
        .get_mut("package")
        .and_then(toml::Value::as_table_mut)
        .expect("ostro-client [package]");
    pkg.insert("publish".to_owned(), toml::Value::Boolean(false));
    if let Some(path) = find_workspace_true(&toml::Value::Table(t.clone()), "") {
        panic!(
            "crates/ostro-client/Cargo.toml is not bundleable: `{path}` uses `workspace = true` \
             (write the version explicitly)"
        );
    }
    let body = toml::to_string(&t).expect("serialize ostro-client manifest");
    format!(
        "# Bundled copy of crates/ostro-client/Cargo.toml (rewritten by ostro-build/build.rs).\n{body}"
    )
    .into_bytes()
}

fn find_workspace_true(v: &toml::Value, at: &str) -> Option<String> {
    let t = v.as_table()?;
    for (k, v) in t {
        let here = if at.is_empty() {
            k.clone()
        } else {
            format!("{at}.{k}")
        };
        if k == "workspace" && v.as_bool() == Some(true) {
            return Some(here);
        }
        if let Some(found) = find_workspace_true(v, &here) {
            return Some(found);
        }
    }
    None
}

fn main() {
    let manifest_dir = PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").expect("manifest"));
    let root = manifest_dir.join("../..");
    let out = PathBuf::from(std::env::var_os("OUT_DIR").expect("OUT_DIR is set"));
    println!("cargo:rerun-if-changed=build.rs");

    let mut b = Bundle {
        root: root.clone(),
        entries: BTreeMap::new(),
    };

    // Rust SDK (its WIT copy is vendored, D-P9-29).
    b.file(
        "sdks/rust/ostro-sdk/Cargo.toml",
        "rust/ostro-sdk/Cargo.toml",
    );
    b.tree("sdks/rust/ostro-sdk/src", "rust/ostro-sdk/src");
    b.file(
        "sdks/rust/ostro-sdk/wit/runtime.wit",
        "rust/ostro-sdk/wit/runtime.wit",
    );
    b.file(
        "sdks/rust/ostro-sdk-macros/Cargo.toml",
        "rust/ostro-sdk-macros/Cargo.toml",
    );
    b.tree(
        "sdks/rust/ostro-sdk-macros/src",
        "rust/ostro-sdk-macros/src",
    );

    // Rust client + test kit (manifest made standalone).
    let client_toml_path = root.join("crates/ostro-client/Cargo.toml");
    println!("cargo:rerun-if-changed={}", client_toml_path.display());
    let client_toml = fs::read_to_string(&client_toml_path).expect("read ostro-client manifest");
    b.insert(
        "rust/ostro-client/Cargo.toml".to_owned(),
        Entry {
            bytes: client_manifest(&client_toml),
            exec: false,
        },
    );
    b.tree("crates/ostro-client/src", "rust/ostro-client/src");

    // Python SDK.
    b.tree("sdks/python/ostro", "python/ostro");

    // TypeScript SDK.
    b.file("sdks/ts/package.json", "ts/package.json");
    b.file("sdks/ts/rolldown.ostro.mjs", "ts/rolldown.ostro.mjs");
    b.tree("sdks/ts/src", "ts/src");
    b.file("sdks/ts/wit/runtime.wit", "ts/wit/runtime.wit");

    // Pinned npm toolchain (jco, rolldown), byte-identical (D-P10-3).
    for f in ["package.json", "package-lock.json", "rolldown.ostro.mjs"] {
        b.file(
            &format!("sdks/scripts/toolchain/{f}"),
            &format!("toolchain/{f}"),
        );
    }

    // Python / TS test kits (plan 10-04 creates them; 10-17 fills them in). A path that
    // does not exist cannot be watched without rerunning this script on every build, so an
    // incremental build made before they appeared keeps a bundle without them;
    // tests/sdk_bundle.rs detects that (`touch crates/ostro-build/build.rs` fixes it).
    b.optional_tree("clients/python/ostro_client", "clients/python/ostro_client");
    b.optional_file("clients/ts/package.json", "clients/ts/package.json");
    b.optional_tree("clients/ts/src", "clients/ts/src");

    let mut builder = tar::Builder::new(Vec::new());
    builder.mode(tar::HeaderMode::Deterministic);
    for (path, e) in &b.entries {
        let mut h = tar::Header::new_gnu();
        h.set_entry_type(tar::EntryType::Regular);
        h.set_size(e.bytes.len() as u64);
        h.set_mode(if e.exec { 0o755 } else { 0o644 });
        h.set_mtime(0);
        h.set_uid(0);
        h.set_gid(0);
        h.set_username("").expect("username");
        h.set_groupname("").expect("groupname");
        builder
            .append_data(&mut h, path, e.bytes.as_slice())
            .unwrap_or_else(|err| panic!("tar append {path}: {err}"));
    }
    builder.finish().expect("finish sdk_bundle.tar");
    let bytes = builder.into_inner().expect("sdk_bundle.tar bytes");
    assert!(
        bytes.len() < 1_048_576,
        "sdk_bundle.tar is {} bytes (limit 1 MiB)",
        bytes.len()
    );
    let sha = Sha256::digest(&bytes);
    let hex: String = sha.iter().map(|b| format!("{b:02x}")).collect();
    fs::write(out.join("sdk_bundle.tar"), &bytes).expect("write sdk_bundle.tar");
    fs::write(out.join("sdk_bundle.sha256"), format!("{hex}\n")).expect("write sha256");
}
