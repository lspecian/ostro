//! 10-08: the embedded SDK bundle is deterministic, small and byte-faithful to the repo
//! (SDKs, frozen WIT, toolchain lock), and materialises idempotently.

use std::collections::BTreeMap;
use std::io::Read;
use std::path::{Path, PathBuf};

use ostro_build::sdk_bundle::{self, BUNDLE, BUNDLE_ID_FILE};

fn repo() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

struct Ent {
    path: String,
    mode: u32,
    mtime: u64,
    uid: u64,
    gid: u64,
    user: Vec<u8>,
    group: Vec<u8>,
    regular: bool,
    bytes: Vec<u8>,
}

fn entries() -> Vec<Ent> {
    let mut a = tar::Archive::new(BUNDLE);
    let mut out = Vec::new();
    for e in a.entries().expect("entries") {
        let mut e = e.expect("entry");
        let h = e.header().clone();
        let mut bytes = Vec::new();
        e.read_to_end(&mut bytes).expect("read");
        out.push(Ent {
            path: e.path().unwrap().to_string_lossy().into_owned(),
            mode: h.mode().unwrap(),
            mtime: h.mtime().unwrap(),
            uid: h.uid().unwrap(),
            gid: h.gid().unwrap(),
            user: h.username_bytes().unwrap_or_default().to_vec(),
            group: h.groupname_bytes().unwrap_or_default().to_vec(),
            regular: h.entry_type() == tar::EntryType::Regular,
            bytes,
        });
    }
    out
}

fn by_path() -> BTreeMap<String, Vec<u8>> {
    entries().into_iter().map(|e| (e.path, e.bytes)).collect()
}

#[test]
fn bundle_lists_expected_sources_and_no_junk() {
    let m = by_path();
    for p in [
        "rust/ostro-sdk/Cargo.toml",
        "rust/ostro-sdk/src/lib.rs",
        "rust/ostro-sdk/wit/runtime.wit",
        "rust/ostro-sdk-macros/Cargo.toml",
        "rust/ostro-sdk-macros/src/lib.rs",
        "rust/ostro-client/Cargo.toml",
        "rust/ostro-client/src/lib.rs",
        "python/ostro/__init__.py",
        "python/ostro/py.typed",
        "python/ostro/wit/runtime.wit",
        "ts/package.json",
        "ts/src/index.ts",
        "ts/wit/runtime.wit",
        "ts/rolldown.ostro.mjs",
        "toolchain/package.json",
        "toolchain/package-lock.json",
        "toolchain/rolldown.ostro.mjs",
    ] {
        assert!(m.contains_key(p), "bundle lacks {p}: {:?}", m.keys());
    }
    for p in m.keys() {
        for seg in p.split('/') {
            assert!(
                !matches!(
                    seg,
                    "target" | "node_modules" | "__pycache__" | "tests" | "test"
                ),
                "excluded segment in {p}"
            );
            assert!(!seg.starts_with('.'), "hidden file {p}");
        }
        assert!(!p.ends_with(".pyc"), "{p}");
        let top = p.split('/').next().unwrap();
        assert!(
            matches!(top, "rust" | "python" | "ts" | "toolchain" | "clients"),
            "unexpected top-level entry {p}"
        );
    }
    // every source file of the bundled trees is present (nothing silently dropped)
    let mut expected = Vec::new();
    walk(
        &repo().join("sdks/rust/ostro-sdk/src"),
        "rust/ostro-sdk/src",
        &mut expected,
    );
    walk(
        &repo().join("sdks/python/ostro"),
        "python/ostro",
        &mut expected,
    );
    walk(&repo().join("sdks/ts/src"), "ts/src", &mut expected);
    walk(
        &repo().join("crates/ostro-client/src"),
        "rust/ostro-client/src",
        &mut expected,
    );
    for (tar_path, file) in expected {
        let got = m
            .get(&tar_path)
            .unwrap_or_else(|| panic!("bundle lacks {tar_path}"));
        assert_eq!(
            got,
            &std::fs::read(&file).unwrap(),
            "{tar_path} differs from the repo"
        );
    }
}

/// The Python/TS kits (10-04) are bundled once they exist in the repo.
#[test]
fn bundle_has_client_kits_when_present() {
    let m = by_path();
    let mut expected = Vec::new();
    let py = repo().join("clients/python/ostro_client");
    if py.is_dir() {
        walk(&py, "clients/python/ostro_client", &mut expected);
    }
    let ts = repo().join("clients/ts/src");
    if ts.is_dir() {
        walk(&ts, "clients/ts/src", &mut expected);
    }
    let ts_pkg = repo().join("clients/ts/package.json");
    if ts_pkg.is_file() {
        expected.push(("clients/ts/package.json".to_owned(), ts_pkg));
    }
    for (tar_path, file) in expected {
        let got = m.get(&tar_path).unwrap_or_else(|| {
            panic!(
                "bundle lacks {tar_path}: the build script ran before the client kits existed \
                 (touch crates/ostro-build/build.rs)"
            )
        });
        assert_eq!(got, &std::fs::read(&file).unwrap(), "{tar_path}");
    }
}

fn walk(dir: &Path, prefix: &str, out: &mut Vec<(String, PathBuf)>) {
    let mut names: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap())
        .collect();
    names.sort_by_key(|e| e.file_name());
    for e in names {
        let name = e.file_name().into_string().unwrap();
        if name.starts_with('.') || name.ends_with(".pyc") {
            continue;
        }
        let p = e.path();
        let tp = format!("{prefix}/{name}");
        if p.is_dir() {
            if !matches!(
                name.as_str(),
                "target" | "node_modules" | "__pycache__" | "tests"
            ) {
                walk(&p, &tp, out);
            }
        } else {
            out.push((tp, p));
        }
    }
}

#[test]
fn headers_are_deterministic_sorted_and_small() {
    let es = entries();
    assert!(!es.is_empty());
    assert!(BUNDLE.len() < 1_048_576, "bundle is {} bytes", BUNDLE.len());
    let paths: Vec<&str> = es.iter().map(|e| e.path.as_str()).collect();
    let mut sorted = paths.clone();
    sorted.sort_unstable();
    assert_eq!(paths, sorted, "entries are not in sorted order");
    for e in &es {
        assert!(e.regular, "{} is not a regular file", e.path);
        assert_eq!(e.mtime, 0, "{}", e.path);
        assert_eq!((e.uid, e.gid), (0, 0), "{}", e.path);
        assert!(e.user.is_empty() && e.group.is_empty(), "{}", e.path);
        assert!(
            e.mode == 0o644 || e.mode == 0o755,
            "{}: {:o}",
            e.path,
            e.mode
        );
    }
    // the build script's sha file agrees with the embedded bytes
    let sha_file = include_str!(concat!(env!("OUT_DIR"), "/sdk_bundle.sha256"));
    assert_eq!(sha_file.trim(), sdk_bundle::bundle_sha256());
    assert_eq!(sdk_bundle::bundle_id().len(), 16);
    assert!(sdk_bundle::bundle_sha256().starts_with(&sdk_bundle::bundle_id()));
    assert_eq!(sdk_bundle::entries().unwrap(), paths);
    eprintln!(
        "sdk bundle: {} entries, {} bytes, id {}",
        es.len(),
        BUNDLE.len(),
        sdk_bundle::bundle_id()
    );
}

#[test]
fn wit_copies_and_toolchain_lock_are_byte_identical() {
    let m = by_path();
    let wit = std::fs::read(repo().join("wit/ostro-runtime-0.1.0/runtime.wit")).unwrap();
    for p in [
        "rust/ostro-sdk/wit/runtime.wit",
        "python/ostro/wit/runtime.wit",
        "ts/wit/runtime.wit",
    ] {
        assert_eq!(m[p], wit, "{p} differs from the frozen WIT");
    }
    for f in ["package.json", "package-lock.json", "rolldown.ostro.mjs"] {
        let src = std::fs::read(repo().join("sdks/scripts/toolchain").join(f)).unwrap();
        assert_eq!(m[&format!("toolchain/{f}")], src, "toolchain/{f}");
    }
    assert_eq!(
        sdk_bundle::read_entry("toolchain/package-lock.json")
            .unwrap()
            .unwrap(),
        std::fs::read(repo().join("sdks/scripts/toolchain/package-lock.json")).unwrap()
    );
    assert!(sdk_bundle::read_entry("nope").unwrap().is_none());
}

#[test]
fn client_manifest_is_standalone() {
    let m = by_path();
    let text = String::from_utf8(m["rust/ostro-client/Cargo.toml"].clone()).unwrap();
    let t: toml::Table = toml::from_str(&text).unwrap();
    assert!(!t.contains_key("lints"), "{text}");
    assert!(!t.contains_key("dev-dependencies"), "{text}");
    assert_eq!(t["package"]["name"].as_str(), Some("ostro-client"));
    assert_eq!(t["package"]["publish"].as_bool(), Some(false));
    assert!(!text.contains("workspace = true"), "{text}");
    assert!(!text.contains("workspace=true"), "{text}");
    assert!(t["features"].get("testing").is_some(), "{text}");
}

fn tree(dir: &Path) -> BTreeMap<String, Vec<u8>> {
    let mut out = Vec::new();
    walk_all(dir, "", &mut out);
    out.into_iter()
        .map(|(k, p)| (k, std::fs::read(p).unwrap()))
        .collect()
}

fn walk_all(dir: &Path, prefix: &str, out: &mut Vec<(String, PathBuf)>) {
    for e in std::fs::read_dir(dir).unwrap() {
        let e = e.unwrap();
        let name = e.file_name().into_string().unwrap();
        let tp = if prefix.is_empty() {
            name
        } else {
            format!("{prefix}/{name}")
        };
        if e.path().is_dir() {
            walk_all(&e.path(), &tp, out);
        } else {
            out.push((tp, e.path()));
        }
    }
}

#[test]
fn materialize_is_idempotent() {
    let tmp = tempfile::tempdir().unwrap();
    let dest = tmp.path().join("proj/target/ostro/sdk");
    sdk_bundle::materialize(&dest).unwrap();
    let first = tree(&dest);
    assert_eq!(
        String::from_utf8(first[BUNDLE_ID_FILE].clone())
            .unwrap()
            .trim(),
        sdk_bundle::bundle_id()
    );
    let mut expected = by_path();
    expected.insert(
        BUNDLE_ID_FILE.to_owned(),
        format!("{}\n", sdk_bundle::bundle_id()).into_bytes(),
    );
    assert_eq!(first, expected, "materialised tree differs from the bundle");

    // second call: a no-op (a marker file placed inside survives, mtimes unchanged)
    let marker = dest.join("rust/ostro-sdk/src/lib.rs");
    let before = std::fs::metadata(&marker).unwrap().modified().unwrap();
    std::fs::write(dest.join("keep-me"), b"x").unwrap();
    sdk_bundle::materialize(&dest).unwrap();
    assert!(
        dest.join("keep-me").exists(),
        "second materialize was not a no-op"
    );
    assert_eq!(
        std::fs::metadata(&marker).unwrap().modified().unwrap(),
        before
    );
    // extracted files carry the extraction time, not the archive's mtime 0
    assert!(before > std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_000_000));

    // a stale id → the directory is replaced as a whole
    std::fs::write(dest.join(BUNDLE_ID_FILE), "stale\n").unwrap();
    sdk_bundle::materialize(&dest).unwrap();
    assert!(!dest.join("keep-me").exists());
    assert_eq!(tree(&dest), first);
    // no temporary directory is left behind
    let siblings: Vec<_> = std::fs::read_dir(dest.parent().unwrap())
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    assert_eq!(siblings, vec![std::ffi::OsString::from("sdk")]);
}
