//! On-disk format corpus (plan 05-12, D18 freeze; QA-02 companion).
//!
//! Source: the committed `tests/format-corpus/v0.1/{data/, format.toml}` once 05-20 freezes
//! it. Until then (generated-on-the-fly mode), a corpus is generated into a temp dir by
//! `examples/gen_format_corpus.rs`, which this test includes.
//!
//! The source is never opened in place. It is copied to a temp dir, and the copy is checked:
//! - it opens in Inspect mode with every partition Ready;
//! - `state_hash`, LEO and applied equal format.toml;
//! - **every per-table digest recorded in format.toml** matches;
//! - tables that exist now but are not in format.toml (placeholder keyspaces added later
//!   under RESERVATIONS §6) are empty;
//! - a second copy with every `state/` dir deleted rebuilds from the log to the same
//!   state_hash, applied offset, per-table digests and **full digest** as the opened copy.
//!
//! The full digest is not compared with format.toml: adding an empty keyspace changes it but
//! must never invalidate the frozen corpus. The log of the copy must contain every command
//! kind, every outbox kind and every `Derived` intent ([`corpus::CENSUS`]).
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

#[path = "../examples/gen_format_corpus.rs"]
mod corpus;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use ostro_partition::{Node, StartMode, TABLES};
use serde::Deserialize;

/// The committed corpus directory (05-20).
fn committed_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/format-corpus/v0.1")
}

/// Size target of a corpus data dir.
const MAX_CORPUS_BYTES: u64 = 2 * 1024 * 1024;

#[derive(Debug, Clone, Deserialize)]
struct FormatToml {
    format: String,
    ostro: String,
    partitions: u16,
    wal_rotation_bytes: u64,
    total_bytes: u64,
    runs: u64,
    partition: Vec<PartToml>,
}

#[derive(Debug, Clone, Deserialize)]
struct PartToml {
    id: u16,
    /// Informational (see the module docs).
    #[allow(dead_code)]
    digest: String,
    state_hash: String,
    leo: u64,
    applied: u64,
    tables: BTreeMap<String, String>,
    #[serde(default)]
    #[allow(dead_code)]
    rows: BTreeMap<String, u64>,
    #[serde(default)]
    #[allow(dead_code)]
    commands: BTreeMap<String, u64>,
}

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).expect("mkdir");
    for e in std::fs::read_dir(src).expect("read_dir") {
        let e = e.expect("entry");
        let to = dst.join(e.file_name());
        if e.file_type().expect("file type").is_dir() {
            copy_dir(&e.path(), &to);
        } else {
            std::fs::copy(e.path(), &to).expect("copy");
        }
    }
}

async fn open_all(dir: &Path) -> Result<(Node, Vec<corpus::PartFormat>), String> {
    let node = corpus::open(dir, StartMode::Inspect).await?;
    let mut parts = Vec::new();
    for p in 0..corpus::P {
        match corpus::part_format(&node, p).await {
            Ok(f) => parts.push(f),
            Err(e) => {
                let _ = node.shutdown().await;
                return Err(e);
            }
        }
    }
    Ok((node, parts))
}

/// Verify the corpus under `src` (`data/` + `format.toml`) on temp copies; returns the
/// opened copy's per-partition formats.
async fn verify(src: &Path, fmt: &FormatToml) -> Result<Vec<corpus::PartFormat>, String> {
    if fmt.partitions != corpus::P {
        return Err(format!(
            "format.toml partitions {} != {}",
            fmt.partitions,
            corpus::P
        ));
    }
    let tmp = ostro_testkit::fs::tempdir();
    let live_dir = tmp.path().join("live");
    let rebuilt_dir = tmp.path().join("rebuilt");
    copy_dir(&src.join("data"), &live_dir);
    copy_dir(&src.join("data"), &rebuilt_dir);
    std::fs::remove_dir_all(rebuilt_dir.join("state")).map_err(|e| format!("rm state: {e}"))?;

    let (live, lp) = open_all(&live_dir).await?;
    let census = {
        let mut m: BTreeMap<String, u64> = BTreeMap::new();
        for p in 0..corpus::P {
            for (k, n) in corpus::log_census(&live, p).await? {
                *m.entry(k).or_default() += n;
            }
        }
        m
    };
    live.shutdown()
        .await
        .map_err(|e| format!("shutdown: {e}"))?;
    let (rebuilt, rp) = open_all(&rebuilt_dir).await?;
    rebuilt
        .shutdown()
        .await
        .map_err(|e| format!("shutdown: {e}"))?;

    if let Some(u) = census.get("undecodable") {
        return Err(format!("{u} undecodable commands in the log"));
    }
    let missing: Vec<&str> = corpus::CENSUS
        .iter()
        .copied()
        .filter(|k| !census.contains_key(*k))
        .collect();
    if !missing.is_empty() {
        return Err(format!("the corpus log lacks {missing:?}"));
    }

    for rec in &fmt.partition {
        let p = usize::from(rec.id);
        let (l, r) = (&lp[p], &rp[p]);
        if corpus::hex(&l.state_hash) != rec.state_hash {
            return Err(format!("p{p}: state_hash differs from format.toml"));
        }
        if l.leo != rec.leo || l.applied != rec.applied {
            return Err(format!(
                "p{p}: leo/applied {}/{} != format.toml {}/{}",
                l.leo, l.applied, rec.leo, rec.applied
            ));
        }
        for (name, want) in &rec.tables {
            let Some((got, _)) = l.tables.get(name) else {
                return Err(format!(
                    "p{p}: table {name} in format.toml no longer exists"
                ));
            };
            if corpus::hex(got) != *want {
                return Err(format!(
                    "p{p}: table {name} digest differs from format.toml"
                ));
            }
        }
        for t in TABLES {
            if !rec.tables.contains_key(t.name()) {
                let rows = l.tables[t.name()].1;
                if rows != 0 {
                    return Err(format!(
                        "p{p}: table {} is not in format.toml but has {rows} rows",
                        t.name()
                    ));
                }
            }
        }
        // Rebuilt from the log == opened.
        if (r.state_hash, r.applied, &r.tables) != (l.state_hash, l.applied, &l.tables) {
            return Err(format!(
                "p{p}: rebuilt state differs from the opened copy (applied {} vs {})",
                r.applied, l.applied
            ));
        }
        if r.digest != l.digest {
            return Err(format!("p{p}: rebuilt full digest != opened full digest"));
        }
    }
    if fmt.partition.len() != usize::from(corpus::P) {
        return Err(format!(
            "format.toml has {} partitions",
            fmt.partition.len()
        ));
    }
    Ok(lp)
}

fn read_format(src: &Path) -> FormatToml {
    let text = std::fs::read_to_string(src.join("format.toml")).expect("read format.toml");
    toml::from_str(&text).expect("parse format.toml")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn format_corpus_opens_rebuilds_and_matches_format_toml() {
    let committed = committed_dir();
    let tmp = ostro_testkit::fs::tempdir();
    let src = if committed.join("format.toml").is_file() {
        println!("format-corpus: committed {}", committed.display());
        committed.clone()
    } else {
        let out = tmp.path().join("generated");
        let g = corpus::generate(&out).await.expect("generate");
        println!(
            "format-corpus: generated on the fly ({} runs, {} bytes, WAL rotation {} bytes)",
            g.runs,
            g.total_bytes,
            corpus::WAL_ROTATION_BYTES
        );
        out
    };
    let fmt = read_format(&src);
    let bytes = corpus::dir_bytes(&src.join("data"));
    println!(
        "format-corpus: {} (ostro {}), {} runs, {bytes} bytes on disk (recorded {}), rotation {}",
        fmt.format, fmt.ostro, fmt.runs, fmt.total_bytes, fmt.wal_rotation_bytes
    );
    assert!(
        bytes < MAX_CORPUS_BYTES,
        "format corpus is {bytes} bytes (target < {MAX_CORPUS_BYTES})"
    );
    let parts = verify(&src, &fmt).await.expect("format corpus verifies");
    for p in &parts {
        println!(
            "format-corpus: p{} leo {} applied {} ok",
            p.id, p.leo, p.applied
        );
    }

    // Negative control (on a copy of format.toml): a changed per-table digest, a changed
    // state hash and a missing partition record are all reported.
    let mut bad = fmt.clone();
    let runs = bad.partition[0].tables.get_mut("runs").expect("runs table");
    *runs = "00".repeat(32);
    let err = verify(&src, &bad).await.expect_err("bad table digest");
    assert!(err.contains("table runs digest"), "{err}");
    let mut bad = fmt.clone();
    bad.partition[1].state_hash = "11".repeat(32);
    let err = verify(&src, &bad).await.expect_err("bad state hash");
    assert!(err.contains("state_hash"), "{err}");
    let mut bad = fmt.clone();
    bad.partition.pop();
    assert!(verify(&src, &bad).await.is_err(), "missing partition");
    // A recorded table missing from format.toml must be empty: dropping a non-empty one
    // from the record is reported (this is how a later placeholder keyspace is treated).
    let mut bad = fmt.clone();
    bad.partition[0].tables.remove("events");
    let err = verify(&src, &bad)
        .await
        .expect_err("unrecorded non-empty table");
    assert!(err.contains("not in format.toml"), "{err}");
}
