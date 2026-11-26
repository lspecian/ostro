//! Offline audit chain verification (D-P14-11, 14-04): `ostro audit verify --data-dir`,
//! the soak and the crash tests.
//!
//! [`verify_dir`] walks every segment oldest first and checks, record by record: the line
//! is newline-terminated, canonical and self-hashing; seqs are contiguous across files;
//! `prev_hash` is the previous record's `hash`; every file starts with `segment_open` at the
//! seq of its name whose `prev_segment_last_hash` is its `prev_hash`. The first record is
//! either seq 1 chaining from the genesis hash of its node and incarnation, or the first
//! kept record of a retention trim, accepted only when a later `retention_trim` record names
//! exactly that `(seq, hash)` as its anchor. Any modified, inserted, deleted or reordered
//! record fails with the seq the chain expected at that point.
//!
//! A journal truncated *at its end* is still a valid chain; [`verify_dir_against`] also
//! checks an exported chain head `(seq, hash)` (W8, `ostro.audit.head`), which catches that.

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use super::chain::{self, Hash};
use super::journal::{self, Record};
use super::segment;
use crate::config::{EnvSource, Overrides, ServerConfig, load_layered};
use crate::p14::P14Error;

/// `ostro audit verify` arguments (the `ostro` binary intercepts the CLI variant).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct VerifyArgs {
    /// The node's data directory (`--data-dir`); the journal is `<data>/audit` unless
    /// `[audit] dir` says otherwise.
    pub data_dir: PathBuf,
    /// The server configuration file (`--config`).
    pub config: Option<PathBuf>,
}

/// A verified journal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifyReport {
    /// Records checked.
    pub records: u64,
    /// Segment files checked.
    pub segments: u64,
    /// First seq on disk (0 = empty journal).
    pub first_seq: u64,
    /// Last seq on disk (0 = empty journal).
    pub last_seq: u64,
    /// Hash of the last record (hex; empty for an empty journal).
    pub last_hash: String,
}

/// The first point where the chain breaks.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("audit chain broken at seq {seq}: {reason}")]
pub struct VerifyError {
    /// The seq the chain expected where it broke.
    pub seq: u64,
    pub reason: String,
}

fn fail<T>(seq: u64, reason: impl Into<String>) -> Result<T, VerifyError> {
    Err(VerifyError {
        seq,
        reason: reason.into(),
    })
}

/// Verify the journal in `dir`.
pub fn verify_dir(dir: &Path) -> Result<VerifyReport, VerifyError> {
    verify_dir_against(dir, None)
}

/// Verify the journal in `dir` and, when given, that it still contains the exported chain
/// head `(seq, hash hex)` (detects truncation or rewrite after the export).
pub fn verify_dir_against(
    dir: &Path,
    head: Option<(u64, &str)>,
) -> Result<VerifyReport, VerifyError> {
    let files = match segment::list_segments(dir) {
        Ok(f) => f,
        Err(e) => return fail(0, format!("{}: {e}", dir.display())),
    };
    let mut records = 0u64;
    let mut first_seq = 0u64;
    let mut expect: Option<u64> = None;
    let mut prev: Option<Hash> = None;
    let mut anchor: Option<(u64, String)> = None;
    let mut last_hash = String::new();
    let mut head_seen: Option<String> = None;
    for (name_seq, path) in &files {
        let at = expect.unwrap_or(*name_seq);
        let f = match File::open(path) {
            Ok(f) => f,
            Err(e) => return fail(at, format!("{}: {e}", path.display())),
        };
        let mut rd = BufReader::new(f);
        let mut line = Vec::new();
        let mut first_in_file = true;
        loop {
            line.clear();
            let n = match rd.read_until(b'\n', &mut line) {
                Ok(n) => n,
                Err(e) => return fail(expect.unwrap_or(at), format!("{}: {e}", path.display())),
            };
            if n == 0 {
                break;
            }
            let want = expect.unwrap_or(*name_seq);
            if line.pop() != Some(b'\n') {
                return fail(want, "torn tail (a record without its newline)");
            }
            let r = match Record::parse_line(&line) {
                Ok(r) => r,
                Err(e) => return fail(want, e.to_string()),
            };
            if r.seq != want {
                return fail(want, format!("expected seq {want}, found seq {}", r.seq));
            }
            let Some(h) = r.hash_bytes() else {
                return fail(want, "malformed hash");
            };
            match prev {
                Some(p) => {
                    if r.prev_hash_bytes() != Some(p) {
                        return fail(want, "prev_hash is not the previous record's hash");
                    }
                }
                None => {
                    first_seq = r.seq;
                    if r.seq == 1 {
                        if r.prev_hash_bytes() != Some(chain::genesis(r.node, r.incarnation)) {
                            return fail(
                                1,
                                "the first record does not chain from the genesis hash",
                            );
                        }
                    } else {
                        anchor = Some((r.seq, r.hash.clone()));
                    }
                }
            }
            if first_in_file {
                if r.action != journal::SEGMENT_OPEN || r.seq != *name_seq {
                    return fail(
                        want,
                        format!(
                            "{} does not start with segment_open at seq {name_seq}",
                            path.display()
                        ),
                    );
                }
                if r.detail(journal::D_PREV_SEGMENT_LAST_HASH) != Some(r.prev_hash.as_str()) {
                    return fail(want, "segment_open prev_segment_last_hash != prev_hash");
                }
                first_in_file = false;
            } else if r.action == journal::SEGMENT_OPEN {
                return fail(want, "segment_open inside a segment");
            }
            if r.action == journal::RETENTION_TRIM
                && let Some((aseq, ahash)) = &anchor
                && r.detail(journal::D_FIRST_KEPT_SEQ) == Some(aseq.to_string().as_str())
                && r.detail(journal::D_FIRST_KEPT_HASH) == Some(ahash.as_str())
            {
                anchor = None;
            }
            if let Some((hseq, _)) = head
                && hseq == r.seq
            {
                head_seen = Some(r.hash.clone());
            }
            records += 1;
            prev = Some(h);
            expect = Some(r.seq + 1);
            last_hash = r.hash;
        }
        if first_in_file {
            return fail(at, format!("{} is empty", path.display()));
        }
    }
    if let Some((aseq, _)) = anchor {
        return fail(
            aseq,
            format!("the journal starts at seq {aseq} without a matching retention_trim anchor"),
        );
    }
    let last_seq = expect.map_or(0, |e| e - 1);
    if let Some((hseq, hhash)) = head {
        match head_seen {
            None => {
                return fail(
                    last_seq + 1,
                    format!(
                        "the journal ends at seq {last_seq}, before the exported head seq {hseq}"
                    ),
                );
            }
            Some(h) if h != hhash => {
                return fail(hseq, "the record at the exported head has a different hash");
            }
            Some(_) => {}
        }
    }
    Ok(VerifyReport {
        records,
        segments: files.len() as u64,
        first_seq,
        last_seq,
        last_hash,
    })
}

/// The journal directory of a node: `[audit] dir` (relative = under the data dir), default
/// `<data>/audit`.
pub fn journal_dir(cfg: &ServerConfig, data_dir: &Path) -> PathBuf {
    match &cfg.audit.dir {
        Some(d) if d.is_absolute() => d.clone(),
        Some(d) => data_dir.join(d),
        None => data_dir.join("audit"),
    }
}

/// Verify the journal under `args.data_dir` and print the report.
pub fn offline(args: VerifyArgs) -> Result<(), P14Error> {
    let cfg = match &args.config {
        Some(path) => load_layered(
            ServerConfig::server_defaults(),
            Some(path),
            true,
            &EnvSource::Process,
            &Overrides::default(),
        )
        .map_err(|e| P14Error::new(format!("ostro audit verify: {e}")))?,
        None => ServerConfig::server_defaults(),
    };
    let dir = journal_dir(&cfg, &args.data_dir);
    if !dir.is_dir() {
        return Err(P14Error::new(format!(
            "ostro audit verify: no audit journal at {}",
            dir.display()
        )));
    }
    match verify_dir(&dir) {
        Ok(r) => {
            println!(
                "audit journal OK: {} records in {} segments, seq {}..{}, head {}",
                r.records, r.segments, r.first_seq, r.last_seq, r.last_hash
            );
            Ok(())
        }
        Err(e) => Err(P14Error::new(format!(
            "ostro audit verify: {}: {e}",
            dir.display()
        ))),
    }
}
