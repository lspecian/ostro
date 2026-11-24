//! 13-04 Task 2 (CLUS-06, D-P13-6; 05-20 Phase 13 rule 2): partition apply fail-stops on a
//! command written by a newer Ostro instead of rejecting it and continuing.
//!
//! - `reencode_identity_over_goldens` / `reencode_identity_over_the_format_corpus`: prost
//!   re-encodes every frozen golden command and every command in the v0.1 format corpus
//!   logs to exactly its bytes, so `encoded_len() != bytes.len()` detects unknown fields
//!   (the detection path that landed; the D-P13-6 wire-walk fallback is not needed);
//! - an unknown `Cmd` oneof tag, an unknown top-level field and an unknown field nested in
//!   a known command → `Err(StoreError::NewerFormat)`, cursor and hash untouched, nothing
//!   written; genuinely empty bytes keep `Rejected(Invalid(Empty))`; undecodable bytes keep
//!   `Rejected(Invalid(Decode))`;
//! - through the apply machine: the partition halts (`PartitionHealth::Failed` with "written
//!   by a newer Ostro; upgrade this binary"), `applied` stays at the offending offset and
//!   later submits fail;
//! - failpoints build: the `upgrade::mutant::reject_and_continue` mutant restores the old
//!   reject-and-continue behaviour and the fail-stop property fails under it.
//!
//! The `level-emulation` leg (an emulated level-1 binary fail-stops on `RepinRun`) lives in
//! `ostro-repl-conformance/tests/level_emulation_apply.rs`: `ostro-types/level-emulation`
//! is reachable only through the failpoints chains of ostro-server / ostro /
//! ostro-repl-conformance (13-01), not ostro-partition's.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

#[path = "../examples/gen_format_corpus.rs"]
mod corpus;

use bytes::Bytes;
use common::{T, cfg, new_run, open_single, start_cmd, start_run};
use ostro_log::ReplicatedLog;
use ostro_partition::apply::{
    Action, ApplyOutcome, Cursor, InvalidCode, RejectReason, apply_command,
};
use ostro_partition::{PartitionHealth, PartitionStore, StartMode, StoreConfig, StoreError};
use ostro_types::command::{decode_command, encode_command};
use ostro_types::{Epoch, PartitionCount, PartitionId};
use prost::Message;

const GOLDEN: &str = include_str!("../../ostro-types/tests/golden/commands.txt");
const NEWER: &str = "written by a newer Ostro; upgrade this binary";

fn unhex(s: &str) -> Vec<u8> {
    assert!(s.len().is_multiple_of(2), "odd hex length");
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex"))
        .collect()
}

fn varint(mut v: u64, out: &mut Vec<u8>) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// A length-delimited field `tag` carrying `payload`.
fn ld_field(tag: u32, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    varint(u64::from(tag) << 3 | 2, &mut out);
    varint(payload.len() as u64, &mut out);
    out.extend_from_slice(payload);
    out
}

/// A varint field `tag` = `v`.
fn varint_field(tag: u32, v: u64) -> Vec<u8> {
    let mut out = Vec::new();
    varint(u64::from(tag) << 3, &mut out);
    varint(v, &mut out);
    out
}

fn assert_reencodes(what: &str, bytes: &[u8]) {
    let c = decode_command(bytes).unwrap_or_else(|e| panic!("{what}: decode: {e}"));
    assert_eq!(
        c.encoded_len(),
        bytes.len(),
        "{what}: prost re-encoding differs in length"
    );
    assert_eq!(
        c.encode_to_vec(),
        bytes,
        "{what}: prost re-encoding differs"
    );
}

// ---------------------------------------------------------------- re-encode identity

#[test]
fn reencode_identity_over_goldens() {
    let mut n = 0;
    for line in GOLDEN.lines() {
        if line.starts_with('#') || line.trim().is_empty() {
            continue;
        }
        let (name, hex) = line.split_once('\t').expect("name<TAB>hex");
        assert_reencodes(name, &unhex(hex.trim()));
        n += 1;
    }
    assert!(n >= 30, "golden set too small: {n}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reencode_identity_over_the_format_corpus() {
    let src = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/format-corpus/v0.1/data");
    let tmp = ostro_testkit::fs::tempdir();
    let dir = tmp.path().join("copy");
    common::copy_dir(&src, &dir);
    let node = corpus::open(&dir, StartMode::Inspect).await.expect("open");
    let mut n = 0u64;
    for p in 0..corpus::P {
        let h = node.handle(PartitionId(p)).expect("partition ready");
        let log = h.log();
        let hw = *log.high_watermark().borrow();
        let mut next = log.log_start();
        while next < hw {
            let batches = log.read(next, hw, 1 << 20).await.expect("read");
            let last = batches.last().expect("short read below HW").last();
            for vb in &batches {
                for (off, bytes) in vb.commands_with_offsets() {
                    assert_reencodes(&format!("p{p}@{off}"), bytes);
                    n += 1;
                }
            }
            next = last + 1;
        }
    }
    node.shutdown().await.expect("shutdown");
    assert!(n > 100, "corpus too small: {n} commands");
}

// ---------------------------------------------------------------- pure apply

struct H {
    _dir: tempfile::TempDir,
    store: PartitionStore,
    cur: Cursor,
}

impl H {
    fn new() -> H {
        let dir = ostro_testkit::fs::tempdir();
        let (store, meta) = PartitionStore::open(
            dir.path(),
            PartitionId(0),
            PartitionCount::new(1).expect("non-zero"),
            &StoreConfig::default(),
        )
        .expect("open store");
        let cur = Cursor::from_meta(&meta);
        H {
            _dir: dir,
            store,
            cur,
        }
    }

    /// Apply `bytes` at the next offset; commit only on `Ok`.
    fn apply(&mut self, bytes: &[u8]) -> Result<ApplyOutcome, StoreError> {
        let mut b = self.store.batch();
        let mut actions: Vec<Action> = Vec::new();
        let off = self.cur.next;
        let out = apply_command(&mut b, &mut self.cur, off, Epoch(0), bytes, &mut actions)?;
        let mu = self.cur.meta_update();
        b.commit(&mu).expect("commit");
        Ok(out)
    }

    fn digest(&self) -> [u8; 32] {
        self.store.digest().expect("digest")
    }
}

fn start_bytes() -> Vec<u8> {
    encode_command(&start_cmd(&new_run())).to_vec()
}

/// Unknown command tag 999 (an empty length-delimited field).
fn unknown_tag() -> Vec<u8> {
    ld_field(999, &[])
}

/// A known `StartRun` plus an unknown top-level `Command` field.
fn unknown_top_level_field() -> Vec<u8> {
    let mut b = start_bytes();
    b.extend(varint_field(1000, 7));
    b
}

/// A `StartRun` whose message carries an unknown field (StartRun is `Cmd` tag 2).
fn unknown_nested_field() -> Vec<u8> {
    let mut inner = start_run(&new_run()).encode_to_vec();
    inner.extend(varint_field(900, 1));
    ld_field(2, &inner)
}

fn assert_fail_stop(h: &mut H, what: &str, bytes: &[u8]) {
    let before = (h.cur.next, h.cur.hash);
    let digest = h.digest();
    match h.apply(bytes) {
        Err(e @ StoreError::NewerFormat(_)) => {
            assert!(e.to_string().contains(NEWER), "{what}: {e}")
        }
        other => panic!("{what}: expected NewerFormat, got {other:?}"),
    }
    assert_eq!((h.cur.next, h.cur.hash), before, "{what}: cursor moved");
    assert_eq!(h.digest(), digest, "{what}: state changed");
}

#[test]
fn unknown_input_fails_stop_and_moves_nothing() {
    let mut h = H::new();
    // Something accepted first, so the fail-stop is checked against a non-trivial state.
    assert_eq!(h.apply(&start_bytes()).unwrap(), ApplyOutcome::Accepted);
    assert_fail_stop(&mut h, "unknown Cmd tag", &unknown_tag());
    assert_fail_stop(
        &mut h,
        "unknown top-level field",
        &unknown_top_level_field(),
    );
    assert_fail_stop(&mut h, "unknown nested field", &unknown_nested_field());
    // A bare unknown field with no oneof at all.
    assert_fail_stop(&mut h, "unknown field only", &varint_field(1000, 1));
}

#[test]
fn empty_and_undecodable_keep_their_rejections() {
    let mut h = H::new();
    let next = h.cur.next;
    assert_eq!(
        h.apply(&[]).unwrap(),
        ApplyOutcome::Rejected(RejectReason::Invalid(InvalidCode::Empty))
    );
    assert_eq!(
        h.cur.next,
        next.next(),
        "an empty command consumes its offset"
    );
    assert_eq!(
        h.apply(&[0xff, 0xff, 0xff]).unwrap(),
        ApplyOutcome::Rejected(RejectReason::Invalid(InvalidCode::Decode))
    );
    // Known input behaves as before.
    assert_eq!(h.apply(&start_bytes()).unwrap(), ApplyOutcome::Accepted);
}

// ---------------------------------------------------------------- apply machine

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_partition_halts_on_a_newer_command() {
    let tmp = ostro_testkit::fs::tempdir();
    let tp = open_single(tmp.path(), cfg()).await;
    assert_eq!(
        tp.handle.submit_one(start_cmd(&new_run())).await.unwrap(),
        ApplyOutcome::Accepted
    );
    let log = tp.handle.log().clone();
    let receipt = log
        .append(tp.handle.epoch(), vec![Bytes::from(unknown_tag())])
        .await
        .expect("append");
    let mut health = tp.handle.machine().health();
    tokio::time::timeout(
        T,
        health.wait_for(|h| matches!(h, PartitionHealth::Failed { .. })),
    )
    .await
    .expect("partition did not halt")
    .unwrap();
    match &*tp.handle.machine().health().borrow() {
        PartitionHealth::Failed { reason } => {
            assert!(reason.contains(NEWER), "reason: {reason}");
        }
        h => panic!("{h:?}"),
    }
    assert_eq!(
        tp.applied(),
        receipt.base,
        "applied must stop before the newer command"
    );
    let r = tokio::time::timeout(T, tp.handle.submit_one(start_cmd(&new_run())))
        .await
        .expect("submit hung");
    assert!(r.is_err(), "a halted partition accepted a submit: {r:?}");
    let _ = tp.close().await;
}

// ---------------------------------------------------------------- mutant

#[cfg(feature = "failpoints")]
#[test]
fn reject_and_continue_mutant_bites() {
    const MUTANT: &str = "upgrade::mutant::reject_and_continue";
    assert!(ostro_types::compat::MUTANTS.contains(&MUTANT));
    let fails_stop = |h: &mut H| matches!(h.apply(&unknown_tag()), Err(StoreError::NewerFormat(_)));
    let mut h = H::new();
    assert!(fails_stop(&mut h), "property holds without the mutant");
    fail::cfg(MUTANT, "return").unwrap();
    let mut m = H::new();
    let bitten = !fails_stop(&mut m);
    let continued = m.cur.next;
    fail::remove(MUTANT);
    assert!(
        bitten,
        "the mutant bites: unknown input rejected and continued"
    );
    assert_eq!(continued.0, 1, "the old behaviour consumed the offset");
    assert!(fails_stop(&mut h), "holds again after fail::remove");
}
