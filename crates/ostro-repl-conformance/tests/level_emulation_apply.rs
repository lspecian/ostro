//! 13-04 Task 2, level-emulation leg (D-P13-6/7): with `ostro-types/level-emulation` (on
//! in this crate's failpoints chain) and `OSTRO_TEST_LEVEL_MAX=1`, partition apply behaves
//! like a level-1 binary and fail-stops on a `RepinRun` (feature level 2) instead of
//! applying it. `level_max()` is read once per process, so the emulated leg runs in a child
//! process of this test binary (`child_level_1`), with the variable set only there.
//!
//! Lives here rather than in `ostro-partition/tests/apply_newer_format.rs`: the
//! `level-emulation` feature is reachable only through the failpoints chains of
//! ostro-server / ostro / ostro-repl-conformance (13-01), not ostro-partition's.
#![cfg(feature = "failpoints")]

use std::process::Command as Proc;

use ostro_partition::apply::{Action, Cursor, apply_command};
use ostro_partition::{PartitionStore, StoreConfig, StoreError};
use ostro_types::command::{RepinRun, encode_command};
use ostro_types::compat::{LEVEL, TEST_LEVEL_MAX_ENV, level_max};
use ostro_types::{Command, Epoch, PartitionCount, PartitionId, RunId};

const CHILD: &str = "OSTRO_13_04_LEVEL_CHILD";

fn apply_repin() -> Result<(), StoreError> {
    let dir = tempfile::tempdir().unwrap();
    let (store, meta) = PartitionStore::open(
        dir.path(),
        PartitionId(0),
        PartitionCount::new(1).unwrap(),
        &StoreConfig::default(),
    )
    .unwrap();
    let mut cur = Cursor::from_meta(&meta);
    let run = RunId::new_v7(PartitionId(0));
    let bytes = encode_command(&Command::repin_run(RepinRun {
        run_id: run.as_bytes().to_vec(),
        from_sha: vec![1; 32],
        to_sha: vec![2; 32],
        expected_next_seq: 1,
        requested_at_ms: 1,
    }));
    let before = (cur.next, cur.hash);
    let mut b = store.batch();
    let mut actions: Vec<Action> = Vec::new();
    let off = cur.next;
    let r = apply_command(&mut b, &mut cur, off, Epoch(0), &bytes, &mut actions).map(|_| ());
    if r.is_err() {
        assert_eq!((cur.next, cur.hash), before, "cursor moved on a fail-stop");
    }
    r
}

/// Runs only inside the child process (`OSTRO_TEST_LEVEL_MAX=1`).
#[test]
fn child_level_1() {
    if std::env::var_os(CHILD).is_none() {
        return;
    }
    assert_eq!(level_max(), 1, "emulated level");
    match apply_repin() {
        Err(e @ StoreError::NewerFormat(_)) => {
            let m = e.to_string();
            assert!(
                m.contains("written by a newer Ostro; upgrade this binary"),
                "{m}"
            );
            assert!(m.contains("repin_run"), "{m}");
        }
        other => panic!("an emulated level-1 binary applied RepinRun: {other:?}"),
    }
}

#[test]
fn emulated_level_1_binary_fail_stops_on_repin_run() {
    if std::env::var_os(CHILD).is_some() {
        return;
    }
    // This process is a full level-2 binary: RepinRun is known (13-17 semantics; the
    // 13-01 stub rejects it deterministically) and never fail-stops.
    assert_eq!(level_max(), LEVEL);
    apply_repin().expect("level 2 applies RepinRun without a fail-stop");

    let out = Proc::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "child_level_1",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD, "1")
        .env(TEST_LEVEL_MAX_ENV, "1")
        .output()
        .expect("spawn child");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "child failed\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stdout.contains("1 passed"),
        "child ran no test\nstdout:\n{stdout}"
    );
}
