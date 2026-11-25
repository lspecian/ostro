//! `read_meta_log` over metadata log stores written by the real vendored `RaftLogStore`.
//!
//! Regression (p12-campaign-r3, fault ~5,051): all three nodes snapshotted their metadata
//! log at exactly the commit index (60002 == commit), which `create_snapshot` allows
//! (`index <= commit`). The reader replayed `snapshot_index + 1..=commit`, a reversed range,
//! and panicked inside `BTreeMap::range`. With nothing after the snapshot committed, the
//! snapshot state is the committed state.

use std::path::Path;

use ostro_meta::{MetaCommand, MetaState, encode_command, encode_snapshot};
use ostro_raft_driver::raft::eraftpb::{ConfState, Entry, HardState};
use ostro_raft_driver::{LogStoreOptions, RaftLogStore};
use ostro_repl_conformance::harness::inproc::read_meta_log;

fn init() -> MetaCommand {
    MetaCommand::InitCluster {
        cluster_id: "conf-0".into(),
        node_id: 1,
        partitions: 4,
        created_at_ms: 0,
        token_key: [3; 32],
    }
}

fn entry(index: u64, data: Vec<u8>) -> Entry {
    Entry {
        index,
        term: 1,
        data: data.into(),
        ..Entry::default()
    }
}

fn conf() -> ConfState {
    ConfState {
        voters: vec![1, 2, 3],
        ..ConfState::default()
    }
}

/// A store holding a leader no-op at 1 and `InitCluster` at 2, committed through 2.
fn store_committed_through_2(dir: &Path) -> (RaftLogStore, MetaState) {
    let opts = LogStoreOptions {
        sync: false,
        ..LogStoreOptions::default()
    };
    let store = RaftLogStore::open(dir, opts).expect("open store");
    store
        .append(&[entry(1, Vec::new()), entry(2, encode_command(&init()))])
        .expect("append");
    store.set_conf_state(conf()).expect("conf state");
    store
        .set_hardstate(HardState {
            term: 1,
            vote: 1,
            commit: 2,
        })
        .expect("hardstate");
    let mut state = MetaState::default();
    state.apply(2, init());
    (store, state)
}

#[test]
fn snapshot_at_the_commit_index_replays_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (store, state) = store_committed_through_2(dir.path());
    store
        .create_snapshot(2, conf(), encode_snapshot(&state))
        .expect("snapshot at commit");
    drop(store);

    let dump = read_meta_log(dir.path()).expect("snapshot_index == commit must read cleanly");
    assert_eq!((dump.commit, dump.snapshot_index), (2, 2));
    assert!(dump.entries.is_empty(), "{:?}", dump.entries);
    assert!(dump.undecodable.is_empty(), "{:?}", dump.undecodable);
    assert_eq!(dump.voters, vec![1, 2, 3]);
    assert_eq!(dump.snapshot_state, state);
    assert_eq!(dump.state, state);
}

#[test]
fn entries_committed_after_the_snapshot_are_replayed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (store, state) = store_committed_through_2(dir.path());
    store
        .create_snapshot(2, conf(), encode_snapshot(&state))
        .expect("snapshot at commit");
    // Entry 3 committed after the snapshot; entry 4 appended but not committed.
    let bump = MetaCommand::InitCluster {
        cluster_id: "conf-1".into(),
        node_id: 2,
        partitions: 4,
        created_at_ms: 1,
        token_key: [4; 32],
    };
    store
        .append(&[entry(3, encode_command(&bump)), entry(4, Vec::new())])
        .expect("append");
    store.set_commit(3).expect("commit 3");
    drop(store);

    let dump = read_meta_log(dir.path()).expect("read");
    assert_eq!((dump.commit, dump.snapshot_index), (3, 2));
    assert_eq!(dump.snapshot_state, state);
    let indexes: Vec<u64> = dump.entries.iter().map(|e| e.index).collect();
    assert_eq!(indexes, vec![3]);
    let mut expected = state.clone();
    expected.apply(3, bump);
    assert_eq!(dump.state, expected);
}
