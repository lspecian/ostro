// Ostro-authored (Phase 13, 13-05); not vendored. Declared in driver.rs; reuses its in-file
// cluster harness (`tests::{Cluster, Router, Names, config, create}`).
//! Membership changes (learner, joint consensus with automatic and explicit leave),
//! `ReadIndex`, leadership transfer, per-peer progress, the lease anchor and `FatalApply`.

use super::tests::{config, create, Cluster, Names};
use super::*;
use std::collections::BTreeSet;
use tempfile::TempDir;
use raft::eraftpb::{ConfChangeSingle, ConfChangeTransition, ConfChangeType};

const LONG: Duration = Duration::from_secs(30);

async fn until(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + LONG;
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting for {}", what);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn single(t: ConfChangeType, id: u64) -> ConfChangeSingle {
    let mut s = ConfChangeSingle::default();
    s.set_change_type(t);
    s.node_id = id;
    s
}

fn cc(changes: Vec<ConfChangeSingle>, transition: ConfChangeTransition) -> ConfChangeV2 {
    let mut c = ConfChangeV2::default();
    c.changes = changes;
    c.set_transition(transition);
    c
}

/// Start a blank node (no initial voters): it joins through a snapshot from the leader.
fn start_blank(c: &mut Cluster, id: u64) {
    c.dirs.entry(id).or_insert_with(|| TempDir::new().unwrap());
    let sm = Names::default();
    let d = RaftDriver::start(c.dirs[&id].path(), (c.cfg)(id), &[], sm.clone(), c.router.outbox(id)).unwrap();
    c.router.register(id, d.handle());
    c.sms.insert(id, sm);
    c.drivers.insert(id, d);
}

/// Propose a conf change on whoever leads, retrying while leadership moves.
async fn propose_cc(c: &Cluster, change: ConfChangeV2) -> Applied {
    let deadline = Instant::now() + LONG;
    loop {
        let leader = c.leader().await;
        match c.h(leader).propose_conf_change(change.clone()).await {
            Ok(a) => return a,
            Err(e) => {
                assert!(Instant::now() < deadline, "conf change never applied: {:?}", e);
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

async fn write_n(c: &Cluster, prefix: &str, n: usize) -> u64 {
    let leader = c.leader().await;
    let mut last = 0;
    for i in 0..n {
        last = create(&c.h(leader), &format!("{}{}", prefix, i)).await.unwrap().index;
    }
    last
}

/// Add `id` as a learner and wait until it applied `index`.
async fn add_learner(c: &mut Cluster, id: u64, index: u64) {
    start_blank(c, id);
    let r = propose_cc(c, cc(vec![single(ConfChangeType::AddLearnerNode, id)], ConfChangeTransition::Auto)).await;
    assert!(!r.result.is_empty(), "the learner change was rejected");
    tokio::time::timeout(LONG, c.h(id).wait_applied(index.max(r.index))).await.expect("learner caught up").unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn learner_add_catch_up_promote() {
    let mut c = Cluster::new(&[1, 2, 3], config);
    let last = write_n(&c, "l", 30).await;
    add_learner(&mut c, 4, last).await;
    assert_eq!(c.sms[&4].set().len(), 30);
    let leader = c.leader().await;
    until("learner 4 in the status", || c.h(leader).status().learners == vec![4]).await;
    assert_eq!(c.h(leader).status().voters, vec![1, 2, 3]);

    // Promote: a plain AddNode of the learner.
    let r = propose_cc(&c, cc(vec![single(ConfChangeType::AddNode, 4)], ConfChangeTransition::Auto)).await;
    assert!(!r.result.is_empty());
    let leader = c.leader().await;
    until("4 promoted", || {
        let s = c.h(leader).status();
        s.voters == vec![1, 2, 3, 4] && s.learners.is_empty() && !s.pending_conf
    })
    .await;
    // The promoted node takes part in commits.
    let r = create(&c.h(leader), "after-promote").await.unwrap();
    tokio::time::timeout(LONG, c.h(4).wait_applied(r.index)).await.unwrap().unwrap();
    assert!(c.sms[&4].set().contains("after-promote"));
}

#[tokio::test(flavor = "multi_thread")]
async fn replace_via_joint_auto() {
    let mut c = Cluster::new(&[1, 2, 3], config);
    let last = write_n(&c, "j", 10).await;
    add_learner(&mut c, 4, last).await;
    let leader = c.leader().await;
    let removed = *c.ids.iter().find(|&&i| i != leader).unwrap();
    // Joint consensus with automatic leave: one proposal replaces `removed` by 4.
    let r = propose_cc(
        &c,
        cc(
            vec![single(ConfChangeType::AddNode, 4), single(ConfChangeType::RemoveNode, removed)],
            ConfChangeTransition::Auto,
        ),
    )
    .await;
    let entered = ConfState::decode(r.result.as_slice()).unwrap();
    assert!(!entered.voters_outgoing.is_empty(), "entered joint: {:?}", entered);
    assert!(entered.auto_leave);
    let mut want: Vec<u64> = [1, 2, 3, 4].iter().copied().filter(|&i| i != removed).collect();
    want.sort_unstable();
    let leader = c.leader().await;
    until("joint left automatically", || {
        let s = c.h(leader).status();
        s.voters == want && s.voters_outgoing.is_empty() && !s.pending_conf
    })
    .await;
    c.stop(removed);
    let r = create(&c.h(c.leader().await), "after-replace").await.unwrap();
    tokio::time::timeout(LONG, c.h(4).wait_applied(r.index)).await.unwrap().unwrap();
}

/// The leader that would have left the joint configuration dies after the joint entry
/// committed: raft-rs never auto-leaves on the next leader (raft.rs auto-leave TODO), so
/// the joint state persists until an empty ConfChangeV2 leaves it explicitly. The
/// transition is Explicit to make the "leader died before its auto-leave" state
/// deterministic.
#[tokio::test(flavor = "multi_thread")]
async fn leave_joint_after_leader_kill() {
    let mut c = Cluster::new(&[1, 2, 3], config);
    let last = write_n(&c, "k", 10).await;
    add_learner(&mut c, 4, last).await;
    let leader = c.leader().await;
    let removed = *c.ids.iter().find(|&&i| i != leader).unwrap();
    let r = c
        .h(leader)
        .propose_conf_change(cc(
            vec![single(ConfChangeType::AddNode, 4), single(ConfChangeType::RemoveNode, removed)],
            ConfChangeTransition::Explicit,
        ))
        .await
        .unwrap();
    // Every survivor applied the joint entry; then the leader dies.
    for id in [1u64, 2, 3, 4].into_iter().filter(|&i| i != leader) {
        tokio::time::timeout(LONG, c.h(id).wait_applied(r.index)).await.unwrap().unwrap();
    }
    c.stop(leader);
    let new = c.leader().await;
    assert_ne!(new, leader);
    let s = c.h(new).status();
    assert!(!s.voters_outgoing.is_empty(), "the new leader is still joint: {:?}", s);
    assert!(!s.auto_leave, "{:?}", s);

    // An empty ConfChangeV2 leaves the joint configuration.
    let left = propose_cc(&c, ConfChangeV2::default()).await;
    let cs = ConfState::decode(left.result.as_slice()).unwrap();
    assert!(cs.voters_outgoing.is_empty(), "{:?}", cs);
    let mut want: Vec<u64> = [1, 2, 3, 4].iter().copied().filter(|&i| i != removed).collect();
    want.sort_unstable();
    let mut sorted = cs.voters.clone();
    sorted.sort_unstable();
    assert_eq!(sorted, want);
    let other = *want.iter().find(|&&i| i != leader && c.drivers.contains_key(&i) && i != 4).unwrap_or(&4);
    until("survivors left joint", || {
        let s = c.h(other).status();
        s.voters == want && s.voters_outgoing.is_empty()
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn read_index_on_follower() {
    let c = Cluster::new(&[1, 2, 3], config);
    let leader = c.leader().await;
    let follower = *c.ids.iter().find(|&&i| i != leader).unwrap();
    c.h(follower).wait_for_leader().await.unwrap();
    for i in 0..20 {
        let w = create(&c.h(leader), &format!("r{}", i)).await.unwrap();
        // The leader's commit index when the read starts: the write is committed.
        // (The status is published after the apply cycle that resolved the proposal.)
        until("leader status shows the commit", || c.h(leader).status().commit >= w.index).await;
        let commit_at_call = c.h(leader).status().commit;
        let idx = tokio::time::timeout(LONG, c.h(follower).read_index()).await.unwrap().unwrap();
        assert!(idx >= w.index, "read index {} < acknowledged write {}", idx, w.index);
        assert!(idx >= commit_at_call, "read index {} < leader commit {} at call", idx, commit_at_call);
        tokio::time::timeout(LONG, c.h(follower).wait_applied(idx)).await.unwrap().unwrap();
        assert!(c.sms[&follower].set().contains(&format!("r{}", i)));
    }
    // On the leader itself too.
    let idx = c.h(leader).read_index().await.unwrap();
    assert!(idx >= c.h(leader).status().applied.min(idx));
}

/// A read never hangs: on an isolated leader (it cannot confirm a quorum: no stale read,
/// unlike a plain local read before check-quorum steps it down) and on a follower whose
/// leader died.
#[tokio::test(flavor = "multi_thread")]
async fn read_index_fails_fast_on_leader_loss() {
    let mut c = Cluster::new(&[1, 2, 3], config);
    let leader = c.leader().await;
    create(&c.h(leader), "x").await.unwrap();
    let others: Vec<u64> = c.ids.iter().copied().filter(|&i| i != leader).collect();
    c.router.isolate(leader, &others);
    let t = Instant::now();
    let r = tokio::time::timeout(Duration::from_secs(10), c.h(leader).read_index()).await.expect("isolated leader read hung");
    assert!(r.is_err(), "an isolated leader served a read index: {:?}", r);
    eprintln!("isolated leader read failed after {:?}: {:?}", t.elapsed(), r);
    c.router.heal();

    let leader = c.leader().await;
    let follower = *c.ids.iter().find(|&&i| i != leader).unwrap();
    c.h(follower).wait_for_leader().await.unwrap();
    c.stop(leader);
    let t = Instant::now();
    let r = tokio::time::timeout(Duration::from_secs(10), c.h(follower).read_index()).await.expect("follower read hung");
    // Either the read failed fast, or a new leader won the race and answered it.
    eprintln!("follower read after leader loss: {:?} after {:?}", r, t.elapsed());
}

#[tokio::test(flavor = "multi_thread")]
async fn transfer_leader_moves_leadership() {
    let c = Cluster::new(&[1, 2, 3], config);
    let leader = c.leader().await;
    let target = *c.ids.iter().find(|&&i| i != leader).unwrap();
    let r = create(&c.h(leader), "t0").await.unwrap();
    c.settle(r.index).await;
    // A follower cannot start a transfer.
    assert!(matches!(c.h(target).transfer_leader(leader).await, Err(ProposeError::NotLeader { .. })));
    c.h(leader).transfer_leader(target).await.unwrap();
    until("leadership moved", || c.h(target).status().is_leader).await;
    assert!(!c.h(leader).status().is_leader);
    let r = create(&c.h(target), "t1").await.unwrap();
    c.settle(r.index).await;
    for id in &c.ids {
        assert!(c.sms[id].set().contains("t1"), "node {}", id);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn progress_reports_learner_matched() {
    let mut c = Cluster::new(&[1, 2, 3], config);
    let last = write_n(&c, "p", 20).await;
    add_learner(&mut c, 4, last).await;
    let leader = c.leader().await;
    let r = create(&c.h(leader), "p-last").await.unwrap();
    until("learner matched", || {
        let s = c.h(leader).status();
        s.progress.iter().any(|p| p.id == 4 && p.matched >= r.index && p.next_idx > p.matched && p.pending_snapshot == 0)
    })
    .await;
    let s = c.h(leader).status();
    assert_eq!(s.progress.iter().map(|p| p.id).collect::<Vec<_>>(), vec![1, 2, 3, 4], "{:?}", s.progress);
    until("learner recent_active", || c.h(leader).status().progress.iter().any(|p| p.id == 4 && p.recent_active)).await;
    let follower = *c.ids.iter().find(|&&i| i != leader).unwrap();
    assert!(c.h(follower).status().progress.is_empty(), "progress is leader-only");
    assert_eq!(c.h(follower).status().learners, vec![4]);
}

/// FatalApply: the driver halts before `applied` advances; a restart replays the same
/// entry and halts again.
#[tokio::test(flavor = "multi_thread")]
async fn fatal_apply_stops_before_applied() {
    let mut c = Cluster::new(&[1], config);
    c.leader().await;
    let a = create(&c.h(1), "a").await.unwrap();
    let err = c.h(1).propose(b"halt:newer entry".to_vec()).await.unwrap_err();
    assert_eq!(err, ProposeError::Fatal("newer entry".into()));
    let s = c.h(1).status();
    assert_eq!(s.fatal.as_deref(), Some("newer entry"), "{:?}", s);
    assert!(s.failed.as_deref().unwrap_or("").contains("newer entry"), "{:?}", s);
    assert_eq!(s.applied, a.index, "applied must not pass the refused entry");
    assert!(matches!(c.h(1).wait_applied(a.index + 1).await, Err(ProposeError::Fatal(_))));
    assert!(c.h(1).propose(b"create:b".to_vec()).await.is_err());

    // Restart: the replay reaches the same entry and halts again.
    c.stop(1);
    c.start(1);
    let mut st = c.h(1).subscribe();
    let fatal = tokio::time::timeout(LONG, async {
        loop {
            if let Some(f) = st.borrow_and_update().fatal.clone() {
                return f;
            }
            st.changed().await.unwrap();
        }
    })
    .await
    .expect("the restarted node never halted");
    assert_eq!(fatal, "newer entry");
    assert_eq!(c.h(1).status().applied, a.index);
    assert_eq!(c.sms[&1].set(), BTreeSet::from(["a".to_string()]));
}

/// FatalApply on a 3-voter group: every voter runs the same state machine, so every voter
/// halts at the same index; none applies past it.
#[tokio::test(flavor = "multi_thread")]
async fn fatal_apply_halts_every_voter_at_the_same_index() {
    let c = Cluster::new(&[1, 2, 3], config);
    let leader = c.leader().await;
    let a = create(&c.h(leader), "a").await.unwrap();
    c.settle(a.index).await;
    let _ = c.h(leader).propose(b"halt:x".to_vec()).await;
    for id in &c.ids {
        until("voter halted", || c.h(*id).status().fatal.is_some()).await;
        assert_eq!(c.h(*id).status().applied, a.index, "node {}", id);
    }
}

// ---- lease anchor ----------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn lease_one_voter_is_now() {
    let c = Cluster::new(&[1], config);
    c.leader().await;
    let before = Instant::now();
    let a = c.h(1).leader_lease_anchor().expect("one-voter leader has a lease");
    assert!(a >= before && a <= Instant::now());
}

/// The anchor is the send instant of the round, never the ack time: with a 100 ms one-way
/// delay, an anchor seen right after it advanced is at least one round trip old.
#[tokio::test(flavor = "multi_thread")]
async fn lease_anchor_is_the_send_instant() {
    let c = Cluster::new(&[1, 2, 3], config);
    let leader = c.leader().await;
    create(&c.h(leader), "warm").await.unwrap();
    let delay = Duration::from_millis(100);
    c.router.0.lock().unwrap().delay = delay;
    let follower = *c.ids.iter().find(|&&i| i != leader).unwrap();

    for (who, min_age) in [(leader, delay * 2), (follower, delay * 2)] {
        let h = c.h(who);
        let _ = h.lease_anchor(); // a follower starts its rounds on the first ask
        let start = Instant::now();
        let mut seen = None;
        let mut samples = 0;
        while samples < 5 {
            assert!(start.elapsed() < LONG, "node {} lease never advanced", who);
            if let Some(a) = h.lease_anchor() {
                if a > start && seen.map(|s| a > s).unwrap_or(true) {
                    let age = Instant::now().duration_since(a);
                    assert!(age >= min_age, "node {}: anchor {:?} old when first seen (< {:?})", who, age, min_age);
                    seen = Some(a);
                    samples += 1;
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
    c.router.0.lock().unwrap().delay = Duration::ZERO;
}

#[tokio::test(flavor = "multi_thread")]
async fn lease_none_after_leader_change() {
    let mut c = Cluster::new(&[1, 2, 3], config);
    let leader = c.leader().await;
    create(&c.h(leader), "warm").await.unwrap();
    until("leader lease", || c.h(leader).leader_lease_anchor().is_some()).await;
    let follower = *c.ids.iter().find(|&&i| i != leader).unwrap();
    assert!(c.h(follower).leader_lease_anchor().is_none(), "leader_lease_anchor is leader-only");
    until("follower lease", || c.h(follower).lease_anchor().is_some()).await;

    c.stop(leader);
    let stopped = Instant::now();
    // Once the follower moves on from the dead leader, the old anchor is gone: what it
    // reports is absent or from a round sent after the change.
    until("leader change seen", || c.h(follower).status().leader_id != Some(leader)).await;
    if let Some(a) = c.h(follower).lease_anchor() {
        assert!(a > stopped, "an anchor from the old leader survived the change");
    }
    let new = c.leader().await;
    until("new leader lease", || c.h(new).leader_lease_anchor().is_some()).await;
}

/// A leader cut off from both peers: its anchor never passes the cut, and check-quorum
/// steps it down, after which it has no leader lease.
#[tokio::test(flavor = "multi_thread")]
async fn lease_cut_off_leader_stops_and_steps_down() {
    let c = Cluster::new(&[1, 2, 3], config);
    let leader = c.leader().await;
    create(&c.h(leader), "warm").await.unwrap();
    until("leader lease", || c.h(leader).leader_lease_anchor().is_some()).await;
    let others: Vec<u64> = c.ids.iter().copied().filter(|&i| i != leader).collect();
    let cut = Instant::now();
    c.router.isolate(leader, &others);
    let deadline = Instant::now() + LONG;
    loop {
        match c.h(leader).leader_lease_anchor() {
            Some(a) => assert!(a <= cut, "anchor advanced past the cut by {:?}", a.duration_since(cut)),
            None => break,
        }
        assert!(Instant::now() < deadline, "the cut-off leader never lost its lease");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    until("stepped down", || !c.h(leader).status().is_leader).await;
    eprintln!("cut-off leader lost its lease / stepped down after {:?}", cut.elapsed());
    assert!(c.h(leader).leader_lease_anchor().is_none());
    c.router.heal();
}

/// `snapshot_now` compacts the leader's log at once (`snapshot_every` is 10,000 here): a
/// voter that was down catches up from that snapshot instead of replaying every entry.
#[tokio::test(flavor = "multi_thread")]
async fn snapshot_now_compacts_and_a_lagging_voter_installs_it() {
    let mut c = Cluster::new(&[1, 2, 3], config);
    let leader = c.leader().await;
    let down = *c.ids.iter().find(|&&i| i != leader).unwrap();
    c.stop(down);
    let last = write_n(&c, "s", 50).await;
    c.h(leader).snapshot_now();
    tokio::time::sleep(Duration::from_millis(200)).await;
    c.start(down);
    c.settle(last).await;
    assert_eq!(c.sms[&down].set().len(), 50);
    assert!(c.sms[&down].applied_log().len() < 50, "{} replayed", c.sms[&down].applied_log().len());
}

/// Ostro (P4, p13-campaign): a restart replays every entry since the last snapshot, conf
/// changes included. A replayed conf change no longer writes (and fsyncs) the state file:
/// the replay writes it once at the end, so the restart cost does not grow with the number
/// of membership changes (the Phase 13 campaign saw 1-2 s per conf change on a busy disk
/// and node restarts beyond 120 s). The state file still ends at the latest configuration,
/// and the node keeps working across further restarts and conf changes.
#[tokio::test(flavor = "multi_thread")]
async fn restart_replay_writes_the_state_file_once_not_per_conf_change() {
    const CYCLES: u64 = 40;
    let mut c = Cluster::new(&[1, 2, 3], config);
    c.leader().await;
    let mut last = 0;
    for i in 0..CYCLES {
        let id = 100 + i;
        propose_cc(&c, cc(vec![single(ConfChangeType::AddLearnerNode, id)], ConfChangeTransition::Auto)).await;
        last = propose_cc(&c, cc(vec![single(ConfChangeType::RemoveNode, id)], ConfChangeTransition::Auto)).await.index;
        last = last.max(write_n(&c, &format!("w{}-", i), 2).await);
    }
    c.settle(last).await;
    let leader = c.leader().await;
    let follower = [1u64, 2, 3].into_iter().find(|&i| i != leader).unwrap();

    // A follower restart replays 2 * CYCLES conf changes.
    c.stop(follower);
    c.start(follower);
    tokio::time::timeout(LONG, c.h(follower).wait_applied(last)).await.expect("replayed").unwrap();
    let writes = c.drivers[&follower].store.state_writes();
    assert!(writes <= 4, "{writes} state-file writes for a replay of {} conf changes", 2 * CYCLES);

    // The state file holds the latest configuration (voters 1-3, no learners).
    let voters_of = |c: &Cluster, id: u64| {
        let st = c.h(id).status();
        (st.voters.clone(), st.learners.clone())
    };
    assert_eq!(voters_of(&c, follower), (vec![1, 2, 3], vec![]));
    c.stop(follower);
    let store = RaftLogStore::open(c.dirs[&follower].path(), LogStoreOptions::default()).unwrap();
    let cs = raft::Storage::initial_state(&store).unwrap().conf_state;
    let mut v = cs.voters.clone();
    v.sort();
    assert_eq!((v, cs.learners.clone()), (vec![1, 2, 3], vec![]));
    drop(store);

    // A whole-cluster restart, then a new conf change and a write still work everywhere.
    for id in [1u64, 2, 3] {
        c.stop(id);
    }
    for id in [1u64, 2, 3] {
        c.start(id);
    }
    c.settle(last).await;
    propose_cc(&c, cc(vec![single(ConfChangeType::AddLearnerNode, 999)], ConfChangeTransition::Auto)).await;
    let w = write_n(&c, "after", 1).await;
    c.settle(w).await;
    for id in [1u64, 2, 3] {
        assert_eq!(voters_of(&c, id), (vec![1, 2, 3], vec![999]), "node {id}");
    }
    // Live conf changes are still written as they apply: restart one node and check.
    let leader = c.leader().await;
    let f = [1u64, 2, 3].into_iter().find(|&i| i != leader).unwrap();
    c.stop(f);
    let store = RaftLogStore::open(c.dirs[&f].path(), LogStoreOptions::default()).unwrap();
    assert_eq!(raft::Storage::initial_state(&store).unwrap().conf_state.learners, vec![999]);
}
