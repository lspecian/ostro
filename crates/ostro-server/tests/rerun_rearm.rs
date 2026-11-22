//! Rerun below the control plane (plan 11-07, OPS-02, D-P11-6/18) with the
//! `guests/p11-rerun{,-v2}` components. The rerun StartRun is built by hand here (the
//! control plane that builds it is 11-10):
//!
//! - the source `pipeline {fail_at: 3, sleep_between_ms: 200}` on `p11-rerun` fails at `s3`;
//! - the rerun copies events `0..cut` (cut = right after the `TimerStarted` between `s2` and
//!   `s3`, so the timer is open), carries `__ostro/rerun_from=<cut>`, and runs on
//!   `p11-rerun-v2` (where `s3` never fails);
//! - the executor's pre-activation re-arm schedules the open timer, it fires, `s3..s5` run
//!   live and the result is `5`; `s1`/`s2` are never re-executed (their events are the
//!   copied rows);
//! - replay markers: copied rows keep the source's `attempt`, the first live engine event
//!   has `attempt != 0`, an apply-originated `TimerFired` has 0;
//! - failpoint `rerun::before_rearm` (feature `failpoints`, run with `--test-threads=1`):
//!   the job ends before the re-arm, leaving the run `Running` as a crash there would;
//!   after a restart on the same data dir the re-arm runs and the run completes (in-process
//!   per D-P11-32);
//! - the same rerun on a `workers` queue re-arms through the remote runner's shared prelude.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;
#[path = "common/p11.rs"]
mod p11;
#[path = "common/p7_workers.rs"]
mod p7_workers;

use std::sync::Arc;
use std::time::Duration;

use ostro_meta::records::PLACEMENT_WORKERS;
use ostro_meta::{MetaCommand, MetaRecord, MetaResult, QueuePolicy};
use ostro_partition::store::keys::event_key;
use ostro_partition::store::values::{RunRecord, RunStatus};
use ostro_partition::{Table, query};
use ostro_server::rerun::{analyze_cut, rerun_from_tag};
use ostro_types::RunId;
use ostro_types::command::{JournalEvent, RERUN_FROM_TAG};
use ostro_types::journal::{Event, EventBody, EventKind};
use p7_workers::ServerWorkers;
use p11::P11Server;

const LIMIT: Duration = Duration::from_secs(90);
const SOURCE_INPUT: &[u8] = br#"{"fail_at": 3, "sleep_between_ms": 200}"#;

struct Modules {
    v1: [u8; 32],
    v2: [u8; 32],
}

async fn server() -> (P11Server, Modules) {
    let s = P11Server::builder().start().await;
    s.wait_ready(Duration::from_secs(20)).await;
    let v1 = s.deploy_guest("p11-rerun", None).await;
    let v2 = s.deploy_guest("p11-rerun-v2", None).await;
    (s, Modules { v1, v2 })
}

async fn terminal(s: &P11Server, run: &RunId) -> RunRecord {
    s.wait_record(run, LIMIT, |r| r.status.is_terminal()).await
}

/// The raw stored `events` rows of `run`, in seq order.
fn raw_rows(s: &P11Server, run: &RunId) -> Vec<Vec<u8>> {
    let reader = s.handle_of(run).reader();
    let mut out = Vec::new();
    for seq in 0.. {
        match reader
            .get(Table::Events, &event_key(run, seq))
            .expect("read")
        {
            Some(v) => out.push(v.to_vec()),
            None => break,
        }
    }
    out
}

/// Field 4 (`attempt`, fixed64) of a stored prost `JournalEvent` (prost is not a dev
/// dependency here, so the four-field message is walked by hand).
fn attempt_of(mut b: &[u8]) -> u64 {
    fn varint(b: &mut &[u8]) -> u64 {
        let mut v = 0u64;
        for shift in (0..64).step_by(7) {
            let (x, rest) = b.split_first().expect("varint");
            *b = rest;
            v |= u64::from(x & 0x7f) << shift;
            if x & 0x80 == 0 {
                break;
            }
        }
        v
    }
    let mut attempt = 0;
    while !b.is_empty() {
        let key = varint(&mut b);
        match (key >> 3, key & 7) {
            (4, 1) => {
                let (x, rest) = b.split_at(8);
                attempt = u64::from_le_bytes(x.try_into().expect("8 bytes"));
                b = rest;
            }
            (_, 0) => {
                varint(&mut b);
            }
            (_, 1) => b = &b[8..],
            (_, 2) => {
                let n = usize::try_from(varint(&mut b)).expect("len");
                b = &b[n..];
            }
            (_, 5) => b = &b[4..],
            other => panic!("unexpected wire key {other:?}"),
        }
    }
    attempt
}

/// The stored rows of `run` as `JournalEvent`s (attempt included).
fn rows(s: &P11Server, run: &RunId) -> Vec<JournalEvent> {
    let h = s.handle_of(run);
    let raw = raw_rows(s, run);
    let evs = query::journal_rows(&h.reader(), run, 0, usize::MAX).expect("journal rows");
    assert_eq!(evs.len(), raw.len());
    evs.into_iter()
        .zip(raw)
        .map(|((seq, kind, payload), r)| JournalEvent {
            seq,
            kind,
            payload: Some(payload),
            attempt: attempt_of(&r),
        })
        .collect()
}

/// Engine-written (guest command) events, codes 16-63; inputs (64+) come from apply.
fn is_command(r: &JournalEvent) -> bool {
    (16..64).contains(&r.kind)
}

fn steps_named(j: &[Event], name: &str) -> usize {
    j.iter()
        .filter(|e| matches!(&e.body, EventBody::StepCompleted(s) if s.name == name))
        .count()
}

/// Runs the failing source to `Failed`; returns it with its cut (one past the open timer).
async fn failed_source(s: &P11Server, m: &Modules) -> (RunId, u64) {
    let src = s
        .start_raw("pipeline", 1, SOURCE_INPUT, &m.v1, |_| {})
        .await;
    let rec = terminal(s, &src).await;
    assert_eq!(rec.status, RunStatus::Failed, "{rec:?}");
    let j = s.journal(&src).await;
    let timer = j
        .iter()
        .find(|e| e.body.kind() == EventKind::TimerStarted)
        .expect("TimerStarted")
        .seq;
    let cut = timer + 1;
    let plan = analyze_cut(&j, cut).expect("a cut after an open timer is safe");
    assert_eq!((plan.open_timers, plan.open_waits), (1, 0));
    (src, cut)
}

/// Submits the hand-built rerun of `src` from `cut` on `p11-rerun-v2`.
async fn submit_rerun(s: &P11Server, m: &Modules, src: &RunId, cut: u64, queue: &str) -> RunId {
    let src_rows = rows(s, src);
    let src_rec = s.run_record(src);
    let cut_us = usize::try_from(cut).expect("cut");
    let queue = queue.to_owned();
    let src_id = *src;
    let run = s
        .start_raw("pipeline", 2, SOURCE_INPUT, &m.v2, move |st| {
            st.queue = queue;
            st.rng_seed = src_rec.rng_seed.clone();
            st.run_started = Some(src_rows[0].clone());
            st.rerun_prefix = src_rows[1..cut_us].to_vec();
            st.rerun_from_seq = cut;
            st.retry_of = src_id.as_bytes().to_vec();
            st.tags = vec![format!("{RERUN_FROM_TAG}{cut}")];
        })
        .await;
    let rec = s.run_record(&run);
    assert_eq!(rerun_from_tag(&rec.tags), Some(cut));
    run
}

/// The rerun completed from the cut without re-executing the copied steps.
async fn assert_rerun_completed(s: &P11Server, src: &RunId, run: &RunId, cut: u64) {
    let rec = terminal(s, run).await;
    assert_eq!(rec.status, RunStatus::Completed, "{rec:?}");
    let out: serde_json::Value =
        serde_json::from_slice(&s.result_bytes(run).await.expect("result")).expect("json");
    assert_eq!(out, serde_json::json!(5));

    let cut_us = usize::try_from(cut).expect("cut");
    let src_rows = rows(s, src);
    let new_rows = rows(s, run);
    assert_eq!(
        raw_rows(s, run)[..cut_us],
        raw_rows(s, src)[..cut_us],
        "events 0..cut are the source rows, byte for byte"
    );
    assert_eq!(new_rows[..cut_us], src_rows[..cut_us]);
    let j = s.journal(run).await;
    for (name, n) in [("s1", 1), ("s2", 1), ("s3", 1), ("s4", 1), ("s5", 1)] {
        assert_eq!(
            steps_named(&j, name),
            n,
            "{name}: copied steps never re-run"
        );
    }
    // The re-armed timer fired: the first event after the cut.
    let fired = &j[cut_us];
    assert!(
        matches!(fired.body, EventBody::TimerFired(t) if t.op == cut - 1),
        "{:?}",
        fired.body
    );
    // Replay markers (D-P11-18).
    assert_eq!(new_rows[cut_us].attempt, 0, "apply-originated TimerFired");
    for r in &new_rows[1..cut_us] {
        assert_eq!(
            is_command(r),
            r.attempt != 0,
            "copied rows keep the source's stamp"
        );
    }
    let first_live = new_rows[cut_us..]
        .iter()
        .find(|r| is_command(r))
        .expect("a live engine event");
    assert_ne!(
        first_live.attempt, 0,
        "the first live engine event is stamped"
    );
    assert_eq!(
        first_live.attempt >> 32,
        u64::from(rec.current_attempt.expect("attempt").0)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rerun_rearms_the_open_timer_and_completes_on_the_fixed_module() {
    let (mut s, m) = server().await;
    let (src, cut) = failed_source(&s, &m).await;
    // The source (not a rerun) carries no tag; its stamps are exact.
    assert_eq!(rerun_from_tag(&s.run_record(&src).tags), None);
    let src_rows = rows(&s, &src);
    assert!(
        src_rows[1..]
            .iter()
            .all(|r| is_command(r) == (r.attempt != 0))
    );
    let run = submit_rerun(&s, &m, &src, cut, "default").await;
    assert_rerun_completed(&s, &src, &run, cut).await;
    s.shutdown().await;
}

#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_before_rearm_still_fires_the_timer_after_restart() {
    let scenario = fail::FailScenario::setup();
    let (mut s, m) = server().await;
    let (src, cut) = failed_source(&s, &m).await;
    fail::cfg("rerun::before_rearm", "return").expect("cfg");
    let t0 = std::time::Instant::now();
    let run = submit_rerun(&s, &m, &src, cut, "default").await;
    s.wait_record(&run, LIMIT, |r| r.status == RunStatus::Running)
        .await;
    // The timer is past due: had it been re-armed it would fire at once.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let rec = s.run_record(&run);
    assert_eq!(rec.next_seq, cut, "no re-arm, no live event: {rec:?}");
    assert_eq!(rec.status, RunStatus::Running, "left Running as by a crash");
    fail::remove("rerun::before_rearm");
    let t1 = std::time::Instant::now();
    s.restart().await;
    s.wait_ready(Duration::from_secs(20)).await;
    assert_rerun_completed(&s, &src, &run, cut).await;
    eprintln!(
        "kill-test timings: blocked phase {:?}, restart to completion {:?}",
        t1 - t0,
        t1.elapsed()
    );
    s.shutdown().await;
    scenario.teardown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rerun_on_a_worker_queue_rearms_through_the_remote_runner() {
    let (mut s, m) = server().await;
    let (src, cut) = failed_source(&s, &m).await;
    let meta = s.h().meta();
    let record = MetaRecord::QueuePolicy(QueuePolicy {
        queue: "wq".into(),
        placement: Some(PLACEMENT_WORKERS),
        ..QueuePolicy::default()
    });
    let key = record.key();
    let expected_version = meta.view().read(|st| st.get(&key).map(|v| v.version));
    let r = meta
        .propose(MetaCommand::Put {
            key,
            expected_version,
            record,
        })
        .await
        .expect("meta put");
    assert!(matches!(r, MetaResult::Ok { .. }), "{r:?}");
    let ws = ServerWorkers::new(Arc::clone(s.h().p7())).await;
    let mut w = ws.spawn("w1", 2, &[]).await;

    let run = submit_rerun(&s, &m, &src, cut, "wq").await;
    assert_rerun_completed(&s, &src, &run, cut).await;
    let id = run.to_string();
    w.wait_event(p7_workers::T, |e| {
        matches!(e, ostro_worker::WorkerEvent::ActivationStarted { run_id, .. } if *run_id == id)
    })
    .await;
    drop(w);
    drop(ws);
    s.shutdown().await;
}
