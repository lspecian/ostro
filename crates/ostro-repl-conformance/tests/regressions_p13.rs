//! 13-18 regression tests: every fix 13-18 made has one here, naming what found it.
//!
//! | test | fix | found by |
//! |------|-----|----------|
//! | `restart_of_retired_module_refused` | retry-restart / rerun / DLQ / bulk restart refuse a retired module (`rerun::check_restart_module`) | 13-15 `protects_retryable_terminal_runs` (restart-path finding) |
//! | `restart_lands_on_a_led_partition` | a restart's new run goes to a partition the serving node leads (`rerun::restart_partition`) | 13-15 `protects_retryable_terminal_runs` (up to 40 `not_leader` attempts), 13-12 / 13-17 handover |
//! | `gates_answer_409` | every gate answers 409 `feature_not_enabled` | 13-14 / 13-16 / 13-17 handover (409 vs 412) |
//! | `outcome_stream_replays_and_fail_stops` | the `OutcomeRecord` producer (`harness::outcomes`) | 13-08 / 13-10 handover (`no_reject_continue` had no producer) |
//!
//! Unit-level regressions live with their code: `ostro-server` `p13_routing_unit`
//! (`activation_passes_the_meta_barrier_first`, `start_with_a_meta_token_passes_the_meta_barrier`,
//! `migrate_gate_is_decided_once_at_the_entry_node`), `p13::promote::tests::
//! self_cluster_addr_keeps_the_advertised_address`, `rerun::tests::pick_led_stays_on_led_partitions`,
//! `ostro-cluster` `placement_controller::rebalance_moves_carry_the_injected_clock`.
//! Run with `--test-threads=1`.

use bytes::Bytes;
use ostro_partition::apply::{ApplyOutcome, InvalidCode, RejectReason};
use ostro_repl_conformance::harness::outcomes::partition_outcomes;
use ostro_repl_conformance::oracle::disk::{LogScan, ScannedRecord};
use ostro_repl_conformance::oracle::p13_upgrade::no_reject_continue;
use ostro_repl_conformance::scenarios::integration_p13;
use ostro_repl_conformance::scenarios::runner::{Outcome, run_scenario};
use ostro_types::command::{RepinRun, encode_command};
use ostro_types::{Command, Epoch, Offset, PartitionCount, PartitionId, RunId};

async fn run(name: &str) {
    let s = integration_p13::regressions()
        .into_iter()
        .find(|s| s.name == name)
        .unwrap_or_else(|| panic!("no regression {name}"));
    let started = std::time::Instant::now(); // clock-exempt: report timing
    let outcome = run_scenario(&s).await;
    println!("{name}: {} in {:?}", outcome.label(), started.elapsed());
    assert!(
        matches!(outcome, Outcome::Passed),
        "{name}: {}",
        outcome.label()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn restart_of_retired_module_refused() {
    run("restart_of_retired_module_refused").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn restart_lands_on_a_led_partition() {
    run("restart_lands_on_a_led_partition").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn gates_answer_409() {
    run("gates_answer_409").await;
}

fn record(base: u64, commands: Vec<Bytes>) -> ScannedRecord {
    ScannedRecord {
        base: Offset(base),
        end: Offset(base + commands.len() as u64),
        epoch: Epoch(0),
        envelope: Bytes::new(),
        commands,
    }
}

fn repin() -> Bytes {
    encode_command(&Command::repin_run(RepinRun {
        run_id: RunId::new_v7(PartitionId(0)).as_bytes().to_vec(),
        from_sha: vec![1; 32],
        to_sha: vec![2; 32],
        expected_next_seq: 1,
        requested_at_ms: 1,
    }))
}

/// The producer replays real apply verdicts and stops at a fail-stop; a stream with a
/// reject-and-continue record is flagged by the oracle (its input is now produced).
#[test]
fn outcome_stream_replays_and_fail_stops() {
    let known = repin();
    let mut newer = known.to_vec();
    newer.extend_from_slice(&[0xC0, 0x3E, 0x01]); // field 1000 = 1: unknown to this build
    let scan = LogScan {
        node: 7,
        partition: PartitionId(0),
        records: vec![
            record(0, vec![known.clone()]),
            record(1, vec![Bytes::from(newer), known]),
        ],
        stopped: None,
    };
    let st = partition_outcomes(&scan, PartitionCount::new(1).unwrap()).expect("replay");
    assert_eq!(st.records.len(), 1, "{st:?}");
    assert_eq!(st.records[0].offset, 0);
    let (off, why) = st.halted.clone().expect("fail-stop");
    assert_eq!(off, 1, "{why}");
    assert!(why.contains("newer"), "{why}");
    assert!(no_reject_continue(&st.records).is_ok());
    // A hand-made reject-and-continue record: the oracle flags it.
    let mut rec = st.records[0].clone();
    rec.outcome = ApplyOutcome::Rejected(RejectReason::Invalid(InvalidCode::Empty));
    assert!(!no_reject_continue(&[rec]).is_ok());
}
