//! Single-voter smoke test: the vendored raft driver elects itself, applies a
//! proposal, and replays its durable log into a fresh state machine on restart.

use std::time::Duration;

use ostro_meta::NullOutbox;
use ostro_raft_driver::{DriverConfig, RaftDriver, StateMachine};

/// Appends every command; `apply` returns the new count as little-endian u64.
#[derive(Default)]
struct CounterSm {
    commands: Vec<Vec<u8>>,
}

impl StateMachine for CounterSm {
    fn apply(&mut self, _index: u64, _term: u64, command: &[u8]) -> Vec<u8> {
        self.commands.push(command.to_vec());
        (self.commands.len() as u64).to_le_bytes().to_vec()
    }
    fn snapshot(&self) -> Vec<u8> {
        (self.commands.len() as u64).to_le_bytes().to_vec()
    }
    fn restore(&mut self, _index: u64, data: &[u8]) -> Result<(), String> {
        let n = u64::from_le_bytes(data.try_into().map_err(|_| "bad snapshot".to_string())?);
        self.commands = vec![Vec::new(); n as usize];
        Ok(())
    }
}

fn count(result: &[u8]) -> u64 {
    u64::from_le_bytes(result.try_into().expect("8-byte count"))
}

async fn start_and_propose(dir: &std::path::Path, data: &[u8]) -> u64 {
    let driver = RaftDriver::start(
        dir,
        DriverConfig::new(1),
        &[1],
        CounterSm::default(),
        NullOutbox,
    )
    .expect("driver starts");
    let h = driver.handle();
    let leader = tokio::time::timeout(Duration::from_secs(5), h.wait_for_leader())
        .await
        .expect("leader within 5 s")
        .expect("leader elected");
    assert_eq!(leader, 1);
    let applied = h.propose(data.to_vec()).await.expect("proposal applied");
    assert!(h.status().is_leader);
    assert!(applied.index > 0);
    driver.stop();
    count(&applied.result)
}

#[tokio::test(flavor = "multi_thread")]
async fn single_voter_applies_proposal() {
    let dir = tempfile::tempdir().unwrap();
    assert_eq!(start_and_propose(dir.path(), b"hello").await, 1);
    // Restart in the same dir with a fresh state machine: the driver replays the
    // durable log (count 1) before serving, so the next proposal yields 2.
    assert_eq!(start_and_propose(dir.path(), b"world").await, 2);
}
