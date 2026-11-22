//! Static-cluster bootstrap placement (12-10, D-P12-14, B2).
//!
//! Epoch 0 means unplaced/Offline: the partition was never led and its log is empty. The
//! controller places every epoch-0 partition once `ClusterInfo` exists, for **any** voter
//! count (a single node gets `replicas = [self]`), with a `Put(Partition)` that is a CAS
//! from version 0 (`expected_version: Some(0)`). A partition already at epoch ≥ 1 is never
//! re-placed, whatever its replica count (growing a pre-Phase-12 RF=1 data dir is Phase 13's
//! `ostro cluster init`).

use ostro_meta::ext::{ELECTION_REASON_BOOTSTRAP, ElectionInfo, put_election_info};
use ostro_meta::{MetaCommand, MetaKey, MetaRecord, PartitionAssignment};

/// Round-robin replica sets: partition `p` gets `[v[(p + i) % n] for i in 0..rf]` over the
/// sorted, de-duplicated `voters` (`rf` clamped to `1..=n`). Deterministic; preferred leaders
/// (`replicas[0]`) are spread evenly (±1). No voters → empty replica sets.
pub fn placement(voters_sorted: &[u64], partitions: u16, rf: usize) -> Vec<Vec<u64>> {
    let mut v: Vec<u64> = voters_sorted.iter().copied().filter(|&n| n != 0).collect();
    v.sort_unstable();
    v.dedup();
    let n = v.len();
    (0..usize::from(partitions))
        .map(|p| {
            if n == 0 {
                return Vec::new();
            }
            (0..rf.clamp(1, n)).map(|i| v[(p + i) % n]).collect()
        })
        .collect()
}

/// The bootstrap record for `current` (an epoch-0 assignment): epoch 1, `leader =
/// replicas[0]`, `isr = [leader]`, `min_insync = min(min_insync, rf)`, ext tag 80 =
/// `Bootstrap`; `isr_version` and other ext entries are kept. `None` when `replicas` is
/// empty or the stored ext is malformed.
pub fn bootstrap_record(
    current: &PartitionAssignment,
    replicas: Vec<u64>,
    min_insync: u8,
    now_ms: u64,
) -> Option<PartitionAssignment> {
    let leader = *replicas.first()?;
    let rf = u8::try_from(replicas.len()).unwrap_or(u8::MAX);
    let info = ElectionInfo {
        reason: ELECTION_REASON_BOOTSTRAP,
        elected_at_ms: now_ms,
        prev_leader: 0,
    };
    let ext = put_election_info(&current.ext, &info).ok()?;
    Some(PartitionAssignment {
        partition: current.partition,
        leader,
        epoch: 1,
        isr: vec![leader],
        replicas,
        isr_version: current.isr_version,
        min_insync: min_insync.clamp(1, rf.max(1)),
        ext,
    })
}

/// The `Put(Partition)` CAS from version 0 that places `record`.
pub fn bootstrap_command(record: PartitionAssignment) -> MetaCommand {
    MetaCommand::Put {
        key: MetaKey::Partition(record.partition),
        expected_version: Some(0),
        record: MetaRecord::Partition(record),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_robin_spreads_leaders() {
        let p = placement(&[3, 1, 2], 6, 3);
        assert_eq!(
            p,
            vec![
                vec![1, 2, 3],
                vec![2, 3, 1],
                vec![3, 1, 2],
                vec![1, 2, 3],
                vec![2, 3, 1],
                vec![3, 1, 2]
            ]
        );
        let p = placement(&[1, 2, 3], 8, 2);
        let mut leaders = [0usize; 4];
        for r in &p {
            assert_eq!(r.len(), 2);
            assert_ne!(r[0], r[1]);
            leaders[r[0] as usize] += 1;
        }
        assert!(leaders[1..].iter().max().unwrap() - leaders[1..].iter().min().unwrap() <= 1);
    }

    #[test]
    fn single_node_and_clamping() {
        assert_eq!(placement(&[7], 3, 3), vec![vec![7], vec![7], vec![7]]);
        assert_eq!(placement(&[1, 2], 1, 5), vec![vec![1, 2]]);
        assert_eq!(placement(&[1, 2], 1, 0), vec![vec![1]]);
        assert_eq!(placement(&[], 2, 3), vec![Vec::<u64>::new(), Vec::new()]);
    }

    #[test]
    fn bootstrap_record_shape() {
        let cur = PartitionAssignment {
            partition: 4,
            leader: 1,
            epoch: 0,
            isr: vec![1],
            replicas: vec![1],
            isr_version: 0,
            min_insync: 1,
            ext: Vec::new(),
        };
        let r = bootstrap_record(&cur, vec![2, 3, 1], 2, 99).unwrap();
        assert_eq!(
            (r.leader, r.epoch, r.isr.clone(), r.min_insync),
            (2, 1, vec![2], 2)
        );
        let r = bootstrap_record(&cur, vec![2], 2, 99).unwrap();
        assert_eq!(r.min_insync, 1);
        let info = ostro_meta::ext::get_election_info(&r.ext).unwrap().unwrap();
        assert_eq!(info.reason, ELECTION_REASON_BOOTSTRAP);
        assert!(bootstrap_record(&cur, Vec::new(), 1, 0).is_none());
    }
}
