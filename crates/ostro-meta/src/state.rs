//! The metadata state and its deterministic, versioned apply (META-03).
//!
//! `apply` always validates first and mutates only on success, so a rejected
//! command leaves the state byte-identical.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::command::{MetaCommand, MetaKey, MetaRecord, MetaResult};
use crate::ext::ElectionInfo;
use crate::records::{
    ActiveVersion, ApiToken, CLUSTER_FORMAT, ClusterInfo, Connection, EgressPolicy, Grant, Member,
    ModuleRecord, PartitionAssignment, QueueConfig, QueuePolicy, Schedule, SealedSecret,
    TOKEN_KEY_PLAINTEXT_ID, TOKEN_KEY_SECRET, Tenant, Versioned, Webhook, WorkflowPolicy,
    WorkflowVersion,
};
use crate::validate::validate_record;

/// All metadata records. One map per kind; `BTreeMap` keeps iteration (and so
/// snapshots) deterministic.
///
/// Field order is the positional snapshot encoding: maps follow
/// [`crate::MetaKind`] order and new maps are appended last (D20).
#[derive(Default, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetaState {
    cluster: Option<Versioned<ClusterInfo>>,
    members: BTreeMap<u64, Versioned<Member>>,
    partitions: BTreeMap<u16, Versioned<PartitionAssignment>>,
    modules: BTreeMap<[u8; 32], Versioned<ModuleRecord>>,
    workflows: BTreeMap<(String, u32), Versioned<WorkflowVersion>>,
    actives: BTreeMap<String, Versioned<ActiveVersion>>,
    queues: BTreeMap<String, Versioned<QueueConfig>>,
    schedules: BTreeMap<String, Versioned<Schedule>>,
    tokens: BTreeMap<String, Versioned<ApiToken>>,
    tenants: BTreeMap<String, Versioned<Tenant>>,
    secrets: BTreeMap<String, Versioned<SealedSecret>>,
    grants: BTreeMap<String, Versioned<Grant>>,
    connections: BTreeMap<String, Versioned<Connection>>,
    egress: BTreeMap<String, Versioned<EgressPolicy>>,
    workflow_policies: BTreeMap<String, Versioned<WorkflowPolicy>>,
    queue_policies: BTreeMap<String, Versioned<QueuePolicy>>,
    webhooks: BTreeMap<String, Versioned<Webhook>>,
}

/// An ISR in an `ElectLeader` / `AlterIsr` command: non-empty, no node 0, no duplicates.
fn check_isr_shape(isr: &[u64]) -> Result<(), String> {
    check_node_set("the ISR", isr)
}

/// A node set (`what` names it in the message): non-empty, no node 0, no duplicates.
fn check_node_set(what: &str, set: &[u64]) -> Result<(), String> {
    if set.is_empty() {
        return Err(format!("{what} must not be empty"));
    }
    if set.contains(&0) {
        return Err(format!("{what} {set:?} contains node 0"));
    }
    for (i, n) in set.iter().enumerate() {
        if set[..i].contains(n) {
            return Err(format!("{what} {set:?} lists node {n} twice"));
        }
    }
    Ok(())
}

fn wrap<T: Clone>(v: &Versioned<T>, f: impl FnOnce(T) -> MetaRecord) -> Versioned<MetaRecord> {
    v.clone().map(f)
}

impl MetaState {
    /// Apply one command at Raft `index`. Deterministic; never panics.
    pub fn apply(&mut self, index: u64, cmd: MetaCommand) -> MetaResult {
        match cmd {
            MetaCommand::InitCluster {
                cluster_id,
                node_id,
                partitions,
                created_at_ms,
                token_key,
            } => self.init_cluster(
                index,
                cluster_id,
                node_id,
                partitions,
                created_at_ms,
                token_key,
            ),
            MetaCommand::Put {
                key,
                expected_version,
                record,
            } => self.put(index, key, expected_version, record),
            MetaCommand::Delete {
                key,
                expected_version,
            } => self.delete(key, expected_version),
            MetaCommand::SetPartitionLeader {
                partition,
                epoch,
                leader,
                isr,
            } => self.set_partition_leader(index, partition, epoch, leader, isr),
            // Phase 12 (D-P12-7, 12-05): validated at apply, independent of the controller.
            MetaCommand::ElectLeader {
                partition,
                epoch,
                leader,
                isr,
                expected_isr_version,
                info,
            } => self.elect_leader(
                index,
                partition,
                epoch,
                leader,
                isr,
                expected_isr_version,
                &info,
            ),
            MetaCommand::AlterIsr {
                partition,
                epoch,
                expected_isr_version,
                isr,
            } => self.alter_isr(index, partition, epoch, expected_isr_version, isr),
            MetaCommand::ResignLeader {
                partition,
                epoch,
                node,
                info,
            } => self.resign_leader(index, partition, epoch, node, &info),
            // Phase 13 placeholder (13-01, P-15): never proposed.
            MetaCommand::ReservedP12_7 {} => MetaResult::Invalid("reserved".into()),
            // Phase 13 (D-P13-12): shape 13-01, apply 13-10.
            MetaCommand::AlterReplicas {
                partition,
                epoch,
                expected_isr_version,
                replicas,
                isr,
                min_insync,
                reassignment,
            } => self.alter_replicas(
                index,
                partition,
                epoch,
                expected_isr_version,
                replicas,
                isr,
                min_insync,
                reassignment.as_ref(),
            ),
        }
    }

    fn init_cluster(
        &mut self,
        index: u64,
        cluster_id: String,
        node_id: u64,
        partitions: u16,
        created_at_ms: u64,
        token_key: [u8; 32],
    ) -> MetaResult {
        if let Some(existing) = &self.cluster {
            if existing.value.partitions != partitions {
                return MetaResult::Invalid(format!(
                    "partition count mismatch: cluster has {} partitions, InitCluster asked for {}",
                    existing.value.partitions, partitions
                ));
            }
            return MetaResult::Ok {
                version: existing.version,
            };
        }
        if partitions == 0 {
            return MetaResult::Invalid("partition count must be at least 1".into());
        }
        if let Some((&p, _)) = self.partitions.range(partitions..).next() {
            return MetaResult::Invalid(format!(
                "partition {p} already assigned but InitCluster asked for {partitions} partitions"
            ));
        }
        fn v<T>(index: u64, value: T) -> Versioned<T> {
            Versioned {
                version: 1,
                updated_index: index,
                value,
            }
        }
        self.cluster = Some(v(
            index,
            ClusterInfo {
                cluster_id,
                partitions,
                format: CLUSTER_FORMAT,
                created_at_ms,
                ext: Vec::new(),
            },
        ));
        self.members.entry(node_id).or_insert_with(|| {
            v(
                index,
                Member {
                    node_id,
                    addr: String::new(),
                    joined_at_ms: created_at_ms,
                    cluster_addr: None,
                    repl_addr: None,
                    ext: Vec::new(),
                },
            )
        });
        for partition in 0..partitions {
            self.partitions
                .entry(partition)
                .or_insert_with(|| Versioned {
                    version: 0,
                    updated_index: index,
                    value: PartitionAssignment {
                        partition,
                        leader: node_id,
                        epoch: 0,
                        isr: vec![node_id],
                        // Phase 12 F7 defaults: RF 1, ISR CAS token 0.
                        replicas: vec![node_id],
                        isr_version: 0,
                        min_insync: 1,
                        ext: Vec::new(),
                    },
                });
        }
        self.secrets
            .entry(TOKEN_KEY_SECRET.to_string())
            .or_insert_with(|| {
                v(
                    index,
                    SealedSecret {
                        name: TOKEN_KEY_SECRET.to_string(),
                        ciphertext: token_key.to_vec(),
                        key_id: TOKEN_KEY_PLAINTEXT_ID.to_string(),
                        ext: Vec::new(),
                    },
                )
            });
        MetaResult::Ok { version: 1 }
    }

    fn put(
        &mut self,
        index: u64,
        key: MetaKey,
        expected: Option<u64>,
        record: MetaRecord,
    ) -> MetaResult {
        let natural = record.key();
        if natural != key {
            return MetaResult::Invalid(format!(
                "key {key:?} does not match record key {natural:?}"
            ));
        }
        if let Err(msg) = validate_record(&record) {
            return MetaResult::Invalid(format!("{:?}: {msg}", key.kind()));
        }
        let current = self.get(&key);
        let version = match (expected, &current) {
            (None, Some(cur)) => {
                return MetaResult::Conflict {
                    current: cur.version,
                };
            }
            (Some(_), None) => return MetaResult::NotFound,
            (Some(e), Some(cur)) if e != cur.version => {
                return MetaResult::StaleVersion {
                    current: cur.version,
                };
            }
            (None, None) => match &record {
                MetaRecord::Partition(p) => u64::from(p.epoch),
                _ => 1,
            },
            (Some(e), Some(cur)) => match (&record, &cur.value) {
                // Partition records: version == epoch, and epochs never regress.
                (MetaRecord::Partition(p), _) if u64::from(p.epoch) <= cur.version => {
                    return MetaResult::StaleVersion {
                        current: cur.version,
                    };
                }
                (MetaRecord::Partition(p), _) => u64::from(p.epoch),
                (MetaRecord::Cluster(new), MetaRecord::Cluster(old))
                    if new.partitions != old.partitions =>
                {
                    return MetaResult::Invalid(format!(
                        "partition count mismatch: cluster has {} partitions, Put asked for {}",
                        old.partitions, new.partitions
                    ));
                }
                _ => e + 1,
            },
        };
        self.insert(Versioned {
            version,
            updated_index: index,
            value: record,
        });
        MetaResult::Ok { version }
    }

    fn delete(&mut self, key: MetaKey, expected: u64) -> MetaResult {
        match self.get(&key) {
            None => MetaResult::NotFound,
            Some(cur) if cur.version != expected => MetaResult::StaleVersion {
                current: cur.version,
            },
            Some(cur) => {
                self.remove(&key);
                MetaResult::Ok {
                    version: cur.version,
                }
            }
        }
    }

    fn set_partition_leader(
        &mut self,
        index: u64,
        partition: u16,
        epoch: u32,
        leader: u64,
        isr: Vec<u64>,
    ) -> MetaResult {
        let Some(cur) = self.partitions.get_mut(&partition) else {
            return MetaResult::NotFound;
        };
        let (cur_epoch, new_epoch) = (cur.version, u64::from(epoch));
        if new_epoch < cur_epoch {
            return MetaResult::StaleVersion { current: cur_epoch };
        }
        if new_epoch == cur_epoch {
            return if cur.value.leader == leader && cur.value.isr == isr {
                MetaResult::Ok { version: cur_epoch }
            } else {
                MetaResult::Conflict { current: cur_epoch }
            };
        }
        // Validation is unchanged from Phase 4 (12-RESEARCH F8). The Phase 12
        // F7 fields (replicas, isr_version, min_insync) and ext are stored
        // only and carried over untouched.
        let prev = std::mem::take(&mut cur.value);
        *cur = Versioned {
            version: new_epoch,
            updated_index: index,
            value: PartitionAssignment {
                partition,
                leader,
                epoch,
                isr,
                replicas: prev.replicas,
                isr_version: prev.isr_version,
                min_insync: prev.min_insync,
                ext: prev.ext,
            },
        };
        MetaResult::Ok { version: new_epoch }
    }

    /// `ElectLeader` (12-05, SC3: a leader is elected only from the published ISR).
    ///
    /// Checks, in order: the partition exists (`NotFound`); `epoch` exceeds the current
    /// epoch (`StaleVersion { current: epoch }`); `expected_isr_version` equals the
    /// current `isr_version` (`StaleVersion { current: isr_version }`); the new ISR is
    /// non-empty, has no duplicates or 0, and is a subset of the current published ISR,
    /// also for `leader = 0` (offline); a non-zero `leader` is in the published ISR and in
    /// the new ISR (`Invalid`). On success the epoch (= version) becomes `epoch`,
    /// `isr_version` is bumped and ext tag 80 is set from `info` verbatim (never
    /// validated); `replicas`, `min_insync` and every other ext entry are kept.
    #[allow(clippy::too_many_arguments)]
    fn elect_leader(
        &mut self,
        index: u64,
        partition: u16,
        epoch: u32,
        leader: u64,
        isr: Vec<u64>,
        expected_isr_version: u64,
        info: &ElectionInfo,
    ) -> MetaResult {
        let Some(cur) = self.partitions.get(&partition) else {
            return MetaResult::NotFound;
        };
        if u64::from(epoch) <= cur.version {
            return MetaResult::StaleVersion {
                current: cur.version,
            };
        }
        if expected_isr_version != cur.value.isr_version {
            return MetaResult::StaleVersion {
                current: cur.value.isr_version,
            };
        }
        if let Err(msg) = check_isr_shape(&isr) {
            return MetaResult::Invalid(format!("ElectLeader p{partition}: {msg}"));
        }
        let published = &cur.value.isr;
        if leader != 0 && !published.contains(&leader) {
            return MetaResult::Invalid(format!(
                "ElectLeader p{partition}: leader {leader} is not in the published ISR {published:?}"
            ));
        }
        if let Some(n) = isr.iter().find(|n| !published.contains(n)) {
            return MetaResult::Invalid(format!(
                "ElectLeader p{partition}: ISR member {n} is not in the published ISR {published:?} (an election only shrinks the ISR)"
            ));
        }
        if leader != 0 && !isr.contains(&leader) {
            return MetaResult::Invalid(format!(
                "ElectLeader p{partition}: leader {leader} is not in the new ISR {isr:?}"
            ));
        }
        let Some(isr_version) = cur.value.isr_version.checked_add(1) else {
            return MetaResult::Invalid(format!("ElectLeader p{partition}: isr_version overflow"));
        };
        let ext = match crate::ext::put_election_info(&cur.value.ext, info) {
            Ok(ext) => ext,
            Err(e) => {
                return MetaResult::Invalid(format!(
                    "ElectLeader p{partition}: stored ext is malformed: {e}"
                ));
            }
        };
        let cur = self.partitions.get_mut(&partition).expect("checked above");
        let prev = std::mem::take(&mut cur.value);
        *cur = Versioned {
            version: u64::from(epoch),
            updated_index: index,
            value: PartitionAssignment {
                partition,
                leader,
                epoch,
                isr,
                replicas: prev.replicas,
                isr_version,
                min_insync: prev.min_insync,
                ext,
            },
        };
        MetaResult::Ok {
            version: u64::from(epoch),
        }
    }

    /// `AlterIsr` (12-05): the leader publishes a new ISR without an epoch bump.
    ///
    /// Checks: the partition exists (`NotFound`); `epoch` equals the current epoch
    /// (`StaleVersion { current: epoch }`); `expected_isr_version` matches
    /// (`StaleVersion { current: isr_version }`); the ISR is non-empty, has no duplicates
    /// or 0, is a subset of `replicas`, and contains the current leader (`Invalid`; an
    /// offline partition, leader 0, cannot alter its ISR). On success `isr_version` is
    /// bumped; epoch, version, leader, replicas and ext are unchanged.
    fn alter_isr(
        &mut self,
        index: u64,
        partition: u16,
        epoch: u32,
        expected_isr_version: u64,
        isr: Vec<u64>,
    ) -> MetaResult {
        let Some(cur) = self.partitions.get_mut(&partition) else {
            return MetaResult::NotFound;
        };
        if u64::from(epoch) != cur.version {
            return MetaResult::StaleVersion {
                current: cur.version,
            };
        }
        if expected_isr_version != cur.value.isr_version {
            return MetaResult::StaleVersion {
                current: cur.value.isr_version,
            };
        }
        if let Err(msg) = check_isr_shape(&isr) {
            return MetaResult::Invalid(format!("AlterIsr p{partition}: {msg}"));
        }
        if let Some(n) = isr.iter().find(|n| !cur.value.replicas.contains(n)) {
            return MetaResult::Invalid(format!(
                "AlterIsr p{partition}: ISR member {n} is not a replica {:?}",
                cur.value.replicas
            ));
        }
        if !isr.contains(&cur.value.leader) {
            return MetaResult::Invalid(format!(
                "AlterIsr p{partition}: leader {} is not in the new ISR {isr:?}",
                cur.value.leader
            ));
        }
        let Some(isr_version) = cur.value.isr_version.checked_add(1) else {
            return MetaResult::Invalid(format!("AlterIsr p{partition}: isr_version overflow"));
        };
        cur.value.isr = isr;
        cur.value.isr_version = isr_version;
        cur.updated_index = index;
        MetaResult::Ok {
            version: cur.version,
        }
    }

    /// `AlterReplicas` (13-10, D-P13-12): reassign a partition's replica set (KIP-455 shape)
    /// without an epoch bump.
    ///
    /// `isr_version` is the CAS token for the whole replica-and-ISR state: every command that
    /// changes `replicas`, `isr` or `min_insync` (`ElectLeader`, `AlterIsr`, `ResignLeader`,
    /// `AlterReplicas`) bumps it, so a proposer that read version `v` changes exactly the
    /// state it observed.
    ///
    /// Checks, in order: the partition exists (`NotFound`); it is placed, `epoch != 0`
    /// (`Invalid`: bootstrap placement owns unplaced partitions, D-P13-29c); `epoch` equals
    /// the current epoch (`StaleVersion { current: epoch }`); `expected_isr_version` matches
    /// (`StaleVersion { current: isr_version }`); `replicas` and `isr` are non-empty, have no
    /// duplicates or 0, and `isr ⊆ replicas`; a non-zero current leader is in `replicas` and
    /// in `isr`; `1 <= min_insync <= |isr|` (all `Invalid`). On success `replicas`, `isr` and
    /// `min_insync` are replaced, `isr_version` is bumped, and ext tag 100 is set from
    /// `reassignment` verbatim (`None` removes it; never validated, 05-20 rule 5). Epoch,
    /// version, leader and every other ext entry are unchanged. Any failure changes nothing.
    #[allow(clippy::too_many_arguments)]
    fn alter_replicas(
        &mut self,
        index: u64,
        partition: u16,
        epoch: u32,
        expected_isr_version: u64,
        replicas: Vec<u64>,
        isr: Vec<u64>,
        min_insync: u8,
        reassignment: Option<&crate::ext::Reassignment>,
    ) -> MetaResult {
        let Some(cur) = self.partitions.get(&partition) else {
            return MetaResult::NotFound;
        };
        if cur.version == 0 || epoch == 0 {
            return MetaResult::Invalid(format!(
                "AlterReplicas p{partition}: the partition is unplaced (epoch 0)"
            ));
        }
        if u64::from(epoch) != cur.version {
            return MetaResult::StaleVersion {
                current: cur.version,
            };
        }
        if expected_isr_version != cur.value.isr_version {
            return MetaResult::StaleVersion {
                current: cur.value.isr_version,
            };
        }
        if let Err(msg) = check_node_set("the replica set", &replicas) {
            return MetaResult::Invalid(format!("AlterReplicas p{partition}: {msg}"));
        }
        if let Err(msg) = check_isr_shape(&isr) {
            return MetaResult::Invalid(format!("AlterReplicas p{partition}: {msg}"));
        }
        if let Some(n) = isr.iter().find(|n| !replicas.contains(n)) {
            return MetaResult::Invalid(format!(
                "AlterReplicas p{partition}: ISR member {n} is not in the replicas {replicas:?}"
            ));
        }
        let leader = cur.value.leader;
        if leader != 0 && !replicas.contains(&leader) {
            return MetaResult::Invalid(format!(
                "AlterReplicas p{partition}: leader {leader} is not in the replicas {replicas:?}"
            ));
        }
        if leader != 0 && !isr.contains(&leader) {
            return MetaResult::Invalid(format!(
                "AlterReplicas p{partition}: leader {leader} is not in the ISR {isr:?}"
            ));
        }
        if min_insync == 0 || usize::from(min_insync) > isr.len() {
            return MetaResult::Invalid(format!(
                "AlterReplicas p{partition}: min_insync {min_insync} must be in 1..={}",
                isr.len()
            ));
        }
        let Some(isr_version) = cur.value.isr_version.checked_add(1) else {
            return MetaResult::Invalid(format!(
                "AlterReplicas p{partition}: isr_version overflow"
            ));
        };
        let ext = match reassignment {
            Some(r) => crate::ext::put_reassignment(&cur.value.ext, r),
            None => crate::ext::ext_remove(&cur.value.ext, crate::ext::EXT_REASSIGNMENT),
        };
        let ext = match ext {
            Ok(ext) => ext,
            Err(e) => {
                return MetaResult::Invalid(format!(
                    "AlterReplicas p{partition}: stored ext is malformed: {e}"
                ));
            }
        };
        let cur = self.partitions.get_mut(&partition).expect("checked above");
        cur.value.replicas = replicas;
        cur.value.isr = isr;
        cur.value.min_insync = min_insync;
        cur.value.isr_version = isr_version;
        cur.value.ext = ext;
        cur.updated_index = index;
        MetaResult::Ok {
            version: cur.version,
        }
    }

    /// `ResignLeader` (12-05): `node`, the leader at `epoch`, steps down.
    ///
    /// Checks: the partition exists (`NotFound`); `epoch` equals the current epoch
    /// (`StaleVersion { current: epoch }`); `node` is non-zero and the current leader
    /// (`Invalid`). On success the partition is at `epoch + 1` with leader 0, `node` is
    /// removed from the ISR when the ISR has other members (kept when it is the only
    /// one), `isr_version` is bumped and ext tag 80 is set from `info` verbatim.
    fn resign_leader(
        &mut self,
        index: u64,
        partition: u16,
        epoch: u32,
        node: u64,
        info: &ElectionInfo,
    ) -> MetaResult {
        let Some(cur) = self.partitions.get(&partition) else {
            return MetaResult::NotFound;
        };
        if u64::from(epoch) != cur.version {
            return MetaResult::StaleVersion {
                current: cur.version,
            };
        }
        if node == 0 || cur.value.leader != node {
            return MetaResult::Invalid(format!(
                "ResignLeader p{partition}: node {node} is not the leader (leader is {})",
                cur.value.leader
            ));
        }
        let (Some(new_epoch), Some(isr_version)) =
            (epoch.checked_add(1), cur.value.isr_version.checked_add(1))
        else {
            return MetaResult::Invalid(format!("ResignLeader p{partition}: epoch overflow"));
        };
        let ext = match crate::ext::put_election_info(&cur.value.ext, info) {
            Ok(ext) => ext,
            Err(e) => {
                return MetaResult::Invalid(format!(
                    "ResignLeader p{partition}: stored ext is malformed: {e}"
                ));
            }
        };
        let cur = self.partitions.get_mut(&partition).expect("checked above");
        let prev = std::mem::take(&mut cur.value);
        let isr = if prev.isr.len() > 1 {
            prev.isr.into_iter().filter(|&n| n != node).collect()
        } else {
            prev.isr
        };
        *cur = Versioned {
            version: u64::from(new_epoch),
            updated_index: index,
            value: PartitionAssignment {
                partition,
                leader: 0,
                epoch: new_epoch,
                isr,
                replicas: prev.replicas,
                isr_version,
                min_insync: prev.min_insync,
                ext,
            },
        };
        MetaResult::Ok {
            version: u64::from(new_epoch),
        }
    }

    /// The record under `key`, cloned into a [`MetaRecord`].
    pub fn get(&self, key: &MetaKey) -> Option<Versioned<MetaRecord>> {
        match key {
            MetaKey::Cluster => self.cluster.as_ref().map(|v| wrap(v, MetaRecord::Cluster)),
            MetaKey::Member(k) => self.members.get(k).map(|v| wrap(v, MetaRecord::Member)),
            MetaKey::Partition(k) => self
                .partitions
                .get(k)
                .map(|v| wrap(v, MetaRecord::Partition)),
            MetaKey::Module(k) => self.modules.get(k).map(|v| wrap(v, MetaRecord::Module)),
            MetaKey::Workflow(n, ver) => self
                .workflows
                .get(&(n.clone(), *ver))
                .map(|v| wrap(v, MetaRecord::Workflow)),
            MetaKey::Active(k) => self.actives.get(k).map(|v| wrap(v, MetaRecord::Active)),
            MetaKey::Queue(k) => self.queues.get(k).map(|v| wrap(v, MetaRecord::Queue)),
            MetaKey::Schedule(k) => self.schedules.get(k).map(|v| wrap(v, MetaRecord::Schedule)),
            MetaKey::Token(k) => self.tokens.get(k).map(|v| wrap(v, MetaRecord::Token)),
            MetaKey::Tenant(k) => self.tenants.get(k).map(|v| wrap(v, MetaRecord::Tenant)),
            MetaKey::Secret(k) => self.secrets.get(k).map(|v| wrap(v, MetaRecord::Secret)),
            MetaKey::Grant(k) => self.grants.get(k).map(|v| wrap(v, MetaRecord::Grant)),
            MetaKey::Connection(k) => self
                .connections
                .get(k)
                .map(|v| wrap(v, MetaRecord::Connection)),
            MetaKey::Egress(k) => self.egress.get(k).map(|v| wrap(v, MetaRecord::Egress)),
            MetaKey::WorkflowPolicy(k) => self
                .workflow_policies
                .get(k)
                .map(|v| wrap(v, MetaRecord::WorkflowPolicy)),
            MetaKey::QueuePolicy(k) => self
                .queue_policies
                .get(k)
                .map(|v| wrap(v, MetaRecord::QueuePolicy)),
            MetaKey::Webhook(k) => self.webhooks.get(k).map(|v| wrap(v, MetaRecord::Webhook)),
        }
    }

    /// Store a record under its own key (callers validated key == record.key()).
    fn insert(&mut self, rec: Versioned<MetaRecord>) {
        let Versioned {
            version,
            updated_index,
            value,
        } = rec;
        macro_rules! v {
            ($x:expr) => {
                Versioned {
                    version,
                    updated_index,
                    value: $x,
                }
            };
        }
        match value {
            MetaRecord::Cluster(r) => self.cluster = Some(v!(r)),
            MetaRecord::Member(r) => {
                self.members.insert(r.node_id, v!(r));
            }
            MetaRecord::Partition(r) => {
                self.partitions.insert(r.partition, v!(r));
            }
            MetaRecord::Module(r) => {
                self.modules.insert(r.sha256, v!(r));
            }
            MetaRecord::Workflow(r) => {
                self.workflows.insert((r.name.clone(), r.version), v!(r));
            }
            MetaRecord::Active(r) => {
                self.actives.insert(r.name.clone(), v!(r));
            }
            MetaRecord::Queue(r) => {
                self.queues.insert(r.name.clone(), v!(r));
            }
            MetaRecord::Schedule(r) => {
                self.schedules.insert(r.name.clone(), v!(r));
            }
            MetaRecord::Token(r) => {
                self.tokens.insert(r.id.clone(), v!(r));
            }
            MetaRecord::Tenant(r) => {
                self.tenants.insert(r.id.clone(), v!(r));
            }
            MetaRecord::Secret(r) => {
                self.secrets.insert(r.name.clone(), v!(r));
            }
            MetaRecord::Grant(r) => {
                self.grants.insert(r.id.clone(), v!(r));
            }
            MetaRecord::Connection(r) => {
                self.connections.insert(r.name.clone(), v!(r));
            }
            MetaRecord::Egress(r) => {
                self.egress.insert(r.name.clone(), v!(r));
            }
            MetaRecord::WorkflowPolicy(r) => {
                self.workflow_policies.insert(r.workflow.clone(), v!(r));
            }
            MetaRecord::QueuePolicy(r) => {
                self.queue_policies.insert(r.queue.clone(), v!(r));
            }
            MetaRecord::Webhook(r) => {
                self.webhooks.insert(r.name.clone(), v!(r));
            }
        }
    }

    fn remove(&mut self, key: &MetaKey) {
        match key {
            MetaKey::Cluster => self.cluster = None,
            MetaKey::Member(k) => {
                self.members.remove(k);
            }
            MetaKey::Partition(k) => {
                self.partitions.remove(k);
            }
            MetaKey::Module(k) => {
                self.modules.remove(k);
            }
            MetaKey::Workflow(n, ver) => {
                self.workflows.remove(&(n.clone(), *ver));
            }
            MetaKey::Active(k) => {
                self.actives.remove(k);
            }
            MetaKey::Queue(k) => {
                self.queues.remove(k);
            }
            MetaKey::Schedule(k) => {
                self.schedules.remove(k);
            }
            MetaKey::Token(k) => {
                self.tokens.remove(k);
            }
            MetaKey::Tenant(k) => {
                self.tenants.remove(k);
            }
            MetaKey::Secret(k) => {
                self.secrets.remove(k);
            }
            MetaKey::Grant(k) => {
                self.grants.remove(k);
            }
            MetaKey::Connection(k) => {
                self.connections.remove(k);
            }
            MetaKey::Egress(k) => {
                self.egress.remove(k);
            }
            MetaKey::WorkflowPolicy(k) => {
                self.workflow_policies.remove(k);
            }
            MetaKey::QueuePolicy(k) => {
                self.queue_policies.remove(k);
            }
            MetaKey::Webhook(k) => {
                self.webhooks.remove(k);
            }
        }
    }

    // ---- accessors -------------------------------------------------------

    pub fn cluster(&self) -> Option<&Versioned<ClusterInfo>> {
        self.cluster.as_ref()
    }

    pub fn member(&self, node_id: u64) -> Option<&Versioned<Member>> {
        self.members.get(&node_id)
    }

    pub fn members(&self) -> impl Iterator<Item = &Versioned<Member>> {
        self.members.values()
    }

    pub fn partition(&self, partition: u16) -> Option<&Versioned<PartitionAssignment>> {
        self.partitions.get(&partition)
    }

    pub fn partitions(&self) -> impl Iterator<Item = &Versioned<PartitionAssignment>> {
        self.partitions.values()
    }

    pub fn module(&self, sha256: &[u8; 32]) -> Option<&Versioned<ModuleRecord>> {
        self.modules.get(sha256)
    }

    pub fn modules(&self) -> impl Iterator<Item = &Versioned<ModuleRecord>> {
        self.modules.values()
    }

    pub fn workflow(&self, name: &str, version: u32) -> Option<&Versioned<WorkflowVersion>> {
        self.workflows.get(&(name.to_string(), version))
    }

    /// Every workflow version, ordered by (name, version).
    pub fn workflows(&self) -> impl Iterator<Item = &Versioned<WorkflowVersion>> {
        self.workflows.values()
    }

    pub fn active(&self, name: &str) -> Option<&Versioned<ActiveVersion>> {
        self.actives.get(name)
    }

    pub fn queue(&self, name: &str) -> Option<&Versioned<QueueConfig>> {
        self.queues.get(name)
    }

    pub fn queues(&self) -> impl Iterator<Item = &Versioned<QueueConfig>> {
        self.queues.values()
    }

    pub fn token(&self, id: &str) -> Option<&Versioned<ApiToken>> {
        self.tokens.get(id)
    }

    /// Every API token, by id (Phase 7 07-09: worker join tokens are listed through it).
    pub fn tokens(&self) -> impl Iterator<Item = &Versioned<ApiToken>> {
        self.tokens.values()
    }

    pub fn schedule(&self, name: &str) -> Option<&Versioned<Schedule>> {
        self.schedules.get(name)
    }

    /// Every schedule, ordered by name.
    pub fn schedules(&self) -> impl Iterator<Item = &Versioned<Schedule>> {
        self.schedules.values()
    }

    pub fn tenant(&self, id: &str) -> Option<&Versioned<Tenant>> {
        self.tenants.get(id)
    }

    pub fn tenants(&self) -> impl Iterator<Item = &Versioned<Tenant>> {
        self.tenants.values()
    }

    pub fn workflow_policy(&self, workflow: &str) -> Option<&Versioned<WorkflowPolicy>> {
        self.workflow_policies.get(workflow)
    }

    pub fn queue_policy(&self, queue: &str) -> Option<&Versioned<QueuePolicy>> {
        self.queue_policies.get(queue)
    }

    pub fn webhook(&self, name: &str) -> Option<&Versioned<Webhook>> {
        self.webhooks.get(name)
    }

    /// Every webhook, ordered by name.
    pub fn webhooks(&self) -> impl Iterator<Item = &Versioned<Webhook>> {
        self.webhooks.values()
    }

    /// Every grant, ordered by id (Phase 6, 06-14: the bridge's `MetaView::grants_for`
    /// filters them by subject).
    pub fn grants(&self) -> impl Iterator<Item = &Versioned<Grant>> {
        self.grants.values()
    }

    /// Every sealed secret, ordered by full record name (Phase 6, 06-16: the admin API
    /// lists names and key ids, never values).
    pub fn secrets(&self) -> impl Iterator<Item = &Versioned<SealedSecret>> {
        self.secrets.values()
    }

    /// Every connection, ordered by name (Phase 6, 06-16).
    pub fn connections(&self) -> impl Iterator<Item = &Versioned<Connection>> {
        self.connections.values()
    }

    /// Every egress policy, ordered by module (Phase 6, 06-16).
    pub fn egress_policies(&self) -> impl Iterator<Item = &Versioned<EgressPolicy>> {
        self.egress.values()
    }

    /// The await-token HMAC key written by `InitCluster` (Phase 4: stored as
    /// plaintext in a [`SealedSecret`]; see [`TOKEN_KEY_SECRET`]).
    ///
    /// Raw bytes only: once Phase 14 sealed the record (aes256gcm, an operator sealing key)
    /// this answers `None`. Servers read it through `ostro_server::p14::reseal::token_key`,
    /// which opens both forms.
    pub fn token_key(&self) -> Option<[u8; 32]> {
        self.secrets
            .get(TOKEN_KEY_SECRET)
            .and_then(|s| s.value.ciphertext.as_slice().try_into().ok())
    }
}
