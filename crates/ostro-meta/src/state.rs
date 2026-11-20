//! The metadata state and its deterministic, versioned apply (META-03).
//!
//! `apply` always validates first and mutates only on success, so a rejected
//! command leaves the state byte-identical.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::command::{MetaCommand, MetaKey, MetaRecord, MetaResult};
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
    pub fn token_key(&self) -> Option<[u8; 32]> {
        self.secrets
            .get(TOKEN_KEY_SECRET)
            .and_then(|s| s.value.ciphertext.as_slice().try_into().ok())
    }
}
