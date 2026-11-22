//! Measured replication oracles (12-03). Each returns a report naming the culprit; a
//! scenario turns a failed oracle into a `ScenarioFailure { oracle, detail }`.

pub mod cluster_formed;
pub mod disk;
pub mod election;
pub mod la_rule;
pub mod objstore;
pub mod replica;
pub mod standby;
pub mod status;
