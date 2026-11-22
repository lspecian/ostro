//! ostro-repl-conformance: the Phase 12 replication conformance suite (CLUS-01,
//! D-P12-18/19). Test-only, never published.
//!
//! Every module is declared here once by 12-02, so later plans own disjoint files:
//!
//! | module | owner | contents |
//! |--------|-------|----------|
//! | [`harness`] | 12-02 (API) | [`harness::ClusterHarness`], [`harness::ClusterSpec`] and the shared types |
//! | `harness::{certs, fault, inproc}` | 12-03 | test PKI, in-memory transport + FaultLayer, tier-B `InProcCluster` |
//! | `harness::{process, chaos_proxy}` | 12-09 | tier-C multi-process clusters behind a TCP chaos proxy |
//! | [`oracle`] | 12-03 | measured oracles (disk, replica, standby, LA rule, election, objstore, status, cluster_formed) |
//! | [`scenarios`] | 12-02 (registry) | [`scenarios::Scenario`], [`scenarios::all`]; groups by 12-06 / 12-07 / 12-17; `runner` by 12-03 |
//! | [`pending`] | 12-02 (lists by 12-06 / 12-07 / 12-17) | scenarios not integrated yet ([`pending::is_pending`]) |
//! | [`mutants`] | 12-07 | mutant → scenarios matrix |

pub mod harness;
pub mod mutants;
pub mod oracle;
pub mod pending;
pub mod scenarios;

// Dependencies declared once by plan 12-01 for the harness, oracles and scenarios (12-03,
// 12-06, 12-07, 12-09). Anchored so cargo's unused-dependency lint stays quiet until the
// owning modules consume them.
mod dep_anchors {
    use ostro_server as _;
    use ostro_testkit as _;
    use proptest as _;
}
