//! Tier-B version-gate scenarios (13-14; CLUS-06): auto-finalize after the last upgrade,
//! hold, manual finalize, the upload gate, downgrade before and after finalization
//! (`scenarios::version_gate`). Each runs on its own cluster started at the emulated level 1.
//!
//! - The plan's tests (`auto_finalize_after_last_upgrade`, ...) run on [`NODES`] = 3 nodes,
//!   two of them joined through `add_node` (the real join flow; restarts of joined nodes
//!   need 13-09's 774e59d).
//! - `single_node_*` run the same scenario bodies on one node.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::time::Duration;

use ostro_repl_conformance::scenarios::version_gate::{self, GateFn, NODES};

/// Hard bound per scenario (three joins, three or four restarts, the workload drain).
const DEADLINE: Duration = Duration::from_secs(600);

async fn scenario_on(nodes: usize, name: &str) {
    let body: GateFn = version_gate::scenarios()
        .into_iter()
        .find(|(n, _)| *n == name)
        .unwrap_or_else(|| panic!("no scenario {name}"))
        .1;
    match tokio::time::timeout(DEADLINE, version_gate::run(nodes, body)).await {
        Ok(Ok(())) => {}
        Ok(Err(f)) => panic!("{name} ({nodes} nodes): {f}"),
        Err(_) => panic!("{name} ({nodes} nodes): deadline {DEADLINE:?} exceeded"),
    }
}

/// One plan test on [`NODES`] nodes and its single-node variant.
macro_rules! gate_tests {
    ($($name:ident, $single:ident;)*) => {$(
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn $name() {
            scenario_on(NODES, stringify!($name)).await;
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn $single() {
            scenario_on(1, stringify!($name)).await;
        }
    )*};
}

gate_tests! {
    auto_finalize_after_last_upgrade, single_node_auto_finalize_after_last_upgrade;
    hold_keeps_window_open, single_node_hold_keeps_window_open;
    manual_finalize_refused_until_all_support, single_node_manual_finalize_refused_until_all_support;
    gated_upload_refused_before_finalize, single_node_gated_upload_refused_before_finalize;
    downgrade_before_finalize_ok, single_node_downgrade_before_finalize_ok;
    downgrade_after_finalize_refused, single_node_downgrade_after_finalize_refused;
}

#[test]
fn every_scenario_is_listed() {
    let names: Vec<&str> = version_gate::scenarios().iter().map(|(n, _)| *n).collect();
    assert_eq!(
        names,
        [
            "auto_finalize_after_last_upgrade",
            "hold_keeps_window_open",
            "manual_finalize_refused_until_all_support",
            "gated_upload_refused_before_finalize",
            "downgrade_before_finalize_ok",
            "downgrade_after_finalize_refused",
        ]
    );
}
