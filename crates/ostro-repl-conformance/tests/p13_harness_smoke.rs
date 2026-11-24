//! 13-08 Task 1: the Phase 13 harness lifecycle in tiers B and C (what wave 2 can prove).
//!
//! - `start_single` serves a run end to end (tier B; tier C `#[ignore]`).
//! - `upgrade(node, Some(1))` restarts the node with the override (tier B:
//!   `ServerOptions::level_max_override`, measured through the node's Phase 13 context;
//!   tier C: `OSTRO_TEST_LEVEL_MAX=1`, read from `/proc/<pid>/environ`) and it serves again
//!   with its data; `upgrade(node, None)` brings it back to this build's level. The node starts
//!   at level 1 (`start_single_with`), so the cluster is never finalized above the emulated
//!   level (the boot fence would refuse that, correctly).
//! - `add_node` / `cluster_status` reach the server: either they succeed (13-09 landed) or
//!   the server answers with an HTTP error; a transport failure fails the test. 13-09 owns the
//!   success assertions in its own test file.
//!
//! Tier C runs with `--ignored` after `cargo build -p ostro --features failpoints
//! --target-dir target/failpoints` (or with `OSTRO_FAILPOINT_BIN`); it prints a `SKIP:` line
//! when the binary is missing.

use std::time::Duration;

use ostro_repl_conformance::harness::inproc::{InProcCluster, TEST_WORKFLOW, is_http_error};
use ostro_repl_conformance::harness::p13::{NodeOpts, P13Harness};
use ostro_repl_conformance::harness::process::{
    ProcCluster, ProcOptions, SKIP_NO_BIN, failpoint_bin, loopback_aliases_supported, parse_joined,
};
use ostro_repl_conformance::harness::{ApiClient, ClusterHarness, ClusterSpec, HarnessError};
use serde_json::json;

const RUN_T: Duration = Duration::from_secs(120);

/// Deploy the test module, start one run and wait for it; its run id.
async fn serve_one_run(api: &ApiClient, i: u32) -> String {
    assert!(api.ready().await, "node ready");
    api.deploy_test_module().await.expect("deploy");
    let ack = api
        .start_run(TEST_WORKFLOW, json!({ "i": i }), None)
        .await
        .expect("start run");
    assert_eq!(
        api.wait_run(&ack.run_id, RUN_T).await.expect("wait"),
        "completed"
    );
    ack.run_id
}

/// The run is still there and terminal (data kept across the restart).
async fn still_completed(api: &ApiClient, run_id: &str) {
    assert_eq!(
        api.wait_run(run_id, RUN_T)
            .await
            .expect("wait after restart"),
        "completed",
        "run {run_id} survives the restart"
    );
}

/// The call reached a server: success, or an HTTP error answer (never a transport error).
fn reached_server<T: std::fmt::Debug>(what: &str, r: &Result<T, HarnessError>) {
    match r {
        Ok(v) => println!("{what}: ok ({v:?})"),
        Err(e) => {
            assert!(is_http_error(e), "{what} must reach the server: {e}");
            println!("{what}: server answered {e}");
        }
    }
}

#[test]
fn joined_line_parser_matches_the_binary() {
    assert_eq!(
        parse_joined("joined cluster conf-7 as node 4 (data: /x/join4)"),
        Some(("conf-7".to_owned(), 4))
    );
    assert_eq!(parse_joined("joined cluster c as node x (data: /d)"), None);
    assert_eq!(
        parse_joined("ostro server listening on http://1.2.3.4:5"),
        None
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tier_b_start_single_serves_a_run() {
    let c = InProcCluster::start_single(ClusterSpec::single())
        .await
        .expect("start_single");
    assert_eq!(c.node_ids(), vec![1]);
    let cfg = c.config(1);
    assert!(cfg.cluster.peers.is_empty(), "no [[cluster.peers]]");
    assert_eq!(cfg.cluster.advertise, vec!["mem://1".to_owned()]);
    assert!(cfg.cluster.tls.ca_cert_file.is_some(), "test CA imported");
    assert_eq!(c.level_max(1), None);
    serve_one_run(&c.client(1), 0).await;
    c.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tier_b_upgrade_restarts_at_the_emulated_level_with_its_data() {
    let mut c = InProcCluster::start_single_with(ClusterSpec::single(), Some(1))
        .await
        .expect("start_single at level 1");
    let level = |c: &InProcCluster| c.handle(1).and_then(|h| h.p13()).map(|p| p.level_max);
    assert_eq!(level(&c), Some(1), "node runs at the emulated level");
    let run = serve_one_run(&c.client(1), 1).await;

    c.upgrade(1, Some(1)).await.expect("upgrade to Some(1)");
    assert_eq!(c.level_max(1), Some(1));
    assert_eq!(level(&c), Some(1), "restarted with the override");
    still_completed(&c.client(1), &run).await;
    serve_one_run(&c.client(1), 2).await;

    c.upgrade(1, None)
        .await
        .expect("upgrade to the build level");
    assert_eq!(c.level_max(1), None);
    assert_eq!(
        level(&c),
        Some(ostro_types::compat::LEVEL),
        "restarted at this build's level"
    );
    still_completed(&c.client(1), &run).await;
    c.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tier_b_add_node_and_status_reach_the_server() {
    let mut c = InProcCluster::start_single(ClusterSpec::single())
        .await
        .expect("start_single");
    assert!(c.client(1).ready().await);
    let status = c.cluster_status(1).await;
    reached_server("cluster_status", &status);
    let added = c.add_node(NodeOpts::default()).await;
    reached_server("add_node", &added);
    c.stop().await;
}

// ---- tier C -------------------------------------------------------------------------

/// `true` when the tier-C test must skip (reason printed).
fn skip() -> bool {
    if failpoint_bin().is_none() {
        println!("{SKIP_NO_BIN}");
        return true;
    }
    if !loopback_aliases_supported() {
        println!("SKIP: loopback aliases 127.r.r.x cannot be bound on this host");
        return true;
    }
    false
}

/// `OSTRO_TEST_LEVEL_MAX` in the environment of process `pid` (`None` = unset).
fn env_level(pid: u32) -> Option<String> {
    let env = std::fs::read(format!("/proc/{pid}/environ")).expect("read environ");
    env.split(|b| *b == 0)
        .filter_map(|kv| std::str::from_utf8(kv).ok())
        .find_map(|kv| kv.strip_prefix("OSTRO_TEST_LEVEL_MAX="))
        .map(str::to_owned)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "tier C: needs the failpoint binary (cargo build -p ostro --features failpoints --target-dir target/failpoints)"]
async fn tier_c_single_upgrade_and_join_reach_the_server() {
    if skip() {
        return;
    }
    let mut c =
        ProcCluster::start_single_with(ClusterSpec::single(), ProcOptions::default(), Some(1))
            .await
            .expect("start_single at level 1");
    let pid = c.pid(1).expect("running");
    assert_eq!(env_level(pid).as_deref(), Some("1"));
    assert!(c.config(1).cluster.peers.is_empty());
    assert_eq!(c.config(1).cluster.advertise, vec![c.ip(1).to_string()]);
    let run = serve_one_run(&c.client(1), 0).await;

    c.upgrade(1, Some(1)).await.expect("upgrade to Some(1)");
    let pid2 = c.pid(1).expect("running after upgrade");
    assert_ne!(pid, pid2, "a new process");
    assert_eq!(
        env_level(pid2).as_deref(),
        Some("1"),
        "the env is on the respawned process"
    );
    still_completed(&c.client(1), &run).await;
    serve_one_run(&c.client(1), 1).await;

    c.upgrade(1, None)
        .await
        .expect("upgrade to the build level");
    let pid3 = c.pid(1).expect("running after the second upgrade");
    assert_eq!(env_level(pid3), None, "None removes the env");
    still_completed(&c.client(1), &run).await;

    let status = c.cluster_status(1).await;
    reached_server("cluster_status", &status);
    let added = c.add_node(NodeOpts::default()).await;
    reached_server("add_node", &added);
    if let Ok(id) = added {
        // 13-09 landed: the joined node takes the Phase 12 fault primitives by id.
        assert!(c.proxy_for(id, 1).is_some(), "proxy link {id} -> 1");
        c.isolate(id);
        c.heal(id);
        c.kill(id).await;
    }
    c.stop().await;
}
