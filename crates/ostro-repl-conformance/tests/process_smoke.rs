//! 12-09 Task 2: the tier-C multi-process cluster. Gated on the failpoint binary (W14):
//! without `OSTRO_FAILPOINT_BIN` or `target/failpoints/debug/ostro`, each test prints a
//! `SKIP:` line and returns, so a plain `cargo test` never builds a second binary. Hosts that
//! cannot bind `127.r.r.x` (macOS without aliases) skip the same way.
//!
//! - RF=1, one process: 20 acknowledged runs, `kill -9`, restart from the same data dir,
//!   every run completes and the acked-on-disk oracle confirms every ack.
//! - `#[ignore]`: three processes start with peers pointing at the chaos proxy and answer
//!   health, form one cluster (measured `cluster_formed`); one is killed and restarted.

use std::time::{Duration, Instant};

use ostro_repl_conformance::harness::inproc::{TEST_WORKFLOW, read_meta_log};
use ostro_repl_conformance::harness::process::{
    CLUSTER_PORT, KillOutcome, PROXY_PORT_BASE, ProcCluster, SKIP_NO_BIN, failpoint_bin,
    loopback_aliases_supported, parse_ready,
};
use ostro_repl_conformance::harness::{ClusterHarness, ClusterSpec};
use ostro_repl_conformance::oracle::{cluster_formed, disk};
use serde_json::json;

const RUN_T: Duration = Duration::from_secs(120);

/// `true` when the test must skip (reason printed).
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

fn pid_alive(pid: u32) -> bool {
    std::path::Path::new(&format!("/proc/{pid}")).exists()
}

#[test]
fn ready_line_parser_matches_the_04_19_format() {
    let a = parse_ready(
        "ostro server listening on http://127.9.9.1:33809 (data: /x/y) ready in 2959 ms",
    );
    assert_eq!(a, Some("127.9.9.1:33809".parse().expect("addr")));
    assert_eq!(parse_ready("ostro server listening on https://x"), None);
    assert_eq!(parse_ready("INFO ready"), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rf1_process_survives_kill9_and_every_ack_is_on_disk() {
    if skip() {
        return;
    }
    let t0 = Instant::now();
    let mut c = ProcCluster::start(ClusterSpec::single())
        .await
        .expect("start");
    let started = t0.elapsed();
    assert_eq!(c.node_ids(), vec![1]);
    let ip = c.ip(1);
    assert_eq!(ip.octets()[0], 127);
    assert_eq!(ip.octets()[1], c.r());
    assert_eq!(ip.octets()[3], 1);
    let api = c.client(1);
    assert!(
        api.base_url.starts_with(&format!("http://{ip}:")),
        "{}",
        api.base_url
    );
    assert!(api.ready().await, "node 1 ready");
    api.deploy_test_module().await.expect("deploy");

    let mut acks = Vec::new();
    for i in 0..20 {
        acks.push(
            api.start_run(TEST_WORKFLOW, json!({ "i": i }), None)
                .await
                .expect("start run"),
        );
    }
    // Keyed workload on one partition too (the oracle reads every partition it names).
    acks.extend(api.append_workload(1, 4).await.expect("workload"));

    let pid = c.pid(1).expect("pid");
    assert_eq!(c.kill9(1).await.expect("kill"), KillOutcome::Killed);
    let killed = c.timings().last_kill;
    assert!(!pid_alive(pid), "pid {pid} is gone after kill -9");
    assert!(!c.is_running(1));
    assert!(!c.client(1).ready().await, "a killed node does not answer");
    assert_eq!(
        c.kill9(1).await.expect("kill twice"),
        KillOutcome::NotRunning
    );

    let root = c.node_disk(1).root;
    c.restart(1).await.expect("restart");
    let restarted = c.timings().last_start;
    assert_eq!(c.node_disk(1).root, root, "the data dir is reused");
    assert_ne!(c.pid(1), Some(pid), "a new process");
    let api = c.client(1);
    for a in &acks {
        assert_eq!(
            api.wait_run(&a.run_id, RUN_T).await.expect("after kill -9"),
            "completed",
            "run {} survives kill -9",
            a.run_id
        );
    }
    let report = disk::acked_on_disk(&[c.node_disk(1)], &acks, 1);
    assert!(report.is_ok(), "{report}");

    // The harness API reads meta offline: bootstrap and leadership are there.
    assert!(!c.meta_log(1).await.is_empty());
    let a = c.assignment(0).await;
    assert_eq!((a.leader, a.replicas.clone()), (1, vec![1]), "{a:?}");
    let log = std::fs::read_to_string(c.log_path(1)).expect("log");
    assert_eq!(
        log.matches("=== harness: spawn node 1 ===").count(),
        2,
        "{log}"
    );
    println!(
        "tier-C timings: start {started:?}, kill -9 -> reaped {killed:?}, restart -> ready {restarted:?}"
    );
    c.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "tier C: three processes (~10 s)"]
async fn three_processes_start_with_proxied_peers_and_answer_health() {
    if skip() {
        return;
    }
    let spec = ClusterSpec {
        seed: 9,
        ..ClusterSpec::default()
    };
    let mut c = ProcCluster::start(spec).await.expect("start");
    assert_eq!(c.node_ids(), vec![1, 2, 3]);
    let r = c.r();
    for n in c.node_ids() {
        assert!(c.client(n).ready().await, "node {n} ready");
        let cfg = c.config(n);
        assert_eq!(cfg.cluster.listen.port(), CLUSTER_PORT);
        assert_eq!(
            cfg.cluster.listen.ip().to_string(),
            format!("127.{r}.{r}.{n}")
        );
        assert_eq!(cfg.cluster.peers.len(), 2);
        for p in &cfg.cluster.peers {
            // Node n reaches peer p through the proxy listener of n -> p.
            assert_eq!(p.addr, c.proxy().addr(n, p.id).to_string());
            assert_eq!(
                p.addr,
                format!("127.{r}.{r}.{}:{}", p.id, PROXY_PORT_BASE + n as u16)
            );
        }
        assert!(cfg.cluster.tls.ca_cert_file.is_some());
        assert_eq!(cfg.cluster.advertise, vec![format!("127.{r}.{r}.{n}")]);
        // Since 12-14 the processes form one three-voter meta group.
        let dump = read_meta_log(&c.node_disk(n).root.join("meta")).expect("meta log");
        let mut voters = dump.voters.clone();
        voters.sort_unstable();
        assert_eq!(voters, vec![1, 2, 3], "node {n}");
    }
    assert_eq!(c.proxy().links().len(), 6);
    // Formation over real processes, measured like tier B (12-17: green since 12-14).
    let spec = c.spec().clone();
    let formed = cluster_formed::check(&c, &spec).await;
    assert!(formed.is_ok(), "tier-C formation: {formed}");
    println!("tier-C cluster_formed: {formed}");

    let pid = c.pid(2).expect("pid");
    c.kill(2).await;
    assert!(!pid_alive(pid));
    assert!(!c.client(2).ready().await);
    assert!(c.client(1).ready().await, "the others keep running");
    c.isolate(2);
    assert!(c.proxy().rule(1, 2).cut);
    c.restart(2).await.expect("restart");
    assert!(c.client(2).ready().await);
    assert!(!c.proxy().rule(1, 2).cut, "restart heals");
    println!(
        "tier-C 3-node restart -> ready {:?}",
        c.timings().last_start
    );
    c.stop().await;
}
