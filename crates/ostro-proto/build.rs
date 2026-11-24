//! Codegen via protox (pure Rust); no `protoc` needed.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    const PROTOS: &[&str] = &[
        "proto/ostro/health/v1/health.proto",
        "proto/ostro/api/v1/common.proto",
        "proto/ostro/api/v1/modules.proto",
        "proto/ostro/api/v1/workflows.proto",
        "proto/ostro/api/v1/runs.proto",
        "proto/ostro/api/v1/tokens.proto",
        // Phase 8 (08-10): schedules + run logs, webhooks.
        "proto/ostro/api/v1/schedules.proto",
        "proto/ostro/api/v1/webhooks.proto",
        // Phase 6 (06-08): Secrets + Connectors (bridge administration).
        "proto/ostro/api/v1/bridge.proto",
        // Phase 7 (07-11): Queues (flow, pause, demand) + Workers (admin, join).
        "proto/ostro/api/v1/queues.proto",
        "proto/ostro/api/v1/workers.proto",
        // Phase 11 (11-09): RunOps (rerun, DLQ, bulk, limits, failure policy) + Inventory.
        "proto/ostro/api/v1/ops.proto",
        // Phase 12 (12-02): node-to-node Replication + MetaPeer (cluster listener only).
        "proto/ostro/cluster/v1/cluster.proto",
        // Phase 13 (13-03): Cluster + ModuleLifecycle (public), Membership + NodeOps
        // (cluster listener only).
        "proto/ostro/api/v1/cluster.proto",
        "proto/ostro/cluster/v1/membership.proto",
    ];
    for proto in PROTOS {
        println!("cargo:rerun-if-changed={proto}");
    }
    println!("cargo:rerun-if-changed=proto");
    let fds = protox::compile(PROTOS, ["proto/"])?;
    // Phase 7 (07-11): messages carrying a join-token secret get a hand-written,
    // redacting `Debug` in src/lib.rs (D-W3: secrets never reach logs).
    tonic_prost_build::configure()
        .skip_debug([
            ".ostro.api.v1.WorkerTokenCreated",
            ".ostro.api.v1.JoinWorkerRequest",
            // Phase 13 (13-03): node join tokens.
            ".ostro.api.v1.JoinClusterRequest",
            ".ostro.api.v1.NodeTokenCreated",
        ])
        .compile_fds(fds)?;
    Ok(())
}
