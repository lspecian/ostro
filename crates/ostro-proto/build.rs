//! Codegen via protox (pure Rust); no `protoc` needed.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    const PROTOS: &[&str] = &[
        "proto/ostro/health/v1/health.proto",
        "proto/ostro/api/v1/common.proto",
        "proto/ostro/api/v1/modules.proto",
        "proto/ostro/api/v1/workflows.proto",
        "proto/ostro/api/v1/runs.proto",
    ];
    for proto in PROTOS {
        println!("cargo:rerun-if-changed={proto}");
    }
    println!("cargo:rerun-if-changed=proto");
    let fds = protox::compile(PROTOS, ["proto/"])?;
    tonic_prost_build::configure().compile_fds(fds)?;
    Ok(())
}
