//! Codegen via protox (pure Rust); no `protoc` needed.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    const PROTO: &str = "proto/ostro/health/v1/health.proto";
    println!("cargo:rerun-if-changed={PROTO}");
    println!("cargo:rerun-if-changed=proto");
    let fds = protox::compile([PROTO], ["proto/"])?;
    tonic_prost_build::configure().compile_fds(fds)?;
    Ok(())
}
