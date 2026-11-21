//! Codegen via protox (pure Rust); no `protoc` needed (same as ostro-proto).

fn main() -> Result<(), Box<dyn std::error::Error>> {
    const PROTOS: &[&str] = &["proto/ostro/worker/v1/execute.proto"];
    for proto in PROTOS {
        println!("cargo:rerun-if-changed={proto}");
    }
    println!("cargo:rerun-if-changed=proto");
    let fds = protox::compile(PROTOS, ["proto/"])?;
    tonic_prost_build::configure().compile_fds(fds)?;
    Ok(())
}
