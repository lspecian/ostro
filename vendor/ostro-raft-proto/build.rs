// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.
// Vendored from chronik-stream@08715f4 crates/raft-proto/build.rs; modified by Ostro (see VENDORED.md).
// Rewritten to use protox (pure-Rust .proto compiler) so no `protoc` is needed.

fn main() {
    let fds = protox::compile(["proto/eraftpb.proto"], ["proto/"]).expect("protox");
    let mut config = prost_build::Config::new();
    config.bytes(["."]);
    config.compile_fds(fds).expect("Failed to compile protos");
    println!("cargo:rerun-if-changed=proto/eraftpb.proto");
}
