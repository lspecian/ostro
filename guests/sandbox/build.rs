// The Rust wasm32 shadow stack (linear memory, default 1 MiB) must be much larger
// than the engine's native `max_wasm_stack` (512 KiB), so that deep recursion in the
// `deep` / `deep-call` workflows hits Wasmtime's stack-overflow trap (ENG-06) rather
// than running the shadow stack out of linear memory first (which surfaces as an
// out-of-bounds memory trap instead of `stack_overflow`).
fn main() {
    println!("cargo:rustc-link-arg-cdylib=-zstack-size=16777216");
}
