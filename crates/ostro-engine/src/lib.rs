//! ostro-engine: deterministic Wasm execution engine (skeleton).

/// Build the Wasmtime configuration used for deterministic workflow execution.
///
/// The threads proposal is unavailable because the `threads` feature is not
/// enabled (do not call `wasm_threads(false)`; it does not exist then).
pub fn engine_config() -> wasmtime::Config {
    let mut config = wasmtime::Config::new();
    config
        .wasm_component_model(true)
        .cranelift_nan_canonicalization(true)
        .relaxed_simd_deterministic(true)
        .consume_fuel(true)
        .epoch_interruption(true);
    config
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn engine_builds_from_config() -> wasmtime::Result<()> {
        let _engine = wasmtime::Engine::new(&engine_config())?;
        Ok(())
    }
}
