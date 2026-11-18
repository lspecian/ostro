//! ostro-engine: deterministic Wasm execution engine (skeleton).

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn engine_builds_from_config() -> wasmtime::Result<()> {
        let _engine = wasmtime::Engine::new(&engine_config())?;
        Ok(())
    }
}
