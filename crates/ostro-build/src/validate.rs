//! Local validation of a built component: the same `ostro_engine` path as a module upload
//! (`Runtime::prepare`: core-module rejection, the import allow-list, the exports shape
//! incl. ENG-1, `instantiate_pre` against the production linker) followed by
//! `Runtime::describe` for the workflow list. The import rules are the engine's; nothing is
//! re-implemented here.
//!
//! The runtime is throwaway: on-demand allocation (no pool reservation), no AOT disk cache,
//! the default process-local token key; nothing is persisted.
//!
//! Owner: 10-12.

use std::collections::BTreeSet;

use ostro_engine::config::{EngineConfig, LanguageClass, PoolConfig};
use ostro_engine::error::EngineError;
use ostro_engine::runtime::{Runtime, RuntimeOptions};

use crate::pipeline::{BuildError, block_on};

/// Validate `wasm` like the server's upload does and return its workflows (sorted by name
/// and version, deduplicated).
pub fn validate_and_describe(wasm: &[u8]) -> Result<Vec<(String, u32)>, BuildError> {
    let rt = Runtime::new(
        EngineConfig {
            // The largest per-instance memory floor, so any language's guest fits.
            class: LanguageClass::Mixed,
            pool: PoolConfig {
                enabled: false,
                ..PoolConfig::default()
            },
            ..EngineConfig::default()
        },
        RuntimeOptions::default(),
    )
    .map_err(|e| BuildError::Validation(format!("cannot start the local validator: {e}")))?;
    let prepared = rt.prepare(wasm).map_err(|e| match e {
        EngineError::Validation(v) => BuildError::Validation(v.join("; ")),
        other => BuildError::Validation(other.to_string()),
    })?;
    let metas = block_on(|| rt.describe(&prepared))?
        .map_err(|e| BuildError::Validation(format!("describe() of the module failed: {e}")))?;
    let mut out = BTreeSet::new();
    for m in metas {
        if m.name.is_empty() {
            return Err(BuildError::Validation(
                "describe() returned a workflow with an empty name".to_owned(),
            ));
        }
        out.insert((m.name, m.version));
    }
    Ok(out.into_iter().collect())
}
