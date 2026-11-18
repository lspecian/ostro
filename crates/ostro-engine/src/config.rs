//! Wasmtime engine configuration (ENG-01) and pooling allocator sizing (ENG-08).
//!
//! The flags set here are part of determinism (NaN bits, relaxed SIMD) and of the
//! AOT compatibility hash, so `tests/engine_config.rs` pins the full `Debug` output
//! of the resulting [`wasmtime::Config`] with an insta snapshot.

use std::time::Duration;

use wasmtime::{InstanceAllocationStrategy, PoolingAllocationConfig, WasmBacktraceDetails};

const KIB: usize = 1024;
const MIB: usize = 1024 * KIB;
const GIB: u64 = 1024 * 1024 * 1024;

/// Guest language class. Drives per-instance pool sizing (S2 measurements).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LanguageClass {
    Rust,
    Python,
    TypeScript,
}

impl LanguageClass {
    /// Core module instances per component instance (S2: Rust 3, Python 38, TS 3).
    pub fn core_instances_per_instance(self) -> u32 {
        match self {
            Self::Rust => 3,
            Self::Python => 38,
            Self::TypeScript => 3,
        }
    }

    /// Tables per component instance (S2: 2 for every language).
    pub fn tables_per_instance(self) -> u32 {
        2
    }

    /// Minimum `max_memory_size` the language's runtime needs to start (S2).
    pub fn min_memory_bytes(self) -> u64 {
        match self {
            Self::Rust => 1_114_112,
            Self::Python => 13_172_736,
            Self::TypeScript => 9_502_720,
        }
    }
}

/// What to do when the pooling allocator cannot reserve its address space.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fallback {
    /// Log a warning and use the on-demand allocator (default).
    OnDemand,
    /// Return [`crate::error::EngineError::PoolReservation`].
    Fail,
}

/// Pooling allocator settings (`engine.pool.*`).
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// `engine.pool.enabled`: use the pooling allocator (default true).
    pub enabled: bool,
    /// `engine.pool.total_instances`: concurrent live component instances (default 1000).
    pub total_instances: u32,
    /// `engine.pool.max_memory_bytes`: per-instance linear memory cap (default 256 MiB;
    /// raised to the language class floor if lower).
    pub max_memory_bytes: u64,
    /// `engine.pool.max_unused_warm_slots`: warm slots kept for reuse (default 100).
    pub max_unused_warm_slots: u32,
    /// `engine.pool.fallback`: behaviour when the reservation fails (default on-demand).
    pub fallback: Fallback,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            total_instances: 1000,
            max_memory_bytes: 256 * MIB as u64,
            max_unused_warm_slots: 100,
            fallback: Fallback::OnDemand,
        }
    }
}

/// Engine settings (`engine.*`). Field names are a contract read by later plans.
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// `engine.class`: language class used for pool sizing (default Rust).
    pub class: LanguageClass,
    /// `engine.pool`: pooling allocator settings.
    pub pool: PoolConfig,
    /// `engine.async_stack_size`: fiber stack for async host calls (default 4 MiB).
    pub async_stack_size: usize,
    /// `engine.max_wasm_stack`: guest stack limit (default 512 KiB).
    pub max_wasm_stack: usize,
    /// `engine.max_table_elements`: per-table element cap (default 20,000).
    pub max_table_elements: usize,
    /// `engine.epoch_tick`: epoch ticker period (default 10 ms).
    pub epoch_tick: Duration,
    /// `engine.activation_timeout`: wall-clock limit per activation (default 60 s).
    pub activation_timeout: Duration,
    /// `engine.activation_fuel`: fuel per activation (default 10,000,000,000).
    pub activation_fuel: u64,
    /// `engine.parallel_compilation`: compile functions in parallel (default true).
    pub parallel_compilation: bool,
    /// `engine.compile_permits`: concurrent component compiles (default 2).
    pub compile_permits: usize,
    /// `engine.hot_wait`: how long a waiting activation stays parked hot (default 2 s).
    pub hot_wait: Duration,
    /// `engine.node_memory_budget`: soft budget across all live instances (default 1 GiB).
    pub node_memory_budget: u64,
    /// `engine.continue_as_new_events`: journal length that suggests continue-as-new
    /// (default 10,000).
    pub continue_as_new_events: u64,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            class: LanguageClass::Rust,
            pool: PoolConfig::default(),
            async_stack_size: 4 * MIB,
            max_wasm_stack: 512 * KIB,
            max_table_elements: 20_000,
            epoch_tick: Duration::from_millis(10),
            activation_timeout: Duration::from_secs(60),
            activation_fuel: 10_000_000_000,
            parallel_compilation: true,
            compile_permits: 2,
            hot_wait: Duration::from_secs(2),
            node_memory_budget: GIB,
            continue_as_new_events: 10_000,
        }
    }
}

impl EngineConfig {
    /// Effective per-instance memory cap: `engine.pool.max_memory_bytes` raised to the
    /// language class floor.
    pub fn effective_max_memory(&self) -> u64 {
        self.pool
            .max_memory_bytes
            .max(self.class.min_memory_bytes())
    }
}

/// Flags shared by the pooling and on-demand configurations.
fn base_config(c: &EngineConfig) -> wasmtime::Config {
    // The threads, GC, stack-switching and component-model-async proposals are not
    // available: their cargo features are not enabled (so `wasm_threads` etc. do not
    // exist and must not be called).
    let mut config = wasmtime::Config::new();
    config
        .wasm_component_model(true)
        .cranelift_nan_canonicalization(true)
        .relaxed_simd_deterministic(true)
        .consume_fuel(true)
        .epoch_interruption(true)
        .max_wasm_stack(c.max_wasm_stack)
        .async_stack_size(c.async_stack_size)
        // Environment-independent (WASMTIME_BACKTRACE_DETAILS must not change the
        // compat hash).
        .wasm_backtrace_details(WasmBacktraceDetails::Disable)
        .parallel_compilation(c.parallel_compilation);
    config
}

/// Pooling allocator sizing derived from the engine config and language class.
pub fn pooling_allocation_config(c: &EngineConfig) -> PoolingAllocationConfig {
    let n = c.pool.total_instances;
    let class = c.class;
    let mut p = PoolingAllocationConfig::default();
    p.total_component_instances(n)
        .total_core_instances(n.saturating_mul(class.core_instances_per_instance()))
        .total_memories(n)
        .total_tables(n.saturating_mul(class.tables_per_instance()))
        .total_stacks(n)
        .max_memory_size(usize::try_from(c.effective_max_memory()).unwrap_or(usize::MAX))
        .max_unused_warm_slots(c.pool.max_unused_warm_slots)
        .table_elements(c.max_table_elements);
    p
}

/// Build the deterministic Wasmtime configuration (pooling allocator when
/// `engine.pool.enabled`).
pub fn build_wasmtime_config(c: &EngineConfig) -> wasmtime::Config {
    let mut config = base_config(c);
    if c.pool.enabled {
        config.allocation_strategy(InstanceAllocationStrategy::Pooling(
            pooling_allocation_config(c),
        ));
    }
    config
}

/// Same flags as [`build_wasmtime_config`] with the on-demand allocator (used as the
/// pool-reservation fallback).
pub fn build_on_demand_config(c: &EngineConfig) -> wasmtime::Config {
    let mut config = base_config(c);
    config.allocation_strategy(InstanceAllocationStrategy::OnDemand);
    config
}

/// Full description of the Wasmtime configuration [`build_wasmtime_config`] produces.
///
/// `wasmtime::Config`'s `Debug` output omits stack sizes, backtrace details and the
/// allocation strategy, so the settings Ostro applies outside the `Debug` view are
/// appended after it. Pinned by the `wasmtime_config_debug` insta snapshot.
pub fn describe_wasmtime_config(c: &EngineConfig) -> String {
    let mut out = format!("{:#?}", build_wasmtime_config(c));
    out.push_str("\n--- settings not shown by wasmtime::Config Debug ---\n");
    out.push_str(&format!("max_wasm_stack: {}\n", c.max_wasm_stack));
    out.push_str(&format!("async_stack_size: {}\n", c.async_stack_size));
    out.push_str("wasm_backtrace_details: Disable\n");
    if c.pool.enabled {
        out.push_str(&format!(
            "allocation_strategy: Pooling({:#?})\n",
            pooling_allocation_config(c)
        ));
    } else {
        out.push_str("allocation_strategy: OnDemand\n");
    }
    out
}

/// Build the Wasmtime configuration used for deterministic workflow execution with
/// the default [`EngineConfig`].
pub fn engine_config() -> wasmtime::Config {
    build_wasmtime_config(&EngineConfig::default())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn engine_builds_from_config() -> wasmtime::Result<()> {
        let _engine = wasmtime::Engine::new(&build_on_demand_config(&EngineConfig::default()))?;
        Ok(())
    }

    #[test]
    fn language_class_sizing() {
        assert_eq!(LanguageClass::Rust.core_instances_per_instance(), 3);
        assert_eq!(LanguageClass::Python.core_instances_per_instance(), 38);
        assert_eq!(LanguageClass::TypeScript.core_instances_per_instance(), 3);
        let py = EngineConfig {
            class: LanguageClass::Python,
            pool: PoolConfig {
                max_memory_bytes: 1,
                ..PoolConfig::default()
            },
            ..EngineConfig::default()
        };
        assert_eq!(py.effective_max_memory(), 13_172_736);
    }
}
