//! SDK test guest (plan 04-06, WF-07): plain Rust std/crate non-determinism is made
//! deterministic by the host's WASI shim. Reads `SystemTime::now()`, iterates a std
//! `HashMap` with 50 keys and draws `rand::random::<u64>()`, both in the workflow body and
//! inside a durable step. The output is JSON of every observed value; replaying the
//! recorded journal must reproduce identical output bytes.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use ostro_sdk::prelude::*;

/// Values observed at one point of the run.
#[derive(Debug, Serialize, Deserialize)]
pub struct Observed {
    /// `SystemTime::now()` as nanoseconds since the Unix epoch.
    pub time_nanos: u64,
    /// `rand::random::<u64>()`.
    pub random: u64,
    /// Iteration order of a fresh std `HashMap` with keys `k0..k49`.
    pub map_order: Vec<String>,
}

/// `sdk-native` output.
#[derive(Debug, Serialize)]
pub struct Output {
    pub outside: Observed,
    pub inside: Observed,
}

fn observe() -> Observed {
    let time_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX));
    let map: HashMap<String, u32> = (0..50).map(|i| (format!("k{i}"), i)).collect();
    Observed {
        time_nanos,
        random: rand::random::<u64>(),
        map_order: map.into_keys().collect(),
    }
}

#[step]
fn observe_in_step() -> Result<Observed, StepError> {
    Ok(observe())
}

#[workflow(name = "sdk-native", version = 1)]
pub fn sdk_native(_ctx: &Ctx, _input: serde_json::Value) -> Result<Output, WorkflowError> {
    let outside = observe();
    let inside = observe_in_step()?;
    Ok(Output { outside, inside })
}

ostro_sdk::export!(SdkNativeWorkflow);
