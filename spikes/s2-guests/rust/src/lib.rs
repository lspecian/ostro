//! Spike S2 Rust guest (wit-bindgen 0.63, wasm32-wasip2). Throwaway.
//!
//! `run`, `run-reseeded` and `run-dropped-step` stay free of WASI calls so the
//! harness can measure them without a WASI implementation. The probes use std
//! APIs that reach WASI on purpose; whether they trap is itself a finding.

wit_bindgen::generate!({ path: "../../wit", world: "spike" });

use ostro::spike::context;
use ostro::spike::journal::{self, StepStart};
use std::collections::HashMap;
use std::hash::{BuildHasher, Hasher};

struct G;

/// splitmix64: Rust "reseed" uses the host u64 directly as PRNG state.
fn splitmix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

impl Guest for G {
    fn run(input: String) -> String {
        let out = format!("hello {input}");
        let resource_seq = match journal::begin_step("greet") {
            StepStart::Replayed(s) => return s,
            StepStart::Execute(step) => {
                let seq = step.seq();
                step.complete(&out);
                seq
            }
        };
        let h = journal::begin_step_u64("greet-u64");
        journal::complete_u64(h, &out);
        format!(r#"{{"out":"{out}","resource_seq":{resource_seq},"u64_seq":{h}}}"#)
    }

    fn probe_random() -> String {
        // Ambient randomness probe: RandomState keys come from WASI random.
        let vals: Vec<u64> = (0..3)
            .map(|_| {
                let mut h = std::collections::hash_map::RandomState::new().build_hasher();
                h.write(b"ostro");
                h.finish()
            })
            .collect();
        format!("{vals:?}")
    }

    fn probe_time() -> String {
        let d = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        format!("{d}")
    }

    fn probe_hash() -> String {
        let mut m = HashMap::new();
        for (i, k) in ["a", "b", "c", "d"].into_iter().enumerate() {
            m.insert(k, i);
        }
        let keys: Vec<&str> = m.keys().copied().collect();
        format!("{keys:?}")
    }

    fn probe_uuid() -> String {
        "n/a".to_string()
    }

    fn run_reseeded(seed_from_host: bool) -> String {
        let seed = if seed_from_host { context::random_u64() } else { 0 };
        let mut st = seed;
        let draws: Vec<u64> = (0..3).map(|_| splitmix(&mut st)).collect();
        format!(r#"{{"seed":{},"draws":{draws:?}}}"#, if seed_from_host { seed.to_string() } else { "null".into() })
    }

    fn run_dropped_step() -> String {
        match journal::begin_step("dropped") {
            StepStart::Replayed(s) => format!(r#"{{"replayed":"{s}"}}"#),
            StepStart::Execute(step) => {
                let seq = step.seq();
                drop(step); // no complete()
                format!(r#"{{"dropped_seq":{seq}}}"#)
            }
        }
    }
}

export!(G);
