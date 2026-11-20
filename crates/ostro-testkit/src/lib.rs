//! ostro-testkit: offline fault injection for Phase 2 storage tests.
//!
//! kill -9 never produces torn writes (the page cache survives a process
//! kill), so torn, zeroed and truncated tails are injected offline on closed
//! files instead (02-RESEARCH Pitfall 2).

pub mod corrupt;
pub mod fs;

/// Number of proptest cases: `PROPTEST_CASES` when set and parseable, else `default`.
pub fn proptest_cases(default: u32) -> u32 {
    std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Single test so the env var is never mutated concurrently.
    #[test]
    fn proptest_cases_reads_env_or_default() {
        // SAFETY: only this test touches PROPTEST_CASES in this test binary.
        unsafe { std::env::remove_var("PROPTEST_CASES") };
        assert_eq!(proptest_cases(64), 64);
        unsafe { std::env::set_var("PROPTEST_CASES", "7") };
        assert_eq!(proptest_cases(64), 7);
        unsafe { std::env::set_var("PROPTEST_CASES", "not-a-number") };
        assert_eq!(proptest_cases(64), 64);
        unsafe { std::env::remove_var("PROPTEST_CASES") };
    }
}

// Phase 8 (plan 08-01): in-process OTLP collector dependencies for 08-04, anchored so cargo's
// unused-dependency lint stays quiet until that module lands.
mod phase8_dep_anchors {
    use opentelemetry_proto as _;
    use tokio as _;
    use tonic as _;
}
