//! ostro-testkit: offline fault injection for Phase 2 storage tests.
//!
//! kill -9 never produces torn writes (the page cache survives a process
//! kill), so torn, zeroed and truncated tails are injected offline on closed
//! files instead (02-RESEARCH Pitfall 2).

pub mod corrupt;
pub mod fs;

/// Number of proptest cases: `PROPTEST_CASES` when set and parseable, else `default`.
pub fn proptest_cases(default: u32) -> u32 {
    unimplemented!("{default}")
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
