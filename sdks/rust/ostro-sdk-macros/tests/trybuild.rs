//! Compile-pass / compile-fail UI tests for `#[workflow]` and `#[step]` (host target).
//!
//! Regenerate `.stderr` snapshots with `TRYBUILD=overwrite cargo test -p ostro-sdk-macros`
//! after a deliberate diagnostic change (they are toolchain-dependent).

#[test]
fn ui() {
    let t = trybuild::TestCases::new();
    t.pass("tests/ui/pass_basic.rs");
    t.compile_fail("tests/ui/fail_missing_name.rs");
    t.compile_fail("tests/ui/fail_bad_retry.rs");
}
