//! ostro-worker fault-injection suite (`--features failpoints`; filled by 07-07/07-21).
//! Runs single-threaded in ci-local's "failpoint suites" step (the fail registry is global).

#[cfg(feature = "failpoints")]
#[test]
fn failpoints_feature_compiles() {
    let _scenario = fail::FailScenario::setup();
}
