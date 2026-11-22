//! Phase 12: modules declared by 12-02

// Dependencies declared once by plan 12-01 for the harness, oracles and scenarios (12-02,
// 12-03, 12-06, 12-07, 12-09). Anchored so cargo's unused-dependency lint stays quiet until
// the owning modules consume them.
mod dep_anchors {
    use ostro_server as _;
    use ostro_testkit as _;
    use proptest as _;
}
