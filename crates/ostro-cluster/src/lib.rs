//! Phase 12: modules declared by 12-02

// Dependencies declared once by plan 12-01 for later Phase 12 plans (12-05 meta peer/authz,
// 12-10 controller, 12-11 fetch, 12-12 install, 12-14 service). Anchored so cargo's
// unused-dependency lint stays quiet until the owning modules consume them.
mod dep_anchors {
    use ostro_meta as _;
    use ostro_partition as _;
    use ostro_proto as _;
    use x509_parser as _;
}
