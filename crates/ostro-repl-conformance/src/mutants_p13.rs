//! The Phase 13 mutant matrix (D-P13-25): each of the eight
//! `ostro_types::compat::MUTANTS` failpoints → the scenarios that must fail with it.
//! 13-02 stub; 13-18 fills it, 13-20 makes it mandatory in CI (`OSTRO_P13_REQUIRE_ALL=1`).

/// `(mutant, scenarios)` pairs (empty until 13-18).
pub fn matrix() -> Vec<(&'static str, Vec<&'static str>)> {
    vec![]
}
