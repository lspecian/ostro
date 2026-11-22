//! Scenarios whose behaviour is not integrated yet (D-P12-19). One list per owning wave:
//! [`core`] (12-06), [`rest`] (12-07), [`process`] (12-17). A pending scenario is reported
//! `PENDING` in normal runs and must fail in `red_run`.

pub mod core;
pub mod process;
pub mod rest;

/// Whether `name` is on any pending list.
pub fn is_pending(name: &str) -> bool {
    all_lists().iter().any(|list| list.contains(&name))
}

/// The three lists, in owner order.
pub fn all_lists() -> [&'static [&'static str]; 3] {
    [core::PENDING, rest::PENDING, process::PENDING]
}
