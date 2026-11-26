//! 14-01: the crate and its module tree link (filled by 14-09/14-10/14-23).

// Path resolution fails to compile if a module of the 14-01 contract is missing.
#[allow(unused_imports)]
use ostro_jepsen::{
    bench,
    checker::{duplicate, fencing, lost},
    history,
    nemesis::{clock, disk, kill, partition},
    report, run, soak, workload,
};

#[test]
fn crate_links() {
    assert!(module_path!().contains("skeleton"));
}
