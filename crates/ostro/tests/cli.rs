use std::process::Command;

#[test]
fn build_info_reports_mimalloc_and_target() {
    let out = Command::new(env!("CARGO_BIN_EXE_ostro"))
        .arg("build-info")
        .output()
        .expect("run ostro");
    assert!(out.status.success());
    let s = String::from_utf8(out.stdout).unwrap();
    assert_eq!(s.lines().count(), 1, "single line: {s}");
    assert!(s.contains("allocator=mimalloc"), "{s}");
    assert!(s.contains("target="), "{s}");
}

#[test]
fn version_flag_prints_version() {
    let out = Command::new(env!("CARGO_BIN_EXE_ostro"))
        .arg("--version")
        .output()
        .expect("run ostro");
    assert!(out.status.success());
    let s = String::from_utf8(out.stdout).unwrap();
    assert!(s.contains(env!("CARGO_PKG_VERSION")), "{s}");
}
