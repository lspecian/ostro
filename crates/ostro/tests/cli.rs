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

#[test]
fn help_lists_subcommands() {
    let out = Command::new(env!("CARGO_BIN_EXE_ostro"))
        .arg("--help")
        .output()
        .expect("run ostro");
    assert!(out.status.success());
    let s = String::from_utf8(out.stdout).unwrap();
    for sub in ["dev", "server", "openapi", "build-info"] {
        assert!(
            s.lines().any(|l| l.trim_start().starts_with(sub)),
            "--help lists {sub}: {s}"
        );
    }
}

#[test]
fn openapi_prints_the_served_document() {
    let out = Command::new(env!("CARGO_BIN_EXE_ostro"))
        .arg("openapi")
        .output()
        .expect("run ostro");
    assert!(out.status.success());
    let doc: serde_json::Value = serde_json::from_slice(&out.stdout).expect("JSON");
    let paths = doc["paths"].as_object().expect("paths object");
    assert!(paths.contains_key("/v1/workflows/{name}/runs"), "{paths:?}");
    // Same document as the library (and so as GET /openapi.json).
    let lib = serde_json::to_value(ostro_api::openapi()).unwrap();
    assert_eq!(doc, lib);
}

#[test]
fn server_with_missing_config_file_fails_with_one_line() {
    let out = Command::new(env!("CARGO_BIN_EXE_ostro"))
        .args([
            "server",
            "--print-config",
            "--config",
            "/nonexistent/ostro.toml",
        ])
        .env_remove("OSTRO_CONFIG")
        .output()
        .expect("run ostro");
    assert!(!out.status.success());
    let err = String::from_utf8(out.stderr).unwrap();
    assert_eq!(err.lines().count(), 1, "{err}");
    assert!(err.contains("/nonexistent/ostro.toml"), "{err}");
}
