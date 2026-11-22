//! 10-03: the Phase 10 CLI surface of the real `ostro` binary before any implementation
//! lands: `--help` lists the local verbs, each stub exits 2 naming its plan, replay
//! selector conflicts are usage errors, and the shared `p10` spawn helper parses the
//! frozen ready line of a real `ostro dev`.

#[path = "common/p10.rs"]
mod p10;

use p10::*;

/// The `Commands:` section of `ostro --help`: the verb names.
fn root_verbs() -> Vec<String> {
    let dir = tempfile::tempdir().expect("tempdir");
    let (code, out, err) = run_cli_in(dir.path(), &["--help"], &[]);
    assert_eq!(code, 0, "{err}");
    let mut verbs = Vec::new();
    let mut in_cmds = false;
    for line in out.lines() {
        if line.trim_end() == "Commands:" {
            in_cmds = true;
            continue;
        }
        if in_cmds {
            if !line.starts_with("  ") {
                break;
            }
            if let Some(v) = line.split_whitespace().next() {
                verbs.push(v.to_owned());
            }
        }
    }
    verbs
}

#[test]
fn help_lists_phase10_verbs() {
    let verbs = root_verbs();
    for v in ["init", "build", "toolchain", "doctor", "replay"] {
        assert!(verbs.iter().any(|x| x == v), "{v} missing from {verbs:?}");
    }
    // the 05-14 / 11-14 verbs are still there
    for v in [
        "run",
        "deploy",
        "rerun",
        "dlq",
        "bulk",
        "limits",
        "build-info",
    ] {
        assert!(verbs.iter().any(|x| x == v), "{v} missing from {verbs:?}");
    }
}

#[test]
fn verb_help_exits_0() {
    let dir = tempfile::tempdir().expect("tempdir");
    for args in [
        &["init", "--help"][..],
        &["build", "--help"],
        &["toolchain", "--help"],
        &["toolchain", "install", "--help"],
        &["toolchain", "list", "--help"],
        &["toolchain", "prune", "--help"],
        &["doctor", "--help"],
        &["replay", "--help"],
    ] {
        let (code, out, err) = run_cli_in(dir.path(), args, &[]);
        assert_eq!(code, 0, "{args:?}: {err}");
        assert!(out.contains("Usage: ostro"), "{args:?}: {out}");
    }
    let (_, out, _) = run_cli_in(dir.path(), &["replay", "--help"], &[]);
    for flag in [
        "[RUN]...",
        "--run <ID>",
        "--sample <N>",
        "--workflow",
        "--status",
        "--corpus <DIR>",
        "--module <PATH>",
        "--allow-missing",
        "--json",
        "--server",
    ] {
        assert!(out.contains(flag), "replay --help lacks {flag}: {out}");
    }
    let (_, out, _) = run_cli_in(dir.path(), &["build", "--help"], &[]);
    assert!(!out.contains("--in-container"), "hidden flag shown: {out}");
}

#[test]
fn stubs_exit_2_naming_their_plan() {
    let dir = tempfile::tempdir().expect("tempdir");
    // `replay` is implemented (10-11, tests/replay_cli.rs).
    let (args, verb, plan) = (&["build", "--json"][..], "build", "10-12");
    let (code, out, err) = run_cli_in(dir.path(), args, &[]);
    assert_eq!(code, 2, "{args:?}: {out}{err}");
    assert_eq!(
        err.trim_end(),
        format!("ostro: error: {verb} is not yet implemented ({plan})"),
        "{args:?}"
    );
    assert!(out.is_empty(), "{args:?}: {out}");
    // the stub created nothing in the cwd
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
}

#[test]
fn replay_selector_conflicts_exit_2() {
    let dir = tempfile::tempdir().expect("tempdir");
    for args in [
        &["replay", "r1", "--sample", "3", "--module", "x.wasm"][..],
        &["replay", "--run", "r1", "--corpus", "d"],
        &["replay", "--sample", "3", "--corpus", "d"],
        &["replay", "r1"],
        &["replay", "--module", "x.wasm"],
    ] {
        let (code, _, err) = run_cli_in(dir.path(), args, &[]);
        assert_eq!(code, 2, "{args:?}: {err}");
        assert!(
            !err.contains("not yet implemented"),
            "{args:?} reached the stub: {err}"
        );
        assert!(err.contains("Usage:"), "{args:?}: {err}");
    }
}

#[test]
fn spawn_dev_parses_ready_line_and_serves() {
    let dev = spawn_dev(&[], &[("OSTRO_LOG__LEVEL", "warn")]);
    assert!(dev.ready.ephemeral, "{:?}", dev.ready);
    assert!(dev.addr.ip().is_loopback());
    assert_ne!(dev.addr.port(), 0, "{:?}", dev.ready);
    assert!(!dev.ready.name.is_empty());
    assert!(!dev.data.as_os_str().is_empty());
    let (status, body) = http_get(dev.addr, "/v1/workflows");
    assert_eq!(status, 200, "{body}\nstderr: {}", dev.stderr_so_far());
    // a REST verb through the helper reaches the same server
    let (code, v, err) = dev.cli_json(&["ls"]);
    assert_eq!(code, 0, "{v}\n{err}");
    assert!(v.is_object(), "{v}");
}
