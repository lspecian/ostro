//! CLI-02 through the real binary (plan 04-19): `ostro server` zero-config defaults,
//! TOML < env < flags precedence via `--print-config`, a live start from a TOML file,
//! and one-line failures for bad configuration.
#![cfg(unix)]

mod common;

use std::path::Path;

use common::{SERIAL, Spawned, T, free_addr, http, run_to_end, server};

/// The value printed for `key` inside `[section]` of a `--print-config` dump.
fn toml_value(dump: &str, section: &str, key: &str) -> Option<String> {
    let header = format!("[{section}]");
    let mut inside = false;
    for line in dump.lines() {
        let line = line.trim();
        if line.starts_with('[') {
            inside = line == header;
            continue;
        }
        if inside
            && let Some((k, v)) = line.split_once(" = ")
            && k == key
        {
            return Some(v.to_owned());
        }
    }
    None
}

fn write(dir: &Path, name: &str, body: &str) -> String {
    let p = dir.join(name);
    std::fs::write(&p, body).expect("write config");
    p.to_str().expect("utf-8 path").to_owned()
}

fn print_config(cwd: &Path, args: &[&str], env: &[(&str, &str)]) -> String {
    let mut cmd = server(cwd, &["--print-config"]);
    cmd.args(args);
    for (k, v) in env {
        cmd.env(k, v);
    }
    let (st, out, err) = run_to_end(cmd);
    assert!(st.success(), "--print-config failed: {err}");
    out
}

#[test]
fn print_config_defaults() {
    let cwd = tempfile::tempdir().expect("cwd");
    let out = print_config(cwd.path(), &[], &[]);
    assert_eq!(
        toml_value(&out, "api", "listen").as_deref(),
        Some("\"0.0.0.0:7700\""),
        "{out}"
    );
    assert_eq!(
        toml_value(&out, "node", "partitions").as_deref(),
        Some("16"),
        "{out}"
    );
    assert_eq!(
        toml_value(&out, "node", "data_dir").as_deref(),
        Some("\"./ostro-data\""),
        "{out}"
    );
    assert_eq!(
        toml_value(&out, "executor", "lease_ttl").as_deref(),
        Some("\"30s\""),
        "{out}"
    );
    // Printing never starts the server or creates the data dir.
    assert!(!cwd.path().join("ostro-data").exists());
}

#[test]
fn toml_then_env_then_flags() {
    let cwd = tempfile::tempdir().expect("cwd");
    let file = write(
        cwd.path(),
        "x.toml",
        "[api]\nlisten = \"127.0.0.1:1111\"\n\n[executor]\nlease_ttl = \"30s\"\nslots = 8\n",
    );

    // TOML over the defaults.
    let out = print_config(cwd.path(), &["-c", &file], &[]);
    assert_eq!(
        toml_value(&out, "api", "listen").as_deref(),
        Some("\"127.0.0.1:1111\"")
    );
    assert_eq!(toml_value(&out, "executor", "slots").as_deref(), Some("8"));

    // OSTRO_CONFIG names the file too.
    let out = print_config(cwd.path(), &[], &[("OSTRO_CONFIG", &file)]);
    assert_eq!(toml_value(&out, "executor", "slots").as_deref(), Some("8"));

    // Env beats TOML.
    let env = [
        ("OSTRO_EXECUTOR__LEASE_TTL", "5s"),
        ("OSTRO_API__LISTEN", "127.0.0.1:2222"),
    ];
    let out = print_config(cwd.path(), &["-c", &file], &env);
    assert_eq!(
        toml_value(&out, "executor", "lease_ttl").as_deref(),
        Some("\"5s\""),
        "{out}"
    );
    assert_eq!(
        toml_value(&out, "api", "listen").as_deref(),
        Some("\"127.0.0.1:2222\"")
    );
    assert_eq!(toml_value(&out, "executor", "slots").as_deref(), Some("8"));

    // Flags beat env and TOML.
    let out = print_config(
        cwd.path(),
        &[
            "-c",
            &file,
            "--listen",
            "127.0.0.1:3333",
            "--data-dir",
            "/srv/x",
        ],
        &env,
    );
    assert_eq!(
        toml_value(&out, "api", "listen").as_deref(),
        Some("\"127.0.0.1:3333\"")
    );
    assert_eq!(
        toml_value(&out, "node", "data_dir").as_deref(),
        Some("\"/srv/x\"")
    );
    assert_eq!(
        toml_value(&out, "executor", "lease_ttl").as_deref(),
        Some("\"5s\"")
    );
}

#[tokio::test]
async fn live_start_with_toml() {
    let _g = SERIAL.lock().await;
    let tmp = tempfile::tempdir().expect("tmp");
    let addr = free_addr();
    let data = tmp.path().join("data");
    let file = write(
        tmp.path(),
        "x.toml",
        &format!(
            "[node]\ndata_dir = \"{}\"\npartitions = 4\n\n[api]\nlisten = \"{addr}\"\n\n[log]\nlevel = \"warn\"\n",
            data.display()
        ),
    );
    let client = http();
    let mut s = Spawned::spawn(server(tmp.path(), &["-c", &file]));
    let took = s.wait_readyz(&client, addr).await;
    let ready = s.ready_line(T);
    println!(
        "ostro server -c x.toml (P=4, on disk): /readyz {} ms after spawn",
        took.as_millis()
    );
    assert_eq!(ready.name, "ostro server");
    assert_eq!(ready.addr, addr);
    assert_eq!(ready.data_dir, data);
    assert!(!ready.ephemeral);
    s.stop_clean("TERM");
    assert!(data.is_dir());

    // The stored partition count is enforced: P=8 on the same dir fails with both values.
    let (st, _out, err) = run_to_end({
        let mut c = server(tmp.path(), &["-c", &file]);
        c.env("OSTRO_NODE__PARTITIONS", "8");
        c
    });
    assert!(!st.success());
    let err = err.trim();
    assert_eq!(err.lines().count(), 1, "{err}");
    assert!(err.contains('4') && err.contains('8'), "{err}");
}

#[test]
fn bad_config_fails() {
    let cwd = tempfile::tempdir().expect("cwd");

    // Unknown key in the file: named in a one-line error, non-zero exit.
    let file = write(cwd.path(), "bad.toml", "[api]\nbogus_key = 1\n");
    for args in [
        vec!["--print-config", "-c", file.as_str()],
        vec!["-c", file.as_str()],
    ] {
        let (st, _out, err) = run_to_end(server(cwd.path(), &args));
        assert!(!st.success(), "{args:?}");
        let err = err.trim();
        assert_eq!(err.lines().count(), 1, "{err}");
        assert!(err.contains("bogus_key"), "{err}");
    }

    // Unknown key in a sectioned env var.
    let mut cmd = server(cwd.path(), &["--print-config"]);
    cmd.env("OSTRO_EXECUTOR__NOPE", "1");
    let (st, _out, err) = run_to_end(cmd);
    assert!(!st.success());
    assert!(err.contains("nope"), "{err}");

    // Bad duration value.
    let mut cmd = server(cwd.path(), &["--print-config"]);
    cmd.env("OSTRO_EXECUTOR__LEASE_TTL", "soon");
    let (st, _out, err) = run_to_end(cmd);
    assert!(!st.success());
    assert!(err.contains("soon"), "{err}");

    // Missing explicit file.
    let (st, _out, err) = run_to_end(server(cwd.path(), &["-c", "/nonexistent/x.toml"]));
    assert!(!st.success());
    assert!(err.contains("/nonexistent/x.toml"), "{err}");
}
