//! eph-fix: an ephemeral server whose shm candidate is too small puts its data dir in the
//! OS temp dir, and a large module deploys there. Before the fix, `ostro dev --ephemeral`
//! always used `/dev/shm`, and in a default Docker container (64 MB shm) the first Python
//! deploy failed with ENOSPC (HTTP 503 "storing the module failed").
//!
//! This test binary sets the process-wide shm-candidate override, so it holds only this
//! test.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::time::Duration;

use common::{ServerOpts, TestServer, guest_wasm, server_config};
use reqwest::StatusCode;
use sha2::{Digest, Sha256};

/// Bytes of padding: the module alone is larger than Docker's 64 MB shm, and still below
/// the 64 MiB `max_module_bytes` default (the guest is ~160 KB).
const PAD: usize = 62 * 1024 * 1024;

fn leb128(mut n: usize, out: &mut Vec<u8>) {
    loop {
        let byte = u8::try_from(n & 0x7f).expect("7 bits");
        n >>= 7;
        if n == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

/// `wasm` plus a trailing custom section of `pad` bytes (same workflows, larger module).
fn padded(wasm: &[u8], pad: usize) -> Vec<u8> {
    let name = b"ostro-eph-fix-pad";
    let mut body = Vec::with_capacity(pad + name.len() + 8);
    leb128(name.len(), &mut body);
    body.extend_from_slice(name);
    body.resize(body.len() + pad, 0xA5);
    let mut out = wasm.to_vec();
    out.push(0); // custom section id
    leb128(body.len(), &mut out);
    out.extend_from_slice(&body);
    out
}

fn sha_hex(b: &[u8]) -> String {
    Sha256::digest(b)
        .iter()
        .map(|x| format!("{x:02x}"))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn small_shm_falls_back_to_temp_dir_and_large_deploy_succeeds() {
    // A real, writable candidate whose free space is below the threshold.
    let fake_shm = tempfile::Builder::new()
        .prefix("ostro-eph-fix-shm-")
        .tempdir()
        .expect("candidate");
    ostro_server::ephemeral::set_shm_candidate_for_tests(Some((
        fake_shm.path().to_path_buf(),
        u64::MAX,
    )));
    assert!(!ostro_server::ephemeral::would_use_shm());

    let unused = tempfile::tempdir().expect("unused data_dir");
    let opts = ServerOpts {
        ephemeral: true,
        ..ServerOpts::default()
    };
    let mut s = TestServer::start_cfg(server_config(unused.path(), &opts), None).await;
    s.wait_ready(Duration::from_secs(5)).await;
    let dir = s.h().data_dir().to_path_buf();
    assert!(s.h().is_ephemeral());
    assert!(
        dir.starts_with(std::env::temp_dir()),
        "{} not in the temp dir",
        dir.display()
    );
    assert!(!dir.starts_with(fake_shm.path()), "{}", dir.display());
    assert!(
        dir.file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.starts_with("ostro-")),
        "{}",
        dir.display()
    );
    // Nothing was left on the rejected candidate (the write probe is removed).
    assert_eq!(std::fs::read_dir(fake_shm.path()).expect("ls").count(), 0);

    let wasm = padded(&guest_wasm("sdk-basic"), PAD);
    assert!(wasm.len() > 64_000_000, "larger than Docker's default shm");
    assert!(
        wasm.len() as u64 <= s.cfg.api.max_module_bytes,
        "{}",
        wasm.len()
    );
    let sha = sha_hex(&wasm);
    let r = s
        .http
        .put(s.url(&format!("/v1/modules/{sha}?language=rust")))
        .header("content-type", "application/wasm")
        .body(wasm.clone())
        .send()
        .await
        .expect("PUT module");
    let st = r.status();
    let body: serde_json::Value = r.json().await.expect("json body");
    assert_eq!(st, StatusCode::CREATED, "{body}");
    assert_eq!(body["module"]["size_bytes"], wasm.len());
    let raw: [u8; 32] = Sha256::digest(&wasm).into();
    assert!(s.h().runtime().prepared(&raw).is_some(), "module prepared");

    s.shutdown().await;
    assert!(!dir.exists(), "{} removed after shutdown", dir.display());
    ostro_server::ephemeral::set_shm_candidate_for_tests(None);
}
