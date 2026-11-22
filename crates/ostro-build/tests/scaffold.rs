//! 10-02 scaffold checks: the fixture server serves what it publishes, the unsafe archives
//! contain the raw entries the 10-07 extraction tests rely on, and the contract stub
//! answers `NotImplemented`.

#[path = "common/fixture_server.rs"]
mod fixture_server;

use std::io::Read as _;
use std::time::Duration;

use fixture_server::{FixtureServer, sha256_hex};

fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("reqwest client")
}

async fn fetch(url: &str) -> (reqwest::StatusCode, Vec<u8>) {
    let resp = client().get(url).send().await.expect("request");
    let status = resp.status();
    (status, resp.bytes().await.expect("body").to_vec())
}

/// `(path, link target, entry type)` of every entry, read raw.
fn entries(tar_bytes: &[u8]) -> Vec<(String, Option<String>, tar::EntryType, u32)> {
    let mut ar = tar::Archive::new(tar_bytes);
    ar.entries()
        .expect("entries")
        .map(|e| {
            let e = e.expect("entry");
            let path = String::from_utf8(e.path_bytes().into_owned()).expect("utf8 path");
            let link = e
                .link_name_bytes()
                .map(|l| String::from_utf8(l.into_owned()).expect("utf8 link"));
            let mode = e.header().mode().expect("mode");
            (path, link, e.header().entry_type(), mode)
        })
        .collect()
}

fn gunzip(bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    flate2::read::GzDecoder::new(bytes)
        .read_to_end(&mut out)
        .expect("gunzip");
    out
}

#[tokio::test]
async fn downloads_good_archive_and_checks_sha() {
    let (base, shas, counts) = FixtureServer::start().await;
    let (status, body) = fetch(&format!("{base}/good.tar.gz")).await;
    assert_eq!(status, reqwest::StatusCode::OK);
    assert_eq!(sha256_hex(&body), shas["good.tar.gz"]);
    assert_eq!(counts.get("good.tar.gz"), 1);

    let es = entries(&gunzip(&body));
    let tool = es
        .iter()
        .find(|e| e.0 == "tool-1.0/bin/tool")
        .expect("tool entry");
    assert_eq!(tool.2, tar::EntryType::Regular);
    assert_eq!(tool.3 & 0o777, 0o755);

    // Any directory prefix serves the same file (mirror rewrite).
    let (_, again) = fetch(&format!("{base}/dist/v1/good.tar.gz")).await;
    assert_eq!(again, body);
    assert_eq!(counts.get("good.tar.gz"), 2);

    let (status, _) = fetch(&format!("{base}/missing.tar.gz")).await;
    assert_eq!(status, reqwest::StatusCode::NOT_FOUND);
    assert_eq!(counts.total(), 3);
}

#[tokio::test]
async fn every_published_sha_matches_except_badsha() {
    let s = FixtureServer::spawn_default().await;
    for (name, sha) in &s.shas {
        let (status, body) = fetch(&s.url(name)).await;
        assert_eq!(status, reqwest::StatusCode::OK, "{name}");
        if *name == "badsha.tar.gz" {
            assert_ne!(&sha256_hex(&body), sha, "badsha must not match its pin");
        } else {
            assert_eq!(&sha256_hex(&body), sha, "{name}");
        }
    }
    assert_eq!(
        s.files["big.bin"].len(),
        fixture_server::BIG_BIN_BYTES,
        "big.bin size"
    );

    // xz decodes to the same tar as the gz.
    let mut xz_tar = Vec::new();
    lzma_rust2::XzReader::new(&s.files["good.tar.xz"][..], false)
        .read_to_end(&mut xz_tar)
        .expect("unxz");
    assert_eq!(xz_tar, gunzip(&s.files["good.tar.gz"]));
}

#[tokio::test]
async fn unsafe_archives_hold_raw_entries() {
    let s = FixtureServer::spawn_default().await;
    let raw = |name: &str| entries(&gunzip(&s.files[name]));
    use tar::EntryType::{Link, Regular, Symlink};

    assert_eq!(raw("traversal.tar.gz")[0].0, "../evil");
    assert_eq!(raw("abs.tar.gz")[0].0, "/tmp/evil");
    assert_eq!(raw("abs.tar.gz")[0].2, Regular);

    let esc = raw("symlink-escape.tar.gz");
    assert_eq!(
        (esc[1].0.as_str(), esc[1].1.as_deref(), esc[1].2),
        ("bin/x", Some("../../outside"), Symlink)
    );
    let chain = raw("symlink-chain.tar.gz");
    assert_eq!(
        (chain[0].0.as_str(), chain[0].1.as_deref()),
        ("a", Some("b"))
    );
    assert_eq!(
        (chain[1].0.as_str(), chain[1].1.as_deref()),
        ("b", Some("../../outside"))
    );
    let ok = raw("symlink-ok.tar.gz");
    assert!(
        ok.iter()
            .any(|e| e.0 == "bin/npm" && e.1.as_deref() == Some("../lib/npm-cli.js"))
    );
    let hard = raw("hardlink-escape.tar.gz");
    assert_eq!(
        (hard[1].0.as_str(), hard[1].1.as_deref(), hard[1].2),
        ("bin/passwd", Some("../../etc/passwd"), Link)
    );
}

#[tokio::test]
async fn tampered_mirror_publishes_same_shas_but_other_bytes() {
    let a = FixtureServer::spawn_default().await;
    let b = FixtureServer::spawn_tampered().await;
    assert_eq!(a.shas, b.shas, "generation is deterministic");
    let (_, body) = fetch(&b.url("good.tar.gz")).await;
    assert_ne!(sha256_hex(&body), b.shas["good.tar.gz"]);
    assert_eq!(b.counts.get("good.tar.gz"), 1);
    assert_eq!(a.counts.total(), 0);
}

#[tokio::test]
async fn slow_bin_stalls_after_the_headers() {
    let s = FixtureServer::spawn_default().await;
    let mut resp = tokio::time::timeout(
        Duration::from_secs(5),
        client().get(s.url("slow.bin")).send(),
    )
    .await
    .expect("headers arrive")
    .expect("request");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let first = tokio::time::timeout(Duration::from_secs(5), resp.chunk())
        .await
        .expect("first chunk arrives")
        .expect("chunk")
        .expect("some bytes");
    assert_eq!(first.len(), fixture_server::SLOW_FIRST_CHUNK);
    let next = tokio::time::timeout(Duration::from_millis(500), resp.chunk()).await;
    assert!(next.is_err(), "slow.bin must stall after its first chunk");
}

#[test]
fn build_stub_answers_not_implemented() {
    let err = ostro_build::build(ostro_build::BuildOptions::default()).unwrap_err();
    assert!(matches!(
        err,
        ostro_build::BuildError::NotImplemented("10-12")
    ));
    // 10-08 replaced the empty-tar build.rs stub with the real bundle (tests/sdk_bundle.rs)
    assert!(ostro_build::sdk_bundle::SDK_BUNDLE.len() > 1024);
    assert!(ostro_build::pipeline::IGNORE.contains(&"node_modules/"));
}
