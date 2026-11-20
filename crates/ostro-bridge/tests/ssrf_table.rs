//! FX-08 bypass table (06-RESEARCH § SSRF defence): every payload is denied with the
//! default `[egress]` config, by the URL gate or by the resolver, with the expected
//! `Denied::reason()`. No network: names resolve through `StaticLookup`.
//!
//! Every host and IP in the table is also listed in the module's allow rules, so the
//! address rows can only be denied by the classifier (W2).

use std::net::IpAddr;
use std::sync::Arc;

use ostro_bridge::egress::{Denied, Egress, EgressConfig, StaticLookup};
use ostro_meta::records::EgressPolicy;

/// Names the table resolves (no network).
fn lookup() -> StaticLookup {
    StaticLookup::new()
        .with("localhost", &["127.0.0.1", "::1"])
        .with("metadata.google.internal", &["169.254.169.254"])
        .with("169.254.169.254.nip.io", &["169.254.169.254"])
        .with("rebind.attacker.test", &["93.184.216.34", "10.0.0.1"])
        .with("v6only.attacker.test", &["fd00:ec2::254"])
        .with("mapped.attacker.test", &["::ffff:169.254.169.254"])
        .with("api.example.com", &["93.184.216.34"])
}

/// Allow rules naming every host and literal below, on http and https.
fn policy() -> EgressPolicy {
    let hosts = [
        "localhost",
        "metadata.google.internal",
        "169.254.169.254.nip.io",
        "rebind.attacker.test",
        "v6only.attacker.test",
        "mapped.attacker.test",
        "api.example.com",
        "evil.com",
        "127.0.0.0/8",
        "0.0.0.0/8",
        "169.254.0.0/16",
        "100.64.0.0/10",
        "10.0.0.0/8",
        "[::/0]",
    ];
    let mut allow = Vec::new();
    for h in hosts {
        allow.push(format!("http://{h}"));
        allow.push(format!("https://{h}"));
    }
    EgressPolicy {
        name: "acme/ssrf".into(),
        allow,
        ext: Vec::new(),
    }
}

fn egress(cfg: EgressConfig) -> Egress {
    Egress::new(cfg, Arc::new(lookup())).unwrap()
}

/// Gate, then (for names) the resolver, as the HTTP connector does.
async fn decide(e: &Egress, url: &str) -> Result<Vec<IpAddr>, Denied> {
    let p = policy();
    let checked = e.check(Some(&p), url)?;
    match checked.url.host() {
        Some(url::Host::Domain(d)) => e.resolve(d).await,
        Some(url::Host::Ipv4(v4)) => Ok(vec![v4.into()]),
        Some(url::Host::Ipv6(v6)) => Ok(vec![v6.into()]),
        None => unreachable!("gate admits only URLs with a host"),
    }
}

/// (payload, expected reason)
const TABLE: &[(&str, &str)] = &[
    // loopback
    ("http://127.0.0.1/", "blocked"),
    ("http://localhost/", "blocked"),
    ("http://localhost./", "blocked"),
    // literal encodings of 127.0.0.1
    ("http://2130706433/", "blocked"),
    ("http://0177.0.0.1/", "blocked"),
    ("http://0x7f.1/", "blocked"),
    ("http://127.1/", "blocked"),
    ("http://0x7f000001/", "blocked"),
    ("http://%31%32%37.0.0.1/", "blocked"),
    ("http://127\u{3002}0\u{3002}0\u{3002}1/", "blocked"),
    // this-network
    ("http://0/", "blocked"),
    ("http://0.0.0.0/", "blocked"),
    // metadata v4
    ("http://169.254.169.254/latest/meta-data/", "blocked"),
    ("http://169.254.170.2/v2/credentials", "blocked"),
    ("http://169.254.170.23/v1/credentials", "blocked"),
    (
        "http://metadata.google.internal/computeMetadata/v1/",
        "blocked",
    ),
    ("http://100.100.100.200/latest/meta-data/", "blocked"),
    // metadata v6 (ULA)
    ("http://[fd00:ec2::254]/", "blocked"),
    ("http://[fd00:ec2::23]/", "blocked"),
    ("http://[fd20:ce::254]/", "blocked"),
    // IPv4-mapped / compatible
    ("http://[::ffff:169.254.169.254]/", "blocked"),
    ("http://[::ffff:a9fe:a9fe]/", "blocked"),
    ("http://[0:0:0:0:0:ffff:127.0.0.1]/", "blocked"),
    ("http://[::127.0.0.1]/", "blocked"),
    // NAT64
    ("http://[64:ff9b::a9fe:a9fe]/", "blocked"),
    ("http://[64:ff9b:1::808:808]/", "blocked"),
    // 6to4, Teredo
    ("http://[2002:a9fe:a9fe::]/", "blocked"),
    ("http://[2001:0:4136:e378:8000:63bf:3fff:fdd2]/", "blocked"),
    // unspecified / loopback / link-local / site-local / multicast
    ("http://[::]/", "blocked"),
    ("http://[::1]/", "blocked"),
    ("http://[fe80::1]/", "blocked"),
    ("http://[fec0::1]/", "blocked"),
    ("http://[ff02::1]/", "blocked"),
    // names resolving to blocked addresses, mixed answers
    ("http://169.254.169.254.nip.io/", "blocked"),
    ("http://rebind.attacker.test/", "blocked"),
    ("https://v6only.attacker.test/", "blocked"),
    ("https://mapped.attacker.test/", "blocked"),
    // userinfo confusion
    ("http://user:pw@evil.com@127.0.0.1/", "userinfo"),
    ("http://user@api.example.com/", "userinfo"),
    // invalid hosts
    ("http://1.2.3.4.5/", "parse"),
    ("http://08.1/", "parse"),
    // secrets never go in URLs
    (
        "https://api.example.com/?k={{secret:stripe}}",
        "placeholder",
    ),
];

#[tokio::test]
async fn every_bypass_payload_is_denied_by_default() {
    let e = egress(EgressConfig::default());
    let mut failures = Vec::new();
    for (url, want) in TABLE {
        match decide(&e, url).await {
            Ok(ips) => failures.push(format!("{url}: ALLOWED {ips:?}")),
            Err(d) if d.reason() != *want => {
                failures.push(format!("{url}: denied with {:?}, want {want}", d))
            }
            Err(_) => {}
        }
    }
    assert!(
        failures.is_empty(),
        "bypass table failures:\n{}",
        failures.join("\n")
    );
}

#[tokio::test]
async fn unlisted_literals_are_ip_literal_denied() {
    let e = egress(EgressConfig::default());
    let p = EgressPolicy {
        name: "acme/none".into(),
        allow: vec!["https://api.example.com".into()],
        ext: Vec::new(),
    };
    for url in [
        "https://8.8.8.8/",
        "https://[2606:4700::1111]/",
        "https://134744072/",
    ] {
        assert_eq!(
            e.check(Some(&p), url).unwrap_err().reason(),
            "ip_literal",
            "{url}"
        );
    }
    // A blocked literal reports `blocked` even when unlisted.
    assert_eq!(
        e.check(Some(&p), "https://10.0.0.1/").unwrap_err().reason(),
        "blocked"
    );
}

#[tokio::test]
async fn control_public_name_passes() {
    let e = egress(EgressConfig::default());
    let ips = decide(&e, "https://api.example.com/v1/charges")
        .await
        .unwrap();
    assert_eq!(ips, vec!["93.184.216.34".parse::<IpAddr>().unwrap()]);
    // No policy: deny by default.
    assert_eq!(
        e.check(None, "https://api.example.com/")
            .unwrap_err()
            .reason(),
        "not_allowed"
    );
}

#[tokio::test]
async fn default_config_blocks_loopback_and_allow_private_opens_it() {
    assert!(EgressConfig::default().allow_private.is_empty());
    let closed = egress(EgressConfig::default());
    assert_eq!(
        decide(&closed, "http://127.0.0.1/")
            .await
            .unwrap_err()
            .reason(),
        "blocked"
    );

    let open = egress(EgressConfig {
        allow_private: vec!["127.0.0.0/8".parse().unwrap()],
        ..EgressConfig::default()
    });
    let lo: IpAddr = "127.0.0.1".parse().unwrap();
    assert_eq!(decide(&open, "http://127.0.0.1/").await.unwrap(), vec![lo]);
    // localhost still resolves to ::1 too, which 127.0.0.0/8 does not cover: denied.
    assert_eq!(
        decide(&open, "http://localhost/")
            .await
            .unwrap_err()
            .reason(),
        "blocked"
    );
    // Other private ranges stay blocked.
    assert_eq!(
        decide(&open, "http://10.0.0.1/")
            .await
            .unwrap_err()
            .reason(),
        "blocked"
    );
    assert_eq!(
        decide(&open, "http://169.254.169.254/")
            .await
            .unwrap_err()
            .reason(),
        "blocked"
    );
}

#[tokio::test]
async fn denial_maps_to_egress_denied_failure() {
    let e = egress(EgressConfig::default());
    let d = decide(&e, "http://metadata.google.internal/")
        .await
        .unwrap_err();
    let ostro_types::journal::Failure::NonRetryable(info) = d.to_failure() else {
        panic!("not NonRetryable");
    };
    assert_eq!(info.kind, "egress_denied");
    let v: serde_json::Value = serde_json::from_slice(&info.details.unwrap().data).unwrap();
    assert_eq!(v["reason"], "blocked");
    assert_eq!(v["ip"], "169.254.169.254");
}
