//! Address classification against the IANA special-purpose registries (FX-08).
//!
//! An address is blocked when it, or the IPv4 address embedded in it (IPv4-mapped,
//! IPv4-compatible, NAT64 well-known prefix, 6to4), falls in a special-purpose block, unless
//! a node-level `allow_private` CIDR covers it (P-6).
//!
//! Embedded-address unwrapping is what stops `[::ffff:169.254.169.254]`,
//! `[64:ff9b::a9fe:a9fe]` and `[2002:a9fe:a9fe::]` from reaching the metadata service.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::LazyLock;

use ipnet::{IpNet, Ipv4Net, Ipv6Net};

/// Blocked IPv4 ranges. Source: IANA IPv4 Special-Purpose Address Registry
/// (iana-ipv4-special-registry), plus multicast (RFC 5771) and reserved 240/4.
const V4_BLOCKED: &[&str] = &[
    "0.0.0.0/8",          // "This network" (RFC 791); 0.0.0.0 reaches localhost on Linux
    "10.0.0.0/8",         // Private-Use (RFC 1918)
    "100.64.0.0/10",      // Shared Address Space / CGNAT (RFC 6598); Alibaba IMDS 100.100.100.200
    "127.0.0.0/8",        // Loopback (RFC 1122)
    "169.254.0.0/16",     // Link Local (RFC 3927); AWS/GCP/Azure/OCI IMDS, ECS, EKS pod identity
    "172.16.0.0/12",      // Private-Use (RFC 1918)
    "192.0.0.0/24",       // IETF Protocol Assignments (RFC 6890); incl. 192.0.0.192 (OCI legacy)
    "192.0.2.0/24",       // Documentation TEST-NET-1 (RFC 5737)
    "192.88.99.0/24",     // Deprecated 6to4 Relay Anycast (RFC 7526)
    "192.168.0.0/16",     // Private-Use (RFC 1918)
    "198.18.0.0/15",      // Benchmarking (RFC 2544)
    "198.51.100.0/24",    // Documentation TEST-NET-2 (RFC 5737)
    "203.0.113.0/24",     // Documentation TEST-NET-3 (RFC 5737)
    "224.0.0.0/4",        // Multicast (RFC 5771)
    "240.0.0.0/4",        // Reserved (RFC 1112)
    "255.255.255.255/32", // Limited Broadcast (RFC 919)
];

/// Blocked IPv6 ranges. Source: IANA IPv6 Special-Purpose Address Registry
/// (iana-ipv6-special-registry), plus fec0::/10 (deprecated site-local, RFC 3879) and
/// multicast ff00::/8 (RFC 4291). `::ffff:0:0/96`, `::/96`, `64:ff9b::/96` and `2002::/16`
/// are not listed: they are unwrapped by [`effective_v4`] and classified as IPv4.
const V6_BLOCKED: &[&str] = &[
    "::/128",         // Unspecified (RFC 4291)
    "::1/128",        // Loopback (RFC 4291)
    "64:ff9b:1::/48", // IPv4-IPv6 Translat., local use (RFC 8215): embedding is operator-defined
    "100::/64",       // Discard-Only (RFC 6666)
    "2001::/32",      // TEREDO (RFC 4380)
    "2001:2::/48",    // Benchmarking (RFC 5180)
    "2001:10::/28",   // Deprecated ORCHID (RFC 4843)
    "2001:20::/28",   // ORCHIDv2 (RFC 7343)
    "2001:db8::/32",  // Documentation (RFC 3849)
    "3fff::/20",      // Documentation (RFC 9637)
    "5f00::/16",      // Segment Routing SIDs (RFC 9602)
    "fc00::/7",       // Unique-Local (RFC 4193); AWS IMDS fd00:ec2::254, GCP fd20:ce::254
    "fe80::/10",      // Link-Local Unicast (RFC 4291)
    "fec0::/10",      // Site-local, deprecated (RFC 3879)
    "ff00::/8",       // Multicast (RFC 4291)
];

static V4_TABLE: LazyLock<Vec<Ipv4Net>> = LazyLock::new(|| {
    V4_BLOCKED
        .iter()
        .map(|s| s.parse().expect("static IPv4 block table"))
        .collect()
});

static V6_TABLE: LazyLock<Vec<Ipv6Net>> = LazyLock::new(|| {
    V6_BLOCKED
        .iter()
        .map(|s| s.parse().expect("static IPv6 block table"))
        .collect()
});

/// The IPv4 address embedded in `ip`, if any:
/// - `::ffff:0:0/96` (IPv4-mapped): the low 32 bits;
/// - `::/96` (IPv4-compatible, deprecated), except `::` and `::1`: the low 32 bits;
/// - `64:ff9b::/96` (NAT64 well-known prefix, RFC 6052): the low 32 bits;
/// - `2002::/16` (6to4, RFC 3056): bits 16..48.
pub fn effective_v4(ip: Ipv6Addr) -> Option<Ipv4Addr> {
    if let Some(v4) = ip.to_ipv4_mapped() {
        return Some(v4);
    }
    let s = ip.segments();
    let low = |a: u16, b: u16| Ipv4Addr::new((a >> 8) as u8, a as u8, (b >> 8) as u8, b as u8);
    if s[..6] == [0, 0, 0, 0, 0, 0] {
        // `::` and `::1` are unspecified/loopback, not compatible addresses (both blocked
        // by the IPv6 table anyway).
        return (s[6] != 0 || s[7] > 1).then(|| low(s[6], s[7]));
    }
    if s[..6] == [0x64, 0xff9b, 0, 0, 0, 0] {
        return Some(low(s[6], s[7]));
    }
    if s[0] == 0x2002 {
        return Some(low(s[1], s[2]));
    }
    None
}

fn v4_hit(v4: Ipv4Addr) -> bool {
    V4_TABLE.iter().any(|n| n.contains(&v4))
}

fn v6_hit(v6: Ipv6Addr) -> bool {
    V6_TABLE.iter().any(|n| n.contains(&v6))
}

fn allowed(ip: IpAddr, allow_private: &[IpNet]) -> bool {
    allow_private.iter().any(|n| n.contains(&ip))
}

/// Whether `ip` is blocked: it, or its embedded IPv4 address, is in a special-purpose
/// range, and no `allow_private` CIDR covers it (or its embedded IPv4 address).
pub fn is_blocked(ip: IpAddr, allow_private: &[IpNet]) -> bool {
    match ip {
        IpAddr::V4(v4) => !allowed(ip, allow_private) && v4_hit(v4),
        IpAddr::V6(v6) => {
            let eff = effective_v4(v6);
            if allowed(ip, allow_private) || eff.is_some_and(|v4| allowed(v4.into(), allow_private))
            {
                return false;
            }
            v6_hit(v6) || eff.is_some_and(v4_hit)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn b(s: &str) -> bool {
        is_blocked(s.parse().unwrap(), &[])
    }

    /// One address inside each IPv4 row and one just outside it.
    #[test]
    fn v4_rows() {
        let rows = [
            ("0.255.255.255", "1.0.0.0"),
            ("10.255.255.255", "11.0.0.0"),
            ("100.64.0.0", "100.63.255.255"),
            ("100.127.255.255", "100.128.0.0"),
            ("127.0.0.1", "128.0.0.0"),
            ("169.254.169.254", "169.255.0.0"),
            ("172.31.255.255", "172.32.0.0"),
            ("172.16.0.0", "172.15.255.255"),
            ("192.0.0.192", "192.0.1.0"),
            ("192.0.2.1", "192.0.3.0"),
            ("192.88.99.1", "192.88.100.0"),
            ("192.168.1.1", "192.169.0.0"),
            ("198.19.255.255", "198.20.0.0"),
            ("198.18.0.0", "198.17.255.255"),
            ("198.51.100.7", "198.51.101.0"),
            ("203.0.113.7", "203.0.114.0"),
            ("224.0.0.1", "223.255.255.255"),
            ("254.255.255.255", "223.255.255.254"),
            ("255.255.255.255", "8.8.8.8"),
        ];
        for (inside, outside) in rows {
            assert!(b(inside), "{inside} must be blocked");
            assert!(!b(outside), "{outside} must not be blocked");
        }
        assert_eq!(V4_TABLE.len(), 16);
    }

    /// One address inside each IPv6 row and one just outside it.
    #[test]
    fn v6_rows() {
        let rows = [
            ("::", "2606:4700::1111"),
            ("::1", "2606:4700::1111"),
            ("64:ff9b:1::1", "64:ff9b:2::"),
            ("100::1", "100:0:0:1::"),
            ("2001:0:4136:e378:8000:63bf:3fff:fdd2", "2001:1::"),
            ("2001:2::1", "2001:2:1::"),
            ("2001:10::1", "2001:30::"),
            ("2001:2f::1", "2001:30::"),
            ("2001:db8::1", "2001:db9::"),
            ("3fff:fff::1", "3fff:1000::"),
            ("5f00::1", "5f01::"),
            ("fd00:ec2::254", "fe00::"),
            ("fc00::1", "fbff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"),
            ("fe80::1", "fe7f:ffff:ffff:ffff:ffff:ffff:ffff:ffff"),
            ("febf::1", "fe00::"),
            ("fec0::1", "fe00::"),
            ("ff02::1", "fe00::1"),
        ];
        for (inside, outside) in rows {
            assert!(b(inside), "{inside} must be blocked");
            assert!(!b(outside), "{outside} must not be blocked");
        }
        assert_eq!(V6_TABLE.len(), 15);
    }

    #[test]
    fn unwrap_embedded_v4() {
        let e = |s: &str| effective_v4(s.parse().unwrap());
        let imds = Some(Ipv4Addr::new(169, 254, 169, 254));
        assert_eq!(e("::ffff:169.254.169.254"), imds);
        assert_eq!(e("::ffff:a9fe:a9fe"), imds);
        assert_eq!(e("::169.254.169.254"), imds);
        assert_eq!(e("64:ff9b::a9fe:a9fe"), imds);
        assert_eq!(e("2002:a9fe:a9fe::"), imds);
        assert_eq!(e("2002:a9fe:a9fe:1234::1"), imds);
        assert_eq!(e("::"), None);
        assert_eq!(e("::1"), None);
        assert_eq!(e("::2"), Some(Ipv4Addr::new(0, 0, 0, 2)));
        assert_eq!(e("2606:4700::1111"), None);
        assert_eq!(e("64:ff9b:1::a9fe:a9fe"), None); // blocked whole, never unwrapped

        for s in [
            "::ffff:169.254.169.254",
            "::ffff:127.0.0.1",
            "::127.0.0.1",
            "64:ff9b::a9fe:a9fe",
            "64:ff9b::10.0.0.1",
            "2002:a9fe:a9fe::",
            "2002:7f00:1::",
            "64:ff9b:1::8.8.8.8",
        ] {
            assert!(b(s), "{s} must be blocked");
        }
        for s in [
            "::ffff:8.8.8.8",
            "64:ff9b::808:808",
            "2002:808:808::1",
            "::8.8.8.8",
        ] {
            assert!(!b(s), "{s} must not be blocked");
        }
    }

    #[test]
    fn public_addresses_pass() {
        for s in [
            "1.1.1.1",
            "8.8.8.8",
            "93.184.216.34",
            "2606:4700::1111",
            "2a00:1450::1",
        ] {
            assert!(!b(s), "{s}");
        }
    }

    #[test]
    fn allow_private_overrides() {
        let lo: Vec<IpNet> = vec!["127.0.0.0/8".parse().unwrap()];
        let ip = |s: &str| s.parse::<IpAddr>().unwrap();
        assert!(!is_blocked(ip("127.0.0.1"), &lo));
        assert!(!is_blocked(ip("::ffff:127.0.0.1"), &lo));
        assert!(is_blocked(ip("::1"), &lo), "::1 needs its own entry");
        assert!(is_blocked(ip("10.0.0.1"), &lo));
        assert!(is_blocked(ip("169.254.169.254"), &lo));
        let v6: Vec<IpNet> = vec!["fd00:ec2::/32".parse().unwrap()];
        assert!(!is_blocked(ip("fd00:ec2::254"), &v6));
        assert!(is_blocked(ip("fd20:ce::254"), &v6));
    }

    proptest! {
        #[test]
        fn mapped_equals_v4(raw in any::<u32>()) {
            let v4 = Ipv4Addr::from(raw);
            prop_assert_eq!(is_blocked(v4.into(), &[]), is_blocked(v4.to_ipv6_mapped().into(), &[]));
        }

        #[test]
        fn nat64_equals_v4(raw in any::<u32>()) {
            let v4 = Ipv4Addr::from(raw);
            let o = v4.octets();
            let nat64 = Ipv6Addr::new(
                0x64, 0xff9b, 0, 0, 0, 0,
                u16::from_be_bytes([o[0], o[1]]),
                u16::from_be_bytes([o[2], o[3]]),
            );
            prop_assert_eq!(is_blocked(v4.into(), &[]), is_blocked(nat64.into(), &[]));
        }
    }
}
