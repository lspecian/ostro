---
phase: 06-effects-bridge-connectors-secrets
plan: 04
subsystem: effects-bridge
tags: [egress, ssrf, security, reqwest, hickory, fx-08]
requires:
  - "06-02 ostro-bridge contract (origin::{Origin, OriginPattern}, meta_view::MetaView, kinds::EGRESS_DENIED) and every Phase 6 pin (6c9db25)"
provides:
  - "ostro_bridge::egress: EgressConfig ([egress] section), classifier, URL gate, allow-list rules, EgressResolver, the single guarded client, operator client, read_capped"
  - "SSRF bypass table test (tests/ssrf_table.rs) and client behaviour tests (tests/egress_client.rs)"
affects: [06-07, 06-10, 06-11, 06-12]
tech-stack:
  added: []
  patterns:
    - "Checks live where the bypass happens: IP literals in the URL gate (hyper-util skips the resolver for them), names in the reqwest resolver (any blocked answer fails the name; vetted addrs pin the connect)"
    - "Resolver denials travel through reqwest's error chain as ResolveDenied; Denied::from_error recovers them (W2)"
key-files:
  created:
    - crates/ostro-bridge/src/egress/classify.rs
    - crates/ostro-bridge/src/egress/config.rs
    - crates/ostro-bridge/src/egress/policy.rs
    - crates/ostro-bridge/src/egress/url_gate.rs
    - crates/ostro-bridge/src/egress/resolver.rs
    - crates/ostro-bridge/src/egress/client.rs
    - crates/ostro-bridge/tests/ssrf_table.rs
    - crates/ostro-bridge/tests/egress_client.rs
  modified:
    - crates/ostro-bridge/src/egress/mod.rs
key-decisions:
  - "A blocked IP literal reports reason `blocked` even when no rule lists it; an unblocked unlisted literal reports `ip_literal`"
  - "IP/CIDR allow rules: `scheme://<ip>[:port]` (any url-normalised spelling) and `scheme://<ip>/<len>[:port]`, `scheme://[v6/len][:port]`"
  - "Trailing host dots are stripped both from request URLs (CheckedUrl.url is rewritten) and from host rules"
  - "read_capped returns ReadError {TooLarge(TooLarge{cap, seen}), Body(reqwest::Error)} rather than a bare TooLarge, so body transport errors are not mislabelled response_too_large"
  - "HickoryLookup falls back to hickory's GOOGLE public-DNS config when /etc/resolv.conf is unreadable (hickory 0.26 has no ResolverConfig::default)"
metrics:
  duration: "~45 min"
  completed: 2026-11-20
---

# Phase 6 Plan 04: Egress Core Summary

**This plan builds the SSRF-hardened egress layer, the FX-08 mechanics. It has four parts:**
- **Classifier:** the IANA special-purpose v4/v6 tables, which unwrap IPv4-mapped, IPv4-compatible, NAT64 (64:ff9b::/96) and 6to4 addresses. 64:ff9b:1::/48 and Teredo are blocked outright.
- **URL gate:** parses with the `url` crate, applies the allow-list, and classifies IP literals.
- **Resolver:** a `reqwest::dns::Resolve` built on hickory. If any answer is blocked, the whole name fails. Otherwise it returns only the vetted addresses, so connections are pinned to them.
- **Clients:** one guarded client (`no_proxy`, `Policy::none`, no referer, no compression) and an operator client, plus a streaming body reader with a byte cap.

**Every row of the research bypass table is denied with the default config and the expected reason.**

## Final API (consumed by 06-07, 06-10, 06-11, 06-12)

```rust
// ostro_bridge::egress (mod.rs re-exports)
pub struct EgressConfig {                       // #[serde(default, deny_unknown_fields)]
    pub allow_private: Vec<ipnet::IpNet>,       // ["127.0.0.0/8", "::1"] strings; default []
    pub connect_timeout: Duration,              // "5s"
    pub default_timeout: Duration,              // "30s"
    pub max_response_bytes: u64,                // 4 MiB; int or "4MiB"
    pub hard_max_response_bytes: u64,           // 64 MiB
    pub max_redirects: u8,                      // 5
    pub rate_max_wait: Duration,                // "30s"
    pub idempotency_window: Duration,           // "24h"
    pub safety_margin: Duration,                // "5m"
}
impl EgressConfig { pub fn validate(&self) -> Result<(), String>;
                    pub fn response_cap(&self, requested: Option<u64>) -> u64; }
pub fn config::parse_duration(s: &str) -> Result<Duration, String>;
pub fn config::parse_bytes(s: &str) -> Result<u64, String>;

pub enum Denied { Parse(String), Scheme, Userinfo, Port, Placeholder, NotAllowed,
                  IpLiteral(IpAddr), Blocked(IpAddr), Dns(String) }   // Clone, PartialEq, Error
impl Denied {
    pub fn reason(&self) -> &'static str;       // parse|scheme|userinfo|port|placeholder|not_allowed|ip_literal|blocked|dns
    pub fn to_failure(&self) -> ostro_types::journal::Failure;  // NonRetryable{kind:"egress_denied", message:"egress denied: <rule>", details: application/json {"reason", "ip"?}}
    pub fn from_error(err: &(dyn std::error::Error + 'static)) -> Option<Denied>; // walks source() for ResolveDenied
}
pub struct CheckedUrl { pub url: url::Url, pub origin: Origin, pub rule: AllowRule }

pub enum AllowRule { Host { scheme: String, port: u16, pattern: OriginPattern },
                     Ip { scheme: String, port: u16, net: IpNet },
                     Connection(String) }
impl AllowRule { pub fn scheme_port(&self) -> Option<(&str, u16)>; }   // + Display
pub fn parse_rule(s: &str) -> Result<AllowRule, RuleError>;   // RuleError { rule, reason }
pub fn allows_connection(policy: Option<&EgressPolicy>, name: &str) -> bool;
pub fn policy::rules(policy: &EgressPolicy) -> Vec<AllowRule>;  // invalid entries skipped (warn)

#[async_trait] pub trait Lookup: Send + Sync { async fn lookup(&self, host: &str) -> std::io::Result<Vec<IpAddr>>; }
pub struct HickoryLookup { .. }  impl HickoryLookup { pub fn new() -> std::io::Result<Self>; }
pub struct StaticLookup(pub BTreeMap<String, Vec<IpAddr>>);
impl StaticLookup { pub fn new() -> Self; pub fn with(self, host: &str, ips: &[&str]) -> Self; }
pub struct EgressResolver;  impl EgressResolver { pub fn new(lookup: Arc<dyn Lookup>, allow_private: Arc<[IpNet]>) -> Self; }
pub struct ResolveDenied { pub host: String, pub denied: Denied }  // Display "egress_denied: reason=<r> <host>: <msg>"
pub async fn resolver::vet(lookup: &dyn Lookup, allow_private: &[IpNet], host: &str) -> Result<Vec<IpAddr>, Denied>;

pub enum EgressError { Config(String), Client(reqwest::Error) }
pub struct Egress;
impl Egress {
    pub fn new(cfg: EgressConfig, lookup: Arc<dyn Lookup>) -> Result<Self, EgressError>;
    pub fn config(&self) -> &EgressConfig;
    pub fn check(&self, policy: Option<&EgressPolicy>, url: &str) -> Result<CheckedUrl, Denied>;
    pub async fn resolve(&self, host: &str) -> Result<Vec<IpAddr>, Denied>;
    pub fn client(&self) -> &reqwest::Client;
    pub fn operator_client(cfg: &EgressConfig) -> Result<reqwest::Client, EgressError>;
}
pub async fn read_capped(resp: reqwest::Response, cap: u64) -> Result<bytes::Bytes, ReadError>;
pub enum ReadError { TooLarge(TooLarge), Body(reqwest::Error) }
pub struct TooLarge { pub cap: u64, pub seen: u64 }
pub fn effective_v4(ip: Ipv6Addr) -> Option<Ipv4Addr>;
pub fn is_blocked(ip: IpAddr, allow_private: &[IpNet]) -> bool;
```

The gate checks in this order:
1. `{{secret:` anywhere in the URL, raw or percent-encoded, fails with `placeholder`.
2. The `url` parse.
3. Userinfo.
4. Scheme other than http/https.
5. No policy: `not_allowed`.
6. Trailing dot stripped from the host.
7. A blocked IP literal: `blocked`.
8. No rule admits the host: `ip_literal` for a literal, `not_allowed` for a name.
9. A rule admits the host but has a different scheme: `scheme`.
10. Same scheme but a different port: `port`.

## Verified library names (MEDIUM in research, now confirmed)

- **reqwest 0.13.5:**
  - `reqwest::dns::{Resolve, Resolving, Name, Addrs}` exist as the research assumed.
  - `Resolving = Pin<Box<dyn Future<Output = Result<Addrs, BoxError>> + Send>>` and `Addrs = Box<dyn Iterator<Item = SocketAddr> + Send>`.
  - `Name::as_str()` exists, and `Name: FromStr`.
  - `ClientBuilder::dns_resolver<R: IntoResolve>(R)` accepts a bare `R: Resolve + 'static`; we pass the `EgressResolver` by value.
  - `no_proxy()`, `redirect(Policy::none())`, `referer(bool)` and `connect_timeout(Duration)` behave as documented.
  - With the bridge's features (`rustls`, `stream`, `json`, `http2`), no `Accept-Encoding` header is sent. A test asserts this.
- **hickory-resolver 0.26.3:**
  - `hickory_resolver::TokioResolver::builder_tokio() -> Result<ResolverBuilder<TokioRuntimeProvider>, NetError>` reads the system config.
  - The fallback is `Resolver::builder_with_config(ResolverConfig::udp_and_tcp(&hickory_resolver::config::GOOGLE), hickory_resolver::net::runtime::TokioRuntimeProvider::default())`.
  - `.build() -> Result<Resolver<_>, NetError>` and `lookup_ip(host).await?.iter()`.
  - `ResolverConfig` has no `Default`.

## Verification

- `cargo test -p ostro-bridge --locked -- --test-threads=1`: lib **36 passed**, egress_client **5 passed**, ssrf_table **5 passed**, doc 0. The same result with `--features failpoints`. Re-run on main after rebasing past 08-04 (`e655e73`): same counts.
- `cargo test -p ostro-bridge --lib egress`: 23 passed. These cover the classifier rows and proptests, the config, policy, gate, resolver and Denied shape.
- `cargo clippy -p ostro-bridge --all-targets -D warnings`: clean with default features, `--all-features` and `--no-default-features`. `cargo build -p ostro-bridge` has 0 warnings. `cargo fmt --check` is clean.
- **Sensitivity check:** temporarily removing `.no_proxy()` and switching to `Policy::limited(3)` made `env_proxies_are_ignored` and `redirects_are_not_followed` fail. The proxy listener served the request. The change was reverted.
- **Not run:** the full `bash scripts/ci-local.sh` gate. Four other executors were building at the same time, and the gate takes over 30 minutes (failpoint suites and the campaign). This plan changes only the egress module and two new test files in ostro-bridge, and no other crate uses the module yet.

## Deviations from Plan

1. **[Rule 2] `read_capped` error type.** It returns `ReadError { TooLarge(TooLarge), Body(reqwest::Error) }` instead of `Result<_, TooLarge>`. A failing body stream must not be reported as `response_too_large`.
2. **[Rule 3] The config test uses a serde_json map, not TOML.** ostro-bridge has no `toml` dev-dependency, and only 06-02 may edit Cargo files. The serde derive does not depend on the format. 06-07 feeds the same struct from the TOML server config.
3. **API additions beyond the interface sketch:**
   - `EgressConfig::{validate, response_cap}`, `config::{parse_duration, parse_bytes}`;
   - `Egress::{config, resolve}`, `Denied::from_error`, `ResolveDenied`, `resolver::vet`, `policy::rules`;
   - `AllowRule::scheme_port`, `StaticLookup::with`.
   - `HickoryLookup` holds the resolver, so it is not a unit struct; it is built with `HickoryLookup::new()`.
   - `EgressConfig` also accepts the `hard_max_response_bytes` key.
4. **One commit per task.** Each task was committed with its tests and implementation together, with no separate RED commits, to keep the number of CAS updates on a busy main low.

## Notes for later plans

- **06-10 (HTTP connector):**
  - Call `egress.check(policy, url)` before sending and on **every** redirect hop.
  - Send through `egress.client()`.
  - Map a `reqwest::Error` with `Denied::from_error(&err)` to `d.to_failure()`. Transport errors without a `ResolveDenied` become `http_transport`.
  - Read bodies with `read_capped(resp, egress.config().response_cap(target_cap))`.
- **06-12 (objects):** use `Egress::operator_client(&cfg)` for operator endpoints.
- **06-07 / 06-10 config:** embed `EgressConfig` as `[egress]`.
- **lib.rs `dep_anchors`:** still anchors reqwest, hickory_resolver, ipnet, jiff and bytes. That is harmless; 06-02 owns lib.rs.

## Commits

| Task | Commit | Description |
|------|--------|-------------|
| 1 | 8b9defb | feat(06-04): egress classifier tables, embedded-v4 unwrap and [egress] config |
| 2 | 1ba40bc | feat(06-04): URL gate, allow-list rules, vetting resolver and SSRF bypass table |
| 3 | e655e73 | feat(06-04): single guarded egress client, operator client and capped body reader |

## Self-Check: PASSED

- FOUND: crates/ostro-bridge/src/egress/{mod,config,classify,policy,url_gate,resolver,client}.rs, crates/ostro-bridge/tests/{ssrf_table,egress_client}.rs
- FOUND: commits 8b9defb, 1ba40bc, e655e73 on main
