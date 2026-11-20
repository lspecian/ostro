# Egress security: SSRF defence and allow-lists

Guest workflows ask the host to make HTTP requests (`http-fetch`). Without guards, a module could
reach the cloud metadata service, the node's own admin API or anything on the private network.
This document describes how Ostro stops that (FX-08), and how operators open exactly the
destinations a module needs.

Implementation: `crates/ostro-bridge/src/egress/`. End-to-end proof:
`crates/ostro-server/tests/fx_ssrf_e2e.rs` (SC4) and `crates/ostro-bridge/tests/ssrf_table.rs`.

---

## 1. Threat model

**Attacker:** the author of a deployed module, or anyone who can influence the URLs a module
fetches (user input, redirects from a third-party API, DNS records the attacker controls).

**Assets:**
- cloud instance metadata (IMDS) and the credentials it hands out (AWS/GCP/Azure/OCI/DO at
  `169.254.169.254`, ECS at `169.254.170.2`, EKS pod identity at `169.254.170.23`, Alibaba at
  `100.100.100.200`, AWS IPv6 at `fd00:ec2::254`, GCP IPv6 at `fd20:ce::254`);
- loopback services on the node (the Ostro API itself, databases, sidecars);
- the private network (RFC 1918, ULA, link-local);
- secret values resolved for other destinations ([secrets.md](secrets.md)).

**Out of scope:** an operator who configures `allow_private` or an `object` connection pointing
at an internal address. Operators are trusted.

**Principle:** the guarantees come from *where* checks happen, not from clever string matching.
URL strings are never pattern-matched for addresses. Addresses are classified after the WHATWG
URL parser has normalised them, and every DNS answer is classified before a socket is opened.

---

## 2. The three layers

### Layer 1: URL gate (`Egress::check`)

Runs before anything else, for the first request and for every redirect hop:

1. A `{{secret:` placeholder anywhere in the URL (raw or percent-encoded) → `placeholder`.
2. WHATWG parse with the `url` crate (`url` 2.5.8). Every inet_aton spelling (`2130706433`,
   `0177.0.0.1`, `0x7f.1`, `127.1`, `%31%32%37.0.0.1`, ideographic dots) normalises to
   `Host::Ipv4(127.0.0.1)` here. Unparsable hosts (`1.2.3.4.5`, `08.1`) → `parse`.
3. Userinfo present → `userinfo`. (`http://user:pw@evil.com@127.0.0.1/` really has host
   127.0.0.1.)
4. Scheme other than `http`/`https` → `scheme`.
5. The module has no egress policy → `not_allowed` (deny by default).
6. A trailing dot is stripped from the host.
7. **IP-literal hosts are classified here.** hyper-util never calls the resolver for a literal,
   so this is the only place a literal can be stopped. A blocked literal → `blocked`.
8. No rule admits the host → `ip_literal` (for a literal) or `not_allowed` (for a name).
9. A rule admits the host with another scheme → `scheme`; with another port → `port`.

### Layer 2: resolver (`EgressResolver`, a `reqwest::dns::Resolve`)

- Resolves names with hickory (system config), never getaddrinfo.
- Collects **every** A and AAAA answer and classifies each one. If **any** answer is blocked, the
  whole name fails (`blocked`). This defeats mixed answers and rebinding.
- Returns only the vetted addresses. hyper connects only to those, so the connection is pinned:
  there is no second resolution between the check and the connect, and pooled connections stay on
  the vetted address.
- A resolution failure → `dns`.

### Layer 3: client (`egress/client.rs`)

There is exactly one guarded reqwest client, built once:
- `.no_proxy()`: `HTTP_PROXY`, `HTTPS_PROXY` and `ALL_PROXY` in the server environment are
  ignored. Otherwise a proxy would resolve internal names on the attacker's behalf.
- `redirect::Policy::none()`: redirects are followed manually by the HTTP connector (§4).
- `.referer(false)`, no gzip/brotli decompression (decompression bombs, size-cap accounting).
- `connect_timeout` (5 s), per-attempt timeout, streaming body read with a hard byte cap.

A test greps ostro-bridge so that the reqwest client builder is called only from
`egress/client.rs`. A default-constructed client elsewhere would read proxy variables, follow 10
redirects and resolve through getaddrinfo, silently skipping every guard.

Every denial fails the effect `non-retryable{kind: "egress_denied", message: "egress denied:
<rule>"}` with `details: {"reason": <one of the reasons above>, "ip"?: <address>}`. Nothing is
sent.

---

## 3. The classifier and the IANA lists

An address is blocked when it, **or the IPv4 address embedded in it**, falls in a special-purpose
block, unless a node-level `allow_private` CIDR covers it.

Unwrapping:
- IPv4-mapped `::ffff:0:0/96` → the embedded IPv4.
- IPv4-compatible `::/96` (except `::` and `::1`) → the embedded IPv4.
- NAT64 well-known prefix `64:ff9b::/96` (RFC 6052) → the last 32 bits.
- 6to4 `2002::/16` → bits 16–48.
- NAT64 local-use `64:ff9b:1::/48` (RFC 8215) is **blocked outright**: its embedding is
  operator-defined. Teredo `2001::/32` is blocked outright.

**IPv4 blocks** (IANA IPv4 special-purpose registry):
`0.0.0.0/8`, `10.0.0.0/8`, `100.64.0.0/10`, `127.0.0.0/8`, `169.254.0.0/16`, `172.16.0.0/12`,
`192.0.0.0/24` (incl. 192.0.0.192, OCI legacy metadata), `192.0.2.0/24`, `192.88.99.0/24`,
`192.168.0.0/16`, `198.18.0.0/15`, `198.51.100.0/24`, `203.0.113.0/24`, `224.0.0.0/4`,
`240.0.0.0/4`, `255.255.255.255/32`.

**IPv6 blocks** (IANA IPv6 special-purpose registry):
`::/128`, `::1/128`, `64:ff9b:1::/48`, `100::/64`, `2001::/32`, `2001:2::/48`, `2001:10::/28`,
`2001:20::/28`, `2001:db8::/32`, `3fff::/20`, `5f00::/16`, `fc00::/7`, `fe80::/10`,
`fec0::/10`, `ff00::/8` (plus the unwrapped prefixes above).

---

## 4. Redirects (P-5)

The client never follows redirects. A reqwest redirect policy has no module context, so it could
apply neither the per-module allow-list nor the IP-literal classifier (the resolver never sees a
literal). Instead:

- **A request that carries a `{{secret:` placeholder never follows redirects.** The 3xx is
  returned to the guest as the response. (reqwest strips only `Authorization`, `Cookie` and
  `Proxy-Authorization` on cross-host redirects, so a templated `X-Api-Key` would otherwise be
  forwarded to wherever the redirect points.)
- An untemplated request is followed by the HTTP connector, up to `[egress] max_redirects` (5)
  hops. Every hop goes through the full URL gate again (allow-list **and** IP literals) and the
  resolver. Cross-origin hops drop `Authorization`, `Cookie`, `Proxy-Authorization` and the
  idempotency-key header. Past the limit, the last 3xx is the response.

---

## 5. Bypass table

Every row is denied with the default configuration (`allow_private = []`), even when the module's
policy lists the host explicitly. `ssrf_table.rs` covers the table at the bridge level and
`fx_ssrf_e2e.rs` through a real guest; the reason column is the `details.reason` observed there.

| Payload | What it really is | Blocked by | Reason |
|---------|-------------------|------------|--------|
| `http://127.0.0.1/` | loopback | URL gate, classifier `127/8` | `blocked` |
| `http://localhost/`, `http://localhost./` | resolves to 127.0.0.1 / ::1 | resolver | `blocked` |
| `http://2130706433/` | decimal → `127.0.0.1` | URL gate (normalised literal) | `blocked` |
| `http://0177.0.0.1/`, `http://0x7f.1/`, `http://127.1/`, `http://0x7f000001/` | octal / hex / short forms → `127.0.0.1` | URL gate | `blocked` |
| `http://%31%32%37.0.0.1/`, `http://127。0。0。1/` | percent-encoded / ideographic dots → `127.0.0.1` | URL gate | `blocked` |
| `http://0/`, `http://0.0.0.0/` | `0.0.0.0`, reaches localhost on Linux | classifier `0/8` | `blocked` |
| `http://169.254.169.254/` | AWS/GCP/Azure/OCI/DO IMDS | classifier `169.254/16` | `blocked` |
| `http://169.254.170.2/`, `http://169.254.170.23/` | ECS task metadata, EKS pod identity | classifier `169.254/16` | `blocked` |
| `http://metadata.google.internal/` | resolves to 169.254.169.254 | resolver | `blocked` |
| `http://100.100.100.200/` | Alibaba IMDS (CGNAT range) | classifier `100.64/10` | `blocked` |
| `http://[fd00:ec2::254]/`, `http://[fd00:ec2::23]/` | AWS IMDS over IPv6 | classifier `fc00::/7` | `blocked` |
| `http://[fd20:ce::254]/` | GCP metadata over IPv6 | classifier `fc00::/7` | `blocked` |
| `http://[::ffff:169.254.169.254]/`, `http://[::ffff:a9fe:a9fe]/`, `http://[0:0:0:0:0:ffff:127.0.0.1]/` | IPv4-mapped | unwrap, then classify v4 | `blocked` |
| `http://[::127.0.0.1]/` | IPv4-compatible (deprecated `::/96`) | unwrap, then classify v4 | `blocked` |
| `http://[64:ff9b::a9fe:a9fe]/` | NAT64 well-known embedding 169.254.169.254 | unwrap, then classify v4 | `blocked` |
| `http://[64:ff9b:1::1]/` | NAT64 local-use | blocked outright | `blocked` |
| `http://[2002:a9fe:a9fe::]/` | 6to4 embedding 169.254.169.254 | unwrap, then classify v4 | `blocked` |
| `http://[2001:0::1]/` | Teredo | blocked outright (`2001::/32`) | `blocked` |
| `http://[::]/`, `http://[::1]/`, `http://[fe80::1]/`, `http://[fec0::1]/`, `http://[ff02::1]/` | unspecified / loopback / link-local / site-local / multicast | classifier | `blocked` |
| `http://169.254.169.254.nip.io/`, attacker DNS → private, rebinding with TTL 0 | name resolving to a private address | resolver: every answer classified, any blocked answer fails the name; pinned connect | `blocked` |
| `302 Location: http://127.0.0.1/` or `http://[::1]/` | redirect to an internal address | templated: no redirects at all; untemplated: every hop re-checked | `blocked` (untemplated) / 3xx returned (templated) |
| `HTTPS_PROXY=http://attacker` in the server environment | a proxy resolving names for us | `.no_proxy()` (a recording listener sees 0 connections) | request goes direct |
| `http://user:pw@evil.com@127.0.0.1/` | userinfo confusion: host is 127.0.0.1 | URL gate rejects userinfo | `userinfo` |
| `http://1.2.3.4.5/`, `http://08.1/` | invalid hosts | URL parse | `parse` |
| `http://api.example.test/` with a module allowed only `https://api.example.test` | scheme downgrade | URL gate | `scheme` |
| `https://api.example.test:8443/` with only `https://api.example.test` allowed | other port | URL gate | `port` |
| `https://a.b.example.test/` with `https://*.example.test` allowed | two labels under a one-label wildcard | URL gate | `not_allowed` |
| any URL, module without an egress policy | — | deny by default | `not_allowed` |
| `{{secret:x}}` in the URL (raw or `%7B%7Bsecret:`) | secret exfiltration via the URL | URL gate | `placeholder` |

---

## 6. `allow_private` (operator override)

```toml
[egress]
allow_private = ["10.20.0.0/16", "fd12:3456::/48"]
```

- Node-level only (P-6). The CIDRs exempt matching addresses from the classifier for every module.
  A module still needs an allow rule for the host. Per-rule `allow_private` is deferred: the
  resolver is client-wide and cannot see which module rule matched.
- **The default is empty.** Test suites set `127.0.0.0/8` explicitly because their mock servers
  listen on loopback; a test asserts that the default blocks loopback. Never ship a config with
  loopback or `169.254.0.0/16` allowed.

---

## 7. Per-module policies (deny by default)

An `EgressPolicy{name: <module>, allow: [rule…]}` lists the destinations of one module. The module
name is the tenant-qualified **deployed** module name (`<tenant>/<name>`, or `<name>` in the
default tenant), never the module sha, so redeploys keep their policy. **A module without a policy
has no egress at all.**

Rules:
- `https://api.stripe.com`: exact host (normalised by the URL parser: lower case, IDNA, no
  trailing dot). The port defaults to 443 for `https` and 80 for `http`.
- `https://api.example.com:8443`: exact host and port.
- `https://*.example.com`: exactly one extra leading label (`a.example.com`, not
  `a.b.example.com`, not `example.com`). A bare `*` or a `*` anywhere else is refused.
- `http://10.20.1.5`, `https://[fd12::/16]:443`: IP literals and CIDRs. Only these rules admit an
  IP-literal URL, and only for addresses the classifier does not block (or that `allow_private`
  covers).
- `connection:<name>`: the module may use the named SQL/object connection (P-7). An unknown
  connection is `connection_unknown`; a connection not listed is `egress_denied`.

**Rules must be written in canonical form.** The admin API validates each rule and refuses
non-canonical spellings: `http://%31%32%37.0.0.1` ("invalid host") and `http://[::127.0.0.1]`
("invalid IPv6 literal") are rejected, while `http://127.0.0.1` and `http://[::7f00:1]` are
accepted. Write hosts lower case, IPv4 in dotted-quad form, and IPv6 in its compressed form inside
brackets. This affects only how rules are written: request URLs in any spelling are still
normalised by the parser and classified.

```sh
ostro egress set payments --allow https://api.stripe.com --allow connection:orders-db
ostro egress get payments
```

REST: `PUT /v1/egress/{module}` with `{"allow": [...]}`; gRPC: `ostro.api.v1.Connectors`.

---

## 8. Operator-endpoint exemption

Object-store connections (`kind = "object"`) point at operator-configured, trusted endpoints. They
use a separate **operator client**: the same no-proxy / no-redirect / no-referer settings, but
normal DNS resolution and no private-range block, so an S3-compatible store such as SeaweedFS on
localhost works. The SQL connector connects through sqlx to the configured host. Vault and
Kubernetes secret providers use their own clients configured from `[secrets]`.

The exemption covers only the endpoint the operator wrote into the connection or provider config.
A module reaches it only through `connection:<name>` in its egress policy; it never gets the
operator client for arbitrary URLs.

---

## 9. Environment proxies

The guarded client and the operator client both ignore `HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY`
and their lower-case forms. `fx_ssrf_e2e::env_proxies_are_ignored` sets all six to a recording
listener and asserts that it accepts zero connections. An operator who needs an egress proxy must
put it in the network path (transparent proxy, firewall); Ostro has no proxy setting in Phase 6.
