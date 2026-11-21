# TLS: client listener modes, cluster CA, mTLS and ACME

Phase 7 (SEC-01) puts TLS on the client listener (REST + gRPC + `/metrics` on one port) and
mTLS on the cluster listener the workers use. This document covers the `[api.tls]` modes, file
reload, ACME, the cluster CA and its files, mTLS, ports, the CLI, and what Phase 12 changes.

Implementation: `crates/ostro-server/src/tls/` (`ca.rs`, `dev_cert.rs`, `resolver.rs`,
`listener.rs`, `acme.rs`). Tests: `crates/ostro-server/tests/{tls_ca,tls_listener,acme}.rs`
and `crates/ostro/tests/worker_e2e.rs` (`listener` legs).

---

## 1. `[api.tls]` modes

```toml
[api.tls]
mode = "auto"              # auto | off | self-signed | files | acme
cert_file = "/tls/tls.crt" # files mode: PEM chain, leaf first
key_file = "/tls/tls.key"  # PKCS#8, SEC1 or PKCS#1
allow_self_signed = false  # server profile: allow `auto` to self-sign on a non-loopback listener
reload_poll = "30s"        # files mode: mtime poll (0 disables; SIGHUP always reloads)
```

| Mode | Behaviour |
|------|-----------|
| `off` | plaintext |
| `auto` (default, D-W8, user review) | loopback listener: plaintext (the `ostro dev` default `127.0.0.1:7700` and the `http://` ready line are unchanged); non-loopback in `ostro dev`: self-signed; non-loopback in `ostro server`: **refuses to start** unless `files`/`acme` is configured or `allow_self_signed = true` (then self-signed with a warning) |
| `self-signed` | a leaf issued by the cluster CA (`<data_dir>/tls/dev.crt`), chain `[leaf, CA]`, SANs hostname, `localhost`, `127.0.0.1`, `::1` and the listen IP; reused across restarts while valid for more than 30 days |
| `files` | the PEM files; reloaded on SIGHUP and when their mtime changes. A failed reload (unreadable file, key that does not match) keeps the served certificate and logs an error |
| `acme` | certificates from an ACME CA (§3) |

- The profile that decides the `auto` rule is the binary's (`ostro dev` or `ostro server`),
  passed explicitly to the server (`ServerOptions::with_tls_profile`, 07-22). A library caller
  that does not pass one gets the configuration-derived profile.
- TLS is served by axum-server with rustls (aws-lc-rs) and one certificate resolver; ALPN
  `h2, http/1.1`, so REST and gRPC share the port.
- The ready line shows the scheme and the served leaf's fingerprint:
  `ostro server listening on https://0.0.0.0:7700 (sha256 fingerprint <hex>) (data: ...) ready in N ms`.

## 2. The cluster CA and file layout (D-W5)

On first start the server creates a cluster-scoped CA (ECDSA P-256, bound to `cluster_id`) and a
node certificate, in about 100-200 ms; later starts reuse them in under 1 ms.

| File | Mode | Content |
|------|------|---------|
| `<data_dir>/tls/ca.crt` | 0644 | CA (CN `ostro cluster <cluster_id>`, CA:true pathlen 0, 10 years) |
| `<data_dir>/tls/ca.key` | 0600 | CA key (PKCS#8) |
| `<data_dir>/tls/node.crt` | 0644 | `[node leaf, CA]`: one SAN URI `ostro://<cluster_id>/node/<node_id>`, the `[cluster] advertise` hosts, `localhost`, `127.0.0.1`, `::1`; serverAuth + clientAuth, 1 year |
| `<data_dir>/tls/node.key` | 0600 | node key |
| `<data_dir>/tls/dev.crt`, `dev.key` | 0644 / 0600 | `self-signed` mode only |
| `<data_dir>/tls/acme/` | 0700 | ACME cache (§3) |

- The directory is 0700; every write is create-new temp file, fsync, rename, directory fsync.
- A half-present pair, a corrupt file, a key that does not match, or a CA of another cluster is
  a startup error naming the file. Nothing is regenerated silently.
- The CA signs worker client certificates at join (docs/workers.md §2). Its sha256 (DER) is the
  `--ca-hash` workers pin.

## 3. ACME (D-W6, D-W23)

```toml
[api.tls]
mode = "acme"

[api.tls.acme]
domains = ["ostro.example.com"]
email = "ops@example.com"
directory = "https://acme-v02.api.letsencrypt.org/directory"   # the default
# directory_ca_file = "/etc/ostro/pebble.minica.pem"           # trust only this CA for the directory
challenge = "tls-alpn-01"     # or "http-01"
http01_listen = "0.0.0.0:80"  # http-01 only
# cache_dir = "<data_dir>/tls/acme"
```

- Client: instant-acme 0.8.5 (aws-lc-rs). One account per directory, cached as
  `account-<16 hex of sha256(directory)>.json` (0600); the issued chain and its key
  (`cert.pem`, `key.pem`, 0600, a new P-256 key per certificate).
- **Challenges.** `tls-alpn-01` answers on the client listener itself (port **443** for a public
  CA): the resolver serves the challenge certificate (critical `acmeIdentifier` extension) only to
  ClientHellos that offer exactly `acme-tls/1`. `http-01` serves
  `/.well-known/acme-challenge/<token>` on `http01_listen` (port **80**) during the order.
- At start a cached certificate that covers every domain and is still valid is installed
  immediately; the directory is not contacted while it has more than 1/3 of its lifetime left.
  Until a first certificate exists the listener has none (the ready line omits the fingerprint).
- **Renewal** at 2/3 of the lifetime, scheduled from the injected node clock (Phase 10 F5;
  `tls::acme::set_clock` is called by the Phase 7 seam). Each sleep is capped at 1 h and the
  clock is re-read. A failure keeps the old certificate, logs a warning with `retry_in_s`, and
  retries with backoff `clamp(60 s x 2^(n-1) x [0.75, 1.25), 1 min, 1 h)`.
- **Testing.** Fake-ACME unit tests always run. Pebble (loopback only) proves the order flow and
  real HTTP-01 / TLS-ALPN-01 validation:

  ```bash
  GOBIN=$PWD/target/pebble/bin go install github.com/letsencrypt/pebble/v2/cmd/pebble@v2.10.3
  # config: listen 127.0.0.1:34000, httpPort 35002, tlsPort 35001 (07-10-SUMMARY "Pebble outcome")
  OSTRO_TEST_ACME_DIRECTORY=https://127.0.0.1:34000/dir OSTRO_TEST_ACME_CA=<pebble.minica.pem> \
  OSTRO_TEST_ACME_HTTP_PORT=35002 OSTRO_TEST_ACME_TLS_PORT=35001 \
    cargo test -p ostro-server --test acme --locked -- pebble --test-threads=1
  ```

  The same leg runs in the optional `acme-pebble` `workflow_dispatch` CI job. No test contacts a
  public directory. Let's Encrypt staging on a public host is human_needed.

## 4. mTLS on the cluster listener

- `[cluster] listen` (server profile `0.0.0.0:7701`, dev `127.0.0.1:0`) serves the node chain and
  **always requires a client certificate** chaining to the cluster CA or to
  `[cluster.tls] client_ca` (cert-manager-issued worker certificates, D-W25).
- Identity comes from the certificate's single `ostro://` SAN URI: join-issued workers
  `ostro://<cluster>/worker/<token_id>/<worker_id>` (the token must still exist), external
  workers `ostro://<cluster>/worker/ext/<worker_id>`. Node identities are refused in Phase 7.
- Workers trust only the cluster CA for this listener (from the join response, pinned by
  `--ca-hash`).

## 5. Ports

| Port | Use |
|------|-----|
| 7700 | client listener: REST, gRPC, `/metrics`, worker join, KEDA demand endpoint; also the TLS-ALPN-01 challenge when it is exposed as 443 |
| 7701 | cluster listener (mTLS, workers) |
| 80 | ACME HTTP-01 only, during orders (`http01_listen`) |
| 443 | public ACME TLS-ALPN-01: map it to the client listener |

## 6. The CLI over TLS

`ostro --server https://host:7700 ...` (or `OSTRO_SERVER`) verifies the server with the platform
roots (`files` / `acme` listeners). For a self-signed listener pass the cluster CA:
`--ca-file <data_dir>/tls/ca.crt` (or `OSTRO_CA_FILE`); only that CA is trusted then. `ostro
backup --server https://...` takes the same flag.

## 7. Phase 12 note

Phase 7 keeps the CA on the node that created it. A multi-node cluster needs one shared CA:
`ClusterCa::import_pem` installs it, node certificates already carry the node-identity SAN, and
Phase 12 decides how the CA reaches the other nodes (candidate: a sealed meta secret
`__ostro/cluster-ca`) and adds node identities to the cluster listener for peer replication.
