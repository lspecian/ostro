# Installing Ostro

Ostro is one binary, `ostro`, for every role: the CLI, `ostro dev`, `ostro server` and
`ostro worker`. This guide covers the release artefacts and how to verify them, the install
script, macOS, the container image, Kubernetes and building from source, then the settings a
first server needs.

## 1. Release artefacts

Each release `v<V>` publishes:

| Asset | What |
|---|---|
| `ostro-<V>-x86_64-unknown-linux-musl.tar.gz` | Linux x86_64, static musl binary (runs on any distribution) |
| `ostro-<V>-aarch64-unknown-linux-musl.tar.gz` | Linux arm64, static musl binary |
| `ostro-<V>-x86_64-apple-darwin.tar.gz`, `ostro-<V>-aarch64-apple-darwin.tar.gz` | macOS binaries, signed with a Developer ID and notarized |
| `ostro-<V>-macos-universal.pkg` | macOS installer (universal binary into `/usr/local/bin`), signed, notarized and stapled |
| `SHA256SUMS` | sha256 of every asset |
| `SHA256SUMS.minisig` | minisign signature of `SHA256SUMS` with the release key |
| `minisign.pub` | the release public key (also below) |
| `install.sh` | the install script (§3) |
| `VERSION` | the version number, used by `install.sh` to find the latest release |

Each tarball holds `ostro`, `LICENSE`, `NOTICE` and `README.md`. The Linux tarballs are
reproducible: the same source and `SOURCE_DATE_EPOCH` give the same bytes. Releases also push
a container image and the Helm chart to the registry named in the release notes (§5, §6).

## 2. Verifying a download

`SHA256SUMS` lists every asset, and `SHA256SUMS.minisig` signs that list with the Ostro release
key. Check the signature first, then the checksum.

### The release public key

<!-- The release workflow publishes the key line with the first release
     (repository variable OSTRO_RELEASE_MINISIGN_PUBKEY). Replace the placeholder then. -->

```text
untrusted comment: ostro release key
RW<release public key: published with the first release>
```

The same key is attached to every release as `minisign.pub` and embedded in the published
`install.sh`. The matching secret key is held by the release workflow as a repository secret,
without a passphrase (the signing tool reads passphrases only from a terminal, so a
passphrase-protected key cannot be used unattended); access to the repository's secrets is
what protects it. A change of the release key is announced in the release notes.

### Checking by hand

```sh
V=1.0.0
minisign -Vm SHA256SUMS -P 'RW<release public key>'      # or: -p minisign.pub
grep " ostro-$V-x86_64-unknown-linux-musl.tar.gz\$" SHA256SUMS | sha256sum -c   # macOS: shasum -a 256 -c
tar -xzf "ostro-$V-x86_64-unknown-linux-musl.tar.gz" ostro
./ostro --version
```

`rsign verify -P '<key>' -x SHA256SUMS.minisig SHA256SUMS` (rsign2) works as well.

## 3. The install script

```sh
curl -fsSL https://github.com/<owner>/ostro/releases/latest/download/install.sh | sh
```

(The release notes give the exact URL; the release workflow stamps the download location and
the public key into the published script.) The script picks the asset for your OS and
architecture (Linux and macOS, x86_64 and arm64), downloads it with `SHA256SUMS`, and installs
`ostro` into `~/.local/bin`:

- the tarball's sha256 is **always** checked against `SHA256SUMS`; a mismatch installs nothing;
- `SHA256SUMS.minisig` is checked when `minisign` or `rsign` is on your `PATH`. Without either,
  the script prints a warning and continues, unless a signature is required;
- downloads are https only.

| Flag | Meaning |
|---|---|
| `--version V` | install version `V` (default: the latest, read from `VERSION`) |
| `--system` | install into `/usr/local/bin` (uses `sudo` only when needed and available) |
| `--dir D` | install into `D` |

| Environment | Meaning |
|---|---|
| `OSTRO_REQUIRE_SIGNATURE=1` | fail unless `SHA256SUMS.minisig` verifies (needs `minisign` or `rsign` and a known public key) |
| `OSTRO_INSTALL_DIR` | install directory (default `$HOME/.local/bin`) |
| `OSTRO_DOWNLOAD_BASE` | where releases live: `<base>/v<V>/<asset>` and `<base>/latest/VERSION`, for a mirror. https only (plain http only for `127.0.0.1` / `localhost`) |
| `OSTRO_MINISIGN_PUBKEY` | override the embedded public key (the `RW…` key line) |

Recommended for automation:

```sh
curl -fsSL https://github.com/<owner>/ostro/releases/latest/download/install.sh \
  | OSTRO_REQUIRE_SIGNATURE=1 sh -s -- --version 1.0.0 --dir /opt/ostro/bin
```

## 4. macOS

Two options, both with signed and notarized binaries:

- **The installer package.** Download `ostro-<V>-macos-universal.pkg` and open it, or run
  `sudo installer -pkg ostro-<V>-macos-universal.pkg -target /`. It installs a universal
  (arm64 + x86_64) `ostro` into `/usr/local/bin`. The package is stapled, so Gatekeeper accepts
  it offline.
- **The install script** (§3). It installs the signed per-architecture binary. Files downloaded
  with `curl` carry no quarantine attribute, so no Gatekeeper prompt appears.

Building workflows on macOS uses darwin toolchains of the same pins; see
[getting-started.md](getting-started.md#toolchain-notes) for the `--docker` fallback.

## 5. Container image

The image is `FROM scratch`: the static musl binary, a CA bundle, an empty `/data` and `/tmp`.
It runs as uid/gid 65532 (non-root), with entrypoint `/ostro`, default command `server`,
working directory `/data` and ports 7700 (client API: REST, gRPC, UI) and 7701 (cluster
listener). It is published for `linux/amd64` and `linux/arm64` by the release workflow, and
built locally without Docker by `scripts/build-image.sh`. Contents, verification and the
exact image configuration are in [deploy/image/README.md](../deploy/image/README.md).

Always mount a volume at `/data` (writable for uid 65532):

```sh
podman volume create ostro-data
podman run --rm -p 7700:7700 -v ostro-data:/data \
  -e OSTRO_API__TLS__ALLOW_SELF_SIGNED=true \
  -e OSTRO_SEALING_KEY="$(head -c 32 /dev/urandom | base64)" \
  ghcr.io/<owner>/ostro:<V>
```

(Keep the sealing key: generate it once and store it, do not regenerate it per start; see §8.)
`ostro server` listens on `0.0.0.0` in the image, so it refuses to start without TLS
(certificate files or ACME) unless a self-signed certificate is explicitly allowed, as above for
an evaluation. On first start it writes the bootstrap admin bearer to
`/data/ostro-data/admin.token`.

## 6. Kubernetes

Use the Helm chart (`deploy/helm/ostro`, published as an OCI chart with each release). It runs
a static 1-, 3- or 5-voter server StatefulSet, a remote worker pool, optional KEDA scaling and
NetworkPolicies. See [kubernetes.md](kubernetes.md).

## 7. Building from source

You need Rust 1.100 (pinned in `rust-toolchain.toml`).

```sh
cargo build --release --locked -p ostro                     # target/release/ostro (glibc)
# A static musl binary, as released (needs cargo-zigbuild and zig):
cargo zigbuild --release --locked -p ostro --target x86_64-unknown-linux-musl
```

`bash scripts/release-dry-run.sh --target x86_64-unknown-linux-musl` builds the release
tarballs locally, checks that the binaries are static and below the size limit, and signs
`SHA256SUMS` with a throwaway key.

Shell completions: `ostro completions bash|zsh|fish|powershell|elvish` (see
`ostro completions --help` for where to put the script).

## 8. Configuring a server

`ostro server` reads a TOML file (`--config`, or `OSTRO_CONFIG`) and environment overrides
`OSTRO_<SECTION>__<KEY>` (for example `OSTRO_AUTH__MODE=token`, `OSTRO_NODE__DATA_DIR=/data`).
`ostro server --print-config` prints the effective configuration and exits. Unknown keys in
most sections are refused.

Before you expose a server:

1. **TLS.** Configure `[api.tls]` (`files` with your certificate, `acme`, or `self-signed`);
   see [tls.md](tls.md).
2. **Authentication.** `ostro server` defaults to `[auth] mode = "token"`. Take the bootstrap
   admin bearer (`<data_dir>/admin.token`, or supply one with `OSTRO_BOOTSTRAP_TOKEN`), create
   scoped tokens, then revoke the bootstrap token. See
   [tenancy-and-auth.md](tenancy-and-auth.md#the-first-admin-token-bootstrap).
3. **Sealing key.** Set `OSTRO_SEALING_KEY` (base64 of 32 random bytes) or
   `[secrets] sealing_key_file`. Without it the server stores no secrets, and the API-token key,
   the cluster CA and the UI session key in metadata stay unsealed. Every node of a cluster needs
   the same key. Back it up apart from the data directory; rotation is described in
   [secrets.md](secrets.md#52-rotation).
4. **Backups.** Configure bottomless backups ([bottomless.md](bottomless.md)) and size the node
   ([operations-sizing.md](operations-sizing.md)).
5. **Supervisor and open files.** Run the server under a supervisor that restarts it, and
   give it a high open-file limit. With systemd, set `Restart=on-failure` and
   `LimitNOFILE=1048576` in the `[Service]` section.

   If the metadata Raft driver stops on a storage error such as `EMFILE`, the server exits
   with status 1 so that the supervisor restarts it. `[api] max_connections` caps client
   connections, by default at half the soft open-file limit. See
   [cluster-operations.md](cluster-operations.md#41-fail-stop-on-storage-errors-open-file-limit-and-connection-cap-p14-fixups-4).

Environment variables used by Ostro:

| Variable | Used by | Meaning |
|---|---|---|
| `OSTRO_CONFIG` | server | configuration file |
| `OSTRO_<SECTION>__<KEY>` | server | override one configuration key |
| `OSTRO_SEALING_KEY`, `OSTRO_SEALING_KEY_OLD` | server, `ostro restore` | current sealing key; old keys during a rotation (comma-separated) |
| `OSTRO_BOOTSTRAP_TOKEN` | server | the bootstrap admin bearer (token mode) |
| `OSTRO_SECRET_<NAME>` | server | the env secret provider ([secrets.md](secrets.md#6-providers)) |
| `OSTRO_SERVER`, `OSTRO_TOKEN`, `OSTRO_CA_FILE` | CLI | server URL (default `http://127.0.0.1:7700`), bearer, CA certificate for an https server |
| `OSTRO_LISTEN` | `ostro dev` | listen address |
| `OSTRO_WORKER_JOIN`, `OSTRO_WORKER_TOKEN`, `OSTRO_WORKER_CA_HASH`, ... | `ostro worker` | join settings ([workers.md](workers.md)) |
| `OSTRO_TOOLCHAIN_DIR`, `OSTRO_OFFLINE`, `OSTRO_TOOLCHAIN_MIRROR` | `ostro build` | toolchain cache, offline mode, download mirror ([toolchains.md](toolchains.md)) |
| `OSTRO_DOWNLOAD_BASE`, `OSTRO_INSTALL_DIR`, `OSTRO_REQUIRE_SIGNATURE`, `OSTRO_MINISIGN_PUBKEY` | `install.sh` | §3 |
