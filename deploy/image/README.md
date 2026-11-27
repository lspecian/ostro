# Ostro container image

The Ostro image is a `FROM scratch` image: the static musl `ostro` binary, a CA bundle and two
empty directories. It is built **without Docker** by `scripts/build-image.sh` (Phase 14,
D-P14-25) with [crane](https://github.com/google/go-containerregistry) v0.22.1 against an
ephemeral registry on 127.0.0.1, and exported as an OCI image layout.

## Contents

One layer, built reproducibly (Python `tarfile`, every mtime = `SOURCE_DATE_EPOCH`, default the
commit time of `HEAD`):

| Path | Mode | Owner | What |
| --- | --- | --- | --- |
| `/ostro` | 0755 | 0:0 | static musl binary (mimalloc), the only executable |
| `/etc/ssl/certs/ca-certificates.crt` | 0644 | 0:0 | Mozilla CA bundle from curl.se (MPL-2.0), pinned in `cacert.pin` |
| `/data/` | 0750 | 65532:65532 | data directory (mount a volume here) |
| `/tmp/` | 1777 | 0:0 | scratch space |

There is no shell, no libc, no package manager and no other file: `scripts/build-image.sh`
verifies the exact file list on every build.

Image configuration:

- `User` `65532:65532` (non-root; the Helm chart's pod security context uses the same ids)
- `Entrypoint` `["/ostro"]`, `Cmd` `["server"]`
- `WorkingDir` `/data` (relative paths such as the default `./ostro-data` land on the volume)
- `ExposedPorts` `7700/tcp` (client API: REST, gRPC, UI) and `7701/tcp` (cluster listener)
- `Env` `SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt`
- OCI labels: `org.opencontainers.image.{title,description,licenses,source}` from `labels.env`,
  plus `version` (workspace version), `revision` (git commit) and `created`
  (`SOURCE_DATE_EPOCH`), filled at build time
- Platforms: `linux/amd64`; `linux/arm64` too when an aarch64 musl binary exists, in which case
  the result is a multi-arch OCI index

Why scratch: the binary is fully static, so nothing else is needed at run time. Fewer files
means less to patch and a smaller attack surface. Running as a fixed non-root uid works with
`runAsNonRoot` policies.

## Building and verifying

```sh
cargo zigbuild --release --locked -p ostro --target x86_64-unknown-linux-musl
bash scripts/build-image.sh            # builds target/image/oci, then verifies it
```

Options: `--binary-x86_64 P`, `--binary-aarch64 P`, `--tag local/ostro:dev`,
`--out target/image`, `--verify` (verify an existing layout only), `--no-verify`, `--push REF`
(CI only, below). The script prints the index digest (also written to
`target/image/index.digest`).

Verification starts a fresh loopback registry, pushes the layout into it and checks:
`crane config` (user, entrypoint, cmd, ports, env, working dir, labels, platform), the exact
file list, modes and owners of `crane export`, the CA bundle's sha256, the platforms of the
index, and that the extracted `/ostro --version` runs with an empty environment. It prints
`verify: OK`, or the differences and a non-zero exit status.

Nothing leaves the machine: the registry listens on 127.0.0.1 only, keeps its storage under
the output directory and is stopped by the script.

## Running

`/data` must be a writable volume owned by (or writable for) uid/gid 65532. A scratch image
cannot declare a `VOLUME` through crane, so always mount one; without it data is lost with the
container. Load the layout into a local runtime first, e.g.
`skopeo copy oci:target/image/oci:dev containers-storage:localhost/ostro:dev` (Podman) or
`skopeo copy oci:target/image/oci:dev docker-daemon:ostro:dev` (Docker).

The server profile listens on `0.0.0.0`, so it refuses to start with the default
`[api.tls] mode = "auto"` unless TLS is configured (certificate files or ACME) or a self-signed
certificate is explicitly allowed. For a quick evaluation, allow the self-signed certificate:

```sh
# Podman (rootless): the named volume is created with the right owner.
podman volume create ostro-data
podman run --rm -p 7700:7700 -v ostro-data:/data \
  -e OSTRO_API__TLS__ALLOW_SELF_SIGNED=true localhost/ostro:dev

# Docker: a bind mount must be owned by 65532.
sudo install -d -o 65532 -g 65532 -m 0750 /srv/ostro-data
docker run --rm -p 7700:7700 -v /srv/ostro-data:/data \
  -e OSTRO_API__TLS__ALLOW_SELF_SIGNED=true ostro:dev

# Any other subcommand replaces Cmd:
podman run --rm localhost/ostro:dev --version
podman run --rm localhost/ostro:dev server --config /etc/ostro/node.toml   # with -v for the file
```

With the default data directory (`./ostro-data` under the working directory `/data`) the
first start writes the bootstrap admin token to `/data/ostro-data/admin.token`. Without a
sealing key the node refuses to store secrets (startup warning). The image has no
`/etc/resolv.conf`; container runtimes and Kubernetes provide one.

Server-profile settings (auth token mode, sealing key, TLS) are passed as a config file or
`OSTRO_*` environment variables (`OSTRO_NODE__DATA_DIR`, `OSTRO_AUTH__MODE`,
`OSTRO_SEALING_KEY`, ...); see `docs/install.md` and `docs/tenancy-and-auth.md`.

### Kubernetes

Use the Helm chart in `deploy/helm/ostro` (`docs/kubernetes.md`): it runs this image with
`runAsUser`/`runAsGroup`/`fsGroup` 65532, a PersistentVolumeClaim per server pod at `/data`, an
`emptyDir` at `/tmp`, and `args: [server, --config, ...]`. Point `image.repository`/`image.tag`
at wherever the image was published.

## CA bundle refresh

`cacert.pin` holds the URL of a dated curl.se bundle and its sha256. To refresh:

1. Pick the newest dated file on <https://curl.se/docs/caextract.html> (`cacert-YYYY-MM-DD.pem`).
2. Download it into a fresh empty directory and compute its sha256; compare with
   <https://curl.se/ca/cacert.pem.sha256> published for the same bundle.
3. Update `url=` and `sha256=` in `cacert.pin` (and the comment line), rebuild, check
   `verify: OK`, commit.

The script downloads the pinned file into its own empty directory and refuses it unless the
sha256 matches; a verified copy is cached in `target/image/cache/`.

## Publishing (human_needed)

`--push REF` is the only code path that contacts a non-loopback registry. It copies the
verified index to `REF` with `crane copy` and is refused unless `CI=true`. It is meant for the
release workflow (`.github/workflows/release.yml`) with registry credentials from repository
secrets. Pushing is **human_needed**: it is never run locally or by executors. The user
chooses the registry and repository, provides the credentials and triggers the release run.
