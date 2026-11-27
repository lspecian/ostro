---
phase: 14-multi-tenancy-security-1-0-release
plan: 12
subsystem: deploy
tags: [container-image, oci, crane, scratch, DIST-01]
requires: ["14-01b"]
provides: ["scripts/build-image.sh (Docker-free FROM scratch OCI image build + verification)", "deploy/image/{README.md,cacert.pin,labels.env}", "NOTICE: Mozilla CA bundle (MPL-2.0)"]
affects: ["14-27 (CI step for the image build; release.yml --push, human_needed)", "14-25 (docs/install.md / docs/kubernetes.md can link deploy/image/README.md; README names docs/install.md, docs/kubernetes.md, docs/tenancy-and-auth.md, which 14-25 writes)", "14-11 (an aarch64 musl binary at the default path makes the index multi-arch)"]
tech-stack:
  added: ["crane 0.22.1 (scripts/fetch-tools.sh, already pinned by 14-01b)", "curl.se cacert-2026-11-05.pem (pinned sha256)"]
  patterns: ["ephemeral loopback `crane registry serve` started and stopped by the script", "reproducible layer tar via Python tarfile with SOURCE_DATE_EPOCH", "verification against a second fresh loopback registry"]
key-files:
  created:
    - scripts/build-image.sh
    - deploy/image/README.md
    - deploy/image/cacert.pin
    - deploy/image/labels.env
  modified:
    - NOTICE
decisions:
  - "No D-P14-25 fallback needed: crane v0.22.1 has append --oci-empty-base, mutate --user/--entrypoint/--cmd/--exposed-ports/--env/--label/--workdir/--set-platform, index append and registry serve."
  - "Image config adds WorkingDir /data (not in the plan): the server's default data_dir is the relative ./ostro-data, which would otherwise resolve to the unwritable /ostro-data."
  - "Platform is set with `crane mutate --set-platform linux/<arch>` (append's global --platform does not stamp an empty base)."
  - "The layout's index.json gets org.opencontainers.image.ref.name = <tag> written by the script instead of crane pull --annotate-ref (that would record the random loopback port and break reproducibility)."
  - "--push needs verification (refused with --no-verify) and copies the index from the verify registry by digest; refused unless CI=true, checked before anything else runs."
  - "Binaries are checked before use: little-endian ELF64, e_machine matching the platform, no PT_INTERP (static)."
metrics:
  duration: "~45 min (plus a 23 min cold musl build on dell-4)"
  completed: 2026-11-27
---

# Phase 14 Plan 12: FROM scratch image without Docker Summary

`scripts/build-image.sh` builds the Ostro `FROM scratch` OCI image with crane v0.22.1 against
an ephemeral registry on 127.0.0.1. It exports the image to `target/image/oci` and verifies
config, exact contents, CA pin, binary hash and `/ostro --version`, ending with `verify: OK`.
Builds are byte-reproducible (same index digest from two builds of the same tree). All build
and run work happened on dell-4 (lane 14-12). Nothing was pushed anywhere.

**Gate incomplete: the consolidated gate is still pending on dell-4** (see "Unverified" below).

## Commits

| Task | Commit | Files |
|---|---|---|
| 1: build script, CA pin, labels, NOTICE | 1b6853a | scripts/build-image.sh, deploy/image/cacert.pin, deploy/image/labels.env, NOTICE |
| 2: verification + README | 526a450 | scripts/build-image.sh, deploy/image/README.md |

## crane v0.22.1 flag evidence (dell-4, `crane <cmd> --help`)

- `crane version` → `0.22.1`
- `append`: `-b/--base`, `-f/--new_layer strings`, `-t/--new_tag`, `--oci-empty-base` ("empty base image will have OCI media types instead of Docker"), `-o/--output`, `--set-base-image-annotations`
- `mutate`: `-a/--annotation`, `--append`, `--cmd strings`, `--entrypoint strings`, `-e/--env keyToValue`, `--exposed-ports strings`, `-l/--label stringToString`, `--repo`, `--set-platform`, `-t/--tag`, `-u/--user`, `-w/--workdir`
- `index append`: `--docker-empty-base` (default empty base is OCI), `--flatten`, `-m/--manifest strings`, `-t/--tag`
- `registry serve`: `--address`, `--disk`; with `--address 127.0.0.1:0` it logs `serving on port N` (parsed by the script)
- `pull --format oci` (+ `--annotate-ref`, unused), `push` (`--index`, unused: a layout with one index entry pushes that index), `export`, `config`, `copy`, `manifest`, `digest`; global `--platform`, `--insecure` (not needed: go-containerregistry uses plain HTTP for 127.0.0.1 automatically).

There was no Python OCI-writer fallback.

## CA bundle pin

`deploy/image/cacert.pin`: `url=https://curl.se/ca/cacert-2026-11-05.pem`,
`sha256=427c064216761a1e72a8b30f0d954d08cb6e4435a82091fb865afd20c8497285` (newest dated
bundle on curl.se/docs/caextract.html on 2026-11-27; Mozilla data as of 2026-11-05;
118 certificates). The hash was computed after downloading into a fresh empty directory and is
identical to the published `https://curl.se/ca/cacert.pem.sha256`. The script downloads into
`mktemp -d`, compares the sha256 before the file is moved or used, and caches it in
`target/image/cache/`. NOTICE gained a Mozilla CA bundle (MPL-2.0, via curl.se) section.

## Image (final HEAD 526a450, dell-4)

- Binary: musl `ostro` 62,261,936 B, sha256 `074298d20ce0554ee20e2829fa289953e2eab8210e449b6aab3bde0994f15f25`
- Index (OCI image index, linux/amd64): `sha256:2a22b2f7dfd86b0fe91131b8e3e0e576c58a202b58222aa6f0705746b19062f0` (347 B)
- linux/amd64 manifest: `sha256:a239194fdf94b0d5d61cc27999d5d1a772a9a73bcb0bc0e0c17a82ac3dd50dcc` (406 B)
- Config: 891 B; gzip layer ~30.2 MB (30,166,198 B)
- Config contents (checked): `User 65532:65532`, `Entrypoint ["/ostro"]`, `Cmd ["server"]`,
  `WorkingDir /data`, `ExposedPorts 7700/tcp, 7701/tcp`,
  `Env SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt`, labels
  `org.opencontainers.image.{title=ostro, description, licenses=Apache-2.0, source=https://github.com/lspecian/ostro, version=0.1.0, revision=<HEAD>, created=<SOURCE_DATE_EPOCH>}`.
  `created` (image) stays at crane's `0001-01-01T00:00:00Z`.
- Digests change with each commit because the `revision`/`created` labels follow HEAD. Two
  builds of the same tree gave the identical index digest (`sha256:8419beb6…` at d332c6a+wip).

### Verified file list (`crane export`, exact)

| Entry | Type | Mode | Owner |
|---|---|---|---|
| `data/` | dir | 0750 | 65532:65532 |
| `etc/` | dir | 0755 | 0:0 |
| `etc/ssl/` | dir | 0755 | 0:0 |
| `etc/ssl/certs/` | dir | 0755 | 0:0 |
| `etc/ssl/certs/ca-certificates.crt` | file | 0644 | 0:0 (sha256 = pin) |
| `ostro` | file | 0755 | 0:0 (sha256 = input binary) |
| `tmp/` | dir | 1777 | 0:0 |

No other entry exists: no shell, no libc. The extracted `/ostro --version` printed
`ostro 0.1.0` with an empty environment.

## Verification runs (all dell-4, lane 14-12)

- Plan Task 1 verify at 526a450: musl zigbuild, build, `index.json` present, pin and NOTICE greps, `CI= … --push example.invalid/…` refused (exit 1, before any work) → PLAN-VERIFY-1-OK.
- Plan Task 2 verify at 526a450: build output contains `verify: OK`; README greps for 65532 and human_needed → PLAN-VERIFY-2-OK.
- `--verify` alone (existing layout) → `verify: OK`.
- Negative: `--verify --binary-x86_64 /bin/ls` → `MISMATCH linux/amd64: ostro sha256 …`, exit 1.
- shellcheck 0.11.0 `-x scripts/build-image.sh` clean.
- Extra runtime check (beyond the plan): the image layer was extracted and run under `unshare -r --net chroot <rootfs>`. `/ostro --version` and `/ostro build-info` (`target=x86_64-unknown-linux-musl allocator=mimalloc`) ran. `/ostro server` with `OSTRO_NODE__DATA_DIR=/data OSTRO_API__TLS__ALLOW_SELF_SIGNED=true` booted: meta raft leader, cluster finalized at level 2, 16 partitions placed, bootstrap `admin.token` written under `/data`. It stopped cleanly on SIGTERM (`ostro server stopped elapsed_ms=187`).
  - Finding: with no configuration, `Cmd ["server"]` exits with `invalid TLS configuration: [api.tls] mode=auto on a non-loopback listener requires files or acme (or allow_self_signed = true)`. This is the intended server-profile behaviour, so the README run examples pass `-e OSTRO_API__TLS__ALLOW_SELF_SIGNED=true` for evaluation and point to TLS files/ACME for production. The Helm chart sets `[api.tls] mode = self-signed` itself.
  - The image has no `/etc/resolv.conf`, so egress logs "system DNS config unreadable; using public DNS fallback" in the chroot. Container runtimes and Kubernetes inject one.

## Unverified (consolidated gate pending on dell-4; human_needed items)

1. **Plan-level CI gate**: `ostro-k8s-ci --parallel` (the plan's `<verification>`) was not run. The cluster is withdrawn, and the full ci-local is reserved for the orchestrator's dell-4 `gate` lane. This plan changes no Rust code and no Cargo files; it only adds scripts/docs/NOTICE.
2. **Real container runtime**: there is no podman/crun/runc/skopeo/docker in the dell-4 CI container (user `ci`, no root; installing needs apt as root). Not verified:
   - the layout loads with `skopeo copy oci:target/image/oci:dev containers-storage:…` / `docker-daemon:…`;
   - `podman run` honours User/WorkingDir/ports, and `/data` volume ownership works.
   - Also to watch: crane's `index append` writes `artifactType: application/vnd.oci.image.config.v1+json` on the manifest descriptor. That is allowed by OCI 1.1, but it needs checking against real runtimes.
   - Needed: a dell-4 follow-up with root (or a host with rootless podman + skopeo) running `skopeo copy oci:target/image/oci:dev containers-storage:localhost/ostro:dev && podman run --rm localhost/ostro:dev --version`, plus a server run with a volume.
3. **Multi-arch index**: not exercised. No aarch64 musl binary exists (14-11 / remote CI leg). The code path builds an arm64 manifest, adds it to the index and checks the platform set; for arm64 the binary is checked (ELF machine, static, sha256) but not run on an amd64 host.
4. **`--push`**: never run (by design; human_needed). Only the refusal without `CI=true` was tested.
5. **musl size step** under the 70 MB limit at newer main is gate work. The binary measured here was 62.26 MB, built at d332c6a; cargo found it fresh at 526a450.

## Deviations from Plan

### Auto-fixed issues

**1. [Rule 1 - Bug] Registry storage directory must exist**
- **Found during:** Task 1 (first dell-4 run)
- **Issue:** `crane registry serve --disk D` fails uploads with `open D/upload-…: no such file or directory` unless D exists.
- **Fix:** `start_registry` creates `D/blobs` before starting.
- **Commit:** 1b6853a

**2. [Rule 2 - Missing critical] WorkingDir /data**
- **Found during:** Task 1 design (server config default `data_dir = ./ostro-data`)
- **Issue:** with cwd `/`, a default-configured server would try to create `/ostro-data`, which uid 65532 cannot write.
- **Fix:** `crane mutate --workdir /data`. The verification asserts it, and the README documents it.
- **Commit:** 1b6853a / 526a450

**3. [Rule 2] Binary sanity checks and verify-registry cleanup**
- ELF64/arch/static checks before a binary goes into a layer. The trap removes `registry/` and `verify-registry/` on every exit path (found when a failed verify left `verify-registry/` behind).
- **Commit:** 1b6853a / 526a450

### Other

- Builds and runs happened on dell-4 via `ostro-dell4-ci --lane 14-12` (not on venezia, not on the k8s cluster, per the 2026-11-27 brief), not via the plan's `target/slot-N` local build.

## Notes for other plans

- **14-27:** add a CI step `bash scripts/build-image.sh` after "musl release (static, mimalloc)", in ci-local and in ci.yml's musl leg; it needs network once for crane and the CA bundle, then both are cached under target/. release.yml: `CI=true bash scripts/build-image.sh --binary-x86_64 … --binary-aarch64 … --push <registry>/ostro:<version>` after a registry login; human_needed. No workflow branch was created: this plan has no workflow hunks.
- **14-25:** deploy/image/README.md names `docs/install.md`, `docs/kubernetes.md`, `docs/tenancy-and-auth.md` (as code spans, not links). Link deploy/image/README.md from docs/install.md once it exists.
- **14-26 / product:** consider documenting in `ostro server --help` that a non-loopback listener needs TLS config; the scratch image's default Cmd fails fast without it.

## Self-Check: PASSED

Commits 1b6853a and 526a450 are on main; scripts/build-image.sh (100755), deploy/image/{README.md,cacert.pin,labels.env} exist; NOTICE carries the MPL-2.0 section.
