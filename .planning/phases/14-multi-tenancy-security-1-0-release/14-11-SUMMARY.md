---
phase: 14-multi-tenancy-security-1-0-release
plan: 11
subsystem: release
tags: [release, musl, reproducible, minisign, install, github-actions, notarization]
requires: ["14-01b", "14-12 (scripts/build-image.sh, used by release.yml --push)", "14-13 (scripts/helm-check.sh + chart, used by release.yml)"]
provides:
  - "scripts/release-dry-run.sh: static musl builds, static/size/version checks, reproducible tarballs, SHA256SUMS + ephemeral minisign signature"
  - "install.sh: verified installer (sha256 always, minisign when available / required)"
  - "scripts/test-install.sh: install.sh against a local http.server (5 cases)"
  - ".github/workflows/release.yml on wip/14-11-workflows (orchestrator applies)"
affects: ["14-25 (docs/install.md: public key, OSTRO_* env, curl | sh)", "14-27 (CI steps for release-dry-run/test-install; release.yml application)", "14-34 (musl size record: 62.26 MB x86_64)"]
tech-stack:
  added: []
  patterns: ["GNU tar --sort=name --mtime --owner=0 --numeric-owner | gzip -n, packaged twice and compared", "install.sh tested with a shim-only PATH so a host minisign/rsign cannot leak in"]
key-files:
  created: [scripts/release-dry-run.sh, scripts/test-install.sh, install.sh, ".github/workflows/release.yml (wip/14-11-workflows only)"]
  modified: []
decisions:
  - "Release layout <base>/v<V>/<asset> + <base>/latest/VERSION; for a GitHub base ending in /releases/download, latest resolves through /releases/latest/download/VERSION (the release job uploads VERSION)."
  - "OSTRO_RELEASE_MINISIGN_KEY is a password-less rsign secret key (base64): rsign2 reads passwords only from a terminal, so a passphrase secret cannot be fed in CI. The GitHub secret store is the protection."
  - "The public key comes from repository variable OSTRO_RELEASE_MINISIGN_PUBKEY: the release job stamps it into install.sh (MINISIGN_PUBKEY) together with DEFAULT_DOWNLOAD_BASE; the checksums job verifies SHA256SUMS.minisig with it."
  - "release-dry-run.sh gained --target (repeatable) so each release.yml linux runner builds only its own target; --version is checked on the binary for the host architecture."
  - "release.yml has a small version job (tag without v, must equal the workspace version; else the workspace version) feeding VERSION env to every job."
  - "The macOS .pkg carries a lipo universal binary; the per-arch bare binaries are signed and also notarized (zip submission, ticket online), then tarred for install.sh."
metrics:
  duration: "~1h50m wall (~55 min of it cold dell-4 musl builds)"
  completed: 2026-11-27
---

# Phase 14 Plan 11: Release artefact pipeline Summary

A local release dry run builds static musl x86_64 and aarch64 `ostro` with cargo-zigbuild. It
proves both binaries static (file/readelf) and below the shared 70 MB limit
(scripts/lib/binary-size.sh), packages bit-reproducible tarballs, and writes SHA256SUMS signed with
an ephemeral rsign2 key. A POSIX `install.sh` always checks the sha256 and checks the minisign
signature (or requires it). It passed 5/5 cases against a local http.server. A SHA-pinned
`release.yml` (linux, macOS sign/notarize/staple, image, chart, draft release, publish jobs gated)
is on `wip/14-11-workflows`.

**Gate incomplete — consolidated gate pending on dell-4.** Only the plan's task verifies ran (on
dell-4, lane 14-11). No ci-local or full gate run was made for this plan (see "Unverified").

## Commits

| Task | Commit | Files |
| ---- | ------ | ----- |
| 1: release dry run | 5b7ce07 | scripts/release-dry-run.sh |
| 2: install + tests | 9d032bc | install.sh, scripts/test-install.sh |
| 3: release workflow | 4555e01 (branch `wip/14-11-workflows` only, parent 9d032bc) | .github/workflows/release.yml |

All three scripts are committed with mode 100755.

## Artefacts (dell-4, lane 14-11; same blobs as 5b7ce07/9d032bc)

| artefact | bytes | sha256 | static |
| -------- | ----- | ------ | ------ |
| ostro-0.1.0-x86_64-unknown-linux-musl.tar.gz | 25956756 | 29495abc441bc1e773d89d03d1727329c0daf69d1caaa89f2149ffbffa091d39 | yes |
| ostro-0.1.0-aarch64-unknown-linux-musl.tar.gz | 24022312 | b6f5a76da980eb36af6bc11f2f0f8fd2e68028e15c12aa8bc63f5389a5b59c80 | yes |

The tarball sha256 depends on SOURCE_DATE_EPOCH (default: the last commit time). These came from
epoch 1795787152.

- x86_64: `ELF 64-bit LSB executable, x86-64, statically linked, stripped`; no INTERP, no NEEDED;
  **62.26 MB (62261936 bytes)**, limit 70 MB (70000000, scripts/binary-size-limit), +39.56 MB vs the
  Phase 4 22.7 MB baseline; `ostro --version` = `ostro 0.1.0`.
- aarch64: `ELF 64-bit LSB executable, ARM aarch64, statically linked, stripped`; no INTERP, no
  NEEDED; **50.30 MB (50304624 bytes)**; --version not run (x86_64 host, no qemu).
- Tarball members (C-sorted, uid/gid 0, fixed mtime): LICENSE 0644, NOTICE 0644, README.md 0644,
  ostro 0755.
- SHA256SUMS (sorted) verified with `sha256sum -c`; SHA256SUMS.minisig verified with
  `rsign verify -p dist/minisign.pub`. The ephemeral key pair stays in `target/release-dry-run/keys/`
  (mode 700); `git ls-files --error-unmatch target/release-dry-run/keys` fails as required.
- The script sources scripts/lib/binary-size.sh and contains no 60 MB / 60000000 literal (the
  verify grep passed).

## Reproducibility check

Each tarball is packaged twice into separate temp dirs and the two sha256 values are compared (the
script fails on a difference). They matched for both targets in both dell-4 runs: x86_64 b27eacb5…
and aarch64 acebd4b4… in the first run (epoch 1795781929); 29495abc… and b6f5a76d… after the
member-order fix (epoch 1795787152). Cold build times on dell-4 (8 jobs): x86_64 29m10s, aarch64
24m23s. Warm reruns take ~1 s.

## aarch64 status

Built on dell-4: `rustup target add aarch64-unknown-linux-musl` succeeded (online) and cargo
zigbuild produced a static aarch64 binary. It was not executed (no aarch64 host or qemu).
release.yml builds it natively on `ubuntu-24.04-arm`, where the script's host-arch --version check
applies.

## Install test results (scripts/test-install.sh on dell-4, python3 -m http.server on 127.0.0.1)

| # | case | result |
| - | ---- | ------ |
| 1 | rsign on PATH, OSTRO_REQUIRE_SIGNATURE=1, version `latest` | PASS: signature OK (rsign), checksum OK, installed, `ostro 0.1.0` |
| 2 | tarball byte flipped | PASS: "checksum mismatch … not installing", install dir empty |
| 3 | SHA256SUMS.minisig from another ephemeral key, required | PASS: "signature verification FAILED", install dir empty |
| 4 | no verifier on PATH, required | PASS: "signature required … neither minisign nor rsign is on PATH", install dir empty |
| 5 | no verifier, not required, `--version v0.1.0` | PASS: warning, checksum OK, installed, runs |

shellcheck 0.11.0 (via scripts/fetch-tools.sh) gave zero findings on `-s sh install.sh`, on
`scripts/test-install.sh scripts/release-dry-run.sh`, and on `-x scripts/release-dry-run.sh`.

## release.yml validation (task 3 verify, run against the worktree copy)

- YAML parses, and the jobs version/linux/macos/checksums/image/chart/release are present.
- Every `uses:` is pinned to a 40-hex SHA.
- `APPLE_*` secrets appear only in `macos`; `OSTRO_RELEASE_MINISIGN_KEY` only in `checksums`.
- There is no `pull_request` trigger.
- `image`, `chart` and `release` have `if: github.event_name == 'push' || !inputs.dry_run`.
- The version is `VERSION="${REF_NAME#v}"` from env. No `${{ }}` expression appears inside any
  `run:` script (checked over the parsed YAML, not only with the ref_name grep).

The workflow was never run. That is human_needed: it needs an Apple Developer ID, the release key
and a GitHub remote.

### Action SHAs

| action | SHA | tag | source |
| ------ | --- | --- | ------ |
| actions/checkout | 3d3c42e5aac5ba805825da76410c181273ba90b1 | v7.0.1 | ci.yml |
| mlugg/setup-zig | d1434d08867e3ee9daa34448df10607b98908d29 | v2.2.1 | ci.yml |
| taiki-e/install-action | 75a7c0f87bd666af3cdb75c5b9e9d03ab6044e6f | v2.90.6 | ci.yml |
| actions/upload-artifact | cf430e030ddbb5b0abf93d22962f4752f3646cd9 | v7.0.2 | git ls-remote 2026-11-27 (lightweight tag = commit) |
| actions/download-artifact | 9000827ccba6bdab643e8b6fd33ac0654aef8333 | v8.0.2 | git ls-remote 2026-11-27 (lightweight tag = commit) |

No keychain or release action is used. The macOS keychain is set up with `security` directly, the
draft release uses the runner's `gh` CLI, and helm is the runner's preinstalled helm.

## rcodesign dry run

macOS cross-build unavailable locally; macOS signing stays in release.yml (human_needed).
Tried on dell-4 (lane 14-11): `rustup target add aarch64-apple-darwin` worked and
`cargo zigbuild --release --locked -p ostro --target aarch64-apple-darwin` compiled every crate,
but the final link failed with `unable to find system framework` for SystemConfiguration,
CoreFoundation and Security (zig has no macOS SDK frameworks). So rcodesign was never run (it was
not fetched). The darwin target was removed from dell-4's toolchain again afterwards.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Tarball members not in sorted order**
- Found during: Task 1 (first dell-4 run).
- Issue: GNU tar `--sort=name` does not reorder members named on the command line, so the archive
  listed `ostro LICENSE NOTICE README.md`. The output was still reproducible but not sorted as
  intended.
- Fix: the staged files are listed in C-locale order. Folded into 5b7ce07.

**2. [Rule 3 - Blocking] shellcheck SC1091 without -x**
- The plan's verify runs shellcheck without `-x`, which reports SC1091 (info) on the sourced
  binary-size helper.
- Fix: added `disable=SC1091` next to the `source=` directive. The helper is still followed with
  `-x`, which is also clean. Folded into 5b7ce07.

### Additions (within files_modified)

- `release-dry-run.sh --target T` (repeatable) and the host-arch --version check. release.yml's
  linux matrix runs one target per runner (the x86_64 runner and `ubuntu-24.04-arm`).
- `install.sh`: the `--version=V` and `--dir=D` forms; the `OSTRO_MINISIGN_PUBKEY` override (already
  registered in RESERVATIONS); a non-https base is refused (plain http only for 127.0.0.1/localhost).
- release.yml: a `version` job, and a mismatch between the tag and the workspace version fails. The
  checksums job falls back to an ephemeral dry-run key only for a workflow_dispatch run without the
  secret; a tag push without the key fails.

### Plan wording vs execution

- The plan's verification names `ostro-k8s-ci --parallel`, but the cluster is withdrawn and builds
  moved to dell-4 (exec brief). The plan-level gate is left to the orchestrator's consolidated dell-4
  gate.
- The Task 1/2 verifies ran at the pre-rebase commits (base e7edd8e). The CAS put the same blobs onto
  0642342. None of the intervening commits (14-12, 14-07, 14-23) touch these files. 14-12 changed
  NOTICE, which only changes the tarball contents.

## Unverified (gate incomplete — consolidated gate pending on dell-4)

1. No ci-local or consolidated gate run for this plan at 9d032bc (lint group, tests). The plan adds
   only shell scripts, but the gate has not run.
2. The task verifies ran at fa633f2/daee790 (same blobs) and were not re-run at 9d032bc on main.
3. release.yml was only validated statically. It has never run on GitHub (no remote; human_needed),
   so none of these are tested: macOS signing, notarization and stapling; the ubuntu-24.04-arm leg;
   the image push (build-image.sh --push); helm push; `gh release create`; artifact upload and
   download.
4. The aarch64 binary was never executed, and no macOS binary was built (see the rcodesign section).
5. install.sh on macOS (the Darwin path, `shasum -a 256`, the `minisign` CLI branch) and the
   `--system`/sudo path were not exercised. Only the rsign verifier branch on Linux was tested.
6. The musl size was measured once (62.26 MB, x86_64). The gate's "musl binary size limit" step on
   main is still the authoritative check.

## User setup (human_needed)

- Apple: `APPLE_DEVELOPER_ID_APP_P12_BASE64`, `APPLE_DEVELOPER_ID_INSTALLER_P12_BASE64`,
  `APPLE_P12_PASSWORD`, `APPLE_NOTARY_KEY_ID`, `APPLE_NOTARY_ISSUER_ID`, `APPLE_NOTARY_KEY_P8_BASE64`.
- Release key: run `rsign generate -W -p ostro.pub -s ostro.key` offline. Set the secret
  `OSTRO_RELEASE_MINISIGN_KEY` to `base64 -w0 ostro.key`, and the repository **variable**
  `OSTRO_RELEASE_MINISIGN_PUBKEY` to line 2 of ostro.pub (also published in docs/install.md, 14-25).
- Packages: the image and chart jobs push to ghcr.io/<owner> with GITHUB_TOKEN (`packages: write`).

## Notes for other plans

- 14-27: add ci-local/CI steps `bash scripts/release-dry-run.sh --target x86_64-unknown-linux-musl`
  (after the musl release step, so the build is shared) and `bash scripts/test-install.sh`. Apply
  release.yml from `wip/14-11-workflows` (4555e01).
- 14-25: docs/install.md should cover `curl -fsSL <release>/install.sh | sh`,
  `OSTRO_DOWNLOAD_BASE`, `OSTRO_INSTALL_DIR`, `OSTRO_REQUIRE_SIGNATURE=1`, `--system`, the published
  public key and the macOS .pkg. install.sh's default base
  `https://github.com/ostro-dev/ostro/releases/download` is a placeholder that the release job
  stamps with the real repository.
- 14-34: musl x86_64 is 62.26 MB at e7edd8e (limit 70 MB); aarch64 is 50.30 MB.

## Workflow hunks

New file `.github/workflows/release.yml` (branch `wip/14-11-workflows`, commit 4555e01). Apply with
`git show 4555e01:.github/workflows/release.yml > .github/workflows/release.yml`. Full content:

```yaml
# Ostro release pipeline (Phase 14, 14-11; DIST-01, D-P14-23/24).
# Tag v* = real release (draft GitHub release); workflow_dispatch defaults to a dry run that
# builds, signs and notarizes but publishes nothing (image, chart and release jobs skipped).
# Third-party actions are pinned to full commit SHAs (ci.yml pins; artifact actions resolved
# with git ls-remote on 2026-11-27). Versions are derived in shell from env, never by
# interpolating expressions into scripts.
#
# Secrets (user_setup in 14-11-PLAN; each referenced only in its own job):
#   macos:     Developer ID Application/Installer .p12 (base64) + password, App Store Connect
#              API key (id, issuer, .p8 base64) for notarytool
#   checksums: OSTRO_RELEASE_MINISIGN_KEY = base64 of a password-less minisign secret key
#              (`rsign generate -W`; rsign reads passwords only from a terminal)
# Repository variable: OSTRO_RELEASE_MINISIGN_PUBKEY = the public key line (stamped into
# install.sh and used to verify SHA256SUMS.minisig).
name: Release

on:
  push:
    tags: ['v*']
  workflow_dispatch:
    inputs:
      dry_run:
        description: "Build, sign and notarize only; publish nothing"
        type: boolean
        default: true

permissions:
  contents: read

concurrency:
  group: release-${{ github.ref }}
  cancel-in-progress: false

env:
  CARGO_TERM_COLOR: always
  CARGO_ZIGBUILD_VERSION: 0.24.1
  ZIG_VERSION: 0.17.1

jobs:
  version:
    name: version
    runs-on: ubuntu-24.04
    outputs:
      version: ${{ steps.v.outputs.version }}
    steps:
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
      - id: v
        name: Derive the version (tag without the v prefix; else the workspace version)
        env:
          REF_NAME: ${{ github.ref_name }}
          REF_TYPE: ${{ github.ref_type }}
        run: |
          set -euo pipefail
          cargo_version="$(python3 -c 'import tomllib; print(tomllib.load(open("Cargo.toml","rb"))["workspace"]["package"]["version"])')"
          if [ "$REF_TYPE" = tag ]; then
            VERSION="${REF_NAME#v}"
            if [ "$VERSION" != "$cargo_version" ]; then
              echo "tag $REF_NAME does not match the workspace version $cargo_version" >&2
              exit 1
            fi
          else
            VERSION="$cargo_version"
          fi
          echo "version=$VERSION" >> "$GITHUB_OUTPUT"
          echo "release version $VERSION"

  linux:
    name: linux (${{ matrix.target }})
    needs: version
    runs-on: ${{ matrix.runner }}
    strategy:
      fail-fast: true
      matrix:
        include:
          - runner: ubuntu-24.04
            target: x86_64-unknown-linux-musl
          - runner: ubuntu-24.04-arm
            target: aarch64-unknown-linux-musl
    env:
      T: ${{ matrix.target }}
      VERSION: ${{ needs.version.outputs.version }}
    steps:
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
      - name: Install Rust toolchain (rust-toolchain.toml) + target
        run: |
          rustup toolchain install
          rustup target add "$T"
          rustup show
      - uses: mlugg/setup-zig@d1434d08867e3ee9daa34448df10607b98908d29 # v2.2.1
        with:
          version: ${{ env.ZIG_VERSION }}
      - uses: taiki-e/install-action@75a7c0f87bd666af3cdb75c5b9e9d03ab6044e6f # v2.90.6
        with:
          tool: cargo-zigbuild@${{ env.CARGO_ZIGBUILD_VERSION }}
      - name: Build, check (static, size, --version) and package
        # the real signature is made once over every artefact in the checksums job
        run: bash scripts/release-dry-run.sh --version "$VERSION" --target "$T" --out dist --no-sign
      - uses: actions/upload-artifact@cf430e030ddbb5b0abf93d22962f4752f3646cd9 # v7.0.2
        with:
          name: linux-${{ matrix.target }}
          path: dist/ostro-*.tar.gz
          if-no-files-found: error

  macos:
    name: macos (signed, notarized)
    needs: version
    runs-on: macos-14
    env:
      VERSION: ${{ needs.version.outputs.version }}
    steps:
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
      - name: Install Rust toolchain (rust-toolchain.toml) + darwin targets
        run: |
          rustup toolchain install
          rustup target add aarch64-apple-darwin x86_64-apple-darwin
          rustup show
      - name: Build ostro (aarch64 + x86_64)
        run: |
          set -euo pipefail
          for t in aarch64-apple-darwin x86_64-apple-darwin; do
            cargo build --release --locked -p ostro --target "$t"
          done
          mkdir -p out
          cp target/aarch64-apple-darwin/release/ostro out/ostro-aarch64
          cp target/x86_64-apple-darwin/release/ostro out/ostro-x86_64
          lipo -create -output out/ostro-universal out/ostro-aarch64 out/ostro-x86_64
          test "$(out/ostro-aarch64 --version)" = "ostro $VERSION"
      - name: Import the Developer ID certificates into a temporary keychain
        env:
          APP_P12: ${{ secrets.APPLE_DEVELOPER_ID_APP_P12_BASE64 }}
          INSTALLER_P12: ${{ secrets.APPLE_DEVELOPER_ID_INSTALLER_P12_BASE64 }}
          P12_PASSWORD: ${{ secrets.APPLE_P12_PASSWORD }}
        run: |
          set -euo pipefail
          KEYCHAIN="$RUNNER_TEMP/ostro-signing.keychain-db"
          KEYCHAIN_PASSWORD="$(openssl rand -hex 24)"
          security create-keychain -p "$KEYCHAIN_PASSWORD" "$KEYCHAIN"
          security set-keychain-settings -lut 21600 "$KEYCHAIN"
          security unlock-keychain -p "$KEYCHAIN_PASSWORD" "$KEYCHAIN"
          printf '%s' "$APP_P12" | base64 --decode > "$RUNNER_TEMP/app.p12"
          printf '%s' "$INSTALLER_P12" | base64 --decode > "$RUNNER_TEMP/installer.p12"
          security import "$RUNNER_TEMP/app.p12" -k "$KEYCHAIN" -P "$P12_PASSWORD" -f pkcs12 \
            -T /usr/bin/codesign -T /usr/bin/productsign
          security import "$RUNNER_TEMP/installer.p12" -k "$KEYCHAIN" -P "$P12_PASSWORD" -f pkcs12 \
            -T /usr/bin/codesign -T /usr/bin/productsign
          rm -f "$RUNNER_TEMP/app.p12" "$RUNNER_TEMP/installer.p12"
          security set-key-partition-list -S apple-tool:,apple: -s -k "$KEYCHAIN_PASSWORD" "$KEYCHAIN" >/dev/null
          # put the temporary keychain first in the search list (keep the existing ones)
          existing=()
          while IFS= read -r k; do existing+=("$(echo "$k" | tr -d ' "')"); done < <(security list-keychains -d user)
          security list-keychains -d user -s "$KEYCHAIN" "${existing[@]}"
          APP_ID="$(security find-identity -v -p codesigning "$KEYCHAIN" | awk '/Developer ID Application/ { print $2; exit }')"
          INSTALLER_ID="$(security find-identity -v "$KEYCHAIN" | awk '/Developer ID Installer/ { print $2; exit }')"
          test -n "$APP_ID" && test -n "$INSTALLER_ID"
          {
            echo "KEYCHAIN=$KEYCHAIN"
            echo "APP_ID=$APP_ID"
            echo "INSTALLER_ID=$INSTALLER_ID"
          } >> "$GITHUB_ENV"
      - name: codesign (hardened runtime, secure timestamp)
        run: |
          set -euo pipefail
          for b in out/ostro-aarch64 out/ostro-x86_64 out/ostro-universal; do
            codesign --force --options runtime --timestamp --keychain "$KEYCHAIN" \
              --identifier dev.ostro.cli --sign "$APP_ID" "$b"
            codesign --verify --strict --verbose=2 "$b"
          done
      - name: Build and sign the .pkg (installs /usr/local/bin/ostro)
        run: |
          set -euo pipefail
          mkdir -p pkgroot
          cp out/ostro-universal pkgroot/ostro
          pkgbuild --root pkgroot --install-location /usr/local/bin --identifier dev.ostro.cli \
            --version "$VERSION" out/ostro-unsigned.pkg
          productsign --sign "$INSTALLER_ID" --keychain "$KEYCHAIN" --timestamp \
            out/ostro-unsigned.pkg "out/ostro-$VERSION-macos-universal.pkg"
          pkgutil --check-signature "out/ostro-$VERSION-macos-universal.pkg"
      - name: Notarize (notarytool --wait), staple and assess
        env:
          NOTARY_KEY_ID: ${{ secrets.APPLE_NOTARY_KEY_ID }}
          NOTARY_ISSUER_ID: ${{ secrets.APPLE_NOTARY_ISSUER_ID }}
          NOTARY_KEY_P8: ${{ secrets.APPLE_NOTARY_KEY_P8_BASE64 }}
        run: |
          set -euo pipefail
          key="$RUNNER_TEMP/notary.p8"
          printf '%s' "$NOTARY_KEY_P8" | base64 --decode > "$key"
          pkg="out/ostro-$VERSION-macos-universal.pkg"
          xcrun notarytool submit "$pkg" --key "$key" --key-id "$NOTARY_KEY_ID" \
            --issuer "$NOTARY_ISSUER_ID" --wait
          # bare binaries cannot be stapled; notarizing them puts their ticket online
          ditto -c -k --keepParent out/ostro-aarch64 out/bare-aarch64.zip
          ditto -c -k --keepParent out/ostro-x86_64 out/bare-x86_64.zip
          for z in out/bare-aarch64.zip out/bare-x86_64.zip; do
            xcrun notarytool submit "$z" --key "$key" --key-id "$NOTARY_KEY_ID" \
              --issuer "$NOTARY_ISSUER_ID" --wait
          done
          rm -f "$key"
          xcrun stapler staple "$pkg"
          xcrun stapler validate "$pkg"
          spctl -a -vv -t install "$pkg"
          codesign --verify --strict --verbose=2 out/ostro-aarch64
          codesign --verify --strict --verbose=2 out/ostro-x86_64
      - name: Package the signed bare binaries for curl installs
        run: |
          set -euo pipefail
          for a in aarch64 x86_64; do
            stage="$RUNNER_TEMP/stage-$a"
            mkdir -p "$stage"
            cp "out/ostro-$a" "$stage/ostro"
            cp LICENSE NOTICE README.md "$stage/"
            tar -C "$stage" -czf "out/ostro-$VERSION-$a-apple-darwin.tar.gz" ostro LICENSE NOTICE README.md
          done
      - name: Remove the temporary keychain
        if: always()
        run: |
          if [ -n "${KEYCHAIN:-}" ]; then security delete-keychain "$KEYCHAIN"; fi
      - uses: actions/upload-artifact@cf430e030ddbb5b0abf93d22962f4752f3646cd9 # v7.0.2
        with:
          name: macos
          path: |
            out/ostro-*-apple-darwin.tar.gz
            out/ostro-*-macos-universal.pkg
          if-no-files-found: error

  checksums:
    name: checksums (SHA256SUMS + minisign)
    needs: [version, linux, macos]
    runs-on: ubuntu-24.04
    env:
      VERSION: ${{ needs.version.outputs.version }}
      EVENT_NAME: ${{ github.event_name }}
      PUBKEY: ${{ vars.OSTRO_RELEASE_MINISIGN_PUBKEY }}
    steps:
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
      - name: Install Rust toolchain (rust-toolchain.toml; for rsign2)
        run: rustup toolchain install
      - uses: actions/download-artifact@9000827ccba6bdab643e8b6fd33ac0654aef8333 # v8.0.2
        with:
          path: dist
          merge-multiple: true
      - name: SHA256SUMS over every artefact, signed with the release key
        env:
          OSTRO_RELEASE_MINISIGN_KEY: ${{ secrets.OSTRO_RELEASE_MINISIGN_KEY }}
        run: |
          set -euo pipefail
          cd dist
          ls -l
          test -f "ostro-$VERSION-x86_64-unknown-linux-musl.tar.gz"
          test -f "ostro-$VERSION-aarch64-unknown-linux-musl.tar.gz"
          sha256sum -- ostro-* | LC_ALL=C sort -k2 > SHA256SUMS
          cat SHA256SUMS
          rsign="$(bash ../scripts/fetch-tools.sh rsign2 | sed -n 's/^rsign2=//p')"
          key="$RUNNER_TEMP/release.key"
          if [ -n "$OSTRO_RELEASE_MINISIGN_KEY" ]; then
            printf '%s' "$OSTRO_RELEASE_MINISIGN_KEY" | base64 --decode > "$key"
          elif [ "$EVENT_NAME" = push ]; then
            echo "OSTRO_RELEASE_MINISIGN_KEY is not set: a tag release must be signed" >&2
            exit 1
          else
            echo "::warning::no release key: signing with an ephemeral DRY-RUN key"
            "$rsign" generate -W -f -p "$RUNNER_TEMP/dry-run.pub" -s "$key" >/dev/null
            PUBKEY="$(sed -n 2p "$RUNNER_TEMP/dry-run.pub")"
          fi
          "$rsign" sign -W -s "$key" -x SHA256SUMS.minisig \
            -t "ostro $VERSION SHA256SUMS" SHA256SUMS >/dev/null
          rm -f "$key"
          if [ -n "$PUBKEY" ]; then
            "$rsign" verify -P "$PUBKEY" -x SHA256SUMS.minisig SHA256SUMS
          elif [ "$EVENT_NAME" = push ]; then
            echo "repository variable OSTRO_RELEASE_MINISIGN_PUBKEY is not set" >&2
            exit 1
          fi
      - uses: actions/upload-artifact@cf430e030ddbb5b0abf93d22962f4752f3646cd9 # v7.0.2
        with:
          name: checksums
          path: |
            dist/SHA256SUMS
            dist/SHA256SUMS.minisig
          if-no-files-found: error

  image:
    name: image (ghcr.io, multi-arch)
    needs: [version, linux]
    if: github.event_name == 'push' || !inputs.dry_run
    runs-on: ubuntu-24.04
    permissions:
      contents: read
      packages: write
    env:
      VERSION: ${{ needs.version.outputs.version }}
      OWNER: ${{ github.repository_owner }}
      CI: "true"
    steps:
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
      - uses: actions/download-artifact@9000827ccba6bdab643e8b6fd33ac0654aef8333 # v8.0.2
        with:
          pattern: linux-*
          path: dist
          merge-multiple: true
      - name: Build and push the scratch image (scripts/build-image.sh, 14-12)
        env:
          GH_TOKEN: ${{ github.token }}
          ACTOR: ${{ github.actor }}
        run: |
          set -euo pipefail
          owner="${OWNER,,}"
          for a in x86_64 aarch64; do
            mkdir -p "bin/$a"
            tar -xzf "dist/ostro-$VERSION-$a-unknown-linux-musl.tar.gz" -C "bin/$a" ostro
          done
          crane="$(bash scripts/fetch-tools.sh crane | sed -n 's/^crane=//p')"
          printf '%s' "$GH_TOKEN" | "$crane" auth login ghcr.io -u "$ACTOR" --password-stdin
          bash scripts/build-image.sh --binary-x86_64 bin/x86_64/ostro --binary-aarch64 bin/aarch64/ostro \
            --push "ghcr.io/$owner/ostro:$VERSION"

  chart:
    name: chart (oci://ghcr.io)
    needs: [version, image]
    if: github.event_name == 'push' || !inputs.dry_run
    runs-on: ubuntu-24.04
    permissions:
      contents: read
      packages: write
    env:
      VERSION: ${{ needs.version.outputs.version }}
      OWNER: ${{ github.repository_owner }}
    steps:
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
      - name: Validate, package and push the Helm chart
        env:
          GH_TOKEN: ${{ github.token }}
          ACTOR: ${{ github.actor }}
        run: |
          set -euo pipefail
          owner="${OWNER,,}"
          helm version
          bash scripts/helm-check.sh
          helm package deploy/helm/ostro --version "$VERSION" --app-version "$VERSION" -d chart-dist
          printf '%s' "$GH_TOKEN" | helm registry login ghcr.io -u "$ACTOR" --password-stdin
          helm push "chart-dist/ostro-$VERSION.tgz" "oci://ghcr.io/$owner/charts"

  release:
    name: release (draft)
    needs: [version, checksums, image, chart]
    if: github.event_name == 'push' || !inputs.dry_run
    runs-on: ubuntu-24.04
    permissions:
      contents: write
    env:
      VERSION: ${{ needs.version.outputs.version }}
      REPO: ${{ github.repository }}
      PUBKEY: ${{ vars.OSTRO_RELEASE_MINISIGN_PUBKEY }}
    steps:
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
      - uses: actions/download-artifact@9000827ccba6bdab643e8b6fd33ac0654aef8333 # v8.0.2
        with:
          path: dist
          merge-multiple: true
      - name: Stamp install.sh (download base, public key) and draft the release
        env:
          GH_TOKEN: ${{ github.token }}
        run: |
          set -euo pipefail
          test -n "$PUBKEY"
          base="https://github.com/$REPO/releases/download"
          sed -e "s|^DEFAULT_DOWNLOAD_BASE=.*|DEFAULT_DOWNLOAD_BASE=\"$base\"|" \
              -e "s|^MINISIGN_PUBKEY=.*|MINISIGN_PUBKEY=\"$PUBKEY\"|" install.sh > dist/install.sh
          grep -q "^MINISIGN_PUBKEY=\"$PUBKEY\"" dist/install.sh
          printf '%s\n' "$VERSION" > dist/VERSION
          printf 'untrusted comment: ostro release key\n%s\n' "$PUBKEY" > dist/minisign.pub
          (cd dist && sha256sum -c SHA256SUMS)
          gh release create "v$VERSION" --repo "$REPO" --draft --verify-tag \
            --title "Ostro $VERSION" --notes "Ostro $VERSION. Verify: SHA256SUMS + SHA256SUMS.minisig (minisign.pub)." \
            dist/ostro-* dist/SHA256SUMS dist/SHA256SUMS.minisig dist/install.sh dist/VERSION dist/minisign.pub
```
