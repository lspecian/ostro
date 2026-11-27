#!/usr/bin/env bash
# Release dry run (Phase 14, 14-11; DIST-01, D-P14-23): builds the static musl `ostro`
# binaries, proves they are static and under the shared size limit, packages reproducible
# tarballs, writes SHA256SUMS and signs it with an ephemeral minisign key.
#
# usage: scripts/release-dry-run.sh [--version V] [--out DIR] [--target T]... [--no-sign]
#   --version V   release version (a leading "v" is dropped); default: the `ostro` package
#                 version from `cargo metadata`. `ostro --version` must report it.
#   --out DIR     output directory (default target/release-dry-run/dist; emptied first)
#   --target T    build only T (repeatable). Default: x86_64-unknown-linux-musl and
#                 aarch64-unknown-linux-musl (aarch64 is skipped when its target cannot be
#                 installed offline: the remote CI leg builds it).
#   --no-sign     skip the ephemeral signature (release.yml signs once over every artefact
#                 with the real key in its `checksums` job)
#
# Steps per target: cargo zigbuild --release --locked -p ostro; `file` says statically
# linked / static-pie linked, `readelf -lW` has no INTERP, `readelf -dW` has no NEEDED; size
# below the limit in scripts/binary-size-limit (scripts/lib/binary-size.sh, D-P14-54); the
# binary for the host architecture prints `ostro <V>` for --version; the tarball
# ostro-<V>-<target>.tar.gz (ostro, LICENSE, NOTICE, README.md) is packaged twice with
# sorted names, a fixed mtime (SOURCE_DATE_EPOCH, default the last commit time), uid/gid 0
# and `gzip -n`, and both packagings must have the same sha256.
#
# The ephemeral key pair lives in target/release-dry-run/keys/ (under target/, never
# committed); dist/minisign.pub is its public key. The real release key is the user's.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
# shellcheck source=lib/binary-size.sh disable=SC1091  # followed with shellcheck -x
source "$ROOT/scripts/lib/binary-size.sh"

DRY="$ROOT/target/release-dry-run"
OUT="$DRY/dist"
VERSION=""
SIGN=1
TARGETS=()
DEFAULT_TARGETS=(x86_64-unknown-linux-musl aarch64-unknown-linux-musl)
P4_BASELINE_MB=22.7 # Phase 4 musl binary (04-20)

die() { echo "release-dry-run: $*" >&2; exit 1; }

while [ $# -gt 0 ]; do
  case $1 in
    --version) [ $# -ge 2 ] || die "--version needs a value"; VERSION=$2; shift 2 ;;
    --out) [ $# -ge 2 ] || die "--out needs a value"; OUT=$2; shift 2 ;;
    --target) [ $# -ge 2 ] || die "--target needs a value"; TARGETS+=("$2"); shift 2 ;;
    --no-sign) SIGN=0; shift ;;
    -h | --help) sed -n '2,26p' "$0"; exit 0 ;;
    *) die "unknown argument: $1" ;;
  esac
done
[ ${#TARGETS[@]} -gt 0 ] || TARGETS=("${DEFAULT_TARGETS[@]}")
mkdir -p "$OUT"
OUT="$(cd "$OUT" && pwd)"

if [ -z "$VERSION" ]; then
  VERSION="$(cargo metadata --no-deps --format-version 1 --locked |
    python3 -c 'import json,sys; print(next(p["version"] for p in json.load(sys.stdin)["packages"] if p["name"] == "ostro"))')"
fi
VERSION="${VERSION#v}"
[[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+([-+][0-9A-Za-z.+-]+)?$ ]] || die "invalid version ${VERSION@Q}"

LIMIT="$(ostro_max_binary_bytes)"
LIMIT_MB="$(ostro_max_binary_mb)"
EPOCH="${SOURCE_DATE_EPOCH:-$(git -C "$ROOT" log -1 --format=%ct)}"
[[ "$EPOCH" =~ ^[0-9]+$ ]] || die "invalid SOURCE_DATE_EPOCH ${EPOCH@Q}"
TARGET_DIR="${CARGO_TARGET_DIR:-$ROOT/target}"
case "$(uname -m)" in
  x86_64 | amd64) HOST_ARCH=x86_64 ;;
  aarch64 | arm64) HOST_ARCH=aarch64 ;;
  *) HOST_ARCH="$(uname -m)" ;;
esac

SCRATCH="$(mktemp -d "${TMPDIR:-/tmp}/ostro-release-dry-run.XXXXXX")"
trap 'rm -rf "$SCRATCH"' EXIT

echo "release-dry-run: version $VERSION, targets ${TARGETS[*]}, out $OUT, SOURCE_DATE_EPOCH=$EPOCH"
echo "release-dry-run: size limit $LIMIT bytes ($LIMIT_MB MB, scripts/binary-size-limit)"
# Only our own artefacts are removed from the output directory.
rm -f "$OUT"/ostro-*.tar.gz "$OUT/SHA256SUMS" "$OUT/SHA256SUMS.minisig" "$OUT/minisign.pub"

# --- 1. build ---------------------------------------------------------------------------
BUILT=()
for t in "${TARGETS[@]}"; do
  if [ "$t" != x86_64-unknown-linux-musl ] && ! rustup target list --installed | grep -qx "$t"; then
    if ! rustup target add "$t"; then
      echo "SKIP ${t%%-*} (target unavailable; remote CI leg)"
      continue
    fi
  fi
  echo "release-dry-run: cargo zigbuild --release --locked -p ostro --target $t"
  cargo zigbuild --release --locked -p ostro --target "$t"
  BUILT+=("$t")
done
[ ${#BUILT[@]} -gt 0 ] || die "nothing was built"

# --- 2. static, size and version checks -------------------------------------------------
check_binary() { # target bin
  local t=$1 bin=$2 desc size
  [ -x "$bin" ] || die "$t: binary missing: $bin"
  desc="$(file -b "$bin")"
  echo "$t: $desc"
  grep -Eq 'statically linked|static-pie linked' <<<"$desc" || die "$t: not statically linked"
  if readelf -lW "$bin" | grep -q 'INTERP'; then die "$t: has a program interpreter (INTERP)"; fi
  if readelf -dW "$bin" | grep -q '(NEEDED)'; then die "$t: has NEEDED shared libraries"; fi
  size="$(stat -c %s "$bin")"
  awk -v b="$size" -v l="$LIMIT" -v lmb="$LIMIT_MB" -v base="$P4_BASELINE_MB" -v t="$t" 'BEGIN {
    mb = b / 1000000
    printf "%s: size=%.2f MB (%d bytes) limit=%s MB (%d bytes) delta_vs_P4=%+.2f MB (baseline %s MB)\n", t, mb, b, lmb, l, mb - base, base
    exit (b < l) ? 0 : 1
  }' || die "$t: binary is $size bytes, not below the limit $LIMIT (NFR-2.1, scripts/binary-size-limit)"
  if [ "${t%%-*}" = "$HOST_ARCH" ]; then
    local got
    got="$("$bin" --version)"
    echo "$t: --version: $got"
    [ "$got" = "ostro $VERSION" ] || die "$t: --version printed ${got@Q}, want 'ostro $VERSION'"
  else
    echo "$t: --version not run (not the host architecture $HOST_ARCH)"
  fi
}

# --- 3. reproducible tarballs -----------------------------------------------------------
package() { # bin outfile
  local bin=$1 out=$2 stage f
  stage="$(mktemp -d "$SCRATCH/stage.XXXXXX")"
  install -m 0755 "$bin" "$stage/ostro"
  install -m 0644 "$ROOT/LICENSE" "$stage/LICENSE"
  install -m 0644 "$ROOT/NOTICE" "$stage/NOTICE"
  if [ -f "$ROOT/README.md" ]; then install -m 0644 "$ROOT/README.md" "$stage/README.md"; fi
  # --sort=name does not reorder members named on the command line: list them sorted (C).
  local files=()
  while IFS= read -r f; do files+=("$f"); done < <(cd "$stage" && printf '%s\n' * | LC_ALL=C sort)
  for f in "${files[@]}"; do touch -h -d "@$EPOCH" "$stage/$f"; done
  tar --format=gnu --sort=name --mtime="@$EPOCH" --owner=0 --group=0 --numeric-owner \
    -C "$stage" -cf - "${files[@]}" | gzip -n -9 >"$out"
}

declare -A SHA SIZE
for t in "${BUILT[@]}"; do
  bin="$TARGET_DIR/$t/release/ostro"
  check_binary "$t" "$bin"
  name="ostro-$VERSION-$t.tar.gz"
  a="$(mktemp -d "$SCRATCH/pkg-a.XXXXXX")"
  b="$(mktemp -d "$SCRATCH/pkg-b.XXXXXX")"
  package "$bin" "$a/$name"
  package "$bin" "$b/$name"
  sa="$(sha256sum "$a/$name" | cut -d' ' -f1)"
  sb="$(sha256sum "$b/$name" | cut -d' ' -f1)"
  [ "$sa" = "$sb" ] || die "$t: tarball is not reproducible ($sa vs $sb)"
  echo "$t: reproducible: two packagings give sha256 $sa"
  mv "$a/$name" "$OUT/$name"
  SHA[$t]=$sa
  SIZE[$t]="$(stat -c %s "$OUT/$name")"
done

# --- 4. SHA256SUMS + ephemeral minisign signature ---------------------------------------
(cd "$OUT" && sha256sum -- ostro-*.tar.gz | LC_ALL=C sort -k2 >SHA256SUMS)
(cd "$OUT" && sha256sum -c --quiet SHA256SUMS) || die "SHA256SUMS does not verify"

if [ "$SIGN" = 1 ]; then
  RSIGN="$(bash "$ROOT/scripts/fetch-tools.sh" rsign2 | sed -n 's/^rsign2=//p')"
  [ -x "$RSIGN" ] || die "rsign (rsign2) not available"
  KEYS="$DRY/keys"
  mkdir -p "$KEYS"
  chmod 700 "$KEYS"
  rm -f "$KEYS/dry-run.key" "$KEYS/dry-run.pub"
  "$RSIGN" generate -W -f -c "ostro release DRY RUN key (ephemeral)" \
    -p "$KEYS/dry-run.pub" -s "$KEYS/dry-run.key" >/dev/null
  "$RSIGN" sign -W -s "$KEYS/dry-run.key" -p "$KEYS/dry-run.pub" -x "$OUT/SHA256SUMS.minisig" \
    -t "ostro $VERSION SHA256SUMS (dry run)" "$OUT/SHA256SUMS" >/dev/null
  cp "$KEYS/dry-run.pub" "$OUT/minisign.pub"
  "$RSIGN" verify -q -p "$OUT/minisign.pub" -x "$OUT/SHA256SUMS.minisig" "$OUT/SHA256SUMS" ||
    die "SHA256SUMS.minisig does not verify"
  echo "release-dry-run: SHA256SUMS.minisig verified with dist/minisign.pub (ephemeral key in $KEYS)"
else
  echo "release-dry-run: --no-sign: SHA256SUMS not signed"
fi

# --- 5. summary -------------------------------------------------------------------------
echo
printf '%-50s %12s  %-64s  %s\n' artefact bytes sha256 static
for t in "${BUILT[@]}"; do
  printf '%-50s %12s  %-64s  %s\n' "ostro-$VERSION-$t.tar.gz" "${SIZE[$t]}" "${SHA[$t]}" yes
done
echo "release-dry-run: OK ($OUT)"
