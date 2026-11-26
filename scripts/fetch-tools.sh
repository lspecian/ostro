#!/usr/bin/env bash
# Pinned, checksum-verified tool downloads into target/tools (Phase 14, 14-01b; D-P14-44/46).
#
# usage: scripts/fetch-tools.sh [tool...]
#   no arguments = every required tool (rcodesign is optional: name it explicitly)
#
# Tools (Linux x86_64 only; macOS later):
#   kubeconform 0.8.0, crane 0.22.1, shellcheck 0.11.0, rcodesign 0.29.0 (optional),
#   cargo-fuzz 0.13.2 and rsign2 0.6.7 (cargo install --locked into target/tools/cargo),
#   weed 4.54 (SeaweedFS, test S3), kafka 4.1.2 (Apache Kafka, test broker),
#   jre 21.0.13.1+2 (Eclipse Temurin JRE for the test broker)
#
# Each archive is downloaded into its own fresh, empty directory, its sha256 (Kafka: the
# sha512 published by Apache) is verified before anything is extracted, and it is extracted
# into another fresh directory before being moved into place as target/tools/<tool>-<version>/.
# A tool that is already in place (marker file `.ostro-tool-ok`) is not fetched again.
# Prints `<tool>=<path>` for every requested tool. Never writes outside target/tools.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TOOLS="$ROOT/target/tools"
MARK=".ostro-tool-ok"

# name|version|url|algo:checksum|kind|member (path inside the archive; "." = the whole tree)
# Checksums were taken from the upstream release pages on 2026-11-26 (GitHub release asset
# digests; rcodesign's .sha256 file; Apache's .sha512; Adoptium's checksum).
PINS=(
  "kubeconform|0.8.0|https://github.com/yannh/kubeconform/releases/download/v0.8.0/kubeconform-linux-amd64.tar.gz|sha256:9bc2bffbf71f261128533edaf912153948b7ff238f9a531ae6d34466ec287883|tgz|kubeconform"
  "crane|0.22.1|https://github.com/google/go-containerregistry/releases/download/v0.22.1/go-containerregistry_Linux_x86_64.tar.gz|sha256:0ab7a1d6932a213aed964ce97666c3077fe691c8606413674a8b3e0b9ec4cda0|tgz|crane"
  "shellcheck|0.11.0|https://github.com/koalaman/shellcheck/releases/download/v0.11.0/shellcheck-v0.11.0.linux.x86_64.tar.gz|sha256:b7af85e41cc99489dcc21d66c6d5f3685138f06d34651e6d34b42ec6d54fe6f6|tgz|shellcheck-v0.11.0/shellcheck"
  "rcodesign|0.29.0|https://github.com/indygreg/apple-platform-rs/releases/download/apple-codesign/0.29.0/apple-codesign-0.29.0-x86_64-unknown-linux-musl.tar.gz|sha256:dbe85cedd8ee4217b64e9a0e4c2aef92ab8bcaaa41f20bde99781ff02e600002|tgz|apple-codesign-0.29.0-x86_64-unknown-linux-musl/rcodesign"
  "weed|4.54|https://github.com/seaweedfs/seaweedfs/releases/download/4.54/linux_amd64.tar.gz|sha256:e7424a82eee327809bfa449fda4924c9a0f42f6669a01288eebb1b79674c8196|tgz|weed"
  "kafka|4.1.2|https://archive.apache.org/dist/kafka/4.1.2/kafka_2.13-4.1.2.tgz|sha512:78ac6e488b1071122f9608dfdb363f6fe50e1dbbc492347002c0398dfbf77e0d8caa5bf794c5937379721004dfe92025937d238b0faf4eb715529417fd43b491|tgz|kafka_2.13-4.1.2"
  "jre|21.0.13.1_2|https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.13.1%2B2/OpenJDK21U-jre_x64_linux_hotspot_21.0.13.1_2.tar.gz|sha256:18f44fab9065217f3ba61d0eef8a7b5f3c3713aa7293001b0d4ac10ede3850f0|tgz|jdk-21.0.13.1+2-jre"
)
# Fallback mirrors (same checksum).
declare -A MIRROR=(
  [kafka]="https://downloads.apache.org/kafka/4.1.2/kafka_2.13-4.1.2.tgz"
)
# cargo-installed tools: name|crate|version|binary
CARGO_PINS=(
  "cargo-fuzz|cargo-fuzz|0.13.2|cargo-fuzz"
  "rsign2|rsign2|0.6.7|rsign"
)
REQUIRED=(kubeconform crane shellcheck weed kafka jre cargo-fuzz rsign2)
# Path printed for each tool (relative to target/tools/<tool>-<version>).
declare -A SHOW=(
  [kubeconform]=kubeconform [crane]=crane [shellcheck]=shellcheck [rcodesign]=rcodesign
  [weed]=weed [kafka]=. [jre]=.
)

die() { echo "fetch-tools: $*" >&2; exit 1; }

show() { # name dest
  local m=${SHOW[$1]}
  if [ "$m" = . ]; then echo "$1=$2"; else echo "$1=$2/$m"; fi
}

# Temporary download/extraction directories, removed on exit.
SCRATCH=()
cleanup() { if [ ${#SCRATCH[@]} -gt 0 ]; then rm -rf "${SCRATCH[@]}"; fi; }
trap cleanup EXIT

platform_ok() {
  [ "$(uname -s)" = Linux ] && [ "$(uname -m)" = x86_64 ] ||
    die "unsupported platform $(uname -s)/$(uname -m): Linux x86_64 only (macOS later)"
}

checksum_ok() { # file algo:hex
  local file=$1 want=$2 algo hex got
  algo=${want%%:*}
  hex=${want#*:}
  case $algo in
    sha256) got=$(sha256sum "$file" | cut -d' ' -f1) ;;
    sha512) got=$(sha512sum "$file" | cut -d' ' -f1) ;;
    *) die "unknown checksum algorithm $algo" ;;
  esac
  if [ "$got" != "$hex" ]; then
    echo "fetch-tools: $algo mismatch for $(basename "$file"): got $got, want $hex" >&2
    return 1
  fi
}

download() { # url out
  curl -fsSL --retry 3 --retry-delay 2 --connect-timeout 20 -o "$2" "$1"
}

fetch_archive() { # pin line
  local name version url sum kind member dest dl x src
  IFS='|' read -r name version url sum kind member <<<"$1"
  dest="$TOOLS/$name-$version"
  if [ -f "$dest/$MARK" ]; then
    show "$name" "$dest"
    return 0
  fi
  mkdir -p "$TOOLS"
  # A fresh, empty directory per download and per extraction (untrusted data rule).
  dl=$(mktemp -d "$TOOLS/.dl-$name.XXXXXX")
  x=$(mktemp -d "$TOOLS/.x-$name.XXXXXX")
  SCRATCH+=("$dl" "$x")
  if ! download "$url" "$dl/archive" || ! checksum_ok "$dl/archive" "$sum"; then
    local alt=${MIRROR[$name]:-}
    [ -n "$alt" ] || die "$name $version: download or checksum failed ($url)"
    rm -f "$dl/archive"
    download "$alt" "$dl/archive" || die "$name $version: download failed ($url, $alt)"
    checksum_ok "$dl/archive" "$sum" || die "$name $version: checksum failed"
  fi
  case $kind in
    tgz) tar -xzf "$dl/archive" -C "$x" --no-same-owner ;;
    *) die "unknown archive kind $kind" ;;
  esac
  src="$x/$member"
  [ -e "$src" ] || die "$name $version: $member not found in the archive"
  rm -rf "$dest"
  if [ -d "$src" ]; then
    mv "$src" "$dest"
  else
    mkdir -p "$dest"
    mv "$src" "$dest/"
    chmod +x "$dest/$(basename "$member")"
  fi
  printf '%s %s %s\n' "$name" "$version" "$sum" >"$dest/$MARK"
  rm -rf "$dl" "$x"
  show "$name" "$dest"
}

fetch_cargo() { # pin line
  local name crate version bin root
  IFS='|' read -r name crate version bin <<<"$1"
  root="$TOOLS/cargo"
  if [ -x "$root/bin/$bin" ] && "$root/bin/$bin" --version 2>/dev/null | grep -q "$version"; then
    echo "$name=$root/bin/$bin"
    return 0
  fi
  cargo install --locked --quiet --version "$version" --root "$root" "$crate" >&2 ||
    die "cargo install $crate $version failed"
  echo "$name=$root/bin/$bin"
}

fetch_one() {
  local t=$1 p
  for p in "${PINS[@]}"; do
    if [ "${p%%|*}" = "$t" ]; then
      fetch_archive "$p"
      return 0
    fi
  done
  for p in "${CARGO_PINS[@]}"; do
    if [ "${p%%|*}" = "$t" ]; then
      fetch_cargo "$p"
      return 0
    fi
  done
  die "unknown tool $t (known: kubeconform crane shellcheck rcodesign cargo-fuzz rsign2 weed kafka jre)"
}

main() {
  platform_ok
  local want=("$@")
  [ ${#want[@]} -gt 0 ] || want=("${REQUIRED[@]}")
  local t
  for t in "${want[@]}"; do
    fetch_one "$t"
  done
}

main "$@"
