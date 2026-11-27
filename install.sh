#!/bin/sh
# Ostro installer (Phase 14, D-P14-24). POSIX sh.
#
#   curl -fsSL <release>/install.sh | sh
#   sh install.sh [--version V] [--system] [--dir D]
#
#   --version V   install version V (default: latest, read from <base>/latest/VERSION)
#   --system      install into /usr/local/bin (uses sudo only when needed and present)
#   --dir D       install into D
#
# Environment:
#   OSTRO_DOWNLOAD_BASE      where releases live: <base>/v<V>/<asset> and <base>/latest/VERSION
#                            (for a GitHub base ending in /releases/download, latest is
#                            resolved through /releases/latest/download/VERSION). The default
#                            is stamped at publish time.
#   OSTRO_INSTALL_DIR        install directory (default $HOME/.local/bin)
#   OSTRO_REQUIRE_SIGNATURE  1 = fail unless SHA256SUMS.minisig verifies (minisign or rsign
#                            on PATH and a public key known)
#   OSTRO_MINISIGN_PUBKEY    override the embedded minisign public key (base64 key line)
#
# The tarball's sha256 is always checked against SHA256SUMS. SHA256SUMS.minisig is checked
# when `minisign` or `rsign` is on PATH (required with OSTRO_REQUIRE_SIGNATURE=1).
set -eu

# Stamped by the release workflow at publish time.
DEFAULT_DOWNLOAD_BASE="https://github.com/ostro-dev/ostro/releases/download"
MINISIGN_PUBKEY=""

BASE="${OSTRO_DOWNLOAD_BASE:-$DEFAULT_DOWNLOAD_BASE}"
BASE="${BASE%/}"
PUBKEY="${OSTRO_MINISIGN_PUBKEY:-$MINISIGN_PUBKEY}"
REQUIRE_SIG="${OSTRO_REQUIRE_SIGNATURE:-0}"
VERSION="latest"
DIR="${OSTRO_INSTALL_DIR:-}"
SYSTEM=0

say() { printf 'ostro-install: %s\n' "$*" >&2; }
warn() { printf 'ostro-install: warning: %s\n' "$*" >&2; }
die() { printf 'ostro-install: error: %s\n' "$*" >&2; exit 1; }

while [ $# -gt 0 ]; do
  case $1 in
    --version) [ $# -ge 2 ] || die "--version needs a value"; VERSION=$2; shift 2 ;;
    --version=*) VERSION=${1#--version=}; shift ;;
    --system) SYSTEM=1; shift ;;
    --dir) [ $# -ge 2 ] || die "--dir needs a value"; DIR=$2; shift 2 ;;
    --dir=*) DIR=${1#--dir=}; shift ;;
    -h | --help) sed -n '2,23p' "$0" 2>/dev/null || :; exit 0 ;;
    *) die "unknown argument: $1" ;;
  esac
done

if [ "$SYSTEM" = 1 ]; then
  DIR=/usr/local/bin
elif [ -z "$DIR" ]; then
  [ -n "${HOME:-}" ] || die "HOME is not set; use --dir or OSTRO_INSTALL_DIR"
  DIR="$HOME/.local/bin"
fi

# Plain http only for a local test server; everything else is https only.
case $BASE in
  http://127.0.0.1:* | http://127.0.0.1/* | http://localhost:* | http://localhost/*) PROTO='=https,http' ;;
  https://*) PROTO='=https' ;;
  *) die "OSTRO_DOWNLOAD_BASE must be an https:// URL (http only for 127.0.0.1/localhost): $BASE" ;;
esac

command -v curl >/dev/null 2>&1 || die "curl is required"
command -v tar >/dev/null 2>&1 || die "tar is required"

fetch() { # url out
  curl -fsSL --proto "$PROTO" --retry 3 -o "$2" "$1" || die "download failed: $1"
}

# --- platform -----------------------------------------------------------------------------
os=$(uname -s)
arch=$(uname -m)
case $arch in
  x86_64 | amd64) arch=x86_64 ;;
  aarch64 | arm64) arch=aarch64 ;;
  *) die "unsupported architecture: $arch (x86_64 and aarch64 only)" ;;
esac
case $os in
  Linux) target="$arch-unknown-linux-musl" ;;
  Darwin) target="$arch-apple-darwin" ;; # the signed bare binary (curl sets no quarantine)
  *) die "unsupported OS: $os (Linux and macOS only)" ;;
esac

tmp=$(mktemp -d "${TMPDIR:-/tmp}/ostro-install.XXXXXX") || die "mktemp failed"
trap 'rm -rf "$tmp"' EXIT
trap 'exit 130' INT TERM

# --- version ------------------------------------------------------------------------------
if [ "$VERSION" = latest ]; then
  case $BASE in
    https://github.com/*/releases/download) latest_url="${BASE%/download}/latest/download/VERSION" ;;
    *) latest_url="$BASE/latest/VERSION" ;;
  esac
  fetch "$latest_url" "$tmp/VERSION"
  VERSION=$(tr -d ' \t\r\n' <"$tmp/VERSION")
fi
VERSION=${VERSION#v}
case $VERSION in
  '' | *[!0-9A-Za-z.+-]*) die "invalid version: '$VERSION'" ;;
esac

asset="ostro-$VERSION-$target.tar.gz"
url="$BASE/v$VERSION"
say "installing ostro $VERSION ($target) from $url"
fetch "$url/$asset" "$tmp/$asset"
fetch "$url/SHA256SUMS" "$tmp/SHA256SUMS"

# --- signature over SHA256SUMS ------------------------------------------------------------
sig_ok=0
if curl -fsSL --proto "$PROTO" --retry 3 -o "$tmp/SHA256SUMS.minisig" "$url/SHA256SUMS.minisig" 2>/dev/null; then
  have_sig=1
else
  have_sig=0
fi
verifier=""
if command -v minisign >/dev/null 2>&1; then
  verifier=minisign
elif command -v rsign >/dev/null 2>&1; then
  verifier=rsign
fi
if [ "$have_sig" = 1 ] && [ -n "$verifier" ] && [ -n "$PUBKEY" ]; then
  if [ "$verifier" = minisign ]; then
    minisign -Vm "$tmp/SHA256SUMS" -x "$tmp/SHA256SUMS.minisig" -P "$PUBKEY" >/dev/null 2>&1 ||
      die "signature verification FAILED for SHA256SUMS (minisign); not installing"
  else
    rsign verify -q -P "$PUBKEY" -x "$tmp/SHA256SUMS.minisig" "$tmp/SHA256SUMS" >/dev/null 2>&1 ||
      die "signature verification FAILED for SHA256SUMS (rsign); not installing"
  fi
  sig_ok=1
  say "signature OK ($verifier)"
fi
if [ "$sig_ok" = 0 ]; then
  if [ "$have_sig" = 0 ]; then
    why="SHA256SUMS.minisig not found"
  elif [ -z "$verifier" ]; then
    why="neither minisign nor rsign is on PATH"
  else
    why="no minisign public key (set OSTRO_MINISIGN_PUBKEY)"
  fi
  if [ "$REQUIRE_SIG" = 1 ]; then
    die "signature required (OSTRO_REQUIRE_SIGNATURE=1) but cannot be verified: $why"
  fi
  warn "signature not verified: $why (sha256 is still checked)"
fi

# --- sha256 of the tarball ----------------------------------------------------------------
want=$(awk -v f="$asset" '$2 == f || $2 == "*" f { print $1; exit }' "$tmp/SHA256SUMS")
[ -n "$want" ] || die "$asset is not listed in SHA256SUMS"
if command -v sha256sum >/dev/null 2>&1; then
  got=$(sha256sum "$tmp/$asset" | awk '{ print $1 }')
elif command -v shasum >/dev/null 2>&1; then
  got=$(shasum -a 256 "$tmp/$asset" | awk '{ print $1 }')
else
  die "sha256sum or shasum is required"
fi
[ "$got" = "$want" ] || die "checksum mismatch for $asset (got $got, want $want); not installing"
say "checksum OK"

# --- extract and install atomically -------------------------------------------------------
mkdir "$tmp/x"
tar -xzf "$tmp/$asset" -C "$tmp/x" ostro || die "cannot extract ostro from $asset"
[ -f "$tmp/x/ostro" ] || die "$asset does not contain ostro"
chmod 0755 "$tmp/x/ostro"

SUDO=""
if [ ! -d "$DIR" ]; then
  mkdir -p "$DIR" 2>/dev/null || SUDO=sudo
fi
if [ -z "$SUDO" ] && [ ! -w "$DIR" ]; then SUDO=sudo; fi
if [ -n "$SUDO" ]; then
  [ "$SYSTEM" = 1 ] || die "$DIR is not writable"
  command -v sudo >/dev/null 2>&1 || die "$DIR is not writable and sudo is not available"
  say "using sudo to write $DIR"
  sudo mkdir -p "$DIR"
fi
# Copy next to the destination, then rename: the old binary is replaced in one step.
$SUDO cp "$tmp/x/ostro" "$DIR/.ostro.install.$$"
$SUDO chmod 0755 "$DIR/.ostro.install.$$"
$SUDO mv -f "$DIR/.ostro.install.$$" "$DIR/ostro"

say "installed $DIR/ostro"
case ":${PATH:-}:" in
  *":$DIR:"*) ;;
  *) say "add $DIR to your PATH, e.g.: export PATH=\"$DIR:\$PATH\"" ;;
esac
