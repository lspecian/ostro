#!/usr/bin/env bash
# install.sh against a local http server (Phase 14, 14-11; D-P14-24).
#
# usage: scripts/test-install.sh [--dist DIR]
#   DIR: the output of scripts/release-dry-run.sh (default target/release-dry-run/dist; must
#   hold a signed x86_64 tarball, SHA256SUMS, SHA256SUMS.minisig and minisign.pub).
#
# Serves a temp copy of the dist directory as <root>/v<V>/... plus <root>/latest/VERSION with
# `python3 -m http.server --bind 127.0.0.1 0` and runs install.sh with
# OSTRO_DOWNLOAD_BASE=http://127.0.0.1:<port>. Cases:
#   1. rsign on PATH, OSTRO_REQUIRE_SIGNATURE=1, version latest -> installs; ostro --version runs
#   2. tarball with one byte flipped -> fails with a checksum message, installs nothing
#   3. SHA256SUMS.minisig from another ephemeral key, signature required -> fails, installs nothing
#   4. no verifier on PATH, signature required -> fails, installs nothing
#   5. no verifier on PATH, not required, --version V -> installs with a warning
# install.sh always runs with a PATH of shims for the few tools it needs, so a minisign or
# rsign installed on the machine cannot leak into the "no verifier" cases.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DIST="$ROOT/target/release-dry-run/dist"
while [ $# -gt 0 ]; do
  case $1 in
    --dist) DIST=$2; shift 2 ;;
    *) echo "test-install: unknown argument: $1" >&2; exit 2 ;;
  esac
done

die() { echo "test-install: FAIL: $*" >&2; exit 1; }

[ "$(uname -s)" = Linux ] || die "Linux only (macOS later)"
case "$(uname -m)" in
  x86_64) TARGET=x86_64-unknown-linux-musl ;;
  aarch64) TARGET=aarch64-unknown-linux-musl ;;
  *) die "unsupported architecture $(uname -m)" ;;
esac
shopt -s nullglob
tarballs=("$DIST"/ostro-*-"$TARGET".tar.gz)
shopt -u nullglob
[ ${#tarballs[@]} -eq 1 ] ||
  die "want exactly one $TARGET tarball in $DIST, found ${#tarballs[@]} (run scripts/release-dry-run.sh first)"
TARBALL="$(basename "${tarballs[0]}")"
VERSION="${TARBALL#ostro-}"
VERSION="${VERSION%-"$TARGET".tar.gz}"
for f in SHA256SUMS SHA256SUMS.minisig minisign.pub; do
  [ -s "$DIST/$f" ] || die "$DIST/$f missing (run scripts/release-dry-run.sh without --no-sign)"
done
PUBKEY="$(sed -n 2p "$DIST/minisign.pub")"
[ -n "$PUBKEY" ] || die "cannot read the public key from $DIST/minisign.pub"
RSIGN="$(bash "$ROOT/scripts/fetch-tools.sh" rsign2 | sed -n 's/^rsign2=//p')"
[ -x "$RSIGN" ] || die "rsign (rsign2) not available"

WORK="$(mktemp -d "${TMPDIR:-/tmp}/ostro-test-install.XXXXXX")"
SERVER_PID=""
cleanup() {
  if [ -n "$SERVER_PID" ]; then kill "$SERVER_PID" 2>/dev/null || :; wait "$SERVER_PID" 2>/dev/null || :; fi
  rm -rf "$WORK"
}
trap cleanup EXIT

# --- served tree: good release + tampered/badsig variants under their own bases ----------
SRV="$WORK/srv"
mk_release() { # dir
  mkdir -p "$1/v$VERSION" "$1/latest"
  cp "$DIST/$TARBALL" "$DIST/SHA256SUMS" "$DIST/SHA256SUMS.minisig" "$1/v$VERSION/"
  printf '%s\n' "$VERSION" >"$1/latest/VERSION"
}
mk_release "$SRV/good"
mk_release "$SRV/tampered"
python3 - "$SRV/tampered/v$VERSION/$TARBALL" <<'EOF'
import sys
p = sys.argv[1]
b = bytearray(open(p, "rb").read())
b[len(b) // 2] ^= 0x01
open(p, "wb").write(b)
EOF
mk_release "$SRV/badsig"
mkdir -p "$WORK/otherkey"
"$RSIGN" generate -W -f -c "other ephemeral key" -p "$WORK/otherkey/k.pub" -s "$WORK/otherkey/k.key" >/dev/null
"$RSIGN" sign -W -s "$WORK/otherkey/k.key" -p "$WORK/otherkey/k.pub" \
  -x "$SRV/badsig/v$VERSION/SHA256SUMS.minisig" "$SRV/badsig/v$VERSION/SHA256SUMS" >/dev/null

python3 -u -m http.server --bind 127.0.0.1 --directory "$SRV" 0 >"$WORK/server.log" 2>&1 &
SERVER_PID=$!
PORT=""
for _ in $(seq 1 100); do
  PORT="$(sed -n 's/.* port \([0-9][0-9]*\) .*/\1/p' "$WORK/server.log" | head -n1)"
  [ -n "$PORT" ] && break
  kill -0 "$SERVER_PID" 2>/dev/null || { cat "$WORK/server.log" >&2; die "http server exited"; }
  sleep 0.1
done
[ -n "$PORT" ] || { cat "$WORK/server.log" >&2; die "http server did not report its port"; }
URL="http://127.0.0.1:$PORT"
curl -fsS -o /dev/null "$URL/good/latest/VERSION" || die "http server not answering on $URL"
echo "test-install: serving $DIST ($VERSION, $TARGET) on $URL"

# --- PATH shims: only the tools install.sh needs (no minisign/rsign) ---------------------
SHIMS="$WORK/shims"
mkdir -p "$SHIMS"
for t in sh uname curl mktemp rm mkdir tr awk sed cat cp mv chmod tar gzip sha256sum printf; do
  p="$(command -v "$t" 2>/dev/null)" || continue
  case $p in /*) ln -s "$p" "$SHIMS/$t" ;; esac
done
for t in sh curl tar gzip sha256sum awk; do [ -e "$SHIMS/$t" ] || die "required tool $t not found"; done
VERIFIER="$WORK/verifier"
mkdir -p "$VERIFIER"
ln -s "$RSIGN" "$VERIFIER/rsign"
SH="$(command -v sh)"

run_install() { # case-name base path require out-dir [args...]
  local name=$1 base=$2 path=$3 req=$4 dir=$5
  shift 5
  mkdir -p "$dir"
  set +e
  env -i HOME="$WORK/home" PATH="$path" TMPDIR="$WORK" \
    OSTRO_DOWNLOAD_BASE="$URL/$base" OSTRO_INSTALL_DIR="$dir" \
    OSTRO_REQUIRE_SIGNATURE="$req" OSTRO_MINISIGN_PUBKEY="$PUBKEY" \
    "$SH" "$ROOT/install.sh" "$@" >"$WORK/$name.out" 2>&1
  RC=$?
  set -e
  sed "s/^/  [$name] /" "$WORK/$name.out"
}
installed_nothing() { # dir
  [ -z "$(ls -A "$1")" ]
}

# 1. good release, signature required, rsign available
run_install case1 good "$VERIFIER:$SHIMS" 1 "$WORK/i1"
[ "$RC" = 0 ] || die "case 1: install failed (rc=$RC)"
grep -q "signature OK (rsign)" "$WORK/case1.out" || die "case 1: signature was not verified"
out="$("$WORK/i1/ostro" --version)"
[ "$out" = "ostro $VERSION" ] || die "case 1: installed binary printed ${out@Q}"
echo "test-install: PASS 1 (signed install, latest -> $VERSION, '$out')"

# 2. tampered tarball
run_install case2 tampered "$VERIFIER:$SHIMS" 1 "$WORK/i2"
[ "$RC" != 0 ] || die "case 2: tampered tarball was installed"
grep -q "checksum mismatch" "$WORK/case2.out" || die "case 2: no checksum message"
installed_nothing "$WORK/i2" || die "case 2: something was installed"
echo "test-install: PASS 2 (tampered tarball rejected: checksum mismatch)"

# 3. signature from another key, signature required
run_install case3 badsig "$VERIFIER:$SHIMS" 1 "$WORK/i3"
[ "$RC" != 0 ] || die "case 3: bad signature was accepted"
grep -q "signature verification FAILED" "$WORK/case3.out" || die "case 3: no signature message"
installed_nothing "$WORK/i3" || die "case 3: something was installed"
echo "test-install: PASS 3 (signature from another key rejected)"

# 4. no verifier, signature required
run_install case4 good "$SHIMS" 1 "$WORK/i4"
[ "$RC" != 0 ] || die "case 4: installed without a verifier although required"
grep -q "signature required" "$WORK/case4.out" || die "case 4: no 'signature required' message"
installed_nothing "$WORK/i4" || die "case 4: something was installed"
echo "test-install: PASS 4 (no verifier + OSTRO_REQUIRE_SIGNATURE=1 refused)"

# 5. no verifier, not required, explicit version
run_install case5 good "$SHIMS" 0 "$WORK/i5" --version "v$VERSION"
[ "$RC" = 0 ] || die "case 5: install failed (rc=$RC)"
grep -q "warning: signature not verified" "$WORK/case5.out" || die "case 5: no warning"
[ -x "$WORK/i5/ostro" ] || die "case 5: ostro not installed"
"$WORK/i5/ostro" --version >/dev/null || die "case 5: installed binary does not run"
echo "test-install: PASS 5 (no verifier, not required: installed with a warning)"

echo "test-install: OK (5/5)"
