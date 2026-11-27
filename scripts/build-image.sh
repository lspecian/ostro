#!/usr/bin/env bash
# Build the Ostro container image FROM scratch without Docker (Phase 14, 14-12; D-P14-25).
#
# usage: scripts/build-image.sh [--binary-x86_64 P] [--binary-aarch64 P] [--tag local/ostro:dev]
#                               [--out target/image] [--push REF]
#
#   --binary-x86_64 P   static musl ostro for linux/amd64
#                       (default ${CARGO_TARGET_DIR:-target}/x86_64-unknown-linux-musl/release/ostro)
#   --binary-aarch64 P  static musl ostro for linux/arm64; default
#                       ${CARGO_TARGET_DIR:-target}/aarch64-unknown-linux-musl/release/ostro when
#                       that file exists. With both binaries the result is a multi-arch index.
#   --tag T             repository:tag inside the loopback registry and the layout's ref name
#   --out D             output directory (default target/image): D/oci is the OCI image layout,
#                       D/index.digest the index digest, D/cache the verified CA bundle
#   --push REF          CI only (release.yml, CI=true): copy the verified index to REF. This is
#                       the ONLY code path that contacts a non-loopback registry; it is refused
#                       unless CI=true. Pushing is human_needed (deploy/image/README.md).
#
# How: one reproducible layer tar (Python tarfile, mtime = SOURCE_DATE_EPOCH, default the HEAD
# commit time) with /ostro (0755), /etc/ssl/certs/ca-certificates.crt (0644, the curl.se bundle
# pinned in deploy/image/cacert.pin, downloaded into its own empty directory and sha256-checked
# before use), /data/ (0750, 65532:65532) and /tmp/ (1777); an ephemeral `crane registry serve`
# on 127.0.0.1 with its storage under the output directory (started and stopped here, never
# publishing); `crane append --oci-empty-base` + `crane mutate` (user 65532:65532, entrypoint
# /ostro, cmd server, ports 7700/7701, SSL_CERT_FILE, workdir /data, OCI labels from
# deploy/image/labels.env + version/revision/created) per platform; `crane index append`;
# `crane pull --format=oci` into D/oci. crane v0.22.1 comes from scripts/fetch-tools.sh.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_DIR="${CARGO_TARGET_DIR:-$ROOT/target}"

die() { echo "build-image: $*" >&2; exit 1; }
log() { echo "build-image: $*" >&2; }

BIN_X86="$TARGET_DIR/x86_64-unknown-linux-musl/release/ostro"
BIN_ARM=""
ARM_GIVEN=0
TAG="local/ostro:dev"
OUT="$ROOT/target/image"
PUSH=""

while [ $# -gt 0 ]; do
  case $1 in
    --binary-x86_64) [ $# -ge 2 ] || die "$1 needs a value"; BIN_X86=$2; shift 2 ;;
    --binary-aarch64) [ $# -ge 2 ] || die "$1 needs a value"; BIN_ARM=$2; ARM_GIVEN=1; shift 2 ;;
    --tag) [ $# -ge 2 ] || die "$1 needs a value"; TAG=$2; shift 2 ;;
    --out) [ $# -ge 2 ] || die "$1 needs a value"; OUT=$2; shift 2 ;;
    --push) [ $# -ge 2 ] || die "$1 needs a value"; PUSH=$2; shift 2 ;;
    -h | --help) sed -n '2,27p' "$0"; exit 0 ;;
    *) die "unknown argument: $1 (see --help)" ;;
  esac
done

# The push guard comes first: nothing below may run for a refused push.
if [ -n "$PUSH" ] && [ "${CI:-}" != true ]; then
  die "--push contacts a remote registry and runs only in CI (CI=true, release.yml); refused"
fi

[[ $TAG =~ ^[a-z0-9]+([._/-][a-z0-9]+)*:[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$ ]] ||
  die "bad --tag '$TAG' (expected repository:tag, lowercase repository)"
REPO=${TAG%%:*}
TAGNAME=${TAG##*:}

if [ "$ARM_GIVEN" = 0 ] && [ -f "$TARGET_DIR/aarch64-unknown-linux-musl/release/ostro" ]; then
  BIN_ARM="$TARGET_DIR/aarch64-unknown-linux-musl/release/ostro"
fi
[ -f "$BIN_X86" ] || die "x86_64 binary not found: $BIN_X86 (cargo zigbuild --release -p ostro --target x86_64-unknown-linux-musl)"
if [ -n "$BIN_ARM" ] && [ ! -f "$BIN_ARM" ]; then die "aarch64 binary not found: $BIN_ARM"; fi

command -v python3 >/dev/null 2>&1 || die "python3 is required"
mkdir -p "$OUT"
OUT="$(cd "$OUT" && pwd)"

CRANE=$(bash "$ROOT/scripts/fetch-tools.sh" crane | sed -n 's/^crane=//p')
[ -x "$CRANE" ] || die "crane not available from scripts/fetch-tools.sh"
"$CRANE" version | grep -qx '0.22.1' || die "crane is not v0.22.1"

# --- temporary state, the loopback registry and cleanup ---------------------------------------
WORK=$(mktemp -d "$OUT/.work.XXXXXX")
REG_PID=""
REG=""
stop_registry() {
  if [ -n "$REG_PID" ]; then
    kill "$REG_PID" 2>/dev/null || :
    wait "$REG_PID" 2>/dev/null || :
    REG_PID=""
    REG=""
  fi
}
cleanup() {
  stop_registry
  rm -rf "$WORK"
}
trap cleanup EXIT

start_registry() { # storage dir (recreated)
  local dir=$1 port="" i
  rm -rf "$dir"
  mkdir -p "$dir/blobs"
  "$CRANE" registry serve --address 127.0.0.1:0 --disk "$dir/blobs" >"$dir/serve.log" 2>&1 &
  REG_PID=$!
  for i in $(seq 1 200); do
    port=$(sed -n 's/.*serving on port \([0-9][0-9]*\).*/\1/p' "$dir/serve.log")
    [ -n "$port" ] && break
    kill -0 "$REG_PID" 2>/dev/null || { cat "$dir/serve.log" >&2; die "crane registry serve exited"; }
    sleep 0.05
  done
  [ -n "$port" ] || die "crane registry serve did not report its port (try $i)"
  REG="127.0.0.1:$port"
  log "loopback registry $REG (storage $dir/blobs)"
}

# --- inputs ------------------------------------------------------------------------------------
SOURCE_DATE_EPOCH=${SOURCE_DATE_EPOCH:-$(git -C "$ROOT" log -1 --format=%ct 2>/dev/null || echo 0)}
[[ $SOURCE_DATE_EPOCH =~ ^[0-9]+$ ]] || die "SOURCE_DATE_EPOCH must be an integer"
REVISION=$(git -C "$ROOT" rev-parse HEAD 2>/dev/null || echo unknown)
VERSION=$(python3 -I - "$ROOT/Cargo.toml" <<'PY'
import sys, tomllib
with open(sys.argv[1], "rb") as f:
    print(tomllib.load(f)["workspace"]["package"]["version"])
PY
)
CREATED=$(date -u -d "@$SOURCE_DATE_EPOCH" +%Y-%m-%dT%H:%M:%SZ)

check_binary() { # path arch(amd64|arm64): static ELF64 for the arch, no program interpreter
  python3 -I - "$1" "$2" <<'PY'
import struct, sys
path, arch = sys.argv[1], sys.argv[2]
want = {"amd64": 0x3E, "arm64": 0xB7}[arch]
with open(path, "rb") as f:
    h = f.read(64)
    if len(h) < 64 or h[:4] != b"\x7fELF" or h[4] != 2 or h[5] != 1:
        sys.exit(f"{path}: not a little-endian ELF64 file")
    machine = struct.unpack_from("<H", h, 18)[0]
    if machine != want:
        sys.exit(f"{path}: e_machine {machine:#x}, want {want:#x} for linux/{arch}")
    phoff, = struct.unpack_from("<Q", h, 32)
    phentsize, phnum = struct.unpack_from("<HH", h, 54)
    for i in range(phnum):
        f.seek(phoff + i * phentsize)
        if struct.unpack("<I", f.read(4))[0] == 3:  # PT_INTERP
            sys.exit(f"{path}: dynamically linked (has PT_INTERP); a scratch image needs a static binary")
PY
}

fetch_cacert() { # sets CACERT
  local pin="$ROOT/deploy/image/cacert.pin" url sum name cache dl got
  url=$(sed -n 's/^url=//p' "$pin")
  sum=$(sed -n 's/^sha256=//p' "$pin")
  [[ $url =~ ^https://curl\.se/ca/cacert-[0-9]{4}-[0-9]{2}-[0-9]{2}\.pem$ ]] || die "bad url in $pin"
  [[ $sum =~ ^[0-9a-f]{64}$ ]] || die "bad sha256 in $pin"
  name=$(basename "$url")
  cache="$OUT/cache/$name"
  if [ -f "$cache" ] && [ "$(sha256sum "$cache" | cut -d' ' -f1)" = "$sum" ]; then
    CACERT=$cache
    return 0
  fi
  # Untrusted download: its own fresh, empty directory; checked before it is used or moved.
  dl=$(mktemp -d "$OUT/.dl-cacert.XXXXXX")
  curl -fsSL --proto '=https' --retry 3 --retry-delay 2 --connect-timeout 20 -o "$dl/$name" "$url" ||
    { rm -rf "$dl"; die "download failed: $url"; }
  got=$(sha256sum "$dl/$name" | cut -d' ' -f1)
  if [ "$got" != "$sum" ]; then
    rm -rf "$dl"
    die "CA bundle sha256 mismatch: got $got, want $sum ($url)"
  fi
  mkdir -p "$OUT/cache"
  mv "$dl/$name" "$cache"
  rmdir "$dl"
  CACERT=$cache
}

build_layer() { # out.tar binary cacert
  python3 -I - "$1" "$2" "$3" "$SOURCE_DATE_EPOCH" <<'PY'
import os, sys, tarfile
out, binary, cacert, epoch = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])

def entry(name, kind, mode, uid=0, gid=0, size=0):
    ti = tarfile.TarInfo(name)
    ti.type, ti.mode, ti.uid, ti.gid, ti.size, ti.mtime = kind, mode, uid, gid, size, epoch
    ti.uname = ti.gname = ""
    return ti

with open(out, "wb") as f, tarfile.open(fileobj=f, mode="w", format=tarfile.USTAR_FORMAT) as t:
    t.addfile(entry("data/", tarfile.DIRTYPE, 0o750, 65532, 65532))
    for d in ("etc/", "etc/ssl/", "etc/ssl/certs/"):
        t.addfile(entry(d, tarfile.DIRTYPE, 0o755))
    for name, path, mode in (("etc/ssl/certs/ca-certificates.crt", cacert, 0o644),
                             ("ostro", binary, 0o755)):
        with open(path, "rb") as src:
            t.addfile(entry(name, tarfile.REGTYPE, mode, size=os.fstat(src.fileno()).st_size), src)
    t.addfile(entry("tmp/", tarfile.DIRTYPE, 0o1777))
PY
}

LABEL_ARGS=()
while IFS= read -r line; do
  case $line in '' | '#'*) continue ;; esac
  [[ $line =~ ^org\.opencontainers\.image\.[a-z.]+=.+$ ]] || die "bad line in labels.env: $line"
  LABEL_ARGS+=(--label "$line")
done <"$ROOT/deploy/image/labels.env"
LABEL_ARGS+=(--label "org.opencontainers.image.version=$VERSION"
  --label "org.opencontainers.image.revision=$REVISION"
  --label "org.opencontainers.image.created=$CREATED")

# --- build -------------------------------------------------------------------------------------
PLATFORMS=(amd64)
declare -A BIN=([amd64]="$BIN_X86")
if [ -n "$BIN_ARM" ]; then
  PLATFORMS+=(arm64)
  BIN[arm64]="$BIN_ARM"
fi
for p in "${PLATFORMS[@]}"; do check_binary "${BIN[$p]}" "$p"; done
fetch_cacert
log "inputs: version $VERSION, revision $REVISION, SOURCE_DATE_EPOCH $SOURCE_DATE_EPOCH, CA $(basename "$CACERT")"

start_registry "$OUT/registry"
MANIFESTS=()
for p in "${PLATFORMS[@]}"; do
  layer="$WORK/layer-$p.tar"
  build_layer "$layer" "${BIN[$p]}" "$CACERT"
  ref="$REG/$REPO:build-$p"
  "$CRANE" append --oci-empty-base -f "$layer" -t "$ref" >/dev/null
  "$CRANE" mutate "$ref" --set-platform "linux/$p" \
    --user 65532:65532 --entrypoint /ostro --cmd server \
    --exposed-ports 7700/tcp,7701/tcp \
    --env SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt \
    --workdir /data "${LABEL_ARGS[@]}" -t "$ref" >/dev/null
  MANIFESTS+=(-m "$ref")
  log "linux/$p manifest $("$CRANE" digest "$ref") (binary sha256 $(sha256sum "${BIN[$p]}" | cut -d' ' -f1))"
done
"$CRANE" index append "${MANIFESTS[@]}" -t "$REG/$TAG" >/dev/null
INDEX_DIGEST=$("$CRANE" digest "$REG/$TAG")

rm -rf "$OUT/oci"
"$CRANE" pull --format=oci "$REG/$TAG" "$OUT/oci"
# Name the index in the layout (the tag only; crane's --annotate-ref would record the
# ephemeral loopback port and make index.json differ between builds).
python3 -I - "$OUT/oci/index.json" "$TAGNAME" "$INDEX_DIGEST" <<'PY'
import json, sys
path, tag, digest = sys.argv[1], sys.argv[2], sys.argv[3]
with open(path) as f:
    idx = json.load(f)
m = idx["manifests"]
if len(m) != 1 or m[0]["digest"] != digest:
    sys.exit(f"{path}: expected exactly the index {digest}, got {[d['digest'] for d in m]}")
m[0].setdefault("annotations", {})["org.opencontainers.image.ref.name"] = tag
with open(path, "w") as f:
    json.dump(idx, f, sort_keys=True, separators=(",", ":"))
PY
echo "$INDEX_DIGEST" >"$OUT/index.digest"

if [ -n "$PUSH" ]; then
  log "pushing $INDEX_DIGEST to $PUSH (CI)"
  "$CRANE" copy "$REG/$TAG@$INDEX_DIGEST" "$PUSH"
fi
stop_registry
rm -rf "$OUT/registry"

echo "image: $TAG (${PLATFORMS[*]}) -> $OUT/oci"
echo "index: $INDEX_DIGEST"
