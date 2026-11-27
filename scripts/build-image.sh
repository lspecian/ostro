#!/usr/bin/env bash
# Build the Ostro container image FROM scratch without Docker (Phase 14, 14-12; D-P14-25).
#
# usage: scripts/build-image.sh [--binary-x86_64 P] [--binary-aarch64 P] [--tag local/ostro:dev]
#                               [--out target/image] [--verify | --no-verify] [--push REF]
#
#   --binary-x86_64 P   static musl ostro for linux/amd64
#                       (default ${CARGO_TARGET_DIR:-target}/x86_64-unknown-linux-musl/release/ostro)
#   --binary-aarch64 P  static musl ostro for linux/arm64; default
#                       ${CARGO_TARGET_DIR:-target}/aarch64-unknown-linux-musl/release/ostro when
#                       that file exists. With both binaries the result is a multi-arch index.
#   --tag T             repository:tag inside the loopback registry and the layout's ref name
#   --out D             output directory (default target/image): D/oci is the OCI image layout,
#                       D/index.digest the index digest, D/cache the verified CA bundle
#   --verify            only verify the existing layout D/oci (no build)
#   --no-verify         build only (verification runs by default after a build)
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
#
# Verification: a fresh loopback registry, `crane push D/oci`, then per platform `crane config`
# (user, entrypoint, cmd, ports, env, workdir, labels, platform) and `crane export` (exactly
# ostro, etc/, etc/ssl/, etc/ssl/certs/, etc/ssl/certs/ca-certificates.crt, data/, tmp/ with
# their modes and owners; no shell, no libc; CA bundle sha256 = the pin; binary sha256 = the
# input), the index's platform set, and the extracted /ostro --version (host platform, empty
# environment). Prints `verify: OK`, or the differences and exits non-zero.
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
DO_BUILD=1
DO_VERIFY=1

while [ $# -gt 0 ]; do
  case $1 in
    --binary-x86_64) [ $# -ge 2 ] || die "$1 needs a value"; BIN_X86=$2; shift 2 ;;
    --binary-aarch64) [ $# -ge 2 ] || die "$1 needs a value"; BIN_ARM=$2; ARM_GIVEN=1; shift 2 ;;
    --tag) [ $# -ge 2 ] || die "$1 needs a value"; TAG=$2; shift 2 ;;
    --out) [ $# -ge 2 ] || die "$1 needs a value"; OUT=$2; shift 2 ;;
    --push) [ $# -ge 2 ] || die "$1 needs a value"; PUSH=$2; shift 2 ;;
    --verify) DO_BUILD=0; DO_VERIFY=1; shift ;;
    --no-verify) DO_VERIFY=0; shift ;;
    -h | --help) sed -n '2,36p' "$0"; exit 0 ;;
    *) die "unknown argument: $1 (see --help)" ;;
  esac
done

# The push guard comes first: nothing below may run for a refused push.
if [ -n "$PUSH" ] && [ "${CI:-}" != true ]; then
  die "--push contacts a remote registry and runs only in CI (CI=true, release.yml); refused"
fi
if [ -n "$PUSH" ] && [ "$DO_VERIFY" = 0 ]; then die "--push needs verification (drop --no-verify)"; fi

[[ $TAG =~ ^[a-z0-9]+([._/-][a-z0-9]+)*:[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$ ]] ||
  die "bad --tag '$TAG' (expected repository:tag, lowercase repository)"
REPO=${TAG%%:*}
TAGNAME=${TAG##*:}

if [ "$ARM_GIVEN" = 0 ] && [ -f "$TARGET_DIR/aarch64-unknown-linux-musl/release/ostro" ]; then
  BIN_ARM="$TARGET_DIR/aarch64-unknown-linux-musl/release/ostro"
fi
if [ "$DO_BUILD" = 1 ] && [ ! -f "$BIN_X86" ]; then
  die "x86_64 binary not found: $BIN_X86 (cargo zigbuild --release -p ostro --target x86_64-unknown-linux-musl)"
fi
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
  rm -rf "$WORK" "$OUT/registry" "$OUT/verify-registry"
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

build_image() { # sets INDEX_DIGEST; writes $OUT/oci and $OUT/index.digest
  local p layer ref
  local -a manifests=()
  for p in "${PLATFORMS[@]}"; do check_binary "${BIN[$p]}" "$p"; done
  fetch_cacert
  log "inputs: version $VERSION, revision $REVISION, SOURCE_DATE_EPOCH $SOURCE_DATE_EPOCH, CA $(basename "$CACERT")"
  start_registry "$OUT/registry"
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
    manifests+=(-m "$ref")
    log "linux/$p manifest $("$CRANE" digest "$ref") (binary sha256 $(sha256sum "${BIN[$p]}" | cut -d' ' -f1))"
  done
  "$CRANE" index append "${manifests[@]}" -t "$REG/$TAG" >/dev/null
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
  stop_registry
  rm -rf "$OUT/registry"
  echo "image: $TAG (${PLATFORMS[*]}) -> $OUT/oci"
  echo "index: $INDEX_DIGEST"
}

# --- verify ------------------------------------------------------------------------------------
host_platform() {
  case $(uname -m) in
    x86_64) echo amd64 ;;
    aarch64 | arm64) echo arm64 ;;
    *) echo other ;;
  esac
}

# Checks one platform's config + flattened filesystem; prints MISMATCH lines, fails on any.
check_platform() { # platform config.json fs.tar extract-dir expected-binary-sha|"" run(0|1)
  python3 -I - "$@" "$PIN_SHA" "$VERSION" "$ROOT/deploy/image/labels.env" <<'PY'
import hashlib, json, os, re, subprocess, sys, tarfile
plat, cfg_path, fs_path, xdir, bin_sha, run, pin_sha, version, labels_env = sys.argv[1:10]
bad = []
def want(what, got, exp):
    if got != exp:
        bad.append(f"{what}: got {got!r}, want {exp!r}")

with open(cfg_path) as f:
    cfg = json.load(f)
want("os", cfg.get("os"), "linux")
want("architecture", cfg.get("architecture"), plat)
c = cfg.get("config") or {}
want("User", c.get("User"), "65532:65532")
want("Entrypoint", c.get("Entrypoint"), ["/ostro"])
want("Cmd", c.get("Cmd"), ["server"])
want("WorkingDir", c.get("WorkingDir"), "/data")
want("ExposedPorts", sorted((c.get("ExposedPorts") or {}).keys()), ["7700/tcp", "7701/tcp"])
env = c.get("Env") or []
if "SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt" not in env:
    bad.append(f"Env: SSL_CERT_FILE missing in {env!r}")
labels = c.get("Labels") or {}
exp_labels = {}
with open(labels_env) as f:
    for line in f:
        line = line.strip()
        if line and not line.startswith("#"):
            k, v = line.split("=", 1)
            exp_labels[k] = v
exp_labels["org.opencontainers.image.version"] = version
for k, v in exp_labels.items():
    want(f"label {k}", labels.get(k), v)
rev = labels.get("org.opencontainers.image.revision", "")
if not re.fullmatch(r"[0-9a-f]{40}|unknown", rev):
    bad.append(f"label revision: {rev!r}")
created = labels.get("org.opencontainers.image.created", "")
if not re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ", created):
    bad.append(f"label created: {created!r}")
extra = sorted(set(labels) - set(exp_labels)
               - {"org.opencontainers.image.revision", "org.opencontainers.image.created"})
if extra:
    bad.append(f"unexpected labels: {extra}")

# name -> (kind, mode, uid, gid): exactly these entries, nothing else (no shell, no libc).
expected = {
    "data": ("dir", 0o750, 65532, 65532),
    "etc": ("dir", 0o755, 0, 0),
    "etc/ssl": ("dir", 0o755, 0, 0),
    "etc/ssl/certs": ("dir", 0o755, 0, 0),
    "etc/ssl/certs/ca-certificates.crt": ("file", 0o644, 0, 0),
    "ostro": ("file", 0o755, 0, 0),
    "tmp": ("dir", 0o1777, 0, 0),
}
got = {}
with tarfile.open(fs_path) as t:
    for m in t.getmembers():
        name = m.name
        while name.startswith("./"):
            name = name[2:]
        name = name.strip("/")
        if not name:
            continue
        kind = "dir" if m.isdir() else "file" if m.isreg() else f"type {m.type!r}"
        if name in got:
            bad.append(f"duplicate entry {name}")
        got[name] = (kind, m.mode, m.uid, m.gid)
        if name == "ostro" and m.isreg():
            with open(os.path.join(xdir, "ostro"), "wb") as out:
                out.write(t.extractfile(m).read())
        if name == "etc/ssl/certs/ca-certificates.crt" and m.isreg():
            want("CA bundle sha256", hashlib.sha256(t.extractfile(m).read()).hexdigest(), pin_sha)
missing = sorted(set(expected) - set(got))
unexpected = sorted(set(got) - set(expected))
if missing or unexpected:
    bad.append(f"file list: missing {missing}, unexpected {unexpected}")
for name in sorted(set(expected) & set(got)):
    e, g = expected[name], got[name]
    if e != g:
        bad.append(f"{name}: got {g[0]} {g[1]:o} {g[2]}:{g[3]}, want {e[0]} {e[1]:o} {e[2]}:{e[3]}")

if got.get("ostro", ("",))[0] == "file":
    binary = os.path.join(xdir, "ostro")
    with open(binary, "rb") as f:
        sha = hashlib.sha256(f.read()).hexdigest()
    if bin_sha:
        want("ostro sha256", sha, bin_sha)
    if run == "1":
        os.chmod(binary, 0o755)
        p = subprocess.run([binary, "--version"], env={}, cwd=xdir,
                           capture_output=True, text=True, timeout=60)
        out = p.stdout.strip()
        print(f"verify: linux/{plat} /ostro --version -> {out!r} (empty environment)")
        if p.returncode != 0 or not out.startswith("ostro "):
            bad.append(f"/ostro --version: exit {p.returncode}, stdout {out!r}, "
                       f"stderr {p.stderr.strip()!r}")
    else:
        print(f"verify: linux/{plat} /ostro not run (host is not linux/{plat})")
print(f"verify: linux/{plat} config OK={not bad}; entries: {', '.join(sorted(got))}")
for b in bad:
    print(f"MISMATCH linux/{plat}: {b}")
sys.exit(1 if bad else 0)
PY
}

verify_image() { # sets VERIFIED_REF (loopback registry stays up for --push)
  local layout="$OUT/oci" vref digest want_digest p host plats want_plats xdir bsha run fails=0
  [ -f "$layout/index.json" ] || die "no OCI layout at $layout (build it first)"
  want_digest=$(python3 -I -c 'import json,sys; m=json.load(open(sys.argv[1]))["manifests"]; print(m[0]["digest"] if len(m)==1 else "")' "$layout/index.json")
  [ -n "$want_digest" ] || die "$layout/index.json must reference exactly one index"
  start_registry "$OUT/verify-registry"
  vref="$REG/$REPO:verify"
  "$CRANE" push "$layout" "$vref" >/dev/null
  digest=$("$CRANE" digest "$vref")
  if [ "$digest" != "$want_digest" ]; then
    echo "MISMATCH: pushed digest $digest, layout index $want_digest"
    fails=1
  fi
  "$CRANE" manifest "$vref" >"$WORK/index.json"
  plats=$(python3 -I - "$WORK/index.json" <<'PY'
import json, sys
idx = json.load(open(sys.argv[1]))
if idx.get("mediaType") != "application/vnd.oci.image.index.v1+json":
    sys.exit(f"not an OCI image index: {idx.get('mediaType')}")
print(" ".join(sorted(f"{m['platform']['os']}/{m['platform']['architecture']}"
                      for m in idx["manifests"])))
PY
)
  want_plats=$(printf 'linux/%s\n' "${PLATFORMS[@]}" | sort | tr '\n' ' ')
  want_plats=${want_plats% }
  if [ "$plats" != "$want_plats" ]; then
    echo "MISMATCH: index platforms '$plats', want '$want_plats'"
    fails=1
  fi
  echo "verify: index $digest platforms $plats"
  host=$(host_platform)
  for p in "${PLATFORMS[@]}"; do
    case " $plats " in *" linux/$p "*) ;; *) continue ;; esac
    "$CRANE" config --platform "linux/$p" "$vref" >"$WORK/config-$p.json"
    "$CRANE" export --platform "linux/$p" "$vref" "$WORK/fs-$p.tar"
    xdir="$WORK/x-$p"
    mkdir -p "$xdir"
    bsha=""
    if [ -f "${BIN[$p]}" ]; then bsha=$(sha256sum "${BIN[$p]}" | cut -d' ' -f1); fi
    run=0
    if [ "$p" = "$host" ]; then run=1; fi
    if ! check_platform "$p" "$WORK/config-$p.json" "$WORK/fs-$p.tar" "$xdir" "$bsha" "$run"; then
      fails=1
    fi
  done
  if [ "$fails" != 0 ]; then die "verification failed (MISMATCH lines above)"; fi
  echo "verify: OK"
  VERIFIED_REF="$vref@$digest"
}

PIN_SHA=$(sed -n 's/^sha256=//p' "$ROOT/deploy/image/cacert.pin")
VERIFIED_REF=""
if [ "$DO_BUILD" = 1 ]; then build_image; fi
if [ "$DO_VERIFY" = 1 ]; then
  verify_image
  if [ -n "$PUSH" ]; then
    log "pushing ${VERIFIED_REF#*@} to $PUSH (CI)"
    "$CRANE" copy "$VERIFIED_REF" "$PUSH"
  fi
  stop_registry
  rm -rf "$OUT/verify-registry"
fi
