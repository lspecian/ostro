#!/usr/bin/env bash
# Pinned polyglot guest build (Phase 9, plan 09-02; D-P9-1, D-P9-3 step 1, D-P9-28).
#
#   build-guest.sh <py|ts|ts-sm> <guest_dir> <out.wasm>
#
#   py     componentize-py 0.25.1 --stub-wasi; entry module app.py; sdks/python/ostro is
#          staged next to it when present.
#   ts     jco 1.42.0, QuickJS backend (sync export, WASI stubbed); entry index.ts;
#          sdks/ts is staged as node_modules/ostro when present.
#   ts-sm  jco 1.42.0 + componentize-js 0.23.0, StarlingMonkey backend, --disable all.
#
# Deterministic wrapper (D-P9-3 step 1): sources are staged into a fresh directory with
# every mtime set to SOURCE_DATE_EPOCH (default: commit time of HEAD), and the build runs
# with PYTHONHASHSEED=0 PYTHONDONTWRITEBYTECODE=1 TZ=UTC LC_ALL=C SOURCE_DATE_EPOCH.
# The WIT is the repo's frozen wit/ostro-runtime-0.1.0 (the SDK copies are byte-identical).
# The npm toolchain is pinned in sdks/scripts/toolchain (package-lock.json).
#
# Prints: built <out> sha256=<hex> bytes=<n>
set -euo pipefail

die() {
    echo "build-guest.sh: $*" >&2
    exit 2
}

[ "$#" -eq 3 ] || die "usage: build-guest.sh <py|ts|ts-sm> <guest_dir> <out.wasm>"
MODE="$1"
GUEST_DIR="$(cd "$2" 2>/dev/null && pwd)" || die "guest dir not found: $2"
OUT_ARG="$3"
mkdir -p "$(dirname "$OUT_ARG")"
OUT="$(cd "$(dirname "$OUT_ARG")" && pwd)/$(basename "$OUT_ARG")"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
WIT="$ROOT/wit/ostro-runtime-0.1.0"
TOOLCHAIN="$HERE/toolchain"
BUNDLE_CONFIG="$TOOLCHAIN/rolldown.ostro.mjs"

UV_VERSION="0.7.15"
NODE_MAJOR="24"

require_uv() {
    command -v uv >/dev/null 2>&1 || die "uv not found (need uv $UV_VERSION, D-P9-28)"
    local v
    v="$(uv --version | awk '{print $2}')"
    [ "$v" = "$UV_VERSION" ] || die "uv $v found, need exactly $UV_VERSION (D-P9-28)"
}

require_node() {
    command -v node >/dev/null 2>&1 || die "node not found (need Node $NODE_MAJOR)"
    command -v npm >/dev/null 2>&1 || die "npm not found (need Node $NODE_MAJOR)"
    local v
    v="$(node --version)"
    case "$v" in
        "v$NODE_MAJOR".*) ;;
        *) die "node $v found, need v$NODE_MAJOR.x" ;;
    esac
}

# Installs the pinned npm toolchain when node_modules is missing or older than the lock.
ensure_toolchain() {
    local stamp="$TOOLCHAIN/node_modules/.package-lock.json"
    if [ ! -f "$stamp" ] || [ "$TOOLCHAIN/package-lock.json" -nt "$stamp" ]; then
        (cd "$TOOLCHAIN" && npm ci --silent --no-audit --no-fund) >&2
    fi
    [ -x "$TOOLCHAIN/node_modules/.bin/jco" ] || die "jco missing after npm ci in $TOOLCHAIN"
}

# Deterministic environment (D-P9-3 step 1).
SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH:-$(git -C "$ROOT" log -1 --format=%ct)}"
export PYTHONHASHSEED=0 PYTHONDONTWRITEBYTECODE=1 TZ=UTC LC_ALL=C SOURCE_DATE_EPOCH

STAGE="$(mktemp -d "${TMPDIR:-/tmp}/ostro-guest.XXXXXXXX")"
trap 'rm -rf "$STAGE"' EXIT

# Copies a source tree into the stage, without caches and installed dependencies.
stage_copy() {
    local src="$1" dst="$2"
    mkdir -p "$dst"
    (cd "$src" && find . -type f \
        -not -path '*/node_modules/*' -not -path '*/__pycache__/*' \
        -not -path '*/.venv/*' -not -name '*.pyc' -print0) |
        while IFS= read -r -d '' f; do
            mkdir -p "$dst/$(dirname "$f")"
            cp "$src/$f" "$dst/$f"
        done
}

pin_mtimes() {
    find "$STAGE" -exec touch -h -d "@$SOURCE_DATE_EPOCH" {} +
}

build_py() {
    require_uv
    stage_copy "$GUEST_DIR" "$STAGE/src"
    if [ -d "$ROOT/sdks/python/ostro" ]; then
        stage_copy "$ROOT/sdks/python/ostro" "$STAGE/src/ostro"
    fi
    [ -f "$STAGE/src/app.py" ] || die "$GUEST_DIR has no app.py"
    pin_mtimes
    (cd "$STAGE/src" && uv tool run --from componentize-py==0.25.1 componentize-py \
        -d "$WIT" -w ostro:runtime/workflow@0.1.0 \
        componentize --stub-wasi -p "$STAGE/src" app -o "$OUT")
}

stage_ts() {
    require_node
    ensure_toolchain
    stage_copy "$GUEST_DIR" "$STAGE/src"
    if [ -f "$ROOT/sdks/ts/package.json" ]; then
        stage_copy "$ROOT/sdks/ts" "$STAGE/src/node_modules/ostro"
    fi
    [ -f "$STAGE/src/index.ts" ] || die "$GUEST_DIR has no index.ts"
    pin_mtimes
}

build_ts() {
    stage_ts
    (cd "$STAGE/src" && "$TOOLCHAIN/node_modules/.bin/jco" componentize "$STAGE/src/index.ts" \
        --wit "$WIT" --world-name workflow \
        --backend quickjs --backend-qjs-disable-async --backend-qjs-stub-wasi \
        --bundle-config "$BUNDLE_CONFIG" -o "$OUT")
}

build_ts_sm() {
    stage_ts
    (cd "$STAGE/src" && "$TOOLCHAIN/node_modules/.bin/jco" componentize "$STAGE/src/index.ts" \
        --wit "$WIT" --world-name workflow \
        --disable all \
        --bundle-config "$BUNDLE_CONFIG" -o "$OUT")
}

case "$MODE" in
    py) build_py ;;
    ts) build_ts ;;
    ts-sm) build_ts_sm ;;
    *) die "unknown mode '$MODE' (py|ts|ts-sm)" ;;
esac

[ -s "$OUT" ] || die "no output written to $OUT"
echo "built $OUT sha256=$(sha256sum "$OUT" | cut -d' ' -f1) bytes=$(stat -c %s "$OUT")"
