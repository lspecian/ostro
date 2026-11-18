#!/usr/bin/env bash
# Spike S2 guest build entry point.
#
#   build.sh <py|ts|rust|all> [suffix]
#
# Writes spikes/out/{py,ts,rust}<suffix>.wasm and prints sha256 + byte size.
# Pinned toolchains: componentize-py 0.25.1 (--stub-wasi), jco 1.42.0 /
# componentize-js 0.23.0 (--disable all), wit-bindgen 0.63 on wasm32-wasip2
# with Rust 1.100.0. PYTHONHASHSEED, if set by the caller, passes through.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WIT="$HERE/../wit"
OUT="$HERE/../out"
LANG_SEL="${1:-all}"
SUFFIX="${2:-}"
UV="${UV:-$(command -v uv || echo "$HOME/.local/bin/uv")}"
RUST_TOOLCHAIN="${RUST_TOOLCHAIN:-1.100.0}"

mkdir -p "$OUT"

report() {
    local f="$1"
    local sha size
    sha="$(sha256sum "$f" | cut -d' ' -f1)"
    size="$(stat -c %s "$f")"
    echo "built $(basename "$f") sha256=$sha bytes=$size"
}

build_py() {
    local out="$OUT/py$SUFFIX.wasm"
    (cd "$HERE/python" && \
        "$UV" tool run --from componentize-py==0.25.1 componentize-py \
            -d "$WIT" -w spike componentize --stub-wasi -p . app -o "$out")
    report "$out"
}

build_ts() {
    local out="$OUT/ts$SUFFIX.wasm"
    (cd "$HERE/ts" && npm ci --silent --no-audit --no-fund && \
        npx --no-install jco componentize src/index.ts --wit "$WIT" \
            --world-name spike --disable all -o "$out")
    report "$out"
}

build_rust() {
    local out="$OUT/rust$SUFFIX.wasm"
    (cd "$HERE/rust" && \
        cargo "+$RUST_TOOLCHAIN" build --release --target wasm32-wasip2)
    cp "$HERE/rust/target/wasm32-wasip2/release/s2_guest_rust.wasm" "$out"
    report "$out"
}

case "$LANG_SEL" in
    py) build_py ;;
    ts) build_ts ;;
    rust) build_rust ;;
    all) build_py; build_ts; build_rust ;;
    *) echo "usage: $0 <py|ts|rust|all> [suffix]" >&2; exit 2 ;;
esac
