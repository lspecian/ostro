#!/usr/bin/env bash
# D-P9-3 step 2: builds componentize-py v0.25.1 from source with the vendored minimal
# determinism patch (sdks/python/toolchain/componentize-py-0.25.1-deterministic.patch).
#
#   build-componentize-py-fork.sh            # build (no-op when the binary is current)
#   build-componentize-py-fork.sh --print    # print the binary path only
#
# Layout (all gitignored, under target/toolchains or $OSTRO_TOOLCHAINS_DIR):
#   componentize-py-src/                     clone of the v0.25.1 tag, patch applied
#   componentize-py-target/                  cargo target dir
#   componentize-py-0.25.1-ostro/bin/componentize-py   installed binary
#   componentize-py-0.25.1-ostro/PATCH_SHA256          sha256 of the patch it was built from
#
# Use the fork with: OSTRO_COMPONENTIZE_PY=<binary> sdks/scripts/build-guest.sh py ...
#
# Network (first build only), official upstreams only: the componentize-py tag from
# github.com/bytecodealliance/componentize-py; build.rs then downloads wasi-sdk 33 from
# github.com/WebAssembly/wasi-sdk releases (unless WASI_SDK_PATH points at one), the CPython
# tarball dicej/cpython v3.14.0-wasi-sdk-30 it references, and zlib 1.3.1 / SQLite 3.51.2
# from their upstream release URLs. Needs make, clang (or gcc) and a Rust toolchain >= 1.94
# with the wasm32-wasip2 target. Cold build: ~4-6 GB disk, 15-30 min on 12 cores.
#
# Rust toolchain: componentize-py has no rust-toolchain file and runs a nested cargo build for
# its wasm runtime with every RUST*/CARGO* variable removed, so a RUSTUP_TOOLCHAIN export
# would not reach it. The script therefore writes a rust-toolchain.toml into the clone
# (channel $OSTRO_CPY_RUST_TOOLCHAIN, default 1.100.0 = the repo's pin, which has
# wasm32-wasip2) and sets RUSTUP_TOOLCHAIN to the same value for the outer build.
set -euo pipefail

die() {
    echo "build-componentize-py-fork.sh: $*" >&2
    exit 2
}

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
TOOLCHAINS="${OSTRO_TOOLCHAINS_DIR:-$ROOT/target/toolchains}"
PATCH="$ROOT/sdks/python/toolchain/componentize-py-0.25.1-deterministic.patch"
TAG="v0.25.1"
TAG_COMMIT="2a53e46a0083619f2c85ac5f20abe4132cda4b88"
REPO_URL="https://github.com/bytecodealliance/componentize-py"
RUST_TC="${OSTRO_CPY_RUST_TOOLCHAIN:-1.100.0}"

SRC="$TOOLCHAINS/componentize-py-src"
TGT="$TOOLCHAINS/componentize-py-target"
INSTALL="$TOOLCHAINS/componentize-py-0.25.1-ostro"
BIN="$INSTALL/bin/componentize-py"

if [ "${1:-}" = "--print" ]; then
    echo "$BIN"
    exit 0
fi

[ -f "$PATCH" ] || die "patch not found: $PATCH"
PATCH_SHA="$(sha256sum "$PATCH" | cut -d' ' -f1)"

if [ -x "$BIN" ] && [ "$(cat "$INSTALL/PATCH_SHA256" 2>/dev/null)" = "$PATCH_SHA" ]; then
    echo "componentize-py fork up to date: $BIN (patch $PATCH_SHA)"
    exit 0
fi

command -v git >/dev/null || die "git not found"
command -v make >/dev/null || die "make not found"
command -v rustup >/dev/null || die "rustup not found"
rustup target list --installed --toolchain "$RUST_TC" 2>/dev/null | grep -qx wasm32-wasip2 ||
    die "Rust toolchain $RUST_TC with target wasm32-wasip2 required (rustup target add wasm32-wasip2 --toolchain $RUST_TC)"

mkdir -p "$TOOLCHAINS"
if [ ! -d "$SRC/.git" ]; then
    git clone -q --depth 1 --branch "$TAG" "$REPO_URL" "$SRC"
fi
got="$(git -C "$SRC" rev-parse HEAD)"
[ "$got" = "$TAG_COMMIT" ] || die "$SRC is at $got, expected $TAG $TAG_COMMIT"

# Reset tracked sources to the tag (keeps the untracked cpython/ build cache), then patch.
git -C "$SRC" checkout -q -- .
rm -f "$SRC/src/deterministic.rs"
git -C "$SRC" apply --whitespace=nowarn "$PATCH"
printf '[toolchain]\nchannel = "%s"\ntargets = ["wasm32-wasip2"]\n' "$RUST_TC" >"$SRC/rust-toolchain.toml"

echo "building componentize-py $TAG + $(basename "$PATCH") with Rust $RUST_TC" >&2
(cd "$SRC" && RUSTUP_TOOLCHAIN="$RUST_TC" CARGO_TARGET_DIR="$TGT" \
    cargo build --release --locked --bin componentize-py)

mkdir -p "$INSTALL/bin"
cp "$TGT/release/componentize-py" "$BIN.tmp"
mv "$BIN.tmp" "$BIN"
echo "$PATCH_SHA" >"$INSTALL/PATCH_SHA256"
echo "installed $BIN sha256=$(sha256sum "$BIN" | cut -d' ' -f1) (patch $PATCH_SHA, rust $RUST_TC)"
