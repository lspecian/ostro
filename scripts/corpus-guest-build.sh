#!/usr/bin/env bash
# corpus-guest-build.sh: path-neutral wasm32-wasip2 release build for replay-corpus guests.
#
# A guest's panic and Location strings embed the absolute paths of the sources they come
# from (the repository, the cargo registry, the toolchain's rust-src). A module recorded
# into tests/replay-corpus keeps those bytes forever, so every guest that ends up in the
# corpus (record_corpus, export_run from a server it was deployed to, the crash campaign's
# crash-saga) is built with this script. It remaps, via rustc --remap-path-prefix:
#
#   <repository root>                     -> /ostro
#   <cargo home>     ($CARGO_HOME, ~/.cargo)  -> /cargo
#   <rustup home>    ($RUSTUP_HOME, ~/.rustup) -> /rustup
#   <sysroot>/lib/rustlib/src/rust        -> /rustc/<rustc commit-hash>  (rustc's own std remap)
#   <target dir>                          -> /target
#
# so the module bytes do not depend on where the checkout, the target dir or the home
# directory live. After the build every top-level .wasm in <target-dir>/wasm32-wasip2/release
# is checked (see --check below); a leak fails the build.
#
# Usage:
#   scripts/corpus-guest-build.sh <manifest-path> <target-dir> [extra cargo build args...]
#   scripts/corpus-guest-build.sh --check <file.wasm>...
#
# Examples:
#   scripts/corpus-guest-build.sh guests/Cargo.toml target/guests-corpus -p crash-saga
#   scripts/corpus-guest-build.sh examples/order-saga/Cargo.toml target/order-saga-corpus
#
# --check fails (exit 1) when a file contains the home directory, /tmp/, the repository
# root, the cargo or rustup home, or a session path. check-vendored.sh check 4 scans the
# committed corpus for session paths; this check is stricter and runs before a commit.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
CARGO_HOME_DIR="${CARGO_HOME:-$HOME/.cargo}"
RUSTUP_HOME_DIR="${RUSTUP_HOME:-$HOME/.rustup}"

# Split so check-vendored.sh check 4 does not match this file.
P_SESSION="claude""-1000|scratch""pad"

check_files() {
  local bad=0 f pat
  for f in "$@"; do
    local hits=""
    for pat in "$HOME/" "/tmp/" "$ROOT/" "$CARGO_HOME_DIR/" "$RUSTUP_HOME_DIR/"; do
      if grep -qaF -- "$pat" "$f"; then hits+=" $pat"; fi
    done
    if grep -qaE -- "$P_SESSION" "$f"; then hits+=" <session path>"; fi
    if [[ -n "$hits" ]]; then
      printf 'corpus-guest-build: %s embeds local paths:%s\n' "$f" "$hits" >&2
      bad=1
    fi
  done
  return "$bad"
}

if [[ "${1:-}" == "--check" ]]; then
  shift
  [[ $# -gt 0 ]] || { echo "usage: $0 --check <file.wasm>..." >&2; exit 2; }
  check_files "$@"
  exit $?
fi

if [[ $# -lt 2 ]]; then
  echo "usage: $0 <manifest-path> <target-dir> [extra cargo build args...]" >&2
  exit 2
fi
manifest="$1"
target_dir="$2"
shift 2
case "$target_dir" in
  /*) ;;
  *) target_dir="$ROOT/$target_dir" ;;
esac
mkdir -p "$target_dir"
target_dir="$(cd "$target_dir" && pwd -P)"

sysroot="$(rustc --print sysroot)"
commit="$(rustc -vV | sed -n 's/^commit-hash: //p')"
[[ -n "$commit" ]] || { echo "corpus-guest-build: no commit-hash in rustc -vV" >&2; exit 1; }

# Later remaps win when several match (rustc applies the last matching one), so the most
# specific prefixes come last. CARGO_ENCODED_RUSTFLAGS keeps paths with spaces intact.
sep=$'\x1f'
flags="--remap-path-prefix=$CARGO_HOME_DIR=/cargo"
flags+="$sep--remap-path-prefix=$RUSTUP_HOME_DIR=/rustup"
flags+="$sep--remap-path-prefix=$ROOT=/ostro"
flags+="$sep--remap-path-prefix=$target_dir=/target"
flags+="$sep--remap-path-prefix=$sysroot/lib/rustlib/src/rust=/rustc/$commit"

(
  cd "$ROOT"
  unset RUSTFLAGS CARGO_BUILD_RUSTFLAGS CARGO_BUILD_TARGET CARGO_TARGET_DIR RUSTC_WRAPPER
  CARGO_ENCODED_RUSTFLAGS="$flags" "${CARGO:-cargo}" build --release --target wasm32-wasip2 \
    --manifest-path "$manifest" --target-dir "$target_dir" "$@"
)

shopt -s nullglob
wasm=("$target_dir"/wasm32-wasip2/release/*.wasm)
[[ ${#wasm[@]} -gt 0 ]] || { echo "corpus-guest-build: no .wasm in $target_dir/wasm32-wasip2/release" >&2; exit 1; }
check_files "${wasm[@]}"
