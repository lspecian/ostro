#!/usr/bin/env bash
# Reproducible-build check (plan 09-17, D-P9-3): builds each conformance guest 3 times from
# the same checkout, at different times and with fresh caches, and compares sha256.
#
#   repro-check.sh [rust|py|ts|all]...      (default: all = rust py ts)
#
#   rust  guests/conf-rust, cargo --release --target wasm32-wasip2, a fresh
#         --target-dir target/repro/rust-<i> per build. Must be identical (hard failure).
#   py    conformance/guests/python through build-guest.sh py (released componentize-py
#         0.25.1, or the D-P9-3 fork when OSTRO_COMPONENTIZE_PY points at it).
#   ts    conformance/guests/ts through build-guest.sh ts (jco 1.42.0, QuickJS).
#
# Each polyglot build gets a new empty OSTRO_POLYGLOT_CACHE and its own output file; builds
# are >= REPRO_GAP seconds apart (default 2). Prints sha256 + size per build and
# `REPRO <lang> OK` or `REPRO <lang> DIFF <bytes differing>`.
#
# Exit status: non-zero when Rust differs, or when py/ts differ and docs/sdk-builds.md has no
# `RESIDUAL-ACCEPTED: <lang>` line (an accepted, documented residual per D-P9-3 step 3).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
OUT="${REPRO_OUT:-$ROOT/target/repro}"
GAP="${REPRO_GAP:-2}"
DOC="$ROOT/docs/sdk-builds.md"
N=3

langs=("$@")
[ "${#langs[@]}" -gt 0 ] || langs=(all)
if [ "${langs[*]}" = "all" ]; then
    langs=(rust py ts)
fi
for l in "${langs[@]}"; do
    case "$l" in
        rust | py | ts) ;;
        *)
            echo "repro-check.sh: unknown language '$l' (rust|py|ts|all)" >&2
            exit 2
            ;;
    esac
done

mkdir -p "$OUT"

build_rust() {
    local i="$1" dst="$2" td="$OUT/rust-$i"
    rm -rf "$td"
    (cd "$ROOT" && env -u RUSTFLAGS -u CARGO_ENCODED_RUSTFLAGS -u CARGO_BUILD_TARGET \
        -u CARGO_TARGET_DIR -u RUSTC_WRAPPER \
        cargo build --quiet --release --locked --target wasm32-wasip2 \
        --manifest-path guests/Cargo.toml --target-dir "$td" -p conf-rust)
    cp "$td/wasm32-wasip2/release/conf_rust.wasm" "$dst"
    rm -rf "$td"
}

build_poly() {
    local mode="$1" dir="$2" dst="$3" cache
    cache="$(mktemp -d "${TMPDIR:-/tmp}/ostro-repro-cache.XXXXXXXX")"
    OSTRO_POLYGLOT_CACHE="$cache" bash "$HERE/build-guest.sh" "$mode" "$ROOT/$dir" "$dst" >/dev/null
    rm -rf "$cache"
}

# Differing bytes between two files: positional count when lengths match, else a note.
diff_bytes() {
    local a="$1" b="$2" la lb
    la="$(stat -c %s "$a")"
    lb="$(stat -c %s "$b")"
    if [ "$la" = "$lb" ]; then
        # cmp exits 1 when the files differ; that is the expected case here.
        { cmp -l "$a" "$b" || [ "$?" -eq 1 ]; } | wc -l | tr -d ' '
    else
        echo "length-${la}-vs-${lb}"
    fi
}

status=0
for lang in "${langs[@]}"; do
    shas=()
    files=()
    for i in $(seq 1 "$N"); do
        [ "$i" -eq 1 ] || sleep "$GAP"
        f="$OUT/$lang-$i.wasm"
        rm -f "$f"
        case "$lang" in
            rust) build_rust "$i" "$f" ;;
            py) build_poly py conformance/guests/python "$f" ;;
            ts) build_poly ts conformance/guests/ts "$f" ;;
        esac
        sha="$(sha256sum "$f" | cut -d' ' -f1)"
        echo "build $lang-$i sha256=$sha bytes=$(stat -c %s "$f")"
        shas+=("$sha")
        files+=("$f")
    done
    worst=0
    for i in 1 2; do
        if [ "${shas[$i]}" != "${shas[0]}" ]; then
            d="$(diff_bytes "${files[0]}" "${files[$i]}")"
            echo "  $lang-1 vs $lang-$((i + 1)): $d bytes differ"
            worst="$d"
        fi
    done
    if [ "$worst" = "0" ]; then
        echo "REPRO $lang OK"
    else
        echo "REPRO $lang DIFF $worst"
        if [ "$lang" = "rust" ]; then
            echo "repro-check.sh: Rust guests must be bit-reproducible (D-P9-3)" >&2
            status=1
        elif grep -q "^RESIDUAL-ACCEPTED: $lang\b" "$DOC" 2>/dev/null; then
            echo "  accepted residual for $lang (docs/sdk-builds.md, D-P9-3 step 3)"
        else
            echo "repro-check.sh: $lang differs and docs/sdk-builds.md records no RESIDUAL-ACCEPTED: $lang" >&2
            status=1
        fi
    fi
done
exit "$status"
