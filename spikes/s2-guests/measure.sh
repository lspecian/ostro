#!/usr/bin/env bash
# Spike S2 measurement protocol (plan 01-05).
#
#   measure.sh <env|pool|perf|probes|repro|all>
#
# Drives build.sh and spikes/s2-harness (one process per measurement) and
# appends compact JSON lines to docs/spikes/data/S2/. Every latency line is
# tagged with guest, allocator, kind and /proc/loadavg at measurement time.
#
# Environment knobs:
#   N_INST   instantiate iterations   (default 1000)
#   N_MEM    live instances for memory (default 50)
#   N_COLD   cold-start processes      (default 20)
#   GUESTS   guests to measure         (default "py ts rust")
#   RUN_TAG  free-form tag added to perf lines (e.g. "rerun-low-load")
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
OUT="$ROOT/spikes/out"
DATA="$ROOT/docs/spikes/data/S2"
HARNESS="$ROOT/spikes/s2-harness/target/release/s2-harness"
N_INST="${N_INST:-1000}"
N_MEM="${N_MEM:-50}"
N_COLD="${N_COLD:-20}"
GUESTS="${GUESTS:-py ts rust}"
RUN_TAG="${RUN_TAG:-}"
PHASE="${1:-all}"

mkdir -p "$DATA" "$OUT"
cd "$ROOT"

if [[ ! -x "$HARNESS" ]]; then
    (cd "$ROOT/spikes/s2-harness" && cargo +1.100.0 build --release)
fi

loadavg() { cut -d' ' -f1-3 /proc/loadavg; }

# tag <file> <guest> <allocator> <kind> [extra-json] : read one JSON line on
# stdin, add tags, append compact JSON to <file>. The harness's own
# allocator object (strategy + pooling overrides) is kept as allocator_config.
tag() {
    local file="$1" guest="$2" alloc="$3" kind="$4" extra="${5:-}"
    [[ -n "$extra" ]] || extra='{}'
    jq -c --arg g "$guest" --arg a "$alloc" --arg k "$kind" \
        --arg l "$(loadavg)" --arg t "$RUN_TAG" --argjson x "$extra" \
        '(if (.allocator|type) == "object" then {allocator_config:.allocator} else {} end) as $c | . + $c + {guest:$g, allocator:$a, kind:$k, loadavg:$l} + (if $t == "" then {} else {run_tag:$t} end) + $x' \
        >>"$file"
}

# Force a from-scratch Rust guest build (otherwise cargo reuses the artefact
# and "independent builds" are byte copies).
clean_rust() {
    (cd "$HERE/rust" && cargo +1.100.0 clean --release --target wasm32-wasip2 -q)
}

ensure_wasm() {
    local g="$1"
    [[ -f "$OUT/$g.wasm" ]] || bash "$HERE/build.sh" "$g"
}

phase_env() {
    local f="$DATA/env.txt"
    {
        echo "# S2 environment ($(date -u +%Y-%m-%dT%H:%M:%SZ))"
        echo "## uname"; uname -a
        echo "## lscpu"; lscpu | head -20
        echo "## nproc"; nproc
        echo "## free -m"; free -m
        echo "## loadavg"; cat /proc/loadavg
        echo "## toolchains"
        echo "rustc: $(rustc +1.100.0 --version 2>&1)"
        echo "componentize-py: $(cd "$HERE/python" && "${UV:-$(command -v uv || echo "$HOME/.local/bin/uv")}" tool run --from componentize-py==0.25.1 componentize-py --version 2>&1 | tail -1)"
        echo "jco: $(cd "$HERE/ts" && npx --no-install jco --version 2>&1 | tail -1)"
        echo "componentize-js: $(cd "$HERE/ts" && jq -r .version node_modules/@bytecodealliance/componentize-js/package.json 2>&1)"
        echo "node: $(node --version 2>&1)"
        echo "wit-bindgen: $(grep -A2 'name = "wit-bindgen"' "$HERE/rust/Cargo.lock" | grep version | head -1 | tr -d ' ')"
        echo "wasmtime: $(grep -A2 'name = "wasmtime"$' "$ROOT/spikes/s2-harness/Cargo.lock" | grep version | head -1 | tr -d ' ')"
        echo "/usr/bin/time: $([[ -x /usr/bin/time ]] && echo present || echo absent)"
    } >"$f"
    echo "wrote $f"
}

phase_perf() {
    local f="$DATA/perf.jsonl"
    local g a flag i rss line t0 t1
    for g in $GUESTS; do
        ensure_wasm "$g"
        # compile: its own process; peak RSS via /usr/bin/time when present.
        if [[ -x /usr/bin/time ]]; then
            line="$(/usr/bin/time -f 'TIME_MAXRSS_KB=%M' -o "$OUT/.time.$g" \
                "$HARNESS" compile "$OUT/$g.wasm" "$OUT/$g.cwasm")"
            rss="$(sed -n 's/^TIME_MAXRSS_KB=//p' "$OUT/.time.$g")"
            rm -f "$OUT/.time.$g"
            echo "$line" | tag "$f" "$g" "n/a" compile "{\"time_maxrss_kb\":$rss}"
        else
            "$HARNESS" compile "$OUT/$g.wasm" "$OUT/$g.cwasm" | tag "$f" "$g" "n/a" compile
        fi
        for a in on-demand pooling; do
            flag=""
            if [[ "$a" == pooling ]]; then
                flag="--pooling"
                # The Python component instantiates 38 core instances (see
                # pool-limits.jsonl); the wasmtime default of 1000 total core
                # instances only fits 26 live Python instances, so size the
                # pool for N_MEM live instances. ts/rust fit the defaults.
                [[ "$g" == py ]] && flag="$flag --pool-total-core-instances $((N_MEM * 38))"
            fi
            # shellcheck disable=SC2086
            "$HARNESS" instantiate $flag --n "$N_INST" "$OUT/$g.cwasm" | tag "$f" "$g" "$a" instantiate
            # shellcheck disable=SC2086
            "$HARNESS" memory $flag --n "$N_MEM" "$OUT/$g.cwasm" | tag "$f" "$g" "$a" memory
            for ((i = 1; i <= N_COLD; i++)); do
                t0="$(date +%s%N)"
                # shellcheck disable=SC2086
                line="$("$HARNESS" cold $flag "$OUT/$g.cwasm")"
                t1="$(date +%s%N)"
                echo "$line" | tag "$f" "$g" "$a" cold \
                    "{\"iter\":$i,\"process_wall_ms\":$(awk "BEGIN{print ($t1-$t0)/1e6}")}"
            done
        done
        echo "perf done: $g"
    done
}

# Smallest pooling limits per guest: probe one live instance against
# shrinking totals. Input for Phase 3 ENG-08 pool sizing.
phase_pool() {
    local f="$DATA/pool-limits.jsonl" g knob v found declared
    : >"$f"
    for g in py ts rust; do
        ensure_wasm "$g"
        [[ -f "$OUT/$g.cwasm" ]] || "$HARNESS" compile "$OUT/$g.wasm" "$OUT/$g.cwasm" >/dev/null
        for knob in total-core-instances total-memories total-tables; do
            found=null
            for v in $(seq 1 64); do
                if "$HARNESS" memory --pooling --n 1 "--pool-$knob" "$v" "$OUT/$g.cwasm" >/dev/null 2>&1; then
                    found="$v"; break
                fi
            done
            jq -cn --arg g "$g" --arg k "$knob" --argjson v "$found" \
                '{guest:$g, kind:"pool-limit", knob:$k, min_per_component_instance:$v}' >>"$f"
        done
        declared="$("$HARNESS" memory --n 1 "$OUT/$g.cwasm" | jq .declared_linear_memory_max_bytes)"
        for v in "$declared" "$((declared - 65536))"; do
            if "$HARNESS" memory --pooling --n 1 --pool-max-memory-size "$v" "$OUT/$g.cwasm" >/dev/null 2>&1; then
                found=true; else found=false; fi
            jq -cn --arg g "$g" --argjson v "$v" --argjson ok "$found" \
                '{guest:$g, kind:"pool-limit", knob:"max-memory-size", bytes:$v, fits:$ok}' >>"$f"
        done
    done
    echo "wrote $f"
}

phase_probes() {
    local fi="$DATA/imports.jsonl" fp="$DATA/probes.jsonl" g
    : >"$fi"; : >"$fp"
    for g in py ts rust; do
        ensure_wasm "$g"
        "$HARNESS" imports "$OUT/$g.wasm" | tag "$fi" "$g" "n/a" imports
        [[ -f "$OUT/$g.cwasm" ]] || "$HARNESS" compile "$OUT/$g.wasm" "$OUT/$g.cwasm" >/dev/null
        "$HARNESS" probes "$OUT/$g.cwasm" | tag "$fp" "$g" "on-demand" probes '{"build":"a"}'
    done
    # Second, independent build of each guest: answers "random across builds".
    for g in py ts rust; do
        [[ "$g" == rust ]] && clean_rust
        bash "$HERE/build.sh" "$g" -b
        "$HARNESS" compile "$OUT/$g-b.wasm" "$OUT/$g-b.cwasm" >/dev/null
        "$HARNESS" probes "$OUT/$g-b.cwasm" | tag "$fp" "$g" "on-demand" probes '{"build":"b"}'
    done
}

phase_repro() {
    local f="$DATA/repro.txt" g s base
    {
        echo "# S2 build reproducibility ($(date -u +%Y-%m-%dT%H:%M:%SZ))"
        echo "# build.sh <lang> <suffix>; sha256 and bytes per build"
    } >"$f"
    for g in py ts rust; do
        echo "## $g (PYTHONHASHSEED unset)" >>"$f"
        for s in r1 r2 r3; do
            [[ "$g" == rust ]] && clean_rust
            env -u PYTHONHASHSEED bash "$HERE/build.sh" "$g" "-$s" | grep '^built' >>"$f"
        done
    done
    echo "## py (PYTHONHASHSEED=0)" >>"$f"
    for s in h1 h2 h3; do
        PYTHONHASHSEED=0 bash "$HERE/build.sh" py "-$s" | grep '^built' >>"$f"
    done
    echo "## first differing bytes (cmp -l | head -20), offset is 1-based decimal, bytes octal" >>"$f"
    for set in "py:r1 r2 r3" "ts:r1 r2 r3" "rust:r1 r2 r3" "py:h1 h2 h3"; do
        g="${set%%:*}"; base=""
        for s in ${set#*:}; do
            if [[ -z "$base" ]]; then base="$s"; continue; fi
            if cmp -s "$OUT/$g-$base.wasm" "$OUT/$g-$s.wasm"; then
                echo "$g-$base vs $g-$s: identical" >>"$f"
            else
                echo "$g-$base vs $g-$s: differ ($(cmp -l "$OUT/$g-$base.wasm" "$OUT/$g-$s.wasm" | wc -l) bytes)" >>"$f"
                cmp -l "$OUT/$g-$base.wasm" "$OUT/$g-$s.wasm" | head -20 | sed 's/^/  /' >>"$f" || true
                echo "  section attribution (wasm_sections.py, first 8 paths):" >>"$f"
                python3 -I "$HERE/wasm_sections.py" "$OUT/$g-$base.wasm" "$OUT/$g-$s.wasm" \
                    | head -9 | sed 's/^/    /' >>"$f" || true
            fi
        done
    done
    echo "wrote $f"
}

case "$PHASE" in
    env) phase_env ;;
    perf) phase_env; phase_pool; phase_perf ;;
    pool) phase_pool ;;
    probes) phase_probes ;;
    repro) phase_repro ;;
    all) phase_env; phase_pool; phase_perf; phase_probes; phase_repro ;;
    *) echo "usage: $0 <env|pool|perf|probes|repro|all>" >&2; exit 2 ;;
esac
