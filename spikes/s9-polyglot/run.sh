#!/usr/bin/env bash
# Spike S9 (plan 09-04): polyglot toolchain measurements and behaviour probes.
#
#   run.sh <build|measure|probes|repro|all>
#
#   build    builds py-asyncio, py-base, py-loop, ts-qjs, ts-sm into out/
#   measure  inspect / pool(n=50) / instantiate(200) / aot / fuel per guest (Task 1)
#   probes   lowering, export naming, result mapping, surrogates, TS SdkPromise on the
#            real engine (QJS + SM), Python OstroLoop in the embedded CPython (Task 2)
#   repro    3 wrapper + 3 plain builds of py-base and ts-qjs, byte diffs (Task 3)
#
# Every measurement appends JSON lines to docs/spikes/data/S9/measurements.jsonl
# (S9_FRESH=1 truncates it first); repro writes docs/spikes/data/S9/repro.txt.
# sdks/scripts/build-guest.sh (09-02) did not exist when this ran, so the pinned build
# commands are inline here: componentize-py 0.25.1 (--stub-wasi) via uv, jco 1.42.0 with
# the bundled componentize-qjs 0.5.0 / componentize-js 0.23.0 (ts/package.json).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
WIT="$ROOT/wit/ostro-runtime-0.1.0"
OUT="$HERE/out"
DATA="$ROOT/docs/spikes/data/S9"
JSONL="$DATA/measurements.jsonl"
UV="${UV:-$(command -v uv || echo "$HOME/.local/bin/uv")}"
CPY=("$UV" tool run --from componentize-py==0.25.1 componentize-py)
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$ROOT/target/s9-harness}"
H="$CARGO_TARGET_DIR/release/s9-harness"

mkdir -p "$OUT" "$DATA"

log() { printf '== %s\n' "$*" >&2; }

harness() {
    if [ ! -f "$HERE/harness/Cargo.lock" ]; then
        # Never committed: seed from the workspace lock so shared crates match it.
        cp "$ROOT/Cargo.lock" "$HERE/harness/Cargo.lock"
    fi
    (cd "$HERE/harness" && cargo build --release --quiet)
}

ts_deps() {
    (cd "$HERE/ts" && npm ci --silent --no-audit --no-fund)
}

# Rolldown config merged by jco: es2016 lowering, ostro:* external, and a renderChunk
# hook that dumps the final bundle to $S9_DUMP (lowering evidence).
write_bundle_config() {
    cat > "$OUT/rolldown.ostro.mjs" <<'EOF'
import { writeFileSync } from "node:fs";
export default {
  transform: { target: "es2016" },
  external: [/^ostro:/],
  plugins: [{
    name: "s9-dump",
    renderChunk(code) {
      if (process.env.S9_DUMP) writeFileSync(process.env.S9_DUMP, code);
      return null;
    },
  }],
};
EOF
}

# build_py <out.wasm> <variant: asyncio|base|loop> [stage-dir]
build_py() {
    local out="$1" variant="$2" stage="${3:-}"
    if [ -z "$stage" ]; then stage="$(mktemp -d "$OUT/stage-py.XXXXXX")"; fi
    case "$variant" in
        asyncio) cp "$HERE/py/app_asyncio.py" "$stage/app.py" ;;
        base) sed '/^# --- SDK-IMPORTS BEGIN ---$/,/^# --- SDK-IMPORTS END ---$/d' \
                "$HERE/py/app_asyncio.py" > "$stage/app.py" ;;
        loop) cp "$HERE/py/app_asyncio.py" "$stage/app.py"
              cp "$HERE/py/loop_proto.py" "$stage/loop_proto.py" ;;
    esac
    if [ -n "${S9_MTIME:-}" ]; then
        find "$stage" -exec touch -h -d "@$S9_MTIME" {} +
    fi
    (cd "$stage" && "${CPY[@]}" -d "$WIT" -w ostro:runtime/workflow@0.1.0 \
        componentize --stub-wasi -p . app -o "$out" >/dev/null)
}

# build_ts <entry.ts> <qjs|sm> <out.wasm> [dump.js]
build_ts() {
    local entry="$1" backend="$2" out="$3" dump="${4:-}"
    local flags
    if [ "$backend" = qjs ]; then
        flags=(--backend quickjs --backend-qjs-disable-async --backend-qjs-stub-wasi)
    else
        flags=(--disable all)
    fi
    (cd "$HERE/ts" && S9_DUMP="$dump" npx --no-install jco componentize "$entry" \
        --wit "$WIT" --world-name workflow "${flags[@]}" \
        --bundle-config "$OUT/rolldown.ostro.mjs" -o "$out")
}

report() {
    local f="$1"
    echo "built $(basename "$f") sha256=$(sha256sum "$f" | cut -d' ' -f1) bytes=$(stat -c %s "$f")"
}

do_build() {
    write_bundle_config
    ts_deps
    log "python guests"
    build_py "$OUT/py-asyncio.wasm" asyncio; report "$OUT/py-asyncio.wasm"
    build_py "$OUT/py-base.wasm" base; report "$OUT/py-base.wasm"
    build_py "$OUT/py-loop.wasm" loop; report "$OUT/py-loop.wasm"
    log "ts guests"
    build_ts "$HERE/ts/probe.ts" qjs "$OUT/ts-qjs.wasm" "$OUT/ts-qjs.bundle.js" >/dev/null
    report "$OUT/ts-qjs.wasm"
    build_ts "$HERE/ts/probe.ts" sm "$OUT/ts-sm.wasm" "$OUT/ts-sm.bundle.js" >/dev/null
    report "$OUT/ts-sm.wasm"
}

backend_of() {
    case "$1" in
        py-*) echo componentize-py ;;
        ts-qjs | lowering-*) echo quickjs ;;
        ts-sm) echo starlingmonkey ;;
    esac
}

do_measure() {
    harness
    for g in py-asyncio py-base ts-qjs ts-sm; do
        local w="$OUT/$g.wasm" b
        b="$(backend_of "$g")"
        log "measure $g ($b)"
        "$H" inspect "$w" --guest "$g" --backend "$b" --out "$JSONL" >/dev/null
        "$H" pool "$w" --n 50 --guest "$g" --backend "$b" --out "$JSONL" >/dev/null
        "$H" instantiate "$w" --iters 200 --guest "$g" --backend "$b" --out "$JSONL" >/dev/null
        "$H" aot "$w" --guest "$g" --backend "$b" --out "$JSONL" >/dev/null
        "$H" fuel "$w" --workflow hello --guest "$g" --backend "$b" --out "$JSONL" >/dev/null
    done
    summary
}

summary() {
    python3 -I - "$JSONL" <<'EOF'
import json, sys
rows = {}
for line in open(sys.argv[1]):
    d = json.loads(line)
    if d.get("probe"):
        continue
    rows.setdefault(d["guest"], {})[d["cmd"]] = d
cols = ["guest", "wasm MB", "core inst (static/pool)", "mem floor MB", "pool max_mem MB",
        "RSS/inst KB", "inst p50 us", "describe p50 us", "AOT s", "cwasm MB", "hello fuel", "load"]
print(" | ".join(cols))
for g in ["py-asyncio", "py-base", "ts-qjs", "ts-sm"]:
    r = rows.get(g)
    if not r or not all(k in r for k in ("inspect", "pool", "instantiate", "aot", "fuel")):
        continue
    i, p, n, a, f = (r[k] for k in ("inspect", "pool", "instantiate", "aot", "fuel"))
    print(" | ".join(str(x) for x in [
        g, round(i["wasm_bytes"] / 1e6, 2),
        f'{i["core_instances"]}/{p["min_core_instances_per_component"]}',
        round(i["declared_memory_floor_bytes"] / 2**20, 2),
        round((p["min_max_memory_size_bytes"] or 0) / 2**20, 2),
        round(p["rss_delta_per_instance_bytes"] / 1024),
        n["instantiate"]["p50_us"], n["first_call_describe"]["p50_us"],
        round(a["compile_ms"] / 1e3, 2), round(a["cwasm_bytes"] / 1e6, 1),
        f["runs"][0]["first"]["fuel_used"], n["loadavg"][0],
    ]))
EOF
}

note() { "$H" note --guest "$1" --backend "$2" --probe "$3" --out "$JSONL" "${@:4}" >/dev/null; }

# Leftover async constructs in a dumped bundle: `async function`, `async (`/`async x =>`,
# `await` as a keyword (strings and identifiers like `isAsync` are ignored by \b + space).
leftover_async() {
    grep -nE '(^|[^."A-Za-z0-9_])(async[[:space:]]+(function|\(|[A-Za-z_$][A-Za-z0-9_$]*[[:space:]]*=>)|await[[:space:]])' "$1" \
        || test $? = 1
}

do_probes() {
    harness
    write_bundle_config
    ts_deps
    local wf=(hello s9-probe)
    local res=(res-ret-plain res-ret-ok-tagged res-ret-err-tagged res-throw-failure res-throw-err-tagged res-throw-error)
    local sur=(surrogate-check surrogate-step surrogate-output env)

    log "lowering (question 9)"
    local v
    for v in notla tla; do
        local d="$OUT/lowering-$v"
        rm -rf "$d"; mkdir -p "$d"
        cp "$HERE/ts/probe.ts" "$HERE/ts/lowering_fixture.ts" "$d/"
        if [ "$v" = tla ]; then
            printf 'export const depValue: string = await Promise.resolve("dep-tla");\n' > "$d/dep.ts"
        else
            printf 'export const depValue: string = "dep-plain";\n' > "$d/dep.ts"
        fi
        for b in qjs sm; do
            local rc=0
            build_ts "$d/lowering_fixture.ts" "$b" "$OUT/lowering-$v-$b.wasm" "$d/bundle-$b.js" \
                > "$d/build-$b.log" 2>&1 || rc=$?
            local left warn
            left="$(leftover_async "$d/bundle-$b.js" | head -5 | python3 -I -c 'import json,sys; print(json.dumps(sys.stdin.read().splitlines()))')"
            warn="$(grep -oE '\[[A-Z_]+\][^│]*' "$d/build-$b.log" | sort -u | head -5 | python3 -I -c 'import json,sys; print(json.dumps(sys.stdin.read().splitlines()))')"
            note "lowering-$v" "$(if [ $b = qjs ]; then echo quickjs; else echo starlingmonkey; fi)" lowering \
                --field variant="$v" --field build_exit="$rc" --json leftover_async_lines="$left" \
                --json rolldown_diagnostics="$warn" --field bundle_bytes="$(stat -c %s "$d/bundle-$b.js")"
            if [ "$rc" = 0 ]; then
                "$H" engine "$OUT/lowering-$v-$b.wasm" --workflow lowering --guest "lowering-$v-$b" \
                    --backend "$(if [ $b = qjs ]; then echo quickjs; else echo starlingmonkey; fi)" \
                    --probe lowering --out "$JSONL" >/dev/null
            fi
        done
    done

    log "TS SdkPromise prototype on the real engine (QJS + SM)"
    for g in ts-qjs ts-sm; do
        local b; b="$(backend_of "$g")"
        "$H" inspect "$OUT/$g.wasm" --guest "$g" --backend "$b" --probe ts_engine --out "$JSONL" >/dev/null
        "$H" engine "$OUT/$g.wasm" "${wf[@]/#/--workflow=}" --guest "$g" --backend "$b" --probe ts_engine --out "$JSONL" >/dev/null
        "$H" fuel "$OUT/$g.wasm" --workflow s9-probe --guest "$g" --backend "$b" --probe ts_engine --out "$JSONL" >/dev/null
        log "result mapping + surrogates ($g)"
        "$H" engine "$OUT/$g.wasm" "${res[@]/#/--workflow=}" --guest "$g" --backend "$b" --probe result_mapping --out "$JSONL" >/dev/null
        "$H" engine "$OUT/$g.wasm" "${sur[@]/#/--workflow=}" --guest "$g" --backend "$b" --probe surrogates --out "$JSONL" >/dev/null
    done

    log "componentize-py export naming + library WIT discovery"
    local n="$OUT/naming"; rm -rf "$n"; mkdir -p "$n/wrong" "$n/lib"
    sed 's/^class Workflows(wit_world.exports.Workflows):/class WitWorld(wit_world.exports.Workflows):/' \
        "$HERE/py/app_asyncio.py" > "$n/wrong/app.py"
    local rc=0 err
    (cd "$n/wrong" && "${CPY[@]}" -d "$WIT" -w ostro:runtime/workflow@0.1.0 componentize --stub-wasi \
        -p . app -o x.wasm) > "$n/wrong.log" 2>&1 || rc=$?
    err="$(grep -m1 -E "AttributeError" "$n/wrong.log" || test $? = 1)"
    note py-naming componentize-py export_naming --field case=class_named_WitWorld \
        --field build_exit="$rc" --field error="$err"
    note py-naming componentize-py export_naming --field case=class_named_Workflows \
        --field required_class="app.Workflows(wit_world.exports.Workflows)" \
        --field bindings_module=wit_world --field export_types_module=wit_world.exports.workflows \
        --field result_err="raise componentize_py_types.Err(failure)" \
        --field poll_loop_source="written by 'componentize-py bindings <dir>' as <dir>/poll_loop.py (bundled template)" \
        --field build_exit=0
    mkdir -p "$n/lib/ostrolib/wit"
    cp "$WIT/runtime.wit" "$n/lib/ostrolib/wit/"
    printf 'wit_directory = "wit"\n' > "$n/lib/ostrolib/componentize-py.toml"
    : > "$n/lib/ostrolib/__init__.py"
    cp "$HERE/py/app_asyncio.py" "$n/lib/app.py"
    local c
    for c in "with-w-no-import" "no-w-import"; do
        rc=0
        if [ "$c" = "no-w-import" ]; then
            printf '\nimport ostrolib  # noqa\n' >> "$n/lib/app.py"
            (cd "$n/lib" && "${CPY[@]}" componentize --stub-wasi -p . app -o "$c.wasm") > "$n/$c.log" 2>&1 || rc=$?
        else
            (cd "$n/lib" && "${CPY[@]}" -w ostro:runtime/workflow@0.1.0 componentize --stub-wasi -p . app -o "$c.wasm") > "$n/$c.log" 2>&1 || rc=$?
        fi
        note py-naming componentize-py export_naming --field case="library_toml_$c" \
            --field toml='wit_directory = "wit"' --field build_exit="$rc" --field log="$(tail -1 "$n/$c.log")"
    done

    log "Python OstroLoop prototype inside the embedded CPython"
    "$H" fuel "$OUT/py-loop.wasm" --workflow loop-check --workflow classes --guest py-loop \
        --backend componentize-py --probe py_loop --out "$JSONL" >/dev/null
    "$H" fuel "$OUT/py-base.wasm" --workflow classes --guest py-base \
        --backend componentize-py --probe py_loop --out "$JSONL" >/dev/null
    "$H" engine "$OUT/py-asyncio.wasm" --workflow hello --guest py-asyncio \
        --backend componentize-py --probe py_engine_eng1 --out "$JSONL" >/dev/null
}

# ---------------------------------------------------------------------------------
# Task 3: reproducibility under the D-P9-3 step-1 wrapper
# ---------------------------------------------------------------------------------

do_repro() {
    write_bundle_config
    ts_deps
    local sde r="$OUT/repro"
    sde="$(git -C "$ROOT" log -1 --format=%ct HEAD)"
    rm -rf "$r"; mkdir -p "$r"
    local i
    for mode in plain wrapper; do
        for i in 1 2 3; do
            local t0; t0="$(date +%s)"
            if [ "$mode" = wrapper ]; then
                local st; st="$(mktemp -d "$r/stage-py-$i.XXXXXX")"
                (export PYTHONHASHSEED=0 PYTHONDONTWRITEBYTECODE=1 TZ=UTC LC_ALL=C SOURCE_DATE_EPOCH="$sde" S9_MTIME="$sde"
                 build_py "$r/py-$mode-$i.wasm" base "$st")
                local sts; sts="$(mktemp -d "$r/stage-ts-$i.XXXXXX")"
                cp "$HERE/ts/probe.ts" "$sts/probe.ts"
                find "$sts" -exec touch -h -d "@$sde" {} +
                (export PYTHONHASHSEED=0 PYTHONDONTWRITEBYTECODE=1 TZ=UTC LC_ALL=C SOURCE_DATE_EPOCH="$sde"
                 build_ts "$sts/probe.ts" qjs "$r/qjs-$mode-$i.wasm" >/dev/null)
            else
                build_py "$r/py-$mode-$i.wasm" base
                build_ts "$HERE/ts/probe.ts" qjs "$r/qjs-$mode-$i.wasm" >/dev/null
            fi
            echo "$mode $i start=$t0 end=$(date +%s)" >> "$r/times.txt"
            sleep 3
        done
    done
    python3 -I - "$r" "$sde" > "$DATA/repro.txt" <<'EOF'
import hashlib, os, struct, sys
r, sde = sys.argv[1], int(sys.argv[2])
times = {}
for line in open(os.path.join(r, "times.txt")):
    mode, i, s, e = line.split()
    times[(mode, i)] = (int(s.split("=")[1]), int(e.split("=")[1]))
lo = min(s for s, _ in times.values()) - 3600
hi = max(e for _, e in times.values()) + 60
print("S9 reproducibility (plan 09-04 Task 3)")
print(f"SOURCE_DATE_EPOCH (HEAD commit time) = {sde}")
print("wrapper = D-P9-3 step 1: sources staged into a fresh temp dir with every mtime = SOURCE_DATE_EPOCH,")
print("          PYTHONHASHSEED=0 PYTHONDONTWRITEBYTECODE=1 TZ=UTC LC_ALL=C SOURCE_DATE_EPOCH exported.")
print("plain   = same pinned commands, fresh temp stage dir (python) / in-tree source (ts), no env.")
print("Toolchains: componentize-py 0.25.1 (--stub-wasi), jco 1.42.0 + componentize-qjs 0.5.0;")
print("builds >= 3 s apart (sleep 3).")
print()
def ts_hits(a, b, offs):
    """Diff offsets covered by a 4-byte LE u32 that is a plausible Unix time in both files."""
    hits = {}
    for o in offs:
        for start in range(o - 3, o + 1):
            if start < 0 or start + 4 > len(a):
                continue
            x = struct.unpack_from("<I", a, start)[0]
            y = struct.unpack_from("<I", b, start)[0]
            if x != y and lo <= x <= hi and lo <= y <= hi:
                hits[start] = (x, y)
                break
    return hits
for lang in ("py", "qjs"):
    for mode in ("plain", "wrapper"):
        files = [os.path.join(r, f"{lang}-{mode}-{i}.wasm") for i in (1, 2, 3)]
        blobs = [open(f, "rb").read() for f in files]
        label = {"py": "Python py-base (componentize-py)", "qjs": "TS ts-qjs (QuickJS)"}[lang]
        print(f"== {label}, {mode}")
        for f, b in zip(files, blobs):
            print(f"  {os.path.basename(f)}  sha256={hashlib.sha256(b).hexdigest()}  bytes={len(b)}")
        for x, y in ((0, 1), (1, 2), (0, 2)):
            a, b = blobs[x], blobs[y]
            pair = f"build{x+1} vs build{y+1}"
            if a == b:
                print(f"  {pair}: identical")
                continue
            n = min(len(a), len(b))
            offs = [k for k in range(n) if a[k] != b[k]]
            print(f"  {pair}: lengths {len(a)} vs {len(b)}; differing bytes (cmp -l) = {len(offs)}"
                  f"{' + length tail ' + str(abs(len(a)-len(b))) if len(a) != len(b) else ''}")
            print(f"    first offsets: {[hex(o) for o in offs[:12]]}")
            runs, prev = [], None
            for o in offs:
                if prev is None or o > prev + 1:
                    runs.append([o, o])
                else:
                    runs[-1][1] = o
                prev = o
            print(f"    contiguous runs: {len(runs)}; sizes: {sorted(set(e - s + 1 for s, e in runs))[:10]}")
            hits = ts_hits(a, b, offs)
            covered = sum(1 for o in offs if any(s <= o < s + 4 for s in hits))
            print(f"    4-byte LE Unix timestamps in [{lo}, {hi}] covering diffs: {len(hits)} values, "
                  f"{covered}/{len(offs)} differing bytes")
            for s in list(hits)[:6]:
                u, v = hits[s]
                ctx = a[max(0, s - 24):s + 28]
                printable = "".join(chr(c) if 32 <= c < 127 else "." for c in ctx)
                print(f"      @{hex(s)}: {u} vs {v} (delta {v - u} s)  context: {printable!r}")
            if not hits:
                for s, e in runs[:4]:
                    print(f"      run {hex(s)}..{hex(e)}: {a[s:e+1].hex()} vs {b[s:e+1].hex()}")
        print()
EOF
    cat "$DATA/repro.txt"
}

case "${1:-all}" in
    build) do_build ;;
    measure) [ -n "${S9_FRESH:-}" ] && : > "$JSONL"; do_measure ;;
    probes) do_probes ;;
    repro) do_repro ;;
    summary) summary ;;
    all) [ -n "${S9_FRESH:-}" ] && : > "$JSONL"; harness; do_build; do_measure; do_probes; do_repro ;;
    *) echo "usage: $0 <build|measure|probes|repro|summary|all>" >&2; exit 2 ;;
esac
