#!/usr/bin/env bash
# Time-boxed libFuzzer runs of the Ostro fuzz targets (Phase 14, 14-07, SEC-05; D-P14-17).
#
# usage: scripts/fuzz-smoke.sh [--merge] [target...]
#
#   OSTRO_FUZZ_SECS   seconds per target (default 60)
#
# - Toolchain: the dated nightly of fuzz/rust-toolchain.toml; cargo-fuzz from
#   scripts/fetch-tools.sh (pinned 0.13.2). Built with debug assertions (overflow checks).
# - Default targets: all 15 of fuzz/Cargo.toml. A target whose body is still a stub
#   ("stub until 14-0N" on its entry point) is reported SKIP (stub).
# - Each target runs `cargo fuzz run <t> fuzz/artifacts/corpus/<t> fuzz/corpus/<t>` with
#   -max_total_time=$OSTRO_FUZZ_SECS -rss_limit_mb=2048 -timeout=10 -print_final_stats=1.
#   New corpus entries go to fuzz/artifacts/corpus/<t>/ (gitignored); the committed seeds in
#   fuzz/corpus/<t>/ are read only. Crash / timeout / OOM inputs land in fuzz/artifacts/<t>/.
# - Prints a summary table (target, execs, exec/s, cov, new units, result) and exits non-zero
#   if any target crashed, timed out or ran out of memory.
# - --merge: instead of fuzzing, merge fuzz/artifacts/corpus/<t>/ into fuzz/corpus/<t>/ with
#   libFuzzer's -merge=1 (only inputs that add coverage), then fail if a committed corpus
#   exceeds 2 MiB. Run it before committing corpus growth; regressions are committed
#   separately (minimised with `cargo fuzz tmin`, see fuzz/regressions/README.md).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

SECS="${OSTRO_FUZZ_SECS:-60}"
CAP_BYTES=$((2 * 1024 * 1024))
MERGE=0
if [ "${1:-}" = "--merge" ]; then
  MERGE=1
  shift
fi

NIGHTLY=$(sed -n 's/^channel *= *"\(.*\)"/\1/p' fuzz/rust-toolchain.toml)
[ -n "$NIGHTLY" ] || { echo "fuzz-smoke: no channel in fuzz/rust-toolchain.toml" >&2; exit 2; }
if ! rustup run "$NIGHTLY" rustc --version >/dev/null 2>&1; then
  echo "fuzz-smoke: installing $NIGHTLY" >&2
  rustup toolchain install "$NIGHTLY" --profile minimal >&2
fi

FUZZ_LINE=$(bash scripts/fetch-tools.sh cargo-fuzz | grep '^cargo-fuzz=')
CARGO_FUZZ="${FUZZ_LINE#cargo-fuzz=}"
[ -x "$CARGO_FUZZ" ] || { echo "fuzz-smoke: cargo-fuzz not found ($FUZZ_LINE)" >&2; exit 2; }
# cargo-fuzz runs as `cargo fuzz`: put its directory on PATH.
PATH="$(dirname "$CARGO_FUZZ"):$PATH"
export PATH

ALL=$(sed -n 's/^name = "\([a-z_]*\)"$/\1/p' fuzz/Cargo.toml | grep -v '^ostro-fuzz$' || true)
if [ "$#" -gt 0 ]; then
  TARGETS=("$@")
else
  read -r -d '' -a TARGETS <<<"$ALL" || true
fi

is_stub() {
  grep -B1 -E "pub fn $1\(" crates/ostro-fuzz-targets/src/*.rs | grep -q 'stub until'
}

for t in "${TARGETS[@]}"; do
  if ! grep -qx "$t" <<<"$ALL"; then
    echo "fuzz-smoke: unknown target $t (known: $(tr '\n' ' ' <<<"$ALL"))" >&2
    exit 2
  fi
done

if [ "$MERGE" = 1 ]; then
  status=0
  for t in "${TARGETS[@]}"; do
    work="fuzz/artifacts/corpus/$t"
    if is_stub "$t" || [ ! -d "$work" ]; then
      echo "merge $t: nothing to merge"
      continue
    fi
    mkdir -p "fuzz/corpus/$t"
    before=$(find "fuzz/corpus/$t" -type f | wc -l)
    cargo "+$NIGHTLY" fuzz run --fuzz-dir fuzz --debug-assertions "$t" "fuzz/corpus/$t" "$work" -- \
      -merge=1 -rss_limit_mb=2048 -timeout=10 >"fuzz/artifacts/merge-$t.log" 2>&1
    after=$(find "fuzz/corpus/$t" -type f | wc -l)
    bytes=$(du -sb "fuzz/corpus/$t" | cut -f1)
    echo "merge $t: $before -> $after files, $bytes bytes"
    if [ "$bytes" -gt "$CAP_BYTES" ]; then
      echo "merge $t: corpus exceeds 2 MiB; run 'cargo fuzz cmin $t fuzz/corpus/$t' and drop large inputs" >&2
      status=1
    fi
  done
  exit "$status"
fi

rows=()
status=0
for t in "${TARGETS[@]}"; do
  if is_stub "$t"; then
    rows+=("$t|-|-|-|-|SKIP (stub)")
    continue
  fi
  work="fuzz/artifacts/corpus/$t"
  art="fuzz/artifacts/$t"
  mkdir -p "$work" "$art" "fuzz/corpus/$t"
  log="fuzz/artifacts/$t.log"
  marker="fuzz/artifacts/.start-$t"
  touch "$marker"
  rc=0
  cargo "+$NIGHTLY" fuzz run --fuzz-dir fuzz --debug-assertions "$t" "$work" "fuzz/corpus/$t" -- \
    -max_total_time="$SECS" -rss_limit_mb=2048 -timeout=10 -print_final_stats=1 \
    -artifact_prefix="$art/" >"$log" 2>&1 || rc=$?
  stat() { sed -n "s/^stat::$1: *//p" "$log" | tail -n 1; }
  execs=$(stat number_of_executed_units)
  eps=$(stat average_exec_per_sec)
  units=$(stat new_units_added)
  cov=$(grep -oE 'cov: [0-9]+' "$log" | tail -n 1 | cut -d' ' -f2 || true)
  result=ok
  if [ "$rc" -ne 0 ]; then
    found=$(find "$art" -maxdepth 1 -type f \( -name 'crash-*' -o -name 'timeout-*' -o -name 'oom-*' -o -name 'leak-*' \) -newer "$marker" 2>/dev/null | head -n 1)
    case "$(basename "${found:-none}")" in
      crash-*) result="CRASH $found" ;;
      timeout-*) result="TIMEOUT $found" ;;
      oom-*) result="OOM $found" ;;
      leak-*) result="LEAK $found" ;;
      *) result="FAIL (exit $rc, see $log)" ;;
    esac
    status=1
  fi
  rows+=("$t|${execs:--}|${eps:--}|${cov:--}|${units:--}|$result")
  rm -f "$marker"
done

printf '%-22s %12s %8s %7s %6s  %s\n' target execs exec/s cov new result
for r in "${rows[@]}"; do
  IFS='|' read -r t e s c u res <<<"$r"
  printf '%-22s %12s %8s %7s %6s  %s\n' "$t" "$e" "$s" "$c" "$u" "$res"
done
exit "$status"
