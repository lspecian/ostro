#!/usr/bin/env bash
# The shared musl binary size limit (NFR-2.1, D-P14-54): one value in scripts/binary-size-limit
# (bytes, decimal), read by scripts/ci-local.sh and scripts/release-dry-run.sh. Source this
# file; OSTRO_MAX_BINARY_BYTES overrides the file value (an integer number of bytes).

ostro_binary_size_limit_file() {
  echo "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/binary-size-limit"
}

# The limit in bytes; exits non-zero (with a message) when the value is not an integer.
ostro_max_binary_bytes() {
  local file v
  file="$(ostro_binary_size_limit_file)"
  if [[ -n "${OSTRO_MAX_BINARY_BYTES:-}" ]]; then
    v="$OSTRO_MAX_BINARY_BYTES"
  else
    [[ -r "$file" ]] || { echo "binary size limit file missing: $file" >&2; return 1; }
    v="$(tr -d '[:space:]' <"$file")"
  fi
  if [[ ! "$v" =~ ^[0-9]+$ ]] || [[ "$v" -eq 0 ]]; then
    echo "invalid binary size limit ${v@Q} (want a positive integer of bytes)" >&2
    return 1
  fi
  echo "$v"
}

# The limit in decimal MB for messages (60000000 -> 60).
ostro_max_binary_mb() {
  local b
  b="$(ostro_max_binary_bytes)" || return 1
  awk -v b="$b" 'BEGIN { mb = b / 1000000; if (mb == int(mb)) printf "%d\n", mb; else printf "%.1f\n", mb }'
}
