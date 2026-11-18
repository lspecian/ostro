#!/usr/bin/env bash
# check-vendored.sh: provenance and isolation checks for vendored code (see VENDORED.md).
#
#   1. every file under vendor/ has a `| <path> |` row in VENDORED.md
#   2. no forbidden Chronik/tokio-uring references in crates/ or vendor/, no
#      `[patch` table in any tracked Cargo.toml, no CHRONIK_* env reads in vendor/
#   3. cargo metadata: no git sources, every path package lives inside the repo
#   4. no tracked file outside .planning/ leaks a local session path, and no
#      build manifest / build script / cargo config references the upstream repo
#      (comment lines are provenance and allowed, as are VENDORED.md and NOTICE)
#   5. every vendored crate manifest has edition = "2021" and no [workspace] table
#
# Prints PASS/FAIL per check; exits 1 if any check fails.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
ROOT="$(pwd -P)"

failures=0
pass() { printf 'PASS  %s\n' "$1"; }
fail() {
  printf 'FAIL  %s\n' "$1"
  if [ -n "${2:-}" ]; then printf '%s\n' "$2" | sed 's/^/      /'; fi
  failures=$((failures + 1))
}

# --- 1. VENDORED.md row per vendored file ---------------------------------
missing=""
while IFS= read -r f; do
  f="${f#./}"
  if ! grep -qF "| $f |" VENDORED.md; then
    missing+="$f"$'\n'
  fi
done < <(find vendor -type f -not -path '*/target/*' | sort)
if [ -z "$missing" ]; then
  pass "1 every vendor/ file has a VENDORED.md row"
else
  fail "1 vendor/ files without a VENDORED.md row" "${missing%$'\n'}"
fi

# --- 2. forbidden references ----------------------------------------------
hits="$(grep -rnE 'chronik_common|chronik_monitoring|chronik_raft_bridge|tokio-uring|tokio_uring' \
  --include='*.rs' --include='Cargo.toml' crates vendor || true)"
patch_hits="$(git ls-files -z -- 'Cargo.toml' '*/Cargo.toml' | xargs -0 -r grep -nE '^[[:space:]]*\[patch' || true)"
env_hits="$(grep -rnF 'env::var("CHRONIK_' vendor || true)"
all2="$(printf '%s\n%s\n%s' "$hits" "$patch_hits" "$env_hits" | sed '/^$/d')"
if [ -z "$all2" ]; then
  pass "2 no forbidden references, no [patch], no CHRONIK_* env reads"
else
  fail "2 forbidden references found" "$all2"
fi

# --- 3. dependency sources ------------------------------------------------
if ! meta="$(cargo metadata --format-version 1 --locked 2>/dev/null)"; then
  fail "3 cargo metadata --locked failed (Cargo.lock out of date?)"
else
  git_srcs="$(jq -r '.packages[] | select((.source // "") | startswith("git+")) | "\(.name) \(.source)"' <<<"$meta")"
  outside="$(jq -r --arg root "$ROOT/" '.packages[] | select(.source == null) | select(.manifest_path | startswith($root) | not) | "\(.name) \(.manifest_path)"' <<<"$meta")"
  if [ -z "$git_srcs" ] && [ -z "$outside" ]; then
    pass "3 no git sources, all path packages inside the repo"
  else
    fail "3 disallowed dependency sources" "$(printf '%s\n%s' "$git_srcs" "$outside" | sed '/^$/d')"
  fi
fi

# --- 4. no session paths, no build references to the upstream repo -------
# Patterns are split so this script does not match itself.
p_session="claude""-1000|scratch""pad"
leaks="$(git ls-files -z | grep -zv '^\.planning/' | xargs -0 -r grep -lE "$p_session" 2>/dev/null || true)"
p_upstream="chronik""-stream"
build_refs=""
while IFS= read -r -d '' f; do
  case "$f" in
    *.rs) refs="$(grep -nE "$p_upstream" "$f" | grep -vE '^[0-9]+:[[:space:]]*//' || true)" ;;
    *) refs="$(grep -nE "$p_upstream" "$f" | grep -vE '^[0-9]+:[[:space:]]*#' || true)" ;;
  esac
  if [ -n "$refs" ]; then build_refs+="$f: $refs"$'\n'; fi
done < <(git ls-files -z -- 'Cargo.toml' '*/Cargo.toml' 'build.rs' '*/build.rs' '.cargo/*' 'Cargo.lock')
if [ -z "$leaks" ] && [ -z "$build_refs" ]; then
  pass "4 no session paths in tracked files, no build references to the upstream repo"
else
  fail "4 session-path leaks or upstream build references" "$(printf '%s\n%s' "$leaks" "${build_refs%$'\n'}" | sed '/^$/d')"
fi

# --- 5. vendored manifests: edition 2021, no [workspace] ------------------
bad5=""
for m in vendor/*/Cargo.toml; do
  grep -qE '^edition[[:space:]]*=[[:space:]]*"2021"' "$m" || bad5+="$m: edition is not \"2021\""$'\n'
  if grep -qE '^[[:space:]]*\[workspace' "$m"; then bad5+="$m: has a [workspace] table"$'\n'; fi
done
if [ -z "$bad5" ]; then
  pass "5 vendored manifests use edition 2021 and have no [workspace]"
else
  fail "5 vendored manifest problems" "${bad5%$'\n'}"
fi

if [ "$failures" -gt 0 ]; then
  printf 'check-vendored: %d check(s) failed\n' "$failures"
  exit 1
fi
printf 'check-vendored: all checks passed\n'
