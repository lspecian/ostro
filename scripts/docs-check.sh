#!/usr/bin/env bash
# docs-check (Phase 14, D-P14-27): keep documentation tied to compiled code and valid links.
#
#   bash scripts/docs-check.sh [paths...]      (default: README.md docs)
#
# 1. Marker rule: every ```rust / ```python / ```typescript / ```ts / ```toml block carries
#    `<!-- ostro:snippet path=<file> lines=<a>-<b> -->` (bytes must match lines a..b of a file
#    that CI builds or tests) or `<!-- ostro:illustrative -->` (only under docs/migrations/,
#    or with `pseudo` on the marker line for explicit pseudo-code).
# 2. Links: every relative link and #anchor resolves, and every ostro:snippet block matches
#    its source lines byte for byte (scripts/check-links.py, standard library, offline).
#
# Paths are relative to the repository root. Exit status 1 on any failure.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

if [[ $# -eq 0 ]]; then
  set -- README.md docs
fi

status=0
echo "== docs-check: marker rule"
if ! python3 -I scripts/check-links.py --markers "$@"; then
  status=1
fi
echo "== docs-check: links, anchors and snippets"
if ! python3 -I scripts/check-links.py "$@"; then
  status=1
fi
if [[ $status -ne 0 ]]; then
  echo "docs-check: FAIL"
  exit 1
fi
echo "docs-check: PASS"
