---
phase: 10-developer-cli-test-kits
plan: 04
subsystem: clients
tags: [python, typescript, go, clients, scaffold, sdk-06, sdk-07]
requires:
  - phase: 10-developer-cli-test-kits
    provides: "10-01 name reservations (ostro-client, github.com/lspecian/ostro/clients/go); 10-02 ci-local/CI client steps gated on the marker files"
provides:
  - "clients/python: PyPI ostro-client 0.1.0, module ostro_client (__version__), Python >=3.9, hatchling==1.27.0 backend (same as sdks/python), dependencies = [], pytest testpaths = [tests]"
  - "clients/ts: npm ostro-client 0.1.0, ESM, exports \".\" {types: ./src/index.d.ts, default: ./src/index.js}, engines node >=18, files [src/, README.md], no dependencies; exports VERSION"
  - "clients/go: module github.com/lspecian/ostro/clients/go, go 1.23, no require; package ostro with const Version"
affects: [10-14, 10-17, 10-20, 10-08]
tech-stack:
  added: []
  patterns:
    - "Zero-dependency client packages (stdlib / Node built-ins / Go stdlib only)"
key-files:
  created:
    - clients/python/pyproject.toml
    - clients/python/ostro_client/__init__.py
    - clients/python/tests/test_scaffold.py
    - clients/ts/package.json
    - clients/ts/src/index.js
    - clients/ts/src/index.d.ts
    - clients/ts/test/scaffold.test.js
    - clients/go/go.mod
    - clients/go/ostro/doc.go
    - clients/go/ostro/scaffold_test.go
  modified: []
key-decisions:
  - "TS `npm test` runs `node --test 'test/**/*.test.js'` (the glob ci-local/CI already use) instead of `node --test test/`, which needs a test/index.js"
  - "No routes.py/routes.js/routes.go yet: contract.rs fails on an existing table with zero pairs, so 10-14 creates them"
metrics:
  duration: "~15min"
  completed: 2026-11-22
---

# Phase 10 Plan 04: Python/TS/Go client scaffolds Summary

The three hand-written client packages now exist: Python `ostro-client` (module `ostro_client`), npm `ostro-client` (ESM, with types) and the Go module `github.com/lspecian/ostro/clients/go`. None has runtime dependencies. Each has one green placeholder version test. Because the marker files (`clients/python/pyproject.toml`, `clients/ts/package.json`, `clients/go/go.mod`) now exist, the 10-02 ci-local and CI client steps run their suites instead of skipping with "scaffold pending (10-04)".

## Package metadata

| Package | Name / path | Version | Runtime floor | Deps | Test |
|---------|-------------|---------|---------------|------|------|
| Python | `ostro-client`, module `ostro_client` | 0.1.0 | Python >=3.9 | none (hatchling==1.27.0 is build-only) | tests/test_scaffold.py::test_version |
| TS | `ostro-client` (no scope), `"type": "module"` | 0.1.0 | Node >=18 | none | test/scaffold.test.js (VERSION == package.json version) |
| Go | `github.com/lspecian/ostro/clients/go`, package `ostro` | 0.1.0 (const) | go 1.23 | no `require` | ostro/scaffold_test.go::TestVersion |

All three use the Apache-2.0 licence from the repo LICENSE.

## Test outputs

- Python (uv 0.7.15, `--python 3.12 --with pytest==8.4.2`): `1 passed`. Also `1 passed` on Python 3.9.
- TS (Node 24.13.0, `node --test --test-reporter=tap 'clients/ts/test/**/*.test.js'`): `# pass 1`, `# fail 0`. `npm test` in clients/ts is green too.
- Go 1.23.10: `go vet ./...` is clean, and `go test ./...` prints `ok github.com/lspecian/ostro/clients/go/ostro`. gofmt is clean.
- `cargo test -p ostro-build --test sdk_bundle`: 6 passed, including `bundle_has_client_kits_when_present`, which now bundles clients/python/ostro_client and clients/ts/{package.json,src}.
- `cargo test -p ostro-client --test contract`: 5 passed (the polyglot tables are still absent and pending until 10-14).
- Full `scripts/ci-local.sh --no-musl`: not run.

## Task Commits

1. **Task 1: Python, TS and Go scaffolds with placeholder tests**: `53bce34`

## Deviations from Plan

**1. [Rule 1 - Bug] TS test script and verify command use the glob**
- **Issue:** the plan's `"test": "node --test test/"` and its verify `node --test clients/ts/test/` fail with Node 24 when test/ has no index.js (see the 10-02 open issue).
- **Fix:** package.json uses `node --test 'test/**/*.test.js'`. The verify ran with the glob form that ci-local and CI use.
- **Files:** clients/ts/package.json

**2. Paths:** the worktree was in the session scratchpad, not /tmp/ostro-wt-10-04, and the go output file was the scratchpad `10-04-go.txt`, as the executor brief requires.

## Notes for later plans

- 10-08 bundle: an incremental build made before this commit keeps a bundle without `clients/`. Run `touch crates/ostro-build/build.rs` once (clean builds and CI are unaffected).
- 10-14: add `routes.py`, `src/routes.js` and `ostro/routes.go` in the formats documented in crates/ostro-client/tests/contract.rs.
- 10-17: add the pytest11 entry point and the test kits. `README.md` is listed in the TS `files` but does not exist yet (10-14/10-17).

## Self-Check: PASSED

- All 10 files exist on main at 53bce34. `git show --stat 53bce34` lists 10 files.
