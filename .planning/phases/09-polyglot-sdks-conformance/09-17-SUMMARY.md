---
phase: 09-polyglot-sdks-conformance
plan: 17
subsystem: sdk-builds
tags: [reproducible-builds, d-p9-3, componentize-py, quickjs, packaging, ci, sdk-04, wave-7]
requires:
  - phase: 09-polyglot-sdks-conformance
    provides: "09-02 build-guest.sh + polyglot cache; 09-04 S9 repro verdict and fork estimate; 09-11/14/15 engine leg + guests; 09-16 full table"
provides:
  - "sdks/scripts/repro-check.sh: 3-build sha256 comparison per language (Rust hard, py/ts against RESIDUAL-ACCEPTED markers)"
  - "sdks/scripts/build-componentize-py-fork.sh + sdks/python/toolchain/componentize-py-0.25.1-deterministic.patch (D-P9-3 step 2, opt-in via OSTRO_COMPONENTIZE_PY)"
  - "C-REBUILD-01: conformance/spec/engine/C-REBUILD-01.json + crates/ostro-conformance/tests/rebuild.rs + conformance/fixtures/py-setorder"
  - "docs/sdk-builds.md: toolchains, recipe, ladder outcome, residuals, Phase 10 artefact, packaging"
  - "ci-local polyglot block (--no-polyglot) and SHA-pinned ci.yml polyglot job; ostro-server:children_failpoints in both failpoint lists"
affects: [09-18, 09-19, 09-20, phase-10]
tech-stack:
  added: []
  patterns:
    - "Replay-on-rebuild: record on the cached build, replay on a fresh build of the same source"
    - "Accepted residuals are machine-checked: repro-check greps `RESIDUAL-ACCEPTED: <lang>` in docs/sdk-builds.md"
key-files:
  created:
    - sdks/scripts/repro-check.sh
    - sdks/scripts/build-componentize-py-fork.sh
    - sdks/python/toolchain/README.md
    - sdks/python/toolchain/componentize-py-0.25.1-deterministic.patch
    - conformance/fixtures/py-setorder/app.py
    - conformance/spec/engine/C-REBUILD-01.json
    - crates/ostro-conformance/tests/rebuild.rs
    - docs/sdk-builds.md
  modified:
    - sdks/scripts/build-guest.sh
    - sdks/scripts/toolchain/rolldown.ostro.mjs
    - sdks/ts/rolldown.ostro.mjs
    - crates/ostro-conformance/src/polyglot.rs
    - sdks/python/pyproject.toml
    - scripts/ci-local.sh
    - .github/workflows/ci.yml
key-decisions:
  - "D-P9-3 ladder: Rust bit-reproducible; Python reached step 3 after a working fork (73-76 B residual from WASI file birth/access times at pre-init) -> RESIDUAL-ACCEPTED py, SC2 Python human_needed; TS/QuickJS step 3 (pre-init entropy value + layout drift) -> RESIDUAL-ACCEPTED ts, SC2 TS human_needed"
  - "Phase 10 artefact (D-P9-30): the released componentize-py 0.25.1 wheel; the fork stays an opt-in, locally/CI-built toolchain (OSTRO_COMPONENTIZE_PY) and is not built in CI"
  - "The polyglot cache key includes the componentize-py selection so fork and released builds never share cache entries"
  - "ci-local/ci.yml run the whole ostro-conformance package with --features polyglot (covers polyglot_python/ts/cross and rebuild); polyglot_cross::full_table overlaps the per-language targets but is kept for the cross-language table"
requirements-completed: [SDK-04, SDK-02, SDK-03]
duration: ~2 h 40 min of work (plus a ~2 h 30 min ci-local run)
completed: 2026-11-22
---

# Phase 9 Plan 17: Reproducible builds, packaging, polyglot CI Summary

**Rust guests are bit-reproducible (3 builds, sha256 `debb9349…eb79f2770`). Python and TypeScript are not, and now carry machine-checked, documented residuals (D-P9-3 step 3). For Python a working componentize-py 0.25.1 fork (step 2, built in 6.5 min) pins the str hash secret, entropy, clocks, mtimes and adapter order. That shrinks the diff from whole-snapshot layout changes (lengths moving by up to 64 KB) to 73–76 bytes at a fixed length. The rest is file birth/access times that wasmtime-wasi reports during pre-init. C-REBUILD-01 shows that journals recorded on one build replay on an independent rebuild with 0 appended events for all 27 engine scenarios in Rust, Python and TS. The set-of-str hazard is detected as NonDeterminism with the released toolchain (with the fork the order is the same). Both packages pass their dry runs. ci-local gains a polyglot block and ci.yml a SHA-pinned `polyglot` job.**

## Commits

| Task | Commit | Content |
|------|--------|---------|
| 1 | `780e2b1` | repro-check.sh, fork patch + build script, build-guest.sh `OSTRO_COMPONENTIZE_PY`, C-REBUILD-01 (spec, rebuild.rs, py-setorder), polyglot cache key, rolldown `external`, docs/sdk-builds.md |
| 2 | `ee848a2` | pyproject.toml packaging (sdist without tests, caches excluded) |
| 3 | `6e8d588` | ci-local polyglot block + `--no-polyglot`, ci.yml `polyglot` job, `ostro-server:children_failpoints` in both failpoint lists |

## D-P9-3 ladder outcome

| Language | Wrapper (step 1) | Fork (step 2) | Rung |
|---|---|---|---|
| Rust | identical: 3 × `debb9349c16860ca0b88c04b2b365b33e53097c9ca8c0ddb7ff2c1eeb79f2770`, 287,002 B | n/a | bit-reproducible (hard requirement met) |
| Python | 3 different shas and lengths per run (21,995,290–22,059,308 B) | same length 21,128,563 B; attempt 1: 102–103 B differ; attempt 2 (atime pinned too): **73–76 B** | step 3: `RESIDUAL-ACCEPTED: py`, SC2 Python human_needed |
| TS (QuickJS) | 3 different shas per run, 2,308,337–2,308,382 B; the first diff is one 8-byte snapshot value (4 B differ), then segment-layout drift | n/a | step 3: `RESIDUAL-ACCEPTED: ts`, SC2 TS human_needed |

Python fork residual: 13 data segments of the main core module's snapshot. 38 B are `{sec, nsec}` wall-clock pairs next to the pinned mtime, i.e. file status-change/birth and access times (wasmtime-wasi 46 maps `status-change-timestamp` to the host birth time; relatime moves atime on read). The remaining ~35 B are 1–4-byte values in small heap objects, not attributed. The str hash secret is pinned (`py-setorder` gives the same order on every fork build).

Fork build log: clone of v0.25.1 (`2a53e46a`), Rust 1.100.0 via a `rust-toolchain.toml` written into the clone. The runtime's nested cargo strips `RUST*` variables, so `RUSTUP_TOOLCHAIN` alone does not reach it. `build.rs` downloads wasi-sdk 33, CPython `v3.14.0-wasi-sdk-30`, zlib 1.3.1 and SQLite 3.51.2. Attempt 1 (cold): 6 min 31 s, 1.5 GB peak RSS, 3.6 GB disk. Attempt 2: 42 s incremental. The time box was two attempts, so the ladder stopped at step 3. The upstream PR text and the componentize-qjs issue text are in `sdks/python/toolchain/README.md` (filing them is human_needed).

## Phase 10 artefact (F2, D-P9-30)

Phase 10's hermetic `ostro build` ships the **released componentize-py 0.25.1 wheel**. Its residual is the whole snapshot layout plus a per-build str hash secret. The fork stays opt-in (`OSTRO_COMPONENTIZE_PY`, `build-componentize-py-fork.sh`) because there is no hosted artefact, and CI does not build it (~3.6 GB, 6–30 min cold).

## C-REBUILD-01

`cargo test -p ostro-conformance --features polyglot --test rebuild -- --test-threads=1`: 4/4 pass.
- rust: build A = B (asserted); 27 scenarios / 29 journals replay with 0 appended events.
- py, released wheel: A ≠ B; 27 / 29 replay with 0 appended events; C-NDET-01 stays NonDeterminism on B.
- py, fork: A ≠ B at the same length; 27 / 29 replay.
- ts: A ≠ B; 27 / 29 replay.
- py-setorder, released wheel: the step order differs between builds, and replaying A on B → NonDeterminism (asserted). With the fork: same order (asserted), and A replays on B.

## Packaging (dry runs, nothing published)

- `uv build`: `ostro-0.1.0-py3-none-any.whl`.
  - Contents: 13 `ostro/*.py` files, `ostro/py.typed`, `ostro/wit/runtime.wit` and the dist-info.
  - Metadata: Name ostro, Version 0.1.0, License-Expression Apache-2.0, Requires-Python >=3.12, no dependencies.
- `ostro-0.1.0.tar.gz`: the same package files plus `pyproject.toml`, `PKG-INFO` and `.gitignore`. Tests are no longer included (fixed in `pyproject.toml`).
- `npm pack --dry-run --json`: `package.json`, `rolldown.ostro.mjs`, 16 `src/*.ts` files and `wit/runtime.wit`. `README.md` is listed in `files` and will appear once 09-20 writes it. No package.json change was needed.

## CI

Action tag → SHA (resolved with `git ls-remote --tags`):

| Action | Tag | SHA |
|---|---|---|
| actions/setup-node | v7.1.0 | `949feb2413d6458794dcd2491c4babbbce0c15c1` |
| actions/setup-python | v7.1.0 | `6d4fd22b20465ccc0f7354476588e8df7c9ca0e0` |
| astral-sh/setup-uv | v10.4.0 | `1db35dd8eee11c5894872e0b383ae6c406cf738d` |
| actions/checkout (existing pin) | v7.0.1 | `3d3c42e5aac5ba805825da76410c181273ba90b1` |
| Swatinem/rust-cache (existing pin) | v2.9.3 | `d1527c8f67ef9af357290475d4af4b0fab8f12e0` |

ci-local `--no-musl` at `ee848a2` + the task-3 diff. Timings:
- toolchain 0 s, fmt 2 s, clippy 64 s, build 46 s, sdk lint 22 s, sdk test 25 s, guests 9 s, order-saga 21 s;
- cargo test 3,807 s;
- **polyglot toolchains 1 s, python sdk tests 2 s (148 passed), ts sdk tests 0 s (`# pass 99`), polyglot conformance PASS 177 s (17 targets incl. rebuild 4/4), clippy (polyglot) 2 s**;
- sleepers 36 s, failpoint sync 1 s, failpoint suites 3,211 s, campaign smoke 623 s, phase 7 54 s;
- check-vendored, deny fixtures, forbidden crates and workflow validation all PASS.

`cargo deny check` FAILED on that base: RUSTSEC-2026-0592 in `url` 2.5.8, a new advisory unrelated to this plan. Main had meanwhile moved to `url` 2.5.9. After rebasing task 3 onto main (`3df92f0`), I re-ran:
- `cargo deny check`: advisories ok, bans ok, licenses ok, sources ok;
- failpoint list sync: OK;
- `children_failpoints`: 4/4;
- ostro-server failpoints clippy: clean;
- polyglot clippy: clean;
- the polyglot conformance package (see the coordinator report).

The full ~2.5 h gate was not repeated on the rebased head.

## Coordinator hand-offs addressed

- 09-10:
  - Reviewed `transform.inject` in both rolldown copies (sound: it binds the SDK's deterministic globals at module evaluation and the export test pins it) and the Node resolve hook (sound: `registerHooks` maps `ostro:runtime/*` to throwing stubs generated from the frozen WIT).
  - Added `external: [/^ostro:/]` to both copies. jco merges it with its own externals, the UNRESOLVED_IMPORT warnings are gone, and the copies stay byte-identical.
- 09-12: the TS steps use `node --test --test-reporter=tap` and grep `^# pass`.
- 09-08: `ostro-server:children_failpoints` is appended in both lists, after 12-01's ostro-cluster / ostro-repl-conformance entries (merged during the rebase; no line dropped).
- 09-14/15/16: the polyglot step runs the whole `ostro-conformance` package with `--features polyglot`, so polyglot_python, polyglot_ts, polyglot_cross and rebuild all run. `full_table` overlaps the per-language targets (~52 s) but stays for the cross-language table.

## Deviations from Plan

1. [Rule 2 - correctness] `crates/ostro-conformance/src/polyglot.rs` (not in files_modified): the cache key now includes the componentize-py selection (`OSTRO_COMPONENTIZE_PY` path, size and mtime). Without it, a fork run would silently reuse components built with the released wheel. Python cache entries rebuild once.
2. [Coordinator item] `sdks/ts/rolldown.ostro.mjs` and `sdks/scripts/toolchain/rolldown.ostro.mjs` gained `external` (not in files_modified).
3. `repro-check.sh` prints `DIFF length-<a>-vs-<b>` when the lengths differ (no positional byte count makes sense then).
4. `build.rs` downloads zlib and SQLite from their upstream release URLs, besides the three upstreams the plan lists. These downloads are part of componentize-py's own build.
5. The ci.yml `polyglot` job is appended at the end of the file (append-only). It does not build the fork, because step 2 was not adopted as closing the gap.

## Open issues

- SC2 Python and SC2 TS: human_needed (upstream fixes; texts ready in `sdks/python/toolchain/README.md`).
- The `polyglot` CI job is new and untested on GitHub. Watch the runner disk and time: about 3 min of conformance locally, plus 3 cold Rust guest builds and 6 polyglot builds in repro-check.
- `RESIDUAL-ACCEPTED` markers make repro-check pass while residuals exist. A future toolchain that becomes reproducible should drop its marker.

## Self-Check: PASSED

Commits 780e2b1, ee848a2, 6e8d588 exist on main; all key files present.
