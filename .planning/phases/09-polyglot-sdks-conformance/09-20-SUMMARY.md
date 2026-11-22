---
phase: 09-polyglot-sdks-conformance
plan: 20
subsystem: phase-gate
tags: [gate, docs, sdk, conformance, children, reservations, validation]
requires:
  - phase: 09-polyglot-sdks-conformance
    provides: "09-01..09-19 and eng2-mem SUMMARYs"
provides:
  - "PHASE-9 GATE passed: ci-local --no-musl all steps passed at 762bbc5, every gate command green, SC1-SC5 + WF-07 verdicts recorded"
  - "sdks/python/README.md, sdks/ts/README.md, Rust README 'Children, saga, random_f64'"
  - "docs/conformance.md, docs/children.md, docs/storage-format.md §18.1"
  - "RESERVATIONS Phase 9 rows flipped; REQUIREMENTS ticks; 09-VALIDATION complete; 09-DECISIONS amendments A1-A32; STATE results"
affects: [10, 11]
tech-stack:
  added: []
  patterns: ["docs-only commits built on the current main blob through a private index (index-09-20) and a CAS update-ref, without touching the worktree under a running gate"]
key-files:
  created:
    - sdks/python/README.md
    - sdks/ts/README.md
    - docs/conformance.md
    - docs/children.md
    - .planning/phases/09-polyglot-sdks-conformance/09-20-SUMMARY.md
  modified:
    - sdks/rust/README.md
    - docs/storage-format.md
    - .planning/RESERVATIONS.md
    - .planning/REQUIREMENTS.md
    - .planning/STATE.md
    - .planning/ROADMAP.md
    - .planning/phases/09-polyglot-sdks-conformance/09-VALIDATION.md
    - .planning/phases/09-polyglot-sdks-conformance/09-DECISIONS.md
key-decisions:
  - "SDK-04 left unticked (Partial): reseeding is verified, but Python/TS builds carry an accepted D-P9-3 residual (human_needed)"
  - "WF-07 ticked: C-NATIVE-01 + C-RAND-02 green for Python (09-14) and TS (09-15) and again in the gate run"
  - "P9 block: Intent 19 ChildJournal is the only Phase 9 ordinal (D-P9-31); every other slot reads 'unused at Phase 9 end; fallback not needed'"
  - "Musl leg not run (coordinator instruction --no-musl); Phase 9 checkbox in ROADMAP left for the coordinator"
requirements-completed: [SDK-02, SDK-03, SDK-05, WF-06, WF-07, WF-10]
duration: ~3 h 40 min (2 h 33 min ci-local)
completed: 2026-11-22
---

# Phase 9 Plan 20: PHASE-9 GATE Summary

**The gate is green. `ci-local --no-musl` passed every step, polyglot block included, at 762bbc5. All 46 catalogue specs are present and the three-language suite passes 100%. Rust builds are bit-reproducible, and the Python/TS residuals are accepted as D-P9-3 step 3. A 20-iteration crash-leg re-run had 0 violations. WIT is unchanged since the 09-01 baseline, and the reservations check passes. The SDK READMEs, docs/conformance.md and docs/children.md describe the shipped behaviour. Phase 9's evidence, amendments and human_needed items are recorded.**

## Commits
- `96e9cee` docs(09-20): SDK READMEs, conformance and children docs
- `636f1c4` docs(09-20): flip Phase 9 reservation rows
- `e07599f` docs(09-20): Phase 9 validation, decisions fold-in and state
- the final docs commit: this SUMMARY and the ROADMAP plan tick

All four are docs-only. The gate ran on the code at `762bbc5`, and 09-20 changed no code.

## Gate command log
These ran in a worktree at main `762bbc5` with `CARGO_TARGET_DIR=<repo>/target/slot-1`, `nice -n 10` and `CARGO_BUILD_JOBS=4`. Phase 11/12 executors were building at the same time.

| # | Command | Result | Time |
|---|---------|--------|------|
| 1 | `bash scripts/ci-local.sh --no-musl` | `all steps passed`, EXIT=0 (13:13:06Z → 15:45:54Z) | 2 h 33 min |
| 2 | `cargo test -p ostro-engine --test replay_corpus --locked` | ok, 5 passed | 8 s |
| 2 | `cargo test -p ostro-witcheck --locked` | ok (`frozen_0_1_0_is_byte_identical`, `runtime_0_1_chain_is_additive`) | 10 s |
| 2 | `git diff --quiet 38c3b66 main -- wit/` | empty | — |
| 3 | `cargo test -p ostro-conformance --locked --test spec_catalogue -- --ignored all_specs_present` | ok, 1 passed | 4 s |
| 4 | `cargo test -p ostro-conformance --features polyglot --locked -- --test-threads=2` (ci-local polyglot step) | every target ok: polyglot_python 8, polyglot_ts 7, polyglot_cross 4 (incl. `full_table`), polyglot_engine 7, rebuild 4, sdk_engine_smoke 4, engine_leg 16, server_leg 7, smoke/build/catalogue/WIT tests | 233 s |
| 5 | `bash sdks/scripts/repro-check.sh all` | EXIT=0. rust OK (3 × `a5b797da…`, 287,005 B). py and ts DIFF, both "accepted residual" | 67 s |
| 5 | `OSTRO_CRASH_CHAIN_ITERS=20 … --test crash_leg -- --ignored --test-threads=1` (prebuilt failpoint binary, `failpoints=on`) | ok, 2 passed. C-CRASH-01: 20 iter, 45 kill -9, failpoint hits 4/4, 3/3, 3/3. C-CRASH-02: 20 iter, 14 kill -9. 0 violations | 62 s + 348 s build |
| 6 | `cargo deny check` | advisories, bans, licenses, sources ok | 3 s |
| 6 | `cargo tree … -e normal --target all` forbidden crates | no chrono, bincode, ring or openssl-sys | — |
| 7 | FAILPOINT_CRATES vs ci.yml; `uses:` pins; polyglot job | EQUAL (16 entries); 20/20 SHA-pinned; job present | — |

ci-local step times:
- toolchain 0 s, fmt 2 s, clippy 67 s, build 62 s;
- sdk lint 21 s, sdk test 25 s, guests 20 s, order-saga 23 s;
- **cargo test 4050 s**;
- polyglot: toolchains 1 s, python sdk tests 1 s (148 passed), ts sdk tests 0 s (`# pass 99`), conformance 233 s, clippy 24 s;
- sleepers 42 s, failpoint sync 0 s, **failpoint suites 3912 s**, campaign smoke 618 s, phase 7 legs 53 s;
- deny 3 s, vendored 2 s, deny fixtures 0 s, forbidden crates 0 s, workflow validation 8 s;
- SKIP: externals PG/MySQL/S3 (no URLs set, by design) and the three musl steps (`--no-musl`).

## SC1–SC5 verdicts
| SC | Verdict |
|----|---------|
| SC1 | **PASS.** 27 engine and 16 server scenarios pass in Rust, Python and TS: 135/135 cells, including the StarlingMonkey smoke and the TS→Py→Rust chain. |
| SC2 | **Rust PASS.** Python and TS are an accepted residual, so they are human_needed. C-RAND-01/02 and C-VAL-01 pass in every language. C-REBUILD-01 shows replay working across rebuilds. |
| SC3 | **PASS.** The 09-18 run did 100 + 100 iterations with 0 violations, and the 20 + 20 re-run is green. |
| SC4 | **PASS.** C-SAGA-01/02/03 pass in all three languages. |
| SC5 | **PASS on WSL2 (advisory).** Warm instantiate p50 is 0.46 ms for Python, 0.064 ms for QuickJS and 0.011 ms for StarlingMonkey. Native confirmation is human_needed. |
| WF-07 | **PASS.** C-NATIVE-01 and C-RAND-02 pass for Python and TS. |

## Reservations script output (09-20 check over the code at 762bbc5)
```
Code: Cmd [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 20, 21, 27, 45]
Intent [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 19]
Outbox [1, 2, 3, 4, 5, 6]
StartRun tags 1..23, 40, 41
EventKind [1, 16..29, 64..71, 96, 97]
TerminalStatus [0, 1, 2, 3, 4, 5]
Keyspaces 0..16
RejectReason count 16
InvalidCode [(22, 'BadScheduleFire'), (23, 'ScheduleOwnerMismatch'), (24, 'ReservedP8_24'), (25, 'ReservedP9_25'), (26, 'ReservedP9_26'), (27, 'ReservedP9_27'), (28, 'ReservedP9_28'), (29, 'BadRerunPrefix'), (30, 'BadDlqUpdate')]
MetaRecord count 17
P9 block Cmd 34-40             : empty OK
P9 block StartRun 36-39        : empty OK
P9 block Intent 19-22          : empty OK (exists P9: [19])
P9 block EventKind 98-105      : empty OK
P9 block TerminalStatus 10-11  : empty OK
P9 block Keyspace 19           : empty OK
P9 block RejectReason 20-22    : empty OK
P9 block InvalidCode 25-28     : empty OK (slots: ['ReservedP9_25', 'ReservedP9_26', 'ReservedP9_27', 'ReservedP9_28'])
P9 block Outbox 11-12          : empty OK
P9 block MetaRecord 22-23      : empty OK
P9 block meta ext 64-79        : empty OK
Intent 19 name: ChildJournal
RESERVATIONS CHECK: PASS
```
- RESERVATIONS.md gained a Phase 9 end-gate note.
- 11 P9 block rows now read "unused at Phase 9 end; fallback not needed". Intent 19 stays `exists (P9, 09-05)`.
- The 13 §11 name rows now read `exists (P9, <plan>)`.

## Docs written
- **Python README** (274 lines) and **TS README** (268 lines) cover:
  - the build recipe and a complete example;
  - the API reference and the event-loop model (what works and what does not, including top-level await and `crypto.subtle` in TS);
  - the WF-07 determinism rules;
  - D5/D6/D7/D11/F4 and PF-W1..W9, including step exclusivity and `sdk_protocol` timers;
  - children, with "cascade beats shielding";
  - saga, resources, reproducibility and publishing status.
- **Rust README:** a new "Children, saga, random_f64" section. The stale Phase 5 "Current server limitations" note was replaced (see Deviations).
- **docs/conformance.md** (173 lines): architecture, legs, strict checks, projection, comparison, naming, commands, how to add a scenario, how to add a language.
- **docs/children.md** (211 lines): ids, resolution, outcomes, opt-in journaling, row guard, detached children, cancel-op and parent close (including hard-stop reasons), cascade vs shielding, CAN chains (chain_done, deferred expiry), resume after close, repair table, failpoints, limits.
- **docs/storage-format.md §18.1:** 09-08's row guard, detached-row rule, duplicate StartChild handling and deferred-expiry mark (09-08 open issue 5).

## Human-needed (final)
1. Publish `ostro` to PyPI and npm (D-P9-19). Add `readme` to pyproject first.
2. Run the new `polyglot` job on remote CI (FOUND-05 pattern).
3. File the componentize-py determinism PR and the componentize-qjs residual issue (texts are in `sdks/python/toolchain/README.md`).
4. SC2 for Python and TS (accepted residuals, D-P9-3 step 3).
5. SC5 confirmation on the native perf machine (WSL2 already met it).
6. User review of D-P9-3 (outcome: step 3). There is no 09-07 fuel review candidate.

## Deviations from Plan
1. **ci-local ran with `--no-musl`** (coordinator instruction); the plan says with musl. The other gate commands ran as written.
2. **The P9 block is not entirely unused.** D-P9-31, a coordinator decision in 09-05, used Intent 19. The flip and the gate note record that. The must-have "no Phase 9 ordinal landed" is superseded by that decision.
3. **[Rule 1, doc] Rust README "Current server limitations".** The section still said effects and children were unsupported. That contradicted shipped behaviour, so it was replaced by a short pointer. The appended section was the only other edit.
4. **docs/storage-format.md §18.1 was added** (not in files_modified). The coordinator assigned it, as 09-08 open issue 5.
5. **REQUIREMENTS:** SDK-04 is left unticked with a Partial note, as the plan prescribes for a D-P9-3 residual.
6. **Status legend in 09-VALIDATION reworded.** The legend contained the literal "⬜ pending", which the plan's own verify (`! grep -q "⬜ pending"`) would reject.
7. **Main moved during the gate** (Phase 11/12 code commits after `762bbc5`, e.g. 11-13, 11-14, 11-17, 12-02). The gate result applies to `762bbc5`. No Phase 9 code changed afterwards.
8. **Commit mechanics.** Commits were built from the current `main` blobs with a private index and `commit-tree`, then landed with a CAS `update-ref`. The gate's worktree was never rebased under the running gate.
9. **PG legs SKIP in this run.** No private Postgres was started, so all three externals are SKIP. 09-01 and 09-08 ran the PG legs green.

## Open issues
- `class = "typescript"` sizes the core budget with the StarlingMonkey figure (A12).
- The crash leg is not wired into ci-local or CI (A30).
- `chain_done` is reserved by convention only (A18).
- A cancelled child op keeps its children row (A19).
- New metrics are missing from docs/metrics.md.
- `docs/data/sdk-resources.jsonl` still holds the pre-eng2-mem QuickJS records.
- `pyproject.toml` has no `readme` field.

These are recorded as `[FOLLOW-UP]` in STATE.md.

## Self-Check: PASSED
- FOUND: sdks/python/README.md, sdks/ts/README.md, docs/conformance.md, docs/children.md on main (96e9cee).
- FOUND: 96e9cee, 636f1c4, e07599f in `git log main`.
- Task verifies: VERIFY2 OK, VERIFY3 OK. Task 1 checks are in the log above; `grep -q "unused at Phase 9 end" .planning/RESERVATIONS.md` is ok.
