---
phase: 06-effects-bridge-connectors-secrets
plan: 20
subsystem: phase-gate
tags: [phase-gate, docs, replay-corpus, reservations, freeze, deny, ci-local, fx-01, fx-02, fx-03, fx-04, fx-05, fx-06, fx-07, fx-08, fx-09, fx-10]

requires:
  - phase: 06-effects-bridge-connectors-secrets
    provides: "06-01..06-19: effect seam, HTTP/SQL/object connectors, egress gate, secrets + providers, admin surface, SC1-SC5 e2e, CI DB service containers"
  - phase: 05-workflow-semantics-core-hardening
    provides: "05-12 replay corpus + export_run tool; WIT 0.1 freeze (wit/FROZEN.toml)"
provides:
  - "docs/connectors.md, docs/security-egress.md, docs/secrets.md (normative effect semantics, SSRF model, secrets model)"
  - "docs/03 §5.2 sweeper correction + ostro-objstore references; docs/02 FR-4.3 Azure footnote"
  - "five fx-* replay-corpus entries replaying green with no EffectDriver"
  - "RESERVATIONS Phase 6 rows flipped to exists (P6)"
  - "06-VALIDATION.md filled: per-task status, Gate results, Human-needed"
affects: [phase-6-verification, 07, 08-22]

tech-stack:
  added: []
  patterns:
    - "Phase gate runs one full ci-local (musl) with a private throwaway PG 14 exported through the untracked .ostro-test-env"

key-files:
  created:
    - docs/connectors.md
    - docs/security-egress.md
    - docs/secrets.md
    - tests/replay-corpus/fx-http-pay/
    - tests/replay-corpus/fx-http-redrive/
    - tests/replay-corpus/fx-sql-exactly-once/
    - tests/replay-corpus/fx-object-local/
    - tests/replay-corpus/fx-canary-scrubbed/
  modified:
    - docs/03-architecture.md
    - docs/02-prd.md
    - tests/replay-corpus/README.md
    - .planning/RESERVATIONS.md
    - .planning/phases/06-effects-bridge-connectors-secrets/06-VALIDATION.md
    - crates/ostro-server/tests/fx_object_e2e.rs (gate fix)
    - crates/ostro-server/tests/phase8_smoke.rs (rustfmt, gate fix)

key-decisions:
  - "Egress allow rules accept canonical spellings only (06-17 finding); documented in docs/security-egress.md, not changed in code (the URLs themselves are still parsed, normalised and denied)"
  - "06-13 provider gaps and the 06-11 SQL result buffering are documented limitations (docs/secrets.md §9, docs/connectors.md §9), not gate blockers"
  - "MySQL 8.4, SeaweedFS, GCS interop, remote CI legs and the user reviews stay human_needed; the OTLP canary leg (D-FX6) was delivered by 08-21 (otlp_canary, clean)"

requirements-completed: [FX-01, FX-02, FX-03, FX-04, FX-05, FX-06, FX-07, FX-08, FX-09, FX-10]

duration: 2h25m (this run; gate 2h03m)
completed: 2026-11-20
---

# Phase 6 Plan 20: Phase 6 Gate Summary

**Phase 6 closes on one full musl ci-local run against a private PG 14 (all 21 steps PASS, MySQL/S3 SKIP rows human_needed, PG legs really run: sql_pg 10 + 13 (failpoints) + 10 (musl), fx_sql_crash pg 2/2). The gate also covers the connectors, egress and secrets docs, five fx-* replay-corpus entries that replay with no EffectDriver, and the Phase 6 RESERVATIONS flip. The WIT sha, the golden prefix, the format corpus and deny.toml are all unchanged.**

## Performance

- **Completed:** 2026-11-20
- **Tasks:** 3/3
- The earlier attempt was cut off by a machine restart during its final ci-local run, after Tasks 1 and 2 had landed. This run finished Task 3 and ran the single shared gate for 06-20 and 08-22 on main `5faa97c`.

## Accomplishments

### Task 1: documentation (398af20)

- **docs/connectors.md** covers:
  - the effect lifecycle;
  - HTTP (the key formula and override, retries, the unknown-outcome window with its provider table, `http-target`);
  - SQL (the exactly-once protocol, the allow-list, the MySQL implicit-commit list, the `ostro_effects` DDL, GC, TLS roots);
  - object (backends, W7 ops, presign);
  - per-node rate limits;
  - the normative error-kind table;
  - terminal-run completions and SQL after COMMIT (B4/W12);
  - templated `Accept-Encoding: identity`.
- **docs/security-egress.md** covers the threat model, the three layers, the IANA lists, the full bypass table (including `fd00:ec2::254`), `allow_private`, manual redirects, env proxies and the operator exemption. It also says **rules must be written in canonical form**: the admin API refuses `http://%31%32%37.0.0.1` and `http://[::127.0.0.1]`, which addresses the 06-17 note.
- **docs/secrets.md** covers:
  - the template grammar and placement;
  - grants;
  - scrubbing limits;
  - the sealed store and key loading;
  - the providers;
  - the admin surface;
  - the `__ostro/` prefix;
  - §9 known limitations, which records the **06-13 gaps**: `#field`/upper-case keys are unreachable from templates, vaultrs keeps its token in a `String`, kube's config inference order, and there is no `renew_self`.
- **docs/connectors.md §9** records the **06-11 SQL follow-up**: results are fully buffered (`fetch_all`) before the caps apply, and streaming with an early stop needs `futures-core`.
- **docs/03:** §5.2 now describes the sweeper (effects rows plus the LeaderAnnounced wake), the EffectDriver seam is described, and the `object_store` references now point to `ostro-objstore`. **docs/02:** FR-4.3 carries the Azure-deferred footnote.
- This run (the 06-20 completion commit): the D-FX6 lines in docs/secrets.md and docs/connectors.md now say the OTLP canary leg ran in 08-21 (`otlp_canary`, clean). They no longer call it pending.

### Task 2: replay corpus + reservations + freeze/deny (ca8d1f3, 6ef3039, a404d76)

- New entries: `fx-http-pay`, `fx-http-redrive` (kill -9 re-drive, same key, one charge), `fx-sql-exactly-once` (PG 14), `fx-object-local` (W7 ops including presign-get), `fx-canary-scrubbed`.
  - All five were exported with `export_run` from path-neutral guests.
  - The canary and DB-password scan found 0 hits.
  - No existing entry was re-recorded.
- `cargo test -p ostro-engine --test replay_corpus` is green with no EffectDriver. It ran inside the gate's `cargo test --workspace`.
- RESERVATIONS:
  - The keyspace #12 effects rows were flipped to `exists (P6, 06-03)`.
  - Cmd 13 CompleteEffect, Intent 9/10 and InvalidCode 15 were already `exists (P6, 06-02)`.
  - The unused spares stay `reserved P6`.
- Freeze and deny re-check on `5faa97c`:

| Check | Result |
|-------|--------|
| `sha256sum wit/ostro-runtime-0.1.0/runtime.wit` vs `wit/FROZEN.toml` | `611d2982…bd98e` = `611d2982…bd98e` (equal) |
| golden_frozen + ostro-witcheck | green (inside `cargo test --workspace`) |
| format_corpus (ostro-partition) | green (inside `cargo test --workspace`) |
| `cargo deny check` | PASS (gate step, stock deny.toml) |
| `cargo tree --workspace -e normal --locked` → no `chrono v` | no chrono; no zstd-sys, openssl-sys or object_store either |
| `git diff d4d150d..HEAD -- deny.toml` (06-02 base) | empty |
| golden `commands.txt` vs 06-02 base | append-only (+3, −0) |

### Task 3: full gate + VALIDATION

- `bash scripts/ci-local.sh` (full, with musl, `OSTRO_TEST_PG_URL` from `.ostro-test-env`) passed every step, with exit 0. The table, PG leg counts and SKIP banners are below and in 06-VALIDATION "Gate results".

| Step | Result | Time |
|------|--------|------|
| toolchain 1.100.0 | PASS | 0 s |
| cargo fmt --check | PASS | 2 s |
| cargo clippy -D warnings | PASS | 55 s |
| cargo build (warning-free) | PASS | 86 s |
| sdk workspace fmt + clippy | PASS | 21 s |
| sdk workspace test + wasm build | PASS | 23 s |
| guests build (wasm32-wasip2) | PASS | 12 s |
| order-saga example | PASS | 21 s |
| external: OSTRO_TEST_MYSQL_URL | SKIP (human_needed) | - |
| external: OSTRO_TEST_S3_URL | SKIP (human_needed) | - |
| cargo test | PASS (1,959 passed) | 2164 s |
| semantics sleepers (serial) | PASS | 39 s |
| failpoint list sync (ci.yml) | PASS | 0 s |
| failpoint suites | PASS (956 passed) | 2093 s |
| server campaign smoke (20 kills) | PASS | 413 s |
| cargo deny check | PASS | 1 s |
| check-vendored.sh | PASS | 1 s |
| check-deny-fixtures.sh | PASS | 1 s |
| no chronik/tokio-uring/prometheus | PASS | 0 s |
| workflow validation | PASS | 2 s |
| zig + cargo-zigbuild present | PASS | 0 s |
| musl release (static, mimalloc) | PASS | 311 s |
| musl zigbuild test | PASS (1,959 passed) | 2146 s |

`all steps passed`, exit 0. Run on main `5faa97c` (worktree, fresh `CARGO_TARGET_DIR`), 2026-11-20 21:50–23:53 CET (about 2 h 03 min). `OSTRO_TEST_PG_URL` was set (banner: `set: OSTRO_TEST_PG_URL (ostro-bridge sql_pg, ostro fx_sql_crash pg_after_commit/pg_before_user_stmt)`), pointing at a private throwaway PostgreSQL 14.24 cluster (127.0.0.1:55468, scram, data dir in the session scratchpad, loaded via the untracked `.ostro-test-env`; nothing committed).

PG legs that really ran:
- `ostro-bridge --test sql_pg`: 10 passed in `cargo test` and 10 passed in `musl zigbuild test`; 13 passed in `failpoint suites` (with the three `failpoints::pg_*` cases).
- `ostro --features failpoints --test fx_sql_crash`: `pg_after_commit` and `pg_before_user_stmt` passed in `failpoint suites`. Re-run alone with `--nocapture`: `N=50 held in 452 ms, restart (readyz) 164 ms, all terminal 471 ms after kill -9` and `N=50 held in 533 ms, restart 170 ms, all terminal 470 ms`.
- The PG server recorded 1,344 committed transactions and 2,506 inserted tuples. The tests drop their own schemas.

The MySQL legs (`sql_mysql`, `fx_sql_crash mysql_*`) and the S3 legs passed as loud SKIPs.

Campaign smoke: `kills=20 lost_acked_runs=0 duplicate_journal_entries=0 non_terminal_runs=0 lost_acked_signals=0 wrong_results=0 other_violations=0`, acked_runs=231, RESULT: PASS.

The musl release binary (`x86_64-unknown-linux-musl/release/ostro`) is 44,033,456 bytes (42.0 MiB), static, mimalloc. No 06-02 size baseline was recorded, so this number is the baseline from now on.

No step failed and no test needed a re-run.

- 06-VALIDATION.md is filled in:
  - all 46 task rows are ✅; the five rows with MySQL/S3 legs carry a "⚠ human_needed" note for those legs;
  - `nyquist_compliant: true` and `wave_0_complete: true`;
  - the Gate results section;
  - the Human-needed section (a–g, each with its command).

## Notes addressed

- **06-17 canonical spellings:** documented in docs/security-egress.md ("Rules must be written in canonical form").
- **06-13 known gaps** and the **06-11 SQL buffering/streaming follow-up:** recorded as documented limitations in docs/secrets.md §9 and docs/connectors.md §9.
- **06-18 FAILPOINT_CRATES:** `scripts/ci-local.sh` lists `ostro:fx_redrive` and `ostro:fx_sql_crash`. The gate's "failpoint list sync (ci.yml)" step confirms ci.yml runs the same commands in the same order.

## Task Commits

1. **Task 1 (docs):** `398af20`, plus `the 06-20 completion commit` (the D-FX6 status lines, this run)
2. **Task 2 (corpus + reservations):** `ca8d1f3`, `6ef3039` (the entry files that ca8d1f3's file list missed), `a404d76`
3. **Task 3 (gate + VALIDATION):** `the 06-20 completion commit (docs D-FX6 lines, VALIDATION, SUMMARY, ROADMAP/STATE hunks)`
4. **Gate fixes:** `8833730` (rustfmt `phase8_smoke.rs`, 08-18 drift) and `8533678` (`fx_object_e2e` `tamper()`)

## Deviations from Plan

1. **[Rule 1 - Bug, test] `fx_object_e2e` `tamper()` (8533678).** It replaced the last signature digit with `A`. The local presign verifier decodes hex case-insensitively, so when that digit was already `a` the "tampered" URL stayed valid. This failed about 1 run in 8 in the first gate attempt. The fix swaps `0` and `1`. Test-only; no product change.
2. **[Rule 3 - Blocking] rustfmt drift in `phase8_smoke.rs` (8833730).** The drift came from 08-18. The gate's `cargo fmt --check` step failed on it. Formatting only.
3. **ca8d1f3's file list missed the entry directories.** 6ef3039 added them unchanged.
4. **One shared gate.** The full ci-local run is shared with 08-22 and ran once on the final code. Everything committed after `5faa97c` is docs/planning only.


## Human-needed

The full list, with commands, is in 06-VALIDATION.md under "Human-needed":
- the MySQL 8.4 legs;
- the SeaweedFS legs;
- GCS S3-interop;
- the remote CI legs (aarch64-musl, macOS);
- user review of FX-05/Azure, D-FX2 and D-FX6;
- the optional real Vault leg.

The OTLP canary leg (D-FX6) is no longer pending: 08-21 ran it, and it was clean.

## Next Phase Readiness

Phase 6 is ready for `/gsd:verify-work`. Per the brief, it was not marked complete here.
